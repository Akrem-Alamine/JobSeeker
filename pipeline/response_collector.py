"""
response_collector.py — Collect delivery failures and auto-replies from Gmail.

Scans INBOX for:
  - Hard bounces (mailer-daemon / postmaster): deletes email + removes contact from DB.
  - Auto-replies / OOO (any language): archives email, contact kept in DB.
"""

import email as emaillib
import imaplib
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from email.header import decode_header


IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

BOUNCE_SENDERS = ['mailer-daemon', 'postmaster']

BOUNCE_SUBJECTS = [
    'Delivery Status Notification (Failure)',
    'Undeliverable:',
    'Undelivered Mail',
    'Email Delivery Failure',
    'Returned mail:',
    'failure notice',
    'Mail delivery failed',
    'Mail Delivery Failed',
    'Delivery Failure',
    "couldn't be delivered",
    "could not be delivered",
    "message not delivered",
    "message was not delivered",
]

# If the subject contains any of these the bounce is soft (temporary) — skip
# UNLESS the body reveals the failure is actually permanent (see _is_hard_bounce)
SOFT_KEYWORDS = ['(delay)', 'incomplete', 'will retry', 'retry for', 'temporary problem']

# ── auto-reply / OOO detection ────────────────────────────────────────────────

# Headers that definitively mark an email as auto-generated
AUTO_REPLY_HEADERS = {
    'Auto-Submitted':           lambda v: v.lower() not in ('no', ''),
    'X-Auto-Response-Suppress': lambda v: bool(v),
    'X-Autoreply':              lambda v: v.lower() in ('yes', 'true', '1'),
    'Precedence':               lambda v: v.lower() in ('auto-reply', 'bulk', 'junk'),
}

# Subject keywords across languages — ASCII-only subset used for IMAP SUBJECT search.
# Non-ASCII keywords are still used in _is_auto_reply() after fetching the full email.
AUTO_REPLY_SUBJECT_KEYWORDS = [
    # English
    'out of office', 'ooo', 'automatic reply', 'auto reply', 'auto-reply',
    'autoreply', 'vacation', 'away from', 'on leave', 'annual leave',
    'on holiday', 'off today', 'be back', 'i am away', 'i will be away',
    'currently away', 'currently out', 'not in office', 'not available',
    # French (ASCII-safe) — "automatique" catches "réponse automatique"
    'hors du bureau', 'absence du bureau', 'en vacances', 'absent', 'automatique',
    # Spanish (ASCII-safe)
    'fuera de la oficina', 'ausencia', 'de vacaciones',
    # German (ASCII-safe)
    'abwesenheit', 'automatische antwort', 'urlaub',
    # Italian
    'fuori ufficio', 'risposta automatica', 'assenza', 'ferie',
    # Portuguese (ASCII-safe)
    'fora do escritorio', 'ausencia', 'ferias',
    # Dutch
    'buiten kantoor', 'afwezig', 'automatisch antwoord',
    # Finnish
    'automaattinen vastaus', 'poissa toimistosta',
    # Swedish/Norwegian/Danish
    'autosvar', 'frånvarande', 'ikke til stede', 'ikke tilstede',
    # Polish
    'automatyczna odpowiedz', 'poza biurem',
    # Turkish
    'otomatik yanit', 'ofis disi',
]

# Additional keywords with non-ASCII characters — used only in post-fetch subject/body check
AUTO_REPLY_SUBJECT_KEYWORDS_EXTENDED = AUTO_REPLY_SUBJECT_KEYWORDS + [
    'réponse automatique', 'congé',
    'respuesta automática',
    'außer haus', 'nicht im büro',
    'fora do escritório', 'resposta automática', 'ausência', 'férias',
    'رد تلقائي', 'خارج المكتب',
]

# Body keywords (fallback when header/subject don't match)
AUTO_REPLY_BODY_KEYWORDS = [
    'i am currently out of the office',
    'i will be out of the office',
    'i am on vacation',
    'i am away',
    'this is an automatic reply',
    'this is an automated reply',
    'this message is sent automatically',
    'do not reply to this email',
    'do not reply to this message',
    'this email was sent automatically',
]


def _is_auto_reply(msg, subject: str, body: str) -> bool:
    """Return True if the email is an automatic reply / OOO."""
    # 1. Check headers
    for header, check in AUTO_REPLY_HEADERS.items():
        val = msg.get(header, '')
        if val and check(val):
            return True
    # 2. Check subject (use extended list including non-ASCII)
    s = subject.lower()
    if any(kw in s for kw in AUTO_REPLY_SUBJECT_KEYWORDS_EXTENDED):
        return True
    # 3. Check body (first 500 chars enough)
    b = body[:500].lower()
    if any(kw in b for kw in AUTO_REPLY_BODY_KEYWORDS):
        return True
    return False


# Body signals that mean the address is permanently dead even inside a "(Delay)" notification
PERMANENT_BODY_SIGNALS = [
    "address couldn't be found",
    "address could not be found",
    "domain couldn't be found",
    "domain could not be found",
    "unable to receive mail",
    "unable to receive email",
    "does not exist",
    "user unknown",
    "no such user",
    "no such address",
    "account does not exist",
    "email account.*does not exist",
    "550 5.1.1",
    "550 5.1.2",
    "550 5.7.13",
    "525 5.7.13",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _since_date(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT MIN(sent_at) FROM outreach WHERE status='sent'"
    ).fetchone()
    if row and row[0]:
        try:
            dt = datetime.fromisoformat(row[0])
            return dt.strftime("%d-%b-%Y")
        except Exception:
            pass
    return (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%d-%b-%Y")


def decode_str(value) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    result = ""
    for part, charset in parts:
        if isinstance(part, bytes):
            result += part.decode(charset or "utf-8", errors="replace")
        else:
            result += str(part)
    return result.strip()


def extract_body(msg) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                charset = part.get_content_charset() or "utf-8"
                payload = part.get_payload(decode=True)
                if payload:
                    body += payload.decode(charset, errors="replace")
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode(charset, errors="replace")
    return body.strip()


def _is_hard_bounce(subject: str, body: str = "") -> bool:
    """
    Return True for permanent failures.
    A soft subject (Delay/incomplete) is still treated as hard if the body
    contains signals that the address/domain is permanently dead.
    """
    s = subject.lower()
    if not any(x in s for x in SOFT_KEYWORDS):
        return True
    # Delay subject — only treat as hard if body reveals permanent failure
    if body:
        b = body.lower()
        return any(x in b for x in PERMANENT_BODY_SIGNALS)
    return False


def _extract_failed_email(msg, body: str) -> str | None:
    """
    Try three methods to extract the failed recipient address from a bounce:
    1. X-Failed-Recipients header
    2. Final-Recipient in DSN multipart/report (RFC 3464)
    3. Body regex patterns
    """
    # 1. X-Failed-Recipients header
    xfr = msg.get('X-Failed-Recipients', '')
    if xfr and '@' in xfr:
        return xfr.strip().lower().split(',')[0].strip()

    # 2. Final-Recipient from DSN part
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == 'message/delivery-status':
                payload = part.get_payload(decode=False)
                if isinstance(payload, str):
                    m = re.search(
                        r'Final-Recipient\s*:.*?rfc822\s*;\s*<?([^\s<>,]+@[^\s<>,]+)>?',
                        payload, re.I
                    )
                    if m:
                        return m.group(1).strip().lower()

    # 3. Body regex patterns
    patterns = [
        r"wasn't delivered to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?",
        r"couldn't be delivered to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?",
        r"message to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?\s+couldn",
        r"your message to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?",
        r"you sent to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?",
        r"sent to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?\s+couldn",
        r"delivery has failed.*?groups?:\s*<?([^\s<>\"]+@[^\s<>\"]+)>?",
        r"message.*?not.*?delivered.*?to\s+<?([^\s<>\"]+@[^\s<>\"]+)>?",
        r"<([^>]+@[^>]+)>.*?(?:couldn't be delivered|wasn't found|failed|blocked|unknown)",
        r"recipient[:\s]+<?([^\s<>\"]+@[^\s<>\"]+)>?",
    ]
    for p in patterns:
        m = re.search(p, body, re.I | re.S)
        if m:
            addr = m.group(1).strip().lower().strip('<>.,;: ')
            if '@' in addr and '.' in addr.split('@')[1]:
                return addr
    return None


# ── DB ────────────────────────────────────────────────────────────────────────

def init_responses_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS responses (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id  INTEGER,
            from_email  TEXT,
            from_name   TEXT,
            subject     TEXT,
            body        TEXT,
            received_at TEXT,
            folder      TEXT,
            gmail_uid   TEXT,
            type        TEXT DEFAULT 'delivery_failure',
            processed   INTEGER DEFAULT 0,
            new_email   TEXT,
            UNIQUE(from_email, gmail_uid)
        )
    """)
    for col, defn in [
        ('type',         'TEXT DEFAULT "delivery_failure"'),
        ('processed',    'INTEGER DEFAULT 0'),
        ('new_email',    'TEXT'),
        ('processed_at', 'TEXT'),
    ]:
        try:
            conn.execute(f"ALTER TABLE responses ADD COLUMN {col} {defn}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


# ── IMAP ──────────────────────────────────────────────────────────────────────

def connect_imap(email: str, password: str) -> imaplib.IMAP4_SSL:
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    mail.login(email, password.replace(" ", ""))
    return mail


# ── collect ───────────────────────────────────────────────────────────────────

def collect(mail: imaplib.IMAP4_SSL, gmail_user: str,
            conn: sqlite3.Connection) -> list[dict]:
    """
    Scan INBOX for hard bounces (delivery_failure) and auto-replies / OOO (auto_reply).
    Saves each to the responses table. Returns list of newly saved items.
    """
    sent_map: dict[str, int] = {
        r['email'].lower(): r['contact_id']
        for r in conn.execute(
            "SELECT contact_id, email FROM outreach WHERE status='sent'"
        ).fetchall()
    }

    since = _since_date(conn)
    saved: list[dict] = []

    status, _ = mail.select('INBOX', readonly=True)
    if status != 'OK':
        return saved

    # ── bounce UIDs (by sender + subject keywords) ────────────────────────────
    bounce_uids: set[bytes] = set()
    for sender in BOUNCE_SENDERS:
        try:
            _, data = mail.uid('search', None, f'SINCE "{since}" FROM "{sender}"')
            if data[0]:
                bounce_uids.update(data[0].split())
        except Exception:
            pass
    for subj in BOUNCE_SUBJECTS:
        try:
            _, data = mail.uid('search', None, f'SINCE "{since}" SUBJECT "{subj}"')
            if data[0]:
                bounce_uids.update(data[0].split())
        except Exception:
            pass

    # ── auto-reply UIDs (by header — language-agnostic catch-all) ────────────────
    auto_uids: set[bytes] = set()
    for header, value in [
        ('Auto-Submitted',           'auto-replied'),
        ('Auto-Submitted',           'auto-generated'),
        ('Precedence',               'auto-reply'),
        ('X-Autoreply',              'yes'),
        ('X-Auto-Response-Suppress', 'All'),
        ('X-Auto-Response-Suppress', 'OOF'),
    ]:
        try:
            _, data = mail.uid('search', None, f'SINCE "{since}" HEADER "{header}" "{value}"')
            if data[0]:
                auto_uids.update(data[0].split())
        except Exception:
            pass

    # ── auto-reply UIDs (by subject keywords — multilingual) ──────────────────
    for kw in AUTO_REPLY_SUBJECT_KEYWORDS:
        try:
            _, data = mail.uid('search', None, f'SINCE "{since}" SUBJECT "{kw}"')
            if data[0]:
                auto_uids.update(data[0].split())
        except Exception:
            pass

    # ── auto-reply UIDs (by body phrases — catches OOO with no special headers/subject) ──
    for phrase in AUTO_REPLY_BODY_KEYWORDS:
        try:
            _, data = mail.uid('search', None, f'SINCE "{since}" BODY "{phrase}"')
            if data[0]:
                auto_uids.update(data[0].split())
        except Exception:
            pass

    all_uids = bounce_uids | auto_uids

    for uid in all_uids:
        uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
        try:
            _, fetch_data = mail.uid('fetch', uid, '(RFC822)')
            if not fetch_data or not fetch_data[0]:
                continue
            raw = fetch_data[0][1]
            msg = emaillib.message_from_bytes(raw)
        except Exception:
            continue

        subject  = decode_str(msg.get('Subject', ''))
        date_raw = msg.get('Date', '')
        from_raw = decode_str(msg.get('From', ''))

        from_email = from_raw
        if '<' in from_raw and '>' in from_raw:
            from_email = from_raw.split('<')[-1].rstrip('>').strip().lower()
        else:
            from_email = from_raw.lower().strip()

        # Skip emails we sent ourselves
        if gmail_user and from_email == gmail_user.lower():
            continue

        body = extract_body(msg)

        # Classify: delivery_failure takes priority over auto_reply
        if uid in bounce_uids and _is_hard_bounce(subject, body):
            email_type   = 'delivery_failure'
            failed_email = _extract_failed_email(msg, body)
            contact_id   = sent_map.get(failed_email) if failed_email else None
            save_email   = failed_email
        elif _is_auto_reply(msg, subject, body):
            email_type   = 'auto_reply'
            contact_id   = sent_map.get(from_email)
            save_email   = None
        else:
            continue

        try:
            conn.execute("""
                INSERT INTO responses
                    (contact_id, from_email, from_name, subject, body,
                     received_at, folder, gmail_uid, type, new_email)
                VALUES (?, ?, ?, ?, ?, ?, 'inbox', ?, ?, ?)
                ON CONFLICT(from_email, gmail_uid) DO UPDATE SET
                    new_email  = excluded.new_email,
                    contact_id = excluded.contact_id,
                    processed  = 0
            """, (contact_id, from_email, from_email, subject, body,
                  date_raw, uid_str, email_type, save_email))
            conn.commit()
            saved.append({
                'from_email': save_email or from_email,
                'subject':    subject,
                'type':       email_type,
                'folder':     'inbox',
            })
        except sqlite3.IntegrityError:
            pass

    try:
        mail.close()
    except Exception:
        pass

    return saved


# ── process ───────────────────────────────────────────────────────────────────

def process_one(mail: imaplib.IMAP4_SSL, response: dict,
                conn: sqlite3.Connection) -> str:
    """
    Move the email to Bin.
    For delivery_failure: also remove the dead contact from DB.
    For auto_reply: just archive — contact is kept.
    """
    resp_id    = response['id']
    contact_id = response['contact_id']
    uid        = str(response['gmail_uid'])
    email_type = response.get('type', 'delivery_failure')
    parts: list[str] = []

    if email_type == 'auto_reply':
        # Auto-reply: remove Important flag, then move to "auto" label.
        try:
            status, _ = mail.select('INBOX', readonly=False)
            if status != 'OK':
                parts.append('inbox unavailable')
            else:
                # 1. Remove Important while we still have the INBOX UID
                try:
                    mail.uid('store', uid, '-FLAGS', '\\Important')
                except Exception:
                    pass
                try:
                    mail.uid('store', uid, '-X-GM-LABELS', '(\\Important)')
                    parts.append('important removed')
                except Exception:
                    parts.append('important removal skipped')

                # 2. Ensure "Auto" label exists
                try:
                    mail.create('"Auto"')
                except Exception:
                    pass  # already exists

                # 3. Move from INBOX to "Auto"
                done = False
                try:
                    rv, _ = mail.uid('MOVE', uid, '"Auto"')
                    if rv == 'OK':
                        parts.append('moved to Auto')
                        done = True
                except Exception:
                    pass
                if not done:
                    rv, _ = mail.uid('copy', uid, '"Auto"')
                    if rv == 'OK':
                        mail.uid('store', uid, '+FLAGS', '\\Deleted')
                        mail.expunge()
                        parts.append('moved to Auto (fallback)')
                    else:
                        parts.append('move to Auto failed')

        except Exception as e:
            parts.append(f'auto label failed: {e}')
        parts.append('contact kept')

    else:
        # Delivery failure: move to Bin and remove dead contact.
        # Primary: MOVE command (RFC 6851). Fallback: COPY + DELETE + EXPUNGE.
        try:
            status, _ = mail.select('INBOX', readonly=False)
            if status != 'OK':
                parts.append('inbox unavailable')
            else:
                done = False
                for bin_folder in ['"[Gmail]/Bin"', '"[Gmail]/Trash"']:
                    try:
                        rv, _ = mail.uid('MOVE', uid, bin_folder)
                        if rv == 'OK':
                            parts.append(f'moved to {bin_folder.strip(chr(34))}')
                            done = True
                            break
                    except Exception:
                        pass
                if not done:
                    for bin_folder in ['"[Gmail]/Bin"', '"[Gmail]/Trash"']:
                        rv, _ = mail.uid('copy', uid, bin_folder)
                        if rv == 'OK':
                            break
                    mail.uid('store', uid, '+FLAGS', '\\Deleted')
                    mail.expunge()
                    parts.append('deleted (fallback)')
        except Exception as e:
            parts.append(f'delete failed: {e}')

        if contact_id:
            conn.execute("DELETE FROM outreach WHERE contact_id=?", (contact_id,))
            conn.execute("DELETE FROM contacts WHERE id=?",         (contact_id,))
            conn.commit()
            parts.append('contact removed')
        else:
            parts.append('contact not found in DB')

    conn.execute(
        "UPDATE responses SET processed=1, processed_at=? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), resp_id),
    )
    conn.commit()
    return ', '.join(parts)


# ── collect + process in one pass ─────────────────────────────────────────────

def collect_and_process(mail: imaplib.IMAP4_SSL, gmail_user: str,
                        conn: sqlite3.Connection) -> dict:
    """
    Scan inbox, save each new response, and immediately process it.
    Returns {bounces, auto_replies, errors}.
    """
    init_responses_table(conn)
    saved = collect(mail, gmail_user, conn)
    stats = {'bounces': 0, 'auto_replies': 0, 'errors': 0}
    if not saved:
        return stats
    pending = [dict(r) for r in conn.execute(
        "SELECT * FROM responses WHERE processed=0 ORDER BY type, id"
    ).fetchall()]
    for resp in pending:
        try:
            process_one(mail, resp, conn)
            if resp['type'] == 'delivery_failure':
                stats['bounces'] += 1
            else:
                stats['auto_replies'] += 1
        except Exception:
            stats['errors'] += 1
    return stats

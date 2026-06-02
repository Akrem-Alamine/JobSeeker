"""
response_collector.py — Collect and process replies to outreach emails.

Classification:
  human_reply      — real person replied
  auto_reply       — any automatic response (OOO, vacation, no-reply bots, etc.)
  delivery_failure — bounce / dead address / unmonitored inbox
"""

import email as emaillib
import imaplib
import re
import sqlite3
import unicodedata
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from pathlib import Path

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993


def _since_date(conn: sqlite3.Connection) -> str:
    """Return IMAP SINCE date: earliest sent email, or 90 days ago as fallback."""
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

# "automat" root covers: automatic, automatique, automatisch, automatico,
# automaattinen, automático, автоматически, etc.
AUTO_ROOT = 'automat'

# Additional auto-reply indicators that don't contain "automat"
AUTO_EXTRA = [
    'out of office', 'out-of-office', 'autosvar', 'autosvara',
    'absence', 'hors du bureau', 'abwesenheit', 'risposta auto',
    'no longer monitored', 'unmonitored', 'i am away', 'i am currently away',
    'i am out', 'i will be out', 'on leave', 'on vacation', 'on holiday',
    'en vacances', 'in urlaub', 'не читается', 'fuera de la oficina',
]

DELIVERY_SENDERS  = [
    'mailer-daemon', 'postmaster', 'mail delivery subsystem',
    'mail delivery system', 'email delivery',
]
DELIVERY_SUBJECTS = [
    'delivery status', 'undeliverable', 'mail delivery failed',
    'failure notice', 'returned mail', 'delivery failure',
    'message not delivered', 'non-delivery', 'message blocked',
    'blocked message', 'delivery failure', 'email delivery failure',
    'couldn\'t be delivered', 'could not be delivered',
]
DEAD_BODY_PATTERNS = [
    'no longer working', 'no longer at', 'no longer with',
    'left the company', "n'exerce plus", 'no longer employed',
    'email is no longer', 'will not be read', 'ne sera plus consultée',
    'décommissionnée', 'unmonitored email',
]


# ── helpers ──────────────────────────────────────────────────────────────────

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


def _extract_failed_email(body: str) -> str | None:
    patterns = [
        r"wasn't delivered to\s+([^\s<>\"]+@[^\s<>\"]+)",
        r"couldn't be delivered to\s+([^\s<>\"]+@[^\s<>\"]+)",
        r"couldn't be delivered.*?([^\s<>\"]+@[^\s<>\"]+)",
        r"message to\s+([^\s<>\"]+@[^\s<>\"]+)\s+has been blocked",
        r"message to\s+([^\s<>\"]+@[^\s<>\"]+)\s+couldn",
        r"your message to\s+([^\s<>\"]+@[^\s<>\"]+)",
        r"failed.*?recipient.*?<([^>]+)>",
        r"delivery.*?failure.*?<([^>]+@[^>]+)>",
        r"<([^>]+@[^>]+)>.*?(?:blocked|failed|undeliverable)",
        r"address.*?<([^>]+@[^>]+)>",
        r"recipient[:\s]+([^\s<>\"]+@[^\s<>\"]+)",
    ]
    for p in patterns:
        m = re.search(p, body, re.I | re.S)
        if m:
            addr = m.group(1).strip().lower().strip('<>.,;:')
            if '@' in addr:
                return addr
    return None


# ── classification ────────────────────────────────────────────────────────────

def _ascii(text: str) -> str:
    """Strip accents so 'automática' → 'automatica', enabling AUTO_ROOT match."""
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')


def classify(subject: str, body: str, from_raw: str) -> str:
    """Returns one of: human_reply, auto_reply, delivery_failure."""
    s    = subject.lower()
    b    = body.lower()
    fr   = from_raw.lower()
    # ASCII-normalized versions catch accented variants (automática, automatický…)
    s_a  = _ascii(s)
    b_a  = _ascii(b)

    # Delivery failure (bounce / undeliverable)
    if any(x in fr for x in DELIVERY_SENDERS):
        return 'delivery_failure'
    if any(x in s for x in DELIVERY_SUBJECTS):
        return 'delivery_failure'

    # Dead inbox (person left, email decommissioned) → treat as failure
    if any(x in b for x in DEAD_BODY_PATTERNS):
        return 'delivery_failure'

    # Auto reply — check both original (catches Cyrillic etc.) and ASCII-normalized
    for _s, _b in [(s, b[:800]), (s_a, b_a[:800])]:
        if AUTO_ROOT in _s or AUTO_ROOT in _b:
            return 'auto_reply'
        if any(x in _s for x in AUTO_EXTRA) or any(x in _b for x in AUTO_EXTRA):
            return 'auto_reply'

    return 'human_reply'


# ── DB setup ──────────────────────────────────────────────────────────────────

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
            type        TEXT DEFAULT 'human_reply',
            processed   INTEGER DEFAULT 0,
            new_email   TEXT,
            UNIQUE(from_email, gmail_uid)
        )
    """)
    for col, defn in [
        ('type',      'TEXT DEFAULT "human_reply"'),
        ('processed', 'INTEGER DEFAULT 0'),
        ('new_email', 'TEXT'),
    ]:
        try:
            conn.execute(f"ALTER TABLE responses ADD COLUMN {col} {defn}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


# ── IMAP connect ──────────────────────────────────────────────────────────────

def connect_imap(email: str, password: str) -> imaplib.IMAP4_SSL:
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    mail.login(email, password)
    return mail


# ── collect ───────────────────────────────────────────────────────────────────

def collect(mail: imaplib.IMAP4_SSL, gmail_user: str,
            conn: sqlite3.Connection) -> list[dict]:
    """
    Scan only the 'Outreach Responses' Gmail label.
    All replies land there because outgoing emails have
    Reply-To: <user>+outreach@<domain> and a Gmail filter routes them.
    """
    sent_map: dict[str, int] = {
        r['email'].lower(): r['contact_id']
        for r in conn.execute(
            "SELECT contact_id, email FROM outreach WHERE status='sent'"
        ).fetchall()
    }

    saved: list[dict] = []
    since = _since_date(conn)

    status, _ = mail.select('"Outreach Responses"', readonly=True)
    if status != "OK":
        return saved

    try:
        _, data = mail.uid('search', None, f'SINCE "{since}" ALL')
    except Exception:
        return saved

    uids = data[0].split() if data[0] else []

    for uid in uids:
        uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)

        try:
            _, fetch_data = mail.uid('fetch', uid, '(RFC822)')
            if not fetch_data or not fetch_data[0]:
                continue
            raw = fetch_data[0][1]
            msg = emaillib.message_from_bytes(raw)
        except Exception:
            continue

        subject  = decode_str(msg.get("Subject", ""))
        date_raw = msg.get("Date", "")
        from_raw = decode_str(msg.get("From", ""))

        from_email, from_name = from_raw, from_raw
        if "<" in from_raw and ">" in from_raw:
            from_email = from_raw.split("<")[-1].rstrip(">").strip().lower()
            from_name  = from_raw.split("<")[0].strip().strip('"')
        else:
            from_email = from_raw.lower().strip()

        if from_email == gmail_user.lower():
            continue

        body       = extract_body(msg)
        resp_type  = classify(subject, body, from_raw)
        contact_id = sent_map.get(from_email)

        if resp_type == 'delivery_failure' and not contact_id:
            failed = _extract_failed_email(body)
            if failed:
                contact_id = sent_map.get(failed)

        try:
            conn.execute("""
                INSERT INTO responses
                    (contact_id, from_email, from_name, subject, body,
                     received_at, folder, gmail_uid, type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(from_email, gmail_uid) DO UPDATE SET
                    type = excluded.type
            """, (contact_id, from_email, from_name, subject, body,
                  date_raw, 'outreach_responses', uid_str, resp_type))
            if conn.total_changes:
                saved.append({'from_email': from_email, 'subject': subject,
                              'type': resp_type, 'folder': 'outreach_responses'})
        except sqlite3.IntegrityError:
            pass

    try:
        mail.close()
    except Exception:
        pass

    conn.commit()
    return saved


# ── process ───────────────────────────────────────────────────────────────────

def _open(mail: imaplib.IMAP4_SSL, folder: str) -> bool:
    candidates = {
        'outreach_responses': ['"Outreach Responses"'],
        'inbox':              ['INBOX'],
        'auto':               ['Auto'],
        'spam':               ['"[Gmail]/Spam"', '"[Google Mail]/Spam"', 'Spam'],
        'updates':            ['"Outreach Responses"'],
    }
    for name in candidates.get(folder, ['"Outreach Responses"']):
        try:
            status, _ = mail.select(name)
            if status == "OK":
                return True
        except Exception:
            pass
    return False


def _mark_important(mail, uid, folder) -> str:
    if not _open(mail, folder):
        return "could not open folder"
    try:
        mail.uid('store', uid, '+X-GM-LABELS', '(\\Important)')
        return "marked as important"
    except Exception:
        try:
            mail.uid('store', uid, '+FLAGS', '\\Flagged')
            return "starred (important label not supported)"
        except Exception as e:
            return f"error: {e}"


def _delete_email(mail, uid, folder):
    if _open(mail, folder):
        try:
            mail.uid('store', uid, '+FLAGS', '\\Deleted')
            mail.expunge()
        except Exception:
            pass


def _ensure_label(mail, name: str):
    try:
        mail.create(name)
    except Exception:
        pass  # already exists or server rejected — safe to continue


def _apply_label(mail, uid, folder, label: str) -> str:
    """Add a Gmail label to the message, leaving it in the inbox."""
    _ensure_label(mail, label)
    if not _open(mail, folder):
        return "could not open folder"
    # Primary: Gmail X-GM-LABELS extension — operates on the message directly
    # Format must be ("LabelName") — quoted string inside parens
    try:
        mail.uid('store', uid, '+X-GM-LABELS', f'("{label}")')
        return f"labeled '{label}'"
    except Exception as e1:
        pass
    # Fallback: COPY to label folder — Gmail maps IMAP folders to labels
    try:
        mail.uid('copy', uid, f'"{label}"')
        return f"labeled '{label}' (copy)"
    except Exception as e2:
        return f"could not apply label '{label}': {e2}"


def process_one(mail: imaplib.IMAP4_SSL, response: dict,
                conn: sqlite3.Connection) -> str:
    resp_id    = response['id']
    resp_type  = response['type']
    contact_id = response['contact_id']
    uid        = str(response['gmail_uid'])
    folder     = response['folder'] or 'inbox'

    try:
        if resp_type == 'human_reply':
            result = _mark_important(mail, uid, folder)

        elif resp_type == 'auto_reply':
            result = _apply_label(mail, uid, folder, 'Auto')

        elif resp_type == 'delivery_failure':
            _delete_email(mail, uid, folder)
            if contact_id:
                conn.execute("DELETE FROM outreach WHERE contact_id=?", (contact_id,))
                conn.execute("DELETE FROM contacts WHERE id=?",         (contact_id,))
                conn.commit()
                result = "deleted from DB and Gmail"
            else:
                result = "deleted from Gmail (contact not in DB)"

        else:
            result = "unknown type"

    except Exception as e:
        result = f"error: {e}"

    conn.execute("UPDATE responses SET processed=1 WHERE id=?", (resp_id,))
    conn.commit()
    return result

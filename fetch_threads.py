"""
Fetch sent emails and their complete conversations via Gmail IMAP.

Schema
------
  conversations  — one row per thread (subject, participants, stats)
  messages       — every message in every thread, tree via parent_id

Usage
-----
    python fetch_threads.py --after 2025-10-01 --before 2026-03-01

    # skip re-fetching sent emails already in DB
    python fetch_threads.py --after 2025-10-01 --before 2026-03-01 --skip-sent

Requires .env with IMAP_HOST, IMAP_PORT, EMAIL_ADDRESS, EMAIL_PASSWORD
"""

import argparse
import base64
import email
import imaplib
import json
import os
import re
import sqlite3
import time
from datetime import date, datetime
from email.header import decode_header, make_header
from email.utils import getaddresses, parseaddr, parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_PATH    = Path("output/emails.db")
BATCH_SIZE = 150     # UIDs per header-fetch IMAP request
BATCH_SLEEP = 0.25  # seconds between batches

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def decode_str(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value or ""


def extract_addresses(header_value: str) -> list[str]:
    pairs = getaddresses([header_value or ""])
    return [a.lower().strip() for _, a in pairs if "@" in a]


def parse_dt(msg) -> datetime | None:
    try:
        return parsedate_to_datetime(msg.get("Date", ""))
    except Exception:
        return None


def imap_date(d: date) -> str:
    return d.strftime("%d-%b-%Y")


def get_plain_body(msg) -> str:
    """Return first plain-text body part, up to 1000 chars."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                charset = part.get_content_charset() or "utf-8"
                try:
                    return part.get_payload(decode=True).decode(charset, errors="replace")[:1000]
                except Exception:
                    pass
    else:
        if msg.get_content_type() == "text/plain":
            charset = msg.get_content_charset() or "utf-8"
            try:
                return msg.get_payload(decode=True).decode(charset, errors="replace")[:1000]
            except Exception:
                pass
    return ""


# ---------------------------------------------------------------------------
# Bounce / DSN parsing
# ---------------------------------------------------------------------------

BOUNCE_FROM = re.compile(
    r"mailer-daemon|postmaster|mail-daemon|delivery.*subsystem",
    re.IGNORECASE,
)
BOUNCE_SUBJ = re.compile(
    r"undeliverable|delivery\s*(status|fail)|returned\s*mail|"
    r"bounce|not\s*delivered|rejected|failure\s*notice",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE)


def classify_message(msg, our_address: str) -> str:
    """Return one of: sent | reply | bounce | auto_reply"""
    sender = parseaddr(msg.get("From", ""))[1].lower()
    subject = decode_str(msg.get("Subject", ""))

    if sender == our_address.lower():
        return "sent"
    if BOUNCE_FROM.search(sender) or BOUNCE_SUBJ.search(subject):
        return "bounce"
    auto = msg.get("Auto-Submitted", "") or msg.get("X-Auto-Response-Suppress", "")
    if auto:
        return "auto_reply"
    return "reply"


def parse_dsn_failed_address(msg) -> str:
    """
    Extract the failed recipient from a DSN (RFC 3464) message.
    Looks inside message/delivery-status MIME parts for Final-Recipient.
    Falls back to regex scan of the plain-text body.
    """
    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "message/delivery-status":
            raw = part.get_payload(decode=False)
            if isinstance(raw, list):
                raw = b"\n".join(p.as_bytes() for p in raw)
            elif isinstance(raw, str):
                raw = raw.encode()
            text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
            for line in text.splitlines():
                if line.lower().startswith("final-recipient"):
                    addrs = EMAIL_RE.findall(line)
                    if addrs:
                        return addrs[0].lower()
        if ct == "text/plain":
            charset = part.get_content_charset() or "utf-8"
            try:
                body = part.get_payload(decode=True).decode(charset, errors="replace")
            except Exception:
                continue
            # Common DSN patterns
            for pattern in [
                r"(?:The following address|address)\s*(?:had permanent|failed)[^:]*:\s*([\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,})",
                r"(?:could not be delivered to|delivery to)\s*[:<]?\s*([\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,})",
                r"(?:failed recipient|recipient address):\s*([\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,})",
            ]:
                m = re.search(pattern, body, re.IGNORECASE)
                if m:
                    return m.group(1).lower()
    return ""


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS conversations (
            thread_id       TEXT PRIMARY KEY,
            subject         TEXT,
            first_sent_at   TEXT,
            last_activity   TEXT,
            participants    TEXT,   -- JSON array
            message_count   INTEGER DEFAULT 0,
            reply_count     INTEGER DEFAULT 0,
            bounce_count    INTEGER DEFAULT 0,
            has_bounce      INTEGER DEFAULT 0,
            bounced_address TEXT
        );

        CREATE TABLE IF NOT EXISTS messages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id   TEXT UNIQUE,
            thread_id    TEXT REFERENCES conversations(thread_id),
            parent_id    TEXT,          -- In-Reply-To → parent message_id
            date         TEXT,
            from_addr    TEXT,
            to_addrs     TEXT,          -- comma-separated
            cc_addrs     TEXT,
            subject      TEXT,
            body_snippet TEXT,
            msg_type     TEXT,          -- sent | reply | bounce | auto_reply
            folder       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_msg_thread  ON messages(thread_id);
        CREATE INDEX IF NOT EXISTS idx_msg_parent  ON messages(parent_id);
        CREATE INDEX IF NOT EXISTS idx_msg_mid     ON messages(message_id);
        CREATE INDEX IF NOT EXISTS idx_conv_thread ON conversations(thread_id);
    """)
    conn.commit()


def upsert_conversation(conn: sqlite3.Connection, thread_id: str, subject: str,
                        date_str: str, participants: list[str]):
    existing = conn.execute(
        "SELECT thread_id FROM conversations WHERE thread_id = ?", (thread_id,)
    ).fetchone()
    if not existing:
        conn.execute("""
            INSERT INTO conversations (thread_id, subject, first_sent_at, last_activity, participants)
            VALUES (?, ?, ?, ?, ?)
        """, (thread_id, subject, date_str, date_str, json.dumps(sorted(set(participants)))))


def insert_message(conn: sqlite3.Connection, row: dict):
    conn.execute("""
        INSERT OR IGNORE INTO messages
            (message_id, thread_id, parent_id, date, from_addr, to_addrs,
             cc_addrs, subject, body_snippet, msg_type, folder)
        VALUES
            (:message_id, :thread_id, :parent_id, :date, :from_addr, :to_addrs,
             :cc_addrs, :subject, :body_snippet, :msg_type, :folder)
    """, row)


def update_conversation_stats(conn: sqlite3.Connection):
    """Recompute aggregated stats for all conversations from messages table."""
    conn.executescript("""
        UPDATE conversations SET
            message_count = (
                SELECT COUNT(*) FROM messages m WHERE m.thread_id = conversations.thread_id
            ),
            reply_count = (
                SELECT COUNT(*) FROM messages m
                WHERE m.thread_id = conversations.thread_id AND m.msg_type = 'reply'
            ),
            bounce_count = (
                SELECT COUNT(*) FROM messages m
                WHERE m.thread_id = conversations.thread_id AND m.msg_type = 'bounce'
            ),
            has_bounce = (
                SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                FROM messages m
                WHERE m.thread_id = conversations.thread_id AND m.msg_type = 'bounce'
            ),
            last_activity = (
                SELECT MAX(m.date) FROM messages m WHERE m.thread_id = conversations.thread_id
            ),
            participants = (
                SELECT json_group_array(DISTINCT addr)
                FROM (
                    SELECT from_addr AS addr FROM messages m WHERE m.thread_id = conversations.thread_id
                    UNION
                    SELECT value FROM messages m,
                        json_each('["' || replace(replace(m.to_addrs, ',', '","'), ' ', '') || '"]')
                    WHERE m.thread_id = conversations.thread_id
                )
            );
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# IMAP helpers
# ---------------------------------------------------------------------------

def imap_connect(host: str, port: int, addr: str, pw: str) -> imaplib.IMAP4_SSL:
    imap = imaplib.IMAP4_SSL(host, port)
    imap.login(addr, pw)
    return imap


def select_folder(imap: imaplib.IMAP4_SSL, folder: str) -> bool:
    status, _ = imap.select(f'"{folder}"', readonly=True)
    return status == "OK"


def search_folder(imap: imaplib.IMAP4_SSL, folder: str, criteria: str) -> list[bytes]:
    if not select_folder(imap, folder):
        return []
    status, data = imap.search(None, criteria)
    if status != "OK" or not data[0]:
        return []
    return data[0].split()


def fetch_headers_batch(imap: imaplib.IMAP4_SSL, uids: list[bytes]) -> list[tuple[bytes, email.message.Message]]:
    """Fetch only threading headers for a batch. Returns (uid, parsed_msg) pairs."""
    uid_str = b",".join(uids)
    fields = "(UID BODY.PEEK[HEADER.FIELDS (MESSAGE-ID IN-REPLY-TO REFERENCES FROM TO CC SUBJECT DATE X-GM-THRID)])"
    try:
        status, data = imap.fetch(uid_str, fields)
    except (imaplib.IMAP4.abort, imaplib.IMAP4.error):
        return []
    if status != "OK" or not data:
        return []

    results = []
    for i, item in enumerate(data):
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], bytes):
            parsed = email.message_from_bytes(item[1])
            results.append((uids[min(i, len(uids)-1)], parsed))
    return results


def fetch_full_message(imap: imaplib.IMAP4_SSL, uid: bytes) -> email.message.Message | None:
    try:
        status, data = imap.fetch(uid, "(RFC822)")
    except (imaplib.IMAP4.abort, imaplib.IMAP4.error):
        return None
    if status != "OK" or not data or data[0] is None:
        return None
    return email.message_from_bytes(data[0][1])


# ---------------------------------------------------------------------------
# Phase 1 — Sent emails
# ---------------------------------------------------------------------------

def collect_sent(conn: sqlite3.Connection, imap: imaplib.IMAP4_SSL,
                 our_addr: str, after: date, before: date):
    criteria = f'(SINCE "{imap_date(after)}" BEFORE "{imap_date(before)}")'
    sent_folders = ["[Gmail]/Sent Mail", "Sent", "Sent Items", "INBOX.Sent"]

    uids = []
    for folder in sent_folders:
        uids = search_folder(imap, folder, criteria)
        if uids:
            print(f"  Found {len(uids)} sent messages in '{folder}'")
            break

    if not uids:
        print("  No sent messages found. Check folder name or date range.")
        return

    already = {r[0] for r in conn.execute("SELECT message_id FROM messages WHERE msg_type='sent'").fetchall()}

    batches = [uids[i:i+BATCH_SIZE] for i in range(0, len(uids), BATCH_SIZE)]
    for batch in tqdm(batches, desc="Sent email headers"):
        pairs = fetch_headers_batch(imap, batch)
        for uid, hdr in pairs:
            mid = hdr.get("Message-ID", "").strip()
            if mid in already:
                continue

            thread_id = hdr.get("X-GM-THRID", "").strip() or mid
            subject   = decode_str(hdr.get("Subject"))
            dt        = parse_dt(hdr)
            date_str  = dt.isoformat() if dt else ""
            recipients = extract_addresses(hdr.get("To", ""))
            from_addr  = parseaddr(hdr.get("From", ""))[1].lower()
            participants = list({from_addr} | set(recipients))

            upsert_conversation(conn, thread_id, subject, date_str, participants)
            insert_message(conn, {
                "message_id":   mid,
                "thread_id":    thread_id,
                "parent_id":    hdr.get("In-Reply-To", "").strip(),
                "date":         date_str,
                "from_addr":    from_addr,
                "to_addrs":     ",".join(recipients),
                "cc_addrs":     ",".join(extract_addresses(hdr.get("Cc", ""))),
                "subject":      subject,
                "body_snippet": "",   # filled in Phase 2 if needed
                "msg_type":     "sent",
                "folder":       "Sent",
            })

        time.sleep(BATCH_SLEEP)

    conn.commit()
    sent_count = conn.execute("SELECT COUNT(*) FROM messages WHERE msg_type='sent'").fetchone()[0]
    print(f"  {sent_count} sent messages stored.")


# ---------------------------------------------------------------------------
# Phase 2 — Replies + bounces via broad header scan (no X-GM-THRID)
# ---------------------------------------------------------------------------

def _reconnect_and_select(host, port, our_addr, password, folder):
    imap = imap_connect(host, port, our_addr, password)
    select_folder(imap, folder)
    return imap


def collect_conversations(conn: sqlite3.Connection,
                          host: str, port: int, our_addr: str, password: str,
                          after: date, before: date):
    """
    Scan All Mail headers-only for the date range.
    Keep only messages whose In-Reply-To / References reference a sent Message-ID.
    Full-fetch only those matches.
    This replaces the broken X-GM-THRID per-thread approach.
    """
    # Load all sent Message-IDs into a set for O(1) lookup
    sent_ids: set[str] = {
        r[0] for r in conn.execute(
            "SELECT message_id FROM messages WHERE msg_type='sent'"
        ).fetchall() if r[0]
    }
    already_mids: set[str] = {
        r[0] for r in conn.execute("SELECT message_id FROM messages").fetchall() if r[0]
    }
    # Map sent message_id → thread_id so we can link replies correctly
    mid_to_thread: dict[str, str] = {
        r[0]: r[1] for r in conn.execute(
            "SELECT message_id, thread_id FROM messages WHERE msg_type='sent'"
        ).fetchall() if r[0]
    }

    folder = "[Gmail]/All Mail"
    imap = imap_connect(host, port, our_addr, password)
    if not select_folder(imap, folder):
        print(f"  Could not open {folder}")
        imap.logout()
        return

    # Use today as the end so we catch replies/bounces that arrived after the
    # campaign's --before date (bounces can take days/weeks to arrive).
    today = date.today()
    criteria = f'(SINCE "{imap_date(after)}" BEFORE "{imap_date(today)}")'
    status, data = imap.search(None, criteria)
    if status != "OK" or not data[0]:
        print("  No messages found in All Mail for this date range.")
        imap.logout()
        return

    all_uids = data[0].split()
    print(f"  {len(all_uids)} messages in All Mail to header-scan ...")

    matched: list[tuple[bytes, str]] = []  # (uid, resolved_thread_id)

    batches = [all_uids[i:i+BATCH_SIZE] for i in range(0, len(all_uids), BATCH_SIZE)]
    for batch in tqdm(batches, desc="  Header scan"):
        try:
            pairs = fetch_headers_batch(imap, batch)
        except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
            imap = _reconnect_and_select(host, port, our_addr, password, folder)
            pairs = fetch_headers_batch(imap, batch)

        for uid, hdr in pairs:
            mid = hdr.get("Message-ID", "").strip()
            if not mid or mid in already_mids:
                continue

            in_reply_to = hdr.get("In-Reply-To", "").strip()
            references  = set(hdr.get("References", "").split())
            if in_reply_to:
                references.add(in_reply_to)

            # Find which sent message this references
            matched_sent_mid = next((r for r in references if r in sent_ids), None)
            if matched_sent_mid:
                thread_id = mid_to_thread.get(matched_sent_mid, matched_sent_mid)
                matched.append((uid, thread_id))
                already_mids.add(mid)

        time.sleep(BATCH_SLEEP)

    print(f"  {len(matched)} replies/bounces matched — full-fetching ...")
    new_messages = 0

    for uid, thread_id in tqdm(matched, desc="  Full fetch"):
        try:
            msg = fetch_full_message(imap, uid)
        except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
            imap = _reconnect_and_select(host, port, our_addr, password, folder)
            msg = fetch_full_message(imap, uid)

        if msg is None:
            continue

        mid        = msg.get("Message-ID", "").strip()
        msg_type   = classify_message(msg, our_addr)
        dt         = parse_dt(msg)
        from_addr  = parseaddr(msg.get("From", ""))[1].lower()

        bounced_addr = ""
        if msg_type == "bounce":
            bounced_addr = parse_dsn_failed_address(msg)
            if bounced_addr:
                conn.execute(
                    "UPDATE conversations SET bounced_address=?, has_bounce=1 WHERE thread_id=?",
                    (bounced_addr, thread_id)
                )

        insert_message(conn, {
            "message_id":   mid,
            "thread_id":    thread_id,
            "parent_id":    msg.get("In-Reply-To", "").strip(),
            "date":         dt.isoformat() if dt else "",
            "from_addr":    from_addr,
            "to_addrs":     ",".join(extract_addresses(msg.get("To", ""))),
            "cc_addrs":     ",".join(extract_addresses(msg.get("Cc", ""))),
            "subject":      decode_str(msg.get("Subject")),
            "body_snippet": get_plain_body(msg)[:500],
            "msg_type":     msg_type,
            "folder":       folder,
        })
        new_messages += 1

    conn.commit()
    imap.logout()
    print(f"  Added {new_messages} new messages.")


# ---------------------------------------------------------------------------
# Phase 3 — Dedicated bounce scan across all folders
# ---------------------------------------------------------------------------

def collect_bounces(conn: sqlite3.Connection,
                    host: str, port: int, our_addr: str, password: str):
    """
    Search INBOX, Spam, and All Mail for mailer-daemon / delivery-failure
    messages that weren't threaded with our sent emails (no In-Reply-To).
    Parses DSN bodies to find the failed recipient address.
    """
    already_mids: set[str] = {
        r[0] for r in conn.execute("SELECT message_id FROM messages").fetchall() if r[0]
    }

    # IMAP OR takes exactly 2 arguments — use separate queries and merge.
    # No date restriction: bounces can arrive long after the campaign ends.
    bounce_queries = [
        'FROM "mailer-daemon"',
        'FROM "postmaster"',
        'SUBJECT "undeliverable"',
        'SUBJECT "delivery failure"',
        'SUBJECT "delivery status"',
        'SUBJECT "returned mail"',
        'SUBJECT "mail delivery"',
    ]

    folders_to_scan = ["INBOX", "[Gmail]/Spam", "[Gmail]/All Mail"]
    bounce_uids: list[tuple[bytes, str]] = []
    seen_uid_folder: set[tuple[bytes, str]] = set()

    imap = imap_connect(host, port, our_addr, password)
    for folder in folders_to_scan:
        folder_total = 0
        for query in bounce_queries:
            uids = search_folder(imap, folder, query)
            for uid in uids:
                key = (uid, folder)
                if key not in seen_uid_folder:
                    seen_uid_folder.add(key)
                    bounce_uids.append(key)
                    folder_total += 1
        print(f"  {folder}: {folder_total} potential bounce messages")

    # Deduplicate by header (same message can appear in multiple folders)
    seen_mids: set[str] = set()
    new = 0

    for uid, folder in tqdm(bounce_uids, desc="Bounce full-fetch"):
        try:
            if not select_folder(imap, folder):
                continue
            msg = fetch_full_message(imap, uid)
        except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
            imap = _reconnect_and_select(host, port, our_addr, password, folder)
            msg = fetch_full_message(imap, uid)

        if msg is None:
            continue

        mid = msg.get("Message-ID", "").strip()
        if not mid or mid in already_mids or mid in seen_mids:
            continue
        seen_mids.add(mid)

        # Only process if it really looks like a bounce
        if not (BOUNCE_FROM.search(msg.get("From", "")) or
                BOUNCE_SUBJ.search(decode_str(msg.get("Subject", "")))):
            continue

        failed_addr = parse_dsn_failed_address(msg)

        # Link to the original sent conversation via References or failed address
        irt     = msg.get("In-Reply-To", "").strip()
        ref_ids = set(msg.get("References", "").split())
        if irt:
            ref_ids.add(irt)

        matched_thread = None
        for ref in ref_ids:
            row = conn.execute(
                "SELECT thread_id FROM messages WHERE message_id=?", (ref,)
            ).fetchone()
            if row:
                matched_thread = row[0]
                break

        if not matched_thread and failed_addr:
            row = conn.execute(
                "SELECT thread_id FROM messages WHERE msg_type='sent' AND to_addrs LIKE ?",
                (f"%{failed_addr}%",)
            ).fetchone()
            if row:
                matched_thread = row[0]

        if not matched_thread:
            matched_thread = mid  # orphan bounce

        dt = parse_dt(msg)
        insert_message(conn, {
            "message_id":   mid,
            "thread_id":    matched_thread,
            "parent_id":    irt,
            "date":         dt.isoformat() if dt else "",
            "from_addr":    parseaddr(msg.get("From", ""))[1].lower(),
            "to_addrs":     ",".join(extract_addresses(msg.get("To", ""))),
            "cc_addrs":     "",
            "subject":      decode_str(msg.get("Subject")),
            "body_snippet": get_plain_body(msg)[:500],
            "msg_type":     "bounce",
            "folder":       folder,
        })
        if failed_addr:
            conn.execute(
                "UPDATE conversations SET bounced_address=?, has_bounce=1 WHERE thread_id=?",
                (failed_addr, matched_thread)
            )
        already_mids.add(mid)
        new += 1

    conn.commit()
    imap.logout()
    print(f"  {new} new bounce messages stored.")


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_csv(conn: sqlite3.Connection):
    import pandas as pd
    out = Path("output")
    out.mkdir(exist_ok=True)

    pd.read_sql("SELECT * FROM conversations ORDER BY first_sent_at", conn)\
      .to_csv(out / "conversations.csv", index=False)

    pd.read_sql("SELECT * FROM messages ORDER BY thread_id, date", conn)\
      .to_csv(out / "messages.csv", index=False)

    df_b = pd.read_sql(
        "SELECT * FROM conversations WHERE has_bounce = 1 ORDER BY first_sent_at", conn
    )
    df_b.to_csv(out / "bounced_conversations.csv", index=False)

    addrs = df_b["bounced_address"].dropna().str.strip()
    addrs = addrs[addrs != ""].unique()
    (out / "bounced_addresses.txt").write_text("\n".join(sorted(addrs)))

    counts = conn.execute("""
        SELECT msg_type, COUNT(*) FROM messages GROUP BY msg_type
    """).fetchall()

    print("\nExported:")
    print(f"  conversations.csv         : {conn.execute('SELECT COUNT(*) FROM conversations').fetchone()[0]} rows")
    print(f"  messages.csv              : {conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]} rows")
    print(f"  bounced_conversations.csv : {len(df_b)} rows")
    print(f"  bounced_addresses.txt     : {len(addrs)} addresses")
    print("\nMessage breakdown:")
    for msg_type, count in sorted(counts):
        print(f"  {msg_type:<15}: {count}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--after",       required=True, help="YYYY-MM-DD start (inclusive)")
    parser.add_argument("--before",      required=True, help="YYYY-MM-DD end (exclusive)")
    parser.add_argument("--skip-sent",   action="store_true", help="Skip sent email fetch (already in DB)")
    parser.add_argument("--skip-convos", action="store_true", help="Skip conversation fetch")
    parser.add_argument("--skip-bounces",action="store_true", help="Skip dedicated bounce scan")
    args = parser.parse_args()

    after  = date.fromisoformat(args.after)
    before = date.fromisoformat(args.before)

    host     = os.getenv("IMAP_HOST", "imap.gmail.com")
    port     = int(os.getenv("IMAP_PORT", "993"))
    our_addr = os.getenv("EMAIL_ADDRESS", "")
    password = os.getenv("EMAIL_PASSWORD", "")

    if not our_addr or not password:
        print("ERROR: Set EMAIL_ADDRESS and EMAIL_PASSWORD in .env")
        return

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # --- Phase 1: Sent emails ---
    if args.skip_sent:
        n = conn.execute("SELECT COUNT(*) FROM messages WHERE msg_type='sent'").fetchone()[0]
        print(f"\nPhase 1 skipped — {n} sent messages already in DB.")
    else:
        print(f"\nPhase 1 — Fetching sent emails ({after} → {before}) ...")
        imap = imap_connect(host, port, our_addr, password)
        collect_sent(conn, imap, our_addr, after, before)
        imap.logout()

    # --- Phase 2: Full conversations ---
    if not args.skip_convos:
        n = conn.execute("SELECT COUNT(DISTINCT thread_id) FROM conversations").fetchone()[0]
        print(f"\nPhase 2 — Fetching full conversations for {n} threads ...")
        collect_conversations(conn, host, port, our_addr, password, after, before)
    else:
        print("\nPhase 2 skipped.")

    # --- Phase 3: Dedicated bounce scan ---
    if not args.skip_bounces:
        print("\nPhase 3 — Scanning INBOX for bounce messages ...")
        collect_bounces(conn, host, port, our_addr, password)
    else:
        print("\nPhase 3 skipped.")

    # --- Stats & export ---
    print("\nUpdating conversation stats ...")
    update_conversation_stats(conn)

    print("Exporting CSVs ...")
    export_csv(conn)
    conn.close()

    print(f"\nDone. Database: {DB_PATH}")


if __name__ == "__main__":
    main()

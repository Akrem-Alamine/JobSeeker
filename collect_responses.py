"""
collect_responses.py — Collect replies to outreach emails from Gmail.

Searches Inbox and Trash for emails FROM any address we sent to,
stores them in the `responses` table, and prints a summary.

Usage:
  python3 collect_responses.py
"""

import email as emaillib
import getpass
import imaplib
import sqlite3
from datetime import datetime, timezone
from email.header import decode_header
from pathlib import Path

DB_PATH   = Path("output/leads.db")
IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

FOLDERS_TO_SCAN = [
    ("INBOX", "inbox"),
]


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_responses_table(conn):
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
            UNIQUE(from_email, gmail_uid),
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        )
    """)
    conn.commit()


def get_sent_addresses(conn) -> dict[str, int]:
    """Return {email: contact_id} for all sent outreach."""
    rows = conn.execute(
        "SELECT contact_id, email FROM outreach WHERE status = 'sent'"
    ).fetchall()
    return {r["email"].lower(): r["contact_id"] for r in rows}


def decode_str(value) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    result = ""
    for part, charset in parts:
        if isinstance(part, bytes):
            result += part.decode(charset or "utf-8", errors="replace")
        else:
            result += part
    return result.strip()


def extract_body(msg) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                charset = part.get_content_charset() or "utf-8"
                body += part.get_payload(decode=True).decode(charset, errors="replace")
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode(charset, errors="replace")
    return body.strip()


def scan_folder(mail, folder_imap: str, folder_label: str,
                sent_addresses: dict, conn, since_date: str) -> int:
    status, _ = mail.select(folder_imap, readonly=True)
    if status != "OK":
        return 0

    _, data = mail.uid("search", None, f'SINCE "{since_date}"')
    uids = data[0].split() if data[0] else []
    print(f"  {folder_label}: {len(uids)} emails found since {since_date}")
    if not uids:
        return 0

    saved = 0
    for i, uid in enumerate(uids, 1):
        print(f"  {folder_label}: checking [{i}/{len(uids)}]…", end="\r", flush=True)

        _, data = mail.uid("fetch", uid, "(RFC822)")
        if not data or not data[0]:
            continue

        raw = data[0][1]
        msg = emaillib.message_from_bytes(raw)

        from_raw   = decode_str(msg.get("From", ""))
        subject    = decode_str(msg.get("Subject", ""))
        date_raw   = msg.get("Date", "")

        # Extract just the email address from "Name <email>"
        from_email = ""
        from_name  = from_raw
        if "<" in from_raw and ">" in from_raw:
            from_email = from_raw.split("<")[-1].rstrip(">").strip().lower()
            from_name  = from_raw.split("<")[0].strip().strip('"')
        else:
            from_email = from_raw.lower().strip()

        if from_email not in sent_addresses:
            print(f"    no match: {from_email}")
            continue

        contact_id = sent_addresses[from_email]
        body       = extract_body(msg)
        uid_str    = uid.decode() if isinstance(uid, bytes) else str(uid)

        try:
            conn.execute("""
                INSERT OR IGNORE INTO responses
                    (contact_id, from_email, from_name, subject, body, received_at, folder, gmail_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (contact_id, from_email, from_name, subject, body,
                  date_raw, folder_label, uid_str))
            if conn.total_changes:
                saved += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    print(f"  {folder_label}: {saved} new response(s) saved.           ")
    return saved


def show_summary(conn):
    rows = conn.execute("""
        SELECT r.from_email, r.from_name, r.subject, r.received_at, r.folder,
               c.company
        FROM responses r
        LEFT JOIN contacts c ON c.id = r.contact_id
        ORDER BY r.received_at DESC
    """).fetchall()

    total = len(rows)
    print(f"\n{'─'*70}")
    print(f"  Responses collected — {total} total")
    print(f"{'─'*70}")
    if not rows:
        print("  No responses yet.")
    else:
        for r in rows:
            print(f"  [{r['received_at'][:16]}] {r['folder']:<6}  "
                  f"{r['from_email']:<38}  {r['subject'][:40]}")
    print(f"{'─'*70}\n")


def main():
    conn = get_conn()
    init_responses_table(conn)

    sent_addresses = get_sent_addresses(conn)
    if not sent_addresses:
        print("No sent emails found in DB.")
        conn.close()
        return

    print(f"Loaded {len(sent_addresses)} sent email addresses to match against.\n")

    gmail_user = input("Gmail address : ").strip()
    gmail_pass = getpass.getpass("App password  : ")

    print(f"\nConnecting to {IMAP_HOST}…")
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(gmail_user, gmail_pass)
    except imaplib.IMAP4.error as e:
        print(f"  Login failed: {e}")
        conn.close()
        return

    since_date = "27-May-2026"
    total_saved = 0

    # Detect trash/bin folder name
    _, folder_list = mail.list()
    trash_folder = None
    for f in folder_list:
        name = f.decode() if isinstance(f, bytes) else f
        for kw in ["Trash", "Bin", "Deleted", "Corbeille", "Papierkorb"]:
            if kw.lower() in name.lower():
                parts = name.split('"')
                folder_name = parts[-2] if len(parts) >= 2 else name.split()[-1]
                trash_folder = (f'"{folder_name}"', "trash")
                break
        if trash_folder:
            break

    folders = list(FOLDERS_TO_SCAN)
    if trash_folder:
        folders.append(trash_folder)
        print(f"  Detected trash folder: {trash_folder[0]}")
    else:
        print("  Warning: could not detect trash folder.")

    for folder_imap, folder_label in folders:
        saved = scan_folder(mail, folder_imap, folder_label,
                            sent_addresses, conn, since_date)
        total_saved += saved

    mail.logout()
    conn.close()

    print(f"\nDone — {total_saved} new response(s) collected.\n")

    # Re-open to show summary
    conn = get_conn()
    show_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()

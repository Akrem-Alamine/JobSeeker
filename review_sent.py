"""
review_sent.py — Review sent emails and reset bad-letter sends.

Usage:
  python3 review_sent.py                        # show full summary
  python3 review_sent.py --reset                # delete bad-letter DB records → resend next batch
  python3 review_sent.py --clean-gmail          # remove bad-letter emails from Gmail Sent folder
  python3 review_sent.py --reset --clean-gmail  # do both
"""

import argparse
import imaplib
import sqlite3
import getpass
from pathlib import Path

DB_PATH      = Path("output/leads.db")
BAD_MARKER   = "[Letter generation failed"
IMAP_HOST    = "imap.gmail.com"
IMAP_PORT    = 993
SENT_FOLDER  = '"[Gmail]/Sent Mail"'
TRASH_FOLDER = '"[Gmail]/Trash"'


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_bad_emails() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT email FROM outreach WHERE letter LIKE ?", (f"%{BAD_MARKER}%",)
    ).fetchall()
    conn.close()
    return [r["email"] for r in rows]


def show():
    conn = get_conn()

    total   = conn.execute("SELECT COUNT(*) FROM outreach").fetchone()[0]
    sent_ok = conn.execute(
        "SELECT COUNT(*) FROM outreach WHERE status='sent' AND (letter IS NULL OR letter NOT LIKE ?)",
        (f"%{BAD_MARKER}%",)
    ).fetchone()[0]
    failed  = conn.execute("SELECT COUNT(*) FROM outreach WHERE status='failed'").fetchone()[0]
    bad     = conn.execute("SELECT COUNT(*) FROM outreach WHERE letter LIKE ?", (f"%{BAD_MARKER}%",)).fetchone()[0]

    print(f"\n{'─'*62}")
    print(f"  Outreach — all time")
    print(f"{'─'*62}")
    print(f"  Total records       : {total}")
    print(f"  Sent OK             : {sent_ok}")
    print(f"  Bad letter (sent)   : {bad}  ← sent with generation error")
    print(f"  Failed (SMTP)       : {failed}")
    print(f"{'─'*62}\n")

    if bad:
        rows = conn.execute("""
            SELECT email, company, sent_at FROM outreach
            WHERE letter LIKE ?
            ORDER BY sent_at DESC
        """, (f"%{BAD_MARKER}%",)).fetchall()

        print(f"  Bad-letter emails ({bad} total):\n")
        for r in rows:
            print(f"    [{r['sent_at'][:19]}]  {r['email']:<40}  {r['company'] or ''}")
        print()
        print(f"  --reset        delete these {bad} DB records so they resend next batch")
        print(f"  --clean-gmail  remove them from your Gmail Sent folder")
    else:
        print("  No bad-letter emails found.")

    print()
    conn.close()


def reset():
    conn  = get_conn()
    count = conn.execute(
        "SELECT COUNT(*) FROM outreach WHERE letter LIKE ?", (f"%{BAD_MARKER}%",)
    ).fetchone()[0]

    if count == 0:
        print("Nothing to reset in DB.")
        conn.close()
        return

    print(f"\nAbout to delete {count} bad-letter outreach record(s) from DB.")
    print("These contacts will be picked up again in the next batch.\n")
    confirm = input("Type 'yes' to confirm: ").strip().lower()
    if confirm != "yes":
        print("Aborted.")
        conn.close()
        return

    conn.execute("DELETE FROM outreach WHERE letter LIKE ?", (f"%{BAD_MARKER}%",))
    conn.commit()
    conn.close()
    print(f"  Done — {count} records deleted.")


EXACT_MARKER = "[Letter generation failed — check LLM backend]"


def _get_body(mail, uid) -> str:
    """Fetch and decode the plain-text body of a message."""
    import email as emaillib
    _, data = mail.uid("fetch", uid, "(RFC822)")
    if not data or not data[0]:
        return ""
    raw = data[0][1]
    msg = emaillib.message_from_bytes(raw)
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
        body = msg.get_payload(decode=True).decode(charset, errors="replace")
    return body


def clean_gmail():
    print(f"\nScanning Gmail Sent folder — fetching each email to find exact bad-letter text.")
    gmail_user = input("Gmail address : ").strip()
    gmail_pass = getpass.getpass("App password  : ")

    print(f"\nConnecting to {IMAP_HOST}…")
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(gmail_user, gmail_pass)
    except imaplib.IMAP4.error as e:
        print(f"  Login failed: {e}")
        return

    status, _ = mail.select(SENT_FOLDER)
    if status != "OK":
        print(f"  Could not open Sent folder.")
        mail.logout()
        return

    # Limit scan to emails sent since May 29 (when the bad batch was sent)
    _, data = mail.uid("search", None, 'SINCE "29-May-2026"')
    uids = data[0].split() if data[0] else []

    if not uids:
        print("  No emails found in Sent since 29-May-2026.")
        mail.close()
        mail.logout()
        return

    print(f"  Scanning {len(uids)} emails from Sent since 29-May-2026…")
    bad_uids = []
    for i, uid in enumerate(uids, 1):
        print(f"  Checking [{i}/{len(uids)}]…", end="\r", flush=True)
        body = _get_body(mail, uid)
        if EXACT_MARKER in body:
            bad_uids.append(uid)

    print()
    if not bad_uids:
        print("  No emails with the bad-letter marker found.")
        mail.close()
        mail.logout()
        return

    print(f"  Found {len(bad_uids)} bad email(s). Moving to Trash…")
    for uid in bad_uids:
        mail.uid("copy", uid, TRASH_FOLDER)
        mail.uid("store", uid, "+FLAGS", "\\Deleted")

    mail.expunge()
    mail.close()
    mail.logout()

    print(f"  Done — {len(bad_uids)} email(s) moved to Trash.")


def restore_gmail():
    print(f"\nRestoring emails to Sent by labelling them in All Mail.")
    gmail_user = input("Gmail address : ").strip()
    gmail_pass = getpass.getpass("App password  : ")

    print(f"\nConnecting to {IMAP_HOST}…")
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(gmail_user, gmail_pass)
    except imaplib.IMAP4.error as e:
        print(f"  Login failed: {e}")
        return

    # Open All Mail — every Gmail message lives here regardless of labels
    status, _ = mail.select('"[Gmail]/All Mail"')
    if status != "OK":
        print("  Could not open [Gmail]/All Mail.")
        mail.logout()
        return

    # Find all sent emails from this address in the past 3 days
    _, data = mail.uid("search", None, 'FROM "{}" SINCE "28-May-2026"'.format(gmail_user))
    uids = data[0].split() if data[0] else []

    if not uids:
        print(f"  No emails from {gmail_user} found in All Mail since 28-May-2026.")
        mail.close()
        mail.logout()
        return

    print(f"  Found {len(uids)} email(s) in All Mail. Adding Sent label…")
    # Apply the \\Sent label via Gmail IMAP extension
    uid_list = b",".join(uids)
    mail.uid("store", uid_list, "+X-GM-LABELS", "\\Sent")

    mail.close()
    mail.logout()

    print(f"  Done — {len(uids)} email(s) labelled as Sent. Refresh your Gmail to see them.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset",         action="store_true", help="Delete bad-letter DB records")
    parser.add_argument("--clean-gmail",   action="store_true", help="Remove bad-letter emails from Gmail Sent folder")
    parser.add_argument("--restore-gmail", action="store_true", help="Restore bad-letter emails from Trash back to Sent")
    args = parser.parse_args()

    show()
    if args.reset:
        reset()
    if args.clean_gmail:
        clean_gmail()
    if args.restore_gmail:
        restore_gmail()

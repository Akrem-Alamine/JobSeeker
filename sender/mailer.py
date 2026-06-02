import json
import smtplib
import sqlite3
import threading
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "output" / "leads.db"

_progress: dict = {"total": 0, "done": 0, "success": 0, "failed": 0, "running": False, "error": ""}
_lock = threading.Lock()


def get_progress() -> dict:
    with _lock:
        return dict(_progress)


def render(template: str, contact: dict, config: dict) -> str:
    greeting = (contact.get("first_name") or "").strip()
    if not greeting:
        greeting = f"{(contact.get('company') or 'your company').strip()} team"

    company = (contact.get("company") or "your company").strip()

    return (
        template
        .replace("{{greeting}}",              greeting)
        .replace("{{first_name}}",            contact.get("first_name") or greeting)
        .replace("{{company}}",               company)
        .replace("{{your_name}}",             config.get("your_name", ""))
        .replace("{{your_email}}",            config.get("your_email", ""))
        .replace("{{your_university}}",       config.get("your_university", ""))
        .replace("{{your_graduation}}",       config.get("your_graduation", ""))
        .replace("{{your_internship_company}}", config.get("your_internship_company", ""))
    )


def _fetch_batch(size: int) -> list[dict]:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"""
        SELECT id, email, first_name, company
        FROM contacts
        WHERE email NOT LIKE '_noemail_%'
          AND email_status != 'undeliverable'
          AND (outreach_status IS NULL OR outreach_status NOT IN ('sent', 'sending'))
        ORDER BY
            CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 0 ELSE 1 END,
            id
        LIMIT {int(size)}
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _mark(contact_id: int, status: str):
    conn = sqlite3.connect(str(DB_PATH))
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE contacts SET outreach_status=?, outreach_sent_at=?, updated_at=? WHERE id=?",
        (status, now, now, contact_id),
    )
    conn.commit()
    conn.close()


def start_batch(config: dict):
    with _lock:
        if _progress.get("running"):
            return
        _progress.update({"total": 0, "done": 0, "success": 0, "failed": 0, "running": True, "error": ""})

    def _run():
        batch = _fetch_batch(config.get("batch_size", 1500))

        with _lock:
            _progress["total"] = len(batch)

        if not batch:
            with _lock:
                _progress.update({"running": False, "error": "No contacts remaining."})
            return

        # Mark all as 'sending' so a crash doesn't re-send them
        conn = sqlite3.connect(str(DB_PATH))
        conn.executemany(
            "UPDATE contacts SET outreach_status='sending' WHERE id=?",
            [(r["id"],) for r in batch],
        )
        conn.commit()
        conn.close()

        try:
            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
            server.starttls()
            server.login(config["your_email"], config["gmail_app_password"])
        except Exception as e:
            # Roll back 'sending' status
            conn = sqlite3.connect(str(DB_PATH))
            conn.executemany(
                "UPDATE contacts SET outreach_status=NULL WHERE id=? AND outreach_status='sending'",
                [(r["id"],) for r in batch],
            )
            conn.commit()
            conn.close()
            with _lock:
                _progress.update({"running": False, "error": f"Gmail login failed: {e}"})
            return

        subject_tpl = config["template_subject"]
        body_tpl    = config["template_body"]

        for contact in batch:
            try:
                subject = render(subject_tpl, contact, config)
                body    = render(body_tpl,    contact, config)

                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"]    = f"{config['your_name']} <{config['your_email']}>"
                msg["To"]      = contact["email"]
                msg.attach(MIMEText(body, "plain", "utf-8"))

                server.sendmail(config["your_email"], contact["email"], msg.as_string())
                _mark(contact["id"], "sent")

                with _lock:
                    _progress["success"] += 1
            except Exception:
                _mark(contact["id"], "failed")
                with _lock:
                    _progress["failed"] += 1

            with _lock:
                _progress["done"] += 1

            time.sleep(0.5)  # ~2 emails/sec to stay under Gmail limits

        try:
            server.quit()
        except Exception:
            pass

        with _lock:
            _progress["running"] = False

    threading.Thread(target=_run, daemon=True).start()

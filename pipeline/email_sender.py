"""
Send emails via Gmail SMTP with CV attached.
"""

import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def _build_message(sender_email: str, recipient_email: str, candidate_name: str,
                   letter_body: str, cv_path: str,
                   reply_to: str = "") -> MIMEMultipart:
    subject = f"Speculative Application - {candidate_name}"
    msg = MIMEMultipart()
    msg["From"]    = f"{candidate_name} <{sender_email}>"
    msg["To"]      = recipient_email
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.attach(MIMEText(letter_body, "plain", "utf-8"))
    cv_file = Path(cv_path)
    if cv_file.exists():
        with open(cv_file, "rb") as f:
            part = MIMEApplication(f.read(), Name=cv_file.name)
        part["Content-Disposition"] = f'attachment; filename="{cv_file.name}"'
        msg.attach(part)
    return msg


def open_smtp(sender_email: str, app_password: str) -> smtplib.SMTP:
    """Open and return a persistent authenticated SMTP session."""
    server = smtplib.SMTP("smtp.gmail.com", 587, timeout=60)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(sender_email, app_password)
    return server


def send_with_server(
    server: smtplib.SMTP,
    sender_email: str,
    app_password: str,
    recipient_email: str,
    candidate_name: str,
    letter_body: str,
    cv_path: str,
    reply_to: str = "",
) -> tuple[bool, str, smtplib.SMTP]:
    """
    Send one email on an existing SMTP session.
    Reconnects up to 3 times on any connection error.
    Returns (success, error, server) — server may be a new instance after reconnect.
    """
    import time

    def _is_gmail_limit(code, raw_msg):
        text = raw_msg.decode(errors='replace') if isinstance(raw_msg, bytes) else str(raw_msg)
        return code == 550 and '5.4.5' in text

    email_msg = _build_message(sender_email, recipient_email, candidate_name, letter_body, cv_path, reply_to)
    for attempt in range(3):
        try:
            server.sendmail(sender_email, recipient_email, email_msg.as_string())
            return True, "", server
        except smtplib.SMTPRecipientsRefused as e:
            for addr, (code, raw) in e.recipients.items():
                if _is_gmail_limit(code, raw):
                    return False, 'DAILY_LIMIT_EXCEEDED', server
            return False, f"Recipient refused: {recipient_email}", server
        except smtplib.SMTPSenderRefused as e:
            if _is_gmail_limit(e.smtp_code, e.smtp_error):
                return False, 'DAILY_LIMIT_EXCEEDED', server
            return False, f"Sender refused: {e.smtp_error}", server
        except smtplib.SMTPDataError as e:
            if _is_gmail_limit(e.smtp_code, e.smtp_error):
                return False, 'DAILY_LIMIT_EXCEEDED', server
            code = e.smtp_code
            raw  = e.smtp_error.decode(errors='replace') if isinstance(e.smtp_error, bytes) else str(e.smtp_error)
            if code in (421, 450) and attempt < 2:
                time.sleep(30 * (attempt + 1))
                try:
                    server = open_smtp(sender_email, app_password)
                    continue
                except Exception as re:
                    return False, f"Reconnect failed after rate limit: {re}", server
            return False, f"SMTP {code}: {raw}", server
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectionError, OSError):
            if attempt < 2:
                try:
                    server = open_smtp(sender_email, app_password)
                    continue
                except Exception as e:
                    return False, f"Reconnect failed: {e}", server
            return False, "Connection lost after reconnect attempts", server
        except Exception as e:
            return False, str(e), server
    return False, "Send failed", server


def send_application(
    sender_email: str,
    app_password: str,
    recipient_email: str,
    recipient_name: str,
    company_name: str,
    candidate_name: str,
    letter_body: str,
    cv_path: str,
    reply_to: str = "",
) -> tuple[bool, str]:
    """Send one email opening its own connection — for single sends only."""
    msg = _build_message(sender_email, recipient_email, candidate_name, letter_body, cv_path, reply_to)
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=60) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender_email, app_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        return True, ""
    except smtplib.SMTPAuthenticationError:
        return False, "Gmail authentication failed — check your app password"
    except smtplib.SMTPRecipientsRefused:
        return False, f"Recipient refused: {recipient_email}"
    except Exception as e:
        return False, str(e)


def test_connection(sender_email: str, app_password: str) -> tuple[bool, str]:
    """Test Gmail credentials without sending."""
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(sender_email, app_password)
        return True, ""
    except smtplib.SMTPAuthenticationError:
        return False, "Authentication failed — check your Gmail app password"
    except Exception as e:
        return False, str(e)

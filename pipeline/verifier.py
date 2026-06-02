"""
Email verification — DNS MX + optional SMTP.
Shared by all pipeline sources.
"""

import re
import smtplib
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import dns.resolver
import dns.exception

EMAIL_RE = re.compile(r'^[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}$', re.IGNORECASE)
ROLE_PREFIXES = {
    "info", "contact", "admin", "support", "hello", "help", "sales",
    "marketing", "noreply", "no-reply", "team", "office", "mail",
    "enquiries", "hr", "jobs", "careers", "press", "media",
}
DISPOSABLE = {
    "mailinator.com", "guerrillamail.com", "tempmail.com", "yopmail.com",
    "trashmail.com", "fakeinbox.com", "maildrop.cc", "sharklasers.com",
}

resolver = dns.resolver.Resolver()
resolver.timeout  = 5
resolver.lifetime = 5
_mx_cache: dict[str, list[str]] = {}


def _get_mx(domain: str) -> list[str]:
    if domain in _mx_cache:
        return _mx_cache[domain]
    try:
        records = sorted(resolver.resolve(domain, "MX"), key=lambda r: r.preference)
        result  = [str(r.exchange).rstrip(".") for r in records]
    except Exception:
        result = []
    _mx_cache[domain] = result
    return result


def _smtp(email: str, mx: list[str], timeout: int = 8) -> str:
    for host in mx[:2]:
        try:
            with smtplib.SMTP(timeout=timeout) as s:
                s.connect(host, 25)
                s.ehlo_or_helo_if_needed()
                s.mail("verify@check.local")
                code, _ = s.rcpt(email)
                s.quit()
                if code == 250:
                    return "deliverable"
                if code in (550, 551, 553, 554):
                    return "undeliverable"
                return "unknown"
        except Exception:
            continue
    return "unknown"


def verify(email: str, smtp: bool = False) -> dict:
    email = email.strip().lower()
    if not EMAIL_RE.match(email):
        return {"email": email, "status": "undeliverable", "reason": "invalid_syntax"}

    local, domain = email.rsplit("@", 1)

    if domain in DISPOSABLE:
        return {"email": email, "status": "risky", "reason": "disposable"}
    if local.split("+")[0] in ROLE_PREFIXES:
        return {"email": email, "status": "risky", "reason": "role_address"}

    mx = _get_mx(domain)
    if not mx:
        return {"email": email, "status": "undeliverable", "reason": "no_mx"}

    if smtp:
        status = _smtp(email, mx)
        return {"email": email, "status": status, "reason": "smtp_check"}

    return {"email": email, "status": "deliverable", "reason": "mx_found"}


def verify_batch(emails: list[str], workers: int = 30, smtp: bool = False) -> list[dict]:
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(verify, e, smtp): e for e in emails}
        for f in as_completed(futs):
            results.append(f.result())
    return results

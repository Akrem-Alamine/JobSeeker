"""
Given a person's name and company domain, guess and verify their email address.
"""

import re
import socket
import smtplib
import time
from itertools import product

import dns.resolver
import dns.exception

PATTERNS = [
    "{f}.{l}",
    "{f}{l}",
    "{f}",
    "{l}",
    "{fi}{l}",
    "{f}.{li}",
    "{f}-{l}",
    "{fi}.{l}",
]

resolver = dns.resolver.Resolver()
resolver.timeout  = 4
resolver.lifetime = 4
_mx_cache: dict[str, list[str]] = {}


def _clean(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower().strip())


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


def _smtp_check(email: str, mx_hosts: list[str], timeout: int = 8) -> bool:
    for mx in mx_hosts[:2]:
        try:
            with smtplib.SMTP(timeout=timeout) as s:
                s.connect(mx, 25)
                s.ehlo_or_helo_if_needed()
                s.mail("verify@check.local")
                code, _ = s.rcpt(email)
                s.quit()
                return code == 250
        except Exception:
            continue
    return False


def generate_candidates(first: str, last: str, domain: str) -> list[str]:
    f  = _clean(first)
    l  = _clean(last)
    fi = f[:1]
    li = l[:1]
    if not f or not l or not domain:
        return []
    candidates = []
    for pat in PATTERNS:
        try:
            addr = pat.format(f=f, l=l, fi=fi, li=li)
            candidates.append(f"{addr}@{domain}")
        except KeyError:
            pass
    return candidates


def find_email(first: str, last: str, domain: str, use_smtp: bool = True) -> str | None:
    """
    Try common email patterns for a person at a domain.
    Returns the first working email or None.
    """
    mx = _get_mx(domain)
    if not mx:
        return None

    candidates = generate_candidates(first, last, domain)
    for email in candidates:
        if use_smtp:
            if _smtp_check(email, mx):
                return email
            time.sleep(0.5)
        else:
            return email  # DNS-only: return first pattern
    return None

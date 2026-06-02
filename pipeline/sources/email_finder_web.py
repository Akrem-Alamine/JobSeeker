"""
Email-Finder-Web source — finds real emails for contacts stored as _noemail_
placeholders by searching DDG for publicly listed email addresses.

This is complementary to the pattern-based pipeline/email_finder.py:
instead of guessing patterns, it searches the web for the actual email
string and extracts it from public pages.

Writes directly to DB (like bs4_miner). Returns [] to skip orchestrator.
"""

import re
import sqlite3
import time
import threading

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from .base import BaseSource
from pipeline.db import DB_PATH

DDG_URL   = "https://html.duckduckgo.com/html/"
DDG_DELAY = 1.2
BATCH_LIMIT = 2000

# Domains to reject even if found in search results
BLACKLISTED_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "hunter.io", "apollo.io", "clearbit.com", "zoominfo.com", "lusha.com",
    "rocketreach.co", "snov.io", "leadleaper.com", "contactout.com",
    "noemail.placeholder", "example.com", "test.com",
}

# Generic local parts to reject
GENERIC_LOCALS = {
    "info", "contact", "hello", "support", "admin", "team", "sales",
    "hr", "marketing", "help", "noreply", "no-reply", "mail",
    "office", "enquiries", "enquiry", "general",
}

EMAIL_RE = re.compile(r'[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}')

DDG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_ddg_lock     = threading.Lock()
_ddg_last_req = [0.0]


def _ddg_search(query: str) -> str:
    """Run one DDG HTML search, return aggregated text from snippets."""
    with _ddg_lock:
        gap = DDG_DELAY - (time.time() - _ddg_last_req[0])
        if gap > 0:
            time.sleep(gap)
        _ddg_last_req[0] = time.time()
    try:
        resp = requests.post(
            DDG_URL,
            data={"q": query, "kl": "en-us"},
            headers=DDG_HEADERS,
            timeout=12,
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        return " ".join(
            el.get_text(" ", strip=True)
            for el in soup.select(".result__snippet")[:8]
        )
    except Exception:
        return ""


def _extract_emails(text: str, domain: str) -> list[str]:
    """Extract emails from text, keeping only those matching the target domain."""
    found = []
    for m in EMAIL_RE.finditer(text):
        email = m.group().lower().strip(".,;)")
        parts = email.split("@")
        if len(parts) != 2:
            continue
        local, edom = parts
        if edom != domain:
            continue
        if edom in BLACKLISTED_DOMAINS:
            continue
        if local in GENERIC_LOCALS:
            continue
        found.append(email)
    return list(dict.fromkeys(found))   # deduplicate, preserve order


def _find_email_for(first: str, last: str, domain: str, company: str) -> str | None:
    """Search DDG for a real email for this person. Returns email or None."""
    name = f"{first} {last}".strip()
    if not name or not domain:
        return None

    queries = [
        f'"{name}" "@{domain}"',
        f'"{name}" {company} email',
        f'"{name}" {domain} contact',
    ]

    for q in queries:
        text   = _ddg_search(q)
        emails = _extract_emails(text, domain)
        if emails:
            return emails[0]

    return None


class EmailFinderWebSource(BaseSource):
    name         = "email_finder_web"
    requires_key = False

    def fetch(self) -> list[dict]:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT id, first_name, last_name, company, company_domain
            FROM contacts
            WHERE email LIKE '_noemail_%'
              AND first_name IS NOT NULL AND first_name != ''
              AND company_domain IS NOT NULL AND company_domain != ''
            ORDER BY id
            LIMIT ?
        """, (BATCH_LIMIT,)).fetchall()
        conn.close()

        total = len(rows)
        if total == 0:
            print("  [email_finder_web] No noemail contacts to process.")
            return []

        print(f"  [email_finder_web] {total} contacts to search for emails")

        found = 0
        for r in tqdm(rows, unit="contact", ncols=80):
            email = _find_email_for(
                r["first_name"] or "",
                r["last_name"]  or "",
                r["company_domain"],
                r["company"] or r["company_domain"],
            )
            if not email:
                continue

            # Check if this email already exists to avoid UNIQUE constraint errors
            conn = sqlite3.connect(str(DB_PATH))
            existing = conn.execute(
                "SELECT id FROM contacts WHERE email=?", (email,)
            ).fetchone()
            if existing:
                conn.close()
                continue

            conn.execute(
                "UPDATE contacts SET email=?, email_status='unverified' WHERE id=?",
                (email, r["id"]),
            )
            conn.commit()
            conn.close()
            found += 1

        print(f"  [email_finder_web] Found and updated {found}/{total} emails")
        return []   # writes directly to DB; nothing for orchestrator to process

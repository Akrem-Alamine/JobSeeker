"""
DuckDuckGo Enrich Source — queries DDG Instant API for executive info
on companies that have 0 contacts scraped so far. Runs sequentially
(rate-limited to 1 req/1.5s) after the main scrape step.
"""

import re
import time

import requests

from .base import BaseSource

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

DDG_ROLE_MAP = {
    "ceo": "CEO", "chief executive": "CEO",
    "founder": "Founder", "co-founder": "Co-Founder",
    "cto": "CTO", "chief technology": "CTO",
    "coo": "COO", "chief operating": "COO",
    "cfo": "CFO", "chief financial": "CFO",
    "president": "President", "chairman": "Chairman",
}

FAKE_NAMES = {
    "executive team", "leadership", "founders", "meet our team", "our team",
    "board of directors", "management team", "team", "our leadership",
    "the team", "about us", "advisory board",
}

NAME_BAD_WORDS = {
    "team", "meet", "our", "board", "executive", "leadership",
    "management", "founders", "staff", "about", "directors",
    "investors", "advisor", "advisory", "committee",
}


def _is_valid_name(name: str) -> bool:
    name = name.strip()
    if not name or len(name) < 4 or len(name) > 50:
        return False
    if name.lower() in FAKE_NAMES:
        return False
    words = name.split()
    if len(words) < 2 or len(words) > 5:
        return False
    if any(c.isdigit() for c in name):
        return False
    if not words[0][0].isupper():
        return False
    if any(w.lower() in NAME_BAD_WORDS for w in words):
        return False
    if not all(re.match(r"^[A-Za-z'\-\.]+$", w) for w in words):
        return False
    return True


def _ddg_query(company_name: str, domain: str) -> list[dict]:
    people = []
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": company_name, "format": "json",
                    "no_redirect": "1", "no_html": "1", "skip_disambig": "1"},
            timeout=8, headers=HEADERS,
        )
        data = r.json()

        # Infobox (structured — most reliable)
        for item in data.get("Infobox", {}).get("content", []):
            label = item.get("label", "").lower()
            value = item.get("value", "").strip()
            if not value:
                continue
            role = next((v for k, v in DDG_ROLE_MAP.items() if k in label), None)
            if not role:
                continue
            for name in re.split(r",\s*|\s+and\s+", value):
                name = name.strip()
                if _is_valid_name(name):
                    parts = name.split(" ", 1)
                    people.append({
                        "first_name":     parts[0],
                        "last_name":      parts[1] if len(parts) > 1 else "",
                        "full_name":      name,
                        "title":          role,
                        "company":        company_name,
                        "company_domain": domain,
                        "email":          "",
                        "source":         "ddg_enrich",
                        "tags":           ["ddg"],
                    })

        # AbstractText patterns (fallback)
        if not people:
            abstract = data.get("AbstractText", "")
            for pattern in [
                r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:is|serves as|,)\s+(?:the\s+)?(?:CEO|founder|CTO)',
                r'(?:CEO|founder|CTO)\s+(?:is\s+)?([A-Z][a-z]+ [A-Z][a-z]+)',
                r'founded by ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            ]:
                for name in re.findall(pattern, abstract):
                    name = name.strip()
                    if _is_valid_name(name):
                        parts = name.split(" ", 1)
                        people.append({
                            "first_name":     parts[0],
                            "last_name":      parts[1] if len(parts) > 1 else "",
                            "full_name":      name,
                            "title":          "Executive",
                            "company":        company_name,
                            "company_domain": domain,
                            "email":          "",
                            "source":         "ddg_enrich",
                            "tags":           ["ddg"],
                        })
    except Exception:
        pass
    return people


class DDGEnrichSource(BaseSource):
    name         = "ddg_enrich"
    requires_key = False

    def fetch(self) -> list[dict]:
        from pipeline import db as DB
        import sqlite3

        conn = sqlite3.connect("output/leads.db")
        rows = conn.execute("""
            SELECT co.name, co.domain
            FROM companies co
            WHERE co.scraped = 2
            ORDER BY co.added_at
        """).fetchall()
        conn.close()

        total = len(rows)
        print(f"  [DDGEnrich] {total} exhausted companies — querying DDG Instant API")
        print(f"  [DDGEnrich] Rate: 1 req/1.5s → ~{total*1.5/60:.0f} minutes (~{total*1.5/3600:.1f}h)")

        contacts = []
        found    = 0

        for i, (name, domain) in enumerate(rows):
            people = _ddg_query(name or domain, domain)
            if people:
                contacts.extend(people)
                found += len(people)
                DB.mark_company_scraped(domain, 1)   # upgrade scraped=2 → scraped=1

            if (i + 1) % 100 == 0 or i == total - 1:
                print(f"  [DDGEnrich] {i+1}/{total} queried — {found} contacts found")
            time.sleep(1.5)

        return contacts

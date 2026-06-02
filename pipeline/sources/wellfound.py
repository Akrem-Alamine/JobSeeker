"""
Wellfound (AngelList) source — startup founders and technical leads.
Scrapes public company and people pages.
No API key required.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS
from .base import BaseSource

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

SEARCH_QUERIES = [
    'site:wellfound.com "CTO" "software" OR "tech"',
    'site:wellfound.com "CEO" "SaaS" OR "technology"',
    'site:wellfound.com "Founder" "engineering" OR "software"',
    'site:wellfound.com "VP Engineering" startup',
    'site:wellfound.com "Head of Engineering" startup',
    'site:wellfound.com "Co-Founder" "CTO" OR "technical"',
    'site:angellist.com "CTO" "software"',
    'site:angellist.com "Founder" "engineering"',
]


def _parse_wellfound_profile(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")

        name_tag  = soup.find("h1") or soup.find(class_=re.compile(r"name|heading", re.I))
        title_tag = soup.find(class_=re.compile(r"role|title|position|headline", re.I))
        co_tag    = soup.find(class_=re.compile(r"company|organization|employer", re.I))

        full_name = name_tag.get_text(strip=True) if name_tag else ""
        title     = title_tag.get_text(strip=True)[:100] if title_tag else ""
        company   = co_tag.get_text(strip=True)[:80] if co_tag else ""

        if not full_name:
            return None

        # Email from page
        email_match = re.search(r'[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}', soup.get_text())
        email       = email_match.group(0).lower() if email_match else ""

        parts = full_name.split(" ", 1)
        return {
            "first_name": parts[0],
            "last_name":  parts[1] if len(parts) > 1 else "",
            "full_name":  full_name,
            "title":      title,
            "company":    company,
            "email":      email,
            "source_url": url,
            "tags":       ["wellfound"],
        }
    except Exception:
        return None


class WellfoundSource(BaseSource):
    name         = "wellfound"
    requires_key = False

    def fetch(self) -> list[dict]:
        contacts  = []
        seen_urls: set[str] = set()

        with DDGS() as ddg:
            for query in SEARCH_QUERIES:
                print(f"  [Wellfound] Query: {query[:60]}...")
                try:
                    results = list(ddg.text(query, max_results=15))
                except Exception as e:
                    print(f"  [Wellfound] Search error: {e}")
                    time.sleep(5)
                    continue

                for result in results:
                    url = result.get("href", "") or result.get("url", "")
                    if not url or url in seen_urls:
                        continue
                    if "wellfound.com/u/" not in url and "angel.co/u/" not in url:
                        continue
                    seen_urls.add(url)

                    # Try snippet first
                    title_str = result.get("title", "")
                    snippet   = result.get("body", "")
                    person    = {"source_url": url, "tags": ["wellfound"]}

                    if " - " in title_str:
                        name_part, rest = title_str.split(" - ", 1)
                        person["full_name"]  = name_part.strip()
                        parts = name_part.strip().split(" ", 1)
                        person["first_name"] = parts[0]
                        person["last_name"]  = parts[1] if len(parts) > 1 else ""
                        rest = rest.replace("| Wellfound", "").replace("| AngelList", "").strip()
                        if " at " in rest:
                            t, c = rest.split(" at ", 1)
                            person["title"]   = t.strip()[:100]
                            person["company"] = c.strip()[:80]
                        else:
                            person["title"] = rest[:100]
                        contacts.append(person)
                    else:
                        data = _parse_wellfound_profile(url)
                        if data:
                            contacts.append(data)
                        time.sleep(1)

                    time.sleep(0.8)
                time.sleep(3)

        print(f"  [Wellfound] Total: {len(contacts)} founders found")
        return contacts

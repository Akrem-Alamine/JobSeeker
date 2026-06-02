"""
LinkedIn source — finds decision-makers via DuckDuckGo search.
Searches for LinkedIn profiles matching IT leadership roles,
then visits public profile pages to extract name/title/company.
No login required — uses publicly visible profile data only.
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
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SEARCH_QUERIES = [
    '"CTO" "technology company" linkedin.com/in',
    '"Chief Technology Officer" "software" linkedin.com/in',
    '"CEO" "SaaS" OR "software startup" linkedin.com/in',
    '"Founder" "tech startup" OR "software" linkedin.com/in',
    '"VP of Engineering" "company" linkedin.com/in',
    '"Head of Engineering" "software" linkedin.com/in',
    '"Director of Engineering" linkedin.com/in',
    '"Co-Founder CTO" OR "CTO and Co-Founder" linkedin.com/in',
    '"Chief Information Officer" "IT" linkedin.com/in',
    '"Managing Director" "technology" linkedin.com/in',
    '"Engineering Manager" "software" linkedin.com/in',
    '"DevOps Lead" OR "Platform Engineering Lead" linkedin.com/in',
    '"Head of Infrastructure" OR "Head of Platform" linkedin.com/in',
    '"VP Technology" "software company" linkedin.com/in',
    '"Principal Engineer" "software" linkedin.com/in',
]

LI_URL_RE = re.compile(r'linkedin\.com/in/[\w\-]+', re.IGNORECASE)


def _extract_profile_data(url: str) -> dict | None:
    """Visit a public LinkedIn profile and extract visible data."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")

        # Name
        name_tag = (
            soup.find("h1") or
            soup.find(class_=re.compile(r"top-card.*name|profile.*name", re.I))
        )
        full_name = name_tag.get_text(strip=True) if name_tag else ""

        # Title
        title_tag = (
            soup.find(class_=re.compile(r"top-card.*subtitle|headline|title", re.I)) or
            soup.find("h2")
        )
        title = title_tag.get_text(strip=True)[:120] if title_tag else ""

        # Company — often in title or separate field
        company = ""
        company_tag = soup.find(class_=re.compile(r"company|workplace|employer", re.I))
        if company_tag:
            company = company_tag.get_text(strip=True)[:80]
        elif " at " in title:
            parts   = title.split(" at ", 1)
            title   = parts[0].strip()
            company = parts[1].strip()

        if not full_name:
            return None

        parts = full_name.strip().split(" ", 1)
        first = parts[0]
        last  = parts[1] if len(parts) > 1 else ""

        return {
            "first_name":  first,
            "last_name":   last,
            "full_name":   full_name,
            "title":       title,
            "company":     company,
            "linkedin_url": url,
            "source_url":  url,
            "tags":        ["linkedin"],
        }
    except Exception:
        return None


class LinkedInSource(BaseSource):
    name         = "linkedin"
    requires_key = False

    def fetch(self) -> list[dict]:
        contacts = []
        seen_urls: set[str] = set()

        with DDGS() as ddg:
            for query in SEARCH_QUERIES:
                print(f"  [LinkedIn] Query: {query[:60]}...")
                try:
                    results = list(ddg.text(query, max_results=20))
                except Exception as e:
                    print(f"  [LinkedIn] Search error: {e}")
                    time.sleep(5)
                    continue

                for result in results:
                    url = result.get("href", "") or result.get("url", "")
                    # Normalize LinkedIn URL
                    m = LI_URL_RE.search(url)
                    if not m:
                        continue
                    li_url = "https://www." + m.group(0).split("?")[0]
                    if li_url in seen_urls:
                        continue
                    seen_urls.add(li_url)

                    # Try to extract info from the search snippet first
                    snippet = result.get("body", "") or result.get("description", "")
                    title   = result.get("title", "")

                    # Parse snippet: usually "Name - Title at Company | LinkedIn"
                    person = {"linkedin_url": li_url, "source_url": li_url, "tags": ["linkedin"]}
                    if " - " in title:
                        name_part, rest = title.split(" - ", 1)
                        person["full_name"] = name_part.strip()
                        parts = name_part.strip().split(" ", 1)
                        person["first_name"] = parts[0]
                        person["last_name"]  = parts[1] if len(parts) > 1 else ""

                        # Title and company from rest
                        rest = rest.replace("| LinkedIn", "").strip()
                        if " at " in rest:
                            t, c = rest.split(" at ", 1)
                            person["title"]   = t.strip()[:100]
                            person["company"] = c.strip()[:80]
                        else:
                            person["title"] = rest[:100]

                    if person.get("full_name"):
                        contacts.append(person)
                    else:
                        # Fall back to visiting the profile page
                        data = _extract_profile_data(li_url)
                        if data:
                            contacts.append(data)
                        time.sleep(1.5)

                    time.sleep(0.8)

                time.sleep(3)  # pause between search queries

        print(f"  [LinkedIn] Total: {len(contacts)} profiles found")
        return contacts

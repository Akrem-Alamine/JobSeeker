"""
Apollo.io source — B2B contact database with millions of verified contacts.
Free tier: 50 exports/month. Paid tiers unlock much more.

Requires: APOLLO_KEY in config
API docs: https://apolloio.github.io/apollo-api-docs/
"""

import time
import requests
from .base import BaseSource

TITLES_TO_SEARCH = [
    ["CTO", "Chief Technology Officer"],
    ["CEO", "Chief Executive Officer"],
    ["Founder", "Co-Founder"],
    ["VP Engineering", "Vice President Engineering"],
    ["Director of Engineering"],
    ["Head of Engineering"],
    ["CIO", "Chief Information Officer"],
    ["CPO", "Chief Product Officer"],
    ["Head of Technology"],
    ["Managing Director"],
]

TECH_INDUSTRIES = [
    "Information Technology and Services",
    "Computer Software",
    "Internet",
    "Telecommunications",
    "Computer Networking",
    "Semiconductors",
    "Computer Hardware",
    "Defense & Space",
    "Cybersecurity",
]


class ApolloSource(BaseSource):
    name         = "apollo"
    requires_key = True

    def __init__(self, config: dict):
        super().__init__(config)
        self.key  = config.get("APOLLO_KEY", "")
        self.base = "https://api.apollo.io/v1"

    def _search(self, titles: list[str], page: int = 1) -> list[dict]:
        try:
            r = requests.post(
                f"{self.base}/mixed_people/search",
                headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
                json={
                    "api_key":        self.key,
                    "person_titles":  titles,
                    "industry_tag_ids": [],
                    "page":           page,
                    "per_page":       25,
                    "prospected_by_current_team": "no",
                },
                timeout=20,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data.get("people", [])
        except Exception as e:
            print(f"  [Apollo] Error: {e}")
            return []

    def fetch(self) -> list[dict]:
        contacts = []
        seen     = set()

        for title_group in TITLES_TO_SEARCH:
            print(f"  [Apollo] Searching: {title_group[0]}")
            for page in range(1, 5):
                people = self._search(title_group, page)
                if not people:
                    break

                for p in people:
                    email = (p.get("email") or "").lower()
                    key   = email or f"{p.get('first_name','')}_{p.get('last_name','')}_{p.get('organization', {}).get('name', '')}"
                    if key in seen:
                        continue
                    seen.add(key)

                    org = p.get("organization") or p.get("employment_history", [{}])[0] if p.get("employment_history") else {}

                    contacts.append({
                        "first_name":     p.get("first_name", ""),
                        "last_name":      p.get("last_name", ""),
                        "title":          p.get("title", ""),
                        "company":        org.get("name", "") if isinstance(org, dict) else "",
                        "company_domain": p.get("organization", {}).get("primary_domain", "") if p.get("organization") else "",
                        "email":          email,
                        "email_status":   "deliverable" if email else "unverified",
                        "linkedin_url":   p.get("linkedin_url", ""),
                        "city":           p.get("city", ""),
                        "country":        p.get("country", ""),
                        "source_url":     f"https://app.apollo.io/#/people/{p.get('id', '')}",
                        "tags":           ["apollo"],
                    })

                time.sleep(2)
            time.sleep(3)

        print(f"  [Apollo] Total: {len(contacts)} contacts")
        return contacts

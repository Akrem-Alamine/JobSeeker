"""
Crunchbase source — finds founders and C-suite at tech companies.
Uses Crunchbase Basic API (free tier).

Requires: CRUNCHBASE_KEY in config
API docs: https://data.crunchbase.com/docs
"""

import time
import requests
from .base import BaseSource

TECH_CATEGORIES = [
    "software", "information-technology", "networking",
    "cybersecurity", "artificial-intelligence", "machine-learning",
    "cloud-computing", "data-analytics", "devops", "saas",
    "fintech", "enterprise-software", "mobile", "internet-of-things",
    "open-source", "developer-tools", "infrastructure",
]

TARGET_ROLES = [
    "ceo", "cto", "cio", "cpo", "coo", "founder", "co-founder",
    "vp engineering", "director of engineering", "head of engineering",
    "managing director", "president", "general manager",
]


class CrunchbaseSource(BaseSource):
    name         = "crunchbase"
    requires_key = True

    def __init__(self, config: dict):
        super().__init__(config)
        self.key  = config.get("CRUNCHBASE_KEY", "")
        self.base = "https://api.crunchbase.com/api/v4"

    def _post(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{self.base}/{endpoint}?user_key={self.key}"
        try:
            r = requests.post(url, json=payload, timeout=20)
            if r.status_code == 429:
                time.sleep(60)
                r = requests.post(url, json=payload, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"  [Crunchbase] Error: {e}")
        return None

    def _search_people(self, title_keyword: str, after_id: str = None) -> tuple[list, str | None]:
        payload = {
            "field_ids": [
                "first_name", "last_name", "primary_job_title",
                "primary_organization", "short_description",
                "profile_image_url", "linkedin",
            ],
            "query": [
                {
                    "field_id": "primary_job_title",
                    "operator_id": "contains",
                    "values": [title_keyword],
                },
                {
                    "field_id": "facet_ids",
                    "operator_id": "includes",
                    "values": ["person"],
                },
            ],
            "limit": 25,
        }
        if after_id:
            payload["after_id"] = after_id

        data = self._post("searches/people", payload)
        if not data:
            return [], None

        entities  = data.get("entities", [])
        next_page = data.get("paging", {}).get("next_page_url")
        after     = data.get("paging", {}).get("after_id")

        contacts = []
        for e in entities:
            props = e.get("properties", {})
            org   = props.get("primary_organization", {})
            li    = props.get("linkedin", {})

            contacts.append({
                "first_name":   props.get("first_name", ""),
                "last_name":    props.get("last_name", ""),
                "title":        props.get("primary_job_title", ""),
                "company":      org.get("value", "") if isinstance(org, dict) else "",
                "linkedin_url": li.get("value", "") if isinstance(li, dict) else "",
                "source_url":   f"https://crunchbase.com/person/{e.get('identifier', {}).get('permalink', '')}",
                "tags":         ["crunchbase"],
            })

        return contacts, after

    def fetch(self) -> list[dict]:
        contacts = []
        seen     = set()

        for role in TARGET_ROLES:
            print(f"  [Crunchbase] Searching: {role}")
            after_id = None

            for page in range(20):  # up to 500 results per role
                batch, after_id = self._search_people(role, after_id)
                for c in batch:
                    key = f"{c.get('first_name','')}_{c.get('last_name','')}_{c.get('company','')}"
                    if key not in seen:
                        seen.add(key)
                        contacts.append(c)

                if not after_id or not batch:
                    break
                time.sleep(1.5)

            time.sleep(2)

        print(f"  [Crunchbase] Total: {len(contacts)} people found")
        return contacts

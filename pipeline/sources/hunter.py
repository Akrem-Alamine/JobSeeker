"""
Hunter.io source — finds emails by company domain.
Free tier: 25 domain searches / month.
Paid tiers unlock more.

Requires: HUNTER_KEY in config
"""

import time
import requests
from .base import BaseSource


class HunterSource(BaseSource):
    name         = "hunter"
    requires_key = True

    def __init__(self, config: dict):
        super().__init__(config)
        self.key  = config.get("HUNTER_KEY", "")
        self.base = "https://api.hunter.io/v2"

    def _domain_search(self, domain: str) -> list[dict]:
        try:
            r = requests.get(f"{self.base}/domain-search", params={
                "domain": domain, "api_key": self.key, "limit": 100,
            }, timeout=15)
            if r.status_code != 200:
                return []
            data = r.json().get("data", {})
        except Exception:
            return []

        company = data.get("organization", "")
        contacts = []

        for email_obj in data.get("emails", []):
            email     = email_obj.get("value", "")
            first     = email_obj.get("first_name", "")
            last      = email_obj.get("last_name", "")
            title     = email_obj.get("position", "")
            linkedin  = email_obj.get("linkedin", "")
            confidence = email_obj.get("confidence", 0)

            if confidence < 50:
                continue

            contacts.append({
                "first_name":   first,
                "last_name":    last,
                "title":        title,
                "company":      company,
                "company_domain": domain,
                "email":        email,
                "email_status": "deliverable" if confidence > 80 else "unverified",
                "linkedin_url": linkedin,
                "source_url":   f"https://hunter.io/domain-search/{domain}",
                "tags":         ["hunter"],
            })
        return contacts

    def fetch(self) -> list[dict]:
        # Get unique domains from our existing contacts DB
        try:
            import sqlite3
            conn = sqlite3.connect("output/leads.db")
            domains = [r[0] for r in conn.execute(
                "SELECT DISTINCT company_domain FROM contacts WHERE company_domain != '' LIMIT 500"
            ).fetchall()]
            conn.close()
        except Exception:
            domains = []

        # Also pull domains from the original all_fixed database
        try:
            import csv
            with open("data/all_fixed.csv", encoding="utf-8-sig", errors="replace") as f:
                for row in csv.DictReader(f):
                    email = row.get("Email", "")
                    if "@" in email:
                        domain = email.split("@")[1].strip().lower()
                        if domain not in domains:
                            domains.append(domain)
        except Exception:
            pass

        domains = list(set(domains))[:25]  # free tier limit
        print(f"  [Hunter] Searching {len(domains)} domains")

        contacts = []
        for domain in domains:
            batch = self._domain_search(domain)
            contacts.extend(batch)
            print(f"  [Hunter] {domain}: {len(batch)} emails")
            time.sleep(1.5)

        return contacts

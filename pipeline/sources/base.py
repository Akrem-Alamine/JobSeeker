"""Abstract base class for all lead sources."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone


class BaseSource(ABC):
    name: str = "base"
    requires_key: bool = False

    def __init__(self, config: dict):
        self.config   = config
        self.new      = 0
        self.updated  = 0
        self.errors   = 0
        self.started  = datetime.now(timezone.utc).isoformat()

    @abstractmethod
    def fetch(self) -> list[dict]:
        """Fetch raw contacts from the source. Return list of contact dicts."""

    def run(self) -> list[dict]:
        contacts = self.fetch()
        # Normalize all contacts
        return [self._normalize(c) for c in contacts if c]

    def _normalize(self, c: dict) -> dict:
        """Ensure all contacts have a consistent structure."""
        email = (c.get("email") or "").strip().lower()
        first = (c.get("first_name") or "").strip()
        last  = (c.get("last_name")  or "").strip()
        full  = (c.get("full_name")  or f"{first} {last}").strip()

        domain = ""
        if "@" in email:
            domain = email.split("@")[1]
        elif c.get("company_domain"):
            domain = c["company_domain"]

        return {
            "first_name":     first,
            "last_name":      last,
            "full_name":      full,
            "title":          c.get("title", ""),
            "company":        c.get("company", ""),
            "company_domain": domain,
            "email":          email,
            "email_status":   c.get("email_status", "unverified"),
            "linkedin_url":   c.get("linkedin_url", ""),
            "github_url":     c.get("github_url", ""),
            "twitter_url":    c.get("twitter_url", ""),
            "website":        c.get("website", ""),
            "city":           c.get("city", ""),
            "country":        c.get("country", ""),
            "industry":       c.get("industry", "technology"),
            "tags":           c.get("tags", []),
            "source":         self.name,
            "source_url":     c.get("source_url", ""),
        }

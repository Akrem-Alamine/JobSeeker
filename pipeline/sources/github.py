"""
GitHub source — finds tech leaders via GitHub user search.
Extracts public emails, company, location from profiles.
Also mines commit history for hidden emails.

Requires: GITHUB_TOKEN in config (free personal access token)
Rate limit: 5000 req/hour authenticated, 30 search req/min
"""

import re
import time
import requests
from .base import BaseSource

SEARCH_QUERIES = [
    '"CTO" in:bio',
    '"CEO" in:bio',
    '"Founder" in:bio',
    '"Co-Founder" in:bio',
    '"Chief Technology Officer" in:bio',
    '"VP Engineering" in:bio',
    '"VP of Engineering" in:bio',
    '"Head of Engineering" in:bio',
    '"Director of Engineering" in:bio',
    '"Engineering Manager" in:bio followers:>100',
    '"Head of Infrastructure" in:bio',
    '"DevOps Lead" in:bio',
    '"Principal Engineer" in:bio followers:>200',
    '"Staff Engineer" in:bio followers:>200',
    '"Platform Engineering" in:bio followers:>100',
    '"Chief Architect" in:bio',
    '"Managing Director" in:bio',
    '"General Manager" in:bio technology',
    '"Head of IT" in:bio',
    '"CIO" in:bio',
]

EMAIL_RE = re.compile(r'[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}')


class GitHubSource(BaseSource):
    name         = "github"
    requires_key = True

    def __init__(self, config: dict):
        super().__init__(config)
        token = config.get("GITHUB_TOKEN", "")
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.base = "https://api.github.com"
        self.seen_logins: set[str] = set()

    def _get(self, url: str, params: dict = None, retries: int = 3) -> dict | None:
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=self.headers, params=params, timeout=15)
                if r.status_code == 403:
                    reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                    wait  = max(reset - int(time.time()), 5)
                    print(f"  [GitHub] Rate limited — waiting {wait}s")
                    time.sleep(wait)
                    continue
                if r.status_code == 200:
                    return r.json()
                time.sleep(2 ** attempt)
            except Exception as e:
                time.sleep(2 ** attempt)
        return None

    def _search_users(self, query: str) -> list[str]:
        """Return list of login names matching query (up to 1000)."""
        logins = []
        for page in range(1, 11):
            data = self._get(f"{self.base}/search/users", {
                "q": query, "per_page": 100, "page": page
            })
            if not data or not data.get("items"):
                break
            logins.extend(item["login"] for item in data["items"])
            if len(data["items"]) < 100:
                break
            time.sleep(2)  # search API: 30 req/min
        return logins

    def _get_user_email_from_commits(self, login: str) -> str:
        """Check recent public push events for commit emails."""
        data = self._get(f"{self.base}/users/{login}/events/public", {"per_page": 10})
        if not data:
            return ""
        for event in data:
            if event.get("type") != "PushEvent":
                continue
            for commit in event.get("payload", {}).get("commits", []):
                email = commit.get("author", {}).get("email", "")
                if email and "noreply" not in email and "@" in email:
                    return email.lower()
        return ""

    def _get_profile(self, login: str) -> dict | None:
        return self._get(f"{self.base}/users/{login}")

    def fetch(self) -> list[dict]:
        contacts = []

        for query in SEARCH_QUERIES:
            print(f"  [GitHub] Query: {query}")
            logins = self._search_users(query)
            print(f"  [GitHub] Found {len(logins)} users")

            for login in logins:
                if login in self.seen_logins:
                    continue
                self.seen_logins.add(login)

                profile = self._get_profile(login)
                if not profile:
                    continue

                # Extract name
                full_name = (profile.get("name") or "").strip()
                parts     = full_name.split(" ", 1)
                first     = parts[0] if parts else ""
                last      = parts[1] if len(parts) > 1 else ""

                # Extract email
                email = (profile.get("email") or "").strip().lower()
                if not email:
                    email = self._get_user_email_from_commits(login)
                    time.sleep(0.3)

                # Extract other fields
                company  = (profile.get("company") or "").strip().lstrip("@")
                location = (profile.get("location") or "").strip()
                blog     = (profile.get("blog") or "").strip()
                bio      = (profile.get("bio") or "").strip()

                # Extract title from bio
                title = ""
                bio_lower = bio.lower()
                for t in ["cto", "ceo", "founder", "co-founder", "vp engineering",
                          "vp of engineering", "head of engineering", "director of engineering",
                          "engineering manager", "chief technology officer",
                          "chief executive officer", "managing director"]:
                    if t in bio_lower:
                        title = t.title()
                        break

                contacts.append({
                    "first_name":   first,
                    "last_name":    last,
                    "full_name":    full_name,
                    "title":        title,
                    "company":      company,
                    "email":        email,
                    "github_url":   profile.get("html_url", ""),
                    "website":      blog,
                    "city":         location,
                    "source_url":   profile.get("html_url", ""),
                    "tags":         ["github"],
                })
                time.sleep(0.5)

            time.sleep(3)  # pause between queries

        return contacts

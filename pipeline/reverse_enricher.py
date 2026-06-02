"""
Reverse email enrichment — fills missing contact fields using free APIs.

Services tried in order per contact:
  1. Gravatar  — MD5 hash lookup, no auth (optional, slow: 100 req/hr)
  2. GitHub    — search users by email (30 req/min with token → ~19 hrs for 34k)
                  Returns: name, company, bio, location, GitHub URL

Progress is tracked via contacts.reverse_searched:
  0 = not yet processed (default)
  1 = processed (skip on next run)

Only EMPTY fields are updated — existing values are never overwritten.
Run: python run_pipeline.py --reverse-enrich
"""

import hashlib
import os
import re
import sqlite3
import time
from pathlib import Path

import requests
from tqdm import tqdm

DB_PATH      = Path("output/leads.db")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

GRAVATAR_TIMEOUT = 6
GITHUB_TIMEOUT   = 8

# ── Country extraction from free-text location strings ───────────────────────

COUNTRY_HINTS = {
    "usa": "United States", "u.s.a": "United States", "u.s.": "United States",
    "united states": "United States", "new york": "United States",
    "san francisco": "United States", "los angeles": "United States",
    "seattle": "United States", "boston": "United States", "chicago": "United States",
    "austin": "United States", "silicon valley": "United States",
    "uk": "United Kingdom", "u.k.": "United Kingdom",
    "united kingdom": "United Kingdom", "london": "United Kingdom",
    "england": "United Kingdom", "scotland": "United Kingdom",
    "germany": "Germany", "deutschland": "Germany", "berlin": "Germany",
    "munich": "Germany", "hamburg": "Germany", "frankfurt": "Germany",
    "france": "France", "paris": "France", "lyon": "France",
    "netherlands": "Netherlands", "amsterdam": "Netherlands",
    "canada": "Canada", "toronto": "Canada", "vancouver": "Canada",
    "australia": "Australia", "sydney": "Australia", "melbourne": "Australia",
    "india": "India", "bangalore": "India", "mumbai": "India",
    "israel": "Israel", "tel aviv": "Israel",
    "sweden": "Sweden", "stockholm": "Sweden",
    "norway": "Norway", "oslo": "Norway",
    "denmark": "Denmark", "copenhagen": "Denmark",
    "finland": "Finland", "helsinki": "Finland",
    "spain": "Spain", "madrid": "Spain", "barcelona": "Spain",
    "italy": "Italy", "rome": "Italy", "milan": "Italy",
    "switzerland": "Switzerland", "zurich": "Switzerland",
    "austria": "Austria", "vienna": "Austria",
    "belgium": "Belgium", "brussels": "Belgium",
    "poland": "Poland", "warsaw": "Poland",
    "brazil": "Brazil", "são paulo": "Brazil", "sao paulo": "Brazil",
    "singapore": "Singapore",
    "japan": "Japan", "tokyo": "Japan",
    "china": "China", "beijing": "China", "shanghai": "China",
    "south korea": "South Korea", "seoul": "South Korea",
    "portugal": "Portugal", "lisbon": "Portugal",
    "ireland": "Ireland", "dublin": "Ireland",
    "czech republic": "Czech Republic", "prague": "Czech Republic",
    "romania": "Romania", "bucharest": "Romania",
    "hungary": "Hungary", "budapest": "Hungary",
    "ukraine": "Ukraine", "kyiv": "Ukraine",
    "russia": "Russia", "moscow": "Russia",
    "turkey": "Turkey", "istanbul": "Turkey",
    "south africa": "South Africa", "cape town": "South Africa",
    "mexico": "Mexico", "mexico city": "Mexico",
    "argentina": "Argentina", "buenos aires": "Argentina",
    "colombia": "Colombia", "bogotá": "Colombia",
    "chile": "Chile", "santiago": "Chile",
    "new zealand": "New Zealand", "auckland": "New Zealand",
}


def _location_to_country(location: str) -> str:
    """Heuristically extract a country name from a free-text location string."""
    if not location:
        return ""
    loc = location.lower().strip()
    # Try full-string match first, then last comma-separated segment
    segments = [loc] + [s.strip() for s in loc.split(",")]
    for seg in segments:
        if seg in COUNTRY_HINTS:
            return COUNTRY_HINTS[seg]
    return ""


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' into (first, rest)."""
    parts = full_name.strip().split(" ", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _looks_like_title(text: str) -> bool:
    """Return True if the bio/description looks like it contains a job title."""
    if not text or len(text) > 150:
        return False
    title_words = {"ceo", "cto", "coo", "cfo", "founder", "engineer", "developer",
                   "director", "manager", "head", "vp", "president", "lead", "architect",
                   "scientist", "researcher", "designer", "product", "software", "data"}
    return any(w in text.lower() for w in title_words)


def _extract_linkedin(accounts: list[dict]) -> str:
    for a in accounts:
        url = a.get("url") or a.get("profile_url") or ""
        if "linkedin.com" in url:
            return url
    return ""


def _extract_github(accounts: list[dict]) -> str:
    for a in accounts:
        url = a.get("url") or a.get("profile_url") or ""
        if "github.com" in url:
            return url
    return ""


# ── Gravatar ──────────────────────────────────────────────────────────────────

def _gravatar_lookup(email: str) -> dict:
    """
    Look up profile via Gravatar's legacy JSON endpoint (no auth required).
    Returns a dict of enrichment fields, empty if nothing found.
    """
    md5 = hashlib.md5(email.strip().lower().encode()).hexdigest()
    try:
        r = requests.get(
            f"https://www.gravatar.com/{md5}.json",
            timeout=GRAVATAR_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if r.status_code != 200:
            return {}
        data  = r.json()
        entry = data.get("entry", [{}])[0]
    except Exception:
        return {}

    result = {}

    # Name
    display = entry.get("displayName") or entry.get("preferredUsername") or ""
    if display and " " in display:
        f, l = _split_name(display)
        result["first_name"] = f
        result["last_name"]  = l

    # Location → country
    loc = entry.get("currentLocation") or ""
    country = _location_to_country(loc)
    if country:
        result["country"] = country

    # Social accounts
    accounts = entry.get("accounts", [])
    li = _extract_linkedin(accounts)
    if li:
        result["linkedin_url"] = li
    gh = _extract_github(accounts)
    if gh:
        result["github_url"] = gh

    # Bio → title candidate
    about = entry.get("aboutMe") or ""
    if about and _looks_like_title(about) and not result.get("title"):
        result["title"] = about[:100]

    return result


# ── GitHub ────────────────────────────────────────────────────────────────────

_gh_headers = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
if GITHUB_TOKEN:
    _gh_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

_gh_last_req = [0.0]
_gh_delay    = 2.1 if GITHUB_TOKEN else 6.1   # 30/min auth, 10/min unauth


def _github_lookup(email: str) -> dict:
    """
    Search GitHub for a user by email address.
    Only finds users who have made their email public.
    """
    # Rate limit
    gap = _gh_delay - (time.time() - _gh_last_req[0])
    if gap > 0:
        time.sleep(gap)
    _gh_last_req[0] = time.time()

    try:
        r = requests.get(
            "https://api.github.com/search/users",
            params={"q": f"{email}+in:email", "per_page": 1},
            headers=_gh_headers,
            timeout=GITHUB_TIMEOUT,
        )
        if r.status_code == 422:   # unprocessable — email not searchable
            return {}
        items = r.json().get("items", [])
        if not items:
            return {}
        login = items[0]["login"]
    except Exception:
        return {}

    # Fetch full profile
    try:
        r2 = requests.get(
            f"https://api.github.com/users/{login}",
            headers=_gh_headers,
            timeout=GITHUB_TIMEOUT,
        )
        user = r2.json()
    except Exception:
        return {}

    result = {}

    name = (user.get("name") or "").strip()
    if name and " " in name:
        f, l = _split_name(name)
        result["first_name"] = f
        result["last_name"]  = l

    loc = user.get("location") or ""
    country = _location_to_country(loc)
    if country:
        result["country"] = country

    gh_url = user.get("html_url") or ""
    if gh_url:
        result["github_url"] = gh_url

    # Company field (GitHub users often put "@CompanyName" here)
    company = (user.get("company") or "").strip().lstrip("@")
    if company:
        result["company"] = company

    # Bio as title candidate
    bio = (user.get("bio") or "").strip()
    if bio and _looks_like_title(bio):
        result["title"] = bio[:100]

    return result


# ── Main enricher ─────────────────────────────────────────────────────────────

def _needs_enrichment(row: dict) -> bool:
    """Return True if this contact is missing enough data to be worth searching."""
    missing_name    = not row.get("first_name") or not row.get("last_name")
    missing_country = not row.get("country")
    missing_social  = not row.get("linkedin_url") and not row.get("github_url")
    return missing_name or missing_country or missing_social


def _apply_updates(cid: int, existing: dict, enriched: dict, conn: sqlite3.Connection):
    """Write only fields that are currently empty in the DB."""
    updates = {}
    for field, val in enriched.items():
        if not val:
            continue
        current = existing.get(field) or ""
        if not current:   # only fill empty fields
            updates[field] = val

    if not updates:
        return

    # Rebuild full_name if name was updated
    if "first_name" in updates or "last_name" in updates:
        f = updates.get("first_name") or existing.get("first_name") or ""
        l = updates.get("last_name")  or existing.get("last_name")  or ""
        full = f"{f} {l}".strip()
        if full:
            updates["full_name"] = full

    sets = ", ".join(f"{k}=?" for k in updates)
    conn.execute(f"UPDATE contacts SET {sets} WHERE id=?", list(updates.values()) + [cid])


def run(skip_github: bool = False, use_gravatar: bool = False):
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Only process contacts not yet searched
    rows = conn.execute("""
        SELECT id, first_name, last_name, country, linkedin_url, github_url, title, email
        FROM contacts
        WHERE email NOT LIKE '_noemail_%'
          AND email LIKE '%@%'
          AND (reverse_searched IS NULL OR reverse_searched = 0)
        ORDER BY id
    """).fetchall()
    conn.close()

    rows  = [dict(r) for r in rows if _needs_enrichment(dict(r))]
    total = len(rows)

    if total == 0:
        print("  [reverse_enricher] All contacts already processed.")
        return

    gh_note = f" | GitHub: {'30 req/min' if GITHUB_TOKEN else '10 req/min (no token)'}"
    grav_note = " | Gravatar: ON (slow, 100 req/hr)" if use_gravatar else ""
    print(f"  [reverse_enricher] {total:,} remaining contacts{gh_note}{grav_note}")
    if not use_gravatar:
        print("  [reverse_enricher] Gravatar skipped by default (too slow). Use --use-gravatar to enable.")

    gravatar_hits = 0
    github_hits   = 0
    total_updated = 0

    bar = tqdm(total=total, unit="contact", ncols=80,
               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")

    for row in rows:
        email    = row["email"]
        enriched = {}

        # ── Gravatar (optional, slow) ─────────────────────────────────────────
        if use_gravatar:
            grav = _gravatar_lookup(email)
            if grav:
                enriched.update(grav)
                gravatar_hits += 1

        # ── GitHub ────────────────────────────────────────────────────────────
        if not skip_github:
            has_name     = enriched.get("first_name") and enriched.get("last_name")
            has_location = enriched.get("country")
            if not (has_name and has_location):
                gh = _github_lookup(email)
                if gh:
                    for k, v in gh.items():
                        if k not in enriched:
                            enriched[k] = v
                    github_hits += 1

        # ── Write results + mark as searched ──────────────────────────────────
        conn = sqlite3.connect(str(DB_PATH))
        try:
            if enriched:
                _apply_updates(row["id"], row, enriched, conn)
                total_updated += 1
            conn.execute(
                "UPDATE contacts SET reverse_searched=1 WHERE id=?", (row["id"],)
            )
            conn.commit()
        finally:
            conn.close()

        bar.set_postfix(gh=github_hits, grav=gravatar_hits, updated=total_updated, refresh=False)
        bar.update(1)

    bar.close()
    print(
        f"\n  [reverse_enricher] Done — "
        f"GitHub hits: {github_hits} | Gravatar hits: {gravatar_hits} | "
        f"Updated: {total_updated}/{total}"
    )

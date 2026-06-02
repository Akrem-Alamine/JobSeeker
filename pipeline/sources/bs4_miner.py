"""
BeautifulSoup Contact Miner — enrichment for scraped=2 companies.

Strategy per company:
  1. Fetch homepage → discover and score internal links
  2. Visit top-scoring pages + common contact paths (/contact, /about, /team …)
  3. Extract emails from mailto: links and page text
  4. Pair emails with nearby names (HTML context) or infer from email pattern
  5. Infer title from email prefix (ceo@ → CEO, founder@ → Founder, …)
  6. Batch-verify and save directly — bypasses orchestrator title filter

Runs with ThreadPoolExecutor (WORKERS threads), HTTP capped at 40 concurrent
connections so the router stays happy.
"""

import concurrent.futures as cf
import json
import re
import threading
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
from tqdm import tqdm

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from .base import BaseSource

WORKERS    = 50
TIMEOUT    = 8
MAX_PAGES  = 6   # max extra pages to visit per company (beyond homepage)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

_http_sem = threading.Semaphore(40)

EMAIL_RE = re.compile(r'[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}')
NAME_RE  = re.compile(r'^[A-Z][a-z]{1,20}([\s\-][A-Z][a-z]{1,25}){1,3}$')

# URL path fragments that suggest contact/team content
CONTACT_URL_KW = {
    "team", "about", "contact", "people", "leadership", "who",
    "staff", "founders", "management", "executives", "board",
    "our-team", "about-us", "contact-us", "who-we-are",
    "our-people", "meet", "company", "uber-uns", "equipe",
    "nosotros", "sobre", "kontakt",
}

# Common contact paths to always try
COMMON_PATHS = [
    "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/our-team", "/people", "/leadership",
    "/founders", "/management", "/executives", "/company",
]

# Email prefixes that map to a title
EMAIL_TITLE_MAP = {
    "ceo":         "CEO",
    "cto":         "CTO",
    "cfo":         "CFO",
    "coo":         "COO",
    "cpo":         "CPO",
    "cso":         "CSO",
    "founder":     "Founder",
    "cofounder":   "Co-Founder",
    "president":   "President",
    "director":    "Director",
    "vp":          "VP",
    "md":          "Managing Director",
    "owner":       "Owner",
    "partner":     "Partner",
    "manager":     "Manager",
    "lead":        "Lead",
    "head":        "Head",
    "engineer":    "Engineer",
    "dev":         "Developer",
    "tech":        "Tech Lead",
}

# Email prefixes to skip entirely (generic / non-person)
GENERIC_PREFIXES = {
    "info", "contact", "hello", "hi", "support", "sales", "help",
    "admin", "office", "team", "mail", "email", "noreply", "no-reply",
    "hr", "jobs", "careers", "recruitment", "marketing", "press",
    "media", "partnership", "legal", "privacy", "abuse", "billing",
    "accounting", "finance", "security", "data", "dpo", "gdpr",
    "webmaster", "postmaster", "enquiries", "enquiry", "inquiry",
    "general", "newsletter", "news", "shop", "store", "service",
    "services", "booking", "reservations", "feedback", "hello",
}

# Name words that indicate it's not a real person name
JUNK_NAME_WORDS = {
    "team", "meet", "our", "board", "executive", "leadership",
    "management", "founders", "staff", "about", "directors",
    "investors", "advisor", "advisory", "committee", "ceo", "cto",
    "info", "contact", "support", "admin", "hello",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_internal(href: str, base_domain: str) -> bool:
    if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
        return False
    parsed = urlparse(href)
    if not parsed.netloc:
        return True
    link_domain = parsed.netloc.lower().lstrip("www.")
    base = base_domain.lower().lstrip("www.")
    return link_domain == base or link_domain.endswith("." + base)


def _score_url(url: str, link_text: str = "") -> int:
    score = 0
    url_lower   = url.lower()
    text_lower  = link_text.lower()
    for kw in CONTACT_URL_KW:
        if kw in url_lower:
            score += 2
        if kw in text_lower:
            score += 1
    return score


def _extract_name_from_email(email: str) -> tuple[str, str]:
    """Guess (first, last) from the email local part."""
    local = email.split("@")[0].lower()
    local = re.sub(r'\d+$', '', local)
    parts = re.split(r'[._\-]', local)
    parts = [p for p in parts if p and len(p) > 1]

    if not parts:
        return "", ""
    if len(parts) == 1:
        name = parts[0]
        if name in GENERIC_PREFIXES or name in JUNK_NAME_WORDS:
            return "", ""
        return name.title(), ""

    first, *rest = parts
    if first in GENERIC_PREFIXES or first in JUNK_NAME_WORDS:
        return "", ""
    last = " ".join(rest).title()
    return first.title(), last


def _find_name_near_element(soup: BeautifulSoup, email: str) -> str:
    """Try to find a person's name near the email in the HTML tree."""
    # 1. Check link text of the mailto: element itself
    for a in soup.find_all("a", href=re.compile(r'mailto:' + re.escape(email), re.I)):
        text = a.get_text(strip=True)
        if NAME_RE.match(text):
            return text

    # 2. Walk up the DOM from the email text node, scan nearby text
    node = soup.find(string=re.compile(re.escape(email), re.I))
    if node:
        parent = node.parent
        for _ in range(4):
            if parent is None:
                break
            text = parent.get_text(" ", strip=True)
            for m in re.finditer(r'([A-Z][a-z]{1,20}(?:\s[A-Z][a-z]{1,25}){1,3})', text):
                candidate = m.group(1)
                words = candidate.split()
                if all(w.lower() not in JUNK_NAME_WORDS for w in words):
                    return candidate
            parent = parent.parent

    return ""


def _extract_contacts_from_page(html: str, domain: str, company_name: str) -> list[dict]:
    """Extract person contacts from a single HTML page."""
    soup     = BeautifulSoup(html, "lxml")
    contacts = []
    seen     = set()

    def _add(email: str, first: str, last: str, title: str):
        email = email.lower().strip()
        if not email or email in seen:
            return
        if not first and not last:
            return
        seen.add(email)
        contacts.append({
            "first_name":     first,
            "last_name":      last,
            "full_name":      f"{first} {last}".strip(),
            "title":          title,
            "email":          email,
            "company":        company_name,
            "company_domain": domain,
            "source":         "bs4_miner",
            "email_status":   "unverified",
        })

    # 1. JSON-LD Person objects
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data  = json.loads(tag.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") == "Person":
                    name  = (item.get("name") or "").strip()
                    email = (item.get("email") or "").replace("mailto:", "").strip().lower()
                    title = (item.get("jobTitle") or "").strip()
                    if email and "@" in email:
                        parts = name.split(" ", 1)
                        _add(email, parts[0], parts[1] if len(parts) > 1 else "", title)
        except Exception:
            pass

    # 2. mailto: links
    for a in soup.find_all("a", href=re.compile(r'^mailto:', re.I)):
        href  = a["href"]
        email = href[7:].split("?")[0].strip().lower()
        if not email or "@" not in email or email in seen:
            continue
        prefix = email.split("@")[0]
        if prefix in GENERIC_PREFIXES:
            continue
        email_domain = email.split("@")[1]
        if not (email_domain == domain or email_domain.endswith("." + domain)):
            continue

        title = EMAIL_TITLE_MAP.get(prefix, "")
        name  = _find_name_near_element(soup, email)
        if NAME_RE.match(name):
            parts = name.split(" ", 1)
            first, last = parts[0], (parts[1] if len(parts) > 1 else "")
        else:
            first, last = _extract_name_from_email(email)

        if first:
            _add(email, first, last, title)

    # 3. Email addresses found in raw text
    page_text = soup.get_text()
    for email in EMAIL_RE.findall(page_text):
        email = email.lower()
        if email in seen:
            continue
        if "@" not in email:
            continue
        prefix       = email.split("@")[0]
        email_domain = email.split("@")[1]
        if prefix in GENERIC_PREFIXES:
            continue
        if not (email_domain == domain or email_domain.endswith("." + domain)):
            continue

        title = EMAIL_TITLE_MAP.get(prefix, "")
        first, last = _extract_name_from_email(email)
        if first:
            _add(email, first, last, title)

    return contacts


def _get(session: requests.Session, url: str) -> requests.Response | None:
    try:
        with _http_sem:
            r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
        return r if r.ok else None
    except Exception:
        return None


def _scrape_company(domain: str, company_name: str) -> list[dict]:
    """Scrape a company for contact emails. Returns deduplicated contact list."""
    base_url = f"https://{domain}"
    session  = requests.Session()
    session.headers.update(HEADERS)

    visited   = set()
    all_contacts: list[dict] = []
    candidates: list[tuple[int, str]] = []

    # Homepage
    resp = _get(session, base_url)
    if resp:
        visited.add(base_url)
        all_contacts.extend(_extract_contacts_from_page(resp.text, domain, company_name))
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            abs_ = urljoin(base_url, href)
            if _is_internal(href, domain) and abs_ not in visited:
                score = _score_url(abs_, a.get_text(strip=True))
                if score > 0:
                    candidates.append((score, abs_))

    # Always-try paths + top discovered links
    urls_to_visit: list[str] = []
    for path in COMMON_PATHS:
        u = base_url + path
        if u not in visited:
            urls_to_visit.append(u)

    candidates.sort(reverse=True)
    for _, url in candidates[:MAX_PAGES]:
        if url not in visited and url not in urls_to_visit:
            urls_to_visit.append(url)

    for url in urls_to_visit[: MAX_PAGES + len(COMMON_PATHS)]:
        if url in visited:
            continue
        visited.add(url)
        resp = _get(session, url)
        if resp:
            all_contacts.extend(_extract_contacts_from_page(resp.text, domain, company_name))

    # Deduplicate by email
    seen_emails: set[str] = set()
    unique: list[dict] = []
    for c in all_contacts:
        e = c["email"]
        if e and e not in seen_emails:
            seen_emails.add(e)
            unique.append(c)

    return unique


# ── Source class ──────────────────────────────────────────────────────────────

class BS4MinerSource(BaseSource):
    name         = "bs4_miner"
    requires_key = False

    def fetch(self) -> list[dict]:
        from pipeline import db as DB
        from pipeline.verifier import verify_batch
        import sqlite3

        conn = sqlite3.connect("output/leads.db")
        rows = conn.execute("""
            SELECT co.name, co.domain, co.scraped
            FROM companies co
            WHERE co.scraped != 1
            ORDER BY co.added_at
        """).fetchall()
        conn.close()

        total = len(rows)
        if total == 0:
            print("  [BS4Miner] Nothing to do — no scraped=2 companies")
            return []

        print(f"  [BS4Miner] {total:,} companies (scraped=0 or 2) — mining contact emails ({WORKERS} workers)")

        raw_contacts: list[dict] = []
        companies_hit = [0]
        lock = threading.Lock()

        def _process(row):
            name, domain, scraped = row
            contacts = _scrape_company(domain, name or domain)
            if contacts:
                with lock:
                    companies_hit[0] += 1
                DB.mark_company_scraped(domain, 1)
            elif scraped == 0:
                # Never visited before and miner found nothing → mark exhausted
                DB.mark_company_scraped(domain, 2)
            return contacts

        bar = tqdm(total=total, unit="co", ncols=80,
                   bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

        with cf.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(_process, row): row for row in rows}
            for fut in cf.as_completed(futures):
                try:
                    result = fut.result()
                    if result:
                        with lock:
                            raw_contacts.extend(result)
                except Exception:
                    pass
                bar.update(1)
                bar.set_postfix(hits=companies_hit[0], contacts=len(raw_contacts), refresh=False)
        bar.close()

        print(f"  [BS4Miner] {companies_hit[0]:,}/{total:,} companies yielded contacts — {len(raw_contacts):,} raw")

        # Deduplicate against existing DB
        new_contacts: list[dict] = []
        seen_new: set[str] = set()
        for c in raw_contacts:
            email = c["email"].lower()
            if email and email not in seen_new and not DB.email_exists(email):
                seen_new.add(email)
                new_contacts.append(c)

        print(f"  [BS4Miner] {len(new_contacts):,} not yet in DB — verifying emails …")

        if not new_contacts:
            return []

        # Batch email verification
        emails      = [c["email"] for c in new_contacts]
        ver_results = {r["email"]: r["status"]
                       for r in verify_batch(emails, workers=20, smtp=False)}
        for c in new_contacts:
            c["email_status"] = ver_results.get(c["email"], "unverified")

        # Save — skip only confirmed undeliverable
        inserted = 0
        for c in new_contacts:
            if c["email_status"] != "undeliverable":
                result = DB.upsert_contact(c)
                if result == "inserted":
                    inserted += 1

        print(f"  [BS4Miner] Saved {inserted:,} new contacts")

        # Return [] — already saved; orchestrator has nothing to do
        return []

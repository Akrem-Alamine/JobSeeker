"""
Company website scraper — escalating extraction pipeline.

Per company, steps run fastest → slowest, stopping as soon as a
directive contact (CEO/CTO/Founder/Director) is found:

  Step 1 — DB check           (instant)   already have a directive?
  Step 2 — DDG Instant API    (1 call)    structured infobox
  Step 3 — Requests + extract (multi)     JSON-LD, Schema.org, HTML
  Step 4 — Playwright         (JS sites)  headless browser fallback

scraped column values:
  0 = not yet processed
  1 = done, directive contact found
  2 = exhausted all steps, no directive found
"""

import concurrent.futures as cf
import json
import re
import socket
import time
import threading

import warnings

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from tqdm import tqdm

try:
    import cloudscraper as _cloudscraper
    _CS_AVAILABLE = True
except ImportError:
    _CS_AVAILABLE = False

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from .base import BaseSource

_abort    = threading.Event()   # set when main loop is done; workers check this to exit early
_http_sem = threading.Semaphore(40)  # cap total concurrent HTTP connections to protect router

# ── Status constants ──────────────────────────────────────────────────────────

SCRAPED_SUCCESS  = 1   # found directive contact
SCRAPED_EMPTY    = 2   # tried everything, nothing found

# ── URL paths ─────────────────────────────────────────────────────────────────

TEAM_PATHS = [
    "/team", "/about", "/about-us", "/leadership", "/people",
    "/management", "/our-team", "/company", "/board", "/executives",
    "/staff", "/founders", "/who-we-are",
]

TEAM_PATHS_PW = [
    "/team", "/about", "/about-us", "/leadership",
    "/people", "/founders", "/our-team", "/company",
]

# ── Title keywords ────────────────────────────────────────────────────────────

TITLE_WORDS = [
    "ceo", "cto", "cfo", "coo", "cpo", "cso", "cio",
    "founder", "co-founder", "cofounder",
    "director", "head of", "vp", "vice president", "president",
    "chief", "principal", "partner", "managing director",
    "manager", "lead", "engineer", "general manager",
]

BLOCK_TITLE_WORDS = {
    "ceo", "cto", "cfo", "coo", "cpo", "cso", "cio",
    "founder", "co-founder", "cofounder",
    "director", "head of", "vp", "vice president", "president",
    "chief", "principal", "partner", "managing director", "general manager",
}

KEYWORDS = BLOCK_TITLE_WORDS

# Titles that count as "directive" — triggers early exit and scraped=1
DIRECTIVE_TITLES = {
    "ceo", "chief executive", "cto", "chief technology",
    "cfo", "chief financial", "coo", "chief operating",
    "cpo", "cso", "cio", "founder", "co-founder", "cofounder",
    "president", "vice president", "director", "managing director",
    "general manager", "head of", "vp", "chairman", "partner",
}

FAKE_NAMES = {
    "executive team", "leadership", "founders", "meet our team", "our team",
    "board of directors", "management team", "team", "our leadership",
    "meet the team", "the team", "about us", "our founders", "the founders",
    "our board", "board members", "senior leadership", "leadership team",
    "executive leadership", "management", "staff", "people", "our people",
    "meet our executives", "meet our founders", "meet our board",
    "company leadership", "our management", "our staff", "advisory board",
}

NAME_BAD_WORDS = {
    "team", "meet", "our", "board", "executive", "leadership",
    "management", "founders", "staff", "about", "directors",
    "investors", "advisor", "advisory", "committee",
}

JS_MARKERS = [
    "__NEXT_DATA__", "ng-version", "data-reactroot", "window.__NUXT__",
    "__svelte", "gatsby-focus-wrapper", "__remix_manifest", "react-root",
    "data-reactid", "__vue",
]

DDG_ROLE_MAP = {
    "ceo": "CEO", "chief executive": "CEO",
    "founder": "Founder", "co-founder": "Co-Founder",
    "cto": "CTO", "chief technology": "CTO",
    "coo": "COO", "chief operating": "COO",
    "cfo": "CFO", "chief financial": "CFO",
    "president": "President", "chairman": "Chairman",
    "vp": "VP", "vice president": "Vice President",
    "director": "Director",
}

EMAIL_RE = re.compile(r'[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}')

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# ── Network health tracker ────────────────────────────────────────────────────
# Strategy: don't count errors. On every ConnectionError, do a direct DNS check.
# If 8.8.8.8:53 is reachable → just a bad server, keep going.
# Only block if DNS itself is down.

_net_checker    = threading.Lock()   # one thread polls; others wait silently
_dns_ok_until   = 0.0               # cache: network known-good until this timestamp
_dns_cache_lock = threading.Lock()


def _network_is_up() -> bool:
    """Cached DNS probe — avoids 30 workers all hitting 8.8.8.8 at once."""
    global _dns_ok_until
    with _dns_cache_lock:
        if time.time() < _dns_ok_until:
            return True
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        with _dns_cache_lock:
            _dns_ok_until = time.time() + 10   # known-good for 10 s
        return True
    except OSError:
        return False


def _wait_for_network():
    """Only one thread polls DNS; the rest wait silently."""
    if _net_checker.acquire(blocking=False):
        try:
            print("\n  [WebScraper] Network down — waiting to reconnect...")
            while not _network_is_up():
                time.sleep(15)
            print("  [WebScraper] Network restored — resuming.")
        finally:
            _net_checker.release()
    else:
        with _net_checker:   # block silently until checker finishes
            pass


# ── Thread-local HTTP sessions ────────────────────────────────────────────────

_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session


# ── Playwright — thread-local browsers, capped to 3 total instances ────────────
# Playwright sync API binds each browser to the thread that created it (greenlets).
# We cap total RAM by allowing only _PW_SLOTS threads to ever own a browser.
# Other threads simply skip Playwright — their JS companies become scraped=2
# and are picked up by the --step enrich DDG pass.

_PW_SLOTS       = 3
_pw_browser_sem = threading.Semaphore(_PW_SLOTS)   # claimed once per thread, never released


def _get_thread_browser():
    """Return this thread's browser, or None if all slots are taken."""
    if hasattr(_local, "pw_browser"):
        return _local.pw_browser
    if getattr(_local, "pw_denied", False):
        return None
    if _pw_browser_sem.acquire(blocking=False):
        from playwright.sync_api import sync_playwright
        _local.pw_playwright = sync_playwright().start()
        _local.pw_browser    = _local.pw_playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                "--disk-cache-size=0", "--disable-application-cache",
                "--disable-cache", "--media-cache-size=0",
            ],
        )
        return _local.pw_browser
    _local.pw_denied = True
    return None


def _fetch_playwright(url: str) -> BeautifulSoup | None:
    browser = _get_thread_browser()
    if browser is None:
        return None
    page = None
    try:
        page = browser.new_page()
        page.set_default_timeout(12000)
        page.set_extra_http_headers({"User-Agent": HEADERS["User-Agent"]})
        page.goto(url, timeout=15000, wait_until="domcontentloaded")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        if len(soup.get_text(strip=True)) > 200:
            return soup
    except Exception:
        pass
    finally:
        if page:
            try: page.close()
            except Exception: pass
    return None


# ── DDG dedicated thread (rate-limited, non-blocking for workers) ─────────────

_DDG_STOP = object()


class _DDGThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True, name="ddg-worker")
        self.q = queue.Queue()

    def run(self):
        while True:
            item = self.q.get()
            if item is _DDG_STOP:
                break
            company, domain, out, evt = item
            try:
                out["people"] = _ddg_query(company, domain)
            except Exception:
                out["people"] = []
            finally:
                evt.set()
            time.sleep(1.5)  # rate limit

    def ask(self, company: str, domain: str, timeout: float = 12.0) -> list[dict]:
        out: dict = {}
        evt = threading.Event()
        self.q.put((company, domain, out, evt))
        evt.wait(timeout=timeout)
        return out.get("people", [])

    def stop(self):
        self.q.put(_DDG_STOP)


_ddg_thread: _DDGThread | None = None


def _ddg_query(company: str, domain: str) -> list[dict]:
    people = []
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": company, "format": "json",
                    "no_redirect": "1", "no_html": "1", "skip_disambig": "1"},
            timeout=8, headers=HEADERS,
        )
        data = r.json()

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
                    people.append(_make_contact(
                        parts[0], parts[1] if len(parts) > 1 else "",
                        role, domain, company, tag="ddg"))

        if not people:
            abstract = data.get("AbstractText", "")
            for pattern in [
                r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:is|,)\s+(?:the\s+)?(?:CEO|founder|CTO)',
                r'(?:CEO|founder|CTO)\s+(?:is\s+)?([A-Z][a-z]+ [A-Z][a-z]+)',
                r'founded by ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            ]:
                for name in re.findall(pattern, abstract):
                    name = name.strip()
                    if _is_valid_name(name):
                        parts = name.split(" ", 1)
                        people.append(_make_contact(
                            parts[0], parts[1] if len(parts) > 1 else "",
                            "Executive", domain, company, tag="ddg"))
    except Exception:
        pass
    return people


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has_directive(people: list[dict]) -> bool:
    for p in people:
        t = p.get("title", "").lower()
        if any(d in t for d in DIRECTIVE_TITLES):
            return True
    return False


def _make_contact(first, last, title, domain, company, tag="web_scraper") -> dict:
    return {
        "first_name":     first,
        "last_name":      last,
        "full_name":      f"{first} {last}".strip(),
        "title":          title[:100],
        "company":        company,
        "company_domain": domain,
        "email":          "",
        "source":         "web_scraper",
        "tags":           ["web_scraper", tag],
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


def _clean_title(raw: str) -> str:
    raw = re.sub(r'(?i)linkedin\s*profile\s*', '', raw)
    raw = re.sub(r'(?i)(follow|connect)\s+on\s+\w+', '', raw)
    raw = re.sub(r'(?i)view\s+profile', '', raw)
    for sep in ["|", "—", "·", "•", "\n", "  "]:
        if sep in raw:
            parts = [p.strip() for p in raw.split(sep) if p.strip()]
            for part in parts:
                if any(w in part.lower() for w in TITLE_WORDS):
                    raw = part
                    break
    raw = re.sub(r'(?i)(meet our|our team|board of directors?|executive team)', '', raw)
    return raw.strip()[:100]


def _is_js_rendered(html: str, soup: BeautifulSoup) -> bool:
    if len(soup.get_text(strip=True)) < 300:
        return True
    return any(m in html for m in JS_MARKERS)


def _is_team_page(soup: BeautifulSoup) -> bool:
    return any(w in soup.get_text().lower() for w in KEYWORDS)


# ── Extraction methods ────────────────────────────────────────────────────────

def _extract_json_ld(soup: BeautifulSoup, domain: str, company: str) -> list[dict]:
    people = []

    def _from_item(item: dict) -> dict | None:
        name  = (item.get("name") or "").strip()
        title = (item.get("jobTitle") or "").strip()
        email = (item.get("email") or "").strip().lower().replace("mailto:", "")
        if not _is_valid_name(name) or not title:
            return None
        parts = name.split(" ", 1)
        return {
            "first_name": parts[0], "last_name": parts[1] if len(parts) > 1 else "",
            "full_name": name, "title": title[:100], "company": company,
            "company_domain": domain, "email": email,
            "source": "web_scraper", "tags": ["web_scraper", "json_ld"],
        }

    def _walk(obj):
        if isinstance(obj, list):
            for x in obj: _walk(x)
        elif isinstance(obj, dict):
            if obj.get("@type") == "Person":
                p = _from_item(obj)
                if p: people.append(p)
            for key in ("member", "employee", "founder", "alumni", "director", "author"):
                _walk(obj.get(key, []))

    for script in soup.find_all("script", type="application/ld+json"):
        try: _walk(json.loads(script.string or ""))
        except Exception: pass
    return people


def _extract_schema_org(soup: BeautifulSoup, domain: str, company: str) -> list[dict]:
    people = []
    for el in soup.find_all(attrs={"itemtype": re.compile(r"schema\.org/Person", re.I)}):
        name_el  = el.find(attrs={"itemprop": "name"})
        title_el = el.find(attrs={"itemprop": "jobTitle"})
        email_el = el.find(attrs={"itemprop": "email"})
        name  = name_el.get_text(strip=True)  if name_el  else ""
        title = title_el.get_text(strip=True) if title_el else ""
        email = (email_el.get_text(strip=True) if email_el else "").replace("mailto:", "").lower()
        if not _is_valid_name(name) or not title:
            continue
        parts = name.split(" ", 1)
        people.append({
            "first_name": parts[0], "last_name": parts[1] if len(parts) > 1 else "",
            "full_name": name, "title": title[:100], "company": company,
            "company_domain": domain, "email": email,
            "source": "web_scraper", "tags": ["web_scraper", "schema_org"],
        })
    return people


def _extract_html(soup: BeautifulSoup, domain: str, company: str) -> list[dict]:
    people     = []
    seen_names = set()
    emails_page = {e.lower() for e in EMAIL_RE.findall(soup.get_text()) if domain in e}

    class_blocks = soup.find_all(class_=re.compile(
        r"team|member|person|staff|leader|people|bio|card|profile|executive", re.I
    ))
    candidates = class_blocks or soup.find_all(["article", "li"], limit=200)

    for block in candidates:
        if len(people) >= 30:
            break
        text = block.get_text(" ", strip=True)
        if not (10 < len(text) < 800):
            continue
        if not any(w in text.lower() for w in BLOCK_TITLE_WORDS):
            continue

        name_tag = (
            block.find(class_=re.compile(r"\bname\b", re.I)) or
            block.find(["h2", "h3", "h4", "strong", "b"])
        )
        if not name_tag:
            continue

        full_name = name_tag.get_text(strip=True)
        if not _is_valid_name(full_name) or full_name in seen_names:
            continue
        seen_names.add(full_name)

        title = ""
        for sib in name_tag.next_siblings:
            if hasattr(sib, "get_text"):
                t = sib.get_text(" ", strip=True)
                if t and any(w in t.lower() for w in TITLE_WORDS):
                    title = _clean_title(t)
                    break
        if not title:
            for part in re.split(r'[\n|·•\t]', text.replace(full_name, "")):
                part = part.strip()
                if part and any(w in part.lower() for w in TITLE_WORDS):
                    title = _clean_title(part)
                    break
        if not title:
            continue

        block_emails = EMAIL_RE.findall(text)
        email = block_emails[0].lower() if block_emails else ""
        parts = full_name.strip().split(" ", 1)
        people.append({
            "first_name": parts[0], "last_name": parts[1] if len(parts) > 1 else "",
            "full_name": full_name, "title": title, "company": company,
            "company_domain": domain, "email": email,
            "source": "web_scraper", "tags": ["web_scraper", "html"],
        })

    if not people:
        for email in list(emails_page)[:5]:
            local = email.split("@")[0]
            if any(w in local for w in ["info","contact","admin","support","hello","team"]):
                continue
            people.append({
                "email": email, "company": company, "company_domain": domain,
                "source": "web_scraper", "tags": ["web_scraper", "email_fallback"],
            })
    return people


def _extract_all(soup: BeautifulSoup, domain: str, company: str) -> list[dict]:
    people = []
    people.extend(_extract_json_ld(soup, domain, company))
    people.extend(_extract_schema_org(soup, domain, company))
    people.extend(_extract_html(soup, domain, company))
    seen, unique = set(), []
    for p in people:
        key = p.get("full_name", p.get("email", "")).lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(p)
        if len(unique) >= 40:
            break
    return unique


# ── HTTP fetch with network retry ─────────────────────────────────────────────

def _fetch_cloudscraper(url: str) -> tuple[BeautifulSoup | None, bool]:
    """Cloudflare-bypass fallback using cloudscraper."""
    if not _CS_AVAILABLE:
        return None, False
    try:
        cs = _cloudscraper.create_scraper(browser={"browser": "firefox", "platform": "linux"})
        with _http_sem:
            r = cs.get(url, timeout=12, allow_redirects=True)
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
            soup = BeautifulSoup(r.text, "lxml")
            return soup, _is_js_rendered(r.text, soup)
    except Exception:
        pass
    return None, False


def _fetch_requests(url: str) -> tuple[BeautifulSoup | None, bool]:
    for attempt in range(3):
        try:
            with _http_sem:
                r = _get_session().get(url, timeout=6, allow_redirects=True)
            if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
                soup = BeautifulSoup(r.text, "lxml")
                return soup, _is_js_rendered(r.text, soup)
            # Cloudflare or bot-protection — try cloudscraper
            if r.status_code in (403, 429, 503):
                return _fetch_cloudscraper(url)
            return None, False
        except requests.exceptions.Timeout:
            return None, False
        except requests.exceptions.ConnectionError:
            if not _network_is_up():
                _wait_for_network()
            elif attempt < 2:
                time.sleep(2 ** attempt)
        except Exception:
            return None, False
    return None, False


# ── Per-company escalation ────────────────────────────────────────────────────

def _scrape_one(row: dict) -> tuple[str, list[dict], int]:
    """
    Returns (domain, contacts, status)
      status 1 = directive found
      status 2 = exhausted, nothing found

    Escalation (fastest → slowest):
      Step 1 — DB check    (instant)
      Step 2 — Requests    (JSON-LD + Schema.org + HTML, 13 paths)
      Step 3 — Playwright  (only if JS rendering was detected)
    DDG is NOT run here — it runs as a separate --step enrich pass.
    """
    from pipeline import db as DB

    domain  = row["domain"]
    company = row["name"] or domain
    all_people: list[dict] = []

    try:
        # ── Step 1: DB check (instant) ────────────────────────────────────
        if DB.has_directive_contact(domain):
            return domain, [], SCRAPED_SUCCESS

        # ── Step 2: Requests + extraction ────────────────────────────────
        js_detected = False
        for path in TEAM_PATHS:
            if _abort.is_set():
                break
            soup, is_js = _fetch_requests(f"https://{domain}{path}")
            if soup is None:
                continue
            if is_js:
                js_detected = True
                continue
            if _is_team_page(soup):
                found = _extract_all(soup, domain, company)
                all_people.extend(found)
                if _has_directive(found):
                    return domain, all_people, SCRAPED_SUCCESS

        # ── Step 3: Playwright — only if JS was detected ──────────────────
        if js_detected and not _abort.is_set():
            for path in TEAM_PATHS_PW:
                if _abort.is_set():
                    break
                soup = _fetch_playwright(f"https://{domain}{path}")
                if soup and _is_team_page(soup):
                    found = _extract_all(soup, domain, company)
                    all_people.extend(found)
                    if _has_directive(found):
                        return domain, all_people, SCRAPED_SUCCESS

    except Exception:
        pass

    # Deduplicate
    seen, unique = set(), []
    for p in all_people:
        key = p.get("full_name", p.get("email", "")).lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(p)

    status = SCRAPED_SUCCESS if _has_directive(unique) else SCRAPED_EMPTY
    return domain, unique, status


# ── Source class ───────────────────────────────────────────────────────────────

class WebScraperSource(BaseSource):
    name         = "web_scraper"
    requires_key = False

    def fetch(self) -> list[dict]:
        from pipeline import db as DB

        limit   = int(self.config.get("SCRAPER_LIMIT", 500))
        workers = int(self.config.get("SCRAPER_WORKERS", 30))
        queue_  = DB.get_unscraped_companies(limit=limit)

        print(f"  [WebScraper] {len(queue_)} companies — {workers} workers")
        print(f"  [WebScraper] Steps: DB → Requests → Playwright (JS only)")

        _abort.clear()
        contacts = []
        found    = 0
        success  = 0
        empty    = 0

        executor = cf.ThreadPoolExecutor(max_workers=workers)
        try:
            futures_map  = {executor.submit(_scrape_one, row): row for row in queue_}
            future_times = {f: time.time() for f in futures_map}
            pending      = set(futures_map.keys())

            with tqdm(total=len(queue_), unit="co", ncols=90) as bar:
                while pending:
                    done, pending = cf.wait(
                        pending, timeout=5, return_when=cf.FIRST_COMPLETED
                    )

                    for future in done:
                        row = futures_map[future]
                        try:
                            domain, people, status = future.result()
                        except Exception:
                            domain = row["domain"]
                            people = []
                            status = SCRAPED_EMPTY

                        DB.mark_company_scraped(domain, status)
                        found += len(people)
                        if status == SCRAPED_SUCCESS:
                            success += 1
                        else:
                            empty += 1
                        if people:
                            contacts.extend(people)
                        bar.set_postfix(found=found, hit=success, empty=empty)
                        bar.update(1)

                    # Evict only RUNNING futures stuck > 120 s
                    now   = time.time()
                    stuck = [f for f in pending
                             if f.running() and now - future_times[f] > 120]
                    for f in stuck:
                        pending.discard(f)
                        DB.mark_company_scraped(futures_map[f]["domain"], SCRAPED_EMPTY)
                        empty += 1
                        bar.update(1)
        finally:
            # Signal all worker threads to abort, then don't wait for stragglers
            _abort.set()
            executor.shutdown(wait=False, cancel_futures=True)

        print(f"  [WebScraper] Done — {success} with directives, {empty} exhausted, {found} contacts")
        return contacts

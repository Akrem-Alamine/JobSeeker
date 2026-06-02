"""
Web search enrichment — DuckDuckGo + Ollama contact cleaner.

For each contact, searches DuckDuckGo (LinkedIn-focused query), feeds the
result snippets to a local Ollama model, and writes corrected fields back.

contacts.search_status values:
  NULL        — not yet processed
  'enriched'  — search found usable data (DB updated)
  'not_found' — search ran but yielded no useful data
"""

import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from pipeline.llm_client import llm_generate_json, LLM_BACKEND

DB_PATH         = "output/leads.db"
DDG_URL         = "https://html.duckduckgo.com/html/"
DDG_DELAY       = 1.0    # minimum seconds between DDG requests (shared across workers)
DEFAULT_WORKERS = 3
REQUEST_TIMEOUT = 12

# Only these fields may be written by the LLM (prevents prompt-injection overwrites)
ALLOWED_FIELDS = {"first_name", "last_name", "title", "company", "country"}

DDG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Shared DDG rate-limiter (all worker threads share this)
_ddg_lock     = threading.Lock()
_ddg_last_req = [0.0]


def _ddg_search(query: str) -> list[str]:
    """Return up to 5 result-snippet strings from DuckDuckGo HTML search."""
    with _ddg_lock:
        gap = DDG_DELAY - (time.time() - _ddg_last_req[0])
        if gap > 0:
            time.sleep(gap)
        _ddg_last_req[0] = time.time()
    try:
        resp = requests.post(
            DDG_URL,
            data={"q": query, "kl": "en-us"},
            headers=DDG_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        return [
            el.get_text(" ", strip=True)
            for el in soup.select(".result__snippet")[:5]
            if el.get_text(strip=True)
        ]
    except Exception:
        return []


def _llm_extract(contact: dict, snippets: list[str]) -> dict:
    """Ask the LLM to extract clean fields from search snippets."""
    if not snippets:
        return {}

    name    = f"{contact.get('first_name') or ''} {contact.get('last_name') or ''}".strip()
    company = contact.get("company") or contact.get("company_domain") or ""
    email   = contact.get("email") or ""
    snippet_block = "\n".join(f"[{i+1}] {s}" for i, s in enumerate(snippets))

    prompt = (
        "Extract contact information from these web search results.\n\n"
        f"Person we are looking for:\n"
        f"- Name: {name or 'unknown'}\n"
        f"- Company: {company or 'unknown'}\n"
        f"- Email: {email}\n\n"
        f"Search results:\n{snippet_block}\n\n"
        "Return ONLY a JSON object — no markdown, no explanation:\n"
        '{"first_name": "...", "last_name": "...", "title": "...", '
        '"company": "...", "country": "..."}\n\n'
        "Rules:\n"
        "- Only fill a field if you are confident it refers to this exact person\n"
        "- Use null for any field you are not sure about\n"
        "- title = current job title only (e.g. \"CTO\") — no company name inside it\n"
        "- country = English country name (e.g. \"Germany\", \"United States\")\n"
        "- Do NOT invent or guess — only use what the search results explicitly state\n"
    )

    return llm_generate_json(prompt, max_tokens=150, temperature=0.05)


def _process(row: dict) -> tuple[int, str, dict]:
    """Run DDG search + Ollama extraction for one contact. Thread-safe."""
    cid   = row["id"]
    first = (row.get("first_name") or "").strip()
    last  = (row.get("last_name")  or "").strip()
    name  = f"{first} {last}".strip()

    if not name:
        local = (row.get("email") or "").split("@")[0]
        name  = re.sub(r'[._\-]', " ", local).strip()

    company = (
        (row.get("company") or "").strip()
        or (row.get("company_domain") or "").strip()
    )

    if not name:
        return cid, "not_found", {}

    query    = f'"{name}" {company} linkedin'.strip() if company else f'"{name}" linkedin'
    snippets = _ddg_search(query)
    extracted = _llm_extract(row, snippets) if snippets else {}

    updates = {
        k: str(v).strip()
        for k, v in extracted.items()
        if k in ALLOWED_FIELDS
        and v
        and str(v).lower() not in ("null", "none", "", "unknown")
    }

    if updates:
        return cid, "enriched", updates
    return cid, "not_found", {}


def _write(cid: int, status: str, updates: dict):
    """Persist one result to the DB (thread-safe: each call owns its connection)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        if updates:
            safe = {k: v for k, v in updates.items() if k in ALLOWED_FIELDS}
            sets = ", ".join(f"{k}=?" for k in safe) + ", search_status=?"
            conn.execute(
                f"UPDATE contacts SET {sets} WHERE id=?",
                list(safe.values()) + [status, cid],
            )
        else:
            conn.execute(
                "UPDATE contacts SET search_status=? WHERE id=?", (status, cid)
            )
        conn.commit()
    finally:
        conn.close()


def run(workers: int = DEFAULT_WORKERS):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, first_name, last_name, title, company, company_domain, email
        FROM contacts
        WHERE search_status IS NULL
        ORDER BY id
    """).fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        print("  [web_searcher] All contacts already searched.")
        return

    eta = round(total * DDG_DELAY / workers / 3600, 1)
    print(
        f"  [web_searcher] {total:,} contacts | "
        f"workers={workers}  DDG_DELAY={DDG_DELAY}s  ETA≈{eta}h  LLM={LLM_BACKEND}"
    )

    enriched  = 0
    not_found = 0
    bar = tqdm(
        total=total, unit="contact", ncols=80,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, dict(r)): None for r in rows}
        for fut in as_completed(futures):
            cid, status, updates = fut.result()
            _write(cid, status, updates)
            if status == "enriched":
                enriched += 1
            else:
                not_found += 1
            bar.set_postfix(enriched=enriched, not_found=not_found, refresh=False)
            bar.update(1)

    bar.close()
    print(
        f"\n  [web_searcher] Done — "
        f"{enriched:,} enriched  {not_found:,} not found  {total:,} total"
    )

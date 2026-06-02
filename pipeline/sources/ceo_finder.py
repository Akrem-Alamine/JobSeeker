"""
CEO-Finder source — finds the top executive for companies with no contacts.

Strategy (fast-first):
  1. Ask LLM about 20 companies in a single request — 20x fewer API calls,
     stays within Groq free-tier RPM limit. ~0.5s/batch = ~0.025s/company.
  2. Companies the LLM doesn't know → 1 DDG search as fallback (rate-limited).

With 10 workers: 62k companies ≈ 1–2 hours total (was 650h with per-company calls).
Progress tracked via companies.ceo_searched — safe to interrupt and resume.
"""

import re
import json
import sqlite3
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from .base import BaseSource
from pipeline.llm_client import llm_generate_json_array, llm_generate_json, LLM_BACKEND
from pipeline.db import DB_PATH

DDG_URL    = "https://html.duckduckgo.com/html/"
DDG_DELAY  = 2.0
WORKERS    = 10
BATCH_SIZE = 20   # companies per LLM call

DDG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

FAKE_NAMES = {
    "executive team", "leadership", "founders", "our team", "team",
    "management", "board of directors", "staff", "john doe", "jane doe",
    "first last", "full name", "your name", "null", "none", "unknown", "n/a",
}

_ddg_lock     = threading.Lock()
_ddg_last_req = [0.0]
_write_lock   = threading.Lock()


# ── Batch LLM query (20 companies per call) ───────────────────────────────────

def _llm_batch(rows: list[dict]) -> list[dict]:
    """Ask the LLM about up to BATCH_SIZE companies in a single request."""
    lines = "\n".join(
        f"{i+1}. {(r.get('name') or r['domain']).strip()} (domain: {r['domain']})"
        for i, r in enumerate(rows)
    )
    prompt = (
        f"For each company below, identify the current CEO, Founder, or top executive "
        f"from your training knowledge.\n\n"
        f"{lines}\n\n"
        "Return ONLY a JSON array with exactly one object per company (same order):\n"
        '[{"name": "Full Name", "title": "CEO"}, ...]\n\n'
        "Rules:\n"
        "- name = real full name (first + last)\n"
        "- title = exact role (CEO, Co-Founder, CTO, etc.)\n"
        '- If unknown or not confident: {"name": null, "title": null}\n'
        "- Do NOT guess or invent names\n"
    )
    return llm_generate_json_array(prompt, expected=len(rows), max_tokens=len(rows) * 40)


# ── DDG fallback (1 query only) ───────────────────────────────────────────────

def _ddg_search_one(query: str) -> list[str]:
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
            timeout=8,
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        return [
            el.get_text(" ", strip=True)
            for el in soup.select(".result__snippet")[:5]
            if el.get_text(strip=True)
        ]
    except Exception:
        return []


def _llm_from_snippets(company: str, snippets: list[str]) -> dict:
    if not snippets:
        return {}
    block  = "\n".join(f"[{i+1}] {s}" for i, s in enumerate(snippets))
    prompt = (
        f"From these search results about '{company}', extract the CEO or Founder.\n\n"
        f"{block}\n\n"
        "Return ONLY JSON:\n"
        '{"name": "Full Name", "title": "CEO"}\n'
        "Use null if not found. Do not invent.\n"
    )
    return llm_generate_json(prompt, max_tokens=60, temperature=0.0)


# ── Validation ────────────────────────────────────────────────────────────────

def _is_valid(name: str, title: str) -> bool:
    if not name or not title:
        return False
    name = name.strip()
    if name.lower() in FAKE_NAMES or str(title).lower() in ("null", "none"):
        return False
    if len(name) < 4 or len(name) > 60:
        return False
    words = name.split()
    if len(words) < 2 or len(words) > 5:
        return False
    if any(c.isdigit() for c in name):
        return False
    if not all(re.match(r"^[A-Za-z'\-\.]+$", w) for w in words):
        return False
    return True


# ── Batch worker ──────────────────────────────────────────────────────────────

def _process_batch(rows: list[dict]) -> list[tuple[int, dict | None]]:
    """Process a batch of companies; returns list of (company_id, contact_or_None)."""
    llm_results = _llm_batch(rows)
    results = []

    for row, llm_res in zip(rows, llm_results):
        company = (row.get("name") or "").strip() or row["domain"]
        domain  = row["domain"]
        cid     = row["id"]

        name  = (llm_res.get("name") or "").strip()
        title = (llm_res.get("title") or "").strip()

        # DDG fallback only for companies the LLM genuinely didn't know
        if not _is_valid(name, title):
            snippets = _ddg_search_one(f'"{company}" CEO founder')
            fallback = _llm_from_snippets(company, snippets)
            name  = (fallback.get("name") or "").strip()
            title = (fallback.get("title") or "").strip()

        if not _is_valid(name, title):
            results.append((cid, None))
            continue

        parts = name.split(" ", 1)
        results.append((cid, {
            "first_name":     parts[0],
            "last_name":      parts[1] if len(parts) > 1 else "",
            "full_name":      name,
            "title":          title[:100],
            "company":        company,
            "company_domain": domain,
            "email":          "",
            "source":         "ceo_finder",
            "tags":           ["ceo_finder", LLM_BACKEND],
        }))

    return results


def _mark_searched(ids: list[int]):
    if not ids:
        return
    conn = sqlite3.connect(str(DB_PATH))
    conn.executemany(
        "UPDATE companies SET ceo_searched=1 WHERE id=?", [(i,) for i in ids]
    )
    conn.commit()
    conn.close()


# ── Source class ──────────────────────────────────────────────────────────────

class CEOFinderSource(BaseSource):
    name         = "ceo_finder"
    requires_key = False

    def fetch(self) -> list[dict]:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        companies = conn.execute("""
            SELECT co.id, co.domain, co.name
            FROM companies co
            WHERE (co.ceo_searched IS NULL OR co.ceo_searched = 0)
              AND NOT EXISTS (
                SELECT 1 FROM contacts ct
                WHERE ct.company_domain = co.domain
                  AND ct.title IS NOT NULL AND ct.title != ''
              )
            ORDER BY co.id
        """).fetchall()
        conn.close()

        total = len(companies)
        if total == 0:
            print("  [ceo_finder] All companies already searched.")
            return []

        n_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        eta = round(n_batches * 0.6 / WORKERS / 3600, 1)
        print(
            f"  [ceo_finder] {total:,} companies | batch={BATCH_SIZE} "
            f"workers={WORKERS}  LLM={LLM_BACKEND}  ETA≈{eta}h"
        )

        rows = [dict(r) for r in companies]
        batches = [rows[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

        all_contacts = []
        found        = 0
        searched_ids = []
        flush_every  = 200

        bar = tqdm(total=total, unit="co", ncols=80,
                   bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_process_batch, batch): batch for batch in batches}
            for fut in as_completed(futures):
                for cid, contact in fut.result():
                    searched_ids.append(cid)
                    if contact:
                        all_contacts.append(contact)
                        found += 1

                    if len(searched_ids) >= flush_every:
                        _mark_searched(searched_ids)
                        searched_ids = []

                bar.set_postfix(found=found, refresh=False)
                bar.update(len(futures[fut]))

        bar.close()
        _mark_searched(searched_ids)

        print(f"  [ceo_finder] Found executives for {found:,}/{total:,} companies")
        return all_contacts

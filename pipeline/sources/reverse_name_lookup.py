"""
Reverse name lookup — enriches contacts by identifying the person behind
each email using 6 free OSINT sources (Gravatar, GitHub, Keybase, EmailRep,
DuckDuckGo, email pattern). Unconditionally overwrites first_name, last_name,
full_name, and company whenever a result is found.

Progress tracked via contacts.reverse_searched — safe to interrupt and resume.
"""

import sys
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

# Pull in the reverse-lookup project that lives next to this repo
_LOOKUP_DIR = Path(__file__).resolve().parents[3] / "reverse-lookup"
if _LOOKUP_DIR.is_dir() and str(_LOOKUP_DIR) not in sys.path:
    sys.path.insert(0, str(_LOOKUP_DIR))

try:
    from lookup import reverse_lookup  # type: ignore
except ImportError as e:
    raise ImportError(
        f"Could not import reverse-lookup project from {_LOOKUP_DIR}. "
        "Make sure /home/alamino/Projects/reverse-lookup/ exists."
    ) from e

from .base import BaseSource
from pipeline.db import DB_PATH, get_conn

WORKERS = 30


def _process(row: dict) -> tuple[int, dict]:
    """Run reverse lookup for one email; return (contact_id, fields_to_update).
    Only includes a field in updates when the new value differs from the current one."""
    result  = reverse_lookup(row["email"])
    updates = {}

    name = (result.get("name") or "").strip()
    if name:
        parts    = name.split(" ", 1)
        new_fn   = parts[0]
        new_ln   = parts[1] if len(parts) > 1 else ""
        new_full = name
        if new_fn   != (row.get("first_name") or "").strip():
            updates["first_name"] = new_fn
        if new_ln   != (row.get("last_name")  or "").strip():
            updates["last_name"]  = new_ln
        if new_full != (row.get("full_name")   or "").strip():
            updates["full_name"]  = new_full

    company = (result.get("company") or "").strip().lstrip("@").strip()
    if company and company != (row.get("company") or "").strip():
        updates["company"] = company

    return row["id"], updates


def _flush(conn: sqlite3.Connection, pending: list[tuple[int, dict]]):
    for cid, updates in pending:
        if updates:
            cols = ", ".join(f"{k}=?" for k in updates) + ", reverse_searched=1"
            conn.execute(
                f"UPDATE contacts SET {cols} WHERE id=?",
                list(updates.values()) + [cid],
            )
        else:
            conn.execute(
                "UPDATE contacts SET reverse_searched=1 WHERE id=?", (cid,)
            )
    conn.commit()


class ReverseNameLookupSource(BaseSource):
    name         = "reverse_name_lookup"
    requires_key = False

    def fetch(self) -> list[dict]:
        conn = get_conn()
        rows = conn.execute("""
            SELECT id, email, first_name, last_name, full_name, company
            FROM contacts
            WHERE email_status = 'deliverable'
              AND email NOT LIKE '_noemail_%'
              AND email LIKE '%@%'
              AND (reverse_searched IS NULL OR reverse_searched = 0)
            ORDER BY id
        """).fetchall()
        conn.close()

        total = len(rows)
        if total == 0:
            print("  [reverse_name_lookup] All contacts already processed.")
            return []

        print(f"  [reverse_name_lookup] {total:,} contacts to enrich | workers={WORKERS}")

        rows     = [dict(r) for r in rows]
        enriched = 0
        pending: list[tuple[int, dict]] = []
        flush_every = 50

        bar  = tqdm(total=total, unit="ct", ncols=80,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
        conn = get_conn()

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_process, row): row for row in rows}
            for fut in as_completed(futures):
                cid, updates = fut.result()
                pending.append((cid, updates))
                if updates:
                    enriched += 1

                if len(pending) >= flush_every:
                    _flush(conn, pending)
                    pending = []

                bar.set_postfix(enriched=enriched, refresh=False)
                bar.update(1)

        _flush(conn, pending)
        conn.close()
        bar.close()

        print(f"  [reverse_name_lookup] Enriched {enriched:,}/{total:,} contacts")
        return []  # enrichment is done in-place; no new contacts to hand to the pipeline

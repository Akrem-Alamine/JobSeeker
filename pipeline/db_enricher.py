"""
Database enrichment — fills missing/corrupted fields using email patterns.

Steps:
  0. Clean corrupted names (control characters)
  1. Fix names from email  (first, last, full_name)
  2. Rebuild full_name from first + last where missing
  3. Fill company names from domain  (companies table)
  4. Fill contact company names from domain  (contacts table)
  5. Fill company countries via ip-api.com batch (free)
  6. Sync country from companies to contacts  (via domain, not company_id)
"""

import re
import time

import requests
import sqlite3


DB_PATH     = "output/leads.db"
IPAPI_BATCH = "http://ip-api.com/batch"
BATCH_SIZE  = 100
BATCH_SLEEP = 4.5   # 15 req/min → safe

# Known non-person words that appear in first_name due to bad imports
JUNK_FIRST = {
    "hiring", "ceo", "cto", "cfo", "coo", "president", "director",
    "computer", "universite", "university", "head", "manager", "lead",
    "engineer", "developer", "sales", "marketing", "hr", "recruiter",
    "biocopy", "dynaprog", "ecomiio", "actionable", "constructor",
    "info", "contact", "team", "admin", "support", "hello",
}

# TLDs and tech suffixes that appear in company-name-as-first_name
TECH_SUFFIXES = re.compile(r'\.(ai|io|com|de|fr|nl|tech|co|app|net|org)$', re.I)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Step 0: Clean corrupted names ────────────────────────────────────────────

_CTRL_RE = re.compile(r'[\x00-\x1f\x7f]')   # ASCII control chars only; keep Unicode letters

def clean_names(conn):
    rows = conn.execute("SELECT id, first_name, last_name, full_name FROM contacts").fetchall()
    fixed = 0
    for r in rows:
        f = _CTRL_RE.sub('', r["first_name"] or "").strip()
        l = _CTRL_RE.sub('', r["last_name"]  or "").strip()
        n = _CTRL_RE.sub('', r["full_name"]  or "").strip()
        if f != (r["first_name"] or "") or l != (r["last_name"] or "") or n != (r["full_name"] or ""):
            conn.execute("UPDATE contacts SET first_name=?, last_name=?, full_name=? WHERE id=?",
                         (f, l, n, r["id"]))
            fixed += 1
    conn.commit()
    print(f"  [clean_names] {fixed:,} rows cleaned of control characters")


# ── Name helpers ──────────────────────────────────────────────────────────────

def _looks_like_person(name: str, email: str = "") -> bool:
    """Return True if this looks like a real first name (not a company name)."""
    if not name:
        return False
    name = name.strip()
    if len(name) < 2 or len(name) > 20:
        return False
    if any(c.isdigit() for c in name):
        return False
    if TECH_SUFFIXES.search(name):
        return False
    if name.lower() in JUNK_FIRST:
        return False
    if not name[0].isalpha():
        return False
    if not re.match(r"^[A-Za-zÀ-ÿ'\-]+$", name):
        return False
    # If the name matches the email domain (e.g. first_name="Acadys", email="x@acadys.com")
    # it's a company name that leaked into the name field
    if email:
        domain = email.split("@")[-1].split(".")[0].lower()
        if name.lower() == domain or name.lower().replace(" ", "") == domain:
            return False
    # Contains known role keywords (bceo, vceo, etc.)
    lower = name.lower()
    if any(role in lower for role in ("ceo", "cto", "cfo", "coo", "vp", "cpo")):
        return False
    return True


def _is_corrupted_last(last: str) -> bool:
    return not last or last.strip('"\' ') == '' or '"""' in last


# Honorifics / title prefixes that appear before names in emails
EMAIL_TITLE_PREFIXES = {"dr", "mr", "mrs", "ms", "prof", "sir", "rev", "mx", "drs", "ir"}

# Punctuation-only strings that are junk first names (single chars, dots, dashes)
_JUNK_FIRST_RE = re.compile(r'^[^A-Za-zÀ-ÿ]{0,3}$')


def _extract_from_email(email: str) -> tuple[str, str]:
    """
    Best-effort (first, last) extraction from email local part.

    Patterns handled:
      dr.firstname.lastname@   → Firstname, Lastname  (title prefix stripped)
      firstname.lastname@      → Firstname, Lastname
      firstname_lastname@      → Firstname, Lastname
      firstname-lastname@      → Firstname, Lastname
      f.lastname@              → '', Lastname  (initial too short)
      firstname.m.lastname@    → Firstname, Lastname  (middle initial skipped)
      firstname@               → Firstname, ''
    """
    local = email.split("@")[0].lower()

    # Remove digits-only suffixes: john.doe2 → john.doe
    local = re.sub(r'\d+$', '', local)

    parts = re.split(r'[._\-]', local)
    parts = [p for p in parts if p]   # remove empty

    # Strip leading honorific prefixes: dr., mr., ms., prof. …
    while parts and parts[0] in EMAIL_TITLE_PREFIXES:
        parts = parts[1:]

    if not parts:
        return "", ""

    if len(parts) == 1:
        return parts[0].title(), ""

    if len(parts) == 2:
        first, last = parts
        if len(first) <= 2:
            return "", last.title()
        return first.title(), last.title()

    # 3+ parts: skip middle initials (single char parts)
    first = parts[0]
    remaining = [p for p in parts[1:] if len(p) > 1]
    last = " ".join(remaining).title() if remaining else parts[-1].title()
    if len(first) <= 2:
        return "", last
    return first.title(), last


# ── Step 1: Fix names from email ──────────────────────────────────────────────

def fix_names(conn):
    rows = conn.execute("""
        SELECT id, first_name, last_name, full_name, email
        FROM contacts
        WHERE email LIKE '%@%'
    """).fetchall()

    fixed_first = 0
    fixed_last  = 0
    skipped     = 0

    for r in rows:
        first = (r["first_name"] or "").strip()
        last  = (r["last_name"]  or "").strip()
        email = r["email"] or ""

        need_first = not _looks_like_person(first, email)
        need_last  = _is_corrupted_last(last)

        if not need_first and not need_last:
            skipped += 1
            continue

        ex_first, ex_last = _extract_from_email(email)

        new_first = first
        new_last  = last

        if need_first and ex_first and _looks_like_person(ex_first):
            new_first = ex_first
            fixed_first += 1
        elif need_first and not ex_first:
            # Email gave nothing better — at least clear punctuation junk (., -, etc.)
            if _JUNK_FIRST_RE.match(first):
                new_first = ""
                fixed_first += 1

        if need_last and ex_last:
            new_last = ex_last
            fixed_last += 1

        # If first was junk but we got a good first from email, also fix last
        if need_first and ex_first and ex_last and _is_corrupted_last(new_last):
            new_last = ex_last

        new_full = f"{new_first} {new_last}".strip()

        conn.execute(
            "UPDATE contacts SET first_name=?, last_name=?, full_name=? WHERE id=?",
            (new_first, new_last, new_full, r["id"])
        )

    conn.commit()
    print(f"  [fix_names] first_name fixed: {fixed_first} | last_name fixed: {fixed_last} | skipped: {skipped}")


# ── Step 2: Rebuild full_name from first + last ───────────────────────────────

def rebuild_full_names(conn):
    cur = conn.execute("""
        UPDATE contacts
        SET full_name = TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,''))
        WHERE (full_name IS NULL OR full_name = '')
          AND (first_name != '' OR last_name != '')
    """)
    conn.commit()
    print(f"  [rebuild_full_names] {cur.rowcount:,} full names rebuilt from first + last")


# ── Step 3: Fill company names from domain ────────────────────────────────────

def _domain_to_name(domain: str) -> str:
    """stripe.com → Stripe   |   my-cool-startup.io → My Cool Startup"""
    parts = domain.lower().split(".")
    if parts[0] in ("www", "app", "mail", "api", "dev", "portal", "go"):
        parts = parts[1:]
    core = parts[0] if parts else domain
    return core.replace("-", " ").replace("_", " ").title()


def fill_company_names(conn):
    rows = conn.execute("""
        SELECT id, domain FROM companies
        WHERE name IS NULL OR name = ''
    """).fetchall()

    if not rows:
        print("  [fill_company_names] Nothing to do")
        return

    for r in rows:
        conn.execute("UPDATE companies SET name=? WHERE id=?",
                     (_domain_to_name(r["domain"]), r["id"]))
    conn.commit()
    print(f"  [fill_company_names] Filled {len(rows):,} company names from domain")


# ── Step 4: Fill contact company names from domain ───────────────────────────

def fill_contact_companies(conn):
    rows = conn.execute("""
        SELECT c.id, c.company_domain
        FROM contacts c
        WHERE (c.company IS NULL OR c.company = '')
          AND c.company_domain IS NOT NULL AND c.company_domain != ''
    """).fetchall()
    if not rows:
        print("  [fill_contact_companies] Nothing to do")
        return
    for r in rows:
        conn.execute("UPDATE contacts SET company=? WHERE id=?",
                     (_domain_to_name(r["company_domain"]), r["id"]))
    conn.commit()
    print(f"  [fill_contact_companies] Filled {len(rows):,} contact company names from domain")


# ── Step 5: Fill countries via ip-api.com batch ───────────────────────────────

def fill_countries(conn):
    rows = conn.execute("""
        SELECT id, domain FROM companies
        WHERE country IS NULL OR country = ''
        ORDER BY id
    """).fetchall()

    if not rows:
        print("  [fill_countries] Nothing to do")
        return

    total   = len(rows)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    filled  = 0

    print(f"  [fill_countries] {total:,} companies — {batches} batches (~{batches*BATCH_SLEEP/60:.0f} min)")

    for b in range(batches):
        chunk   = rows[b * BATCH_SIZE:(b + 1) * BATCH_SIZE]
        payload = [{"query": r["domain"], "fields": "country,countryCode,status"}
                   for r in chunk]
        try:
            resp    = requests.post(IPAPI_BATCH, json=payload, timeout=15)
            results = resp.json()
        except Exception as e:
            print(f"  [fill_countries] Batch {b+1} error: {e}")
            time.sleep(BATCH_SLEEP)
            continue

        for r, res in zip(chunk, results):
            if res.get("status") == "success" and res.get("country"):
                conn.execute("UPDATE companies SET country=? WHERE id=?",
                             (res["country"], r["id"]))
                filled += 1

        conn.commit()
        if (b + 1) % 20 == 0 or b == batches - 1:
            print(f"  [fill_countries] {b+1}/{batches} batches — {filled:,} countries filled")
        time.sleep(BATCH_SLEEP)

    print(f"  [fill_countries] Done — {filled:,} / {total:,} filled")


# ── Step 6: Sync country from companies → contacts (by domain) ───────────────

def sync_contact_countries(conn):
    # Join by company_domain (not company_id) so all 34k contacts benefit,
    # not just the 13k that have company_id populated.
    cur = conn.execute("""
        UPDATE contacts
        SET country = (
            SELECT co.country FROM companies co
            WHERE co.domain = contacts.company_domain
              AND co.country IS NOT NULL AND co.country != ''
            LIMIT 1
        )
        WHERE (contacts.country IS NULL OR contacts.country = '')
          AND contacts.company_domain IS NOT NULL AND contacts.company_domain != ''
    """)
    conn.commit()
    print(f"  [sync_countries] {cur.rowcount:,} contact countries synced from companies")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(skip_countries: bool = False):
    conn = get_conn()

    print("\n── Step 0: Clean corrupted names (control characters)")
    clean_names(conn)

    print("\n── Step 1: Fix names from email")
    fix_names(conn)

    print("\n── Step 2: Rebuild missing full_name from first + last")
    rebuild_full_names(conn)

    print("\n── Step 3: Fill company names from domain (companies table)")
    fill_company_names(conn)

    print("\n── Step 4: Fill contact company names from domain (contacts table)")
    fill_contact_companies(conn)

    if not skip_countries:
        print("\n── Step 5: Fill company countries (ip-api.com batch)")
        fill_countries(conn)

        print("\n── Step 6: Sync countries to contacts (by domain)")
        sync_contact_countries(conn)
    else:
        print("\n── Skipping country enrichment (--no-countries)")

    conn.close()
    print("\n  DB enrichment complete.")

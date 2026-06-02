"""
Central SQLite database for the lead generation pipeline.

Schema
──────
companies   — one row per company (domain UNIQUE)
contacts    — one row per person (email UNIQUE), FK → companies.id
run_history — one row per pipeline run
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("output/leads.db")

TARGET_TITLES = {
    "ceo", "chief executive officer",
    "cto", "chief technology officer",
    "cio", "chief information officer",
    "cpo", "chief product officer",
    "coo", "chief operating officer",
    "cso", "chief security officer",
    "founder", "co-founder", "cofounder",
    "vp engineering", "vp of engineering", "vice president engineering",
    "vp technology", "vp of technology",
    "vp product", "vp of product",
    "director of engineering", "engineering director",
    "head of engineering", "head of technology", "head of it",
    "director of technology", "director of it",
    "engineering manager", "senior engineering manager",
    "chief architect", "principal architect", "solutions architect",
    "devops lead", "head of devops", "head of infrastructure",
    "head of platform", "platform lead",
    "general manager", "managing director",
    "president", "technical lead", "tech lead",
}


def normalize_title(title: str) -> str:
    return title.lower().strip()


def is_target_title(title: str) -> bool:
    t = normalize_title(title)
    return any(target in t for target in TARGET_TITLES)


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


# ─────────────────────────────────────────────────────────────────────────────
#  Schema initialisation + migration
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    """Create tables if they don't exist, then run any needed migrations."""
    conn = get_conn()
    conn.executescript("""
        -- ── Companies ──────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS companies (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT,
            domain         TEXT UNIQUE,
            country        TEXT,
            country_code   TEXT,
            industry       TEXT DEFAULT 'technology',
            description    TEXT,
            size           TEXT,
            employee_count INTEGER,
            linkedin_url   TEXT,
            source         TEXT,
            scraped        INTEGER DEFAULT 0,
            verified       INTEGER DEFAULT 0,
            added_at       TEXT,
            updated_at     TEXT
        );

        -- ── Contacts ───────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS contacts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id       INTEGER REFERENCES companies(id) ON DELETE SET NULL,
            first_name       TEXT,
            last_name        TEXT,
            full_name        TEXT,
            title            TEXT,
            title_normalized TEXT,
            company          TEXT,
            company_domain   TEXT,
            email            TEXT UNIQUE,
            email_status     TEXT DEFAULT 'unverified',
            phone            TEXT,
            linkedin_url     TEXT,
            github_url       TEXT,
            twitter_url      TEXT,
            website          TEXT,
            city             TEXT,
            country          TEXT,
            industry         TEXT,
            tags             TEXT,
            source           TEXT,
            source_url       TEXT,
            created_at       TEXT,
            updated_at       TEXT,
            verified_at      TEXT
        );

        -- ── Run history ────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS run_history (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            source         TEXT,
            started_at     TEXT,
            completed_at   TEXT,
            new_contacts   INTEGER DEFAULT 0,
            updated        INTEGER DEFAULT 0,
            errors         INTEGER DEFAULT 0,
            status         TEXT,
            notes          TEXT
        );

        -- ── Indexes ────────────────────────────────────────────────────────
    """)
    conn.commit()

    # ── Migrations (safe to run on existing DB) ───────────────────────────
    _migrate(conn)

    # All indexes created after _migrate() so new columns are guaranteed to exist
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_contacts_email        ON contacts(email)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_domain       ON contacts(company_domain)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_title        ON contacts(title_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_source       ON contacts(source)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_email_status ON contacts(email_status)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_company_id   ON contacts(company_id)",
        "CREATE INDEX IF NOT EXISTS idx_companies_domain      ON companies(domain)",
        "CREATE INDEX IF NOT EXISTS idx_companies_scraped     ON companies(scraped)",
        "CREATE INDEX IF NOT EXISTS idx_companies_country     ON companies(country)",
        "CREATE INDEX IF NOT EXISTS idx_companies_verified      ON companies(verified)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_search_status ON contacts(search_status)",
    ]:
        conn.execute(ddl)
    conn.commit()
    conn.close()


def _migrate(conn: sqlite3.Connection):
    """Add columns that didn't exist in earlier schema versions."""
    existing_company_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(companies)").fetchall()
    }
    existing_contact_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(contacts)").fetchall()
    }

    new_company_cols = {
        "country_code":   "TEXT",
        "size":           "TEXT",
        "employee_count": "INTEGER",
        "linkedin_url":   "TEXT",
        "verified":       "INTEGER DEFAULT 0",
        "updated_at":     "TEXT",
    }
    new_contact_cols = {
        "company_id":    "INTEGER",
        "search_status": "TEXT",
        "outreach_status": "TEXT",
    }

    for col, col_type in new_company_cols.items():
        if col not in existing_company_cols:
            conn.execute(f"ALTER TABLE companies ADD COLUMN {col} {col_type}")

    for col, col_type in new_contact_cols.items():
        if col not in existing_contact_cols:
            conn.execute(f"ALTER TABLE contacts ADD COLUMN {col} {col_type}")

    conn.commit()

    # Link existing contacts to companies where domain matches
    _link_contacts_to_companies(conn)


def _link_contacts_to_companies(conn: sqlite3.Connection):
    """Populate contacts.company_id for any contact whose company_domain matches a company."""
    conn.execute("""
        UPDATE contacts
        SET company_id = (
            SELECT id FROM companies
            WHERE companies.domain = contacts.company_domain
            LIMIT 1
        )
        WHERE company_id IS NULL
          AND company_domain != ''
    """)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
#  Company CRUD
# ─────────────────────────────────────────────────────────────────────────────

def upsert_company(data: dict) -> tuple[int, str]:
    """
    Insert or update a company record.
    Returns (id, 'inserted' | 'updated' | 'skipped').
    Domain is required; records without domain are skipped.
    """
    domain = (data.get("domain") or "").strip().lower()
    if not domain or "." not in domain or len(domain) > 100:
        return 0, "skipped"

    now  = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    try:
        existing = conn.execute(
            "SELECT id, scraped FROM companies WHERE domain = ?", (domain,)
        ).fetchone()

        row = {
            "name":           (data.get("name") or "").strip()[:200],
            "domain":         domain,
            "country":        (data.get("country") or "").strip()[:100],
            "country_code":   (data.get("country_code") or "").strip()[:10].upper(),
            "industry":       (data.get("industry") or "technology").strip()[:100],
            "description":    (data.get("description") or "").strip()[:500],
            "size":           (data.get("size") or "").strip()[:50],
            "employee_count": data.get("employee_count"),
            "linkedin_url":   (data.get("linkedin_url") or "").strip()[:300],
            "source":         (data.get("source") or "").strip()[:100],
            "updated_at":     now,
        }

        if existing:
            # Update non-empty fields only
            updates = {k: v for k, v in row.items() if v and k != "domain"}
            if updates:
                sets = ", ".join(f"{k}=?" for k in updates)
                conn.execute(
                    f"UPDATE companies SET {sets} WHERE domain=?",
                    list(updates.values()) + [domain],
                )
                conn.commit()
            return existing["id"], "updated"
        else:
            # Check if we already have contacts for this domain; if so, mark scraped
            has_contacts = conn.execute(
                "SELECT 1 FROM contacts WHERE company_domain=? LIMIT 1", (domain,)
            ).fetchone()
            row["scraped"]  = 1 if has_contacts else 0
            row["added_at"] = now

            cur = conn.execute(
                f"INSERT INTO companies ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                list(row.values()),
            )
            conn.commit()
            company_id = cur.lastrowid

            # Immediately link existing contacts
            if has_contacts:
                conn.execute(
                    "UPDATE contacts SET company_id=? WHERE company_domain=? AND company_id IS NULL",
                    (company_id, domain),
                )
                conn.commit()

            return company_id, "inserted"
    finally:
        conn.close()


def get_company_id(domain: str) -> int | None:
    """Return the companies.id for a domain, or None if not found."""
    conn = get_conn()
    try:
        row = conn.execute("SELECT id FROM companies WHERE domain=?", (domain,)).fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def get_unscraped_companies(limit: int = 500) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, domain, name, country FROM companies WHERE scraped=0 ORDER BY added_at LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_company_scraped(domain: str, value: int = 1):
    """value: 1 = found directive contact, 2 = exhausted (no directive found)."""
    conn = get_conn()
    conn.execute(
        "UPDATE companies SET scraped=?, updated_at=? WHERE domain=?",
        (value, datetime.now(timezone.utc).isoformat(), domain),
    )
    conn.commit()
    conn.close()


def has_directive_contact(domain: str) -> bool:
    """Return True if this company already has a directive-level contact in DB."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT title FROM contacts WHERE company_domain=? LIMIT 20",
            (domain,),
        ).fetchall()
        return any(is_target_title(r[0]) for r in rows if r[0])
    finally:
        conn.close()


def get_known_domains() -> set[str]:
    """All domains already in the system (companies + contacts tables)."""
    conn = get_conn()
    try:
        c_domains = {r[0] for r in conn.execute(
            "SELECT DISTINCT company_domain FROM contacts WHERE company_domain != ''"
        ).fetchall()}
        co_domains = {r[0] for r in conn.execute("SELECT domain FROM companies").fetchall()}
        return c_domains | co_domains
    finally:
        conn.close()


def companies_stats() -> dict:
    conn = get_conn()
    try:
        total   = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=0").fetchone()[0]
        scraped = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=1").fetchone()[0]
        by_country = dict(conn.execute(
            "SELECT country, COUNT(*) FROM companies WHERE country != '' "
            "GROUP BY country ORDER BY COUNT(*) DESC LIMIT 20"
        ).fetchall())
        return {"total": total, "pending": pending, "scraped": scraped, "by_country": by_country}
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  Contact CRUD
# ─────────────────────────────────────────────────────────────────────────────

def upsert_contact(data: dict) -> str:
    """
    Insert or update a contact.
    Resolves company_id automatically from company_domain.
    Returns 'inserted' | 'updated' | 'skipped'.
    """
    import hashlib
    email = (data.get("email") or "").strip().lower()

    if not email or "@" not in email:
        name    = (data.get("full_name") or
                   f"{data.get('first_name','')} {data.get('last_name','')}").strip()
        company = data.get("company", "")
        source  = data.get("source", "")
        if not name:
            return "skipped"
        key   = hashlib.md5(f"{name}|{company}|{source}".encode()).hexdigest()[:12]
        email = f"_noemail_{key}@noemail.placeholder"
        data  = {**data, "email": email, "email_status": "no_email"}

    now  = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    try:
        existing = conn.execute(
            "SELECT id FROM contacts WHERE email=?", (email,)
        ).fetchone()

        domain = (data.get("company_domain") or "").strip().lower()
        if not domain and "@" in email and not email.startswith("_noemail_"):
            domain = email.split("@")[1]

        # Resolve company_id from domain
        company_id = data.get("company_id")
        if not company_id and domain:
            row = conn.execute(
                "SELECT id FROM companies WHERE domain=?", (domain,)
            ).fetchone()
            company_id = row["id"] if row else None

        title = data.get("title", "")
        row = {
            "company_id":       company_id,
            "first_name":       (data.get("first_name") or "").strip(),
            "last_name":        (data.get("last_name") or "").strip(),
            "full_name":        (data.get("full_name") or "").strip(),
            "title":            title,
            "title_normalized": normalize_title(title),
            "company":          (data.get("company") or "").strip(),
            "company_domain":   domain,
            "email":            email,
            "email_status":     data.get("email_status", "unverified"),
            "phone":            (data.get("phone") or "").strip(),
            "linkedin_url":     (data.get("linkedin_url") or "").strip(),
            "github_url":       (data.get("github_url") or "").strip(),
            "twitter_url":      (data.get("twitter_url") or "").strip(),
            "website":          (data.get("website") or "").strip(),
            "city":             (data.get("city") or "").strip(),
            "country":          (data.get("country") or "").strip(),
            "industry":         (data.get("industry") or "technology").strip(),
            "tags":             json.dumps(data.get("tags", [])),
            "source":           (data.get("source") or "").strip(),
            "source_url":       (data.get("source_url") or "").strip(),
            "updated_at":       now,
        }

        if existing:
            updates = {k: v for k, v in row.items() if v}
            if updates:
                sets = ", ".join(f"{k}=?" for k in updates)
                conn.execute(
                    f"UPDATE contacts SET {sets} WHERE email=?",
                    list(updates.values()) + [email],
                )
                conn.commit()
            return "updated"
        else:
            row["created_at"] = now
            cols = ", ".join(row.keys())
            vals = ", ".join("?" * len(row))
            conn.execute(f"INSERT INTO contacts ({cols}) VALUES ({vals})", list(row.values()))
            conn.commit()
            return "inserted"
    finally:
        conn.close()


def email_exists(email: str) -> bool:
    conn = get_conn()
    try:
        return conn.execute(
            "SELECT 1 FROM contacts WHERE email=?", (email.lower().strip(),)
        ).fetchone() is not None
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  Logging / export / stats
# ─────────────────────────────────────────────────────────────────────────────

def log_run(source: str, started_at: str, new: int, updated: int,
            errors: int, status: str, notes: str = ""):
    conn = get_conn()
    conn.execute("""
        INSERT INTO run_history
            (source, started_at, completed_at, new_contacts, updated, errors, status, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (source, started_at, datetime.now(timezone.utc).isoformat(),
          new, updated, errors, status, notes))
    conn.commit()
    conn.close()


def export_csv(path: str = "output/leads_database.csv"):
    import csv
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            ct.first_name, ct.last_name, ct.title,
            ct.company, ct.company_domain,
            co.country  AS company_country,
            co.size     AS company_size,
            ct.email, ct.email_status,
            ct.linkedin_url, ct.github_url,
            ct.city, ct.country, ct.industry, ct.source, ct.created_at
        FROM contacts ct
        LEFT JOIN companies co ON ct.company_id = co.id
        WHERE ct.email_status != 'undeliverable'
        ORDER BY ct.created_at DESC
    """).fetchall()
    conn.close()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "First Name", "Last Name", "Title", "Company", "Domain",
            "Company Country", "Company Size",
            "Email", "Email Status", "LinkedIn", "GitHub",
            "City", "Country", "Industry", "Source", "Added",
        ])
        writer.writerows(rows)
    return len(rows)


def stats() -> dict:
    conn = get_conn()
    total     = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    by_status = dict(conn.execute(
        "SELECT email_status, COUNT(*) FROM contacts GROUP BY email_status"
    ).fetchall())
    by_source = dict(conn.execute(
        "SELECT source, COUNT(*) FROM contacts GROUP BY source ORDER BY COUNT(*) DESC LIMIT 10"
    ).fetchall())
    co        = companies_stats()
    conn.close()
    return {
        "total":      total,
        "by_status":  by_status,
        "by_source":  by_source,
        "companies":  co,
    }

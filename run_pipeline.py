"""
Lead generation pipeline — manual step runner.

STEPS (run in order, or independently):

  Step 1 — Discover companies
    python run_pipeline.py --step discover
    Searches tech directories, GitHub orgs, Wikipedia, G2, Capterra, etc.
    Adds new company domains to the queue. Fast (~2 min).

  Step 2 — Scrape company websites
    python run_pipeline.py --step scrape [--limit 500]
    Visits /team /about /leadership pages of queued companies.
    Default: 500 companies per run. Run repeatedly to process the full queue.

  Step 3 — Find people (social/conference sources)
    python run_pipeline.py --step people
    Runs: LinkedIn, conferences, ProductHunt, Wellfound.

  Step 4 — Find people via paid APIs (optional, needs keys in .env)
    python run_pipeline.py --step apis
    Runs: GitHub, Crunchbase, Hunter, Apollo.

  Step 5 — Find & verify emails for no-email contacts
    python run_pipeline.py --step verify [--smtp]

  Utilities:
    python run_pipeline.py --stats
    python run_pipeline.py --export
    python run_pipeline.py --import-csv output/final_database.csv
    python run_pipeline.py --queue-status   (show companies queue count)
"""

import argparse
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline import db as DB
from pipeline.orchestrator import run_source, run_all, ALL_SOURCES, FREE_SOURCES, CONFIG


# ── Step definitions ──────────────────────────────────────────────────────────

STEPS = {
    "discover": {
        "sources": ["company_discovery"],
        "desc":    "Discover new tech companies and add them to the scrape queue",
    },
    "scrape": {
        "sources": ["web_scraper"],
        "desc":    "Scrape company websites for team/leadership pages",
    },
    "wikidata": {
        "sources": ["wikidata_people"],
        "desc":    "Bulk Wikidata SPARQL — fetch CEO/Founder/COO for known companies (free)",
    },
    "enrich": {
        "sources": ["ddg_enrich"],
        "desc":    "DuckDuckGo Instant API — enrich companies with 0 contacts found so far",
    },
    "mine": {
        "sources": ["bs4_miner"],
        "desc":    "BeautifulSoup email miner — extract contact emails from exhausted company sites",
    },
    "ceo": {
        "sources": ["ceo_finder"],
        "desc":    "DDG + Ollama CEO finder — find top executive for companies with no contacts yet",
    },
    "emails": {
        "sources": ["email_finder_web"],
        "desc":    "DDG email finder — search web for real emails for _noemail_ contacts",
    },
    "people": {
        "sources": ["conferences", "linkedin", "producthunt", "wellfound"],
        "desc":    "Find decision-makers from LinkedIn, conferences, ProductHunt, Wellfound",
    },
    "apis": {
        "sources": ["github", "crunchbase", "hunter", "apollo"],
        "desc":    "Find contacts via paid/key APIs (GitHub, Crunchbase, Hunter, Apollo)",
    },
}


# ── Utilities ─────────────────────────────────────────────────────────────────

def import_csv(csv_path: str):
    import csv
    DB.init_db()
    inserted = 0
    skipped  = 0

    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row   = {k.strip(): v.strip() for k, v in row.items()}
            email = row.get("Email") or row.get("email") or ""
            if not email or "@" not in email:
                skipped += 1
                continue

            result = DB.upsert_contact({
                "first_name":     row.get("First Name") or row.get("first_name", ""),
                "last_name":      row.get("Last Name")  or row.get("last_name", ""),
                "title":          row.get("Title")      or row.get("title", ""),
                "company":        row.get("Company")    or row.get("company", ""),
                "company_domain": email.split("@")[1] if "@" in email else "",
                "email":          email,
                "email_status":   row.get("Email Status") or row.get("email_status", "unverified"),
                "country":        row.get("Country") or row.get("country", ""),
                "linkedin_url":   row.get("LinkedIn") or row.get("linkedin_url", ""),
                "source":         "import",
            })
            if result == "inserted":
                inserted += 1
            else:
                skipped += 1

    print(f"Import complete: {inserted} inserted, {skipped} skipped/updated")


def show_stats():
    DB.init_db()
    s = DB.stats()
    print(f"\nLeads Database Stats")
    print(f"{'='*40}")
    print(f"Total contacts: {s['total']}")
    print(f"\nBy email status:")
    for status, n in s.get("by_status", {}).items():
        print(f"  {status:<22}: {n}")
    print(f"\nBy source (top 10):")
    for src, n in s.get("by_source", {}).items():
        print(f"  {src:<22}: {n}")


def show_queue_status():
    DB.init_db()
    import sqlite3
    conn = sqlite3.connect("output/leads.db")
    total    = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    pending  = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=0").fetchone()[0]
    scraped  = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=1").fetchone()[0]
    conn.close()
    print(f"\nCompany Queue Status")
    print(f"{'='*40}")
    print(f"  Total in queue : {total}")
    print(f"  Pending scrape : {pending}")
    print(f"  Already scraped: {scraped}")


def verify_db(batch_size: int = 200, workers: int = 30, smtp: bool = True):
    """Re-verify ALL contacts (any status) and update their status in the DB."""
    import sqlite3
    from tqdm import tqdm
    from pipeline.verifier import verify_batch

    DB.init_db()
    conn = sqlite3.connect("output/leads.db")
    rows = conn.execute("""
        SELECT id, email FROM contacts
        WHERE email NOT LIKE '_noemail_%'
          AND email LIKE '%@%'
          AND email_status NOT IN ('undeliverable')
        ORDER BY id
    """).fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        print("No contacts to verify.")
        return

    mode = "SMTP" if smtp else "MX-only"
    print(f"\nVerifying {total:,} contacts  |  mode={mode}  workers={workers}  batch={batch_size}")
    print(f"{'='*60}")

    deliverable   = 0
    undeliverable = 0
    risky         = 0
    unknown       = 0

    bar = tqdm(
        total=total,
        unit="email",
        ncols=80,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    )

    for i in range(0, total, batch_size):
        batch   = rows[i : i + batch_size]
        emails  = [r[1] for r in batch]
        id_map  = {r[1]: r[0] for r in batch}
        results = verify_batch(emails, workers=workers, smtp=smtp)

        conn = sqlite3.connect("output/leads.db")
        for res in results:
            status = res["status"]
            cid    = id_map.get(res["email"])
            if cid:
                conn.execute(
                    "UPDATE contacts SET email_status=?, verified_at=datetime('now') WHERE id=?",
                    (status, cid),
                )
            if   status == "deliverable":   deliverable   += 1
            elif status == "undeliverable": undeliverable += 1
            elif status == "risky":         risky         += 1
            else:                           unknown       += 1
        conn.commit()
        conn.close()

        bar.update(len(batch))
        bar.set_postfix(
            ok=deliverable, bad=undeliverable, risky=risky, unk=unknown, refresh=False
        )

    bar.close()

    print(f"\n{'='*60}")
    print(f"  Deliverable   : {deliverable:,}")
    print(f"  Undeliverable : {undeliverable:,}")
    print(f"  Risky         : {risky:,}")
    print(f"  Unknown       : {unknown:,}")
    print(f"  TOTAL         : {total:,}")


# ── Full enrichment pipeline ──────────────────────────────────────────────────

def _full_enrich(smtp: bool = False, workers: int = 30):
    import time

    steps = [
        ("Wikidata executives",        "~5 min",   None),
        ("CEO-Finder (62k companies)", "~62 hrs",  None),
        ("Email-Finder-Web (531)",     "~30 min",  None),
        ("DB Enrich — names/companies","~5 min",   None),
        ("Email Verification",         "~25 min",  None),
        ("Web Search (DDG + Groq)",    "~3.5 hrs", None),
        ("Country enrichment",         "~47 min",  None),
        ("Reverse Email (GitHub)",     "~19 hrs",  None),
    ]

    print("\n" + "="*60)
    print("  FULL ENRICHMENT PIPELINE")
    print("="*60)
    print("  Steps and estimated time:")
    total_mins = 0
    for i, (name, eta, _) in enumerate(steps, 1):
        print(f"  {i}. {name:<35} {eta}")
    print("  " + "-"*56)
    print("  Total estimated time: ~85 hours (run over several nights)")
    print("="*60 + "\n")

    def banner(step_num, name, eta):
        print(f"\n{'='*60}")
        print(f"  STEP {step_num}/8 — {name}  (ETA: {eta})")
        print(f"{'='*60}")

    DB.init_db()

    # 1 — Wikidata
    banner(1, "Wikidata executives", "~5 min")
    run_all(sources=["wikidata_people"], use_smtp=False)

    # 2 — CEO-Finder
    banner(2, "CEO-Finder (DDG + Groq)", "~30 min")
    run_all(sources=["ceo_finder"], use_smtp=False)

    # 3 — Email-Finder-Web
    banner(3, "Email-Finder-Web", "~30 min")
    run_all(sources=["email_finder_web"], use_smtp=False)

    # 4 — DB Enrich (names + companies, no countries yet)
    banner(4, "DB Enrich — names / companies", "~5 min")
    from pipeline.db_enricher import run as enrich_run
    enrich_run(skip_countries=True)

    # 5 — Email verification
    banner(5, "Email Verification", "~25 min")
    verify_db(smtp=smtp)

    # 6 — Web Search enrichment (DDG + Groq for all contacts)
    banner(6, "Web Search Enrichment (DDG + Groq)", "~3.5 hrs")
    from pipeline.web_searcher import run as search_run
    search_run(workers=3)

    # 7 — Country enrichment via ip-api.com
    banner(7, "Country enrichment (ip-api.com)", "~47 min")
    enrich_run(skip_countries=False)

    # 8 — Reverse email lookup (GitHub)
    banner(8, "Reverse Email Lookup (GitHub)", "~19 hrs")
    from pipeline.reverse_enricher import run as reverse_run
    reverse_run(skip_github=False, use_gravatar=False)

    print("\n" + "="*60)
    print("  FULL ENRICHMENT COMPLETE")
    print("="*60)
    show_stats()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Lead generation pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(
            f"  --step {name:<10}  {info['desc']}"
            for name, info in STEPS.items()
        )
    )
    parser.add_argument("--step",        choices=list(STEPS.keys()),
                        help="Run a named pipeline step")
    parser.add_argument("--sources",     nargs="+", choices=list(ALL_SOURCES.keys()),
                        help="Run specific sources by name")
    parser.add_argument("--limit",       type=int, default=500,
                        help="Max companies to scrape per web_scraper run (default: 500)")
    parser.add_argument("--workers",     type=int, default=30,
                        help="Parallel workers for web scraper (default: 30)")
    parser.add_argument("--smtp",        action="store_true",
                        help="Enable SMTP email verification (slower, more accurate)")
    parser.add_argument("--stats",       action="store_true",
                        help="Show database stats and exit")
    parser.add_argument("--queue-status", action="store_true",
                        help="Show company scrape queue status and exit")
    parser.add_argument("--import-csv",  metavar="FILE",
                        help="Import existing contacts CSV into DB")
    parser.add_argument("--export",      action="store_true",
                        help="Export DB to CSV and exit")
    parser.add_argument("--verify-db",   action="store_true",
                        help="SMTP-verify all confirmed/unverified contacts in the DB")
    parser.add_argument("--enrich-db",   action="store_true",
                        help="Enrich DB: fix names, fill company names/countries")
    parser.add_argument("--no-countries", action="store_true",
                        help="Skip country enrichment when using --enrich-db")
    parser.add_argument("--search-db",     action="store_true",
                        help="Web-search every contact (DDG + Ollama) to enrich name/title/company/country")
    parser.add_argument("--reverse-enrich", action="store_true",
                        help="Reverse email lookup via Gravatar + GitHub to fill missing name/country/social URLs")
    parser.add_argument("--skip-github",   action="store_true",
                        help="Skip GitHub search when running --reverse-enrich")
    parser.add_argument("--use-gravatar",  action="store_true",
                        help="Also query Gravatar during --reverse-enrich (slow: 100 req/hr)")
    parser.add_argument("--full-enrich",   action="store_true",
                        help="Run ALL enrichment steps in sequence (see ETA below)")
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if args.queue_status:
        show_queue_status()
        return

    if args.import_csv:
        import_csv(args.import_csv)
        return

    if args.export:
        DB.init_db()
        n = DB.export_csv()
        print(f"Exported {n} contacts to output/leads_database.csv")
        return

    if args.verify_db:
        verify_db(smtp=args.smtp)
        return

    if args.enrich_db:
        from pipeline.db_enricher import run as enrich_run
        enrich_run(skip_countries=args.no_countries)
        return

    if args.search_db:
        DB.init_db()
        from pipeline.web_searcher import run as search_run
        search_run(workers=args.workers)
        return

    if args.reverse_enrich:
        from pipeline.reverse_enricher import run as reverse_run
        reverse_run(skip_github=args.skip_github, use_gravatar=args.use_gravatar)
        return

    if args.full_enrich:
        _full_enrich(smtp=args.smtp, workers=args.workers)
        return

    # Determine which sources to run
    if args.step:
        sources = STEPS[args.step]["sources"]
        print(f"\nStep: {args.step.upper()} — {STEPS[args.step]['desc']}")
    elif args.sources:
        sources = args.sources
    else:
        parser.print_help()
        return

    # Pass scraper config into CONFIG
    CONFIG["SCRAPER_LIMIT"]   = str(args.limit)
    CONFIG["SCRAPER_WORKERS"] = str(args.workers)

    DB.init_db()
    run_all(sources=sources, use_smtp=args.smtp)


if __name__ == "__main__":
    main()

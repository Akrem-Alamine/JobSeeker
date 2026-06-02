"""
Pipeline orchestrator — runs all sources, deduplicates, verifies,
and saves results to the leads database.
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

from . import db as DB
from .verifier import verify_batch
from .email_finder import find_email
from .sources.github             import GitHubSource
from .sources.crunchbase         import CrunchbaseSource
from .sources.hunter             import HunterSource
from .sources.web_scraper        import WebScraperSource
from .sources.conferences        import ConferencesSource
from .sources.linkedin           import LinkedInSource
from .sources.apollo             import ApolloSource
from .sources.producthunt        import ProductHuntSource
from .sources.wellfound          import WellfoundSource
from .sources.company_discovery  import CompanyDiscoverySource
from .sources.wikidata_people    import WikidataPeopleSource
from .sources.ddg_enrich         import DDGEnrichSource
from .sources.bs4_miner            import BS4MinerSource
from .sources.ceo_finder           import CEOFinderSource
from .sources.email_finder_web     import EmailFinderWebSource
from .sources.reverse_name_lookup  import ReverseNameLookupSource

load_dotenv(Path(".env"))

CONFIG = {
    "GITHUB_TOKEN":    os.getenv("GITHUB_TOKEN", ""),
    "CRUNCHBASE_KEY":  os.getenv("CRUNCHBASE_KEY", ""),
    "HUNTER_KEY":      os.getenv("HUNTER_KEY", ""),
    "APOLLO_KEY":      os.getenv("APOLLO_KEY", ""),
}

ALL_SOURCES = {
    "company_discovery": CompanyDiscoverySource,
    "wikidata_people":   WikidataPeopleSource,
    "ddg_enrich":        DDGEnrichSource,
    "bs4_miner":         BS4MinerSource,
    "ceo_finder":          CEOFinderSource,
    "email_finder_web":    EmailFinderWebSource,
    "reverse_name_lookup": ReverseNameLookupSource,
    "github":      GitHubSource,
    "crunchbase":  CrunchbaseSource,
    "hunter":      HunterSource,
    "web_scraper": WebScraperSource,
    "conferences": ConferencesSource,
    "linkedin":    LinkedInSource,
    "apollo":      ApolloSource,
    "producthunt": ProductHuntSource,
    "wellfound":   WellfoundSource,
}

# Sources that work with no API key
FREE_SOURCES = {"company_discovery", "wikidata_people", "ddg_enrich", "bs4_miner", "ceo_finder", "email_finder_web", "reverse_name_lookup", "web_scraper", "conferences", "linkedin", "producthunt", "wellfound"}


def _needs_key(source_name: str) -> bool:
    cls = ALL_SOURCES.get(source_name)
    return getattr(cls, "requires_key", False) if cls else False


def _key_available(source_name: str) -> bool:
    key_map = {
        "github":     "GITHUB_TOKEN",
        "crunchbase": "CRUNCHBASE_KEY",
        "hunter":     "HUNTER_KEY",
        "apollo":     "APOLLO_KEY",
    }
    key = key_map.get(source_name, "")
    return bool(CONFIG.get(key, ""))


def run_source(source_name: str, use_smtp: bool = False) -> dict:
    cls = ALL_SOURCES.get(source_name)
    if not cls:
        return {"source": source_name, "status": "unknown_source"}

    if _needs_key(source_name) and not _key_available(source_name):
        print(f"\n[{source_name}] Skipped — no API key configured")
        return {"source": source_name, "status": "skipped_no_key"}

    started = datetime.now(timezone.utc).isoformat()
    print(f"\n{'='*60}")
    print(f"Source: {source_name.upper()}  [{started[:19]}]")
    print(f"{'='*60}")

    try:
        source   = cls(CONFIG)
        contacts = source.run()
    except Exception as e:
        print(f"  [{source_name}] Fatal error: {e}")
        DB.log_run(source_name, started, 0, 0, 1, "error", str(e))
        return {"source": source_name, "status": "error", "error": str(e)}

    print(f"  [{source_name}] Raw contacts fetched: {len(contacts)}")

    # Filter to target titles only
    from .db import is_target_title
    targeted = [c for c in contacts if is_target_title(c.get("title", ""))]
    print(f"  [{source_name}] After title filter: {len(targeted)}")

    # Deduplicate against existing DB
    new_contacts = [c for c in targeted if not DB.email_exists(c.get("email", "")) or not c.get("email")]
    print(f"  [{source_name}] New (not in DB): {len(new_contacts)}")

    # For contacts without email, try to find one
    need_email = [c for c in new_contacts if not c.get("email") and c.get("first_name") and c.get("company_domain")]
    if need_email:
        print(f"  [{source_name}] Finding emails for {len(need_email)} contacts...")
        for c in need_email:
            email = find_email(c["first_name"], c["last_name"], c["company_domain"], use_smtp=False)
            if email:
                c["email"]        = email
                c["email_status"] = "unverified"

    # Verify emails for contacts that have one
    has_email = [c for c in new_contacts if c.get("email")]
    if has_email:
        print(f"  [{source_name}] Verifying {len(has_email)} emails...")
        emails     = [c["email"] for c in has_email]
        ver_results = {r["email"]: r["status"] for r in verify_batch(emails, workers=20, smtp=use_smtp)}
        for c in has_email:
            c["email_status"] = ver_results.get(c["email"], "unverified")

    # Remove confirmed undeliverable
    valid = [c for c in new_contacts if c.get("email_status") != "undeliverable"]
    print(f"  [{source_name}] After verification: {len(valid)} contacts")

    # Save to database
    inserted = 0
    updated  = 0
    for c in valid:
        result = DB.upsert_contact(c)
        if result == "inserted":
            inserted += 1
        elif result == "updated":
            updated += 1

    print(f"  [{source_name}] Saved: {inserted} new, {updated} updated")
    DB.log_run(source_name, started, inserted, updated, 0, "success")

    return {
        "source":   source_name,
        "fetched":  len(contacts),
        "targeted": len(targeted),
        "new":      len(new_contacts),
        "inserted": inserted,
        "updated":  updated,
        "status":   "success",
    }


def run_all(sources: list[str] = None, use_smtp: bool = False) -> list[dict]:
    DB.init_db()

    if sources is None:
        sources = list(ALL_SOURCES.keys())

    results = []
    for name in sources:
        result = run_source(name, use_smtp=use_smtp)
        results.append(result)

    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE")
    print(f"{'='*60}")

    s = DB.stats()
    print(f"\nDatabase stats:")
    print(f"  Total contacts : {s['total']}")
    for status, n in s.get("by_status", {}).items():
        print(f"  {status:<20}: {n}")
    print(f"\nBy source:")
    for src, n in s.get("by_source", {}).items():
        print(f"  {src:<20}: {n}")

    return results

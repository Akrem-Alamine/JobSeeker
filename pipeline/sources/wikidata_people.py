"""
Wikidata Executive Source — bulk SPARQL query for CEO/Founder/COO/CTO
of tech companies. Matches results against domains already in our DB.
"""

import re
import time

import requests

from .base import BaseSource

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
SPARQL_HEADERS  = {
    "User-Agent": "LeadPipeline/1.0 (contact: pipeline@local)",
    "Accept":     "application/json",
}

COMPANY_TYPES = [
    ("wd:Q783794",  "software company"),
    ("wd:Q1194970", "IT company"),
    ("wd:Q902198",  "internet company"),
]

# Wikidata property → job title
EXEC_ROLES = [
    ("wdt:P169",  "Chief Executive Officer"),
    ("wdt:P112",  "Co-Founder"),
    ("wdt:P3320", "Board Member"),
    ("wdt:P1789", "Chief Operating Officer"),
    ("wdt:P5769", "Editor-in-Chief"),
]

PAGE_SIZE  = 1000
MAX_PAGES  = 30
SLEEP_BETWEEN_PAGES = 4


def _extract_domain(url: str) -> str:
    url = url.lower().strip()
    for prefix in ("https://", "http://", "www."):
        if url.startswith(prefix):
            url = url[len(prefix):]
    return url.rstrip("/").split("/")[0].split("?")[0]


def _sparql(query: str) -> list[dict]:
    try:
        r = requests.get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            headers=SPARQL_HEADERS,
            timeout=60,
        )
        if r.status_code == 429:
            time.sleep(30)
            return []
        if r.status_code != 200:
            return []
        return r.json().get("results", {}).get("bindings", [])
    except Exception:
        return []


class WikidataPeopleSource(BaseSource):
    name         = "wikidata_people"
    requires_key = False

    def fetch(self) -> list[dict]:
        from pipeline import db as DB

        known_domains = DB.get_known_domains()
        print(f"  [WikidataPeople] Querying executives for {len(known_domains):,} known domains")

        unions = "\n    UNION\n    ".join(
            f'{{ ?company {prop} ?person . BIND("{title}" AS ?title) }}'
            for prop, title in EXEC_ROLES
        )

        contacts = []
        seen = set()

        for type_id, type_label in COMPANY_TYPES:
            print(f"  [WikidataPeople] Type: {type_label}")
            for page in range(MAX_PAGES):
                offset = page * PAGE_SIZE
                query  = f"""
                SELECT ?website ?personLabel ?title WHERE {{
                  ?company wdt:P31/wdt:P279* {type_id} .
                  ?company wdt:P856 ?website .
                  {{
                    {unions}
                  }}
                  SERVICE wikibase:label {{
                    bd:serviceParam wikibase:language "en" .
                    ?person rdfs:label ?personLabel .
                  }}
                }}
                LIMIT {PAGE_SIZE} OFFSET {offset}
                """

                bindings = _sparql(query)
                if not bindings:
                    break

                for b in bindings:
                    website = b.get("website", {}).get("value", "")
                    domain  = _extract_domain(website)
                    if not domain or domain not in known_domains:
                        continue

                    name  = b.get("personLabel", {}).get("value", "").strip()
                    title = b.get("title",       {}).get("value", "").strip()

                    if not name or not title:
                        continue
                    if re.match(r"^Q\d+$", name):  # Wikidata entity ID, no label
                        continue

                    key = f"{name.lower()}|{domain}"
                    if key in seen:
                        continue
                    seen.add(key)

                    parts = name.split(" ", 1)
                    contacts.append({
                        "first_name":     parts[0],
                        "last_name":      parts[1] if len(parts) > 1 else "",
                        "full_name":      name,
                        "title":          title,
                        "company":        domain,
                        "company_domain": domain,
                        "email":          "",
                        "source":         "wikidata_people",
                        "tags":           ["wikidata"],
                    })

                print(f"    offset={offset:>6}: {len(bindings)} rows → {len(contacts)} matched so far")

                if len(bindings) < PAGE_SIZE:
                    break
                time.sleep(SLEEP_BETWEEN_PAGES)

        print(f"  [WikidataPeople] Total executives found: {len(contacts)}")
        return contacts

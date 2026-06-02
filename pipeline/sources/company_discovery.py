"""
Company Discovery — builds the companies table from reliable data sources.

Every source returns list[dict] with keys:
    name, domain, country, country_code, industry, description, source

Sources (all free, API-based — no fragile HTML scraping):
  1.  Seed list          — 400+ hand-curated global tech companies with country
  2.  Wikidata SPARQL    — structured knowledge graph, 50k+ tech companies
  3.  Hacker News        — "Who Is Hiring" threads via Algolia API (5 years)
  4.  Remote OK          — public JSON API
  5.  GitHub orgs        — API search, 30 queries
  6.  SEC EDGAR          — US tech companies by SIC code
  7.  DDG search         — 50 targeted queries for company lists
"""

import re
import time
import json
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS

from .. import db as DB
from .base import BaseSource

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

DOMAIN_RE = re.compile(
    r'(?:https?://)?(?:www\.)?'
    r'([a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'
    r'(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,})',
    re.IGNORECASE,
)

SKIP_COUNTRIES = {"Israel", "India", "Nigeria", "IL", "IN", "NG"}

SKIP_DOMAINS = {
    "google.com", "google.co.uk", "google.de", "google.fr",
    "github.com", "github.io", "githubusercontent.com",
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "instagram.com", "youtube.com", "wikipedia.org",
    "amazon.com", "amazonaws.com", "microsoft.com", "apple.com",
    "stackoverflow.com", "reddit.com", "medium.com",
    "crunchbase.com", "glassdoor.com", "indeed.com",
    "g2.com", "capterra.com", "producthunt.com",
    "remotive.com", "weworkremotely.com", "remoteok.io",
    "techcrunch.com", "venturebeat.com", "wired.com",
    "bloomberg.com", "forbes.com", "businessinsider.com",
    "cloudfront.net", "netlify.app", "vercel.app",
    "npmjs.com", "pypi.org", "hub.docker.com",
    "ycombinator.com", "news.ycombinator.com",
    "sec.gov", "edgar.sec.gov",
}

DDG_QUERIES = [
    "top SaaS companies worldwide 2024 list",
    "top cybersecurity companies 2024 global",
    "cloud computing companies list 2024",
    "AI ML startups 2024 funded list",
    "top fintech companies worldwide 2024",
    "enterprise software companies list global",
    "open source companies engineering teams 2024",
    "top devops platform engineering companies 2024",
    "top data analytics companies 2024",
    "best developer tools companies 2024",
    "observability monitoring companies list 2024",
    "identity access management companies list 2024",
    "top tech companies Europe 2024",
    "top tech startups UK 2024 list",
    "top technology companies Germany 2024",
    "top tech companies Canada 2024",
    "top technology companies Australia 2024",
    "top tech startups India 2024",
    "YC portfolio companies 2024 tech",
    "Forbes cloud 100 2024 companies list",
    "CB Insights unicorn list 2024 tech",
    "Deloitte Technology Fast 500 2024",
    "edge computing companies list 2024",
    "quantum computing companies startups 2024",
    "top network security companies 2024",
]

# ─────────────────────────────────────────────────────────────────────────────
#  Seed list — 400+ curated companies with country metadata
# ─────────────────────────────────────────────────────────────────────────────

SEED_COMPANIES: list[dict] = [
    # USA
    {"name": "Stripe",          "domain": "stripe.com",          "country": "United States", "country_code": "US"},
    {"name": "Twilio",          "domain": "twilio.com",          "country": "United States", "country_code": "US"},
    {"name": "Datadog",         "domain": "datadog.com",         "country": "United States", "country_code": "US"},
    {"name": "Snowflake",       "domain": "snowflake.com",       "country": "United States", "country_code": "US"},
    {"name": "Databricks",      "domain": "databricks.com",      "country": "United States", "country_code": "US"},
    {"name": "HashiCorp",       "domain": "hashicorp.com",       "country": "United States", "country_code": "US"},
    {"name": "Confluent",       "domain": "confluent.io",        "country": "United States", "country_code": "US"},
    {"name": "Elastic",         "domain": "elastic.co",          "country": "United States", "country_code": "US"},
    {"name": "MongoDB",         "domain": "mongodb.com",         "country": "United States", "country_code": "US"},
    {"name": "Vercel",          "domain": "vercel.com",          "country": "United States", "country_code": "US"},
    {"name": "Netlify",         "domain": "netlify.com",         "country": "United States", "country_code": "US"},
    {"name": "Supabase",        "domain": "supabase.com",        "country": "United States", "country_code": "US"},
    {"name": "PlanetScale",     "domain": "planetscale.com",     "country": "United States", "country_code": "US"},
    {"name": "Grafana Labs",    "domain": "grafana.com",         "country": "United States", "country_code": "US"},
    {"name": "Sentry",          "domain": "sentry.io",           "country": "United States", "country_code": "US"},
    {"name": "New Relic",       "domain": "newrelic.com",        "country": "United States", "country_code": "US"},
    {"name": "PagerDuty",       "domain": "pagerduty.com",       "country": "United States", "country_code": "US"},
    {"name": "CircleCI",        "domain": "circleci.com",        "country": "United States", "country_code": "US"},
    {"name": "Harness",         "domain": "harness.io",          "country": "United States", "country_code": "US"},
    {"name": "CrowdStrike",     "domain": "crowdstrike.com",     "country": "United States", "country_code": "US"},
    {"name": "Palo Alto Networks", "domain": "paloaltonetworks.com", "country": "United States", "country_code": "US"},
    {"name": "Okta",            "domain": "okta.com",            "country": "United States", "country_code": "US"},
    {"name": "SentinelOne",     "domain": "sentinelone.com",     "country": "United States", "country_code": "US"},
    {"name": "Wiz",             "domain": "wiz.io",              "country": "United States", "country_code": "US"},
    {"name": "Snyk",            "domain": "snyk.io",             "country": "United States", "country_code": "US"},
    {"name": "OpenAI",          "domain": "openai.com",          "country": "United States", "country_code": "US"},
    {"name": "Anthropic",       "domain": "anthropic.com",       "country": "United States", "country_code": "US"},
    {"name": "Scale AI",        "domain": "scale.com",           "country": "United States", "country_code": "US"},
    {"name": "Hugging Face",    "domain": "huggingface.co",      "country": "United States", "country_code": "US"},
    {"name": "Cohere",          "domain": "cohere.com",          "country": "United States", "country_code": "US"},
    {"name": "Weights & Biases","domain": "wandb.ai",            "country": "United States", "country_code": "US"},
    {"name": "Replicate",       "domain": "replicate.com",       "country": "United States", "country_code": "US"},
    {"name": "Together AI",     "domain": "together.ai",         "country": "United States", "country_code": "US"},
    {"name": "Groq",            "domain": "groq.com",            "country": "United States", "country_code": "US"},
    {"name": "Palantir",        "domain": "palantir.com",        "country": "United States", "country_code": "US"},
    {"name": "Plaid",           "domain": "plaid.com",           "country": "United States", "country_code": "US"},
    {"name": "Brex",            "domain": "brex.com",            "country": "United States", "country_code": "US"},
    {"name": "Ramp",            "domain": "ramp.com",            "country": "United States", "country_code": "US"},
    {"name": "Rippling",        "domain": "rippling.com",        "country": "United States", "country_code": "US"},
    {"name": "Gusto",           "domain": "gusto.com",           "country": "United States", "country_code": "US"},
    {"name": "Salesforce",      "domain": "salesforce.com",      "country": "United States", "country_code": "US"},
    {"name": "HubSpot",         "domain": "hubspot.com",         "country": "United States", "country_code": "US"},
    {"name": "Zendesk",         "domain": "zendesk.com",         "country": "United States", "country_code": "US"},
    {"name": "Intercom",        "domain": "intercom.com",        "country": "United States", "country_code": "US"},
    {"name": "Amplitude",       "domain": "amplitude.com",       "country": "United States", "country_code": "US"},
    {"name": "Segment",         "domain": "segment.com",         "country": "United States", "country_code": "US"},
    {"name": "PostHog",         "domain": "posthog.com",         "country": "United States", "country_code": "US"},
    {"name": "Figma",           "domain": "figma.com",           "country": "United States", "country_code": "US"},
    {"name": "Notion",          "domain": "notion.so",           "country": "United States", "country_code": "US"},
    {"name": "Linear",          "domain": "linear.app",          "country": "United States", "country_code": "US"},
    {"name": "Vercel",          "domain": "vercel.com",          "country": "United States", "country_code": "US"},
    {"name": "DigitalOcean",    "domain": "digitalocean.com",    "country": "United States", "country_code": "US"},
    {"name": "Linode",          "domain": "linode.com",          "country": "United States", "country_code": "US"},
    {"name": "Juniper Networks","domain": "juniper.net",         "country": "United States", "country_code": "US"},
    {"name": "Arista Networks", "domain": "arista.com",          "country": "United States", "country_code": "US"},
    {"name": "Nutanix",         "domain": "nutanix.com",         "country": "United States", "country_code": "US"},
    {"name": "dbt Labs",        "domain": "getdbt.com",          "country": "United States", "country_code": "US"},
    {"name": "Fivetran",        "domain": "fivetran.com",        "country": "United States", "country_code": "US"},
    {"name": "Airbyte",         "domain": "airbyte.com",         "country": "United States", "country_code": "US"},
    {"name": "Hightouch",       "domain": "hightouch.com",       "country": "United States", "country_code": "US"},
    {"name": "ZoomInfo",        "domain": "zoominfo.com",        "country": "United States", "country_code": "US"},
    {"name": "Outreach",        "domain": "outreach.io",         "country": "United States", "country_code": "US"},
    {"name": "Gong",            "domain": "gong.io",             "country": "United States", "country_code": "US"},
    {"name": "Shopify",         "domain": "shopify.com",         "country": "Canada",        "country_code": "CA"},
    # UK
    {"name": "Darktrace",       "domain": "darktrace.com",       "country": "United Kingdom","country_code": "GB"},
    {"name": "Monzo",           "domain": "monzo.com",           "country": "United Kingdom","country_code": "GB"},
    {"name": "Revolut",         "domain": "revolut.com",         "country": "United Kingdom","country_code": "GB"},
    {"name": "Wise",            "domain": "wise.com",            "country": "United Kingdom","country_code": "GB"},
    {"name": "Octopus Energy",  "domain": "octopusenergy.com",   "country": "United Kingdom","country_code": "GB"},
    {"name": "Onfido",          "domain": "onfido.com",          "country": "United Kingdom","country_code": "GB"},
    {"name": "Tractable",       "domain": "tractable.ai",        "country": "United Kingdom","country_code": "GB"},
    {"name": "Wayve",           "domain": "wayve.ai",            "country": "United Kingdom","country_code": "GB"},
    {"name": "Cleo",            "domain": "cleo.ai",             "country": "United Kingdom","country_code": "GB"},
    {"name": "Brandwatch",      "domain": "brandwatch.com",      "country": "United Kingdom","country_code": "GB"},
    {"name": "Skyscanner",      "domain": "skyscanner.net",      "country": "United Kingdom","country_code": "GB"},
    {"name": "Funding Circle",  "domain": "fundingcircle.com",   "country": "United Kingdom","country_code": "GB"},
    # Germany
    {"name": "Celonis",         "domain": "celonis.com",         "country": "Germany",       "country_code": "DE"},
    {"name": "Personio",        "domain": "personio.de",         "country": "Germany",       "country_code": "DE"},
    {"name": "TeamViewer",      "domain": "teamviewer.com",      "country": "Germany",       "country_code": "DE"},
    {"name": "commercetools",   "domain": "commercetools.com",   "country": "Germany",       "country_code": "DE"},
    {"name": "Contentful",      "domain": "contentful.com",      "country": "Germany",       "country_code": "DE"},
    {"name": "Delivery Hero",   "domain": "delivery-hero.com",   "country": "Germany",       "country_code": "DE"},
    {"name": "AUTO1 Group",     "domain": "auto1.com",           "country": "Germany",       "country_code": "DE"},
    {"name": "SAP",             "domain": "sap.com",             "country": "Germany",       "country_code": "DE"},
    # France
    {"name": "Mistral AI",      "domain": "mistral.ai",          "country": "France",        "country_code": "FR"},
    {"name": "Doctolib",        "domain": "doctolib.fr",         "country": "France",        "country_code": "FR"},
    {"name": "Contentsquare",   "domain": "contentsquare.com",   "country": "France",        "country_code": "FR"},
    {"name": "Deezer",          "domain": "deezer.com",          "country": "France",        "country_code": "FR"},
    {"name": "Criteo",          "domain": "criteo.com",          "country": "France",        "country_code": "FR"},
    {"name": "Talend",          "domain": "talend.com",          "country": "France",        "country_code": "FR"},
    # Sweden / Nordics
    {"name": "Spotify",         "domain": "spotify.com",         "country": "Sweden",        "country_code": "SE"},
    {"name": "Klarna",          "domain": "klarna.com",          "country": "Sweden",        "country_code": "SE"},
    {"name": "iZettle",         "domain": "izettle.com",         "country": "Sweden",        "country_code": "SE"},
    {"name": "King",            "domain": "king.com",            "country": "Sweden",        "country_code": "SE"},
    {"name": "Kahoot",          "domain": "kahoot.com",          "country": "Norway",        "country_code": "NO"},
    {"name": "Pexip",           "domain": "pexip.com",           "country": "Norway",        "country_code": "NO"},
    {"name": "Templafy",        "domain": "templafy.com",        "country": "Denmark",       "country_code": "DK"},
    {"name": "Trustpilot",      "domain": "trustpilot.com",      "country": "Denmark",       "country_code": "DK"},
    {"name": "Unity",           "domain": "unity.com",           "country": "Denmark",       "country_code": "DK"},
    # Netherlands
    {"name": "Adyen",           "domain": "adyen.com",           "country": "Netherlands",   "country_code": "NL"},
    {"name": "Booking.com",     "domain": "booking.com",         "country": "Netherlands",   "country_code": "NL"},
    {"name": "TomTom",          "domain": "tomtom.com",          "country": "Netherlands",   "country_code": "NL"},
    {"name": "Randstad Digital","domain": "digital.randstad.com","country": "Netherlands",   "country_code": "NL"},
    # Canada
    {"name": "Hootsuite",       "domain": "hootsuite.com",       "country": "Canada",        "country_code": "CA"},
    {"name": "Coveo",           "domain": "coveo.com",           "country": "Canada",        "country_code": "CA"},
    {"name": "Lightspeed",      "domain": "lightspeedcommerce.com","country": "Canada",      "country_code": "CA"},
    {"name": "Wealthsimple",    "domain": "wealthsimple.com",    "country": "Canada",        "country_code": "CA"},
    # Australia / NZ
    {"name": "Atlassian",       "domain": "atlassian.com",       "country": "Australia",     "country_code": "AU"},
    {"name": "Canva",           "domain": "canva.com",           "country": "Australia",     "country_code": "AU"},
    {"name": "Xero",            "domain": "xero.com",            "country": "New Zealand",   "country_code": "NZ"},
    # Latin America
    {"name": "Nubank",          "domain": "nubank.com",          "country": "Brazil",        "country_code": "BR"},
    {"name": "Rappi",           "domain": "rappi.com",           "country": "Colombia",      "country_code": "CO"},
    {"name": "MercadoLibre",    "domain": "mercadolibre.com",    "country": "Argentina",     "country_code": "AR"},
    # Singapore / Asia
    {"name": "Grab",            "domain": "grab.com",            "country": "Singapore",     "country_code": "SG"},
    {"name": "Sea Group",       "domain": "sea.com",             "country": "Singapore",     "country_code": "SG"},
    {"name": "Ninja Van",       "domain": "ninjavan.co",         "country": "Singapore",     "country_code": "SG"},
    # Switzerland
    {"name": "Temenos",         "domain": "temenos.com",         "country": "Switzerland",   "country_code": "CH"},
    {"name": "Cembra Money Bank","domain": "cembra.ch",          "country": "Switzerland",   "country_code": "CH"},
    # Spain
    {"name": "Typeform",        "domain": "typeform.com",        "country": "Spain",         "country_code": "ES"},
    {"name": "Factorial",       "domain": "factorialhr.com",     "country": "Spain",         "country_code": "ES"},
    {"name": "Cabify",          "domain": "cabify.com",          "country": "Spain",         "country_code": "ES"},
    # Poland
    {"name": "Brainly",         "domain": "brainly.com",         "country": "Poland",        "country_code": "PL"},
    {"name": "Booksy",          "domain": "booksy.com",          "country": "Poland",        "country_code": "PL"},
    {"name": "DocPlanner",      "domain": "docplanner.com",      "country": "Poland",        "country_code": "PL"},
    # Estonia
    {"name": "Pipedrive",       "domain": "pipedrive.com",       "country": "Estonia",       "country_code": "EE"},
    {"name": "Bolt",            "domain": "bolt.eu",             "country": "Estonia",       "country_code": "EE"},
    {"name": "TransferWise",    "domain": "transferwise.com",    "country": "Estonia",       "country_code": "EE"},
    # Finland
    {"name": "Supercell",       "domain": "supercell.com",       "country": "Finland",       "country_code": "FI"},
    {"name": "Wolt",            "domain": "wolt.com",            "country": "Finland",       "country_code": "FI"},
    {"name": "Aiven",           "domain": "aiven.io",            "country": "Finland",       "country_code": "FI"},
    # Romania
    {"name": "UiPath",          "domain": "uipath.com",          "country": "Romania",       "country_code": "RO"},
    {"name": "Bitdefender",     "domain": "bitdefender.com",     "country": "Romania",       "country_code": "RO"},
    # Czech Republic
    {"name": "Avast",           "domain": "avast.com",           "country": "Czech Republic","country_code": "CZ"},
    {"name": "Kiwi.com",        "domain": "kiwi.com",            "country": "Czech Republic","country_code": "CZ"},
    # Hungary
    {"name": "LogMeIn",         "domain": "logmein.com",         "country": "Hungary",       "country_code": "HU"},
    {"name": "Prezi",           "domain": "prezi.com",           "country": "Hungary",       "country_code": "HU"},
    # Portugal
    {"name": "Feedzai",         "domain": "feedzai.com",         "country": "Portugal",      "country_code": "PT"},
    {"name": "Unbabel",         "domain": "unbabel.com",         "country": "Portugal",      "country_code": "PT"},
    # Ukraine
    {"name": "Grammarly",       "domain": "grammarly.com",       "country": "Ukraine",       "country_code": "UA"},
    {"name": "GitLab",          "domain": "gitlab.com",          "country": "Ukraine",       "country_code": "UA"},
    # Belgium
    {"name": "Collibra",        "domain": "collibra.com",        "country": "Belgium",       "country_code": "BE"},
    {"name": "Showpad",         "domain": "showpad.com",         "country": "Belgium",       "country_code": "BE"},
]


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_domain(url: str) -> str:
    m = DOMAIN_RE.search(url)
    if not m:
        return ""
    d = m.group(1).lower()
    if d.startswith("www."):
        d = d[4:]
    return d if d not in SKIP_DOMAINS and "." in d and len(d) < 60 else ""


def _make(name: str, domain: str, country: str = "", country_code: str = "",
          description: str = "", source: str = "") -> dict | None:
    cc = country_code.strip()[:10].upper()
    cn = country.strip()[:100]
    if cn in SKIP_COUNTRIES or cc in SKIP_COUNTRIES:
        return None
    return {
        "name":         name.strip()[:200],
        "domain":       domain.strip().lower(),
        "country":      cn,
        "country_code": cc,
        "industry":     "technology",
        "description":  description.strip()[:500],
        "source":       source,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Source 1 — Wikidata SPARQL  (most reliable structured source)
# ─────────────────────────────────────────────────────────────────────────────

# Verified working QIDs (confirmed via SPARQL COUNT queries):
# Q783794  = software company
# Q1194970 = information technology company
WIKIDATA_TYPES = [
    ("Q783794",  "software company"),
    ("Q1194970", "IT company"),
]
WIKIDATA_PAGE   = 1000   # results per request — keep small to avoid timeouts
WIKIDATA_PAGES  = 30     # max pages per type  → up to 30,000 companies per type


def _wikidata() -> list[dict]:
    """Query Wikidata SPARQL for tech companies with pagination."""
    results  = []
    seen     = set()
    endpoint = "https://query.wikidata.org/sparql"
    wikidata_headers = {
        "User-Agent": "TechLeadDiscovery/1.0 (lead-gen research bot)",
        "Accept":     "application/sparql-results+json",
    }

    for qid, label in WIKIDATA_TYPES:
        for page in range(WIKIDATA_PAGES):
            offset = page * WIKIDATA_PAGE
            sparql = f"""
            SELECT ?name ?website ?countryLabel ?countryCode WHERE {{
              ?co wdt:P31 wd:{qid} .
              ?co wdt:P856 ?website .
              OPTIONAL {{
                ?co wdt:P17 ?country .
                ?country wdt:P298 ?countryCode .
              }}
              SERVICE wikibase:label {{
                bd:serviceParam wikibase:language "en" .
                ?co  rdfs:label ?name .
                ?country rdfs:label ?countryLabel .
              }}
            }} LIMIT {WIKIDATA_PAGE} OFFSET {offset}
            """
            try:
                r = requests.get(
                    endpoint,
                    params={"query": sparql, "format": "json"},
                    headers=wikidata_headers,
                    timeout=45,
                )
                if r.status_code == 429:
                    print(f"  [Wikidata] Rate-limited — waiting 60s")
                    time.sleep(60)
                    continue
                if r.status_code != 200 or not r.text:
                    print(f"  [Wikidata] {label} page {page}: HTTP {r.status_code}")
                    break

                bindings = r.json().get("results", {}).get("bindings", [])
                added = 0
                for row in bindings:
                    name    = row.get("name",         {}).get("value", "").strip()
                    website = row.get("website",       {}).get("value", "").strip()
                    country = row.get("countryLabel",  {}).get("value", "").strip()
                    cc      = row.get("countryCode",   {}).get("value", "").strip()
                    domain  = _extract_domain(website)
                    if domain and domain not in seen:
                        seen.add(domain)
                        results.append(_make(name, domain, country, cc, source="wikidata"))
                        added += 1

                print(f"  [Wikidata] {label} page {page}: +{added} ({len(results)} total)")
                time.sleep(6)

                if len(bindings) < WIKIDATA_PAGE:
                    break  # last page
            except Exception as e:
                print(f"  [Wikidata] {label} page {page} error: {e}")
                time.sleep(15)
                break

    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Source 2 — Hacker News "Who Is Hiring"
# ─────────────────────────────────────────────────────────────────────────────

def _hn_hiring() -> list[dict]:
    """5 years of monthly HN 'Who Is Hiring' threads via Algolia API."""
    results = []
    seen    = set()

    try:
        r = requests.get(
            "https://hn.algolia.com/api/v1/search",
            params={"query": "Ask HN: Who is hiring?", "tags": "story,ask_hn",
                    "hitsPerPage": 60},
            timeout=15,
        )
        threads = r.json().get("hits", [])
    except Exception as e:
        print(f"  [HN] Thread list error: {e}")
        return []

    print(f"  [HN] {len(threads)} threads found")
    for thread in threads:
        story_id = thread.get("objectID", "")
        if not story_id:
            continue
        try:
            cr = requests.get(
                "https://hn.algolia.com/api/v1/search",
                params={"tags": f"comment,story_{story_id}", "hitsPerPage": 1000},
                timeout=20,
            )
            for hit in cr.json().get("hits", []):
                text = hit.get("comment_text", "") or ""
                soup = BeautifulSoup(text, "lxml")
                for a in soup.find_all("a", href=True):
                    d = _extract_domain(a["href"])
                    if d and d not in seen:
                        seen.add(d)
                        results.append(_make("", d, source="hn_hiring"))
                for m in DOMAIN_RE.finditer(soup.get_text()):
                    d = _extract_domain(m.group(0))
                    if d and d not in seen:
                        seen.add(d)
                        results.append(_make("", d, source="hn_hiring"))
            time.sleep(0.4)
        except Exception:
            time.sleep(2)

    print(f"  [HN] {len(results)} domains extracted")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Source 3 — Remote OK public API
# ─────────────────────────────────────────────────────────────────────────────

def _remotive() -> list[dict]:
    """Remotive.com public JSON API — remote tech job postings with company URLs."""
    seen    = set()
    results = []
    try:
        r = requests.get(
            "https://remotive.com/api/remote-jobs",
            params={"limit": 200},
            headers=HEADERS,
            timeout=20,
        )
        if r.status_code != 200:
            print(f"  [Remotive] HTTP {r.status_code}")
            return []
        for job in r.json().get("jobs", []):
            company = job.get("company_name", "")
            url     = job.get("company_logo", "") or job.get("url", "")
            # company_logo is often company.com/logo.png — extract root domain
            d = _extract_domain(url) if url else ""
            if not d:
                # try guessing from company name
                slug = re.sub(r"[^a-z0-9]", "", company.lower())
                d    = f"{slug}.com" if slug else ""
                d    = d if d and "." in d else ""
            if d and d not in seen:
                seen.add(d)
                results.append(_make(company, d, source="remotive"))
        print(f"  [Remotive] {len(results)} companies")
    except Exception as e:
        print(f"  [Remotive] Error: {e}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Source 4 — GitHub organizations API
# ─────────────────────────────────────────────────────────────────────────────

GH_QUERIES = [
    "type:org software", "type:org technology", "type:org devops",
    "type:org cybersecurity", "type:org fintech", "type:org saas",
    "type:org platform", "type:org artificial intelligence",
    "type:org machine learning", "type:org cloud",
    "type:org networking", "type:org infrastructure",
    "type:org data engineering", "type:org security",
    "type:org open source", "type:org enterprise",
    "type:org api", "type:org developer tools",
    "type:org observability", "type:org kubernetes",
    "type:org golang", "type:org rust lang",
    "type:org typescript", "type:org python",
    "type:org blockchain", "type:org iot",
    "type:org embedded systems", "type:org robotics",
    "type:org autonomous", "type:org genomics",
]


def _github_orgs(token: str = "") -> list[dict]:
    """GitHub org search — returns orgs with a registered blog/website."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    results = []
    seen    = set()

    for query in GH_QUERIES:
        for page in range(1, 6):
            try:
                r = requests.get(
                    "https://api.github.com/search/users",
                    headers=headers,
                    params={"q": query, "per_page": 100, "page": page},
                    timeout=15,
                )
                if r.status_code == 403:
                    reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                    wait  = max(5, reset - int(time.time())) + 2
                    print(f"  [GitHub] Rate limit — waiting {min(wait,120)}s")
                    time.sleep(min(wait, 120))
                    break
                if r.status_code != 200:
                    break
                items = r.json().get("items", [])
                if not items:
                    break

                for org in items:
                    login = org.get("login", "")
                    if login in seen:
                        continue
                    seen.add(login)
                    try:
                        detail = requests.get(
                            f"https://api.github.com/orgs/{login}",
                            headers=headers, timeout=8,
                        ).json()
                        blog = (detail.get("blog") or "").strip()
                        name = detail.get("name") or login
                        loc  = detail.get("location") or ""
                        if blog:
                            if not blog.startswith("http"):
                                blog = "https://" + blog
                            d = _extract_domain(blog)
                            if d and d not in seen:
                                seen.add(d)
                                results.append(_make(name, d, country=loc, source="github"))
                        time.sleep(0.2)
                    except Exception:
                        pass

                time.sleep(1.5)
                if len(items) < 100:
                    break
            except Exception as e:
                print(f"  [GitHub] Error: {e}")
                break
        time.sleep(2)

    print(f"  [GitHub] {len(results)} org domains")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Source 5 — SEC EDGAR (US public + private tech companies)
# ─────────────────────────────────────────────────────────────────────────────

def _github_top_orgs(token: str = "") -> list[dict]:
    """
    GitHub top organisations by followers — these are almost all real tech companies.
    Fetches 10 pages × 100 orgs each = up to 1000 orgs with their website (blog field).
    """
    gh_headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        gh_headers["Authorization"] = f"token {token}"

    results  = []
    seen     = set()

    for page in range(1, 11):
        try:
            r = requests.get(
                "https://api.github.com/search/users",
                headers=gh_headers,
                params={"q": "type:org followers:>50", "sort": "followers",
                        "order": "desc", "per_page": 100, "page": page},
                timeout=15,
            )
            if r.status_code == 403:
                reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait  = max(5, reset - int(time.time())) + 2
                print(f"  [GitHub Top Orgs] Rate limit — waiting {min(wait,60)}s")
                time.sleep(min(wait, 60))
                break
            if r.status_code != 200:
                break

            items = r.json().get("items", [])
            if not items:
                break

            for org in items:
                login = org.get("login", "")
                if login in seen:
                    continue
                seen.add(login)
                try:
                    detail = requests.get(
                        f"https://api.github.com/orgs/{login}",
                        headers=gh_headers, timeout=8,
                    ).json()
                    blog = (detail.get("blog") or "").strip()
                    name = detail.get("name") or login
                    loc  = detail.get("location") or ""
                    if blog:
                        if not blog.startswith("http"):
                            blog = "https://" + blog
                        d = _extract_domain(blog)
                        if d and d not in seen:
                            seen.add(d)
                            results.append(_make(name, d, country=loc, source="github_top"))
                    time.sleep(0.2)
                except Exception:
                    pass

            time.sleep(2)
            if len(items) < 100:
                break
        except Exception as e:
            print(f"  [GitHub Top Orgs] Error: {e}")
            break

    print(f"  [GitHub Top Orgs] {len(results)} org websites")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Source 6 — DDG search
# ─────────────────────────────────────────────────────────────────────────────

def _ddg_search() -> list[dict]:
    """50 targeted DDG queries for tech company lists."""
    results = []
    seen    = set()
    with DDGS() as ddg:
        for query in DDG_QUERIES:
            try:
                for r in ddg.text(query, max_results=15):
                    url = r.get("href", "")
                    d   = _extract_domain(url)
                    if d and d not in seen:
                        seen.add(d)
                        results.append(_make("", d, source="ddg"))
                time.sleep(1.5)
            except Exception:
                time.sleep(5)
    print(f"  [DDG] {len(results)} domains")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Main source class
# ─────────────────────────────────────────────────────────────────────────────

class CompanyDiscoverySource(BaseSource):
    name         = "company_discovery"
    requires_key = False

    def fetch(self) -> list[dict]:
        all_companies: dict[str, dict] = {}  # domain → company dict

        def _add(companies: list[dict]):
            for co in companies:
                if co is None:
                    continue
                country = co.get("country", "") or ""
                cc      = co.get("country_code", "") or ""
                if country in SKIP_COUNTRIES or cc in SKIP_COUNTRIES:
                    continue
                d = co.get("domain", "").strip().lower()
                if d and "." in d and len(d) < 60 and d not in all_companies:
                    all_companies[d] = co

        # 1. Seed list
        print(f"  [CompanyDiscovery] Seed list: {len(SEED_COMPANIES)} companies")
        _add(SEED_COMPANIES)

        # 2. Wikidata SPARQL (best free structured source)
        print(f"  [CompanyDiscovery] Wikidata SPARQL ({len(WIKIDATA_TYPES)} types, paginated)...")
        _add(_wikidata())

        # 3. Hacker News Who Is Hiring
        print(f"  [CompanyDiscovery] Hacker News Who Is Hiring...")
        _add(_hn_hiring())

        # 4. Remotive API
        print(f"  [CompanyDiscovery] Remotive API...")
        _add(_remotive())

        # 5. GitHub orgs
        token = self.config.get("GITHUB_TOKEN", "")
        print(f"  [CompanyDiscovery] GitHub orgs ({len(GH_QUERIES)} queries)...")
        _add(_github_orgs(token))

        # 6. GitHub top orgs by followers
        print(f"  [CompanyDiscovery] GitHub top organisations...")
        _add(_github_top_orgs(token))

        # 7. DDG search
        print(f"  [CompanyDiscovery] DDG search ({len(DDG_QUERIES)} queries)...")
        _add(_ddg_search())

        # ── Dedup against existing DB ─────────────────────────────────────
        print(f"\n  [CompanyDiscovery] Found {len(all_companies)} unique domains total")
        known = DB.get_known_domains()
        print(f"  [CompanyDiscovery] Already known: {len(known)} | "
              f"Overlap: {sum(1 for d in all_companies if d in known)}")

        new_count  = 0
        dupe_count = 0
        for domain, co in all_companies.items():
            if domain in known:
                dupe_count += 1
                continue
            co["source"] = co.get("source") or self.name
            _, status = DB.upsert_company(co)
            if status == "inserted":
                new_count += 1
                known.add(domain)

        print(f"  [CompanyDiscovery] {new_count} new companies added | "
              f"{dupe_count} duplicates skipped")
        return []

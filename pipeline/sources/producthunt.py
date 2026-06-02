"""
ProductHunt source — scrapes tech founders and makers from product launches.
No API key required for basic scraping.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS
from .base import BaseSource

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

PH_BASE = "https://www.producthunt.com"

# Top tech categories on ProductHunt
CATEGORIES = [
    "developer-tools", "saas", "artificial-intelligence",
    "productivity", "no-code", "devops", "api",
    "open-source", "security", "data-analytics",
    "cloud", "infrastructure",
]


def _fetch(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception:
        pass
    return None


def _scrape_category(category: str) -> list[dict]:
    url  = f"{PH_BASE}/topics/{category}"
    soup = _fetch(url)
    if not soup:
        return []

    makers  = []
    seen    = set()
    # Product cards
    for item in soup.find_all("a", href=re.compile(r"/posts/")):
        title = item.get_text(strip=True)
        href  = item.get("href", "")
        if not title or href in seen:
            continue
        seen.add(href)

        # Visit product page to get makers
        product_url = PH_BASE + href if href.startswith("/") else href
        product_soup = _fetch(product_url)
        if not product_soup:
            time.sleep(0.5)
            continue

        # Find maker profiles
        for maker_link in product_soup.find_all("a", href=re.compile(r"/@[\w]+")):
            username = re.search(r"/@([\w\-]+)", maker_link["href"])
            if not username:
                continue
            uname = username.group(1)
            if uname in seen:
                continue
            seen.add(uname)

            # Get maker's display name from link text or nearby element
            name_text = maker_link.get_text(strip=True)
            if not name_text or len(name_text) > 50:
                continue

            parts = name_text.strip().split(" ", 1)
            makers.append({
                "first_name":  parts[0],
                "last_name":   parts[1] if len(parts) > 1 else "",
                "full_name":   name_text,
                "title":       "Founder",  # PH makers are typically founders
                "website":     f"{PH_BASE}/@{uname}",
                "source_url":  product_url,
                "tags":        ["producthunt", category],
            })

        time.sleep(1)

    return makers


class ProductHuntSource(BaseSource):
    name         = "producthunt"
    requires_key = False

    def fetch(self) -> list[dict]:
        all_contacts = []
        seen_names   = set()

        for category in CATEGORIES:
            print(f"  [ProductHunt] Category: {category}")
            makers = _scrape_category(category)
            for m in makers:
                key = m.get("full_name", "")
                if key and key not in seen_names:
                    seen_names.add(key)
                    all_contacts.append(m)
            print(f"  [ProductHunt] {category}: {len(makers)} makers")
            time.sleep(2)

        print(f"  [ProductHunt] Total: {len(all_contacts)} founders/makers")
        return all_contacts

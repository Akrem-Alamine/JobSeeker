"""
Conference speaker scraper — extracts speakers from major tech conferences.
All public pages, no API key needed.

Targets: KubeCon, Web Summit, VivaTech, QCon, GOTO, GitHub Universe,
         Collision, InfoQ, Sessionize public directory, and more.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from .base import BaseSource

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

EMAIL_RE = re.compile(r'[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}')

CONFERENCES = [
    # -- Static HTML, confirmed working --
    {"name": "Devoxx UK",          "url": "https://www.devoxx.co.uk/speakers/",              "type": "generic"},
    {"name": "NDC Oslo",           "url": "https://ndcoslo.com/speakers",                    "type": "generic"},
    {"name": "NDC London",         "url": "https://ndclondon.com/speakers",                  "type": "generic"},
    {"name": "NDC Minnesota",      "url": "https://ndcminnesota.com/speakers",               "type": "generic"},
    {"name": "NDC Sydney",         "url": "https://ndcsydney.com/speakers",                  "type": "generic"},
    {"name": "NDC Porto",          "url": "https://ndcporto.com/speakers",                   "type": "generic"},
    {"name": "QCon London",        "url": "https://qconlondon.com/speakers",                 "type": "generic"},
    {"name": "QCon San Francisco", "url": "https://qconsf.com/speakers",                     "type": "generic"},
    {"name": "QCon Plus",          "url": "https://plus.qconferences.com/speakers",           "type": "generic"},
    {"name": "InfoQ Articles",     "url": "https://www.infoq.com/presentations/",            "type": "infoq"},
    # -- FOSDEM (static site, huge open source conference) --
    {"name": "FOSDEM 2025",        "url": "https://fosdem.org/2025/schedule/speakers/",      "type": "fosdem"},
    {"name": "FOSDEM 2024",        "url": "https://fosdem.org/2024/schedule/speakers/",      "type": "fosdem"},
    # -- DevConf --
    {"name": "DevConf.cz 2024",    "url": "https://www.devconf.info/cz/speakers/",          "type": "generic"},
    # -- GopherCon --
    {"name": "GopherCon 2024",     "url": "https://www.gophercon.com/agenda/speakers",       "type": "generic"},
    # -- PyCon --
    {"name": "PyCon Italy 2024",   "url": "https://pycon.it/en/speakers",                    "type": "generic"},
    {"name": "EuroPython 2024",    "url": "https://ep2024.europython.eu/speakers",            "type": "generic"},
    # -- DevOps & Cloud --
    {"name": "DevOpsDays",         "url": "https://devopsdays.org/speakers",                 "type": "generic"},
    {"name": "KubeCon schedule",   "url": "https://kccnceu2025.sched.com/",                  "type": "sched"},
    {"name": "OpenInfra Summit",   "url": "https://openinfra.dev/summit/",                   "type": "generic"},
    # -- Security --
    {"name": "RSA Conference",     "url": "https://www.rsaconference.com/speakers",           "type": "generic"},
    {"name": "Black Hat",          "url": "https://www.blackhat.com/us-24/speakers.html",    "type": "generic"},
    # -- AI/ML --
    {"name": "NeurIPS",            "url": "https://nips.cc/virtual/2024/calendar",            "type": "generic"},
    # -- Regional tech --
    {"name": "Turing Fest",        "url": "https://www.turingfest.com/speakers/",            "type": "generic"},
    {"name": "DotJS",              "url": "https://www.dotjs.io/speakers",                   "type": "generic"},
    {"name": "ScotlandJS",         "url": "https://scotlandjs.com",                          "type": "generic"},
    {"name": "FullStack",          "url": "https://skillsmatter.com/conferences",             "type": "generic"},
]


def _fetch(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception:
        pass
    return None


def _parse_generic(soup: BeautifulSoup, conf_name: str) -> list[dict]:
    """Generic parser that works for most conference pages."""
    speakers = []
    seen     = set()

    # Common patterns: elements with class containing 'speaker', 'presenter', 'person'
    cards = (
        soup.find_all(class_=re.compile(r"speaker|presenter|person|panelist|keynote", re.I)) or
        soup.find_all(["article", "li"], class_=re.compile(r"card|item|member", re.I))
    )

    for card in cards:
        text = card.get_text(" ", strip=True)
        if len(text) < 5 or len(text) > 600:
            continue

        # Name: first heading or strong
        name_tag = (
            card.find(["h2", "h3", "h4", "h5"]) or
            card.find(class_=re.compile(r"name|title", re.I)) or
            card.find("strong")
        )
        if not name_tag:
            continue

        full_name = name_tag.get_text(strip=True)
        if not full_name or len(full_name) > 60 or full_name in seen:
            continue
        seen.add(full_name)

        # Title + company: remaining lines
        remaining = text.replace(full_name, "").strip()
        lines     = [l.strip() for l in remaining.split("\n") if l.strip()]
        title     = lines[0][:80] if lines else ""
        company   = lines[1][:80] if len(lines) > 1 else ""

        # LinkedIn
        linkedin = ""
        for a in card.find_all("a", href=True):
            if "linkedin.com/in/" in a["href"]:
                linkedin = a["href"]
                break

        parts = full_name.split(" ", 1)
        first = parts[0]
        last  = parts[1] if len(parts) > 1 else ""

        speakers.append({
            "first_name":  first,
            "last_name":   last,
            "full_name":   full_name,
            "title":       title,
            "company":     company,
            "linkedin_url": linkedin,
            "source_url":  conf_name,
            "tags":        ["conference", conf_name.lower().replace(" ", "_")],
        })

    return speakers


def _parse_fosdem(soup: BeautifulSoup) -> list[dict]:
    speakers = []
    seen     = set()
    for a in soup.find_all("a", href=re.compile(r"/schedule/speaker/")):
        full_name = a.get_text(strip=True)
        if not full_name or full_name in seen or len(full_name) > 60:
            continue
        seen.add(full_name)
        parts = full_name.split(" ", 1)
        speakers.append({
            "first_name": parts[0],
            "last_name":  parts[1] if len(parts) > 1 else "",
            "full_name":  full_name,
            "title":      "Speaker",
            "source_url": "https://fosdem.org",
            "tags":       ["conference", "fosdem"],
        })
    return speakers


def _parse_sched(soup: BeautifulSoup) -> list[dict]:
    speakers = []
    seen     = set()
    for card in soup.find_all(class_=re.compile(r"sched-person|speaker", re.I)):
        name_tag  = card.find(["h2", "h3", "strong", "a"])
        title_tag = card.find(class_=re.compile(r"title|role|company", re.I))
        if not name_tag:
            continue
        full_name = name_tag.get_text(strip=True)
        if not full_name or full_name in seen or len(full_name) > 60:
            continue
        seen.add(full_name)
        parts = full_name.split(" ", 1)
        speakers.append({
            "first_name": parts[0],
            "last_name":  parts[1] if len(parts) > 1 else "",
            "full_name":  full_name,
            "title":      title_tag.get_text(strip=True)[:80] if title_tag else "",
            "tags":       ["conference"],
        })
    return speakers


def _parse_infoq(soup: BeautifulSoup) -> list[dict]:
    speakers = []
    seen     = set()
    for item in soup.find_all("div", class_=re.compile(r"speaker|author", re.I)):
        name_tag = item.find(["h3", "h4", "a"])
        if not name_tag:
            continue
        full_name = name_tag.get_text(strip=True)
        if not full_name or full_name in seen:
            continue
        seen.add(full_name)
        title_tag = item.find(class_=re.compile(r"title|role|position", re.I))
        title     = title_tag.get_text(strip=True) if title_tag else ""
        parts     = full_name.split(" ", 1)
        speakers.append({
            "first_name": parts[0],
            "last_name":  parts[1] if len(parts) > 1 else "",
            "full_name":  full_name,
            "title":      title,
            "tags":       ["conference", "infoq"],
        })
    return speakers


class ConferencesSource(BaseSource):
    name         = "conferences"
    requires_key = False

    def fetch(self) -> list[dict]:
        all_contacts = []

        for conf in CONFERENCES:
            print(f"  [Conferences] Scraping: {conf['name']}")
            soup = _fetch(conf["url"])
            if not soup:
                print(f"  [Conferences] Could not reach: {conf['url']}")
                time.sleep(1)
                continue

            if conf["type"] == "infoq":
                people = _parse_infoq(soup)
            elif conf["type"] == "fosdem":
                people = _parse_fosdem(soup)
            elif conf["type"] == "sched":
                people = _parse_sched(soup)
            else:
                people = _parse_generic(soup, conf["name"])

            print(f"  [Conferences] {conf['name']}: {len(people)} speakers")
            all_contacts.extend(people)
            time.sleep(2)

        return all_contacts

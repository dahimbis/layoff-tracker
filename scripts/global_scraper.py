#!/usr/bin/env python3
"""
global_scraper.py — Scrapes international layoff news for non-US companies.
Complements the WARN Act pipeline which only covers USA.

Sources:
  - TechCrunch layoffs tag RSS
  - Reuters Technology RSS
  - Google News RSS (layoffs keyword)
  - Hacker News Algolia API (layoff/laid off posts)

Output: data/pending_global.csv (requires human review before merging)

Install: pip install feedparser requests beautifulsoup4
Run:     python scripts/global_scraper.py
"""

import csv
import hashlib
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import feedparser
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run: pip install feedparser requests beautifulsoup4")
    exit(1)

ROOT = Path(__file__).parent.parent
PENDING = ROOT / "data" / "pending_global.csv"
USA_CSV = ROOT / "data" / "layoffs_usa.csv"

# Skip companies already well-covered by WARN Act (US companies)
# Focus on international
INTERNATIONAL_FOCUS = True

RSS_SOURCES = [
    {
        "name": "TechCrunch Layoffs",
        "url": "https://techcrunch.com/tag/layoffs/feed/",
        "focus": "global",
    },
    {
        "name": "Reuters Technology",
        "url": "https://feeds.reuters.com/reuters/technologyNews",
        "focus": "global",
    },
    {
        "name": "The Verge",
        "url": "https://www.theverge.com/rss/index.xml",
        "focus": "tech",
    },
    {
        "name": "BBC Business",
        "url": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "focus": "global",
    },
    {
        "name": "Financial Times",
        "url": "https://www.ft.com/rss/home/uk",
        "focus": "global",
    },
]

# Google News RSS for specific regions (no API key needed)
GOOGLE_NEWS_FEEDS = [
    "https://news.google.com/rss/search?q=layoffs+2025&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=company+layoffs+Europe&hl=en&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=tech+layoffs+Asia+2025&hl=en&gl=SG&ceid=SG:en",
    "https://news.google.com/rss/search?q=retrenchment+India+2025&hl=en&gl=IN&ceid=IN:en",
]

# HN Algolia API — searches for "layoff" stories posted in last 30 days
HN_API = "https://hn.algolia.com/api/v1/search?query=layoffs&tags=story&numericFilters=created_at_i>{ts}"

LAYOFF_KW = [
    "layoff", "layoffs", "laid off", "workforce reduction", "job cut",
    "headcount reduction", "downsizing", "redundanc", "restructuring",
    "retrenchment", "job losses", "position eliminat", "workforce adjustment",
    "mass termination",
]

# Known US-headquartered companies (skip for international focus)
US_HQ_SIGNALS = [
    "san francisco", "new york", "seattle", "austin", "boston",
    "chicago", "los angeles", "menlo park", "cupertino", "redmond",
    "inc.", "corp.", "llc", "nasdaq", "nyse",
]

COUNT_RE = [
    re.compile(r"(\d[\d,]+)\s+(?:employees|workers|jobs|people|positions|staff)\s+(?:laid off|cut|eliminated|let go|fired|made redundant)", re.I),
    re.compile(r"(?:laying off|cutting|eliminating|reducing by|shedding)\s+(\d[\d,]+)\s+(?:employees|workers|jobs|positions)", re.I),
    re.compile(r"cut(?:ting)?\s+(?:its\s+)?(?:workforce|staff|headcount)\s+by\s+(\d[\d,]+)", re.I),
    re.compile(r"(\d+(?:\.\d+)?)\s*[Kk]\s+(?:employees|workers|jobs)", re.I),
]

PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%\s+of\s+(?:its\s+)?(?:global\s+)?(?:workforce|staff|employees)", re.I)

COMPANY_RE = re.compile(
    r"^(?:EXCLUSIVE[:\s]+|BREAKING[:\s]+|REPORT[:\s]+)?([A-Z][A-Za-z0-9\s&\.,\-\']{2,40}?)"
    r"(?:\s+(?:is|to|will|plans?|lays?|cuts?|announces?|confirms?|fires?|eliminates?|shed))",
)


def is_layoff(title: str, summary: str) -> bool:
    text = (title + " " + summary).lower()
    return any(kw in text for kw in LAYOFF_KW)


def extract_count(text: str) -> int:
    for pat in COUNT_RE:
        m = pat.search(text)
        if m:
            val = m.group(1).replace(",", "")
            if val.replace(".", "").isdigit():
                n = float(val)
                # Handle "2.5K" style
                if re.search(r'\d+[Kk]', m.group(0)):
                    n *= 1000
                return int(n)
    return -1


def extract_pct(text: str) -> str:
    m = PCT_RE.search(text)
    return m.group(1) if m else ""


def extract_company(title: str) -> str:
    m = COMPANY_RE.match(title.strip())
    if m:
        return m.group(1).strip().rstrip(",. ")
    return "REVIEW_NEEDED"


def dedup_key(url: str) -> str:
    return hashlib.md5(url.strip().lower().encode()).hexdigest()


def load_seen_urls() -> set:
    seen = set()
    if PENDING.exists():
        with open(PENDING, newline="") as f:
            seen.update(dedup_key(r.get("source_url", "")) for r in csv.DictReader(f))
    return seen


def fetch_rss(source: dict, seen: set) -> list[dict]:
    candidates = []
    print(f"  → {source['name']} ...")
    try:
        parsed = feedparser.parse(source["url"])
        for entry in parsed.entries[:50]:
            title = entry.get("title", "")
            summary = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text()
            link = entry.get("link", "").strip()

            if not link or dedup_key(link) in seen:
                continue
            if not is_layoff(title, summary):
                continue

            try:
                dt = datetime(*entry.published_parsed[:6])
                # Only last 30 days
                if dt < datetime.utcnow() - timedelta(days=30):
                    continue
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                date_str = datetime.today().strftime("%Y-%m-%d")

            full = title + " " + summary
            count = extract_count(full)
            pct = extract_pct(full)
            company = extract_company(title)

            candidates.append({
                "company": company,
                "country": "REVIEW_NEEDED",
                "city": "",
                "layoff_date": date_str,
                "employees_laid_off": count,
                "percentage_laid_off": pct,
                "department": "",
                "stage": "REVIEW_NEEDED",
                "source": source["name"],
                "source_url": link,
                "headline": title[:250],
                "verified": "false",
                "added_date": datetime.today().strftime("%Y-%m-%d"),
            })
            seen.add(dedup_key(link))
        time.sleep(1)
    except Exception as e:
        print(f"     ⚠ Error: {e}")
    return candidates


def fetch_hn(seen: set) -> list[dict]:
    """Fetch layoff-related posts from Hacker News via Algolia API."""
    candidates = []
    try:
        ts = int((datetime.utcnow() - timedelta(days=14)).timestamp())
        url = HN_API.format(ts=ts)
        resp = requests.get(url, timeout=15)
        data = resp.json()
        print(f"  → Hacker News ({data.get('nbHits', 0)} results) ...")

        for hit in data.get("hits", []):
            title = hit.get("title", "")
            link = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}"

            if not is_layoff(title, ""):
                continue
            if dedup_key(link) in seen:
                continue

            ts_val = hit.get("created_at_i", 0)
            date_str = datetime.utcfromtimestamp(ts_val).strftime("%Y-%m-%d") if ts_val else ""

            candidates.append({
                "company": extract_company(title),
                "country": "REVIEW_NEEDED",
                "city": "",
                "layoff_date": date_str,
                "employees_laid_off": extract_count(title),
                "percentage_laid_off": extract_pct(title),
                "department": "",
                "stage": "REVIEW_NEEDED",
                "source": "Hacker News",
                "source_url": link,
                "headline": title[:250],
                "verified": "false",
                "added_date": datetime.today().strftime("%Y-%m-%d"),
            })
            seen.add(dedup_key(link))
        time.sleep(0.5)
    except Exception as e:
        print(f"     ⚠ HN error: {e}")
    return candidates


def save(candidates: list[dict]) -> int:
    if not candidates:
        return 0

    # Deduplicate within batch
    seen_keys = set()
    unique = []
    for c in candidates:
        k = dedup_key(c["source_url"])
        if k not in seen_keys:
            seen_keys.add(k)
            unique.append(c)

    fieldnames = [
        "company", "country", "city", "layoff_date", "employees_laid_off",
        "percentage_laid_off", "department", "stage", "source", "source_url",
        "headline", "verified", "added_date"
    ]

    write_header = not PENDING.exists()
    with open(PENDING, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(unique)

    return len(unique)


if __name__ == "__main__":
    print("\n🌍 Global Layoff Scraper\n")
    seen = load_seen_urls()
    all_candidates = []

    # RSS feeds
    for source in RSS_SOURCES + [{"name": f"Google News {i+1}", "url": url} for i, url in enumerate(GOOGLE_NEWS_FEEDS)]:
        results = fetch_rss(source, seen)
        all_candidates.extend(results)
        print(f"     {len(results)} candidates")

    # Hacker News
    hn = fetch_hn(seen)
    all_candidates.extend(hn)
    print(f"     {len(hn)} candidates")

    n = save(all_candidates)
    if n > 0:
        print(f"\n✅ {n} new international candidates saved to {PENDING}")
        print("   Review 'country', 'company', 'stage' fields before merging.\n")
    else:
        print("\n✅ No new international candidates found.\n")

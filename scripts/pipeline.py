#!/usr/bin/env python3
"""
pipeline.py — Core data pipeline for the Global Layoff Tracker.

Step 1: Download WARN Act notices from US state government websites
        using the `warn-scraper` package (Stanford Big Local News).
Step 2: Normalize all state CSVs into a unified schema.
Step 3: Deduplicate against existing data.
Step 4: Append new records to data/layoffs_usa.csv.
Step 5: Regenerate data/stats.json for the dashboard.

Install:
    pip install warn-scraper pandas

Run:
    python scripts/pipeline.py                    # scrape all states
    python scripts/pipeline.py --states CA NY TX  # specific states
    python scripts/pipeline.py --states CA --year 2025
"""

import argparse
import concurrent.futures
import csv
import hashlib
import json
import logging
import sys
import tempfile
import time as _time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Run: pip install pandas warn-scraper")
    sys.exit(1)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
USA_CSV = DATA_DIR / "layoffs_usa.csv"
STATS_JSON = DATA_DIR / "stats.json"
DASHBOARD_JSON = ROOT / "dashboard" / "data.json"
LOG_FILE = DATA_DIR / "pipeline.log"

DATA_DIR.mkdir(exist_ok=True)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger(__name__)

# ── Target states (highest layoff volume) ────────────────────────────────────
# These are the states warn-scraper supports with reliable data.
# Full list: https://warn-scraper.readthedocs.io/en/latest/scrapers/
HIGH_PRIORITY_STATES = [
    "CA",  # California — largest tech hub, most WARN filings
    "NY",  # New York — finance + media
    "TX",  # Texas — energy + tech (Tesla, Dell, TI)
    "WA",  # Washington — Amazon, Microsoft, Boeing
    "IL",  # Illinois — finance, manufacturing
    "FL",  # Florida — large general workforce
    "PA",  # Pennsylvania — healthcare, manufacturing
    "OH",  # Ohio — manufacturing, retail
    "GA",  # Georgia — logistics, film
    "NJ",  # New Jersey — pharma, finance
    "MA",  # Massachusetts — biotech, finance
    "CO",  # Colorado — tech
    "AZ",  # Arizona — semiconductor (Intel, TSMC)
    "NC",  # North Carolina — finance (Bank of America, Wells Fargo)
    "MI",  # Michigan — automotive (Ford, GM, Stellantis)
    "MN",  # Minnesota — medical devices
    "MO",  # Missouri — manufacturing
    "VA",  # Virginia — defense, federal contractors
    "MD",  # Maryland — federal contractors
    "OR",  # Oregon — Intel, Nike
]

# ── Unified output schema ─────────────────────────────────────────────────────
OUTPUT_COLUMNS = [
    "record_id",        # MD5 hash for deduplication
    "company",
    "state",
    "country",
    "city",
    "layoff_date",      # effective date from WARN
    "notice_date",      # date notice was filed (may differ)
    "employees_laid_off",
    "layoff_type",      # Layoff / Closure / Relocation
    "industry",
    "source",           # "WARN-{STATE}"
    "source_url",       # state portal URL
    "raw_notice_date",  # original string from state portal
    "notes",
    "added_date",
]

# ── State portal URLs (for source attribution) ────────────────────────────────
STATE_PORTAL_URLS = {
    "CA": "https://edd.ca.gov/en/jobs_and_training/layoff_services_warn",
    "NY": "https://labor.ny.gov/app/warn/",
    "TX": "https://www.twc.texas.gov/data-reports/warn-notice",
    "WA": "https://www.esd.wa.gov/about-employees/WARN",
    "IL": "https://www2.illinois.gov/ides/IDES%20Forms%20and%20Publications/CLI019L.pdf",
    "FL": "https://floridajobs.org/business-growth-and-partnerships/business-retention/warn-act-employer-information",
    "PA": "https://www.dli.pa.gov/Individuals/Workforce-Development/warn/Pages/default.aspx",
    "OH": "https://jfs.ohio.gov/warn/",
    "GA": "https://www.dol.state.ga.us/public/es/warn/search",
    "NJ": "https://www.nj.gov/labor/lwdhome/warn/",
    "MA": "https://www.mass.gov/service-details/worker-adjustment-and-retraining-notification-warn-act",
    "CO": "https://cdle.colorado.gov/employers/layoff-and-separation-information/worker-adjustment-retraining-notification-act",
    "AZ": "https://www.azjobconnection.gov/ada/r/warn_lookups/new",
    "NC": "https://www.des.nc.gov/des/warn-notices",
    "MI": "https://www.michigan.gov/leo/bureaus-agencies/wd/warn",
    "MN": "https://www.uimn.org/employers/employer-resources/employer-layoff-information.jsp",
    "MO": "https://jobs.mo.gov/warn",
    "VA": "https://www.vec.virginia.gov/warn-notices",
    "MD": "https://www.dllr.state.md.us/employment/warn.shtml",
    "OR": "https://www.oregon.gov/employ/Businesses/Pages/WARN.aspx",
}


def run_warn_scraper(states: list[str], output_dir: Path, max_minutes: int = 0) -> dict[str, Path]:
    """
    Run warn-scraper using its Python Runner API to download WARN data.
    Returns a dict of {state_code: csv_path}.

    max_minutes: stop after N minutes total (0 = unlimited).
                 Each state gets at most 2 minutes regardless.
    """
    try:
        from warn.runner import Runner
    except ImportError:
        log.error("warn-scraper not installed. Run: pip install warn-scraper")
        sys.exit(1)

    cache_dir = output_dir / "cache"
    cache_dir.mkdir(exist_ok=True)
    runner = Runner(data_dir=output_dir, cache_dir=cache_dir)

    deadline = (_time.monotonic() + max_minutes * 60) if max_minutes else None
    PER_STATE_TIMEOUT = 120  # 2 min max per state regardless of total budget

    results = {}
    for state in states:
        # Check overall time budget
        if deadline and _time.monotonic() >= deadline:
            log.warning(f"  ⏱ Time budget ({max_minutes}m) reached — stopping before {state}")
            break

        remaining = (deadline - _time.monotonic()) if deadline else PER_STATE_TIMEOUT
        timeout = min(PER_STATE_TIMEOUT, remaining)
        if timeout <= 0:
            break

        log.info(f"  Scraping {state}...")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(runner.scrape, state.lower())
                try:
                    csv_path = future.result(timeout=timeout)
                except concurrent.futures.TimeoutError:
                    log.warning(f"    ⚠ {state}: timed out after {timeout:.0f}s, skipping")
                    continue

            if csv_path and Path(csv_path).exists():
                results[state] = Path(csv_path)
                row_count = sum(1 for _ in open(csv_path)) - 1
                log.info(f"    ✓ {state}: {row_count} rows → {csv_path}")
            else:
                candidates = list(output_dir.glob(f"{state.lower()}*.csv"))
                if candidates:
                    results[state] = candidates[0]
                    log.info(f"    ✓ {state}: found at {candidates[0]}")
                else:
                    log.warning(f"    ⚠ {state}: no output file found")
        except Exception as e:
            log.warning(f"    ⚠ {state}: {type(e).__name__} — {str(e)[:120]}")
    return results


def make_record_id(company: str, state: str, date: str, count: str) -> str:
    """Generate a stable deduplication key."""
    raw = f"{company.lower().strip()}|{state.lower()}|{date}|{count}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def normalize_date(raw: str) -> str:
    """Try to parse various date formats into YYYY-MM-DD."""
    if not raw or not raw.strip():
        return ""
    raw = raw.strip()
    formats = [
        "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
        "%m/%d/%y", "%B %d, %Y", "%b %d, %Y",
        "%Y/%m/%d", "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw  # return as-is if we can't parse


def normalize_count(raw: str) -> int:
    """Parse employee count from various formats."""
    if not raw:
        return -1
    clean = raw.strip().replace(",", "").replace("+", "")
    try:
        return int(clean)
    except ValueError:
        return -1


def normalize_state_csv(state: str, csv_path: Path) -> list[dict]:
    """
    Read a warn-scraper output CSV and normalize to our unified schema.
    warn-scraper outputs vary by state, but common column names are used.
    """
    try:
        df = pd.read_csv(csv_path, dtype=str, on_bad_lines="skip")
    except Exception as e:
        log.error(f"Could not read {csv_path}: {e}")
        return []

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    records = []
    today = datetime.today().strftime("%Y-%m-%d")

    # Column name mapping — warn-scraper uses different names per state
    col_maps = {
        "company": ["company", "employer", "employer_name", "company_name", "firm_name", "business_name"],
        "city": ["city", "location", "address", "locality", "municipality"],
        "layoff_date": ["effective_date", "layoff_date", "date", "closure_date", "separation_date", "event_date"],
        "notice_date": ["notice_date", "received_date", "notice_received", "filing_date", "date_received"],
        "employees_laid_off": ["number_of_employees", "employees", "workers_affected", "affected_employees",
                                "no_employees", "num_affected", "laid_off", "employees_affected"],
        "layoff_type": ["type", "layoff_type", "event_type", "type_of_layoff", "action_type"],
        "notes": ["notes", "comments", "remarks", "reason", "description"],
    }

    def get_col(df, keys):
        for k in keys:
            if k in df.columns:
                return k
        return None

    company_col = get_col(df, col_maps["company"])
    city_col = get_col(df, col_maps["city"])
    date_col = get_col(df, col_maps["layoff_date"])
    notice_col = get_col(df, col_maps["notice_date"])
    count_col = get_col(df, col_maps["employees_laid_off"])
    type_col = get_col(df, col_maps["layoff_type"])
    notes_col = get_col(df, col_maps["notes"])

    if not company_col:
        log.warning(f"  {state}: could not find company column in {list(df.columns[:8])}")
        return []

    for _, row in df.iterrows():
        company = str(row.get(company_col, "")).strip()
        if not company or company.lower() in ("nan", "none", ""):
            continue

        raw_date = str(row.get(date_col, "")) if date_col else ""
        raw_notice = str(row.get(notice_col, "")) if notice_col else ""
        raw_count = str(row.get(count_col, "")) if count_col else ""
        city = str(row.get(city_col, "")).strip() if city_col else ""
        layoff_type = str(row.get(type_col, "")).strip() if type_col else ""
        notes = str(row.get(notes_col, "")).strip() if notes_col else ""

        layoff_date = normalize_date(raw_date)
        notice_date = normalize_date(raw_notice)
        emp_count = normalize_count(raw_count)

        record_id = make_record_id(company, state, layoff_date or raw_date, raw_count)

        records.append({
            "record_id": record_id,
            "company": company,
            "state": state,
            "country": "USA",
            "city": city,
            "layoff_date": layoff_date,
            "notice_date": notice_date,
            "employees_laid_off": emp_count,
            "layoff_type": layoff_type or "Layoff",
            "industry": "",          # WARN notices don't always include industry
            "source": f"WARN-{state}",
            "source_url": STATE_PORTAL_URLS.get(state, ""),
            "raw_notice_date": raw_date,
            "notes": notes[:300] if notes and notes.lower() != "nan" else "",
            "added_date": today,
        })

    log.info(f"  {state}: normalized {len(records)} records")
    return records


def load_existing_ids() -> set[str]:
    if not USA_CSV.exists():
        return set()
    with open(USA_CSV, newline="", encoding="utf-8") as f:
        return {row["record_id"] for row in csv.DictReader(f) if row.get("record_id")}


def append_new_records(new_records: list[dict]) -> int:
    existing_ids = load_existing_ids()
    fresh = [r for r in new_records if r["record_id"] not in existing_ids]

    if not fresh:
        return 0

    write_header = not USA_CSV.exists()
    with open(USA_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(fresh)

    return len(fresh)


def generate_stats() -> dict:
    if not USA_CSV.exists():
        return {}

    df = pd.read_csv(USA_CSV, dtype=str)
    df["employees_laid_off"] = pd.to_numeric(df["employees_laid_off"], errors="coerce").fillna(-1).astype(int)
    df["year_month"] = df["layoff_date"].str[:7]

    valid = df[df["employees_laid_off"] > 0]
    total = int(valid["employees_laid_off"].sum())

    by_state = valid.groupby("state")["employees_laid_off"].sum().sort_values(ascending=False).head(20).to_dict()
    by_type = df.groupby("layoff_type").size().to_dict()

    # Timeline
    timeline_raw = valid.groupby("year_month")["employees_laid_off"].sum()
    timeline = [{"month": k, "employees": int(v)} for k, v in sorted(timeline_raw.items()) if k and k != "nan"]

    top_companies = (
        valid.groupby("company")["employees_laid_off"].sum()
        .sort_values(ascending=False).head(25).reset_index()
        .rename(columns={"employees_laid_off": "total"})
        .to_dict(orient="records")
    )

    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_sources": ["WARN Act — US State Government Portals"],
        "summary": {
            "total_records": len(df),
            "total_employees_laid_off": total,
            "states_covered": int(df["state"].nunique()),
            "companies_tracked": int(df["company"].nunique()),
        },
        "by_state": {k: int(v) for k, v in by_state.items()},
        "by_type": {k: int(v) for k, v in by_type.items()},
        "timeline": timeline,
        "top_companies": top_companies,
        "records": df.where(pd.notna(df), None).to_dict(orient="records"),
    }

    with open(STATS_JSON, "w") as f:
        json.dump(stats, f, indent=2, default=str)

    DASHBOARD_JSON.parent.mkdir(exist_ok=True)
    with open(DASHBOARD_JSON, "w") as f:
        json.dump(stats, f, indent=2, default=str)

    return stats


def main():
    parser = argparse.ArgumentParser(description="WARN Act data pipeline")
    parser.add_argument("--states", nargs="+", default=HIGH_PRIORITY_STATES,
                        help="State codes to scrape (default: top 20 states)")
    parser.add_argument("--all-states", action="store_true",
                        help="Pass 'all' to warn-scraper (scrapes all supported states)")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip scraping, just re-normalize existing cache")
    parser.add_argument("--max-minutes", type=int, default=0, metavar="N",
                        help="Stop scraping after N minutes (0 = unlimited). Each state "
                             "also has a hard 2-minute cap. Example: --max-minutes 10")
    args = parser.parse_args()

    states = ["all"] if args.all_states else [s.upper() for s in args.states]

    log.info("=" * 60)
    log.info("  Global Layoff Tracker — WARN Act Pipeline")
    log.info("=" * 60)
    log.info(f"  States: {states}")
    log.info(f"  Output: {USA_CSV}")
    if args.max_minutes:
        log.info(f"  Time limit: {args.max_minutes} minutes (2 min cap per state)")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Step 1: Download
        if not args.skip_scrape:
            if states == ["all"]:
                scraped = run_warn_scraper(HIGH_PRIORITY_STATES, tmp, args.max_minutes)
            else:
                scraped = run_warn_scraper(states, tmp, args.max_minutes)
        else:
            scraped = {s: tmp / f"{s.lower()}.csv" for s in states if (tmp / f"{s.lower()}.csv").exists()}
            log.info(f"  Skipping scrape, using cached files: {list(scraped.keys())}")

        # Step 2: Normalize
        all_records = []
        for state, csv_path in scraped.items():
            records = normalize_state_csv(state, csv_path)
            all_records.extend(records)

        log.info(f"\n  Total normalized records: {len(all_records)}")

        # Step 3 & 4: Deduplicate and append
        added = append_new_records(all_records)
        log.info(f"  New records added: {added}")

    # Step 5: Stats
    stats = generate_stats()
    s = stats.get("summary", {})
    log.info(f"\n  📊 Stats:")
    log.info(f"     Total records    : {s.get('total_records', 0):,}")
    log.info(f"     Total laid off   : {s.get('total_employees_laid_off', 0):,}")
    log.info(f"     States covered   : {s.get('states_covered', 0)}")
    log.info(f"     Companies        : {s.get('companies_tracked', 0):,}")
    log.info(f"\n  ✅ Pipeline complete. Output: {USA_CSV}\n")


if __name__ == "__main__":
    main()

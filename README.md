# 🌍 Global Layoff Tracker

> Real-time layoff data pulled automatically from **US government WARN Act filings** + **international news sources**. 
> No fake data. No manual entry. Everything is scraped from primary sources daily.

![Daily Scrape](https://img.shields.io/github/actions/workflow/status/dahimbis/layoff-tracker/daily_scrape.yml?label=Daily%20Scrape&style=flat-square)
![Dashboard](https://img.shields.io/github/actions/workflow/status/dahimbis/layoff-tracker/deploy_dashboard.yml?label=Dashboard&style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## 🔴 Live Dashboard

**→ [dahimbis.github.io/layoff-tracker](https://dahimbis.github.io/layoff-tracker)**

---

## 📡 Where the Data Comes From

### 🇺🇸 USA - WARN Act (Automated, Government-Verified)

The federal **Worker Adjustment and Retraining Notification (WARN) Act** legally requires
companies with 100+ employees to file 60-day advance notice before mass layoffs.
Every US state publishes this data publicly.

This repo uses **[`warn-scraper`](https://github.com/biglocalnews/warn-scraper)**, 
a production Python package maintained by Stanford's [Big Local News](https://biglocalnews.org/) 
program to download WARN filings directly from state government websites.

**States covered (scraped daily):**

| State | Portal |
|-------|--------|
| CA | [edd.ca.gov](https://edd.ca.gov/en/jobs_and_training/layoff_services_warn) |
| NY | [labor.ny.gov](https://labor.ny.gov/app/warn/) |
| TX | [twc.texas.gov](https://www.twc.texas.gov/data-reports/warn-notice) |
| WA | [esd.wa.gov](https://www.esd.wa.gov/about-employees/WARN) |
| IL | Illinois Dept. of Employment Security |
| FL | FloridaJobs.org |
| PA | PA Dept. of Labor & Industry |
| OH | Ohio JFS |
| + 12 more | See `scripts/pipeline.py` for full list |

### 🌍 International - News Scraping (Pending Human Review)

For non-US companies, we scrape:
- **TechCrunch** layoffs tag RSS
- **Reuters** Technology RSS
- **BBC Business** RSS
- **Google News** RSS (layoffs keyword, multiple regions)
- **Hacker News** via Algolia API

International candidates land in `data/pending_global.csv` and are reviewed via GitHub Issues before being merged.

---

## 📁 Repo Structure

```
layoff-tracker/
├── data/
│   ├── layoffs_usa.csv       ← Auto-populated from WARN Act scraping
│   ├── pending_global.csv    ← International candidates needing review
│   ├── stats.json            ← Auto-generated dashboard data
│   └── pipeline.log          ← Daily scrape logs
│
├── scripts/
│   ├── pipeline.py           ← Main WARN Act pipeline (warn-scraper wrapper)
│   └── global_scraper.py     ← International news scraper
│
├── dashboard/
│   ├── index.html            ← GitHub Pages dashboard
│   └── data.json             ← Dashboard data (copy of stats.json)
│
├── .github/workflows/
│   ├── daily_scrape.yml      ← Runs pipeline + global scraper every day at 6am UTC
│   └── deploy_dashboard.yml  ← Deploys dashboard on data updates
│
└── requirements.txt
```

---

## 🚀 Setup (Get Real Data in 5 Minutes)

### 1. Fork & Clone
```bash
git clone https://github.com/dahimbis/layoff-tracker.git
cd layoff-tracker
```

### 2. Install
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline locally
```bash
# Scrape top 20 US states
python scripts/pipeline.py

# Scrape specific states
python scripts/pipeline.py --states CA NY TX WA

# Scrape ALL supported states (~40)
python scripts/pipeline.py --all-states

# Run global news scraper
python scripts/global_scraper.py
```

### 4. View the data
```bash
# Check what was scraped
wc -l data/layoffs_usa.csv
head -5 data/layoffs_usa.csv

# Serve dashboard locally
cd dashboard && python -m http.server 8080
# Open http://localhost:8080
```

### 5. Enable GitHub Actions (for daily automation)

In your GitHub repo:
1. Go to **Settings → Pages** → set source to **GitHub Actions**
2. Go to **Actions** tab → enable workflows
3. The `daily_scrape.yml` workflow will run every day at 6am UTC automatically

**That's it.** The pipeline will:
- Download fresh WARN filings from all state portals
- Deduplicate against existing data
- Commit new records directly to `data/layoffs_usa.csv`
- Regenerate dashboard data
- Auto-deploy the dashboard

---

## 📊 Data Schema (`layoffs_usa.csv`)

| Field | Description |
|-------|-------------|
| `record_id` | MD5 hash for deduplication |
| `company` | Employer name from WARN filing |
| `state` | US state code |
| `country` | Always `USA` for WARN data |
| `city` | City from filing |
| `layoff_date` | Effective date of layoffs |
| `notice_date` | Date notice was filed with state |
| `employees_laid_off` | Count from filing (-1 if unknown) |
| `layoff_type` | `Layoff`, `Closure`, or `Relocation` |
| `source` | `WARN-{STATE}` |
| `source_url` | State portal URL |

---

## 🤝 Contributing

### Add international layoffs
Review `data/pending_global.csv` - these are auto-scraped but need human verification of:
- `country` - fill in the actual country
- `company` -  verify the extracted name is correct
- `employees_laid_off` - verify the number

Open a PR editing `data/pending_global.csv` → `data/layoffs_global.csv`.

### Improve state scrapers
If a state scraper breaks, open an issue at [`biglocalnews/warn-scraper`](https://github.com/biglocalnews/warn-scraper/issues) (the upstream package).

---

## ⚠️ Notes

- WARN Act data typically lags real-world layoffs by days to weeks
- Some states are more timely than others (CA and NY are fastest)
- Not all layoffs trigger WARN Act requirements (too small, exceptions apply)
- International data quality varies — always check `source_url`

---

## 📄 License

MIT. WARN Act data is public domain (government records). News-scraped content is attributed to sources.

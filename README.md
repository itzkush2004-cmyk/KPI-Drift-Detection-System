# 📊 KPI Drift Detection System

An automated analytics pipeline that monitors 15 retail KPIs on a weekly cadence, detects statistically significant drift, and generates LLM-powered root cause hypotheses — replicating the kind of post-load QC workflow used in analytics consulting engagements.

---

## The Problem

In analytics consulting, 5+ weekly reports with 50–100 KPIs each create a massive manual QC burden. Analysts spend hours comparing current week to prior week across all metrics, increasing the chance of missing signals or making errors. Downstream: inaccurate commentary reaches the client.

## The Solution

An end-to-end pipeline that runs automatically after data loads, flags KPIs that have drifted beyond statistical thresholds, and uses an LLM to generate a structured hypothesis — not just "revenue is down 18%" but _why_ it likely happened, using correlated KPIs, day-of-week patterns, country-level breakdowns, and SKU-level concentration.

---

## Demo

![Report Screenshot](/report_screenshot.png)

**Three-section HTML report — generated automatically every week:**

- 🔴 Critical Issues — bad-direction drifts exceeding 30% WoW or Z > 2.5
- 🟡 Warnings — bad-direction drifts exceeding 15% WoW or Z > 1.5
- 🟢 Performing Well — good-direction swings of the same magnitude
- Each card has a collapsible AI Analysis dropdown with the full LLM narrative

---

## Architecture

```
CSV Extract (daily)
      │
      ▼
raw_transactions (SQLite)
      │
      ▼
kpi_daily  ──── 15 KPIs computed per day
      │
      ▼
kpi_weekly ──── Aggregated Mon–Sun
      │
      ▼
Drift Engine ── WoW % + Z-Score vs 13-week baseline
      │
      ▼
LLM Narrator ── Groq (Llama 3.3 70B) → root cause hypothesis
      │
      ▼
HTML Report + Streamlit Dashboard
```

---

## KPIs Monitored (15)

| #   | KPI                   | Higher is Better |
| --- | --------------------- | ---------------- |
| 1   | Daily Revenue         | ✅               |
| 2   | Avg Order Value       | ✅               |
| 3   | Revenue per Customer  | ✅               |
| 4   | Total Orders          | ✅               |
| 5   | Total Quantity Sold   | ✅               |
| 6   | Unique Customers      | ✅               |
| 7   | Unique SKUs Sold      | ✅               |
| 8   | Top SKU Revenue Share | ✅               |
| 9   | Avg Items per Order   | ✅               |
| 10  | Return Rate           | ❌               |
| 11  | Returned Revenue      | ❌               |
| 12  | UK Revenue Share      | ✅               |
| 13  | International Order % | ✅               |
| 14  | New Customer %        | ✅               |
| 15  | Avg Unit Price        | ✅               |

KPIs marked ❌ are inverted — an increase is treated as a negative signal and colored accordingly.

---

## Drift Detection Logic

For each KPI at end of week:

```
WoW % Change    = (Current Week - Prior Week) / |Prior Week|
Z-Score         = (Current Week - 13W Mean)  / 13W Std Dev

CRITICAL        → |Z| > 2.5  OR  |WoW| > 30%
WARNING         → |Z| > 1.5  OR  |WoW| > 15%

Good direction  → movement is favorable for that KPI's polarity
Bad direction   → movement is unfavorable
```

Severity + direction together determine which section a KPI lands in.

---

## LLM Narrative — The Wow Factor

When a KPI drifts, the system assembles a structured context payload before calling the LLM:

- Full 15-KPI snapshot for the week (to identify correlated movements)
- Day-of-week revenue breakdown (was it one bad day or consistent?)
- Top 3 countries by revenue (geography concentration)
- Top 3 SKUs by revenue (product concentration)
- Consecutive drift counter (is this a trend or a one-off?)

The LLM returns a structured response in three parts:

```
CORRELATED KPIs: [which other KPIs moved in the same direction]
HYPOTHESIS:      [2-3 sentence root cause]
RECOMMENDED ACTION: [one specific investigation step]
```

This is what separates it from a simple threshold alert — the system reasons across KPIs, not just flags a single number.

---

## Dataset

**UCI Online Retail II** — UK-based online retailer, Dec 2009 – Dec 2011  
~1M transactions at invoice-item grain  
Source: [Kaggle](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci) | License: CC BY 4.0

---

## Tech Stack

| Layer             | Technology                       |
| ----------------- | -------------------------------- |
| Storage           | SQLite (via Python `sqlite3`)    |
| Data Processing   | Pandas                           |
| LLM API           | Groq — Llama 3.3 70B (free tier) |
| Report Templating | Jinja2                           |
| Dashboard         | Streamlit + Plotly               |
| Scheduling        | APScheduler / manual CLI         |

---

## Setup

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/kpi-drift-detection.git
cd kpi-drift-detection

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your Groq API key
#    In config.py, replace:
GROQ_API_KEY = "YOUR_GROQ_API_KEY"
#    Get a free key at: https://console.groq.com

# 5. Add the dataset
#    Download online_retail_II.csv from Kaggle and place in project root
```

---

## Running the Pipeline

```bash
# Initialize DB (run once)
python daily_job.py --setup

# Backfill all historical data (~5–10 min)
python daily_job.py --backfill

# Simulate a single day
python daily_job.py --date 2011-06-12

# Run end-of-week processing for a specific week
python daily_job.py --week 2011-06-06

# Launch dashboard
streamlit run app.py
```

---

## File Structure

```
kpi-drift-detection/
├── config.py               # API key, paths, thresholds
├── db_setup.py             # SQLite schema
├── ingest.py               # Daily data loader
├── compute_kpis.py         # 15 KPI calculations
├── drift_engine.py         # WoW, Z-score, direction-aware bucketing
├── narrator.py             # Groq API integration + prompt engineering
├── report_generator.py     # Jinja2 HTML report
├── daily_job.py            # Pipeline orchestrator (CLI)
├── app.py                  # Streamlit dashboard
├── requirements.txt
└── reports/                # Auto-generated weekly HTML reports
```

---

## Relevance to Real Consulting Workflows

This project directly mirrors the post-extract QC step in a real analytics consulting pipeline:

- Client sends daily data extracts → loaded into SQL tables
- Daily jobs refresh dashboards (Power BI / Excel)
- **This system sits at the end of that chain** — after data loads, before reports go to the client
- In production: swap SQLite for SQL Server, swap CSV ingestion for stored procedure calls, plug the HTML report into an email distribution

---

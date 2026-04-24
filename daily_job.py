# =============================================================
# daily_job.py — Main orchestrator for the KPI Drift pipeline
#
# Simulates the daily job that runs after the client data extract
# is loaded. In production this would be a cron job / Task Scheduler.
#
# Usage:
#   python daily_job.py --date 2011-06-12       # run for a specific date
#   python daily_job.py --backfill               # run all dates in dataset
#   python daily_job.py --week 2011-06-06        # run drift for a specific week start
# =============================================================

import sqlite3
import argparse
from datetime import date, timedelta, datetime

from config import DB_PATH, SIM_START_DATE, SIM_END_DATE
from db_setup import create_tables
from ingest import ingest_date, get_available_dates
from compute_kpis import compute_daily_kpis, compute_weekly_kpis
from drift_engine import run_drift_detection
from narrator import generate_narratives
from report_generator import generate_html_report


def get_week_bounds(d: date) -> tuple[str, str]:
    """Returns (Monday, Sunday) of the week containing date d."""
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


def run_daily(sim_date: str, conn: sqlite3.Connection, run_drift: bool = True):
    """
    Full pipeline for a single simulated date:
      1. Ingest raw data for the date
      2. Compute daily KPIs
      3. If it's Sunday (end of week): aggregate weekly, detect drift, narrate, report
    """
    print(f"\n{'='*60}")
    print(f"  📅 Daily Job: {sim_date}")
    print(f"{'='*60}")

    # Step 1: Ingest
    rows = ingest_date(sim_date, conn)

    # Step 2: Compute daily KPIs (only if data was loaded)
    if rows > 0:
        compute_daily_kpis(sim_date, conn)
    else:
        print(f"  ⏭️  Skipping KPI compute (no data for {sim_date})")
        return

    # Step 3: End-of-week processing
    d = date.fromisoformat(sim_date)
    if d.weekday() == 6 and run_drift:   # Sunday
        week_start, week_end = get_week_bounds(d)
        print(f"\n  📊 End-of-week processing: {week_start} → {week_end}")
        run_end_of_week(week_start, week_end, conn)


def run_end_of_week(week_start: str, week_end: str, conn: sqlite3.Connection):
    """
    Runs the full weekly pipeline:
    aggregate → drift detect → narrate → report
    """
    # Weekly aggregation
    compute_weekly_kpis(week_start, week_end, conn)

    # Drift detection
    drift_records = run_drift_detection(week_start, week_end, conn)

    if not drift_records:
        print("  ✅ No drift detected this week.")
    else:
        # LLM narratives
        drift_records = generate_narratives(drift_records, conn)

    # HTML report (always generate, even if no drifts)
    report_path = generate_html_report(week_start, week_end, drift_records, conn)
    print(f"\n  📄 Report: {report_path}")


def backfill(conn: sqlite3.Connection):
    """
    Processes all available dates in the dataset sequentially.
    This seeds the DB with all historical data + drift history.
    Runs end-of-week logic only for Sundays.
    """
    all_dates = get_available_dates()
    start = date.fromisoformat(SIM_START_DATE)
    end   = date.fromisoformat(SIM_END_DATE)

    filtered = [d for d in all_dates if start <= d <= end]
    print(f"\n🚀 Backfill: {len(filtered)} dates from {SIM_START_DATE} to {SIM_END_DATE}")

    for d in filtered:
        run_daily(d.isoformat(), conn, run_drift=True)

    print(f"\n✅ Backfill complete.")


def main():
    parser = argparse.ArgumentParser(description="KPI Drift Detection — Daily Job")
    parser.add_argument("--date",     type=str, help="Run for a specific date (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="Run all dates in dataset")
    parser.add_argument("--week",     type=str, help="Run end-of-week for a specific week start (YYYY-MM-DD)")
    parser.add_argument("--setup",    action="store_true", help="Only run DB setup")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    if args.setup:
        print("✅ DB setup complete.")

    elif args.backfill:
        backfill(conn)

    elif args.date:
        run_daily(args.date, conn, run_drift=True)

    elif args.week:
        d = date.fromisoformat(args.week)
        monday = d - timedelta(days=d.weekday())
        sunday = monday + timedelta(days=6)
        run_end_of_week(monday.isoformat(), sunday.isoformat(), conn)

    else:
        # Default: run for the latest available date
        dates = get_available_dates()
        if dates:
            latest = max(dates)
            run_daily(latest.isoformat(), conn, run_drift=True)
        else:
            print("❌ No dates found. Load data first.")

    conn.close()


if __name__ == "__main__":
    main()

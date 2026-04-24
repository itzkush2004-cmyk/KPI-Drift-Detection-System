# =============================================================
# drift_engine.py — KPI Drift Detection (direction-aware)
# =============================================================

import sqlite3
import pandas as pd
import numpy as np
from datetime import date, timedelta
from config import DB_PATH, WOW_THRESHOLD, ZSCORE_THRESHOLD, CRITICAL_ZSCORE
from compute_kpis import KPI_META

KPI_COLUMNS = list(KPI_META.keys())

# ── Direction semantics ────────────────────────────────────────
# "lower_is_better" KPIs: an increase is BAD, a decrease is GOOD
LOWER_IS_BETTER = {"return_rate", "returned_revenue"}

# Everything else: higher is better
# This drives both severity bucket AND color in the report


def is_good_direction(kpi_name: str, wow_pct: float) -> bool:
    """Returns True if the movement is in the GOOD direction for this KPI."""
    if kpi_name in LOWER_IS_BETTER:
        return wow_pct <= 0   # decrease = good
    else:
        return wow_pct >= 0   # increase = good


def _get_weekly_history(week_start: str, conn: sqlite3.Connection, n_weeks: int = 14) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT * FROM kpi_weekly WHERE week_start < ? ORDER BY week_start DESC LIMIT ?",
        conn, params=(week_start, n_weeks)
    )
    return df.sort_values("week_start").reset_index(drop=True)


def _zscore(current: float, baseline: pd.Series) -> float:
    if len(baseline) < 3:
        return 0.0
    mu  = baseline.mean()
    std = baseline.std()
    if std == 0:
        return 0.0
    return (current - mu) / std


def _severity(kpi_name: str, zscore: float, wow_pct: float) -> str:
    """
    Severity is magnitude-only (how big the swing is).
    Direction (good/bad) is handled separately for bucketing and coloring.
    """
    az = abs(zscore)
    aw = abs(wow_pct)
    if az >= CRITICAL_ZSCORE or aw >= 0.30:
        return "CRITICAL"
    elif az >= ZSCORE_THRESHOLD or aw >= WOW_THRESHOLD:
        return "WARNING"
    return "STABLE"


def _consecutive_drift_count(kpi_name: str, week_start: str,
                              direction: str, conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("""
        SELECT wow_pct_change FROM drift_log
        WHERE kpi_name = ? AND week_start < ?
        ORDER BY week_start DESC LIMIT 8
    """, (kpi_name, week_start))
    rows = cur.fetchall()
    count = 0
    for (wow,) in rows:
        row_dir = "UP" if wow > 0 else "DOWN"
        if row_dir == direction and abs(wow) >= WOW_THRESHOLD:
            count += 1
        else:
            break
    return count


def _build_context_payload(week_start: str, week_end: str,
                            conn: sqlite3.Connection) -> dict:
    df = pd.read_sql_query(
        """
        SELECT invoice_date, quantity, unit_price, country, stock_code, description
        FROM raw_transactions
        WHERE invoice_date BETWEEN ? AND ? AND quantity > 0
        """,
        conn, params=(week_start, week_end)
    )
    if df.empty:
        return {}

    df["revenue"]      = df["quantity"] * df["unit_price"]
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])
    df["day_name"]     = df["invoice_date"].dt.day_name()

    dow_rev     = df.groupby("day_name")["revenue"].sum().round(2).to_dict()
    country_rev = (
        df.groupby("country")["revenue"].sum()
          .sort_values(ascending=False).head(3).round(2).to_dict()
    )
    sku_rev = (
        df.groupby(["stock_code", "description"])["revenue"].sum()
          .sort_values(ascending=False).head(3).reset_index()
    )
    top_skus = [
        {"sku": r["stock_code"], "desc": r["description"], "revenue": round(r["revenue"], 2)}
        for _, r in sku_rev.iterrows()
    ]
    return {
        "day_of_week_revenue": dow_rev,
        "top_countries":       country_rev,
        "top_skus":            top_skus,
    }


def run_drift_detection(week_start: str, week_end: str,
                        conn: sqlite3.Connection) -> list[dict]:
    """
    Returns drift records with these extra fields:
      - good_direction (bool): is the movement in the good direction?
      - bucket: 'CRITICAL_BAD', 'WARNING_BAD', 'WELL_PERFORMING'
    Only logs CRITICAL/WARNING to drift_log.
    WELL_PERFORMING (good direction, big positive swing) is included in
    the returned list for the report but NOT logged as a problem.
    """
    cur_row = pd.read_sql_query(
        "SELECT * FROM kpi_weekly WHERE week_start = ?",
        conn, params=(week_start,)
    )
    if cur_row.empty:
        print(f"  ⚠️  No weekly KPIs for {week_start}. Run compute first.")
        return []

    cur     = cur_row.iloc[0]
    history = _get_weekly_history(week_start, conn, n_weeks=13)

    prior_row = pd.read_sql_query(
        "SELECT * FROM kpi_weekly WHERE week_start < ? ORDER BY week_start DESC LIMIT 1",
        conn, params=(week_start,)
    )
    prior = prior_row.iloc[0] if not prior_row.empty else None

    context  = _build_context_payload(week_start, week_end, conn)
    now_str  = date.today().isoformat()

    drift_records = []

    for kpi in KPI_COLUMNS:
        cur_val   = float(cur[kpi])
        prior_val = float(prior[kpi]) if prior is not None else None
        baseline  = history[kpi].astype(float) if kpi in history.columns else pd.Series(dtype=float)

        if prior_val is not None and prior_val != 0:
            wow_pct = (cur_val - prior_val) / abs(prior_val)
        else:
            wow_pct = 0.0

        z   = _zscore(cur_val, baseline)
        sev = _severity(kpi, z, wow_pct)

        if sev == "STABLE":
            continue

        good = is_good_direction(kpi, wow_pct)

        # Bucket assignment
        if good:
            bucket = "WELL_PERFORMING"
        elif sev == "CRITICAL":
            bucket = "CRITICAL_BAD"
        else:
            bucket = "WARNING_BAD"

        direction = "UP" if wow_pct >= 0 else "DOWN"
        consec    = _consecutive_drift_count(kpi, week_start, direction, conn)

        record = {
            "week_start":        week_start,
            "week_end":          week_end,
            "kpi_name":          kpi,
            "kpi_label":         KPI_META[kpi]["label"],
            "current_value":     cur_val,
            "prior_week_value":  prior_val,
            "wow_pct_change":    round(wow_pct, 4),
            "zscore":            round(z, 3),
            "severity":          sev,
            "direction":         direction,
            "good_direction":    good,
            "bucket":            bucket,
            "consecutive_weeks": consec + 1,
            "context":           context,
            "all_kpis_current":  {k: float(cur[k]) for k in KPI_COLUMNS},
        }
        drift_records.append(record)

        # Only log problem drifts to drift_log
        if not good:
            conn.execute("""
                INSERT INTO drift_log
                (week_start, kpi_name, current_value, prior_week_value,
                 wow_pct_change, zscore, severity, consecutive_weeks, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                week_start, kpi, cur_val, prior_val,
                round(wow_pct, 4), round(z, 3), sev,
                consec + 1, now_str
            ))

    conn.commit()

    bad   = [r for r in drift_records if not r["good_direction"]]
    good_ = [r for r in drift_records if r["good_direction"]]
    print(f"  ✅ Drift detection: {len(bad)} problem KPI(s), {len(good_)} well-performing KPI(s) for {week_start}")
    return drift_records


def get_drift_summary(conn: sqlite3.Connection, last_n_weeks: int = 8) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT * FROM drift_log ORDER BY week_start DESC, severity DESC LIMIT ?",
        conn, params=(last_n_weeks * 20,)
    )
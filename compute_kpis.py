# =============================================================
# compute_kpis.py — Computes all 15 KPIs for a given date
#                   and writes to kpi_daily table.
# =============================================================

import sqlite3
import pandas as pd
from datetime import date, timedelta
from config import DB_PATH


def compute_daily_kpis(sim_date: str, conn: sqlite3.Connection) -> dict | None:
    """
    Reads raw_transactions for sim_date, computes 15 KPIs,
    upserts into kpi_daily. Returns the KPI dict or None if no data.
    """
    cur = conn.cursor()

    # ── Pull day's transactions ────────────────────────────────
    df = pd.read_sql_query(
        "SELECT * FROM raw_transactions WHERE invoice_date = ?",
        conn, params=(sim_date,)
    )

    if df.empty:
        print(f"  ⚠️  No transactions for {sim_date} — skipping KPI compute.")
        return None

    # ── Separate forward sales vs returns (negative qty = return) ──
    sales   = df[df["quantity"] > 0].copy()
    returns = df[df["quantity"] < 0].copy()

    sales["revenue"]   = sales["quantity"]   * sales["unit_price"]
    returns["rev_ret"] = returns["quantity"].abs() * returns["unit_price"]

    # ── KPI 1: Daily Revenue ───────────────────────────────────
    daily_revenue = sales["revenue"].sum()

    # ── KPI 2: Avg Order Value ─────────────────────────────────
    order_revenue = sales.groupby("invoice_no")["revenue"].sum()
    avg_order_value = order_revenue.mean() if len(order_revenue) > 0 else 0.0

    # ── KPI 3: Revenue per Customer ───────────────────────────
    cust_revenue = sales[sales["customer_id"] != "UNKNOWN"] \
                       .groupby("customer_id")["revenue"].sum()
    revenue_per_customer = cust_revenue.mean() if len(cust_revenue) > 0 else 0.0

    # ── KPI 4: Total Orders ───────────────────────────────────
    total_orders = sales["invoice_no"].nunique()

    # ── KPI 5: Total Quantity Sold ────────────────────────────
    total_quantity = int(sales["quantity"].sum())

    # ── KPI 6: Unique Customers ───────────────────────────────
    unique_customers = sales[sales["customer_id"] != "UNKNOWN"]["customer_id"].nunique()

    # ── KPI 7: Unique SKUs Sold ───────────────────────────────
    unique_skus = sales["stock_code"].nunique()

    # ── KPI 8: Top SKU Revenue Share ─────────────────────────
    sku_rev = sales.groupby("stock_code")["revenue"].sum()
    top_sku_revenue_share = (
        (sku_rev.max() / daily_revenue) if daily_revenue > 0 else 0.0
    )

    # ── KPI 9: Avg Items per Order ────────────────────────────
    items_per_order = sales.groupby("invoice_no")["quantity"].sum()
    avg_items_per_order = items_per_order.mean() if len(items_per_order) > 0 else 0.0

    # ── KPI 10: Return Rate (% of invoices that have a return) ──
    total_invoices = df["invoice_no"].nunique()
    return_invoices = returns["invoice_no"].nunique()
    return_rate = (return_invoices / total_invoices) if total_invoices > 0 else 0.0

    # ── KPI 11: Returned Revenue ──────────────────────────────
    returned_revenue = returns["rev_ret"].sum()

    # ── KPI 12: UK Revenue Share ──────────────────────────────
    uk_rev = sales[sales["country"] == "United Kingdom"]["revenue"].sum()
    uk_revenue_share = (uk_rev / daily_revenue) if daily_revenue > 0 else 0.0

    # ── KPI 13: International Order % ────────────────────────
    intl_orders = sales[sales["country"] != "United Kingdom"]["invoice_no"].nunique()
    international_order_pct = (
        (intl_orders / total_orders) if total_orders > 0 else 0.0
    )

    # ── KPI 14: New Customer % ────────────────────────────────
    # A customer is "new" if their first_date equals sim_date
    day_customers = sales[sales["customer_id"] != "UNKNOWN"]["customer_id"].unique().tolist()
    new_count = 0
    if day_customers:
        placeholders = ",".join(["?" for _ in day_customers])
        cur.execute(
            f"SELECT customer_id FROM customer_first_seen "
            f"WHERE customer_id IN ({placeholders}) AND first_date = ?",
            day_customers + [sim_date]
        )
        new_count = len(cur.fetchall())
    new_customer_pct = (new_count / unique_customers) if unique_customers > 0 else 0.0

    # ── KPI 15: Avg Unit Price ────────────────────────────────
    avg_unit_price = sales["unit_price"].mean() if len(sales) > 0 else 0.0

    # ── Write to kpi_daily ────────────────────────────────────
    kpi = {
        "kpi_date":               sim_date,
        "daily_revenue":          round(daily_revenue, 2),
        "avg_order_value":        round(avg_order_value, 2),
        "revenue_per_customer":   round(revenue_per_customer, 2),
        "total_orders":           total_orders,
        "total_quantity":         total_quantity,
        "unique_customers":       unique_customers,
        "unique_skus":            unique_skus,
        "top_sku_revenue_share":  round(top_sku_revenue_share, 4),
        "avg_items_per_order":    round(avg_items_per_order, 2),
        "return_rate":            round(return_rate, 4),
        "returned_revenue":       round(returned_revenue, 2),
        "uk_revenue_share":       round(uk_revenue_share, 4),
        "international_order_pct":round(international_order_pct, 4),
        "new_customer_pct":       round(new_customer_pct, 4),
        "avg_unit_price":         round(avg_unit_price, 2),
    }

    conn.execute("""
        INSERT OR REPLACE INTO kpi_daily VALUES (
            :kpi_date, :daily_revenue, :avg_order_value, :revenue_per_customer,
            :total_orders, :total_quantity, :unique_customers, :unique_skus,
            :top_sku_revenue_share, :avg_items_per_order, :return_rate,
            :returned_revenue, :uk_revenue_share, :international_order_pct,
            :new_customer_pct, :avg_unit_price
        )
    """, kpi)
    conn.commit()

    print(f"  ✅ KPIs computed for {sim_date} | Revenue: £{daily_revenue:,.2f}")
    return kpi


def compute_weekly_kpis(week_start: str, week_end: str, conn: sqlite3.Connection) -> dict | None:
    """
    Aggregates kpi_daily rows for the given week into kpi_weekly.
    week_start and week_end are ISO date strings (Monday → Sunday).
    """
    df = pd.read_sql_query(
        "SELECT * FROM kpi_daily WHERE kpi_date BETWEEN ? AND ?",
        conn, params=(week_start, week_end)
    )

    if df.empty:
        print(f"  ⚠️  No daily KPIs found for week {week_start} → {week_end}")
        return None

    # Simple mean across trading days (excludes weekend gaps naturally)
    agg = df.drop(columns=["kpi_date"]).mean(numeric_only=True)

    weekly = {
        "week_start":              week_start,
        "week_end":                week_end,
        "daily_revenue":           round(agg["daily_revenue"], 2),
        "avg_order_value":         round(agg["avg_order_value"], 2),
        "revenue_per_customer":    round(agg["revenue_per_customer"], 2),
        "total_orders":            int(agg["total_orders"]),
        "total_quantity":          int(agg["total_quantity"]),
        "unique_customers":        int(agg["unique_customers"]),
        "unique_skus":             int(agg["unique_skus"]),
        "top_sku_revenue_share":   round(agg["top_sku_revenue_share"], 4),
        "avg_items_per_order":     round(agg["avg_items_per_order"], 2),
        "return_rate":             round(agg["return_rate"], 4),
        "returned_revenue":        round(agg["returned_revenue"], 2),
        "uk_revenue_share":        round(agg["uk_revenue_share"], 4),
        "international_order_pct": round(agg["international_order_pct"], 4),
        "new_customer_pct":        round(agg["new_customer_pct"], 4),
        "avg_unit_price":          round(agg["avg_unit_price"], 2),
    }

    conn.execute("""
        INSERT OR REPLACE INTO kpi_weekly VALUES (
            :week_start, :week_end, :daily_revenue, :avg_order_value,
            :revenue_per_customer, :total_orders, :total_quantity,
            :unique_customers, :unique_skus, :top_sku_revenue_share,
            :avg_items_per_order, :return_rate, :returned_revenue,
            :uk_revenue_share, :international_order_pct,
            :new_customer_pct, :avg_unit_price
        )
    """, weekly)
    conn.commit()

    print(f"  ✅ Weekly KPIs aggregated: {week_start} → {week_end}")
    return weekly


# ── KPI metadata — used by drift engine and narrator ──────────
KPI_META = {
    "daily_revenue":           {"label": "Daily Revenue",            "format": "£{:,.2f}",  "unit": "GBP"},
    "avg_order_value":         {"label": "Avg Order Value",          "format": "£{:,.2f}",  "unit": "GBP"},
    "revenue_per_customer":    {"label": "Revenue per Customer",     "format": "£{:,.2f}",  "unit": "GBP"},
    "total_orders":            {"label": "Total Orders",             "format": "{:,}",       "unit": "count"},
    "total_quantity":          {"label": "Total Quantity Sold",      "format": "{:,}",       "unit": "units"},
    "unique_customers":        {"label": "Unique Customers",         "format": "{:,}",       "unit": "count"},
    "unique_skus":             {"label": "Unique SKUs Sold",         "format": "{:,}",       "unit": "count"},
    "top_sku_revenue_share":   {"label": "Top SKU Revenue Share",    "format": "{:.1%}",     "unit": "%"},
    "avg_items_per_order":     {"label": "Avg Items per Order",      "format": "{:.1f}",     "unit": "items"},
    "return_rate":             {"label": "Return Rate",              "format": "{:.1%}",     "unit": "%"},
    "returned_revenue":        {"label": "Returned Revenue",         "format": "£{:,.2f}",  "unit": "GBP"},
    "uk_revenue_share":        {"label": "UK Revenue Share",         "format": "{:.1%}",     "unit": "%"},
    "international_order_pct": {"label": "International Order %",    "format": "{:.1%}",     "unit": "%"},
    "new_customer_pct":        {"label": "New Customer %",           "format": "{:.1%}",     "unit": "%"},
    "avg_unit_price":          {"label": "Avg Unit Price",           "format": "£{:.2f}",   "unit": "GBP"},
}

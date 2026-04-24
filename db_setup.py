# =============================================================
# db_setup.py — Creates all SQLite tables
# Run once before anything else.
# =============================================================

import sqlite3
import os
from config import DB_PATH

def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()

    # ── Raw transactions (append-only, mimics daily SQL extract load) ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_transactions (
            invoice_no    TEXT,
            stock_code    TEXT,
            description   TEXT,
            quantity      INTEGER,
            invoice_date  TEXT,      -- stored as ISO date string YYYY-MM-DD
            unit_price    REAL,
            customer_id   TEXT,
            country       TEXT,
            load_date     TEXT        -- the date this row was "loaded" (sim date)
        )
    """)

    # ── Daily KPI snapshot ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_daily (
            kpi_date                  TEXT,
            daily_revenue             REAL,
            avg_order_value           REAL,
            revenue_per_customer      REAL,
            total_orders              INTEGER,
            total_quantity            INTEGER,
            unique_customers          INTEGER,
            unique_skus               INTEGER,
            top_sku_revenue_share     REAL,
            avg_items_per_order       REAL,
            return_rate               REAL,
            returned_revenue          REAL,
            uk_revenue_share          REAL,
            international_order_pct   REAL,
            new_customer_pct          REAL,
            avg_unit_price            REAL,
            PRIMARY KEY (kpi_date)
        )
    """)

    # ── Weekly KPI aggregates ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_weekly (
            week_start                TEXT,
            week_end                  TEXT,
            daily_revenue             REAL,
            avg_order_value           REAL,
            revenue_per_customer      REAL,
            total_orders              INTEGER,
            total_quantity            INTEGER,
            unique_customers          INTEGER,
            unique_skus               INTEGER,
            top_sku_revenue_share     REAL,
            avg_items_per_order       REAL,
            return_rate               REAL,
            returned_revenue          REAL,
            uk_revenue_share          REAL,
            international_order_pct   REAL,
            new_customer_pct          REAL,
            avg_unit_price            REAL,
            PRIMARY KEY (week_start)
        )
    """)

    # ── Drift log — every flagged KPI per week ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drift_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start       TEXT,
            kpi_name         TEXT,
            current_value    REAL,
            prior_week_value REAL,
            wow_pct_change   REAL,
            zscore           REAL,
            severity         TEXT,   -- 'CRITICAL', 'WARNING', 'STABLE'
            consecutive_weeks INTEGER DEFAULT 1,  -- how many weeks in a row drifted
            llm_narrative    TEXT,
            created_at       TEXT
        )
    """)

    # ── Customer history — tracks first-seen date for new vs repeat logic ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_first_seen (
            customer_id   TEXT PRIMARY KEY,
            first_date    TEXT
        )
    """)

    conn.commit()
    print("✅ All tables created successfully.")


if __name__ == "__main__":
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    conn.close()
    print(f"✅ Database ready at: {DB_PATH}")

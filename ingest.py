# =============================================================
# ingest.py — Daily data loader (simulates incremental extract)
#
# In real life: SQL Server gets a daily extract from the client.
# Here: we partition the CSV by date and load one day at a time.
# Call ingest_date("2011-01-15") to load that day's transactions.
# =============================================================

import sqlite3
import pandas as pd
import os
from datetime import date, timedelta
from config import CSV_PATH, DB_PATH

# ── Load and clean the full CSV once into memory ──────────────
_df_cache = None

def _load_source() -> pd.DataFrame:
    global _df_cache
    if _df_cache is not None:
        return _df_cache

    print(f"📂 Loading source CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1", low_memory=False)

    # Standardise column names
    df.columns = [c.strip() for c in df.columns]
    rename_map = {
        "Invoice":     "invoice_no",
        "StockCode":   "stock_code",
        "Description": "description",
        "Quantity":    "quantity",
        "InvoiceDate": "invoice_date",
        "Price":       "unit_price",
        "Customer ID": "customer_id",
        "Country":     "country",
    }
    df = df.rename(columns=rename_map)

    # Parse date — the CSV has datetime strings
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df = df.dropna(subset=["invoice_date"])
    df["invoice_date"] = df["invoice_date"].dt.date   # keep as date only

    # Drop rows missing critical fields
    df = df.dropna(subset=["invoice_no", "quantity", "unit_price"])
    df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce").fillna(0).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0)
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].replace("nan", "UNKNOWN")

    _df_cache = df
    print(f"✅ Source loaded: {len(df):,} rows | "
          f"{df['invoice_date'].min()} → {df['invoice_date'].max()}")
    return df


def ingest_date(sim_date: str, conn: sqlite3.Connection) -> int:
    """
    Load all transactions for sim_date into raw_transactions.
    Returns number of rows inserted.
    """
    target = date.fromisoformat(sim_date)
    df = _load_source()

    day_df = df[df["invoice_date"] == target].copy()
    if day_df.empty:
        print(f"  ⚠️  No data for {sim_date} (weekend or holiday — normal).")
        return 0

    # Check if already loaded (idempotent)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM raw_transactions WHERE load_date = ?", (sim_date,)
    )
    if cur.fetchone()[0] > 0:
        print(f"  ℹ️  {sim_date} already loaded. Skipping.")
        return 0

    # Update customer_first_seen
    new_customers = []
    for cid in day_df["customer_id"].unique():
        if cid == "UNKNOWN":
            continue
        cur.execute(
            "INSERT OR IGNORE INTO customer_first_seen (customer_id, first_date) VALUES (?,?)",
            (cid, sim_date)
        )

    # Insert transactions
    day_df["load_date"] = sim_date
    day_df["invoice_date"] = day_df["invoice_date"].astype(str)

    rows = day_df[[
        "invoice_no", "stock_code", "description", "quantity",
        "invoice_date", "unit_price", "customer_id", "country", "load_date"
    ]].values.tolist()

    conn.executemany("""
        INSERT INTO raw_transactions
        (invoice_no, stock_code, description, quantity, invoice_date,
         unit_price, customer_id, country, load_date)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    print(f"  ✅ Ingested {len(rows):,} rows for {sim_date}")
    return len(rows)


def get_available_dates() -> list:
    """Returns sorted list of all unique dates in the source CSV."""
    df = _load_source()
    return sorted(df["invoice_date"].unique())


if __name__ == "__main__":
    # Quick test — load a single day
    conn = sqlite3.connect(DB_PATH)
    ingest_date("2011-01-04", conn)
    conn.close()

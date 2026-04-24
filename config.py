# =============================================================
# config.py — Central configuration for KPI Drift System
# =============================================================

import os

# --- Paths ---
BASE_DIR = r"C:\KPI Drift Detection System"
CSV_PATH = os.path.join(BASE_DIR, "online_retail_II.csv")
DB_PATH  = os.path.join(BASE_DIR, "retail.db")

# --- LLM (Groq) ---
GROQ_API_KEY = ""   # <-- get free key at console.groq.com
GROQ_MODEL   = "llama-3.3-70b-versatile"
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"

# --- Drift Detection Thresholds ---
WOW_THRESHOLD    = 0.15   # 15% WoW change triggers a flag
ZSCORE_THRESHOLD = 1.5    # Z-score beyond this = drift
CRITICAL_ZSCORE  = 2.5    # Z-score beyond this = critical

# --- Scheduling ---
SIM_START_DATE = "2010-12-01"
SIM_END_DATE   = "2011-12-09"

# --- Reporting ---
REPORT_DIR  = os.path.join(BASE_DIR, "reports")
REPORT_DAY  = "Sunday"

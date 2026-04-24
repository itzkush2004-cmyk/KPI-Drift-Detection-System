# =============================================================
# narrator.py — LLM Narrative Generator (Groq / Llama 3.3 70B)
# =============================================================

import sqlite3
import requests
from config import GROQ_API_KEY, GROQ_URL, GROQ_MODEL, DB_PATH
from compute_kpis import KPI_META


def _build_prompt(drift_record: dict) -> str:
    kpi_label  = drift_record["kpi_label"]
    cur_val    = drift_record["current_value"]
    prior_val  = drift_record["prior_week_value"]
    wow_pct    = drift_record["wow_pct_change"] * 100
    z          = drift_record["zscore"]
    direction  = drift_record["direction"]
    sev        = drift_record["severity"]
    consec     = drift_record["consecutive_weeks"]
    ctx        = drift_record.get("context", {})
    all_kpis   = drift_record.get("all_kpis_current", {})

    # Full KPI snapshot
    kpi_snapshot_lines = []
    for k, v in all_kpis.items():
        meta  = KPI_META.get(k, {})
        label = meta.get("label", k)
        fmt   = meta.get("format", "{}")
        try:
            formatted = fmt.format(v)
        except Exception:
            formatted = str(v)
        kpi_snapshot_lines.append(f"  - {label}: {formatted}")
    kpi_snapshot = "\n".join(kpi_snapshot_lines)

    # Context breakdowns
    dow = ctx.get("day_of_week_revenue", {})
    dow_str = ", ".join([f"{d}: £{v:,.0f}" for d, v in sorted(dow.items())])

    countries = ctx.get("top_countries", {})
    country_str = ", ".join([f"{c}: £{v:,.0f}" for c, v in countries.items()])

    skus = ctx.get("top_skus", [])
    sku_str = "; ".join([
        f"{s['desc']} ({s['sku']}): £{s['revenue']:,.0f}" for s in skus
    ])

    consecutive_note = (
        f"⚠️ This is the {consec}{'st' if consec==1 else 'nd' if consec==2 else 'rd' if consec==3 else 'th'} "
        f"consecutive week this KPI has drifted {direction}. This is a PERSISTENT TREND, not a one-off spike."
        if consec > 1 else
        "This appears to be a one-off deviation (first occurrence)."
    )

    prompt = f"""
You are a senior analytics consultant analyzing weekly KPI drift for an e-commerce retail client.

FLAGGED KPI
-----------
KPI: {kpi_label}
Current Week Value: {cur_val}
Prior Week Value: {prior_val}
WoW Change: {wow_pct:+.1f}%
Z-Score vs 13-Week Baseline: {z:+.2f}
Severity: {sev}
Direction: {direction}

{consecutive_note}

FULL CURRENT WEEK KPI SNAPSHOT (all 15 KPIs)
---------------------------------------------
{kpi_snapshot}

WEEKLY TRANSACTION BREAKDOWN
-----------------------------
Revenue by Day of Week: {dow_str if dow_str else "Not available"}
Top 3 Countries by Revenue: {country_str if country_str else "Not available"}
Top 3 SKUs by Revenue: {sku_str if sku_str else "Not available"}

TASK
----
1. Identify which other KPIs in the snapshot are CORRELATED with this drift. Cite specific values.
2. Generate a concise ROOT CAUSE HYPOTHESIS explaining WHY this KPI changed. Use day-of-week, country, and SKU data as evidence where relevant.
3. State ONE specific RECOMMENDED ACTION the analytics team should investigate.

FORMAT your response EXACTLY as:

CORRELATED KPIs: [list the correlated KPIs and values in 1-2 sentences]

HYPOTHESIS: [2-3 sentence root cause hypothesis]

RECOMMENDED ACTION: [1 specific, actionable investigation step]

Be direct and specific. Do not hedge. Do not repeat numbers verbatim — synthesize into an insight.
""".strip()

    return prompt


def call_groq(prompt: str) -> str:
    """Calls Groq API (OpenAI-compatible) and returns the text response."""
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {
                "role":    "system",
                "content": "You are a senior analytics consultant. Be concise, specific, and data-driven."
            },
            {
                "role":    "user",
                "content": prompt
            }
        ],
        "temperature": 0.3,
        "max_tokens":  400,
    }

    try:
        resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except requests.exceptions.HTTPError as e:
        return f"[API Error {resp.status_code}]: {resp.text[:200]}"
    except Exception as e:
        return f"[Error calling Groq]: {str(e)}"


def generate_narratives(drift_records: list[dict], conn: sqlite3.Connection) -> list[dict]:
    if not drift_records:
        print("  ℹ️  No drift records to narrate.")
        return []

    print(f"  🤖 Generating LLM narratives for {len(drift_records)} drifted KPI(s)...")

    for record in drift_records:
        kpi   = record["kpi_name"]
        week  = record["week_start"]
        label = record["kpi_label"]

        print(f"     → Narrating: {label} ({record['severity']})")
        prompt    = _build_prompt(record)
        narrative = call_groq(prompt)

        record["llm_narrative"] = narrative

        conn.execute("""
            UPDATE drift_log
            SET llm_narrative = ?
            WHERE week_start = ? AND kpi_name = ?
        """, (narrative, week, kpi))

    conn.commit()
    print(f"  ✅ Narratives generated and saved.")
    return drift_records


if __name__ == "__main__":
    # Quick test
    mock = {
        "week_start":        "2011-06-06",
        "week_end":          "2011-06-12",
        "kpi_name":          "daily_revenue",
        "kpi_label":         "Daily Revenue",
        "current_value":     12000.0,
        "prior_week_value":  16000.0,
        "wow_pct_change":    -0.25,
        "zscore":            -2.1,
        "severity":          "CRITICAL",
        "direction":         "DOWN",
        "consecutive_weeks": 2,
        "context": {
            "day_of_week_revenue": {"Monday": 3000, "Tuesday": 2000, "Wednesday": 4000, "Thursday": 3000},
            "top_countries":       {"United Kingdom": 9000, "Germany": 2000, "France": 1000},
            "top_skus":            [{"sku": "85123A", "desc": "WHITE HANGING HEART T-LIGHT HOLDER", "revenue": 1200}]
        },
        "all_kpis_current": {k: 0.0 for k in KPI_META.keys()},
    }
    conn = sqlite3.connect(DB_PATH)
    result = generate_narratives([mock], conn)
    print("\n--- NARRATIVE ---")
    print(result[0]["llm_narrative"])
    conn.close()

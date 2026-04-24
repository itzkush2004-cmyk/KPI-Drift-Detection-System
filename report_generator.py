# =============================================================
# report_generator.py — HTML Weekly Digest (v2)
#
# Sections:
#   🔴 Critical Issues    — bad direction + CRITICAL magnitude
#   🟡 Warnings           — bad direction + WARNING magnitude
#   🟢 Performing Well    — good direction, any magnitude
#
# Colors driven by direction × KPI polarity, NOT Z-score.
# Z-score removed from report entirely.
# Each card has a dropdown for the full AI narrative.
# =============================================================

import os
import sqlite3
import pandas as pd
from datetime import date
from jinja2 import Template
from config import REPORT_DIR, DB_PATH
from compute_kpis import KPI_META
from drift_engine import LOWER_IS_BETTER

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>KPI Drift Report — {{ week_start }}</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: #f0f2f7;
    color: #1a1a2e;
    padding: 28px 20px;
  }

  /* ── Header ── */
  .header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    color: white;
    padding: 28px 32px;
    border-radius: 14px;
    margin-bottom: 28px;
  }
  .header h1  { font-size: 20px; font-weight: 700; margin-bottom: 6px; }
  .header sub { font-size: 13px; opacity: 0.65; }

  /* ── Summary pills ── */
  .summary {
    display: flex;
    gap: 16px;
    margin-bottom: 32px;
    flex-wrap: wrap;
  }
  .pill {
    flex: 1;
    min-width: 140px;
    background: white;
    border-radius: 12px;
    padding: 18px 20px;
    text-align: center;
    box-shadow: 0 2px 10px rgba(0,0,0,0.06);
  }
  .pill .num  { font-size: 34px; font-weight: 800; line-height: 1; }
  .pill .lbl  { font-size: 12px; color: #777; margin-top: 5px; }
  .pill.red   .num { color: #e63946; }
  .pill.amber .num { color: #e07b22; }
  .pill.green .num { color: #2a9d8f; }

  /* ── Section container (3 columns) ── */
  .sections {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 20px;
    align-items: start;
  }

  .section { display: flex; flex-direction: column; gap: 12px; }

  .section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 16px;
    border-radius: 10px;
    font-size: 13px;
    font-weight: 700;
    letter-spacing: 0.4px;
    margin-bottom: 4px;
  }
  .section-header.critical { background: #fde8ea; color: #c0303b; }
  .section-header.warning  { background: #fef3e2; color: #b5620e; }
  .section-header.good     { background: #e4f5f2; color: #1f7a6e; }

  .empty-state {
    background: white;
    border-radius: 10px;
    padding: 20px;
    text-align: center;
    font-size: 13px;
    color: #aaa;
    box-shadow: 0 1px 6px rgba(0,0,0,0.05);
  }

  /* ── KPI Card ── */
  .card {
    background: white;
    border-radius: 11px;
    padding: 16px 18px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    border-left: 5px solid #ccc;
  }
  .card.critical-bad { border-left-color: #e63946; }
  .card.warning-bad  { border-left-color: #e07b22; }
  .card.well         { border-left-color: #2a9d8f; }

  .card-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 10px;
  }
  .card-top .kpi-name {
    font-size: 13px;
    font-weight: 700;
    color: #1a1a2e;
    line-height: 1.3;
  }

  .badge {
    font-size: 10px;
    font-weight: 700;
    padding: 3px 9px;
    border-radius: 20px;
    white-space: nowrap;
    flex-shrink: 0;
    margin-left: 8px;
  }
  .badge.critical { background: #fde8ea; color: #c0303b; }
  .badge.warning  { background: #fef3e2; color: #b5620e; }
  .badge.good     { background: #e4f5f2; color: #1f7a6e; }

  /* ── Metric row ── */
  .metrics {
    display: flex;
    gap: 16px;
    margin-bottom: 10px;
    flex-wrap: wrap;
  }
  .metric .val {
    font-size: 17px;
    font-weight: 700;
    line-height: 1.1;
  }
  .metric .lbl {
    font-size: 10px;
    color: #999;
    margin-top: 2px;
  }

  /* Direction-aware WoW colors */
  .wow-good { color: #2a9d8f; }   /* green  = good movement  */
  .wow-bad  { color: #e63946; }   /* red    = bad movement   */

  /* ── Consecutive drift badge ── */
  .consec {
    font-size: 11px;
    color: #e07b22;
    font-weight: 600;
    margin-bottom: 8px;
  }

  /* ── Dropdown for AI narrative ── */
  details {
    border-top: 1px solid #f0f0f0;
    padding-top: 8px;
    margin-top: 4px;
  }
  details summary {
    font-size: 11px;
    font-weight: 600;
    color: #5c6bc0;
    cursor: pointer;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 6px;
    user-select: none;
  }
  details summary::before {
    content: '▶';
    font-size: 9px;
    transition: transform 0.2s;
  }
  details[open] summary::before { transform: rotate(90deg); }
  details summary::-webkit-details-marker { display: none; }

  .narrative {
    background: #f5f7ff;
    border-radius: 8px;
    padding: 12px 14px;
    margin-top: 8px;
    font-size: 12px;
    line-height: 1.75;
    color: #333;
    border: 1px solid #e3e7f5;
    white-space: pre-wrap;
  }

  /* ── Footer ── */
  .footer {
    text-align: center;
    font-size: 11px;
    color: #bbb;
    margin-top: 36px;
  }

  @media (max-width: 900px) {
    .sections { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <h1>📊 KPI Drift Detection Report</h1>
  <sub>Week: {{ week_start }} → {{ week_end }} &nbsp;|&nbsp; Generated: {{ generated_on }} &nbsp;|&nbsp; {{ total_kpis }} KPIs Monitored</sub>
</div>

<!-- Summary pills -->
<div class="summary">
  <div class="pill red">
    <div class="num">{{ critical_count }}</div>
    <div class="lbl">🔴 Critical Issues</div>
  </div>
  <div class="pill amber">
    <div class="num">{{ warning_count }}</div>
    <div class="lbl">🟡 Warnings</div>
  </div>
  <div class="pill green">
    <div class="num">{{ good_count }}</div>
    <div class="lbl">🟢 Performing Well</div>
  </div>
</div>

<!-- 3-column sections -->
<div class="sections">

  <!-- CRITICAL -->
  <div class="section">
    <div class="section-header critical">🔴 Critical Issues</div>
    {% if critical_records %}
      {% for r in critical_records %}
      <div class="card critical-bad">
        <div class="card-top">
          <div class="kpi-name">{{ r.kpi_label }}</div>
          <span class="badge critical">CRITICAL</span>
        </div>
        <div class="metrics">
          <div class="metric">
            <div class="val">{{ r.current_fmt }}</div>
            <div class="lbl">This Week</div>
          </div>
          <div class="metric">
            <div class="val">{{ r.prior_fmt }}</div>
            <div class="lbl">Prior Week</div>
          </div>
          <div class="metric">
            <div class="val {{ 'wow-good' if r.good_direction else 'wow-bad' }}">
              {{ '+' if r.wow_pct_change > 0 else '' }}{{ "%.1f"|format(r.wow_pct_change * 100) }}%
            </div>
            <div class="lbl">WoW Change</div>
          </div>
        </div>
        {% if r.consecutive_weeks > 1 %}
        <div class="consec">⚠️ {{ r.consecutive_weeks }} consecutive weeks {{ r.direction }}</div>
        {% endif %}
        {% if r.llm_narrative %}
        <details>
          <summary>🤖 View AI Analysis</summary>
          <div class="narrative">{{ r.llm_narrative }}</div>
        </details>
        {% endif %}
      </div>
      {% endfor %}
    {% else %}
      <div class="empty-state">No critical issues this week ✅</div>
    {% endif %}
  </div>

  <!-- WARNING -->
  <div class="section">
    <div class="section-header warning">🟡 Warnings</div>
    {% if warning_records %}
      {% for r in warning_records %}
      <div class="card warning-bad">
        <div class="card-top">
          <div class="kpi-name">{{ r.kpi_label }}</div>
          <span class="badge warning">WARNING</span>
        </div>
        <div class="metrics">
          <div class="metric">
            <div class="val">{{ r.current_fmt }}</div>
            <div class="lbl">This Week</div>
          </div>
          <div class="metric">
            <div class="val">{{ r.prior_fmt }}</div>
            <div class="lbl">Prior Week</div>
          </div>
          <div class="metric">
            <div class="val {{ 'wow-good' if r.good_direction else 'wow-bad' }}">
              {{ '+' if r.wow_pct_change > 0 else '' }}{{ "%.1f"|format(r.wow_pct_change * 100) }}%
            </div>
            <div class="lbl">WoW Change</div>
          </div>
        </div>
        {% if r.consecutive_weeks > 1 %}
        <div class="consec">⚠️ {{ r.consecutive_weeks }} consecutive weeks {{ r.direction }}</div>
        {% endif %}
        {% if r.llm_narrative %}
        <details>
          <summary>🤖 View AI Analysis</summary>
          <div class="narrative">{{ r.llm_narrative }}</div>
        </details>
        {% endif %}
      </div>
      {% endfor %}
    {% else %}
      <div class="empty-state">No warnings this week ✅</div>
    {% endif %}
  </div>

  <!-- WELL PERFORMING -->
  <div class="section">
    <div class="section-header good">🟢 Performing Well</div>
    {% if good_records %}
      {% for r in good_records %}
      <div class="card well">
        <div class="card-top">
          <div class="kpi-name">{{ r.kpi_label }}</div>
          <span class="badge good">{{ r.severity }}</span>
        </div>
        <div class="metrics">
          <div class="metric">
            <div class="val">{{ r.current_fmt }}</div>
            <div class="lbl">This Week</div>
          </div>
          <div class="metric">
            <div class="val">{{ r.prior_fmt }}</div>
            <div class="lbl">Prior Week</div>
          </div>
          <div class="metric">
            <div class="val wow-good">
              {{ '+' if r.wow_pct_change > 0 else '' }}{{ "%.1f"|format(r.wow_pct_change * 100) }}%
            </div>
            <div class="lbl">WoW Change</div>
          </div>
        </div>
        {% if r.llm_narrative %}
        <details>
          <summary>🤖 View AI Analysis</summary>
          <div class="narrative">{{ r.llm_narrative }}</div>
        </details>
        {% endif %}
      </div>
      {% endfor %}
    {% else %}
      <div class="empty-state">No standout performers this week</div>
    {% endif %}
  </div>

</div>

<div class="footer">
  KPI Drift Detection System &nbsp;|&nbsp; Python + SQLite + Groq (Llama 3.3 70B)
</div>

</body>
</html>
"""


def _format_value(kpi_name: str, value) -> str:
    meta = KPI_META.get(kpi_name, {})
    fmt  = meta.get("format", "{}")
    try:
        return fmt.format(float(value))
    except Exception:
        return str(value)


def generate_html_report(week_start: str, week_end: str,
                         drift_records: list[dict],
                         conn: sqlite3.Connection) -> str:
    os.makedirs(REPORT_DIR, exist_ok=True)

    # Attach formatted display values
    for r in drift_records:
        r["current_fmt"] = _format_value(r["kpi_name"], r["current_value"])
        r["prior_fmt"]   = _format_value(r["kpi_name"], r.get("prior_week_value") or 0)

    # Split into three buckets
    critical_records = sorted(
        [r for r in drift_records if r["bucket"] == "CRITICAL_BAD"],
        key=lambda x: -abs(x["wow_pct_change"])
    )
    warning_records = sorted(
        [r for r in drift_records if r["bucket"] == "WARNING_BAD"],
        key=lambda x: -abs(x["wow_pct_change"])
    )
    good_records = sorted(
        [r for r in drift_records if r["bucket"] == "WELL_PERFORMING"],
        key=lambda x: -abs(x["wow_pct_change"])
    )

    tmpl = Template(HTML_TEMPLATE)
    html = tmpl.render(
        week_start       = week_start,
        week_end         = week_end,
        generated_on     = date.today().isoformat(),
        total_kpis       = len(KPI_META),
        critical_count   = len(critical_records),
        warning_count    = len(warning_records),
        good_count       = len(good_records),
        critical_records = critical_records,
        warning_records  = warning_records,
        good_records     = good_records,
    )

    filename = f"drift_report_{week_start}.html"
    filepath = os.path.join(REPORT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  ✅ Report saved: {filepath}")
    return filepath
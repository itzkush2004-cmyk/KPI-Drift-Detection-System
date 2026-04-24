# =============================================================
# app.py — Streamlit Dashboard for KPI Drift Detection System
#
# Run: streamlit run app.py
# =============================================================

import sqlite3
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta

from config import DB_PATH
from compute_kpis import KPI_META

# ── Page Config ───────────────────────────────────────────────
st.set_page_config(
    page_title="KPI Drift Detection System",
    page_icon="📊",
    layout="wide"
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
  .metric-card { background: #f8f9ff; border-radius: 8px; padding: 12px 16px; margin: 4px 0; }
  .severity-CRITICAL { border-left: 4px solid #e63946; }
  .severity-WARNING  { border-left: 4px solid #f4a261; }
  .narrative-box { background: #eef2ff; border-radius: 8px; padding: 14px; font-size: 13px; line-height: 1.7; margin-top: 8px; border: 1px solid #c5cae9; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def load_drift_log(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT * FROM drift_log ORDER BY week_start DESC, severity DESC",
        conn
    )


def load_kpi_daily(conn, kpi_name: str) -> pd.DataFrame:
    return pd.read_sql_query(
        f"SELECT kpi_date, {kpi_name} FROM kpi_daily ORDER BY kpi_date",
        conn
    )


def load_kpi_weekly(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT * FROM kpi_weekly ORDER BY week_start DESC",
        conn
    )


def severity_icon(sev: str) -> str:
    return {"CRITICAL": "🔴", "WARNING": "🟡", "STABLE": "🟢"}.get(sev, "⚪")


# ── Main App ──────────────────────────────────────────────────
conn = get_conn()

st.title("📊 KPI Drift Detection System")
st.caption("Retail Analytics | Weekly Cadence | Powered by Gemini 2.0 Flash")

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    drift_df = load_drift_log(conn)
    weekly_df = load_kpi_weekly(conn)

    if drift_df.empty:
        st.warning("No drift data yet. Run daily_job.py --backfill first.")
        st.stop()

    available_weeks = sorted(drift_df["week_start"].unique(), reverse=True)
    selected_week = st.selectbox(
        "Select Week",
        available_weeks,
        format_func=lambda w: f"Week of {w}"
    )

    severity_filter = st.multiselect(
        "Severity",
        ["CRITICAL", "WARNING"],
        default=["CRITICAL", "WARNING"]
    )

    st.divider()
    st.header("Run Pipeline")
    run_date = st.date_input("Simulate Date", value=date(2011, 6, 12))
    if st.button("▶ Run Daily Job", use_container_width=True):
        import subprocess
        result = subprocess.run(
            ["python", "daily_job.py", "--date", run_date.isoformat()],
            capture_output=True, text=True, cwd="."
        )
        st.code(result.stdout or result.stderr)
        st.rerun()


# ── Summary Row ───────────────────────────────────────────────
week_drifts = drift_df[drift_df["week_start"] == selected_week]
week_drifts = week_drifts[week_drifts["severity"].isin(severity_filter)]

all_week = drift_df[drift_df["week_start"] == selected_week]
col1, col2, col3, col4 = st.columns(4)
col1.metric("KPIs Monitored",  len(KPI_META))
col2.metric("🔴 Critical",      (all_week["severity"] == "CRITICAL").sum())
col3.metric("🟡 Warnings",      (all_week["severity"] == "WARNING").sum())
col4.metric("Weeks of History", len(available_weeks))

st.divider()

# ── Drift Cards ───────────────────────────────────────────────
st.subheader(f"Drift Alerts — Week of {selected_week}")

if week_drifts.empty:
    st.success("✅ No drift detected for this week with the selected filters.")
else:
    for _, row in week_drifts.iterrows():
        icon  = severity_icon(row["severity"])
        label = KPI_META.get(row["kpi_name"], {}).get("label", row["kpi_name"])
        wow   = row["wow_pct_change"] * 100
        direction = "↑" if wow > 0 else "↓"

        with st.expander(f"{icon} {label} — {direction}{abs(wow):.1f}% WoW ({row['severity']})", expanded=(row["severity"] == "CRITICAL")):
            c1, c2, c3, c4 = st.columns(4)
            fmt = KPI_META.get(row["kpi_name"], {}).get("format", "{:.2f}")
            try:
                cur_fmt  = fmt.format(float(row["current_value"]))
                prior_fmt = fmt.format(float(row["prior_week_value"])) if row["prior_week_value"] else "N/A"
            except Exception:
                cur_fmt = str(row["current_value"])
                prior_fmt = str(row["prior_week_value"])

            c1.metric("This Week",  cur_fmt)
            c2.metric("Prior Week", prior_fmt)
            c3.metric("WoW %",      f"{'+' if wow > 0 else ''}{wow:.1f}%")
            c4.metric("Z-Score",    f"{row['zscore']:+.2f}σ")

            if row.get("consecutive_weeks", 1) > 1:
                st.warning(f"⚠️ Persistent trend: {int(row['consecutive_weeks'])} consecutive weeks drifting")

            if row.get("llm_narrative"):
                st.markdown("**🤖 AI Analysis**")
                st.markdown(
                    f'<div class="narrative-box">{row["llm_narrative"]}</div>',
                    unsafe_allow_html=True
                )

st.divider()

# ── KPI Trend Chart ───────────────────────────────────────────
st.subheader("KPI Trend Explorer")

kpi_options = {v["label"]: k for k, v in KPI_META.items()}
selected_label = st.selectbox("Select KPI", list(kpi_options.keys()))
selected_kpi   = kpi_options[selected_label]

daily_df = load_kpi_daily(conn, selected_kpi)

if not daily_df.empty:
    daily_df["kpi_date"] = pd.to_datetime(daily_df["kpi_date"])
    daily_df = daily_df.sort_values("kpi_date")

    # Rolling average
    daily_df["rolling_4w"] = daily_df[selected_kpi].rolling(window=28).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_df["kpi_date"], y=daily_df[selected_kpi],
        name="Daily", line=dict(color="#4361ee", width=1.5), opacity=0.7
    ))
    fig.add_trace(go.Scatter(
        x=daily_df["kpi_date"], y=daily_df["rolling_4w"],
        name="4-Week Rolling Avg", line=dict(color="#f72585", width=2, dash="dash")
    ))

    # Overlay drift markers for this KPI
    kpi_drifts = drift_df[drift_df["kpi_name"] == selected_kpi].copy()
    if not kpi_drifts.empty:
        kpi_drifts["week_start"] = pd.to_datetime(kpi_drifts["week_start"])
        crit = kpi_drifts[kpi_drifts["severity"] == "CRITICAL"]
        warn = kpi_drifts[kpi_drifts["severity"] == "WARNING"]

        if not crit.empty:
            vals = daily_df[daily_df["kpi_date"].isin(crit["week_start"])][selected_kpi]
            fig.add_trace(go.Scatter(
                x=crit["week_start"], y=vals,
                mode="markers", name="Critical Drift",
                marker=dict(color="#e63946", size=10, symbol="x")
            ))
        if not warn.empty:
            vals = daily_df[daily_df["kpi_date"].isin(warn["week_start"])][selected_kpi]
            fig.add_trace(go.Scatter(
                x=warn["week_start"], y=vals,
                mode="markers", name="Warning",
                marker=dict(color="#f4a261", size=8, symbol="triangle-up")
            ))

    fig.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(gridcolor="#f0f0f0"),
        yaxis=dict(gridcolor="#f0f0f0"),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Drift Heatmap ─────────────────────────────────────────────
st.subheader("Drift History Heatmap")

if not drift_df.empty:
    pivot = drift_df.pivot_table(
        index="kpi_name", columns="week_start",
        values="wow_pct_change", aggfunc="mean"
    )
    pivot.index = [KPI_META.get(k, {}).get("label", k) for k in pivot.index]

    fig2 = px.imshow(
        pivot * 100,
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        aspect="auto",
        labels=dict(color="WoW %"),
    )
    fig2.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Raw Drift Log ─────────────────────────────────────────────
with st.expander("📋 Full Drift Log"):
    display_cols = ["week_start", "kpi_name", "severity", "wow_pct_change", "zscore", "consecutive_weeks"]
    st.dataframe(
        drift_df[display_cols].head(100),
        use_container_width=True,
        hide_index=True
    )

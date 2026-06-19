"""
Customer Health Command Center
Streamlit Databricks App
Workspace: https://dbc-69445b27-9472.cloud.databricks.com

Queries Unity Catalog gold tables via a Databricks SQL warehouse.
Auth is handled automatically by Databricks Apps (injected token).
The app service principal must have SELECT on gold tables and
CAN USE on the SQL warehouse — see sql/00_setup_catalog_and_permissions.sql.
"""

import os
import streamlit as st
import pandas as pd
import altair as alt
from databricks import sql as dbsql

# ---------------------------------------------------------------------------
# Configuration — pulled from environment variables injected by Databricks Apps
# ---------------------------------------------------------------------------

DATABRICKS_HOST      = os.environ.get("DATABRICKS_HOST", "https://dbc-69445b27-9472.cloud.databricks.com")
DATABRICKS_TOKEN     = os.environ.get("DATABRICKS_TOKEN", "")
CATALOG              = os.environ.get("DATABRICKS_CATALOG", "dn_saas_demo")
GOLD                 = f"{CATALOG}.gold"

# HTTP path can be set via env var or overridden in the sidebar
DEFAULT_HTTP_PATH    = os.environ.get("DATABRICKS_WAREHOUSE_HTTP_PATH", "")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Customer Health Command Center",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def get_connection(http_path: str):
    """
    Opens a Databricks SQL connection using the app service principal token.
    Raises a clear error if required env vars are missing.
    """
    if not DATABRICKS_TOKEN:
        st.error(
            "DATABRICKS_TOKEN is not set. "
            "This app must run inside Databricks Apps or with a personal access token."
        )
        st.stop()
    if not http_path:
        st.error(
            "SQL Warehouse HTTP path is not configured. "
            "Set DATABRICKS_WAREHOUSE_HTTP_PATH or enter it in the sidebar."
        )
        st.stop()

    return dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=http_path,
        access_token=DATABRICKS_TOKEN,
    )


# ---------------------------------------------------------------------------
# Data loading — cached per HTTP path so filters don't re-query
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner="Loading account health data…")
def load_account_health(http_path: str) -> pd.DataFrame:
    """
    Loads the full gold_account_health table. Filtering happens in-memory
    so the sidebar filters are instant without additional warehouse queries.
    """
    query = f"""
        SELECT
            account_id,
            account_name,
            segment,
            region,
            industry,
            arr,
            plan_tier,
            csm_name,
            days_to_renewal,
            active_users_30d,
            login_recency_days,
            adopted_features_30d,
            total_sessions_30d,
            avg_session_minutes_30d,
            ticket_count_30d,
            avg_resolution_hours_30d,
            avg_csat_30d,
            contract_status,
            payment_status,
            health_score,
            churn_risk_band,
            risk_reason,
            next_best_action
        FROM {GOLD}.gold_account_health
        ORDER BY health_score ASC
    """
    with get_connection(http_path) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()


@st.cache_data(ttl=300, show_spinner=False)
def load_exec_kpis(http_path: str) -> dict:
    """Single-row executive KPI table."""
    query = f"SELECT * FROM {GOLD}.gold_exec_kpis LIMIT 1"
    with get_connection(http_path) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row)) if row else {}


@st.cache_data(ttl=300, show_spinner=False)
def load_risk_segments(http_path: str) -> pd.DataFrame:
    """Segment-level risk aggregates."""
    query = f"""
        SELECT segment, plan_tier, region,
               account_count, total_arr, avg_health_score,
               high_risk_count, medium_risk_count, low_risk_count,
               arr_at_risk
        FROM {GOLD}.gold_risk_segments
    """
    with get_connection(http_path) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()


# ---------------------------------------------------------------------------
# Helper: apply sidebar filters to the account dataframe
# ---------------------------------------------------------------------------

def apply_filters(df: pd.DataFrame, segments, plans, regions, risk_bands) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if segments:
        mask &= df["segment"].isin(segments)
    if plans:
        mask &= df["plan_tier"].isin(plans)
    if regions:
        mask &= df["region"].isin(regions)
    if risk_bands:
        mask &= df["churn_risk_band"].isin(risk_bands)
    return df[mask].copy()


# ---------------------------------------------------------------------------
# Helper: colour a risk band string
# ---------------------------------------------------------------------------

RISK_COLORS = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}

def risk_badge(band: str) -> str:
    return f"{RISK_COLORS.get(band, '⚪')} {band}"


# ---------------------------------------------------------------------------
# Helper: plain-language executive summary
# ---------------------------------------------------------------------------

def build_exec_summary(df: pd.DataFrame) -> str:
    if df.empty:
        return "No accounts match the current filters."

    total         = len(df)
    arr_total     = df["arr"].sum()
    high_risk     = (df["churn_risk_band"] == "High").sum()
    arr_at_risk   = df[df["churn_risk_band"].isin(["High","Medium"])]["arr"].sum()
    avg_score     = df["health_score"].mean()
    no_activity   = (df["active_users_30d"] == 0).sum()
    payment_issue = df["payment_status"].isin(["Overdue","Failed"]).sum()

    lines = [
        f"**{total} accounts** with **${arr_total:,.0f}** in ARR are in scope.",
        f"Average health score is **{avg_score:.1f} / 100**.",
    ]

    if high_risk:
        lines.append(
            f"**{high_risk} accounts** ({high_risk/total:.0%}) are High Risk, "
            f"representing **${arr_at_risk:,.0f}** in ARR exposure."
        )
    if no_activity:
        lines.append(
            f"**{no_activity} account{'s' if no_activity > 1 else ''}** "
            f"had zero product activity in the last 30 days."
        )
    if payment_issue:
        lines.append(
            f"**{payment_issue} account{'s' if payment_issue > 1 else ''}** "
            f"{'have' if payment_issue > 1 else 'has'} an overdue or failed payment."
        )
    if high_risk == 0:
        lines.append("No accounts are currently flagged as High Risk in this view.")

    return "  \n".join(lines)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("⚙️ Filters")

    http_path = st.text_input(
        "SQL Warehouse HTTP Path",
        value=DEFAULT_HTTP_PATH,
        placeholder="/sql/1.0/warehouses/…",
        help="Set DATABRICKS_WAREHOUSE_HTTP_PATH env var to pre-populate this.",
    )

    st.divider()

    # Load data first to populate filter options
    if not http_path:
        st.warning("Enter a SQL Warehouse HTTP path to continue.")
        st.stop()

    try:
        df_all = load_account_health(http_path)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.stop()

    segment_opts  = sorted(df_all["segment"].unique())
    plan_opts     = sorted(df_all["plan_tier"].unique())
    region_opts   = sorted(df_all["region"].unique())
    risk_opts     = ["High", "Medium", "Low"]

    f_segments  = st.multiselect("Segment",        segment_opts, default=segment_opts)
    f_plans     = st.multiselect("Plan Tier",       plan_opts,    default=plan_opts)
    f_regions   = st.multiselect("Region",          region_opts,  default=region_opts)
    f_risk      = st.multiselect("Churn Risk Band", risk_opts,    default=risk_opts)

    st.divider()
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.caption("Data: dn_saas_demo.gold.gold_account_health")


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

df = apply_filters(df_all, f_segments, f_plans, f_regions, f_risk)

# ── Header ──────────────────────────────────────────────────────────────────
st.title("📊 Customer Health Command Center")
st.caption(
    "A unified view of account health across usage, support, and subscription signals. "
    "Powered by Databricks Unity Catalog · dn_saas_demo.gold"
)
st.divider()

# ── KPI row ─────────────────────────────────────────────────────────────────
try:
    kpis = load_exec_kpis(http_path)
except Exception:
    kpis = {}

# Compute filtered KPIs from the dataframe (reflects sidebar filters)
filtered_arr          = df["arr"].sum()
filtered_avg_score    = df["health_score"].mean() if not df.empty else 0
filtered_high_risk_pct = (
    100.0 * (df["churn_risk_band"] == "High").sum() / len(df) if not df.empty else 0
)
filtered_arr_at_risk  = df[df["churn_risk_band"].isin(["High","Medium"])]["arr"].sum()

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric(
        label="Total ARR (filtered)",
        value=f"${filtered_arr:,.0f}",
        help="Sum of ARR for accounts matching current filters.",
    )
with c2:
    st.metric(
        label="Avg Health Score",
        value=f"{filtered_avg_score:.1f}",
        help="Mean health score across filtered accounts (0–100).",
    )
with c3:
    delta_color = "inverse" if filtered_high_risk_pct > 15 else "normal"
    st.metric(
        label="% High Risk",
        value=f"{filtered_high_risk_pct:.1f}%",
        help="Percentage of filtered accounts with High churn risk band.",
    )
with c4:
    st.metric(
        label="ARR at Risk",
        value=f"${filtered_arr_at_risk:,.0f}",
        help="ARR from High + Medium risk accounts in the current filter.",
    )

st.divider()

# ── Executive summary ────────────────────────────────────────────────────────
with st.expander("📝 Executive Summary", expanded=True):
    st.markdown(build_exec_summary(df))

st.divider()

# ── Main account table ───────────────────────────────────────────────────────
st.subheader("Account Health Overview")

if df.empty:
    st.info("No accounts match the current filter selection.")
else:
    display_cols = [
        "account_name", "segment", "plan_tier", "region",
        "arr", "health_score", "churn_risk_band",
        "active_users_30d", "days_to_renewal",
        "risk_reason",
    ]

    display_df = df[display_cols].copy()
    display_df["arr"]           = display_df["arr"].apply(lambda x: f"${x:,.0f}")
    display_df["health_score"]  = display_df["health_score"].apply(lambda x: f"{x:.1f}")
    display_df["churn_risk_band"] = display_df["churn_risk_band"].apply(risk_badge)
    display_df.columns = [
        "Account", "Segment", "Plan", "Region",
        "ARR", "Health Score", "Churn Risk",
        "Active Users (30d)", "Days to Renewal",
        "Primary Risk Reason",
    ]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=400,
    )

st.divider()

# ── Account detail panel ─────────────────────────────────────────────────────
st.subheader("Account Detail")

if not df.empty:
    account_names = df.sort_values("health_score")["account_name"].tolist()
    selected_name = st.selectbox(
        "Select an account to inspect:",
        options=account_names,
        index=0,
        help="Sorted by health score ascending — worst accounts first.",
    )

    row = df[df["account_name"] == selected_name].iloc[0]

    d1, d2, d3 = st.columns(3)

    with d1:
        st.markdown(f"**Account:** {row['account_name']}")
        st.markdown(f"**Segment:** {row['segment']}  |  **Plan:** {row['plan_tier']}")
        st.markdown(f"**Region:** {row['region']}  |  **Industry:** {row['industry']}")
        st.markdown(f"**ARR:** ${row['arr']:,.0f}")
        st.markdown(f"**CSM:** {row['csm_name']}")
        st.markdown(f"**Renewal in:** {int(row['days_to_renewal'])} days")
        st.markdown(f"**Contract:** {row['contract_status']}  |  **Payment:** {row['payment_status']}")

    with d2:
        st.markdown("**Usage Signals (30d)**")
        st.metric("Active Users",        int(row["active_users_30d"]))
        st.metric("Login Recency",       f"{int(row['login_recency_days'])} days")
        st.metric("Features Adopted",    int(row["adopted_features_30d"]))
        st.metric("Total Sessions",      int(row["total_sessions_30d"]))
        st.metric("Avg Session Minutes", f"{row['avg_session_minutes_30d']:.1f}")

    with d3:
        st.markdown("**Support Signals (30d)**")
        st.metric("Tickets",            int(row["ticket_count_30d"]))
        st.metric("Avg Resolution",     f"{row['avg_resolution_hours_30d']:.0f} hrs")
        csat = row["avg_csat_30d"]
        st.metric("Avg CSAT",           f"{csat:.2f} / 5.0")
        st.divider()
        band = row["churn_risk_band"]
        st.markdown(f"**Health Score:** {row['health_score']:.1f}")
        st.markdown(f"**Churn Risk:** {risk_badge(band)}")
        st.markdown(f"**Risk Reason:** {row['risk_reason']}")
        st.info(f"💡 **Next Best Action:** {row['next_best_action']}")

st.divider()

# ── Charts ───────────────────────────────────────────────────────────────────
st.subheader("Risk & Revenue Analysis")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("**Churn Risk Distribution by Segment**")
    if not df.empty:
        risk_by_seg = (
            df.groupby(["segment", "churn_risk_band"])
              .size()
              .reset_index(name="count")
        )
        chart = (
            alt.Chart(risk_by_seg)
            .mark_bar()
            .encode(
                x=alt.X("segment:N", title="Segment", sort="-y"),
                y=alt.Y("count:Q", title="Account Count"),
                color=alt.Color(
                    "churn_risk_band:N",
                    scale=alt.Scale(
                        domain=["High", "Medium", "Low"],
                        range=["#e74c3c", "#f39c12", "#2ecc71"],
                    ),
                    legend=alt.Legend(title="Risk Band"),
                ),
                tooltip=["segment", "churn_risk_band", "count"],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)

with chart_col2:
    st.markdown("**ARR at Risk by Plan Tier**")
    if not df.empty:
        arr_by_plan = (
            df[df["churn_risk_band"].isin(["High", "Medium"])]
            .groupby(["plan_tier", "churn_risk_band"])["arr"]
            .sum()
            .reset_index()
        )
        chart2 = (
            alt.Chart(arr_by_plan)
            .mark_bar()
            .encode(
                x=alt.X("plan_tier:N", title="Plan Tier",
                         sort=["Starter", "Growth", "Professional", "Enterprise"]),
                y=alt.Y("arr:Q", title="ARR at Risk ($)"),
                color=alt.Color(
                    "churn_risk_band:N",
                    scale=alt.Scale(
                        domain=["High", "Medium"],
                        range=["#e74c3c", "#f39c12"],
                    ),
                    legend=alt.Legend(title="Risk Band"),
                ),
                tooltip=["plan_tier", "churn_risk_band",
                         alt.Tooltip("arr:Q", format="$,.0f")],
            )
            .properties(height=300)
        )
        st.altair_chart(chart2, use_container_width=True)

# Health score vs ARR scatter
st.markdown("**Health Score vs ARR**")
if not df.empty:
    scatter_df = df[["account_name", "health_score", "arr",
                      "churn_risk_band", "segment"]].copy()
    scatter = (
        alt.Chart(scatter_df)
        .mark_circle(size=60, opacity=0.75)
        .encode(
            x=alt.X("health_score:Q", title="Health Score (0–100)", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("arr:Q", title="ARR ($)"),
            color=alt.Color(
                "churn_risk_band:N",
                scale=alt.Scale(
                    domain=["High", "Medium", "Low"],
                    range=["#e74c3c", "#f39c12", "#2ecc71"],
                ),
                legend=alt.Legend(title="Risk Band"),
            ),
            shape=alt.Shape("segment:N", legend=alt.Legend(title="Segment")),
            tooltip=[
                "account_name", "segment",
                alt.Tooltip("health_score:Q", format=".1f"),
                alt.Tooltip("arr:Q", format="$,.0f"),
                "churn_risk_band",
            ],
        )
        .properties(height=350)
        .interactive()
    )
    st.altair_chart(scatter, use_container_width=True)

st.divider()
st.caption(
    "Customer Health Command Center · "
    "Powered by Databricks Unity Catalog · "
    "Data refreshes every 5 minutes (cache TTL=300s) · "
    "Contact: archanainapudi@gmail.com"
)

# =============================================================================
# DEMO WALKTHROUGH SCRIPT (for presenter use)
# =============================================================================
#
# 1. OPEN THE APP
#    "This is the Customer Health Command Center — a Streamlit app deployed
#     as a Databricks App. It queries our Unity Catalog gold layer in real
#     time through a Serverless SQL warehouse."
#
# 2. KPI ROW
#    "At the top we have four executive KPIs: total ARR in scope, average
#     health score, percentage of accounts flagged High Risk, and total
#     ARR at risk. These update when I change the filters."
#
# 3. EXECUTIVE SUMMARY
#    "The executive summary auto-generates a plain-English narrative from
#     the filtered data — no manual slide building."
#
# 4. SIDEBAR FILTERS
#    "I can slice by segment, plan tier, region, or risk band.
#     Watch the KPIs and table update instantly — no re-query because
#     we cached the full result set at load time."
#    → Filter to High Risk only. Show how the table and KPIs change.
#
# 5. ACCOUNT TABLE
#    "The table shows every account sorted worst-first by health score.
#     I can see risk reason in plain English — no decoding of model scores."
#
# 6. ACCOUNT DETAIL PANEL
#    "I'll select the lowest-scoring account."
#    → Select the first account in the dropdown.
#    "Usage: zero active users in 30 days. 3 P1 tickets. CSAT of 2.1.
#     Renewal in 18 days. Next best action: schedule re-engagement call."
#    "This is what a CSM sees every morning."
#
# 7. CHARTS
#    "Risk by segment shows Mid-Market has the most High Risk accounts.
#     ARR at risk by plan shows Starter tier is highest — smaller accounts,
#     faster to churn. The scatter shows a clear cluster of low-score,
#     high-ARR accounts — those are the priority for the week."
#
# 8. CLOSE
#    "The entire stack — data generation, medallion pipeline, scoring model,
#     and app — runs in a single Databricks workspace. No external services,
#     no additional infrastructure."
# =============================================================================

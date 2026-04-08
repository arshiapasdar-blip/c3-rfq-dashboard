import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

logger = logging.getLogger(__name__)

import tools.sales_data as sales_data
import tools.sourcing_data as sourcing_data
import tools.report_data as report_data
import tools.deepdive_data as deepdive_data
import tools.chat_agent as chat_agent
from tools.report_data import METRIC_DIMENSIONS, COMPUTED_METRICS, run_any_report
from tools.db import run_query, get_credential_source
from tools.auth import get_current_user_email, resolve_user_display, get_hub_names, build_hub_scope

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="C3 Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Page background gradient ── */
    .stApp {
        background: linear-gradient(135deg, #dde8f5 0%, #ede8f7 60%, #e4eef8 100%);
        background-attachment: fixed;
    }

    /* ── Glassmorphism metric cards ── */
    div[data-testid="metric-container"] {
        background: rgba(255, 255, 255, 0.60);
        backdrop-filter: blur(14px);
        -webkit-backdrop-filter: blur(14px);
        border: 1px solid rgba(255, 255, 255, 0.75);
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(99, 102, 241, 0.08);
        padding: 18px 20px !important;
    }
    div[data-testid="metric-container"] label {
        font-size: 11px !important;
        font-weight: 700 !important;
        letter-spacing: 0.6px !important;
        text-transform: uppercase !important;
        color: #64748b !important;
        font-family: "Inter", sans-serif !important;
    }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 30px !important;
        font-weight: 700 !important;
        color: #1e293b !important;
        font-family: "Inter", sans-serif !important;
    }

    /* ── Sidebar glass ── */
    section[data-testid="stSidebar"] {
        background: rgba(255, 255, 255, 0.55) !important;
        backdrop-filter: blur(20px) !important;
        -webkit-backdrop-filter: blur(20px) !important;
        border-right: 1px solid rgba(255, 255, 255, 0.6) !important;
    }

    /* ── Pill-shaped tabs ── */
    .stTabs [data-baseweb="tab-list"] { gap: 8px !important; }
    .stTabs [data-baseweb="tab-border"] { display: none !important; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 22px !important;
        padding: 8px 22px !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        background: rgba(255, 255, 255, 0.45) !important;
        border: 1px solid rgba(255, 255, 255, 0.65) !important;
        backdrop-filter: blur(8px) !important;
        -webkit-backdrop-filter: blur(8px) !important;
        font-family: "Inter", sans-serif !important;
        color: #475569 !important;
    }
    .stTabs [aria-selected="true"][data-baseweb="tab"] {
        background: rgba(255, 255, 255, 0.88) !important;
        border-color: rgba(99, 102, 241, 0.35) !important;
        color: #6366f1 !important;
    }

    /* ── General typography ── */
    h2 { margin-top: 0.5rem !important; font-family: "Inter", sans-serif; }
    .section-header {
        font-size: 15px;
        font-weight: 600;
        color: #555;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
        font-family: "Inter", sans-serif;
    }

    /* ── Report Builder GA-style ── */
    .rb-config-panel {
        background: rgba(255, 255, 255, 0.65);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.75);
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(99, 102, 241, 0.06);
        padding: 20px 22px 16px 22px;
        margin-bottom: 16px;
    }
    .rb-config-title {
        font-size: 11px;
        font-weight: 600;
        color: #80868b;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 12px;
        font-family: "Inter", sans-serif;
    }
    .rb-pill {
        display: inline-block;
        background: rgba(232, 240, 254, 0.9);
        color: #1a73e8;
        border-radius: 12px;
        padding: 3px 12px;
        font-size: 13px;
        font-weight: 500;
        margin-right: 6px;
        margin-bottom: 4px;
    }
    .rb-pill-gray {
        display: inline-block;
        background: rgba(241, 243, 244, 0.9);
        color: #5f6368;
        border-radius: 12px;
        padding: 3px 12px;
        font-size: 13px;
        font-weight: 500;
        margin-right: 6px;
    }
    .rb-result-title {
        font-size: 20px;
        font-weight: 500;
        color: #202124;
        margin: 0 0 4px 0;
        line-height: 1.3;
        font-family: "Inter", sans-serif;
    }
    .rb-result-meta {
        font-size: 12px;
        color: #80868b;
        margin-bottom: 18px;
    }
    .rb-divider {
        border: none;
        border-top: 1px solid #e8eaed;
        margin: 16px 0;
    }
    .rb-empty-state {
        text-align: center;
        padding: 60px 20px;
        color: #80868b;
    }
    .rb-empty-state .icon { font-size: 48px; margin-bottom: 12px; }
    .rb-empty-state .title { font-size: 18px; font-weight: 500; color: #3c4043; margin-bottom: 6px; }
    .rb-empty-state .sub { font-size: 14px; }

    /* ── White input fields (text inputs, selects, text areas, number inputs) ── */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div > div[data-baseweb="select"] > div,
    .stMultiSelect > div > div > div[data-baseweb="select"] > div,
    .stDateInput > div > div > input,
    div[data-baseweb="input"] > input,
    div[data-baseweb="textarea"] > textarea,
    div[data-baseweb="base-input"] > input {
        background-color: #ffffff !important;
        color: #1e293b !important;
    }
    /* select dropdown container */
    div[data-baseweb="select"] > div:first-child {
        background-color: #ffffff !important;
    }
    /* ── Ask AI – Futuristic Chat ───────────────────────────────────────────── */

    /* Page header */
    .ai-page-header {
        display: flex;
        align-items: center;
        gap: 16px;
        padding: 8px 4px 24px 4px;
        border-bottom: 1px solid rgba(99, 102, 241, 0.13);
        margin-bottom: 24px;
    }
    .ai-header-icon {
        width: 50px; height: 50px;
        background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
        border-radius: 16px;
        display: flex; align-items: center; justify-content: center;
        font-size: 22px; color: white;
        box-shadow: 0 6px 22px rgba(99, 102, 241, 0.42);
        flex-shrink: 0;
    }
    .ai-header-title {
        font-size: 22px; font-weight: 700;
        color: #1e293b; font-family: "Inter", sans-serif;
        line-height: 1.1;
    }
    .ai-header-sub {
        font-size: 13px; color: #64748b;
        margin-top: 4px; font-family: "Inter", sans-serif;
    }
    .ai-header-badge {
        margin-left: auto;
        background: rgba(34, 197, 94, 0.10);
        border: 1px solid rgba(34, 197, 94, 0.28);
        color: #16a34a;
        font-size: 11px; font-weight: 600; letter-spacing: 0.5px;
        padding: 5px 13px; border-radius: 20px; white-space: nowrap;
    }

    /* Empty state */
    .ai-empty-state {
        text-align: center;
        padding: 52px 20px 36px;
    }
    .ai-empty-icon { font-size: 54px; display: block; margin-bottom: 16px; }
    .ai-empty-title {
        font-size: 20px; font-weight: 700; color: #1e293b;
        margin-bottom: 8px; font-family: "Inter", sans-serif;
    }
    .ai-empty-sub {
        font-size: 14px; color: #64748b;
        max-width: 440px; margin: 0 auto 26px;
        line-height: 1.75; font-family: "Inter", sans-serif;
    }
    .ai-prompt-chips { display: flex; gap: 8px; flex-wrap: wrap; justify-content: center; }
    .ai-chip {
        background: rgba(255, 255, 255, 0.78);
        border: 1px solid rgba(99, 102, 241, 0.22);
        color: #4f46e5; font-size: 13px; font-weight: 500;
        padding: 7px 16px; border-radius: 22px;
        backdrop-filter: blur(8px); font-family: "Inter", sans-serif;
    }

    /* Message containers */
    [data-testid="stChatMessage"] {
        background: transparent !important;
        border: none !important;
        padding: 4px 0 !important;
        gap: 10px !important;
    }
    /* User bubble */
    [data-message-author-role="user"] [data-testid="stChatMessageContent"] {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%) !important;
        border: none !important;
        border-radius: 20px 4px 20px 20px !important;
        box-shadow: 0 4px 20px rgba(99, 102, 241, 0.30) !important;
        color: #ffffff !important;
    }
    [data-message-author-role="user"] [data-testid="stChatMessageContent"] p,
    [data-message-author-role="user"] [data-testid="stChatMessageContent"] li,
    [data-message-author-role="user"] [data-testid="stChatMessageContent"] span,
    [data-message-author-role="user"] [data-testid="stChatMessageContent"] strong,
    [data-message-author-role="user"] [data-testid="stChatMessageContent"] em {
        color: #ffffff !important;
    }
    /* Assistant bubble */
    [data-message-author-role="assistant"] [data-testid="stChatMessageContent"] {
        background: rgba(255, 255, 255, 0.84) !important;
        border: 1px solid rgba(99, 102, 241, 0.15) !important;
        border-radius: 4px 20px 20px 20px !important;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.06) !important;
        backdrop-filter: blur(16px) !important;
        -webkit-backdrop-filter: blur(16px) !important;
        color: #1e293b !important;
    }
    /* Avatars → rounded squares */
    [data-testid="stChatMessageAvatarUser"] {
        background: linear-gradient(135deg, #6366f1, #818cf8) !important;
        border-radius: 12px !important;
        box-shadow: 0 2px 10px rgba(99, 102, 241, 0.38) !important;
        overflow: hidden !important;
    }
    [data-testid="stChatMessageAvatarAssistant"] {
        background: linear-gradient(135deg, #1e293b, #334155) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(99, 102, 241, 0.28) !important;
        overflow: hidden !important;
    }
    /* Chat input – pill shaped, centered, narrower */
    [data-testid="stChatInput"] > div {
        border-radius: 28px !important;
        border: 1.5px solid rgba(99, 102, 241, 0.28) !important;
        background: rgba(255, 255, 255, 0.93) !important;
        backdrop-filter: blur(16px) !important;
        -webkit-backdrop-filter: blur(16px) !important;
        box-shadow: 0 8px 32px rgba(99, 102, 241, 0.12), 0 2px 8px rgba(0,0,0,0.04) !important;
        overflow: hidden !important;
        max-width: 760px !important;
        margin: 0 auto !important;
    }
    [data-testid="stChatInput"] textarea {
        font-size: 14.5px !important;
        font-family: "Inter", sans-serif !important;
        color: #1e293b !important;
        padding: 14px 20px !important;
        background-color: #ffffff !important;
    }
    [data-testid="stChatInput"] textarea::placeholder { color: #94a3b8 !important; }
    .stChatInput > div { background-color: #ffffff !important; }
    .stChatInput textarea { background-color: #ffffff !important; }
</style>
""", unsafe_allow_html=True)

# ─── Colour palette ───────────────────────────────────────────────────────────
BLUE = "#4a6cf7"
GREEN = "#22c55e"
ORANGE = "#f97316"
RED = "#ef4444"
PURPLE = "#a855f7"
COLORS = [BLUE, GREEN, ORANGE, RED, PURPLE, "#06b6d4", "#eab308", "#ec4899"]

RESULT_COLORS = {
    "Won": GREEN,
    "Pending": BLUE,
    "Lost / No Quote": RED,
    "Declined": ORANGE,
    "No Response": "#94a3b8",
    "Cancelled": "#64748b",
    "Other": "#cbd5e1",
}

STATUS_COLORS = {
    "Not Started": "#94a3b8",
    "Initiated": "#cbd5e1",
    "Sent to Sourcing": BLUE,
    "In Progress": ORANGE,
    "Sourced": "#06b6d4",
    "Quoted": PURPLE,
    "Won": GREEN,
    "Closed": "#64748b",
}

def _chart(fig, height: int = None) -> go.Figure:
    """Apply consistent glassmorphism-friendly styling to any Plotly figure."""
    upd = dict(
        font=dict(family="Inter, sans-serif", size=12, color="#334155"),
        paper_bgcolor="rgba(255,255,255,0.55)",
        plot_bgcolor="rgba(255,255,255,0.0)",
        margin=dict(l=4, r=4, t=28, b=4),
        xaxis=dict(showgrid=False, tickfont=dict(size=11), zeroline=False),
        yaxis=dict(gridcolor="#e2e8f0", zeroline=False, tickfont=dict(size=11)),
        hoverlabel=dict(
            bgcolor="white", bordercolor="#e2e8f0",
            font=dict(family="Inter, sans-serif", size=12),
        ),
    )
    if height:
        upd["height"] = height
    fig.update_layout(**upd)
    try:
        fig.update_traces(marker_cornerradius=5, selector=dict(type="bar"))
    except Exception:
        pass
    return fig


@st.cache_data(ttl=3600)
def get_earliest_date() -> date:
    """Return the earliest CreatedDate found across CustomerRfqs and SupplierRfqs."""
    sql = """
        SELECT MIN(CreatedDate) AS earliest FROM (
            SELECT MIN(CreatedDate) AS CreatedDate FROM CustomerRfqs WHERE Deleted = 0
            UNION ALL
            SELECT MIN(CreatedDate) FROM SupplierRfqs
        ) sub
    """
    df = run_query(sql)
    if df.empty or df.iloc[0]["earliest"] is None:
        return date(2020, 1, 1)
    val = df.iloc[0]["earliest"]
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()


# ─── Parallel data loader ─────────────────────────────────────────────────────
# All 18 dashboard queries run concurrently inside a single cached function.
# TTL raised to 15 min — reduces cold-load frequency without losing freshness.
@st.cache_data(ttl=900, show_spinner=False)
def load_dashboard_data(start_str: str, end_str: str, hub_scope_key: str = "all") -> dict:
    # hub_scope_key is used for cache partitioning — rebuild the SQL from it
    _hub_sql = build_hub_scope(hub_scope_key) if hub_scope_key != "all" else ""
    tasks = {
        "crfq_kpis":           (sales_data.get_crfq_kpis,                      [start_str, end_str, _hub_sql]),
        "df_customers":        (sales_data.get_top_customers,                   [start_str, end_str, 10, _hub_sql]),
        "df_monthly_crfq":     (sales_data.get_monthly_crfq_trend_range,        [start_str, end_str, _hub_sql]),
        "df_rfq_results":      (sales_data.get_rfq_result_breakdown,            [start_str, end_str, _hub_sql]),
        "df_mpns":             (sales_data.get_top_mpns,                        [start_str, end_str, 20, _hub_sql]),
        "df_sales_reps":       (sales_data.get_sales_rep_leaderboard,           [start_str, end_str, _hub_sql]),
        "df_quote_value":      (sales_data.get_quote_value_by_customer,         [start_str, end_str, 10, _hub_sql]),
        "df_countries":        (sales_data.get_customer_country_distribution,   [start_str, end_str, _hub_sql]),
        "srfq_kpis":           (sourcing_data.get_srfq_kpis,                    [start_str, end_str]),
        "top_supplier_value":  (sourcing_data.get_top_supplier_value,           [start_str, end_str]),
        "df_suppliers":        (sourcing_data.get_top_suppliers,                [start_str, end_str]),
        "df_response_rates":   (sourcing_data.get_supplier_response_rates,      [start_str, end_str]),
        "df_monthly_srfq":     (sourcing_data.get_monthly_srfq_trend_range,     [start_str, end_str]),
        "df_sourcing_status":  (sourcing_data.get_sourcing_status_breakdown,    [start_str, end_str]),
        "df_sourced_mpns":     (sourcing_data.get_top_sourced_mpns,             [start_str, end_str]),
        "df_supplier_types":   (sourcing_data.get_supplier_type_distribution,   [start_str, end_str]),
        "df_margin":           (sourcing_data.get_margin_analysis,              [start_str, end_str]),
        "df_top_manufacturers":(sourcing_data.get_top_manufacturers,            [start_str, end_str]),
    }
    results: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fn, *args): key for key, (fn, args) in tasks.items()}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                results[key] = fut.result()
            except Exception as exc:
                logger.error("Data load failed for %s: %s", key, exc)
                results[key] = None
    return results


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("C3 Analytics")
    st.caption(f"DB: {get_credential_source()}")
    st.markdown("---")

    st.subheader("Filters")

    from datetime import timedelta
    _today = date.today()

    _preset = st.selectbox("Date range", [
        "All time",
        "Last 7 days", "Last 28 days", "Last 30 days",
        "This month", "Last month", "Last 90 days",
        "Quarter to date", "This year (Jan–Today)",
        "Last calendar year", "Custom",
    ], index=2)

    if _preset == "All time":
        start_date, end_date = get_earliest_date(), _today
    elif _preset == "Last 7 days":
        start_date, end_date = _today - timedelta(days=6), _today
    elif _preset == "Last 28 days":
        start_date, end_date = _today - timedelta(days=27), _today
    elif _preset == "Last 30 days":
        start_date, end_date = _today - timedelta(days=29), _today
    elif _preset == "This month":
        start_date, end_date = _today.replace(day=1), _today
    elif _preset == "Last month":
        _first_this = _today.replace(day=1)
        _last_prev = _first_this - timedelta(days=1)
        start_date, end_date = _last_prev.replace(day=1), _last_prev
    elif _preset == "Last 90 days":
        start_date, end_date = _today - timedelta(days=89), _today
    elif _preset == "Quarter to date":
        _q_start_month = ((_today.month - 1) // 3) * 3 + 1
        start_date, end_date = _today.replace(month=_q_start_month, day=1), _today
    elif _preset == "This year (Jan–Today)":
        start_date, end_date = _today.replace(month=1, day=1), _today
    elif _preset == "Last calendar year":
        _ly = _today.year - 1
        start_date, end_date = date(_ly, 1, 1), date(_ly, 12, 31)
    else:  # Custom
        start_date = st.date_input("From", value=date(2024, 1, 1))
        end_date = st.date_input("To", value=_today)
        if start_date > end_date:
            st.error("'From' must be before 'To'")
            st.stop()

    selected_year = start_date.year
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    st.markdown("---")

    # ── Hub filter ────────────────────────────────────────────────────────────
    st.subheader("Hub")
    _hub_names = get_hub_names()
    selected_hub = st.selectbox("Filter by Hub", ["All Hubs"] + _hub_names)
    hub_scope_key = selected_hub if selected_hub != "All Hubs" else "all"
    hub_sql = build_hub_scope(selected_hub)
    st.caption("Applies to Sales & Report Builder only")

    st.markdown("---")

    # ── User identity ─────────────────────────────────────────────────────────
    _user_email = get_current_user_email()
    if _user_email:
        _uid, _uname = resolve_user_display(_user_email)
        st.caption(f"User: {_uname}")
    else:
        st.caption("User: Not identified")

    if st.button("Refresh Data", use_container_width=True):
        load_dashboard_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
    st.caption("Data: C3_Web SQL Server")
    if selected_hub != "All Hubs":
        st.caption(f"Filtered: {selected_hub} hub")
    else:
        st.caption("Showing all historical data.")


# ─── Helpers ──────────────────────────────────────────────────────────────────
def fmt_number(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def safe_metric(label, value, help_text=None):
    st.metric(label=label, value=value, help=help_text)


def empty_chart_msg(msg="No data available for the selected period."):
    st.info(msg)


def chart_error(e):
    st.warning(f"Chart unavailable: {e}")


# ─── Load all data ────────────────────────────────────────────────────────────
try:
    with st.spinner("Loading dashboard data..."):
        _data = load_dashboard_data(start_str, end_str, hub_scope_key)
    crfq_kpis             = _data["crfq_kpis"]
    if crfq_kpis is None:
        raise RuntimeError(
            "No database connection. Run `az login` in your terminal to authenticate, "
            "then restart the dashboard."
        )
    df_customers          = _data["df_customers"]
    df_monthly_crfq       = _data["df_monthly_crfq"]
    df_rfq_results        = _data["df_rfq_results"]
    df_mpns               = _data["df_mpns"]
    df_sales_reps         = _data["df_sales_reps"]
    df_quote_value        = _data["df_quote_value"]
    df_countries          = _data["df_countries"]
    srfq_kpis             = _data["srfq_kpis"]
    top_supplier_name, top_supplier_val = _data["top_supplier_value"] or ("N/A", 0.0)
    df_suppliers          = _data["df_suppliers"]
    df_response_rates     = _data["df_response_rates"]
    df_monthly_srfq       = _data["df_monthly_srfq"]
    df_sourcing_status    = _data["df_sourcing_status"]
    df_sourced_mpns       = _data["df_sourced_mpns"]
    df_supplier_types     = _data["df_supplier_types"]
    df_margin             = _data["df_margin"]
    df_top_manufacturers  = _data["df_top_manufacturers"]
except RuntimeError as conn_err:
    st.error(f"Database connection failed: {conn_err}")
    st.stop()

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_sales, tab_sourcing, tab_reports, tab_deep, tab_ai = st.tabs([
    "Sales Dashboard", "Sourcing Dashboard", "Report Builder", "Deep Dive", "Ask Jerzy",
])


# ══════════════════════════════════════════════════════════════════════════════
#  SALES DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
with tab_sales:
    st.header("Sales Dashboard")
    st.caption(f"Showing data from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")

    # ── KPI Row ───────────────────────────────────────────────────────────────
    k1, k2, k3 = st.columns(3)
    with k1:
        safe_metric("Total CRFQs", fmt_number(crfq_kpis["total_crfqs"]),
                    "Non-deleted customer RFQs in period")
    with k2:
        safe_metric("Quote Rate", f"{crfq_kpis['quote_rate']}%",
                    "Parts that received a quote vs total parts requested")
    with k3:
        safe_metric("Active Customers", fmt_number(crfq_kpis["active_customers"]),
                    "Distinct customers with RFQ activity in period")

    st.markdown("---")

    # ── Row 2: Top Customers + RFQ Results ────────────────────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Top 10 Hottest Customers")
        try:
            if df_customers.empty:
                empty_chart_msg()
            else:
                df_c = df_customers.sort_values("RfqCount")
                fig = px.bar(
                    df_c, x="RfqCount", y="CustomerName", orientation="h",
                    text="RfqCount", color_discrete_sequence=[BLUE],
                    labels={"RfqCount": "RFQ Count", "CustomerName": ""},
                )
                fig.update_traces(textposition="outside")
                _chart(fig, height=340)
                fig.update_layout(yaxis=dict(tickfont=dict(size=12)), margin=dict(r=20))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    with col_right:
        st.subheader("Quote Status Breakdown (Part Lines)")
        try:
            if df_rfq_results.empty:
                empty_chart_msg()
            else:
                _qs_colors = {"Won": GREEN, "Quoted": PURPLE, "Lost": RED, "Not Quoted": "#94a3b8", "Other": "#cbd5e1"}
                color_map = {row["ResultLabel"]: _qs_colors.get(row["ResultLabel"], "#94a3b8")
                             for _, row in df_rfq_results.iterrows()}
                fig = px.pie(
                    df_rfq_results, values="Count", names="ResultLabel",
                    hole=0.45, color="ResultLabel", color_discrete_map=color_map,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                _chart(fig, height=340)
                fig.update_layout(showlegend=True, legend=dict(orientation="v", x=1, y=0.5))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    st.markdown("---")

    # ── Row 3: Monthly CRFQ Trend ─────────────────────────────────────────────
    st.subheader(f"Monthly CRFQ Volume — {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
    try:
        if df_monthly_crfq.empty:
            empty_chart_msg()
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_monthly_crfq["Period"], y=df_monthly_crfq["RfqCount"],
                name="CRFQs", mode="lines+markers",
                line=dict(color=BLUE, width=2.5),
                fill="tozeroy", fillcolor="rgba(74,108,247,0.10)",
                marker=dict(size=6),
            ))
            _chart(fig, height=280)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        chart_error(e)

    st.markdown("---")

    # ── Row 4: Top MPNs + Quote Value by Customer ─────────────────────────────
    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("Top 20 Most Requested MPNs")
        try:
            if df_mpns.empty:
                empty_chart_msg()
            else:
                fig = px.bar(
                    df_mpns, x="Mpn", y="RequestCount",
                    text="RequestCount", color_discrete_sequence=[PURPLE],
                    labels={"Mpn": "MPN", "RequestCount": "Request Count"},
                )
                fig.update_traces(textposition="outside")
                _chart(fig, height=320)
                fig.update_layout(margin=dict(b=60), xaxis=dict(tickangle=-45, tickfont=dict(size=10)))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    with col_r2:
        st.subheader("Top 10 Customers by Quote Value")
        try:
            if df_quote_value.empty:
                empty_chart_msg()
            else:
                df_qv = df_quote_value.sort_values("TotalQuoteValue")
                fig = px.bar(
                    df_qv, x="TotalQuoteValue", y="CustomerName", orientation="h",
                    color_discrete_sequence=[GREEN],
                    labels={"TotalQuoteValue": "Quote Value", "CustomerName": ""},
                )
                _chart(fig, height=320)
                fig.update_layout(xaxis=dict(tickformat="$,.0f"))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    st.markdown("---")

    # ── Row 5: Sales Rep Leaderboard + Country Distribution ───────────────────
    col_l3, col_r3 = st.columns([3, 2])

    with col_l3:
        st.subheader("Sales Rep Leaderboard")
        try:
            if df_sales_reps.empty:
                empty_chart_msg()
            else:
                display_reps = df_sales_reps.rename(columns={
                    "SalesRep": "Sales Rep", "RfqCount": "RFQs",
                    "WonCount": "Won", "WinRate": "Win Rate (%)",
                })
                st.dataframe(
                    display_reps,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Win Rate (%)": st.column_config.ProgressColumn(
                            "Win Rate (%)", min_value=0, max_value=100, format="%.1f%%"
                        )
                    },
                    height=320,
                )
        except Exception as e:
            chart_error(e)

    with col_r3:
        st.subheader("Customer Countries")
        try:
            if df_countries.empty:
                empty_chart_msg("No country data for selected period.")
            else:
                df_ct = df_countries.head(12).sort_values("CustomerCount")
                fig = px.bar(
                    df_ct, x="CustomerCount", y="Country", orientation="h",
                    color_discrete_sequence=[ORANGE],
                    labels={"CustomerCount": "Customers", "Country": ""},
                )
                _chart(fig, height=320)
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCING DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
with tab_sourcing:
    st.header("Sourcing Dashboard")
    st.caption(f"Showing data from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")

    # ── KPI Row ───────────────────────────────────────────────────────────────
    k1, k2, k3 = st.columns(3)
    with k1:
        safe_metric("Total SRFQs Sent", fmt_number(srfq_kpis["total_srfqs"]),
                    "Supplier RFQs sent in period")
    with k2:
        safe_metric("Response Rate", f"{srfq_kpis['response_rate']}%",
                    "Percentage of SRFQs that received a response")
    with k3:
        safe_metric(
            "Top Supplier (Won Value)",
            f"${top_supplier_val:,.0f}" if top_supplier_val > 0 else "N/A",
            f"Highest value won: {top_supplier_name}",
        )

    st.markdown("---")

    # ── Row 2: Top Suppliers + Supplier Type Distribution ─────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Top 10 Hottest Suppliers")
        try:
            if df_suppliers.empty:
                empty_chart_msg()
            else:
                df_s = df_suppliers.sort_values("SrfqCount")
                fig = px.bar(
                    df_s, x="SrfqCount", y="SupplierName", orientation="h",
                    text="SrfqCount", color_discrete_sequence=[BLUE],
                    labels={"SrfqCount": "SRFQ Count", "SupplierName": ""},
                )
                fig.update_traces(textposition="outside")
                _chart(fig, height=340)
                fig.update_layout(margin=dict(r=20))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    with col_right:
        st.subheader("Supplier Type Distribution")
        try:
            if df_supplier_types.empty:
                empty_chart_msg()
            else:
                type_labels = {
                    "ATD": "Authorized (ATD)",
                    "MFR": "Manufacturer (MFR)",
                    "BRK": "Broker (BRK)",
                    "HYB": "Hybrid (HYB)",
                }
                df_st = df_supplier_types.copy()
                df_st["TypeLabel"] = df_st["SupplierType"].map(lambda x: type_labels.get(x, x))
                fig = px.pie(
                    df_st, values="Count", names="TypeLabel",
                    hole=0.4, color_discrete_sequence=COLORS,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                _chart(fig, height=340)
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    st.markdown("---")

    # ── Row 3: Supplier Response Rate Comparison ──────────────────────────────
    st.subheader("Supplier Response Rate Comparison (Top 20 by Volume)")
    try:
        if df_response_rates.empty:
            empty_chart_msg()
        else:
            df_rr = df_response_rates.sort_values("TotalSent")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Responded",
                x=df_rr["TotalResponded"], y=df_rr["SupplierName"],
                orientation="h", marker_color=GREEN,
                text=df_rr["ResponseRate"].astype(str) + "%",
                textposition="auto",
            ))
            fig.add_trace(go.Bar(
                name="No Response",
                x=df_rr["NoResponse"], y=df_rr["SupplierName"],
                orientation="h", marker_color="#e2e8f0",
            ))
            _chart(fig, height=480)
            fig.update_layout(
                barmode="stack",
                margin=dict(r=20),
                legend=dict(orientation="h", yanchor="bottom", y=1, x=0),
                xaxis=dict(title="SRFQ Count"),
                yaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        chart_error(e)

    st.markdown("---")

    # ── Row 4: Monthly SRFQ Trend + Sourcing Status ───────────────────────────
    col_l2, col_r2 = st.columns([3, 2])

    with col_l2:
        st.subheader(f"Monthly SRFQ Volume — {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
        try:
            if df_monthly_srfq.empty:
                empty_chart_msg()
            else:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_monthly_srfq["Period"], y=df_monthly_srfq["SrfqCount"],
                    name="Total SRFQs", mode="lines+markers",
                    line=dict(color=BLUE, width=2.5),
                    fill="tozeroy", fillcolor="rgba(74,108,247,0.10)",
                    marker=dict(size=6),
                ))
                fig.add_trace(go.Scatter(
                    x=df_monthly_srfq["Period"], y=df_monthly_srfq["RespondedCount"],
                    name="Responded", mode="lines+markers",
                    line=dict(color=GREEN, width=2, dash="dot"),
                    fill="tozeroy", fillcolor="rgba(34,197,94,0.08)",
                    marker=dict(size=6),
                ))
                _chart(fig, height=300)
                fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1, x=0))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    with col_r2:
        st.subheader("Part Sourcing Status")
        try:
            if df_sourcing_status.empty:
                empty_chart_msg()
            else:
                color_map = {row["StatusLabel"]: STATUS_COLORS.get(row["StatusLabel"], "#94a3b8")
                             for _, row in df_sourcing_status.iterrows()}
                fig = px.pie(
                    df_sourcing_status, values="Count", names="StatusLabel",
                    hole=0.45, color="StatusLabel", color_discrete_map=color_map,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                _chart(fig, height=300)
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    st.markdown("---")

    # ── Row 5: Top Sourced MPNs + Margin Analysis ─────────────────────────────
    col_l3, col_r3 = st.columns(2)

    with col_l3:
        st.subheader("Top Sourced MPNs")
        try:
            if df_sourced_mpns.empty:
                empty_chart_msg()
            else:
                fig = px.bar(
                    df_sourced_mpns, x="Mpn", y="SrfqCount",
                    text="SrfqCount", color_discrete_sequence=[PURPLE],
                    labels={"Mpn": "MPN", "SrfqCount": "Sourcing Count"},
                )
                fig.update_traces(textposition="outside")
                _chart(fig, height=320)
                fig.update_layout(margin=dict(b=60), xaxis=dict(tickangle=-45, tickfont=dict(size=10)))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    with col_r3:
        st.subheader("Margin Analysis (Won Orders)")
        try:
            if df_margin.empty:
                empty_chart_msg("No won orders found for this period.")
            else:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    name="Sale Value", x=df_margin["Period"], y=df_margin["SaleValue"],
                    marker_color=GREEN, opacity=0.85,
                ))
                fig.add_trace(go.Bar(
                    name="Cost Value", x=df_margin["Period"], y=df_margin["CostValue"],
                    marker_color=RED, opacity=0.85,
                ))
                if "MarginPct" in df_margin.columns:
                    fig.add_trace(go.Scatter(
                        name="Margin %", x=df_margin["Period"], y=df_margin["MarginPct"],
                        mode="lines+markers", line=dict(color=BLUE, width=2),
                        yaxis="y2",
                    ))
                _chart(fig, height=320)
                fig.update_layout(
                    barmode="group",
                    margin=dict(r=40, b=40),
                    xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
                    yaxis=dict(tickformat="$,.0f"),
                    yaxis2=dict(
                        overlaying="y", side="right", showgrid=False,
                        ticksuffix="%", title="Margin %",
                    ),
                    legend=dict(orientation="h", yanchor="bottom", y=1, x=0),
                )
                st.caption("Values in native currency — multi-currency conversion not applied.")
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            chart_error(e)

    st.markdown("---")

    # ── Row 6: Top Manufacturers ──────────────────────────────────────────────
    st.subheader("Top Manufacturer Brands (by SRFQ Volume)")
    try:
        if df_top_manufacturers.empty:
            empty_chart_msg("No manufacturer data available for this period.")
        else:
            df_m = df_top_manufacturers.sort_values("SrfqCount")
            fig = px.bar(
                df_m, x="SrfqCount", y="Manufacturer", orientation="h",
                text="SrfqCount", color_discrete_sequence=[PURPLE],
                labels={"SrfqCount": "SRFQ Count", "Manufacturer": ""},
            )
            fig.update_traces(textposition="outside")
            _chart(fig, height=max(300, len(df_m) * 32 + 40))
            fig.update_layout(margin=dict(r=20))
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        chart_error(e)


# ══════════════════════════════════════════════════════════════════════════════
#  REPORT BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def _fmt(val) -> str:
    """Format a number as 1.2M / 450K / 3,241 for chart labels."""
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if abs(v) >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"{v/1_000:.1f}K"
    return f"{v:,.0f}"


GA_BLUE = "#1a73e8"
GA_COLORS = ["#1a73e8", "#34a853", "#fbbc04", "#ea4335", "#9334e6",
             "#00bcd4", "#ff7043", "#0097a7"]

# ── Report Builder: dimension + metric groupings (GA Free Form style) ──────────
_ALL_DIMS_ORDERED = [
    "Month", "Year",                                      # Time
    "Customer", "Sales Rep", "Country", "RFQ Result",    # Customer side
    "MPN", "Sourcing Status",                             # Parts
    "Supplier", "Supplier Type",                          # Supplier side
]
_METRIC_GROUPS = {
    "Customer RFQ": [
        "CRFQ Count", "Unique Customers", "Total Parts Requested",
        "Quoted Parts", "Sale Value", "Win Rate (%)",
    ],
    "Sourcing": [
        "SRFQ Count", "Responded SRFQs", "Won SRFQs",
        "Unique Suppliers", "Response Rate (%)",
    ],
    "Parts": ["Sourced Parts"],
}
_ALL_METRICS_ORDERED = [m for ms in _METRIC_GROUPS.values() for m in ms]


with tab_reports:

    # ── 3-panel layout: left config (1/5) | main content (4/5) ────────────────
    col_cfg, col_main = st.columns([1, 4], gap="large")

    # ══════════════════════════════════════════════════════════════════════
    # LEFT PANEL — GA Free Form style
    # ══════════════════════════════════════════════════════════════════════
    with col_cfg:
        st.markdown('<div class="rb-config-panel">', unsafe_allow_html=True)

        # Technique (static)
        st.markdown('<div class="rb-config-title">Technique</div>', unsafe_allow_html=True)
        st.selectbox("technique_", ["Free form"], label_visibility="collapsed", key="rb_technique")

        st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)

        # Visualization
        st.markdown('<div class="rb-config-title">Visualization</div>', unsafe_allow_html=True)
        chart_type = st.radio(
            "Visualization", ["Bar", "Horizontal Bar", "Line", "Pie", "Table only"],
            label_visibility="collapsed", key="rb_chart",
        )

        st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)

        # Rows (dimension)
        st.markdown('<div class="rb-config-title">Rows</div>', unsafe_allow_html=True)
        selected_dim = st.selectbox(
            "Dimension", _ALL_DIMS_ORDERED,
            label_visibility="collapsed", key="rb_dim",
        )

        st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)

        # Metrics (Values) — grouped checkboxes filtered to valid dims
        st.markdown('<div class="rb-config-title">Metrics (Values)</div>', unsafe_allow_html=True)
        _valid_metrics = [m for m, dims in METRIC_DIMENSIONS.items() if selected_dim in dims]
        selected_metrics = []
        for _grp_name, _grp_metrics in _METRIC_GROUPS.items():
            _grp_valid = [m for m in _grp_metrics if m in _valid_metrics]
            if not _grp_valid:
                continue
            st.markdown(
                f'<div style="font-size:10px;color:#9aa0a6;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.8px;margin:8px 0 2px 0;">'
                f'{_grp_name}</div>',
                unsafe_allow_html=True,
            )
            for _m in _grp_valid:
                if st.checkbox(_m, key=f"rb_m_{_m}"):
                    selected_metrics.append(_m)

        _unavail_count = len([m for m in _ALL_METRICS_ORDERED if m not in _valid_metrics])
        if _unavail_count:
            st.markdown(
                f'<div style="font-size:11px;color:#9aa0a6;margin-top:6px;">'
                f'{_unavail_count} metric(s) not available for "{selected_dim}"</div>',
                unsafe_allow_html=True,
            )

        st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)

        # Show rows
        st.markdown('<div class="rb-config-title">Show Rows</div>', unsafe_allow_html=True)
        top_n_opt = st.select_slider(
            "Show rows", options=["5", "10", "20", "50", "All"],
            value="20", label_visibility="collapsed", key="rb_topn",
        )
        top_n = None if top_n_opt == "All" else int(top_n_opt)

        st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)

        # Filters
        st.markdown('<div class="rb-config-title">Filters</div>', unsafe_allow_html=True)
        filter_customer = st.text_input("Customer contains", placeholder="e.g. Flex", key="rb_cust")
        filter_supplier = st.text_input("Supplier contains", placeholder="e.g. Arrow", key="rb_supp")
        filter_mpn      = st.text_input("MPN contains", placeholder="e.g. 1N4148", key="rb_mpn")

        st.markdown('</div>', unsafe_allow_html=True)

        run_clicked = st.button("Apply", type="primary", use_container_width=True, key="rb_run")

    # ══════════════════════════════════════════════════════════════════════
    # MAIN CONTENT AREA
    # ══════════════════════════════════════════════════════════════════════
    with col_main:

        if not run_clicked:
            st.markdown("""
            <div class="rb-empty-state">
                <div class="icon">📊</div>
                <div class="title">Build your exploration</div>
                <div class="sub">Pick a <strong>Row dimension</strong>, select one or more
                <strong>Metrics</strong>, then click <strong>Apply</strong>.</div>
            </div>
            """, unsafe_allow_html=True)

        elif not selected_metrics:
            st.markdown("""
            <div class="rb-empty-state">
                <div class="icon">☑️</div>
                <div class="title">No metrics selected</div>
                <div class="sub">Check at least one metric in the <strong>Metrics (Values)</strong>
                section on the left, then click <strong>Apply</strong>.</div>
            </div>
            """, unsafe_allow_html=True)

        else:
            # Meta bar
            _active = " · ".join(filter(None, [
                f"Customer: {filter_customer}" if filter_customer.strip() else "",
                f"Supplier: {filter_supplier}" if filter_supplier.strip() else "",
                f"MPN: {filter_mpn}" if filter_mpn.strip() else "",
            ]))
            _meta = " · ".join(filter(None, [
                f"{start_date.strftime('%d %b %Y')} – {end_date.strftime('%d %b %Y')}",
                f"Top {top_n} rows" if top_n else "All rows",
                _active,
            ]))
            st.markdown(
                f'<p class="rb-result-title">Rows: {selected_dim}</p>'
                f'<p class="rb-result-meta">{_meta}</p>',
                unsafe_allow_html=True,
            )

            # Fetch all selected metrics
            with st.spinner("Loading data..."):
                _metric_dfs: dict = {}
                _errors = []
                for _metric in selected_metrics:
                    try:
                        _df = run_any_report(
                            metric=_metric, dimension=selected_dim,
                            start_date=start_str, end_date=end_str,
                            top_n=top_n,
                            filter_customer=filter_customer,
                            filter_supplier=filter_supplier,
                            filter_mpn=filter_mpn,
                            hub_sql=hub_sql,
                        )
                        _metric_dfs[_metric] = _df
                    except RuntimeError as _e:
                        _errors.append(f"{_metric}: {_e}")
                    except Exception as _e:
                        _errors.append(f"{_metric}: {_e}")

            for _err in _errors:
                st.error(_err)

            # Build multi-metric summary table
            _summary_df = None
            _primary_metric = None
            for _metric, _df in _metric_dfs.items():
                if _df.empty:
                    continue
                if _summary_df is None:
                    _summary_df = _df.rename(columns={"Value": _metric})
                    _primary_metric = _metric
                else:
                    _summary_df = _summary_df.merge(
                        _df.rename(columns={"Value": _metric}), on="Label", how="outer",
                    )

            if _summary_df is None or _summary_df.empty:
                st.markdown("""
                <div class="rb-empty-state">
                    <div class="icon">🔍</div>
                    <div class="title">No results</div>
                    <div class="sub">No data for this combination. Try widening the date range or removing filters.</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                _summary_df = _summary_df.fillna(0)
                _display_df = _summary_df.rename(columns={"Label": selected_dim})

                _badge = len(selected_metrics)
                st.markdown(
                    f'<span style="background:#e8f0fe;color:#1a73e8;border-radius:10px;'
                    f'padding:2px 10px;font-size:12px;font-weight:600;">'
                    f'{_badge} metric{"s" if _badge > 1 else ""}</span>',
                    unsafe_allow_html=True,
                )

                # Summary table (always shown)
                _tbl_h = min(520, (len(_display_df) + 1) * 35 + 38)
                st.dataframe(
                    _display_df, use_container_width=True, hide_index=True, height=_tbl_h,
                    column_config={
                        col: st.column_config.NumberColumn(
                            col, format="%.1f%%" if "%" in col else "%.0f",
                        )
                        for col in _display_df.columns if col != selected_dim
                    },
                )

                # CSV export
                _csv = _display_df.to_csv(index=False)
                _safe_d = selected_dim.replace(" ", "_")
                _safe_m = "_".join(m.replace(" ", "_").replace("/", "-").replace("(", "").replace(")", "")
                                   for m in selected_metrics[:3])
                st.download_button(
                    "Export CSV", _csv,
                    file_name=f"explore_{_safe_d}_{_safe_m}.csv",
                    mime="text/csv", key="rb_export",
                )

                # Chart (skip if "Table only" or no primary metric)
                if chart_type != "Table only" and _primary_metric is not None:
                    st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="rb-config-title" style="margin-bottom:8px;">'
                        f'Chart: {_primary_metric} by {selected_dim}</div>',
                        unsafe_allow_html=True,
                    )
                    _chart_df = _summary_df[["Label", _primary_metric]].rename(
                        columns={_primary_metric: "Value"}
                    )
                    _is_time = selected_dim in ("Month", "Year")
                    _is_pct = "%" in _primary_metric
                    _hover_y = "%{y:.1f}%" if _is_pct else "%{y:,.0f}"
                    _chart_layout = dict(
                        plot_bgcolor="white", paper_bgcolor="white",
                        font=dict(family="Google Sans, Inter, sans-serif", size=12, color="#3c4043"),
                        margin=dict(l=0, r=16, t=16, b=60),
                    )
                    try:
                        if chart_type == "Pie":
                            _fig = px.pie(
                                _chart_df, values="Value", names="Label",
                                hole=0.45, color_discrete_sequence=GA_COLORS,
                            )
                            _fig.update_traces(
                                textposition="inside", textinfo="percent",
                                hovertemplate="<b>%{label}</b><br>%{value:,.1f}<extra></extra>",
                            )
                            _fig.update_layout(
                                height=420, legend=dict(orientation="v", x=1.02, y=0.5),
                                margin=dict(l=0, r=120, t=16, b=16),
                                paper_bgcolor="white",
                                font=dict(family="Google Sans, Inter, sans-serif", size=12),
                            )
                            st.plotly_chart(_fig, use_container_width=True)
                        elif chart_type == "Line":
                            _fig = px.line(
                                _chart_df, x="Label", y="Value", markers=True,
                                color_discrete_sequence=[GA_BLUE],
                                labels={"Label": selected_dim, "Value": _primary_metric},
                            )
                            _fig.update_traces(
                                line_width=2, marker_size=6,
                                hovertemplate=f"<b>%{{x}}</b><br>{_hover_y}<extra></extra>",
                            )
                            _fig.update_layout(
                                height=380,
                                xaxis=dict(showgrid=False, tickangle=-30 if not _is_time else 0,
                                           linecolor="#e8eaed", tickcolor="#e8eaed"),
                                yaxis=dict(gridcolor="#f1f3f4", zeroline=False,
                                           ticksuffix="%" if _is_pct else ""),
                                **_chart_layout,
                            )
                            st.plotly_chart(_fig, use_container_width=True)
                        elif chart_type == "Horizontal Bar":
                            _df_s = _chart_df.sort_values("Value", ascending=True)
                            _fig = px.bar(
                                _df_s, x="Value", y="Label", orientation="h",
                                color_discrete_sequence=[GA_BLUE],
                                labels={"Label": selected_dim, "Value": _primary_metric},
                            )
                            _fig.update_traces(
                                text=_df_s["Value"].map(_fmt),
                                textposition="outside",
                                hovertemplate=f"<b>%{{y}}</b><br>{_hover_y.replace('%{y','%{x')}<extra></extra>",
                            )
                            _fig.update_layout(
                                height=max(380, len(_chart_df) * 30 + 60),
                                xaxis=dict(gridcolor="#f1f3f4", zeroline=False,
                                           showticklabels=False,
                                           ticksuffix="%" if _is_pct else ""),
                                yaxis=dict(tickfont=dict(size=11), automargin=True),
                                margin=dict(l=0, r=80, t=16, b=16),
                                plot_bgcolor="white", paper_bgcolor="white",
                                font=dict(family="Google Sans, Inter, sans-serif", size=12),
                            )
                            st.plotly_chart(_fig, use_container_width=True)
                        else:  # Bar
                            _fig = px.bar(
                                _chart_df, x="Label", y="Value",
                                color_discrete_sequence=[GA_BLUE],
                                labels={"Label": selected_dim, "Value": _primary_metric},
                            )
                            _fig.update_traces(
                                text=_chart_df["Value"].map(_fmt),
                                textposition="outside",
                                hovertemplate=f"<b>%{{x}}</b><br>{_hover_y}<extra></extra>",
                            )
                            _fig.update_layout(
                                height=380,
                                xaxis=dict(tickangle=-35 if not _is_time else 0,
                                           tickfont=dict(size=10), showgrid=False,
                                           linecolor="#e8eaed"),
                                yaxis=dict(gridcolor="#f1f3f4", zeroline=False,
                                           showticklabels=False,
                                           ticksuffix="%" if _is_pct else ""),
                                **_chart_layout,
                            )
                            st.plotly_chart(_fig, use_container_width=True)
                    except Exception as _ce:
                        chart_error(_ce)


# ══════════════════════════════════════════════════════════════════════════════
#  DEEP DIVE
# ══════════════════════════════════════════════════════════════════════════════
def _dd_hbar(df, x_col, y_col, height=None):
    """Reusable horizontal bar for Deep Dive charts."""
    df_s = df.sort_values(x_col, ascending=True)
    fig = px.bar(
        df_s, x=x_col, y=y_col, orientation="h",
        color_discrete_sequence=[GA_BLUE],
    )
    fig.update_traces(
        text=df_s[x_col].map(_fmt), textposition="outside",
        hovertemplate=f"<b>%{{y}}</b><br>%{{x:,.0f}}<extra></extra>",
    )
    h = height or max(300, len(df_s) * 30 + 60)
    _chart(fig, height=h)
    fig.update_layout(
        xaxis=dict(showticklabels=False),
        yaxis=dict(automargin=True),
        margin=dict(r=70),
    )
    return fig


def _dd_donut(df, values_col, names_col, colors=None):
    fig = px.pie(
        df, values=values_col, names=names_col,
        hole=0.5,
        color_discrete_sequence=colors or GA_COLORS,
    )
    fig.update_traces(
        textposition="inside", textinfo="percent",
        hovertemplate="<b>%{label}</b><br>%{value:,.0f}<extra></extra>",
    )
    _chart(fig, height=320)
    fig.update_layout(
        legend=dict(orientation="v", x=1.02, y=0.5, font=dict(size=11)),
        margin=dict(r=120),
    )
    return fig


def _dd_line(df, x_col, y_cols: list, names: list, height=320):
    fig = go.Figure()
    line_colors = [GA_BLUE, "#34a853", "#fbbc04"]
    fill_colors = ["rgba(26,115,232,0.10)", "rgba(52,168,83,0.08)", "rgba(251,188,4,0.08)"]
    for i, (col, name) in enumerate(zip(y_cols, names)):
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df[col], name=name,
            mode="lines+markers",
            line=dict(color=line_colors[i % len(line_colors)], width=2),
            fill="tozeroy" if i == 0 else "none",
            fillcolor=fill_colors[i % len(fill_colors)],
            marker=dict(size=5),
            hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:,.0f}}<extra></extra>",
        ))
    _chart(fig, height=height)
    fig.update_layout(
        xaxis=dict(tickangle=-30),
        legend=dict(orientation="h", y=1.08),
        margin=dict(b=60),
    )
    return fig


with tab_deep:
    # ── Search bar ─────────────────────────────────────────────────────────────
    st.markdown('<p class="rb-result-title">Entity Deep Dive</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="rb-result-meta">Search for any customer, supplier, or MPN to see a full profile.</p>',
        unsafe_allow_html=True,
    )

    srch_type = st.radio(
        "Entity type", ["Customer", "Supplier", "MPN", "Manufacturer"],
        horizontal=True, key="dd_type",
    )
    srch_col, btn_col = st.columns([4, 1])
    with srch_col:
        srch_query = st.text_input(
            "Search",
            placeholder=f"Type a {srch_type.lower()} name…",
            label_visibility="collapsed", key="dd_query",
        )
    with btn_col:
        srch_clicked = st.button("Search", type="primary", use_container_width=True, key="dd_search")

    # ── Run search ─────────────────────────────────────────────────────────────
    dd_results = st.session_state.get("dd_results", [])
    dd_type_for_results = st.session_state.get("dd_type_for_results", srch_type)

    if srch_clicked:
        if not srch_query.strip():
            st.warning("Enter a search term first.")
        else:
            with st.spinner("Searching…"):
                if srch_type == "Customer":
                    dd_results = deepdive_data.search_customers(srch_query)
                elif srch_type == "Supplier":
                    dd_results = deepdive_data.search_suppliers(srch_query)
                elif srch_type == "Manufacturer":
                    dd_results = deepdive_data.search_manufacturers(srch_query)
                else:
                    dd_results = deepdive_data.search_mpns(srch_query)
            st.session_state["dd_results"] = dd_results
            st.session_state["dd_type_for_results"] = srch_type
            dd_type_for_results = srch_type

    # ── Entity selector + profile ──────────────────────────────────────────────
    if dd_results and dd_type_for_results == srch_type:
        st.markdown(f"**{len(dd_results)}** match{'es' if len(dd_results) != 1 else ''} found:")
        placeholder = f"-- Select a {srch_type.lower()} --"
        selected_entity = st.selectbox(
            f"Select {srch_type}", [placeholder] + dd_results,
            label_visibility="collapsed", key="dd_entity",
        )

        if selected_entity == placeholder:
            st.info(f"Select a {srch_type.lower()} from the list above to view its profile.")
        else:
            st.markdown('<hr class="rb-divider">', unsafe_allow_html=True)

        # ── Customer profile ───────────────────────────────────────────────────
        if selected_entity != placeholder and srch_type == "Customer":
            with st.spinner(f"Loading profile for {selected_entity}…"):
                kpis          = deepdive_data.get_customer_kpis(selected_entity, start_str, end_str)
                df_trend      = deepdive_data.get_customer_monthly_trend(selected_entity, start_str, end_str)
                df_results    = deepdive_data.get_customer_rfq_results(selected_entity, start_str, end_str)
                df_mpns       = deepdive_data.get_customer_top_mpns(selected_entity, start_str, end_str)
                df_suppliers  = deepdive_data.get_customer_top_suppliers(selected_entity, start_str, end_str)
                df_mfrs       = deepdive_data.get_customer_manufacturers(selected_entity, start_str, end_str)
                df_reps       = deepdive_data.get_customer_sales_reps(selected_entity, start_str, end_str)

            if not kpis:
                st.info("No data found for this customer in the selected date range.")
            else:
                # Header
                meta_bits = [b for b in [kpis.get("country"), f"Sales Rep: {kpis.get('sales_rep')}"] if b]
                st.markdown(
                    f'<p class="rb-result-title">{selected_entity}'
                    + (f'  <span class="rb-pill-gray">{"  ·  ".join(meta_bits)}</span>' if meta_bits else "")
                    + "</p>",
                    unsafe_allow_html=True,
                )

                # KPI row
                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric("Total RFQs",   f"{kpis['total_rfqs']:,}")
                k2.metric("Total Lines",  f"{kpis['total_parts']:,}")
                k3.metric("Quoted Rate",  f"{kpis['quoted_rate']}%",
                          help=f"{kpis['quoted_parts']:,} quoted / {kpis['total_parts']:,} total lines")
                k4.metric("Quoted → Won", f"{kpis['quoted_won_ratio']}%",
                          help=f"{kpis['won_parts']:,} won / {kpis['quoted_parts']:,} quoted lines")
                k5.metric("Won Rate",     f"{kpis['won_rate']}%",
                          help=f"{kpis['won_parts']:,} won / {kpis['total_parts']:,} total lines")

                st.markdown("")
                c_left, c_right = st.columns(2)

                with c_left:
                    st.markdown('<div class="rb-config-title">Monthly RFQ Trend</div>', unsafe_allow_html=True)
                    if not df_trend.empty:
                        st.plotly_chart(_dd_line(df_trend, "Month", ["RfqCount"], ["RFQs"]), use_container_width=True)
                    else:
                        st.caption("No trend data.")

                with c_right:
                    st.markdown('<div class="rb-config-title">Quote Status Breakdown</div>', unsafe_allow_html=True)
                    if not df_results.empty:
                        _quote_colors = {"Won": GREEN, "Quoted": PURPLE, "Lost": RED, "Not Quoted": "#94a3b8"}
                        result_colors = [_quote_colors.get(r, "#cbd5e1") for r in df_results["Label"]]
                        st.plotly_chart(_dd_donut(df_results, "Count", "Label", result_colors), use_container_width=True)
                    else:
                        st.caption("No result data.")

                st.markdown('<div class="rb-config-title" style="margin-top:8px;">Top MPNs Requested</div>', unsafe_allow_html=True)
                if not df_mpns.empty:
                    st.plotly_chart(_dd_hbar(df_mpns, "RequestCount", "Mpn", height=max(280, len(df_mpns) * 28 + 40)), use_container_width=True)
                else:
                    st.caption("No MPN data.")

                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Supplier Intelligence</div>', unsafe_allow_html=True)

                sup_left, sup_right = st.columns(2)
                with sup_left:
                    st.markdown('<div class="rb-config-title">Top Suppliers Handling Orders</div>', unsafe_allow_html=True)
                    if not df_suppliers.empty:
                        st.plotly_chart(_dd_hbar(df_suppliers, "SrfqCount", "Supplier",
                                                 height=max(280, len(df_suppliers) * 28 + 40)), use_container_width=True)
                    else:
                        st.caption("No supplier data (no SRFQs linked to this customer).")

                with sup_right:
                    st.markdown('<div class="rb-config-title">Manufacturers Used</div>', unsafe_allow_html=True)
                    if not df_mfrs.empty:
                        st.plotly_chart(_dd_hbar(df_mfrs, "SrfqCount", "Manufacturer",
                                                 height=max(280, len(df_mfrs) * 28 + 40)), use_container_width=True)
                    else:
                        st.caption("No manufacturer data.")

                if not df_reps.empty:
                    st.markdown('<div class="rb-config-title" style="margin-top:8px;">Sales Rep Activity</div>', unsafe_allow_html=True)
                    st.dataframe(
                        df_reps.rename(columns={"SalesRep": "Sales Rep", "RfqCount": "RFQs", "Won": "Won"}),
                        use_container_width=True, hide_index=True,
                    )

        # ── Supplier profile ───────────────────────────────────────────────────
        elif selected_entity != placeholder and srch_type == "Supplier":
            with st.spinner(f"Loading profile for {selected_entity}…"):
                kpis          = deepdive_data.get_supplier_kpis(selected_entity, start_str, end_str)
                df_trend      = deepdive_data.get_supplier_monthly_trend(selected_entity, start_str, end_str)
                df_status     = deepdive_data.get_supplier_status_breakdown(selected_entity, start_str, end_str)
                df_mpns       = deepdive_data.get_supplier_top_mpns(selected_entity, start_str, end_str)
                df_customers  = deepdive_data.get_supplier_top_customers(selected_entity, start_str, end_str)
                df_mfrs       = deepdive_data.get_supplier_manufacturers(selected_entity, start_str, end_str)
                df_reps       = deepdive_data.get_supplier_sales_reps(selected_entity, start_str, end_str)

            if not kpis:
                st.info("No data found for this supplier in the selected date range.")
            else:
                types_str = "  ·  ".join(kpis.get("supplier_types") or []) or "Unknown type"
                st.markdown(
                    f'<p class="rb-result-title">{selected_entity}'
                    f'  <span class="rb-pill-gray">{types_str}</span></p>',
                    unsafe_allow_html=True,
                )

                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric("SRFQs Sent",    f"{kpis['total_srfqs']:,}",
                          help=f"Across {kpis['total_crfqs']:,} distinct CRFQs")
                k2.metric("Response Rate", f"{kpis['response_rate']}%",
                          help=f"{kpis['responded']:,} responded / {kpis['total_srfqs']:,} sent")
                k3.metric("Responded",     f"{kpis['responded']:,}")
                k4.metric("Won Lines",     f"{kpis['won_parts']:,}",
                          help=f"Lines where the customer deal was Won (QuoteStatus = 30)")
                k5.metric("Win Rate",      f"{kpis['win_rate']}%",
                          help=f"{kpis['won_parts']:,} won / {kpis['total_srfqs']:,} sent")

                st.markdown("")
                s_left, s_right = st.columns(2)

                with s_left:
                    st.markdown('<div class="rb-config-title">Monthly Volume & Responses</div>', unsafe_allow_html=True)
                    if not df_trend.empty:
                        st.plotly_chart(
                            _dd_line(df_trend, "Month", ["SrfqCount", "RespondedCount"], ["Sent", "Responded"]),
                            use_container_width=True,
                        )
                    else:
                        st.caption("No trend data.")

                with s_right:
                    st.markdown('<div class="rb-config-title">Quote Status Breakdown</div>', unsafe_allow_html=True)
                    if not df_status.empty:
                        _quote_colors = {"Won": GREEN, "Quoted": PURPLE, "Lost": RED, "Not Quoted": "#94a3b8"}
                        status_colors = [_quote_colors.get(r, "#cbd5e1") for r in df_status["Label"]]
                        st.plotly_chart(_dd_donut(df_status, "Count", "Label", status_colors), use_container_width=True)
                    else:
                        st.caption("No status data.")

                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Parts & Customer Intelligence</div>', unsafe_allow_html=True)

                pi_left, pi_right = st.columns(2)
                with pi_left:
                    st.markdown('<div class="rb-config-title">Top MPNs Quoted</div>', unsafe_allow_html=True)
                    if not df_mpns.empty:
                        st.plotly_chart(_dd_hbar(df_mpns, "SrfqCount", "Mpn",
                                                 height=max(280, len(df_mpns) * 28 + 40)), use_container_width=True)
                    else:
                        st.caption("No MPN data.")

                with pi_right:
                    st.markdown('<div class="rb-config-title">Customers Served</div>', unsafe_allow_html=True)
                    if not df_customers.empty:
                        st.plotly_chart(_dd_hbar(df_customers, "SrfqCount", "Customer",
                                                 height=max(280, len(df_customers) * 28 + 40)), use_container_width=True)
                    else:
                        st.caption("No customer data.")

                if not df_mfrs.empty:
                    st.markdown('<div class="rb-config-title">Top Manufacturer Brands</div>', unsafe_allow_html=True)
                    st.plotly_chart(_dd_hbar(df_mfrs, "SrfqCount", "Manufacturer",
                                             height=max(280, len(df_mfrs) * 28 + 40)), use_container_width=True)

                if not df_reps.empty:
                    st.markdown('<div class="rb-config-title" style="margin-top:8px;">Sourcing Requestors</div>', unsafe_allow_html=True)
                    st.dataframe(
                        df_reps.rename(columns={"SourcingRequestor": "Sourcing Requestor", "SrfqCount": "SRFQs Sent", "Won": "Won Lines"}),
                        use_container_width=True, hide_index=True,
                    )

        # ── MPN profile ────────────────────────────────────────────────────────
        elif selected_entity != placeholder and srch_type == "MPN":
            with st.spinner(f"Loading profile for {selected_entity}…"):
                kpis           = deepdive_data.get_mpn_kpis(selected_entity, start_str, end_str)
                df_trend       = deepdive_data.get_mpn_monthly_trend(selected_entity, start_str, end_str)
                df_customers   = deepdive_data.get_mpn_top_customers(selected_entity, start_str, end_str)
                df_sourcing    = deepdive_data.get_mpn_sourcing_breakdown(selected_entity, start_str, end_str)
                df_suppliers   = deepdive_data.get_mpn_top_suppliers(selected_entity, start_str, end_str)
                df_mfrs        = deepdive_data.get_mpn_manufacturers(selected_entity, start_str, end_str)
                df_reps        = deepdive_data.get_mpn_sales_reps(selected_entity, start_str, end_str)
                df_pricing     = deepdive_data.get_mpn_pricing_trend(selected_entity, start_str, end_str)
                df_history     = deepdive_data.get_mpn_part_history(selected_entity, start_str, end_str)

            if not kpis:
                st.info("No data found for this MPN in the selected date range.")
            else:
                st.markdown(
                    f'<p class="rb-result-title">{selected_entity}</p>',
                    unsafe_allow_html=True,
                )

                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric("Times Requested", f"{kpis['request_count']:,}")
                k2.metric("Units Requested", f"{kpis['total_qty']:,}")
                k3.metric("Unique Customers", f"{kpis['unique_customers']:,}")
                k4.metric("Sourced", f"{kpis['sourced_count']:,}")
                k5.metric("Sourced Rate", f"{kpis['sourced_rate']}%")

                # ── Pricing Trend ─────────────────────────────────────────────
                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Pricing Trend</div>', unsafe_allow_html=True)
                st.markdown('<p class="rb-result-meta">Quoted prices (sale &amp; cost) are the accepted prices — use these to see if this MPN is getting cheaper or more expensive over time.</p>', unsafe_allow_html=True)
                if not df_pricing.empty:
                    df_pricing["Date"] = pd.to_datetime(df_pricing["Date"])
                    fig_price = go.Figure()

                    # Quoted Sale Price — prominent green line (accepted sale price to customer)
                    df_qsale = df_pricing[df_pricing["PriceType"] == "Quoted Sale Price"]
                    if not df_qsale.empty:
                        fig_price.add_trace(go.Scatter(
                            x=df_qsale["Date"], y=df_qsale["Price"],
                            name="Quoted Sale Price",
                            mode="lines+markers",
                            line=dict(color="#34a853", width=3),
                            marker=dict(size=8, symbol="diamond"),
                            customdata=df_qsale[["Entity", "Qty"]].values,
                            hovertemplate="<b>%{customdata[0]}</b><br>Sale: $%{y:,.2f}<br>Qty: %{customdata[1]:,.0f}<br>%{x|%Y-%m-%d}<extra></extra>",
                        ))

                    # Quoted Cost — prominent blue line (accepted cost from supplier)
                    df_qcost = df_pricing[df_pricing["PriceType"] == "Quoted Cost"]
                    if not df_qcost.empty:
                        fig_price.add_trace(go.Scatter(
                            x=df_qcost["Date"], y=df_qcost["Price"],
                            name="Quoted Cost",
                            mode="lines+markers",
                            line=dict(color="#1a73e8", width=3),
                            marker=dict(size=8, symbol="diamond"),
                            customdata=df_qcost[["Entity", "Qty"]].values,
                            hovertemplate="<b>%{customdata[0]}</b><br>Cost: $%{y:,.2f}<br>Qty: %{customdata[1]:,.0f}<br>%{x|%Y-%m-%d}<extra></extra>",
                        ))

                    # Supplier offers — faded red dots (all offers, not just accepted)
                    df_offers = df_pricing[df_pricing["PriceType"] == "Supplier Offer"]
                    if not df_offers.empty:
                        fig_price.add_trace(go.Scatter(
                            x=df_offers["Date"], y=df_offers["Price"],
                            name="Supplier Offers (all)",
                            mode="markers",
                            marker=dict(size=6, color="rgba(234,67,53,0.4)", symbol="circle"),
                            customdata=df_offers[["Entity", "Qty"]].values,
                            hovertemplate="<b>%{customdata[0]}</b><br>Offer: $%{y:,.2f}<br>Qty: %{customdata[1]:,.0f}<br>%{x|%Y-%m-%d}<extra></extra>",
                        ))

                    _chart(fig_price, height=400)
                    fig_price.update_layout(
                        legend=dict(orientation="h", y=1.10),
                        xaxis=dict(title=""),
                        yaxis=dict(title="Price", tickprefix="$", tickformat=",.0f"),
                        margin=dict(b=60, t=40),
                    )
                    st.plotly_chart(fig_price, use_container_width=True)
                else:
                    st.caption("No pricing data available for this MPN.")

                st.markdown("")
                m_left, m_right = st.columns(2)

                with m_left:
                    st.markdown('<div class="rb-config-title">Monthly Demand Trend</div>', unsafe_allow_html=True)
                    if not df_trend.empty:
                        st.plotly_chart(_dd_line(df_trend, "Month", ["RequestCount"], ["Requests"]), use_container_width=True)
                    else:
                        st.caption("No trend data.")

                with m_right:
                    st.markdown('<div class="rb-config-title">Sourcing Status Breakdown</div>', unsafe_allow_html=True)
                    if not df_sourcing.empty:
                        status_colors = [STATUS_COLORS.get(r, "#cbd5e1") for r in df_sourcing["Label"]]
                        st.plotly_chart(_dd_donut(df_sourcing, "Count", "Label", status_colors), use_container_width=True)
                    else:
                        st.caption("No sourcing data.")

                st.markdown('<div class="rb-config-title" style="margin-top:8px;">Top Customers</div>', unsafe_allow_html=True)
                if not df_customers.empty:
                    st.plotly_chart(
                        _dd_hbar(df_customers, "RequestCount", "CustomerName",
                                 height=max(280, len(df_customers) * 28 + 40)),
                        use_container_width=True,
                    )
                else:
                    st.caption("No customer data.")

                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Supplier Intelligence</div>', unsafe_allow_html=True)

                si_left, si_right = st.columns(2)
                with si_left:
                    st.markdown('<div class="rb-config-title">Top Suppliers Quoting this MPN</div>', unsafe_allow_html=True)
                    if not df_suppliers.empty:
                        st.plotly_chart(_dd_hbar(df_suppliers, "SrfqCount", "Supplier",
                                                 height=max(280, len(df_suppliers) * 28 + 40)), use_container_width=True)
                    else:
                        st.caption("No supplier data.")

                with si_right:
                    st.markdown('<div class="rb-config-title">Manufacturers for this MPN</div>', unsafe_allow_html=True)
                    if not df_mfrs.empty:
                        st.plotly_chart(_dd_hbar(df_mfrs, "SrfqCount", "Manufacturer",
                                                 height=max(280, len(df_mfrs) * 28 + 40)), use_container_width=True)
                    else:
                        st.caption("No manufacturer data.")

                if not df_reps.empty:
                    st.markdown('<div class="rb-config-title" style="margin-top:8px;">Sales Reps Involved</div>', unsafe_allow_html=True)
                    st.dataframe(
                        df_reps.rename(columns={"SalesRep": "Sales Rep", "PartCount": "Part Requests", "TotalQty": "Total Qty"}),
                        use_container_width=True, hide_index=True,
                    )

                # ── Part History Table ────────────────────────────────────────
                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Part History</div>', unsafe_allow_html=True)
                if not df_history.empty:
                    df_hist_display = df_history.copy()
                    df_hist_display["Date"] = pd.to_datetime(df_hist_display["Date"]).dt.strftime("%Y-%m-%d")
                    df_hist_display["Price"] = pd.to_numeric(df_hist_display["Price"], errors="coerce")
                    df_hist_display["Qty"] = pd.to_numeric(df_hist_display["Qty"], errors="coerce").fillna(0).astype(int)
                    st.markdown(f"**{len(df_hist_display)}** records across {df_hist_display['Source'].nunique()} source types")
                    st.dataframe(
                        df_hist_display[["Source", "RefId", "Date", "Mpn", "Qty", "Price", "Status", "Contact", "SourceName"]]
                        .rename(columns={"RefId": "Ref ID", "SourceName": "Source Name"}),
                        use_container_width=True, hide_index=True, height=400,
                    )
                else:
                    st.caption("No part history records found.")

        # ── Manufacturer profile ───────────────────────────────────────────────
        elif selected_entity != placeholder and srch_type == "Manufacturer":
            with st.spinner(f"Loading profile for {selected_entity}…"):
                kpis           = deepdive_data.get_mfr_kpis(selected_entity, start_str, end_str)
                df_trend       = deepdive_data.get_mfr_monthly_trend(selected_entity, start_str, end_str)
                df_sourcing    = deepdive_data.get_mfr_sourcing_breakdown(selected_entity, start_str, end_str)
                df_best_supps  = deepdive_data.get_mfr_best_suppliers(selected_entity, start_str, end_str)
                df_mpns        = deepdive_data.get_mfr_top_mpns(selected_entity, start_str, end_str)
                df_customers   = deepdive_data.get_mfr_top_customers(selected_entity, start_str, end_str)
                df_reps        = deepdive_data.get_mfr_sales_reps(selected_entity, start_str, end_str)

            if not kpis:
                st.info("No data found for this manufacturer in the selected date range.")
            else:
                st.markdown(
                    f'<p class="rb-result-title">{selected_entity}</p>',
                    unsafe_allow_html=True,
                )

                k1, k2, k3, k4, k5, k6 = st.columns(6)
                k1.metric("Part Requests", f"{kpis['total_parts']:,}")
                k2.metric("Unique MPNs", f"{kpis['unique_mpns']:,}")
                k3.metric("Customers", f"{kpis['unique_customers']:,}")
                k4.metric("Sourced Rate", f"{kpis['sourced_rate']}%")
                k5.metric("Quote Rate", f"{kpis['quote_rate']}%")
                k6.metric("Win Rate", f"{kpis['win_rate']}%")

                # ── Best Suppliers (core insight) ─────────────────────────────
                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Best Suppliers for This Brand</div>', unsafe_allow_html=True)
                st.markdown('<p class="rb-result-meta">Ranked by wins, response rate, and volume — use this to decide who to source from.</p>', unsafe_allow_html=True)
                if not df_best_supps.empty:
                    df_supp_display = df_best_supps.copy()
                    df_supp_display["AvgCost"] = pd.to_numeric(df_supp_display["AvgCost"], errors="coerce")
                    df_supp_display["AvgCost"] = df_supp_display["AvgCost"].apply(
                        lambda x: f"${x:,.2f}" if pd.notna(x) else "—"
                    )
                    df_supp_display["ResponseRate"] = df_supp_display["ResponseRate"].apply(lambda x: f"{x}%" if pd.notna(x) else "—")
                    df_supp_display["WinRate"] = df_supp_display["WinRate"].apply(lambda x: f"{x}%" if pd.notna(x) else "—")
                    st.dataframe(
                        df_supp_display.rename(columns={
                            "TotalSRFQs": "SRFQs", "ResponseRate": "Response %",
                            "WinRate": "Win %", "AvgCost": "Avg Cost",
                        }),
                        use_container_width=True, hide_index=True,
                    )
                else:
                    st.caption("No supplier data for this manufacturer.")

                st.markdown("")
                m_left, m_right = st.columns(2)

                with m_left:
                    st.markdown('<div class="rb-config-title">Monthly Demand Trend</div>', unsafe_allow_html=True)
                    if not df_trend.empty:
                        st.plotly_chart(_dd_line(df_trend, "Month", ["RequestCount"], ["Requests"]), use_container_width=True)
                    else:
                        st.caption("No trend data.")

                with m_right:
                    st.markdown('<div class="rb-config-title">Sourcing Status Breakdown</div>', unsafe_allow_html=True)
                    if not df_sourcing.empty:
                        status_colors = [STATUS_COLORS.get(r, "#cbd5e1") for r in df_sourcing["Label"]]
                        st.plotly_chart(_dd_donut(df_sourcing, "Count", "Label", status_colors), use_container_width=True)
                    else:
                        st.caption("No sourcing data.")

                # ── Top MPNs ──────────────────────────────────────────────────
                st.markdown("---")
                st.markdown('<div class="rb-config-title" style="font-size:15px;font-weight:600;margin-bottom:4px;">Top Part Numbers</div>', unsafe_allow_html=True)
                if not df_mpns.empty:
                    st.plotly_chart(
                        _dd_hbar(df_mpns, "RequestCount", "Mpn",
                                 height=max(280, len(df_mpns) * 28 + 40)),
                        use_container_width=True,
                    )
                else:
                    st.caption("No MPN data.")

                # ── Top Customers ─────────────────────────────────────────────
                st.markdown('<div class="rb-config-title" style="margin-top:8px;">Top Customers Requesting This Brand</div>', unsafe_allow_html=True)
                if not df_customers.empty:
                    st.plotly_chart(
                        _dd_hbar(df_customers, "RequestCount", "CustomerName",
                                 height=max(280, len(df_customers) * 28 + 40)),
                        use_container_width=True,
                    )
                else:
                    st.caption("No customer data.")

                # ── Sales Reps ────────────────────────────────────────────────
                if not df_reps.empty:
                    st.markdown('<div class="rb-config-title" style="margin-top:8px;">Sales Rep Activity</div>', unsafe_allow_html=True)
                    st.dataframe(
                        df_reps.rename(columns={
                            "SalesRep": "Sales Rep", "PartCount": "Part Requests",
                            "Won": "Won", "TotalQty": "Total Qty",
                        }),
                        use_container_width=True, hide_index=True,
                    )

    elif srch_clicked and not dd_results:
        st.markdown("""
        <div class="rb-empty-state">
            <div class="icon">🔍</div>
            <div class="title">No matches found</div>
            <div class="sub">Try a different search term.</div>
        </div>
        """, unsafe_allow_html=True)

    elif not dd_results:
        st.markdown("""
        <div class="rb-empty-state">
            <div class="icon">🔎</div>
            <div class="title">Search for an entity</div>
            <div class="sub">Type a customer name, supplier name, MPN, or manufacturer above and click <strong>Search</strong>.</div>
        </div>
        """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  ASK AI
# ══════════════════════════════════════════════════════════════════════════════
with tab_ai:
    # ── Header ──────────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="ai-page-header">
      <div class="ai-header-icon">✦</div>
      <div>
        <div class="ai-header-title">Ask Jerzy</div>
        <div class="ai-header-sub">Full database · All-time data · MPN knowledge built-in</div>
      </div>
      <div class="ai-header-badge">● No date limits</div>
    </div>
    """, unsafe_allow_html=True)

    # ── State ────────────────────────────────────────────────────────────────────
    if "ai_messages" not in st.session_state:
        st.session_state.ai_messages = []

    # ── Conversation or empty state ──────────────────────────────────────────────
    if not st.session_state.ai_messages:
        st.markdown("""
        <div class="ai-empty-state">
          <span class="ai-empty-icon">✦</span>
          <div class="ai-empty-title">Your data analyst, Jerzy</div>
          <div class="ai-empty-sub">
            Ask anything — customers, suppliers, MPNs, win rates, trends, all-time history.
            No filters, no date limits.
          </div>
          <div class="ai-prompt-chips">
            <span class="ai-chip">Who are our top customers all time?</span>
            <span class="ai-chip">What's our overall win rate?</span>
            <span class="ai-chip">Top 10 MPNs by sourcing volume</span>
            <span class="ai-chip">Tell me about MPN LM358</span>
          </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for msg in st.session_state.ai_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # ── Input ────────────────────────────────────────────────────────────────────
    if prompt := st.chat_input("Ask Jerzy anything about your data…", key="ai_input"):
        st.session_state.ai_messages.append({"role": "user", "content": prompt})
        with st.spinner("Thinking…"):
            try:
                answer = chat_agent.ask_data(st.session_state.ai_messages)
            except Exception as exc:
                answer = f"⚠️ Error: {exc}"
        st.session_state.ai_messages.append({"role": "assistant", "content": answer})
        st.rerun()

    # ── Clear button ─────────────────────────────────────────────────────────────
    if st.session_state.ai_messages:
        c1, c2, c3 = st.columns([5, 2, 5])
        with c2:
            if st.button("↺  Clear chat", key="ai_clear", use_container_width=True):
                st.session_state.ai_messages = []
                st.rerun()

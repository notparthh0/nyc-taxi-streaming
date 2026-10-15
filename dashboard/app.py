import json
import os

import pandas as pd
# pyrefly: ignore [missing-import]
import streamlit as st

st.set_page_config(
    page_title="NYC Taxi | Stream Monitor",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
METRICS_FILE = os.path.join(DATA_DIR, "metrics.json")
ANOMALIES_FILE = os.path.join(DATA_DIR, "anomalies.json")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&display=swap');
:root { --page:#101211; --panel:#171a18; --border:#303631; --text:#f0f2ed; --muted:#a0a9a0; --yellow:#f2c94c; --green:#a7d7a1; --red:#ff8c78; }
.stApp { background:var(--page); color:var(--text); }
[data-testid="stHeader"] { background:rgba(16,18,17,.92); }
.block-container { width:100%; max-width:none; padding:1.6rem clamp(1.1rem, 3vw, 3rem) 3rem; }
html, body, [class*="css"] { font-family:'DM Sans',sans-serif; }
h1,h2,h3 { color:var(--text)!important; letter-spacing:-.035em; }
h1 { font-size:clamp(2rem,3.1vw,3rem)!important; font-weight:700!important; line-height:1.08!important; }
h2 { font-size:1.25rem!important; font-weight:600!important; }
p,label,.stCaption { color:var(--muted); }
code,.mono { font-family:'DM Mono',monospace; }
.eyebrow { color:var(--yellow)!important; text-transform:uppercase; letter-spacing:.13em; font:500 .72rem 'DM Mono',monospace; line-height:1.5; margin:0 0 .8rem 0; padding:0; display:block; text-decoration:none!important; border:0!important; background:none!important; position:static; }\n.eyebrow::before,.eyebrow::after { content:none!important; display:none!important; }
.subtitle { color:var(--muted); font-size:.98rem; margin-top:-.6rem; }
.status { display:inline-flex; align-items:center; gap:.55rem; border:1px solid var(--border); border-radius:5px; padding:.48rem .7rem; color:var(--muted); font:500 .75rem 'DM Mono',monospace; background:var(--panel); }
.status-dot { width:7px; height:7px; border-radius:50%; display:inline-block; background:var(--green); }
.metric-card { box-sizing:border-box; width:100%; background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:1.1rem 1.15rem 1rem; min-height:142px; }
.metric-label { color:var(--muted); font-size:.82rem; margin-bottom:.9rem; }
.metric-value { color:var(--text); font-size:clamp(1.35rem,2vw,2.2rem); line-height:1.15; font-weight:700; letter-spacing:-.035em; overflow-wrap:anywhere; }
.metric-foot { color:var(--muted); font:.72rem 'DM Mono',monospace; margin-top:.8rem; }
.section-kicker { color:var(--muted); font:.7rem 'DM Mono',monospace; text-transform:uppercase; letter-spacing:.13em; }
.panel-title { font-size:1.08rem; font-weight:600; color:var(--text); margin:.2rem 0 .25rem; }
.panel-desc { font-size:.84rem; color:var(--muted); margin-bottom:1rem; }
.empty { padding:2rem 1rem; text-align:center; color:var(--muted); border:1px dashed var(--border); border-radius:6px; font-size:.9rem; }
.table-note { color:var(--muted); font-size:.78rem; margin-top:.6rem; }
div[data-testid="stDataFrame"] { border:1px solid var(--border); border-radius:6px; overflow:hidden; }
div[data-testid="stAlert"] { border-radius:6px; }
</style>
""", unsafe_allow_html=True)

def load_json_safe(filepath: str, default):
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default

def fmt_number(value):
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "—"

def metric_card(label, value, footnote):
    st.markdown(f"""
    <div class="metric-card">
      <div class="metric-label">{label}</div>
      <div class="metric-value">{value}</div>
      <div class="metric-foot">{footnote}</div>
    </div>""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Monitor settings")
    refresh_seconds = st.select_slider("Refresh interval", options=[5, 10, 15, 30, 60], value=10, format_func=lambda x: f"{x} seconds")
    st.caption("Reads the latest local JSON snapshots.")

header_left, header_right = st.columns([4, 1])
with header_left:
    st.markdown('<div class="eyebrow">Mobility intelligence / operations</div>', unsafe_allow_html=True)
    st.title("Taxi stream monitor")
    st.markdown('<div class="subtitle">Five-minute demand windows, zone-level outliers, and generated context.</div>', unsafe_allow_html=True)
with header_right:
    st.markdown('<div style="display:flex;justify-content:flex-end;padding-top:1.1rem"><span class="status"><span class="status-dot"></span> SNAPSHOT FILES</span></div>', unsafe_allow_html=True)

st.markdown("<div style='height:1.25rem'></div>", unsafe_allow_html=True)

metrics = load_json_safe(METRICS_FILE, {
    "total_records_processed": 0,
    "anomalies_detected": 0,
    "last_window_time": "Waiting for stream",
    "top_zones": [],
})
anomalies = load_json_safe(ANOMALIES_FILE, [])
if not isinstance(metrics, dict):
    metrics = {}
if not isinstance(anomalies, list):
    anomalies = []

st.markdown('<div class="section-kicker">Current snapshot</div>', unsafe_allow_html=True)
m1, m2, m3 = st.columns([1, 1, 1.3], gap="medium")
with m1:
    metric_card("Records processed", fmt_number(metrics.get("total_records_processed")), "CUMULATIVE")
with m2:
    metric_card("Anomalies detected", fmt_number(metrics.get("anomalies_detected")), "CUMULATIVE")
with m3:
    metric_card("Last completed window", str(metrics.get("last_window_time") or "Waiting for stream"), "LATEST WINDOW")

st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
left, right = st.columns([1.05, 1.5], gap="large")

with left:
    st.markdown('<div class="section-kicker">01 / Zone activity</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Highest trip volume</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-desc">Top zones in the latest completed five-minute window.</div>', unsafe_allow_html=True)
    top_zones = metrics.get("top_zones", [])
    if isinstance(top_zones, list) and top_zones:
        df_top = pd.DataFrame(top_zones)
        if {"zone", "trips"}.issubset(df_top.columns):
            df_top = df_top[["zone", "trips"]].copy()
            df_top["zone"] = df_top["zone"].astype(str).map(lambda x: f"Zone {x}")
            df_top["trips"] = pd.to_numeric(df_top["trips"], errors="coerce")
            df_top = df_top.dropna(subset=["trips"]).sort_values("trips", ascending=True).tail(10)
            if not df_top.empty:
                st.bar_chart(df_top.set_index("zone"), horizontal=True, height=350, color="#f2c94c")
            else:
                st.markdown('<div class="empty">Zone records were found, but trip counts were not valid numbers.</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="empty">Zone snapshot needs <code>zone</code> and <code>trips</code> fields.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="empty">Waiting for the first completed five-minute window.</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="section-kicker">02 / Anomaly review</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Recent demand anomalies</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-desc">Observed counts compared with the zone baseline, with generated explanations.</div>', unsafe_allow_html=True)
    if anomalies:
        df_anomalies = pd.DataFrame(anomalies)
        display_cols = {"zone_id":"Zone", "window":"Time window", "observed_count":"Observed", "baseline_mean":"Baseline", "explanation":"Explanation"}
        available_cols = [col for col in display_cols if col in df_anomalies.columns]
        if available_cols:
            df_display = df_anomalies[available_cols].rename(columns=display_cols).copy()
            for col in ["Observed", "Baseline"]:
                if col in df_display:
                    df_display[col] = pd.to_numeric(df_display[col], errors="coerce")
            st.dataframe(df_display, use_container_width=True, hide_index=True, height=350, column_config={
                "Explanation": st.column_config.TextColumn("Explanation", width="large"),
                "Time window": st.column_config.TextColumn("Time window", width="medium"),
            })
        else:
            st.markdown('<div class="empty">Anomaly records do not contain the expected display fields.</div>', unsafe_allow_html=True)
        st.markdown('<div class="table-note">Generated explanations are context, not a substitute for reviewing the observed counts.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="empty">No anomalies recorded yet. Detection requires at least three completed windows of history per zone.</div>', unsafe_allow_html=True)

st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
st.markdown('<div class="section-kicker">03 / Feed status</div>', unsafe_allow_html=True)
s1, s2 = st.columns([1, 2], gap="medium")
with s1:
    st.markdown('<div class="panel-title">Snapshot files</div>', unsafe_allow_html=True)
    for label, path in [("Metrics", METRICS_FILE), ("Anomalies", ANOMALIES_FILE)]:
        exists = os.path.exists(path)
        color = "#a7d7a1" if exists else "#ff8c78"
        state = "AVAILABLE" if exists else "MISSING"
        st.markdown(f'<div style="display:flex;justify-content:space-between;gap:1rem;border-top:1px solid #303631;padding:.7rem 0"><span style="color:#c6cec6">{label}</span><span style="color:{color};font:500 .72rem DM Mono,monospace">{state}</span></div>', unsafe_allow_html=True)
with s2:
    st.markdown('<div class="panel-title">Operational notes</div>', unsafe_allow_html=True)
    st.markdown("""
    - This view polls local JSON snapshots; it does not connect directly to Kafka.
    - The refresh interval controls how often the page reloads.
    - Missing or malformed JSON files fall back to an empty state.
    """)

st.markdown('<div style="border-top:1px solid #303631;margin-top:2rem;padding-top:1rem;color:#737d74;font:.72rem DM Mono,monospace;display:flex;justify-content:space-between;gap:1rem;flex-wrap:wrap"><span>NYC TAXI / STREAM MONITOR</span><span>LOCAL SNAPSHOT VIEW · NO SYNTHETIC METRICS</span></div>', unsafe_allow_html=True)

try:
    # pyrefly: ignore [missing-import]
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=refresh_seconds * 1000, key="stream_monitor_refresh")
except ImportError:
    st.caption("For automatic refresh, install `streamlit-autorefresh` with `pip install streamlit-autorefresh`.")

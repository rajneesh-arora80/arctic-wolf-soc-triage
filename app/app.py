import streamlit as st
import pandas as pd
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
import os
import uuid
from datetime import datetime

st.set_page_config(page_title="🐺 Arctic Wolf SOC Triage", page_icon="🐺", layout="wide")

CATALOG = "rajarora_febar"
SCHEMA = "soc_alert_triage"

@st.cache_resource
def get_connection():
    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    if not warehouses:
        st.error("No SQL warehouses available.")
        return None
    connection = dbsql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST", w.config.host),
        http_path=f"/sql/1.0/warehouses/{warehouses[0].id}",
        credentials_provider=lambda: w.config._header_factory()
    )
    return connection

@st.cache_data(ttl=60)
def run_query(query):
    conn = get_connection()
    if conn is None: return pd.DataFrame()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

st.sidebar.title("🐺 Arctic Wolf SOC")
st.sidebar.markdown("### Alert Filters")
risk_filter = st.sidebar.multiselect("Risk Tier", ["critical","high","medium","low"], default=["critical","high"])
source_filter = st.sidebar.multiselect("Source Type", ["firewall","edr","auth","email","cloud_audit"], default=["firewall","edr","auth","email","cloud_audit"])
limit = st.sidebar.slider("Max Alerts", 10, 200, 50)

st.title("🐺 Arctic Wolf SOC Alert Triage Workbench")
st.markdown("**Intelligent alert prioritization powered by Databricks**")

col1, col2, col3, col4 = st.columns(4)
try:
    kpis = run_query(f"SELECT COUNT(*) as total_alerts, SUM(CASE WHEN risk_tier IN ('critical','high') THEN 1 ELSE 0 END) as high_risk, SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_matches, ROUND(AVG(composite_risk_score),2) as avg_risk FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts")
    if not kpis.empty:
        col1.metric("Total Alerts", f"{int(kpis['total_alerts'].iloc[0]):,}")
        col2.metric("High Risk", f"{int(kpis['high_risk'].iloc[0]):,}", delta="Needs attention")
        col3.metric("Threat Intel Matches", f"{int(kpis['threat_matches'].iloc[0]):,}")
        col4.metric("Avg Risk Score", f"{kpis['avg_risk'].iloc[0]:.2f}")
except Exception as e:
    st.warning(f"Could not load KPIs: {e}")

st.divider()
st.subheader("📋 Alert Queue — Prioritized by Risk")
risk_list = "','".join(risk_filter)
source_list = "','".join(source_filter)
try:
    alerts = run_query(f"""SELECT e.event_id, e.event_timestamp, e.source_type, e.customer_name, e.risk_tier, e.composite_risk_score, e.severity_score, e.threat_intel_match, e.src_ip, e.alert_type, p.ml_predicted_severity, p.ml_confidence FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts e LEFT JOIN {CATALOG}.{SCHEMA}.gold_alert_predictions p ON e.event_id = p.event_id WHERE e.risk_tier IN ('{risk_list}') AND e.source_type IN ('{source_list}') ORDER BY e.composite_risk_score DESC LIMIT {limit}""")
    if not alerts.empty:
        st.dataframe(alerts[['event_timestamp','source_type','customer_name','risk_tier','composite_risk_score','ml_predicted_severity','ml_confidence','threat_intel_match','src_ip','alert_type']], use_container_width=True, height=400)
        st.markdown(f"*Showing {len(alerts)} alerts sorted by risk score*")
    else:
        st.info("No alerts match the current filters.")
except Exception as e:
    st.error(f"Could not load alerts: {e}")

st.divider()
st.subheader("🔍 Alert Detail & Triage")
if 'alerts' in dir() and not alerts.empty:
    selected = st.selectbox("Select alert", alerts['event_id'].tolist(), format_func=lambda x: f"{alerts[alerts['event_id']==x]['source_type'].iloc[0]} | {alerts[alerts['event_id']==x]['customer_name'].iloc[0]} | Risk: {alerts[alerts['event_id']==x]['composite_risk_score'].iloc[0]}")
    if selected:
        detail = alerts[alerts['event_id']==selected].iloc[0]
        cl, cr = st.columns(2)
        with cl:
            st.markdown("### Alert Context")
            st.json({"Event ID": detail['event_id'], "Timestamp": str(detail['event_timestamp']), "Source": detail['source_type'], "Customer": detail['customer_name'], "Risk Tier": detail['risk_tier'], "Risk Score": float(detail['composite_risk_score']), "Threat Intel Match": bool(detail['threat_intel_match']), "Source IP": str(detail.get('src_ip','N/A')), "ML Prediction": str(detail.get('ml_predicted_severity','N/A'))})
        with cr:
            st.markdown("### ⚡ Triage Actions")
            ca, cb = st.columns(2)
            with ca:
                if st.button("🔴 Escalate", use_container_width=True): st.success("Escalated to Tier 2")
                if st.button("🟡 Investigate", use_container_width=True): st.info("Marked for investigation")
            with cb:
                if st.button("🟢 Suppress", use_container_width=True): st.success("Suppressed")
                if st.button("⚪ False Positive", use_container_width=True): st.success("Marked as FP")
            st.text_area("Analyst Notes", placeholder="Add investigation notes...")

st.divider()
st.subheader("📊 Quick Analytics")
tab1, tab2 = st.tabs(["Risk Distribution", "Source Breakdown"])
with tab1:
    try:
        rd = run_query(f"SELECT risk_tier, COUNT(*) as count FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts GROUP BY risk_tier ORDER BY count DESC")
        if not rd.empty: st.bar_chart(rd.set_index('risk_tier'))
    except: pass
with tab2:
    try:
        sd = run_query(f"SELECT source_type, COUNT(*) as count FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts GROUP BY source_type ORDER BY count DESC")
        if not sd.empty: st.bar_chart(sd.set_index('source_type'))
    except: pass

st.divider()
st.caption("🐺 Arctic Wolf SOC Alert Triage — Powered by Databricks | Lakeflow → UC → ML/GenAI → Lakebase → DBSQL → Genie → App")
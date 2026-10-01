import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="🐺 Arctic Wolf SOC Triage", page_icon="🐺", layout="wide")

CATALOG = "rajarora_febar"
SCHEMA = "soc_alert_triage"

def get_connection():
    from databricks import sql as dbsql
    connection = dbsql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH", "/sql/1.0/warehouses/auto"),
        credentials_provider="databricks-oauth"
    )
    return connection

def run_query(query):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

st.title("🐺 Arctic Wolf SOC Alert Triage Workbench")
st.markdown("**Intelligent alert prioritization powered by Databricks**")

# Sidebar
st.sidebar.title("🐺 Arctic Wolf SOC")
risk_filter = st.sidebar.multiselect("Risk Tier", ["critical","high","medium","low"], default=["critical","high"])
source_filter = st.sidebar.multiselect("Source Type", ["firewall","edr","auth","email","cloud_audit"], default=["firewall","edr","auth","email","cloud_audit"])

# KPIs
try:
    kpis = run_query(f"""
        SELECT COUNT(*) as total_alerts,
               SUM(CASE WHEN risk_tier IN ('critical','high') THEN 1 ELSE 0 END) as high_risk,
               SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_matches,
               ROUND(AVG(composite_risk_score),2) as avg_risk
        FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts
    """)
    if not kpis.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Alerts", f"{int(kpis['total_alerts'].iloc[0]):,}")
        c2.metric("High Risk", f"{int(kpis['high_risk'].iloc[0]):,}")
        c3.metric("Threat Intel Matches", f"{int(kpis['threat_matches'].iloc[0]):,}")
        c4.metric("Avg Risk Score", f"{kpis['avg_risk'].iloc[0]}")
except Exception as e:
    st.warning(f"Could not load KPIs: {e}")

st.divider()

# Alert Queue
st.subheader("📋 Alert Queue — Prioritized by Risk")
risk_list = "','".join(risk_filter)
source_list = "','".join(source_filter)
try:
    alerts = run_query(f"""
        SELECT event_id, event_timestamp, source_type, customer_name, risk_tier,
               composite_risk_score, threat_intel_match, src_ip, alert_type
        FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts
        WHERE risk_tier IN ('{risk_list}') AND source_type IN ('{source_list}')
        ORDER BY composite_risk_score DESC LIMIT 50
    """)
    if not alerts.empty:
        st.dataframe(alerts, use_container_width=True, height=400)
except Exception as e:
    st.error(f"Could not load alerts: {e}")

st.divider()

# Triage Actions
st.subheader("⚡ Triage Actions")
c1, c2, c3, c4 = st.columns(4)
if c1.button("🔴 Escalate", use_container_width=True): st.success("Alert escalated to Tier 2")
if c2.button("🟡 Investigate", use_container_width=True): st.info("Marked for investigation")
if c3.button("🟢 Suppress", use_container_width=True): st.success("Alert suppressed")
if c4.button("⚪ False Positive", use_container_width=True): st.success("Marked as false positive")

st.divider()

# Analytics
st.subheader("📊 Quick Analytics")
tab1, tab2 = st.tabs(["Risk Distribution", "Source Breakdown"])
with tab1:
    try:
        rd = run_query(f"SELECT risk_tier, COUNT(*) as count FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts GROUP BY risk_tier")
        if not rd.empty: st.bar_chart(rd.set_index('risk_tier'))
    except: pass
with tab2:
    try:
        sd = run_query(f"SELECT source_type, COUNT(*) as count FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts GROUP BY source_type")
        if not sd.empty: st.bar_chart(sd.set_index('source_type'))
    except: pass

st.caption("🐺 Arctic Wolf SOC — Lakeflow → UC → ML/GenAI → Lakebase → DBSQL → Genie → App")
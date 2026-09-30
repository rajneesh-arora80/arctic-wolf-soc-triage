import streamlit as st
import pandas as pd
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
import os
import uuid
from datetime import datetime

# ============================================================
# Arctic Wolf SOC Alert Triage — Analyst Workbench
# ============================================================
# This Streamlit app is deployed as a Databricks App.
# It reads enriched alerts from Lakebase (or DBSQL) and
# allows analysts to triage alerts with one-click actions.
# ============================================================

st.set_page_config(
    page_title="🐺 Arctic Wolf SOC Triage",
    page_icon="🐺",
    layout="wide"
)

# Configuration
CATALOG = "rajneesh_febar"
SCHEMA = "soc_alert_triage"

# ============================================================
# Database Connection
# ============================================================
@st.cache_resource
def get_connection():
    """Connect to Databricks SQL warehouse."""
    # When running as a Databricks App, use the workspace client
    w = WorkspaceClient()
    
    # Get the first available SQL warehouse
    warehouses = list(w.warehouses.list())
    if not warehouses:
        st.error("No SQL warehouses available. Please create one.")
        return None
    
    warehouse = warehouses[0]
    
    connection = dbsql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST", w.config.host),
        http_path=f"/sql/1.0/warehouses/{warehouse.id}",
        credentials_provider=lambda: w.config._header_factory()
    )
    return connection

@st.cache_data(ttl=60)
def run_query(query):
    """Execute a SQL query and return results as a DataFrame."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

# ============================================================
# Sidebar — Filters
# ============================================================
st.sidebar.title("🐺 Arctic Wolf SOC")
st.sidebar.markdown("### Alert Filters")

risk_filter = st.sidebar.multiselect(
    "Risk Tier",
    ["critical", "high", "medium", "low"],
    default=["critical", "high"]
)

source_filter = st.sidebar.multiselect(
    "Source Type",
    ["firewall", "edr", "auth", "email", "cloud_audit"],
    default=["firewall", "edr", "auth", "email", "cloud_audit"]
)

limit = st.sidebar.slider("Max Alerts", 10, 200, 50)

# ============================================================
# Main Content
# ============================================================
st.title("🐺 Arctic Wolf SOC Alert Triage Workbench")
st.markdown("**Intelligent alert prioritization powered by Databricks**")

# ============================================================
# KPI Cards
# ============================================================
col1, col2, col3, col4 = st.columns(4)

kpi_query = f"""
    SELECT 
        COUNT(*) as total_alerts,
        SUM(CASE WHEN risk_tier IN ('critical', 'high') THEN 1 ELSE 0 END) as high_risk,
        SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_matches,
        ROUND(AVG(composite_risk_score), 2) as avg_risk
    FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts
"""

try:
    kpis = run_query(kpi_query)
    if not kpis.empty:
        col1.metric("Total Alerts", f"{int(kpis['total_alerts'].iloc[0]):,}")
        col2.metric("High Risk", f"{int(kpis['high_risk'].iloc[0]):,}", delta="Needs attention")
        col3.metric("Threat Intel Matches", f"{int(kpis['threat_matches'].iloc[0]):,}")
        col4.metric("Avg Risk Score", f"{kpis['avg_risk'].iloc[0]:.2f}")
except Exception as e:
    st.warning(f"Could not load KPIs: {e}")

st.divider()

# ============================================================
# Alert Queue — Prioritized by ML + Risk Score
# ============================================================
st.subheader("📋 Alert Queue — Prioritized by Risk")

risk_list = "', '".join(risk_filter)
source_list = "', '".join(source_filter)

alert_query = f"""
    SELECT 
        e.event_id,
        e.event_timestamp,
        e.source_type,
        e.customer_name,
        e.risk_tier,
        e.composite_risk_score,
        e.severity_score,
        e.threat_intel_match,
        e.threat_category,
        e.src_ip,
        e.username,
        e.alert_type,
        e.mitre_tactic,
        e.criticality,
        e.department,
        p.ml_predicted_severity,
        p.ml_confidence
    FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts e
    LEFT JOIN {CATALOG}.{SCHEMA}.gold_alert_predictions p ON e.event_id = p.event_id
    WHERE e.risk_tier IN ('{risk_list}')
    AND e.source_type IN ('{source_list}')
    ORDER BY e.composite_risk_score DESC
    LIMIT {limit}
"""

try:
    alerts = run_query(alert_query)
    
    if not alerts.empty:
        # Color-code risk tiers
        def highlight_risk(row):
            colors = {
                'critical': 'background-color: #FF362180',
                'high': 'background-color: #FFAB0040',
                'medium': 'background-color: #077A9D20',
                'low': 'background-color: #00A97220'
            }
            return [colors.get(row['risk_tier'], '')] * len(row)
        
        st.dataframe(
            alerts[['event_timestamp', 'source_type', 'customer_name', 'risk_tier', 
                    'composite_risk_score', 'ml_predicted_severity', 'ml_confidence',
                    'threat_intel_match', 'src_ip', 'alert_type']].style.apply(highlight_risk, axis=1),
            use_container_width=True,
            height=400
        )
        
        st.markdown(f"*Showing {len(alerts)} alerts sorted by risk score*")
    else:
        st.info("No alerts match the current filters.")
except Exception as e:
    st.error(f"Could not load alerts: {e}")

st.divider()

# ============================================================
# Alert Detail & Triage Actions
# ============================================================
st.subheader("🔍 Alert Detail & Triage")

if 'alerts' in dir() and not alerts.empty:
    selected_event = st.selectbox(
        "Select an alert to investigate",
        alerts['event_id'].tolist(),
        format_func=lambda x: f"{alerts[alerts['event_id']==x]['source_type'].iloc[0]} | {alerts[alerts['event_id']==x]['customer_name'].iloc[0]} | Risk: {alerts[alerts['event_id']==x]['composite_risk_score'].iloc[0]}"
    )
    
    if selected_event:
        alert_detail = alerts[alerts['event_id'] == selected_event].iloc[0]
        
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.markdown("### Alert Context")
            st.json({
                "Event ID": alert_detail['event_id'],
                "Timestamp": str(alert_detail['event_timestamp']),
                "Source": alert_detail['source_type'],
                "Customer": alert_detail['customer_name'],
                "Risk Tier": alert_detail['risk_tier'],
                "Risk Score": float(alert_detail['composite_risk_score']),
                "Severity": int(alert_detail['severity_score']),
                "Threat Intel Match": bool(alert_detail['threat_intel_match']),
                "Threat Category": str(alert_detail.get('threat_category', 'N/A')),
                "Source IP": str(alert_detail.get('src_ip', 'N/A')),
                "MITRE Tactic": str(alert_detail.get('mitre_tactic', 'N/A')),
                "ML Prediction": str(alert_detail.get('ml_predicted_severity', 'N/A')),
                "ML Confidence": float(alert_detail.get('ml_confidence', 0))
            })
        
        with col_right:
            st.markdown("### 🤖 AI Investigation Summary")
            try:
                summary_query = f"""
                    SELECT investigation_summary 
                    FROM {CATALOG}.{SCHEMA}.gold_investigation_summaries
                    WHERE event_id = '{selected_event}'
                """
                summary = run_query(summary_query)
                if not summary.empty:
                    st.info(summary['investigation_summary'].iloc[0])
                else:
                    st.warning("No AI summary available for this alert.")
            except:
                st.warning("Investigation summaries table not yet populated.")
            
            st.markdown("### ⚡ Triage Actions")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🔴 Escalate", use_container_width=True):
                    st.success(f"Alert {selected_event[:8]}... escalated to Tier 2")
                if st.button("🟡 Investigate", use_container_width=True):
                    st.info(f"Alert {selected_event[:8]}... marked for investigation")
            with col_b:
                if st.button("🟢 Suppress", use_container_width=True):
                    st.success(f"Alert {selected_event[:8]}... suppressed")
                if st.button("⚪ False Positive", use_container_width=True):
                    st.success(f"Alert {selected_event[:8]}... marked as false positive")
            
            notes = st.text_area("Analyst Notes", placeholder="Add investigation notes...")

st.divider()

# ============================================================
# Analytics Section
# ============================================================
st.subheader("📊 Quick Analytics")

tab1, tab2, tab3 = st.tabs(["Risk Distribution", "Source Breakdown", "Customer Risk"])

with tab1:
    try:
        risk_dist = run_query(f"""
            SELECT risk_tier, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts
            GROUP BY risk_tier
            ORDER BY count DESC
        """)
        if not risk_dist.empty:
            st.bar_chart(risk_dist.set_index('risk_tier'))
    except Exception as e:
        st.warning(f"Could not load risk distribution: {e}")

with tab2:
    try:
        source_dist = run_query(f"""
            SELECT source_type, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts
            GROUP BY source_type
            ORDER BY count DESC
        """)
        if not source_dist.empty:
            st.bar_chart(source_dist.set_index('source_type'))
    except Exception as e:
        st.warning(f"Could not load source breakdown: {e}")

with tab3:
    try:
        customer_risk = run_query(f"""
            SELECT customer_name, 
                   COUNT(*) as alerts,
                   ROUND(AVG(composite_risk_score), 2) as avg_risk
            FROM {CATALOG}.{SCHEMA}.gold_enriched_alerts
            WHERE risk_tier IN ('critical', 'high')
            GROUP BY customer_name
            ORDER BY alerts DESC
            LIMIT 10
        """)
        if not customer_risk.empty:
            st.dataframe(customer_risk, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not load customer risk: {e}")

# Footer
st.divider()
st.caption("🐺 Arctic Wolf SOC Alert Triage — Powered by Databricks | Lakeflow → Unity Catalog → ML/GenAI → Lakebase → DBSQL → Genie → App")

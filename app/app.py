import streamlit as st

st.set_page_config(page_title="🐺 Arctic Wolf SOC Triage", page_icon="🐺", layout="wide")

st.title("🐺 Arctic Wolf SOC Alert Triage Workbench")
st.markdown("**Intelligent alert prioritization powered by Databricks**")
st.success("✅ App is running!")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Alerts", "500,000")
c2.metric("High Risk", "25,432")
c3.metric("Threat Intel Matches", "1,247")
c4.metric("Avg Risk Score", "4.82")

st.divider()
st.subheader("⚡ Triage Actions")
col1, col2, col3, col4 = st.columns(4)
if col1.button("🔴 Escalate", use_container_width=True): st.success("Escalated")
if col2.button("🟡 Investigate", use_container_width=True): st.info("Investigating")
if col3.button("🟢 Suppress", use_container_width=True): st.success("Suppressed")
if col4.button("⚪ False Positive", use_container_width=True): st.success("Marked FP")

st.caption("🐺 Arctic Wolf SOC — Lakeflow → UC → ML/GenAI → Lakebase → DBSQL → Genie → App")

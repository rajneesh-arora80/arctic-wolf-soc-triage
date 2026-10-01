import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="🐺 Arctic Wolf SOC Triage", page_icon="🐺", layout="wide")

CATALOG = "rajarora_febar"
SCHEMA = "soc_alert_triage"

st.title("🐺 Arctic Wolf SOC Alert Triage Workbench")
st.markdown("**Intelligent alert prioritization powered by Databricks**")

# Debug info
st.write("**Debug Info:**")
st.write(f"- DATABRICKS_HOST: {os.getenv('DATABRICKS_HOST', 'NOT SET')}")
st.write(f"- HTTP_PATH: {os.getenv('DATABRICKS_SQL_WAREHOUSE_HTTP_PATH', 'NOT SET')}")
st.write(f"- TOKEN present: {bool(os.getenv('DATABRICKS_TOKEN'))}")

try:
    from databricks import sql as dbsql
    conn = dbsql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT 1 as test")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    st.success(f"✅ Database connected! Test query returned: {result[0]}")
except Exception as e:
    st.error(f"❌ Connection failed: {e}")

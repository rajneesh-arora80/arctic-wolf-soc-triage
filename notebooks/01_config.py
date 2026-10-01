# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Configuration — Arctic Wolf SOC Alert Triage
# MAGIC
# MAGIC Shared configuration for all notebooks. Update the catalog name below before running.

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# ========================================
# UPDATE THESE VALUES FOR YOUR ENVIRONMENT
# ========================================
CATALOG_NAME = "rajarora_febar"
SCHEMA_NAME = "soc_alert_triage"

# Derived paths
FULL_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_NAME}"

# Table names
TABLES = {
    "raw_security_events": f"{FULL_SCHEMA}.raw_security_events",
    "threat_intel_iocs": f"{FULL_SCHEMA}.threat_intel_iocs",
    "asset_inventory": f"{FULL_SCHEMA}.asset_inventory",
    "customers": f"{FULL_SCHEMA}.customers",
    "normalized_events": f"{FULL_SCHEMA}.silver_normalized_events",
    "enriched_alerts": f"{FULL_SCHEMA}.gold_enriched_alerts",
    "historical_triage": f"{FULL_SCHEMA}.historical_triage_decisions",
    "analyst_actions": f"{FULL_SCHEMA}.analyst_actions",
    "alert_predictions": f"{FULL_SCHEMA}.gold_alert_predictions",
    "investigation_summaries": f"{FULL_SCHEMA}.gold_investigation_summaries",
}

print(f"✅ Configuration loaded")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schema:  {SCHEMA_NAME}")
print(f"   Tables:  {len(TABLES)} tables configured")
# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Step 2: Lakeflow Ingestion — DLT Pipeline (Bronze → Silver → Gold)
# MAGIC
# MAGIC This notebook defines a **Lakeflow (Delta Live Tables) pipeline** that processes
# MAGIC raw security events through the medallion architecture:
# MAGIC
# MAGIC - **Bronze**: Raw events as-is (append-only, immutable)
# MAGIC - **Silver**: Parsed, deduplicated, OCSF-normalized events
# MAGIC - **Gold**: Enriched alerts with threat intel, asset context, and risk scores
# MAGIC
# MAGIC ### How to Run
# MAGIC 1. Create a new DLT Pipeline in your workspace
# MAGIC 2. Point it to this notebook
# MAGIC 3. Set the target schema to `rajarora_febar.soc_alert_triage`
# MAGIC 4. Run the pipeline
# MAGIC
# MAGIC **Alternative**: If you prefer to run this as a standard notebook (not DLT),
# MAGIC use the `02b_lakeflow_standard.py` notebook instead.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG_NAME = "rajarora_febar"
SCHEMA_NAME = "soc_alert_triage"
FULL_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_NAME}"

# COMMAND ----------

# DBTITLE 1,Bronze Layer — Raw Security Events (Append-Only)
@dlt.table(
    name="bronze_security_events",
    comment="Raw security events ingested from Arctic Wolf MDR telemetry sources. Immutable append-only layer.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_security_events():
    """Ingest raw security events from the source table."""
    return (
        spark.readStream
        .format("delta")
        .table(f"{FULL_SCHEMA}.raw_security_events")
    )

# COMMAND ----------

# DBTITLE 1,Silver Layer — Normalized & Parsed Events (OCSF-aligned)
@dlt.table(
    name="silver_normalized_events",
    comment="Parsed and normalized security events aligned to OCSF schema. Deduplicated and quality-checked.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_source", "source_type IN ('firewall', 'edr', 'auth', 'email', 'cloud_audit')")
@dlt.expect("valid_severity", "severity_score BETWEEN 1 AND 10")
def silver_normalized_events():
    """Parse raw payloads, normalize to OCSF-aligned schema, and deduplicate."""
    return (
        dlt.read_stream("bronze_security_events")
        .withColumn("parsed_payload", F.from_json(F.col("raw_payload"), MapType(StringType(), StringType())))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("event_hour", F.hour(F.to_timestamp(F.col("event_timestamp"))))
        .withColumn("is_business_hours", 
                     (F.col("event_hour").between(8, 17)) & 
                     (F.dayofweek(F.col("event_date")).between(2, 6)))
        # Extract common OCSF fields from parsed payload
        .withColumn("action", F.col("parsed_payload").getItem("action"))
        .withColumn("src_ip", F.coalesce(
            F.col("parsed_payload").getItem("src_ip"),
            F.col("parsed_payload").getItem("source_ip")
        ))
        .withColumn("dst_ip", F.col("parsed_payload").getItem("dst_ip"))
        .withColumn("username", F.coalesce(
            F.col("parsed_payload").getItem("username"),
            F.col("parsed_payload").getItem("principal")
        ))
        .withColumn("alert_type", F.col("parsed_payload").getItem("alert_type"))
        .withColumn("mitre_tactic", F.col("parsed_payload").getItem("mitre_tactic"))
        .withColumn("process_name", F.col("parsed_payload").getItem("process_name"))
        .withColumn("geo_location", F.col("parsed_payload").getItem("geo_location"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .drop("raw_payload", "parsed_payload")
        .dropDuplicates(["event_id"])
    )

# COMMAND ----------

# DBTITLE 1,Gold Layer — Enriched Alerts with Threat Intel & Risk Scoring
@dlt.table(
    name="gold_enriched_alerts",
    comment="Enriched security alerts with threat intelligence correlation, asset context, and composite risk scores. Ready for ML classification and analyst triage.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_enriched_alerts():
    """Enrich normalized events with threat intel, asset context, and risk scores."""
    events = dlt.read_stream("silver_normalized_events")
    
    # Load reference tables
    threat_intel = spark.table(f"{FULL_SCHEMA}.threat_intel_iocs").filter("is_active = true")
    assets = spark.table(f"{FULL_SCHEMA}.asset_inventory")
    customers = spark.table(f"{FULL_SCHEMA}.customers")
    
    # Threat intel enrichment — match source IPs against known-bad IOCs
    ip_iocs = threat_intel.filter("ioc_type = 'ip_address'").select(
        F.col("ioc_value").alias("matched_ioc"),
        F.col("threat_category"),
        F.col("confidence").alias("ioc_confidence")
    )
    
    enriched = (
        events
        # Join with threat intel on source IP
        .join(F.broadcast(ip_iocs), events.src_ip == ip_iocs.matched_ioc, "left")
        # Join with asset inventory
        .join(
            assets.select("asset_id", "asset_type", "criticality", "department", "os_type"),
            "asset_id", "left"
        )
        # Join with customer info
        .join(
            customers.select("customer_id", "customer_name", "industry", "tier", "region"),
            "customer_id", "left"
        )
        # Compute composite risk score
        .withColumn("threat_intel_match", F.when(F.col("matched_ioc").isNotNull(), True).otherwise(False))
        .withColumn("criticality_score", 
            F.when(F.col("criticality") == "critical", 4)
             .when(F.col("criticality") == "high", 3)
             .when(F.col("criticality") == "medium", 2)
             .otherwise(1))
        .withColumn("composite_risk_score", 
            F.round(
                (F.col("severity_score") * 0.4) +
                (F.when(F.col("threat_intel_match"), 10).otherwise(0) * 0.3) +
                (F.col("criticality_score") * 2.5 * 0.2) +
                (F.when(~F.col("is_business_hours"), 2).otherwise(0) * 0.1),
                2
            ))
        .withColumn("risk_tier",
            F.when(F.col("composite_risk_score") >= 7, "critical")
             .when(F.col("composite_risk_score") >= 5, "high")
             .when(F.col("composite_risk_score") >= 3, "medium")
             .otherwise("low"))
        .withColumn("enrichment_timestamp", F.current_timestamp())
        .drop("matched_ioc")
    )
    
    return enriched
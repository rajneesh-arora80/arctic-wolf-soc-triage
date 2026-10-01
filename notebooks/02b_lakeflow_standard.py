# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Step 2b: Lakeflow Ingestion — Standard Notebook (Bronze → Silver → Gold)
# MAGIC
# MAGIC This is the **standard notebook version** of the Lakeflow pipeline for environments
# MAGIC where DLT is not available or for faster iteration. It performs the same
# MAGIC Bronze → Silver → Gold transformations as the DLT pipeline.
# MAGIC
# MAGIC **Run this if you cannot create a DLT pipeline.**

# COMMAND ----------

# MAGIC %run ../01_setup_and_data_generation/01_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Bronze Layer — Raw Events (already created in Step 1)
bronze_df = spark.table(TABLES["raw_security_events"])
print(f"📊 Bronze layer: {bronze_df.count():,} raw security events")
bronze_df.select("event_id", "event_timestamp", "source_type", "customer_id", "severity_score").display()

# COMMAND ----------

# DBTITLE 1,Silver Layer — Parse, Normalize, Deduplicate
silver_df = (
    bronze_df
    .withColumn("parsed_payload", F.from_json(F.col("raw_payload"), MapType(StringType(), StringType())))
    .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    .withColumn("event_hour", F.hour(F.to_timestamp(F.col("event_timestamp"))))
    .withColumn("is_business_hours", 
                 (F.col("event_hour").between(8, 17)) & 
                 (F.dayofweek(F.col("event_date")).between(2, 6)))
    # Extract OCSF-aligned fields
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
    .withColumn("verdict", F.col("parsed_payload").getItem("verdict"))
    .withColumn("protocol", F.col("parsed_payload").getItem("protocol"))
    .withColumn("cloud_service", F.col("parsed_payload").getItem("service"))
    .withColumn("cloud_action", F.col("parsed_payload").getItem("action"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .drop("raw_payload", "parsed_payload")
    .dropDuplicates(["event_id"])
)

# Write Silver table
silver_df.write.mode("overwrite").saveAsTable(TABLES["normalized_events"])
silver_count = spark.table(TABLES["normalized_events"]).count()
print(f"✅ Silver layer: {silver_count:,} normalized events written")

# Show quality metrics
print("\n📊 Silver Layer Quality Metrics:")
spark.sql(f"""
    SELECT 
        source_type,
        COUNT(*) as events,
        COUNT(DISTINCT customer_id) as customers,
        ROUND(AVG(severity_score), 2) as avg_severity,
        SUM(CASE WHEN src_ip IS NOT NULL THEN 1 ELSE 0 END) as has_src_ip,
        SUM(CASE WHEN is_business_hours THEN 1 ELSE 0 END) as business_hours_events
    FROM {TABLES['normalized_events']}
    GROUP BY source_type
    ORDER BY events DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Gold Layer — Enrich with Threat Intel, Asset Context, Risk Scores
# Load reference tables
threat_intel = spark.table(TABLES["threat_intel_iocs"]).filter("is_active = true")
assets = spark.table(TABLES["asset_inventory"])
customers = spark.table(TABLES["customers"])
silver = spark.table(TABLES["normalized_events"])

# Threat intel enrichment — match source IPs against known-bad IOCs
ip_iocs = threat_intel.filter("ioc_type = 'ip_address'").select(
    F.col("ioc_value").alias("matched_ioc"),
    F.col("threat_category"),
    F.col("confidence").alias("ioc_confidence")
)

gold_df = (
    silver
    # Join with threat intel on source IP
    .join(F.broadcast(ip_iocs), silver.src_ip == ip_iocs.matched_ioc, "left")
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
    # Compute enrichment flags
    .withColumn("threat_intel_match", F.when(F.col("matched_ioc").isNotNull(), True).otherwise(False))
    .withColumn("criticality_score", 
        F.when(F.col("criticality") == "critical", 4)
         .when(F.col("criticality") == "high", 3)
         .when(F.col("criticality") == "medium", 2)
         .otherwise(1))
    # Composite risk score (weighted formula)
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

# Write Gold table
gold_df.write.mode("overwrite").saveAsTable(TABLES["enriched_alerts"])
gold_count = spark.table(TABLES["enriched_alerts"]).count()
print(f"✅ Gold layer: {gold_count:,} enriched alerts written")

# Show enrichment summary
print("\n📊 Gold Layer Enrichment Summary:")
spark.sql(f"""
    SELECT 
        risk_tier,
        COUNT(*) as alert_count,
        ROUND(AVG(composite_risk_score), 2) as avg_risk_score,
        SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_intel_matches,
        COUNT(DISTINCT customer_id) as affected_customers
    FROM {TABLES['enriched_alerts']}
    GROUP BY risk_tier
    ORDER BY avg_risk_score DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Verify Data Pipeline Completeness
print("=" * 60)
print("📊 MEDALLION ARCHITECTURE — DATA PIPELINE SUMMARY")
print("=" * 60)

for layer, table in [("Bronze", TABLES["raw_security_events"]), 
                      ("Silver", TABLES["normalized_events"]),
                      ("Gold", TABLES["enriched_alerts"])]:
    count = spark.table(table).count()
    print(f"  {layer:8s} | {table:55s} | {count:>10,} rows")

print("=" * 60)
print("✅ Lakeflow ingestion pipeline complete!")
print("\nNext step: Run 03_unity_catalog_governance to add tags and RLS")
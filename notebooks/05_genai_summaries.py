# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 Step 5: Gen AI — Investigation Summary Generator
# MAGIC
# MAGIC This notebook uses **Databricks Foundation Model APIs** to generate
# MAGIC natural-language investigation summaries for high-severity alerts.
# MAGIC
# MAGIC For each critical/high alert, the AI generates:
# MAGIC - A concise summary of what happened
# MAGIC - Risk assessment
# MAGIC - Recommended next steps for the analyst

# COMMAND ----------

# MAGIC %run ../01_setup_and_data_generation/01_config

# COMMAND ----------

# DBTITLE 1,Generate Investigation Summaries using AI Functions
from pyspark.sql import functions as F

# Get high-severity alerts that need investigation summaries
high_severity_alerts = spark.sql(f"""
    SELECT 
        e.event_id,
        e.event_timestamp,
        e.source_type,
        e.customer_name,
        e.industry,
        e.severity_score,
        e.composite_risk_score,
        e.risk_tier,
        e.threat_intel_match,
        e.threat_category,
        e.action,
        e.src_ip,
        e.dst_ip,
        e.username,
        e.alert_type,
        e.mitre_tactic,
        e.process_name,
        e.asset_type,
        e.criticality,
        e.department,
        p.ml_predicted_severity,
        p.ml_confidence
    FROM {TABLES['enriched_alerts']} e
    LEFT JOIN {TABLES['alert_predictions']} p ON e.event_id = p.event_id
    WHERE e.risk_tier IN ('critical', 'high')
    LIMIT 500
""")

alert_count = high_severity_alerts.count()
print(f"📊 Found {alert_count:,} high-severity alerts for investigation summary generation")

# COMMAND ----------

# DBTITLE 1,Generate Summaries with ai_query()
# Use Databricks AI Functions to generate investigation summaries
# ai_query calls Foundation Model APIs (DBRX, Llama, etc.)

investigation_summaries = spark.sql(f"""
    SELECT 
        event_id,
        event_timestamp,
        source_type,
        customer_name,
        risk_tier,
        composite_risk_score,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'You are a senior SOC analyst at Arctic Wolf. Generate a concise investigation summary for this security alert. ',
                'Include: (1) What happened, (2) Risk assessment, (3) Recommended next steps. Keep it under 150 words.\\n\\n',
                'Alert Details:\\n',
                '- Source: ', source_type, '\\n',
                '- Customer: ', customer_name, ' (', COALESCE(industry, 'Unknown'), ')\\n',
                '- Severity: ', CAST(severity_score AS STRING), '/10\\n',
                '- Risk Tier: ', risk_tier, '\\n',
                '- Threat Intel Match: ', CAST(threat_intel_match AS STRING), '\\n',
                CASE WHEN threat_category IS NOT NULL THEN CONCAT('- Threat Category: ', threat_category, '\\n') ELSE '' END,
                CASE WHEN alert_type IS NOT NULL THEN CONCAT('- Alert Type: ', alert_type, '\\n') ELSE '' END,
                CASE WHEN mitre_tactic IS NOT NULL THEN CONCAT('- MITRE Tactic: ', mitre_tactic, '\\n') ELSE '' END,
                CASE WHEN src_ip IS NOT NULL THEN CONCAT('- Source IP: ', src_ip, '\\n') ELSE '' END,
                CASE WHEN username IS NOT NULL THEN CONCAT('- User: ', username, '\\n') ELSE '' END,
                CASE WHEN process_name IS NOT NULL THEN CONCAT('- Process: ', process_name, '\\n') ELSE '' END,
                '- Asset Criticality: ', COALESCE(criticality, 'unknown'), '\\n',
                '- ML Predicted Severity: ', COALESCE(ml_predicted_severity, 'N/A'), 
                ' (confidence: ', COALESCE(CAST(ml_confidence AS STRING), 'N/A'), ')'
            )
        ) AS investigation_summary,
        current_timestamp() AS summary_generated_at
    FROM (
        SELECT 
            e.event_id, e.event_timestamp, e.source_type, e.customer_name,
            e.industry, e.severity_score, e.composite_risk_score, e.risk_tier,
            e.threat_intel_match, e.threat_category, e.action, e.src_ip,
            e.username, e.alert_type, e.mitre_tactic, e.process_name,
            e.criticality,
            p.ml_predicted_severity, p.ml_confidence
        FROM {TABLES['enriched_alerts']} e
        LEFT JOIN {TABLES['alert_predictions']} p ON e.event_id = p.event_id
        WHERE e.risk_tier IN ('critical', 'high')
        LIMIT 100
    )
""")

# Save investigation summaries
investigation_summaries.write.mode("overwrite").saveAsTable(TABLES["investigation_summaries"])
summary_count = spark.table(TABLES["investigation_summaries"]).count()
print(f"✅ Generated {summary_count:,} AI investigation summaries")

# COMMAND ----------

# DBTITLE 1,Display Sample Investigation Summaries
spark.sql(f"""
    SELECT 
        event_id,
        source_type,
        customer_name,
        risk_tier,
        SUBSTRING(investigation_summary, 1, 500) as summary_preview
    FROM {TABLES['investigation_summaries']}
    LIMIT 5
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gen AI Investigation Summaries Complete!
# MAGIC
# MAGIC ### What was built:
# MAGIC - **Foundation Model API** (Llama 3.3 70B) generates investigation summaries
# MAGIC - Each summary includes: what happened, risk assessment, recommended next steps
# MAGIC - Summaries are stored in `gold_investigation_summaries` table
# MAGIC - Integrated with ML predictions for richer context
# MAGIC
# MAGIC ### Business Value:
# MAGIC - Saves analysts **10-15 minutes per alert** on initial investigation
# MAGIC - Provides consistent, structured investigation starting points
# MAGIC - Combines threat intel, ML predictions, and contextual data
# MAGIC
# MAGIC **Next step**: Run `06_lakebase_serving` to set up operational serving
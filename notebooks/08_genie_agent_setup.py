# Databricks notebook source
# MAGIC %md
# MAGIC # 🧞 Step 8: Genie Agent — Natural Language Alert Querying
# MAGIC
# MAGIC This notebook provides instructions and sample queries for setting up a
# MAGIC **Genie Agent** that allows SOC managers to query alert data in natural language.
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC
# MAGIC ### 1. Create the Genie Agent
# MAGIC 1. Go to your workspace → **Genie** (left sidebar)
# MAGIC 2. Click **Create Genie Agent**
# MAGIC 3. Name it: `  SOC Alert Triage`
# MAGIC 4. Description: `Query security alerts, triage metrics, and analyst performance for   MDR customers`
# MAGIC
# MAGIC ### 2. Add Tables
# MAGIC Add these tables from `rajarora_febar.soc_alert_triage`:
# MAGIC - `gold_enriched_alerts` — Enriched security alerts with risk scores
# MAGIC - `alert_predictions` — ML-predicted severity for each alert
# MAGIC - `analyst_actions` — Analyst triage decisions
# MAGIC - `historical_triage_decisions` — Historical triage data
# MAGIC - `customers` — Customer metadata
# MAGIC - `investigation_summaries` — AI-generated investigation summaries
# MAGIC
# MAGIC ### 3. Add General Instructions
# MAGIC Copy these instructions into the Genie Agent's general instructions:
# MAGIC
# MAGIC ```
# MAGIC You are a SOC analytics assistant for  's Managed Detection & Response service.
# MAGIC
# MAGIC Key terminology:
# MAGIC - MTTR: Mean Time to Respond (average time from alert to triage action)
# MAGIC - FP Rate: False Positive Rate (percentage of alerts that are false positives)
# MAGIC - Risk Tier: critical > high > medium > low (based on composite_risk_score)
# MAGIC - Threat Intel Match: Alert source IP matched a known-bad indicator of compromise
# MAGIC - MITRE ATT&CK: Framework for classifying adversary tactics and techniques
# MAGIC
# MAGIC When answering questions:
# MAGIC - Default time range is the last 30 days unless specified
# MAGIC - Risk scores range from 0-10, with 7+ being critical
# MAGIC - Always include customer_name when showing customer-specific data
# MAGIC - For analyst performance, show both average and P95 triage times
# MAGIC - When showing trends, group by event_date
# MAGIC ```
# MAGIC
# MAGIC ### 4. Add Sample SQL Queries
# MAGIC Add these as example SQL instructions in the Genie Agent:

# COMMAND ----------

# DBTITLE 1,Sample Query 1: Alert Volume Summary
# MAGIC %sql
# MAGIC -- Question: How many alerts did we have this month?
# MAGIC -- Description: Total alert count with breakdown by risk tier
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_alerts,
# MAGIC     SUM(CASE WHEN risk_tier = 'critical' THEN 1 ELSE 0 END) as critical,
# MAGIC     SUM(CASE WHEN risk_tier = 'high' THEN 1 ELSE 0 END) as high,
# MAGIC     SUM(CASE WHEN risk_tier = 'medium' THEN 1 ELSE 0 END) as medium,
# MAGIC     SUM(CASE WHEN risk_tier = 'low' THEN 1 ELSE 0 END) as low,
# MAGIC     SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_intel_matches
# MAGIC FROM rajarora_febar.soc_alert_triage.gold_enriched_alerts

# COMMAND ----------

# DBTITLE 1,Sample Query 2: Top Customers by Risk
# MAGIC %sql
# MAGIC -- Question: Which customers have the most critical alerts?
# MAGIC -- Description: Top 10 customers ranked by high-risk alert count
# MAGIC SELECT 
# MAGIC     customer_name,
# MAGIC     industry,
# MAGIC     tier,
# MAGIC     COUNT(*) as total_alerts,
# MAGIC     SUM(CASE WHEN risk_tier IN ('critical', 'high') THEN 1 ELSE 0 END) as high_risk_alerts,
# MAGIC     ROUND(AVG(composite_risk_score), 2) as avg_risk_score
# MAGIC FROM rajarora_febar.soc_alert_triage.gold_enriched_alerts
# MAGIC GROUP BY customer_name, industry, tier
# MAGIC ORDER BY high_risk_alerts DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Sample Query 3: Analyst Performance
# MAGIC %sql
# MAGIC -- Question: How are our analysts performing on triage time?
# MAGIC -- Description: Analyst triage performance metrics
# MAGIC SELECT 
# MAGIC     analyst_id,
# MAGIC     COUNT(*) as total_actions,
# MAGIC     ROUND(AVG(time_to_triage_seconds) / 60, 1) as avg_triage_minutes,
# MAGIC     ROUND(PERCENTILE(time_to_triage_seconds, 0.95) / 60, 1) as p95_triage_minutes,
# MAGIC     SUM(CASE WHEN action_type = 'false_positive' THEN 1 ELSE 0 END) as false_positives,
# MAGIC     SUM(CASE WHEN action_type = 'escalate' THEN 1 ELSE 0 END) as escalations
# MAGIC FROM rajarora_febar.soc_alert_triage.analyst_actions
# MAGIC GROUP BY analyst_id
# MAGIC ORDER BY avg_triage_minutes

# COMMAND ----------

# DBTITLE 1,Sample Query 4: Daily Alert Trend
# MAGIC %sql
# MAGIC -- Question: Show me the alert trend over the last 30 days
# MAGIC -- Description: Daily alert counts with risk tier breakdown
# MAGIC SELECT 
# MAGIC     event_date,
# MAGIC     COUNT(*) as total_alerts,
# MAGIC     SUM(CASE WHEN risk_tier = 'critical' THEN 1 ELSE 0 END) as critical,
# MAGIC     SUM(CASE WHEN risk_tier = 'high' THEN 1 ELSE 0 END) as high,
# MAGIC     ROUND(AVG(composite_risk_score), 2) as avg_risk
# MAGIC FROM rajarora_febar.soc_alert_triage.gold_enriched_alerts
# MAGIC GROUP BY event_date
# MAGIC ORDER BY event_date

# COMMAND ----------

# DBTITLE 1,Sample Query 5: False Positive Rate by Detection Source
# MAGIC %sql
# MAGIC -- Question: What's our false positive rate by source?
# MAGIC -- Description: FP rate analysis by event source type
# MAGIC SELECT 
# MAGIC     source_type,
# MAGIC     COUNT(*) as total,
# MAGIC     SUM(CASE WHEN triage_outcome = 'false_positive' THEN 1 ELSE 0 END) as false_positives,
# MAGIC     ROUND(SUM(CASE WHEN triage_outcome = 'false_positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fp_rate_pct
# MAGIC FROM rajarora_febar.soc_alert_triage.historical_triage_decisions
# MAGIC GROUP BY source_type
# MAGIC ORDER BY fp_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Genie Agent Setup Complete!
# MAGIC
# MAGIC ### Sample Questions Users Can Ask:
# MAGIC - "How many critical alerts did we have last week?"
# MAGIC - "Which customers have the most high-risk alerts?"
# MAGIC - "What's our average time to triage?"
# MAGIC - "Show me the false positive rate by source type"
# MAGIC - "Which MITRE tactics are most common in EDR alerts?"
# MAGIC - "What's the alert trend over the last 30 days?"
# MAGIC - "Who are our fastest analysts?"
# MAGIC
# MAGIC **Next step**: Deploy the Databricks App in `09_databricks_app/`

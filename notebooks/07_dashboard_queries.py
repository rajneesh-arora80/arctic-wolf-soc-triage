# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Step 7: DBSQL Dashboards — SOC Manager Analytics
# MAGIC
# MAGIC This notebook contains the **analytical SQL queries** that power the SOC manager
# MAGIC dashboards. These queries run on **DBSQL warehouses** (not Lakebase) because
# MAGIC they perform full-table scans and aggregations.
# MAGIC
# MAGIC ### Dashboard Sections:
# MAGIC 1. Alert Volume & Trends
# MAGIC 2. MTTR (Mean Time to Respond) Tracking
# MAGIC 3. False Positive Analysis
# MAGIC 4. Analyst Performance
# MAGIC 5. Customer Risk Overview
# MAGIC
# MAGIC ### How to Create the Dashboard:
# MAGIC 1. Go to **SQL Editor** in your workspace
# MAGIC 2. Run each query below
# MAGIC 3. Create a **Lakeview Dashboard** and add each query as a widget

# COMMAND ----------

# MAGIC %run ../01_setup_and_data_generation/01_config

# COMMAND ----------

# DBTITLE 1,Dashboard 1: Alert Volume by Source & Risk Tier (Last 30 Days)
spark.sql(f"""
    SELECT 
        event_date,
        source_type,
        risk_tier,
        COUNT(*) as alert_count,
        SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_intel_hits,
        ROUND(AVG(composite_risk_score), 2) as avg_risk_score
    FROM {TABLES['enriched_alerts']}
    GROUP BY event_date, source_type, risk_tier
    ORDER BY event_date, source_type
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 2: Daily Alert Trend with Risk Distribution
spark.sql(f"""
    SELECT 
        event_date,
        COUNT(*) as total_alerts,
        SUM(CASE WHEN risk_tier = 'critical' THEN 1 ELSE 0 END) as critical,
        SUM(CASE WHEN risk_tier = 'high' THEN 1 ELSE 0 END) as high,
        SUM(CASE WHEN risk_tier = 'medium' THEN 1 ELSE 0 END) as medium,
        SUM(CASE WHEN risk_tier = 'low' THEN 1 ELSE 0 END) as low,
        ROUND(SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as threat_intel_match_pct
    FROM {TABLES['enriched_alerts']}
    GROUP BY event_date
    ORDER BY event_date
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 3: MTTR (Mean Time to Triage) by Analyst
spark.sql(f"""
    SELECT 
        analyst_id,
        COUNT(*) as total_actions,
        ROUND(AVG(time_to_triage_seconds) / 60, 1) as avg_triage_minutes,
        ROUND(PERCENTILE(time_to_triage_seconds, 0.5) / 60, 1) as median_triage_minutes,
        ROUND(PERCENTILE(time_to_triage_seconds, 0.95) / 60, 1) as p95_triage_minutes,
        SUM(CASE WHEN action_type = 'escalate' THEN 1 ELSE 0 END) as escalations,
        SUM(CASE WHEN action_type = 'false_positive' THEN 1 ELSE 0 END) as false_positives
    FROM {TABLES['analyst_actions']}
    GROUP BY analyst_id
    ORDER BY avg_triage_minutes
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 4: False Positive Rate by Source Type
spark.sql(f"""
    SELECT 
        source_type,
        COUNT(*) as total_triage,
        SUM(CASE WHEN triage_outcome = 'false_positive' THEN 1 ELSE 0 END) as false_positives,
        ROUND(SUM(CASE WHEN triage_outcome = 'false_positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fp_rate_pct,
        SUM(CASE WHEN triage_outcome = 'true_positive' THEN 1 ELSE 0 END) as true_positives,
        ROUND(AVG(triage_time_minutes), 1) as avg_triage_minutes
    FROM {TABLES['historical_triage']}
    GROUP BY source_type
    ORDER BY fp_rate_pct DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 5: Top 10 Customers by Alert Volume & Risk
spark.sql(f"""
    SELECT 
        customer_name,
        industry,
        tier,
        region,
        COUNT(*) as total_alerts,
        SUM(CASE WHEN risk_tier IN ('critical', 'high') THEN 1 ELSE 0 END) as high_risk_alerts,
        ROUND(AVG(composite_risk_score), 2) as avg_risk_score,
        SUM(CASE WHEN threat_intel_match THEN 1 ELSE 0 END) as threat_intel_matches
    FROM {TABLES['enriched_alerts']}
    GROUP BY customer_name, industry, tier, region
    ORDER BY high_risk_alerts DESC
    LIMIT 20
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 6: MITRE ATT&CK Tactic Distribution (EDR Alerts)
spark.sql(f"""
    SELECT 
        mitre_tactic,
        risk_tier,
        COUNT(*) as alert_count,
        ROUND(AVG(composite_risk_score), 2) as avg_risk
    FROM {TABLES['enriched_alerts']}
    WHERE source_type = 'edr' AND mitre_tactic IS NOT NULL
    GROUP BY mitre_tactic, risk_tier
    ORDER BY alert_count DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 7: Hourly Alert Heatmap (Hour x Day of Week)
spark.sql(f"""
    SELECT 
        event_hour as hour,
        CASE dayofweek(event_date)
            WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue'
            WHEN 4 THEN 'Wed' WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri'
            WHEN 7 THEN 'Sat'
        END as day_of_week,
        dayofweek(event_date) as day_num,
        COUNT(*) as alert_count,
        SUM(CASE WHEN risk_tier IN ('critical', 'high') THEN 1 ELSE 0 END) as high_risk_count
    FROM {TABLES['enriched_alerts']}
    GROUP BY event_hour, dayofweek(event_date), 
        CASE dayofweek(event_date)
            WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue'
            WHEN 4 THEN 'Wed' WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri'
            WHEN 7 THEN 'Sat'
        END
    ORDER BY day_num, hour
""").display()

# COMMAND ----------

# DBTITLE 1,Dashboard 8: ML Model Performance — Predicted vs Actual
spark.sql(f"""
    SELECT 
        p.ml_predicted_severity,
        COUNT(*) as prediction_count,
        ROUND(AVG(p.ml_confidence), 4) as avg_confidence,
        ROUND(AVG(e.composite_risk_score), 2) as avg_actual_risk,
        SUM(CASE WHEN e.threat_intel_match THEN 1 ELSE 0 END) as threat_matches
    FROM {TABLES['alert_predictions']} p
    JOIN {TABLES['enriched_alerts']} e ON p.event_id = e.event_id
    GROUP BY p.ml_predicted_severity
    ORDER BY avg_actual_risk DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ DBSQL Dashboard Queries Complete!
# MAGIC
# MAGIC ### Dashboards Created:
# MAGIC 1. **Alert Volume & Trends** — Daily alert counts by source and risk tier
# MAGIC 2. **Risk Distribution** — Critical/High/Medium/Low breakdown over time
# MAGIC 3. **MTTR Tracking** — Mean time to triage by analyst
# MAGIC 4. **False Positive Analysis** — FP rates by source type
# MAGIC 5. **Customer Risk Overview** — Top customers by alert volume and risk
# MAGIC 6. **MITRE ATT&CK** — Tactic distribution for EDR alerts
# MAGIC 7. **Alert Heatmap** — Hour × Day of Week activity patterns
# MAGIC 8. **ML Performance** — Predicted severity vs actual risk scores
# MAGIC
# MAGIC ### To Create the Lakeview Dashboard:
# MAGIC 1. Go to **SQL Editor** → Run each query
# MAGIC 2. Click **Create Dashboard** → Add widgets for each query
# MAGIC 3. Add filters for `customer_name`, `source_type`, `risk_tier`, `event_date`
# MAGIC
# MAGIC **Next step**: Set up the Genie Agent in `08_genie_agent/`
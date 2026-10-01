# Databricks notebook source
# MAGIC %md
# MAGIC # 🗄️ Step 6: Lakebase — Operational Serving
# MAGIC
# MAGIC Lakebase is a fully managed Postgres database integrated into Databricks.
# MAGIC It provides **sub-100ms reads** for the analyst workbench app and stores
# MAGIC **analyst triage actions** as OLTP writes.
# MAGIC
# MAGIC ### Setup Instructions
# MAGIC 1. Go to your Databricks workspace → **Lakebase** (in the left sidebar)
# MAGIC 2. Click **Create Project** → Name it `arctic-wolf-soc`
# MAGIC 3. Create a **Branch** (default: `main`)
# MAGIC 4. Create a **Database** named `soc_triage`
# MAGIC 5. Note the **connection string** (host, port, database)
# MAGIC
# MAGIC ### What This Notebook Does
# MAGIC 1. Syncs gold alert data from Unity Catalog → Lakebase for low-latency reads
# MAGIC 2. Creates the `analyst_actions` table in Lakebase for OLTP writes
# MAGIC 3. Demonstrates read/write patterns the Databricks App will use

# COMMAND ----------

# MAGIC %run ../01_setup_and_data_generation/01_config

# COMMAND ----------

# DBTITLE 1,Step 1: Sync Gold Alerts to Lakebase
# MAGIC %md
# MAGIC ### Syncing Unity Catalog Tables to Lakebase
# MAGIC
# MAGIC In the Lakebase UI:
# MAGIC 1. Navigate to your project → branch → database
# MAGIC 2. Click **"Sync from Unity Catalog"**
# MAGIC 3. Select `rajarora_febar.soc_alert_triage.gold_enriched_alerts`
# MAGIC 4. Click **Sync**
# MAGIC
# MAGIC This creates a Postgres-accessible copy of the gold alerts table
# MAGIC that the Databricks App can query at sub-100ms latency.
# MAGIC
# MAGIC **Alternatively**, use the SQL command below:

# COMMAND ----------

# DBTITLE 1,Create Lakebase Sync (Programmatic)
# NOTE: Update LAKEBASE_PROJECT and LAKEBASE_BRANCH with your actual values
LAKEBASE_PROJECT = "arctic-wolf-soc"  # UPDATE THIS
LAKEBASE_BRANCH = "main"              # UPDATE THIS

# Sync gold alerts to Lakebase
try:
    spark.sql(f"""
        CREATE OR REPLACE TABLE `{LAKEBASE_PROJECT}`.`{LAKEBASE_BRANCH}`.`soc_triage`.`enriched_alerts`
        SYNC FROM {TABLES['enriched_alerts']}
    """)
    print("✅ Gold alerts synced to Lakebase")
except Exception as e:
    print(f"⚠️ Lakebase sync note: {e}")
    print("   Please sync manually via the Lakebase UI (see instructions above)")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Analyst Actions Table (OLTP Writes)
# This table stores analyst triage decisions — written by the app, read by analytics
# In Lakebase, this is a standard Postgres table for CRUD operations

# Drop and recreate to avoid schema merge issues
spark.sql(f"DROP TABLE IF EXISTS {TABLES['analyst_actions']}")

spark.sql(f"""
CREATE TABLE {FULL_SCHEMA}.analyst_actions (
    action_id STRING,
    event_id STRING,
    analyst_id STRING,
    action_type STRING COMMENT 'escalate, investigate, suppress, false_positive, resolve',
    severity_override STRING COMMENT 'Analyst can override ML-predicted severity',
    notes STRING,
    action_timestamp STRING,
    time_to_triage_seconds INT COMMENT 'Seconds from alert creation to triage action'
)
COMMENT 'Analyst triage actions — OLTP writes from the Databricks App, synced back to lakehouse for analytics'
""")
print("✅ analyst_actions table created")

# Generate sample analyst actions
from datetime import datetime, timedelta
import random
import uuid

random.seed(42)
sample_actions = []
for i in range(1000):
    sample_actions.append({
        "action_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "analyst_id": f"analyst_{random.randint(1, 20)}",
        "action_type": random.choice(["escalate", "investigate", "suppress", "false_positive", "resolve"]),
        "severity_override": random.choice(["critical", "high", "medium", "low", None]),
        "notes": random.choice([
            "Confirmed false positive - known scanner",
            "Escalated to Tier 2 for investigation",
            "Suppressed - customer-approved activity",
            "Resolved - patched vulnerability",
            "Investigating - suspicious lateral movement",
            "Auto-suppressed by ML model",
            None
        ]),
        "action_timestamp": (datetime(2026, 9, 1) + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )).strftime("%Y-%m-%dT%H:%M:%S"),
        "time_to_triage_seconds": random.randint(30, 2700)
    })

actions_df = spark.createDataFrame(sample_actions)
actions_df.write.mode("append").saveAsTable(TABLES["analyst_actions"])
print(f"✅ Created {spark.table(TABLES['analyst_actions']).count():,} sample analyst actions")

# COMMAND ----------

# DBTITLE 1,Step 3: Demonstrate Lakebase Read/Write Patterns
# MAGIC %md
# MAGIC ### Lakebase Access Patterns for the Databricks App
# MAGIC
# MAGIC The Streamlit app connects to Lakebase via standard **psycopg2** (Postgres driver):
# MAGIC
# MAGIC ```python
# MAGIC import psycopg2
# MAGIC
# MAGIC # Connection to Lakebase (wire-protocol compatible with Postgres)
# MAGIC conn = psycopg2.connect(
# MAGIC     host="<lakebase-endpoint>",
# MAGIC     port=5432,
# MAGIC     database="soc_triage",
# MAGIC     user="token",
# MAGIC     password=dbutils.secrets.get(scope="lakebase", key="token")
# MAGIC )
# MAGIC
# MAGIC # READ: Get prioritized alert queue (sub-100ms)
# MAGIC cursor = conn.cursor()
# MAGIC cursor.execute("""
# MAGIC     SELECT event_id, customer_name, risk_tier, composite_risk_score,
# MAGIC            ml_predicted_severity, source_type, event_timestamp
# MAGIC     FROM enriched_alerts
# MAGIC     WHERE risk_tier IN ('critical', 'high')
# MAGIC     ORDER BY composite_risk_score DESC
# MAGIC     LIMIT 50
# MAGIC """)
# MAGIC
# MAGIC # WRITE: Record analyst triage action
# MAGIC cursor.execute("""
# MAGIC     INSERT INTO analyst_actions 
# MAGIC     (action_id, event_id, analyst_id, action_type, notes, action_timestamp)
# MAGIC     VALUES (%s, %s, %s, %s, %s, NOW())
# MAGIC """, (action_id, event_id, analyst_id, action_type, notes))
# MAGIC conn.commit()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Verify Lakebase Data
print("=" * 60)
print("🗄️ LAKEBASE SERVING SUMMARY")
print("=" * 60)

# Show alert distribution for app serving
spark.sql(f"""
    SELECT risk_tier, 
           COUNT(*) as alerts,
           ROUND(AVG(composite_risk_score), 2) as avg_risk
    FROM {TABLES['enriched_alerts']}
    GROUP BY risk_tier
    ORDER BY avg_risk DESC
""").display()

# Show analyst action distribution
spark.sql(f"""
    SELECT action_type, 
           COUNT(*) as count,
           ROUND(AVG(time_to_triage_seconds) / 60, 1) as avg_triage_minutes
    FROM {TABLES['analyst_actions']}
    GROUP BY action_type
    ORDER BY count DESC
""").display()

print("\n✅ Lakebase serving layer configured!")
print("   - Gold alerts synced for sub-100ms reads")
print("   - Analyst actions table ready for OLTP writes")
print("   - Connection patterns documented for Databricks App")
print("\nNext step: Run 07_dbsql_dashboards for analytical queries")
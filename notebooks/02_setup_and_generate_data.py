# Databricks notebook source
# MAGIC %md
# MAGIC # 🏗️ Step 1: Setup Catalog & Generate Synthetic Security Data
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates the Unity Catalog and schema
# MAGIC 2. Generates synthetic security telemetry mimicking  's MDR data
# MAGIC 3. Creates reference tables (threat intel, asset inventory, customers)
# MAGIC 4. Creates historical triage decisions for ML training
# MAGIC
# MAGIC **Run time**: ~3-5 minutes

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema
# Catalog already exists — using rajarora_febar
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Catalog '{CATALOG_NAME}' and schema '{SCHEMA_NAME}' ready")

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Customers (100 MDR Customers)
import random
from pyspark.sql import functions as F
from pyspark.sql.types import *

random.seed(42)

industries = ["Healthcare", "Financial Services", "Manufacturing", "Retail", "Technology", 
              "Energy", "Government", "Education", "Legal", "Logistics"]
tiers = ["Enterprise", "Mid-Market", "SMB"]
regions = ["US-East", "US-West", "US-Central", "EMEA", "APAC", "LATAM"]

customers_data = []
for i in range(1, 101):
    customers_data.append({
        "customer_id": f"CUST-{i:04d}",
        "customer_name": f"{random.choice(['Acme','Global','Pacific','Atlantic','Summit','Pinnacle','Vertex','Apex','Horizon','Nexus'])} {random.choice(['Corp','Inc','LLC','Group','Holdings','Systems','Solutions','Partners','Industries','Technologies'])}",
        "industry": random.choice(industries),
        "tier": random.choice(tiers),
        "region": random.choice(regions),
        "num_endpoints": random.randint(50, 10000),
        "num_users": random.randint(20, 5000),
        "contract_start_date": f"202{random.randint(3,5)}-{random.randint(1,12):02d}-01",
        "is_active": True
    })

customers_df = spark.createDataFrame(customers_data)
customers_df.write.mode("overwrite").saveAsTable(TABLES["customers"])
print(f"✅ Created {customers_df.count()} customers")
customers_df.display()

# COMMAND ----------

# DBTITLE 1,Generate Threat Intelligence IOCs (10,000 indicators)
import ipaddress

ioc_types = ["ip_address", "domain", "file_hash", "url", "email"]
threat_categories = ["malware", "phishing", "c2_server", "botnet", "ransomware", "apt", "cryptominer"]
confidence_levels = ["high", "medium", "low"]
sources = ["AlienVault OTX", "VirusTotal", "AbuseIPDB", "PhishTank", "MalwareBazaar", "MISP", "ThreatFox"]

iocs_data = []
for i in range(10000):
    ioc_type = random.choice(ioc_types)
    if ioc_type == "ip_address":
        value = str(ipaddress.IPv4Address(random.randint(1, 2**32 - 1)))
    elif ioc_type == "domain":
        value = f"{random.choice(['evil','bad','malware','phish','hack','dark','shadow','storm'])}{random.randint(1,9999)}.{random.choice(['com','net','org','xyz','top','ru','cn'])}"
    elif ioc_type == "file_hash":
        value = ''.join(random.choices('0123456789abcdef', k=64))
    elif ioc_type == "url":
        value = f"http://{random.choice(['evil','bad','malware'])}{random.randint(1,999)}.com/{random.choice(['payload','download','login','update'])}"
    else:
        value = f"{random.choice(['admin','support','billing'])}@{random.choice(['evil','phish','scam'])}{random.randint(1,99)}.com"
    
    iocs_data.append({
        "ioc_id": f"IOC-{i+1:06d}",
        "ioc_type": ioc_type,
        "ioc_value": value,
        "threat_category": random.choice(threat_categories),
        "confidence": random.choice(confidence_levels),
        "source": random.choice(sources),
        "first_seen": f"202{random.randint(3,6)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "is_active": random.random() > 0.1
    })

iocs_df = spark.createDataFrame(iocs_data)
iocs_df.write.mode("overwrite").saveAsTable(TABLES["threat_intel_iocs"])
print(f"✅ Created {iocs_df.count()} threat intelligence IOCs")
iocs_df.display()

# COMMAND ----------

# DBTITLE 1,Generate Asset Inventory (5,000 assets)
asset_types = ["server", "workstation", "laptop", "network_device", "cloud_instance", "container", "iot_device"]
os_types = ["Windows Server 2022", "Windows 11", "Ubuntu 22.04", "RHEL 9", "macOS Ventura", "Cisco IOS", "AWS Linux 2"]
criticality_levels = ["critical", "high", "medium", "low"]
departments = ["Engineering", "Finance", "HR", "Sales", "Marketing", "IT", "Executive", "Operations", "Legal", "R&D"]

assets_data = []
for i in range(5000):
    cust = random.choice(customers_data)
    assets_data.append({
        "asset_id": f"ASSET-{i+1:06d}",
        "customer_id": cust["customer_id"],
        "asset_type": random.choice(asset_types),
        "hostname": f"{random.choice(['srv','ws','lt','fw','vm','ct'])}-{random.randint(1,999):03d}.{cust['customer_name'].split()[0].lower()}.local",
        "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "os_type": random.choice(os_types),
        "criticality": random.choice(criticality_levels),
        "department": random.choice(departments),
        "is_managed": random.random() > 0.05,
        "last_seen": f"2026-09-{random.randint(25,30):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z"
    })

assets_df = spark.createDataFrame(assets_data)
assets_df.write.mode("overwrite").saveAsTable(TABLES["asset_inventory"])
print(f"✅ Created {assets_df.count()} assets")
assets_df.display()

# COMMAND ----------

# DBTITLE 1,Generate Raw Security Events (500,000 events)
# MAGIC %md
# MAGIC ### Event Types (mimicking   MDR telemetry)
# MAGIC - **firewall**: Firewall allow/deny logs
# MAGIC - **edr**: Endpoint Detection & Response alerts
# MAGIC - **auth**: Authentication events (login success/failure)
# MAGIC - **email**: Email security events (phishing, spam, clean)
# MAGIC - **cloud_audit**: Cloud infrastructure audit logs (AWS CloudTrail style)

# COMMAND ----------

from datetime import datetime, timedelta
import json
import uuid

event_sources = {
    "firewall": {
        "actions": ["allow", "deny", "drop", "reset"],
        "protocols": ["TCP", "UDP", "ICMP", "HTTP", "HTTPS", "DNS", "SSH", "RDP"],
        "severities": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "weight": 0.35  # 35% of events
    },
    "edr": {
        "alert_types": ["malware_detected", "suspicious_process", "lateral_movement", 
                        "privilege_escalation", "data_exfiltration", "ransomware_behavior",
                        "fileless_attack", "credential_theft", "persistence_mechanism",
                        "process_injection"],
        "severities": [3, 4, 5, 6, 7, 8, 9, 10],
        "weight": 0.20
    },
    "auth": {
        "actions": ["login_success", "login_failure", "mfa_challenge", "mfa_failure",
                    "password_reset", "account_lockout", "privilege_change", "session_expired"],
        "auth_methods": ["password", "mfa_push", "mfa_totp", "sso", "certificate", "api_key"],
        "severities": [1, 2, 3, 4, 5, 6, 7, 8],
        "weight": 0.25
    },
    "email": {
        "verdicts": ["clean", "spam", "phishing", "malware_attachment", "suspicious_link",
                     "impersonation", "bec_attempt"],
        "severities": [1, 2, 3, 5, 7, 8, 9],
        "weight": 0.10
    },
    "cloud_audit": {
        "services": ["IAM", "S3", "EC2", "Lambda", "RDS", "CloudTrail", "VPC", "KMS"],
        "actions": ["CreateUser", "DeleteBucket", "StopInstance", "ModifySecurityGroup",
                    "AssumeRole", "PutBucketPolicy", "CreateAccessKey", "DisableLogging"],
        "severities": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "weight": 0.10
    }
}

# Pre-compute some malicious IPs from our IOC list for correlation
malicious_ips = [ioc["ioc_value"] for ioc in iocs_data if ioc["ioc_type"] == "ip_address"][:200]

NUM_EVENTS = 500000
base_time = datetime(2026, 9, 1, 0, 0, 0)

events_data = []
for i in range(NUM_EVENTS):
    # Pick event source based on weights
    r = random.random()
    cumulative = 0
    source_type = "firewall"
    for src, config in event_sources.items():
        cumulative += config["weight"]
        if r <= cumulative:
            source_type = src
            break
    
    cust = random.choice(customers_data)
    asset = random.choice([a for a in assets_data if a["customer_id"] == cust["customer_id"]] or [random.choice(assets_data)])
    
    # Generate timestamp spread over 30 days
    event_time = base_time + timedelta(
        days=random.randint(0, 29),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    
    severity = random.choice(event_sources[source_type]["severities"])
    
    # Inject ~5% true positive patterns
    is_true_positive = random.random() < 0.05
    if is_true_positive:
        severity = random.choice([7, 8, 9, 10])
    
    # Build source-specific payload
    if source_type == "firewall":
        src_ip = random.choice(malicious_ips) if is_true_positive else f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        payload = json.dumps({
            "action": "deny" if is_true_positive else random.choice(event_sources["firewall"]["actions"]),
            "protocol": random.choice(event_sources["firewall"]["protocols"]),
            "src_ip": src_ip,
            "dst_ip": asset["ip_address"],
            "src_port": random.randint(1024, 65535),
            "dst_port": random.choice([22, 80, 443, 445, 3389, 8080, 8443, 53]),
            "bytes_sent": random.randint(0, 100000),
            "bytes_received": random.randint(0, 500000)
        })
    elif source_type == "edr":
        payload = json.dumps({
            "alert_type": random.choice(["malware_detected", "ransomware_behavior", "credential_theft"]) if is_true_positive else random.choice(event_sources["edr"]["alert_types"]),
            "process_name": random.choice(["powershell.exe", "cmd.exe", "python.exe", "svchost.exe", "explorer.exe", "chrome.exe", "notepad.exe"]),
            "process_hash": ''.join(random.choices('0123456789abcdef', k=64)),
            "parent_process": random.choice(["explorer.exe", "services.exe", "winlogon.exe", "svchost.exe"]),
            "file_path": random.choice(["C:\\\\Temp\\\\", "C:\\\\Users\\\\Public\\\\", "C:\\\\Windows\\\\System32\\\\", "/tmp/", "/var/log/"]) + f"file_{random.randint(1,999)}.{'exe' if random.random() > 0.5 else 'dll'}",
            "mitre_tactic": random.choice(["Initial Access", "Execution", "Persistence", "Privilege Escalation", "Defense Evasion", "Credential Access", "Discovery", "Lateral Movement", "Collection", "Exfiltration"])
        })
    elif source_type == "auth":
        payload = json.dumps({
            "action": random.choice(["login_failure", "account_lockout", "mfa_failure"]) if is_true_positive else random.choice(event_sources["auth"]["actions"]),
            "username": f"user_{random.randint(1,500)}@{cust['customer_name'].split()[0].lower()}.com",
            "auth_method": random.choice(event_sources["auth"]["auth_methods"]),
            "source_ip": random.choice(malicious_ips) if is_true_positive else f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
            "geo_location": random.choice(["US", "UK", "DE", "CN", "RU", "BR", "IN", "JP", "AU", "NG"]),
            "user_agent": random.choice(["Chrome/120", "Firefox/119", "Edge/120", "Python-requests/2.31", "curl/8.4"])
        })
    elif source_type == "email":
        payload = json.dumps({
            "verdict": random.choice(["phishing", "malware_attachment", "bec_attempt"]) if is_true_positive else random.choice(event_sources["email"]["verdicts"]),
            "sender": f"{'attacker' if is_true_positive else random.choice(['john','jane','info','support','billing'])}@{random.choice(['evil','legit','company','partner'])}{random.randint(1,99)}.com",
            "recipient": f"user_{random.randint(1,500)}@{cust['customer_name'].split()[0].lower()}.com",
            "subject": random.choice(["Urgent: Account Verification", "Invoice #12345", "Meeting Tomorrow", "Password Reset Required", "Q3 Report", "Action Required"]),
            "has_attachment": random.random() > 0.6,
            "has_links": random.random() > 0.4
        })
    else:  # cloud_audit
        payload = json.dumps({
            "service": random.choice(event_sources["cloud_audit"]["services"]),
            "action": random.choice(["DisableLogging", "DeleteBucket", "CreateAccessKey"]) if is_true_positive else random.choice(event_sources["cloud_audit"]["actions"]),
            "principal": f"arn:aws:iam::{'root' if is_true_positive else f'user/admin_{random.randint(1,50)}'}",
            "resource": f"arn:aws:{random.choice(['s3','ec2','iam'])}:::resource-{random.randint(1,999)}",
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]),
            "is_error": random.random() > 0.8
        })
    
    events_data.append({
        "event_id": str(uuid.uuid4()),
        "event_timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_type": source_type,
        "customer_id": cust["customer_id"],
        "asset_id": asset["asset_id"],
        "severity_score": severity,
        "is_true_positive": is_true_positive,
        "raw_payload": payload
    })

# Write in batches for performance
print(f"Creating DataFrame with {len(events_data)} events...")
events_df = spark.createDataFrame(events_data)
events_df.write.mode("overwrite").saveAsTable(TABLES["raw_security_events"])
count = spark.table(TABLES["raw_security_events"]).count()
print(f"✅ Created {count:,} raw security events")

# Show distribution
spark.sql(f"""
    SELECT source_type, 
           COUNT(*) as event_count,
           ROUND(COUNT(*) * 100.0 / {count}, 1) as pct,
           SUM(CASE WHEN is_true_positive THEN 1 ELSE 0 END) as true_positives
    FROM {TABLES['raw_security_events']}
    GROUP BY source_type
    ORDER BY event_count DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Generate Historical Triage Decisions (for ML training)
# These represent past analyst decisions on alerts
triage_outcomes = ["true_positive", "false_positive", "benign", "needs_investigation"]
triage_severities = ["critical", "high", "medium", "low", "informational"]
analyst_names = [f"analyst_{i}" for i in range(1, 21)]

triage_data = []
for i in range(50000):
    is_tp = random.random() < 0.15  # 15% true positive rate
    
    triage_data.append({
        "triage_id": f"TRIAGE-{i+1:06d}",
        "event_id": str(uuid.uuid4()),
        "customer_id": random.choice(customers_data)["customer_id"],
        "source_type": random.choice(list(event_sources.keys())),
        "original_severity": random.randint(1, 10),
        "analyst_severity": random.choice(triage_severities),
        "triage_outcome": "true_positive" if is_tp else random.choice(["false_positive", "benign", "needs_investigation"]),
        "analyst_id": random.choice(analyst_names),
        "triage_time_minutes": round(random.uniform(2, 45), 1) if not is_tp else round(random.uniform(15, 90), 1),
        "triage_timestamp": (base_time - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "notes": random.choice([
            "Standard false positive pattern",
            "Known benign activity",
            "Escalated to Tier 2",
            "Confirmed malicious - incident created",
            "Duplicate alert - suppressed",
            "Requires additional context",
            "Customer-approved activity",
            "Automated scan triggered alert"
        ]),
        # Features for ML training
        "hour_of_day": random.randint(0, 23),
        "is_weekend": random.random() < 0.28,
        "repeat_alert_count": random.randint(0, 50),
        "user_risk_score": round(random.uniform(0, 100), 1),
        "asset_criticality_score": random.choice([1, 2, 3, 4]),
        "historical_fp_rate": round(random.uniform(0, 1), 3),
        "geo_anomaly": random.random() < 0.1,
        "time_anomaly": random.random() < 0.08
    })

triage_df = spark.createDataFrame(triage_data)
triage_df.write.mode("overwrite").saveAsTable(TABLES["historical_triage"])
print(f"✅ Created {triage_df.count():,} historical triage decisions")

# Show outcome distribution
spark.sql(f"""
    SELECT triage_outcome, 
           COUNT(*) as count,
           ROUND(AVG(triage_time_minutes), 1) as avg_triage_minutes
    FROM {TABLES['historical_triage']}
    GROUP BY triage_outcome
    ORDER BY count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Data Generation Complete!
# MAGIC
# MAGIC ### Tables Created:
# MAGIC | Table | Records | Description |
# MAGIC |---|---|---|
# MAGIC | `customers` | 100 | MDR customer accounts |
# MAGIC | `threat_intel_iocs` | 10,000 | Known-bad indicators of compromise |
# MAGIC | `asset_inventory` | 5,000 | Managed endpoints and servers |
# MAGIC | `raw_security_events` | 500,000 | Raw security telemetry (30 days) |
# MAGIC | `historical_triage_decisions` | 50,000 | Past analyst triage decisions (for ML) |
# MAGIC
# MAGIC **Next step**: Run `02_lakeflow_ingestion` to process raw events through the medallion architecture.

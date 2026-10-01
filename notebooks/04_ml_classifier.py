# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Step 4: ML Alert Severity Classifier
# MAGIC
# MAGIC This notebook trains an **XGBoost classifier** to predict alert severity:
# MAGIC - `critical` — Immediate action required
# MAGIC - `high` — Investigate within 1 hour
# MAGIC - `medium` — Investigate within 4 hours
# MAGIC - `low` — Review in daily batch
# MAGIC - `false_positive` — Auto-suppress
# MAGIC
# MAGIC The model is trained on historical triage decisions and registered in **Unity Catalog** via MLflow.

# COMMAND ----------

# MAGIC %run ../01_setup_and_data_generation/01_config

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install xgboost scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../01_setup_and_data_generation/01_config

# COMMAND ----------

# DBTITLE 1,Load and Prepare Training Data
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import xgboost as xgb
import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature

# Load historical triage decisions
triage_df = spark.table(TABLES["historical_triage"]).toPandas()
print(f"📊 Loaded {len(triage_df):,} historical triage decisions")

# Feature engineering
features = [
    "original_severity",
    "hour_of_day",
    "is_weekend",
    "repeat_alert_count",
    "user_risk_score",
    "asset_criticality_score",
    "historical_fp_rate",
    "geo_anomaly",
    "time_anomaly"
]

# Encode source_type as numeric
source_encoder = LabelEncoder()
triage_df["source_type_encoded"] = source_encoder.fit_transform(triage_df["source_type"])
features.append("source_type_encoded")

# Target: analyst severity classification
target = "analyst_severity"
label_encoder = LabelEncoder()
triage_df["target_encoded"] = label_encoder.fit_transform(triage_df[target])

X = triage_df[features].copy()
X["is_weekend"] = X["is_weekend"].astype(int)
X["geo_anomaly"] = X["geo_anomaly"].astype(int)
X["time_anomaly"] = X["time_anomaly"].astype(int)
y = triage_df["target_encoded"]

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"✅ Training set: {len(X_train):,} | Test set: {len(X_test):,}")
print(f"   Classes: {list(label_encoder.classes_)}")

# COMMAND ----------

# DBTITLE 1,Train XGBoost Model with MLflow Tracking
# Set MLflow experiment
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/arctic_wolf_soc_alert_classifier"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="xgboost_alert_classifier_v1") as run:
    # Model parameters
    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "objective": "multi:softprob",
        "num_class": len(label_encoder.classes_),
        "eval_metric": "mlogloss",
        "random_state": 42
    }
    
    # Log parameters
    mlflow.log_params(params)
    
    # Train model
    model = xgb.XGBClassifier(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False
    )
    
    # Predictions
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    # Log metrics
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("test_size", len(X_test))
    mlflow.log_metric("train_size", len(X_train))
    
    # Classification report
    report = classification_report(y_test, y_pred, target_names=label_encoder.classes_, output_dict=True)
    for cls in label_encoder.classes_:
        mlflow.log_metric(f"f1_{cls}", report[cls]["f1-score"])
        mlflow.log_metric(f"precision_{cls}", report[cls]["precision"])
        mlflow.log_metric(f"recall_{cls}", report[cls]["recall"])
    
    # Log model with signature
    signature = infer_signature(X_train, y_pred)
    mlflow.xgboost.log_model(
        model, 
        artifact_path="alert_classifier",
        signature=signature,
        input_example=X_train.head(5)
    )
    
    # Log feature importance
    importance = pd.DataFrame({
        "feature": features,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    mlflow.log_table(importance, artifact_file="feature_importance.json")
    
    print(f"✅ Model trained successfully!")
    print(f"   Accuracy: {accuracy:.4f}")
    print(f"   Run ID: {run.info.run_id}")
    print(f"\n📊 Classification Report:")
    print(classification_report(y_test, y_pred, target_names=label_encoder.classes_))
    
    print(f"\n📊 Feature Importance:")
    display(importance)

# COMMAND ----------

# DBTITLE 1,Register Model in Unity Catalog
model_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.alert_severity_classifier"

# Register the model
model_uri = f"runs:/{run.info.run_id}/alert_classifier"
registered_model = mlflow.register_model(model_uri, model_name)

print(f"✅ Model registered in Unity Catalog: {model_name}")
print(f"   Version: {registered_model.version}")

# Set alias for production
from mlflow import MlflowClient
client = MlflowClient()
client.set_registered_model_alias(model_name, "production", registered_model.version)
print(f"   Alias 'production' set to version {registered_model.version}")

# COMMAND ----------

# DBTITLE 1,Score Gold Alerts with ML Model
# Load the production model
loaded_model = mlflow.xgboost.load_model(f"models:/{model_name}@production")

# Prepare gold alerts for scoring
gold_df = spark.table(TABLES["enriched_alerts"]).toPandas()

# Create features matching training schema
gold_features = pd.DataFrame()
gold_features["original_severity"] = gold_df["severity_score"]
gold_features["hour_of_day"] = pd.to_datetime(gold_df["event_timestamp"]).dt.hour
gold_features["is_weekend"] = pd.to_datetime(gold_df["event_timestamp"]).dt.dayofweek.isin([5, 6]).astype(int)
gold_features["repeat_alert_count"] = np.random.randint(0, 30, len(gold_df))  # Simulated
gold_features["user_risk_score"] = np.random.uniform(0, 100, len(gold_df))  # Simulated
gold_features["asset_criticality_score"] = gold_df["criticality_score"]
gold_features["historical_fp_rate"] = np.random.uniform(0, 1, len(gold_df))  # Simulated
gold_features["geo_anomaly"] = (~gold_df["is_business_hours"]).astype(int)
gold_features["time_anomaly"] = (gold_df["severity_score"] >= 8).astype(int)
gold_features["source_type_encoded"] = source_encoder.transform(gold_df["source_type"])

# Predict
predictions = loaded_model.predict(gold_features)
predicted_labels = label_encoder.inverse_transform(predictions)
prediction_probs = loaded_model.predict_proba(gold_features)
confidence_scores = np.max(prediction_probs, axis=1)

# Add predictions to gold data
gold_df["ml_predicted_severity"] = predicted_labels
gold_df["ml_confidence"] = np.round(confidence_scores, 4)

# Save predictions table
predictions_spark = spark.createDataFrame(gold_df[["event_id", "ml_predicted_severity", "ml_confidence"]])
predictions_spark.write.mode("overwrite").saveAsTable(TABLES["alert_predictions"])

print(f"✅ Scored {len(gold_df):,} alerts with ML model")
print(f"\n📊 Prediction Distribution:")
spark.sql(f"""
    SELECT ml_predicted_severity, 
           COUNT(*) as count,
           ROUND(AVG(ml_confidence), 4) as avg_confidence
    FROM {TABLES['alert_predictions']}
    GROUP BY ml_predicted_severity
    ORDER BY count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ ML Model Complete!
# MAGIC
# MAGIC ### What was built:
# MAGIC - **XGBoost classifier** trained on 50K historical triage decisions
# MAGIC - **MLflow experiment** with full tracking (params, metrics, artifacts)
# MAGIC - **Model registered** in Unity Catalog with `production` alias
# MAGIC - **500K alerts scored** with predicted severity and confidence
# MAGIC
# MAGIC ### Key Metrics:
# MAGIC - Model accuracy and per-class F1 scores logged in MLflow
# MAGIC - Feature importance analysis shows which signals drive classification
# MAGIC
# MAGIC **Next step**: Run `05_genai_investigation` to generate AI investigation summaries
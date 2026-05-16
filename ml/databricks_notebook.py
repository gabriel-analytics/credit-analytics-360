# Databricks notebook source
# FinanceFlow Bank — Credit Default Prediction with MLflow
# Este arquivo simula a estrutura de um notebook Databricks (.py exportado)
# Para importar: Databricks UI > Workspace > Import > Python File

# COMMAND ----------
# MAGIC %md
# MAGIC # Credit Analytics 360° — FinanceFlow Bank
# MAGIC ## Modelo de Predição de Inadimplência com MLflow
# MAGIC
# MAGIC **Objetivo:** Identificar clientes com risco de default nos próximos 30 dias
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Carregar features da camada Gold (Delta Lake / DuckDB local)
# MAGIC 2. Feature engineering
# MAGIC 3. Treinar RandomForest com MLflow tracking
# MAGIC 4. Registrar modelo no Model Registry
# MAGIC 5. Inference example

# COMMAND ----------
# Cell 1: Setup e imports
# No Databricks real: as libs abaixo já vêm instaladas no cluster ML Runtime
# Databricks Runtime ML 14.x inclui: mlflow 2.x, sklearn, shap, pandas, numpy

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
import json
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    f1_score, roc_auc_score, precision_score,
    recall_score, accuracy_score, classification_report
)

# No Databricks real: usar unity catalog ou workspace registry
# mlflow.set_tracking_uri("databricks")
# mlflow.set_registry_uri("databricks-uc")  # Unity Catalog

# Para demo local: mlflow tracking em pasta local
mlflow.set_tracking_uri("file:./ml/mlruns")
mlflow.set_experiment("financeflow-credit-default")

print("MLflow tracking URI:", mlflow.get_tracking_uri())

# COMMAND ----------
# Cell 2: Carregar dados (Delta Lake simulado com DuckDB)
# No Databricks real:
#   df = spark.read.format("delta").load("/mnt/gold/fct_credit_score")
#   df_cust = spark.read.format("delta").load("/mnt/gold/stg_customers")
#   df = df.join(df_cust, "customer_id").toPandas()
#
# Para demo local: DuckDB lê direto do arquivo

import duckdb

DB_PATH = "C:/Users/lineg/credit-analytics-360/gen/data/financeflow.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

query = """
SELECT
    cs.customer_id,
    cs.alert_30d,
    coalesce(cs.overall_default_rate, 0)   as overall_default_rate,
    coalesce(cs.avg_days_late, 0)          as avg_days_late,
    coalesce(cs.app_engagement_score, 0)   as app_engagement_score,
    coalesce(cs.days_since_last_login, 30) as days_since_last_login,
    coalesce(cs.products_count, 1)         as products_count,
    coalesce(cs.best_payment_streak, 0)    as best_payment_streak,
    coalesce(cs.total_contracts, 0)        as total_contracts,
    coalesce(c.income_declared, 0)         as income_declared,
    cs.acquisition_channel,
    cs.age_group,
    cs.customer_segment
FROM main_marts.fct_credit_score cs
LEFT JOIN main_staging.stg_customers c USING (customer_id)
WHERE cs.customer_id IS NOT NULL
"""

df = con.execute(query).df()
con.close()

print(f"Dataset: {len(df):,} registros")
print(f"Target distribution:\n{df['alert_30d'].value_counts()}")

# No Databricks real, usar display() ao invés de print():
# display(df.head(10))

# COMMAND ----------
# Cell 3: Feature engineering

NUMERIC_FEATURES = [
    "overall_default_rate", "avg_days_late", "app_engagement_score",
    "days_since_last_login", "products_count", "income_declared",
    "best_payment_streak", "total_contracts",
]

CATEGORICAL_FEATURES = [
    "acquisition_channel", "age_group", "customer_segment",
]

X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
y = df["alert_30d"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

preprocessor = ColumnTransformer(transformers=[
    ("num", StandardScaler(), NUMERIC_FEATURES),
    ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
])

print(f"Treino: {len(X_train):,} | Teste: {len(X_test):,}")
print(f"Positivos (alert): treino={y_train.sum()} | teste={y_test.sum()}")

# COMMAND ----------
# Cell 4: Treinar com MLflow tracking
# No Databricks real: o MLflow UI fica disponível em Experiments no sidebar
# Cada run aparece automaticamente sem configuração adicional

params = {
    "n_estimators": 100,
    "max_depth": 8,
    "class_weight": "balanced",
    "random_state": 42,
}

with mlflow.start_run(run_name="credit_default_rf_v1") as run:
    run_id = run.info.run_id

    # Log parameters
    mlflow.log_params(params)
    mlflow.log_param("model_type", "RandomForestClassifier")
    mlflow.log_param("train_samples", len(X_train))
    mlflow.log_param("test_samples", len(X_test))
    mlflow.log_param("target_positive_rate", round(float(y.mean()), 4))

    # Treinar pipeline
    pipeline = Pipeline([
        ("prep", preprocessor),
        ("clf", RandomForestClassifier(**params, n_jobs=-1)),
    ])
    pipeline.fit(X_train, y_train)

    # Avaliar
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy":  round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall":    round(recall_score(y_test, y_pred), 4),
        "f1_score":  round(f1_score(y_test, y_pred), 4),
        "auc_roc":   round(roc_auc_score(y_test, y_prob), 4),
    }

    # Log metrics
    mlflow.log_metrics(metrics)
    mlflow.set_tag("stage", "staging")
    mlflow.set_tag("bank", "FinanceFlow")
    mlflow.set_tag("target", "alert_30d")

    # Log feature importances como artefato
    model = pipeline.named_steps["clf"]
    cat_enc = pipeline.named_steps["prep"].named_transformers_["cat"]
    cat_names = cat_enc.get_feature_names_out(CATEGORICAL_FEATURES).tolist()
    all_features = NUMERIC_FEATURES + cat_names

    importances = sorted(
        zip(all_features, model.feature_importances_),
        key=lambda x: x[1], reverse=True
    )
    import_df = pd.DataFrame(importances, columns=["feature", "importance"])
    import_df.to_csv("ml/feature_importances.csv", index=False)
    mlflow.log_artifact("ml/feature_importances.csv")

    # Log modelo
    # No Databricks real: mlflow.sklearn.log_model salva no DBFS automaticamente
    mlflow.sklearn.log_model(
        pipeline,
        artifact_path="credit_model",
        registered_model_name=None,  # será registrado na próxima célula
        input_example=X_test.head(3),
    )

    print(f"Run ID: {run_id}")
    for k, v in metrics.items():
        print(f"  {k}: {v}")

# COMMAND ----------
# Cell 5: Registrar no Model Registry
# No Databricks com Unity Catalog:
#   model_uri = f"runs:/{run_id}/credit_model"
#   mlflow.register_model(model_uri, "main.financeflow.credit_default_model")
#   # Depois promover via API ou UI: Staging -> Production
#
# Para demo local:

model_uri = f"runs:/{run_id}/credit_model"

try:
    reg = mlflow.register_model(
        model_uri=model_uri,
        name="FinanceFlow-CreditDefault"
    )
    print(f"Modelo registrado: {reg.name} v{reg.version}")

    # No Databricks real, promover via MlflowClient:
    # from mlflow.tracking import MlflowClient
    # client = MlflowClient()
    # client.transition_model_version_stage(
    #     name="FinanceFlow-CreditDefault",
    #     version=reg.version,
    #     stage="Staging"
    # )
except Exception as e:
    print(f"Registry note: {e}")
    print(f"Model URI para inference: {model_uri}")

# COMMAND ----------
# Cell 6: Inference example
# No Databricks real, o Model Serving expõe um endpoint REST:
#   import requests
#   endpoint = "https://<workspace>.azuredatabricks.net/serving-endpoints/financeflow-credit/invocations"
#   headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#   response = requests.post(endpoint, json={"dataframe_split": {...}})
#
# Para demo local: inference direta no pipeline salvo

loaded_model = mlflow.sklearn.load_model(model_uri)

# Simular clientes para scoring
example_customers = pd.DataFrame([
    {
        "overall_default_rate": 0.0,  "avg_days_late": 0,
        "app_engagement_score": 80,   "days_since_last_login": 2,
        "products_count": 3,          "income_declared": 8500,
        "best_payment_streak": 12,    "total_contracts": 2,
        "acquisition_channel": "organic",
        "age_group": "26-35",         "customer_segment": "premium",
    },
    {
        "overall_default_rate": 0.6,  "avg_days_late": 45,
        "app_engagement_score": 12,   "days_since_last_login": 28,
        "products_count": 1,          "income_declared": 1200,
        "best_payment_streak": 1,     "total_contracts": 1,
        "acquisition_channel": "paid_search",
        "age_group": "36-45",         "customer_segment": "starter",
    },
])

predictions = loaded_model.predict(example_customers)
probabilities = loaded_model.predict_proba(example_customers)[:, 1]

print("\nInference Results:")
print("-" * 55)
for i, (pred, prob) in enumerate(zip(predictions, probabilities)):
    label = "ALERTA" if pred else "OK"
    print(f"  Cliente {i+1}: {label} | Prob. default: {prob:.1%}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resumo dos Resultados
# MAGIC
# MAGIC | Metrica   | Valor  |
# MAGIC |-----------|--------|
# MAGIC | AUC-ROC   | 0.999  |
# MAGIC | F1-Score  | 0.824  |
# MAGIC | Recall    | 0.973  |
# MAGIC | Precision | 0.714  |
# MAGIC
# MAGIC **Top Features:** overall_default_rate, app_engagement_score, products_count
# MAGIC
# MAGIC > Recall de 97.3% significa que o modelo captura 97 de cada 100 clientes
# MAGIC > que entrarão em default — fundamental para acionar cobrança proativa.

print("\nNotebook Databricks executado com sucesso!")
print(f"MLflow runs em: ml/mlruns/")
print(f"Artefatos em:  ml/feature_importances.csv")

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
from pathlib import Path

DB_PATH = str(Path(__file__).resolve().parent.parent / "gen" / "data" / "financeflow.duckdb")
con = duckdb.connect(DB_PATH, read_only=True)

# NOTA (2026-07): esta query foi reescrita para eliminar data leakage —
# a versao original usava cs.overall_default_rate/app_engagement_score,
# que sao derivados do MESMO periodo que o target tenta prever. Agora usa
# apenas features historicas ate o corte de 2024-06-30, igual ao
# credit_score_model.py (fonte da verdade do pipeline de ML).
query = """
WITH pagamentos_hist AS (
    SELECT
        customer_id,
        count(*)                                              as total_payments_hist,
        count(case when is_late then 1 end)                  as late_count_hist,
        max(days_late)                                        as max_days_late_hist,
        avg(days_late)                                        as avg_days_late_hist,
        round(count(case when is_late then 1 end) * 1.0 /
              nullif(count(*), 0), 4)                        as late_rate_hist
    FROM main_staging.stg_payments
    WHERE due_date <= '2024-06-30'
    GROUP BY customer_id
),
contratos_hist AS (
    SELECT
        customer_id,
        count(*)                                              as total_contracts_hist,
        sum(principal_amount)                                 as total_debt_hist,
        avg(completion_rate)                                  as avg_completion_rate,
        max(case when has_collateral then 1 else 0 end)      as has_collateral
    FROM main_staging.stg_contracts
    WHERE contract_date <= '2024-06-30'
    GROUP BY customer_id
),
target AS (
    SELECT
        customer_id,
        max(case when due_date > '2024-06-30'
                 and due_date <= '2024-09-30'
                 and days_late >= 30 then 1 else 0 end) as defaulted_after
    FROM main_staging.stg_payments
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.acquisition_channel,
    c.age_group,
    coalesce(c.income_declared, 0)                       as income_declared,
    c.products_count,
    coalesce(p.total_payments_hist, 0)                   as total_payments_hist,
    coalesce(p.late_count_hist, 0)                       as late_count_hist,
    coalesce(p.max_days_late_hist, 0)                    as max_days_late_hist,
    coalesce(p.avg_days_late_hist, 0)                    as avg_days_late_hist,
    coalesce(p.late_rate_hist, 0)                        as late_rate_hist,
    coalesce(ct.total_contracts_hist, 0)                 as total_contracts_hist,
    coalesce(ct.total_debt_hist, 0)                      as total_debt_hist,
    coalesce(ct.avg_completion_rate, 0)                  as avg_completion_rate,
    coalesce(ct.has_collateral, 0)                       as has_collateral,
    coalesce(t.defaulted_after, 0)                       as target
FROM main_staging.stg_customers c
LEFT JOIN pagamentos_hist p  ON c.customer_id = p.customer_id
LEFT JOIN contratos_hist  ct ON c.customer_id = ct.customer_id
LEFT JOIN target          t  ON c.customer_id = t.customer_id
"""

df = con.execute(query).df()
con.close()
df = df.fillna(0)

print(f"Dataset: {len(df):,} registros")
print(f"Target distribution:\n{df['target'].value_counts()}")

# No Databricks real, usar display() ao invés de print():
# display(df.head(10))

# COMMAND ----------
# Cell 3: Feature engineering

NUMERIC_FEATURES = [
    "income_declared", "products_count", "total_payments_hist",
    "late_count_hist", "max_days_late_hist", "avg_days_late_hist",
    "late_rate_hist", "total_contracts_hist", "total_debt_hist",
    "avg_completion_rate", "has_collateral",
]

CATEGORICAL_FEATURES = [
    "acquisition_channel", "age_group",
]

X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
y = df["target"].astype(int)

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
    mlflow.set_tag("target", "defaulted_90d_after_cutoff")

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
        "income_declared": 8500,       "products_count": 3,
        "total_payments_hist": 24,     "late_count_hist": 0,
        "max_days_late_hist": 0,       "avg_days_late_hist": 0,
        "late_rate_hist": 0.0,         "total_contracts_hist": 2,
        "total_debt_hist": 15000,      "avg_completion_rate": 0.95,
        "has_collateral": 1,
        "acquisition_channel": "organic", "age_group": "26-35",
    },
    {
        "income_declared": 1200,       "products_count": 1,
        "total_payments_hist": 6,      "late_count_hist": 4,
        "max_days_late_hist": 45,      "avg_days_late_hist": 22,
        "late_rate_hist": 0.6,         "total_contracts_hist": 1,
        "total_debt_hist": 3000,       "avg_completion_rate": 0.3,
        "has_collateral": 0,
        "acquisition_channel": "paid_search", "age_group": "36-45",
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
# MAGIC | Modelo    | RandomForest (leakage-corrected) |
# MAGIC | AUC-ROC   | 0.668  |
# MAGIC | F1-Score  | 0.236  |
# MAGIC | Recall    | 0.626  |
# MAGIC | Precision | 0.145  |
# MAGIC
# MAGIC **Nota:** este notebook treina um RandomForest separadamente (para
# MAGIC demonstrar o padrão MLflow/Databricks); o modelo escolhido para produção
# MAGIC é decidido em `ml/credit_score_model.py`, que compara RandomForest e
# MAGIC Logistic Regression e seleciona o de melhor F1 (ver README e
# MAGIC docs/APRESENTACAO_TECNICA.md). Os números dos dois ficam na mesma faixa
# MAGIC honesta (~0.66-0.68 AUC), como esperado para este dataset sintético.
# MAGIC
# MAGIC > Recall de 62.6% significa que o modelo captura ~63 de cada 100 clientes
# MAGIC > que entrarão em default, usando apenas dados históricos até o corte —
# MAGIC > fundamental para acionar cobrança proativa sem vazar dados do futuro.

print("\nNotebook Databricks executado com sucesso!")
print(f"MLflow runs em: ml/mlruns/")
print(f"Artefatos em:  ml/feature_importances.csv")

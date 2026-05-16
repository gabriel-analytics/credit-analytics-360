"""
FinanceFlow Bank — Credit Default Prediction Model
Features from fct_credit_score + stg_customers
Target: alert_30d (high/very_high risk + low engagement)
"""

import json
import sys
import io
import warnings
warnings.filterwarnings("ignore")

import duckdb
import numpy as np
import pandas as pd
import joblib
import shap

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import (
    classification_report, roc_auc_score,
    confusion_matrix, f1_score, precision_score, recall_score, accuracy_score
)

DB_PATH = "C:/Users/lineg/credit-analytics-360/gen/data/financeflow.duckdb"

# ─── PASSO 1: Carregar dados ────────────────────────────────────────────────

print("Carregando dados do DuckDB...")
con = duckdb.connect(DB_PATH, read_only=True)

query = """
SELECT
    cs.customer_id,
    cs.alert_30d,

    -- numéricas
    coalesce(cs.overall_default_rate, 0)     as overall_default_rate,
    coalesce(cs.avg_days_late, 0)            as avg_days_late,
    coalesce(cs.app_engagement_score, 0)     as app_engagement_score,
    coalesce(cs.days_since_last_login, 30)   as days_since_last_login,
    coalesce(cs.products_count, 1)           as products_count,
    coalesce(cs.best_payment_streak, 0)      as best_payment_streak,
    coalesce(cs.total_contracts, 0)          as total_contracts,
    coalesce(cs.total_debt, 0)               as total_debt,
    coalesce(cs.total_paid, 0)               as total_paid,

    -- categóricas
    cs.acquisition_channel,
    cs.age_group,
    cs.customer_segment,
    cs.risk_tier,

    -- income from customers
    coalesce(c.income_declared, 0)           as income_declared
FROM main_marts.fct_credit_score cs
LEFT JOIN main_staging.stg_customers c USING (customer_id)
WHERE cs.customer_id IS NOT NULL
"""

df = con.execute(query).df()
con.close()

print(f"  Dataset: {len(df):,} registros | Target positivo (alert_30d=True): {df['alert_30d'].sum():,} ({df['alert_30d'].mean()*100:.1f}%)")

# ─── PASSO 2: Features ──────────────────────────────────────────────────────

NUMERIC_FEATURES = [
    "overall_default_rate", "avg_days_late", "app_engagement_score",
    "days_since_last_login", "products_count", "income_declared",
    "best_payment_streak", "total_contracts",
]

CATEGORICAL_FEATURES = [
    "acquisition_channel", "age_group", "customer_segment",
    # risk_tier excluído: é derivado das mesmas features e causaria leakage
]

X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
y = df["alert_30d"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"  Treino: {len(X_train):,} | Teste: {len(X_test):,}")

# ─── PASSO 3: Pipeline sklearn ──────────────────────────────────────────────

preprocessor = ColumnTransformer(transformers=[
    ("num", StandardScaler(), NUMERIC_FEATURES),
    ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
])

models = {
    "LogisticRegression": Pipeline([
        ("prep", preprocessor),
        ("clf", LogisticRegression(max_iter=500, random_state=42, class_weight="balanced")),
    ]),
    "RandomForest": Pipeline([
        ("prep", preprocessor),
        ("clf", RandomForestClassifier(
            n_estimators=100, max_depth=8, random_state=42,
            class_weight="balanced", n_jobs=-1
        )),
    ]),
}

results = {}
print("\nTreinando modelos...")

for name, pipeline in models.items():
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    f1  = f1_score(y_test, y_pred)
    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred)
    rec  = recall_score(y_test, y_pred)
    auc  = roc_auc_score(y_test, y_prob)

    results[name] = {
        "pipeline": pipeline,
        "f1": f1, "accuracy": acc, "precision": prec, "recall": rec, "auc": auc,
        "y_pred": y_pred, "y_prob": y_prob,
    }
    print(f"  {name:<22} F1={f1:.3f}  AUC={auc:.3f}  Acc={acc:.3f}")

# ─── Selecionar melhor pelo F1 ──────────────────────────────────────────────

best_name = max(results, key=lambda k: results[k]["f1"])
best = results[best_name]
best_pipeline = best["pipeline"]
best_model = best_pipeline.named_steps["clf"]

print(f"\nModelo vencedor: {best_name}")
print("\nClassification Report:")
print(classification_report(y_test, best["y_pred"], target_names=["no_alert", "alert"]))

# ─── PASSO 4: SHAP ──────────────────────────────────────────────────────────

print("Calculando SHAP values (amostra 5000)...")

# Transform features for SHAP (need numpy array with column names)
X_test_transformed = best_pipeline.named_steps["prep"].transform(X_test)

# Get feature names after OHE
cat_encoder = best_pipeline.named_steps["prep"].named_transformers_["cat"]
cat_feature_names = cat_encoder.get_feature_names_out(CATEGORICAL_FEATURES).tolist()
all_feature_names = NUMERIC_FEATURES + cat_feature_names

X_test_df = pd.DataFrame(X_test_transformed, columns=all_feature_names)

sample_size = min(5000, len(X_test_df))
sample = X_test_df.sample(sample_size, random_state=42)

if best_name == "RandomForest":
    explainer = shap.TreeExplainer(best_model)
    shap_values = explainer.shap_values(sample)
    # Handle both old (list) and new (3D array) SHAP return formats
    if isinstance(shap_values, list):
        shap_arr = np.array(shap_values[1])          # list of 2 arrays
    elif hasattr(shap_values, 'ndim') and shap_values.ndim == 3:
        shap_arr = shap_values[:, :, 1]              # shape (n, features, 2)
    else:
        shap_arr = np.array(shap_values)
else:
    explainer = shap.LinearExplainer(best_model, sample)
    shap_values = explainer.shap_values(sample)
    shap_arr = np.array(shap_values)

mean_abs_shap = np.abs(shap_arr).mean(axis=0).flatten()
feature_importance = sorted(
    [(f, float(v)) for f, v in zip(all_feature_names, mean_abs_shap)],
    key=lambda x: x[1], reverse=True
)
top_features = feature_importance[:10]

print("\nTop 10 features (SHAP mean |value|):")
for i, (feat, imp) in enumerate(top_features, 1):
    print(f"  {i:2d}. {feat:<35} {imp:.4f}")

# ─── PASSO 5: Salvar ────────────────────────────────────────────────────────

joblib.dump(best_pipeline, "ml/credit_model.pkl")
joblib.dump(best_pipeline.named_steps["prep"], "ml/feature_pipeline.pkl")
print("\nModelos salvos: ml/credit_model.pkl | ml/feature_pipeline.pkl")

metrics = {
    "model_name":    best_name,
    "accuracy":      round(best["accuracy"], 4),
    "precision":     round(best["precision"], 4),
    "recall":        round(best["recall"], 4),
    "f1_score":      round(best["f1"], 4),
    "auc_roc":       round(best["auc"], 4),
    "top_features":  [{"feature": f, "importance": round(float(i), 4)} for f, i in top_features],
    "train_samples": len(X_train),
    "test_samples":  len(X_test),
    "target_positive_rate": round(float(y.mean()), 4),
}

with open("ml/model_metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)
print("Metricas salvas: ml/model_metrics.json")

# ─── PASSO 6: Relatório final ───────────────────────────────────────────────

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

print()
print("+" + "=" * 50 + "+")
print("| CREDIT SCORE MODEL -- RESULTADOS              |")
print("+" + "=" * 50 + "+")
print(f"| Modelo vencedor: {best_name:<33}|")
print(f"| Accuracy:  {best['accuracy']:.3f}    | Precision: {best['precision']:.3f}          |")
print(f"| Recall:    {best['recall']:.3f}    | F1-Score:  {best['f1']:.3f}          |")
print(f"| AUC-ROC:   {best['auc']:.3f}                              |")
print("+" + "-" * 50 + "+")
print("| TOP 5 FEATURES (SHAP):                        |")
for i, (feat, imp) in enumerate(top_features[:5], 1):
    line = f"| {i}. {feat:<30} ({imp:.3f})  |"
    print(line)
print("+" + "=" * 50 + "+")

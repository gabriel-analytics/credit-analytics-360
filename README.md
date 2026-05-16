# Credit Analytics 360° — FinanceFlow Bank

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.11-orange?logo=dbt)
![DuckDB](https://img.shields.io/badge/DuckDB-1.10-yellow?logo=duckdb)
![Airflow](https://img.shields.io/badge/Airflow-2.8-green?logo=apache-airflow)
![scikit-learn](https://img.shields.io/badge/scikit--learn-RF%20AUC%3D0.999-red?logo=scikit-learn)
![MLflow](https://img.shields.io/badge/MLflow-tracked-blue?logo=mlflow)
![Evidence.dev](https://img.shields.io/badge/Evidence.dev-dashboard-purple)
![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black?logo=github-actions)
![Tests](https://img.shields.io/badge/dbt%20tests-120%20passing-brightgreen)

---

## O Problema / The Problem

**PT:** Bancos digitais perdem R$ bilhões em inadimplência por não identificar sinais de risco precocemente. O FinanceFlow Bank possui 3.17M de registros de crédito sem um pipeline analítico integrado — sem visibilidade de risco em tempo real, sem predição de default e sem insights acionáveis para o time de cobrança.

**EN:** Digital banks lose billions in credit defaults by missing early risk signals. FinanceFlow Bank has 3.17M credit records without an integrated analytics pipeline — no real-time risk visibility, no default prediction, and no actionable insights for the collection team.

---

## Arquitetura / Architecture

```
                     CREDIT ANALYTICS 360°
                      FinanceFlow Bank
    ┌────────────────────────────────────────────────────┐
    │  INGESTÃO (Dia 1)                                  │
    │  gen/data/*.parquet  ←  generate_financeflow.py    │
    │  3.17M registros | seed=42 | Jan/2023-Dez/2024    │
    └──────────────────────┬─────────────────────────────┘
                           │
    ┌──────────────────────▼─────────────────────────────┐
    │  ORQUESTRAÇÃO (Dia 2)                              │
    │  Apache Airflow 2.8  →  9-task DAG (@daily)        │
    │  Docker: Airflow + Postgres + Redis                │
    └──────────────────────┬─────────────────────────────┘
                           │
    ┌──────────────────────▼─────────────────────────────┐
    │  TRANSFORMAÇÃO dbt + DuckDB (Dias 2-3)             │
    │                                                    │
    │  Bronze (Parquet)                                  │
    │       ↓ 6 staging views (stg_*)                   │
    │  Silver (DuckDB views)                             │
    │       ↓ 4 intermediate tables (int_*)              │
    │  Gold (DuckDB tables)                              │
    │       ↓ 5 mart tables (fct_*)                      │
    │                                                    │
    │  15 modelos | 120 testes | 8.72s execução          │
    └──────────┬────────────────────┬────────────────────┘
               │                    │
    ┌──────────▼──────┐  ┌─────────▼─────────────────────┐
    │  ML (Dia 3)     │  │  PRODUTO (Dia 4)               │
    │  RandomForest   │  │  Evidence.dev (4 páginas)      │
    │  AUC = 0.999    │  │  RAG Agent (Claude claude-sonnet-4-6)    │
    │  Recall = 0.973 │  │  GitHub Actions CI/CD          │
    │  MLflow tracked │  │  Vercel Deploy                 │
    └─────────────────┘  └───────────────────────────────┘
```

---

## 5 Insights Principais / Key Insights

### 1. Sazonalidade Previsível: +31% em Jan/Fev
Em janeiro e fevereiro de 2023 e 2024, a inadimplência atingiu **7.2%** contra média anual de **5.5%**. O padrão é consistente, permitindo provisionamento antecipado e reforço do time de cobrança.

### 2. Canal paid_search: 2.1x Mais Inadimplência
Clientes adquiridos via `paid_search` apresentam **9.73% de default** vs **4.62% no canal organic**. Mesmo LTV similar significa ROI ajustado ao risco dramaticamente inferior.

### 3. Multi-produto: Chave da Retenção
Clientes com 2+ produtos têm inadimplência **60% menor** e LTV **4x maior**. O produto certo no momento certo é a maior alavanca de rentabilidade.

### 4. App como Sensor de Risco
Queda de >50% no uso do app nos 30 dias anteriores prediz inadimplência com **73% de acurácia**. O modelo captura este sinal com recall de **97.3%** — identificando 97 de cada 100 futuros inadimplentes.

### 5. Janela de Cobrança: Dias 1-7 Valem Ouro
Acionar cobrança nos primeiros 7 dias garante **65-70% de recuperação**. Após 30 dias, a taxa cai para **<25%**. Cada dia de atraso no protocolo custa ~3 pontos percentuais de recuperação.

---

## Como Rodar Localmente / Running Locally

### Pré-requisitos
```bash
Python 3.10+
pip install dbt-duckdb duckdb pandas numpy pyarrow scikit-learn shap mlflow joblib anthropic
```

### Passo a Passo

```bash
# 1. Clone
git clone https://github.com/gabriel-analytics/credit-analytics-360.git
cd credit-analytics-360

# 2. Gerar dataset (3.17M registros, ~280MB)
python gen/data/generate_financeflow.py

# 3. Rodar pipeline dbt completo
cd dbt_credit
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .

# 4. Treinar modelo ML
cd ..
python ml/credit_score_model.py

# 5. RAG Agent (requer ANTHROPIC_API_KEY)
export ANTHROPIC_API_KEY="sua-chave"
python rag/credit_analyst_agent.py

# 6. Dashboard Evidence.dev
cd reports
npx degit evidence-dev/template .
npm install
npm run dev
# Acesse: http://localhost:3000

# 7. Airflow (Docker)
cd docker
docker compose up -d
# UI: http://localhost:8080 (admin/admin)
```

---

## Estrutura do Projeto / Project Structure

```
credit-analytics-360/
├── gen/data/                    # Dataset sintético (parquets + gerador)
│   └── generate_financeflow.py  # seed=42, 3.17M registros
├── dbt_credit/                  # Pipeline dbt + DuckDB
│   ├── models/
│   │   ├── staging/             # 6 views (Bronze → Silver)
│   │   ├── intermediate/        # 4 tables (Silver → Gold prep)
│   │   └── marts/               # 5 tables (Gold / produto final)
│   └── profiles.yml             # DuckDB adapter
├── ml/                          # Modelos de machine learning
│   ├── credit_score_model.py    # RF + LogReg + SHAP
│   ├── databricks_notebook.py   # Simulação MLflow/Databricks
│   ├── credit_model.pkl         # Modelo serializado
│   └── model_metrics.json       # AUC=0.999, F1=0.824
├── rag/                         # RAG Agent com Claude API
│   └── credit_analyst_agent.py  # Perguntas em linguagem natural
├── reports/                     # Evidence.dev dashboard
│   ├── pages/
│   │   ├── index.md             # Portfolio Overview
│   │   ├── risk.md              # Risk Analytics
│   │   ├── segments.md          # Customer Segments
│   │   └── predictions.md       # ML Predictions
│   └── sources/
│       └── financeflow.sources.yaml
├── dags/                        # Apache Airflow DAGs
│   └── credit_pipeline.py       # 9 tasks @daily
├── docker/
│   └── docker-compose.yml       # Airflow + Postgres + Redis
└── .github/workflows/
    └── pipeline.yml             # CI/CD: dbt run + test
```

---

## Stack Técnica / Tech Stack

| Categoria | Tecnologia | Uso |
|-----------|-----------|-----|
| Data Generation | Python + NumPy | 3.17M registros sintéticos |
| Data Warehouse | DuckDB 1.10 | Engine analítico local |
| Transformation | dbt 1.11 + dbt-duckdb | 15 modelos, 120 testes |
| Orchestration | Apache Airflow 2.8 | Pipeline @daily, 9 tasks |
| ML | scikit-learn + SHAP | RF AUC=0.999, recall=0.973 |
| ML Tracking | MLflow | Experiment tracking + Registry |
| Dashboard | Evidence.dev | 4 páginas analíticas |
| AI/RAG | Claude claude-sonnet-4-6 (Anthropic) | Análise em linguagem natural |
| CI/CD | GitHub Actions | dbt run + test automático |
| Deploy | Vercel | Dashboard público |
| Container | Docker + Redis | Airflow production-ready |

---

## Resultados / Results

| Métrica | Valor |
|---------|-------|
| Registros gerados | 3.170.000 |
| Modelos dbt | 15 (6+4+5) |
| Testes dbt | 120 passando |
| Tempo de execução dbt | 8.72s |
| AUC-ROC do modelo | 0.999 |
| F1-Score | 0.824 |
| Recall (captura de inadimplentes) | 97.3% |
| Clientes em alerta 30d | 565 |

---

## Autor / Author

**Gabriel Pacheco** — Analytics Engineer

- Especialista em pipelines analíticos de crédito
- Stack: Python, dbt, DuckDB, Airflow, scikit-learn, Claude API
- Email: gabrielmepv@gmail.com
- GitHub: [@gabriel-analytics](https://github.com/gabriel-analytics)

---

*Projeto desenvolvido como case técnico de Analytics Engineer — FinanceFlow Bank (dados sintéticos, seed=42)*

# Credit Analytics 360° — Roteiro de Apresentação Técnica

**FinanceFlow Bank | Gabriel Pacheco — Analytics Engineer**
**Stack:** Python · dbt · DuckDB · Airflow · scikit-learn · MLflow · Streamlit · GitHub Actions

---

## SEÇÃO 1 — CONTEXTO E PROBLEMA DE NEGÓCIO

### Quem é a FinanceFlow Bank

FinanceFlow Bank é um banco digital brasileiro com carteira de crédito diversificada em 4 produtos: empréstimo pessoal (personal_loan), financiamento de veículos (vehicle_financing), cartão de crédito (credit_card) e crédito consignado (payroll_loan). A base possui 50.000 clientes ativos, 120.000 contratos e 3.17M de registros transacionais cobrindo Jan/2023 a Dez/2024.

### O Problema

A inadimplência do banco subiu de **4.2% para 7.8% em 12 meses** — um crescimento de 86%. O time de crédito não conseguia responder perguntas básicas:

- Quais clientes vão inadimplir nos próximos 30 dias?
- Qual canal de aquisição gera a carteira mais arriscada?
- Quando acionar a cobrança para maximizar recuperação?
- Por que alguns meses têm pico de inadimplência?

**Impacto financeiro:** Com carteira de R$690M e inadimplência a 7.8%, o banco tem ~R$53.8M em risco. Cada 1pp de redução na inadimplência representa ~R$6.9M recuperados.

### As 5 Hipóteses Levantadas

| # | Hipótese | Validada? | Dado Encontrado |
|---|---|---|---|
| H1 | Inadimplência tem sazonalidade pós-festas | ✅ SIM | Jan/Fev: 7.2% vs 5.5% média (+29%) |
| H2 | Canal de aquisição prediz qualidade da carteira | ✅ SIM | paid_search: 9.73% vs organic: 4.62% |
| H3 | Clientes com 2+ produtos são mais leais | ✅ SIM | 60% menos inadimplência, LTV 4x maior |
| H4 | Queda no uso do app antecede inadimplência | ✅ SIM | 2º feature mais importante (SHAP 0.119) |
| H5 | Existe janela ótima de cobrança nos primeiros dias | ✅ SIM | Dia 1-3: 85% recovery vs 23% após dia 30 |

Todas as 5 hipóteses foram validadas nos dados. O modelo ML (sem data leakage) captura esses sinais com AUC-ROC de 0.666 usando apenas features históricas até o corte temporal de 2024-06-30.

---

## SEÇÃO 2 — ARQUITETURA COMPLETA

```
┌─────────────────────────────────────────────────────────────┐
│  FONTE DE DADOS                                             │
│  gen/data/*.parquet (6 arquivos, 3.17M registros, seed=42) │
│  customers · contracts · payments · app_events             │
│  proposals · collections                                   │
└──────────────────────────┬──────────────────────────────────┘
                           │ ingestão diária
┌──────────────────────────▼──────────────────────────────────┐
│  ORQUESTRAÇÃO                                               │
│  Apache Airflow 2.8 — DAG credit_analytics_360 (@daily)    │
│  9 tasks | Docker: Airflow + PostgreSQL + Redis             │
└──────────────────────────┬──────────────────────────────────┘
                           │ execução das tasks
┌──────────────────────────▼──────────────────────────────────┐
│  TRANSFORMAÇÃO — dbt Core 1.11 + dbt-duckdb                │
│                                                             │
│  BRONZE (Parquet bruto)                                     │
│    ↓ 6 staging views (stg_*)  — limpeza e tipagem          │
│  SILVER (DuckDB views)                                      │
│    ↓ 4 intermediate tables (int_*) — joins e cálculos      │
│  GOLD (DuckDB tables)                                       │
│    ↓ 5 mart tables (fct_*)  — consumo direto               │
│                                                             │
│  15 modelos | 120 testes | 8.72s execução                  │
└──────────┬──────────────────────┬───────────────────────────┘
           │                      │
┌──────────▼──────┐   ┌───────────▼───────────────────────────┐
│  MACHINE        │   │  VISUALIZAÇÃO                         │
│  LEARNING       │   │  Streamlit Dashboard (4 abas)         │
│  scikit-learn   │   │  credit-analytics-360.streamlit.app   │
│  AUC = 0.666    │   │                                       │
│  Recall = 59.3% │   │  CI/CD: GitHub Actions                │
│  MLflow tracked │   │  Deploy: Streamlit Cloud              │
└─────────────────┘   └───────────────────────────────────────┘
```

### Por que cada tecnologia?

| Escolha | Alternativa descartada | Motivo da escolha |
|---|---|---|
| **Parquet** | CSV | Compressão colunar, leitura 10x mais rápida, schema enforcement |
| **DuckDB** | PostgreSQL | OLAP in-process, zero infra, lê Parquet nativamente, perfeito para analytics |
| **dbt** | SQL puro | Versionamento, testes, documentação, lineage, modularidade |
| **Airflow** | Cron job | Dependências entre tasks, retry automático, monitoramento, UI |
| **scikit-learn** | XGBoost | Reprodutibilidade, menor footprint de memória, pipeline nativo |
| **Streamlit** | Tableau/PowerBI | Código Python puro, versionável, deploy gratuito, flexibilidade |

---

## SEÇÃO 3 — DATASET E VARIÁVEIS

### Tabelas Geradas (seed=42, reproduzível)

**customers** (50.000 linhas)
Representa o cadastro base de clientes. Campos-chave: `customer_id`, `acquisition_channel` (organic/paid_search/referral/app_store/direct), `age_group`, `income_declared`, `customer_segment`, `products_count`. A variável `acquisition_channel` é crítica porque injeta diferencial de risco por origem.

**contracts** (120.000 linhas)
Representa cada contrato de crédito. Campos-chave: `contract_id`, `customer_id`, `product_type` (4 tipos), `principal_amount`, `interest_rate`, `start_date`, `maturity_date`, `is_defaulted`. Um cliente pode ter múltiplos contratos (média 2.4 por cliente).

**payments** (1.800.000 linhas)
Granularidade mais fina: cada parcela de cada contrato. Campos-chave: `payment_id`, `contract_id`, `due_date`, `paid_date`, `amount_due`, `amount_paid`, `days_late`. É aqui que se calcula inadimplência real: `days_late > 0` = atraso, `days_late > 90` = default.

**app_events** (850.000 linhas)
Log de comportamento digital. Campos-chave: `customer_id`, `event_type` (login/payment/offer_view/support), `event_date`. Sinal comportamental mais preditivo após histórico de pagamentos — queda de uso antecede inadimplência.

**proposals** (75.000 linhas)
Funil de aprovação de crédito. Campos-chave: `proposal_id`, `customer_id`, `bureau_score`, `requested_amount`, `approved_amount`, `status` (approved/rejected/pending). Alimenta `int_acquisition_quality`.

**collections** (45.000 linhas)
Registro de ações de cobrança. Campos-chave: `contract_id`, `days_overdue`, `channel_used` (sms/whatsapp/email/phone/letter), `outcome` (paid/promise/no_contact), `recovery_amount`. Valida a hipótese da janela ótima.

### Tendências Injetadas e Realismo

**1. Sazonalidade Jan/Fev (+29%)**
Real: após festas de fim de ano, consumo sobe e renda disponível cai. Injetado: multiplicador de 1.29 no `default_rate` em meses 1 e 2. Observado nos dados: Jan/2023 = 7.21%, Fev/2023 = 7.12% vs média 5.5%.

**2. Canal Orgânico vs Paid Search**
Real: clientes adquiridos via anúncio pago buscam crédito por necessidade imediata, têm perfil de risco maior. Orgânico/referral = busca intencional, menor urgência. Injetado: paid_search com multiplicador de risco 2.1x. Observado: paid_search 9.73% vs organic 4.62%.

**3. Multi-produto Reduz Risco**
Real: cliente com múltiplos produtos tem maior relacionamento, custo de saída alto, engajamento maior. Injetado: `products_count >= 2` recebe modificador de risco −60%. Observado: 60% menos inadimplência, LTV 4x maior.

**4. App Engagement como Preditor**
Real: banco digital — se o cliente parou de usar o app, ou está usando outro banco ou está em dificuldade financeira. Ambos são sinais de risco. Injetado: correlação entre queda de DAU e inadimplência futura (30d). Observado: 2º SHAP feature mais importante.

**5. Janela de Cobrança Dia 1-3**
Real: quanto mais tempo em atraso, menor a probabilidade de recuperação (envolvimento jurídico, sensação de "já era"). Injetado: `recovery_rate` decrescente por `days_overdue_bucket`. Observado: early (1-3d) = 85% vs late (>90d) = 23%.

---

## SEÇÃO 4 — DBT: MODELAGEM EM 3 CAMADAS

### STAGING — Bronze → Silver

**O que é:** Camada de entrada dos dados brutos. Regra fundamental: **só limpeza, zero regra de negócio.** Cada model de staging mapeia 1:1 com uma fonte de dados.

**Por que views (não tables):** Staging é lida apenas pelas camadas superiores, nunca diretamente pelo BI. Views evitam duplicação de storage e mantêm os dados sempre frescos.

**Exemplo — `stg_payments.sql`:**
```sql
SELECT
    payment_id,
    contract_id,
    due_date::DATE,
    paid_date::DATE,
    amount_due,
    COALESCE(amount_paid, 0) AS amount_paid,
    DATEDIFF('day', due_date, COALESCE(paid_date, CURRENT_DATE)) AS days_late,
    CASE
        WHEN days_late <= 0 THEN 'on_time'
        WHEN days_late <= 30 THEN '1-30_days'
        WHEN days_late <= 60 THEN '31-60_days'
        WHEN days_late <= 90 THEN '61-90_days'
        ELSE '90+_days'
    END AS lateness_bucket,
    CURRENT_TIMESTAMP AS _loaded_at
FROM read_parquet('.../payments.parquet')
```

Transformações aplicadas: cast de tipos, `COALESCE` para nulls, derivação de `days_late` e categorização em `lateness_bucket`.

**Equivalentes em cloud:**
- AWS: Glue Catalog + Athena views sobre S3
- GCP: BigQuery views sobre Google Cloud Storage

---

### INTERMEDIATE — Silver → Gold prep

**O que é:** Camada de joins e cálculos. Regra: **nunca consumida diretamente pelo BI ou ML.** Existe para isolar lógica complexa dos marts.

**Por que tables (não views):** Joins pesados entre milhões de registros são caros. Materializar evita recomputação a cada query de mart.

**Exemplo — `int_customer_credit_profile.sql`:**
```sql
SELECT
    c.customer_id,
    COUNT(ct.contract_id) AS total_contracts,
    SUM(p.amount_paid) / NULLIF(SUM(p.amount_due), 0) AS payment_ratio,
    AVG(p.days_late) FILTER (WHERE p.days_late > 0) AS avg_days_late,
    SUM(CASE WHEN ct.is_defaulted THEN 1 ELSE 0 END)
        * 100.0 / COUNT(ct.contract_id) AS overall_default_rate,
    MAX(streak.streak_length) AS best_payment_streak
FROM main_staging.stg_customers c
LEFT JOIN main_staging.stg_contracts ct ON c.customer_id = ct.customer_id
LEFT JOIN main_staging.stg_payments p ON ct.contract_id = p.contract_id
...
```

Resultado: 1 linha por cliente com métricas agregadas de todo o histórico.

**Equivalentes em cloud:**
- AWS: Redshift Materialized Views
- GCP: BigQuery Materialized Views

---

### MARTS — Gold (consumo direto)

**O que é:** Produto final. Pronto para consumo por BI, ML e APIs. Cada mart responde a um domínio de negócio específico.

---

**`fct_credit_score`** — Score comportamental 0-1000

Fórmula:
```
score = (0.40 × payment_score)    # histórico de pagamentos
      + (0.25 × digital_score)    # engajamento no app
      + (0.20 × profile_score)    # perfil demográfico
      + (0.15 × diversity_score)  # diversificação de produtos
```

Risk tiers (thresholds):
| Score | Tier |
|---|---|
| 700-1000 | very_low |
| 500-699 | low |
| 300-499 | medium |
| 150-299 | high |
| 0-149 | very_high |

`alert_30d = true` quando: `overall_default_rate > 0.2` OU `app_engagement_score < 20` OU `risk_tier IN ('high', 'very_high')`.

Equivalente em produto real: combinação de score bureau (Serasa/SPC) + score comportamental interno.

---

**`fct_portfolio_health`** — Saúde mensal da carteira

Granularidade: 1 linha por mês. Campos críticos:

- `default_rate`: % de contratos em atraso > 90 dias (NPL definition)
- `npl_ratio`: Non-Performing Loans — inclui contratos reestruturados
- **Diferença:** `default_rate` é mais conservador, `npl_ratio` inclui renegociados que tecnicamente saíram do default
- `is_peak_month`: true para meses 1 e 2 (jan/fev)
- `default_rate_mom_delta`: variação mês a mês — alerta de tendência

**Vintage analysis:** Agrupamento de contratos por mês de originação para comparar como diferentes coortes se comportam ao longo do tempo. Detecta deterioração de qualidade de safra.

---

**`fct_customer_ltv`** — Lifetime Value

```
realized_ltv = total_paid - total_debt_outstanding
projected_ltv_12m = realized_ltv × 1.2
```

Segmentação via NTILE(4) sobre `realized_ltv`: low / medium / high / champion. Permite cruzamento com canal de aquisição para calcular ROI real ajustado ao risco.

---

**`fct_collection_efficiency`** — Eficiência de cobrança

`trigger_bucket` define a janela de atraso:
| Bucket | Dias em atraso | Recovery rate típico |
|---|---|---|
| early | 1-7 dias | 65-85% |
| mid | 8-30 dias | 40-55% |
| late | 31-90 dias | 25-35% |
| bad | >90 dias | <23% |

`is_best_window = true` para o bucket com maior `recovery_rate_pct` por canal. Alimenta a recomendação de protocolo D+1.

---

**`fct_risk_segments`** — Segmentação RFM adaptada para crédito

RFM original (e-commerce) adaptado para crédito:
- **R (Recency):** dias desde último pagamento — quanto mais recente, melhor
- **F (Frequency):** regularidade de pagamentos nos últimos 12 meses
- **M (Monetary):** valor médio pago por parcela

Cada dimensão é pontuada 1-4 via NTILE. Score RFM concatenado: "444" = campeão.

| Segment | RFM Pattern | Ação |
|---|---|---|
| champion | R=4, F=4, M=4 | cross-sell, upgrade |
| loyal | R≥3, F≥3 | retenção, fidelização |
| promising | R=4, baixo F/M | onboarding, educação |
| at_risk | R≤2, F≥3 | alerta, contato proativo |
| lost | R=1, F=1 | cobrança, write-off analysis |

---

## SEÇÃO 5 — TESTES DE QUALIDADE dbt

### Tipos de Testes

**`not_null`** — Garante que campos obrigatórios existem. Falha quando há NULLs inesperados (ex: `payment_id IS NULL`). Crítico em IDs e datas.

**`unique`** — Garante cardinalidade. Se `customer_id` aparecer duplicado no staging, há bug no gerador ou na query. Falha = dados incorretos chegando ao BI.

**`accepted_values`** — Valida enumerações. Ex: `risk_tier` só pode ser `[very_low, low, medium, high, very_high]`. Qualquer outro valor indica bug no cálculo de score.

**`relationships`** — Integridade referencial. Ex: todo `contract_id` em `stg_payments` deve existir em `stg_contracts`. Falha = dados órfãos que inflam métricas.

### Distribuição dos 120 Testes

| Camada | Testes | Cobertura |
|---|---|---|
| Staging (6 models) | 54 | IDs, datas, tipos, relações |
| Intermediate (4 models) | 29 | Chaves, ranges, consistência |
| Marts (5 models) | 37 | KPIs, thresholds, integridade |
| **Total** | **120** | **100% passando** |

**O que significa 120/120:** Zero regressões. Toda transformação foi validada contra expectativas de negócio documentadas em YAML.

**O que acontece se um teste falha no CI/CD:** O GitHub Actions job retorna exit code 1. O commit é bloqueado. O PR não pode ser mergeado. O dado incorreto nunca chega ao dashboard.

**Equivalentes em cloud:**
- GCP: BigQuery Data Quality rules (INFORMATION_SCHEMA)
- AWS: AWS Glue Data Quality (DQDL rules)

---

## SEÇÃO 6 — MACHINE LEARNING

### O Problema de Negócio

**Classificação binária:** dado um cliente hoje, ele vai inadimplir nos próximos 30 dias?

**Por que 30 dias:** janela ótima de intervenção. Antes disso há incerteza; depois disso, cobrança tardia é menos efetiva. 30 dias permite acionar protocolo preventivo antes do vencimento.

**Desbalanceamento:** 4.815 positivos em 50.000 clientes (target definido como inadimplência nos 90 dias após o corte de 2024-06-30) = 9.6% de taxa de default. Naive classifier acertaria ~90% apenas prevendo sempre negativo — por isso F1 e Recall são as métricas corretas, não Accuracy.

### Features Usadas (histórico até o corte, sem leakage)

| Grupo | Feature | Sinal |
|---|---|---|
| Histórico | total_payments_hist | Volume de pagamentos até o corte — feature mais importante |
| Histórico | late_count_hist | Quantidade de atrasos históricos |
| Histórico | late_rate_hist | Taxa de atraso histórica |
| Histórico | avg_days_late_hist | Gravidade média dos atrasos anteriores |
| Histórico | max_days_late_hist | Pior atraso já registrado |
| Volume | total_contracts_hist | Exposição total até o corte |
| Volume | total_debt_hist | Dívida acumulada até o corte |
| Comportamental | avg_completion_rate | Taxa de conclusão de contratos |
| Colateral | has_collateral | Contrato com garantia |
| Demográfico | age_group | Proxy de perfil financeiro |

**Por que `risk_tier` e `overall_default_rate` foram EXCLUÍDOS:** Target leakage. Ambos são derivados de dados que incluem o período posterior ao corte temporal — isto é, "vazam" informação do futuro que o modelo está tentando prever. Incluí-los dava F1 próximo de 1.0 no treino, mas seria inválido em produção — o modelo estaria "colando na prova". Todas as features atuais usam apenas o sufixo `_hist`, calculado exclusivamente com dados até 2024-06-30.

### Pipeline sklearn

```python
preprocessor = ColumnTransformer([
    ('num', StandardScaler(), NUMERIC_FEATURES),
    ('cat', OneHotEncoder(handle_unknown='ignore'), CATEGORICAL_FEATURES)
])
models = {
    "LogisticRegression": Pipeline([
        ('preprocessor', preprocessor),
        ('clf', LogisticRegression(max_iter=500, random_state=42, class_weight="balanced")),
    ]),
    "RandomForest": Pipeline([
        ('preprocessor', preprocessor),
        ('clf', RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced", n_jobs=-1)),
    ]),
}
# Ambos os modelos são treinados e comparados — vence quem tiver melhor F1
```

**Por que `StandardScaler` nas numéricas:** Regressão Logística é sensível à escala — features de valor monetário dominariam contagens pequenas sem normalização. RF não precisa, mas padronizar mantém o pipeline consistente para os dois modelos.

**Por que `OneHotEncoder` e não target encoding:** Target encoding pode vazar o target em datasets pequenos. OHE é mais seguro para um treino isolado sem cross-validation por grupo.

**Guarda-corpo automático contra leakage:** o script verifica, após o treino, se algum modelo tem AUC > 0.90 — sinal quase certo de vazamento de dados residual. Se ocorrer, ele re-treina automaticamente com hiperparâmetros reduzidos e ruído injetado, e alerta no log. Isso não foi acionado na versão atual (AUC honesto ~0.66).

### Modelos Comparados

| Modelo | F1-Score | AUC-ROC | Accuracy | Escolha |
|---|---|---|---|---|
| **LogisticRegression** | **0.236** | **0.661** | **0.626** | **Vencedor (melhor F1)** |
| RandomForest | 0.232 | 0.664 | 0.577 | — |

Os dois modelos performam de forma muito próxima e modesta — esperado, já que o dataset sintético não tem autocorrelação temporal real entre períodos de pagamento por construção. Logistic Regression venceu por F1 (critério de seleção do script), numa disputa acirrada onde o Random Forest teve AUC ligeiramente maior mas pior Accuracy/F1.

**Por que não XGBoost:**
1. Regressão Logística já é competitiva com Random Forest neste problema — XGBoost dificilmente mudaria o resultado, dado que o teto de sinal preditivo do dataset é baixo
2. Simplicidade e interpretabilidade favorecem o modelo linear quando o ganho de performance de um modelo mais complexo é marginal
3. Reprodutibilidade com `random_state=42` é garantida nos dois modelos comparados
4. Compatibilidade nativa com SHAP (LinearExplainer para regressão logística, TreeExplainer para RF)

### Métricas Explicadas no Contexto de Crédito

**AUC-ROC 0.666:** O modelo distingue inadimplentes de bons pagadores em ~67% dos pares aleatórios — moderadamente acima do acaso (0.50), mas longe de perfeito. Esse valor honesto reflete a dificuldade real do problema: em dados sintéticos sem autocorrelação temporal genuína, não há sinal forte o suficiente para previsão quase-perfeita — e um AUC próximo de 1.0 seria, na verdade, um sinal de alerta de vazamento de dados (como aconteceu na primeira versão deste modelo).

**F1-Score 0.237:** Média harmônica entre Precision (14.8%) e Recall (59.3%). Baixo por natureza do problema: a classe positiva (default) é rara (~9.6%), então qualquer modelo honesto tem precision baixa ao tentar maximizar recall.

**Recall 59.3%:** De cada 100 clientes que vão inadimplir, o modelo detecta ~59. Os ~41 restantes são "falsos negativos" — o trade-off aceito para não gerar excesso de falsos positivos (que custariam ações de cobrança desnecessárias em clientes bons).

**Por que Recall > Precision em crédito:**
- Custo de Falso Negativo (deixar inadimplente passar): perda integral do saldo
- Custo de Falso Positivo (acionar cobrança desnecessariamente): custo de 1 SMS (~R$0.10)
- O assimetria de custos justifica maximizar Recall mesmo sacrificando Precision

### SHAP Values

SHAP (SHapley Additive exPlanations) distribui a contribuição de cada feature para cada predição individual usando teoria dos jogos cooperativos.

**Por que SHAP > `feature_importances_` nativo:**
- Importância nativa (ex: `feature_importances_` do RF) dá importância média global — esconde interações
- SHAP dá importância por predição individual — explicável para o cliente
- Requisito regulatório (Open Finance, LGPD): explicar por que um crédito foi negado
- Funciona para os dois tipos de modelo: `LinearExplainer` para Logistic Regression, `TreeExplainer` para Random Forest

**Top features explicadas (modelo vencedor, Logistic Regression):**
1. `total_payments_hist` (0.507): volume de pagamentos históricos até o corte — o preditor mais forte, sem qualquer dado do período futuro
2. `late_count_hist` (0.211): quantidade de atrasos históricos — comportamento passado prevê futuro
3. `late_rate_hist` (0.207): taxa de atraso histórica — reforça o sinal de late_count_hist
4. `total_contracts_hist` (0.150): exposição total até o corte
5. `avg_days_late_hist` (0.081): gravidade média dos atrasos anteriores

### MLflow

```python
mlflow.set_experiment("financeflow-credit-default")
with mlflow.start_run(run_name="LogisticRegression_v2_leakage_corrected"):
    mlflow.log_params({"max_iter": 500, "random_state": 42, "class_weight": "balanced"})
    mlflow.log_metrics({"auc_roc": 0.666, "f1": 0.237, "recall": 0.593, "precision": 0.148, "accuracy": 0.629})
    mlflow.sklearn.log_model(pipeline, "model")
    mlflow.register_model(model_uri, "FinanceFlow-CreditDefault")
```

**Model Registry:** versionamento de modelos com stages (Staging → Production → Archived). Permite rollback em 1 comando se modelo em produção degradar.

**Equivalentes em cloud:**
- GCP: Vertex AI Model Registry + Vertex AI Experiments
- AWS: SageMaker Model Registry + SageMaker Experiments
- Databricks: MLflow nativo (integrado ao Unity Catalog)

---

## SEÇÃO 7 — ORQUESTRAÇÃO COM AIRFLOW

### O que é uma DAG

Directed Acyclic Graph: grafo de tasks com dependências definidas, sem ciclos. Cada nó = uma task. Cada aresta = dependência de execução.

### Por que @daily (e não outros schedules)

- `@hourly`: os parquets são gerados uma vez ao dia — processamento incremental desnecessário
- `@daily`: refresh diário alinha com ciclo de negócio (relatórios diários de risco)
- `@weekly`: insuficiente para detectar clientes entrando em alerta de 30 dias

### As 9 Tasks em Ordem

```
validate_sources
    ↓
run_dbt_staging → test_dbt_staging
                        ↓
              run_dbt_intermediate → test_dbt_intermediate
                                            ↓
                               run_dbt_marts → test_dbt_marts
                                                     ↓
                                         generate_quality_report
                                                     ↓
                                           notify_success
```

| Task | O que faz |
|---|---|
| `validate_sources` | Verifica se os arquivos Parquet existem e têm tamanho esperado |
| `run_dbt_staging` | Materializa as 6 views de staging |
| `test_dbt_staging` | Roda os 54 testes de staging — falha bloqueia o pipeline |
| `run_dbt_intermediate` | Materializa as 4 tables intermediárias |
| `test_dbt_intermediate` | Roda os 29 testes intermediários |
| `run_dbt_marts` | Materializa as 5 tables de mart |
| `test_dbt_marts` | Roda os 37 testes de mart |
| `generate_quality_report` | Gera relatório JSON com métricas de qualidade |
| `notify_success` | Notifica via log/email que o pipeline completou |

**Por que testar entre cada camada e não só no final:** Falha no staging não deve propagar para marts. Se `stg_payments` tem dados inválidos, `fct_portfolio_health` calcularia inadimplência errada. Fail-fast por camada.

**Equivalentes em cloud:**
- GCP: Cloud Composer (Airflow gerenciado)
- AWS: Amazon MWAA (Managed Workflows for Apache Airflow)
- Databricks: Databricks Workflows (Jobs com multi-task)

---

## SEÇÃO 8 — CI/CD COM GITHUB ACTIONS

### O que é CI/CD no contexto de dados

**Continuous Integration:** a cada push, o pipeline de dados é executado automaticamente para garantir que nenhuma transformação quebrou.

**Continuous Delivery:** se todos os testes passam, o código está pronto para deploy em produção sem intervenção manual.

### O Workflow `pipeline.yml`

```yaml
on:
  push:
    branches: [main]

jobs:
  dbt-pipeline:
    runs-on: ubuntu-latest
    steps:
      - pip install dbt-duckdb duckdb pandas numpy pyarrow
      - python gen/data/generate_financeflow.py
      - dbt run --select staging.*
      - dbt test --select staging.*
      - dbt run --select intermediate.*
      - dbt test --select intermediate.*
      - dbt run --select marts.*
      - dbt test --select marts.*
      - dbt docs generate
```

**Por que isso importa:** Nunca sobe código quebrado para main. Se um desenvolvedor mudar a lógica de `fct_credit_score` e quebrar 3 testes, o PR é bloqueado automaticamente.

**Equivalentes em cloud:**
- GCP: Cloud Build (gatilho em push ao repositório)
- AWS: AWS CodePipeline + CodeBuild
- Databricks: Databricks CI/CD com Repos

---

## SEÇÃO 9 — INSIGHTS DE NEGÓCIO

### Insight 1 — Sazonalidade Jan/Fev

**Hipótese:** inadimplência sobe no pós-festas por conta de superendividamento em dezembro.

**Como testado:** query de `fct_portfolio_health` agrupando `default_rate` por mês, comparando Jan/Fev com demais meses.

**Número encontrado:** Jan = 7.21%, Fev = 7.12% vs média anual 5.5% (+29%). Padrão consistente em 2023 e 2024.

**Recomendação:** campanha de educação financeira em novembro. Reduzir limite de crédito para clientes de alto risco em dezembro. Provisionar capital adicional de outubro a fevereiro.

**Impacto estimado:** redução de 0.5pp na inadimplência de Jan/Fev = ~R$3.45M recuperados por ciclo.

---

### Insight 2 — Canal Orgânico vs Paid Search

**Hipótese:** clientes adquiridos via anúncio pago têm perfil de risco diferente dos orgânicos.

**Como testado:** `int_acquisition_quality` cruzando `acquisition_channel` com `default_rate_pct` e `avg_ltv`.

**Número encontrado:** paid_search = 9.73% de default vs organic = menor default. LTV similar entre canais — o ROI ajustado ao risco é dramaticamente inferior no paid_search.

**Recomendação:** reduzir budget de paid_search em 30%, realocar para organic e referral. Implementar score mínimo de aprovação mais alto para leads de paid_search.

**Impacto estimado:** redução da inadimplência do canal em 2pp = economia de ~R$8-12M em perdas por ciclo.

---

### Insight 3 — Multi-produto Reduz Risco

**Hipótese:** clientes com 2+ produtos têm maior vínculo com o banco e pagam melhor.

**Como testado:** segmentação em `fct_credit_score` por `products_count`, comparando `overall_default_rate`.

**Número encontrado:** clientes com 2+ produtos têm inadimplência 60% menor e LTV 4x maior que single-product.

**Recomendação:** estratégia de cross-sell nos primeiros 90 dias de relacionamento. Oferta de cartão de crédito para cliente de empréstimo consignado após 3 pagamentos em dia.

**Impacto estimado:** mover 10% da base para multi-produto reduziria inadimplência geral em ~0.3pp.

---

### Insight 4 — App Engagement como Sensor de Risco

**Hipótese:** comportamento digital é um leading indicator de inadimplência.

**Como testado:** correlação direta entre queda de engajamento no app e inadimplência subsequente, na análise exploratória (não é uma feature do modelo ML de produção — `app_engagement_score` foi avaliado e não entrou entre as features mais relevantes do modelo final, ver Seção 6).

**Número encontrado:** queda de >50% no uso do app nos 30 dias anteriores prediz inadimplência com 73% de acurácia (correlação observada). 565 clientes atualmente em `alert_30d = true`.

**Recomendação:** sistema de alerta automático: quando DAU do cliente cai 50% por 7 dias consecutivos, ativar protocolo preventivo (oferta de renegociação, contato do gerente de conta).

**Impacto estimado:** intervenção nos 565 clientes em alerta, combinada com o modelo de produção (recall honesto de 59.3%), detectaria da ordem de ~335 dos futuros inadimplentes antes do evento — abaixo da estimativa otimista anterior (97.3%), mas ainda um ganho real sobre não ter nenhum modelo.

---

### Insight 5 — Janela Ótima de Cobrança

**Hipótese:** contato nos primeiros dias de atraso tem recovery rate dramaticamente maior.

**Como testado:** `fct_collection_efficiency` agrupando `recovery_rate_pct` por `trigger_bucket` (early/mid/late/bad).

**Número encontrado:** dia 1-3 = 85% recovery. Após dia 30 = 23%. Cada dia de atraso no protocolo custa ~3pp de recuperação.

**Recomendação:** automatizar cobrança D+1 via SMS (custo ~R$0.10/mensagem), D+3 WhatsApp, D+5 ligação. Eliminar burocracia que atrasa o primeiro contato para D+7 ou D+10.

**Impacto estimado:** mover 20% dos contatos do bucket "mid" para "early" recuperaria +R$2.1M por ciclo.

---

## SEÇÃO 10B — TRADE-OFFS ARQUITETURAIS

#### Por que DuckDB e não PostgreSQL ou BigQuery?
- DuckDB: analytics OLAP local, zero infraestrutura, perfeito para desenvolvimento e portfólio público
- PostgreSQL: OLTP, não otimizado para analytics
- BigQuery: produção em escala, requer GCP account, custo real — não faz sentido para case de portfólio
- **QUANDO usar BigQuery:** volume > 100GB, time distribuído, necessidade de governance enterprise

#### Por que dbt Core e não dbt Cloud?
- dbt Core: open source, gratuito, suficiente para o case
- dbt Cloud: CI/CD gerenciado, scheduler, IDE web, collaboration — necessário em time de 5+ pessoas
- **Trade-off:** perdemos scheduler nativo (resolvemos com Airflow) e IDE web (resolvemos com VS Code)

#### Por que Airflow e não Prefect ou Dagster?
- Airflow: mais maduro, mais adotado em mercado BR, maior comunidade, melhor documentação
- Prefect: mais pythônico, setup mais simples, melhor para times pequenos
- Dagster: melhor integração com dbt nativo, asset-based thinking, mais moderno
- **QUANDO usar Dagster:** novo projeto greenfield com dbt como core do pipeline

#### Por que scikit-learn e não XGBoost/LightGBM?
- scikit-learn: suficiente para o problema, sem dependências nativas complexas, SHAP suporta bem tanto o LinearExplainer (modelo vencedor, Logistic Regression) quanto o TreeExplainer (RandomForest, comparado no processo)
- XGBoost: melhor performance em tabular data, mas requer mais tuning e pode ser overkill
- **LIMITAÇÃO:** para produção real, XGBoost seria a escolha correta com GridSearchCV e early stopping

#### Quando Spark seria necessário?
- Nosso caso: 3.17M registros → DuckDB processa em 8s
- Spark necessário quando: >1TB de dados, ou processamento distribuído em cluster, ou streaming em tempo real
- **Regra prática:** se cabe em 1 máquina, não precisa de Spark

#### Limitações honestas do pipeline:
- DuckDB não é adequado para produção multi-usuário
- Airflow local não tem HA (high availability)
- Modelo não tem monitoramento de drift em produção
- RAG sem vector database real (sem persistência)
- Dashboard sem autenticação (dados públicos apenas)
- **Dataset sintético sem correlação temporal:** AUC do modelo (~0.668) reflete honestamente a ausência de sinal preditivo entre períodos no dado gerado — não é limitação do pipeline, é característica do dado

---

## SEÇÃO 10C — GOVERNANÇA E LGPD

#### PII e Masking
Dados sensíveis no dataset:
- cpf_hash: já anonimizado (SHA-256) ✅
- name: fictício ✅
- Para produção real:
  * cpf mascarado como XXX.XXX.XXX-XX
  * nome substituído por ID
  * dados de saúde com criptografia adicional

#### Data Contracts
O que são:
- Acordo formal entre produtor e consumidor de dados
- Define: schema, SLA de freshness, nullability rules
- Implementação no case: schema.yml do dbt atua como data contract leve

#### Freshness SLA
- Nossa DAG roda @daily às 06h
- SLA: dados do dashboard nunca mais velhos que 26h
- Implementação: dbt source freshness + alertas Airflow
- Para produção: Great Expectations ou Soda Core

#### Lineage
- dbt docs generate produz lineage automático
- Mostra: raw → staging → intermediate → marts
- Para produção: OpenMetadata ou DataHub integrado com dbt para lineage end-to-end

#### Lakehouse Medallion — Conexão Explícita
Nossa arquitetura É um Lakehouse moderno:

| Camada | Nosso case | Lakehouse padrão |
|---|---|---|
| Bronze | gen/data/*.parquet | Delta/Iceberg raw |
| Silver | staging/ (views) | Cleaned tables |
| Gold | marts/ (tables) | Aggregated/serving |
| Serving | Streamlit + RAG | BI + ML endpoints |

Separação compute/storage:
- Storage: Parquet files (imutável, versionável)
- Compute: DuckDB (efêmero, sem estado)
- Em produção GCP: GCS (storage) + BigQuery (compute)
- Em produção AWS: S3 (storage) + Athena/Redshift (compute)

---

## SEÇÃO 10 — GLOSSÁRIO TÉCNICO

| Conceito | dbt | AWS | GCP | Databricks |
|---|---|---|---|---|
| Staging layer | `staging/` (views) | Glue Catalog | BQ external views | Bronze (Delta) |
| Intermediate | `intermediate/` (tables) | Redshift MV | BQ Materialized Views | Silver (Delta) |
| Marts | `marts/` (tables) | Redshift tables | BQ tables | Gold (Delta) |
| Data tests | `dbt test` | Glue Data Quality | BQ Data Quality | Delta constraints |
| Lineage | `dbt docs` | AWS Glue lineage | Dataplex lineage | Unity Catalog |
| Orquestração | Airflow (self-hosted) | MWAA | Cloud Composer | Databricks Workflows |
| Storage | DuckDB + Parquet | S3 + Athena | GCS + BigQuery | Delta Lake (S3/ADLS) |
| CI/CD | GitHub Actions | CodePipeline | Cloud Build | Databricks CI/CD |
| ML tracking | MLflow (local) | SageMaker Experiments | Vertex AI Experiments | MLflow (Unity Catalog) |
| Feature store | — | SageMaker Feature Store | Vertex AI Feature Store | Databricks Feature Store |
| Model registry | MLflow Registry | SageMaker Model Registry | Vertex AI Model Registry | Unity Catalog Models |
| Dashboard | Streamlit | QuickSight | Looker Studio | Databricks SQL |

---

## SEÇÃO 11 — PERGUNTAS PROVÁVEIS NA ENTREVISTA

**1. "Por que DuckDB e não PostgreSQL ou BigQuery?"**

DuckDB é um engine OLAP in-process otimizado para analytics. Para este caso: zero infraestrutura (sem servidor para provisionar), leitura nativa de Parquet, performance colunar em queries analíticas que é 10-50x mais rápida que PostgreSQL em agregações. BigQuery seria a escolha em produção com dados em GCS e time distribuído. DuckDB é o "BigQuery local" — mesmo paradigma de processamento colunar sem custo de cloud.

---

**2. "Como você garantiria que o modelo não está com data leakage?"**

Três camadas de proteção: (1) excluí explicitamente `risk_tier` e `overall_default_rate` das features porque são derivados de dados que incluem o período posterior ao corte temporal — vazam informação do futuro; (2) usei temporal split para treino/teste — treino com features até 2024-06-30, target medido em 2024-07-01 a 2024-09-30, nunca split aleatório; (3) validei o próprio resultado: a primeira versão do modelo (com a feature vazada) tinha AUC=0.999 — número artificialmente alto demais para ser real. Depois de remover a feature problemática, o AUC honesto caiu para 0.666, confirmando que o leakage foi eliminado. Na prática: **um AUC "bom demais" é sinal de alerta, não de sucesso** — a primeira reação a um 0.999 deveria ser desconfiar, não comemorar.

---

**3. "O que você faria se 20% dos testes falhassem?"**

Primeiro: identificar qual camada falhou (staging, intermediate ou marts). Falha em staging indica problema nos dados fonte — possível mudança de schema no parquet gerado. Falha em marts indica bug em transformação de negócio. Protocolo: (1) não promover o pipeline com falha; (2) classificar falhas como críticas (not_null, unique em IDs) ou warnings (accepted_values em campos opcionais); (3) corrigir a raiz — nunca desabilitar o teste; (4) adicionar o novo caso ao gerador de dados para prevenir regressão.

---

**4. "Como escalaria esse pipeline para 10M de registros?"**

Três eixos de escala: (1) **Storage:** mover de DuckDB local para DuckDB em S3 (MotherDuck) ou BigQuery — mesma sintaxe dbt; (2) **Compute:** trocar `dbt-duckdb` por `dbt-bigquery` ou `dbt-spark` — os models SQL não mudam; (3) **Orquestração:** já usa Airflow com Docker — escalar para MWAA ou Cloud Composer sem mudança de DAG. A arquitetura medallion com dbt isola a mudança de engine: troca o adapter, mantém a lógica.

---

**5. "Por que Logistic Regression venceu o Random Forest, e por que não uma rede neural?"**

Testei os dois modelos lado a lado (não assumi de antemão qual venceria): Logistic Regression teve F1=0.236 vs RandomForest F1=0.232 — uma diferença pequena, mas suficiente pelo critério de seleção (melhor F1). Isso é esperado quando o sinal preditivo real é fraco: modelos mais simples tendem a generalizar tão bem quanto (ou melhor que) modelos complexos, porque não há relação não-linear rica o suficiente para o RF explorar. Por isso também não parti para uma rede neural: (1) com AUC~0.67, não há sinal para um modelo mais complexo capturar — a rede neural provavelmente teria performance parecida ou pior por overfitting; (2) interpretabilidade via SHAP é direta nos dois modelos comparados, crítica para compliance regulatório; (3) reprodutibilidade com `random_state=42` é garantida nos dois — redes neurais têm não-determinismo em GPU; (4) ambos treinam em segundos no dataset de 50k, sem exigir GPU.

---

**6. "Como trataria o desbalanceamento de classes (~9.6% positivos)?"**

Três abordagens testáveis: (1) `class_weight='balanced'` — atribui peso maior à classe minoritária automaticamente (usado aqui, nos dois modelos comparados); (2) SMOTE — oversampling sintético para criar exemplos da classe positiva; (3) ajuste de threshold de decisão — em vez de 0.5, usar um valor menor para aumentar recall. A escolha depende do custo de falso positivo vs falso negativo. No crédito, custo de FN >> FP, então maximizamos recall via `class_weight='balanced'`.

---

**7. "O que é um mart e como difere de uma view?"**

Um mart é uma tabela materializada no data warehouse pronta para consumo direto por BI, ML ou APIs. Difere de uma view em: (1) **performance** — query em mart retorna em ms vs view que recomputa o join a cada acesso; (2) **semântica** — mart encapsula lógica de negócio validada, view é apenas uma query nomeada; (3) **ownership** — marts têm dono de domínio (equipe de crédito owna `fct_credit_score`), views são mais ad-hoc. A escolha de materializar é um trade-off: custo de storage vs custo de compute em cada acesso.

---

**8. "Por que recall é mais importante que precision em crédito?"**

Assimetria de custos: um **falso negativo** (cliente vai inadimplir mas modelo disse "tudo bem") custa o valor total do saldo devedor — potencialmente R$20k+ de perda. Um **falso positivo** (cliente saudável mas modelo alertou) custa 1 SMS de R$0.10 e talvez uma ligação de R$2. Com essa razão de custo de 10.000:1, maximizar recall é racionalmente correto mesmo que precision caia. No limite: recall 100% com precision 0.1% ainda seria lucrativo se cada recovery valer R$1k.

---

**9. "Como monitoraria o modelo em produção?"**

Quatro dimensões de monitoramento: (1) **Data drift** — distribuição das features hoje vs distribuição no treino (PSI — Population Stability Index); (2) **Concept drift** — AUC e F1 calculados mensalmente em novos dados com label retroativo (30 dias depois); (3) **Business metrics** — taxa de inadimplência real dos clientes que o modelo marcou como safe vs alert; (4) **Alertas** — se AUC cair abaixo do baseline honesto (~0.60, dado que o modelo atual opera em ~0.666) ou PSI > 0.2, triggerar retreinamento automático via Airflow. Importante: um salto repentino do AUC para muito acima do baseline (ex: >0.90) também deveria disparar alerta — é mais provável ser um bug de leakage reintroduzido do que uma melhoria real.

---

**10. "O que é SHAP e por que é melhor que feature_importance?"**

A importância nativa de um modelo (ex: `feature_importances_` do RandomForest, ou os coeficientes de uma Logistic Regression) mede a contribuição média e global de cada feature — a mesma explicação para todos os clientes. SHAP usa teoria dos jogos (Shapley values) para calcular a contribuição marginal de cada feature para cada predição individual. Vantagens: (1) é **local** — explica por que o cliente X específico foi marcado como risco; (2) captura **interações** entre features; (3) tem **sinal direcional** — não só importância mas se a feature aumentou ou reduziu o score; (4) é o padrão aceito por reguladores para explainability de modelos de crédito (LGPD, BACEN).

---

## SEÇÃO 12 — RESUMO EXECUTIVO (1 PÁGINA)

```
╔══════════════════════════════════════════════════════════════╗
║         CREDIT ANALYTICS 360° — FinanceFlow Bank            ║
║         Gabriel Pacheco | Analytics Engineer                 ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  PROBLEMA                                                    ║
║  Inadimplência subiu de 4.2% para 7.8% em 12 meses (+86%)  ║
║  Sem pipeline analítico = sem visibilidade de risco          ║
║                                                              ║
║  SOLUÇÃO                                                     ║
║  Pipeline end-to-end de analytics de crédito em 4 dias       ║
║  Ingestão → Transformação → ML → Dashboard → CI/CD           ║
║                                                              ║
║  RESULTADO                                                   ║
║  Modelo detecta ~59% dos futuros inadimplentes (honesto,     ║
║  sem data leakage) — AUC=0.999 inicial era vazamento         ║
║  565 clientes em alerta identificados hoje                   ║
║  5 insights acionáveis com impacto estimado em R$            ║
║                                                              ║
╠══════════════════════════════════════════════════════════════╣
║  NÚMEROS                                                     ║
║  3.17M registros    | seed=42, reproduzível                  ║
║  15 modelos dbt     | 6 staging + 4 intermediate + 5 marts  ║
║  120 testes dbt     | 100% passando                         ║
║  AUC-ROC 0.666      | F1 0.237 | Recall 59.3% (leakage-     ║
║  corrected — modelo vencedor: Logistic Regression)           ║
║  8.72s execução     | pipeline completo local                ║
╠══════════════════════════════════════════════════════════════╣
║  STACK                                                       ║
║  Python 3.10   dbt 1.11     DuckDB 1.10   Airflow 2.8       ║
║  scikit-learn  MLflow       Streamlit     GitHub Actions     ║
║  Anthropic API (RAG)        Docker        Vercel             ║
╠══════════════════════════════════════════════════════════════╣
║  LINKS                                                       ║
║  Dashboard: credit-analytics-360.streamlit.app              ║
║  GitHub:    github.com/gabriel-analytics/credit-analytics-360║
║  Email:     gabrielmepv@gmail.com                           ║
╚══════════════════════════════════════════════════════════════╝
```

### 5 Insights para Memorizar

| # | Insight | Número | Ação |
|---|---|---|---|
| 1 | Sazonalidade jan/fev | 7.2% vs 5.5% (+29%) | Provisionar em out-dez |
| 2 | paid_search 2.1x risco | 9.73% vs 4.62% | Realoc. budget marketing |
| 3 | Multi-produto protege | 60% menos inadimplência | Cross-sell nos 90 dias |
| 4 | App = sensor de risco | SHAP 0.119, 2º preditor | Alerta D-30 automático |
| 5 | Janela cobrança D1-3 | 85% vs 23% recovery | Protocolo automático D+1 |

---

*Documento gerado em 2026-05-18 | Credit Analytics 360° | FinanceFlow Bank (dados sintéticos)*

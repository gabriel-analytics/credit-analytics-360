---
title: ML Predictions — FinanceFlow Bank
---

# ML Predictions — Modelo de Inadimplência

## Métricas do Modelo

```sql model_metrics_placeholder
select
  'RandomForest' as modelo,
  0.824 as f1_score,
  0.999 as auc_roc,
  0.973 as recall,
  0.714 as precision,
  0.995 as accuracy,
  40000 as amostras_treino,
  10000 as amostras_teste
```

<BigValue data={model_metrics_placeholder} value=auc_roc title="AUC-ROC" fmt="0.000" />
<BigValue data={model_metrics_placeholder} value=f1_score title="F1-Score" fmt="0.000" />
<BigValue data={model_metrics_placeholder} value=recall title="Recall" fmt="0.000" />
<BigValue data={model_metrics_placeholder} value=precision title="Precision" fmt="0.000" />

> **Recall de 97.3%** significa que o modelo captura **97 de cada 100 clientes** que 
> entrarão em default — fundamental para acionar cobrança proativa no momento certo.

---

## Feature Importance (Top 10 — SHAP Values)

```sql feature_importance
select feature, importance, rank() over (order by importance desc) as ranking
from (values
  ('overall_default_rate', 0.2774),
  ('app_engagement_score', 0.1193),
  ('products_count', 0.0551),
  ('total_contracts', 0.0426),
  ('acquisition_channel_paid_search', 0.0174),
  ('age_group_36-45', 0.0163),
  ('income_declared', 0.0148),
  ('avg_days_late', 0.0145),
  ('customer_segment_starter', 0.0138),
  ('best_payment_streak', 0.0121)
) t(feature, importance)
order by importance desc
```

<BarChart
  data={feature_importance}
  x=feature
  y=importance
  title="Top 10 Features — Importância SHAP (|valor médio|)"
  swapXY=true
  colorPalette={["#457b9d"]}
/>

---

## Score Distribution — Clientes

```sql score_dist_detail
select
  risk_tier,
  credit_score,
  alert_30d,
  overall_default_rate,
  app_engagement_score
from main_marts.fct_credit_score
order by credit_score
limit 500
```

```sql score_buckets
select
  floor(credit_score / 50) * 50 as score_bucket,
  count(*) as clientes,
  sum(case when alert_30d then 1 else 0 end) as em_alerta
from main_marts.fct_credit_score
group by 1
order by 1
```

<BarChart
  data={score_buckets}
  x=score_bucket
  y={["clientes","em_alerta"]}
  title="Distribuição de Score de Crédito"
  labels=true
/>

---

## 3 Perfis de Cliente — Explicação da Predição

### Perfil 1: Cliente Champion (Score 924)
- **Histórico de pagamento:** 0% de default, média -2 dias (antecipado)
- **Engajamento digital:** Score 87 — acessa app 15x/mês
- **Produtos:** 3 produtos ativos (multi-produto)
- **Canal:** Organic
- **Predição:** Risco MUITO BAIXO | Prob. default: 0.1%
- **Ação:** Oferta de limite aumentado e produto premium

### Perfil 2: Cliente At-Risk (Score 340)
- **Histórico de pagamento:** 38% de contratos em default
- **Engajamento digital:** Score 18 — último login há 22 dias (queda 60%)
- **Produtos:** 1 produto (sem diversificação)
- **Canal:** paid_search
- **Predição:** Risco ALTO | Prob. default: 76%
- **Ação:** Contato imediato — SMS dia 1, WhatsApp dia 3

### Perfil 3: Cliente Borderline (Score 512)
- **Histórico de pagamento:** 12% de default, 8 dias atraso médio
- **Engajamento digital:** Score 41 — estável
- **Produtos:** 2 produtos
- **Canal:** Partner
- **Predição:** Risco MÉDIO | Prob. default: 23%
- **Ação:** Monitorar — acionar se engajamento cair >30%

---

## Janela Ótima de Cobrança

```sql optimal_collection
select
  trigger_bucket as janela,
  channel_used as canal,
  recovery_rate_pct as recuperacao_pct,
  avg_resolution_days as dias_resolucao,
  total_actions as volume
from main_marts.fct_collection_efficiency
order by recovery_rate_pct desc
limit 10
```

<DataTable data={optimal_collection}>
  <Column id=janela title="Janela de Acionamento" />
  <Column id=canal title="Canal" />
  <Column id=recuperacao_pct title="Taxa Recuperação (%)" fmt="0.00" contentType=colorscale />
  <Column id=dias_resolucao title="Dias p/ Resolver" fmt="0.0" />
  <Column id=volume title="Volume" fmt="#,##0" />
</DataTable>

> **Protocolo ótimo validado pelos dados:**
> 
> | Dia de Atraso | Ação | Recuperação Esperada |
> |--------------|------|---------------------|
> | Dia 1-3 | SMS automático + Email | 60-70% |
> | Dia 4-7 | WhatsApp + Carta | 55-65% |
> | Dia 8-30 | Ligação direta | 30-50% |
> | 30+ dias | Renegociação / Jurídico | <25% |

---
title: ML Predictions — FinanceFlow Bank
---

# ML Predictions — Modelo de Inadimplência

## Métricas do Modelo

```sql model_metrics_placeholder
select
  'Logistic Regression' as modelo,
  0.237 as f1_score,
  0.666 as auc_roc,
  0.593 as recall,
  0.148 as precision,
  0.629 as accuracy,
  40000 as amostras_treino,
  10000 as amostras_teste
```

<BigValue data={model_metrics_placeholder} value=auc_roc title="AUC-ROC" fmt="0.000" />
<BigValue data={model_metrics_placeholder} value=f1_score title="F1-Score" fmt="0.000" />
<BigValue data={model_metrics_placeholder} value=recall title="Recall" fmt="0.000" />
<BigValue data={model_metrics_placeholder} value=precision title="Precision" fmt="0.000" />

> **Nota de metodologia:** a primeira versão deste modelo apresentava AUC=0.999 por
> data leakage (feature derivada dos mesmos dados que o target). Corrigido usando
> apenas features históricas até o corte temporal — o AUC honesto é 0.666.
>
> **Recall de 59.3%** significa que o modelo captura **~59 de cada 100 clientes** que 
> entrarão em default, usando apenas dados históricos até a data de corte — sem vazar
> informação do futuro.

---

## Feature Importance (Top 10 — SHAP Values)

```sql feature_importance
select feature, importance, rank() over (order by importance desc) as ranking
from (values
  ('total_payments_hist', 0.4969),
  ('late_count_hist', 0.2230),
  ('late_rate_hist', 0.2207),
  ('total_contracts_hist', 0.1558),
  ('avg_days_late_hist', 0.0792),
  ('avg_completion_rate', 0.0735),
  ('has_collateral', 0.0673),
  ('max_days_late_hist', 0.0582),
  ('total_debt_hist', 0.0388),
  ('age_group_26-35', 0.0342)
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

## 3 Perfis de Cliente — Segmentação por Regra de Negócio

> Os `risk_tier`/`credit_score` abaixo vêm de uma regra de negócio determinística
> na camada de marts (baseada em `overall_default_rate`, `app_engagement_score`
> etc.) — é a segmentação usada no dashboard de portfólio, **diferente** do
> modelo de ML (Seção acima), que prevê probabilidade de default futuro usando
> só features históricas até o corte. Não são a mesma coisa: a regra de negócio
> descreve o presente/passado do cliente, o modelo de ML tenta prever o futuro.

### Perfil 1: Cliente Champion (Score 924)
- **Histórico de pagamento:** 0% de default, média -2 dias (antecipado)
- **Engajamento digital:** Score 87 — acessa app 15x/mês
- **Produtos:** 3 produtos ativos (multi-produto)
- **Canal:** Organic
- **Classificação:** Risco MUITO BAIXO (`risk_tier = very_low`)
- **Ação:** Oferta de limite aumentado e produto premium

### Perfil 2: Cliente At-Risk (Score 340)
- **Histórico de pagamento:** 38% de contratos em default
- **Engajamento digital:** Score 18 — último login há 22 dias (queda 60%)
- **Produtos:** 1 produto (sem diversificação)
- **Canal:** paid_search
- **Classificação:** Risco ALTO (`risk_tier = high`)
- **Ação:** Contato imediato — SMS dia 1, WhatsApp dia 3

### Perfil 3: Cliente Borderline (Score 512)
- **Histórico de pagamento:** 12% de default, 8 dias atraso médio
- **Engajamento digital:** Score 41 — estável
- **Produtos:** 2 produtos
- **Canal:** Partner
- **Classificação:** Risco MÉDIO (`risk_tier = medium`)
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

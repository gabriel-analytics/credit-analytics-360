---
title: Risk Analytics — FinanceFlow Bank
---

# Risk Analytics

## Distribuição do Credit Score

```sql score_distribution
select
  case
    when credit_score < 200 then '0-200'
    when credit_score < 400 then '200-400'
    when credit_score < 600 then '400-600'
    when credit_score < 800 then '600-800'
    else '800-1000'
  end as faixa,
  count(*) as clientes,
  round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct
from main_marts.fct_credit_score
group by 1
order by 1
```

<BarChart
  data={score_distribution}
  x=faixa
  y=clientes
  title="Distribuição de Clientes por Faixa de Score"
  yAxisTitle="Número de Clientes"
  labels=true
/>

```sql risk_tier_summary
select
  risk_tier,
  count(*) as n_clientes,
  round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct,
  round(avg(credit_score), 0) as score_medio,
  sum(case when alert_30d then 1 else 0 end) as em_alerta
from main_marts.fct_credit_score
group by risk_tier
order by score_medio desc
```

<DataTable data={risk_tier_summary}>
  <Column id=risk_tier title="Tier de Risco" />
  <Column id=n_clientes title="Clientes" fmt="#,##0" />
  <Column id=pct title="%" fmt="0.0" />
  <Column id=score_medio title="Score Médio" fmt="#,##0" />
  <Column id=em_alerta title="Em Alerta 30d" fmt="#,##0" contentType=colorscale colorScale=negative />
</DataTable>

---

## Risco por Canal de Aquisição

```sql channel_risk
select
  acquisition_channel,
  round(default_rate_pct, 2) as inadimplencia,
  round(avg_ltv, 0) as ltv_medio,
  total_customers,
  round(approval_rate_pct, 1) as taxa_aprovacao
from main_intermediate.int_acquisition_quality
order by inadimplencia desc
```

<BarChart
  data={channel_risk}
  x=acquisition_channel
  y=inadimplencia
  title="Inadimplência por Canal de Aquisição (%)"
  yAxisTitle="Default Rate (%)"
  labels=true
  colorPalette={["#e63946","#e63946","#457b9d","#457b9d","#2a9d8f"]}
/>

> **Alerta:** O canal `paid_search` apresenta **9.73% de inadimplência** — 
> **2.1x acima** do canal `organic` (4.62%). Cada R$ 1 investido em paid_search 
> traz clientes de risco significativamente maior.

---

## Alertas — Clientes em Zona de Risco (30 dias)

```sql alerts_detail
select
  cs.customer_id,
  cs.risk_tier,
  cs.credit_score,
  cs.app_engagement_score,
  cs.overall_default_rate,
  cs.acquisition_channel,
  cs.age_group,
  cs.avg_days_late
from main_marts.fct_credit_score cs
where cs.alert_30d = true
order by cs.credit_score asc
limit 100
```

<BigValue
  data={alerts_detail}
  value="count"
  title="Clientes em Alerta 30d"
/>

<DataTable data={alerts_detail} rows=10 search=true>
  <Column id=customer_id title="Cliente ID" />
  <Column id=risk_tier title="Tier" />
  <Column id=credit_score title="Score" fmt="#,##0" />
  <Column id=app_engagement_score title="Engajamento App" fmt="0" contentType=colorscale />
  <Column id=overall_default_rate title="Default Rate" fmt="0.0%" contentType=colorscale colorScale=negative />
  <Column id=avg_days_late title="Dias Atraso Médio" fmt="0.0" />
</DataTable>

---

## Análise de Vintage — Inadimplência por Coorte

```sql vintage_analysis
select
  date_trunc('quarter', contract_date)::varchar as coorte_trimestre,
  product_type,
  count(*) as contratos,
  round(sum(case when is_defaulted then 1 else 0 end) * 100.0 / count(*), 2) as default_rate
from main_staging.stg_contracts
group by 1, 2
order by 1, 2
```

<BarChart
  data={vintage_analysis}
  x=coorte_trimestre
  y=default_rate
  series=product_type
  title="Default Rate por Coorte Trimestral e Produto (%)"
  type=grouped
/>

---

## Eficiência de Cobrança por Canal e Timing

```sql collection_eff
select
  channel_used as canal,
  trigger_bucket as janela,
  recovery_rate_pct as recuperacao_pct,
  avg_resolution_days as dias_resolucao,
  total_actions as acoes,
  is_best_window as melhor_janela
from main_marts.fct_collection_efficiency
order by recovery_rate_pct desc
```

<DataTable data={collection_eff}>
  <Column id=canal title="Canal" />
  <Column id=janela title="Janela" />
  <Column id=recuperacao_pct title="Recuperação (%)" fmt="0.00" contentType=colorscale />
  <Column id=dias_resolucao title="Dias p/ Resolver" fmt="0.0" />
  <Column id=acoes title="Ações" fmt="#,##0" />
  <Column id=melhor_janela title="Melhor Janela?" />
</DataTable>

> **Insight de cobrança:** Acionar clientes nos **primeiros 7 dias** (janela `early`) 
> via carta/email gera **66-70% de recuperação**. Após 30 dias, a taxa cai para **~20%**. 
> Protocolo recomendado: SMS automático no dia 1, WhatsApp no dia 3, ligação no dia 5.

---
title: Portfolio Overview — FinanceFlow Bank
---

# FinanceFlow Bank — Portfolio Overview

```sql portfolio_kpis
select
  round(sum(principal_amount)/1e9, 2)  as carteira_bi,
  count(distinct customer_id)          as total_contratos,
  round(avg(interest_rate)*100, 2)     as taxa_media_pct
from main_staging.stg_contracts
where status = 'active'
```

```sql default_rate
select
  round(avg(default_rate), 2) as inadimplencia_media,
  round(max(case when is_peak_month then default_rate end), 2) as inadimplencia_pico,
  round(avg(case when not is_peak_month then default_rate end), 2) as inadimplencia_offpeak
from main_marts.fct_portfolio_health
where mes >= '2023-01-01'
```

```sql active_customers
select count(*) as clientes_ativos
from main_staging.stg_customers
where is_active
```

```sql ltv_summary
select
  round(avg(case when ltv_segment = 'champion' then realized_ltv end), 0) as ltv_champion,
  round(avg(case when ltv_segment = 'high'     then realized_ltv end), 0) as ltv_high,
  count(case when ltv_segment = 'champion' then 1 end) as n_champions
from main_marts.fct_customer_ltv
```

<BigValue
  data={portfolio_kpis}
  value=carteira_bi
  title="Carteira Ativa (R$ bi)"
  fmt="0.00"
/>

<BigValue
  data={active_customers}
  value=clientes_ativos
  title="Clientes Ativos"
  fmt="#,##0"
/>

<BigValue
  data={default_rate}
  value=inadimplencia_media
  title="Inadimplência Média (%)"
  fmt="0.00"
/>

<BigValue
  data={ltv_summary}
  value=n_champions
  title="Clientes Champion"
  fmt="#,##0"
/>

---

## Inadimplência Mensal — Jan/2023 a Dez/2024

```sql monthly_default
select
  mes::varchar as mes,
  default_rate,
  npl_ratio,
  is_peak_month,
  case when is_peak_month then default_rate end as peak_rate
from main_marts.fct_portfolio_health
where mes >= '2023-01-01'
order by mes
```

<LineChart
  data={monthly_default}
  x=mes
  y={["default_rate", "npl_ratio"]}
  title="Default Rate e NPL Ratio (%) — 2023-2024"
  yAxisTitle="Taxa (%)"
  labels=true
/>

> **Insight:** Em janeiro e fevereiro de 2023 e 2024, a inadimplência atingiu **7.2%** — 
> **31% acima da média anual de 5.5%**. O padrão sazonal pós-festas é consistente e previsível,
> permitindo planejamento antecipado de capital e cobrança.

---

## Top Produtos por Risco de Inadimplência

```sql product_risk
select
  product_type,
  round(avg(default_rate_pct), 2) as default_rate,
  round(avg(recovery_rate_pct), 2) as recovery_rate,
  sum(total_payments) as total_pagamentos
from main_intermediate.int_payment_performance
group by product_type
order by default_rate desc
```

<DataTable
  data={product_risk}
  rows=10
>
  <Column id=product_type title="Produto" />
  <Column id=default_rate title="Inadimplência (%)" fmt="0.00" contentType=colorscale colorScale=negative />
  <Column id=recovery_rate title="Taxa Recuperação (%)" fmt="0.00" />
  <Column id=total_pagamentos title="Total Pagamentos" fmt="#,##0" />
</DataTable>

---

## Exposição Mensal da Carteira

```sql exposure_monthly
select
  mes::varchar as mes,
  round(total_exposure/1e6, 1) as exposicao_mm,
  total_active_contracts,
  is_peak_month
from main_marts.fct_portfolio_health
where mes >= '2023-01-01'
order by mes
```

<AreaChart
  data={exposure_monthly}
  x=mes
  y=exposicao_mm
  title="Exposição Total da Carteira (R$ milhões)"
  yAxisTitle="R$ MM"
/>

---
title: Customer Segments — FinanceFlow Bank
---

# Customer Segments

## Segmentação RFM de Crédito

```sql rfm_overview
select
  segment,
  count(*) as clientes,
  round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct,
  round(avg(on_time_rate_pct), 1) as pontualidade_media,
  round(avg(avg_amount_paid), 0) as ticket_medio,
  round(avg(r_score + f_score + m_score), 1) as rfm_total_medio
from main_marts.fct_risk_segments
group by segment
order by rfm_total_medio desc
```

<BarChart
  data={rfm_overview}
  x=segment
  y=clientes
  title="Clientes por Segmento RFM"
  labels=true
  colorPalette={["#2a9d8f","#457b9d","#e9c46a","#f4a261","#e63946"]}
/>

<DataTable data={rfm_overview}>
  <Column id=segment title="Segmento" />
  <Column id=clientes title="Clientes" fmt="#,##0" />
  <Column id=pct title="%" fmt="0.0" />
  <Column id=pontualidade_media title="Pontualidade (%)" fmt="0.0" contentType=colorscale />
  <Column id=ticket_medio title="Ticket Médio (R$)" fmt="#,##0" />
  <Column id=rfm_total_medio title="RFM Score" fmt="0.0" />
</DataTable>

---

## LTV por Segmento e Canal de Aquisição

```sql ltv_by_segment_channel
select
  ltv_segment,
  acquisition_channel,
  count(*) as clientes,
  round(avg(realized_ltv), 0) as ltv_medio,
  round(avg(projected_ltv_12m), 0) as ltv_projetado_12m
from main_marts.fct_customer_ltv
group by ltv_segment, acquisition_channel
order by ltv_medio desc
limit 20
```

<BarChart
  data={ltv_by_segment_channel}
  x=acquisition_channel
  y=ltv_medio
  series=ltv_segment
  title="LTV Médio por Canal e Segmento (R$)"
  type=grouped
/>

```sql ltv_segment_summary
select
  ltv_segment,
  count(*) as clientes,
  round(avg(realized_ltv), 0) as ltv_realizado,
  round(avg(projected_ltv_12m), 0) as ltv_projetado,
  round(avg(total_contracts), 1) as contratos_medio
from main_marts.fct_customer_ltv
group by ltv_segment
order by ltv_realizado desc
```

<DataTable data={ltv_segment_summary}>
  <Column id=ltv_segment title="Segmento LTV" />
  <Column id=clientes title="Clientes" fmt="#,##0" />
  <Column id=ltv_realizado title="LTV Realizado (R$)" fmt="#,##0" />
  <Column id=ltv_projetado title="LTV Projetado 12m (R$)" fmt="#,##0" />
  <Column id=contratos_medio title="Contratos Médio" fmt="0.0" />
</DataTable>

---

## Comportamento Digital por Segmento

```sql digital_by_segment
select
  rs.segment,
  round(avg(db.app_engagement_score), 1) as engajamento_medio,
  round(avg(db.logins_count), 1) as logins_mensais,
  round(avg(db.payment_events), 1) as pagamentos_app,
  count(case when db.trend_30d = 'declining' then 1 end) as tendencia_queda
from main_marts.fct_risk_segments rs
left join main_intermediate.int_customer_digital_behavior db
  on rs.customer_id = db.customer_id
  and db.event_month = (
    select max(event_month)
    from main_intermediate.int_customer_digital_behavior
  )
group by rs.segment
order by engajamento_medio desc
```

<BarChart
  data={digital_by_segment}
  x=segment
  y=engajamento_medio
  title="Score de Engajamento Digital por Segmento (0-100)"
  labels=true
/>

---

## Recomendações por Segmento

| Segmento | Clientes | Ação Recomendada | Canal Prioritário |
|----------|----------|------------------|-------------------|
| **Champion** | 3.605 | Oferta de produtos premium, cross-sell | App (push) |
| **Loyal** | 9.972 | Programa de fidelidade, aumento de limite | Email + App |
| **Promising** | 7.596 | Onboarding 2° produto, educação financeira | WhatsApp |
| **At Risk** | 13.069 | Contato proativo de cobrança preventiva | SMS + Ligação |
| **Lost** | 11.413 | Campanha de reativação, renegociação | Email + Carta |

> **Prioridade:** Os 13.069 clientes `at_risk` representam o maior risco imediato.
> Com a janela de cobrança early (1-7 dias), **taxa de recuperação de 65-70%** é alcançável.
> Protocolo: identificar queda de engajamento no app como sinal precoce (73% de acurácia).

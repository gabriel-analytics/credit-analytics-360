with profile as (
    select
        customer_id,
        customer_segment,
        acquisition_channel,
        total_debt,
        total_paid,
        total_contracts,
        active_contracts,
        avg_interest_rate
    from {{ ref('int_customer_credit_profile') }}
),

channel_quality as (
    select
        acquisition_channel,
        avg_ltv            as channel_avg_ltv,
        default_rate_pct   as channel_default_rate,
        avg_ticket         as channel_avg_ticket
    from {{ ref('int_acquisition_quality') }}
),

ltv_calc as (
    select
        p.customer_id,
        p.customer_segment,
        p.acquisition_channel,
        p.total_contracts,
        p.active_contracts,
        p.total_debt,
        p.total_paid,
        p.avg_interest_rate,

        -- realized LTV: total paid minus cost of debt (simplified)
        round(p.total_paid - p.total_debt, 2)       as realized_ltv,

        -- projected 12m: apply 20% growth assumption on realized
        round((p.total_paid - p.total_debt) * 1.2, 2) as projected_ltv_12m,

        cq.channel_avg_ltv,
        cq.channel_default_rate,
        cq.channel_avg_ticket
    from profile p
    left join channel_quality cq using (acquisition_channel)
),

with_quartiles as (
    select
        *,
        ntile(4) over (order by realized_ltv)       as ltv_quartile
    from ltv_calc
),

final as (
    select
        customer_id,
        customer_segment,
        acquisition_channel,
        total_contracts,
        active_contracts,
        total_debt,
        total_paid,
        avg_interest_rate,
        realized_ltv,
        projected_ltv_12m,
        channel_avg_ltv,
        channel_default_rate,
        channel_avg_ticket,
        ltv_quartile,

        case ltv_quartile
            when 1 then 'low'
            when 2 then 'medium'
            when 3 then 'high'
            when 4 then 'champion'
        end                                         as ltv_segment,

        -- vs channel benchmark
        round(realized_ltv - coalesce(channel_avg_ltv, 0), 2) as ltv_vs_channel_avg,

        current_timestamp                           as _loaded_at
    from with_quartiles
)

select * from final

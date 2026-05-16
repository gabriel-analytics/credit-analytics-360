with profile as (
    select * from {{ ref('int_customer_credit_profile') }}
),

-- get latest month engagement per customer
behavior_latest as (
    select
        customer_id,
        app_engagement_score,
        days_since_last_login,
        trend_30d
    from {{ ref('int_customer_digital_behavior') }}
    qualify row_number() over (
        partition by customer_id order by event_month desc
    ) = 1
),

customers as (
    select
        customer_id,
        age_group,
        income_declared,
        customer_segment
    from {{ ref('stg_customers') }}
),

-- normalize income to 0-100 using percentile rank
income_ranked as (
    select
        customer_id,
        income_declared,
        percent_rank() over (order by income_declared) * 100 as income_pct
    from customers
),

joined as (
    select
        p.customer_id,
        p.customer_segment,
        p.acquisition_channel,
        p.age_group,
        p.products_count,
        p.is_multi_product,
        p.total_contracts,
        p.overall_default_rate,
        p.avg_days_late,
        p.best_payment_streak,
        p.total_debt,
        p.total_paid,

        coalesce(b.app_engagement_score, 0)     as app_engagement_score,
        coalesce(b.days_since_last_login, 30)   as days_since_last_login,
        coalesce(b.trend_30d, 'stable')         as trend_30d,

        coalesce(ir.income_pct, 0)              as income_pct,

        -- age group score (0-100): younger pays better at same score
        case c.age_group
            when '18-25' then 75
            when '26-35' then 80
            when '36-45' then 65
            when '46-60' then 70
            when '60+'   then 72
            else 50
        end                                      as age_score
    from profile p
    left join behavior_latest b using (customer_id)
    left join customers       c using (customer_id)
    left join income_ranked  ir using (customer_id)
),

scored as (
    select
        *,

        -- payment history component (0-400): 40% weight
        round(
            (1 - coalesce(overall_default_rate, 0)) * 400
        , 0)                                                                as component_payment,

        -- digital behavior component (0-250): 25% weight
        round(app_engagement_score * 2.5, 0)                               as component_digital,

        -- profile component (0-200): 20% weight
        round((age_score * 0.5 + income_pct * 0.5) * 2.0, 0)              as component_profile,

        -- diversification component (0-150): 15% weight
        round(least(products_count, 4) / 4.0 * 150, 0)                    as component_diversification
    from joined
),

final as (
    select
        customer_id,
        customer_segment,
        acquisition_channel,
        age_group,
        products_count,
        is_multi_product,
        total_contracts,
        overall_default_rate,
        avg_days_late,
        best_payment_streak,
        app_engagement_score,
        days_since_last_login,
        trend_30d,
        income_pct,
        total_debt,
        total_paid,

        component_payment,
        component_digital,
        component_profile,
        component_diversification,

        -- final score
        least(1000, greatest(0,
            component_payment
            + component_digital
            + component_profile
            + component_diversification
        ))                                                                  as credit_score,

        -- risk tier
        case
            when (component_payment + component_digital + component_profile + component_diversification) >= 800 then 'very_low'
            when (component_payment + component_digital + component_profile + component_diversification) >= 600 then 'low'
            when (component_payment + component_digital + component_profile + component_diversification) >= 400 then 'medium'
            when (component_payment + component_digital + component_profile + component_diversification) >= 200 then 'high'
            else 'very_high'
        end                                                                 as risk_tier,

        -- 30-day alert: high/very_high risk AND low digital engagement
        case
            when (component_payment + component_digital + component_profile + component_diversification) < 400
                 and app_engagement_score < 30
            then true
            else false
        end                                                                 as alert_30d,

        current_timestamp                                                   as _loaded_at
    from scored
)

select * from final

with customers as (
    select * from {{ ref('stg_customers') }}
),

contracts as (
    select * from {{ ref('stg_contracts') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

contract_agg as (
    select
        customer_id,
        count(*)                                                        as total_contracts,
        sum(case when is_active_contract then 1 else 0 end)            as active_contracts,
        sum(case when is_defaulted       then 1 else 0 end)            as defaulted_contracts,
        sum(principal_amount)                                           as total_debt,
        sum(case when is_settled then principal_amount else 0 end)     as settled_debt,
        round(avg(interest_rate), 4)                                    as avg_interest_rate,
        max(contract_date)                                              as last_contract_date,
        string_agg(distinct product_type, ', ')                        as product_types
    from contracts
    group by customer_id
),

payment_agg as (
    select
        customer_id,
        count(*)                                                        as total_payments,
        sum(cast(is_defaulted as integer))                             as defaulted_payments,
        sum(coalesce(amount_paid, 0))                                  as total_paid,
        round(avg(case when days_late is not null then days_late else 0 end), 1)
                                                                        as avg_days_late,
        sum(case when due_month >= date_trunc('month', current_date - interval '12 months')
                 and not is_defaulted then 1 else 0 end)               as on_time_last_12m,
        sum(case when due_month >= date_trunc('month', current_date - interval '12 months')
                 then 1 else 0 end)                                    as total_last_12m
    from payments
    group by customer_id
),

-- consecutive on-time months (simple proxy: ratio in last 12m)
final as (
    select
        c.customer_id,
        c.customer_segment,
        c.acquisition_channel,
        c.age_group,
        c.products_count,
        c.is_multi_product,
        c.income_declared,

        coalesce(ca.total_contracts,    0)                             as total_contracts,
        coalesce(ca.active_contracts,   0)                             as active_contracts,
        coalesce(ca.defaulted_contracts,0)                             as defaulted_contracts,
        coalesce(ca.total_debt,         0)                             as total_debt,
        coalesce(ca.settled_debt,       0)                             as settled_debt,
        ca.avg_interest_rate,
        ca.last_contract_date,
        ca.product_types,

        coalesce(pa.total_payments,    0)                              as total_payments,
        coalesce(pa.defaulted_payments,0)                              as defaulted_payments,
        coalesce(pa.total_paid,        0)                              as total_paid,
        coalesce(pa.avg_days_late,     0)                              as avg_days_late,

        -- overall default rate at contract level
        case
            when coalesce(ca.total_contracts, 0) = 0 then null
            else round(
                cast(coalesce(ca.defaulted_contracts,0) as double)
                / ca.total_contracts, 4
            )
        end                                                             as overall_default_rate,

        -- best payment streak proxy (on-time ratio last 12m, 0-12)
        case
            when coalesce(pa.total_last_12m, 0) = 0 then 0
            else round(
                cast(pa.on_time_last_12m as double)
                / pa.total_last_12m * 12, 1
            )
        end                                                             as best_payment_streak
    from customers c
    left join contract_agg ca using (customer_id)
    left join payment_agg  pa using (customer_id)
)

select * from final

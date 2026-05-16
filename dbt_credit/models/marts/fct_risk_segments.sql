with payments as (
    select
        customer_id,
        payment_date,
        due_date,
        amount_paid,
        is_defaulted,
        is_late,
        status
    from {{ ref('stg_payments') }}
    where payment_date is not null or status = 'defaulted'
),

customers as (
    select
        customer_id,
        customer_segment,
        acquisition_channel,
        age_group,
        products_count
    from {{ ref('stg_customers') }}
),

customer_payment_agg as (
    select
        customer_id,

        -- Recency: days since last payment (lower = more recent = better)
        date_diff('day',
            max(coalesce(payment_date, due_date)),
            current_date
        )                                                                   as days_since_last_payment,

        -- Frequency: on-time payment rate (higher = more regular = better)
        round(
            sum(case when not is_late and not is_defaulted then 1 else 0 end)::double
            / nullif(count(*), 0) * 100,
        1)                                                                  as on_time_rate_pct,

        -- Monetary: average amount paid
        round(avg(coalesce(amount_paid, 0)), 2)                            as avg_amount_paid,

        count(*)                                                            as total_payments,
        sum(cast(is_defaulted as integer))                                 as total_defaults
    from payments
    group by customer_id
),

with_quartiles as (
    select
        customer_id,
        days_since_last_payment,
        on_time_rate_pct,
        avg_amount_paid,
        total_payments,
        total_defaults,

        -- R: recency quartile — INVERTED (1=most recent=best, but we score 4=best)
        5 - ntile(4) over (order by days_since_last_payment desc)          as r_score,

        -- F: frequency quartile (4=most regular=best)
        ntile(4) over (order by on_time_rate_pct)                          as f_score,

        -- M: monetary quartile (4=highest avg payment=best)
        ntile(4) over (order by avg_amount_paid)                           as m_score
    from customer_payment_agg
),

with_rfm as (
    select
        *,
        -- RFM string score e.g. "444"
        r_score::varchar || f_score::varchar || m_score::varchar           as rfm_score,
        r_score + f_score + m_score                                        as rfm_total
    from with_quartiles
),

segmented as (
    select
        *,
        case
            when rfm_score in ('444','443','434','344') then 'champion'
            when rfm_score like '3%3%' or rfm_score like '4%3%'
                 or rfm_score like '3%4%'                then 'loyal'
            when rfm_score like '2%'
                 or rfm_score in ('314','313','312','311') then 'at_risk'
            when rfm_score like '1%'                     then 'lost'
            else 'promising'
        end                                                                 as segment
    from with_rfm
),

final as (
    select
        s.customer_id,
        c.customer_segment,
        c.acquisition_channel,
        c.age_group,
        c.products_count,
        s.days_since_last_payment,
        s.on_time_rate_pct,
        s.avg_amount_paid,
        s.total_payments,
        s.total_defaults,
        s.r_score,
        s.f_score,
        s.m_score,
        s.rfm_score,
        s.rfm_total,
        s.segment,
        current_timestamp                                                   as _loaded_at
    from segmented s
    left join customers c using (customer_id)
)

select * from final

with payments as (
    select * from {{ ref('stg_payments') }}
),

contracts as (
    select contract_id, product_type, customer_id
    from {{ ref('stg_contracts') }}
),

joined as (
    select
        p.*,
        c.product_type
    from payments p
    left join contracts c using (contract_id)
),

monthly as (
    select
        due_month,
        due_year,
        due_month_num,
        product_type,

        count(*)                                                            as total_payments,
        sum(cast(is_defaulted as integer))                                 as defaulted_count,
        sum(cast(is_late      as integer))                                 as late_count,
        sum(coalesce(amount_due,  0))                                      as total_amount_due,
        sum(coalesce(amount_paid, 0))                                      as total_amount_paid,
        round(avg(case when days_late > 0 then days_late end), 1)         as avg_days_late,

        round(
            cast(sum(cast(is_defaulted as integer)) as double)
            / nullif(count(*), 0) * 100,
        2)                                                                  as default_rate_pct,

        round(
            cast(sum(coalesce(amount_paid, 0)) as double)
            / nullif(sum(coalesce(amount_due, 0)), 0) * 100,
        2)                                                                  as recovery_rate_pct
    from joined
    group by due_month, due_year, due_month_num, product_type
),

with_seasonal as (
    select
        *,
        -- seasonal index: ratio to annual average default rate
        round(
            default_rate_pct
            / nullif(
                avg(default_rate_pct) over (
                    partition by due_year, product_type
                ),
            0),
        3)                                                                  as seasonal_index,

        -- jan/feb flag
        case when due_month_num in (1, 2) then true else false end         as is_high_season
    from monthly
)

select * from with_seasonal

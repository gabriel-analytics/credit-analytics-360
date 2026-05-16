with payments as (
    select
        contract_id,
        customer_id,
        due_month,
        due_year,
        due_month_num,
        status,
        is_defaulted,
        lateness_bucket,
        amount_due
    from {{ ref('stg_payments') }}
),

contracts as (
    select
        contract_id,
        principal_amount,
        status          as contract_status,
        is_active_contract,
        is_defaulted    as contract_defaulted,
        product_type
    from {{ ref('stg_contracts') }}
),

-- one row per contract per month it had payment activity
monthly_base as (
    select
        p.due_month                                                         as mes,
        p.due_month_num,
        p.due_year,
        c.contract_id,
        c.principal_amount,
        c.product_type,
        c.is_active_contract,
        c.contract_defaulted,
        p.is_defaulted                                                      as payment_defaulted,
        p.lateness_bucket,
        p.amount_due
    from payments p
    left join contracts c using (contract_id)
),

aggregated as (
    select
        mes,
        due_month_num,
        due_year,

        count(distinct contract_id)                                         as total_active_contracts,
        round(sum(principal_amount), 2)                                     as total_exposure,
        sum(amount_due)                                                     as total_amount_due,

        -- default rate: payments in default / total payments
        round(
            sum(cast(payment_defaulted as integer))::double
            / nullif(count(*), 0) * 100,
        2)                                                                  as default_rate,

        -- NPL ratio: contracts with 90+ days late / total distinct contracts
        round(
            count(distinct case
                when lateness_bucket = '90+_days' then contract_id
            end)::double
            / nullif(count(distinct contract_id), 0) * 100,
        2)                                                                  as npl_ratio,

        -- product mix
        round(
            count(distinct case when product_type = 'personal_loan'     then contract_id end)::double
            / nullif(count(distinct contract_id), 0) * 100,
        1)                                                                  as pct_personal_loan,

        round(
            count(distinct case when product_type = 'vehicle_financing' then contract_id end)::double
            / nullif(count(distinct contract_id), 0) * 100,
        1)                                                                  as pct_vehicle_financing
    from monthly_base
    group by mes, due_month_num, due_year
),

final as (
    select
        *,
        -- peak months: January and February (post-holiday seasonality)
        case when due_month_num in (1, 2) then true else false end          as is_peak_month,

        -- month-over-month default rate change
        round(
            default_rate
            - lag(default_rate) over (order by mes),
        2)                                                                  as default_rate_mom_delta,

        current_timestamp                                                   as _loaded_at
    from aggregated
)

select * from final
order by mes

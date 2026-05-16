with customers as (
    select customer_id, acquisition_channel, customer_segment, income_declared
    from {{ ref('stg_customers') }}
),

contracts as (
    select
        customer_id,
        contract_id,
        product_type,
        principal_amount,
        interest_rate,
        installments_total,
        is_defaulted,
        is_settled,
        completion_rate
    from {{ ref('stg_contracts') }}
),

payments as (
    select
        customer_id,
        contract_id,
        amount_due,
        amount_paid,
        is_defaulted
    from {{ ref('stg_payments') }}
),

proposals as (
    select
        customer_id,
        is_approved,
        is_rejected,
        requested_amount,
        approved_amount
    from {{ ref('stg_proposals') }}
),

proposal_agg as (
    select
        customer_id,
        count(*)                                                        as total_proposals,
        sum(cast(is_approved as integer))                              as approved_proposals,
        sum(cast(is_rejected as integer))                              as rejected_proposals,
        sum(coalesce(approved_amount, 0))                              as total_approved_amount
    from proposals
    group by customer_id
),

contract_agg as (
    select
        customer_id,
        count(*)                                                        as total_contracts,
        sum(cast(is_defaulted as integer))                             as defaulted_contracts,
        sum(principal_amount)                                           as total_principal,
        round(avg(interest_rate), 4)                                   as avg_interest_rate,
        round(avg(completion_rate), 4)                                 as avg_completion_rate,
        round(avg(installments_total), 1)                              as avg_term_months
    from contracts
    group by customer_id
),

payment_agg as (
    select
        customer_id,
        sum(coalesce(amount_due,  0))                                  as total_due,
        sum(coalesce(amount_paid, 0))                                  as total_paid
    from payments
    group by customer_id
),

customer_level as (
    select
        c.customer_id,
        c.acquisition_channel,
        c.customer_segment,
        c.income_declared,

        coalesce(pa.total_proposals,       0)                          as total_proposals,
        coalesce(pa.approved_proposals,    0)                          as approved_proposals,
        coalesce(pa.total_approved_amount, 0)                          as total_approved_amount,

        coalesce(ca.total_contracts,       0)                          as total_contracts,
        coalesce(ca.defaulted_contracts,   0)                          as defaulted_contracts,
        coalesce(ca.total_principal,       0)                          as total_principal,
        ca.avg_interest_rate,
        ca.avg_completion_rate,
        ca.avg_term_months,

        coalesce(py.total_due,  0)                                     as total_due,
        coalesce(py.total_paid, 0)                                     as total_paid
    from customers c
    left join proposal_agg  pa using (customer_id)
    left join contract_agg  ca using (customer_id)
    left join payment_agg   py using (customer_id)
),

channel_agg as (
    select
        acquisition_channel,

        count(distinct customer_id)                                    as total_customers,

        -- approval rate
        round(
            sum(cast(approved_proposals as double))
            / nullif(sum(total_proposals), 0) * 100,
        2)                                                              as approval_rate_pct,

        -- default rate
        round(
            sum(cast(defaulted_contracts as double))
            / nullif(sum(total_contracts), 0) * 100,
        2)                                                              as default_rate_pct,

        -- avg ticket
        round(
            sum(total_principal)
            / nullif(sum(total_contracts), 0),
        2)                                                              as avg_ticket,

        -- avg LTV (total paid per customer with contracts)
        round(
            sum(total_paid)
            / nullif(count(distinct case when total_contracts > 0 then customer_id end), 0),
        2)                                                              as avg_ltv,

        -- payback period proxy: avg term × completion rate
        round(
            avg(case when avg_term_months is not null
                then avg_term_months * coalesce(avg_completion_rate, 0) end),
        1)                                                              as avg_payback_months,

        round(avg(income_declared), 2)                                 as avg_income
    from customer_level
    group by acquisition_channel
)

select * from channel_agg

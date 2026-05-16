with source as (
    select * from read_parquet('C:/Users/lineg/credit-analytics-360/gen/data/payments.parquet')
),

renamed as (
    select
        payment_id,
        contract_id,
        customer_id,
        cast(due_date as date)                                      as due_date,
        cast(payment_date as date)                                  as payment_date,
        cast(amount_due as double)                                  as amount_due,
        cast(amount_paid as double)                                 as amount_paid,
        cast(days_late as integer)                                  as days_late,
        payment_method,
        status,

        -- derived flags
        case when days_late > 0  then true else false end           as is_late,
        case when status = 'defaulted' then true else false end     as is_defaulted,

        -- lateness bucket
        case
            when days_late <= 0            then 'on_time'
            when days_late between 1  and 30  then '1-30_days'
            when days_late between 31 and 60  then '31-60_days'
            when days_late between 61 and 90  then '61-90_days'
            else '90+_days'
        end                                                         as lateness_bucket,

        -- time dims
        date_trunc('month', cast(due_date as date))                 as due_month,
        extract('year'  from cast(due_date as date))                as due_year,
        extract('month' from cast(due_date as date))                as due_month_num,

        -- metadata
        current_timestamp                                           as _loaded_at,
        'payments.parquet'                                          as _source
    from source
)

select * from renamed

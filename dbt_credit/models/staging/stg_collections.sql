with source as (
    select * from read_parquet('C:/Users/lineg/credit-analytics-360/gen/data/collections.parquet')
),

renamed as (
    select
        collection_id,
        contract_id,
        customer_id,
        cast(trigger_date as date)                                  as trigger_date,
        cast(days_overdue_at_trigger as integer)                    as days_overdue_at_trigger,
        channel_used,
        outcome,
        cast(recovery_amount as double)                             as recovery_amount,
        cast(resolution_days as integer)                            as resolution_days,

        -- derived flags
        case
            when outcome in ('paid', 'renegotiated') then true
            else false
        end                                                         as is_recovered,

        case when outcome is null then true else false end          as is_missing_outcome,

        -- overdue bucket
        case
            when days_overdue_at_trigger between 1  and 3   then 'early_1_3d'
            when days_overdue_at_trigger between 4  and 30  then 'early_4_30d'
            when days_overdue_at_trigger between 31 and 90  then 'mid_31_90d'
            else 'late_90plus'
        end                                                         as overdue_bucket,

        date_trunc('month', cast(trigger_date as date))             as trigger_month,

        -- metadata
        current_timestamp                                           as _loaded_at,
        'collections.parquet'                                       as _source
    from source
)

select * from renamed

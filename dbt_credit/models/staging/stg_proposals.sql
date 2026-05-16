with source as (
    select * from read_parquet('C:/Users/lineg/credit-analytics-360/gen/data/proposals.parquet')
),

renamed as (
    select
        proposal_id,
        customer_id,
        cast(proposal_date as date)                                 as proposal_date,
        product_type,
        cast(requested_amount as double)                            as requested_amount,
        cast(approved_amount as double)                             as approved_amount,
        cast(interest_rate_offered as double)                       as interest_rate_offered,
        decision,
        rejection_reason,
        cast(bureau_score as integer)                               as bureau_score,

        -- derived flags
        case when decision = 'approved' then true else false end    as is_approved,
        case when decision = 'rejected' then true else false end    as is_rejected,
        case when bureau_score is null  then true else false end    as is_missing_bureau,

        -- approval ratio (share of requested that was approved)
        case
            when decision = 'approved' and requested_amount > 0
            then round(approved_amount / requested_amount, 4)
            else null
        end                                                         as approval_ratio,

        -- bureau score band
        case
            when bureau_score is null          then 'unknown'
            when bureau_score < 300            then 'very_low'
            when bureau_score between 300 and 499 then 'low'
            when bureau_score between 500 and 699 then 'medium'
            when bureau_score between 700 and 849 then 'high'
            else 'very_high'
        end                                                         as bureau_score_band,

        date_trunc('month', cast(proposal_date as date))            as proposal_month,

        -- metadata
        current_timestamp                                           as _loaded_at,
        'proposals.parquet'                                         as _source
    from source
)

select * from renamed

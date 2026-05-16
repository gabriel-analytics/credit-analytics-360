with collections as (
    select
        collection_id,
        contract_id,
        customer_id,
        trigger_date,
        days_overdue_at_trigger,
        channel_used,
        outcome,
        recovery_amount,
        resolution_days,
        is_recovered,
        is_missing_outcome,
        overdue_bucket,
        trigger_month
    from {{ ref('stg_collections') }}
),

with_trigger_bucket as (
    select
        *,
        case
            when days_overdue_at_trigger between 1  and 7  then 'early'
            when days_overdue_at_trigger between 8  and 30 then 'mid'
            when days_overdue_at_trigger between 31 and 90 then 'late'
            else 'bad'
        end                                                                 as trigger_bucket
    from collections
),

channel_bucket_agg as (
    select
        channel_used,
        trigger_bucket,

        count(*)                                                            as total_actions,
        sum(cast(is_recovered as integer))                                  as recovered_count,
        sum(cast(is_missing_outcome as integer))                            as missing_outcome_count,

        round(
            sum(cast(is_recovered as integer))::double
            / nullif(count(*), 0) * 100,
        2)                                                                  as recovery_rate_pct,

        round(avg(case when is_recovered then resolution_days end), 1)     as avg_resolution_days,
        round(sum(coalesce(recovery_amount, 0)), 2)                        as total_recovered_amount,
        round(avg(coalesce(recovery_amount, 0)), 2)                        as avg_recovery_amount,
        round(avg(days_overdue_at_trigger), 1)                             as avg_days_overdue
    from with_trigger_bucket
    group by channel_used, trigger_bucket
),

-- find the best trigger window per channel
best_window as (
    select
        channel_used,
        trigger_bucket                                                      as best_trigger_bucket,
        recovery_rate_pct                                                   as best_recovery_rate
    from channel_bucket_agg
    qualify row_number() over (
        partition by channel_used
        order by recovery_rate_pct desc
    ) = 1
),

final as (
    select
        cba.*,
        bw.best_trigger_bucket,
        bw.best_recovery_rate,
        case
            when cba.trigger_bucket = bw.best_trigger_bucket then true
            else false
        end                                                                 as is_best_window,
        current_timestamp                                                   as _loaded_at
    from channel_bucket_agg cba
    left join best_window bw using (channel_used)
)

select * from final
order by channel_used, trigger_bucket

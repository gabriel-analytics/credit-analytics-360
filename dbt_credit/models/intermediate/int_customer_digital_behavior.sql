with customers as (
    select customer_id, customer_segment, acquisition_channel
    from {{ ref('stg_customers') }}
),

events as (
    select * from {{ ref('stg_app_events') }}
),

monthly as (
    select
        customer_id,
        event_month,
        count(*)                                                        as events_total,
        sum(cast(is_login_event   as integer))                         as logins_count,
        sum(cast(is_payment_event as integer))                         as payment_events,
        sum(cast(is_offer_event   as integer))                         as offer_events,
        count(distinct event_day)                                       as active_days,
        round(avg(session_duration_seconds), 0)                        as avg_session_seconds,
        max(event_day)                                                  as last_event_day
    from events
    group by customer_id, event_month
),

with_lag as (
    select
        *,
        lag(events_total) over (
            partition by customer_id order by event_month
        )                                                               as prev_month_events
    from monthly
),

scored as (
    select
        customer_id,
        event_month,
        logins_count,
        payment_events,
        offer_events,
        active_days,
        avg_session_seconds,
        last_event_day,
        events_total,
        prev_month_events,

        -- days since last login relative to month end
        date_diff('day', last_event_day, (event_month + interval '1 month' - interval '1 day')::date)
                                                                        as days_since_last_login,

        -- engagement score 0-100
        least(100, round(
              (logins_count   * 5.0)
            + (payment_events * 15.0)
            + (active_days    * 3.0)
            + (least(avg_session_seconds, 600) / 600.0 * 20.0)
        , 0))                                                           as app_engagement_score,

        -- trend: growing / stable / declining
        case
            when prev_month_events is null then 'new'
            when events_total > prev_month_events * 1.1  then 'growing'
            when events_total < prev_month_events * 0.5  then 'declining'
            else 'stable'
        end                                                             as trend_30d
    from with_lag
),

final as (
    select
        s.*,
        c.customer_segment,
        c.acquisition_channel
    from scored s
    left join customers c using (customer_id)
)

select * from final

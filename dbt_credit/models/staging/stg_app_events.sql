with source as (
    select * from read_parquet('C:/Users/lineg/credit-analytics-360/gen/data/app_events.parquet')
),

renamed as (
    select
        event_id,
        customer_id,
        cast(event_date as timestamp)                               as event_date,
        event_type,
        channel,
        cast(session_duration_seconds as integer)                   as session_duration_seconds,

        -- time dims
        cast(event_date as date)                                    as event_day,
        date_trunc('month', cast(event_date as date))               as event_month,
        extract('hour'       from cast(event_date as timestamp))    as event_hour,
        extract('dow'        from cast(event_date as date))         as day_of_week,
        extract('year'       from cast(event_date as date))         as event_year,

        -- flags
        case when event_type = 'make_payment'  then true else false end  as is_payment_event,
        case when event_type = 'login'         then true else false end  as is_login_event,
        case when event_type in ('view_offer', 'accept_offer', 'reject_offer')
             then true else false end                                     as is_offer_event,

        -- metadata
        current_timestamp                                           as _loaded_at,
        'app_events.parquet'                                        as _source
    from source
)

select * from renamed

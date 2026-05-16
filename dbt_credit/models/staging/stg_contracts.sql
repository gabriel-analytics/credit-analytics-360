with source as (
    select * from read_parquet('C:/Users/lineg/credit-analytics-360/gen/data/contracts.parquet')
),

renamed as (
    select
        contract_id,
        customer_id,
        product_type,
        cast(contract_date as date)                                 as contract_date,
        cast(maturity_date as date)                                 as maturity_date,
        cast(principal_amount as double)                            as principal_amount,
        cast(interest_rate as double)                               as interest_rate,
        cast(installments_total as integer)                         as installments_total,
        cast(installments_paid as integer)                          as installments_paid,
        status,
        collateral,

        -- derived
        date_diff(
            'day',
            current_date,
            cast(maturity_date as date)
        )                                                           as days_to_maturity,

        round(
            cast(installments_paid as double)
            / nullif(cast(installments_total as double), 0),
            4
        )                                                           as completion_rate,

        case when status = 'defaulted'  then true else false end    as is_defaulted,
        case when status = 'active'     then true else false end    as is_active_contract,
        case when status = 'settled'    then true else false end    as is_settled,
        case when collateral != 'none'  then true else false end    as has_collateral,

        -- metadata
        current_timestamp                                           as _loaded_at,
        'contracts.parquet'                                         as _source
    from source
)

select * from renamed

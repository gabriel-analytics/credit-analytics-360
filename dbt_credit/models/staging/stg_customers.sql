with source as (
    select * from read_parquet('C:/Users/lineg/credit-analytics-360/gen/data/customers.parquet')
),

renamed as (
    select
        customer_id,
        name,
        cpf_hash,
        cast(birth_date as date)                                    as birth_date,
        age_group,
        acquisition_channel,
        state,
        city,
        cast(income_declared as double)                             as income_declared,
        cast(signup_date as date)                                   as signup_date,
        customer_segment,
        cast(is_active as boolean)                                  as is_active,
        cast(products_count as integer)                             as products_count,

        -- derived
        date_diff('year', cast(birth_date as date), current_date)  as age,
        case
            when income_declared = 0 then true
            else false
        end                                                         as has_zero_income,
        case
            when products_count >= 2 then true
            else false
        end                                                         as is_multi_product,

        -- metadata
        current_timestamp                                           as _loaded_at,
        'customers.parquet'                                         as _source
    from source
)

select * from renamed

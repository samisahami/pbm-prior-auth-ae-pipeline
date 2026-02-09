{{ config(materialized='view') }}

with src as (
    select * 
    from {{ source('pbm_bronze', 'requests') }}
),

clean as (
    select
        cast(request_id as bigint)         as request_id,
        cast(member_id as bigint)          as member_id,
        cast(provider_id as bigint)        as provider_id,
        cast(drug_id as bigint)            as drug_id,
        cast(payer_id as bigint)           as payer_id,
        cast(request_date as timestamp)    as request_ts,
        lower(trim(status))                as status
    from src
)

select * from clean

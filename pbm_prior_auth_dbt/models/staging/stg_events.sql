{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('pbm_bronze', 'events') }}
),

clean as (
    select
        cast(request_id as bigint)              as request_id,
        lower(trim(event_type))                 as event_type,
        cast(event_timestamp as timestamp)      as event_ts
    from src
)

select * from clean

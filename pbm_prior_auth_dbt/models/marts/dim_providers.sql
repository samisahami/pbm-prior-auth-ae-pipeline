{{ config(materialized='table') }}

select
    provider_id,
    specialty,
    state
from {{ source('pbm_bronze', 'providers') }}
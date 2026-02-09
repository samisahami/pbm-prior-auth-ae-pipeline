{{ config(materialized='table') }}

select
    drug_id,
    drug_name,
    drug_class
from {{ source('pbm_bronze', 'drugs') }}


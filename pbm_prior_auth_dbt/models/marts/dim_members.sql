{{ config(materialized='table') }}

with members as (
    select *
    from {{ source('pbm_bronze', 'members') }}
)

select
    member_id,
    member_dob,
    gender
from members

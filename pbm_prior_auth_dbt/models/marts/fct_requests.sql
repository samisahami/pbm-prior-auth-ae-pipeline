{{ config(materialized='table') }}

with requests as (
    select *
    from {{ ref('stg_requests') }}
),

members as (
    select *
    from {{ source('pbm_bronze', 'members') }}
),

providers as (
    select *
    from {{ source('pbm_bronze', 'providers') }}
),

drugs as (
    select *
    from {{ source('pbm_bronze', 'drugs') }}
),

payers as (
    select *
    from {{ source('pbm_bronze', 'payers') }}
)

select
    r.request_id,
    r.request_ts,
    r.status,

    r.member_id,
    datediff(year, m.member_dob, current_date()) as member_age,  -- Fixed!
    m.gender as member_gender,

    r.provider_id,
    p.specialty as provider_specialty,

    r.drug_id,
    d.drug_name,
    d.drug_class,

    r.payer_id,
    py.payer_name,

    -- ⭐ business metrics
    case
        when r.status = 'approved' then 1
        else 0
    end as is_approved,

    case
        when r.status = 'denied' then 1
        else 0
    end as is_denied,

    case
        when r.status = 'pending' then 1
        else 0
    end as is_pending

from requests r
left join members m
    on r.member_id = m.member_id
left join providers p
    on r.provider_id = p.provider_id
left join drugs d
    on r.drug_id = d.drug_id
left join payers py
    on r.payer_id = py.payer_id

{{ config(materialized='table') }}

with events as (
    select
        request_id,
        event_type,
        event_ts
    from {{ ref('stg_events') }}
),

requests as (
    select
        request_id,
        member_id,
        provider_id,
        drug_id,
        payer_id,
        status,
        request_ts,
        member_age
    from {{ ref('fct_requests') }}
),

joined as (
    select
        md5(concat(
            cast(e.request_id as string),
            cast(e.event_ts as string),
            e.event_type
        )) as event_sk,
        
        e.request_id,
        r.member_id,
        r.provider_id,
        r.drug_id,
        r.payer_id,
        e.event_type,
        e.event_ts,
        r.status as request_status,
        r.request_ts,
        r.member_age,
        
        case when r.status = 'approved' then 1 else 0 end as is_approved,

        datediff(minute, r.request_ts, e.event_ts) as minutes_from_request_to_event
        
    from events e
    left join requests r
        on e.request_id = r.request_id
)

select * from joined

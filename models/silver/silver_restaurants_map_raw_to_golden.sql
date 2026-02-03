{{ config(materialized='table') }}

with base as (
    select
        raw_id,
        name_clean,
        address_clean,
        phone_e164,
        phone_valid,
        case
            when phone_valid and phone_e164 is not null then phone_e164
            when name_clean is not null and address_clean is not null then md5(name_clean || '|' || address_clean)
            when name_clean is not null then md5(name_clean)
            else md5(raw_id::text)
        end as dedup_key
    from {{ ref('silver_restaurants_clean') }}
),
with_ids as (
    select
        *,
        uuid_generate_v5(uuid_nil(), dedup_key) as restaurant_id
    from base
)

select
    raw_id,
    restaurant_id,
    case
        when phone_valid and phone_e164 is not null then 'phone_exact'
        when name_clean is not null and address_clean is not null then 'name_address_exact'
        else 'fallback_key'
    end as merge_reason,
    case
        when phone_valid and phone_e164 is not null then 100.0
        when name_clean is not null and address_clean is not null then 90.0
        else 50.0
    end as similarity_score,
    current_timestamp as merged_at,
    current_timestamp as created_at
from with_ids

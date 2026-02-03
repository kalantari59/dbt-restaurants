{{ config(materialized='table') }}

with base as (
    select
        raw_id,
        name_clean,
        address_clean,
        phone_e164,
        phone_valid,
        website_domain,
        yelp_business_id,
        latitude,
        longitude,
        category,
        price_band,
        price_min,
        price_max,
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
),
agg as (
    select
        restaurant_id,
        max(name_clean) as canonical_name,
        max(address_clean) as canonical_address,
        max(phone_e164) as canonical_phone_e164,
        max(website_domain) as canonical_website_domain,
        max(yelp_business_id) as canonical_yelp_url,
        max(latitude) as canonical_latitude,
        max(longitude) as canonical_longitude,
        array_agg(distinct category) as categories,
        max(price_band) as price_band,
        min(price_min) as price_min,
        max(price_max) as price_max,
        count(*) as source_count
    from with_ids
    group by restaurant_id
)

select
    restaurant_id,
    current_timestamp as created_at,
    current_timestamp as updated_at,
    canonical_name,
    canonical_address,
    canonical_phone_e164,
    canonical_website_domain,
    canonical_yelp_url,
    canonical_latitude,
    canonical_longitude,
    categories,
    price_band,
    price_min,
    price_max,
    (
        (case when canonical_name is not null then 20 else 0 end) +
        (case when canonical_address is not null then 20 else 0 end) +
        (case when canonical_phone_e164 is not null then 20 else 0 end) +
        (case when canonical_latitude is not null and canonical_longitude is not null then 20 else 0 end) +
        (case when canonical_website_domain is not null or canonical_yelp_url is not null then 20 else 0 end)
    ) as confidence_score,
    source_count,
    current_timestamp as first_seen_at,
    current_timestamp as last_seen_at
from agg

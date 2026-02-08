{{ config(materialized='table') }}

select
    g.restaurant_id,
    g.canonical_name as name,
    g.categories[1] as category_primary,
    g.categories,
    g.price_band,
    g.canonical_address as address,
    g.canonical_latitude as latitude,
    g.canonical_longitude as longitude,
    'Toronto' as city,
    'ON' as province,
    g.canonical_phone_e164 as phone_e164,
    g.canonical_website_domain as website_domain,
    g.canonical_yelp_url as yelp_url,
    e.yelp_rating,
    e.yelp_review_count,
    coalesce(e.is_closed, false) as is_closed,
    (g.canonical_phone_e164 is not null) as has_phone,
    (g.canonical_latitude is not null and g.canonical_longitude is not null) as has_geo,
    (g.canonical_website_domain is not null) as has_website,
    g.confidence_score,
    g.source_count,
    g.first_seen_at,
    current_timestamp as last_updated_at,
    current_timestamp as created_at
from {{ ref('silver_restaurants_golden') }} g
left join {{ ref('silver_restaurants_enriched') }} e
    on g.restaurant_id = e.restaurant_id

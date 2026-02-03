{{ config(materialized='table') }}

with base as (
    select
        restaurant_id,
        --hash the restaurant_id to a number between 0 and 200
        ( ascii(substr(restaurant_id::text, 1, 1)) +
        ascii(substr(restaurant_id::text, 2, 1)) +
        ascii(substr(restaurant_id::text, 3, 1)) +
        ascii(substr(restaurant_id::text, 4, 1)) 
    ) as hash_int
    from {{ ref('silver_restaurants_golden') }}
)

select
    restaurant_id,
    --yelp rating is a random number between 3.0 and 5.0
    round(3.0 + (hash_int % 200) / 100.0, 1) as yelp_rating,
    10 + (hash_int % 490) as yelp_review_count,
    --is_closed is a random boolean  probability of 5%
    (hash_int % 20 = 0) as is_closed,
    current_timestamp as last_enriched_at,
    'mock' as enrichment_source,
    current_timestamp as created_at
from base

{{ config(materialized='table') }}

with src as (
    select * from {{ ref('trt_rest') }}
)

select
    gen_random_uuid() as raw_id,
    '{{ invocation_id }}' as load_batch_id,
    current_timestamp as ingested_at,
    'trt_rest.csv' as source_file,
    "Category" as category,
    "Restaurant Address" as restaurant_address,
    "Restaurant Name" as restaurant_name,
    "Restaurant Phone" as restaurant_phone,
    "Restaurant Price Range" as restaurant_price_range,
    "Restaurant Website" as restaurant_website,
    "Restaurant Yelp URL" as restaurant_yelp_url,
    "Restaurant Latitude" as restaurant_latitude,
    "Restaurant Longitude" as restaurant_longitude
from src

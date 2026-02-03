{{ config(materialized='table') }}

select
    restaurant_id,
    'success' as status,
    1 as attempts,
    current_timestamp as last_enriched_at,
    null::text as error_message,
    current_timestamp as created_at,
    current_timestamp as updated_at
from {{ ref('silver_restaurants_golden') }}

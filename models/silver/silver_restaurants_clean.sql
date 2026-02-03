{{ config(materialized='table') }}

with src as (
    select * from {{ ref('bronze_restaurants_raw') }}
),
base as (
    select
        raw_id,
        current_timestamp as clean_updated_at,
        --collapsing all spaces (space, tab, newline) into a single space,
        lower(trim(regexp_replace(restaurant_name, '\s+', ' ', 'g'))) as name_clean,
        --lower(trim({{ regex_collapse_whitespace('restaurant_name') }})) as name_clean,
        
        --collapsing all newlines and carriage returns into a single space,collapsing all spaces (space, tab, newline) into a single space,
        trim(regexp_replace(regexp_replace(restaurant_address, '\r|\n', ' ', 'g'), '\s+', ' ', 'g')) as address_clean,
        --removing all non-digits
        regexp_replace(restaurant_phone, '\D', '', 'g') as phone_digits,
        restaurant_price_range,
        restaurant_website,
        restaurant_yelp_url,
        restaurant_latitude,
        restaurant_longitude,
        category
    from src
),
phones as (
    select
        *,
        case
            --if the phone number is 10 digits, add a +1 to the beginning
            when length(phone_digits) = 10 then '+1' || phone_digits
            --if the phone number is 11 digits and the first digit is a 1, add a + to the beginning
            when length(phone_digits) = 11 and left(phone_digits, 1) = '1' then '+' || phone_digits
            else null
        end as phone_e164,
        case
            when length(phone_digits) = 10 then true
            when length(phone_digits) = 11 and left(phone_digits, 1) = '1' then true
            else false
        end as phone_valid
    from base
),
-- Price types :
-- 1. Under $10
-- 2. Range of numbers  $20-$45
-- 3. Single number with a +  $61+
prices as (
    select
        *,
        case
            --if the price range is 'under $10', set the price min to 0
            when restaurant_price_range ilike 'under%' then 0
            --if the price range is a range of numbers, split the range into a min and max and convert to integer
            when restaurant_price_range ~ '\d+\s*[-–]\s*\d+' then split_part(regexp_replace(restaurant_price_range, '[^0-9\-]', '', 'g'), '-', 1)::int
            --if the price range is a single number with a +, convert to integer
            when restaurant_price_range ~ '\d+\+' then regexp_replace(restaurant_price_range, '[^0-9]', '', 'g')::int
            else null
        end as price_min,
        case
            when restaurant_price_range ilike 'under%' then regexp_replace(restaurant_price_range, '[^0-9]', '', 'g')::int
            when restaurant_price_range ~ '\d+\s*[-–]\s*\d+' then split_part(regexp_replace(restaurant_price_range, '[^0-9\-]', '', 'g'), '-', 2)::int
            else null
        end as price_max
    from phones
),
urls as (
    select
        *,
        --removing all non-alphanumeric characters and converting to lowercase  and removing the path from the website
        nullif(lower(regexp_replace(regexp_replace(restaurant_website, '^https?://', ''), '/.*$', '')), '') as website_domain,
        case
            --if the yelp url contains 'yelp.', then it is a valid yelp url
            when restaurant_yelp_url ilike '%yelp.%' then true
            --otherwise, it is not a valid yelp url
            else false
        end as yelp_url_valid,
        --extracting the business id from the yelp url
        substring(restaurant_yelp_url from '/biz/([^/?]+)') as yelp_business_id
    from prices
),
geo as (
    select
        *,
        case
            --if the latitude is between 43.0 and 44.0 and the longitude is between -80.0 and -79.0, then it is a valid geo location for toronto
            when restaurant_latitude between 43.0 and 44.0
             and restaurant_longitude between -80.0 and -79.0
            then true
            else false
        end as geo_valid
    from urls
)

select
    raw_id,
    clean_updated_at,
    name_clean,
    address_clean,
    phone_e164,
    phone_valid,
    'CA' as phone_country,
    price_min,
    price_max,
    case
        when restaurant_price_range ilike 'under%' then 'Under $10'
        --if the price range is a range of numbers, format the price range as $20-$45
        when restaurant_price_range ~ '\d+\s*[-–]\s*\d+' then
            '$' || split_part(regexp_replace(restaurant_price_range, '[^0-9\-]', '', 'g'), '-', 1) ||
            '-' || split_part(regexp_replace(restaurant_price_range, '[^0-9\-]', '', 'g'), '-', 2)
        when restaurant_price_range ~ '\d+\+' then '$61+'
        else null
    end as price_band,
    website_domain,
    yelp_url_valid,
    yelp_business_id,
    --converting the latitude and longitude to a numeric value with 8 decimal places
    restaurant_latitude::numeric(10,8) as latitude,
    restaurant_longitude::numeric(11,8) as longitude,
    geo_valid,
    --creating a jsonb object with the data quality flags
    jsonb_strip_nulls(jsonb_build_object(
        'missing_phone', phone_e164 is null,
        --if the phone number is missing, set the data quality flag to true
        'invalid_phone', phone_valid = false,
        'missing_geo', restaurant_latitude is null or restaurant_longitude is null,
        'invalid_geo', geo_valid = false,
        'price_parse_failed', price_min is null and price_max is null,
        'bad_yelp_url', restaurant_yelp_url is not null and yelp_url_valid = false
    )) as dq_flags,
    category
from geo

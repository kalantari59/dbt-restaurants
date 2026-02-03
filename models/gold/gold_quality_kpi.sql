{{ config(materialized='table') }}

with raw_stats as (
    select count(*) as raw_count from {{ ref('bronze_restaurants_raw') }}
),
clean_stats as (
    select
        count(*) as clean_count,
        count(case when not phone_valid then 1 end) as missing_phone,
        count(case when phone_e164 is not null and not phone_valid then 1 end) as invalid_phone,
        count(case when not geo_valid then 1 end) as missing_geo,
        count(case when latitude is not null and not geo_valid then 1 end) as invalid_geo,
        count(case when not (price_min is not null or price_max is not null) then 1 end) as price_parse_failed
    from {{ ref('silver_restaurants_clean') }}
),
golden_stats as (
    select count(*) as golden_count from {{ ref('silver_restaurants_golden') }}
),
enrich_stats as (
    select
        count(*) as total_enrich,
        count(case when status = 'success' then 1 end) as success_count,
        count(case when status = 'failed' then 1 end) as failed_count,
        count(case when status = 'pending' then 1 end) as pending_count
    from {{ ref('enrichment_status') }}
)

select
    current_date as kpi_date,
    '{{ invocation_id }}' as run_id,
    rs.raw_count,
    cs.clean_count,
    gs.golden_count,
    case when rs.raw_count > 0
        then round(100.0 * (1 - gs.golden_count::numeric / rs.raw_count::numeric), 2)
        else 0 end as dedup_rate,
    case when cs.clean_count > 0
        then round(100.0 * cs.missing_phone::numeric / cs.clean_count::numeric, 2)
        else 0 end as missing_phone_rate,
    case when cs.clean_count > 0
        then round(100.0 * cs.invalid_phone::numeric / cs.clean_count::numeric, 2)
        else 0 end as invalid_phone_rate,
    case when cs.clean_count > 0
        then round(100.0 * cs.missing_geo::numeric / cs.clean_count::numeric, 2)
        else 0 end as missing_geo_rate,
    case when cs.clean_count > 0
        then round(100.0 * cs.invalid_geo::numeric / cs.clean_count::numeric, 2)
        else 0 end as invalid_geo_rate,
    case when cs.clean_count > 0
        then round(100.0 * cs.price_parse_failed::numeric / cs.clean_count::numeric, 2)
        else 0 end as price_parse_failed_rate,
    case when es.total_enrich > 0
        then round(100.0 * es.success_count::numeric / es.total_enrich::numeric, 2)
        else 0 end as enrichment_success_rate,
    case when es.total_enrich > 0
        then round(100.0 * es.failed_count::numeric / es.total_enrich::numeric, 2)
        else 0 end as enrichment_failed_rate,
    es.pending_count as enrichment_pending_count,
    current_timestamp as calculated_at
from raw_stats rs
cross join clean_stats cs
cross join golden_stats gs
cross join enrich_stats es

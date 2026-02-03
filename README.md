# Toronto Restaurants dbt Project

This folder contains a dbt version of the Toronto Restaurants Data Platform.

## Quick Start

1) Install dbt-core + dbt-postgres
```
pip install dbt-core dbt-postgres
```

2) Create the database (Postgres must be running)
```
psql -U postgres -h localhost -p 5432 -c "CREATE DATABASE restaurants_dbt;"
```

3) Copy profile
```
mkdir %USERPROFILE%\.dbt
copy profiles.example.yml %USERPROFILE%\.dbt\profiles.yml
```

4) Seed the raw CSV
```
dbt seed --project-dir .
```

5) Run models
```
dbt run --project-dir .
```

6) Run tests
```
dbt test --project-dir .
```

## Project Layers

- `models/bronze`: Raw ingestion from seed (bronze layer)
- `models/silver`: Cleaning, deduplication, enrichment (silver layer)
- `models/gold`: BI-ready tables and KPIs (gold layer)

## Model Guide

### Bronze

- `models/bronze/bronze_restaurants_raw.sql`
  - Source: `seeds/trt_rest.csv` via `ref('trt_rest')`
  - Adds ingestion metadata (`raw_id`, `load_batch_id`, `ingested_at`, `source_file`)
  - Normalizes column names into a consistent schema for downstream models

### Silver

- `models/silver/silver_restaurants_clean.sql`
  - Cleans and standardizes name, address, and phone
  - Parses price ranges into `price_min`, `price_max`, and `price_band`
  - Extracts website domain and Yelp business id
  - Validates geo bounds for Toronto; builds `dq_flags`

- `models/silver/silver_restaurants_golden.sql`
  - Builds a stable `restaurant_id` using deterministic UUID v5
  - Deduplicates to a canonical record per restaurant
  - Computes `confidence_score`, aggregates categories, and tracks `source_count`

- `models/silver/silver_restaurants_map_raw_to_golden.sql`
  - Maps each raw record to `restaurant_id`
  - Explains merge logic with `merge_reason` and `similarity_score`

- `models/silver/silver_restaurants_enriched.sql`
  - Adds mock enrichment fields (rating, review count, closed flag)
  - Simulates an external enrichment source

- `models/silver/enrichment_status.sql`
  - Tracks enrichment status per `restaurant_id`
  - Currently marks all records as `success`

### Gold

- `models/gold/gold_restaurant_dim.sql`
  - Final restaurant dimension for BI use
  - Joins golden records with enrichment fields
  - Adds convenience booleans (`has_phone`, `has_geo`, `has_website`)

- `models/gold/gold_quality_kpi.sql`
  - Daily data quality KPIs: dedup rate, missing/invalid phone/geo, price parse failure
  - Enrichment success/failure rates

### Tests

- `models/schema.yml`
  - Column-level `unique` and `not_null` tests for core identifiers and names

## Regex Cheat Sheet

### Used in this project

Patterns and where they appear (Postgres regex syntax):

- `\s+` (collapse multiple whitespace)  
  Used in `silver_restaurants_clean.sql` for name/address cleanup.
- `\r|\n` (line breaks)  
  Used to remove newlines from addresses.
- `\D` (non-digit)  
  Used to strip phone numbers to digits only.
- `\d+\s*[-–]\s*\d+` (numeric range like `10-20` or `10 – 20`)  
  Used to parse price ranges.
- `\d+\+` (numbers followed by `+`)  
  Used to detect `61+` style price ranges.
- `[^0-9\-]` (anything except digits or `-`)  
  Used to clean price range strings before splitting.
- `[^0-9]` (anything except digits)  
  Used to extract numeric values from strings.
- `^https?://` (http/https prefix at start)  
  Used to strip protocol from website URLs.
- `/.*$` (remove everything after first slash)  
  Used to extract the base domain from URLs.
- `/biz/([^/?]+)` (capture Yelp business id)  
  Used with `substring(... from ...)` to extract Yelp IDs.

### Not used yet (common patterns)

- `^` / `$` anchors for full-string validation (e.g., `^\d{10}$`)
- Character classes like `\w`, `\b`, `\S`
- Quantifiers like `{n}` / `{n,m}` for fixed-length checks
- Alternation with groups (e.g., `(foo|bar)`)
- Lookarounds (e.g., `(?=...)`, `(?!...)`)

## Notes

- Seeds are loaded from `seeds/trt_rest.csv`
- Extensions `pgcrypto`, `pg_trgm`, and `uuid-ossp` are created on-run

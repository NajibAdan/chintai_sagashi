{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='listing_id'
) }}
with snapshots as (

    select *
    from {{ ref('fct_listing_snapshots') }}

)

select
    listing_id,

    min(crawl_date) as first_seen_date,
    max(crawl_date) as last_seen_date,

    count(distinct crawl_date) as observed_days,

    date_diff(
        'day',
        min(crawl_date),
        max(crawl_date)
    ) + 1 as listing_span_days,

    arg_min(rent_yen, crawl_date)
        as first_seen_rent_yen,

    arg_max(rent_yen, crawl_date)
        as last_seen_rent_yen,

    min(rent_yen) as minimum_rent_yen,
    max(rent_yen) as maximum_rent_yen,

    max(rent_yen) - min(rent_yen)
        as rent_range_yen,

    arg_max(city, crawl_date) as city,
    arg_max(property_name, crawl_date) as property_name,
    arg_max(area_m2, crawl_date) as area_m2,
    arg_max(floor_plan, crawl_date) as floor_plan

from snapshots

group by listing_id
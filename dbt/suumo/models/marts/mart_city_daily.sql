{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='crawl_date',
    begin='2025-10-10',
    batch_size='day',
    partitioned_by=['crawl_date']
) }}
select
    crawl_date,
    prefecture,
    city,

    count(*) as listing_count,

    median(rent_yen) as median_rent_yen,

    median(area_m2) as median_area_m2,

    median(rent_per_m2) as median_rent_per_m2,

    count(*) filter (
        where floor_plan = '1K'
    ) as one_k_listing_count,

    count(*) filter (
        where floor_plan = '1DK'
    ) as one_dk_listing_count,

    count(*) filter (
        where floor_plan = '1LDK'
    ) as one_ldk_listing_count,

from {{ ref('fct_listing_snapshots') }}

group by
    crawl_date,
    prefecture,
    city
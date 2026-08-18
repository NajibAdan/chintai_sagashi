{{ config(
    materialized='external',
     location='s3://{{ env_var("BUCKET_NAME") }}/data/curated/{{ model.name }}.parquet'
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

order by 
    crawl_date desc,
    count(*)
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='crawl_date',
    begin='2025-10-10',
    batch_size='day',
    partitioned_by=['crawl_date']
) }}

with listings as (

    select *
    from {{ ref('stg_suumo_listings') }}
),

deduplicated as (

    select *
    from listings

    qualify row_number() over (
        partition by listing_id, crawl_date
        order by source_file
    ) = 1

)

select
    listing_id,
    crawl_date,

    listing_id || '_' || cast(crawl_date as varchar)
        as listing_snapshot_id,

    prefecture,
    city,

    url,

    property_name,
    location,

    rent_yen,
    management_fee,

    rent_yen + coalesce(management_fee, 0)
        as monthly_cost_yen,
    deposit,
    gratuity,
    floor_plan,
    area_m2,

    rent_yen / nullif(area_m2, 0)
        as rent_per_m2,

    floor,

    building_age,
    building_type,

    stations

from deduplicated
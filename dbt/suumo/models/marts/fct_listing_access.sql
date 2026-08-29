{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='crawl_date',
    begin='2025-10-10',
    batch_size='day',
    partitioned_by=['crawl_date']
) }}

with snapshots as (

    select
        listing_id,
        crawl_date,
        prefecture,
        city,
        stations

    from {{ ref('fct_listing_snapshots') }}

    {% if is_incremental() %}

    where crawl_date > (
        select coalesce(
            max(crawl_date),
            date '1900-01-01'
        )
        from {{ this }}
    )

    {% endif %}

),

unnested as (

    select
        listing_id,
        crawl_date,
        prefecture,
        city,

        unnest(stations) as access,
        generate_subscripts(stations, 1) as access_id

    from snapshots

),

classified as (

    select
        *,

        access like '%バス%' as is_bus,
        access like '%駅%' as is_train,
        access like '%車%' as is_car

    from unnested

)

select
    listing_id,
    crawl_date,
    city,
    prefecture,

    access_id,
    access,

    is_train,

    case
        when is_train then
            try_cast(
                regexp_extract(access, '歩([0-9]+)分', 1)
                as integer
            )
    end as station_walk_minutes,

    is_bus,

    case
        when is_bus then
            try_cast(
                regexp_extract(access, 'バス([0-9]+)分', 1)
                as integer
            )
    end as bus_minutes,

    is_car,

    case
        when is_car then
            try_cast(
                regexp_extract(access, '車([0-9]+)分', 1)
                as integer
            )
    end as station_drive_minutes

from classified
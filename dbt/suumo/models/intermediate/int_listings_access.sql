with source as (
    select * from {{ ref('stg_suumo_listings') }}
),
unnested as (
select
    listing_id,
    crawl_date,
    prefecture,
    city,
    unnest(stations, recursive := true) as access
from source
),

access_type_classification as (
    select *,
    case when access like '%バス%' then 1 else 0 end as is_bus,
    case when access like '%駅%' then 1 else 0 end as is_train,
    case when access like '%車%' then 1 else 0 end as is_car
    from unnested
),

distance_extract as (
    select 
        listing_id,
        crawl_date,
        prefecture,
        city,
        access,
        is_bus,
        row_number() over(partition by listing_id) as access_id,
        case when is_bus then  
            regexp_extract(
                access,
                'バス([0-9]+)分',
                1
            ) else null end as bus_minutes,
        is_train,
        case when is_train then  
            regexp_extract(
                access,
                '歩([0-9]+)分',
                1
            ) else null end as station_walk_minutes,
        is_car,
        case when is_car then  
            regexp_extract(
                access,
                '車([0-9]+)分',
                1
            ) else null end as station_drive_minutes
    from access_type_classification
),

order_results as (

    select * from distance_extract
    order by listing_id, access_id
)

select * from order_results
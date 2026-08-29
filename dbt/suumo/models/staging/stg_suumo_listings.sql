{{ config(
    materialized='view',
    event_time='crawl_date'
) }}
with source as (

    select *
    from {{ source('suumo_raw', 'listings') }}

),

cleaned as (

    select
        regexp_extract(
            url,
            'bc=([0-9]+)',
            1
        ) as listing_id,

        cast(crawl_date as date) as crawl_date,
        cast(REPLACE(cast(crawl_ts as VARCHAR), '+00:00Z', '+00:00') as TIMESTAMPTZ) as crawl_ts,
        location,
        prefecture,
        city,
        stations,

        url,
        property_name,

        {{ parse_jpy('monthly_rent') }} as rent_yen,
        {{ parse_jpy('deposit') }} as deposit,
        {{ parse_jpy('gratuity') }} as gratuity,
        {{ parse_jpy('management_fee') }} as management_fee,
        madori as floor_plan,
        cast(REPLACE(menseki, 'm2', '') as double) as area_m2,
        apartment_floor as floor,
        case when building_age = '新築' then 1 else 0 end as is_building_new,
        case when building_age = '新築' then '0' else 
            regexp_extract(
            building_age,
            '築([0-9]+)年',
            1
        ) end as building_age,

        building_type,
        url_key,

        filename as source_file

    from source

)

select *
from cleaned
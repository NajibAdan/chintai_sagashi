{{ config(
    materialized='external',
    location='s3://{{ env_var("BUCKET_NAME") }}/data/curated/{{ model.name }}.parquet'
) }}

select 
    listing_id,
    crawl_date,
    city,
    prefecture,
    access_id,
    access,
    is_train,
    station_walk_minutes,
    is_bus,
    bus_minutes as bus_minutes,
    is_car,
    station_drive_minutes

from {{ ref('int_listing_access') }}


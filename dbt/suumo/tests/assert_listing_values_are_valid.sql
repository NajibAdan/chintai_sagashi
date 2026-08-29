select
    listing_id,
    crawl_date,
    monthly_cost_yen,
    area_m2
from {{ ref('fct_listing_snapshots') }}
where
    monthly_cost_yen <= 0
    or area_m2 <= 0
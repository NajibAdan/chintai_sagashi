select
    listing_id,
    crawl_date
from {{ ref('fct_listing_snapshots') }}
where crawl_date > current_date
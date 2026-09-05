select 
    listing_id,
    crawl_date,
    count(1) as row_count
from {{ ref('fct_listing_snapshots')}}
group by 
    listing_id,
    crawl_date
having count(1) > 1
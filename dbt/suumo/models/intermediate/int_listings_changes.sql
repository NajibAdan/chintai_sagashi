with snapshots as (

    select *
    from {{ ref('fct_listing_snapshots') }}

),

with_previous as (

    select
        *,

        lag(crawl_date) over (
            partition by listing_id
            order by crawl_date
        ) as previous_crawl_date,

        lag(rent_yen) over (
            partition by listing_id
            order by crawl_date
        ) as previous_rent_yen

    from snapshots

)

select
    *,

    rent_yen - previous_rent_yen
        as rent_change_yen,

    case
        when previous_rent_yen is null then 'first_seen'
        when rent_yen < previous_rent_yen then 'rent_decreased'
        when rent_yen > previous_rent_yen then 'rent_increased'
        else 'unchanged'
    end as change_type

from with_previous
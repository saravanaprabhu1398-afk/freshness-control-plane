-- One row per directional market (trip component) on a ticket in the 10% sample.
select
    itin_id,
    mkt_id,
    cast(year as integer)                     as year,
    cast(quarter as integer)                  as quarter,
    cast(year as varchar) || '-Q' || cast(quarter as varchar) as period,
    origin,
    dest,
    ticketing_carrier                         as carrier,
    operating_carrier,
    cast(mkt_coupons as integer)              as coupons,
    cast(mkt_coupons as integer) = 1          as is_nonstop,
    cast(passengers as double)                as passengers,
    cast(market_fare_usd as double)           as fare_usd,
    cast(market_distance_mi as double)        as distance_mi,
    _loaded_at
from {{ source('raw', 'db1b_market') }}

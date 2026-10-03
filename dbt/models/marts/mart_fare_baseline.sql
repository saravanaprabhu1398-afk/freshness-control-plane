-- Real fare baseline per nonstop route and ticketing carrier for the latest DB1B quarter.
-- Only cells with enough sampled tickets are kept, so medians are not driven by a handful of fares.
{% set min_sample = 30 %}
with markets as (
    select * from {{ ref('stg_db1b_market') }}
    where is_nonstop
),
latest as (
    select max(year * 10 + quarter) as yq from markets
)
select
    origin || ':' || dest || ':' || carrier || ':ALL'    as fare_key,
    origin,
    dest,
    carrier,
    'ALL'                                                as cabin,
    any_value(period)                                    as period,
    any_value(year)                                      as year,
    any_value(quarter)                                   as quarter,
    count(*)                                             as sample_size,
    sum(passengers)                                      as sampled_passengers,
    round(quantile_cont(fare_usd, 0.5), 2)               as median_fare_usd,
    round(quantile_cont(fare_usd, 0.25), 2)              as p25_fare_usd,
    round(quantile_cont(fare_usd, 0.75), 2)              as p75_fare_usd,
    round(avg(fare_usd), 2)                              as mean_fare_usd,
    max(_loaded_at)                                      as loaded_at
from markets, latest
where markets.year * 10 + markets.quarter = latest.yq
group by origin, dest, carrier
having count(*) >= {{ min_sample }}

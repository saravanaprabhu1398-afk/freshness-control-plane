-- p25 <= median <= p75, and fares stay inside the load filter bounds.
select fare_key from {{ ref('mart_fare_baseline') }}
where not (p25_fare_usd <= median_fare_usd and median_fare_usd <= p75_fare_usd)
   or median_fare_usd not between 25 and 2500

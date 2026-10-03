-- Historical on-time performance per route, carrier and month (ground truth for ops questions).
with flights as (
    select * from {{ ref('stg_bts_ontime') }}
)
select
    origin || '-' || dest || '-' || carrier || '-' || cast(year as varchar) || '-' || lpad(cast(month as varchar), 2, '0')
                                                         as route_month_key,
    origin,
    dest,
    carrier,
    year,
    month,
    count(*)                                             as scheduled_flights,
    count(*) filter (where is_cancelled)                 as cancelled_flights,
    count(*) filter (where is_diverted)                  as diverted_flights,
    count(*) filter (where is_on_time_arrival)           as on_time_arrivals,
    count(*) filter (where is_on_time_arrival) / count(*)::double              as on_time_rate,
    count(*) filter (where is_cancelled) / count(*)::double                    as cancellation_rate,
    avg(arr_delay_min) filter (where not is_cancelled and not is_diverted)      as avg_arr_delay_min,
    quantile_cont(arr_delay_min, 0.5) filter (where not is_cancelled and not is_diverted) as p50_arr_delay_min,
    quantile_cont(arr_delay_min, 0.9) filter (where not is_cancelled and not is_diverted) as p90_arr_delay_min,
    max(_loaded_at)                                      as loaded_at
from flights
group by origin, dest, carrier, year, month

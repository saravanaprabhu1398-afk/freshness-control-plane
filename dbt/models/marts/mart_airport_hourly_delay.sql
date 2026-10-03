-- Typical departure delay by tracked origin airport and scheduled local hour, per month.
-- Gives the agent historical context for a live flight ("ORD at 18:00 usually runs ~25 min late").
with flights as (
    select * from {{ ref('stg_bts_ontime') }}
    where origin in ('ATL', 'ORD', 'DFW', 'DEN', 'LAX', 'JFK', 'SFO', 'SEA')
      and sched_dep_hour_local is not null
)
select
    origin || '-' || lpad(cast(sched_dep_hour_local as varchar), 2, '0') || '-' || cast(year as varchar)
        || '-' || lpad(cast(month as varchar), 2, '0')   as airport_hour_key,
    origin                                               as airport,
    sched_dep_hour_local,
    year,
    month,
    count(*)                                             as scheduled_departures,
    avg(dep_delay_min) filter (where not is_cancelled)   as avg_dep_delay_min,
    quantile_cont(dep_delay_min, 0.5) filter (where not is_cancelled) as p50_dep_delay_min,
    count(*) filter (where is_cancelled) / count(*)::double as cancellation_rate
from flights
group by origin, sched_dep_hour_local, year, month

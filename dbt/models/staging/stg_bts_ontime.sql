-- One row per scheduled flight that touches a tracked airport. Typed, renamed, flags as booleans.
select
    cast(flight_date as date)                                  as flight_date,
    cast(year as integer)                                      as year,
    cast(month as integer)                                     as month,
    carrier,
    flight_number,
    nullif(trim(tail_number), '')                              as tail_number,
    origin,
    dest,
    {{ hhmm_to_hour('crs_dep_time') }}                         as sched_dep_hour_local,
    {{ hhmm_to_hour('crs_arr_time') }}                         as sched_arr_hour_local,
    cast(dep_delay_min as double)                              as dep_delay_min,
    cast(arr_delay_min as double)                              as arr_delay_min,
    coalesce(cast(cancelled as double), 0) = 1                 as is_cancelled,
    coalesce(cast(diverted as double), 0) = 1                  as is_diverted,
    nullif(trim(cancellation_code), '')                        as cancellation_code,
    -- BTS definition: arrived less than 15 minutes late
    coalesce(cast(cancelled as double), 0) = 0
      and coalesce(cast(diverted as double), 0) = 0
      and cast(arr_del15 as double) = 0                        as is_on_time_arrival,
    cast(distance_mi as double)                                as distance_mi,
    cast(carrier_delay_min as double)                          as carrier_delay_min,
    cast(weather_delay_min as double)                          as weather_delay_min,
    cast(nas_delay_min as double)                              as nas_delay_min,
    cast(security_delay_min as double)                         as security_delay_min,
    cast(late_aircraft_delay_min as double)                    as late_aircraft_delay_min,
    _loaded_at
from {{ source('raw', 'bts_ontime') }}

-- Rates must lie in [0, 1].
select route_month_key from {{ ref('mart_route_ontime') }}
where on_time_rate not between 0 and 1 or cancellation_rate not between 0 and 1

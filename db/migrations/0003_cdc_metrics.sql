-- CDC observability (Phase 2): per-window event counts and latency, written by `fcp cdc stats`.
--   capture latency : Postgres commit (source.ts_ms) -> Debezium processed the change (ts_ms)
--   delivery latency: Postgres commit -> our consumer received the Kafka message

create table metrics.cdc_window (
  id                 bigserial primary key,
  window_start       timestamptz not null,
  window_end         timestamptz not null,
  table_name         text        not null,
  op                 char(1)     not null,
  events             integer     not null,
  p50_capture_ms     integer,
  p95_capture_ms     integer,
  p50_delivery_ms    integer,
  p95_delivery_ms    integer
);
create index on metrics.cdc_window (window_end desc);

-- Freshness Control Plane: database schema.
-- Runs once, on first start of the postgres container (docker-entrypoint-initdb.d).
-- Design: docs/SDD.md §2.

create extension if not exists vector;

create schema if not exists source;     -- operational state, watched by CDC (Debezium)
create schema if not exists telemetry;  -- high-churn observations, deliberately NOT captured by CDC
create schema if not exists freshness;  -- registry, contracts, breaches
create schema if not exists index;      -- vector index (Phase 3)
create schema if not exists metrics;    -- everything the dashboard reads
create schema if not exists ops;        -- operational bookkeeping (API budgets)

-- ---------------------------------------------------------------------------
-- source.*  : business state. A row changes only when a business fact changes,
-- so every WAL entry here is a meaningful change event (ADR-001).
-- ---------------------------------------------------------------------------

create table source.flight_state (
  flight_key       text primary key,            -- icao24:callsign:first_seen_epoch
  icao24           text        not null,
  callsign         text        not null,
  status           text        not null check (status in ('airborne', 'on_ground', 'landed')),
  near_airport     text,                        -- IATA of tracked airport where last observed
  est_dep_airport  text,                        -- ICAO, from OpenSky flights endpoint
  est_arr_airport  text,
  first_seen       timestamptz not null,
  last_seen        timestamptz,                 -- set once OpenSky's batch flight record arrives
  source           text        not null default 'opensky',
  simulated        boolean     not null default false,
  updated_at       timestamptz not null default now()   -- time of last business change
);
create index on source.flight_state (icao24, callsign);

create table source.fare (
  fare_key         text primary key,            -- origin:dest:carrier:cabin
  origin           text          not null,      -- IATA
  dest             text          not null,
  carrier          text          not null,
  cabin            text          not null,      -- 'ALL' for DB1B market fares (no cabin in DB1BMarket)
  fare_usd         numeric(10,2) not null,
  baseline_usd     numeric(10,2) not null,      -- DB1B route x carrier median
  baseline_period  text          not null,      -- e.g. '2025-Q2'
  sample_size      integer       not null,
  effective_at     timestamptz   not null,      -- when this fare became the current fare
  source           text          not null check (source in ('bts_db1b', 'drift_sim')),
  simulated        boolean       not null,
  updated_at       timestamptz   not null default now()
);

create table source.airport (
  ident            text primary key,            -- OurAirports ident (ICAO for large US airports)
  iata             text,
  name             text          not null,
  municipality     text,
  region           text,
  type             text          not null,
  latitude         double precision not null,
  longitude        double precision not null,
  elevation_ft     integer,
  source           text          not null default 'ourairports',
  updated_at       timestamptz   not null default now()
);
create index on source.airport (iata);

-- Full before-images in change events (SDD §3).
alter table source.flight_state replica identity full;
alter table source.fare         replica identity full;
alter table source.airport      replica identity full;

-- Debezium uses this publication (Phase 2).
create publication fcp_source for table source.flight_state, source.fare, source.airport;

-- ---------------------------------------------------------------------------
-- telemetry.* : positions change every poll. Keeping them out of source.* means
-- a moving aircraft does not create a CDC event or an embedding every 5 minutes.
-- ---------------------------------------------------------------------------

create table telemetry.flight_position (
  flight_key       text primary key references source.flight_state (flight_key) on delete cascade,
  icao24           text        not null,
  callsign         text        not null,
  first_observed   timestamptz not null,
  last_observed    timestamptz not null,
  last_contact     timestamptz not null,
  latitude         double precision,
  longitude        double precision,
  baro_altitude_m  double precision,
  velocity_ms      double precision,
  on_ground        boolean     not null
);
create index on telemetry.flight_position (icao24, callsign, last_observed desc);

-- ---------------------------------------------------------------------------
-- freshness.* : one registry row per record (or per dataset for lake tables).
-- Two clocks, kept separate on purpose:
--   last_verified_at : when we last confirmed the value against its source
--   data_as_of       : the point in time the value describes
-- Contracts choose which clock their SLA is measured on.
-- ---------------------------------------------------------------------------

create table freshness.contract (
  name             text primary key,            -- e.g. 'flight_status', 'fare'
  measure          text     not null check (measure in ('last_verified_at', 'data_as_of')),
  max_age          interval not null,
  on_breach        text     not null check (on_breach in ('flag', 'warn')),
  description      text,
  version          integer  not null,
  loaded_at        timestamptz not null default now()
);

create table freshness.registry (
  record_key       text primary key,            -- '<table>:<pk>' or 'dataset:<name>'
  contract         text        not null,
  last_verified_at timestamptz not null,
  last_changed_at  timestamptz not null,
  data_as_of       timestamptz not null
);
create index on freshness.registry (contract, last_verified_at);

create table freshness.sla_breach (
  id               bigserial primary key,
  contract         text        not null,
  record_key       text        not null,
  age              interval    not null,
  max_age          interval    not null,
  detected_at      timestamptz not null default now(),
  resolved_at      timestamptz
);
create unique index on freshness.sla_breach (record_key) where resolved_at is null;

-- ---------------------------------------------------------------------------
-- index.* : vector index (populated from Phase 3)
-- ---------------------------------------------------------------------------

create table index.chunks (
  chunk_id         text primary key,
  source_table     text        not null,
  record_key       text        not null,
  chunk_text       text        not null,
  content_hash     char(64)    not null,
  embedding        vector(384) not null,
  source           text        not null,
  simulated        boolean     not null,
  data_as_of       timestamptz not null,
  indexed_at       timestamptz not null default now()
);
create index on index.chunks using hnsw (embedding vector_cosine_ops);
create index on index.chunks (record_key);

-- ---------------------------------------------------------------------------
-- metrics.* : every stage writes here; the dashboard reads only from here
-- ---------------------------------------------------------------------------

create table metrics.ingest_run (
  id               bigserial primary key,
  job              text        not null,        -- e.g. 'opensky_states'
  started_at       timestamptz not null,
  finished_at      timestamptz,
  status           text        not null check (status in ('running', 'succeeded', 'failed', 'skipped')),
  rows_seen        integer,
  rows_changed     integer,
  rows_unchanged   integer,
  credits_used     integer,
  detail           jsonb
);
create index on metrics.ingest_run (job, started_at desc);

create table metrics.reindex_log (
  id                        bigserial primary key,
  event_ts                  timestamptz not null,
  kafka_offset              bigint,
  source_table              text        not null,
  record_key                text        not null,
  op                        char(1)     not null,
  chunks_considered         integer     not null,
  chunks_reembedded         integer     not null,
  chunks_skipped_same_hash  integer     not null,
  embed_ms                  integer,
  e2e_latency_ms            integer
);

create table metrics.full_rebuild_log (
  id               bigserial primary key,
  run_ts           timestamptz not null,
  total_chunks     integer     not null,
  embed_ms         integer     not null
);

create table metrics.agent_query_log (
  id               bigserial primary key,
  ts               timestamptz not null default now(),
  question         text        not null,
  mode             text        not null check (mode in ('naive', 'aware')),
  cited_chunks     text[]      not null,
  max_age          interval,
  label            text,
  answer           text        not null
);

-- ---------------------------------------------------------------------------
-- ops.* : daily API credit budgets (AR-7). Reservation is a single atomic UPDATE.
-- ---------------------------------------------------------------------------

create table ops.api_budget (
  provider         text    not null,            -- 'opensky'
  endpoint_group   text    not null,            -- OpenSky budgets are per endpoint
  day              date    not null,
  credit_limit     integer not null,
  credits_used     integer not null default 0,
  updated_at       timestamptz not null default now(),
  primary key (provider, endpoint_group, day)
);

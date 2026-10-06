-- Phase 3: selective re-indexing.

-- Which version of the source record a chunk was built from (the row's updated_at). If the
-- source row has changed since (source.<table>.updated_at > chunks.source_changed_at), the index
-- is behind the source and the chunk is stale even when the source itself was verified a minute
-- ago (ADR-015). Both values come from the same Postgres clock, so the comparison is exact.
alter table index.chunks add column source_changed_at timestamptz;
alter table index.chunks add column embed_model text;
alter table index.chunks add column tokens integer;
create index on index.chunks (source_table);

-- Per-record outcome of the consumer.
alter table metrics.reindex_log add column batch_id bigint;
alter table metrics.reindex_log add column action text
  check (action in ('embedded', 'skipped_same_hash', 'deleted', 'deleted_missing'));
alter table metrics.reindex_log add column tokens integer;
create index on metrics.reindex_log (event_ts desc);

-- Per-batch totals: the savings evidence.
create table metrics.reindex_batch (
  id               bigserial primary key,
  started_at       timestamptz not null,
  finished_at      timestamptz not null,
  events           integer not null,     -- Kafka messages in the batch
  records          integer not null,     -- distinct records after collapsing
  collapsed        integer not null,     -- events superseded by a later event for the same record
  embedded         integer not null,
  skipped_same_hash integer not null,
  deleted          integer not null,
  tokens           integer not null,
  embed_ms         integer not null,
  max_e2e_ms       integer,               -- worst commit -> index latency in the batch
  embed_model      text not null
);
create index on metrics.reindex_batch (finished_at desc);

-- Full re-embed benchmark (the naive baseline).
alter table metrics.full_rebuild_log add column tokens integer;
alter table metrics.full_rebuild_log add column embed_model text;
alter table metrics.full_rebuild_log add column chunks_by_table jsonb;
alter table metrics.full_rebuild_log add column applied boolean not null default false;

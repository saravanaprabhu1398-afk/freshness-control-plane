# SDD — Software Design Document

| | |
|---|---|
| **Status** | Draft v0.1 |
| **Last updated** | 2026-10-02 |
| **Related** | [PRD](PRD.md) · [ARD](ARD.md) |

## 1. Repository layout

```
ingestion/
  common/ratelimit.py        token-bucket + daily budget (AR-7)
  opensky/poller.py          states + arrivals/departures → source.flight_state
  bts/ontime.py              monthly CSV → raw.bts_ontime (Iceberg)
  bts/db1b.py                quarterly DB1B → raw.db1b → source.fare (baseline)
  ourairports/load.py        airports → source.airport
  fares/drift_sim.py         fare drift → source.fare (simulated=true)
cdc/debezium/                connector JSON
reindex/consumer.py          Kafka → diff → chunk → hash → embed → upsert
reindex/chunker.py           record → chunk text templates
reindex/full_rebuild.py      baseline benchmark only
contracts/freshness_sla.yaml
freshness/enforcer.py        SLA evaluation, breach logging
agent/agent.py               Claude tool-use loop
agent/tools.py               search_index, check_freshness, get_baseline
eval/golden/*.yaml           golden questions
eval/run.py                  naive vs. aware, pre/post drift
dashboard/app.py             Streamlit
orchestration/dagster/       assets, schedules, sensors
dbt/                         staging + marts
```

## 2. Data model (Postgres)

### 2.1 `source.*`: operational state (watched by CDC)

```sql
create table source.flight_state (
  flight_key       text primary key,      -- icao24 || ':' || first_seen_epoch
  icao24           text not null,
  callsign         text,
  est_dep_airport  text,                  -- ICAO
  est_arr_airport  text,
  first_seen       timestamptz,
  last_seen        timestamptz,
  status           text not null,         -- scheduled|airborne|landed|diverted|unknown
  last_position    jsonb,                 -- {lat, lon, alt_m, velocity, ts}
  source           text not null default 'opensky',
  simulated        boolean not null default false,
  fetched_at       timestamptz not null,
  last_verified_at timestamptz not null
);

create table source.fare (
  fare_key         text primary key,      -- origin:dest:carrier:cabin
  origin           text not null,         -- IATA
  dest             text not null,
  carrier          text not null,
  cabin            text not null,
  fare_usd         numeric(10,2) not null,
  baseline_usd     numeric(10,2) not null, -- DB1B route median
  effective_at     timestamptz not null,
  source           text not null,         -- 'bts_db1b' | 'drift_sim'
  simulated        boolean not null,
  last_verified_at timestamptz not null
);

create table source.airport (
  ident text primary key, iata text, name text, city text,
  lat double precision, lon double precision, type text,
  source text not null default 'ourairports', last_verified_at timestamptz not null
);
```

**Upsert rule (ADR-001).** `ON CONFLICT (pk) DO UPDATE ... WHERE (excluded.<business cols>) IS DISTINCT FROM (t.<business cols>)`, so polls with unchanged values produce no WAL entry.

**Re-verification.** A poll that sees the same values still proves the data is current. It updates only `freshness.registry.last_verified_at` (§2.3), not the `source.*` row, so it creates no CDC event and causes no re-embedding.

### 2.2 `index.chunks`: vector index

```sql
create table index.chunks (
  chunk_id         text primary key,      -- <table>:<pk>
  source_table     text not null,
  record_key       text not null,
  chunk_text       text not null,
  content_hash     char(64) not null,     -- sha256(chunk_text)
  embedding        vector(384) not null,
  source           text not null,
  simulated        boolean not null,
  data_as_of       timestamptz not null,  -- record's effective/fetched time
  indexed_at       timestamptz not null
);
create index on index.chunks using hnsw (embedding vector_cosine_ops);
```

### 2.3 `freshness.*`

```sql
create table freshness.registry (
  record_key       text primary key,      -- <table>:<pk>
  source           text not null,
  last_verified_at timestamptz not null,
  last_changed_at  timestamptz not null
);
create table freshness.contract (           -- loaded from YAML
  source text primary key, max_age interval not null,
  on_breach text not null, version int not null
);
create table freshness.sla_breach (
  id bigserial primary key, source text, record_key text,
  age interval, max_age interval, detected_at timestamptz, resolved_at timestamptz
);
```

### 2.4 `metrics.*`

```sql
create table metrics.reindex_log (
  id bigserial primary key, event_ts timestamptz, kafka_offset bigint,
  source_table text, record_key text, op char(1),
  chunks_considered int, chunks_reembedded int, chunks_skipped_same_hash int,
  embed_ms int, e2e_latency_ms int            -- source commit → vector upsert
);
create table metrics.full_rebuild_log (
  id bigserial primary key, run_ts timestamptz, total_chunks int, embed_ms int
);
create table metrics.agent_query_log (
  id bigserial primary key, ts timestamptz, question text, mode text, -- naive|aware
  cited_chunks text[], max_age interval, label text, answer text
);
```

## 3. CDC

The connector is `cdc/debezium/source-connector.json`:

| Setting | Value |
|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` |
| `plugin.name` | `pgoutput` |
| `table.include.list` | `source.flight_state,source.fare,source.airport` |
| `topic.prefix` | `fcp` → topics `fcp.source.flight_state`, … |
| Unwrap SMT | **Not** applied. The consumer needs the `before` and `after` values. |

`REPLICA IDENTITY FULL` is set on the source tables so the `before` image is complete.

**Event contract** (the fields the consumer relies on):

```json
{ "op": "c|u|d|r", "ts_ms": 1759400000000,
  "source": { "table": "fare", "lsn": 123456 },
  "before": { ... } | null, "after": { ... } | null }
```

## 4. Selective re-index algorithm

```
on event e:
  key      = e.source.table + ":" + pk(e.after or e.before)
  if e.op == 'd':
      delete from index.chunks where record_key = key; log; commit offset; return
  changed  = {col for col in business_cols(table) if before[col] != after[col]}
  if changed == ∅: log skip; commit; return               # e.g. housekeeping-only update
  text     = chunker.render(table, e.after)                # deterministic template
  h        = sha256(text)
  if h == current_hash(key): log skip_same_hash; commit; return
  vec      = embed(text)
  BEGIN
    upsert index.chunks(chunk_id, text, h, vec, data_as_of, indexed_at=now())
    upsert freshness.registry(key, last_changed_at=now(), last_verified_at=now())
    insert metrics.reindex_log(...)
  COMMIT
  commit kafka offset                                      # after DB commit → at-least-once, idempotent by hash
```

- **One chunk per record in v1.** Records are small, and the templates are written as natural-language facts. For example: *"Fare JFK→LAX on DL economy: $289.00 as of 2026-10-02T14:00Z (simulated drift; DB1B Q2-2026 median $264.00)."*
- **Batching.** The consumer micro-batches up to 64 events or 500 ms before embedding.
- **Baseline benchmark.** `full_rebuild.py` re-embeds every record and logs to `metrics.full_rebuild_log`.
- **Savings metric:**
  `savings = 1 − Σ chunks_reembedded / (full_rebuild_chunks × rebuild_count_equivalent)`
  Here `rebuild_count_equivalent` is how many times the naive approach would rebuild in the same window, for example hourly. The method is stated in the case study.

## 5. Freshness SLA enforcement

1. **Load contracts.** `contracts/freshness_sla.yaml` is loaded into `freshness.contract` at startup and whenever the file's hash changes (AR-5).
2. **Run the enforcer.** A Dagster sensor runs every 60 s and executes:
   ```sql
   select r.record_key, r.source, now() - r.last_verified_at as age, c.max_age
   from freshness.registry r join freshness.contract c using (source)
   where now() - r.last_verified_at > c.max_age;
   ```
3. **Record breaches.** New breaches are inserted, and a breach is resolved when the record is verified again.
4. **Roll up per source.** `breach_pct`, `p50_age` and `p95_age` per source are written to `metrics.freshness_snapshot` for the dashboard.
5. **Catalog sync (optional).** When the catalog is enabled, `freshness.sla_hours` and `freshness.last_breach_at` are written as Unity Catalog table properties.

## 6. Freshness-aware agent

**Model:** Claude Sonnet (latest), via the Anthropic SDK with tool use.

**Tools:**

| Tool | Returns |
|---|---|
| `search_index(query, k=8, filters)` | Chunks with `chunk_id`, `text`, `data_as_of`, `source`, `simulated` |
| `check_freshness(chunk_ids)` | Per chunk: `age`, `max_age`, `status ∈ {fresh, stale, unknown}` |
| `get_baseline(route|airport)` | BTS on-time stats from the dbt marts, used for context |

**Policy** (system prompt plus a final check in code):
- Every factual claim must cite chunk IDs.
- `check_freshness` must be called on every cited chunk before answering. The code enforces this: if it wasn't called, the agent loop forces the call.
- The label is computed **in code** from the freshness results, not chosen by the model:

  | Freshness result | Label |
  |---|---|
  | All cited chunks fresh | `VERIFIED` |
  | Any cited chunk stale | `POSSIBLY STALE — data as of <min data_as_of>` |
  | All key chunks stale and past `2 × max_age`, or nothing retrieved | `CANNOT ANSWER RELIABLY` |

- Simulated data is always disclosed in the answer footer.

**Response schema:**

```json
{ "answer": "...", "label": "VERIFIED|POSSIBLY_STALE|CANNOT_ANSWER",
  "as_of": "2026-10-02T14:00:00Z", "citations": ["source.fare:JFK:LAX:DL:Y"],
  "simulated_data_used": true }
```

**Naive mode** (for the eval): same retrieval, no `check_freshness`, and no label.

## 7. Evaluation design

- **Golden set** (`eval/golden/*.yaml`): about 50 questions.

  | Group | Count | Example |
  |---|---|---|
  | Fares | ~20 | "current fare JFK→LAX on DL" |
  | Flight state | ~20 | "has callsign UAL123 landed?" |
  | Baseline | ~10 | "ORD→SFO on-time % in June" |

  Each question has an `expected_fn`, which computes the correct answer from the current `source.*` state at eval time, so ground truth moves with drift.
- **Protocol:**
  1. Snapshot T0, then run naive and aware.
  2. Inject drift: apply a fare-shock batch and replay a recorded OpenSky delta. With the re-index consumer **paused**, this creates an index that is stale on purpose.
  3. Run both modes. Aware should flag the stale answers, and naive should answer wrongly with confidence.
  4. Resume the consumer, then run both again. Aware should return `VERIFIED` with correct answers.
- **Scoring:**
  - `accuracy`: numeric answers within tolerance (fares ±1%), categorical answers exact match.
  - `confident_wrong_rate`
  - `stale_flag_precision` and `stale_flag_recall`, with ground truth "is stale" = cited data older than its SLA or different from the current source.
- **Output:** `eval/reports/<ts>.json` plus a markdown summary that the case study uses.

## 8. Ingestion details

| Source | Cadence | Budget / notes |
|---|---|---|
| OpenSky `/flights/arrival` and `/flights/departure` per airport | 8 airports, hourly windows | Uses OAuth2 client credentials (the token is refreshed about every 30 min); the rate limiter caps usage at **80%** of the account's daily credits |
| OpenSky `/states/all` within a bounding box | Every 5 min, one US-region box | Area-limited calls cost fewer credits |
| BTS On-Time | Monthly download | Bulk file; no rate concern |
| BTS DB1B | Quarterly download | Aggregated to route × carrier × cabin medians |
| OurAirports | Weekly | Static CSV |
| Fare drift sim | Every 15 min | Seasonal multiplier × days-to-departure curve × N(0, σ) + Poisson(λ) shocks of ±15–40% |

For demos and CI, every OpenSky response is recorded to `data/recordings/` so it can be replayed offline.

## 9. Observability

- Structured JSON logs that include `record_key` and `kafka_offset`.
- All metrics are kept in `metrics.*` (AR-9), and the dashboard reads only from these tables.
- Dagster run history covers the pollers, the enforcer and dbt.

## 10. Testing

| Layer | Tests |
|---|---|
| Unit | Chunker determinism, hash skip, label computation, rate limiter, drift-sim distribution |
| Contract | Debezium event fixture → consumer produces the expected DB writes |
| Integration | Docker Compose up → upsert a fare → vector updated within 60 s (NFR-1) |
| dbt | `unique`, `not_null`, `accepted_values(status)`, source freshness |
| Eval | Golden-set run in CI on recorded data, with the Claude call mocked for determinism |

## 11. Phase 1 build checklist
- [ ] `docker-compose.yml` (postgres+pgvector, kafka, connect) + `Makefile` (`up`, `down`, `seed`)
- [ ] `db/init.sql`: schemas from §2
- [ ] `ingestion/common/ratelimit.py`
- [ ] OpenSky poller (OAuth2, recorded replay)
- [ ] BTS On-Time + DB1B loaders → Iceberg raw
- [ ] OurAirports loader
- [ ] Fare baseline from DB1B → `source.fare`
- [ ] dbt staging + marts (`mart_route_ontime`, `mart_fare_baseline`)
- [ ] Dagster assets and schedules for the above
- [ ] `docs/data-sources.md`

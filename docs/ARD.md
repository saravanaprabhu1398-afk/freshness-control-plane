# ARD — Architecture Requirements & Decisions

| | |
|---|---|
| **Status** | Draft v0.1 |
| **Last updated** | 2026-10-02 |
| **Related** | [PRD](PRD.md) · [SDD](SDD.md) |

This document covers the system's components, how data flows between them, the architecture requirements that follow from the PRD, and the key decisions as ADRs. Detailed schemas and algorithms are in the [SDD](SDD.md).

## 1. System context

```
            ┌───────────── External sources (free tier) ──────────────┐
            │  OpenSky API    BTS On-Time    BTS DB1B    OurAirports  │
            └──────┬──────────────┬────────────┬────────────┬─────────┘
                   │ poll         │ monthly    │ quarterly  │ one-off
                   ▼              ▼            ▼            ▼
            ┌───────────────── Ingestion (Dagster assets) ────────────┐
            │  pollers · fare-drift simulator · rate limiter         │
            └──────┬───────────────────────────────────────┬──────────┘
                   │ upsert                               │ append
                   ▼                                      ▼
        ┌─────────────────────┐                 ┌──────────────────────┐
        │ Postgres: source.*  │                 │ Iceberg: raw.*       │
        │ (operational state) │                 │ (history / baseline) │
        └─────────┬───────────┘                 └──────────┬───────────┘
                  │ WAL (logical replication)              │ dbt
                  ▼                                        ▼
        ┌─────────────────────┐                 ┌──────────────────────┐
        │ Debezium → Kafka    │                 │ dbt marts (DuckDB)   │
        │ topics per table    │                 │ baseline / ground    │
        └─────────┬───────────┘                 │ truth for eval       │
                  ▼                             └──────────────────────┘
        ┌──────────────────────────────┐
        │ Re-index consumer            │──▶ metrics.reindex_log
        │ diff → chunk → hash → embed  │
        └───────┬──────────────┬───────┘
                ▼              ▼
     ┌──────────────────┐  ┌───────────────────────┐   ┌─────────────────┐
     │ pgvector:        │  │ freshness.registry    │◀──│ SLA enforcer    │
     │ index.chunks     │  │ last_verified_at      │   │ contracts/*.yaml│
     └────────┬─────────┘  └──────────┬────────────┘   └──────┬──────────┘
              │                       │                       ▼
              ▼                       ▼               freshness.sla_breach
        ┌──────────────────────────────────────┐
        │ Freshness-aware agent (Claude)       │──▶ answer + freshness label
        └──────────────────────────────────────┘
              ▲                                    ┌───────────────────────┐
        eval harness (golden set, drift inject)    │ Dashboard (Streamlit) │
                                                   │ + Unity Catalog (OSS) │
                                                   └───────────────────────┘
```

## 2. Components

| Component | Responsibility | Technology |
|---|---|---|
| Ingestion | Poll and download sources, apply rate limits, upsert current state, append history | Python, httpx, Dagster assets and schedules |
| Fare-drift simulator | Apply realistic fare changes on top of the DB1B baseline | Python, Dagster schedule |
| Source DB | Current operational state that CDC watches | Postgres 16 (`wal_level=logical`) |
| Lake / baseline | Immutable history and ground truth | Iceberg (PyIceberg SQL catalog), dbt-duckdb |
| CDC | Turn row changes into events | Debezium Postgres connector, Kafka (KRaft) |
| Re-index consumer | Compare changes, rebuild chunks, check hashes, embed only changed chunks, upsert vectors | Python, confluent-kafka, sentence-transformers |
| Vector index | Retrieval | pgvector (HNSW) |
| Freshness registry | `last_verified_at` per record and chunk | Postgres schema `freshness` |
| SLA enforcer | Evaluate contracts and record breaches | Dagster sensor every 1 min |
| Agent | Retrieve, check freshness, generate a labeled answer | Claude API with tool use |
| Eval harness | Golden set, drift injection, scoring | pytest-style runner, JSON reports |
| Dashboard | Lag, savings and breaches | Streamlit |
| Catalog | Contract and freshness metadata | Unity Catalog OSS (optional, P1) |

## 3. Architecture requirements

| ID | Requirement | Traces to |
|---|---|---|
| AR-1 | Every change to the source tables must produce exactly one logical change event (at-least-once delivery, with idempotent consumers). | FR-3 |
| AR-2 | Re-indexing is driven by events and scoped to the record. No component may re-embed the full dataset, except the explicit baseline benchmark. | FR-4, NFR-2 |
| AR-3 | Embedding is content-addressed: a chunk is re-embedded only if `sha256(chunk_text)` changed. | FR-4 |
| AR-4 | Freshness is a first-class column on every chunk (`last_verified_at`, `source`, `simulated`). | FR-6, NFR-5 |
| AR-5 | SLAs are declared in config, not code. Changing one requires no deploy. | FR-7 |
| AR-6 | The agent never produces an answer without first calling the freshness check on every retrieved chunk it cites. | FR-9 |
| AR-7 | All external calls go through a shared rate limiter with a daily budget. | NFR-3 |
| AR-8 | The whole stack runs on one laptop with Docker Compose. | NFR-4 |
| AR-9 | Everything can be observed: every stage writes structured metrics to `metrics.*` tables, which the dashboard reads. | FR-5, FR-11 |

## 4. Architecture Decision Records

### ADR-001 — Pollers write to Postgres, and CDC reads Postgres
- **Context:** The sources are HTTP APIs and files. Debezium needs a database change log.
- **Decision:** Pollers *upsert* the current state into `source.*` tables, and Debezium reads the Postgres change log (WAL).
- **Consequences:**
  - Real API changes become real CDC events.
  - Unchanged polls produce no events, because the upsert is a no-op when values are identical (`ON CONFLICT ... WHERE row IS DISTINCT FROM`).
  - We gain a natural compare-and-skip step.
- **Alternative rejected:** Pollers publishing straight to Kafka. That gives no before/after image and no idempotent state, and it no longer demonstrates CDC.

### ADR-002 — Fares: BTS DB1B baseline plus simulated drift (replaces Amadeus)
- **Context:** The brief planned to use the Amadeus Self-Service sandbox, but Amadeus decommissioned Self-Service on **2026-07-17**. Live fare APIs are paid or tightly rate-limited.
- **Decision:** Use BTS DB1B (Airline Origin & Destination Survey, a 10% ticket sample, quarterly) as the *real* fare baseline. Aggregate it to route × quarter fare distributions, then simulate drift on top.
- **Consequences:**
  - The baseline is real, free and citable. The drift is simulated and labeled as such.
  - DB1B shows historical fares paid, not live offers, and the case study says so.

### ADR-003 — OpenSky is the live drift source; BTS is the baseline
- **Context:** OpenSky provides aircraft state vectors and arrival/departure records (first seen, last seen, estimated airports). It does **not** provide scheduled times, delays, cancellations or gates. BTS does provide those, but publishes about 2 months behind.
- **Decision:** Live drift comes from OpenSky state transitions (scheduled → airborne → landed, diversions). Delay context comes from joining with the BTS baseline per route, carrier and hour.
- **Consequences:**
  - The agent's ops questions are framed around live state plus historical performance, not live delay minutes.
  - Scope is US airports only.

### ADR-004 — pgvector in the source Postgres, not a separate vector DB
- **Decision:** Use pgvector with an HNSW index.
- **Consequences:**
  - One less service.
  - Vector updates and freshness updates commit in a single transaction, so the index and the registry can't disagree.
  - Not tuned for very large scale, which is acceptable at about 10⁵ chunks.

### ADR-005 — Local embedding model
- **Decision:** `BAAI/bge-small-en-v1.5` (384 dimensions) via sentence-transformers.
- **Consequences:**
  - Zero cost, and it runs offline.
  - Cost savings are reported in embedding operations and seconds, and converted to $ at a published API embedding price for the case study, with the method stated.

### ADR-006 — Freshness enforced at query time, not only at ingest
- **Decision:** The agent calls a `check_freshness(chunk_ids)` tool before it answers. The response format requires a freshness label.
- **Rationale:** An index can be up to date and the data still stale, for example when the source itself stops updating. Only a check at query time protects the answer.
- **Consequences:** Adds about 1 DB round-trip per query, which is negligible.

### ADR-007 — Dagster for orchestration; dbt-duckdb over Iceberg for the baseline
- **Decision:** Reuse the FlightPulse stack (Dagster, dbt, Iceberg). Run dbt against DuckDB reading Iceberg locally, with no Spark or Trino.
- **Consequences:**
  - Lightweight.
  - Moving to Spark or Trino in the cloud only changes the dbt adapter.

### ADR-008 — Unity Catalog OSS is optional
- **Decision:** Contracts and freshness metadata live first in Postgres (`freshness.contract`, `freshness.sla_breach`). They are mirrored to Unity Catalog OSS as table properties and tags when it is enabled.
- **Consequences:** The core demo has no dependency on Unity Catalog, and the catalog integration is still shown.

## 5. Deployment view (local)

`docker-compose.yml` runs these services:

| Service | Role |
|---|---|
| `postgres` | Postgres 16 with pgvector and logical replication |
| `kafka` | Kafka (KRaft mode) |
| `connect` | Debezium |
| `dagster-webserver`, `dagster-daemon` | Orchestration |
| `reindex-consumer` | Selective re-index |
| `dashboard` | Streamlit |
| `unity-catalog` | Optional, via a Compose profile |

Iceberg data sits in a `./warehouse` volume. Terraform (`infra/terraform`) is a later, optional cloud target (managed Postgres, MSK/Confluent, S3 for Iceberg).

## 6. Security and compliance
- Secrets (`OPENSKY_CLIENT_ID/SECRET`, `ANTHROPIC_API_KEY`) are kept only in `.env`, which git ignores.
- OpenSky's terms are for non-commercial or research use, and this project is a non-commercial portfolio piece. Attribution goes in the README.
- No personal data is stored. Aircraft are identified by ICAO24 and callsign only.

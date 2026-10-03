# Freshness Control Plane

[![CI](https://github.com/saravanaprabhu1398-afk/freshness-control-plane/actions/workflows/ci.yml/badge.svg)](https://github.com/saravanaprabhu1398-afk/freshness-control-plane/actions/workflows/ci.yml)

**Detects stale aviation data and stops a RAG agent from presenting outdated answers with full confidence.**

RAG systems going silently stale is a real production problem. The source data changes, but the index and the agent's answers still reflect the old data. This project builds a control plane that:

1. **Detects** changes in source aviation data (flight status and fares) using CDC.
2. **Re-indexes selectively**: it re-embeds only the records or chunks that changed, not the whole dataset.
3. **Enforces a freshness SLA** defined as a data contract (for example, "no fare answer may cite data older than 24h").
4. **Makes the agent freshness-aware**: answers that rely on data past its SLA are flagged with the data's timestamp.

It is designed as a standalone sub-system that plugs into [FlightPulse](#flightpulse-integration) (dbt · Apache Iceberg · Dagster · Terraform).

## Data sources

| Source | Use | Real or simulated |
|---|---|---|
| [OpenSky Network](https://opensky-network.org/) | Live flight state at 8 US hubs: airborne, on the ground, landed (primary drift source) | **Real** |
| [BTS On-Time Performance](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ) | Historical delays and cancellations per route (ground truth) | **Real** |
| [BTS DB1B Market](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FHK) | Fare baseline: median fare per route and carrier | **Real** (historical; newest quarter is about 15 months old) |
| [OurAirports](https://ourairports.com/data/) | Airport reference data | **Real** |
| Fare drift | Realistic price changes applied on top of the baseline | **Simulated** (no free live-fare API; Amadeus Self-Service shut down 2026-07-17) |

All API usage stays within free-tier limits, enforced by a credit budget; there is no scraping. Filters, limits and attribution: [docs/data-sources.md](docs/data-sources.md).

## Design docs

- [PRD](docs/PRD.md): problem, scope, requirements, success metrics
- [ARD](docs/ARD.md): architecture, requirements, and decision records (ADRs)
- [SDD](docs/SDD.md): schemas, CDC contract, re-index algorithm, agent and eval design
- [Data sources](docs/data-sources.md): what is real, what is simulated, and every filter applied

## Architecture

![Freshness Control Plane system architecture](docs/diagrams/architecture.svg)

The numbered purple path is the change hot path: ① a poller upserts the current state, ② Postgres writes the change to its WAL, ③ Debezium publishes it to Kafka, ④ the re-index consumer picks it up, ⑤ only chunks whose content hash changed are re-embedded, in the same transaction as the freshness registry update, and ⑥ at query time the agent checks the freshness of every chunk it cites. Details: [ARD](docs/ARD.md) · [SDD](docs/SDD.md).

## Quickstart

You need Docker and [uv](https://docs.astral.sh/uv/). The commands below run on a 16 GB laptop.

```bash
cp .env.example .env
```

Optionally, add your OpenSky API client ID and secret to `.env`. Without them, the poller runs anonymously every 20 minutes.

```bash
make install
```

```bash
make seed
```

`make seed` starts Postgres, Kafka and Debezium, applies migrations, registers the CDC connector, then loads the real baseline: airports, 6 months of BTS on-time data, the newest DB1B quarter, the dbt marts and the fare seed.

```bash
make poll
```

`make poll` runs one live OpenSky poll.

```bash
make tail
```

`make tail` shows change events as they stream: record ID, commit time, the exact delta, and the latency from commit to consumer. Run `make poll` or `make drift` in another terminal to create some.

```bash
make status
```

`make status` shows what is loaded, how fresh it is, and the API credits used today.

```bash
make dagster
```

`make dagster` opens the Dagster UI at http://localhost:3000 and turns on the schedules.

Run `make check` for lint, `mypy --strict`, and the unit and integration tests (the same checks as CI).

## Repo layout

| Path | Purpose |
|---|---|
| `src/fcp/ingestion/` | Source loaders: OpenSky (live + daily), BTS on-time, DB1B, OurAirports, fare baseline |
| `src/fcp/common/` | Change-aware upserts, freshness registry, API credit budget, settings, logging |
| `src/fcp/orchestration/` | Dagster assets, jobs and schedules |
| `dbt/` | Staging and marts over Iceberg (dbt-duckdb), with data tests |
| `db/migrations/` | Ordered SQL migrations for `source`, `telemetry`, `freshness`, `index`, `metrics`, `ops` |
| `contracts/` | Freshness SLA data contracts |
| `cdc/` | Debezium connector config |
| `src/fcp/cdc/` | Connector registration and self-healing, the change-event parser, `cdc tail` and `cdc stats` |
| `eval/` | Golden question set (Phase 6) |
| `tests/` | Unit tests, and integration tests against the local Postgres |
| `docs/` | PRD, ARD, SDD, data sources, diagrams |

## Roadmap

- [x] **Phase 1:** Data sourcing and baseline
- [x] **Phase 2:** CDC change detection (Debezium + Kafka)
- [ ] **Phase 3:** Selective re-indexing with cost/time-saved tracking
- [ ] **Phase 4:** Freshness SLA as data contracts
- [ ] **Phase 5:** Freshness-aware agent
- [ ] **Phase 6:** Evaluation under drift
- [ ] **Phase 7:** Dashboard
- [ ] **Phase 8:** Case study write-up

## Results so far

**Phase 1, measured on 2026-10-02**

| What | Result |
|---|---|
| Real baseline loaded | 1,781,411 flights touching the 8 hubs (BTS, Feb–Jul 2026); 235 route × carrier fare baselines (DB1B 2025 Q2); 873 US airports |
| Idempotent reloads | Re-running the Dagster baseline job: 873 of 873 airports and 235 of 235 fares unchanged, with no rows rewritten |
| Live polling | 4,521 aircraft in the bounding box, 568 tracked at the hubs, 4 OpenSky credits per poll |
| Change selectivity | On a second poll 3 minutes later, **77% of tracked flights were unchanged and wrote nothing** (448 of 583). Only 36 real status changes and 99 new flights reached the change log, which is what Phase 3's selective re-index will build on |

**Phase 2, measured on 2026-10-03**

| What | Result |
|---|---|
| Initial snapshot | 667 flights, 235 fares and 873 airports in Kafka, exactly matching the tables |
| Events carry what the brief asks for | Record ID, commit time and an exact delta, e.g. `fare_usd: 98.00 -> 64.00, simulated: False -> True` |
| Change selectivity | A live poll a few minutes after the previous one: 666 of 768 flights (87%) unchanged and silent. A drift tick: 225 of 235 fares (96%) unchanged |
| Latency, from a short sample (2 commits, 112 events) | Commit → Debezium p95 177–241 ms; commit → our consumer p95 587–694 ms |
| Resilience | Kafka data and the connector survive a forced container recreate; a deleted connector is re-registered by the Dagster sensor and resumes from saved offsets without re-snapshotting |

## Success criteria

- A working CDC → selective re-index pipeline running on real flight data
- A documented freshness SLA that is enforced and reportable
- An agent that clearly flags stale answers instead of presenting them with full confidence
- A case study with concrete before/after numbers (re-index cost/time saved, drift caught, accuracy preserved)

## FlightPulse integration

This repo is built to slot into FlightPulse as the **Freshness Control Plane** sub-system. dbt models target the FlightPulse Iceberg catalog, and Dagster assets are written so they can be imported into the FlightPulse code location.

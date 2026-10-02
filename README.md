# Freshness Control Plane

**Detects stale aviation data and stops a RAG agent from presenting outdated answers with full confidence.**

RAG systems going silently stale is a real production problem. The source data changes, but the index and the agent's answers still reflect the old data. This project builds a control plane that:

1. **Detects** changes in source aviation data (flight status, schedules, fares) using CDC.
2. **Re-indexes selectively**: it re-embeds only the records or chunks that changed, not the whole dataset.
3. **Enforces a freshness SLA** defined as a data contract (for example, "no fare answer may cite data older than 24h").
4. **Makes the agent freshness-aware**: answers that rely on data past its SLA are flagged with the data's timestamp.

It is designed as a standalone sub-system that plugs into [FlightPulse](#flightpulse-integration) (dbt · Apache Iceberg · Dagster · Terraform).

## Data sources

| Source | Use | Real or simulated |
|---|---|---|
| [OpenSky Network](https://opensky-network.org/) | Live/historical flight states, delays, status changes (primary drift source) | **Real** |
| [US DOT BTS](https://www.transtats.bts.gov/) | Historical on-time performance (ground-truth baseline) | **Real** |
| [OurAirports](https://ourairports.com/data/) / [OpenFlights](https://openflights.org/data.html) | Airport and route reference data (stable layer) | **Real** |
| Amadeus for Developers (sandbox) | Initial fare snapshot | **Real snapshot** |
| Fare drift | Realistic fare-change patterns applied on top of the snapshot | **Simulated** (avoids live-pricing rate limits) |

All API usage stays within free-tier limits. No scraping.

## Architecture

```
 OpenSky / BTS / Fares ──▶ ingestion ──▶ Postgres (source tables)
                                              │  Debezium
                                              ▼
                                        Kafka change events
                                     {record_id, ts, delta}
                                              │
                         ┌────────────────────┴───────────────────┐
                         ▼                                        ▼
                selective re-index                     freshness registry
           (re-embed changed chunks only)         (last_verified_ts per record)
                         │                                        │
                         ▼                                        ▼
                    vector index ◀──── freshness-aware agent ────▶ SLA contracts
                                              │
                                              ▼
                                eval harness · dashboard
```

## Repo layout

| Path | Phase | Purpose |
|---|---|---|
| `ingestion/` | 1 | Source pullers (OpenSky, BTS, OurAirports, fares + fare drift simulator) |
| `dbt/` | 1 | Baseline ground-truth models (Iceberg) |
| `cdc/` | 2 | Debezium connector configs, Kafka topics |
| `reindex/` | 3 | Change-event consumer that diffs records and re-embeds only affected chunks |
| `contracts/` | 4 | Freshness SLA data contracts |
| `agent/` | 5 | RAG agent with freshness checks at query time |
| `eval/` | 6 | Golden question set and before/after-drift evaluation |
| `dashboard/` | 7 | Freshness lag, re-index savings, SLA breach log |
| `orchestration/dagster/` | — | Dagster assets/jobs |
| `infra/terraform/` | — | Infrastructure |
| `docs/` | 8 | Architecture notes and case study |

## Roadmap

- [ ] **Phase 1:** Data sourcing and baseline
- [ ] **Phase 2:** CDC change detection (Debezium + Kafka)
- [ ] **Phase 3:** Selective re-indexing with cost/time-saved tracking
- [ ] **Phase 4:** Freshness SLA as data contracts
- [ ] **Phase 5:** Freshness-aware agent
- [ ] **Phase 6:** Evaluation under drift
- [ ] **Phase 7:** Dashboard
- [ ] **Phase 8:** Case study write-up

## Success criteria

- A working CDC → selective re-index pipeline running on real flight data
- A documented freshness SLA that is enforced and reportable
- An agent that clearly flags stale answers instead of presenting them with full confidence
- A case study with concrete before/after numbers (re-index cost/time saved, drift caught, accuracy preserved)

## FlightPulse integration

This repo is built to slot into FlightPulse as the **Freshness Control Plane** sub-system. dbt models target the FlightPulse Iceberg catalog, and Dagster assets are written so they can be imported into the FlightPulse code location.

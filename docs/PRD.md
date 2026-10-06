# PRD — Freshness Control Plane

| | |
|---|---|
| **Status** | Draft v0.1 |
| **Owner** | Saravana Prabhu |
| **Last updated** | 2026-10-02 |
| **Related** | [ARD](ARD.md) · [SDD](SDD.md) · [Project brief](project-brief.md) |

## 1. Problem

RAG systems answer from an index, and that index is a snapshot. When the source data changes, the index and the agent's answers keep showing the old data with full confidence. Nothing signals the problem, so no error is raised. In aviation ops and fares, that means an agent can say a flight is "on time" after it has diverted, or quote a fare that changed six hours ago.

There are two common responses, and both fall short:
- **Rebuild the whole index on a schedule.** This is expensive, and the data is still stale between rebuilds.
- **Ignore the problem.** Answers then sound confident but are wrong.

## 2. Goal

Build a control plane that **detects** source changes, **re-indexes only what changed**, **enforces a freshness SLA** as a data contract, and makes the agent **say plainly how old its data is** at query time.

## 3. Users and use cases

| Persona | Need |
|---|---|
| **Ops analyst** (agent user) | "Is UA123 from ORD delayed?" "What's typical on-time performance for ORD→SFO in March?" They want correct answers, or a clear warning when the data may be old. |
| **Fare analyst** (agent user) | "What's the current average fare JFK→LAX?" Fares change quickly, and a stale fare quoted with confidence is a business error. |
| **Platform engineer** (operator) | "Which sources are past their SLA right now? How much did selective re-indexing save this week?" |
| **Hiring reviewer** (portfolio audience) | Wants clear architecture, honest labeling of real vs. simulated data, and concrete before/after numbers. |

## 4. Scope

### In scope (v1)
- US airports only, because BTS covers US domestic flights. Initial set: ATL, ORD, DFW, DEN, LAX, JFK, SFO, SEA.
- Sources:

  | Source | Data | Real or simulated |
  |---|---|---|
  | OpenSky | Live flight state and arrival/departure records | Real |
  | BTS On-Time Performance | Scheduled vs. actual times, delays, cancellations | Real |
  | BTS DB1B | Fares | Real historical baseline, plus simulated fare drift |
  | OurAirports | Airport reference data | Real |

- CDC → selective re-index → freshness registry → SLA enforcement → freshness-aware agent → evaluation → dashboard → case study.
- Runs entirely locally with Docker Compose. Terraform covers an optional cloud deployment.

### Out of scope (v1)
- Gate changes. No free source provides them.
- Live, bookable fare offers. The Amadeus Self-Service APIs were shut down on 2026-07-17, and other live sources are paid or rate-limited.
- Non-US routes, multiple tenants, auth/RBAC, and production-grade high availability.

## 5. Requirements

### Functional

| ID | Requirement | Priority |
|---|---|---|
| FR-1 | Ingest OpenSky, BTS On-Time, BTS DB1B and OurAirports into source tables, staying within free-tier limits. | P0 |
| FR-2 | Simulate realistic fare drift on top of the DB1B baseline, and tag every simulated row `simulated=true`. | P0 |
| FR-3 | Emit a CDC event for every insert, update and delete in the source tables, carrying `record_id`, `changed_at`, `op` and the before/after values. | P0 |
| FR-4 | On each change, re-embed **only** the chunks whose text content actually changed. | P0 |
| FR-5 | Log re-index cost (tokens, seconds, chunks) next to a simulated full re-embed baseline. | P0 |
| FR-6 | Keep `last_verified_at` for every record and chunk. | P0 |
| FR-7 | Declare freshness SLAs per source in a versioned contract file (`contracts/freshness_sla.yaml`). | P0 |
| FR-8 | Detect SLA breaches, record them with timestamps, and make them queryable. | P0 |
| FR-9 | At query time, the agent checks how old every cited record is and labels the answer **VERIFIED**, **POSSIBLY STALE (as of T)** or **CANNOT ANSWER**. | P0 |
| FR-10 | Run a golden question set against a naive agent and the freshness-aware agent, before and after a drift event. | P0 |
| FR-11 | Dashboard showing freshness lag per source, re-index savings and the SLA breach log. | P1 |
| FR-12 | Register SLA contracts and freshness metadata in a catalog (open-source Unity Catalog). | P1 |
| FR-13 | 1-page case study with concrete numbers. | P0 |

### Non-functional

| ID | Requirement |
|---|---|
| NFR-1 | Change-to-index latency: p95 under 60 s from source write to updated vector. |
| NFR-2 | Selective re-index should cost ≥90% fewer embedding operations than a full re-embed at typical change rates. This is a hypothesis to measure, not an assumption. |
| NFR-3 | Free-tier API compliance. OpenSky calls are budgeted below the daily credit allowance, and there is no scraping. |
| NFR-4 | Reproducible: `make up && make seed` brings up a working demo on a laptop with 16 GB RAM. |
| NFR-5 | Honesty: every metric and answer can be traced to real or simulated data. |
| NFR-6 | Secrets are kept only in `.env`, which git ignores. |

## 6. Success metrics

| Metric | Target |
|---|---|
| Embedding operations saved vs. full re-embed | ≥ 90% |
| Stale-answer flag recall (stale answers correctly flagged) | ≥ 95% |
| Stale-answer flag precision (fresh answers not wrongly flagged) | ≥ 90% |
| Accuracy after drift: freshness-aware vs. naive | Freshness-aware gives no confident wrong answers. The naive agent's count is measured and reported. |
| Change-to-index p95 latency | < 60 s |

## 7. Milestones

| Phase | Deliverable | Done when |
|---|---|---|
| 1 | Data sourcing and baseline | Stack starts, pollers run, dbt baseline builds from real data |
| 2 | CDC | Debezium events visible in Kafka for every source table |
| 3 | Selective re-index | Consumer re-embeds changed chunks only; savings are logged |
| 4 | Freshness SLA | Breaches are detected, stored and queryable |
| 5 | Freshness-aware agent | Answers carry freshness labels |
| 6 | Evaluation | Before/after report generated |
| 7 | Dashboard | Streamlit view is live |
| 8 | Case study | README section with numbers |

## 8. Risks

| Risk | Mitigation |
|---|---|
| OpenSky credit limits or outages | Poll a fixed airport set and budget credits; cache responses; replay recorded responses for demos |
| BTS publishes about 2 months behind | BTS is only the *baseline*; live drift comes from OpenSky |
| DB1B fares are quarterly and historical, not live offers | State this in the case study; fare drift is explicitly simulated |
| The local stack is too heavy | pgvector stays in the source Postgres; Unity Catalog is optional (fallback: a contract registry table) |
| Metrics look too good | Publish the raw eval set and methods; report failures too |

## 9. Open questions
1. Which model should the agent use? The default is Claude Sonnet, which is cheap enough for eval runs.
2. ~~Should simulated fare drift follow real DB1B seasonality, or a simple stochastic model?~~ **Resolved in Phase 2:** a stochastic model, with mean-reverting reprices plus rare shocks. One DB1B quarter cannot support a seasonal claim, so the simulator does not make one (SDD §8).

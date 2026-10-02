# Project: AI Data Freshness & Drift Control Plane (Aviation Ops & Fares)

## Context
I have an existing personal project, **FlightPulse**, built with dbt, Apache Iceberg, Dagster, and Terraform. I'm extending it into a new portfolio project that demonstrates production AI-platform reliability — specifically, detecting when source data has gone stale and preventing an AI/RAG system from confidently citing outdated information. This is aimed at "AI Platform Engineer / LLMOps Engineer" roles, where silent RAG staleness is a known, real production problem almost no other candidate's portfolio addresses.

## Domain & data sources
Using real aviation data rather than a simulated/toy dataset:
- **OpenSky Network** (free) — real-time and historical flight tracking data: delays, cancellations, gate/status changes. Use this as the primary source of genuine, naturally-occurring "drift" events.
- **US DOT BTS (Bureau of Transportation Statistics)** (free) — historical on-time performance data, monthly updates, for a stable baseline/ground-truth layer.
- **OurAirports / OpenFlights** (free static datasets) — airport/route reference data for the stable layer that drift is compared against.
- **Fares**: pull a real snapshot from a free-tier fare API (e.g. Amadeus for Developers sandbox) as the initial dataset, then programmatically simulate realistic fare-change patterns on top of it rather than fighting live-pricing rate limits — this is a legitimate, explainable choice for a portfolio project.

## Goal
Build a control plane that: (1) detects when source aviation data (flight status, schedule, fare) changes, (2) selectively re-indexes only what changed rather than the full dataset, (3) enforces a freshness SLA, and (4) makes an AI agent answering ops/fare questions aware of and transparent about data staleness — rather than confidently citing outdated information.

## Deliverables
1. A CDC pipeline (Debezium + Kafka, consistent with prior production CDC work) watching the OpenSky/BTS/fare source feeds for changes
2. A diff mechanism that isolates exactly which records/chunks changed, avoiding full dataset re-embedding/re-indexing on every update
3. A freshness SLA defined as a data contract (e.g. "no fare/schedule answer should cite data older than X hours"), enforced and reportable via Unity Catalog (or equivalent metadata layer)
4. A RAG/agent interface over this data that checks freshness at query time and flags answers based on stale data rather than presenting them with full confidence
5. An evaluation step showing answer quality/accuracy before and after a drift event, and confirming stale answers get correctly flagged
6. A lightweight control-plane dashboard: freshness lag per data source, re-index volume/cost savings from selective updates vs. full re-embed, and any SLA breaches with timestamps
7. A written case study (~1 page) documenting the problem, architecture, and concrete before/after numbers, added to the FlightPulse repo README as a named sub-system (e.g. "Freshness Control Plane")

## Suggested phases
**Phase 1 — Data sourcing & baseline**
Pull real flight status/delay data from OpenSky and historical performance data from BTS. Pull a real fare snapshot from a free-tier fare API. Load into FlightPulse's existing Iceberg/dbt pipeline as the baseline "ground truth" layer.

**Phase 2 — CDC change-detection layer**
Set up Debezium + Kafka watching for changes in flight status/schedule (from OpenSky) and simulated fare changes (programmatically applied on top of the real snapshot). Each change event should carry record ID, change timestamp, and the delta.

**Phase 3 — Selective re-indexing**
On a change event, update only the affected records/chunks in whatever index or table the AI agent queries — not a full dataset refresh. Track and log the cost/time saved vs. a full re-embed baseline.

**Phase 4 — Freshness SLA**
Add a "last_verified_timestamp" per record. Define and enforce a data contract (e.g. 24-hour freshness SLA for fares, shorter for flight status). Make this queryable/reportable.

**Phase 5 — Freshness-aware agent**
Wire freshness checks into the agent's query-time logic. If cited data is past SLA, the response should flag it as potentially stale with a timestamp, rather than presenting it with full confidence.

**Phase 6 — Evaluation under drift**
Build or reuse a golden question set (ops/fare questions with known-correct answers). Run it before and after simulating a drift event. Confirm stale answers get flagged and that freshness-aware responses outperform a naive baseline that ignores drift.

**Phase 7 — Dashboard**
Build a simple notebook or lightweight web view: freshness lag per source, re-index savings, SLA breach log.

**Phase 8 — Write-up**
Document the problem, architecture, and before/after numbers as a case study for the FlightPulse README.

## Constraints
- Prefer real data (OpenSky, BTS) for flight status/schedule drift since these are freely available and give authentic CDC events — use simulated-but-realistic drift only for fares where live data access is rate-limited, and be explicit in the write-up about which parts are real vs. simulated.
- Keep API usage within free-tier limits; do not scrape or exceed rate limits on any third-party source.
- New code/config should extend the existing FlightPulse repo (dbt, Iceberg, Dagster, Terraform) rather than starting a separate standalone repo.

## Success criteria
- A working CDC-to-selective-reindex pipeline running on real flight data
- A documented freshness SLA enforced and reportable
- A freshness-aware agent that demonstrably flags stale answers rather than confidently presenting them
- A case study with concrete before/after numbers (re-index cost/time saved, drift caught, accuracy preserved) suitable for a resume link or interview discussion

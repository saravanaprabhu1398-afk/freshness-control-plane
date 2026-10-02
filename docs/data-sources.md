# Data sources

Each source below is marked **real** or **simulated** (PRD NFR-5). Every number in the case study can be traced back to one of these.

| Source | Real or simulated | What we take | Refresh | Lag at source | Limits we respect |
|---|---|---|---|---|---|
| [OpenSky Network REST API](https://openskynetwork.github.io/opensky-api/rest.html) | **Real** | Live state vectors inside one bounding box around the tracked airports; completed flights for those airports | States every 5 min (20 min anonymous); flights daily at 06:00 UTC | States: seconds. Flights: batch-processed, previous day only | Per-endpoint daily credits (anonymous 400, account 4,000). We spend at most 80%, reserve before each call, and reconcile with `X-Rate-Limit-Remaining` |
| [BTS On-Time Performance](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ) | **Real** | Every flight touching a tracked airport, 25 of 110 columns | Checked weekly; newest 6 months kept | ~60 days after month end (July 2026 published late Sep 2026) | Bulk file download, ~32 MB/month, cached locally |
| [BTS DB1B Market](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FHK) | **Real** | Ticket fares between tracked airports (10% sample) | Checked weekly; newest quarter used | **~15 months** after quarter end (2025 Q2 was the newest available in Oct 2026) | Bulk file download, ~110 MB/quarter (2.1 GB unzipped), cached locally |
| [OurAirports](https://ourairports.com/data/) | **Real** | US large and medium airports with an IATA code | Weekly | Community-maintained, near real time | Single 12 MB CSV |
| Fare drift (Phase 2) | **Simulated** | Price changes applied on top of the DB1B baseline | Every 15 min | n/a | Always written with `simulated = true` and `source = 'drift_sim'` |

## Tracked airports

ATL, ORD, DFW, DEN, LAX, JFK, SFO and SEA, all US hubs, because BTS covers US domestic flights only. They are defined in [`src/fcp/common/airports.py`](../src/fcp/common/airports.py).

## OpenSky: what the data can and cannot tell us

- **It can tell us:** each aircraft's position, altitude, speed and whether it is on the ground, every few seconds. A day later, it also gives each completed flight's estimated departure and arrival airports.
- **It cannot tell us:** schedules, delays, cancellations or gates. Status is therefore limited to `airborne`, `on_ground` and `landed`. Historical delay context comes from BTS (ADR-003).
- **Which aircraft we keep:** aircraft with a callsign, within 80 km of a tracked airport, and either on the ground or below 4,000 m. This drops cruise-altitude overflights. Both thresholds can be configured (`FCP_TRACK_RADIUS_KM`, `FCP_TRACK_MAX_ALTITUDE_M`).
- **What counts as one flight:** the same aircraft (`icao24`) with the same callsign, seen again within 6 hours, is the same flight. Its key is `icao24:callsign:first_seen_epoch`.
- **Where positions go:** to `telemetry.flight_position`, which CDC deliberately ignores. A moving aircraft is not a business change; a take-off or a landing is.
- **Measured on 2026-10-02:**

  | | Value |
  |---|---|
  | Aircraft in the bounding box | 4,521 |
  | Kept at or near the 8 airports | 568 |
  | Second poll, 3 minutes later | 583 flights: 448 unchanged (77%), 99 new, 36 status changes |

## DB1B filters

| Filter | Why |
|---|---|
| Origin and destination both tracked | The fare questions cover these routes |
| `BulkFare = 0` | Bulk or tour fares are not comparable |
| `$25 ≤ MktFare ≤ $2,500` | BTS notes that very low fares are often award tickets; the upper bound trims data-entry outliers |
| Nonstop markets only (`MktCoupons = 1`, applied in dbt) | One market = one flight |
| At least 30 sampled tickets per route × carrier | So that medians are not set by a handful of fares |

The result for 2025 Q2 is 235 route × carrier baselines, for example JFK→LAX on DL with a median of $548.50 from 3,656 sampled tickets. DB1BMarket has no cabin field, so `cabin = 'ALL'`.

**Honesty note.** The fare baseline is a real historical median, about 15 months old. It is *not* a live price. The freshness contract `fare_baseline` records its age, and from Phase 5 the agent must name the quarter whenever it uses it.

## Attribution

- OpenSky Network data: Schäfer et al., *Bringing up OpenSky: A large-scale ADS-B sensor network for research*, IPSN 2014. Non-commercial and research use, per the [OpenSky terms](https://opensky-network.org/about/terms-of-use).
- BTS data: U.S. Department of Transportation, Bureau of Transportation Statistics (public domain).
- OurAirports: public domain.

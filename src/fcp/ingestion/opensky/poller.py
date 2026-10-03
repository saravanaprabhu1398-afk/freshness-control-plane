"""OpenSky pollers: live states (every few minutes) and completed flights (daily).

Both write business state to source.flight_state through a change-aware upsert, so only real
status changes reach the WAL, Debezium and, later, the re-index consumer. Positions go to
telemetry.flight_position, which CDC ignores.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any

import psycopg

from fcp.common.airports import TRACKED_AIRPORTS, covering_bbox
from fcp.common.db import FreshnessEntry, RunStats, connect, record_freshness, track_run, upsert_changed
from fcp.common.logging import get_logger
from fcp.common.settings import Settings, get_settings
from fcp.ingestion.opensky.client import OpenSkyClient, flights_cost, previous_utc_day_window, states_cost
from fcp.ingestion.opensky.transform import (
    SAME_FLIGHT_GAP,
    FlightRecord,
    KnownFlight,
    StateObservation,
    parse_flights,
    parse_states,
    resolve_flight_key,
    resolve_state_key,
)

CONTRACT = "flight_status"
TABLE = "source.flight_state"
BUSINESS_COLS = ("status", "near_airport", "est_dep_airport", "est_arr_airport", "last_seen")
INSERT_ONLY_COLS = ("icao24", "callsign", "first_seen", "source", "simulated")
# `landed` is final for a flight key; batch-only fields are never erased by a live poll.
OVERRIDES = {
    "status": "case when t.status = 'landed' then t.status else excluded.status end",
    "near_airport": "coalesce(excluded.near_airport, t.near_airport)",
    "est_dep_airport": "coalesce(excluded.est_dep_airport, t.est_dep_airport)",
    "est_arr_airport": "coalesce(excluded.est_arr_airport, t.est_arr_airport)",
    "last_seen": "coalesce(excluded.last_seen, t.last_seen)",
}

log = get_logger(__name__)


def _known_from_telemetry(
    conn: psycopg.Connection[Any], obs: Sequence[StateObservation]
) -> list[KnownFlight]:
    if not obs:
        return []
    since = min(o.last_contact for o in obs) - SAME_FLIGHT_GAP
    rows = conn.execute(
        """
        select p.flight_key, p.icao24, p.callsign, f.first_seen, p.last_observed
          from telemetry.flight_position p join source.flight_state f using (flight_key)
         where p.icao24 = any(%s) and p.last_observed >= %s
        """,
        ([o.icao24 for o in obs], since),
    ).fetchall()
    return [
        KnownFlight(r["flight_key"], r["icao24"], r["callsign"], r["first_seen"], r["last_observed"])
        for r in rows
    ]


def _known_from_state(conn: psycopg.Connection[Any], recs: Sequence[FlightRecord]) -> list[KnownFlight]:
    if not recs:
        return []
    lo = min(r.first_seen for r in recs) - SAME_FLIGHT_GAP
    hi = max(r.last_seen for r in recs)
    rows = conn.execute(
        """
        select flight_key, icao24, callsign, first_seen from source.flight_state
         where icao24 = any(%s) and first_seen between %s and %s
        """,
        ([r.icao24 for r in recs], lo, hi),
    ).fetchall()
    return [KnownFlight(r["flight_key"], r["icao24"], r["callsign"], r["first_seen"], None) for r in rows]


def apply_states(
    conn: psycopg.Connection[Any], observations: Sequence[StateObservation], stats: RunStats
) -> None:
    """Upsert live observations. Separated from fetching so tests can drive it directly."""
    known = _known_from_telemetry(conn, observations)
    state_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    freshness: list[FreshnessEntry] = []
    seen_keys: set[str] = set()
    for o in observations:
        key, first_seen = resolve_state_key(o, known)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        state_rows.append(
            {
                "flight_key": key,
                "icao24": o.icao24,
                "callsign": o.callsign,
                "status": o.status,
                "near_airport": o.near_airport,
                "est_dep_airport": None,
                "est_arr_airport": None,
                "first_seen": first_seen,
                "last_seen": None,
                "source": "opensky",
                "simulated": False,
            }
        )
        position_rows.append(
            {
                "flight_key": key,
                "icao24": o.icao24,
                "callsign": o.callsign,
                "first_observed": o.last_contact,
                "last_observed": o.last_contact,
                "last_contact": o.last_contact,
                "latitude": o.latitude,
                "longitude": o.longitude,
                "baro_altitude_m": o.baro_altitude_m,
                "velocity_ms": o.velocity_ms,
                "on_ground": o.on_ground,
            }
        )
        freshness.append(FreshnessEntry(f"{TABLE}:{key}", CONTRACT, o.last_contact))

    result = upsert_changed(
        conn,
        TABLE,
        state_rows,
        key="flight_key",
        business_cols=BUSINESS_COLS,
        insert_only_cols=INSERT_ONLY_COLS,
        overrides=OVERRIDES,
    )
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into telemetry.flight_position as p
              (flight_key, icao24, callsign, first_observed, last_observed, last_contact,
               latitude, longitude, baro_altitude_m, velocity_ms, on_ground)
            values (%(flight_key)s, %(icao24)s, %(callsign)s, %(first_observed)s, %(last_observed)s,
                    %(last_contact)s, %(latitude)s, %(longitude)s, %(baro_altitude_m)s, %(velocity_ms)s,
                    %(on_ground)s)
            on conflict (flight_key) do update set
              last_observed = excluded.last_observed, last_contact = excluded.last_contact,
              latitude = excluded.latitude, longitude = excluded.longitude,
              baro_altitude_m = excluded.baro_altitude_m, velocity_ms = excluded.velocity_ms,
              on_ground = excluded.on_ground
            """,
            position_rows,
        )
    record_freshness(conn, freshness, changed_keys={f"{TABLE}:{k}" for k in result.changed})
    stats.add(result)
    stats.detail.update(inserted=len(result.inserted), updated=len(result.updated))


def apply_flights(conn: psycopg.Connection[Any], records: Sequence[FlightRecord], stats: RunStats) -> None:
    known = _known_from_state(conn, records)
    rows: list[dict[str, Any]] = []
    freshness: list[FreshnessEntry] = []
    seen: set[str] = set()
    for r in records:
        key, first_seen = resolve_flight_key(r, known)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "flight_key": key,
                "icao24": r.icao24,
                "callsign": r.callsign,
                "status": "landed",
                "near_airport": r.near_airport,
                "est_dep_airport": r.est_dep_airport,
                "est_arr_airport": r.est_arr_airport,
                "first_seen": first_seen,
                "last_seen": r.last_seen,
                "source": "opensky",
                "simulated": False,
            }
        )
        freshness.append(FreshnessEntry(f"{TABLE}:{key}", CONTRACT, r.last_seen))
    result = upsert_changed(
        conn,
        TABLE,
        rows,
        key="flight_key",
        business_cols=BUSINESS_COLS,
        insert_only_cols=INSERT_ONLY_COLS,
        overrides=OVERRIDES,
    )
    record_freshness(conn, freshness, changed_keys={f"{TABLE}:{k}" for k in result.changed})
    stats.add(result)
    stats.detail.update(inserted=len(result.inserted), updated=len(result.updated))


def poll_states(settings: Settings | None = None) -> RunStats:
    """One live poll: a single bounding box around all tracked airports."""
    settings = settings or get_settings()
    bbox = covering_bbox(settings.track_radius_km)
    client = OpenSkyClient(settings)
    try:
        with track_run("opensky_states") as stats:
            payload = client.get_states(bbox)
            stats.credits_used = 0 if settings.opensky_mode == "replay" else states_cost(bbox)
            observations = parse_states(
                payload, radius_km=settings.track_radius_km, max_altitude_m=settings.track_max_altitude_m
            )
            stats.detail.update(
                aircraft_in_box=len(payload.get("states") or []), tracked=len(observations), bbox=asdict(bbox)
            )
            with connect() as conn:
                apply_states(conn, observations, stats)
        log.info("opensky.states.done", **{k: getattr(stats, k) for k in ("rows_seen", "rows_changed")})
        return stats
    finally:
        client.close()


def poll_flights(settings: Settings | None = None, *, now: datetime | None = None) -> RunStats:
    """Completed flights for the previous UTC day at every tracked airport (needs credentials)."""
    settings = settings or get_settings()
    begin, end = previous_utc_day_window(now)
    client = OpenSkyClient(settings)
    try:
        with track_run("opensky_flights") as stats:
            raw: list[dict[str, Any]] = []
            for ap in TRACKED_AIRPORTS:
                raw += client.get_arrivals(ap.icao, begin, end)
                raw += client.get_departures(ap.icao, begin, end)
            if settings.opensky_mode != "replay":
                stats.credits_used = 2 * len(TRACKED_AIRPORTS) * flights_cost(begin, end)
            records = parse_flights(raw)
            stats.detail.update(window=[begin, end], raw_rows=len(raw), flights=len(records))
            with connect() as conn:
                apply_flights(conn, records, stats)
        return stats
    finally:
        client.close()

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from fcp.common.db import RunStats
from fcp.ingestion.opensky.poller import apply_flights, apply_states
from fcp.ingestion.opensky.transform import FlightRecord, StateObservation

pytestmark = pytest.mark.integration
T0 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def obs(*, on_ground: bool, at: datetime) -> StateObservation:
    return StateObservation(
        icao24="test01",
        callsign="TST100",
        last_contact=at,
        latitude=41.97,
        longitude=-87.90,
        baro_altitude_m=None if on_ground else 900.0,
        velocity_ms=70.0,
        on_ground=on_ground,
        near_airport="ORD",
    )


def flight_rows(db: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    return db.execute(
        "select flight_key, status, est_arr_airport from source.flight_state "
        "where icao24 = 'test01' order by first_seen"
    ).fetchall()


def test_status_change_lifecycle(db: psycopg.Connection[Any]) -> None:
    s1 = RunStats()
    apply_states(db, [obs(on_ground=True, at=T0)], s1)
    assert s1.rows_changed == 1

    s2 = RunStats()
    apply_states(db, [obs(on_ground=True, at=T0 + timedelta(minutes=5))], s2)
    assert (s2.rows_changed, s2.rows_unchanged) == (0, 1)  # same flight, nothing changed

    s3 = RunStats()
    apply_states(db, [obs(on_ground=False, at=T0 + timedelta(minutes=10))], s3)
    assert s3.rows_changed == 1  # took off: a real change event
    [row] = flight_rows(db)
    assert row["status"] == "airborne"

    # Next day the batch record arrives: same flight, now landed with an arrival airport.
    rec = FlightRecord("test01", "TST100", T0 + timedelta(minutes=5), T0 + timedelta(hours=4), "KORD", "KSFO")
    apply_flights(db, [rec], RunStats())
    [row] = flight_rows(db)
    assert (row["status"], row["est_arr_airport"]) == ("landed", "KSFO")

    # A late live sighting under the same key must not un-land the flight.
    apply_states(db, [obs(on_ground=False, at=T0 + timedelta(minutes=15))], RunStats())
    [row] = flight_rows(db)
    assert row["status"] == "landed"


def test_registry_tracks_every_observed_flight(db: psycopg.Connection[Any]) -> None:
    apply_states(db, [obs(on_ground=True, at=T0)], RunStats())
    row = db.execute(
        "select contract, data_as_of from freshness.registry where record_key like "
        "'source.flight_state:test01:%'"
    ).fetchone()
    assert row == {"contract": "flight_status", "data_as_of": T0}

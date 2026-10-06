from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fcp.reindex.chunker import build, content_hash

T = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)

FARE = {
    "fare_key": "JFK:LAX:DL:ALL",
    "origin": "JFK",
    "dest": "LAX",
    "carrier": "DL",
    "cabin": "ALL",
    "fare_usd": "548.50",
    "baseline_usd": "548.50",
    "baseline_period": "2025-Q2",
    "sample_size": 3656,
    "effective_at": "2025-06-30T23:59:59.999999Z",
    "source": "bts_db1b",
    "simulated": False,
    "updated_at": "2026-10-06T10:00:00Z",
}


def test_fare_text_names_airports_carrier_and_real_provenance() -> None:
    chunk = build("source.fare", "JFK:LAX:DL:ALL", FARE, T)
    assert "John F. Kennedy International (JFK) to Los Angeles International (LAX)" in chunk.text
    assert "DL (Delta Air Lines)" in chunk.text
    assert "$548.50" in chunk.text
    assert "real BTS DB1B 2025-Q2 median" in chunk.text
    assert "SIMULATED" not in chunk.text
    assert chunk.record_key == chunk.chunk_id == "source.fare:JFK:LAX:DL:ALL"
    assert chunk.data_as_of == datetime(2025, 6, 30, 23, 59, 59, 999999, tzinfo=UTC)  # effective_at


def test_simulated_fare_says_so_in_the_text() -> None:
    chunk = build(
        "source.fare", "k", {**FARE, "fare_usd": "560.00", "simulated": True, "source": "drift_sim"}, T
    )
    assert "SIMULATED current price" in chunk.text
    assert chunk.simulated is True


def test_bookkeeping_changes_do_not_change_the_hash() -> None:
    a = build("source.fare", "k", FARE, T)
    b = build("source.fare", "k", {**FARE, "updated_at": "2026-10-07T00:00:00Z"}, T)
    assert a.content_hash == b.content_hash  # updated_at is not a fact
    c = build("source.fare", "k", {**FARE, "fare_usd": "549.00"}, T)
    assert c.content_hash != a.content_hash


def test_debezium_strings_and_postgres_types_render_identically() -> None:
    from_db = {
        **FARE,
        "fare_usd": Decimal("548.50"),
        "baseline_usd": Decimal("548.50"),
        "effective_at": datetime(2025, 6, 30, 23, 59, 59, 999999, tzinfo=UTC),
    }
    assert build("source.fare", "k", FARE, T).text == build("source.fare", "k", from_db, T).text


def test_flight_states_render_status_specific_sentences() -> None:
    base = {
        "icao24": "abc123",
        "callsign": "UAL123",
        "near_airport": "ORD",
        "est_dep_airport": None,
        "est_arr_airport": None,
        "first_seen": "2026-10-06T09:58:30Z",
        "last_seen": None,
    }
    airborne = build("source.flight_state", "f", {**base, "status": "airborne"}, T).text
    assert "is airborne near Chicago O'Hare International (ORD)" in airborne
    assert "First seen 2026-10-06 09:58 UTC" in airborne
    ground = build("source.flight_state", "f", {**base, "status": "on_ground"}, T).text
    assert "on the ground at Chicago O'Hare International (ORD)" in ground
    landed = build(
        "source.flight_state",
        "f",
        {
            **base,
            "status": "landed",
            "est_dep_airport": "KORD",
            "est_arr_airport": "KSFO",
            "last_seen": "2026-10-06T14:10:00Z",
        },
        T,
    )
    assert "has landed at San Francisco International (SFO) after departing Chicago O'Hare" in landed.text
    assert landed.data_as_of == datetime(2026, 10, 6, 14, 10, tzinfo=UTC)


def test_airport_text() -> None:
    row = {
        "ident": "KSEA",
        "iata": "SEA",
        "name": "Seattle–Tacoma International Airport",  # noqa: RUF001 (en dash, as OurAirports spells it)
        "municipality": "Seattle",
        "region": "US-WA",
        "type": "large_airport",
        "latitude": 47.4479,
        "longitude": -122.3103,
        "elevation_ft": 433,
    }
    text = build("source.airport", "KSEA", row, T).text
    assert text == (
        "Seattle–Tacoma International Airport (SEA / KSEA), Seattle, US-WA. Large airport at "  # noqa: RUF001
        "latitude 47.4479, longitude -122.3103, elevation 433 ft."
    )


def test_content_hash_is_sha256_hex() -> None:
    assert len(content_hash("x")) == 64

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fcp.common.airports import BY_IATA
from fcp.ingestion.opensky.transform import (
    KnownFlight,
    make_flight_key,
    parse_flights,
    parse_states,
    resolve_flight_key,
    resolve_state_key,
)

T0 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
ORD = BY_IATA["ORD"]


def state(
    icao24: str = "abc123",
    callsign: str | None = "UAL123 ",
    *,
    lat: float = ORD.latitude,
    lon: float = ORD.longitude,
    alt: float | None = 500.0,
    on_ground: bool = False,
    contact: datetime = T0,
) -> list[object]:
    # Field order per OpenSky docs: icao24, callsign, origin_country, time_position, last_contact,
    # longitude, latitude, baro_altitude, on_ground, velocity, ...
    return [
        icao24,
        callsign,
        "United States",
        int(contact.timestamp()),
        int(contact.timestamp()),
        lon,
        lat,
        alt,
        on_ground,
        120.0,
        90.0,
        0.0,
        None,
        alt,
        None,
        False,
        0,
        0,
    ]


def parse(*states: list[object]) -> list:  # type: ignore[type-arg]
    return parse_states(
        {"time": int(T0.timestamp()), "states": list(states)}, radius_km=80, max_altitude_m=4000
    )


def test_keeps_low_aircraft_near_tracked_airport_and_strips_callsign() -> None:
    [obs] = parse(state())
    assert obs.callsign == "UAL123"
    assert obs.near_airport == "ORD"
    assert obs.status == "airborne"


def test_on_ground_status() -> None:
    [obs] = parse(state(on_ground=True, alt=None))
    assert obs.status == "on_ground"


def test_drops_cruise_overflights_far_aircraft_and_missing_callsigns() -> None:
    assert parse(state(alt=11_000)) == []  # overflight at cruise altitude
    assert parse(state(lat=ORD.latitude + 3)) == []  # not near any tracked airport
    assert parse(state(callsign="   ")) == []
    assert parse(state(callsign=None)) == []


def test_latest_contact_wins_for_duplicate_aircraft() -> None:
    [obs] = parse(state(contact=T0), state(contact=T0 + timedelta(seconds=30), on_ground=True))
    assert obs.on_ground is True


def test_state_key_reused_within_gap_and_new_after_gap() -> None:
    [obs] = parse(state())
    known = [KnownFlight("k1", obs.icao24, obs.callsign, T0 - timedelta(hours=1), T0 - timedelta(minutes=5))]
    assert resolve_state_key(obs, known) == ("k1", T0 - timedelta(hours=1))

    stale = [KnownFlight("k1", obs.icao24, obs.callsign, T0 - timedelta(hours=9), T0 - timedelta(hours=7))]
    key, first_seen = resolve_state_key(obs, stale)
    assert key == make_flight_key(obs.icao24, obs.callsign, T0) and first_seen == T0


def test_flights_parse_dedupes_and_maps_near_airport() -> None:
    row = {
        "icao24": "ABC123",
        "callsign": "UAL123 ",
        "firstSeen": int(T0.timestamp()),
        "lastSeen": int((T0 + timedelta(hours=4)).timestamp()),
        "estDepartureAirport": "KORD",
        "estArrivalAirport": "KSFO",
    }
    [rec] = parse_flights([row, dict(row)])  # same flight from arrivals and departures
    assert rec.icao24 == "abc123"
    assert rec.near_airport == "SFO"  # arrival preferred


def test_flight_record_matches_live_tracked_flight_within_tolerance() -> None:
    [rec] = parse_flights(
        [
            {
                "icao24": "abc123",
                "callsign": "UAL123",
                "firstSeen": int(T0.timestamp()),
                "lastSeen": int((T0 + timedelta(hours=4)).timestamp()),
                "estDepartureAirport": "KORD",
                "estArrivalAirport": "KSFO",
            }
        ]
    )
    live = KnownFlight("live-key", "abc123", "UAL123", T0 - timedelta(minutes=20), None)
    assert resolve_flight_key(rec, [live]) == ("live-key", T0 - timedelta(minutes=20))
    too_early = KnownFlight("old", "abc123", "UAL123", T0 - timedelta(hours=5), None)
    assert resolve_flight_key(rec, [too_early])[0] == make_flight_key("abc123", "UAL123", T0)

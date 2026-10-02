"""Pure transformations of OpenSky payloads (no I/O, fully unit-tested).

What OpenSky can and cannot tell us (ADR-003): live aircraft state (position, on-ground
flag) and, a day later, completed flights with estimated departure/arrival airports.
It has no schedules, delays, cancellations or gates. Status here is therefore limited to
`airborne`, `on_ground` and `landed`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fcp.common.airports import BY_ICAO, nearest_tracked

# A sighting within this window of the previous sighting is the same flight.
SAME_FLIGHT_GAP = timedelta(hours=6)
# A batch flight record matches a live-tracked flight if we first saw it no earlier than
# this before OpenSky's firstSeen (we may have caught it on the ground before take-off).
FIRST_SEEN_TOLERANCE = timedelta(hours=3)

# State vector indices (https://openskynetwork.github.io/opensky-api/rest.html)
I_ICAO24, I_CALLSIGN, I_LAST_CONTACT, I_LON, I_LAT, I_BARO_ALT, I_ON_GROUND, I_VELOCITY = (
    0,
    1,
    4,
    5,
    6,
    7,
    8,
    9,
)


@dataclass(frozen=True, slots=True)
class StateObservation:
    icao24: str
    callsign: str
    last_contact: datetime
    latitude: float
    longitude: float
    baro_altitude_m: float | None
    velocity_ms: float | None
    on_ground: bool
    near_airport: str  # IATA

    @property
    def status(self) -> str:
        return "on_ground" if self.on_ground else "airborne"


@dataclass(frozen=True, slots=True)
class FlightRecord:
    icao24: str
    callsign: str
    first_seen: datetime
    last_seen: datetime
    est_dep_airport: str | None  # ICAO
    est_arr_airport: str | None

    @property
    def near_airport(self) -> str | None:
        """IATA of the tracked airport this flight touched (arrival preferred)."""
        for icao in (self.est_arr_airport, self.est_dep_airport):
            if icao and icao in BY_ICAO:
                return BY_ICAO[icao].iata
        return None


def _epoch(value: Any) -> datetime:
    return datetime.fromtimestamp(int(value), UTC)


def parse_states(
    payload: Mapping[str, Any], *, radius_km: float, max_altitude_m: float
) -> list[StateObservation]:
    """Keep aircraft with a callsign that are on the ground at, or low near, a tracked airport.

    Cruise-altitude overflights are dropped: they are not arriving or departing.
    If an aircraft appears more than once, the most recent contact wins.
    """
    latest: dict[str, StateObservation] = {}
    for s in payload.get("states") or []:
        callsign = (s[I_CALLSIGN] or "").strip()
        lat, lon = s[I_LAT], s[I_LON]
        if not callsign or lat is None or lon is None or s[I_LAST_CONTACT] is None:
            continue
        on_ground = bool(s[I_ON_GROUND])
        baro = s[I_BARO_ALT]
        if not on_ground and (baro is None or baro > max_altitude_m):
            continue
        airport = nearest_tracked(lat, lon, radius_km)
        if airport is None:
            continue
        obs = StateObservation(
            icao24=s[I_ICAO24].lower(),
            callsign=callsign,
            last_contact=_epoch(s[I_LAST_CONTACT]),
            latitude=float(lat),
            longitude=float(lon),
            baro_altitude_m=None if baro is None else float(baro),
            velocity_ms=None if s[I_VELOCITY] is None else float(s[I_VELOCITY]),
            on_ground=on_ground,
            near_airport=airport.iata,
        )
        prev = latest.get(obs.icao24)
        if prev is None or obs.last_contact > prev.last_contact:
            latest[obs.icao24] = obs
    return list(latest.values())


def parse_flights(payload: Iterable[Mapping[str, Any]]) -> list[FlightRecord]:
    """Parse /flights/arrival or /flights/departure rows; de-duplicate across both."""
    out: dict[tuple[str, str, datetime], FlightRecord] = {}
    for f in payload:
        callsign = (f.get("callsign") or "").strip()
        if not callsign or f.get("firstSeen") is None or f.get("lastSeen") is None:
            continue
        rec = FlightRecord(
            icao24=str(f["icao24"]).lower(),
            callsign=callsign,
            first_seen=_epoch(f["firstSeen"]),
            last_seen=_epoch(f["lastSeen"]),
            est_dep_airport=f.get("estDepartureAirport"),
            est_arr_airport=f.get("estArrivalAirport"),
        )
        out[(rec.icao24, rec.callsign, rec.first_seen)] = rec
    return list(out.values())


def make_flight_key(icao24: str, callsign: str, first_seen: datetime) -> str:
    return f"{icao24}:{callsign}:{int(first_seen.timestamp())}"


@dataclass(frozen=True, slots=True)
class KnownFlight:
    flight_key: str
    icao24: str
    callsign: str
    first_seen: datetime
    last_observed: datetime | None  # from telemetry; None if only known from batch records


def resolve_state_key(obs: StateObservation, known: Iterable[KnownFlight]) -> tuple[str, datetime]:
    """Reuse the key of a flight seen recently with the same aircraft and callsign, else start one.

    Returns (flight_key, first_seen).
    """
    best: KnownFlight | None = None
    for k in known:
        if k.icao24 != obs.icao24 or k.callsign != obs.callsign or k.last_observed is None:
            continue
        if obs.last_contact - k.last_observed <= SAME_FLIGHT_GAP and (
            best is None or k.last_observed > (best.last_observed or k.last_observed)
        ):
            best = k
    if best is not None:
        return best.flight_key, best.first_seen
    return make_flight_key(obs.icao24, obs.callsign, obs.last_contact), obs.last_contact


def resolve_flight_key(rec: FlightRecord, known: Iterable[KnownFlight]) -> tuple[str, datetime]:
    """Match a batch flight record to a live-tracked flight, else create a key from firstSeen."""
    for k in known:
        if (
            k.icao24 == rec.icao24
            and k.callsign == rec.callsign
            and rec.first_seen - FIRST_SEEN_TOLERANCE <= k.first_seen <= rec.last_seen
        ):
            return k.flight_key, k.first_seen
    return make_flight_key(rec.icao24, rec.callsign, rec.first_seen), rec.first_seen

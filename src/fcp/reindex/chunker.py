"""Render a source record as one retrieval chunk (SDD §4). Pure and deterministic.

Rules that keep the content hash meaningful:
* Only business facts go into the text. Bookkeeping such as `updated_at` never does, so a
  write that changes no fact produces the same text, the same hash, and no embedding.
* Timestamps that are facts (first seen, landed at) are rendered to the minute in UTC.
* Simulated values say so in the text itself, so the model sees it even if metadata is dropped.

Rows arrive either from Debezium (`after` image: numerics as strings, timestamps as ISO-8601)
or from Postgres directly (Decimal, datetime); both render to identical text.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from fcp.common.airports import BY_IATA, BY_ICAO

CARRIERS: dict[str, str] = {
    "AA": "American Airlines", "AS": "Alaska Airlines", "B6": "JetBlue", "DL": "Delta Air Lines",
    "F9": "Frontier Airlines", "G4": "Allegiant Air", "HA": "Hawaiian Airlines", "NK": "Spirit Airlines",
    "SY": "Sun Country Airlines", "UA": "United Airlines", "WN": "Southwest Airlines",
}  # fmt: skip


@dataclass(frozen=True, slots=True)
class Chunk:
    chunk_id: str  # == record_key: one chunk per record in v1
    source_table: str
    record_key: str
    text: str
    content_hash: str
    simulated: bool
    source: str
    data_as_of: datetime
    source_changed_at: datetime | None


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _minute(value: Any) -> str:
    ts = _ts(value)
    return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC") if ts else "unknown"


def _money(value: Any) -> str:
    return f"${Decimal(str(value)):,.2f}"


def _airport(iata: str | None) -> str:
    if not iata:
        return "an unknown airport"
    ap = BY_IATA.get(iata)
    return f"{ap.name} ({iata})" if ap else iata


def _icao_airport(icao: str | None) -> str | None:
    if not icao:
        return None
    ap = BY_ICAO.get(icao)
    return f"{ap.name} ({ap.iata})" if ap else icao


def _carrier(code: str) -> str:
    name = CARRIERS.get(code)
    return f"{code} ({name})" if name else code


def render_fare(r: Mapping[str, Any]) -> str:
    route = f"{_airport(r['origin'])} to {_airport(r['dest'])}"
    cabin = "all cabins" if r["cabin"] == "ALL" else f"cabin {r['cabin']}"
    baseline = (
        f"BTS DB1B {r['baseline_period']} median for this route and carrier: {_money(r['baseline_usd'])} "
        f"from {int(r['sample_size']):,} sampled tickets."
    )
    if r["simulated"]:
        return (
            f"Fare {route} on {_carrier(r['carrier'])}, {cabin}: {_money(r['fare_usd'])} one way. "
            f"This is a SIMULATED current price. {baseline}"
        )
    return (
        f"Fare {route} on {_carrier(r['carrier'])}, {cabin}: {_money(r['fare_usd'])} one way. "
        f"This is the real {baseline}"
    )


def render_flight(r: Mapping[str, Any]) -> str:
    who = f"Flight {r['callsign']} (aircraft {r['icao24']})"
    dep, arr = _icao_airport(r.get("est_dep_airport")), _icao_airport(r.get("est_arr_airport"))
    near = _airport(r.get("near_airport"))
    if r["status"] == "landed":
        where = f"landed at {arr}" if arr else f"landed near {near}"
        line = f"{who} has {where}" + (f" after departing {dep}." if dep else ".")
        if r.get("last_seen"):
            line += f" Last seen {_minute(r['last_seen'])}."
    elif r["status"] == "on_ground":
        line = f"{who} is on the ground at {near}."
    else:
        line = f"{who} is airborne near {near}."
    route = ""
    if r["status"] != "landed" and (dep or arr):
        route = f" Estimated route: {dep or 'unknown'} to {arr or 'unknown'}."
    return f"{line}{route} First seen {_minute(r['first_seen'])}. Source: OpenSky Network live tracking."


def render_airport(r: Mapping[str, Any]) -> str:
    kind = {"large_airport": "Large airport", "medium_airport": "Medium airport"}.get(r["type"], "Airport")
    place = ", ".join(p for p in (r.get("municipality"), r.get("region")) if p)
    elev = f", elevation {int(r['elevation_ft']):,} ft" if r.get("elevation_ft") is not None else ""
    return (
        f"{r['name']} ({r.get('iata') or 'no IATA code'} / {r['ident']}){', ' + place if place else ''}. "
        f"{kind} at latitude {float(r['latitude']):.4f}, longitude {float(r['longitude']):.4f}{elev}."
    )


RENDERERS = {
    "source.fare": render_fare,
    "source.flight_state": render_flight,
    "source.airport": render_airport,
}


def data_as_of(table: str, row: Mapping[str, Any], committed_at: datetime) -> datetime:
    """The time the chunk's facts describe: a fare's effective time, else when it was recorded."""
    if table == "source.fare":
        return _ts(row["effective_at"]) or committed_at
    if table == "source.flight_state" and row.get("status") == "landed" and row.get("last_seen"):
        return _ts(row["last_seen"]) or committed_at
    return committed_at


def build(table: str, key: str, row: Mapping[str, Any], committed_at: datetime) -> Chunk:
    text = RENDERERS[table](row)
    record_key = f"{table}:{key}"
    return Chunk(
        chunk_id=record_key,
        source_table=table,
        record_key=record_key,
        text=text,
        content_hash=content_hash(text),
        simulated=bool(row.get("simulated", False)),
        source=str(row.get("source", "")),
        data_as_of=data_as_of(table, row, committed_at),
        source_changed_at=_ts(row.get("updated_at")),
    )

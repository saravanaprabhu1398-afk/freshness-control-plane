"""OurAirports reference data → source.airport (public domain, https://ourairports.com/data/).

Keeps US large and medium airports that have an IATA code. Changes here are rare, so
most weekly runs re-verify every row and change none.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from typing import Any

import httpx

from fcp.common.db import FreshnessEntry, RunStats, connect, record_freshness, track_run, upsert_changed
from fcp.common.logging import get_logger

URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
TABLE = "source.airport"
CONTRACT = "airport_reference"
KEEP_TYPES = frozenset({"large_airport", "medium_airport"})
BUSINESS_COLS = ("iata", "name", "municipality", "region", "type", "latitude", "longitude", "elevation_ft")

log = get_logger(__name__)


def parse(text: str) -> list[dict[str, Any]]:
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        if r["iso_country"] != "US" or r["type"] not in KEEP_TYPES or not r["iata_code"]:
            continue
        rows.append(
            {
                "ident": r["ident"],
                "iata": r["iata_code"],
                "name": r["name"],
                "municipality": r["municipality"] or None,
                "region": r["iso_region"] or None,
                "type": r["type"],
                "latitude": float(r["latitude_deg"]),
                "longitude": float(r["longitude_deg"]),
                "elevation_ft": int(r["elevation_ft"]) if r["elevation_ft"] else None,
                "source": "ourairports",
            }
        )
    return rows


def load() -> RunStats:
    with track_run("ourairports") as stats:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            response = client.get(URL)
            response.raise_for_status()
        rows = parse(response.text)
        now = datetime.now(UTC)
        with connect() as conn:
            result = upsert_changed(
                conn, TABLE, rows, key="ident", business_cols=BUSINESS_COLS, insert_only_cols=("source",)
            )
            record_freshness(
                conn,
                [FreshnessEntry(f"{TABLE}:{r['ident']}", CONTRACT, now) for r in rows],
                changed_keys={f"{TABLE}:{k}" for k in result.changed},
                verified_at=now,
            )
        stats.add(result)
        log.info("ourairports.loaded", rows=len(rows), changed=len(result.changed))
    return stats

"""BTS DB1B Market (10% airline ticket sample) → Iceberg raw.db1b_market (ADR-002).

This is the *real* fare baseline. DB1B is quarterly and is published with a long lag
(~15 months as of 2026-10), so the baseline describes historical fares paid, not live
offers. That age is recorded honestly in the freshness registry (contract `fare_baseline`).

Filters applied at load (documented in docs/data-sources.md):
  * origin and destination both tracked airports
  * BulkFare = 0          (bulk/tour fares are not comparable)
  * 25 <= MktFare <= 2500 (BTS notes very low fares are often frequent-flyer awards)
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.expressions import And, EqualTo

from fcp.common.airports import TRACKED_IATA
from fcp.common.db import FreshnessEntry, connect, record_freshness, track_run
from fcp.common.logging import get_logger
from fcp.common.settings import Settings, get_settings
from fcp.ingestion.bts.common import download, exists, extracted_csv, http_client
from fcp.ingestion.lake import replace_partition

TABLE = "db1b_market"
DATASET_KEY = "dataset:raw.db1b_market"
CONTRACT = "fare_baseline"
MIN_FARE, MAX_FARE = 25, 2500

COLUMNS: dict[str, str] = {
    "ItinID": "itin_id",
    "MktID": "mkt_id",
    "MktCoupons": "mkt_coupons",
    "Year": "year",
    "Quarter": "quarter",
    "Origin": "origin",
    "Dest": "dest",
    "TkCarrier": "ticketing_carrier",
    "RPCarrier": "reporting_carrier",
    "OpCarrier": "operating_carrier",
    "Passengers": "passengers",
    "MktFare": "market_fare_usd",
    "MktDistance": "market_distance_mi",
    "NonStopMiles": "nonstop_miles",
}

log = get_logger(__name__)


def filename(year: int, quarter: int) -> str:
    return f"Origin_and_Destination_Survey_DB1BMarket_{year}_{quarter}.zip"


def recent_quarters(today: date, lookback: int = 12) -> list[tuple[int, int]]:
    """Candidate (year, quarter) pairs, newest first."""
    y, q = today.year, (today.month - 1) // 3 + 1
    out = []
    for _ in range(lookback):
        out.append((y, q))
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return out


def quarter_end(year: int, quarter: int) -> datetime:
    first_of_next = date(year + (quarter == 4), 1 if quarter == 4 else quarter * 3 + 1, 1)
    return datetime.combine(first_of_next - timedelta(days=1), datetime.max.time(), UTC)


def read_filtered(csv_path: Path) -> pa.Table:
    select = ", ".join(f'"{src}" as {dst}' for src, dst in COLUMNS.items())
    airports = sorted(TRACKED_IATA)
    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            select {select}
            from read_csv(?, header = true, auto_detect = true, null_padding = true,
                          types = {{'ItinID': 'VARCHAR', 'MktID': 'VARCHAR'}})
            where "Origin" in ? and "Dest" in ?
              and "BulkFare" = 0 and "MktFare" between ? and ?
            """,
            [str(csv_path), airports, airports, MIN_FARE, MAX_FARE],
        ).to_arrow_table()
    finally:
        con.close()


def load(settings: Settings | None = None) -> tuple[int, int] | None:
    """Load the newest available DB1B quarter. Returns (year, quarter) or None."""
    settings = settings or get_settings()
    with track_run("bts_db1b") as stats, http_client() as client:
        latest = next(
            (yq for yq in recent_quarters(datetime.now(UTC).date()) if exists(client, filename(*yq))), None
        )
        if latest is None:
            stats.status = "skipped"
            stats.detail["reason"] = "no DB1B quarter found"
            return None
        year, quarter = latest
        zip_path = download(client, filename(year, quarter), settings.downloads_dir)
        with extracted_csv(zip_path) as csv_path:
            data = read_filtered(csv_path)
        rows = replace_partition(
            TABLE,
            data,
            And(EqualTo(term="year", value=year), EqualTo(term="quarter", value=quarter)),
            settings,
        )
        stats.rows_seen = stats.rows_changed = rows
        stats.detail.update(year=year, quarter=quarter)
        log.info("bts.db1b.loaded", year=year, quarter=quarter, rows=rows)
        with connect() as conn:
            record_freshness(
                conn,
                [FreshnessEntry(DATASET_KEY, CONTRACT, quarter_end(year, quarter))],
                changed_keys={DATASET_KEY},
            )
    return latest

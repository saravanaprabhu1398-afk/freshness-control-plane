"""BTS Reporting Carrier On-Time Performance → Iceberg raw.bts_ontime.

Monthly files, published roughly two months after the month ends. We keep only flights that
touch a tracked airport, with the columns the baseline marts need. Values are not cleaned
here (raw layer); parsing happens in dbt staging.
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

TABLE = "bts_ontime"
DATASET_KEY = "dataset:raw.bts_ontime"
CONTRACT = "on_time_performance"

# source column -> raw column
COLUMNS: dict[str, str] = {
    "Year": "year",
    "Month": "month",
    "FlightDate": "flight_date",
    "Reporting_Airline": "carrier",
    "Flight_Number_Reporting_Airline": "flight_number",
    "Tail_Number": "tail_number",
    "Origin": "origin",
    "Dest": "dest",
    "CRSDepTime": "crs_dep_time",
    "DepTime": "dep_time",
    "DepDelay": "dep_delay_min",
    "DepDel15": "dep_del15",
    "CRSArrTime": "crs_arr_time",
    "ArrTime": "arr_time",
    "ArrDelay": "arr_delay_min",
    "ArrDel15": "arr_del15",
    "Cancelled": "cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "diverted",
    "Distance": "distance_mi",
    "CarrierDelay": "carrier_delay_min",
    "WeatherDelay": "weather_delay_min",
    "NASDelay": "nas_delay_min",
    "SecurityDelay": "security_delay_min",
    "LateAircraftDelay": "late_aircraft_delay_min",
}
# Read hhmm times and identifiers as text so leading zeros survive.
TEXT_COLUMNS = (
    "CRSDepTime",
    "DepTime",
    "CRSArrTime",
    "ArrTime",
    "Flight_Number_Reporting_Airline",
    "Tail_Number",
    "CancellationCode",
)

log = get_logger(__name__)


def filename(year: int, month: int) -> str:
    return f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"


def recent_months(today: date, count: int, lookback: int = 6) -> list[tuple[int, int]]:
    """Candidate (year, month) pairs, newest first, starting from last month."""
    out: list[tuple[int, int]] = []
    y, m = today.year, today.month
    for _ in range(count + lookback):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def read_filtered(csv_path: Path) -> pa.Table:
    """Read the BTS CSV with DuckDB, keeping tracked-airport flights and needed columns."""
    select = ", ".join(f'"{src}" as {dst}' for src, dst in COLUMNS.items())
    types = {c: "VARCHAR" for c in TEXT_COLUMNS}
    airports = sorted(TRACKED_IATA)
    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            select {select}
            from read_csv(?, header = true, types = ?, auto_detect = true)
            where "Origin" in ? or "Dest" in ?
            """,
            [str(csv_path), types, airports, airports],
        ).to_arrow_table()
    finally:
        con.close()


def load(settings: Settings | None = None, *, months: int | None = None) -> dict[str, int]:
    """Load the newest `months` available months. Returns rows written per month."""
    settings = settings or get_settings()
    months = months or settings.bts_ontime_months
    written: dict[str, int] = {}
    with track_run("bts_ontime") as stats, http_client() as client:
        available = [
            ym for ym in recent_months(datetime.now(UTC).date(), months) if exists(client, filename(*ym))
        ][:months]
        if not available:
            stats.status = "skipped"
            stats.detail["reason"] = "no BTS on-time files found"
            return written
        for year, month in sorted(available):
            zip_path = download(client, filename(year, month), settings.downloads_dir)
            with extracted_csv(zip_path) as csv_path:
                data = read_filtered(csv_path)
            rows = replace_partition(
                TABLE,
                data,
                And(EqualTo(term="year", value=year), EqualTo(term="month", value=month)),
                settings,
            )
            written[f"{year}-{month:02d}"] = rows
            log.info("bts.ontime.loaded", year=year, month=month, rows=rows)
        stats.rows_seen = stats.rows_changed = sum(written.values())
        stats.detail["months"] = written

        newest_y, newest_m = max(available)
        month_end = (date(newest_y, newest_m, 28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        with connect() as conn:
            record_freshness(
                conn,
                [
                    FreshnessEntry(
                        DATASET_KEY, CONTRACT, datetime.combine(month_end, datetime.max.time(), UTC)
                    )
                ],
                changed_keys={DATASET_KEY},
            )
    return written

"""End-to-end: Iceberg raw tables -> dbt staging/marts, on synthetic but realistic rows."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.expressions import And, EqualTo

from fcp.common.settings import Settings
from fcp.ingestion.lake import replace_partition
from fcp.transform import dbt


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    for k in ("FCP_DUCKDB_PATH", "FCP_ICEBERG_URI", "FCP_ICEBERG_WAREHOUSE"):
        monkeypatch.delenv(k, raising=False)
    return Settings(warehouse_dir=tmp_path / "warehouse")


def ontime_rows(n: int, *, cancelled_every: int = 10) -> pa.Table:
    rows = []
    for i in range(n):
        late = i % 3 == 0
        rows.append(
            {
                "year": 2026,
                "month": 7,
                "flight_date": "2026-07-01",
                "carrier": "UA",
                "flight_number": str(i),
                "tail_number": "N1",
                "origin": "ORD",
                "dest": "SFO",
                "crs_dep_time": "0705",
                "dep_time": "0710",
                "dep_delay_min": 30.0 if late else 2.0,
                "dep_del15": 1.0 if late else 0.0,
                "crs_arr_time": "0950",
                "arr_time": "1000",
                "arr_delay_min": 40.0 if late else -5.0,
                "arr_del15": 1.0 if late else 0.0,
                "cancelled": 1.0 if i % cancelled_every == 0 else 0.0,
                "cancellation_code": "",
                "diverted": 0.0,
                "distance_mi": 1846.0,
                "carrier_delay_min": 0.0,
                "weather_delay_min": 0.0,
                "nas_delay_min": 0.0,
                "security_delay_min": 0.0,
                "late_aircraft_delay_min": 0.0,
            }
        )
    return pa.Table.from_pylist(rows)


def db1b_rows(n: int) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "itin_id": str(i),
                "mkt_id": f"m{i}",
                "mkt_coupons": 1,
                "year": 2025,
                "quarter": 2,
                "origin": "JFK",
                "dest": "LAX",
                "ticketing_carrier": "DL",
                "reporting_carrier": "DL",
                "operating_carrier": "DL",
                "passengers": 1.0,
                "market_fare_usd": 200.0 + i,
                "market_distance_mi": 2475.0,
                "nonstop_miles": 2475.0,
            }
            for i in range(n)
        ]
    )


def test_raw_to_marts(settings: Settings) -> None:
    month = And(EqualTo(term="year", value=2026), EqualTo(term="month", value=7))
    replace_partition("bts_ontime", ontime_rows(30), month, settings)
    replace_partition("bts_ontime", ontime_rows(30), month, settings)  # idempotent reload
    replace_partition(
        "db1b_market",
        db1b_rows(41),
        And(EqualTo(term="year", value=2025), EqualTo(term="quarter", value=2)),
        settings,
    )

    dbt.run(["build"], settings)

    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    route = con.execute(
        "select scheduled_flights, cancelled_flights, on_time_arrivals from marts.mart_route_ontime"
    ).fetchall()
    fare = con.execute(
        "select fare_key, median_fare_usd, sample_size from marts.mart_fare_baseline"
    ).fetchall()
    con.close()
    # 30 flights (not 60: reload replaced the month), 3 cancelled, 20 on time minus cancelled ones among them
    assert route == [(30, 3, 18)]
    assert fare == [("JFK:LAX:DL:ALL", 220.0, 41)]


def test_fare_baseline_drops_thin_samples(settings: Settings) -> None:
    replace_partition(
        "bts_ontime",
        ontime_rows(5),
        And(EqualTo(term="year", value=2026), EqualTo(term="month", value=7)),
        settings,
    )
    replace_partition(
        "db1b_market",
        db1b_rows(10),
        And(EqualTo(term="year", value=2025), EqualTo(term="quarter", value=2)),
        settings,
    )
    dbt.run(["build"], settings)
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    assert con.execute("select count(*) from marts.mart_fare_baseline").fetchone() == (0,)
    con.close()

from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from pathlib import Path

from fcp.ingestion.bts import db1b, ontime


def test_recent_months_start_last_month_and_cross_year() -> None:
    assert ontime.recent_months(date(2026, 2, 15), count=3, lookback=0) == [(2026, 1), (2025, 12), (2025, 11)]


def test_recent_quarters_and_quarter_end() -> None:
    assert db1b.recent_quarters(date(2026, 1, 10), lookback=3) == [(2026, 1), (2025, 4), (2025, 3)]
    assert db1b.quarter_end(2025, 2).date() == date(2025, 6, 30)
    assert db1b.quarter_end(2025, 4).date() == date(2025, 12, 31)
    assert db1b.quarter_end(2025, 4).tzinfo == UTC


def _write_csv(path: Path, header: list[str], rows: list[list[object]]) -> Path:
    with path.open("w", newline="") as fh:
        w = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(header)
        w.writerows(rows)
    return path


def test_ontime_read_filtered_keeps_tracked_airports_and_text_times(tmp_path: Path) -> None:
    header = [*ontime.COLUMNS, "Unused"]

    def row(origin: str, dest: str, dep: str) -> list[object]:
        values = {c: 0 for c in ontime.COLUMNS}
        values.update(
            Year=2026,
            Month=7,
            FlightDate="2026-07-01",
            Reporting_Airline="UA",
            Flight_Number_Reporting_Airline="0042",
            Tail_Number="N1",
            Origin=origin,
            Dest=dest,
            CRSDepTime=dep,
            DepTime=dep,
            CRSArrTime="0905",
            ArrTime="0910",
            CancellationCode="",
        )
        return [values[c] for c in ontime.COLUMNS] + ["x"]

    path = _write_csv(
        tmp_path / "ontime.csv",
        header,
        [row("ORD", "SFO", "0705"), row("BOS", "MIA", "0800"), row("BOS", "JFK", "2400")],
    )
    table = ontime.read_filtered(path)
    assert table.num_rows == 2  # BOS-MIA touches no tracked airport
    assert set(table.column("origin").to_pylist()) == {"ORD", "BOS"}
    assert table.column("crs_dep_time").to_pylist()[0] == "0705"  # leading zero kept
    assert table.column("flight_number").to_pylist()[0] == "0042"
    assert "Unused" not in table.column_names


def test_db1b_read_filtered_applies_route_bulk_and_fare_filters(tmp_path: Path) -> None:
    header = [*db1b.COLUMNS, "BulkFare"]

    def row(mkt_id: str, origin: str, dest: str, fare: float, bulk: float = 0.0) -> list[object]:
        v = {c: 1 for c in db1b.COLUMNS}
        v.update(
            ItinID="1",
            MktID=mkt_id,
            Year=2025,
            Quarter=2,
            Origin=origin,
            Dest=dest,
            TkCarrier="DL",
            RPCarrier="DL",
            OpCarrier="DL",
            MktFare=fare,
        )
        return [v[c] for c in db1b.COLUMNS] + [bulk]

    path = _write_csv(
        tmp_path / "db1b.csv",
        header,
        [
            row("a", "JFK", "LAX", 300.0),
            row("b", "JFK", "BOS", 300.0),  # BOS not tracked
            row("c", "JFK", "LAX", 10.0),  # likely award ticket
            row("d", "JFK", "LAX", 300.0, 1.0),  # bulk fare
            row("e", "JFK", "LAX", 9000.0),  # outlier
        ],
    )
    table = db1b.read_filtered(path)
    assert table.column("mkt_id").to_pylist() == ["a"]


def test_quarter_end_is_end_of_day() -> None:
    assert db1b.quarter_end(2025, 1) == datetime(2025, 3, 31, 23, 59, 59, 999999, tzinfo=UTC)

"""Parser tests against real Debezium 3.5 messages captured from the local stack."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from fcp.cdc.events import compute_delta, parse

FIXTURES = Path(__file__).parents[1] / "fixtures" / "debezium"


def load(name: str) -> tuple[bytes, bytes]:
    doc: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text())
    return json.dumps(doc["key"]).encode(), json.dumps(doc["value"]).encode()


def test_update_carries_record_id_commit_time_and_delta() -> None:
    event = parse(*load("airport_update"))
    assert event is not None
    assert event.op == "u"
    assert event.record_key == "source.airport:TEST-FIXTURE"  # same format as freshness.registry
    assert event.delta == {"name": ("Fixture Field", "Fixture Field Renamed")}  # updated_at excluded
    assert event.committed_at.tzinfo is not None
    assert 0 <= event.capture_latency_ms < 60_000


def test_create_and_delete_report_full_row_as_delta() -> None:
    created = parse(*load("airport_create"))
    deleted = parse(*load("airport_delete"))
    assert created is not None and deleted is not None
    assert (created.op, created.before) == ("c", None)
    assert created.delta["name"] == (None, "Fixture Field")
    assert (deleted.op, deleted.after) == ("d", None)
    assert deleted.delta["iata"] == ("ZZZ", None)


def test_snapshot_read_keeps_numeric_as_exact_string() -> None:
    event = parse(*load("fare_snapshot_read"))
    assert event is not None
    assert event.op == "r"
    assert event.table == "source.fare"
    assert isinstance(event.after["fare_usd"], str)  # decimal.handling.mode=string: no float rounding
    assert event.key == event.after["fare_key"]


def test_tombstone_and_foreign_table_are_ignored() -> None:
    key, value = load("airport_update")
    assert parse(key, None) is None
    other = json.loads(value)
    other["source"]["table"] = "not_tracked"
    assert parse(key, json.dumps(other).encode()) is None


def test_key_falls_back_to_row_image_when_message_has_no_key() -> None:
    _, value = load("airport_update")
    event = parse(None, value)
    assert event is not None
    assert event.key == "TEST-FIXTURE"


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        ({"a": 1, "updated_at": "t1"}, {"a": 1, "updated_at": "t2"}, {}),
        ({"a": 1}, {"a": 2}, {"a": (1, 2)}),
        (None, {"a": 1}, {"a": (None, 1)}),
    ],
)
def test_compute_delta(
    before: dict[str, Any] | None, after: dict[str, Any] | None, expected: dict[str, Any]
) -> None:
    assert compute_delta(before, after) == expected

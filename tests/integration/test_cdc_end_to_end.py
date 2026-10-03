"""A row change in Postgres arrives in Kafka as a parsed ChangeEvent with the right delta."""

from __future__ import annotations

import time
import uuid

import pytest

from fcp.cdc import connect as cdc_connect
from fcp.cdc.consumer import make_consumer
from fcp.cdc.events import ChangeEvent, parse, topics
from fcp.common.db import connect

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def _connector_running() -> None:
    try:
        state = cdc_connect.status()
    except Exception as exc:
        pytest.skip(f"Kafka Connect not reachable: {exc}")
    if state["connector"]["state"] != "RUNNING":
        pytest.skip("Debezium connector not registered; run `make cdc`")


def test_insert_update_delete_flow_through_debezium() -> None:
    ident = f"T-{uuid.uuid4().hex[:8]}"
    consumer = make_consumer()
    consumer.subscribe(topics())
    deadline = time.monotonic() + 30
    while not consumer.assignment() and time.monotonic() < deadline:  # wait for partitions
        consumer.poll(0.5)

    with connect(autocommit=True) as conn:
        conn.execute(
            "insert into source.airport (ident, iata, name, type, latitude, longitude) "
            "values (%s, 'ZZZ', 'CDC Test', 'small_airport', 1, 2)",
            (ident,),
        )
        conn.execute(
            "update source.airport set name = 'CDC Test 2', updated_at = now() where ident = %s", (ident,)
        )
        conn.execute("delete from source.airport where ident = %s", (ident,))

    events: list[ChangeEvent] = []
    deadline = time.monotonic() + 60
    try:
        while len(events) < 3 and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            event = parse(msg.key(), msg.value())
            if event is not None and event.key == ident:
                events.append(event)
    finally:
        consumer.close()

    assert [e.op for e in events] == ["c", "u", "d"]  # one partition per key keeps order
    assert events[1].delta == {"name": ("CDC Test", "CDC Test 2")}
    assert all(e.record_key == f"source.airport:{ident}" for e in events)
    assert all(e.capture_latency_ms < 10_000 for e in events)

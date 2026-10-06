"""Kafka consumption helpers: `fcp cdc tail` (human view) and `fcp cdc stats` (latency evidence).

The Phase 3 re-index consumer builds on `iter_events`.
"""

from __future__ import annotations

import statistics
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError

from fcp.cdc.events import ChangeEvent, parse, topics
from fcp.common.db import connect
from fcp.common.settings import get_settings


def make_consumer(group_id: str | None = None, *, from_beginning: bool = False) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap,
            # A throwaway group by default, so inspection never moves a real consumer's offsets.
            "group.id": group_id or f"fcp-inspect-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": group_id is not None,
            # Closing an inspection consumer is routine; do not print librdkafka disconnect noise.
            "log.connection.close": False,
            "log_level": 3,
        }
    )


@dataclass(frozen=True, slots=True)
class Received:
    event: ChangeEvent
    received_at: datetime

    @property
    def delivery_latency_ms(self) -> int:
        return int((self.received_at - self.event.committed_at).total_seconds() * 1000)


def iter_events(
    consumer: Consumer,
    *,
    seconds: float | None = None,
    limit: int | None = None,
    tables: list[str] | None = None,
) -> Iterator[Received]:
    consumer.subscribe(topics())
    deadline = time.monotonic() + seconds if seconds else None
    count = 0
    try:
        while deadline is None or time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error: {err}")
            event = parse(msg.key(), msg.value())
            if event is None or (tables and event.table not in tables):
                continue
            yield Received(event, datetime.now(UTC))
            count += 1
            if limit is not None and count >= limit:
                return
    finally:
        consumer.close()


def _pct(values: list[int], q: float) -> int | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return int(statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1])


@dataclass(frozen=True, slots=True)
class WindowStats:
    table: str
    op: str
    events: int
    p50_capture_ms: int | None
    p95_capture_ms: int | None
    p50_delivery_ms: int | None
    p95_delivery_ms: int | None


def measure(seconds: float) -> list[WindowStats]:
    """Consume new events for `seconds`, write per (table, op) latency stats to metrics.cdc_window.

    Snapshot reads (op 'r') are excluded from latency: their commit time is the snapshot time.
    """
    start = datetime.now(UTC)
    groups: dict[tuple[str, str], list[Received]] = defaultdict(list)
    for r in iter_events(make_consumer(), seconds=seconds):
        groups[(r.event.table, r.event.op)].append(r)
    end = datetime.now(UTC)

    results = []
    for (table, op), items in sorted(groups.items()):
        cap = [i.event.capture_latency_ms for i in items] if op != "r" else []
        dlv = [i.delivery_latency_ms for i in items] if op != "r" else []
        results.append(
            WindowStats(table, op, len(items), _pct(cap, 50), _pct(cap, 95), _pct(dlv, 50), _pct(dlv, 95))
        )
    with connect() as conn:
        for s in results:
            conn.execute(
                """
                insert into metrics.cdc_window (window_start, window_end, table_name, op, events,
                  p50_capture_ms, p95_capture_ms, p50_delivery_ms, p95_delivery_ms)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    start,
                    end,
                    s.table,
                    s.op,
                    s.events,
                    s.p50_capture_ms,
                    s.p95_capture_ms,
                    s.p50_delivery_ms,
                    s.p95_delivery_ms,
                ),
            )
    return results


def format_event(r: Received, *, width: int = 60) -> str:
    e = r.event
    if e.op in ("c", "r"):
        detail = "new record" if e.op == "c" else "snapshot"
    elif e.op == "d":
        detail = "deleted"
    else:
        detail = ", ".join(f"{k}: {_short(v[0])} -> {_short(v[1])}" for k, v in e.delta.items())
    return (
        f"{e.committed_at:%H:%M:%S} {e.op} {e.table:<20} {e.key[:40]:<40} {detail[: width * 3]}"
        f"  ({r.delivery_latency_ms} ms)"
    )


def _short(v: object) -> str:
    s = "null" if v is None else str(v)
    return s if len(s) <= 24 else s[:21] + "..."

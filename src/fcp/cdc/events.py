"""Debezium change events -> typed `ChangeEvent` (record id, commit time, delta).

Wire format (cdc/debezium/fcp-source.json): JSON converter without schemas, so the key is
`{"<pk>": "<value>"}` and the value is the Debezium envelope
`{"before", "after", "source": {"table", "schema", "ts_ms", "lsn", ...}, "op", "ts_ms"}`.
Numeric columns arrive as strings (`decimal.handling.mode=string`); timestamptz as ISO-8601.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

Op = Literal["c", "u", "d", "r"]  # create, update, delete, read (snapshot)

TABLE_KEYS: dict[str, str] = {
    "source.flight_state": "flight_key",
    "source.fare": "fare_key",
    "source.airport": "ident",
}
# Columns that change on every business change and so carry no information in a delta.
BOOKKEEPING = frozenset({"updated_at"})


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    table: str  # e.g. "source.fare"
    op: Op
    key: str  # primary-key value
    committed_at: datetime  # Postgres commit time (source.ts_ms)
    captured_at: datetime  # when Debezium processed the change (envelope ts_ms)
    lsn: int | None
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    delta: dict[str, tuple[Any, Any]] = field(default_factory=dict)  # column -> (old, new)

    @property
    def record_key(self) -> str:
        """Same key format as freshness.registry and index.chunks."""
        return f"{self.table}:{self.key}"

    @property
    def capture_latency_ms(self) -> int:
        return int((self.captured_at - self.committed_at).total_seconds() * 1000)


def _ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, UTC)


def compute_delta(before: dict[str, Any] | None, after: dict[str, Any] | None) -> dict[str, tuple[Any, Any]]:
    """Columns whose value differs, excluding bookkeeping columns.

    Inserts and snapshot reads report every column as (None, value); deletes as (value, None).
    """
    b, a = before or {}, after or {}
    return {
        col: (b.get(col), a.get(col))
        for col in sorted(set(b) | set(a))
        if col not in BOOKKEEPING and b.get(col) != a.get(col)
    }


def parse(key: bytes | None, value: bytes | None) -> ChangeEvent | None:
    """Parse one Kafka message. Returns None for tombstones and messages from other tables."""
    if value is None:
        return None
    envelope = json.loads(value)
    source = envelope.get("source") or {}
    table = f"{source.get('schema')}.{source.get('table')}"
    pk_col = TABLE_KEYS.get(table)
    if pk_col is None:
        return None
    before, after = envelope.get("before"), envelope.get("after")
    # Prefer the message key; fall back to the row image.
    pk = json.loads(key)[pk_col] if key else (after or before or {})[pk_col]
    return ChangeEvent(
        table=table,
        op=envelope["op"],
        key=str(pk),
        committed_at=_ms(source["ts_ms"]),
        captured_at=_ms(envelope["ts_ms"]),
        lsn=source.get("lsn"),
        before=before,
        after=after,
        delta=compute_delta(before, after),
    )


def topics(prefix: str = "fcp") -> list[str]:
    return [f"{prefix}.{t}" for t in TABLE_KEYS]

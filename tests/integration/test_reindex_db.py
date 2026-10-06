"""process_batch against real Postgres + pgvector, in a scratch index table (rolled back)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from fcp.cdc.events import ChangeEvent
from fcp.reindex.consumer import process_batch
from fcp.reindex.embedder import HashEmbedder

pytestmark = pytest.mark.integration
T = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)
TABLE = "index.chunks_test"


@pytest.fixture
def scratch(db: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    db.execute(f"create table {TABLE} (like index.chunks including defaults including constraints)")
    db.execute(f"alter table {TABLE} add primary key (chunk_id)")
    return db


def ev(ident: str, op: str, name: str | None) -> ChangeEvent:
    after = (
        None
        if name is None
        else {
            "ident": ident,
            "iata": "ZZZ",
            "name": name,
            "municipality": None,
            "region": None,
            "type": "small_airport",
            "latitude": 1.0,
            "longitude": 2.0,
            "elevation_ft": None,
            "source": "ourairports",
            "updated_at": "2026-10-06T10:00:00Z",
        }
    )
    return ChangeEvent("source.airport", op, ident, T, T, None, None, after)  # type: ignore[arg-type]


def chunk(db: psycopg.Connection[Any], ident: str) -> dict[str, Any] | None:
    return db.execute(
        f"select chunk_text, embed_model, source_changed_at from {TABLE} where chunk_id = %s",
        (f"source.airport:{ident}",),
    ).fetchone()


def test_insert_replay_update_delete(scratch: psycopg.Connection[Any]) -> None:
    e = HashEmbedder()
    first = process_batch(scratch, [ev("T1", "c", "Test Field")], e, table=TABLE)
    assert (first.embedded, first.skipped) == (1, 0)
    row = chunk(scratch, "T1")
    assert row is not None
    assert row["embed_model"] == "hash-test"
    assert row["source_changed_at"] == T  # version of the source row the chunk was built from

    replay = process_batch(scratch, [ev("T1", "c", "Test Field")], e, table=TABLE)
    assert (replay.embedded, replay.skipped) == (0, 1)  # at-least-once delivery is free

    update = process_batch(scratch, [ev("T1", "u", "Test Field Renamed")], e, table=TABLE)
    assert update.embedded == 1
    row = chunk(scratch, "T1")
    assert row is not None
    assert "Test Field Renamed" in row["chunk_text"]

    gone = process_batch(scratch, [ev("T1", "d", None)], e, table=TABLE)
    assert gone.deleted == 1
    assert chunk(scratch, "T1") is None


def test_batch_metrics_are_recorded(scratch: psycopg.Connection[Any]) -> None:
    process_batch(
        scratch, [ev("T2", "c", "A"), ev("T2", "u", "B"), ev("T3", "c", "C")], HashEmbedder(), table=TABLE
    )
    batch = scratch.execute(
        "select events, records, collapsed, embedded, embed_model from metrics.reindex_batch "
        "order by id desc limit 1"
    ).fetchone()
    assert batch == {"events": 3, "records": 2, "collapsed": 1, "embedded": 2, "embed_model": "hash-test"}
    actions = scratch.execute(
        "select record_key, action from metrics.reindex_log where record_key like "
        "'source.airport:T_' order by record_key"
    ).fetchall()
    assert [(a["record_key"], a["action"]) for a in actions] == [
        ("source.airport:T2", "embedded"),
        ("source.airport:T3", "embedded"),
    ]

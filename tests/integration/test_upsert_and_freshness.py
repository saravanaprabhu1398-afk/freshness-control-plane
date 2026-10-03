from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from fcp.common.db import FreshnessEntry, record_freshness, upsert_changed

pytestmark = pytest.mark.integration


@pytest.fixture
def temp_table(db: psycopg.Connection[Any]) -> str:
    db.execute(
        "create temp table t_upsert (id text primary key, a int, b text, c text, "
        "updated_at timestamptz not null default now())"
    )
    return "pg_temp.t_upsert"


def _xmin(db: psycopg.Connection[Any], key: str) -> int:
    return int(
        db.execute("select xmin::text::bigint as x from pg_temp.t_upsert where id = %s", (key,)).fetchone()[
            "x"
        ]
    )  # type: ignore[index]


def test_insert_then_noop_then_update(db: psycopg.Connection[Any], temp_table: str) -> None:
    rows = [{"id": "r1", "a": 1, "b": "x"}, {"id": "r2", "a": 2, "b": "y"}]
    first = upsert_changed(db, temp_table, rows, key="id", business_cols=("a", "b"))
    assert first.inserted == {"r1", "r2"} and not first.updated and not first.unchanged

    version = _xmin(db, "r1")
    db.commit()  # new transaction so a rewrite would get a new xmin
    again = upsert_changed(db, temp_table, rows, key="id", business_cols=("a", "b"))
    assert again.unchanged == {"r1", "r2"} and not again.changed
    assert _xmin(db, "r1") == version  # unchanged row was not rewritten -> no WAL, no CDC event

    changed = upsert_changed(
        db, temp_table, [{"id": "r1", "a": 5, "b": "x"}], key="id", business_cols=("a", "b")
    )
    assert changed.updated == {"r1"}


def test_overrides_keep_existing_value(db: psycopg.Connection[Any], temp_table: str) -> None:
    keep = {"c": "coalesce(excluded.c, t.c)"}
    upsert_changed(
        db,
        temp_table,
        [{"id": "r1", "a": 1, "c": "kept"}],
        key="id",
        business_cols=("a", "c"),
        overrides=keep,
    )
    res = upsert_changed(
        db, temp_table, [{"id": "r1", "a": 1, "c": None}], key="id", business_cols=("a", "c"), overrides=keep
    )
    assert res.unchanged == {"r1"}
    assert db.execute("select c from pg_temp.t_upsert").fetchone()["c"] == "kept"  # type: ignore[index]


def test_record_freshness_moves_last_changed_only_on_change(db: psycopg.Connection[Any]) -> None:
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    entry = FreshnessEntry("test:rec", "fare", t1)
    record_freshness(db, [entry], changed_keys={"test:rec"}, verified_at=t1)
    record_freshness(db, [entry], changed_keys=set(), verified_at=t2)
    row = db.execute(
        "select last_verified_at, last_changed_at from freshness.registry where record_key='test:rec'"
    ).fetchone()
    assert row == {"last_verified_at": t2, "last_changed_at": t1}

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from fcp.common.db import RunStats, update_changed
from fcp.common.migrate import upgrade
from fcp.ingestion.fares.drift import DriftParams, apply_tick

pytestmark = pytest.mark.integration


def test_migrations_are_idempotent() -> None:
    assert upgrade() == []


def test_update_changed_writes_only_real_changes(db: psycopg.Connection[Any]) -> None:
    db.execute(
        "create temp table t_upd (id text primary key, a int not null, b text not null, "
        "updated_at timestamptz not null default now())"
    )
    db.execute("insert into t_upd (id, a, b) values ('r1', 1, 'x'), ('r2', 2, 'y')")
    res = update_changed(
        db,
        "pg_temp.t_upd",
        [{"id": "r1", "a": 1}, {"id": "r2", "a": 5}, {"id": "nope", "a": 1}],
        key="id",
        business_cols=("a",),
    )
    assert res.updated == {"r2"}
    assert res.unchanged == {"r1", "nope"}
    assert not res.inserted


def test_drift_tick_is_applied_once(db: psycopg.Connection[Any]) -> None:
    # Own fares, so the test does not depend on a seeded database (CI starts empty).
    with db.cursor() as cur:
        cur.executemany(
            "insert into source.fare (fare_key, origin, dest, carrier, cabin, fare_usd, baseline_usd, "
            "baseline_period, sample_size, effective_at, source, simulated) values "
            "(%s, 'TST', 'TSU', 'ZZ', 'ALL', 300, 300, '2025-Q2', 100, now(), 'bts_db1b', false)",
            [(f"TEST:{i:03d}",) for i in range(100)],
        )
    tick = 10**12  # far outside real tick indexes; rolled back with the transaction anyway
    now = datetime.now(UTC)
    params = DriftParams(p_reprice=0.5)
    first, second = RunStats(), RunStats()
    assert apply_tick(db, first, seed=1, tick=tick, now=now, params=params) is True
    assert first.rows_changed > 0
    rows = db.execute(
        "select count(*) as n from source.fare where simulated and source = 'drift_sim' "
        "and effective_at = %s",
        (now,),
    ).fetchone()
    assert rows == {"n": first.rows_changed}  # every change labelled as simulated
    assert apply_tick(db, second, seed=1, tick=tick, now=now, params=params) is False
    assert second.status == "skipped"

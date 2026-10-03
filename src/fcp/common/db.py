"""Postgres helpers: connections, change-aware upserts, freshness registry, run logging."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from fcp.common.settings import get_settings

Row = Mapping[str, Any]


def connect(*, autocommit: bool = False) -> psycopg.Connection[dict[str, Any]]:
    return psycopg.connect(get_settings().pg_dsn, autocommit=autocommit, row_factory=dict_row)


@dataclass(slots=True)
class UpsertResult:
    inserted: set[str] = field(default_factory=set)
    updated: set[str] = field(default_factory=set)
    unchanged: set[str] = field(default_factory=set)

    @property
    def changed(self) -> set[str]:
        return self.inserted | self.updated


def upsert_changed(
    conn: psycopg.Connection[Any],
    table: str,
    rows: Sequence[Row],
    *,
    key: str,
    business_cols: Sequence[str],
    insert_only_cols: Sequence[str] = (),
    overrides: Mapping[str, str] | None = None,
) -> UpsertResult:
    """Insert new rows; update existing rows only when a business column actually changes.

    Unchanged rows are not written at all, so they produce no WAL record and therefore no
    CDC event (ADR-001). `updated_at` is bumped only on a real change.

    `overrides` maps a business column to a SQL expression over `t` (current row) and
    `excluded` (incoming row), e.g. keep a value the incoming row does not know about:
    ``{"last_seen": "coalesce(excluded.last_seen, t.last_seen)"}``.
    Expressions are trusted constants written in this codebase, never user input.
    """
    result = UpsertResult()
    if not rows:
        return result
    overrides = dict(overrides or {})
    schema, name = table.split(".")
    cols = [key, *business_cols, *insert_only_cols]
    new_exprs = [sql.SQL(overrides.get(c, f"excluded.{c}")) for c in business_cols]

    stmt = sql.SQL(
        "insert into {table} as t ({cols}) values ({vals}) "
        "on conflict ({key}) do update set {sets}, updated_at = now() "
        "where ({old}) is distinct from ({new}) "
        "returning t.{key} as key, (xmax = 0) as inserted"
    ).format(
        table=sql.Identifier(schema, name),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        vals=sql.SQL(", ").join(sql.Placeholder(c) for c in cols),
        key=sql.Identifier(key),
        sets=sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(c), e)
            for c, e in zip(business_cols, new_exprs, strict=True)
        ),
        old=sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c in business_cols),
        new=sql.SQL(", ").join(new_exprs),
    )
    with conn.cursor() as cur:
        cur.executemany(stmt, rows, returning=True)
        while True:
            for rec in cur.fetchall():
                rec_key = rec["key"] if isinstance(rec, Mapping) else rec[0]
                is_insert = rec["inserted"] if isinstance(rec, Mapping) else rec[1]
                (result.inserted if is_insert else result.updated).add(rec_key)
            if not cur.nextset():
                break
    result.unchanged = {r[key] for r in rows} - result.changed
    return result


def update_changed(
    conn: psycopg.Connection[Any],
    table: str,
    rows: Sequence[Row],
    *,
    key: str,
    business_cols: Sequence[str],
) -> UpsertResult:
    """Update existing rows only where a business column actually changes (never inserts).

    Same guarantee as `upsert_changed`: an unchanged row is not written, so it produces no WAL
    record and no CDC event. Use it when the caller only knows the changing columns (an
    INSERT ... ON CONFLICT would fail NOT NULL checks on the columns it does not supply).
    Keys that do not exist are reported as unchanged.
    """
    result = UpsertResult()
    if not rows:
        return result
    schema, name = table.split(".")
    stmt = sql.SQL(
        "update {table} as t set {sets}, updated_at = now() "
        "where t.{key} = {key_ph} and ({old}) is distinct from ({new}) "
        "returning t.{key} as key"
    ).format(
        table=sql.Identifier(schema, name),
        sets=sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder(c)) for c in business_cols
        ),
        key=sql.Identifier(key),
        key_ph=sql.Placeholder(key),
        old=sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c in business_cols),
        new=sql.SQL(", ").join(sql.Placeholder(c) for c in business_cols),
    )
    with conn.cursor() as cur:
        cur.executemany(stmt, rows, returning=True)
        while True:
            for rec in cur.fetchall():
                result.updated.add(rec["key"] if isinstance(rec, Mapping) else rec[0])
            if not cur.nextset():
                break
    result.unchanged = {r[key] for r in rows} - result.updated
    return result


@dataclass(frozen=True, slots=True)
class FreshnessEntry:
    record_key: str
    contract: str
    data_as_of: datetime


def record_freshness(
    conn: psycopg.Connection[Any],
    entries: Sequence[FreshnessEntry],
    *,
    changed_keys: set[str],
    verified_at: datetime | None = None,
) -> None:
    """Mark records as verified now; bump `last_changed_at` only for records that changed.

    Writes go to freshness.registry, which is not captured by CDC, so re-verifying an
    unchanged record costs one small UPDATE and no embedding.
    """
    if not entries:
        return
    ts = verified_at or datetime.now(UTC)
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into freshness.registry as r
              (record_key, contract, last_verified_at, last_changed_at, data_as_of)
            select k, c, %(ts)s, %(ts)s, d
            from unnest(%(keys)s::text[], %(contracts)s::text[], %(as_of)s::timestamptz[]) as u(k, c, d)
            on conflict (record_key) do update
              set last_verified_at = excluded.last_verified_at,
                  data_as_of       = excluded.data_as_of,
                  contract         = excluded.contract
            """,
            {
                "ts": ts,
                "keys": [e.record_key for e in entries],
                "contracts": [e.contract for e in entries],
                "as_of": [e.data_as_of for e in entries],
            },
        )
        changed = [e.record_key for e in entries if e.record_key in changed_keys]
        if changed:
            cur.execute(
                "update freshness.registry set last_changed_at = %(ts)s where record_key = any(%(keys)s)",
                {"ts": ts, "keys": changed},
            )


@dataclass(slots=True)
class RunStats:
    rows_seen: int = 0
    rows_changed: int = 0
    rows_unchanged: int = 0
    credits_used: int = 0
    status: str = "succeeded"
    detail: dict[str, Any] = field(default_factory=dict)

    def add(self, result: UpsertResult) -> None:
        self.rows_seen += len(result.changed) + len(result.unchanged)
        self.rows_changed += len(result.changed)
        self.rows_unchanged += len(result.unchanged)


@contextmanager
def track_run(job: str) -> Iterator[RunStats]:
    """Record a run in metrics.ingest_run, including failures (own autocommit connection)."""
    stats = RunStats()
    with connect(autocommit=True) as log_conn:
        run_id = log_conn.execute(
            "insert into metrics.ingest_run (job, started_at, status) "
            "values (%s, now(), 'running') returning id",
            (job,),
        ).fetchone()["id"]  # type: ignore[index]
        try:
            yield stats
        except BaseException as exc:
            stats.status = "failed"
            stats.detail["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            log_conn.execute(
                """
                update metrics.ingest_run
                   set finished_at = now(), status = %s, rows_seen = %s, rows_changed = %s,
                       rows_unchanged = %s, credits_used = %s, detail = %s
                 where id = %s
                """,
                (
                    stats.status,
                    stats.rows_seen,
                    stats.rows_changed,
                    stats.rows_unchanged,
                    stats.credits_used,
                    json.dumps(stats.detail, default=str),
                    run_id,
                ),
            )

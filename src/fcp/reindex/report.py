"""Savings report: what selective re-indexing embedded vs two naive baselines (PRD FR-5, NFR-2).

Definitions (stated in the case study exactly as here):

* **Selective (measured):** chunks the consumer actually embedded from change events in the
  window, excluding the one-off initial build from the Debezium snapshot (op = 'r').
Baselines, from least to most naive:

* **Per change event:** re-embed the record of every change event, without collapsing events
  within a batch or checking content hashes. Isolates what the consumer itself saves.
* **Per row refreshed:** re-embed every row each refresh job touched, changed or not (no change
  detection anywhere). Captures what change-aware writes + CDC save upstream.
* **Full corpus per refresh:** re-embed the whole corpus each time a refresh job wrote anything
  (live poll, drift tick, reference load). The common naive RAG pipeline. Corpus size, tokens and
  time come from the latest full-rebuild benchmark.

Only the real model's work is counted; rows written by the test embedder are excluded.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fcp.common.db import connect

REFRESH_JOBS = (
    "opensky_states",
    "opensky_flights",
    "fare_drift",
    "fare_shock",
    "ourairports",
    "fare_baseline_seed",
)


@dataclass(frozen=True, slots=True)
class Savings:
    window: timedelta
    change_events: int
    collapsed: int
    skipped_same_hash: int
    embedded: int
    deleted: int
    selective_tokens: int
    selective_embed_ms: int
    refresh_runs: int
    rows_refreshed: int
    corpus_chunks: int | None
    full_tokens: int | None
    full_embed_ms: int | None

    @property
    def per_event_embeds(self) -> int:
        return self.change_events - self.deleted

    @property
    def per_row_embeds(self) -> int:
        return self.rows_refreshed

    @property
    def full_embeds(self) -> int | None:
        return None if self.corpus_chunks is None else self.corpus_chunks * self.refresh_runs

    @property
    def full_tokens_total(self) -> int | None:
        return None if self.full_tokens is None else self.full_tokens * self.refresh_runs

    @property
    def full_ms_total(self) -> int | None:
        return None if self.full_embed_ms is None else self.full_embed_ms * self.refresh_runs

    @staticmethod
    def _saving(actual: int, baseline: int | None) -> float | None:
        return None if not baseline else 1 - actual / baseline

    @property
    def saving_vs_per_event(self) -> float | None:
        return self._saving(self.embedded, self.per_event_embeds)

    @property
    def saving_vs_per_row(self) -> float | None:
        return self._saving(self.embedded, self.per_row_embeds)

    @property
    def saving_vs_full(self) -> float | None:
        return self._saving(self.selective_tokens, self.full_tokens_total)


def compute(window: timedelta | None = None, *, since: datetime | None = None) -> Savings:
    """Savings for events after `since`, or within the last `window` (default 24 h)."""
    start = since or datetime.now(UTC) - (window or timedelta(hours=24))
    window = datetime.now(UTC) - start
    with connect() as conn:
        sel = conn.execute(
            """
            select count(*) filter (where op <> 'r')                                         as events,
                   count(*) filter (where op <> 'r' and action = 'skipped_same_hash')         as skipped,
                   count(*) filter (where op <> 'r' and action = 'embedded')                  as embedded,
                   count(*) filter (where op <> 'r' and action like 'deleted%%')              as deleted,
                   coalesce(sum(tokens) filter (where op <> 'r' and action = 'embedded'), 0)  as tokens,
                   coalesce(sum(embed_ms) filter (where op <> 'r' and action = 'embedded'), 0) as embed_ms
            from metrics.reindex_log l
            where event_ts >= %s
              and not exists (select 1 from metrics.reindex_batch b
                              where b.id = l.batch_id and b.embed_model = 'hash-test')
            """,
            (start,),
        ).fetchone()
        # Collapsed events never reach reindex_log (they were superseded inside a batch).
        collapsed = conn.execute(
            "select coalesce(sum(collapsed), 0) as c from metrics.reindex_batch "
            "where finished_at >= %s and embed_model <> 'hash-test'",
            (start,),
        ).fetchone()
        runs = conn.execute(
            """
            select count(*) as n, coalesce(sum(rows_seen), 0) as rows from metrics.ingest_run
            where started_at >= %s and status = 'succeeded' and job = any(%s) and rows_changed > 0
            """,
            (start, list(REFRESH_JOBS)),
        ).fetchone()
        full = conn.execute(
            "select total_chunks, tokens, embed_ms from metrics.full_rebuild_log order by run_ts desc limit 1"
        ).fetchone()
    if sel is None or collapsed is None or runs is None:  # aggregates always return a row
        raise RuntimeError("metrics query returned no row")
    return Savings(
        window=window,
        change_events=int(sel["events"]) + int(collapsed["c"]),
        collapsed=int(collapsed["c"]),
        skipped_same_hash=int(sel["skipped"]),
        embedded=int(sel["embedded"]),
        deleted=int(sel["deleted"]),
        selective_tokens=int(sel["tokens"]),
        selective_embed_ms=int(sel["embed_ms"]),
        refresh_runs=int(runs["n"]),
        rows_refreshed=int(runs["rows"]),
        corpus_chunks=full["total_chunks"] if full else None,
        full_tokens=full["tokens"] if full else None,
        full_embed_ms=full["embed_ms"] if full else None,
    )


def format_report(s: Savings) -> str:
    def pct(v: float | None) -> str:
        return "n/a" if v is None else f"{v:.1%}"

    def num(v: int | None) -> str:
        return "n/a (run `fcp reindex benchmark`)" if v is None else f"{v:,}"

    hours = s.window.total_seconds() / 3600
    lines = [
        f"Selective re-indexing, last {hours:g} h",
        f"  change events received          {s.change_events:>12,}",
        f"    superseded within a batch     {s.collapsed:>12,}",
        f"    skipped, content unchanged    {s.skipped_same_hash:>12,}",
        f"    deleted                       {s.deleted:>12,}",
        f"    embedded                      {s.embedded:>12,}   ({s.selective_tokens:,} tokens, "
        f"{s.selective_embed_ms / 1000:.1f} s)",
        "",
        "Compared with re-embedding...                        embeds      saving",
        f"  every change event (no collapse, no hash check) {s.per_event_embeds:>10,}"
        f"   {pct(s.saving_vs_per_event):>9}",
        f"  every row the {s.refresh_runs:,} refresh runs touched          {s.per_row_embeds:>10,}"
        f"   {pct(s.saving_vs_per_row):>9}",
        f"  the full corpus on each of those runs           {num(s.full_embeds):>10}"
        f"   {pct(s.saving_vs_full):>9}"
        f"  (tokens: {num(s.full_tokens_total)})",
    ]
    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class TableCoverage:
    table: str
    source_rows: int
    chunks: int
    missing: int  # source rows with no chunk
    behind: int  # chunk built from an older version of the row (index lag, ADR-015)
    orphans: int  # chunk whose source row no longer exists


COVERAGE_SQL = """
select %(table)s as table,
       (select count(*) from {src}) as source_rows,
       (select count(*) from index.chunks where source_table = %(table)s) as chunks,
       (select count(*) from {src} s where not exists
          (select 1 from index.chunks c where c.chunk_id = %(prefix)s || s.{key})) as missing,
       (select count(*) from {src} s join index.chunks c on c.chunk_id = %(prefix)s || s.{key}
          where s.updated_at > c.source_changed_at) as behind,
       (select count(*) from index.chunks c where c.source_table = %(table)s and not exists
          (select 1 from {src} s where %(prefix)s || s.{key} = c.chunk_id)) as orphans
"""


def coverage() -> list[TableCoverage]:
    from psycopg import sql

    from fcp.reindex.rebuild import TABLES

    out = []
    with connect() as conn:
        for table, key in TABLES.items():
            schema, name = table.split(".")
            q = sql.SQL(COVERAGE_SQL).format(src=sql.Identifier(schema, name), key=sql.Identifier(key))
            r = conn.execute(q, {"table": table, "prefix": f"{table}:"}).fetchone()
            assert r
            out.append(
                TableCoverage(table, r["source_rows"], r["chunks"], r["missing"], r["behind"], r["orphans"])
            )
    return out


def consumer_lag(group_id: str) -> dict[str, int]:
    """Messages not yet processed by the consumer group, per topic."""
    from confluent_kafka import Consumer, TopicPartition

    from fcp.cdc.events import topics
    from fcp.common.settings import get_settings

    c = Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "log_level": 3,
        }
    )
    try:
        lag: dict[str, int] = {}
        for topic in topics():
            meta = c.list_topics(topic, timeout=10).topics.get(topic)
            if meta is None or meta.error is not None:
                continue
            parts = [TopicPartition(topic, p) for p in meta.partitions]
            committed = c.committed(parts, timeout=10)
            total = 0
            for tp in committed:
                low, high = c.get_watermark_offsets(tp, timeout=10)
                pos = tp.offset if tp.offset >= 0 else low
                total += max(high - pos, 0)
            lag[topic] = total
        return lag
    finally:
        c.close()

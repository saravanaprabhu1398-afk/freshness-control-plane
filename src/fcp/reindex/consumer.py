"""Selective re-index consumer (SDD §4, ARD AR-2/AR-3).

For each micro-batch of Debezium change events:

1. Collapse: keep only the latest event per record. Earlier events for the same record in the
   batch are superseded and never embedded.
2. Render each surviving record to chunk text and hash it. If the hash equals the indexed one,
   skip (no embedding). This makes replays and no-op updates free: delivery is at-least-once.
3. Embed only the changed chunks, in one call.
4. One Postgres transaction: upsert/delete chunks, write per-record and per-batch metrics.
5. Only then commit Kafka offsets. A crash between 4 and 5 replays the batch, and step 2 turns
   the replay into skips.
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError
from psycopg import sql

from fcp.cdc.events import ChangeEvent, parse, topics
from fcp.common.db import connect
from fcp.common.logging import get_logger
from fcp.common.settings import get_settings
from fcp.reindex.chunker import Chunk, build
from fcp.reindex.embedder import Embedder, default_embedder

GROUP_ID = "fcp-reindex"
INDEX_TABLE = "index.chunks"
log = get_logger(__name__)


# ----------------------------------------------------------------------------- planning (pure)


@dataclass(frozen=True, slots=True)
class Plan:
    upserts: list[tuple[ChangeEvent, Chunk]] = field(default_factory=list)  # hash changed or new
    skips: list[tuple[ChangeEvent, Chunk]] = field(default_factory=list)  # same hash as indexed
    deletes: list[ChangeEvent] = field(default_factory=list)
    events: int = 0
    collapsed: int = 0


def collapse(events: Sequence[ChangeEvent]) -> list[ChangeEvent]:
    """Latest event per record, in arrival order (Kafka keeps per-record order within a partition)."""
    latest: dict[str, ChangeEvent] = {}
    for e in events:
        latest.pop(e.record_key, None)  # re-insert so dict order follows the latest arrival
        latest[e.record_key] = e
    return list(latest.values())


def plan_batch(events: Sequence[ChangeEvent], indexed_hashes: dict[str, str]) -> Plan:
    survivors = collapse(events)
    plan = Plan(events=len(events), collapsed=len(events) - len(survivors))
    for e in survivors:
        if e.op == "d" or e.after is None:
            plan.deletes.append(e)
            continue
        chunk = build(e.table, e.key, e.after, e.committed_at)
        target = plan.skips if indexed_hashes.get(chunk.chunk_id) == chunk.content_hash else plan.upserts
        target.append((e, chunk))
    return plan


# ----------------------------------------------------------------------------- applying (DB)


@dataclass(slots=True)
class BatchResult:
    events: int = 0
    records: int = 0
    collapsed: int = 0
    embedded: int = 0
    skipped: int = 0
    deleted: int = 0
    tokens: int = 0
    embed_ms: int = 0
    max_e2e_ms: int | None = None


def table_ident(name: str) -> sql.Composable:
    schema, table = name.split(".")
    return sql.Identifier(schema, table)


def indexed_hashes(
    conn: psycopg.Connection[Any], chunk_ids: Iterable[str], table: str = INDEX_TABLE
) -> dict[str, str]:
    ids = list(chunk_ids)
    if not ids:
        return {}
    rows = conn.execute(
        sql.SQL("select chunk_id, content_hash from {} where chunk_id = any(%s)").format(table_ident(table)),
        (ids,),
    ).fetchall()
    return {r["chunk_id"]: r["content_hash"] for r in rows}


def process_batch(
    conn: psycopg.Connection[Any],
    events: Sequence[ChangeEvent],
    embedder: Embedder,
    *,
    table: str = INDEX_TABLE,
) -> BatchResult:
    """Plan, embed and write one batch inside the caller's transaction."""
    started = datetime.now(UTC)
    plan = plan_batch(events, indexed_hashes(conn, {e.record_key for e in events}, table))
    result = BatchResult(
        events=plan.events,
        collapsed=plan.collapsed,
        records=len(plan.upserts) + len(plan.skips) + len(plan.deletes),
    )

    texts = [c.text for _, c in plan.upserts]
    t0 = time.perf_counter()
    vectors = embedder.embed_passages(texts) if texts else []
    result.embed_ms = int((time.perf_counter() - t0) * 1000)
    per_chunk_tokens = [embedder.count_tokens([t]) for t in texts]
    result.tokens = sum(per_chunk_tokens)

    upsert = sql.SQL(
        """
        insert into {t} (chunk_id, source_table, record_key, chunk_text, content_hash, embedding, source,
                         simulated, data_as_of, indexed_at, source_changed_at, embed_model, tokens)
        values (%(chunk_id)s, %(source_table)s, %(record_key)s, %(text)s, %(hash)s, %(embedding)s::vector,
                %(source)s, %(simulated)s, %(data_as_of)s, now(), %(source_changed_at)s, %(model)s,
                %(tokens)s)
        on conflict (chunk_id) do update set
          chunk_text = excluded.chunk_text, content_hash = excluded.content_hash,
          embedding = excluded.embedding,
          source = excluded.source, simulated = excluded.simulated, data_as_of = excluded.data_as_of,
          indexed_at = now(), source_changed_at = excluded.source_changed_at,
          embed_model = excluded.embed_model, tokens = excluded.tokens
        """
    ).format(t=table_ident(table))
    now = datetime.now(UTC)
    log_rows: list[tuple[Any, ...]] = []
    with conn.cursor() as cur:
        if plan.upserts:
            cur.executemany(
                upsert,
                [
                    {
                        "chunk_id": c.chunk_id,
                        "source_table": c.source_table,
                        "record_key": c.record_key,
                        "text": c.text,
                        "hash": c.content_hash,
                        "embedding": vector_literal(v),
                        "source": c.source,
                        "simulated": c.simulated,
                        "data_as_of": c.data_as_of,
                        "source_changed_at": c.source_changed_at,
                        "model": embedder.name,
                        "tokens": n,
                    }
                    for (_, c), v, n in zip(plan.upserts, vectors, per_chunk_tokens, strict=True)
                ],
            )
        for e in plan.deletes:
            deleted = cur.execute(
                sql.SQL("delete from {} where chunk_id = %s").format(table_ident(table)), (e.record_key,)
            ).rowcount
            log_rows.append((e, "deleted" if deleted else "deleted_missing", 0, 0))
        result.embedded, result.skipped, result.deleted = (
            len(plan.upserts),
            len(plan.skips),
            len(plan.deletes),
        )
        per_chunk_ms = result.embed_ms // max(len(plan.upserts), 1)
        log_rows += [
            (e, "embedded", n, per_chunk_ms) for (e, _), n in zip(plan.upserts, per_chunk_tokens, strict=True)
        ]
        log_rows += [(e, "skipped_same_hash", 0, 0) for e, _ in plan.skips]
        e2e = [int((now - e.committed_at).total_seconds() * 1000) for e, *_ in log_rows]
        result.max_e2e_ms = max(e2e) if e2e else None

        batch_id = cur.execute(
            """
            insert into metrics.reindex_batch (started_at, finished_at, events, records, collapsed, embedded,
              skipped_same_hash, deleted, tokens, embed_ms, max_e2e_ms, embed_model)
            values (%s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id
            """,
            (
                started,
                result.events,
                result.records,
                result.collapsed,
                result.embedded,
                result.skipped,
                result.deleted,
                result.tokens,
                result.embed_ms,
                result.max_e2e_ms,
                embedder.name,
            ),
        ).fetchone()["id"]  # type: ignore[index]
        cur.executemany(
            """
            insert into metrics.reindex_log (event_ts, kafka_offset, source_table, record_key, op,
              chunks_considered,
              chunks_reembedded, chunks_skipped_same_hash, embed_ms, e2e_latency_ms, batch_id, action, tokens)
            values (%s, null, %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    e.committed_at,
                    e.table,
                    e.record_key,
                    e.op,
                    int(action == "embedded"),
                    int(action == "skipped_same_hash"),
                    ms,
                    lat,
                    batch_id,
                    action,
                    n,
                )
                for (e, action, n, ms), lat in zip(log_rows, e2e, strict=True)
            ],
        )
    return result


def vector_literal(v: Sequence[float]) -> str:
    return "[" + ",".join(f"{x:.7f}" for x in v) + "]"


# ----------------------------------------------------------------------------- loop (Kafka)


def make_consumer(group_id: str = GROUP_ID) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap,
            "group.id": group_id,
            # A new group starts from the snapshot events, which builds the full index once.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "log.connection.close": False,
            "log_level": 3,
        }
    )


def run(
    *,
    seconds: float | None = None,
    max_batch: int = 64,
    max_wait_s: float = 0.5,
    embedder: Embedder | None = None,
    group_id: str = GROUP_ID,
    table: str = INDEX_TABLE,
    key_prefix: str | None = None,
    stop_event: threading.Event | None = None,
) -> BatchResult:
    """Consume until stopped or for `seconds`. Returns totals.

    On the main thread, SIGINT/SIGTERM stop it cleanly after the current batch. Off the main
    thread (Python allows signal handlers only on the main thread), pass `stop_event` instead.
    `key_prefix` restricts processing to records whose key starts with it (tests only).
    """
    embedder = embedder or default_embedder()
    consumer = make_consumer(group_id)
    consumer.subscribe(topics())
    totals = BatchResult()
    stop_event = stop_event or threading.Event()
    previous: dict[signal.Signals, Any] = {}
    if threading.current_thread() is threading.main_thread():
        previous = {s: signal.signal(s, lambda *_: stop_event.set()) for s in (signal.SIGINT, signal.SIGTERM)}
    deadline = time.monotonic() + seconds if seconds else None
    log.info("reindex.started", group=group_id, table=table, model=embedder.name)
    try:
        with connect() as conn:
            while not stop_event.is_set() and (deadline is None or time.monotonic() < deadline):
                _heartbeat()
                batch = _poll_batch(consumer, max_batch, max_wait_s)
                events = [e for e in batch if key_prefix is None or e.key.startswith(key_prefix)]
                if not batch:
                    continue
                if events:
                    with conn.transaction():
                        r = process_batch(conn, events, embedder, table=table)
                    _add(totals, r)
                    log.info(
                        "reindex.batch",
                        events=r.events,
                        embedded=r.embedded,
                        skipped=r.skipped,
                        deleted=r.deleted,
                        collapsed=r.collapsed,
                        embed_ms=r.embed_ms,
                        max_e2e_ms=r.max_e2e_ms,
                    )
                consumer.commit(asynchronous=False)  # only after the DB transaction committed
    finally:
        consumer.close()
        for s, h in previous.items():
            signal.signal(s, h)
    log.info("reindex.stopped", embedded=totals.embedded, skipped=totals.skipped, deleted=totals.deleted)
    return totals


def _heartbeat() -> None:
    """Prove liveness to the container health check (an idle consumer is still healthy)."""
    path = get_settings().heartbeat_file
    if path is not None:
        path.touch()


def _poll_batch(consumer: Consumer, max_batch: int, max_wait_s: float) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    deadline = time.monotonic() + max_wait_s
    while len(events) < max_batch:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        msgs = consumer.consume(num_messages=max_batch - len(events), timeout=remaining)
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error: {err}")
            event = parse(msg.key(), msg.value())
            if event is not None:
                events.append(event)
    return events


def _add(totals: BatchResult, r: BatchResult) -> None:
    for name in ("events", "records", "collapsed", "embedded", "skipped", "deleted", "tokens", "embed_ms"):
        setattr(totals, name, getattr(totals, name) + getattr(r, name))
    if r.max_e2e_ms is not None:
        totals.max_e2e_ms = max(totals.max_e2e_ms or 0, r.max_e2e_ms)

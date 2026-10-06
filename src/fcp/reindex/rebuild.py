"""Full re-embed of the whole corpus: the naive baseline (SDD §4), and a recovery tool.

* `benchmark()` embeds every source record and records tokens and time in
  metrics.full_rebuild_log without touching the index. This is what a pipeline without change
  detection pays on every refresh.
* `benchmark(apply=True)` also replaces the index from the source tables (recovery when Kafka
  retention no longer holds the snapshot, or after changing the chunk template).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from fcp.common.db import connect
from fcp.common.logging import get_logger
from fcp.reindex.chunker import Chunk, build
from fcp.reindex.consumer import INDEX_TABLE, table_ident, vector_literal
from fcp.reindex.embedder import Embedder, default_embedder

TABLES = {"source.fare": "fare_key", "source.flight_state": "flight_key", "source.airport": "ident"}
log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RebuildResult:
    chunks: int
    tokens: int
    embed_ms: int
    by_table: dict[str, int]
    applied: bool


def load_chunks() -> list[Chunk]:
    chunks: list[Chunk] = []
    with connect() as conn:
        for table, key in TABLES.items():
            schema, name = table.split(".")
            for row in conn.execute(f'select * from "{schema}"."{name}" order by {key}').fetchall():
                chunks.append(build(table, str(row[key]), row, row["updated_at"]))
    return chunks


def benchmark(
    *, apply: bool = False, embedder: Embedder | None = None, batch_size: int = 256
) -> RebuildResult:
    embedder = embedder or default_embedder()
    chunks = load_chunks()
    texts = [c.text for c in chunks]
    t0 = time.perf_counter()
    vectors: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        vectors += embedder.embed_passages(texts[i : i + batch_size])
    embed_ms = int((time.perf_counter() - t0) * 1000)
    tokens = embedder.count_tokens(texts)
    by_table: dict[str, int] = {}
    for c in chunks:
        by_table[c.source_table] = by_table.get(c.source_table, 0) + 1

    with connect() as conn:
        if apply:
            _replace_index(conn, chunks, vectors, embedder)
        conn.execute(
            """
            insert into metrics.full_rebuild_log (run_ts, total_chunks, embed_ms, tokens, embed_model,
              chunks_by_table, applied)
            values (now(), %s, %s, %s, %s, %s, %s)
            """,
            (len(chunks), embed_ms, tokens, embedder.name, json.dumps(by_table), apply),
        )
    log.info("reindex.full_rebuild", chunks=len(chunks), tokens=tokens, embed_ms=embed_ms, applied=apply)
    return RebuildResult(len(chunks), tokens, embed_ms, by_table, apply)


def _replace_index(conn: Any, chunks: list[Chunk], vectors: list[list[float]], embedder: Embedder) -> None:
    t = table_ident(INDEX_TABLE)
    from psycopg import sql

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("delete from {} where not (chunk_id = any(%s))").format(t),
            ([c.chunk_id for c in chunks],),
        )
        cur.executemany(
            sql.SQL(
                """
                insert into {t} (chunk_id, source_table, record_key, chunk_text, content_hash, embedding,
                  source,
                  simulated, data_as_of, indexed_at, source_changed_at, embed_model, tokens)
                values (%s, %s, %s, %s, %s, %s::vector, %s, %s, %s, now(), %s, %s, null)
                on conflict (chunk_id) do update set chunk_text = excluded.chunk_text,
                  content_hash = excluded.content_hash, embedding = excluded.embedding,
                  source = excluded.source,
                  simulated = excluded.simulated, data_as_of = excluded.data_as_of, indexed_at = now(),
                  source_changed_at = excluded.source_changed_at, embed_model = excluded.embed_model
                """
            ).format(t=t),
            [
                (
                    c.chunk_id,
                    c.source_table,
                    c.record_key,
                    c.text,
                    c.content_hash,
                    vector_literal(v),
                    c.source,
                    c.simulated,
                    c.data_as_of,
                    c.source_changed_at,
                    embedder.name,
                )
                for c, v in zip(chunks, vectors, strict=True)
            ],
        )

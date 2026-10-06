"""Postgres -> Debezium -> Kafka -> consumer -> pgvector, into a scratch index table."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator

import pytest

from fcp.cdc import connect as cdc_connect
from fcp.common.db import connect
from fcp.reindex.consumer import run
from fcp.reindex.embedder import HashEmbedder

pytestmark = pytest.mark.integration


@pytest.fixture
def scratch_table() -> Iterator[str]:
    try:
        if cdc_connect.status()["connector"]["state"] != "RUNNING":
            pytest.skip("Debezium connector not running; run `make cdc`")
    except Exception as exc:
        pytest.skip(f"Kafka Connect not reachable: {exc}")
    name = f"index.chunks_e2e_{uuid.uuid4().hex[:8]}"
    with connect(autocommit=True) as conn:
        conn.execute(f"create table {name} (like index.chunks including defaults including constraints)")
        conn.execute(f"alter table {name} add primary key (chunk_id)")
    yield name
    with connect(autocommit=True) as conn:
        conn.execute(f"drop table if exists {name}")
        # Keep the shared metrics clean: these rows come from the test embedder only.
        conn.execute(
            "delete from metrics.reindex_log where batch_id in "
            "(select id from metrics.reindex_batch where embed_model = 'hash-test')"
        )
        conn.execute("delete from metrics.reindex_batch where embed_model = 'hash-test'")


def test_change_reaches_the_index_and_unchanged_replay_is_skipped(scratch_table: str) -> None:
    prefix = f"E2E-{uuid.uuid4().hex[:6]}-"
    ident = prefix + "A"
    group = f"fcp-reindex-test-{uuid.uuid4().hex[:6]}"
    results = {}
    stop = threading.Event()

    def consume() -> None:
        results["totals"] = run(
            seconds=60,
            embedder=HashEmbedder(),
            group_id=group,
            table=scratch_table,
            key_prefix=prefix,
            stop_event=stop,
        )

    worker = threading.Thread(target=consume)
    worker.start()
    try:
        time.sleep(3)
        with connect(autocommit=True) as conn:
            conn.execute(
                "insert into source.airport (ident, iata, name, type, latitude, longitude) "
                "values (%s, 'ZZZ', 'E2E Field', 'small_airport', 1, 2)",
                (ident,),
            )
            conn.execute(
                "update source.airport set name = 'E2E Field Renamed', updated_at = now() where ident = %s",
                (ident,),
            )
            conn.execute(
                "update source.airport set updated_at = now() where ident = %s", (ident,)
            )  # no fact changed
        deadline = time.monotonic() + 30
        row = None
        while time.monotonic() < deadline:
            with connect() as conn:
                row = conn.execute(
                    f"select chunk_text from {scratch_table} where chunk_id = %s",
                    (f"source.airport:{ident}",),
                ).fetchone()
            if row and "Renamed" in row["chunk_text"]:
                break
            time.sleep(1)
        assert row is not None and "E2E Field Renamed" in row["chunk_text"]
    finally:
        with connect(autocommit=True) as conn:
            conn.execute("delete from source.airport where ident = %s", (ident,))
        time.sleep(3)  # let the delete flow through before stopping
        stop.set()
        worker.join()
    totals = results["totals"]
    # c + u (+ u with no fact change) + d: the bookkeeping-only update never costs an embedding.
    assert totals.embedded <= 2
    assert totals.skipped + totals.collapsed >= 1

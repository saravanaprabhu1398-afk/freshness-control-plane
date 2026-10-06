from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fcp.cdc.events import ChangeEvent
from fcp.reindex.chunker import build
from fcp.reindex.consumer import collapse, plan_batch
from fcp.reindex.embedder import HashEmbedder

T = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)


def airport(ident: str, name: str) -> dict[str, Any]:
    return {
        "ident": ident,
        "iata": "ZZZ",
        "name": name,
        "municipality": None,
        "region": None,
        "type": "medium_airport",
        "latitude": 1.0,
        "longitude": 2.0,
        "elevation_ft": None,
    }


def ev(ident: str, op: str, after: dict[str, Any] | None, n: int = 0) -> ChangeEvent:
    return ChangeEvent(
        "source.airport",
        op,
        ident,
        T + timedelta(seconds=n),
        T + timedelta(seconds=n),
        None,  # type: ignore[arg-type]
        None,
        after,
    )


def test_collapse_keeps_latest_event_per_record() -> None:
    events = [
        ev("A", "c", airport("A", "a1"), 0),
        ev("B", "c", airport("B", "b1"), 1),
        ev("A", "u", airport("A", "a2"), 2),
    ]
    survivors = collapse(events)
    assert [(e.key, e.after["name"]) for e in survivors if e.after] == [("B", "b1"), ("A", "a2")]


def test_plan_skips_unchanged_embeds_changed_and_deletes() -> None:
    same = airport("SAME", "Same Field")
    indexed = {"source.airport:SAME": build("source.airport", "SAME", same, T).content_hash}
    events = [
        ev("SAME", "u", same),
        ev("NEW", "c", airport("NEW", "New Field")),
        ev("GONE", "d", None),
        ev("NEW", "u", airport("NEW", "New Field 2")),
    ]
    plan = plan_batch(events, indexed)
    assert [c.record_key for _, c in plan.skips] == ["source.airport:SAME"]
    assert [c.text.split(" (")[0] for _, c in plan.upserts] == ["New Field 2"]  # only the latest version
    assert [e.key for e in plan.deletes] == ["GONE"]
    assert (plan.events, plan.collapsed) == (4, 1)


def test_create_then_delete_in_one_batch_is_a_delete() -> None:
    plan = plan_batch([ev("X", "c", airport("X", "x")), ev("X", "d", None)], {})
    assert not plan.upserts
    assert [e.key for e in plan.deletes] == ["X"]


def test_hash_embedder_is_deterministic_and_normalised() -> None:
    e = HashEmbedder()
    a, b = e.embed_passages(["hello", "hello"])
    assert a == b
    assert len(a) == 384
    assert abs(sum(x * x for x in a) - 1) < 1e-9

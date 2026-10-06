"""The real model: right shape, normalised, and retrieval ranks the relevant passage first."""

from __future__ import annotations

import math

import pytest

from fcp.reindex.embedder import FastEmbedder

pytestmark = pytest.mark.model


def cos(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


def test_bge_small_ranks_relevant_passage_first() -> None:
    e = FastEmbedder()
    passages = [
        "Fare John F. Kennedy International (JFK) to Los Angeles International (LAX) "
        "on DL (Delta Air Lines): $548.50.",
        "Flight UAL247 (aircraft ab819c) is on the ground at Chicago O'Hare International (ORD).",
        "Seattle-Tacoma International Airport (SEA / KSEA), elevation 433 ft.",
    ]
    vectors = e.embed_passages(passages)
    assert all(len(v) == 384 and math.isclose(math.sqrt(cos(v, v)), 1, abs_tol=1e-3) for v in vectors)
    query = e.embed_query("How much does Delta charge from New York to Los Angeles?")
    scores = [cos(query, v) for v in vectors]
    assert scores.index(max(scores)) == 0
    assert e.count_tokens(passages) > 30

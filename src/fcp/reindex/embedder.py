"""Embedding backends (ADR-005). Production: BAAI/bge-small-en-v1.5 via fastembed (ONNX, local).

`HashEmbedder` is a deterministic stand-in for tests: no model download, stable vectors, and
identical texts give identical vectors. It must never be used to build the real index.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Sequence
from functools import lru_cache
from typing import Protocol

from fcp.common.settings import get_settings

DIM = 384


class Embedder(Protocol):
    name: str
    dim: int

    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]: ...
    def embed_query(self, text: str) -> list[float]: ...
    def count_tokens(self, texts: Sequence[str]) -> int: ...


class FastEmbedder:
    name = "BAAI/bge-small-en-v1.5"
    dim = DIM

    def __init__(self) -> None:
        from fastembed import TextEmbedding

        cache = get_settings().data_dir / "models"
        cache.mkdir(parents=True, exist_ok=True)
        self._model = TextEmbedding(self.name, cache_dir=str(cache))
        if self._model.embedding_size != self.dim:
            raise RuntimeError(f"{self.name} returned dim {self._model.embedding_size}, expected {self.dim}")

    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]:
        return [v.tolist() for v in self._model.passage_embed(list(texts))]

    def embed_query(self, text: str) -> list[float]:
        return next(iter(self._model.query_embed([text]))).tolist()  # type: ignore[no-any-return]

    def count_tokens(self, texts: Sequence[str]) -> int:
        return int(self._model.token_count(list(texts))) if texts else 0


class HashEmbedder:
    """Deterministic, model-free vectors for tests (unit-normalised, 384-d)."""

    name = "hash-test"
    dim = DIM

    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._vec(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._vec(text)

    def count_tokens(self, texts: Sequence[str]) -> int:
        return sum(len(t.split()) for t in texts)

    def _vec(self, text: str) -> list[float]:
        raw = b"".join(hashlib.sha256(f"{i}:{text}".encode()).digest() for i in range(DIM * 4 // 32))
        values = [v / 2**31 for v in struct.unpack(f"<{DIM}i", raw)]
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]


@lru_cache(maxsize=1)
def default_embedder() -> Embedder:
    return FastEmbedder()

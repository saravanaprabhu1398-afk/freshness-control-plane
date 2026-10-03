from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from fcp.common.settings import get_settings


def _postgres_available() -> bool:
    try:
        with psycopg.connect(get_settings().pg_dsn, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _postgres_available():
        return
    skip = pytest.mark.skip(reason="Postgres not reachable; run `make up` first")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def db() -> Iterator[psycopg.Connection[Any]]:
    """A connection whose transaction is always rolled back: tests leave no trace."""
    from fcp.common.db import connect

    conn = connect()
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()

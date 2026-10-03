"""Run dbt against the same Iceberg catalog and DuckDB file the loaders use."""

from __future__ import annotations

import os
from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

from fcp.common.settings import REPO_ROOT, Settings, get_settings

DBT_DIR = REPO_ROOT / "dbt"


def dbt_env(settings: Settings | None = None) -> dict[str, str]:
    settings = settings or get_settings()
    settings.warehouse_dir.mkdir(parents=True, exist_ok=True)
    return {
        "FCP_DUCKDB_PATH": str(settings.duckdb_path),
        "FCP_ICEBERG_URI": settings.iceberg_catalog_uri,
        "FCP_ICEBERG_WAREHOUSE": Path(settings.warehouse_dir).resolve().as_uri(),
    }


def run(args: list[str], settings: Settings | None = None) -> dbtRunnerResult:
    """Run a dbt command (e.g. ['build']) with project/profiles dirs and env set."""
    os.environ.update(dbt_env(settings))
    try:
        result = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    finally:
        _release_duckdb()
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {result.exception or 'see dbt output'}")
    return result


def _release_duckdb() -> None:
    """dbt-duckdb keeps its DuckDB handle open at class level after a run. Close it so the same
    process (e.g. `fcp seed`, Dagster) can open the database file again, read-only or otherwise."""
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager

    env = DuckDBConnectionManager._ENV
    if env is not None:
        env.close()  # type: ignore[no-untyped-call]
        DuckDBConnectionManager._ENV = None

"""Iceberg lake (raw.*): immutable history and the ground-truth baseline (ADR-007).

Uses a PyIceberg SQL catalog backed by SQLite, with data files under warehouse/. dbt-duckdb
reads the same catalog through its iceberg plugin.
"""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import BooleanExpression

from fcp.common.settings import Settings, get_settings

CATALOG_NAME = "fcp"
NAMESPACE = "raw"


def catalog(settings: Settings | None = None) -> Catalog:
    settings = settings or get_settings()
    settings.warehouse_dir.mkdir(parents=True, exist_ok=True)
    cat = load_catalog(
        CATALOG_NAME,
        type="sql",
        uri=settings.iceberg_catalog_uri,
        warehouse=Path(settings.warehouse_dir).resolve().as_uri(),
    )
    with suppress(NamespaceAlreadyExistsError):
        cat.create_namespace(NAMESPACE)
    return cat


def replace_partition(
    table_name: str, data: pa.Table, where: BooleanExpression, settings: Settings | None = None
) -> int:
    """Idempotently replace the rows matching `where` with `data` (one Iceberg snapshot).

    Creates the table from the Arrow schema on first write. Re-loading the same month or
    quarter therefore never duplicates rows.
    """
    cat = catalog(settings)
    ident = f"{NAMESPACE}.{table_name}"
    data = data.append_column(
        "_loaded_at", pa.array([datetime.now(UTC)] * data.num_rows, pa.timestamp("us", "UTC"))
    )
    try:
        table = cat.load_table(ident)
    except NoSuchTableError:
        cat.create_table(ident, schema=data.schema).append(data)  # nothing to replace yet
        return int(data.num_rows)
    table.overwrite(data, overwrite_filter=where)
    return int(data.num_rows)

"""Minimal, ordered SQL migrations (db/migrations/NNNN_name.sql).

Why not the Postgres image's init scripts: those run only on an empty data directory, so a
schema change in a later phase would never reach an existing environment. Each migration runs
in its own transaction and is recorded with a checksum; editing an applied file is an error.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from fcp.common.db import connect
from fcp.common.logging import get_logger
from fcp.common.settings import REPO_ROOT, get_settings

MIGRATIONS_DIR = REPO_ROOT / "db" / "migrations"
log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Migration:
    version: str
    name: str
    path: Path

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.path.read_bytes()).hexdigest()


class MigrationError(RuntimeError):
    pass


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found = []
    for path in sorted(directory.glob("*.sql")):
        version, _, name = path.stem.partition("_")
        if not version.isdigit() or not name:
            raise MigrationError(f"bad migration file name {path.name}; expected NNNN_name.sql")
        found.append(Migration(version, name, path))
    versions = [m.version for m in found]
    if len(set(versions)) != len(versions):
        raise MigrationError(f"duplicate migration versions in {directory}")
    return found


def upgrade(directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply pending migrations in order. Returns the versions applied."""
    migrations = discover(directory)
    applied_now: list[str] = []
    with connect(autocommit=True) as conn:
        conn.execute("create schema if not exists ops")
        conn.execute(
            """
            create table if not exists ops.schema_migrations (
              version text primary key, name text not null, checksum char(64) not null,
              applied_at timestamptz not null default now()
            )
            """
        )
        done = {
            r["version"]: r["checksum"]
            for r in conn.execute("select version, checksum from ops.schema_migrations")
        }

        # Environments created by Phase 1 have the 0001 schema from the image's init scripts but
        # no migration history. Adopt it rather than re-running it.
        if not done and migrations and _initial_schema_present(conn):
            first = migrations[0]
            conn.execute(
                "insert into ops.schema_migrations (version, name, checksum) values (%s, %s, %s)",
                (first.version, first.name, first.checksum),
            )
            done[first.version] = first.checksum
            log.info("migrate.adopted", version=first.version)

        for m in migrations:
            if m.version in done:
                if done[m.version] != m.checksum:
                    raise MigrationError(f"migration {m.path.name} was edited after it was applied")
                continue
            with conn.transaction():
                conn.execute(m.path.read_text())  # trusted repo file
                conn.execute(
                    "insert into ops.schema_migrations (version, name, checksum) values (%s, %s, %s)",
                    (m.version, m.name, m.checksum),
                )
            applied_now.append(m.version)
            log.info("migrate.applied", version=m.version, name=m.name)

        _set_cdc_password(conn)
    return applied_now


def _initial_schema_present(conn: psycopg.Connection[Any]) -> bool:
    row = conn.execute("select to_regclass('source.flight_state') is not null as present").fetchone()
    return bool(row and row["present"])


def _set_cdc_password(conn: psycopg.Connection[Any]) -> None:
    """Keep the CDC role's password in sync with FCP_CDC_PASSWORD (no-op before 0002)."""
    exists = conn.execute("select 1 from pg_roles where rolname = 'fcp_cdc'").fetchone()
    if not exists:
        return
    password = get_settings().cdc_password.get_secret_value()
    conn.execute(sql.SQL("alter role fcp_cdc with login password {}").format(sql.Literal(password)))

"""Load freshness contracts from YAML into freshness.contract (single source of truth: the YAML)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from fcp.common.db import connect
from fcp.common.settings import get_settings

MEASURES = frozenset({"last_verified_at", "data_as_of"})
ON_BREACH = frozenset({"flag", "warn"})


@dataclass(frozen=True, slots=True)
class Contract:
    name: str
    measure: str
    max_age: str  # Postgres interval literal, e.g. '15 minutes'
    on_breach: str
    description: str


def parse(doc: dict[str, Any]) -> tuple[int, list[Contract]]:
    version = int(doc["version"])
    contracts: list[Contract] = []
    seen: set[str] = set()
    for c in doc["contracts"]:
        if c["name"] in seen:
            raise ValueError(f"duplicate contract {c['name']}")
        if c["measure"] not in MEASURES:
            raise ValueError(f"{c['name']}: measure must be one of {sorted(MEASURES)}")
        if c["on_breach"] not in ON_BREACH:
            raise ValueError(f"{c['name']}: on_breach must be one of {sorted(ON_BREACH)}")
        seen.add(c["name"])
        contracts.append(
            Contract(
                c["name"],
                c["measure"],
                str(c["max_age"]),
                c["on_breach"],
                " ".join(str(c.get("description", "")).split()),
            )
        )
    return version, contracts


def sync(path: Path | None = None) -> list[Contract]:
    path = path or get_settings().contracts_file
    version, contracts = parse(yaml.safe_load(path.read_text()))
    with connect() as conn:
        conn.execute(
            "delete from freshness.contract where not (name = any(%s))", ([c.name for c in contracts],)
        )
        for c in contracts:
            conn.execute(
                """
                insert into freshness.contract (name, measure, max_age, on_breach, description, version)
                values (%s, %s, %s::interval, %s, %s, %s)
                on conflict (name) do update set measure = excluded.measure, max_age = excluded.max_age,
                  on_breach = excluded.on_breach, description = excluded.description,
                  version = excluded.version, loaded_at = now()
                """,
                (c.name, c.measure, c.max_age, c.on_breach, c.description, version),
            )
    return contracts

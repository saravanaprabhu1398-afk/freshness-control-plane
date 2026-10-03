"""Kafka Connect REST client for the Debezium source connector (idempotent registration)."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from fcp.common.logging import get_logger
from fcp.common.settings import REPO_ROOT, get_settings

CONNECTOR_NAME = "fcp-source"
CONFIG_FILE = REPO_ROOT / "cdc" / "debezium" / "fcp-source.json"
log = get_logger(__name__)


def connector_config() -> dict[str, str]:
    config: dict[str, str] = json.loads(CONFIG_FILE.read_text())
    config["database.dbname"] = get_settings().pg_db
    return config


def _client() -> httpx.Client:
    return httpx.Client(base_url=get_settings().connect_url, timeout=30)


def is_healthy(state: dict[str, Any]) -> bool:
    tasks = state.get("tasks", [])
    return (
        state.get("connector", {}).get("state") == "RUNNING"
        and bool(tasks)
        and all(t["state"] == "RUNNING" for t in tasks)
    )


def ensure_running(*, wait_s: float = 60) -> dict[str, Any]:
    """Self-healing entry point (Dagster sensor): register if missing, restart failed tasks."""
    state = status()
    if is_healthy(state):
        return state
    if state["connector"]["state"] != "NOT_REGISTERED" and any(
        t["state"] == "FAILED" for t in state["tasks"]
    ):
        with _client() as c:
            c.post(
                f"/connectors/{CONNECTOR_NAME}/restart", params={"includeTasks": "true", "onlyFailed": "true"}
            )
        log.warning("cdc.connector.restarted_failed_tasks")
    return register(wait_s=wait_s)


def register(*, wait_s: float = 60) -> dict[str, Any]:
    """Create or update the connector (PUT is idempotent), then wait until it is RUNNING."""
    with _client() as c:
        response = c.put(f"/connectors/{CONNECTOR_NAME}/config", json=connector_config())
        response.raise_for_status()
        log.info("cdc.connector.configured", name=CONNECTOR_NAME, created=response.status_code == 201)
        deadline = time.monotonic() + wait_s
        while True:
            state = status(c)
            if is_healthy(state):
                return state
            failed = [t for t in state.get("tasks", []) if t["state"] == "FAILED"]
            if failed:
                raise RuntimeError(f"connector task failed: {failed[0].get('trace', '')[:2000]}")
            if time.monotonic() > deadline:
                raise TimeoutError(f"connector not RUNNING after {wait_s}s: {state}")
            time.sleep(1)


def status(client: httpx.Client | None = None) -> dict[str, Any]:
    own = client is None
    c = client or _client()
    try:
        response = c.get(f"/connectors/{CONNECTOR_NAME}/status")
        if response.status_code == 404:
            return {"connector": {"state": "NOT_REGISTERED"}, "tasks": []}
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        return result
    finally:
        if own:
            c.close()

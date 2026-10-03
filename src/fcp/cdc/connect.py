"""Kafka Connect REST client for the Debezium source connector (idempotent registration)."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
from confluent_kafka import KafkaError, KafkaException

# NewTopic is documented under confluent_kafka.admin but its stubs do not re-export it.
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from fcp.cdc.events import topics
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


def ensure_topics() -> list[str]:
    """Create the source topics up front, with the settings from the connector config.

    Debezium would create them on the first change, but on an empty database that means a
    consumer started before any change subscribes to topics that do not exist yet and can miss
    the first events until its metadata refreshes. Explicit topics also make their settings
    deterministic. Returns the topics created now (existing ones are left as they are).
    """
    cfg = connector_config()
    admin = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap})
    wanted = [
        NewTopic(
            t,
            num_partitions=int(cfg["topic.creation.default.partitions"]),
            replication_factor=int(cfg["topic.creation.default.replication.factor"]),
            config={
                "cleanup.policy": cfg["topic.creation.default.cleanup.policy"],
                "retention.ms": cfg["topic.creation.default.retention.ms"],
            },
        )
        for t in topics(cfg["topic.prefix"])
    ]
    created = []
    for name, future in admin.create_topics(wanted, request_timeout=30).items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    if created:
        log.info("cdc.topics.created", topics=created)
    return created


def register(*, wait_s: float = 60) -> dict[str, Any]:
    """Create topics, create or update the connector (PUT is idempotent), wait until RUNNING."""
    ensure_topics()
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

SHELL := /bin/bash
export DAGSTER_HOME := $(CURDIR)/.dagster

.PHONY: help install up up-app down cdc seed poll drift tail reindex savings dagster status test test-unit lint typecheck check diagrams

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install:  ## Install Python 3.12 deps with uv (all extras: pipeline + dev)
	uv sync --all-extras

up:  ## Start Postgres, Kafka and Debezium Connect; apply database migrations
	docker compose up -d --wait
	uv run fcp db upgrade

up-app: cdc  ## Also run the re-index consumer as a container (builds the app image)
	docker compose --profile app up -d --build --wait reindex

cdc: up  ## Register (or update) the Debezium connector and wait until it is RUNNING
	uv run fcp cdc register

down:  ## Stop the stack, including the app profile (data volumes are kept)
	docker compose --profile app down

seed: cdc  ## Load the real-data baseline: contracts, airports, BTS, DB1B, dbt marts, fares
	uv run fcp seed

poll: up  ## Run one live OpenSky poll
	uv run fcp ingest opensky-states

drift: up  ## Apply the current fare-drift tick (SIMULATED; idempotent per tick)
	uv run fcp drift tick

tail: ## Watch change events (record id, commit time, delta) for 60 s
	uv run fcp cdc tail --seconds 60

reindex: cdc  ## Run the selective re-index consumer in the foreground (Ctrl-C to stop)
	uv run fcp reindex run

savings: ## Index coverage, consumer lag, and savings vs naive baselines (last 24 h)
	uv run fcp reindex status
	uv run fcp reindex report --hours 24

dagster: up  ## Start Dagster (UI on http://localhost:3000) with schedules
	@mkdir -p $(DAGSTER_HOME)
	uv run dagster dev -m fcp.orchestration.definitions

status:  ## Show loaded data, freshness and API budget
	uv run fcp status

test: cdc  ## All tests (unit + integration against the local stack, including CDC end to end)
	uv run pytest

test-unit:  ## Unit tests only (no services, no model download)
	uv run pytest -m "not integration and not model"

lint:  ## Ruff lint + format check
	uv run ruff check src tests
	uv run ruff format --check src tests

typecheck:  ## mypy --strict
	uv run mypy

check: lint typecheck test  ## Everything CI runs

diagrams:  ## Regenerate architecture.svg and validate Mermaid sources
	python3 docs/diagrams/src/architecture.py
	@for f in docs/diagrams/mermaid/*.mmd; do \
		npx -y -p @mermaid-js/mermaid-cli@11 mmdc -q -i $$f -o /tmp/fcp-$$(basename $$f .mmd).svg || exit 1; \
		echo "ok  $$f"; \
	done

SHELL := /bin/bash
export DAGSTER_HOME := $(CURDIR)/.dagster

.PHONY: help install up down seed poll dagster status test test-unit lint typecheck check diagrams

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install:  ## Install Python 3.12 deps with uv
	uv sync

up:  ## Start Postgres, Kafka and Debezium Connect; wait until healthy
	docker compose up -d --wait

down:  ## Stop the stack (data volumes are kept)
	docker compose down

seed: up  ## Load the real-data baseline: contracts, airports, BTS, DB1B, dbt marts, fares
	uv run fcp seed

poll: up  ## Run one live OpenSky poll
	uv run fcp ingest opensky-states

dagster: up  ## Start Dagster (UI on http://localhost:3000) with schedules
	@mkdir -p $(DAGSTER_HOME)
	uv run dagster dev -m fcp.orchestration.definitions

status:  ## Show loaded data, freshness and API budget
	uv run fcp status

test: up  ## All tests (unit + integration against local Postgres)
	uv run pytest

test-unit:  ## Unit tests only (no services needed)
	uv run pytest -m "not integration"

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

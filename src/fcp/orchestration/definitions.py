"""Dagster code location: assets, jobs and schedules for Phase 1 (`uv run dagster dev`).

Live polling cadence follows the API budget (NFR-3): every 5 minutes with OpenSky
credentials (4 credits x 288 = 1,152 of 3,200 allowed), every 20 minutes anonymously
(4 x 72 = 288 of 320). Anonymous mode therefore cannot meet the 15-minute flight_status
SLA, and the registry will show it. That is intended: the SLA describes the product need, not
what the free tier allows.
"""

import os
from collections.abc import Iterator
from typing import Any

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from fcp.common.db import RunStats
from fcp.common.logging import configure
from fcp.common.settings import get_settings
from fcp.transform.dbt import DBT_DIR, dbt_env

configure()
settings = get_settings()
os.environ.update(dbt_env(settings))  # dbt profile reads these at parse and run time

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()


def _result(stats: RunStats) -> dg.MaterializeResult[Any]:
    return dg.MaterializeResult(
        metadata={
            "rows_seen": stats.rows_seen,
            "rows_changed": stats.rows_changed,
            "rows_unchanged": stats.rows_unchanged,
            "credits_used": stats.credits_used,
            "status": stats.status,
        }
    )


# ----------------------------------------------------------------------------- reference & lake


@dg.asset(group_name="freshness", description="Load contracts/freshness_sla.yaml into freshness.contract.")
def freshness_contracts() -> dg.MaterializeResult[Any]:
    from fcp.freshness import contracts

    loaded = contracts.sync()
    return dg.MaterializeResult(metadata={"contracts": [c.name for c in loaded]})


@dg.asset(group_name="reference", key=["source", "airport"], description="OurAirports -> source.airport.")
def airports() -> dg.MaterializeResult[Any]:
    from fcp.ingestion import ourairports

    return _result(ourairports.load())


@dg.asset(group_name="lake", key=["raw", "bts_ontime"], description="BTS on-time (latest months) -> Iceberg.")
def raw_bts_ontime() -> dg.MaterializeResult[Any]:
    from fcp.ingestion.bts import ontime

    written = ontime.load()
    return dg.MaterializeResult(metadata={"months": written, "rows": sum(written.values())})


@dg.asset(group_name="lake", key=["raw", "db1b_market"], description="BTS DB1B (latest quarter) -> Iceberg.")
def raw_db1b_market() -> dg.MaterializeResult[Any]:
    from fcp.ingestion.bts import db1b

    latest = db1b.load()
    return dg.MaterializeResult(metadata={"quarter": f"{latest[0]}-Q{latest[1]}" if latest else "none"})


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    yield from dbt.cli(["build"], context=context).stream()


@dg.asset(
    group_name="reference",
    key=["source", "fare"],
    deps=[dg.AssetKey(["marts", "mart_fare_baseline"])],
    description="Seed source.fare from the real DB1B baseline (never overwrites same-quarter drift).",
)
def fare_baseline() -> dg.MaterializeResult[Any]:
    from fcp.ingestion.fares import baseline

    return _result(baseline.seed())


# ----------------------------------------------------------------------------- live


@dg.asset(
    group_name="live",
    key=["source", "flight_state"],
    description="OpenSky live states -> source.flight_state.",
)
def opensky_states() -> dg.MaterializeResult[Any]:
    from fcp.ingestion.opensky import poller

    return _result(poller.poll_states())


@dg.asset(
    group_name="live",
    key=["source", "flight_state_batch"],
    deps=[dg.AssetKey(["source", "flight_state"])],
    description="OpenSky completed flights for yesterday (needs credentials) -> source.flight_state.",
)
def opensky_flights() -> dg.MaterializeResult[Any]:
    from fcp.ingestion.opensky import poller

    if not settings.opensky_authenticated:
        return dg.MaterializeResult(metadata={"status": "skipped: no OpenSky credentials"})
    return _result(poller.poll_flights())


# ----------------------------------------------------------------------------- jobs & schedules

baseline_job = dg.define_asset_job(
    "baseline",
    selection=dg.AssetSelection.groups("freshness", "reference", "lake")
    | dg.AssetSelection.assets(dbt_models),
    description="Contracts, reference data, BTS lake loads, dbt marts and the fare baseline.",
)
live_states_job = dg.define_asset_job("live_states", selection=[opensky_states])
flights_job = dg.define_asset_job("opensky_flights", selection=[opensky_flights])

STATES_CRON = "*/5 * * * *" if settings.opensky_authenticated else "*/20 * * * *"

defs = dg.Definitions(
    assets=[
        freshness_contracts,
        airports,
        raw_bts_ontime,
        raw_db1b_market,
        dbt_models,
        fare_baseline,
        opensky_states,
        opensky_flights,
    ],
    jobs=[baseline_job, live_states_job, flights_job],
    schedules=[
        dg.ScheduleDefinition(job=live_states_job, cron_schedule=STATES_CRON, execution_timezone="UTC"),
        dg.ScheduleDefinition(job=flights_job, cron_schedule="0 6 * * *", execution_timezone="UTC"),
        # BTS publishes monthly; checking weekly picks up a new month within days at no API cost.
        dg.ScheduleDefinition(job=baseline_job, cron_schedule="0 3 * * 1", execution_timezone="UTC"),
    ],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)

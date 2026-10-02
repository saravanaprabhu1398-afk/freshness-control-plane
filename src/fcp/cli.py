"""`fcp` command-line entry point. Every pipeline step can run without Dagster.

fcp contracts sync
fcp ingest {airports,opensky-states,opensky-flights,bts-ontime,db1b,fare-baseline}
fcp dbt build
fcp seed            # contracts + all baseline loads + dbt build + fare seed
fcp status          # what is loaded, how fresh it is, budget used today
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from typing import Any

from fcp.common.logging import configure, get_logger

log = get_logger("fcp.cli")


def _ingest(step: str, months: int | None) -> Any:
    if step == "airports":
        from fcp.ingestion import ourairports

        return ourairports.load()
    if step == "opensky-states":
        from fcp.ingestion.opensky import poller

        return poller.poll_states()
    if step == "opensky-flights":
        from fcp.ingestion.opensky import poller

        return poller.poll_flights()
    if step == "bts-ontime":
        from fcp.ingestion.bts import ontime

        return ontime.load(months=months)
    if step == "db1b":
        from fcp.ingestion.bts import db1b

        return db1b.load()
    if step == "fare-baseline":
        from fcp.ingestion.fares import baseline

        return baseline.seed()
    raise ValueError(step)


def _seed(months: int | None) -> None:
    from fcp.freshness import contracts
    from fcp.transform import dbt

    steps: list[tuple[str, Callable[[], Any]]] = [
        ("contracts", contracts.sync),
        ("airports", lambda: _ingest("airports", None)),
        ("bts-ontime", lambda: _ingest("bts-ontime", months)),
        ("db1b", lambda: _ingest("db1b", None)),
        ("dbt build", lambda: dbt.run(["build"])),
        ("fare-baseline", lambda: _ingest("fare-baseline", None)),
    ]
    for name, fn in steps:
        log.info("seed.step", step=name)
        fn()
    log.info("seed.done")


def _status() -> None:
    from fcp.common.db import connect

    queries = {
        "Source tables": """
            select 'source.flight_state' as table, count(*) as rows, max(updated_at) as last_change
              from source.flight_state
            union all select 'source.fare', count(*), max(updated_at) from source.fare
            union all select 'source.airport', count(*), max(updated_at) from source.airport
        """,
        "Freshness by contract": """
            select r.contract, count(*) as records, max(r.last_verified_at) as last_verified,
                   min(r.data_as_of) as oldest_data, c.max_age, c.measure
            from freshness.registry r left join freshness.contract c on c.name = r.contract
            group by r.contract, c.max_age, c.measure order by r.contract
        """,
        "API budget today": """
            select provider, endpoint_group, credits_used, credit_limit from ops.api_budget
            where day = (now() at time zone 'utc')::date order by endpoint_group
        """,
        "Latest runs": """
            select distinct on (job) job, status, started_at, rows_seen, rows_changed, rows_unchanged,
                   credits_used
            from metrics.ingest_run order by job, started_at desc
        """,
    }
    with connect() as conn:
        for title, q in queries.items():
            rows = conn.execute(q).fetchall()
            print(f"\n{title}")
            if not rows:
                print("  (none)")
                continue
            cols = list(rows[0].keys())
            widths = [max(len(c), *(len(_fmt(r[c])) for r in rows)) for c in cols]
            print("  " + "  ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
            for r in rows:
                print("  " + "  ".join(_fmt(r[c]).ljust(w) for c, w in zip(cols, widths, strict=True)))


def _fmt(v: Any) -> str:
    if v is None:
        return "-"
    if hasattr(v, "isoformat"):
        return str(v.isoformat(timespec="seconds") if hasattr(v, "hour") else v.isoformat())
    return str(v)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fcp", description="Freshness Control Plane")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("contracts", help="freshness contracts")
    c.add_argument("action", choices=["sync"])

    i = sub.add_parser("ingest", help="run one ingestion step")
    i.add_argument(
        "step",
        choices=["airports", "opensky-states", "opensky-flights", "bts-ontime", "db1b", "fare-baseline"],
    )
    i.add_argument("--months", type=int, help="bts-ontime: number of months to load")

    d = sub.add_parser("dbt", help="run dbt with the project's environment")
    d.add_argument("args", nargs=argparse.REMAINDER)

    s = sub.add_parser("seed", help="load the full real-data baseline")
    s.add_argument("--months", type=int, help="BTS on-time months to load")

    sub.add_parser("status", help="show loaded data, freshness and budget")

    args = parser.parse_args(argv)
    configure(args.log_level)

    if args.cmd == "contracts":
        from fcp.freshness import contracts

        loaded = contracts.sync()
        log.info("contracts.synced", contracts=[x.name for x in loaded])
    elif args.cmd == "ingest":
        result = _ingest(args.step, args.months)
        log.info("ingest.done", step=args.step, result=str(result))
    elif args.cmd == "dbt":
        from fcp.transform import dbt

        dbt.run(args.args or ["build"])
    elif args.cmd == "seed":
        _seed(args.months)
    elif args.cmd == "status":
        _status()
    return 0


if __name__ == "__main__":
    sys.exit(main())

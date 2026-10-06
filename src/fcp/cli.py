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
    from fcp.common import migrate
    from fcp.freshness import contracts
    from fcp.transform import dbt

    steps: list[tuple[str, Callable[[], Any]]] = [
        ("db upgrade", migrate.upgrade),
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

    dbp = sub.add_parser("db", help="database schema")
    dbp.add_argument("action", choices=["upgrade"])

    cdc = sub.add_parser("cdc", help="Debezium connector and change events")
    cdc.add_argument("action", choices=["register", "status", "tail", "stats"])
    cdc.add_argument("--seconds", type=float, default=60, help="tail/stats: how long to listen")
    cdc.add_argument("--limit", type=int, help="tail: stop after N events")
    cdc.add_argument("--table", action="append", help="tail: only this table, e.g. source.fare")
    cdc.add_argument("--from-beginning", action="store_true", help="tail: replay retained events")

    rx = sub.add_parser("reindex", help="selective re-index consumer and savings")
    rx.add_argument("action", choices=["run", "benchmark", "report", "status"])
    rx.add_argument("--seconds", type=float, help="run: stop after N seconds (default: until Ctrl-C)")
    rx.add_argument("--apply", action="store_true", help="benchmark: also rebuild the index from source")
    rx.add_argument("--hours", type=float, default=24, help="report: window size")
    rx.add_argument("--since", help="report: ISO timestamp to measure from (overrides --hours)")

    dr = sub.add_parser("drift", help="fare-drift simulator (SIMULATED data)")
    dr.add_argument("action", choices=["tick", "shock"])
    dr.add_argument("--fraction", type=float, default=0.2, help="shock: share of fares to move")
    dr.add_argument("--multiplier", type=float, default=1.25, help="shock: price multiplier")

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
    elif args.cmd == "db":
        from fcp.common import migrate

        log.info("db.upgraded", applied=migrate.upgrade())
    elif args.cmd == "cdc":
        _cdc(args)
    elif args.cmd == "reindex":
        _reindex(args)
    elif args.cmd == "drift":
        from fcp.ingestion.fares import drift

        if args.action == "tick":
            stats = drift.run_tick()
        else:
            stats = drift.run_shock(fraction=args.fraction, multiplier=args.multiplier)
        log.info(
            "drift.done",
            action=args.action,
            changed=stats.rows_changed,
            unchanged=stats.rows_unchanged,
            detail=stats.detail,
        )
    return 0


def _reindex(args: argparse.Namespace) -> None:
    from datetime import timedelta

    from fcp.reindex import consumer, rebuild, report

    if args.action == "run":
        totals = consumer.run(seconds=args.seconds)
        log.info(
            "reindex.done",
            embedded=totals.embedded,
            skipped=totals.skipped,
            deleted=totals.deleted,
            collapsed=totals.collapsed,
        )
    elif args.action == "benchmark":
        r = rebuild.benchmark(apply=args.apply)
        print(
            f"full re-embed: {r.chunks:,} chunks, {r.tokens:,} tokens, {r.embed_ms / 1000:.1f} s "
            f"({r.embed_ms / max(r.chunks, 1):.2f} ms/chunk){'  [index rebuilt]' if r.applied else ''}"
        )
        for table, n in sorted(r.by_table.items()):
            print(f"  {table:<22} {n:>7,}")
    elif args.action == "report":
        from datetime import datetime

        since = datetime.fromisoformat(args.since) if args.since else None
        print(report.format_report(report.compute(timedelta(hours=args.hours), since=since)))
    else:
        print(f"{'table':<22} {'source':>8} {'chunks':>8} {'missing':>8} {'behind':>8} {'orphans':>8}")
        for c in report.coverage():
            print(
                f"{c.table:<22} {c.source_rows:>8,} {c.chunks:>8,} {c.missing:>8,} "
                f"{c.behind:>8,} {c.orphans:>8,}"
            )
        lag = report.consumer_lag(consumer.GROUP_ID)
        print("consumer lag (messages): " + ", ".join(f"{t.split('.')[-1]}={n:,}" for t, n in lag.items()))


def _cdc(args: argparse.Namespace) -> None:
    from fcp.cdc import connect as cdc_connect
    from fcp.cdc import consumer

    if args.action == "register":
        state = cdc_connect.register()
        log.info(
            "cdc.registered",
            connector=state["connector"]["state"],
            tasks=[t["state"] for t in state["tasks"]],
        )
    elif args.action == "status":
        state = cdc_connect.status()
        print(
            f"connector: {state['connector']['state']}  tasks: {[t['state'] for t in state.get('tasks', [])]}"
        )
    elif args.action == "tail":
        c = consumer.make_consumer(from_beginning=args.from_beginning)
        for r in consumer.iter_events(c, seconds=args.seconds, limit=args.limit, tables=args.table):
            print(consumer.format_event(r), flush=True)
    else:
        results = consumer.measure(args.seconds)
        print(f"{'table':<20} {'op':<3} {'events':>6} {'capture p50/p95 ms':>20} {'delivery p50/p95 ms':>21}")
        for w in results:
            print(
                f"{w.table:<20} {w.op:<3} {w.events:>6} {f'{w.p50_capture_ms}/{w.p95_capture_ms}':>20} "
                f"{f'{w.p50_delivery_ms}/{w.p95_delivery_ms}':>21}"
            )


if __name__ == "__main__":
    sys.exit(main())

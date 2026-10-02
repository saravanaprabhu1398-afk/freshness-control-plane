"""Seed source.fare from the dbt fare baseline (real DB1B medians, not simulated).

Each route x carrier gets one current fare equal to its DB1B median. From Phase 2 the drift
simulator moves `fare_usd` away from `baseline_usd`; this seed never overwrites a simulated
fare that is newer than the baseline it came from.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb

from fcp.common.db import FreshnessEntry, RunStats, connect, record_freshness, track_run, upsert_changed
from fcp.common.settings import Settings, get_settings
from fcp.ingestion.bts.db1b import quarter_end

TABLE = "source.fare"
CONTRACT = "fare"
BUSINESS_COLS = (
    "fare_usd",
    "baseline_usd",
    "baseline_period",
    "sample_size",
    "effective_at",
    "source",
    "simulated",
)
# Keep a drifted (simulated) fare if it is based on the same baseline quarter; replace it when
# a newer DB1B quarter arrives.
KEEP_DRIFT = "t.baseline_period = excluded.baseline_period and t.simulated"
OVERRIDES = {
    c: f"case when {KEEP_DRIFT} then t.{c} else excluded.{c} end"
    for c in ("fare_usd", "effective_at", "source", "simulated")
}


def read_baseline(settings: Settings) -> list[dict[str, Any]]:
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        rows = con.execute(
            """
            select fare_key, origin, dest, carrier, cabin, median_fare_usd, period, year, quarter, sample_size
            from marts.mart_fare_baseline order by fare_key
            """
        ).fetchall()
    finally:
        con.close()
    out = []
    for fare_key, origin, dest, carrier, cabin, median, period, year, quarter, n in rows:
        out.append(
            {
                "fare_key": fare_key,
                "origin": origin,
                "dest": dest,
                "carrier": carrier,
                "cabin": cabin,
                "fare_usd": median,
                "baseline_usd": median,
                "baseline_period": period,
                "sample_size": int(n),
                "effective_at": quarter_end(int(year), int(quarter)),
                "source": "bts_db1b",
                "simulated": False,
            }
        )
    return out


def seed(settings: Settings | None = None) -> RunStats:
    settings = settings or get_settings()
    with track_run("fare_baseline_seed") as stats:
        rows = read_baseline(settings)
        now = datetime.now(UTC)
        with connect() as conn:
            result = upsert_changed(
                conn,
                TABLE,
                rows,
                key="fare_key",
                business_cols=BUSINESS_COLS,
                insert_only_cols=("origin", "dest", "carrier", "cabin"),
                overrides=OVERRIDES,
            )
            record_freshness(
                conn,
                [FreshnessEntry(f"{TABLE}:{r['fare_key']}", CONTRACT, r["effective_at"]) for r in rows],
                changed_keys={f"{TABLE}:{k}" for k in result.changed},
                verified_at=now,
            )
        stats.add(result)
        stats.detail["baseline_period"] = rows[0]["baseline_period"] if rows else None
    return stats

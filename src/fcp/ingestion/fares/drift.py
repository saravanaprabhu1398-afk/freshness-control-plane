"""Fare-drift simulator (PRD FR-2, ADR-002). SIMULATED data, always labelled as such.

Why simulate: there is no free live-fare API (Amadeus Self-Service shut down 2026-07-17), and
DB1B gives a real but historical baseline. This module moves current fares away from that
baseline in a way that is realistic enough to exercise CDC and selective re-indexing:

* Airlines reprice in discrete steps, not continuously. Each fare is repriced on a tick with
  probability `p_reprice`; most fares do not change on a given tick.
* A repriced fare follows a mean-reverting random walk in log space around its baseline
  (an Ornstein-Uhlenbeck step): x' = x + theta * (0 - x) + sigma * N(0, 1), x = ln(fare / baseline).
* Rare shocks model sales and surges: with probability `p_shock` a fare moves by +/-15-40%.
* Fares are whole dollars and stay within [0.4, 2.5] x baseline and the $25-$2,500 load bounds.

No seasonality is modelled: one DB1B quarter cannot support a seasonal claim, so we do not make one.

Reproducible: the random stream for a tick depends only on (seed, tick index, fare), so the same
sequence of ticks from the same starting prices always produces the same prices. Ticks are
idempotent (ops.drift_tick): re-running an applied tick changes nothing.
"""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg

from fcp.common.db import FreshnessEntry, RunStats, connect, record_freshness, track_run, update_changed
from fcp.common.settings import Settings, get_settings

TABLE = "source.fare"
CONTRACT = "fare"
BUSINESS_COLS = ("fare_usd", "effective_at", "source", "simulated")
MIN_FARE, MAX_FARE = 25, 2500
MIN_RATIO, MAX_RATIO = 0.4, 2.5


@dataclass(frozen=True, slots=True)
class DriftParams:
    p_reprice: float = 0.04  # per fare per tick (~3.8 reprices per fare per day at 15-min ticks)
    theta: float = 0.2  # mean-reversion strength per reprice
    sigma: float = 0.06  # log-price volatility per reprice
    p_shock: float = 0.002  # per fare per tick
    shock_min: float = 0.15
    shock_max: float = 0.40


DEFAULT_PARAMS = DriftParams()


@dataclass(frozen=True, slots=True)
class Fare:
    fare_key: str
    fare_usd: float
    baseline_usd: float


@dataclass(frozen=True, slots=True)
class Reprice:
    fare_key: str
    old_usd: float
    new_usd: float
    shock: bool


def tick_index(now: datetime, tick_minutes: int) -> int:
    return int(now.timestamp()) // (tick_minutes * 60)


def _bounded(new: float, baseline: float) -> float:
    lo = max(MIN_FARE, MIN_RATIO * baseline)
    hi = min(MAX_FARE, MAX_RATIO * baseline)
    return float(round(min(max(new, lo), hi)))


def simulate_tick(
    fares: Sequence[Fare], *, seed: int, tick: int, params: DriftParams = DEFAULT_PARAMS
) -> list[Reprice]:
    """Pure function: which fares change on this tick, and to what. Order-independent."""
    out: list[Reprice] = []
    for f in sorted(fares, key=lambda x: x.fare_key):
        # Per-fare stream: adding or removing a route does not change other routes' prices.
        rng = random.Random(f"{seed}:{tick}:{f.fare_key}")  # noqa: S311 (simulation, not crypto)
        reprice = rng.random() < params.p_reprice
        shock = rng.random() < params.p_shock
        if not (reprice or shock):
            continue
        x = math.log(f.fare_usd / f.baseline_usd)
        if reprice:
            x += params.theta * (0.0 - x) + params.sigma * rng.gauss(0.0, 1.0)
        if shock:
            magnitude = rng.uniform(params.shock_min, params.shock_max)
            x += math.log1p(magnitude if rng.random() < 0.5 else -magnitude)
        new = _bounded(f.baseline_usd * math.exp(x), f.baseline_usd)
        if new != f.fare_usd:
            out.append(Reprice(f.fare_key, f.fare_usd, new, shock))
    return out


def apply_shock(fares: Sequence[Fare], *, fraction: float, multiplier: float, seed: int) -> list[Reprice]:
    """Deterministic drift event for evaluation: move `fraction` of fares by `multiplier`."""
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    rng = random.Random(f"shock:{seed}")  # noqa: S311 (simulation, not crypto)
    chosen = rng.sample(sorted(fares, key=lambda f: f.fare_key), max(1, round(len(fares) * fraction)))
    out = []
    for f in chosen:
        new = _bounded(f.fare_usd * multiplier, f.baseline_usd)
        if new != f.fare_usd:
            out.append(Reprice(f.fare_key, f.fare_usd, new, True))
    return out


def _load(conn: psycopg.Connection[Any]) -> list[Fare]:
    rows = conn.execute(
        "select fare_key, fare_usd, baseline_usd from source.fare order by fare_key"
    ).fetchall()
    return [Fare(r["fare_key"], float(r["fare_usd"]), float(r["baseline_usd"])) for r in rows]


def _write(
    conn: psycopg.Connection[Any],
    fares: Sequence[Fare],
    changes: Sequence[Reprice],
    now: datetime,
    stats: RunStats,
) -> None:
    rows = [
        {
            "fare_key": c.fare_key,
            "fare_usd": Decimal(f"{c.new_usd:.2f}"),
            "effective_at": now,
            "source": "drift_sim",
            "simulated": True,
        }
        for c in changes
    ]
    result = update_changed(conn, TABLE, rows, key="fare_key", business_cols=BUSINESS_COLS)
    # The simulator is the fare feed: each tick re-asserts every current fare.
    changed = {f"{TABLE}:{k}" for k in result.changed}
    record_freshness(
        conn,
        [FreshnessEntry(f"{TABLE}:{f.fare_key}", CONTRACT, now) for f in fares],
        changed_keys=changed,
        verified_at=now,
    )
    stats.rows_seen = len(fares)
    stats.rows_changed = len(result.changed)
    stats.rows_unchanged = len(fares) - len(result.changed)
    stats.detail.update(shocks=sum(c.shock for c in changes), simulated=True)


def apply_tick(
    conn: psycopg.Connection[Any],
    stats: RunStats,
    *,
    seed: int,
    tick: int,
    now: datetime,
    params: DriftParams = DEFAULT_PARAMS,
) -> bool:
    """Apply one tick inside the caller's transaction. Returns False if it was already applied."""
    stats.detail["tick"] = tick
    claimed = conn.execute(
        "insert into ops.drift_tick (tick, fares_changed) values (%s, 0) "
        "on conflict do nothing returning tick",
        (tick,),
    ).fetchone()
    if claimed is None:
        stats.status = "skipped"
        stats.detail["reason"] = "tick already applied"
        return False
    fares = _load(conn)
    changes = simulate_tick(fares, seed=seed, tick=tick, params=params)
    _write(conn, fares, changes, now, stats)
    conn.execute("update ops.drift_tick set fares_changed = %s where tick = %s", (stats.rows_changed, tick))
    return True


def run_tick(
    settings: Settings | None = None, *, now: datetime | None = None, params: DriftParams = DEFAULT_PARAMS
) -> RunStats:
    """Apply the current tick. Idempotent: a tick that was already applied is skipped."""
    settings = settings or get_settings()
    now = now or datetime.now(UTC)
    with track_run("fare_drift") as stats, connect() as conn:
        apply_tick(
            conn,
            stats,
            seed=settings.drift_seed,
            tick=tick_index(now, settings.drift_tick_minutes),
            now=now,
            params=params,
        )
    return stats


def run_shock(*, fraction: float, multiplier: float, seed: int | None = None) -> RunStats:
    settings = get_settings()
    now = datetime.now(UTC)
    with track_run("fare_shock") as stats, connect() as conn:
        fares = _load(conn)
        changes = apply_shock(
            fares,
            fraction=fraction,
            multiplier=multiplier,
            seed=settings.drift_seed if seed is None else seed,
        )
        _write(conn, fares, changes, now, stats)
        stats.detail.update(fraction=fraction, multiplier=multiplier)
    return stats

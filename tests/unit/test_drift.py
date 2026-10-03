from __future__ import annotations

import math
import statistics

import pytest

from fcp.ingestion.fares.drift import DriftParams, Fare, apply_shock, simulate_tick

FARES = [Fare(f"R{i:03d}:X:YY:ALL", 300.0, 300.0) for i in range(200)]


def test_same_seed_and_tick_give_same_reprices() -> None:
    assert simulate_tick(FARES, seed=1, tick=10) == simulate_tick(list(reversed(FARES)), seed=1, tick=10)
    assert simulate_tick(FARES, seed=1, tick=10) != simulate_tick(FARES, seed=2, tick=10)


def test_a_fare_price_does_not_depend_on_other_fares() -> None:
    full = {r.fare_key: r for r in simulate_tick(FARES, seed=7, tick=3)}
    alone = {r.fare_key: r for r in simulate_tick(FARES[:50], seed=7, tick=3)}
    assert {k: v for k, v in full.items() if k in alone} == alone


def test_only_a_small_share_of_fares_reprice_per_tick() -> None:
    params = DriftParams()
    changed = [len(simulate_tick(FARES, seed=5, tick=t, params=params)) / len(FARES) for t in range(200)]
    # Expected share ~ p_reprice + p_shock (minus repricings that round to the same dollar).
    assert statistics.mean(changed) == pytest.approx(params.p_reprice + params.p_shock, abs=0.01)


def test_prices_are_whole_dollars_within_bounds() -> None:
    cheap = [Fare(f"C{i}", 30.0, 30.0) for i in range(300)]
    for tick in range(50):
        for r in simulate_tick(cheap, seed=9, tick=tick, params=DriftParams(p_reprice=1, sigma=1.0)):
            assert r.new_usd == round(r.new_usd)
            assert 25 <= r.new_usd <= 75  # max(25, 0.4 x 30) .. min(2500, 2.5 x 30)


def test_walk_mean_reverts_to_baseline() -> None:
    fare = Fare("MR", 300.0, 300.0)
    path = []
    for tick in range(3000):
        for r in simulate_tick([fare], seed=11, tick=tick, params=DriftParams(p_reprice=1, p_shock=0)):
            fare = Fare(fare.fare_key, r.new_usd, fare.baseline_usd)
        path.append(math.log(fare.fare_usd / fare.baseline_usd))
    assert abs(statistics.mean(path)) < 0.05  # centred on the baseline
    assert statistics.pstdev(path) < 0.2  # stationary, not a drifting random walk


def test_shock_is_deterministic_and_sized() -> None:
    first = apply_shock(FARES, fraction=0.2, multiplier=1.25, seed=1)
    assert first == apply_shock(FARES, fraction=0.2, multiplier=1.25, seed=1)
    assert len(first) == 40
    assert all(r.new_usd == 375.0 and r.shock for r in first)
    with pytest.raises(ValueError, match="fraction"):
        apply_shock(FARES, fraction=0, multiplier=1.1, seed=1)

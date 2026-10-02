from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from fcp.common.airports import BBox
from fcp.common.settings import OpenSkyMode, Settings
from fcp.ingestion.opensky import client as osc


@pytest.mark.parametrize(("area_side", "cost"), [(5, 1), (9, 2), (19, 3), (21, 4)])
def test_states_cost_by_area(area_side: float, cost: int) -> None:
    assert osc.states_cost(BBox(0, 0, area_side, area_side)) == cost
    assert osc.states_cost(None) == 4


def test_flights_cost_by_day_partitions() -> None:
    day = osc.SECONDS_PER_DAY
    assert osc.flights_cost(0, day - 1) == 30  # one partition
    assert osc.flights_cost(0, 2 * day - 1) == 30  # two
    assert osc.flights_cost(0, 3 * day - 1) == 180  # three -> 60 x 3
    with pytest.raises(ValueError, match="end must be"):
        osc.flights_cost(10, 0)


def test_previous_utc_day_window_is_one_partition() -> None:
    begin, end = osc.previous_utc_day_window(datetime(2026, 10, 2, 6, 30, tzinfo=UTC))
    assert datetime.fromtimestamp(begin, UTC) == datetime(2026, 10, 1, tzinfo=UTC)
    assert end - begin == osc.SECONDS_PER_DAY - 1
    assert osc.flights_cost(begin, end) == 30


class FakeBudget:
    def __init__(self) -> None:
        self.reserved = self.refunded = 0
        self.reconciled: list[int] = []

    def reserve(self, cost: int) -> int:
        self.reserved += cost
        return 100

    def refund(self, cost: int) -> None:
        self.refunded += cost

    def reconcile(self, remaining: int) -> None:
        self.reconciled.append(remaining)


def make_client(tmp_path: Path, **overrides: object) -> tuple[osc.OpenSkyClient, FakeBudget]:
    settings = Settings(data_dir=tmp_path, **overrides)  # type: ignore[arg-type]
    c = osc.OpenSkyClient(settings, http=httpx.Client())
    fake = FakeBudget()
    c.budgets = {k: fake for k in c.budgets}  # type: ignore[misc]
    return c, fake


@respx.mock
def test_authenticated_call_uses_bearer_token_and_reconciles_budget(tmp_path: Path) -> None:
    token = respx.post(osc.TOKEN_URL).respond(json={"access_token": "tok", "expires_in": 1800})
    states = respx.get(f"{osc.API_BASE}/states/all").respond(
        json={"time": 1, "states": []}, headers={"X-Rate-Limit-Remaining": "3990"}
    )
    c, budget = make_client(tmp_path, opensky_client_id="id", opensky_client_secret="secret")

    assert c.get_states(BBox(0, 0, 1, 1)) == {"time": 1, "states": []}
    assert states.calls.last.request.headers["Authorization"] == "Bearer tok"
    c.get_states(BBox(0, 0, 1, 1))
    assert token.call_count == 1  # token cached
    assert budget.reserved == 2 and budget.reconciled == [3990, 3990]


@respx.mock
def test_rate_limit_raises_with_retry_after(tmp_path: Path) -> None:
    respx.get(f"{osc.API_BASE}/states/all").respond(429, headers={"X-Rate-Limit-Retry-After-Seconds": "120"})
    c, _ = make_client(tmp_path)
    with pytest.raises(osc.RateLimitedError) as exc:
        c.get_states(BBox(0, 0, 1, 1))
    assert exc.value.retry_after_s == 120


@respx.mock
def test_connection_error_refunds_reserved_credits(tmp_path: Path) -> None:
    respx.get(f"{osc.API_BASE}/states/all").mock(side_effect=httpx.ConnectError("down"))
    c, budget = make_client(tmp_path)
    c._get.retry.sleep = lambda _: None  # type: ignore[attr-defined]
    with pytest.raises(httpx.ConnectError):
        c.get_states(BBox(0, 0, 1, 1))
    assert budget.refunded == budget.reserved == 1


def test_flights_require_credentials(tmp_path: Path) -> None:
    c, _ = make_client(tmp_path)
    with pytest.raises(PermissionError):
        c.get_arrivals("KORD", 0, 10)


def test_replay_serves_states_in_capture_order(tmp_path: Path) -> None:
    rec_dir = tmp_path / "recordings" / "opensky" / "states_all"
    rec_dir.mkdir(parents=True)
    for i in range(2):
        (rec_dir / f"2026100{i}T000000Z_x.json").write_text(
            json.dumps({"path": "/states/all", "params": {}, "payload": {"time": i}})
        )
    c, budget = make_client(tmp_path, opensky_mode=OpenSkyMode.REPLAY)
    assert [c.get_states(BBox(0, 0, 1, 1))["time"] for _ in range(3)] == [0, 1, 0]
    assert budget.reserved == 0  # replay never spends credits

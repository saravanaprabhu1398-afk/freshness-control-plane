from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

from fcp.common.db import connect
from fcp.common.ratelimit import BudgetExceededError, DailyBudget

pytestmark = pytest.mark.integration


@pytest.fixture
def provider() -> Iterator[str]:
    name = f"test-{uuid.uuid4().hex[:8]}"
    yield name
    with connect(autocommit=True) as conn:
        conn.execute("delete from ops.api_budget where provider = %s", (name,))


def test_reserve_until_limit_then_refuse(provider: str) -> None:
    budget = DailyBudget(provider, "states", daily_allowance=10, fraction=0.8)  # limit 8
    assert budget.reserve(4) == 4
    assert budget.reserve(4) == 0
    with pytest.raises(BudgetExceededError):
        budget.reserve(1)
    budget.refund(4)
    assert budget.reserve(1) == 3


def test_reconcile_adopts_higher_provider_usage(provider: str) -> None:
    budget = DailyBudget(provider, "states", daily_allowance=100, fraction=1.0)
    budget.reserve(5)
    budget.reconcile(provider_remaining=60)  # provider says 40 used
    with pytest.raises(BudgetExceededError):
        budget.reserve(61)
    assert budget.reserve(60) == 0

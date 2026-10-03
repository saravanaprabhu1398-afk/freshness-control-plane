"""Daily API credit budget (ARD AR-7, PRD NFR-3).

Every external call reserves its credit cost *before* it is made. Reservation is a single
conditional UPDATE, so concurrent pollers cannot overspend. When the provider reports its
own remaining balance (OpenSky's X-Rate-Limit-Remaining header) we reconcile with it,
because the provider's count is the one that matters.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import psycopg

from fcp.common.db import connect


class BudgetExceededError(RuntimeError):
    """Raised when a call would exceed today's budget. Callers should skip, not retry."""


class DailyBudget:
    def __init__(self, provider: str, endpoint_group: str, *, daily_allowance: int, fraction: float) -> None:
        if not 0 < fraction <= 1:
            raise ValueError("fraction must be in (0, 1]")
        self.provider = provider
        self.endpoint_group = endpoint_group
        self.daily_allowance = daily_allowance
        self.limit = int(daily_allowance * fraction)

    @staticmethod
    def _today() -> date:
        return datetime.now(UTC).date()

    def _ensure_row(self, conn: psycopg.Connection[Any]) -> None:
        conn.execute(
            """
            insert into ops.api_budget (provider, endpoint_group, day, credit_limit)
            values (%s, %s, %s, %s) on conflict do nothing
            """,
            (self.provider, self.endpoint_group, self._today(), self.limit),
        )

    def reserve(self, cost: int) -> int:
        """Reserve `cost` credits. Returns credits left under our limit, or raises."""
        if cost <= 0:
            raise ValueError("cost must be positive")
        with connect(autocommit=True) as conn:
            self._ensure_row(conn)
            row = conn.execute(
                """
                update ops.api_budget
                   set credits_used = credits_used + %(cost)s, credit_limit = %(limit)s, updated_at = now()
                 where provider = %(p)s and endpoint_group = %(e)s and day = %(d)s
                   and credits_used + %(cost)s <= %(limit)s
                returning credit_limit - credits_used as remaining
                """,
                {
                    "cost": cost,
                    "limit": self.limit,
                    "p": self.provider,
                    "e": self.endpoint_group,
                    "d": self._today(),
                },
            ).fetchone()
        if row is None:
            raise BudgetExceededError(
                f"{self.provider}/{self.endpoint_group}: reserving {cost} credits would exceed "
                f"today's limit of {self.limit}"
            )
        return int(row["remaining"])

    def refund(self, cost: int) -> None:
        """Give back credits for a call that never reached the provider (e.g. connection error)."""
        with connect(autocommit=True) as conn:
            conn.execute(
                """
                update ops.api_budget set credits_used = greatest(credits_used - %s, 0), updated_at = now()
                 where provider = %s and endpoint_group = %s and day = %s
                """,
                (cost, self.provider, self.endpoint_group, self._today()),
            )

    def reconcile(self, provider_remaining: int) -> None:
        """Adopt the provider's view of usage if it is higher than ours."""
        used_by_provider = max(self.daily_allowance - provider_remaining, 0)
        with connect(autocommit=True) as conn:
            self._ensure_row(conn)
            conn.execute(
                """
                update ops.api_budget set credits_used = greatest(credits_used, %s), updated_at = now()
                 where provider = %s and endpoint_group = %s and day = %s
                """,
                (used_by_provider, self.provider, self.endpoint_group, self._today()),
            )

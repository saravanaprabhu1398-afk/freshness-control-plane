"""OpenSky Network REST client.

API reference: https://openskynetwork.github.io/opensky-api/rest.html
  * Auth: OAuth2 client-credentials; tokens last ~30 min. Anonymous access is allowed with
    a smaller allowance and 10 s time resolution.
  * Credits are charged per endpoint per day: anonymous 400, standard account 4,000.
  * /states/all costs 1-4 credits depending on bounding-box area.
  * /flights/* costs depend on how many day partitions the query crosses; arrivals and
    departures are batch-processed nightly, so only the previous day or earlier is available.

Modes (FCP_OPENSKY_MODE): live, record (live + save responses), replay (offline).
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from fcp.common.airports import BBox
from fcp.common.logging import get_logger
from fcp.common.ratelimit import DailyBudget
from fcp.common.settings import OpenSkyMode, Settings

API_BASE = "https://opensky-network.org/api"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"  # noqa: S105 (a URL, not a secret)
ALLOWANCE_ANONYMOUS = 400
ALLOWANCE_STANDARD = 4000
SECONDS_PER_DAY = 86_400

log = get_logger(__name__)


def states_cost(bbox: BBox | None) -> int:
    """Credit cost of /states/all for a bounding box (None = global)."""
    if bbox is None:
        return 4
    area = bbox.area_sq_deg
    if area <= 25:
        return 1
    if area <= 100:
        return 2
    if area <= 400:
        return 3
    return 4


def flights_cost(begin: int, end: int) -> int:
    """Credit cost of /flights/* for [begin, end] (epoch seconds), by UTC day partitions crossed.

    Queries within the last 24 h are cheaper (4 credits) per the docs, but we always charge
    the partition price: over-estimating is safe, and reconcile() corrects it from headers.
    """
    if end < begin:
        raise ValueError("end must be >= begin")
    partitions = end // SECONDS_PER_DAY - begin // SECONDS_PER_DAY + 1
    if partitions <= 2:
        return 30
    for upper, per_partition in ((10, 60), (15, 120), (20, 240), (25, 480)):
        if partitions <= upper:
            return per_partition * partitions
    return 960 * partitions


class RateLimitedError(RuntimeError):
    def __init__(self, retry_after_s: int | None) -> None:
        super().__init__(f"OpenSky rate limit hit; retry after {retry_after_s}s")
        self.retry_after_s = retry_after_s


class ReplayMissError(RuntimeError):
    """Replay mode was asked for a response that was never recorded."""


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500


@dataclass
class _Token:
    value: str
    expires_at: float


class OpenSkyClient:
    def __init__(self, settings: Settings, *, http: httpx.Client | None = None) -> None:
        self.settings = settings
        self.mode = settings.opensky_mode
        self._http = http or httpx.Client(timeout=httpx.Timeout(30.0, connect=10.0))
        self._token: _Token | None = None
        allowance = ALLOWANCE_STANDARD if settings.opensky_authenticated else ALLOWANCE_ANONYMOUS
        frac = settings.opensky_budget_fraction
        # OpenSky budgets are per endpoint, so we keep one budget per endpoint group.
        self.budgets = {
            group: DailyBudget("opensky", group, daily_allowance=allowance, fraction=frac)
            for group in ("states", "flights_arrival", "flights_departure")
        }

    # ------------------------------------------------------------------ public API

    def get_states(self, bbox: BBox) -> dict[str, Any]:
        params = {"lamin": bbox.lamin, "lomin": bbox.lomin, "lamax": bbox.lamax, "lomax": bbox.lomax}
        return cast(dict[str, Any], self._call("states", "/states/all", params, states_cost(bbox)))

    def get_arrivals(self, airport_icao: str, begin: int, end: int) -> list[dict[str, Any]]:
        params = {"airport": airport_icao, "begin": begin, "end": end}
        return cast(
            list[dict[str, Any]],
            self._call("flights_arrival", "/flights/arrival", params, flights_cost(begin, end)),
        )

    def get_departures(self, airport_icao: str, begin: int, end: int) -> list[dict[str, Any]]:
        params = {"airport": airport_icao, "begin": begin, "end": end}
        return cast(
            list[dict[str, Any]],
            self._call("flights_departure", "/flights/departure", params, flights_cost(begin, end)),
        )

    def close(self) -> None:
        self._http.close()

    # ------------------------------------------------------------------ internals

    def _call(self, group: str, path: str, params: dict[str, Any], cost: int) -> Any:
        if self.mode is OpenSkyMode.REPLAY:
            return self._replay(path, params)

        if path.startswith("/flights") and not self.settings.opensky_authenticated:
            raise PermissionError("OpenSky /flights endpoints need credentials (OPENSKY_CLIENT_ID/SECRET)")

        budget = self.budgets[group]
        remaining = budget.reserve(cost)
        try:
            response = self._get(path, params)
        except httpx.TransportError:
            budget.refund(cost)  # the request never reached OpenSky
            raise

        if (hdr := response.headers.get("X-Rate-Limit-Remaining")) is not None:
            budget.reconcile(int(hdr))
        log.info(
            "opensky.call",
            path=path,
            cost=cost,
            budget_remaining=remaining,
            provider_remaining=hdr,
            status=response.status_code,
        )

        if response.status_code == 429:
            retry_after = response.headers.get("X-Rate-Limit-Retry-After-Seconds")
            raise RateLimitedError(int(retry_after) if retry_after else None)
        if response.status_code == 404 and path.startswith("/flights"):
            payload: Any = []  # OpenSky answers 404 when no flights match
        else:
            response.raise_for_status()
            payload = response.json()

        if self.mode is OpenSkyMode.RECORD:
            self._record(path, params, payload)
        return payload

    @retry(
        retry=retry_if_exception(_is_transient),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _get(self, path: str, params: dict[str, Any]) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self._bearer()}"} if self.settings.opensky_authenticated else {}
        response = self._http.get(f"{API_BASE}{path}", params=params, headers=headers)
        if response.status_code == 401 and self.settings.opensky_authenticated:
            self._token = None  # expired early; fetch a new one once
            headers = {"Authorization": f"Bearer {self._bearer()}"}
            response = self._http.get(f"{API_BASE}{path}", params=params, headers=headers)
        if response.status_code >= 500:
            response.raise_for_status()
        return response

    def _bearer(self) -> str:
        now = time.monotonic()
        if self._token and now < self._token.expires_at - 60:
            return self._token.value
        if not (self.settings.opensky_client_id and self.settings.opensky_client_secret):
            raise PermissionError("OpenSky credentials are not configured")
        response = self._http.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.settings.opensky_client_id,
                "client_secret": self.settings.opensky_client_secret.get_secret_value(),
            },
        )
        response.raise_for_status()
        body = response.json()
        self._token = _Token(body["access_token"], now + float(body.get("expires_in", 1800)))
        return self._token.value

    # ------------------------------------------------------------------ record / replay

    def _recording_path(self, path: str, params: dict[str, Any]) -> Path:
        # Recordings are grouped by endpoint; the file name encodes the exact query.
        # For /states/all the time-varying part is the capture time, so replay serves the
        # recordings in order (see _replay).
        endpoint = path.strip("/").replace("/", "_")
        digest = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:12]
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return self.settings.recordings_dir / endpoint / f"{stamp}_{digest}.json"

    def _record(self, path: str, params: dict[str, Any], payload: Any) -> None:
        target = self._recording_path(path, params)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps({"path": path, "params": params, "payload": payload}))

    def _replay(self, path: str, params: dict[str, Any]) -> Any:
        endpoint_dir = self.settings.recordings_dir / path.strip("/").replace("/", "_")
        files = sorted(endpoint_dir.glob("*.json")) if endpoint_dir.exists() else []
        if path == "/states/all":
            # Serve state snapshots in capture order, one per call, so a replay walks
            # through the same sequence of real changes. Cursor persists across runs.
            cursor_file = endpoint_dir / ".cursor"
            idx = int(cursor_file.read_text()) if cursor_file.exists() else 0
            if not files:
                raise ReplayMissError(f"no recordings in {endpoint_dir}")
            chosen = files[idx % len(files)]
            cursor_file.write_text(str(idx + 1))
            return json.loads(chosen.read_text())["payload"]
        for f in files:
            rec = json.loads(f.read_text())
            if rec["params"] == params:
                return rec["payload"]
        raise ReplayMissError(f"no recording for {path} {params}")


def previous_utc_day_window(now: datetime | None = None) -> tuple[int, int]:
    """[00:00, 23:59:59] of the previous UTC day, in epoch seconds (one day partition)."""
    ts = int((now or datetime.now(UTC)).timestamp())
    start_today = ts - ts % SECONDS_PER_DAY
    return start_today - SECONDS_PER_DAY, start_today - 1


__all__ = [
    "OpenSkyClient",
    "RateLimitedError",
    "ReplayMissError",
    "flights_cost",
    "previous_utc_day_window",
    "states_cost",
]

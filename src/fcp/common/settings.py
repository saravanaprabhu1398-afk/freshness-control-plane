"""Runtime configuration, read from environment variables and `.env`.

Every setting has a safe local default except credentials, which are optional:
without OpenSky credentials the poller runs anonymously (400 credits/day) or replays recordings.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[3]


class OpenSkyMode(StrEnum):
    LIVE = "live"  # call the API
    RECORD = "record"  # call the API and save every response to recordings_dir
    REPLAY = "replay"  # never call the API; serve saved responses (offline demos, CI)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env", env_prefix="FCP_", extra="ignore", env_nested_delimiter="__"
    )

    # Postgres
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "fcp"
    pg_user: str = "fcp"
    pg_password: SecretStr = SecretStr("fcp_local_only")

    # OpenSky (ADR-003). Credentials are optional; see module docstring.
    opensky_client_id: str | None = Field(default=None, validation_alias="OPENSKY_CLIENT_ID")
    opensky_client_secret: SecretStr | None = Field(default=None, validation_alias="OPENSKY_CLIENT_SECRET")
    opensky_mode: OpenSkyMode = OpenSkyMode.LIVE
    # Fraction of the daily credit allowance we allow ourselves to spend (NFR-3).
    opensky_budget_fraction: float = Field(default=0.8, gt=0, le=1)
    # An aircraft counts as "at" a tracked airport within this radius and below this altitude.
    track_radius_km: float = 80.0
    track_max_altitude_m: float = 4000.0

    # Paths
    data_dir: Path = REPO_ROOT / "data"
    warehouse_dir: Path = REPO_ROOT / "warehouse"
    contracts_file: Path = REPO_ROOT / "contracts" / "freshness_sla.yaml"

    # BTS
    bts_ontime_months: int = Field(default=6, ge=1, le=24)

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password.get_secret_value()}"
        )

    @property
    def opensky_authenticated(self) -> bool:
        return bool(self.opensky_client_id and self.opensky_client_secret)

    @property
    def recordings_dir(self) -> Path:
        return self.data_dir / "recordings" / "opensky"

    @property
    def downloads_dir(self) -> Path:
        return self.data_dir / "downloads"

    @property
    def duckdb_path(self) -> Path:
        return self.warehouse_dir / "fcp.duckdb"

    @property
    def iceberg_catalog_uri(self) -> str:
        return f"sqlite:///{self.warehouse_dir / 'iceberg_catalog.db'}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

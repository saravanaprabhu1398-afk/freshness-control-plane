"""Shared BTS (transtats.bts.gov) download logic: discovery of the latest file, cached download."""

from __future__ import annotations

import shutil
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from fcp.common.logging import get_logger

PREZIP = "https://transtats.bts.gov/PREZIP"
log = get_logger(__name__)


def exists(client: httpx.Client, filename: str) -> bool:
    response = client.head(f"{PREZIP}/{filename}")
    return response.status_code == 200


def download(client: httpx.Client, filename: str, dest_dir: Path) -> Path:
    """Download once into the cache directory; reuse the cached file afterwards."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / filename
    if target.exists() and zipfile.is_zipfile(target):
        return target
    tmp = target.with_suffix(".part")
    with client.stream("GET", f"{PREZIP}/{filename}") as response:
        response.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in response.iter_bytes(1 << 20):
                fh.write(chunk)
    tmp.rename(target)
    log.info("bts.downloaded", file=filename, bytes=target.stat().st_size)
    return target


@contextmanager
def extracted_csv(zip_path: Path) -> Iterator[Path]:
    """Extract the single CSV inside a BTS zip to a temporary directory."""
    with zipfile.ZipFile(zip_path) as zf, TemporaryDirectory(prefix="fcp-bts-") as tmp:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected one CSV in {zip_path.name}, found {members}")
        out = Path(tmp) / "data.csv"
        with zf.open(members[0]) as src, out.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        yield out


def http_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(120.0, connect=15.0),
        follow_redirects=True,
        headers={"User-Agent": "freshness-control-plane (portfolio project; github)"},
    )

# syntax=docker/dockerfile:1
# Application image for long-running services (the selective re-index consumer).
# Dependencies are installed from uv.lock in their own layer, so code changes rebuild in seconds.
# Only core dependencies: the pipeline extra (Dagster, dbt, Iceberg) is not needed by services.
# uv's download cache lives in a BuildKit cache mount, never in an image layer.
FROM python:3.12.15-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.12.22 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY cdc ./cdc
COPY contracts ./contracts
COPY db ./db
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev

# Models are cached on a volume at /data/models, not baked into the image.
ENV FCP_DATA_DIR=/data \
    FCP_HEARTBEAT_FILE=/tmp/fcp-reindex.heartbeat
# /data/models must exist and belong to the service user before the volume is mounted: Docker
# copies this ownership into a new named volume (otherwise it is root-owned and unwritable).
RUN useradd --create-home --uid 10001 fcp && mkdir -p /data/models && chown -R fcp /data
USER fcp

ENTRYPOINT ["/app/.venv/bin/fcp"]
CMD ["reindex", "run"]

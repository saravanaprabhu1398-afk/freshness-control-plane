"""Structured JSON logging (SDD §9). Call `configure()` once per process."""

from __future__ import annotations

import logging
import sys

import structlog


def configure(level: str = "INFO", *, json: bool | None = None) -> None:
    """Configure structlog. Pretty console output on a TTY, JSON lines otherwise."""
    use_json = (not sys.stderr.isatty()) if json is None else json
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]

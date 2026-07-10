"""
Shared structured-logging configuration (JSON logs -> easy to grep/ship to
Prometheus textfile collector or stdout aggregation later).

Level is taken from the LOG_LEVEL env var (default INFO) so a debug run
doesn't require a code change.
"""
from __future__ import annotations

import logging
import os

import structlog

_configured = False


def _level_from_env() -> int:
    """Retrieves the logging level from the environment.

    Returns:
        int: The resolved logging level (e.g., logging.INFO).
    """
    name = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, name, logging.INFO)


def configure_logging(level: int | None = None) -> None:
    """Configures the global structured logging setup.

    Idempotent global setup — safe to call from every get_logger().

    Args:
        level (int | None, optional): The explicit logging level to set.
            If None, it defaults to the level from the environment.
    """
    global _configured
    if _configured:
        return
    resolved = level if level is not None else _level_from_env()
    logging.basicConfig(format="%(message)s", level=resolved)
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            # Renders exc_info / log.exception tracebacks into the JSON
            # payload instead of silently dropping them.
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(resolved),
    )
    _configured = True


def get_logger(name: str) -> structlog.BoundLogger:
    """Retrieves a configured structured logger.

    Args:
        name (str): The name for the logger, typically __name__.

    Returns:
        structlog.BoundLogger: A bound logger instance ready for structured logging.
    """
    configure_logging()
    return structlog.get_logger(name)

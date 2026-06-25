"""CLI verbosity and logging helpers."""

from __future__ import annotations

import logging

TRACE_LEVEL = 5
_TRACE_LEVEL_NAME = "TRACE"


def _install_trace_level() -> None:
    """Register the local TRACE logging level once."""
    if logging.getLevelName(TRACE_LEVEL) != _TRACE_LEVEL_NAME:
        logging.addLevelName(TRACE_LEVEL, _TRACE_LEVEL_NAME)


def normalize_verbosity(value: int | str | None) -> int:
    """Return a non-negative verbosity count."""
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def verbosity_to_log_level(verbosity: int | str | None) -> int:
    """Map ``-v`` counts to concrete logging levels."""
    _install_trace_level()
    normalized = normalize_verbosity(verbosity)
    if normalized <= 0:
        return logging.WARNING
    if normalized == 1:
        return logging.INFO
    if normalized == 2:
        return logging.DEBUG
    return TRACE_LEVEL


def verbosity_to_log_level_name(verbosity: int | str | None) -> str:
    """Return a stable public level name for a verbosity count."""
    level = verbosity_to_log_level(verbosity)
    name = logging.getLevelName(level)
    return str(name if isinstance(name, str) else level)


def configure_logging(
    verbosity: int | str | None,
    *,
    force: bool = False,
) -> int:
    """Configure standard logging for CLI verbosity flags."""
    level = verbosity_to_log_level(verbosity)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
        force=force,
    )
    logging.getLogger("histdatacom").setLevel(level)
    if normalize_verbosity(verbosity) >= 3:
        logging.getLogger("temporalio").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
    return level


def verbosity_metadata(verbosity: int | str | None) -> dict[str, object]:
    """Return JSON-compatible metadata for persisted requests."""
    normalized = normalize_verbosity(verbosity)
    return {
        "verbosity": normalized,
        "log_level": verbosity_to_log_level_name(normalized),
    }

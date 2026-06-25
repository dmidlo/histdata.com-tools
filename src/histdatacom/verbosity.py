"""CLI verbosity and logging helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import logging
from os import PathLike
from typing import Any

TRACE_LEVEL = 5
_TRACE_LEVEL_NAME = "TRACE"
REDACTED_LOG_VALUE = "[redacted]"
MAX_LOG_STRING_LENGTH = 240
MAX_LOG_SEQUENCE_ITEMS = 12
SENSITIVE_LOG_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "password",
    "private_key",
    "secret",
    "token",
)


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


def safe_log_extra(**values: Any) -> dict[str, object]:
    """Return bounded, redacted metadata suitable for ``logging.extra``."""
    return {
        str(key): _safe_log_value(str(key), value)
        for key, value in values.items()
    }


def _safe_log_value(key: str, value: Any) -> object:
    if _sensitive_log_key(key):
        return REDACTED_LOG_VALUE
    if isinstance(value, Mapping):
        return {
            str(item_key): _safe_log_value(str(item_key), item_value)
            for item_key, item_value in tuple(value.items())[
                :MAX_LOG_SEQUENCE_ITEMS
            ]
        }
    if isinstance(value, (str, bytes)):
        return _bounded_log_string(value)
    if isinstance(value, PathLike):
        return _bounded_log_string(value)
    if isinstance(value, Sequence):
        return [
            _safe_log_value(key, item)
            for item in tuple(value)[:MAX_LOG_SEQUENCE_ITEMS]
        ]
    if isinstance(value, set | frozenset):
        return [
            _safe_log_value(key, item)
            for item in tuple(sorted(value, key=str))[:MAX_LOG_SEQUENCE_ITEMS]
        ]
    if value is None or isinstance(value, bool | int | float):
        return value
    return _bounded_log_string(value)


def _sensitive_log_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in SENSITIVE_LOG_KEY_PARTS)


def _bounded_log_string(value: object) -> str:
    text = (
        value.decode("utf-8", errors="replace")
        if isinstance(value, bytes)
        else str(value)
    )
    if len(text) <= MAX_LOG_STRING_LENGTH:
        return text
    return f"{text[: MAX_LOG_STRING_LENGTH - 3]}..."

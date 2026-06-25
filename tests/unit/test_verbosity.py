"""Tests for CLI verbosity mapping."""

from __future__ import annotations

import logging

from histdatacom.verbosity import (
    REDACTED_LOG_VALUE,
    TRACE_LEVEL,
    configure_logging,
    safe_log_extra,
    verbosity_metadata,
    verbosity_to_log_level,
    verbosity_to_log_level_name,
)


def test_verbosity_counts_map_to_stable_log_levels() -> None:
    """The public -v/-vv/-vvv contract should be deterministic."""
    assert verbosity_to_log_level(0) == logging.WARNING
    assert verbosity_to_log_level(1) == logging.INFO
    assert verbosity_to_log_level(2) == logging.DEBUG
    assert verbosity_to_log_level(3) == TRACE_LEVEL
    assert verbosity_to_log_level_name(3) == "TRACE"


def test_configure_logging_returns_configured_level() -> None:
    """Callers can inspect the configured logging level."""
    assert configure_logging(2) == logging.DEBUG


def test_verbosity_metadata_is_json_ready() -> None:
    """Run requests should carry compact verbosity metadata when needed."""
    assert verbosity_metadata(3) == {
        "verbosity": 3,
        "log_level": "TRACE",
    }


def test_safe_log_extra_redacts_and_bounds_metadata() -> None:
    """Structured log metadata should be safe for user bug reports."""
    extra = safe_log_extra(
        request_id="run-logs",
        password="secret",
        influx_config={
            "INFLUX_TOKEN": "token-value",
            "INFLUX_URL": "http://127.0.0.1:8086",
        },
        notes="x" * 300,
        values=list(range(20)),
    )

    assert extra["request_id"] == "run-logs"
    assert extra["password"] == REDACTED_LOG_VALUE
    assert extra["influx_config"] == {
        "INFLUX_TOKEN": REDACTED_LOG_VALUE,
        "INFLUX_URL": "http://127.0.0.1:8086",
    }
    assert str(extra["notes"]).endswith("...")
    assert len(extra["values"]) == 12

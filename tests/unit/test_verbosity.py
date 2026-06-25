"""Tests for CLI verbosity mapping."""

from __future__ import annotations

import logging

from histdatacom.verbosity import (
    TRACE_LEVEL,
    configure_logging,
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

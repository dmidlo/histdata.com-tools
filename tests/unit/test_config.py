"""Pytest unit tests for histdatacom.config.py."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from histdatacom import config


def test_config() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_config_has_no_parser_args_global() -> None:
    """Runtime config should not expose stale parser argument globals."""
    assert not hasattr(config, "ARGS")


def test_post_headers_are_immutable_and_factory_returns_fresh_headers() -> None:
    """Default HistData POST headers should not share mutable request state."""
    assert isinstance(config.POST_HEADERS, Mapping)
    with pytest.raises(TypeError):
        config.POST_HEADERS["Referer"] = "mutated"  # type: ignore[index]

    first = config.default_post_headers()
    second = config.default_post_headers()

    first["Referer"] = "request-one"

    assert second["Referer"] == ""
    assert config.POST_HEADERS["Referer"] == ""
    assert first is not second

"""Pytest unit tests for histdatacom.concurrency.py."""

from __future__ import annotations

import pytest


def test_concurrency() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_get_pool_cpu_count_uses_existing_cpu_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public CPU policy remains available for orchestration worker sizing."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)

    assert concurrency.get_pool_cpu_count(None) == 7
    assert concurrency.get_pool_cpu_count("low") == 3
    assert concurrency.get_pool_cpu_count("medium") == 5
    assert concurrency.get_pool_cpu_count("high") == 7
    assert concurrency.get_pool_cpu_count("50") == 3
    assert concurrency.get_pool_cpu_count("200") == 15


def test_get_pool_cpu_count_rejects_bad_cpu_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed CPU policy values should keep exiting nonzero."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)

    with pytest.raises(SystemExit):
        concurrency.get_pool_cpu_count("too-much")


def test_old_pool_runtime_classes_are_removed() -> None:
    """The old manager-backed runtime should not be importable."""
    import histdatacom.concurrency as concurrency

    assert not hasattr(concurrency, "QueueManager")
    assert not hasattr(concurrency, "ThreadPool")
    assert not hasattr(concurrency, "ProcessPool")

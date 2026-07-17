"""Tests for import-safe cross-platform resource measurements."""

from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace

import pytest

from histdatacom import resource_usage


def test_package_imports_without_unix_resource_module() -> None:
    """A fresh Windows-like interpreter can import every former call site."""
    script = """
import builtins

real_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name == "resource":
        raise ModuleNotFoundError("No module named 'resource'")
    return real_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

import histdatacom.data_analytics.feed_epochs_v2
import histdatacom.market_context.corpus
import histdatacom.market_context.positioning
import histdatacom.orchestration.performance
import histdatacom.synthetic.benchmark_corpus
import histdatacom.synthetic.motif_library
import histdatacom.synthetic.observation_calibration
import histdatacom.synthetic.reconstruction_handlers
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_peak_rss_bytes_returns_zero_without_resource_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Windows must import cleanly when the Unix resource module is absent."""

    def missing_resource(_name: str) -> None:
        raise ModuleNotFoundError("No module named 'resource'")

    monkeypatch.setattr(resource_usage, "import_module", missing_resource)

    assert resource_usage.peak_rss_bytes() == 0


@pytest.mark.parametrize(
    ("platform", "expected"),
    [("darwin", 123), ("linux", 123 * 1024), ("freebsd", 123 * 1024)],
)
def test_peak_rss_bytes_normalizes_platform_units(
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
    expected: int,
) -> None:
    """Unix measurements retain the existing byte-normalization contract."""
    fake_resource = SimpleNamespace(
        RUSAGE_SELF=0,
        getrusage=lambda _who: SimpleNamespace(ru_maxrss=123),
    )
    monkeypatch.setattr(
        resource_usage,
        "import_module",
        lambda _name: fake_resource,
    )
    monkeypatch.setattr(resource_usage.sys, "platform", platform)

    assert resource_usage.peak_rss_bytes() == expected

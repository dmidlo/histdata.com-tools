"""Tests for portable process resource-usage probes."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import histdatacom.resource_usage as resource_usage


class _FakeResource:
    RUSAGE_SELF = 0

    @staticmethod
    def getrusage(_target: int) -> SimpleNamespace:
        return SimpleNamespace(ru_maxrss=123)


class _FakeFunction:
    def __init__(self, callback: Any) -> None:
        self.callback = callback
        self.argtypes: list[Any] = []
        self.restype: Any = None

    def __call__(self, *args: Any) -> Any:
        return self.callback(*args)


def test_resource_peak_rss_normalizes_linux_kib_and_preserves_macos_bytes() -> (
    None
):
    linux = resource_usage._resource_peak_rss_measurement(
        _FakeResource,
        platform_name="linux",
    )
    macos = resource_usage._resource_peak_rss_measurement(
        _FakeResource,
        platform_name="darwin",
    )

    assert linux == resource_usage.PeakRssMeasurement(
        bytes=123 * 1024,
        source="resource.ru_maxrss",
        available=True,
    )
    assert macos == resource_usage.PeakRssMeasurement(
        bytes=123,
        source="resource.ru_maxrss",
        available=True,
    )


def test_windows_peak_rss_uses_peak_working_set_bytes(monkeypatch: Any) -> None:
    get_current_process = _FakeFunction(lambda: 1234)

    def populate_counters(
        _process: int,
        counters_pointer: Any,
        _size: int,
    ) -> int:
        counters_pointer._obj.PeakWorkingSetSize = 987_654
        return 1

    get_process_memory_info = _FakeFunction(populate_counters)

    def fake_windll(name: str, *, use_last_error: bool) -> SimpleNamespace:
        assert use_last_error is True
        if name == "kernel32":
            return SimpleNamespace(GetCurrentProcess=get_current_process)
        assert name == "psapi"
        return SimpleNamespace(GetProcessMemoryInfo=get_process_memory_info)

    monkeypatch.setattr(
        resource_usage.ctypes,
        "WinDLL",
        fake_windll,
        raising=False,
    )

    assert resource_usage._windows_peak_working_set_bytes() == 987_654


def test_public_probe_records_windows_source_when_resource_is_unavailable(
    monkeypatch: Any,
) -> None:
    def missing_resource(_name: str) -> Any:
        raise ModuleNotFoundError("resource")

    monkeypatch.setattr(resource_usage, "import_module", missing_resource)
    monkeypatch.setattr(
        resource_usage,
        "sys",
        SimpleNamespace(platform="win32"),
    )
    monkeypatch.setattr(
        resource_usage,
        "_windows_peak_working_set_bytes",
        lambda: 456_789,
    )

    assert resource_usage.peak_rss_measurement() == (
        resource_usage.PeakRssMeasurement(
            bytes=456_789,
            source="windows.PeakWorkingSetSize",
            available=True,
        )
    )
    assert resource_usage.peak_rss_bytes() == 456_789


def test_public_probe_has_explicit_unavailable_sentinel(
    monkeypatch: Any,
) -> None:
    def missing_resource(_name: str) -> Any:
        raise ModuleNotFoundError("resource")

    monkeypatch.setattr(resource_usage, "import_module", missing_resource)
    monkeypatch.setattr(
        resource_usage,
        "sys",
        SimpleNamespace(platform="unknown"),
    )

    assert resource_usage.peak_rss_measurement() == (
        resource_usage.PeakRssMeasurement(
            bytes=0,
            source="unavailable",
            available=False,
        )
    )
    assert resource_usage.peak_rss_bytes() == 0


def test_public_release_surfaces_import_without_resource_module() -> None:
    root = Path(__file__).resolve().parents[2]
    program = """
import builtins

real_import = builtins.__import__

def guarded_import(name, *args, **kwargs):
    if name == "resource":
        raise ModuleNotFoundError("resource is unavailable")
    return real_import(name, *args, **kwargs)

builtins.__import__ = guarded_import

import histdatacom.data_analytics.feed_epochs_v2
import histdatacom.market_context.corpus
import histdatacom.market_context.positioning
import histdatacom.synthetic.benchmark_corpus
import histdatacom.synthetic.motif_library
import histdatacom.synthetic.observation_calibration
import histdatacom.synthetic.reconstruction_handlers
"""
    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr

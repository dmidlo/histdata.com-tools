"""Portable process resource-usage probes.

Python's :mod:`resource` module is unavailable on Windows.  Release and
scientific resource audits still need a process-level peak resident-memory
measurement there, so this module normalizes Unix ``ru_maxrss`` and uses the
Windows process API without adding a runtime dependency.
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass
from importlib import import_module
import sys
from typing import Any


@dataclass(frozen=True)
class PeakRssMeasurement:
    """A peak resident-memory observation and its platform source."""

    bytes: int
    source: str
    available: bool


class _WindowsProcessMemoryCounters(ctypes.Structure):
    """Subset-compatible ``PROCESS_MEMORY_COUNTERS`` layout."""

    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def peak_rss_measurement() -> PeakRssMeasurement:
    """Return peak process RSS in bytes with explicit source availability."""
    try:
        resource_module = import_module("resource")
    except (ImportError, ModuleNotFoundError):
        resource_module = None
    if resource_module is not None:
        measurement = _resource_peak_rss_measurement(
            resource_module,
            platform_name=sys.platform,
        )
        if measurement.available:
            return measurement
    if sys.platform.startswith("win"):
        peak = _windows_peak_working_set_bytes()
        if peak > 0:
            return PeakRssMeasurement(
                bytes=peak,
                source="windows.PeakWorkingSetSize",
                available=True,
            )
    return PeakRssMeasurement(bytes=0, source="unavailable", available=False)


def peak_rss_bytes() -> int:
    """Return peak process RSS bytes or the explicit unavailable sentinel ``0``."""
    return peak_rss_measurement().bytes


def _resource_peak_rss_measurement(
    resource_module: Any,
    *,
    platform_name: str,
) -> PeakRssMeasurement:
    try:
        usage = resource_module.getrusage(resource_module.RUSAGE_SELF)
        peak = int(getattr(usage, "ru_maxrss", 0) or 0)
    except (AttributeError, OSError, TypeError, ValueError):
        return PeakRssMeasurement(
            bytes=0, source="unavailable", available=False
        )
    if peak <= 0:
        return PeakRssMeasurement(
            bytes=0, source="unavailable", available=False
        )
    if platform_name.startswith("linux"):
        peak *= 1024
    return PeakRssMeasurement(
        bytes=peak,
        source="resource.ru_maxrss",
        available=True,
    )


def _windows_peak_working_set_bytes() -> int:
    """Return Windows peak working-set bytes, or ``0`` when the API is absent."""
    try:
        windll_factory: Any = getattr(ctypes, "WinDLL")
        kernel32 = windll_factory("kernel32", use_last_error=True)
        psapi = windll_factory("psapi", use_last_error=True)
        get_current_process = kernel32.GetCurrentProcess
        get_current_process.argtypes = []
        get_current_process.restype = ctypes.c_void_p
        get_process_memory_info = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_WindowsProcessMemoryCounters),
            ctypes.c_ulong,
        ]
        get_process_memory_info.restype = ctypes.c_int
        counters = _WindowsProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        succeeded = get_process_memory_info(
            get_current_process(),
            ctypes.byref(counters),
            counters.cb,
        )
    except (AttributeError, OSError, TypeError, ValueError):
        return 0
    if not succeeded:
        return 0
    return max(0, int(counters.PeakWorkingSetSize))

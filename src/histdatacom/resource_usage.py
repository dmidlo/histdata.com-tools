"""Cross-platform process resource measurements.

The standard-library :mod:`resource` module is unavailable on Windows.  Keep
that optional platform dependency behind one import-safe boundary so package
imports and resource-bounded workflows remain usable everywhere.
"""

from __future__ import annotations

from importlib import import_module
import sys


def peak_rss_bytes() -> int:
    """Return peak resident memory in bytes, or zero when unmeasurable."""
    try:
        resource = import_module("resource")
    except ModuleNotFoundError:
        return 0
    usage = resource.getrusage(resource.RUSAGE_SELF)
    peak = int(getattr(usage, "ru_maxrss", 0) or 0)
    if peak <= 0:
        return 0
    return peak if sys.platform.startswith("darwin") else peak * 1024

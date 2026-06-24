"""Polars cache helpers for data-quality scans."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.data_quality.contracts import QualityTarget, QualityTargetKind
from histdatacom.histdata_ascii import CACHE_FILENAME, read_polars_cache


@dataclass(frozen=True, slots=True)
class FreshPolarsCache:
    """A Polars IPC cache that is safe to use for a quality scan."""

    path: Path
    frame: Any
    source: str = "sibling"


def read_fresh_sibling_polars_cache(
    target: QualityTarget,
    *,
    required_columns: tuple[str, ...],
) -> FreshPolarsCache | None:
    """Return a fresh sibling Polars cache for a CSV target, if available."""
    if target.kind is not QualityTargetKind.CSV:
        return None

    csv_path = Path(target.path)
    cache_path = csv_path.with_name(CACHE_FILENAME)
    try:
        csv_stat = csv_path.stat()
        cache_stat = cache_path.stat()
    except OSError:
        return None
    if cache_stat.st_mtime_ns < csv_stat.st_mtime_ns:
        return None

    try:
        frame = read_polars_cache(cache_path)
    except (OSError, ValueError):
        return None

    columns = set(getattr(frame, "columns", ()))
    if not set(required_columns).issubset(columns):
        return None
    return FreshPolarsCache(path=cache_path, frame=frame, source="sibling")


def read_quality_polars_cache(
    target: QualityTarget,
    *,
    required_columns: tuple[str, ...],
) -> FreshPolarsCache | None:
    """Return a direct cache target or fresh sibling CSV cache, if usable."""
    if target.kind is QualityTargetKind.CACHE:
        cache_path = Path(target.path)
        try:
            frame = read_polars_cache(cache_path)
        except (OSError, ValueError):
            return None

        columns = set(getattr(frame, "columns", ()))
        if not set(required_columns).issubset(columns):
            return None
        return FreshPolarsCache(path=cache_path, frame=frame, source="direct")

    return read_fresh_sibling_polars_cache(
        target,
        required_columns=required_columns,
    )

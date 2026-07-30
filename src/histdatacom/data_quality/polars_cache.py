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
    fresh: bool | None = True
    source_mtime_ns: int | None = None
    cache_mtime_ns: int | None = None


def read_fresh_sibling_polars_cache(
    target: QualityTarget,
    *,
    required_columns: tuple[str, ...],
) -> FreshPolarsCache | None:
    """Return a fresh sibling Polars cache for a CSV target, if available."""
    cache = read_fingerprint_parity_polars_cache(
        target,
        required_columns=required_columns,
    )
    if (
        cache is None
        or target.kind is not QualityTargetKind.CSV
        or cache.fresh is not True
    ):
        return None
    return cache


def read_fingerprint_parity_polars_cache(
    target: QualityTarget,
    *,
    required_columns: tuple[str, ...],
) -> FreshPolarsCache | None:
    """Return a readable direct or sibling cache with freshness evidence."""
    if target.kind is QualityTargetKind.CACHE:
        cache = read_quality_polars_cache(
            target,
            required_columns=required_columns,
        )
        if cache is None:
            return None
        try:
            cache_mtime_ns = cache.path.stat().st_mtime_ns
        except OSError:
            cache_mtime_ns = None
        return FreshPolarsCache(
            path=cache.path,
            frame=cache.frame,
            source="direct",
            fresh=None,
            cache_mtime_ns=cache_mtime_ns,
        )
    if target.kind not in {QualityTargetKind.CSV, QualityTargetKind.ZIP}:
        return None

    source_path = Path(target.path)
    cache_path = source_path.with_name(CACHE_FILENAME)
    try:
        source_stat = source_path.stat()
        cache_stat = cache_path.stat()
        frame = read_polars_cache(cache_path)
    except (OSError, ValueError):
        return None
    columns = set(getattr(frame, "columns", ()))
    if not set(required_columns).issubset(columns):
        return None
    return FreshPolarsCache(
        path=cache_path,
        frame=frame,
        source="sibling",
        fresh=cache_stat.st_mtime_ns >= source_stat.st_mtime_ns,
        source_mtime_ns=source_stat.st_mtime_ns,
        cache_mtime_ns=cache_stat.st_mtime_ns,
    )


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

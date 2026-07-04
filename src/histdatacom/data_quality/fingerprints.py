"""Structural contracts for deterministic time-series fingerprints."""

from __future__ import annotations

import csv
import hashlib
import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.polars_cache import read_quality_polars_cache
from histdatacom.histdata_ascii import (
    M1,
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    normalize_ascii_row,
)
from histdatacom.publication_safety import publish_safe_path
from histdatacom.runtime_contracts import JSONValue

TIME_SERIES_FINGERPRINT_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint.v1"
)
TIME_SERIES_FINGERPRINT_METADATA_KEY = "time_series_fingerprint"
SERIES_FINGERPRINT_RULE_ID = "fingerprint.series"
CROSS_SERIES_FINGERPRINT_RULE_ID = "fingerprint.cross_series"
SERIES_FINGERPRINT_SUMMARY_CODE = "FINGERPRINT_SERIES_SUMMARY"
SERIES_FINGERPRINT_SOURCE_UNAVAILABLE_CODE = "FINGERPRINT_SOURCE_UNAVAILABLE"

DEFAULT_FINGERPRINT_QUANTILES = (
    0.01,
    0.05,
    0.25,
    0.5,
    0.75,
    0.95,
    0.99,
)
DEFAULT_FINGERPRINT_LAGS = (1, 2, 3, 5, 10, 30, 60, 240, 1440)
DEFAULT_FINGERPRINT_ROLLING_WINDOWS = (60, 240, 1440)
DEFAULT_FINGERPRINT_HISTOGRAM_BINS = 32
DEFAULT_FINGERPRINT_MAX_ROWS = 1_000_000
DEFAULT_FINGERPRINT_ROUNDING_DIGITS = 12
SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES = (M1, TICK)
SUPPORTED_SERIES_FINGERPRINT_KINDS = (
    QualityTargetKind.CSV,
    QualityTargetKind.ZIP,
    QualityTargetKind.CACHE,
)


@dataclass(frozen=True, slots=True)
class HistDataFingerprintProfile:
    """Operator-tunable limits for deterministic fingerprint summaries."""

    quantiles: tuple[float, ...] = DEFAULT_FINGERPRINT_QUANTILES
    lags: tuple[int, ...] = DEFAULT_FINGERPRINT_LAGS
    rolling_windows: tuple[int, ...] = DEFAULT_FINGERPRINT_ROLLING_WINDOWS
    histogram_bins: int = DEFAULT_FINGERPRINT_HISTOGRAM_BINS
    max_rows: int = DEFAULT_FINGERPRINT_MAX_ROWS
    rounding_digits: int = DEFAULT_FINGERPRINT_ROUNDING_DIGITS

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "quantiles": list(self.quantiles),
            "lags": list(self.lags),
            "rolling_windows": list(self.rolling_windows),
            "histogram_bins": self.histogram_bins,
            "max_rows": self.max_rows,
            "rounding_digits": self.rounding_digits,
        }


@dataclass(slots=True)
class HistDataSeriesFingerprintRule:
    """Emit canonical target-scoped time-series fingerprints."""

    profile: HistDataFingerprintProfile = field(
        default_factory=HistDataFingerprintProfile
    )
    rule_id: str = SERIES_FINGERPRINT_RULE_ID
    description: str = (
        "Emit deterministic target-scoped time-series fingerprints."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return one bounded fingerprint finding for one target."""
        payload = _series_fingerprint_payload(target)
        source = cast(dict[str, JSONValue], payload["source"])
        unavailable = source.get("kind") == "unavailable"
        code = (
            SERIES_FINGERPRINT_SOURCE_UNAVAILABLE_CODE
            if unavailable
            else SERIES_FINGERPRINT_SUMMARY_CODE
        )
        message = (
            "Target source is unavailable for canonical fingerprinting."
            if unavailable
            else "Canonical target time-series fingerprint."
        )
        return (
            QualityFinding(
                severity=QualitySeverity.INFO,
                code=code,
                message=message,
                rule_id=self.rule_id,
                target=target,
                location=QualityLocation(
                    path=target.path,
                    column=TIME_SERIES_FINGERPRINT_METADATA_KEY,
                ),
                metadata={TIME_SERIES_FINGERPRINT_METADATA_KEY: payload},
            ),
        )


def fingerprint_quality_rules(
    profile: HistDataFingerprintProfile | None = None,
) -> tuple[QualityRule, ...]:
    """Return target-scoped fingerprint quality rules."""
    rule: QualityRule = HistDataSeriesFingerprintRule(
        profile=profile or HistDataFingerprintProfile()
    )
    return (rule,)


def _series_fingerprint_payload(target: QualityTarget) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
        "target_axis": _target_axis(target),
        "coverage": _empty_coverage(parsed_row_count=None),
        "source": _unavailable_source(
            target,
            reason=_unsupported_reason(target),
        ),
    }
    if _unsupported_reason(target):
        payload["fingerprint_id"] = _fingerprint_id(payload)
        return payload

    columns = columns_for_timeframe(target.timeframe)
    cache = read_quality_polars_cache(target, required_columns=columns)
    if cache is not None:
        payload["coverage"] = _coverage_from_frame(cache.frame)
        payload["source"] = {
            "kind": "cache",
            "cache_source": cache.source,
            "path": publish_safe_path(str(cache.path)),
        }
        payload["fingerprint_id"] = _fingerprint_id(payload)
        return payload

    if target.kind is QualityTargetKind.CACHE:
        payload["coverage"] = _empty_coverage(parsed_row_count=None)
        payload["source"] = _unavailable_source(
            target,
            reason="cache_unavailable",
        )
        payload["fingerprint_id"] = _fingerprint_id(payload)
        return payload

    try:
        text_payload = _read_text_payload(target)
    except (OSError, UnicodeDecodeError, zipfile.BadZipFile) as exc:
        payload["source"] = _unavailable_source(
            target,
            reason="source_unreadable",
            error=exc,
        )
        payload["fingerprint_id"] = _fingerprint_id(payload)
        return payload
    except ValueError as exc:
        payload["source"] = _unavailable_source(
            target,
            reason=str(exc),
        )
        payload["fingerprint_id"] = _fingerprint_id(payload)
        return payload

    payload["coverage"] = _coverage_from_text(
        text_payload.text,
        timeframe=target.timeframe,
    )
    if target.kind is QualityTargetKind.ZIP:
        payload["source"] = {
            "kind": "zip_member",
            "path": publish_safe_path(target.path),
            "member": text_payload.source_member,
        }
    else:
        payload["source"] = {
            "kind": "csv_text",
            "path": publish_safe_path(target.path),
        }
    payload["fingerprint_id"] = _fingerprint_id(payload)
    return payload


@dataclass(frozen=True, slots=True)
class _TextPayload:
    text: str
    source_member: str = ""


@dataclass(slots=True)
class _CoverageScan:
    row_count: int = 0
    parsed_row_count: int = 0
    start_timestamp_utc_ms: int | None = None
    end_timestamp_utc_ms: int | None = None

    def to_payload(self) -> dict[str, JSONValue]:
        """Return canonical coverage metadata."""
        return {
            "row_count": self.row_count,
            "parsed_row_count": self.parsed_row_count,
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "duration_ms": _duration_ms(
                self.start_timestamp_utc_ms,
                self.end_timestamp_utc_ms,
            ),
        }


def _target_axis(target: QualityTarget) -> dict[str, JSONValue]:
    return {
        "data_format": target.data_format,
        "timeframe": target.timeframe,
        "symbol": target.symbol,
        "period": target.period,
        "kind": target.kind.value,
    }


def _empty_coverage(
    *,
    parsed_row_count: int | None,
) -> dict[str, JSONValue]:
    return {
        "row_count": 0,
        "parsed_row_count": parsed_row_count,
        "start_timestamp_utc_ms": None,
        "end_timestamp_utc_ms": None,
        "duration_ms": None,
    }


def _coverage_from_frame(frame: Any) -> dict[str, JSONValue]:
    row_count = int(getattr(frame, "height", 0) or 0)
    start = _cache_timestamp_at(frame, 0)
    end = _cache_timestamp_at(frame, row_count - 1)
    return {
        "row_count": row_count,
        "parsed_row_count": row_count,
        "start_timestamp_utc_ms": start,
        "end_timestamp_utc_ms": end,
        "duration_ms": _duration_ms(start, end),
    }


def _coverage_from_text(
    text: str,
    *,
    timeframe: str,
) -> dict[str, JSONValue]:
    scan = _CoverageScan()
    reader = csv.reader(
        text.splitlines(),
        delimiter=delimiter_for_timeframe(timeframe),
    )
    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        scan.row_count += 1
        try:
            parsed = normalize_ascii_row(timeframe, row)
        except ValueError:
            continue
        timestamp = int(parsed[0])
        scan.parsed_row_count += 1
        if scan.start_timestamp_utc_ms is None:
            scan.start_timestamp_utc_ms = timestamp
        scan.end_timestamp_utc_ms = timestamp
    return scan.to_payload()


def _cache_timestamp_at(frame: Any, row_index: int) -> int | None:
    if row_index < 0:
        return None
    columns = getattr(frame, "columns", ())
    if "datetime" not in columns:
        return None
    try:
        value = frame.get_column("datetime")[row_index]
    except (IndexError, TypeError):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _duration_ms(
    start_timestamp_utc_ms: int | None,
    end_timestamp_utc_ms: int | None,
) -> int | None:
    if start_timestamp_utc_ms is None or end_timestamp_utc_ms is None:
        return None
    return end_timestamp_utc_ms - start_timestamp_utc_ms


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        return _TextPayload(text=path.read_bytes().decode("utf-8"))

    with zipfile.ZipFile(path) as archive:
        members = tuple(
            name
            for name in archive.namelist()
            if not name.endswith("/") and Path(name).suffix.lower() == ".csv"
        )
        if len(members) != 1:
            raise ValueError("zip_csv_member_unavailable")
        member = members[0]
        return _TextPayload(
            text=archive.read(member).decode("utf-8"),
            source_member=member,
        )


def _unsupported_reason(target: QualityTarget) -> str:
    if target.data_format != "ascii":
        return "unsupported_data_format"
    if target.timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES:
        return "unsupported_timeframe"
    if target.kind not in SUPPORTED_SERIES_FINGERPRINT_KINDS:
        return "unsupported_target_kind"
    return ""


def _unavailable_source(
    target: QualityTarget,
    *,
    reason: str,
    error: Exception | None = None,
) -> dict[str, JSONValue]:
    source: dict[str, JSONValue] = {
        "kind": "unavailable",
        "path": publish_safe_path(target.path),
        "reason": reason,
    }
    if error is not None:
        source["error_type"] = type(error).__name__
        source["error"] = str(error)[:240]
    return source


def _fingerprint_id(payload: dict[str, JSONValue]) -> str:
    encoded = json.dumps(
        _fingerprint_material(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _fingerprint_material(
    payload: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    material = dict(payload)
    material.pop("fingerprint_id", None)
    source = dict(cast(dict[str, JSONValue], material.get("source") or {}))
    source.pop("path", None)
    material["source"] = source
    return material

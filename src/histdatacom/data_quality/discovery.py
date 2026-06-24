"""Local target discovery for offline data-quality assessments."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any

from histdatacom.data_quality.contracts import (
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.format_support import (
    data_format_from_code,
    known_histdata_format_codes,
    known_histdata_timeframes,
    quality_support_for_target,
)
from histdatacom.histdata_ascii import CACHE_FILENAME, M1, TICK
from histdatacom.runtime_contracts import JSONValue

QUALITY_CHECK_GROUPS = (
    "all",
    "inventory",
    "ingestion",
    "time",
    "bars",
    "ticks",
    "domain",
    "modeling",
)

_FORMAT_CODE_PATTERN = "|".join(known_histdata_format_codes())
_TIMEFRAME_PATTERN = "|".join(
    sorted(known_histdata_timeframes(), key=len, reverse=True)
)
_HISTDATA_DATA_FILENAME_RE = re.compile(
    rf"^DAT_(?P<format>{_FORMAT_CODE_PATTERN})_"
    rf"(?P<symbol>[A-Z0-9]+)_(?P<timeframe>{_TIMEFRAME_PATTERN})_"
    r"(?P<period>\d{4}(?:\d{2})?)(?:_[A-Z0-9_]+)?"
    r"(?:\.(?:csv|xlsx))?$",
    re.IGNORECASE,
)
_HISTDATA_ARCHIVE_FILENAME_RE = re.compile(
    rf"^HISTDATA_COM_(?P<format>{_FORMAT_CODE_PATTERN})_"
    rf"(?P<symbol>[A-Z0-9]+)_(?P<timeframe>{_TIMEFRAME_PATTERN})"
    r"(?P<period>\d{4}(?:\d{2})?)$",
    re.IGNORECASE,
)


class QualityDiscoveryError(ValueError):
    """Raised when local quality target discovery cannot continue."""


@dataclass(frozen=True, slots=True)
class QualityDiscoveryResult:
    """Result of scanning local filesystem paths for quality targets."""

    roots: tuple[str, ...] = ()
    targets: tuple[QualityTarget, ...] = ()
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def target_count(self) -> int:
        """Return the number of discovered targets."""
        return len(self.targets)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "roots": list(self.roots),
            "target_count": self.target_count,
            "targets": [target.to_dict() for target in self.targets],
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityDiscoveryResult":
        """Create a discovery result from JSON-compatible data."""
        return cls(
            roots=tuple(str(root) for root in data.get("roots", ())),
            targets=tuple(
                QualityTarget.from_dict(target)
                for target in data.get("targets", ())
            ),
            metadata=dict(data.get("metadata") or {}),
        )


def normalize_quality_check_groups(
    groups: Iterable[str] | None,
) -> tuple[str, ...]:
    """Return stable quality check group selections."""
    normalized = tuple(
        dict.fromkeys(
            str(group).strip().lower()
            for group in (groups or ("all",))
            if str(group).strip()
        )
    )
    if not normalized:
        return ("all",)

    unsupported = sorted(set(normalized).difference(QUALITY_CHECK_GROUPS))
    if unsupported:
        msg = "unsupported quality check group(s): " + ", ".join(unsupported)
        raise QualityDiscoveryError(msg)

    if "all" in normalized and len(normalized) > 1:
        msg = "--quality-checks all cannot be combined with specific groups"
        raise QualityDiscoveryError(msg)

    return normalized


def discover_quality_targets(
    paths: Iterable[str | Path],
) -> QualityDiscoveryResult:
    """Discover ZIP, CSV, and cache targets from local filesystem paths."""
    roots = _normalize_roots(paths)
    seen_paths: set[str] = set()
    targets: list[QualityTarget] = []

    for root in roots:
        if not root.exists():
            raise QualityDiscoveryError(
                f"quality target path does not exist: {root}"
            )
        if root.is_dir():
            candidates = sorted(
                path for path in root.rglob("*") if path.is_file()
            )
        elif root.is_file():
            candidates = [root]
        else:
            raise QualityDiscoveryError(
                f"quality target path is not a file or directory: {root}"
            )

        for candidate in candidates:
            target = quality_target_from_path(candidate)
            if target is None:
                if root == candidate:
                    raise QualityDiscoveryError(
                        "unsupported quality target file type: " f"{candidate}"
                    )
                continue
            resolved = str(candidate.resolve())
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            targets.append(target)

    return QualityDiscoveryResult(
        roots=tuple(str(root) for root in roots),
        targets=tuple(sorted(targets, key=lambda target: target.path)),
        metadata={"supported_kinds": [kind.value for kind in _TARGET_KINDS]},
    )


def quality_target_from_path(path: str | Path) -> QualityTarget | None:
    """Return a quality target for a supported file path, if any."""
    source = Path(path)
    kind = _target_kind(source)
    if kind is None:
        return None

    metadata = _metadata_from_filename(source)
    metadata["filename"] = source.name
    metadata["quality_support"] = quality_support_for_target(
        data_format=str(metadata.get("data_format", "") or ""),
        timeframe=str(metadata.get("timeframe", "") or ""),
        kind=kind.value,
    ).to_metadata()
    return QualityTarget(
        path=str(source.resolve()),
        kind=kind,
        data_format=str(metadata.get("data_format", "") or ""),
        timeframe=str(metadata.get("timeframe", "") or ""),
        symbol=str(metadata.get("symbol", "") or ""),
        period=str(metadata.get("period", "") or ""),
        metadata=metadata,
    )


def quality_metadata_from_filename(path: str | Path) -> dict[str, JSONValue]:
    """Return normalized HistData metadata parsed from a local filename."""
    return _metadata_from_filename(Path(path))


def _normalize_roots(paths: Iterable[str | Path]) -> tuple[Path, ...]:
    roots = tuple(Path(path).expanduser() for path in paths)
    if not roots:
        raise QualityDiscoveryError(
            "at least one quality target path is required"
        )
    return roots


def _target_kind(path: Path) -> QualityTargetKind | None:
    if path.name == CACHE_FILENAME:
        return QualityTargetKind.CACHE
    suffix = path.suffix.lower()
    if suffix == ".zip":
        return QualityTargetKind.ZIP
    if suffix == ".csv":
        return QualityTargetKind.CSV
    if suffix == ".xlsx":
        return QualityTargetKind.SPREADSHEET
    return None


def _metadata_from_filename(path: Path) -> dict[str, JSONValue]:
    if path.name == CACHE_FILENAME:
        return _metadata_from_cache_path(path)

    filename = path.name
    if path.suffix.lower() == ".zip":
        filename = path.with_suffix("").name

    match = _HISTDATA_DATA_FILENAME_RE.match(filename)
    if match is None:
        match = _HISTDATA_ARCHIVE_FILENAME_RE.match(filename)
    if match is None:
        return {}

    data_format = data_format_from_code(match.group("format"))
    if not data_format:
        return {}
    timeframe = match.group("timeframe").upper()

    return {
        "data_format": data_format,
        "format_code": match.group("format").upper(),
        "symbol": match.group("symbol").upper(),
        "timeframe": timeframe,
        "period": match.group("period"),
    }


def _metadata_from_cache_path(path: Path) -> dict[str, JSONValue]:
    parts = path.parts
    for index, part in enumerate(parts):
        if part.upper() != "ASCII":
            continue
        try:
            timeframe = parts[index + 1].upper()
            symbol = parts[index + 2].upper()
        except IndexError:
            return {}
        if timeframe not in {M1, TICK}:
            return {}

        period_parts = parts[index + 3 : -1]
        period = _period_from_path_parts(period_parts)
        if period:
            metadata: dict[str, JSONValue] = {
                "data_format": "ascii",
                "format_code": "ASCII",
                "symbol": symbol,
                "timeframe": timeframe,
                "period": period,
            }
            metadata["quality_support"] = quality_support_for_target(
                data_format="ascii",
                timeframe=timeframe,
                kind=QualityTargetKind.CACHE.value,
            ).to_metadata()
            return metadata
    return {}


def _period_from_path_parts(parts: tuple[str, ...]) -> str:
    if not parts:
        return ""
    year = parts[0]
    if len(year) != 4 or not year.isdigit():
        return ""
    if len(parts) == 1:
        return year
    month = parts[1]
    if not month.isdigit():
        return ""
    month_value = int(month)
    if not 1 <= month_value <= 12:
        return ""
    return f"{year}{month_value:02d}"


_TARGET_KINDS = (
    QualityTargetKind.ZIP,
    QualityTargetKind.CSV,
    QualityTargetKind.SPREADSHEET,
    QualityTargetKind.CACHE,
)

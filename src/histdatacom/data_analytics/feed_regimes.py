"""Feed-regime analytics for HistData ASCII tick artifacts."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
from math import ceil
from pathlib import Path
import re

from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    EST_NO_DST_OFFSET_MS,
    TICK,
    read_ascii_file,
    read_polars_cache,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

ANALYTICS_REPORT_SCHEMA_VERSION = "histdatacom.feed-regime-report.v1"
FEED_REGIME_OPERATION = "feed-regime-detection"
DEFAULT_QUIET_GAP_MS = 60_000
_ASCII_FORMAT = "ascii"
_CSV_SUFFIX = ".csv"
_ZIP_SUFFIX = ".zip"

_HISTDATA_DATA_FILENAME_RE = re.compile(
    r"^DAT_(?P<format>ASCII)_(?P<symbol>[A-Z0-9]+)_"
    r"(?P<timeframe>[A-Z0-9]+)_(?P<period>\d{4}(?:\d{2})?)"
    r"(?:_[A-Z0-9_]+)?(?:\.(?:csv|xlsx))?$",
    re.IGNORECASE,
)
_HISTDATA_ARCHIVE_FILENAME_RE = re.compile(
    r"^HISTDATA_COM_(?P<format>ASCII)_(?P<symbol>[A-Z0-9]+)_"
    r"(?P<timeframe>[A-Z0-9]+)(?P<period>\d{4}(?:\d{2})?)$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class AnalyticsTarget:
    """One local artifact considered by feed-regime analytics."""

    path: str
    kind: str
    data_format: str = ""
    timeframe: str = ""
    symbol: str = ""
    period: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def is_supported_tick_target(self) -> bool:
        """Return whether the target can feed tick-regime analytics."""
        return self.data_format == _ASCII_FORMAT and self.timeframe == TICK

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "path": self.path,
            "kind": self.kind,
            "data_format": self.data_format,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
            "period": self.period,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class AnalyticsDiscoveryResult:
    """Result of discovering local analytics targets."""

    roots: tuple[str, ...] = ()
    targets: tuple[AnalyticsTarget, ...] = ()
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


@dataclass(frozen=True, slots=True)
class FeedPeriodProfile:
    """Tick-feed summary statistics for one symbol/time bucket."""

    symbol: str
    period: str
    bucket: str
    row_count: int
    start_utc_ms: int
    end_utc_ms: int
    tick_rate_per_hour: float
    median_interarrival_ms: float
    p95_interarrival_ms: float
    max_interarrival_ms: int
    quiet_gap_count: int
    quote_update_count: int
    quote_update_ratio: float
    zero_change_run_count: int
    zero_change_tick_count: int
    spread_min: float
    spread_median: float
    spread_mean: float
    spread_max: float
    session_counts: dict[str, int] = field(default_factory=dict)
    target_paths: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "symbol": self.symbol,
            "period": self.period,
            "bucket": self.bucket,
            "row_count": self.row_count,
            "start_utc_ms": self.start_utc_ms,
            "end_utc_ms": self.end_utc_ms,
            "tick_rate_per_hour": _round_float(self.tick_rate_per_hour),
            "median_interarrival_ms": _round_float(self.median_interarrival_ms),
            "p95_interarrival_ms": _round_float(self.p95_interarrival_ms),
            "max_interarrival_ms": self.max_interarrival_ms,
            "quiet_gap_count": self.quiet_gap_count,
            "quote_update_count": self.quote_update_count,
            "quote_update_ratio": _round_float(self.quote_update_ratio),
            "zero_change_run_count": self.zero_change_run_count,
            "zero_change_tick_count": self.zero_change_tick_count,
            "spread_min": _round_float(self.spread_min),
            "spread_median": _round_float(self.spread_median),
            "spread_mean": _round_float(self.spread_mean),
            "spread_max": _round_float(self.spread_max),
            "session_counts": dict(sorted(self.session_counts.items())),
            "target_paths": list(self.target_paths),
        }


@dataclass(frozen=True, slots=True)
class FeedRegimeEra:
    """A contiguous run of similar feed behavior for one symbol."""

    symbol: str
    label: str
    bucket: str
    period_start: str
    period_end: str
    start_utc_ms: int
    end_utc_ms: int
    profile_count: int
    row_count: int
    mean_tick_rate_per_hour: float
    median_interarrival_ms: float
    quote_update_ratio: float
    quiet_gap_count: int
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "symbol": self.symbol,
            "label": self.label,
            "bucket": self.bucket,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "start_utc_ms": self.start_utc_ms,
            "end_utc_ms": self.end_utc_ms,
            "profile_count": self.profile_count,
            "row_count": self.row_count,
            "mean_tick_rate_per_hour": _round_float(
                self.mean_tick_rate_per_hour
            ),
            "median_interarrival_ms": _round_float(self.median_interarrival_ms),
            "quote_update_ratio": _round_float(self.quote_update_ratio),
            "quiet_gap_count": self.quiet_gap_count,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class FeedRegimeReport:
    """Machine-readable feed-regime analytics output."""

    discovery: AnalyticsDiscoveryResult
    period_profiles: tuple[FeedPeriodProfile, ...] = ()
    regimes: tuple[FeedRegimeEra, ...] = ()
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def summary(self) -> dict[str, JSONValue]:
        """Return compact report-level summary statistics."""
        symbols: list[JSONValue] = []
        symbols.extend(
            sorted({profile.symbol for profile in self.period_profiles})
        )
        return {
            "operation": FEED_REGIME_OPERATION,
            "target_count": self.discovery.target_count,
            "supported_target_count": sum(
                1
                for target in self.discovery.targets
                if target.is_supported_tick_target
            ),
            "profile_count": len(self.period_profiles),
            "regime_count": len(self.regimes),
            "symbols": symbols,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible report payload."""
        return {
            "schema_version": ANALYTICS_REPORT_SCHEMA_VERSION,
            "operation": FEED_REGIME_OPERATION,
            "summary": self.summary(),
            "discovery": self.discovery.to_dict(),
            "period_profiles": [
                profile.to_dict() for profile in self.period_profiles
            ],
            "regimes": [regime.to_dict() for regime in self.regimes],
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class _TickObservation:
    utc_ms: int
    bid: float
    ask: float
    symbol: str
    target_path: str


def discover_analytics_targets(
    paths: Iterable[str | Path],
) -> AnalyticsDiscoveryResult:
    """Discover local files usable by data-analytics operations."""
    roots = _normalize_roots(paths)
    seen_paths: set[str] = set()
    targets: list[AnalyticsTarget] = []
    for root in roots:
        if not root.exists():
            raise ValueError(f"analytics target path does not exist: {root}")
        candidates = (
            sorted(path for path in root.rglob("*") if path.is_file())
            if root.is_dir()
            else [root]
        )
        for candidate in candidates:
            target = analytics_target_from_path(candidate)
            if target is None:
                if root == candidate:
                    raise ValueError(
                        f"unsupported analytics target file type: {candidate}"
                    )
                continue
            resolved = str(candidate.resolve())
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            targets.append(target)

    return AnalyticsDiscoveryResult(
        roots=tuple(str(root) for root in roots),
        targets=tuple(sorted(targets, key=lambda target: target.path)),
        metadata={
            "operation": FEED_REGIME_OPERATION,
            "supported_timeframe": TICK,
            "quality_semantics": "analytics-only; no pass/fail status",
        },
    )


def analytics_target_from_path(path: str | Path) -> AnalyticsTarget | None:
    """Return a local analytics target for supported artifact paths."""
    source = Path(path)
    kind = _target_kind(source)
    if kind is None:
        return None
    metadata = _metadata_from_filename(source)
    metadata["filename"] = source.name
    metadata["supported_for_feed_regimes"] = (
        metadata.get("data_format") == _ASCII_FORMAT
        and metadata.get("timeframe") == TICK
    )
    return AnalyticsTarget(
        path=str(source.resolve()),
        kind=kind,
        data_format=str(metadata.get("data_format", "") or ""),
        timeframe=str(metadata.get("timeframe", "") or ""),
        symbol=str(metadata.get("symbol", "") or ""),
        period=str(metadata.get("period", "") or ""),
        metadata=metadata,
    )


def analyze_feed_regimes(
    paths: Iterable[str | Path],
    *,
    bucket: str = "month",
    quiet_gap_ms: int = DEFAULT_QUIET_GAP_MS,
) -> FeedRegimeReport:
    """Analyze tick-rate and quote-update regimes across local targets."""
    normalized_bucket = _normalize_bucket(bucket)
    discovery = discover_analytics_targets(paths)
    observations_by_bucket: dict[tuple[str, str], list[_TickObservation]]
    observations_by_bucket = defaultdict(list)
    for target in discovery.targets:
        for observation in _target_tick_observations(target):
            period = _period_for_observation(observation, normalized_bucket)
            observations_by_bucket[(observation.symbol, period)].append(
                observation
            )

    profiles = tuple(
        _profile_observations(
            symbol=symbol,
            period=period,
            bucket=normalized_bucket,
            observations=tuple(observations),
            quiet_gap_ms=quiet_gap_ms,
        )
        for (symbol, period), observations in sorted(
            observations_by_bucket.items()
        )
    )
    regimes = _segment_regimes(profiles)
    return FeedRegimeReport(
        discovery=discovery,
        period_profiles=profiles,
        regimes=regimes,
        metadata={
            "quiet_gap_ms": quiet_gap_ms,
            "bucket": normalized_bucket,
            "quality_semantics": (
                "Feed-regime analytics are descriptive feature-engineering "
                "signals and do not imply data-quality pass/fail status."
            ),
        },
    )


def feed_regime_report_to_json(report: FeedRegimeReport) -> str:
    """Return deterministic formatted JSON for a feed-regime report."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def write_feed_regime_report(
    report: FeedRegimeReport,
    path: str | Path,
) -> ArtifactRef:
    """Write a feed-regime report and return its artifact reference."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = f"{feed_regime_report_to_json(report)}\n".encode("utf-8")
    output.write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    return ArtifactRef(
        kind="feed-regime-report",
        path=str(output.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "schema_version": ANALYTICS_REPORT_SCHEMA_VERSION,
            "operation": FEED_REGIME_OPERATION,
            "target_count": report.discovery.target_count,
            "profile_count": len(report.period_profiles),
            "regime_count": len(report.regimes),
        },
    )


def format_feed_regime_console_summary(
    report: FeedRegimeReport,
    *,
    artifact: ArtifactRef | None = None,
) -> str:
    """Return a compact human-readable feed-regime summary."""
    summary = report.summary()
    lines = [
        "Feed regime analytics",
        f"targets: {summary['target_count']}",
        f"supported tick targets: {summary['supported_target_count']}",
        f"profiles: {summary['profile_count']}",
        f"regimes: {summary['regime_count']}",
    ]
    if artifact is not None:
        lines.append(f"report: {artifact.path}")
    if not report.period_profiles:
        lines.append("No supported ASCII tick targets discovered.")
        return "\n".join(lines)

    lines.extend(("", "Regime eras"))
    for regime in report.regimes:
        lines.append(
            "- "
            f"{regime.symbol} {regime.period_start}-{regime.period_end} "
            f"{regime.label} "
            f"rate={_round_float(regime.mean_tick_rate_per_hour)}/hour "
            f"median_gap={_round_float(regime.median_interarrival_ms)}ms"
        )
    return "\n".join(lines)


def _normalize_roots(paths: Iterable[str | Path]) -> tuple[Path, ...]:
    roots = tuple(Path(path).expanduser() for path in paths)
    if not roots:
        raise ValueError("at least one analytics target path is required")
    return roots


def _target_kind(path: Path) -> str | None:
    if path.name == CACHE_FILENAME:
        return "cache"
    suffix = path.suffix.lower()
    if suffix == _CSV_SUFFIX:
        return "csv"
    if suffix == _ZIP_SUFFIX:
        return "zip"
    return None


def _metadata_from_filename(path: Path) -> dict[str, JSONValue]:
    if path.name == CACHE_FILENAME:
        return {
            "data_format": "",
            "timeframe": "",
            "symbol": "",
            "period": "",
            "cache_metadata_required": True,
        }
    stem = path.stem if path.suffix.lower() == _ZIP_SUFFIX else path.name
    match = _HISTDATA_DATA_FILENAME_RE.match(path.name)
    if match is None:
        match = _HISTDATA_ARCHIVE_FILENAME_RE.match(stem)
    if match is None:
        return {
            "data_format": "",
            "timeframe": "",
            "symbol": "",
            "period": "",
        }
    groups = match.groupdict()
    return {
        "data_format": str(groups.get("format", "")).lower(),
        "timeframe": str(groups.get("timeframe", "")).upper(),
        "symbol": str(groups.get("symbol", "")).upper(),
        "period": str(groups.get("period", "")),
    }


def _target_tick_observations(
    target: AnalyticsTarget,
) -> tuple[_TickObservation, ...]:
    if not target.is_supported_tick_target:
        return ()
    if target.kind == "cache":
        return _cache_tick_observations(target)
    batch = read_ascii_file(Path(target.path), target.timeframe)
    return tuple(
        _TickObservation(
            utc_ms=int(row[0]),
            bid=float(row[1]),
            ask=float(row[2]),
            symbol=target.symbol or "UNKNOWN",
            target_path=target.path,
        )
        for row in batch.rows
    )


def _cache_tick_observations(
    target: AnalyticsTarget,
) -> tuple[_TickObservation, ...]:
    frame = read_polars_cache(Path(target.path))
    required = {"datetime", "bid", "ask"}
    if not required.issubset(set(frame.columns)):
        return ()
    symbol = target.symbol or "UNKNOWN"
    return tuple(
        _TickObservation(
            utc_ms=int(row["datetime"]),
            bid=float(row["bid"]),
            ask=float(row["ask"]),
            symbol=symbol,
            target_path=target.path,
        )
        for row in frame.select(["datetime", "bid", "ask"]).iter_rows(
            named=True
        )
    )


def _normalize_bucket(bucket: str) -> str:
    normalized = str(bucket or "month").strip().lower()
    if normalized not in {"month", "year"}:
        raise ValueError("feed-regime bucket must be 'month' or 'year'")
    return normalized


def _period_for_observation(
    observation: _TickObservation,
    bucket: str,
) -> str:
    source = _source_datetime(observation.utc_ms)
    if bucket == "year":
        return f"{source.year:04d}"
    return f"{source.year:04d}{source.month:02d}"


def _profile_observations(
    *,
    symbol: str,
    period: str,
    bucket: str,
    observations: Sequence[_TickObservation],
    quiet_gap_ms: int,
) -> FeedPeriodProfile:
    ordered = tuple(sorted(observations, key=lambda item: item.utc_ms))
    timestamps = [item.utc_ms for item in ordered]
    deltas = [
        max(0, current - previous)
        for previous, current in zip(timestamps, timestamps[1:], strict=False)
    ]
    spreads = [item.ask - item.bid for item in ordered]
    quote_update_count = _quote_update_count(ordered)
    zero_runs, zero_ticks = _zero_change_runs(ordered)
    duration_ms = max(0, timestamps[-1] - timestamps[0]) if timestamps else 0
    rate = len(ordered) * 3_600_000 / duration_ms if duration_ms > 0 else 0.0
    return FeedPeriodProfile(
        symbol=symbol,
        period=period,
        bucket=bucket,
        row_count=len(ordered),
        start_utc_ms=timestamps[0] if timestamps else 0,
        end_utc_ms=timestamps[-1] if timestamps else 0,
        tick_rate_per_hour=rate,
        median_interarrival_ms=_median(deltas),
        p95_interarrival_ms=_percentile(deltas, 95),
        max_interarrival_ms=max(deltas) if deltas else 0,
        quiet_gap_count=sum(1 for delta in deltas if delta >= quiet_gap_ms),
        quote_update_count=quote_update_count,
        quote_update_ratio=(
            quote_update_count / max(1, len(ordered) - 1) if ordered else 0.0
        ),
        zero_change_run_count=zero_runs,
        zero_change_tick_count=zero_ticks,
        spread_min=min(spreads) if spreads else 0.0,
        spread_median=_median(spreads),
        spread_mean=(sum(spreads) / len(spreads) if spreads else 0.0),
        spread_max=max(spreads) if spreads else 0.0,
        session_counts=_session_counts(ordered),
        target_paths=tuple(sorted({item.target_path for item in ordered})),
    )


def _segment_regimes(
    profiles: Sequence[FeedPeriodProfile],
) -> tuple[FeedRegimeEra, ...]:
    grouped: dict[str, list[FeedPeriodProfile]] = defaultdict(list)
    for profile in profiles:
        grouped[profile.symbol].append(profile)

    regimes: list[FeedRegimeEra] = []
    for symbol, symbol_profiles in sorted(grouped.items()):
        ordered = sorted(symbol_profiles, key=lambda item: item.period)
        labels = _regime_labels(ordered)
        current: list[FeedPeriodProfile] = []
        current_label = ""
        for profile, label in zip(ordered, labels, strict=True):
            if current and label != current_label:
                regimes.append(_era(symbol, current_label, tuple(current)))
                current = []
            current_label = label
            current.append(profile)
        if current:
            regimes.append(_era(symbol, current_label, tuple(current)))
    return tuple(regimes)


def _regime_labels(profiles: Sequence[FeedPeriodProfile]) -> tuple[str, ...]:
    if len(profiles) <= 1:
        return tuple("stable" for _ in profiles)
    rates = [profile.tick_rate_per_hour for profile in profiles]
    max_rate = max(rates)
    min_positive = min((rate for rate in rates if rate > 0), default=0.0)
    if max_rate <= 0 or (min_positive and max_rate / min_positive < 1.5):
        return tuple("stable" for _ in profiles)
    labels: list[str] = []
    for rate in rates:
        ratio = rate / max_rate if max_rate else 0.0
        if ratio < 0.25:
            labels.append("sparse")
        elif ratio < 0.70:
            labels.append("transitional")
        else:
            labels.append("dense")
    return tuple(labels)


def _era(
    symbol: str,
    label: str,
    profiles: Sequence[FeedPeriodProfile],
) -> FeedRegimeEra:
    rates = [profile.tick_rate_per_hour for profile in profiles]
    gaps = [profile.median_interarrival_ms for profile in profiles]
    updates = [profile.quote_update_ratio for profile in profiles]
    return FeedRegimeEra(
        symbol=symbol,
        label=label,
        bucket=profiles[0].bucket,
        period_start=profiles[0].period,
        period_end=profiles[-1].period,
        start_utc_ms=min(profile.start_utc_ms for profile in profiles),
        end_utc_ms=max(profile.end_utc_ms for profile in profiles),
        profile_count=len(profiles),
        row_count=sum(profile.row_count for profile in profiles),
        mean_tick_rate_per_hour=sum(rates) / len(rates),
        median_interarrival_ms=_median(gaps),
        quote_update_ratio=sum(updates) / len(updates),
        quiet_gap_count=sum(profile.quiet_gap_count for profile in profiles),
        metadata={
            "periods": [profile.period for profile in profiles],
            "rate_min": _round_float(min(rates) if rates else 0.0),
            "rate_max": _round_float(max(rates) if rates else 0.0),
        },
    )


def _quote_update_count(observations: Sequence[_TickObservation]) -> int:
    return sum(
        1
        for previous, current in zip(
            observations,
            observations[1:],
            strict=False,
        )
        if previous.bid != current.bid or previous.ask != current.ask
    )


def _zero_change_runs(
    observations: Sequence[_TickObservation],
) -> tuple[int, int]:
    runs = 0
    ticks = 0
    current_run = 1
    for previous, current in zip(
        observations,
        observations[1:],
        strict=False,
    ):
        if previous.bid == current.bid and previous.ask == current.ask:
            current_run += 1
            continue
        if current_run > 1:
            runs += 1
            ticks += current_run
        current_run = 1
    if current_run > 1:
        runs += 1
        ticks += current_run
    return runs, ticks


def _session_counts(
    observations: Sequence[_TickObservation],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for observation in observations:
        sessions = _sessions_for_utc(_utc_datetime(observation.utc_ms))
        counts.update(sessions or ("market_closed",))
    return dict(counts)


def _sessions_for_utc(value: datetime) -> tuple[str, ...]:
    minute = value.hour * 60 + value.minute
    sessions: list[str] = []
    if _minute_in_window(minute, 0, 8 * 60):
        sessions.append("asia")
    if _minute_in_window(minute, 7 * 60, 16 * 60):
        sessions.append("london")
    if _minute_in_window(minute, 13 * 60, 22 * 60):
        sessions.append("new_york")
    return tuple(sessions)


def _minute_in_window(minute: int, start: int, end: int) -> bool:
    return start <= minute < end


def _source_datetime(utc_ms: int) -> datetime:
    return _utc_datetime(utc_ms) - timedelta(milliseconds=EST_NO_DST_OFFSET_MS)


def _utc_datetime(utc_ms: int) -> datetime:
    return datetime.fromtimestamp(utc_ms / 1000, tz=timezone.utc)


def _median(values: Sequence[float | int]) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _percentile(values: Sequence[float | int], percentile: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = max(
        0, min(len(ordered) - 1, ceil(percentile / 100 * len(ordered)) - 1)
    )
    return ordered[index]


def _round_float(value: float) -> float:
    return round(float(value), 6)

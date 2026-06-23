"""Tick bid/ask spread quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import zipfile

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.symbols import normalize_histdata_symbol
from histdatacom.histdata_ascii import (
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_TICK_SPREAD_RULE_ID = "ticks.ascii.spread"
ASCII_TICK_MICROSTRUCTURE_RULE_ID = "ticks.ascii.microstructure"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_TICK_SAMPLES = 5
TICK_PRICE_COLUMNS = ("bid", "ask")
DEFAULT_SESSION_PROFILE = "default"
DUPLICATE_TICK_OWNER_RULE_ID = "time.ascii.timestamp_sequence"
DUPLICATE_TICK_OWNER_FINDING_CODE = "ASCII_TICK_DUPLICATE_ROW"


@dataclass(frozen=True, slots=True)
class HistDataTickSpreadThresholds:
    """Configurable tick spread thresholds and severities."""

    zero_spread_run_length: int = 1

    def __post_init__(self) -> None:
        """Validate threshold values at construction time."""
        if self.zero_spread_run_length < 1:
            msg = "zero_spread_run_length must be at least 1"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold metadata."""
        return {"zero_spread_run_length": self.zero_spread_run_length}


DEFAULT_TICK_SPREAD_THRESHOLDS = HistDataTickSpreadThresholds()


@dataclass(frozen=True, slots=True)
class HistDataTickMicrostructureThresholds:
    """Configurable warning thresholds for tick microstructure checks."""

    stale_quote_run_length: int = 3
    stale_max_gap_ms: int = 60_000
    burst_max_interval_ms: int = 100
    burst_run_length: int = 3
    one_sided_run_length: int = 2

    def __post_init__(self) -> None:
        """Validate threshold values at construction time."""
        if self.stale_quote_run_length < 2:
            msg = "stale_quote_run_length must be at least 2"
            raise ValueError(msg)
        if self.stale_max_gap_ms < 0:
            msg = "stale_max_gap_ms must be non-negative"
            raise ValueError(msg)
        if self.burst_max_interval_ms < 0:
            msg = "burst_max_interval_ms must be non-negative"
            raise ValueError(msg)
        if self.burst_run_length < 2:
            msg = "burst_run_length must be at least 2"
            raise ValueError(msg)
        if self.one_sided_run_length < 1:
            msg = "one_sided_run_length must be at least 1"
            raise ValueError(msg)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold metadata."""
        return {
            "stale_quote_run_length": self.stale_quote_run_length,
            "stale_max_gap_ms": self.stale_max_gap_ms,
            "burst_max_interval_ms": self.burst_max_interval_ms,
            "burst_run_length": self.burst_run_length,
            "one_sided_run_length": self.one_sided_run_length,
        }


DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS = HistDataTickMicrostructureThresholds()


@dataclass(frozen=True, slots=True)
class _TickMicrostructureThresholdSelection:
    thresholds: HistDataTickMicrostructureThresholds
    source: str
    symbol_key: str
    session_key: str
    profile_key: str

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible threshold selection metadata."""
        return {
            "source": self.source,
            "symbol_key": self.symbol_key,
            "session_key": self.session_key,
            "profile_key": self.profile_key,
            "values": self.thresholds.to_metadata(),
        }


@dataclass(frozen=True, slots=True)
class _TextPayload:
    data: bytes
    source_member: str = ""


@dataclass(frozen=True, slots=True)
class _SourceReadError(Exception):
    code: str
    message: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class _TickSpreadSample:
    row_number: int
    timestamp_source: str
    timestamp_utc_ms: int | None
    column: str
    bid: float | None
    ask: float | None
    spread: float | None
    raw_values: tuple[str, ...]
    source_member: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text, when available."""
        if self.timestamp_utc_ms is None:
            return ""
        return _utc_iso_from_ms(self.timestamp_utc_ms)

    def values_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible bid/ask/spread values."""
        return {
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.spread,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible sample."""
        return {
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "utc_timestamp": self.utc_timestamp,
            "column": self.column,
            "values": self.values_metadata(),
            "raw_values": list(self.raw_values),
            "source_member": self.source_member,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class _ZeroSpreadRunSample:
    start: _TickSpreadSample
    end: _TickSpreadSample
    run_length: int

    @property
    def row_number(self) -> int:
        """Return the first row number in the run."""
        return self.start.row_number

    @property
    def timestamp_source(self) -> str:
        """Return the first source timestamp in the run."""
        return self.start.timestamp_source

    @property
    def timestamp_utc_ms(self) -> int | None:
        """Return the first normalized timestamp in the run."""
        return self.start.timestamp_utc_ms

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the run start."""
        return self.start.utc_timestamp

    @property
    def source_member(self) -> str:
        """Return source ZIP member context, when available."""
        return self.start.source_member

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible zero-spread run context."""
        return {
            "run_start_row_number": self.start.row_number,
            "run_end_row_number": self.end.row_number,
            "run_length": self.run_length,
            "run_start_timestamp_source": self.start.timestamp_source,
            "run_end_timestamp_source": self.end.timestamp_source,
            "run_start_timestamp_utc_ms": self.start.timestamp_utc_ms,
            "run_end_timestamp_utc_ms": self.end.timestamp_utc_ms,
            "run_start_utc_timestamp": self.start.utc_timestamp,
            "run_end_utc_timestamp": self.end.utc_timestamp,
            "values": self.start.values_metadata(),
            "source_member": self.source_member,
        }


@dataclass(frozen=True, slots=True)
class _TickMicrostructureRunSample:
    start: _TickSpreadSample
    end: _TickSpreadSample
    run_length: int
    metric: str
    direction: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def row_number(self) -> int:
        """Return the first row number in the run."""
        return self.start.row_number

    @property
    def timestamp_source(self) -> str:
        """Return the first source timestamp in the run."""
        return self.start.timestamp_source

    @property
    def timestamp_utc_ms(self) -> int | None:
        """Return the first normalized timestamp in the run."""
        return self.start.timestamp_utc_ms

    @property
    def utc_timestamp(self) -> str:
        """Return canonical UTC timestamp text for the run start."""
        return self.start.utc_timestamp

    @property
    def source_member(self) -> str:
        """Return source ZIP member context, when available."""
        return self.start.source_member

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible run context."""
        return {
            "run_start_row_number": self.start.row_number,
            "run_end_row_number": self.end.row_number,
            "run_length": self.run_length,
            "metric": self.metric,
            "direction": self.direction,
            "run_start_timestamp_source": self.start.timestamp_source,
            "run_end_timestamp_source": self.end.timestamp_source,
            "run_start_timestamp_utc_ms": self.start.timestamp_utc_ms,
            "run_end_timestamp_utc_ms": self.end.timestamp_utc_ms,
            "run_start_utc_timestamp": self.start.utc_timestamp,
            "run_end_utc_timestamp": self.end.utc_timestamp,
            "start_values": self.start.values_metadata(),
            "end_values": self.end.values_metadata(),
            "source_member": self.source_member,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class _TickSpreadScan:
    row_count: int = 0
    parsed_row_count: int = 0
    missing_bid_ask_count: int = 0
    invalid_bid_ask_count: int = 0
    negative_spread_count: int = 0
    zero_spread_count: int = 0
    zero_spread_run_count: int = 0
    min_spread: float | None = None
    max_spread: float | None = None
    missing_bid_ask: list[_TickSpreadSample] = field(default_factory=list)
    invalid_bid_ask: list[_TickSpreadSample] = field(default_factory=list)
    negative_spreads: list[_TickSpreadSample] = field(default_factory=list)
    zero_spread_runs: list[_ZeroSpreadRunSample] = field(default_factory=list)


@dataclass(slots=True)
class _TickMicrostructureScan:
    row_count: int = 0
    parsed_row_count: int = 0
    invalid_tick_count: int = 0
    duplicate_row_count: int = 0
    stale_quote_repeat_count: int = 0
    stale_quote_run_count: int = 0
    stale_quote_run_row_count: int = 0
    burst_interval_count: int = 0
    burst_run_count: int = 0
    burst_tick_count: int = 0
    one_sided_movement_count: int = 0
    one_sided_run_count: int = 0
    bid_only_movement_count: int = 0
    ask_only_movement_count: int = 0
    duplicate_rows: list[_TickSpreadSample] = field(default_factory=list)
    stale_quote_runs: list[_TickMicrostructureRunSample] = field(
        default_factory=list
    )
    burst_runs: list[_TickMicrostructureRunSample] = field(default_factory=list)
    one_sided_runs: list[_TickMicrostructureRunSample] = field(
        default_factory=list
    )


@dataclass(slots=True)
class HistDataAsciiTickSpreadRule:
    """Validate tick bid/ask ordering and zero-spread regimes."""

    thresholds: HistDataTickSpreadThresholds = DEFAULT_TICK_SPREAD_THRESHOLDS
    zero_spread_severity: QualitySeverity = QualitySeverity.WARNING
    negative_spread_severity: QualitySeverity = QualitySeverity.ERROR
    schema_severity: QualitySeverity = QualitySeverity.ERROR
    rule_id: str = ASCII_TICK_SPREAD_RULE_ID
    description: str = (
        "Validate HistData tick bid/ask parseability, ask-bid spread "
        "non-negativity, and zero-spread runs."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return tick spread findings for one target."""
        if not _is_tick_ascii_text_target(target):
            return ()

        try:
            delimiter = delimiter_for_timeframe(TICK)
            columns = columns_for_timeframe(TICK)
            payload = _read_text_payload(target)
            text = payload.data.decode("utf-8")
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_SPREAD_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe supported "
                    "HistData tick ASCII data.",
                    metadata={"timeframe": target.timeframe, "error": str(exc)},
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_SPREAD_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "tick spread checks.",
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": payload.source_member,
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code,
                    message=exc.message,
                    metadata=exc.metadata,
                ),
            )

        scan = _scan_tick_spread_rows(
            text,
            target=target,
            delimiter=delimiter,
            columns=columns,
            source_member=payload.source_member,
            thresholds=self.thresholds,
        )
        return _spread_findings(
            target=target,
            scan=scan,
            source_member=payload.source_member,
            thresholds=self.thresholds,
            zero_spread_severity=self.zero_spread_severity,
            negative_spread_severity=self.negative_spread_severity,
            schema_severity=self.schema_severity,
            rule_id=self.rule_id,
        )


@dataclass(slots=True)
class HistDataAsciiTickMicrostructureRule:
    """Profile tick duplicate, stale, burst, and one-sided quote patterns."""

    thresholds: HistDataTickMicrostructureThresholds = (
        DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS
    )
    thresholds_by_symbol: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(default_factory=dict)
    thresholds_by_session: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(default_factory=dict)
    thresholds_by_symbol_session: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ] = field(default_factory=dict)
    session_name: str = DEFAULT_SESSION_PROFILE
    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = ASCII_TICK_MICROSTRUCTURE_RULE_ID
    description: str = (
        "Profile HistData tick microstructure anomalies including exact "
        "duplicate row summaries, stale quotes, bursts, and one-sided "
        "bid/ask movement."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return tick microstructure findings for one target."""
        if not _is_tick_ascii_text_target(target):
            return ()

        try:
            delimiter = delimiter_for_timeframe(TICK)
            columns = columns_for_timeframe(TICK)
            payload = _read_text_payload(target)
            text = payload.data.decode("utf-8")
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_MICROSTRUCTURE_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe supported "
                    "HistData tick ASCII data.",
                    rule_id=self.rule_id,
                    metadata={"timeframe": target.timeframe, "error": str(exc)},
                ),
            )
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TICK_MICROSTRUCTURE_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8 for "
                    "tick microstructure checks.",
                    rule_id=self.rule_id,
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": payload.source_member,
                    },
                ),
            )
        except _SourceReadError as exc:
            return (
                _finding(
                    target,
                    code=exc.code.replace("SPREAD", "MICROSTRUCTURE"),
                    message=exc.message.replace(
                        "spread checks",
                        "microstructure checks",
                    ),
                    rule_id=self.rule_id,
                    metadata=exc.metadata,
                ),
            )

        threshold_selection = _microstructure_threshold_selection(
            target,
            thresholds=self.thresholds,
            thresholds_by_symbol=self.thresholds_by_symbol,
            thresholds_by_session=self.thresholds_by_session,
            thresholds_by_symbol_session=self.thresholds_by_symbol_session,
            session_name=self.session_name,
        )
        scan = _scan_tick_microstructure_rows(
            text,
            delimiter=delimiter,
            columns=columns,
            source_member=payload.source_member,
            thresholds=threshold_selection.thresholds,
        )
        return _microstructure_findings(
            target=target,
            scan=scan,
            source_member=payload.source_member,
            threshold_selection=threshold_selection,
            severity=self.warning_severity,
            rule_id=self.rule_id,
        )


def ticks_quality_rules() -> tuple[QualityRule, ...]:
    """Return tick quality rules in deterministic execution order."""
    spread_rule: QualityRule = HistDataAsciiTickSpreadRule()
    microstructure_rule: QualityRule = HistDataAsciiTickMicrostructureRule()
    return (spread_rule, microstructure_rule)


def _is_tick_ascii_text_target(target: QualityTarget) -> bool:
    return (
        target.data_format == "ascii"
        and target.timeframe == TICK
        and target.kind in {QualityTargetKind.CSV, QualityTargetKind.ZIP}
    )


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        try:
            return _TextPayload(path.read_bytes())
        except OSError as exc:
            raise _source_error(
                "ASCII_TICK_SPREAD_SOURCE_UNREADABLE",
                "ASCII file could not be read for tick spread checks.",
                exc,
            ) from exc

    try:
        with zipfile.ZipFile(path) as archive:
            members = tuple(
                name
                for name in archive.namelist()
                if not name.endswith("/")
                and Path(name).suffix.lower() == ".csv"
            )
            if len(members) != 1:
                raise _SourceReadError(
                    code="ASCII_TICK_SPREAD_ZIP_MEMBER_UNAVAILABLE",
                    message="ZIP archive must contain exactly one CSV member "
                    "for tick spread checks.",
                    metadata={"csv_members": list(members)},
                )
            member = members[0]
            return _TextPayload(
                data=archive.read(member),
                source_member=member,
            )
    except _SourceReadError:
        raise
    except zipfile.BadZipFile as exc:
        raise _source_error(
            "ASCII_TICK_SPREAD_ZIP_UNREADABLE",
            "ZIP archive could not be opened for tick spread checks.",
            exc,
        ) from exc
    except (KeyError, OSError) as exc:
        raise _source_error(
            "ASCII_TICK_SPREAD_SOURCE_UNREADABLE",
            "ASCII source could not be read for tick spread checks.",
            exc,
        ) from exc


def _source_error(
    code: str,
    message: str,
    exc: Exception,
) -> _SourceReadError:
    return _SourceReadError(
        code=code,
        message=message,
        metadata={"error_type": type(exc).__name__, "error": str(exc)},
    )


def _scan_tick_spread_rows(
    text: str,
    *,
    target: QualityTarget,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    thresholds: HistDataTickSpreadThresholds,
) -> _TickSpreadScan:
    scan = _TickSpreadScan()
    expected_count = len(columns)
    zero_run_start: _TickSpreadSample | None = None
    zero_run_end: _TickSpreadSample | None = None
    zero_run_length = 0

    for row_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        row = _parse_row(raw, delimiter)
        if tuple(row) == columns:
            continue

        missing_column = _missing_bid_ask_column(row, expected_count)
        if missing_column:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            scan.missing_bid_ask_count += 1
            _append_spread_sample(
                scan.missing_bid_ask,
                _sample_from_row(
                    row,
                    row_number=row_number,
                    column=missing_column,
                    source_member=source_member,
                    metadata={
                        "expected_field_count": expected_count,
                        "field_count": len(row),
                        "required_columns": list(TICK_PRICE_COLUMNS),
                    },
                ),
            )
            continue

        parsed = _parsed_tick_spread_sample(
            row,
            row_number=row_number,
            source_member=source_member,
        )
        if parsed is None:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            scan.invalid_bid_ask_count += 1
            _append_spread_sample(
                scan.invalid_bid_ask,
                _invalid_bid_ask_sample(
                    row,
                    row_number=row_number,
                    source_member=source_member,
                ),
            )
            continue

        scan.parsed_row_count += 1
        spread = parsed.spread
        if spread is not None:
            scan.min_spread = (
                spread
                if scan.min_spread is None
                else min(scan.min_spread, spread)
            )
            scan.max_spread = (
                spread
                if scan.max_spread is None
                else max(scan.max_spread, spread)
            )

        if spread is not None and spread < 0.0:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0
            scan.negative_spread_count += 1
            _append_spread_sample(scan.negative_spreads, parsed)
        elif spread == 0.0:
            scan.zero_spread_count += 1
            if zero_run_start is None:
                zero_run_start = parsed
            zero_run_end = parsed
            zero_run_length += 1
        else:
            _finalize_zero_spread_run(
                scan,
                zero_run_start,
                zero_run_end,
                zero_run_length,
                thresholds,
            )
            zero_run_start = None
            zero_run_end = None
            zero_run_length = 0

    _finalize_zero_spread_run(
        scan,
        zero_run_start,
        zero_run_end,
        zero_run_length,
        thresholds,
    )
    return scan


def _scan_tick_microstructure_rows(
    text: str,
    *,
    delimiter: str,
    columns: tuple[str, ...],
    source_member: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> _TickMicrostructureScan:
    scan = _TickMicrostructureScan()
    expected_count = len(columns)
    previous: _TickSpreadSample | None = None
    seen_rows: dict[tuple[str, ...], _TickSpreadSample] = {}

    stale_start: _TickSpreadSample | None = None
    stale_end: _TickSpreadSample | None = None
    stale_length = 0

    burst_start: _TickSpreadSample | None = None
    burst_end: _TickSpreadSample | None = None
    burst_length = 0

    one_sided_start: _TickSpreadSample | None = None
    one_sided_end: _TickSpreadSample | None = None
    one_sided_previous: _TickSpreadSample | None = None
    one_sided_length = 0
    one_sided_direction = ""

    for row_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        row = _parse_row(raw, delimiter)
        if tuple(row) == columns:
            continue

        parsed = (
            None
            if _missing_bid_ask_column(row, expected_count)
            else _parsed_tick_spread_sample(
                row,
                row_number=row_number,
                source_member=source_member,
            )
        )
        if parsed is None:
            (
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
            ) = _finalize_microstructure_runs(
                scan,
                stale_start,
                stale_end,
                stale_length,
                burst_start,
                burst_end,
                burst_length,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                thresholds,
            )
            scan.invalid_tick_count += 1
            previous = None
            continue

        scan.parsed_row_count += 1
        duplicate = seen_rows.get(parsed.raw_values)
        if duplicate is not None:
            scan.duplicate_row_count += 1
            _append_spread_sample(
                scan.duplicate_rows,
                _duplicate_microstructure_sample(parsed, duplicate),
            )
        else:
            seen_rows[parsed.raw_values] = parsed

        if previous is None:
            previous = parsed
            continue

        interval_ms = _tick_interval_ms(previous, parsed)
        if _is_stale_quote_pair(previous, parsed, interval_ms, thresholds):
            scan.stale_quote_repeat_count += 1
            if stale_start is None:
                stale_start = previous
                stale_length = 2
            else:
                stale_length += 1
            stale_end = parsed
        else:
            _finalize_stale_quote_run(
                scan,
                stale_start,
                stale_end,
                stale_length,
                thresholds,
            )
            stale_start = None
            stale_end = None
            stale_length = 0

        if _is_burst_interval(interval_ms, thresholds):
            scan.burst_interval_count += 1
            if burst_start is None:
                burst_start = previous
                burst_length = 2
            else:
                burst_length += 1
            burst_end = parsed
        else:
            _finalize_burst_run(
                scan,
                burst_start,
                burst_end,
                burst_length,
                thresholds,
            )
            burst_start = None
            burst_end = None
            burst_length = 0

        direction = _one_sided_quote_direction(previous, parsed)
        if direction:
            scan.one_sided_movement_count += 1
            if direction == "bid_only":
                scan.bid_only_movement_count += 1
            else:
                scan.ask_only_movement_count += 1

            if one_sided_start is not None and (
                one_sided_direction == direction
            ):
                one_sided_length += 1
                one_sided_end = parsed
            else:
                _finalize_one_sided_run(
                    scan,
                    one_sided_start,
                    one_sided_end,
                    one_sided_previous,
                    one_sided_length,
                    one_sided_direction,
                    thresholds,
                )
                one_sided_start = parsed
                one_sided_end = parsed
                one_sided_previous = previous
                one_sided_length = 1
                one_sided_direction = direction
        else:
            _finalize_one_sided_run(
                scan,
                one_sided_start,
                one_sided_end,
                one_sided_previous,
                one_sided_length,
                one_sided_direction,
                thresholds,
            )
            one_sided_start = None
            one_sided_end = None
            one_sided_previous = None
            one_sided_length = 0
            one_sided_direction = ""

        previous = parsed

    _finalize_microstructure_runs(
        scan,
        stale_start,
        stale_end,
        stale_length,
        burst_start,
        burst_end,
        burst_length,
        one_sided_start,
        one_sided_end,
        one_sided_previous,
        one_sided_length,
        one_sided_direction,
        thresholds,
    )
    return scan


def _parsed_tick_spread_sample(
    row: list[str],
    *,
    row_number: int,
    source_member: str,
) -> _TickSpreadSample | None:
    values = tuple(cell.strip() for cell in row)
    try:
        bid = float(values[1])
        ask = float(values[2])
    except (ValueError, IndexError):
        return None
    if not math.isfinite(bid) or not math.isfinite(ask):
        return None

    timestamp_source = values[0]
    timestamp_utc_ms = _timestamp_utc_ms_or_none(timestamp_source)
    spread = ask - bid
    return _TickSpreadSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=timestamp_utc_ms,
        column="ask" if spread < 0.0 else "spread",
        bid=bid,
        ask=ask,
        spread=spread,
        raw_values=values,
        source_member=source_member,
    )


def _sample_from_row(
    row: list[str],
    *,
    row_number: int,
    column: str,
    source_member: str,
    metadata: dict[str, JSONValue] | None = None,
) -> _TickSpreadSample:
    values = tuple(cell.strip() for cell in row)
    timestamp_source = values[0] if values else ""
    return _TickSpreadSample(
        row_number=row_number,
        timestamp_source=timestamp_source,
        timestamp_utc_ms=_timestamp_utc_ms_or_none(timestamp_source),
        column=column,
        bid=_float_or_none(values[1] if len(values) > 1 else ""),
        ask=_float_or_none(values[2] if len(values) > 2 else ""),
        spread=None,
        raw_values=values,
        source_member=source_member,
        metadata=dict(metadata or {}),
    )


def _invalid_bid_ask_sample(
    row: list[str],
    *,
    row_number: int,
    source_member: str,
) -> _TickSpreadSample:
    values = tuple(cell.strip() for cell in row)
    bid_raw = values[1] if len(values) > 1 else ""
    ask_raw = values[2] if len(values) > 2 else ""
    column = "bid" if _float_or_none(bid_raw) is None else "ask"
    return _sample_from_row(
        row,
        row_number=row_number,
        column=column,
        source_member=source_member,
        metadata={
            "raw_bid": bid_raw,
            "raw_ask": ask_raw,
            "error": "bid and ask must parse as finite decimal numbers",
        },
    )


def _missing_bid_ask_column(row: list[str], expected_count: int) -> str:
    if len(row) != expected_count:
        if len(row) <= 1:
            return "bid"
        if len(row) <= 2:
            return "ask"
        return "schema"
    if not row[1].strip():
        return "bid"
    if not row[2].strip():
        return "ask"
    return ""


def _finalize_zero_spread_run(
    scan: _TickSpreadScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    run_length: int,
    thresholds: HistDataTickSpreadThresholds,
) -> None:
    if start is None or end is None:
        return
    if run_length < thresholds.zero_spread_run_length:
        return
    scan.zero_spread_run_count += 1
    _append_zero_spread_run(
        scan.zero_spread_runs,
        _ZeroSpreadRunSample(
            start=start,
            end=end,
            run_length=run_length,
        ),
    )


def _finalize_microstructure_runs(
    scan: _TickMicrostructureScan,
    stale_start: _TickSpreadSample | None,
    stale_end: _TickSpreadSample | None,
    stale_length: int,
    burst_start: _TickSpreadSample | None,
    burst_end: _TickSpreadSample | None,
    burst_length: int,
    one_sided_start: _TickSpreadSample | None,
    one_sided_end: _TickSpreadSample | None,
    one_sided_previous: _TickSpreadSample | None,
    one_sided_length: int,
    one_sided_direction: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> tuple[
    None,
    None,
    int,
    None,
    None,
    int,
    None,
    None,
    None,
    int,
    str,
]:
    _finalize_stale_quote_run(
        scan,
        stale_start,
        stale_end,
        stale_length,
        thresholds,
    )
    _finalize_burst_run(
        scan,
        burst_start,
        burst_end,
        burst_length,
        thresholds,
    )
    _finalize_one_sided_run(
        scan,
        one_sided_start,
        one_sided_end,
        one_sided_previous,
        one_sided_length,
        one_sided_direction,
        thresholds,
    )
    return (None, None, 0, None, None, 0, None, None, None, 0, "")


def _finalize_stale_quote_run(
    scan: _TickMicrostructureScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    run_length: int,
    thresholds: HistDataTickMicrostructureThresholds,
) -> None:
    if start is None or end is None:
        return
    if run_length < thresholds.stale_quote_run_length:
        return
    scan.stale_quote_run_count += 1
    scan.stale_quote_run_row_count += run_length
    _append_microstructure_run(
        scan.stale_quote_runs,
        _TickMicrostructureRunSample(
            start=start,
            end=end,
            run_length=run_length,
            metric="stale_quote_rows",
            metadata={
                "stale_max_gap_ms": thresholds.stale_max_gap_ms,
                "threshold_run_length": thresholds.stale_quote_run_length,
            },
        ),
    )


def _finalize_burst_run(
    scan: _TickMicrostructureScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    run_length: int,
    thresholds: HistDataTickMicrostructureThresholds,
) -> None:
    if start is None or end is None:
        return
    if run_length < thresholds.burst_run_length:
        return
    scan.burst_run_count += 1
    scan.burst_tick_count += run_length
    _append_microstructure_run(
        scan.burst_runs,
        _TickMicrostructureRunSample(
            start=start,
            end=end,
            run_length=run_length,
            metric="burst_ticks",
            metadata={
                "burst_max_interval_ms": thresholds.burst_max_interval_ms,
                "threshold_run_length": thresholds.burst_run_length,
                "duration_ms": _tick_interval_ms(start, end),
            },
        ),
    )


def _finalize_one_sided_run(
    scan: _TickMicrostructureScan,
    start: _TickSpreadSample | None,
    end: _TickSpreadSample | None,
    previous: _TickSpreadSample | None,
    run_length: int,
    direction: str,
    thresholds: HistDataTickMicrostructureThresholds,
) -> None:
    if start is None or end is None or previous is None or not direction:
        return
    if run_length < thresholds.one_sided_run_length:
        return
    scan.one_sided_run_count += 1
    _append_microstructure_run(
        scan.one_sided_runs,
        _TickMicrostructureRunSample(
            start=start,
            end=end,
            run_length=run_length,
            metric="one_sided_quote_movements",
            direction=direction,
            metadata={
                "threshold_run_length": thresholds.one_sided_run_length,
                "previous_row_number": previous.row_number,
                "previous_timestamp_source": previous.timestamp_source,
                "previous_timestamp_utc_ms": previous.timestamp_utc_ms,
                "previous_values": previous.values_metadata(),
            },
        ),
    )


def _duplicate_microstructure_sample(
    sample: _TickSpreadSample,
    duplicate: _TickSpreadSample,
) -> _TickSpreadSample:
    return _TickSpreadSample(
        row_number=sample.row_number,
        timestamp_source=sample.timestamp_source,
        timestamp_utc_ms=sample.timestamp_utc_ms,
        column="row",
        bid=sample.bid,
        ask=sample.ask,
        spread=sample.spread,
        raw_values=sample.raw_values,
        source_member=sample.source_member,
        metadata={
            "duplicate_of_row": duplicate.row_number,
            "duplicate_timestamp_utc_ms": duplicate.timestamp_utc_ms,
            "dedupe_policy": "summary-only",
            "owner_rule_id": DUPLICATE_TICK_OWNER_RULE_ID,
            "owner_finding_code": DUPLICATE_TICK_OWNER_FINDING_CODE,
        },
    )


def _tick_interval_ms(
    previous: _TickSpreadSample,
    current: _TickSpreadSample,
) -> int | None:
    if previous.timestamp_utc_ms is None or current.timestamp_utc_ms is None:
        return None
    return current.timestamp_utc_ms - previous.timestamp_utc_ms


def _is_stale_quote_pair(
    previous: _TickSpreadSample,
    current: _TickSpreadSample,
    interval_ms: int | None,
    thresholds: HistDataTickMicrostructureThresholds,
) -> bool:
    return (
        interval_ms is not None
        and 0 <= interval_ms <= thresholds.stale_max_gap_ms
        and previous.bid == current.bid
        and previous.ask == current.ask
    )


def _is_burst_interval(
    interval_ms: int | None,
    thresholds: HistDataTickMicrostructureThresholds,
) -> bool:
    return (
        interval_ms is not None
        and 0 <= interval_ms <= thresholds.burst_max_interval_ms
    )


def _one_sided_quote_direction(
    previous: _TickSpreadSample,
    current: _TickSpreadSample,
) -> str:
    bid_changed = previous.bid != current.bid
    ask_changed = previous.ask != current.ask
    if bid_changed and not ask_changed:
        return "bid_only"
    if ask_changed and not bid_changed:
        return "ask_only"
    return ""


def _microstructure_threshold_selection(
    target: QualityTarget,
    *,
    thresholds: HistDataTickMicrostructureThresholds,
    thresholds_by_symbol: Mapping[str, HistDataTickMicrostructureThresholds],
    thresholds_by_session: Mapping[str, HistDataTickMicrostructureThresholds],
    thresholds_by_symbol_session: Mapping[
        str,
        HistDataTickMicrostructureThresholds,
    ],
    session_name: str,
) -> _TickMicrostructureThresholdSelection:
    symbol_key = normalize_histdata_symbol(target.symbol)
    session_key = _normalize_session_key(session_name)
    profile_key = f"{symbol_key}:{session_key}" if symbol_key else session_key

    symbol_session_profiles = _normalized_symbol_session_thresholds(
        thresholds_by_symbol_session
    )
    if profile_key in symbol_session_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=symbol_session_profiles[profile_key],
            source="symbol-session",
            symbol_key=symbol_key,
            session_key=session_key,
            profile_key=profile_key,
        )

    symbol_profiles = _normalized_symbol_thresholds(thresholds_by_symbol)
    if symbol_key in symbol_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=symbol_profiles[symbol_key],
            source="symbol",
            symbol_key=symbol_key,
            session_key=session_key,
            profile_key=symbol_key,
        )

    session_profiles = _normalized_session_thresholds(thresholds_by_session)
    if session_key in session_profiles:
        return _TickMicrostructureThresholdSelection(
            thresholds=session_profiles[session_key],
            source="session",
            symbol_key=symbol_key,
            session_key=session_key,
            profile_key=session_key,
        )

    return _TickMicrostructureThresholdSelection(
        thresholds=thresholds,
        source="default",
        symbol_key=symbol_key,
        session_key=session_key,
        profile_key=DEFAULT_SESSION_PROFILE,
    )


def _normalized_symbol_thresholds(
    thresholds: Mapping[str, HistDataTickMicrostructureThresholds],
) -> dict[str, HistDataTickMicrostructureThresholds]:
    return {
        normalize_histdata_symbol(symbol): value
        for symbol, value in thresholds.items()
        if normalize_histdata_symbol(symbol)
    }


def _normalized_session_thresholds(
    thresholds: Mapping[str, HistDataTickMicrostructureThresholds],
) -> dict[str, HistDataTickMicrostructureThresholds]:
    return {
        _normalize_session_key(session): value
        for session, value in thresholds.items()
        if _normalize_session_key(session)
    }


def _normalized_symbol_session_thresholds(
    thresholds: Mapping[str, HistDataTickMicrostructureThresholds],
) -> dict[str, HistDataTickMicrostructureThresholds]:
    normalized: dict[str, HistDataTickMicrostructureThresholds] = {}
    for key, value in thresholds.items():
        symbol_key, session_key = _split_symbol_session_key(key)
        if symbol_key and session_key:
            normalized[f"{symbol_key}:{session_key}"] = value
    return normalized


def _split_symbol_session_key(key: str) -> tuple[str, str]:
    raw = str(key or "").strip()
    separator = ":" if ":" in raw else "/"
    if separator not in raw:
        return "", ""
    symbol, session = raw.split(separator, maxsplit=1)
    return normalize_histdata_symbol(symbol), _normalize_session_key(session)


def _normalize_session_key(value: str) -> str:
    normalized = str(value or DEFAULT_SESSION_PROFILE).strip().lower()
    return normalized.replace(" ", "_") or DEFAULT_SESSION_PROFILE


def _spread_findings(
    *,
    target: QualityTarget,
    scan: _TickSpreadScan,
    source_member: str,
    thresholds: HistDataTickSpreadThresholds,
    zero_spread_severity: QualitySeverity,
    negative_spread_severity: QualitySeverity,
    schema_severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TICK_SPREAD_SUMMARY",
            message="Tick bid/ask spread integrity profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "row_count": scan.row_count,
                "parsed_row_count": scan.parsed_row_count,
                "missing_bid_ask_count": scan.missing_bid_ask_count,
                "invalid_bid_ask_count": scan.invalid_bid_ask_count,
                "negative_spread_count": scan.negative_spread_count,
                "zero_spread_count": scan.zero_spread_count,
                "zero_spread_run_count": scan.zero_spread_run_count,
                "min_spread": scan.min_spread,
                "max_spread": scan.max_spread,
                "thresholds": thresholds.to_metadata(),
            },
        )
    ]
    if scan.missing_bid_ask:
        findings.append(
            _spread_sample_finding(
                target,
                code="ASCII_TICK_BID_ASK_MISSING",
                message="Tick rows are missing one or more required bid/ask "
                "schema fields.",
                severity=schema_severity,
                samples=scan.missing_bid_ask,
                row_count=scan.missing_bid_ask_count,
                source_member=source_member,
                rule_id=rule_id,
            )
        )
    if scan.invalid_bid_ask:
        findings.append(
            _spread_sample_finding(
                target,
                code="ASCII_TICK_BID_ASK_INVALID",
                message="Tick bid/ask fields must parse as finite decimal "
                "numbers.",
                severity=schema_severity,
                samples=scan.invalid_bid_ask,
                row_count=scan.invalid_bid_ask_count,
                source_member=source_member,
                rule_id=rule_id,
            )
        )
    if scan.negative_spreads:
        findings.append(
            _spread_sample_finding(
                target,
                code="ASCII_TICK_NEGATIVE_SPREAD",
                message="Tick ask price is below bid price.",
                severity=negative_spread_severity,
                samples=scan.negative_spreads,
                row_count=scan.negative_spread_count,
                source_member=source_member,
                rule_id=rule_id,
            )
        )
    if scan.zero_spread_runs:
        findings.append(
            _zero_spread_run_finding(
                target,
                samples=scan.zero_spread_runs,
                zero_spread_count=scan.zero_spread_count,
                source_member=source_member,
                thresholds=thresholds,
                severity=zero_spread_severity,
                rule_id=rule_id,
            )
        )
    return tuple(findings)


def _spread_sample_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_TickSpreadSample],
    row_count: int,
    source_member: str,
    rule_id: str,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column=first.column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "values": first.values_metadata(),
                **dict(first.metadata),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            "row_count": row_count,
            "samples": _spread_samples(samples),
        },
    )


def _zero_spread_run_finding(
    target: QualityTarget,
    *,
    samples: list[_ZeroSpreadRunSample],
    zero_spread_count: int,
    source_member: str,
    thresholds: HistDataTickSpreadThresholds,
    severity: QualitySeverity,
    rule_id: str,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code="ASCII_TICK_ZERO_SPREAD_RUN",
        message="Tick rows contain one or more zero-spread runs.",
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column="spread",
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "run_length": first.run_length,
                "values": first.start.values_metadata(),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            "row_count": len(samples),
            "zero_spread_count": zero_spread_count,
            "thresholds": thresholds.to_metadata(),
            "samples": _zero_spread_run_samples(samples),
        },
    )


def _microstructure_findings(
    *,
    target: QualityTarget,
    scan: _TickMicrostructureScan,
    source_member: str,
    threshold_selection: _TickMicrostructureThresholdSelection,
    severity: QualitySeverity,
    rule_id: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = [
        _finding(
            target,
            code="ASCII_TICK_MICROSTRUCTURE_SUMMARY",
            message="Tick duplicate, stale, burst, and one-sided quote "
            "microstructure profile.",
            severity=QualitySeverity.INFO,
            rule_id=rule_id,
            metadata={
                **_base_metadata(target, source_member=source_member),
                "row_count": scan.row_count,
                "parsed_row_count": scan.parsed_row_count,
                "invalid_tick_count": scan.invalid_tick_count,
                "duplicate_row_count": scan.duplicate_row_count,
                "stale_quote_repeat_count": scan.stale_quote_repeat_count,
                "stale_quote_run_count": scan.stale_quote_run_count,
                "stale_quote_run_row_count": (scan.stale_quote_run_row_count),
                "burst_interval_count": scan.burst_interval_count,
                "burst_run_count": scan.burst_run_count,
                "burst_tick_count": scan.burst_tick_count,
                "one_sided_movement_count": (scan.one_sided_movement_count),
                "one_sided_run_count": scan.one_sided_run_count,
                "bid_only_movement_count": scan.bid_only_movement_count,
                "ask_only_movement_count": scan.ask_only_movement_count,
                "threshold_profile": threshold_selection.to_metadata(),
                "duplicate_detection": {
                    "summary_only": True,
                    "owner_rule_id": DUPLICATE_TICK_OWNER_RULE_ID,
                    "owner_finding_code": DUPLICATE_TICK_OWNER_FINDING_CODE,
                },
                "duplicate_samples": _spread_samples(scan.duplicate_rows),
            },
        )
    ]
    if scan.stale_quote_runs:
        findings.append(
            _microstructure_run_finding(
                target,
                code="ASCII_TICK_STALE_QUOTE_RUN",
                message="Tick bid/ask quotes are unchanged across a stale "
                "run inside the configured active-window gap.",
                severity=severity,
                samples=scan.stale_quote_runs,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="bid,ask",
                count_metadata={
                    "stale_quote_repeat_count": (scan.stale_quote_repeat_count),
                    "stale_quote_run_count": scan.stale_quote_run_count,
                    "stale_quote_run_row_count": (
                        scan.stale_quote_run_row_count
                    ),
                },
            )
        )
    if scan.burst_runs:
        findings.append(
            _microstructure_run_finding(
                target,
                code="ASCII_TICK_BURST_RUN",
                message="Tick timestamps contain one or more dense burst "
                "runs under the configured interval threshold.",
                severity=severity,
                samples=scan.burst_runs,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="datetime",
                count_metadata={
                    "burst_interval_count": scan.burst_interval_count,
                    "burst_run_count": scan.burst_run_count,
                    "burst_tick_count": scan.burst_tick_count,
                },
            )
        )
    if scan.one_sided_runs:
        findings.append(
            _microstructure_run_finding(
                target,
                code="ASCII_TICK_ONE_SIDED_QUOTE_RUN",
                message="Tick quotes contain one-sided bid/ask movement "
                "runs.",
                severity=severity,
                samples=scan.one_sided_runs,
                source_member=source_member,
                threshold_selection=threshold_selection,
                rule_id=rule_id,
                column="bid,ask",
                count_metadata={
                    "one_sided_movement_count": (scan.one_sided_movement_count),
                    "one_sided_run_count": scan.one_sided_run_count,
                    "bid_only_movement_count": (scan.bid_only_movement_count),
                    "ask_only_movement_count": (scan.ask_only_movement_count),
                },
            )
        )
    return tuple(findings)


def _microstructure_run_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    samples: list[_TickMicrostructureRunSample],
    source_member: str,
    threshold_selection: _TickMicrostructureThresholdSelection,
    rule_id: str,
    column: str,
    count_metadata: dict[str, JSONValue],
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        severity=severity,
        rule_id=rule_id,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            timestamp_source=first.timestamp_source,
            timestamp_utc_ms=first.timestamp_utc_ms,
            column=column,
            metadata={
                "source_timezone": SOURCE_TIMEZONE,
                "source_utc_offset": SOURCE_UTC_OFFSET,
                "utc_timestamp": first.utc_timestamp,
                "source_member": first.source_member,
                "run_length": first.run_length,
                "metric": first.metric,
                "direction": first.direction,
                "values": first.start.values_metadata(),
                **dict(first.metadata),
            },
        ),
        metadata={
            **_base_metadata(target, source_member=source_member),
            **count_metadata,
            "row_count": len(samples),
            "threshold_profile": threshold_selection.to_metadata(),
            "samples": _microstructure_run_samples(samples),
        },
    )


def _base_metadata(
    target: QualityTarget,
    *,
    source_member: str,
) -> dict[str, JSONValue]:
    return {
        "symbol": target.symbol,
        "timeframe": target.timeframe,
        "period": target.period,
        "data_format": target.data_format,
        "source_member": source_member,
        "source_timezone": SOURCE_TIMEZONE,
        "source_utc_offset": SOURCE_UTC_OFFSET,
        "canonical_timezone": CANONICAL_TIMEZONE,
        "price_columns": list(TICK_PRICE_COLUMNS),
        "quote_sides": list(TICK_PRICE_COLUMNS),
        "spread_definition": "ask - bid",
    }


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    rule_id: str = ASCII_TICK_SPREAD_RULE_ID,
    location: QualityLocation | None = None,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.from_value(severity),
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )


def _parse_row(raw: str, delimiter: str) -> list[str]:
    return next(csv.reader((raw,), delimiter=delimiter), [])


def _timestamp_utc_ms_or_none(value: str) -> int | None:
    if not value:
        return None
    try:
        return int(parse_histdata_datetime_to_utc_ms(value, TICK))
    except ValueError:
        return None


def _float_or_none(value: str) -> float | None:
    try:
        parsed = float(value.strip())
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _append_spread_sample(
    samples: list[_TickSpreadSample],
    sample: _TickSpreadSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(sample)


def _append_zero_spread_run(
    samples: list[_ZeroSpreadRunSample],
    sample: _ZeroSpreadRunSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(sample)


def _append_microstructure_run(
    samples: list[_TickMicrostructureRunSample],
    sample: _TickMicrostructureRunSample,
) -> None:
    if len(samples) < MAX_TICK_SAMPLES:
        samples.append(sample)


def _spread_samples(samples: Iterable[_TickSpreadSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _zero_spread_run_samples(
    samples: Iterable[_ZeroSpreadRunSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _microstructure_run_samples(
    samples: Iterable[_TickMicrostructureRunSample],
) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _utc_iso_from_ms(timestamp_utc_ms: int) -> str:
    seconds, milliseconds = divmod(timestamp_utc_ms, 1000)
    return (
        datetime.fromtimestamp(seconds, tz=timezone.utc)
        .replace(microsecond=milliseconds * 1000)
        .isoformat()
        .replace("+00:00", "Z")
    )

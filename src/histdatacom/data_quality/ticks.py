"""Tick bid/ask spread quality checks for HistData ASCII artifacts."""

from __future__ import annotations

import csv
import math
from collections.abc import Iterable
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
from histdatacom.histdata_ascii import (
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_TICK_SPREAD_RULE_ID = "ticks.ascii.spread"
SOURCE_TIMEZONE = "EST-no-DST"
SOURCE_UTC_OFFSET = "-05:00"
CANONICAL_TIMEZONE = "UTC"
MAX_TICK_SAMPLES = 5
TICK_PRICE_COLUMNS = ("bid", "ask")


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


def ticks_quality_rules() -> tuple[QualityRule, ...]:
    """Return tick quality rules in deterministic execution order."""
    spread_rule: QualityRule = HistDataAsciiTickSpreadRule()
    return (spread_rule,)


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


def _spread_samples(samples: Iterable[_TickSpreadSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _zero_spread_run_samples(
    samples: Iterable[_ZeroSpreadRunSample],
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

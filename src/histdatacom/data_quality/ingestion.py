"""Raw text ingestion checks for local HistData ASCII artifacts."""

from __future__ import annotations

import csv
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
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
    columns_for_timeframe,
    delimiter_for_timeframe,
    normalize_ascii_row,
    parse_histdata_datetime_to_utc_ms,
)
from histdatacom.runtime_contracts import JSONValue

ASCII_TEXT_INGESTION_RULE_ID = "ingestion.ascii.text"
ASCII_SCHEMA_INGESTION_RULE_ID = "ingestion.ascii.schema"
INT32_MAX = 2**31 - 1
MAX_ROW_SAMPLES = 5


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
class _RowSample:
    row_number: int
    field_count: int
    raw: str

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible row sample."""
        return {
            "row_number": self.row_number,
            "field_count": self.field_count,
            "raw": self.raw[:200],
        }


@dataclass(frozen=True, slots=True)
class _SchemaSample:
    row_number: int
    column: str
    raw_value: str
    error: str

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded JSON-compatible schema sample."""
        return {
            "row_number": self.row_number,
            "column": self.column,
            "raw_value": self.raw_value[:120],
            "error": self.error[:200],
        }


@dataclass(frozen=True, slots=True)
class _LineEndingScan:
    counts: dict[str, int]

    @property
    def used_styles(self) -> tuple[str, ...]:
        """Return line-ending styles present in the payload."""
        return tuple(style for style, count in self.counts.items() if count)

    @property
    def has_malformed(self) -> bool:
        """Return whether bare carriage returns were found."""
        return bool(self.counts["cr"])

    @property
    def is_inconsistent(self) -> bool:
        """Return whether multiple line-ending styles were found."""
        return len(self.used_styles) > 1

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible counts."""
        return dict(self.counts)


@dataclass(slots=True)
class _RowScan:
    row_count: int = 0
    header_row_number: int | None = None
    delimiter_samples: list[_RowSample] = field(default_factory=list)
    field_count_samples: list[_RowSample] = field(default_factory=list)
    delimiter_count: int = 0
    field_count_error_count: int = 0


@dataclass(slots=True)
class _SchemaScan:
    parsed_row_count: int = 0
    bad_timestamp_count: int = 0
    bad_numeric_count: int = 0
    bad_volume_count: int = 0
    shifted_row_count: int = 0
    invalid_row_count: int = 0
    bad_timestamps: list[_SchemaSample] = field(default_factory=list)
    bad_numerics: list[_SchemaSample] = field(default_factory=list)
    bad_volumes: list[_SchemaSample] = field(default_factory=list)
    shifted_rows: list[_SchemaSample] = field(default_factory=list)
    invalid_rows: list[_SchemaSample] = field(default_factory=list)


@dataclass(slots=True)
class HistDataAsciiTextIngestionRule:
    """Validate text-level HistData ASCII ingestion assumptions."""

    rule_id: str = ASCII_TEXT_INGESTION_RULE_ID
    description: str = (
        "Validate HistData ASCII text decoding, dialect, headers, and fields."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return raw text ingestion findings for one target."""
        if not _is_ascii_text_target(target):
            return ()

        try:
            delimiter = delimiter_for_timeframe(target.timeframe)
            columns = columns_for_timeframe(target.timeframe)
            payload = _read_text_payload(target)
        except ValueError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TEXT_METADATA_UNSUPPORTED",
                    message="Target metadata does not describe a supported "
                    "HistData ASCII timeframe.",
                    metadata={
                        "timeframe": target.timeframe,
                        "error": str(exc),
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

        line_endings = _scan_line_endings(payload.data)
        try:
            text = payload.data.decode("utf-8")
        except UnicodeDecodeError as exc:
            return (
                _finding(
                    target,
                    code="ASCII_TEXT_ENCODING_INVALID",
                    message="ASCII file does not decode as strict UTF-8.",
                    metadata={
                        "encoding": "utf-8",
                        "error": str(exc),
                        "byte_start": exc.start,
                        "byte_end": exc.end,
                        "source_member": payload.source_member,
                    },
                ),
            )

        findings: list[QualityFinding] = []
        findings.extend(_line_ending_findings(target, line_endings))
        row_scan = _scan_rows(text, delimiter=delimiter, columns=columns)
        if row_scan.header_row_number is not None:
            findings.append(
                _finding(
                    target,
                    code="ASCII_HEADER_ROW_PRESENT",
                    message="HistData ASCII files are expected to be "
                    "headerless.",
                    location=QualityLocation(
                        path=target.path,
                        row_number=row_scan.header_row_number,
                        metadata={"source_member": payload.source_member},
                    ),
                    metadata={
                        "expected_headerless": True,
                        "columns": list(columns),
                        "source_member": payload.source_member,
                    },
                )
            )
        if row_scan.delimiter_count:
            findings.append(
                _finding(
                    target,
                    code="ASCII_DELIMITER_MISMATCH",
                    message="Rows appear to use a delimiter that does not "
                    "match the HistData timeframe dialect.",
                    location=QualityLocation(
                        path=target.path,
                        row_number=row_scan.delimiter_samples[0].row_number,
                        metadata={"source_member": payload.source_member},
                    ),
                    metadata={
                        "expected_delimiter": delimiter,
                        "suspect_delimiter": _wrong_delimiter(delimiter),
                        "row_count": row_scan.delimiter_count,
                        "samples": _samples(row_scan.delimiter_samples),
                        "source_member": payload.source_member,
                    },
                )
            )
        if row_scan.field_count_error_count:
            findings.append(
                _finding(
                    target,
                    code="ASCII_ROW_FIELD_COUNT_INVALID",
                    message="Rows have the wrong number of fields for the "
                    "HistData timeframe schema.",
                    location=QualityLocation(
                        path=target.path,
                        row_number=row_scan.field_count_samples[0].row_number,
                        metadata={"source_member": payload.source_member},
                    ),
                    metadata={
                        "expected_field_count": len(columns),
                        "row_count": row_scan.field_count_error_count,
                        "samples": _samples(row_scan.field_count_samples),
                        "source_member": payload.source_member,
                    },
                )
            )
        return tuple(findings)


@dataclass(slots=True)
class HistDataAsciiSchemaIngestionRule:
    """Validate strict typed HistData ASCII schema assumptions."""

    rule_id: str = ASCII_SCHEMA_INGESTION_RULE_ID
    description: str = (
        "Validate HistData ASCII timestamps, price fields, and volume types."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return strict typed schema findings for one target."""
        if not _is_ascii_text_target(target):
            return ()

        try:
            delimiter = delimiter_for_timeframe(target.timeframe)
            columns = columns_for_timeframe(target.timeframe)
            payload = _read_text_payload(target)
            text = payload.data.decode("utf-8")
        except (ValueError, UnicodeDecodeError, _SourceReadError):
            return ()

        scan = _scan_schema_rows(
            text,
            timeframe=target.timeframe,
            delimiter=delimiter,
            columns=columns,
        )
        return _schema_findings(
            target=target,
            scan=scan,
            columns=columns,
            source_member=payload.source_member,
        )


def ingestion_quality_rules() -> tuple[QualityRule, ...]:
    """Return ingestion quality rules in deterministic execution order."""
    text_rule: QualityRule = HistDataAsciiTextIngestionRule()
    schema_rule: QualityRule = HistDataAsciiSchemaIngestionRule()
    return (text_rule, schema_rule)


def _is_ascii_text_target(target: QualityTarget) -> bool:
    return target.data_format == "ascii" and target.kind in {
        QualityTargetKind.CSV,
        QualityTargetKind.ZIP,
    }


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        try:
            return _TextPayload(path.read_bytes())
        except OSError as exc:
            raise _source_error(
                "ASCII_TEXT_UNREADABLE",
                "ASCII file could not be read.",
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
                    code="ASCII_TEXT_ZIP_MEMBER_UNAVAILABLE",
                    message="ZIP archive must contain exactly one CSV member "
                    "for text ingestion checks.",
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
            "ASCII_TEXT_ZIP_UNREADABLE",
            "ZIP archive could not be opened for text ingestion checks.",
            exc,
        ) from exc
    except (KeyError, OSError) as exc:
        raise _source_error(
            "ASCII_TEXT_UNREADABLE",
            "ASCII source could not be read for text ingestion checks.",
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


def _scan_line_endings(data: bytes) -> _LineEndingScan:
    counts = {"lf": 0, "crlf": 0, "cr": 0}
    index = 0
    while index < len(data):
        value = data[index]
        if value == 13:
            if index + 1 < len(data) and data[index + 1] == 10:
                counts["crlf"] += 1
                index += 2
            else:
                counts["cr"] += 1
                index += 1
            continue
        if value == 10:
            counts["lf"] += 1
        index += 1
    return _LineEndingScan(counts=counts)


def _line_ending_findings(
    target: QualityTarget,
    line_endings: _LineEndingScan,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = []
    if line_endings.is_inconsistent:
        findings.append(
            _finding(
                target,
                code="ASCII_LINE_ENDINGS_INCONSISTENT",
                message="ASCII file mixes multiple line-ending styles.",
                severity=QualitySeverity.WARNING,
                metadata={
                    "line_endings": line_endings.to_dict(),
                    "styles": list(line_endings.used_styles),
                },
            )
        )
    if line_endings.has_malformed:
        findings.append(
            _finding(
                target,
                code="ASCII_LINE_ENDINGS_MALFORMED",
                message="ASCII file contains bare carriage-return line "
                "endings.",
                severity=QualitySeverity.WARNING,
                metadata={"line_endings": line_endings.to_dict()},
            )
        )
    return tuple(findings)


def _scan_rows(
    text: str,
    *,
    delimiter: str,
    columns: tuple[str, ...],
) -> _RowScan:
    scan = _RowScan()
    expected_count = len(columns)
    for row_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        scan.row_count += 1
        row = _parse_row(raw, delimiter)
        if scan.row_count == 1 and tuple(row) == columns:
            scan.header_row_number = row_number
        if _has_wrong_delimiter(raw, delimiter):
            scan.delimiter_count += 1
            _append_sample(
                scan.delimiter_samples,
                _RowSample(
                    row_number=row_number,
                    field_count=len(row),
                    raw=raw,
                ),
            )
        if len(row) != expected_count:
            scan.field_count_error_count += 1
            _append_sample(
                scan.field_count_samples,
                _RowSample(
                    row_number=row_number,
                    field_count=len(row),
                    raw=raw,
                ),
            )
    return scan


def _scan_schema_rows(
    text: str,
    *,
    timeframe: str,
    delimiter: str,
    columns: tuple[str, ...],
) -> _SchemaScan:
    scan = _SchemaScan()
    expected_count = len(columns)
    for row_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        row = _parse_row(raw, delimiter)
        if len(row) != expected_count or tuple(row) == columns:
            continue
        if _row_looks_shifted(row, timeframe=timeframe):
            scan.shifted_row_count += 1
            _append_schema_sample(
                scan.shifted_rows,
                _SchemaSample(
                    row_number=row_number,
                    column="datetime",
                    raw_value=row[0],
                    error="source timestamp appears outside the datetime "
                    "column",
                ),
            )
            continue

        has_schema_error = False
        try:
            parse_histdata_datetime_to_utc_ms(row[0], timeframe)
        except ValueError as exc:
            has_schema_error = True
            scan.bad_timestamp_count += 1
            _append_schema_sample(
                scan.bad_timestamps,
                _SchemaSample(
                    row_number=row_number,
                    column="datetime",
                    raw_value=row[0],
                    error=str(exc),
                ),
            )

        for column, raw_value in zip(columns[1:-1], row[1:-1], strict=True):
            try:
                _parse_price_value(raw_value)
            except ValueError as exc:
                has_schema_error = True
                scan.bad_numeric_count += 1
                _append_schema_sample(
                    scan.bad_numerics,
                    _SchemaSample(
                        row_number=row_number,
                        column=column,
                        raw_value=raw_value,
                        error=str(exc),
                    ),
                )

        try:
            _parse_volume_value(row[-1])
        except ValueError as exc:
            has_schema_error = True
            scan.bad_volume_count += 1
            _append_schema_sample(
                scan.bad_volumes,
                _SchemaSample(
                    row_number=row_number,
                    column=columns[-1],
                    raw_value=row[-1],
                    error=str(exc),
                ),
            )

        if has_schema_error:
            continue

        try:
            normalize_ascii_row(timeframe, row)
        except ValueError as exc:
            scan.invalid_row_count += 1
            _append_schema_sample(
                scan.invalid_rows,
                _SchemaSample(
                    row_number=row_number,
                    column="",
                    raw_value=raw,
                    error=str(exc),
                ),
            )
            continue
        scan.parsed_row_count += 1
    return scan


def _schema_findings(
    *,
    target: QualityTarget,
    scan: _SchemaScan,
    columns: tuple[str, ...],
    source_member: str,
) -> tuple[QualityFinding, ...]:
    findings: list[QualityFinding] = []
    price_columns = columns[1:-1]
    if scan.shifted_rows:
        findings.append(
            _schema_finding(
                target,
                code="ASCII_ROW_SCHEMA_SHIFTED",
                message="Row values appear shifted away from the HistData "
                "timeframe schema.",
                samples=scan.shifted_rows,
                metadata={
                    "columns": list(columns),
                    "price_columns": list(price_columns),
                    "source_member": source_member,
                },
                row_count=scan.shifted_row_count,
            )
        )
    if scan.bad_timestamps:
        findings.append(
            _schema_finding(
                target,
                code="ASCII_TIMESTAMP_INVALID",
                message="Timestamp values do not parse with strict HistData "
                "source timestamp semantics.",
                samples=scan.bad_timestamps,
                metadata={
                    "timeframe": target.timeframe,
                    "source_timezone": "EST-no-DST",
                    "source_member": source_member,
                },
                row_count=scan.bad_timestamp_count,
            )
        )
    if scan.bad_numerics:
        findings.append(
            _schema_finding(
                target,
                code="ASCII_NUMERIC_INVALID",
                message="Price fields must parse as finite decimal numbers.",
                samples=scan.bad_numerics,
                metadata={
                    "price_columns": list(price_columns),
                    "source_member": source_member,
                },
                row_count=scan.bad_numeric_count,
            )
        )
    if scan.bad_volumes:
        findings.append(
            _schema_finding(
                target,
                code="ASCII_VOLUME_INVALID",
                message="Volume fields must parse as non-negative int32 "
                "values; zero volume is allowed for HistData FX.",
                samples=scan.bad_volumes,
                metadata={
                    "volume_column": columns[-1],
                    "min_value": 0,
                    "max_value": INT32_MAX,
                    "zero_volume_allowed": True,
                    "structurally_uninformative": True,
                    "source_member": source_member,
                },
                row_count=scan.bad_volume_count,
            )
        )
    if scan.invalid_rows:
        findings.append(
            _schema_finding(
                target,
                code="ASCII_ROW_SCHEMA_INVALID",
                message="Rows failed canonical HistData ASCII normalization.",
                samples=scan.invalid_rows,
                metadata={
                    "columns": list(columns),
                    "source_member": source_member,
                },
                row_count=scan.invalid_row_count,
            )
        )
    return tuple(findings)


def _schema_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    samples: list[_SchemaSample],
    metadata: dict[str, JSONValue],
    row_count: int,
) -> QualityFinding:
    first = samples[0]
    return _finding(
        target,
        code=code,
        message=message,
        location=QualityLocation(
            path=target.path,
            row_number=first.row_number,
            column=first.column,
            metadata={
                "source_member": str(metadata.get("source_member") or "")
            },
        ),
        metadata={
            **metadata,
            "row_count": row_count,
            "samples": _schema_samples(samples),
        },
    )


def _parse_row(raw: str, delimiter: str) -> list[str]:
    return next(csv.reader((raw,), delimiter=delimiter), [])


def _row_looks_shifted(row: list[str], *, timeframe: str) -> bool:
    try:
        parse_histdata_datetime_to_utc_ms(row[0], timeframe)
    except ValueError:
        return any(
            _is_valid_source_timestamp(value, timeframe) for value in row[1:]
        )
    return False


def _is_valid_source_timestamp(value: str, timeframe: str) -> bool:
    try:
        parse_histdata_datetime_to_utc_ms(value, timeframe)
    except ValueError:
        return False
    return True


def _parse_price_value(value: str) -> float:
    raw = value.strip()
    try:
        parsed = float(raw)
    except ValueError as exc:
        msg = "expected finite decimal number"
        raise ValueError(msg) from exc
    if not math.isfinite(parsed):
        msg = "expected finite decimal number"
        raise ValueError(msg)
    return parsed


def _parse_volume_value(value: str) -> int:
    raw = value.strip()
    if not raw.lstrip("+-").isdigit():
        msg = "expected integer volume"
        raise ValueError(msg)
    parsed = int(raw)
    if parsed < 0 or parsed > INT32_MAX:
        msg = f"expected int32 volume between 0 and {INT32_MAX}"
        raise ValueError(msg)
    return parsed


def _has_wrong_delimiter(raw: str, delimiter: str) -> bool:
    wrong = _wrong_delimiter(delimiter)
    return delimiter not in raw and wrong in raw


def _wrong_delimiter(delimiter: str) -> str:
    return "," if delimiter == ";" else ";"


def _append_sample(samples: list[_RowSample], sample: _RowSample) -> None:
    if len(samples) < MAX_ROW_SAMPLES:
        samples.append(sample)


def _append_schema_sample(
    samples: list[_SchemaSample],
    sample: _SchemaSample,
) -> None:
    if len(samples) < MAX_ROW_SAMPLES:
        samples.append(sample)


def _samples(samples: Iterable[_RowSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _schema_samples(samples: Iterable[_SchemaSample]) -> list[JSONValue]:
    return [sample.to_dict() for sample in samples]


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    location: QualityLocation | None = None,
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=ASCII_TEXT_INGESTION_RULE_ID,
        target=target,
        location=location or QualityLocation(path=target.path),
        metadata=dict(metadata or {}),
    )

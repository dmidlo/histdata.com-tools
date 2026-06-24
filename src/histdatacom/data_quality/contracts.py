"""Serializable contracts for HistData data-quality assessments."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from histdatacom.runtime_contracts import JSONValue


class QualitySeverity(str, Enum):
    """Severity levels emitted by data-quality checks."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    @classmethod
    def from_value(
        cls, value: str | "QualitySeverity" | None
    ) -> "QualitySeverity":
        """Normalize a public severity value."""
        if isinstance(value, cls):
            return value

        normalized = str(value or "").strip().lower()
        if not normalized:
            return cls.INFO

        aliases = {
            "warn": cls.WARNING,
            "failed": cls.ERROR,
            "failure": cls.ERROR,
            "fatal": cls.ERROR,
            "fail": cls.ERROR,
        }
        if normalized in aliases:
            return aliases[normalized]

        try:
            return cls(normalized)
        except ValueError as exc:
            msg = f"unknown quality severity: {value!r}"
            raise ValueError(msg) from exc

    @property
    def rank(self) -> int:
        """Return severity order for aggregation."""
        return {
            self.INFO: 0,
            self.WARNING: 1,
            self.ERROR: 2,
        }[self]

    @classmethod
    def max(
        cls, severities: Iterable[str | "QualitySeverity" | None]
    ) -> "QualitySeverity":
        """Return the highest severity in an iterable."""
        highest = cls.INFO
        for severity in severities:
            candidate = cls.from_value(severity)
            if candidate.rank > highest.rank:
                highest = candidate
        return highest


class QualityStatus(str, Enum):
    """Aggregate pass/warning/fail status for quality output."""

    CLEAN = "clean"
    WARNING = "warning"
    FAILED = "failed"

    @classmethod
    def from_value(cls, value: str | "QualityStatus" | None) -> "QualityStatus":
        """Normalize a public status value."""
        if isinstance(value, cls):
            return value

        normalized = str(value or "").strip().lower()
        if not normalized:
            return cls.CLEAN

        aliases = {
            "ok": cls.CLEAN,
            "pass": cls.CLEAN,
            "passed": cls.CLEAN,
            "success": cls.CLEAN,
            "error": cls.FAILED,
            "fail": cls.FAILED,
            "failure": cls.FAILED,
        }
        if normalized in aliases:
            return aliases[normalized]

        try:
            return cls(normalized)
        except ValueError as exc:
            msg = f"unknown quality status: {value!r}"
            raise ValueError(msg) from exc

    @classmethod
    def from_severity_counts(
        cls,
        *,
        warning_count: int = 0,
        error_count: int = 0,
    ) -> "QualityStatus":
        """Return aggregate status from warning and error counts."""
        if error_count:
            return cls.FAILED
        if warning_count:
            return cls.WARNING
        return cls.CLEAN


class QualityTargetKind(str, Enum):
    """Kind of artifact being assessed."""

    UNKNOWN = "unknown"
    DIRECTORY = "directory"
    ZIP = "zip"
    CSV = "csv"
    SPREADSHEET = "spreadsheet"
    CACHE = "cache"

    @classmethod
    def from_value(
        cls, value: str | "QualityTargetKind" | None
    ) -> "QualityTargetKind":
        """Normalize an artifact kind value."""
        if isinstance(value, cls):
            return value

        normalized = str(value or "").strip().lower().replace("_", "-")
        if not normalized:
            return cls.UNKNOWN

        aliases = {
            "dir": cls.DIRECTORY,
            "folder": cls.DIRECTORY,
            "archive": cls.ZIP,
            "zipfile": cls.ZIP,
            "zip-file": cls.ZIP,
            "csv-file": cls.CSV,
            "xlsx": cls.SPREADSHEET,
            "workbook": cls.SPREADSHEET,
            "spreadsheet-file": cls.SPREADSHEET,
            "excel": cls.SPREADSHEET,
            "excel-file": cls.SPREADSHEET,
            "file": cls.CSV,
            "polars-cache": cls.CACHE,
            "cache-file": cls.CACHE,
            "parquet": cls.CACHE,
        }
        if normalized in aliases:
            return aliases[normalized]

        try:
            return cls(normalized)
        except ValueError:
            return cls.UNKNOWN


@dataclass(frozen=True, slots=True)
class QualityTarget:
    """A ZIP, CSV, cache, or directory target for quality checks."""

    path: str
    kind: QualityTargetKind = QualityTargetKind.UNKNOWN
    data_format: str = ""
    timeframe: str = ""
    symbol: str = ""
    period: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "path": self.path,
            "kind": self.kind.value,
            "data_format": self.data_format,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
            "period": self.period,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityTarget":
        """Create a quality target from JSON-compatible data."""
        return cls(
            path=str(data.get("path", "")),
            kind=QualityTargetKind.from_value(data.get("kind")),
            data_format=str(data.get("data_format", "") or ""),
            timeframe=str(data.get("timeframe", "") or ""),
            symbol=str(data.get("symbol", "") or ""),
            period=str(data.get("period", "") or ""),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True, slots=True)
class QualityLocation:
    """Optional record-level context for a quality finding."""

    path: str = ""
    row_number: int | None = None
    timestamp_source: str = ""
    timestamp_utc_ms: int | None = None
    column: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "path": self.path,
            "row_number": self.row_number,
            "timestamp_source": self.timestamp_source,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "column": self.column,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "QualityLocation":
        """Create a location from JSON-compatible data."""
        data = data or {}
        row_number = data.get("row_number")
        timestamp_utc_ms = data.get("timestamp_utc_ms")
        return cls(
            path=str(data.get("path", "") or ""),
            row_number=(None if row_number is None else int(row_number)),
            timestamp_source=str(data.get("timestamp_source", "") or ""),
            timestamp_utc_ms=(
                None if timestamp_utc_ms is None else int(timestamp_utc_ms)
            ),
            column=str(data.get("column", "") or ""),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True, slots=True)
class QualityFinding:
    """A structured finding emitted by one quality check."""

    severity: QualitySeverity
    code: str
    message: str
    rule_id: str
    target: QualityTarget
    location: QualityLocation = field(default_factory=QualityLocation)
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
            "rule_id": self.rule_id,
            "target": self.target.to_dict(),
            "location": self.location.to_dict(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityFinding":
        """Create a quality finding from JSON-compatible data."""
        return cls(
            severity=QualitySeverity.from_value(data.get("severity")),
            code=str(data.get("code", "") or ""),
            message=str(data.get("message", "") or ""),
            rule_id=str(data.get("rule_id", "") or ""),
            target=QualityTarget.from_dict(data.get("target") or {}),
            location=QualityLocation.from_dict(data.get("location")),
            metadata=dict(data.get("metadata") or {}),
        )


class QualityRule(Protocol):
    """Protocol implemented by concrete data-quality checks."""

    rule_id: str
    description: str

    def evaluate(self, target: QualityTarget) -> Iterable[QualityFinding]:
        """Return findings for one target."""


class QualityRunRule(Protocol):
    """Protocol implemented by checks that need the full target set."""

    rule_id: str
    description: str

    def evaluate_run(
        self,
        targets: Iterable[QualityTarget],
        *,
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> "QualityReport":
        """Return a report fragment for a whole quality run."""


def _severity_counts(
    findings: Iterable[QualityFinding],
) -> Counter[QualitySeverity]:
    """Return finding counts keyed by normalized severity."""
    return Counter(finding.severity for finding in findings)


def _status_from_findings(findings: Iterable[QualityFinding]) -> QualityStatus:
    """Return aggregate status for a finding iterable."""
    counts = _severity_counts(findings)
    return QualityStatus.from_severity_counts(
        warning_count=counts[QualitySeverity.WARNING],
        error_count=counts[QualitySeverity.ERROR],
    )


@dataclass(frozen=True, slots=True)
class QualityRuleResult:
    """Findings produced by one rule for one target."""

    rule_id: str
    target: QualityTarget
    findings: tuple[QualityFinding, ...] = ()

    @property
    def status(self) -> QualityStatus:
        """Return aggregate status for this rule result."""
        return _status_from_findings(self.findings)

    @property
    def max_severity(self) -> QualitySeverity:
        """Return the highest finding severity for this rule result."""
        return QualitySeverity.max(
            finding.severity for finding in self.findings
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "rule_id": self.rule_id,
            "target": self.target.to_dict(),
            "findings": [finding.to_dict() for finding in self.findings],
            "status": self.status.value,
            "max_severity": self.max_severity.value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityRuleResult":
        """Create a rule result from JSON-compatible data."""
        return cls(
            rule_id=str(data.get("rule_id", "") or ""),
            target=QualityTarget.from_dict(data.get("target") or {}),
            findings=tuple(
                QualityFinding.from_dict(finding)
                for finding in data.get("findings", ())
            ),
        )


@dataclass(frozen=True, slots=True)
class QualityTargetSummary:
    """Aggregate data-quality result for one assessed target."""

    target: QualityTarget
    rule_count: int
    finding_count: int
    info_count: int
    warning_count: int
    error_count: int
    status: QualityStatus
    max_severity: QualitySeverity

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "target": self.target.to_dict(),
            "rule_count": self.rule_count,
            "finding_count": self.finding_count,
            "info_count": self.info_count,
            "warning_count": self.warning_count,
            "error_count": self.error_count,
            "status": self.status.value,
            "max_severity": self.max_severity.value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityTargetSummary":
        """Create a target summary from JSON-compatible data."""
        return cls(
            target=QualityTarget.from_dict(data.get("target") or {}),
            rule_count=int(data.get("rule_count", 0) or 0),
            finding_count=int(data.get("finding_count", 0) or 0),
            info_count=int(data.get("info_count", 0) or 0),
            warning_count=int(data.get("warning_count", 0) or 0),
            error_count=int(data.get("error_count", 0) or 0),
            status=QualityStatus.from_value(data.get("status")),
            max_severity=QualitySeverity.from_value(data.get("max_severity")),
        )


@dataclass(frozen=True, slots=True)
class QualityRunSummary:
    """Aggregate data-quality result for an assessment run."""

    target_count: int
    rule_count: int
    finding_count: int
    info_count: int
    warning_count: int
    error_count: int
    status: QualityStatus
    max_severity: QualitySeverity

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "target_count": self.target_count,
            "rule_count": self.rule_count,
            "finding_count": self.finding_count,
            "info_count": self.info_count,
            "warning_count": self.warning_count,
            "error_count": self.error_count,
            "status": self.status.value,
            "max_severity": self.max_severity.value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityRunSummary":
        """Create a run summary from JSON-compatible data."""
        return cls(
            target_count=int(data.get("target_count", 0) or 0),
            rule_count=int(data.get("rule_count", 0) or 0),
            finding_count=int(data.get("finding_count", 0) or 0),
            info_count=int(data.get("info_count", 0) or 0),
            warning_count=int(data.get("warning_count", 0) or 0),
            error_count=int(data.get("error_count", 0) or 0),
            status=QualityStatus.from_value(data.get("status")),
            max_severity=QualitySeverity.from_value(data.get("max_severity")),
        )


@dataclass(frozen=True, slots=True)
class QualityReport:
    """Complete data-quality assessment output."""

    targets: tuple[QualityTarget, ...] = ()
    rule_results: tuple[QualityRuleResult, ...] = ()
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[QualityFinding, ...]:
        """Return all findings in target/rule evaluation order."""
        return tuple(
            finding
            for result in self.rule_results
            for finding in result.findings
        )

    @property
    def status(self) -> QualityStatus:
        """Return aggregate run status."""
        return _status_from_findings(self.findings)

    @property
    def max_severity(self) -> QualitySeverity:
        """Return the highest finding severity in the run."""
        return QualitySeverity.max(
            finding.severity for finding in self.findings
        )

    @property
    def rule_count(self) -> int:
        """Return the number of distinct evaluated rules."""
        return len({result.rule_id for result in self.rule_results})

    @property
    def target_summaries(self) -> tuple[QualityTargetSummary, ...]:
        """Return per-target aggregate summaries."""
        summaries = []
        for target in self.targets:
            results = tuple(
                result
                for result in self.rule_results
                if result.target == target
            )
            findings = tuple(
                finding for result in results for finding in result.findings
            )
            counts = _severity_counts(findings)
            summaries.append(
                QualityTargetSummary(
                    target=target,
                    rule_count=len({result.rule_id for result in results}),
                    finding_count=len(findings),
                    info_count=counts[QualitySeverity.INFO],
                    warning_count=counts[QualitySeverity.WARNING],
                    error_count=counts[QualitySeverity.ERROR],
                    status=QualityStatus.from_severity_counts(
                        warning_count=counts[QualitySeverity.WARNING],
                        error_count=counts[QualitySeverity.ERROR],
                    ),
                    max_severity=QualitySeverity.max(
                        finding.severity for finding in findings
                    ),
                )
            )
        return tuple(summaries)

    def summary(self) -> QualityRunSummary:
        """Return aggregate run counts and status."""
        findings = self.findings
        counts = _severity_counts(findings)
        return QualityRunSummary(
            target_count=len(self.targets),
            rule_count=self.rule_count,
            finding_count=len(findings),
            info_count=counts[QualitySeverity.INFO],
            warning_count=counts[QualitySeverity.WARNING],
            error_count=counts[QualitySeverity.ERROR],
            status=QualityStatus.from_severity_counts(
                warning_count=counts[QualitySeverity.WARNING],
                error_count=counts[QualitySeverity.ERROR],
            ),
            max_severity=QualitySeverity.max(
                finding.severity for finding in findings
            ),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "targets": [target.to_dict() for target in self.targets],
            "rule_results": [result.to_dict() for result in self.rule_results],
            "target_summaries": [
                summary.to_dict() for summary in self.target_summaries
            ],
            "summary": self.summary().to_dict(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "QualityReport":
        """Create a quality report from JSON-compatible data."""
        return cls(
            targets=tuple(
                QualityTarget.from_dict(target)
                for target in data.get("targets", ())
            ),
            rule_results=tuple(
                QualityRuleResult.from_dict(result)
                for result in data.get("rule_results", ())
            ),
            metadata=dict(data.get("metadata") or {}),
        )

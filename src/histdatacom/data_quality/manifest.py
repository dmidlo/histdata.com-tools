"""Coverage manifest checks for discovered HistData artifacts."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.activity_stages import read_repository_data_file
from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualityRunRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.runtime_contracts import JSONValue

COVERAGE_MANIFEST_SCHEMA_VERSION = "histdatacom.coverage-manifest.v1"
COVERAGE_MANIFEST_RULE_ID = "inventory.coverage.manifest"
COVERAGE_METADATA_KEY = "coverage_manifest"


@dataclass(frozen=True, order=True, slots=True)
class CoverageDimension:
    """A normalized dataset coverage coordinate."""

    data_format: str
    timeframe: str
    symbol: str
    period: str

    @classmethod
    def from_target(cls, target: QualityTarget) -> "CoverageDimension | None":
        """Return a coverage coordinate parsed from a discovered target."""
        if not (
            target.data_format
            and target.timeframe
            and target.symbol
            and target.period
        ):
            return None
        return cls(
            data_format=_normalize_format(target.data_format),
            timeframe=_normalize_timeframe(target.timeframe),
            symbol=_normalize_symbol(target.symbol),
            period=str(target.period),
        )

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "CoverageDimension":
        """Create a coverage coordinate from JSON-like metadata."""
        return cls(
            data_format=_normalize_format(str(data.get("data_format") or "")),
            timeframe=_normalize_timeframe(str(data.get("timeframe") or "")),
            symbol=_normalize_symbol(str(data.get("symbol") or "")),
            period=str(data.get("period") or ""),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "data_format": self.data_format,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
            "period": self.period,
        }


@dataclass(slots=True)
class HistDataCoverageManifestRule:
    """Build expected-vs-observed dataset coverage for a quality run."""

    rule_id: str = COVERAGE_MANIFEST_RULE_ID
    description: str = (
        "Compare discovered files to expected symbol/format/timeframe periods."
    )

    def evaluate_run(
        self,
        targets: Iterable[QualityTarget],
        *,
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> QualityReport:
        """Return a coverage manifest report fragment for all targets."""
        target_tuple = tuple(targets)
        metadata_map = dict(metadata or {})
        coverage_config = _coverage_config(metadata_map)
        expected = _expected_dimensions(target_tuple, coverage_config)
        observed = _observed_targets(target_tuple)
        manifest = _coverage_manifest_payload(
            expected=expected,
            observed=observed,
            targets=target_tuple,
            expected_source=_expected_source(coverage_config, target_tuple),
        )
        findings = _coverage_findings(manifest, coverage_config)
        if not findings:
            return QualityReport(
                metadata={COVERAGE_METADATA_KEY: manifest},
            )

        manifest_target = _manifest_target(coverage_config, manifest)
        return QualityReport(
            targets=(manifest_target,),
            rule_results=(
                QualityRuleResult(
                    rule_id=self.rule_id,
                    target=manifest_target,
                    findings=tuple(
                        _finding_with_target(finding, manifest_target)
                        for finding in findings
                    ),
                ),
            ),
            metadata={COVERAGE_METADATA_KEY: manifest},
        )


def manifest_quality_run_rules() -> tuple[QualityRunRule, ...]:
    """Return coverage manifest run rules in deterministic execution order."""
    rule: QualityRunRule = HistDataCoverageManifestRule()
    return (rule,)


def coverage_manifest_metadata(
    *,
    roots: Iterable[str] = (),
    request: Mapping[str, Any] | None = None,
    expected_dimensions: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, JSONValue]:
    """Return quality-run metadata consumed by the manifest rule."""
    payload: dict[str, JSONValue] = {
        "roots": [str(root) for root in roots],
    }
    if request:
        payload["request"] = _json_mapping(request)
    if expected_dimensions is not None:
        payload["expected_dimensions"] = [
            CoverageDimension.from_mapping(item).to_dict()
            for item in expected_dimensions
        ]
    return payload


def _coverage_config(
    metadata: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    raw_config = metadata.get(COVERAGE_METADATA_KEY)
    config = dict(raw_config) if isinstance(raw_config, Mapping) else {}
    explicit = metadata.get("expected_dimensions")
    if explicit and "expected_dimensions" not in config:
        config["expected_dimensions"] = explicit
    return config


def _expected_dimensions(
    targets: tuple[QualityTarget, ...],
    config: Mapping[str, JSONValue],
) -> tuple[CoverageDimension, ...]:
    explicit = _explicit_expected_dimensions(config)
    if explicit:
        return explicit

    repo_expected = _repo_expected_dimensions(targets, config)
    if repo_expected:
        return repo_expected

    return tuple(
        sorted({dimension for dimension, _ in _observed_targets(targets)})
    )


def _expected_source(
    config: Mapping[str, JSONValue],
    targets: tuple[QualityTarget, ...],
) -> str:
    if _explicit_expected_dimensions(config):
        return "metadata"
    if _repo_expected_dimensions(targets, config):
        return "repo"
    return "observed"


def _explicit_expected_dimensions(
    config: Mapping[str, JSONValue],
) -> tuple[CoverageDimension, ...]:
    values = config.get("expected_dimensions")
    if not isinstance(values, list):
        return ()
    return tuple(
        sorted(
            {
                CoverageDimension.from_mapping(item)
                for item in values
                if isinstance(item, Mapping)
            }
        )
    )


def _repo_expected_dimensions(
    targets: tuple[QualityTarget, ...],
    config: Mapping[str, JSONValue],
) -> tuple[CoverageDimension, ...]:
    observed_dimensions = tuple(
        dimension for dimension, _ in _observed_targets(targets)
    )
    if not observed_dimensions:
        return ()

    formats = {dimension.data_format for dimension in observed_dimensions}
    timeframes = {dimension.timeframe for dimension in observed_dimensions}
    symbols = {dimension.symbol.lower() for dimension in observed_dimensions}
    expected: set[CoverageDimension] = set()
    for repo_path in _repo_paths(config):
        try:
            repo_data = read_repository_data_file(repo_path)
        except (OSError, ValueError):
            continue
        for symbol, range_data in repo_data.items():
            if symbol in {"hash", "hash_utc"}:
                continue
            if symbol.lower() not in symbols:
                continue
            if not isinstance(range_data, Mapping):
                continue
            start = str(range_data.get("start") or "")
            end = str(range_data.get("end") or "")
            if not _valid_period(start) or not _valid_period(end):
                continue
            for period in _iter_months(start, end):
                for data_format in formats:
                    for timeframe in timeframes:
                        expected.add(
                            CoverageDimension(
                                data_format=data_format,
                                timeframe=timeframe,
                                symbol=_normalize_symbol(symbol),
                                period=period,
                            )
                        )
    return tuple(sorted(expected))


def _repo_paths(config: Mapping[str, JSONValue]) -> tuple[Path, ...]:
    roots = config.get("roots")
    if not isinstance(roots, list):
        return ()
    paths: list[Path] = []
    for root_value in roots:
        root = Path(str(root_value)).expanduser()
        repo_path = root / ".repo" if root.is_dir() else root.parent / ".repo"
        if repo_path.exists():
            paths.append(repo_path)
    return tuple(dict.fromkeys(paths))


def _observed_targets(
    targets: Iterable[QualityTarget],
) -> tuple[tuple[CoverageDimension, QualityTarget], ...]:
    observed: list[tuple[CoverageDimension, QualityTarget]] = []
    for target in targets:
        dimension = CoverageDimension.from_target(target)
        if dimension is not None:
            observed.append((dimension, target))
    return tuple(observed)


def _coverage_manifest_payload(
    *,
    expected: tuple[CoverageDimension, ...],
    observed: tuple[tuple[CoverageDimension, QualityTarget], ...],
    targets: tuple[QualityTarget, ...],
    expected_source: str,
) -> dict[str, JSONValue]:
    observed_by_dimension: dict[CoverageDimension, list[QualityTarget]] = (
        defaultdict(list)
    )
    for dimension, target in observed:
        observed_by_dimension[dimension].append(target)

    expected_set = set(expected)
    observed_set = set(observed_by_dimension)
    missing = tuple(sorted(expected_set.difference(observed_set)))
    unexpected = _unexpected_targets(
        targets=targets,
        observed=observed,
        expected=expected_set,
    )
    duplicates = _duplicate_targets(observed)
    present = tuple(
        sorted(
            dimension
            for dimension in expected
            if dimension in observed_by_dimension
        )
    )
    payload: dict[str, JSONValue] = {
        "schema_version": COVERAGE_MANIFEST_SCHEMA_VERSION,
        "expected_source": expected_source,
        "expected_count": len(expected),
        "present_count": len(present),
        "missing_count": len(missing),
        "duplicate_count": len(duplicates),
        "unexpected_count": len(unexpected),
        "expected_dimensions": [
            dimension.to_dict() for dimension in sorted(expected)
        ],
        "present": [
            _present_entry(dimension, observed_by_dimension[dimension])
            for dimension in present
        ],
        "missing": [dimension.to_dict() for dimension in missing],
        "duplicates": _json_dict_list(duplicates),
        "unexpected": _json_dict_list(unexpected),
    }
    return payload


def _present_entry(
    dimension: CoverageDimension,
    targets: list[QualityTarget],
) -> dict[str, JSONValue]:
    return {
        "dimension": dimension.to_dict(),
        "target_count": len(targets),
        "artifact_kinds": _json_str_list(
            sorted({target.kind.value for target in targets})
        ),
        "paths": _json_str_list(sorted(target.path for target in targets)),
    }


def _duplicate_targets(
    observed: tuple[tuple[CoverageDimension, QualityTarget], ...],
) -> list[dict[str, JSONValue]]:
    grouped: dict[tuple[CoverageDimension, str], list[QualityTarget]] = (
        defaultdict(list)
    )
    for dimension, target in observed:
        grouped[(dimension, target.kind.value)].append(target)

    duplicates: list[dict[str, JSONValue]] = []
    for (dimension, artifact_kind), targets in sorted(grouped.items()):
        if len(targets) < 2:
            continue
        duplicates.append(
            {
                "dimension": dimension.to_dict(),
                "artifact_kind": artifact_kind,
                "paths": _json_str_list(
                    sorted(target.path for target in targets)
                ),
            }
        )
    return duplicates


def _unexpected_targets(
    *,
    targets: tuple[QualityTarget, ...],
    observed: tuple[tuple[CoverageDimension, QualityTarget], ...],
    expected: set[CoverageDimension],
) -> list[dict[str, JSONValue]]:
    observed_by_path = {
        target.path: dimension for dimension, target in observed
    }
    unexpected: list[dict[str, JSONValue]] = []
    for target in sorted(targets, key=lambda item: item.path):
        dimension = observed_by_path.get(target.path)
        if dimension is None:
            unexpected.append(
                {
                    "path": target.path,
                    "kind": target.kind.value,
                    "reason": "unclassified",
                }
            )
            continue
        if dimension not in expected:
            unexpected.append(
                {
                    "path": target.path,
                    "kind": target.kind.value,
                    "reason": "not-in-expected",
                    "dimension": dimension.to_dict(),
                }
            )
    return unexpected


def _coverage_findings(
    manifest: Mapping[str, JSONValue],
    config: Mapping[str, JSONValue],
) -> tuple[QualityFinding, ...]:
    target = _manifest_target(config, manifest)
    findings: list[QualityFinding] = []
    for missing in _list_of_dicts(manifest.get("missing")):
        findings.append(
            _finding(
                target,
                code="COVERAGE_PERIOD_MISSING",
                message="Expected dataset period is missing from local targets.",
                metadata={"dimension": dict(missing)},
            )
        )
    for duplicate in _list_of_dicts(manifest.get("duplicates")):
        findings.append(
            _finding(
                target,
                code="COVERAGE_DUPLICATE_FILE",
                message="Multiple files cover the same dataset dimension.",
                severity=QualitySeverity.WARNING,
                metadata=dict(duplicate),
            )
        )
    for unexpected in _list_of_dicts(manifest.get("unexpected")):
        findings.append(
            _finding(
                target,
                code="COVERAGE_UNEXPECTED_FILE",
                message="Local file is outside expected dataset coverage.",
                severity=QualitySeverity.WARNING,
                metadata=dict(unexpected),
                path=str(unexpected.get("path") or target.path),
            )
        )
    return tuple(findings)


def _manifest_target(
    config: Mapping[str, JSONValue],
    manifest: Mapping[str, JSONValue],
) -> QualityTarget:
    roots = config.get("roots")
    root = ""
    if isinstance(roots, list) and roots:
        root = str(roots[0])
    return QualityTarget(
        path=root or "coverage-manifest",
        kind=QualityTargetKind.DIRECTORY,
        metadata={
            "manifest": "coverage",
            "schema_version": COVERAGE_MANIFEST_SCHEMA_VERSION,
            "expected_source": str(manifest.get("expected_source") or ""),
            "expected_count": _json_int(manifest.get("expected_count")),
            "present_count": _json_int(manifest.get("present_count")),
            "missing_count": _json_int(manifest.get("missing_count")),
            "duplicate_count": _json_int(manifest.get("duplicate_count")),
            "unexpected_count": _json_int(manifest.get("unexpected_count")),
        },
    )


def _finding_with_target(
    finding: QualityFinding,
    target: QualityTarget,
) -> QualityFinding:
    return QualityFinding(
        severity=finding.severity,
        code=finding.code,
        message=finding.message,
        rule_id=finding.rule_id,
        target=target,
        location=finding.location,
        metadata=dict(finding.metadata),
    )


def _finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity = QualitySeverity.ERROR,
    metadata: dict[str, JSONValue] | None = None,
    path: str = "",
) -> QualityFinding:
    return QualityFinding(
        severity=severity,
        code=code,
        message=message,
        rule_id=COVERAGE_MANIFEST_RULE_ID,
        target=target,
        location=QualityLocation(path=path or target.path),
        metadata=dict(metadata or {}),
    )


def _iter_months(start: str, end: str) -> tuple[str, ...]:
    start_year = int(start[:4])
    start_month = int(start[4:])
    end_year = int(end[:4])
    end_month = int(end[4:])
    months = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year:04d}{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return tuple(months)


def _valid_period(value: str) -> bool:
    if len(value) != 6 or not value.isdigit():
        return False
    month = int(value[4:])
    return 1 <= month <= 12


def _json_int(value: JSONValue | None) -> int:
    if isinstance(value, bool | list | dict) or value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _json_str_list(values: Iterable[str]) -> list[JSONValue]:
    return [str(value) for value in values]


def _json_dict_list(
    values: Iterable[Mapping[str, JSONValue]],
) -> list[JSONValue]:
    return [dict(value) for value in values]


def _normalize_format(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_timeframe(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper().replace("_", "")


def _list_of_dicts(value: JSONValue | None) -> tuple[dict[str, JSONValue], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


def _json_mapping(values: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {str(key): _json_value(value) for key, value in values.items()}


def _json_value(value: Any) -> JSONValue:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return _json_mapping(value)
    if isinstance(value, Iterable):
        return [_json_value(item) for item in value]
    return str(value)

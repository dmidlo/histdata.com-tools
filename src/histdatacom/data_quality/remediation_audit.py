"""Remediation-catalog completeness audit helpers."""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import TypeVar, cast

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualitySeverity,
)
from histdatacom.data_quality.remediation import (
    remediation_hints_for_finding_code,
)
from histdatacom.data_quality.reporting import (
    QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT,
    QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT,
    quality_remediation_coverage_summary,
)
from histdatacom.publication_safety import publish_safe_path
from histdatacom.runtime_contracts import JSONValue

QUALITY_REMEDIATION_CATALOG_AUDIT_SCHEMA_VERSION = (
    "histdatacom.quality-remediation-catalog-audit.v1"
)
DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT = (
    QUALITY_PAYLOAD_REMEDIATION_COVERAGE_GROUP_LIMIT
)
DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT = 16
DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT = 8
DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT = (
    QUALITY_PAYLOAD_REMEDIATION_COVERAGE_TARGET_AXIS_LIMIT
)

_SEVERITY_RANK = {"info": 1, "warning": 2, "error": 3}
_SEVERITY_SORT = {"error": 0, "warning": 1, "info": 2}
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class KnownQualityFindingCode:
    """One known data-quality finding code emitted by source or fixtures."""

    rule_id: str
    finding_code: str
    severity: QualitySeverity = QualitySeverity.ERROR
    source: str = ""
    severity_source: str = ""


@dataclass(slots=True)
class _CodeAggregate:
    rule_id: str
    finding_code: str
    mapped: bool = False
    occurrence_count: int = 0
    severity_counts: Counter[str] = field(default_factory=Counter)
    source_counts: Counter[str] = field(default_factory=Counter)

    @property
    def max_severity(self) -> str:
        return max(
            self.severity_counts,
            key=lambda severity: _SEVERITY_RANK.get(severity, 0),
            default=QualitySeverity.INFO.value,
        )


def discover_known_quality_findings(
    source_root: str | Path | None = None,
) -> tuple[KnownQualityFindingCode, ...]:
    """Return known finding codes discovered from data-quality source calls."""
    root = (
        Path(source_root)
        if source_root is not None
        else Path(__file__).resolve().parent
    )
    findings: list[KnownQualityFindingCode] = []
    for path in sorted(root.glob("*.py")):
        if path.name in {
            "__init__.py",
            "contracts.py",
            "remediation.py",
            "remediation_audit.py",
            "reporting.py",
        }:
            continue
        findings.extend(_known_findings_from_source(path, root=root))
    return tuple(
        sorted(
            findings,
            key=lambda item: (
                _SEVERITY_SORT.get(item.severity.value, 9),
                item.rule_id,
                item.finding_code,
                item.source,
            ),
        )
    )


def audit_remediation_catalog(
    *,
    known_findings: Iterable[KnownQualityFindingCode] | None = None,
    reports: Iterable[QualityReport | tuple[str, QualityReport]] = (),
    code_limit: int = DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
    rule_limit: int = DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    source_limit: int = DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    target_axis_limit: int = (
        DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT
    ),
) -> dict[str, JSONValue]:
    """Return a bounded remediation-catalog completeness audit payload."""
    known = tuple(
        known_findings
        if known_findings is not None
        else discover_known_quality_findings()
    )
    known_aggregates = _known_code_aggregates(known)
    report_payloads = [
        _report_coverage_payload(
            source,
            report,
            code_limit=code_limit,
            target_axis_limit=target_axis_limit,
        )
        for source, report in _normalized_reports(reports)
    ]
    unmapped_known = sorted(
        (
            aggregate
            for aggregate in known_aggregates.values()
            if not aggregate.mapped
        ),
        key=_code_aggregate_sort_key,
    )
    included_unmapped_known = _bounded_sequence(
        unmapped_known,
        limit=code_limit,
    )
    report_summary = _report_coverage_summary(report_payloads)
    summary = _audit_summary(
        known_aggregates,
        report_summary=report_summary,
    )
    payload: dict[str, JSONValue] = {
        "schema_version": (QUALITY_REMEDIATION_CATALOG_AUDIT_SCHEMA_VERSION),
        "status": (
            "needs-remediation-guidance"
            if summary["unmapped_warning_error_gap_count"]
            else "covered"
        ),
        "summary": summary,
        "known_code_counts": _known_code_counts(
            known_aggregates,
            rule_limit=rule_limit,
            code_limit=code_limit,
        ),
        "known_unmapped_codes": [
            _code_aggregate_payload(
                aggregate,
                source_limit=source_limit,
            )
            for aggregate in included_unmapped_known
        ],
        "report_coverage": cast(JSONValue, report_payloads),
        "payload_limits": {
            "known_unmapped_codes": _payload_limit_metadata(
                len(unmapped_known),
                code_limit,
            ),
            "known_code_sources": {
                "limit": source_limit,
                "applies_per_code": True,
            },
            "known_rule_id_counts": _payload_limit_metadata(
                _counter_distinct_count(
                    aggregate.rule_id for aggregate in known_aggregates.values()
                ),
                rule_limit,
            ),
            "known_finding_code_counts": _payload_limit_metadata(
                _counter_distinct_count(
                    aggregate.finding_code
                    for aggregate in known_aggregates.values()
                ),
                code_limit,
            ),
            "report_unmapped_groups": {
                "limit": code_limit,
                "target_axis_limit": target_axis_limit,
                "applies_per_report": True,
            },
        },
    }
    return payload


def audit_remediation_catalog_report_paths(
    report_paths: Iterable[str | Path],
    *,
    known_findings: Iterable[KnownQualityFindingCode] | None = None,
    code_limit: int = DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
    rule_limit: int = DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    source_limit: int = DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    target_axis_limit: int = (
        DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT
    ),
) -> dict[str, JSONValue]:
    """Audit the catalog with optional saved quality-report evidence."""
    reports = tuple(
        (publish_safe_path(str(path)), load_quality_report(path))
        for path in report_paths
    )
    return audit_remediation_catalog(
        known_findings=known_findings,
        reports=reports,
        code_limit=code_limit,
        rule_limit=rule_limit,
        source_limit=source_limit,
        target_axis_limit=target_axis_limit,
    )


def load_quality_report(path: str | Path) -> QualityReport:
    """Read a JSON quality report from disk."""
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        msg = f"quality report must be a JSON object: {path}"
        raise ValueError(msg)
    return QualityReport.from_dict(payload)


def remediation_catalog_audit_to_json(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return deterministic JSON for a remediation-catalog audit."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def format_remediation_catalog_audit(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return concise human-readable remediation-catalog audit lines."""
    summary = _mapping_payload(payload.get("summary"))
    lines = [
        "Remediation catalog audit",
        f"status: {_optional_string(payload, 'status') or 'unknown'}",
        (
            "known codes: "
            f"{_int_value(summary, 'known_code_count')} "
            f"mapped: {_int_value(summary, 'mapped_known_code_count')} "
            f"unmapped: {_int_value(summary, 'unmapped_known_code_count')}"
        ),
        (
            "warning/error gaps: "
            f"{_int_value(summary, 'unmapped_warning_error_gap_count')}"
        ),
    ]
    report_count = _int_value(summary, "report_count")
    if report_count:
        lines.append(
            "reports: "
            f"{report_count} "
            f"findings: {_int_value(summary, 'report_finding_count')} "
            "unmapped warning/error groups: "
            f"{_int_value(summary, 'report_unmapped_warning_error_group_count')}"
        )
    groups = [
        item
        for item in _list_payload(payload.get("known_unmapped_codes"))
        if _optional_string(item, "max_severity") in {"error", "warning"}
    ]
    lines.extend(("", "Unmapped warning/error known codes"))
    if not groups:
        lines.append("- none")
    else:
        lines.extend(f"- {_format_code_group(item)}" for item in groups)
    info_count = _int_value(summary, "unmapped_info_only_code_count")
    if info_count:
        lines.extend(
            (
                "",
                "INFO-only unmapped known codes",
                f"- count: {info_count}",
            )
        )
    limits = _mapping_payload(payload.get("payload_limits"))
    code_limit = _mapping_payload(limits.get("known_unmapped_codes"))
    omitted = _int_value(code_limit, "omitted_count")
    if omitted:
        lines.append(f"- omitted by limit: {omitted}")
    return "\n".join(lines)


def remediation_catalog_audit_has_warning_error_gaps(
    payload: Mapping[str, JSONValue],
) -> bool:
    """Return whether an audit found unmapped warning/error guidance gaps."""
    summary = _mapping_payload(payload.get("summary"))
    return bool(_int_value(summary, "unmapped_warning_error_gap_count"))


def _known_findings_from_source(
    path: Path,
    *,
    root: Path,
) -> tuple[KnownQualityFindingCode, ...]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return ()
    constants = _module_string_constants(tree)
    findings: list[KnownQualityFindingCode] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        code = _string_keyword(node, "code")
        if not code or not _looks_like_finding_code(code):
            continue
        severity, severity_source = _severity_from_call(node)
        rule_id = _rule_id_from_call(node, constants)
        source = _relative_source(path, root=root, line_number=node.lineno)
        findings.append(
            KnownQualityFindingCode(
                rule_id=rule_id,
                finding_code=code,
                severity=severity,
                source=source,
                severity_source=severity_source,
            )
        )
    return tuple(findings)


def _known_code_aggregates(
    known_findings: Iterable[KnownQualityFindingCode],
) -> dict[tuple[str, str], _CodeAggregate]:
    aggregates: dict[tuple[str, str], _CodeAggregate] = {}
    for known in known_findings:
        rule_id = known.rule_id or "unknown"
        key = (rule_id, known.finding_code)
        aggregate = aggregates.setdefault(
            key,
            _CodeAggregate(
                rule_id=rule_id,
                finding_code=known.finding_code,
            ),
        )
        aggregate.occurrence_count += 1
        aggregate.severity_counts[known.severity.value] += 1
        if known.source:
            aggregate.source_counts[known.source] += 1
        aggregate.mapped = aggregate.mapped or _known_code_is_mapped(known)
    return aggregates


def _known_code_is_mapped(known: KnownQualityFindingCode) -> bool:
    rule_id = known.rule_id if known.rule_id != "unknown" else ""
    return bool(
        remediation_hints_for_finding_code(
            known.finding_code,
            rule_id=rule_id,
        )
    )


def _report_coverage_payload(
    source: str,
    report: QualityReport,
    *,
    code_limit: int,
    target_axis_limit: int,
) -> dict[str, JSONValue]:
    coverage = quality_remediation_coverage_summary(
        report,
        group_limit=code_limit,
        target_axis_limit=target_axis_limit,
    )
    return {
        "source": publish_safe_path(source),
        "summary": report.summary().to_dict(),
        "remediation_coverage": coverage,
    }


def _report_coverage_summary(
    report_payloads: Sequence[Mapping[str, JSONValue]],
) -> dict[str, JSONValue]:
    report_count = len(report_payloads)
    finding_count = 0
    mapped_finding_count = 0
    unmapped_finding_count = 0
    unmapped_warning_error_group_count = 0
    for payload in report_payloads:
        coverage = _mapping_payload(payload.get("remediation_coverage"))
        finding_count += _int_value(coverage, "finding_count")
        mapped_finding_count += _int_value(coverage, "mapped_finding_count")
        unmapped_finding_count += _int_value(
            coverage,
            "unmapped_finding_count",
        )
        unmapped_warning_error_group_count += _int_value(
            coverage,
            "unmapped_warning_error_group_count",
        )
    return {
        "report_count": report_count,
        "report_finding_count": finding_count,
        "report_mapped_finding_count": mapped_finding_count,
        "report_unmapped_finding_count": unmapped_finding_count,
        "report_unmapped_warning_error_group_count": (
            unmapped_warning_error_group_count
        ),
    }


def _audit_summary(
    aggregates: Mapping[tuple[str, str], _CodeAggregate],
    *,
    report_summary: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    known_code_count = len(aggregates)
    mapped_known_code_count = sum(
        1 for aggregate in aggregates.values() if aggregate.mapped
    )
    unmapped_known_code_count = known_code_count - mapped_known_code_count
    unmapped_warning_error_code_count = sum(
        1
        for aggregate in aggregates.values()
        if not aggregate.mapped
        and aggregate.max_severity in {"error", "warning"}
    )
    unmapped_info_code_count = sum(
        1
        for aggregate in aggregates.values()
        if not aggregate.mapped and aggregate.max_severity == "info"
    )
    known_warning_error_code_count = sum(
        1
        for aggregate in aggregates.values()
        if aggregate.max_severity in {"error", "warning"}
    )
    report_gap_count = _int_value(
        report_summary,
        "report_unmapped_warning_error_group_count",
    )
    return {
        "known_code_count": known_code_count,
        "known_finding_occurrence_count": sum(
            aggregate.occurrence_count for aggregate in aggregates.values()
        ),
        "known_warning_error_code_count": known_warning_error_code_count,
        "mapped_known_code_count": mapped_known_code_count,
        "unmapped_known_code_count": unmapped_known_code_count,
        "unmapped_warning_error_code_count": (
            unmapped_warning_error_code_count
        ),
        "unmapped_info_only_code_count": unmapped_info_code_count,
        "unmapped_warning_error_gap_count": (
            unmapped_warning_error_code_count + report_gap_count
        ),
        **dict(report_summary),
    }


def _known_code_counts(
    aggregates: Mapping[tuple[str, str], _CodeAggregate],
    *,
    rule_limit: int,
    code_limit: int,
) -> dict[str, JSONValue]:
    severity_counts: Counter[str] = Counter()
    rule_id_counts: Counter[str] = Counter()
    finding_code_counts: Counter[str] = Counter()
    unmapped_rule_id_counts: Counter[str] = Counter()
    unmapped_finding_code_counts: Counter[str] = Counter()
    for aggregate in aggregates.values():
        severity_counts.update(aggregate.severity_counts)
        rule_id_counts[aggregate.rule_id] += aggregate.occurrence_count
        finding_code_counts[
            aggregate.finding_code
        ] += aggregate.occurrence_count
        if not aggregate.mapped:
            unmapped_rule_id_counts[
                aggregate.rule_id
            ] += aggregate.occurrence_count
            unmapped_finding_code_counts[
                aggregate.finding_code
            ] += aggregate.occurrence_count
    return {
        "severity_counts": _counter_payload(severity_counts),
        "rule_id_counts": _named_counter_payloads(
            rule_id_counts,
            key_name="rule_id",
            limit=rule_limit,
        ),
        "finding_code_counts": _named_counter_payloads(
            finding_code_counts,
            key_name="finding_code",
            limit=code_limit,
        ),
        "unmapped_rule_id_counts": _named_counter_payloads(
            unmapped_rule_id_counts,
            key_name="rule_id",
            limit=rule_limit,
        ),
        "unmapped_finding_code_counts": _named_counter_payloads(
            unmapped_finding_code_counts,
            key_name="finding_code",
            limit=code_limit,
        ),
    }


def _code_aggregate_payload(
    aggregate: _CodeAggregate,
    *,
    source_limit: int,
) -> dict[str, JSONValue]:
    sources = _named_counter_payloads(
        aggregate.source_counts,
        key_name="source",
        limit=source_limit,
    )
    return {
        "rule_id": aggregate.rule_id,
        "finding_code": aggregate.finding_code,
        "mapped": aggregate.mapped,
        "occurrence_count": aggregate.occurrence_count,
        "max_severity": aggregate.max_severity,
        "severity_counts": _counter_payload(aggregate.severity_counts),
        "source_count": len(aggregate.source_counts),
        "included_source_count": len(sources),
        "omitted_source_count": max(
            0,
            len(aggregate.source_counts) - len(sources),
        ),
        "sources": sources,
    }


def _normalized_reports(
    reports: Iterable[QualityReport | tuple[str, QualityReport]],
) -> tuple[tuple[str, QualityReport], ...]:
    normalized: list[tuple[str, QualityReport]] = []
    for index, item in enumerate(reports, start=1):
        if isinstance(item, QualityReport):
            normalized.append((f"report-{index}", item))
            continue
        source, report = item
        normalized.append((str(source), report))
    return tuple(normalized)


def _module_string_constants(tree: ast.AST) -> dict[str, str]:
    constants: dict[str, str] = {}
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id.isupper():
                constants[target.id] = node.value.value
    return constants


def _string_keyword(node: ast.Call, name: str) -> str:
    for keyword in node.keywords:
        if keyword.arg != name:
            continue
        if isinstance(keyword.value, ast.Constant):
            value = keyword.value.value
            return value if isinstance(value, str) else ""
    return ""


def _rule_id_from_call(
    node: ast.Call,
    constants: Mapping[str, str],
) -> str:
    for keyword in node.keywords:
        if keyword.arg != "rule_id":
            continue
        value = keyword.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        if isinstance(value, ast.Name):
            return constants.get(value.id, value.id)
        return _rule_id_from_expression(value, constants)
    return "unknown"


def _rule_id_from_expression(
    value: ast.AST,
    constants: Mapping[str, str],
) -> str:
    expression = ast.unparse(value)
    if expression in constants:
        return constants[expression]
    if expression.endswith(".rule_id"):
        return expression
    return "unknown"


def _severity_from_call(
    node: ast.Call,
) -> tuple[QualitySeverity, str]:
    for keyword in node.keywords:
        if keyword.arg == "severity":
            severity = _severity_from_expression(keyword.value)
            return severity, ast.unparse(keyword.value)
    return QualitySeverity.ERROR, "default_error"


def _severity_from_expression(value: ast.AST) -> QualitySeverity:
    if isinstance(value, ast.Attribute) and isinstance(value.value, ast.Name):
        if value.value.id == "QualitySeverity":
            return QualitySeverity.from_value(value.attr)
    expression = ast.unparse(value).lower()
    if "info" in expression:
        return QualitySeverity.INFO
    if "warning" in expression or "warn" in expression:
        return QualitySeverity.WARNING
    if "error" in expression or "negative_spread" in expression:
        return QualitySeverity.ERROR
    return QualitySeverity.WARNING


def _looks_like_finding_code(value: str) -> bool:
    return (
        value == value.upper()
        and "_" in value
        and any(char.isalpha() for char in value)
    )


def _relative_source(
    path: Path,
    *,
    root: Path,
    line_number: int,
) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError:
        relative = Path(path.name)
    return f"data_quality/{relative.as_posix()}:{line_number}"


def _code_aggregate_sort_key(
    aggregate: _CodeAggregate,
) -> tuple[int, int, str, str]:
    return (
        _SEVERITY_SORT.get(aggregate.max_severity, 9),
        -aggregate.occurrence_count,
        aggregate.rule_id,
        aggregate.finding_code,
    )


def _format_code_group(group: Mapping[str, JSONValue]) -> str:
    sources = _list_payload(group.get("sources"))
    source = (
        _optional_string(sources[0], "source")
        if sources and isinstance(sources[0], Mapping)
        else ""
    )
    suffix = f" source={source}" if source else ""
    omitted = _int_value(group, "omitted_source_count")
    if omitted:
        suffix += f" (+{omitted} more sources)"
    return (
        f"{_optional_string(group, 'max_severity') or 'info'} "
        f"{_optional_string(group, 'finding_code')} "
        f"rule={_optional_string(group, 'rule_id') or 'unknown'} "
        f"occurrences={_int_value(group, 'occurrence_count')}"
        f"{suffix}"
    )


def _payload_limit_metadata(
    total_count: int,
    limit: int,
) -> dict[str, JSONValue]:
    included_count = total_count if limit < 0 else min(total_count, limit)
    return {
        "limit": limit,
        "total_count": total_count,
        "included_count": included_count,
        "omitted_count": max(0, total_count - included_count),
        "truncated": total_count > included_count,
    }


def _bounded_sequence(values: Sequence[_T], *, limit: int) -> Sequence[_T]:
    if limit < 0:
        return values
    return values[:limit]


def _counter_distinct_count(values: Iterable[str]) -> int:
    return len(set(values))


def _counter_payload(counter: Counter[str]) -> dict[str, JSONValue]:
    return {
        key: counter[key]
        for key in sorted(counter, key=lambda item: (-counter[item], item))
    }


def _named_counter_payloads(
    counter: Counter[str],
    *,
    key_name: str,
    limit: int,
) -> list[JSONValue]:
    names = sorted(counter, key=lambda item: (-counter[item], item))
    if limit >= 0:
        names = names[:limit]
    return [{key_name: name, "count": counter[name]} for name in names]


def _mapping_payload(value: object) -> dict[str, JSONValue]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_payload(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _optional_string(
    mapping: Mapping[str, JSONValue],
    key: str,
) -> str:
    value = mapping.get(key)
    return value if isinstance(value, str) else ""


def _int_value(
    mapping: Mapping[str, JSONValue],
    key: str,
) -> int:
    value = mapping.get(key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return 0

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
from histdatacom.data_quality.limits import (
    BoundedReportLimit,
    bounded_report_limit,
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
_ATTRIBUTION_STATUS_SORT = {"unresolved": 0, "inferred": 1, "exact": 2}
_FINDING_CODE_RULE_PREFIXES = (
    ("ASCII_TICK_SPREAD_REGIME_", "ticks.ascii.spread_regimes"),
    ("ASCII_TICK_MICROSTRUCTURE_", "ticks.ascii.microstructure"),
    ("ASCII_TICK_ONE_SIDED_", "ticks.ascii.microstructure"),
    ("ASCII_TICK_STALE_", "ticks.ascii.microstructure"),
    ("ASCII_TICK_BURST_", "ticks.ascii.microstructure"),
    ("ASCII_TICK_BID_ASK_", "ticks.ascii.spread"),
    ("ASCII_TICK_NEGATIVE_SPREAD", "ticks.ascii.spread"),
    ("ASCII_TICK_ZERO_SPREAD", "ticks.ascii.spread"),
    ("ASCII_TICK_SPREAD_", "ticks.ascii.spread"),
    ("DOMAIN_CALENDAR_", "domain.calendar_sessions"),
    ("DOMAIN_CROSS_INSTRUMENT_", "domain.cross_instrument_consistency"),
    ("PROVENANCE_", "provenance.manifest.lineage"),
    ("ASCII_TIMESTAMP_CONTINUITY_", "time.ascii.continuity"),
    ("ASCII_TIMESTAMP_EXPECTED_SESSION_CLOSURE_GAP", "time.ascii.gaps"),
    ("ASCII_TIMESTAMP_SUSPICIOUS_GAP", "time.ascii.gaps"),
    ("ASCII_TIMESTAMP_WEEKEND_ACTIVITY", "time.ascii.gaps"),
    ("ASCII_TIMESTAMP_GAP_", "time.ascii.gaps"),
    ("ASCII_TIMESTAMP_SOURCE_", "time.ascii.est_no_dst"),
    ("ASCII_TIMESTAMP_EST_NO_DST_", "time.ascii.est_no_dst"),
    ("ASCII_TIMESTAMP_UTC_", "time.ascii.est_no_dst"),
    ("HISTDATA_FORMAT_", "inventory.format_support"),
    ("COVERAGE_", "inventory.coverage.manifest"),
    ("FINGERPRINT_", "fingerprint.series"),
)
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class KnownQualityFindingCode:
    """One known data-quality finding code emitted by source or fixtures."""

    rule_id: str
    finding_code: str
    severity: QualitySeverity = QualitySeverity.ERROR
    source: str = ""
    severity_source: str = ""
    source_family: str = ""
    source_helper: str = ""
    finding_code_prefix: str = ""
    attribution_status: str = "exact"
    attribution_reason: str = "provided_rule_id"


@dataclass(frozen=True, slots=True)
class _RuleAttribution:
    rule_id: str
    status: str
    reason: str


@dataclass(slots=True)
class _CodeAggregate:
    rule_id: str
    finding_code: str
    mapped: bool = False
    occurrence_count: int = 0
    severity_counts: Counter[str] = field(default_factory=Counter)
    source_counts: Counter[str] = field(default_factory=Counter)
    source_family_counts: Counter[str] = field(default_factory=Counter)
    source_helper_counts: Counter[str] = field(default_factory=Counter)
    finding_code_prefix_counts: Counter[str] = field(default_factory=Counter)
    attribution_status_counts: Counter[str] = field(default_factory=Counter)
    attribution_reason_counts: Counter[str] = field(default_factory=Counter)

    @property
    def max_severity(self) -> str:
        return max(
            self.severity_counts,
            key=lambda severity: _SEVERITY_RANK.get(severity, 0),
            default=QualitySeverity.INFO.value,
        )


@dataclass(slots=True)
class _ReportGapAggregate:
    rule_id: str
    finding_code: str
    occurrence_count: int = 0
    group_count: int = 0
    severity_counts: Counter[str] = field(default_factory=Counter)
    report_source_counts: Counter[str] = field(default_factory=Counter)

    @property
    def max_severity(self) -> str:
        return _max_severity(self.severity_counts)


@dataclass(slots=True)
class _ReportGapEvidence:
    exact_counts: Counter[str] = field(default_factory=Counter)
    finding_code_counts: Counter[str] = field(default_factory=Counter)
    group_counts: Counter[str] = field(default_factory=Counter)
    severity_counts: Counter[str] = field(default_factory=Counter)
    aggregates: dict[tuple[str, str], _ReportGapAggregate] = field(
        default_factory=dict
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
    code_limit: int | None = DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
    rule_limit: int | None = DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    source_limit: int | None = DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    target_axis_limit: int | None = (
        DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT
    ),
) -> dict[str, JSONValue]:
    """Return a bounded remediation-catalog completeness audit payload."""
    code_limit_state = bounded_report_limit(
        code_limit,
        default_limit=DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
    )
    rule_limit_state = bounded_report_limit(
        rule_limit,
        default_limit=DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    )
    source_limit_state = bounded_report_limit(
        source_limit,
        default_limit=DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    )
    target_axis_limit_state = bounded_report_limit(
        target_axis_limit,
        default_limit=DEFAULT_REMEDIATION_CATALOG_AUDIT_TARGET_AXIS_LIMIT,
    )
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
            code_limit=code_limit_state.effective_limit,
            target_axis_limit=target_axis_limit_state.effective_limit,
        )
        for source, report in _normalized_reports(reports)
    ]
    report_gap_evidence = _report_gap_evidence(report_payloads)
    unmapped_known = sorted(
        (
            aggregate
            for aggregate in known_aggregates.values()
            if not aggregate.mapped
        ),
        key=_code_aggregate_sort_key,
    )
    ranked_gaps = _ranked_gap_payloads(
        unmapped_known,
        report_gap_evidence=report_gap_evidence,
        source_limit=source_limit_state.effective_limit,
    )
    included_ranked_gaps = list(code_limit_state.slice(ranked_gaps))
    included_unmapped_known = code_limit_state.slice(unmapped_known)
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
            rule_limit=rule_limit_state.effective_limit,
            code_limit=code_limit_state.effective_limit,
        ),
        "known_unmapped_codes": [
            _code_aggregate_payload(
                aggregate,
                source_limit=source_limit_state.effective_limit,
            )
            for aggregate in included_unmapped_known
        ],
        "ranked_gaps": cast(JSONValue, included_ranked_gaps),
        "report_coverage": cast(JSONValue, report_payloads),
        "payload_limits": {
            "ranked_gaps": _payload_limit_metadata(
                len(ranked_gaps),
                code_limit_state,
            ),
            "known_unmapped_codes": _payload_limit_metadata(
                len(unmapped_known),
                code_limit_state,
            ),
            "known_code_sources": {
                **source_limit_state.limit_payload(),
                "applies_per_code": True,
            },
            "ranked_gap_sources": {
                **source_limit_state.limit_payload(),
                "applies_per_gap": True,
            },
            "ranked_gap_report_sources": {
                **source_limit_state.limit_payload(),
                "applies_per_gap": True,
            },
            "known_rule_id_counts": _payload_limit_metadata(
                _counter_distinct_count(
                    aggregate.rule_id for aggregate in known_aggregates.values()
                ),
                rule_limit_state,
            ),
            "known_finding_code_counts": _payload_limit_metadata(
                _counter_distinct_count(
                    aggregate.finding_code
                    for aggregate in known_aggregates.values()
                ),
                code_limit_state,
            ),
            "attribution_reason_counts": _payload_limit_metadata(
                _counter_distinct_count(
                    reason
                    for aggregate in known_aggregates.values()
                    for reason in aggregate.attribution_reason_counts
                ),
                rule_limit_state,
            ),
            "unresolved_source_helper_counts": _payload_limit_metadata(
                _counter_distinct_count(
                    helper
                    for aggregate in known_aggregates.values()
                    if aggregate.attribution_status_counts["unresolved"]
                    for helper in aggregate.source_helper_counts
                ),
                rule_limit_state,
            ),
            "unresolved_finding_code_prefix_counts": (
                _payload_limit_metadata(
                    _counter_distinct_count(
                        prefix
                        for aggregate in known_aggregates.values()
                        if aggregate.attribution_status_counts["unresolved"]
                        for prefix in aggregate.finding_code_prefix_counts
                    ),
                    rule_limit_state,
                )
            ),
            "report_unmapped_groups": {
                **code_limit_state.limit_payload(),
                "target_axis_limit": target_axis_limit_state.effective_limit,
                "target_axes": target_axis_limit_state.limit_payload(),
                "applies_per_report": True,
            },
        },
    }
    return payload


def audit_remediation_catalog_report_paths(
    report_paths: Iterable[str | Path],
    *,
    known_findings: Iterable[KnownQualityFindingCode] | None = None,
    code_limit: int | None = DEFAULT_REMEDIATION_CATALOG_AUDIT_CODE_LIMIT,
    rule_limit: int | None = DEFAULT_REMEDIATION_CATALOG_AUDIT_RULE_LIMIT,
    source_limit: int | None = DEFAULT_REMEDIATION_CATALOG_AUDIT_SOURCE_LIMIT,
    target_axis_limit: int | None = (
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
        (
            "attribution occurrences: "
            f"exact={_int_value(summary, 'exact_attribution_occurrence_count')} "
            "inferred="
            f"{_int_value(summary, 'inferred_attribution_occurrence_count')} "
            "unresolved="
            f"{_int_value(summary, 'unresolved_attribution_occurrence_count')}"
        ),
    ]
    code_counts = _mapping_payload(payload.get("known_code_counts"))
    unresolved_families = _format_named_counts(
        code_counts.get("unresolved_source_family_counts"),
        name_key="source_family",
    )
    if unresolved_families:
        lines.append(f"unresolved families: {unresolved_families}")
    unresolved_helpers = _format_named_counts(
        code_counts.get("unresolved_source_helper_counts"),
        name_key="source_helper",
    )
    if unresolved_helpers:
        lines.append(f"unresolved helpers: {unresolved_helpers}")
    report_count = _int_value(summary, "report_count")
    if report_count:
        lines.append(
            "reports: "
            f"{report_count} "
            f"findings: {_int_value(summary, 'report_finding_count')} "
            "unmapped warning/error groups: "
            f"{_int_value(summary, 'report_unmapped_warning_error_group_count')}"
        )
    ranked_groups = [
        item
        for item in _list_payload(payload.get("ranked_gaps"))
        if _optional_string(item, "max_severity") in {"error", "warning"}
    ]
    lines.extend(("", "Ranked remediation gaps"))
    if not ranked_groups:
        lines.append("- none")
    else:
        lines.extend(f"- {_format_ranked_gap(item)}" for item in ranked_groups)

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
    class_rule_ids = _class_rule_ids(tree, constants)
    parents = _parent_map(tree)
    source_family = _source_family_for_path(path)
    findings: list[KnownQualityFindingCode] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        code = _string_keyword(node, "code")
        if not code or not _looks_like_finding_code(code):
            continue
        severity, severity_source = _severity_from_call(node)
        attribution = _rule_attribution_from_call(
            node,
            code,
            tree,
            constants,
            class_rule_ids=class_rule_ids,
            parents=parents,
            source_family=source_family,
        )
        source_helper = _nearest_function_name(node, parents)
        source = _relative_source(path, root=root, line_number=node.lineno)
        findings.append(
            KnownQualityFindingCode(
                rule_id=attribution.rule_id,
                finding_code=code,
                severity=severity,
                source=source,
                severity_source=severity_source,
                source_family=source_family,
                source_helper=source_helper,
                finding_code_prefix=_finding_code_prefix(code),
                attribution_status=attribution.status,
                attribution_reason=attribution.reason,
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
        family = known.source_family or _source_family_from_source(known.source)
        if family:
            aggregate.source_family_counts[family] += 1
        if known.source_helper:
            aggregate.source_helper_counts[known.source_helper] += 1
        prefix = known.finding_code_prefix or _finding_code_prefix(
            known.finding_code
        )
        if prefix:
            aggregate.finding_code_prefix_counts[prefix] += 1
        aggregate.attribution_status_counts[
            known.attribution_status or "unresolved"
        ] += 1
        aggregate.attribution_reason_counts[
            known.attribution_reason or "no_rule_context"
        ] += 1
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


def _report_gap_evidence(
    report_payloads: Sequence[Mapping[str, JSONValue]],
) -> _ReportGapEvidence:
    evidence = _ReportGapEvidence()
    for payload in report_payloads:
        source = _optional_string(payload, "source") or "report"
        coverage = _mapping_payload(payload.get("remediation_coverage"))
        for group in _list_payload(coverage.get("unmapped_groups")):
            finding_code = _optional_string(group, "finding_code")
            if not finding_code:
                continue
            rule_id = _optional_string(group, "rule_id") or "unknown"
            key = _rule_code_key(rule_id, finding_code)
            aggregate_key = (rule_id, finding_code)
            aggregate = evidence.aggregates.setdefault(
                aggregate_key,
                _ReportGapAggregate(
                    rule_id=rule_id,
                    finding_code=finding_code,
                ),
            )
            occurrence_count = _int_value(group, "occurrence_count")
            evidence.exact_counts[key] += occurrence_count
            evidence.finding_code_counts[finding_code] += occurrence_count
            evidence.group_counts[finding_code] += 1
            aggregate.occurrence_count += occurrence_count
            aggregate.group_count += 1
            aggregate.report_source_counts[source] += 1
            group_severity_counts = _mapping_payload(
                group.get("severity_counts")
            )
            for severity, count in group_severity_counts.items():
                if isinstance(count, (int, float)):
                    int_count = int(count)
                    evidence.severity_counts[
                        f"{finding_code}\0{severity}"
                    ] += int_count
                    aggregate.severity_counts[severity] += int_count
            if not aggregate.severity_counts:
                max_severity = _optional_string(group, "max_severity")
                if max_severity:
                    aggregate.severity_counts[max_severity] += occurrence_count
    return evidence


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
    attribution_status_counts: Counter[str] = Counter()
    for aggregate in aggregates.values():
        attribution_status_counts.update(aggregate.attribution_status_counts)
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
        "exact_attribution_occurrence_count": attribution_status_counts[
            "exact"
        ],
        "inferred_attribution_occurrence_count": attribution_status_counts[
            "inferred"
        ],
        "unresolved_attribution_occurrence_count": attribution_status_counts[
            "unresolved"
        ],
        **dict(report_summary),
    }


def _ranked_gap_payloads(
    aggregates: Sequence[_CodeAggregate],
    *,
    report_gap_evidence: _ReportGapEvidence,
    source_limit: int,
) -> list[JSONValue]:
    known_keys = {
        (aggregate.rule_id, aggregate.finding_code) for aggregate in aggregates
    }
    ranked = sorted(
        [
            _ranked_gap_payload(
                aggregate,
                report_gap_evidence=report_gap_evidence,
                source_limit=source_limit,
            )
            for aggregate in aggregates
        ]
        + [
            _report_gap_payload(
                aggregate,
                source_limit=source_limit,
            )
            for key, aggregate in report_gap_evidence.aggregates.items()
            if key not in known_keys
        ],
        key=_ranked_gap_sort_key,
    )
    return [{**gap, "rank": index} for index, gap in enumerate(ranked, start=1)]


def _ranked_gap_payload(
    aggregate: _CodeAggregate,
    *,
    report_gap_evidence: _ReportGapEvidence,
    source_limit: int,
) -> dict[str, JSONValue]:
    severity_counts = _counter_payload(aggregate.severity_counts)
    report_occurrence_count = (
        report_gap_evidence.exact_counts[
            _rule_code_key(aggregate.rule_id, aggregate.finding_code)
        ]
        or report_gap_evidence.finding_code_counts[aggregate.finding_code]
    )
    sources = _named_counter_payloads(
        aggregate.source_counts,
        key_name="source",
        limit=source_limit,
    )
    source_family_counts = _named_counter_payloads(
        aggregate.source_family_counts,
        key_name="source_family",
        limit=source_limit,
    )
    source_family = _primary_source_family(aggregate.source_family_counts)
    payload: dict[str, JSONValue] = {
        "finding_code": aggregate.finding_code,
        "rule_id": aggregate.rule_id,
        "mapped": aggregate.mapped,
        "max_severity": aggregate.max_severity,
        "severity_counts": severity_counts,
        "source_family": source_family,
        "source_family_counts": source_family_counts,
        "source_helper_counts": _named_counter_payloads(
            aggregate.source_helper_counts,
            key_name="source_helper",
            limit=source_limit,
        ),
        "finding_code_prefix_counts": _named_counter_payloads(
            aggregate.finding_code_prefix_counts,
            key_name="finding_code_prefix",
            limit=source_limit,
        ),
        "attribution_status": _primary_attribution_status(aggregate),
        "attribution_reason": _primary_counter_key(
            aggregate.attribution_reason_counts
        ),
        "attribution_status_counts": _counter_payload(
            aggregate.attribution_status_counts
        ),
        "attribution_reason_counts": _counter_payload(
            aggregate.attribution_reason_counts
        ),
        "known_source_occurrence_count": aggregate.occurrence_count,
        "source_count": len(aggregate.source_counts),
        "included_source_count": len(sources),
        "omitted_source_count": max(
            0,
            len(aggregate.source_counts) - len(sources),
        ),
        "sources": sources,
        "report_occurrence_count": report_occurrence_count,
        "report_group_count": report_gap_evidence.group_counts[
            aggregate.finding_code
        ],
        "rank_reasons": _rank_reasons(
            aggregate,
            report_occurrence_count=report_occurrence_count,
            source_family=source_family,
        ),
    }
    return payload


def _report_gap_payload(
    aggregate: _ReportGapAggregate,
    *,
    source_limit: int,
) -> dict[str, JSONValue]:
    report_sources = _named_counter_payloads(
        aggregate.report_source_counts,
        key_name="source",
        limit=source_limit,
    )
    source_family = _source_family_from_rule_id(aggregate.rule_id)
    return {
        "finding_code": aggregate.finding_code,
        "rule_id": aggregate.rule_id,
        "mapped": False,
        "max_severity": aggregate.max_severity,
        "severity_counts": _counter_payload(aggregate.severity_counts),
        "source_family": source_family,
        "source_family_counts": [],
        "source_helper_counts": [],
        "finding_code_prefix_counts": [],
        "attribution_status": "runtime_report",
        "attribution_reason": "report_rule_id",
        "attribution_status_counts": {},
        "attribution_reason_counts": {},
        "known_source_occurrence_count": 0,
        "source_count": 0,
        "included_source_count": 0,
        "omitted_source_count": 0,
        "sources": [],
        "report_occurrence_count": aggregate.occurrence_count,
        "report_group_count": aggregate.group_count,
        "report_source_count": len(aggregate.report_source_counts),
        "included_report_source_count": len(report_sources),
        "omitted_report_source_count": max(
            0,
            len(aggregate.report_source_counts) - len(report_sources),
        ),
        "reports": report_sources,
        "rank_reasons": _report_rank_reasons(
            aggregate,
            source_family=source_family,
        ),
    }


def _ranked_gap_sort_key(
    gap: Mapping[str, JSONValue],
) -> tuple[int, int, int, int, int, int, str, str, str]:
    severity_counts = _mapping_payload(gap.get("severity_counts"))
    max_severity = _optional_string(gap, "max_severity")
    return (
        0 if max_severity in {"error", "warning"} else 1,
        _SEVERITY_SORT.get(max_severity, 9),
        -_int_value(severity_counts, "error"),
        -_int_value(severity_counts, "warning"),
        -_int_value(gap, "report_occurrence_count"),
        -_int_value(gap, "known_source_occurrence_count"),
        _optional_string(gap, "source_family"),
        _optional_string(gap, "finding_code"),
        _optional_string(gap, "rule_id"),
    )


def _rank_reasons(
    aggregate: _CodeAggregate,
    *,
    report_occurrence_count: int,
    source_family: str,
) -> list[JSONValue]:
    reasons: list[JSONValue] = [
        f"severity={aggregate.max_severity}",
        f"source_family={source_family or 'unknown'}",
        f"known_sources={aggregate.occurrence_count}",
    ]
    if report_occurrence_count:
        reasons.insert(2, f"report_occurrences={report_occurrence_count}")
    return reasons


def _report_rank_reasons(
    aggregate: _ReportGapAggregate,
    *,
    source_family: str,
) -> list[JSONValue]:
    return [
        f"severity={aggregate.max_severity}",
        f"source_family={source_family or 'unknown'}",
        f"report_occurrences={aggregate.occurrence_count}",
        "known_sources=0",
    ]


def _known_code_counts(
    aggregates: Mapping[tuple[str, str], _CodeAggregate],
    *,
    rule_limit: int,
    code_limit: int,
) -> dict[str, JSONValue]:
    severity_counts: Counter[str] = Counter()
    rule_id_counts: Counter[str] = Counter()
    source_family_counts: Counter[str] = Counter()
    finding_code_counts: Counter[str] = Counter()
    unmapped_rule_id_counts: Counter[str] = Counter()
    unmapped_source_family_counts: Counter[str] = Counter()
    unmapped_finding_code_counts: Counter[str] = Counter()
    attribution_status_counts: Counter[str] = Counter()
    attribution_reason_counts: Counter[str] = Counter()
    unresolved_source_family_counts: Counter[str] = Counter()
    unresolved_source_helper_counts: Counter[str] = Counter()
    unresolved_finding_code_prefix_counts: Counter[str] = Counter()
    for aggregate in aggregates.values():
        severity_counts.update(aggregate.severity_counts)
        rule_id_counts[aggregate.rule_id] += aggregate.occurrence_count
        source_family_counts.update(aggregate.source_family_counts)
        finding_code_counts[
            aggregate.finding_code
        ] += aggregate.occurrence_count
        attribution_status_counts.update(aggregate.attribution_status_counts)
        attribution_reason_counts.update(aggregate.attribution_reason_counts)
        if aggregate.attribution_status_counts["unresolved"]:
            unresolved_source_family_counts.update(
                aggregate.source_family_counts
            )
            unresolved_source_helper_counts.update(
                aggregate.source_helper_counts
            )
            unresolved_finding_code_prefix_counts.update(
                aggregate.finding_code_prefix_counts
            )
        if not aggregate.mapped:
            unmapped_rule_id_counts[
                aggregate.rule_id
            ] += aggregate.occurrence_count
            unmapped_source_family_counts.update(aggregate.source_family_counts)
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
        "source_family_counts": _named_counter_payloads(
            source_family_counts,
            key_name="source_family",
            limit=rule_limit,
        ),
        "finding_code_counts": _named_counter_payloads(
            finding_code_counts,
            key_name="finding_code",
            limit=code_limit,
        ),
        "attribution_status_counts": _counter_payload(
            attribution_status_counts
        ),
        "attribution_reason_counts": _named_counter_payloads(
            attribution_reason_counts,
            key_name="attribution_reason",
            limit=rule_limit,
        ),
        "unresolved_source_family_counts": _named_counter_payloads(
            unresolved_source_family_counts,
            key_name="source_family",
            limit=rule_limit,
        ),
        "unresolved_source_helper_counts": _named_counter_payloads(
            unresolved_source_helper_counts,
            key_name="source_helper",
            limit=rule_limit,
        ),
        "unresolved_finding_code_prefix_counts": _named_counter_payloads(
            unresolved_finding_code_prefix_counts,
            key_name="finding_code_prefix",
            limit=rule_limit,
        ),
        "unmapped_rule_id_counts": _named_counter_payloads(
            unmapped_rule_id_counts,
            key_name="rule_id",
            limit=rule_limit,
        ),
        "unmapped_source_family_counts": _named_counter_payloads(
            unmapped_source_family_counts,
            key_name="source_family",
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
    source_family_counts = _named_counter_payloads(
        aggregate.source_family_counts,
        key_name="source_family",
        limit=source_limit,
    )
    return {
        "rule_id": aggregate.rule_id,
        "finding_code": aggregate.finding_code,
        "mapped": aggregate.mapped,
        "occurrence_count": aggregate.occurrence_count,
        "max_severity": aggregate.max_severity,
        "severity_counts": _counter_payload(aggregate.severity_counts),
        "source_family": _primary_source_family(aggregate.source_family_counts),
        "source_family_counts": source_family_counts,
        "source_helper_counts": _named_counter_payloads(
            aggregate.source_helper_counts,
            key_name="source_helper",
            limit=source_limit,
        ),
        "finding_code_prefix_counts": _named_counter_payloads(
            aggregate.finding_code_prefix_counts,
            key_name="finding_code_prefix",
            limit=source_limit,
        ),
        "attribution_status": _primary_attribution_status(aggregate),
        "attribution_reason": _primary_counter_key(
            aggregate.attribution_reason_counts
        ),
        "attribution_status_counts": _counter_payload(
            aggregate.attribution_status_counts
        ),
        "attribution_reason_counts": _counter_payload(
            aggregate.attribution_reason_counts
        ),
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


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    return {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }


def _class_rule_ids(
    tree: ast.AST,
    constants: Mapping[str, str],
) -> dict[str, str]:
    rule_ids: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for child in node.body:
            value: ast.AST | None = None
            if isinstance(child, ast.AnnAssign):
                if isinstance(child.target, ast.Name):
                    if child.target.id == "rule_id":
                        value = child.value
            elif isinstance(child, ast.Assign):
                if any(
                    isinstance(target, ast.Name) and target.id == "rule_id"
                    for target in child.targets
                ):
                    value = child.value
            if value is None:
                continue
            rule_id = _rule_id_literal(value, constants)
            if rule_id:
                rule_ids[node.name] = rule_id
                break
    return rule_ids


def _string_keyword(node: ast.Call, name: str) -> str:
    for keyword in node.keywords:
        if keyword.arg != name:
            continue
        if isinstance(keyword.value, ast.Constant):
            value = keyword.value.value
            return value if isinstance(value, str) else ""
    return ""


def _rule_attribution_from_call(
    node: ast.Call,
    finding_code: str,
    tree: ast.AST,
    constants: Mapping[str, str],
    *,
    class_rule_ids: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
    source_family: str,
) -> _RuleAttribution:
    explicit = _explicit_rule_attribution(
        node,
        constants,
        class_rule_ids=class_rule_ids,
        parents=parents,
        source_family=source_family,
    )
    if explicit is not None:
        return explicit

    helper_candidates = _enclosing_function_rule_candidates(
        node,
        tree,
        constants,
        class_rule_ids=class_rule_ids,
        parents=parents,
        source_family=source_family,
    )
    if len(helper_candidates) == 1:
        return _RuleAttribution(
            rule_id=next(iter(helper_candidates)),
            status="inferred",
            reason="unique_helper_rule",
        )

    prefix_rule_id = _finding_code_rule_id(finding_code)
    if prefix_rule_id and (
        not helper_candidates or prefix_rule_id in helper_candidates
    ):
        return _RuleAttribution(
            rule_id=prefix_rule_id,
            status="inferred",
            reason="finding_code_prefix",
        )

    module_rule_ids = set(class_rule_ids.values())
    if len(module_rule_ids) == 1:
        return _RuleAttribution(
            rule_id=next(iter(module_rule_ids)),
            status="inferred",
            reason="unique_module_rule",
        )

    if len(helper_candidates) > 1:
        reason = "ambiguous_helper_rules"
    elif len(module_rule_ids) > 1:
        reason = "multiple_module_rules"
    else:
        reason = "no_rule_context"
    return _RuleAttribution(
        rule_id=_unresolved_rule_id(source_family),
        status="unresolved",
        reason=reason,
    )


def _explicit_rule_attribution(
    node: ast.Call,
    constants: Mapping[str, str],
    *,
    class_rule_ids: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
    source_family: str,
) -> _RuleAttribution | None:
    for keyword in node.keywords:
        if keyword.arg != "rule_id":
            continue
        value = keyword.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return _RuleAttribution(
                rule_id=value.value,
                status="exact",
                reason="literal_rule_id",
            )
        if isinstance(value, ast.Name) and value.id in constants:
            return _RuleAttribution(
                rule_id=constants[value.id],
                status="exact",
                reason="module_constant",
            )
        rule_id = _rule_id_from_expression(
            value,
            constants,
            class_rule_ids=class_rule_ids,
            parents=parents,
            source_family=source_family,
        )
        if not rule_id.endswith(".unresolved"):
            is_self = (
                isinstance(value, ast.Attribute)
                and isinstance(value.value, ast.Name)
                and value.value.id == "self"
            )
            return _RuleAttribution(
                rule_id=rule_id,
                status="exact" if is_self else "inferred",
                reason=("class_rule_id" if is_self else "local_rule_object"),
            )
    return None


def _enclosing_function_rule_candidates(
    node: ast.AST,
    tree: ast.AST,
    constants: Mapping[str, str],
    *,
    class_rule_ids: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
    source_family: str,
) -> set[str]:
    function = _nearest_function(node, parents)
    if function is None:
        return set()
    return _function_rule_candidates(
        function,
        tree,
        constants,
        class_rule_ids=class_rule_ids,
        parents=parents,
        source_family=source_family,
        visited=set(),
    )


def _function_rule_candidates(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    tree: ast.AST,
    constants: Mapping[str, str],
    *,
    class_rule_ids: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
    source_family: str,
    visited: set[int],
) -> set[str]:
    identity = id(function)
    if identity in visited:
        return set()
    visited = {*visited, identity}
    candidates: set[str] = set()

    class_rule_id = _nearest_class_rule_id(
        function,
        parents=parents,
        class_rule_ids=class_rule_ids,
    )
    if class_rule_id:
        candidates.add(class_rule_id)
    candidates.update(
        _function_rule_id_annotations(function, class_rule_ids).values()
    )
    candidates.update(
        _function_rule_id_assertions(function, class_rule_ids).values()
    )
    candidates.update(
        _function_rule_id_assignments(function, class_rule_ids).values()
    )
    default_rule_id = _function_default_rule_id(function, constants)
    if default_rule_id:
        candidates.add(default_rule_id)

    if class_rule_id:
        return candidates

    for call in _calls_to_function(tree, function.name):
        argument_rule_id = _call_rule_id_argument(
            call,
            function,
            constants,
            class_rule_ids=class_rule_ids,
            parents=parents,
            source_family=source_family,
        )
        if argument_rule_id:
            candidates.add(argument_rule_id)
        caller_class_rule_id = _nearest_class_rule_id(
            call,
            parents=parents,
            class_rule_ids=class_rule_ids,
        )
        if caller_class_rule_id:
            candidates.add(caller_class_rule_id)
            continue
        caller = _nearest_function(call, parents)
        if caller is None or caller is function:
            continue
        candidates.update(
            _function_rule_candidates(
                caller,
                tree,
                constants,
                class_rule_ids=class_rule_ids,
                parents=parents,
                source_family=source_family,
                visited=visited,
            )
        )
    return candidates


def _calls_to_function(
    tree: ast.AST, function_name: str
) -> tuple[ast.Call, ...]:
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _call_function_name(node.func) == function_name
    ]
    return tuple(sorted(calls, key=lambda item: (item.lineno, item.col_offset)))


def _call_function_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _call_rule_id_argument(
    call: ast.Call,
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    constants: Mapping[str, str],
    *,
    class_rule_ids: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
    source_family: str,
) -> str:
    value: ast.AST | None = None
    for keyword in call.keywords:
        if keyword.arg == "rule_id":
            value = keyword.value
            break
    if value is None:
        args = list(function.args.posonlyargs) + list(function.args.args)
        rule_indexes = [
            index for index, arg in enumerate(args) if arg.arg == "rule_id"
        ]
        if rule_indexes and rule_indexes[0] < len(call.args):
            value = call.args[rule_indexes[0]]
    if value is None:
        return ""
    rule_id = _rule_id_from_expression(
        value,
        constants,
        class_rule_ids=class_rule_ids,
        parents=parents,
        source_family=source_family,
    )
    return "" if rule_id.endswith(".unresolved") else rule_id


def _function_default_rule_id(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    constants: Mapping[str, str],
) -> str:
    positional = list(function.args.posonlyargs) + list(function.args.args)
    defaults = list(function.args.defaults)
    if defaults:
        for arg, default in zip(positional[-len(defaults) :], defaults):
            if arg.arg == "rule_id":
                return _rule_id_literal(default, constants)
    for arg, kw_default in zip(
        function.args.kwonlyargs,
        function.args.kw_defaults,
    ):
        if arg.arg == "rule_id":
            return _rule_id_literal(kw_default, constants)
    return ""


def _rule_id_from_expression(
    value: ast.AST,
    constants: Mapping[str, str],
    *,
    class_rule_ids: Mapping[str, str],
    parents: Mapping[ast.AST, ast.AST],
    source_family: str,
) -> str:
    expression = ast.unparse(value)
    if expression in constants:
        return constants[expression]
    literal = _rule_id_literal(value, constants)
    if literal:
        return literal
    if isinstance(value, ast.Attribute) and value.attr == "rule_id":
        owner = value.value
        if isinstance(owner, ast.Name):
            if owner.id == "self":
                class_rule_id = _nearest_class_rule_id(
                    value,
                    parents=parents,
                    class_rule_ids=class_rule_ids,
                )
                if class_rule_id:
                    return class_rule_id
            function_rule_id = _function_rule_id_for_name(
                value,
                owner.id,
                parents=parents,
                class_rule_ids=class_rule_ids,
            )
            if function_rule_id:
                return function_rule_id
    return _unresolved_rule_id(source_family)


def _rule_id_literal(
    value: ast.AST | None,
    constants: Mapping[str, str],
) -> str:
    if value is None:
        return ""
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    if isinstance(value, ast.Name):
        return constants.get(value.id, "")
    if isinstance(value, ast.Call):
        for keyword in value.keywords:
            if keyword.arg == "default":
                return _rule_id_literal(keyword.value, constants)
    return ""


def _nearest_class_rule_id(
    node: ast.AST,
    *,
    parents: Mapping[ast.AST, ast.AST],
    class_rule_ids: Mapping[str, str],
) -> str:
    parent = parents.get(node)
    while parent is not None:
        if isinstance(parent, ast.ClassDef):
            return class_rule_ids.get(parent.name, "")
        parent = parents.get(parent)
    return ""


def _function_rule_id_for_name(
    node: ast.AST,
    name: str,
    *,
    parents: Mapping[ast.AST, ast.AST],
    class_rule_ids: Mapping[str, str],
) -> str:
    function = _nearest_function(node, parents)
    if function is None:
        return ""
    annotations = _function_rule_id_annotations(function, class_rule_ids)
    if name in annotations:
        return annotations[name]
    asserted = _function_rule_id_assertions(function, class_rule_ids)
    if name in asserted:
        return asserted[name]
    assigned = _function_rule_id_assignments(function, class_rule_ids)
    return assigned.get(name, "")


def _nearest_function(
    node: ast.AST,
    parents: Mapping[ast.AST, ast.AST],
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    parent = parents.get(node)
    while parent is not None:
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return parent
        parent = parents.get(parent)
    return None


def _nearest_function_name(
    node: ast.AST,
    parents: Mapping[ast.AST, ast.AST],
) -> str:
    function = _nearest_function(node, parents)
    return function.name if function is not None else "<module>"


def _function_rule_id_annotations(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    class_rule_ids: Mapping[str, str],
) -> dict[str, str]:
    annotated: dict[str, str] = {}
    args = (
        list(function.args.args)
        + list(function.args.kwonlyargs)
        + list(function.args.posonlyargs)
    )
    for arg in args:
        class_name = _annotation_name(arg.annotation)
        if class_name in class_rule_ids:
            annotated[arg.arg] = class_rule_ids[class_name]
    return annotated


def _function_rule_id_assertions(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    class_rule_ids: Mapping[str, str],
) -> dict[str, str]:
    asserted: dict[str, str] = {}
    for node in ast.walk(function):
        if not isinstance(node, ast.Assert):
            continue
        test = node.test
        if not isinstance(test, ast.Call):
            continue
        if not _call_name_is(test.func, "isinstance"):
            continue
        if len(test.args) < 2:
            continue
        target, class_expr = test.args[0], test.args[1]
        if not isinstance(target, ast.Name):
            continue
        class_name = _annotation_name(class_expr)
        if class_name in class_rule_ids:
            asserted[target.id] = class_rule_ids[class_name]
    return asserted


def _function_rule_id_assignments(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    class_rule_ids: Mapping[str, str],
) -> dict[str, str]:
    assigned: dict[str, str] = {}
    for node in ast.walk(function):
        target: ast.Name | None = None
        value: ast.AST | None = None
        annotation: ast.AST | None = None
        if isinstance(node, ast.AnnAssign) and isinstance(
            node.target, ast.Name
        ):
            target = node.target
            value = node.value
            annotation = node.annotation
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            if isinstance(node.targets[0], ast.Name):
                target = node.targets[0]
                value = node.value
        if target is None:
            continue
        class_name = _annotation_name(annotation)
        if not class_name and isinstance(value, ast.Call):
            class_name = _annotation_name(value.func)
        if class_name in class_rule_ids:
            assigned[target.id] = class_rule_ids[class_name]
    return assigned


def _annotation_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _finding_code_rule_id(finding_code: str) -> str:
    for prefix, rule_id in _FINDING_CODE_RULE_PREFIXES:
        if finding_code.startswith(prefix):
            return rule_id
    return ""


def _finding_code_prefix(finding_code: str) -> str:
    for prefix, _rule_id in _FINDING_CODE_RULE_PREFIXES:
        if finding_code.startswith(prefix):
            return prefix.removesuffix("_")
    parts = [part for part in finding_code.split("_") if part]
    return "_".join(parts[:3])


def _call_name_is(node: ast.AST, name: str) -> bool:
    return isinstance(node, ast.Name) and node.id == name


def _source_family_for_path(path: Path) -> str:
    name = path.name
    if name in {"calendar.py"}:
        return "domain/calendar"
    if name in {"symbols.py"}:
        return "domain"
    if name in {"format_support.py", "inventory.py", "manifest.py"}:
        return "inventory"
    family_by_name = {
        "fingerprints.py": "fingerprint",
        "ingestion.py": "ingestion",
        "modeling.py": "modeling",
        "provenance.py": "provenance",
        "ticks.py": "ticks",
        "time.py": "time",
    }
    return family_by_name.get(name, path.stem)


def _source_family_from_source(source: str) -> str:
    if not source.startswith("data_quality/"):
        return ""
    source_path = source.removeprefix("data_quality/").split(":", 1)[0]
    return _source_family_for_path(Path(source_path))


def _source_family_from_rule_id(rule_id: str) -> str:
    if rule_id.startswith("domain.calendar."):
        return "domain/calendar"
    if not rule_id or rule_id == "unknown":
        return ""
    return rule_id.split(".", 1)[0]


def _primary_source_family(counter: Counter[str]) -> str:
    if not counter:
        return ""
    return sorted(counter, key=lambda item: (-counter[item], item))[0]


def _primary_counter_key(counter: Counter[str]) -> str:
    if not counter:
        return ""
    return sorted(counter, key=lambda item: (-counter[item], item))[0]


def _primary_attribution_status(aggregate: _CodeAggregate) -> str:
    if not aggregate.attribution_status_counts:
        return "unresolved"
    return min(
        aggregate.attribution_status_counts,
        key=lambda item: (_ATTRIBUTION_STATUS_SORT.get(item, 9), item),
    )


def _max_severity(counter: Counter[str]) -> str:
    return max(
        counter,
        key=lambda severity: _SEVERITY_RANK.get(severity, 0),
        default=QualitySeverity.INFO.value,
    )


def _unresolved_rule_id(source_family: str) -> str:
    normalized = source_family.replace("/", ".") or "unknown"
    return f"{normalized}.unresolved"


def _rule_code_key(rule_id: str, finding_code: str) -> str:
    return f"{rule_id}\0{finding_code}"


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
    attribution = _optional_string(group, "attribution_status")
    attribution_reason = _optional_string(group, "attribution_reason")
    if attribution:
        suffix += f" attribution={attribution}"
    if attribution_reason:
        suffix += f"({attribution_reason})"
    return (
        f"{_optional_string(group, 'max_severity') or 'info'} "
        f"{_optional_string(group, 'finding_code')} "
        f"rule={_optional_string(group, 'rule_id') or 'unknown'} "
        f"occurrences={_int_value(group, 'occurrence_count')}"
        f"{suffix}"
    )


def _format_ranked_gap(group: Mapping[str, JSONValue]) -> str:
    reasons = _string_list(group.get("rank_reasons"))
    reason_text = f" reasons={'; '.join(reasons)}" if reasons else ""
    attribution = _optional_string(group, "attribution_status") or "unknown"
    attribution_reason = _optional_string(group, "attribution_reason")
    attribution_text = f" attribution={attribution}"
    if attribution_reason:
        attribution_text += f"({attribution_reason})"
    return (
        f"#{_int_value(group, 'rank')} "
        f"{_optional_string(group, 'max_severity') or 'info'} "
        f"{_optional_string(group, 'finding_code')} "
        f"family={_optional_string(group, 'source_family') or 'unknown'} "
        f"rule={_optional_string(group, 'rule_id') or 'unknown'} "
        f"reports={_int_value(group, 'report_occurrence_count')} "
        f"known_sources={_int_value(group, 'known_source_occurrence_count')}"
        f"{attribution_text}"
        f"{reason_text}"
    )


def _format_named_counts(value: object, *, name_key: str) -> str:
    parts = []
    for item in _list_payload(value):
        name = _optional_string(item, name_key)
        if name:
            parts.append(f"{name}={_int_value(item, 'count')}")
    return ", ".join(parts)


def _payload_limit_metadata(
    total_count: int,
    limit: int | BoundedReportLimit | None,
) -> dict[str, JSONValue]:
    if isinstance(limit, BoundedReportLimit):
        limit_state = limit
    else:
        limit_state = bounded_report_limit(
            limit,
            default_limit=limit if isinstance(limit, int) else 0,
        )
    payload: dict[str, JSONValue] = limit_state.count_payload(total_count)
    return payload


def _bounded_sequence(
    values: Sequence[_T],
    *,
    limit: int | BoundedReportLimit,
) -> Sequence[_T]:
    if isinstance(limit, BoundedReportLimit):
        return cast(Sequence[_T], limit.slice(values))
    return cast(
        Sequence[_T],
        bounded_report_limit(limit, default_limit=limit).slice(values),
    )


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
    limit_state = bounded_report_limit(limit, default_limit=limit)
    names = limit_state.slice(names)
    return [{key_name: name, "count": counter[name]} for name in names]


def _mapping_payload(value: object) -> dict[str, JSONValue]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_payload(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


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

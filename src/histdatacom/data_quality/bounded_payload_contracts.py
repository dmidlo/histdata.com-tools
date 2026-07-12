"""Application-owned contract checks for bounded quality-report payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.engine import (
    QUALITY_ENGINE_METADATA_KEY,
    run_quality_assessment,
)
from histdatacom.data_quality.fingerprints import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    CROSS_SERIES_FINGERPRINT_RULE_ID,
    CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_PARITY_SCHEMA_VERSION,
)
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.profiles import QUALITY_REPORTING_METADATA_KEY
from histdatacom.data_quality.synthetic_constraints import (
    synthetic_constraints_from_fingerprint,
)
from histdatacom.data_quality.reporting import (
    QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY,
    QualityExitPolicy,
    bounded_quality_payload,
)
from histdatacom.publication_safety import publish_safe_json_mapping
from histdatacom.runtime_contracts import JSONValue

BOUNDED_PAYLOAD_CONTRACT_AUDIT_SCHEMA_VERSION = (
    "histdatacom.bounded-payload-contract-audit.v1"
)

_REQUIRED_LIMIT_KEYS = frozenset(
    (
        "limit",
        "effective_limit",
        "requested_limit",
        "default_limit",
        "minimum_limit",
        "maximum_limit",
        "unbounded",
    )
)
_COUNT_KEYS = frozenset(
    ("total_count", "included_count", "omitted_count", "truncated")
)


@dataclass(frozen=True, slots=True)
class _BoundedSequenceContract:
    name: str
    metadata_path: tuple[str, ...]
    sequence_path: tuple[str, ...]
    total_path: tuple[str, ...]
    included_path: tuple[str, ...]
    omitted_path: tuple[str, ...]
    truncated_path: tuple[str, ...]


_SEQUENCE_CONTRACTS: tuple[_BoundedSequenceContract, ...] = (
    _BoundedSequenceContract(
        "discovery_targets",
        ("payload_limits", "discovery_targets"),
        ("discovery", "targets"),
        ("discovery", "target_count"),
        ("discovery", "target_included_count"),
        ("discovery", "target_omitted_count"),
        ("payload_limits", "discovery_targets", "truncated"),
    ),
    _BoundedSequenceContract(
        "target_summaries",
        ("payload_limits", "target_summaries"),
        ("target_summaries",),
        ("payload_limits", "target_summaries", "total_count"),
        ("payload_limits", "target_summaries", "included_count"),
        ("payload_limits", "target_summaries", "omitted_count"),
        ("payload_limits", "target_summaries", "truncated"),
    ),
    _BoundedSequenceContract(
        "cross_target_summaries",
        ("payload_limits", "cross_target_summaries"),
        ("cross_target_summaries",),
        ("payload_limits", "cross_target_summaries", "total_count"),
        ("payload_limits", "cross_target_summaries", "included_count"),
        ("payload_limits", "cross_target_summaries", "omitted_count"),
        ("payload_limits", "cross_target_summaries", "truncated"),
    ),
    _BoundedSequenceContract(
        "next_actions",
        ("payload_limits", "next_actions"),
        ("next_actions", "actions"),
        ("next_actions", "action_count"),
        ("next_actions", "included_action_count"),
        ("next_actions", "omitted_action_count"),
        ("next_actions", "truncated"),
    ),
    _BoundedSequenceContract(
        "remediation_coverage",
        ("payload_limits", "remediation_coverage"),
        ("remediation_coverage", "unmapped_groups"),
        ("remediation_coverage", "unmapped_group_count"),
        ("remediation_coverage", "included_unmapped_group_count"),
        ("remediation_coverage", "omitted_unmapped_group_count"),
        ("remediation_coverage", "unmapped_truncated"),
    ),
    _BoundedSequenceContract(
        "remediation_catalog_audit",
        ("payload_limits", "remediation_catalog_audit"),
        ("remediation_catalog_audit", "ranked_gaps"),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "ranked_gaps",
            "total_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "ranked_gaps",
            "included_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "ranked_gaps",
            "omitted_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "ranked_gaps",
            "truncated",
        ),
    ),
    _BoundedSequenceContract(
        "remediation_plan",
        (
            "remediation_catalog_audit",
            "payload_limits",
            "remediation_plan",
        ),
        ("remediation_catalog_audit", "remediation_plan", "items"),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "remediation_plan",
            "total_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "remediation_plan",
            "included_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "remediation_plan",
            "omitted_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "remediation_plan",
            "truncated",
        ),
    ),
    _BoundedSequenceContract(
        "remediation_attribution_reasons",
        (
            "remediation_catalog_audit",
            "payload_limits",
            "attribution_reason_counts",
        ),
        (
            "remediation_catalog_audit",
            "known_code_counts",
            "attribution_reason_counts",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "attribution_reason_counts",
            "total_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "attribution_reason_counts",
            "included_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "attribution_reason_counts",
            "omitted_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "attribution_reason_counts",
            "truncated",
        ),
    ),
    _BoundedSequenceContract(
        "remediation_unresolved_helpers",
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_source_helper_counts",
        ),
        (
            "remediation_catalog_audit",
            "known_code_counts",
            "unresolved_source_helper_counts",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_source_helper_counts",
            "total_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_source_helper_counts",
            "included_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_source_helper_counts",
            "omitted_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_source_helper_counts",
            "truncated",
        ),
    ),
    _BoundedSequenceContract(
        "remediation_unresolved_prefixes",
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_finding_code_prefix_counts",
        ),
        (
            "remediation_catalog_audit",
            "known_code_counts",
            "unresolved_finding_code_prefix_counts",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_finding_code_prefix_counts",
            "total_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_finding_code_prefix_counts",
            "included_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_finding_code_prefix_counts",
            "omitted_count",
        ),
        (
            "remediation_catalog_audit",
            "payload_limits",
            "unresolved_finding_code_prefix_counts",
            "truncated",
        ),
    ),
    _BoundedSequenceContract(
        "quality_engine_skip_events",
        ("quality_engine", "skip_events", "limit_metadata", "events"),
        ("quality_engine", "skip_events", "events"),
        ("quality_engine", "skip_events", "event_count"),
        ("quality_engine", "skip_events", "included_event_count"),
        ("quality_engine", "skip_events", "omitted_event_count"),
        ("quality_engine", "skip_events", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_distribution",
        ("fingerprint_distribution", "limit_metadata", "targets"),
        ("fingerprint_distribution", "target_summaries"),
        ("fingerprint_distribution", "target_count"),
        ("fingerprint_distribution", "included_target_count"),
        ("fingerprint_distribution", "omitted_target_count"),
        ("fingerprint_distribution", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_distribution_attention",
        ("fingerprint_distribution_attention", "limit_metadata", "targets"),
        ("fingerprint_distribution_attention", "target_summaries"),
        ("fingerprint_distribution_attention", "attention_target_count"),
        (
            "fingerprint_distribution_attention",
            "included_attention_target_count",
        ),
        (
            "fingerprint_distribution_attention",
            "omitted_attention_target_count",
        ),
        ("fingerprint_distribution_attention", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_topology",
        ("fingerprint_topology", "limit_metadata", "targets"),
        ("fingerprint_topology", "target_summaries"),
        ("fingerprint_topology", "target_count"),
        ("fingerprint_topology", "included_target_count"),
        ("fingerprint_topology", "omitted_target_count"),
        ("fingerprint_topology", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_cross_series",
        (
            "fingerprint_cross_series",
            "limit_metadata",
            "groups",
        ),
        ("fingerprint_cross_series", "groups"),
        ("fingerprint_cross_series", "group_count"),
        ("fingerprint_cross_series", "included_group_count"),
        ("fingerprint_cross_series", "omitted_group_count"),
        ("fingerprint_cross_series", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_topology_attention",
        ("fingerprint_topology_attention", "limit_metadata", "targets"),
        ("fingerprint_topology_attention", "target_summaries"),
        ("fingerprint_topology_attention", "attention_target_count"),
        ("fingerprint_topology_attention", "included_attention_target_count"),
        ("fingerprint_topology_attention", "omitted_attention_target_count"),
        ("fingerprint_topology_attention", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_regime",
        ("fingerprint_regime", "limit_metadata", "targets"),
        ("fingerprint_regime", "target_summaries"),
        ("fingerprint_regime", "target_count"),
        ("fingerprint_regime", "included_target_count"),
        ("fingerprint_regime", "omitted_target_count"),
        ("fingerprint_regime", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_readiness",
        ("fingerprint_readiness", "limit_metadata", "targets"),
        ("fingerprint_readiness", "target_summaries"),
        ("fingerprint_readiness", "target_count"),
        ("fingerprint_readiness", "included_target_count"),
        ("fingerprint_readiness", "omitted_target_count"),
        ("fingerprint_readiness", "truncated"),
    ),
    _BoundedSequenceContract(
        "fingerprint_readiness_risk",
        ("fingerprint_readiness_risk", "limit_metadata", "targets"),
        ("fingerprint_readiness_risk", "target_risks"),
        ("fingerprint_readiness_risk", "risk_target_count"),
        ("fingerprint_readiness_risk", "included_target_count"),
        ("fingerprint_readiness_risk", "omitted_target_count"),
        ("fingerprint_readiness_risk", "truncated"),
    ),
)


def bounded_payload_contract_audit(
    payload: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    """Validate bounded payload metadata against generated report surfaces."""
    payload_source = "provided" if payload is not None else "representative"
    bounded_payload = (
        publish_safe_json_mapping(dict(payload))
        if payload is not None
        else representative_bounded_quality_payload()
    )
    findings: list[dict[str, JSONValue]] = []
    checks: list[dict[str, JSONValue]] = []

    _record_check(
        checks,
        findings,
        "required_surfaces",
        _audit_required_surfaces(bounded_payload, findings),
    )
    _record_check(
        checks,
        findings,
        "limit_metadata",
        _audit_limit_metadata_surfaces(bounded_payload, findings),
    )
    _record_check(
        checks,
        findings,
        "sequence_counts",
        _audit_sequence_contracts(bounded_payload, findings),
    )

    error_count = sum(
        1 for finding in findings if finding["severity"] == "error"
    )
    warning_count = sum(
        1 for finding in findings if finding["severity"] == "warning"
    )
    audit_payload: dict[str, JSONValue] = {
        "schema_version": BOUNDED_PAYLOAD_CONTRACT_AUDIT_SCHEMA_VERSION,
        "status": "fail" if error_count else "pass",
        "payload_source": payload_source,
        "check_count": len(checks),
        "error_count": error_count,
        "warning_count": warning_count,
        "finding_count": len(findings),
        "checked_surfaces": {
            "sequence_contract_count": len(_SEQUENCE_CONTRACTS),
            "limit_metadata_count": sum(
                1 for _path, _metadata in _iter_limit_payloads(bounded_payload)
            ),
        },
        "checks": cast(JSONValue, checks),
        "findings": cast(JSONValue, findings),
        "non_goals": [
            "does not read local market data",
            "does not mutate caches or reports",
            "does not automate GitHub, CI, merge, or release workflow",
        ],
    }
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(
        audit_payload
    )
    return safe_payload


def representative_bounded_quality_payload() -> dict[str, JSONValue]:
    """Return a generated payload that exercises bounded report surfaces."""
    report = representative_quality_report()
    payload = bounded_quality_payload(
        operation="data-quality-contract-audit",
        check_groups=("fingerprint", "time", "domain"),
        discovery={
            "roots": ["data"],
            "target_count": len(report.targets),
            "targets": [
                {
                    "path": target.path,
                    "kind": target.kind.value,
                    "data_format": target.data_format,
                    "timeframe": target.timeframe,
                    "symbol": target.symbol,
                    "period": target.period,
                }
                for target in report.targets
            ],
        },
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
        discovery_target_limit=2,
        target_summary_limit=2,
        cross_target_summary_limit=2,
    )
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def representative_quality_report() -> QualityReport:
    """Return a generated report that exercises public quality surfaces."""
    return _representative_quality_report()


def format_bounded_payload_contract_audit(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return concise human-readable bounded payload audit text."""
    lines = [
        "Bounded Payload Contract Audit",
        f"status: {payload.get('status', 'unknown')}",
        f"checks: {payload.get('check_count', 0)}",
        f"findings: {payload.get('finding_count', 0)}",
        f"errors: {payload.get('error_count', 0)}",
        f"warnings: {payload.get('warning_count', 0)}",
    ]
    for check in _list_of_mappings(payload.get("checks")):
        lines.append(
            "- "
            f"{check.get('name', 'unknown')}: "
            f"{check.get('status', 'unknown')} "
            f"(checked={check.get('checked_count', 0)}, "
            f"errors={check.get('error_count', 0)}, "
            f"warnings={check.get('warning_count', 0)})"
        )
    findings = _list_of_mappings(payload.get("findings"))
    if not findings:
        lines.append("No bounded payload contract drift detected.")
        return "\n".join(lines)
    lines.append("Findings")
    for finding in findings[:8]:
        lines.append(
            "- "
            f"{finding.get('severity', 'error')} "
            f"{finding.get('code', '')} "
            f"{finding.get('path', '')}: "
            f"{finding.get('message', '')}"
        )
    if len(findings) > 8:
        lines.append(f"- {len(findings) - 8} additional findings omitted")
    return "\n".join(lines)


def _audit_required_surfaces(
    payload: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    for contract in _SEQUENCE_CONTRACTS:
        metadata = _value_at_path(payload, contract.metadata_path)
        if not isinstance(metadata, Mapping):
            _add_finding(
                findings,
                code="bounded_payload_metadata_missing",
                path=_format_path(contract.metadata_path),
                message="bounded sequence metadata is missing",
                expected="mapping with limit metadata",
                actual=metadata,
            )
        sequence = _value_at_path(payload, contract.sequence_path)
        if not isinstance(sequence, list):
            _add_finding(
                findings,
                code="bounded_payload_sequence_missing",
                path=_format_path(contract.sequence_path),
                message="bounded sequence output is missing",
                expected="list",
                actual=sequence,
            )
    return (len(_SEQUENCE_CONTRACTS) * 2, *before)


def _audit_limit_metadata_surfaces(
    payload: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    checked = 0
    for path, metadata in _iter_limit_payloads(cast(JSONValue, payload)):
        checked += 1
        _audit_limit_payload(metadata, path=path, findings=findings)
        if _COUNT_KEYS <= metadata.keys():
            _audit_count_payload(metadata, path=path, findings=findings)
    return (checked, *before)


def _audit_sequence_contracts(
    payload: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    for contract in _SEQUENCE_CONTRACTS:
        metadata = _mapping_at_path(payload, contract.metadata_path)
        sequence = _list_at_path(payload, contract.sequence_path)
        if metadata is None or sequence is None:
            continue
        expected_total = _int_at_path(payload, contract.total_path)
        expected_included = _int_at_path(payload, contract.included_path)
        expected_omitted = _int_at_path(payload, contract.omitted_path)
        expected_truncated = _bool_at_path(payload, contract.truncated_path)
        metadata_path = _format_path(contract.metadata_path)
        if _COUNT_KEYS <= metadata.keys():
            _audit_count_payload(
                metadata,
                path=metadata_path,
                findings=findings,
                expected_total=expected_total,
                expected_included=expected_included,
                expected_omitted=expected_omitted,
                expected_truncated=expected_truncated,
                expected_sequence_count=len(sequence),
            )
        else:
            _audit_bounded_sequence_summary(
                metadata,
                path=metadata_path,
                findings=findings,
                expected_total=expected_total,
                expected_included=expected_included,
                expected_omitted=expected_omitted,
                expected_truncated=expected_truncated,
                expected_sequence_count=len(sequence),
            )
    return (len(_SEQUENCE_CONTRACTS), *before)


def _audit_limit_payload(
    payload: Mapping[str, JSONValue],
    *,
    path: str,
    findings: list[dict[str, JSONValue]],
) -> None:
    for key in _REQUIRED_LIMIT_KEYS:
        if key not in payload:
            _add_finding(
                findings,
                code="bounded_payload_limit_metadata_missing_field",
                path=f"{path}.{key}",
                message="limit metadata field is missing",
                expected=key,
                actual=None,
            )
    if not _REQUIRED_LIMIT_KEYS <= payload.keys():
        return

    limit = _int_value(payload.get("limit"))
    effective = _int_value(payload.get("effective_limit"))
    requested = _optional_int_value(payload.get("requested_limit"))
    default = _int_value(payload.get("default_limit"))
    minimum = _int_value(payload.get("minimum_limit"))
    maximum = _optional_int_value(payload.get("maximum_limit"))
    unbounded = payload.get("unbounded")

    if limit is None or effective is None:
        _add_finding(
            findings,
            code="bounded_payload_limit_metadata_type_mismatch",
            path=path,
            message="limit and effective_limit must be integers",
            expected="integer limit fields",
            actual=dict(payload),
        )
        return
    if limit != effective:
        _add_finding(
            findings,
            code="bounded_payload_limit_alias_mismatch",
            path=f"{path}.limit",
            message="legacy limit field must equal effective_limit",
            expected=effective,
            actual=limit,
        )
    if default is None or minimum is None or not isinstance(unbounded, bool):
        _add_finding(
            findings,
            code="bounded_payload_limit_metadata_type_mismatch",
            path=path,
            message=(
                "default_limit, minimum_limit, and unbounded must have "
                "bounded limit metadata types"
            ),
            expected="integer defaults and boolean unbounded",
            actual=dict(payload),
        )
        return

    raw_limit = default if requested is None else requested
    if unbounded:
        expected_effective = -1
    else:
        expected_effective = max(minimum, raw_limit)
        if maximum is not None:
            expected_effective = min(expected_effective, maximum)
    if effective != expected_effective:
        _add_finding(
            findings,
            code="bounded_payload_effective_limit_mismatch",
            path=f"{path}.effective_limit",
            message="effective limit must match requested/default clamp rules",
            expected=expected_effective,
            actual=effective,
        )


def _audit_count_payload(
    payload: Mapping[str, JSONValue],
    *,
    path: str,
    findings: list[dict[str, JSONValue]],
    expected_total: int | None = None,
    expected_included: int | None = None,
    expected_omitted: int | None = None,
    expected_truncated: bool | None = None,
    expected_sequence_count: int | None = None,
) -> None:
    for key in _COUNT_KEYS:
        if key not in payload:
            _add_finding(
                findings,
                code="bounded_payload_count_metadata_missing_field",
                path=f"{path}.{key}",
                message="count metadata field is missing",
                expected=key,
                actual=None,
            )
    if not _COUNT_KEYS <= payload.keys():
        return
    total = _int_value(payload.get("total_count"))
    included = _int_value(payload.get("included_count"))
    omitted = _int_value(payload.get("omitted_count"))
    truncated = payload.get("truncated")
    if (
        total is None
        or included is None
        or omitted is None
        or not isinstance(truncated, bool)
    ):
        _add_finding(
            findings,
            code="bounded_payload_count_metadata_type_mismatch",
            path=path,
            message=(
                "total_count, included_count, omitted_count, and truncated "
                "must have bounded count metadata types"
            ),
            expected="integer counts and boolean truncated",
            actual=dict(payload),
        )
        return

    if expected_total is not None and total != expected_total:
        _add_count_mismatch(
            findings,
            path=f"{path}.total_count",
            expected=expected_total,
            actual=total,
        )
    if expected_included is not None and included != expected_included:
        _add_count_mismatch(
            findings,
            path=f"{path}.included_count",
            expected=expected_included,
            actual=included,
        )
    if (
        expected_sequence_count is not None
        and included != expected_sequence_count
    ):
        _add_count_mismatch(
            findings,
            path=f"{path}.included_count",
            expected=expected_sequence_count,
            actual=included,
            message="included_count must match emitted sequence length",
        )
    expected_omitted_count = (
        expected_omitted
        if expected_omitted is not None
        else max(0, total - included)
    )
    if omitted != expected_omitted_count:
        _add_finding(
            findings,
            code="bounded_payload_omitted_count_mismatch",
            path=f"{path}.omitted_count",
            message="omitted_count must match total_count - included_count",
            expected=expected_omitted_count,
            actual=omitted,
        )
    expected_truncated_value = (
        expected_truncated
        if expected_truncated is not None
        else total > included
    )
    if truncated is not expected_truncated_value:
        _add_finding(
            findings,
            code="bounded_payload_truncation_mismatch",
            path=f"{path}.truncated",
            message="truncated must reflect omitted bounded payload items",
            expected=expected_truncated_value,
            actual=truncated,
        )
    if included > total:
        _add_count_mismatch(
            findings,
            path=f"{path}.included_count",
            expected=f"<= {total}",
            actual=included,
        )


def _audit_bounded_sequence_summary(
    metadata: Mapping[str, JSONValue],
    *,
    path: str,
    findings: list[dict[str, JSONValue]],
    expected_total: int | None,
    expected_included: int | None,
    expected_omitted: int | None,
    expected_truncated: bool | None,
    expected_sequence_count: int,
) -> None:
    _audit_limit_payload(metadata, path=path, findings=findings)
    if expected_total is None:
        _add_count_mismatch(
            findings,
            path=f"{path}.total",
            expected="integer total count field",
            actual=None,
        )
        return
    if expected_included is None:
        _add_count_mismatch(
            findings,
            path=f"{path}.included",
            expected="integer included count field",
            actual=None,
        )
        return
    if expected_omitted is None:
        _add_count_mismatch(
            findings,
            path=f"{path}.omitted",
            expected="integer omitted count field",
            actual=None,
        )
        return
    if expected_truncated is None:
        _add_finding(
            findings,
            code="bounded_payload_truncation_mismatch",
            path=f"{path}.truncated",
            message="bounded sequence summary must expose truncation state",
            expected="boolean truncated field",
            actual=None,
        )
        return
    if expected_included != expected_sequence_count:
        _add_count_mismatch(
            findings,
            path=f"{path}.included",
            expected=expected_sequence_count,
            actual=expected_included,
            message="included count must match emitted sequence length",
        )
    omitted = max(0, expected_total - expected_included)
    if expected_omitted != omitted:
        _add_finding(
            findings,
            code="bounded_payload_omitted_count_mismatch",
            path=f"{path}.omitted",
            message="omitted count must match total count - included count",
            expected=omitted,
            actual=expected_omitted,
        )
    truncated = expected_total > expected_included
    if expected_truncated is not truncated:
        _add_finding(
            findings,
            code="bounded_payload_truncation_mismatch",
            path=f"{path}.truncated",
            message="truncated must reflect omitted bounded payload items",
            expected=truncated,
            actual=expected_truncated,
        )


def _iter_limit_payloads(
    value: JSONValue,
    *,
    path: str = "",
) -> tuple[tuple[str, Mapping[str, JSONValue]], ...]:
    results: list[tuple[str, Mapping[str, JSONValue]]] = []
    if isinstance(value, Mapping):
        if _REQUIRED_LIMIT_KEYS.intersection(value.keys()):
            results.append((path or "$", value))
        for key, item in value.items():
            child_path = key if not path else f"{path}.{key}"
            results.extend(_iter_limit_payloads(item, path=child_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            results.extend(_iter_limit_payloads(item, path=f"{path}[{index}]"))
    return tuple(results)


class _RepresentativeQualitySkipRule:
    rule_id = "time.ascii.gaps"
    description = "semantic scans prefer extracted CSVs"

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        del target
        return ()


def _representative_quality_report() -> QualityReport:
    valid_tick = _target(
        "data/EURUSD-T-valid.csv", symbol="EURUSD", timeframe="T"
    )
    limited_tick = _target(
        "data/GBPUSD-T-limited.csv", symbol="GBPUSD", timeframe="T"
    )
    tick = _target("data/USDJPY-T.csv", symbol="USDJPY", timeframe="T")
    duplicate = _target(
        "data/duplicate.csv",
        symbol="AUDUSD",
        timeframe="T",
    )
    missing = _target("data/missing.csv", symbol="CADCHF", timeframe="T")
    negative_spread = _target(
        "data/negative-spread.csv",
        symbol="EURGBP",
        timeframe="T",
    )
    directory = QualityTarget(
        path="data/ASCII/T",
        kind=QualityTargetKind.DIRECTORY,
        data_format="ascii",
        timeframe="T",
    )
    duplicate_archive = QualityTarget(
        path="data/EURUSD-T-valid.zip",
        kind=QualityTargetKind.ZIP,
        data_format=valid_tick.data_format,
        timeframe=valid_tick.timeframe,
        symbol=valid_tick.symbol,
        period=valid_tick.period,
    )

    fingerprint_findings = (
        _fingerprint_finding(valid_tick, _valid_tick_fingerprint_payload()),
        _fingerprint_finding(limited_tick, _limited_tick_fingerprint_payload()),
        _fingerprint_finding(tick, _tick_fingerprint_payload(symbol="USDJPY")),
    )
    duplicate_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="ASCII_TICK_DUPLICATE_ROW",
        message="Tick file contains exact duplicate timestamp, bid, ask, and volume rows.",
        rule_id="time.ascii.sequence",
        target=duplicate,
        location=QualityLocation(path=duplicate.path),
    )
    missing_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="FILE_MISSING",
        message="expected local file is missing",
        rule_id="file.exists",
        target=missing,
        location=QualityLocation(path=missing.path),
    )
    negative_spread_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="NEGATIVE_SPREAD",
        message="tick ask is below bid",
        rule_id="ticks.spread",
        target=negative_spread,
        location=QualityLocation(path=negative_spread.path),
    )
    cross_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR",
        message="triangular relationship differs from the direct pair",
        rule_id="domain.cross_instrument_consistency",
        target=directory,
        location=QualityLocation(
            path=directory.path,
            metadata={"period": "201202", "timeframe": "T"},
        ),
        metadata={
            "samples": [
                {
                    "denominator_symbol": "CADCHF",
                    "direct_symbol": "AUDCAD",
                    "numerator_symbol": "AUDCHF",
                    "period": "201202",
                    "timeframe": "T",
                }
            ]
        },
    )
    targets = (
        duplicate_archive,
        valid_tick,
        limited_tick,
        tick,
        duplicate,
        missing,
        negative_spread,
        directory,
    )
    skip_report = run_quality_assessment(
        targets=targets,
        rules=(_RepresentativeQualitySkipRule(),),
    )
    quality_engine = skip_report.metadata[QUALITY_ENGINE_METADATA_KEY]
    return QualityReport(
        targets=targets,
        rule_results=(
            *skip_report.rule_results,
            *(
                QualityRuleResult(
                    rule_id=SERIES_FINGERPRINT_RULE_ID,
                    target=finding.target,
                    findings=(finding,),
                )
                for finding in fingerprint_findings
            ),
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=duplicate,
                findings=(duplicate_finding,),
            ),
            QualityRuleResult(
                rule_id="file.exists",
                target=missing,
                findings=(missing_finding,),
            ),
            QualityRuleResult(
                rule_id="ticks.spread",
                target=negative_spread,
                findings=(negative_spread_finding,),
            ),
            QualityRuleResult(
                rule_id="domain.cross_instrument_consistency",
                target=directory,
                findings=(cross_finding,),
            ),
        ),
        metadata={
            QUALITY_ENGINE_METADATA_KEY: quality_engine,
            CROSS_SERIES_FINGERPRINT_METADATA_KEY: (
                _representative_cross_series_fingerprint()
            ),
            QUALITY_REPORTING_METADATA_KEY: {
                QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY: {
                    "enabled": True,
                }
            },
        },
    )


def _representative_cross_series_fingerprint() -> dict[str, JSONValue]:
    group_limit = bounded_report_limit(32, default_limit=32)
    correlation_limit = bounded_report_limit(32, default_limit=32)
    return {
        "schema_version": CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION,
        "rule_id": CROSS_SERIES_FINGERPRINT_RULE_ID,
        "status": "valid",
        "fx_series_count": 3,
        "group_count": 1,
        "incomplete_group_count": 0,
        "included_group_count": 1,
        "omitted_group_count": 0,
        "truncated": False,
        "limit_metadata": {
            "groups": group_limit.limit_payload(),
            "correlations_per_group": correlation_limit.limit_payload(),
        },
        "triangular_consistency": {
            "candidate_count": 1,
            "warning_count": 0,
            "error_count": 0,
        },
        "inverse_consistency": {
            "candidate_count": 0,
            "warning_count": 0,
            "error_count": 0,
        },
        "stale_join_risk": {"risk_count": 0, "samples": []},
        "panel_coverage": [
            {
                "timeframe": "T",
                "symbols": ["EURGBP", "EURUSD", "GBPUSD"],
                "union_period_count": 1,
                "common_period_count": 1,
                "common_first_period": "201202",
                "common_last_period": "201202",
                "unequal_period_ranges": False,
                "limiting_start_symbols": [
                    "EURGBP",
                    "EURUSD",
                    "GBPUSD",
                ],
                "limiting_end_symbols": [
                    "EURGBP",
                    "EURUSD",
                    "GBPUSD",
                ],
                "first_period_by_symbol": {
                    "EURGBP": "201202",
                    "EURUSD": "201202",
                    "GBPUSD": "201202",
                },
                "last_period_by_symbol": {
                    "EURGBP": "201202",
                    "EURUSD": "201202",
                    "GBPUSD": "201202",
                },
                "missing_period_count_by_symbol": {
                    "EURGBP": 0,
                    "EURUSD": 0,
                    "GBPUSD": 0,
                },
            }
        ],
        "groups": [
            {
                "group_id": "ascii:T:201202",
                "symbols": ["EURGBP", "EURUSD", "GBPUSD"],
                "expected_symbols": ["EURGBP", "EURUSD", "GBPUSD"],
                "missing_symbols": [],
                "complete": True,
                "timestamp_grid": {
                    "common_timestamp_count": 3,
                    "union_timestamp_count": 3,
                    "common_timestamp_ratio": 1.0,
                    "missing_by_symbol": {
                        "EURGBP": 0,
                        "EURUSD": 0,
                        "GBPUSD": 0,
                    },
                },
                "coverage_ranges": {"unequal_ranges": False},
                "return_correlation": {
                    "pair_count": 3,
                    "included_pair_count": 3,
                    "omitted_pair_count": 0,
                    "truncated": False,
                    "limit_metadata": {
                        "pairs": correlation_limit.limit_payload()
                    },
                    "pairs": [],
                },
            }
        ],
    }


def _target(path: str, *, symbol: str, timeframe: str) -> QualityTarget:
    return QualityTarget(
        path=path,
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=timeframe,
        symbol=symbol,
        period="201202",
    )


def _fingerprint_finding(
    target: QualityTarget,
    payload: dict[str, JSONValue],
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=target,
        metadata={"time_series_fingerprint": payload},
    )


def _valid_tick_fingerprint_payload(
    *, symbol: str = "EURUSD"
) -> dict[str, JSONValue]:
    return _tick_fingerprint_payload(symbol=symbol)


def _limited_tick_fingerprint_payload() -> dict[str, JSONValue]:
    payload = _valid_tick_fingerprint_payload(symbol="GBPUSD")
    payload["temporal_topology"] = _topology_payload(
        invalid_timestamp_count=1,
        duplicate_timestamp_count=1,
        non_monotonic_count=1,
        suspicious_gap_count=1,
    )
    microstructure_dynamics = cast(
        dict[str, JSONValue], payload["microstructure_dynamics"]
    )
    microstructure_dynamics["sequence_status"] = "limited"
    microstructure_dynamics["limitations"] = [
        "invalid_timestamps_skipped",
        "duplicate_rows",
    ]
    microstructure_dynamics["invalid_row_count"] = 1
    microstructure_dynamics["truncated"] = True
    payload["dependence"] = _dependence_payload(
        status="limited",
        reason="invalid_timestamps_skipped",
        skipped_lags={"3": "insufficient_sample_count"},
    )
    payload["fingerprint_audit"] = _audit_payload(
        sections=(
            "coverage",
            "temporal_topology",
            "calendar_regimes",
            "tick_distribution",
            "conditional_distributions",
            "microstructure_dynamics",
            "dependence",
        ),
        section_statuses={
            "coverage": "valid",
            "temporal_topology": "limited",
            "calendar_regimes": "valid",
            "tick_distribution": "valid",
            "conditional_distributions": "valid",
            "microstructure_dynamics": "limited",
            "dependence": "limited",
        },
        micro_status="limited",
        micro_reason="invalid_timestamps_skipped",
        tick_spread_eligible=True,
        tick_spread_emitted=True,
    )
    payload["synthetic_constraints"] = synthetic_constraints_from_fingerprint(
        payload
    )
    return payload


def _tick_fingerprint_payload(
    *, symbol: str = "EURUSD"
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "target_axis": _axis(symbol=symbol, timeframe="T", kind="csv"),
        "coverage": {"row_count": 5, "parsed_row_count": 5},
        "temporal_topology": _topology_payload(computed_from="text_scan"),
        "tick_distribution": {
            "row_count": 5,
            "sampled_row_count": 5,
            "usable_row_count": 5,
            "invalid_row_count": 0,
            "partial_row_count": 0,
            "truncated": False,
        },
        "calendar_regimes": _calendar_regimes_payload(),
        "conditional_distributions": {
            "basis": "text",
            "metric": "tick_spread",
            "row_count": 5,
            "sampled_row_count": 5,
            "usable_row_count": 5,
            "invalid_row_count": 0,
            "truncated": False,
            "by_active_session": {
                "london": {"spread": _numeric(count=3, median=0.0002)}
            },
            "by_special_tag": {
                "london_4pm_fix_window": {
                    "spread": _numeric(count=1, median=0.0003)
                }
            },
        },
        "cache_source_parity": _cache_source_parity_payload(symbol),
        "microstructure_dynamics": _microstructure_dynamics_payload(),
        "dependence": _dependence_payload(status="ok"),
        "fingerprint_audit": _audit_payload(
            sections=(
                "coverage",
                "temporal_topology",
                "calendar_regimes",
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
                "dependence",
            ),
            section_statuses={
                "coverage": "valid",
                "temporal_topology": "valid",
                "calendar_regimes": "valid",
                "tick_distribution": "valid",
                "conditional_distributions": "valid",
                "microstructure_dynamics": "valid",
                "dependence": "valid",
            },
            micro_status="valid",
            tick_spread_eligible=True,
            tick_spread_emitted=True,
        ),
        "source": {"kind": "csv_text"},
    }
    payload["synthetic_constraints"] = synthetic_constraints_from_fingerprint(
        payload
    )
    return payload


def _cache_source_parity_payload(symbol: str) -> dict[str, JSONValue]:
    return {
        "schema_version": TIME_SERIES_FINGERPRINT_PARITY_SCHEMA_VERSION,
        "status": "match",
        "advisory": True,
        "target_axis": _axis(symbol=symbol, timeframe="T", kind="csv"),
        "base_grain": {"data_format": "ascii", "timeframe": "T"},
        "compared_section_count": 9,
        "matching_section_count": 9,
        "mismatched_section_count": 0,
        "skipped_section_count": 0,
        "mismatch_code_count": 0,
        "included_mismatch_code_count": 0,
        "omitted_mismatch_code_count": 0,
        "truncated": False,
        "limit_metadata": {
            "mismatches": {
                "requested_limit": 16,
                "default_limit": 16,
                "effective_limit": 16,
                "unbounded": False,
            }
        },
        "mismatch_codes": [],
        "skipped_reasons": [],
        "bases": {
            "raw_source": {
                "status": "available",
                "kind": "csv_text",
                "path": f"DAT_ASCII_{symbol}_T_201202.csv",
                "member": None,
                "row_count": 3,
            },
            "raw_cache": {
                "status": "available",
                "path": ".data",
                "cache_source": "sibling",
                "fresh": True,
                "freshness": "fresh",
                "row_count": 3,
            },
            "enriched_cache": {
                "status": "available",
                "training_schema_version": (
                    "histdatacom.ascii-tick-training-features.v1"
                ),
                "cache_was_enriched": True,
                "legacy_cache_enriched_on_read": False,
            },
            "quality_report": {
                "status": "available",
                "projection_kind": "audit_from_enriched_rows",
            },
            "influx_projection": {
                "status": "available",
                "projection_kind": "same_point_enriched_fields",
                "missing_required_field_count": 0,
            },
        },
        "comparisons": [
            {"section": section, "status": "match"}
            for section in (
                "coverage",
                "temporal_topology",
                "calendar_regimes",
                "conditional_distributions",
                "training_columns",
                "row_identity",
                "duplicate_timestamps",
                "quality_report_projection",
                "influx_projection",
            )
        ],
    }


def _axis(*, symbol: str, timeframe: str, kind: str) -> dict[str, JSONValue]:
    return {
        "data_format": "ascii",
        "timeframe": timeframe,
        "symbol": symbol,
        "period": "201202",
        "kind": kind,
    }


def _topology_payload(
    *,
    computed_from: str = "text_scan",
    invalid_timestamp_count: int = 0,
    duplicate_timestamp_count: int = 0,
    non_monotonic_count: int = 0,
    suspicious_gap_count: int = 0,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "row_count": 4,
        "parsed_row_count": 4 - invalid_timestamp_count,
        "invalid_timestamp_count": invalid_timestamp_count,
        "duplicate_timestamp_count": duplicate_timestamp_count,
        "non_monotonic_count": non_monotonic_count,
        "median_interval_ms": 60_000,
        "max_gap_ms": 60_000,
        "suspicious_gap_count": suspicious_gap_count,
        "expected_session_closure_count": 0,
        "weekend_activity_count": 0,
        "sampling_basis": "observed_sequence",
        "computed_from": computed_from,
        "cache_source": None,
    }
    if duplicate_timestamp_count:
        payload["inspection_context"] = {
            "schema_version": "histdatacom.timestamp-topology-inspection.v1",
            "duplicate_timestamps": {
                "total_count": 1,
                "included_count": 1,
                "omitted_count": 0,
                "truncated": False,
                "limit_metadata": {
                    "samples": bounded_report_limit(
                        1,
                        default_limit=5,
                        minimum_limit=0,
                        maximum_limit=5,
                        allow_unbounded=False,
                    ).count_payload(1)
                },
                "duplicate_row_count": duplicate_timestamp_count,
                "samples": [
                    {
                        "row_number": 2,
                        "timestamp_source": "20120201 000000000",
                        "timestamp_source_truncated": False,
                        "timestamp_utc_ms": 1328072400000,
                        "utc_timestamp": "2012-02-01T05:00:00Z",
                        "occurrence_count": duplicate_timestamp_count + 1,
                        "exact_row_group_count": 1,
                    }
                ],
            },
        }
    return payload


def _calendar_regimes_payload() -> dict[str, JSONValue]:
    return {
        "status": "ok",
        "computed_from": "text_scan",
        "cache_source": None,
        "row_count": 4,
        "parsed_row_count": 4,
        "invalid_timestamp_count": 0,
        "session_state_counts": {"market_open": 4},
        "active_session_counts": {"london": 2, "new_york": 2},
        "special_tag_counts": {"daily_rollover": 1},
        "holiday_tag_counts": {},
        "event_tag_counts": {},
        "hour_of_day_counts": {"11": 2, "12": 2},
        "day_of_week_counts": {"wednesday": 4},
        "calendar_profile_complete": False,
        "missing_optional_calendar_data": True,
        "calendar_policy": {
            "holiday_calendar_source": "static_month_day_major_holidays",
            "holiday_calendar_complete": False,
            "holiday_calendar_static_advisory": True,
            "calendar_profile": {
                "name": "static-major-holidays",
                "source": "static_month_day_major_holidays",
                "version": "1",
                "complete": False,
                "static_advisory": True,
            },
        },
    }


def _microstructure_dynamics_payload() -> dict[str, JSONValue]:
    return {
        "basis": "observed_sequence",
        "row_order": "source_text_order",
        "computed_from": "text_scan",
        "cache_source": None,
        "regular_grid": False,
        "sequence_status": "ok",
        "limitations": [],
        "row_count": 5,
        "sampled_row_count": 5,
        "usable_row_count": 5,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
        "interarrival_ms": _numeric(count=4, median=250.0),
        "spread": _numeric(count=5, median=0.0001),
        "spread_change": _numeric(count=4, median=0.0),
        "absolute_spread_change": _numeric(count=4, median=0.0001),
        "zero_spread_count": 0,
        "negative_spread_count": 0,
        "zero_spread_rate": 0.0,
        "negative_spread_rate": 0.0,
        "spread_jump": {"threshold": 0.0002, "count": 1, "rate": 0.25},
        "stale_quote": {
            "repeat_count": 1,
            "repeat_rate": 0.25,
            "run_count": 1,
            "affected_row_count": 2,
        },
        "burst": {"interval_count": 2, "burst_rate": 0.5, "run_count": 1},
        "one_sided_movement": {
            "count": 2,
            "rate": 0.5,
            "bid_only_count": 1,
            "ask_only_count": 1,
            "run_count": 0,
        },
    }


def _dependence_payload(
    *,
    status: str,
    reason: str | None = None,
    skipped_lags: Mapping[str, str] | None = None,
) -> dict[str, JSONValue]:
    skipped_lags = skipped_lags or {}
    computed_lag_count = 2
    payload: dict[str, JSONValue] = {
        "basis": "observed_sequence",
        "acf_basis": "observed_sequence",
        "row_order": "source_text_order",
        "computed_from": "text_scan",
        "cache_source": None,
        "regular_grid": False,
        "dependence_status": status,
        "limitations": [reason] if reason else [],
        "row_count": 4,
        "sampled_row_count": 4,
        "usable_row_count": 4,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
        "lags": [1, 3],
        "computed_lag_count": computed_lag_count,
        "skipped_lag_count": len(skipped_lags),
        "close_log_return_acf": _acf_payload(
            sample_count=3,
            skipped_lags=skipped_lags,
        ),
    }
    if reason:
        payload["reason"] = reason
    return payload


def _acf_payload(
    *,
    sample_count: int,
    skipped_lags: Mapping[str, str],
) -> dict[str, JSONValue]:
    return {
        "sample_count": sample_count,
        "lag_acf": {"1": 0.1, "2": 0.05},
        "computed_lag_count": 2,
        "skipped_lags": {
            lag: {"reason": reason, "sample_count": sample_count}
            for lag, reason in skipped_lags.items()
        },
        "skipped_lag_count": len(skipped_lags),
    }


def _audit_payload(
    *,
    sections: tuple[str, ...],
    section_statuses: Mapping[str, str],
    micro_status: str,
    micro_reason: str | None = None,
    tick_spread_eligible: bool = False,
    tick_spread_emitted: bool = False,
) -> dict[str, JSONValue]:
    tick_spread: dict[str, JSONValue] = {
        "eligible": tick_spread_eligible,
        "status": "eligible" if tick_spread_eligible else "ineligible",
        "emitted": tick_spread_emitted,
    }
    if not tick_spread_eligible:
        tick_spread["reason"] = "unsupported_timeframe"
    return {
        "sections_expected": list(sections),
        "sections_emitted": list(sections),
        "sections_skipped": {},
        "section_statuses": dict(section_statuses),
        "conditional_distribution_eligibility": {"tick_spread": tick_spread},
        "profile_completeness": {
            "source": "quality_profile",
            "calendar_profile_complete": False,
            "missing_optional_calendar_data": True,
            "calendar_profile_name": "static-major-holidays",
            "calendar_profile_source": "static_month_day_major_holidays",
            "calendar_profile_version": "1",
            "calendar_profile_static_advisory": True,
        },
        "dynamics_readiness": {
            "microstructure_dynamics": _readiness_payload(
                status=micro_status,
                reason=micro_reason,
                row_count=5 if micro_status != "skipped" else 0,
            ),
        },
    }


def _readiness_payload(
    *,
    status: str,
    row_count: int,
    reason: str | None = None,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "status": status,
        "basis": "observed_sequence" if row_count else "unknown",
        "row_order": "source_text_order" if row_count else "unknown",
        "computed_from": "text_scan" if row_count else "unknown",
        "cache_source": None,
        "regular_grid": False,
        "limitations": [reason] if reason else [],
        "row_count": row_count,
        "sampled_row_count": row_count,
        "usable_row_count": row_count,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
    }
    if reason:
        payload["reason"] = reason
    return payload


def _numeric(*, count: int, median: float) -> dict[str, JSONValue]:
    return {
        "count": count,
        "min": 0.0,
        "max": median,
        "mean": median,
        "median": median,
        "mad": 0.0,
        "quantiles": {"0.95": median, "0.99": median},
    }


def _record_check(
    checks: list[dict[str, JSONValue]],
    findings: list[dict[str, JSONValue]],
    name: str,
    result: tuple[int, int, int],
) -> None:
    checked_count, before_errors, before_warnings = result
    after_errors, after_warnings = _finding_counts(findings)
    error_count = after_errors - before_errors
    warning_count = after_warnings - before_warnings
    checks.append(
        {
            "name": name,
            "status": "fail" if error_count else "pass",
            "checked_count": checked_count,
            "error_count": error_count,
            "warning_count": warning_count,
        }
    )


def _finding_counts(
    findings: Sequence[Mapping[str, JSONValue]],
) -> tuple[int, int]:
    return (
        sum(1 for finding in findings if finding.get("severity") == "error"),
        sum(1 for finding in findings if finding.get("severity") == "warning"),
    )


def _add_count_mismatch(
    findings: list[dict[str, JSONValue]],
    *,
    path: str,
    expected: JSONValue,
    actual: JSONValue,
    message: str = "bounded count metadata does not match emitted payload",
) -> None:
    _add_finding(
        findings,
        code="bounded_payload_count_mismatch",
        path=path,
        message=message,
        expected=expected,
        actual=actual,
    )


def _add_finding(
    findings: list[dict[str, JSONValue]],
    *,
    code: str,
    path: str,
    message: str,
    expected: JSONValue,
    actual: JSONValue,
) -> None:
    findings.append(
        {
            "severity": "error",
            "code": code,
            "path": path,
            "message": message,
            "expected": expected,
            "actual": actual,
        }
    )


def _mapping_at_path(
    payload: Mapping[str, JSONValue],
    path: tuple[str, ...],
) -> Mapping[str, JSONValue] | None:
    value = _value_at_path(payload, path)
    return value if isinstance(value, Mapping) else None


def _list_at_path(
    payload: Mapping[str, JSONValue],
    path: tuple[str, ...],
) -> list[JSONValue] | None:
    value = _value_at_path(payload, path)
    return value if isinstance(value, list) else None


def _int_at_path(
    payload: Mapping[str, JSONValue],
    path: tuple[str, ...],
) -> int | None:
    return _int_value(_value_at_path(payload, path))


def _bool_at_path(
    payload: Mapping[str, JSONValue],
    path: tuple[str, ...],
) -> bool | None:
    value = _value_at_path(payload, path)
    return value if isinstance(value, bool) else None


def _value_at_path(
    payload: Mapping[str, JSONValue],
    path: tuple[str, ...],
) -> JSONValue:
    current: JSONValue = cast(JSONValue, payload)
    for part in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _format_path(path: tuple[str, ...]) -> str:
    return ".".join(path)


def _int_value(value: JSONValue) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    return None


def _optional_int_value(value: JSONValue) -> int | None:
    if value is None:
        return None
    return _int_value(value)


def _list_of_mappings(value: JSONValue) -> tuple[Mapping[str, JSONValue], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))

"""Deterministic non-mutating repair plans for saved quality reports."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from histdatacom.data_quality.contracts import QualityFinding, QualityReport
from histdatacom.data_quality.limits import (
    BoundedReportLimit,
    bounded_report_limit,
)
from histdatacom.data_quality.remediation import (
    QualityRemediationHint,
    remediation_hints_for_finding_code,
)
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import JSONValue

QUALITY_REPAIR_PLAN_SCHEMA_VERSION = "histdatacom.quality-repair-plan.v1"
DEFAULT_QUALITY_REPAIR_PLAN_ITEM_LIMIT = 16
MAXIMUM_QUALITY_REPAIR_PLAN_ITEM_LIMIT = 64
DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT = 8
MAXIMUM_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT = 32
QUALITY_REPAIR_PLAN_DISPLAY_LIMIT = 8

_ZIP_INVENTORY_RULE_ID = "inventory.zip.integrity"


@dataclass(frozen=True, slots=True)
class _OperationSpec:
    category: str
    next_step: str
    preconditions: tuple[str, ...]
    evidence_needed: tuple[str, ...]
    required_evidence_groups: tuple[tuple[str, ...], ...]
    priority: int


_OPERATION_SPECS: Mapping[str, _OperationSpec] = {
    "ZIP_UNREADABLE": _OperationSpec(
        category="restore_read_access",
        next_step=(
            "Restore read access to the archive or replace it with a readable copy."
        ),
        preconditions=(
            "Confirm the archive is expected at the reported target axis.",
            "Inspect ownership, permissions, and filesystem availability manually.",
        ),
        evidence_needed=("read failure type", "expected archive identity"),
        required_evidence_groups=(),
        priority=10,
    ),
    "ZIP_CORRUPT": _OperationSpec(
        category="redownload_archive",
        next_step=(
            "Replace the corrupt archive with a trusted copy from the original source."
        ),
        preconditions=(
            "Confirm the archive target identity and period.",
            "Retain the original until the replacement passes ZIP integrity checks.",
        ),
        evidence_needed=("archive target identity", "replacement provenance"),
        required_evidence_groups=(),
        priority=20,
    ),
    "ZIP_CRC_ERROR": _OperationSpec(
        category="redownload_archive",
        next_step=(
            "Replace the CRC-failed archive with a trusted copy and rerun inventory checks."
        ),
        preconditions=(
            "Record the failing member before replacing the archive.",
            "Verify the replacement against the expected target axis.",
        ),
        evidence_needed=("failing ZIP member", "replacement provenance"),
        required_evidence_groups=(("bad_member",),),
        priority=30,
    ),
    "HISTDATA_ZIP_FILENAME_INVALID": _OperationSpec(
        category="rename_archive",
        next_step=(
            "Rename the archive to an accepted HistData filename after verifying its contents."
        ),
        preconditions=(
            "Confirm the observed archive contains the reported symbol and period.",
            "Confirm the destination filename does not already exist.",
        ),
        evidence_needed=(
            "observed archive filename",
            "accepted destination filename",
        ),
        required_evidence_groups=(
            ("observed_filename",),
            ("expected_filename", "accepted_filenames"),
        ),
        priority=40,
    ),
    "ZIP_MEMBER_MISSING": _OperationSpec(
        category="restore_archive_member",
        next_step=(
            "Restore the expected data member from a trusted source, then rebuild and verify the archive."
        ),
        preconditions=(
            "Confirm the expected member identity from the archive target axis.",
            "Do not synthesize or copy data from a different period.",
        ),
        evidence_needed=("expected member name", "trusted replacement source"),
        required_evidence_groups=(("expected_member",),),
        priority=50,
    ),
    "ZIP_MEMBER_UNEXPECTED": _OperationSpec(
        category="rebuild_archive_members",
        next_step=(
            "Inspect the observed members and rebuild the archive with the expected HistData data member."
        ),
        preconditions=(
            "Compare every observed member axis with the expected archive axis.",
            "Preserve the original archive until rebuilt contents pass inventory checks.",
        ),
        evidence_needed=("expected member name", "observed member names"),
        required_evidence_groups=(
            ("expected_member",),
            ("observed_members",),
        ),
        priority=60,
    ),
    "HISTDATA_ZIP_MEMBER_FILENAME_INVALID": _OperationSpec(
        category="rename_archive_member",
        next_step=(
            "Rename the data member to the expected HistData member name when its target identity is verified."
        ),
        preconditions=(
            "Confirm the member contents match the archive target axis.",
            "Rebuild the archive rather than editing an open ZIP in place.",
        ),
        evidence_needed=("observed member name", "expected member name"),
        required_evidence_groups=(
            ("observed_member",),
            ("expected_member",),
        ),
        priority=70,
    ),
    "ZIP_EXTRA_MEMBER": _OperationSpec(
        category="inspect_archive_members",
        next_step=(
            "Inspect unexpected members and rebuild the archive only after confirming they are not required data."
        ),
        preconditions=(
            "Classify each extra member before removal.",
            "Preserve the original archive until rebuilt contents pass inventory checks.",
        ),
        evidence_needed=("extra member names", "expected member name"),
        required_evidence_groups=(("extra_members",),),
        priority=80,
    ),
}

_EVIDENCE_KEYS = (
    "observed_filename",
    "expected_filename",
    "accepted_filenames",
    "expected_pattern",
    "observed_member",
    "expected_member",
    "observed_members",
    "extra_members",
    "bad_member",
    "error_type",
)


@dataclass(frozen=True, slots=True)
class _PlanCandidate:
    finding: QualityFinding
    hint: QualityRemediationHint | None
    operation: _OperationSpec | None

    @property
    def sort_key(
        self,
    ) -> tuple[str, str, str, str, str, str, int, int, str, str, str]:
        target = self.finding.target
        priority = self.operation.priority if self.operation else 900
        return (
            target.symbol,
            target.period,
            target.data_format,
            target.timeframe,
            target.kind.value,
            publish_safe_path(target.path),
            priority,
            -self.finding.severity.rank,
            self.finding.rule_id,
            self.finding.code,
            _stable_finding_key(self.finding),
        )


def quality_repair_plan(
    report: QualityReport,
    *,
    report_path: str = "",
    item_limit: int | None = DEFAULT_QUALITY_REPAIR_PLAN_ITEM_LIMIT,
    evidence_limit: int | None = DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
) -> dict[str, JSONValue]:
    """Return a bounded advisory repair plan without changing user data."""
    item_limit_state = bounded_report_limit(
        item_limit,
        default_limit=DEFAULT_QUALITY_REPAIR_PLAN_ITEM_LIMIT,
        maximum_limit=MAXIMUM_QUALITY_REPAIR_PLAN_ITEM_LIMIT,
        allow_unbounded=False,
    )
    evidence_limit_state = bounded_report_limit(
        evidence_limit,
        default_limit=DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
        maximum_limit=MAXIMUM_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
        allow_unbounded=False,
    )
    candidates = sorted(
        (_candidate_for_finding(finding) for finding in report.findings),
        key=lambda candidate: candidate.sort_key,
    )
    all_items = [
        _repair_plan_item(
            candidate,
            rank=rank,
            evidence_limit=evidence_limit_state,
        )
        for rank, candidate in enumerate(candidates, start=1)
    ]
    included_items = item_limit_state.slice(all_items)
    proposal_status_counts = Counter(
        _string_value(_mapping(item.get("operation")).get("proposal_status"))
        for item in all_items
    )
    operation_category_counts = Counter(
        _string_value(_mapping(item.get("operation")).get("category"))
        for item in all_items
    )
    count_payload = item_limit_state.count_payload(len(all_items))
    summary = report.summary()
    payload: dict[str, JSONValue] = {
        "schema_version": QUALITY_REPAIR_PLAN_SCHEMA_VERSION,
        "mode": "non_mutating",
        "apply_supported": False,
        "mutating_operations_performed": False,
        "input_report": {
            "path": publish_safe_path(report_path),
            "status": summary.status.value,
            "target_count": summary.target_count,
            "finding_count": summary.finding_count,
        },
        "plan_item_count": len(all_items),
        "included_plan_item_count": _int_value(
            count_payload.get("included_count")
        ),
        "omitted_plan_item_count": _int_value(
            count_payload.get("omitted_count")
        ),
        "truncated": bool(count_payload["truncated"]),
        "proposal_status_counts": _sorted_count_payload(proposal_status_counts),
        "operation_category_counts": _sorted_count_payload(
            operation_category_counts
        ),
        "items": cast(JSONValue, included_items),
        "payload_limits": {
            "items": count_payload,
            "evidence_per_item": evidence_limit_state.limit_payload(),
        },
        "safety": {
            "advisory_only": True,
            "automatic_execution": "unsupported",
            "network_access_performed": False,
            "filesystem_mutation_performed": False,
            "requires_user_verification_before_action": True,
        },
    }
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def quality_repair_plan_to_json(payload: Mapping[str, JSONValue]) -> str:
    """Return deterministic formatted JSON for a repair plan."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def format_quality_repair_plan(payload: Mapping[str, JSONValue]) -> str:
    """Return concise human-readable repair-plan output."""
    source = _mapping(payload.get("input_report"))
    lines = [
        "Quality repair plan",
        f"mode: {payload.get('mode', 'non_mutating')}",
        f"report: {source.get('path', '') or 'in-memory'}",
        (
            "items: "
            f"{payload.get('plan_item_count', 0)} "
            f"included: {payload.get('included_plan_item_count', 0)} "
            f"omitted: {payload.get('omitted_plan_item_count', 0)}"
        ),
        "safety: advisory only; no files, archives, permissions, or network state changed",
    ]
    items = _mapping_sequence(payload.get("items"))
    if not items:
        plan_item_count = _int_value(payload.get("plan_item_count"))
        if plan_item_count:
            lines.append(f"- all {plan_item_count} plan items omitted by limit")
        else:
            lines.append("- no findings to plan")
        return "\n".join(lines)
    for item in items[:QUALITY_REPAIR_PLAN_DISPLAY_LIMIT]:
        operation = _mapping(item.get("operation"))
        target = _mapping(item.get("target"))
        confidence = _mapping(item.get("confidence"))
        lines.append(
            "- "
            f"#{item.get('rank', 0)} "
            f"{item.get('severity', 'info')} "
            f"{item.get('finding_code', 'unknown')} "
            f"[{operation.get('proposal_status', 'unsupported')}/"
            f"{operation.get('category', 'unsupported')}; "
            f"confidence={confidence.get('level', 'low')}] "
            f"{target.get('path', '') or 'unknown target'}: "
            f"{operation.get('next_step', '')}"
        )
    hidden = max(
        0,
        _int_value(payload.get("plan_item_count"))
        - min(len(items), QUALITY_REPAIR_PLAN_DISPLAY_LIMIT),
    )
    if hidden:
        lines.append(f"- additional plan items: {hidden}")
    return "\n".join(lines)


def _candidate_for_finding(finding: QualityFinding) -> _PlanCandidate:
    hints = remediation_hints_for_finding_code(
        finding.code,
        rule_id=finding.rule_id,
    )
    hint = hints[0] if hints else None
    operation = (
        _OPERATION_SPECS.get(finding.code)
        if finding.rule_id == _ZIP_INVENTORY_RULE_ID
        else None
    )
    return _PlanCandidate(finding=finding, hint=hint, operation=operation)


def _repair_plan_item(
    candidate: _PlanCandidate,
    *,
    rank: int,
    evidence_limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    finding = candidate.finding
    evidence = _evidence_payload(finding.metadata, evidence_limit)
    missing_evidence = _missing_evidence(
        finding.metadata,
        (
            candidate.operation.required_evidence_groups
            if candidate.operation
            else ()
        ),
    )
    operation = _operation_payload(candidate, missing_evidence)
    target = finding.target
    hint = candidate.hint
    return {
        "rank": rank,
        "finding_code": finding.code,
        "rule_id": finding.rule_id,
        "severity": finding.severity.value,
        "remediation_hint_code": hint.code if hint else "",
        "action_kind": hint.action_kind if hint else "",
        "target": {
            "path": publish_safe_path(target.path or finding.location.path),
            "target_axis": {
                "data_format": target.data_format,
                "timeframe": target.timeframe,
                "symbol": target.symbol,
                "period": target.period,
                "kind": target.kind.value,
            },
        },
        "operation": operation,
        "preconditions": (
            list(candidate.operation.preconditions)
            if candidate.operation
            else [
                "Obtain a supported remediation mapping and diagnostic context."
            ]
        ),
        "evidence_needed": (
            list(candidate.operation.evidence_needed)
            if candidate.operation
            else ["supported operation contract"]
        ),
        "missing_evidence": list(missing_evidence),
        "evidence": evidence,
        "confidence": _confidence_payload(candidate, missing_evidence),
    }


def _operation_payload(
    candidate: _PlanCandidate,
    missing_evidence: Sequence[str],
) -> dict[str, JSONValue]:
    operation = candidate.operation
    hint = candidate.hint
    if operation is None:
        reason = "unmapped_finding" if hint is None else "unsupported_action"
        return {
            "category": "unsupported",
            "proposal_status": "unsupported",
            "specificity": "advisory",
            "automation_status": "unsupported",
            "reason": reason,
            "next_step": (
                "No concrete repair operation is supported for this finding; inspect it manually."
            ),
        }
    contextual = bool(missing_evidence)
    return {
        "category": operation.category,
        "proposal_status": "needs_context" if contextual else "proposed",
        "specificity": "contextual" if contextual else "exact",
        "automation_status": "manual_only",
        "reason": (
            "required_evidence_missing"
            if contextual
            else "exact_rule_finding_operation_mapping"
        ),
        "next_step": _contextualized_next_step(
            candidate.finding,
            operation,
            contextual=contextual,
        ),
    }


def _contextualized_next_step(
    finding: QualityFinding,
    operation: _OperationSpec,
    *,
    contextual: bool,
) -> str:
    metadata = finding.metadata
    if finding.code == "HISTDATA_ZIP_FILENAME_INVALID":
        observed = _string_value(metadata.get("observed_filename"))
        expected = _string_value(metadata.get("expected_filename"))
        if not expected:
            expected = _first_string(metadata.get("accepted_filenames"))
        if observed and expected:
            return f"Rename archive {observed!r} to {expected!r} after verifying its contents."
    if finding.code == "HISTDATA_ZIP_MEMBER_FILENAME_INVALID":
        observed = _string_value(metadata.get("observed_member"))
        expected = _string_value(metadata.get("expected_member"))
        if observed and expected:
            return f"Rebuild the archive with member {observed!r} renamed to {expected!r}."
    if contextual:
        return f"{operation.next_step} Required evidence is still missing."
    return operation.next_step


def _confidence_payload(
    candidate: _PlanCandidate,
    missing_evidence: Sequence[str],
) -> dict[str, JSONValue]:
    if candidate.operation is None:
        return {
            "level": "low",
            "basis": [
                "no supported exact operation mapping",
                (
                    "remediation hint exists"
                    if candidate.hint
                    else "remediation hint is unmapped"
                ),
            ],
        }
    if missing_evidence:
        return {
            "level": "medium",
            "basis": [
                "exact rule and finding operation mapping",
                "required diagnostic evidence is incomplete",
            ],
        }
    return {
        "level": "high",
        "basis": [
            "exact rule and finding operation mapping",
            "required diagnostic evidence is present",
            "operation remains manual and non-mutating in the application",
        ],
    }


def _evidence_payload(
    metadata: Mapping[str, JSONValue],
    limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    evidence_items: list[dict[str, JSONValue]] = []
    for key in _EVIDENCE_KEYS:
        if key not in metadata:
            continue
        value = metadata.get(key)
        if _is_scalar(value):
            evidence_items.append({"kind": key, "value": value})
        elif isinstance(value, list):
            evidence_items.extend(
                {"kind": key, "value": item}
                for item in value
                if _is_scalar(item)
            )
    evidence_items.extend(_topology_inspection_evidence(metadata))
    count_payload = limit.count_payload(len(evidence_items))
    return {
        **count_payload,
        "items": cast(JSONValue, limit.slice(evidence_items)),
    }


def _topology_inspection_evidence(
    metadata: Mapping[str, JSONValue],
) -> list[dict[str, JSONValue]]:
    fingerprint = _mapping(metadata.get("time_series_fingerprint"))
    topology = _mapping(fingerprint.get("temporal_topology"))
    context = _mapping(
        topology.get("inspection_context") or metadata.get("inspection_context")
    )
    evidence: list[dict[str, JSONValue]] = []
    for name in sorted(context):
        if name == "schema_version":
            continue
        section = _mapping(context.get(name))
        if not section:
            continue
        counts: dict[str, JSONValue] = {}
        for key in (
            "total_count",
            "included_count",
            "omitted_count",
            "truncated",
        ):
            value = section.get(key)
            if value is not None:
                counts[key] = value
        if counts:
            evidence.append(
                {
                    "kind": f"inspection_context.{name}",
                    "value": counts,
                }
            )
    return evidence


def _missing_evidence(
    metadata: Mapping[str, JSONValue],
    required_groups: Sequence[Sequence[str]],
) -> tuple[str, ...]:
    missing: list[str] = []
    for group in required_groups:
        if not any(_has_evidence(metadata.get(key)) for key in group):
            missing.append(" or ".join(group))
    return tuple(missing)


def _has_evidence(value: JSONValue | None) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value)
    return value is not None


def _is_scalar(value: JSONValue | None) -> bool:
    return isinstance(value, (str, int, float, bool))


def _first_string(value: JSONValue | None) -> str:
    if not isinstance(value, list):
        return ""
    for item in value:
        if isinstance(item, str) and item:
            return item
    return ""


def _sorted_count_payload(counts: Counter[str]) -> dict[str, JSONValue]:
    return {
        key: count for key, count in sorted(counts.items()) if key and count > 0
    }


def _mapping(value: JSONValue | None) -> Mapping[str, JSONValue]:
    return value if isinstance(value, Mapping) else {}


def _mapping_sequence(value: JSONValue | None) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _string_value(value: JSONValue | None) -> str:
    return value if isinstance(value, str) else ""


def _int_value(value: JSONValue | None) -> int:
    return (
        value if isinstance(value, int) and not isinstance(value, bool) else 0
    )


def _stable_finding_key(finding: QualityFinding) -> str:
    payload: dict[str, JSONValue] = {
        "message": finding.message,
        "evidence": _evidence_payload(
            finding.metadata,
            bounded_report_limit(
                MAXIMUM_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
                default_limit=DEFAULT_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
                maximum_limit=MAXIMUM_QUALITY_REPAIR_PLAN_EVIDENCE_LIMIT,
                allow_unbounded=False,
            ),
        ),
        "location": {
            "path": publish_safe_path(finding.location.path),
            "row_number": finding.location.row_number,
            "timestamp_utc_ms": finding.location.timestamp_utc_ms,
            "column": finding.location.column,
        },
    }
    return json.dumps(
        publish_safe_json_mapping(payload),
        sort_keys=True,
        separators=(",", ":"),
    )

"""Bounded next-work recommendations from saved fingerprint reports."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, cast

from histdatacom.data_quality.contracts import QualityReport, QualityTarget
from histdatacom.data_quality.fingerprint_discovery import (
    fingerprint_schema_discovery,
)
from histdatacom.data_quality.fingerprints import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
)
from histdatacom.data_quality.limits import bounded_report_limit
from histdatacom.data_quality.reporting import (
    fingerprint_readiness_risk_summary,
    quality_report_to_json,
)
from histdatacom.data_quality.training_features import (
    IDENTITY_COLUMNS,
    TRAINING_REQUIRED_COLUMNS,
    TRAINING_SCHEMA_VERSION,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import JSONValue

FINGERPRINT_NEXT_WORK_SCHEMA_VERSION = "histdatacom.fingerprint-next-work.v1"
DEFAULT_FINGERPRINT_NEXT_WORK_ALTERNATE_LIMIT = 3
DEFAULT_FINGERPRINT_NEXT_WORK_TARGET_AXIS_LIMIT = 5

_PREREQUISITE_SECTIONS = {
    "decomposition": ("stationarity_diagnostics",),
    "classical_baseline_diagnostics": (
        "stationarity_diagnostics",
        "decomposition",
    ),
    "synthetic_constraints": (
        "stationarity_diagnostics",
        "decomposition",
        "cross_series_fingerprint",
        "ascii_tick_training_substrate",
    ),
}
_PREREQUISITE_ISSUES = {
    "synthetic_constraints": ("#331",),
}
_DOWNSTREAM_CONSUMERS = {
    "stationarity_diagnostics": (
        "decomposition",
        "classical_baseline_diagnostics (#332)",
        "synthetic_constraints (#333)",
    ),
    "decomposition": (
        "classical_baseline_diagnostics (#332)",
        "synthetic_constraints (#333)",
    ),
    "cross_series_fingerprint": (
        "classical_baseline_diagnostics (#332)",
        "synthetic_constraints (#333)",
    ),
}


def fingerprint_next_work_recommendation(
    reports: Sequence[tuple[str, QualityReport]],
    *,
    alternate_limit: int | None = None,
    target_axis_limit: int | None = None,
    discovery: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    """Recommend bounded fingerprint work from already-saved reports."""
    alternate_state = bounded_report_limit(
        alternate_limit,
        default_limit=DEFAULT_FINGERPRINT_NEXT_WORK_ALTERNATE_LIMIT,
    )
    target_state = bounded_report_limit(
        target_axis_limit,
        default_limit=DEFAULT_FINGERPRINT_NEXT_WORK_TARGET_AXIS_LIMIT,
    )
    discovery_payload = dict(discovery or fingerprint_schema_discovery())
    report_inputs = _report_inputs(reports)
    evidence = _collect_evidence(reports)
    candidates = _risk_candidates(evidence)
    candidates.extend(_report_gap_candidates(evidence))
    candidates.extend(_planned_candidates(evidence, discovery_payload))
    candidates.sort(key=_candidate_sort_key)

    ranked = [
        _ranked_candidate(candidate, rank, target_state)
        for rank, candidate in enumerate(candidates, start=1)
    ]
    recommendation = ranked[0] if ranked else None
    all_alternates = ranked[1:]
    alternates = alternate_state.slice(all_alternates)
    included_count = (1 if recommendation is not None else 0) + len(alternates)
    omitted_count = max(0, len(ranked) - included_count)
    payload: dict[str, JSONValue] = {
        "schema_version": FINGERPRINT_NEXT_WORK_SCHEMA_VERSION,
        "status": "recommended" if recommendation else "no_work",
        "input_report_count": len(report_inputs),
        "input_reports": cast(JSONValue, report_inputs),
        "recommendation_count": len(ranked),
        "included_recommendation_count": included_count,
        "omitted_recommendation_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {
            "alternates": alternate_state.count_payload(len(all_alternates)),
            "representative_target_axes": target_state.limit_payload(),
        },
        "basis": {
            "source": "saved_quality_reports",
            "market_data_rescanned": False,
            "repository_workflow_inspected": False,
            "discovery_schema_version": discovery_payload.get("schema_version"),
            "fingerprint_evidence_report_count": evidence[
                "fingerprint_evidence_report_count"
            ],
            "eligible_ascii_tick_target_count": evidence[
                "eligible_target_count"
            ],
            "ignored_non_base_target_count": evidence["ignored_target_count"],
            "base_grain": {"data_format": "ascii", "timeframe": TICK},
            "training_substrate": evidence["training_substrate"],
            "cross_series": evidence["cross_series"],
            "saved_surface_presence_counts": evidence[
                "saved_surface_presence_counts"
            ],
        },
        "recommendation": recommendation,
        "alternates": cast(JSONValue, alternates),
        "no_work_reason": (
            "no fingerprint evidence or registered product gap was present"
            if recommendation is None
            else None
        ),
        "non_goals": [
            "does not rescan market data",
            "does not create, update, close, or rank GitHub issues",
            "does not inspect CI, branches, pull requests, or releases",
            "does not change quality pass/fail status",
        ],
    }
    safe_payload: object = publish_safe_json_mapping(payload)
    if not isinstance(safe_payload, Mapping):
        return {}
    return {
        str(key): cast(JSONValue, value) for key, value in safe_payload.items()
    }


def format_fingerprint_next_work(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return concise human-readable next-work recommendation text."""
    lines = [
        "Next fingerprint work",
        f"reports: {payload.get('input_report_count', 0)}",
    ]
    recommendation = _mapping(payload.get("recommendation"))
    if not recommendation:
        lines.append(
            f"status: no work ({payload.get('no_work_reason', 'no evidence')})"
        )
        return "\n".join(lines)
    lines.extend(_format_candidate_lines(recommendation, primary=True))
    alternates = _mapping_rows(payload.get("alternates"))
    if alternates:
        lines.append("alternates:")
        for alternate in alternates:
            issue = str(alternate.get("issue_reference") or "")
            issue_text = f" {issue}" if issue else ""
            lines.append(
                f"- #{alternate.get('rank', '?')} "
                f"{alternate.get('capability', 'unknown')}{issue_text}: "
                f"{alternate.get('rationale', '')}"
            )
    omitted = _int(payload.get("omitted_recommendation_count"))
    if omitted:
        lines.append(f"additional recommendations omitted: {omitted}")
    return "\n".join(lines)


def _format_candidate_lines(
    candidate: Mapping[str, JSONValue],
    *,
    primary: bool,
) -> list[str]:
    issue = str(candidate.get("issue_reference") or "")
    issue_text = f" ({issue})" if issue else ""
    prefix = "recommendation" if primary else "candidate"
    lines = [
        f"{prefix}: #{candidate.get('rank', 1)} "
        f"{candidate.get('capability', 'unknown')}{issue_text}",
        f"confidence: {candidate.get('confidence', 'unknown')}",
        f"why: {candidate.get('rationale', '')}",
        f"affected ascii/T targets: {candidate.get('affected_target_count', 0)}",
    ]
    reasons = _string_rows(candidate.get("reason_codes"))
    if reasons:
        lines.append("reasons: " + ", ".join(reasons))
    prerequisites = _string_rows(candidate.get("prerequisite_sections"))
    if prerequisites:
        lines.append("prerequisites: " + ", ".join(prerequisites))
    criteria = _string_rows(candidate.get("suggested_acceptance_criteria"))
    if criteria:
        lines.append("suggested acceptance criteria:")
        lines.extend(f"- {criterion}" for criterion in criteria)
    return lines


def _report_inputs(
    reports: Sequence[tuple[str, QualityReport]],
) -> list[dict[str, JSONValue]]:
    inputs: list[dict[str, JSONValue]] = []
    for index, (name, report) in enumerate(reports, start=1):
        risk = _saved_risk_summary(report)
        encoded = quality_report_to_json(report).encode("utf-8")
        inputs.append(
            {
                "report_name": publish_safe_path(name) or f"report-{index}",
                "content_sha256": hashlib.sha256(encoded).hexdigest(),
                "target_count": len(report.targets),
                "fingerprint_evidence": _has_fingerprint_evidence(report),
                "risk_target_count": _int(risk.get("risk_target_count")),
                "risk_payload_truncated": risk.get("truncated") is True,
            }
        )
    return inputs


def _collect_evidence(
    reports: Sequence[tuple[str, QualityReport]],
) -> dict[str, Any]:
    section_rows: dict[str, dict[str, Any]] = {}
    section_status_counts: dict[str, Counter[str]] = {}
    all_axes: dict[str, dict[str, JSONValue]] = {}
    report_surface_gap_count = 0
    truncated_risk_report_count = 0
    fingerprint_evidence_report_count = 0
    eligible_target_count = 0
    ignored_target_count = 0
    cross_payloads: list[Mapping[str, JSONValue]] = []
    training_versions: Counter[str] = Counter()
    surface_presence_counts: Counter[str] = Counter()

    for _, report in reports:
        if _has_fingerprint_evidence(report):
            fingerprint_evidence_report_count += 1
        for target in report.targets:
            axis = _target_axis(target)
            if _is_ascii_tick_axis(axis):
                all_axes[_axis_key(axis)] = axis
                eligible_target_count += 1
            else:
                ignored_target_count += 1
        risk = _saved_risk_summary(report)
        _record_surface_presence(surface_presence_counts, report, risk)
        if risk.get("truncated") is True:
            truncated_risk_report_count += 1
        report_surface_gap_count += _report_surface_gap_count(risk)
        _merge_section_status_counts(section_status_counts, risk)
        if not _mapping(risk.get("section_status_counts")):
            _merge_readiness_section_status_counts(
                section_status_counts,
                report,
            )
        for target_risk in _mapping_rows(risk.get("target_risks")):
            axis = _mapping(target_risk.get("target_axis"))
            if not _is_ascii_tick_axis(axis):
                continue
            safe_axis = cast(dict[str, JSONValue], dict(axis))
            all_axes[_axis_key(safe_axis)] = safe_axis
            for section_risk in _mapping_rows(target_risk.get("section_risks")):
                _merge_section_risk(
                    section_rows,
                    safe_axis,
                    section_risk,
                )
        cross = _mapping(
            report.metadata.get(CROSS_SERIES_FINGERPRINT_METADATA_KEY)
        )
        if cross:
            cross_payloads.append(cross)
            version = str(
                _mapping(cross.get("row_identity")).get(
                    "training_schema_version"
                )
                or ""
            )
            if version:
                training_versions[version] += 1
        metadata_version = str(
            report.metadata.get("training_schema_version") or ""
        )
        if metadata_version:
            training_versions[metadata_version] += 1
        for finding in report.findings:
            version = str(finding.metadata.get("training_schema_version") or "")
            if version:
                training_versions[version] += 1

    cross_series = _cross_series_evidence(cross_payloads)
    training_substrate = _training_substrate_evidence(
        training_versions,
        cross_series,
    )
    return {
        "section_rows": section_rows,
        "section_status_counts": section_status_counts,
        "all_axes": list(all_axes.values()),
        "report_surface_gap_count": report_surface_gap_count,
        "truncated_risk_report_count": truncated_risk_report_count,
        "fingerprint_evidence_report_count": (
            fingerprint_evidence_report_count
        ),
        "eligible_target_count": eligible_target_count,
        "ignored_target_count": ignored_target_count,
        "cross_series": cross_series,
        "training_substrate": training_substrate,
        "saved_surface_presence_counts": dict(
            sorted(surface_presence_counts.items())
        ),
    }


def _merge_section_risk(
    rows: dict[str, dict[str, Any]],
    axis: dict[str, JSONValue],
    section_risk: Mapping[str, JSONValue],
) -> None:
    section = str(section_risk.get("section") or "unknown")
    row = rows.setdefault(
        section,
        {
            "axes": {},
            "reason_counts": Counter(),
            "status_counts": Counter(),
            "risk_score": 0,
        },
    )
    row["axes"][_axis_key(axis)] = axis
    row["risk_score"] += _int(section_risk.get("score"))
    row["status_counts"][str(section_risk.get("status") or "unknown")] += 1
    reasons = _string_rows(section_risk.get("reasons"))
    if not reasons:
        reasons = ["section_not_ready"]
    row["reason_counts"].update(reasons)


def _risk_candidates(evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = []
    for section, row in evidence["section_rows"].items():
        axes = list(row["axes"].values())
        reason_counts: Counter[str] = row["reason_counts"]
        status_counts: Counter[str] = row["status_counts"]
        reasons = _ordered_counter_keys(reason_counts)
        statuses = _ordered_counter_keys(status_counts)
        severity_priority = (
            500
            if set(statuses) & {"missing", "unavailable", "skipped"}
            else 470
        )
        candidates.append(
            {
                "_priority": severity_priority,
                "_score": int(row["risk_score"]),
                "kind": "section_readiness",
                "capability": section,
                "section": section,
                "issue_reference": None,
                "rationale": (
                    f"{len(axes)} ascii/T target(s) report {section} as "
                    f"{', '.join(statuses)}; address the emitted reasons "
                    "before advancing dependent fingerprint work."
                ),
                "affected_target_count": len(axes),
                "representative_target_axes": axes,
                "reason_codes": reasons,
                "reason_counts": dict(sorted(reason_counts.items())),
                "prerequisite_sections": [],
                "prerequisite_issues": [],
                "downstream_consumers": list(
                    _DOWNSTREAM_CONSUMERS.get(section, ())
                ),
                "confidence": "high" if len(axes) > 1 else "medium",
                "basis": {
                    "source": "fingerprint_readiness_risk",
                    "risk_score": int(row["risk_score"]),
                    "status_counts": dict(sorted(status_counts.items())),
                },
                "suggested_acceptance_criteria": [
                    f"Make {section} valid or explicitly not applicable for the affected ascii/T targets.",
                    "Preserve the enriched single-row training surface and deterministic row identity.",
                    "Expose bounded reason codes and representative target axes in saved reports.",
                    "Add deterministic tests for missing, limited, and valid evidence.",
                ],
            }
        )
    return candidates


def _report_gap_candidates(
    evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    axes = list(evidence["all_axes"])
    gap_count = int(evidence["report_surface_gap_count"])
    if gap_count:
        candidates.append(
            _generic_gap_candidate(
                priority=460,
                capability="fingerprint_report_surfaces",
                rationale=(
                    f"Saved readiness metadata reports {gap_count} missing or "
                    "incomplete fingerprint report surface state(s)."
                ),
                axes=axes,
                reasons=["missing_report_surface"],
            )
        )
    truncated_count = int(evidence["truncated_risk_report_count"])
    if truncated_count:
        candidates.append(
            _generic_gap_candidate(
                priority=430,
                capability="fingerprint_readiness_drill_down",
                rationale=(
                    f"{truncated_count} saved risk report(s) omit ranked target "
                    "evidence needed to compare all affected sections."
                ),
                axes=axes,
                reasons=["readiness_risk_truncated"],
            )
        )
    return candidates


def _generic_gap_candidate(
    *,
    priority: int,
    capability: str,
    rationale: str,
    axes: list[dict[str, JSONValue]],
    reasons: list[str],
) -> dict[str, Any]:
    return {
        "_priority": priority,
        "_score": len(axes),
        "kind": "report_gap",
        "capability": capability,
        "section": None,
        "issue_reference": None,
        "rationale": rationale,
        "affected_target_count": len(axes),
        "representative_target_axes": axes,
        "reason_codes": reasons,
        "reason_counts": {reason: 1 for reason in reasons},
        "prerequisite_sections": [],
        "prerequisite_issues": [],
        "downstream_consumers": [],
        "confidence": "high",
        "basis": {"source": "saved_report_surface_metadata"},
        "suggested_acceptance_criteria": [
            "Emit the missing report surface from saved fingerprint evidence.",
            "Keep the output bounded with explicit truncation metadata.",
            "Add JSON, human-rendering, and compatibility tests.",
        ],
    }


def _planned_candidates(
    evidence: Mapping[str, Any],
    discovery: Mapping[str, JSONValue],
) -> list[dict[str, Any]]:
    if not int(evidence["fingerprint_evidence_report_count"]):
        return []
    sections = _mapping(discovery.get("sections"))
    implemented = _mapping(sections.get("implemented"))
    planned = _mapping(sections.get("planned"))
    implemented_names = {
        str(row.get("name") or "")
        for row in (
            _mapping_rows(implemented.get("target_sections"))
            + _mapping_rows(implemented.get("run_sections"))
        )
    }
    candidates = []
    planned_rows = _mapping_rows(
        planned.get("target_sections")
    ) + _mapping_rows(planned.get("run_sections"))
    for row in planned_rows:
        capability = str(row.get("name") or "")
        if not capability:
            continue
        prerequisites = list(_PREREQUISITE_SECTIONS.get(capability, ()))
        prerequisite_evidence = _prerequisite_evidence(
            prerequisites,
            implemented_names,
            evidence,
        )
        blockers = [
            str(item["section"])
            for item in prerequisite_evidence
            if item["ready"] is not True
        ]
        reasons = (
            [f"prerequisite_not_ready:{section}" for section in blockers]
            if blockers
            else ["registered_planned_capability"]
        )
        issue = str(row.get("issue") or "") or None
        candidates.append(
            {
                "_priority": 210 if not blockers else 170,
                "_score": int(evidence["eligible_target_count"]),
                "kind": "planned_capability",
                "capability": capability,
                "section": capability,
                "issue_reference": issue,
                "rationale": _planned_rationale(
                    capability,
                    blockers,
                    int(evidence["eligible_target_count"]),
                ),
                "affected_target_count": int(evidence["eligible_target_count"]),
                "representative_target_axes": list(evidence["all_axes"]),
                "reason_codes": reasons,
                "reason_counts": {reason: 1 for reason in reasons},
                "prerequisite_sections": prerequisites,
                "prerequisite_issues": list(
                    _PREREQUISITE_ISSUES.get(capability, ())
                ),
                "prerequisite_evidence": prerequisite_evidence,
                "downstream_consumers": list(
                    _DOWNSTREAM_CONSUMERS.get(capability, ())
                ),
                "confidence": "high" if not blockers else "low",
                "basis": {
                    "source": "fingerprint_discovery_and_saved_reports",
                    "registry_status": row.get("status"),
                    "training_substrate": evidence["training_substrate"],
                    "cross_series": evidence["cross_series"],
                },
                "suggested_acceptance_criteria": _planned_acceptance_criteria(
                    capability
                ),
            }
        )
    return candidates


def _prerequisite_evidence(
    prerequisites: Sequence[str],
    implemented_names: set[str],
    evidence: Mapping[str, Any],
) -> list[dict[str, JSONValue]]:
    rows: list[dict[str, JSONValue]] = []
    status_counts = evidence["section_status_counts"]
    cross = evidence["cross_series"]
    for section in prerequisites:
        if section == "ascii_tick_training_substrate":
            training = evidence["training_substrate"]
            observed_status = str(
                training.get("training_facing_columns_status") or "unknown"
            )
            rows.append(
                {
                    "section": section,
                    "implemented": training.get(
                        "legacy_raw_cache_enrichment_on_read_supported"
                    )
                    is True,
                    "observed_valid_target_count": _int(
                        evidence.get("eligible_target_count")
                    ),
                    "ready": observed_status in {"confirmed", "partial"},
                    "basis": f"training_columns_{observed_status}",
                }
            )
            continue
        if section == "cross_series_fingerprint":
            observed_count = _int(cross.get("observed_report_count"))
            ready = section in implemented_names and observed_count > 0
            rows.append(
                {
                    "section": section,
                    "implemented": section in implemented_names,
                    "observed_valid_target_count": observed_count,
                    "ready": ready,
                    "basis": "cross_series_report_metadata",
                }
            )
            continue
        counts: Counter[str] = status_counts.get(section, Counter())
        valid_count = counts.get("valid", 0) + counts.get("computed", 0)
        ready = section in implemented_names and valid_count > 0
        rows.append(
            {
                "section": section,
                "implemented": section in implemented_names,
                "observed_valid_target_count": valid_count,
                "ready": ready,
                "basis": "readiness_section_status_counts",
            }
        )
    return rows


def _planned_rationale(
    capability: str,
    blockers: Sequence[str],
    target_count: int,
) -> str:
    if blockers:
        return (
            f"{capability} is registered as planned, but saved evidence does "
            f"not yet confirm {', '.join(blockers)}; complete those product "
            "prerequisites first."
        )
    return (
        f"{capability} is the next registered capability and its known "
        f"prerequisites are present for {target_count} ascii/T target(s)."
    )


def _planned_acceptance_criteria(capability: str) -> list[str]:
    criteria = [
        f"Implement {capability} from existing fingerprint and readiness evidence without rescanning in this recommendation command.",
        "Preserve ascii/T as the only base grain and the enriched single-row training surface.",
        "Keep row identity durable across duplicate timestamps and legacy cache enrichment.",
        "Emit deterministic bounded JSON and concise human-readable evidence.",
    ]
    if capability in {"synthetic_constraints", "cross_series_fingerprint"}:
        criteria.append(
            "Include bounded duplicate-timestamp, unequal-range, and triangle evidence."
        )
    return criteria


def _ranked_candidate(
    candidate: Mapping[str, Any],
    rank: int,
    target_state: Any,
) -> dict[str, JSONValue]:
    result = {
        key: cast(JSONValue, value)
        for key, value in candidate.items()
        if not key.startswith("_") and key != "representative_target_axes"
    }
    axes = list(candidate.get("representative_target_axes", ()))
    included_axes = target_state.slice(axes)
    result.update(
        {
            "rank": rank,
            "representative_target_axes": cast(JSONValue, included_axes),
            "target_axis_limit_metadata": target_state.count_payload(len(axes)),
        }
    )
    return result


def _candidate_sort_key(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        -int(candidate.get("_priority", 0)),
        -int(candidate.get("_score", 0)),
        -int(candidate.get("affected_target_count", 0)),
        str(candidate.get("capability") or ""),
        str(candidate.get("issue_reference") or ""),
    )


def _saved_risk_summary(report: QualityReport) -> dict[str, JSONValue]:
    saved = _mapping(
        report.metadata.get(TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY)
    )
    if saved:
        return cast(dict[str, JSONValue], dict(saved))
    computed = fingerprint_readiness_risk_summary(
        report,
        target_limit=-1,
        section_limit=-1,
        reason_limit=-1,
    )
    return computed or {}


def _has_fingerprint_evidence(report: QualityReport) -> bool:
    if _saved_risk_summary(report):
        return True
    if _mapping(
        report.metadata.get(
            TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
        )
    ):
        return True
    return any(
        _mapping(finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY))
        for finding in report.findings
    )


def _merge_section_status_counts(
    target: dict[str, Counter[str]],
    risk: Mapping[str, JSONValue],
) -> None:
    for section, raw_counts in _mapping(
        risk.get("section_status_counts")
    ).items():
        counts = target.setdefault(section, Counter())
        for status, count in _mapping(raw_counts).items():
            counts[status] += _int(count)


def _merge_readiness_section_status_counts(
    target: dict[str, Counter[str]],
    report: QualityReport,
) -> None:
    readiness = _mapping(
        report.metadata.get(
            TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
        )
    )
    for summary in _mapping_rows(readiness.get("target_summaries")):
        axis = _mapping(summary.get("target_axis"))
        if not _is_ascii_tick_axis(axis):
            continue
        for section, status in _mapping(
            summary.get("section_statuses")
        ).items():
            target.setdefault(section, Counter())[str(status)] += 1


def _record_surface_presence(
    counts: Counter[str],
    report: QualityReport,
    risk: Mapping[str, JSONValue],
) -> None:
    surfaces = {
        "coverage": TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
        "topology": TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
        "distribution": (
            TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY
        ),
        "regime": TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
        "readiness": TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
        "readiness_risk": (TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY),
        "cross_series": CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    }
    for name, key in surfaces.items():
        if _mapping(report.metadata.get(key)):
            counts[name] += 1
    if risk and not _mapping(
        report.metadata.get(TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY)
    ):
        counts["readiness_risk"] += 1
    for finding in report.findings:
        fingerprint = _mapping(
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        )
        for section in (
            "conditional_distributions",
            "microstructure_dynamics",
            "dependence",
            "stationarity_diagnostics",
            "decomposition",
        ):
            if _mapping(fingerprint.get(section)):
                counts[section] += 1


def _report_surface_gap_count(risk: Mapping[str, JSONValue]) -> int:
    evidence = _mapping(risk.get("report_surface_evidence"))
    gap_count = 0
    for key in (
        "report_metadata_state_counts",
        "bounded_payload_state_counts",
        "cli_summary_state_counts",
    ):
        for state, count in _mapping(evidence.get(key)).items():
            if state != "present":
                gap_count += _int(count)
    return gap_count


def _cross_series_evidence(
    payloads: Sequence[Mapping[str, JSONValue]],
) -> dict[str, JSONValue]:
    identity_columns: set[str] = set()
    training_versions: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    group_count = 0
    incomplete_group_count = 0
    duplicate_timestamp_row_count = 0
    unequal_range_group_count = 0
    triangle_candidate_count = 0
    triangle_compared_timestamp_count = 0
    cache_source_count = 0
    for payload in payloads:
        status_counts[str(payload.get("status") or "unknown")] += 1
        group_count += _int(payload.get("group_count"))
        incomplete_group_count += _int(payload.get("incomplete_group_count"))
        row_identity = _mapping(payload.get("row_identity"))
        identity_columns.update(_string_rows(row_identity.get("columns")))
        duplicate_timestamp_row_count += _int(
            row_identity.get("duplicate_timestamp_row_count")
        )
        version = str(row_identity.get("training_schema_version") or "")
        if version:
            training_versions[version] += 1
        cache_source_count += sum(
            _int(value)
            for value in _mapping(payload.get("cache_source_counts")).values()
        )
        triangular = _mapping(payload.get("triangular_consistency"))
        triangle_candidate_count += _int(triangular.get("candidate_count"))
        triangle_compared_timestamp_count += _int(
            triangular.get("compared_timestamp_count")
        )
        for group in _mapping_rows(payload.get("groups")):
            coverage = _mapping(group.get("coverage_ranges"))
            if coverage.get("unequal_ranges") is True:
                unequal_range_group_count += 1
        for panel in _mapping_rows(payload.get("panel_coverage")):
            if panel.get("unequal_period_ranges") is True:
                unequal_range_group_count += 1
    return {
        "observed_report_count": len(payloads),
        "status_counts": dict(sorted(status_counts.items())),
        "group_count": group_count,
        "incomplete_group_count": incomplete_group_count,
        "row_identity_columns": cast(
            JSONValue,
            sorted(identity_columns),
        ),
        "training_schema_version_counts": dict(
            sorted(training_versions.items())
        ),
        "duplicate_timestamp_row_count": duplicate_timestamp_row_count,
        "unequal_range_group_count": unequal_range_group_count,
        "triangle_candidate_count": triangle_candidate_count,
        "triangle_compared_timestamp_count": triangle_compared_timestamp_count,
        "cache_source_count": cache_source_count,
    }


def _training_substrate_evidence(
    versions: Counter[str],
    cross_series: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    for version, count in _mapping(
        cross_series.get("training_schema_version_counts")
    ).items():
        versions[version] += _int(count)
    identity_columns = set(
        _string_rows(cross_series.get("row_identity_columns"))
    )
    current_count = versions.get(TRAINING_SCHEMA_VERSION, 0)
    if current_count:
        column_status = "confirmed"
    elif identity_columns:
        column_status = "partial"
    else:
        column_status = "unknown"
    required_identity = set(IDENTITY_COLUMNS)
    observed_identity = required_identity & identity_columns
    cache_source_count = _int(cross_series.get("cache_source_count"))
    return {
        "schema_version": TRAINING_SCHEMA_VERSION,
        "observed_schema_version_counts": dict(sorted(versions.items())),
        "training_facing_columns_status": column_status,
        "required_column_count": len(TRAINING_REQUIRED_COLUMNS),
        "required_identity_column_count": len(required_identity),
        "observed_identity_column_count": len(observed_identity),
        "observed_identity_columns": cast(
            JSONValue,
            sorted(observed_identity),
        ),
        "single_row_training_surface": True,
        "legacy_raw_cache_enrichment_on_read_supported": True,
        "observed_cache_projection_count": cache_source_count,
        "observed_enriched_cache_projection": (
            cache_source_count > 0 and current_count > 0
        ),
        "timestamp_is_durable_identity": False,
    }


def _target_axis(target: QualityTarget) -> dict[str, JSONValue]:
    return {
        "data_format": target.data_format,
        "symbol": target.symbol,
        "timeframe": target.timeframe,
        "period": target.period,
        "kind": target.kind.value,
    }


def _is_ascii_tick_axis(axis: Mapping[str, JSONValue]) -> bool:
    return (
        str(axis.get("data_format") or "").lower() == "ascii"
        and str(axis.get("timeframe") or "").upper() == TICK
    )


def _axis_key(axis: Mapping[str, JSONValue]) -> str:
    return "|".join(
        str(axis.get(key) or "")
        for key in ("data_format", "symbol", "timeframe", "period", "kind")
    )


def _ordered_counter_keys(counter: Counter[str]) -> list[str]:
    return [
        key
        for key, _ in sorted(
            counter.items(), key=lambda item: (-item[1], item[0])
        )
    ]


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _mapping_rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [_mapping(item) for item in value if isinstance(item, Mapping)]


def _string_rows(value: object) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item) for item in value if str(item)]


def _int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0

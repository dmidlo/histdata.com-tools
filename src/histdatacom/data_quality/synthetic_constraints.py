"""Generator-facing synthetic tick constraint and validation contracts."""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any, cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityReport,
)
from histdatacom.data_quality.limits import (
    BoundedReportLimit,
    bounded_report_limit,
)
from histdatacom.data_quality.training_features import (
    IDENTITY_COLUMNS,
    QUALITY_ISSUE_COLUMNS,
    SYNTHETIC_PLACEHOLDER_COLUMNS,
    TRAINING_REQUIRED_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    ensure_tick_training_features,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.runtime_contracts import JSONValue

SYNTHETIC_CONSTRAINTS_SCHEMA_VERSION = (
    "histdatacom.synthetic-fingerprint-constraints.v1"
)
SYNTHETIC_CONSTRAINT_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.synthetic-fingerprint-constraint-summary.v1"
)
SYNTHETIC_VALIDATION_SCHEMA_VERSION = (
    "histdatacom.synthetic-fingerprint-validation.v1"
)
SYNTHETIC_CONSTRAINT_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_synthetic_constraint_summary"
)
SYNTHETIC_CONSTRAINT_BOUNDED_PAYLOAD_KEY = "fingerprint_synthetic_constraints"
TIME_SERIES_FINGERPRINT_METADATA_KEY = "time_series_fingerprint"

DEFAULT_SYNTHETIC_CONSTRAINT_CATEGORY_LIMIT = 32
DEFAULT_SYNTHETIC_CONSTRAINT_HINT_LIMIT = 32
DEFAULT_SYNTHETIC_CONSTRAINT_SUMMARY_LIMIT = 32
DEFAULT_SYNTHETIC_VALIDATION_TARGET_LIMIT = 32
DEFAULT_SYNTHETIC_VALIDATION_MISMATCH_LIMIT = 32

_DEFECT_SPECS = (
    (
        "avoid_negative_spread",
        "dq_issue_negative_spread",
        "negative_spread_count",
        "hard",
    ),
    (
        "avoid_duplicate_timestamps",
        "dq_issue_duplicate_timestamp",
        "duplicate_timestamp_count",
        "advisory",
    ),
    (
        "avoid_non_monotonic_timestamps",
        "dq_issue_non_monotonic_timestamp",
        "non_monotonic_count",
        "hard",
    ),
    (
        "avoid_suspicious_non_session_gaps",
        "dq_issue_suspicious_gap",
        "suspicious_gap_count",
        "advisory",
    ),
    (
        "avoid_invalid_rows",
        "dq_issue_invalid_row",
        "invalid_row_count",
        "hard",
    ),
    (
        "avoid_partial_rows",
        "dq_issue_partial_row",
        "partial_row_count",
        "hard",
    ),
    (
        "avoid_topology_unavailable",
        "dq_issue_topology_unavailable",
        "topology_unavailable_count",
        "hard",
    ),
    (
        "avoid_fingerprint_unready",
        "dq_issue_fingerprint_unready",
        "fingerprint_unready_count",
        "advisory",
    ),
)


def synthetic_constraints_from_training_frame(
    training_frame: Any,
    *,
    fingerprint: Mapping[str, JSONValue] | None = None,
    target: Any | None = None,
    category_limit: int | None = DEFAULT_SYNTHETIC_CONSTRAINT_CATEGORY_LIMIT,
    hint_limit: int | None = DEFAULT_SYNTHETIC_CONSTRAINT_HINT_LIMIT,
) -> dict[str, JSONValue]:
    """Derive constraints with the enriched tick frame as the primary input."""
    payload = dict(fingerprint or {})
    if "target_axis" not in payload:
        payload["target_axis"] = _target_axis_from_target(target)
    if "source" not in payload:
        payload["source"] = {
            "kind": "cache",
            "cache_source": "direct",
        }
    return synthetic_constraints_from_fingerprint(
        payload,
        training_frame=training_frame,
        target=target,
        category_limit=category_limit,
        hint_limit=hint_limit,
    )


def synthetic_constraints_from_fingerprint(
    fingerprint: Mapping[str, JSONValue],
    *,
    training_frame: Any | None = None,
    target: Any | None = None,
    category_limit: int | None = DEFAULT_SYNTHETIC_CONSTRAINT_CATEGORY_LIMIT,
    hint_limit: int | None = DEFAULT_SYNTHETIC_CONSTRAINT_HINT_LIMIT,
) -> dict[str, JSONValue]:
    """Derive bounded ASCII-tick generator constraints from one fingerprint."""
    category_state = bounded_report_limit(
        category_limit,
        default_limit=DEFAULT_SYNTHETIC_CONSTRAINT_CATEGORY_LIMIT,
    )
    hint_state = bounded_report_limit(
        hint_limit,
        default_limit=DEFAULT_SYNTHETIC_CONSTRAINT_HINT_LIMIT,
    )
    target_axis = _mapping(fingerprint.get("target_axis"))
    source = _mapping(fingerprint.get("source"))
    supported = (
        _text(target_axis.get("data_format")) == "ascii"
        and _text(target_axis.get("timeframe")) == TICK
    )
    training, enriched = _training_substrate_payload(
        training_frame,
        target=target,
        source=source,
    )
    defects = _defect_constraints(fingerprint, enriched)
    stylized = _stylized_fact_constraints(fingerprint, enriched)
    artifacts = _source_artifact_constraints(fingerprint, training)
    hints = _constraint_hints(fingerprint, defects, stylized)
    limitations = _constraint_limitations(
        fingerprint,
        training,
        supported=supported,
    )
    included_defects = category_state.slice(defects)
    included_stylized = category_state.slice(stylized)
    included_artifacts = category_state.slice(artifacts)
    included_hints = hint_state.slice(hints)
    status = "ready"
    if not supported:
        status = "unavailable"
    elif limitations:
        status = "limited"
    payload: dict[str, JSONValue] = {
        "schema_version": SYNTHETIC_CONSTRAINTS_SCHEMA_VERSION,
        "status": status,
        "advisory": True,
        "base_grain": {"data_format": "ascii", "timeframe": TICK},
        "target_axis": dict(target_axis),
        "training_substrate": training,
        "output_contract": {
            "observed_columns_preserved": ["bid", "ask"],
            "synthetic_output_columns": list(SYNTHETIC_PLACEHOLDER_COLUMNS),
            "durable_identity_columns": [
                "series_id",
                "period",
                "row_id",
            ],
            "timestamp_is_sole_identity": False,
            "non_tick_input_constraints_supported": False,
            "generation_in_scope": False,
        },
        "defect_count": len(defects),
        "stylized_fact_count": len(stylized),
        "source_artifact_count": len(artifacts),
        "hint_count": len(hints),
        "included_defect_count": len(included_defects),
        "included_stylized_fact_count": len(included_stylized),
        "included_source_artifact_count": len(included_artifacts),
        "included_hint_count": len(included_hints),
        "omitted_defect_count": max(0, len(defects) - len(included_defects)),
        "omitted_stylized_fact_count": max(
            0, len(stylized) - len(included_stylized)
        ),
        "omitted_source_artifact_count": max(
            0, len(artifacts) - len(included_artifacts)
        ),
        "omitted_hint_count": max(0, len(hints) - len(included_hints)),
        "truncated": any(
            (
                len(included_defects) < len(defects),
                len(included_stylized) < len(stylized),
                len(included_artifacts) < len(artifacts),
                len(included_hints) < len(hints),
            )
        ),
        "limit_metadata": {
            "categories": category_state.limit_payload(),
            "hints": hint_state.limit_payload(),
        },
        "defects_to_avoid": cast(JSONValue, included_defects),
        "stylized_facts_to_preserve": cast(JSONValue, included_stylized),
        "source_artifacts_to_parameterize": cast(JSONValue, included_artifacts),
        "advisory_hints": cast(JSONValue, included_hints),
        "limitation_codes": cast(JSONValue, limitations),
    }
    payload["constraint_id"] = _payload_id(payload)
    return payload


def synthetic_constraint_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_SYNTHETIC_CONSTRAINT_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return a bounded report-level rollup of synthetic constraints."""
    limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_SYNTHETIC_CONSTRAINT_SUMMARY_LIMIT,
    )
    targets: list[dict[str, JSONValue]] = []
    status_counts: Counter[str] = Counter()
    defect_counts: Counter[str] = Counter()
    hint_counts: Counter[str] = Counter()
    substrate_counts: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _mapping(
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        )
        constraints = _mapping(fingerprint.get("synthetic_constraints"))
        if not constraints:
            continue
        status = _text(constraints.get("status")) or "unknown"
        training = _mapping(constraints.get("training_substrate"))
        substrate_status = _text(training.get("status")) or "unknown"
        status_counts[status] += 1
        substrate_counts[substrate_status] += 1
        defects = _mapping_rows(constraints.get("defects_to_avoid"))
        for defect in defects:
            if _int(defect.get("observed_count")) > 0:
                defect_counts[_text(defect.get("code"))] += 1
        hints = _strings(constraints.get("advisory_hints"))
        hint_counts.update(hints)
        targets.append(
            {
                "target_axis": dict(_mapping(constraints.get("target_axis"))),
                "status": status,
                "constraint_id": constraints.get("constraint_id"),
                "training_substrate_status": substrate_status,
                "observed_defect_count": sum(
                    1
                    for defect in defects
                    if _int(defect.get("observed_count")) > 0
                ),
                "stylized_fact_count": _int(
                    constraints.get("stylized_fact_count")
                ),
                "advisory_hints": cast(JSONValue, hints),
                "limitation_codes": cast(
                    JSONValue,
                    _strings(constraints.get("limitation_codes")),
                ),
            }
        )
    if not targets:
        return None
    targets.sort(key=_target_sort_key)
    included = limit_state.slice(targets)
    omitted = max(0, len(targets) - len(included))
    return {
        "schema_version": SYNTHETIC_CONSTRAINT_SUMMARY_SCHEMA_VERSION,
        "target_count": len(targets),
        "ready_target_count": status_counts.get("ready", 0),
        "limited_target_count": status_counts.get("limited", 0),
        "unavailable_target_count": status_counts.get("unavailable", 0),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "limit_metadata": {"targets": limit_state.limit_payload()},
        "status_counts": _counter_payload(status_counts),
        "training_substrate_status_counts": _counter_payload(substrate_counts),
        "observed_defect_target_counts": _counter_payload(defect_counts),
        "advisory_hint_target_counts": _counter_payload(hint_counts),
        "target_summaries": cast(JSONValue, included),
    }


def validate_synthetic_constraint_reports(
    reference: QualityReport,
    candidate: QualityReport,
    *,
    target_limit: int | None = DEFAULT_SYNTHETIC_VALIDATION_TARGET_LIMIT,
    mismatch_limit: int | None = DEFAULT_SYNTHETIC_VALIDATION_MISMATCH_LIMIT,
) -> dict[str, JSONValue]:
    """Compare candidate fingerprint constraints with reference constraints."""
    target_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_SYNTHETIC_VALIDATION_TARGET_LIMIT,
    )
    mismatch_state = bounded_report_limit(
        mismatch_limit,
        default_limit=DEFAULT_SYNTHETIC_VALIDATION_MISMATCH_LIMIT,
    )
    references = _constraints_by_axis(reference.findings)
    candidates = _constraints_by_axis(candidate.findings)
    results: list[dict[str, JSONValue]] = []
    mismatch_code_counts: Counter[str] = Counter()
    for axis_key, constraints in sorted(references.items()):
        candidate_constraints = candidates.get(axis_key)
        result = _validate_target_constraints(
            constraints,
            candidate_constraints,
            mismatch_state=mismatch_state,
        )
        results.append(result)
        for code, count in _mapping(result.get("mismatch_code_counts")).items():
            mismatch_code_counts[code] += _int(count)
    results.sort(key=_validation_target_sort_key)
    included = target_state.slice(results)
    omitted = max(0, len(results) - len(included))
    compared = sum(
        1 for item in results if item.get("status") != "not_compared"
    )
    mismatched = sum(1 for item in results if item.get("status") == "mismatch")
    status = "not_compared"
    if compared:
        status = "mismatch" if mismatched else "match"
    return {
        "schema_version": SYNTHETIC_VALIDATION_SCHEMA_VERSION,
        "status": status,
        "advisory": True,
        "base_grain": {"data_format": "ascii", "timeframe": TICK},
        "reference_target_count": len(references),
        "candidate_target_count": len(candidates),
        "compared_target_count": compared,
        "matching_target_count": sum(
            1 for item in results if item.get("status") == "match"
        ),
        "mismatched_target_count": mismatched,
        "not_compared_target_count": sum(
            1 for item in results if item.get("status") == "not_compared"
        ),
        "mismatch_count": sum(mismatch_code_counts.values()),
        "mismatch_code_counts": _counter_payload(mismatch_code_counts),
        "included_target_count": len(included),
        "omitted_target_count": omitted,
        "truncated": omitted > 0,
        "limit_metadata": {
            "targets": target_state.limit_payload(),
            "mismatches": mismatch_state.limit_payload(),
        },
        "target_results": cast(JSONValue, included),
    }


def format_synthetic_constraint_summary_lines(
    summary: Mapping[str, JSONValue] | None,
) -> list[str]:
    """Return concise human-readable constraint summary lines."""
    if not summary:
        return []
    lines = [
        "",
        "Synthetic fingerprint constraints",
        (
            "- targets: "
            f"{_int(summary.get('target_count'))} "
            f"ready: {_int(summary.get('ready_target_count'))} "
            f"limited: {_int(summary.get('limited_target_count'))} "
            f"unavailable: {_int(summary.get('unavailable_target_count'))}"
        ),
    ]
    defect_counts = _mapping(summary.get("observed_defect_target_counts"))
    if defect_counts:
        lines.append("- observed defects: " + _format_counts(defect_counts))
    for target in _mapping_rows(summary.get("target_summaries")):
        axis = _mapping(target.get("target_axis"))
        lines.append(
            "- "
            f"{_format_axis(axis)}: {_text(target.get('status'))} "
            f"facts={_int(target.get('stylized_fact_count'))} "
            f"observed-defects={_int(target.get('observed_defect_count'))}"
        )
    return lines


def format_synthetic_validation(payload: Mapping[str, JSONValue]) -> str:
    """Return concise human-readable synthetic validation output."""
    lines = [
        "Synthetic fingerprint validation",
        f"status: {_text(payload.get('status'))}",
        (
            "targets: "
            f"{_int(payload.get('reference_target_count'))} reference, "
            f"{_int(payload.get('candidate_target_count'))} candidate, "
            f"{_int(payload.get('mismatched_target_count'))} mismatched"
        ),
    ]
    counts = _mapping(payload.get("mismatch_code_counts"))
    if counts:
        lines.append("mismatch codes: " + _format_counts(counts))
    for result in _mapping_rows(payload.get("target_results")):
        axis = _mapping(result.get("target_axis"))
        codes = ",".join(_strings(result.get("mismatch_codes"))) or "none"
        lines.append(
            f"- {_format_axis(axis)}: {_text(result.get('status'))} "
            f"codes={codes}"
        )
    return "\n".join(lines)


def _training_substrate_payload(
    frame: Any | None,
    *,
    target: Any | None,
    source: Mapping[str, JSONValue],
) -> tuple[dict[str, JSONValue], Any | None]:
    unavailable: dict[str, JSONValue] = {
        "status": "unavailable",
        "reason": "training_frame_unavailable",
        "training_schema_version": None,
        "row_count": 0,
        "required_column_count": len(TRAINING_REQUIRED_COLUMNS),
        "missing_required_columns": list(TRAINING_REQUIRED_COLUMNS),
    }
    if frame is None:
        return unavailable, None
    raw_columns = set(getattr(frame, "columns", ()))
    try:
        enriched = ensure_tick_training_features(frame, target=target)
    except (KeyError, OSError, TypeError, ValueError) as exc:
        unavailable["reason"] = "training_enrichment_unavailable"
        unavailable["error_type"] = type(exc).__name__
        return unavailable, None
    columns = set(getattr(enriched, "columns", ()))
    missing = sorted(set(TRAINING_REQUIRED_COLUMNS) - columns)
    input_kind = _text(source.get("kind")) or "unknown"
    cache_input = input_kind == "cache"
    input_was_enriched = "training_schema_version" in raw_columns
    return (
        {
            "status": "available" if not missing else "limited",
            "training_schema_version": TRAINING_SCHEMA_VERSION,
            "row_count": int(getattr(enriched, "height", 0) or 0),
            "required_column_count": len(TRAINING_REQUIRED_COLUMNS),
            "column_count": len(columns),
            "missing_required_columns": cast(JSONValue, missing),
            "input_kind": input_kind,
            "cache_source": source.get("cache_source"),
            "cache_was_enriched": cache_input and input_was_enriched,
            "legacy_cache_enriched_on_read": (
                cache_input and not input_was_enriched
            ),
            "source_rows_enriched_in_memory": (
                not cache_input and not input_was_enriched
            ),
            "identity_columns_present": set(IDENTITY_COLUMNS).issubset(columns),
            "quality_issue_columns_present": set(
                QUALITY_ISSUE_COLUMNS
            ).issubset(columns),
            "synthetic_output_columns_present": set(
                SYNTHETIC_PLACEHOLDER_COLUMNS
            ).issubset(columns),
        },
        enriched,
    )


def _defect_constraints(
    fingerprint: Mapping[str, JSONValue],
    frame: Any | None,
) -> list[dict[str, JSONValue]]:
    fallback = _fingerprint_defect_counts(fingerprint)
    constraints: list[dict[str, JSONValue]] = []
    for code, column, fallback_key, severity in _DEFECT_SPECS:
        count = _frame_true_count(frame, column)
        source = "training_feature_column"
        if count is None:
            count = fallback.get(fallback_key, 0)
            source = "fingerprint_fallback"
        constraints.append(
            {
                "code": code,
                "issue_column": column,
                "severity": severity,
                "requirement": "must_be_zero",
                "observed_count": count,
                "source": source,
            }
        )
    missing_columns = []
    if frame is not None:
        missing_columns = sorted(
            set(TRAINING_REQUIRED_COLUMNS) - set(frame.columns)
        )
    constraints.append(
        {
            "code": "avoid_unsupported_schema",
            "issue_column": None,
            "severity": "hard",
            "requirement": "canonical_training_schema_required",
            "observed_count": 1 if missing_columns else 0,
            "source": "training_schema_contract",
            "missing_column_count": len(missing_columns),
        }
    )
    constraints.append(
        {
            "code": "avoid_structurally_invalid_timestamps",
            "issue_column": None,
            "severity": "hard",
            "requirement": "must_be_zero",
            "observed_count": fallback.get("invalid_timestamp_count", 0),
            "source": "temporal_topology",
        }
    )
    return constraints


def _fingerprint_defect_counts(
    fingerprint: Mapping[str, JSONValue],
) -> dict[str, int]:
    topology = _mapping(fingerprint.get("temporal_topology"))
    distribution = _mapping(fingerprint.get("tick_distribution"))
    audit = _mapping(fingerprint.get("fingerprint_audit"))
    statuses = _mapping(audit.get("section_statuses"))
    unavailable_topology = (
        1
        if _text(topology.get("computed_from")) == "unavailable"
        or topology.get("parsed_row_count") is None
        else 0
    )
    unready = sum(
        1
        for status in statuses.values()
        if _text(status) in {"limited", "unavailable", "skipped"}
    )
    return {
        "negative_spread_count": _int(
            distribution.get("negative_spread_count")
        ),
        "duplicate_timestamp_count": _int(
            topology.get("duplicate_timestamp_count")
        ),
        "non_monotonic_count": _int(topology.get("non_monotonic_count")),
        "suspicious_gap_count": _int(topology.get("suspicious_gap_count")),
        "invalid_row_count": _int(distribution.get("invalid_row_count")),
        "partial_row_count": _int(distribution.get("partial_row_count")),
        "topology_unavailable_count": unavailable_topology,
        "fingerprint_unready_count": unready,
        "invalid_timestamp_count": _int(
            topology.get("invalid_timestamp_count")
        ),
    }


def _stylized_fact_constraints(
    fingerprint: Mapping[str, JSONValue],
    training_frame: Any | None,
) -> list[dict[str, JSONValue]]:
    topology = _mapping(fingerprint.get("temporal_topology"))
    calendar = _mapping(fingerprint.get("calendar_regimes"))
    distribution = _mapping(fingerprint.get("tick_distribution"))
    dynamics = _mapping(fingerprint.get("microstructure_dynamics"))
    dependence = _mapping(fingerprint.get("dependence"))
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    decomposition = _mapping(fingerprint.get("decomposition"))
    facts: list[dict[str, JSONValue]] = []
    _append_fact(
        facts,
        "session_activity_mix",
        "calendar_regimes.session_state_counts",
        calendar.get("session_state_counts"),
        "distribution_l1",
        0.1,
    )
    _append_fact(
        facts,
        "active_session_mix",
        "calendar_regimes.active_session_counts",
        calendar.get("active_session_counts"),
        "distribution_l1",
        0.1,
    )
    _append_fact(
        facts,
        "calendar_special_tag_mix",
        "calendar_regimes.special_tag_counts",
        calendar.get("special_tag_counts"),
        "distribution_l1",
        0.1,
    )
    _append_fact(
        facts,
        "gap_bucket_shape",
        "temporal_topology.gap_bucket_counts",
        topology.get("gap_bucket_counts"),
        "distribution_l1",
        0.1,
    )
    _append_fact(
        facts,
        "interval_topology",
        "temporal_topology",
        _selected(
            topology,
            (
                "sampling_basis",
                "min_interval_ms",
                "median_interval_ms",
                "max_gap_ms",
                "expected_session_closure_count",
                "weekend_activity_count",
            ),
        ),
        "numeric_relative_tolerance",
        0.1,
    )
    _append_fact(
        facts,
        "spread_distribution",
        "tick_distribution.spread",
        _without_count(_mapping(distribution.get("spread"))),
        "numeric_relative_tolerance",
        0.1,
    )
    _append_fact(
        facts,
        "precision_regime",
        "training_substrate.observed_float_decimal_places",
        _training_precision_regime(training_frame),
        "distribution_l1",
        0.0,
    )
    for code, key in (
        ("spread_jump_behavior", "spread_jump"),
        ("stale_quote_runs", "stale_quote"),
        ("burst_behavior", "burst"),
        ("one_sided_movement", "one_sided_movement"),
    ):
        _append_fact(
            facts,
            code,
            f"microstructure_dynamics.{key}",
            dynamics.get(key),
            "numeric_relative_tolerance",
            0.1,
        )
    absolute_acf = _mapping(dependence.get("absolute_spread_change_acf"))
    _append_fact(
        facts,
        "volatility_clustering_proxy",
        "dependence.absolute_spread_change_acf.lag_acf",
        absolute_acf.get("lag_acf"),
        "numeric_absolute_tolerance",
        0.2,
    )
    shift = _mapping(stationarity.get("first_middle_last_distribution_shift"))
    _append_fact(
        facts,
        "return_tail_shape",
        "stationarity_diagnostics.first_middle_last_distribution_shift.return",
        shift.get("return"),
        "numeric_relative_tolerance",
        0.1,
    )
    _append_fact(
        facts,
        "rolling_drift",
        "stationarity_diagnostics.rolling_windows",
        stationarity.get("rolling_windows"),
        "numeric_relative_tolerance",
        0.1,
    )
    _append_fact(
        facts,
        "stationarity_transform_policy",
        "stationarity_diagnostics.recommended_transforms",
        stationarity.get("recommended_transforms"),
        "exact",
        0.0,
    )
    _append_fact(
        facts,
        "structural_regime_proxy",
        "decomposition.structural_break_proxy",
        decomposition.get("structural_break_proxy"),
        "numeric_relative_tolerance",
        0.1,
    )
    return facts


def _append_fact(
    facts: list[dict[str, JSONValue]],
    code: str,
    source: str,
    value: JSONValue | None,
    comparison: str,
    tolerance: float,
) -> None:
    if value in (None, {}, []):
        return
    facts.append(
        {
            "code": code,
            "source": source,
            "comparison": comparison,
            "tolerance": tolerance,
            "value": value,
        }
    )


def _source_artifact_constraints(
    fingerprint: Mapping[str, JSONValue],
    training: Mapping[str, JSONValue],
) -> list[dict[str, JSONValue]]:
    source = _mapping(fingerprint.get("source"))
    coverage = _mapping(fingerprint.get("coverage"))
    topology = _mapping(fingerprint.get("temporal_topology"))
    calendar = _mapping(fingerprint.get("calendar_regimes"))
    dynamics = _mapping(fingerprint.get("microstructure_dynamics"))
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    artifacts = [
        _artifact(
            "parameterize_source_provenance",
            "source",
            _selected(source, ("kind", "cache_source")),
        ),
        _artifact(
            "parameterize_source_coverage",
            "coverage",
            _selected(
                coverage,
                (
                    "row_count",
                    "parsed_row_count",
                    "start_timestamp_utc_ms",
                    "end_timestamp_utc_ms",
                ),
            ),
        ),
        _artifact(
            "parameterize_gap_topology",
            "temporal_topology",
            _selected(
                topology,
                (
                    "computed_from",
                    "cache_source",
                    "sampling_basis",
                    "expected_session_closure_count",
                    "suspicious_gap_count",
                    "weekend_activity_count",
                ),
            ),
        ),
        _artifact(
            "parameterize_calendar_policy",
            "calendar_regimes.calendar_policy.calendar_profile",
            _mapping(calendar.get("calendar_policy")).get("calendar_profile"),
        ),
        _artifact(
            "parameterize_dynamics_limitations",
            "microstructure_dynamics.limitations",
            dynamics.get("limitations"),
        ),
        _artifact(
            "parameterize_stationarity_limitations",
            "stationarity_diagnostics",
            {
                "limitations": stationarity.get("limitations"),
                "zero_variance_metrics": stationarity.get(
                    "zero_variance_metrics"
                ),
                "skipped_window_reason_counts": stationarity.get(
                    "skipped_window_reason_counts"
                ),
            },
        ),
        _artifact(
            "parameterize_training_substrate",
            "training_substrate",
            _selected(
                training,
                (
                    "status",
                    "training_schema_version",
                    "cache_was_enriched",
                    "legacy_cache_enriched_on_read",
                    "source_rows_enriched_in_memory",
                    "row_count",
                ),
            ),
        ),
        _artifact(
            "defer_raw_m1_ohlc_constraints",
            "protocol_scope",
            {
                "non_tick_input_constraints_supported": False,
                "reason": "ascii_tick_only_protocol",
            },
        ),
    ]
    return [
        artifact
        for artifact in artifacts
        if artifact["value"] not in (None, {}, [])
    ]


def _artifact(
    code: str, source: str, value: JSONValue | None
) -> dict[str, JSONValue]:
    return {"code": code, "source": source, "value": value}


def _constraint_hints(
    fingerprint: Mapping[str, JSONValue],
    defects: list[dict[str, JSONValue]],
    stylized: list[dict[str, JSONValue]],
) -> list[str]:
    topology = _mapping(fingerprint.get("temporal_topology"))
    stationarity = _mapping(fingerprint.get("stationarity_diagnostics"))
    hints = [
        "preserve_session_activity_mix",
        "preserve_spread_regime_mix",
        "parameterize_gap_topology",
        "parameterize_cache_provenance",
        "write_only_synth_columns",
        "preserve_observed_bid_ask",
        "preserve_row_identity",
    ]
    if _int(topology.get("expected_session_closure_count")) > 0:
        hints.append("preserve_expected_weekend_closures")
    if (
        _int(topology.get("suspicious_gap_count")) > 0
        or _text(topology.get("sampling_basis")) != "observed_sequence"
    ):
        hints.append("do_not_train_on_irregular_grid_without_policy")
    if _strings(stationarity.get("recommended_transforms")):
        hints.append("apply_stationarity_transform_policy")
    hints.extend(
        _text(defect.get("code"))
        for defect in defects
        if _text(defect.get("code"))
    )
    if not stylized:
        hints.append("do_not_generate_without_reference_stylized_facts")
    return _ordered_unique(hints)


def _constraint_limitations(
    fingerprint: Mapping[str, JSONValue],
    training: Mapping[str, JSONValue],
    *,
    supported: bool,
) -> list[str]:
    limitations: list[str] = []
    if not supported:
        limitations.append("unsupported_base_grain")
    if training.get("status") != "available":
        limitations.append("training_substrate_unavailable")
    audit = _mapping(fingerprint.get("fingerprint_audit"))
    statuses = _mapping(audit.get("section_statuses"))
    if any(_text(value) == "unavailable" for value in statuses.values()):
        limitations.append("fingerprint_sections_unavailable")
    if not _mapping(fingerprint.get("stationarity_diagnostics")):
        limitations.append("stationarity_diagnostics_unavailable")
    if not _mapping(fingerprint.get("microstructure_dynamics")):
        limitations.append("microstructure_dynamics_unavailable")
    return _ordered_unique(limitations)


def _constraints_by_axis(
    findings: Iterable[QualityFinding],
) -> dict[tuple[str, str, str, str], Mapping[str, JSONValue]]:
    rows: dict[tuple[str, str, str, str], Mapping[str, JSONValue]] = {}
    for finding in findings:
        fingerprint = _mapping(
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        )
        constraints = _mapping(fingerprint.get("synthetic_constraints"))
        if not constraints:
            continue
        axis = _mapping(constraints.get("target_axis"))
        rows[_axis_key(axis)] = constraints
    return rows


def _validate_target_constraints(
    reference: Mapping[str, JSONValue],
    candidate: Mapping[str, JSONValue] | None,
    *,
    mismatch_state: BoundedReportLimit,
) -> dict[str, JSONValue]:
    axis = dict(_mapping(reference.get("target_axis")))
    if not candidate:
        code = "synthetic_candidate_target_missing"
        detail: dict[str, JSONValue] = {
            "code": code,
            "category": "target",
            "constraint_code": "target_axis",
        }
        included_codes = mismatch_state.slice([code])
        included_details = mismatch_state.slice([detail])
        return {
            "target_axis": axis,
            "status": "not_compared",
            "reason": "candidate_target_missing",
            "compared_fact_count": 0,
            "mismatch_count": 1,
            "mismatch_code_count": 1,
            "included_mismatch_code_count": len(included_codes),
            "omitted_mismatch_code_count": 1 - len(included_codes),
            "included_mismatch_count": len(included_details),
            "omitted_mismatch_count": 1 - len(included_details),
            "truncated": not included_codes or not included_details,
            "mismatch_code_counts": {code: 1},
            "mismatch_codes": cast(JSONValue, included_codes),
            "mismatch_details": cast(JSONValue, included_details),
        }
    mismatches: list[dict[str, JSONValue]] = []
    for defect in _mapping_rows(candidate.get("defects_to_avoid")):
        if (
            _text(defect.get("requirement"))
            in {"must_be_zero", "canonical_training_schema_required"}
            and _int(defect.get("observed_count")) > 0
        ):
            code = _text(defect.get("code"))
            mismatches.append(
                {
                    "code": f"synthetic_candidate_{code}_present",
                    "category": "defect",
                    "constraint_code": code,
                    "observed_count": _int(defect.get("observed_count")),
                }
            )
    reference_facts = {
        _text(item.get("code")): item
        for item in _mapping_rows(reference.get("stylized_facts_to_preserve"))
    }
    candidate_facts = {
        _text(item.get("code")): item
        for item in _mapping_rows(candidate.get("stylized_facts_to_preserve"))
    }
    compared_fact_count = 0
    for code, fact in sorted(reference_facts.items()):
        candidate_fact = candidate_facts.get(code)
        if candidate_fact is None:
            mismatches.append(
                {
                    "code": "synthetic_candidate_stylized_fact_missing",
                    "category": "stylized_fact",
                    "constraint_code": code,
                }
            )
            continue
        compared_fact_count += 1
        if not _fact_matches(fact, candidate_fact):
            mismatches.append(
                {
                    "code": f"synthetic_candidate_{code}_mismatch",
                    "category": "stylized_fact",
                    "constraint_code": code,
                    "comparison": fact.get("comparison"),
                    "tolerance": fact.get("tolerance"),
                }
            )
    reference_output = _mapping(reference.get("output_contract"))
    candidate_output = _mapping(candidate.get("output_contract"))
    for field, mismatch_code in (
        (
            "durable_identity_columns",
            "synthetic_candidate_training_identity_mismatch",
        ),
        (
            "synthetic_output_columns",
            "synthetic_candidate_output_contract_mismatch",
        ),
        (
            "observed_columns_preserved",
            "synthetic_candidate_observed_column_contract_mismatch",
        ),
    ):
        if reference_output.get(field) != candidate_output.get(field):
            mismatches.append(
                {
                    "code": mismatch_code,
                    "category": "output_contract",
                    "constraint_code": field,
                }
            )
    mismatch_codes = sorted(
        {_text(item.get("code")) for item in mismatches if item.get("code")}
    )
    included_details = mismatch_state.slice(mismatches)
    included_codes = mismatch_state.slice(mismatch_codes)
    code_counts = Counter(
        _text(item.get("code")) for item in mismatches if item.get("code")
    )
    return {
        "target_axis": axis,
        "status": "mismatch" if mismatches else "match",
        "compared_fact_count": compared_fact_count,
        "mismatch_count": len(mismatches),
        "mismatch_code_count": len(mismatch_codes),
        "included_mismatch_code_count": len(included_codes),
        "omitted_mismatch_code_count": max(
            0, len(mismatch_codes) - len(included_codes)
        ),
        "included_mismatch_count": len(included_details),
        "omitted_mismatch_count": max(
            0, len(mismatches) - len(included_details)
        ),
        "truncated": (
            len(included_details) < len(mismatches)
            or len(included_codes) < len(mismatch_codes)
        ),
        "mismatch_code_counts": _counter_payload(code_counts),
        "mismatch_codes": cast(JSONValue, included_codes),
        "mismatch_details": cast(JSONValue, included_details),
    }


def _fact_matches(
    reference: Mapping[str, JSONValue],
    candidate: Mapping[str, JSONValue],
) -> bool:
    comparison = _text(reference.get("comparison"))
    tolerance = _float(reference.get("tolerance"))
    left = reference.get("value")
    right = candidate.get("value")
    if comparison == "exact":
        return left == right
    if comparison == "distribution_l1":
        return _distribution_distance(left, right) <= tolerance
    if comparison == "numeric_absolute_tolerance":
        return _numeric_values_match(left, right, tolerance, relative=False)
    if comparison == "numeric_relative_tolerance":
        return _numeric_values_match(left, right, tolerance, relative=True)
    return left == right


def _distribution_distance(
    left: JSONValue | None, right: JSONValue | None
) -> float:
    left_map = _numeric_leaf_map(left)
    right_map = _numeric_leaf_map(right)
    if not left_map and not right_map:
        return 0.0 if left == right else math.inf
    keys = set(left_map) | set(right_map)
    left_total = sum(max(0.0, left_map.get(key, 0.0)) for key in keys)
    right_total = sum(max(0.0, right_map.get(key, 0.0)) for key in keys)
    if left_total <= 0.0 or right_total <= 0.0:
        return 0.0 if left_map == right_map else math.inf
    return (
        sum(
            abs(
                left_map.get(key, 0.0) / left_total
                - right_map.get(key, 0.0) / right_total
            )
            for key in keys
        )
        / 2.0
    )


def _numeric_values_match(
    left: JSONValue | None,
    right: JSONValue | None,
    tolerance: float,
    *,
    relative: bool,
) -> bool:
    left_map = _leaf_map(left)
    right_map = _leaf_map(right)
    if set(left_map) != set(right_map):
        return False
    for key, left_value in left_map.items():
        right_value = right_map[key]
        if isinstance(left_value, bool) or isinstance(right_value, bool):
            if left_value != right_value:
                return False
            continue
        if isinstance(left_value, (int, float)) and isinstance(
            right_value, (int, float)
        ):
            delta = abs(float(left_value) - float(right_value))
            allowed = tolerance
            if relative:
                allowed = max(1e-12, abs(float(left_value)) * tolerance)
            if delta > allowed:
                return False
            continue
        if left_value != right_value:
            return False
    return True


def _numeric_leaf_map(value: JSONValue | None) -> dict[str, float]:
    return {
        key: float(item)
        for key, item in _leaf_map(value).items()
        if not isinstance(item, bool) and isinstance(item, (int, float))
    }


def _leaf_map(value: JSONValue | None, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, Mapping):
        rows: dict[str, Any] = {}
        for key, item in sorted(value.items()):
            path = f"{prefix}.{key}" if prefix else str(key)
            rows.update(_leaf_map(item, path))
        return rows
    if isinstance(value, list):
        rows = {}
        for index, item in enumerate(value):
            path = f"{prefix}[{index}]"
            rows.update(_leaf_map(item, path))
        return rows
    return {prefix or "value": value}


def _frame_true_count(frame: Any | None, column: str) -> int | None:
    if frame is None or column not in getattr(frame, "columns", ()):
        return None
    try:
        return int(frame.get_column(column).fill_null(False).sum() or 0)
    except (AttributeError, TypeError, ValueError):
        return None


def _training_precision_regime(frame: Any | None) -> dict[str, JSONValue]:
    if frame is None:
        return {}
    counts: Counter[str] = Counter()
    for column in ("bid", "ask"):
        if column not in getattr(frame, "columns", ()):
            continue
        try:
            values = frame.get_column(column).to_list()
        except (AttributeError, KeyError, TypeError, ValueError):
            continue
        for value in values:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            if not math.isfinite(float(value)):
                continue
            text = f"{float(value):.12f}".rstrip("0").rstrip(".")
            places = len(text.split(".", 1)[1]) if "." in text else 0
            counts[f"{column}:{places}"] += 1
    if not counts:
        return {}
    return {
        "basis": "observed_float_value",
        "decimal_place_counts": _counter_payload(counts),
        "source_text_precision_preserved": False,
    }


def _selected(
    payload: Mapping[str, JSONValue],
    fields: tuple[str, ...],
) -> dict[str, JSONValue]:
    return {field: payload[field] for field in fields if field in payload}


def _without_count(payload: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
    return {key: value for key, value in payload.items() if key != "count"}


def _payload_id(payload: Mapping[str, JSONValue]) -> str:
    import hashlib

    material = {
        key: value for key, value in payload.items() if key != "constraint_id"
    }
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _axis_key(axis: Mapping[str, JSONValue]) -> tuple[str, str, str, str]:
    return (
        _text(axis.get("data_format")),
        _text(axis.get("timeframe")),
        _text(axis.get("symbol")),
        _text(axis.get("period")),
    )


def _target_axis_from_target(target: Any | None) -> dict[str, JSONValue]:
    return {
        "data_format": _text(getattr(target, "data_format", "")) or "ascii",
        "timeframe": _text(getattr(target, "timeframe", "")) or TICK,
        "symbol": _text(getattr(target, "symbol", "")),
        "period": _text(getattr(target, "period", "")),
        "kind": _text(getattr(getattr(target, "kind", None), "value", ""))
        or "cache",
    }


def _target_sort_key(target: Mapping[str, JSONValue]) -> tuple[str, ...]:
    return _axis_key(_mapping(target.get("target_axis")))


def _validation_target_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[int, str, str, str, str]:
    ranks = {"mismatch": 0, "not_compared": 1, "match": 2}
    axis = _mapping(target.get("target_axis"))
    return (ranks.get(_text(target.get("status")), 99), *_axis_key(axis))


def _counter_payload(counter: Counter[str]) -> dict[str, JSONValue]:
    return {key: counter[key] for key in sorted(counter) if key}


def _ordered_unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    rows: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return rows


def _mapping(value: object) -> Mapping[str, JSONValue]:
    return value if isinstance(value, Mapping) else {}


def _mapping_rows(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return int(value)
    return 0


def _float(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return 0.0


def _format_counts(counts: Mapping[str, JSONValue]) -> str:
    return ", ".join(
        f"{key}={_int(value)}" for key, value in sorted(counts.items())
    )


def _format_axis(axis: Mapping[str, JSONValue]) -> str:
    return ":".join(
        (
            _text(axis.get("data_format")) or "unknown",
            _text(axis.get("timeframe")) or "unknown",
            _text(axis.get("symbol")) or "unknown",
            _text(axis.get("period")) or "unknown",
        )
    )

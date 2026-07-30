"""Tests for generator-facing synthetic fingerprint constraints."""

from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path

import polars as pl

from histdatacom.data_quality import (
    SYNTHETIC_CONSTRAINTS_SCHEMA_VERSION,
    SYNTHETIC_CONSTRAINT_SUMMARY_SCHEMA_VERSION,
    SYNTHETIC_VALIDATION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    synthetic_constraint_summary,
    synthetic_constraints_from_fingerprint,
    synthetic_constraints_from_training_frame,
    validate_synthetic_constraint_reports,
)
from histdatacom.data_quality.training_features import (
    SYNTHETIC_PLACEHOLDER_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    ensure_tick_training_features,
)
from histdatacom.histdata_ascii import (
    TICK,
    format_influx_line,
    parse_ascii_lines,
    to_polars_frame,
)
from tests.fixtures.histdata_ascii.quality_cases import CLEAN_TICK_ROWS


def test_constraints_derive_categories_from_enriched_tick_rows() -> None:
    """The protocol should expose bounded row-first generator contracts."""
    target = _target()
    frame = ensure_tick_training_features(_clean_frame(), target=target)
    fingerprint = _fingerprint_stub()

    first = synthetic_constraints_from_training_frame(
        frame,
        fingerprint=fingerprint,
        target=target,
    )
    second = synthetic_constraints_from_training_frame(
        frame,
        fingerprint=fingerprint,
        target=target,
    )

    assert first["schema_version"] == SYNTHETIC_CONSTRAINTS_SCHEMA_VERSION
    assert first["status"] == "ready"
    assert first["constraint_id"] == second["constraint_id"]
    training = _mapping(first["training_substrate"])
    assert training["training_schema_version"] == TRAINING_SCHEMA_VERSION
    assert training["identity_columns_present"] is True
    assert training["synthetic_output_columns_present"] is True
    output = _mapping(first["output_contract"])
    assert output["observed_columns_preserved"] == ["bid", "ask"]
    assert output["synthetic_output_columns"] == list(
        SYNTHETIC_PLACEHOLDER_COLUMNS
    )
    assert output["durable_identity_columns"] == [
        "series_id",
        "period",
        "row_id",
    ]
    assert output["timestamp_is_sole_identity"] is False
    assert output["non_tick_input_constraints_supported"] is False
    assert output["generation_in_scope"] is True
    assert output["generation_issue"] == "#81"
    assert output["generation_method"] == "empirical_block_bootstrap"
    assert {
        item["code"] for item in _mapping_rows(first["defects_to_avoid"])
    } >= {
        "avoid_negative_spread",
        "avoid_duplicate_timestamps",
        "avoid_non_monotonic_timestamps",
        "avoid_suspicious_non_session_gaps",
        "avoid_invalid_rows",
        "avoid_partial_rows",
        "avoid_topology_unavailable",
        "avoid_fingerprint_unready",
        "avoid_unsupported_schema",
        "avoid_structurally_invalid_timestamps",
    }
    assert {
        item["code"]
        for item in _mapping_rows(first["stylized_facts_to_preserve"])
    } >= {
        "session_activity_mix",
        "gap_bucket_shape",
        "spread_distribution",
        "precision_regime",
        "stale_quote_runs",
        "volatility_clustering_proxy",
        "rolling_drift",
        "stationarity_transform_policy",
    }
    assert "write_only_synth_columns" in first["advisory_hints"]
    assert "preserve_row_identity" in first["advisory_hints"]
    assert "raw_m1_ohlc_constraints_deferred" not in first["limitation_codes"]
    assert "bidquote" not in json.dumps(first, sort_keys=True)


def test_constraints_use_explicit_row_issue_columns_for_defects() -> None:
    """Row issue columns should drive generator defect observations."""
    rows = (
        "20120201 000000000,1.200000,1.100000,0",
        "20120201 000000000,1.200000,1.100000,0",
        "20120131 235959000,1.200000,1.200200,0",
    )
    target = _target()
    frame = ensure_tick_training_features(
        to_polars_frame(parse_ascii_lines(TICK, rows)),
        target=target,
    )
    assert frame.get_column("row_id").to_list() == [1, 2, 3]
    assert frame.get_column("timestamp_utc_ms").n_unique() == 2

    payload = synthetic_constraints_from_fingerprint(
        _fingerprint_stub(),
        training_frame=frame,
        target=target,
    )
    defects = {
        item["code"]: item
        for item in _mapping_rows(payload["defects_to_avoid"])
    }

    assert defects["avoid_negative_spread"]["observed_count"] == 2
    assert defects["avoid_duplicate_timestamps"]["observed_count"] == 2
    assert defects["avoid_non_monotonic_timestamps"]["observed_count"] == 1
    assert all(
        item["source"] == "training_feature_column"
        for item in defects.values()
        if item.get("issue_column")
    )


def test_constraints_are_bounded_and_publish_safe(tmp_path: Path) -> None:
    """Constraint categories should truncate without publishing local paths."""
    target = _target(path=str(tmp_path / "secret" / ".data"))
    payload = synthetic_constraints_from_fingerprint(
        _fingerprint_stub(),
        training_frame=_clean_frame(),
        target=target,
        category_limit=1,
        hint_limit=1,
    )

    assert payload["included_defect_count"] == 1
    assert payload["included_stylized_fact_count"] == 1
    assert payload["included_source_artifact_count"] == 1
    assert payload["included_hint_count"] == 1
    assert payload["truncated"] is True
    assert str(tmp_path) not in json.dumps(payload, sort_keys=True)


def test_validation_matches_and_reports_stable_drift_codes() -> None:
    """Candidate fingerprint drift should yield deterministic mismatch codes."""
    reference_constraints = synthetic_constraints_from_fingerprint(
        _fingerprint_stub(),
        training_frame=_clean_frame(),
        target=_target(),
    )
    matching = _report(reference_constraints)

    matched = validate_synthetic_constraint_reports(matching, matching)

    assert matched["schema_version"] == SYNTHETIC_VALIDATION_SCHEMA_VERSION
    assert matched["status"] == "match"
    assert matched["matching_target_count"] == 1

    candidate_constraints = deepcopy(reference_constraints)
    defects = _mapping_rows(candidate_constraints["defects_to_avoid"])
    defects[0]["observed_count"] = 2
    facts = _mapping_rows(candidate_constraints["stylized_facts_to_preserve"])
    spread = next(
        item for item in facts if item["code"] == "spread_distribution"
    )
    _mapping(_mapping(spread["value"])["quantiles"])["0.5"] = 0.01
    mismatched = validate_synthetic_constraint_reports(
        matching,
        _report(candidate_constraints),
    )
    codes = set(
        _mapping_rows(mismatched["target_results"])[0]["mismatch_codes"]
    )

    assert mismatched["status"] == "mismatch"
    assert "synthetic_candidate_avoid_negative_spread_present" in codes
    assert "synthetic_candidate_spread_distribution_mismatch" in codes


def test_validation_reports_missing_candidate_and_bounds_targets() -> None:
    """Missing candidate axes and target limits should remain explicit."""
    constraints = synthetic_constraints_from_fingerprint(
        _fingerprint_stub(),
        training_frame=_clean_frame(),
        target=_target(),
    )

    payload = validate_synthetic_constraint_reports(
        _report(constraints),
        QualityReport(),
        target_limit=0,
        mismatch_limit=0,
    )

    assert payload["status"] == "not_compared"
    assert payload["not_compared_target_count"] == 1
    assert payload["included_target_count"] == 0
    assert payload["omitted_target_count"] == 1
    assert payload["truncated"] is True


def test_constraint_summary_is_bounded_and_issue_visible() -> None:
    """Reports should expose constraints without nested finding inspection."""
    constraints = synthetic_constraints_from_fingerprint(
        _fingerprint_stub(),
        training_frame=_clean_frame(),
        target=_target(),
    )
    defects = _mapping_rows(constraints["defects_to_avoid"])
    defects[0]["observed_count"] = 1

    summary = synthetic_constraint_summary(
        _report(constraints).findings,
        target_limit=1,
    )

    assert summary is not None
    assert summary["schema_version"] == (
        SYNTHETIC_CONSTRAINT_SUMMARY_SCHEMA_VERSION
    )
    assert summary["target_count"] == 1
    assert summary["observed_defect_target_counts"] == {
        "avoid_negative_spread": 1
    }


def test_populated_synthetic_columns_share_the_enriched_influx_point() -> None:
    """Influx projection should not regress to market-only candidate points."""
    target = _target()
    frame = ensure_tick_training_features(_clean_frame(), target=target)
    frame = frame.with_columns(
        [
            pl.col("bid").add(0.01).alias("synth_bid"),
            pl.col("ask").add(0.01).alias("synth_ask"),
            pl.col("spread").alias("synth_spread"),
            pl.col("mid").add(0.01).alias("synth_mid"),
            pl.lit(1).cast(pl.Int32).alias("synth_method_code"),
            pl.lit(0.9).alias("synth_confidence"),
            pl.lit(True).alias("synth_usable"),
        ]
    )
    line = format_influx_line(
        "EURUSD",
        "ascii",
        TICK,
        frame.row(0),
        columns=frame.columns,
    )

    assert frame.get_column("bid")[0] == 1.3066
    assert frame.get_column("ask")[0] == 1.30677
    assert "bidquote=1.3066" in line
    assert "askquote=1.30677" in line
    assert "synth_bid=1.3166" in line
    assert "synth_ask=1.31677" in line
    assert "synth_method_code=1i" in line
    assert "synth_usable=true" in line
    assert line.count(" ") == 2


def test_synthetic_validation_matches_golden_fixture() -> None:
    """Topology and defect validation output should remain golden-testable."""
    reference_constraints = synthetic_constraints_from_fingerprint(
        _fingerprint_stub(),
        training_frame=_clean_frame(),
        target=_target(),
    )
    candidate_constraints = deepcopy(reference_constraints)
    defects = _mapping_rows(candidate_constraints["defects_to_avoid"])
    next(
        item for item in defects if item["code"] == "avoid_duplicate_timestamps"
    )["observed_count"] = 2
    facts = _mapping_rows(candidate_constraints["stylized_facts_to_preserve"])
    gap_shape = next(
        item for item in facts if item["code"] == "gap_bucket_shape"
    )
    _mapping(gap_shape["value"])["gt_5m"] = 3
    payload = validate_synthetic_constraint_reports(
        _report(reference_constraints),
        _report(candidate_constraints),
    )
    expected = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "data_quality_reports"
        / "synthetic_validation.json"
    )
    if os.environ.get("HISTDATACOM_UPDATE_QUALITY_GOLDENS") == "1":
        fixture.write_text(expected, encoding="utf-8")

    assert fixture.read_text(encoding="utf-8") == expected


def _clean_frame() -> pl.DataFrame:
    return to_polars_frame(parse_ascii_lines(TICK, CLEAN_TICK_ROWS))


def _target(path: str = ".data") -> QualityTarget:
    return QualityTarget(
        path=path,
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )


def _fingerprint_stub() -> dict:
    return {
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": "EURUSD",
            "period": "201202",
            "kind": "cache",
        },
        "source": {"kind": "cache", "cache_source": "direct", "path": ".data"},
        "coverage": {
            "row_count": 3,
            "parsed_row_count": 3,
            "start_timestamp_utc_ms": 1,
            "end_timestamp_utc_ms": 3,
        },
        "temporal_topology": {
            "computed_from": "direct_cache",
            "cache_source": "direct",
            "sampling_basis": "observed_sequence",
            "invalid_timestamp_count": 0,
            "duplicate_timestamp_count": 0,
            "non_monotonic_count": 0,
            "suspicious_gap_count": 0,
            "expected_session_closure_count": 0,
            "weekend_activity_count": 0,
            "min_interval_ms": 313,
            "median_interval_ms": 5665,
            "max_gap_ms": 11017,
            "gap_bucket_counts": {"gt_1m": 0, "gt_5m": 0},
        },
        "calendar_regimes": {
            "session_state_counts": {"market_open": 3},
            "active_session_counts": {"asia": 3},
            "special_tag_counts": {},
            "calendar_policy": {
                "calendar_profile": {
                    "name": "test-profile",
                    "complete": True,
                }
            },
        },
        "tick_distribution": {
            "negative_spread_count": 0,
            "invalid_row_count": 0,
            "partial_row_count": 0,
            "spread": {
                "count": 3,
                "min": 0.00017,
                "max": 0.00017,
                "mean": 0.00017,
                "median": 0.00017,
                "mad": 0.0,
                "quantiles": {"0.5": 0.00017},
            },
        },
        "microstructure_dynamics": {
            "spread_jump": {"count": 0, "rate": 0.0},
            "stale_quote": {"run_count": 0, "repeat_rate": 0.0},
            "burst": {"run_count": 0, "burst_rate": 0.0},
            "one_sided_movement": {"count": 0, "rate": 0.0},
            "limitations": [],
        },
        "dependence": {"absolute_spread_change_acf": {"lag_acf": {"1": 0.2}}},
        "stationarity_diagnostics": {
            "first_middle_last_distribution_shift": {
                "return": {"status": "computed", "median_shift": 0.0}
            },
            "rolling_windows": {
                "2": {"status": "computed", "absolute_change": 0.0}
            },
            "recommended_transforms": ["log_return"],
            "limitations": [],
            "zero_variance_metrics": [],
            "skipped_window_reason_counts": {},
        },
        "decomposition": {
            "structural_break_proxy": {
                "status": "computed",
                "candidate_count": 0,
            }
        },
        "fingerprint_audit": {
            "section_statuses": {
                "coverage": "valid",
                "temporal_topology": "valid",
                "calendar_regimes": "valid",
                "tick_distribution": "valid",
                "microstructure_dynamics": "valid",
                "dependence": "valid",
                "stationarity_diagnostics": "valid",
                "decomposition": "valid",
            }
        },
    }


def _report(constraints: dict) -> QualityReport:
    target = _target()
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Synthetic constraint test fingerprint.",
        rule_id="fingerprint.series",
        target=target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "target_axis": constraints["target_axis"],
                "synthetic_constraints": constraints,
            }
        },
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="fingerprint.series",
                target=target,
                findings=(finding,),
            ),
        ),
    )


def _mapping(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _mapping_rows(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]

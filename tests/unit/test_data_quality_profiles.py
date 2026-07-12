"""Tests for operator-configurable data-quality profiles."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from histdatacom.data_quality import (
    DEFAULT_QUALITY_PROFILE_SOURCE,
    QUALITY_PROFILE_SCHEMA_VERSION,
    QUALITY_REPORTING_METADATA_KEY,
    SERIES_FINGERPRINT_RULE_ID,
    AutoregressiveProfile,
    ClassicalModelInputProfile,
    ExponentialSmoothingProfile,
    HistDataSeriesFingerprintRule,
    QualityFinding,
    QualityProfileError,
    QualityReport,
    QualityStatus,
    SeasonalExogenousProfile,
    StateSpaceProfile,
    apply_quality_profile_overrides,
    discover_quality_targets,
    load_quality_profile_file,
    load_quality_profile_file_resolution,
    quality_profile_report_metadata,
    quality_rules_for_groups,
    resolve_quality_profile,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import TICK
from tests.fixtures.histdata_ascii.quality_cases import (
    HistDataAsciiCase,
    write_ascii_case,
)


def test_default_profile_keeps_profile_metadata_defaults() -> None:
    """No profile should preserve deterministic profile metadata defaults."""
    metadata = quality_profile_report_metadata(None)["quality_profile"]
    assert metadata["source"] == DEFAULT_QUALITY_PROFILE_SOURCE
    assert metadata["is_default"] is True


def test_profile_symbol_session_microstructure_override(
    tmp_path: Path,
) -> None:
    """Symbol/session tick profiles should select the configured threshold."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_one_sided_profiled_ok",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_ONE_SIDED_OK.csv",
            rows=(
                "20120201 000003660,1.306600,1.306770,0",
                "20120201 000004660,1.306610,1.306770,25",
                "20120201 000005660,1.306620,1.306770,25",
            ),
        ),
    )
    profile = {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "rollover-symbol-profile",
        "rules": {
            "ticks.ascii.microstructure": {
                "session_name": "rollover",
                "thresholds_by_symbol_session": {
                    "EURUSD:rollover": {"one_sided_run_length": 3}
                },
            }
        },
    }

    report = _report_for_path(
        path,
        groups=("ticks",),
        profile=profile,
    )

    summary = _finding(report, "ASCII_TICK_MICROSTRUCTURE_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["one_sided_movement_count"] == 2
    assert summary.metadata["one_sided_run_count"] == 0
    assert summary.metadata["threshold_profile"]["source"] == "symbol-session"
    assert summary.metadata["threshold_profile"]["profile_key"] == (
        "EURUSD:rollover"
    )


def test_profile_modeling_assumptions_are_reported_in_metadata(
    tmp_path: Path,
) -> None:
    """Report metadata should identify operator profile provenance."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="tick_profiled_modeling_assumptions",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202.csv",
            rows=("20120201 000003660,1.306600,1.306770,0",),
        ),
    )
    profile = {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "strict-ci",
        "modeling_assumptions": {
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "target_horizon_minutes": 5,
        },
    }

    report = _report_for_path(
        path,
        groups=("modeling",),
        profile=profile,
    )

    profile_metadata = report.metadata["quality_profile"]
    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert profile_metadata["name"] == "strict-ci"
    assert profile_metadata["configured_modeling_assumption_keys"] == [
        "ask_side_execution_model",
        "current_bar_action_timing",
        "spread_cost_model",
        "target_horizon_minutes",
    ]
    assert summary.metadata["target_horizon"]["status"] == "feasible"


def test_profile_reporting_enables_remediation_catalog_audit_metadata() -> None:
    """Reporting profile switches should flow into report metadata."""
    metadata = quality_profile_report_metadata(
        {
            "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
            "name": "catalog-audit-profile",
            "reporting": {
                "remediation_catalog_audit": {
                    "enabled": True,
                }
            },
        }
    )

    profile_metadata = metadata["quality_profile"]

    assert metadata[QUALITY_REPORTING_METADATA_KEY] == {
        "remediation_catalog_audit": {"enabled": True}
    }
    assert profile_metadata["is_default"] is False
    assert profile_metadata["configured_reporting_keys"] == [
        "remediation_catalog_audit"
    ]
    assert profile_metadata["reporting"] == {
        "remediation_catalog_audit": {"enabled": True}
    }


def test_profile_fingerprint_knobs_flow_to_rule_surface() -> None:
    """Fingerprint controls should validate and flow through rule factories."""
    rules = quality_rules_for_groups(
        ("fingerprint",),
        profile={
            "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
            "name": "fingerprint-profile",
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "quantiles": [0.1, 0.5, 0.9],
                    "lags": [1, 5, 30],
                    "rolling_windows": [60, 240],
                    "histogram_bins": 16,
                    "max_rows": 1000,
                    "rounding_digits": 8,
                    "topology_inspection_sample_limit": 2,
                    "distribution_attention": {
                        "invalid_row_min_count": 2,
                        "invalid_row_min_rate": 0.5,
                        "zero_spread_min_count": 3,
                        "zero_spread_min_rate": 0.25,
                        "negative_spread_min_count": 4,
                        "negative_spread_min_rate": 0.1,
                        "flag_truncated_distribution": False,
                        "flag_cache_float_precision": False,
                    },
                    "cache_source_parity": {
                        "enabled": True,
                        "mismatch_limit": 7,
                    },
                    "classical_baselines": {
                        "enabled": True,
                        "evaluation_fraction": 0.25,
                        "minimum_training_rows": 12,
                        "minimum_evaluation_rows": 4,
                        "rolling_windows": [3, 9],
                        "session_seasonal_enabled": False,
                        "rounding_digits": 6,
                    },
                }
            },
        },
    )

    assert len(rules) == 1
    assert isinstance(rules[0], HistDataSeriesFingerprintRule)
    assert rules[0].profile.to_metadata() == {
        "quantiles": [0.1, 0.5, 0.9],
        "lags": [1, 5, 30],
        "rolling_windows": [60, 240],
        "histogram_bins": 16,
        "max_rows": 1000,
        "rounding_digits": 8,
        "topology_inspection_sample_limit": 2,
        "distribution_attention": {
            "invalid_row_min_count": 2,
            "invalid_row_min_rate": 0.5,
            "zero_spread_min_count": 3,
            "zero_spread_min_rate": 0.25,
            "negative_spread_min_count": 4,
            "negative_spread_min_rate": 0.1,
            "flag_truncated_distribution": False,
            "flag_cache_float_precision": False,
        },
        "cache_source_parity": {
            "enabled": True,
            "mismatch_limit": 7,
        },
        "classical_baselines": {
            "enabled": True,
            "evaluation_fraction": 0.25,
            "minimum_training_rows": 12,
            "minimum_evaluation_rows": 4,
            "rolling_windows": [3, 9],
            "session_seasonal_enabled": False,
            "rounding_digits": 6,
        },
        "classical_model_input": ClassicalModelInputProfile().to_metadata(),
        "exponential_smoothing": ExponentialSmoothingProfile().to_metadata(),
        "autoregressive": AutoregressiveProfile().to_metadata(),
        "seasonal_exogenous": SeasonalExogenousProfile().to_metadata(),
        "state_space": StateSpaceProfile().to_metadata(),
    }


def test_classical_model_input_profile_flows_to_rule_surface() -> None:
    """Regularization, fold, transform, and resource controls should parse."""
    rules = quality_rules_for_groups(
        ("fingerprint",),
        profile={
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "classical_model_input": {
                        "enabled": True,
                        "frequency_ms": 1_000,
                        "midpoint_aggregation": "median",
                        "spread_aggregation": "mean",
                        "transform": "log_return",
                        "horizons": [1, 3],
                        "fold_kind": "rolling",
                        "minimum_training_observations": 10,
                        "minimum_evaluation_observations": 2,
                        "rolling_window": 12,
                        "resources": {"max_folds": 8},
                    }
                }
            }
        },
    )

    model_input = rules[0].profile.classical_model_input
    assert model_input.enabled is True
    assert model_input.frequency_ms == 1_000
    assert model_input.midpoint_aggregation == "median"
    assert model_input.spread_aggregation == "mean"
    assert model_input.transform == "log_return"
    assert model_input.horizons == (1, 3)
    assert model_input.fold_kind == "rolling"
    assert model_input.rolling_window == 12
    assert model_input.resources.max_folds == 8


def test_exponential_smoothing_profile_flows_to_rule_surface() -> None:
    """Explicit fitted-family configurations should parse without search."""
    rules = quality_rules_for_groups(
        ("fingerprint",),
        profile={
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "exponential_smoothing": {
                        "enabled": True,
                        "projection_specification_id": "hw",
                        "projection_horizon": 3,
                        "baseline_rolling_windows": [3, 9],
                        "specifications": [
                            {
                                "specification_id": "hw",
                                "family": "holt_winters",
                                "trend": "add",
                                "seasonal": "mul",
                                "seasonal_periods": 12,
                                "initialization_method": "estimated",
                                "parameter_bounds": [
                                    {
                                        "parameter": "smoothing_level",
                                        "lower": 0.01,
                                        "upper": 0.99,
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        },
    )

    profile = rules[0].profile.exponential_smoothing
    assert profile.enabled is True
    assert profile.projection_specification_id == "hw"
    assert profile.projection_horizon == 3
    assert profile.baseline_rolling_windows == (3, 9)
    assert profile.specifications[0].family == "holt_winters"
    assert profile.specifications[0].seasonal == "mul"
    assert profile.specifications[0].parameter_bounds == (
        ("smoothing_level", 0.01, 0.99),
    )


@pytest.mark.parametrize(
    "profile",
    (
        {"rules": {"bad.rule": {}}},
        {
            "rules": {
                "ticks.ascii.spread": {"zero_spread_severity": "catastrophic"}
            }
        },
        {
            "rules": {
                "ticks.ascii.microstructure": {
                    "thresholds": {"stale_max_gap_ms": -1}
                }
            }
        },
        {"rules": {SERIES_FINGERPRINT_RULE_ID: {"quantiles": [0.5, 0.1]}}},
        {"rules": {SERIES_FINGERPRINT_RULE_ID: {"lags": [1, 1]}}},
        {"rules": {SERIES_FINGERPRINT_RULE_ID: {"histogram_bins": 0}}},
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "topology_inspection_sample_limit": 6
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {"distribution_attention": "loose"}
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "distribution_attention": {"invalid_row_min_count": 0}
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "distribution_attention": {"zero_spread_min_rate": 1.5}
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "distribution_attention": {
                        "flag_truncated_distribution": "yes"
                    }
                }
            }
        },
        {"rules": {SERIES_FINGERPRINT_RULE_ID: {"cache_source_parity": True}}},
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "cache_source_parity": {"mismatch_limit": -1}
                }
            }
        },
        {"rules": {SERIES_FINGERPRINT_RULE_ID: {"classical_baselines": True}}},
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "classical_baselines": {"evaluation_fraction": 1.0}
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "classical_baselines": {"minimum_training_rows": 0}
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {"classical_model_input": True}
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "classical_model_input": {"horizons": [3, 1]}
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "classical_model_input": {
                        "fold_kind": "rolling",
                        "rolling_window": 1,
                    }
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "exponential_smoothing": {"specifications": []}
                }
            }
        },
        {
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "exponential_smoothing": {
                        "specifications": [
                            {
                                "specification_id": "bad",
                                "family": "holt_winters",
                            }
                        ]
                    }
                }
            }
        },
        {"reporting": "enabled"},
        {"reporting": {"unknown": {}}},
        {"reporting": {"remediation_catalog_audit": True}},
        {"reporting": {"remediation_catalog_audit": {"enabled": "yes"}}},
        {"rules": {SERIES_FINGERPRINT_RULE_ID: {"unknown": True}}},
    ),
)
def test_invalid_profiles_fail_with_clear_errors(profile: dict) -> None:
    """Unknown rule IDs, severities, and negative thresholds should fail."""
    with pytest.raises(QualityProfileError):
        quality_rules_for_groups(("all",), profile=profile)


def test_quality_profile_file_loads_json_payload(tmp_path: Path) -> None:
    """CLI profile files should validate into request-safe payloads."""
    profile_path = tmp_path / "quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "file-profile",
                "rules": {
                    "ingestion.ascii.row_count": {
                        "min_row_count": 10,
                        "min_size_bytes": 200,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    profile = load_quality_profile_file(profile_path)

    assert profile.name == "file-profile"
    assert profile.source == "file"
    assert profile.source_path == str(profile_path)
    assert profile.row_count_profile().min_row_count == 10


def test_default_profile_resolution_attributes_every_value_to_builtin() -> None:
    """Default resolution should expose deterministic built-in provenance."""
    resolution = resolve_quality_profile()
    payload = resolution.to_payload()

    assert resolution.profile.is_default
    assert [channel["kind"] for channel in payload["input_channels"]] == [
        "built_in_default"
    ]
    assert {item["source"] for item in payload["effective_value_sources"]} == {
        "built_in_default"
    }


def test_profile_file_resolution_preserves_nested_and_yaml_selection_sources(
    tmp_path: Path,
) -> None:
    """Nested thresholds should retain file, name, and YAML selection facts."""
    profile_path = tmp_path / "quality-profile.json"
    config_path = tmp_path / "histdatacom.yaml"
    profile_path.write_text(
        json.dumps(
            {
                "name": "nested-file-profile",
                "rules": {
                    "ingestion.ascii.row_count": {
                        "min_row_count": 25,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    resolution = load_quality_profile_file_resolution(
        profile_path,
        config_path=str(config_path),
        selected_by="yaml_config",
    )
    payload = resolution.to_payload()
    sources = {
        item["path"]: item for item in payload["effective_value_sources"]
    }

    assert [channel["kind"] for channel in payload["input_channels"]] == [
        "built_in_default",
        "yaml_config",
        "named_profile",
        "profile_file",
    ]
    assert sources["/name"]["source"] == "named_profile"
    assert sources["/rules/ingestion.ascii.row_count/min_row_count"] == {
        "path": "/rules/ingestion.ascii.row_count/min_row_count",
        "value": 25,
        "source": "profile_file",
        "profile_name": "nested-file-profile",
        "source_path": str(profile_path),
        "selected_by": "yaml_config",
    }


def test_profile_resolution_override_records_previous_source_and_value() -> (
    None
):
    """Overrides should preserve what source and value they replaced."""
    resolution = resolve_quality_profile(
        {
            "name": "override-profile",
            "reporting": {"remediation_catalog_audit": {"enabled": False}},
        },
        source="file",
        source_path="quality-profile.json",
    )

    overridden = apply_quality_profile_overrides(
        resolution,
        {"reporting.remediation_catalog_audit.enabled": True},
        source="cli_override",
    )
    sources = {
        item["path"]: item
        for item in overridden.to_payload()["effective_value_sources"]
    }

    assert sources["/reporting/remediation_catalog_audit/enabled"] == {
        "path": "/reporting/remediation_catalog_audit/enabled",
        "value": True,
        "source": "cli_override",
        "profile_name": "override-profile",
        "override": True,
        "previous_source": "profile_file",
        "overridden_source": "profile_file",
        "previous_value": False,
    }


def _report_for_path(
    path: Path,
    *,
    groups: tuple[str, ...],
    profile: dict,
) -> QualityReport:
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(groups, profile=profile),
        metadata=quality_profile_report_metadata(profile),
    )


def _finding(report: QualityReport, code: str) -> QualityFinding:
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]

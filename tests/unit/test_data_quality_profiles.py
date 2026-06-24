"""Tests for operator-configurable data-quality profiles."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from histdatacom.data_quality import (
    ASSET_CLASS_METAL,
    DEFAULT_QUALITY_PROFILE_SOURCE,
    QUALITY_PROFILE_SCHEMA_VERSION,
    HistDataAsciiM1OutlierRule,
    QualityFinding,
    QualityProfileError,
    QualityReport,
    QualityStatus,
    discover_quality_targets,
    load_quality_profile_file,
    quality_profile_report_metadata,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import M1, TICK
from tests.fixtures.histdata_ascii.quality_cases import (
    HistDataAsciiCase,
    write_ascii_case,
)


def test_default_profile_keeps_rule_defaults() -> None:
    """No profile should preserve deterministic rule defaults."""
    rules = quality_rules_for_groups(("bars",))
    outlier_rule = next(
        rule for rule in rules if rule.rule_id == "bars.ascii.m1_outliers"
    )

    assert isinstance(outlier_rule, HistDataAsciiM1OutlierRule)
    assert outlier_rule.thresholds.max_open_jump_ratio == 0.005
    assert ASSET_CLASS_METAL in outlier_rule.thresholds_by_asset_class
    assert (
        outlier_rule.thresholds_by_asset_class[
            ASSET_CLASS_METAL
        ].max_open_jump_ratio
        == 0.03
    )
    metadata = quality_profile_report_metadata(None)["quality_profile"]
    assert metadata["source"] == DEFAULT_QUALITY_PROFILE_SOURCE
    assert metadata["is_default"] is True


def test_profile_asset_outlier_override_suppresses_m1_jump_warning(
    tmp_path: Path,
) -> None:
    """Asset-class thresholds should flow through the public rule factory."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_profile_asset_open_jump_ok",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_OPEN_JUMP_OK.csv",
            rows=(
                "20120201 000000;1.000000;1.000010;0.999990;1.000000;0",
                "20120201 000100;1.050000;1.050010;1.049990;1.050000;0",
            ),
        ),
    )
    profile = {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "loose-fx-profile",
        "rules": {
            "bars.ascii.m1_outliers": {
                "thresholds_by_asset_class": {
                    "fx": {"max_open_jump_ratio": 0.10}
                }
            }
        },
    }

    report = _report_for_path(
        path,
        groups=("bars",),
        profile=profile,
    )

    summary = _finding(report, "ASCII_M1_OUTLIER_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["open_jump_count"] == 0
    assert summary.metadata["threshold_selection"]["source"] == "asset_class"
    assert summary.metadata["threshold_selection"]["key"] == "fx"


def test_profile_precision_asset_override_configures_non_fx_rule(
    tmp_path: Path,
) -> None:
    """Precision profiles should make non-FX rules explicit and local."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_xauusd_profiled_precision",
            timeframe=M1,
            filename="DAT_ASCII_XAUUSD_M1_201202.csv",
            rows=("20120201 000000;1730.120;1730.125;1730.100;1730.110;0",),
        ),
    )
    profile = {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "metal-precision",
        "rules": {
            "bars.ascii.m1_precision": {
                "precision_rules_by_asset_class": {
                    "metal": {
                        "name": "operator_metal_three_decimal_bid",
                        "expected_decimal_places": [3],
                        "pip_size": "0.01",
                        "tick_size": "0.001",
                    }
                }
            }
        },
    }

    report = _report_for_path(
        path,
        groups=("bars",),
        profile=profile,
    )

    summary = _finding(report, "ASCII_M1_PRECISION_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["precision_rule_available"] is True
    assert summary.metadata["symbol_metadata"]["precision_rule"]["name"] == (
        "operator_metal_three_decimal_bid"
    )
    assert not any(
        finding.code == "ASCII_M1_PRECISION_RULE_UNAVAILABLE"
        for finding in report.findings
    )


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
            name="m1_profiled_modeling_assumptions",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202.csv",
            rows=("20120201 000000;1.306600;1.306610;1.306590;1.306600;0",),
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


def test_profile_runs_do_not_mutate_default_rule_state() -> None:
    """A configured run must not leak thresholds into later default runs."""
    configured = quality_rules_for_groups(
        ("bars",),
        profile={
            "rules": {
                "bars.ascii.m1_outliers": {
                    "thresholds": {"max_open_jump_ratio": 0.25}
                }
            }
        },
    )
    default = quality_rules_for_groups(("bars",))

    configured_outlier = next(
        rule for rule in configured if rule.rule_id == "bars.ascii.m1_outliers"
    )
    default_outlier = next(
        rule for rule in default if rule.rule_id == "bars.ascii.m1_outliers"
    )
    assert isinstance(configured_outlier, HistDataAsciiM1OutlierRule)
    assert isinstance(default_outlier, HistDataAsciiM1OutlierRule)
    assert configured_outlier.thresholds.max_open_jump_ratio == 0.25
    assert default_outlier.thresholds.max_open_jump_ratio == 0.005


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

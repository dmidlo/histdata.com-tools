"""Tests for modeling-readiness data-quality advisories."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from histdatacom.data_quality import (
    MODELING_READINESS_METADATA_KEY,
    MODELING_READINESS_RULE_ID,
    QualityStatus,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_TICK_CASE,
    write_ascii_case,
)


def test_modeling_group_registers_readiness_rule() -> None:
    """The advertised modeling group should execute readiness advisories."""
    assert [
        rule.rule_id for rule in quality_rules_for_groups(("modeling",))
    ] == [
        MODELING_READINESS_RULE_ID,
    ]
    assert MODELING_READINESS_RULE_ID in {
        rule.rule_id for rule in quality_rules_for_groups(("all",))
    }


def test_m1_modeling_readiness_warns_for_bid_only_and_bar_leakage(
    tmp_path: Path,
) -> None:
    """Bid-only M1 bars need explicit modeling assumptions for backtests."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_M1_CASE))

    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_BID_ONLY_EXECUTION_RISK",
        "MODELING_CURRENT_BAR_LEAKAGE_RISK",
        "MODELING_SPREAD_COST_MISSING",
    ]
    assert summary.metadata["data_defect"] is False
    assert summary.metadata["advisory"] is True
    readiness = summary.metadata[MODELING_READINESS_METADATA_KEY]
    assert readiness["finding_kind"] == "modeling_assumption"
    assert readiness["format_profile"]["price_basis"] == "bid_ohlc"
    assert readiness["cost_assumptions"]["spread_cost"]["status"] == "missing"
    assert readiness["target_horizon"]["status"] == "unconfigured"
    for warning in report.findings[1:]:
        assert warning.metadata["data_defect"] is False
        assert warning.metadata["finding_domain"] == "modeling_readiness"
        assert warning.metadata["finding_kind"] == "modeling_assumption"


def test_tick_modeling_readiness_reports_spread_cost_available(
    tmp_path: Path,
) -> None:
    """Tick bid/ask targets can supply spread costs without M1 warnings."""
    report = _report_for_path(write_ascii_case(tmp_path, CLEAN_TICK_CASE))

    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    costs = summary.metadata["cost_assumptions"]
    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
    ]
    assert summary.metadata["format_profile"]["price_basis"] == "bid_ask_tick"
    assert costs["spread_cost"]["status"] == "available"
    assert costs["spread_cost"]["source"] == "tick_bid_ask"
    assert costs["slippage"]["status"] == "missing"
    assert costs["rollover"]["status"] == "missing"


def test_modeling_readiness_honors_explicit_execution_assumptions(
    tmp_path: Path,
) -> None:
    """Explicit execution assumptions should suppress M1 advisory warnings."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "slippage_model": "one_tick",
            "rollover_cost_model": "excluded_intraday_strategy",
            "target_horizon_minutes": 5,
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    readiness = summary.metadata[MODELING_READINESS_METADATA_KEY]
    costs = readiness["cost_assumptions"]
    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
    ]
    assert costs["spread_cost"]["status"] == "configured"
    assert costs["spread_cost"]["source"] == "fixed_session_profile"
    assert costs["slippage"]["status"] == "configured"
    assert costs["rollover"]["status"] == "configured"
    assert readiness["target_horizon"]["status"] == "feasible"


def test_modeling_target_horizon_warns_when_not_larger_than_granularity(
    tmp_path: Path,
) -> None:
    """Configured horizons must exceed the target granularity."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "target_horizon_ms": 60_000,
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    warning = _finding(
        report,
        "MODELING_TARGET_HORIZON_FEASIBILITY_WARNING",
    )
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_TARGET_HORIZON_FEASIBILITY_WARNING",
    ]
    assert warning.metadata["target_horizon"]["value_ms"] == 60_000
    assert (
        warning.metadata["target_horizon"]["minimum_recommended_ms"] == 60_000
    )
    assert warning.metadata["target_horizon"]["status"] == "too_short"
    assert warning.metadata["data_defect"] is False


def test_modeling_warns_for_m1_high_low_fill_without_intrabar_model(
    tmp_path: Path,
) -> None:
    """High/low fills need an intrabar path model for M1 bars."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "uses_bar_high_low_for_fills": True,
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    warning = _finding(
        report,
        "MODELING_HIGH_LOW_EXECUTION_REALISM_RISK",
    )
    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    execution = summary.metadata[MODELING_READINESS_METADATA_KEY][
        "execution_assumptions"
    ]
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_HIGH_LOW_EXECUTION_REALISM_RISK",
    ]
    assert execution["uses_high_low_fills"] is True
    assert execution["high_low_execution_model"] is False
    assert warning.metadata["missing_assumption"] == "high_low_execution_model"


def test_modeling_warns_for_required_slippage_and_rollover_costs(
    tmp_path: Path,
) -> None:
    """Backtests and overnight positions need explicit cost assumptions."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "backtest_execution": True,
            "holds_overnight": True,
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    costs = summary.metadata[MODELING_READINESS_METADATA_KEY][
        "cost_assumptions"
    ]
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_SLIPPAGE_COST_MISSING",
        "MODELING_ROLLOVER_COST_MISSING",
    ]
    assert costs["slippage"]["required"] is True
    assert costs["rollover"]["required"] is True


def test_modeling_warns_for_forward_fill_and_cross_instrument_alignment(
    tmp_path: Path,
) -> None:
    """Joined sparse features need stale-fill and timestamp alignment policy."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "join_fill_method": "forward_fill",
            "joined_symbols": ["EURUSD", "GBPUSD"],
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    alignment = summary.metadata[MODELING_READINESS_METADATA_KEY][
        "feature_alignment_assumptions"
    ]
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_STALE_FORWARD_FILL_RISK",
        "MODELING_CROSS_INSTRUMENT_ALIGNMENT_RISK",
    ]
    assert alignment["uses_forward_fill"] is True
    assert alignment["uses_cross_instrument_features"] is True
    assert alignment["stale_forward_fill_risk"] is True
    assert alignment["cross_instrument_alignment_risk"] is True


def test_modeling_ignores_empty_alignment_and_calendar_assumptions(
    tmp_path: Path,
) -> None:
    """Empty list metadata should not create modeling-readiness warnings."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "joined_symbols": [],
            "calendar_event_policy": "",
        },
        metadata={
            "event_tags": [],
            "calendar": {"calendar_event_tags": ["", "none"]},
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    summary = _finding(report, "MODELING_READINESS_SUMMARY")
    readiness = summary.metadata[MODELING_READINESS_METADATA_KEY]
    assert report.status is QualityStatus.CLEAN
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
    ]
    assert (
        readiness["feature_alignment_assumptions"][
            "uses_cross_instrument_features"
        ]
        is False
    )
    assert readiness["calendar_regime_assumptions"]["required"] is False


def test_modeling_warns_for_training_transform_leakage_policy(
    tmp_path: Path,
) -> None:
    """Scaling and similar transforms need training-only fit policy."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
            "scaling": "zscore",
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    warning = _finding(report, "MODELING_TRAIN_TEST_LEAKAGE_RISK")
    leakage = warning.metadata["leakage_assumptions"]
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_TRAIN_TEST_LEAKAGE_RISK",
    ]
    assert leakage["leakage_sensitive_transform_keys"] == ["scaling"]
    assert leakage["training_only_policy_configured"] is False


def test_modeling_calendar_event_tags_require_policy_when_available(
    tmp_path: Path,
) -> None:
    """Calendar-profile event tags should flow into modeling readiness."""
    target = _target_for_path(
        write_ascii_case(tmp_path, CLEAN_M1_CASE),
        modeling_assumptions={
            "ask_side_execution_model": True,
            "current_bar_action_timing": "after_bar_close",
            "spread_cost_model": "fixed_session_profile",
        },
        metadata={
            "event_tags": ["crisis:covid_shock"],
            "calendar_profile": {"source": "operator-config"},
        },
    )

    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(("modeling",)),
    )

    warning = _finding(report, "MODELING_CALENDAR_REGIME_POLICY_MISSING")
    regime = warning.metadata["calendar_regime_assumptions"]
    assert report.status is QualityStatus.WARNING
    assert [finding.code for finding in report.findings] == [
        "MODELING_READINESS_SUMMARY",
        "MODELING_CALENDAR_REGIME_POLICY_MISSING",
    ]
    assert regime["target_event_tags"] == ["crisis:covid_shock"]
    assert regime["calendar_profile_source"] == "operator-config"


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("modeling",)),
    )


def _target_for_path(
    path: Path,
    *,
    modeling_assumptions: dict[str, object],
    metadata: dict[str, object] | None = None,
):
    discovery = discover_quality_targets((path,))
    target = discovery.targets[0]
    return replace(
        target,
        metadata={
            **target.metadata,
            **(metadata or {}),
            "modeling_assumptions": modeling_assumptions,
        },
    )


def _finding(report, code: str):
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]

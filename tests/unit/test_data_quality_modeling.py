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


def _report_for_path(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("modeling",)),
    )


def _target_for_path(path: Path, *, modeling_assumptions: dict[str, object]):
    discovery = discover_quality_targets((path,))
    target = discovery.targets[0]
    return replace(
        target,
        metadata={
            **target.metadata,
            "modeling_assumptions": modeling_assumptions,
        },
    )


def _finding(report, code: str):
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]

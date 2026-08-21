"""Exact marked-Hawkes raw-proposal residual diagnostics."""

from __future__ import annotations

import math
from dataclasses import replace
from pathlib import Path

import pytest

import histdatacom.synthetic.hawkes_residuals as residual_module
from histdatacom.synthetic.benchmark import BenchmarkEventV1
from histdatacom.synthetic.event_clock import EventClockCalibrationWindowV1
from histdatacom.synthetic.hawkes_residuals import (
    HawkesResidualPolicyV1,
    HawkesResidualReportV1,
    HawkesResidualStage,
    HawkesResidualStatus,
    HawkesResidualWindowV1,
    evaluate_marked_hawkes_residuals,
    read_hawkes_residual_report,
    run_hawkes_residual_power_study,
    write_hawkes_residual_report,
)
from histdatacom.synthetic.marked_hawkes import (
    HawkesExcitationStructure,
    MarkedHawkesConfigV1,
    MarkedHawkesFitStatus,
    fit_marked_hawkes_challenger,
)

SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
SECOND = 1_000_000_000
START = 1_700_000_000_000_000_000


def _events(start_ns: int, *, count: int = 24) -> tuple[BenchmarkEventV1, ...]:
    events: list[BenchmarkEventV1] = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        bid = 1.0 + symbol_index / 10
        ask = bid + 0.0002
        for index in range(count):
            event_time_ns = (
                start_ns + (index + 1) * 100_000_000 + symbol_index * 2_000_000
            )
            if index % 4 == 0:
                bid += 0.00001
            elif index % 4 == 1:
                ask += 0.00001
            elif index % 4 == 2:
                bid += 0.00001
                ask += 0.00001
            events.append(
                BenchmarkEventV1(
                    source_event_id=f"{symbol}-{start_ns}-{index}",
                    symbol=symbol,
                    event_time_ns=event_time_ns,
                    event_sequence=index,
                    bid=bid,
                    ask=ask,
                    epoch_id="technology_epoch_03",
                    session="london",
                    event_state="observed",
                    sparsity="dense-reference",
                    anchor_id=(
                        f"anchor-{symbol}-{start_ns}-{index}"
                        if index in {0, count - 1}
                        else None
                    ),
                )
            )
    return tuple(events)


def _calibration_windows() -> tuple[EventClockCalibrationWindowV1, ...]:
    return tuple(
        EventClockCalibrationWindowV1(
            window_id=f"calibration-{index}",
            start_ns=START + index * 10 * SECOND,
            end_ns=START + index * 10 * SECOND + 4 * SECOND,
            events=_events(START + index * 10 * SECOND),
        )
        for index in range(2)
    )


def _protected_windows() -> tuple[HawkesResidualWindowV1, ...]:
    return tuple(
        HawkesResidualWindowV1(
            window_id=f"{split}-{index}",
            split_kind=split,
            start_ns=START + (10 + index) * 10 * SECOND,
            end_ns=START + (10 + index) * 10 * SECOND + 4 * SECOND,
            epoch_id="technology_epoch_03",
            session="london",
            observation_scenario_id=f"observation-scenario-{index % 2}",
            events=_events(START + (10 + index) * 10 * SECOND),
            support_boundary_truncation_count=(index + 1) % 2,
        )
        for split in ("validation", "final_holdout")
        for index in range(3)
    )


def test_exact_compensator_and_power_study_are_deterministic() -> None:
    accumulated = [0.0]
    residual_module._accumulate_hazard(
        accumulated,
        recursion=(2.0,),
        baseline=(2.0,),
        excitation=((0.5,),),
        decay=1.0,
        elapsed_seconds=3.0,
    )

    assert accumulated[0] == pytest.approx(6.0 + (1.0 - math.exp(-3.0)))

    policy = HawkesResidualPolicyV1(power_replications=128)
    first = run_hawkes_residual_power_study(
        policy, observed_time_support=256, observed_mark_support=256
    )
    assert first == run_hawkes_residual_power_study(
        policy, observed_time_support=256, observed_mark_support=256
    )
    assert {item.family for item in first} == {
        "wrong_baseline",
        "wrong_decay",
        "wrong_excitation",
        "wrong_mark_probabilities",
    }
    assert all(item.status is HawkesResidualStatus.PASSED for item in first)
    assert all(
        max(item.false_positive_by_sample_size.values())
        <= policy.maximum_false_positive_rate
        for item in first
    )


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    "structure",
    (
        HawkesExcitationStructure.DIAGONAL,
        HawkesExcitationStructure.FULL,
    ),
)
def test_raw_proposal_reports_are_row_free_stratified_and_content_addressed(
    tmp_path: Path, structure: HawkesExcitationStructure
) -> None:
    config = MarkedHawkesConfigV1(structure)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    policy = HawkesResidualPolicyV1(
        alpha=0.01,
        minimum_residual_count=32,
        minimum_stratum_count=8,
        minimum_window_count=2,
        maximum_absolute_lag1=0.95,
        power_replications=64,
    )

    reports = evaluate_marked_hawkes_residuals(
        config,
        fit,
        _protected_windows(),
        engine_id=f"histdatacom.marked-hawkes.{structure.value}",
        policy=policy,
    )

    assert tuple(item.split_kind for item in reports) == (
        "validation",
        "final_holdout",
    )
    for report in reports:
        assert (
            report.payload()["diagnostic_stage"]
            == HawkesResidualStage.RAW_PROPOSAL.value
        )
        assert (
            report.payload()["analytic_compensator_applies_to_final_product"]
            is False
        )
        assert report.payload()["residual_rows_embedded"] is False
        assert {item.dimension for item in report.strata} == {
            "overall",
            "symbol",
            "epoch",
            "session",
            "event_state",
            "observation_scenario",
        }
        assert report.right_censoring_count == 3 * len(SYMBOLS)
        assert report.reset_count == 3 * len(SYMBOLS)
        assert report.residual_count == 3 * len(SYMBOLS) * (24 - 2)
        assert report.mark_residual_count == report.residual_count
        assert report.protected_anchor_truncation_count == 6 * 3
        assert {
            item.key
            for item in report.strata
            if item.dimension == "event_state"
        } == {"observed"}
        ref = write_hawkes_residual_report(report, tmp_path)
        assert read_hawkes_residual_report(ref.path) == report
        assert HawkesResidualReportV1.from_dict(report.to_dict()) == report


def test_unavailable_fit_retains_explicit_refusal() -> None:
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.DIAGONAL)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    refused = replace(
        fit,
        status=MarkedHawkesFitStatus.REFUSED,
        converged=False,
        log_likelihood=None,
        parameters={},
        uncertainty={},
        failure_reason="forced_refusal",
        fit_id="",
    )

    reports = evaluate_marked_hawkes_residuals(
        config,
        refused,
        _protected_windows(),
        engine_id="histdatacom.marked-hawkes.diagonal_self_excitation",
    )

    assert all(item.status is HawkesResidualStatus.REFUSED for item in reports)
    assert all(item.strata == () for item in reports)
    assert all(
        item.reason_codes == ("fitted_hawkes_parameters_unavailable",)
        for item in reports
    )

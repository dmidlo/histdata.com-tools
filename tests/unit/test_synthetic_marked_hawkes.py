"""Marked Hawkes contracts, stability, synchronized generation, and carving."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

import histdatacom.synthetic.marked_hawkes as hawkes_module
from histdatacom.market_context import (
    MarketContextCalendarStateV1,
    MarketContextMissingReason,
    MarketContextQueryStatus,
    MarketContextQueryV1,
    MarketContextView,
)
from histdatacom.synthetic.benchmark import (
    BenchmarkEventV1,
    BenchmarkScenarioV1,
    BenchmarkSplitKind,
)
from histdatacom.synthetic.carving import (
    CarvingBatchStatus,
    HistoricalCarvingConstraintSetV1,
    ReconstructionCandidateBatchV1,
    carve_reconstruction_candidates,
)
from histdatacom.synthetic.contracts import SyntheticEventV1
from histdatacom.synthetic.event_clock import EventClockCalibrationWindowV1
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.marked_hawkes import (
    HawkesExcitationStructure,
    MarkedHawkesConfigV1,
    MarkedHawkesFitResultV1,
    MarkedHawkesFitStatus,
    MarkedHawkesGenerationEvidenceV1,
    MarkedHawkesGenerationStatus,
    MarkedHawkesResourceLimitsV1,
    build_fitted_marked_hawkes_generator,
    build_marked_hawkes_candidate_batches,
    default_marked_hawkes_configs,
    fit_marked_hawkes_challenger,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
SECOND = 1_000_000_000
CALIBRATION_START = 1_600_000_000_000_000_000
GENERATION_START = 1_700_000_000_000_000_000


def _events(
    start_ns: int,
    *,
    epoch_id: str = "technology_epoch_03",
    session: str = "london",
    count: int = 24,
) -> tuple[BenchmarkEventV1, ...]:
    result = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        bid = 1.0 + symbol_index / 10
        ask = bid + 0.0002
        for index in range(count):
            cluster = index // 4
            within_cluster = index % 4
            event_time_ns = (
                start_ns
                + (cluster + 1) * 500_000_000
                + within_cluster * 20_000_000
                + symbol_index * 2_000_000
            )
            if index % 4 == 0:
                bid += 0.00001
            elif index % 4 == 1:
                ask += 0.00001
            elif index % 4 == 2:
                bid += 0.00001
                ask += 0.00001
            result.append(
                BenchmarkEventV1(
                    source_event_id=f"{epoch_id}-{session}-{symbol}-{start_ns}-{index}",
                    symbol=symbol,
                    event_time_ns=event_time_ns,
                    event_sequence=index,
                    bid=bid,
                    ask=ask,
                    epoch_id=epoch_id,
                    session=session,
                    event_state="observed",
                    sparsity="dense-reference",
                    anchor_id=f"anchor-{symbol}-{start_ns}-{index}",
                )
            )
    return tuple(result)


def _calibration_windows() -> tuple[EventClockCalibrationWindowV1, ...]:
    starts = (
        CALIBRATION_START,
        CALIBRATION_START + 86_400 * SECOND,
    )
    return tuple(
        EventClockCalibrationWindowV1(
            window_id=f"calibration-{index}",
            start_ns=start,
            end_ns=start + 10 * SECOND,
            events=_events(start),
        )
        for index, start in enumerate(starts)
    )


def _anchors(
    start_ns: int = GENERATION_START,
    *,
    epoch_id: str = "technology_epoch_03",
    session: str = "london",
) -> tuple[BenchmarkEventV1, ...]:
    return tuple(
        BenchmarkEventV1(
            source_event_id=f"degraded-{epoch_id}-{symbol}-{index}",
            symbol=symbol,
            event_time_ns=start_ns + index * SECOND,
            event_sequence=index,
            bid=1.0 + symbol_index / 10 + index * 0.00003,
            ask=1.0002 + symbol_index / 10 + index * 0.00003,
            epoch_id=epoch_id,
            session=session,
            event_state="observed",
            sparsity="uniform-thinning-0.35",
            anchor_id=f"anchor-{epoch_id}-{symbol}-{index}",
        )
        for symbol_index, symbol in enumerate(SYMBOLS)
        for index in range(5)
    )


def _scenario(epoch_id: str = "technology_epoch_03") -> BenchmarkScenarioV1:
    return BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id=epoch_id,
        severity_id="uniform-thinning-0.35",
        observation_operator_id="operator-1",
        degradation_parameters={"retention_probability": 0.35},
    )


def _window(run_id: str = "run-hawkes") -> ReconstructionWindowV1:
    return ReconstructionWindowV1(
        run_id=run_id,
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=GENERATION_START,
        core_end_ns=GENERATION_START + 5 * SECOND,
    )


@pytest.mark.parametrize("config", default_marked_hawkes_configs())
def test_configs_and_fits_round_trip_deterministically(
    config: MarkedHawkesConfigV1,
) -> None:
    restored = MarkedHawkesConfigV1.from_json(config.to_json())
    windows = _calibration_windows()

    first = fit_marked_hawkes_challenger(restored, windows)
    second = fit_marked_hawkes_challenger(restored, windows)

    assert first == second
    assert first.status is MarkedHawkesFitStatus.FITTED
    assert first.converged
    assert first.log_likelihood is not None
    assert first.fit_id.startswith("marked-hawkes-fit:sha256:")
    assert MarkedHawkesFitResultV1.from_json(first.to_json()) == first
    assert first.symbols == SYMBOLS
    assert first.diagnostics["calibration_history_reset_count"] == 2
    assert first.diagnostics["conditioning_cell_count"] == 2
    assert first.uncertainty["method"] == "responsibility-count-wald-95-v1"
    models = first.parameters["conditioning_models"]
    assert isinstance(models, dict)
    assert set(models) == {
        "exact|technology_epoch_03|london",
        "session|london",
    }


def test_fixed_ablation_registry_is_nested_and_has_no_default() -> None:
    configs = default_marked_hawkes_configs()

    assert tuple(item.excitation_structure for item in configs) == tuple(
        HawkesExcitationStructure
    )
    assert len({item.config_id for item in configs}) == 3
    for config in configs:
        fit = fit_marked_hawkes_challenger(config, _calibration_windows())
        model = fit.parameters["conditioning_models"][
            "exact|technology_epoch_03|london"
        ]
        matrix = model["excitation_matrix"]
        radius = model["spectral_radius"]
        assert radius < config.maximum_branching_ratio < 1.0
        if config.excitation_structure is HawkesExcitationStructure.ZERO:
            assert all(value == 0.0 for row in matrix for value in row)
        if config.excitation_structure is HawkesExcitationStructure.DIAGONAL:
            assert all(
                value == 0.0
                for destination, row in enumerate(matrix)
                for source, value in enumerate(row)
                if destination != source
            )
        if config.excitation_structure is HawkesExcitationStructure.FULL:
            assert any(
                value > 0.0
                for destination, row in enumerate(matrix)
                for source, value in enumerate(row)
                if destination != source
            )


def test_unstable_or_structurally_tampered_fit_fails_at_construction() -> None:
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.FULL)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    parameters = json.loads(json.dumps(fit.parameters))
    for model in parameters["conditioning_models"].values():
        model["excitation_matrix"] = [
            [1.05 if row == column else 0.0 for column in range(3)]
            for row in range(3)
        ]
        model["spectral_radius"] = 1.05
        model["stability_margin"] = -0.05

    with pytest.raises(ValueError, match="unstable"):
        replace(fit, parameters=parameters, fit_id="")
    uncertainty = json.loads(json.dumps(fit.uncertainty))
    uncertainty["method"] = "unsupported"
    with pytest.raises(ValueError, match="method"):
        replace(fit, uncertainty=uncertainty, fit_id="")


def test_fit_refuses_leakage_resource_and_mixed_conditioning_inputs() -> None:
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.ZERO)
    empty = fit_marked_hawkes_challenger(config, ())
    leaked = fit_marked_hawkes_challenger(
        config,
        _calibration_windows(),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        as_of_ns=_calibration_windows()[0].end_ns,
    )
    mixed_events = list(_events(CALIBRATION_START))
    mixed_events[0] = replace(
        mixed_events[0], session="new_york", benchmark_event_id=""
    )
    mixed = fit_marked_hawkes_challenger(
        config,
        (
            EventClockCalibrationWindowV1(
                window_id="mixed",
                start_ns=CALIBRATION_START,
                end_ns=CALIBRATION_START + 10 * SECOND,
                events=tuple(mixed_events),
            ),
        ),
    )
    tiny = MarkedHawkesConfigV1(
        HawkesExcitationStructure.ZERO,
        limits=MarkedHawkesResourceLimitsV1(max_fit_events=10),
    )
    bounded = fit_marked_hawkes_challenger(tiny, _calibration_windows())
    diagnostic_limited = fit_marked_hawkes_challenger(
        MarkedHawkesConfigV1(
            HawkesExcitationStructure.ZERO,
            limits=MarkedHawkesResourceLimitsV1(max_diagnostics=1),
        ),
        _calibration_windows(),
    )

    assert empty.status is MarkedHawkesFitStatus.REFUSED
    assert empty.failure_reason == "missing_calibration_windows"
    assert leaked.failure_reason == "calibration_not_available_as_of"
    assert mixed.failure_reason == "mixed_conditioning_window"
    assert bounded.failure_reason == "fit_event_limit"
    assert "diagnostic count" in (diagnostic_limited.failure_reason or "")
    assert not leaked.parameters and not mixed.uncertainty


@pytest.mark.parametrize("config", default_marked_hawkes_configs())
def test_generation_is_deterministic_synchronized_marked_and_anchor_safe(
    config: MarkedHawkesConfigV1,
) -> None:
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    window = _window()
    anchors = _anchors()

    first = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    second = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert first.events == second.events
    assert first.event_lineage == second.event_lineage
    assert first.evidence.generated_event_count == (
        second.evidence.generated_event_count
    )
    assert first.evidence.proposal_count == second.evidence.proposal_count
    assert first.evidence.status in {
        MarkedHawkesGenerationStatus.GENERATED,
        MarkedHawkesGenerationStatus.EMPTY,
    }
    assert (
        MarkedHawkesGenerationEvidenceV1.from_json(first.evidence.to_json())
        == first.evidence
    )
    retained = tuple(
        item
        for item in first.events
        if not item.sparsity.startswith("marked-hawkes-")
    )
    assert retained == tuple(
        sorted(
            anchors,
            key=lambda item: (
                item.event_time_ns,
                item.symbol,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
    )
    proposals = tuple(
        item
        for item in first.events
        if item.sparsity.startswith("marked-hawkes-")
    )
    assert len(proposals) == len(first.event_lineage)
    assert {item.event_state for item in proposals} <= {
        "ask_only",
        "bid_only",
        "joint",
        "unchanged",
    }
    for proposal in proposals:
        symbol_anchors = [
            item for item in anchors if item.symbol == proposal.symbol
        ]
        assert any(
            left.event_time_ns < proposal.event_time_ns < right.event_time_ns
            for left, right in zip(symbol_anchors, symbol_anchors[1:])
        )


def test_conditioning_uses_exact_then_documented_session_backoff() -> None:
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.ZERO)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )

    exact = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
    )
    fallback = generator.generate_with_evidence(
        _anchors(epoch_id="technology_epoch_99"),
        scenario=_scenario("technology_epoch_99"),
        window=_window(),
        ensemble_member_id="member-01",
    )
    unsupported = generator.generate_with_evidence(
        _anchors(epoch_id="technology_epoch_99", session="unsupported"),
        scenario=_scenario("technology_epoch_99"),
        window=_window(),
        ensemble_member_id="member-01",
    )

    assert exact.evidence.conditioning_support_level == "exact_epoch_session"
    assert fallback.evidence.conditioning_support_level == "session_backoff"
    assert unsupported.evidence.status is MarkedHawkesGenerationStatus.REFUSED
    assert "conditioning support" in (unsupported.evidence.failure_reason or "")


def test_prior_history_is_bounded_and_changes_full_hawkes_semantics() -> None:
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.FULL)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    history = tuple(
        BenchmarkEventV1(
            source_event_id=f"history-{symbol}",
            symbol=symbol,
            event_time_ns=GENERATION_START - 10_000_000,
            event_sequence=0,
            bid=1.0 + index / 10,
            ask=1.0002 + index / 10,
            epoch_id="technology_epoch_03",
            session="london",
            event_state="observed",
            sparsity="history",
        )
        for index, symbol in enumerate(SYMBOLS)
    )

    with_history = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
        history_events=history,
    )
    leaked = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
        history_events=(
            replace(
                history[0],
                event_time_ns=GENERATION_START,
                benchmark_event_id="",
            ),
        ),
    )
    no_history = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
    )
    outside_lookback = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
        history_events=(
            replace(
                history[0],
                event_time_ns=(
                    GENERATION_START - config.limits.max_history_ns - 1
                ),
                benchmark_event_id="",
            ),
        ),
    )

    assert with_history.evidence.history_event_count == len(history)
    assert leaked.events == ()
    assert leaked.evidence.status is MarkedHawkesGenerationStatus.REFUSED
    assert "prior-only" in (leaked.evidence.failure_reason or "")
    assert outside_lookback.evidence.history_event_count == 0
    assert outside_lookback.events == no_history.events


def test_resource_overflow_refuses_without_partial_rows() -> None:
    limits = MarkedHawkesResourceLimitsV1(
        max_generated_events_per_interval=1,
        max_generated_events_per_window=1,
        max_ogata_proposals=1,
    )
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.FULL, limits=limits)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )

    result = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
    )

    assert result.evidence.status is MarkedHawkesGenerationStatus.REFUSED
    assert result.events == ()
    assert result.event_lineage == ()
    assert result.evidence.proposal_count > limits.max_ogata_proposals
    assert "limit exceeded" in (result.evidence.failure_reason or "")


def test_hawkes_batches_use_shared_carving_and_detect_anchor_tampering() -> (
    None
):
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.FULL)
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:hawkes-test",
        require_fingerprint_validation=False,
    )
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source-version:hawkes-test",),
        configuration_ids=(config.config_id, constraints.constraint_set_id),
        ensemble_member_ids=("member-01",),
        base_seed=451,
    )
    window = _window(run.run_id)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    result = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    observed = tuple(
        SyntheticEventV1.observed(
            symbol=item.symbol,
            event_time_ns=item.event_time_ns,
            event_sequence=item.event_sequence,
            bid=item.bid,
            ask=item.ask,
            run_id=run.run_id,
            ensemble_member_id="member-01",
            source_version_id="source-version:hawkes-test",
            source_series_id=f"series:{item.symbol}",
            source_period="202001",
            source_row_id=index,
        )
        for index, item in enumerate(_anchors(), start=1)
    )
    batches = build_marked_hawkes_candidate_batches(
        run=run,
        window=window,
        config=config,
        fit_result=fit,
        generation_result=result,
        observed_events=observed,
        session_state="active",
        special_tags=("ordinary",),
    )
    tampered = (
        replace(observed[0], bid=observed[0].bid + 0.00001, event_id=""),
        *observed[1:],
    )
    with pytest.raises(ValueError, match="anchors differ"):
        build_marked_hawkes_candidate_batches(
            run=run,
            window=window,
            config=config,
            fit_result=fit,
            generation_result=result,
            observed_events=tampered,
            session_state="active",
        )
    batch = next(item for item in batches if item.status.value == "generated")
    assert isinstance(batch, ReconstructionCandidateBatchV1)
    calendar = MarketContextCalendarStateV1(
        timestamp_utc_ns=window.core_start_ns,
        session_state="active",
        clock_sessions=("london",),
        active_sessions=("london",),
        overlaps=(),
        special_tags=("ordinary",),
        holiday_tags=(),
        event_tags=(),
        calendar_tags=("ordinary",),
        profile_source="calendar-profile:hawkes-test",
        profile_version="1.0.0",
        profile_complete=True,
        limitations=("deterministic Hawkes test fixture",),
    )
    context = MarketContextQueryV1(
        timeline_id="market-context:hawkes-test",
        view=MarketContextView.EX_POST,
        start_ns=window.core_start_ns,
        end_ns=window.core_end_ns,
        as_of_ns=None,
        events=(),
        status=MarketContextQueryStatus.MISSING,
        missing_reason=MarketContextMissingReason.NO_MATCHING_EVENT,
        calendar_state=calendar,
        requested_symbols=(batch.symbol,),
        window_id=window.window_id,
    )
    anchor_ids = {batch.left_anchor_event_id, batch.right_anchor_event_id}
    carved = carve_reconstruction_candidates(
        run=run,
        window=window,
        candidate_batch=batch,
        observed_events=tuple(
            item for item in observed if item.event_id in anchor_ids
        ),
        market_context=context,
        constraints=constraints,
        fingerprint_evidence=None,
    )

    assert carved.status is CarvingBatchStatus.ACCEPTED
    assert carved.rejection_summary.accepted_count == len(batch.events)


def test_simultaneous_calibration_events_do_not_excite_at_same_timestamp() -> (
    None
):
    windows = _calibration_windows()
    indexed = hawkes_module._indexed_windows(windows, SYMBOLS)
    baseline = [1.0, 1.0, 1.0]
    excitation = [
        [0.1, 0.1, 0.1],
        [0.1, 0.1, 0.1],
        [0.1, 0.1, 0.1],
    ]

    first = hawkes_module._hawkes_log_likelihood(
        indexed, baseline, excitation, decay_per_second=2.0
    )
    reversed_same_time = tuple(
        (
            start,
            end,
            tuple(sorted(events, key=lambda item: (item[0], -item[1]))),
        )
        for start, end, events in indexed
    )
    second = hawkes_module._hawkes_log_likelihood(
        reversed_same_time, baseline, excitation, decay_per_second=2.0
    )

    assert first == second

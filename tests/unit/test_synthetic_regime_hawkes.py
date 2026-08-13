"""Regime Hawkes contracts, inference, generation, context, and carving."""

from __future__ import annotations

import copy
from dataclasses import replace
from itertools import pairwise

import pytest

import histdatacom.synthetic.regime_hawkes as regime_module
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
from histdatacom.synthetic.regime_hawkes import (
    RegimeHawkesConfigV1,
    RegimeHawkesFitResultV1,
    RegimeHawkesFitStatus,
    RegimeHawkesGenerationEvidenceV1,
    RegimeHawkesGenerationResultV1,
    RegimeHawkesGenerationStatus,
    RegimeHawkesModulation,
    RegimeHawkesWindowContextV1,
    build_fitted_regime_hawkes_generator,
    build_regime_hawkes_candidate_batches,
    default_regime_hawkes_configs,
    fit_regime_hawkes_challenger,
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
                    source_event_id=(
                        f"{epoch_id}-{session}-{symbol}-{start_ns}-{index}"
                    ),
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
    return tuple(
        EventClockCalibrationWindowV1(
            window_id=f"calibration-{index}",
            start_ns=start,
            end_ns=start + 10 * SECOND,
            events=_events(start),
        )
        for index, start in enumerate(
            (CALIBRATION_START, CALIBRATION_START + 86_400 * SECOND)
        )
    )


def _window(run_id: str = "run-regime") -> ReconstructionWindowV1:
    return ReconstructionWindowV1(
        run_id=run_id,
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=GENERATION_START,
        core_end_ns=GENERATION_START + 5 * SECOND,
    )


def _context(window: ReconstructionWindowV1) -> RegimeHawkesWindowContextV1:
    return RegimeHawkesWindowContextV1(
        window_id=window.window_id,
        session="london",
        technology_assignment_kind="epoch",
        technology_label="technology_epoch_03",
        feed_epoch_definition_id="feed-epoch-definition:test",
        epoch_id="technology-epoch:test",
    )


def _anchors() -> tuple[BenchmarkEventV1, ...]:
    return tuple(
        BenchmarkEventV1(
            source_event_id=f"degraded-{symbol}-{index}",
            symbol=symbol,
            event_time_ns=GENERATION_START + index * SECOND,
            event_sequence=index,
            bid=1.0 + symbol_index / 10 + index * 0.00003,
            ask=1.0002 + symbol_index / 10 + index * 0.00003,
            epoch_id="technology_epoch_03",
            session="london",
            event_state="observed",
            sparsity="uniform-thinning-0.35",
            anchor_id=f"anchor-{symbol}-{index}",
        )
        for symbol_index, symbol in enumerate(SYMBOLS)
        for index in range(5)
    )


def _scenario() -> BenchmarkScenarioV1:
    return BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id="technology_epoch_03",
        severity_id="uniform-thinning-0.35",
        observation_operator_id="operator-1",
        degradation_parameters={"retention_probability": 0.35},
    )


@pytest.mark.parametrize("config", default_regime_hawkes_configs())
def test_configs_and_fits_round_trip_deterministically(
    config: RegimeHawkesConfigV1,
) -> None:
    restored = RegimeHawkesConfigV1.from_json(config.to_json())

    first = fit_regime_hawkes_challenger(restored, _calibration_windows())
    second = fit_regime_hawkes_challenger(restored, _calibration_windows())

    assert first == second
    assert first.status is RegimeHawkesFitStatus.FITTED
    assert first.converged
    assert first.fit_id.startswith("regime-hawkes-fit:sha256:")
    assert RegimeHawkesFitResultV1.from_json(first.to_json()) == first
    assert first.diagnostics["calibration_history_reset_count"] == 2
    assert first.diagnostics["conditioning_cell_count"] == 2
    assert first.diagnostics["minimum_state_occupancy"] >= 0.03
    assert first.diagnostics["minimum_activity_contrast"] >= 0.01
    assert first.diagnostics["maximum_spectral_radius"] < 0.9
    model = first.parameters["conditioning_models"][
        "exact|technology_epoch_03|london"
    ]
    assert model["activity_levels"][0] < model["activity_levels"][1]
    assert model["expected_transition_count"] >= 0.25
    likelihood_trace = model["log_likelihood_trace"]
    assert len(likelihood_trace) == model["iteration_count"]
    assert likelihood_trace[-1] == pytest.approx(model["log_likelihood"])
    assert all(right >= left for left, right in pairwise(likelihood_trace))
    state = first.state_diagnostics["exact|technology_epoch_03|london"]
    for evidence in state["windows"]:
        assert all(
            sum(probabilities) == pytest.approx(1.0)
            for probabilities in evidence["filtered_probabilities"]
        )
        assert all(
            sum(probabilities) == pytest.approx(1.0)
            for probabilities in evidence["smoothed_probabilities"]
        )
        assert any(
            filtered != smoothed
            for filtered, smoothed in zip(
                evidence["filtered_probabilities"],
                evidence["smoothed_probabilities"],
            )
        )
    recovered = [
        int(probabilities[1] > probabilities[0])
        for probabilities in state["windows"][0]["smoothed_probabilities"]
    ]
    assert sum(recovered[1:7]) >= 5
    assert not any(recovered[8:])


def test_generalized_em_rolls_back_a_materially_decreasing_proposal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = default_regime_hawkes_configs()[1]
    original_m_step = regime_module._m_step_parameters
    call_count = 0

    def hostile_m_step(config_value, binned, symbols, gamma):
        nonlocal call_count
        call_count += 1
        baselines, matrices, marks = original_m_step(
            config_value, binned, symbols, gamma
        )
        if call_count == 2:
            baselines = [
                [value * 1_000_000.0 for value in row] for row in baselines
            ]
        return baselines, matrices, marks

    monkeypatch.setattr(regime_module, "_m_step_parameters", hostile_m_step)

    model, _, _ = regime_module._fit_conditioning_model(
        config,
        _calibration_windows(),
        SYMBOLS,
        deadline=float("inf"),
    )

    assert call_count == 2
    assert model["iteration_count"] == 1
    assert model["log_likelihood_trace"] == [model["log_likelihood"]]


def test_fixed_ablations_share_only_the_declared_excitation_parameters() -> (
    None
):
    fits = {
        config.modulation: fit_regime_hawkes_challenger(
            config, _calibration_windows()
        )
        for config in default_regime_hawkes_configs()
    }

    baseline_model = fits[RegimeHawkesModulation.BASELINE_ONLY].parameters[
        "conditioning_models"
    ]["exact|technology_epoch_03|london"]
    full_model = fits[
        RegimeHawkesModulation.BASELINE_AND_EXCITATION
    ].parameters["conditioning_models"]["exact|technology_epoch_03|london"]

    assert (
        baseline_model["excitation_matrices"][0]
        == baseline_model["excitation_matrices"][1]
    )
    assert (
        full_model["excitation_matrices"][0]
        != full_model["excitation_matrices"][1]
    )
    assert tuple(
        item.modulation for item in default_regime_hawkes_configs()
    ) == (
        RegimeHawkesModulation.BASELINE_ONLY,
        RegimeHawkesModulation.BASELINE_AND_EXCITATION,
    )


def test_technology_transition_and_point_in_time_context_are_strict() -> None:
    transition = RegimeHawkesWindowContextV1(
        window_id="window-1",
        session="london",
        technology_assignment_kind="transition",
        technology_label="transition:201001-201002",
        feed_epoch_definition_id="definition-1",
        boundary_id="boundary-1",
        boundary_support=0.75,
        uncertainty_start_period="200912",
        uncertainty_end_period="201003",
        observed_context_id="context-1",
        observed_context_available_ns=10,
        use_time_ns=10,
        filtered_initial_probabilities=(0.8, 0.2),
    )

    assert (
        RegimeHawkesWindowContextV1.from_json(transition.to_json())
        == transition
    )
    assert transition.technology_assignment_kind == "transition"
    assert (
        transition.identity_payload()["latent_market_state_is_technology_epoch"]
        is False
    )
    with pytest.raises(ValueError, match="unavailable"):
        replace(
            transition,
            observed_context_available_ns=11,
            context_id="",
        )
    with pytest.raises(ValueError, match="boundary"):
        RegimeHawkesWindowContextV1(
            window_id="window-1",
            session="london",
            technology_assignment_kind="transition",
            technology_label="transition:201001-201002",
            feed_epoch_definition_id="definition-1",
        )


def test_transition_windows_remain_a_distinct_fit_stratum() -> None:
    windows = tuple(
        EventClockCalibrationWindowV1(
            window_id=item.window_id,
            start_ns=item.start_ns,
            end_ns=item.end_ns,
            events=tuple(
                replace(
                    event,
                    epoch_id="transition:201001-201002",
                    benchmark_event_id="",
                )
                for event in item.events
            ),
        )
        for item in _calibration_windows()
    )
    contexts = tuple(
        RegimeHawkesWindowContextV1(
            window_id=item.window_id,
            session="london",
            technology_assignment_kind="transition",
            technology_label="transition:201001-201002",
            feed_epoch_definition_id="definition-1",
            boundary_id="boundary-1",
            boundary_support=0.75,
            uncertainty_start_period="200912",
            uncertainty_end_period="201003",
        )
        for item in windows
    )

    fit = fit_regime_hawkes_challenger(
        default_regime_hawkes_configs()[0],
        windows,
        window_contexts=contexts,
    )

    assert fit.status is RegimeHawkesFitStatus.FITTED
    assert fit.diagnostics["technology_transition_cell_count"] == 1
    assert (
        "exact|transition:201001-201002|london"
        in fit.parameters["conditioning_models"]
    )


def test_fit_refusals_expose_no_parameters_or_state_paths() -> None:
    config = default_regime_hawkes_configs()[1]
    empty = fit_regime_hawkes_challenger(config, ())
    leaked = fit_regime_hawkes_challenger(
        config,
        _calibration_windows(),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        as_of_ns=CALIBRATION_START,
    )
    missing_as_of = fit_regime_hawkes_challenger(
        config,
        _calibration_windows(),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
    )
    unexpected_as_of = fit_regime_hawkes_challenger(
        config,
        _calibration_windows(),
        as_of_ns=CALIBRATION_START,
    )
    invalid_input = fit_regime_hawkes_challenger(
        config,
        (object(),),  # type: ignore[arg-type]
    )
    tiny = replace(
        config,
        limits=replace(config.limits, max_fit_bins=10, limits_id=""),
        config_id="",
    )
    bounded = fit_regime_hawkes_challenger(tiny, _calibration_windows())
    diagnostic_bounded_config = replace(
        config,
        limits=replace(
            config.limits,
            max_diagnostic_bytes=1_024,
            limits_id="",
        ),
        config_id="",
    )
    diagnostic_bounded = fit_regime_hawkes_challenger(
        diagnostic_bounded_config, _calibration_windows()
    )
    iteration_limited_config = replace(
        config,
        limits=replace(config.limits, max_iterations=2, limits_id=""),
        config_id="",
    )
    iteration_limited = fit_regime_hawkes_challenger(
        iteration_limited_config, _calibration_windows()
    )

    for result in (
        empty,
        leaked,
        missing_as_of,
        unexpected_as_of,
        invalid_input,
        bounded,
        diagnostic_bounded,
        iteration_limited,
    ):
        assert result.status is not RegimeHawkesFitStatus.FITTED
        assert not result.parameters
        assert not result.uncertainty
        assert not result.state_diagnostics
        assert result.failure_reason


@pytest.mark.parametrize("config", default_regime_hawkes_configs())
def test_generation_is_deterministic_anchor_safe_and_context_bound(
    config: RegimeHawkesConfigV1,
) -> None:
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    window = _window()
    context = _context(window)
    generator = build_fitted_regime_hawkes_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts=(context,),
    )

    first = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    second = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert first.events == second.events
    assert first.event_lineage == second.event_lineage
    assert first.evidence.generated_event_count == (
        second.evidence.generated_event_count
    )
    assert first.evidence.poisson_iteration_count == (
        second.evidence.poisson_iteration_count
    )
    assert first.evidence.state_bin_counts == second.evidence.state_bin_counts
    assert first.evidence.status in {
        RegimeHawkesGenerationStatus.GENERATED,
        RegimeHawkesGenerationStatus.EMPTY,
    }
    assert (
        RegimeHawkesGenerationEvidenceV1.from_json(first.evidence.to_json())
        == first.evidence
    )
    original = [item for item in first.events if item.anchor_id is not None]
    assert original == sorted(
        _anchors(), key=regime_module._benchmark_event_key
    )
    assert first.evidence.window_context_id == context.context_id
    assert first.evidence.input_event_content_sha256
    assert first.evidence.history_content_sha256
    assert (
        sum(first.evidence.state_bin_counts)
        == first.evidence.processed_bin_count
    )
    assert first.evidence.initial_state_policy == (
        "fitted-window-reset-distribution-v1"
    )
    assert all(
        lineage.state_label in {"calm", "active"}
        and 0.0 <= lineage.filtered_state_probability <= 1.0
        for lineage in first.event_lineage
    )
    anchors_by_symbol = {
        symbol: [item for item in _anchors() if item.symbol == symbol]
        for symbol in SYMBOLS
    }
    assert all(
        any(
            left.event_time_ns < event.event_time_ns < right.event_time_ns
            for left, right in pairwise(values)
        )
        for event in first.events
        if event.sparsity.startswith("regime-hawkes-")
        for values in (anchors_by_symbol[event.symbol],)
    )

    null_anchor = replace(
        _anchors()[0],
        anchor_id=None,
        benchmark_event_id="",
    )
    mixed_anchor_ids = (null_anchor, *_anchors()[1:])
    null_safe = generator.generate_with_evidence(
        mixed_anchor_ids,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    assert null_safe.evidence.status in {
        RegimeHawkesGenerationStatus.GENERATED,
        RegimeHawkesGenerationStatus.EMPTY,
    }
    assert null_safe.evidence.failure_reason is None

    event_by_id = {item.source_event_id: item for item in first.events}
    for lineage in first.event_lineage:
        event = event_by_id[lineage.source_event_id]
        left, right = regime_module._bracketing_anchors(
            anchors_by_symbol[event.symbol], event.event_time_ns
        )
        assert (event.bid, event.ask) == regime_module._project_quote(
            left, right, event.event_time_ns, lineage.event_state
        )
        assert event.event_state == lineage.event_state
        assert lineage.excitation_source_contribution >= 0.0
        assert isinstance(lineage.state_transitioned, bool)


def test_generation_fails_closed_for_context_history_and_tampered_stability() -> (
    None
):
    config = default_regime_hawkes_configs()[1]
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    window = _window()
    context = _context(window)
    generator = build_fitted_regime_hawkes_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts=(context,),
    )
    future_history = replace(
        _anchors()[0],
        event_time_ns=window.core_start_ns,
        source_event_id="future-history",
        benchmark_event_id="",
    )
    refused = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=(future_history,),
    )
    assert refused.evidence.status is RegimeHawkesGenerationStatus.REFUSED
    assert not refused.events
    assert refused.evidence.lineage_content_sha256 is None

    old_history = replace(
        _anchors()[0],
        event_time_ns=(window.core_start_ns - config.limits.max_history_ns - 1),
        source_event_id="outside-lookback-history",
        benchmark_event_id="",
    )
    without_history = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    outside_lookback = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=(old_history,),
    )
    assert outside_lookback.events == without_history.events
    assert outside_lookback.event_lineage == without_history.event_lineage
    assert outside_lookback.evidence.history_event_count == 0
    assert (
        outside_lookback.evidence.history_content_sha256
        == without_history.evidence.history_content_sha256
    )

    model = fit.parameters["conditioning_models"][
        "exact|technology_epoch_03|london"
    ]
    model["spectral_radii"][0] = 0.99
    with pytest.raises(ValueError, match="content identity"):
        build_fitted_regime_hawkes_generator(
            config,
            fit,
            ensemble_member_ids=("member-01",),
            window_contexts=(context,),
        )


def test_regime_batches_satisfy_generic_carving_and_anchor_digest() -> None:
    config = default_regime_hawkes_configs()[1]
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:regime-test",
        require_fingerprint_validation=False,
    )
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source-version:regime-test",),
        configuration_ids=(config.config_id, constraints.constraint_set_id),
        ensemble_member_ids=("member-01",),
        base_seed=452,
    )
    window = _window(run.run_id)
    context = replace(
        _context(window),
        observed_context_id="ctx0.2",
        observed_context_available_ns=window.core_start_ns,
        use_time_ns=window.core_start_ns,
        filtered_initial_probabilities=(0.8, 0.2),
        context_id="",
    )
    generator = build_fitted_regime_hawkes_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts=(context,),
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
            source_version_id="source-version:regime-test",
            source_series_id=f"series:{item.symbol}",
            source_period="202001",
            source_row_id=index,
        )
        for index, item in enumerate(_anchors(), start=1)
    )
    batches = build_regime_hawkes_candidate_batches(
        run=run,
        window=window,
        config=config,
        fit_result=fit,
        generation_result=result,
        window_context=context,
        observed_events=observed,
        session_state="active",
        special_tags=("ordinary",),
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
        profile_source="calendar-profile:regime-test",
        profile_version="1.0.0",
        profile_complete=True,
        limitations=("deterministic regime test fixture",),
    )
    market_context = MarketContextQueryV1(
        timeline_id="market-context:regime-test",
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
        market_context=market_context,
        constraints=constraints,
        fingerprint_evidence=None,
    )
    assert carved.status is CarvingBatchStatus.ACCEPTED


def test_smoothed_diagnostics_do_not_drive_generation() -> None:
    config = default_regime_hawkes_configs()[0]
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    diagnostics = copy.deepcopy(fit.state_diagnostics)
    window_diagnostics = diagnostics["exact|technology_epoch_03|london"][
        "windows"
    ][0]
    window_diagnostics["smoothed_probabilities"] = [
        [1.0, 0.0] for _ in window_diagnostics["smoothed_probabilities"]
    ]
    scalar_diagnostics = dict(fit.diagnostics)
    for _ in range(8):
        measured = regime_module._diagnostic_payload_bytes(
            fit.uncertainty,
            scalar_diagnostics,
            diagnostics,
        )
        if scalar_diagnostics["diagnostic_bytes"] == measured:
            break
        scalar_diagnostics["diagnostic_bytes"] = measured
    diagnostic_variant = replace(
        fit,
        diagnostics=scalar_diagnostics,
        state_diagnostics=diagnostics,
        fit_id="",
    )
    assert diagnostic_variant.fit_id != fit.fit_id

    window = _window()
    context = _context(window)
    results = []
    for selected_fit in (fit, diagnostic_variant):
        generator = build_fitted_regime_hawkes_generator(
            config,
            selected_fit,
            ensemble_member_ids=("member-01",),
            window_contexts=(context,),
        )
        results.append(
            generator.generate_with_evidence(
                _anchors(),
                scenario=_scenario(),
                window=window,
                ensemble_member_id="member-01",
            )
        )

    def event_process(
        result: RegimeHawkesGenerationResultV1,
    ) -> tuple[tuple[object, ...], ...]:
        generated = [
            item
            for item in result.events
            if item.sparsity.startswith("regime-hawkes-")
        ]
        return tuple(
            (
                item.symbol,
                item.event_time_ns,
                item.event_sequence,
                item.bid,
                item.ask,
                item.event_state,
            )
            for item in generated
        )

    def lineage_process(
        result: RegimeHawkesGenerationResultV1,
    ) -> tuple[tuple[object, ...], ...]:
        return tuple(
            sorted(
                (
                    item.destination_symbol,
                    item.bin_start_ns,
                    item.state_label,
                    item.filtered_state_probability,
                    item.conditional_intensity,
                    item.excitation_source_symbol or "",
                    item.excitation_source_contribution,
                    item.state_transitioned,
                    item.event_state,
                )
                for item in result.event_lineage
            )
        )

    assert event_process(results[0]) == event_process(results[1])
    assert lineage_process(results[0]) == lineage_process(results[1])
    assert results[0].evidence.state_bin_counts == (
        results[1].evidence.state_bin_counts
    )
    assert results[0].evidence.final_filtered_probabilities == (
        results[1].evidence.final_filtered_probabilities
    )


def test_generation_memory_preflight_refuses_without_partial_output() -> None:
    base = default_regime_hawkes_configs()[1]
    config = replace(
        base,
        limits=replace(
            base.limits,
            max_peak_memory_bytes=500_000,
            limits_id="",
        ),
        config_id="",
    )
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    assert fit.status is RegimeHawkesFitStatus.FITTED
    window = _window()
    generator = build_fitted_regime_hawkes_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts=(_context(window),),
    )

    result = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert result.evidence.status is RegimeHawkesGenerationStatus.REFUSED
    assert (
        result.evidence.failure_reason
        == "generation memory estimate exceeds limit"
    )
    assert not result.events
    assert not result.event_lineage


def test_generation_enforces_aggregate_synchronized_bin_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def two_events_per_symbol(*_args: object) -> tuple[int, int]:
        return 2, 1

    monkeypatch.setattr(regime_module, "_poisson", two_events_per_symbol)
    base = default_regime_hawkes_configs()[1]
    config = replace(
        base,
        limits=replace(
            base.limits,
            max_generated_events_per_bin=3,
            limits_id="",
        ),
        config_id="",
    )
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    window = _window()
    generator = build_fitted_regime_hawkes_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts=(_context(window),),
    )

    result = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert result.evidence.status is RegimeHawkesGenerationStatus.REFUSED
    assert result.evidence.failure_reason == (
        "aggregate per-bin generated-event limit exceeded"
    )
    assert not result.events
    assert not result.event_lineage


def test_forward_backward_is_scaled_and_canonicalization_fixes_label_switching() -> (
    None
):
    filtered, smoothed, transitions, likelihood = (
        regime_module._scaled_forward_backward(
            [0.5, 0.5],
            [[0.9, 0.1], [0.2, 0.8]],
            [[-1_000.0, -1_001.0], [-1_002.0, -1_000.0]],
        )
    )
    assert all(sum(row) == pytest.approx(1.0) for row in filtered)
    assert all(sum(row) == pytest.approx(1.0) for row in smoothed)
    assert sum(sum(row) for row in transitions) == pytest.approx(1.0)
    assert likelihood < 0.0

    baselines, _, _, transition, initial, gamma = (
        regime_module._canonicalize_states(
            [[10.0], [1.0]],
            [[[0.1]], [[0.01]]],
            [[{"unchanged": 1.0}], [{"unchanged": 1.0}]],
            [[0.8, 0.2], [0.1, 0.9]],
            [0.7, 0.3],
            [[0.8, 0.2]],
            decay_per_second=2.0,
        )
    )
    assert baselines == [[1.0], [10.0]]
    assert transition == [[0.9, 0.1], [0.2, 0.8]]
    assert initial == [0.3, 0.7]
    assert gamma == [[0.2, 0.8]]


def test_model_validation_detects_low_occupancy_and_invalid_transitions() -> (
    None
):
    config = default_regime_hawkes_configs()[1]
    fit = fit_regime_hawkes_challenger(config, _calibration_windows())
    source = fit.parameters["conditioning_models"][
        "exact|technology_epoch_03|london"
    ]
    low_occupancy = copy.deepcopy(source)
    low_occupancy["occupancy"] = [0.999, 0.001]
    with pytest.raises(
        regime_module.RegimeHawkesGenerationError,
        match="occupancy",
    ):
        regime_module._validate_model(low_occupancy, config)

    invalid_transition = copy.deepcopy(source)
    invalid_transition["transition_matrix"] = [[1.0, 0.0], [0.2, 0.8]]
    with pytest.raises(ValueError, match="transition matrix"):
        regime_module._validate_model(invalid_transition, config)

    collapsed = copy.deepcopy(source)
    collapsed["baseline_rates_per_second"][1] = copy.deepcopy(
        collapsed["baseline_rates_per_second"][0]
    )
    collapsed["excitation_matrices"][1] = copy.deepcopy(
        collapsed["excitation_matrices"][0]
    )
    collapsed["spectral_radii"][1] = collapsed["spectral_radii"][0]
    collapsed["activity_levels"][1] = collapsed["activity_levels"][0]
    collapsed["activity_contrast"] = 0.0
    with pytest.raises(ValueError, match="switched or collapsed"):
        regime_module._validate_model(collapsed, config)


def test_spectral_radius_handles_periodic_reducible_and_invalid_matrices() -> (
    None
):
    assert regime_module._spectral_radius([[0.0, 2.0], [0.5, 0.0]]) == (
        pytest.approx(1.0)
    )
    assert regime_module._spectral_radius(
        [[0.0, 1.0, 0.0], [0.0, 0.0, 2.0], [0.0, 0.0, 0.0]]
    ) == pytest.approx(0.0)
    assert regime_module._spectral_radius(
        [[0.2, 0.0, 0.0], [0.0, 0.0, 3.0], [0.0, 0.25, 0.0]]
    ) == pytest.approx(3.0**0.5 / 2.0)
    with pytest.raises(ValueError, match="non-negative"):
        regime_module._spectral_radius([[0.0, -0.1], [0.1, 0.0]])

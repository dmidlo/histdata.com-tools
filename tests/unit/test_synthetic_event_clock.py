"""Classical event-clock contracts, fits, bounds, and benchmark adapters."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

import histdatacom.synthetic.event_clock as event_clock_module

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
from histdatacom.synthetic.event_clock import (
    AutoregressiveConditionalDurationConfigV1,
    CoxProcessConfigV1,
    EventClockCalibrationWindowV1,
    EventClockConfigurationV1,
    EventClockFamily,
    EventClockFitResultV1,
    EventClockFitStatus,
    EventClockGenerationEvidenceV1,
    EventClockGenerationStatus,
    EventClockResourceLimitsV1,
    HiddenMarkovDurationMarkConfigV1,
    NonHomogeneousPoissonConfigV1,
    build_event_clock_candidate_batches,
    build_fitted_event_clock_generator,
    default_event_clock_configs,
    event_clock_config_from_dict,
    fit_event_clock_challenger,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
SECOND = 1_000_000_000


def _events(start_ns: int, count: int = 24) -> tuple[BenchmarkEventV1, ...]:
    events = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        elapsed = 0
        bid = 1.0 + symbol_index / 10
        ask = bid + 0.0002
        for index in range(count):
            elapsed += (100_000_000, 350_000_000)[index % 2]
            if index % 4 == 0:
                bid += 0.0001
                ask += 0.0001
            elif index % 4 == 1:
                bid += 0.0001
            elif index % 4 == 2:
                ask += 0.0001
            events.append(
                BenchmarkEventV1(
                    source_event_id=f"{symbol}-{start_ns}-{index}",
                    symbol=symbol,
                    event_time_ns=start_ns + elapsed,
                    event_sequence=index,
                    bid=bid,
                    ask=ask,
                    epoch_id="technology_epoch_03",
                    session="london",
                    event_state="observed",
                    sparsity="dense-reference",
                    anchor_id=f"anchor-{symbol}-{start_ns}-{index}",
                )
            )
    return tuple(events)


def _calibration_windows() -> tuple[EventClockCalibrationWindowV1, ...]:
    return tuple(
        EventClockCalibrationWindowV1(
            window_id=f"calibration-{index}",
            start_ns=start,
            end_ns=start + 10 * SECOND,
            events=_events(start, count=24 + index),
        )
        for index, start in enumerate(
            (1_600_000_000_000_000_000, 1_600_086_400_000_000_000)
        )
    )


def _boundary_calibration_windows() -> (
    tuple[EventClockCalibrationWindowV1, ...]
):
    windows = []
    for window_index, duration_ns in enumerate((100_000_000, SECOND)):
        start = 1_500_000_000_000_000_000 + window_index * 86_400 * SECOND
        events = tuple(
            BenchmarkEventV1(
                source_event_id=(
                    f"boundary-{window_index}-{symbol}-{event_index}"
                ),
                symbol=symbol,
                event_time_ns=start + (event_index + 1) * duration_ns,
                event_sequence=event_index,
                bid=1.0 + symbol_index / 10 + event_index * 0.00001,
                ask=1.0002 + symbol_index / 10 + event_index * 0.00001,
                epoch_id="technology_epoch_03",
                session="london",
                event_state="observed",
                sparsity="dense-reference",
            )
            for symbol_index, symbol in enumerate(SYMBOLS)
            for event_index in range(7)
        )
        windows.append(
            EventClockCalibrationWindowV1(
                window_id=f"boundary-{window_index}",
                start_ns=start,
                end_ns=start + 10 * SECOND,
                events=events,
            )
        )
    return tuple(windows)


def _scenario() -> BenchmarkScenarioV1:
    return BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id="technology_epoch_03",
        severity_id="uniform-thinning-0.35",
        observation_operator_id="operator-1",
        degradation_parameters={"retention_probability": 0.35},
    )


def _degraded(start_ns: int) -> tuple[BenchmarkEventV1, ...]:
    selected = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        for index in range(5):
            bid = 1.0 + symbol_index / 10 + index * 0.0001
            selected.append(
                BenchmarkEventV1(
                    source_event_id=f"degraded-{symbol}-{index}",
                    symbol=symbol,
                    event_time_ns=start_ns + index * SECOND,
                    event_sequence=index,
                    bid=bid,
                    ask=bid + 0.0002,
                    epoch_id="technology_epoch_03",
                    session="london",
                    event_state="observed",
                    sparsity="uniform-thinning-0.35",
                    anchor_id=f"anchor-{symbol}-{index}",
                )
            )
    return tuple(selected)


@pytest.mark.parametrize("config", default_event_clock_configs())
def test_family_configs_round_trip_and_fits_are_deterministic(
    config: EventClockConfigurationV1,
) -> None:
    typed = event_clock_config_from_dict(config.to_dict())
    windows = _calibration_windows()

    first = fit_event_clock_challenger(typed, windows)
    second = fit_event_clock_challenger(typed, windows)

    assert first == second
    assert first.status is EventClockFitStatus.FITTED
    assert first.converged
    assert first.log_likelihood is not None
    assert first.fit_id.startswith("event-clock-fit:sha256:")
    assert EventClockFitResultV1.from_dict(first.to_dict()) == first
    assert EventClockFitResultV1.from_json(first.to_json()) == first
    assert first.symbols == SYMBOLS
    assert first.parameters["symbols"]


def test_family_configs_have_explicit_distinct_contracts() -> None:
    configs = default_event_clock_configs()

    assert tuple(item.family for item in configs) == (
        EventClockFamily.NHPP,
        EventClockFamily.COX,
        EventClockFamily.ACD,
        EventClockFamily.HIDDEN_MARKOV,
    )
    assert len({item.schema_version for item in configs}) == 4
    assert len({item.config_id for item in configs}) == 4
    assert isinstance(configs[0], NonHomogeneousPoissonConfigV1)
    assert isinstance(configs[1], CoxProcessConfigV1)
    assert isinstance(configs[2], AutoregressiveConditionalDurationConfigV1)
    assert isinstance(configs[3], HiddenMarkovDurationMarkConfigV1)


def test_duration_models_reset_recursions_at_calibration_boundaries() -> None:
    windows = _boundary_calibration_windows()
    acd = fit_event_clock_challenger(
        AutoregressiveConditionalDurationConfigV1(), windows
    )
    hidden = fit_event_clock_challenger(
        HiddenMarkovDurationMarkConfigV1(), windows
    )

    for model in acd.parameters["symbols"].values():
        assert model["calibration_sequence_count"] == 2
        assert model["recursion_reset_at_window_boundary"] is True
    for model in hidden.parameters["symbols"].values():
        assert model["calibration_sequence_count"] == 2
        assert model["transition_reset_at_window_boundary"] is True
        assert model["initial_probabilities"] == pytest.approx([0.5, 0.5])
        assert model["transition_matrix"][0][1] == pytest.approx(1 / 12)
        assert model["transition_matrix"][1][0] == pytest.approx(1 / 12)


def test_fit_rejects_point_in_time_leakage_without_parameters() -> None:
    windows = _calibration_windows()
    result = fit_event_clock_challenger(
        NonHomogeneousPoissonConfigV1(),
        windows,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        as_of_ns=windows[0].end_ns,
    )

    assert result.status is EventClockFitStatus.REFUSED
    assert result.failure_reason == "calibration_not_available_as_of"
    assert not result.converged
    assert result.parameters == {}


def test_generation_refuses_missing_conditioning_support() -> None:
    config = NonHomogeneousPoissonConfigV1()
    fit = fit_event_clock_challenger(config, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-conditioning",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )
    unsupported = tuple(
        replace(
            item,
            session="unseen-session",
            benchmark_event_id="",
        )
        for item in _degraded(start)
    )
    backoff = generator.generate_with_evidence(
        _degraded(start),
        scenario=replace(_scenario(), epoch_id="unseen-epoch", scenario_id=""),
        window=window,
        ensemble_member_id="member-01",
    )

    result = generator.generate_with_evidence(
        unsupported,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert backoff.evidence.conditioning_support_level == "session_backoff"
    assert result.events == ()
    assert result.evidence.status is EventClockGenerationStatus.REFUSED
    assert result.evidence.conditioning_support_level == "unsupported"
    assert "conditioning support" in (result.evidence.failure_reason or "")


def test_fit_rejects_event_and_memory_limits_without_truncating() -> None:
    windows = _calibration_windows()
    limits = EventClockResourceLimitsV1(
        max_fit_events=10,
        max_peak_memory_bytes=10_000,
    )
    result = fit_event_clock_challenger(
        NonHomogeneousPoissonConfigV1(limits=limits), windows
    )

    assert result.status is EventClockFitStatus.REFUSED
    assert result.failure_reason == "fit_event_limit"
    assert result.fitted_event_count == sum(
        len(item.events) for item in windows
    )
    assert result.parameters == {}


def test_calibration_hash_rejects_tamper_and_rows_stay_out_of_metadata() -> (
    None
):
    window = _calibration_windows()[0]
    assert window.metadata()["events_inline"] is False

    with pytest.raises(ValueError, match="content hash differs"):
        replace(window, content_sha256="0" * 64)

    with pytest.raises(ValueError, match="calibration windows only"):
        replace(window, split_kind="validation", content_sha256="")


@pytest.mark.parametrize("config", default_event_clock_configs())
def test_generation_is_synchronized_bounded_and_deterministic(
    config: EventClockConfigurationV1,
) -> None:
    typed = event_clock_config_from_dict(config.to_dict())
    fit = fit_event_clock_challenger(typed, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        typed, fit, ensemble_member_ids=("member-01", "member-02")
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-1",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )

    first = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    second = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert first.events == second.events
    assert (
        EventClockGenerationEvidenceV1.from_json(first.evidence.to_json())
        == first.evidence
    )
    assert first.evidence.status in {
        EventClockGenerationStatus.GENERATED,
        EventClockGenerationStatus.EMPTY,
    }
    assert {item.symbol for item in first.events} == set(SYMBOLS)
    assert all(item.ensemble_member_id == "member-01" for item in first.events)
    assert len(first.events) <= (
        len(_degraded(start)) + typed.limits.max_generated_events_per_window
    )
    assert generator.candidate_id == generator.candidate.candidate_id
    assert generator.candidate.parameters["fit_id"] == fit.fit_id


def test_generation_memory_is_scoped_to_operation_high_water(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = NonHomogeneousPoissonConfigV1()
    fit = fit_event_clock_challenger(config, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-memory-baseline",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )
    process_peaks = iter((600 * 1024**2, 608 * 1024**2))
    monkeypatch.setattr(
        event_clock_module, "peak_rss_bytes", lambda: next(process_peaks)
    )

    result = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert result.evidence.status in {
        EventClockGenerationStatus.GENERATED,
        EventClockGenerationStatus.EMPTY,
    }
    assert result.evidence.peak_memory_bytes == 8 * 1024**2


def test_hidden_markov_generation_uses_state_specific_marks() -> None:
    config = HiddenMarkovDurationMarkConfigV1()
    fit = fit_event_clock_challenger(config, _calibration_windows())
    parameters = json.loads(json.dumps(fit.parameters))
    for model in parameters["symbols"].values():
        model["mark_probabilities"] = [
            {"bid_only": 1.0},
            {"ask_only": 1.0},
        ]
        model["quote_profile"]["mark_probabilities"] = {"joint": 1.0}
    marked_fit = replace(fit, parameters=parameters, fit_id="")
    generator = build_fitted_event_clock_generator(
        config, marked_fit, ensemble_member_ids=("member-01",)
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-hidden-marks",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )

    result = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    proposals = tuple(
        item
        for item in result.events
        if item.sparsity.startswith("event-clock-")
    )

    assert proposals
    assert {item.event_state for item in proposals} <= {
        "ask_only",
        "bid_only",
    }


def test_generation_history_is_prior_only_bounded_and_identity_bound() -> None:
    config = NonHomogeneousPoissonConfigV1()
    fit = fit_event_clock_challenger(config, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-history",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )
    history = tuple(
        BenchmarkEventV1(
            source_event_id=f"history-{symbol}",
            symbol=symbol,
            event_time_ns=start - SECOND,
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

    generated = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=history,
    )
    leaked = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=(
            replace(history[0], event_time_ns=start, benchmark_event_id=""),
        ),
    )

    assert generated.evidence.history_event_count == len(history)
    assert leaked.events == ()
    assert leaked.evidence.status is EventClockGenerationStatus.REFUSED
    assert "prior-only" in (leaked.evidence.failure_reason or "")


def test_event_clock_batches_use_the_shared_carving_contract() -> None:
    config = NonHomogeneousPoissonConfigV1()
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:event-clock-test",
        require_fingerprint_validation=False,
    )
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source-version:event-clock-test",),
        configuration_ids=(config.config_id, constraints.constraint_set_id),
        ensemble_member_ids=("member-01",),
        base_seed=450,
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )
    fit = fit_event_clock_challenger(config, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    result = generator.generate_with_evidence(
        _degraded(start),
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
            source_version_id="source-version:event-clock-test",
            source_series_id=f"series:{item.symbol}",
            source_period="202001",
            source_row_id=index,
        )
        for index, item in enumerate(_degraded(start), start=1)
    )
    batches = build_event_clock_candidate_batches(
        run=run,
        window=window,
        config=config,
        fit_result=fit,
        generation_result=result,
        observed_events=observed,
        session_state="active",
        special_tags=("ordinary",),
    )
    tampered_observed = (
        replace(
            observed[0],
            bid=observed[0].bid + 0.00001,
            event_id="",
        ),
        *observed[1:],
    )
    with pytest.raises(ValueError, match="anchors differ"):
        build_event_clock_candidate_batches(
            run=run,
            window=window,
            config=config,
            fit_result=fit,
            generation_result=result,
            observed_events=tampered_observed,
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
        profile_source="calendar-profile:event-clock-test",
        profile_version="1.0.0",
        profile_complete=True,
        limitations=("deterministic event-clock test fixture",),
    )
    context = MarketContextQueryV1(
        timeline_id="market-context:event-clock-test",
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
    anchors = tuple(item for item in observed if item.event_id in anchor_ids)
    carved = carve_reconstruction_candidates(
        run=run,
        window=window,
        candidate_batch=batch,
        observed_events=anchors,
        market_context=context,
        constraints=constraints,
        fingerprint_evidence=None,
    )

    assert carved.status is CarvingBatchStatus.ACCEPTED
    assert carved.rejection_summary.accepted_count == len(batch.events)
    assert all(
        item.constraint_set_id == constraints.constraint_set_id
        for item in carved.accepted_events
    )


def test_generation_resource_overflow_refuses_without_partial_rows() -> None:
    limits = EventClockResourceLimitsV1(
        max_generated_events_per_interval=1,
        max_generated_events_per_window=1,
    )
    config = NonHomogeneousPoissonConfigV1(limits=limits)
    fit = fit_event_clock_challenger(config, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-1",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )

    result = generator.generate_with_evidence(
        _degraded(start),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert result.evidence.status is EventClockGenerationStatus.REFUSED
    assert (
        result.evidence.failure_reason
        == "generated interval cardinality exceeds limit"
    )
    assert result.events == ()


def test_generation_rejects_incomplete_synchronized_symbols() -> None:
    config = CoxProcessConfigV1()
    fit = fit_event_clock_challenger(config, _calibration_windows())
    generator = build_fitted_event_clock_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    start = 1_700_000_000_000_000_000
    window = ReconstructionWindowV1(
        run_id="run-1",
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=start,
        core_end_ns=start + 5 * SECOND,
    )
    incomplete = tuple(
        item for item in _degraded(start) if item.symbol != "GBPUSD"
    )

    result = generator.generate_with_evidence(
        incomplete,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )

    assert result.events == ()
    assert result.evidence.status is EventClockGenerationStatus.REFUSED
    assert result.evidence.failure_reason == (
        "every symbol requires two immutable anchors"
    )


def test_configuration_and_fit_identity_tamper_fail_closed() -> None:
    config = AutoregressiveConditionalDurationConfigV1()
    payload = config.to_dict()
    payload["coefficient_grid_size"] = 3
    with pytest.raises(ValueError, match="config_id differs"):
        event_clock_config_from_dict(payload)

    fit = fit_event_clock_challenger(config, _calibration_windows())
    fit_payload = fit.to_dict()
    fit_payload["log_likelihood"] = float(fit.log_likelihood or 0.0) + 1.0
    with pytest.raises(ValueError, match="fit_id differs"):
        EventClockFitResultV1.from_dict(fit_payload)

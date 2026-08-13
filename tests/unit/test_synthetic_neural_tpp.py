"""Bounded RMTPP dataset, training, generation, and carving contracts."""

from __future__ import annotations

import copy
import math
import time
from dataclasses import replace
from itertools import pairwise

import pytest

import histdatacom.synthetic.neural_tpp as neural_module
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
from histdatacom.synthetic.neural_tpp import (
    NeuralTPPCheckpointV1,
    NeuralTPPConfigV1,
    NeuralTPPDatasetManifestV1,
    NeuralTPPFitResultV1,
    NeuralTPPFitStatus,
    NeuralTPPGenerationEvidenceV1,
    NeuralTPPGenerationStatus,
    NeuralTPPResourceLimitsV1,
    NeuralTPPWindowContextV1,
    build_fitted_neural_tpp_generator,
    build_neural_tpp_candidate_batches,
    build_neural_tpp_protected_window,
    default_neural_tpp_config,
    fit_neural_tpp_challenger,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
SESSIONS = ("asia", "london", "new_york")
SECOND = 1_000_000_000
CALIBRATION_START = 1_600_000_000_000_000_000
GENERATION_START = 1_700_000_000_000_000_000


def _events(
    start_ns: int,
    variant: int,
    session: str,
    *,
    equal_time: bool = False,
) -> tuple[BenchmarkEventV1, ...]:
    events = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        bid = 1.0 + symbol_index / 10
        ask = bid + 0.0002
        for index in range(9 + variant):
            event_time_ns = (
                start_ns
                + (index + 1) * (60_000_000 + variant * 9_000_000)
                + (index % (2 + variant % 3)) * 2_000_000
                + symbol_index * 700_000
            )
            if equal_time and index == 1:
                event_time_ns = start_ns + 60_000_000
            if index % 4 == 0:
                bid += 0.00001
            elif index % 4 == 1:
                ask += 0.00001
            elif index % 4 == 2:
                bid += 0.00001
                ask += 0.00001
            events.append(
                BenchmarkEventV1(
                    source_event_id=f"source-{variant}-{symbol}-{index}",
                    symbol=symbol,
                    event_time_ns=event_time_ns,
                    event_sequence=index,
                    bid=bid,
                    ask=ask,
                    epoch_id="technology_epoch_03",
                    session=session,
                    event_state="observed",
                    sparsity="dense-reference",
                    anchor_id=f"anchor-{variant}-{symbol}-{index}",
                )
            )
    return tuple(events)


def _calibration() -> tuple[
    tuple[EventClockCalibrationWindowV1, ...],
    tuple[NeuralTPPWindowContextV1, ...],
]:
    windows = []
    contexts = []
    for occurrence in range(2):
        for session_index, session in enumerate(SESSIONS):
            variant = occurrence * len(SESSIONS) + session_index
            start_ns = CALIBRATION_START + variant * 100 * SECOND
            window = EventClockCalibrationWindowV1(
                window_id=f"neural-calibration-{variant}",
                start_ns=start_ns,
                end_ns=start_ns + 8 * SECOND,
                events=_events(start_ns, variant, session),
            )
            windows.append(window)
            contexts.append(_context(window.window_id, session=session))
    return tuple(windows), tuple(contexts)


def _context(
    window_id: str, *, session: str = "london"
) -> NeuralTPPWindowContextV1:
    return NeuralTPPWindowContextV1(
        window_id=window_id,
        session=session,
        technology_assignment_kind="epoch",
        technology_label="technology_epoch_03",
        feed_epoch_definition_id="feed-epoch-definition:test",
        epoch_id="technology-epoch:test",
    )


def _config() -> NeuralTPPConfigV1:
    limits = replace(
        NeuralTPPResourceLimitsV1(),
        max_history_events=1,
    )
    return replace(
        default_neural_tpp_config(),
        max_epochs=12,
        early_stopping_patience=4,
        limits=limits,
        config_id="",
    )


@pytest.fixture(scope="module")
def fitted() -> tuple[NeuralTPPConfigV1, NeuralTPPFitResultV1]:
    config = _config()
    windows, contexts = _calibration()
    fit = fit_neural_tpp_challenger(
        config,
        windows,
        window_contexts=contexts,
    )
    assert fit.status is NeuralTPPFitStatus.FITTED, fit.failure_reason
    return config, fit


def _scenario() -> BenchmarkScenarioV1:
    return BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id="technology_epoch_03",
        severity_id="uniform-thinning-0.35",
        observation_operator_id="operator:test",
        degradation_parameters={"retention_probability": 0.35},
    )


def _anchors(start_ns: int = GENERATION_START) -> tuple[BenchmarkEventV1, ...]:
    return tuple(
        BenchmarkEventV1(
            source_event_id=f"degraded-{symbol}-{index}",
            symbol=symbol,
            event_time_ns=start_ns + index * SECOND,
            event_sequence=index,
            bid=1.0 + symbol_index / 10 + index * 0.00003,
            ask=1.0002 + symbol_index / 10 + index * 0.00003,
            epoch_id="technology_epoch_03",
            session="london",
            event_state="observed",
            sparsity="uniform-thinning-0.35",
            anchor_id=f"degraded-anchor-{symbol}-{index}",
        )
        for symbol_index, symbol in enumerate(SYMBOLS)
        for index in range(5)
    )


def _run_and_window(
    config: NeuralTPPConfigV1,
    constraints: HistoricalCarvingConstraintSetV1 | None = None,
) -> tuple[ReconstructionRunV1, ReconstructionWindowV1]:
    configuration_ids = (config.config_id,)
    if constraints is not None:
        configuration_ids += (constraints.constraint_set_id,)
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source-version:neural-test",),
        configuration_ids=configuration_ids,
        ensemble_member_ids=("member-01",),
        base_seed=453,
    )
    window = ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=GENERATION_START,
        core_end_ns=GENERATION_START + 5 * SECOND,
    )
    return run, window


def test_config_dataset_checkpoint_and_fit_round_trip(
    fitted: tuple[NeuralTPPConfigV1, NeuralTPPFitResultV1],
) -> None:
    config, fit = fitted
    windows, contexts = _calibration()
    repeated = fit_neural_tpp_challenger(
        config,
        windows,
        window_contexts=contexts,
    )

    assert NeuralTPPConfigV1.from_json(config.to_json()) == config
    assert NeuralTPPFitResultV1.from_json(fit.to_json()) == fit
    assert fit.dataset_manifest is not None
    assert (
        NeuralTPPDatasetManifestV1.from_json(fit.dataset_manifest.to_json())
        == fit.dataset_manifest
    )
    assert fit.checkpoint is not None
    assert (
        NeuralTPPCheckpointV1.from_json(fit.checkpoint.to_json())
        == fit.checkpoint
    )
    assert repeated.fit_id == fit.fit_id
    assert repeated.checkpoint == fit.checkpoint
    assert repeated.training_manifest is not None
    assert fit.training_manifest is not None
    assert (
        repeated.training_manifest.training_id
        == fit.training_manifest.training_id
    )
    assert (
        fit.checkpoint.input_dimension
        == len(fit.checkpoint.mark_vocabulary) + 2
    )
    assert fit.dataset_manifest.window_count_by_role == {"train": 3, "tune": 3}
    assert fit.dataset_manifest.exact_duplicate_count == 0
    assert fit.dataset_manifest.near_duplicate_collision_count == 0
    assert fit.dataset_manifest.overlap_count == 0
    assert (
        fit.training_manifest.loss_trace[0]["tune_negative_log_likelihood"]
        > fit.tune_negative_log_likelihood
    )
    assert (
        fit.training_manifest.loss_trace[0]["tune_mark_negative_log_likelihood"]
        > fit.checkpoint.tune_mark_negative_log_likelihood
    )


def test_equal_time_ordering_uses_the_declared_minimum_elapsed() -> None:
    config = _config()
    start_ns = CALIBRATION_START
    window = EventClockCalibrationWindowV1(
        window_id="equal-time",
        start_ns=start_ns,
        end_ns=start_ns + 8 * SECOND,
        events=_events(start_ns, 0, "london", equal_time=True),
    )
    vocabulary = {
        value: index
        for index, value in enumerate(neural_module._mark_vocabulary(SYMBOLS))
    }

    sequence = neural_module._training_sequence(window, vocabulary, config)

    assert len(sequence.marks) == len(window.events)
    assert config.minimum_elapsed_seconds in sequence.durations_seconds
    assert sequence.censor_seconds > 0.0


def test_full_bptt_gradient_matches_finite_difference() -> None:
    config = _config()
    windows, contexts = _calibration()
    dataset, train, _, mean, scale = neural_module._build_dataset(
        config,
        windows,
        contexts,
        (),
        None,
        SYMBOLS,
    )
    mark_count = len(dataset.mark_vocabulary)
    parameters = neural_module._initialize_parameters(
        config,
        train,
        mark_count + 2,
        mark_count,
    )
    metrics, gradients = neural_module._loss_metrics_and_gradients(
        parameters,
        train,
        config,
        mean,
        scale,
        mark_count,
        gradients_required=True,
        deadline=time.perf_counter() + 10.0,
    )
    assert math.isfinite(metrics["negative_log_likelihood"])
    assert gradients is not None
    epsilon = 1e-6
    losses = []
    for offset in (-epsilon, epsilon):
        perturbed = copy.deepcopy(parameters)
        recurrent = perturbed["recurrent_weights"]
        assert isinstance(recurrent, list)
        assert isinstance(recurrent[0], list)
        recurrent[0][0] += offset
        measured, _ = neural_module._loss_metrics_and_gradients(
            perturbed,
            train,
            config,
            mean,
            scale,
            mark_count,
            gradients_required=False,
            deadline=time.perf_counter() + 10.0,
        )
        losses.append(measured["negative_log_likelihood"])
    finite_difference = (losses[1] - losses[0]) / (2.0 * epsilon)

    assert gradients["recurrent_weights"][0][0] == pytest.approx(
        finite_difference, rel=2e-4, abs=2e-6
    )


def test_protected_duplicate_and_future_ex_ante_data_fail_closed() -> None:
    config = _config()
    windows, contexts = _calibration()
    protected = build_neural_tpp_protected_window(
        windows[0],
        contexts[0],
        role="validation",
        symbols=SYMBOLS,
    )

    leaked = fit_neural_tpp_challenger(
        config,
        windows,
        window_contexts=contexts,
        protected_windows=(protected,),
    )
    future = fit_neural_tpp_challenger(
        config,
        windows,
        window_contexts=contexts,
        information_mode="ex_ante_simulation",
        as_of_ns=CALIBRATION_START,
    )

    assert "rows" not in protected.to_json()
    assert leaked.status is NeuralTPPFitStatus.FAILED
    assert "leakage audit" in (leaked.failure_reason or "")
    assert leaked.checkpoint is None
    assert future.status is NeuralTPPFitStatus.REFUSED
    assert "point_in_time" in (future.failure_reason or "")


def test_generation_is_deterministic_anchor_safe_and_resource_bounded(
    fitted: tuple[NeuralTPPConfigV1, NeuralTPPFitResultV1],
) -> None:
    config, fit = fitted
    _, window = _run_and_window(config)
    context = _context(window.window_id)
    generator = build_fitted_neural_tpp_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts={window.window_id: context},
    )
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

    assert first.evidence.status is NeuralTPPGenerationStatus.GENERATED
    assert first.events == second.events
    assert first.event_lineage == second.event_lineage
    assert first.evidence.semantic_seed == second.evidence.semantic_seed
    assert first.evidence.input_event_content_sha256 == (
        second.evidence.input_event_content_sha256
    )
    assert (
        NeuralTPPGenerationEvidenceV1.from_json(first.evidence.to_json())
        == first.evidence
    )
    output_anchor_ids = {
        item.benchmark_event_id
        for item in first.events
        if not item.sparsity.startswith("neural-tpp-")
    }
    assert output_anchor_ids == {item.benchmark_event_id for item in anchors}
    generated = tuple(
        item for item in first.events if item.sparsity.startswith("neural-tpp-")
    )
    assert len({item.event_time_ns for item in generated}) == len(generated)
    by_symbol = {
        symbol: sorted(
            (item for item in anchors if item.symbol == symbol),
            key=lambda item: item.event_time_ns,
        )
        for symbol in SYMBOLS
    }
    assert all(
        any(
            left.event_time_ns < event.event_time_ns < right.event_time_ns
            for left, right in pairwise(values)
        )
        for event in generated
        for values in (by_symbol[event.symbol],)
    )
    history = replace(
        anchors[0],
        source_event_id="prior-history",
        event_time_ns=window.input_start_ns - SECOND,
        benchmark_event_id="",
    )
    with_history = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=(history,),
    )
    overflow = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=(
            history,
            replace(
                history,
                source_event_id="prior-2",
                benchmark_event_id="",
            ),
        ),
    )

    assert with_history.evidence.history_event_count == 1
    assert overflow.evidence.status is NeuralTPPGenerationStatus.REFUSED
    assert overflow.events == ()
    assert overflow.event_lineage == ()


def test_checkpoint_tamper_and_generation_context_mismatch_close() -> None:
    config, fit = _config(), None
    windows, contexts = _calibration()
    fit = fit_neural_tpp_challenger(
        config,
        windows,
        window_contexts=contexts,
    )
    assert fit.checkpoint is not None
    payload = fit.checkpoint.to_dict()
    parameters = copy.deepcopy(payload["parameters"])
    assert isinstance(parameters, dict)
    parameters["time_bias"] = float(parameters["time_bias"]) + 0.1
    payload["parameters"] = parameters
    with pytest.raises(ValueError, match="checkpoint_id differs"):
        NeuralTPPCheckpointV1.from_dict(payload)

    _, window = _run_and_window(config)
    generator = build_fitted_neural_tpp_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts={window.window_id: _context(window.window_id)},
    )
    other_window = replace(
        window,
        core_start_ns=window.core_start_ns + 10 * SECOND,
        core_end_ns=window.core_end_ns + 10 * SECOND,
        window_id="",
        synchronization_unit_id="",
    )
    result = generator.generate_with_evidence(
        _anchors(other_window.core_start_ns),
        scenario=_scenario(),
        window=other_window,
        ensemble_member_id="member-01",
    )
    assert result.evidence.status is NeuralTPPGenerationStatus.FAILED
    assert result.events == ()


def test_candidate_batches_are_accepted_by_shared_carving(
    fitted: tuple[NeuralTPPConfigV1, NeuralTPPFitResultV1],
) -> None:
    config, fit = fitted
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:neural-test",
        require_fingerprint_validation=False,
    )
    run, window = _run_and_window(config, constraints)
    context = _context(window.window_id)
    generator = build_fitted_neural_tpp_generator(
        config,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts={window.window_id: context},
    )
    anchors = _anchors()
    result = generator.generate_with_evidence(
        anchors,
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
            source_version_id="source-version:neural-test",
            source_series_id=f"series:{item.symbol}",
            source_period="202001",
            source_row_id=index,
        )
        for index, item in enumerate(anchors, start=1)
    )
    batches = build_neural_tpp_candidate_batches(
        run=run,
        window=window,
        config=config,
        fit_result=fit,
        generation_result=result,
        context=context,
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
        profile_source="calendar-profile:neural-test",
        profile_version="1.0.0",
        profile_complete=True,
        limitations=("deterministic neural test fixture",),
    )
    market_context = MarketContextQueryV1(
        timeline_id="market-context:neural-test",
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
    selected_anchors = tuple(
        item for item in observed if item.event_id in anchor_ids
    )
    carved = carve_reconstruction_candidates(
        run=run,
        window=window,
        candidate_batch=batch,
        observed_events=selected_anchors,
        market_context=market_context,
        constraints=constraints,
        fingerprint_evidence=None,
    )

    assert carved.status is CarvingBatchStatus.ACCEPTED
    assert carved.rejection_summary.accepted_count == len(batch.events)
    assert all(
        item.constraint_set_id == constraints.constraint_set_id
        for item in carved.accepted_events
    )

"""Bounded marked Add-Thin fit, generation, and carving contracts."""

from __future__ import annotations

import copy
import random
from dataclasses import replace
from itertools import pairwise

import pytest

import histdatacom.synthetic.add_thin as add_thin_module
from histdatacom.market_context import (
    MarketContextCalendarStateV1,
    MarketContextMissingReason,
    MarketContextQueryStatus,
    MarketContextQueryV1,
    MarketContextView,
)
from histdatacom.synthetic.add_thin import (
    AddThinCheckpointV1,
    AddThinConfigV1,
    AddThinDatasetManifestV1,
    AddThinFitResultV1,
    AddThinFitStatus,
    AddThinGenerationError,
    AddThinGenerationEvidenceV1,
    AddThinGenerationStatus,
    AddThinProtectedWindowV1,
    AddThinResourceLimitsV1,
    AddThinWindowContextV1,
    build_add_thin_candidate_batches,
    build_add_thin_protected_window,
    build_fitted_add_thin_generator,
    default_add_thin_config,
    fit_add_thin_challenger,
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


def _context(
    window_id: str, *, session: str = "london"
) -> AddThinWindowContextV1:
    return AddThinWindowContextV1(
        window_id=window_id,
        session=session,
        technology_assignment_kind="epoch",
        technology_label="technology_epoch_03",
        feed_epoch_definition_id="feed-epoch-definition:test",
        epoch_id="feed-epoch:test:03",
    )


def _calibration(
    occurrences: int = 2,
) -> tuple[
    tuple[EventClockCalibrationWindowV1, ...],
    tuple[AddThinWindowContextV1, ...],
]:
    windows = []
    contexts = []
    for occurrence in range(occurrences):
        for session_index, session in enumerate(SESSIONS):
            variant = occurrence * len(SESSIONS) + session_index
            start_ns = CALIBRATION_START + variant * 100 * SECOND
            window = EventClockCalibrationWindowV1(
                window_id=f"add-thin-calibration-{variant}",
                start_ns=start_ns,
                end_ns=start_ns + 8 * SECOND,
                events=_events(start_ns, variant, session),
            )
            windows.append(window)
            contexts.append(_context(window.window_id, session=session))
    return tuple(windows), tuple(contexts)


def _config() -> AddThinConfigV1:
    limits = replace(AddThinResourceLimitsV1(), max_history_events=1)
    return replace(default_add_thin_config(), limits=limits, config_id="")


@pytest.fixture(scope="module")
def fitted() -> tuple[AddThinConfigV1, AddThinFitResultV1]:
    config = _config()
    windows, contexts = _calibration()
    fit = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=contexts,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )
    assert fit.status is AddThinFitStatus.FITTED, fit.failure_reason
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
    config: AddThinConfigV1,
    constraints: HistoricalCarvingConstraintSetV1 | None = None,
) -> tuple[ReconstructionRunV1, ReconstructionWindowV1]:
    configuration_ids = (config.config_id,)
    if constraints is not None:
        configuration_ids += (constraints.constraint_set_id,)
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source-version:add-thin-test",),
        configuration_ids=configuration_ids,
        ensemble_member_ids=("member-01",),
        base_seed=454,
    )
    window = ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id="member-01",
        symbols=SYMBOLS,
        core_start_ns=GENERATION_START,
        core_end_ns=GENERATION_START + 5 * SECOND,
    )
    return run, window


def test_config_dataset_checkpoint_fit_and_context_round_trip(
    fitted: tuple[AddThinConfigV1, AddThinFitResultV1],
) -> None:
    config, fit = fitted
    windows, contexts = _calibration()
    repeated = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=contexts,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )

    assert AddThinConfigV1.from_json(config.to_json()) == config
    assert AddThinFitResultV1.from_json(fit.to_json()) == fit
    assert (
        AddThinWindowContextV1.from_json(contexts[0].to_json()) == contexts[0]
    )
    assert fit.dataset_manifest is not None
    assert (
        AddThinDatasetManifestV1.from_json(fit.dataset_manifest.to_json())
        == fit.dataset_manifest
    )
    assert fit.checkpoint is not None
    assert (
        AddThinCheckpointV1.from_json(fit.checkpoint.to_json())
        == fit.checkpoint
    )
    assert repeated.fit_id == fit.fit_id
    assert repeated.checkpoint == fit.checkpoint
    assert fit.dataset_manifest.protected_window_count == 0
    assert {item.role for item in fit.dataset_manifest.windows} == {
        "train",
        "tune",
    }
    assert fit.dataset_manifest.exact_duplicate_count == 0
    assert fit.dataset_manifest.near_duplicate_collision_count == 0
    assert fit.dataset_manifest.interval_overlap_count == 0
    assert len(fit.checkpoint.mark_vocabulary) == 12
    assert (
        fit.checkpoint.tune_objective < fit.checkpoint.baseline_tune_objective
    )


def test_calibration_split_scales_beyond_the_minimum_window_count() -> None:
    config = _config()
    windows, contexts = _calibration(occurrences=4)

    fit = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=contexts,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )

    assert fit.status is AddThinFitStatus.FITTED, fit.failure_reason
    assert fit.dataset_manifest is not None
    role_counts = {
        role: sum(item.role == role for item in fit.dataset_manifest.windows)
        for role in ("train", "tune")
    }
    assert role_counts == {"train": 6, "tune": 6}


def test_forward_corruption_and_reverse_schedule_coefficients() -> None:
    config = _config()
    limits = config.limits
    clean = tuple(index % 12 for index in range(500))
    retained_total = 0
    noise_total = 0
    repetitions = 200
    for seed in range(repetitions):
        retained, missing, noise = add_thin_module._forward_corrupt_cells(
            clean,
            keep_probability=0.6,
            noise_mean=30.0,
            cell_count=12,
            rng=random.Random(seed),
            work=[0],
            limits=limits,
        )
        assert len(retained) + len(missing) == len(clean)
        assert clean == tuple(index % 12 for index in range(500))
        retained_total += len(retained)
        noise_total += len(noise)
    assert retained_total / (repetitions * len(clean)) == pytest.approx(
        0.6, abs=0.005
    )
    assert noise_total / repetitions == pytest.approx(12.0, abs=0.8)

    c_coefficient, d_coefficient, e_probability = (
        add_thin_module._reverse_coefficients(config, 1)
    )
    assert c_coefficient == pytest.approx(0.5)
    assert d_coefficient == pytest.approx(0.05)
    assert e_probability == pytest.approx(0.375)
    assert config.cumulative_keep_probabilities == pytest.approx(
        (0.8, 0.6, 0.4, 0.2)
    )
    points, collision_count = add_thin_module._sample_hpp_points(
        5,
        start_ns=0,
        end_ns=3,
        mark_count=12,
        origin="collision-fixture",
        created_step=1,
        seed=454,
        rng=random.Random(454),
        occupied=set(),
    )
    assert len(points) + collision_count == 5
    assert len({item.event_time_ns for item in points}) == len(points)
    assert collision_count >= 3
    with pytest.raises(AddThinGenerationError, match="mark is unsupported"):
        add_thin_module._project_quote(
            _anchors()[0],
            _anchors()[1],
            GENERATION_START + SECOND // 2,
            "unknown",
        )
    with pytest.raises(
        add_thin_module._AddThinRefusal,
        match="events_per_bin_limit_exceeded",
    ):
        add_thin_module._sample_cell_points(
            (20.0,),
            start_ns=0,
            end_ns=SECOND,
            bins=1,
            mark_count=1,
            origin="resource-fixture",
            created_step=1,
            seed=454,
            rng=random.Random(454),
            occupied=set(),
            work=[0],
            limits=replace(limits, max_events_per_bin=1),
        )


def test_protected_duplicate_equal_time_and_future_data_fail_closed() -> None:
    config = _config()
    windows, contexts = _calibration()
    protected = build_add_thin_protected_window(
        windows[0], contexts[0], role="validation", symbols=SYMBOLS
    )
    assert AddThinProtectedWindowV1.from_json(protected.to_json()) == protected
    assert "events" not in protected.to_json()
    leaked = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=contexts,
        protected_windows=(protected,),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )
    future = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=contexts,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        as_of_ns=CALIBRATION_START,
    )
    assert leaked.status is AddThinFitStatus.REFUSED
    assert "crosses split roles" in (leaked.failure_reason or "")
    assert leaked.checkpoint is None
    assert future.status is AddThinFitStatus.REFUSED
    assert "future events" in (future.failure_reason or "")

    as_of_ns = max(item.end_ns for item in windows)
    future_contexts = (
        replace(
            contexts[0],
            observed_context_id="observed-context:future",
            observed_context_available_ns=as_of_ns + 1,
            observed_context_used_ns=as_of_ns + 1,
            context_id="",
        ),
        *contexts[1:],
    )
    future_context = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=future_contexts,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        as_of_ns=as_of_ns,
    )
    assert future_context.status is AddThinFitStatus.REFUSED
    assert "future context" in (future_context.failure_reason or "")

    transition = AddThinWindowContextV1(
        window_id="transition-window",
        session="london",
        technology_assignment_kind="transition",
        technology_label="technology_transition_03_04",
        feed_epoch_definition_id="feed-epoch-definition:test",
        boundary_id="feed-boundary:test:03-04",
        boundary_support=0.9,
        uncertainty_start_period="202003",
        uncertainty_end_period="202004",
    )
    assert AddThinWindowContextV1.from_json(transition.to_json()) == transition
    assert transition.epoch_id is None
    with pytest.raises(ValueError, match="epoch identity"):
        replace(transition, epoch_id="feed-epoch:test:03", context_id="")

    tiny_fit_config = replace(
        config,
        limits=replace(config.limits, max_fit_events=1),
        config_id="",
    )
    resource_refusal = fit_add_thin_challenger(
        tiny_fit_config,
        windows,
        window_contexts=contexts,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )
    assert resource_refusal.status is AddThinFitStatus.REFUSED
    assert resource_refusal.dataset_manifest is None
    assert resource_refusal.checkpoint is None

    clean_protected = []
    for index, role in enumerate(("validation", "final_holdout"), start=10):
        start_ns = CALIBRATION_START + index * 100 * SECOND
        window = EventClockCalibrationWindowV1(
            window_id=f"protected-{role}",
            start_ns=start_ns,
            end_ns=start_ns + 8 * SECOND,
            events=_events(start_ns, index, "london"),
        )
        clean_protected.append(
            build_add_thin_protected_window(
                window,
                _context(window.window_id),
                role=role,
                symbols=SYMBOLS,
            )
        )
    protected_fit = fit_add_thin_challenger(
        config,
        windows,
        window_contexts=contexts,
        protected_windows=tuple(clean_protected),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )
    assert protected_fit.status is AddThinFitStatus.FITTED
    assert protected_fit.dataset_manifest is not None
    assert protected_fit.dataset_manifest.protected_windows == tuple(
        sorted(clean_protected, key=lambda item: (item.role, item.start_ns))
    )
    assert '"events"' not in protected_fit.dataset_manifest.to_json()

    equal_window = replace(
        windows[0],
        window_id="equal-time-add-thin",
        events=_events(windows[0].start_ns, 0, "asia", equal_time=True),
        content_sha256="",
    )
    vocabulary = tuple(
        f"{symbol}:{mark}"
        for symbol in SYMBOLS
        for mark in add_thin_module.MARK_STATES
    )
    rows = add_thin_module._window_marks(
        equal_window.events,
        vocabulary=vocabulary,
        start_ns=equal_window.start_ns,
        end_ns=equal_window.end_ns,
        bins=config.time_bin_count,
    )
    assert len(rows) == len(equal_window.events)


def test_generation_is_deterministic_anchor_safe_and_fully_accounted(
    fitted: tuple[AddThinConfigV1, AddThinFitResultV1],
) -> None:
    config, fit = fitted
    _, window = _run_and_window(config)
    context = _context(window.window_id)
    generator = build_fitted_add_thin_generator(
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

    assert first.evidence.status is AddThinGenerationStatus.GENERATED
    assert first.events == second.events
    assert first.event_lineage == second.event_lineage
    assert first.evidence.semantic_seed == second.evidence.semantic_seed
    assert (
        AddThinGenerationEvidenceV1.from_json(first.evidence.to_json())
        == first.evidence
    )
    assert len(first.evidence.step_evidence) == len(
        config.step_keep_probabilities
    )
    assert all(
        step.b_count + step.e_count + step.thinned_count == step.input_count
        and step.b_count + step.c_count + step.d_count + step.e_count
        == step.output_count
        for step in first.evidence.step_evidence
    )
    assert all(item.final_survival for item in first.event_lineage)
    output_anchor_ids = {
        item.benchmark_event_id
        for item in first.events
        if not item.sparsity.startswith("add-thin-")
    }
    assert output_anchor_ids == {item.benchmark_event_id for item in anchors}
    generated = tuple(
        item for item in first.events if item.sparsity.startswith("add-thin-")
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
            for left, right in pairwise(by_symbol[event.symbol])
        )
        for event in generated
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
            replace(history, source_event_id="prior-2", benchmark_event_id=""),
        ),
    )
    assert with_history.evidence.history_event_count == 1
    assert with_history.evidence.history_conditioning_scale != 1.0
    assert overflow.evidence.status is AddThinGenerationStatus.REFUSED
    assert overflow.events == ()
    assert overflow.event_lineage == ()


def test_checkpoint_tamper_context_mismatch_and_resource_refusal_close(
    fitted: tuple[AddThinConfigV1, AddThinFitResultV1],
) -> None:
    config, fit = fitted
    assert fit.checkpoint is not None
    payload = fit.checkpoint.to_dict()
    clean = copy.deepcopy(payload["clean_intensity"])
    assert isinstance(clean, list)
    assert isinstance(clean[0], list)
    clean[0][0] = float(clean[0][0]) + 0.1
    payload["clean_intensity"] = clean
    with pytest.raises(
        ValueError, match="parameter bytes differ|checkpoint_id differs"
    ):
        AddThinCheckpointV1.from_dict(payload)

    _, window = _run_and_window(config)
    generator = build_fitted_add_thin_generator(
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
    mismatch = generator.generate_with_evidence(
        _anchors(other_window.core_start_ns),
        scenario=_scenario(),
        window=other_window,
        ensemble_member_id="member-01",
    )
    assert mismatch.evidence.status is AddThinGenerationStatus.FAILED
    assert mismatch.events == ()
    mismatched_epoch = generator.generate_with_evidence(
        _anchors(),
        scenario=replace(
            _scenario(), epoch_id="technology_epoch_04", scenario_id=""
        ),
        window=window,
        ensemble_member_id="member-01",
    )
    assert mismatched_epoch.evidence.status is AddThinGenerationStatus.FAILED
    assert "scenario/context epoch differs" in (
        mismatched_epoch.evidence.failure_reason or ""
    )

    tiny_limits = replace(config.limits, max_generation_points=1)
    tiny_config = replace(config, limits=tiny_limits, config_id="")
    tiny_fit = fit_add_thin_challenger(
        tiny_config,
        _calibration()[0],
        window_contexts=_calibration()[1],
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )
    assert tiny_fit.status is AddThinFitStatus.FITTED
    _, tiny_window = _run_and_window(tiny_config)
    tiny_generator = build_fitted_add_thin_generator(
        tiny_config,
        tiny_fit,
        ensemble_member_ids=("member-01",),
        window_contexts={
            tiny_window.window_id: _context(tiny_window.window_id)
        },
    )
    refused = tiny_generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=tiny_window,
        ensemble_member_id="member-01",
    )
    assert refused.evidence.status is AddThinGenerationStatus.REFUSED
    assert refused.events == ()


def test_candidate_batches_are_accepted_by_shared_carving(
    fitted: tuple[AddThinConfigV1, AddThinFitResultV1],
) -> None:
    config, fit = fitted
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:add-thin-test",
        require_fingerprint_validation=False,
    )
    run, window = _run_and_window(config, constraints)
    context = _context(window.window_id)
    generator = build_fitted_add_thin_generator(
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
            source_version_id="source-version:add-thin-test",
            source_series_id=f"series:{item.symbol}",
            source_period="202001",
            source_row_id=index,
        )
        for index, item in enumerate(anchors, start=1)
    )
    batches = build_add_thin_candidate_batches(
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
        profile_source="calendar-profile:add-thin-test",
        profile_version="1.0.0",
        profile_complete=True,
        limitations=("deterministic Add-Thin test fixture",),
    )
    market_context = MarketContextQueryV1(
        timeline_id="market-context:add-thin-test",
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

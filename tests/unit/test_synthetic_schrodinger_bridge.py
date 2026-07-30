"""Scientific and integration tests for the bounded Schrödinger bridge."""

from __future__ import annotations

import copy
from dataclasses import replace
from itertools import pairwise

import pytest

from histdatacom.broker_capture import (
    BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION,
)
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
from histdatacom.synthetic.broker_transfer import (
    BrokerProfileSelectionV1,
    BrokerTransferConfigV1,
    BrokerTransferStatus,
)
from histdatacom.synthetic.carving import (
    CarvingBatchStatus,
    HistoricalCarvingConstraintSetV1,
    HistoricalCarvingQuarantineV1,
    ReconstructionCandidateBatchV1,
    carve_reconstruction_candidates,
)
from histdatacom.synthetic.contracts import SyntheticEventV1
from histdatacom.synthetic.event_clock import EventClockCalibrationWindowV1
from histdatacom.synthetic.schrodinger_bridge import (
    SB_PROMOTION_POLICY,
    SchrodingerBridgeBrokerTargetV1,
    SchrodingerBridgeCheckpointV1,
    SchrodingerBridgeConfigV1,
    SchrodingerBridgeDatasetManifestV1,
    SchrodingerBridgeFitResultV1,
    SchrodingerBridgeFitStatus,
    SchrodingerBridgeGenerationEvidenceV1,
    SchrodingerBridgeGenerationStatus,
    SchrodingerBridgeResourceLimitsV1,
    SchrodingerBridgeWindowContextV1,
    _project_quote,
    build_fitted_schrodinger_bridge_generator,
    build_schrodinger_bridge_broker_target,
    build_schrodinger_bridge_candidate_batches,
    build_schrodinger_bridge_protected_window,
    default_schrodinger_bridge_config,
    fit_schrodinger_bridge_challenger,
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
    start_ns: int, variant: int, session: str
) -> tuple[BenchmarkEventV1, ...]:
    events: list[BenchmarkEventV1] = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        bid = 1.0 + symbol_index / 10
        ask = bid + 0.0002
        for index in range(12):
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
                    event_time_ns=(
                        start_ns
                        + (index + 1) * 550_000_000
                        + variant * 2_000_000
                        + symbol_index * 700_000
                    ),
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
    window_id: str,
    *,
    session: str = "london",
) -> SchrodingerBridgeWindowContextV1:
    return SchrodingerBridgeWindowContextV1(
        window_id=window_id,
        session=session,
        technology_assignment_kind="epoch",
        technology_label="technology_epoch_03",
        feed_epoch_definition_id="feed-epoch-definition:test",
        epoch_id="feed-epoch:test:03",
    )


def _calibration() -> tuple[
    tuple[EventClockCalibrationWindowV1, ...],
    tuple[SchrodingerBridgeWindowContextV1, ...],
]:
    windows = []
    contexts = []
    for occurrence in range(2):
        for session_index, session in enumerate(SESSIONS):
            variant = occurrence * len(SESSIONS) + session_index
            start_ns = CALIBRATION_START + variant * 100 * SECOND
            window = EventClockCalibrationWindowV1(
                window_id=f"bridge-calibration-{variant}",
                start_ns=start_ns,
                end_ns=start_ns + 8 * SECOND,
                events=_events(start_ns, variant, session),
            )
            windows.append(window)
            contexts.append(_context(window.window_id, session=session))
    return tuple(windows), tuple(contexts)


def _selection() -> BrokerProfileSelectionV1:
    metrics = {
        "active_quote_interarrival_ns": 100_000_000.0,
        "spread": 0.00018,
        "stale_quote_rate": 0.05,
        "exact_duplicate_rate": 0.01,
        "burst_interval_rate": 0.08,
        "quiet_interval_rate": 0.04,
    }
    return BrokerProfileSelectionV1(
        fingerprint_id="broker-fingerprint:test",
        fingerprint_schema_version=BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION,
        requested_condition={"symbol": "EURUSD", "session": "london"},
        requested_condition_id="broker-condition:requested",
        effective_condition_id="broker-condition:effective",
        support_status="supported",
        status=BrokerTransferStatus.APPLIED,
        selected_at_utc_ns=GENERATION_START - SECOND,
        profile_effective_start_utc_ns=CALIBRATION_START - SECOND,
        profile_effective_end_utc_ns=None,
        supersedes_fingerprint_id=None,
        metrics=metrics,
        metric_condition_ids={
            name: "broker-condition:effective" for name in metrics
        },
    )


def _config() -> SchrodingerBridgeConfigV1:
    limits = replace(
        SchrodingerBridgeResourceLimitsV1(),
        max_history_events=2,
        max_solver_iterations=600,
    )
    return replace(
        default_schrodinger_bridge_config(),
        time_bin_count=4,
        sinkhorn_tolerance=1e-8,
        limits=limits,
        config_id="",
    )


def _target(
    config: SchrodingerBridgeConfigV1,
) -> SchrodingerBridgeBrokerTargetV1:
    windows, _ = _calibration()
    return build_schrodinger_bridge_broker_target(
        _selection(),
        BrokerTransferConfigV1(strength=0.5),
        windows,
        time_bin_count=config.time_bin_count,
    )


@pytest.fixture(scope="module")
def fitted() -> tuple[
    SchrodingerBridgeConfigV1,
    SchrodingerBridgeBrokerTargetV1,
    SchrodingerBridgeFitResultV1,
]:
    config = _config()
    target = _target(config)
    windows, contexts = _calibration()
    fit = fit_schrodinger_bridge_challenger(
        config,
        target,
        windows,
        window_contexts=contexts,
    )
    assert fit.status is SchrodingerBridgeFitStatus.FITTED, fit.failure_reason
    return config, target, fit


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
        for index in range(6)
    )


def test_triangle_projection_compares_against_the_marked_proposal() -> None:
    config = _config()
    target = _target(config)
    quotes = {
        "EURGBP": ((0.7999, 0.8001), (0.7999, 0.8001)),
        "EURUSD": ((0.9999, 1.0001), (0.9990, 1.0010)),
        "GBPUSD": ((1.2499, 1.2501), (1.2499, 1.2501)),
    }
    anchors = {
        symbol: tuple(
            BenchmarkEventV1(
                source_event_id=f"projection-{symbol}-{index}",
                symbol=symbol,
                event_time_ns=GENERATION_START + index * SECOND,
                event_sequence=index,
                bid=bid,
                ask=ask,
                epoch_id="technology_epoch_03",
                session="london",
                event_state="observed",
                sparsity="dense-reference",
                anchor_id=f"projection-anchor-{symbol}-{index}",
            )
            for index, (bid, ask) in enumerate(values)
        )
        for symbol, values in quotes.items()
    }

    _, _, _, _, before, after = _project_quote(
        anchors,
        symbol="EURUSD",
        mark="ask_only",
        event_time_ns=GENERATION_START + SECOND // 2,
        target=target,
        config=config,
    )

    assert before > 0.0
    assert after < before


def _run_and_window(
    config: SchrodingerBridgeConfigV1,
    target: SchrodingerBridgeBrokerTargetV1,
    constraints: HistoricalCarvingConstraintSetV1 | None = None,
    *,
    member_id: str = "member-01",
) -> tuple[ReconstructionRunV1, ReconstructionWindowV1]:
    configuration_ids: tuple[str, ...] = (config.config_id, target.target_id)
    if constraints is not None:
        configuration_ids += (constraints.constraint_set_id,)
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source-version:bridge-test",),
        configuration_ids=configuration_ids,
        ensemble_member_ids=("member-01", "member-02"),
        base_seed=455,
    )
    return run, ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id=member_id,
        symbols=SYMBOLS,
        core_start_ns=GENERATION_START,
        core_end_ns=GENERATION_START + 6 * SECOND,
    )


def test_contracts_solver_hypothesis_and_protected_boundary_round_trip(
    fitted: tuple[
        SchrodingerBridgeConfigV1,
        SchrodingerBridgeBrokerTargetV1,
        SchrodingerBridgeFitResultV1,
    ],
) -> None:
    config, target, fit = fitted
    windows, contexts = _calibration()
    protected_window = EventClockCalibrationWindowV1(
        window_id="bridge-protected",
        start_ns=CALIBRATION_START + 10_000 * SECOND,
        end_ns=CALIBRATION_START + 10_008 * SECOND,
        events=_events(CALIBRATION_START + 10_000 * SECOND, 99, "london"),
    )
    protected = build_schrodinger_bridge_protected_window(
        protected_window,
        _context(protected_window.window_id),
        role="validation",
    )
    with_protected = fit_schrodinger_bridge_challenger(
        config,
        target,
        windows,
        window_contexts=contexts,
        protected_windows=(protected,),
    )

    assert SchrodingerBridgeConfigV1.from_json(config.to_json()) == config
    assert SchrodingerBridgeBrokerTargetV1.from_json(target.to_json()) == target
    assert target.broker_support_status == "supported"
    assert target.selected_at_utc_ns == _selection().selected_at_utc_ns
    stale_selection = replace(
        _selection(),
        profile_effective_end_utc_ns=_selection().selected_at_utc_ns,
        selection_id="",
    )
    with pytest.raises(ValueError, match="stale broker profile"):
        build_schrodinger_bridge_broker_target(
            stale_selection,
            BrokerTransferConfigV1(strength=0.5),
            windows,
            time_bin_count=config.time_bin_count,
        )
    assert SchrodingerBridgeFitResultV1.from_json(fit.to_json()) == fit
    assert fit.dataset_manifest is not None
    assert fit.checkpoint is not None
    assert (
        SchrodingerBridgeDatasetManifestV1.from_json(
            fit.dataset_manifest.to_json()
        )
        == fit.dataset_manifest
    )
    assert (
        SchrodingerBridgeCheckpointV1.from_json(fit.checkpoint.to_json())
        == fit.checkpoint
    )
    solver = fit.checkpoint.solver_evidence
    assert solver.converged
    assert solver.maximum_marginal_residual <= config.sinkhorn_tolerance
    assert solver.support_missing_count == 0
    assert solver.solver_work > 0
    assert solver.minimum_positive_kernel > 0.0
    assert solver.maximum_scaling >= solver.minimum_positive_scaling > 0.0
    assert solver.quantization_mean_abs_error > 0.0
    assert solver.window_boundary_transition_l1 >= 0.0
    volatile_solver = replace(
        solver,
        wall_time_ms=solver.wall_time_ms + 1,
        peak_memory_bytes=solver.peak_memory_bytes + 1,
    )
    volatile_checkpoint = replace(
        fit.checkpoint,
        solver_evidence=volatile_solver,
        checkpoint_id="",
    )
    assert volatile_checkpoint.checkpoint_id == fit.checkpoint.checkpoint_id
    volatile_fit = replace(
        fit,
        checkpoint=volatile_checkpoint,
        solver_evidence=volatile_solver,
        fit_wall_time_ms=fit.fit_wall_time_ms + 1,
        fit_peak_memory_bytes=fit.fit_peak_memory_bytes + 1,
        fit_id="",
    )
    assert volatile_fit.fit_id == fit.fit_id
    assert SchrodingerBridgeFitResultV1.from_json(volatile_fit.to_json()) == (
        volatile_fit
    )
    assert fit.checkpoint.tune_joint_nll >= 0.0
    assert fit.checkpoint.source_iid_tune_nll >= 0.0
    assert fit.checkpoint.uniform_tune_nll > 0.0
    assert (
        fit.to_dict()["hypothesis_id"]
        == "issue-455-joint-heldout-improvement-v1"
    )
    assert {
        "linear-interpolation",
        "empirical-motif",
        "acd-1-1-exponential",
        "marked-hawkes-diagonal-self-excitation",
        "regime-hawkes-baseline-and-excitation",
    }.issubset(set(fit.to_dict()["transparent_comparators"]))
    assert fit.to_dict()["promotion_policy"] == SB_PROMOTION_POLICY
    assert fit.to_dict()["automatic_winner"] is False
    assert with_protected.status is SchrodingerBridgeFitStatus.FITTED
    assert with_protected.dataset_manifest is not None
    assert with_protected.dataset_manifest.protected_window_count == 1
    assert (
        with_protected.dataset_manifest.protected_windows[0].to_dict()[
            "rows_inline"
        ]
        is False
    )


def test_non_convergence_off_support_and_tampering_fail_closed() -> None:
    windows, contexts = _calibration()
    base = _config()
    target = _target(base)
    nonconverged_config = replace(
        base,
        sinkhorn_max_iterations=1,
        sinkhorn_tolerance=1e-15,
        config_id="",
    )
    nonconverged = fit_schrodinger_bridge_challenger(
        nonconverged_config,
        target,
        windows,
        window_contexts=contexts,
    )
    assert nonconverged.status is SchrodingerBridgeFitStatus.REFUSED
    assert nonconverged.checkpoint is None
    assert nonconverged.solver_evidence is not None
    assert not nonconverged.solver_evidence.converged
    assert nonconverged.solver_evidence.residual_trace
    assert "sinkhorn_non_convergence" in (nonconverged.failure_reason or "")

    unsupported_config = replace(
        base,
        transition_smoothing=0.0,
        maximum_transport_cost=0.0,
        config_id="",
    )
    unsupported_target = replace(
        target,
        transfer_strength=1.0,
        target_id="",
    )
    unsupported = fit_schrodinger_bridge_challenger(
        unsupported_config,
        unsupported_target,
        windows,
        window_contexts=contexts,
    )
    assert unsupported.status is SchrodingerBridgeFitStatus.REFUSED
    assert unsupported.checkpoint is None
    assert unsupported.solver_evidence is not None
    assert unsupported.solver_evidence.support_missing_count > 0
    assert "endpoint_off_support" in (unsupported.failure_reason or "")

    fitted_result = fit_schrodinger_bridge_challenger(
        base,
        target,
        windows,
        window_contexts=contexts,
    )
    assert fitted_result.checkpoint is not None
    payload = fitted_result.checkpoint.to_dict()
    coupling = copy.deepcopy(payload["endpoint_coupling"])
    assert isinstance(coupling, list) and isinstance(coupling[0], list)
    coupling[0][0] = float(coupling[0][0]) + 0.1
    payload["endpoint_coupling"] = coupling
    with pytest.raises(ValueError, match="checkpoint_id differs"):
        SchrodingerBridgeCheckpointV1.from_dict(payload)


def test_generation_is_deterministic_anchor_safe_triangle_conditioned_and_streaming_bounded(
    fitted: tuple[
        SchrodingerBridgeConfigV1,
        SchrodingerBridgeBrokerTargetV1,
        SchrodingerBridgeFitResultV1,
    ],
) -> None:
    config, target, fit = fitted
    _, window = _run_and_window(config, target)
    context = _context(window.window_id)
    generator = build_fitted_schrodinger_bridge_generator(
        config,
        target,
        fit,
        ensemble_member_ids=("member-01", "member-02"),
        window_contexts={window.window_id: context},
    )
    anchors = _anchors()
    first = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    repeated = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    _, second_window = _run_and_window(config, target, member_id="member-02")
    second_context = _context(second_window.window_id)
    second_generator = build_fitted_schrodinger_bridge_generator(
        config,
        target,
        fit,
        ensemble_member_ids=("member-01", "member-02"),
        window_contexts={second_window.window_id: second_context},
    )
    second_member = second_generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=second_window,
        ensemble_member_id="member-02",
    )

    assert first.evidence.status is SchrodingerBridgeGenerationStatus.GENERATED
    assert first.events == repeated.events
    assert first.event_lineage == repeated.event_lineage
    assert (
        second_member.evidence.status
        is SchrodingerBridgeGenerationStatus.GENERATED
    )
    assert first.event_lineage != second_member.event_lineage
    assert (
        SchrodingerBridgeGenerationEvidenceV1.from_json(
            first.evidence.to_json()
        )
        == first.evidence
    )
    output_anchor_ids = {
        item.benchmark_event_id
        for item in first.events
        if not item.sparsity.startswith("schrodinger-bridge-")
    }
    assert output_anchor_ids == {item.benchmark_event_id for item in anchors}
    generated = tuple(
        item
        for item in first.events
        if item.sparsity.startswith("schrodinger-bridge-")
    )
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
    assert all(
        len(item.state_path) == config.bridge_steps + 1
        and item.state_path[0] == item.source_state
        and item.state_path[-1] == item.target_state
        and item.triangle_residual_after
        <= item.triangle_residual_before + 1e-12
        for item in first.event_lineage
    )
    asynchronous_anchors = tuple(
        item
        for item in anchors
        if not (
            item.symbol == "EURGBP" and item.event_time_ns == GENERATION_START
        )
    )
    asynchronous = generator.generate_with_evidence(
        asynchronous_anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    assert asynchronous.evidence.status in {
        SchrodingerBridgeGenerationStatus.GENERATED,
        SchrodingerBridgeGenerationStatus.EMPTY,
    }
    assert asynchronous.evidence.skipped_outside_anchor_count > 0
    history = replace(
        anchors[0],
        source_event_id="strict-prior-history",
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
    assert with_history.evidence.history_event_count == 1
    assert with_history.evidence.boundary_conditioning_l1 > 0.0
    future_history = generator.generate_with_evidence(
        anchors,
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
        history_events=(
            replace(
                history,
                event_time_ns=window.core_start_ns,
                benchmark_event_id="",
            ),
        ),
    )
    assert (
        future_history.evidence.status
        is SchrodingerBridgeGenerationStatus.FAILED
    )
    assert future_history.events == ()


def test_forbidden_intervals_are_exact_and_not_posthoc_filters(
    fitted: tuple[
        SchrodingerBridgeConfigV1,
        SchrodingerBridgeBrokerTargetV1,
        SchrodingerBridgeFitResultV1,
    ],
) -> None:
    config, target, fit = fitted
    _, window = _run_and_window(config, target)
    quarantines = tuple(
        HistoricalCarvingQuarantineV1(
            symbol=symbol,
            start_ns=window.core_start_ns,
            end_ns=window.core_end_ns,
            reason="forbidden-test-window",
            source_id="quarantine-source:test",
        )
        for symbol in SYMBOLS
    )
    generator = build_fitted_schrodinger_bridge_generator(
        config,
        target,
        fit,
        ensemble_member_ids=("member-01",),
        window_contexts={window.window_id: _context(window.window_id)},
        quarantines=quarantines,
    )
    result = generator.generate_with_evidence(
        _anchors(),
        scenario=_scenario(),
        window=window,
        ensemble_member_id="member-01",
    )
    assert result.evidence.status is SchrodingerBridgeGenerationStatus.EMPTY
    assert result.evidence.generated_event_count == 0
    assert result.evidence.skipped_quarantine_count > 0
    assert result.events == tuple(
        sorted(
            _anchors(),
            key=lambda item: (
                item.event_time_ns,
                item.symbol,
                item.event_sequence,
                item.source_event_id,
            ),
        )
    )


def test_candidate_batches_are_accepted_by_shared_carving(
    fitted: tuple[
        SchrodingerBridgeConfigV1,
        SchrodingerBridgeBrokerTargetV1,
        SchrodingerBridgeFitResultV1,
    ],
) -> None:
    config, target, fit = fitted
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:bridge-test",
        require_fingerprint_validation=False,
    )
    run, window = _run_and_window(config, target, constraints)
    context = _context(window.window_id)
    generator = build_fitted_schrodinger_bridge_generator(
        config,
        target,
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
            source_version_id="source-version:bridge-test",
            source_series_id=f"series:{item.symbol}",
            source_period="202001",
            source_row_id=index,
        )
        for index, item in enumerate(anchors, start=1)
    )
    batches = build_schrodinger_bridge_candidate_batches(
        run=run,
        window=window,
        config=config,
        broker_target=target,
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
        profile_source="calendar-profile:bridge-test",
        profile_version="1.0.0",
        profile_complete=True,
        limitations=("deterministic bridge test fixture",),
    )
    market_context = MarketContextQueryV1(
        timeline_id="market-context:bridge-test",
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

"""Marked Hawkes contracts, stability, synchronized generation, and carving."""

from __future__ import annotations

import json
import random
from dataclasses import replace
from itertools import pairwise

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
    LEGACY_MARK_POLICY,
    LEGACY_UNBOUNDED_CARDINALITY_POLICY,
    OPERATOR_CONDITIONED_CARDINALITY_POLICY,
    TRANSITION_CONDITIONED_MARK_POLICY,
    HawkesExcitationStructure,
    MarkedHawkesCandidateLineageV1,
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


def test_operator_conditioned_count_predictive_is_deterministic_and_variable() -> (
    None
):
    def draw(seed: int) -> tuple[int, ...]:
        rng = random.Random(seed)
        return tuple(
            hawkes_module._sample_negative_binomial_failures(4, 0.35, rng)
            for _ in range(32)
        )

    draws = draw(451)

    assert draws == draw(451)
    assert len(set(draws)) > 1
    assert all(value >= 0 for value in draws)

    config = MarkedHawkesConfigV1(HawkesExcitationStructure.DIAGONAL)
    first = hawkes_module._operator_conditioned_missing_counts(
        config,
        _anchors(),
        scenario=_scenario(),
        ensemble_member_id="member-01",
    )
    replay = hawkes_module._operator_conditioned_missing_counts(
        config,
        _anchors(),
        scenario=_scenario(),
        ensemble_member_id="member-01",
    )
    ensemble = {
        tuple(
            sorted(
                hawkes_module._operator_conditioned_missing_counts(
                    config,
                    _anchors(),
                    scenario=_scenario(),
                    ensemble_member_id=f"member-{index:02d}",
                ).items()
            )
        )
        for index in range(1, 17)
    }

    assert first == replay
    assert len(ensemble) > 1


def test_operator_conditioned_identity_with_only_fixed_anchors_is_empty() -> (
    None
):
    """A zero modeled deficit is a successful empty generation, not refusal."""
    config = MarkedHawkesConfigV1(HawkesExcitationStructure.DIAGONAL)
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    generator = build_fitted_marked_hawkes_generator(
        config, fit, ensemble_member_ids=("member-01",)
    )
    anchors = _events(GENERATION_START)
    scenario = BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id="technology_epoch_03",
        severity_id="dense-identity",
        observation_operator_id="operator-identity",
        degradation_parameters={"retention_probability": 1.0},
    )

    assert (
        hawkes_module._sample_negative_binomial_failures(
            0, 1.0, random.Random(451)
        )
        == 0
    )
    assert hawkes_module._operator_conditioned_missing_counts(
        config,
        anchors,
        scenario=scenario,
        ensemble_member_id="member-01",
    ) == dict.fromkeys(SYMBOLS, 0)

    result = generator.generate_with_evidence(
        anchors,
        scenario=scenario,
        window=_window(),
        ensemble_member_id="member-01",
    )

    assert result.evidence.status is MarkedHawkesGenerationStatus.EMPTY
    assert result.evidence.generated_event_count == 0
    assert result.event_lineage == ()
    assert result.events == tuple(
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


def test_operator_conditioned_count_predictive_fails_closed_at_resource_bound() -> (
    None
):
    with pytest.raises(
        hawkes_module.MarkedHawkesGenerationError,
        match="exceeds generation bound",
    ):
        hawkes_module._sample_negative_binomial_failures(
            4,
            1e-12,
            random.Random(451),
            maximum_failures=5,
        )

    config = MarkedHawkesConfigV1(
        HawkesExcitationStructure.DIAGONAL,
        limits=replace(
            hawkes_module.MarkedHawkesResourceLimitsV1(),
            max_generated_events_per_interval=5,
            max_generated_events_per_window=5,
            max_candidate_amplification=1.0,
            limits_id="",
        ),
    )
    scenario = BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.VALIDATION,
        epoch_id="technology_epoch_03",
        severity_id="uniform-thinning-near-zero",
        observation_operator_id="operator-1",
        degradation_parameters={"retention_probability": 1e-12},
    )
    with pytest.raises(
        hawkes_module.MarkedHawkesGenerationError,
        match="exceeds generation bound",
    ):
        hawkes_module._operator_conditioned_missing_counts(
            config,
            _anchors(),
            scenario=scenario,
            ensemble_member_id="member-01",
        )


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
            anchor_id=(
                f"anchor-{epoch_id}-{symbol}-{index}"
                if index in {0, 4}
                else None
            ),
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
    assert all(item.limits.max_fit_events >= 32 * 3 * 256 for item in configs)
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


def test_transition_conditioned_policy_is_default_and_legacy_fits_replay() -> (
    None
):
    current = MarkedHawkesConfigV1(HawkesExcitationStructure.DIAGONAL)
    assert current.mark_policy == TRANSITION_CONDITIONED_MARK_POLICY
    assert current.cardinality_policy == OPERATOR_CONDITIONED_CARDINALITY_POLICY
    current_fit = fit_marked_hawkes_challenger(current, _calibration_windows())
    assert current_fit.parameters["mark_policy"] == current.mark_policy
    for model in current_fit.parameters["conditioning_models"].values():
        assert set(model["mark_transition_counts"]) == set(SYMBOLS)

    legacy = MarkedHawkesConfigV1(
        HawkesExcitationStructure.DIAGONAL,
        mark_policy=LEGACY_MARK_POLICY,
        cardinality_policy=LEGACY_UNBOUNDED_CARDINALITY_POLICY,
    )
    legacy_payload = legacy.to_dict()
    assert "cardinality_policy" not in legacy_payload
    assert "cardinality_estimator" not in legacy_payload
    assert "conditional_path_draw_limit" not in legacy_payload
    assert "conditional_oversample_factor" not in legacy_payload
    assert MarkedHawkesConfigV1.from_dict(legacy_payload) == legacy
    legacy_fit = fit_marked_hawkes_challenger(legacy, _calibration_windows())
    legacy_parameters = json.loads(json.dumps(legacy_fit.parameters))
    legacy_parameters.pop("mark_policy")
    old_shape = replace(legacy_fit, parameters=legacy_parameters, fit_id="")

    assert MarkedHawkesFitResultV1.from_json(old_shape.to_json()) == old_shape


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
    changed_telemetry = replace(
        first.evidence,
        wall_time_ms=first.evidence.wall_time_ms + 1,
        peak_memory_bytes=first.evidence.peak_memory_bytes + 1,
        evidence_id="",
    )
    assert changed_telemetry.evidence_id == first.evidence.evidence_id
    assert (
        changed_telemetry.to_dict()["wall_time_ms"]
        == first.evidence.wall_time_ms + 1
    )
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
    assert proposals
    assert len(proposals) <= 45
    assert {item.symbol for item in proposals} <= set(SYMBOLS)
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
            for left, right in pairwise(symbol_anchors)
        )
    by_lineage = {item.source_event_id: item for item in first.event_lineage}
    for symbol in SYMBOLS:
        ordered = sorted(
            (item for item in first.events if item.symbol == symbol),
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.benchmark_event_id,
            ),
        )
        for previous, event in pairwise(ordered):
            if event.source_event_id not in by_lineage:
                continue
            bid_changed = event.bid != previous.bid
            ask_changed = event.ask != previous.ask
            realized = (
                "joint"
                if bid_changed and ask_changed
                else (
                    "bid_only"
                    if bid_changed
                    else "ask_only" if ask_changed else "unchanged"
                )
            )
            assert by_lineage[event.source_event_id].event_state == realized


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
    config = MarkedHawkesConfigV1(
        HawkesExcitationStructure.FULL,
        cardinality_policy=LEGACY_UNBOUNDED_CARDINALITY_POLICY,
        limits=limits,
    )
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


def test_operator_path_draw_truncates_at_candidate_bound_without_partial_leak() -> (
    None
):
    limits = MarkedHawkesResourceLimitsV1(
        max_generated_events_per_interval=1,
        max_generated_events_per_window=1,
    )
    config = MarkedHawkesConfigV1(
        HawkesExcitationStructure.FULL,
        limits=limits,
    )
    fit = fit_marked_hawkes_challenger(config, _calibration_windows())
    _, _, model = hawkes_module._conditioning_model(
        fit,
        _anchors(),
        _scenario(),
    )

    events, lineages, _ = hawkes_module._simulate_events(
        config,
        fit,
        model,
        _anchors(),
        scenario=_scenario(),
        window=_window(),
        ensemble_member_id="member-01",
        history_events=(),
        proposal_counter=[0],
        missing_scale_override=1.0,
        truncate_at_candidate_bound=True,
    )

    assert len(lineages) <= 1
    assert len(events) == len(_anchors()) + len(lineages)


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
    assert {item.source_version_id for item in batch.events} == {
        "source-version:hawkes-test"
    }
    assert (
        tuple(
            MarkedHawkesCandidateLineageV1.from_dict(item.to_dict())
            for item in batch.event_lineage
        )
        == batch.event_lineage
    )
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

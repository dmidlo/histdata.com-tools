"""Regression tests for bounded reconstructed-history strategy sensitivity."""

from __future__ import annotations

from dataclasses import replace
from typing import Sequence

import pytest

from histdatacom.synthetic import (
    ReferenceMomentumStrategyV1,
    StrategyEvaluationCaseV1,
    StrategyEvaluationFailure,
    StrategyEvaluationPlanV1,
    StrategyEvaluationPolicyV1,
    StrategyExecutionSpecificationV1,
    StrategyQuoteV1,
    StrategySensitivityReportV1,
    StrategySignalStateV1,
    StrategySignalV1,
    StrategySourceKind,
    StrategyWindowStatus,
    evaluate_strategy_sensitivity,
    strategy_sensitivity_benchmark_hooks,
)
from histdatacom.synthetic.benchmark import BenchmarkEventV1
from histdatacom.synthetic.information import (
    InformationAuditReportV1,
    InformationMode,
)
from tests.unit.test_synthetic_bars import _derive_boundary_bars
from tests.unit.test_synthetic_contracts import _generated, _observed

RUN_ID = "reconstruction-run:sha256:strategy-fixture"
MANIFEST_ID = "information-manifest:sha256:strategy-fixture"
SECOND = 1_000_000_000


def _audit(
    mode: InformationMode = InformationMode.EX_ANTE_SIMULATION,
    *,
    strategy_valid: bool | None = None,
) -> InformationAuditReportV1:
    if strategy_valid is None:
        strategy_valid = mode is InformationMode.EX_ANTE_SIMULATION
    return InformationAuditReportV1(
        run_id=RUN_ID,
        policy_id="information-policy:sha256:strategy-fixture",
        manifest_id=MANIFEST_ID,
        window_plan_id="window-plan:sha256:strategy-fixture",
        information_mode=mode,
        accepted=True,
        total_violation_count=0,
        findings=(),
        evidence_truncated=False,
        valid_for=("strategy_usefulness_claims",) if strategy_valid else (),
        invalid_for=() if strategy_valid else ("strategy_usefulness_claims",),
        summary="accepted fixture information boundary",
    )


def _case(
    source_kind: StrategySourceKind,
    *,
    audit: InformationAuditReportV1,
    member: str,
    alignment: str = "window-a",
    start_ns: int = 0,
    end_ns: int = 8 * SECOND,
    broker_profile_id: str | None = None,
    invalid_for_backtest_reason: str | None = None,
    source_scope: str | None = None,
    bar_interval_code: str | None = None,
) -> StrategyEvaluationCaseV1:
    return StrategyEvaluationCaseV1(
        run_id=RUN_ID,
        alignment_window_id=alignment,
        source_kind=source_kind,
        source_artifact_id=f"artifact:{source_kind.value}:{member}:{alignment}",
        symbol="EURUSD",
        start_ns=start_ns,
        end_ns=end_ns,
        information_mode=audit.information_mode,
        information_manifest_id=MANIFEST_ID,
        information_audit_id=audit.audit_id,
        ensemble_member_id=member,
        broker_profile_id=broker_profile_id,
        source_scope=source_scope,
        bar_interval_code=bar_interval_code,
        invalid_for_backtest_reason=invalid_for_backtest_reason,
    )


def _engine() -> ReferenceMomentumStrategyV1:
    return ReferenceMomentumStrategyV1(
        lookback_ns=SECOND,
        decision_interval_ns=20 * SECOND,
        threshold_bps=0.0,
    )


def _policy(
    *,
    horizons_ns: tuple[int, ...] = (SECOND,),
    max_quotes_per_window: int = 100,
    max_payload_bytes: int = 4_194_304,
) -> StrategyEvaluationPolicyV1:
    return StrategyEvaluationPolicyV1(
        horizons_ns=horizons_ns,
        max_cases=32,
        max_quotes_per_window=max_quotes_per_window,
        max_signals_per_window=16,
        max_pending_signals=16,
        max_slices=128,
        max_payload_bytes=max_payload_bytes,
    )


def _plan(
    cases: tuple[StrategyEvaluationCaseV1, ...],
    *,
    engine: ReferenceMomentumStrategyV1 | None = None,
    execution: StrategyExecutionSpecificationV1 | None = None,
    policy: StrategyEvaluationPolicyV1 | None = None,
    invalid_for_backtest_reason: str | None = None,
) -> StrategyEvaluationPlanV1:
    selected = engine or _engine()
    return StrategyEvaluationPlanV1(
        run_id=RUN_ID,
        strategy=selected.specification,
        execution=execution or StrategyExecutionSpecificationV1(),
        policy=policy or _policy(),
        cases=cases,
        invalid_for_backtest_reason=invalid_for_backtest_reason,
    )


def _quotes(
    member: str,
    values: Sequence[float],
    *,
    times: Sequence[int] | None = None,
    epoch: str = "modern",
    session: str = "london",
    event_state: str = "quiet",
    sparsity: str = "dense",
    broker_profile_id: str | None = None,
    spread: float = 0.0,
) -> tuple[StrategyQuoteV1, ...]:
    selected_times = times or tuple(
        index * SECOND for index in range(len(values))
    )
    return tuple(
        StrategyQuoteV1(
            source_event_id=f"{member}:{index}",
            symbol="eurusd",
            event_time_ns=selected_times[index],
            event_sequence=0,
            bid=value,
            ask=value + spread,
            epoch_id=epoch,
            session=session,
            event_state=event_state,
            sparsity=sparsity,
            ensemble_member_id=member,
            broker_profile_id=broker_profile_id,
        )
        for index, value in enumerate(values)
    )


def _three_cases(
    audit: InformationAuditReportV1,
) -> tuple[StrategyEvaluationCaseV1, ...]:
    return (
        _case(StrategySourceKind.OBSERVED, audit=audit, member="reference"),
        _case(
            StrategySourceKind.DEGRADED_HOLDOUT,
            audit=audit,
            member="degraded",
        ),
        _case(
            StrategySourceKind.RECONSTRUCTED,
            audit=audit,
            member="member-a",
        ),
    )


def _streams(
    cases: Sequence[StrategyEvaluationCaseV1],
    *,
    dense: Sequence[float] = (1.0, 1.001, 1.002, 1.003),
    degraded: Sequence[float] = (1.0, 1.001, 1.0005, 1.003),
    reconstructed: Sequence[float] = (1.0, 1.001, 1.0018, 1.003),
) -> dict[str, tuple[StrategyQuoteV1, ...]]:
    values = {
        StrategySourceKind.OBSERVED: dense,
        StrategySourceKind.DEGRADED_HOLDOUT: degraded,
        StrategySourceKind.RECONSTRUCTED: reconstructed,
        StrategySourceKind.UNCONDITIONED_RECONSTRUCTION: reconstructed,
        StrategySourceKind.BROKER_CONDITIONED: reconstructed,
        StrategySourceKind.DERIVED_BARS: reconstructed,
    }
    return {
        item.case_id: _quotes(
            item.ensemble_member_id,
            values[item.source_kind],
            sparsity=item.source_kind.value,
            broker_profile_id=item.broker_profile_id,
        )
        for item in cases
    }


def test_versioned_inputs_and_report_round_trip_without_profit_claims() -> None:
    """Every assumption and result is deterministic, bounded metadata."""
    audit = _audit()
    engine = _engine()
    cases = _three_cases(audit)
    execution = StrategyExecutionSpecificationV1(
        entry_latency_ns=SECOND,
        max_execution_wait_ns=SECOND,
        slippage_bps_per_side=0.25,
        fixed_cost_bps_per_side=0.1,
    )
    policy = _policy(horizons_ns=(SECOND, 2 * SECOND))
    plan = _plan(cases, engine=engine, execution=execution, policy=policy)

    assert StrategyEvaluationPlanV1.from_dict(plan.to_dict()) == plan
    assert (
        StrategyExecutionSpecificationV1.from_dict(execution.to_dict())
        == execution
    )
    assert StrategyEvaluationPolicyV1.from_dict(policy.to_dict()) == policy
    assert all(
        StrategyEvaluationCaseV1.from_dict(item.to_dict()) == item
        for item in cases
    )

    report = evaluate_strategy_sensitivity(
        plan,
        _streams(cases),
        {audit.audit_id: audit},
        engine,
    )
    payload = report.to_dict()

    assert StrategySensitivityReportV1.from_json(report.to_json()) == report
    assert payload["output_mode"] == "bounded-derived-metadata"
    assert payload["event_schema_augmented"] is False
    assert payload["profit_claim"] is False
    assert payload["investment_recommendation"] is False
    assert payload["automatic_winner"] is False
    assert all(
        item.to_dict()["quotes_retained"] is False
        for item in report.window_results
    )
    assert all(
        item.to_dict()["outcomes_retained"] is False
        for item in report.window_results
    )


def test_ex_post_and_mixed_information_require_invalid_backtest_labels() -> (
    None
):
    """Ex-post evidence cannot silently enter a prospective comparison."""
    ex_ante = _audit()
    ex_post = _audit(InformationMode.EX_POST_RECONSTRUCTION)
    with pytest.raises(ValueError, match="ex-post.*invalid-for-backtest"):
        _case(
            StrategySourceKind.RECONSTRUCTED,
            audit=ex_post,
            member="member-a",
        )
    observed = _case(
        StrategySourceKind.OBSERVED,
        audit=ex_ante,
        member="reference",
    )
    reconstructed = _case(
        StrategySourceKind.RECONSTRUCTED,
        audit=ex_post,
        member="member-a",
        invalid_for_backtest_reason="ex-post historical counterfactual",
    )
    with pytest.raises(ValueError, match="mixed information modes"):
        _plan((observed, reconstructed))

    engine = _engine()
    plan = _plan(
        (observed, reconstructed),
        engine=engine,
        invalid_for_backtest_reason="mixed ex-ante and ex-post comparison",
    )
    report = evaluate_strategy_sensitivity(
        plan,
        _streams((observed, reconstructed)),
        {ex_ante.audit_id: ex_ante, ex_post.audit_id: ex_post},
        engine,
    )

    assert report.valid_for_backtest is False
    assert report.to_dict()["backtest_label"] == "invalid-for-backtest"
    assert all(not item.valid_for_backtest for item in report.window_results)


def test_information_audit_must_open_strategy_usefulness_gate() -> None:
    """An accepted ex-ante audit without strategy validity still fails closed."""
    audit = _audit(strategy_valid=False)
    engine = _engine()
    cases = _three_cases(audit)

    with pytest.raises(ValueError, match="strategy-usefulness"):
        evaluate_strategy_sensitivity(
            _plan(cases, engine=engine),
            _streams(cases),
            {audit.audit_id: audit},
            engine,
        )


def test_alignment_requires_identical_symbol_and_half_open_window() -> None:
    """Source surfaces cannot be compared across different time support."""
    audit = _audit()
    first = _case(StrategySourceKind.OBSERVED, audit=audit, member="reference")
    misaligned = _case(
        StrategySourceKind.RECONSTRUCTED,
        audit=audit,
        member="member-a",
        end_ns=7 * SECOND,
    )
    with pytest.raises(ValueError, match="differ in symbol or time bounds"):
        _plan((first, misaligned))

    duplicate = replace(
        first,
        source_artifact_id="artifact:observed:duplicate",
        case_id="",
    )
    with pytest.raises(ValueError, match="role is duplicated"):
        _plan((first, duplicate))


def test_reference_accounting_applies_latency_spread_slippage_and_costs() -> (
    None
):
    """The transparent fixture has reviewable execution arithmetic."""
    audit = _audit()
    engine = _engine()
    cases = _three_cases(audit)
    execution = StrategyExecutionSpecificationV1(
        entry_latency_ns=SECOND,
        max_execution_wait_ns=SECOND,
        slippage_bps_per_side=0.5,
        fixed_cost_bps_per_side=0.25,
    )
    plan = _plan(
        cases,
        engine=engine,
        execution=execution,
        policy=_policy(horizons_ns=(SECOND, 2 * SECOND)),
    )
    streams = {
        item.case_id: _quotes(
            item.ensemble_member_id,
            (1.0, 1.001, 1.002, 1.003, 1.004),
            sparsity=item.source_kind.value,
            spread=0.0002,
        )
        for item in cases
    }

    report = evaluate_strategy_sensitivity(
        plan, streams, {audit.audit_id: audit}, engine
    )
    observed = next(
        item
        for item in report.window_results
        if item.source_kind is StrategySourceKind.OBSERVED
    )

    assert observed.status is StrategyWindowStatus.COMPLETED
    assert observed.signal_count == 1
    assert {item.horizon_ns for item in observed.slices} == {
        SECOND,
        2 * SECOND,
    }
    assert all(item.mean_entry_delay_ns == SECOND for item in observed.slices)
    assert all(
        item.mean_net_execution_response_bps < item.mean_gross_response_bps
        for item in observed.slices
    )
    assert all(item.mean_cost_drag_bps > 0 for item in observed.slices)
    assert observed.mean_spread_bps > 0


def test_results_stratify_and_retain_member_uncertainty() -> None:
    """Epoch/session/event/broker/member strata and dispersion remain explicit."""
    audit = _audit()
    engine = _engine()
    cases = (
        _case(StrategySourceKind.OBSERVED, audit=audit, member="reference"),
        _case(
            StrategySourceKind.RECONSTRUCTED,
            audit=audit,
            member="member-a",
        ),
        _case(
            StrategySourceKind.RECONSTRUCTED,
            audit=audit,
            member="member-b",
        ),
        _case(
            StrategySourceKind.BROKER_CONDITIONED,
            audit=audit,
            member="member-c",
            broker_profile_id="broker:demo-v1",
        ),
    )
    streams = {
        cases[0].case_id: _quotes("reference", (1.0, 1.001, 1.002, 1.003)),
        cases[1].case_id: _quotes(
            "member-a",
            (1.0, 1.001, 1.0018, 1.003),
            epoch="epoch-modern",
            session="new_york",
            event_state="news",
            sparsity="reconstructed",
        ),
        cases[2].case_id: _quotes(
            "member-b",
            (1.0, 1.001, 1.0014, 1.003),
            epoch="epoch-modern",
            session="new_york",
            event_state="news",
            sparsity="reconstructed",
        ),
        cases[3].case_id: _quotes(
            "member-c",
            (1.0, 1.001, 1.0016, 1.003),
            epoch="epoch-modern",
            session="new_york",
            event_state="news",
            sparsity="reconstructed",
            broker_profile_id="broker:demo-v1",
        ),
    }

    report = evaluate_strategy_sensitivity(
        _plan(cases, engine=engine),
        streams,
        {audit.audit_id: audit},
        engine,
    )
    reconstructed = next(
        item
        for item in report.uncertainty_summaries
        if item.source_kind is StrategySourceKind.RECONSTRUCTED
    )
    broker = next(
        item
        for item in report.uncertainty_summaries
        if item.source_kind is StrategySourceKind.BROKER_CONDITIONED
    )

    assert reconstructed.epoch_id == "epoch-modern"
    assert reconstructed.session == "new_york"
    assert reconstructed.event_state == "news"
    assert reconstructed.ensemble_member_ids == ("member-a", "member-b")
    assert reconstructed.standard_deviation_bps > 0
    assert broker.broker_profile_id == "broker:demo-v1"


def test_reverse_degradation_reports_approach_to_dense_reference() -> None:
    """Reconstructed execution response is compared with dense and degraded."""
    audit = _audit()
    engine = _engine()
    cases = _three_cases(audit)

    report = evaluate_strategy_sensitivity(
        _plan(cases, engine=engine),
        _streams(cases),
        {audit.audit_id: audit},
        engine,
    )

    assert len(report.restoration_results) == 1
    restoration = report.restoration_results[0]
    assert restoration.candidate_source_kind is StrategySourceKind.RECONSTRUCTED
    assert restoration.candidate_absolute_error_bps < (
        restoration.degraded_absolute_error_bps
    )
    assert restoration.restoration_gain_bps > 0
    assert restoration.approaches_dense_reference is True
    assert report.restoration_unavailable_count == 0

    reconstructed = next(
        item
        for item in report.window_results
        if item.source_kind is StrategySourceKind.RECONSTRUCTED
    )
    hooks = strategy_sensitivity_benchmark_hooks(reconstructed)
    assert (
        hooks["downstream_sensitivity"]
        == reconstructed.slices[0].mean_net_execution_response_bps
    )
    assert hooks["strategy_missing_support_rate"] == 0.0


def test_missing_horizon_support_and_missing_stream_are_reported() -> None:
    """Incomplete exits and absent case streams remain explicit rates."""
    audit = _audit()
    engine = _engine()
    cases = _three_cases(audit)
    plan = _plan(
        cases,
        engine=engine,
        policy=_policy(horizons_ns=(10 * SECOND,)),
    )
    streams = _streams(cases)
    del streams[cases[2].case_id]

    report = evaluate_strategy_sensitivity(
        plan, streams, {audit.audit_id: audit}, engine
    )

    assert report.summary["missing_support_window_rate"] == pytest.approx(1.0)
    assert report.summary["missing_support_outcome_rate"] == pytest.approx(1.0)
    assert all(
        item.status is StrategyWindowStatus.MISSING_SUPPORT
        for item in report.window_results
    )
    assert report.summary["status_counts"]["missing_support"] == 3


def test_no_trade_failure_and_resource_refusal_rates_are_bounded() -> None:
    """Every non-result status has a bounded reason and rate."""
    audit = _audit()
    engine = _engine()
    cases = _three_cases(audit)
    constant_streams = {
        item.case_id: _quotes(
            item.ensemble_member_id,
            (1.0, 1.0, 1.0, 1.0),
            sparsity=item.source_kind.value,
        )
        for item in cases
    }
    no_trade = evaluate_strategy_sensitivity(
        _plan(cases, engine=engine),
        constant_streams,
        {audit.audit_id: audit},
        engine,
    )
    assert no_trade.summary["no_trade_window_rate"] == 1.0
    with pytest.raises(ValueError, match="completed window"):
        strategy_sensitivity_benchmark_hooks(no_trade.window_results[0])

    refusal = evaluate_strategy_sensitivity(
        _plan(
            cases,
            engine=engine,
            policy=_policy(max_quotes_per_window=2),
        ),
        _streams(cases),
        {audit.audit_id: audit},
        engine,
    )
    assert refusal.summary["refused_window_rate"] == 1.0
    assert all(not item.slices for item in refusal.window_results)

    failing = _FailingEngine(engine)
    failed = evaluate_strategy_sensitivity(
        _plan(cases, engine=engine),
        _streams(cases),
        {audit.audit_id: audit},
        failing,
    )
    assert failed.summary["failure_window_rate"] == 1.0
    assert all(
        item.reason == "fixture strategy failure"
        for item in failed.window_results
    )


class _FailingState:
    def observe(self, quote: StrategyQuoteV1) -> Sequence[StrategySignalV1]:
        del quote
        raise StrategyEvaluationFailure("fixture strategy failure")


class _FailingEngine:
    def __init__(self, reference: ReferenceMomentumStrategyV1) -> None:
        self.specification = reference.specification

    def start_window(
        self, evaluation_case: StrategyEvaluationCaseV1
    ) -> StrategySignalStateV1:
        del evaluation_case
        return _FailingState()


def test_quote_stream_order_and_support_are_fail_closed() -> None:
    """Malformed streams are contract errors, not plausible result artifacts."""
    audit = _audit()
    engine = _engine()
    cases = _three_cases(audit)
    streams = _streams(cases)
    streams[cases[0].case_id] = tuple(reversed(streams[cases[0].case_id]))
    with pytest.raises(ValueError, match="not strictly ordered"):
        evaluate_strategy_sensitivity(
            _plan(cases, engine=engine),
            streams,
            {audit.audit_id: audit},
            engine,
        )

    streams = _streams(cases)
    outside = replace(
        streams[cases[0].case_id][0],
        event_time_ns=cases[0].end_ns,
        quote_id="",
    )
    streams[cases[0].case_id] = (outside,)
    with pytest.raises(ValueError, match="outside aligned"):
        evaluate_strategy_sensitivity(
            _plan(cases, engine=engine),
            streams,
            {audit.audit_id: audit},
            engine,
        )


def test_engine_specification_and_payload_bounds_fail_closed() -> None:
    """Logic drift and oversized report metadata cannot pass silently."""
    audit = _audit()
    engine = _engine()
    other = ReferenceMomentumStrategyV1(threshold_bps=1.0)
    cases = _three_cases(audit)
    with pytest.raises(ValueError, match="engine specification differs"):
        evaluate_strategy_sensitivity(
            _plan(cases, engine=engine),
            _streams(cases),
            {audit.audit_id: audit},
            other,
        )

    with pytest.raises(ValueError, match="payload exceeds"):
        evaluate_strategy_sensitivity(
            _plan(
                cases,
                engine=engine,
                policy=_policy(max_payload_bytes=256),
            ),
            _streams(cases),
            {audit.audit_id: audit},
            engine,
        )


def test_market_surface_adapters_cover_benchmark_reconstruction_and_bars() -> (
    None
):
    """All source families enter through one minimal normalized quote contract."""
    benchmark = BenchmarkEventV1(
        source_event_id="benchmark-source:1",
        symbol="EURUSD",
        event_time_ns=SECOND,
        event_sequence=2,
        bid=1.1,
        ask=1.1002,
        epoch_id="modern",
        session="london",
        event_state="quiet",
        sparsity="degraded",
        ensemble_member_id="member-a",
    )
    benchmark_quote = StrategyQuoteV1.from_benchmark_event(
        benchmark, broker_profile_id="broker:test"
    )
    assert (
        StrategyQuoteV1.from_dict(benchmark_quote.to_dict()) == benchmark_quote
    )
    assert benchmark_quote.session == "london"
    assert benchmark_quote.broker_profile_id == "broker:test"

    left = _observed(1)
    right = _observed(2)
    generated = _generated(left, right)
    reconstructed = StrategyQuoteV1.from_synthetic_event(
        generated,
        epoch_id="legacy",
        session="new_york",
        event_state="news",
        sparsity="reconstructed",
    )
    assert reconstructed.source_event_id == generated.event_id
    assert reconstructed.ensemble_member_id == generated.ensemble_member_id

    bar = _derive_boundary_bars()[0]
    bar_quote = StrategyQuoteV1.from_derived_bar(
        bar, session="london", event_state="quiet"
    )
    assert bar_quote.source_event_id == bar.bar_id
    assert bar_quote.bid == bar.bid_close
    assert bar_quote.ask == bar.ask_close
    assert bar_quote.sparsity == "derived_bar"
    assert bar_quote.source_scope == bar.scope.value
    assert bar_quote.bar_interval_code == bar.interval_code


def test_derived_bar_cases_require_manifest_scope_and_interval() -> None:
    """Bar comparisons cannot hide product scope or aggregation semantics."""
    audit = _audit()
    with pytest.raises(ValueError, match="requires scope and interval"):
        _case(
            StrategySourceKind.DERIVED_BARS,
            audit=audit,
            member="member-a",
        )
    case = _case(
        StrategySourceKind.DERIVED_BARS,
        audit=audit,
        member="member-a",
        source_scope="merged",
        bar_interval_code="1m",
    )
    assert case.source_scope == "merged"
    assert case.bar_interval_code == "1m"
    observed = _case(
        StrategySourceKind.OBSERVED,
        audit=audit,
        member="reference",
    )
    engine = _engine()
    with pytest.raises(ValueError, match="bar quote scope differs"):
        evaluate_strategy_sensitivity(
            _plan((observed, case), engine=engine),
            _streams((observed, case)),
            {audit.audit_id: audit},
            engine,
        )


def test_multiple_alignment_windows_contribute_rolling_stability() -> None:
    """Repeated aligned windows remain distinct inputs to uncertainty summaries."""
    audit = _audit()
    engine = _engine()
    first = (
        _case(
            StrategySourceKind.OBSERVED,
            audit=audit,
            member="reference-a",
            alignment="window-a",
        ),
        _case(
            StrategySourceKind.RECONSTRUCTED,
            audit=audit,
            member="member-a",
            alignment="window-a",
        ),
    )
    second = (
        _case(
            StrategySourceKind.OBSERVED,
            audit=audit,
            member="reference-b",
            alignment="window-b",
        ),
        _case(
            StrategySourceKind.RECONSTRUCTED,
            audit=audit,
            member="member-b",
            alignment="window-b",
        ),
    )
    cases = first + second
    streams = _streams(cases)
    streams[second[1].case_id] = _quotes(
        "member-b",
        (1.0, 1.001, 1.0012, 1.003),
        sparsity="reconstructed",
    )

    report = evaluate_strategy_sensitivity(
        _plan(cases, engine=engine),
        streams,
        {audit.audit_id: audit},
        engine,
    )
    reconstructed = next(
        item
        for item in report.uncertainty_summaries
        if item.source_kind is StrategySourceKind.RECONSTRUCTED
    )

    assert reconstructed.window_count == 2
    assert reconstructed.ensemble_member_ids == ("member-a", "member-b")
    assert reconstructed.standard_deviation_bps > 0

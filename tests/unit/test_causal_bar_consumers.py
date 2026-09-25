"""Real committed products drive all four opt-in consumer operations."""

from dataclasses import replace
from contextlib import contextmanager, closing

import polars as pl
import pytest

from histdatacom.broker_capture.bar_fingerprints import (
    _state_summaries,
    compare_broker_delivery_fingerprints_with_bar_state,
    fit_broker_delivery_fingerprint_with_bar_state,
)
from histdatacom.broker_capture.storage import (
    discover_broker_capture_session_manifests,
)
from histdatacom.broker_plugin_policy import provider_native_inputs
from histdatacom.broker_plugin_policy.native_inputs import fingerprint_for
from histdatacom.data_quality.bar_training_features import (
    enrich_tick_cache_with_causal_bar_features,
)
from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_conditioning import (
    query_reference_motifs_with_bar_state,
)
from histdatacom.synthetic.bar_features import (
    BarFeatureConsumerResultV1,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.bar_strategy import (
    CausalBarStrategyInputV1,
    evaluate_causal_bar_strategy,
)
from histdatacom.synthetic.bars import load_derived_bar_manifest
from histdatacom.synthetic.strategy_sensitivity import (
    StrategyEvaluationCaseV1,
    StrategyEvaluationPlanV1,
    StrategyExecutionSpecificationV1,
    StrategyQuoteV1,
    StrategySourceKind,
)
from tests.unit.test_synthetic_bar_features import (
    ANTE,
    OBS,
    POST,
    _published_snapshot,
    published_source as _published_source_fixture,
)
from tests.unit.test_synthetic_motifs import _index
from tests.fixtures.broker_provider_policy import (
    generated_legacy_request,
    generated_provider_scope,
)
from tests.unit.test_synthetic_strategy_sensitivity import (
    _audit,
    _engine,
    _policy,
)


def _prepared_source(root):
    with closing(_published_source_fixture.__wrapped__(root)) as setup:
        source, product, directory = next(setup)
        fingerprint = fingerprint_for(product.manifest.broker_profile_id)
    captures = discover_broker_capture_session_manifests(
        directory / "broker-capture"
    )
    requests = tuple(
        generated_legacy_request(item.session) for item in captures
    )
    return source, product, directory, fingerprint, requests


@contextmanager
def _prepared_scopes(*sources):
    roots = tuple(
        root
        for item in sources
        for root in (item[3], *item[4], item[1].manifest)
    )
    subjects = tuple(
        subject for item in sources for subject in (item[3], *item[4])
    )
    with provider_native_inputs(*roots), generated_provider_scope(*subjects):
        yield


@pytest.fixture
def published_source(tmp_path):
    prepared = _prepared_source(tmp_path)
    with _prepared_scopes(prepared):
        yield prepared[:3]


def test_reference_retrieval_uses_matching_metrics_and_verified_cutoff(
    published_source,
):
    source, _, _ = published_source
    snapshot = _published_snapshot(source)
    result = query_reference_motifs_with_bar_state(
        _index(),
        source=source,
        snapshot=snapshot,
        information_mode=POST,
        scope=OBS,
        interval_code="1m",
    )
    body = result.result()
    assert body["query"]["used_at_ns"] == snapshot.decision_time_ns
    assert body["query"]["condition"]["metrics"] == {
        "return_value": snapshot.cells[0]
        .feature("mid_log_open_close_return")
        .value,
        "tick_intensity": snapshot.cells[0]
        .feature("tick_intensity_per_second")
        .value,
    }
    assert body["retrieval"]["query"]["query_id"] == body["query"]["query_id"]
    assert result.snapshot_ids == (snapshot.artifact_id,)
    assert BarFeatureConsumerResultV1.from_json(result.to_json()) == result
    missing = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=snapshot.decision_time_ns,
        policy=snapshot.policy,
    )
    with pytest.raises(ValueError, match="complete closed bar"):
        query_reference_motifs_with_bar_state(
            _index(),
            source=source,
            snapshot=missing,
            information_mode=POST,
            scope=OBS,
            interval_code="1m",
        )
    with pytest.raises(ValueError, match="information modes"):
        query_reference_motifs_with_bar_state(
            _index(),
            source=source,
            snapshot=snapshot,
            information_mode=ANTE,
            scope=OBS,
            interval_code="1m",
        )


def _strategy_inputs(source, product):
    scopes = (OBS, ActivitySliceScope.MERGED)
    engine = _engine()
    audit = replace(_audit(POST), run_id=product.manifest.run_id, audit_id="")
    cases, inputs = [], {}
    bar_manifest = load_derived_bar_manifest(source.bar_manifest_path)
    for scope in scopes:
        first = _published_snapshot(source, scope=scope)
        later = source.snapshot(
            symbol="EURUSD",
            decision_time_ns=first.decision_time_ns + 1_000_000_000,
            policy=first.policy,
            availability=first.availability,
        )
        case = StrategyEvaluationCaseV1(
            run_id=product.manifest.run_id,
            alignment_window_id=scope.value,
            source_kind=StrategySourceKind.DERIVED_BARS,
            source_artifact_id=bar_manifest.manifest_id,
            symbol="EURUSD",
            start_ns=first.decision_time_ns,
            end_ns=later.decision_time_ns + 1,
            information_mode=POST,
            information_manifest_id=audit.manifest_id,
            information_audit_id=audit.audit_id,
            ensemble_member_id=product.manifest.ensemble_member_id,
            source_scope=scope.value,
            bar_interval_code="1m",
            invalid_for_backtest_reason="availability declarations are not historical certificates",
        )
        cases.append(case)
        inputs[case.case_id] = CausalBarStrategyInputV1(source, (first, later))
    plan = StrategyEvaluationPlanV1(
        product.manifest.run_id,
        engine.specification,
        StrategyExecutionSpecificationV1(),
        _policy(),
        tuple(cases),
        invalid_for_backtest_reason="diagnostic replay-clock study",
    )
    return plan, inputs, {audit.audit_id: audit}, engine


def test_strategy_executes_existing_engine_at_decision_clock(
    published_source, monkeypatch
):
    source, product, _ = published_source
    plan, inputs, audits, engine = _strategy_inputs(source, product)
    captured = []
    original = StrategyQuoteV1.__post_init__

    def capture(quote):
        original(quote)
        captured.append(quote)

    monkeypatch.setattr(StrategyQuoteV1, "__post_init__", capture)
    result = evaluate_causal_bar_strategy(plan, inputs, audits, engine)
    assert len(captured) == 4
    assert all(
        quote.event_time_ns
        >= inputs[next(iter(inputs))].snapshots[0].cells[0].bar_end_ns
        for quote in captured
    )
    assert all(
        quote.source_event_id.startswith("causal-bar-snapshot:")
        for quote in captured
    )
    report = result.result()["report"]
    assert all(
        window["quote_count"] == 2 for window in report["window_results"]
    )
    assert not result.historical_availability_verified
    with pytest.raises(ValueError, match="invalid-for-backtest"):
        evaluate_causal_bar_strategy(
            replace(plan, invalid_for_backtest_reason=None, plan_id=""),
            inputs,
            audits,
            engine,
        )
    with pytest.raises(TypeError, match="snapshots, not legacy quotes"):
        CausalBarStrategyInputV1(source, (captured[0],))
    first_case = plan.cases[0]
    unavailable = tuple(
        source.snapshot(
            symbol=item.symbol,
            decision_time_ns=item.decision_time_ns,
            policy=item.policy,
        )
        for item in inputs[first_case.case_id].snapshots
    )
    missing_inputs = {
        **inputs,
        first_case.case_id: CausalBarStrategyInputV1(source, unavailable),
    }
    with pytest.raises(ValueError, match="missing or partial"):
        evaluate_causal_bar_strategy(plan, missing_inputs, audits, engine)
    unknown = replace(first_case, source_artifact_id="unverified", case_id="")
    bad_plan = replace(plan, cases=(unknown, plan.cases[1]), plan_id="")
    bad_inputs = {
        unknown.case_id: inputs[first_case.case_id],
        plan.cases[1].case_id: inputs[plan.cases[1].case_id],
    }
    with pytest.raises(ValueError, match="source identity"):
        evaluate_causal_bar_strategy(bad_plan, bad_inputs, audits, engine)


def test_training_enrichment_is_row_exact_and_does_not_mutate_observed_columns(
    published_source,
):
    source, _, _ = published_source
    snapshot = _published_snapshot(source, mode=ANTE)
    timestamp_ms = snapshot.decision_time_ns // 1_000_000
    frame = pl.DataFrame(
        {
            "datetime": [timestamp_ms, timestamp_ms],
            "bid": [1.1, 1.2],
            "ask": [1.1001, 1.2001],
            "vol": [0.0, 0.0],
        }
    )
    baseline = enrich_tick_cache_with_training_features(
        frame,
        symbol="EURUSD",
        data_format="ascii",
        timeframe="T",
        period="202311",
    )
    enriched = enrich_tick_cache_with_causal_bar_features(
        frame,
        source=source,
        snapshots=(snapshot,),
        information_mode=ANTE,
        symbol="EURUSD",
        period="202311",
    )
    assert enriched.select(baseline.columns).equals(baseline)
    assert enriched.height == frame.height
    prefix = "causal_bar_v1__observed__1m__mid_close__"
    assert (
        enriched[prefix + "value"].to_list()
        == [snapshot.cells[0].feature("mid_close").value] * 2
    )
    assert (
        enriched["causal_bar_v1__snapshot_id"].to_list()
        == [snapshot.artifact_id] * 2
    )
    with pytest.raises(ValueError, match="overwritten"):
        enrich_tick_cache_with_causal_bar_features(
            enriched,
            source=source,
            snapshots=(snapshot,),
            information_mode=ANTE,
            symbol="EURUSD",
            period="202311",
        )
    with pytest.raises(ValueError, match="exact row-cutoff"):
        enrich_tick_cache_with_causal_bar_features(
            frame.with_columns(pl.col("datetime") + 1),
            source=source,
            snapshots=(snapshot,),
            information_mode=ANTE,
            symbol="EURUSD",
            period="202311",
        )
    with pytest.raises(ValueError, match="information modes"):
        enrich_tick_cache_with_causal_bar_features(
            frame,
            source=source,
            snapshots=(snapshot,),
            information_mode=POST,
            symbol="EURUSD",
            period="202311",
        )
    missing = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=snapshot.decision_time_ns,
        policy=snapshot.policy,
    )
    missing_frame = enrich_tick_cache_with_causal_bar_features(
        frame,
        source=source,
        snapshots=(missing,),
        information_mode=ANTE,
        symbol="EURUSD",
        period="202311",
    )
    assert missing_frame[prefix + "value"].to_list() == [None, None]
    assert missing_frame[prefix + "state"].to_list() == [
        "unavailable",
        "unavailable",
    ]
    assert missing_frame.schema == enriched.schema


def _fit(source, root, snapshot):
    captures = discover_broker_capture_session_manifests(
        root / "broker-capture"
    )
    return fit_broker_delivery_fingerprint_with_bar_state(
        root / "broker-capture",
        captures,
        source=source,
        snapshots=(snapshot,),
        information_mode=POST,
    )


def test_broker_fit_invokes_capture_fit_preserves_events_and_deduplicates_state(
    published_source,
):
    source, product, root = published_source
    snapshot = _published_snapshot(source)
    result = _fit(source, root, snapshot)
    body = result.result()
    assert (
        body["delivery_fingerprint"]["fingerprint_id"]
        == product.manifest.broker_profile_id
    )
    assert body["source_product_manifest_id"] == product.manifest.manifest_id
    assert any(
        item["name"] == "mid_close" and item["support_count"] == 1
        for item in body["state_summaries"]
    )
    later = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=snapshot.decision_time_ns + 1,
        policy=snapshot.policy,
        availability=snapshot.availability,
    )
    captures = discover_broker_capture_session_manifests(
        root / "broker-capture"
    )
    deduplicated = fit_broker_delivery_fingerprint_with_bar_state(
        root / "broker-capture",
        captures,
        source=source,
        snapshots=(snapshot, later),
        information_mode=POST,
    )
    assert deduplicated.result()["state_summaries"] == body["state_summaries"]
    with pytest.raises(ValueError, match="information modes"):
        fit_broker_delivery_fingerprint_with_bar_state(
            root / "broker-capture",
            captures,
            source=source,
            snapshots=(snapshot,),
            information_mode=ANTE,
        )
    with pytest.raises(ValueError, match="unique snapshots"):
        fit_broker_delivery_fingerprint_with_bar_state(
            root / "broker-capture",
            captures,
            source=source,
            snapshots=(snapshot, snapshot),
            information_mode=POST,
        )


def test_broker_comparison_runs_existing_delivery_and_separate_state_comparison(
    tmp_path, monkeypatch
):
    from tests.unit import test_synthetic_persistence as fixture_module

    first = _prepared_source(tmp_path / "first")
    original = fixture_module._capture
    monkeypatch.setattr(
        fixture_module,
        "_capture",
        lambda root, seed, wall_start_ns: original(
            root, seed=seed + 1, wall_start_ns=wall_start_ns
        ),
    )
    second = _prepared_source(tmp_path / "other")
    with _prepared_scopes(first, second):
        _compare_prepared_sources(first, second)


def _compare_prepared_sources(first, second):
    source, _, root = first[:3]
    other, _, other_root = second[:3]
    left = _fit(source, root, _published_snapshot(source))
    right = _fit(other, other_root, _published_snapshot(other))
    result = compare_broker_delivery_fingerprints_with_bar_state(
        left, right, information_mode=POST
    )
    assert result.result()["delivery_comparison"]
    assert result.result()["state_comparisons"]
    assert result.result()["reference_result_id"] == left.artifact_id
    assert not result.empirical_qualification_claim
    with pytest.raises(ValueError, match="same-mode"):
        compare_broker_delivery_fingerprints_with_bar_state(
            left, right, information_mode=ANTE
        )
    for field, value in (
        ("mean", 999.0),
        ("support_count", 99),
        ("population_variance", 999.0),
    ):
        forged = left.result()
        forged["state_summaries"][0][field] = value
        resealed = replace(left, result_json=canonical_bar_feature_json(forged))
        with pytest.raises(ValueError, match="do not replay"):
            compare_broker_delivery_fingerprints_with_bar_state(
                resealed, right, information_mode=POST
            )
    forged = left.result()
    forged["unknown"] = True
    with pytest.raises(ValueError, match="result schema"):
        compare_broker_delivery_fingerprints_with_bar_state(
            replace(left, result_json=canonical_bar_feature_json(forged)),
            right,
            information_mode=POST,
        )


def test_broker_summary_arithmetic_handles_extreme_finite_support():
    def row(value):
        return {
            "symbol": "EURUSD",
            "scope": "observed",
            "interval_code": "1m",
            "name": "mid_close",
            "value": value,
        }

    summary = _state_summaries([row(1e308), row(1e308)])[0]
    assert summary["mean"] == 1e308
    assert summary["population_variance"] == 0.0
    with pytest.raises(ValueError, match="not representable"):
        _state_summaries([row(-1e308), row(1e308)])

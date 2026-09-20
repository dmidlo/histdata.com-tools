"""Executed estimates, ex-ante gates, binding replay and atomic persistence."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.forecasting import (
    FeatureColumnV1,
    FeatureKind,
    FeaturePeriodV1,
    FeatureRequestV1,
    ForecastHorizon,
    ForecastTargetKind,
    VintageFeatureStoreV1,
    read_feature_artifact,
)
from histdatacom.forecasting.contracts import DAY_NS
from histdatacom.forecasting.engine_artifacts import (
    read_engine_artifact,
    write_engine_artifact,
)
from histdatacom.forecasting.engine_contracts import EngineReadiness
from histdatacom.forecasting.engine_runner import (
    HISTORICAL_MEAN,
    HISTORICAL_MEDIAN,
    LAST_VALUE,
    REFERENCE_BLEND,
    ForecastEngineComparisonV1,
    ForecastEngineModelV1,
    ForecastEngineRunnerV1,
    ForecastEngineScoreV1,
    ForecastEngineSnapshotV1,
    default_forecast_registry,
)
from tests.fixtures.forecast_contracts_v1 import RELEASE_TIME, calendar_fixture
from tests.fixtures.forecast_engine_registry_v1 import engine_comparison_fixture
from tests.fixtures.forecast_feature_store_v1 import (
    CUTOFF,
    PERIODS,
    feature_forecast_fixture,
    inputs_fixture,
    observation,
    revision,
    store_fixture,
)


def test_execute_comparison_and_all_artifact_roundtrips(tmp_path: Path) -> None:
    comparison = engine_comparison_fixture()
    assert comparison.metrics["absolute_errors"] == {
        HISTORICAL_MEAN: 1.0,
        HISTORICAL_MEDIAN: 1.0,
        REFERENCE_BLEND: 0.5,
    }
    assert comparison.metrics["evidence_unit_count"] == 1
    assert comparison.metrics["independent_model_count"] is None
    score = next(
        item
        for item in comparison.scores
        if item.snapshot.bound_model.engine_key == REFERENCE_BLEND
    )
    assert score.metrics["observed_target"] == 3.0  # not the later revision 8
    bound = score.snapshot.bound_model
    for artifact in (
        bound.registry,
        bound.registry.successor("1.0.1"),
        bound,
        score.snapshot,
        score,
        comparison,
    ):
        path = write_engine_artifact(artifact, tmp_path)
        assert read_engine_artifact(path) == artifact
        assert write_engine_artifact(artifact, tmp_path) == path
        with pytest.raises(ValueError):
            read_feature_artifact(path)
    score.snapshot.verify_against(calendar_fixture(), store_fixture())


@pytest.mark.parametrize(
    "key,expected",
    [
        (HISTORICAL_MEAN, 11 / 3),
        (HISTORICAL_MEDIAN, 2.0),
        (LAST_VALUE, 8.0),
        (REFERENCE_BLEND, 35 / 6),
    ],
)
def test_real_estimator_math_not_renamed_duplicate_engines(
    key: str, expected: float
) -> None:
    records = (observation(0), observation(1), observation(2, value=8))
    inputs = inputs_fixture(store=VintageFeatureStoreV1(records))
    snapshot = feature_forecast_fixture()
    runner = ForecastEngineRunnerV1(default_forecast_registry())
    model = runner.fit(
        key,
        inputs,
        column="cpi",
        horizon=snapshot.cutoff.horizon,
        target_kind=snapshot.target.kind,
        trained_at_ns=CUTOFF,
    )
    result = runner.generate(
        model,
        inputs,
        cutoff=snapshot.cutoff,
        target=snapshot.target,
        generated_at_ns=CUTOFF,
    )
    assert result.forecast.distribution.point == pytest.approx(expected)


def test_last_known_comparator_uses_new_inference_observation_while_mean_is_fixed_fit() -> (
    None
):
    snapshot = feature_forecast_fixture()
    records = tuple(observation(index) for index in range(3))
    period = FeaturePeriodV1(
        "new-month", PERIODS[-1].end_ns, CUTOFF - 2 * DAY_NS
    )
    new = replace(
        records[-1],
        period=period,
        value=20,
        published_at_ns=CUTOFF - DAY_NS,
        available_at_ns=CUTOFF - DAY_NS,
    )
    store = VintageFeatureStoreV1((*records, new))
    features = store.snapshot(
        FeatureRequestV1(
            CUTOFF, (*PERIODS, period), (FeatureColumnV1("cpi", "macro.cpi"),)
        )
    )
    inference = replace(snapshot.inputs, features=features)
    runner = ForecastEngineRunnerV1(default_forecast_registry())
    points = []
    for key in (LAST_VALUE, HISTORICAL_MEAN):
        model = runner.fit(
            key,
            snapshot.model.training_inputs,
            column="cpi",
            horizon=snapshot.cutoff.horizon,
            target_kind=snapshot.target.kind,
            trained_at_ns=snapshot.model.trained_at_ns,
        )
        points.append(
            runner.generate(
                model,
                inference,
                cutoff=snapshot.cutoff,
                target=snapshot.target,
                generated_at_ns=CUTOFF,
            ).forecast.distribution.point
        )
    assert points == [20.0, 2.0]


@pytest.mark.parametrize(
    "mutation",
    [
        "target",
        "channel",
        "frequency",
        "missing",
        "sample",
        "columns",
        "training-clock",
    ],
)
def test_fit_refusals_precede_executable_call(
    mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import histdatacom.forecasting.engine_runner as implementation

    snapshot = feature_forecast_fixture()
    inputs = snapshot.model.training_inputs
    target = snapshot.target.kind
    trained = snapshot.model.trained_at_ns
    if mutation == "target":
        target = ForecastTargetKind.CONSENSUS
    elif mutation == "training-clock":
        trained = inputs.cutoff_at_ns - 1
    elif mutation == "sample":
        inputs = replace(
            inputs,
            features=store_fixture().snapshot(
                replace(inputs.features.request, periods=PERIODS[:2])
            ),
        )
    elif mutation == "columns":
        inputs = replace(
            inputs,
            features=store_fixture().snapshot(
                replace(
                    inputs.features.request,
                    columns=(
                        FeatureColumnV1("cpi", "macro.cpi"),
                        FeatureColumnV1("other", "unknown"),
                    ),
                )
            ),
        )
    else:
        records = tuple(observation(index) for index in range(3))
        if mutation == "missing":
            records = records[:-1]
        else:
            records = tuple(
                replace(
                    item,
                    definition=(
                        replace(item.definition, kind=FeatureKind.MARKET)
                        if mutation == "channel"
                        else replace(item.definition, frequency="daily")
                    ),
                )
                for item in records
            )
        inputs = replace(
            inputs,
            features=VintageFeatureStoreV1(records).snapshot(
                inputs.features.request
            ),
        )

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("implementation invoked before admission")

    monkeypatch.setattr(implementation, "train_feature_baseline", fail)
    with pytest.raises(ValueError):
        ForecastEngineRunnerV1(default_forecast_registry()).fit(
            REFERENCE_BLEND,
            inputs,
            column="cpi",
            horizon=snapshot.cutoff.horizon,
            target_kind=target,
            trained_at_ns=trained,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "horizon",
        "generation-clock",
        "cutoff",
        "target-scale",
        "target-base",
        "stale-schedule",
    ],
)
def test_generate_refusals_precede_implementation(
    mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import histdatacom.forecasting.engine_runner as implementation

    snapshot = feature_forecast_fixture()
    runner = ForecastEngineRunnerV1(default_forecast_registry())
    model = runner.fit(
        REFERENCE_BLEND,
        snapshot.model.training_inputs,
        column="cpi",
        horizon=(
            ForecastHorizon.T1D
            if mutation == "horizon"
            else snapshot.cutoff.horizon
        ),
        target_kind=snapshot.target.kind,
        trained_at_ns=snapshot.model.trained_at_ns,
    )
    cutoff, target, inputs, generated = (
        snapshot.cutoff,
        snapshot.target,
        snapshot.inputs,
        CUTOFF,
    )
    if mutation == "generation-clock":
        generated -= 20 * DAY_NS
    elif mutation == "cutoff":
        inputs = snapshot.model.training_inputs
    elif mutation == "target-scale":
        target = replace(target, scale=100)
    elif mutation == "target-base":
        target = replace(target, base="other")
    elif mutation == "stale-schedule":
        cutoff = replace(cutoff, schedule_release_id="unknown-schedule")

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("prediction invoked before admission")

    monkeypatch.setattr(implementation, "forecast_feature_baseline", fail)
    with pytest.raises(ValueError):
        runner.generate(
            model,
            inputs,
            cutoff=cutoff,
            target=target,
            generated_at_ns=generated,
        )


def test_declared_engine_is_not_executable_or_production_eligible() -> None:
    registry = default_forecast_registry()
    declaration = replace(
        registry.engine(REFERENCE_BLEND),
        engine_key="future-arima",
        techniques=("univariate-state-space/ARIMA",),
        readiness=EngineReadiness.DECLARED,
    )
    registry = replace(registry, engines=(*registry.engines, declaration))
    snapshot = feature_forecast_fixture()
    with pytest.raises(ValueError, match="no installed executable"):
        ForecastEngineRunnerV1(registry).fit(
            declaration.engine_key,
            snapshot.inputs,
            column="cpi",
            horizon=snapshot.cutoff.horizon,
            target_kind=snapshot.target.kind,
            trained_at_ns=CUTOFF,
        )
    with pytest.raises(ValueError, match="no production admission"):
        registry.require_production(REFERENCE_BLEND)


def test_changed_runner_code_and_forged_fit_or_prediction_refuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import histdatacom.forecasting.engine_runner as implementation

    score = engine_comparison_fixture().scores[0]
    model = score.snapshot.bound_model
    state = json.loads(model.model.state_json)
    state["estimate"] = 1234
    with pytest.raises(ValueError, match="executed exact replay"):
        replace(model, model=replace(model.model, state_json=json.dumps(state)))
    from histdatacom.forecasting import ForecastDistributionV1

    with pytest.raises(ValueError, match="executed exact replay"):
        replace(
            score.snapshot,
            forecast=replace(
                score.snapshot.forecast,
                distribution=ForecastDistributionV1(((1234, 1.0),)),
            ),
        )
    monkeypatch.setattr(
        implementation, "_implementation_digest", lambda: "0" * 64
    )
    with pytest.raises(ValueError, match="identity changed"):
        ForecastEngineModelV1.from_json(model.to_json())


def test_future_insertions_preserve_bound_forecast_identity_and_backdated_change_refuses() -> (
    None
):
    comparison = engine_comparison_fixture()
    snapshot = comparison.scores[0].snapshot
    records = tuple(observation(index) for index in range(3))
    expanded = VintageFeatureStoreV1((*records, revision(records[-1])))
    inference = inputs_fixture(store=expanded)
    runner = ForecastEngineRunnerV1(snapshot.bound_model.registry)
    repeated = runner.generate(
        snapshot.bound_model,
        inference,
        cutoff=snapshot.forecast.cutoff,
        target=snapshot.forecast.target,
        generated_at_ns=CUTOFF,
    )
    assert repeated.snapshot_id == snapshot.snapshot_id
    repeated.verify_against(calendar_fixture(), expanded)
    changed = VintageFeatureStoreV1(
        (replace(records[0], value=101), *records[1:])
    )
    with pytest.raises(ValueError):
        repeated.verify_against(calendar_fixture(), changed)


def test_score_and_persistence_refuse_bare_feature_projection(
    tmp_path: Path,
) -> None:
    snapshot = feature_forecast_fixture()
    with pytest.raises(ValueError, match="bare"):
        ForecastEngineRunnerV1(default_forecast_registry()).score(snapshot, calendar_fixture(), scored_at_ns=RELEASE_TIME + DAY_NS)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bare"):
        write_engine_artifact(snapshot, tmp_path)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field", ["registry", "binding", "metrics", "forecast"]
)
def test_nested_wire_mutations_refuse(field: str) -> None:
    comparison = engine_comparison_fixture()
    payload = json.loads(comparison.to_json())
    if field == "registry":
        payload["scores"][0]["snapshot"]["bound_model"]["registry"]["engines"][
            0
        ]["scientific_role"] = "production-qualified"
    elif field == "binding":
        payload["scores"][0]["snapshot"]["bound_model"]["binding_sha256"] = (
            "0" * 64
        )
    elif field == "metrics":
        payload["metrics"]["evidence_unit_count"] = 3
    else:
        payload["scores"][0]["snapshot"]["forecast"]["distribution"][
            "point"
        ] = 123
    with pytest.raises(ValueError):
        ForecastEngineComparisonV1.from_json(json.dumps(payload))


def test_comparison_refuses_missing_benchmark_and_task_mismatch() -> None:
    comparison = engine_comparison_fixture()
    with pytest.raises(ValueError, match="execution set"):
        replace(comparison, scores=comparison.scores[1:])
    score = comparison.scores[0]
    later = replace(
        score,
        feature_score=replace(
            score.feature_score,
            scored_at_ns=score.feature_score.scored_at_ns + 1,
        ),
    )
    with pytest.raises(ValueError, match="identical task"):
        replace(comparison, scores=(later, *comparison.scores[1:]))


def test_persistence_is_no_clobber_and_does_not_block_on_special_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = default_forecast_registry()
    path = write_engine_artifact(artifact, tmp_path)
    path.write_text("corruption", encoding="utf-8")
    with pytest.raises(ValueError, match="differs"):
        write_engine_artifact(artifact, tmp_path)
    assert path.read_text() == "corruption"
    assert not tuple(tmp_path.glob(".engine-artifact-*"))
    link = tmp_path / "link.json"
    link.symlink_to(path)
    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)
    for invalid in (link, fifo, tmp_path):
        with pytest.raises(ValueError, match="regular"):
            read_engine_artifact(invalid)

    def interrupt(*args: object, **kwargs: object) -> None:
        raise OSError("simulated interrupted publication")

    monkeypatch.setattr(os, "link", interrupt)
    with pytest.raises(OSError, match="interrupted"):
        write_engine_artifact(artifact, tmp_path / "interrupted")
    assert not tuple((tmp_path / "interrupted").iterdir())


def test_installed_code_digests_bind_both_adapter_and_legacy_model() -> None:
    import histdatacom.forecasting.engine_runner as runner
    import histdatacom.forecasting.feature_forecasts as legacy

    comparison = engine_comparison_fixture()
    model = next(
        item.snapshot.bound_model
        for item in comparison.scores
        if item.snapshot.bound_model.engine_key == REFERENCE_BLEND
    )
    assert (
        model.binding_sha256
        == hashlib.sha256(Path(runner.__file__).read_bytes()).hexdigest()
    )
    assert (
        model.model.implementation_sha256
        == hashlib.sha256(Path(legacy.__file__).read_bytes()).hexdigest()
    )
    assert model.model.version == "1.0"


def test_every_accepted_final_envelope_fits_and_roundtrips_at_exact_byte_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import histdatacom.forecasting.contracts as contracts

    comparison = engine_comparison_fixture()
    score = comparison.scores[0]
    for artifact, reader in (
        (score.snapshot.bound_model, ForecastEngineModelV1.from_json),
        (score.snapshot, ForecastEngineSnapshotV1.from_json),
        (score, ForecastEngineScoreV1.from_json),
        (comparison, ForecastEngineComparisonV1.from_json),
    ):
        payload = artifact.to_json()
        with monkeypatch.context() as scoped:
            scoped.setattr(
                contracts,
                "MAX_FORECAST_ARTIFACT_BYTES",
                len(payload.encode("utf-8")),
            )
            assert reader(payload).to_json() == payload
            scoped.setattr(
                contracts,
                "MAX_FORECAST_ARTIFACT_BYTES",
                len(payload.encode("utf-8")) - 1,
            )
            with pytest.raises(ValueError, match="byte bound"):
                reader(payload)
    payload = score.snapshot.bound_model.to_json()
    monkeypatch.setattr(
        contracts, "MAX_FORECAST_ARTIFACT_BYTES", len(payload.encode("utf-8"))
    )
    with pytest.raises(ValueError, match="byte bound"):
        replace(score.snapshot)


def test_final_identity_overhead_and_excessive_depth_refuse_without_recursion_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import histdatacom.forecasting.contracts as contracts

    registry = default_forecast_registry()
    body = registry.to_dict()
    del body["id"]
    body_length = len(contracts._json(body).encode("utf-8"))
    with monkeypatch.context() as scoped:
        scoped.setattr(contracts, "MAX_FORECAST_ARTIFACT_BYTES", body_length)
        with pytest.raises(ValueError, match="byte bound"):
            replace(registry)
    payload = registry.to_dict()
    nested: dict[str, object] = {}
    for _ in range(100):
        nested = {"nested": nested}
    payload["unknown"] = nested  # type: ignore[assignment]
    with pytest.raises(ValueError, match="nesting"):
        type(registry).from_json(json.dumps(payload))
    payload["unknown"] = payload
    with pytest.raises(ValueError, match="nesting"):
        type(registry).from_dict(payload)


def test_extreme_finite_even_sample_median_has_documented_finite_refusal() -> (
    None
):
    snapshot = feature_forecast_fixture()
    records = tuple(observation(index, value=1e308) for index in range(3))
    first = records[0]
    older_period = FeaturePeriodV1(
        "older", first.period.start_ns - 10 * DAY_NS, first.period.start_ns
    )
    older = replace(
        first,
        period=older_period,
        definition=replace(first.definition, known_at_ns=older_period.start_ns),
        published_at_ns=older_period.end_ns,
        available_at_ns=older_period.end_ns + DAY_NS,
    )
    matrix = VintageFeatureStoreV1((older, *records)).snapshot(
        FeatureRequestV1(
            CUTOFF,
            (older_period, *PERIODS),
            (FeatureColumnV1("cpi", "macro.cpi"),),
        )
    )
    inputs = replace(snapshot.inputs, features=matrix)
    with pytest.raises(ValueError, match="finite"):
        ForecastEngineRunnerV1(default_forecast_registry()).fit(
            HISTORICAL_MEDIAN,
            inputs,
            column="cpi",
            horizon=snapshot.cutoff.horizon,
            target_kind=snapshot.target.kind,
            trained_at_ns=CUTOFF,
        )

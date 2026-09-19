"""Full feature-aware train/predict/score/persist/replay acceptance examples."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.forecasting import (
    FeatureColumnV1,
    FeatureEvidenceV1,
    FeatureMatrixSnapshotV1,
    FeatureRequestV1,
    ForecastFeatureInputsV1,
    ForecastFeatureModelV1,
    ForecastFeatureScoreV1,
    ForecastFeatureSnapshotV1,
    ForecastScaleV1,
    ForecastTargetKind,
    RevisionMeasure,
    SurpriseReference,
    VintageFeatureStoreV1,
    capture_forecast_feature_inputs,
    forecast_feature_baseline,
    read_feature_artifact,
    read_forecast_artifact,
    train_feature_baseline,
    write_feature_artifact,
)
from histdatacom.forecasting.contracts import DAY_NS
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
)
from tests.fixtures.forecast_contracts_v1 import (
    RELEASE_TIME,
    calendar_fixture,
    nested_forecast_state,
    snapshot_fixture,
)
from tests.fixtures.forecast_feature_store_v1 import (
    CUTOFF,
    PERIODS,
    feature_forecast_fixture,
    inputs_fixture,
    observation,
    revision,
    store_fixture,
)


def test_executed_train_predict_score_persist_and_replay(
    tmp_path: Path,
) -> None:
    snapshot = feature_forecast_fixture()
    assert snapshot.distribution.point == 2.5  # training mean 2; latest 3
    corpus = calendar_fixture()
    score = ForecastFeatureScoreV1(
        snapshot, corpus.to_json(), RELEASE_TIME + 40 * DAY_NS
    )
    assert score.metrics["observed_target"] == 3.0  # not revised 8
    assert score.metrics["absolute_error"] == 0.5
    assert score.metrics["independent_value_added"] == 0.5
    assert snapshot.model.model_id.startswith("forecast-feature-model:")
    for artifact in (snapshot.inputs.features, snapshot, score):
        path = write_feature_artifact(artifact, tmp_path)
        restored = read_feature_artifact(path)
        assert restored == artifact
        assert write_feature_artifact(artifact, tmp_path) == path
        with pytest.raises(ValueError):
            read_forecast_artifact(path)
    snapshot.verify_against(corpus, store_fixture())
    assert (
        ForecastFeatureScoreV1.from_json(score.to_json()).metrics
        == score.metrics
    )


def test_future_values_calendar_catalog_and_classification_do_not_change_historical_hashes() -> (
    None
):
    corpus = calendar_fixture()
    original = inputs_fixture(corpus=corpus)
    expanded = EconomicCalendarCorpusV1(
        corpus.coverage_start_ns - DAY_NS,
        corpus.coverage_end_ns + 100 * DAY_NS,
        False,
        corpus.releases,
        corpus.forecasts,
        ("Later global catalog classification",),
    )
    records = tuple(observation(index) for index in range(3))
    future = replace(
        revision(records[-1]),
        definition=replace(
            records[-1].definition,
            known_at_ns=CUTOFF + DAY_NS,
            classification="final-sample-regime",
        ),
    )
    store = VintageFeatureStoreV1((*records, future))
    later = inputs_fixture(corpus=expanded, store=store)
    assert later.to_dict() == original.to_dict()
    original.verify_against(expanded, store)
    snapshot = feature_forecast_fixture()
    replayed = forecast_feature_baseline(
        snapshot.model,
        later,
        cutoff=snapshot.cutoff,
        target=snapshot.target,
        generated_at_ns=snapshot.generated_at_ns,
    )
    assert replayed.snapshot_id == snapshot.snapshot_id


@pytest.mark.parametrize(
    "section", ["features", "training", "state", "projection", "metrics"]
)
def test_full_envelope_rejects_tampered_feature_model_and_projection_evidence(
    section: str,
) -> None:
    score = ForecastFeatureScoreV1(
        feature_forecast_fixture(),
        calendar_fixture().to_json(),
        RELEASE_TIME + 40 * DAY_NS,
    )
    payload = json.loads(score.to_json())
    if section == "features":
        payload["snapshot"]["inputs"]["features"]["observations"][0][
            "value"
        ] = 100
    elif section == "training":
        payload["snapshot"]["model"]["training_inputs"]["features"]["cells"][0][
            "value"
        ] = 100
    elif section == "state":
        payload["snapshot"]["model"]["state"]["training_mean"] = 100
    elif section == "projection":
        payload["calendar_semantics_projection_id"] += "changed"
    else:
        payload["metrics"]["absolute_error"] = 0
    with pytest.raises(ValueError):
        ForecastFeatureScoreV1.from_json(json.dumps(payload))


def test_changed_feature_input_changes_full_identity_even_if_calendar_projection_matches() -> (
    None
):
    first = feature_forecast_fixture()
    records = tuple(observation(index) for index in range(3))
    changed_store = VintageFeatureStoreV1(
        (*records, observation(0, key="macro.other", value=101))
    )
    changed_matrix = changed_store.snapshot(
        FeatureRequestV1(
            CUTOFF,
            PERIODS,
            (
                FeatureColumnV1("cpi", "macro.cpi"),
                FeatureColumnV1("other", "macro.other"),
            ),
        )
    )
    changed_inputs = replace(first.inputs, features=changed_matrix)
    second = replace(first, inputs=changed_inputs)
    assert (
        first._calendar_projection().snapshot_id
        == second._calendar_projection().snapshot_id
    )
    assert first.snapshot_id != second.snapshot_id
    one = ForecastFeatureScoreV1(
        first, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS
    )
    two = ForecastFeatureScoreV1(
        second, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS
    )
    assert one.metrics == two.metrics
    assert one.score_id != two.score_id


def test_overlap_cannot_rewrite_history_but_disjoint_inference_grid_is_allowed() -> (
    None
):
    snapshot = feature_forecast_fixture()
    records = tuple(observation(index) for index in range(3))
    altered = inputs_fixture(
        store=VintageFeatureStoreV1(
            (replace(records[0], value=99), *records[1:])
        )
    )
    with pytest.raises(ValueError, match="overlapping training vintage"):
        replace(snapshot, inputs=altered)
    smaller = store_fixture().snapshot(
        FeatureRequestV1(
            CUTOFF, PERIODS[1:], (FeatureColumnV1("cpi", "macro.cpi"),)
        )
    )
    accepted = replace(
        snapshot, inputs=replace(snapshot.inputs, features=smaller)
    )
    assert len(accepted.model.training_inputs.features.request.periods) == 3
    assert len(accepted.inputs.features.request.periods) == 2


@pytest.mark.parametrize("field,value", [("scale", 100.0), ("base", "2020")])
def test_arithmetic_baseline_refuses_target_multiplier_and_base_mismatch(
    field: str, value: object
) -> None:
    snapshot = feature_forecast_fixture()
    target = replace(snapshot.target, **{field: value})
    with pytest.raises(ValueError, match="target feature semantics"):
        forecast_feature_baseline(
            snapshot.model,
            snapshot.inputs,
            cutoff=snapshot.cutoff,
            target=target,
            generated_at_ns=CUTOFF,
        )


def test_baseline_commits_code_bytes_and_replay_refuses_changed_executable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    import histdatacom.forecasting.feature_forecasts as implementation

    snapshot = feature_forecast_fixture()
    assert (
        snapshot.model.implementation_sha256
        == hashlib.sha256(
            Path(implementation.__file__).read_bytes()
        ).hexdigest()
    )
    assert (
        snapshot.model.implementation_sha256
        != json.loads(snapshot.model.state_json)["algorithm_spec_sha256"]
    )
    monkeypatch.setattr(
        implementation, "_baseline_implementation_sha256", lambda: "0" * 64
    )
    with pytest.raises(ValueError, match="exact feature fit replay"):
        forecast_feature_baseline(
            snapshot.model,
            snapshot.inputs,
            cutoff=snapshot.cutoff,
            target=snapshot.target,
            generated_at_ns=CUTOFF,
        )


def test_no_model_or_artifact_api_accepts_bare_calendar_or_expost_table(
    tmp_path: Path,
) -> None:
    bare = snapshot_fixture()
    with pytest.raises(TypeError, match="sealed ex-ante"):
        train_feature_baseline({"latest": [1, 2, 3]}, column="cpi", trained_at_ns=CUTOFF)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bare calendar"):
        ForecastFeatureScoreV1(bare, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="complete feature artifact"):
        write_feature_artifact(bare, tmp_path)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="feature-aware training"):
        ForecastFeatureModelV1("wrong", "1.0", "a" * 64, '{"mean":2}', bare.inputs, CUTOFF)  # type: ignore[arg-type]


def test_cutoff_generation_and_transform_fit_constraints_fail_closed() -> None:
    snapshot = feature_forecast_fixture()
    with pytest.raises(ValueError, match="cutoff mismatch"):
        replace(
            snapshot.inputs,
            calendar_inputs=replace(
                snapshot.inputs.calendar_inputs, cutoff_at_ns=CUTOFF - 1
            ),
        )
    with pytest.raises(ValueError, match="training inputs follow"):
        replace(
            snapshot.model,
            trained_at_ns=snapshot.model.training_inputs.cutoff_at_ns - 1,
        )
    fresh = revision(observation(2), available_at_ns=CUTOFF)
    records = tuple(observation(index) for index in range(3))
    changed = inputs_fixture(store=VintageFeatureStoreV1((*records, fresh)))
    with pytest.raises(
        ValueError, match="knowledge follows forecast generation"
    ):
        replace(snapshot, inputs=changed, generated_at_ns=CUTOFF - 1)
    with pytest.raises(ValueError, match="input information-set/cutoff"):
        replace(snapshot, inputs=inputs_fixture(cutoff=CUTOFF - 1))


def test_training_replays_actual_fit_and_refuses_relabelled_or_missing_data() -> (
    None
):
    snapshot = feature_forecast_fixture()
    state = json.loads(snapshot.model.state_json)
    state["training_mean"] = 999
    model = replace(snapshot.model, state_json=json.dumps(state))
    with pytest.raises(ValueError, match="exact feature fit replay"):
        forecast_feature_baseline(
            model,
            snapshot.inputs,
            cutoff=snapshot.cutoff,
            target=snapshot.target,
            generated_at_ns=CUTOFF,
        )
    missing = inputs_fixture(
        store=VintageFeatureStoreV1((observation(0), observation(2)))
    )
    with pytest.raises(ValueError, match="unavailable"):
        train_feature_baseline(missing, column="cpi", trained_at_ns=CUTOFF)
    records = tuple(observation(index) for index in range(3))
    mixed = inputs_fixture(
        store=VintageFeatureStoreV1(
            (
                records[0],
                replace(
                    records[1],
                    definition=replace(
                        records[1].definition, seasonality="unadjusted"
                    ),
                ),
                records[2],
            )
        )
    )
    with pytest.raises(ValueError, match="mixed semantic"):
        train_feature_baseline(mixed, column="cpi", trained_at_ns=CUTOFF)


def test_replay_detects_missing_or_changed_actual_source_vintages() -> None:
    snapshot = feature_forecast_fixture()
    missing = VintageFeatureStoreV1((observation(0), observation(1)))
    with pytest.raises(
        ValueError, match="information set is missing or changed"
    ):
        snapshot.verify_against(calendar_fixture(), missing)
    corpus = calendar_fixture()
    no_survey = replace(corpus, forecasts=(), corpus_id="")
    with pytest.raises(ValueError, match="calendar information set changed"):
        snapshot.verify_against(no_survey, store_fixture())


def test_every_accepted_value_object_roundtrips_inside_its_own_envelope() -> (
    None
):
    inputs = inputs_fixture()
    assert ForecastFeatureInputsV1.from_dict(inputs.to_dict()) == inputs
    model = train_feature_baseline(inputs, column="cpi", trained_at_ns=CUTOFF)
    assert ForecastFeatureModelV1.from_dict(model.to_dict()) == model
    assert (
        FeatureMatrixSnapshotV1.from_json(inputs.features.to_json())
        == inputs.features
    )
    snapshot = feature_forecast_fixture()
    assert ForecastFeatureSnapshotV1.from_json(snapshot.to_json()) == snapshot


def test_explicit_calendar_request_bounds_do_not_silently_truncate_chains() -> (
    None
):
    matrix = store_fixture().snapshot(
        FeatureRequestV1(
            CUTOFF, PERIODS, (FeatureColumnV1("cpi", "macro.cpi"),)
        )
    )
    with pytest.raises(ValueError, match="outside corpus coverage"):
        capture_forecast_feature_inputs(
            calendar_fixture(),
            matrix,
            calendar_event_keys=("fixture.cpi.event-0",),
            calendar_coverage_start_ns=RELEASE_TIME - 2 * DAY_NS,
            calendar_coverage_end_ns=RELEASE_TIME - DAY_NS,
        )


def test_artifact_refuses_corrupted_existing_file(tmp_path: Path) -> None:
    snapshot = feature_forecast_fixture()
    path = write_feature_artifact(snapshot, tmp_path)
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="differs from sealed"):
        write_feature_artifact(snapshot, tmp_path)
    with pytest.raises(ValueError, match="digest mismatch"):
        read_feature_artifact(path)


def test_backdated_insertion_cannot_change_explicit_training_missingness() -> (
    None
):
    snapshot = feature_forecast_fixture()
    columns = (
        FeatureColumnV1("cpi", "macro.cpi"),
        FeatureColumnV1("other", "macro.other"),
    )
    training_matrix = store_fixture().snapshot(
        FeatureRequestV1(CUTOFF - DAY_NS, PERIODS, columns)
    )
    training = replace(snapshot.model.training_inputs, features=training_matrix)
    model = train_feature_baseline(
        training, column="cpi", trained_at_ns=CUTOFF - DAY_NS
    )
    records = tuple(observation(index) for index in range(3))
    backdated = observation(0, key="macro.other")
    inference_matrix = VintageFeatureStoreV1((*records, backdated)).snapshot(
        FeatureRequestV1(CUTOFF, PERIODS, columns)
    )
    with pytest.raises(ValueError, match="overlapping training vintage"):
        replace(
            snapshot,
            model=model,
            inputs=replace(snapshot.inputs, features=inference_matrix),
        )


def test_interrupted_write_never_publishes_partial_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import histdatacom.forecasting.feature_artifacts as persistence

    def interrupted_sync(descriptor: int) -> None:
        raise OSError("simulated interrupted fsync")

    monkeypatch.setattr(persistence.os, "fsync", interrupted_sync)
    with pytest.raises(OSError, match="interrupted"):
        write_feature_artifact(feature_forecast_fixture(), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_atomic_publication_is_complete_before_link_and_never_clobbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import histdatacom.forecasting.feature_artifacts as persistence

    snapshot = feature_forecast_fixture()
    link = persistence.os.link
    observed: list[bytes] = []

    def checked_link(
        source: Path, target: Path, *, follow_symlinks: bool
    ) -> None:
        observed.append(source.read_bytes())
        assert not target.exists()
        link(source, target, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(persistence.os, "link", checked_link)
    path = write_feature_artifact(snapshot, tmp_path)
    assert observed == [snapshot.to_json().encode()]
    assert path.read_bytes() == observed[0]
    assert list(tmp_path.glob(".feature-artifact-*")) == []


@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo"])
def test_artifact_reader_refuses_nonregular_files_without_blocking(
    tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "unsafe.json"
    if kind == "directory":
        path.mkdir()
    elif kind == "symlink":
        path.symlink_to(tmp_path / "absent.json")
    else:
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO unsupported on platform")
        os.mkfifo(path)
    with pytest.raises(ValueError, match="regular non-symlink"):
        read_feature_artifact(path)


@pytest.mark.parametrize("depth", [40, 48, 52, 54, 56, 58, 60, 62, 64, 100])
def test_all_accepted_expanded_envelopes_roundtrip_before_composition_refuses(
    depth: int,
) -> None:
    base = observation(0)
    try:
        receipt = replace(
            base.evidence, record_json=json.dumps(nested_forecast_state(depth))
        )
        assert FeatureEvidenceV1.from_dict(receipt.to_dict()) == receipt
        record = replace(base, evidence=receipt)
        assert type(record).from_dict(record.to_dict()) == record
        store = VintageFeatureStoreV1((record, observation(1), observation(2)))
        matrix = store.snapshot(
            FeatureRequestV1(
                CUTOFF, PERIODS, (FeatureColumnV1("cpi", "macro.cpi"),)
            )
        )
        assert FeatureMatrixSnapshotV1.from_json(matrix.to_json()) == matrix
        inputs = replace(inputs_fixture(), features=matrix)
        assert ForecastFeatureInputsV1.from_dict(inputs.to_dict()) == inputs
        model = train_feature_baseline(
            inputs, column="cpi", trained_at_ns=CUTOFF
        )
        assert ForecastFeatureModelV1.from_dict(model.to_dict()) == model
        template = snapshot_fixture()
        snapshot = forecast_feature_baseline(
            model,
            inputs,
            cutoff=template.cutoff,
            target=template.target,
            generated_at_ns=CUTOFF,
        )
        assert (
            ForecastFeatureSnapshotV1.from_json(snapshot.to_json()) == snapshot
        )
        score = ForecastFeatureScoreV1(
            snapshot, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS
        )
        assert ForecastFeatureScoreV1.from_json(score.to_json()) == score
    except ValueError as exc:
        assert depth >= 52
        assert "nesting bounds" in str(exc)
    else:
        assert depth < 64


def test_sealed_identity_bytes_count_toward_final_wire_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import histdatacom.forecasting.contracts as bounds

    snapshot = feature_forecast_fixture()
    exact = len(snapshot.to_json().encode())
    monkeypatch.setattr(bounds, "MAX_FORECAST_ARTIFACT_BYTES", exact)
    assert ForecastFeatureSnapshotV1.from_json(snapshot.to_json()) == snapshot
    monkeypatch.setattr(bounds, "MAX_FORECAST_ARTIFACT_BYTES", exact - 1)
    with pytest.raises(ValueError, match="byte bound"):
        replace(snapshot)


def test_duplicate_keys_and_extreme_encoded_json_refuse_without_recursion_leak() -> (
    None
):
    with pytest.raises(ValueError, match="duplicate JSON key"):
        FeatureMatrixSnapshotV1.from_json('{"a":1,"a":2}')
    encoded = '{"nested":' * 1000 + "0" + "}" * 1000
    with pytest.raises(ValueError, match="nesting"):
        replace(observation(0).evidence, record_json=encoded)


@pytest.mark.parametrize(
    "kind,expected",
    [
        (ForecastTargetKind.CONSENSUS, 2.0),
        (ForecastTargetKind.FIRST_RELEASE_ACTUAL, 3.0),
        (ForecastTargetKind.REVISION, 5.0),
        (ForecastTargetKind.SURPRISE, 1.0),
    ],
)
def test_full_feature_envelope_preserves_distinct_target_scoring(
    kind: ForecastTargetKind, expected: float
) -> None:
    base = feature_forecast_fixture()
    target = replace(
        base.target,
        kind=kind,
        consensus_reference=(
            None
            if kind is ForecastTargetKind.REVISION
            else base.target.consensus_reference
        ),
        revision_sequence=2 if kind is ForecastTargetKind.REVISION else None,
        revision_measure=(
            RevisionMeasure.CHANGE_FROM_FIRST
            if kind is ForecastTargetKind.REVISION
            else None
        ),
        surprise_reference=(
            SurpriseReference.OBSERVED_CONSENSUS
            if kind is ForecastTargetKind.SURPRISE
            else None
        ),
    )
    snapshot = replace(base, target=target)
    score = ForecastFeatureScoreV1(
        snapshot, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS
    )
    assert score.metrics["observed_target"] == expected
    assert (
        ForecastFeatureScoreV1.from_json(score.to_json()).metrics
        == score.metrics
    )


def test_predicted_consensus_surprise_binds_full_feature_reference_identity() -> (
    None
):
    base = feature_forecast_fixture()
    consensus = replace(
        base, target=replace(base.target, kind=ForecastTargetKind.CONSENSUS)
    )
    target = replace(
        base.target,
        kind=ForecastTargetKind.SURPRISE,
        consensus_reference=None,
        surprise_reference=SurpriseReference.PREDICTED_CONSENSUS,
    )
    snapshot = replace(base, target=target, predicted_consensus=consensus)
    score = ForecastFeatureScoreV1(
        snapshot, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS
    )
    assert score.metrics["observed_target"] == 0.5
    assert (
        score.metrics["predicted_consensus_snapshot_id"]
        == consensus.snapshot_id
    )
    assert ForecastFeatureScoreV1.from_json(score.to_json()) == score


def test_full_feature_score_requires_committed_exante_normalization() -> None:
    base = feature_forecast_fixture()
    scale = ForecastScaleV1(
        "historical-error-scale",
        2.0,
        base.target.unit,
        CUTOFF,
        "Fixed fixture scale; not estimated from holdout outcomes.",
    )
    snapshot = replace(base, normalizer_id=str(scale.to_dict()["id"]))
    score = ForecastFeatureScoreV1(
        snapshot,
        calendar_fixture().to_json(),
        RELEASE_TIME + 40 * DAY_NS,
        scale,
    )
    assert score.metrics["normalized_absolute_error"] == 0.25
    assert ForecastFeatureScoreV1.from_json(score.to_json()) == score
    with pytest.raises(ValueError, match="committed normalization is missing"):
        ForecastFeatureScoreV1(
            snapshot, calendar_fixture().to_json(), RELEASE_TIME + 40 * DAY_NS
        )

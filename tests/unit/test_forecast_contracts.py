"""Point-in-time and mutation regressions for immutable forecast snapshots."""

from __future__ import annotations

import json
from collections import UserDict
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import MappingProxyType

import pytest

import histdatacom.forecasting.contracts as forecast_contracts
from histdatacom.forecasting import (
    ConsensusTiming,
    ForecastCutoffV1,
    ForecastDistributionV1,
    ForecastHorizon,
    ForecastInputsV1,
    ForecastModelIdentityV1,
    ForecastSnapshotV1,
    ForecastTargetKind,
    PointStatistic,
    SurpriseReference,
    capture_forecast_inputs,
    read_forecast_artifact,
    write_forecast_artifact,
)
from histdatacom.forecasting.contracts import DAY_NS, HOUR_NS
from tests.fixtures.forecast_contracts_v1 import (
    calendar_fixture,
    nested_forecast_state,
    snapshot_fixture,
    utc_text,
)


def test_snapshot_replays_exact_normalized_vintages_and_freezes_metadata() -> (
    None
):
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    encoded = snapshot.to_json()
    assert ForecastSnapshotV1.from_json(encoded) == snapshot
    assert len(snapshot.inputs.calendar.releases) == 1
    assert len(snapshot.inputs.calendar.forecasts) == 1
    snapshot.inputs.verify_against(corpus)
    restored = snapshot.inputs.calendar
    restored.releases[0].source.metadata["fixture"]["mutable_at_origin"] = False
    assert snapshot.to_json() == encoded
    with pytest.raises(FrozenInstanceError):
        snapshot.generated_at_ns += 1


@pytest.mark.parametrize(
    "path",
    [
        ("cutoff", "cutoff_at_ns"),
        ("target", "unit"),
        ("model", "name"),
        ("distribution", "point"),
        ("inputs", "cutoff_at_ns"),
    ],
)
def test_nested_identity_tampering_fails_closed(path: tuple[str, str]) -> None:
    data = json.loads(snapshot_fixture().to_json())
    original = data[path[0]][path[1]]
    data[path[0]][path[1]] = (
        original + 1
        if isinstance(original, (int, float))
        else original + "changed"
    )
    with pytest.raises((ValueError, TypeError)):
        ForecastSnapshotV1.from_json(json.dumps(data))


@pytest.mark.parametrize(
    "horizon,lead",
    [
        (ForecastHorizon.T30D, 30 * DAY_NS),
        (ForecastHorizon.T7D, 7 * DAY_NS),
        (ForecastHorizon.T1D, DAY_NS),
        (ForecastHorizon.T1H, HOUR_NS),
    ],
)
def test_fixed_horizons_require_exact_cutoff(
    horizon: ForecastHorizon, lead: int
) -> None:
    cutoff = ForecastCutoffV1(horizon, 0, lead, "schedule")
    assert cutoff.horizon is horizon
    with pytest.raises(ValueError, match="horizon/cutoff"):
        replace(cutoff, cutoff_at_ns=1)
    with pytest.raises(ValueError, match="fixed horizon"):
        replace(cutoff, final_pre_release_lead_ns=1)


def test_final_pre_release_has_explicit_distinct_schedule_policy() -> None:
    with pytest.raises(ValueError, match="explicit lead"):
        ForecastCutoffV1(
            ForecastHorizon.FINAL_PRE_RELEASE, 0, HOUR_NS, "schedule"
        )
    final = ForecastCutoffV1(
        ForecastHorizon.FINAL_PRE_RELEASE, 0, HOUR_NS, "schedule", HOUR_NS
    )
    fixed = ForecastCutoffV1(ForecastHorizon.T1H, 0, HOUR_NS, "schedule")
    assert final.to_dict()["id"] != fixed.to_dict()["id"]


def test_future_inputs_and_post_cutoff_model_fail_closed() -> None:
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    with pytest.raises(ValueError, match="unavailable at cutoff"):
        ForecastInputsV1(snapshot.cutoff.cutoff_at_ns, corpus.to_json())
    with pytest.raises(ValueError, match="generation follows cutoff"):
        replace(
            snapshot,
            model=replace(
                snapshot.model, trained_at_ns=snapshot.cutoff.cutoff_at_ns + 1
            ),
        )
    with pytest.raises(ValueError, match="input vintage follows"):
        replace(
            snapshot, generated_at_ns=snapshot.cutoff.cutoff_at_ns - 2 * DAY_NS
        )


def test_source_availability_not_retrieval_or_publication_decides_inputs() -> (
    None
):
    corpus = calendar_fixture(availability_delay_ns=DAY_NS)
    initial = corpus.releases[1]
    before = capture_forecast_inputs(
        corpus, cutoff_at_ns=initial.released_at_ns
    )
    at_available = capture_forecast_inputs(
        corpus, cutoff_at_ns=initial.available_at_ns
    )
    assert initial not in before.calendar.releases
    assert initial in at_available.calendar.releases
    assert initial.source.retrieved_at_ns > initial.available_at_ns


def test_missing_or_changed_exact_vintage_fails_replay() -> None:
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    missing = replace(corpus, forecasts=corpus.forecasts[1:], corpus_id="")
    with pytest.raises(ValueError, match="exact input forecast"):
        snapshot.inputs.verify_against(missing)
    changed = replace(corpus.forecasts[0], value=99.0, forecast_id="")
    with pytest.raises(ValueError, match="exact input forecast"):
        snapshot.inputs.verify_against(
            replace(
                corpus, forecasts=(changed, corpus.forecasts[1]), corpus_id=""
            )
        )


def test_consensus_is_cutoff_specific_unless_future_evolution_explicit() -> (
    None
):
    snapshot = snapshot_fixture(kind=ForecastTargetKind.CONSENSUS)
    reference = snapshot.target.consensus_reference
    later = replace(
        reference, target_at_ns=snapshot.cutoff.cutoff_at_ns + DAY_NS
    )
    with pytest.raises(ValueError, match="consensus/cutoff"):
        replace(
            snapshot, target=replace(snapshot.target, consensus_reference=later)
        )
    future = replace(later, timing=ConsensusTiming.FUTURE_EVOLUTION)
    evolved = replace(
        snapshot, target=replace(snapshot.target, consensus_reference=future)
    )
    assert evolved.snapshot_id != snapshot.snapshot_id
    actual = snapshot_fixture()
    with pytest.raises(ValueError, match="explicit evolution target"):
        replace(
            actual, target=replace(actual.target, consensus_reference=future)
        )


def test_schedule_anchor_must_be_latest_visible_vintage() -> None:
    snapshot = snapshot_fixture()
    with pytest.raises(ValueError, match="latest known schedule"):
        replace(
            snapshot,
            cutoff=replace(snapshot.cutoff, schedule_release_id="other"),
        )
    with pytest.raises(ValueError, match="target/release unit"):
        replace(snapshot, target=replace(snapshot.target, unit="index"))


def test_rescheduled_forecast_keeps_known_schedule_not_future_release_clock() -> (
    None
):
    from histdatacom.market_context.economic_calendar import (
        EconomicReleaseStatus,
    )

    corpus = calendar_fixture()
    schedule = corpus.releases[0]
    new_time = schedule.scheduled_for_ns + DAY_NS
    rescheduled = replace(
        schedule,
        revision_sequence=1,
        supersedes_release_id=schedule.release_id,
        status=EconomicReleaseStatus.RESCHEDULED,
        scheduled_for_ns=new_time,
        scheduled_lexical=utc_text(new_time),
        available_at_ns=schedule.scheduled_for_ns - 9 * DAY_NS,
        first_observed_at_ns=schedule.scheduled_for_ns - 9 * DAY_NS,
        schedule_change_reason="Synthetic delay",
        release_id="",
    )
    inputs_corpus = replace(
        corpus, releases=(schedule, rescheduled), corpus_id=""
    )
    snapshot = snapshot_fixture()
    cutoff = new_time - 7 * DAY_NS
    with pytest.raises(ValueError, match="latest known schedule"):
        replace(
            snapshot,
            inputs=capture_forecast_inputs(
                inputs_corpus, cutoff_at_ns=snapshot.cutoff.cutoff_at_ns
            ),
        )
    correct = replace(
        snapshot,
        cutoff=ForecastCutoffV1(
            ForecastHorizon.T7D, cutoff, new_time, rescheduled.release_id
        ),
        inputs=capture_forecast_inputs(inputs_corpus, cutoff_at_ns=cutoff),
        target=replace(
            snapshot.target,
            consensus_reference=replace(
                snapshot.target.consensus_reference, target_at_ns=cutoff
            ),
        ),
        generated_at_ns=cutoff,
    )
    assert correct.cutoff.schedule_release_id == rescheduled.release_id


@pytest.mark.parametrize(
    "support",
    [
        (),
        ((1.0, 0.0),),
        ((1.0, -1.0), (2.0, 2.0)),
        ((1.0, 0.5),),
        ((1.0, 0.5), (1.0, 0.5)),
        ((2.0, 0.5), (1.0, 0.5)),
        ((float("nan"), 1.0),),
        ((1.0, float("inf")),),
    ],
)
def test_invalid_distributions_rejected(
    support: tuple[tuple[float, float], ...],
) -> None:
    with pytest.raises(ValueError):
        ForecastDistributionV1(support)


def test_distribution_point_and_model_state_are_content_bound() -> None:
    mean = ForecastDistributionV1(((1.0, 0.5), (5.0, 0.5)))
    median = replace(mean, point_statistic=PointStatistic.MEDIAN)
    assert mean.point == 3.0
    assert median.point == 1.0
    snapshot = snapshot_fixture()
    changed = replace(
        snapshot.model,
        state_json='{"algorithm":"constant","parameters":{"value":9.0}}',
    )
    assert changed.model_id != snapshot.model.model_id
    assert replace(snapshot, model=changed).snapshot_id != snapshot.snapshot_id


def test_predicted_surprise_requires_same_cutoff_information_and_event() -> (
    None
):
    consensus = snapshot_fixture(kind=ForecastTargetKind.CONSENSUS)
    actual = snapshot_fixture(with_reference=False)
    surprise = replace(
        actual,
        target=replace(
            actual.target,
            kind=ForecastTargetKind.SURPRISE,
            surprise_reference=SurpriseReference.PREDICTED_CONSENSUS,
        ),
        predicted_consensus=consensus,
    )
    assert ForecastSnapshotV1.from_json(surprise.to_json()) == surprise
    with pytest.raises(ValueError, match="sealed consensus prediction"):
        replace(surprise, predicted_consensus=None)
    other = snapshot_fixture(
        calendar_fixture(index=1), kind=ForecastTargetKind.CONSENSUS
    )
    with pytest.raises(ValueError, match="horizon/cutoff/input mismatch"):
        replace(surprise, predicted_consensus=other)


def test_huge_integer_support_fails_with_bounded_value_error() -> None:
    with pytest.raises(ValueError, match="finite"):
        ForecastDistributionV1(((10**1000, 1.0),))


@pytest.mark.parametrize("point", [[], [1], [1, 0.5, 999], "12", None])
def test_distribution_decoder_rejects_malformed_pairs(point: object) -> None:
    payload = ForecastDistributionV1(((1.0, 1.0),)).to_dict()
    payload["support"] = [point]
    with pytest.raises(ValueError, match="value/probability pairs"):
        ForecastDistributionV1.from_dict(payload)


def test_distribution_and_json_decode_bounds() -> None:
    payload = ForecastDistributionV1(((1.0, 1.0),)).to_dict()
    payload["support"] = [[1.0, 1.0]] * 10001
    with pytest.raises(ValueError, match="point count outside"):
        ForecastDistributionV1.from_dict(payload)
    with pytest.raises(ValueError, match="nesting bounds"):
        ForecastSnapshotV1.from_json(
            '{"nested":' + "[" * 2000 + "0" + "]" * 2000 + "}"
        )


def test_model_and_snapshot_expanded_depth_boundaries_roundtrip() -> None:
    original = snapshot_fixture()
    # A state at depth 63 fits inside a model's `state` field at depth 64.
    model = replace(
        original.model,
        state_json=json.dumps(nested_forecast_state(63)),
    )
    assert (
        ForecastModelIdentityV1.from_dict(
            json.loads(json.dumps(model.to_dict()))
        )
        == model
    )
    with pytest.raises(ValueError, match="nesting bounds"):
        replace(
            original.model, state_json=json.dumps(nested_forecast_state(64))
        )
    # A valid child may exceed the enclosing snapshot's total depth budget.
    with pytest.raises(ValueError, match="nesting bounds"):
        replace(original, model=model)
    snapshot = replace(
        original,
        model=replace(
            original.model,
            state_json=json.dumps(nested_forecast_state(62)),
        ),
    )
    assert ForecastSnapshotV1.from_json(snapshot.to_json()) == snapshot


@pytest.mark.parametrize("metadata_depth,accepted", [(59, True), (60, False)])
def test_input_wrapper_counts_toward_structural_bound(
    metadata_depth: int, accepted: bool
) -> None:
    original = snapshot_fixture().inputs
    corpus = original.calendar
    release = corpus.releases[0]
    source = replace(
        release.source,
        metadata=nested_forecast_state(metadata_depth),
        source_id="",
    )
    corpus = replace(
        corpus,
        releases=(replace(release, source=source, release_id=""),),
        corpus_id="",
    )
    if accepted:
        inputs = ForecastInputsV1(original.cutoff_at_ns, corpus.to_json())
        assert (
            ForecastInputsV1.from_dict(json.loads(json.dumps(inputs.to_dict())))
            == inputs
        )
    else:
        with pytest.raises(ValueError, match="nesting bounds"):
            ForecastInputsV1(original.cutoff_at_ns, corpus.to_json())


@pytest.mark.parametrize("container", [dict, list, tuple])
def test_structural_check_handles_deep_python_containers_without_recursion(
    container: type,
) -> None:
    payload = snapshot_fixture().model.to_dict()
    nested = 0
    for _ in range(2000):
        nested = (
            {"nested": nested} if container is dict else container((nested,))
        )
    payload["state"] = {"nested": nested}
    with pytest.raises(ValueError, match="nesting bounds"):
        ForecastModelIdentityV1.from_dict(payload)


def test_structural_check_rejects_python_cycles_without_recursion() -> None:
    payload = snapshot_fixture().model.to_dict()
    cyclic = {}
    cyclic["nested"] = cyclic
    payload["state"] = cyclic
    with pytest.raises(ValueError, match="nesting bounds"):
        ForecastModelIdentityV1.from_dict(payload)


@pytest.mark.parametrize("mapping_type", [MappingProxyType, UserDict])
def test_encoder_checks_the_exact_mapping_tree_it_serializes(
    mapping_type: type,
) -> None:
    accepted = mapping_type(nested_forecast_state(64))
    assert json.loads(forecast_contracts._json(accepted)) == accepted
    with pytest.raises(ValueError, match="nesting bounds"):
        forecast_contracts._json(mapping_type(nested_forecast_state(65)))


def test_tuple_encoded_as_json_array_has_matching_depth_semantics() -> None:
    original = snapshot_fixture().model
    payload = original.to_dict()
    nested = 0
    for _ in range(62):
        nested = (nested,)
    payload["state"] = {"nested": nested}
    canonical_state = json.dumps(payload["state"])
    expected = replace(original, state_json=canonical_state)
    # Seal against canonical lists, then exercise the tuple-valued Python input.
    payload["id"] = expected.model_id
    restored = ForecastModelIdentityV1.from_dict(payload)
    assert restored == expected
    assert (
        ForecastModelIdentityV1.from_dict(
            json.loads(json.dumps(restored.to_dict()))
        )
        == restored
    )


@pytest.mark.parametrize("kind", ["inputs", "model"])
def test_constructor_byte_bound_includes_final_identity_field(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = snapshot_fixture()
    original = snapshot.inputs if kind == "inputs" else snapshot.model
    data = original.to_dict()
    encoded = forecast_contracts.canonical_contract_json(data).encode("utf-8")
    body = {key: value for key, value in data.items() if key != "id"}
    limit = len(encoded) - 1
    assert (
        len(forecast_contracts.canonical_contract_json(body).encode("utf-8"))
        < limit
    )
    monkeypatch.setattr(
        forecast_contracts, "MAX_FORECAST_ARTIFACT_BYTES", limit
    )
    with pytest.raises(ValueError, match="byte bound"):
        if kind == "inputs":
            replace(snapshot.inputs)
        else:
            replace(snapshot.model)


def test_nested_predicted_consensus_counts_toward_whole_snapshot_bound() -> (
    None
):
    consensus = snapshot_fixture(kind=ForecastTargetKind.CONSENSUS)
    consensus = replace(
        consensus,
        model=replace(
            consensus.model, state_json=json.dumps(nested_forecast_state(62))
        ),
    )
    assert ForecastSnapshotV1.from_json(consensus.to_json()) == consensus
    outer = snapshot_fixture(with_reference=False)
    with pytest.raises(ValueError, match="nesting bounds"):
        replace(
            outer,
            target=replace(
                outer.target,
                kind=ForecastTargetKind.SURPRISE,
                surprise_reference=SurpriseReference.PREDICTED_CONSENSUS,
            ),
            predicted_consensus=consensus,
        )


def test_content_addressed_snapshot_artifact_and_corruption(
    tmp_path: Path,
) -> None:
    snapshot = snapshot_fixture()
    path = write_forecast_artifact(snapshot, tmp_path)
    assert write_forecast_artifact(snapshot, tmp_path) == path
    assert read_forecast_artifact(path) == snapshot
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="digest mismatch"):
        read_forecast_artifact(path)
    with pytest.raises(ValueError, match="differs from sealed"):
        write_forecast_artifact(snapshot, tmp_path)


def test_unknown_fields_missing_identity_and_duplicate_json_keys_rejected() -> (
    None
):
    payload = snapshot_fixture().to_json()
    data = json.loads(payload)
    data["extra"] = "not accepted"
    with pytest.raises(ValueError, match="payload mismatch"):
        ForecastSnapshotV1.from_json(json.dumps(data))
    data.pop("extra")
    data.pop("id")
    with pytest.raises(ValueError, match="payload mismatch"):
        ForecastSnapshotV1.from_json(json.dumps(data))
    with pytest.raises(ValueError, match="duplicate JSON key"):
        ForecastSnapshotV1.from_json('{"x":1,"x":2}')

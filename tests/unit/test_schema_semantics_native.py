"""Non-vacuous native semantic profiles with exact mutation witnesses."""

import json
from dataclasses import replace

import pytest

from histdatacom.broker_plugin_lifecycle.contracts import (
    BrokerLifecycleCompletion,
    BrokerLifecycleState,
)
from histdatacom.schema_semantics import (
    project_semantics,
    semantic_profiles,
    verify_semantic_projection,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
)
from tests.fixtures.schema_semantics_native import native_artifacts


@pytest.fixture(scope="module")
def artifacts(tmp_path_factory):
    return native_artifacts(tmp_path_factory.mktemp("semantic-native"))


@pytest.mark.parametrize("name", [p.name for p in semantic_profiles()])
def test_every_reviewed_native_reader_and_independent_projector(
    artifacts, name
):
    artifact = artifacts[name]
    raw = artifact.to_json()
    changed_format = json.dumps(artifact.to_dict(), sort_keys=False, indent=2)
    original = project_semantics(name, raw)
    other = project_semantics(name, changed_format)
    assert original.semantic_json == other.semantic_json
    assert original.input_sha256 != other.input_sha256
    assert verify_semantic_projection(original, raw) == original
    assert original.semantic_sha256 == other.semantic_sha256


@pytest.mark.parametrize("name", [p.name for p in semantic_profiles()])
def test_actual_encoding_migration_proof_for_every_native_profile(
    artifacts, name
):
    from histdatacom.schema_semantics import validate_lossless_evidence
    from tests.fixtures.schema_semantics_graph import graph_fixture

    registry, proofs = graph_fixture(
        json.dumps(artifacts[name].to_dict(), indent=3), name
    )
    assert len(validate_lossless_evidence(registry, proofs).proof_ids) == 3


@pytest.mark.parametrize("name", [p.name for p in semantic_profiles()])
def test_all_profiles_refuse_unknown_missing_and_forged_envelope_fields(
    artifacts, name
):
    data = artifacts[name].to_dict()
    with pytest.raises(ValueError):
        project_semantics(
            name, json.dumps({**data, "future_scientific_field": 1})
        )
    with pytest.raises(ValueError):
        project_semantics(
            name,
            json.dumps(
                {k: v for k, v in data.items() if k != "schema_version"}
            ),
        )
    identity = next(
        p.self_identity_field for p in semantic_profiles() if p.name == name
    )
    data[identity] = "forged"
    with pytest.raises(ValueError):
        project_semantics(name, json.dumps(data))


def test_nested_ignored_fields_and_reader_sorting_never_hide_changes(artifacts):
    stream = artifacts["synthetic-stream-v1"]
    data = stream.to_dict()
    data["events"][0]["future_origin"] = "secretly-observed"
    with pytest.raises(ValueError, match="normalized/ignored"):
        project_semantics("synthetic-stream-v1", json.dumps(data))
    event = stream.events[0]
    second = replace(
        event,
        event_time_ns=event.event_time_ns + 1,
        source_row_id=2,
        event_id="",
    )
    stream = SyntheticEventStreamV1(
        stream.run_id, stream.ensemble_member_id, stream.symbol, (event, second)
    )
    data = stream.to_dict()
    data["events"].reverse()
    with pytest.raises(ValueError, match="normalized/ignored"):
        project_semantics("synthetic-stream-v1", json.dumps(data))


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_time_ns", 1_000_000_002),
        ("bid", 1.0001),
        ("ensemble_member_id", "member-2"),
        ("run_id", "scenario-2"),
        ("source_version_id", "other-source"),
        ("source_row_id", 2),
    ],
)
def test_source_clock_quote_member_scenario_mutations_change_semantics(
    artifacts, field, value
):
    event = artifacts["synthetic-event-v1"]
    changed = replace(event, **{field: value}, event_id="")
    assert (
        project_semantics("synthetic-event-v1", event.to_json()).semantic_sha256
        != project_semantics(
            "synthetic-event-v1", changed.to_json()
        ).semantic_sha256
    )


def test_origin_signed_zero_and_exact_clock_coercion(artifacts):
    event = artifacts["synthetic-event-v1"]
    synthetic = replace(
        event,
        origin=SyntheticEventOrigin.SYNTHETIC,
        source_row_id=None,
        source_series_id=None,
        source_period=None,
        anchor_interval_id="interval-1",
        left_anchor_event_id="event-left",
        right_anchor_event_id="event-right",
        generator_id="fixture",
        generator_version="1.0",
        generator_config_id="config-1",
        constraint_set_id="constraint-1",
        event_id="",
    )
    positive = replace(synthetic, confidence=0.0, event_id="")
    negative = replace(synthetic, confidence=-0.0, event_id="")
    assert (
        project_semantics(
            "synthetic-event-v1", positive.to_json()
        ).semantic_sha256
        != project_semantics(
            "synthetic-event-v1", negative.to_json()
        ).semantic_sha256
    )
    assert (
        project_semantics("synthetic-event-v1", event.to_json()).semantic_sha256
        != project_semantics(
            "synthetic-event-v1", synthetic.to_json()
        ).semantic_sha256
    )
    data = event.to_dict()
    data["event_time_ns"] = float(data["event_time_ns"])
    with pytest.raises(ValueError):
        project_semantics("synthetic-event-v1", json.dumps(data))


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_artifact_sha256", "b" * 64),
        ("available_at_ns", 10_000_000_000),
        ("as_of_ns", 13_000_000_000),
        ("policy_id", "policy-2"),
        ("support_start_ns", 1),
        ("omitted_record_count", 1),
    ],
)
def test_source_hash_availability_policy_and_support(artifacts, field, value):
    projection = artifacts["evidence-projection-v1"]
    changed = replace(projection, **{field: value}, projection_id="")
    assert (
        project_semantics(
            "evidence-projection-v1", projection.to_json()
        ).semantic_sha256
        != project_semantics(
            "evidence-projection-v1", changed.to_json()
        ).semantic_sha256
    )


def test_decimal_lexemes_plugin_identity_and_terminal_outcome(artifacts):
    event = artifacts["broker-event-v1"]
    changed = replace(event, quote=replace(event.quote, bid="1.0"))
    assert (
        project_semantics("broker-event-v1", event.to_json()).semantic_sha256
        != project_semantics(
            "broker-event-v1", changed.to_json()
        ).semantic_sha256
    )
    metadata = artifacts["broker-metadata-v1"]
    changed = replace(metadata, plugin_version="1.1.0")
    assert (
        project_semantics(
            "broker-metadata-v1", metadata.to_json()
        ).semantic_sha256
        != project_semantics(
            "broker-metadata-v1", changed.to_json()
        ).semantic_sha256
    )
    manifest = artifacts["broker-lifecycle-manifest-v1"]
    changed = replace(
        manifest,
        state=BrokerLifecycleState.STOPPED,
        completion=BrokerLifecycleCompletion.PARTIAL,
        unknown_loss=True,
    )
    assert (
        project_semantics(
            "broker-lifecycle-manifest-v1", manifest.to_json()
        ).semantic_sha256
        != project_semantics(
            "broker-lifecycle-manifest-v1", changed.to_json()
        ).semantic_sha256
    )


def test_native_numeric_lexeme_timezones_and_forecast_outcome_stay_semantic(
    artifacts,
):
    release = artifacts["economic-release-v1"]
    changed = replace(release, actual_lexical="3.000", release_id="")
    assert (
        project_semantics(
            "economic-release-v1", release.to_json()
        ).semantic_sha256
        != project_semantics(
            "economic-release-v1", changed.to_json()
        ).semantic_sha256
    )
    data = artifacts["forecast-score-v1"].to_dict()
    data["snapshot"]["future_unknown_field"] = 1
    with pytest.raises(ValueError):
        project_semantics("forecast-score-v1", json.dumps(data))


def test_projection_does_not_trust_resealed_false_semantic_hash(artifacts):
    raw = artifacts["synthetic-event-v1"].to_json()
    proof = project_semantics("synthetic-event-v1", raw)
    forged = replace(proof, semantic_json='{"meaning":"forged"}')
    with pytest.raises(ValueError, match="does not recompute"):
        verify_semantic_projection(forged, raw)


def test_forecast_actual_outcomes_and_training_target_support_change_meaning(
    artifacts,
):
    score = artifacts["forecast-score-v1"]
    from histdatacom.forecasting import ForecastScoreV1
    from tests.fixtures.forecast_contracts_v1 import calendar_fixture

    changed_corpus = calendar_fixture(actual=4.0)
    changed_score = ForecastScoreV1(
        score.snapshot_json,
        changed_corpus.to_json(),
        changed_corpus.coverage_end_ns,
    )
    assert score.metrics != changed_score.metrics
    assert (
        project_semantics("forecast-score-v1", score.to_json()).semantic_sha256
        != project_semantics(
            "forecast-score-v1", changed_score.to_json()
        ).semantic_sha256
    )
    batch = artifacts["training-temporal-batch-v1"]
    changed_batch = replace(
        batch, maximum_dependency_span_ns=batch.maximum_dependency_span_ns + 1
    )
    assert (
        project_semantics(
            "training-temporal-batch-v1", batch.to_json()
        ).semantic_sha256
        != project_semantics(
            "training-temporal-batch-v1", changed_batch.to_json()
        ).semantic_sha256
    )


def test_native_clock_exactness_above_float_precision_and_timezone_claim(
    artifacts,
):
    event = artifacts["synthetic-event-v1"]
    first = replace(event, event_time_ns=2**53, event_id="")
    second = replace(event, event_time_ns=2**53 + 1, event_id="")
    assert (
        project_semantics("synthetic-event-v1", first.to_json()).semantic_sha256
        != project_semantics(
            "synthetic-event-v1", second.to_json()
        ).semantic_sha256
    )
    release = artifacts["economic-release-v1"]
    changed = replace(release, source_timezone="Etc/UTC", release_id="")
    assert (
        project_semantics(
            "economic-release-v1", release.to_json()
        ).semantic_sha256
        != project_semantics(
            "economic-release-v1", changed.to_json()
        ).semantic_sha256
    )

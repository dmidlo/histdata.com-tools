"""Independent canonical math and strict wire mutation controls for #718."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom.experiments import (
    ArtifactReferenceV1,
    ComponentField,
    EvidenceKind,
    ExperimentBundleV1,
    ExperimentResultV1,
    MetricDefinitionV1,
    MetricPayloadV1,
    ResultState,
    RetainedFixtureV1,
    fixture_specification,
    scientific_differences,
)
from histdatacom.experiments._wire import canonical_json, load_json
from tests.fixtures.experiment_bundles import (
    bundle_fixture,
    metrics,
    native_fixture,
)


def oracle(kind, payload):
    text = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    return (
        f"experiment-{kind}:sha256:"
        + hashlib.sha256(text.encode("ascii")).hexdigest()
    )


def test_independent_scientific_and_result_identity_oracle():
    bundle, _ = bundle_fixture()
    assert bundle.experiment_id == oracle(
        "scientific",
        {"version": 1, "inputs": bundle.scientific_inputs.to_payload()},
    )
    result = ExperimentResultV1(
        bundle.experiment_id,
        metrics().metrics[0].artifact_id,
        "fixture-all",
        MetricPayloadV1(ResultState.AVAILABLE, 1.25, 4, None, None, None),
    )
    output_hash = hashlib.sha256(
        json.dumps(
            result.payload.to_payload(), sort_keys=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    assert result.result_id == oracle(
        "metric-result",
        {
            "experiment_id": bundle.experiment_id,
            "metric_id": result.metric_id,
            "stratum_id": "fixture-all",
            "output_payload_hash": output_hash,
        },
    )


def test_order_display_invariance_and_full_envelope_identity():
    bundle, _ = bundle_fixture()
    wire = bundle.to_dict()
    reordered = {key: wire[key] for key in reversed(wire)}
    assert ExperimentBundleV1.from_dict(reordered) == bundle
    changed = replace(
        bundle, display_title="A new title", descriptive_branch="other"
    )
    assert changed.experiment_id == bundle.experiment_id
    assert changed.artifact_id != bundle.artifact_id
    assert scientific_differences(bundle, changed) == ()
    assert ExperimentBundleV1.from_json(changed.to_json()) == changed
    with pytest.raises(FrozenInstanceError):
        bundle.display_title = "mutated"


@pytest.mark.parametrize(
    "field",
    [field.value for field in ComponentField]
    + [
        "fit_policy",
        "runtime",
        "lock",
        "sbom",
        "replay_policy",
        "hypothesis_id",
        "commit",
        "namespace",
        "seeds",
    ],
)
def test_every_material_component_changes_scientific_identity(field):
    a, _ = bundle_fixture()
    value = (
        (11,)
        if field == "seeds"
        else "c" * 40 if field == "commit" else "changed"
    )
    b, _ = bundle_fixture(changes={field: value})
    assert a.experiment_id != b.experiment_id
    assert scientific_differences(a, b)


@pytest.mark.parametrize(
    "changes, expected",
    [
        (
            {"feature_projection": "strategies", "model_weights": "new"},
            ("components.feature_projection", "components.model_weights"),
        ),
        ({"decision_policy": "threshold"}, ("components.decision_policy",)),
        (
            {"dataset_products": "transported", "transport": "mapping"},
            ("components.dataset_products", "components.transport"),
        ),
        ({"uncertainty": "members"}, ("components.uncertainty",)),
    ],
)
def test_exact_comparison_coordinates(changes, expected):
    a, _ = bundle_fixture()
    b, _ = bundle_fixture(changes=changes)
    assert scientific_differences(a, b) == expected


def test_metric_semantics_have_content_identity_and_registry_binding():
    registry = metrics()
    metric = registry.metrics[0]
    changed = replace(metric, formula="different formula")
    assert metric.artifact_id != changed.artifact_id
    assert (
        replace(registry, metrics=(changed,)).artifact_id
        != registry.artifact_id
    )
    wire = metric.to_dict()
    wire["payload"]["formula"] = "changed without changing ID"
    with pytest.raises(ValueError, match="identity"):
        MetricDefinitionV1.from_dict(wire)


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), float("-inf"), True, 1, "1.0"]
)
def test_strict_finite_metric_scalar(value):
    with pytest.raises(ValueError):
        MetricPayloadV1(ResultState.AVAILABLE, value, 1, None, None, None)


def test_available_unavailable_and_uncertainty_boundaries():
    MetricPayloadV1(ResultState.UNAVAILABLE, None, 0, None, None, "no support")
    MetricPayloadV1(ResultState.AVAILABLE, -0.0, 1, -1.0, 1.0, None)
    for args in [
        (ResultState.AVAILABLE, 0.0, 0, None, None, None),
        (ResultState.UNAVAILABLE, 1.0, 0, None, None, "missing"),
        (ResultState.AVAILABLE, 1.0, 1, 2.0, 1.0, None),
        (ResultState.UNAVAILABLE, None, 0, 0.0, 1.0, "missing"),
    ]:
        with pytest.raises(ValueError):
            MetricPayloadV1(*args)


def test_large_external_reference_is_metadata_only(monkeypatch):
    monkeypatch.setattr(
        "builtins.open", lambda *a, **k: pytest.fail("no file read")
    )
    reference = ArtifactReferenceV1(
        "external:model",
        "model:declared",
        "a" * 64,
        100 * 1024**3,
        EvidenceKind.DECLARED_EXTERNAL,
    )
    assert reference.byte_length == 100 * 1024**3
    assert len(canonical_json(reference)) < 400


def test_native_fixture_replays_real_reader_and_refuses_resealed_unknowns():
    retained = native_fixture()
    assert RetainedFixtureV1.from_json(retained.to_json()) == retained
    payload = json.loads(retained.payload_json)
    payload["events"][0]["unrecognized_provenance"] = "claimed"
    encoded = canonical_json(payload)
    reference = replace(
        retained.reference,
        sha256=hashlib.sha256(encoded.encode()).hexdigest(),
        byte_length=len(encoded),
    )
    with pytest.raises(ValueError, match="unknown|noncanonical"):
        RetainedFixtureV1(reference, encoded)
    with pytest.raises(ValueError, match="identity"):
        RetainedFixtureV1(
            replace(retained.reference, native_id="not-the-stream"),
            retained.payload_json,
        )


def test_strict_nested_fields_and_canonical_text():
    bundle, _ = bundle_fixture()
    wire = bundle.to_dict()
    wire["payload"]["scientific_inputs"]["environment"]["extra"] = "future"
    with pytest.raises(ValueError, match="field"):
        ExperimentBundleV1.from_dict(wire)
    with pytest.raises(ValueError, match="noncanonical"):
        ExperimentBundleV1.from_json(json.dumps(bundle.to_dict(), indent=2))
    with pytest.raises(ValueError, match="duplicate"):
        load_json('{"x":1,"x":1}')


def test_bounds_before_repeated_alias_or_string_expansion():
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="bound"):
        fixture_specification("cyclic", cyclic)
    dag = ["x"]
    for _ in range(8):
        dag = [dag] * 8
    with pytest.raises(ValueError, match="bound"):
        canonical_json(dag)
    with pytest.raises(ValueError, match="bound"):
        canonical_json(["😀" * 10000] * 100)
    with pytest.raises(ValueError, match="collection"):
        canonical_json([None] * 4097)
    with pytest.raises(ValueError, match="integer"):
        canonical_json(2**63)


def test_direct_constructor_detaches_mutable_inputs():
    bundle, _ = bundle_fixture()
    inputs = bundle.scientific_inputs.to_payload()
    restored = type(bundle.scientific_inputs).from_payload(inputs)
    inputs["components"].clear()
    assert len(restored.components) == len(ComponentField)


def test_requirement_asset_has_resolved_public_atoms_and_explicit_nonclaims():
    import ast
    from importlib.resources import files
    from pathlib import Path
    from histdatacom import experiments

    asset = json.loads(
        files(experiments).joinpath("assets/requirements_v1.json").read_text()
    )
    assert asset["global_525_complete"] is asset["global_691_complete"] is False
    assert asset["scientific_qualification"] == "not_claimed"
    assert len(asset["atoms"]) >= 39
    assert len({a["id"] for a in asset["atoms"]}) == len(asset["atoms"])
    for atom in asset["atoms"]:
        if atom["api"] != "requirements_v1.json":
            assert atom["api"] in experiments.__all__
        filename, test = atom["test"].split("::")
        tree = ast.parse(Path(__file__).with_name(filename).read_text())
        assert test in {
            node.name for node in tree.body if isinstance(node, ast.FunctionDef)
        }

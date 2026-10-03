"""Generated wire parity and traversal bounds, not empirical qualification."""

from collections import Counter
from dataclasses import fields
import hashlib
from unittest.mock import patch

import pytest

from histdatacom.synthetic import bar_features as bar
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.triangle_bar_features import TriangleBarPolicyV1
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionSnapshotV1,
    _projection_cells,
)
from tests.fixtures.triangle_bar_features_v1 import BASE, POST, math_snapshot

FAMILIES = ("bar", "triangle", "projection")
# Captured using the pre-#767 implementation, without publication or native IO.
GOLDEN = {
    "bar": (
        "29a89bdd40637bc1083413a40bd1ee9579aaf100e1a786a28b67c97e01f4ce95",
        "causal-bar-snapshot:sha256:"
        "6025c789e273bdc4dcacc69d1d9c0af3728297272f1620bd01ff6591a1a30a51",
    ),
    "triangle": (
        "5a2ae90fa2655f8220b5d5d2d788a916a3f5fd015fac0830612a208390190eb1",
        "causal-bar-triangle-snapshot:sha256:"
        "7e777b956adcb8ba1ce6db6483d2a1ecb0b7a9cbd7a76e4a835200982163cbc3",
    ),
    "projection": (
        "ce7e51f52571d73298b843a72841ed49bedf05a2e5cda85aa0ab1c5ced1ef4e3",
        "causal-bar-triangle-projection-snapshot:sha256:"
        "51cb7cc323f46eb16f5b258025ea8ec73111c78d1c445674f57acca5ba6492fb",
    ),
}


@pytest.fixture(scope="module")
def artifacts():
    # Actual native constructors and numerical replay validate three generated
    # legs. Observed projection has no invented retained projection evidence.
    policy = TriangleBarPolicyV1(
        bar.BarFeaturePolicyV1(
            POST,
            intervals=("1m",),
            scopes=(ActivitySliceScope.OBSERVED,),
            feature_names=("bid_close",),
            allow_prior_generated_state=True,
        ),
        BASE,
        max_endpoint_age_ns=60_000_000_000,
    )
    triangle = math_snapshot(policy=policy, strata=False)
    projection = TriangleProjectionSnapshotV1(
        triangle, None, _projection_cells(triangle, None)
    )
    return {
        "bar": triangle.leg_snapshots[0],
        "triangle": triangle,
        "projection": projection,
    }


def _legacy_to_dict(self):
    """The exact historical two-traversal algorithm, including nested calls."""
    return {**self.identity_payload(), "artifact_id": self.artifact_id}


def _occurrences(value, depth=0):
    """Independent dataclass traversal; count occurrences, not unique objects."""
    if isinstance(value, bar._Artifact):
        yield id(value), depth
        for field in fields(value):
            yield from _occurrences(getattr(value, field.name), depth + 1)
    elif isinstance(value, (tuple, list)):
        for item in value:
            yield from _occurrences(item, depth)


@pytest.mark.parametrize("family", FAMILIES)
def test_native_canonical_bytes_and_ids_match_historical_algorithm(
    artifacts, family
):
    artifact = artifacts[family]
    with patch.object(bar._Artifact, "to_dict", _legacy_to_dict):
        historical_dict = artifact.to_dict()
        historical_json = artifact.to_json()
    assert artifact.to_dict() == historical_dict
    assert artifact.to_json() == historical_json
    golden_sha, golden_id = GOLDEN[family]
    assert hashlib.sha256(historical_json.encode()).hexdigest() == golden_sha
    assert artifact.artifact_id == golden_id
    assert artifact.to_dict()["artifact_id"] == golden_id
    assert type(artifact).from_json(historical_json) == artifact


@pytest.mark.parametrize("family", FAMILIES)
def test_each_output_traverses_each_nested_payload_once(artifacts, family):
    artifact = artifacts[family]
    calls = Counter()
    original = bar._Artifact.identity_payload
    occurrences = tuple(_occurrences(artifact))
    expected = Counter(identity for identity, _ in occurrences)

    def counted(self):
        calls[id(self)] += 1
        return original(self)

    with patch.object(bar._Artifact, "identity_payload", counted):
        for method in (
            artifact.to_dict,
            artifact.to_json,
            lambda: artifact.artifact_id,
            artifact.to_dict,
        ):
            calls.clear()
            method()
            assert calls == expected
        # Deterministic counterfactual: each historical recursive edge doubled
        # the traversal. No timing thresholds or benchmark noise are involved.
        historical_calls = Counter()
        for identity, depth in occurrences:
            historical_calls[identity] += 2 ** (depth + 1)
        calls.clear()
        with patch.object(bar._Artifact, "to_dict", _legacy_to_dict):
            artifact.to_dict()
        assert calls == historical_calls
    if family == "projection":
        # projection -> triangle -> leg snapshot -> feature cell -> value
        assert max(depth for _, depth in occurrences) == 4


def _mutate_containers(value):
    if isinstance(value, dict):
        for item in tuple(value.values()):
            _mutate_containers(item)
        value["artifact_id"] = "detached caller mutation"
    elif isinstance(value, list):
        for item in tuple(value):
            _mutate_containers(item)
        value.append("detached caller mutation")


@pytest.mark.parametrize("family", FAMILIES)
def test_all_returned_nested_containers_are_detached(artifacts, family):
    artifact = artifacts[family]
    before = artifact.to_json()
    wire = artifact.to_dict()
    _mutate_containers(wire)
    assert bar.canonical_bar_feature_json(wire) != before
    assert artifact.to_json() == before


def _cell():
    value = bar.BarFeatureValueV1(
        "bid_close",
        1.25,
        bar.BarFeatureState.AVAILABLE,
        ("derived-bar:sha256:" + "1" * 64,),
        60_000_000_000,
    )
    return bar.BarFeatureCellV1(
        ActivitySliceScope.OBSERVED,
        "1m",
        0,
        60_000_000_000,
        bar.BarFeatureState.AVAILABLE,
        False,
        (value,),
    )


def test_later_calls_recompute_identity_without_persistent_cache():
    artifact = _cell()
    old_json, old_id = artifact.to_json(), artifact.artifact_id
    # Illicit low-level mutation detects stale caches; it is not a supported
    # mutation API, and no authority or source replay is bypassed here.
    object.__setattr__(artifact.values[0], "value", 1.5)
    assert artifact.to_dict()["artifact_id"] == artifact.artifact_id != old_id
    assert artifact.to_json() != old_json
    assert type(artifact).from_json(old_json).to_json() == old_json


@pytest.mark.parametrize(
    "bad",
    (object(), float("nan"), 2**63),
    ids=("unsupported-type", "nonfinite", "outside-int64"),
)
def test_serialization_still_rejects_malformed_values(bad):
    artifact = _cell()
    artifact.to_dict()
    object.__setattr__(artifact.values[0], "value", bad)
    with pytest.raises(ValueError):
        artifact.to_dict()
    with pytest.raises(ValueError):
        artifact.to_json()


def test_serialization_still_rejects_oversized_nested_collections():
    artifact = _cell()
    object.__setattr__(
        artifact.values[0],
        "source_bar_ids",
        ("x",) * (bar.MAX_BAR_FEATURE_COLLECTION_ITEMS + 1),
    )
    with pytest.raises(ValueError, match="collection exceeds"):
        artifact.to_dict()


@pytest.mark.parametrize("family", FAMILIES)
def test_native_reader_still_rejects_forged_ids_and_unknown_fields(
    artifacts, family
):
    artifact = artifacts[family]
    wire = artifact.to_dict()
    for mutation in (
        {**wire, "artifact_id": "forged"},
        {**wire, "unknown": True},
    ):
        with pytest.raises(ValueError):
            type(artifact).from_dict(mutation)


def test_constructor_normalization_and_semantic_validation_remain_active():
    artifact = _cell()
    wire = artifact.to_dict()
    # Recompute both wire IDs after a semantic corruption; native validation
    # must still reject it instead of merely checking identity consistency.
    value = wire["values"][0]
    value["state"] = bar.BarFeatureState.UNAVAILABLE.value
    payload = {key: item for key, item in value.items() if key != "artifact_id"}
    value["artifact_id"] = (
        "causal-bar-value:sha256:"
        + hashlib.sha256(
            bar.canonical_bar_feature_json(payload).encode()
        ).hexdigest()
    )
    payload = {key: item for key, item in wire.items() if key != "artifact_id"}
    wire["artifact_id"] = (
        "causal-bar-cell:sha256:"
        + hashlib.sha256(
            bar.canonical_bar_feature_json(payload).encode()
        ).hexdigest()
    )
    with pytest.raises(ValueError, match="only available features"):
        type(artifact).from_dict(wire)

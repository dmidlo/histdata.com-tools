"""Strict public contracts and complete bounded logical taxonomy fixtures."""

from dataclasses import FrozenInstanceError, replace
from importlib.resources import files
import json
from pathlib import Path

import pytest

from histdatacom.attribution.contracts import (
    AttributionEvidenceKind,
    AttributionFeatureShardV1,
    AttributionFeatureV1,
    AttributionReferenceV1,
    AttributionTaxonomyV1,
    AttributionValueV1,
    ExplanationPolicyRegistryV1,
    PLANES,
    TIMEFRAMES,
    reference,
)
from histdatacom.attribution.taxonomy import (
    attribution_feature_groups,
    resolve_attribution_taxonomy,
)
from tests.fixtures.decision_attribution import (
    declared_reference,
    example_registry,
    reference_inputs,
)


def test_full_executed_registry_canonical_roundtrip_and_immutability():
    registry = example_registry()
    assert type(registry).from_json(registry.to_json()) == registry
    with pytest.raises(FrozenInstanceError):
        registry.attributions = ()
    payload = registry.to_dict()
    payload["extra"] = 1
    with pytest.raises(ValueError):
        type(registry).from_dict(payload)
    with pytest.raises(ValueError):
        type(registry).from_json(json.dumps(registry.to_dict(), indent=2))


@pytest.mark.parametrize(
    "field,value",
    [
        ("absolute_tolerance", True),
        ("relative_tolerance", float("nan")),
        ("maximum_evaluations", 65537),
        ("version", "01.0.0"),
        ("causal_nonclaim", "SHAP proves historical causality"),
    ],
)
def test_exact_policy_types_bounds_nonclaims(field, value):
    with pytest.raises(ValueError):
        replace(reference_inputs()[1], **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("instability_threshold", 0.1),
        ("maximum_evaluations", 100),
        ("absolute_tolerance", 1e-10),
        ("declared_at_ns", 3),
    ],
)
def test_material_policy_changes_require_successor_identity(field, value):
    policy = reference_inputs()[1]
    assert replace(policy, **{field: value}).artifact_id != policy.artifact_id
    with pytest.raises(ValueError, match="reused"):
        policies = tuple(
            sorted(
                (policy, replace(policy, **{field: value})),
                key=lambda p: p.artifact_id,
            )
        )
        ExplanationPolicyRegistryV1("1.0.0", policies)


def test_large_external_reference_does_not_allocate_or_claim_verification():
    ref = AttributionReferenceV1(
        "private.model.v1",
        "external-model",
        "f" * 64,
        2**62,
        AttributionEvidenceKind.DECLARED,
    )
    assert ref.byte_length == 2**62
    assert len(str(ref.to_payload())) < 400


@pytest.mark.parametrize("plane", PLANES)
def test_every_wide_feature_plane_has_public_group_membership(plane):
    options = {}
    if plane in ("market.bar", "market.indicator"):
        options["timeframe"] = "1m"
    if plane == "strategy":
        options.update(
            strategy_id="FXES-1000",
            strategy_family="declared-fixture-family",
            dependency_class="derived_bars",
        )
    feature = AttributionFeatureV1(
        plane + ".fixture",
        plane,
        "fixture-semantic",
        "dimensionless",
        **options,
    )
    assert "plane:" + plane in attribution_feature_groups(feature)
    if plane == "strategy":
        assert len(attribution_feature_groups(feature)) == 5


@pytest.mark.parametrize("width", TIMEFRAMES)
def test_all_seven_timeframes_remain_distinct(width):
    feature = AttributionFeatureV1(
        "market.bar.EURUSD." + width + ".close",
        "market.bar",
        "close.v1",
        "USD_per_EUR",
        width,
    )
    assert "timeframe:" + width in attribution_feature_groups(feature)


def test_ten_thousand_logical_features_use_complete_separate_shards():
    catalog = declared_reference("synthetic-taxonomy-not-compiled-strategies")
    features = tuple(
        AttributionFeatureV1(
            f"strategy.FXES-{i%1000+1:03d}.channel-{i:05d}",
            "strategy",
            "fixture-semantic",
            "dimensionless",
            strategy_id=f"FXES-{i%1000+1:03d}",
            strategy_family=f"fixture-family-{i%5}",
            dependency_class="fixture-bars",
        )
        for i in range(10001)
    )
    features = tuple(sorted(features, key=lambda f: f.name))
    shards = tuple(
        AttributionFeatureShardV1(catalog, features[i : i + 256])
        for i in range(0, len(features), 256)
    )
    refs = tuple(
        sorted((reference(s) for s in shards), key=lambda r: r.native_id)
    )
    taxonomy = AttributionTaxonomyV1("1.0.0", catalog, refs, 10001)
    assert len(resolve_attribution_taxonomy(taxonomy, shards)) == 10001
    selected = (features[0].name, features[-1].name)
    assert resolve_attribution_taxonomy(taxonomy, shards, names=selected) == (
        features[0],
        features[-1],
    )
    with pytest.raises(ValueError):
        resolve_attribution_taxonomy(taxonomy, shards[:-1])
    with pytest.raises(ValueError):
        resolve_attribution_taxonomy(taxonomy, shards, names=("absent",))


def test_imputed_values_require_retained_exact_mask():
    _, _, snapshot, _ = reference_inputs()
    first = replace(snapshot.values[0], imputed=True, mask_name="mask")
    with pytest.raises(ValueError, match="mask"):
        replace(snapshot, values=(first, snapshot.values[1]))
    mask = AttributionValueV1(
        AttributionFeatureV1("mask", "missingness", "mask.v1", "boolean"),
        1.0,
        "fixture",
        1,
    )
    assert (
        replace(snapshot, values=(mask, first, snapshot.values[1]))
        .values[1]
        .imputed
    )
    with pytest.raises(ValueError, match="mask"):
        replace(
            snapshot,
            values=(replace(mask, value=0.0), first, snapshot.values[1]),
        )


@pytest.mark.parametrize("version", ["1", "01.0.0", "1.2", "future", "-1.0.0"])
def test_registry_and_taxonomy_use_canonical_release_semver(version):
    registry = example_registry().policy_registry
    with pytest.raises(ValueError, match="SemVer"):
        replace(registry, version=version)
    catalog = declared_reference()
    shard = AttributionFeatureShardV1(
        catalog, (reference_inputs()[2].values[0].feature,)
    )
    with pytest.raises(ValueError, match="SemVer"):
        AttributionTaxonomyV1(version, catalog, (reference(shard),), 1)


def test_packaged_requirement_atoms_are_scoped_and_resolve_tests():
    asset = json.loads(
        files("histdatacom.attribution")
        .joinpath("assets/requirements_v1.json")
        .read_text(encoding="utf-8")
    )
    assert asset["version"] == "1.0.0"
    assert asset["issue"] == 719
    atoms = asset["public_requirements"]
    assert len(atoms) == 39
    assert len({atom["id"] for atom in atoms}) == 39
    root = Path(__file__).resolve().parents[2]
    for atom in atoms:
        assert atom["planned"] and atom["implemented"]
        assert atom["execution_scope"] == "synthetic_conformance_only"
        assert atom["scientifically_passed"] is False
        assert atom["api"] and atom["tests"]
        for selector in atom["tests"]:
            path, function = selector.split("::")
            assert "def " + function + "(" in (root / path).read_text(
                encoding="utf-8"
            )

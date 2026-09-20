"""Independent missingness and native dependency-closure regressions for719."""

from dataclasses import replace
import hashlib
import json

import pytest

from histdatacom.attribution.contracts import (
    AttributionFeatureV1,
    AttributionGroupV1,
    AttributionState,
    AttributionValueV1,
    ContributionV1,
    FeatureSpace,
    PolynomialTermV1,
    ReferenceModelV1,
    reference,
)
from histdatacom.attribution.interventions import (
    NativeTriangleInterventionV1,
    TriangleInterventionScenarioV1,
    make_triangle_intervention,
)
from histdatacom.attribution.mapping import (
    AttributionMappingV1,
    DiagonalFeatureMapV1,
    MappedAttributionV1,
)
from histdatacom.attribution.reference import explain_reference
from tests.fixtures.decision_attribution import reference_inputs


def _masked_mapping(*, renamed=False, swap=(), relabel_mask=False):
    """Run a real four-coordinate model with two distinct imputation masks."""
    _, policy, raw, background = reference_inputs()
    names = ("mask_a", "mask_b", "x1", "x2")
    features = tuple(
        AttributionFeatureV1(
            name,
            "missingness" if name.startswith("mask_") else "market.tick",
            "fixture." + name + ".v1",
            "dimensionless",
        )
        for name in names
    )
    raw = replace(
        raw,
        values=tuple(
            AttributionValueV1(
                feature,
                1.0,
                "source-" + feature.name,
                10,
                feature.name in ("x1", "x2"),
                {"x1": "mask_a", "x2": "mask_b"}.get(feature.name),
            )
            for feature in features
        ),
    )
    baseline = replace(
        raw,
        values=tuple(
            replace(
                value,
                value=1.0 if value.feature.plane == "missingness" else 0.0,
                available_at_ns=1,
            )
            for value in raw.values
        ),
        cutoff_at_ns=1,
    )
    background = replace(background, snapshots=(baseline,))
    forward = {
        name: "normalized." + name if renamed else name for name in names
    }
    mapping = AttributionMappingV1(
        tuple(
            DiagonalFeatureMapV1(name, forward[name], 1.0, 0.0)
            for name in names
        ),
        0,
    )

    def transform(snapshot, role):
        values = []
        for value in snapshot.values:
            feature = replace(value.feature, name=forward[value.feature.name])
            if relabel_mask and value.feature.name == "mask_a":
                feature = replace(feature, semantic_id="unrelated-mask.v1")
            mask = value.mask_name
            if role in swap and mask is not None:
                mask = {"mask_a": "mask_b", "mask_b": "mask_a"}[mask]
            values.append(
                replace(
                    value,
                    feature=feature,
                    mask_name=None if mask is None else forward[mask],
                )
            )
        return replace(
            snapshot,
            space=FeatureSpace.TRANSFORMED,
            preprocessing=reference(mapping),
            values=tuple(values),
        )

    transformed_names = tuple(forward.values())
    model = ReferenceModelV1(
        transformed_names,
        (PolynomialTermV1(1.0, (forward["x1"],)),),
        "fixture.response",
        "dimensionless",
    )
    policy = replace(
        policy,
        groups=(AttributionGroupV1("all", None, transformed_names, "plane"),),
        reporting_cut=("all",),
    )
    result = explain_reference(
        model,
        policy,
        transform(raw, "target"),
        replace(background, snapshots=(transform(baseline, "background"),)),
        generated_at_ns=20,
    )
    # Refusal must come from the mapping boundary, not an unsupported model.
    assert result.state is AttributionState.IDENTIFIED
    backward = {
        transformed: original for original, transformed in forward.items()
    }
    contributions = tuple(
        ContributionV1(backward[value.name], value.value)
        for value in result.contributions
    )
    return result, mapping, raw, background, contributions


@pytest.mark.parametrize("renamed", (False, True))
def test_exact_mask_axis_correspondence_remains_mappable(renamed):
    mapped = MappedAttributionV1(*_masked_mapping(renamed=renamed))
    assert MappedAttributionV1.from_json(mapped.to_json()) == mapped
    raw = {value.feature.name: value for value in mapped.raw_snapshot.values}
    transformed = {
        value.feature.name: value for value in mapped.source.snapshot.values
    }
    axes = {
        axis.raw_name: axis.transformed_name for axis in mapped.mapping.axes
    }
    for name in ("x1", "x2"):
        assert transformed[axes[name]].mask_name == axes[raw[name].mask_name]


@pytest.mark.parametrize("renamed", (False, True))
@pytest.mark.parametrize(
    "swap", (("target",), ("background",), ("target", "background"))
)
def test_swapped_equal_valued_masks_cannot_claim_coordinate_mapping(
    renamed, swap
):
    arguments = _masked_mapping(renamed=renamed, swap=swap)
    with pytest.raises(ValueError):
        MappedAttributionV1(*arguments)


def test_renamed_mask_cannot_change_its_missingness_semantics():
    arguments = _masked_mapping(renamed=True, relabel_mask=True)
    with pytest.raises(ValueError):
        MappedAttributionV1(*arguments)


def _resealed_mapping_json(mapped, change):
    """Recompute the real outer hash, not merely submit a stale-ID mutation."""
    envelope = json.loads(mapped.to_json())
    envelope["payload"]["raw_background"].update(change)

    def encode(value):
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )

    subject = {
        "schema_version": envelope["schema_version"],
        "payload": envelope["payload"],
    }
    envelope["artifact_id"] = (
        "attribution-mapped:sha256:"
        + hashlib.sha256(encode(subject).encode("ascii")).hexdigest()
    )
    return encode(envelope)


def test_raw_background_metadata_preservation_has_positive_wire_control():
    mapped = MappedAttributionV1(*_masked_mapping(renamed=True))
    assert mapped.source.state is AttributionState.IDENTIFIED
    assert mapped.raw_background.role == mapped.source.background.role
    assert mapped.raw_background.role == "synthetic_fixture"
    assert (
        mapped.raw_background.declared_at_ns
        == mapped.source.background.declared_at_ns
        == 2
    )
    encoded = _resealed_mapping_json(mapped, {})
    assert encoded == mapped.to_json()
    assert MappedAttributionV1.from_json(encoded) == mapped


@pytest.mark.parametrize("route", ("constructor", "resealed_json"))
@pytest.mark.parametrize(
    "change",
    (
        pytest.param({"role": "protected"}, id="protected"),
        pytest.param({"role": "declared_train"}, id="declared-train"),
        pytest.param({"role": "unknown"}, id="unknown"),
        pytest.param({"declared_at_ns": 0}, id="before-background-cutoff"),
        pytest.param({"declared_at_ns": 3}, id="different-valid-clock"),
        pytest.param({"declared_at_ns": 21}, id="after-explanation"),
    ),
)
def test_mapping_cannot_reclassify_or_redate_raw_background(change, route):
    mapped = MappedAttributionV1(*_masked_mapping(renamed=True))
    assert mapped.source.state is AttributionState.IDENTIFIED
    assert mapped.source.generated_at_ns == 20
    # Even a separately valid declaration at3 changes the retained coalition
    # background. This is metadata equality, not only a no-future-clock check.
    if route == "constructor":
        changed_background = replace(mapped.raw_background, **change)
        with pytest.raises(ValueError):
            replace(mapped, raw_background=changed_background)
    else:
        encoded = _resealed_mapping_json(mapped, change)
        with pytest.raises(ValueError):
            MappedAttributionV1.from_json(encoded)


def test_arbitrary_price_dependent_context_is_not_a_closed_intervention():
    stale = AttributionValueV1(
        AttributionFeatureV1(
            "market.indicator.EURUSD.twice",
            "market.indicator",
            "twice-current-eurusd.v1",
            "USD_per_EUR",
            timeframe="1m",
        ),
        2.0,
        "derived-from-eurusd-1",
        10,
    )
    # Both the old price and this derived value are internally consistent.
    # Keeping the value at2 after EURUSD becomes2 would certify a stale feature.
    with pytest.raises(ValueError):
        scenario = TriangleInterventionScenarioV1(
            1.0, 2.0, 10, 10, 20, 0, 100, "fixture", unchanged_context=(stale,)
        )
        make_triangle_intervention(scenario, eurusd=2.0, gbpusd=2.0)


def test_context_free_intervention_retains_native_dependency_regeneration():
    scenario = TriangleInterventionScenarioV1(
        1.0, 2.0, 10, 10, 20, 0, 100, "fixture"
    )
    intervention = make_triangle_intervention(scenario, eurusd=2.0, gbpusd=2.0)
    values = {
        value.feature.name: value.value
        for value in intervention.after_snapshot.values
    }
    assert values == {
        "market.tick.eurgbp.mid": 1.0,
        "market.tick.eurusd.mid": 2.0,
        "market.tick.gbpusd.mid": 2.0,
        "triangle.event_residual": 0.0,
    }
    assert (
        NativeTriangleInterventionV1.from_json(intervention.to_json())
        == intervention
    )

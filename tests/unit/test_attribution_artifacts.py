"""Independent synthetic checks of durable attribution and replay boundaries."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from histdatacom.attribution import artifacts as io
from histdatacom.attribution.artifacts import (
    artifact_filename,
    read_attribution_artifact,
    write_attribution_artifact,
)

from histdatacom.attribution.contracts import (
    AttributionBackgroundV1,
    AttributionEvidenceKind,
    AttributionFeatureV1,
    AttributionGroupV1,
    AttributionReferenceV1,
    AttributionSnapshotV1,
    AttributionValueV1,
    ExplanationPolicyV1,
    FeatureSpace,
    GroupMethod,
    PolynomialTermV1,
    ReferenceModelV1,
)
from histdatacom.attribution.reference import explain_reference
from histdatacom.attribution.registry import AttributionRegistryV1
from tests.fixtures.decision_attribution import example_registry


@pytest.fixture(scope="module")
def registry():
    return example_registry()


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )


def _resealed_path(tmp_path, wire):
    # Independent envelope hashing deliberately bypasses the production writer.
    # A content-address check alone therefore cannot detect these forgeries.
    digest = hashlib.sha256(
        _canonical(
            {
                "schema_version": wire["schema_version"],
                "payload": wire["payload"],
            }
        ).encode("ascii")
    ).hexdigest()
    wire["artifact_id"] = "attribution-registry:sha256:" + digest
    path = tmp_path / f"registry-{digest}.json"
    path.write_text(_canonical(wire), encoding="ascii")
    return path


def test_canonical_roundtrip_and_idempotent_publication(tmp_path, registry):
    path = write_attribution_artifact(registry, tmp_path)
    original = path.read_bytes()
    assert path.name == artifact_filename(registry)
    assert original == registry.to_json().encode("ascii")
    assert read_attribution_artifact(path, AttributionRegistryV1) == registry
    assert write_attribution_artifact(registry, tmp_path) == path
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "mutation",
    (
        "balanced_singletons",
        "balanced_groups",
        "interaction",
        "background_range",
        "sibling_sensitivity",
        "balanced_outputs",
        "evaluations",
        "reason",
        "state",
        "group_residual",
    ),
)
def test_resealed_outputs_require_actual_model_replay(
    tmp_path, registry, mutation
):
    wire = json.loads(registry.to_json())
    attribution = wire["payload"]["attributions"][0]
    if mutation in ("balanced_singletons", "balanced_groups"):
        field = (
            "contributions" if mutation == "balanced_singletons" else "groups"
        )
        attribution[field][0]["value"] += 0.25
        attribution[field][1]["value"] -= 0.25
    elif mutation == "interaction":
        attribution["interactions"][0]["value"] += 0.25
    elif mutation in ("background_range", "sibling_sensitivity"):
        attribution["ambiguity"][0][mutation] += 0.25
    elif mutation == "balanced_outputs":
        attribution["raw_output"] += 1.0
        attribution["baseline_output"] += 1.0
    elif mutation == "evaluations":
        attribution["evaluations"] += 1
    elif mutation == "reason":
        attribution["reason"] = "invented_reference_diagnostic"
    elif mutation == "state":
        attribution["state"] = "attribution_nonidentifiable"
    else:
        attribution["group_additive_residual"] = 0.25
    path = _resealed_path(tmp_path, wire)
    with pytest.raises(ValueError, match="exact model/explainer replay"):
        read_attribution_artifact(path, AttributionRegistryV1)


def test_writer_replays_mutated_frozen_object_before_temporary_file(tmp_path):
    registry = example_registry()
    object.__setattr__(registry.attributions[0], "raw_output", 42.0)
    with pytest.raises(ValueError, match="exact model/explainer replay"):
        write_attribution_artifact(registry, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_wrong_name_noncanonical_encoding_and_wrong_type(tmp_path, registry):
    path = write_attribution_artifact(registry, tmp_path)
    wrong = tmp_path / "wrong.json"
    wrong.write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match="filename"):
        read_attribution_artifact(wrong, AttributionRegistryV1)
    with pytest.raises(ValueError, match="wrong attribution schema"):
        read_attribution_artifact(path, ExplanationPolicyV1)
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="noncanonical"):
        read_attribution_artifact(path, AttributionRegistryV1)


@pytest.mark.parametrize("malformation", ("duplicate", "unknown", "bool"))
def test_reader_rejects_ambiguous_or_wrongly_typed_wire(
    tmp_path, registry, malformation
):
    wire = json.loads(registry.to_json())
    if malformation == "unknown":
        wire["payload"]["attributions"][0]["claimed_verified"] = True
    elif malformation == "bool":
        wire["payload"]["attributions"][0]["generated_at_ns"] = True
    path = _resealed_path(tmp_path, wire)
    if malformation == "duplicate":
        path.write_text(
            path.read_text(encoding="ascii").replace(
                '"evaluations":12', '"evaluations":12,"evaluations":12'
            ),
            encoding="ascii",
        )
        assert path.read_text(encoding="ascii").count('"evaluations"') == 2
    with pytest.raises(ValueError, match="duplicate|field|scalar"):
        read_attribution_artifact(path, AttributionRegistryV1)


def test_oversized_file_rejected_before_json_reader(
    tmp_path, registry, monkeypatch
):
    path = tmp_path / artifact_filename(registry)
    with path.open("wb") as handle:
        handle.truncate(io.MAX_BYTES + 1)

    def unexpected_decode(*args, **kwargs):
        pytest.fail("oversized file reached JSON decoder")

    monkeypatch.setattr(AttributionRegistryV1, "from_json", unexpected_decode)
    with pytest.raises(ValueError, match="bounded"):
        read_attribution_artifact(path, AttributionRegistryV1)


def _symlink(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except NotImplementedError:
        pytest.skip("symlinks unsupported")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege unavailable")
        raise


def test_reader_and_writer_refuse_symlink_parent(tmp_path, registry):
    real = tmp_path / "real"
    real.mkdir()
    path = write_attribution_artifact(registry, real)
    alias = tmp_path / "alias"
    _symlink(alias, real, directory=True)
    with pytest.raises(ValueError, match="nonsymlink"):
        read_attribution_artifact(alias / path.name, AttributionRegistryV1)
    with pytest.raises(ValueError, match="nonsymlink"):
        write_attribution_artifact(registry, alias)
    assert path.read_bytes() == registry.to_json().encode("ascii")


def test_reader_and_writer_refuse_symlink_leaf(tmp_path, registry):
    sentinel = tmp_path / "sentinel.json"
    sentinel.write_bytes(b"retain unrelated fixture bytes")
    path = tmp_path / artifact_filename(registry)
    _symlink(path, sentinel)
    with pytest.raises(ValueError, match="regular"):
        read_attribution_artifact(path, AttributionRegistryV1)
    with pytest.raises(ValueError, match="regular"):
        write_attribution_artifact(registry, tmp_path)
    assert path.is_symlink()
    assert sentinel.read_bytes() == b"retain unrelated fixture bytes"
    assert list(tmp_path.glob(".attribution-*")) == []


def test_existing_conflict_is_preserved(tmp_path, registry):
    path = tmp_path / artifact_filename(registry)
    path.write_bytes(b"preexisting distinct evidence")
    with pytest.raises(ValueError, match="overwrite"):
        write_attribution_artifact(registry, tmp_path)
    assert path.read_bytes() == b"preexisting distinct evidence"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("same_content", (True, False))
def test_competing_publisher_is_idempotent_or_refused(
    tmp_path, registry, monkeypatch, same_content
):
    native_link = io.os.link
    path = tmp_path / artifact_filename(registry)
    competing = (
        registry.to_json().encode("ascii") if same_content else b"other writer"
    )

    def race(source, destination, **kwargs):
        Path(destination).write_bytes(competing)
        return native_link(source, destination, **kwargs)

    monkeypatch.setattr(io.os, "link", race)
    if same_content:
        assert write_attribution_artifact(registry, tmp_path) == path
    else:
        with pytest.raises(ValueError, match="overwrite"):
            write_attribution_artifact(registry, tmp_path)
    assert path.read_bytes() == competing
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("phase", ("file_fsync", "link", "directory_fsync"))
def test_publication_failure_cleans_temporary_and_preserves_complete_bytes(
    tmp_path, registry, monkeypatch, phase
):
    def fail(*args, **kwargs):
        raise OSError("synthetic publication interruption")

    if phase == "file_fsync":
        monkeypatch.setattr(io.os, "fsync", fail)
    elif phase == "link":
        monkeypatch.setattr(io.os, "link", fail)
    else:
        monkeypatch.setattr(io, "_fsync_directory", fail)
    with pytest.raises(OSError, match="synthetic publication interruption"):
        write_attribution_artifact(registry, tmp_path)
    path = tmp_path / artifact_filename(registry)
    if phase == "directory_fsync":
        # Link already succeeded; durability was not reported as successful.
        assert path.read_bytes() == registry.to_json().encode("ascii")
        assert list(tmp_path.iterdir()) == [path]
    else:
        assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mutation", ("append", "replace"))
def test_read_mutation_refused_even_when_replacement_bytes_match(
    tmp_path, registry, monkeypatch, mutation
):
    path = write_attribution_artifact(registry, tmp_path)
    original = path.read_bytes()
    native_read = io.os.read
    changed = False

    def race(descriptor, count):
        nonlocal changed
        result = native_read(descriptor, count)
        if not changed:
            changed = True
            if mutation == "append":
                with path.open("ab") as handle:
                    handle.write(b" ")
            else:
                replacement = tmp_path / "replacement.json"
                replacement.write_bytes(original)
                replacement.replace(path)
        return result

    monkeypatch.setattr(io.os, "read", race)
    with pytest.raises(ValueError, match="changed during read"):
        read_attribution_artifact(path, AttributionRegistryV1)


@pytest.mark.skipif(
    not hasattr(os, "mkfifo") or not hasattr(os, "O_NONBLOCK"),
    reason="requires POSIX FIFO and nonblocking open",
)
def test_real_raced_fifo_is_nonblocking_and_refused(
    tmp_path, registry, monkeypatch
):
    path = write_attribution_artifact(registry, tmp_path)
    native_open = io.os.open

    def race(target, flags, *args, **kwargs):
        if Path(target) == path:
            assert flags & os.O_NONBLOCK
            if hasattr(os, "O_NOFOLLOW"):
                assert flags & os.O_NOFOLLOW
            path.unlink()
            os.mkfifo(path)
        return native_open(target, flags, *args, **kwargs)

    monkeypatch.setattr(io.os, "open", race)
    with pytest.raises(ValueError, match="changed before read"):
        read_attribution_artifact(path, AttributionRegistryV1)


@pytest.mark.parametrize("method", (GroupMethod.SUM, GroupMethod.DIRECT))
def test_serialized_group_conservation_is_not_implied_by_singletons(method):
    # Deliberately numerical-only fixture: no native data provenance claim.
    # In exact arithmetic, singleton contributions are (10**16, 1, -10**16).
    # They serialize exactly and sum to the model output 1. Grouping the first
    # two rounds their sum to 10**16, so the serialized groups instead sum to 0.
    # A verified artifact must not retain that false group-level explanation.
    names = ("a", "b", "c")
    config = AttributionReferenceV1(
        "histdatacom.attribution-test-configuration.v1",
        "explicit-synthetic-numerical-fixture",
        "a" * 64,
        1,
        AttributionEvidenceKind.FIXTURE,
    )
    features = tuple(
        AttributionFeatureV1(name, "market.tick", name, "dimensionless")
        for name in names
    )

    def snapshot(value, cutoff):
        return AttributionSnapshotV1(
            tuple(
                AttributionValueV1(feature, value, "fixture", cutoff)
                for feature in features
            ),
            cutoff,
            FeatureSpace.RAW,
            config,
            config,
            config,
            ("EURUSD",),
            "synthetic-session",
            "synthetic-numerical-reference",
        )

    model = ReferenceModelV1(
        names,
        (
            PolynomialTermV1(1e16, ("a",)),
            PolynomialTermV1(1.0, ("b",)),
            PolynomialTermV1(-1e16, ("c",)),
        ),
        "reference-score",
        "dimensionless",
    )
    policy = ExplanationPolicyV1(
        "1.0.0",
        "exact-polynomial-coalitions.v1",
        method,
        (
            AttributionGroupV1("ab", None, ("a", "b"), "plane"),
            AttributionGroupV1("c", None, ("c",), "plane"),
        ),
        ("ab", "c"),
        1e-6,
        0.0,
        0.0,
        1024,
        "background-range-and-sibling-ablation.v1",
        5,
    )
    background = AttributionBackgroundV1(
        (snapshot(0.0, 5),), 6, "synthetic_fixture"
    )
    with pytest.raises(ValueError, match="group|additive|serialization"):
        explain_reference(
            model, policy, snapshot(1.0, 10), background, generated_at_ns=11
        )

"""Bounded research-shard storage using newly generated synthetic fixtures."""

from dataclasses import replace
import errno
import hashlib
import os
from pathlib import Path
import shutil
from unittest.mock import patch

import pytest

from histdatacom.data_quality import training_weight_artifacts as artifacts
from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_weight_candidates import (
    generate_training_weight_candidate_shard,
)
from histdatacom.data_quality.training_weight_lineage import (
    create_training_weight_degradation,
)
from tests.fixtures.training_weight_sources import (
    fixture_weight_model,
    fixture_weight_source,
)


@pytest.fixture(scope="module")
def bundle(tmp_path_factory):
    root = tmp_path_factory.mktemp("research-weight-shard")
    plan, _ = fixture_weight_source(
        root / "source", dates=("2010-01-04",), step_seconds=8
    )
    degradation = create_training_weight_degradation(plan, root / "subset")
    model = fixture_weight_model(root / "model")
    shard = generate_training_weight_candidate_shard(
        degradation, "201001-1", model
    )
    assert len(shard.days) == 1 and len(shard.refusals) == 7
    original = artifacts.replay_training_weight_candidate_shard
    # Real fresh-source/model/generator replay proves both default public paths.
    with patch.object(
        artifacts, "replay_training_weight_candidate_shard", wraps=original
    ) as replay:
        path = artifacts.write_training_weight_candidate_shard(
            shard, root / "artifacts"
        )
        assert artifacts.read_training_weight_candidate_shard(path) == shard
        assert replay.call_count == 2
    return shard, path


def copy_bundle(bundle, directory):
    shard, original = bundle
    shutil.copytree(original.parent, directory)
    return shard, directory / original.name


def read_manifest(path):
    return artifacts.TrainingWeightCandidateShardManifestV1.from_json(
        path.read_text("ascii")
    )


def save_manifest(manifest, directory):
    payload = manifest.to_json().encode("ascii")
    path = (
        directory
        / f"weight-candidate-shard-{hashlib.sha256(payload).hexdigest()}.json"
    )
    path.write_bytes(payload)
    return path


def test_real_evidence_claim_requires_approval_before_any_component_read(
    tmp_path, monkeypatch
):
    from histdatacom.data_quality.training_weight_lineage import (
        TrainingWeightRefusedDayV1,
        WeightDayRefusal,
        WeightEvidenceKind,
    )

    def reference(kind):
        return artifacts.TrainingWeightComponentV1(
            kind,
            artifacts._KINDS[kind] + ":sha256:" + "a" * 64,
            artifacts.TrainingWeightFileV1(
                f"weight-{kind}-{'b' * 64}.json", 8, "b" * 64
            ),
        )

    # A deliberately unsupported declaration is not source evidence or approval.
    manifest = artifacts.TrainingWeightCandidateShardManifestV1(
        WeightEvidenceKind.PREREGISTERED,
        "201001-1",
        reference("source-plan"),
        reference("parent-ownership"),
        None,
        None,
        (),
        (
            TrainingWeightRefusedDayV1(
                "2010-01-01", WeightDayRefusal.MISSING_BOUNDARY
            ),
        ),
        reference("model"),
        (),
        (
            artifacts.TrainingWeightCandidateRefusalV1(
                "2010-01-01", "source:unavailable"
            ),
        ),
    )
    path = save_manifest(manifest, tmp_path)
    original = artifacts.read_training_regular

    def read(target, maximum):
        assert Path(target) == path, "approval must precede component reads"
        return original(target, maximum)

    monkeypatch.setattr(artifacts, "read_training_regular", read)
    with pytest.raises(ValueError, match="explicit coordinator approval"):
        artifacts.read_training_weight_candidate_shard(path)


def test_actual_write_and_read_replay_complete_research_inventory(bundle):
    shard, path = bundle
    manifest = read_manifest(path)
    assert len(manifest.days) == 1 and len(manifest.refusals) == 7
    assert len(manifest.bridges) == 1 and len(manifest.source_refusals) == 20
    assert (
        manifest.evidence_kind.value
        == "synthetic_contract_fixture_not_empirical_evidence"
    )
    assert all("production" not in item.kind for item in manifest.components)
    assert all(
        item.file.size_bytes <= artifacts.MAX_TRAINING_BYTES
        for item in manifest.components
    )
    assert len(shard.days[0].candidates) == 12
    assert all(
        candidate.status == "research_candidate_not_production_qualified"
        for candidate in shard.days[0].candidates
    )
    assert not tuple(path.parent.glob(".weight-*"))


def test_exact_existing_files_are_idempotent_but_mismatches_never_overwrite(
    bundle, tmp_path, monkeypatch
):
    shard, path = copy_bundle(bundle, tmp_path / "artifacts")
    # Default replay was proven above; isolate filesystem behavior here.
    monkeypatch.setattr(
        artifacts,
        "replay_training_weight_candidate_shard",
        lambda shard, **_: shard,
    )
    before = {item.name: item.read_bytes() for item in path.parent.iterdir()}
    assert (
        artifacts.write_training_weight_candidate_shard(shard, path.parent)
        == path
    )
    assert before == {
        item.name: item.read_bytes() for item in path.parent.iterdir()
    }
    component = path.parent / read_manifest(path).components[0].file.path
    component.write_bytes(b"foreign existing bytes")
    with pytest.raises(ValueError):
        artifacts.write_training_weight_candidate_shard(shard, path.parent)
    assert component.read_bytes() == b"foreign existing bytes"
    assert path.read_bytes() == before[path.name]


def test_component_tampering_and_identity_mismatch_fail_before_replay(
    bundle, tmp_path, monkeypatch
):
    _, path = copy_bundle(bundle, tmp_path / "artifacts")
    monkeypatch.setattr(
        artifacts,
        "replay_training_weight_candidate_shard",
        lambda *_, **__: pytest.fail("must not replay tampered components"),
    )
    manifest = read_manifest(path)
    forged = replace(
        manifest,
        model=replace(
            manifest.model,
            component_id="training-weight-model:sha256:" + "f" * 64,
        ),
    )
    with pytest.raises(ValueError, match="identity differs"):
        artifacts.read_training_weight_candidate_shard(
            save_manifest(forged, path.parent)
        )
    component = path.parent / manifest.model.file.path
    data = component.read_bytes()
    component.write_bytes(b"!" + data[1:])
    with pytest.raises(ValueError, match="exact bytes"):
        artifacts.read_training_weight_candidate_shard(path)


def test_resealed_month_inventory_still_requires_full_reexecution(
    bundle, tmp_path
):
    _, path = copy_bundle(bundle, tmp_path / "artifacts")
    manifest = read_manifest(path)
    forged = replace(manifest, source_refusals=manifest.source_refusals[1:])
    with pytest.raises(ValueError, match="refusal inventory"):
        artifacts.read_training_weight_candidate_shard(
            save_manifest(forged, path.parent)
        )


def test_source_or_model_changes_refuse_despite_unchanged_artifact_hashes(
    bundle,
):
    shard, path = bundle
    target = Path(shard.model.source_files[0].path)
    original = target.read_bytes()
    try:
        target.write_bytes(b"changed model source bytes")
        with pytest.raises(ValueError):
            artifacts.read_training_weight_candidate_shard(path)
    finally:
        target.write_bytes(original)


def test_manifest_names_unknown_fields_and_relative_escape_refuse(
    bundle, tmp_path
):
    _, path = copy_bundle(bundle, tmp_path / "artifacts")
    alternate = path.parent / "wrong-name.json"
    alternate.write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match="filename"):
        artifacts.read_training_weight_candidate_shard(alternate)
    manifest = read_manifest(path)
    with pytest.raises(ValueError, match="locator"):
        replace(
            manifest.model,
            file=replace(manifest.model.file, path="../outside.json"),
        )
    raw = manifest.to_dict()
    raw["production_admitted"] = True
    raw.pop("artifact_id")
    raw["artifact_id"] = (
        "training-weight-candidate-shard-manifest:sha256:"
        + hashlib.sha256(training_json(raw).encode()).hexdigest()
    )
    payload = training_json(raw).encode()
    forged = (
        path.parent
        / f"weight-candidate-shard-{hashlib.sha256(payload).hexdigest()}.json"
    )
    forged.write_bytes(payload)
    with pytest.raises(ValueError, match="unknown"):
        artifacts.read_training_weight_candidate_shard(forged)


@pytest.mark.parametrize(
    "limit", ["MAX_SHARD_COMPONENTS", "MAX_SHARD_TOTAL_BYTES"]
)
def test_aggregate_preflight_precedes_component_reads_and_generator(
    bundle, monkeypatch, limit
):
    shard, path = bundle
    monkeypatch.setattr(artifacts, limit, 1)
    original = artifacts.read_training_regular
    reads = []

    def read(target, size):
        reads.append(Path(target))
        assert Path(target) == path
        return original(target, size)

    monkeypatch.setattr(artifacts, "read_training_regular", read)
    monkeypatch.setattr(
        artifacts,
        "replay_training_weight_candidate_shard",
        lambda *_, **__: pytest.fail("budget must precede replay"),
    )
    with pytest.raises(ValueError, match="budget"):
        artifacts.read_training_weight_candidate_shard(path)
    assert reads == [path]
    with pytest.raises(ValueError, match="budget"):
        artifacts.write_training_weight_candidate_shard(shard, path.parent)


def test_excess_day_inventory_refuses_before_replay(
    bundle, tmp_path, monkeypatch
):
    shard, _ = bundle
    monkeypatch.setattr(
        artifacts,
        "replay_training_weight_candidate_shard",
        lambda *_, **__: pytest.fail("preflight must run first"),
    )
    with pytest.raises(ValueError, match="schedule"):
        artifacts.write_training_weight_candidate_shard(
            replace(shard, days=shard.days * 9), tmp_path / "bad"
        )
    assert not (tmp_path / "bad").exists()


def test_interrupted_publication_has_no_root_and_can_be_recovered(
    bundle, tmp_path, monkeypatch
):
    shard, _ = bundle
    destination = tmp_path / "interrupted"
    monkeypatch.setattr(
        artifacts,
        "replay_training_weight_candidate_shard",
        lambda shard, **_: shard,
    )
    original = artifacts.os.link
    count = 0

    def interrupted(*args, **kwargs):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("synthetic interruption")
        return original(*args, **kwargs)

    monkeypatch.setattr(artifacts.os, "link", interrupted)
    with pytest.raises(OSError, match="interruption"):
        artifacts.write_training_weight_candidate_shard(shard, destination)
    assert tuple(destination.glob("weight-*.json"))
    assert not tuple(destination.glob("weight-candidate-shard-*"))
    assert not tuple(destination.glob(".weight-*"))
    monkeypatch.setattr(artifacts.os, "link", original)
    assert artifacts.write_training_weight_candidate_shard(
        shard, destination
    ).is_file()


def test_symlink_root_and_component_are_refused(bundle, tmp_path, monkeypatch):
    shard, path = copy_bundle(bundle, tmp_path / "artifacts")
    link = tmp_path / "alias"
    try:
        link.symlink_to(path.parent, target_is_directory=True)
    except NotImplementedError:
        pytest.skip("synthetic symlink creation unsupported")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("synthetic symlink creation unsupported")
        raise
    with pytest.raises(ValueError, match="symlink"):
        artifacts.read_training_weight_candidate_shard(link / path.name)
    monkeypatch.setattr(
        artifacts,
        "replay_training_weight_candidate_shard",
        lambda shard, **_: shard,
    )
    with pytest.raises(ValueError, match="symlink"):
        artifacts.write_training_weight_candidate_shard(shard, link)
    component = path.parent / read_manifest(path).model.file.path
    component.unlink()
    component.symlink_to(bundle[1].parent / component.name)
    with pytest.raises(ValueError, match="regular"):
        artifacts.read_training_weight_candidate_shard(path)


@pytest.mark.parametrize("code", [errno.EIO, errno.ENOSPC])
def test_symlink_fixture_does_not_hide_unrelated_oserror(
    tmp_path, monkeypatch, code
):
    error = OSError(code, "synthetic filesystem failure")
    path = tmp_path / "unread-fixture-manifest.json"
    monkeypatch.setitem(globals(), "copy_bundle", lambda *args: (None, path))

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(Path, "symlink_to", fail)
    with pytest.raises(OSError) as raised:
        test_symlink_root_and_component_are_refused(None, tmp_path, monkeypatch)
    assert raised.value is error


@pytest.mark.skipif(
    not hasattr(os, "mkfifo"), reason="FIFO canary requires POSIX"
)
def test_fifo_and_oversized_manifest_refuse_boundedly(bundle, tmp_path):
    _, path = copy_bundle(bundle, tmp_path / "artifacts")
    component = path.parent / read_manifest(path).model.file.path
    component.unlink()
    os.mkfifo(component)
    with pytest.raises(ValueError, match="regular"):
        artifacts.read_training_weight_candidate_shard(path)
    oversize = tmp_path / "oversize.json"
    with oversize.open("wb") as stream:
        stream.truncate(artifacts.MAX_TRAINING_BYTES + 1)
    with pytest.raises(ValueError, match="bounded"):
        artifacts.read_training_weight_candidate_shard(oversize)

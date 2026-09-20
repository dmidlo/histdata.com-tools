"""Independent durable-overlap tests using actual synthetic IPC sources only."""

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_quality import training_overlap_artifacts as io
from histdatacom.data_quality.training_overlap_artifacts import (
    read_training_overlap_artifact,
    write_training_overlap_artifact,
)
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapBatchV1,
    TrainingOverlapSelectionV1,
)
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
    replay_training_overlap,
)
from histdatacom.datasets import histdata_cache_path
from tests.fixtures.training_overlap_v1 import overlap_fixture


@pytest.fixture(scope="module")
def batch(tmp_path_factory):
    return materialize_training_overlap(
        overlap_fixture(tmp_path_factory.mktemp("overlap-source"))
    )


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )


def _reseal(value):
    # Independent recursive hashing, not the production constructors/writer.
    # References are deliberately retained; the replay must prove their truth.
    if isinstance(value, list):
        return [_reseal(child) for child in value]
    if not isinstance(value, dict):
        return value
    result = {key: _reseal(child) for key, child in value.items()}
    if "artifact_id" in result and str(result.get("schema_version")).startswith(
        "histdatacom.training-"
    ):
        payload = {
            key: child for key, child in result.items() if key != "artifact_id"
        }
        digest = hashlib.sha256(_canonical(payload).encode()).hexdigest()
        kind = (
            result["schema_version"]
            .removeprefix("histdatacom.")
            .removesuffix(".v1")
        )
        result["artifact_id"] = kind + ":sha256:" + digest
    return result


def _write_forged(tmp_path, wire):
    encoded = _canonical(_reseal(wire)).encode()
    name = (
        "training-overlap-batch-"
        + hashlib.sha256(encoded).hexdigest()
        + ".json"
    )
    path = tmp_path / name
    path.write_bytes(encoded)
    return path


def test_roundtrip_requires_explicit_expost_and_is_idempotent(tmp_path, batch):
    path = write_training_overlap_artifact(batch, tmp_path)
    original = path.read_bytes()
    assert read_training_overlap_artifact(path, allow_expost=True) == batch
    assert write_training_overlap_artifact(batch, tmp_path) == path
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]
    for rejected in (False, 1, None, "true"):
        with pytest.raises(ValueError, match="explicit ex-post"):
            read_training_overlap_artifact(path, allow_expost=rejected)


@pytest.mark.parametrize(
    "mutation",
    (
        "mass",
        "reserved_mass",
        "source_values",
        "group",
        "dependency_span",
        "drop_row",
        "row_cutoff",
        "selection_without_projection",
    ),
)
def test_valid_resealed_structure_is_not_a_source_replay_proof(
    tmp_path, batch, mutation
):
    wire = json.loads(batch.to_json())
    if mutation == "mass":
        allocation = next(
            a for a in wire["allocations"] if a["complete_row_count"]
        )
        allocation["denominator"] = str(int(allocation["denominator"]) * 2)
    elif mutation == "reserved_mass":
        wire["allocations"][0]["reserved_numerator"] = "2"
    elif mutation == "source_values":
        coordinate = wire["coordinates"][0]
        values = json.loads(coordinate["value_json"])
        values["bid"] += 0.00001
        coordinate["value_json"] = _canonical(values)
    elif mutation == "group":
        wire["windows"][0]["group_id"] = "invented-overlap-group"
    elif mutation == "dependency_span":
        wire["maximum_dependency_span_ns"] += 1
    elif mutation == "drop_row":
        wire["rows"].pop()
    elif mutation == "row_cutoff":
        wire["rows"][0]["analytical_cutoff_ns"] += 1
    else:
        wire["selection"]["coordinate_ids"] = []
    path = _write_forged(tmp_path, wire)
    # The new envelope and nested content identities really are self-consistent.
    TrainingOverlapBatchV1.from_json(path.read_text())
    with pytest.raises(ValueError, match="complete source/native/math replay"):
        read_training_overlap_artifact(path, allow_expost=True)


@pytest.mark.parametrize("selection", (None, ()))
def test_deleted_original_source_invalidates_even_empty_retained_view(
    tmp_path, selection
):
    source_root = tmp_path / "source"
    plan = overlap_fixture(source_root)
    batch = materialize_training_overlap(
        plan, TrainingOverlapSelectionV1(coordinate_ids=selection)
    )
    destination = tmp_path / "artifact"
    path = write_training_overlap_artifact(batch, destination)
    assert read_training_overlap_artifact(path, allow_expost=True) == batch
    # Delete only this test's generated IPC; no user/historical source is used.
    histdata_cache_path(
        source_root / "ASCII" / "T", "EURUSD", "202001"
    ).unlink()
    with pytest.raises((ValueError, OSError)):
        read_training_overlap_artifact(path, allow_expost=True)
    with pytest.raises((ValueError, OSError)):
        write_training_overlap_artifact(batch, tmp_path / "unpublished")
    assert not (tmp_path / "unpublished").exists()


def test_forged_frozen_object_fails_before_publication(tmp_path, batch):
    altered = replace(batch)
    object.__setattr__(altered, "maximum_dependency_span_ns", 0)
    with pytest.raises(ValueError, match="complete source/native/math replay"):
        write_training_overlap_artifact(altered, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_empty_view_retains_complete_ledger_and_original_mass(tmp_path, batch):
    empty = materialize_training_overlap(
        batch.plan, TrainingOverlapSelectionV1(coordinate_ids=())
    )
    assert empty.rows == ()
    assert empty.coordinates == batch.coordinates
    assert empty.windows == batch.windows
    assert empty.allocations == batch.allocations
    assert empty.occurrences == batch.occurrences
    assert empty.maximum_dependency_span_ns == batch.maximum_dependency_span_ns
    assert replay_training_overlap(empty) == empty
    path = write_training_overlap_artifact(empty, tmp_path)
    assert read_training_overlap_artifact(path, allow_expost=True) == empty


def _symlink(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except NotImplementedError:
        pytest.skip("symlinks unsupported")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege unavailable")
        raise


def test_symlink_parent_refused_without_modifying_source(tmp_path, batch):
    real = tmp_path / "real"
    path = write_training_overlap_artifact(batch, real)
    original = path.read_bytes()
    alias = tmp_path / "alias"
    _symlink(alias, real, directory=True)
    with pytest.raises(ValueError, match="parent.*real directory"):
        read_training_overlap_artifact(alias / path.name, allow_expost=True)
    with pytest.raises(ValueError, match="parent.*real directory"):
        write_training_overlap_artifact(batch, alias)
    assert path.read_bytes() == original


def test_symlink_leaf_and_conflicting_existing_file_are_preserved(
    tmp_path, batch
):
    path = write_training_overlap_artifact(batch, tmp_path)
    original = path.read_bytes()
    path.write_bytes(b"conflicting existing evidence")
    with pytest.raises(ValueError, match="conflicts"):
        write_training_overlap_artifact(batch, tmp_path)
    assert path.read_bytes() == b"conflicting existing evidence"
    sentinel = tmp_path / "sentinel.json"
    sentinel.write_bytes(original)
    path.unlink()
    _symlink(path, sentinel)
    with pytest.raises(ValueError, match="regular"):
        read_training_overlap_artifact(path, allow_expost=True)
    with pytest.raises(ValueError, match="regular"):
        write_training_overlap_artifact(batch, tmp_path)
    assert path.is_symlink()
    assert sentinel.read_bytes() == original
    assert list(tmp_path.glob(".overlap-*")) == []


@pytest.mark.parametrize("mutation", ("append", "replace"))
def test_file_changes_during_read_are_rejected(
    tmp_path, batch, monkeypatch, mutation
):
    path = write_training_overlap_artifact(batch, tmp_path)
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
    with pytest.raises(ValueError, match="changed during bounded read"):
        read_training_overlap_artifact(path, allow_expost=True)


@pytest.mark.skipif(
    not hasattr(os, "mkfifo") or not hasattr(os, "O_NONBLOCK"),
    reason="requires POSIX FIFO and nonblocking open",
)
def test_raced_fifo_never_blocks_reader(tmp_path, batch, monkeypatch):
    path = write_training_overlap_artifact(batch, tmp_path)
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
    with pytest.raises(ValueError, match="changed before open"):
        read_training_overlap_artifact(path, allow_expost=True)


@pytest.mark.parametrize("phase", ("file_fsync", "link", "directory_fsync"))
def test_publication_interruption_leaves_no_partial_artifact(
    tmp_path, batch, monkeypatch, phase
):
    def fail(*args, **kwargs):
        raise OSError("synthetic interruption")

    if phase == "file_fsync":
        monkeypatch.setattr(io.os, "fsync", fail)
    elif phase == "link":
        monkeypatch.setattr(io.os, "link", fail)
    else:
        monkeypatch.setattr(io, "_fsync_directory", fail)
    with pytest.raises(OSError, match="synthetic interruption"):
        write_training_overlap_artifact(batch, tmp_path)
    paths = list(tmp_path.iterdir())
    if phase == "directory_fsync":
        assert len(paths) == 1
        assert paths[0].read_bytes() == batch.to_json().encode()
    else:
        assert paths == []


def test_oversized_file_fails_before_replay(tmp_path, monkeypatch):
    path = tmp_path / "untrusted.json"
    with path.open("wb") as handle:
        handle.truncate(8 * 1024 * 1024 + 1)

    def unexpected_replay(*args, **kwargs):
        pytest.fail("oversized input reached source replay")

    monkeypatch.setattr(io, "replay_training_overlap", unexpected_replay)
    with pytest.raises(ValueError, match="bounded regular"):
        read_training_overlap_artifact(path, allow_expost=True)

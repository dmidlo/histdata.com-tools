"""Genuine generated IPC -> wide view -> TRAIN fit -> guarded publication."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_quality import (
    training_preprocessing_artifacts as artifacts,
)
from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    MAX_TRAINING_BYTES,
)
from histdatacom.data_quality.training_preprocessing_contracts import (
    PreprocessingFitMode,
    TrainingPreprocessingPlanV1,
    TrainingPreprocessingStepV1,
)
from histdatacom.data_quality.training_preprocessing_views import (
    fit_training_preprocessing,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
)
from tests.fixtures.training_join_v1 import join_fixture
from tests.fixtures.training_substrate_v1 import BASE

_POSIX_IO_AVAILABLE = os.name == "posix" and all(
    hasattr(os, name) for name in ("O_NOFOLLOW", "O_NONBLOCK", "O_DIRECTORY")
)
_POSIX_IO_REASON = "native fit publication requires POSIX no-follow I/O"
_requires_posix_io = pytest.mark.skipif(
    not _POSIX_IO_AVAILABLE, reason=_POSIX_IO_REASON
)


def _fit(root):
    native = join_fixture(root)
    wide = build_training_wide_plan(
        (native,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    ownership = native.spine.ownership
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                unit.artifact_id,
                (
                    TemporalPartition.TRAIN
                    if unit.start_ns <= BASE
                    else (
                        TemporalPartition.VALIDATION
                        if unit.start_ns == BASE + DAY_NS
                        else TemporalPartition.TEST
                    )
                ),
            )
            for unit in ownership.units
        ),
    )
    columns = (native.columns[0].name,)
    plan = TrainingPreprocessingPlanV1(
        wide,
        split,
        "artifact-synthetic-fold-1",
        PreprocessingFitMode.TRAIN_FIT,
        tuple(
            sorted(
                a.evidence_unit_id
                for a in split.assignments
                if a.partition is TemporalPartition.TRAIN
            )
        ),
        columns,
        (TrainingPreprocessingStepV1("scale", "zscale", columns),),
    )
    return fit_training_preprocessing(plan)


@pytest.fixture(scope="module")
def native_fit(tmp_path_factory):
    if not _POSIX_IO_AVAILABLE:
        pytest.skip(_POSIX_IO_REASON)
    return _fit(tmp_path_factory.mktemp("preprocessing-artifact-native"))


def _read(path, fit):
    return artifacts.read_training_preprocessing_fit(
        path, expected_fit_id=fit.artifact_id
    )


def test_actual_native_fit_roundtrip_and_idempotent_no_clobber(
    tmp_path, native_fit
):
    path = artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    data = native_fit.to_json().encode("ascii")
    assert (
        path.name
        == f"training-preprocessing-fit-{hashlib.sha256(data).hexdigest()}.json"
    )
    assert path.read_bytes() == data
    assert _read(path, native_fit) == native_fit
    membership = json.loads(native_fit.membership_json)
    assert membership["fit_unit_ids"] == list(native_fit.plan.fit_unit_ids)
    # The actual adapter declares all January, not just the three populated
    # synthetic days. Empty owned days must remain assigned too.
    assert len(native_fit.plan.split.assignments) == 31
    assert all(
        row["unit_id"] in native_fit.plan.fit_unit_ids
        for row in membership["rows"]
    )
    before = path.stat()
    assert (
        artifacts.write_training_preprocessing_fit(native_fit, tmp_path) == path
    )
    assert path.stat().st_ino == before.st_ino
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert tuple(tmp_path.iterdir()) == (path,)


def test_detached_actual_replay_result_not_mutable_caller_is_published(
    tmp_path, native_fit, monkeypatch
):
    caller = type(native_fit).from_json(native_fit.to_json())
    expected = caller.to_json().encode("ascii")
    real_replay = artifacts.replay_training_preprocessing_fit

    def mutate_after_replay(fit, *, expected_fit_id):
        verified = real_replay(fit, expected_fit_id=expected_fit_id)
        object.__setattr__(fit, "_cached_json", "{}")
        return verified

    monkeypatch.setattr(
        artifacts, "replay_training_preprocessing_fit", mutate_after_replay
    )
    path = artifacts.write_training_preprocessing_fit(caller, tmp_path)
    assert path.read_bytes() == expected


def test_resealed_parameter_fit_is_not_authority(tmp_path, native_fit):
    step = native_fit.steps[0]
    payload = json.loads(step.kernel_fit_json)
    stats = payload["parameters"]["columns"][0]
    assert stats["mean"] != stats["minimum"]
    stats["mean"] = stats["minimum"]
    altered = replace(
        step,
        kernel_fit_json=json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ),
    )
    forged = replace(native_fit, steps=(altered,))
    data = forged.to_json().encode("ascii")
    path = tmp_path / artifacts._filename(data)
    path.write_bytes(data)
    with pytest.raises(ValueError, match="native refit"):
        _read(path, forged)
    with pytest.raises(ValueError, match="native refit"):
        artifacts.write_training_preprocessing_fit(forged, tmp_path / "new")
    assert not (tmp_path / "new").exists()
    assert path.read_bytes() == data


def test_resealed_membership_omission_is_not_authority(tmp_path, native_fit):
    membership = json.loads(native_fit.membership_json)
    membership["rows"] = membership["rows"][:-1]
    forged = replace(
        native_fit,
        membership_json=json.dumps(
            membership, sort_keys=True, separators=(",", ":")
        ),
    )
    data = forged.to_json().encode("ascii")
    path = tmp_path / artifacts._filename(data)
    path.write_bytes(data)
    with pytest.raises(ValueError, match="native refit"):
        _read(path, forged)


@_requires_posix_io
def test_current_native_source_bytes_are_required_on_every_read(tmp_path):
    from histdatacom.datasets import DatasetCatalog

    actual = _fit(tmp_path / "source")
    path = artifacts.write_training_preprocessing_fit(
        actual, tmp_path / "output"
    )
    source = actual.plan.wide_plan.tiles[0].join_plan.spine.source
    partition = (
        DatasetCatalog.from_json(source.catalog_json).versions[0].partitions[0]
    )
    Path(partition.artifact.path).write_bytes(
        b"owned synthetic input corruption"
    )
    with pytest.raises(ValueError):
        _read(path, actual)
    assert path.read_bytes() == actual.to_json().encode("ascii")


@pytest.mark.parametrize(
    "expected",
    ["", "wrong", "training-preprocessing-fit:sha256:" + "0" * 64, True],
)
def test_expected_identity_is_mandatory_and_exact(
    tmp_path, native_fit, expected
):
    path = artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    with pytest.raises(ValueError, match="expected"):
        artifacts.read_training_preprocessing_fit(
            path, expected_fit_id=expected
        )


@pytest.mark.parametrize("kind", ["file", "directory", "ancestor"])
def test_no_follow_file_directory_and_ancestor_symlinks(
    tmp_path, native_fit, kind
):
    real = artifacts.write_training_preprocessing_fit(
        native_fit, tmp_path / "real"
    )
    link = tmp_path / "link"
    if kind == "file":
        link.symlink_to(real)
        with pytest.raises(ValueError, match="regular file"):
            _read(link, native_fit)
    else:
        link.symlink_to(real.parent, target_is_directory=True)
        with pytest.raises(ValueError, match="real directory"):
            artifacts.write_training_preprocessing_fit(
                native_fit, link if kind == "directory" else link / "child"
            )
        with pytest.raises(ValueError, match="real directory"):
            _read(link / real.name, native_fit)
    assert _read(real, native_fit) == native_fit


@pytest.mark.parametrize("same", [False, True])
def test_atomic_competing_publication_never_overwrites(
    tmp_path, native_fit, monkeypatch, same
):
    data = (
        native_fit.to_json().encode("ascii")
        if same
        else b"other writer's retained bytes"
    )

    def compete(source, target, **kwargs):
        Path(target).write_bytes(data)
        raise FileExistsError("owned concurrent publication control")

    monkeypatch.setattr(artifacts.os, "link", compete)
    if same:
        path = artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
        assert _read(path, native_fit) == native_fit
    else:
        with pytest.raises(ValueError, match="concurrent"):
            artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    assert not tuple(tmp_path.glob(".preprocessing-*"))
    (path,) = tuple(tmp_path.glob("training-preprocessing-fit-*"))
    assert path.read_bytes() == data


def test_existing_different_bytes_never_overwritten(tmp_path, native_fit):
    data = native_fit.to_json().encode("ascii")
    target = tmp_path / artifacts._filename(data)
    target.write_bytes(b"preserved different bytes")
    with pytest.raises(ValueError, match="existing"):
        artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    assert target.read_bytes() == b"preserved different bytes"


@pytest.mark.parametrize("dangling", [False, True])
def test_writer_existing_content_addressed_symlink_never_followed(
    tmp_path, native_fit, dangling
):
    data = native_fit.to_json().encode("ascii")
    occupant = tmp_path / "preserved-target.json"
    if not dangling:
        occupant.write_bytes(data)
    target = tmp_path / artifacts._filename(data)
    target.symlink_to(occupant)
    with pytest.raises(ValueError, match="bounded regular file"):
        artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    assert target.is_symlink()
    assert target.readlink() == occupant
    if dangling:
        assert not occupant.exists()
    else:
        assert occupant.read_bytes() == data
    assert not tuple(tmp_path.glob(".preprocessing-*"))


def test_writer_cached_identity_does_not_hide_mutated_raw_fit_plan(
    tmp_path, native_fit
):
    caller = type(native_fit).from_json(native_fit.to_json())
    original_id = caller.artifact_id
    original_wire = caller.to_json()
    object.__setattr__(caller.plan, "fold_id", "mutated-raw-fold")
    # Frozen objects can still be attacked through object.__setattr__. Cached
    # wire/identity remain unchanged, but the boundary must read actual fields.
    assert caller.artifact_id == original_id
    assert caller.to_json() == original_wire
    destination = tmp_path / "uncreated-output"
    with pytest.raises(ValueError, match="expected subject"):
        artifacts.write_training_preprocessing_fit(caller, destination)
    assert not destination.exists()


def test_prelink_interruption_retains_no_false_committed_fit(
    tmp_path, native_fit, monkeypatch
):
    def interrupt(*args, **kwargs):
        raise InterruptedError("owned prelink interruption")

    monkeypatch.setattr(artifacts.os, "link", interrupt)
    with pytest.raises(InterruptedError):
        artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    assert not tuple(tmp_path.iterdir())


def test_managed_namespace_refuses_before_mkdir_or_replay(
    tmp_path, native_fit, monkeypatch
):
    (tmp_path / ".histdatacom-retention.json").write_text("{}")

    def poison(*args, **kwargs):
        pytest.fail("native execution preceded unmanaged target admission")

    monkeypatch.setattr(artifacts, "replay_training_preprocessing_fit", poison)
    with pytest.raises(ValueError, match="managed"):
        artifacts.write_training_preprocessing_fit(
            native_fit, tmp_path / "child"
        )
    assert not (tmp_path / "child").exists()


@pytest.mark.parametrize("kind", ["directory", "fifo", "oversized", "empty"])
@_requires_posix_io
def test_guarded_reader_refuses_nonregular_and_oversized_without_decode(
    tmp_path, kind
):
    path = tmp_path / "artifact.json"
    if kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "oversized":
        with path.open("wb") as stream:
            stream.truncate(MAX_TRAINING_BYTES + 1)
    else:
        path.touch()
    with pytest.raises(ValueError, match="bounded regular"):
        artifacts._read(path)


def test_reader_refuses_wrong_content_filename_and_noncanonical_wire(
    tmp_path, native_fit
):
    wrong = tmp_path / "renamed.json"
    wrong.write_text(native_fit.to_json())
    with pytest.raises(ValueError, match="filename"):
        _read(wrong, native_fit)
    data = (native_fit.to_json() + "\n").encode("ascii")
    noncanonical = tmp_path / artifacts._filename(data)
    noncanonical.write_bytes(data)
    with pytest.raises(ValueError, match="canonical"):
        _read(noncanonical, native_fit)


def test_same_bytes_replacement_during_real_native_replay_refuses(
    tmp_path, native_fit, monkeypatch
):
    path = artifacts.write_training_preprocessing_fit(native_fit, tmp_path)
    native_replay = artifacts.replay_training_preprocessing_fit

    def replace_after_replay(fit, *, expected_fit_id):
        verified = native_replay(fit, expected_fit_id=expected_fit_id)
        alternate = path.with_suffix(".replacement")
        alternate.write_bytes(path.read_bytes())
        alternate.replace(path)
        return verified

    monkeypatch.setattr(
        artifacts, "replay_training_preprocessing_fit", replace_after_replay
    )
    with pytest.raises(ValueError, match="changed during native replay"):
        _read(path, native_fit)


@_requires_posix_io
def test_wrong_subject_refuses_without_execution(tmp_path):
    with pytest.raises(TypeError, match="exact fit"):
        artifacts.write_training_preprocessing_fit(object(), tmp_path)


@pytest.mark.parametrize(
    "capability", ("O_NOFOLLOW", "O_NONBLOCK", "O_DIRECTORY")
)
def test_unsupported_capability_refuses_without_execution(
    tmp_path, monkeypatch, capability
):
    # These negative cases intentionally do not use the POSIX native fixture:
    # Windows and any host lacking a required flag must execute this refusal.
    def poison(*args, **kwargs):
        pytest.fail("native replay preceded required capability admission")

    monkeypatch.setattr(artifacts, "replay_training_preprocessing_fit", poison)
    monkeypatch.delattr(artifacts.os, capability, raising=False)
    destination = tmp_path / "uncreated-output"
    with pytest.raises(ValueError, match="POSIX"):
        artifacts.write_training_preprocessing_fit(object(), destination)
    with pytest.raises(ValueError, match="POSIX"):
        artifacts.read_training_preprocessing_fit(
            tmp_path / "absent.json",
            expected_fit_id="training-preprocessing-fit:sha256:" + "0" * 64,
        )
    assert not destination.exists()

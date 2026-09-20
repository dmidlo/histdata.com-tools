"""Synthetic-only durable joins: replay, no-clobber writes and read bounds."""

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_quality import training_join_artifacts as artifacts
from histdatacom.data_quality import training_join_contracts as contracts
from histdatacom.data_quality import training_lineage
from histdatacom.data_quality.training_join_contracts import (
    JoinInformationMode,
    JoinState,
    TrainingJoinBatchV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
)
from histdatacom.datasets import DatasetCatalog
from tests.fixtures.training_join_v1 import join_fixture


@pytest.fixture
def batch(tmp_path):
    return materialize_training_joins(join_fixture(tmp_path / "source"))


def _filename(data: bytes) -> str:
    return "training-join-batch-" + hashlib.sha256(data).hexdigest() + ".json"


def _read(path):
    return artifacts.read_training_join_artifact(
        path, information_mode=JoinInformationMode.EX_POST
    )


def test_canonical_write_read_is_idempotent_and_replayed(tmp_path, batch):
    directory = tmp_path / "published"
    path = artifacts.write_training_join_artifact(batch, directory)
    assert path.name == _filename(batch.to_json().encode())
    assert path.read_bytes() == batch.to_json().encode()
    assert _read(path) == batch
    assert artifacts.write_training_join_artifact(batch, directory) == path
    assert tuple(directory.iterdir()) == (path,)


def test_reader_requires_exact_information_mode(tmp_path, batch):
    path = artifacts.write_training_join_artifact(batch, tmp_path / "out")
    with pytest.raises(TypeError, match="explicit information mode"):
        artifacts.read_training_join_artifact(path, information_mode="ex_post")
    with pytest.raises(ValueError, match="different mode"):
        artifacts.read_training_join_artifact(
            path, information_mode=JoinInformationMode.NORMALIZED_AS_OF
        )


@pytest.mark.parametrize(
    "field", ("value", "null", "clock", "origin", "parent")
)
def test_resealed_tampering_refuses_before_write_and_during_read(
    tmp_path, batch, field
):
    row = batch.rows[0]
    cell = row.values[0]
    changes = {
        "value": {"value_json": '{"value":42.0}'},
        "null": {
            "value_json": '{"value":null}',
            "state": JoinState.UNAVAILABLE,
        },
        "clock": {"source_time_ns": cell.source_time_ns - 1},
        "origin": {"origin": "synthetic_but_falsely_relabelled"},
        "parent": {"parent_source_ids": ("invented-parent",)},
    }
    forged = replace(
        batch,
        rows=(replace(row, values=(replace(cell, **changes[field]),)),)
        + batch.rows[1:],
    )
    assert forged.artifact_id != batch.artifact_id
    assert TrainingJoinBatchV1.from_json(forged.to_json()) == forged
    destination = tmp_path / "not-published"
    with pytest.raises(ValueError, match="differ from source replay"):
        artifacts.write_training_join_artifact(forged, destination)
    assert not destination.exists()
    data = forged.to_json().encode()
    path = tmp_path / _filename(data)
    path.write_bytes(data)
    with pytest.raises(ValueError, match="differ from source replay"):
        _read(path)
    assert path.read_bytes() == data  # Retained bad evidence is not rewritten.


def test_filename_and_noncanonical_bytes_refuse(tmp_path, batch):
    canonical = batch.to_json().encode()
    wrong_name = tmp_path / "wrong-name.json"
    wrong_name.write_bytes(canonical)
    with pytest.raises(ValueError, match="filename"):
        _read(wrong_name)
    spaced = canonical + b"\n"
    content_named = tmp_path / _filename(spaced)
    content_named.write_bytes(spaced)
    with pytest.raises(ValueError, match="noncanonical"):
        _read(content_named)


def test_symlink_read_and_write_refuse_without_overwriting_target(
    tmp_path, batch
):
    real = artifacts.write_training_join_artifact(batch, tmp_path / "real")
    directory = tmp_path / "alias"
    directory.mkdir()
    link = directory / real.name
    link.symlink_to(real)
    with pytest.raises(ValueError, match="regular file"):
        _read(link)
    with pytest.raises(ValueError, match="regular file"):
        artifacts.write_training_join_artifact(batch, directory)
    assert real.read_bytes() == batch.to_json().encode()
    assert link.is_symlink()
    assert tuple(directory.iterdir()) == (link,)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFO canary")
def test_fifo_and_raced_fifo_refuse_without_blocking(
    tmp_path, batch, monkeypatch
):
    fifo = tmp_path / "direct-fifo.json"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular file"):
        _read(fifo)
    path = artifacts.write_training_join_artifact(batch, tmp_path / "race")
    original_open = os.open

    def raced_open(candidate, flags, *args, **kwargs):
        if Path(candidate) == path:
            assert flags & os.O_NONBLOCK, "raced FIFO open must not block"
            assert flags & os.O_NOFOLLOW
            path.unlink()
            os.mkfifo(path)
        return original_open(candidate, flags, *args, **kwargs)

    monkeypatch.setattr(training_lineage.os, "open", raced_open)
    with pytest.raises(ValueError, match="changed during open"):
        _read(path)


@pytest.mark.parametrize("same_content", (True, False))
def test_atomic_competing_writer_never_clobbers(
    tmp_path, batch, monkeypatch, same_content
):
    directory = tmp_path / "race"
    competitor = (
        batch.to_json().encode() if same_content else b"retained competitor"
    )

    def competing_link(source, destination, **kwargs):
        Path(destination).write_bytes(competitor)
        raise FileExistsError("synthetic competing publisher")

    monkeypatch.setattr(artifacts.os, "link", competing_link)
    if same_content:
        result = artifacts.write_training_join_artifact(batch, directory)
        assert _read(result) == batch
    else:
        with pytest.raises(ValueError, match="conflicts"):
            artifacts.write_training_join_artifact(batch, directory)
    files = tuple(directory.iterdir())
    assert len(files) == 1
    assert files[0].read_bytes() == competitor
    assert not files[0].name.startswith(".join-")


@pytest.mark.parametrize("operation", ("link", "fsync"))
def test_prepublication_interruption_leaves_no_artifact_or_temporary(
    tmp_path, batch, monkeypatch, operation
):
    def interrupted(*args, **kwargs):
        raise OSError("synthetic prepublication interruption")

    directory = tmp_path / "interrupted"
    monkeypatch.setattr(artifacts.os, operation, interrupted)
    with pytest.raises(OSError, match="synthetic prepublication"):
        artifacts.write_training_join_artifact(batch, directory)
    assert not tuple(directory.iterdir())


@pytest.mark.parametrize("projection_empty", (True, False))
def test_empty_rows_replay_actual_source_corruption_before_publish(
    tmp_path, projection_empty
):
    plan = join_fixture(tmp_path / "source", empty=True)
    if projection_empty:
        plan = replace(plan, columns=())
    empty = materialize_training_joins(plan)
    assert not empty.rows
    path = artifacts.write_training_join_artifact(empty, tmp_path / "retained")
    catalog = DatasetCatalog.from_json(plan.spine.source.catalog_json)
    source = Path(catalog.versions[0].partitions[0].artifact.path)
    source.write_bytes(source.read_bytes() + b"synthetic corruption")
    with pytest.raises(ValueError):
        _read(path)
    unpublished = tmp_path / "unpublished"
    with pytest.raises(ValueError):
        artifacts.write_training_join_artifact(empty, unpublished)
    assert not unpublished.exists()


@pytest.mark.parametrize(
    ("name", "limit"),
    (("MAX_JOIN_ROWS", 2), ("MAX_JOIN_COLUMNS", 0), ("MAX_JOIN_CELLS", 2)),
)
def test_projected_resource_limits_refuse_before_source_execution(
    tmp_path, monkeypatch, name, limit
):
    plan = join_fixture(tmp_path / "source")
    monkeypatch.setattr(contracts, name, limit)
    with pytest.raises(ValueError, match="budget exceeded"):
        replace(plan)


def test_embedded_source_and_nonregular_or_oversized_input_bounds(tmp_path):
    plan = join_fixture(tmp_path / "source")
    with pytest.raises(ValueError, match="cardinality"):
        replace(plan.sources[0], paths=tuple(f"path-{i}" for i in range(33)))
    with pytest.raises(ValueError, match="2 MiB"):
        replace(
            plan.sources[0],
            evidence_json=('"' + "x" * (2 * 1024 * 1024) + '"',),
        )
    with pytest.raises(ValueError, match="regular file"):
        _read(tmp_path)
    oversized = tmp_path / "oversized.json"
    with oversized.open("wb") as stream:
        stream.truncate(8 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="regular file"):
        _read(oversized)

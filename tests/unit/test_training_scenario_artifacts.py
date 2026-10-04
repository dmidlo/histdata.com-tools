"""Actual native observed-view publication and scoped filesystem faults."""

from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_quality import training_scenario_artifacts as artifacts
from histdatacom.data_quality.training_contracts import (
    MAX_TRAINING_BYTES,
    TrainingConsumerMode,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_scenario_contracts import (
    ScenarioViewKind,
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioRequestV1,
)
from histdatacom.data_quality.training_scenario_views import (
    materialize_training_scenario_view,
)
from tests.fixtures.training_substrate_v1 import (
    BASE,
    SYMBOLS,
    TIMES,
    observed_source,
)


@pytest.fixture(scope="module")
def observed_view(tmp_path_factory):
    source, _ = observed_source(tmp_path_factory.mktemp("scenario-observed"))
    plan = TrainingScenarioPlanV1(
        source, build_training_ownership(source), TrainingScenarioPolicyV1()
    )
    request = TrainingScenarioRequestV1(
        ScenarioViewKind.OBSERVED_ONLY,
        TrainingConsumerMode.DESCRIPTIVE,
        BASE,
        max(TIMES) + 1,
        SYMBOLS,
    )
    return materialize_training_scenario_view(plan, request)


def read(path, view, **kwargs):
    return artifacts.read_training_scenario_artifact(
        path,
        consumer_mode=kwargs.pop("consumer_mode", view.request.consumer_mode),
        expected_view_id=kwargs.pop("expected_view_id", view.artifact_id),
        **kwargs,
    )


def test_actual_observed_view_roundtrip_and_idempotent_publication(
    tmp_path, observed_view
):
    path = artifacts.write_training_scenario_artifact(observed_view, tmp_path)
    data = observed_view.to_json().encode("ascii")
    assert path.name == (
        f"training-scenario-view-{hashlib.sha256(data).hexdigest()}.json"
    )
    assert path.read_bytes() == data
    assert len(observed_view.rows) == len(TIMES) * len(SYMBOLS)
    assert read(path, observed_view) == observed_view
    before = path.stat()
    assert (
        artifacts.write_training_scenario_artifact(observed_view, tmp_path)
        == path
    )
    assert path.stat().st_ino == before.st_ino
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert tuple(tmp_path.iterdir()) == (path,)


def test_reader_requires_explicit_mode_and_exact_subject(
    tmp_path, observed_view
):
    path = artifacts.write_training_scenario_artifact(observed_view, tmp_path)
    with pytest.raises(TypeError, match="consumer mode"):
        read(path, observed_view, consumer_mode="descriptive_ex_post_analysis")
    with pytest.raises(ValueError, match="consumer mode"):
        read(
            path,
            observed_view,
            consumer_mode=TrainingConsumerMode.RECONSTRUCTION,
        )
    with pytest.raises(ValueError, match="view ID"):
        read(path, observed_view, expected_view_id="not-the-requested-view")


def test_publication_uses_detached_native_replay_not_mutable_caller(
    tmp_path, observed_view, monkeypatch
):
    caller = type(observed_view).from_json(observed_view.to_json())
    expected = caller.to_json().encode("ascii")
    native_replay = artifacts.replay_training_scenario_view

    def mutate_after_actual_replay(view, *, expected_view_id):
        verified = native_replay(view, expected_view_id=expected_view_id)
        object.__setattr__(view, "_cached_json", "{}")
        return verified

    # Fault injection changes only caller memory after actual native replay.
    monkeypatch.setattr(
        artifacts, "replay_training_scenario_view", mutate_after_actual_replay
    )
    path = artifacts.write_training_scenario_artifact(caller, tmp_path)
    assert path.read_bytes() == expected


def test_source_tampering_refuses_a_byte_valid_retained_view(tmp_path):
    from histdatacom.datasets import DatasetCatalog

    source, _ = observed_source(tmp_path / "source")
    plan = TrainingScenarioPlanV1(
        source, build_training_ownership(source), TrainingScenarioPolicyV1()
    )
    view = materialize_training_scenario_view(
        plan,
        TrainingScenarioRequestV1(
            ScenarioViewKind.OBSERVED_ONLY,
            TrainingConsumerMode.DESCRIPTIVE,
            BASE,
            max(TIMES) + 1,
            SYMBOLS,
        ),
    )
    path = artifacts.write_training_scenario_artifact(view, tmp_path / "out")
    original = path.read_bytes()
    partition = (
        DatasetCatalog.from_json(source.catalog_json).versions[0].partitions[0]
    )
    Path(partition.artifact.path).write_bytes(b"corrupt owned synthetic input")
    with pytest.raises(ValueError):
        read(path, view)
    assert path.read_bytes() == original


def test_resealed_omitted_rows_do_not_become_native_authority(
    tmp_path, observed_view
):
    forged = replace(observed_view, rows=observed_view.rows[:-1])
    data = forged.to_json().encode("ascii")
    path = tmp_path / artifacts._filename(data)
    path.write_bytes(data)
    with pytest.raises(ValueError, match="differs|different"):
        read(path, forged)
    assert path.read_bytes() == data


@pytest.mark.parametrize("kind", ("file", "directory"))
def test_symlink_publication_or_read_is_not_followed(
    tmp_path, observed_view, kind
):
    path = artifacts.write_training_scenario_artifact(
        observed_view, tmp_path / "real"
    )
    if kind == "file":
        link = tmp_path / path.name
        link.symlink_to(path)
        with pytest.raises(ValueError, match="regular file"):
            read(link, observed_view)
    else:
        link = tmp_path / "linked-directory"
        link.symlink_to(path.parent, target_is_directory=True)
        with pytest.raises(ValueError, match="directory"):
            artifacts.write_training_scenario_artifact(observed_view, link)
    assert read(path, observed_view) == observed_view


@pytest.mark.parametrize("same", (False, True))
def test_competing_publication_is_never_overwritten(
    tmp_path, observed_view, monkeypatch, same
):
    retained = (
        observed_view.to_json().encode("ascii")
        if same
        else b"retain competing writer evidence"
    )

    def compete(source, destination, **kwargs):
        Path(destination).write_bytes(retained)
        raise FileExistsError("owned deterministic publication race")

    monkeypatch.setattr(artifacts.os, "link", compete)
    if same:
        path = artifacts.write_training_scenario_artifact(
            observed_view, tmp_path
        )
        assert read(path, observed_view) == observed_view
    else:
        with pytest.raises(ValueError, match="concurrent"):
            artifacts.write_training_scenario_artifact(observed_view, tmp_path)
    assert not tuple(tmp_path.glob(".scenario-*"))
    (path,) = tuple(tmp_path.glob("training-scenario-view-*.json"))
    assert path.read_bytes() == retained


def test_interruption_before_link_has_no_committed_view(
    tmp_path, observed_view, monkeypatch
):
    def interrupt(*args, **kwargs):
        raise InterruptedError("owned synthetic pre-publication interruption")

    monkeypatch.setattr(artifacts.os, "link", interrupt)
    with pytest.raises(InterruptedError):
        artifacts.write_training_scenario_artifact(observed_view, tmp_path)
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize("kind", ("directory", "oversized", "fifo"))
def test_guarded_read_refuses_special_and_oversize_files(tmp_path, kind):
    path = tmp_path / "untrusted.json"
    if kind == "directory":
        path.mkdir()
    elif kind == "oversized":
        with path.open("wb") as stream:
            stream.truncate(MAX_TRAINING_BYTES + 1)
    else:
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO vector needs POSIX")
        os.mkfifo(path)
    with pytest.raises(ValueError, match="bounded regular"):
        artifacts._read(path)

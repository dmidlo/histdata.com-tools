"""Bounded strict envelopes, atomic publication, and fresh source replay."""

import hashlib
import json
import os
from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_temporal_artifacts import (
    read_training_temporal_artifact,
    write_training_temporal_artifact,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalInformationMode,
    TrainingTemporalBatchV1,
    TrainingTemporalLabelV1,
)
from histdatacom.data_quality.training_temporal_views import (
    materialize_training_temporal,
    replay_training_temporal,
    training_temporal_rows,
)
from tests.fixtures.training_temporal_v1 import temporal_plan, temporal_source


@pytest.fixture
def batch(tmp_path):
    source, ownership, _ = temporal_source(tmp_path / "source")
    return materialize_training_temporal(temporal_plan(source, ownership))


def test_persist_read_canonical_idempotent_and_mode(batch, tmp_path):
    path = write_training_temporal_artifact(batch, tmp_path / "out")
    assert write_training_temporal_artifact(batch, path.parent) == path
    assert (
        read_training_temporal_artifact(
            path, information_mode=batch.plan.information_mode
        )
        == batch
    )
    with pytest.raises(ValueError, match="mode differs"):
        read_training_temporal_artifact(
            path, information_mode=TemporalInformationMode.NORMALIZED_AS_OF
        )


@pytest.mark.parametrize(
    "mutation", ["label", "feature", "clock", "exclusion", "span"]
)
def test_correctly_resealed_false_content_fails_reexecution(
    batch, tmp_path, mutation
):
    row = batch.rows[0]
    if mutation == "label":
        row = replace(row, outcome=replace(row.outcome, value=42.0))
    elif mutation == "feature":
        row = replace(row, features=(replace(row.features[0], value=42.0),))
    elif mutation == "clock":
        row = replace(row, outcome=replace(row.outcome, available_at_ns=0))
    elif mutation == "exclusion":
        row = replace(
            row, status="excluded", reasons=("embargo_dependency_span",)
        )
    forged = replace(
        batch,
        rows=(row,),
        maximum_dependency_span_ns=batch.maximum_dependency_span_ns
        + (mutation == "span"),
    )
    assert forged.artifact_id != batch.artifact_id
    # Strict decoding succeeds: this is a valid newly sealed envelope, not a bad hash.
    assert TrainingTemporalBatchV1.from_json(forged.to_json()) == forged
    with pytest.raises(ValueError, match="differ from replay"):
        replay_training_temporal(forged)
    payload = forged.to_json().encode()
    path = (
        tmp_path
        / f"training-temporal-batch-{hashlib.sha256(payload).hexdigest()}.json"
    )
    path.write_bytes(payload)
    with pytest.raises(ValueError, match="differ from replay"):
        read_training_temporal_artifact(
            path, information_mode=batch.plan.information_mode
        )


def test_empty_selection_cannot_hide_actual_source_mutation(batch):
    from histdatacom.datasets import DatasetCatalog

    catalog = DatasetCatalog.from_dict(
        json.loads(batch.plan.source.catalog_json)
    )
    version = next(
        v
        for v in catalog.versions
        if v.dataset_version_id == batch.plan.source.dataset_version_id
    )
    from pathlib import Path

    target = Path(version.partitions[0].artifact.path)
    original = target.read_bytes()
    target.write_bytes(original + b"corruption")
    with pytest.raises((ValueError, OSError)):
        training_temporal_rows(
            batch, information_mode=batch.plan.information_mode, example_keys=()
        )


def test_unknown_duplicate_oversized_and_scalar_forms_refuse(batch):
    payload = batch.to_dict()
    payload["unknown"] = True
    with pytest.raises(ValueError):
        TrainingTemporalBatchV1.from_dict(payload)
    with pytest.raises(ValueError, match="invalid training JSON"):
        TrainingTemporalBatchV1.from_json(
            '{"schema_version":"x","schema_version":"x"}'
        )
    with pytest.raises(ValueError):
        TrainingTemporalBatchV1.from_json("[" * 10000 + "]" * 10000)
    with pytest.raises(ValueError):
        replace(batch.plan.examples[0].label, horizon_ns=True)
    with pytest.raises(ValueError):
        replace(batch.plan.examples[0].label, deadband=float("inf"))
    with pytest.raises(ValueError):
        replace(batch.plan, examples=batch.plan.examples * 257)
    with pytest.raises(ValueError):
        TrainingTemporalLabelV1.from_json(training_json({"arbitrary": 1}))


def test_filename_and_noncanonical_text_refuse(batch, tmp_path):
    path = write_training_temporal_artifact(batch, tmp_path)
    renamed = tmp_path / "wrong.json"
    renamed.write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match="filename"):
        read_training_temporal_artifact(
            renamed, information_mode=batch.plan.information_mode
        )
    payload = json.dumps(batch.to_dict(), indent=2).encode()
    changed = (
        tmp_path
        / f"training-temporal-batch-{hashlib.sha256(payload).hexdigest()}.json"
    )
    changed.write_bytes(payload)
    with pytest.raises(ValueError, match="canonical"):
        read_training_temporal_artifact(
            changed, information_mode=batch.plan.information_mode
        )


def test_interrupted_atomic_publish_leaves_no_claimed_target(
    batch, tmp_path, monkeypatch
):
    import histdatacom.data_quality.training_temporal_artifacts as module

    def interrupted(*args, **kwargs):
        raise OSError("simulated interrupted publication")

    monkeypatch.setattr(module.os, "link", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        write_training_temporal_artifact(batch, tmp_path / "out")
    assert list((tmp_path / "out").iterdir()) == []


def test_existing_conflicting_file_never_overwritten(batch, tmp_path):
    payload = batch.to_json().encode()
    target = (
        tmp_path
        / f"training-temporal-batch-{hashlib.sha256(payload).hexdigest()}.json"
    )
    target.write_bytes(b"unrelated existing bytes")
    with pytest.raises(ValueError, match="existing temporal"):
        write_training_temporal_artifact(batch, tmp_path)
    assert target.read_bytes() == b"unrelated existing bytes"


def test_nonregular_inputs_are_refused_without_blocking(batch, tmp_path):
    path = write_training_temporal_artifact(batch, tmp_path / "original")
    folder = tmp_path / "links"
    folder.mkdir()
    alias = folder / path.name
    try:
        alias.symlink_to(path)
    except NotImplementedError:
        pass
    except OSError as exc:
        if os.name != "nt" or getattr(exc, "winerror", None) != 1314:
            raise
    else:
        with pytest.raises(ValueError, match="regular"):
            read_training_temporal_artifact(
                alias, information_mode=batch.plan.information_mode
            )
        alias.unlink()
    if hasattr(os, "mkfifo"):
        os.mkfifo(alias)
        with pytest.raises(ValueError, match="regular"):
            read_training_temporal_artifact(
                alias, information_mode=batch.plan.information_mode
            )

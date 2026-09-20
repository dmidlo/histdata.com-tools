"""Atomic no-clobber temporal manifests with mandatory clean source replay."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .training_lineage import read_training_regular
from .training_temporal_contracts import (
    TemporalInformationMode,
    TrainingTemporalBatchV1,
)
from .training_temporal_views import replay_training_temporal


def write_training_temporal_artifact(
    batch: TrainingTemporalBatchV1, directory: str | Path
) -> Path:
    """Re-execute before publication; interrupted writes never claim a target."""
    replay_training_temporal(batch)
    payload = batch.to_json().encode()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    target = (
        root
        / f"training-temporal-batch-{hashlib.sha256(payload).hexdigest()}.json"
    )
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".temporal-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if read_training_regular(target) != payload:
                raise ValueError(
                    "existing temporal artifact differs from canonical bytes"
                )
        else:
            if os.name != "nt":
                descriptor = os.open(
                    root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                )
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink()
    return target


def read_training_temporal_artifact(
    path: str | Path, *, information_mode: TemporalInformationMode
) -> TrainingTemporalBatchV1:
    """Check canonical bytes/name/mode, then re-execute every source dependency."""
    if type(information_mode) is not TemporalInformationMode:
        raise TypeError(
            "temporal read requires an explicit typed information mode"
        )
    path = Path(path)
    payload = read_training_regular(path)
    expected = (
        f"training-temporal-batch-{hashlib.sha256(payload).hexdigest()}.json"
    )
    if path.name != expected:
        raise ValueError("temporal artifact filename/content digest differs")
    batch = TrainingTemporalBatchV1.from_json(payload.decode())
    if batch.to_json().encode() != payload:
        raise ValueError("temporal artifact is not canonical JSON")
    if batch.plan.information_mode != information_mode:
        raise ValueError("temporal artifact information mode differs")
    return replay_training_temporal(batch)

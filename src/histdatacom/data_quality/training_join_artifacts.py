"""Canonical no-clobber publication, with mandatory executable join replay."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .training_join_contracts import JoinInformationMode, TrainingJoinBatchV1
from .training_join_views import replay_training_joins
from .training_lineage import read_training_regular


def write_training_join_artifact(
    batch: TrainingJoinBatchV1, directory: str | Path
) -> Path:
    replay_training_joins(batch)
    data = batch.to_json().encode()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    target = (
        root / f"training-join-batch-{hashlib.sha256(data).hexdigest()}.json"
    )
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".join-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if read_training_regular(target) != data:
                raise ValueError(
                    "existing join artifact conflicts with canonical bytes"
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


def read_training_join_artifact(
    path: str | Path, *, information_mode: JoinInformationMode
) -> TrainingJoinBatchV1:
    if type(information_mode) is not JoinInformationMode:
        raise TypeError(
            "join artifact reader requires explicit information mode"
        )
    source = Path(path)
    data = read_training_regular(source)
    if (
        source.name
        != f"training-join-batch-{hashlib.sha256(data).hexdigest()}.json"
    ):
        raise ValueError("join artifact filename does not bind actual bytes")
    batch = TrainingJoinBatchV1.from_json(data.decode())
    if (
        batch.to_json().encode() != data
        or batch.plan.information_mode is not information_mode
    ):
        raise ValueError(
            "join artifact is noncanonical or has a different mode"
        )
    return replay_training_joins(batch)

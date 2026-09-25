"""Write-once canonical training batches with mandatory clean-source replay."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .training_contracts import TrainingBatchV1, TrainingConsumerMode
from .training_lineage import read_training_regular
from .training_views import replay_training_batch
from .training_provider_policy import (
    publish_training_policy_receipt,
    require_training_retention,
    training_provider_subject,
    verify_training_policy_receipt,
)


def write_training_artifact(
    batch: TrainingBatchV1, directory: str | Path
) -> Path:
    """Replay before persistence; publish exact bytes without overwriting."""
    subject = training_provider_subject(batch)
    require_training_retention(subject)
    replay_training_batch(batch)
    payload = batch.to_json().encode()
    digest = hashlib.sha256(payload).hexdigest()
    root = Path(directory)
    require_training_retention(subject)
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"training-batch-{digest}.json"
    temporary: Path | None = None
    try:
        require_training_retention(subject)
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".training-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            require_training_retention(subject)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        publish_training_policy_receipt(subject, target, payload)
        require_training_retention(subject)
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if read_training_regular(target) != payload:
                raise ValueError(
                    "existing training artifact differs from canonical bytes"
                )
        else:
            if os.name != "nt":
                fd = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
    finally:
        if temporary is not None:
            temporary.unlink()
    verify_training_policy_receipt(subject, target, required=True)
    return target


def read_training_artifact(
    path: str | Path, *, consumer_mode: TrainingConsumerMode
) -> TrainingBatchV1:
    """Verify filename, exact canonical envelope, mode and all upstream bytes."""
    if type(consumer_mode) is not TrainingConsumerMode:
        raise TypeError(
            "training artifact reader requires an explicit typed mode"
        )
    source = Path(path)
    payload = read_training_regular(source)
    expected = f"training-batch-{hashlib.sha256(payload).hexdigest()}.json"
    if source.name != expected:
        raise ValueError("training artifact filename/content digest differs")
    batch = TrainingBatchV1.from_json(payload.decode("utf-8"))
    if batch.to_json().encode() != payload:
        raise ValueError("training artifact is not exact canonical JSON")
    if batch.request.consumer_mode != consumer_mode:
        raise ValueError("training artifact consumer mode differs")
    verify_training_policy_receipt(training_provider_subject(batch), source)
    return replay_training_batch(batch)

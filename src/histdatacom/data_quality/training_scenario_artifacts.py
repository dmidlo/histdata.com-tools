"""Write-once scenario views; every read freshly replays the native sources.

Content addressing checks retained bytes, not execution authority or empirical
qualification. Publication assumes a cooperative, quiescent local filesystem;
it is not an authentication boundary against another process with write access.
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from pathlib import Path

from histdatacom.managed_artifact_boundary import (
    assert_unmanaged_mutation_paths,
)

from .training_contracts import MAX_TRAINING_BYTES, TrainingConsumerMode
from .training_scenario_contracts import TrainingScenarioViewV1
from .training_scenario_views import replay_training_scenario_view


def _stamp(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read(path: Path) -> bytes:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_TRAINING_BYTES:
        raise ValueError("scenario view requires a bounded regular file")
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        data = stream.read(MAX_TRAINING_BYTES + 1)
        after = os.fstat(stream.fileno())
    if (
        _stamp(before) != _stamp(opened)
        or _stamp(opened) != _stamp(after)
        or _stamp(after) != _stamp(path.lstat())
        or len(data) != before.st_size
    ):
        raise ValueError("scenario view changed during guarded read")
    return data


def _filename(data: bytes) -> str:
    return f"training-scenario-view-{hashlib.sha256(data).hexdigest()}.json"


def _sync_directory(directory: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(
        directory,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_training_scenario_artifact(
    view: TrainingScenarioViewV1, directory: str | Path
) -> Path:
    """Freshly verify and atomically retain a complete immutable view.

    No original scientific input is rewritten. An interrupted temporary file is
    not a committed view and is never consulted by this reader or writer.
    """
    if type(view) is not TrainingScenarioViewV1:
        raise TypeError("scenario publication requires the exact view type")
    assert_unmanaged_mutation_paths((directory,))
    verified = replay_training_scenario_view(
        view, expected_view_id=view.artifact_id
    )
    # Publish the detached result that was replayed, not the caller's object.
    # Frozen dataclasses still permit deliberate object.__setattr__ mutation.
    data = verified.to_json().encode("ascii")
    if len(data) > MAX_TRAINING_BYTES:
        raise ValueError("scenario view exceeds publication byte bound")
    root = Path(directory)
    if root.is_symlink():
        raise ValueError("scenario publication directory must be real")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("scenario publication directory must be real")
    assert_unmanaged_mutation_paths((root,))
    target = root / _filename(data)
    if target.exists() or target.is_symlink():
        if _read(target) != data:
            raise ValueError("existing scenario artifact differs")
        return target
    descriptor, temporary = tempfile.mkstemp(prefix=".scenario-", dir=root)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        assert_unmanaged_mutation_paths((root,))
        try:
            os.link(temporary_path, target, follow_symlinks=False)
        except FileExistsError:
            if _read(target) != data:
                raise ValueError(
                    "concurrent scenario artifact differs"
                ) from None
        _sync_directory(root)
    finally:
        temporary_path.unlink()
    _sync_directory(root)
    if _read(target) != data:
        raise ValueError("published scenario artifact changed")
    return target


def read_training_scenario_artifact(
    path: str | Path,
    *,
    consumer_mode: TrainingConsumerMode,
    expected_view_id: str,
) -> TrainingScenarioViewV1:
    """Check exact bytes and intended identity, then replay the whole view.

    The expected identity is a caller-selected subject, not a cryptographic claim
    that a producer or the original execution environment was authenticated.
    """
    if type(consumer_mode) is not TrainingConsumerMode:
        raise TypeError("scenario reader requires an explicit consumer mode")
    if type(expected_view_id) is not str or not expected_view_id:
        raise ValueError("scenario reader requires an expected view identity")
    source = Path(path)
    data = _read(source)
    if source.name != _filename(data):
        raise ValueError("scenario artifact filename/content digest differs")
    view = TrainingScenarioViewV1.from_json(data.decode("ascii"))
    if view.to_json().encode("ascii") != data:
        raise ValueError("scenario artifact is not exact canonical JSON")
    if view.request.consumer_mode is not consumer_mode:
        raise ValueError("scenario artifact consumer mode differs")
    result = replay_training_scenario_view(
        view, expected_view_id=expected_view_id
    )
    if _read(source) != data:
        raise ValueError("scenario artifact changed during native replay")
    return result

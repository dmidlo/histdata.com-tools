"""Bounded atomic content-addressed overlap artifacts with fresh source replay."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from pathlib import Path

from .training_overlap_contracts import TrainingOverlapBatchV1
from .training_overlap_views import replay_training_overlap


def _parents(path: Path) -> tuple[tuple[str, int, int], ...]:
    result = []
    for parent in reversed(path.absolute().parents):
        try:
            stamp = parent.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(stamp.st_mode) or stat.S_ISLNK(stamp.st_mode):
            raise ValueError("overlap artifact parent must be a real directory")
        result.append((str(parent), stamp.st_dev, stamp.st_ino))
    return tuple(result)


def _stamp(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
    )


def _read_overlap_regular(path: Path) -> bytes:
    parents = _parents(path)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > 8 * 1024 * 1024:
        raise ValueError("overlap artifact must be a bounded regular file")
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if _stamp(opened) != _stamp(before) or not stat.S_ISREG(opened.st_mode):
            raise ValueError("overlap artifact changed before open")
        parts = []
        remaining = 8 * 1024 * 1024 + 1
        while remaining:
            part = os.read(descriptor, min(65536, remaining))
            if not part:
                break
            parts.append(part)
            remaining -= len(part)
        data = b"".join(parts)
        if (
            len(data) > 8 * 1024 * 1024
            or len(data) != before.st_size
            or _stamp(os.fstat(descriptor)) != _stamp(before)
            or _stamp(path.lstat()) != _stamp(before)
            or _parents(path) != parents
        ):
            raise ValueError("overlap artifact changed during bounded read")
        return data
    finally:
        os.close(descriptor)


def _fsync_directory(root: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(
            root,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def write_training_overlap_artifact(
    batch: TrainingOverlapBatchV1, directory: str | Path
) -> Path:
    replay_training_overlap(batch)
    data = batch.to_json().encode()
    root = Path(directory)
    _parents(root / "unpublished-overlap")
    root.mkdir(parents=True, exist_ok=True)
    target = (
        root / f"training-overlap-batch-{hashlib.sha256(data).hexdigest()}.json"
    )
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".overlap-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            if stream.write(data) != len(data):
                raise OSError("short overlap artifact write")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if _read_overlap_regular(target) != data:
                raise ValueError(
                    "existing overlap artifact conflicts with canonical bytes"
                )
        _fsync_directory(root)
    finally:
        if temporary is not None:
            temporary.unlink()
    return target


def read_training_overlap_artifact(
    path: str | Path, *, allow_expost: bool
) -> TrainingOverlapBatchV1:
    if allow_expost is not True:
        raise ValueError("overlap artifact requires explicit ex-post admission")
    source = Path(path)
    data = _read_overlap_regular(source)
    if (
        source.name
        != f"training-overlap-batch-{hashlib.sha256(data).hexdigest()}.json"
    ):
        raise ValueError("overlap artifact filename does not bind actual bytes")
    result = TrainingOverlapBatchV1.from_json(data.decode())
    if result.to_json().encode() != data:
        raise ValueError("overlap artifact is not canonical")
    return replay_training_overlap(result)

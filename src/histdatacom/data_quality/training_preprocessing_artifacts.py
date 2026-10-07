"""Write-once preprocessing fits with fresh native refitting on read/write.

Exact bytes/content addressing are integrity checks, not execution authority.
This POSIX no-follow publication protocol assumes a cooperative local filesystem,
not an authentication boundary against privileged concurrent writers. Native
refitting also performs its current source/rights checks; no cached fit flag or
caller-provided parameter hash substitutes for that execution.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
from pathlib import Path

from histdatacom.managed_artifact_boundary import (
    assert_unmanaged_mutation_paths,
)

from .training_contracts import MAX_TRAINING_BYTES
from .training_preprocessing_contracts import TrainingPreprocessingFitV1
from .training_preprocessing_views import replay_training_preprocessing_fit

_EXPECTED_ID = re.compile(r"training-preprocessing-fit:sha256:[0-9a-f]{64}\Z")


def _platform() -> None:
    if os.name != "posix" or not all(
        hasattr(os, name)
        for name in ("O_NOFOLLOW", "O_NONBLOCK", "O_DIRECTORY")
    ):
        raise ValueError("preprocessing artifacts require POSIX no-follow I/O")


def _path(value: str | Path) -> Path:
    if type(value) not in (str, type(Path())):
        raise TypeError("preprocessing artifact path must be exact text/Path")
    text = str(value)
    if (
        not text
        or len(text) > 4096
        or "\x00" in text
        or ".." in Path(text).parts
    ):
        raise ValueError("invalid bounded preprocessing artifact path")
    return Path(text).absolute()


def _stamp(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _ancestors(
    directory: Path, *, missing: bool = False
) -> tuple[tuple[Path, tuple[int, ...]], ...]:
    paths = (directory, *directory.parents)
    if len(paths) > 128:
        raise ValueError("preprocessing artifact path exceeds depth bound")
    found = []
    for path in paths:
        try:
            details = path.lstat()
        except FileNotFoundError:
            if missing:
                continue
            raise
        if not stat.S_ISDIR(details.st_mode):
            raise ValueError(
                "preprocessing artifact requires real directory ancestors"
            )
        # Directory times necessarily change when publishing a child. Identity
        # and mode must not; file content has its separate full metadata guard.
        found.append((path, (details.st_dev, details.st_ino, details.st_mode)))
    return tuple(found)


def _check_ancestors(
    expected: tuple[tuple[Path, tuple[int, ...]], ...],
) -> None:
    for path, stamp in expected:
        details = path.lstat()
        if (details.st_dev, details.st_ino, details.st_mode) != stamp:
            raise ValueError("preprocessing directory identity changed")


def _read(path: Path) -> tuple[bytes, tuple[int, ...]]:
    _platform()
    ancestors = _ancestors(path.parent)
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or not 0 < before.st_size <= MAX_TRAINING_BYTES
    ):
        raise ValueError("preprocessing fit requires a bounded regular file")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or _stamp(before) != _stamp(opened):
            raise ValueError("preprocessing file changed before guarded read")
        data = stream.read(MAX_TRAINING_BYTES + 1)
        after = os.fstat(stream.fileno())
    if (
        _stamp(before) != _stamp(after)
        or _stamp(after) != _stamp(path.lstat())
        or len(data) != before.st_size
    ):
        raise ValueError("preprocessing file changed during guarded read")
    _check_ancestors(ancestors)
    return data, _stamp(after)


def _filename(data: bytes) -> str:
    return f"training-preprocessing-fit-{hashlib.sha256(data).hexdigest()}.json"


def _sync(directory: Path) -> None:
    descriptor = os.open(
        directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_training_preprocessing_fit(
    fit: TrainingPreprocessingFitV1, directory: str | Path
) -> Path:
    """Freshly replay native fit, then atomically retain its detached result.

    An identical existing artifact is idempotent; any other occupant refuses.
    An interrupted temporary file is not committed authority for any reader.
    """
    _platform()
    if type(fit) is not TrainingPreprocessingFitV1:
        raise TypeError("preprocessing writer requires the exact fit type")
    root = _path(directory)
    assert_unmanaged_mutation_paths((root,))
    initial = _ancestors(root, missing=True)
    verified = replay_training_preprocessing_fit(
        fit, expected_fit_id=fit.artifact_id
    )
    # Only the independent replay result is serialized, never mutable caller
    # memory (even a frozen dataclass can be modified via object.__setattr__).
    data = verified.to_json().encode("ascii")
    if not 0 < len(data) <= MAX_TRAINING_BYTES:
        raise ValueError("preprocessing publication exceeds byte bound")
    _check_ancestors(initial)
    assert_unmanaged_mutation_paths((root,))
    _ancestors(root, missing=True)
    root.mkdir(parents=True, exist_ok=True)
    ancestors = _ancestors(root)
    target = root / _filename(data)
    assert_unmanaged_mutation_paths((root, target))
    if target.exists() or target.is_symlink():
        if _read(target)[0] != data:
            raise ValueError("existing preprocessing artifact differs")
        return target
    descriptor, name = tempfile.mkstemp(prefix=".preprocessing-", dir=root)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        _check_ancestors(ancestors)
        assert_unmanaged_mutation_paths((root, target))
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if _read(target)[0] != data:
                raise ValueError(
                    "concurrent preprocessing artifact differs"
                ) from None
        _sync(root)
    finally:
        _check_ancestors(ancestors)
        assert_unmanaged_mutation_paths((temporary,))
        temporary.unlink()
    _sync(root)
    _check_ancestors(ancestors)
    assert_unmanaged_mutation_paths((root, target))
    if _read(target)[0] != data:
        raise ValueError("published preprocessing artifact changed")
    return target


def read_training_preprocessing_fit(
    path: str | Path, *, expected_fit_id: str
) -> TrainingPreprocessingFitV1:
    """Admit canonical bytes and intended subject, then refit all native inputs."""
    if (
        type(expected_fit_id) is not str
        or _EXPECTED_ID.fullmatch(expected_fit_id) is None
    ):
        raise ValueError(
            "preprocessing reader requires an exact expected fit ID"
        )
    source = _path(path)
    ancestors = _ancestors(source.parent)
    data, stamp = _read(source)
    if source.name != _filename(data):
        raise ValueError(
            "preprocessing artifact filename/content digest differs"
        )
    try:
        fit = TrainingPreprocessingFitV1.from_json(data.decode("ascii"))
    except UnicodeDecodeError as error:
        raise ValueError(
            "preprocessing artifact is not canonical ASCII"
        ) from error
    if fit.to_json().encode("ascii") != data:
        raise ValueError("preprocessing artifact is not exact canonical JSON")
    if fit.artifact_id != expected_fit_id:
        raise ValueError("preprocessing artifact differs from expected fit ID")
    result = replay_training_preprocessing_fit(
        fit, expected_fit_id=expected_fit_id
    )
    _check_ancestors(ancestors)
    if _read(source) != (data, stamp):
        raise ValueError("preprocessing artifact changed during native replay")
    return result

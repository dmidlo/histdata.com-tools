"""Bounded no-follow canonical reads and no-clobber content-addressed writes."""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from typing import TypeVar

from ._wire import MAX_BYTES, Artifact

_A = TypeVar("_A", bound=Artifact)


def artifact_filename(artifact: Artifact) -> str:
    return (
        artifact.KIND + "-" + artifact.artifact_id.rsplit(":", 1)[1] + ".json"
    )


def _directory(path: Path) -> None:
    # Reject symlinks in every existing path component, not merely the leaf.
    current = path.absolute()
    while True:
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError(
                "attribution directory must be regular and nonsymlink"
            )
        if current.parent == current:
            break
        current = current.parent


def _read_bytes(path: Path) -> bytes:
    _directory(path.parent)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BYTES:
        raise ValueError("attribution file must be regular and bounded")
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (
            opened.st_dev,
            opened.st_ino,
        ) != (before.st_dev, before.st_ino):
            raise ValueError("attribution file changed before read")
        chunks: list[bytes] = []
        count = 0
        while True:
            chunk = os.read(descriptor, min(65536, MAX_BYTES + 1 - count))
            if not chunk:
                break
            chunks.append(chunk)
            count += len(chunk)
            if count > MAX_BYTES:
                raise ValueError("attribution file byte bound")
        after = os.fstat(descriptor)
        final = path.lstat()

        def stamp(s: os.stat_result) -> tuple[int, int, int, int, int]:
            return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)

        if (
            stamp(before) != stamp(after)
            or stamp(after) != stamp(final)
            or count != after.st_size
        ):
            raise ValueError("attribution file changed during read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def read_attribution_artifact(path: str | Path, artifact_type: type[_A]) -> _A:
    """Read exact bytes/schema/ID and run all native/graph validation for type."""
    source = Path(path)
    try:
        encoded = _read_bytes(source).decode("ascii")
        result = artifact_type.from_json(encoded)
        if source.name != artifact_filename(result):
            raise ValueError("attribution content filename mismatch")
        return result
    except (OSError, UnicodeError) as exc:
        raise ValueError(
            "unable to read canonical attribution artifact"
        ) from exc


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return  # Atomic publication supported; POSIX directory durability only.
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_attribution_artifact(artifact: _A, directory: str | Path) -> Path:
    """Publish canonical bytes last, never replace an existing historical file.

    Parent directory must already exist. Same-content repeats are idempotent;
    different bytes or symlinks at the content path refuse. POSIX fsync is used
    for file and directory durability; this is not a hostile-user filesystem.
    """
    target = Path(directory)
    _directory(target)
    encoded = artifact.to_json().encode("ascii")
    # Replay before publication, including nested native and registry checks.
    type(artifact).from_json(encoded.decode("ascii"))
    destination = target / artifact_filename(artifact)
    descriptor, temporary = tempfile.mkstemp(prefix=".attribution-", dir=target)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            if handle.write(encoded) != len(encoded):
                raise ValueError("short attribution artifact write")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, destination, follow_symlinks=False)
        except FileExistsError:
            if _read_bytes(destination) != encoded:
                raise ValueError("refusing to overwrite attribution evidence")
        _fsync_directory(target)
        return destination
    finally:
        temporary_path.unlink(missing_ok=True)

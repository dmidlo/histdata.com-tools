"""Private, cooperative POSIX filesystem boundary for the managed store.

No routine here decides that an artifact may be deleted. The store lifecycle
must verify native provenance and durably journal intent before calling unlink.
External privileged filesystem mutation is not an enforceable sandbox boundary.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.managed_artifact_boundary import (
    MANAGED_ARTIFACT_MARKER,
    assert_unmanaged_mutation_paths,
)

from .canonical import MAX_WIRE_BYTES
from .contracts import MAX_PAYLOAD_BYTES, RetentionPolicyV1, StoreMarkerV1

STORE_DIRECTORIES = (
    "descriptors",
    "holds",
    "journal",
    "objects",
    "plans",
    "receipts",
    "recipes",
    "roots",
    "tmp",
    "transactions",
)
LOCK_NAME = ".retention.lock"
MAX_STORE_FILES = 16_384
MAX_CONTROL_BYTES = 64 * 1024 * 1024
_CONTROL_NAME = re.compile(r"[0-9a-f]{64}\.json\Z")
_PAYLOAD_NAME = re.compile(r"[0-9a-f]{32}\.(?:csv|data|json|scratch)\Z")
_TEMP_NAME = re.compile(r"[0-9a-f]{32}\.tmp\Z")


class RetentionFilesystemError(ValueError):
    """Safety or durability could not be established; no retry is implied."""


@dataclass(frozen=True, slots=True)
class FileObservation:
    """Non-wire observation tied to an open store session, not deletion rights."""

    relative_path: str
    sha256: str
    size_bytes: int
    device: int
    inode: int
    mode: int
    mtime_ns: int
    ctime_ns: int


def _platform() -> Any:
    try:
        import fcntl
    except ImportError as exc:
        raise RetentionFilesystemError("POSIX locking is required") from exc
    if os.name != "posix" or any(
        not hasattr(os, name)
        for name in ("O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC", "O_NONBLOCK")
    ):
        raise RetentionFilesystemError(
            "required no-follow POSIX primitives unavailable"
        )
    if not {os.open, os.stat, os.unlink, os.mkdir, os.rename}.issubset(
        os.supports_dir_fd
    ):
        raise RetentionFilesystemError("required dirfd operations unavailable")
    return fcntl


def _absolute_path(path: str | os.PathLike[str]) -> str:
    value = os.fspath(path)
    if (
        type(value) is not str
        or not value
        or len(value) > 4096
        or "\x00" in value
    ):
        raise RetentionFilesystemError(
            "an explicit real directory path is required"
        )
    if any(part == ".." for part in value.split("/")):
        raise RetentionFilesystemError("parent traversal is not a store path")
    absolute = os.path.abspath(value)
    if len(absolute.encode("utf-8")) > 4096 or absolute in {
        "/",
        str(Path.home()),
    }:
        raise RetentionFilesystemError("broad or oversized store path refused")
    return absolute


def _directory_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _bounded_names(descriptor: int, maximum: int) -> list[str]:
    """Refuse before allocating an unbounded list of filesystem entries."""
    names: list[str] = []
    with os.scandir(descriptor) as entries:
        for entry in entries:
            if len(names) >= maximum:
                raise RetentionFilesystemError(
                    "aggregate managed directory bound exceeded"
                )
            names.append(entry.name)
    return sorted(names)


def _open_directory(path: str, *, create_leaf: bool = False) -> int:
    """Open every component without following links; return only the leaf fd."""
    descriptor = os.open("/", _directory_flags())
    try:
        parts = path.strip("/").split("/")
        for index, part in enumerate(parts):
            if not part or part in {".", ".."}:
                raise RetentionFilesystemError(
                    "invalid real directory component"
                )
            if index:
                try:
                    os.stat(
                        MANAGED_ARTIFACT_MARKER,
                        dir_fd=descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    pass
                else:
                    raise RetentionFilesystemError(
                        "nested managed store refused"
                    )
            try:
                child = os.open(part, _directory_flags(), dir_fd=descriptor)
            except FileNotFoundError:
                if not create_leaf or index != len(parts) - 1:
                    raise
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
                os.fsync(descriptor)
                child = os.open(part, _directory_flags(), dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _regular(value: os.stat_result, *, device: int, maximum: int) -> None:
    if (
        not stat.S_ISREG(value.st_mode)
        or value.st_nlink != 1
        or value.st_dev != device
        or not 0 <= value.st_size <= maximum
    ):
        raise RetentionFilesystemError(
            "unsafe file type, hardlink, device or size"
        )


def create_store_filesystem(
    path: str | os.PathLike[str], policy: RetentionPolicyV1
) -> StoreMarkerV1:
    """Initialize an explicitly empty namespace; failed initialization is retained.

    The marker precedes every payload/control directory. An interrupted or
    malformed marker therefore still makes legacy mutation routines refuse.
    """
    _platform()
    if type(policy) is not RetentionPolicyV1:
        raise RetentionFilesystemError("exact retention policy required")
    policy._readmit()
    absolute = _absolute_path(path)
    assert_unmanaged_mutation_paths((absolute,), recursive=True)
    root_fd = _open_directory(absolute, create_leaf=True)
    try:
        if _bounded_names(root_fd, 1):
            raise RetentionFilesystemError(
                "managed store requires a new empty namespace"
            )
        info = os.fstat(root_fd)
        marker = StoreMarkerV1(
            store_id=uuid.uuid4().hex,
            absolute_path=absolute,
            device=info.st_dev,
            inode=info.st_ino,
            created_ns=time.time_ns(),
            policy=policy,
            state="initializing",
        )
        _write_exclusive(
            root_fd, MANAGED_ARTIFACT_MARKER, marker.to_json().encode("ascii")
        )
        _write_exclusive(root_fd, LOCK_NAME, b"", mode=0o600)
        for name in STORE_DIRECTORIES:
            os.mkdir(name, mode=0o700, dir_fd=root_fd)
            directory = os.open(name, _directory_flags(), dir_fd=root_fd)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        os.fsync(root_fd)
        ready = StoreMarkerV1(
            store_id=marker.store_id,
            absolute_path=marker.absolute_path,
            device=marker.device,
            inode=marker.inode,
            created_ns=marker.created_ns,
            policy=policy,
            state="ready",
        )
        # Only this initialization transition replaces the reserved marker.
        temporary = uuid.uuid4().hex + ".tmp"
        _write_exclusive(root_fd, temporary, ready.to_json().encode("ascii"))
        os.rename(
            temporary,
            MANAGED_ARTIFACT_MARKER,
            src_dir_fd=root_fd,
            dst_dir_fd=root_fd,
        )
        os.fsync(root_fd)
        with StoreSession(absolute) as verified:
            if verified.marker != ready:
                raise RetentionFilesystemError(
                    "new store changed before initialization completed"
                )
            verified.inventory()
        return ready
    finally:
        os.close(root_fd)


def _write_exclusive(
    directory: int, name: str, data: bytes, *, mode: int = 0o400
) -> None:
    descriptor = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
        dir_fd=directory,
    )
    try:
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset : offset + 65_536])
            if written <= 0:
                raise RetentionFilesystemError("short immutable store write")
            offset += written
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.fsync(directory)


class StoreSession:
    """Private exclusive session. Every operation checks the pinned namespace."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = _absolute_path(path)
        self._fcntl = _platform()
        self._root_fd = -1
        self._lock_fd = -1
        self._directories: dict[str, int] = {}
        try:
            self._root_fd = _open_directory(self.path)
            info = os.fstat(self._root_fd)
            self.device, self.inode = info.st_dev, info.st_ino
            self._lock_fd = os.open(
                LOCK_NAME,
                os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                dir_fd=self._root_fd,
            )
            lock_info = os.fstat(self._lock_fd)
            _regular(lock_info, device=self.device, maximum=0)
            self._lock_identity = (lock_info.st_dev, lock_info.st_ino)
            try:
                self._fcntl.flock(
                    self._lock_fd, self._fcntl.LOCK_EX | self._fcntl.LOCK_NB
                )
            except BlockingIOError as exc:
                raise RetentionFilesystemError("managed store is busy") from exc
            for name in STORE_DIRECTORIES:
                directory = os.open(
                    name, _directory_flags(), dir_fd=self._root_fd
                )
                self._directories[name] = directory
                if os.fstat(directory).st_dev != self.device:
                    raise RetentionFilesystemError(
                        "cross-device managed directory"
                    )
            self._check_root()
            marker_bytes, marker_observation = self._read(
                MANAGED_ARTIFACT_MARKER, MAX_WIRE_BYTES, keep_bytes=True
            )
            self.marker = StoreMarkerV1.from_json(marker_bytes.decode("ascii"))
            if (
                self.marker.state != "ready"
                or self.marker.absolute_path != self.path
                or (self.marker.device, self.marker.inode)
                != (self.device, self.inode)
            ):
                raise RetentionFilesystemError(
                    "store incomplete, relocated or identity changed"
                )
            self._marker_bytes = marker_bytes
            self._marker_observation = marker_observation
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> StoreSession:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def close(self) -> None:
        for descriptor in self._directories.values():
            os.close(descriptor)
        self._directories.clear()
        for name in ("_lock_fd", "_root_fd"):
            descriptor = getattr(self, name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, name, -1)

    def _check_root(self) -> None:
        if self._root_fd < 0:
            raise RetentionFilesystemError("closed managed store session")
        current = _open_directory(self.path)
        try:
            info = os.fstat(current)
            if (info.st_dev, info.st_ino) != (self.device, self.inode):
                raise RetentionFilesystemError("managed root identity changed")
        finally:
            os.close(current)
        if set(
            _bounded_names(self._root_fd, len(STORE_DIRECTORIES) + 2)
        ) != set(STORE_DIRECTORIES) | {
            MANAGED_ARTIFACT_MARKER,
            LOCK_NAME,
        }:
            raise RetentionFilesystemError(
                "unexpected or missing store-root member"
            )
        lock = os.stat(LOCK_NAME, dir_fd=self._root_fd, follow_symlinks=False)
        _regular(lock, device=self.device, maximum=0)
        if (lock.st_dev, lock.st_ino) != self._lock_identity:
            raise RetentionFilesystemError("managed lock identity changed")
        for name, descriptor in self._directories.items():
            live = os.stat(name, dir_fd=self._root_fd, follow_symlinks=False)
            held = os.fstat(descriptor)
            if not stat.S_ISDIR(live.st_mode) or (live.st_dev, live.st_ino) != (
                held.st_dev,
                held.st_ino,
            ):
                raise RetentionFilesystemError(
                    "managed child directory identity changed"
                )
        if hasattr(self, "_marker_bytes"):
            marker_bytes, observed = self._read(
                MANAGED_ARTIFACT_MARKER, MAX_WIRE_BYTES, keep_bytes=True
            )
            if (
                marker_bytes != self._marker_bytes
                or observed != self._marker_observation
            ):
                raise RetentionFilesystemError(
                    "immutable managed policy marker changed"
                )

    def _location(self, relative_path: str) -> tuple[int, str]:
        if type(relative_path) is not str:
            raise RetentionFilesystemError(
                "exact generated relative path required"
            )
        if relative_path in {MANAGED_ARTIFACT_MARKER, LOCK_NAME}:
            return self._root_fd, relative_path
        parts = relative_path.split("/")
        if len(parts) != 2 or parts[0] not in self._directories:
            raise RetentionFilesystemError("unknown managed relative path")
        directory, name = parts
        pattern = (
            _PAYLOAD_NAME
            if directory == "objects"
            else _TEMP_NAME if directory == "tmp" else _CONTROL_NAME
        )
        if not pattern.fullmatch(name):
            raise RetentionFilesystemError(
                "unsafe or unexpected managed filename"
            )
        return self._directories[directory], name

    def _read(
        self, relative_path: str, maximum: int, *, keep_bytes: bool
    ) -> tuple[bytes, FileObservation]:
        directory, name = self._location(relative_path)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=directory,
        )
        try:
            before = os.fstat(descriptor)
            _regular(before, device=self.device, maximum=maximum)
            digest, pieces, count = hashlib.sha256(), [], 0
            while True:
                piece = os.read(descriptor, min(65_536, maximum - count + 1))
                if not piece:
                    break
                count += len(piece)
                if count > maximum:
                    raise RetentionFilesystemError(
                        "managed read exceeded bound"
                    )
                digest.update(piece)
                if keep_bytes:
                    pieces.append(piece)
            after = os.fstat(descriptor)
            current = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (
                count != before.st_size
                or _identity(before) != _identity(after)
                or _identity(before) != _identity(current)
            ):
                raise RetentionFilesystemError(
                    "managed file changed while reading"
                )
            observation = FileObservation(
                relative_path,
                digest.hexdigest(),
                count,
                before.st_dev,
                before.st_ino,
                before.st_mode,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            return b"".join(pieces), observation
        finally:
            os.close(descriptor)

    def read_bytes(
        self, relative_path: str, *, maximum: int = MAX_WIRE_BYTES
    ) -> bytes:
        self._check_root()
        if type(maximum) is not int or not 0 <= maximum <= MAX_PAYLOAD_BYTES:
            raise RetentionFilesystemError("invalid managed read bound")
        result, _ = self._read(relative_path, maximum, keep_bytes=True)
        return result

    def inventory(self) -> tuple[FileObservation, ...]:
        """Hash complete bounded inventory, including all unexpected-file checks."""
        self._check_root()
        result: list[FileObservation] = []
        control_bytes = payload_bytes = 0
        for directory in ("", *STORE_DIRECTORIES):
            descriptor = (
                self._root_fd if not directory else self._directories[directory]
            )
            maximum_names = (
                len(STORE_DIRECTORIES) + 2
                if not directory
                else MAX_STORE_FILES - len(result)
            )
            names = _bounded_names(descriptor, maximum_names)
            for name in names:
                if not directory and name in STORE_DIRECTORIES:
                    continue
                relative = f"{directory}/{name}" if directory else name
                maximum = (
                    MAX_PAYLOAD_BYTES
                    if directory == "objects"
                    else MAX_WIRE_BYTES
                )
                _, observed = self._read(relative, maximum, keep_bytes=False)
                result.append(observed)
                if directory == "objects":
                    payload_bytes += observed.size_bytes
                else:
                    control_bytes += observed.size_bytes
                if (
                    len(result) > MAX_STORE_FILES
                    or payload_bytes > MAX_PAYLOAD_BYTES
                    or control_bytes > MAX_CONTROL_BYTES
                ):
                    raise RetentionFilesystemError(
                        "aggregate managed inventory bound exceeded"
                    )
            if _bounded_names(descriptor, maximum_names) != names:
                raise RetentionFilesystemError(
                    "managed directory changed while scanning"
                )
        for observed in result:
            directory_fd, name = self._location(observed.relative_path)
            info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                info.st_dev,
                info.st_ino,
                info.st_mode,
                info.st_mtime_ns,
                info.st_ctime_ns,
                info.st_size,
                info.st_nlink,
            ) != (
                observed.device,
                observed.inode,
                observed.mode,
                observed.mtime_ns,
                observed.ctime_ns,
                observed.size_bytes,
                1,
            ):
                raise RetentionFilesystemError(
                    "managed inventory changed while scanning"
                )
        self._check_root()
        if self.read_bytes(MANAGED_ARTIFACT_MARKER) != self._marker_bytes:
            raise RetentionFilesystemError(
                "immutable managed policy marker changed"
            )
        return tuple(sorted(result, key=lambda item: item.relative_path))

    def write_immutable(
        self, relative_path: str, data: bytes
    ) -> FileObservation:
        """Exclusive durable publication; errors preserve any partial bytes."""
        self._check_root()
        directory, name = self._location(relative_path)
        maximum = (
            MAX_PAYLOAD_BYTES
            if relative_path.startswith("objects/")
            else MAX_WIRE_BYTES
        )
        if type(data) is not bytes or len(data) > maximum:
            raise RetentionFilesystemError("invalid immutable managed bytes")
        if directory == self._root_fd:
            raise RetentionFilesystemError(
                "reserved root members cannot be written"
            )
        if (
            relative_path.split("/")[0] not in {"objects", "tmp"}
            and name != hashlib.sha256(data).hexdigest() + ".json"
        ):
            raise RetentionFilesystemError(
                "control filename must bind its exact bytes"
            )
        _write_exclusive(directory, name, data)
        _, observation = self._read(relative_path, maximum, keep_bytes=False)
        if observation.sha256 != hashlib.sha256(data).hexdigest():
            raise RetentionFilesystemError(
                "immutable publication readback mismatch"
            )
        self._check_root()
        return observation

    def _unlink_payload(self, expected: FileObservation) -> None:
        """Private syscall seam; the higher-level store must journal intent first."""
        self._check_root()
        if type(
            expected
        ) is not FileObservation or not expected.relative_path.startswith(
            "objects/"
        ):
            raise RetentionFilesystemError(
                "only an observed payload can be unlinked"
            )
        _, current = self._read(
            expected.relative_path, MAX_PAYLOAD_BYTES, keep_bytes=False
        )
        if current != expected:
            raise RetentionFilesystemError(
                "payload changed after deletion planning"
            )
        directory, name = self._location(expected.relative_path)
        os.unlink(name, dir_fd=directory)
        # An exception here means durability is indeterminate, not that unlink
        # did not happen. The caller must stop and retain an honest outcome.
        os.fsync(directory)
        self._check_root()

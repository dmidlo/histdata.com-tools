"""Private bounded no-follow storage. Native facades own current-rights checks.

The callbacks are trusted host integration hooks, not caller-provided grants.
This module deliberately exports no public writer or scientific admission API.
"""

from __future__ import annotations

import os
import re
import stat
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import TypeVar, cast

from ._wire import MAX_BYTES, Artifact, load_json
from .chain import (
    _ChainState,
    link_provenance_entry,
    make_provenance_entry,
    verify_provenance_chain,
)
from .contracts import (
    BrokerProvenanceCheckpointV1,
    BrokerProvenanceEntryKind,
    BrokerProvenanceHeaderV1,
    BrokerProvenanceLinkV1,
    BrokerProvenanceSealV1,
    BrokerProvenanceTerminalV1,
    BrokerProvenanceVerificationReason,
    BrokerProvenanceVerificationV1,
)

_RESERVED = frozenset(
    {"header.json", "chain.jsonl", "checkpoints.jsonl", "seal.json"}
)
_MAX_METADATA_FILES = 16
_T = TypeVar("_T", bound=Artifact)


def _metadata_name(name: str) -> None:
    if (
        type(name) is not str
        or name in _RESERVED
        or re.fullmatch(r"[a-z][a-z0-9-]{0,63}\.json", name) is None
    ):
        raise ValueError("invalid provenance metadata filename")


def _directory_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _inventory(directory_fd: int) -> set[str]:
    names: set[str] = set()
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            if len(names) >= len(_RESERVED) + _MAX_METADATA_FILES:
                raise ValueError("provenance file inventory exceeds bound")
            names.add(entry.name)
    return names


def _regular(fd: int, maximum: int) -> int:
    info = os.fstat(fd)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_size > maximum
    ):
        raise ValueError("unsafe or oversized provenance file")
    return info.st_size


def _signature(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


class _ProvenanceWriter:
    def __init__(
        self,
        directory: str | Path,
        header: BrokerProvenanceHeaderV1,
        *,
        authorize_artifact: Callable[[Artifact], None],
        guard_text: Callable[[str], None],
        metadata: Mapping[str, str] | None = None,
    ):
        if type(header) is not BrokerProvenanceHeaderV1:
            raise ValueError("exact provenance header required")
        self._state = _ChainState(header)
        self.header = self._state.header
        requested = Path(directory).absolute()
        # Native capture APIs accept aliases in an existing parent. Resolve
        # only that parent; the final output directory remains no-follow and
        # no-clobber, including when its basename is already a symlink.
        self.directory = requested.parent.resolve(strict=True) / requested.name
        self.authorize_artifact = authorize_artifact
        self.guard_text = guard_text
        self._directory_fd: int | None = None
        self._chain_fd: int | None = None
        self._checkpoint_fd: int | None = None
        self._written_bytes = 0
        self._closed = False
        self._sealed = False
        self._owner = threading.current_thread()
        self._busy = False
        values = dict(metadata or {})
        if len(values) > _MAX_METADATA_FILES:
            raise ValueError("provenance metadata inventory exceeds bound")
        for name, text in values.items():
            _metadata_name(name)
            if type(load_json(text)) is not dict:
                raise ValueError("canonical provenance metadata required")
        parent = os.open(self.directory.parent, _directory_flags())
        try:
            self.guard_text(self.header.to_json())
            self.authorize_artifact(self.header)
            os.mkdir(self.directory.name, mode=0o700, dir_fd=parent)
            self.authorize_artifact(self.header)
            os.fsync(parent)
            self._directory_fd = os.open(
                self.directory.name, _directory_flags(), dir_fd=parent
            )
            self._write_file("header.json", self.header.to_json(), self.header)
            for name in sorted(values):
                self._write_file(name, values[name], self.header)
            self._chain_fd = self._create_file("chain.jsonl", self.header)
            self._checkpoint_fd = self._create_file(
                "checkpoints.jsonl", self.header
            )
        except BaseException:
            self.close()
            raise
        finally:
            os.close(parent)

    @property
    def entry_count(self) -> int:
        return self._state.count

    @property
    def root_sha256(self) -> str:
        return self._state.root

    @property
    def last_epoch(self) -> int:
        return max(self._state.max_epoch, 0)

    def _create_file(self, name: str, artifact: Artifact) -> int:
        if self._directory_fd is None or self._closed:
            raise ValueError("provenance writer is closed")
        self.authorize_artifact(artifact)
        fd = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=self._directory_fd,
        )
        try:
            self._sync_directory(artifact)
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _sync_directory(self, artifact: Artifact) -> None:
        if self._directory_fd is None:
            raise ValueError("provenance directory is closed")
        self.authorize_artifact(artifact)
        os.fsync(self._directory_fd)

    def _write_bytes(self, fd: int, text: str, artifact: Artifact) -> None:
        data = memoryview(text.encode("ascii"))
        if (
            self._written_bytes + len(data)
            > self.header.policy.max_capture_bytes
        ):
            raise ValueError("provenance capture byte bound")
        while data:
            self.authorize_artifact(artifact)
            written = os.write(fd, data)
            if written <= 0:
                raise OSError("provenance write made no progress")
            self._written_bytes += written
            data = data[written:]
        self.authorize_artifact(artifact)
        os.fsync(fd)

    def _write_file(self, name: str, text: str, artifact: Artifact) -> None:
        self.guard_text(text)
        # Guards may observe revocation; authorization follows the guard and
        # precedes creation, every write, and every explicit durability barrier.
        fd = self._create_file(name, artifact)
        try:
            self._write_bytes(fd, text, artifact)
        finally:
            os.close(fd)
        self._sync_directory(artifact)

    def _append(
        self,
        kind: BrokerProvenanceEntryKind,
        epoch: int,
        payload_json: str,
        native_record_id: str | None,
    ) -> BrokerProvenanceLinkV1:
        if self._closed or self._sealed or self._chain_fd is None:
            raise ValueError("provenance writer is not appendable")
        try:
            entry = make_provenance_entry(
                self.header,
                self._state.count,
                epoch,
                kind,
                payload_json,
                native_record_id=native_record_id,
            )
            link = link_provenance_entry(self._state.root, entry)
            checkpoint_due = self._state.accept(link)
            text = link.to_json() + "\n"
            self.guard_text(text)
            self._write_bytes(self._chain_fd, text, entry)
            if checkpoint_due:
                checkpoint = self._state.expected_checkpoint()
                self._state.accept_checkpoint(checkpoint)
                if self._checkpoint_fd is None:
                    raise ValueError("provenance checkpoint stream is closed")
                text = checkpoint.to_json() + "\n"
                self.guard_text(text)
                self._write_bytes(self._checkpoint_fd, text, checkpoint)
            return link
        except BaseException:
            self.close()
            raise

    def append(
        self,
        kind: BrokerProvenanceEntryKind,
        epoch: int,
        payload_json: str,
        *,
        native_record_id: str | None = None,
    ) -> BrokerProvenanceLinkV1:
        self._begin_operation()
        try:
            if kind is BrokerProvenanceEntryKind.TERMINAL:
                raise ValueError("use finish for the provenance terminal")
            return self._append(kind, epoch, payload_json, native_record_id)
        finally:
            self._busy = False

    def finish(
        self, terminal: BrokerProvenanceTerminalV1, *, epoch: int
    ) -> BrokerProvenanceSealV1:
        self._begin_operation()
        try:
            if type(terminal) is not BrokerProvenanceTerminalV1:
                raise ValueError("exact provenance terminal required")
            self._append(
                BrokerProvenanceEntryKind.TERMINAL,
                epoch,
                terminal.to_json(),
                None,
            )
            seal = self._state.make_seal()
            self._write_file("seal.json", seal.to_json(), seal)
            self._sealed = True
            return seal
        finally:
            self._busy = False
            self.close()

    def _begin_operation(self) -> None:
        if self._owner is not threading.current_thread() or self._busy:
            raise ValueError(
                "provenance writer requires one nonreentrant owner"
            )
        self._busy = True

    def close(self) -> None:
        self._closed = True
        for name in ("_chain_fd", "_checkpoint_fd", "_directory_fd"):
            fd = getattr(self, name)
            if fd is not None:
                setattr(self, name, None)
                os.close(fd)


@dataclass(frozen=True, slots=True)
class BrokerProvenanceReadResultV1:
    header: BrokerProvenanceHeaderV1
    links: tuple[BrokerProvenanceLinkV1, ...]
    checkpoints: tuple[BrokerProvenanceCheckpointV1, ...]
    seal: BrokerProvenanceSealV1 | None
    verification: BrokerProvenanceVerificationV1
    metadata: Mapping[str, str]
    partial_tail: bool


def _read_provenance(
    directory: str | Path,
    *,
    authorize_artifact: Callable[[Artifact], None],
    guard_text: Callable[[str], None],
    expected_header_id: str | None = None,
    expected_seal_id: str | None = None,
) -> BrokerProvenanceReadResultV1:
    """Private IO primitive; public native readers pre-check actual rights."""
    directory_fd = os.open(Path(directory), _directory_flags())
    directory_identity = os.fstat(directory_fd)
    total = 0
    signatures: dict[str, tuple[int, ...]] = {}
    try:
        names = _inventory(directory_fd)
        if not {"header.json", "chain.jsonl", "checkpoints.jsonl"} <= names:
            raise ValueError("missing provenance file")
        metadata_names = names - _RESERVED
        if len(metadata_names) > _MAX_METADATA_FILES:
            raise ValueError("provenance metadata inventory exceeds bound")
        for name in metadata_names:
            _metadata_name(name)

        def read_text(name: str, maximum: int) -> str:
            nonlocal total
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=directory_fd,
            )
            try:
                size = _regular(fd, maximum)
                signature = _signature(os.fstat(fd))
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    data = stream.read(maximum + 1)
                if (
                    len(data) != size
                    or len(data) > maximum
                    or _signature(os.fstat(fd)) != signature
                ):
                    raise ValueError("changed or oversized provenance file")
                signatures[name] = signature
                total += len(data)
                return data.decode("ascii")
            finally:
                os.close(fd)

        text = read_text("header.json", MAX_BYTES)
        header = BrokerProvenanceHeaderV1.from_json(text)
        guard_text(text)
        authorize_artifact(header)
        metadata: dict[str, str] = {}
        for name in sorted(metadata_names):
            text = read_text(name, MAX_BYTES)
            if type(load_json(text)) is not dict:
                raise ValueError("canonical provenance metadata required")
            guard_text(text)
            authorize_artifact(header)
            metadata[name] = text
            if total > header.policy.max_capture_bytes:
                raise ValueError("provenance capture byte bound")

        def read_records(
            name: str, cls: type[_T], maximum_count: int
        ) -> tuple[tuple[_T, ...], bool]:
            nonlocal total
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=directory_fd,
            )
            records: list[_T] = []
            partial = False
            try:
                _regular(fd, header.policy.max_capture_bytes)
                signature = _signature(os.fstat(fd))
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    while True:
                        data = stream.readline(
                            header.policy.max_entry_bytes + 1
                        )
                        if not data:
                            break
                        total += len(data)
                        if (
                            len(data) > header.policy.max_entry_bytes
                            or total > header.policy.max_capture_bytes
                            or len(records) >= maximum_count
                        ):
                            raise ValueError("provenance stream bound")
                        if not data.endswith(b"\n"):
                            partial = True
                            break
                        text = data[:-1].decode("ascii")
                        item = cls.from_json(text)
                        guard_text(text)
                        authorize_artifact(
                            cast(BrokerProvenanceLinkV1, item).entry
                            if type(item) is BrokerProvenanceLinkV1
                            else item
                        )
                        records.append(item)
                if _signature(os.fstat(fd)) != signature:
                    raise ValueError("provenance stream changed during read")
                signatures[name] = signature
                return tuple(records), partial
            finally:
                os.close(fd)

        links, chain_tail = read_records(
            "chain.jsonl", BrokerProvenanceLinkV1, header.policy.max_entries
        )
        checkpoints, checkpoint_tail = read_records(
            "checkpoints.jsonl",
            BrokerProvenanceCheckpointV1,
            header.policy.max_checkpoints,
        )
        seal = None
        if "seal.json" in names:
            text = read_text("seal.json", MAX_BYTES)
            seal = BrokerProvenanceSealV1.from_json(text)
            guard_text(text)
            authorize_artifact(seal)
        if total > header.policy.max_capture_bytes:
            raise ValueError("provenance capture byte bound")
        verification = verify_provenance_chain(
            header,
            links,
            checkpoints,
            seal,
            expected_header_id=expected_header_id,
            expected_seal_id=expected_seal_id,
        )
        partial = chain_tail or checkpoint_tail
        if partial:
            verification = replace(
                verification, reason=BrokerProvenanceVerificationReason.PARTIAL
            )
        # Later reads and guards can observe revocation of earlier artifacts.
        authorize_artifact(header)
        if seal is not None:
            authorize_artifact(seal)
        if _inventory(directory_fd) != names or any(
            _signature(
                os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            )
            != signature
            for name, signature in signatures.items()
        ):
            raise ValueError("provenance inventory changed during replay")
        current_directory = os.stat(directory, follow_symlinks=False)
        if (
            not stat.S_ISDIR(current_directory.st_mode)
            or current_directory.st_dev != directory_identity.st_dev
            or current_directory.st_ino != directory_identity.st_ino
        ):
            raise ValueError("provenance directory replaced during replay")
        return BrokerProvenanceReadResultV1(
            header,
            links,
            checkpoints,
            seal,
            verification,
            MappingProxyType(metadata),
            partial,
        )
    finally:
        os.close(directory_fd)

"""Locked, create-once receipt storage; structural reads are not authority.

Only this private storage layer writes receipt directories. No product/source
payload is copied, changed or removed. An interrupted temporary file is retained
as uncommitted work, never interpreted as a completed receipt.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar, cast

from typing_extensions import Self

from histdatacom.campaign_index_contracts import (
    CampaignArtifactRefV1,
    canonical,
    load_json,
)
from histdatacom.campaign_receipt_contracts import (
    MAX_PRODUCT_INPUT_FILES,
    MAX_VERIFICATION_PRODUCTS,
    RECEIPT_TYPES,
    CampaignControlReceiptV1,
    CampaignProductReceiptV1,
    CampaignReceipt,
    CampaignReceiptRefV1,
    CampaignVerificationCheckpointV1,
    CampaignVerificationJournalV1,
    CampaignVerificationRootV1,
    CampaignVerificationRunV1,
    CampaignVerificationShardV1,
    CampaignVerificationSummaryV1,
)
from histdatacom.managed_artifact_boundary import (
    assert_unmanaged_mutation_paths,
)

MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_STORE_FILES = 8 * MAX_VERIFICATION_PRODUCTS + 128
MAX_STORE_BYTES = 64 * 1024**3
_T = TypeVar("_T", bound=CampaignReceipt)
_TYPES = {cls.KIND: cls for cls in RECEIPT_TYPES}
_DIRECTORIES = (*_TYPES, "tmp")
_MARKER = "store.json"
_LOCK = ".lock"
_HEX_FILE = re.compile(r"[0-9a-f]{64}\.json\Z")
_TEMP_FILE = re.compile(r"[0-9a-f]{32}\.tmp\Z")


class CampaignReceiptStoreError(ValueError):
    """The exact receipt namespace cannot be safely read or modified."""


def _absolute(value: str | Path) -> Path:
    text = os.fspath(value)
    if (
        type(text) is not str
        or not text
        or len(text) > 4096
        or len(text.encode("utf-8")) > 4096
        or "\x00" in text
    ):
        raise CampaignReceiptStoreError("invalid receipt directory")
    path = Path(text).expanduser().absolute()
    if ".." in path.parts or path == Path(path.anchor):
        raise CampaignReceiptStoreError("unsafe receipt directory")
    return path


def _platform() -> Any:
    if os.name != "posix" or any(
        not hasattr(os, name)
        for name in ("O_NOFOLLOW", "O_NONBLOCK", "O_DIRECTORY")
    ):
        raise CampaignReceiptStoreError(
            "POSIX no-follow receipt store required"
        )
    import fcntl

    return fcntl


def _dir_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK


def _open_directory(path: Path, *, create: bool = False) -> int:
    descriptor = os.open(path.anchor, _dir_flags())
    try:
        for i, part in enumerate(path.parts[1:]):
            try:
                child = os.open(part, _dir_flags(), dir_fd=descriptor)
            except FileNotFoundError:
                if not create or i != len(path.parts) - 2:
                    raise
                os.mkdir(part, 0o700, dir_fd=descriptor)
                os.fsync(descriptor)
                child = os.open(part, _dir_flags(), dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _regular(info: os.stat_result, maximum: int, *, links: int = 1) -> None:
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != links
        or not 0 <= info.st_size <= maximum
    ):
        raise CampaignReceiptStoreError("unsafe receipt file type/size/links")


def _names(descriptor: int, maximum: int) -> tuple[str, ...]:
    names: list[str] = []
    with os.scandir(descriptor) as entries:
        for entry in entries:
            if len(names) >= maximum:
                raise CampaignReceiptStoreError("receipt inventory file bound")
            names.append(entry.name)
    return tuple(sorted(names))


def _read(
    descriptor: int, name: str, maximum: int, *, paired: bool = False
) -> bytes:
    before = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
    _regular(before, maximum, links=2 if paired and before.st_nlink == 2 else 1)
    opened = os.open(
        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor
    )
    try:
        if _identity(os.fstat(opened)) != _identity(before):
            raise CampaignReceiptStoreError("receipt changed while opening")
        chunks: list[bytes] = []
        size = 0
        while chunk := os.read(opened, min(65536, maximum + 1 - size)):
            size += len(chunk)
            if size > before.st_size:
                raise CampaignReceiptStoreError("receipt grew while reading")
            chunks.append(chunk)
        if size != before.st_size or _identity(os.fstat(opened)) != _identity(
            before
        ):
            raise CampaignReceiptStoreError("receipt changed while reading")
    finally:
        os.close(opened)
    if _identity(os.stat(name, dir_fd=descriptor, follow_symlinks=False)) != (
        _identity(before)
    ):
        raise CampaignReceiptStoreError("receipt replaced while reading")
    return b"".join(chunks)


def _write_new(descriptor: int, name: str, data: bytes) -> None:
    opened = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
        dir_fd=descriptor,
    )
    try:
        offset = 0
        while offset < len(data):
            count = os.write(opened, data[offset : offset + 65536])
            if count <= 0:
                raise CampaignReceiptStoreError("short receipt write")
            offset += count
        os.fchmod(opened, 0o400)
        os.fsync(opened)
    finally:
        os.close(opened)
    os.fsync(descriptor)


def _detached(value: CampaignReceipt) -> CampaignReceipt:
    if type(value) not in RECEIPT_TYPES:
        raise CampaignReceiptStoreError("exact closed receipt class required")
    # Class-method dispatch deliberately avoids any instance-overridden method.
    from dataclasses import fields

    result = type(value)(
        **{
            field.name: getattr(value, field.name)
            for field in fields(cast(Any, value))
        }
    )
    return type(value).from_json(result.to_json())


class CampaignReceiptStore:
    """Exclusive bounded store session. Low-level writes never certify inputs."""

    def __init__(
        self,
        directory: str | Path,
        *,
        create: CampaignVerificationRunV1 | None = None,
    ) -> None:
        self.path = _absolute(directory)
        self._fcntl = _platform()
        self._root_fd = self._lock_fd = -1
        self._directories: dict[str, int] = {}
        self._paired: dict[tuple[str, str], tuple[int, int]] = {}
        self._total_files = self._total_bytes = 0
        try:
            assert_unmanaged_mutation_paths((self.path,))
            if create is not None:
                if type(create) is not CampaignVerificationRunV1:
                    raise CampaignReceiptStoreError("exact native run required")
                create = cast(CampaignVerificationRunV1, _detached(create))
                self.assert_external(create.forbidden_roots)
            self._root_fd = _open_directory(
                self.path, create=create is not None
            )
            root_info = os.fstat(self._root_fd)
            self._root_identity = (root_info.st_dev, root_info.st_ino)
            if create is not None:
                if _names(self._root_fd, 1):
                    raise CampaignReceiptStoreError(
                        "new empty receipt store required"
                    )
                _write_new(self._root_fd, _LOCK, b"")
            self._lock_fd = os.open(
                _LOCK,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=self._root_fd,
            )
            lock_info = os.fstat(self._lock_fd)
            _regular(lock_info, 0)
            self._lock_identity = (lock_info.st_dev, lock_info.st_ino)
            try:
                self._fcntl.flock(
                    self._lock_fd, self._fcntl.LOCK_EX | self._fcntl.LOCK_NB
                )
            except BlockingIOError as error:
                raise CampaignReceiptStoreError(
                    "receipt store is busy"
                ) from error
            for name in _DIRECTORIES:
                if create is not None:
                    os.mkdir(name, 0o700, dir_fd=self._root_fd)
                descriptor = os.open(name, _dir_flags(), dir_fd=self._root_fd)
                self._directories[name] = descriptor
                if os.fstat(descriptor).st_dev != root_info.st_dev:
                    raise CampaignReceiptStoreError(
                        "cross-device receipt directory"
                    )
            if create is not None:
                os.fsync(self._root_fd)
                ref = self.write_immutable(create)
                marker = canonical(
                    {
                        "schema_version": "histdatacom.campaign-receipt-store.v1",
                        "run": ref.to_payload(),
                        "directory": str(self.path),
                        "device": root_info.st_dev,
                        "inode": root_info.st_ino,
                    }
                ).encode("ascii")
                _write_new(self._root_fd, _MARKER, marker)
            self._admit_link_pairs()
            self._marker = _read(self._root_fd, _MARKER, MAX_RECEIPT_BYTES)
            marker_value = load_json(self._marker.decode("ascii"))
            if (
                type(marker_value) is not dict
                or set(marker_value)
                != {"schema_version", "run", "directory", "device", "inode"}
                or canonical(marker_value).encode("ascii") != self._marker
            ):
                raise CampaignReceiptStoreError("invalid receipt store marker")
            if (
                marker_value["schema_version"]
                != "histdatacom.campaign-receipt-store.v1"
                or marker_value["directory"] != str(self.path)
                or (marker_value["device"], marker_value["inode"])
                != self._root_identity
            ):
                raise CampaignReceiptStoreError(
                    "receipt store moved or changed"
                )
            self.run_ref = CampaignReceiptRefV1.from_payload(
                marker_value["run"]
            )
            self.run = self.read(self.run_ref, CampaignVerificationRunV1)
            self.assert_external(self.run.forbidden_roots)
            self._inventory()
        except BaseException:
            self.close()
            raise

    def assert_external(self, roots: tuple[str, ...]) -> None:
        for root in roots:
            other = _absolute(root)
            if (
                self.path == other
                or self.path.is_relative_to(other)
                or other.is_relative_to(self.path)
            ):
                raise CampaignReceiptStoreError(
                    "receipt store overlaps generation roots"
                )

    def _check(self) -> None:
        if self._root_fd < 0:
            raise CampaignReceiptStoreError("closed receipt store")
        assert_unmanaged_mutation_paths((self.path,))
        current = _open_directory(self.path)
        try:
            if (
                os.fstat(current).st_dev,
                os.fstat(current).st_ino,
            ) != self._root_identity:
                raise CampaignReceiptStoreError("receipt root changed")
        finally:
            os.close(current)
        lock = os.stat(_LOCK, dir_fd=self._root_fd, follow_symlinks=False)
        _regular(lock, 0)
        if (lock.st_dev, lock.st_ino) != self._lock_identity:
            raise CampaignReceiptStoreError("receipt lock changed")
        for name, descriptor in self._directories.items():
            item = os.stat(name, dir_fd=self._root_fd, follow_symlinks=False)
            held = os.fstat(descriptor)
            if not stat.S_ISDIR(item.st_mode) or (item.st_dev, item.st_ino) != (
                held.st_dev,
                held.st_ino,
            ):
                raise CampaignReceiptStoreError("receipt subdirectory changed")
        if (
            hasattr(self, "_marker")
            and _read(self._root_fd, _MARKER, MAX_RECEIPT_BYTES) != self._marker
        ):
            raise CampaignReceiptStoreError("receipt marker changed")
        for (kind, name), identity in self._paired.items():
            info = os.stat(
                name, dir_fd=self._directories[kind], follow_symlinks=False
            )
            if (info.st_dev, info.st_ino) != identity or info.st_nlink != 2:
                raise CampaignReceiptStoreError(
                    "interrupted publication link changed"
                )

    def _admit_link_pairs(self) -> None:
        """Recognize ONLY the two retained links of interrupted publication."""
        found: dict[tuple[int, int], list[tuple[str, str]]] = {}
        count = 0
        for kind, directory in self._directories.items():
            for name in _names(directory, MAX_STORE_FILES - count):
                count += 1
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if info.st_nlink == 2:
                    found.setdefault((info.st_dev, info.st_ino), []).append(
                        (kind, name)
                    )
                elif info.st_nlink != 1:
                    raise CampaignReceiptStoreError("external receipt hardlink")
        for identity, members in found.items():
            if (
                len(members) != 2
                or sum(kind == "tmp" for kind, _ in members) != 1
            ):
                raise CampaignReceiptStoreError("unaccounted receipt hardlink")
            data = None
            for kind, name in members:
                if not (_TEMP_FILE if kind == "tmp" else _HEX_FILE).fullmatch(
                    name
                ):
                    raise CampaignReceiptStoreError(
                        "invalid paired receipt name"
                    )
                current = _read(
                    self._directories[kind],
                    name,
                    MAX_RECEIPT_BYTES,
                    paired=True,
                )
                if data is not None and current != data:
                    raise CampaignReceiptStoreError(
                        "paired publication bytes differ"
                    )
                data = current
                if (
                    kind != "tmp"
                    and hashlib.sha256(current).hexdigest() + ".json" != name
                ):
                    raise CampaignReceiptStoreError(
                        "paired receipt hash differs"
                    )
                self._paired[(kind, name)] = identity

    def _read_member(self, kind: str, name: str) -> bytes:
        return _read(
            self._directories[kind],
            name,
            MAX_RECEIPT_BYTES,
            paired=(kind, name) in self._paired,
        )

    def _inventory(self) -> None:
        self._check()
        if set(_names(self._root_fd, len(_DIRECTORIES) + 2)) != {
            *_DIRECTORIES,
            _MARKER,
            _LOCK,
        }:
            raise CampaignReceiptStoreError("unexpected receipt root member")
        count = total = 0
        for kind, directory in self._directories.items():
            names = _names(directory, MAX_STORE_FILES - count)
            for name in names:
                if not (_TEMP_FILE if kind == "tmp" else _HEX_FILE).fullmatch(
                    name
                ):
                    raise CampaignReceiptStoreError(
                        "unexpected receipt filename"
                    )
                data = self._read_member(kind, name)
                count += 1
                total += len(data)
                if total > MAX_STORE_BYTES:
                    raise CampaignReceiptStoreError(
                        "aggregate receipt store byte bound"
                    )
                if kind != "tmp":
                    if hashlib.sha256(data).hexdigest() + ".json" != name:
                        raise CampaignReceiptStoreError(
                            "receipt filename/hash differs"
                        )
                    value = _TYPES[kind].from_json(data.decode("ascii"))
                    if (
                        kind not in {"run", "summary"}
                        and getattr(value, "run_id", None)
                        != self.run.artifact_id
                    ):
                        raise CampaignReceiptStoreError("foreign receipt run")
                    if kind == "run" and value != self.run:
                        raise CampaignReceiptStoreError(
                            "multiple receipt run identities"
                        )
            if names != _names(directory, MAX_STORE_FILES):
                raise CampaignReceiptStoreError("receipt inventory changed")
        self._total_files, self._total_bytes = count, total
        self._check()

    def read(self, ref: CampaignReceiptRefV1, cls: type[_T]) -> _T:
        self._check()
        if type(ref) is not CampaignReceiptRefV1 or cls not in RECEIPT_TYPES:
            raise CampaignReceiptStoreError(
                "exact closed receipt reference required"
            )
        ref = CampaignReceiptRefV1.from_payload(ref.to_payload())
        if ref.kind != cls.KIND:
            raise CampaignReceiptStoreError("unexpected receipt reference kind")
        data = self._read_member(ref.kind, ref.sha256 + ".json")
        if (
            len(data) != ref.size_bytes
            or hashlib.sha256(data).hexdigest() != ref.sha256
        ):
            raise CampaignReceiptStoreError(
                "receipt bytes differ from reference"
            )
        value = cls.from_json(data.decode("ascii"))
        if value.artifact_id != ref.artifact_id:
            raise CampaignReceiptStoreError(
                "receipt identity differs from reference"
            )
        self._check()
        return value

    def write_immutable(self, record: CampaignReceipt) -> CampaignReceiptRefV1:
        self._check()
        value = _detached(record)
        data = value.to_json().encode("ascii")
        digest = hashlib.sha256(data).hexdigest()
        ref = CampaignReceiptRefV1(
            value.KIND, value.artifact_id, len(data), digest
        )
        if (
            hasattr(self, "run")
            and value.KIND not in {"run", "summary"}
            and getattr(value, "run_id", None) != self.run.artifact_id
        ):
            raise CampaignReceiptStoreError("cannot write foreign run receipt")
        directory = self._directories[value.KIND]
        name = digest + ".json"
        try:
            existing = self._read_member(value.KIND, name)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            if existing != data:
                raise CampaignReceiptStoreError(
                    "existing immutable receipt differs"
                )
            self._check()
            return ref
        if (
            self._total_files + 1 > MAX_STORE_FILES
            or self._total_bytes + len(data) > MAX_STORE_BYTES
        ):
            raise CampaignReceiptStoreError(
                "prospective receipt store capacity bound"
            )
        temporary = uuid.uuid4().hex + ".tmp"
        temp_dir = self._directories["tmp"]
        _write_new(temp_dir, temporary, data)
        self._check()
        # Only remove our own freshly written temporary link. Recovery also
        # recognizes an exact two-link pair if this process is interrupted.
        os.link(
            temporary,
            name,
            src_dir_fd=temp_dir,
            dst_dir_fd=directory,
            follow_symlinks=False,
        )
        os.fsync(directory)
        os.unlink(temporary, dir_fd=temp_dir)
        os.fsync(temp_dir)
        self._total_files += 1
        self._total_bytes += len(data)
        if self.read(ref, type(value)).to_json().encode("ascii") != data:
            raise CampaignReceiptStoreError(
                "receipt publication readback differs"
            )
        return ref

    def refs(self, kind: str) -> Iterator[CampaignReceiptRefV1]:
        if kind not in _TYPES:
            raise CampaignReceiptStoreError("unknown receipt kind")
        self._check()

        for name in _names(self._directories[kind], MAX_STORE_FILES):
            if _HEX_FILE.fullmatch(name) is None:
                raise CampaignReceiptStoreError("invalid receipt filename")
            data = self._read_member(kind, name)
            if hashlib.sha256(data).hexdigest() + ".json" != name:
                raise CampaignReceiptStoreError("receipt filename/hash differs")
            value = _TYPES[kind].from_json(data.decode("ascii"))
            yield CampaignReceiptRefV1(
                kind, value.artifact_id, len(data), name[:-5]
            )
        self._check()

    def reserve_capacity(self, *, files: int, size_bytes: int) -> None:
        """Prospective logical-byte admission, not an OS disk reservation."""
        self._check()
        if (
            type(files) is not int
            or type(size_bytes) is not int
            or files < 0
            or size_bytes < 0
            or self._total_files + files > MAX_STORE_FILES
            or self._total_bytes + size_bytes > MAX_STORE_BYTES
        ):
            raise CampaignReceiptStoreError(
                "prospective receipt store capacity bound"
            )

    def admit_namespace(self) -> None:
        """Recheck every closed member before accepting a final outcome."""
        self._inventory()
        self.journal()

    def find(
        self, identity: str, cls: type[_T]
    ) -> tuple[CampaignReceiptRefV1, _T]:
        if (
            type(identity) is not str
            or re.fullmatch(
                rf"campaign-receipt-{cls.KIND}:sha256:[0-9a-f]{{64}}", identity
            )
            is None
        ):
            raise CampaignReceiptStoreError("invalid expected receipt identity")
        found = None
        for ref in self.refs(cls.KIND):
            if ref.artifact_id == identity:
                if found is not None:
                    raise CampaignReceiptStoreError(
                        "duplicate receipt identity"
                    )
                found = (ref, self.read(ref, cls))
        if found is None:
            raise CampaignReceiptStoreError("expected receipt not retained")
        return found

    def journal(
        self,
    ) -> tuple[tuple[CampaignReceiptRefV1, CampaignVerificationJournalV1], ...]:
        entries = sorted(
            (
                (ref, self.read(ref, CampaignVerificationJournalV1))
                for ref in self.refs("journal")
            ),
            key=lambda pair: pair[1].ordinal,
        )
        previous = None
        active_products: list[CampaignReceiptRefV1] = []
        active_shards: list[CampaignReceiptRefV1] = []
        committed: set[CampaignReceiptRefV1] = set()
        reused: CampaignReceiptRefV1 | None = None
        started = False
        for ordinal, (ref, entry) in enumerate(entries):
            if (
                entry.run_id != self.run.artifact_id
                or entry.ordinal != ordinal
                or entry.previous != previous
            ):
                raise CampaignReceiptStoreError(
                    "journal is not one exact contiguous chain"
                )
            subject: Any = self.read(entry.subject, _TYPES[entry.subject.kind])
            if entry.event == "started":
                if entry.subject != self.run_ref:
                    raise CampaignReceiptStoreError(
                        "journal start has foreign run"
                    )
                active_products.clear()
                active_shards.clear()
                reused = None
                started = True
            elif entry.event == "resumed":
                if entry.subject not in committed:
                    raise CampaignReceiptStoreError(
                        "resumed checkpoint was not previously committed"
                    )
                active_products = [
                    item_ref
                    for item_ref, _ in iter_checkpoint_products(self, subject)
                ]
                active_shards = list(subject.shards)
                reused = entry.subject
                started = True
            elif entry.event == "failed":
                started = False  # A later attempt requires a fresh start.
            elif not started:
                raise CampaignReceiptStoreError(
                    "journal outcome has no started attempt"
                )
            elif entry.event == "product":
                if entry.product_ordinal != len(
                    active_products
                ) or subject.ordinal != len(active_products):
                    raise CampaignReceiptStoreError(
                        "journal product prefix differs"
                    )
                active_products.append(entry.subject)
            elif entry.event == "shard":
                first = len(active_shards) * self.run.products_per_shard
                if (
                    subject.products_per_shard != self.run.products_per_shard
                    or subject.ordinal != len(active_shards)
                    or subject.products != tuple(active_products[first:])
                ):
                    raise CampaignReceiptStoreError(
                        "journal shard prefix differs"
                    )
                active_shards.append(entry.subject)
            elif entry.event == "checkpoint":
                first = len(active_shards) * self.run.products_per_shard
                if (
                    subject.products_per_shard != self.run.products_per_shard
                    or subject.journal_head != previous
                    or subject.shards != tuple(active_shards)
                    or subject.pending_products
                    != tuple(active_products[first:])
                    or subject.next_product_ordinal != len(active_products)
                ):
                    raise CampaignReceiptStoreError(
                        "journal checkpoint prefix differs"
                    )
                committed.add(entry.subject)
            elif entry.event == "completed":
                if (
                    subject.reused_checkpoint != reused
                    or subject.shards != tuple(active_shards)
                    or subject.product_count != len(active_products)
                ):
                    raise CampaignReceiptStoreError(
                        "journal root prefix differs"
                    )
                started = False
            previous = ref
        return tuple(entries)

    def close(self) -> None:
        for descriptor in self._directories.values():
            os.close(descriptor)
        self._directories.clear()
        if self._lock_fd >= 0:
            os.close(self._lock_fd)
            self._lock_fd = -1
        if self._root_fd >= 0:
            os.close(self._root_fd)
            self._root_fd = -1

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()


def iter_checkpoint_products(
    store: CampaignReceiptStore, checkpoint: CampaignVerificationCheckpointV1
) -> Iterator[tuple[CampaignReceiptRefV1, CampaignProductReceiptV1]]:
    """Admit an exact complete-shard prefix, without granting replay authority."""
    if (
        checkpoint.run_id != store.run.artifact_id
        or checkpoint.products_per_shard != store.run.products_per_shard
        or checkpoint.pending_products
    ):
        raise CampaignReceiptStoreError("checkpoint full-shard prefix differs")
    ordinal = 0
    coordinates: set[tuple[str, str, str]] = set()
    for shard_ordinal, ref in enumerate(checkpoint.shards):
        shard = store.read(ref, CampaignVerificationShardV1)
        if (
            shard.run_id,
            shard.ordinal,
            shard.first_product_ordinal,
            shard.products_per_shard,
            len(shard.products),
        ) != (
            checkpoint.run_id,
            shard_ordinal,
            ordinal,
            checkpoint.products_per_shard,
            checkpoint.products_per_shard,
        ):
            raise CampaignReceiptStoreError("checkpoint shard geometry differs")
        totals = [0, 0, 0, 0]
        for product_ref in shard.products:
            product = store.read(product_ref, CampaignProductReceiptV1)
            if (
                product.run_id != checkpoint.run_id
                or product.ordinal != ordinal
            ):
                raise CampaignReceiptStoreError(
                    "checkpoint product ordinal/run differs"
                )
            core = product.verification
            coordinate = (core.run_id, core.window_id, core.ensemble_member_id)
            if coordinate in coordinates:
                raise CampaignReceiptStoreError(
                    "checkpoint repeats a product coordinate"
                )
            coordinates.add(coordinate)
            for index, value in enumerate(
                (
                    core.observed_event_count,
                    core.synthetic_event_count,
                    product.elapsed_ns,
                    product.read_bytes,
                )
            ):
                totals[index] += value
            ordinal += 1
            yield product_ref, product
        if tuple(totals) != (
            shard.observed_event_count,
            shard.synthetic_event_count,
            shard.elapsed_ns,
            shard.read_bytes,
        ):
            raise CampaignReceiptStoreError(
                "checkpoint shard measured totals differ"
            )
    if ordinal != checkpoint.next_product_ordinal:
        raise CampaignReceiptStoreError(
            "checkpoint product denominator differs"
        )


def _input_digest(files: Any) -> str:
    digest = hashlib.sha256(b"histdatacom.campaign-inputs.v1\n")
    for item in files:
        digest.update(
            canonical(
                {
                    "path": item.path,
                    "sha256": item.sha256,
                    "size_bytes": item.size_bytes,
                }
            ).encode("ascii")
            + b"\n"
        )
    return digest.hexdigest()


def iter_tree_controls(
    store: CampaignReceiptStore, root: CampaignVerificationRootV1
) -> Iterator[tuple[CampaignReceiptRefV1, CampaignControlReceiptV1]]:
    """Validate complete global and per-plan leaf framing, including no-output plans."""
    global_digest = hashlib.sha256(b"histdatacom.campaign-inputs.v1\n")
    plan_digest = hashlib.sha256(b"histdatacom.campaign-plan-inputs.v1\n")
    previous_path = ""
    plans: set[str] = set()
    shards: set[str] = set()
    ordinal = 0
    for scope, refs in (
        ("global", root.global_controls),
        ("plan", root.plan_controls),
    ):
        for position, ref in enumerate(refs):
            control = store.read(ref, CampaignControlReceiptV1)
            if (control.run_id, control.ordinal, control.scope) != (
                root.run_id,
                ordinal,
                scope,
            ):
                raise CampaignReceiptStoreError(
                    "control ordinal/scope/run differs"
                )
            if _input_digest(control.input_files) != control.inputs_sha256:
                raise CampaignReceiptStoreError("control input digest differs")
            if scope == "global":
                if (
                    position + 1 < len(refs)
                    and len(control.input_files) != MAX_PRODUCT_INPUT_FILES
                ):
                    raise CampaignReceiptStoreError(
                        "global control chunk geometry differs"
                    )
                for item in control.input_files:
                    if item.path <= previous_path:
                        raise CampaignReceiptStoreError(
                            "global control paths overlap or reorder"
                        )
                    previous_path = item.path
                    global_digest.update(
                        canonical(
                            {
                                "path": item.path,
                                "sha256": item.sha256,
                                "size_bytes": item.size_bytes,
                            }
                        ).encode("ascii")
                        + b"\n"
                    )
            else:
                assert (
                    control.plan_id is not None and control.shard_id is not None
                )
                if control.plan_id in plans or control.shard_id in shards:
                    raise CampaignReceiptStoreError(
                        "duplicate control plan/shard"
                    )
                plans.add(control.plan_id)
                shards.add(control.shard_id)
                plan_digest.update(
                    canonical(
                        {
                            "shard_id": control.shard_id,
                            "inputs_sha256": control.inputs_sha256,
                        }
                    ).encode("ascii")
                    + b"\n"
                )
            ordinal += 1
            yield ref, control
    summary = CampaignVerificationSummaryV1.from_json(root.summary_json)
    digest = hashlib.sha256(
        canonical(
            {
                "domain": "histdatacom.campaign-control-inputs.v1",
                "global_controls_sha256": global_digest.hexdigest(),
                "plan_inputs_sha256": plan_digest.hexdigest(),
            }
        ).encode("ascii")
    ).hexdigest()
    if digest != summary.control_inputs_sha256:
        raise CampaignReceiptStoreError("root control summary differs")


def iter_tree_products(
    store: CampaignReceiptStore, root: CampaignVerificationRootV1
) -> Iterator[tuple[CampaignReceiptRefV1, CampaignProductReceiptV1]]:
    """Validate all ordered child identities/counts with one product in memory."""
    if (
        root.run_id != store.run.artifact_id
        or root.products_per_shard != store.run.products_per_shard
    ):
        raise CampaignReceiptStoreError("root/run binding differs")
    ordinal = observed = synthetic = elapsed = read_bytes = 0
    reused_shards = 0
    if root.reused_checkpoint is not None:
        checkpoint = store.read(
            root.reused_checkpoint, CampaignVerificationCheckpointV1
        )
        for _ in iter_checkpoint_products(store, checkpoint):
            pass
        reused_shards = len(checkpoint.shards)
        if (
            checkpoint.inventory_sha256 != root.initial_inventory_sha256
            or checkpoint.shards != root.shards[:reused_shards]
        ):
            raise CampaignReceiptStoreError(
                "root reused checkpoint prefix differs"
            )
    plan_bindings: set[tuple[str | None, str | None]] = set()
    for _, control in iter_tree_controls(store, root):
        elapsed += control.elapsed_ns
        read_bytes += control.read_bytes
        if control.scope == "plan":
            plan_bindings.add((control.plan_id, control.shard_id))
    core_digest = hashlib.sha256(b"histdatacom.campaign-products.v1\n")
    inputs_digest = hashlib.sha256(b"histdatacom.campaign-product-inputs.v1\n")
    coordinates: set[tuple[str, str, str]] = set()
    for shard_ordinal, ref in enumerate(root.shards):
        shard = store.read(ref, CampaignVerificationShardV1)
        if (
            shard.run_id,
            shard.ordinal,
            shard.first_product_ordinal,
            shard.products_per_shard,
        ) != (root.run_id, shard_ordinal, ordinal, root.products_per_shard) or (
            shard_ordinal + 1 < len(root.shards)
            and len(shard.products) != root.products_per_shard
        ):
            raise CampaignReceiptStoreError("shard prefix geometry differs")
        totals = [0, 0, 0, 0]
        for product_ref in shard.products:
            product = store.read(product_ref, CampaignProductReceiptV1)
            if product.run_id != root.run_id or product.ordinal != ordinal:
                raise CampaignReceiptStoreError("product ordinal/run differs")
            core = product.verification
            if (core.plan_id, core.shard_id) not in plan_bindings:
                raise CampaignReceiptStoreError(
                    "product lacks its exact plan control"
                )
            coordinate = (core.run_id, core.window_id, core.ensemble_member_id)
            if coordinate in coordinates:
                raise CampaignReceiptStoreError(
                    "tree repeats a product coordinate"
                )
            coordinates.add(coordinate)
            core_digest.update(product.core_json.encode("ascii") + b"\n")
            leaves = hashlib.sha256(b"histdatacom.campaign-inputs.v1\n")
            for item in product.input_files:
                leaves.update(
                    canonical(
                        {
                            "path": item.path,
                            "sha256": item.sha256,
                            "size_bytes": item.size_bytes,
                        }
                    ).encode("ascii")
                    + b"\n"
                )
            inputs_digest.update(
                canonical(
                    {"ordinal": ordinal, "inputs_sha256": leaves.hexdigest()}
                ).encode("ascii")
                + b"\n"
            )
            for index, value in enumerate(
                (
                    core.observed_event_count,
                    core.synthetic_event_count,
                    product.elapsed_ns,
                    product.read_bytes,
                )
            ):
                totals[index] += value
            ordinal += 1
            yield product_ref, product
        if tuple(totals) != (
            shard.observed_event_count,
            shard.synthetic_event_count,
            shard.elapsed_ns,
            shard.read_bytes,
        ):
            raise CampaignReceiptStoreError("shard measured totals differ")
        observed += totals[0]
        synthetic += totals[1]
        if shard_ordinal >= reused_shards:
            elapsed += totals[2]
            read_bytes += totals[3]
    if (
        ordinal != root.product_count
        or root.elapsed_ns < elapsed
        or root.read_bytes < read_bytes
    ):
        raise CampaignReceiptStoreError(
            "root product or measured totals differ"
        )
    summary = CampaignVerificationSummaryV1.from_json(root.summary_json)
    if (
        observed,
        synthetic,
        core_digest.hexdigest(),
        inputs_digest.hexdigest(),
    ) != (
        summary.observed_event_count,
        summary.synthetic_event_count,
        summary.product_verifications_sha256,
        summary.product_inputs_sha256,
    ) or summary.index_id != store.run.index_id:
        raise CampaignReceiptStoreError(
            "root summary does not reconcile children"
        )
    index_ref = CampaignArtifactRefV1.from_dict(
        load_json(summary.index_ref_json)
    )
    if (
        index_ref.path != store.run.index_path
        or index_ref.sha256 != store.run.index_sha256
    ):
        raise CampaignReceiptStoreError("root summary index differs from run")


def read_campaign_verification_tree(
    store_directory: str | Path, *, expected_root_id: str
) -> CampaignVerificationRootV1:
    """Read exact retained tree; never establishes current input integrity."""
    with CampaignReceiptStore(store_directory) as store:
        ref, root = store.find(expected_root_id, CampaignVerificationRootV1)
        entries = store.journal()
        if not any(
            entry.event == "completed" and entry.subject == ref
            for _, entry in entries
        ):
            raise CampaignReceiptStoreError(
                "root lacks durable completion event"
            )
        for _ in iter_tree_products(store, root):
            pass
        store.admit_namespace()
        return root


def get_campaign_verification_root_ref(
    store_directory: str | Path, *, expected_root_id: str
) -> CampaignArtifactRefV1:
    """Return exact root bytes after structural validation, not fresh authority."""
    with CampaignReceiptStore(store_directory) as store:
        ref, root = store.find(expected_root_id, CampaignVerificationRootV1)
        if not any(
            event.event == "completed" and event.subject == ref
            for _, event in store.journal()
        ):
            raise CampaignReceiptStoreError(
                "root lacks durable completion event"
            )
        for _ in iter_tree_products(store, root):
            pass
        store.admit_namespace()
        return CampaignArtifactRefV1(
            "campaign_verification_root_v1",
            str(store.path / "root" / (ref.sha256 + ".json")),
            ref.size_bytes,
            ref.sha256,
            canonical(
                {
                    "verification_root_id": root.artifact_id,
                    "run_id": root.run_id,
                    "product_index_id": CampaignVerificationSummaryV1.from_json(
                        root.summary_json
                    ).index_id,
                    "status": "complete",
                    "verification_level": "structural_receipt_tree_only",
                    "standalone_publication_authority": False,
                }
            ),
        )

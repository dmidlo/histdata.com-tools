"""Fsynced append-only partitions and hash-bound, explicitly partial manifests."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from copy import copy
from dataclasses import dataclass, replace
import hashlib
import os
from pathlib import Path
import stat
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

from .contracts import (
    MAX_LIFECYCLE_BYTES,
    BrokerLifecycleCompletion as Completion,
    BrokerLifecycleError,
    BrokerLifecycleReason as Reason,
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePartitionV1,
    BrokerLifecycleRecordV1,
)
from .evidence import Evidence


def _open_read(path: Path, maximum: int) -> int:
    descriptor = -1
    try:
        if os.name != "posix":
            raise ValueError
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_size > maximum:
            raise ValueError
        return descriptor
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        raise BrokerLifecycleError(Reason.INTEGRITY) from None


def _read(path: Path, maximum: int) -> bytes:
    descriptor = _open_read(path, maximum)
    with os.fdopen(descriptor, "rb") as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise BrokerLifecycleError(Reason.INTEGRITY)
    return data


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Journal:
    def __init__(
        self,
        directory: Path,
        header: BrokerLifecycleHeaderV1,
        *,
        provider_request: BrokerSDKInvocationV1,
        before_persist: Callable[[str], None] | None = None,
    ) -> None:
        from histdatacom.broker_plugin_capabilities.execution import (
            _provider_request,
        )

        self.header = header
        self.provider_request = _provider_request(header.plan, provider_request)
        self.before_persist = before_persist
        self.evidence = Evidence(header)
        self._retain(header)
        # A new caller-owned local directory is mandatory; never reuse a run.
        directory.mkdir(mode=0o700)
        _sync_directory(directory.parent)
        self.directory = directory
        self.partitions: list[BrokerLifecyclePartitionV1] = []
        self.descriptor: int | None = None
        self.hash = hashlib.sha256()
        self.bytes = self.records = self.events = self.total_bytes = 0
        self.first_sequence = 0
        self.poisoned = False
        self.manifest = BrokerLifecycleManifestV1(
            header, self.evidence.state, Completion.OPEN
        )
        self.publish(self.manifest)

    def _name(self, partial: bool = True) -> Path:
        return self.directory / (
            f"partition-{len(self.partitions):04d}."
            + ("partial" if partial else "jsonl")
        )

    def append(self, record: BrokerLifecycleRecordV1) -> None:
        if self.poisoned:
            raise BrokerLifecycleError(Reason.PERSISTENCE)
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKLifecycleV1,
        )
        from histdatacom.broker_plugin_policy.contracts import (
            BrokerPolicyOperation,
        )
        from histdatacom.broker_plugin_policy.scope import (
            require_provider_operation,
        )

        if record.kind in ("identity", "session", "event"):
            require_provider_operation(
                BrokerSDKLifecycleV1(
                    self.provider_request,
                    self.header,
                    record,
                    self.evidence.session,
                    self.evidence.identity,
                ),
                BrokerPolicyOperation.CAPTURE,
            )
        self._retain(record)
        encoded = (record.to_json() + "\n").encode("ascii")
        self._remaining_bytes(len(encoded))
        policy = self.header.policy
        if (
            len(encoded) > policy.partition_bytes
            or self.total_bytes + len(encoded) > policy.max_capture_bytes
        ):
            raise BrokerLifecycleError(Reason.PERSISTENCE)
        if self.records and (
            self.records >= policy.partition_events
            or self.bytes + len(encoded) > policy.partition_bytes
        ):
            self.seal()
        if self.descriptor is None:
            if len(self.partitions) >= policy.max_partitions:
                raise BrokerLifecycleError(Reason.PERSISTENCE)
            self.descriptor = os.open(
                self._name(),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
            # A durable file body alone does not make its newly created name
            # durable. The ACK boundary includes the directory entry too.
            _sync_directory(self.directory)
            self.first_sequence = record.capture_sequence
        # Verify before append. A filesystem exception never results in an ACK.
        candidate = copy(self.evidence)
        candidate.accept(record)
        self._retain(record)
        data = memoryview(encoded)
        try:
            while data:
                written = os.write(self.descriptor, data)
                if written <= 0:
                    raise OSError
                data = data[written:]
            os.fsync(self.descriptor)
        except Exception:
            # Never append a terminal record after an unknown partial write.
            # Keep OPEN evidence for inspection; replay refuses this tail.
            self.poisoned = True
            raise BrokerLifecycleError(Reason.PERSISTENCE) from None
        self.evidence = candidate
        self.hash.update(encoded)
        self.bytes += len(encoded)
        self.total_bytes += len(encoded)
        self.records += 1
        self.events += record.kind == "event"

    def _receipt(self) -> BrokerLifecyclePartitionV1 | None:
        if not self.records:
            return None
        return BrokerLifecyclePartitionV1(
            len(self.partitions),
            self.hash.hexdigest(),
            self.bytes,
            self.first_sequence,
            self.records,
            self.events,
        )

    def seal(self) -> None:
        receipt = self._receipt()
        if receipt is None:
            return
        self._retain(receipt)
        assert self.descriptor is not None
        os.fsync(self.descriptor)
        os.close(self.descriptor)
        self.descriptor = None
        self._retain(receipt)
        os.link(self._name(), self._name(False), follow_symlinks=False)
        self._name().unlink()
        _sync_directory(self.directory)
        self.partitions.append(receipt)
        self.hash = hashlib.sha256()
        self.bytes = self.records = self.events = 0

    def publish(self, manifest: BrokerLifecycleManifestV1) -> None:
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKLifecycleV1,
        )
        from histdatacom.broker_plugin_policy.scope import BrokerPolicyError
        from histdatacom.broker_plugin_policy.storage import (
            write_broker_policy_receipt,
        )

        self._retain(manifest)
        encoded = manifest.to_json().encode("ascii")
        # Include the old sidecar and transient new native body in the same
        # capture-byte ceiling; no off-quota admission evidence is hidden.
        write_broker_policy_receipt(
            self.directory.resolve() / "manifest.json.provider-policy.json",
            BrokerSDKLifecycleV1(
                self.provider_request,
                self.header,
                manifest,
            ),
            native_artifact_name="manifest.json",
            native_file_bytes=encoded,
            replace_existing=True,
            maximum_bytes=min(
                8 * 1024 * 1024, self._remaining_bytes(len(encoded))
            ),
            before_persist=self.before_persist,
        )
        self._remaining_bytes(len(encoded))
        temporary = self.directory / "manifest.pending"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            self._retain(manifest)
            os.replace(temporary, self.directory / "manifest.json")
            _sync_directory(self.directory)
            self.manifest = manifest
        except BrokerPolicyError:
            raise
        except Exception:
            raise BrokerLifecycleError(Reason.PERSISTENCE) from None

    def finish(
        self, manifest: BrokerLifecycleManifestV1, *, complete: bool
    ) -> BrokerLifecycleManifestV1:
        if self.poisoned:
            raise BrokerLifecycleError(Reason.PERSISTENCE)
        if complete:
            self.seal()
        elif self.descriptor is not None:
            os.fsync(self.descriptor)
            os.close(self.descriptor)
            self.descriptor = None
        result = replace(
            manifest,
            completion=Completion.COMPLETE if complete else Completion.PARTIAL,
            partitions=tuple(self.partitions),
            partial_partition=None if complete else self._receipt(),
        )
        self.publish(result)
        return result

    def close(self) -> None:
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None

    def _retain(self, record: object) -> None:
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKLifecycleV1,
        )
        from histdatacom.broker_plugin_policy.contracts import (
            BrokerPolicyOperation,
        )
        from histdatacom.broker_plugin_policy.scope import (
            require_provider_operation,
        )

        # Compact manifests do not enumerate every buffered payload class.
        # The declared envelope is the conservative complete upper bound;
        # actual records are additionally checked without rewriting V1 bytes.
        require_provider_operation(
            self.provider_request, BrokerPolicyOperation.RETAIN_LOCAL
        )
        require_provider_operation(
            BrokerSDKLifecycleV1(
                self.provider_request,
                self.header,
                record,
                self.evidence.session,
                self.evidence.identity,
            ),
            BrokerPolicyOperation.RETAIN_LOCAL,
        )

    def _remaining_bytes(self, reserved: int = 0) -> int:
        size = 0
        with os.scandir(self.directory) as entries:
            for index, entry in enumerate(entries):
                if index >= 260 or not entry.is_file(follow_symlinks=False):
                    raise BrokerLifecycleError(Reason.PERSISTENCE)
                size += entry.stat(follow_symlinks=False).st_size
        remaining = self.header.policy.max_capture_bytes - size - reserved
        if remaining < 0:
            raise BrokerLifecycleError(Reason.PERSISTENCE)
        return remaining


@dataclass(frozen=True, slots=True)
class BrokerLifecycleInspectionV1:
    """Metadata/shape inspection only; complete is not a verified certificate.

    Use replay_broker_lifecycle to verify every referenced byte and binding.
    """

    manifest: BrokerLifecycleManifestV1
    partial_files: tuple[str, ...]
    complete: bool


def inspect_broker_lifecycle(directory: Path) -> BrokerLifecycleInspectionV1:
    """Read claimed local completion; do not hash or authenticate partitions."""
    try:
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError
        manifest = BrokerLifecycleManifestV1.from_json(
            _read(directory / "manifest.json", MAX_LIFECYCLE_BYTES).decode(
                "ascii"
            )
        )
        # Bounded listing; unexpected files mean interrupted/foreign state.
        names: list[str] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if len(names) >= 260 or not entry.is_file(
                    follow_symlinks=False
                ):
                    raise ValueError
                names.append(entry.name)
        partial = tuple(
            sorted(
                name
                for name in names
                if name.endswith(".partial") or name == "manifest.pending"
            )
        )
        expected = {"manifest.json"} | {
            f"partition-{item.ordinal:04d}.jsonl"
            for item in manifest.partitions
        }
        if "manifest.json.provider-policy.json" in names:
            expected.add("manifest.json.provider-policy.json")
        if manifest.partial_partition is not None:
            expected.add(
                f"partition-{manifest.partial_partition.ordinal:04d}.partial"
            )
        if (
            manifest.completion is not Completion.OPEN
            and set(names) != expected
        ):
            raise ValueError
        complete = manifest.completion is Completion.COMPLETE and not partial
        return BrokerLifecycleInspectionV1(manifest, partial, complete)
    except Exception:
        raise BrokerLifecycleError(Reason.INTEGRITY) from None


def replay_broker_lifecycle(
    directory: Path,
    *,
    provider_request: BrokerSDKInvocationV1,
) -> Iterator[BrokerLifecycleRecordV1]:
    """Verify the entire hash-bound retained run before yielding any record.

    OPEN interrupted tails are intentionally not admitted. They remain visible
    through inspect_broker_lifecycle, without fabricating a trusted prefix.
    """
    from histdatacom.broker_plugin_policy.scope import BrokerPolicyError

    try:
        yield from _replay(directory, provider_request)
    except BrokerPolicyError:
        raise
    except Exception:
        raise BrokerLifecycleError(Reason.INTEGRITY) from None


def _replay(
    directory: Path,
    provider_request: BrokerSDKInvocationV1,
) -> Iterator[BrokerLifecycleRecordV1]:
    from histdatacom.broker_plugin_capabilities.execution import (
        _provider_request,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKLifecycleV1
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )
    from histdatacom.broker_plugin_policy.storage import (
        read_broker_policy_receipt,
        verify_broker_policy_receipt,
    )

    inspection = inspect_broker_lifecycle(directory)
    manifest = inspection.manifest
    request = _provider_request(manifest.header.plan, provider_request)
    require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
    # Match the writer's canonical parent without following either file leaf.
    native_path = directory.resolve(strict=True) / "manifest.json"
    sidecar = native_path.with_name(native_path.name + ".provider-policy.json")
    if sidecar.exists() or sidecar.is_symlink():
        verify_broker_policy_receipt(
            read_broker_policy_receipt(sidecar),
            BrokerSDKLifecycleV1(request, manifest.header, manifest),
            native_path,
        )
    if manifest.completion is Completion.OPEN:
        raise BrokerLifecycleError(Reason.INTEGRITY)
    receipts = manifest.partitions + (
        ()
        if manifest.partial_partition is None
        else (manifest.partial_partition,)
    )
    evidence = Evidence(manifest.header)
    total_bytes = 0
    for receipt in receipts:
        require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
        suffix = "partial" if receipt is manifest.partial_partition else "jsonl"
        data = _read(
            directory / f"partition-{receipt.ordinal:04d}.{suffix}",
            receipt.byte_count,
        )
        if (
            len(data) != receipt.byte_count
            or hashlib.sha256(data).hexdigest() != receipt.sha256
            or not data.endswith(b"\n")
        ):
            raise BrokerLifecycleError(Reason.INTEGRITY)
        records = events = 0
        for line in data.splitlines():
            record = BrokerLifecycleRecordV1.from_json(line.decode("ascii"))
            evidence.accept(record)
            records += 1
            events += record.kind == "event"
        total_bytes += len(data)
        if records != receipt.record_count or events != receipt.event_count:
            raise BrokerLifecycleError(Reason.INTEGRITY)
    if (
        evidence.state is not manifest.state
        or evidence.sequence != manifest.appended_records
        or evidence.event_count != manifest.appended_events
        or evidence.error_diagnostics != manifest.error_diagnostics
        or total_bytes > manifest.header.policy.max_capture_bytes
        or (evidence.unknown_loss and not manifest.unknown_loss)
        or (
            evidence.unknown_loss and manifest.completion is Completion.COMPLETE
        )
    ):
        raise BrokerLifecycleError(Reason.INTEGRITY)
    # Re-open and re-hash on the yielding pass: no unbounded retained run.
    yielding = Evidence(manifest.header)
    for receipt in receipts:
        require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
        suffix = "partial" if receipt is manifest.partial_partition else "jsonl"
        data = _read(
            directory / f"partition-{receipt.ordinal:04d}.{suffix}",
            receipt.byte_count,
        )
        if hashlib.sha256(data).hexdigest() != receipt.sha256:
            raise BrokerLifecycleError(Reason.INTEGRITY)
        for line in data.splitlines():
            record = BrokerLifecycleRecordV1.from_json(line.decode("ascii"))
            require_provider_operation(
                BrokerSDKLifecycleV1(
                    request,
                    manifest.header,
                    record,
                    yielding.session,
                    yielding.identity,
                ),
                BrokerPolicyOperation.MATERIAL_USE,
            )
            yielding.accept(record)
            yield record

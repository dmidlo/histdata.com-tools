"""Crash-safe append-only storage and verified replay for broker captures."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from histdatacom.broker_capture.adapters import (
    BrokerCaptureEventConsumerV1,
    BrokerCaptureEventSourceV1,
    consume_broker_capture_source,
)
from histdatacom.broker_capture.contracts import (
    BROKER_CAPTURE_DATA_ARTIFACT_KIND,
    BrokerCaptureEventV1,
    BrokerCapturePartitionManifestV1,
    BrokerCaptureReplaySummaryV1,
    BrokerCaptureSessionManifestV1,
    BrokerCaptureSessionState,
    BrokerCaptureSessionV1,
    BrokerCaptureStoragePolicyV1,
)
from histdatacom.runtime_contracts import ArtifactRef

SESSION_MANIFEST_FILENAME = "session.manifest.json"
PARTITION_DATA_TEMPLATE = "partition-{ordinal:06d}.jsonl"
PARTITION_MANIFEST_TEMPLATE = "partition-{ordinal:06d}.manifest.json"


class BrokerCaptureStorageError(RuntimeError):
    """Base class for fail-closed capture-storage errors."""


class BrokerCaptureQuotaError(BrokerCaptureStorageError):
    """Hard disk quota would be exceeded."""


class BrokerCaptureBackpressureError(BrokerCaptureStorageError):
    """Configured high-watermark backpressure refused another event."""


class BrokerCaptureRetentionError(BrokerCaptureStorageError):
    """Immutable retention ceiling refused another partition."""


class BrokerCaptureIntegrityError(BrokerCaptureStorageError):
    """Stored capture evidence failed hash, count, or ordering verification."""


class BrokerCaptureExistingSessionError(BrokerCaptureStorageError):
    """A new writer would overlap an existing session directory."""


@dataclass(frozen=True, slots=True)
class BrokerCaptureSessionInspectionV1:
    """Read-only detection of incomplete or unadvertised session artifacts."""

    session_directory: str
    advertised_partition_count: int
    partial_artifacts: tuple[str, ...]
    orphan_data_artifacts: tuple[str, ...]
    orphan_manifest_artifacts: tuple[str, ...]

    @property
    def clean(self) -> bool:
        """Return whether every on-disk artifact is committed and advertised."""
        return not (
            self.partial_artifacts
            or self.orphan_data_artifacts
            or self.orphan_manifest_artifacts
        )


class AppendOnlyBrokerCaptureWriterV1:
    """Rotate canonical JSONL partitions and publish only completed evidence."""

    def __init__(
        self,
        root: str | Path,
        *,
        session: BrokerCaptureSessionV1,
        storage_policy: BrokerCaptureStoragePolicyV1,
    ) -> None:
        self.root = Path(root)
        self.session = session
        self.storage_policy = storage_policy
        self.session_directory = self.root / session.session_id
        if self.session_directory.exists() and any(
            self.session_directory.iterdir()
        ):
            raise BrokerCaptureExistingSessionError(
                "capture session directory already contains evidence"
            )
        self.session_directory.mkdir(parents=True, exist_ok=True)
        self._partitions: list[BrokerCapturePartitionManifestV1] = []
        self._event_kind_counts: dict[str, int] = {}
        self._total_events = 0
        self._last_monotonic_ns: int | None = None
        self._file: BinaryIO | None = None
        self._partial_path: Path | None = None
        self._partition_event_count = 0
        self._partition_bytes = 0
        self._partition_first_event: BrokerCaptureEventV1 | None = None
        self._partition_last_event: BrokerCaptureEventV1 | None = None
        self._partition_kind_counts: dict[str, int] = {}
        self._closed = False
        self._manifest = self._publish_session_manifest(
            BrokerCaptureSessionState.OPEN, limitations=()
        )

    @property
    def manifest(self) -> BrokerCaptureSessionManifestV1:
        """Return the latest compact manifest snapshot."""
        return self._manifest

    def append(self, event: BrokerCaptureEventV1) -> None:
        """Append one event, rotating before limits are crossed."""
        if self._closed:
            raise BrokerCaptureStorageError("capture writer is closed")
        if event.session_id != self.session.session_id:
            raise ValueError("capture event belongs to another session")
        if event.capture_sequence != self._total_events:
            raise ValueError("capture event sequence is not contiguous")
        if (
            self._last_monotonic_ns is not None
            and event.receive_time_monotonic_ns < self._last_monotonic_ns
        ):
            raise ValueError("capture event monotonic time moved backwards")
        line = (event.to_json() + "\n").encode("utf-8")
        if len(line) > self.storage_policy.max_partition_bytes:
            raise BrokerCaptureQuotaError(
                "one capture event exceeds the partition byte limit"
            )
        if self._should_rotate(event, len(line)):
            self._finalize_partition()
        starting_partition = self._file is None
        self._enforce_storage_limits(len(line), starting_partition)
        if starting_partition:
            self._open_partition()
        assert self._file is not None
        try:
            self._file.write(line)
            self._file.flush()
            if self.storage_policy.fsync_each_event:
                os.fsync(self._file.fileno())
        except OSError as err:
            raise BrokerCaptureStorageError(
                "capture partition append failed"
            ) from err
        self._partition_bytes += len(line)
        self._partition_event_count += 1
        if self._partition_first_event is None:
            self._partition_first_event = event
        self._partition_last_event = event
        self._partition_kind_counts[event.kind.value] = (
            self._partition_kind_counts.get(event.kind.value, 0) + 1
        )
        self._event_kind_counts[event.kind.value] = (
            self._event_kind_counts.get(event.kind.value, 0) + 1
        )
        self._total_events += 1
        self._last_monotonic_ns = event.receive_time_monotonic_ns

    def close(
        self,
        *,
        completed: bool = True,
        limitations: Sequence[str] = (),
    ) -> BrokerCaptureSessionManifestV1:
        """Finalize valid data and publish terminal capture health."""
        if self._closed:
            return self._manifest
        self._finalize_partition()
        state = (
            BrokerCaptureSessionState.COMPLETED
            if completed
            else BrokerCaptureSessionState.FAILED
        )
        self._manifest = self._publish_session_manifest(
            state, limitations=limitations
        )
        self._closed = True
        return self._manifest

    def __enter__(self) -> "AppendOnlyBrokerCaptureWriterV1":
        return self

    def __exit__(
        self, exc_type: object, exc: object, traceback: object
    ) -> None:
        if exc_type is None:
            self.close(completed=True)
            return
        limitation = (
            f"collector_failure:{getattr(exc_type, '__name__', 'unknown')}"
        )
        self.close(completed=False, limitations=(limitation,))

    def _should_rotate(
        self, event: BrokerCaptureEventV1, line_bytes: int
    ) -> bool:
        if self._file is None or self._partition_first_event is None:
            return False
        if (
            self._partition_event_count
            >= self.storage_policy.max_partition_events
        ):
            return True
        if (
            self._partition_bytes + line_bytes
            > self.storage_policy.max_partition_bytes
        ):
            return True
        elapsed = (
            event.receive_time_monotonic_ns
            - self._partition_first_event.receive_time_monotonic_ns
        )
        return elapsed >= self.storage_policy.max_partition_duration_ns

    def _enforce_storage_limits(
        self, line_bytes: int, starting_partition: bool
    ) -> None:
        if starting_partition and (
            len(self._partitions) >= self.storage_policy.max_retained_partitions
        ):
            raise BrokerCaptureRetentionError(
                "capture retention ceiling reached; committed evidence was not deleted"
            )
        projected = (
            _directory_size(self.session_directory)
            + line_bytes
            + self.storage_policy.manifest_reserve_bytes
        )
        if projected > self.storage_policy.max_session_bytes:
            raise BrokerCaptureQuotaError(
                "capture session disk quota would be exceeded"
            )
        if projected > self.storage_policy.high_watermark_bytes:
            raise BrokerCaptureBackpressureError(
                "capture storage high watermark requires backpressure"
            )

    def _open_partition(self) -> None:
        ordinal = len(self._partitions)
        partial_name = (
            PARTITION_DATA_TEMPLATE.format(ordinal=ordinal) + ".partial"
        )
        partial_path = self.session_directory / partial_name
        try:
            self._file = partial_path.open("xb")
        except OSError as err:
            raise BrokerCaptureStorageError(
                "could not create capture partial partition"
            ) from err
        self._partial_path = partial_path
        self._partition_event_count = 0
        self._partition_bytes = 0
        self._partition_first_event = None
        self._partition_last_event = None
        self._partition_kind_counts = {}

    def _finalize_partition(self) -> None:
        if self._file is None:
            return
        file_handle = self._file
        partial_path = self._partial_path
        first = self._partition_first_event
        last = self._partition_last_event
        if partial_path is None or first is None or last is None:
            raise BrokerCaptureStorageError(
                "capture partition state is incomplete"
            )
        try:
            file_handle.flush()
            os.fsync(file_handle.fileno())
            file_handle.close()
            ordinal = len(self._partitions)
            final_path = (
                self.session_directory
                / PARTITION_DATA_TEMPLATE.format(ordinal=ordinal)
            )
            partial_path.replace(final_path)
            _fsync_directory(self.session_directory)
            size_bytes = final_path.stat().st_size
            if size_bytes != self._partition_bytes:
                raise BrokerCaptureIntegrityError(
                    "capture partition byte count changed before publication"
                )
            artifact = ArtifactRef(
                kind=BROKER_CAPTURE_DATA_ARTIFACT_KIND,
                path=str(final_path.relative_to(self.root)).replace(
                    os.sep, "/"
                ),
                size_bytes=size_bytes,
                sha256=_file_sha256(final_path),
                metadata={
                    "encoding": "utf-8",
                    "format": "canonical-json-lines",
                    "ordering": "capture_sequence",
                },
            )
            partition = BrokerCapturePartitionManifestV1(
                session_id=self.session.session_id,
                policy_id=self.storage_policy.policy_id,
                partition_ordinal=ordinal,
                data_artifact=artifact,
                event_count=self._partition_event_count,
                first_capture_sequence=first.capture_sequence,
                last_capture_sequence=last.capture_sequence,
                first_receive_time_utc_ns=first.receive_time_utc_ns,
                last_receive_time_utc_ns=last.receive_time_utc_ns,
                first_receive_time_monotonic_ns=(
                    first.receive_time_monotonic_ns
                ),
                last_receive_time_monotonic_ns=(last.receive_time_monotonic_ns),
                event_kind_counts=dict(self._partition_kind_counts),
            )
            partition_manifest_path = (
                self.session_directory
                / PARTITION_MANIFEST_TEMPLATE.format(ordinal=ordinal)
            )
            _atomic_write_text(
                partition_manifest_path, partition.to_json() + "\n"
            )
            self._partitions.append(partition)
            self._manifest = self._publish_session_manifest(
                BrokerCaptureSessionState.OPEN, limitations=()
            )
        except BrokerCaptureStorageError:
            raise
        except OSError as err:
            raise BrokerCaptureStorageError(
                "capture partition finalization failed"
            ) from err
        finally:
            if not file_handle.closed:
                file_handle.close()
            self._file = None
            self._partial_path = None
            self._partition_event_count = 0
            self._partition_bytes = 0
            self._partition_first_event = None
            self._partition_last_event = None
            self._partition_kind_counts = {}

    def _publish_session_manifest(
        self,
        state: BrokerCaptureSessionState,
        *,
        limitations: Sequence[str],
    ) -> BrokerCaptureSessionManifestV1:
        partitions = tuple(self._partitions)
        event_count = sum(item.event_count for item in partitions)
        counts: dict[str, int] = {}
        for item in partitions:
            for kind, count in item.event_kind_counts.items():
                counts[kind] = counts.get(kind, 0) + count
        manifest = BrokerCaptureSessionManifestV1(
            session=self.session,
            storage_policy=self.storage_policy,
            state=state,
            partitions=partitions,
            event_count=event_count,
            event_kind_counts=counts,
            first_capture_sequence=(
                partitions[0].first_capture_sequence if partitions else None
            ),
            last_capture_sequence=(
                partitions[-1].last_capture_sequence if partitions else None
            ),
            partial_artifact_count=0,
            limitations=tuple(limitations),
        )
        _atomic_write_text(
            self.session_directory / SESSION_MANIFEST_FILENAME,
            manifest.to_json() + "\n",
        )
        return manifest


@dataclass(frozen=True, slots=True)
class BrokerCaptureReplaySourceV1:
    """Verified event source over completed, advertised capture partitions."""

    root: Path
    manifest: BrokerCaptureSessionManifestV1

    def __init__(
        self,
        root: str | Path,
        manifest: BrokerCaptureSessionManifestV1,
    ) -> None:
        object.__setattr__(self, "root", Path(root))
        object.__setattr__(self, "manifest", manifest)

    @property
    def session_id(self) -> str:
        """Return the replayed capture session identity."""
        return self.manifest.session.session_id

    def iter_events(self) -> Iterable[BrokerCaptureEventV1]:
        """Verify hashes/counts/order and yield events partition by partition."""
        return self._event_iterator()

    def _event_iterator(self) -> Iterator[BrokerCaptureEventV1]:
        verify_broker_capture_partition_manifests(self.root, self.manifest)
        expected_sequence = self.manifest.first_capture_sequence
        total_count = 0
        combined_counts: dict[str, int] = {}
        for partition in self.manifest.partitions:
            path = _contained_artifact_path(
                self.root, partition.data_artifact.path
            )
            if not path.is_file():
                raise BrokerCaptureIntegrityError(
                    "advertised capture partition is missing"
                )
            if path.stat().st_size != partition.data_artifact.size_bytes:
                raise BrokerCaptureIntegrityError(
                    "capture partition size does not match manifest"
                )
            if _file_sha256(path) != partition.data_artifact.sha256:
                raise BrokerCaptureIntegrityError(
                    "capture partition hash does not match manifest"
                )
            partition_count = 0
            partition_counts: dict[str, int] = {}
            first_event: BrokerCaptureEventV1 | None = None
            last_event: BrokerCaptureEventV1 | None = None
            try:
                with path.open("rt", encoding="utf-8", newline="") as handle:
                    for line in handle:
                        if not line.endswith("\n") or not line.strip():
                            raise BrokerCaptureIntegrityError(
                                "capture partition contains a partial JSON line"
                            )
                        event = BrokerCaptureEventV1.from_json(line)
                        if event.session_id != self.session_id:
                            raise BrokerCaptureIntegrityError(
                                "capture partition contains another session"
                            )
                        if expected_sequence is None:
                            expected_sequence = event.capture_sequence
                        if event.capture_sequence != expected_sequence:
                            raise BrokerCaptureIntegrityError(
                                "capture replay sequence is not contiguous"
                            )
                        if (
                            last_event is not None
                            and event.receive_time_monotonic_ns
                            < last_event.receive_time_monotonic_ns
                        ):
                            raise BrokerCaptureIntegrityError(
                                "capture replay monotonic time moved backwards"
                            )
                        if first_event is None:
                            first_event = event
                        last_event = event
                        partition_count += 1
                        total_count += 1
                        partition_counts[event.kind.value] = (
                            partition_counts.get(event.kind.value, 0) + 1
                        )
                        combined_counts[event.kind.value] = (
                            combined_counts.get(event.kind.value, 0) + 1
                        )
                        expected_sequence += 1
                        yield event
            except UnicodeDecodeError as err:
                raise BrokerCaptureIntegrityError(
                    "capture partition is not valid UTF-8"
                ) from err
            if (
                partition_count != partition.event_count
                or partition_counts != partition.event_kind_counts
                or first_event is None
                or last_event is None
                or first_event.capture_sequence
                != partition.first_capture_sequence
                or last_event.capture_sequence
                != partition.last_capture_sequence
                or first_event.receive_time_utc_ns
                != partition.first_receive_time_utc_ns
                or last_event.receive_time_utc_ns
                != partition.last_receive_time_utc_ns
                or first_event.receive_time_monotonic_ns
                != partition.first_receive_time_monotonic_ns
                or last_event.receive_time_monotonic_ns
                != partition.last_receive_time_monotonic_ns
            ):
                raise BrokerCaptureIntegrityError(
                    "capture partition contents do not reconcile with manifest"
                )
        if (
            total_count != self.manifest.event_count
            or combined_counts != self.manifest.event_kind_counts
        ):
            raise BrokerCaptureIntegrityError(
                "capture replay does not reconcile with session manifest"
            )


def load_broker_capture_session_manifest(
    path: str | Path,
) -> BrokerCaptureSessionManifestV1:
    """Load and verify one published session manifest."""
    return BrokerCaptureSessionManifestV1.from_json(
        Path(path).read_text(encoding="utf-8")
    )


def discover_broker_capture_session_manifests(
    root: str | Path,
) -> tuple[BrokerCaptureSessionManifestV1, ...]:
    """Discover only atomically published session manifests."""
    capture_root = Path(root)
    manifests_list: list[BrokerCaptureSessionManifestV1] = []
    for path in sorted(capture_root.glob(f"*/{SESSION_MANIFEST_FILENAME}")):
        if not path.is_file():
            continue
        manifest = load_broker_capture_session_manifest(path)
        verify_broker_capture_partition_manifests(capture_root, manifest)
        manifests_list.append(manifest)
    manifests = tuple(manifests_list)
    return tuple(sorted(manifests, key=lambda item: item.session.session_id))


def verify_broker_capture_partition_manifests(
    root: str | Path,
    manifest: BrokerCaptureSessionManifestV1,
) -> BrokerCaptureSessionManifestV1:
    """Verify every advertised partition sidecar against the session catalog."""
    session_directory = Path(root) / manifest.session.session_id
    for partition in manifest.partitions:
        sidecar_path = session_directory / PARTITION_MANIFEST_TEMPLATE.format(
            ordinal=partition.partition_ordinal
        )
        if not sidecar_path.is_file():
            raise BrokerCaptureIntegrityError(
                "advertised capture partition manifest is missing"
            )
        try:
            restored = BrokerCapturePartitionManifestV1.from_json(
                sidecar_path.read_text(encoding="utf-8")
            )
        except (OSError, TypeError, ValueError) as err:
            raise BrokerCaptureIntegrityError(
                "capture partition manifest is invalid"
            ) from err
        if restored != partition:
            raise BrokerCaptureIntegrityError(
                "capture partition manifest does not match session catalog"
            )
    return manifest


def inspect_broker_capture_session(
    root: str | Path,
    session_id: str,
) -> BrokerCaptureSessionInspectionV1:
    """Detect partial and orphan artifacts without advertising them."""
    capture_root = Path(root)
    session_directory = capture_root / session_id
    manifest_path = session_directory / SESSION_MANIFEST_FILENAME
    manifest = (
        load_broker_capture_session_manifest(manifest_path)
        if manifest_path.is_file()
        else None
    )
    advertised_data = {
        Path(item.data_artifact.path).name
        for item in (() if manifest is None else manifest.partitions)
    }
    advertised_manifests = {
        PARTITION_MANIFEST_TEMPLATE.format(ordinal=item.partition_ordinal)
        for item in (() if manifest is None else manifest.partitions)
    }
    partial = tuple(
        path.name for path in sorted(session_directory.glob("*.partial"))
    )
    orphan_data = tuple(
        path.name
        for path in sorted(session_directory.glob("partition-*.jsonl"))
        if path.name not in advertised_data
    )
    orphan_manifests = tuple(
        path.name
        for path in sorted(session_directory.glob("partition-*.manifest.json"))
        if path.name not in advertised_manifests
    )
    return BrokerCaptureSessionInspectionV1(
        session_directory=str(session_directory),
        advertised_partition_count=(
            0 if manifest is None else len(manifest.partitions)
        ),
        partial_artifacts=partial,
        orphan_data_artifacts=orphan_data,
        orphan_manifest_artifacts=orphan_manifests,
    )


def replay_broker_capture_session(
    root: str | Path,
    manifest: BrokerCaptureSessionManifestV1,
    *,
    consumers: Sequence[BrokerCaptureEventConsumerV1] = (),
) -> BrokerCaptureReplaySummaryV1:
    """Replay verified evidence through the live-compatible consumer seam."""
    digest_consumer = _LogicalDigestConsumer()
    source: BrokerCaptureEventSourceV1 = BrokerCaptureReplaySourceV1(
        root, manifest
    )
    result = consume_broker_capture_source(
        source,
        consumers=(digest_consumer, *consumers),
    )
    return BrokerCaptureReplaySummaryV1(
        session_id=result.session_id,
        manifest_id=manifest.manifest_id,
        partition_count=len(manifest.partitions),
        event_count=result.event_count,
        event_kind_counts=result.event_kind_counts,
        first_capture_sequence=result.first_capture_sequence,
        last_capture_sequence=result.last_capture_sequence,
        logical_content_sha256=digest_consumer.hexdigest(),
    )


class _LogicalDigestConsumer:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()

    def on_event(self, event: BrokerCaptureEventV1) -> None:
        self._digest.update(event.to_json().encode("utf-8"))
        self._digest.update(b"\n")

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    partial_path = path.with_name(path.name + ".partial")
    try:
        with partial_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        partial_path.replace(path)
        _fsync_directory(path.parent)
    except OSError as err:
        raise BrokerCaptureStorageError(
            "atomic capture manifest publication failed"
        ) from err


def _contained_artifact_path(root: Path, relative_path: str) -> Path:
    root_resolved = root.resolve()
    candidate = (root / relative_path).resolve()
    if not candidate.is_relative_to(root_resolved):
        raise BrokerCaptureIntegrityError(
            "capture artifact escaped the configured root"
        )
    return candidate


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _directory_size(path: Path) -> int:
    return sum(
        item.stat().st_size for item in path.rglob("*") if item.is_file()
    )


def _fsync_directory(path: Path) -> None:
    flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


__all__ = [
    "AppendOnlyBrokerCaptureWriterV1",
    "BrokerCaptureBackpressureError",
    "BrokerCaptureExistingSessionError",
    "BrokerCaptureIntegrityError",
    "BrokerCaptureQuotaError",
    "BrokerCaptureReplaySourceV1",
    "BrokerCaptureRetentionError",
    "BrokerCaptureSessionInspectionV1",
    "BrokerCaptureStorageError",
    "discover_broker_capture_session_manifests",
    "inspect_broker_capture_session",
    "load_broker_capture_session_manifest",
    "replay_broker_capture_session",
    "verify_broker_capture_partition_manifests",
]

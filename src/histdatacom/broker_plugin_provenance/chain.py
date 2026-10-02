"""Reference sequential chain; an unanchored root cannot authenticate itself."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

from .contracts import (
    BrokerProvenanceCheckpointV1,
    BrokerProvenanceEntryKind,
    BrokerProvenanceEntryV1,
    BrokerProvenanceHeaderV1,
    BrokerProvenanceLinkV1,
    BrokerProvenanceNativeFamily,
    BrokerProvenanceSealV1,
    BrokerProvenanceTerminalV1,
    BrokerProvenanceVerificationV1,
    payload_chunks,
)
from .contracts import (
    BrokerProvenanceVerificationReason as Reason,
)

EMPTY_CHECKPOINT_SHA256 = hashlib.sha256(b"").hexdigest()


def provenance_header_digest(header: BrokerProvenanceHeaderV1) -> str:
    if type(header) is not BrokerProvenanceHeaderV1:
        raise ValueError("exact provenance header required")
    return hashlib.sha256(header.to_json().encode("ascii")).hexdigest()


def make_provenance_entry(
    header: BrokerProvenanceHeaderV1,
    sequence: int,
    epoch: int,
    kind: BrokerProvenanceEntryKind,
    payload_json: str,
    *,
    native_record_id: str | None = None,
) -> BrokerProvenanceEntryV1:
    return BrokerProvenanceEntryV1(
        header.artifact_id,
        sequence,
        epoch,
        kind,
        native_record_id,
        payload_chunks(payload_json),
    )


def link_provenance_entry(
    previous_sha256: str, entry: BrokerProvenanceEntryV1
) -> BrokerProvenanceLinkV1:
    if type(entry) is not BrokerProvenanceEntryV1:
        raise ValueError("exact provenance entry required")
    from ._wire import digest

    digest(previous_sha256)
    value = hashlib.sha256(
        bytes.fromhex(previous_sha256) + entry.to_json().encode("ascii")
    ).hexdigest()
    return BrokerProvenanceLinkV1(entry, previous_sha256, value)


def checkpoint_digest(
    previous_sha256: str, checkpoint: BrokerProvenanceCheckpointV1
) -> str:
    from ._wire import digest

    digest(previous_sha256)
    if type(checkpoint) is not BrokerProvenanceCheckpointV1:
        raise ValueError("exact provenance checkpoint required")
    return hashlib.sha256(
        bytes.fromhex(previous_sha256) + checkpoint.to_json().encode("ascii")
    ).hexdigest()


class _ChainFailure(ValueError):
    def __init__(self, reason: Reason):
        super().__init__(reason.value)
        self.reason = reason


class _ChainState:
    """Only computed state; source-stream semantic checks belong to replay."""

    def __init__(self, header: BrokerProvenanceHeaderV1):
        self.header = BrokerProvenanceHeaderV1.from_json(header.to_json())
        self.header_id = self.header.artifact_id
        self.root = provenance_header_digest(self.header)
        self.count = 0
        self.max_epoch = -1
        self.native_epoch = -1
        self.terminal: BrokerProvenanceTerminalV1 | None = None
        self.checkpoint_count = 0
        self.checkpoint_root = EMPTY_CHECKPOINT_SHA256
        self.bytes = len(self.header.to_json())

    def accept(self, link: BrokerProvenanceLinkV1) -> bool:
        if type(link) is not BrokerProvenanceLinkV1:
            raise _ChainFailure(Reason.INVALID_ENTRY)
        try:
            text = link.to_json()
            link = BrokerProvenanceLinkV1.from_json(text)
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            raise _ChainFailure(Reason.INVALID_ENTRY) from exc
        entry = link.entry
        # from_json accepted these exact canonical bytes and detached all fields.
        size = len(text) + 1
        if (
            self.count >= self.header.policy.max_entries
            or size > self.header.policy.max_entry_bytes
            or self.bytes + size > self.header.policy.max_capture_bytes
        ):
            raise _ChainFailure(Reason.LIMIT)
        if entry.header_id != self.header_id:
            raise _ChainFailure(Reason.INVALID_HEADER)
        if entry.sequence != self.count:
            raise _ChainFailure(Reason.SEQUENCE)
        if self.terminal is not None:
            raise _ChainFailure(Reason.TERMINAL)
        if entry.epoch > self.max_epoch + 1:
            raise _ChainFailure(Reason.EPOCH)
        if entry.kind is BrokerProvenanceEntryKind.NATIVE_RECORD:
            if entry.epoch < self.native_epoch:
                raise _ChainFailure(Reason.EPOCH)
            self.native_epoch = entry.epoch
        max_epoch = max(self.max_epoch, entry.epoch)
        if entry.kind is BrokerProvenanceEntryKind.TERMINAL:
            if entry.epoch != max(self.max_epoch, 0):
                raise _ChainFailure(Reason.EPOCH)
            try:
                terminal = BrokerProvenanceTerminalV1.from_json(
                    entry.payload_json
                )
            except (TypeError, ValueError) as exc:
                raise _ChainFailure(Reason.TERMINAL) from exc
            if (
                terminal.header_id != self.header_id
                or terminal.stopped_at_monotonic_ns
                < self.header.started_at_monotonic_ns
                or (
                    self.header.family
                    is BrokerProvenanceNativeFamily.LIFECYCLE_V1
                )
                != (terminal.permission_execution_id is not None)
            ):
                raise _ChainFailure(Reason.TERMINAL)
            self.terminal = terminal
        if (
            link.previous_sha256 != self.root
            or hashlib.sha256(
                bytes.fromhex(self.root) + entry.to_json().encode("ascii")
            ).hexdigest()
            != link.sha256
        ):
            raise _ChainFailure(Reason.CHAIN_DIGEST)
        self.root = link.sha256
        self.count += 1
        self.max_epoch = max_epoch
        self.bytes += size
        return (
            self.count % self.header.policy.checkpoint_interval == 0
            or self.terminal is not None
        )

    def expected_checkpoint(self) -> BrokerProvenanceCheckpointV1:
        return BrokerProvenanceCheckpointV1(
            self.header_id, self.count, self.root, self.max_epoch
        )

    def accept_checkpoint(
        self, checkpoint: BrokerProvenanceCheckpointV1
    ) -> None:
        if type(checkpoint) is not BrokerProvenanceCheckpointV1:
            raise _ChainFailure(Reason.CHECKPOINT)
        try:
            checkpoint = BrokerProvenanceCheckpointV1.from_json(
                checkpoint.to_json()
            )
        except (TypeError, ValueError) as exc:
            raise _ChainFailure(Reason.CHECKPOINT) from exc
        if checkpoint != self.expected_checkpoint():
            raise _ChainFailure(Reason.CHECKPOINT)
        size = len(checkpoint.to_json()) + 1
        if (
            self.checkpoint_count >= self.header.policy.max_checkpoints
            or self.bytes + size > self.header.policy.max_capture_bytes
        ):
            raise _ChainFailure(Reason.LIMIT)
        self.checkpoint_root = checkpoint_digest(
            self.checkpoint_root, checkpoint
        )
        self.checkpoint_count += 1
        self.bytes += size

    def make_seal(self) -> BrokerProvenanceSealV1:
        if self.terminal is None:
            raise _ChainFailure(Reason.TERMINAL)
        return BrokerProvenanceSealV1(
            self.header_id,
            self.terminal,
            self.root,
            self.count,
            self.checkpoint_count,
            self.checkpoint_root,
            self.max_epoch,
        )


def verify_provenance_chain(
    header: BrokerProvenanceHeaderV1,
    links: Iterable[BrokerProvenanceLinkV1],
    checkpoints: Iterable[BrokerProvenanceCheckpointV1],
    seal: BrokerProvenanceSealV1 | None,
    *,
    expected_header_id: str | None = None,
    expected_seal_id: str | None = None,
) -> BrokerProvenanceVerificationV1:
    """Replay bounded ordered evidence; no file, credential, or network access.

    Both externally expected identities are necessary for VERIFIED. A caller
    that takes an expected ID from the same untrusted bundle only demonstrates
    self-consistency, not independent identity or authorization.
    """
    if type(header) is not BrokerProvenanceHeaderV1:
        raise ValueError("exact provenance header required")
    try:
        state = _ChainState(header)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid provenance header") from exc

    def result(reason: Reason) -> BrokerProvenanceVerificationV1:
        return BrokerProvenanceVerificationV1(
            state.header,
            seal,
            reason,
            state.root,
            state.count,
            state.checkpoint_count,
        )

    if expected_header_id is not None and expected_header_id != state.header_id:
        return result(Reason.ANCHOR_MISMATCH)
    checkpoint_iter = iter(checkpoints)
    try:
        for link in links:
            if state.accept(link):
                try:
                    checkpoint = next(checkpoint_iter)
                except StopIteration as exc:
                    raise _ChainFailure(Reason.CHECKPOINT) from exc
                state.accept_checkpoint(checkpoint)
        if next(checkpoint_iter, None) is not None:
            raise _ChainFailure(Reason.CHECKPOINT)
        if seal is None:
            return result(Reason.PARTIAL)
        if type(seal) is not BrokerProvenanceSealV1:
            return result(Reason.SEAL)
        try:
            seal = BrokerProvenanceSealV1.from_json(seal.to_json())
        except (TypeError, ValueError):
            return result(Reason.SEAL)
        if state.terminal is None or seal != state.make_seal():
            return result(Reason.SEAL)
        if (
            state.bytes + len(seal.to_json())
            > state.header.policy.max_capture_bytes
        ):
            return result(Reason.LIMIT)
        if (
            expected_seal_id is not None
            and expected_seal_id != seal.artifact_id
        ):
            return result(Reason.ANCHOR_MISMATCH)
        if not (
            state.terminal.native_complete
            and state.terminal.observations_complete
        ):
            return result(Reason.PARTIAL)
        return result(
            Reason.VERIFIED
            if expected_header_id is not None and expected_seal_id is not None
            else Reason.UNANCHORED
        )
    except _ChainFailure as exc:
        return result(exc.reason)
    except (TypeError, ValueError, OverflowError, RecursionError):
        return result(Reason.INVALID_ENTRY)

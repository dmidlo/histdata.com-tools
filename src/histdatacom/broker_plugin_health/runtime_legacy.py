"""Actual legacy adapter ingress and durable-write health qualification.

The native V1 wire is unchanged. The sibling journal records host observations
as they happen; a pre-existing final manifest cannot recreate those observations.
This synchronous source has no transport queue: its observed queue is empty.
Buffered native writes are measured by ingress-to-fsync residence, not relabelled
as an independently observed asynchronous queue.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from histdatacom.broker_capture.adapters import (
    BrokerCaptureAdapterV1,
    BrokerCaptureClockV1,
    LiveBrokerCaptureSourceV1,
    SystemBrokerCaptureClockV1,
    consume_broker_capture_source,
)
from histdatacom.broker_capture.contracts import (
    BrokerAdapterMessageV1,
    BrokerCaptureEventKind,
    BrokerCaptureEventV1,
    BrokerCapturePartitionManifestV1,
    BrokerCaptureSessionManifestV1,
    BrokerCaptureSessionV1,
    BrokerCaptureStoragePolicyV1,
)
from histdatacom.broker_capture.storage import (
    PARTITION_DATA_TEMPLATE,
    PARTITION_MANIFEST_TEMPLATE,
    SESSION_MANIFEST_FILENAME,
    AppendOnlyBrokerCaptureWriterV1,
    BrokerCaptureReplaySourceV1,
    inspect_broker_capture_session,
)
from histdatacom.broker_plugin_policy.bindings import (
    BrokerLegacyCaptureV1,
    BrokerLegacyRecordV1,
    BrokerProviderOutputContractV1,
)
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyDecisionV1,
    BrokerPolicyOperation,
)
from histdatacom.broker_plugin_policy.health_bindings import (
    BrokerHostHealthEvidenceV1,
)
from histdatacom.broker_plugin_policy.scope import require_provider_operation

from ._wire import canonical_health_json
from .collector import HostHealthRecorder
from .contracts import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthHeaderV1,
    BrokerHostHealthNativeFamily,
    BrokerHostHealthPolicyV1,
    BrokerHostHealthState,
    BrokerHostHealthUnavailableV1,
)
from .contracts import (
    BrokerHostHealthReason as Reason,
)
from .replay import (
    MAX_NATIVE_HEALTH_BYTES,
    historical_host_health_status,
    make_legacy_health_header,
    replay_legacy_host_health,
)
from .storage import HostHealthEvidenceWriter, _observations, _read

_DEFAULT_HEALTH_POLICY = BrokerHostHealthPolicyV1()


class BrokerHostHealthAdmissionError(ValueError):
    """Missing or unqualified host evidence cannot admit a new scientific fit."""


class LegacyCaptureHealthObserver:
    """Shared host-owned adapter/writer association, not plugin claims.

    Each native record is bound while the actual source produces it, then
    acknowledged once by the actual writer after fsync. Replaying old records
    through a writer alone cannot manufacture an ingress association.
    """

    def __init__(self, recorder: HostHealthRecorder) -> None:
        if type(recorder) is not HostHealthRecorder:
            raise ValueError("exact host health recorder required")
        if (
            recorder.header.family
            is not BrokerHostHealthNativeFamily.LEGACY_CAPTURE_V1
        ):
            raise ValueError("legacy host health header required")
        if recorder.header.queue_capacity != 1:
            raise ValueError(
                "synchronous legacy transport queue capacity is one"
            )
        self.recorder = recorder
        self.epoch = 0
        self._native_epoch = 0
        self._ingress: dict[int, tuple[str | None, int]] = {}
        self._bound: dict[str, tuple[BrokerCaptureEventV1, int | None, int]] = (
            {}
        )
        self._next_record = 0
        self._started = False

    def require_session(self, session: BrokerCaptureSessionV1) -> None:
        if (
            type(self) is not LegacyCaptureHealthObserver
            or type(session) is not BrokerCaptureSessionV1
        ):
            raise ValueError("exact host observer and native session required")
        header = self.recorder.header
        if (
            header.capture_id != session.session_id
            or header.plugin_id != session.adapter_id
            or header.provider_id != session.adapter_id
            or header.configuration_id != session.adapter_config_sha256
            or header.started_at_utc_ns != session.started_at_utc_ns
            or header.started_at_monotonic_ns != session.started_at_monotonic_ns
        ):
            raise ValueError("legacy observer session binding differs")

    def start(self) -> None:
        if self._started:
            raise ValueError("legacy observer already started")
        self.recorder.queue(0, self.epoch)
        self._started = True

    def ingress(self, message: object, sample: tuple[int, int]) -> int:
        if not self._started:
            raise ValueError("legacy health observer was not started")
        malformed = (
            type(message) is not BrokerAdapterMessageV1
            or message.kind is BrokerCaptureEventKind.CLOCK_CORRECTION
        )
        event_id = None
        if not malformed:
            assert isinstance(message, BrokerAdapterMessageV1)
            # Revalidate the closed native contract before extracting identity.
            message = BrokerAdapterMessageV1.from_json(message.to_json())
            event_id = message.message_id
            if message.kind in (
                BrokerCaptureEventKind.RECONNECT,
                BrokerCaptureEventKind.PROCESS_RESTART,
            ):
                self.epoch += 1
        ingress = self.recorder.ingress(
            event_id, self.epoch, malformed=malformed, sample=sample
        )
        self._ingress[ingress] = (event_id, self.epoch)
        if malformed:
            self.refuse_ingress(ingress, Reason.MALFORMED)
            raise ValueError(
                "legacy adapter yielded malformed or host-only message"
            )
        return ingress

    def bind(self, event: BrokerCaptureEventV1, ingress: int | None) -> None:
        if type(event) is not BrokerCaptureEventV1:
            raise ValueError("exact native capture event required")
        if (
            event.session_id != self.recorder.header.capture_id
            or event.capture_sequence != self._next_record
            or event.event_id in self._bound
        ):
            raise ValueError(
                "native health association order or session differs"
            )
        if event.kind is BrokerCaptureEventKind.CLOCK_CORRECTION:
            if ingress is not None:
                raise ValueError("host clock control has no adapter ingress")
        elif ingress is None or self._ingress.get(ingress) != (
            event.message.message_id,
            self.epoch,
        ):
            raise ValueError(
                "native record lacks exact observed adapter ingress"
            )
        if event.kind in (
            BrokerCaptureEventKind.RECONNECT,
            BrokerCaptureEventKind.PROCESS_RESTART,
        ):
            self._native_epoch += 1
            # This synchronous source has no pending transport queue. Observe
            # that actual empty state at the new native epoch boundary, after
            # ingress's reused clock sample, rather than carrying epoch zero's
            # queue measurement across reconnects during later replay.
            self.recorder.queue(0, self._native_epoch)
        if ingress is not None and self._native_epoch != self.epoch:
            raise ValueError("legacy ingress/native epoch differs")
        self._bound[event.event_id] = (event, ingress, self._native_epoch)
        self._next_record += 1

    def require_bound(self, event: BrokerCaptureEventV1) -> None:
        if type(event) is not BrokerCaptureEventV1:
            raise ValueError(
                "exact native event required for durable observation"
            )
        bound = self._bound.get(event.event_id)
        if bound is None or bound[0].to_json() != event.to_json():
            raise ValueError(
                "native write has no actual adapter ingress association"
            )

    def persisted(self, event: BrokerCaptureEventV1) -> None:
        self.require_bound(event)
        _, ingress, epoch = self._bound[event.event_id]
        self.recorder.persisted(
            event.event_id,
            None if ingress is None else event.message.message_id,
            epoch,
            ingress,
        )
        del self._bound[event.event_id]
        if ingress is not None:
            del self._ingress[ingress]

    def refuse_ingress(
        self, ingress: int, reason: Reason = Reason.PROVIDER_POLICY
    ) -> None:
        binding = self._ingress.get(ingress)
        if binding is not None:
            event_id, epoch = binding
            self.recorder.refused(ingress, event_id, epoch, reason)
            del self._ingress[ingress]

    def refuse_event(self, event: object) -> None:
        if type(event) is BrokerCaptureEventV1:
            bound = self._bound.get(event.event_id)
            if bound is not None and bound[1] is not None:
                self.refuse_ingress(bound[1], Reason.PERSISTENCE)


@dataclass(frozen=True, slots=True)
class BrokerLegacyHostHealthCaptureResultV1:
    manifest: BrokerCaptureSessionManifestV1
    audit: BrokerHostHealthAuditV1
    health_directory: Path


def _request_json(request: BrokerLegacyCaptureV1) -> str:
    if (
        type(request) is not BrokerLegacyCaptureV1
        or type(request.session) is not BrokerCaptureSessionV1
        or type(request.output_contract) is not BrokerProviderOutputContractV1
        or request.output_contract.schema_family != "legacy-capture-v1"
    ):
        raise ValueError("exact native legacy invocation required")
    session = BrokerCaptureSessionV1.from_json(request.session.to_json())
    output = BrokerProviderOutputContractV1.from_json(
        request.output_contract.to_json()
    )
    return canonical_health_json(
        {
            "schema_version": "histdatacom.broker-provider-legacy-invocation.v1",
            "session": session.to_dict(),
            "output_contract": output.to_dict(),
        }
    )


def legacy_host_health_directory(
    root: str | Path, session: BrokerCaptureSessionV1
) -> Path:
    return Path(root) / (session.session_id + "-host-health")


def capture_legacy_with_host_health(
    root: str | Path,
    *,
    provider_request: BrokerLegacyCaptureV1,
    adapter: BrokerCaptureAdapterV1,
    storage_policy: BrokerCaptureStoragePolicyV1,
    symbols: tuple[str, ...],
    clock: BrokerCaptureClockV1 | None = None,
    policy: BrokerHostHealthPolicyV1 = _DEFAULT_HEALTH_POLICY,
    clock_correction_threshold_ns: int = 5_000_000,
) -> BrokerLegacyHostHealthCaptureResultV1:
    """Capture actual adapter calls plus durable native/observation journals.

    Call within an explicit current provider-policy scope. Tests supply generated
    adapters and clocks; no provider selection, login or campaign is implied.
    The same host clock is sampled for ingress and persistence, and its ingress
    sample is also used by the native receive timestamp.
    """
    invocation_json = _request_json(provider_request)
    decision = require_provider_operation(
        provider_request, BrokerPolicyOperation.CAPTURE
    )
    require_provider_operation(
        provider_request, BrokerPolicyOperation.RETAIN_LOCAL
    )
    session = provider_request.session
    header = make_legacy_health_header(
        session,
        decision,
        provider_request=provider_request,
        symbols=symbols,
        queue_capacity=1,
        policy=policy,
    )

    def authorize(artifact: object) -> None:
        # The journal also retains the exact native session and invocation,
        # including any declared opaque/private metadata. Aggregate HEALTH
        # classification alone must not authorize these retained source bytes.
        require_provider_operation(
            provider_request, BrokerPolicyOperation.RETAIN_LOCAL
        )
        require_provider_operation(
            BrokerHostHealthEvidenceV1(
                provider_request, session, header, artifact
            ),
            BrokerPolicyOperation.RETAIN_LOCAL,
        )

    authorize(header)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    directory = legacy_host_health_directory(root, session)
    journal = HostHealthEvidenceWriter(
        directory,
        header,
        {
            "invocation.json": invocation_json,
            "native-header.json": session.to_json(),
            "provider-decision.json": decision.to_json(),
        },
        authorize_artifact=authorize,
    )
    host_clock = clock or SystemBrokerCaptureClockV1()
    recorder = HostHealthRecorder(header, host_clock.sample, journal.append)
    observer = LegacyCaptureHealthObserver(recorder)
    writer: AppendOnlyBrokerCaptureWriterV1 | None = None
    try:
        source = LiveBrokerCaptureSourceV1(
            session,
            adapter,
            host_clock,
            clock_correction_threshold_ns,
            provider_request=provider_request,
            health_observer=observer,
        )
        writer = AppendOnlyBrokerCaptureWriterV1(
            root,
            session=session,
            storage_policy=storage_policy,
            provider_request=provider_request,
            health_observer=observer,
        )
        observer.start()
        consume_broker_capture_source(
            source, provider_request=provider_request, sink=writer
        )
        manifest = writer.close()
        recorder.close(observer.epoch)
        native = BrokerCaptureReplaySourceV1(
            root, manifest, provider_request=provider_request
        )
        audit = replay_legacy_host_health(
            header,
            recorder.observations,
            manifest,
            native.iter_events(),
            decision,
            provider_request=provider_request,
        )
        journal.finish(audit)
        return BrokerLegacyHostHealthCaptureResultV1(manifest, audit, directory)
    finally:
        # No health audit is fabricated on an interrupted path. Native cleanup
        # retains its existing FAILED/OPEN semantics, including partial bytes.
        try:
            if writer is not None:
                writer.close(
                    completed=False,
                    limitations=("host_health_capture_interrupted",),
                )
        finally:
            journal.close()


def read_legacy_host_health(
    root: str | Path,
    manifest: BrokerCaptureSessionManifestV1,
    *,
    provider_request: BrokerLegacyCaptureV1,
) -> BrokerHostHealthAuditV1 | BrokerHostHealthUnavailableV1:
    """Independently replay exact native bytes, authority binding and journal."""
    invocation_json = _request_json(provider_request)
    if provider_request.session.to_json() != manifest.session.to_json():
        raise ValueError("legacy health invocation session substitution")
    require_provider_operation(
        provider_request, BrokerPolicyOperation.MATERIAL_USE
    )
    directory = legacy_host_health_directory(root, manifest.session)
    if not directory.exists() and not directory.is_symlink():
        return historical_host_health_status(manifest)
    if directory.is_symlink():
        raise ValueError("legacy health directory may not be a symlink")
    directory = directory.resolve(strict=True)
    expected = {
        "header.json",
        "invocation.json",
        "native-header.json",
        "provider-decision.json",
        "observations.jsonl",
        "audit.json",
    }
    names: set[str] = set()
    with os.scandir(directory) as entries:
        for entry in entries:
            if len(names) >= len(expected) or not entry.is_file(
                follow_symlinks=False
            ):
                raise ValueError(
                    "legacy host health foreign or interrupted inventory"
                )
            names.add(entry.name)
    if names != expected:
        raise ValueError("legacy host health evidence incomplete")
    header = BrokerHostHealthHeaderV1.from_json(
        _read(directory / "header.json")
    )
    require_provider_operation(
        BrokerHostHealthEvidenceV1(
            provider_request, manifest.session, header, header
        ),
        BrokerPolicyOperation.MATERIAL_USE,
    )
    if (
        _read(directory / "invocation.json") != invocation_json
        or _read(directory / "native-header.json") != manifest.session.to_json()
    ):
        raise ValueError("legacy health native binding substitution")
    native_directory = Path(root) / manifest.session.session_id
    if native_directory.is_symlink():
        raise ValueError("legacy native directory may not be a symlink")
    native_expected = {
        SESSION_MANIFEST_FILENAME,
        SESSION_MANIFEST_FILENAME + ".provider-policy.json",
    }
    for partition in manifest.partitions:
        data_name = PARTITION_DATA_TEMPLATE.format(
            ordinal=partition.partition_ordinal
        )
        sidecar_name = PARTITION_MANIFEST_TEMPLATE.format(
            ordinal=partition.partition_ordinal
        )
        if (
            partition.data_artifact.path
            != manifest.session.session_id + "/" + data_name
        ):
            raise ValueError("legacy health native partition path differs")
        native_expected.update((data_name, sidecar_name))
    native_names: set[str] = set()
    with os.scandir(native_directory) as entries:
        for entry in entries:
            if len(native_names) >= len(native_expected) or not entry.is_file(
                follow_symlinks=False
            ):
                raise ValueError(
                    "legacy native health inventory contains foreign or linked files"
                )
            native_names.add(entry.name)
    if native_names != native_expected:
        raise ValueError("legacy native health inventory incomplete")
    retained = BrokerCaptureSessionManifestV1.from_json(
        _read(native_directory / SESSION_MANIFEST_FILENAME)
    )
    if (
        retained.to_json() != manifest.to_json()
        or not inspect_broker_capture_session(
            root, manifest.session.session_id
        ).clean
    ):
        raise ValueError("legacy health native inventory changed or incomplete")
    actual = replay_legacy_host_health(
        header,
        _observations(directory, header.policy.max_observations),
        manifest,
        _read_native_records(native_directory, manifest, provider_request),
        BrokerPolicyDecisionV1.from_json(
            _read(directory / "provider-decision.json")
        ),
        provider_request=provider_request,
    )
    claimed = BrokerHostHealthAuditV1.from_json(_read(directory / "audit.json"))
    if claimed.to_json() != actual.to_json():
        raise ValueError("legacy health audit differs from independent replay")
    # Observation replay follows native reads and can outlast the initial
    # ledger decision. Recheck actual source and aggregate classes at release.
    require_provider_operation(
        provider_request, BrokerPolicyOperation.MATERIAL_USE
    )
    require_provider_operation(
        BrokerHostHealthEvidenceV1(
            provider_request, manifest.session, header, actual
        ),
        BrokerPolicyOperation.MATERIAL_USE,
    )
    return actual


def _read_native_records(
    directory: Path,
    manifest: BrokerCaptureSessionManifestV1,
    request: BrokerLegacyCaptureV1,
) -> Iterator[BrokerCaptureEventV1]:
    """No-follow native leaves; the pure reducer verifies all partition bytes."""
    from histdatacom.broker_plugin_policy.storage import (
        read_broker_policy_receipt,
        verify_broker_policy_receipt,
    )

    native_path = directory / SESSION_MANIFEST_FILENAME
    verify_broker_policy_receipt(
        read_broker_policy_receipt(
            directory / (SESSION_MANIFEST_FILENAME + ".provider-policy.json")
        ),
        BrokerLegacyRecordV1(
            manifest.session, manifest, request.output_contract
        ),
        native_path,
    )
    size = 0
    for partition in manifest.partitions:
        require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
        sidecar = BrokerCapturePartitionManifestV1.from_json(
            _read(
                directory
                / PARTITION_MANIFEST_TEMPLATE.format(
                    ordinal=partition.partition_ordinal
                ),
            )
        )
        if sidecar.to_json() != partition.to_json():
            raise ValueError("legacy health native partition sidecar differs")
        text = _read(
            directory
            / PARTITION_DATA_TEMPLATE.format(
                ordinal=partition.partition_ordinal
            ),
            MAX_NATIVE_HEALTH_BYTES - size,
        )
        size += len(text)
        for line in text.splitlines(keepends=True):
            if not line.endswith("\n"):
                raise ValueError(
                    "legacy health native partition has interrupted tail"
                )
            event = BrokerCaptureEventV1.from_json(line)
            require_provider_operation(
                BrokerLegacyRecordV1(
                    manifest.session, event, request.output_contract
                ),
                BrokerPolicyOperation.MATERIAL_USE,
            )
            yield event


def require_legacy_capture_health(
    root: str | Path,
    manifest: BrokerCaptureSessionManifestV1,
    *,
    provider_request: BrokerLegacyCaptureV1,
) -> BrokerHostHealthAuditV1:
    """Mandatory admission for new fits; never a reusable provider permit."""
    result = read_legacy_host_health(
        root, manifest, provider_request=provider_request
    )
    if (
        type(result) is not BrokerHostHealthAuditV1
        or result.state is not BrokerHostHealthState.QUALIFIED
        or not result.complete_observations
    ):
        raise BrokerHostHealthAdmissionError(
            "native capture lacks qualified complete host health: "
            + result.state.value
        )
    return result

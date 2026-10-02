"""Current-rights, exact-native replay of host provenance evidence.

No broker activation, credential lookup or network access is performed here.
Retained authority snapshots are checked as historical evidence, never installed
as a current resource or provider permission. Expected seals are external anchors.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from .native_common import legacy_invocation_json, native_sha256, parse_ingress

if TYPE_CHECKING:
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureSessionManifestV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerLegacyCaptureV1,
        BrokerSDKInvocationV1,
    )

    from .contracts import (
        BrokerProvenanceSealV1,
        BrokerProvenanceVerificationV1,
    )


def _next_exact(iterator: Iterator[Any], text: str, label: str) -> Any:
    try:
        value = next(iterator)
    except StopIteration:
        raise ValueError("provenance inserts a " + label) from None
    if value.to_json() != text:
        raise ValueError("provenance reorders or substitutes a " + label)
    return value


def _exhausted(iterator: Iterator[Any], label: str) -> None:
    try:
        next(iterator)
    except StopIteration:
        return
    raise ValueError("provenance deletes a " + label)


def _replay_native(
    directory: Path,
    manifest: Any,
    invocation: Any,
    audit: Any,
    records: Iterable[Any],
    observations: Iterable[Any],
    *,
    expected_root: BrokerProvenanceSealV1 | None,
    permission_execution: Any = None,
) -> BrokerProvenanceVerificationV1:
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthObservationKind as ObservationKind,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyDecisionV1,
        BrokerPolicyOperation,
    )
    from histdatacom.broker_plugin_policy.provenance_bindings import (
        BrokerProvenanceEvidenceV1,
    )
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )

    from .contracts import (
        BrokerProvenanceEntryKind as Kind,
    )
    from .contracts import (
        BrokerProvenanceEntryV1,
        BrokerProvenanceHeaderV1,
        BrokerProvenanceLinkV1,
        BrokerProvenanceSealV1,
    )
    from .contracts import (
        BrokerProvenanceVerificationReason as Reason,
    )
    from .runtime import make_native_provenance_header
    from .storage import _read_provenance

    sdk = type(invocation) is BrokerSDKInvocationV1
    native_header = manifest.header if sdk else manifest.session
    native_header_id = (
        native_header.artifact_id if sdk else native_header.session_id
    )
    require_provider_operation(invocation, BrokerPolicyOperation.MATERIAL_USE)
    if expected_root is not None:
        if type(expected_root) is not BrokerProvenanceSealV1:
            raise ValueError("exact expected provenance seal required")
        expected_root = BrokerProvenanceSealV1.from_json(
            expected_root.to_json()
        )
    state: dict[str, Any] = {"header": None, "session": None, "identity": None}

    def authorize(artifact: Any) -> None:
        if type(artifact) is BrokerProvenanceHeaderV1:
            if (
                artifact.native_header_id != native_header_id
                or artifact.native_header_sha256
                != native_sha256(native_header.to_json())
            ):
                raise ValueError("provenance native header substitution")
            if state["header"] is not None and artifact != state["header"]:
                raise ValueError("provenance header changed during replay")
            state["header"] = artifact
        if state["header"] is None:
            raise ValueError("provenance artifact precedes its header")
        entry = (
            artifact.entry
            if type(artifact) is BrokerProvenanceLinkV1
            else artifact if type(artifact) is BrokerProvenanceEntryV1 else None
        )
        if sdk and entry is not None and entry.kind is Kind.NATIVE_RECORD:
            from histdatacom.broker_plugin_lifecycle.contracts import (
                BrokerLifecycleIdentityV1,
                BrokerLifecycleRecordV1,
                BrokerLifecycleSessionV1,
            )

            record = BrokerLifecycleRecordV1.from_json(entry.payload_json)
            if record.kind == "identity":
                state["identity"] = BrokerLifecycleIdentityV1.from_json(
                    record.payload_json
                )
                state["session"] = None
            elif record.kind == "session":
                state["session"] = BrokerLifecycleSessionV1.from_json(
                    record.payload_json
                )
        require_provider_operation(
            invocation, BrokerPolicyOperation.MATERIAL_USE
        )
        require_provider_operation(
            BrokerProvenanceEvidenceV1(
                invocation,
                native_header,
                audit.header,
                state["header"],
                artifact,
                state["session"],
                state["identity"],
            ),
            BrokerPolicyOperation.MATERIAL_USE,
        )

    read = _read_provenance(
        directory,
        authorize_artifact=authorize,
        guard_text=lambda text: None,
        expected_header_id=(
            None if expected_root is None else expected_root.header_id
        ),
        expected_seal_id=(
            None if expected_root is None else expected_root.artifact_id
        ),
    )
    if read.verification.reason not in (
        Reason.VERIFIED,
        Reason.UNANCHORED,
        Reason.PARTIAL,
    ):
        raise ValueError(
            "native provenance chain verification failed: "
            + read.verification.reason.value
        )
    if read.partial_tail:
        raise ValueError(
            "interrupted provenance tail cannot verify a native capture"
        )
    if read.seal is None:
        raise ValueError("native provenance capture has no final seal")
    metadata = read.metadata
    names = {
        "invocation.json",
        "native-header.json",
        "health-header.json",
        "provider-decision.json",
        "environment.json",
    }
    if sdk:
        names |= {
            "permission-manifest.json",
            "permission-context.json",
            "permission-decision.json",
        }
    if set(metadata) != names or (
        metadata["invocation.json"]
        != (invocation.to_json() if sdk else legacy_invocation_json(invocation))
        or metadata["native-header.json"] != native_header.to_json()
        or metadata["health-header.json"] != audit.header.to_json()
    ):
        raise ValueError("provenance native metadata inventory/substitution")
    permission_arguments: dict[str, Any] = {}
    if sdk:
        from histdatacom.broker_plugin_permissions.contracts import (
            BrokerPermissionContextV1,
            BrokerPermissionDecisionV1,
            BrokerPermissionManifestV1,
        )

        permission_arguments = {
            "permission_manifest": BrokerPermissionManifestV1.from_json(
                metadata["permission-manifest.json"]
            ),
            "permission_context": BrokerPermissionContextV1.from_json(
                metadata["permission-context.json"]
            ),
            "permission_decision": BrokerPermissionDecisionV1.from_json(
                metadata["permission-decision.json"]
            ),
        }
    expected_header = make_native_provenance_header(
        invocation,
        native_header,
        audit.header,
        BrokerPolicyDecisionV1.from_json(metadata["provider-decision.json"]),
        environment_json=metadata["environment.json"],
        **permission_arguments,
    )
    if expected_header != read.header:
        raise ValueError(
            "provenance header differs from exact native authority/software"
        )
    terminal = read.seal.terminal
    if (
        terminal.native_manifest_id
        != (manifest.artifact_id if sdk else manifest.manifest_id)
        or terminal.native_manifest_sha256 != native_sha256(manifest.to_json())
        or terminal.health_audit_id != audit.artifact_id
        or terminal.health_audit_sha256 != native_sha256(audit.to_json())
        or terminal.native_complete
        != (
            manifest.completion.value == "complete_local_finite_run"
            if sdk
            else manifest.complete
        )
        or terminal.observations_complete != audit.complete_observations
        or terminal.permission_execution_id
        != (
            None
            if permission_execution is None
            else permission_execution.artifact_id
        )
        or terminal.permission_execution_sha256
        != (
            None
            if permission_execution is None
            else native_sha256(permission_execution.to_json())
        )
    ):
        raise ValueError(
            "provenance terminal substitutes actual native/health/permission evidence"
        )

    native_iterator, observation_iterator = iter(records), iter(observations)
    pending_ingress: dict[int, Any] = {}
    admitted_ingress: dict[int, tuple[Any, int | None]] = {}
    pending_native: dict[str, Any] = {}
    persisted_deliveries: dict[tuple[int, int], str] = {}
    native_epoch = 0
    closed: Any = None
    identity: Any = None
    session: Any = None
    for link in read.links:
        entry = link.entry
        if entry.kind is Kind.HOST_OBSERVATION:
            observation = _next_exact(
                observation_iterator, entry.payload_json, "host observation"
            )
            if observation.kind is ObservationKind.INGRESS:
                pending_ingress[observation.ingress_sequence] = observation
            elif observation.kind is ObservationKind.PERSISTED:
                native_record = pending_native.pop(
                    observation.native_record_id, None
                )
                if native_record is None:
                    raise ValueError(
                        "durable observation precedes its exact native chain record"
                    )
                if observation.ingress_sequence is not None:
                    admitted_pair = admitted_ingress.get(
                        observation.ingress_sequence
                    )
                    if admitted_pair is None:
                        raise ValueError(
                            "durable native event has no admitted ingress chain record"
                        )
                    admitted, delivery = admitted_pair
                    if sdk:
                        if (
                            native_record.kind != "event"
                            or native_record.payload_json != admitted.to_json()
                            or native_record.delivery_sequence != delivery
                            or native_record.epoch != observation.epoch
                            or delivery is None
                        ):
                            raise ValueError(
                                "native SDK delivery differs from its actual admitted ingress"
                            )
                        persisted_deliveries[(observation.epoch, delivery)] = (
                            admitted.artifact_id
                        )
                    elif native_record.message.to_json() != admitted.to_json():
                        raise ValueError(
                            "native legacy event differs from its actual admitted ingress"
                        )
            elif observation.kind is ObservationKind.REFUSED and sdk:
                from histdatacom.broker_plugin_health.contracts import (
                    BrokerHostHealthReason,
                )

                if (
                    observation.reason
                    is BrokerHostHealthReason.DUPLICATE_DELIVERY
                ):
                    admitted_pair = admitted_ingress.get(
                        observation.ingress_sequence
                    )
                    if admitted_pair is None:
                        raise ValueError(
                            "duplicate delivery has no retained admitted ingress"
                        )
                    admitted, delivery = admitted_pair
                    if (
                        delivery is None
                        or persisted_deliveries.get(
                            (observation.epoch, delivery)
                        )
                        != admitted.artifact_id
                    ):
                        raise ValueError(
                            "duplicate delivery differs from the previously persisted input"
                        )
            elif observation.kind is ObservationKind.CLOSE:
                closed = observation
        elif entry.kind is Kind.ADMITTED_INGRESS:
            parsed_payload, sequence, delivery = parse_ingress(
                entry.payload_json, family=read.header.family.value
            )
            payload = cast(Any, parsed_payload)
            observed = pending_ingress.get(sequence)
            payload_id = payload.artifact_id if sdk else payload.message_id
            if (
                observed is None
                or sequence in admitted_ingress
                or observed.epoch != entry.epoch
                or observed.event_id != payload_id
            ):
                raise ValueError(
                    "provenance ingress lacks exact preceding host observation"
                )
            if sdk:
                if session is None or identity is None:
                    raise ValueError(
                        "provenance SDK ingress has no actual native session"
                    )
                atoms = set(
                    permission_arguments["permission_decision"].effective_atoms
                )
                needed = {
                    (
                        "emit:quotes"
                        if payload.event.quote is not None
                        else "emit:health"
                    )
                }
                quote = payload.event.quote
                if quote is not None and any(
                    item is not None
                    for item in (quote.bid_size, quote.ask_size, quote.activity)
                ):
                    needed.add("emit:sizes")
                if (
                    payload.event.raw_provenance is not None
                    or payload.event.extensions
                ):
                    needed.add("raw_payload:emit")
                if not needed <= atoms:
                    raise ValueError(
                        "provenance ingress exceeds recorded permission grant"
                    )
            elif delivery is not None:
                raise ValueError(
                    "legacy ingress invents transport delivery sequence"
                )
            admitted_ingress[sequence] = (payload, delivery)
        elif entry.kind is Kind.NATIVE_RECORD:
            record = _next_exact(
                native_iterator, entry.payload_json, "native record"
            )
            if entry.native_record_id is None:
                raise ValueError("native provenance record has no identity")
            if entry.native_record_id in pending_native:
                raise ValueError("duplicate pending native provenance record")
            pending_native[entry.native_record_id] = record
            if sdk:
                from histdatacom.broker_plugin_lifecycle.contracts import (
                    BrokerLifecycleIdentityV1,
                    BrokerLifecycleSessionV1,
                )

                if record.epoch != entry.epoch:
                    raise ValueError(
                        "provenance native connection epoch substitution"
                    )
                if record.kind == "identity":
                    identity = BrokerLifecycleIdentityV1.from_json(
                        record.payload_json
                    )
                    session = None
                elif record.kind == "session":
                    session = BrokerLifecycleSessionV1.from_json(
                        record.payload_json
                    )
            else:
                from histdatacom.broker_capture.contracts import (
                    BrokerCaptureEventKind,
                )

                if record.kind in (
                    BrokerCaptureEventKind.RECONNECT,
                    BrokerCaptureEventKind.PROCESS_RESTART,
                ):
                    native_epoch += 1
                if entry.epoch != native_epoch:
                    raise ValueError(
                        "provenance legacy connection epoch substitution"
                    )
    _exhausted(native_iterator, "native record")
    _exhausted(observation_iterator, "host observation")
    if terminal.observations_complete and pending_native:
        raise ValueError(
            "complete provenance omits native durable observations"
        )
    # Complete observations mean every parseable ingress was seen by the chain,
    # including exact transport duplicates. Failed/forbidden input remains a
    # health refusal; never invent a raw payload or hash for that missing input.
    for sequence, observed in pending_ingress.items():
        if (
            terminal.native_complete
            and observed.event_id is not None
            and sequence not in admitted_ingress
        ):
            raise ValueError("complete provenance omits admitted ingress")
    if closed is None or (
        terminal.stopped_at_utc_ns,
        terminal.stopped_at_monotonic_ns,
    ) != (closed.utc_ns, closed.monotonic_ns):
        raise ValueError("provenance stop clocks differ from actual host close")
    if (
        expected_root is not None
        and read.seal.to_json() != expected_root.to_json()
    ):
        raise ValueError(
            "provenance differs from externally retained expected root"
        )
    require_provider_operation(invocation, BrokerPolicyOperation.MATERIAL_USE)
    authorize(read.seal)
    return read.verification


def require_legacy_capture_provenance(
    root: str | Path,
    manifest: BrokerCaptureSessionManifestV1,
    *,
    provider_request: BrokerLegacyCaptureV1,
    expected_root: BrokerProvenanceSealV1 | None = None,
) -> BrokerProvenanceVerificationV1:
    """Replay actual legacy bytes and health; old unchained captures refuse."""
    from histdatacom.broker_capture.contracts import (
        BrokerCaptureSessionManifestV1,
    )
    from histdatacom.broker_capture.storage import BrokerCaptureReplaySourceV1
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthAuditV1,
    )
    from histdatacom.broker_plugin_health.runtime_legacy import (
        legacy_host_health_directory,
        read_legacy_host_health,
    )
    from histdatacom.broker_plugin_health.storage import _observations
    from histdatacom.broker_plugin_policy.bindings import BrokerLegacyCaptureV1

    if (
        type(manifest) is not BrokerCaptureSessionManifestV1
        or type(provider_request) is not BrokerLegacyCaptureV1
    ):
        raise ValueError("exact native legacy manifest/invocation required")
    manifest = BrokerCaptureSessionManifestV1.from_json(manifest.to_json())
    audit = read_legacy_host_health(
        root, manifest, provider_request=provider_request
    )
    if type(audit) is not BrokerHostHealthAuditV1:
        raise ValueError("capture has no actual host health evidence")
    result = _replay_native(
        Path(root) / (manifest.session.session_id + "-provenance"),
        manifest,
        provider_request,
        audit,
        BrokerCaptureReplaySourceV1(
            root, manifest, provider_request=provider_request
        ).iter_events(),
        _observations(
            legacy_host_health_directory(root, manifest.session),
            audit.header.policy.max_observations,
        ),
        expected_root=expected_root,
    )
    if (
        result.seal is None
        or not result.seal.terminal.native_complete
        or not result.seal.terminal.observations_complete
    ):
        raise ValueError(
            "incomplete native provenance cannot admit a new scientific fit"
        )
    return result


def read_lifecycle_capture_provenance(
    directory: Path,
    manifest: BrokerLifecycleManifestV1,
    *,
    provider_request: BrokerSDKInvocationV1,
    expected_root: BrokerProvenanceSealV1 | None = None,
) -> BrokerProvenanceVerificationV1:
    """Verify SDK integrity without claiming an SDK scientific fingerprint fit."""
    from histdatacom.broker_plugin_health.storage import (
        _observations,
        _read,
        read_lifecycle_host_health,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_lifecycle.storage import (
        replay_broker_lifecycle,
    )
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionExecutionV1,
        verify_permission_execution,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

    if (
        type(manifest) is not BrokerLifecycleManifestV1
        or type(provider_request) is not BrokerSDKInvocationV1
    ):
        raise ValueError("exact native lifecycle manifest/invocation required")
    manifest = BrokerLifecycleManifestV1.from_json(manifest.to_json())
    health_directory = directory.with_name(directory.name + "-host-health")
    audit = read_lifecycle_host_health(
        health_directory,
        manifest,
        replay_broker_lifecycle(directory, provider_request=provider_request),
        provider_request,
    )
    execution = BrokerPermissionExecutionV1.from_json(
        _read(health_directory / "permission-execution.json")
    )
    verify_permission_execution(execution, provider_request, manifest)
    return _replay_native(
        directory.with_name(directory.name + "-provenance"),
        manifest,
        provider_request,
        audit,
        replay_broker_lifecycle(directory, provider_request=provider_request),
        _observations(health_directory, audit.header.policy.max_observations),
        expected_root=expected_root,
        permission_execution=execution,
    )

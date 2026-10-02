"""Actual host provenance instrumentation; never reconstructs old ingress."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .native_common import (
    host_environment_json,
    ingress_json,
    legacy_invocation_json,
    native_sha256,
    parse_ingress,
    parse_native_json,
)


def make_native_provenance_header(
    invocation: Any,
    native_header: Any,
    health_header: Any,
    provider_decision: Any,
    *,
    environment_json: str,
    permission_manifest: Any = None,
    permission_context: Any = None,
    permission_decision: Any = None,
) -> Any:
    """Derive declared identities from exact actual native parents only."""
    from histdatacom.broker_plugin_health.contracts import (
        BrokerHostHealthHeaderV1,
    )
    from histdatacom.broker_plugin_health.replay import (
        make_legacy_health_header,
        make_lifecycle_health_header,
    )
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerLegacyCaptureV1,
        BrokerSDKInvocationV1,
    )

    from .contracts import (
        BrokerProvenanceConformanceStatus,
        BrokerProvenanceHeaderV1,
    )
    from .contracts import (
        BrokerProvenanceNativeFamily as Family,
    )

    if type(health_header) is not BrokerHostHealthHeaderV1:
        raise ValueError("exact native health header required")
    environment = parse_native_json(environment_json)
    # A retained runtime description is not substituted with the replay host's
    # environment: cross-machine offline verification must remain possible.
    expected_environment_keys = {
        "schema_version",
        "host_distribution",
        "host_version",
        "python_implementation",
        "python_version",
        "python_cache_tag",
        "python_compiler",
        "system",
        "release",
        "machine",
        "byteorder",
        "scope",
    }
    if (
        set(environment) != expected_environment_keys
        or environment["schema_version"]
        != "histdatacom.broker-provenance-environment.v1"
        or environment["host_distribution"] != "histdatacom"
        or environment["scope"]
        != "public_recording_runtime_not_full_dependency_sbom"
        or any(
            value is not None and (type(value) is not str or len(value) > 512)
            for value in environment.values()
        )
    ):
        raise ValueError("invalid retained public host environment")
    shared = {
        "native_header_sha256": native_sha256(native_header.to_json()),
        "provider_decision_id": provider_decision.artifact_id,
        "host_version": environment["host_version"],
        "host_python_version": environment["python_version"],
        "environment_id": "broker-provenance-environment:sha256:"
        + native_sha256(environment_json),
        "started_at_utc_ns": health_header.started_at_utc_ns,
        "started_at_monotonic_ns": health_header.started_at_monotonic_ns,
        "conformance_version": None,
        "conformance_status": BrokerProvenanceConformanceStatus.UNAVAILABLE,
        "conformance_receipt_id": None,
        # Neither native contract declares an independently authenticated feed
        # identity. The complete reviewed config/provider identity is retained.
        "feed_id": None,
    }
    if type(invocation) is BrokerSDKInvocationV1:
        if (
            permission_manifest is None
            or permission_context is None
            or permission_decision is None
        ):
            raise ValueError(
                "SDK provenance requires actual permission evidence"
            )
        rebuilt = make_lifecycle_health_header(
            native_header,
            invocation,
            permission_manifest,
            permission_context,
            permission_decision,
            provider_decision,
            started_at_utc_ns=health_header.started_at_utc_ns,
            started_at_monotonic_ns=health_header.started_at_monotonic_ns,
            policy=health_header.policy,
        )
        candidate = invocation.plan.candidate
        registration = candidate.registration
        result = BrokerProvenanceHeaderV1(
            family=Family.LIFECYCLE_V1,
            capture_id=native_header.artifact_id,
            native_header_id=native_header.artifact_id,
            plugin_id=registration.plugin_id,
            plugin_version=registration.plugin_version,
            provider_id=invocation.configuration_profile.provider_id,
            configuration_id=invocation.configuration_profile.artifact_id,
            configuration_sha256=native_sha256(
                invocation.configuration_profile.public_configuration_json
            ),
            distribution_name=registration.distribution_name,
            distribution_version=registration.distribution_version,
            implementation_sha256=candidate.implementation_sha256,
            registration_sha256=candidate.registration_sha256,
            sdk_version=native_header.inventory.sdk_version,
            event_schema_version="histdatacom.broker-plugin.event.v1",
            permission_manifest_id=permission_manifest.artifact_id,
            permission_context_id=permission_context.artifact_id,
            permission_decision_id=permission_decision.artifact_id,
            permission_grant_id=permission_decision.grant_id,
            **shared,
        )
    elif type(invocation) is BrokerLegacyCaptureV1:
        if (
            any(
                item is not None
                for item in (
                    permission_manifest,
                    permission_context,
                    permission_decision,
                )
            )
            or invocation.session.to_json() != native_header.to_json()
        ):
            raise ValueError("legacy provenance cannot invent SDK authority")
        rebuilt = make_legacy_health_header(
            native_header,
            provider_decision,
            provider_request=invocation,
            symbols=health_header.symbols,
            queue_capacity=health_header.queue_capacity,
            policy=health_header.policy,
        )
        result = BrokerProvenanceHeaderV1(
            family=Family.LEGACY_CAPTURE_V1,
            capture_id=native_header.session_id,
            native_header_id=native_header.session_id,
            plugin_id=native_header.adapter_id,
            plugin_version=native_header.adapter_version,
            provider_id=native_header.adapter_id,
            configuration_id=native_header.adapter_config_sha256,
            configuration_sha256=native_header.adapter_config_sha256,
            distribution_name=None,
            distribution_version=None,
            implementation_sha256=None,
            registration_sha256=None,
            sdk_version=None,
            event_schema_version="histdatacom.broker-capture-event.v1",
            permission_manifest_id=None,
            permission_context_id=None,
            permission_decision_id=None,
            permission_grant_id=None,
            **shared,
        )
    else:
        raise ValueError("unsupported native provenance invocation")
    if rebuilt.to_json() != health_header.to_json():
        raise ValueError("native provenance health binding substitution")
    return result


class NativeProvenanceRecorder:
    """One actual invocation's interleaved host journal, not a public permit."""

    def __init__(
        self,
        directory: Path,
        invocation: Any,
        native_header: Any,
        health_header: Any,
        provider_decision: Any,
        *,
        guard_text: Callable[[str], None] = lambda text: None,
        permission_manifest: Any = None,
        permission_context: Any = None,
        permission_decision: Any = None,
    ) -> None:
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )

        from .storage import _ProvenanceWriter

        self.invocation = invocation
        self.native_header = native_header
        self.health_header = health_header
        self.identity: Any = None
        self.session: Any = None
        self._guard_text = guard_text
        self.sdk = type(invocation) is BrokerSDKInvocationV1
        environment = host_environment_json()
        self.header = make_native_provenance_header(
            invocation,
            native_header,
            health_header,
            provider_decision,
            environment_json=environment,
            permission_manifest=permission_manifest,
            permission_context=permission_context,
            permission_decision=permission_decision,
        )
        metadata = {
            "invocation.json": (
                invocation.to_json()
                if self.sdk
                else legacy_invocation_json(invocation)
            ),
            "native-header.json": native_header.to_json(),
            "health-header.json": health_header.to_json(),
            "provider-decision.json": provider_decision.to_json(),
            "environment.json": environment,
        }
        if self.sdk:
            if (
                permission_manifest is None
                or permission_context is None
                or permission_decision is None
            ):
                raise ValueError(
                    "SDK provenance requires actual permission evidence"
                )
            metadata.update(
                {
                    "permission-manifest.json": permission_manifest.to_json(),
                    "permission-context.json": permission_context.to_json(),
                    "permission-decision.json": permission_decision.to_json(),
                }
            )
        self.directory = directory
        self._writer = _ProvenanceWriter(
            directory,
            self.header,
            authorize_artifact=self._authorize,
            guard_text=guard_text,
            metadata=metadata,
        )

    def authorize_ingress(self, payload: Any) -> None:
        """Authorize exact input before retaining even its derived event ID.

        Failure leaves an incomplete prefix, not a falsely malformed observation
        or a hash of forbidden contents. The actual journal independently repeats
        its fresh authorization immediately before every write.
        """
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerLegacyRecordV1,
            BrokerSDKRecordV1,
        )
        from histdatacom.broker_plugin_policy.contracts import (
            BrokerPolicyOperation,
        )
        from histdatacom.broker_plugin_policy.scope import (
            require_provider_operation,
        )

        if self.sdk:
            from histdatacom.broker_plugin_capabilities import (
                BrokerAdmittedEventV1,
            )
            from histdatacom.broker_plugin_permissions.scope import (
                require_event_permissions,
                require_native_permissions,
            )

            if type(payload) is not BrokerAdmittedEventV1:
                raise ValueError("exact admitted SDK ingress required")
            if self.session is None or self.identity is None:
                raise ValueError("SDK ingress requires actual native parents")
            self._guard_text(payload.to_json())
            require_native_permissions(self.invocation)
            require_event_permissions(payload.event)
            subject: Any = BrokerSDKRecordV1(
                self.invocation,
                payload,
                self.session.session,
                self.identity.metadata.metadata,
            )
        else:
            from histdatacom.broker_capture.contracts import (
                BrokerAdapterMessageV1,
            )

            if type(payload) is not BrokerAdapterMessageV1:
                raise ValueError("exact legacy ingress required")
            self._guard_text(payload.to_json())
            subject = BrokerLegacyRecordV1(
                self.invocation.session,
                payload,
                self.invocation.output_contract,
            )
        for operation in (
            BrokerPolicyOperation.CAPTURE,
            BrokerPolicyOperation.RETAIN_LOCAL,
        ):
            require_provider_operation(self.invocation, operation)
            require_provider_operation(subject, operation)

    def _authorize(self, artifact: Any) -> None:
        from histdatacom.broker_plugin_policy.contracts import (
            BrokerPolicyOperation,
        )
        from histdatacom.broker_plugin_policy.provenance_bindings import (
            BrokerProvenanceEvidenceV1,
        )
        from histdatacom.broker_plugin_policy.scope import (
            require_provider_operation,
        )

        if self.sdk:
            from histdatacom.broker_plugin_capabilities import (
                BrokerAdmittedEventV1,
            )
            from histdatacom.broker_plugin_lifecycle.contracts import (
                BrokerLifecycleIdentityV1,
                BrokerLifecycleRecordV1,
            )
            from histdatacom.broker_plugin_permissions.scope import (
                require_event_permissions,
                require_metadata_permissions,
                require_native_permissions,
            )

            from .contracts import (
                BrokerProvenanceEntryKind as Kind,
            )
            from .contracts import (
                BrokerProvenanceEntryV1,
                BrokerProvenanceLinkV1,
            )

            require_native_permissions(self.invocation)
            entry = (
                artifact.entry
                if type(artifact) is BrokerProvenanceLinkV1
                else (
                    artifact
                    if type(artifact) is BrokerProvenanceEntryV1
                    else None
                )
            )
            if entry is not None and entry.kind is Kind.ADMITTED_INGRESS:
                admitted, _, _ = parse_ingress(
                    entry.payload_json,
                    family="lifecycle_v1",
                )
                assert isinstance(admitted, BrokerAdmittedEventV1)
                require_event_permissions(admitted.event)
            elif entry is not None and entry.kind is Kind.NATIVE_RECORD:
                record = BrokerLifecycleRecordV1.from_json(entry.payload_json)
                if record.kind == "event":
                    require_event_permissions(
                        BrokerAdmittedEventV1.from_json(
                            record.payload_json
                        ).event,
                    )
                elif record.kind == "identity":
                    require_metadata_permissions(
                        BrokerLifecycleIdentityV1.from_json(
                            record.payload_json,
                        ).metadata.metadata,
                    )
        require_provider_operation(
            self.invocation,
            BrokerPolicyOperation.RETAIN_LOCAL,
        )
        require_provider_operation(
            BrokerProvenanceEvidenceV1(
                self.invocation,
                self.native_header,
                self.health_header,
                self.header,
                artifact,
                self.session,
                self.identity,
            ),
            BrokerPolicyOperation.RETAIN_LOCAL,
        )

    def observation(self, observation: Any) -> None:
        from histdatacom.broker_plugin_health.contracts import (
            BrokerHostHealthObservationV1,
        )

        from .contracts import BrokerProvenanceEntryKind as Kind

        if type(observation) is not BrokerHostHealthObservationV1:
            raise ValueError("exact host observation required")
        self._writer.append(
            Kind.HOST_OBSERVATION,
            observation.epoch,
            observation.to_json(),
            native_record_id=observation.artifact_id,
        )

    def ingress(
        self,
        payload: object,
        epoch: int,
        ingress_sequence: int,
        delivery_sequence: int | None = None,
    ) -> None:
        from .contracts import BrokerProvenanceEntryKind as Kind

        text = ingress_json(
            payload,
            family="lifecycle_v1" if self.sdk else "legacy_capture_v1",
            ingress_sequence=ingress_sequence,
            delivery_sequence=delivery_sequence,
        )
        self._writer.append(Kind.ADMITTED_INGRESS, epoch, text)

    def native(self, record: Any, *, epoch: int | None = None) -> None:
        from .contracts import BrokerProvenanceEntryKind as Kind

        if self.sdk:
            from histdatacom.broker_plugin_lifecycle.contracts import (
                BrokerLifecycleIdentityV1,
                BrokerLifecycleRecordV1,
                BrokerLifecycleSessionV1,
            )

            if type(record) is not BrokerLifecycleRecordV1:
                raise ValueError("exact native SDK lifecycle record required")
            # Parents are adopted only after the real journal accepted/fsynced
            # them. The policy resolver independently validates the association.
            if record.kind == "identity":
                self.identity = BrokerLifecycleIdentityV1.from_json(
                    record.payload_json,
                )
                self.session = None
            elif record.kind == "session":
                self.session = BrokerLifecycleSessionV1.from_json(
                    record.payload_json,
                )
            native_id, native_epoch = record.artifact_id, record.epoch
        else:
            from histdatacom.broker_capture.contracts import (
                BrokerCaptureEventV1,
            )

            if type(record) is not BrokerCaptureEventV1 or epoch is None:
                raise ValueError(
                    "exact legacy record and actual epoch required"
                )
            native_id, native_epoch = record.event_id, epoch
        self._writer.append(
            Kind.NATIVE_RECORD,
            native_epoch,
            record.to_json(),
            native_record_id=native_id,
        )

    def finish(
        self,
        manifest: Any,
        audit: Any,
        *,
        epoch: int,
        permission_execution: Any = None,
    ) -> Any:
        from .contracts import BrokerProvenanceTerminalV1

        # The actual CLOSE observation is the stop timestamp; never sample a
        # replay-time clock or invent an event from an existing final manifest.
        # Caller supplies it through set_stop once its real recorder closes.
        if not hasattr(self, "_stopped"):
            raise ValueError("provenance requires actual host stop observation")
        terminal = BrokerProvenanceTerminalV1(
            header_id=self.header.artifact_id,
            stopped_at_utc_ns=self._stopped[0],
            stopped_at_monotonic_ns=self._stopped[1],
            native_manifest_id=(
                manifest.artifact_id if self.sdk else manifest.manifest_id
            ),
            native_manifest_sha256=native_sha256(manifest.to_json()),
            health_audit_id=audit.artifact_id,
            health_audit_sha256=native_sha256(audit.to_json()),
            native_complete=(
                manifest.completion.value == "complete_local_finite_run"
                if self.sdk
                else manifest.complete
            ),
            observations_complete=audit.complete_observations,
            permission_execution_id=(
                None
                if permission_execution is None
                else permission_execution.artifact_id
            ),
            permission_execution_sha256=(
                None
                if permission_execution is None
                else native_sha256(permission_execution.to_json())
            ),
        )
        return self._writer.finish(terminal, epoch=epoch)

    def set_stop(self, observation: Any) -> None:
        from histdatacom.broker_plugin_health.contracts import (
            BrokerHostHealthObservationKind,
            BrokerHostHealthObservationV1,
        )

        if (
            type(observation) is not BrokerHostHealthObservationV1
            or observation.kind is not BrokerHostHealthObservationKind.CLOSE
            or hasattr(self, "_stopped")
        ):
            raise ValueError("exact unique host CLOSE observation required")
        self._stopped = (observation.utc_ns, observation.monotonic_ns)

    def close(self) -> None:
        self._writer.close()

"""Capture-bound SDK governance and offline, current-rights replay access.

New environment.v2 records are sealed through the unchanged provenance header's
environment hash. Original environment.v1 captures report unavailable governance;
they are never relabelled or made dependent on today's installed packages.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from histdatacom.broker_plugin_registry import BrokerPluginCandidateV1
from histdatacom.broker_plugins import (
    BrokerPluginMetadataV1,
    declared_provider_protocol,
)
from histdatacom.broker_plugin_provenance._wire import Artifact as _Artifact

from .contracts import (
    BrokerConformanceReferenceV1,
    BrokerSDKCompatibilityDecisionV1,
    BrokerSDKUse,
    negotiate_broker_sdk,
)

if TYPE_CHECKING:
    from histdatacom.broker_plugin_lifecycle import BrokerLifecycleManifestV1
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_provenance import BrokerProvenanceSealV1


@dataclass(frozen=True, slots=True)
class BrokerCaptureCompatibilityDeclarationV1(_Artifact):
    native_header_id: str
    native_header_sha256: str
    invocation_id: str
    invocation_sha256: str
    provider_id: str
    event_schema_version: str
    decision: BrokerSDKCompatibilityDecisionV1
    conformance: BrokerConformanceReferenceV1 | None = None
    provider_protocol_status: str = (
        "not_yet_observed_see_sealed_runtime_metadata"
    )
    KIND: ClassVar[str] = "capture-compatibility-declaration"

    def _validate(self) -> None:
        from histdatacom.broker_plugin_provenance.contracts import _identity
        from histdatacom.broker_plugin_provenance._wire import digest

        for value in (
            self.native_header_id,
            self.invocation_id,
            self.provider_id,
        ):
            _identity(value)
        for value in (self.native_header_sha256, self.invocation_sha256):
            digest(value)
        self.decision.require_admitted()
        if self.decision.use is not BrokerSDKUse.LIVE:
            raise ValueError(
                "capture requires its actual live admission decision"
            )
        if self.event_schema_version != "histdatacom.broker-plugin.event.v1":
            raise ValueError(
                "no event migration or semantic equivalence is implied"
            )
        if (
            self.provider_protocol_status
            != "not_yet_observed_see_sealed_runtime_metadata"
        ):
            raise ValueError(
                "capture startup cannot invent a provider protocol observation"
            )
        candidate = BrokerPluginCandidateV1.from_json(
            self.decision.candidate_json
        )
        if self.provider_id not in candidate.registration.provider_ids:
            raise ValueError(
                "capture provider differs from installed declaration"
            )
        if self.conformance is not None:
            report = self.conformance.retained_report
            if report.plan.driver.candidate_id != candidate.artifact_id:
                raise ValueError(
                    "conformance evidence belongs to another capture candidate"
                )


def make_capture_compatibility_declaration(
    invocation: Any,
    native_header: Any,
    *,
    host_version: str,
    started_at_utc_ns: int,
    conformance_reference: BrokerConformanceReferenceV1 | None = None,
) -> BrokerCaptureCompatibilityDeclarationV1:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_provenance.native_common import native_sha256

    if (
        type(invocation) is not BrokerSDKInvocationV1
        or type(native_header) is not BrokerLifecycleHeaderV1
    ):
        raise ValueError("exact native SDK capture parents required")
    if invocation.plan.to_json() != native_header.plan.to_json():
        raise ValueError("capture compatibility plan substitution")
    decision = negotiate_broker_sdk(
        invocation.plan.candidate,
        host_version=host_version,
        use=BrokerSDKUse.LIVE,
        evaluated_at_utc_ns=started_at_utc_ns,
        requested_sdk_version=native_header.inventory.sdk_version,
    )
    decision.require_admitted()
    return BrokerCaptureCompatibilityDeclarationV1(
        native_header.artifact_id,
        native_sha256(native_header.to_json()),
        invocation.artifact_id,
        native_sha256(invocation.to_json()),
        invocation.configuration_profile.provider_id,
        "histdatacom.broker-plugin.event.v1",
        decision,
        conformance_reference,
    )


def verify_capture_compatibility_declaration(
    declaration: BrokerCaptureCompatibilityDeclarationV1,
    invocation: Any,
    native_header: Any,
    *,
    host_version: str,
    started_at_utc_ns: int,
) -> None:
    """Reconstruct using RETAINED policy, not the current support catalogue."""
    from histdatacom.broker_plugin_provenance.native_common import native_sha256

    decision = declaration.decision
    expected = negotiate_broker_sdk(
        invocation.plan.candidate,
        host_version=host_version,
        use=BrokerSDKUse.LIVE,
        evaluated_at_utc_ns=started_at_utc_ns,
        requested_sdk_version=native_header.inventory.sdk_version,
        policy=decision.policy,
    )
    if (
        declaration.native_header_id != native_header.artifact_id
        or declaration.native_header_sha256
        != native_sha256(native_header.to_json())
        or declaration.invocation_id != invocation.artifact_id
        or declaration.invocation_sha256 != native_sha256(invocation.to_json())
        or declaration.provider_id
        != invocation.configuration_profile.provider_id
        or expected != decision
    ):
        raise ValueError(
            "retained compatibility substitutes exact native parents"
        )


@dataclass(frozen=True, slots=True)
class BrokerCaptureCompatibilityV1(_Artifact):
    """Detached verified-parent evidence; the object alone is not a replay grant."""

    environment_json: str
    provenance_header_json: str
    provenance_seal_json: str
    runtime_metadata_jsons: tuple[str, ...]
    KIND: ClassVar[str] = "capture-compatibility"

    @property
    def declaration(self) -> BrokerCaptureCompatibilityDeclarationV1:
        from histdatacom.broker_plugin_provenance.native_common import (
            parse_native_json,
        )

        value = parse_native_json(self.environment_json)
        if (
            value.get("schema_version")
            != "histdatacom.broker-provenance-environment.v2"
        ):
            raise ValueError(
                "compatibility evidence requires retained environment.v2"
            )
        return BrokerCaptureCompatibilityDeclarationV1.from_dict(
            value["sdk_compatibility"]
        )

    @property
    def provider_protocol_jsons(self) -> tuple[str, ...]:
        """Known actual typed declarations; empty means not observed, not guessed."""
        protocols = (
            declared_provider_protocol(BrokerPluginMetadataV1.from_json(text))
            for text in self.runtime_metadata_jsons
        )
        return tuple(
            sorted({info.to_json() for info in protocols if info is not None})
        )

    def _validate(self) -> None:
        from histdatacom.broker_plugin_provenance.contracts import (
            BrokerProvenanceHeaderV1,
            BrokerProvenanceSealV1,
        )
        from histdatacom.broker_plugin_provenance.native_common import (
            native_sha256,
        )

        declaration = self.declaration
        header = BrokerProvenanceHeaderV1.from_json(self.provenance_header_json)
        seal = BrokerProvenanceSealV1.from_json(self.provenance_seal_json)
        candidate = BrokerPluginCandidateV1.from_json(
            declaration.decision.candidate_json
        )
        registration = candidate.registration
        if (
            header.environment_id
            != "broker-provenance-environment:sha256:"
            + native_sha256(self.environment_json)
            or header.native_header_id != declaration.native_header_id
            or header.native_header_sha256 != declaration.native_header_sha256
            or header.host_version != declaration.decision.host_version
            or header.sdk_version != declaration.decision.selected_sdk_version
            or header.provider_id != declaration.provider_id
            or header.plugin_id != registration.plugin_id
            or header.plugin_version != registration.plugin_version
            or header.distribution_name != registration.distribution_name
            or header.distribution_version != registration.distribution_version
            or header.registration_sha256 != candidate.registration_sha256
            or header.implementation_sha256 != candidate.implementation_sha256
            or seal.header_id != header.artifact_id
        ):
            raise ValueError(
                "compatibility evidence differs from sealed version axes"
            )
        if self.runtime_metadata_jsons != tuple(
            sorted(set(self.runtime_metadata_jsons))
        ):
            raise ValueError(
                "runtime metadata evidence must be ordered and unique"
            )
        for text in self.runtime_metadata_jsons:
            metadata = BrokerPluginMetadataV1.from_json(text)
            if (
                metadata.to_json() != text
                or metadata.plugin_id != registration.plugin_id
                or metadata.plugin_version != registration.plugin_version
                or metadata.sdk_version != header.sdk_version
            ):
                raise ValueError(
                    "runtime handshake differs from captured generation"
                )
            info = declared_provider_protocol(metadata)
            if info is not None and info.provider_id != header.provider_id:
                raise ValueError(
                    "runtime provider protocol differs from captured provider"
                )


def read_lifecycle_capture_compatibility(
    directory: Path,
    manifest: BrokerLifecycleManifestV1,
    *,
    provider_request: BrokerSDKInvocationV1,
    expected_root: BrokerProvenanceSealV1 | None = None,
) -> BrokerCaptureCompatibilityV1 | None:
    """Offline sealed governance or explicit historical unavailability.

    Native current-rights checks and complete exact replay run first. No discovery,
    installed package lookup, plugin import, provider connection or registry call.
    """
    from histdatacom.broker_plugin_health.storage import _read
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleIdentityV1,
    )
    from histdatacom.broker_plugin_lifecycle.storage import (
        replay_broker_lifecycle,
    )
    from histdatacom.broker_plugin_provenance.native import (
        read_lifecycle_capture_provenance,
    )
    from histdatacom.broker_plugin_provenance.native_common import (
        native_sha256,
        parse_native_json,
    )

    proof = read_lifecycle_capture_provenance(
        directory,
        manifest,
        provider_request=provider_request,
        expected_root=expected_root,
    )
    environment = _read(
        directory.with_name(directory.name + "-provenance") / "environment.json"
    )
    if (
        proof.header.environment_id
        != "broker-provenance-environment:sha256:" + native_sha256(environment)
    ):
        raise ValueError("capture environment changed after verified replay")
    if (
        parse_native_json(environment)["schema_version"]
        == "histdatacom.broker-provenance-environment.v1"
    ):
        return None
    if proof.seal is None:
        raise ValueError("capture compatibility requires a sealed native run")
    metadata = tuple(
        sorted(
            {
                BrokerLifecycleIdentityV1.from_json(
                    record.payload_json
                ).metadata.metadata.to_json()
                for record in replay_broker_lifecycle(
                    directory,
                    provider_request=provider_request,
                    expected_manifest=manifest,
                )
                if record.kind == "identity"
            }
        )
    )
    return BrokerCaptureCompatibilityV1(
        environment,
        proof.header.to_json(),
        proof.seal.to_json(),
        metadata,
    )

"""Offline native execution evidence; never a stored resource-use permit."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, TypeAlias, cast

from ._wire import MAX_BYTES, Artifact, digest, sha256
from .contracts import (
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionDecisionV1,
    BrokerPermissionManifestV1,
)
from .decisions import BrokerPermissionAuthorityV1, decide_broker_permissions

if TYPE_CHECKING:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_security.contracts import (
        BrokerTrustedSecurityReceiptV1,
    )

    NativeArtifact: TypeAlias = (
        BrokerLifecycleHeaderV1
        | BrokerLifecycleManifestV1
        | BrokerTrustedSecurityReceiptV1
    )

_CHUNK_BYTES = 32_768


def _chunks(text: str) -> tuple[str, ...]:
    if (
        type(text) is not str
        or not text
        or not text.isascii()
        or len(text) > MAX_BYTES
    ):
        raise ValueError("permission execution native bytes exceed bounds")
    return tuple(
        text[index : index + _CHUNK_BYTES]
        for index in range(0, len(text), _CHUNK_BYTES)
    )


def _join(chunks: tuple[str, ...]) -> str:
    if not chunks or len(chunks) > MAX_BYTES // _CHUNK_BYTES:
        raise ValueError("permission execution native chunks exceed bounds")
    if (
        any(len(part) != _CHUNK_BYTES for part in chunks[:-1])
        or not 1 <= len(chunks[-1]) <= _CHUNK_BYTES
    ):
        raise ValueError("permission execution chunks are not canonical")
    if any(not part.isascii() for part in chunks):
        raise ValueError(
            "permission execution requires canonical ASCII native bytes"
        )
    return "".join(chunks)


def _restore_native(kind: str, text: str) -> NativeArtifact:
    # Imports stay local: SDK-only imports must not activate host execution or
    # create a security -> permissions -> security initialization cycle.
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_security.contracts import (
        BrokerTrustedSecurityReceiptV1,
    )

    value: NativeArtifact
    if kind == "lifecycle_header":
        value = BrokerLifecycleHeaderV1.from_json(text)
    elif kind == "lifecycle_manifest":
        value = BrokerLifecycleManifestV1.from_json(text)
    elif kind == "trusted_security_receipt":
        value = BrokerTrustedSecurityReceiptV1.from_json(text)
    else:
        raise ValueError("unsupported permission execution native family")
    if value.to_json() != text:
        raise ValueError(
            "permission execution requires exact native writer bytes"
        )
    return value


def _native_kind(native: object) -> str:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_security.contracts import (
        BrokerTrustedSecurityReceiptV1,
    )

    kinds = {
        BrokerLifecycleHeaderV1: "lifecycle_header",
        BrokerLifecycleManifestV1: "lifecycle_manifest",
        BrokerTrustedSecurityReceiptV1: "trusted_security_receipt",
    }
    if type(native) not in kinds:
        raise ValueError("exact native permission execution artifact required")
    return kinds[type(native)]


def _verify_binding(
    manifest: BrokerPermissionManifestV1,
    binding: BrokerPermissionBindingV1,
    invocation: BrokerSDKInvocationV1,
    native: NativeArtifact,
    decision: BrokerPermissionDecisionV1,
) -> None:
    from histdatacom.broker_plugin_capabilities import (
        BrokerAdmittedEventV1,
        BrokerAdmittedMetadataV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_security.contracts import (
        BrokerTrustedSecurityReceiptV1,
    )
    from histdatacom.broker_plugins import (
        BROKER_PLUGIN_SDK_VERSION,
        BrokerEventKind,
    )

    candidate = invocation.plan.candidate
    registration = candidate.registration
    profile = invocation.configuration_profile
    if (
        manifest.candidate_id != candidate.artifact_id
        or binding.candidate_id != candidate.artifact_id
        or manifest.distribution_name != registration.distribution_name
        or manifest.distribution_version != registration.distribution_version
        or manifest.provider_ids != registration.provider_ids
        or manifest.sdk_version != BROKER_PLUGIN_SDK_VERSION
        or not registration.supports_sdk(manifest.sdk_version)
        or binding.manifest_id != manifest.artifact_id
        or binding.sdk_version != manifest.sdk_version
        or binding.configuration_id != profile.artifact_id
        or binding.provider_id != profile.provider_id
        or profile.provider_id not in manifest.provider_ids
        or profile.private_field_names
    ):
        raise ValueError(
            "permission execution differs from exact native invocation"
        )
    if type(native) is BrokerTrustedSecurityReceiptV1:
        receipt = native
        if (
            receipt.plan_json != invocation.plan.to_json()
            or receipt.public_configuration_json
            != profile.public_configuration_json
            or receipt.policy.secret_fields
            or receipt.policy.candidate_id != candidate.artifact_id
            or receipt.software.sdk_version != binding.sdk_version
        ):
            raise ValueError(
                "permission execution differs from trusted native receipt"
            )
        metadata = BrokerAdmittedMetadataV1.from_json(
            receipt.metadata_json
        ).metadata
        if (
            metadata.extensions
            and "raw_payload:emit" not in decision.effective_atoms
        ):
            raise ValueError(
                "trusted native metadata exceeds retained resource grants"
            )
        for raw in receipt.events_json:
            event = BrokerAdmittedEventV1.from_json(raw).event
            needed = {
                (
                    "emit:quotes"
                    if event.kind is BrokerEventKind.QUOTE
                    else "emit:health"
                )
            }
            if event.quote is not None and (
                event.quote.bid_size is not None
                or event.quote.ask_size is not None
                or event.quote.activity is not None
            ):
                needed.add("emit:sizes")
            if event.raw_provenance is not None or event.extensions:
                needed.add("raw_payload:emit")
            if not needed <= set(decision.effective_atoms):
                raise ValueError(
                    "trusted native event exceeds retained resource grants"
                )
    else:
        header = (
            native.header
            if type(native) is BrokerLifecycleManifestV1
            else cast(BrokerLifecycleHeaderV1, native)
        )
        if (
            header.plan.to_json() != invocation.plan.to_json()
            or header.inventory.sdk_version != binding.sdk_version
        ):
            raise ValueError(
                "permission execution differs from native lifecycle header"
            )


@dataclass(frozen=True, slots=True)
class BrokerPermissionExecutionV1(Artifact):
    """Exact historical declaration replay, not authenticity of feed or execution.

    Native constructors check their own contract/identity rules; this record
    additionally replays the permission decision and exact native associations.
    It does not reread current grants or prove that a broker supplied true data.
    """

    KIND: ClassVar[str] = "execution"
    manifest: BrokerPermissionManifestV1
    binding: BrokerPermissionBindingV1
    context: BrokerPermissionContextV1
    decision: BrokerPermissionDecisionV1
    invocation_chunks: tuple[str, ...]
    native_kind: str
    native_artifact_chunks: tuple[str, ...]
    native_artifact_id: str
    native_artifact_sha256: str

    @property
    def invocation_json(self) -> str:
        return _join(self.invocation_chunks)

    @property
    def native_artifact_json(self) -> str:
        return _join(self.native_artifact_chunks)

    def _validate(self) -> None:
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )

        invocation = BrokerSDKInvocationV1.from_json(self.invocation_json)
        native_json = self.native_artifact_json
        native = _restore_native(self.native_kind, native_json)
        digest(self.native_artifact_sha256)
        if (
            native.artifact_id != self.native_artifact_id
            or sha256(native_json) != self.native_artifact_sha256
        ):
            raise ValueError(
                "permission execution native identity differs from retained bytes"
            )
        expected = decide_broker_permissions(
            self.manifest,
            self.binding,
            self.decision.grant_id,
            self.context,
            self.decision.at_ns,
        )
        if (
            not expected.admitted
            or expected.to_json() != self.decision.to_json()
        ):
            raise ValueError("permission execution decision does not replay")
        _verify_binding(
            self.manifest, self.binding, invocation, native, self.decision
        )


def build_permission_execution(
    invocation: BrokerSDKInvocationV1,
    native_artifact: NativeArtifact,
    authority: BrokerPermissionAuthorityV1,
) -> BrokerPermissionExecutionV1:
    """Snapshot one current admission and bind actual native public artifacts."""
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

    if (
        type(invocation) is not BrokerSDKInvocationV1
        or type(authority) is not BrokerPermissionAuthorityV1
    ):
        raise ValueError(
            "exact native invocation and permission authority required"
        )
    kind = _native_kind(native_artifact)
    invocation = BrokerSDKInvocationV1.from_json(invocation.to_json())
    native = _restore_native(kind, native_artifact.to_json())
    context, decision = authority.snapshot_admission()
    native_json = native.to_json()
    return BrokerPermissionExecutionV1(
        authority.manifest,
        authority.binding,
        context,
        decision,
        _chunks(invocation.to_json()),
        kind,
        _chunks(native_json),
        native.artifact_id,
        sha256(native_json),
    )


def verify_permission_execution(
    execution: BrokerPermissionExecutionV1,
    invocation: BrokerSDKInvocationV1,
    native_artifact: NativeArtifact,
) -> None:
    """Independently restore historical evidence and compare supplied native bytes.

    Resource/capture replay separately requires current host permission and
    provider policy. This reader deliberately makes no current-admission claim.
    """
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

    if (
        type(execution) is not BrokerPermissionExecutionV1
        or type(invocation) is not BrokerSDKInvocationV1
    ):
        raise ValueError(
            "exact permission execution and native invocation required"
        )
    checked = BrokerPermissionExecutionV1.from_json(execution.to_json())
    if (
        _native_kind(native_artifact) != checked.native_kind
        or invocation.to_json() != checked.invocation_json
        or native_artifact.to_json() != checked.native_artifact_json
    ):
        raise ValueError(
            "supplied native artifacts differ from permission execution"
        )

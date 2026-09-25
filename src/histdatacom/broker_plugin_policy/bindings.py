"""Closed host-native provider subjects, separate from permission decisions.

These bindings describe the configuration declared to the host. They neither
authenticate a broker account nor interpret provider terms. Native v1 bytes are
never rewritten. Unknown native types and opaque output fail closed unless the
reviewed output contract explicitly includes that shape and its required class.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, cast

from ._wire import (
    MAX_BYTES,
    MAX_DEPTH,
    MAX_ITEMS,
    MAX_NODES,
    Artifact,
    canonical_json,
    load_json,
    ordered,
)
from .contracts import (
    BrokerPolicyBindingV1,
    BrokerPolicyReferenceV1,
    BrokerPolicySubjectV1,
)
from .contracts import (
    BrokerPolicyDataClass as DataClass,
)

if TYPE_CHECKING:
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFitConfigV1,
    )
    from histdatacom.broker_plugin_capabilities.contracts import (
        BrokerCapabilityPlanV1,
    )
    from histdatacom.broker_plugins.contracts import (
        BrokerConfigurationSchemaV1,
        BrokerPluginMetadataV1,
        BrokerSessionV1,
    )


def _ref(
    kind: str, native_id: str, native_json: str
) -> BrokerPolicyReferenceV1:
    data = native_json.encode("utf-8")
    return BrokerPolicyReferenceV1(
        kind, native_id, hashlib.sha256(data).hexdigest(), len(data)
    )


def _composite_ref(
    kind: str, values: dict[str, object]
) -> BrokerPolicyReferenceV1:
    text = canonical_json(values)
    digest = hashlib.sha256(text.encode("ascii")).hexdigest()
    return _ref(kind, f"broker-provider-native-{kind}:sha256:{digest}", text)


def _native_ref(
    value: Any, id_field: str = "artifact_id"
) -> BrokerPolicyReferenceV1:
    # Only called after an exact-type check in this module, never duck typing.
    return _ref(type(value).__name__, getattr(value, id_field), value.to_json())


def _restore_native(value: object, allowed: set[type[Any]]) -> Any:
    if type(value) not in allowed:
        raise ValueError("unsupported native provider artifact type")
    _preflight_native(value)
    native = cast(Any, value)
    text = native.to_json()
    if type(text) is not str or len(text.encode("utf-8")) > 8 * 1024 * 1024:
        raise ValueError(
            "native provider artifact exceeds bounded identity envelope"
        )
    restored = type(native).from_json(text)
    if restored.to_json() != text:
        raise ValueError(
            "native provider artifact does not round-trip byte-for-byte"
        )
    return restored


def _preflight_native(value: object) -> None:
    """Bound expanded native fields before older serializers allocate copies.

    This is resource admission, not a replacement for each native reader. Field
    aliases count repeatedly; no cache can hide aggregate text or node work.
    Final native JSON is still checked independently after serialization.
    """
    nodes = 0
    size = 0

    def visit(item: object, depth: int) -> None:
        nonlocal nodes, size
        nodes += 1
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("native provider expanded node/depth bound")
        if isinstance(item, Enum):
            visit(item.value, depth + 1)
        elif is_dataclass(item) and not isinstance(item, type):
            descriptors = fields(item)
            if len(descriptors) > MAX_ITEMS:
                raise ValueError("native provider collection bound")
            size += 2 + 2 * len(descriptors)
            for field in descriptors:
                visit(field.name, depth + 1)
                visit(getattr(item, field.name), depth + 1)
        elif type(item) is dict:
            if len(item) > MAX_ITEMS:
                raise ValueError("native provider collection bound")
            size += 2 + 2 * len(item)
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("invalid native provider object key")
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif type(item) in (tuple, list):
            sequence = cast(tuple[object, ...] | list[object], item)
            if len(sequence) > MAX_ITEMS:
                raise ValueError("native provider collection bound")
            size += 2 + len(sequence)
            for child in sequence:
                visit(child, depth + 1)
        elif type(item) is str:
            # A cheap lower bound precedes the exact escaped-character charge.
            if size + len(item) > MAX_BYTES:
                raise ValueError("native provider byte bound")
            size += 2
            for char in item:
                point = ord(char)
                size += (
                    2
                    if char in '\\"\b\f\n\r\t'
                    else (
                        6
                        if point < 32 or 127 < point <= 65535
                        else 12 if point > 65535 else 1
                    )
                )
                if size > MAX_BYTES:
                    raise ValueError("native provider byte bound")
        elif item is None or type(item) is bool:
            size += 5
        elif type(item) is int:
            if item.bit_length() > 64:
                raise ValueError("native provider integer bound")
            size += 21
        elif type(item) is float and math.isfinite(item):
            size += 32
        else:
            raise ValueError("unsupported native provider field")
        if size > MAX_BYTES:
            raise ValueError("native provider byte bound")

    visit(value, 0)


@dataclass(frozen=True, slots=True)
class BrokerProviderConfigurationV1(Artifact):
    """Reviewed SDK schema/public config and an opaque private profile identity.

    Private values are not retained or hashed. The operator must bind the opaque
    profile to the appropriate account; the host cannot authenticate that claim.
    """

    KIND: ClassVar[str] = "provider-configuration"
    provider_id: str
    profile_id: str
    schema_json: str
    public_configuration_json: str
    private_field_names: tuple[str, ...] = ()

    def _validate(self) -> None:
        from histdatacom.broker_plugins.contracts import (
            _check_extension_keys,
            _check_json,
            _text,
        )

        _text(self.provider_id, "provider identity")
        _text(self.profile_id, "opaque configuration profile")
        ordered(self.private_field_names)
        schema = self.schema
        public = load_json(self.public_configuration_json)
        if type(public) is not dict:
            raise ValueError("public configuration must be a canonical object")
        _check_json(public)
        _check_extension_keys(public)
        fields = {field.name: field for field in schema.fields}
        if set(public) & set(self.private_field_names):
            raise ValueError("private configuration cannot also be public")
        if set(public) - set(fields) or set(self.private_field_names) - set(
            fields
        ):
            raise ValueError("unknown reviewed configuration field")
        if any(fields[name].secret for name in public) or any(
            not fields[name].secret for name in self.private_field_names
        ):
            raise ValueError(
                "reviewed private field classification differs from schema"
            )
        if {f.name for f in schema.fields if f.required} - (
            set(public) | set(self.private_field_names)
        ):
            raise ValueError("reviewed configuration omits a required field")
        # Validate public values without inventing or hashing private values.
        from histdatacom.broker_plugins.contracts import (
            BrokerConfigurationSchemaV1,
        )

        public_schema = BrokerConfigurationSchemaV1(
            tuple(f for f in schema.fields if not f.secret)
        )
        public_schema.validate_configuration(public)

    @property
    def schema(self) -> BrokerConfigurationSchemaV1:
        from histdatacom.broker_plugins.contracts import (
            BrokerConfigurationSchemaV1,
        )

        value = BrokerConfigurationSchemaV1.from_json(self.schema_json)
        if value.to_json() != self.schema_json:
            raise ValueError(
                "reviewed configuration schema must retain exact native bytes"
            )
        return value

    def verify_schema(self, schema: BrokerConfigurationSchemaV1) -> None:
        from histdatacom.broker_plugins.contracts import (
            BrokerConfigurationSchemaV1,
        )

        if (
            type(schema) is not BrokerConfigurationSchemaV1
            or schema.to_json() != self.schema_json
        ):
            raise ValueError(
                "provider schema differs from reviewed configuration schema"
            )

    def verify_configuration(self, configuration: Mapping[str, object]) -> None:
        """Check ephemeral values against the reviewed projection; retain none."""
        if type(configuration) is not dict:
            raise ValueError(
                "ephemeral provider configuration must be a plain dictionary"
            )
        self.schema.validate_configuration(configuration)
        public = cast(
            dict[str, object], load_json(self.public_configuration_json)
        )
        if set(configuration) != set(public) | set(self.private_field_names):
            raise ValueError(
                "actual configuration field inventory differs from review"
            )
        # Construct only the explicit public projection, never serialize secrets.
        if (
            canonical_json({name: configuration[name] for name in public})
            != self.public_configuration_json
        ):
            raise ValueError("actual public configuration differs from review")


@dataclass(frozen=True, slots=True)
class BrokerProviderOutputContractV1(Artifact):
    """Closed expected output shape; selecting a shape grants no rights."""

    KIND: ClassVar[str] = "output-contract"
    schema_family: str
    event_kinds: tuple[str, ...]
    allow_raw_hashes: bool = False
    allow_opaque_metadata: bool = False
    allow_private_account_metadata: bool = False

    def _validate(self) -> None:
        # The isolated SDK worker intentionally has no site packages. Import
        # only its selected family; legacy package initialization loads the
        # historical-data stack and must not become an SDK dependency.
        if self.schema_family == "sdk-v1":
            from histdatacom.broker_plugins.contracts import BrokerEventKind

            known = {kind.value for kind in BrokerEventKind}
        elif self.schema_family == "legacy-capture-v1":
            from histdatacom.broker_capture.contracts import (
                BrokerCaptureEventKind,
            )

            known = {kind.value for kind in BrokerCaptureEventKind}
        else:
            raise ValueError(
                "unknown native provider output family or event kind"
            )
        ordered(self.event_kinds, nonempty=True)
        if not set(self.event_kinds) <= known:
            raise ValueError(
                "unknown native provider output family or event kind"
            )

    def expected_classes(self) -> set[DataClass]:
        classes = {DataClass.CONTENT_HASHES, DataClass.HEALTH}
        if "quote" in self.event_kinds:
            classes.add(DataClass.NORMALIZED_QUOTES)
        if self.allow_opaque_metadata:
            classes.add(DataClass.RAW_PAYLOAD)
        if self.allow_private_account_metadata:
            classes.add(DataClass.PRIVATE_ACCOUNT)
        return classes


@dataclass(frozen=True, slots=True)
class BrokerSDKInvocationV1:
    plan: BrokerCapabilityPlanV1
    configuration_profile: BrokerProviderConfigurationV1
    output_contract: BrokerProviderOutputContractV1

    def __post_init__(self) -> None:
        from histdatacom.broker_plugin_capabilities.contracts import (
            BrokerCapabilityPlanV1,
        )

        if type(self.plan) is not BrokerCapabilityPlanV1:
            raise ValueError("exact native capability plan required")
        if (
            type(self.configuration_profile)
            is not BrokerProviderConfigurationV1
            or type(self.output_contract) is not BrokerProviderOutputContractV1
        ):
            raise ValueError(
                "exact reviewed configuration and output contract required"
            )
        plan = BrokerCapabilityPlanV1.from_json(self.plan.to_json())
        plan.require_admitted()
        object.__setattr__(self, "plan", plan)
        if self.output_contract.schema_family != "sdk-v1":
            raise ValueError("SDK invocation requires SDK output contract")
        enabled = set(plan.required + plan.enabled_optional)
        if (
            self.output_contract.allow_raw_hashes
            and "raw-hashes.v1" not in enabled
        ):
            raise ValueError("raw hash output was not capability-admitted")
        # Revalidate deeply; frozen dataclasses alone are not an authenticity gate.
        BrokerProviderConfigurationV1.from_json(
            self.configuration_profile.to_json()
        )
        BrokerProviderOutputContractV1.from_json(self.output_contract.to_json())

    def to_json(self) -> str:
        return canonical_json(
            {
                "schema_version": "histdatacom.broker-provider-sdk-invocation.v1",
                "plan_json": self.plan.to_json(),
                "configuration_profile": self.configuration_profile.to_dict(),
                "output_contract": self.output_contract.to_dict(),
            }
        )

    @classmethod
    def from_json(cls, text: str) -> BrokerSDKInvocationV1:
        from histdatacom.broker_plugin_capabilities.contracts import (
            BrokerCapabilityPlanV1,
        )

        value = load_json(text)
        if type(value) is not dict or set(value) != {
            "schema_version",
            "plan_json",
            "configuration_profile",
            "output_contract",
        }:
            raise ValueError("invalid SDK provider invocation envelope")
        if (
            value["schema_version"]
            != "histdatacom.broker-provider-sdk-invocation.v1"
            or type(value["plan_json"]) is not str
        ):
            raise ValueError("invalid SDK provider invocation schema")
        result = cls(
            BrokerCapabilityPlanV1.from_json(value["plan_json"]),
            BrokerProviderConfigurationV1.from_dict(
                value["configuration_profile"]
            ),
            BrokerProviderOutputContractV1.from_dict(value["output_contract"]),
        )
        if result.to_json() != text:
            raise ValueError("noncanonical native SDK invocation")
        return result

    @property
    def artifact_id(self) -> str:
        return (
            "broker-provider-sdk-invocation:sha256:"
            + hashlib.sha256(self.to_json().encode("ascii")).hexdigest()
        )


@dataclass(frozen=True, slots=True)
class BrokerLegacyCaptureV1:
    session: BrokerCaptureSessionV1
    output_contract: BrokerProviderOutputContractV1


@dataclass(frozen=True, slots=True)
class BrokerLegacyRecordV1:
    session: BrokerCaptureSessionV1
    record: object
    output_contract: BrokerProviderOutputContractV1 | None = None


@dataclass(frozen=True, slots=True)
class BrokerFingerprintFitV1:
    """Pre-derivation shape, never an already generated fingerprint."""

    capture_requests: tuple[BrokerLegacyCaptureV1, ...]
    fit_config: BrokerDeliveryFitConfigV1


@dataclass(frozen=True, slots=True)
class BrokerSDKRecordV1:
    invocation: BrokerSDKInvocationV1
    record: object
    session: BrokerSessionV1 | None = None
    metadata: BrokerPluginMetadataV1 | None = None


@dataclass(frozen=True, slots=True)
class BrokerSDKLifecycleV1:
    invocation: BrokerSDKInvocationV1
    header: object
    record: object
    session: object = None
    identity: object = None


@dataclass(frozen=True, slots=True)
class BrokerSDKSecurityV1:
    invocation: BrokerSDKInvocationV1
    receipt: object
    manifest: object = None


@dataclass(frozen=True, slots=True)
class _ResolvedNative:
    subject: BrokerPolicySubjectV1
    artifact_json: str | None


_LEGACY_CONFIGURATION_FIELDS = (
    "adapter_id",
    "adapter_version",
    "adapter_config_sha256",
    "protocol",
    "environment_id",
    "server_id",
    "account_id_sha256",
    "collector_id",
    "collector_version",
)


def legacy_capture_binding(
    session: BrokerCaptureSessionV1,
) -> BrokerPolicyBindingV1:
    """Bind the native provider/configuration fields retained by fingerprints."""
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1

    if type(session) is not BrokerCaptureSessionV1:
        raise ValueError("exact native capture session required")
    session = _restore_native(session, {BrokerCaptureSessionV1})
    return _legacy_binding(session)


def _legacy_binding(value: Any) -> BrokerPolicyBindingV1:
    return BrokerPolicyBindingV1(
        value.adapter_id,
        "legacy-broker-provider-configuration-v1",
        canonical_json(
            {
                field: getattr(value, field)
                for field in _LEGACY_CONFIGURATION_FIELDS
            }
        ),
    )


def sdk_invocation_binding(
    invocation: BrokerSDKInvocationV1,
) -> BrokerPolicyBindingV1:
    if type(invocation) is not BrokerSDKInvocationV1:
        raise ValueError("exact SDK provider invocation required")
    invocation = BrokerSDKInvocationV1.from_json(invocation.to_json())
    return BrokerPolicyBindingV1(
        invocation.configuration_profile.provider_id,
        "sdk-broker-provider-configuration-v1",
        canonical_json(
            {
                "candidate": invocation.plan.candidate.to_dict(),
                "configuration_profile": invocation.configuration_profile.to_dict(),
            }
        ),
    )


def _session_classes(session: BrokerCaptureSessionV1) -> set[DataClass]:
    classes = {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    if (
        session.account_id_sha256 is not None
        or session.host_id_sha256 is not None
    ):
        classes.add(DataClass.PRIVATE_ACCOUNT)
    if session.public_metadata:
        classes.add(DataClass.RAW_PAYLOAD)
    return classes


def _legacy_message_classes(message: Any) -> set[DataClass]:
    from histdatacom.broker_capture.contracts import BrokerCaptureEventKind

    classes = {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    if message.kind is BrokerCaptureEventKind.QUOTE:
        classes.add(DataClass.NORMALIZED_QUOTES)
    if message.public_metadata:
        classes.add(DataClass.RAW_PAYLOAD)
    return classes


def _check_output(
    contract: BrokerProviderOutputContractV1,
    classes: set[DataClass],
    *,
    event_kind: str | None = None,
    raw_hash: bool = False,
) -> None:
    if type(contract) is not BrokerProviderOutputContractV1:
        raise ValueError("exact provider output contract required")
    BrokerProviderOutputContractV1.from_json(contract.to_json())
    if event_kind is not None and event_kind not in contract.event_kinds:
        raise ValueError("provider emitted an undeclared event kind")
    if raw_hash and not contract.allow_raw_hashes:
        raise ValueError("provider emitted disabled raw hash provenance")
    if not classes <= contract.expected_classes():
        raise ValueError(
            "provider emitted data outside the reviewed output contract"
        )


def _legacy_subject(
    native: BrokerLegacyCaptureV1 | BrokerLegacyRecordV1,
) -> _ResolvedNative:
    from histdatacom.broker_capture.contracts import (
        BrokerAdapterMessageV1,
        BrokerCaptureEventV1,
        BrokerCapturePartitionManifestV1,
        BrokerCaptureReplaySummaryV1,
        BrokerCaptureSessionManifestV1,
        BrokerCaptureSessionV1,
    )

    if type(native.session) is not BrokerCaptureSessionV1:
        raise ValueError("exact native capture session required")
    session = _restore_native(native.session, {BrokerCaptureSessionV1})
    binding = legacy_capture_binding(session)
    classes = _session_classes(session)
    parent = _native_ref(session, "session_id")
    artifact_json = None
    if isinstance(native, BrokerLegacyCaptureV1):
        contract = native.output_contract
        if (
            type(contract) is not BrokerProviderOutputContractV1
            or contract.schema_family != "legacy-capture-v1"
        ):
            raise ValueError(
                "legacy capture requires its reviewed native output contract"
            )
        _check_output(contract, classes)
        classes |= contract.expected_classes()
        ref = _composite_ref(
            "legacy-capture-request",
            {"session": parent, "output_contract": contract},
        )
    else:
        allowed = {
            BrokerAdapterMessageV1: "message_id",
            BrokerCaptureEventV1: "event_id",
            BrokerCapturePartitionManifestV1: "partition_id",
            BrokerCaptureReplaySummaryV1: "summary_id",
            BrokerCaptureSessionManifestV1: "manifest_id",
            BrokerCaptureSessionV1: "session_id",
        }
        if type(native.record) not in allowed:
            raise ValueError("unsupported native legacy provider record")
        record = _restore_native(native.record, set(allowed))
        artifact_json = cast(str, record.to_json())
        if type(record) is BrokerCaptureSessionManifestV1:
            if record.session.to_json() != session.to_json():
                raise ValueError(
                    "capture manifest differs from its exact session parent"
                )
            if record.limitations:
                classes.add(DataClass.RAW_PAYLOAD)
        elif type(record) is BrokerCaptureSessionV1:
            if record.to_json() != session.to_json():
                raise ValueError("capture session parent differs")
        elif (
            type(record) is not BrokerAdapterMessageV1
            and record.session_id != session.session_id
        ):
            raise ValueError("capture record refers to another session")
        message = (
            record.message
            if type(record) is BrokerCaptureEventV1
            else record if type(record) is BrokerAdapterMessageV1 else None
        )
        if message is not None:
            classes |= _legacy_message_classes(message)
        if native.output_contract is not None:
            if (
                type(native.output_contract)
                is not BrokerProviderOutputContractV1
                or native.output_contract.schema_family != "legacy-capture-v1"
            ):
                raise ValueError("wrong legacy output family")
            _check_output(
                native.output_contract,
                classes,
                event_kind=None if message is None else message.kind.value,
                raw_hash=message is not None
                and message.raw_message_sha256 is not None,
            )
        ref = _composite_ref(
            "legacy-record",
            {
                "session": parent,
                "record": _native_ref(record, allowed[type(record)]),
            },
        )
    return _ResolvedNative(
        BrokerPolicySubjectV1(ref, (binding,), tuple(sorted(classes))),
        artifact_json,
    )


def _sdk_metadata_classes(metadata: Any) -> set[DataClass]:
    from histdatacom.broker_plugin_capabilities.catalog import (
        PUBLIC_CONTEXT_NAMESPACE,
    )
    from histdatacom.broker_plugin_capabilities.validation import _CONTEXT

    classes = {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    for extension in metadata.extensions:
        if extension.namespace != PUBLIC_CONTEXT_NAMESPACE:
            classes.add(DataClass.RAW_PAYLOAD)
            continue
        context = extension.payload()
        if any(
            name not in _CONTEXT or value not in _CONTEXT[name][1]
            for name, value in context.items()
        ):
            classes.add(DataClass.RAW_PAYLOAD)
        if "account_class" in context:
            classes.add(DataClass.PRIVATE_ACCOUNT)
    return classes


def _fingerprint_fit_subject(native: BrokerFingerprintFitV1) -> _ResolvedNative:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFitConfigV1,
    )

    if type(native.fit_config) is not BrokerDeliveryFitConfigV1:
        raise ValueError(
            "exact native fingerprint fitting configuration required"
        )
    config = cast(Any, native.fit_config)
    _preflight_native(config)
    config_json = canonical_json(config.to_dict())
    config_payload = load_json(config_json)
    if type(config_payload) is not dict:
        raise ValueError("native fingerprint configuration must be an object")
    restored = BrokerDeliveryFitConfigV1.from_dict(config_payload)
    if canonical_json(restored.to_dict()) != config_json:
        raise ValueError(
            "fingerprint configuration is not exact canonical native data"
        )
    if (
        type(native.capture_requests) is not tuple
        or not 1
        <= len(native.capture_requests)
        <= min(128, restored.max_capture_manifests)
        or any(
            type(item) is not BrokerLegacyCaptureV1
            for item in native.capture_requests
        )
    ):
        raise ValueError(
            "fingerprint fit requires bounded exact native capture requests"
        )
    sources = tuple(
        _legacy_subject(item).subject for item in native.capture_requests
    )
    source_ids = tuple(item.native_ref.native_id for item in sources)
    session_ids = tuple(
        item.session.session_id for item in native.capture_requests
    )
    if len(source_ids) != len(set(source_ids)) or len(session_ids) != len(
        set(session_ids)
    ):
        raise ValueError("duplicate native fingerprint source request")
    bindings = {
        binding.artifact_id: binding
        for source in sources
        for binding in source.bindings
    }
    if len(bindings) > 16:
        raise ValueError("fingerprint fit exceeds provider binding limit")
    classes = {
        DataClass.FINGERPRINTS,
        DataClass.HEALTH,
        DataClass.CONTENT_HASHES,
    }
    if any(
        DataClass.PRIVATE_ACCOUNT in source.data_classes for source in sources
    ):
        classes.add(DataClass.PRIVATE_ACCOUNT)
    ref = _composite_ref(
        "fingerprint-fit-request",
        {
            "capture_requests": tuple(source.native_ref for source in sources),
            "fit_config": _ref(
                "BrokerDeliveryFitConfigV1", restored.config_id, config_json
            ),
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            ref,
            tuple(bindings[key] for key in sorted(bindings)),
            tuple(sorted(classes)),
        ),
        None,
    )


def _sdk_record_classes(
    record: Any,
    invocation: BrokerSDKInvocationV1,
    session: BrokerSessionV1 | None,
) -> set[DataClass]:
    from histdatacom.broker_plugin_capabilities.contracts import (
        BrokerAdmittedEventV1,
        BrokerAdmittedInstrumentV1,
        BrokerAdmittedMetadataV1,
        BrokerInvocationAssociation,
        BrokerInvocationBindingV1,
    )
    from histdatacom.broker_plugin_capabilities.validation import (
        validate_broker_capability_event,
        validate_broker_instrument,
        validate_broker_metadata,
        verify_broker_admitted_event,
    )
    from histdatacom.broker_plugins.contracts import (
        BrokerConfigurationSchemaV1,
        BrokerEventV1,
        BrokerInstrumentV1,
        BrokerPluginMetadataV1,
        BrokerSessionV1,
    )

    classes = {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    if type(record) is BrokerAdmittedEventV1:
        verify_broker_admitted_event(record, invocation.plan)
        return _sdk_record_classes(record.event, invocation, session)
    if type(record) is BrokerAdmittedMetadataV1:
        if (
            validate_broker_metadata(invocation.plan, record.metadata).to_json()
            != record.to_json()
        ):
            raise ValueError(
                "admitted metadata differs from native capability replay"
            )
        return _sdk_record_classes(record.metadata, invocation, session)
    if type(record) is BrokerAdmittedInstrumentV1:
        if (
            validate_broker_instrument(
                invocation.plan, record.instrument
            ).to_json()
            != record.to_json()
        ):
            raise ValueError(
                "admitted instrument differs from native capability replay"
            )
    elif type(record) is BrokerPluginMetadataV1:
        validate_broker_metadata(invocation.plan, record)
        classes |= _sdk_metadata_classes(record)
    elif type(record) is BrokerConfigurationSchemaV1:
        invocation.configuration_profile.verify_schema(record)
    elif type(record) is BrokerEventV1:
        validate_broker_capability_event(invocation.plan, record)
        if (
            session is None
            or type(session) is not BrokerSessionV1
            or record.session_id != session.artifact_id
        ):
            raise ValueError(
                "SDK event requires its exact native session parent"
            )
        if record.quote is not None:
            classes.add(DataClass.NORMALIZED_QUOTES)
        if record.extensions or (
            record.diagnostic is not None
            and record.diagnostic.summary != record.diagnostic.code.value
        ):
            classes.add(DataClass.RAW_PAYLOAD)
        _check_output(
            invocation.output_contract,
            classes,
            event_kind=record.kind.value,
            raw_hash=record.raw_provenance is not None,
        )
    elif type(record) is BrokerSessionV1:
        if session is not None and record.to_json() != session.to_json():
            raise ValueError("SDK session parent differs")
    elif type(record) is BrokerInvocationBindingV1:
        if (
            record.plan_id != invocation.plan.artifact_id
            or record.candidate_id != invocation.plan.candidate.artifact_id
        ):
            raise ValueError(
                "SDK invocation binding differs from exact native plan"
            )
        if (
            record.association
            is BrokerInvocationAssociation.INSTALLED_ENTRYPOINT
            and record.module_sha256
            != invocation.plan.candidate.implementation_sha256
        ):
            raise ValueError(
                "SDK invocation binding implementation differs from candidate"
            )
    elif type(record) is BrokerInstrumentV1:
        validate_broker_instrument(invocation.plan, record)
    elif type(record) is not BrokerInstrumentV1:
        raise ValueError("unsupported SDK provider record")
    _check_output(invocation.output_contract, classes)
    return classes


def _sdk_subject(
    native: BrokerSDKInvocationV1 | BrokerSDKRecordV1,
) -> _ResolvedNative:
    from histdatacom.broker_plugin_capabilities.contracts import (
        BrokerAdmittedEventV1,
        BrokerAdmittedInstrumentV1,
        BrokerAdmittedMetadataV1,
        BrokerInvocationBindingV1,
    )
    from histdatacom.broker_plugins.contracts import (
        BrokerConfigurationSchemaV1,
        BrokerEventV1,
        BrokerInstrumentV1,
        BrokerPluginMetadataV1,
        BrokerSessionV1,
    )

    invocation = (
        native
        if isinstance(native, BrokerSDKInvocationV1)
        else native.invocation
    )
    if type(invocation) is not BrokerSDKInvocationV1:
        raise ValueError("exact native SDK invocation required")
    invocation = BrokerSDKInvocationV1.from_json(invocation.to_json())
    binding = sdk_invocation_binding(invocation)
    artifact_json = None
    if isinstance(native, BrokerSDKInvocationV1):
        classes = invocation.output_contract.expected_classes()
        if invocation.configuration_profile.private_field_names:
            classes.add(DataClass.PRIVATE_ACCOUNT)
        ref = _ref(
            "sdk-provider-invocation",
            invocation.artifact_id,
            invocation.to_json(),
        )
    else:
        allowed = {
            BrokerAdmittedEventV1,
            BrokerAdmittedInstrumentV1,
            BrokerAdmittedMetadataV1,
            BrokerInvocationBindingV1,
            BrokerConfigurationSchemaV1,
            BrokerEventV1,
            BrokerInstrumentV1,
            BrokerPluginMetadataV1,
            BrokerSessionV1,
        }
        if type(native.record) not in allowed:
            raise ValueError("unsupported native SDK provider record")
        record = _restore_native(native.record, allowed)
        artifact_json = cast(str, record.to_json())
        session = native.session
        metadata = native.metadata
        parent_classes: set[DataClass] = set()
        if metadata is not None:
            if type(metadata) is not BrokerPluginMetadataV1:
                raise ValueError("exact SDK metadata parent required")
            metadata = BrokerPluginMetadataV1.from_json(metadata.to_json())
            parent_classes |= _sdk_record_classes(metadata, invocation, None)
        if session is not None:
            if type(session) is not BrokerSessionV1:
                raise ValueError("exact native SDK session parent required")
            session = BrokerSessionV1.from_json(session.to_json())
            if metadata is None or session.metadata_id != metadata.artifact_id:
                raise ValueError(
                    "SDK session lacks its exact plan-bound metadata parent"
                )
        if type(record) is BrokerSessionV1 and (
            metadata is None or record.metadata_id != metadata.artifact_id
        ):
            raise ValueError("SDK session differs from plan-bound metadata")
        if type(record) is BrokerInvocationBindingV1 and (
            metadata is None or record.metadata_id != metadata.artifact_id
        ):
            raise ValueError(
                "SDK invocation binding lacks its exact metadata parent"
            )
        if type(record) in (BrokerPluginMetadataV1, BrokerAdmittedMetadataV1):
            actual_metadata = (
                record.metadata
                if type(record) is BrokerAdmittedMetadataV1
                else record
            )
            if (
                metadata is not None
                and actual_metadata.to_json() != metadata.to_json()
            ):
                raise ValueError(
                    "SDK metadata record differs from supplied parent"
                )
        classes = parent_classes | _sdk_record_classes(
            record, invocation, session
        )
        ref = _composite_ref(
            "sdk-record",
            {
                "invocation": _ref(
                    "sdk-provider-invocation",
                    invocation.artifact_id,
                    invocation.to_json(),
                ),
                "record": _native_ref(record),
                "session": None if session is None else _native_ref(session),
                "metadata": None if metadata is None else _native_ref(metadata),
            },
        )
    return _ResolvedNative(
        BrokerPolicySubjectV1(ref, (binding,), tuple(sorted(classes))),
        artifact_json,
    )


def _sdk_lifecycle_subject(
    native: BrokerSDKLifecycleV1,
) -> _ResolvedNative:
    from histdatacom.broker_plugin_capabilities.contracts import (
        BrokerAdmittedEventV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleHeaderV1,
        BrokerLifecycleIdentityV1,
        BrokerLifecycleManifestV1,
        BrokerLifecyclePartitionV1,
        BrokerLifecycleRecordV1,
        BrokerLifecycleSessionV1,
        BrokerLifecycleTransitionV1,
    )

    if (
        type(native.invocation) is not BrokerSDKInvocationV1
        or type(native.header) is not BrokerLifecycleHeaderV1
    ):
        raise ValueError(
            "exact lifecycle header and provider invocation required"
        )
    invocation = BrokerSDKInvocationV1.from_json(native.invocation.to_json())
    header = _restore_native(native.header, {BrokerLifecycleHeaderV1})
    if header.plan.to_json() != invocation.plan.to_json():
        raise ValueError("lifecycle plan differs from reviewed invocation")
    allowed = {
        BrokerLifecycleHeaderV1,
        BrokerLifecycleIdentityV1,
        BrokerLifecycleManifestV1,
        BrokerLifecyclePartitionV1,
        BrokerLifecycleRecordV1,
        BrokerLifecycleSessionV1,
        BrokerLifecycleTransitionV1,
    }
    if type(native.record) not in allowed:
        raise ValueError("unsupported native lifecycle record")
    record = _restore_native(native.record, allowed)
    record_json = cast(str, record.to_json())
    classes = {DataClass.CONTENT_HASHES, DataClass.HEALTH}
    identity: Any = native.identity
    session: Any = native.session
    if identity is not None:
        if type(identity) is not BrokerLifecycleIdentityV1:
            raise ValueError("exact lifecycle identity parent required")
        identity = _restore_native(identity, {BrokerLifecycleIdentityV1})
    if session is not None:
        if type(session) is not BrokerLifecycleSessionV1:
            raise ValueError("exact lifecycle session parent required")
        session = _restore_native(session, {BrokerLifecycleSessionV1})

    def identity_classes(value: Any) -> set[DataClass]:
        invocation.configuration_profile.verify_schema(
            value.configuration_schema
        )
        if (
            value.binding.module_sha256
            != invocation.plan.candidate.implementation_sha256
        ):
            raise ValueError(
                "lifecycle identity implementation differs from reviewed candidate"
            )
        result = _sdk_record_classes(value.binding, invocation, None)
        result |= _sdk_record_classes(value.metadata, invocation, None)
        return result

    def session_classes(value: Any) -> set[DataClass]:
        if (
            identity is None
            or value.session.metadata_id
            != identity.metadata.metadata.artifact_id
        ):
            raise ValueError(
                "lifecycle session lacks its exact metadata identity"
            )
        result = identity_classes(identity)
        for instrument in value.instruments:
            result |= _sdk_record_classes(instrument, invocation, value.session)
        return result

    # Every retained parent reference is validated and classified, even for a
    # transition/header record whose own fields contain only bounded health.
    if identity is not None:
        classes |= identity_classes(identity)
    if session is not None:
        classes |= session_classes(session)

    payload = record
    if type(record) is BrokerLifecycleHeaderV1:
        if record_json != header.to_json():
            raise ValueError("lifecycle header parent differs")
    elif type(record) is BrokerLifecycleManifestV1:
        if record.header.to_json() != header.to_json():
            raise ValueError("lifecycle manifest header differs")
    elif type(record) is BrokerLifecycleRecordV1:
        if record.run_id != header.artifact_id:
            raise ValueError("lifecycle record belongs to another native run")
        constructor: Any = {
            "transition": BrokerLifecycleTransitionV1,
            "identity": BrokerLifecycleIdentityV1,
            "session": BrokerLifecycleSessionV1,
            "event": BrokerAdmittedEventV1,
        }[record.kind]
        payload = constructor.from_json(record.payload_json)
    if type(payload) is BrokerLifecycleIdentityV1:
        if identity is not None and payload.to_json() != identity.to_json():
            raise ValueError("lifecycle identity record differs from parent")
        classes |= identity_classes(payload)
    elif type(payload) is BrokerLifecycleSessionV1:
        if session is not None and payload.to_json() != session.to_json():
            raise ValueError("lifecycle session record differs from parent")
        classes |= session_classes(payload)
    elif type(payload) is BrokerAdmittedEventV1:
        if session is None or identity is None:
            raise ValueError(
                "lifecycle event requires exact identity and session parents"
            )
        classes |= session_classes(session)
        classes |= _sdk_record_classes(payload, invocation, session.session)
    _check_output(invocation.output_contract, classes)
    ref = _composite_ref(
        "sdk-lifecycle-record",
        {
            "invocation": _ref(
                "sdk-provider-invocation",
                invocation.artifact_id,
                invocation.to_json(),
            ),
            "header": _native_ref(header),
            "record": _native_ref(record),
            "identity": None if identity is None else _native_ref(identity),
            "session": None if session is None else _native_ref(session),
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            ref, (sdk_invocation_binding(invocation),), tuple(sorted(classes))
        ),
        record_json,
    )


def _sdk_security_subject(native: BrokerSDKSecurityV1) -> _ResolvedNative:
    from histdatacom.broker_plugin_capabilities.contracts import (
        BrokerAdmittedEventV1,
        BrokerAdmittedMetadataV1,
        BrokerInvocationBindingV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_security.contracts import (
        BrokerSecurityReceiptV1,
        BrokerTrustedSecurityReceiptV1,
    )
    from histdatacom.broker_plugins.contracts import (
        BROKER_PLUGIN_SDK_VERSION,
        BrokerSessionV1,
    )

    if type(native.invocation) is not BrokerSDKInvocationV1 or type(
        native.receipt
    ) not in {BrokerSecurityReceiptV1, BrokerTrustedSecurityReceiptV1}:
        raise ValueError(
            "exact native security receipt and invocation required"
        )
    invocation = BrokerSDKInvocationV1.from_json(native.invocation.to_json())
    receipt = _restore_native(
        native.receipt,
        {BrokerSecurityReceiptV1, BrokerTrustedSecurityReceiptV1},
    )
    if receipt.policy.candidate_id != invocation.plan.candidate.artifact_id:
        raise ValueError(
            "security receipt candidate differs from reviewed provider"
        )
    candidate = invocation.plan.candidate
    if (
        receipt.software.registration_sha256 != candidate.registration_sha256
        or receipt.software.implementation_sha256
        != candidate.implementation_sha256
        or receipt.software.distribution_name
        != candidate.registration.distribution_name
        or receipt.software.distribution_version
        != candidate.registration.distribution_version
        or receipt.software.sdk_version != BROKER_PLUGIN_SDK_VERSION
    ):
        raise ValueError(
            "security software provenance differs from selected candidate"
        )
    if (
        receipt.public_configuration_json
        != invocation.configuration_profile.public_configuration_json
    ):
        raise ValueError(
            "security receipt public configuration differs from review"
        )
    if set(receipt.policy.secret_fields) != set(
        invocation.configuration_profile.private_field_names
    ):
        raise ValueError(
            "security and provider policy private field inventories differ"
        )
    classes = {DataClass.HEALTH, DataClass.CONTENT_HASHES}
    manifest = None
    if type(receipt) is BrokerTrustedSecurityReceiptV1:
        if receipt.plan_json != invocation.plan.to_json():
            raise ValueError("trusted security receipt native plan differs")
        metadata = BrokerAdmittedMetadataV1.from_json(receipt.metadata_json)
        session = BrokerSessionV1.from_json(receipt.session_json)
        classes |= _sdk_record_classes(metadata, invocation, None)
        classes |= _sdk_record_classes(
            BrokerInvocationBindingV1.from_json(
                receipt.invocation_binding_json
            ),
            invocation,
            None,
        )
        if session.metadata_id != metadata.metadata.artifact_id:
            raise ValueError(
                "trusted security receipt session identity differs"
            )
        for text in receipt.events_json:
            classes |= _sdk_record_classes(
                BrokerAdmittedEventV1.from_json(text), invocation, session
            )
        if native.manifest is not None:
            raise ValueError(
                "trusted security receipt has no lifecycle manifest"
            )
    else:
        if type(native.manifest) is not BrokerLifecycleManifestV1:
            raise ValueError(
                "isolated security receipt needs exact lifecycle manifest"
            )
        manifest = _restore_native(native.manifest, {BrokerLifecycleManifestV1})
        if (
            manifest.artifact_id != receipt.native_manifest_id
            or manifest.header.plan.to_json() != invocation.plan.to_json()
        ):
            raise ValueError(
                "isolated security receipt native manifest differs"
            )
    _check_output(invocation.output_contract, classes)
    ref = _composite_ref(
        "sdk-security-receipt",
        {
            "invocation": _ref(
                "sdk-provider-invocation",
                invocation.artifact_id,
                invocation.to_json(),
            ),
            "receipt": _native_ref(receipt),
            "manifest": None if manifest is None else _native_ref(manifest),
        },
    )
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            ref, (sdk_invocation_binding(invocation),), tuple(sorted(classes))
        ),
        cast(str, receipt.to_json()),
    )


def _resolve_native(native_subject: object) -> _ResolvedNative:
    if type(native_subject) in (BrokerLegacyCaptureV1, BrokerLegacyRecordV1):
        return _legacy_subject(cast(Any, native_subject))
    if type(native_subject) is BrokerFingerprintFitV1:
        return _fingerprint_fit_subject(native_subject)
    if type(native_subject) in (BrokerSDKInvocationV1, BrokerSDKRecordV1):
        return _sdk_subject(cast(Any, native_subject))
    if type(native_subject) is BrokerSDKLifecycleV1:
        return _sdk_lifecycle_subject(native_subject)
    if type(native_subject) is BrokerSDKSecurityV1:
        return _sdk_security_subject(native_subject)
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )

    if type(native_subject) is BrokerDeliveryFingerprintV1:
        fingerprint = _restore_native(
            native_subject, {BrokerDeliveryFingerprintV1}
        )
        classes = {
            DataClass.FINGERPRINTS,
            DataClass.CONTENT_HASHES,
            DataClass.HEALTH,
        }
        if fingerprint.account_id_sha256 is not None:
            classes.add(DataClass.PRIVATE_ACCOUNT)
        if _fingerprint_has_opaque_text(fingerprint):
            classes.add(DataClass.RAW_PAYLOAD)
        return _ResolvedNative(
            BrokerPolicySubjectV1(
                _native_ref(fingerprint, "fingerprint_id"),
                (_legacy_binding(fingerprint),),
                tuple(sorted(classes)),
            ),
            cast(str, fingerprint.to_json()),
        )
    from .derived import resolve_derived_native
    from .training_bindings import (
        BrokerTrainingArtifactV1,
        resolve_training_native,
    )

    if type(native_subject) is BrokerTrainingArtifactV1:
        return resolve_training_native(native_subject)
    return resolve_derived_native(native_subject)


def _fingerprint_has_opaque_text(fingerprint: Any) -> bool:
    """Recognize the frozen fitter vocabulary, not arbitrary user text.

    Configuration/source identifiers and condition dimensions remain declared
    canonical identifiers, not evidence of source authenticity. Unknown free
    diagnostics, metric vocabulary or categories conservatively require RAW.
    The native fitter currently produces no categorical count payloads.
    """
    from histdatacom.broker_capture.contracts import BrokerCaptureEventKind

    limitations = {
        "profile_describes_broker_observation_delivery_not_market_truth",
        "cadence_uses_monotonic_receive_time",
        "calendar_conditioning_uses_utc_receive_time",
        "one_or_more_captures_have_nonfatal_health_limitations",
        "sparse_condition_cells_use_explicit_backoff_or_are_unsupported",
        "large_metric_distributions_use_deterministic_bounded_samples",
        "no_versioned_market_context_timeline_supplied",
        "market_context_timeline_is_incomplete",
        "market_context_timeline_does_not_cover_capture_support",
        "calendar_profile_is_incomplete",
    }
    if set(fingerprint.limitations) - limitations:
        return True
    for decision in fingerprint.eligibility_decisions:
        if set(decision.reason_codes) - {
            "capture_limitation_present",
            "clock_corrections_present",
        }:
            return True
    names = {
        "event_intensity_hz",
        "quote_intensity_hz",
        "event_interarrival_ns",
        "outage_or_gap_duration_ns",
        "clock_correction_abs_ns",
        "spread",
        "source_timestamp_precision_ns",
        "price_decimal_places",
        "price_trailing_zero_rate",
        "quote_interarrival_ns",
        "burst_interval_rate",
        "quiet_interval_rate",
        "stale_quote_rate",
        "transition_rate",
        "exact_duplicate_rate",
        "spread_change",
        "absolute_spread_change",
        "active_quote_interarrival_ns",
        "burst_interval_run_length",
        "quiet_interval_run_length",
        "stale_quote_run_length",
        "source_batch_quote_count",
    }
    names.update(
        f"event_kind.{kind.value}_rate" for kind in BrokerCaptureEventKind
    )
    names.update(
        f"event_transition.{left.value}_to_{right.value}_rate"
        for left in BrokerCaptureEventKind
        for right in BrokerCaptureEventKind
    )
    quantiles = {
        f"q{value:.6f}".rstrip("0").rstrip(".")
        for value in fingerprint.fit_config.quantiles
    }
    for cell in fingerprint.cells:
        if set(cell.limitations) - {
            "insufficient_support_no_qualified_backoff",
            "insufficient_support_using_parent_condition",
        }:
            return True
        for metric in cell.metrics:
            if (
                metric.name not in names
                or metric.kind not in {"distribution", "rate"}
                or metric.unit
                not in {
                    "ratio",
                    "ns",
                    "price",
                    "decimal_places",
                    "intervals",
                    "quotes",
                    "events_per_second",
                    "quotes_per_second",
                }
                or metric.category_counts
                or set(metric.quantiles) - quantiles
                or set(metric.limitations)
                - {
                    "metric_has_no_observations",
                    "deterministic_bottom_hash_sample",
                }
            ):
                return True
    return False


def resolve_provider_subject(native_subject: object) -> BrokerPolicySubjectV1:
    """Resolve only registered exact native types; no caller classification hook."""
    return _resolve_native(native_subject).subject


def native_provider_artifact_json(native_subject: object) -> str:
    """Return exact validated native bytes for a storable closed host wrapper.

    The native payload and subject use the same detached snapshot. This helper
    grants no permission; callers must freshly guard each persistence effect.
    Expected-output or invocation-only subjects are not retained native output.
    """
    snapshot = _resolve_native(native_subject)
    if snapshot.artifact_json is None:
        raise ValueError("provider invocation has no storable native artifact")
    return snapshot.artifact_json

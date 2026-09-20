"""Strict public security policy and evidence, separate from native captures."""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, ClassVar, NoReturn, TypeVar, cast, get_type_hints
from typing import get_args, get_origin

MAX_SECURITY_BYTES = 262_144
BROKER_SECURITY_VERSION = "1.0.0"
_T = TypeVar("_T", bound="_Artifact")


class BrokerSecurityReason(str, Enum):
    INVALID_POLICY = "invalid_security_policy"
    AUTHORIZATION = "security_authorization_required"
    UNSUPPORTED = "security_backend_unsupported"
    ISOLATION = "security_enforcement_unavailable"
    SECRET = "private_material_refused"
    RESOLUTION = "secret_resolution_failed"
    IDENTITY = "security_identity_mismatch"
    PUBLICATION = "plugin_publication_forbidden"
    RESOURCE = "security_resource_refused"
    EXECUTION = "security_execution_failed"
    INTEGRITY = "security_integrity_failure"


class BrokerSecurityError(ValueError):
    """Closed diagnostics; never render configuration, paths or exceptions."""

    def __init__(self, reason: BrokerSecurityReason) -> None:
        if type(reason) is not BrokerSecurityReason:
            raise ValueError("invalid security reason")
        self.reason = reason
        super().__init__(reason.value)


def refuse(
    reason: BrokerSecurityReason = BrokerSecurityReason.INVALID_POLICY,
) -> NoReturn:
    raise BrokerSecurityError(reason) from None


class BrokerTrustTier(str, Enum):
    FIRST_PARTY = "first_party"
    REVIEWED = "reviewed_third_party"
    DEVELOPMENT = "untrusted_local_development"


class BrokerSecurityMode(str, Enum):
    TRUSTED_IN_PROCESS = "trusted_in_process"
    KERNEL_ISOLATED = "kernel_isolated"


class BrokerNetworkMode(str, Enum):
    OFF = "off"
    LOOPBACK = "declared_loopback"
    PROVIDER_OWNED = "trusted_provider_owned"


def _atom(value: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,127}", value):
        refuse()


def _identity(value: str, prefix: str) -> None:
    if not re.fullmatch(re.escape(prefix) + r":sha256:[a-f0-9]{64}", value):
        refuse()


def _wire(value: object, budget: list[int], depth: int = 0) -> object:
    if depth > 24:
        refuse()
    if isinstance(value, _Artifact):
        return _wire(value.to_dict(), budget, depth + 1)
    if isinstance(value, Enum):
        return _wire(value.value, budget, depth)
    if value is None or type(value) in (str, bool, int, float):
        if type(value) is str and len(value) > 65_536:
            refuse()
        if type(value) is int and not -(2**63) <= value < 2**63:
            refuse()
        if type(value) is float and not math.isfinite(value):
            refuse()
        budget[0] -= len(json.dumps(value, ensure_ascii=True))
        if budget[0] < 0:
            refuse()
        return value
    if type(value) in (tuple, list, dict):
        if len(cast(Any, value)) > 128:
            refuse()
        budget[0] -= 2 + len(cast(Any, value))
        if budget[0] < 0:
            refuse()
        if type(value) is dict:
            result: dict[str, object] = {}
            for key, item in value.items():
                if type(key) is not str or len(key) > 128:
                    refuse()
                _wire(key, budget, depth + 1)
                result[key] = _wire(item, budget, depth + 1)
            return result
        return [_wire(item, budget, depth + 1) for item in cast(Any, value)]
    refuse()


def canonical_security_json(value: object) -> str:
    try:
        checked = _wire(value, [MAX_SECURITY_BYTES])
        result = json.dumps(
            checked, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        if len(result) > MAX_SECURITY_BYTES:
            refuse()
        return result
    except (Exception, RecursionError):
        refuse()


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            refuse()
        result[key] = value
    return result


def _decode(annotation: object, value: object) -> Any:
    if get_origin(annotation) is tuple:
        if type(value) is not list:
            refuse()
        return tuple(_decode(get_args(annotation)[0], item) for item in value)
    if annotation in (str, int, bool):
        if type(value) is not annotation:
            refuse()
        return value
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation(value)
    if isinstance(annotation, type) and issubclass(annotation, _Artifact):
        if type(value) is not dict:
            refuse()
        return annotation.from_dict(value)
    refuse()


class _Artifact:
    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        try:
            hints = get_type_hints(type(self))
            for field in fields(cast(Any, self)):
                value = _wire(getattr(self, field.name), [MAX_SECURITY_BYTES])
                object.__setattr__(
                    self, field.name, _decode(hints[field.name], value)
                )
            self._validate()
            self.to_json()
        except Exception:
            refuse()

    def _validate(self) -> None:
        raise NotImplementedError

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": "histdatacom.broker-security."
            + self.KIND
            + ".v1",
            **{
                field.name: _wire(
                    getattr(self, field.name), [MAX_SECURITY_BYTES]
                )
                for field in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        digest = hashlib.sha256(
            canonical_security_json(self._payload()).encode("ascii")
        ).hexdigest()
        return "broker-security-" + self.KIND + ":sha256:" + digest

    def to_dict(self) -> dict[str, object]:
        return {**self._payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        return canonical_security_json(self.to_dict())

    @classmethod
    def from_dict(cls: type[_T], value: dict[str, object]) -> _T:
        try:
            canonical_security_json(value)
            hints = get_type_hints(cls)
            result = cls(
                **{
                    field.name: _decode(hints[field.name], value[field.name])
                    for field in fields(cast(Any, cls))
                }
            )
            if result.to_dict() != value:
                refuse()
            return result
        except Exception:
            refuse()

    @classmethod
    def from_json(cls: type[_T], text: str) -> _T:
        try:
            if type(text) is not str or len(text) > MAX_SECURITY_BYTES:
                refuse()
            value = json.loads(text, object_pairs_hook=_pairs)
            if type(value) is not dict:
                refuse()
            return cls.from_dict(value)
        except Exception:
            refuse()


@dataclass(frozen=True, slots=True)
class BrokerSecurityPolicyV1(_Artifact):
    candidate_id: str
    trust: BrokerTrustTier
    mode: BrokerSecurityMode
    network: BrokerNetworkMode = BrokerNetworkMode.OFF
    loopback_ports: tuple[int, ...] = ()
    secret_fields: tuple[str, ...] = ()
    transport_owner: str = "plugin"
    tls_owner: str = "plugin"
    proxy_owner: str = "none"
    KIND: ClassVar[str] = "policy"

    def _validate(self) -> None:
        _identity(self.candidate_id, "broker-plugin-candidate")
        if (
            self.mode is BrokerSecurityMode.TRUSTED_IN_PROCESS
            and self.trust is BrokerTrustTier.DEVELOPMENT
        ):
            refuse()
        if (
            self.network is BrokerNetworkMode.PROVIDER_OWNED
            and self.mode is not BrokerSecurityMode.TRUSTED_IN_PROCESS
        ):
            refuse()
        if tuple(
            sorted(set(self.loopback_ports))
        ) != self.loopback_ports or any(
            type(port) is not int or not 1 <= port <= 65535
            for port in self.loopback_ports
        ):
            refuse()
        if bool(self.loopback_ports) != (
            self.network is BrokerNetworkMode.LOOPBACK
        ):
            refuse()
        if tuple(sorted(set(self.secret_fields))) != self.secret_fields or any(
            not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name)
            for name in self.secret_fields
        ):
            refuse()
        if (
            self.transport_owner != "plugin"
            or self.tls_owner not in ("plugin", "caller_proxy")
            or self.proxy_owner not in ("none", "caller")
        ):
            refuse()
        if self.tls_owner == "caller_proxy" and self.proxy_owner != "caller":
            refuse()


@dataclass(frozen=True, slots=True)
class BrokerSoftwareProvenanceV1(_Artifact):
    candidate_id: str
    distribution_name: str
    distribution_version: str
    sdk_version: str
    registration_sha256: str
    implementation_sha256: str
    installer: str = "unknown"
    origin_kind: str = "index_or_unknown"
    vcs: str = "none"
    source_commit: str = ""
    archive_sha256: str = ""
    attestation_sha256: str = ""
    attestation_verified: bool = False
    KIND: ClassVar[str] = "software"

    def _validate(self) -> None:
        _identity(self.candidate_id, "broker-plugin-candidate")
        _atom(self.distribution_name)
        if not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127}", self.distribution_version
        ):
            refuse()
        if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", self.sdk_version):
            refuse()
        for name in (
            "registration_sha256",
            "implementation_sha256",
            "source_commit",
            "archive_sha256",
            "attestation_sha256",
        ):
            value = getattr(self, name)
            pattern = (
                r"[a-f0-9]{40}|[a-f0-9]{64}"
                if name == "source_commit"
                else r"[a-f0-9]{64}"
            )
            if value and not re.fullmatch(pattern, value):
                refuse()
        if (
            len(self.registration_sha256) != 64
            or len(self.implementation_sha256) != 64
        ):
            refuse()
        if (
            self.installer not in ("pip", "uv", "other", "unknown")
            or self.origin_kind
            not in ("index_or_unknown", "archive", "vcs", "local_directory")
            or self.vcs not in ("none", "git", "hg", "svn", "bzr", "other")
            or self.attestation_verified
        ):
            refuse()


@dataclass(frozen=True, slots=True)
class BrokerSecurityReceiptV1(_Artifact):
    policy: BrokerSecurityPolicyV1
    software: BrokerSoftwareProvenanceV1
    native_manifest_id: str
    public_configuration_json: str
    backend: str
    publication_api_allowed: bool = False
    provider_tls_verified_by_host: bool = False
    scientific_admission: bool = False
    KIND: ClassVar[str] = "receipt"

    def _validate(self) -> None:
        if self.policy.candidate_id != self.software.candidate_id:
            refuse()
        _identity(self.native_manifest_id, "broker-lifecycle-manifest")
        value = json.loads(
            self.public_configuration_json, object_pairs_hook=_pairs
        )
        if (
            type(value) is not dict
            or canonical_security_json(value) != self.public_configuration_json
            or set(self.policy.secret_fields).intersection(value)
        ):
            refuse()
        if (
            self.policy.mode is not BrokerSecurityMode.KERNEL_ISOLATED
            or self.backend != "macos-seatbelt-v1"
            or self.publication_api_allowed
            or self.provider_tls_verified_by_host
            or self.scientific_admission
        ):
            refuse()


@dataclass(frozen=True, slots=True)
class BrokerTrustedSecurityReceiptV1(_Artifact):
    """Finite native events under explicit same-process trust, not isolation."""

    policy: BrokerSecurityPolicyV1
    software: BrokerSoftwareProvenanceV1
    plan_json: str
    metadata_json: str
    invocation_binding_json: str
    session_json: str
    events_json: tuple[str, ...]
    public_configuration_json: str
    KIND: ClassVar[str] = "trusted-receipt"

    def _validate(self) -> None:
        from histdatacom.broker_plugin_capabilities import (
            BrokerAdmittedEventV1,
            BrokerAdmittedMetadataV1,
            BrokerCapabilityPlanV1,
            BrokerInvocationBindingV1,
            BrokerInvocationAssociation,
            validate_broker_metadata,
            verify_broker_admitted_event,
        )
        from histdatacom.broker_plugins import (
            BrokerSessionV1,
            validate_broker_event_stream,
        )

        plan = BrokerCapabilityPlanV1.from_json(self.plan_json)
        metadata = BrokerAdmittedMetadataV1.from_json(self.metadata_json)
        binding = BrokerInvocationBindingV1.from_json(
            self.invocation_binding_json
        )
        session = BrokerSessionV1.from_json(self.session_json)
        if (
            self.policy.mode is not BrokerSecurityMode.TRUSTED_IN_PROCESS
            or self.policy.candidate_id != self.software.candidate_id
            or plan.candidate.artifact_id != self.policy.candidate_id
            or session.metadata_id != metadata.metadata.artifact_id
            or metadata.to_json()
            != validate_broker_metadata(plan, metadata.metadata).to_json()
            or binding.plan_id != plan.artifact_id
            or binding.candidate_id != self.policy.candidate_id
            or binding.metadata_id != metadata.metadata.artifact_id
            or binding.association
            is not BrokerInvocationAssociation.INSTALLED_ENTRYPOINT
            or binding.module_sha256 != self.software.implementation_sha256
            or self.software.registration_sha256
            != plan.candidate.registration_sha256
            or self.software.implementation_sha256
            != plan.candidate.implementation_sha256
            or self.software.distribution_name
            != plan.candidate.registration.distribution_name
            or self.software.distribution_version
            != plan.candidate.registration.distribution_version
            or self.software.sdk_version != metadata.metadata.sdk_version
        ):
            refuse()
        events = tuple(
            BrokerAdmittedEventV1.from_json(item) for item in self.events_json
        )
        for event in events:
            verify_broker_admitted_event(event, plan)
            if event.event.session_id != session.artifact_id:
                refuse()
        tuple(
            validate_broker_event_stream(
                (event.event for event in events), session
            )
        )
        value = json.loads(
            self.public_configuration_json, object_pairs_hook=_pairs
        )
        if (
            type(value) is not dict
            or canonical_security_json(value) != self.public_configuration_json
            or set(self.policy.secret_fields).intersection(value)
        ):
            refuse()

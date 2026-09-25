"""Closed nonsecret declarations; possessing their bytes grants no resources."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar
from urllib.parse import urlsplit

from ._wire import Artifact, Record, identifier, ordered

PERMISSION_RESOURCE_ABI = "host_resources_v1"
PERMISSION_NONCLAIM = (
    "host-mediated resource permissions are not provider rights, scientific "
    "qualification, or confinement of trusted in-process Python"
)
_NAME = r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*"
_SEMVER = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
_EMISSIONS = frozenset(
    {"emit:quotes", "emit:health", "emit:sizes", "raw_payload:emit"}
)


def permission_name(value: str) -> None:
    """An opaque name is neither a path, URL, credential nor wildcard."""
    if (
        type(value) is not str
        or len(value) > 128
        or not re.fullmatch(_NAME, value)
    ):
        raise ValueError("invalid broker permission name")


def validate_permission_atom(atom: str) -> None:
    """Closed atom grammar: no generic filesystem, store or secret authority."""
    if type(atom) is not str or len(atom) > 180:
        raise ValueError("invalid broker permission atom")
    if atom in _EMISSIONS or atom == "subprocess:requested":
        return
    for prefix in ("network:provider:", "secrets:read:", "cache:plugin:"):
        if atom.startswith(prefix):
            permission_name(atom[len(prefix) :])
            return
    raise ValueError("unknown broker permission atom")


def _atoms(values: tuple[str, ...]) -> None:
    ordered(values)
    if len(values) > 128:
        raise ValueError("broker permission atom inventory bound")
    for value in values:
        validate_permission_atom(value)


def _native_id(value: str, kind: str) -> None:
    if re.fullmatch(re.escape(kind) + r":sha256:[0-9a-f]{64}", value) is None:
        raise ValueError("invalid broker permission native identity")


def _ns(value: int) -> None:
    if not 0 <= value < 2**63:
        raise ValueError("invalid broker permission clock")


class BrokerPermissionReason(str, Enum):
    ADMITTED = "admitted"
    BINDING = "permission_binding_mismatch"
    MISSING_GRANT = "permission_grant_missing"
    NOT_YET_VALID = "permission_grant_not_yet_valid"
    EXPIRED = "permission_grant_expired"
    REVOKED = "permission_grant_revoked"
    REQUIRED_DENIED = "required_permission_denied"
    RESOURCE_DENIED = "resource_permission_denied"


@dataclass(frozen=True, slots=True)
class BrokerPermissionEndpointV1(Record):
    endpoint_id: str
    provider_id: str
    origin: str
    path_prefix: str
    methods: tuple[str, ...]
    max_request_bytes: int
    max_response_bytes: int
    timeout_ms: int
    secret_profiles: tuple[str, ...] = ()

    def _validate(self) -> None:
        permission_name(self.endpoint_id)
        permission_name(self.provider_id)
        if not self.origin.isascii() or len(self.origin) > 512:
            raise ValueError("invalid permission endpoint origin")
        try:
            parsed = urlsplit(self.origin)
            port = parsed.port
        except ValueError:
            raise ValueError("invalid permission endpoint origin") from None
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or self.origin != self.origin.lower()
            or any(char.isspace() for char in self.origin)
            or "\\" in self.origin
            or "%" in self.origin
            or (port is not None and not 1 <= port <= 65535)
            or (
                parsed.scheme == "http"
                and parsed.hostname not in ("127.0.0.1", "::1")
            )
        ):
            raise ValueError("invalid permission endpoint origin")
        # Slash-terminated segment prefixes cannot confuse /api/ with /api-evil.
        if (
            not self.path_prefix.startswith("/")
            or not self.path_prefix.endswith("/")
            or len(self.path_prefix) > 1024
            or not self.path_prefix.isascii()
            or any(c in self.path_prefix for c in ("%", "?", "#", "\\"))
            or any(ord(c) < 33 or ord(c) == 127 for c in self.path_prefix)
            or "//" in self.path_prefix
            or any(p in (".", "..") for p in self.path_prefix.split("/"))
        ):
            raise ValueError("invalid permission endpoint path prefix")
        ordered(self.methods, nonempty=True)
        if not set(self.methods) <= {
            "GET",
            "HEAD",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
        }:
            raise ValueError("unsupported permission endpoint method")
        if (
            not 0 <= self.max_request_bytes <= 1_048_576
            or not 1 <= self.max_response_bytes <= 1_048_576
            or not 1 <= self.timeout_ms <= 60_000
        ):
            raise ValueError("permission endpoint resource bound")
        ordered(self.secret_profiles)
        for name in self.secret_profiles:
            permission_name(name)


@dataclass(frozen=True, slots=True)
class BrokerPermissionCacheV1(Record):
    cache_id: str
    max_bytes: int
    max_items: int
    max_item_bytes: int

    def _validate(self) -> None:
        permission_name(self.cache_id)
        if (
            not 1 <= self.max_bytes <= 1_048_576
            or not 1 <= self.max_items <= 1024
            or not 1 <= self.max_item_bytes <= self.max_bytes
        ):
            raise ValueError("permission cache resource bound")


@dataclass(frozen=True, slots=True)
class BrokerPermissionManifestV1(Artifact):
    KIND: ClassVar[str] = "manifest"
    candidate_id: str
    distribution_name: str
    distribution_version: str
    sdk_version: str
    provider_ids: tuple[str, ...]
    required_atoms: tuple[str, ...]
    optional_atoms: tuple[str, ...] = ()
    resource_abi: str = PERMISSION_RESOURCE_ABI
    endpoints: tuple[BrokerPermissionEndpointV1, ...] = ()
    secret_profiles: tuple[str, ...] = ()
    caches: tuple[BrokerPermissionCacheV1, ...] = ()
    subprocess_mode: str = "none"

    def _validate(self) -> None:
        _native_id(self.candidate_id, "broker-plugin-candidate")
        if not re.fullmatch(
            r"[a-z0-9]+(?:-[a-z0-9]+)*", self.distribution_name
        ):
            raise ValueError("permission distribution name is not normalized")
        if len(self.distribution_name) > 128 or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127}", self.distribution_version
        ):
            raise ValueError("invalid permission distribution identity")
        if not re.fullmatch(_SEMVER, self.sdk_version):
            raise ValueError("invalid permission SDK version")
        ordered(self.provider_ids, nonempty=True)
        for provider in self.provider_ids:
            permission_name(provider)
        _atoms(self.required_atoms)
        _atoms(self.optional_atoms)
        if set(self.required_atoms) & set(self.optional_atoms):
            raise ValueError("required and optional permission atoms overlap")
        declared = set(self.required_atoms) | set(self.optional_atoms)
        if self.resource_abi not in (PERMISSION_RESOURCE_ABI, "none"):
            raise ValueError("unsupported broker resource extension ABI")
        if self.resource_abi == "none" and declared - _EMISSIONS:
            raise ValueError(
                "resources require the explicit host extension ABI"
            )
        if self.subprocess_mode not in ("none", "isolated"):
            raise ValueError("unsupported subprocess permission mode")
        if ("subprocess:requested" in declared) != (
            self.subprocess_mode == "isolated"
        ):
            raise ValueError("subprocess declaration and mode differ")
        if len(self.endpoints) > 128 or len(self.caches) > 128:
            raise ValueError("permission resource inventory bound")
        for values, key in (
            (self.endpoints, "endpoint_id"),
            (self.caches, "cache_id"),
        ):
            names = tuple(getattr(item, key) for item in values)
            ordered(names)
        network = {
            "network:provider:" + item.provider_id for item in self.endpoints
        }
        if network != {a for a in declared if a.startswith("network:")}:
            raise ValueError(
                "network permissions require exact endpoint inventory"
            )
        if any(
            item.provider_id not in self.provider_ids for item in self.endpoints
        ):
            raise ValueError("endpoint provider is outside manifest inventory")
        ordered(self.secret_profiles)
        for profile in self.secret_profiles:
            permission_name(profile)
        if {"secrets:read:" + p for p in self.secret_profiles} != {
            a for a in declared if a.startswith("secrets:")
        }:
            raise ValueError(
                "secret permissions require exact opaque profile inventory"
            )
        endpoint_profiles = {
            p for item in self.endpoints for p in item.secret_profiles
        }
        if endpoint_profiles != set(self.secret_profiles):
            raise ValueError(
                "opaque secrets must be bound to declared host transport"
            )
        if {"cache:plugin:" + item.cache_id for item in self.caches} != {
            a for a in declared if a.startswith("cache:")
        }:
            raise ValueError(
                "cache permissions require exact opaque cache inventory"
            )

    @property
    def declared_atoms(self) -> tuple[str, ...]:
        return tuple(sorted(self.required_atoms + self.optional_atoms))


@dataclass(frozen=True, slots=True)
class BrokerPermissionBindingV1(Artifact):
    KIND: ClassVar[str] = "binding"
    candidate_id: str
    manifest_id: str
    sdk_version: str
    provider_id: str
    configuration_id: str

    def _validate(self) -> None:
        _native_id(self.candidate_id, "broker-plugin-candidate")
        identifier(self.manifest_id, "manifest")
        if not re.fullmatch(_SEMVER, self.sdk_version):
            raise ValueError("invalid permission SDK version")
        permission_name(self.provider_id)
        _native_id(
            self.configuration_id,
            "broker-provider-policy-provider-configuration",
        )


@dataclass(frozen=True, slots=True)
class BrokerPermissionGrantV1(Artifact):
    KIND: ClassVar[str] = "grant"
    binding: BrokerPermissionBindingV1
    granted_atoms: tuple[str, ...]
    operator_id: str
    issued_at_ns: int
    expires_at_ns: int
    nonce: str

    def _validate(self) -> None:
        _atoms(self.granted_atoms)
        permission_name(self.operator_id)
        _ns(self.issued_at_ns)
        _ns(self.expires_at_ns)
        if self.expires_at_ns <= self.issued_at_ns:
            raise ValueError("permission grant interval must be nonempty")
        if not re.fullmatch(r"[0-9a-f]{32}", self.nonce):
            raise ValueError("invalid permission grant nonce")


@dataclass(frozen=True, slots=True)
class BrokerPermissionRevocationV1(Artifact):
    KIND: ClassVar[str] = "revocation"
    grant_id: str
    revoked_at_ns: int
    reason: str

    def _validate(self) -> None:
        identifier(self.grant_id, "grant")
        _ns(self.revoked_at_ns)
        permission_name(self.reason)


@dataclass(frozen=True, slots=True)
class BrokerPermissionContextV1(Artifact):
    KIND: ClassVar[str] = "context"
    grants: tuple[BrokerPermissionGrantV1, ...]
    revocations: tuple[BrokerPermissionRevocationV1, ...] = ()
    revision: int = 0

    def _validate(self) -> None:
        _ns(self.revision)
        for values in (self.grants, self.revocations):
            ordered(tuple(item.artifact_id for item in values))
            if len(values) > 128:
                raise ValueError("permission ledger bound")
        grants = {item.artifact_id: item for item in self.grants}
        revoked = [item.grant_id for item in self.revocations]
        if len(revoked) != len(set(revoked)):
            raise ValueError(
                "permission grant has ambiguous revocation records"
            )
        for item in self.revocations:
            if (
                item.grant_id not in grants
                or item.revoked_at_ns < grants[item.grant_id].issued_at_ns
            ):
                raise ValueError(
                    "permission revocation lacks prior retained grant"
                )


@dataclass(frozen=True, slots=True)
class BrokerPermissionDecisionV1(Artifact):
    KIND: ClassVar[str] = "decision"
    binding: BrokerPermissionBindingV1
    grant_id: str
    context_id: str
    at_ns: int
    admitted: bool
    reason: BrokerPermissionReason
    effective_atoms: tuple[str, ...]
    denied_optional_atoms: tuple[str, ...]
    missing_required_atoms: tuple[str, ...]

    def _validate(self) -> None:
        identifier(self.grant_id, "grant")
        identifier(self.context_id, "context")
        _ns(self.at_ns)
        for atoms in (
            self.effective_atoms,
            self.denied_optional_atoms,
            self.missing_required_atoms,
        ):
            _atoms(atoms)
        if self.admitted != (self.reason is BrokerPermissionReason.ADMITTED):
            raise ValueError("permission decision state contradicts reason")
        if (not self.admitted and self.effective_atoms) or (
            self.admitted and self.missing_required_atoms
        ):
            raise ValueError("refused permission decision contains authority")
        if set(self.effective_atoms) & set(self.denied_optional_atoms):
            raise ValueError("permission decision atom states overlap")

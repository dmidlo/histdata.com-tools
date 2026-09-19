"""Immutable declarations for metadata-only installed plugin inspection."""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
import hashlib
import json
import re
from typing import ClassVar, NoReturn, TypeVar, cast
from urllib.parse import urlsplit

from histdatacom.broker_plugins import (
    BROKER_PLUGIN_SDK_VERSION,
    BrokerPluginMetadataV1,
)

BROKER_PLUGIN_ENTRY_POINT_GROUP = "histdatacom.broker_plugins.v1"
MAX_PLUGIN_REGISTRATIONS = 128
MAX_REGISTRATION_BYTES = 65_536
MAX_INVENTORY_BYTES = 262_144
_ID = re.compile(r"[a-z][a-z0-9]*(?:[.-][a-z][a-z0-9_]*)+\Z")
_ATOM = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\Z")
_DIST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?\Z")
_ENTRY = re.compile(
    r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*" r"(?:\.[A-Za-z_]\w*)*\Z",
    re.ASCII,
)
_SHA = re.compile(r"[a-f0-9]{64}\Z")
_ArtifactT = TypeVar("_ArtifactT", bound="_Artifact")


class BrokerPluginRegistryReason(str, Enum):
    INVALID_REGISTRATION = "invalid_registration"
    INVALID_INVENTORY = "invalid_inventory"
    INVALID_SELECTION = "invalid_selection"
    INVALID_BINDING = "invalid_binding"
    METADATA_UNAVAILABLE = "metadata_unavailable"
    PACKAGE_MISMATCH = "package_mismatch"
    RESOURCE_UNAVAILABLE = "resource_unavailable"
    RESOURCE_INTEGRITY = "resource_integrity"
    INVENTORY_LIMIT = "inventory_limit"
    INCOMPLETE_INVENTORY = "incomplete_inventory"
    UNKNOWN_PLUGIN = "unknown_plugin"
    INCOMPATIBLE_SDK = "incompatible_sdk"
    AMBIGUOUS_SELECTION = "ambiguous_selection"
    PERSISTENCE_FAILURE = "persistence_failure"


class BrokerPluginRegistryError(ValueError):
    """Closed reason codes, without exception dumps or unvalidated input."""

    def __init__(
        self,
        reason: BrokerPluginRegistryReason,
        candidates: tuple[str, ...] = (),
    ) -> None:
        if (
            type(reason) is not BrokerPluginRegistryReason
            or type(candidates) is not tuple
            or len(candidates) > MAX_PLUGIN_REGISTRATIONS
            or any(
                type(item) is not str
                or len(item) > 128
                or not re.fullmatch(
                    r"broker-plugin-candidate:sha256:[a-f0-9]{64}", item
                )
                for item in candidates
            )
        ):
            raise ValueError("invalid registry diagnostic")
        self.reason = reason
        self.candidates = candidates
        super().__init__(reason.value)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "refused",
            "reason": self.reason.value,
            "candidate_ids": list(self.candidates),
        }


def _fail(reason: BrokerPluginRegistryReason) -> NoReturn:
    raise BrokerPluginRegistryError(reason)


def _text(value: object, maximum: int = 128) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or value != value.strip()
        or not value.isascii()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
    return value


def normalized_distribution_name(value: str) -> str:
    value = _text(value)
    if not _DIST.fullmatch(value):
        _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
    return re.sub(r"[-_.]+", "-", value).lower()


def _identifier(value: str) -> str:
    if not _ID.fullmatch(_text(value)):
        _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
    return value


def _sha(value: str) -> str:
    if not _SHA.fullmatch(_text(value, 64)):
        _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
    return value


def _ids(values: tuple[str, ...], *, namespaced: bool = False) -> None:
    if type(values) is not tuple or len(values) > MAX_PLUGIN_REGISTRATIONS:
        _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
    for value in values:
        if namespaced:
            _identifier(value)
        elif not _ATOM.fullmatch(_text(value)):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
    if tuple(sorted(set(values))) != values:
        _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)


def semver_key(value: str) -> tuple[int, int, int, tuple[tuple[int, str], ...]]:
    """SemVer precedence; build labels do not affect range comparison."""
    try:
        BrokerPluginMetadataV1("org.histdatacom.version", value, "Version")
    except (TypeError, ValueError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_REGISTRATION
        ) from None
    base, _, _build = value.partition("+")
    core, separator, pre = base.partition("-")
    major, minor, patch = (int(part) for part in core.split("."))
    # Numeric identifiers sort below text and numerically (length then digits).
    prerelease = (
        tuple(
            (0, f"{len(part):03d}:{part}") if part.isdigit() else (1, part)
            for part in pre.split(".")
        )
        if separator
        else ((2, ""),)
    )
    return major, minor, patch, prerelease


def _json_check(value: object, depth: int = 0) -> None:
    if depth > 12:
        _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
    if value is None or type(value) in (bool, int, str):
        if type(value) is str and len(value) > MAX_REGISTRATION_BYTES:
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        if type(value) is int and not -(2**63) <= value < 2**63:
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        return
    if type(value) is list:
        if len(value) > MAX_PLUGIN_REGISTRATIONS:
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        for item in value:
            _json_check(item, depth + 1)
        return
    if type(value) is dict:
        if len(value) > MAX_PLUGIN_REGISTRATIONS:
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        for key, item in value.items():
            if type(key) is not str or len(key) > 128:
                _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
            _json_check(item, depth + 1)
        return
    _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)


def canonical_plugin_registry_json(value: object) -> str:
    _json_check(value)
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    if len(encoded) > MAX_INVENTORY_BYTES:
        _fail(BrokerPluginRegistryReason.INVENTORY_LIMIT)
    return encoded


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        result[key] = value
    return result


def parse_registry_json(
    text: str, limit: int = MAX_INVENTORY_BYTES
) -> dict[str, object]:
    try:
        if type(text) is not str or len(text.encode("utf-8")) > limit:
            _fail(BrokerPluginRegistryReason.INVENTORY_LIMIT)
        result = json.loads(text, object_pairs_hook=_pairs)
        _json_check(result)
        if type(result) is not dict:
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        return cast(dict[str, object], result)
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_INVENTORY
        ) from None


def _dict(value: object) -> dict[str, object]:
    if type(value) is not dict:
        _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
    return cast(dict[str, object], value)


def _tuple(value: object) -> tuple[str, ...]:
    if type(value) is not list or len(value) > MAX_PLUGIN_REGISTRATIONS:
        _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
    if not all(type(item) is str for item in value):
        _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
    return tuple(value)


class _Artifact:
    __slots__ = ()
    KIND: ClassVar[str]

    def _payload(self) -> dict[str, object]:
        raise NotImplementedError

    @property
    def schema_version(self) -> str:
        return f"histdatacom.broker-plugin-{self.KIND}.v1"

    @property
    def artifact_id(self) -> str:
        digest = hashlib.sha256(
            canonical_plugin_registry_json(self._payload()).encode("ascii")
        ).hexdigest()
        return f"broker-plugin-{self.KIND}:sha256:{digest}"

    def to_dict(self) -> dict[str, object]:
        return {**self._payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        return canonical_plugin_registry_json(self.to_dict())

    def _bound(self) -> None:
        self.to_json()

    @classmethod
    def from_json(cls: type[_ArtifactT], text: str) -> _ArtifactT:
        return cls.from_dict(parse_registry_json(text))

    @classmethod
    def from_dict(
        cls: type[_ArtifactT], value: dict[str, object]
    ) -> _ArtifactT:
        raise NotImplementedError

    def _check_payload(self, value: dict[str, object]) -> None:
        if self.to_json() != canonical_plugin_registry_json(value):
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)


@dataclass(frozen=True, slots=True)
class BrokerPluginRegistrationV1(_Artifact):
    """Declarative wheel resource, not executable plugin metadata."""

    plugin_id: str
    plugin_version: str
    display_name: str
    distribution_name: str
    distribution_version: str
    entry_point: str
    sdk_min_version: str
    sdk_max_version: str
    provider_ids: tuple[str, ...]
    capabilities: tuple[str, ...]
    source_revision: str = ""
    source_repository: str = ""
    build_id: str = ""
    KIND: ClassVar[str] = "registration"

    def __post_init__(self) -> None:
        if any(
            type(value) is not str
            for value in (
                self.source_revision,
                self.source_repository,
                self.build_id,
            )
        ):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        _identifier(self.plugin_id)
        semver_key(self.plugin_version)
        try:
            BrokerPluginMetadataV1(
                self.plugin_id, self.plugin_version, self.display_name
            )
        except (ValueError, TypeError):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        object.__setattr__(
            self,
            "distribution_name",
            normalized_distribution_name(self.distribution_name),
        )
        if not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9.!+_-]*", _text(self.distribution_version)
        ):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        if not _ENTRY.fullmatch(_text(self.entry_point, 256)):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        if semver_key(self.sdk_min_version) >= semver_key(self.sdk_max_version):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        _ids(self.provider_ids)
        _ids(self.capabilities)
        if not self.provider_ids:
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        if self.source_revision and not re.fullmatch(
            r"[a-f0-9]{7,64}", _text(self.source_revision, 64)
        ):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        if self.build_id and not _ATOM.fullmatch(_text(self.build_id)):
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        if self.source_repository:
            try:
                url = urlsplit(_text(self.source_repository, 512))
                # Accessing port also validates its syntax and numeric range.
                _ = url.port
            except ValueError:
                raise BrokerPluginRegistryError(
                    BrokerPluginRegistryReason.INVALID_REGISTRATION
                ) from None
            if (
                url.scheme != "https"
                or not url.hostname
                or url.username is not None
                or url.password is not None
                or url.query
                or url.fragment
                or " " in self.source_repository
                or "\\" in self.source_repository
            ):
                _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        self._bound()

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **{
                field.name: getattr(self, field.name)
                for field in fields(self)
                if field.name not in {"provider_ids", "capabilities"}
            },
            "provider_ids": list(self.provider_ids),
            "capabilities": list(self.capabilities),
        }

    def supports_sdk(self, version: str = BROKER_PLUGIN_SDK_VERSION) -> bool:
        return (
            semver_key(self.sdk_min_version)
            <= semver_key(version)
            < semver_key(self.sdk_max_version)
        )

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> BrokerPluginRegistrationV1:
        try:
            obj = cls(
                plugin_id=cast(str, value["plugin_id"]),
                plugin_version=cast(str, value["plugin_version"]),
                display_name=cast(str, value["display_name"]),
                distribution_name=cast(str, value["distribution_name"]),
                distribution_version=cast(str, value["distribution_version"]),
                entry_point=cast(str, value["entry_point"]),
                sdk_min_version=cast(str, value["sdk_min_version"]),
                sdk_max_version=cast(str, value["sdk_max_version"]),
                provider_ids=_tuple(value["provider_ids"]),
                capabilities=_tuple(value["capabilities"]),
                source_revision=cast(str, value["source_revision"]),
                source_repository=cast(str, value["source_repository"]),
                build_id=cast(str, value["build_id"]),
            )
            obj._check_payload(value)
            return obj
        except (KeyError, ValueError, TypeError, AttributeError):
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_REGISTRATION
            ) from None


@dataclass(frozen=True, slots=True)
class BrokerPluginCandidateV1(_Artifact):
    registration: BrokerPluginRegistrationV1
    registration_sha256: str
    implementation_sha256: str
    KIND: ClassVar[str] = "candidate"

    def __post_init__(self) -> None:
        if type(self.registration) is not BrokerPluginRegistrationV1:
            _fail(BrokerPluginRegistryReason.INVALID_REGISTRATION)
        _sha(self.registration_sha256)
        _sha(self.implementation_sha256)
        self._bound()

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registration": self.registration.to_dict(),
            "registration_sha256": self.registration_sha256,
            "implementation_sha256": self.implementation_sha256,
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> BrokerPluginCandidateV1:
        try:
            obj = cls(
                BrokerPluginRegistrationV1.from_dict(
                    _dict(value["registration"])
                ),
                cast(str, value["registration_sha256"]),
                cast(str, value["implementation_sha256"]),
            )
            obj._check_payload(value)
            return obj
        except (KeyError, TypeError, ValueError):
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_INVENTORY
            ) from None


@dataclass(frozen=True, slots=True)
class BrokerPluginDiscoveryDiagnosticV1:
    reason: BrokerPluginRegistryReason
    distribution_name: str = ""
    plugin_id: str = ""

    def __post_init__(self) -> None:
        if (
            type(self.reason) is not BrokerPluginRegistryReason
            or type(self.distribution_name) is not str
            or type(self.plugin_id) is not str
        ):
            _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        if self.distribution_name:
            if (
                normalized_distribution_name(self.distribution_name)
                != self.distribution_name
            ):
                _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
        if self.plugin_id:
            _identifier(self.plugin_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "reason": self.reason.value,
            "distribution_name": self.distribution_name,
            "plugin_id": self.plugin_id,
        }


def candidate_sort_key(
    candidate: BrokerPluginCandidateV1,
) -> tuple[object, ...]:
    reg = candidate.registration
    return (
        reg.plugin_id,
        semver_key(reg.plugin_version),
        reg.plugin_version,
        reg.distribution_name,
        reg.distribution_version,
        reg.entry_point,
        candidate.artifact_id,
    )


@dataclass(frozen=True, slots=True)
class BrokerPluginInventoryV1(_Artifact):
    candidates: tuple[BrokerPluginCandidateV1, ...] = ()
    diagnostics: tuple[BrokerPluginDiscoveryDiagnosticV1, ...] = ()
    sdk_version: str = BROKER_PLUGIN_SDK_VERSION
    KIND: ClassVar[str] = "inventory"

    def __post_init__(self) -> None:
        for values, kind in (
            (self.candidates, BrokerPluginCandidateV1),
            (self.diagnostics, BrokerPluginDiscoveryDiagnosticV1),
        ):
            if (
                type(values) is not tuple
                or len(values) > MAX_PLUGIN_REGISTRATIONS
                or any(type(v) is not kind for v in values)
            ):
                _fail(BrokerPluginRegistryReason.INVENTORY_LIMIT)
        semver_key(self.sdk_version)
        object.__setattr__(
            self,
            "candidates",
            tuple(sorted(self.candidates, key=candidate_sort_key)),
        )
        object.__setattr__(
            self,
            "diagnostics",
            tuple(
                sorted(
                    self.diagnostics,
                    key=lambda d: (
                        d.distribution_name,
                        d.plugin_id,
                        d.reason.value,
                    ),
                )
            ),
        )
        self._bound()

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "entry_point_group": BROKER_PLUGIN_ENTRY_POINT_GROUP,
            "sdk_version": self.sdk_version,
            "purpose": "installed_software_inventory_not_activation",
            "candidates": [item.to_dict() for item in self.candidates],
            "diagnostics": [item.to_dict() for item in self.diagnostics],
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> BrokerPluginInventoryV1:
        try:
            _json_check(value)
            candidates = value["candidates"]
            diagnostics = value["diagnostics"]
            if type(candidates) is not list or type(diagnostics) is not list:
                _fail(BrokerPluginRegistryReason.INVALID_INVENTORY)
            obj = cls(
                tuple(
                    BrokerPluginCandidateV1.from_dict(_dict(c))
                    for c in candidates
                ),
                tuple(
                    BrokerPluginDiscoveryDiagnosticV1(
                        BrokerPluginRegistryReason(_dict(d)["reason"]),
                        cast(str, _dict(d)["distribution_name"]),
                        cast(str, _dict(d)["plugin_id"]),
                    )
                    for d in diagnostics
                ),
                cast(str, value["sdk_version"]),
            )
            obj._check_payload(value)
            return obj
        except (
            KeyError,
            ValueError,
            TypeError,
            AttributeError,
            RecursionError,
        ):
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_INVENTORY
            ) from None

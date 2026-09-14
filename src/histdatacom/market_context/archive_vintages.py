"""Archive-first governance for reconstructing official release vintages.

The raw acquisition layer retains exact official bytes and parser identities;
the release-vintage layer retains immutable temporal mutations.  This module
binds the two without allowing a latest-only API response to masquerade as a
historical initial value or previous-as-known observation.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from itertools import pairwise
from pathlib import Path
from typing import Any, TypeVar, cast

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicReleaseStage,
)
from histdatacom.market_context.official_sources import (
    MAX_OFFICIAL_EVENTS,
    SCOPED_ECONOMIES,
    OfficialRawSnapshotV1,
    OfficialSourceCapability,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceVerificationStatus,
)
from histdatacom.market_context.release_vintages import (
    EconomicReleaseRevisionKind,
    EconomicReleaseVintageChainV1,
)
from histdatacom.runtime_contracts import JSONValue

ECONOMIC_ARCHIVE_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.economic-archive-artifact.v1"
)
ECONOMIC_ARCHIVE_VINTAGE_BINDING_SCHEMA_VERSION = (
    "histdatacom.economic-archive-vintage-binding.v1"
)
ECONOMIC_ARCHIVE_MIRROR_RESOLUTION_SCHEMA_VERSION = (
    "histdatacom.economic-archive-mirror-resolution.v1"
)
ECONOMIC_ARCHIVE_SOURCE_ERA_SCHEMA_VERSION = (
    "histdatacom.economic-archive-source-era.v1"
)
ECONOMIC_VINTAGE_COVERAGE_DECLARATION_SCHEMA_VERSION = (
    "histdatacom.economic-vintage-coverage-declaration.v1"
)
ECONOMIC_VINTAGE_GAP_SCHEMA_VERSION = "histdatacom.economic-vintage-gap.v1"
ECONOMIC_LATEST_VALUE_CROSS_CHECK_SCHEMA_VERSION = (
    "histdatacom.economic-latest-value-cross-check.v1"
)
ECONOMIC_ARCHIVE_VINTAGE_CORPUS_SCHEMA_VERSION = (
    "histdatacom.economic-archive-vintage-corpus.v1"
)
ECONOMIC_ARCHIVE_VINTAGE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.economic-archive-vintage-audit.v1"
)

MAX_ECONOMIC_ARCHIVE_ARTIFACTS = 250_000
MAX_ECONOMIC_ARCHIVE_BINDINGS = 500_000
MAX_ECONOMIC_ARCHIVE_CORPUS_BYTES = 64 * 1024 * 1024
MAX_ECONOMIC_ARCHIVE_SOURCE_ERAS = 50_000

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ECONOMY_CODE_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")


class EconomicVintageRecoveryMode(str, Enum):
    """How one economy/family adapter can support historical vintages."""

    API_VINTAGE_COMPLETE = "api-vintage-complete"
    ARCHIVE_RECONSTRUCTED = "archive-reconstructed"
    SNAPSHOT_DEPENDENT = "snapshot-dependent"
    HISTORICALLY_INCOMPLETE = "historically-incomplete"

    @classmethod
    def from_value(
        cls, value: str | EconomicVintageRecoveryMode
    ) -> EconomicVintageRecoveryMode:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported vintage recovery mode") from exc


class EconomicArchiveArtifactKind(str, Enum):
    """Evidentiary role of one retained official source artifact."""

    CURRENT_API = "current-api"
    CONTEMPORANEOUS_RELEASE = "contemporaneous-release"
    ARCHIVED_TABLE = "archived-table"
    RETAINED_SNAPSHOT = "retained-snapshot"
    OFFICIAL_MIRROR = "official-mirror"
    ARCHIVE_INDEX = "archive-index"

    @classmethod
    def from_value(
        cls, value: str | EconomicArchiveArtifactKind
    ) -> EconomicArchiveArtifactKind:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError(
                "unsupported economic archive artifact kind"
            ) from exc

    @property
    def supports_value_vintage(self) -> bool:
        return self not in {self.ARCHIVE_INDEX}


class EconomicVintageGapReason(str, Enum):
    """Why an expected historical value could not be reconstructed."""

    MISSING_ARTIFACT = "missing-artifact"
    LATEST_ONLY_UNSUPPORTED = "latest-only-unsupported"
    PARSER_GAP = "parser-gap"
    SOURCE_MIGRATION_GAP = "source-migration-gap"
    UNRESOLVED_MIRROR_CONFLICT = "unresolved-mirror-conflict"

    @classmethod
    def from_value(
        cls, value: str | EconomicVintageGapReason
    ) -> EconomicVintageGapReason:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported economic vintage gap reason") from exc


def _required_text(value: object, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _key(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _KEY_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a canonical key")
    return result


def _economy_code(value: object) -> str:
    result = _required_text(value, "economy_code").upper()
    if _ECONOMY_CODE_RE.fullmatch(result) is None:
        raise ValueError("economy_code is invalid")
    return result


def _bounded_ns(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _optional_ns(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _bounded_ns(value, name)


def _bounded_count(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return result


def _identity(value: object, prefix: str, name: str) -> str:
    result = _required_text(value, name)
    if (
        re.fullmatch(rf"{re.escape(prefix)}:sha256:[0-9a-f]{{64}}", result)
        is None
    ):
        raise ValueError(f"{name} is not a {prefix} identity")
    return result


def _texts(
    values: Iterable[object], name: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, name) for item in values}))
    if required and not result:
        raise ValueError(f"{name} must not be empty")
    if len(result) > 256:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _identities(
    values: Iterable[object], prefix: str, name: str
) -> tuple[str, ...]:
    result = tuple(sorted({_identity(item, prefix, name) for item in values}))
    if len(result) > MAX_ECONOMIC_ARCHIVE_ARTIFACTS:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


@dataclass(frozen=True, slots=True)
class EconomicArchiveArtifactV1:
    """Content-addressed official artifact used during vintage recovery."""

    snapshot_id: str
    request_id: str
    source_key: str
    source_id: str
    source_format: OfficialSourceFormat
    content_sha256: str
    retrieved_at_ns: int
    parser_id: str
    parser_version: str
    source_locator: str
    artifact_kind: EconomicArchiveArtifactKind
    limitations: tuple[str, ...]
    artifact_id: str = ""
    schema_version: str = ECONOMIC_ARCHIVE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_ARCHIVE_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported economic archive-artifact schema")
        object.__setattr__(
            self,
            "snapshot_id",
            _identity(self.snapshot_id, "official-snapshot", "snapshot_id"),
        )
        object.__setattr__(
            self,
            "request_id",
            _identity(self.request_id, "official-request", "request_id"),
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "source_id",
            _identity(self.source_id, "official-source", "source_id"),
        )
        object.__setattr__(
            self,
            "source_format",
            OfficialSourceFormat.from_value(self.source_format),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        object.__setattr__(
            self,
            "retrieved_at_ns",
            _bounded_ns(self.retrieved_at_ns, "retrieved_at_ns"),
        )
        object.__setattr__(self, "parser_id", _key(self.parser_id, "parser_id"))
        object.__setattr__(
            self,
            "parser_version",
            _required_text(self.parser_version, "parser_version"),
        )
        locator = _required_text(self.source_locator, "source_locator")
        if len(locator) > 4096:
            raise ValueError("source_locator exceeds the text bound")
        object.__setattr__(self, "source_locator", locator)
        object.__setattr__(
            self,
            "artifact_kind",
            EconomicArchiveArtifactKind.from_value(self.artifact_kind),
        )
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-archive-artifact", self.identity_payload()
        )
        supplied = _optional_text(self.artifact_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "artifact_id does not match deterministic identity"
            )
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "request_id": self.request_id,
            "source_key": self.source_key,
            "source_id": self.source_id,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "retrieved_at_ns": self.retrieved_at_ns,
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "source_locator": self.source_locator,
            "artifact_kind": self.artifact_kind.value,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicArchiveArtifactV1:
        return cls(
            snapshot_id=str(data.get("snapshot_id", "")),
            request_id=str(data.get("request_id", "")),
            source_key=str(data.get("source_key", "")),
            source_id=str(data.get("source_id", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            retrieved_at_ns=cast(int, data.get("retrieved_at_ns")),
            parser_id=str(data.get("parser_id", "")),
            parser_version=str(data.get("parser_version", "")),
            source_locator=str(data.get("source_locator", "")),
            artifact_kind=EconomicArchiveArtifactKind.from_value(
                str(data.get("artifact_kind", ""))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicArchiveVintageBindingV1:
    """One parsed record's exact artifact support for a release vintage."""

    release_id: str
    logical_event_key: str
    revision_sequence: int
    artifact_id: str
    source_key: str
    source_id: str
    normalized_record_sha256: str
    record_locator: str
    source_precedence: int
    binding_id: str = ""
    schema_version: str = ECONOMIC_ARCHIVE_VINTAGE_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_ARCHIVE_VINTAGE_BINDING_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported economic archive vintage-binding schema"
            )
        object.__setattr__(
            self,
            "release_id",
            _identity(
                self.release_id, "economic-calendar-release", "release_id"
            ),
        )
        object.__setattr__(
            self,
            "logical_event_key",
            _key(self.logical_event_key, "logical_event_key"),
        )
        object.__setattr__(
            self,
            "revision_sequence",
            _bounded_count(
                self.revision_sequence, "revision_sequence", MAX_OFFICIAL_EVENTS
            ),
        )
        object.__setattr__(
            self,
            "artifact_id",
            _identity(
                self.artifact_id, "economic-archive-artifact", "artifact_id"
            ),
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "source_id",
            _identity(self.source_id, "official-source", "source_id"),
        )
        object.__setattr__(
            self,
            "normalized_record_sha256",
            _sha256(self.normalized_record_sha256, "normalized_record_sha256"),
        )
        locator = _required_text(self.record_locator, "record_locator")
        if len(locator) > 4096:
            raise ValueError("record_locator exceeds the text bound")
        object.__setattr__(self, "record_locator", locator)
        object.__setattr__(
            self,
            "source_precedence",
            _bounded_count(self.source_precedence, "source_precedence", 10_000),
        )
        expected = _stable_id(
            "economic-archive-vintage-binding", self.identity_payload()
        )
        supplied = _optional_text(self.binding_id)
        if supplied is not None and supplied != expected:
            raise ValueError("binding_id does not match deterministic identity")
        object.__setattr__(self, "binding_id", expected)

    @property
    def record_key(self) -> tuple[str, int]:
        return self.logical_event_key, self.revision_sequence

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_id": self.release_id,
            "logical_event_key": self.logical_event_key,
            "revision_sequence": self.revision_sequence,
            "artifact_id": self.artifact_id,
            "source_key": self.source_key,
            "source_id": self.source_id,
            "normalized_record_sha256": self.normalized_record_sha256,
            "record_locator": self.record_locator,
            "source_precedence": self.source_precedence,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "binding_id": self.binding_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicArchiveVintageBindingV1:
        return cls(
            release_id=str(data.get("release_id", "")),
            logical_event_key=str(data.get("logical_event_key", "")),
            revision_sequence=cast(int, data.get("revision_sequence")),
            artifact_id=str(data.get("artifact_id", "")),
            source_key=str(data.get("source_key", "")),
            source_id=str(data.get("source_id", "")),
            normalized_record_sha256=str(
                data.get("normalized_record_sha256", "")
            ),
            record_locator=str(data.get("record_locator", "")),
            source_precedence=cast(int, data.get("source_precedence")),
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicArchiveMirrorResolutionV1:
    """Deterministic hash-and-precedence resolution of official mirrors."""

    logical_event_key: str
    revision_sequence: int
    candidate_binding_ids: tuple[str, ...]
    selected_binding_id: str
    equivalent_binding_ids: tuple[str, ...]
    conflicting_binding_ids: tuple[str, ...]
    resolution_id: str = ""
    schema_version: str = ECONOMIC_ARCHIVE_MIRROR_RESOLUTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_ARCHIVE_MIRROR_RESOLUTION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported economic archive mirror-resolution schema"
            )
        object.__setattr__(
            self,
            "logical_event_key",
            _key(self.logical_event_key, "logical_event_key"),
        )
        object.__setattr__(
            self,
            "revision_sequence",
            _bounded_count(
                self.revision_sequence, "revision_sequence", MAX_OFFICIAL_EVENTS
            ),
        )
        candidates = _identities(
            self.candidate_binding_ids,
            "economic-archive-vintage-binding",
            "candidate_binding_ids",
        )
        if not candidates:
            raise ValueError("mirror resolution requires candidates")
        selected = _identity(
            self.selected_binding_id,
            "economic-archive-vintage-binding",
            "selected_binding_id",
        )
        equivalent = _identities(
            self.equivalent_binding_ids,
            "economic-archive-vintage-binding",
            "equivalent_binding_ids",
        )
        conflicting = _identities(
            self.conflicting_binding_ids,
            "economic-archive-vintage-binding",
            "conflicting_binding_ids",
        )
        if selected not in candidates:
            raise ValueError("selected mirror binding is not a candidate")
        if (
            set(equivalent) & set(conflicting)
            or selected in equivalent
            or selected in conflicting
            or set(candidates) != {selected, *equivalent, *conflicting}
        ):
            raise ValueError(
                "mirror resolution does not partition its candidates"
            )
        object.__setattr__(self, "candidate_binding_ids", candidates)
        object.__setattr__(self, "selected_binding_id", selected)
        object.__setattr__(self, "equivalent_binding_ids", equivalent)
        object.__setattr__(self, "conflicting_binding_ids", conflicting)
        expected = _stable_id(
            "economic-archive-mirror-resolution", self.identity_payload()
        )
        supplied = _optional_text(self.resolution_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "resolution_id does not match deterministic identity"
            )
        object.__setattr__(self, "resolution_id", expected)

    @property
    def record_key(self) -> tuple[str, int]:
        return self.logical_event_key, self.revision_sequence

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "logical_event_key": self.logical_event_key,
            "revision_sequence": self.revision_sequence,
            "candidate_binding_ids": list(self.candidate_binding_ids),
            "selected_binding_id": self.selected_binding_id,
            "equivalent_binding_ids": list(self.equivalent_binding_ids),
            "conflicting_binding_ids": list(self.conflicting_binding_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "resolution_id": self.resolution_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicArchiveMirrorResolutionV1:
        return cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            revision_sequence=cast(int, data.get("revision_sequence")),
            candidate_binding_ids=tuple(
                str(item)
                for item in _sequence(data.get("candidate_binding_ids"))
            ),
            selected_binding_id=str(data.get("selected_binding_id", "")),
            equivalent_binding_ids=tuple(
                str(item)
                for item in _sequence(data.get("equivalent_binding_ids"))
            ),
            conflicting_binding_ids=tuple(
                str(item)
                for item in _sequence(data.get("conflicting_binding_ids"))
            ),
            resolution_id=str(data.get("resolution_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicArchiveSourceEraV1:
    """One bounded archive system/parser era for an indicator."""

    economy_code: str
    event_family: EconomicEventFamily
    indicator_key: str
    source_key: str
    source_id: str
    effective_from_ns: int
    effective_to_ns: int | None
    parser_id: str
    parser_version: str
    evidence_artifact_ids: tuple[str, ...]
    limitations: tuple[str, ...]
    era_id: str = ""
    schema_version: str = ECONOMIC_ARCHIVE_SOURCE_ERA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_ARCHIVE_SOURCE_ERA_SCHEMA_VERSION:
            raise ValueError("unsupported economic archive source-era schema")
        object.__setattr__(
            self, "economy_code", _economy_code(self.economy_code)
        )
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        object.__setattr__(
            self, "indicator_key", _key(self.indicator_key, "indicator_key")
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "source_id",
            _identity(self.source_id, "official-source", "source_id"),
        )
        start = _bounded_ns(self.effective_from_ns, "effective_from_ns")
        end = _optional_ns(self.effective_to_ns, "effective_to_ns")
        if end is not None and end <= start:
            raise ValueError("archive source era must be a nonempty interval")
        object.__setattr__(self, "effective_from_ns", start)
        object.__setattr__(self, "effective_to_ns", end)
        object.__setattr__(self, "parser_id", _key(self.parser_id, "parser_id"))
        object.__setattr__(
            self,
            "parser_version",
            _required_text(self.parser_version, "parser_version"),
        )
        evidence = _identities(
            self.evidence_artifact_ids,
            "economic-archive-artifact",
            "evidence_artifact_ids",
        )
        if not evidence:
            raise ValueError("archive source era requires artifact evidence")
        object.__setattr__(self, "evidence_artifact_ids", evidence)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-archive-source-era", self.identity_payload()
        )
        supplied = _optional_text(self.era_id)
        if supplied is not None and supplied != expected:
            raise ValueError("era_id does not match deterministic identity")
        object.__setattr__(self, "era_id", expected)

    def contains(self, available_at_ns: int) -> bool:
        instant = _bounded_ns(available_at_ns, "available_at_ns")
        return self.effective_from_ns <= instant and (
            self.effective_to_ns is None or instant < self.effective_to_ns
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "event_family": self.event_family.value,
            "indicator_key": self.indicator_key,
            "source_key": self.source_key,
            "source_id": self.source_id,
            "effective_from_ns": self.effective_from_ns,
            "effective_to_ns": self.effective_to_ns,
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "evidence_artifact_ids": list(self.evidence_artifact_ids),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "era_id": self.era_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicArchiveSourceEraV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            indicator_key=str(data.get("indicator_key", "")),
            source_key=str(data.get("source_key", "")),
            source_id=str(data.get("source_id", "")),
            effective_from_ns=cast(int, data.get("effective_from_ns")),
            effective_to_ns=cast(int | None, data.get("effective_to_ns")),
            parser_id=str(data.get("parser_id", "")),
            parser_version=str(data.get("parser_version", "")),
            evidence_artifact_ids=tuple(
                str(item)
                for item in _sequence(data.get("evidence_artifact_ids"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            era_id=str(data.get("era_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicVintageCoverageDeclarationV1:
    """One adapter's explicit vintage capability for an economy/family cell."""

    economy_code: str
    event_family: EconomicEventFamily
    adapter_id: str
    adapter_version: str
    primary_source_key: str
    primary_source_id: str
    recovery_mode: EconomicVintageRecoveryMode
    api_latest_only: bool
    initial_actual_qualified: bool
    previous_as_known_qualified: bool
    coverage_start_ns: int | None
    coverage_end_ns: int | None
    evidence_artifact_ids: tuple[str, ...]
    qualification_evidence: tuple[str, ...]
    limitations: tuple[str, ...]
    declaration_id: str = ""
    schema_version: str = ECONOMIC_VINTAGE_COVERAGE_DECLARATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_VINTAGE_COVERAGE_DECLARATION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported economic vintage coverage schema")
        object.__setattr__(
            self, "economy_code", _economy_code(self.economy_code)
        )
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        object.__setattr__(
            self, "adapter_id", _key(self.adapter_id, "adapter_id")
        )
        object.__setattr__(
            self,
            "adapter_version",
            _required_text(self.adapter_version, "adapter_version"),
        )
        object.__setattr__(
            self,
            "primary_source_key",
            _key(self.primary_source_key, "primary_source_key"),
        )
        object.__setattr__(
            self,
            "primary_source_id",
            _identity(
                self.primary_source_id, "official-source", "primary_source_id"
            ),
        )
        mode = EconomicVintageRecoveryMode.from_value(self.recovery_mode)
        object.__setattr__(self, "recovery_mode", mode)
        for name in (
            "api_latest_only",
            "initial_actual_qualified",
            "previous_as_known_qualified",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        start = _optional_ns(self.coverage_start_ns, "coverage_start_ns")
        end = _optional_ns(self.coverage_end_ns, "coverage_end_ns")
        if (start is None) != (end is None) or (
            start is not None and end is not None and start >= end
        ):
            raise ValueError(
                "qualified vintage coverage requires a nonempty interval"
            )
        qualified = (
            self.initial_actual_qualified or self.previous_as_known_qualified
        )
        if qualified and start is None:
            raise ValueError("qualified vintage claims require coverage bounds")
        if (
            mode is EconomicVintageRecoveryMode.HISTORICALLY_INCOMPLETE
            and qualified
        ):
            raise ValueError(
                "historically incomplete coverage cannot be qualified"
            )
        if mode is EconomicVintageRecoveryMode.API_VINTAGE_COMPLETE and (
            self.api_latest_only
            or not (
                self.initial_actual_qualified
                and self.previous_as_known_qualified
            )
        ):
            raise ValueError(
                "API-vintage-complete coverage requires both qualified claims"
            )
        artifacts = _identities(
            self.evidence_artifact_ids,
            "economic-archive-artifact",
            "evidence_artifact_ids",
        )
        if (
            mode
            in {
                EconomicVintageRecoveryMode.ARCHIVE_RECONSTRUCTED,
                EconomicVintageRecoveryMode.SNAPSHOT_DEPENDENT,
            }
            and not artifacts
        ):
            raise ValueError(
                "archive/snapshot coverage requires artifact evidence"
            )
        if (
            self.api_latest_only
            and qualified
            and mode
            not in {
                EconomicVintageRecoveryMode.ARCHIVE_RECONSTRUCTED,
                EconomicVintageRecoveryMode.SNAPSHOT_DEPENDENT,
            }
        ):
            raise ValueError(
                "latest-only API cannot qualify historical vintage claims"
            )
        evidence = _texts(
            self.qualification_evidence,
            "qualification_evidence",
            required=qualified,
        )
        limitations = _texts(self.limitations, "limitations", required=True)
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        object.__setattr__(self, "evidence_artifact_ids", artifacts)
        object.__setattr__(self, "qualification_evidence", evidence)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "economic-vintage-coverage-declaration", self.identity_payload()
        )
        supplied = _optional_text(self.declaration_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "declaration_id does not match deterministic identity"
            )
        object.__setattr__(self, "declaration_id", expected)

    @property
    def cell(self) -> tuple[str, EconomicEventFamily]:
        return self.economy_code, self.event_family

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "event_family": self.event_family.value,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "primary_source_key": self.primary_source_key,
            "primary_source_id": self.primary_source_id,
            "recovery_mode": self.recovery_mode.value,
            "api_latest_only": self.api_latest_only,
            "initial_actual_qualified": self.initial_actual_qualified,
            "previous_as_known_qualified": self.previous_as_known_qualified,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "evidence_artifact_ids": list(self.evidence_artifact_ids),
            "qualification_evidence": list(self.qualification_evidence),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "declaration_id": self.declaration_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicVintageCoverageDeclarationV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            adapter_id=str(data.get("adapter_id", "")),
            adapter_version=str(data.get("adapter_version", "")),
            primary_source_key=str(data.get("primary_source_key", "")),
            primary_source_id=str(data.get("primary_source_id", "")),
            recovery_mode=EconomicVintageRecoveryMode.from_value(
                str(data.get("recovery_mode", ""))
            ),
            api_latest_only=cast(bool, data.get("api_latest_only")),
            initial_actual_qualified=cast(
                bool, data.get("initial_actual_qualified")
            ),
            previous_as_known_qualified=cast(
                bool, data.get("previous_as_known_qualified")
            ),
            coverage_start_ns=cast(int | None, data.get("coverage_start_ns")),
            coverage_end_ns=cast(int | None, data.get("coverage_end_ns")),
            evidence_artifact_ids=tuple(
                str(item)
                for item in _sequence(data.get("evidence_artifact_ids"))
            ),
            qualification_evidence=tuple(
                str(item)
                for item in _sequence(data.get("qualification_evidence"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            declaration_id=str(data.get("declaration_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicVintageGapV1:
    """Explicit expected event whose historical value remains unsupported."""

    economy_code: str
    event_family: EconomicEventFamily
    indicator_key: str
    logical_event_key: str
    reference_period: str
    reason: EconomicVintageGapReason
    attempted_artifact_ids: tuple[str, ...]
    details: str
    gap_id: str = ""
    schema_version: str = ECONOMIC_VINTAGE_GAP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_VINTAGE_GAP_SCHEMA_VERSION:
            raise ValueError("unsupported economic vintage-gap schema")
        object.__setattr__(
            self, "economy_code", _economy_code(self.economy_code)
        )
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        for name in ("indicator_key", "logical_event_key"):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        object.__setattr__(
            self,
            "reason",
            EconomicVintageGapReason.from_value(self.reason),
        )
        object.__setattr__(
            self,
            "attempted_artifact_ids",
            _identities(
                self.attempted_artifact_ids,
                "economic-archive-artifact",
                "attempted_artifact_ids",
            ),
        )
        details = _required_text(self.details, "details")
        if len(details) > 4096:
            raise ValueError("gap details exceed the text bound")
        object.__setattr__(self, "details", details)
        expected = _stable_id("economic-vintage-gap", self.identity_payload())
        supplied = _optional_text(self.gap_id)
        if supplied is not None and supplied != expected:
            raise ValueError("gap_id does not match deterministic identity")
        object.__setattr__(self, "gap_id", expected)

    @property
    def cell(self) -> tuple[str, EconomicEventFamily]:
        return self.economy_code, self.event_family

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "event_family": self.event_family.value,
            "indicator_key": self.indicator_key,
            "logical_event_key": self.logical_event_key,
            "reference_period": self.reference_period,
            "reason": self.reason.value,
            "attempted_artifact_ids": list(self.attempted_artifact_ids),
            "details": self.details,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "gap_id": self.gap_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicVintageGapV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            indicator_key=str(data.get("indicator_key", "")),
            logical_event_key=str(data.get("logical_event_key", "")),
            reference_period=str(data.get("reference_period", "")),
            reason=EconomicVintageGapReason.from_value(
                str(data.get("reason", ""))
            ),
            attempted_artifact_ids=tuple(
                str(item)
                for item in _sequence(data.get("attempted_artifact_ids"))
            ),
            details=str(data.get("details", "")),
            gap_id=str(data.get("gap_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicLatestValueCrossCheckV1:
    """Current API comparison that cannot rewrite its historical release."""

    logical_event_key: str
    historical_release_id: str
    current_artifact_id: str
    current_record_locator: str
    historical_normalized_sha256: str
    current_normalized_sha256: str
    checked_at_ns: int
    cross_check_id: str = ""
    schema_version: str = ECONOMIC_LATEST_VALUE_CROSS_CHECK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_LATEST_VALUE_CROSS_CHECK_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported economic latest-value cross-check schema"
            )
        object.__setattr__(
            self,
            "logical_event_key",
            _key(self.logical_event_key, "logical_event_key"),
        )
        object.__setattr__(
            self,
            "historical_release_id",
            _identity(
                self.historical_release_id,
                "economic-calendar-release",
                "historical_release_id",
            ),
        )
        object.__setattr__(
            self,
            "current_artifact_id",
            _identity(
                self.current_artifact_id,
                "economic-archive-artifact",
                "current_artifact_id",
            ),
        )
        locator = _required_text(
            self.current_record_locator, "current_record_locator"
        )
        if len(locator) > 4096:
            raise ValueError("current_record_locator exceeds the text bound")
        object.__setattr__(self, "current_record_locator", locator)
        for name in (
            "historical_normalized_sha256",
            "current_normalized_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        object.__setattr__(
            self,
            "checked_at_ns",
            _bounded_ns(self.checked_at_ns, "checked_at_ns"),
        )
        expected = _stable_id(
            "economic-latest-value-cross-check", self.identity_payload()
        )
        supplied = _optional_text(self.cross_check_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "cross_check_id does not match deterministic identity"
            )
        object.__setattr__(self, "cross_check_id", expected)

    @property
    def matches(self) -> bool:
        return (
            self.historical_normalized_sha256 == self.current_normalized_sha256
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "logical_event_key": self.logical_event_key,
            "historical_release_id": self.historical_release_id,
            "current_artifact_id": self.current_artifact_id,
            "current_record_locator": self.current_record_locator,
            "historical_normalized_sha256": self.historical_normalized_sha256,
            "current_normalized_sha256": self.current_normalized_sha256,
            "checked_at_ns": self.checked_at_ns,
            "historical_value_is_immutable": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "cross_check_id": self.cross_check_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicLatestValueCrossCheckV1:
        return cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            historical_release_id=str(data.get("historical_release_id", "")),
            current_artifact_id=str(data.get("current_artifact_id", "")),
            current_record_locator=str(data.get("current_record_locator", "")),
            historical_normalized_sha256=str(
                data.get("historical_normalized_sha256", "")
            ),
            current_normalized_sha256=str(
                data.get("current_normalized_sha256", "")
            ),
            checked_at_ns=cast(int, data.get("checked_at_ns")),
            cross_check_id=str(data.get("cross_check_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicArchiveVintageCorpusV1:
    """Complete archive evidence graph for immutable official vintages."""

    registry_id: str
    coverage_declarations: tuple[EconomicVintageCoverageDeclarationV1, ...]
    artifacts: tuple[EconomicArchiveArtifactV1, ...]
    bindings: tuple[EconomicArchiveVintageBindingV1, ...]
    mirror_resolutions: tuple[EconomicArchiveMirrorResolutionV1, ...]
    source_eras: tuple[EconomicArchiveSourceEraV1, ...]
    vintage_chains: tuple[EconomicReleaseVintageChainV1, ...]
    gaps: tuple[EconomicVintageGapV1, ...]
    latest_value_cross_checks: tuple[EconomicLatestValueCrossCheckV1, ...]
    complete: bool
    limitations: tuple[str, ...]
    corpus_id: str = ""
    schema_version: str = ECONOMIC_ARCHIVE_VINTAGE_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_ARCHIVE_VINTAGE_CORPUS_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported economic archive vintage-corpus schema"
            )
        object.__setattr__(
            self,
            "registry_id",
            _identity(
                self.registry_id, "official-source-registry", "registry_id"
            ),
        )
        declarations = _typed_sorted(
            self.coverage_declarations,
            EconomicVintageCoverageDeclarationV1,
            "coverage declarations",
            key=lambda item: (item.economy_code, item.event_family.value),
        )
        expected_cells = {
            (economy, family)
            for economy in SCOPED_ECONOMIES
            for family in EconomicEventFamily
        }
        if {item.cell for item in declarations} != expected_cells or len(
            declarations
        ) != len(expected_cells):
            raise ValueError(
                "vintage coverage declarations must cover every economy/family"
            )
        artifacts = _typed_sorted(
            self.artifacts,
            EconomicArchiveArtifactV1,
            "archive artifacts",
            key=lambda item: item.artifact_id,
            maximum=MAX_ECONOMIC_ARCHIVE_ARTIFACTS,
        )
        bindings = _typed_sorted(
            self.bindings,
            EconomicArchiveVintageBindingV1,
            "archive bindings",
            key=lambda item: (
                item.logical_event_key,
                item.revision_sequence,
                item.source_precedence,
                item.binding_id,
            ),
            maximum=MAX_ECONOMIC_ARCHIVE_BINDINGS,
        )
        resolutions = _typed_sorted(
            self.mirror_resolutions,
            EconomicArchiveMirrorResolutionV1,
            "mirror resolutions",
            key=lambda item: item.record_key,
            maximum=MAX_ECONOMIC_ARCHIVE_BINDINGS,
        )
        eras = _typed_sorted(
            self.source_eras,
            EconomicArchiveSourceEraV1,
            "archive source eras",
            key=lambda item: (item.indicator_key, item.effective_from_ns),
            maximum=MAX_ECONOMIC_ARCHIVE_SOURCE_ERAS,
        )
        chains = _typed_sorted(
            self.vintage_chains,
            EconomicReleaseVintageChainV1,
            "release vintage chains",
            key=lambda item: item.initial_release.logical_event_key,
            maximum=MAX_OFFICIAL_EVENTS,
        )
        gaps = _typed_sorted(
            self.gaps,
            EconomicVintageGapV1,
            "vintage gaps",
            key=lambda item: (item.logical_event_key, item.reason.value),
            maximum=MAX_OFFICIAL_EVENTS,
        )
        checks = _typed_sorted(
            self.latest_value_cross_checks,
            EconomicLatestValueCrossCheckV1,
            "latest-value cross-checks",
            key=lambda item: item.logical_event_key,
            maximum=MAX_OFFICIAL_EVENTS,
        )
        _require_unique_ids(artifacts, "artifact_id", "archive artifact")
        _require_unique_ids(bindings, "binding_id", "archive binding")
        _require_unique_ids(resolutions, "resolution_id", "mirror resolution")
        _require_unique_ids(eras, "era_id", "archive source era")
        _require_unique_ids(chains, "chain_id", "release vintage chain")
        _require_unique_ids(gaps, "gap_id", "vintage gap")
        _require_unique_ids(
            checks, "cross_check_id", "latest-value cross-check"
        )
        if len({item.cell for item in declarations}) != len(declarations):
            raise ValueError("vintage coverage repeats an economy/family cell")
        if len({item.record_key for item in resolutions}) != len(resolutions):
            raise ValueError("archive corpus repeats a mirror resolution")
        if len({(item.logical_event_key, item.reason) for item in gaps}) != len(
            gaps
        ):
            raise ValueError("archive corpus repeats a vintage gap")
        if len({item.logical_event_key for item in checks}) != len(checks):
            raise ValueError(
                "archive corpus repeats a latest-value cross-check"
            )
        if not isinstance(self.complete, bool):
            raise TypeError("complete must be boolean")
        if self.complete and (
            gaps
            or any(
                item.recovery_mode
                is EconomicVintageRecoveryMode.HISTORICALLY_INCOMPLETE
                or not item.initial_actual_qualified
                or not item.previous_as_known_qualified
                for item in declarations
            )
        ):
            raise ValueError(
                "complete corpus retains unqualified coverage or gaps"
            )

        artifact_by_id = {item.artifact_id: item for item in artifacts}
        binding_by_id = {item.binding_id: item for item in bindings}
        declaration_by_cell = {item.cell: item for item in declarations}
        for declaration in declarations:
            if not set(declaration.evidence_artifact_ids).issubset(
                artifact_by_id
            ):
                raise ValueError(
                    "coverage declaration names an unknown artifact"
                )
        for binding in bindings:
            artifact = artifact_by_id.get(binding.artifact_id)
            if artifact is None:
                raise ValueError("vintage binding names an unknown artifact")
            if (
                binding.source_key != artifact.source_key
                or binding.source_id != artifact.source_id
            ):
                raise ValueError("vintage binding changes artifact source")

        releases_by_record: dict[tuple[str, int], EconomicCalendarReleaseV1] = (
            {}
        )
        release_by_id: dict[str, EconomicCalendarReleaseV1] = {}
        terminal_release_ids: set[str] = set()
        for chain in chains:
            for release in chain.releases:
                record_key = (
                    release.logical_event_key,
                    release.revision_sequence,
                )
                if (
                    record_key in releases_by_record
                    or release.release_id in release_by_id
                ):
                    raise ValueError("archive corpus repeats a release vintage")
                releases_by_record[record_key] = release
                release_by_id[release.release_id] = release
            terminal_release_ids.add(chain.releases[-1].release_id)
        if set(releases_by_record) != {item.record_key for item in resolutions}:
            raise ValueError(
                "every release vintage requires one mirror resolution"
            )

        used_binding_ids: set[str] = set()
        selected_by_record: dict[
            tuple[str, int], EconomicArchiveVintageBindingV1
        ] = {}
        for resolution in resolutions:
            candidates = [
                binding_by_id.get(item)
                for item in resolution.candidate_binding_ids
            ]
            if any(item is None for item in candidates):
                raise ValueError("mirror resolution names an unknown binding")
            typed_candidates = tuple(
                cast(EconomicArchiveVintageBindingV1, item)
                for item in candidates
            )
            if any(
                item.record_key != resolution.record_key
                for item in typed_candidates
            ):
                raise ValueError(
                    "mirror candidates refer to different release vintages"
                )
            expected_resolution = resolve_economic_archive_mirrors(
                typed_candidates
            )
            if expected_resolution != resolution:
                raise ValueError(
                    "mirror resolution differs from precedence/hash policy"
                )
            selected = binding_by_id[resolution.selected_binding_id]
            release = releases_by_record[resolution.record_key]
            if any(
                item.release_id != release.release_id
                for item in typed_candidates
            ):
                raise ValueError("archive binding differs from release chain")
            artifact = artifact_by_id[selected.artifact_id]
            if not artifact.artifact_kind.supports_value_vintage:
                raise ValueError("archive index cannot support a value vintage")
            declaration = declaration_by_cell[
                (release.economy_code, release.event_family)
            ]
            _validate_selected_recovery_mode(declaration, artifact, release)
            selected_by_record[resolution.record_key] = selected
            overlap = used_binding_ids & set(resolution.candidate_binding_ids)
            if overlap:
                raise ValueError(
                    "archive binding appears in multiple resolutions"
                )
            used_binding_ids.update(resolution.candidate_binding_ids)
        if used_binding_ids != set(binding_by_id):
            raise ValueError("archive corpus contains an unresolved binding")

        _validate_source_eras(eras, artifact_by_id)
        for record_key, selected in selected_by_record.items():
            release = releases_by_record[record_key]
            artifact = artifact_by_id[selected.artifact_id]
            matching_eras = tuple(
                item
                for item in eras
                if item.indicator_key == release.series_key
                and item.source_key == selected.source_key
                and item.source_id == selected.source_id
                and item.contains(release.available_at_ns)
            )
            if len(matching_eras) != 1:
                raise ValueError(
                    "release vintage is not covered by one source era"
                )
            era = matching_eras[0]
            if (
                era.economy_code != release.economy_code
                or era.event_family is not release.event_family
                or era.parser_id != artifact.parser_id
                or era.parser_version != artifact.parser_version
            ):
                raise ValueError("release vintage differs from its source era")
            if (
                artifact.artifact_kind
                is EconomicArchiveArtifactKind.RETAINED_SNAPSHOT
                and release.available_at_ns < artifact.retrieved_at_ns
            ):
                raise ValueError(
                    "retained snapshot is backdated before capture"
                )

        for gap in gaps:
            if gap.cell not in declaration_by_cell:
                raise ValueError("vintage gap lies outside declared coverage")
            if not set(gap.attempted_artifact_ids).issubset(artifact_by_id):
                raise ValueError(
                    "vintage gap names an unknown attempted artifact"
                )
        for check in checks:
            cross_check_release = release_by_id.get(check.historical_release_id)
            cross_check_artifact = artifact_by_id.get(check.current_artifact_id)
            if (
                cross_check_release is None
                or cross_check_release.logical_event_key
                != check.logical_event_key
            ):
                raise ValueError(
                    "latest-value cross-check changes historical release"
                )
            if check.historical_release_id not in terminal_release_ids:
                raise ValueError(
                    "latest-value cross-check must use a terminal release"
                )
            if (
                cross_check_artifact is None
                or cross_check_artifact.artifact_kind
                is not EconomicArchiveArtifactKind.CURRENT_API
            ):
                raise ValueError(
                    "latest-value cross-check requires a current API artifact"
                )
            if check.checked_at_ns < cross_check_artifact.retrieved_at_ns:
                raise ValueError(
                    "latest-value cross-check predates its current artifact"
                )
            selected = selected_by_record[
                (
                    cross_check_release.logical_event_key,
                    cross_check_release.revision_sequence,
                )
            ]
            if (
                check.historical_normalized_sha256
                != selected.normalized_record_sha256
            ):
                raise ValueError(
                    "latest-value cross-check differs from selected archive record"
                )

        referenced_artifacts = {
            item
            for declaration in declarations
            for item in declaration.evidence_artifact_ids
        }
        referenced_artifacts.update(item.artifact_id for item in bindings)
        referenced_artifacts.update(
            item for era in eras for item in era.evidence_artifact_ids
        )
        referenced_artifacts.update(
            item for gap in gaps for item in gap.attempted_artifact_ids
        )
        referenced_artifacts.update(item.current_artifact_id for item in checks)
        if referenced_artifacts != set(artifact_by_id):
            raise ValueError("archive corpus contains an unreferenced artifact")

        object.__setattr__(self, "coverage_declarations", declarations)
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "mirror_resolutions", resolutions)
        object.__setattr__(self, "source_eras", eras)
        object.__setattr__(self, "vintage_chains", chains)
        object.__setattr__(self, "gaps", gaps)
        object.__setattr__(self, "latest_value_cross_checks", checks)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-archive-vintage-corpus", self.identity_payload()
        )
        supplied = _optional_text(self.corpus_id)
        if supplied is not None and supplied != expected:
            raise ValueError("corpus_id does not match deterministic identity")
        object.__setattr__(self, "corpus_id", expected)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_ECONOMIC_ARCHIVE_CORPUS_BYTES
        ):
            raise ValueError(
                "economic archive vintage corpus exceeds byte bound"
            )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "coverage_declarations": [
                item.to_dict() for item in self.coverage_declarations
            ],
            "artifacts": [item.to_dict() for item in self.artifacts],
            "bindings": [item.to_dict() for item in self.bindings],
            "mirror_resolutions": [
                item.to_dict() for item in self.mirror_resolutions
            ],
            "source_eras": [item.to_dict() for item in self.source_eras],
            "vintage_chains": [item.to_dict() for item in self.vintage_chains],
            "gaps": [item.to_dict() for item in self.gaps],
            "latest_value_cross_checks": [
                item.to_dict() for item in self.latest_value_cross_checks
            ],
            "complete": self.complete,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "corpus_id": self.corpus_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicArchiveVintageCorpusV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            coverage_declarations=tuple(
                EconomicVintageCoverageDeclarationV1.from_dict(_mapping(item))
                for item in _sequence(data.get("coverage_declarations"))
            ),
            artifacts=tuple(
                EconomicArchiveArtifactV1.from_dict(_mapping(item))
                for item in _sequence(data.get("artifacts"))
            ),
            bindings=tuple(
                EconomicArchiveVintageBindingV1.from_dict(_mapping(item))
                for item in _sequence(data.get("bindings"))
            ),
            mirror_resolutions=tuple(
                EconomicArchiveMirrorResolutionV1.from_dict(_mapping(item))
                for item in _sequence(data.get("mirror_resolutions"))
            ),
            source_eras=tuple(
                EconomicArchiveSourceEraV1.from_dict(_mapping(item))
                for item in _sequence(data.get("source_eras"))
            ),
            vintage_chains=tuple(
                EconomicReleaseVintageChainV1.from_dict(_mapping(item))
                for item in _sequence(data.get("vintage_chains"))
            ),
            gaps=tuple(
                EconomicVintageGapV1.from_dict(_mapping(item))
                for item in _sequence(data.get("gaps"))
            ),
            latest_value_cross_checks=tuple(
                EconomicLatestValueCrossCheckV1.from_dict(_mapping(item))
                for item in _sequence(data.get("latest_value_cross_checks"))
            ),
            complete=cast(bool, data.get("complete")),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicArchiveVintageCorpusV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic archive vintage corpus is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicArchiveVintageAuditV1:
    """Machine audit of coverage modes and archive reconstruction integrity."""

    corpus_id: str
    registry_id: str
    coverage_cell_count: int
    artifact_count: int
    binding_count: int
    chain_count: int
    release_count: int
    gap_count: int
    recovery_mode_counts: tuple[tuple[str, int], ...]
    incomplete_cells: tuple[str, ...]
    current_value_mismatch_event_keys: tuple[str, ...]
    lower_precedence_conflict_event_keys: tuple[str, ...]
    benchmark_batch_violations: tuple[str, ...]
    simultaneous_revision_violations: tuple[str, ...]
    checks: tuple[tuple[str, bool], ...]
    audit_id: str = ""
    schema_version: str = ECONOMIC_ARCHIVE_VINTAGE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_ARCHIVE_VINTAGE_AUDIT_SCHEMA_VERSION:
            raise ValueError(
                "unsupported economic archive vintage-audit schema"
            )
        object.__setattr__(
            self,
            "corpus_id",
            _identity(
                self.corpus_id, "economic-archive-vintage-corpus", "corpus_id"
            ),
        )
        object.__setattr__(
            self,
            "registry_id",
            _identity(
                self.registry_id, "official-source-registry", "registry_id"
            ),
        )
        for name, maximum in (
            (
                "coverage_cell_count",
                len(SCOPED_ECONOMIES) * len(EconomicEventFamily),
            ),
            ("artifact_count", MAX_ECONOMIC_ARCHIVE_ARTIFACTS),
            ("binding_count", MAX_ECONOMIC_ARCHIVE_BINDINGS),
            ("chain_count", MAX_OFFICIAL_EVENTS),
            ("release_count", MAX_ECONOMIC_ARCHIVE_BINDINGS),
            ("gap_count", MAX_OFFICIAL_EVENTS),
        ):
            _bounded_count(getattr(self, name), name, maximum)
        mode_counts = tuple(
            sorted(
                (str(key), int(value))
                for key, value in self.recovery_mode_counts
            )
        )
        if (
            {key for key, _ in mode_counts}
            != {item.value for item in EconomicVintageRecoveryMode}
            or any(value < 0 for _, value in mode_counts)
            or sum(value for _, value in mode_counts)
            != self.coverage_cell_count
        ):
            raise ValueError(
                "archive audit recovery-mode counts are inconsistent"
            )
        object.__setattr__(self, "recovery_mode_counts", mode_counts)
        for name in (
            "incomplete_cells",
            "current_value_mismatch_event_keys",
            "lower_precedence_conflict_event_keys",
            "benchmark_batch_violations",
            "simultaneous_revision_violations",
        ):
            object.__setattr__(self, name, _texts(getattr(self, name), name))
        checks = tuple(
            sorted((str(name), value) for name, value in self.checks)
        )
        if (
            not checks
            or len({name for name, _ in checks}) != len(checks)
            or any(
                not name or not isinstance(value, bool)
                for name, value in checks
            )
        ):
            raise ValueError(
                "archive vintage audit requires unique boolean checks"
            )
        object.__setattr__(self, "checks", checks)
        expected = _stable_id(
            "economic-archive-vintage-audit", self.identity_payload()
        )
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id does not match deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    @property
    def passed(self) -> bool:
        return all(value for _, value in self.checks)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "registry_id": self.registry_id,
            "coverage_cell_count": self.coverage_cell_count,
            "artifact_count": self.artifact_count,
            "binding_count": self.binding_count,
            "chain_count": self.chain_count,
            "release_count": self.release_count,
            "gap_count": self.gap_count,
            "recovery_mode_counts": [
                {"mode": key, "count": value}
                for key, value in self.recovery_mode_counts
            ],
            "incomplete_cells": list(self.incomplete_cells),
            "current_value_mismatch_event_keys": list(
                self.current_value_mismatch_event_keys
            ),
            "lower_precedence_conflict_event_keys": list(
                self.lower_precedence_conflict_event_keys
            ),
            "benchmark_batch_violations": list(self.benchmark_batch_violations),
            "simultaneous_revision_violations": list(
                self.simultaneous_revision_violations
            ),
            "checks": [
                {"name": name, "passed": passed} for name, passed in self.checks
            ],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicArchiveVintageAuditV1:
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            registry_id=str(data.get("registry_id", "")),
            coverage_cell_count=cast(int, data.get("coverage_cell_count")),
            artifact_count=cast(int, data.get("artifact_count")),
            binding_count=cast(int, data.get("binding_count")),
            chain_count=cast(int, data.get("chain_count")),
            release_count=cast(int, data.get("release_count")),
            gap_count=cast(int, data.get("gap_count")),
            recovery_mode_counts=tuple(
                (
                    str(_mapping(item, "recovery mode count").get("mode", "")),
                    cast(
                        int,
                        _mapping(item, "recovery mode count").get("count"),
                    ),
                )
                for item in _sequence(data.get("recovery_mode_counts"))
            ),
            incomplete_cells=tuple(
                str(item) for item in _sequence(data.get("incomplete_cells"))
            ),
            current_value_mismatch_event_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("current_value_mismatch_event_keys")
                )
            ),
            lower_precedence_conflict_event_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("lower_precedence_conflict_event_keys")
                )
            ),
            benchmark_batch_violations=tuple(
                str(item)
                for item in _sequence(data.get("benchmark_batch_violations"))
            ),
            simultaneous_revision_violations=tuple(
                str(item)
                for item in _sequence(
                    data.get("simultaneous_revision_violations")
                )
            ),
            checks=tuple(
                (
                    str(_mapping(item, "audit check").get("name", "")),
                    cast(bool, _mapping(item, "audit check").get("passed")),
                )
                for item in _sequence(data.get("checks"))
            ),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicArchiveVintageAuditV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic archive vintage audit is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


def build_economic_archive_artifact(
    snapshot: OfficialRawSnapshotV1,
    artifact_kind: EconomicArchiveArtifactKind,
    *,
    source_locator: str | None = None,
    limitations: Sequence[str],
) -> EconomicArchiveArtifactV1:
    """Project an immutable official snapshot into archive evidence."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("archive artifact requires a v1 official snapshot")
    return EconomicArchiveArtifactV1(
        snapshot_id=snapshot.snapshot_id,
        request_id=snapshot.request.request_id,
        source_key=snapshot.request.source_key,
        source_id=snapshot.request.source_id,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        retrieved_at_ns=snapshot.retrieved_at_ns,
        parser_id=snapshot.request.parser_id,
        parser_version=snapshot.request.parser_version,
        source_locator=source_locator or snapshot.resolved_uri,
        artifact_kind=artifact_kind,
        limitations=tuple(limitations),
    )


def conservative_official_vintage_coverage(
    registry: OfficialSourceRegistryV1,
) -> tuple[EconomicVintageCoverageDeclarationV1, ...]:
    """Declare every reviewed cell incomplete until empirical qualification."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError(
            "vintage coverage requires a v1 official source registry"
        )
    return tuple(
        EconomicVintageCoverageDeclarationV1(
            economy_code=economy,
            event_family=family,
            adapter_id=f"{source.parser_id}.vintage",
            adapter_version=source.parser_version,
            primary_source_key=source.source_key,
            primary_source_id=source.source_id,
            recovery_mode=EconomicVintageRecoveryMode.HISTORICALLY_INCOMPLETE,
            api_latest_only=(
                OfficialSourceCapability.HISTORICAL_VINTAGES
                not in source.capabilities
            ),
            initial_actual_qualified=False,
            previous_as_known_qualified=False,
            coverage_start_ns=None,
            coverage_end_ns=None,
            evidence_artifact_ids=(),
            qualification_evidence=(),
            limitations=(
                "Reviewed entrypoint is not empirical historical-vintage coverage.",
            ),
        )
        for economy in registry.scoped_economies
        for family in EconomicEventFamily
        for source in (registry.primary_source(economy, family),)
    )


def resolve_economic_archive_mirrors(
    bindings: Sequence[EconomicArchiveVintageBindingV1],
) -> EconomicArchiveMirrorResolutionV1:
    """Select an official record by precedence and normalized content hash."""
    values = tuple(bindings)
    if not values or any(
        not isinstance(item, EconomicArchiveVintageBindingV1) for item in values
    ):
        raise TypeError("mirror resolution requires v1 archive bindings")
    record_keys = {item.record_key for item in values}
    if len(record_keys) != 1:
        raise ValueError("mirror bindings refer to different release vintages")
    if len({item.binding_id for item in values}) != len(values):
        raise ValueError("mirror resolution repeats a binding")
    ordered = tuple(
        sorted(
            values,
            key=lambda item: (
                item.source_precedence,
                item.source_key,
                item.artifact_id,
                item.binding_id,
            ),
        )
    )
    selected = ordered[0]
    top = tuple(
        item
        for item in ordered
        if item.source_precedence == selected.source_precedence
    )
    if any(
        item.normalized_record_sha256 != selected.normalized_record_sha256
        for item in top
    ):
        raise ValueError("equal-precedence official mirrors conflict")
    equivalent = tuple(
        item.binding_id
        for item in ordered[1:]
        if item.normalized_record_sha256 == selected.normalized_record_sha256
    )
    conflicting = tuple(
        item.binding_id
        for item in ordered[1:]
        if item.normalized_record_sha256 != selected.normalized_record_sha256
    )
    return EconomicArchiveMirrorResolutionV1(
        logical_event_key=selected.logical_event_key,
        revision_sequence=selected.revision_sequence,
        candidate_binding_ids=tuple(item.binding_id for item in ordered),
        selected_binding_id=selected.binding_id,
        equivalent_binding_ids=equivalent,
        conflicting_binding_ids=conflicting,
    )


def build_economic_archive_vintage_corpus(
    registry: OfficialSourceRegistryV1,
    coverage_declarations: Sequence[EconomicVintageCoverageDeclarationV1],
    *,
    artifacts: Sequence[EconomicArchiveArtifactV1],
    bindings: Sequence[EconomicArchiveVintageBindingV1],
    mirror_resolutions: Sequence[EconomicArchiveMirrorResolutionV1],
    source_eras: Sequence[EconomicArchiveSourceEraV1],
    vintage_chains: Sequence[EconomicReleaseVintageChainV1],
    gaps: Sequence[EconomicVintageGapV1] = (),
    latest_value_cross_checks: Sequence[EconomicLatestValueCrossCheckV1] = (),
    complete: bool,
    limitations: Sequence[str],
) -> EconomicArchiveVintageCorpusV1:
    """Validate registry capabilities and construct an archive evidence graph."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("archive corpus requires a v1 official source registry")
    declarations = tuple(coverage_declarations)
    artifact_values = tuple(artifacts)
    era_values = tuple(source_eras)
    for declaration in declarations:
        if not isinstance(declaration, EconomicVintageCoverageDeclarationV1):
            raise TypeError("archive corpus requires v1 coverage declarations")
        source = registry.primary_source(
            declaration.economy_code, declaration.event_family
        )
        if (
            declaration.primary_source_key != source.source_key
            or declaration.primary_source_id != source.source_id
        ):
            raise ValueError("coverage declaration changes its primary source")
        has_vintage_capability = (
            OfficialSourceCapability.HISTORICAL_VINTAGES in source.capabilities
        )
        if not declaration.api_latest_only and not has_vintage_capability:
            raise ValueError(
                "source without vintage capability must be latest-only"
            )
        if (
            declaration.recovery_mode
            is EconomicVintageRecoveryMode.API_VINTAGE_COMPLETE
            and (
                not has_vintage_capability
                or source.verification_status
                is not OfficialSourceVerificationStatus.EMPIRICALLY_VERIFIED
            )
        ):
            raise ValueError(
                "API vintage completeness requires empirical source verification"
            )
    for artifact in artifact_values:
        if not isinstance(artifact, EconomicArchiveArtifactV1):
            raise TypeError("archive corpus requires v1 archive artifacts")
        source = registry.source(artifact.source_key)
        if (
            artifact.source_id != source.source_id
            or artifact.source_format not in source.formats
            or artifact.parser_id != source.parser_id
            or artifact.parser_version != source.parser_version
        ):
            raise ValueError(
                "archive artifact differs from registered source/parser"
            )
        capabilities = set(source.capabilities)
        kind = artifact.artifact_kind
        if (
            kind is EconomicArchiveArtifactKind.CURRENT_API
            and OfficialSourceCapability.CURRENT_VALUES not in capabilities
        ):
            raise ValueError(
                "current API artifact lacks a current-values capability"
            )
        if kind is EconomicArchiveArtifactKind.RETAINED_SNAPSHOT and not (
            capabilities
            & {
                OfficialSourceCapability.CURRENT_VALUES,
                OfficialSourceCapability.HISTORICAL_OBSERVATIONS,
                OfficialSourceCapability.HISTORICAL_VINTAGES,
            }
        ):
            raise ValueError(
                "retained snapshot lacks a value-bearing capability"
            )
        if kind in {
            EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
            EconomicArchiveArtifactKind.ARCHIVED_TABLE,
            EconomicArchiveArtifactKind.OFFICIAL_MIRROR,
            EconomicArchiveArtifactKind.ARCHIVE_INDEX,
        } and not (
            capabilities
            & {
                OfficialSourceCapability.ARCHIVE_ENUMERATION,
                OfficialSourceCapability.HISTORICAL_OBSERVATIONS,
                OfficialSourceCapability.HISTORICAL_VINTAGES,
                OfficialSourceCapability.PUBLICATION_METADATA,
            }
        ):
            raise ValueError("archive artifact lacks an archival capability")
    artifacts_by_id = {item.artifact_id: item for item in artifact_values}
    for declaration in declarations:
        for artifact_id in declaration.evidence_artifact_ids:
            evidence_artifact = artifacts_by_id.get(artifact_id)
            if evidence_artifact is None:
                continue
            source = registry.source(evidence_artifact.source_key)
            if (
                source.economy_code != declaration.economy_code
                or declaration.event_family not in source.event_families
            ):
                raise ValueError(
                    "coverage evidence lies outside its economy/family cell"
                )
    for era in era_values:
        if not isinstance(era, EconomicArchiveSourceEraV1):
            raise TypeError("archive corpus requires v1 archive source eras")
        source = registry.source(era.source_key)
        if (
            era.source_id != source.source_id
            or era.economy_code != source.economy_code
            or era.event_family not in source.event_families
            or era.parser_id != source.parser_id
            or era.parser_version != source.parser_version
        ):
            raise ValueError(
                "archive source era differs from registered source"
            )
    releases_by_id = {
        release.release_id: release
        for chain in vintage_chains
        for release in chain.releases
    }
    for binding in bindings:
        release = releases_by_id.get(binding.release_id)
        if release is None:
            continue
        source = registry.source(binding.source_key)
        if (
            source.economy_code != release.economy_code
            or release.event_family not in source.event_families
        ):
            raise ValueError(
                "archive binding source differs from its release cell"
            )
    for gap in gaps:
        for artifact_id in gap.attempted_artifact_ids:
            gap_artifact = artifacts_by_id.get(artifact_id)
            if gap_artifact is None:
                continue
            source = registry.source(gap_artifact.source_key)
            if (
                source.economy_code != gap.economy_code
                or gap.event_family not in source.event_families
            ):
                raise ValueError(
                    "vintage gap evidence differs from its declared cell"
                )
    for check in latest_value_cross_checks:
        release = releases_by_id.get(check.historical_release_id)
        current_artifact = artifacts_by_id.get(check.current_artifact_id)
        if release is None or current_artifact is None:
            continue
        source = registry.source(current_artifact.source_key)
        if (
            source.economy_code != release.economy_code
            or release.event_family not in source.event_families
        ):
            raise ValueError(
                "current cross-check source differs from its release cell"
            )
    return EconomicArchiveVintageCorpusV1(
        registry_id=registry.registry_id,
        coverage_declarations=declarations,
        artifacts=artifact_values,
        bindings=tuple(bindings),
        mirror_resolutions=tuple(mirror_resolutions),
        source_eras=era_values,
        vintage_chains=tuple(vintage_chains),
        gaps=tuple(gaps),
        latest_value_cross_checks=tuple(latest_value_cross_checks),
        complete=complete,
        limitations=tuple(limitations),
    )


def audit_economic_archive_vintages(
    corpus: EconomicArchiveVintageCorpusV1,
) -> EconomicArchiveVintageAuditV1:
    """Audit difficult archive cases without treating current APIs as history."""
    if not isinstance(corpus, EconomicArchiveVintageCorpusV1):
        raise TypeError("archive audit requires a v1 archive vintage corpus")
    mode_counts = {item.value: 0 for item in EconomicVintageRecoveryMode}
    incomplete: list[str] = []
    for declaration in corpus.coverage_declarations:
        mode_counts[declaration.recovery_mode.value] += 1
        if (
            declaration.recovery_mode
            is EconomicVintageRecoveryMode.HISTORICALLY_INCOMPLETE
            or not declaration.initial_actual_qualified
            or not declaration.previous_as_known_qualified
        ):
            incomplete.append(
                f"{declaration.economy_code}:{declaration.event_family.value}"
            )
    binding_by_id = {item.binding_id: item for item in corpus.bindings}
    conflicts = [
        item.logical_event_key
        for item in corpus.mirror_resolutions
        if item.conflicting_binding_ids
    ]
    benchmark_violations = _benchmark_batch_violations(corpus, binding_by_id)
    simultaneous_violations = _simultaneous_revision_violations(
        corpus.vintage_chains
    )
    current_mismatches = [
        item.logical_event_key
        for item in corpus.latest_value_cross_checks
        if not item.matches
    ]
    return EconomicArchiveVintageAuditV1(
        corpus_id=corpus.corpus_id,
        registry_id=corpus.registry_id,
        coverage_cell_count=len(corpus.coverage_declarations),
        artifact_count=len(corpus.artifacts),
        binding_count=len(corpus.bindings),
        chain_count=len(corpus.vintage_chains),
        release_count=sum(len(item.releases) for item in corpus.vintage_chains),
        gap_count=len(corpus.gaps),
        recovery_mode_counts=tuple(mode_counts.items()),
        incomplete_cells=tuple(incomplete),
        current_value_mismatch_event_keys=tuple(current_mismatches),
        lower_precedence_conflict_event_keys=tuple(conflicts),
        benchmark_batch_violations=tuple(benchmark_violations),
        simultaneous_revision_violations=tuple(simultaneous_violations),
        checks=(
            ("artifact-release-binding", True),
            ("latest-only-api-non-leakage", True),
            ("mirror-precedence-and-hashes", True),
            ("source-era-binding", True),
            ("benchmark-batch-completeness", not benchmark_violations),
            ("simultaneous-previous-revisions", not simultaneous_violations),
        ),
    )


def write_economic_archive_vintage_corpus(
    corpus: EconomicArchiveVintageCorpusV1, path: str | Path
) -> Path:
    """Write one bounded canonical archive-vintage corpus."""
    if not isinstance(corpus, EconomicArchiveVintageCorpusV1):
        raise TypeError("archive corpus writer requires a v1 corpus")
    destination = Path(path)
    payload = corpus.to_json().encode("utf-8")
    if len(payload) > MAX_ECONOMIC_ARCHIVE_CORPUS_BYTES:
        raise ValueError("economic archive vintage corpus exceeds byte bound")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload + b"\n")
    return destination


def read_economic_archive_vintage_corpus(
    path: str | Path,
) -> EconomicArchiveVintageCorpusV1:
    """Restore and verify one bounded archive-vintage corpus."""
    source = Path(path)
    if source.stat().st_size > MAX_ECONOMIC_ARCHIVE_CORPUS_BYTES:
        raise ValueError("economic archive vintage corpus exceeds byte bound")
    try:
        return EconomicArchiveVintageCorpusV1.from_json(
            source.read_text(encoding="utf-8")
        )
    except UnicodeDecodeError as exc:
        raise ValueError(
            "economic archive vintage corpus is not UTF-8"
        ) from exc


def _validate_selected_recovery_mode(
    declaration: EconomicVintageCoverageDeclarationV1,
    artifact: EconomicArchiveArtifactV1,
    release: EconomicCalendarReleaseV1,
) -> None:
    kind = artifact.artifact_kind
    mode = declaration.recovery_mode
    if (
        declaration.api_latest_only
        and kind is EconomicArchiveArtifactKind.CURRENT_API
    ):
        raise ValueError(
            f"latest-only API cannot support historical release {release.logical_event_key}"
        )
    if mode is EconomicVintageRecoveryMode.API_VINTAGE_COMPLETE:
        allowed = {EconomicArchiveArtifactKind.CURRENT_API}
    elif mode is EconomicVintageRecoveryMode.ARCHIVE_RECONSTRUCTED:
        allowed = {
            EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
            EconomicArchiveArtifactKind.ARCHIVED_TABLE,
            EconomicArchiveArtifactKind.OFFICIAL_MIRROR,
        }
    elif mode is EconomicVintageRecoveryMode.SNAPSHOT_DEPENDENT:
        allowed = {EconomicArchiveArtifactKind.RETAINED_SNAPSHOT}
    else:
        allowed = {
            EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
            EconomicArchiveArtifactKind.ARCHIVED_TABLE,
            EconomicArchiveArtifactKind.RETAINED_SNAPSHOT,
            EconomicArchiveArtifactKind.OFFICIAL_MIRROR,
        }
    if kind not in allowed:
        raise ValueError(
            "selected artifact differs from declared recovery mode"
        )


def _validate_source_eras(
    eras: Sequence[EconomicArchiveSourceEraV1],
    artifacts: Mapping[str, EconomicArchiveArtifactV1],
) -> None:
    by_indicator: dict[
        tuple[str, EconomicEventFamily, str], list[EconomicArchiveSourceEraV1]
    ] = defaultdict(list)
    for era in eras:
        evidence = tuple(
            artifacts.get(item) for item in era.evidence_artifact_ids
        )
        if any(item is None for item in evidence):
            raise ValueError("archive source era names an unknown artifact")
        if not any(
            item is not None
            and item.source_key == era.source_key
            and item.source_id == era.source_id
            and item.parser_id == era.parser_id
            and item.parser_version == era.parser_version
            for item in evidence
        ):
            raise ValueError(
                "archive source era lacks evidence from its source"
            )
        by_indicator[
            (era.economy_code, era.event_family, era.indicator_key)
        ].append(era)
    for values in by_indicator.values():
        ordered = sorted(values, key=lambda item: item.effective_from_ns)
        for previous, current in pairwise(ordered):
            if (
                previous.effective_to_ns is None
                or current.effective_from_ns < previous.effective_to_ns
            ):
                raise ValueError("archive source eras overlap")


def _benchmark_batch_violations(
    corpus: EconomicArchiveVintageCorpusV1,
    binding_by_id: Mapping[str, EconomicArchiveVintageBindingV1],
) -> tuple[str, ...]:
    selected_by_record = {
        item.record_key: binding_by_id[item.selected_binding_id]
        for item in corpus.mirror_resolutions
    }
    batches: dict[tuple[str, str], dict[str, set[str]]] = defaultdict(
        lambda: {"observed": set(), "required": set()}
    )
    for chain in corpus.vintage_chains:
        for index, mutation in enumerate(chain.mutations, start=1):
            if (
                mutation.revision_kind
                is not EconomicReleaseRevisionKind.BENCHMARK
            ):
                continue
            release = chain.releases[index]
            binding = selected_by_record[
                (release.logical_event_key, release.revision_sequence)
            ]
            batch = batches[(binding.artifact_id, release.series_id)]
            batch["observed"].add(release.reference_period)
            batch["required"].update(mutation.affected_reference_periods)
    return tuple(
        sorted(
            f"{artifact_id}:{series_id}:{period}"
            for (artifact_id, series_id), periods in batches.items()
            for period in periods["required"] - periods["observed"]
        )
    )


def _simultaneous_revision_violations(
    chains: Sequence[EconomicReleaseVintageChainV1],
) -> tuple[str, ...]:
    first_actual_by_event: dict[str, EconomicCalendarReleaseV1] = {}
    for chain in chains:
        actual = next(
            (
                item
                for item in chain.releases
                if item.actual_value is not None
                and item.stage is not EconomicReleaseStage.REVISION
            ),
            None,
        )
        if actual is not None:
            first_actual_by_event[actual.logical_event_key] = actual
    violations: list[str] = []
    for chain in chains:
        for mutation in chain.mutations:
            if (
                mutation.revision_kind
                is not EconomicReleaseRevisionKind.SIMULTANEOUS_PREVIOUS
            ):
                continue
            trigger = first_actual_by_event.get(
                cast(str, mutation.triggering_logical_event_key)
            )
            if not (
                trigger is not None
                and trigger.released_at_ns
                == mutation.time_evidence.actual_published_at_ns
                and trigger.reference_period_end_ns
                > chain.initial_release.reference_period_end_ns
                and trigger.series_id == chain.initial_release.series_id
                and chain.initial_release.reference_period
                in mutation.affected_reference_periods
            ):
                violations.append(chain.initial_release.logical_event_key)
    return tuple(sorted(violations))


_T = TypeVar("_T")


def _typed_sorted(
    values: Iterable[_T],
    expected_type: type[_T],
    name: str,
    *,
    key: Callable[[_T], Any],
    maximum: int | None = None,
) -> tuple[_T, ...]:
    result = tuple(values)
    if any(not isinstance(item, expected_type) for item in result):
        raise TypeError(f"{name} must use the v1 contract")
    if maximum is not None and len(result) > maximum:
        raise ValueError(f"{name} exceeds the item bound")
    return tuple(sorted(result, key=key))


def _require_unique_ids(
    values: Sequence[object], attribute: str, name: str
) -> None:
    identities = tuple(str(getattr(item, attribute)) for item in values)
    if len(set(identities)) != len(identities):
        raise ValueError(f"{name} identities are not unique")


__all__ = [
    "ECONOMIC_ARCHIVE_ARTIFACT_SCHEMA_VERSION",
    "ECONOMIC_ARCHIVE_MIRROR_RESOLUTION_SCHEMA_VERSION",
    "ECONOMIC_ARCHIVE_SOURCE_ERA_SCHEMA_VERSION",
    "ECONOMIC_ARCHIVE_VINTAGE_AUDIT_SCHEMA_VERSION",
    "ECONOMIC_ARCHIVE_VINTAGE_BINDING_SCHEMA_VERSION",
    "ECONOMIC_ARCHIVE_VINTAGE_CORPUS_SCHEMA_VERSION",
    "ECONOMIC_LATEST_VALUE_CROSS_CHECK_SCHEMA_VERSION",
    "ECONOMIC_VINTAGE_COVERAGE_DECLARATION_SCHEMA_VERSION",
    "ECONOMIC_VINTAGE_GAP_SCHEMA_VERSION",
    "MAX_ECONOMIC_ARCHIVE_ARTIFACTS",
    "MAX_ECONOMIC_ARCHIVE_BINDINGS",
    "MAX_ECONOMIC_ARCHIVE_CORPUS_BYTES",
    "MAX_ECONOMIC_ARCHIVE_SOURCE_ERAS",
    "EconomicArchiveArtifactKind",
    "EconomicArchiveArtifactV1",
    "EconomicArchiveMirrorResolutionV1",
    "EconomicArchiveSourceEraV1",
    "EconomicArchiveVintageAuditV1",
    "EconomicArchiveVintageBindingV1",
    "EconomicArchiveVintageCorpusV1",
    "EconomicLatestValueCrossCheckV1",
    "EconomicVintageCoverageDeclarationV1",
    "EconomicVintageGapReason",
    "EconomicVintageGapV1",
    "EconomicVintageRecoveryMode",
    "audit_economic_archive_vintages",
    "build_economic_archive_artifact",
    "build_economic_archive_vintage_corpus",
    "conservative_official_vintage_coverage",
    "read_economic_archive_vintage_corpus",
    "resolve_economic_archive_mirrors",
    "write_economic_archive_vintage_corpus",
]

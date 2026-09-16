"""Deterministic qualification of the ECB monetary-policy decision archive.

The ECB's public FOEDB database is the authoritative inventory for press
publications.  It is a versioned, chunked JSON database rather than a static
HTML index.  This module retains locks for that complete database and selects
only records whose type and source-authored title identify the monetary-policy
decision series.  The exact decision pages then reconstruct all three key ECB
rates without assuming that their presentation order stayed constant.

The packaged manifest is compact.  Operators retain the larger FOEDB and HTML
corpus and can replay it byte for byte with the refresh command.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import EconomicTimePrecision
from histdatacom.market_context.monetary_policy import (
    MonetaryPolicyActionTiming,
    MonetaryPolicyDecisionDirection,
    MonetaryPolicySettingComponentV1,
    MonetaryPolicySettingKind,
    MonetaryPolicySettingV1,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

ECB_MONETARY_POLICY_SOURCE_KEY: Final = "ea.ecb.monetary-policy"
ECB_MONETARY_POLICY_PARSER_ID: Final = "official.ecb-monetary-policy.v1"
ECB_MONETARY_POLICY_PROGRAM_KEY: Final = "ea.ecb.monetary-policy-decision"
ECB_FOEDB_ROOT_URI: Final = (
    "https://www.ecb.europa.eu/foedb/dbs/foedb/publications.en"
)
ECB_FOEDB_VERSIONS_URI: Final = f"{ECB_FOEDB_ROOT_URI}/versions.json"
ECB_ARCHIVE_START_DATE: Final = "2000-01-01"
ECB_PREDECESSOR_DATE: Final = "1999-12-15"
ECB_LATEST_PACKAGED_DECISION_DATE: Final = "2026-09-10"
ECB_SOURCE_TIMEZONE: Final = "Europe/Berlin"

ECB_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.ecb-artifact.v1"
ECB_FOEDB_ENTRY_SCHEMA_VERSION: Final = "histdatacom.ecb-foedb-entry.v1"
ECB_FOEDB_INDEX_SCHEMA_VERSION: Final = "histdatacom.ecb-foedb-index.v1"
ECB_PREDECESSOR_SCHEMA_VERSION: Final = "histdatacom.ecb-predecessor.v1"
ECB_DECISION_SCHEMA_VERSION: Final = "histdatacom.ecb-decision.v1"
ECB_ARCHIVE_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-policy-archive-manifest.v1"
)

MAX_ECB_DATABASE_RECORDS: Final = 100_000
MAX_ECB_DATABASE_CHUNKS: Final = 512
MAX_ECB_DECISIONS: Final = 1_024
MAX_ECB_ARTIFACT_BYTES: Final = 20_000_000
MAX_ECB_TOTAL_BYTES: Final = 512_000_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_VERSION_RE = re.compile(r"^[0-9]{1,20}$")
_VERSION_HASH_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_DOCUMENT_DATE_RE = re.compile(r"(?:ecb\.mp|pr)(?P<date>\d{6})")
_RATE_RE = re.compile(
    r"(?<![\d.])(?P<value>-?\d+(?:\.\d+)?)\s*" r"(?P<unit>%|per\s+cent)",
    re.IGNORECASE,
)
_COMPONENT_PHRASES: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "main-refinancing-operations": (
            "main refinancing operations",
            "minimum bid rate",
        ),
        "marginal-lending-facility": ("marginal lending facility",),
        "deposit-facility": ("deposit facility",),
    }
)
_COMPONENT_LABELS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "main-refinancing-operations": "Main refinancing operations rate",
        "marginal-lending-facility": "Marginal lending facility rate",
        "deposit-facility": "Deposit facility rate",
    }
)
_DECISION_TITLES: Final = frozenset(
    {"Monetary policy decisions", "Monetary Policy Decisions"}
)
_EMERGENCY_RELEASE_DATES: Final = frozenset({"2001-09-17", "2008-10-08"})


class EcbArtifactRole(str, Enum):
    """Role of one official artifact in the retained ECB corpus."""

    FOEDB_VERSIONS = "foedb-versions"
    FOEDB_METADATA = "foedb-metadata"
    FOEDB_CHUNK = "foedb-chunk"
    DECISION_HTML = "decision-html"
    ACCOUNT_HTML = "account-html"
    STATEMENT_HTML = "statement-html"
    PROJECTION_INDEX_HTML = "projection-index-html"
    PROJECTION_HTML = "projection-html"
    PROJECTION_PDF = "projection-pdf"


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


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    return result


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.ecb.europa.eu"
        or parsed.fragment
    ):
        raise ValueError(f"{name} must use the official ECB HTTPS host")
    return result


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _bounded_int(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _document_release_date(uri: str) -> str:
    match = _DOCUMENT_DATE_RE.search(uri)
    if match is None:
        raise ValueError("ECB decision URI omits its release date")
    token = match.group("date")
    year = (
        1900 + int(token[:2]) if int(token[:2]) >= 90 else 2000 + int(token[:2])
    )
    return date(year, int(token[2:4]), int(token[4:6])).isoformat()


def _version_base_uri(version: str, version_hash: str) -> str:
    if _VERSION_RE.fullmatch(version) is None:
        raise ValueError("ECB FOEDB version is invalid")
    if _VERSION_HASH_RE.fullmatch(version_hash) is None:
        raise ValueError("ECB FOEDB version hash is invalid")
    return f"{ECB_FOEDB_ROOT_URI}/{version}/{version_hash}"


@dataclass(frozen=True, slots=True)
class EcbArchiveArtifactV1:
    """Content-addressed official ECB database or publication artifact."""

    role: EcbArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = ECB_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported ECB artifact schema")
        role = EcbArtifactRole(self.role)
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if role in {
            EcbArtifactRole.DECISION_HTML,
            EcbArtifactRole.ACCOUNT_HTML,
            EcbArtifactRole.STATEMENT_HTML,
            EcbArtifactRole.PROJECTION_INDEX_HTML,
            EcbArtifactRole.PROJECTION_HTML,
        }:
            if source_format is not OfficialSourceFormat.HTML:
                raise ValueError("ECB publication artifact must be HTML")
        elif role is EcbArtifactRole.PROJECTION_PDF:
            if source_format is not OfficialSourceFormat.PDF:
                raise ValueError("ECB projection artifact must be PDF")
        elif source_format is not OfficialSourceFormat.JSON:
            raise ValueError("ECB FOEDB artifact must be JSON")
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(
            self, "source_uri", _https_uri(self.source_uri, "source_uri")
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _bounded_int(
            self.content_length, "content_length", MAX_ECB_ARTIFACT_BYTES
        )
        expected = _stable_id("ecb-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("ECB artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "source_uri": self.source_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbArchiveArtifactV1:
        return cls(
            role=EcbArtifactRole(str(data.get("role", ""))),
            source_uri=str(data.get("source_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbFoedbPublicationV1:
    """One exactly selected FOEDB monetary-policy publication record."""

    record_id: int
    database_timestamp: int
    release_date: str
    source_uri: str
    title: str
    time_precision: EconomicTimePrecision
    published_at_ns: int | None = None
    published_lexical: str | None = None
    publication_id: str = ""
    schema_version: str = ECB_FOEDB_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_FOEDB_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported ECB FOEDB-entry schema")
        _bounded_int(self.record_id, "record_id", 2**63 - 1)
        _bounded_int(self.database_timestamp, "database_timestamp", 2**63 - 1)
        object.__setattr__(
            self, "release_date", _iso_date(self.release_date, "release_date")
        )
        source_uri = _https_uri(self.source_uri, "source_uri")
        if _document_release_date(source_uri) != self.release_date:
            raise ValueError("ECB FOEDB URI and release date differ")
        object.__setattr__(self, "source_uri", source_uri)
        if _required_text(self.title, "title") not in _DECISION_TITLES:
            raise ValueError("ECB FOEDB entry has an unexpected title")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if precision not in {
            EconomicTimePrecision.EXACT_MINUTE,
            EconomicTimePrecision.DATE_ONLY,
        }:
            raise ValueError("ECB FOEDB entry has unsupported time precision")
        object.__setattr__(self, "time_precision", precision)
        lexical = _optional_text(self.published_lexical)
        if precision is EconomicTimePrecision.EXACT_MINUTE:
            if self.published_at_ns is None or lexical is None:
                raise ValueError("exact ECB time requires timestamp evidence")
            _bounded_int(self.published_at_ns, "published_at_ns", 2**63 - 1)
            parsed = datetime.fromisoformat(lexical)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError("ECB published_lexical must retain an offset")
            if int(parsed.timestamp() * 1_000_000_000) != self.published_at_ns:
                raise ValueError("ECB published timestamp evidence differs")
            if parsed.date().isoformat() != self.release_date:
                raise ValueError("ECB exact publication date differs")
        elif self.published_at_ns is not None or lexical is not None:
            raise ValueError("date-only ECB entry cannot claim an exact time")
        object.__setattr__(self, "published_lexical", lexical)
        expected = _stable_id("ecb-foedb-publication", self.identity_payload())
        if self.publication_id and self.publication_id != expected:
            raise ValueError("ECB FOEDB publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def exact_minute(self) -> bool:
        return self.time_precision is EconomicTimePrecision.EXACT_MINUTE

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "database_timestamp": self.database_timestamp,
            "release_date": self.release_date,
            "source_uri": self.source_uri,
            "title": self.title,
            "time_precision": self.time_precision.value,
            "published_at_ns": self.published_at_ns,
            "published_lexical": self.published_lexical,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbFoedbPublicationV1:
        return cls(
            record_id=cast(int, data.get("record_id")),
            database_timestamp=cast(int, data.get("database_timestamp")),
            release_date=str(data.get("release_date", "")),
            source_uri=str(data.get("source_uri", "")),
            title=str(data.get("title", "")),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            published_at_ns=cast(int | None, data.get("published_at_ns")),
            published_lexical=_optional_text(data.get("published_lexical")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbFoedbReleaseIndexV1:
    """Hash-bound FOEDB database version and exact decision selection."""

    as_of_date: str
    database_version: str
    database_version_hash: str
    database_total_records: int
    database_chunk_size: int
    database_chunk_group_size: int
    database_artifacts: tuple[EcbArchiveArtifactV1, ...]
    publications: tuple[EcbFoedbPublicationV1, ...]
    index_id: str = ""
    schema_version: str = ECB_FOEDB_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_FOEDB_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported ECB FOEDB-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if _VERSION_RE.fullmatch(self.database_version) is None:
            raise ValueError("ECB database_version is invalid")
        if _VERSION_HASH_RE.fullmatch(self.database_version_hash) is None:
            raise ValueError("ECB database_version_hash is invalid")
        total = _bounded_int(
            self.database_total_records,
            "database_total_records",
            MAX_ECB_DATABASE_RECORDS,
        )
        chunk_size = _bounded_int(
            self.database_chunk_size,
            "database_chunk_size",
            MAX_ECB_DATABASE_RECORDS,
        )
        group_size = _bounded_int(
            self.database_chunk_group_size,
            "database_chunk_group_size",
            MAX_ECB_DATABASE_RECORDS,
        )
        if not chunk_size or not group_size:
            raise ValueError("ECB database chunk sizes must be positive")
        artifacts = tuple(
            sorted(self.database_artifacts, key=lambda item: item.source_uri)
        )
        expected_chunks = math.ceil(total / chunk_size)
        roles = [item.role for item in artifacts]
        if roles.count(EcbArtifactRole.FOEDB_VERSIONS) != 1:
            raise ValueError("ECB index requires one versions artifact")
        if roles.count(EcbArtifactRole.FOEDB_METADATA) != 1:
            raise ValueError("ECB index requires one metadata artifact")
        if roles.count(EcbArtifactRole.FOEDB_CHUNK) != expected_chunks:
            raise ValueError("ECB index has an incomplete chunk inventory")
        if len({item.source_uri for item in artifacts}) != len(artifacts):
            raise ValueError("ECB index repeats a database artifact")
        publications = tuple(
            sorted(self.publications, key=lambda item: item.release_date)
        )
        if not 2 <= len(publications) <= MAX_ECB_DECISIONS:
            raise ValueError("ECB publication count is outside bounds")
        if publications[0].release_date != ECB_PREDECESSOR_DATE:
            raise ValueError("ECB index omits the required predecessor")
        if publications[1].release_date < ECB_ARCHIVE_START_DATE:
            raise ValueError("ECB target window starts before 2000")
        if publications[-1].release_date > as_of:
            raise ValueError("ECB publication follows the as-of boundary")
        if len({item.record_id for item in publications}) != len(publications):
            raise ValueError("ECB index repeats a FOEDB record")
        if len({item.source_uri for item in publications}) != len(publications):
            raise ValueError("ECB index repeats a decision URI")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "database_artifacts", artifacts)
        object.__setattr__(self, "publications", publications)
        expected = _stable_id("ecb-foedb-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("ECB FOEDB index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_publication_id(self) -> Mapping[str, EcbFoedbPublicationV1]:
        return MappingProxyType(
            {item.publication_id: item for item in self.publications}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "database_version": self.database_version,
            "database_version_hash": self.database_version_hash,
            "database_total_records": self.database_total_records,
            "database_chunk_size": self.database_chunk_size,
            "database_chunk_group_size": self.database_chunk_group_size,
            "database_artifacts": [
                item.to_dict() for item in self.database_artifacts
            ],
            "publications": [item.to_dict() for item in self.publications],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbFoedbReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            database_version=str(data.get("database_version", "")),
            database_version_hash=str(data.get("database_version_hash", "")),
            database_total_records=cast(
                int, data.get("database_total_records")
            ),
            database_chunk_size=cast(int, data.get("database_chunk_size")),
            database_chunk_group_size=cast(
                int, data.get("database_chunk_group_size")
            ),
            database_artifacts=tuple(
                EcbArchiveArtifactV1.from_dict(
                    _mapping(item, "database artifact")
                )
                for item in _sequence(
                    data.get("database_artifacts"), "database_artifacts"
                )
            ),
            publications=tuple(
                EcbFoedbPublicationV1.from_dict(_mapping(item, "publication"))
                for item in _sequence(data.get("publications"), "publications")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbPolicyPredecessorV1:
    """The last pre-window ECB setting used for previous-as-known lineage."""

    publication_id: str
    setting: MonetaryPolicySettingV1
    artifact: EcbArchiveArtifactV1
    source_excerpt: str
    predecessor_id: str = ""
    schema_version: str = ECB_PREDECESSOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PREDECESSOR_SCHEMA_VERSION:
            raise ValueError("unsupported ECB predecessor schema")
        object.__setattr__(
            self,
            "publication_id",
            _required_text(self.publication_id, "publication_id"),
        )
        if not isinstance(self.setting, MonetaryPolicySettingV1):
            raise TypeError("ECB predecessor requires a policy setting")
        if self.artifact.role is not EcbArtifactRole.DECISION_HTML:
            raise ValueError("ECB predecessor requires decision HTML")
        object.__setattr__(
            self,
            "source_excerpt",
            _required_text(self.source_excerpt, "source_excerpt"),
        )
        expected = _stable_id("ecb-predecessor", self.identity_payload())
        if self.predecessor_id and self.predecessor_id != expected:
            raise ValueError("ECB predecessor identity differs")
        object.__setattr__(self, "predecessor_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "setting": self.setting.to_dict(),
            "artifact": self.artifact.to_dict(),
            "source_excerpt": self.source_excerpt,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "predecessor_id": self.predecessor_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbPolicyPredecessorV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("setting"), "setting")
            ),
            artifact=EcbArchiveArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            source_excerpt=str(data.get("source_excerpt", "")),
            predecessor_id=str(data.get("predecessor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryPolicyDecisionV1:
    """One ECB three-rate decision and its immediately prior known setting."""

    publication_id: str
    release_date: str
    action_timing: MonetaryPolicyActionTiming
    previous_setting: MonetaryPolicySettingV1
    new_setting: MonetaryPolicySettingV1
    direction: MonetaryPolicyDecisionDirection
    artifact: EcbArchiveArtifactV1
    source_excerpt: str
    expectation_unavailable_reason: str
    limitations: tuple[str, ...]
    decision_id: str = ""
    schema_version: str = ECB_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_DECISION_SCHEMA_VERSION:
            raise ValueError("unsupported ECB decision schema")
        object.__setattr__(
            self,
            "publication_id",
            _required_text(self.publication_id, "publication_id"),
        )
        object.__setattr__(
            self, "release_date", _iso_date(self.release_date, "release_date")
        )
        timing = MonetaryPolicyActionTiming(self.action_timing)
        if (self.release_date in _EMERGENCY_RELEASE_DATES) != (
            timing is MonetaryPolicyActionTiming.EMERGENCY
        ):
            raise ValueError("ECB emergency classification differs")
        object.__setattr__(self, "action_timing", timing)
        if not isinstance(
            self.previous_setting, MonetaryPolicySettingV1
        ) or not isinstance(self.new_setting, MonetaryPolicySettingV1):
            raise TypeError("ECB decision requires typed policy settings")
        if (
            self.previous_setting.kind is not MonetaryPolicySettingKind.RATE_SET
            or self.new_setting.kind is not MonetaryPolicySettingKind.RATE_SET
        ):
            raise ValueError("ECB decision must preserve the three-rate set")
        direction = MonetaryPolicyDecisionDirection(self.direction)
        equal = (
            self.previous_setting.semantic_id == self.new_setting.semantic_id
        )
        if (direction is MonetaryPolicyDecisionDirection.HOLD) != equal:
            raise ValueError("ECB decision direction differs from rate setting")
        if direction not in {
            MonetaryPolicyDecisionDirection.HOLD,
            MonetaryPolicyDecisionDirection.EASE,
            MonetaryPolicyDecisionDirection.TIGHTEN,
        }:
            raise ValueError("unsupported ECB decision direction")
        object.__setattr__(self, "direction", direction)
        if self.artifact.role is not EcbArtifactRole.DECISION_HTML:
            raise ValueError("ECB decision requires decision HTML")
        object.__setattr__(
            self,
            "source_excerpt",
            _required_text(self.source_excerpt, "source_excerpt"),
        )
        object.__setattr__(
            self,
            "expectation_unavailable_reason",
            _required_text(
                self.expectation_unavailable_reason,
                "expectation_unavailable_reason",
            ),
        )
        values = tuple(
            sorted(
                {
                    _required_text(item, "limitation")
                    for item in self.limitations
                }
            )
        )
        if not values:
            raise ValueError("ECB decision requires limitations")
        object.__setattr__(self, "limitations", values)
        expected = _stable_id("ecb-decision", self.identity_payload())
        if self.decision_id and self.decision_id != expected:
            raise ValueError("ECB decision identity differs")
        object.__setattr__(self, "decision_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "release_date": self.release_date,
            "action_timing": self.action_timing.value,
            "previous_setting": self.previous_setting.to_dict(),
            "new_setting": self.new_setting.to_dict(),
            "direction": self.direction.value,
            "artifact": self.artifact.to_dict(),
            "source_excerpt": self.source_excerpt,
            "expectation_unavailable_reason": self.expectation_unavailable_reason,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbMonetaryPolicyDecisionV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            release_date=str(data.get("release_date", "")),
            action_timing=MonetaryPolicyActionTiming(
                str(data.get("action_timing", ""))
            ),
            previous_setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("previous_setting"), "previous_setting")
            ),
            new_setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("new_setting"), "new_setting")
            ),
            direction=MonetaryPolicyDecisionDirection(
                str(data.get("direction", ""))
            ),
            artifact=EcbArchiveArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            source_excerpt=str(data.get("source_excerpt", "")),
            expectation_unavailable_reason=str(
                data.get("expectation_unavailable_reason", "")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryPolicyArchiveManifestV1:
    """Complete 2000-present ECB decision archive qualification."""

    registry_id: str
    source_id: str
    release_index: EcbFoedbReleaseIndexV1
    predecessor: EcbPolicyPredecessorV1
    decisions: tuple[EcbMonetaryPolicyDecisionV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    exact_minute_count: int
    date_only_count: int
    emergency_count: int
    manifest_id: str = ""
    schema_version: str = ECB_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported ECB archive-manifest schema")
        if not self.registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("ECB manifest registry identity is invalid")
        if not self.source_id.startswith("official-source:sha256:"):
            raise ValueError("ECB manifest source identity is invalid")
        if not isinstance(self.release_index, EcbFoedbReleaseIndexV1):
            raise TypeError("ECB manifest requires a FOEDB release index")
        publications = self.release_index.by_publication_id
        if self.predecessor.publication_id not in publications:
            raise ValueError("ECB predecessor is absent from the index")
        predecessor_entry = publications[self.predecessor.publication_id]
        if predecessor_entry.release_date != ECB_PREDECESSOR_DATE:
            raise ValueError("ECB predecessor date differs")
        if self.predecessor.artifact.source_uri != predecessor_entry.source_uri:
            raise ValueError("ECB predecessor artifact differs from index")
        decisions = tuple(
            sorted(self.decisions, key=lambda item: item.release_date)
        )
        expected_entries = self.release_index.publications[1:]
        if [item.publication_id for item in decisions] != [
            item.publication_id for item in expected_entries
        ]:
            raise ValueError("ECB decisions differ from the FOEDB selection")
        if any(
            decision.artifact.source_uri != entry.source_uri
            or decision.release_date != entry.release_date
            for decision, entry in zip(decisions, expected_entries, strict=True)
        ):
            raise ValueError("ECB decision artifacts differ from the index")
        lineage = (
            self.predecessor.setting,
            *(item.new_setting for item in decisions),
        )
        if any(
            current.previous_setting.semantic_id != previous.semantic_id
            for previous, current in zip(lineage[:-1], decisions, strict=True)
        ):
            raise ValueError("ECB decision setting lineage is discontinuous")
        artifacts = (
            *self.release_index.database_artifacts,
            self.predecessor.artifact,
            *(item.artifact for item in decisions),
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError("ECB raw artifact count differs")
        unique_hashes = {item.content_sha256 for item in artifacts}
        if self.unique_content_sha256_count != len(unique_hashes):
            raise ValueError("ECB unique artifact count differs")
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError("ECB total content bytes differ")
        if not 0 <= self.total_content_bytes <= MAX_ECB_TOTAL_BYTES:
            raise ValueError("ECB total content bytes exceed bound")
        exact = sum(item.exact_minute for item in expected_entries)
        if self.exact_minute_count != exact:
            raise ValueError("ECB exact-minute count differs")
        if self.date_only_count != len(decisions) - exact:
            raise ValueError("ECB date-only count differs")
        emergency = sum(
            item.action_timing is MonetaryPolicyActionTiming.EMERGENCY
            for item in decisions
        )
        if self.emergency_count != emergency:
            raise ValueError("ECB emergency count differs")
        object.__setattr__(self, "decisions", decisions)
        expected = _stable_id("ecb-archive-manifest", self.identity_payload())
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("ECB archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "release_index": self.release_index.to_dict(),
            "predecessor": self.predecessor.to_dict(),
            "decisions": [item.to_dict() for item in self.decisions],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "exact_minute_count": self.exact_minute_count,
            "date_only_count": self.date_only_count,
            "emergency_count": self.emergency_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbMonetaryPolicyArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            release_index=EcbFoedbReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            predecessor=EcbPolicyPredecessorV1.from_dict(
                _mapping(data.get("predecessor"), "predecessor")
            ),
            decisions=tuple(
                EcbMonetaryPolicyDecisionV1.from_dict(
                    _mapping(item, "decision")
                )
                for item in _sequence(data.get("decisions"), "decisions")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            date_only_count=cast(int, data.get("date_only_count")),
            emergency_count=cast(int, data.get("emergency_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EcbMonetaryPolicyArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("ECB archive manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "ECB archive manifest"))


class _EcbDecisionHtmlParser(HTMLParser):
    """Extract source text blocks only from the decision page's main node."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._main_depth = 0
        self._block_depth = 0
        self._title_depth = 0
        self._current: list[str] = []
        self._title: list[str] = []
        self.blocks: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag == "main":
            self._main_depth += 1
            return
        if not self._main_depth:
            return
        if tag in {"h1", "h2"}:
            self._title_depth += 1
        if tag in {"p", "li"} and self._block_depth == 0:
            self._block_depth = 1
            self._current = []
        elif self._block_depth and tag not in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            self._block_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "main" and self._main_depth:
            self._main_depth -= 1
            return
        if not self._main_depth:
            return
        if tag in {"h1", "h2"} and self._title_depth:
            self._title_depth -= 1
        if self._block_depth:
            self._block_depth -= 1
            if self._block_depth == 0:
                text = re.sub(r"\s+", " ", " ".join(self._current)).strip()
                if text and (not self.blocks or self.blocks[-1] != text):
                    self.blocks.append(text)
                self._current = []

    def handle_data(self, data: str) -> None:
        if self._title_depth:
            self._title.append(data)
        if self._block_depth:
            self._current.append(data)

    @property
    def title(self) -> str:
        return re.sub(r"\s+", " ", " ".join(self._title)).strip()


@dataclass(frozen=True, slots=True)
class _ParsedRateSet:
    setting: MonetaryPolicySettingV1
    source_excerpt: str


def _component_position(text: str, component_key: str) -> int:
    positions = [
        text.find(phrase)
        for phrase in _COMPONENT_PHRASES[component_key]
        if phrase in text
    ]
    return min(positions) if positions else -1


def _parse_rate_set(content: bytes) -> _ParsedRateSet:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB decision page is not UTF-8") from exc
    parser = _EcbDecisionHtmlParser()
    parser.feed(text)
    parser.close()
    if "monetary policy decisions" not in parser.title.casefold():
        raise ValueError("ECB decision page title differs")

    selected: list[str] = []
    values: dict[str, tuple[float, str]] = {}
    for block in parser.blocks:
        lower = block.lower()
        present = {
            key
            for key in _COMPONENT_PHRASES
            if _component_position(lower, key) >= 0
        }
        if len(present) != 3:
            continue
        start = min(_component_position(lower, key) for key in present)
        end = lower.find("respectively", start)
        span = block[start:] if end < 0 else block[start:end]
        rates = [
            (float(match.group("value")), match.group(0))
            for match in _RATE_RE.finditer(span)
        ]
        order = sorted(present, key=lambda key: _component_position(lower, key))
        if len(rates) >= 3:
            values = dict(zip(order, rates[:3], strict=True))
            selected = [block]
            break

    if not values:
        for block in parser.blocks:
            lower = block.lower()
            present = {
                key
                for key in _COMPONENT_PHRASES
                if _component_position(lower, key) >= 0
            }
            if not present:
                continue
            start = min(_component_position(lower, key) for key in present)
            end = lower.find("respectively", start)
            span = block[start:] if end < 0 else block[start:end]
            rates = [
                (float(match.group("value")), match.group(0))
                for match in _RATE_RE.finditer(span)
            ]
            order = sorted(
                present, key=lambda key: _component_position(lower, key)
            )
            if len(rates) >= len(order):
                values.update(zip(order, rates[: len(order)], strict=True))
                selected.append(block)

    if set(values) != set(_COMPONENT_PHRASES):
        missing = sorted(set(_COMPONENT_PHRASES) - set(values))
        raise ValueError(f"ECB decision omits rate components: {missing}")
    components = tuple(
        MonetaryPolicySettingComponentV1(
            component_key=key,
            label=_COMPONENT_LABELS[key],
            value=values[key][0],
            unit="percent",
            source_lexical=values[key][1],
        )
        for key in _COMPONENT_PHRASES
    )
    lexical = "; ".join(f"{key}={values[key][1]}" for key in _COMPONENT_PHRASES)
    excerpt = " | ".join(selected)
    if len(excerpt) > 8_192:
        raise ValueError("ECB decision source excerpt exceeds bound")
    return _ParsedRateSet(
        setting=MonetaryPolicySettingV1(
            kind=MonetaryPolicySettingKind.RATE_SET,
            unit="percent",
            definition_version="ecb-key-interest-rates.v1",
            source_lexical=lexical,
            components=components,
        ),
        source_excerpt=excerpt,
    )


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: EcbArtifactRole
) -> EcbArchiveArtifactV1:
    if snapshot.status_code != 200 or not snapshot.content:
        raise ValueError("ECB snapshot is not a complete successful response")
    if (
        snapshot.request.source_key != ECB_MONETARY_POLICY_SOURCE_KEY
        or snapshot.request.parser_id != ECB_MONETARY_POLICY_PARSER_ID
        or snapshot.request.parser_version != "1"
    ):
        raise ValueError("ECB snapshot changes source or parser identity")
    expected_format = (
        OfficialSourceFormat.HTML
        if role
        in {
            EcbArtifactRole.DECISION_HTML,
            EcbArtifactRole.ACCOUNT_HTML,
            EcbArtifactRole.STATEMENT_HTML,
            EcbArtifactRole.PROJECTION_INDEX_HTML,
            EcbArtifactRole.PROJECTION_HTML,
        }
        else (
            OfficialSourceFormat.PDF
            if role is EcbArtifactRole.PROJECTION_PDF
            else OfficialSourceFormat.JSON
        )
    )
    if snapshot.request.source_format is not expected_format:
        raise ValueError("ECB snapshot format differs from artifact role")
    return EcbArchiveArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=expected_format,
        content_sha256=hashlib.sha256(snapshot.content).hexdigest(),
        content_length=len(snapshot.content),
    )


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    if (
        source.parser_id != ECB_MONETARY_POLICY_PARSER_ID
        or source.parser_version != "1"
        or source_format not in source.formats
        or cast(str, urlparse(uri).hostname) not in source.allowed_hosts
    ):
        raise ValueError("ECB registry binding differs")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        page_number=page_number,
    )


def build_ecb_foedb_versions_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the request for the FOEDB version pointer."""
    return _request(registry, ECB_FOEDB_VERSIONS_URI, OfficialSourceFormat.JSON)


def parse_ecb_foedb_version(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[str, str]:
    """Parse the one active FOEDB database version."""
    _artifact(snapshot, EcbArtifactRole.FOEDB_VERSIONS)
    try:
        payload = json.loads(snapshot.content)
        items = _sequence(payload, "FOEDB versions")
        if len(items) != 1:
            raise ValueError("ECB FOEDB must expose one active version")
        item = _mapping(items[0], "FOEDB version")
        version = _required_text(item.get("version"), "version")
        version_hash = _required_text(item.get("hash"), "hash")
        _version_base_uri(version, version_hash)
        return version, version_hash
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("ECB FOEDB version pointer is invalid") from exc


def build_ecb_foedb_metadata_request(
    registry: OfficialSourceRegistryV1,
    version: str,
    version_hash: str,
) -> OfficialSourceRequestV1:
    """Build the request for versioned FOEDB database metadata."""
    uri = f"{_version_base_uri(version, version_hash)}/metadata.json"
    return _request(registry, uri, OfficialSourceFormat.JSON)


def build_ecb_foedb_chunk_requests(
    registry: OfficialSourceRegistryV1,
    version: str,
    version_hash: str,
    *,
    total_records: int,
    chunk_size: int,
    chunk_group_size: int,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the complete bounded request set for one FOEDB version."""
    total = _bounded_int(
        total_records, "total_records", MAX_ECB_DATABASE_RECORDS
    )
    size = _bounded_int(chunk_size, "chunk_size", MAX_ECB_DATABASE_RECORDS)
    group = _bounded_int(
        chunk_group_size, "chunk_group_size", MAX_ECB_DATABASE_RECORDS
    )
    if not size or not group:
        raise ValueError("ECB FOEDB chunk sizes must be positive")
    count = math.ceil(total / size)
    if count > MAX_ECB_DATABASE_CHUNKS:
        raise ValueError("ECB FOEDB chunk count exceeds bound")
    base = _version_base_uri(version, version_hash)
    return tuple(
        _request(
            registry,
            f"{base}/data/{index // group}/chunk_{index}.json",
            OfficialSourceFormat.JSON,
            page_number=index,
        )
        for index in range(count)
    )


@dataclass(frozen=True, slots=True)
class _EcbFoedbDatabaseV1:
    """Validated in-memory view of one complete FOEDB database version."""

    version: str
    version_hash: str
    total_records: int
    chunk_size: int
    chunk_group_size: int
    artifacts: tuple[EcbArchiveArtifactV1, ...]
    records: tuple[Mapping[str, Any], ...]


def _decode_ecb_foedb_database(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
) -> _EcbFoedbDatabaseV1:
    """Validate and decode one complete retained FOEDB database version."""
    version, version_hash = parse_ecb_foedb_version(versions_snapshot)
    version_base = _version_base_uri(version, version_hash)
    if metadata_snapshot.request.uri != f"{version_base}/metadata.json":
        raise ValueError("ECB FOEDB metadata version differs")
    artifacts = [
        _artifact(versions_snapshot, EcbArtifactRole.FOEDB_VERSIONS),
        _artifact(metadata_snapshot, EcbArtifactRole.FOEDB_METADATA),
    ]
    try:
        metadata = _mapping(
            json.loads(metadata_snapshot.content), "FOEDB metadata"
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("ECB FOEDB metadata is invalid") from exc
    if metadata.get("name") != "publications.en":
        raise ValueError("ECB FOEDB metadata names another database")
    total = _bounded_int(
        metadata.get("total_records"),
        "total_records",
        MAX_ECB_DATABASE_RECORDS,
    )
    chunk_size = _bounded_int(
        metadata.get("chunk_size"),
        "chunk_size",
        MAX_ECB_DATABASE_RECORDS,
    )
    group_size = _bounded_int(
        metadata.get("chunk_group_size"),
        "chunk_group_size",
        MAX_ECB_DATABASE_RECORDS,
    )
    header = tuple(
        str(item) for item in _sequence(metadata.get("header"), "header")
    )
    expected_header = (
        "id",
        "pub_timestamp",
        "year",
        "issue_number",
        "type",
        "JEL_Code",
        "Taxonomy",
        "boardmember",
        "Authors",
        "documentTypes",
        "publicationProperties",
        "childrenPublication",
        "relatedPublications",
    )
    if header != expected_header:
        raise ValueError("ECB FOEDB record schema differs")
    expected_chunks = math.ceil(total / chunk_size)
    if len(chunk_snapshots) != expected_chunks:
        raise ValueError("ECB FOEDB retained chunk count differs")

    records: list[Mapping[str, Any]] = []
    by_page = {item.request.page_number: item for item in chunk_snapshots}
    if set(by_page) != set(range(expected_chunks)):
        raise ValueError("ECB FOEDB chunk pages are not contiguous")
    for index in range(expected_chunks):
        snapshot = by_page[index]
        expected_uri = (
            f"{version_base}/data/{index // group_size}/chunk_{index}.json"
        )
        if snapshot.request.uri != expected_uri:
            raise ValueError("ECB FOEDB chunk URI differs")
        artifacts.append(_artifact(snapshot, EcbArtifactRole.FOEDB_CHUNK))
        try:
            flat = _sequence(json.loads(snapshot.content), "FOEDB chunk")
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB FOEDB chunk is invalid") from exc
        if len(flat) % len(header):
            raise ValueError("ECB FOEDB chunk has a partial record")
        records.extend(
            dict(zip(header, flat[offset : offset + len(header)], strict=True))
            for offset in range(0, len(flat), len(header))
        )
    if len(records) != total:
        raise ValueError("ECB FOEDB total record count differs")
    return _EcbFoedbDatabaseV1(
        version=version,
        version_hash=version_hash,
        total_records=total,
        chunk_size=chunk_size,
        chunk_group_size=group_size,
        artifacts=tuple(artifacts),
        records=tuple(records),
    )


def _publication_time(
    release_date: str, database_timestamp: int
) -> tuple[EconomicTimePrecision, int | None, str | None]:
    timestamp = datetime.fromtimestamp(database_timestamp, timezone.utc)
    local = timestamp.astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
    # FOEDB encodes legacy records as local midnight (plus one anomalous 02:00
    # value on 7 September 2017).  From 26 October 2017 it carries the actual
    # 13:45 or 14:15 decision publication clock.
    if release_date >= "2017-10-26":
        if local.date().isoformat() != release_date:
            raise ValueError("ECB exact publication timestamp has wrong date")
        lexical = local.isoformat(timespec="minutes")
        return (
            EconomicTimePrecision.EXACT_MINUTE,
            database_timestamp * 1_000_000_000,
            lexical,
        )
    return EconomicTimePrecision.DATE_ONLY, None, None


def build_ecb_foedb_release_index(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EcbFoedbReleaseIndexV1:
    """Parse every FOEDB record and select the exact decision series."""
    as_of = _iso_date(as_of_date, "as_of_date")
    database = _decode_ecb_foedb_database(
        versions_snapshot, metadata_snapshot, chunk_snapshots
    )

    publications: list[EcbFoedbPublicationV1] = []
    for record in database.records:
        properties = _mapping(
            record.get("publicationProperties") or {},
            "publicationProperties",
        )
        title = _optional_text(properties.get("Title"))
        if record.get("type") != 92 or title not in _DECISION_TITLES:
            continue
        paths = _sequence(record.get("documentTypes"), "documentTypes")
        if len(paths) != 1:
            raise ValueError("ECB decision record must name one document")
        uri = urljoin("https://www.ecb.europa.eu", str(paths[0]))
        release_date = _document_release_date(uri)
        if release_date < ECB_PREDECESSOR_DATE or release_date > as_of:
            continue
        record_id = _bounded_int(record.get("id"), "record id", 2**63 - 1)
        timestamp = _bounded_int(
            record.get("pub_timestamp"), "publication timestamp", 2**63 - 1
        )
        precision, published_at_ns, lexical = _publication_time(
            release_date, timestamp
        )
        publications.append(
            EcbFoedbPublicationV1(
                record_id=record_id,
                database_timestamp=timestamp,
                release_date=release_date,
                source_uri=uri,
                title=title,
                time_precision=precision,
                published_at_ns=published_at_ns,
                published_lexical=lexical,
            )
        )
    return EcbFoedbReleaseIndexV1(
        as_of_date=as_of,
        database_version=database.version,
        database_version_hash=database.version_hash,
        database_total_records=database.total_records,
        database_chunk_size=database.chunk_size,
        database_chunk_group_size=database.chunk_group_size,
        database_artifacts=database.artifacts,
        publications=tuple(publications),
    )


def build_ecb_decision_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact official HTML request for every selected publication."""
    return tuple(
        _request(registry, item.source_uri, OfficialSourceFormat.HTML)
        for item in release_index.publications
    )


def parse_ecb_monetary_policy_setting(
    snapshot: OfficialRawSnapshotV1,
) -> MonetaryPolicySettingV1:
    """Parse the native three-rate setting from one ECB decision page."""
    _artifact(snapshot, EcbArtifactRole.DECISION_HTML)
    return _parse_rate_set(snapshot.content).setting


def _direction(
    previous: MonetaryPolicySettingV1, current: MonetaryPolicySettingV1
) -> MonetaryPolicyDecisionDirection:
    prior = {item.component_key: item.value for item in previous.components}
    latest = {item.component_key: item.value for item in current.components}
    if set(prior) != set(_COMPONENT_PHRASES) or set(latest) != set(prior):
        raise ValueError("ECB policy-setting components differ")
    deltas = [round(latest[key] - prior[key], 10) for key in prior]
    if all(value == 0 for value in deltas):
        return MonetaryPolicyDecisionDirection.HOLD
    if all(value >= 0 for value in deltas):
        return MonetaryPolicyDecisionDirection.TIGHTEN
    if all(value <= 0 for value in deltas):
        return MonetaryPolicyDecisionDirection.EASE
    raise ValueError("ECB decision changes rate components in mixed directions")


def build_ecb_monetary_policy_archive_manifest(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbReleaseIndexV1,
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> EcbMonetaryPolicyArchiveManifestV1:
    """Reconstruct the complete ordered policy setting lineage."""
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    expected_uris = {item.source_uri for item in release_index.publications}
    if set(report_snapshots) != expected_uris:
        raise ValueError("ECB retained report inventory differs")
    predecessor_entry = release_index.publications[0]
    predecessor_snapshot = report_snapshots[predecessor_entry.source_uri]
    predecessor_parsed = _parse_rate_set(predecessor_snapshot.content)
    predecessor = EcbPolicyPredecessorV1(
        publication_id=predecessor_entry.publication_id,
        setting=predecessor_parsed.setting,
        artifact=_artifact(predecessor_snapshot, EcbArtifactRole.DECISION_HTML),
        source_excerpt=predecessor_parsed.source_excerpt,
    )
    previous = predecessor.setting
    decisions: list[EcbMonetaryPolicyDecisionV1] = []
    for entry in release_index.publications[1:]:
        snapshot = report_snapshots[entry.source_uri]
        parsed = _parse_rate_set(snapshot.content)
        decision = EcbMonetaryPolicyDecisionV1(
            publication_id=entry.publication_id,
            release_date=entry.release_date,
            action_timing=(
                MonetaryPolicyActionTiming.EMERGENCY
                if entry.release_date in _EMERGENCY_RELEASE_DATES
                else MonetaryPolicyActionTiming.SCHEDULED
            ),
            previous_setting=previous,
            new_setting=parsed.setting,
            direction=_direction(previous, parsed.setting),
            artifact=_artifact(snapshot, EcbArtifactRole.DECISION_HTML),
            source_excerpt=parsed.source_excerpt,
            expectation_unavailable_reason=(
                "The official ECB decision archive does not publish a "
                "pre-decision event-consensus rate set."
            ),
            limitations=(
                "The archive qualifies the three key ECB interest rates; asset purchases, liquidity operations, and forward guidance remain in the retained source text but are not reduced to this rate-set field.",
                "A scheduled decision classification identifies the ordinary Governing Council series; separate contemporaneous calendar evidence is required before asserting a historical scheduled clock.",
            ),
        )
        decisions.append(decision)
        previous = parsed.setting

    report_artifacts = (
        predecessor.artifact,
        *(item.artifact for item in decisions),
    )
    artifacts = (*release_index.database_artifacts, *report_artifacts)
    exact = sum(item.exact_minute for item in release_index.publications[1:])
    return EcbMonetaryPolicyArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        release_index=release_index,
        predecessor=predecessor,
        decisions=tuple(decisions),
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        exact_minute_count=exact,
        date_only_count=len(decisions) - exact,
        emergency_count=sum(
            item.action_timing is MonetaryPolicyActionTiming.EMERGENCY
            for item in decisions
        ),
    )


def replay_ecb_monetary_policy_archive(
    registry: OfficialSourceRegistryV1,
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    expected: EcbMonetaryPolicyArchiveManifestV1,
) -> EcbMonetaryPolicyArchiveManifestV1:
    """Rebuild the full retained corpus and require deterministic equality."""
    index = build_ecb_foedb_release_index(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_ecb_monetary_policy_archive_manifest(
        registry, index, report_snapshots
    )
    if rebuilt != expected:
        raise ValueError("ECB retained-corpus replay differs")
    return rebuilt


def packaged_ecb_monetary_policy_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / (
        "ecb_monetary_policy_archive_v1.json"
    )


def load_packaged_ecb_monetary_policy_archive_manifest() -> (
    EcbMonetaryPolicyArchiveManifestV1
):
    """Load and validate the packaged compact ECB archive manifest."""
    return EcbMonetaryPolicyArchiveManifestV1.from_json(
        packaged_ecb_monetary_policy_manifest_path().read_text(encoding="utf-8")
    )


__all__ = [
    "ECB_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "ECB_ARCHIVE_START_DATE",
    "ECB_ARTIFACT_SCHEMA_VERSION",
    "ECB_DECISION_SCHEMA_VERSION",
    "ECB_FOEDB_ENTRY_SCHEMA_VERSION",
    "ECB_FOEDB_INDEX_SCHEMA_VERSION",
    "ECB_FOEDB_ROOT_URI",
    "ECB_FOEDB_VERSIONS_URI",
    "ECB_LATEST_PACKAGED_DECISION_DATE",
    "ECB_MONETARY_POLICY_PARSER_ID",
    "ECB_MONETARY_POLICY_PROGRAM_KEY",
    "ECB_MONETARY_POLICY_SOURCE_KEY",
    "ECB_PREDECESSOR_DATE",
    "ECB_PREDECESSOR_SCHEMA_VERSION",
    "EcbArchiveArtifactV1",
    "EcbArtifactRole",
    "EcbFoedbPublicationV1",
    "EcbFoedbReleaseIndexV1",
    "EcbMonetaryPolicyArchiveManifestV1",
    "EcbMonetaryPolicyDecisionV1",
    "EcbPolicyPredecessorV1",
    "build_ecb_decision_requests",
    "build_ecb_foedb_chunk_requests",
    "build_ecb_foedb_metadata_request",
    "build_ecb_foedb_release_index",
    "build_ecb_foedb_versions_request",
    "build_ecb_monetary_policy_archive_manifest",
    "load_packaged_ecb_monetary_policy_archive_manifest",
    "packaged_ecb_monetary_policy_manifest_path",
    "parse_ecb_foedb_version",
    "parse_ecb_monetary_policy_setting",
    "replay_ecb_monetary_policy_archive",
]

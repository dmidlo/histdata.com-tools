"""Deterministic qualification of ECB monetary-policy meeting accounts.

The ECB public FOEDB database supplies the complete publication inventory.
This module selects publication type 20, retains every official HTML account,
parses the source-authored meeting dates, and links ordinary meetings to the
separately qualified policy-decision lineage.  Strategy-review and emergency
meetings remain explicit account events rather than being forced onto a rate
decision that the qualified type-92 series does not contain.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.ecb_monetary_policy_archive import (
    ECB_MONETARY_POLICY_SOURCE_KEY,
    ECB_SOURCE_TIMEZONE,
    EcbArchiveArtifactV1,
    EcbArtifactRole,
    EcbMonetaryPolicyArchiveManifestV1,
    _artifact,
    _bounded_int,
    _decode_ecb_foedb_database,
    _https_uri,
    _iso_date,
    _mapping,
    _optional_text,
    _request,
    _required_text,
    _sequence,
    _stable_id,
)
from histdatacom.market_context.economic_calendar import EconomicTimePrecision
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

ECB_MONETARY_POLICY_ACCOUNTS_PROGRAM_KEY: Final = (
    "ea.ecb.monetary-policy-accounts"
)
ECB_ACCOUNT_ARCHIVE_START_DATE: Final = "2015-02-19"
ECB_LATEST_PACKAGED_ACCOUNT_DATE: Final = "2026-08-27"

ECB_ACCOUNT_ENTRY_SCHEMA_VERSION: Final = "histdatacom.ecb-account-entry.v1"
ECB_ACCOUNT_INDEX_SCHEMA_VERSION: Final = "histdatacom.ecb-account-index.v1"
ECB_ACCOUNT_SCHEMA_VERSION: Final = "histdatacom.ecb-account.v1"
ECB_ACCOUNT_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-policy-accounts-archive-manifest.v1"
)

MAX_ECB_ACCOUNTS: Final = 512
MAX_ECB_ACCOUNT_HEADING_CHARS: Final = 1_024
MAX_ECB_ACCOUNT_LAG_DAYS: Final = 120
MAX_ECB_ACCOUNT_TOTAL_BYTES: Final = 256_000_000

_ACCOUNT_URI_DATE_RE = re.compile(r"(?:ecb\.)?mg(?P<date>\d{6})")
_MEETING_TITLE_RE = re.compile(r"^Meeting of\s+.+$")
_MEETING_DATE_RE = re.compile(
    r"(?P<start>\d{1,2})(?:-(?P<end>\d{1,2}))?\s+"
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)"
    r"(?:\s+(?P<year>20\d{2}))?"
)
_MONTHS: Final[Mapping[str, int]] = {
    name: index
    for index, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}
_GENERIC_ACCOUNT_TITLES: Final = frozenset(
    {
        "Account of the monetary policy meeting",
        (
            "Account of the monetary policy meeting of the Governing "
            "Council of the European Central Bank"
        ),
    }
)
_EMERGENCY_ACCOUNT_MEETING_DATES: Final = frozenset({"2020-03-18"})
_STRATEGY_ACCOUNT_MEETING_DATES: Final = frozenset({"2021-07-07", "2025-06-25"})


def _account_release_date(uri: str) -> str:
    match = _ACCOUNT_URI_DATE_RE.search(uri)
    if match is None:
        raise ValueError("ECB account URI omits its publication date")
    token = match.group("date")
    return date(
        2000 + int(token[:2]), int(token[2:4]), int(token[4:6])
    ).isoformat()


def _texts(values: Sequence[str], name: str) -> tuple[str, ...]:
    result = tuple(_required_text(item, name) for item in values)
    if not result:
        raise ValueError(f"{name} requires at least one item")
    return result


def _valid_source_title(value: object) -> str:
    title = _required_text(value, "source_title")
    if (
        title not in _GENERIC_ACCOUNT_TITLES
        and _MEETING_TITLE_RE.fullmatch(title) is None
    ):
        raise ValueError("ECB account source title is outside known eras")
    return title


class EcbAccountKind(str, Enum):
    """Evidence-backed kind of Governing Council account."""

    MONETARY_POLICY = "monetary-policy"
    EMERGENCY_ACTION = "emergency-action"
    STRATEGY_REVIEW = "strategy-review"


@dataclass(frozen=True, slots=True)
class EcbFoedbAccountPublicationV1:
    """One exactly selected FOEDB type-20 account publication."""

    record_id: int
    database_timestamp: int
    release_date: str
    source_uri: str
    source_title: str
    time_precision: EconomicTimePrecision
    published_at_ns: int | None
    published_lexical: str | None
    publication_id: str = ""
    schema_version: str = ECB_ACCOUNT_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_ACCOUNT_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported ECB account-entry schema")
        _bounded_int(self.record_id, "record_id", 2**63 - 1)
        _bounded_int(self.database_timestamp, "database_timestamp", 2**63 - 1)
        release = _iso_date(self.release_date, "release_date")
        uri = _https_uri(self.source_uri, "source_uri")
        if _account_release_date(uri) != release:
            raise ValueError("ECB account URI and release date differ")
        title = _valid_source_title(self.source_title)
        precision = EconomicTimePrecision.from_value(self.time_precision)
        lexical = _optional_text(self.published_lexical)
        published = self.published_at_ns
        if precision is EconomicTimePrecision.DATE_ONLY:
            if published is not None or lexical is not None:
                raise ValueError("date-only ECB account invents an exact clock")
        elif precision is EconomicTimePrecision.EXACT_MINUTE:
            if published is None or lexical is None:
                raise ValueError("exact ECB account omits its source clock")
            _bounded_int(published, "published_at_ns", 2**63 - 1)
            parsed = datetime.fromisoformat(lexical)
            if parsed.tzinfo is None or parsed.date().isoformat() != release:
                raise ValueError("ECB account publication clock is invalid")
            if published != self.database_timestamp * 1_000_000_000:
                raise ValueError("ECB account epoch differs from FOEDB")
        else:
            raise ValueError("ECB account time precision is unsupported")
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "published_lexical", lexical)
        expected = _stable_id(
            "ecb-account-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("ECB account publication identity differs")
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
            "source_title": self.source_title,
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
    def from_dict(cls, data: Mapping[str, Any]) -> EcbFoedbAccountPublicationV1:
        return cls(
            record_id=cast(int, data.get("record_id")),
            database_timestamp=cast(int, data.get("database_timestamp")),
            release_date=str(data.get("release_date", "")),
            source_uri=str(data.get("source_uri", "")),
            source_title=str(data.get("source_title", "")),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            published_at_ns=cast(int | None, data.get("published_at_ns")),
            published_lexical=_optional_text(data.get("published_lexical")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbFoedbAccountReleaseIndexV1:
    """Hash-bound FOEDB database version and exact type-20 selection."""

    as_of_date: str
    database_version: str
    database_version_hash: str
    database_total_records: int
    database_chunk_size: int
    database_chunk_group_size: int
    database_artifacts: tuple[EcbArchiveArtifactV1, ...]
    publications: tuple[EcbFoedbAccountPublicationV1, ...]
    index_id: str = ""
    schema_version: str = ECB_ACCOUNT_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_ACCOUNT_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported ECB account-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        version = _required_text(self.database_version, "database_version")
        version_hash = _required_text(
            self.database_version_hash, "database_version_hash"
        )
        total = _bounded_int(
            self.database_total_records,
            "database_total_records",
            100_000,
        )
        chunk_size = _bounded_int(
            self.database_chunk_size, "database_chunk_size", 100_000
        )
        group_size = _bounded_int(
            self.database_chunk_group_size,
            "database_chunk_group_size",
            100_000,
        )
        if not chunk_size or not group_size:
            raise ValueError("ECB account-index chunk sizes must be positive")
        artifacts = tuple(self.database_artifacts)
        roles = [item.role for item in artifacts]
        expected_chunks = math.ceil(total / chunk_size)
        if roles.count(EcbArtifactRole.FOEDB_VERSIONS) != 1:
            raise ValueError("ECB account index requires one version artifact")
        if roles.count(EcbArtifactRole.FOEDB_METADATA) != 1:
            raise ValueError("ECB account index requires one metadata artifact")
        if roles.count(EcbArtifactRole.FOEDB_CHUNK) != expected_chunks:
            raise ValueError("ECB account index has incomplete chunks")
        database_roles = {
            EcbArtifactRole.FOEDB_VERSIONS,
            EcbArtifactRole.FOEDB_METADATA,
            EcbArtifactRole.FOEDB_CHUNK,
        }
        if any(role not in database_roles for role in roles):
            raise ValueError("ECB report pages do not belong in database index")
        publications = tuple(
            sorted(
                self.publications,
                key=lambda item: (item.release_date, item.source_uri),
            )
        )
        if not 1 <= len(publications) <= MAX_ECB_ACCOUNTS:
            raise ValueError("ECB account publication count is outside bounds")
        if publications[0].release_date != ECB_ACCOUNT_ARCHIVE_START_DATE:
            raise ValueError("ECB account archive starts at the wrong release")
        if publications[-1].release_date > as_of:
            raise ValueError("ECB account publication follows as-of boundary")
        if len({item.record_id for item in publications}) != len(publications):
            raise ValueError("ECB account index repeats a FOEDB record")
        if len({item.source_uri for item in publications}) != len(publications):
            raise ValueError("ECB account index repeats a source URI")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "database_version", version)
        object.__setattr__(self, "database_version_hash", version_hash)
        object.__setattr__(self, "database_artifacts", artifacts)
        object.__setattr__(self, "publications", publications)
        expected = _stable_id("ecb-account-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("ECB account-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_publication_id(self) -> Mapping[str, EcbFoedbAccountPublicationV1]:
        return {item.publication_id: item for item in self.publications}

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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbFoedbAccountReleaseIndexV1:
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
                EcbArchiveArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(
                    data.get("database_artifacts"), "database_artifacts"
                )
            ),
            publications=tuple(
                EcbFoedbAccountPublicationV1.from_dict(
                    _mapping(item, "publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryPolicyAccountV1:
    """One source-published account linked to its meeting and decision."""

    publication_id: str
    release_date: str
    meeting_start_date: str
    meeting_end_date: str
    account_kind: EcbAccountKind
    linked_decision_publication_id: str | None
    release_lag_days: int
    meeting_year_inferred: bool
    artifact: EcbArchiveArtifactV1
    source_heading: str
    limitations: tuple[str, ...]
    account_id: str = ""
    schema_version: str = ECB_ACCOUNT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_ACCOUNT_SCHEMA_VERSION:
            raise ValueError("unsupported ECB account schema")
        publication_id = _required_text(self.publication_id, "publication_id")
        if not publication_id.startswith("ecb-account-publication:sha256:"):
            raise ValueError("ECB account publication identity is invalid")
        release = _iso_date(self.release_date, "release_date")
        start = _iso_date(self.meeting_start_date, "meeting_start_date")
        end = _iso_date(self.meeting_end_date, "meeting_end_date")
        if not start <= end <= release:
            raise ValueError("ECB account meeting/release dates are unordered")
        if (date.fromisoformat(end) - date.fromisoformat(start)).days > 7:
            raise ValueError("ECB account meeting span exceeds bound")
        lag = (date.fromisoformat(release) - date.fromisoformat(end)).days
        if lag != self.release_lag_days or not 0 <= lag <= (
            MAX_ECB_ACCOUNT_LAG_DAYS
        ):
            raise ValueError("ECB account release lag differs")
        kind = EcbAccountKind(self.account_kind)
        linked = _optional_text(self.linked_decision_publication_id)
        if kind is EcbAccountKind.MONETARY_POLICY:
            if linked is None or not linked.startswith(
                "ecb-foedb-publication:sha256:"
            ):
                raise ValueError("ordinary ECB account lacks its decision link")
        elif linked is not None:
            raise ValueError("exception ECB account invents a decision link")
        if self.artifact.role is not EcbArtifactRole.ACCOUNT_HTML:
            raise ValueError("ECB account requires an account HTML artifact")
        if not isinstance(self.meeting_year_inferred, bool):
            raise TypeError("meeting_year_inferred must be a boolean")
        heading = _required_text(self.source_heading, "source_heading")
        if len(heading) > MAX_ECB_ACCOUNT_HEADING_CHARS:
            raise ValueError("ECB account source heading exceeds bound")
        limitations = _texts(self.limitations, "limitations")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "meeting_start_date", start)
        object.__setattr__(self, "meeting_end_date", end)
        object.__setattr__(self, "account_kind", kind)
        object.__setattr__(self, "linked_decision_publication_id", linked)
        object.__setattr__(self, "source_heading", heading)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("ecb-account", self.identity_payload())
        if self.account_id and self.account_id != expected:
            raise ValueError("ECB account identity differs")
        object.__setattr__(self, "account_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "release_date": self.release_date,
            "meeting_start_date": self.meeting_start_date,
            "meeting_end_date": self.meeting_end_date,
            "account_kind": self.account_kind.value,
            "linked_decision_publication_id": (
                self.linked_decision_publication_id
            ),
            "release_lag_days": self.release_lag_days,
            "meeting_year_inferred": self.meeting_year_inferred,
            "artifact": self.artifact.to_dict(),
            "source_heading": self.source_heading,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "account_id": self.account_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbMonetaryPolicyAccountV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            release_date=str(data.get("release_date", "")),
            meeting_start_date=str(data.get("meeting_start_date", "")),
            meeting_end_date=str(data.get("meeting_end_date", "")),
            account_kind=EcbAccountKind(str(data.get("account_kind", ""))),
            linked_decision_publication_id=_optional_text(
                data.get("linked_decision_publication_id")
            ),
            release_lag_days=cast(int, data.get("release_lag_days")),
            meeting_year_inferred=cast(bool, data.get("meeting_year_inferred")),
            artifact=EcbArchiveArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            source_heading=str(data.get("source_heading", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            account_id=str(data.get("account_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryPolicyAccountsArchiveManifestV1:
    """Complete content-addressed ECB account archive qualification."""

    registry_id: str
    source_id: str
    decision_archive_manifest_id: str
    release_index: EcbFoedbAccountReleaseIndexV1
    accounts: tuple[EcbMonetaryPolicyAccountV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    exact_minute_count: int
    date_only_count: int
    linked_decision_count: int
    exception_count: int
    inferred_meeting_year_count: int
    minimum_release_lag_days: int
    maximum_release_lag_days: int
    manifest_id: str = ""
    schema_version: str = ECB_ACCOUNT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_ACCOUNT_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported ECB account-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        decision_id = _required_text(
            self.decision_archive_manifest_id,
            "decision_archive_manifest_id",
        )
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "ECB account manifest registry identity is invalid"
            )
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError("ECB account manifest source identity is invalid")
        if not decision_id.startswith("ecb-archive-manifest:sha256:"):
            raise ValueError("ECB account decision identity is invalid")
        if not isinstance(self.release_index, EcbFoedbAccountReleaseIndexV1):
            raise TypeError("ECB account manifest requires a release index")
        accounts = tuple(
            sorted(
                self.accounts,
                key=lambda item: (item.release_date, item.artifact.source_uri),
            )
        )
        publications = self.release_index.by_publication_id
        if [item.publication_id for item in accounts] != [
            item.publication_id for item in self.release_index.publications
        ]:
            raise ValueError("ECB accounts differ from FOEDB selection")
        for account in accounts:
            publication = publications[account.publication_id]
            if (
                account.release_date != publication.release_date
                or account.artifact.source_uri != publication.source_uri
            ):
                raise ValueError("ECB account differs from publication index")
        artifacts = (
            *self.release_index.database_artifacts,
            *(item.artifact for item in accounts),
        )
        expected_counts = (
            len(artifacts),
            len({item.content_sha256 for item in artifacts}),
            sum(item.content_length for item in artifacts),
            sum(item.exact_minute for item in self.release_index.publications),
            sum(
                not item.exact_minute
                for item in self.release_index.publications
            ),
            sum(
                item.linked_decision_publication_id is not None
                for item in accounts
            ),
            sum(
                item.account_kind is not EcbAccountKind.MONETARY_POLICY
                for item in accounts
            ),
            sum(item.meeting_year_inferred for item in accounts),
            min(item.release_lag_days for item in accounts),
            max(item.release_lag_days for item in accounts),
        )
        actual_counts = (
            self.raw_artifact_count,
            self.unique_content_sha256_count,
            self.total_content_bytes,
            self.exact_minute_count,
            self.date_only_count,
            self.linked_decision_count,
            self.exception_count,
            self.inferred_meeting_year_count,
            self.minimum_release_lag_days,
            self.maximum_release_lag_days,
        )
        if expected_counts != actual_counts:
            raise ValueError("ECB account manifest summary counts differ")
        if self.total_content_bytes > MAX_ECB_ACCOUNT_TOTAL_BYTES:
            raise ValueError("ECB account corpus exceeds byte bound")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "decision_archive_manifest_id", decision_id)
        object.__setattr__(self, "accounts", accounts)
        expected = _stable_id(
            "ecb-account-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("ECB account-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "decision_archive_manifest_id": self.decision_archive_manifest_id,
            "release_index": self.release_index.to_dict(),
            "accounts": [item.to_dict() for item in self.accounts],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "exact_minute_count": self.exact_minute_count,
            "date_only_count": self.date_only_count,
            "linked_decision_count": self.linked_decision_count,
            "exception_count": self.exception_count,
            "inferred_meeting_year_count": self.inferred_meeting_year_count,
            "minimum_release_lag_days": self.minimum_release_lag_days,
            "maximum_release_lag_days": self.maximum_release_lag_days,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbMonetaryPolicyAccountsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            decision_archive_manifest_id=str(
                data.get("decision_archive_manifest_id", "")
            ),
            release_index=EcbFoedbAccountReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            accounts=tuple(
                EcbMonetaryPolicyAccountV1.from_dict(_mapping(item, "account"))
                for item in _sequence(data.get("accounts"), "accounts")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            date_only_count=cast(int, data.get("date_only_count")),
            linked_decision_count=cast(int, data.get("linked_decision_count")),
            exception_count=cast(int, data.get("exception_count")),
            inferred_meeting_year_count=cast(
                int, data.get("inferred_meeting_year_count")
            ),
            minimum_release_lag_days=cast(
                int, data.get("minimum_release_lag_days")
            ),
            maximum_release_lag_days=cast(
                int, data.get("maximum_release_lag_days")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(
        cls, raw: str | bytes
    ) -> EcbMonetaryPolicyAccountsArchiveManifestV1:
        try:
            value = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB account manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(value, "ECB account manifest"))


class _EcbAccountHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._tag: str | None = None
        self._text: list[str] = []
        self.headings: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag in {"h1", "h2"} and self._tag is None:
            self._tag = tag
            self._text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == self._tag:
            heading = re.sub(r"\s+", " ", " ".join(self._text)).strip()
            if heading:
                self.headings.append(heading)
            self._tag = None
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._tag is not None:
            self._text.append(data)


@dataclass(frozen=True, slots=True)
class _ParsedAccountMeeting:
    start_date: str
    end_date: str
    account_kind: EcbAccountKind
    year_inferred: bool
    source_heading: str


def _parse_account_meeting(
    content: bytes, release_date: str
) -> _ParsedAccountMeeting:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB account page is not UTF-8") from exc
    parser = _EcbAccountHtmlParser()
    parser.feed(text)
    if not any(
        heading.startswith(
            ("Account of the monetary policy meeting", "Meeting of ")
        )
        for heading in parser.headings
    ):
        raise ValueError("ECB account page title differs")
    source_heading = next(
        (
            heading
            for heading in parser.headings
            if "held" in heading.casefold()
            and _MEETING_DATE_RE.search(heading) is not None
        ),
        None,
    )
    if source_heading is None:
        raise ValueError("ECB account page omits its meeting date heading")
    match = cast(re.Match[str], _MEETING_DATE_RE.search(source_heading))
    release = date.fromisoformat(_iso_date(release_date, "release_date"))
    month = _MONTHS[match.group("month")]
    year_text = match.group("year")
    inferred = year_text is None
    year = (
        int(year_text)
        if year_text is not None
        else release.year - int(month > release.month)
    )
    start_day = int(match.group("start"))
    end_day = int(match.group("end") or start_day)
    start = date(year, month, start_day)
    end = date(year, month, end_day)
    end_iso = end.isoformat()
    kind = EcbAccountKind.MONETARY_POLICY
    if end_iso in _EMERGENCY_ACCOUNT_MEETING_DATES:
        kind = EcbAccountKind.EMERGENCY_ACTION
    elif end_iso in _STRATEGY_ACCOUNT_MEETING_DATES:
        kind = EcbAccountKind.STRATEGY_REVIEW
    return _ParsedAccountMeeting(
        start_date=start.isoformat(),
        end_date=end_iso,
        account_kind=kind,
        year_inferred=inferred,
        source_heading=source_heading,
    )


def _account_publication_time(
    release_date: str, database_timestamp: int
) -> tuple[EconomicTimePrecision, int | None, str | None]:
    timestamp = datetime.fromtimestamp(database_timestamp, timezone.utc)
    local = timestamp.astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
    if local.date().isoformat() != release_date or local.second != 0:
        raise ValueError("ECB account FOEDB timestamp differs from release")
    clock = (local.hour, local.minute)
    if clock == (0, 0):
        return EconomicTimePrecision.DATE_ONLY, None, None
    if clock not in {(13, 30), (13, 25)}:
        raise ValueError("ECB account publication clock is outside known eras")
    if clock == (13, 25) and release_date != "2025-08-28":
        raise ValueError(
            "ECB account 13:25 clock is outside its known exception"
        )
    return (
        EconomicTimePrecision.EXACT_MINUTE,
        database_timestamp * 1_000_000_000,
        local.isoformat(timespec="minutes"),
    )


def build_ecb_foedb_account_release_index(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EcbFoedbAccountReleaseIndexV1:
    """Select every FOEDB type-20 account at an explicit boundary."""
    as_of = _iso_date(as_of_date, "as_of_date")
    database = _decode_ecb_foedb_database(
        versions_snapshot, metadata_snapshot, chunk_snapshots
    )
    publications: list[EcbFoedbAccountPublicationV1] = []
    for record in database.records:
        if record.get("type") != 20:
            continue
        properties = _mapping(
            record.get("publicationProperties") or {},
            "publicationProperties",
        )
        title = _valid_source_title(properties.get("Title"))
        paths = _sequence(record.get("documentTypes"), "documentTypes")
        if len(paths) != 1:
            raise ValueError("ECB account record must name one document")
        uri = urljoin("https://www.ecb.europa.eu", str(paths[0]))
        release_date = _account_release_date(uri)
        if (
            release_date < ECB_ACCOUNT_ARCHIVE_START_DATE
            or release_date > as_of
        ):
            continue
        record_id = _bounded_int(record.get("id"), "record id", 2**63 - 1)
        timestamp = _bounded_int(
            record.get("pub_timestamp"), "publication timestamp", 2**63 - 1
        )
        precision, published_at_ns, lexical = _account_publication_time(
            release_date, timestamp
        )
        publications.append(
            EcbFoedbAccountPublicationV1(
                record_id=record_id,
                database_timestamp=timestamp,
                release_date=release_date,
                source_uri=uri,
                source_title=title,
                time_precision=precision,
                published_at_ns=published_at_ns,
                published_lexical=lexical,
            )
        )
    return EcbFoedbAccountReleaseIndexV1(
        as_of_date=as_of,
        database_version=database.version,
        database_version_hash=database.version_hash,
        database_total_records=database.total_records,
        database_chunk_size=database.chunk_size,
        database_chunk_group_size=database.chunk_group_size,
        database_artifacts=database.artifacts,
        publications=tuple(publications),
    )


def build_ecb_account_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbAccountReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request for every selected account."""
    return tuple(
        _request(registry, item.source_uri, OfficialSourceFormat.HTML)
        for item in release_index.publications
    )


def build_ecb_monetary_policy_accounts_archive_manifest(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbAccountReleaseIndexV1,
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    decision_archive: EcbMonetaryPolicyArchiveManifestV1,
) -> EcbMonetaryPolicyAccountsArchiveManifestV1:
    """Parse, link, and hash every selected ECB monetary-policy account."""
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    if decision_archive.registry_id != registry.registry_id:
        raise ValueError("ECB account and decision registry identities differ")
    expected_uris = {item.source_uri for item in release_index.publications}
    if set(report_snapshots) != expected_uris:
        raise ValueError("ECB retained account inventory differs")
    decisions_by_date = {
        item.release_date: item for item in decision_archive.decisions
    }
    accounts: list[EcbMonetaryPolicyAccountV1] = []
    for publication in release_index.publications:
        snapshot = report_snapshots[publication.source_uri]
        parsed = _parse_account_meeting(
            snapshot.content, publication.release_date
        )
        linked: str | None = None
        if parsed.account_kind is EcbAccountKind.MONETARY_POLICY:
            decision = decisions_by_date.get(parsed.end_date)
            if decision is None:
                raise ValueError(
                    "ordinary ECB account has no qualified decision occurrence"
                )
            linked = decision.publication_id
        limitation = (
            "This account covers the 18 March 2020 emergency PEPP meeting; the qualified type-92 three-rate decision series has no same-day occurrence."
            if parsed.account_kind is EcbAccountKind.EMERGENCY_ACTION
            else (
                "This account covers a monetary-policy strategy review rather than a three-rate decision occurrence."
                if parsed.account_kind is EcbAccountKind.STRATEGY_REVIEW
                else "The account is linked to the exact qualified decision occurrence by its source-authored meeting end date."
            )
        )
        accounts.append(
            EcbMonetaryPolicyAccountV1(
                publication_id=publication.publication_id,
                release_date=publication.release_date,
                meeting_start_date=parsed.start_date,
                meeting_end_date=parsed.end_date,
                account_kind=parsed.account_kind,
                linked_decision_publication_id=linked,
                release_lag_days=(
                    date.fromisoformat(publication.release_date)
                    - date.fromisoformat(parsed.end_date)
                ).days,
                meeting_year_inferred=parsed.year_inferred,
                artifact=_artifact(snapshot, EcbArtifactRole.ACCOUNT_HTML),
                source_heading=parsed.source_heading,
                limitations=(
                    limitation,
                    "Accounts are documentary releases and do not publish a numeric event-consensus value or surprise.",
                ),
            )
        )
    artifacts = (
        *release_index.database_artifacts,
        *(item.artifact for item in accounts),
    )
    lags = [item.release_lag_days for item in accounts]
    return EcbMonetaryPolicyAccountsArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        decision_archive_manifest_id=decision_archive.manifest_id,
        release_index=release_index,
        accounts=tuple(accounts),
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        exact_minute_count=sum(
            item.exact_minute for item in release_index.publications
        ),
        date_only_count=sum(
            not item.exact_minute for item in release_index.publications
        ),
        linked_decision_count=sum(
            item.linked_decision_publication_id is not None for item in accounts
        ),
        exception_count=sum(
            item.account_kind is not EcbAccountKind.MONETARY_POLICY
            for item in accounts
        ),
        inferred_meeting_year_count=sum(
            item.meeting_year_inferred for item in accounts
        ),
        minimum_release_lag_days=min(lags),
        maximum_release_lag_days=max(lags),
    )


def replay_ecb_monetary_policy_accounts_archive(
    registry: OfficialSourceRegistryV1,
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    decision_archive: EcbMonetaryPolicyArchiveManifestV1,
    expected: EcbMonetaryPolicyAccountsArchiveManifestV1,
) -> EcbMonetaryPolicyAccountsArchiveManifestV1:
    """Rebuild the retained account corpus and require exact equality."""
    index = build_ecb_foedb_account_release_index(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_ecb_monetary_policy_accounts_archive_manifest(
        registry,
        index,
        report_snapshots,
        decision_archive,
    )
    if rebuilt != expected:
        raise ValueError("ECB account retained-corpus replay differs")
    return rebuilt


def packaged_ecb_monetary_policy_accounts_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / (
        "ecb_monetary_policy_accounts_archive_v1.json"
    )


def load_packaged_ecb_monetary_policy_accounts_archive_manifest() -> (
    EcbMonetaryPolicyAccountsArchiveManifestV1
):
    """Load and validate the packaged compact ECB account manifest."""
    return EcbMonetaryPolicyAccountsArchiveManifestV1.from_json(
        packaged_ecb_monetary_policy_accounts_manifest_path().read_text(
            encoding="utf-8"
        )
    )


__all__ = [
    "ECB_ACCOUNT_ARCHIVE_START_DATE",
    "ECB_ACCOUNT_ENTRY_SCHEMA_VERSION",
    "ECB_ACCOUNT_INDEX_SCHEMA_VERSION",
    "ECB_ACCOUNT_MANIFEST_SCHEMA_VERSION",
    "ECB_ACCOUNT_SCHEMA_VERSION",
    "ECB_LATEST_PACKAGED_ACCOUNT_DATE",
    "ECB_MONETARY_POLICY_ACCOUNTS_PROGRAM_KEY",
    "EcbAccountKind",
    "EcbFoedbAccountPublicationV1",
    "EcbFoedbAccountReleaseIndexV1",
    "EcbMonetaryPolicyAccountV1",
    "EcbMonetaryPolicyAccountsArchiveManifestV1",
    "build_ecb_account_requests",
    "build_ecb_foedb_account_release_index",
    "build_ecb_monetary_policy_accounts_archive_manifest",
    "load_packaged_ecb_monetary_policy_accounts_archive_manifest",
    "packaged_ecb_monetary_policy_accounts_manifest_path",
    "replay_ecb_monetary_policy_accounts_archive",
]

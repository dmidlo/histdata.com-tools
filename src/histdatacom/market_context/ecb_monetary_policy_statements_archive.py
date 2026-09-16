"""Deterministic qualification of ECB monetary-policy statements.

The ECB public FOEDB database supplies the publication inventory.  This module
selects only type-54 statement pages whose source title is part of the known
monetary-policy series and whose date resolves to an independently qualified
policy decision.  Unrelated press events stored under the same historical URL
family remain outside the qualified archive.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
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

ECB_MONETARY_POLICY_STATEMENTS_PROGRAM_KEY: Final = (
    "ea.ecb.monetary-policy-statements"
)
ECB_STATEMENT_ARCHIVE_START_DATE: Final = "2000-01-05"
ECB_LATEST_PACKAGED_STATEMENT_DATE: Final = "2026-09-10"

ECB_STATEMENT_ENTRY_SCHEMA_VERSION: Final = "histdatacom.ecb-statement-entry.v1"
ECB_STATEMENT_INDEX_SCHEMA_VERSION: Final = "histdatacom.ecb-statement-index.v1"
ECB_STATEMENT_SCHEMA_VERSION: Final = "histdatacom.ecb-statement.v1"
ECB_STATEMENT_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-policy-statements-archive-manifest.v1"
)

MAX_ECB_STATEMENTS: Final = 512
MAX_ECB_STATEMENT_HEADING_CHARS: Final = 1_024
MAX_ECB_STATEMENT_TOTAL_BYTES: Final = 256_000_000

_STATEMENT_URI_DATE_RE = re.compile(r"(?:ecb\.)?is(?P<date>\d{6})")
_STATEMENT_TITLES: Final = frozenset(
    {
        "Introductory statement with Q&A",
        "Introductory statement to the press conference (with Q&A)",
        "Monetary policy statement (with Q&A)",
        "Introductory statement with the Q&A",
        "Introductory statement to the press conference (with Q& A)",
        "Transcript of the Press Briefing",
    }
)
_STATEMENT_HEADINGS: Final = frozenset(
    {
        "PRESS CONFERENCE",
        "Introductory statement with Q&A",
        "Introductory statement to the press conference (with Q&A)",
        "Introductory statement",
        "ECB Press conference: Introductory statement",
        "Introductory statement to the press conference",
        "Introductory statement with the Q&A",
        "Introductory statement to the press conference (with Q& A)",
        "Transcript of the Press Briefing",
    }
)


def _statement_release_date(uri: str) -> str:
    match = _STATEMENT_URI_DATE_RE.search(uri)
    if match is None:
        raise ValueError("ECB statement URI omits its publication date")
    token = match.group("date")
    century = 2000 if int(token[:2]) < 90 else 1900
    return date(
        century + int(token[:2]), int(token[2:4]), int(token[4:6])
    ).isoformat()


def _texts(values: Sequence[str], name: str) -> tuple[str, ...]:
    result = tuple(_required_text(item, name) for item in values)
    if not result:
        raise ValueError(f"{name} requires at least one item")
    return result


@dataclass(frozen=True, slots=True)
class EcbFoedbStatementPublicationV1:
    """One exactly selected FOEDB type-54 policy statement."""

    record_id: int
    database_timestamp: int
    release_date: str
    source_uri: str
    source_title: str
    time_precision: EconomicTimePrecision
    published_at_ns: int | None
    published_lexical: str | None
    publication_id: str = ""
    schema_version: str = ECB_STATEMENT_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_STATEMENT_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported ECB statement-entry schema")
        _bounded_int(self.record_id, "record_id", 2**63 - 1)
        _bounded_int(self.database_timestamp, "database_timestamp", 2**63 - 1)
        release = _iso_date(self.release_date, "release_date")
        uri = _https_uri(self.source_uri, "source_uri")
        if _statement_release_date(uri) != release:
            raise ValueError("ECB statement URI and release date differ")
        title = _required_text(self.source_title, "source_title")
        if title not in _STATEMENT_TITLES:
            raise ValueError("ECB statement source title is outside known eras")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        lexical = _optional_text(self.published_lexical)
        published = self.published_at_ns
        if precision is EconomicTimePrecision.DATE_ONLY:
            if published is not None or lexical is not None:
                raise ValueError(
                    "date-only ECB statement invents an exact clock"
                )
        elif precision is EconomicTimePrecision.EXACT_MINUTE:
            if published is None or lexical is None:
                raise ValueError("exact ECB statement omits its source clock")
            _bounded_int(published, "published_at_ns", 2**63 - 1)
            parsed = datetime.fromisoformat(lexical)
            if parsed.tzinfo is None or parsed.date().isoformat() != release:
                raise ValueError("ECB statement publication clock is invalid")
            if published != self.database_timestamp * 1_000_000_000:
                raise ValueError("ECB statement epoch differs from FOEDB")
        else:
            raise ValueError("ECB statement time precision is unsupported")
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "published_lexical", lexical)
        expected = _stable_id(
            "ecb-statement-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("ECB statement publication identity differs")
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbFoedbStatementPublicationV1:
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
class EcbFoedbStatementReleaseIndexV1:
    """Hash-bound FOEDB version and exact decision-linked selection."""

    as_of_date: str
    database_version: str
    database_version_hash: str
    database_total_records: int
    database_chunk_size: int
    database_chunk_group_size: int
    database_artifacts: tuple[EcbArchiveArtifactV1, ...]
    publications: tuple[EcbFoedbStatementPublicationV1, ...]
    index_id: str = ""
    schema_version: str = ECB_STATEMENT_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_STATEMENT_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported ECB statement-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        version = _required_text(self.database_version, "database_version")
        version_hash = _required_text(
            self.database_version_hash, "database_version_hash"
        )
        total = _bounded_int(
            self.database_total_records, "database_total_records", 100_000
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
            raise ValueError("ECB statement-index chunk sizes must be positive")
        artifacts = tuple(self.database_artifacts)
        roles = [item.role for item in artifacts]
        expected_chunks = math.ceil(total / chunk_size)
        if roles.count(EcbArtifactRole.FOEDB_VERSIONS) != 1:
            raise ValueError(
                "ECB statement index requires one version artifact"
            )
        if roles.count(EcbArtifactRole.FOEDB_METADATA) != 1:
            raise ValueError(
                "ECB statement index requires one metadata artifact"
            )
        if roles.count(EcbArtifactRole.FOEDB_CHUNK) != expected_chunks:
            raise ValueError("ECB statement index has incomplete chunks")
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
        if not 1 <= len(publications) <= MAX_ECB_STATEMENTS:
            raise ValueError(
                "ECB statement publication count is outside bounds"
            )
        if publications[0].release_date != ECB_STATEMENT_ARCHIVE_START_DATE:
            raise ValueError(
                "ECB statement archive starts at the wrong release"
            )
        if publications[-1].release_date > as_of:
            raise ValueError("ECB statement publication follows as-of boundary")
        if len({item.release_date for item in publications}) != len(
            publications
        ):
            raise ValueError("ECB statement index repeats a decision date")
        if len({item.record_id for item in publications}) != len(publications):
            raise ValueError("ECB statement index repeats a FOEDB record")
        if len({item.source_uri for item in publications}) != len(publications):
            raise ValueError("ECB statement index repeats a source URI")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "database_version", version)
        object.__setattr__(self, "database_version_hash", version_hash)
        object.__setattr__(self, "database_artifacts", artifacts)
        object.__setattr__(self, "publications", publications)
        expected = _stable_id("ecb-statement-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("ECB statement-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_publication_id(self) -> Mapping[str, EcbFoedbStatementPublicationV1]:
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
    ) -> EcbFoedbStatementReleaseIndexV1:
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
                EcbFoedbStatementPublicationV1.from_dict(
                    _mapping(item, "publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryPolicyStatementV1:
    """One source statement linked to its independently qualified decision."""

    publication_id: str
    release_date: str
    linked_decision_publication_id: str
    artifact: EcbArchiveArtifactV1
    source_heading: str
    limitations: tuple[str, ...]
    statement_id: str = ""
    schema_version: str = ECB_STATEMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_STATEMENT_SCHEMA_VERSION:
            raise ValueError("unsupported ECB statement schema")
        publication_id = _required_text(self.publication_id, "publication_id")
        if not publication_id.startswith("ecb-statement-publication:sha256:"):
            raise ValueError("ECB statement publication identity is invalid")
        release = _iso_date(self.release_date, "release_date")
        linked = _required_text(
            self.linked_decision_publication_id,
            "linked_decision_publication_id",
        )
        if not linked.startswith("ecb-foedb-publication:sha256:"):
            raise ValueError("ECB statement decision identity is invalid")
        if self.artifact.role is not EcbArtifactRole.STATEMENT_HTML:
            raise ValueError("ECB statement requires a statement HTML artifact")
        heading = _required_text(self.source_heading, "source_heading")
        if (
            len(heading) > MAX_ECB_STATEMENT_HEADING_CHARS
            or heading not in _STATEMENT_HEADINGS
        ):
            raise ValueError(
                "ECB statement source heading is outside known eras"
            )
        limitations = _texts(self.limitations, "limitations")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "linked_decision_publication_id", linked)
        object.__setattr__(self, "source_heading", heading)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("ecb-statement", self.identity_payload())
        if self.statement_id and self.statement_id != expected:
            raise ValueError("ECB statement identity differs")
        object.__setattr__(self, "statement_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "release_date": self.release_date,
            "linked_decision_publication_id": self.linked_decision_publication_id,
            "artifact": self.artifact.to_dict(),
            "source_heading": self.source_heading,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "statement_id": self.statement_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbMonetaryPolicyStatementV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            release_date=str(data.get("release_date", "")),
            linked_decision_publication_id=str(
                data.get("linked_decision_publication_id", "")
            ),
            artifact=EcbArchiveArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            source_heading=str(data.get("source_heading", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            statement_id=str(data.get("statement_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryPolicyStatementsArchiveManifestV1:
    """Complete content-addressed ECB policy-statement qualification."""

    registry_id: str
    source_id: str
    decision_archive_manifest_id: str
    release_index: EcbFoedbStatementReleaseIndexV1
    statements: tuple[EcbMonetaryPolicyStatementV1, ...]
    decision_without_statement_publication_ids: tuple[str, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    exact_minute_count: int
    date_only_count: int
    linked_decision_count: int
    decision_publication_count: int
    decision_without_statement_count: int
    manifest_id: str = ""
    schema_version: str = ECB_STATEMENT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_STATEMENT_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported ECB statement-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        decision_id = _required_text(
            self.decision_archive_manifest_id,
            "decision_archive_manifest_id",
        )
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("ECB statement registry identity is invalid")
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError("ECB statement source identity is invalid")
        if not decision_id.startswith("ecb-archive-manifest:sha256:"):
            raise ValueError(
                "ECB statement decision archive identity is invalid"
            )
        if not isinstance(self.release_index, EcbFoedbStatementReleaseIndexV1):
            raise TypeError("ECB statement manifest requires a release index")
        statements = tuple(
            sorted(
                self.statements,
                key=lambda item: (item.release_date, item.artifact.source_uri),
            )
        )
        publications = self.release_index.by_publication_id
        if [item.publication_id for item in statements] != [
            item.publication_id for item in self.release_index.publications
        ]:
            raise ValueError("ECB statements differ from FOEDB selection")
        for statement in statements:
            publication = publications[statement.publication_id]
            if (
                statement.release_date != publication.release_date
                or statement.artifact.source_uri != publication.source_uri
            ):
                raise ValueError("ECB statement differs from publication index")
        missing = tuple(
            sorted(
                _required_text(
                    item, "decision_without_statement_publication_id"
                )
                for item in self.decision_without_statement_publication_ids
            )
        )
        if any(
            not item.startswith("ecb-foedb-publication:sha256:")
            for item in missing
        ):
            raise ValueError(
                "ECB missing-statement decision identity is invalid"
            )
        linked = tuple(
            item.linked_decision_publication_id for item in statements
        )
        if len(set(linked)) != len(linked) or len(set(missing)) != len(missing):
            raise ValueError(
                "ECB statement decision lineage repeats an identity"
            )
        if set(linked) & set(missing):
            raise ValueError("ECB statement decision lineage overlaps absences")
        artifacts = (
            *self.release_index.database_artifacts,
            *(item.artifact for item in statements),
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
            len(linked),
            len(linked) + len(missing),
            len(missing),
        )
        actual_counts = (
            self.raw_artifact_count,
            self.unique_content_sha256_count,
            self.total_content_bytes,
            self.exact_minute_count,
            self.date_only_count,
            self.linked_decision_count,
            self.decision_publication_count,
            self.decision_without_statement_count,
        )
        if expected_counts != actual_counts:
            raise ValueError("ECB statement manifest summary counts differ")
        if self.total_content_bytes > MAX_ECB_STATEMENT_TOTAL_BYTES:
            raise ValueError("ECB statement corpus exceeds byte bound")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "decision_archive_manifest_id", decision_id)
        object.__setattr__(self, "statements", statements)
        object.__setattr__(
            self, "decision_without_statement_publication_ids", missing
        )
        expected = _stable_id(
            "ecb-statement-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("ECB statement-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "decision_archive_manifest_id": self.decision_archive_manifest_id,
            "release_index": self.release_index.to_dict(),
            "statements": [item.to_dict() for item in self.statements],
            "decision_without_statement_publication_ids": list(
                self.decision_without_statement_publication_ids
            ),
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "exact_minute_count": self.exact_minute_count,
            "date_only_count": self.date_only_count,
            "linked_decision_count": self.linked_decision_count,
            "decision_publication_count": self.decision_publication_count,
            "decision_without_statement_count": (
                self.decision_without_statement_count
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbMonetaryPolicyStatementsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            decision_archive_manifest_id=str(
                data.get("decision_archive_manifest_id", "")
            ),
            release_index=EcbFoedbStatementReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            statements=tuple(
                EcbMonetaryPolicyStatementV1.from_dict(
                    _mapping(item, "statement")
                )
                for item in _sequence(data.get("statements"), "statements")
            ),
            decision_without_statement_publication_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("decision_without_statement_publication_ids"),
                    "decision_without_statement_publication_ids",
                )
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            date_only_count=cast(int, data.get("date_only_count")),
            linked_decision_count=cast(int, data.get("linked_decision_count")),
            decision_publication_count=cast(
                int, data.get("decision_publication_count")
            ),
            decision_without_statement_count=cast(
                int, data.get("decision_without_statement_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(
        cls, raw: str | bytes
    ) -> EcbMonetaryPolicyStatementsArchiveManifestV1:
        try:
            value = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB statement manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(value, "ECB statement manifest"))


class _EcbStatementHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_h1 = False
        self._text: list[str] = []
        self.headings: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag == "h1" and not self._in_h1:
            self._in_h1 = True
            self._text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "h1" and self._in_h1:
            heading = re.sub(r"\s+", " ", " ".join(self._text)).strip()
            if heading:
                self.headings.append(heading)
            self._in_h1 = False
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._in_h1:
            self._text.append(data)


def _parse_statement_heading(content: bytes) -> str:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB statement page is not UTF-8") from exc
    parser = _EcbStatementHtmlParser()
    parser.feed(text)
    if (
        len(parser.headings) != 1
        or parser.headings[0] not in _STATEMENT_HEADINGS
    ):
        raise ValueError("ECB statement page heading differs")
    return parser.headings[0]


def _statement_publication_time(
    release_date: str, database_timestamp: int
) -> tuple[EconomicTimePrecision, int | None, str | None]:
    timestamp = datetime.fromtimestamp(database_timestamp, timezone.utc)
    local = timestamp.astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
    if local.date().isoformat() != release_date or local.second != 0:
        raise ValueError("ECB statement FOEDB timestamp differs from release")
    clock = (local.hour, local.minute)
    if clock == (0, 0):
        if release_date > "2017-07-20":
            raise ValueError(
                "ECB statement placeholder clock is outside known era"
            )
        return EconomicTimePrecision.DATE_ONLY, None, None
    if clock not in {(14, 45), (15, 0)}:
        raise ValueError(
            "ECB statement publication clock is outside known eras"
        )
    if clock == (14, 45) and release_date >= "2022-07-21":
        raise ValueError("ECB statement 14:45 clock is outside known era")
    if clock == (15, 0) and release_date < "2022-07-21":
        raise ValueError("ECB statement 15:00 clock is outside known era")
    return (
        EconomicTimePrecision.EXACT_MINUTE,
        database_timestamp * 1_000_000_000,
        local.isoformat(timespec="minutes"),
    )


def build_ecb_foedb_statement_release_index(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    decision_archive: EcbMonetaryPolicyArchiveManifestV1,
    *,
    as_of_date: str,
) -> EcbFoedbStatementReleaseIndexV1:
    """Select the exact decision-linked type-54 statement series."""
    as_of = _iso_date(as_of_date, "as_of_date")
    if as_of != decision_archive.release_index.as_of_date:
        raise ValueError("ECB statement and decision as-of boundaries differ")
    decision_dates = {item.release_date for item in decision_archive.decisions}
    database = _decode_ecb_foedb_database(
        versions_snapshot, metadata_snapshot, chunk_snapshots
    )
    publications: list[EcbFoedbStatementPublicationV1] = []
    for record in database.records:
        if record.get("type") != 54:
            continue
        properties = _mapping(
            record.get("publicationProperties") or {},
            "publicationProperties",
        )
        title = _optional_text(properties.get("Title"))
        if title not in _STATEMENT_TITLES:
            continue
        paths = _sequence(record.get("documentTypes"), "documentTypes")
        if len(paths) != 1:
            raise ValueError("ECB statement record must name one document")
        uri = urljoin("https://www.ecb.europa.eu", str(paths[0]))
        release_date = _statement_release_date(uri)
        if (
            release_date < ECB_STATEMENT_ARCHIVE_START_DATE
            or release_date > as_of
            or release_date not in decision_dates
        ):
            continue
        record_id = _bounded_int(record.get("id"), "record id", 2**63 - 1)
        timestamp = _bounded_int(
            record.get("pub_timestamp"), "publication timestamp", 2**63 - 1
        )
        precision, published_at_ns, lexical = _statement_publication_time(
            release_date, timestamp
        )
        publications.append(
            EcbFoedbStatementPublicationV1(
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
    return EcbFoedbStatementReleaseIndexV1(
        as_of_date=as_of,
        database_version=database.version,
        database_version_hash=database.version_hash,
        database_total_records=database.total_records,
        database_chunk_size=database.chunk_size,
        database_chunk_group_size=database.chunk_group_size,
        database_artifacts=database.artifacts,
        publications=tuple(publications),
    )


def build_ecb_statement_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbStatementReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request for every selected statement."""
    return tuple(
        _request(registry, item.source_uri, OfficialSourceFormat.HTML)
        for item in release_index.publications
    )


def build_ecb_monetary_policy_statements_archive_manifest(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbStatementReleaseIndexV1,
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    decision_archive: EcbMonetaryPolicyArchiveManifestV1,
) -> EcbMonetaryPolicyStatementsArchiveManifestV1:
    """Parse, link, and hash every selected ECB policy statement."""
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    if decision_archive.registry_id != registry.registry_id:
        raise ValueError(
            "ECB statement and decision registry identities differ"
        )
    if decision_archive.manifest_id == "":
        raise ValueError("ECB statement decision archive is unbound")
    expected_uris = {item.source_uri for item in release_index.publications}
    if set(report_snapshots) != expected_uris:
        raise ValueError("ECB retained statement inventory differs")
    decisions_by_date = {
        item.release_date: item for item in decision_archive.decisions
    }
    statements: list[EcbMonetaryPolicyStatementV1] = []
    for publication in release_index.publications:
        decision = decisions_by_date.get(publication.release_date)
        if decision is None:
            raise ValueError(
                "ECB statement has no qualified decision occurrence"
            )
        snapshot = report_snapshots[publication.source_uri]
        statements.append(
            EcbMonetaryPolicyStatementV1(
                publication_id=publication.publication_id,
                release_date=publication.release_date,
                linked_decision_publication_id=decision.publication_id,
                artifact=_artifact(snapshot, EcbArtifactRole.STATEMENT_HTML),
                source_heading=_parse_statement_heading(snapshot.content),
                limitations=(
                    "The statement is decision-day communication and is not a separate rate decision occurrence.",
                    "The statement does not publish a pre-decision market-consensus rate set or numeric surprise.",
                ),
            )
        )
    statement_dates = {item.release_date for item in statements}
    missing = tuple(
        item.publication_id
        for item in decision_archive.decisions
        if item.release_date not in statement_dates
    )
    artifacts = (
        *release_index.database_artifacts,
        *(item.artifact for item in statements),
    )
    return EcbMonetaryPolicyStatementsArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        decision_archive_manifest_id=decision_archive.manifest_id,
        release_index=release_index,
        statements=tuple(statements),
        decision_without_statement_publication_ids=missing,
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
        linked_decision_count=len(statements),
        decision_publication_count=len(decision_archive.decisions),
        decision_without_statement_count=len(missing),
    )


def replay_ecb_monetary_policy_statements_archive(
    registry: OfficialSourceRegistryV1,
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    decision_archive: EcbMonetaryPolicyArchiveManifestV1,
    expected: EcbMonetaryPolicyStatementsArchiveManifestV1,
) -> EcbMonetaryPolicyStatementsArchiveManifestV1:
    """Rebuild the retained statement corpus and require exact equality."""
    index = build_ecb_foedb_statement_release_index(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        decision_archive,
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_ecb_monetary_policy_statements_archive_manifest(
        registry,
        index,
        report_snapshots,
        decision_archive,
    )
    if rebuilt != expected:
        raise ValueError("ECB statement retained-corpus replay differs")
    return rebuilt


def packaged_ecb_monetary_policy_statements_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / (
        "ecb_monetary_policy_statements_archive_v1.json"
    )


def load_packaged_ecb_monetary_policy_statements_archive_manifest() -> (
    EcbMonetaryPolicyStatementsArchiveManifestV1
):
    """Load and validate the packaged compact ECB statement manifest."""
    return EcbMonetaryPolicyStatementsArchiveManifestV1.from_json(
        packaged_ecb_monetary_policy_statements_manifest_path().read_text(
            encoding="utf-8"
        )
    )


__all__ = [
    "ECB_LATEST_PACKAGED_STATEMENT_DATE",
    "ECB_MONETARY_POLICY_STATEMENTS_PROGRAM_KEY",
    "ECB_STATEMENT_ARCHIVE_START_DATE",
    "ECB_STATEMENT_ENTRY_SCHEMA_VERSION",
    "ECB_STATEMENT_INDEX_SCHEMA_VERSION",
    "ECB_STATEMENT_MANIFEST_SCHEMA_VERSION",
    "ECB_STATEMENT_SCHEMA_VERSION",
    "EcbFoedbStatementPublicationV1",
    "EcbFoedbStatementReleaseIndexV1",
    "EcbMonetaryPolicyStatementV1",
    "EcbMonetaryPolicyStatementsArchiveManifestV1",
    "build_ecb_foedb_statement_release_index",
    "build_ecb_monetary_policy_statements_archive_manifest",
    "build_ecb_statement_requests",
    "load_packaged_ecb_monetary_policy_statements_archive_manifest",
    "packaged_ecb_monetary_policy_statements_manifest_path",
    "replay_ecb_monetary_policy_statements_archive",
]

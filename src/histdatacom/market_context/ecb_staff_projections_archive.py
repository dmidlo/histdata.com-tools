"""Deterministic qualification of ECB staff projection releases.

The ECB public FOEDB database supplies the versioned publication inventory,
while the official projection all-releases page independently identifies the
main quarterly series.  This module requires those inventories to reconcile,
retains every main English HTML/PDF artifact, and records the period-projection
scope and source-supported annual horizon without relabelling it as event
consensus.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.ecb_monetary_policy_archive import (
    ECB_MONETARY_POLICY_SOURCE_KEY,
    ECB_SOURCE_TIMEZONE,
    EcbArchiveArtifactV1,
    EcbArtifactRole,
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
from histdatacom.market_context.economic_calendar import (
    EconomicForecastScope,
    EconomicTimePrecision,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

ECB_STAFF_PROJECTIONS_PROGRAM_KEY: Final = "ea.ecb.staff-projections"
ECB_PROJECTION_ARCHIVE_URI: Final = (
    "https://www.ecb.europa.eu/press/projections/html/all-releases.en.html"
)
ECB_PROJECTION_ARCHIVE_START_DATE: Final = "2004-06-03"
ECB_LATEST_PACKAGED_PROJECTION_DATE: Final = "2026-09-10"

ECB_PROJECTION_ARCHIVE_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-projection-archive-entry.v1"
)
ECB_PROJECTION_ARCHIVE_INDEX_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-projection-archive-index.v1"
)
ECB_PROJECTION_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-projection-entry.v1"
)
ECB_PROJECTION_INDEX_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-projection-index.v1"
)
ECB_PROJECTION_SCHEMA_VERSION: Final = "histdatacom.ecb-staff-projection.v1"
ECB_PROJECTION_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-staff-projections-archive-manifest.v1"
)

MAX_ECB_PROJECTIONS: Final = 256
MAX_ECB_PROJECTION_ARTIFACTS: Final = 512
MAX_ECB_PROJECTION_TOTAL_BYTES: Final = 256_000_000
MAX_ECB_PROJECTION_PDF_PAGES: Final = 128

_ROUND_RE = re.compile(
    r"(?:projections|staffprojections)(?P<round>\d{6})", re.IGNORECASE
)
_TITLE_RE = re.compile(
    r"^(?P<producer>ECB|Eurosystem) staff macroeconomic projections for the "
    r"euro area(?:\s*(?:,|-)\s*(?:March|June|September|December)\s+\d{4})?$"
)
_TARGET_CONCEPTS: Final = ("hicp-inflation", "real-gdp-growth")
_EARLY_ROUND_RELEASES: Final = {("2006-08-31", "2006-09")}


def _texts(values: Sequence[str], name: str) -> tuple[str, ...]:
    result = tuple(_required_text(item, name) for item in values)
    if not result:
        raise ValueError(f"{name} requires at least one item")
    return result


def _canonical_uri(value: str) -> str:
    parsed = urlsplit(_https_uri(value, "source_uri"))
    return str(parsed._replace(query="", fragment="").geturl())


def _projection_round(uri: str) -> str:
    match = _ROUND_RE.search(uri)
    if match is None:
        raise ValueError("ECB projection URI omits its projection round")
    token = match.group("round")
    month = int(token[4:6])
    if month not in {3, 6, 9, 12}:
        raise ValueError("ECB projection URI has a non-quarterly round")
    return f"{token[:4]}-{token[4:6]}"


def _validate_round_release_date(
    release_date: str, projection_round: str
) -> None:
    if (
        release_date[:7] != projection_round
        and (
            release_date,
            projection_round,
        )
        not in _EARLY_ROUND_RELEASES
    ):
        raise ValueError("ECB projection release and round differ")


class EcbProjectionProducer(str, Enum):
    """Staff body responsible for one projection round."""

    ECB_STAFF = "ecb-staff"
    EUROSYSTEM_STAFF = "eurosystem-staff"

    @classmethod
    def from_value(
        cls, value: str | EcbProjectionProducer
    ) -> EcbProjectionProducer:
        try:
            return cls(value)
        except ValueError as exc:
            raise ValueError("unsupported ECB projection producer") from exc


def _producer(title: str, projection_round: str) -> EcbProjectionProducer:
    match = _TITLE_RE.fullmatch(_required_text(title, "source_title"))
    if match is None:
        raise ValueError("ECB projection title is outside the qualified series")
    producer = (
        EcbProjectionProducer.ECB_STAFF
        if match.group("producer") == "ECB"
        else EcbProjectionProducer.EUROSYSTEM_STAFF
    )
    month = int(projection_round[-2:])
    expected = (
        EcbProjectionProducer.ECB_STAFF
        if month in {3, 9}
        else EcbProjectionProducer.EUROSYSTEM_STAFF
    )
    if producer is not expected:
        raise ValueError("ECB projection title and quarterly round differ")
    return producer


@dataclass(frozen=True, slots=True)
class EcbProjectionArchiveEntryV1:
    """One main release listed on the official projection archive page."""

    release_date: str
    source_uri: str
    source_title: str
    projection_round: str
    producer: EcbProjectionProducer
    entry_id: str = ""
    schema_version: str = ECB_PROJECTION_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PROJECTION_ARCHIVE_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported ECB projection archive-entry schema")
        release = _iso_date(self.release_date, "release_date")
        uri = _canonical_uri(self.source_uri)
        projection_round = _required_text(
            self.projection_round, "projection_round"
        )
        if _projection_round(uri) != projection_round:
            raise ValueError("ECB archive URI and projection round differ")
        _validate_round_release_date(release, projection_round)
        title = _required_text(self.source_title, "source_title")
        producer = EcbProjectionProducer.from_value(self.producer)
        if _producer(title, projection_round) is not producer:
            raise ValueError("ECB archive producer differs from its title")
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "projection_round", projection_round)
        object.__setattr__(self, "producer", producer)
        expected = _stable_id(
            "ecb-projection-archive-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError("ECB projection archive-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_date": self.release_date,
            "source_uri": self.source_uri,
            "source_title": self.source_title,
            "projection_round": self.projection_round,
            "producer": self.producer.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbProjectionArchiveEntryV1:
        return cls(
            release_date=str(data.get("release_date", "")),
            source_uri=str(data.get("source_uri", "")),
            source_title=str(data.get("source_title", "")),
            projection_round=str(data.get("projection_round", "")),
            producer=EcbProjectionProducer.from_value(
                str(data.get("producer", ""))
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbProjectionArchiveIndexV1:
    """Content-bound official all-releases projection inventory."""

    as_of_date: str
    artifact: EcbArchiveArtifactV1
    entries: tuple[EcbProjectionArchiveEntryV1, ...]
    index_id: str = ""
    schema_version: str = ECB_PROJECTION_ARCHIVE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PROJECTION_ARCHIVE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported ECB projection archive-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if (
            self.artifact.role is not EcbArtifactRole.PROJECTION_INDEX_HTML
            or self.artifact.source_uri != ECB_PROJECTION_ARCHIVE_URI
        ):
            raise ValueError("ECB projection archive-index artifact differs")
        entries = tuple(self.entries)
        if not 1 <= len(entries) <= MAX_ECB_PROJECTIONS:
            raise ValueError("ECB projection archive-index count is invalid")
        dates = [item.release_date for item in entries]
        rounds = [item.projection_round for item in entries]
        if dates != sorted(dates) or len(set(dates)) != len(dates):
            raise ValueError(
                "ECB projection archive dates are not unique and ordered"
            )
        if len(set(rounds)) != len(rounds):
            raise ValueError("ECB projection archive rounds are not unique")
        if dates[0] != ECB_PROJECTION_ARCHIVE_START_DATE:
            raise ValueError("ECB projection archive has an unexpected start")
        if dates[-1] > as_of:
            raise ValueError(
                "ECB projection archive exceeds its as-of boundary"
            )
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "ecb-projection-archive-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected:
            raise ValueError("ECB projection archive-index identity differs")
        object.__setattr__(self, "index_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "artifact": self.artifact.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbProjectionArchiveIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifact=EcbArchiveArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            entries=tuple(
                EcbProjectionArchiveEntryV1.from_dict(
                    _mapping(item, "archive entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbFoedbProjectionPublicationV1:
    """One exactly selected FOEDB type-95 projection publication."""

    record_id: int
    database_timestamp: int
    release_date: str
    source_title: str
    document_uris: tuple[str, ...]
    projection_round: str
    producer: EcbProjectionProducer
    time_precision: EconomicTimePrecision
    published_at_ns: int | None
    published_lexical: str | None
    publication_id: str = ""
    schema_version: str = ECB_PROJECTION_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PROJECTION_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported ECB projection-entry schema")
        _bounded_int(self.record_id, "record_id", 2**63 - 1)
        _bounded_int(self.database_timestamp, "database_timestamp", 2**63 - 1)
        release = _iso_date(self.release_date, "release_date")
        title = _required_text(self.source_title, "source_title")
        projection_round = _required_text(
            self.projection_round, "projection_round"
        )
        uris = tuple(_canonical_uri(item) for item in self.document_uris)
        if not 1 <= len(uris) <= 2 or len(set(uris)) != len(uris):
            raise ValueError(
                "ECB projection requires one or two unique documents"
            )
        formats = tuple(Path(urlsplit(item).path).suffix for item in uris)
        if formats.count(".pdf") != 1 or formats.count(".html") not in {0, 1}:
            raise ValueError("ECB projection requires a PDF and optional HTML")
        if any(_projection_round(item) != projection_round for item in uris):
            raise ValueError("ECB projection documents name different rounds")
        _validate_round_release_date(release, projection_round)
        producer = EcbProjectionProducer.from_value(self.producer)
        if _producer(title, projection_round) is not producer:
            raise ValueError("ECB projection producer differs from its title")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        lexical = _optional_text(self.published_lexical)
        if precision is EconomicTimePrecision.DATE_ONLY:
            if self.published_at_ns is not None or lexical is not None:
                raise ValueError(
                    "date-only ECB projection invents an exact clock"
                )
        elif precision is EconomicTimePrecision.EXACT_MINUTE:
            if self.published_at_ns is None or lexical is None:
                raise ValueError("exact ECB projection omits its source clock")
            _bounded_int(self.published_at_ns, "published_at_ns", 2**63 - 1)
            parsed = datetime.fromisoformat(lexical)
            if parsed.tzinfo is None or parsed.date().isoformat() != release:
                raise ValueError("ECB projection publication clock is invalid")
            if self.published_at_ns != self.database_timestamp * 1_000_000_000:
                raise ValueError("ECB projection epoch differs from FOEDB")
        else:
            raise ValueError("ECB projection time precision is unsupported")
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "document_uris", uris)
        object.__setattr__(self, "projection_round", projection_round)
        object.__setattr__(self, "producer", producer)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "published_lexical", lexical)
        expected = _stable_id(
            "ecb-projection-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("ECB projection publication identity differs")
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
            "source_title": self.source_title,
            "document_uris": list(self.document_uris),
            "projection_round": self.projection_round,
            "producer": self.producer.value,
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
    ) -> EcbFoedbProjectionPublicationV1:
        return cls(
            record_id=cast(int, data.get("record_id")),
            database_timestamp=cast(int, data.get("database_timestamp")),
            release_date=str(data.get("release_date", "")),
            source_title=str(data.get("source_title", "")),
            document_uris=tuple(
                str(item)
                for item in _sequence(
                    data.get("document_uris"), "document_uris"
                )
            ),
            projection_round=str(data.get("projection_round", "")),
            producer=EcbProjectionProducer.from_value(
                str(data.get("producer", ""))
            ),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            published_at_ns=cast(int | None, data.get("published_at_ns")),
            published_lexical=_optional_text(data.get("published_lexical")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbFoedbProjectionReleaseIndexV1:
    """Hash-bound FOEDB inventory reconciled to the official archive page."""

    as_of_date: str
    archive_index_id: str
    database_version: str
    database_version_hash: str
    database_total_records: int
    database_chunk_size: int
    database_chunk_group_size: int
    database_artifacts: tuple[EcbArchiveArtifactV1, ...]
    publications: tuple[EcbFoedbProjectionPublicationV1, ...]
    index_id: str = ""
    schema_version: str = ECB_PROJECTION_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PROJECTION_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported ECB projection-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        archive_id = _required_text(self.archive_index_id, "archive_index_id")
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
            raise ValueError(
                "ECB projection-index chunk sizes must be positive"
            )
        artifacts = tuple(self.database_artifacts)
        roles = [item.role for item in artifacts]
        if roles.count(EcbArtifactRole.FOEDB_VERSIONS) != 1:
            raise ValueError(
                "ECB projection index requires one version artifact"
            )
        if roles.count(EcbArtifactRole.FOEDB_METADATA) != 1:
            raise ValueError(
                "ECB projection index requires one metadata artifact"
            )
        if roles.count(EcbArtifactRole.FOEDB_CHUNK) != math.ceil(
            total / chunk_size
        ):
            raise ValueError(
                "ECB projection index has an incomplete chunk inventory"
            )
        publications = tuple(self.publications)
        if not 1 <= len(publications) <= MAX_ECB_PROJECTIONS:
            raise ValueError("ECB projection publication count is invalid")
        dates = [item.release_date for item in publications]
        rounds = [item.projection_round for item in publications]
        if dates != sorted(dates) or len(set(dates)) != len(dates):
            raise ValueError("ECB projection dates are not unique and ordered")
        if len(set(rounds)) != len(rounds):
            raise ValueError("ECB projection rounds are not unique")
        if dates[0] != ECB_PROJECTION_ARCHIVE_START_DATE or dates[-1] > as_of:
            raise ValueError(
                "ECB projection index has an invalid date boundary"
            )
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "archive_index_id", archive_id)
        object.__setattr__(self, "database_version", version)
        object.__setattr__(self, "database_version_hash", version_hash)
        object.__setattr__(self, "database_artifacts", artifacts)
        object.__setattr__(self, "publications", publications)
        expected = _stable_id("ecb-projection-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("ECB projection-index identity differs")
        object.__setattr__(self, "index_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "archive_index_id": self.archive_index_id,
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
    ) -> EcbFoedbProjectionReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            archive_index_id=str(data.get("archive_index_id", "")),
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
                EcbFoedbProjectionPublicationV1.from_dict(
                    _mapping(item, "publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbStaffProjectionReleaseV1:
    """One period-projection round with explicit scope and horizon."""

    publication_id: str
    release_date: str
    projection_round: str
    producer: EcbProjectionProducer
    economy_code: str
    forecast_scope: EconomicForecastScope
    target_concepts: tuple[str, ...]
    horizon_start_year: int
    horizon_end_year: int
    horizon_source_uri: str
    horizon_source_format: OfficialSourceFormat
    artifacts: tuple[EcbArchiveArtifactV1, ...]
    limitations: tuple[str, ...]
    projection_id: str = ""
    schema_version: str = ECB_PROJECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PROJECTION_SCHEMA_VERSION:
            raise ValueError("unsupported ECB staff-projection schema")
        publication_id = _required_text(self.publication_id, "publication_id")
        release = _iso_date(self.release_date, "release_date")
        projection_round = _required_text(
            self.projection_round, "projection_round"
        )
        producer = EcbProjectionProducer.from_value(self.producer)
        if self.economy_code != "EA":
            raise ValueError("ECB staff projection must retain euro-area scope")
        scope = EconomicForecastScope.from_value(self.forecast_scope)
        if scope is not EconomicForecastScope.CENTRAL_BANK_PROJECTION:
            raise ValueError(
                "ECB staff projection must retain projection scope"
            )
        concepts = tuple(self.target_concepts)
        if concepts != _TARGET_CONCEPTS:
            raise ValueError("ECB staff projection core target concepts differ")
        start = _bounded_int(
            self.horizon_start_year, "horizon_start_year", 9999
        )
        end = _bounded_int(self.horizon_end_year, "horizon_end_year", 9999)
        if start != int(release[:4]) or not 1 <= end - start <= 3:
            raise ValueError("ECB staff projection annual horizon is invalid")
        _validate_round_release_date(release, projection_round)
        artifacts = tuple(self.artifacts)
        if not 1 <= len(artifacts) <= 2:
            raise ValueError("ECB staff projection artifact count is invalid")
        roles = [item.role for item in artifacts]
        if roles.count(EcbArtifactRole.PROJECTION_PDF) != 1 or roles.count(
            EcbArtifactRole.PROJECTION_HTML
        ) not in {0, 1}:
            raise ValueError("ECB staff projection artifacts differ")
        horizon_uri = _canonical_uri(self.horizon_source_uri)
        horizon_format = OfficialSourceFormat.from_value(
            self.horizon_source_format
        )
        matched = [item for item in artifacts if item.source_uri == horizon_uri]
        if len(matched) != 1 or matched[0].source_format is not horizon_format:
            raise ValueError("ECB projection horizon evidence is not retained")
        limitations = _texts(self.limitations, "limitations")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "projection_round", projection_round)
        object.__setattr__(self, "producer", producer)
        object.__setattr__(self, "forecast_scope", scope)
        object.__setattr__(self, "target_concepts", concepts)
        object.__setattr__(self, "horizon_source_uri", horizon_uri)
        object.__setattr__(self, "horizon_source_format", horizon_format)
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("ecb-staff-projection", self.identity_payload())
        if self.projection_id and self.projection_id != expected:
            raise ValueError("ECB staff-projection identity differs")
        object.__setattr__(self, "projection_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "release_date": self.release_date,
            "projection_round": self.projection_round,
            "producer": self.producer.value,
            "economy_code": self.economy_code,
            "forecast_scope": self.forecast_scope.value,
            "target_concepts": list(self.target_concepts),
            "horizon_start_year": self.horizon_start_year,
            "horizon_end_year": self.horizon_end_year,
            "horizon_source_uri": self.horizon_source_uri,
            "horizon_source_format": self.horizon_source_format.value,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "projection_id": self.projection_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbStaffProjectionReleaseV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            release_date=str(data.get("release_date", "")),
            projection_round=str(data.get("projection_round", "")),
            producer=EcbProjectionProducer.from_value(
                str(data.get("producer", ""))
            ),
            economy_code=str(data.get("economy_code", "")),
            forecast_scope=EconomicForecastScope.from_value(
                str(data.get("forecast_scope", ""))
            ),
            target_concepts=tuple(
                str(item)
                for item in _sequence(
                    data.get("target_concepts"), "target_concepts"
                )
            ),
            horizon_start_year=cast(int, data.get("horizon_start_year")),
            horizon_end_year=cast(int, data.get("horizon_end_year")),
            horizon_source_uri=str(data.get("horizon_source_uri", "")),
            horizon_source_format=OfficialSourceFormat.from_value(
                str(data.get("horizon_source_format", ""))
            ),
            artifacts=tuple(
                EcbArchiveArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            projection_id=str(data.get("projection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbStaffProjectionsArchiveManifestV1:
    """Compact receipt for the complete qualified staff-projection archive."""

    registry_id: str
    source_id: str
    archive_index: EcbProjectionArchiveIndexV1
    release_index: EcbFoedbProjectionReleaseIndexV1
    projections: tuple[EcbStaffProjectionReleaseV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    projection_count: int
    ecb_staff_count: int
    eurosystem_staff_count: int
    pdf_artifact_count: int
    html_artifact_count: int
    exact_minute_count: int
    date_only_count: int
    manifest_id: str = ""
    schema_version: str = ECB_PROJECTION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_PROJECTION_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported ECB projection-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if self.release_index.archive_index_id != self.archive_index.index_id:
            raise ValueError("ECB projection archive and FOEDB indexes differ")
        projections = tuple(self.projections)
        publications = self.release_index.publications
        if tuple(item.publication_id for item in projections) != tuple(
            item.publication_id for item in publications
        ):
            raise ValueError(
                "ECB projection manifest publication lineage differs"
            )
        artifacts = (
            *self.release_index.database_artifacts,
            self.archive_index.artifact,
            *(artifact for item in projections for artifact in item.artifacts),
        )
        if len(artifacts) > MAX_ECB_PROJECTION_ARTIFACTS:
            raise ValueError("ECB projection artifact count exceeds bound")
        expected = (
            len(artifacts),
            len({item.content_sha256 for item in artifacts}),
            sum(item.content_length for item in artifacts),
            len(projections),
            sum(
                item.producer is EcbProjectionProducer.ECB_STAFF
                for item in projections
            ),
            sum(
                item.producer is EcbProjectionProducer.EUROSYSTEM_STAFF
                for item in projections
            ),
            sum(
                artifact.role is EcbArtifactRole.PROJECTION_PDF
                for projection in projections
                for artifact in projection.artifacts
            ),
            sum(
                artifact.role is EcbArtifactRole.PROJECTION_HTML
                for projection in projections
                for artifact in projection.artifacts
            ),
            sum(item.exact_minute for item in publications),
            sum(not item.exact_minute for item in publications),
        )
        actual = (
            self.raw_artifact_count,
            self.unique_content_sha256_count,
            self.total_content_bytes,
            self.projection_count,
            self.ecb_staff_count,
            self.eurosystem_staff_count,
            self.pdf_artifact_count,
            self.html_artifact_count,
            self.exact_minute_count,
            self.date_only_count,
        )
        if actual != expected:
            raise ValueError("ECB projection manifest summary counts differ")
        if self.total_content_bytes > MAX_ECB_PROJECTION_TOTAL_BYTES:
            raise ValueError("ECB projection corpus exceeds byte bound")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "projections", projections)
        identity = _stable_id(
            "ecb-projection-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != identity:
            raise ValueError("ECB projection-manifest identity differs")
        object.__setattr__(self, "manifest_id", identity)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "archive_index": self.archive_index.to_dict(),
            "release_index": self.release_index.to_dict(),
            "projections": [item.to_dict() for item in self.projections],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "projection_count": self.projection_count,
            "ecb_staff_count": self.ecb_staff_count,
            "eurosystem_staff_count": self.eurosystem_staff_count,
            "pdf_artifact_count": self.pdf_artifact_count,
            "html_artifact_count": self.html_artifact_count,
            "exact_minute_count": self.exact_minute_count,
            "date_only_count": self.date_only_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbStaffProjectionsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            archive_index=EcbProjectionArchiveIndexV1.from_dict(
                _mapping(data.get("archive_index"), "archive_index")
            ),
            release_index=EcbFoedbProjectionReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            projections=tuple(
                EcbStaffProjectionReleaseV1.from_dict(
                    _mapping(item, "projection")
                )
                for item in _sequence(data.get("projections"), "projections")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            projection_count=cast(int, data.get("projection_count")),
            ecb_staff_count=cast(int, data.get("ecb_staff_count")),
            eurosystem_staff_count=cast(
                int, data.get("eurosystem_staff_count")
            ),
            pdf_artifact_count=cast(int, data.get("pdf_artifact_count")),
            html_artifact_count=cast(int, data.get("html_artifact_count")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            date_only_count=cast(int, data.get("date_only_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(
        cls, raw: str | bytes
    ) -> EcbStaffProjectionsArchiveManifestV1:
        try:
            value = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB projection manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(value, "ECB projection manifest"))


class _ProjectionArchiveHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.current_date: str | None = None
        self.title_div_depth = 0
        self.anchor_href: str | None = None
        self.anchor_text: list[str] = []
        self.rows: list[tuple[str, str, str]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = dict(attrs)
        if tag == "dt" and values.get("isodate"):
            self.current_date = values["isodate"]
        if tag == "div":
            classes = set((values.get("class") or "").split())
            if self.title_div_depth:
                self.title_div_depth += 1
            elif "title" in classes:
                self.title_div_depth = 1
        if tag == "a" and self.title_div_depth and self.anchor_href is None:
            self.anchor_href = values.get("href")
            self.anchor_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.title_div_depth and self.anchor_href is not None:
            title = re.sub(r"\s+", " ", " ".join(self.anchor_text)).strip()
            if self.current_date is not None and title:
                self.rows.append((self.current_date, self.anchor_href, title))
            self.anchor_href = None
            self.anchor_text = []
        if tag == "div" and self.title_div_depth:
            self.title_div_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.anchor_href is not None:
            self.anchor_text.append(data)


class _ProjectionDocumentHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.main_depth = 0
        self.h1_depth = 0
        self.text: list[str] = []
        self.current_heading: list[str] = []
        self.headings: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag == "main":
            self.main_depth += 1
        elif self.main_depth and tag == "h1":
            self.h1_depth += 1
            self.current_heading = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "h1" and self.h1_depth:
            heading = re.sub(
                r"\s+", " ", " ".join(self.current_heading)
            ).strip()
            if heading:
                self.headings.append(heading)
            self.current_heading = []
            self.h1_depth -= 1
        elif tag == "main" and self.main_depth:
            self.main_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.main_depth:
            self.text.append(data)
            if self.h1_depth:
                self.current_heading.append(data)


def build_ecb_projection_archive_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the request for the independent official projection index."""
    return _request(
        registry, ECB_PROJECTION_ARCHIVE_URI, OfficialSourceFormat.HTML
    )


def parse_ecb_projection_archive_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> EcbProjectionArchiveIndexV1:
    """Parse only the 90 main projection rounds from the mixed index page."""
    as_of = _iso_date(as_of_date, "as_of_date")
    artifact = _artifact(snapshot, EcbArtifactRole.PROJECTION_INDEX_HTML)
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB projection archive index is not UTF-8") from exc
    parser = _ProjectionArchiveHtmlParser()
    parser.feed(text)
    parser.close()
    entries: list[EcbProjectionArchiveEntryV1] = []
    for release_date, href, title in parser.rows:
        if _TITLE_RE.fullmatch(title) is None or release_date > as_of:
            continue
        uri = _canonical_uri(urljoin(ECB_PROJECTION_ARCHIVE_URI, href))
        projection_round = _projection_round(uri)
        entries.append(
            EcbProjectionArchiveEntryV1(
                release_date=release_date,
                source_uri=uri,
                source_title=title,
                projection_round=projection_round,
                producer=_producer(title, projection_round),
            )
        )
    return EcbProjectionArchiveIndexV1(
        as_of_date=as_of,
        artifact=artifact,
        entries=tuple(sorted(entries, key=lambda item: item.release_date)),
    )


def _projection_publication_time(
    database_timestamp: int,
) -> tuple[str, EconomicTimePrecision, int | None, str | None]:
    timestamp = datetime.fromtimestamp(database_timestamp, timezone.utc)
    local = timestamp.astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
    if local.second != 0:
        raise ValueError("ECB projection FOEDB timestamp has seconds")
    release_date = local.date().isoformat()
    clock = (local.hour, local.minute)
    if clock == (0, 0):
        if release_date > "2017-09-07":
            raise ValueError(
                "ECB projection placeholder clock is outside known era"
            )
        return release_date, EconomicTimePrecision.DATE_ONLY, None, None
    if clock not in {(15, 30), (15, 45)}:
        raise ValueError(
            "ECB projection publication clock is outside known eras"
        )
    if clock == (15, 30) and release_date >= "2022-09-08":
        raise ValueError("ECB projection 15:30 clock is outside known era")
    if clock == (15, 45) and release_date < "2022-09-08":
        raise ValueError("ECB projection 15:45 clock is outside known era")
    return (
        release_date,
        EconomicTimePrecision.EXACT_MINUTE,
        database_timestamp * 1_000_000_000,
        local.isoformat(timespec="minutes"),
    )


def build_ecb_foedb_projection_release_index(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    archive_index: EcbProjectionArchiveIndexV1,
    *,
    as_of_date: str,
) -> EcbFoedbProjectionReleaseIndexV1:
    """Select type-95 releases and require exact archive-page reconciliation."""
    as_of = _iso_date(as_of_date, "as_of_date")
    if as_of != archive_index.as_of_date:
        raise ValueError("ECB projection index as-of boundaries differ")
    database = _decode_ecb_foedb_database(
        versions_snapshot, metadata_snapshot, chunk_snapshots
    )
    publications: list[EcbFoedbProjectionPublicationV1] = []
    for record in database.records:
        if record.get("type") != 95:
            continue
        timestamp = _bounded_int(
            record.get("pub_timestamp"), "publication timestamp", 2**63 - 1
        )
        release_date, precision, published_at_ns, lexical = (
            _projection_publication_time(timestamp)
        )
        if (
            release_date < ECB_PROJECTION_ARCHIVE_START_DATE
            or release_date > as_of
        ):
            continue
        properties = _mapping(
            record.get("publicationProperties") or {}, "publicationProperties"
        )
        title = _required_text(properties.get("Title"), "source_title")
        paths = _sequence(record.get("documentTypes"), "documentTypes")
        uris = tuple(
            _canonical_uri(urljoin("https://www.ecb.europa.eu", str(item)))
            for item in paths
        )
        projection_round = _projection_round(uris[0])
        publications.append(
            EcbFoedbProjectionPublicationV1(
                record_id=_bounded_int(
                    record.get("id"), "record id", 2**63 - 1
                ),
                database_timestamp=timestamp,
                release_date=release_date,
                source_title=title,
                document_uris=uris,
                projection_round=projection_round,
                producer=_producer(title, projection_round),
                time_precision=precision,
                published_at_ns=published_at_ns,
                published_lexical=lexical,
            )
        )
    publications.sort(key=lambda item: item.release_date)
    by_round = {item.projection_round: item for item in publications}
    if len(by_round) != len(publications) or len(publications) != len(
        archive_index.entries
    ):
        raise ValueError("ECB FOEDB and archive projection counts differ")
    for entry in archive_index.entries:
        publication = by_round.get(entry.projection_round)
        if publication is None:
            raise ValueError("ECB archive projection is absent from FOEDB")
        if (
            publication.release_date != entry.release_date
            or publication.source_title != entry.source_title
            or publication.producer is not entry.producer
            or entry.source_uri not in publication.document_uris
        ):
            raise ValueError("ECB FOEDB and archive projection metadata differ")
    return EcbFoedbProjectionReleaseIndexV1(
        as_of_date=as_of,
        archive_index_id=archive_index.index_id,
        database_version=database.version,
        database_version_hash=database.version_hash,
        database_total_records=database.total_records,
        database_chunk_size=database.chunk_size,
        database_chunk_group_size=database.chunk_group_size,
        database_artifacts=database.artifacts,
        publications=tuple(publications),
    )


def build_ecb_projection_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbProjectionReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build exact HTML/PDF requests for every selected projection round."""
    requests: list[OfficialSourceRequestV1] = []
    for publication in release_index.publications:
        for uri in publication.document_uris:
            source_format = (
                OfficialSourceFormat.PDF
                if urlsplit(uri).path.endswith(".pdf")
                else OfficialSourceFormat.HTML
            )
            requests.append(_request(registry, uri, source_format))
    if len({item.uri for item in requests}) != len(requests):
        raise ValueError("ECB projection request inventory has duplicate URIs")
    return tuple(requests)


def _html_projection_text(content: bytes, expected_title: str) -> str:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB projection HTML is not UTF-8") from exc
    parser = _ProjectionDocumentHtmlParser()
    parser.feed(text)
    parser.close()
    if expected_title not in parser.headings:
        raise ValueError(
            "ECB projection HTML heading differs: "
            f"expected {expected_title!r}, observed {parser.headings!r}"
        )
    result = re.sub(r"\s+", " ", " ".join(parser.text)).strip()
    if not result:
        raise ValueError("ECB projection HTML has no main text")
    return result


def _pdf_projection_text(content: bytes) -> str:
    if not content.startswith(b"%PDF-"):
        raise ValueError("ECB projection PDF lacks a PDF signature")
    try:
        reader = PdfReader(BytesIO(content))
        if not 1 <= len(reader.pages) <= MAX_ECB_PROJECTION_PDF_PAGES:
            raise ValueError(
                "ECB projection PDF page count is outside its bound"
            )
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ValueError("ECB projection PDF cannot be parsed") from exc
    if not text.strip():
        raise ValueError("ECB projection PDF has no extractable text")
    return re.sub(r"\s+", " ", text).strip()


def _projection_horizon(
    publication: EcbFoedbProjectionPublicationV1,
    snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> tuple[int, int, str, OfficialSourceFormat]:
    html_uri = next(
        (item for item in publication.document_uris if item.endswith(".html")),
        None,
    )
    if html_uri is not None:
        text = _html_projection_text(
            snapshots[html_uri].content, publication.source_title
        )
        evidence_uri = html_uri
        evidence_format = OfficialSourceFormat.HTML
        bounded = text[:15_000]
    else:
        pdf_uri = next(
            item for item in publication.document_uris if item.endswith(".pdf")
        )
        text = _pdf_projection_text(snapshots[pdf_uri].content)
        evidence_uri = pdf_uri
        evidence_format = OfficialSourceFormat.PDF
        bounded = text[:7_000]
    lowered = text.casefold()
    if (
        "macroeconomic projections" not in lowered
        or "euro area" not in lowered
        or "real gdp" not in lowered
        or "hicp" not in lowered
    ):
        raise ValueError(
            "ECB projection omits its qualified scope markers: "
            f"{publication.projection_round}"
        )
    start = int(publication.release_date[:4])
    years = {
        int(item)
        for item in re.findall(r"\b(?:19|20)\d{2}\b", bounded)
        if start <= int(item) <= start + 3
    }
    if not years or max(years) <= start:
        raise ValueError(
            "ECB projection annual horizon is unavailable: "
            f"{publication.projection_round}"
        )
    return start, max(years), evidence_uri, evidence_format


def build_ecb_staff_projections_archive_manifest(
    registry: OfficialSourceRegistryV1,
    archive_index: EcbProjectionArchiveIndexV1,
    release_index: EcbFoedbProjectionReleaseIndexV1,
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> EcbStaffProjectionsArchiveManifestV1:
    """Parse, scope, and hash every qualified ECB projection round."""
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    expected_uris = {
        uri for item in release_index.publications for uri in item.document_uris
    }
    if set(report_snapshots) != expected_uris:
        raise ValueError("ECB retained projection inventory differs")
    projections: list[EcbStaffProjectionReleaseV1] = []
    for publication in release_index.publications:
        snapshots = {
            uri: report_snapshots[uri] for uri in publication.document_uris
        }
        artifacts: list[EcbArchiveArtifactV1] = []
        for uri in publication.document_uris:
            snapshot = snapshots[uri]
            role = (
                EcbArtifactRole.PROJECTION_PDF
                if uri.endswith(".pdf")
                else EcbArtifactRole.PROJECTION_HTML
            )
            if role is EcbArtifactRole.PROJECTION_PDF:
                _pdf_projection_text(snapshot.content)
            artifacts.append(_artifact(snapshot, role))
        start, end, evidence_uri, evidence_format = _projection_horizon(
            publication, snapshots
        )
        projections.append(
            EcbStaffProjectionReleaseV1(
                publication_id=publication.publication_id,
                release_date=publication.release_date,
                projection_round=publication.projection_round,
                producer=publication.producer,
                economy_code="EA",
                forecast_scope=EconomicForecastScope.CENTRAL_BANK_PROJECTION,
                target_concepts=_TARGET_CONCEPTS,
                horizon_start_year=start,
                horizon_end_year=end,
                horizon_source_uri=evidence_uri,
                horizon_source_format=evidence_format,
                artifacts=tuple(artifacts),
                limitations=(
                    "The round is an annual euro-area central-bank period projection, not a monthly or quarterly event-consensus estimate.",
                    "The receipt qualifies release identity, core GDP/HICP scope, and annual horizon; it does not transcribe every projected value from the retained report.",
                ),
            )
        )
    all_artifacts = (
        *release_index.database_artifacts,
        archive_index.artifact,
        *(artifact for item in projections for artifact in item.artifacts),
    )
    return EcbStaffProjectionsArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        archive_index=archive_index,
        release_index=release_index,
        projections=tuple(projections),
        raw_artifact_count=len(all_artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in all_artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in all_artifacts),
        projection_count=len(projections),
        ecb_staff_count=sum(
            item.producer is EcbProjectionProducer.ECB_STAFF
            for item in projections
        ),
        eurosystem_staff_count=sum(
            item.producer is EcbProjectionProducer.EUROSYSTEM_STAFF
            for item in projections
        ),
        pdf_artifact_count=sum(
            artifact.role is EcbArtifactRole.PROJECTION_PDF
            for projection in projections
            for artifact in projection.artifacts
        ),
        html_artifact_count=sum(
            artifact.role is EcbArtifactRole.PROJECTION_HTML
            for projection in projections
            for artifact in projection.artifacts
        ),
        exact_minute_count=sum(
            item.exact_minute for item in release_index.publications
        ),
        date_only_count=sum(
            not item.exact_minute for item in release_index.publications
        ),
    )


def replay_ecb_staff_projections_archive(
    registry: OfficialSourceRegistryV1,
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    archive_snapshot: OfficialRawSnapshotV1,
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    expected: EcbStaffProjectionsArchiveManifestV1,
) -> EcbStaffProjectionsArchiveManifestV1:
    """Rebuild the retained projection corpus and require exact equality."""
    archive_index = parse_ecb_projection_archive_index(
        archive_snapshot, as_of_date=expected.release_index.as_of_date
    )
    release_index = build_ecb_foedb_projection_release_index(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        archive_index,
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_ecb_staff_projections_archive_manifest(
        registry, archive_index, release_index, report_snapshots
    )
    if rebuilt != expected:
        raise ValueError("ECB projection retained-corpus replay differs")
    return rebuilt


def packaged_ecb_staff_projections_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / (
        "ecb_staff_projections_archive_v1.json"
    )


def load_packaged_ecb_staff_projections_archive_manifest() -> (
    EcbStaffProjectionsArchiveManifestV1
):
    """Load and validate the packaged compact ECB projection manifest."""
    return EcbStaffProjectionsArchiveManifestV1.from_json(
        packaged_ecb_staff_projections_manifest_path().read_text(
            encoding="utf-8"
        )
    )


__all__ = [
    "ECB_LATEST_PACKAGED_PROJECTION_DATE",
    "ECB_PROJECTION_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "ECB_PROJECTION_ARCHIVE_INDEX_SCHEMA_VERSION",
    "ECB_PROJECTION_ARCHIVE_START_DATE",
    "ECB_PROJECTION_ARCHIVE_URI",
    "ECB_PROJECTION_ENTRY_SCHEMA_VERSION",
    "ECB_PROJECTION_INDEX_SCHEMA_VERSION",
    "ECB_PROJECTION_MANIFEST_SCHEMA_VERSION",
    "ECB_PROJECTION_SCHEMA_VERSION",
    "ECB_STAFF_PROJECTIONS_PROGRAM_KEY",
    "EcbFoedbProjectionPublicationV1",
    "EcbFoedbProjectionReleaseIndexV1",
    "EcbProjectionArchiveEntryV1",
    "EcbProjectionArchiveIndexV1",
    "EcbProjectionProducer",
    "EcbStaffProjectionReleaseV1",
    "EcbStaffProjectionsArchiveManifestV1",
    "build_ecb_foedb_projection_release_index",
    "build_ecb_projection_archive_request",
    "build_ecb_projection_requests",
    "build_ecb_staff_projections_archive_manifest",
    "load_packaged_ecb_staff_projections_archive_manifest",
    "packaged_ecb_staff_projections_manifest_path",
    "parse_ecb_projection_archive_index",
    "replay_ecb_staff_projections_archive",
]

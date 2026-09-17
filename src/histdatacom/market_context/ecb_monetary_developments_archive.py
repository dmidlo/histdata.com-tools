"""Deterministic qualification of ECB monthly monetary developments.

The ECB public FOEDB database supplies the complete publication inventory.
This module selects type 10, retains the immediate pre-2000 predecessor and
every in-window document, and parses the source-authored annual M3 growth
rate together with the previous-period value as known at each release.  The
lineage therefore distinguishes a prior value's first publication from its
subsequent revision without treating a later ECB data endpoint as vintage
evidence.
"""

from __future__ import annotations

import calendar
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
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
from histdatacom.market_context.economic_calendar import EconomicTimePrecision
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

ECB_MONETARY_DEVELOPMENTS_PROGRAM_KEY: Final = "ea.ecb.monetary-developments"
ECB_MONETARY_DEVELOPMENTS_MEASURE_KEY: Final = "m3-annual-growth-rate"
ECB_MONETARY_DEVELOPMENTS_ARCHIVE_START_DATE: Final = "2000-01-28"
ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_DATE: Final = "1999-12-28"
ECB_MONETARY_DEVELOPMENTS_FIRST_EXACT_DATE: Final = "2017-08-28"
ECB_LATEST_PACKAGED_MONETARY_DEVELOPMENTS_DATE: Final = "2026-08-27"

ECB_MONETARY_DEVELOPMENTS_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-developments-entry.v1"
)
ECB_MONETARY_DEVELOPMENTS_INDEX_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-developments-index.v1"
)
ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-developments-predecessor.v1"
)
ECB_MONETARY_DEVELOPMENTS_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-developments-release.v1"
)
ECB_MONETARY_DEVELOPMENTS_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-monetary-developments-archive-manifest.v1"
)

MAX_ECB_MONETARY_DEVELOPMENTS: Final = 512
MAX_ECB_MONETARY_DEVELOPMENTS_ARTIFACTS: Final = 768
MAX_ECB_MONETARY_DEVELOPMENTS_TOTAL_BYTES: Final = 512_000_000
MAX_ECB_MONETARY_DEVELOPMENTS_PDF_PAGES: Final = 64
MAX_ECB_MONETARY_DEVELOPMENTS_EXCERPT_CHARS: Final = 1_024

_FIRST_FOEDB_REFERENCE_PERIOD: Final = "1998-11"
_EXPECTED_PREDECESSOR_REFERENCE_PERIOD: Final = "1999-11"
_EXPECTED_FIRST_REFERENCE_PERIOD: Final = "1999-12"
_GENERIC_TITLE: Final = "Monetary developments in the euro area"
_MONTHS: Final = (
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
)
_DOCUMENT_PERIOD_RE = re.compile(
    r"(?:ecb\.)?m[db](?P<period>\d{4})(?:[~.]|$)", re.IGNORECASE
)
_M3_HEADLINE_RE = re.compile(
    r"annual\s+(?:rate\s+of\s+(?:increase|growth|change)|growth\s+rate)"
    r"\s+(?:in|of)\s+(?:the\s+)?(?:broad\s+monetary\s+aggregate\s+)?"
    r"M3\b",
    re.IGNORECASE,
)
_PERCENT_RE = re.compile(r"(?<![\w.])(?P<number>-?\d+(?:\.\d+)?)\s*%")
_PERIOD_RE = re.compile(r"\d{4}-\d{2}")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_FOEDB_RELEASE_DATE_OVERRIDES: Final[Mapping[str, str]] = {
    "md0112.pdf": "2002-01-28",
    "md0210.pdf": "2002-11-28",
}


def _texts(values: Sequence[str], name: str) -> tuple[str, ...]:
    result = tuple(_required_text(item, name) for item in values)
    if not result:
        raise ValueError(f"{name} requires at least one item")
    return result


def _month(value: object, name: str) -> str:
    result: str = _required_text(value, name)
    if _PERIOD_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    parsed = date.fromisoformat(f"{result}-01")
    if parsed.strftime("%Y-%m") != result:
        raise ValueError(f"{name} is not a canonical month")
    return result


def _shift_month(value: str, offset: int) -> str:
    parsed = date.fromisoformat(f"{_month(value, 'reference_period')}-01")
    ordinal = parsed.year * 12 + parsed.month - 1 + offset
    return f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"


def _previous_month(value: str) -> str:
    return _shift_month(value, -1)


def _month_label(value: str) -> str:
    parsed = date.fromisoformat(f"{_month(value, 'reference_period')}-01")
    return f"{_MONTHS[parsed.month - 1]} {parsed.year}"


def _canonical_uri(value: str) -> str:
    parsed = urlsplit(_https_uri(value, "source_uri"))
    return str(parsed._replace(query="", fragment="").geturl())


def _document_reference_period(uri: str) -> str | None:
    name = Path(urlsplit(uri).path).name
    match = _DOCUMENT_PERIOD_RE.search(name)
    if match is None:
        if name == "m3release.pdf":
            return None
        raise ValueError("ECB monetary-developments URI omits its period")
    token = match.group("period")
    short_year = int(token[:2])
    year = 1900 + short_year if short_year >= 90 else 2000 + short_year
    return _month(f"{year:04d}-{token[2:]}", "document reference period")


def _source_title(value: object, reference_period: str) -> str:
    title: str = _required_text(value, "source_title")
    label = _month_label(reference_period)
    if title not in {
        _GENERIC_TITLE,
        f"{_GENERIC_TITLE}: {label}",
        f"{_GENERIC_TITLE} ({label})",
    }:
        raise ValueError(
            "ECB monetary-developments title is outside known eras"
        )
    return title


def _number(value: str, name: str) -> tuple[str, float]:
    lexical = _required_text(value, name)
    if _NUMBER_RE.fullmatch(lexical) is None:
        raise ValueError(f"{name} is not a source numeric lexical")
    numeric = float(lexical)
    if not math.isfinite(numeric) or not -100.0 <= numeric <= 100.0:
        raise ValueError(f"{name} is outside its bound")
    return lexical, numeric


def _publication_time(
    release_date: str,
    database_timestamp: int,
    *,
    release_date_corrected_from_document: bool,
) -> tuple[EconomicTimePrecision, int | None, str | None]:
    timestamp = datetime.fromtimestamp(database_timestamp, timezone.utc)
    local = timestamp.astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
    if local.second != 0:
        raise ValueError(
            "ECB monetary-developments FOEDB timestamp differs from release"
        )
    if release_date_corrected_from_document:
        if release_date not in _FOEDB_RELEASE_DATE_OVERRIDES.values() or (
            local.hour,
            local.minute,
        ) != (0, 0):
            raise ValueError("ECB money release-date correction differs")
        return EconomicTimePrecision.DATE_ONLY, None, None
    if local.date().isoformat() != release_date:
        raise ValueError(
            "ECB monetary-developments FOEDB timestamp differs from release"
        )
    clock = (local.hour, local.minute)
    if release_date < ECB_MONETARY_DEVELOPMENTS_FIRST_EXACT_DATE:
        if clock != (0, 0):
            raise ValueError(
                "legacy ECB monetary developments invent an exact clock"
            )
        return EconomicTimePrecision.DATE_ONLY, None, None
    if clock != (10, 0):
        raise ValueError("ECB monetary-developments exact clock differs")
    return (
        EconomicTimePrecision.EXACT_MINUTE,
        database_timestamp * 1_000_000_000,
        local.isoformat(timespec="minutes"),
    )


@dataclass(frozen=True, slots=True)
class EcbFoedbMonetaryDevelopmentsPublicationV1:
    """One exactly selected FOEDB type-10 publication."""

    record_id: int
    database_timestamp: int
    release_date: str
    reference_period: str
    reference_period_from_uri: bool
    release_date_corrected_from_document: bool
    source_title: str
    document_uris: tuple[str, ...]
    time_precision: EconomicTimePrecision
    published_at_ns: int | None
    published_lexical: str | None
    publication_id: str = ""
    schema_version: str = ECB_MONETARY_DEVELOPMENTS_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_MONETARY_DEVELOPMENTS_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported ECB monetary-developments entry schema"
            )
        _bounded_int(self.record_id, "record_id", 2**63 - 1)
        _bounded_int(self.database_timestamp, "database_timestamp", 2**63 - 1)
        release = _iso_date(self.release_date, "release_date")
        reference = _month(self.reference_period, "reference_period")
        if not isinstance(self.reference_period_from_uri, bool):
            raise TypeError("reference_period_from_uri must be a boolean")
        if not isinstance(self.release_date_corrected_from_document, bool):
            raise TypeError(
                "release_date_corrected_from_document must be a boolean"
            )
        title = _source_title(self.source_title, reference)
        uris = tuple(_canonical_uri(item) for item in self.document_uris)
        if not 1 <= len(uris) <= 2 or len(set(uris)) != len(uris):
            raise ValueError(
                "ECB monetary developments require one or two documents"
            )
        formats = tuple(Path(urlsplit(item).path).suffix for item in uris)
        if any(item not in {".html", ".pdf"} for item in formats):
            raise ValueError("ECB monetary-developments format differs")
        if formats.count(".html") > 1 or formats.count(".pdf") > 1:
            raise ValueError("ECB monetary-developments formats repeat")
        document_periods = {
            item
            for item in (_document_reference_period(uri) for uri in uris)
            if item is not None
        }
        if document_periods and document_periods != {reference}:
            raise ValueError(
                "ECB monetary-developments URI and reference period differ"
            )
        from_uri = len(document_periods) == 1
        if self.reference_period_from_uri != from_uri:
            raise ValueError(
                "ECB monetary-developments period provenance differs"
            )
        if not from_uri and tuple(
            Path(urlsplit(item).path).name for item in uris
        ) != ("m3release.pdf",):
            raise ValueError(
                "unexpected sequence-inferred ECB reference period"
            )
        local_release = datetime.fromtimestamp(
            self.database_timestamp, timezone.utc
        ).astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
        corrected_dates = {
            _FOEDB_RELEASE_DATE_OVERRIDES.get(Path(urlsplit(item).path).name)
            for item in uris
        } - {None}
        corrected = len(corrected_dates) == 1
        if (
            self.release_date_corrected_from_document != corrected
            or (corrected and corrected_dates != {release})
            or (not corrected and local_release.date().isoformat() != release)
        ):
            raise ValueError("ECB money release-date provenance differs")
        period_start = date.fromisoformat(f"{reference}-01")
        period_end = date(
            period_start.year,
            period_start.month,
            calendar.monthrange(period_start.year, period_start.month)[1],
        )
        lag = (date.fromisoformat(release) - period_end).days
        if not 20 <= lag <= 45:
            raise ValueError("ECB monetary-developments release lag differs")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        lexical = _optional_text(self.published_lexical)
        expected_precision, expected_ns, expected_lexical = _publication_time(
            release,
            self.database_timestamp,
            release_date_corrected_from_document=corrected,
        )
        if (
            precision is not expected_precision
            or self.published_at_ns != expected_ns
            or lexical != expected_lexical
        ):
            raise ValueError(
                "ECB monetary-developments publication time differs"
            )
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "document_uris", uris)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "published_lexical", lexical)
        expected = _stable_id(
            "ecb-monetary-developments-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError(
                "ECB monetary-developments publication identity differs"
            )
        object.__setattr__(self, "publication_id", expected)

    @property
    def exact_minute(self) -> bool:
        return self.time_precision is EconomicTimePrecision.EXACT_MINUTE

    @property
    def primary_document_uri(self) -> str:
        return next(
            (item for item in self.document_uris if item.endswith(".html")),
            self.document_uris[0],
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "database_timestamp": self.database_timestamp,
            "release_date": self.release_date,
            "reference_period": self.reference_period,
            "reference_period_from_uri": self.reference_period_from_uri,
            "release_date_corrected_from_document": (
                self.release_date_corrected_from_document
            ),
            "source_title": self.source_title,
            "document_uris": list(self.document_uris),
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
    ) -> EcbFoedbMonetaryDevelopmentsPublicationV1:
        return cls(
            record_id=cast(int, data.get("record_id")),
            database_timestamp=cast(int, data.get("database_timestamp")),
            release_date=str(data.get("release_date", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_from_uri=cast(
                bool, data.get("reference_period_from_uri")
            ),
            release_date_corrected_from_document=cast(
                bool, data.get("release_date_corrected_from_document")
            ),
            source_title=str(data.get("source_title", "")),
            document_uris=tuple(
                str(item)
                for item in _sequence(
                    data.get("document_uris"), "document_uris"
                )
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
class EcbFoedbMonetaryDevelopmentsReleaseIndexV1:
    """Hash-bound complete FOEDB inventory and bounded type-10 selection."""

    as_of_date: str
    database_version: str
    database_version_hash: str
    database_total_records: int
    database_chunk_size: int
    database_chunk_group_size: int
    database_artifacts: tuple[EcbArchiveArtifactV1, ...]
    type_record_count: int
    pre_window_excluded_count: int
    post_boundary_excluded_count: int
    predecessor: EcbFoedbMonetaryDevelopmentsPublicationV1
    publications: tuple[EcbFoedbMonetaryDevelopmentsPublicationV1, ...]
    index_id: str = ""
    schema_version: str = ECB_MONETARY_DEVELOPMENTS_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_MONETARY_DEVELOPMENTS_INDEX_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported ECB monetary-developments index schema"
            )
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
            raise ValueError(
                "ECB monetary-developments chunk sizes must be positive"
            )
        artifacts = tuple(self.database_artifacts)
        roles = [item.role for item in artifacts]
        if roles.count(EcbArtifactRole.FOEDB_VERSIONS) != 1:
            raise ValueError("ECB money index requires one version artifact")
        if roles.count(EcbArtifactRole.FOEDB_METADATA) != 1:
            raise ValueError("ECB money index requires one metadata artifact")
        if roles.count(EcbArtifactRole.FOEDB_CHUNK) != math.ceil(
            total / chunk_size
        ):
            raise ValueError("ECB money index has incomplete database chunks")
        if any(
            role
            not in {
                EcbArtifactRole.FOEDB_VERSIONS,
                EcbArtifactRole.FOEDB_METADATA,
                EcbArtifactRole.FOEDB_CHUNK,
            }
            for role in roles
        ):
            raise ValueError("ECB release documents do not belong in the index")
        type_count = _bounded_int(
            self.type_record_count, "type_record_count", 100_000
        )
        before = _bounded_int(
            self.pre_window_excluded_count,
            "pre_window_excluded_count",
            type_count,
        )
        after = _bounded_int(
            self.post_boundary_excluded_count,
            "post_boundary_excluded_count",
            type_count,
        )
        predecessor = self.predecessor
        if (
            predecessor.release_date
            != ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_DATE
            or predecessor.reference_period
            != _EXPECTED_PREDECESSOR_REFERENCE_PERIOD
        ):
            raise ValueError("ECB money predecessor boundary differs")
        publications = tuple(self.publications)
        if not 1 <= len(publications) <= MAX_ECB_MONETARY_DEVELOPMENTS:
            raise ValueError("ECB monetary-developments count is invalid")
        if before + 1 + len(publications) + after != type_count:
            raise ValueError("ECB type-10 inventory accounting differs")
        if (
            publications[0].release_date
            != ECB_MONETARY_DEVELOPMENTS_ARCHIVE_START_DATE
            or publications[0].reference_period
            != _EXPECTED_FIRST_REFERENCE_PERIOD
            or publications[-1].release_date > as_of
        ):
            raise ValueError("ECB money archive boundary differs")
        dates = [item.release_date for item in publications]
        periods = [item.reference_period for item in publications]
        if dates != sorted(dates) or len(set(dates)) != len(dates):
            raise ValueError(
                "ECB money release dates are not unique and ordered"
            )
        if len(set(periods)) != len(periods) or any(
            current != _shift_month(periods[0], offset)
            for offset, current in enumerate(periods)
        ):
            raise ValueError("ECB money reference months are not contiguous")
        selected = (predecessor, *publications)
        if len({item.record_id for item in selected}) != len(selected):
            raise ValueError("ECB money index repeats a FOEDB record")
        all_uris = [uri for item in selected for uri in item.document_uris]
        if len(set(all_uris)) != len(all_uris):
            raise ValueError("ECB money index repeats a document URI")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "database_version", version)
        object.__setattr__(self, "database_version_hash", version_hash)
        object.__setattr__(self, "database_artifacts", artifacts)
        object.__setattr__(self, "publications", publications)
        expected = _stable_id(
            "ecb-monetary-developments-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected:
            raise ValueError("ECB monetary-developments index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def selected_publications(
        self,
    ) -> tuple[EcbFoedbMonetaryDevelopmentsPublicationV1, ...]:
        return (self.predecessor, *self.publications)

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
            "type_record_count": self.type_record_count,
            "pre_window_excluded_count": self.pre_window_excluded_count,
            "post_boundary_excluded_count": self.post_boundary_excluded_count,
            "predecessor": self.predecessor.to_dict(),
            "publications": [item.to_dict() for item in self.publications],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbFoedbMonetaryDevelopmentsReleaseIndexV1:
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
            type_record_count=cast(int, data.get("type_record_count")),
            pre_window_excluded_count=cast(
                int, data.get("pre_window_excluded_count")
            ),
            post_boundary_excluded_count=cast(
                int, data.get("post_boundary_excluded_count")
            ),
            predecessor=EcbFoedbMonetaryDevelopmentsPublicationV1.from_dict(
                _mapping(data.get("predecessor"), "predecessor")
            ),
            publications=tuple(
                EcbFoedbMonetaryDevelopmentsPublicationV1.from_dict(
                    _mapping(item, "publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _artifacts(
    values: Sequence[EcbArchiveArtifactV1], name: str
) -> tuple[EcbArchiveArtifactV1, ...]:
    artifacts = tuple(values)
    if not 1 <= len(artifacts) <= 2:
        raise ValueError(f"{name} artifact count is invalid")
    roles = [item.role for item in artifacts]
    if (
        roles.count(EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML) > 1
        or roles.count(EcbArtifactRole.MONETARY_DEVELOPMENTS_PDF) > 1
    ):
        raise ValueError(f"{name} artifact roles repeat")
    if any(
        item
        not in {
            EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML,
            EcbArtifactRole.MONETARY_DEVELOPMENTS_PDF,
        }
        for item in roles
    ):
        raise ValueError(f"{name} artifact role differs")
    return artifacts


@dataclass(frozen=True, slots=True)
class EcbMonetaryDevelopmentsPredecessorV1:
    """Immediate pre-window M3 value used by the first revision comparison."""

    publication_id: str
    release_date: str
    reference_period: str
    actual_lexical: str
    actual_value: float
    value_source_uri: str
    value_source_format: OfficialSourceFormat
    parser_era: str
    adjustment_disclosure: bool
    source_excerpt: str
    artifacts: tuple[EcbArchiveArtifactV1, ...]
    predecessor_id: str = ""
    schema_version: str = ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB money predecessor schema")
        publication_id = _required_text(self.publication_id, "publication_id")
        if not publication_id.startswith(
            "ecb-monetary-developments-publication:sha256:"
        ):
            raise ValueError("ECB money predecessor publication ID differs")
        release = _iso_date(self.release_date, "release_date")
        reference = _month(self.reference_period, "reference_period")
        if (
            release != ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_DATE
            or reference != _EXPECTED_PREDECESSOR_REFERENCE_PERIOD
        ):
            raise ValueError("ECB money predecessor date or period differs")
        actual_lexical, parsed_actual = _number(
            self.actual_lexical, "actual_lexical"
        )
        if not math.isclose(parsed_actual, self.actual_value, abs_tol=1e-15):
            raise ValueError("ECB money predecessor actual value differs")
        value_uri = _canonical_uri(self.value_source_uri)
        value_format = OfficialSourceFormat.from_value(self.value_source_format)
        era = _required_text(self.parser_era, "parser_era")
        expected_era = (
            "html-main-text"
            if value_format is OfficialSourceFormat.HTML
            else "pdf-extracted-text"
        )
        if era != expected_era:
            raise ValueError("ECB money predecessor parser era differs")
        if not isinstance(self.adjustment_disclosure, bool):
            raise TypeError("adjustment_disclosure must be a boolean")
        excerpt = _required_text(self.source_excerpt, "source_excerpt")
        if len(excerpt) > MAX_ECB_MONETARY_DEVELOPMENTS_EXCERPT_CHARS:
            raise ValueError("ECB money predecessor excerpt exceeds bound")
        artifacts = _artifacts(self.artifacts, "predecessor")
        matched = [item for item in artifacts if item.source_uri == value_uri]
        if len(matched) != 1 or matched[0].source_format is not value_format:
            raise ValueError("ECB money predecessor evidence is not retained")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "actual_lexical", actual_lexical)
        object.__setattr__(self, "actual_value", parsed_actual)
        object.__setattr__(self, "value_source_uri", value_uri)
        object.__setattr__(self, "value_source_format", value_format)
        object.__setattr__(self, "parser_era", era)
        object.__setattr__(self, "source_excerpt", excerpt)
        object.__setattr__(self, "artifacts", artifacts)
        expected = _stable_id(
            "ecb-monetary-developments-predecessor", self.identity_payload()
        )
        if self.predecessor_id and self.predecessor_id != expected:
            raise ValueError("ECB money predecessor identity differs")
        object.__setattr__(self, "predecessor_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "release_date": self.release_date,
            "reference_period": self.reference_period,
            "actual_lexical": self.actual_lexical,
            "actual_value": self.actual_value,
            "value_source_uri": self.value_source_uri,
            "value_source_format": self.value_source_format.value,
            "parser_era": self.parser_era,
            "adjustment_disclosure": self.adjustment_disclosure,
            "source_excerpt": self.source_excerpt,
            "artifacts": [item.to_dict() for item in self.artifacts],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "predecessor_id": self.predecessor_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbMonetaryDevelopmentsPredecessorV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            release_date=str(data.get("release_date", "")),
            reference_period=str(data.get("reference_period", "")),
            actual_lexical=str(data.get("actual_lexical", "")),
            actual_value=cast(float, data.get("actual_value")),
            value_source_uri=str(data.get("value_source_uri", "")),
            value_source_format=OfficialSourceFormat.from_value(
                str(data.get("value_source_format", ""))
            ),
            parser_era=str(data.get("parser_era", "")),
            adjustment_disclosure=cast(bool, data.get("adjustment_disclosure")),
            source_excerpt=str(data.get("source_excerpt", "")),
            artifacts=tuple(
                EcbArchiveArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            predecessor_id=str(data.get("predecessor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryDevelopmentsReleaseV1:
    """One monthly M3 release with an occurrence-specific revision lineage."""

    publication_id: str
    previous_publication_id: str
    release_date: str
    reference_period: str
    previous_reference_period: str
    actual_lexical: str
    actual_value: float
    previous_first_published_lexical: str
    previous_first_published_value: float
    previous_as_known_lexical: str
    previous_as_known_value: float
    revision_value: float
    value_source_uri: str
    value_source_format: OfficialSourceFormat
    previous_value_source_uri: str
    parser_era: str
    adjustment_disclosure: bool
    source_excerpt: str
    artifacts: tuple[EcbArchiveArtifactV1, ...]
    release_id: str = ""
    schema_version: str = ECB_MONETARY_DEVELOPMENTS_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_MONETARY_DEVELOPMENTS_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB money release schema")
        publication_id = _required_text(self.publication_id, "publication_id")
        previous_id = _required_text(
            self.previous_publication_id, "previous_publication_id"
        )
        prefix = "ecb-monetary-developments-publication:sha256:"
        if not publication_id.startswith(prefix) or not previous_id.startswith(
            prefix
        ):
            raise ValueError("ECB money publication lineage ID differs")
        release = _iso_date(self.release_date, "release_date")
        reference = _month(self.reference_period, "reference_period")
        previous_reference = _month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_reference != _previous_month(reference):
            raise ValueError("ECB money previous reference period differs")
        actual_lexical, actual = _number(self.actual_lexical, "actual_lexical")
        first_lexical, first = _number(
            self.previous_first_published_lexical,
            "previous_first_published_lexical",
        )
        known_lexical, known = _number(
            self.previous_as_known_lexical, "previous_as_known_lexical"
        )
        if (
            not math.isclose(actual, self.actual_value, abs_tol=1e-15)
            or not math.isclose(
                first, self.previous_first_published_value, abs_tol=1e-15
            )
            or not math.isclose(
                known, self.previous_as_known_value, abs_tol=1e-15
            )
        ):
            raise ValueError("ECB money lexical/numeric values differ")
        revision = round(known - first, 1)
        if not math.isclose(revision, self.revision_value, abs_tol=1e-15):
            raise ValueError("ECB money revision value differs")
        value_uri = _canonical_uri(self.value_source_uri)
        previous_uri = _canonical_uri(self.previous_value_source_uri)
        value_format = OfficialSourceFormat.from_value(self.value_source_format)
        era = _required_text(self.parser_era, "parser_era")
        expected_era = (
            "html-main-text"
            if value_format is OfficialSourceFormat.HTML
            else "pdf-extracted-text"
        )
        if era != expected_era:
            raise ValueError("ECB money release parser era differs")
        if not isinstance(self.adjustment_disclosure, bool):
            raise TypeError("adjustment_disclosure must be a boolean")
        excerpt = _required_text(self.source_excerpt, "source_excerpt")
        if len(excerpt) > MAX_ECB_MONETARY_DEVELOPMENTS_EXCERPT_CHARS:
            raise ValueError("ECB money release excerpt exceeds bound")
        artifacts = _artifacts(self.artifacts, "release")
        matched = [item for item in artifacts if item.source_uri == value_uri]
        if len(matched) != 1 or matched[0].source_format is not value_format:
            raise ValueError("ECB money release value evidence is not retained")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "previous_publication_id", previous_id)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(
            self, "previous_reference_period", previous_reference
        )
        object.__setattr__(self, "actual_lexical", actual_lexical)
        object.__setattr__(self, "actual_value", actual)
        object.__setattr__(
            self, "previous_first_published_lexical", first_lexical
        )
        object.__setattr__(self, "previous_first_published_value", first)
        object.__setattr__(self, "previous_as_known_lexical", known_lexical)
        object.__setattr__(self, "previous_as_known_value", known)
        object.__setattr__(self, "revision_value", revision)
        object.__setattr__(self, "value_source_uri", value_uri)
        object.__setattr__(self, "value_source_format", value_format)
        object.__setattr__(self, "previous_value_source_uri", previous_uri)
        object.__setattr__(self, "parser_era", era)
        object.__setattr__(self, "source_excerpt", excerpt)
        object.__setattr__(self, "artifacts", artifacts)
        expected = _stable_id(
            "ecb-monetary-developments-release", self.identity_payload()
        )
        if self.release_id and self.release_id != expected:
            raise ValueError("ECB money release identity differs")
        object.__setattr__(self, "release_id", expected)

    @property
    def value_was_revised(self) -> bool:
        return not math.isclose(self.revision_value, 0.0, abs_tol=1e-15)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "previous_publication_id": self.previous_publication_id,
            "release_date": self.release_date,
            "reference_period": self.reference_period,
            "previous_reference_period": self.previous_reference_period,
            "actual_lexical": self.actual_lexical,
            "actual_value": self.actual_value,
            "previous_first_published_lexical": (
                self.previous_first_published_lexical
            ),
            "previous_first_published_value": (
                self.previous_first_published_value
            ),
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "revision_value": self.revision_value,
            "value_source_uri": self.value_source_uri,
            "value_source_format": self.value_source_format.value,
            "previous_value_source_uri": self.previous_value_source_uri,
            "parser_era": self.parser_era,
            "adjustment_disclosure": self.adjustment_disclosure,
            "source_excerpt": self.source_excerpt,
            "artifacts": [item.to_dict() for item in self.artifacts],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbMonetaryDevelopmentsReleaseV1:
        return cls(
            publication_id=str(data.get("publication_id", "")),
            previous_publication_id=str(
                data.get("previous_publication_id", "")
            ),
            release_date=str(data.get("release_date", "")),
            reference_period=str(data.get("reference_period", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            actual_lexical=str(data.get("actual_lexical", "")),
            actual_value=cast(float, data.get("actual_value")),
            previous_first_published_lexical=str(
                data.get("previous_first_published_lexical", "")
            ),
            previous_first_published_value=cast(
                float, data.get("previous_first_published_value")
            ),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            previous_as_known_value=cast(
                float, data.get("previous_as_known_value")
            ),
            revision_value=cast(float, data.get("revision_value")),
            value_source_uri=str(data.get("value_source_uri", "")),
            value_source_format=OfficialSourceFormat.from_value(
                str(data.get("value_source_format", ""))
            ),
            previous_value_source_uri=str(
                data.get("previous_value_source_uri", "")
            ),
            parser_era=str(data.get("parser_era", "")),
            adjustment_disclosure=cast(bool, data.get("adjustment_disclosure")),
            source_excerpt=str(data.get("source_excerpt", "")),
            artifacts=tuple(
                EcbArchiveArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbMonetaryDevelopmentsArchiveManifestV1:
    """Compact receipt for the complete qualified monthly M3 lineage."""

    registry_id: str
    source_id: str
    economy_code: str
    measure_key: str
    frequency: str
    unit: str
    transformation: str
    release_index: EcbFoedbMonetaryDevelopmentsReleaseIndexV1
    predecessor: EcbMonetaryDevelopmentsPredecessorV1
    releases: tuple[EcbMonetaryDevelopmentsReleaseV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    release_count: int
    pdf_artifact_count: int
    html_artifact_count: int
    exact_minute_count: int
    date_only_count: int
    adjustment_disclosure_count: int
    revision_occurrence_count: int
    reference_period_inferred_count: int
    release_date_correction_count: int
    limitations: tuple[str, ...]
    manifest_id: str = ""
    schema_version: str = ECB_MONETARY_DEVELOPMENTS_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_MONETARY_DEVELOPMENTS_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB money manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if self.economy_code != "EA":
            raise ValueError("ECB money archive must retain euro-area scope")
        if self.measure_key != ECB_MONETARY_DEVELOPMENTS_MEASURE_KEY:
            raise ValueError("ECB money measure differs")
        if (
            self.frequency,
            self.unit,
            self.transformation,
        ) != ("monthly", "percent", "year-over-year-growth-rate"):
            raise ValueError("ECB money measure semantics differ")
        if (
            self.predecessor.publication_id
            != self.release_index.predecessor.publication_id
        ):
            raise ValueError("ECB money predecessor lineage differs")
        releases = tuple(self.releases)
        publications = self.release_index.publications
        if tuple(item.publication_id for item in releases) != tuple(
            item.publication_id for item in publications
        ):
            raise ValueError("ECB money release lineage differs")
        prior_id = self.predecessor.publication_id
        prior_reference = self.predecessor.reference_period
        prior_lexical = self.predecessor.actual_lexical
        prior_value = self.predecessor.actual_value
        prior_uri = self.predecessor.value_source_uri
        for release, publication in zip(releases, publications, strict=True):
            if (
                release.previous_publication_id != prior_id
                or release.previous_reference_period != prior_reference
                or release.previous_first_published_lexical != prior_lexical
                or not math.isclose(
                    release.previous_first_published_value,
                    prior_value,
                    abs_tol=1e-15,
                )
                or release.previous_value_source_uri != prior_uri
                or release.release_date != publication.release_date
                or release.reference_period != publication.reference_period
            ):
                raise ValueError("ECB money previous-release lineage differs")
            if tuple(item.source_uri for item in release.artifacts) != (
                publication.document_uris
            ):
                raise ValueError("ECB money release artifact lineage differs")
            prior_id = release.publication_id
            prior_reference = release.reference_period
            prior_lexical = release.actual_lexical
            prior_value = release.actual_value
            prior_uri = release.value_source_uri
        artifacts = (
            *self.release_index.database_artifacts,
            *self.predecessor.artifacts,
            *(artifact for item in releases for artifact in item.artifacts),
        )
        if len(artifacts) > MAX_ECB_MONETARY_DEVELOPMENTS_ARTIFACTS:
            raise ValueError("ECB money artifact count exceeds bound")
        expected = (
            len(artifacts),
            len({item.content_sha256 for item in artifacts}),
            sum(item.content_length for item in artifacts),
            len(releases),
            sum(
                item.role is EcbArtifactRole.MONETARY_DEVELOPMENTS_PDF
                for item in artifacts
            ),
            sum(
                item.role is EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML
                for item in artifacts
            ),
            sum(item.exact_minute for item in publications),
            sum(not item.exact_minute for item in publications),
            sum(item.adjustment_disclosure for item in releases),
            sum(item.value_was_revised for item in releases),
            sum(
                not item.reference_period_from_uri
                for item in self.release_index.selected_publications
            ),
            sum(
                item.release_date_corrected_from_document
                for item in self.release_index.selected_publications
            ),
        )
        observed = (
            self.raw_artifact_count,
            self.unique_content_sha256_count,
            self.total_content_bytes,
            self.release_count,
            self.pdf_artifact_count,
            self.html_artifact_count,
            self.exact_minute_count,
            self.date_only_count,
            self.adjustment_disclosure_count,
            self.revision_occurrence_count,
            self.reference_period_inferred_count,
            self.release_date_correction_count,
        )
        if observed != expected:
            raise ValueError("ECB money manifest summary counts differ")
        _bounded_int(
            self.total_content_bytes,
            "total_content_bytes",
            MAX_ECB_MONETARY_DEVELOPMENTS_TOTAL_BYTES,
        )
        limitations = _texts(self.limitations, "limitations")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(self, "limitations", limitations)
        expected_id = _stable_id(
            "ecb-monetary-developments-archive-manifest",
            self.identity_payload(),
        )
        if self.manifest_id and self.manifest_id != expected_id:
            raise ValueError("ECB money manifest identity differs")
        object.__setattr__(self, "manifest_id", expected_id)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "economy_code": self.economy_code,
            "measure_key": self.measure_key,
            "frequency": self.frequency,
            "unit": self.unit,
            "transformation": self.transformation,
            "release_index": self.release_index.to_dict(),
            "predecessor": self.predecessor.to_dict(),
            "releases": [item.to_dict() for item in self.releases],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "release_count": self.release_count,
            "pdf_artifact_count": self.pdf_artifact_count,
            "html_artifact_count": self.html_artifact_count,
            "exact_minute_count": self.exact_minute_count,
            "date_only_count": self.date_only_count,
            "adjustment_disclosure_count": self.adjustment_disclosure_count,
            "revision_occurrence_count": self.revision_occurrence_count,
            "reference_period_inferred_count": (
                self.reference_period_inferred_count
            ),
            "release_date_correction_count": (
                self.release_date_correction_count
            ),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        encoded: str = canonical_contract_json(self.to_dict())
        return encoded

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbMonetaryDevelopmentsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            economy_code=str(data.get("economy_code", "")),
            measure_key=str(data.get("measure_key", "")),
            frequency=str(data.get("frequency", "")),
            unit=str(data.get("unit", "")),
            transformation=str(data.get("transformation", "")),
            release_index=(
                EcbFoedbMonetaryDevelopmentsReleaseIndexV1.from_dict(
                    _mapping(data.get("release_index"), "release_index")
                )
            ),
            predecessor=EcbMonetaryDevelopmentsPredecessorV1.from_dict(
                _mapping(data.get("predecessor"), "predecessor")
            ),
            releases=tuple(
                EcbMonetaryDevelopmentsReleaseV1.from_dict(
                    _mapping(item, "release")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            release_count=cast(int, data.get("release_count")),
            pdf_artifact_count=cast(int, data.get("pdf_artifact_count")),
            html_artifact_count=cast(int, data.get("html_artifact_count")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            date_only_count=cast(int, data.get("date_only_count")),
            adjustment_disclosure_count=cast(
                int, data.get("adjustment_disclosure_count")
            ),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            reference_period_inferred_count=cast(
                int, data.get("reference_period_inferred_count")
            ),
            release_date_correction_count=cast(
                int, data.get("release_date_correction_count")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, value: str) -> EcbMonetaryDevelopmentsArchiveManifestV1:
        try:
            return cls.from_dict(_mapping(json.loads(value)))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB money manifest JSON is invalid") from exc


class _MonetaryDevelopmentsHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_main = False
        self.found_main = False
        self.text: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag == "main":
            self.found_main = True
            self._in_main = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "main":
            self._in_main = False

    def handle_data(self, data: str) -> None:
        if self._in_main:
            self.text.append(data)


@dataclass(frozen=True, slots=True)
class _ParsedM3:
    actual_lexical: str
    actual_value: float
    previous_lexical: str
    previous_value: float
    source_excerpt: str
    parser_era: str
    adjustment_disclosure: bool


def _normalize_document_text(value: str) -> str:
    result = value
    for character in (
        "\N{MINUS SIGN}",
        "\N{NON-BREAKING HYPHEN}",
        "\N{FIGURE DASH}",
        "\N{EN DASH}",
        "\N{EM DASH}",
        "\x96",
    ):
        result = result.replace(character, "-")
    result = result.replace("\N{SOFT HYPHEN}", "").replace("\xa0", " ")
    substitutions = (
        (r"\bmone\s+tary\b", "monetary"),
        (r"\b(?:aggr\s+egate|aggre\s+gate|aggregat\s+e)\b", "aggregate"),
        (r"\bgrow\s+th\b", "growth"),
        (r"\bfr\s+om\b", "from"),
        (r"end-of-\s+month", "end-of-month"),
    )
    for pattern, replacement in substitutions:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    result = re.sub(r"(?<=\d)\.\s+(?=\d+\s*%)", ".", result)
    return re.sub(r"\s+", " ", result).strip()


def _html_text(content: bytes) -> str:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB monetary-developments HTML is not UTF-8") from exc
    parser = _MonetaryDevelopmentsHtmlParser()
    parser.feed(text)
    parser.close()
    if not parser.found_main:
        raise ValueError("ECB monetary-developments HTML omits main content")
    result = _normalize_document_text(" ".join(parser.text))
    if not result:
        raise ValueError("ECB monetary-developments HTML has no main text")
    return result


def _pdf_text(content: bytes, *, require_text: bool = True) -> str:
    if not content.startswith(b"%PDF-"):
        raise ValueError("ECB monetary-developments PDF lacks a signature")
    try:
        reader = PdfReader(BytesIO(content))
        if (
            not 1
            <= len(reader.pages)
            <= MAX_ECB_MONETARY_DEVELOPMENTS_PDF_PAGES
        ):
            raise ValueError("ECB money PDF page count is outside its bound")
        text = " ".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ValueError(
            "ECB monetary-developments PDF cannot be parsed"
        ) from exc
    result = _normalize_document_text(text)
    if require_text and not result:
        raise ValueError("ECB monetary-developments PDF has no text")
    return result


def _parse_m3_statement(
    content: bytes,
    source_format: OfficialSourceFormat,
    reference_period: str,
) -> _ParsedM3:
    if source_format is OfficialSourceFormat.HTML:
        text = _html_text(content)
        era = "html-main-text"
    elif source_format is OfficialSourceFormat.PDF:
        text = _pdf_text(content)
        era = "pdf-extracted-text"
    else:
        raise ValueError("ECB monetary-developments value source differs")
    heading = f"{_GENERIC_TITLE}: {_month_label(reference_period)}"
    if heading.casefold() not in text[:2_500].casefold():
        raise ValueError(
            "ECB monetary-developments heading and reference period differ"
        )
    for headline in _M3_HEADLINE_RE.finditer(text[:15_000]):
        tail = text[headline.start() : headline.start() + 2_500]
        stops = [
            position
            for phrase in ("three-month average", "three- month average")
            if (position := tail.casefold().find(phrase)) >= 0
        ]
        if stops:
            tail = tail[: min(stops)]
        numbers = list(_PERCENT_RE.finditer(tail))
        if not numbers:
            continue
        actual_lexical, actual = _number(
            numbers[0].group("number"), "actual M3"
        )
        between = (
            tail[numbers[0].end() : numbers[1].start()]
            if len(numbers) >= 2
            else tail[numbers[0].end() :]
        )
        unchanged = "unchanged from" in between.casefold()
        if unchanged:
            previous_lexical, previous = actual_lexical, actual
            evidence_end = min(len(tail), numbers[0].end() + 240)
        elif len(numbers) >= 2:
            previous_lexical, previous = _number(
                numbers[1].group("number"), "previous-as-known M3"
            )
            evidence_end = numbers[1].end()
            revision_tail = tail[evidence_end : evidence_end + 180]
            if (
                "revised" in revision_tail.casefold()
                and (closing := revision_tail.find(")")) >= 0
            ):
                evidence_end += closing + 1
        else:
            continue
        excerpt = tail[:evidence_end].strip(" ;")
        if len(excerpt) > MAX_ECB_MONETARY_DEVELOPMENTS_EXCERPT_CHARS:
            excerpt = excerpt[:MAX_ECB_MONETARY_DEVELOPMENTS_EXCERPT_CHARS]
        normalized = text.casefold()
        adjustment = (
            "seasonal and end-of-month calendar effects" in normalized
            or "seasonal and end of month calendar effects" in normalized
        )
        return _ParsedM3(
            actual_lexical=actual_lexical,
            actual_value=actual,
            previous_lexical=previous_lexical,
            previous_value=previous,
            source_excerpt=excerpt,
            parser_era=era,
            adjustment_disclosure=adjustment,
        )
    raise ValueError(
        "ECB monetary-developments M3 statement cannot be qualified: "
        f"{reference_period}"
    )


def build_ecb_foedb_monetary_developments_release_index(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EcbFoedbMonetaryDevelopmentsReleaseIndexV1:
    """Select the complete FOEDB type-10 lineage at an explicit boundary."""
    as_of = _iso_date(as_of_date, "as_of_date")
    database = _decode_ecb_foedb_database(
        versions_snapshot, metadata_snapshot, chunk_snapshots
    )
    records = sorted(
        (record for record in database.records if record.get("type") == 10),
        key=lambda item: _bounded_int(
            item.get("pub_timestamp"), "publication timestamp", 2**63 - 1
        ),
    )
    if not records:
        raise ValueError("ECB FOEDB omits monetary-developments records")
    publications: list[EcbFoedbMonetaryDevelopmentsPublicationV1] = []
    for offset, record in enumerate(records):
        reference = _shift_month(_FIRST_FOEDB_REFERENCE_PERIOD, offset)
        timestamp = _bounded_int(
            record.get("pub_timestamp"), "publication timestamp", 2**63 - 1
        )
        local = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(
            ZoneInfo(ECB_SOURCE_TIMEZONE)
        )
        properties = _mapping(
            record.get("publicationProperties") or {},
            "publicationProperties",
        )
        paths = _sequence(record.get("documentTypes"), "documentTypes")
        uris = tuple(
            urljoin("https://www.ecb.europa.eu", str(item)) for item in paths
        )
        if not uris:
            raise ValueError("ECB monetary-developments record omits documents")
        corrected_dates = {
            _FOEDB_RELEASE_DATE_OVERRIDES.get(Path(urlsplit(item).path).name)
            for item in uris
        } - {None}
        if len(corrected_dates) > 1:
            raise ValueError("ECB money documents name conflicting corrections")
        corrected = bool(corrected_dates)
        release = (
            cast(str, next(iter(corrected_dates)))
            if corrected
            else local.date().isoformat()
        )
        precision, published_at_ns, lexical = _publication_time(
            release,
            timestamp,
            release_date_corrected_from_document=corrected,
        )
        publications.append(
            EcbFoedbMonetaryDevelopmentsPublicationV1(
                record_id=_bounded_int(
                    record.get("id"), "record id", 2**63 - 1
                ),
                database_timestamp=timestamp,
                release_date=release,
                reference_period=reference,
                reference_period_from_uri=all(
                    _document_reference_period(uri) is not None for uri in uris
                ),
                release_date_corrected_from_document=corrected,
                source_title=_source_title(properties.get("Title"), reference),
                document_uris=uris,
                time_precision=precision,
                published_at_ns=published_at_ns,
                published_lexical=lexical,
            )
        )
    by_date = {item.release_date: item for item in publications}
    if len(by_date) != len(publications):
        raise ValueError("ECB type-10 records repeat a release date")
    predecessor = by_date.get(ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_DATE)
    if predecessor is None:
        raise ValueError("ECB type-10 inventory omits the predecessor")
    selected = tuple(
        item
        for item in publications
        if ECB_MONETARY_DEVELOPMENTS_ARCHIVE_START_DATE
        <= item.release_date
        <= as_of
    )
    before = sum(
        item.release_date < ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_DATE
        for item in publications
    )
    after = sum(item.release_date > as_of for item in publications)
    return EcbFoedbMonetaryDevelopmentsReleaseIndexV1(
        as_of_date=as_of,
        database_version=database.version,
        database_version_hash=database.version_hash,
        database_total_records=database.total_records,
        database_chunk_size=database.chunk_size,
        database_chunk_group_size=database.chunk_group_size,
        database_artifacts=database.artifacts,
        type_record_count=len(publications),
        pre_window_excluded_count=before,
        post_boundary_excluded_count=after,
        predecessor=predecessor,
        publications=selected,
    )


def build_ecb_monetary_developments_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbMonetaryDevelopmentsReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact request for every selected main release document."""
    requests = tuple(
        _request(
            registry,
            uri,
            (
                OfficialSourceFormat.HTML
                if uri.endswith(".html")
                else OfficialSourceFormat.PDF
            ),
        )
        for publication in release_index.selected_publications
        for uri in publication.document_uris
    )
    if len({item.uri for item in requests}) != len(requests):
        raise ValueError("ECB money request inventory repeats a URI")
    return requests


def _publication_artifacts(
    publication: EcbFoedbMonetaryDevelopmentsPublicationV1,
    snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> tuple[EcbArchiveArtifactV1, ...]:
    artifacts: list[EcbArchiveArtifactV1] = []
    for uri in publication.document_uris:
        snapshot = snapshots[uri]
        role = (
            EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML
            if uri.endswith(".html")
            else EcbArtifactRole.MONETARY_DEVELOPMENTS_PDF
        )
        if role is EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML:
            _html_text(snapshot.content)
        else:
            _pdf_text(
                snapshot.content,
                require_text=uri == publication.primary_document_uri,
            )
        artifacts.append(_artifact(snapshot, role))
    return tuple(artifacts)


def build_ecb_monetary_developments_archive_manifest(
    registry: OfficialSourceRegistryV1,
    release_index: EcbFoedbMonetaryDevelopmentsReleaseIndexV1,
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> EcbMonetaryDevelopmentsArchiveManifestV1:
    """Parse and link every selected annual M3 growth occurrence."""
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    expected_uris = {
        uri
        for item in release_index.selected_publications
        for uri in item.document_uris
    }
    if set(report_snapshots) != expected_uris:
        raise ValueError("ECB retained monetary-developments inventory differs")
    parsed: dict[str, _ParsedM3] = {}
    artifacts: dict[str, tuple[EcbArchiveArtifactV1, ...]] = {}
    for publication in release_index.selected_publications:
        artifacts[publication.publication_id] = _publication_artifacts(
            publication, report_snapshots
        )
        primary_uri = publication.primary_document_uri
        primary_format = (
            OfficialSourceFormat.HTML
            if primary_uri.endswith(".html")
            else OfficialSourceFormat.PDF
        )
        parsed[publication.publication_id] = _parse_m3_statement(
            report_snapshots[primary_uri].content,
            primary_format,
            publication.reference_period,
        )
    predecessor_publication = release_index.predecessor
    predecessor_parsed = parsed[predecessor_publication.publication_id]
    predecessor = EcbMonetaryDevelopmentsPredecessorV1(
        publication_id=predecessor_publication.publication_id,
        release_date=predecessor_publication.release_date,
        reference_period=predecessor_publication.reference_period,
        actual_lexical=predecessor_parsed.actual_lexical,
        actual_value=predecessor_parsed.actual_value,
        value_source_uri=predecessor_publication.primary_document_uri,
        value_source_format=(
            OfficialSourceFormat.HTML
            if predecessor_publication.primary_document_uri.endswith(".html")
            else OfficialSourceFormat.PDF
        ),
        parser_era=predecessor_parsed.parser_era,
        adjustment_disclosure=(predecessor_parsed.adjustment_disclosure),
        source_excerpt=predecessor_parsed.source_excerpt,
        artifacts=artifacts[predecessor_publication.publication_id],
    )
    releases: list[EcbMonetaryDevelopmentsReleaseV1] = []
    previous_publication = predecessor_publication
    previous_parsed = predecessor_parsed
    for publication in release_index.publications:
        current = parsed[publication.publication_id]
        source_format = (
            OfficialSourceFormat.HTML
            if publication.primary_document_uri.endswith(".html")
            else OfficialSourceFormat.PDF
        )
        releases.append(
            EcbMonetaryDevelopmentsReleaseV1(
                publication_id=publication.publication_id,
                previous_publication_id=previous_publication.publication_id,
                release_date=publication.release_date,
                reference_period=publication.reference_period,
                previous_reference_period=previous_publication.reference_period,
                actual_lexical=current.actual_lexical,
                actual_value=current.actual_value,
                previous_first_published_lexical=(
                    previous_parsed.actual_lexical
                ),
                previous_first_published_value=previous_parsed.actual_value,
                previous_as_known_lexical=current.previous_lexical,
                previous_as_known_value=current.previous_value,
                revision_value=round(
                    current.previous_value - previous_parsed.actual_value, 1
                ),
                value_source_uri=publication.primary_document_uri,
                value_source_format=source_format,
                previous_value_source_uri=(
                    previous_publication.primary_document_uri
                ),
                parser_era=current.parser_era,
                adjustment_disclosure=current.adjustment_disclosure,
                source_excerpt=current.source_excerpt,
                artifacts=artifacts[publication.publication_id],
            )
        )
        previous_publication = publication
        previous_parsed = current
    all_artifacts = (
        *release_index.database_artifacts,
        *(artifact for values in artifacts.values() for artifact in values),
    )
    return EcbMonetaryDevelopmentsArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        economy_code="EA",
        measure_key=ECB_MONETARY_DEVELOPMENTS_MEASURE_KEY,
        frequency="monthly",
        unit="percent",
        transformation="year-over-year-growth-rate",
        release_index=release_index,
        predecessor=predecessor,
        releases=tuple(releases),
        raw_artifact_count=len(all_artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in all_artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in all_artifacts),
        release_count=len(releases),
        pdf_artifact_count=sum(
            item.role is EcbArtifactRole.MONETARY_DEVELOPMENTS_PDF
            for item in all_artifacts
        ),
        html_artifact_count=sum(
            item.role is EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML
            for item in all_artifacts
        ),
        exact_minute_count=sum(
            item.exact_minute for item in release_index.publications
        ),
        date_only_count=sum(
            not item.exact_minute for item in release_index.publications
        ),
        adjustment_disclosure_count=sum(
            item.adjustment_disclosure for item in releases
        ),
        revision_occurrence_count=sum(
            item.value_was_revised for item in releases
        ),
        reference_period_inferred_count=sum(
            not item.reference_period_from_uri
            for item in release_index.selected_publications
        ),
        release_date_correction_count=sum(
            item.release_date_corrected_from_document
            for item in release_index.selected_publications
        ),
        limitations=(
            "The qualified measure is the source-published annual M3 growth rate, not the M3 level or every money-and-credit series in each statistical annex.",
            "Adjustment disclosure records whether the release explicitly states seasonal and end-of-month calendar adjustment; absence is not relabelled as an unadjusted series.",
            "When FOEDB also names a localized PDF, it is retained and hashed while the English HTML page remains the value source.",
            "Revision values compare the current release's previous-period figure with that period's value in the immediately preceding retained release.",
        ),
    )


def replay_ecb_monetary_developments_archive(
    registry: OfficialSourceRegistryV1,
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    report_snapshots: Mapping[str, OfficialRawSnapshotV1],
    expected: EcbMonetaryDevelopmentsArchiveManifestV1,
) -> EcbMonetaryDevelopmentsArchiveManifestV1:
    """Rebuild the retained monthly M3 corpus and require exact equality."""
    release_index = build_ecb_foedb_monetary_developments_release_index(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_ecb_monetary_developments_archive_manifest(
        registry, release_index, report_snapshots
    )
    if rebuilt != expected:
        raise ValueError("ECB monetary-developments retained replay differs")
    return rebuilt


def packaged_ecb_monetary_developments_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / (
        "ecb_monetary_developments_archive_v1.json"
    )


def load_packaged_ecb_monetary_developments_archive_manifest() -> (
    EcbMonetaryDevelopmentsArchiveManifestV1
):
    """Load and validate the packaged compact monthly M3 manifest."""
    return EcbMonetaryDevelopmentsArchiveManifestV1.from_json(
        packaged_ecb_monetary_developments_manifest_path().read_text(
            encoding="utf-8"
        )
    )


__all__ = [
    "ECB_LATEST_PACKAGED_MONETARY_DEVELOPMENTS_DATE",
    "ECB_MONETARY_DEVELOPMENTS_ARCHIVE_START_DATE",
    "ECB_MONETARY_DEVELOPMENTS_ENTRY_SCHEMA_VERSION",
    "ECB_MONETARY_DEVELOPMENTS_FIRST_EXACT_DATE",
    "ECB_MONETARY_DEVELOPMENTS_INDEX_SCHEMA_VERSION",
    "ECB_MONETARY_DEVELOPMENTS_MANIFEST_SCHEMA_VERSION",
    "ECB_MONETARY_DEVELOPMENTS_MEASURE_KEY",
    "ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_DATE",
    "ECB_MONETARY_DEVELOPMENTS_PREDECESSOR_SCHEMA_VERSION",
    "ECB_MONETARY_DEVELOPMENTS_PROGRAM_KEY",
    "ECB_MONETARY_DEVELOPMENTS_RELEASE_SCHEMA_VERSION",
    "EcbFoedbMonetaryDevelopmentsPublicationV1",
    "EcbFoedbMonetaryDevelopmentsReleaseIndexV1",
    "EcbMonetaryDevelopmentsArchiveManifestV1",
    "EcbMonetaryDevelopmentsPredecessorV1",
    "EcbMonetaryDevelopmentsReleaseV1",
    "build_ecb_foedb_monetary_developments_release_index",
    "build_ecb_monetary_developments_archive_manifest",
    "build_ecb_monetary_developments_requests",
    "load_packaged_ecb_monetary_developments_archive_manifest",
    "packaged_ecb_monetary_developments_manifest_path",
    "replay_ecb_monetary_developments_archive",
]

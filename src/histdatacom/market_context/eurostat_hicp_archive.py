"""Deterministic qualification of Eurostat HICP first-release evidence.

Eurostat exposes the HICP lineage through three independent official surfaces:
the migrated HICP publication index, the retired ECOICOP first-published-data
table, and its ECOICOP version 2 successor.  This module retains all three,
recovers source-dated publications that pre-date the migrated index, and keeps
flash estimates distinct from final releases.  It deliberately does not treat
the current revised HICP table as historical-vintage evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urljoin, urlsplit

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

EUROSTAT_HICP_SOURCE_KEY: Final = "ea.eurostat.hicp"
EUROSTAT_HICP_PROGRAM_KEY: Final = "ea.eurostat.hicp-first-releases"
EUROSTAT_HICP_MEASURE_KEY: Final = "hicp-all-items-annual-rate"
EUROSTAT_HICP_SOURCE_TIMEZONE: Final = "Europe/Brussels"
EUROSTAT_HICP_FINAL_START_REFERENCE_PERIOD: Final = "2000-01"
EUROSTAT_HICP_FIRST_FLASH_REFERENCE_PERIOD: Final = "2001-10"
EUROSTAT_HICP_MIGRATED_INDEX_START_DATE: Final = "2004-01-05"
EUROSTAT_HICP_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-09-01"
EUROSTAT_HICP_AS_OF_DATE: Final = "2026-09-15"

EUROSTAT_HICP_PUBLICATIONS_URI: Final = (
    "https://ec.europa.eu/eurostat/en/web/hicp/publications"
)
EUROSTAT_HICP_LEGACY_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "prc_hicp_fp?lang=en&unit=RCH_A&coicop=CP00&geo=EA&geo=DE&geo=FR"
    "&sinceTimePeriod=2000-01"
)
EUROSTAT_HICP_CURRENT_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "prc_hicp_fpd?lang=en&unit=RCH_A&coicop18=TOTAL&geo=EA&geo=DE&geo=FR"
    "&sinceTimePeriod=2000-01"
)

EUROSTAT_HICP_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-artifact.v1"
)
EUROSTAT_HICP_INDEX_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-index-exclusion.v1"
)
EUROSTAT_HICP_PUBLICATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-publication.v1"
)
EUROSTAT_HICP_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-observation.v1"
)
EUROSTAT_HICP_DATASET_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-dataset.v1"
)
EUROSTAT_HICP_OVERLAP_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-overlap-difference.v1"
)
EUROSTAT_HICP_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-release-value.v1"
)
EUROSTAT_HICP_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-release.v1"
)
EUROSTAT_HICP_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-hicp-archive-manifest.v1"
)

MAX_EUROSTAT_HICP_INDEX_PAGES: Final = 256
MAX_EUROSTAT_HICP_PUBLICATIONS: Final = 1_024
MAX_EUROSTAT_HICP_OBSERVATIONS: Final = 8_192
MAX_EUROSTAT_HICP_ARTIFACTS: Final = 2_048
MAX_EUROSTAT_HICP_TOTAL_BYTES: Final = 1_024_000_000
MAX_EUROSTAT_HICP_TITLE_CHARS: Final = 512

_MONTH_RE = re.compile(r"\d{4}-\d{2}")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_PERCENT_RE = re.compile(r"(?<![\w.])(-?\d+(?:\.\d+)?)\s*%")
_RELEASE_SLUG_RE = re.compile(
    r"2-(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{4})-"
    r"(?:ap1?|bp|cp)(?:-|$)",
    re.IGNORECASE,
)
_RELEASE_DOCUMENT_RE = re.compile(
    r"/eurostat/documents/\d+/\d+/2-\d{7,8}-(?:AP1?|BP|CP)-EN\."
    r"(?P<format>HTML|PDF)(?:\.(?:html|pdf))?(?:/|$)",
    re.IGNORECASE,
)
_KNOWN_MALFORMED_RELEASE_TOKEN: Final = "2-2802211-AP-EN.PDF.pdf"
_SPACE_RE = re.compile(r"\s+")
_DATASET_IDS: Final = frozenset({"prc_hicp_fp", "prc_hicp_fpd"})
_ECONOMIES: Final = ("EA", "DE", "FR")
_ECONOMY_ORDER: Final = {code: index for index, code in enumerate(_ECONOMIES)}


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    result = _SPACE_RE.sub(" ", value).strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _bounded_int(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be an array")
    return value


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _month(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    parsed = date.fromisoformat(f"{result}-01")
    if parsed.strftime("%Y-%m") != result:
        raise ValueError(f"{name} is not a canonical month")
    return result


def _shift_month(value: str, offset: int) -> str:
    parsed = date.fromisoformat(f"{_month(value, 'month')}-01")
    ordinal = parsed.year * 12 + parsed.month - 1 + offset
    return f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"


def _month_range(start: str, end: str) -> tuple[str, ...]:
    first = _month(start, "start")
    last = _month(end, "end")
    if last < first:
        raise ValueError("month range is reversed")
    values: list[str] = []
    current = first
    while current <= last:
        values.append(current)
        current = _shift_month(current, 1)
    return tuple(values)


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlsplit(result)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username:
        raise ValueError(f"{name} must be an absolute HTTPS URI")
    if parsed.hostname != "ec.europa.eu":
        raise ValueError(f"{name} must use the Eurostat official host")
    return result


def _canonical_uri(value: object, name: str = "source_uri") -> str:
    parsed = urlsplit(_https_uri(value, name))
    return str(parsed._replace(query="", fragment="").geturl())


def _request_uri(value: object, name: str = "source_uri") -> str:
    parsed = urlsplit(_https_uri(value, name))
    return str(parsed._replace(fragment="").geturl())


def _document_source_format(uri: object) -> OfficialSourceFormat | None:
    value = _request_uri(uri)
    match = _RELEASE_DOCUMENT_RE.search(urlsplit(value).path)
    if match is None:
        return None
    return {
        "html": OfficialSourceFormat.HTML,
        "pdf": OfficialSourceFormat.PDF,
    }[match.group("format").casefold()]


def _publication_uri(value: object) -> str:
    uri = _request_uri(value)
    return (
        uri if _document_source_format(uri) is not None else _canonical_uri(uri)
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _slug_release_date(uri: str) -> str:
    match = _RELEASE_SLUG_RE.search(urlsplit(uri).path)
    if match is None:
        raise ValueError("Eurostat HICP release URI omits its source date")
    token = f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
    return _iso_date(token, "release_date")


def _reference_period(
    release_date: str, stage: EurostatHicpReleaseStage
) -> str:
    release = date.fromisoformat(_iso_date(release_date, "release_date"))
    month = release.strftime("%Y-%m")
    if stage is EurostatHicpReleaseStage.FINAL or release.day <= 7:
        return _shift_month(month, -1)
    return month


def _headline_value(title: str) -> tuple[str, float]:
    match = _PERCENT_RE.search(_required_text(title, "title"))
    if match is None:
        raise ValueError("Eurostat HICP title omits its headline rate")
    lexical = match.group(1)
    value = float(lexical)
    if not math.isfinite(value) or not -25.0 <= value <= 100.0:
        raise ValueError("Eurostat HICP headline rate is outside its bound")
    return lexical, value


class EurostatHicpReleaseStage(str, Enum):
    """Source-authored HICP release stage."""

    FINAL = "final"
    FLASH = "flash"

    @classmethod
    def from_value(
        cls, value: str | EurostatHicpReleaseStage
    ) -> EurostatHicpReleaseStage:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported Eurostat HICP release stage") from exc


_STAGE_ORDER: Final = {
    EurostatHicpReleaseStage.FINAL: 0,
    EurostatHicpReleaseStage.FLASH: 1,
}


def _stage_from_title(
    title: str, release_date: str
) -> EurostatHicpReleaseStage:
    normalized = _required_text(title, "title").casefold()
    released = date.fromisoformat(_iso_date(release_date, "release_date"))
    if normalized.startswith("euro area annual inflation rate"):
        return EurostatHicpReleaseStage.FINAL
    if "estimated" in normalized or "expected" in normalized:
        return EurostatHicpReleaseStage.FLASH
    if normalized.startswith("euro area annual inflation"):
        # Eurostat used the same headline form for both stages in 2013-14.
        # The observed cadence is source-identifiable except for the late
        # January 2013 final, whose 28 February release is explicit here.
        if released.year <= 2012:
            return EurostatHicpReleaseStage.FINAL
        if released.year >= 2015:
            return EurostatHicpReleaseStage.FLASH
        if 8 <= released.day <= 24 or released.isoformat() == "2013-02-28":
            return EurostatHicpReleaseStage.FINAL
        return EurostatHicpReleaseStage.FLASH
    if normalized.startswith("annual inflation") or (
        "euro-zone" in normalized and "annual inflation" in normalized
    ):
        return EurostatHicpReleaseStage.FINAL
    raise ValueError("Eurostat HICP title has an unknown release stage")


class EurostatHicpArtifactRole(str, Enum):
    """Role of one retained HICP source artifact."""

    LEGACY_DATASET = "legacy-first-published-dataset"
    CURRENT_DATASET = "current-first-published-dataset"
    PUBLICATION_INDEX = "publication-index"
    RELEASE_LANDING_HTML = "release-landing-html"
    RELEASE_DOCUMENT_HTML = "release-document-html"
    RELEASE_DOCUMENT_PDF = "release-document-pdf"

    @classmethod
    def from_value(
        cls, value: str | EurostatHicpArtifactRole
    ) -> EurostatHicpArtifactRole:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported Eurostat HICP artifact role") from exc


@dataclass(frozen=True, slots=True)
class EurostatHicpArtifactV1:
    """Hash-bound metadata for one caller-retained official artifact."""

    role: EurostatHicpArtifactRole
    source_uri: str
    resolved_uri: str
    source_format: OfficialSourceFormat
    content_length: int
    content_sha256: str
    page_number: int | None = None
    artifact_id: str = ""
    schema_version: str = EUROSTAT_HICP_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP artifact schema")
        role = EurostatHicpArtifactRole.from_value(self.role)
        source_uri = (
            _canonical_uri(self.source_uri)
            if role is EurostatHicpArtifactRole.RELEASE_LANDING_HTML
            else _request_uri(self.source_uri)
        )
        resolved_uri = _https_uri(self.resolved_uri, "resolved_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            EurostatHicpArtifactRole.LEGACY_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatHicpArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatHicpArtifactRole.PUBLICATION_INDEX: OfficialSourceFormat.HTML,
            EurostatHicpArtifactRole.RELEASE_LANDING_HTML: OfficialSourceFormat.HTML,
            EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
            EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
        }[role]
        if source_format is not expected_format:
            raise ValueError("Eurostat HICP artifact format differs from role")
        content_length = _bounded_int(
            self.content_length, "content_length", MAX_EUROSTAT_HICP_TOTAL_BYTES
        )
        if content_length < 1:
            raise ValueError("Eurostat HICP artifact content is empty")
        digest = _required_text(self.content_sha256, "content_sha256")
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("Eurostat HICP artifact hash is invalid")
        if role is EurostatHicpArtifactRole.PUBLICATION_INDEX:
            if self.page_number is None:
                raise ValueError("Eurostat HICP index artifact needs a page")
            _bounded_int(
                self.page_number,
                "page_number",
                MAX_EUROSTAT_HICP_INDEX_PAGES,
            )
            if source_uri != _index_uri(self.page_number):
                raise ValueError("Eurostat HICP index artifact request differs")
        elif self.page_number is not None:
            raise ValueError("only HICP index artifacts have page numbers")
        expected_dataset_uri = {
            EurostatHicpArtifactRole.LEGACY_DATASET: EUROSTAT_HICP_LEGACY_DATASET_URI,
            EurostatHicpArtifactRole.CURRENT_DATASET: EUROSTAT_HICP_CURRENT_DATASET_URI,
        }.get(role)
        if (
            expected_dataset_uri is not None
            and source_uri != expected_dataset_uri
        ):
            raise ValueError("Eurostat HICP dataset artifact request differs")
        if role is EurostatHicpArtifactRole.RELEASE_LANDING_HTML:
            path = urlsplit(source_uri).path
            if (
                "/web/products-euro-indicators/" not in path
                or _RELEASE_SLUG_RE.search(path) is None
            ):
                raise ValueError("Eurostat HICP landing artifact URI differs")
        if role in {
            EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML,
            EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF,
        }:
            match = _RELEASE_DOCUMENT_RE.search(urlsplit(source_uri).path)
            if match is None:
                raise ValueError("Eurostat HICP document artifact URI differs")
            expected_document_role = {
                "html": EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML,
                "pdf": EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF,
            }[match.group("format").casefold()]
            if role is not expected_document_role:
                raise ValueError(
                    "Eurostat HICP document artifact format differs"
                )
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "resolved_uri", resolved_uri)
        object.__setattr__(self, "source_format", source_format)
        expected = _stable_id("eurostat-hicp-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("Eurostat HICP artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "source_uri": self.source_uri,
            "resolved_uri": self.resolved_uri,
            "source_format": self.source_format.value,
            "content_length": self.content_length,
            "content_sha256": self.content_sha256,
            "page_number": self.page_number,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpArtifactV1:
        return cls(
            role=EurostatHicpArtifactRole.from_value(str(data.get("role", ""))),
            source_uri=str(data.get("source_uri", "")),
            resolved_uri=str(data.get("resolved_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_length=cast(int, data.get("content_length")),
            content_sha256=str(data.get("content_sha256", "")),
            page_number=cast(int | None, data.get("page_number")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpIndexExclusionV1:
    """One source-index card explicitly excluded from HICP scope."""

    source_title: str
    source_uri: str
    card_date: str
    index_page_number: int
    reason: str
    exclusion_id: str = ""
    schema_version: str = EUROSTAT_HICP_INDEX_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_INDEX_EXCLUSION_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP index exclusion schema")
        title = _required_text(self.source_title, "source_title")
        uri = _request_uri(self.source_uri)
        card_date = _iso_date(self.card_date, "card_date")
        page = _bounded_int(
            self.index_page_number,
            "index_page_number",
            MAX_EUROSTAT_HICP_INDEX_PAGES,
        )
        reason = _required_text(self.reason, "reason")
        if (
            reason != "non-euro-area-scope"
            or not title.casefold().startswith("g20 annual inflation")
            or page < 1
        ):
            raise ValueError("Eurostat HICP index exclusion differs")
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "card_date", card_date)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id(
            "eurostat-hicp-index-exclusion", self.identity_payload()
        )
        if self.exclusion_id and self.exclusion_id != expected:
            raise ValueError("Eurostat HICP index exclusion identity differs")
        object.__setattr__(self, "exclusion_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_title": self.source_title,
            "source_uri": self.source_uri,
            "card_date": self.card_date,
            "index_page_number": self.index_page_number,
            "reason": self.reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "exclusion_id": self.exclusion_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpIndexExclusionV1:
        return cls(
            source_title=str(data.get("source_title", "")),
            source_uri=str(data.get("source_uri", "")),
            card_date=str(data.get("card_date", "")),
            index_page_number=cast(int, data.get("index_page_number")),
            reason=str(data.get("reason", "")),
            exclusion_id=str(data.get("exclusion_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpPublicationV1:
    """One official HICP flash or final publication occurrence."""

    release_date: str
    release_date_from_uri: bool
    reference_period: str
    stage: EurostatHicpReleaseStage
    source_title: str
    source_uri: str
    headline_value_lexical: str
    headline_value: float
    page_release_date: str | None
    page_release_date_offset_days: int | None
    card_date: str | None
    card_date_offset_days: int | None
    index_page_number: int | None
    legacy_unindexed: bool
    publication_id: str = ""
    schema_version: str = EUROSTAT_HICP_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP publication schema")
        release_date = _iso_date(self.release_date, "release_date")
        stage = EurostatHicpReleaseStage.from_value(self.stage)
        reference = _month(self.reference_period, "reference_period")
        title = _required_text(self.source_title, "source_title")
        if len(title) > MAX_EUROSTAT_HICP_TITLE_CHARS:
            raise ValueError("Eurostat HICP title exceeds its bound")
        source_uri = _publication_uri(self.source_uri)
        if not isinstance(self.release_date_from_uri, bool):
            raise TypeError("release_date_from_uri must be a boolean")
        if self.release_date_from_uri:
            if _slug_release_date(source_uri) != release_date:
                raise ValueError("Eurostat HICP URI and release date differ")
        elif (
            _KNOWN_MALFORMED_RELEASE_TOKEN not in source_uri
            or release_date != "2011-02-28"
        ):
            raise ValueError("Eurostat HICP source-date correction differs")
        if _stage_from_title(title, release_date) is not stage:
            raise ValueError("Eurostat HICP title and stage differ")
        if _reference_period(release_date, stage) != reference:
            raise ValueError("Eurostat HICP reference-period inference differs")
        lexical = _required_text(
            self.headline_value_lexical, "headline_value_lexical"
        )
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError("Eurostat HICP headline lexical is invalid")
        parsed_value = float(lexical)
        if parsed_value != self.headline_value or not math.isfinite(
            parsed_value
        ):
            raise ValueError("Eurostat HICP headline value differs")
        expected_lexical, expected_value = _headline_value(title)
        if lexical != expected_lexical or parsed_value != expected_value:
            raise ValueError("Eurostat HICP title and headline value differ")
        page_release_date = (
            _iso_date(self.page_release_date, "page_release_date")
            if self.page_release_date is not None
            else None
        )
        if (page_release_date is None) != (
            self.page_release_date_offset_days is None
        ):
            raise ValueError("Eurostat HICP page-date metadata is incomplete")
        if page_release_date is not None:
            page_offset = (
                date.fromisoformat(page_release_date)
                - date.fromisoformat(release_date)
            ).days
            if page_offset not in {-1, 0}:
                raise ValueError(
                    "Eurostat HICP page-date offset is unsupported"
                )
            if page_offset != self.page_release_date_offset_days:
                raise ValueError("Eurostat HICP page-date offset differs")
        card_date = (
            _iso_date(self.card_date, "card_date")
            if self.card_date is not None
            else None
        )
        expected_legacy = release_date < EUROSTAT_HICP_MIGRATED_INDEX_START_DATE
        if not isinstance(self.legacy_unindexed, bool):
            raise TypeError("legacy_unindexed must be a boolean")
        if self.legacy_unindexed != expected_legacy:
            raise ValueError("Eurostat HICP migrated-index provenance differs")
        if expected_legacy:
            if card_date is not None or self.card_date_offset_days is not None:
                raise ValueError(
                    "unindexed Eurostat HICP release has card metadata"
                )
            if self.index_page_number is not None:
                raise ValueError(
                    "unindexed Eurostat HICP release has an index page"
                )
        else:
            if card_date is None or self.card_date_offset_days is None:
                raise ValueError(
                    "indexed Eurostat HICP release needs card metadata"
                )
            offset = (
                date.fromisoformat(card_date) - date.fromisoformat(release_date)
            ).days
            if offset not in {-1, 0} or self.card_date_offset_days != offset:
                raise ValueError("Eurostat HICP card-date offset differs")
            if self.index_page_number is None:
                raise ValueError("indexed Eurostat HICP release needs a page")
            _bounded_int(
                self.index_page_number,
                "index_page_number",
                MAX_EUROSTAT_HICP_INDEX_PAGES,
            )
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "page_release_date", page_release_date)
        object.__setattr__(self, "card_date", card_date)
        expected = _stable_id(
            "eurostat-hicp-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("Eurostat HICP publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def occurrence_key(self) -> tuple[str, str]:
        return self.stage.value, self.reference_period

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_date": self.release_date,
            "release_date_from_uri": self.release_date_from_uri,
            "reference_period": self.reference_period,
            "stage": self.stage.value,
            "source_title": self.source_title,
            "source_uri": self.source_uri,
            "headline_value_lexical": self.headline_value_lexical,
            "headline_value": self.headline_value,
            "page_release_date": self.page_release_date,
            "page_release_date_offset_days": self.page_release_date_offset_days,
            "card_date": self.card_date,
            "card_date_offset_days": self.card_date_offset_days,
            "index_page_number": self.index_page_number,
            "legacy_unindexed": self.legacy_unindexed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpPublicationV1:
        return cls(
            release_date=str(data.get("release_date", "")),
            release_date_from_uri=cast(bool, data.get("release_date_from_uri")),
            reference_period=str(data.get("reference_period", "")),
            stage=EurostatHicpReleaseStage.from_value(
                str(data.get("stage", ""))
            ),
            source_title=str(data.get("source_title", "")),
            source_uri=str(data.get("source_uri", "")),
            headline_value_lexical=str(data.get("headline_value_lexical", "")),
            headline_value=float(cast(float, data.get("headline_value"))),
            page_release_date=_optional_text(
                data.get("page_release_date"), "page_release_date"
            ),
            page_release_date_offset_days=cast(
                int | None, data.get("page_release_date_offset_days")
            ),
            card_date=_optional_text(data.get("card_date"), "card_date"),
            card_date_offset_days=cast(
                int | None, data.get("card_date_offset_days")
            ),
            index_page_number=cast(int | None, data.get("index_page_number")),
            legacy_unindexed=cast(bool, data.get("legacy_unindexed")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpObservationV1:
    """One sparse JSON-stat first-published HICP observation."""

    dataset_id: str
    taxonomy: str
    stage: EurostatHicpReleaseStage
    economy_code: str
    reference_period: str
    value_lexical: str
    value: float
    status: str | None
    flat_index: int
    observation_id: str = ""
    schema_version: str = EUROSTAT_HICP_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP observation schema")
        dataset_id = _required_text(self.dataset_id, "dataset_id")
        if dataset_id not in _DATASET_IDS:
            raise ValueError("unsupported Eurostat HICP dataset")
        taxonomy = _required_text(self.taxonomy, "taxonomy")
        expected_taxonomy = {
            "prc_hicp_fp": "ecoicop-v1",
            "prc_hicp_fpd": "ecoicop-v2",
        }[dataset_id]
        if taxonomy != expected_taxonomy:
            raise ValueError("Eurostat HICP taxonomy differs from dataset")
        stage = EurostatHicpReleaseStage.from_value(self.stage)
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError(
                "Eurostat HICP economy is outside the qualified scope"
            )
        reference = _month(self.reference_period, "reference_period")
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError("Eurostat HICP observation lexical is invalid")
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError("Eurostat HICP observation numeric differs")
        if not -25.0 <= numeric <= 100.0:
            raise ValueError("Eurostat HICP observation is outside its bound")
        status = _optional_text(self.status, "status")
        if status not in {None, "e"}:
            raise ValueError("Eurostat HICP observation status is unsupported")
        flat_index = _bounded_int(
            self.flat_index, "flat_index", MAX_EUROSTAT_HICP_OBSERVATIONS * 8
        )
        object.__setattr__(self, "dataset_id", dataset_id)
        object.__setattr__(self, "taxonomy", taxonomy)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "flat_index", flat_index)
        expected = _stable_id(
            "eurostat-hicp-observation", self.identity_payload()
        )
        if self.observation_id and self.observation_id != expected:
            raise ValueError("Eurostat HICP observation identity differs")
        object.__setattr__(self, "observation_id", expected)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.stage.value, self.economy_code, self.reference_period

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "taxonomy": self.taxonomy,
            "stage": self.stage.value,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "value_lexical": self.value_lexical,
            "value": self.value,
            "status": self.status,
            "flat_index": self.flat_index,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpObservationV1:
        return cls(
            dataset_id=str(data.get("dataset_id", "")),
            taxonomy=str(data.get("taxonomy", "")),
            stage=EurostatHicpReleaseStage.from_value(
                str(data.get("stage", ""))
            ),
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            status=_optional_text(data.get("status"), "status"),
            flat_index=cast(int, data.get("flat_index")),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpDatasetV1:
    """One exact Eurostat JSON-stat dataset response and its observations."""

    dataset_id: str
    taxonomy: str
    label: str
    updated_at: str
    time_start: str
    time_end: str
    artifact: EurostatHicpArtifactV1
    observations: tuple[EurostatHicpObservationV1, ...]
    dataset_receipt_id: str = ""
    schema_version: str = EUROSTAT_HICP_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_DATASET_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP dataset schema")
        dataset_id = _required_text(self.dataset_id, "dataset_id")
        if dataset_id not in _DATASET_IDS:
            raise ValueError("unsupported Eurostat HICP dataset")
        taxonomy = _required_text(self.taxonomy, "taxonomy")
        expected_taxonomy = {
            "prc_hicp_fp": "ecoicop-v1",
            "prc_hicp_fpd": "ecoicop-v2",
        }[dataset_id]
        if taxonomy != expected_taxonomy:
            raise ValueError("Eurostat HICP dataset taxonomy differs")
        label = _required_text(self.label, "label")
        updated = _required_text(self.updated_at, "updated_at")
        try:
            datetime.fromisoformat(updated)
        except ValueError as exc:
            raise ValueError(
                "Eurostat HICP dataset update time is invalid"
            ) from exc
        start = _month(self.time_start, "time_start")
        end = _month(self.time_end, "time_end")
        if end < start:
            raise ValueError("Eurostat HICP dataset time range is reversed")
        expected_role = {
            "prc_hicp_fp": EurostatHicpArtifactRole.LEGACY_DATASET,
            "prc_hicp_fpd": EurostatHicpArtifactRole.CURRENT_DATASET,
        }[dataset_id]
        if self.artifact.role is not expected_role:
            raise ValueError("Eurostat HICP dataset artifact role differs")
        observations = tuple(self.observations)
        if not 1 <= len(observations) <= MAX_EUROSTAT_HICP_OBSERVATIONS:
            raise ValueError("Eurostat HICP observation count is invalid")
        if any(
            item.dataset_id != dataset_id or item.taxonomy != taxonomy
            for item in observations
        ):
            raise ValueError("Eurostat HICP dataset observations differ")
        keys = [item.key for item in observations]
        if len(keys) != len(set(keys)):
            raise ValueError("Eurostat HICP dataset repeats an observation")
        if list(observations) != sorted(
            observations,
            key=lambda item: (
                _STAGE_ORDER[item.stage],
                _ECONOMY_ORDER[item.economy_code],
                item.reference_period,
            ),
        ):
            raise ValueError("Eurostat HICP observations are not canonical")
        if (
            min(item.reference_period for item in observations) != start
            or max(item.reference_period for item in observations) != end
        ):
            raise ValueError("Eurostat HICP dataset time range differs")
        object.__setattr__(self, "dataset_id", dataset_id)
        object.__setattr__(self, "taxonomy", taxonomy)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "time_start", start)
        object.__setattr__(self, "time_end", end)
        object.__setattr__(self, "observations", observations)
        expected = _stable_id("eurostat-hicp-dataset", self.identity_payload())
        if self.dataset_receipt_id and self.dataset_receipt_id != expected:
            raise ValueError("Eurostat HICP dataset identity differs")
        object.__setattr__(self, "dataset_receipt_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "taxonomy": self.taxonomy,
            "label": self.label,
            "updated_at": self.updated_at,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "artifact": self.artifact.to_dict(),
            "observations": [item.to_dict() for item in self.observations],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "dataset_receipt_id": self.dataset_receipt_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpDatasetV1:
        return cls(
            dataset_id=str(data.get("dataset_id", "")),
            taxonomy=str(data.get("taxonomy", "")),
            label=str(data.get("label", "")),
            updated_at=str(data.get("updated_at", "")),
            time_start=str(data.get("time_start", "")),
            time_end=str(data.get("time_end", "")),
            artifact=EurostatHicpArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                EurostatHicpObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            dataset_receipt_id=str(data.get("dataset_receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpOverlapDifferenceV1:
    """One explicit legacy/current first-published-table difference."""

    stage: EurostatHicpReleaseStage
    economy_code: str
    reference_period: str
    legacy_value_lexical: str
    legacy_value: float
    legacy_status: str | None
    current_value_lexical: str
    current_value: float
    current_status: str | None
    numeric_difference: bool
    status_difference: bool
    difference_id: str = ""
    schema_version: str = EUROSTAT_HICP_OVERLAP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_OVERLAP_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP overlap schema")
        stage = EurostatHicpReleaseStage.from_value(self.stage)
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError("Eurostat HICP overlap economy differs")
        reference = _month(self.reference_period, "reference_period")
        legacy_lexical = _required_text(
            self.legacy_value_lexical, "legacy_value_lexical"
        )
        current_lexical = _required_text(
            self.current_value_lexical, "current_value_lexical"
        )
        if (
            _NUMBER_RE.fullmatch(legacy_lexical) is None
            or _NUMBER_RE.fullmatch(current_lexical) is None
        ):
            raise ValueError("Eurostat HICP overlap lexical is invalid")
        legacy_value = float(legacy_lexical)
        current_value = float(current_lexical)
        if (
            legacy_value != self.legacy_value
            or current_value != self.current_value
        ):
            raise ValueError("Eurostat HICP overlap numeric differs")
        legacy_status = _optional_text(self.legacy_status, "legacy_status")
        current_status = _optional_text(self.current_status, "current_status")
        if legacy_status not in {None, "e"} or current_status not in {
            None,
            "e",
        }:
            raise ValueError("Eurostat HICP overlap status is unsupported")
        numeric_difference = legacy_value != current_value
        status_difference = legacy_status != current_status
        if not isinstance(self.numeric_difference, bool) or not isinstance(
            self.status_difference, bool
        ):
            raise TypeError("Eurostat HICP overlap flags must be booleans")
        if (
            self.numeric_difference != numeric_difference
            or self.status_difference != status_difference
            or not (numeric_difference or status_difference)
        ):
            raise ValueError("Eurostat HICP overlap difference flags differ")
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "legacy_status", legacy_status)
        object.__setattr__(self, "current_status", current_status)
        expected = _stable_id("eurostat-hicp-overlap", self.identity_payload())
        if self.difference_id and self.difference_id != expected:
            raise ValueError("Eurostat HICP overlap identity differs")
        object.__setattr__(self, "difference_id", expected)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.stage.value, self.economy_code, self.reference_period

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "stage": self.stage.value,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "legacy_value_lexical": self.legacy_value_lexical,
            "legacy_value": self.legacy_value,
            "legacy_status": self.legacy_status,
            "current_value_lexical": self.current_value_lexical,
            "current_value": self.current_value,
            "current_status": self.current_status,
            "numeric_difference": self.numeric_difference,
            "status_difference": self.status_difference,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "difference_id": self.difference_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatHicpOverlapDifferenceV1:
        return cls(
            stage=EurostatHicpReleaseStage.from_value(
                str(data.get("stage", ""))
            ),
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            legacy_value_lexical=str(data.get("legacy_value_lexical", "")),
            legacy_value=float(cast(float, data.get("legacy_value"))),
            legacy_status=_optional_text(
                data.get("legacy_status"), "legacy_status"
            ),
            current_value_lexical=str(data.get("current_value_lexical", "")),
            current_value=float(cast(float, data.get("current_value"))),
            current_status=_optional_text(
                data.get("current_status"), "current_status"
            ),
            numeric_difference=cast(bool, data.get("numeric_difference")),
            status_difference=cast(bool, data.get("status_difference")),
            difference_id=str(data.get("difference_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


class EurostatHicpValueProvenance(str, Enum):
    """Official evidence surface used for a release-stage value."""

    RELEASE_HEADLINE = "release-headline"
    LEGACY_DATASET = "legacy-first-published-dataset"
    CURRENT_DATASET = "current-first-published-dataset"

    @classmethod
    def from_value(
        cls, value: str | EurostatHicpValueProvenance
    ) -> EurostatHicpValueProvenance:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat HICP value provenance"
            ) from exc


@dataclass(frozen=True, slots=True)
class EurostatHicpReleaseValueV1:
    """One economy-specific value known at one HICP release stage."""

    stage: EurostatHicpReleaseStage
    economy_code: str
    reference_period: str
    value_lexical: str
    value: float
    status: str | None
    provenance: EurostatHicpValueProvenance
    evidence_id: str
    release_value_id: str = ""
    schema_version: str = EUROSTAT_HICP_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP release-value schema")
        stage = EurostatHicpReleaseStage.from_value(self.stage)
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError("Eurostat HICP release-value economy differs")
        reference = _month(self.reference_period, "reference_period")
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError("Eurostat HICP release-value lexical is invalid")
        numeric = float(lexical)
        if numeric != self.value or not -25.0 <= numeric <= 100.0:
            raise ValueError("Eurostat HICP release value is invalid")
        status = _optional_text(self.status, "status")
        if status not in {None, "e"}:
            raise ValueError(
                "Eurostat HICP release-value status is unsupported"
            )
        provenance = EurostatHicpValueProvenance.from_value(self.provenance)
        evidence_id = _required_text(self.evidence_id, "evidence_id")
        expected_prefix = {
            EurostatHicpValueProvenance.RELEASE_HEADLINE: (
                "eurostat-hicp-publication:sha256:"
            ),
            EurostatHicpValueProvenance.LEGACY_DATASET: (
                "eurostat-hicp-observation:sha256:"
            ),
            EurostatHicpValueProvenance.CURRENT_DATASET: (
                "eurostat-hicp-observation:sha256:"
            ),
        }[provenance]
        if not evidence_id.startswith(expected_prefix):
            raise ValueError("Eurostat HICP release-value evidence differs")
        if provenance is EurostatHicpValueProvenance.RELEASE_HEADLINE and (
            economy != "EA" or status is not None
        ):
            raise ValueError("HICP release headline only qualifies EA")
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "provenance", provenance)
        object.__setattr__(self, "evidence_id", evidence_id)
        expected = _stable_id(
            "eurostat-hicp-release-value", self.identity_payload()
        )
        if self.release_value_id and self.release_value_id != expected:
            raise ValueError("Eurostat HICP release-value identity differs")
        object.__setattr__(self, "release_value_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "stage": self.stage.value,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "value_lexical": self.value_lexical,
            "value": self.value,
            "status": self.status,
            "provenance": self.provenance.value,
            "evidence_id": self.evidence_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "release_value_id": self.release_value_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpReleaseValueV1:
        return cls(
            stage=EurostatHicpReleaseStage.from_value(
                str(data.get("stage", ""))
            ),
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            status=_optional_text(data.get("status"), "status"),
            provenance=EurostatHicpValueProvenance.from_value(
                str(data.get("provenance", ""))
            ),
            evidence_id=str(data.get("evidence_id", "")),
            release_value_id=str(data.get("release_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpReleaseV1:
    """One date-only HICP release with independently scoped values."""

    publication: EurostatHicpPublicationV1
    artifacts: tuple[EurostatHicpArtifactV1, ...]
    dataset_ea_value: EurostatHicpReleaseValueV1 | None
    headline_dataset_numeric_difference: bool | None
    values: tuple[EurostatHicpReleaseValueV1, ...]
    release_id: str = ""
    schema_version: str = EUROSTAT_HICP_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP release schema")
        publication = self.publication
        artifacts = tuple(self.artifacts)
        if not 1 <= len(artifacts) <= 8:
            raise ValueError("Eurostat HICP release artifact count is invalid")
        document_roles = {
            EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML,
            EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF,
        }
        primary = artifacts[0]
        if primary.role is EurostatHicpArtifactRole.RELEASE_LANDING_HTML:
            if primary.source_uri != publication.source_uri:
                raise ValueError(
                    "Eurostat HICP release landing artifact differs"
                )
            documents = artifacts[1:]
        elif primary.role in document_roles:
            if primary.source_uri != publication.source_uri:
                raise ValueError(
                    "Eurostat HICP direct document artifact differs"
                )
            documents = artifacts
        else:
            raise ValueError("Eurostat HICP primary release artifact differs")
        if any(item.role not in document_roles for item in documents):
            raise ValueError("Eurostat HICP release document artifact differs")
        artifact_uris = [item.source_uri for item in artifacts]
        if len(artifact_uris) != len(set(artifact_uris)):
            raise ValueError("Eurostat HICP release artifacts repeat a URI")
        for item in documents:
            if publication.release_date_from_uri:
                if (
                    _slug_release_date(item.source_uri)
                    != publication.release_date
                ):
                    raise ValueError(
                        "Eurostat HICP document and release dates differ"
                    )
            elif _KNOWN_MALFORMED_RELEASE_TOKEN not in item.source_uri:
                raise ValueError("Eurostat HICP corrected document URI differs")
        if list(documents) != sorted(
            documents, key=lambda item: (item.role.value, item.source_uri)
        ):
            raise ValueError(
                "Eurostat HICP document artifacts are not canonical"
            )
        if (
            publication.page_release_date is None
            and publication.card_date is None
        ):
            raise ValueError("Eurostat HICP release lacks source-date evidence")
        dataset_ea = self.dataset_ea_value
        if dataset_ea is None:
            if self.headline_dataset_numeric_difference is not None:
                raise ValueError(
                    "HICP absent dataset EA value has a comparison flag"
                )
        else:
            if (
                dataset_ea.economy_code != "EA"
                or dataset_ea.stage is not publication.stage
                or dataset_ea.reference_period != publication.reference_period
                or dataset_ea.provenance
                is EurostatHicpValueProvenance.RELEASE_HEADLINE
            ):
                raise ValueError("Eurostat HICP dataset EA comparison differs")
            if not isinstance(self.headline_dataset_numeric_difference, bool):
                raise TypeError(
                    "HICP headline comparison flag must be a boolean"
                )
            expected_difference = dataset_ea.value != publication.headline_value
            if self.headline_dataset_numeric_difference != expected_difference:
                raise ValueError(
                    "Eurostat HICP headline comparison flag differs"
                )
        values = tuple(self.values)
        if not 1 <= len(values) <= len(_ECONOMIES):
            raise ValueError("Eurostat HICP release value count is invalid")
        economies = [item.economy_code for item in values]
        if len(economies) != len(set(economies)) or economies[0] != "EA":
            raise ValueError("Eurostat HICP release scopes are invalid")
        if economies != [item for item in _ECONOMIES if item in economies]:
            raise ValueError("Eurostat HICP release scopes are not canonical")
        if any(
            item.stage is not publication.stage
            or item.reference_period != publication.reference_period
            for item in values
        ):
            raise ValueError("Eurostat HICP release value occurrence differs")
        ea = values[0]
        if (
            ea.value != publication.headline_value
            or ea.value_lexical != publication.headline_value_lexical
        ):
            raise ValueError("Eurostat HICP EA value and headline differ")
        if (
            ea.provenance is not EurostatHicpValueProvenance.RELEASE_HEADLINE
            or ea.evidence_id != publication.publication_id
        ):
            raise ValueError("Eurostat HICP EA headline provenance differs")
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "values", values)
        expected = _stable_id("eurostat-hicp-release", self.identity_payload())
        if self.release_id and self.release_id != expected:
            raise ValueError("Eurostat HICP release identity differs")
        object.__setattr__(self, "release_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "publication": self.publication.to_dict(),
            "artifacts": [item.to_dict() for item in self.artifacts],
            "dataset_ea_value": (
                self.dataset_ea_value.to_dict()
                if self.dataset_ea_value is not None
                else None
            ),
            "headline_dataset_numeric_difference": (
                self.headline_dataset_numeric_difference
            ),
            "values": [item.to_dict() for item in self.values],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatHicpReleaseV1:
        return cls(
            publication=EurostatHicpPublicationV1.from_dict(
                _mapping(data.get("publication"), "publication")
            ),
            artifacts=tuple(
                EurostatHicpArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            dataset_ea_value=(
                EurostatHicpReleaseValueV1.from_dict(
                    _mapping(data.get("dataset_ea_value"), "dataset_ea_value")
                )
                if data.get("dataset_ea_value") is not None
                else None
            ),
            headline_dataset_numeric_difference=cast(
                bool | None, data.get("headline_dataset_numeric_difference")
            ),
            values=tuple(
                EurostatHicpReleaseValueV1.from_dict(_mapping(item, "value"))
                for item in _sequence(data.get("values"), "values")
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatHicpArchiveManifestV1:
    """Compact receipt for the retained HICP publication and vintage corpus."""

    as_of_date: str
    registry_id: str
    source_id: str
    legacy_dataset: EurostatHicpDatasetV1
    current_dataset: EurostatHicpDatasetV1
    index_artifacts: tuple[EurostatHicpArtifactV1, ...]
    index_exclusions: tuple[EurostatHicpIndexExclusionV1, ...]
    releases: tuple[EurostatHicpReleaseV1, ...]
    overlap_count: int
    overlap_differences: tuple[EurostatHicpOverlapDifferenceV1, ...]
    final_artifact_gap_reference_periods: tuple[str, ...]
    flash_artifact_gap_reference_periods: tuple[str, ...]
    migrated_publication_count: int
    legacy_unindexed_count: int
    final_release_count: int
    flash_release_count: int
    page_date_offset_count: int
    card_date_offset_count: int
    headline_only_release_count: int
    headline_dataset_comparison_count: int
    headline_dataset_difference_count: int
    numeric_overlap_difference_count: int
    status_overlap_difference_count: int
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    manifest_id: str = ""
    schema_version: str = EUROSTAT_HICP_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_HICP_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat HICP manifest schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("Eurostat HICP registry identity is invalid")
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError("Eurostat HICP source identity is invalid")
        if self.legacy_dataset.dataset_id != "prc_hicp_fp":
            raise ValueError("Eurostat HICP legacy dataset differs")
        if self.current_dataset.dataset_id != "prc_hicp_fpd":
            raise ValueError("Eurostat HICP current dataset differs")
        index_artifacts = tuple(self.index_artifacts)
        if not 1 <= len(index_artifacts) <= MAX_EUROSTAT_HICP_INDEX_PAGES:
            raise ValueError("Eurostat HICP index page count is invalid")
        if any(
            item.role is not EurostatHicpArtifactRole.PUBLICATION_INDEX
            for item in index_artifacts
        ):
            raise ValueError("Eurostat HICP index artifact role differs")
        pages = [cast(int, item.page_number) for item in index_artifacts]
        if pages != list(range(1, len(index_artifacts) + 1)):
            raise ValueError("Eurostat HICP index pages are not contiguous")
        exclusions = tuple(self.index_exclusions)
        if list(exclusions) != sorted(
            exclusions,
            key=lambda item: (
                item.index_page_number,
                item.card_date,
                item.source_uri,
            ),
        ):
            raise ValueError("Eurostat HICP index exclusions are not canonical")
        if len({item.source_uri for item in exclusions}) != len(exclusions):
            raise ValueError("Eurostat HICP index exclusions repeat a URI")
        releases = tuple(self.releases)
        if not 1 <= len(releases) <= MAX_EUROSTAT_HICP_PUBLICATIONS:
            raise ValueError("Eurostat HICP release count is invalid")
        if list(releases) != sorted(
            releases,
            key=lambda item: (
                item.publication.release_date,
                item.publication.stage.value,
            ),
        ):
            raise ValueError("Eurostat HICP releases are not canonical")
        occurrence_keys = [item.publication.occurrence_key for item in releases]
        if len(occurrence_keys) != len(set(occurrence_keys)):
            raise ValueError("Eurostat HICP releases repeat an occurrence")
        if any(item.publication.release_date > as_of for item in releases):
            raise ValueError("Eurostat HICP release exceeds archive boundary")
        differences = tuple(self.overlap_differences)
        if list(differences) != sorted(differences, key=lambda item: item.key):
            raise ValueError(
                "Eurostat HICP overlap differences are not canonical"
            )
        if len({item.key for item in differences}) != len(differences):
            raise ValueError("Eurostat HICP overlap audit repeats a key")
        overlap, expected_differences = _overlap_differences(
            self.legacy_dataset, self.current_dataset
        )
        if differences != expected_differences:
            raise ValueError(
                "Eurostat HICP overlap audit differs from datasets"
            )
        final_gaps = tuple(
            _month(item, "final_artifact_gap_reference_periods")
            for item in self.final_artifact_gap_reference_periods
        )
        flash_gaps = tuple(
            _month(item, "flash_artifact_gap_reference_periods")
            for item in self.flash_artifact_gap_reference_periods
        )
        if final_gaps != tuple(sorted(set(final_gaps))):
            raise ValueError(
                "Eurostat HICP final artifact gaps are not canonical"
            )
        if flash_gaps != tuple(sorted(set(flash_gaps))):
            raise ValueError(
                "Eurostat HICP flash artifact gaps are not canonical"
            )
        final_observations = tuple(
            item
            for item in self.current_dataset.observations
            if item.stage is EurostatHicpReleaseStage.FINAL
            and item.economy_code == "EA"
        )
        flash_observations = tuple(
            item
            for dataset in (self.legacy_dataset, self.current_dataset)
            for item in dataset.observations
            if item.stage is EurostatHicpReleaseStage.FLASH
            and item.economy_code == "EA"
        )
        if not final_observations or not flash_observations:
            raise ValueError(
                "Eurostat HICP expected occurrence ranges are unavailable"
            )
        final_expected = set(
            _month_range(
                EUROSTAT_HICP_FINAL_START_REFERENCE_PERIOD,
                max(item.reference_period for item in final_observations),
            )
        )
        flash_expected = set(
            _month_range(
                EUROSTAT_HICP_FIRST_FLASH_REFERENCE_PERIOD,
                max(item.reference_period for item in flash_observations),
            )
        )
        final_published = {
            item.publication.reference_period
            for item in releases
            if item.publication.stage is EurostatHicpReleaseStage.FINAL
        }
        flash_published = {
            item.publication.reference_period
            for item in releases
            if item.publication.stage is EurostatHicpReleaseStage.FLASH
        }
        if final_published - final_expected or flash_published - flash_expected:
            raise ValueError(
                "Eurostat HICP publication falls outside expected range"
            )
        if set(final_gaps) != final_expected - final_published:
            raise ValueError(
                "Eurostat HICP final artifact gap inventory differs"
            )
        if set(flash_gaps) != flash_expected - flash_published:
            raise ValueError(
                "Eurostat HICP flash artifact gap inventory differs"
            )
        numeric_differences = sum(
            item.numeric_difference for item in differences
        )
        status_differences = sum(item.status_difference for item in differences)
        migrated = sum(
            not item.publication.legacy_unindexed for item in releases
        )
        unindexed = len(releases) - migrated
        final_count = sum(
            item.publication.stage is EurostatHicpReleaseStage.FINAL
            for item in releases
        )
        flash_count = len(releases) - final_count
        card_offsets = sum(
            item.publication.card_date_offset_days == -1 for item in releases
        )
        page_offsets = sum(
            item.publication.page_release_date_offset_days == -1
            for item in releases
        )
        headline_only = sum(len(item.values) == 1 for item in releases)
        headline_comparisons = sum(
            item.dataset_ea_value is not None for item in releases
        )
        headline_differences = sum(
            item.headline_dataset_numeric_difference is True
            for item in releases
        )
        release_artifacts = [
            artifact for item in releases for artifact in item.artifacts
        ]
        all_artifacts = [
            self.legacy_dataset.artifact,
            self.current_dataset.artifact,
            *index_artifacts,
            *release_artifacts,
        ]
        if len(all_artifacts) > MAX_EUROSTAT_HICP_ARTIFACTS:
            raise ValueError("Eurostat HICP artifact count exceeds its bound")
        unique_hashes = len({item.content_sha256 for item in all_artifacts})
        total_bytes = sum(item.content_length for item in all_artifacts)
        expected_counts = (
            overlap,
            migrated,
            unindexed,
            final_count,
            flash_count,
            page_offsets,
            card_offsets,
            headline_only,
            headline_comparisons,
            headline_differences,
            numeric_differences,
            status_differences,
            len(all_artifacts),
            unique_hashes,
            total_bytes,
        )
        supplied_counts = tuple(
            _bounded_int(value, name, maximum)
            for value, name, maximum in (
                (
                    self.overlap_count,
                    "overlap_count",
                    MAX_EUROSTAT_HICP_OBSERVATIONS,
                ),
                (
                    self.migrated_publication_count,
                    "migrated_publication_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.legacy_unindexed_count,
                    "legacy_unindexed_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.final_release_count,
                    "final_release_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.flash_release_count,
                    "flash_release_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.page_date_offset_count,
                    "page_date_offset_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.card_date_offset_count,
                    "card_date_offset_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.headline_only_release_count,
                    "headline_only_release_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.headline_dataset_comparison_count,
                    "headline_dataset_comparison_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.headline_dataset_difference_count,
                    "headline_dataset_difference_count",
                    MAX_EUROSTAT_HICP_PUBLICATIONS,
                ),
                (
                    self.numeric_overlap_difference_count,
                    "numeric_overlap_difference_count",
                    MAX_EUROSTAT_HICP_OBSERVATIONS,
                ),
                (
                    self.status_overlap_difference_count,
                    "status_overlap_difference_count",
                    MAX_EUROSTAT_HICP_OBSERVATIONS,
                ),
                (
                    self.raw_artifact_count,
                    "raw_artifact_count",
                    MAX_EUROSTAT_HICP_ARTIFACTS,
                ),
                (
                    self.unique_content_sha256_count,
                    "unique_content_sha256_count",
                    MAX_EUROSTAT_HICP_ARTIFACTS,
                ),
                (
                    self.total_content_bytes,
                    "total_content_bytes",
                    MAX_EUROSTAT_HICP_TOTAL_BYTES,
                ),
            )
        )
        if supplied_counts != expected_counts:
            raise ValueError("Eurostat HICP manifest summary counts differ")
        if total_bytes > MAX_EUROSTAT_HICP_TOTAL_BYTES:
            raise ValueError("Eurostat HICP corpus exceeds its byte bound")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "index_artifacts", index_artifacts)
        object.__setattr__(self, "index_exclusions", exclusions)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(self, "overlap_differences", differences)
        object.__setattr__(
            self, "final_artifact_gap_reference_periods", final_gaps
        )
        object.__setattr__(
            self, "flash_artifact_gap_reference_periods", flash_gaps
        )
        expected = _stable_id(
            "eurostat-hicp-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Eurostat HICP manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def publications(self) -> tuple[EurostatHicpPublicationV1, ...]:
        return tuple(item.publication for item in self.releases)

    @property
    def release_count(self) -> int:
        return len(self.releases)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "legacy_dataset": self.legacy_dataset.to_dict(),
            "current_dataset": self.current_dataset.to_dict(),
            "index_artifacts": [
                item.to_dict() for item in self.index_artifacts
            ],
            "index_exclusions": [
                item.to_dict() for item in self.index_exclusions
            ],
            "releases": [item.to_dict() for item in self.releases],
            "overlap_count": self.overlap_count,
            "overlap_differences": [
                item.to_dict() for item in self.overlap_differences
            ],
            "final_artifact_gap_reference_periods": list(
                self.final_artifact_gap_reference_periods
            ),
            "flash_artifact_gap_reference_periods": list(
                self.flash_artifact_gap_reference_periods
            ),
            "migrated_publication_count": self.migrated_publication_count,
            "legacy_unindexed_count": self.legacy_unindexed_count,
            "final_release_count": self.final_release_count,
            "flash_release_count": self.flash_release_count,
            "page_date_offset_count": self.page_date_offset_count,
            "card_date_offset_count": self.card_date_offset_count,
            "headline_only_release_count": self.headline_only_release_count,
            "headline_dataset_comparison_count": (
                self.headline_dataset_comparison_count
            ),
            "headline_dataset_difference_count": (
                self.headline_dataset_difference_count
            ),
            "numeric_overlap_difference_count": (
                self.numeric_overlap_difference_count
            ),
            "status_overlap_difference_count": (
                self.status_overlap_difference_count
            ),
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatHicpArchiveManifestV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            legacy_dataset=EurostatHicpDatasetV1.from_dict(
                _mapping(data.get("legacy_dataset"), "legacy_dataset")
            ),
            current_dataset=EurostatHicpDatasetV1.from_dict(
                _mapping(data.get("current_dataset"), "current_dataset")
            ),
            index_artifacts=tuple(
                EurostatHicpArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(
                    data.get("index_artifacts"), "index_artifacts"
                )
            ),
            index_exclusions=tuple(
                EurostatHicpIndexExclusionV1.from_dict(
                    _mapping(item, "index_exclusion")
                )
                for item in _sequence(
                    data.get("index_exclusions"), "index_exclusions"
                )
            ),
            releases=tuple(
                EurostatHicpReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            overlap_count=cast(int, data.get("overlap_count")),
            overlap_differences=tuple(
                EurostatHicpOverlapDifferenceV1.from_dict(
                    _mapping(item, "overlap_difference")
                )
                for item in _sequence(
                    data.get("overlap_differences"), "overlap_differences"
                )
            ),
            final_artifact_gap_reference_periods=tuple(
                str(item)
                for item in _sequence(
                    data.get("final_artifact_gap_reference_periods"),
                    "final_artifact_gap_reference_periods",
                )
            ),
            flash_artifact_gap_reference_periods=tuple(
                str(item)
                for item in _sequence(
                    data.get("flash_artifact_gap_reference_periods"),
                    "flash_artifact_gap_reference_periods",
                )
            ),
            migrated_publication_count=cast(
                int, data.get("migrated_publication_count")
            ),
            legacy_unindexed_count=cast(
                int, data.get("legacy_unindexed_count")
            ),
            final_release_count=cast(int, data.get("final_release_count")),
            flash_release_count=cast(int, data.get("flash_release_count")),
            page_date_offset_count=cast(
                int, data.get("page_date_offset_count")
            ),
            card_date_offset_count=cast(
                int, data.get("card_date_offset_count")
            ),
            headline_only_release_count=cast(
                int, data.get("headline_only_release_count")
            ),
            headline_dataset_comparison_count=cast(
                int, data.get("headline_dataset_comparison_count")
            ),
            headline_dataset_difference_count=cast(
                int, data.get("headline_dataset_difference_count")
            ),
            numeric_overlap_difference_count=cast(
                int, data.get("numeric_overlap_difference_count")
            ),
            status_overlap_difference_count=cast(
                int, data.get("status_overlap_difference_count")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, content: str) -> EurostatHicpArchiveManifestV1:
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError("Eurostat HICP manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "manifest"))


@dataclass(frozen=True, slots=True)
class _IndexCard:
    href: str
    title: str
    card_date: str


@dataclass(frozen=True, slots=True)
class _ReleaseDocumentLink:
    uri: str
    source_format: OfficialSourceFormat


class _PublicationIndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.cards: list[_IndexCard] = []
        self._in_article = False
        self._article_depth = 0
        self._href: str | None = None
        self._field: str | None = None
        self._field_depth = 0
        self._buffer: list[str] = []
        self._title: str | None = None
        self._card_date: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = {name: value or "" for name, value in attrs}
        classes = set(values.get("class", "").split())
        if tag == "article" and {"estat-card", "estat-news"} <= classes:
            if self._in_article:
                raise ValueError("nested Eurostat HICP publication cards")
            self._in_article = True
            self._article_depth = 1
            self._href = None
            self._title = None
            self._card_date = None
            return
        if not self._in_article:
            return
        self._article_depth += 1
        if tag == "a" and self._href is None and values.get("href"):
            self._href = values["href"]
        if tag == "span" and "estat-title" in classes:
            self._field = "title"
            self._field_depth = self._article_depth
            self._buffer = []
        elif tag == "span" and (
            "ecl-content-block__secondary-meta-label" in classes
        ):
            self._field = "card_date"
            self._field_depth = self._article_depth
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._in_article and self._field is not None:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if not self._in_article:
            return
        if self._field is not None and self._article_depth == self._field_depth:
            value = _required_text(" ".join(self._buffer), self._field)
            if self._field == "title":
                self._title = value
            else:
                self._card_date = value
            self._field = None
            self._buffer = []
        self._article_depth -= 1
        if tag == "article" and self._article_depth == 0:
            if (
                self._href is None
                or self._title is None
                or self._card_date is None
            ):
                raise ValueError("Eurostat HICP publication card is incomplete")
            self.cards.append(
                _IndexCard(
                    href=self._href,
                    title=self._title,
                    card_date=self._card_date,
                )
            )
            self._in_article = False


class _ReleaseDocumentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._capture: str | None = None
        self._depth = 0
        self._buffer: list[str] = []
        self._text: list[str] = []
        self.headings: list[str] = []
        self.titles: list[str] = []
        self.document_hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = {name: value or "" for name, value in attrs}
        if tag == "a" and _RELEASE_DOCUMENT_RE.search(values.get("href", "")):
            self.document_hrefs.append(values["href"])
        if self._capture is not None:
            self._depth += 1
        elif tag in {"h1", "title"}:
            self._capture = tag
            self._depth = 1
            self._buffer = []

    def handle_data(self, data: str) -> None:
        self._text.append(data)
        if self._capture is not None:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._capture is None:
            return
        self._depth -= 1
        if self._depth == 0:
            value = _SPACE_RE.sub(" ", " ".join(self._buffer)).strip()
            if value:
                target = self.headings if self._capture == "h1" else self.titles
                target.append(value)
            self._capture = None
            self._buffer = []

    def headline(self) -> str:
        candidates = [*self.headings, *self.titles]
        for item in candidates:
            normalized = item.casefold()
            if "inflation" in normalized and _PERCENT_RE.search(item):
                headline = item
                for suffix in (" - Products Euro Indicators", " - Eurostat"):
                    headline = headline.split(suffix, 1)[0]
                return headline.strip()
        raise ValueError("Eurostat HICP release page omits its headline")

    def release_date(self) -> str:
        text = _SPACE_RE.sub(" ", " ".join(self._text)).strip()
        match = re.search(
            r"(?:Release date:|Euro indicators)\s*"
            r"(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]+)\s+"
            r"(?P<year>\d{4})",
            text,
            re.IGNORECASE,
        )
        if match is None:
            raise ValueError(
                "Eurostat HICP release page omits its release date"
            )
        return _card_date(
            f"{match.group('day')} {match.group('month')} {match.group('year')}"
        )

    def document_links(
        self, landing_uri: str
    ) -> tuple[_ReleaseDocumentLink, ...]:
        landing_date = _slug_release_date(landing_uri)
        legacy_stub = (
            "/products-euro-indicators/-/" in urlsplit(landing_uri).path
        )
        links: list[_ReleaseDocumentLink] = []
        for href in self.document_hrefs:
            uri = _request_uri(urljoin(landing_uri, href), "document_uri")
            match = _RELEASE_DOCUMENT_RE.search(urlsplit(uri).path)
            if match is None:
                raise ValueError(
                    "Eurostat HICP release document URI is invalid"
                )
            if _slug_release_date(uri) != landing_date:
                if legacy_stub:
                    raise ValueError(
                        "Eurostat HICP landing and document dates differ"
                    )
                continue
            source_format = {
                "html": OfficialSourceFormat.HTML,
                "pdf": OfficialSourceFormat.PDF,
            }[match.group("format").casefold()]
            links.append(
                _ReleaseDocumentLink(uri=uri, source_format=source_format)
            )
        unique = {(item.uri, item.source_format): item for item in links}
        values = tuple(
            sorted(
                unique.values(),
                key=lambda item: (item.source_format.value, item.uri),
            )
        )
        text = _SPACE_RE.sub(" ", " ".join(self._text)).strip().casefold()
        if (
            not values
            and "electronic format" in text
            and "download publication" in text
        ):
            raise ValueError(
                "Eurostat HICP release stub omits its English document"
            )
        return values


def _card_date(value: str) -> str:
    normalized = _required_text(value.replace("\u00a0", " "), "card_date")
    match = re.fullmatch(
        r"(?P<day>\d{1,2}) (?P<month>[A-Za-z]+) (?P<year>\d{4})", normalized
    )
    if match is None:
        raise ValueError("Eurostat HICP card date is invalid")
    month = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
    }.get(match.group("month"))
    if month is None:
        raise ValueError("Eurostat HICP card date month is invalid")
    try:
        parsed = date(int(match.group("year")), month, int(match.group("day")))
    except ValueError as exc:
        raise ValueError("Eurostat HICP card date is invalid") from exc
    return parsed.isoformat()


def _artifact_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
    role: EurostatHicpArtifactRole,
    *,
    page_number: int | None = None,
) -> EurostatHicpArtifactV1:
    if snapshot.status_code != 200:
        raise ValueError("Eurostat HICP artifact did not return HTTP 200")
    expected_format = {
        EurostatHicpArtifactRole.LEGACY_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatHicpArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatHicpArtifactRole.PUBLICATION_INDEX: OfficialSourceFormat.HTML,
        EurostatHicpArtifactRole.RELEASE_LANDING_HTML: OfficialSourceFormat.HTML,
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
        EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
    }[role]
    if snapshot.request.source_format is not expected_format:
        raise ValueError("Eurostat HICP snapshot format differs")
    if snapshot.request.source_key != EUROSTAT_HICP_SOURCE_KEY:
        raise ValueError("Eurostat HICP snapshot source differs")
    if (
        expected_format is OfficialSourceFormat.PDF
        and not snapshot.content.lstrip().startswith(b"%PDF-")
    ):
        raise ValueError("Eurostat HICP PDF artifact signature differs")
    return EurostatHicpArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        resolved_uri=snapshot.resolved_uri,
        source_format=snapshot.request.source_format,
        content_length=len(snapshot.content),
        content_sha256=snapshot.content_sha256,
        page_number=page_number,
    )


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(EUROSTAT_HICP_SOURCE_KEY)
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


def build_eurostat_hicp_dataset_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, OfficialSourceRequestV1]:
    """Build the exact legacy and current first-published-data requests."""
    return (
        _request(
            registry,
            EUROSTAT_HICP_LEGACY_DATASET_URI,
            OfficialSourceFormat.JSON_STAT,
        ),
        _request(
            registry,
            EUROSTAT_HICP_CURRENT_DATASET_URI,
            OfficialSourceFormat.JSON_STAT,
        ),
    )


def _index_uri(page_number: int) -> str:
    page = _bounded_int(
        page_number, "page_number", MAX_EUROSTAT_HICP_INDEX_PAGES
    )
    if page < 1:
        raise ValueError("Eurostat HICP page number must be positive")
    if page == 1:
        return EUROSTAT_HICP_PUBLICATIONS_URI
    return (
        f"{EUROSTAT_HICP_PUBLICATIONS_URI}?"
        "p_p_id=com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_"
        "INSTANCE_ossus8xjfSrU&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&"
        "_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_"
        "INSTANCE_ossus8xjfSrU_redirect=%2Feurostat%2Fen%2Fweb%2Fhicp%2F"
        "publications&"
        "_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_"
        f"INSTANCE_ossus8xjfSrU_cur={page}&"
        "_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_"
        "INSTANCE_ossus8xjfSrU_delta=3&p_r_p_resetCur=false"
    )


def build_eurostat_hicp_index_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded complete HICP publication-index request set."""
    count = _bounded_int(
        page_count, "page_count", MAX_EUROSTAT_HICP_INDEX_PAGES
    )
    if count < 1:
        raise ValueError("Eurostat HICP index needs at least one page")
    return tuple(
        _request(
            registry,
            _index_uri(page),
            OfficialSourceFormat.HTML,
            page_number=page,
        )
        for page in range(1, count + 1)
    )


def build_eurostat_hicp_release_requests(
    registry: OfficialSourceRegistryV1,
    publications: Sequence[EurostatHicpPublicationV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one HTML request for every selected HICP publication."""
    values = tuple(publications)
    if not 1 <= len(values) <= MAX_EUROSTAT_HICP_PUBLICATIONS:
        raise ValueError("Eurostat HICP publication request count is invalid")
    if len({item.source_uri for item in values}) != len(values):
        raise ValueError("Eurostat HICP publication requests repeat a URI")
    return tuple(
        _request(
            registry,
            item.source_uri,
            _document_source_format(item.source_uri)
            or OfficialSourceFormat.HTML,
        )
        for item in values
    )


def _document_links_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[_ReleaseDocumentLink, ...]:
    artifact = _artifact_from_snapshot(
        snapshot, EurostatHicpArtifactRole.RELEASE_LANDING_HTML
    )
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Eurostat HICP release landing is not UTF-8") from exc
    parser = _ReleaseDocumentParser()
    parser.feed(content)
    parser.close()
    return parser.document_links(artifact.source_uri)


def build_eurostat_hicp_document_requests(
    registry: OfficialSourceRegistryV1,
    landing_snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build every English document request discovered on release landings."""
    snapshots = tuple(landing_snapshots)
    if not 1 <= len(snapshots) <= MAX_EUROSTAT_HICP_PUBLICATIONS:
        raise ValueError("Eurostat HICP landing snapshot count is invalid")
    landing_uris = [_canonical_uri(item.request.uri) for item in snapshots]
    if len(landing_uris) != len(set(landing_uris)):
        raise ValueError("Eurostat HICP landing snapshots repeat a URI")
    links = tuple(
        link
        for snapshot in snapshots
        for link in _document_links_from_snapshot(snapshot)
    )
    if len({item.uri for item in links}) != len(links):
        raise ValueError("Eurostat HICP document inventory repeats a URI")
    return tuple(
        _request(registry, item.uri, item.source_format)
        for item in sorted(
            links, key=lambda item: (item.source_format.value, item.uri)
        )
    )


def build_eurostat_hicp_legacy_release_requests(
    registry: OfficialSourceRegistryV1,
    uris: Sequence[str],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Bind a discovered pre-migration URI inventory to the official source."""
    canonical = tuple(_canonical_uri(item) for item in uris)
    if not canonical or len(canonical) > MAX_EUROSTAT_HICP_PUBLICATIONS:
        raise ValueError("Eurostat HICP legacy URI count is invalid")
    if len(set(canonical)) != len(canonical):
        raise ValueError("Eurostat HICP legacy URI inventory repeats a URI")
    if any(
        _slug_release_date(item) >= EUROSTAT_HICP_MIGRATED_INDEX_START_DATE
        for item in canonical
    ):
        raise ValueError("Eurostat HICP legacy inventory crosses migration")
    return tuple(
        _request(registry, item, OfficialSourceFormat.HTML)
        for item in sorted(canonical, key=_slug_release_date)
    )


def _dimension_codes(
    dimension: Mapping[str, Any], name: str
) -> tuple[str, ...]:
    category = _mapping(dimension.get("category"), f"{name}.category")
    indexes = _mapping(category.get("index"), f"{name}.category.index")
    pairs: list[tuple[int, str]] = []
    for code, position in indexes.items():
        if isinstance(position, bool) or not isinstance(position, int):
            raise TypeError(f"{name} position must be an integer")
        pairs.append((position, str(code)))
    pairs.sort()
    if [item[0] for item in pairs] != list(range(len(pairs))):
        raise ValueError(f"{name} positions are not contiguous")
    return tuple(item[1] for item in pairs)


def _parse_dataset(
    snapshot: OfficialRawSnapshotV1,
    dataset_id: str,
) -> EurostatHicpDatasetV1:
    role = {
        "prc_hicp_fp": EurostatHicpArtifactRole.LEGACY_DATASET,
        "prc_hicp_fpd": EurostatHicpArtifactRole.CURRENT_DATASET,
    }[dataset_id]
    artifact = _artifact_from_snapshot(snapshot, role)
    try:
        payload = _mapping(json.loads(snapshot.content), "dataset")
        lexical_payload = _mapping(
            json.loads(snapshot.content, parse_float=str), "lexical dataset"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Eurostat HICP dataset is invalid JSON") from exc
    expected_ids = {
        "prc_hicp_fp": ["freq", "unit", "coicop", "release", "geo", "time"],
        "prc_hicp_fpd": [
            "freq",
            "unit",
            "coicop18",
            "release",
            "geo",
            "time",
        ],
    }[dataset_id]
    identifiers = [str(item) for item in _sequence(payload.get("id"), "id")]
    sizes = list(_sequence(payload.get("size"), "size"))
    if identifiers != expected_ids or len(sizes) != len(expected_ids):
        raise ValueError("Eurostat HICP dataset dimensions differ")
    if any(
        isinstance(item, bool) or not isinstance(item, int) for item in sizes
    ):
        raise TypeError("Eurostat HICP dataset sizes must be integers")
    dimension = _mapping(payload.get("dimension"), "dimension")
    if _dimension_codes(_mapping(dimension["freq"], "freq"), "freq") != ("M",):
        raise ValueError("Eurostat HICP frequency differs")
    if _dimension_codes(_mapping(dimension["unit"], "unit"), "unit") != (
        "RCH_A",
    ):
        raise ValueError("Eurostat HICP unit differs")
    classification_name = (
        "coicop" if dataset_id == "prc_hicp_fp" else "coicop18"
    )
    expected_classification = "CP00" if dataset_id == "prc_hicp_fp" else "TOTAL"
    if _dimension_codes(
        _mapping(dimension[classification_name], classification_name),
        classification_name,
    ) != (expected_classification,):
        raise ValueError("Eurostat HICP classification differs")
    release_codes = _dimension_codes(
        _mapping(dimension["release"], "release"), "release"
    )
    geo_codes = _dimension_codes(_mapping(dimension["geo"], "geo"), "geo")
    time_codes = _dimension_codes(_mapping(dimension["time"], "time"), "time")
    if release_codes != ("FIN", "FLS") or geo_codes != _ECONOMIES:
        raise ValueError("Eurostat HICP stage or geography differs")
    if tuple(cast(list[int], sizes)) != (
        1,
        1,
        1,
        len(release_codes),
        len(geo_codes),
        len(time_codes),
    ):
        raise ValueError("Eurostat HICP dimension sizes differ")
    values = _mapping(payload.get("value"), "value")
    lexical_values = _mapping(lexical_payload.get("value"), "lexical value")
    statuses = _mapping(payload.get("status", {}), "status")
    if set(values) != set(lexical_values):
        raise ValueError("Eurostat HICP lexical and numeric values differ")
    observations: list[EurostatHicpObservationV1] = []
    time_count = len(time_codes)
    geo_count = len(geo_codes)
    for raw_index, raw_value in values.items():
        try:
            flat_index = int(str(raw_index))
        except ValueError as exc:
            raise ValueError("Eurostat HICP sparse index is invalid") from exc
        maximum = len(release_codes) * geo_count * time_count
        if not 0 <= flat_index < maximum:
            raise ValueError("Eurostat HICP sparse index is outside its cube")
        stage_index, remainder = divmod(flat_index, geo_count * time_count)
        geo_index, time_index = divmod(remainder, time_count)
        stage = {
            "FIN": EurostatHicpReleaseStage.FINAL,
            "FLS": EurostatHicpReleaseStage.FLASH,
        }[release_codes[stage_index]]
        lexical = str(lexical_values[raw_index])
        numeric = float(cast(float, raw_value))
        observations.append(
            EurostatHicpObservationV1(
                dataset_id=dataset_id,
                taxonomy=(
                    "ecoicop-v1"
                    if dataset_id == "prc_hicp_fp"
                    else "ecoicop-v2"
                ),
                stage=stage,
                economy_code=geo_codes[geo_index],
                reference_period=time_codes[time_index],
                value_lexical=lexical,
                value=numeric,
                status=cast(str | None, statuses.get(raw_index)),
                flat_index=flat_index,
            )
        )
    observations.sort(
        key=lambda item: (
            _STAGE_ORDER[item.stage],
            _ECONOMY_ORDER[item.economy_code],
            item.reference_period,
        )
    )
    return EurostatHicpDatasetV1(
        dataset_id=dataset_id,
        taxonomy="ecoicop-v1" if dataset_id == "prc_hicp_fp" else "ecoicop-v2",
        label=str(payload.get("label", "")),
        updated_at=str(payload.get("updated", "")),
        time_start=time_codes[0],
        time_end=time_codes[-1],
        artifact=artifact,
        observations=tuple(observations),
    )


def parse_eurostat_hicp_datasets(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[EurostatHicpDatasetV1, EurostatHicpDatasetV1]:
    """Decode and validate the exact legacy/current JSON-stat pair."""
    by_name = {
        Path(urlsplit(item.request.uri).path).name: item for item in snapshots
    }
    if len(by_name) != len(snapshots):
        raise ValueError("Eurostat HICP dataset snapshots repeat a table")
    if set(by_name) != _DATASET_IDS:
        raise ValueError("Eurostat HICP dataset snapshot pair differs")
    return (
        _parse_dataset(by_name["prc_hicp_fp"], "prc_hicp_fp"),
        _parse_dataset(by_name["prc_hicp_fpd"], "prc_hicp_fpd"),
    )


def _publication_from_card(
    card: _IndexCard,
    *,
    page_number: int,
) -> EurostatHicpPublicationV1:
    uri = _publication_uri(urljoin(EUROSTAT_HICP_PUBLICATIONS_URI, card.href))
    displayed_date = _card_date(card.card_date)
    try:
        release_date = _slug_release_date(uri)
        release_date_from_uri = True
    except ValueError:
        if _KNOWN_MALFORMED_RELEASE_TOKEN not in uri:
            raise
        release_date = displayed_date
        release_date_from_uri = False
    stage = _stage_from_title(card.title, release_date)
    lexical, value = _headline_value(card.title)
    offset = (
        date.fromisoformat(displayed_date) - date.fromisoformat(release_date)
    ).days
    return EurostatHicpPublicationV1(
        release_date=release_date,
        release_date_from_uri=release_date_from_uri,
        reference_period=_reference_period(release_date, stage),
        stage=stage,
        source_title=card.title,
        source_uri=uri,
        headline_value_lexical=lexical,
        headline_value=value,
        page_release_date=None,
        page_release_date_offset_days=None,
        card_date=displayed_date,
        card_date_offset_days=offset,
        index_page_number=page_number,
        legacy_unindexed=False,
    )


def _index_exclusion_from_card(
    card: _IndexCard,
    *,
    page_number: int,
) -> EurostatHicpIndexExclusionV1:
    return EurostatHicpIndexExclusionV1(
        source_title=card.title,
        source_uri=urljoin(EUROSTAT_HICP_PUBLICATIONS_URI, card.href),
        card_date=_card_date(card.card_date),
        index_page_number=page_number,
        reason="non-euro-area-scope",
    )


def parse_eurostat_hicp_publication_index(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> tuple[
    tuple[EurostatHicpArtifactV1, ...],
    tuple[EurostatHicpPublicationV1, ...],
    tuple[EurostatHicpIndexExclusionV1, ...],
]:
    """Parse every contiguous migrated-index page without card-date guessing."""
    as_of = _iso_date(as_of_date, "as_of_date")
    by_page = {item.request.page_number: item for item in snapshots}
    if len(by_page) != len(snapshots):
        raise ValueError("Eurostat HICP index snapshots repeat a page")
    if None in by_page or not by_page:
        raise ValueError("Eurostat HICP index snapshots omit page numbers")
    pages = sorted(cast(int, item) for item in by_page)
    if pages != list(range(1, len(pages) + 1)):
        raise ValueError("Eurostat HICP index snapshots are not contiguous")
    artifacts: list[EurostatHicpArtifactV1] = []
    publications: list[EurostatHicpPublicationV1] = []
    exclusions: list[EurostatHicpIndexExclusionV1] = []
    for page in pages:
        snapshot = by_page[page]
        artifacts.append(
            _artifact_from_snapshot(
                snapshot,
                EurostatHicpArtifactRole.PUBLICATION_INDEX,
                page_number=page,
            )
        )
        try:
            content = snapshot.content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("Eurostat HICP index page is not UTF-8") from exc
        parser = _PublicationIndexParser()
        parser.feed(content)
        parser.close()
        if page != pages[-1] and len(parser.cards) != 3:
            raise ValueError(
                "Eurostat HICP nonterminal index page is incomplete"
            )
        if page == pages[-1] and not 1 <= len(parser.cards) <= 3:
            raise ValueError("Eurostat HICP terminal index page is incomplete")
        for card in parser.cards:
            if card.title.casefold().startswith("g20 annual inflation"):
                exclusion = _index_exclusion_from_card(card, page_number=page)
                if exclusion.card_date <= as_of:
                    exclusions.append(exclusion)
                continue
            publication = _publication_from_card(card, page_number=page)
            if publication.release_date <= as_of:
                publications.append(publication)
    publications.sort(key=lambda item: (item.release_date, item.stage.value))
    if len({item.source_uri for item in publications}) != len(publications):
        raise ValueError("Eurostat HICP index repeats a release URI")
    if len({item.occurrence_key for item in publications}) != len(publications):
        raise ValueError("Eurostat HICP index repeats a release occurrence")
    exclusions.sort(
        key=lambda item: (
            item.index_page_number,
            item.card_date,
            item.source_uri,
        )
    )
    return tuple(artifacts), tuple(publications), tuple(exclusions)


def _publication_from_release_snapshot(
    snapshot: OfficialRawSnapshotV1,
    *,
    indexed_publication: EurostatHicpPublicationV1 | None,
) -> EurostatHicpPublicationV1:
    artifact = _artifact_from_snapshot(
        snapshot, EurostatHicpArtifactRole.RELEASE_LANDING_HTML
    )
    release_date = _slug_release_date(artifact.source_uri)
    if indexed_publication is None:
        if release_date >= EUROSTAT_HICP_MIGRATED_INDEX_START_DATE:
            raise ValueError(
                "Eurostat HICP release snapshot is absent from index"
            )
        card_date = None
        card_date_offset_days = None
        index_page_number = None
        legacy_unindexed = True
    else:
        if artifact.source_uri != indexed_publication.source_uri:
            raise ValueError("Eurostat HICP index and release URI differ")
        card_date = indexed_publication.card_date
        card_date_offset_days = indexed_publication.card_date_offset_days
        index_page_number = indexed_publication.index_page_number
        legacy_unindexed = False
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Eurostat HICP release is not UTF-8") from exc
    parser = _ReleaseDocumentParser()
    parser.feed(content)
    parser.close()
    title = parser.headline()
    page_release_date = parser.release_date()
    page_release_date_offset_days = (
        date.fromisoformat(page_release_date) - date.fromisoformat(release_date)
    ).days
    stage = _stage_from_title(title, release_date)
    lexical, value = _headline_value(title)
    if indexed_publication is not None and (
        stage is not indexed_publication.stage
        or lexical != indexed_publication.headline_value_lexical
        or value != indexed_publication.headline_value
    ):
        raise ValueError("Eurostat HICP index and release headline differ")
    return EurostatHicpPublicationV1(
        release_date=release_date,
        release_date_from_uri=True,
        reference_period=_reference_period(release_date, stage),
        stage=stage,
        source_title=title,
        source_uri=artifact.source_uri,
        headline_value_lexical=lexical,
        headline_value=value,
        page_release_date=page_release_date,
        page_release_date_offset_days=page_release_date_offset_days,
        card_date=card_date,
        card_date_offset_days=card_date_offset_days,
        index_page_number=index_page_number,
        legacy_unindexed=legacy_unindexed,
    )


def _overlap_differences(
    legacy: EurostatHicpDatasetV1,
    current: EurostatHicpDatasetV1,
) -> tuple[int, tuple[EurostatHicpOverlapDifferenceV1, ...]]:
    legacy_by_key = {item.key: item for item in legacy.observations}
    current_by_key = {item.key: item for item in current.observations}
    common = set(legacy_by_key) & set(current_by_key)
    differences: list[EurostatHicpOverlapDifferenceV1] = []
    for key in sorted(common):
        old = legacy_by_key[key]
        new = current_by_key[key]
        if old.value == new.value and old.status == new.status:
            continue
        differences.append(
            EurostatHicpOverlapDifferenceV1(
                stage=old.stage,
                economy_code=old.economy_code,
                reference_period=old.reference_period,
                legacy_value_lexical=old.value_lexical,
                legacy_value=old.value,
                legacy_status=old.status,
                current_value_lexical=new.value_lexical,
                current_value=new.value,
                current_status=new.status,
                numeric_difference=old.value != new.value,
                status_difference=old.status != new.status,
            )
        )
    return len(common), tuple(differences)


def _preferred_observations(
    legacy: EurostatHicpDatasetV1,
    current: EurostatHicpDatasetV1,
) -> Mapping[tuple[str, str, str], EurostatHicpObservationV1]:
    selected = {item.key: item for item in legacy.observations}
    for item in current.observations:
        if item.reference_period >= "2026-01":
            selected[item.key] = item
    return selected


def _release_value_from_observation(
    observation: EurostatHicpObservationV1,
) -> EurostatHicpReleaseValueV1:
    provenance = (
        EurostatHicpValueProvenance.LEGACY_DATASET
        if observation.dataset_id == "prc_hicp_fp"
        else EurostatHicpValueProvenance.CURRENT_DATASET
    )
    return EurostatHicpReleaseValueV1(
        stage=observation.stage,
        economy_code=observation.economy_code,
        reference_period=observation.reference_period,
        value_lexical=observation.value_lexical,
        value=observation.value,
        status=observation.status,
        provenance=provenance,
        evidence_id=observation.observation_id,
    )


def _build_release(
    publication: EurostatHicpPublicationV1,
    artifacts: tuple[EurostatHicpArtifactV1, ...],
    preferred: Mapping[tuple[str, str, str], EurostatHicpObservationV1],
) -> EurostatHicpReleaseV1:
    ea_key = (publication.stage.value, "EA", publication.reference_period)
    ea_observation = preferred.get(ea_key)
    dataset_ea_value = (
        _release_value_from_observation(ea_observation)
        if ea_observation is not None
        else None
    )
    values: list[EurostatHicpReleaseValueV1] = [
        EurostatHicpReleaseValueV1(
            stage=publication.stage,
            economy_code="EA",
            reference_period=publication.reference_period,
            value_lexical=publication.headline_value_lexical,
            value=publication.headline_value,
            status=None,
            provenance=EurostatHicpValueProvenance.RELEASE_HEADLINE,
            evidence_id=publication.publication_id,
        )
    ]
    for economy in ("DE", "FR"):
        observation = preferred.get(
            (publication.stage.value, economy, publication.reference_period)
        )
        if observation is not None:
            values.append(_release_value_from_observation(observation))
    return EurostatHicpReleaseV1(
        publication=publication,
        artifacts=artifacts,
        dataset_ea_value=dataset_ea_value,
        headline_dataset_numeric_difference=(
            dataset_ea_value.value != publication.headline_value
            if dataset_ea_value is not None
            else None
        ),
        values=tuple(values),
    )


def build_eurostat_hicp_archive_manifest(
    registry: OfficialSourceRegistryV1,
    dataset_snapshots: Sequence[OfficialRawSnapshotV1],
    index_snapshots: Sequence[OfficialRawSnapshotV1],
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatHicpArchiveManifestV1:
    """Build the HICP manifest from exact caller-retained official bytes."""
    source = registry.source(EUROSTAT_HICP_SOURCE_KEY)
    legacy, current = parse_eurostat_hicp_datasets(dataset_snapshots)
    index_artifacts, indexed_publications, index_exclusions = (
        parse_eurostat_hicp_publication_index(
            index_snapshots, as_of_date=as_of_date
        )
    )
    landing_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    document_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in release_snapshots:
        is_document = (
            _RELEASE_DOCUMENT_RE.search(urlsplit(snapshot.request.uri).path)
            is not None
        )
        uri = (
            _request_uri(snapshot.request.uri)
            if is_document
            else _canonical_uri(snapshot.request.uri)
        )
        target = document_by_uri if is_document else landing_by_uri
        if uri in target:
            raise ValueError("Eurostat HICP release snapshots repeat a URI")
        target[uri] = snapshot
    indexed_by_uri = {item.source_uri: item for item in indexed_publications}
    primary_snapshot_uris = set(landing_by_uri) | set(document_by_uri)
    if not set(indexed_by_uri) <= primary_snapshot_uris:
        raise ValueError("Eurostat HICP indexed release corpus is incomplete")
    publications = [
        _publication_from_release_snapshot(
            snapshot,
            indexed_publication=indexed_by_uri.get(uri),
        )
        for uri, snapshot in landing_by_uri.items()
    ]
    publications.extend(
        publication
        for uri, publication in indexed_by_uri.items()
        if uri in document_by_uri
    )
    publications.sort(key=lambda item: (item.release_date, item.stage.value))
    if len({item.source_uri for item in publications}) != len(publications):
        raise ValueError("Eurostat HICP publication inventory repeats a URI")
    if len({item.occurrence_key for item in publications}) != len(publications):
        raise ValueError(
            "Eurostat HICP publication inventory repeats an occurrence"
        )
    publication_uris = {item.source_uri for item in publications}
    if set(landing_by_uri) | (set(document_by_uri) & set(indexed_by_uri)) != (
        publication_uris
    ):
        raise ValueError("Eurostat HICP primary release corpus is incomplete")
    preferred = _preferred_observations(legacy, current)
    releases: list[EurostatHicpReleaseV1] = []
    expected_document_uris: set[str] = set()
    for publication in publications:
        landing_snapshot = landing_by_uri.get(publication.source_uri)
        artifacts: tuple[EurostatHicpArtifactV1, ...]
        if landing_snapshot is None:
            document_snapshot = document_by_uri[publication.source_uri]
            expected_document_uris.add(publication.source_uri)
            artifacts = (
                _artifact_from_snapshot(
                    document_snapshot,
                    (
                        EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML
                        if document_snapshot.request.source_format
                        is OfficialSourceFormat.HTML
                        else EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF
                    ),
                ),
            )
        else:
            links = _document_links_from_snapshot(landing_snapshot)
            expected_document_uris.update(item.uri for item in links)
            missing = [
                item.uri for item in links if item.uri not in document_by_uri
            ]
            if missing:
                raise ValueError(
                    "Eurostat HICP release document corpus is incomplete"
                )
            artifacts = (
                _artifact_from_snapshot(
                    landing_snapshot,
                    EurostatHicpArtifactRole.RELEASE_LANDING_HTML,
                ),
                *(
                    _artifact_from_snapshot(
                        document_by_uri[item.uri],
                        (
                            EurostatHicpArtifactRole.RELEASE_DOCUMENT_HTML
                            if item.source_format is OfficialSourceFormat.HTML
                            else EurostatHicpArtifactRole.RELEASE_DOCUMENT_PDF
                        ),
                    )
                    for item in links
                ),
            )
        releases.append(_build_release(publication, artifacts, preferred))
    if set(document_by_uri) != expected_document_uris:
        raise ValueError("Eurostat HICP release document corpus differs")
    overlap_count, differences = _overlap_differences(legacy, current)
    all_artifacts = [
        legacy.artifact,
        current.artifact,
        *index_artifacts,
        *(artifact for item in releases for artifact in item.artifacts),
    ]
    migrated = sum(not item.legacy_unindexed for item in publications)
    unindexed = len(publications) - migrated
    final_count = sum(
        item.stage is EurostatHicpReleaseStage.FINAL for item in publications
    )
    flash_count = len(publications) - final_count
    current_final_periods = tuple(
        item.reference_period
        for item in current.observations
        if item.stage is EurostatHicpReleaseStage.FINAL
        and item.economy_code == "EA"
    )
    flash_periods = tuple(
        item.reference_period
        for dataset in (legacy, current)
        for item in dataset.observations
        if item.stage is EurostatHicpReleaseStage.FLASH
        and item.economy_code == "EA"
    )
    if not current_final_periods or not flash_periods:
        raise ValueError(
            "Eurostat HICP expected occurrence ranges are unavailable"
        )
    final_published = {
        item.reference_period
        for item in publications
        if item.stage is EurostatHicpReleaseStage.FINAL
    }
    flash_published = {
        item.reference_period
        for item in publications
        if item.stage is EurostatHicpReleaseStage.FLASH
    }
    final_expected = set(
        _month_range(
            EUROSTAT_HICP_FINAL_START_REFERENCE_PERIOD,
            max(current_final_periods),
        )
    )
    flash_expected = set(
        _month_range(
            EUROSTAT_HICP_FIRST_FLASH_REFERENCE_PERIOD,
            max(flash_periods),
        )
    )
    return EurostatHicpArchiveManifestV1(
        as_of_date=as_of_date,
        registry_id=registry.registry_id,
        source_id=source.source_id,
        legacy_dataset=legacy,
        current_dataset=current,
        index_artifacts=index_artifacts,
        index_exclusions=index_exclusions,
        releases=tuple(releases),
        overlap_count=overlap_count,
        overlap_differences=differences,
        final_artifact_gap_reference_periods=tuple(
            sorted(final_expected - final_published)
        ),
        flash_artifact_gap_reference_periods=tuple(
            sorted(flash_expected - flash_published)
        ),
        migrated_publication_count=migrated,
        legacy_unindexed_count=unindexed,
        final_release_count=final_count,
        flash_release_count=flash_count,
        page_date_offset_count=sum(
            item.page_release_date_offset_days == -1 for item in publications
        ),
        card_date_offset_count=sum(
            item.card_date_offset_days == -1 for item in publications
        ),
        headline_only_release_count=sum(
            len(item.values) == 1 for item in releases
        ),
        headline_dataset_comparison_count=sum(
            item.dataset_ea_value is not None for item in releases
        ),
        headline_dataset_difference_count=sum(
            item.headline_dataset_numeric_difference is True
            for item in releases
        ),
        numeric_overlap_difference_count=sum(
            item.numeric_difference for item in differences
        ),
        status_overlap_difference_count=sum(
            item.status_difference for item in differences
        ),
        raw_artifact_count=len(all_artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in all_artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in all_artifacts),
    )


def replay_eurostat_hicp_archive(
    registry: OfficialSourceRegistryV1,
    dataset_snapshots: Sequence[OfficialRawSnapshotV1],
    index_snapshots: Sequence[OfficialRawSnapshotV1],
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    expected_manifest: EurostatHicpArchiveManifestV1,
) -> EurostatHicpArchiveManifestV1:
    """Rebuild the HICP receipt and require byte-for-byte identity."""
    rebuilt = build_eurostat_hicp_archive_manifest(
        registry,
        dataset_snapshots,
        index_snapshots,
        release_snapshots,
        as_of_date=expected_manifest.as_of_date,
    )
    if rebuilt.to_json() != expected_manifest.to_json():
        raise ValueError("Eurostat HICP replay differs from packaged manifest")
    return rebuilt


def packaged_eurostat_hicp_manifest_path() -> Path:
    """Return the installed HICP archive-manifest path."""
    return (
        Path(__file__).resolve().parent
        / "assets"
        / "eurostat_hicp_archive_v1.json"
    )


def load_packaged_eurostat_hicp_archive_manifest() -> (
    EurostatHicpArchiveManifestV1
):
    """Load and validate the installed HICP archive manifest."""
    path = packaged_eurostat_hicp_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("packaged Eurostat HICP manifest exceeds size bound")
    return EurostatHicpArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


__all__ = [
    "EUROSTAT_HICP_ARTIFACT_SCHEMA_VERSION",
    "EUROSTAT_HICP_AS_OF_DATE",
    "EUROSTAT_HICP_CURRENT_DATASET_URI",
    "EUROSTAT_HICP_DATASET_SCHEMA_VERSION",
    "EUROSTAT_HICP_FINAL_START_REFERENCE_PERIOD",
    "EUROSTAT_HICP_FIRST_FLASH_REFERENCE_PERIOD",
    "EUROSTAT_HICP_INDEX_EXCLUSION_SCHEMA_VERSION",
    "EUROSTAT_HICP_LATEST_PACKAGED_RELEASE_DATE",
    "EUROSTAT_HICP_LEGACY_DATASET_URI",
    "EUROSTAT_HICP_MANIFEST_SCHEMA_VERSION",
    "EUROSTAT_HICP_MEASURE_KEY",
    "EUROSTAT_HICP_MIGRATED_INDEX_START_DATE",
    "EUROSTAT_HICP_OBSERVATION_SCHEMA_VERSION",
    "EUROSTAT_HICP_OVERLAP_SCHEMA_VERSION",
    "EUROSTAT_HICP_PROGRAM_KEY",
    "EUROSTAT_HICP_PUBLICATIONS_URI",
    "EUROSTAT_HICP_PUBLICATION_SCHEMA_VERSION",
    "EUROSTAT_HICP_RELEASE_SCHEMA_VERSION",
    "EUROSTAT_HICP_SOURCE_KEY",
    "EUROSTAT_HICP_SOURCE_TIMEZONE",
    "EUROSTAT_HICP_VALUE_SCHEMA_VERSION",
    "EurostatHicpArchiveManifestV1",
    "EurostatHicpArtifactRole",
    "EurostatHicpArtifactV1",
    "EurostatHicpDatasetV1",
    "EurostatHicpIndexExclusionV1",
    "EurostatHicpObservationV1",
    "EurostatHicpOverlapDifferenceV1",
    "EurostatHicpPublicationV1",
    "EurostatHicpReleaseStage",
    "EurostatHicpReleaseV1",
    "EurostatHicpReleaseValueV1",
    "EurostatHicpValueProvenance",
    "build_eurostat_hicp_archive_manifest",
    "build_eurostat_hicp_dataset_requests",
    "build_eurostat_hicp_document_requests",
    "build_eurostat_hicp_index_requests",
    "build_eurostat_hicp_legacy_release_requests",
    "build_eurostat_hicp_release_requests",
    "load_packaged_eurostat_hicp_archive_manifest",
    "packaged_eurostat_hicp_manifest_path",
    "parse_eurostat_hicp_datasets",
    "parse_eurostat_hicp_publication_index",
    "replay_eurostat_hicp_archive",
]

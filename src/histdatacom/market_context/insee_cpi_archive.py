"""Deterministic qualification of INSEE CPI first-release evidence.

INSEE's English Solr inventory exposes exact publication timestamps for both
provisional and final monthly CPI releases.  The release pages preserve the
source-authored CPI, core-inflation, and HICP values, while BDM SDMX series
expose the latest revised values.  This module keeps those two evidence classes
separate and makes provisional-to-final revisions explicit.

Raw response bytes remain in an operator-retained corpus.  The packaged
manifest binds every request and response hash, every selected and excluded
inventory row, every parsed first-published value, and four current-series
cross-checks.  Replay performs no network access.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceEntryV1,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

INSEE_CPI_SOURCE_KEY: Final = "fr.insee.cpi"
INSEE_CPI_PROGRAM_KEY: Final = "fr.insee.consumer-price-index"
INSEE_CPI_PARSER_ID: Final = "official.insee-cpi.v1"
INSEE_CPI_PARSER_VERSION: Final = "1"
INSEE_CPI_SOURCE_TIMEZONE: Final = "Europe/Paris"
INSEE_CPI_SEARCH_URI: Final = "https://www.insee.fr/en/solr/consultation"
INSEE_CPI_SERIES_SEARCH_URI: Final = (
    "https://www.insee.fr/en/statistiques/series/ajax/consultation"
)
INSEE_CPI_RELEASE_URI_PREFIX: Final = "https://www.insee.fr/en/statistiques/"
INSEE_CPI_SDMX_URI_PREFIX: Final = (
    "https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/"
)
INSEE_CPI_FINAL_START_PERIOD: Final = "2009-05"
INSEE_CPI_PROVISIONAL_START_PERIOD: Final = "2016-01"
INSEE_CPI_CURRENT_START_PERIOD: Final = "2000-01"
INSEE_CPI_LATEST_PACKAGED_PERIOD: Final = "2026-08"

INSEE_CPI_SEARCH_SCHEMA_VERSION: Final = "histdatacom.insee-cpi-search.v1"
INSEE_CPI_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.insee-cpi-artifact.v1"
INSEE_CPI_EXCLUSION_SCHEMA_VERSION: Final = "histdatacom.insee-cpi-exclusion.v1"
INSEE_CPI_VALUE_SCHEMA_VERSION: Final = "histdatacom.insee-cpi-value.v1"
INSEE_CPI_RELEASE_SCHEMA_VERSION: Final = "histdatacom.insee-cpi-release.v1"
INSEE_CPI_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-cpi-current-observation.v1"
)
INSEE_CPI_SERIES_SCHEMA_VERSION: Final = (
    "histdatacom.insee-cpi-current-series.v1"
)
INSEE_CPI_REVISION_SCHEMA_VERSION: Final = "histdatacom.insee-cpi-revision.v1"
INSEE_CPI_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.insee-cpi-archive-manifest.v1"
)

MAX_INSEE_CPI_RELEASES: Final = 512
MAX_INSEE_CPI_EXCLUSIONS: Final = 1_024
MAX_INSEE_CPI_ARTIFACTS: Final = 1_024
MAX_INSEE_CPI_VALUES: Final = 4_096
MAX_INSEE_CPI_OBSERVATIONS: Final = 4_096
MAX_INSEE_CPI_TOTAL_BYTES: Final = 512_000_000

_SPACE_RE = re.compile(r"\s+")
_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
_SUBTITLE_RE = re.compile(
    r"^Consumer price index - (?P<stage>provisional|final) results - "
    r"(?P<month>[A-Za-z]+) (?P<year>\d{4})$",
    re.IGNORECASE,
)
_SERIES: Final[Mapping[str, tuple[str, str]]] = {
    "011814631": ("cpi-all-items", "month-on-month"),
    "011814632": ("cpi-all-items", "year-on-year"),
    "011814145": ("core-inflation", "year-on-year"),
    "011812232": ("hicp-all-items", "year-on-year"),
}
_CPI_CATALOG_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsField": [],
    "filters": [{"field": "bdm_idFamille", "values": ["129209311"]}],
    "q": '"All items"',
    "rows": 1000,
    "sortFields": [],
    "start": 0,
}
_HICP_CATALOG_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsField": [],
    "filters": [{"field": "bdm_idFamille", "values": ["129143583"]}],
    "q": "Total",
    "rows": 1000,
    "sortFields": [],
    "start": 0,
}
_SEARCH_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsQuery": [],
    "filters": [
        {"field": "diffusion", "values": [True]},
        {"field": "rubrique", "values": ["statistiques"]},
    ],
    "q": "consumer price index",
    "rows": 1000,
    "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
    "start": 0,
}
_MONTHS: Final = {
    name.casefold(): index
    for index, name in enumerate(calendar.month_name)
    if name
}


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    result = _SPACE_RE.sub(" ", value.replace("\xa0", " ")).strip()
    if not result or len(result) > 16_384:
        raise ValueError(f"{name} is invalid")
    return result


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be an array")
    return value


def _bounded_int(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _month(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    if date.fromisoformat(result).isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _timestamp(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(
        None
    ):
        raise ValueError(f"{name} must be UTC")
    return result


def _month_range(start: str, end: str) -> tuple[str, ...]:
    first = date.fromisoformat(f"{_month(start, 'start')}-01")
    stop = date.fromisoformat(f"{_month(end, 'end')}-01")
    values: list[str] = []
    current = first
    while current <= stop:
        values.append(current.strftime("%Y-%m"))
        ordinal = current.year * 12 + current.month
        current = date(ordinal // 12, ordinal % 12 + 1, 1)
    return tuple(values)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _official_uri(value: object, name: str = "source_uri") -> str:
    result = _required_text(value, name)
    parsed = urlsplit(result)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"www.insee.fr", "bdm.insee.fr"}
        or parsed.username is not None
        or parsed.fragment
    ):
        raise ValueError(f"{name} must use an official INSEE HTTPS host")
    return result


def _numeric(value: object, name: str) -> tuple[str, float]:
    lexical = _required_text(value, name)
    normalized = (
        lexical.replace("−", "-")
        .replace("–", "-")
        .replace("—", "-")
        .replace("‑", "-")
        .replace(",", ".")
        .replace("%", "")
        .strip()
    )
    normalized = re.sub(r"^([+-])\s+", r"\1", normalized)
    if normalized.startswith(("+.", "-.")):
        normalized = normalized[:1] + "0" + normalized[1:]
    elif normalized.startswith("."):
        normalized = "0" + normalized
    if _NUMBER_RE.fullmatch(normalized) is None:
        raise ValueError(f"{name} is not numeric")
    numeric = float(normalized)
    if not math.isfinite(numeric) or not -100.0 <= numeric <= 100.0:
        raise ValueError(f"{name} is outside its bound")
    return lexical, numeric


def _body(payload: Mapping[str, JSONValue]) -> str:
    return str(canonical_contract_json(dict(payload)))


def _source(registry: OfficialSourceRegistryV1) -> OfficialSourceEntryV1:
    source = registry.source(INSEE_CPI_SOURCE_KEY)
    if (
        source.parser_id != INSEE_CPI_PARSER_ID
        or source.parser_version != INSEE_CPI_PARSER_VERSION
    ):
        raise ValueError("INSEE CPI registry parser binding differs")
    return source


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    method: OfficialRequestMethod = OfficialRequestMethod.GET,
    body: Mapping[str, JSONValue] | None = None,
) -> OfficialSourceRequestV1:
    source = _source(registry)
    parsed = urlsplit(_official_uri(uri))
    if parsed.hostname not in source.allowed_hosts:
        raise ValueError("INSEE CPI request host differs from registry")
    if source_format not in source.formats:
        raise ValueError("INSEE CPI request format differs from registry")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=method,
        uri=uri,
        source_format=source_format,
        body_text=_body(body) if body is not None else None,
        body_content_type=("application/json" if body is not None else None),
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def build_insee_cpi_search_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact bounded Solr request used for release discovery."""
    return _request(
        registry,
        INSEE_CPI_SEARCH_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_SEARCH_BODY,
    )


def build_insee_cpi_catalog_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, OfficialSourceRequestV1]:
    """Build the two bounded current-series metadata requests."""
    return (
        _request(
            registry,
            INSEE_CPI_SERIES_SEARCH_URI,
            OfficialSourceFormat.JSON,
            method=OfficialRequestMethod.POST,
            body=_CPI_CATALOG_BODY,
        ),
        _request(
            registry,
            INSEE_CPI_SERIES_SEARCH_URI,
            OfficialSourceFormat.JSON,
            method=OfficialRequestMethod.POST,
            body=_HICP_CATALOG_BODY,
        ),
    )


def build_insee_cpi_current_series_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build exact SDMX requests for the four comparable current series."""
    return tuple(
        _request(
            registry,
            f"{INSEE_CPI_SDMX_URI_PREFIX}{series_id}"
            "?startPeriod=2000-01&endPeriod=2026-08",
            OfficialSourceFormat.SDMX_21,
        )
        for series_id in _SERIES
    )


class InseeCpiReleaseStage(str, Enum):
    """Publication stage retained from the source subtitle."""

    PROVISIONAL = "provisional"
    FINAL = "final"


class InseeCpiArtifactRole(str, Enum):
    """Semantic role of one retained official response."""

    RELEASE_SEARCH = "release-search"
    RELEASE_HTML = "release-html"
    CPI_SERIES_CATALOG = "cpi-series-catalog"
    HICP_SERIES_CATALOG = "hicp-series-catalog"
    CURRENT_SDMX = "current-sdmx"


@dataclass(frozen=True, slots=True)
class InseeCpiArtifactV1:
    """Hash-bound receipt for one exact official response."""

    role: InseeCpiArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = INSEE_CPI_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI artifact schema")
        if not isinstance(self.role, InseeCpiArtifactRole):
            raise TypeError("INSEE CPI artifact role is invalid")
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError("INSEE CPI artifact format is invalid")
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        if _DIGEST_RE.fullmatch(self.content_sha256) is None:
            raise ValueError("INSEE CPI artifact digest is invalid")
        if not 0 < self.content_length <= MAX_INSEE_CPI_TOTAL_BYTES:
            raise ValueError("INSEE CPI artifact length is invalid")
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError("INSEE CPI artifact request identity is invalid")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "source_uri": self.source_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiArtifactV1:
        return cls(
            role=InseeCpiArtifactRole(str(data.get("role", ""))),
            source_uri=str(data.get("source_uri", "")),
            source_format=OfficialSourceFormat(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            request_id=str(data.get("request_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: InseeCpiArtifactRole
) -> InseeCpiArtifactV1:
    if (
        snapshot.request.source_key != INSEE_CPI_SOURCE_KEY
        or snapshot.request.parser_id != INSEE_CPI_PARSER_ID
        or snapshot.request.parser_version != INSEE_CPI_PARSER_VERSION
    ):
        raise ValueError("INSEE CPI snapshot names another source")
    return InseeCpiArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class InseeCpiExcludedSearchResultV1:
    """One explicitly classified but unselected Solr result."""

    source_document_id: int
    title: str
    subtitle: str | None
    reason: str
    schema_version: str = INSEE_CPI_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_EXCLUSION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI exclusion schema")
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError("INSEE CPI excluded document ID is invalid")
        _required_text(self.title, "title")
        if self.subtitle is not None:
            _required_text(self.subtitle, "subtitle")
        if self.reason not in {
            "missing-subtitle",
            "other-consumer-price-publication",
            "search-result-outside-release-lineage",
        }:
            raise ValueError("INSEE CPI exclusion reason is invalid")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_document_id": self.source_document_id,
            "title": self.title,
            "subtitle": self.subtitle,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeCpiExcludedSearchResultV1:
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            title=str(data.get("title", "")),
            subtitle=cast(str | None, data.get("subtitle")),
            reason=str(data.get("reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeCpiPublishedValueV1:
    """One source-authored rate retained from a release page."""

    measure: str
    change_basis: str
    lexical: str
    value: float
    source_locator: str
    schema_version: str = INSEE_CPI_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI value schema")
        if self.measure not in {
            "cpi-all-items",
            "core-inflation",
            "hicp-all-items",
        }:
            raise ValueError("INSEE CPI value measure is invalid")
        if self.change_basis not in {"month-on-month", "year-on-year"}:
            raise ValueError("INSEE CPI change basis is invalid")
        _, parsed = _numeric(self.lexical, "lexical")
        if not math.isclose(parsed, float(self.value), abs_tol=1e-12):
            raise ValueError("INSEE CPI lexical and numeric values differ")
        _required_text(self.source_locator, "source_locator")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "measure": self.measure,
            "change_basis": self.change_basis,
            "unit": "percent",
            "lexical": self.lexical,
            "value": self.value,
            "source_locator": self.source_locator,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiPublishedValueV1:
        if data.get("unit") != "percent":
            raise ValueError("INSEE CPI value unit differs")
        numeric = data.get("value")
        if isinstance(numeric, bool) or not isinstance(numeric, (int, float)):
            raise TypeError("INSEE CPI value must be numeric")
        return cls(
            measure=str(data.get("measure", "")),
            change_basis=str(data.get("change_basis", "")),
            lexical=str(data.get("lexical", "")),
            value=float(numeric),
            source_locator=str(data.get("source_locator", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeCpiReleaseV1:
    """One normalized provisional or final monthly release."""

    source_document_id: int
    stage: InseeCpiReleaseStage
    reference_period: str
    title: str
    subtitle: str
    source_uri: str
    published_at_utc: str
    published_at_local: str
    artifact: InseeCpiArtifactV1
    values: tuple[InseeCpiPublishedValueV1, ...]
    release_id: str = ""
    schema_version: str = INSEE_CPI_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI release schema")
        if not 0 < self.source_document_id < 100_000_000:
            raise ValueError("INSEE CPI document ID is invalid")
        if not isinstance(self.stage, InseeCpiReleaseStage):
            raise TypeError("INSEE CPI release stage is invalid")
        period = _month(self.reference_period, "reference_period")
        object.__setattr__(self, "reference_period", period)
        title = _required_text(self.title, "title")
        subtitle = _required_text(self.subtitle, "subtitle")
        match = _SUBTITLE_RE.fullmatch(subtitle)
        if match is None or match.group("stage").casefold() != self.stage.value:
            raise ValueError("INSEE CPI subtitle stage differs")
        if _period_from_subtitle(subtitle) != period:
            raise ValueError("INSEE CPI subtitle period differs")
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "subtitle", subtitle)
        expected_uri = (
            f"{INSEE_CPI_RELEASE_URI_PREFIX}{self.source_document_id}"
        )
        if _official_uri(self.source_uri) != expected_uri:
            raise ValueError("INSEE CPI release URI differs")
        utc = _timestamp(self.published_at_utc, "published_at_utc")
        local = _required_text(self.published_at_local, "published_at_local")
        parsed_utc = datetime.fromisoformat(utc.replace("Z", "+00:00"))
        expected_local = parsed_utc.astimezone(
            ZoneInfo(INSEE_CPI_SOURCE_TIMEZONE)
        ).isoformat()
        if local != expected_local:
            raise ValueError("INSEE CPI local publication timestamp differs")
        if (
            not isinstance(self.artifact, InseeCpiArtifactV1)
            or self.artifact.role is not InseeCpiArtifactRole.RELEASE_HTML
        ):
            raise TypeError("INSEE CPI release artifact is invalid")
        if self.artifact.source_uri != expected_uri:
            raise ValueError("INSEE CPI release artifact URI differs")
        values = tuple(self.values)
        keys = {(item.measure, item.change_basis) for item in values}
        if (
            len(keys) != len(values)
            or ("cpi-all-items", "year-on-year") not in keys
            or ("cpi-all-items", "month-on-month") not in keys
        ):
            raise ValueError("INSEE CPI release headline values differ")
        payload = self.payload()
        expected = _stable_id("insee-cpi-release", payload)
        if self.release_id and self.release_id != expected:
            raise ValueError("INSEE CPI release identity differs")
        object.__setattr__(self, "values", values)
        object.__setattr__(self, "release_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_document_id": self.source_document_id,
            "stage": self.stage.value,
            "reference_period": self.reference_period,
            "title": self.title,
            "subtitle": self.subtitle,
            "source_uri": self.source_uri,
            "published_at_utc": self.published_at_utc,
            "published_at_local": self.published_at_local,
            "artifact": self.artifact.to_dict(),
            "values": [item.to_dict() for item in self.values],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiReleaseV1:
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            stage=InseeCpiReleaseStage(str(data.get("stage", ""))),
            reference_period=str(data.get("reference_period", "")),
            title=str(data.get("title", "")),
            subtitle=str(data.get("subtitle", "")),
            source_uri=str(data.get("source_uri", "")),
            published_at_utc=str(data.get("published_at_utc", "")),
            published_at_local=str(data.get("published_at_local", "")),
            artifact=InseeCpiArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            values=tuple(
                InseeCpiPublishedValueV1.from_dict(_mapping(item, "value"))
                for item in _sequence(data.get("values"), "values")
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeCpiSearchV1:
    """Complete selected/excluded accounting for one Solr response."""

    as_of_date: str
    artifact: InseeCpiArtifactV1
    total_result_count: int
    selected_document_ids: tuple[int, ...]
    exclusions: tuple[InseeCpiExcludedSearchResultV1, ...]
    schema_version: str = INSEE_CPI_SEARCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_SEARCH_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI search schema")
        _iso_date(self.as_of_date, "as_of_date")
        if self.artifact.role is not InseeCpiArtifactRole.RELEASE_SEARCH:
            raise ValueError("INSEE CPI search artifact role differs")
        total = _bounded_int(
            self.total_result_count, "total_result_count", 10_000
        )
        selected = tuple(self.selected_document_ids)
        excluded = tuple(self.exclusions)
        if (
            not selected
            or len(selected) > MAX_INSEE_CPI_RELEASES
            or len(excluded) > MAX_INSEE_CPI_EXCLUSIONS
        ):
            raise ValueError("INSEE CPI search accounting exceeds bounds")
        if len(set(selected)) != len(selected):
            raise ValueError("INSEE CPI selected IDs are not unique")
        if total != len(selected) + len(excluded):
            raise ValueError("INSEE CPI selected/excluded accounting differs")
        if set(selected) & {item.source_document_id for item in excluded}:
            raise ValueError("INSEE CPI selected and excluded IDs overlap")
        object.__setattr__(self, "selected_document_ids", selected)
        object.__setattr__(self, "exclusions", excluded)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "artifact": self.artifact.to_dict(),
            "total_result_count": self.total_result_count,
            "selected_document_ids": list(self.selected_document_ids),
            "exclusions": [item.to_dict() for item in self.exclusions],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiSearchV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifact=InseeCpiArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            total_result_count=cast(int, data.get("total_result_count")),
            selected_document_ids=tuple(
                cast(int, item)
                for item in _sequence(
                    data.get("selected_document_ids"), "selected_document_ids"
                )
            ),
            exclusions=tuple(
                InseeCpiExcludedSearchResultV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeCpiCurrentObservationV1:
    """One latest-revised BDM observation, never a historical vintage."""

    reference_period: str
    lexical: str
    value: float
    status: str
    qualifier: str
    observation_type: str
    schema_version: str = INSEE_CPI_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI observation schema")
        _month(self.reference_period, "reference_period")
        _, parsed = _numeric(self.lexical, "lexical")
        if not math.isclose(parsed, float(self.value), abs_tol=1e-12):
            raise ValueError("INSEE CPI observation lexical differs")
        for name in ("status", "qualifier", "observation_type"):
            _required_text(getattr(self, name), name)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "lexical": self.lexical,
            "value": self.value,
            "status": self.status,
            "qualifier": self.qualifier,
            "observation_type": self.observation_type,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiCurrentObservationV1:
        numeric = data.get("value")
        if isinstance(numeric, bool) or not isinstance(numeric, (int, float)):
            raise TypeError("INSEE CPI current observation must be numeric")
        return cls(
            reference_period=str(data.get("reference_period", "")),
            lexical=str(data.get("lexical", "")),
            value=float(numeric),
            status=str(data.get("status", "")),
            qualifier=str(data.get("qualifier", "")),
            observation_type=str(data.get("observation_type", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeCpiCurrentSeriesV1:
    """One current revised BDM series and all bounded monthly observations."""

    series_id: str
    measure: str
    change_basis: str
    title_en: str
    title_fr: str
    last_update: str
    unit_measure: str
    decimals: int
    artifact: InseeCpiArtifactV1
    observations: tuple[InseeCpiCurrentObservationV1, ...]
    schema_version: str = INSEE_CPI_SERIES_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_SERIES_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI series schema")
        if self.series_id not in _SERIES or _SERIES[self.series_id] != (
            self.measure,
            self.change_basis,
        ):
            raise ValueError("INSEE CPI series semantic binding differs")
        _required_text(self.title_en, "title_en")
        _required_text(self.title_fr, "title_fr")
        _iso_date(self.last_update, "last_update")
        _required_text(self.unit_measure, "unit_measure")
        _bounded_int(self.decimals, "decimals", 12)
        if self.artifact.role is not InseeCpiArtifactRole.CURRENT_SDMX:
            raise ValueError("INSEE CPI series artifact role differs")
        observations = tuple(self.observations)
        periods = tuple(item.reference_period for item in observations)
        if periods != _month_range(
            INSEE_CPI_CURRENT_START_PERIOD, INSEE_CPI_LATEST_PACKAGED_PERIOD
        ):
            raise ValueError("INSEE CPI current-series coverage differs")
        object.__setattr__(self, "observations", observations)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "series_id": self.series_id,
            "measure": self.measure,
            "change_basis": self.change_basis,
            "title_en": self.title_en,
            "title_fr": self.title_fr,
            "last_update": self.last_update,
            "unit_measure": self.unit_measure,
            "decimals": self.decimals,
            "artifact": self.artifact.to_dict(),
            "observations": [item.to_dict() for item in self.observations],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiCurrentSeriesV1:
        return cls(
            series_id=str(data.get("series_id", "")),
            measure=str(data.get("measure", "")),
            change_basis=str(data.get("change_basis", "")),
            title_en=str(data.get("title_en", "")),
            title_fr=str(data.get("title_fr", "")),
            last_update=str(data.get("last_update", "")),
            unit_measure=str(data.get("unit_measure", "")),
            decimals=cast(int, data.get("decimals")),
            artifact=InseeCpiArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                InseeCpiCurrentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeCpiRevisionV1:
    """One directly comparable provisional-to-final headline revision."""

    reference_period: str
    measure: str
    change_basis: str
    provisional_value: float
    final_value: float
    delta: float
    schema_version: str = INSEE_CPI_REVISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_REVISION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI revision schema")
        _month(self.reference_period, "reference_period")
        if self.measure not in {"cpi-all-items", "hicp-all-items"}:
            raise ValueError("INSEE CPI revision measure is invalid")
        if self.change_basis not in {"month-on-month", "year-on-year"}:
            raise ValueError("INSEE CPI revision basis is invalid")
        expected = round(
            float(self.final_value) - float(self.provisional_value), 12
        )
        if not math.isclose(expected, float(self.delta), abs_tol=1e-12):
            raise ValueError("INSEE CPI revision delta differs")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "measure": self.measure,
            "change_basis": self.change_basis,
            "provisional_value": self.provisional_value,
            "final_value": self.final_value,
            "delta": self.delta,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiRevisionV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            measure=str(data.get("measure", "")),
            change_basis=str(data.get("change_basis", "")),
            provisional_value=float(cast(float, data.get("provisional_value"))),
            final_value=float(cast(float, data.get("final_value"))),
            delta=float(cast(float, data.get("delta"))),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class _HtmlTable:
    table_id: str
    caption: str
    rows: tuple[tuple[str, ...], ...]


class _ReleaseHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []
        self.paragraphs: list[str] = []
        self.tables: list[_HtmlTable] = []
        self._table_id: str | None = None
        self._caption: list[str] | None = None
        self._rows: list[tuple[str, ...]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._paragraph: list[str] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = dict(attrs)
        if tag == "table":
            self._table_id = values.get("id") or "<none>"
            self._rows = []
        elif self._table_id is not None and tag == "caption":
            self._caption = []
        elif self._table_id is not None and tag == "tr":
            self._row = []
        elif self._row is not None and tag in {"th", "td"}:
            self._cell = []
        if tag == "p" and "paragraphe" in (values.get("class") or "").split():
            self._paragraph = []

    def handle_endtag(self, tag: str) -> None:
        if (
            tag in {"th", "td"}
            and self._cell is not None
            and self._row is not None
        ):
            self._row.append(_clean(" ".join(self._cell)))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            while self._row and not self._row[-1]:
                self._row.pop()
            if self._row:
                self._rows.append(tuple(self._row))
            self._row = None
        elif tag == "caption" and self._caption is not None:
            self._caption = [_clean(" ".join(self._caption))]
        elif tag == "table" and self._table_id is not None:
            caption = self._caption[0] if self._caption else ""
            self.tables.append(
                _HtmlTable(self._table_id, caption, tuple(self._rows))
            )
            self._table_id = None
            self._caption = None
            self._rows = []
        if tag == "p" and self._paragraph is not None:
            value = _clean(" ".join(self._paragraph))
            if value:
                self.paragraphs.append(value)
            self._paragraph = None

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.text.append(data)
            if self._caption is not None:
                self._caption.append(data)
            if self._cell is not None:
                self._cell.append(data)
            if self._paragraph is not None:
                self._paragraph.append(data)


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", value.replace("\xa0", " ")).strip()


def _parse_html(content: bytes) -> _ReleaseHtmlParser:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("INSEE CPI release is not UTF-8") from exc
    parser = _ReleaseHtmlParser()
    parser.feed(text)
    parser.close()
    return parser


def _period_from_subtitle(subtitle: str) -> str:
    match = _SUBTITLE_RE.fullmatch(_required_text(subtitle, "subtitle"))
    if match is None:
        raise ValueError("INSEE CPI subtitle is outside the selected lineage")
    month_number = _MONTHS.get(match.group("month").casefold())
    if month_number is None:
        raise ValueError("INSEE CPI subtitle month is invalid")
    return f"{int(match.group('year')):04d}-{month_number:02d}"


def _value(
    measure: str,
    change_basis: str,
    lexical: str,
    locator: str,
    *,
    sign: int | None = None,
) -> InseeCpiPublishedValueV1:
    source, numeric = _numeric(lexical, "release value")
    if sign is not None:
        numeric = abs(numeric) * sign
        source = format(numeric, ".12g")
    return InseeCpiPublishedValueV1(
        measure=measure,
        change_basis=change_basis,
        lexical=source,
        value=numeric,
        source_locator=locator,
    )


def _table_values(
    parser: _ReleaseHtmlParser,
    stage: InseeCpiReleaseStage,
    reference_period: str,
) -> dict[tuple[str, str], InseeCpiPublishedValueV1]:
    result: dict[tuple[str, str], InseeCpiPublishedValueV1] = {}
    for table in parser.tables:
        if not table.rows:
            continue
        header = tuple(cell.casefold() for cell in table.rows[0])
        for row_index, row in enumerate(table.rows[1:], start=1):
            if not row:
                continue
            label = row[0].casefold()
            locator = f"table:{table.table_id}:row:{row_index}"
            if row[0] == reference_period:
                if "12-month-rate" in header and "1-month-rate" in header:
                    for column, basis in (
                        (header.index("12-month-rate"), "year-on-year"),
                        (header.index("1-month-rate"), "month-on-month"),
                    ):
                        result[("cpi-all-items", basis)] = _value(
                            "cpi-all-items",
                            basis,
                            row[column],
                            f"{locator}:column:{column}",
                        )
                elif {"ipc", "isj", "ipch"}.issubset(header):
                    for column_name, measure in (
                        ("ipc", "cpi-all-items"),
                        ("isj", "core-inflation"),
                        ("ipch", "hicp-all-items"),
                    ):
                        column = header.index(column_name)
                        result[(measure, "year-on-year")] = _value(
                            measure,
                            "year-on-year",
                            row[column],
                            f"{locator}:column:{column}",
                        )
                continue
            if len(row) < 5:
                continue
            if stage is InseeCpiReleaseStage.PROVISIONAL:
                if label.startswith("cpi - all items"):
                    result[("cpi-all-items", "year-on-year")] = _value(
                        "cpi-all-items",
                        "year-on-year",
                        row[-1],
                        f"{locator}:column:{len(row)-1}",
                    )
                elif label.startswith(
                    ("hicp - all items", "hicp** - all items")
                ):
                    result[("hicp-all-items", "year-on-year")] = _value(
                        "hicp-all-items",
                        "year-on-year",
                        row[-1],
                        f"{locator}:column:{len(row)-1}",
                    )
                continue
            if label in {"overall", "all items (00 e)", "total (00 et)"}:
                measure = "cpi-all-items"
            elif label in {
                "core inflation - all items",
                'all items "core inflation" (4022 s)',
                'total "underlying" (4022 s)',
            }:
                measure = "core-inflation"
            elif label in {
                "hicp - all items",
                "all items hicp (00 h)",
                "total hcpi (00 h)",
            }:
                measure = "hicp-all-items"
            else:
                continue
            result[(measure, "month-on-month")] = _value(
                measure,
                "month-on-month",
                row[-2],
                f"{locator}:column:{len(row)-2}",
            )
            result[(measure, "year-on-year")] = _value(
                measure,
                "year-on-year",
                row[-1],
                f"{locator}:column:{len(row)-1}",
            )
    return result


_PERCENT_TOKEN = r"(?P<value>[+\-−–—‑]?\s*\d+(?:[.,]\d+)?)\s*%"
_UP = r"(?:increase(?:d)?|rise|rose|rising|grow|grew|edge(?:d)? up|pick up|recover(?:ed)?|rebound(?:ed)?|accelerate(?:d)?|slow down|slacken|up)"
_DOWN = r"(?:decrease(?:d)?|decline(?:d)?|fall|fell|drop|dropped|down|edge(?:d)? down|went down|weakened)"


def _directed_rate(text: str) -> tuple[str, float] | None:
    normalized = _clean(text)
    patterns = (
        (1, rf"\b{_UP}\b[^%.:]{{0,80}}?(?:by|to|:)?\s*{_PERCENT_TOKEN}"),
        (-1, rf"\b{_DOWN}\b[^%.:]{{0,80}}?(?:by|to|:)?\s*{_PERCENT_TOKEN}"),
    )
    matches: list[tuple[int, int, re.Match[str]]] = []
    for sign, pattern in patterns:
        matches.extend(
            (match.start(), sign, match)
            for match in re.finditer(pattern, normalized, re.IGNORECASE)
        )
    stable = re.search(
        r"\b(?:stable|steady|unchanged|stability)\b", normalized, re.IGNORECASE
    )
    if stable is not None and (
        not matches or stable.start() < min(item[0] for item in matches)
    ):
        return "0.0", 0.0
    if matches:
        _, sign, match = min(matches, key=lambda item: item[0])
        _, numeric = _numeric(match.group("value"), "narrative rate")
        value = abs(numeric) * sign
        return format(value, ".12g"), value
    return None


def _first_contextual_rate(text: str) -> tuple[str, float] | None:
    normalized = _clean(text)
    match = re.search(_PERCENT_TOKEN, normalized)
    if match is None:
        if re.search(
            r"\b(?:stable|steady|unchanged|stability|standstill)\b",
            normalized,
            re.IGNORECASE,
        ):
            return "0.0", 0.0
        return None
    lexical, numeric = _numeric(match.group("value"), "contextual rate")
    token = match.group("value").strip()
    if token[:1] not in {"+", "-", "−", "–", "—", "‑"} and re.search(
        r"\b(?:decrease|decline|fall|fell|drop|down|edge down)\b",
        normalized[: match.start()],
        re.IGNORECASE,
    ):
        numeric = -abs(numeric)
        lexical = format(numeric, ".12g")
    return lexical, numeric


def _year_on_year_rate(text: str) -> tuple[str, float] | None:
    normalized = _clean(text)
    anchors = tuple(
        re.finditer(
            r"year\s*(?:-| )?\s*(?:on|to)\s*(?:-| )?\s*year|"
            r"annual (?:inflation|rate)|in one year",
            normalized,
            re.IGNORECASE,
        )
    )
    candidates = tuple(re.finditer(_PERCENT_TOKEN, normalized))
    if not anchors or not candidates:
        return None
    anchor = anchors[0]

    def distance(candidate: re.Match[str]) -> int:
        if candidate.end() <= anchor.start():
            return anchor.start() - candidate.end()
        if candidate.start() >= anchor.end():
            return candidate.start() - anchor.end()
        return 0

    following = tuple(
        item
        for item in candidates
        if item.start() >= anchor.end() and item.start() - anchor.end() <= 80
    )
    immediately_preceding = tuple(
        item
        for item in candidates
        if item.end() <= anchor.start() and anchor.start() - item.end() <= 5
    )
    candidate = (
        max(immediately_preceding, key=lambda item: item.end())
        if immediately_preceding
        else (
            min(following, key=lambda item: item.start())
            if following
            else min(
                candidates, key=lambda item: (distance(item), item.start())
            )
        )
    )
    if distance(candidate) > 120:
        return None
    lexical, numeric = _numeric(candidate.group("value"), "year-on-year rate")
    token = candidate.group("value").strip()
    explicit_sign = token[:1] in {"+", "-", "−", "–", "—", "‑"}
    context = normalized[
        max(0, min(anchor.start(), candidate.start()) - 15) : min(
            len(normalized), max(anchor.end(), candidate.end()) + 15
        )
    ]
    if not explicit_sign and (
        re.search(
            r"(?:decrease|decline|fall|fell|weaken)\w*\s+by",
            context,
            re.IGNORECASE,
        )
        or (
            re.search(
                r"(?:prices?|index|it|change)\b[^.;]{0,40}"
                r"(?:declin|decreas|fell|weaken)",
                context,
                re.IGNORECASE,
            )
            and " decreased at " not in context.casefold()
        )
    ):
        numeric = -abs(numeric)
        lexical = format(numeric, ".12g")
    return lexical, numeric


def _narrative_values(
    parser: _ReleaseHtmlParser,
    stage: InseeCpiReleaseStage,
) -> dict[tuple[str, str], InseeCpiPublishedValueV1]:
    result: dict[tuple[str, str], InseeCpiPublishedValueV1] = {}
    paragraphs = parser.paragraphs[:8]
    if stage is InseeCpiReleaseStage.PROVISIONAL:
        for index, paragraph in enumerate(paragraphs):
            lower = paragraph.casefold()
            if "harmonised index" in lower:
                continue
            if "month-on-month" in lower or re.match(r"over one month", lower):
                parsed = _first_contextual_rate(paragraph)
                if parsed is not None:
                    result[("cpi-all-items", "month-on-month")] = _value(
                        "cpi-all-items",
                        "month-on-month",
                        parsed[0],
                        f"paragraph:{index}:month-on-month",
                    )
                    break
        return result

    for index, paragraph in enumerate(paragraphs):
        lower = paragraph.casefold()
        if "consumer price" not in lower:
            continue
        parsed = _directed_rate(paragraph[:700])
        if parsed is not None:
            result[("cpi-all-items", "month-on-month")] = _value(
                "cpi-all-items",
                "month-on-month",
                parsed[0],
                f"paragraph:{index}:headline",
            )
            break
    for index, paragraph in enumerate(paragraphs):
        lower = paragraph.casefold()
        if not any(
            token in lower
            for token in (
                "year-on-year",
                "year on year",
                "year to year",
                "annual inflation",
                "annual rate",
                "in one year",
            )
        ):
            continue
        parsed = _year_on_year_rate(paragraph)
        if parsed is not None:
            result[("cpi-all-items", "year-on-year")] = _value(
                "cpi-all-items",
                "year-on-year",
                parsed[0],
                f"paragraph:{index}:year-on-year",
            )
            return result
    return result


def parse_insee_cpi_search(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> tuple[InseeCpiSearchV1, tuple[Mapping[str, Any], ...]]:
    """Classify every Solr result and retain selected source metadata."""
    boundary = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_SEARCH_BODY)
    ):
        raise ValueError("INSEE CPI search request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("INSEE CPI search JSON is invalid") from exc
    root = _mapping(payload, "search")
    documents = tuple(
        _mapping(item, "search document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _bounded_int(root.get("numFounds"), "numFounds", 10_000)
    if total != len(documents):
        raise ValueError("INSEE CPI Solr response is truncated")
    selected: list[Mapping[str, Any]] = []
    exclusions: list[InseeCpiExcludedSearchResultV1] = []
    seen: set[int] = set()
    for document in documents:
        document_id = _bounded_int(
            document.get("id"), "document id", 1_000_000_000
        )
        if document_id in seen:
            raise ValueError("INSEE CPI Solr response repeats a document")
        seen.add(document_id)
        title = _required_text(document.get("titre"), "title")
        raw_subtitle = document.get("sousTitre")
        subtitle = (
            None
            if raw_subtitle is None or not str(raw_subtitle).strip()
            else _required_text(raw_subtitle, "subtitle")
        )
        match = _SUBTITLE_RE.fullmatch(subtitle or "")
        if match is not None:
            published = _timestamp(
                document.get("dateDiffusion"), "dateDiffusion"
            )
            if (
                datetime.fromisoformat(published.replace("Z", "+00:00"))
                .date()
                .isoformat()
                <= boundary
            ):
                selected.append(document)
                continue
        reason = (
            "missing-subtitle"
            if subtitle is None
            else (
                "other-consumer-price-publication"
                if subtitle.casefold().startswith("consumer price index")
                else "search-result-outside-release-lineage"
            )
        )
        exclusions.append(
            InseeCpiExcludedSearchResultV1(document_id, title, subtitle, reason)
        )
    selected.sort(
        key=lambda item: (
            _period_from_subtitle(str(item["sousTitre"])),
            0 if "provisional" in str(item["sousTitre"]).casefold() else 1,
        )
    )
    search = InseeCpiSearchV1(
        as_of_date=boundary,
        artifact=_artifact(snapshot, InseeCpiArtifactRole.RELEASE_SEARCH),
        total_result_count=total,
        selected_document_ids=tuple(cast(int, item["id"]) for item in selected),
        exclusions=tuple(
            sorted(exclusions, key=lambda item: item.source_document_id)
        ),
    )
    return search, tuple(selected)


def build_insee_cpi_release_requests(
    registry: OfficialSourceRegistryV1,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected release document."""
    return tuple(
        _request(
            registry,
            f"{INSEE_CPI_RELEASE_URI_PREFIX}{_bounded_int(item.get('id'), 'document id', 100_000_000)}",
            OfficialSourceFormat.HTML,
        )
        for item in documents
    )


def _parse_release(
    document: Mapping[str, Any], snapshot: OfficialRawSnapshotV1
) -> InseeCpiReleaseV1:
    document_id = _bounded_int(document.get("id"), "document id", 100_000_000)
    title = _required_text(document.get("titre"), "title")
    subtitle = _required_text(document.get("sousTitre"), "subtitle")
    match = _SUBTITLE_RE.fullmatch(subtitle)
    if match is None:
        raise ValueError("INSEE CPI selected subtitle differs")
    stage = InseeCpiReleaseStage(match.group("stage").casefold())
    period = _period_from_subtitle(subtitle)
    parser = _parse_html(snapshot.content)
    page_text = _clean(" ".join(parser.text))
    if title not in page_text or subtitle not in page_text:
        raise ValueError(f"INSEE CPI page metadata differs: {document_id}")
    values = _table_values(parser, stage, period)
    headline_keys = {
        ("cpi-all-items", "month-on-month"),
        ("cpi-all-items", "year-on-year"),
    }
    if not headline_keys.issubset(values):
        for key, value in _narrative_values(parser, stage).items():
            values.setdefault(key, value)
    if ("cpi-all-items", "month-on-month") not in values or (
        "cpi-all-items",
        "year-on-year",
    ) not in values:
        raise ValueError(f"INSEE CPI page omits headline values: {document_id}")
    published = _timestamp(document.get("dateDiffusion"), "dateDiffusion")
    parsed = datetime.fromisoformat(published.replace("Z", "+00:00"))
    return InseeCpiReleaseV1(
        source_document_id=document_id,
        stage=stage,
        reference_period=period,
        title=title,
        subtitle=subtitle,
        source_uri=f"{INSEE_CPI_RELEASE_URI_PREFIX}{document_id}",
        published_at_utc=published,
        published_at_local=parsed.astimezone(
            ZoneInfo(INSEE_CPI_SOURCE_TIMEZONE)
        ).isoformat(),
        artifact=_artifact(snapshot, InseeCpiArtifactRole.RELEASE_HTML),
        values=tuple(values[key] for key in sorted(values)),
    )


def _catalog_ids(snapshot: OfficialRawSnapshotV1) -> set[str]:
    try:
        root = _mapping(json.loads(snapshot.content), "series catalog")
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("INSEE CPI series catalog JSON is invalid") from exc
    return {
        _required_text(
            _mapping(item, "series metadata").get("idBank"), "idBank"
        )
        for item in _sequence(root.get("documents"), "documents")
        if _mapping(item, "series metadata").get("idBank") is not None
    }


def _parse_current_series(
    snapshot: OfficialRawSnapshotV1,
) -> InseeCpiCurrentSeriesV1:
    series_id = urlsplit(snapshot.request.uri).path.rsplit("/", 1)[-1]
    if series_id not in _SERIES:
        raise ValueError("INSEE CPI current-series ID differs")
    try:
        root = ET.fromstring(snapshot.content)
    except ET.ParseError as exc:
        raise ValueError("INSEE CPI SDMX XML is invalid") from exc
    series_nodes = [node for node in root.iter() if node.tag.endswith("Series")]
    if (
        len(series_nodes) != 1
        or series_nodes[0].attrib.get("IDBANK") != series_id
    ):
        raise ValueError("INSEE CPI SDMX series identity differs")
    metadata = series_nodes[0].attrib
    observations: dict[str, InseeCpiCurrentObservationV1] = {}
    for node in root.iter():
        if not node.tag.endswith("Obs"):
            continue
        period = _month(node.attrib.get("TIME_PERIOD"), "TIME_PERIOD")
        lexical, numeric = _numeric(node.attrib.get("OBS_VALUE"), "OBS_VALUE")
        observations[period] = InseeCpiCurrentObservationV1(
            reference_period=period,
            lexical=lexical,
            value=numeric,
            status=_required_text(node.attrib.get("OBS_STATUS"), "OBS_STATUS"),
            qualifier=_required_text(node.attrib.get("OBS_QUAL"), "OBS_QUAL"),
            observation_type=_required_text(
                node.attrib.get("OBS_TYPE"), "OBS_TYPE"
            ),
        )
    measure, basis = _SERIES[series_id]
    return InseeCpiCurrentSeriesV1(
        series_id=series_id,
        measure=measure,
        change_basis=basis,
        title_en=_required_text(metadata.get("TITLE_EN"), "TITLE_EN"),
        title_fr=_required_text(metadata.get("TITLE_FR"), "TITLE_FR"),
        last_update=_iso_date(metadata.get("LAST_UPDATE"), "LAST_UPDATE"),
        unit_measure=_required_text(
            metadata.get("UNIT_MEASURE"), "UNIT_MEASURE"
        ),
        decimals=int(_required_text(metadata.get("DECIMALS"), "DECIMALS")),
        artifact=_artifact(snapshot, InseeCpiArtifactRole.CURRENT_SDMX),
        observations=tuple(
            observations[period] for period in sorted(observations)
        ),
    )


def _revisions(
    releases: Sequence[InseeCpiReleaseV1],
) -> tuple[InseeCpiRevisionV1, ...]:
    by_key = {(item.reference_period, item.stage): item for item in releases}
    result: list[InseeCpiRevisionV1] = []
    for period in _month_range(
        INSEE_CPI_PROVISIONAL_START_PERIOD, INSEE_CPI_LATEST_PACKAGED_PERIOD
    ):
        provisional = by_key[(period, InseeCpiReleaseStage.PROVISIONAL)]
        final = by_key[(period, InseeCpiReleaseStage.FINAL)]
        provisional_values = {
            (item.measure, item.change_basis): item.value
            for item in provisional.values
        }
        final_values = {
            (item.measure, item.change_basis): item.value
            for item in final.values
        }
        for measure, basis in sorted(
            set(provisional_values) & set(final_values)
        ):
            if measure == "core-inflation":
                continue
            before = provisional_values[(measure, basis)]
            after = final_values[(measure, basis)]
            result.append(
                InseeCpiRevisionV1(
                    period,
                    measure,
                    basis,
                    before,
                    after,
                    round(after - before, 12),
                )
            )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class InseeCpiArchiveManifestV1:
    """Complete, replayable INSEE CPI release and current-series manifest."""

    registry_id: str
    source_id: str
    search: InseeCpiSearchV1
    catalog_artifacts: tuple[InseeCpiArtifactV1, ...]
    releases: tuple[InseeCpiReleaseV1, ...]
    current_series: tuple[InseeCpiCurrentSeriesV1, ...]
    revisions: tuple[InseeCpiRevisionV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    released_value_count: int
    nonzero_revision_count: int
    limitations: tuple[str, ...]
    manifest_id: str = ""
    schema_version: str = INSEE_CPI_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_CPI_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE CPI manifest schema")
        if (
            re.fullmatch(
                r"official-source-registry:sha256:[0-9a-f]{64}",
                self.registry_id,
            )
            is None
            or re.fullmatch(
                r"official-source:sha256:[0-9a-f]{64}", self.source_id
            )
            is None
        ):
            raise ValueError("INSEE CPI manifest registry binding is invalid")
        releases = tuple(self.releases)
        final_periods = tuple(
            item.reference_period
            for item in releases
            if item.stage is InseeCpiReleaseStage.FINAL
        )
        provisional_periods = tuple(
            item.reference_period
            for item in releases
            if item.stage is InseeCpiReleaseStage.PROVISIONAL
        )
        if final_periods != _month_range(
            INSEE_CPI_FINAL_START_PERIOD, INSEE_CPI_LATEST_PACKAGED_PERIOD
        ):
            raise ValueError("INSEE CPI final-release coverage differs")
        if provisional_periods != _month_range(
            INSEE_CPI_PROVISIONAL_START_PERIOD, INSEE_CPI_LATEST_PACKAGED_PERIOD
        ):
            raise ValueError("INSEE CPI provisional-release coverage differs")
        if (
            tuple(item.source_document_id for item in releases)
            != self.search.selected_document_ids
        ):
            raise ValueError("INSEE CPI release/search order differs")
        if {item.series_id for item in self.current_series} != set(_SERIES):
            raise ValueError("INSEE CPI current-series set differs")
        expected_revisions = _revisions(releases)
        if tuple(self.revisions) != expected_revisions:
            raise ValueError("INSEE CPI revision accounting differs")
        artifacts = [
            self.search.artifact,
            *self.catalog_artifacts,
            *(item.artifact for item in releases),
            *(item.artifact for item in self.current_series),
        ]
        by_request = {item.request_id: item for item in artifacts}
        if (
            len(by_request) != len(artifacts)
            or len(artifacts) > MAX_INSEE_CPI_ARTIFACTS
        ):
            raise ValueError("INSEE CPI artifact identity accounting differs")
        calculated = {
            "raw_artifact_count": len(artifacts),
            "unique_content_sha256_count": len(
                {item.content_sha256 for item in artifacts}
            ),
            "total_content_bytes": sum(
                item.content_length for item in artifacts
            ),
            "released_value_count": sum(len(item.values) for item in releases),
            "nonzero_revision_count": sum(
                not math.isclose(item.delta, 0.0, abs_tol=1e-12)
                for item in self.revisions
            ),
        }
        for name, value in calculated.items():
            if getattr(self, name) != value:
                raise ValueError(f"INSEE CPI {name} differs")
        if (
            self.total_content_bytes > MAX_INSEE_CPI_TOTAL_BYTES
            or not self.limitations
        ):
            raise ValueError("INSEE CPI manifest bounds or limitations differ")
        expected = _stable_id("insee-cpi-archive-manifest", self.payload())
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("INSEE CPI manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "program_key": INSEE_CPI_PROGRAM_KEY,
            "frequency": "monthly",
            "search": self.search.to_dict(),
            "catalog_artifacts": [
                item.to_dict() for item in self.catalog_artifacts
            ],
            "releases": [item.to_dict() for item in self.releases],
            "current_series": [item.to_dict() for item in self.current_series],
            "revisions": [item.to_dict() for item in self.revisions],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "released_value_count": self.released_value_count,
            "nonzero_revision_count": self.nonzero_revision_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeCpiArchiveManifestV1:
        if (
            data.get("program_key") != INSEE_CPI_PROGRAM_KEY
            or data.get("frequency") != "monthly"
        ):
            raise ValueError("INSEE CPI manifest program metadata differs")
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            search=InseeCpiSearchV1.from_dict(
                _mapping(data.get("search"), "search")
            ),
            catalog_artifacts=tuple(
                InseeCpiArtifactV1.from_dict(_mapping(item, "catalog artifact"))
                for item in _sequence(
                    data.get("catalog_artifacts"), "catalog_artifacts"
                )
            ),
            releases=tuple(
                InseeCpiReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            current_series=tuple(
                InseeCpiCurrentSeriesV1.from_dict(_mapping(item, "series"))
                for item in _sequence(
                    data.get("current_series"), "current_series"
                )
            ),
            revisions=tuple(
                InseeCpiRevisionV1.from_dict(_mapping(item, "revision"))
                for item in _sequence(data.get("revisions"), "revisions")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            released_value_count=cast(int, data.get("released_value_count")),
            nonzero_revision_count=cast(
                int, data.get("nonzero_revision_count")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> InseeCpiArchiveManifestV1:
        try:
            data = json.loads(payload)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("INSEE CPI manifest JSON is invalid") from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_insee_cpi_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search: InseeCpiSearchV1,
    documents: Sequence[Mapping[str, Any]],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshots: Sequence[OfficialRawSnapshotV1],
    current_series_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> InseeCpiArchiveManifestV1:
    """Build the complete normalized archive from retained official bytes."""
    source = _source(registry)
    selected = tuple(cast(int, item.get("id")) for item in documents)
    if selected != search.selected_document_ids or set(
        release_snapshots
    ) != set(selected):
        raise ValueError("INSEE CPI retained release coverage differs")
    catalogs = tuple(catalog_snapshots)
    if len(catalogs) != 2:
        raise ValueError("INSEE CPI catalog snapshot count differs")
    cpi_ids, hicp_ids = (_catalog_ids(item) for item in catalogs)
    if (
        not {"011814631", "011814632"}.issubset(cpi_ids)
        or "011812232" not in hicp_ids
    ):
        raise ValueError(
            "INSEE CPI current-series catalog omits a selected series"
        )
    if set(current_series_snapshots) != set(_SERIES):
        raise ValueError("INSEE CPI retained current-series coverage differs")
    releases = tuple(
        _parse_release(document, release_snapshots[cast(int, document["id"])])
        for document in documents
    )
    current_series = tuple(
        _parse_current_series(current_series_snapshots[series_id])
        for series_id in _SERIES
    )
    catalog_artifacts = (
        _artifact(catalogs[0], InseeCpiArtifactRole.CPI_SERIES_CATALOG),
        _artifact(catalogs[1], InseeCpiArtifactRole.HICP_SERIES_CATALOG),
    )
    artifacts = [
        search.artifact,
        *catalog_artifacts,
        *(item.artifact for item in releases),
        *(item.artifact for item in current_series),
    ]
    revisions = _revisions(releases)
    return InseeCpiArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        search=search,
        catalog_artifacts=catalog_artifacts,
        releases=releases,
        current_series=current_series,
        revisions=revisions,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        released_value_count=sum(len(item.values) for item in releases),
        nonzero_revision_count=sum(
            not math.isclose(item.delta, 0.0, abs_tol=1e-12)
            for item in revisions
        ),
        limitations=(
            "The official English release-page corpus begins with final May 2009 and provisional January 2016; no earlier release occurrence or value is fabricated.",
            "BDM SDMX observations are latest revised values and are retained only as explicit cross-checks, never as first-published vintages.",
            "Provisional-to-final deltas compare only the same reference month, measure, and change basis; component values and market consensus are not inferred.",
            "The Solr dateDiffusion/embargo timestamp is retained exactly, including three observed sub-minute update timestamps that must not be silently rounded to a scheduled release time.",
        ),
    )


def replay_insee_cpi_archive(
    registry: OfficialSourceRegistryV1,
    manifest: InseeCpiArchiveManifestV1,
    search_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshots: Sequence[OfficialRawSnapshotV1],
    current_series_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> InseeCpiArchiveManifestV1:
    """Rebuild and byte-compare one retained archive manifest offline."""
    if manifest.registry_id != registry.registry_id:
        raise ValueError("INSEE CPI replay registry identity differs")
    search, documents = parse_insee_cpi_search(
        search_snapshot, as_of_date=manifest.search.as_of_date
    )
    if search != manifest.search:
        raise ValueError("INSEE CPI replay search inventory differs")
    rebuilt = build_insee_cpi_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshots,
        current_series_snapshots,
    )
    if rebuilt.to_json() != manifest.to_json():
        raise ValueError("INSEE CPI replay differs from retained manifest")
    return rebuilt


def packaged_insee_cpi_manifest_path() -> Path:
    """Return the packaged INSEE CPI manifest path."""
    return Path(__file__).with_name("assets") / "insee_cpi_archive_v1.json"


def load_packaged_insee_cpi_archive_manifest() -> InseeCpiArchiveManifestV1:
    """Load and fully validate the packaged INSEE CPI archive."""
    return InseeCpiArchiveManifestV1.from_json(
        packaged_insee_cpi_manifest_path().read_text(encoding="utf-8")
    )


__all__ = [
    "INSEE_CPI_CURRENT_START_PERIOD",
    "INSEE_CPI_FINAL_START_PERIOD",
    "INSEE_CPI_LATEST_PACKAGED_PERIOD",
    "INSEE_CPI_PARSER_ID",
    "INSEE_CPI_PARSER_VERSION",
    "INSEE_CPI_PROGRAM_KEY",
    "INSEE_CPI_PROVISIONAL_START_PERIOD",
    "INSEE_CPI_SEARCH_URI",
    "INSEE_CPI_SOURCE_KEY",
    "InseeCpiArchiveManifestV1",
    "InseeCpiArtifactRole",
    "InseeCpiArtifactV1",
    "InseeCpiCurrentObservationV1",
    "InseeCpiCurrentSeriesV1",
    "InseeCpiExcludedSearchResultV1",
    "InseeCpiPublishedValueV1",
    "InseeCpiReleaseStage",
    "InseeCpiReleaseV1",
    "InseeCpiRevisionV1",
    "InseeCpiSearchV1",
    "build_insee_cpi_archive_manifest",
    "build_insee_cpi_catalog_requests",
    "build_insee_cpi_current_series_requests",
    "build_insee_cpi_release_requests",
    "build_insee_cpi_search_request",
    "load_packaged_insee_cpi_archive_manifest",
    "packaged_insee_cpi_manifest_path",
    "parse_insee_cpi_search",
    "replay_insee_cpi_archive",
]

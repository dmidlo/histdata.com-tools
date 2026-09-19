"""Deterministic qualification of INSEE quarterly GDP release evidence.

The archive retains one bounded English Solr inventory, the paired quarterly
``first estimate`` and ``detailed figures`` release pages, and one current
revised BDM SDMX series.  Release values remain source-dated occurrences;
the BDM series is only a labelled latest-state cross-check.
"""

from __future__ import annotations

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

INSEE_GDP_SOURCE_KEY: Final = "fr.insee.quarterly-gdp"
INSEE_GDP_PROGRAM_KEY: Final = "fr.insee.quarterly-national-accounts"
INSEE_GDP_PARSER_ID: Final = "official.insee-quarterly-gdp.v1"
INSEE_GDP_PARSER_VERSION: Final = "1"
INSEE_GDP_SOURCE_TIMEZONE: Final = "Europe/Paris"
INSEE_GDP_SEARCH_URI: Final = "https://www.insee.fr/en/solr/consultation"
INSEE_GDP_RELEASE_URI_PREFIX: Final = "https://www.insee.fr/en/statistiques/"
INSEE_GDP_CURRENT_SERIES_URI: Final = (
    "https://bdm.insee.fr/series/sdmx/data/CNT-2020-PIB-EQB-RF"
    "?startPeriod=2000-Q1&endPeriod=2026-Q2"
)
INSEE_GDP_CURRENT_SERIES_ID: Final = "011794844"
INSEE_GDP_RELEASE_START_PERIOD: Final = "2016-Q3"
INSEE_GDP_CURRENT_START_PERIOD: Final = "2000-Q1"
INSEE_GDP_LATEST_PACKAGED_PERIOD: Final = "2026-Q2"

INSEE_GDP_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.insee-gdp-artifact.v1"
INSEE_GDP_EXCLUSION_SCHEMA_VERSION: Final = "histdatacom.insee-gdp-exclusion.v1"
INSEE_GDP_SEARCH_SCHEMA_VERSION: Final = "histdatacom.insee-gdp-search.v1"
INSEE_GDP_VALUE_SCHEMA_VERSION: Final = "histdatacom.insee-gdp-value.v1"
INSEE_GDP_RELEASE_SCHEMA_VERSION: Final = "histdatacom.insee-gdp-release.v1"
INSEE_GDP_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-gdp-current-observation.v1"
)
INSEE_GDP_SERIES_SCHEMA_VERSION: Final = (
    "histdatacom.insee-gdp-current-series.v1"
)
INSEE_GDP_REVISION_SCHEMA_VERSION: Final = "histdatacom.insee-gdp-revision.v1"
INSEE_GDP_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.insee-gdp-archive-manifest.v1"
)

MAX_INSEE_GDP_RESULTS: Final = 1_000
MAX_INSEE_GDP_ARTIFACT_BYTES: Final = 64 * 1024 * 1024
MAX_INSEE_GDP_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SPACE_RE = re.compile(r"\s+")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"^[+\-]?\d+(?:[.,]\d+)?$")
_QUARTER_RE = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>[1-4])$")
_SUBTITLE_RE = re.compile(
    r"^Quarterly national accounts - "
    r"(?P<stage>first estimate|detailed figures) - "
    r"(?P<quarter>first|second|third|fourth) quarter "
    r"(?P<year>\d{4})$",
    re.IGNORECASE,
)
_QUARTER_NUMBER: Final = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
}
_SEARCH_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsQuery": [],
    "filters": [
        {"field": "diffusion", "values": [True]},
        {"field": "rubrique", "values": ["statistiques"]},
    ],
    "q": "quarterly national accounts",
    "rows": 1000,
    "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
    "start": 0,
}


def _text(value: object, name: str) -> str:
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


def _integer(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _quarter(value: object, name: str) -> str:
    result = _text(value, name)
    if _QUARTER_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-Qn")
    return result


def _quarter_ordinal(value: str) -> int:
    match = cast(
        re.Match[str], _QUARTER_RE.fullmatch(_quarter(value, "quarter"))
    )
    return int(match.group("year")) * 4 + int(match.group("quarter")) - 1


def _quarter_range(start: str, end: str) -> tuple[str, ...]:
    first = _quarter_ordinal(start)
    last = _quarter_ordinal(end)
    if first > last:
        raise ValueError("quarter range is reversed")
    return tuple(
        f"{ordinal // 4}-Q{ordinal % 4 + 1}"
        for ordinal in range(first, last + 1)
    )


def _iso_date(value: object, name: str) -> str:
    result = _text(value, name)
    if date.fromisoformat(result).isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _timestamp(value: object, name: str) -> str:
    result = _text(value, name)
    try:
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(
        None
    ):
        raise ValueError(f"{name} must be UTC")
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _official_uri(value: object, name: str = "source_uri") -> str:
    result = _text(value, name)
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
    lexical = _text(value, name)
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
    if _NUMBER_RE.fullmatch(normalized) is None:
        raise ValueError(f"{name} is not numeric")
    numeric = float(normalized)
    if not math.isfinite(numeric) or not -100.0 <= numeric <= 100.0:
        raise ValueError(f"{name} is outside its bound")
    return lexical, numeric


def _body(payload: Mapping[str, JSONValue]) -> str:
    return str(canonical_contract_json(dict(payload)))


def _source(registry: OfficialSourceRegistryV1) -> OfficialSourceEntryV1:
    source = registry.source(INSEE_GDP_SOURCE_KEY)
    if (
        source.parser_id != INSEE_GDP_PARSER_ID
        or source.parser_version != INSEE_GDP_PARSER_VERSION
    ):
        raise ValueError("INSEE GDP registry parser binding differs")
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
    if urlsplit(_official_uri(uri)).hostname not in source.allowed_hosts:
        raise ValueError("INSEE GDP request host differs from registry")
    if source_format not in source.formats:
        raise ValueError("INSEE GDP request format differs from registry")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=method,
        uri=uri,
        source_format=source_format,
        body_text=_body(body) if body is not None else None,
        body_content_type="application/json" if body is not None else None,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def build_insee_gdp_search_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact bounded Solr request used for release discovery."""
    return _request(
        registry,
        INSEE_GDP_SEARCH_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_SEARCH_BODY,
    )


def build_insee_gdp_release_requests(
    registry: OfficialSourceRegistryV1,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected release document."""
    return tuple(
        _request(
            registry,
            f"{INSEE_GDP_RELEASE_URI_PREFIX}"
            f"{_integer(item.get('id'), 'document id', 1_000_000_000)}",
            OfficialSourceFormat.HTML,
        )
        for item in documents
    )


def build_insee_gdp_current_series_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the bounded latest-state BDM GDP series request."""
    return _request(
        registry,
        INSEE_GDP_CURRENT_SERIES_URI,
        OfficialSourceFormat.SDMX_21,
    )


class InseeGdpReleaseStage(str, Enum):
    """Publication stage retained from the official subtitle."""

    FIRST_ESTIMATE = "first-estimate"
    DETAILED_FIGURES = "detailed-figures"


class InseeGdpArtifactRole(str, Enum):
    """Semantic role of one retained official response."""

    RELEASE_SEARCH = "release-search"
    RELEASE_HTML = "release-html"
    CURRENT_SDMX = "current-sdmx"


@dataclass(frozen=True, slots=True)
class InseeGdpArtifactV1:
    """Hash-bound receipt for one exact official response."""

    role: InseeGdpArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = INSEE_GDP_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP artifact schema")
        if not isinstance(self.role, InseeGdpArtifactRole):
            raise TypeError("INSEE GDP artifact role is invalid")
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError("INSEE GDP artifact format is invalid")
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        if _DIGEST_RE.fullmatch(self.content_sha256) is None:
            raise ValueError("INSEE GDP artifact digest is invalid")
        if not 0 < self.content_length <= MAX_INSEE_GDP_ARTIFACT_BYTES:
            raise ValueError("INSEE GDP artifact length is invalid")
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError("INSEE GDP artifact request identity is invalid")

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
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpArtifactV1:
        return cls(
            role=InseeGdpArtifactRole(str(data.get("role", ""))),
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
    snapshot: OfficialRawSnapshotV1, role: InseeGdpArtifactRole
) -> InseeGdpArtifactV1:
    if (
        snapshot.request.source_key != INSEE_GDP_SOURCE_KEY
        or snapshot.request.parser_id != INSEE_GDP_PARSER_ID
        or snapshot.request.parser_version != INSEE_GDP_PARSER_VERSION
    ):
        raise ValueError("INSEE GDP snapshot names another source")
    return InseeGdpArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class InseeGdpExcludedSearchResultV1:
    """One explicitly classified but unselected Solr result."""

    source_document_id: int
    title: str
    subtitle: str | None
    reason: str
    schema_version: str = INSEE_GDP_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_EXCLUSION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP exclusion schema")
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError("INSEE GDP excluded document ID is invalid")
        object.__setattr__(self, "title", _text(self.title, "title"))
        if self.subtitle is not None:
            object.__setattr__(
                self, "subtitle", _text(self.subtitle, "subtitle")
            )
        if self.reason not in {
            "missing-subtitle",
            "outside-packaged-coverage",
            "other-quarterly-national-accounts-publication",
            "search-result-outside-release-lineage",
        }:
            raise ValueError("INSEE GDP exclusion reason is invalid")

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
    ) -> InseeGdpExcludedSearchResultV1:
        raw_subtitle = data.get("subtitle")
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            title=str(data.get("title", "")),
            subtitle=None if raw_subtitle is None else str(raw_subtitle),
            reason=str(data.get("reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeGdpSearchV1:
    """Complete accounting of the bounded release-search response."""

    as_of_date: str
    artifact: InseeGdpArtifactV1
    total_result_count: int
    selected_document_ids: tuple[int, ...]
    exclusions: tuple[InseeGdpExcludedSearchResultV1, ...]
    schema_version: str = INSEE_GDP_SEARCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_SEARCH_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP search schema")
        object.__setattr__(
            self, "as_of_date", _iso_date(self.as_of_date, "as_of_date")
        )
        if self.artifact.role is not InseeGdpArtifactRole.RELEASE_SEARCH:
            raise ValueError("INSEE GDP search artifact role differs")
        if not 0 < self.total_result_count <= MAX_INSEE_GDP_RESULTS:
            raise ValueError("INSEE GDP search result count is invalid")
        ids = tuple(self.selected_document_ids)
        if len(ids) != len(set(ids)):
            raise ValueError("INSEE GDP selected IDs are not unique")
        excluded_ids = {item.source_document_id for item in self.exclusions}
        if len(ids) + len(excluded_ids) != self.total_result_count:
            raise ValueError("INSEE GDP selected/excluded accounting differs")
        if set(ids) & excluded_ids:
            raise ValueError("INSEE GDP selected and excluded IDs overlap")

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
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpSearchV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifact=InseeGdpArtifactV1.from_dict(
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
                InseeGdpExcludedSearchResultV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeGdpPublishedValueV1:
    """One contemporaneous GDP quarter-on-quarter release value."""

    lexical_value: str
    value: float
    locator: str
    metric: str = "gdp-volume"
    change_basis: str = "quarter-on-quarter"
    unit: str = "percent"
    schema_version: str = INSEE_GDP_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP value schema")
        lexical, parsed = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if not isinstance(self.value, (float, int)) or isinstance(
            self.value, bool
        ):
            raise TypeError("INSEE GDP value must be numeric")
        if abs(float(self.value) - parsed) > 1e-12:
            raise ValueError("INSEE GDP lexical and numeric values differ")
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "locator", _text(self.locator, "locator"))
        if (self.metric, self.change_basis, self.unit) != (
            "gdp-volume",
            "quarter-on-quarter",
            "percent",
        ):
            raise ValueError("INSEE GDP value semantics differ")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric": self.metric,
            "change_basis": self.change_basis,
            "unit": self.unit,
            "lexical_value": self.lexical_value,
            "value": self.value,
            "locator": self.locator,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpPublishedValueV1:
        return cls(
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            locator=str(data.get("locator", "")),
            metric=str(data.get("metric", "")),
            change_basis=str(data.get("change_basis", "")),
            unit=str(data.get("unit", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _subtitle_parts(subtitle: str) -> tuple[InseeGdpReleaseStage, str]:
    match = _SUBTITLE_RE.fullmatch(_text(subtitle, "subtitle"))
    if match is None:
        raise ValueError("INSEE GDP subtitle is outside the selected lineage")
    stage = {
        "first estimate": InseeGdpReleaseStage.FIRST_ESTIMATE,
        "detailed figures": InseeGdpReleaseStage.DETAILED_FIGURES,
    }[match.group("stage").casefold()]
    period = f"{match.group('year')}-Q{_QUARTER_NUMBER[match.group('quarter').casefold()]}"
    return stage, period


@dataclass(frozen=True, slots=True)
class InseeGdpReleaseV1:
    """One source-dated INSEE quarterly GDP publication stage."""

    source_document_id: int
    reference_period: str
    stage: InseeGdpReleaseStage
    title: str
    subtitle: str
    published_at: str
    published_at_local: str
    source_uri: str
    artifact: InseeGdpArtifactV1
    value: InseeGdpPublishedValueV1
    schema_version: str = INSEE_GDP_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP release schema")
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError("INSEE GDP document ID is invalid")
        object.__setattr__(
            self,
            "reference_period",
            _quarter(self.reference_period, "reference_period"),
        )
        if not isinstance(self.stage, InseeGdpReleaseStage):
            raise TypeError("INSEE GDP release stage is invalid")
        expected_stage, expected_period = _subtitle_parts(self.subtitle)
        if (self.stage, self.reference_period) != (
            expected_stage,
            expected_period,
        ):
            raise ValueError("INSEE GDP subtitle stage or period differs")
        object.__setattr__(self, "title", _text(self.title, "title"))
        object.__setattr__(self, "subtitle", _text(self.subtitle, "subtitle"))
        object.__setattr__(
            self, "published_at", _timestamp(self.published_at, "published_at")
        )
        local = _text(self.published_at_local, "published_at_local")
        parsed_local = datetime.fromisoformat(local)
        parsed_utc = datetime.fromisoformat(
            self.published_at.replace("Z", "+00:00")
        )
        if (
            parsed_local.tzinfo is None
            or parsed_local.astimezone(timezone.utc) != parsed_utc
        ):
            raise ValueError("INSEE GDP local publication timestamp differs")
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        expected_uri = (
            f"{INSEE_GDP_RELEASE_URI_PREFIX}{self.source_document_id}"
        )
        if (
            self.source_uri != expected_uri
            or self.artifact.source_uri != expected_uri
        ):
            raise ValueError("INSEE GDP release URI differs")
        if self.artifact.role is not InseeGdpArtifactRole.RELEASE_HTML:
            raise ValueError("INSEE GDP release artifact role differs")

    @property
    def release_id(self) -> str:
        return _stable_id("insee-gdp-release", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_document_id": self.source_document_id,
            "reference_period": self.reference_period,
            "stage": self.stage.value,
            "title": self.title,
            "subtitle": self.subtitle,
            "published_at": self.published_at,
            "published_at_local": self.published_at_local,
            "source_uri": self.source_uri,
            "artifact": self.artifact.to_dict(),
            "value": self.value.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpReleaseV1:
        value = cls(
            source_document_id=cast(int, data.get("source_document_id")),
            reference_period=str(data.get("reference_period", "")),
            stage=InseeGdpReleaseStage(str(data.get("stage", ""))),
            title=str(data.get("title", "")),
            subtitle=str(data.get("subtitle", "")),
            published_at=str(data.get("published_at", "")),
            published_at_local=str(data.get("published_at_local", "")),
            source_uri=str(data.get("source_uri", "")),
            artifact=InseeGdpArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            value=InseeGdpPublishedValueV1.from_dict(
                _mapping(data.get("value"), "value")
            ),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("release_id") != value.release_id:
            raise ValueError("INSEE GDP release identity differs")
        return value


@dataclass(frozen=True, slots=True)
class InseeGdpCurrentObservationV1:
    """One latest-revised observation from the BDM GDP series."""

    reference_period: str
    lexical_value: str
    value: float
    status: str
    schema_version: str = INSEE_GDP_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP observation schema")
        object.__setattr__(
            self,
            "reference_period",
            _quarter(self.reference_period, "reference_period"),
        )
        lexical, numeric = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if abs(float(self.value) - numeric) > 1e-12:
            raise ValueError("INSEE GDP observation lexical differs")
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "status", _text(self.status, "status"))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "lexical_value": self.lexical_value,
            "value": self.value,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpCurrentObservationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            status=str(data.get("status", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeGdpCurrentSeriesV1:
    """Latest revised BDM series, explicitly separate from vintages."""

    series_id: str
    title: str
    last_update: str
    observations: tuple[InseeGdpCurrentObservationV1, ...]
    artifact: InseeGdpArtifactV1
    metric: str = "gdp-volume"
    change_basis: str = "quarter-on-quarter"
    unit: str = "percent"
    semantics: str = "latest-revised-cross-check"
    schema_version: str = INSEE_GDP_SERIES_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_SERIES_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP series schema")
        if self.series_id != INSEE_GDP_CURRENT_SERIES_ID:
            raise ValueError("INSEE GDP current series identity differs")
        object.__setattr__(self, "title", _text(self.title, "title"))
        object.__setattr__(
            self, "last_update", _iso_date(self.last_update, "last_update")
        )
        if (self.metric, self.change_basis, self.unit, self.semantics) != (
            "gdp-volume",
            "quarter-on-quarter",
            "percent",
            "latest-revised-cross-check",
        ):
            raise ValueError("INSEE GDP current series semantics differ")
        values = tuple(self.observations)
        if tuple(item.reference_period for item in values) != _quarter_range(
            INSEE_GDP_CURRENT_START_PERIOD, INSEE_GDP_LATEST_PACKAGED_PERIOD
        ):
            raise ValueError("INSEE GDP current-series coverage differs")
        if self.artifact.role is not InseeGdpArtifactRole.CURRENT_SDMX:
            raise ValueError("INSEE GDP current-series artifact role differs")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "series_id": self.series_id,
            "title": self.title,
            "last_update": self.last_update,
            "metric": self.metric,
            "change_basis": self.change_basis,
            "unit": self.unit,
            "semantics": self.semantics,
            "observations": [item.to_dict() for item in self.observations],
            "artifact": self.artifact.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpCurrentSeriesV1:
        return cls(
            series_id=str(data.get("series_id", "")),
            title=str(data.get("title", "")),
            last_update=str(data.get("last_update", "")),
            observations=tuple(
                InseeGdpCurrentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            artifact=InseeGdpArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            metric=str(data.get("metric", "")),
            change_basis=str(data.get("change_basis", "")),
            unit=str(data.get("unit", "")),
            semantics=str(data.get("semantics", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeGdpRevisionV1:
    """Signed detailed-minus-first-estimate revision for one quarter."""

    reference_period: str
    first_estimate_value: float
    detailed_figures_value: float
    delta: float
    schema_version: str = INSEE_GDP_REVISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_REVISION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP revision schema")
        object.__setattr__(
            self,
            "reference_period",
            _quarter(self.reference_period, "reference_period"),
        )
        expected = round(
            float(self.detailed_figures_value)
            - float(self.first_estimate_value),
            12,
        )
        if abs(float(self.delta) - expected) > 1e-12:
            raise ValueError("INSEE GDP revision delta differs")
        object.__setattr__(
            self, "first_estimate_value", float(self.first_estimate_value)
        )
        object.__setattr__(
            self, "detailed_figures_value", float(self.detailed_figures_value)
        )
        object.__setattr__(self, "delta", float(self.delta))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "first_estimate_value": self.first_estimate_value,
            "detailed_figures_value": self.detailed_figures_value,
            "delta": self.delta,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpRevisionV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            first_estimate_value=float(
                cast(float, data.get("first_estimate_value"))
            ),
            detailed_figures_value=float(
                cast(float, data.get("detailed_figures_value"))
            ),
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
        self.tables: list[_HtmlTable] = []
        self._table_id: str | None = None
        self._caption: list[str] | None = None
        self._rows: list[tuple[str, ...]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

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

    def handle_endtag(self, tag: str) -> None:
        if (
            tag in {"th", "td"}
            and self._cell is not None
            and self._row is not None
        ):
            self._row.append(_SPACE_RE.sub(" ", " ".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            while self._row and not self._row[-1]:
                self._row.pop()
            if self._row:
                self._rows.append(tuple(self._row))
            self._row = None
        elif tag == "caption" and self._caption is not None:
            self._caption = [
                _SPACE_RE.sub(" ", " ".join(self._caption)).strip()
            ]
        elif tag == "table" and self._table_id is not None:
            self.tables.append(
                _HtmlTable(
                    self._table_id,
                    self._caption[0] if self._caption else "",
                    tuple(self._rows),
                )
            )
            self._table_id = None
            self._caption = None
            self._rows = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.text.append(data)
            if self._caption is not None:
                self._caption.append(data)
            if self._cell is not None:
                self._cell.append(data)


def _parse_html(content: bytes) -> _ReleaseHtmlParser:
    try:
        source = content.decode("utf-8")
    except UnicodeError as exc:
        raise ValueError("INSEE GDP release is not UTF-8") from exc
    parser = _ReleaseHtmlParser()
    parser.feed(source)
    parser.close()
    return parser


def _header_period(value: str) -> str | None:
    normalized = _SPACE_RE.sub(" ", value.replace("-", " ")).strip()
    match = re.fullmatch(
        r"(?:(\d{4}) Q([1-4])|Q([1-4]) (\d{4}))",
        normalized,
        re.IGNORECASE,
    )
    if match is None:
        return None
    year = match.group(1) or match.group(4)
    quarter = match.group(2) or match.group(3)
    return f"{year}-Q{quarter}"


def _release_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeGdpPublishedValueV1:
    labels = {"gdp", "gross domestic product", "gross domestic product (gdp)"}
    for table_index, table in enumerate(parser.tables):
        for header_index, header in enumerate(table.rows):
            periods = tuple(_header_period(cell) for cell in header)
            if period not in periods:
                continue
            column = periods.index(period)
            for row_index, row in enumerate(
                table.rows[header_index + 1 :], header_index + 1
            ):
                if not row:
                    continue
                label = re.sub(r"[\s*]+", " ", row[0]).strip().casefold()
                if label not in labels or column >= len(row):
                    continue
                lexical, numeric = _numeric(row[column], "GDP table value")
                return InseeGdpPublishedValueV1(
                    lexical_value=lexical,
                    value=numeric,
                    locator=f"table:{table_index}:{table.table_id}:row:{row_index}:column:{column}",
                )
    raise ValueError(f"INSEE GDP release omits a GDP value for {period}")


def parse_insee_gdp_search(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> tuple[InseeGdpSearchV1, tuple[Mapping[str, Any], ...]]:
    """Classify every Solr result and retain the paired release lineage."""
    boundary = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_SEARCH_BODY)
    ):
        raise ValueError("INSEE GDP search request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("INSEE GDP search JSON is invalid") from exc
    root = _mapping(payload, "search")
    documents = tuple(
        _mapping(item, "search document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _integer(root.get("numFounds"), "numFounds", MAX_INSEE_GDP_RESULTS)
    if total != len(documents):
        raise ValueError("INSEE GDP Solr response is truncated")
    selected: list[Mapping[str, Any]] = []
    exclusions: list[InseeGdpExcludedSearchResultV1] = []
    seen: set[int] = set()
    for document in documents:
        document_id = _integer(document.get("id"), "document id", 1_000_000_000)
        if document_id in seen:
            raise ValueError("INSEE GDP Solr response repeats a document")
        seen.add(document_id)
        title = _text(document.get("titre"), "title")
        raw_subtitle = document.get("sousTitre")
        subtitle = (
            None
            if raw_subtitle is None or not str(raw_subtitle).strip()
            else _text(raw_subtitle, "subtitle")
        )
        match = _SUBTITLE_RE.fullmatch(subtitle or "")
        if match is not None:
            _, period = _subtitle_parts(cast(str, subtitle))
            published = _timestamp(
                document.get("dateDiffusion"), "dateDiffusion"
            )
            published_date = (
                datetime.fromisoformat(published.replace("Z", "+00:00"))
                .date()
                .isoformat()
            )
            if (
                period >= INSEE_GDP_RELEASE_START_PERIOD
                and published_date <= boundary
            ):
                selected.append(document)
                continue
            reason = "outside-packaged-coverage"
        elif subtitle is None:
            reason = "missing-subtitle"
        elif subtitle.casefold().startswith("quarterly national accounts"):
            reason = "other-quarterly-national-accounts-publication"
        else:
            reason = "search-result-outside-release-lineage"
        exclusions.append(
            InseeGdpExcludedSearchResultV1(document_id, title, subtitle, reason)
        )
    selected.sort(
        key=lambda item: (
            _subtitle_parts(str(item["sousTitre"]))[1],
            (
                0
                if _subtitle_parts(str(item["sousTitre"]))[0]
                is InseeGdpReleaseStage.FIRST_ESTIMATE
                else 1
            ),
        )
    )
    search = InseeGdpSearchV1(
        as_of_date=boundary,
        artifact=_artifact(snapshot, InseeGdpArtifactRole.RELEASE_SEARCH),
        total_result_count=total,
        selected_document_ids=tuple(cast(int, item["id"]) for item in selected),
        exclusions=tuple(
            sorted(exclusions, key=lambda item: item.source_document_id)
        ),
    )
    return search, tuple(selected)


def _parse_release(
    document: Mapping[str, Any], snapshot: OfficialRawSnapshotV1
) -> InseeGdpReleaseV1:
    document_id = _integer(document.get("id"), "document id", 1_000_000_000)
    title = _text(document.get("titre"), "title")
    subtitle = _text(document.get("sousTitre"), "subtitle")
    stage, period = _subtitle_parts(subtitle)
    published = _timestamp(document.get("dateDiffusion"), "dateDiffusion")
    local = datetime.fromisoformat(published.replace("Z", "+00:00")).astimezone(
        ZoneInfo(INSEE_GDP_SOURCE_TIMEZONE)
    )
    parser = _parse_html(snapshot.content)
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).strip()
    if title not in page_text or subtitle not in page_text:
        raise ValueError(f"INSEE GDP page metadata differs: {document_id}")
    return InseeGdpReleaseV1(
        source_document_id=document_id,
        reference_period=period,
        stage=stage,
        title=title,
        subtitle=subtitle,
        published_at=published,
        published_at_local=local.isoformat(),
        source_uri=f"{INSEE_GDP_RELEASE_URI_PREFIX}{document_id}",
        artifact=_artifact(snapshot, InseeGdpArtifactRole.RELEASE_HTML),
        value=_release_value(parser, period),
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_current_series(
    snapshot: OfficialRawSnapshotV1,
) -> InseeGdpCurrentSeriesV1:
    try:
        root = ET.fromstring(snapshot.content)
    except ET.ParseError as exc:
        raise ValueError("INSEE GDP SDMX XML is invalid") from exc
    matches = [
        item
        for item in root.iter()
        if _local_name(item.tag) == "Series"
        and item.attrib.get("IDBANK") == INSEE_GDP_CURRENT_SERIES_ID
    ]
    if len(matches) != 1:
        raise ValueError("INSEE GDP SDMX series identity differs")
    series = matches[0]
    if (
        series.attrib.get("FREQ") != "T"
        or series.attrib.get("OPERATION") != "PIB"
        or series.attrib.get("NATURE") != "TAUX"
        or series.attrib.get("UNIT_MEASURE") != "POURCENT"
        or series.attrib.get("CORRECTION") != "CVS-CJO"
    ):
        raise ValueError("INSEE GDP SDMX series semantics differ")
    observations = []
    for item in series:
        if _local_name(item.tag) != "Obs":
            continue
        lexical, numeric = _numeric(item.attrib.get("OBS_VALUE"), "OBS_VALUE")
        observations.append(
            InseeGdpCurrentObservationV1(
                reference_period=_quarter(
                    item.attrib.get("TIME_PERIOD"), "TIME_PERIOD"
                ),
                lexical_value=lexical,
                value=numeric,
                status=str(item.attrib.get("OBS_STATUS", "<missing>")),
            )
        )
    observations.sort(key=lambda item: _quarter_ordinal(item.reference_period))
    return InseeGdpCurrentSeriesV1(
        series_id=INSEE_GDP_CURRENT_SERIES_ID,
        title=_text(series.attrib.get("TITLE_EN"), "TITLE_EN"),
        last_update=_iso_date(series.attrib.get("LAST_UPDATE"), "LAST_UPDATE"),
        observations=tuple(observations),
        artifact=_artifact(snapshot, InseeGdpArtifactRole.CURRENT_SDMX),
    )


_LIMITATIONS: Final = (
    "The qualified paired English release-page lineage begins at 2016-Q3; three exact 2009 detailed releases remain classified outside the packaged comparison window.",
    "Release-page values are contemporaneous source-dated occurrences; the BDM series is a current revised base-2020 cross-check and never replaces a historical vintage.",
    "Stage revisions compare the headline GDP quarter-on-quarter rate only; component, annual, level, overhang, benchmark, and rebase changes remain outside this slice.",
)


@dataclass(frozen=True, slots=True)
class InseeGdpArchiveManifestV1:
    """Complete replayable INSEE quarterly GDP evidence manifest."""

    registry_id: str
    source_id: str
    search: InseeGdpSearchV1
    releases: tuple[InseeGdpReleaseV1, ...]
    current_series: InseeGdpCurrentSeriesV1
    revisions: tuple[InseeGdpRevisionV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    released_value_count: int
    nonzero_revision_count: int
    limitations: tuple[str, ...] = _LIMITATIONS
    source_key: str = INSEE_GDP_SOURCE_KEY
    program_key: str = INSEE_GDP_PROGRAM_KEY
    parser_id: str = INSEE_GDP_PARSER_ID
    parser_version: str = INSEE_GDP_PARSER_VERSION
    schema_version: str = INSEE_GDP_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_GDP_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE GDP manifest schema")
        if (
            self.source_key,
            self.program_key,
            self.parser_id,
            self.parser_version,
        ) != (
            INSEE_GDP_SOURCE_KEY,
            INSEE_GDP_PROGRAM_KEY,
            INSEE_GDP_PARSER_ID,
            INSEE_GDP_PARSER_VERSION,
        ):
            raise ValueError("INSEE GDP manifest program metadata differs")
        for value, prefix, name in (
            (self.registry_id, "official-source-registry", "registry_id"),
            (self.source_id, "official-source", "source_id"),
        ):
            if re.fullmatch(rf"{prefix}:sha256:[0-9a-f]{{64}}", value) is None:
                raise ValueError(f"INSEE GDP manifest {name} is invalid")
        releases = tuple(self.releases)
        periods = _quarter_range(
            INSEE_GDP_RELEASE_START_PERIOD, INSEE_GDP_LATEST_PACKAGED_PERIOD
        )
        for stage in InseeGdpReleaseStage:
            actual = tuple(
                item.reference_period
                for item in releases
                if item.stage is stage
            )
            if actual != periods:
                raise ValueError(
                    f"INSEE GDP {stage.value} release coverage differs"
                )
        if (
            tuple(item.source_document_id for item in releases)
            != self.search.selected_document_ids
        ):
            raise ValueError("INSEE GDP release/search order differs")
        if tuple(item.reference_period for item in self.revisions) != periods:
            raise ValueError("INSEE GDP revision coverage differs")
        artifacts = (
            self.search.artifact,
            *(item.artifact for item in releases),
            self.current_series.artifact,
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError("INSEE GDP raw artifact count differs")
        if self.unique_content_sha256_count != len(
            {item.content_sha256 for item in artifacts}
        ):
            raise ValueError("INSEE GDP unique artifact count differs")
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError("INSEE GDP total content bytes differ")
        if not 0 < self.total_content_bytes <= MAX_INSEE_GDP_TOTAL_BYTES:
            raise ValueError("INSEE GDP total content bytes are outside bounds")
        if self.released_value_count != len(releases):
            raise ValueError("INSEE GDP released value count differs")
        if self.nonzero_revision_count != sum(
            abs(item.delta) > 1e-12 for item in self.revisions
        ):
            raise ValueError("INSEE GDP nonzero revision count differs")
        if tuple(self.limitations) != _LIMITATIONS:
            raise ValueError("INSEE GDP limitations differ")

    @property
    def manifest_id(self) -> str:
        return _stable_id("insee-gdp-archive-manifest", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "source_key": self.source_key,
            "program_key": self.program_key,
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "search": self.search.to_dict(),
            "releases": [item.to_dict() for item in self.releases],
            "current_series": self.current_series.to_dict(),
            "revisions": [item.to_dict() for item in self.revisions],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "released_value_count": self.released_value_count,
            "nonzero_revision_count": self.nonzero_revision_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeGdpArchiveManifestV1:
        value = cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            search=InseeGdpSearchV1.from_dict(
                _mapping(data.get("search"), "search")
            ),
            releases=tuple(
                InseeGdpReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            current_series=InseeGdpCurrentSeriesV1.from_dict(
                _mapping(data.get("current_series"), "current_series")
            ),
            revisions=tuple(
                InseeGdpRevisionV1.from_dict(_mapping(item, "revision"))
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
            source_key=str(data.get("source_key", "")),
            program_key=str(data.get("program_key", "")),
            parser_id=str(data.get("parser_id", "")),
            parser_version=str(data.get("parser_version", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("manifest_id") != value.manifest_id:
            raise ValueError("INSEE GDP manifest identity differs")
        return value

    @classmethod
    def from_json(cls, payload: str) -> InseeGdpArchiveManifestV1:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError("INSEE GDP manifest JSON is invalid") from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_insee_gdp_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search: InseeGdpSearchV1,
    documents: Sequence[Mapping[str, Any]],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeGdpArchiveManifestV1:
    """Build and fully validate the deterministic archive manifest."""
    source = _source(registry)
    ids = tuple(
        _integer(item.get("id"), "document id", 1_000_000_000)
        for item in documents
    )
    if ids != search.selected_document_ids or set(release_snapshots) != set(
        ids
    ):
        raise ValueError("INSEE GDP retained release inventory differs")
    releases = tuple(
        _parse_release(document, release_snapshots[document_id])
        for document, document_id in zip(documents, ids, strict=True)
    )
    current_series = _parse_current_series(current_series_snapshot)
    by_period_stage = {
        (item.reference_period, item.stage): item for item in releases
    }
    revisions = tuple(
        InseeGdpRevisionV1(
            reference_period=period,
            first_estimate_value=by_period_stage[
                (period, InseeGdpReleaseStage.FIRST_ESTIMATE)
            ].value.value,
            detailed_figures_value=by_period_stage[
                (period, InseeGdpReleaseStage.DETAILED_FIGURES)
            ].value.value,
            delta=round(
                by_period_stage[
                    (period, InseeGdpReleaseStage.DETAILED_FIGURES)
                ].value.value
                - by_period_stage[
                    (period, InseeGdpReleaseStage.FIRST_ESTIMATE)
                ].value.value,
                12,
            ),
        )
        for period in _quarter_range(
            INSEE_GDP_RELEASE_START_PERIOD, INSEE_GDP_LATEST_PACKAGED_PERIOD
        )
    )
    artifacts = (
        search.artifact,
        *(item.artifact for item in releases),
        current_series.artifact,
    )
    return InseeGdpArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        search=search,
        releases=releases,
        current_series=current_series,
        revisions=revisions,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        released_value_count=len(releases),
        nonzero_revision_count=sum(
            abs(item.delta) > 1e-12 for item in revisions
        ),
    )


def replay_insee_gdp_archive(
    registry: OfficialSourceRegistryV1,
    manifest: InseeGdpArchiveManifestV1,
    search_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeGdpArchiveManifestV1:
    """Rebuild retained evidence and require byte-independent equality."""
    if registry.registry_id != manifest.registry_id:
        raise ValueError("INSEE GDP replay registry identity differs")
    search, documents = parse_insee_gdp_search(
        search_snapshot, as_of_date=manifest.search.as_of_date
    )
    rebuilt = build_insee_gdp_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        current_series_snapshot,
    )
    if rebuilt != manifest:
        raise ValueError("INSEE GDP replay differs from retained manifest")
    return rebuilt


def packaged_insee_gdp_manifest_path() -> Path:
    """Return the packaged INSEE quarterly GDP manifest path."""
    return Path(__file__).with_name("assets") / "insee_gdp_archive_v1.json"


def load_packaged_insee_gdp_archive_manifest() -> InseeGdpArchiveManifestV1:
    """Load and fully validate the packaged archive."""
    path = packaged_insee_gdp_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("packaged INSEE GDP manifest exceeds size bound")
    return InseeGdpArchiveManifestV1.from_json(path.read_text(encoding="utf-8"))


__all__ = [
    "INSEE_GDP_CURRENT_SERIES_ID",
    "INSEE_GDP_CURRENT_SERIES_URI",
    "INSEE_GDP_CURRENT_START_PERIOD",
    "INSEE_GDP_LATEST_PACKAGED_PERIOD",
    "INSEE_GDP_PARSER_ID",
    "INSEE_GDP_PARSER_VERSION",
    "INSEE_GDP_PROGRAM_KEY",
    "INSEE_GDP_RELEASE_START_PERIOD",
    "INSEE_GDP_SEARCH_URI",
    "INSEE_GDP_SOURCE_KEY",
    "InseeGdpArchiveManifestV1",
    "InseeGdpArtifactRole",
    "InseeGdpArtifactV1",
    "InseeGdpCurrentObservationV1",
    "InseeGdpCurrentSeriesV1",
    "InseeGdpExcludedSearchResultV1",
    "InseeGdpPublishedValueV1",
    "InseeGdpReleaseStage",
    "InseeGdpReleaseV1",
    "InseeGdpRevisionV1",
    "InseeGdpSearchV1",
    "build_insee_gdp_archive_manifest",
    "build_insee_gdp_current_series_request",
    "build_insee_gdp_release_requests",
    "build_insee_gdp_search_request",
    "load_packaged_insee_gdp_archive_manifest",
    "packaged_insee_gdp_manifest_path",
    "parse_insee_gdp_search",
    "replay_insee_gdp_archive",
]

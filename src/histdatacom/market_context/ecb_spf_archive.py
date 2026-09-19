"""Deterministic qualification of ECB professional-forecast vintages.

The ECB Survey of Professional Forecasters (SPF) is a quarterly expectation
vintage, not an event-consensus substitute.  This module binds the official
all-data and all-reports indexes, every detailed results page from 1999 Q1,
the complete individual-forecast archive, and the dataset description.  It
retains source-labelled aggregate horizons without inventing pre-2015 release
days from later website-migration metadata.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from html.parser import HTMLParser
from itertools import zip_longest
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urljoin, urlparse, urlsplit

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.ecb_monetary_policy_archive import (
    _https_uri,
    _iso_date,
    _mapping,
    _optional_text,
    _required_text,
    _sequence,
    _stable_id,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

ECB_SPF_SOURCE_KEY: Final = "ea.ecb.spf"
ECB_SPF_PROGRAM_KEY: Final = "ea.ecb.survey-of-professional-forecasters"
ECB_SPF_PARSER_ID: Final = "official.ecb-spf.v1"
ECB_SPF_PARSER_VERSION: Final = "1"
ECB_SPF_ALL_DATA_URI: Final = (
    "https://www.ecb.europa.eu/stats/ecb_surveys/"
    "survey_of_professional_forecasters/html/all_data.en.html"
)
ECB_SPF_ALL_REPORTS_URI: Final = (
    "https://www.ecb.europa.eu/stats/ecb_surveys/"
    "survey_of_professional_forecasters/html/all-releases.en.html"
)
ECB_SPF_MICRODATA_URI: Final = (
    "https://www.ecb.europa.eu/stats/prices/indic/forecast/shared/files/"
    "SPF_individual_forecasts.zip"
)
ECB_SPF_DESCRIPTION_URI: Final = (
    "https://www.ecb.europa.eu/stats/prices/indic/forecast/shared/files/"
    "SPF_dataset_description.pdf"
)
ECB_SPF_FIRST_ROUND: Final = "1999-Q1"
ECB_SPF_ISSUE_WINDOW_FIRST_ROUND: Final = "2000-Q1"
ECB_SPF_FIRST_EXACT_DATE_ROUND: Final = "2015-Q1"
ECB_SPF_LATEST_PACKAGED_ROUND: Final = "2026-Q3"

ECB_SPF_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.ecb-spf-artifact.v1"
ECB_SPF_FORECAST_SCHEMA_VERSION: Final = "histdatacom.ecb-spf-forecast.v1"
ECB_SPF_ROUND_SCHEMA_VERSION: Final = "histdatacom.ecb-spf-round.v1"
ECB_SPF_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-spf-archive-manifest.v1"
)

MAX_ECB_SPF_ROUNDS: Final = 160
MAX_ECB_SPF_FORECASTS_PER_ROUND: Final = 32
MAX_ECB_SPF_ARTIFACTS: Final = 192
MAX_ECB_SPF_TOTAL_BYTES: Final = 96 * 1024 * 1024
MAX_ECB_SPF_ARCHIVE_UNCOMPRESSED_BYTES: Final = 32 * 1024 * 1024

_ROUND_RE = re.compile(r"table_3_(?P<year>\d{4})q(?P<quarter>[1-4])\.en\.html$")
_REPORT_RE = re.compile(r"ecb\.spf(?P<year>\d{4})q(?P<quarter>[1-4])")
_PERIOD_RE = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>[1-4])$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _period(value: object, name: str = "survey_period") -> str:
    text: str = _required_text(value, name)
    if _PERIOD_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a canonical year-quarter")
    return text


def _period_key(value: str) -> tuple[int, int]:
    match = cast(re.Match[str], _PERIOD_RE.fullmatch(_period(value)))
    return int(match.group("year")), int(match.group("quarter"))


def _periods(first: str, last: str) -> tuple[str, ...]:
    year, quarter = _period_key(first)
    end = _period_key(last)
    result: list[str] = []
    while (year, quarter) <= end:
        result.append(f"{year:04d}-Q{quarter}")
        quarter += 1
        if quarter == 5:
            year += 1
            quarter = 1
    return tuple(result)


def _period_from_uri(uri: str) -> str:
    match = _ROUND_RE.search(urlsplit(uri).path.rsplit("/", 1)[-1])
    if match is None:
        raise ValueError("ECB SPF round URI omits its canonical period")
    return f"{match.group('year')}-Q{match.group('quarter')}"


def _clean(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _bounded_int(value: object, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _sha256(value: object, name: str) -> str:
    text: str = _required_text(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return text


class EcbSpfArtifactRole(str, Enum):
    """Role of one retained official ECB SPF artifact."""

    ALL_DATA_INDEX = "all-data-index"
    ALL_REPORTS_INDEX = "all-reports-index"
    ROUND_PAGE = "round-page"
    INDIVIDUAL_MICRODATA = "individual-microdata"
    DATASET_DESCRIPTION = "dataset-description"


_ROLE_FORMAT: Final[Mapping[EcbSpfArtifactRole, OfficialSourceFormat]] = {
    EcbSpfArtifactRole.ALL_DATA_INDEX: OfficialSourceFormat.HTML,
    EcbSpfArtifactRole.ALL_REPORTS_INDEX: OfficialSourceFormat.HTML,
    EcbSpfArtifactRole.ROUND_PAGE: OfficialSourceFormat.HTML,
    EcbSpfArtifactRole.INDIVIDUAL_MICRODATA: OfficialSourceFormat.ARCHIVE,
    EcbSpfArtifactRole.DATASET_DESCRIPTION: OfficialSourceFormat.PDF,
}


class EcbSpfConcept(str, Enum):
    """Qualified aggregate concept published in a detailed round table."""

    HICP_INFLATION = "hicp-inflation"
    CORE_HICP_INFLATION = "core-hicp-inflation"
    REAL_GDP_GROWTH = "real-gdp-growth"
    UNEMPLOYMENT_RATE = "unemployment-rate"


_CONCEPT_HEADING: Final[Mapping[str, EcbSpfConcept]] = {
    "hicp inflation forecasts": EcbSpfConcept.HICP_INFLATION,
    "core hicp inflation forecasts": EcbSpfConcept.CORE_HICP_INFLATION,
    "core inflation forecasts": EcbSpfConcept.CORE_HICP_INFLATION,
    "real gdp growth forecasts": EcbSpfConcept.REAL_GDP_GROWTH,
    "unemployment rate forecasts": EcbSpfConcept.UNEMPLOYMENT_RATE,
}
_CONCEPT_UNIT: Final[Mapping[EcbSpfConcept, str]] = {
    EcbSpfConcept.HICP_INFLATION: "year-on-year-percent-change",
    EcbSpfConcept.CORE_HICP_INFLATION: "year-on-year-percent-change",
    EcbSpfConcept.REAL_GDP_GROWTH: "annual-percent-change",
    EcbSpfConcept.UNEMPLOYMENT_RATE: "percent-of-labour-force",
}


class EcbSpfAvailabilityPrecision(str, Enum):
    """Evidence level for when one survey round became available."""

    SURVEY_ROUND_ONLY = "survey-round-only"
    EXACT_DATE = "exact-date"


@dataclass(frozen=True, slots=True)
class EcbSpfArtifactV1:
    """Content-addressed official ECB SPF artifact."""

    role: EcbSpfArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = ECB_SPF_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_SPF_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported ECB SPF artifact schema")
        role = EcbSpfArtifactRole(self.role)
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format is not _ROLE_FORMAT[role]:
            raise ValueError("ECB SPF artifact role and format differ")
        uri = _https_uri(self.source_uri, "source_uri")
        digest = _sha256(self.content_sha256, "content_sha256")
        length = _bounded_int(
            self.content_length,
            "content_length",
            1,
            MAX_ECB_SPF_TOTAL_BYTES,
        )
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "content_length", length)
        identity = _stable_id("ecb-spf-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != identity:
            raise ValueError("ECB SPF artifact identity differs")
        object.__setattr__(self, "artifact_id", identity)

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
    def from_dict(cls, data: Mapping[str, Any]) -> EcbSpfArtifactV1:
        return cls(
            role=EcbSpfArtifactRole(str(data.get("role", ""))),
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
class EcbSpfForecastV1:
    """One source-labelled aggregate forecast occurrence."""

    survey_period: str
    concept: EcbSpfConcept
    horizon_label: str
    unit: str
    mean_lexical: str
    mean_value: float
    standard_deviation_lexical: str | None
    standard_deviation: float | None
    reply_count: int | None
    source_locator: str
    forecast_id: str = ""
    schema_version: str = ECB_SPF_FORECAST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_SPF_FORECAST_SCHEMA_VERSION:
            raise ValueError("unsupported ECB SPF forecast schema")
        period = _period(self.survey_period)
        concept = EcbSpfConcept(self.concept)
        horizon = _required_text(self.horizon_label, "horizon_label")
        unit = _required_text(self.unit, "unit")
        if unit != _CONCEPT_UNIT[concept]:
            raise ValueError("ECB SPF forecast concept and unit differ")
        mean_lexical = _required_text(self.mean_lexical, "mean_lexical")
        mean = _finite(self.mean_value, "mean_value")
        if float(mean_lexical) != mean:
            raise ValueError("ECB SPF mean lexical and numeric values differ")
        deviation_lexical = _optional_text(self.standard_deviation_lexical)
        deviation = self.standard_deviation
        if (deviation_lexical is None) != (deviation is None):
            raise ValueError(
                "ECB SPF deviation requires lexical and numeric values"
            )
        if deviation is not None:
            deviation = _finite(deviation, "standard_deviation")
            if (
                deviation < 0
                or float(cast(str, deviation_lexical)) != deviation
            ):
                raise ValueError("ECB SPF standard deviation differs")
        replies = self.reply_count
        if replies is not None:
            replies = _bounded_int(replies, "reply_count", 1, 10_000)
        locator = _required_text(self.source_locator, "source_locator")
        object.__setattr__(self, "survey_period", period)
        object.__setattr__(self, "concept", concept)
        object.__setattr__(self, "horizon_label", horizon)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "mean_lexical", mean_lexical)
        object.__setattr__(self, "mean_value", mean)
        object.__setattr__(
            self, "standard_deviation_lexical", deviation_lexical
        )
        object.__setattr__(self, "standard_deviation", deviation)
        object.__setattr__(self, "reply_count", replies)
        object.__setattr__(self, "source_locator", locator)
        identity = _stable_id("ecb-spf-forecast", self.identity_payload())
        if self.forecast_id and self.forecast_id != identity:
            raise ValueError("ECB SPF forecast identity differs")
        object.__setattr__(self, "forecast_id", identity)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "survey_period": self.survey_period,
            "concept": self.concept.value,
            "horizon_label": self.horizon_label,
            "unit": self.unit,
            "mean_lexical": self.mean_lexical,
            "mean_value": self.mean_value,
            "standard_deviation_lexical": self.standard_deviation_lexical,
            "standard_deviation": self.standard_deviation,
            "reply_count": self.reply_count,
            "source_locator": self.source_locator,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "forecast_id": self.forecast_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbSpfForecastV1:
        deviation = data.get("standard_deviation")
        replies = data.get("reply_count")
        return cls(
            survey_period=str(data.get("survey_period", "")),
            concept=EcbSpfConcept(str(data.get("concept", ""))),
            horizon_label=str(data.get("horizon_label", "")),
            unit=str(data.get("unit", "")),
            mean_lexical=str(data.get("mean_lexical", "")),
            mean_value=cast(float, data.get("mean_value")),
            standard_deviation_lexical=(
                str(data["standard_deviation_lexical"])
                if data.get("standard_deviation_lexical") is not None
                else None
            ),
            standard_deviation=(
                cast(float, deviation) if deviation is not None else None
            ),
            reply_count=cast(int, replies) if replies is not None else None,
            source_locator=str(data.get("source_locator", "")),
            forecast_id=str(data.get("forecast_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbSpfRoundV1:
    """One immutable quarterly SPF aggregate-results vintage."""

    survey_period: str
    source_uri: str
    availability_date: str | None
    availability_precision: EcbSpfAvailabilityPrecision
    page_metadata_date: str | None
    forecasts: tuple[EcbSpfForecastV1, ...]
    artifact: EcbSpfArtifactV1
    round_id: str = ""
    schema_version: str = ECB_SPF_ROUND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_SPF_ROUND_SCHEMA_VERSION:
            raise ValueError("unsupported ECB SPF round schema")
        period = _period(self.survey_period)
        uri = _https_uri(self.source_uri, "source_uri")
        if _period_from_uri(uri) != period:
            raise ValueError("ECB SPF round URI and period differ")
        precision = EcbSpfAvailabilityPrecision(self.availability_precision)
        availability = (
            _iso_date(self.availability_date, "availability_date")
            if self.availability_date is not None
            else None
        )
        if (precision is EcbSpfAvailabilityPrecision.EXACT_DATE) != (
            availability is not None
        ):
            raise ValueError("ECB SPF round date and precision differ")
        metadata_date = (
            _iso_date(self.page_metadata_date, "page_metadata_date")
            if self.page_metadata_date is not None
            else None
        )
        forecasts = tuple(self.forecasts)
        if not 1 <= len(forecasts) <= MAX_ECB_SPF_FORECASTS_PER_ROUND:
            raise ValueError("ECB SPF round forecast count is outside bounds")
        if any(item.survey_period != period for item in forecasts):
            raise ValueError("ECB SPF forecast and round periods differ")
        keys = {(item.concept, item.horizon_label) for item in forecasts}
        if len(keys) != len(forecasts):
            raise ValueError("ECB SPF round repeats a concept horizon")
        required = {
            EcbSpfConcept.HICP_INFLATION,
            EcbSpfConcept.REAL_GDP_GROWTH,
            EcbSpfConcept.UNEMPLOYMENT_RATE,
        }
        if not required <= {item.concept for item in forecasts}:
            raise ValueError("ECB SPF round omits a historical core concept")
        if (
            self.artifact.role is not EcbSpfArtifactRole.ROUND_PAGE
            or self.artifact.source_uri != uri
        ):
            raise ValueError("ECB SPF round artifact differs")
        object.__setattr__(self, "survey_period", period)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "availability_date", availability)
        object.__setattr__(self, "availability_precision", precision)
        object.__setattr__(self, "page_metadata_date", metadata_date)
        object.__setattr__(self, "forecasts", forecasts)
        identity = _stable_id("ecb-spf-round", self.identity_payload())
        if self.round_id and self.round_id != identity:
            raise ValueError("ECB SPF round identity differs")
        object.__setattr__(self, "round_id", identity)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "survey_period": self.survey_period,
            "source_uri": self.source_uri,
            "availability_date": self.availability_date,
            "availability_precision": self.availability_precision.value,
            "page_metadata_date": self.page_metadata_date,
            "forecasts": [item.to_dict() for item in self.forecasts],
            "artifact": self.artifact.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "round_id": self.round_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbSpfRoundV1:
        return cls(
            survey_period=str(data.get("survey_period", "")),
            source_uri=str(data.get("source_uri", "")),
            availability_date=(
                str(data["availability_date"])
                if data.get("availability_date") is not None
                else None
            ),
            availability_precision=EcbSpfAvailabilityPrecision(
                str(data.get("availability_precision", ""))
            ),
            page_metadata_date=(
                str(data["page_metadata_date"])
                if data.get("page_metadata_date") is not None
                else None
            ),
            forecasts=tuple(
                EcbSpfForecastV1.from_dict(_mapping(item, "forecast"))
                for item in _sequence(data.get("forecasts"), "forecasts")
            ),
            artifact=EcbSpfArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            round_id=str(data.get("round_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbSpfArchiveManifestV1:
    """Compact replay receipt for the complete official ECB SPF corpus."""

    registry_id: str
    source_id: str
    as_of_date: str
    support_artifacts: tuple[EcbSpfArtifactV1, ...]
    rounds: tuple[EcbSpfRoundV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    round_count: int
    issue_window_round_count: int
    exact_date_count: int
    survey_round_only_count: int
    forecast_count: int
    headline_hicp_forecast_count: int
    core_hicp_forecast_count: int
    real_gdp_forecast_count: int
    unemployment_forecast_count: int
    microdata_member_count: int
    limitations: tuple[str, ...]
    manifest_id: str = ""
    schema_version: str = ECB_SPF_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_SPF_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported ECB SPF manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        support = tuple(self.support_artifacts)
        expected_roles = (
            EcbSpfArtifactRole.ALL_DATA_INDEX,
            EcbSpfArtifactRole.ALL_REPORTS_INDEX,
            EcbSpfArtifactRole.INDIVIDUAL_MICRODATA,
            EcbSpfArtifactRole.DATASET_DESCRIPTION,
        )
        if tuple(item.role for item in support) != expected_roles:
            raise ValueError("ECB SPF support-artifact roles differ")
        rounds = tuple(self.rounds)
        periods = tuple(item.survey_period for item in rounds)
        if periods != _periods(
            ECB_SPF_FIRST_ROUND, ECB_SPF_LATEST_PACKAGED_ROUND
        ):
            raise ValueError("ECB SPF manifest round lineage is not gap-free")
        if any(
            item.availability_date is not None
            and item.availability_date > as_of
            for item in rounds
        ):
            raise ValueError("ECB SPF manifest contains future availability")
        artifacts = (*support, *(item.artifact for item in rounds))
        if len(artifacts) > MAX_ECB_SPF_ARTIFACTS:
            raise ValueError("ECB SPF artifact count exceeds bound")
        forecasts = tuple(
            item for round_ in rounds for item in round_.forecasts
        )
        counts = (
            len(artifacts),
            len({item.content_sha256 for item in artifacts}),
            sum(item.content_length for item in artifacts),
            len(rounds),
            sum(
                period >= ECB_SPF_ISSUE_WINDOW_FIRST_ROUND for period in periods
            ),
            sum(
                item.availability_precision
                is EcbSpfAvailabilityPrecision.EXACT_DATE
                for item in rounds
            ),
            sum(
                item.availability_precision
                is EcbSpfAvailabilityPrecision.SURVEY_ROUND_ONLY
                for item in rounds
            ),
            len(forecasts),
            sum(
                item.concept is EcbSpfConcept.HICP_INFLATION
                for item in forecasts
            ),
            sum(
                item.concept is EcbSpfConcept.CORE_HICP_INFLATION
                for item in forecasts
            ),
            sum(
                item.concept is EcbSpfConcept.REAL_GDP_GROWTH
                for item in forecasts
            ),
            sum(
                item.concept is EcbSpfConcept.UNEMPLOYMENT_RATE
                for item in forecasts
            ),
            len(rounds),
        )
        declared = (
            self.raw_artifact_count,
            self.unique_content_sha256_count,
            self.total_content_bytes,
            self.round_count,
            self.issue_window_round_count,
            self.exact_date_count,
            self.survey_round_only_count,
            self.forecast_count,
            self.headline_hicp_forecast_count,
            self.core_hicp_forecast_count,
            self.real_gdp_forecast_count,
            self.unemployment_forecast_count,
            self.microdata_member_count,
        )
        if declared != counts:
            raise ValueError("ECB SPF manifest summary counts differ")
        if self.total_content_bytes > MAX_ECB_SPF_TOTAL_BYTES:
            raise ValueError("ECB SPF retained corpus exceeds byte bound")
        limitations = tuple(
            _required_text(item, "limitations") for item in self.limitations
        )
        if not limitations:
            raise ValueError("ECB SPF manifest limitations are required")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "support_artifacts", support)
        object.__setattr__(self, "rounds", rounds)
        object.__setattr__(self, "limitations", limitations)
        identity = _stable_id(
            "ecb-spf-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != identity:
            raise ValueError("ECB SPF manifest identity differs")
        object.__setattr__(self, "manifest_id", identity)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "as_of_date": self.as_of_date,
            "support_artifacts": [
                item.to_dict() for item in self.support_artifacts
            ],
            "rounds": [item.to_dict() for item in self.rounds],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "round_count": self.round_count,
            "issue_window_round_count": self.issue_window_round_count,
            "exact_date_count": self.exact_date_count,
            "survey_round_only_count": self.survey_round_only_count,
            "forecast_count": self.forecast_count,
            "headline_hicp_forecast_count": self.headline_hicp_forecast_count,
            "core_hicp_forecast_count": self.core_hicp_forecast_count,
            "real_gdp_forecast_count": self.real_gdp_forecast_count,
            "unemployment_forecast_count": self.unemployment_forecast_count,
            "microdata_member_count": self.microdata_member_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EcbSpfArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            as_of_date=str(data.get("as_of_date", "")),
            support_artifacts=tuple(
                EcbSpfArtifactV1.from_dict(_mapping(item, "support artifact"))
                for item in _sequence(
                    data.get("support_artifacts"), "support_artifacts"
                )
            ),
            rounds=tuple(
                EcbSpfRoundV1.from_dict(_mapping(item, "round"))
                for item in _sequence(data.get("rounds"), "rounds")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            round_count=cast(int, data.get("round_count")),
            issue_window_round_count=cast(
                int, data.get("issue_window_round_count")
            ),
            exact_date_count=cast(int, data.get("exact_date_count")),
            survey_round_only_count=cast(
                int, data.get("survey_round_only_count")
            ),
            forecast_count=cast(int, data.get("forecast_count")),
            headline_hicp_forecast_count=cast(
                int, data.get("headline_hicp_forecast_count")
            ),
            core_hicp_forecast_count=cast(
                int, data.get("core_hicp_forecast_count")
            ),
            real_gdp_forecast_count=cast(
                int, data.get("real_gdp_forecast_count")
            ),
            unemployment_forecast_count=cast(
                int, data.get("unemployment_forecast_count")
            ),
            microdata_member_count=cast(
                int, data.get("microdata_member_count")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, payload: str) -> EcbSpfArchiveManifestV1:
        decoded = json.loads(payload)
        return cls.from_dict(_mapping(decoded, "ECB SPF manifest"))


class _SpfHtmlParser(HTMLParser):
    """Extract links, metadata, headings, and semantic tables."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str | None, str]] = []
        self.current_date: str | None = None
        self.page_metadata_date: str | None = None
        self.h1 = ""
        self._h1_parts: list[str] | None = None
        self._h2_parts: list[str] | None = None
        self._heading = ""
        self._table_heading = ""
        self.tables: list[tuple[str, tuple[tuple[str, ...], ...]]] = []
        self._rows: list[tuple[str, ...]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = {key.casefold(): value for key, value in attrs}
        if tag == "meta" and values.get("property") == "article:published_time":
            content = values.get("content")
            if content:
                self.page_metadata_date = content[:10]
        elif tag == "dt":
            value = values.get("isodate")
            if value:
                self.current_date = value
        elif tag == "a":
            href = values.get("href")
            if href:
                self.links.append((self.current_date, href))
        elif tag == "h1":
            self._h1_parts = []
        elif tag == "h2":
            self._h2_parts = []
        elif tag == "table":
            self._table_heading = self._heading
            self._rows = []
        elif tag == "tr" and self._rows is not None:
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)
        if self._h1_parts is not None:
            self._h1_parts.append(data)
        if self._h2_parts is not None:
            self._h2_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell_parts is not None:
            cast(list[str], self._row).append(_clean("".join(self._cell_parts)))
            self._cell_parts = None
        elif tag == "tr" and self._row is not None:
            cast(list[tuple[str, ...]], self._rows).append(tuple(self._row))
            self._row = None
        elif tag == "table" and self._rows is not None:
            self.tables.append((self._table_heading, tuple(self._rows)))
            self._rows = None
        elif tag == "h1" and self._h1_parts is not None:
            self.h1 = _clean("".join(self._h1_parts))
            self._h1_parts = None
        elif tag == "h2" and self._h2_parts is not None:
            self._heading = _clean("".join(self._h2_parts))
            self._h2_parts = None


def _parse_html(content: bytes) -> _SpfHtmlParser:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB SPF HTML is not UTF-8") from exc
    parser = _SpfHtmlParser()
    parser.feed(text)
    parser.close()
    return parser


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(ECB_SPF_SOURCE_KEY)
    host = cast(str, urlparse(uri).hostname)
    if (
        source.parser_id != ECB_SPF_PARSER_ID
        or source.parser_version != ECB_SPF_PARSER_VERSION
        or source_format not in source.formats
        or host not in source.allowed_hosts
    ):
        raise ValueError("ECB SPF registry binding differs")
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


def build_ecb_spf_support_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build deterministic requests for the four archive support artifacts."""
    return (
        _request(registry, ECB_SPF_ALL_DATA_URI, OfficialSourceFormat.HTML),
        _request(registry, ECB_SPF_ALL_REPORTS_URI, OfficialSourceFormat.HTML),
        _request(registry, ECB_SPF_MICRODATA_URI, OfficialSourceFormat.ARCHIVE),
        _request(registry, ECB_SPF_DESCRIPTION_URI, OfficialSourceFormat.PDF),
    )


def parse_ecb_spf_round_index(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[tuple[str, str], ...]:
    """Parse and require the complete gap-free detailed-results inventory."""
    _artifact(snapshot, EcbSpfArtifactRole.ALL_DATA_INDEX)
    parser = _parse_html(snapshot.content)
    indexed: dict[str, str] = {}
    for _date, href in parser.links:
        uri = urljoin(snapshot.request.uri, href)
        if _ROUND_RE.search(urlsplit(uri).path.rsplit("/", 1)[-1]) is None:
            continue
        period = _period_from_uri(uri)
        previous = indexed.setdefault(period, uri)
        if previous != uri:
            raise ValueError("ECB SPF index repeats a period with another URI")
    expected = _periods(ECB_SPF_FIRST_ROUND, ECB_SPF_LATEST_PACKAGED_ROUND)
    if tuple(sorted(indexed, key=_period_key)) != expected:
        raise ValueError("ECB SPF detailed-results inventory is not gap-free")
    return tuple((period, indexed[period]) for period in expected)


def build_ecb_spf_round_requests(
    registry: OfficialSourceRegistryV1,
    all_data_snapshot: OfficialRawSnapshotV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one bounded request for each indexed detailed-results page."""
    return tuple(
        _request(
            registry,
            uri,
            OfficialSourceFormat.HTML,
            page_number=index,
        )
        for index, (_period_value, uri) in enumerate(
            parse_ecb_spf_round_index(all_data_snapshot), start=1
        )
    )


def parse_ecb_spf_report_dates(
    snapshot: OfficialRawSnapshotV1,
) -> Mapping[str, str]:
    """Parse source-authored exact report dates from the modern archive."""
    _artifact(snapshot, EcbSpfArtifactRole.ALL_REPORTS_INDEX)
    parser = _parse_html(snapshot.content)
    dates: dict[str, str] = {}
    for lexical_date, href in parser.links:
        match = _REPORT_RE.search(href)
        if match is None or lexical_date is None or "special" in href:
            continue
        period = f"{match.group('year')}-Q{match.group('quarter')}"
        value = _iso_date(lexical_date, "report_date")
        previous = dates.setdefault(period, value)
        if previous != value:
            raise ValueError("ECB SPF report archive dates conflict")
    expected = _periods(
        ECB_SPF_FIRST_EXACT_DATE_ROUND, ECB_SPF_LATEST_PACKAGED_ROUND
    )
    if tuple(sorted(dates, key=_period_key)) != expected:
        raise ValueError("ECB SPF exact-date report inventory is not gap-free")
    return dates


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: EcbSpfArtifactRole
) -> EcbSpfArtifactV1:
    if snapshot.status_code != 200 or not snapshot.content:
        raise ValueError("ECB SPF snapshot is not a complete response")
    if (
        snapshot.request.source_key != ECB_SPF_SOURCE_KEY
        or snapshot.request.parser_id != ECB_SPF_PARSER_ID
        or snapshot.request.parser_version != ECB_SPF_PARSER_VERSION
        or snapshot.request.source_format is not _ROLE_FORMAT[role]
    ):
        raise ValueError("ECB SPF snapshot changes source, parser, or format")
    return EcbSpfArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=hashlib.sha256(snapshot.content).hexdigest(),
        content_length=len(snapshot.content),
    )


def _row(rows: Sequence[Sequence[str]], label: str) -> tuple[str, ...] | None:
    wanted = label.casefold()
    for row in rows:
        if row and _clean(row[0]).casefold() == wanted:
            return tuple(row)
    return None


def _forecast_rows(
    period: str,
    concept: EcbSpfConcept,
    table_number: int,
    rows: Sequence[Sequence[str]],
) -> tuple[EcbSpfForecastV1, ...]:
    mean = _row(rows, "Mean point estimate")
    deviation = _row(rows, "Standard deviation")
    replies = _row(rows, "Number of replies")
    if mean is None or deviation is None or replies is None:
        raise ValueError("ECB SPF concept table omits aggregate rows")
    mean_index = list(rows).index(mean)
    headers = tuple(rows[mean_index - 1]) if mean_index else ()
    if len(headers) == len(mean) - 1:
        headers = ("", *headers)
    if len(headers) != len(mean):
        raise ValueError("ECB SPF concept table header width differs")
    if len(deviation) != len(mean) or len(replies) != len(mean):
        raise ValueError("ECB SPF aggregate row widths differ")
    result: list[EcbSpfForecastV1] = []
    for column, (horizon, mean_text, deviation_text, replies_text) in enumerate(
        zip_longest(headers[1:], mean[1:], deviation[1:], replies[1:]),
        start=1,
    ):
        if None in {horizon, mean_text, deviation_text, replies_text}:
            raise ValueError("ECB SPF aggregate columns differ")
        horizon = _clean(cast(str, horizon))
        mean_text = _clean(cast(str, mean_text))
        deviation_text = _clean(cast(str, deviation_text))
        replies_text = _clean(cast(str, replies_text))
        if not mean_text or mean_text in {"-", ".."}:
            continue
        if not horizon:
            raise ValueError("ECB SPF populated forecast omits its horizon")
        standard_lexical = (
            deviation_text
            if deviation_text and deviation_text not in {"-", ".."}
            else None
        )
        reply_count = (
            int(float(replies_text))
            if replies_text and replies_text not in {"-", ".."}
            else None
        )
        result.append(
            EcbSpfForecastV1(
                survey_period=period,
                concept=concept,
                horizon_label=horizon,
                unit=_CONCEPT_UNIT[concept],
                mean_lexical=mean_text,
                mean_value=float(mean_text),
                standard_deviation_lexical=standard_lexical,
                standard_deviation=(
                    float(standard_lexical)
                    if standard_lexical is not None
                    else None
                ),
                reply_count=reply_count,
                source_locator=(
                    f"table:{table_number}:mean-point-estimate:column:{column}"
                ),
            )
        )
    if not result:
        raise ValueError("ECB SPF concept table has no mean forecasts")
    return tuple(result)


def parse_ecb_spf_round(
    snapshot: OfficialRawSnapshotV1,
    *,
    report_dates: Mapping[str, str],
) -> EcbSpfRoundV1:
    """Parse one detailed round page without inferring unavailable dates."""
    artifact = _artifact(snapshot, EcbSpfArtifactRole.ROUND_PAGE)
    period = _period_from_uri(snapshot.request.uri)
    parser = _parse_html(snapshot.content)
    year, quarter = _period_key(period)
    if parser.h1 != f"{year} Q{quarter}":
        raise ValueError("ECB SPF round heading and URI differ")
    forecasts: list[EcbSpfForecastV1] = []
    seen: set[EcbSpfConcept] = set()
    table_number = 0
    for heading, rows in parser.tables:
        concept = _CONCEPT_HEADING.get(heading.casefold())
        if concept is None:
            continue
        table_number += 1
        if concept in seen:
            raise ValueError("ECB SPF round repeats a concept table")
        seen.add(concept)
        forecasts.extend(_forecast_rows(period, concept, table_number, rows))
    exact_date = report_dates.get(period)
    precision = (
        EcbSpfAvailabilityPrecision.EXACT_DATE
        if exact_date is not None
        else EcbSpfAvailabilityPrecision.SURVEY_ROUND_ONLY
    )
    return EcbSpfRoundV1(
        survey_period=period,
        source_uri=snapshot.request.uri,
        availability_date=exact_date,
        availability_precision=precision,
        page_metadata_date=parser.page_metadata_date,
        forecasts=tuple(forecasts),
        artifact=artifact,
    )


def _validate_microdata(content: bytes, periods: Sequence[str]) -> int:
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
    except (zipfile.BadZipFile, OSError) as exc:
        raise ValueError("ECB SPF microdata is not a readable ZIP") from exc
    infos = archive.infolist()
    names = tuple(item.filename for item in infos)
    expected = {period.replace("-", "") + ".csv" for period in periods}
    if len(names) != len(set(names)) or set(names) != expected:
        raise ValueError("ECB SPF microdata round members differ")
    if any(
        item.is_dir()
        or item.flag_bits & 0x1
        or item.file_size <= 0
        or item.file_size > 2 * 1024 * 1024
        or "/" in item.filename
        or "\\" in item.filename
        for item in infos
    ):
        raise ValueError("ECB SPF microdata member is unsafe or invalid")
    total = sum(item.file_size for item in infos)
    if total > MAX_ECB_SPF_ARCHIVE_UNCOMPRESSED_BYTES:
        raise ValueError("ECB SPF microdata uncompressed size exceeds bound")
    for name in names:
        if b"INFLATION EXPECTATIONS" not in archive.read(name)[:512].upper():
            raise ValueError("ECB SPF microdata member omits its header")
    return len(infos)


def _validate_description(content: bytes) -> None:
    if not content.startswith(b"%PDF"):
        raise ValueError("ECB SPF dataset description is not a PDF")
    try:
        reader = PdfReader(io.BytesIO(content), strict=True)
    except Exception as exc:
        raise ValueError(
            "ECB SPF dataset description PDF is unreadable"
        ) from exc
    if not 1 <= len(reader.pages) <= 256:
        raise ValueError("ECB SPF dataset description page count is invalid")


def build_ecb_spf_archive_manifest(
    registry: OfficialSourceRegistryV1,
    all_data_snapshot: OfficialRawSnapshotV1,
    all_reports_snapshot: OfficialRawSnapshotV1,
    microdata_snapshot: OfficialRawSnapshotV1,
    description_snapshot: OfficialRawSnapshotV1,
    round_snapshots: Mapping[str, OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EcbSpfArchiveManifestV1:
    """Build the complete content-addressed ECB SPF archive receipt."""
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(ECB_SPF_SOURCE_KEY)
    indexed = parse_ecb_spf_round_index(all_data_snapshot)
    periods = tuple(period for period, _uri in indexed)
    expected_uris = {uri for _period_value, uri in indexed}
    if set(round_snapshots) != expected_uris:
        raise ValueError("ECB SPF retained round inventory differs")
    report_dates = parse_ecb_spf_report_dates(all_reports_snapshot)
    if any(value > as_of for value in report_dates.values()):
        raise ValueError("ECB SPF report archive exceeds as-of boundary")
    microdata_artifact = _artifact(
        microdata_snapshot, EcbSpfArtifactRole.INDIVIDUAL_MICRODATA
    )
    member_count = _validate_microdata(microdata_snapshot.content, periods)
    description_artifact = _artifact(
        description_snapshot, EcbSpfArtifactRole.DATASET_DESCRIPTION
    )
    _validate_description(description_snapshot.content)
    support = (
        _artifact(all_data_snapshot, EcbSpfArtifactRole.ALL_DATA_INDEX),
        _artifact(all_reports_snapshot, EcbSpfArtifactRole.ALL_REPORTS_INDEX),
        microdata_artifact,
        description_artifact,
    )
    rounds = tuple(
        parse_ecb_spf_round(round_snapshots[uri], report_dates=report_dates)
        for _period_value, uri in indexed
    )
    artifacts = (*support, *(item.artifact for item in rounds))
    forecasts = tuple(item for round_ in rounds for item in round_.forecasts)
    return EcbSpfArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        as_of_date=as_of,
        support_artifacts=support,
        rounds=rounds,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        round_count=len(rounds),
        issue_window_round_count=sum(
            item.survey_period >= ECB_SPF_ISSUE_WINDOW_FIRST_ROUND
            for item in rounds
        ),
        exact_date_count=sum(
            item.availability_precision
            is EcbSpfAvailabilityPrecision.EXACT_DATE
            for item in rounds
        ),
        survey_round_only_count=sum(
            item.availability_precision
            is EcbSpfAvailabilityPrecision.SURVEY_ROUND_ONLY
            for item in rounds
        ),
        forecast_count=len(forecasts),
        headline_hicp_forecast_count=sum(
            item.concept is EcbSpfConcept.HICP_INFLATION for item in forecasts
        ),
        core_hicp_forecast_count=sum(
            item.concept is EcbSpfConcept.CORE_HICP_INFLATION
            for item in forecasts
        ),
        real_gdp_forecast_count=sum(
            item.concept is EcbSpfConcept.REAL_GDP_GROWTH for item in forecasts
        ),
        unemployment_forecast_count=sum(
            item.concept is EcbSpfConcept.UNEMPLOYMENT_RATE
            for item in forecasts
        ),
        microdata_member_count=member_count,
        limitations=(
            "Pre-2015 round pages carry later migration metadata; no original release day is inferred.",
            "SPF vintages are independent professional forecasts, not ECB or Eurosystem staff projections.",
            "Quarterly SPF values are not event-level consensus and cannot supply release surprise.",
            "Individual respondent rows remain in the retained official microdata ZIP rather than the compact manifest.",
        ),
    )


def replay_ecb_spf_archive(
    registry: OfficialSourceRegistryV1,
    expected: EcbSpfArchiveManifestV1,
    all_data_snapshot: OfficialRawSnapshotV1,
    all_reports_snapshot: OfficialRawSnapshotV1,
    microdata_snapshot: OfficialRawSnapshotV1,
    description_snapshot: OfficialRawSnapshotV1,
    round_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> EcbSpfArchiveManifestV1:
    """Rebuild the retained official corpus and require exact equality."""
    rebuilt = build_ecb_spf_archive_manifest(
        registry,
        all_data_snapshot,
        all_reports_snapshot,
        microdata_snapshot,
        description_snapshot,
        round_snapshots,
        as_of_date=expected.as_of_date,
    )
    if rebuilt != expected:
        raise ValueError("ECB SPF retained-corpus replay differs")
    return rebuilt


def packaged_ecb_spf_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / "ecb_spf_archive_v1.json"


def load_packaged_ecb_spf_archive_manifest() -> EcbSpfArchiveManifestV1:
    """Load and validate the compact packaged ECB SPF receipt."""
    return EcbSpfArchiveManifestV1.from_json(
        packaged_ecb_spf_manifest_path().read_text(encoding="utf-8")
    )


__all__ = [
    "ECB_SPF_ALL_DATA_URI",
    "ECB_SPF_ALL_REPORTS_URI",
    "ECB_SPF_ARTIFACT_SCHEMA_VERSION",
    "ECB_SPF_DESCRIPTION_URI",
    "ECB_SPF_FIRST_EXACT_DATE_ROUND",
    "ECB_SPF_FIRST_ROUND",
    "ECB_SPF_FORECAST_SCHEMA_VERSION",
    "ECB_SPF_ISSUE_WINDOW_FIRST_ROUND",
    "ECB_SPF_LATEST_PACKAGED_ROUND",
    "ECB_SPF_MANIFEST_SCHEMA_VERSION",
    "ECB_SPF_MICRODATA_URI",
    "ECB_SPF_PARSER_ID",
    "ECB_SPF_PARSER_VERSION",
    "ECB_SPF_PROGRAM_KEY",
    "ECB_SPF_ROUND_SCHEMA_VERSION",
    "ECB_SPF_SOURCE_KEY",
    "EcbSpfArchiveManifestV1",
    "EcbSpfArtifactRole",
    "EcbSpfArtifactV1",
    "EcbSpfAvailabilityPrecision",
    "EcbSpfConcept",
    "EcbSpfForecastV1",
    "EcbSpfRoundV1",
    "build_ecb_spf_archive_manifest",
    "build_ecb_spf_round_requests",
    "build_ecb_spf_support_requests",
    "load_packaged_ecb_spf_archive_manifest",
    "packaged_ecb_spf_manifest_path",
    "parse_ecb_spf_report_dates",
    "parse_ecb_spf_round",
    "parse_ecb_spf_round_index",
    "replay_ecb_spf_archive",
]

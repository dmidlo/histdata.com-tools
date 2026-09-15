"""Occurrence-specific Census New Residential Sales archive.

The official archive spans legacy text and native PDF releases, contains a
broken December 2018 link, and omits separate artifacts for four months that
were first published inside government-disruption catch-up reports.  This
module preserves those publication facts while reconstructing the initial and
immediately preceding seasonally-adjusted annual-rate levels for new
single-family home sales.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import html
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.market_context.us_backfill import (
    US_BACKFILL_START_DATE,
    UnitedStatesBackfillProfileV1,
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

NEW_HOME_SALES_INDEX_ENTRY_SCHEMA_VERSION = (
    "histdatacom.new-home-sales-index-entry.v1"
)
NEW_HOME_SALES_INDEX_SCHEMA_VERSION = "histdatacom.new-home-sales-index.v1"
NEW_HOME_SALES_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.new-home-sales-artifact.v1"
)
NEW_HOME_SALES_MEASURE_SCHEMA_VERSION = "histdatacom.new-home-sales-measure.v1"
NEW_HOME_SALES_PUBLICATION_SCHEMA_VERSION = (
    "histdatacom.new-home-sales-publication.v1"
)
NEW_HOME_SALES_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.new-home-sales-archive-manifest.v1"
)
NEW_HOME_SALES_INDEX_ENVELOPE_SCHEMA_VERSION = (
    "histdatacom.new-home-sales-index-envelope.v1"
)

NEW_HOME_SALES_SOURCE_KEY = "us.census.new-residential-sales"
NEW_HOME_SALES_PROGRAM_KEY = "us.census.new-home-sales"
NEW_HOME_SALES_INDEX_URI = (
    "https://www.census.gov/construction/nrs/data/releases.html"
)
NEW_HOME_SALES_RETIRED_INDEX_URI = (
    "https://www.census.gov/construction/nrs/historical_data/index.html"
)
NEW_HOME_SALES_RELEASE_SCHEDULE_URI = (
    "https://www.census.gov/economic-indicators/"
)
NEW_HOME_SALES_FIRST_REFERENCE_PERIOD = "2000-01"
NEW_HOME_SALES_PREDECESSOR_REFERENCE_PERIOD = "1999-12"
NEW_HOME_SALES_TEXT_LAST_REFERENCE_PERIOD = "2001-03"
NEW_HOME_SALES_LATEST_PACKAGED_REFERENCE_PERIOD = "2026-07"
NEW_HOME_SALES_MEASURE_KEY = "new-single-family-houses-sold"
NEW_HOME_SALES_PREDECESSOR_URI = (
    "https://www.census.gov/construction/nrs/txt/c25_9912.txt"
)
NEW_HOME_SALES_MISSED_SEPARATE_PERIODS = (
    "2025-09",
    "2025-11",
    "2026-02",
)
NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS = (
    "2013-10",
    "2025-10",
    "2025-12",
    "2026-03",
)
NEW_HOME_SALES_DISRUPTION_PRIMARY_PERIODS = (
    "2013-10",
    "2018-12",
    "2019-01",
    "2019-02",
    "2025-10",
    "2025-12",
    "2026-03",
)
NEW_HOME_SALES_DECEMBER_2018_LISTED_URI = (
    "https://www.census.gov/construction/nrs/pdf/newressales_201909.pdf"
)
NEW_HOME_SALES_DECEMBER_2018_ARTIFACT_URI = (
    "https://www.census.gov/construction/nrs/pdf/newressales_201812.pdf"
)
NEW_HOME_SALES_2013_SEPTEMBER_URI = (
    "https://www.census.gov/construction/nrs/pdf/newressales_201309.pdf"
)
NEW_HOME_SALES_2013_OCTOBER_URI = (
    "https://www.census.gov/construction/nrs/pdf/newressales_201310.pdf"
)

MAX_NEW_HOME_SALES_INDEX_BYTES = 4 * 1024 * 1024
MAX_NEW_HOME_SALES_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_NEW_HOME_SALES_RELEASES = 600

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_DATE_RE = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<day>\d{1,2}),\s*"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)
_TIME_RE = re.compile(
    r"(?P<hour>10)\s*:\s*(?P<minute>00)\s*"
    r"(?:A\.?\s*M\.?\s*)?(?P<zone>EST|EDT|ET)",
    re.IGNORECASE,
)
_NOTICE_RE = re.compile(
    r"notice of (?:revision|methodology change)|annual revision|"
    r"seasonal adjustment revisions|revised estimates.{0,120}back to|"
    r"new estimation methods|historical revision|new monthly samples|"
    r"increase in the universe",
    re.IGNORECASE | re.DOTALL,
)
_MONTH_NUMBERS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}

# Filled only with independently audited source rows and their exact artifact
# hashes.  Ordinary reports remain prose-parsed rather than hash enumerated.
_SPECIAL_VALUES: Mapping[str, tuple[tuple[str, int, str, int], ...]] = (
    MappingProxyType(
        {
            "f0fb8d9228a4e15a0958ec3a0a0dd16d472e0f9e9eeecc7c97748094a3cbe529": (
                ("2013-09", 354, "2013-08", 379),
                ("2013-10", 444, "2013-09", 354),
            ),
            "9ea08a2e8afe6c5d46efff6e16ebff8a2fa09b5ee5e7f12e990745654993ca88": (
                ("2025-09", 738, "2025-08", 711),
                ("2025-10", 737, "2025-09", 738),
            ),
            "bdebe3469f3eb1e2ac37a9054e3e8a70042b71a3fe340d8edf55c707ae9bcc68": (
                ("2025-11", 758, "2025-10", 656),
                ("2025-12", 745, "2025-11", 758),
            ),
            "10eeb112260a03ec04e0a28cef1580f2ba224c20a9315f6b80e10eb3d775a5b7": (
                ("2026-02", 635, "2026-01", 583),
                ("2026-03", 682, "2026-02", 635),
            ),
        }
    )
)
_SPECIAL_PRIMARY_HASHES: Mapping[str, str] = MappingProxyType(
    {
        "2013-10": "f0fb8d9228a4e15a0958ec3a0a0dd16d472e0f9e9eeecc7c97748094a3cbe529",
        "2025-10": "9ea08a2e8afe6c5d46efff6e16ebff8a2fa09b5ee5e7f12e990745654993ca88",
        "2025-12": "bdebe3469f3eb1e2ac37a9054e3e8a70042b71a3fe340d8edf55c707ae9bcc68",
        "2026-03": "10eeb112260a03ec04e0a28cef1580f2ba224c20a9315f6b80e10eb3d775a5b7",
    }
)


def _required_text(value: object, name: str) -> str:
    result = str(value or "").strip()
    if not result or len(result) > 16_384:
        raise ValueError(f"{name} is invalid")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a SHA-256 digest")
    return result


def _year_month(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _YEAR_MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a year-month")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} is not an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} is not a canonical ISO date")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    if not result.startswith("https://"):
        raise ValueError(f"{name} is not an HTTPS URI")
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _next_month(period: str) -> str:
    year, month = map(int, period.split("-"))
    month += 1
    if month == 13:
        year += 1
        month = 1
    return f"{year:04d}-{month:02d}"


def _previous_month(period: str) -> str:
    year, month = map(int, period.split("-"))
    month -= 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"


def _month_end(period: str) -> str:
    year, month = map(int, period.split("-"))
    return f"{period}-{calendar.monthrange(year, month)[1]:02d}"


def _normalized_source_text(value: str) -> str:
    result = unicodedata.normalize("NFKC", value).replace("\xa0", " ")
    translation: dict[str | int, str | int | None] = {
        "‐": "-",
        "‑": "-",
        "‒": "-",
        "–": "-",
        "—": "-",
    }
    result = result.translate(str.maketrans(translation))
    result = re.sub(r"(?<=\d)\s+(?=[,\d])", "", result)
    result = re.sub(r"(?<=,)\s+(?=\d)", "", result)
    return re.sub(r"\s+", " ", result).strip()


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[dict[str, str]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() == "a":
            self.anchors.append(
                {key.lower(): value or "" for key, value in attrs}
            )


def _index_period(title: str) -> str | None:
    match = re.fullmatch(
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})"
        r"(?:\s+(?:New Residential Sales|Housing Sales)\s+Release)?",
        html.unescape(title).strip(),
        re.IGNORECASE,
    )
    if match is None:
        return None
    return (
        f"{int(match.group(2)):04d}-"
        f"{_MONTH_NUMBERS[match.group(1).lower()]:02d}"
    )


@dataclass(frozen=True, slots=True)
class CensusNewHomeSalesReleaseIndexEntryV1:
    """One archive-listed NRS artifact and any bounded URI correction."""

    reference_period: str
    listed_uri: str
    artifact_uri: str
    correction_kind: str
    entry_id: str = ""
    schema_version: str = NEW_HOME_SALES_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NEW_HOME_SALES_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported new-home-sales index-entry schema")
        period = _year_month(self.reference_period, "reference_period")
        listed = _https_uri(self.listed_uri, "listed_uri")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        correction = _required_text(self.correction_kind, "correction_kind")
        expected = (
            (
                NEW_HOME_SALES_DECEMBER_2018_LISTED_URI,
                NEW_HOME_SALES_DECEMBER_2018_ARTIFACT_URI,
                "wrong-target",
            )
            if period == "2018-12"
            else (listed, listed, "none")
        )
        if (listed, artifact, correction) != expected:
            raise ValueError("new-home-sales link correction differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "listed_uri", listed)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "correction_kind", correction)
        expected_id = _stable_id(
            "new-home-sales-index-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected_id:
            raise ValueError("new-home-sales index-entry identity differs")
        object.__setattr__(self, "entry_id", expected_id)

    @property
    def link_corrected(self) -> bool:
        return self.correction_kind != "none"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "listed_uri": self.listed_uri,
            "artifact_uri": self.artifact_uri,
            "correction_kind": self.correction_kind,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusNewHomeSalesReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            listed_uri=str(data.get("listed_uri", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            correction_kind=str(data.get("correction_kind", "")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusNewHomeSalesReleaseIndexV1:
    """Hash-bound enumeration of every target NRS artifact link."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[CensusNewHomeSalesReleaseIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = NEW_HOME_SALES_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NEW_HOME_SALES_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported new-home-sales release-index schema")
        if (
            _https_uri(self.source_uri, "source_uri")
            != NEW_HOME_SALES_INDEX_URI
        ):
            raise ValueError("new-home-sales index URI differs")
        sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length,
            "content_length",
            MAX_NEW_HOME_SALES_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        releases = tuple(self.releases)
        if (
            not releases
            or len(releases) > MAX_NEW_HOME_SALES_RELEASES
            or any(
                not isinstance(item, CensusNewHomeSalesReleaseIndexEntryV1)
                for item in releases
            )
        ):
            raise TypeError("new-home-sales index releases are invalid")
        periods = tuple(item.reference_period for item in releases)
        if periods != tuple(sorted(set(periods))):
            raise ValueError("new-home-sales index is not uniquely ordered")
        last = periods[-1]
        if last < NEW_HOME_SALES_LATEST_PACKAGED_REFERENCE_PERIOD:
            raise ValueError(
                "new-home-sales index ends before qualified corpus"
            )
        expected: list[str] = []
        period = NEW_HOME_SALES_FIRST_REFERENCE_PERIOD
        while period <= last:
            if period not in NEW_HOME_SALES_MISSED_SEPARATE_PERIODS:
                expected.append(period)
            period = _next_month(period)
        if periods != tuple(expected):
            raise ValueError("new-home-sales index has an unexplained gap")
        if sum(item.link_corrected for item in releases) != 1:
            raise ValueError("new-home-sales index correction count differs")
        if len({item.artifact_uri for item in releases}) != len(releases):
            raise ValueError("new-home-sales index repeats an artifact URI")
        object.__setattr__(self, "content_sha256", sha)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", releases)
        expected_id = _stable_id(
            "new-home-sales-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected_id:
            raise ValueError("new-home-sales index identity differs")
        object.__setattr__(self, "index_id", expected_id)

    @property
    def by_period(
        self,
    ) -> Mapping[str, CensusNewHomeSalesReleaseIndexEntryV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.releases}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "as_of_date": self.as_of_date,
            "releases": [item.to_dict() for item in self.releases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusNewHomeSalesReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                CensusNewHomeSalesReleaseIndexEntryV1.from_dict(
                    _mapping(item, "new-home-sales index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusNewHomeSalesArtifactV1:
    """Compact hash evidence for one retained Census response."""

    artifact_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = NEW_HOME_SALES_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NEW_HOME_SALES_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported new-home-sales artifact schema")
        uri = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format not in {
            OfficialSourceFormat.HTML,
            OfficialSourceFormat.PDF,
            OfficialSourceFormat.TEXT,
        }:
            raise ValueError("new-home-sales artifact format differs")
        sha = _sha256(self.content_sha256, "content_sha256")
        limit = (
            MAX_NEW_HOME_SALES_INDEX_BYTES
            if source_format is OfficialSourceFormat.HTML
            else MAX_NEW_HOME_SALES_ARTIFACT_BYTES
        )
        _positive_int(self.content_length, "content_length", limit)
        object.__setattr__(self, "artifact_uri", uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_sha256", sha)
        expected = _stable_id(
            "new-home-sales-artifact", self.identity_payload()
        )
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("new-home-sales artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusNewHomeSalesArtifactV1:
        return cls(
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _level_thousands(
    lexical: object, numeric: object, name: str
) -> tuple[str, int]:
    text = _required_text(lexical, f"{name}_lexical")
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+", text) is None:
        raise ValueError(f"{name}_lexical is not a source unit count")
    units = int(text.replace(",", ""))
    if units % 1000 or not 1 <= units // 1000 <= 10_000:
        raise ValueError(f"{name}_lexical is not whole thousands")
    if not isinstance(numeric, int) or isinstance(numeric, bool):
        raise TypeError(f"{name}_thousands must be an integer")
    if numeric != units // 1000:
        raise ValueError(f"{name} lexical and numeric levels differ")
    return text, numeric


@dataclass(frozen=True, slots=True)
class CensusNewHomeSalesMeasureV1:
    """One newly published sales level and its prior-vintage lineage."""

    reference_period: str
    actual_thousands: int
    actual_lexical: str
    previous_reference_period: str
    source_previous_reference_period: str
    revised_previous_thousands: int
    revised_previous_lexical: str
    previous_as_known_reference_period: str | None
    previous_as_known_thousands: int | None
    previous_as_known_lexical: str | None
    previous_artifact_uri: str | None
    revision_comparable: bool
    comparison_basis: str
    measure_id: str = ""
    schema_version: str = NEW_HOME_SALES_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NEW_HOME_SALES_MEASURE_SCHEMA_VERSION:
            raise ValueError("unsupported new-home-sales measure schema")
        period = _year_month(self.reference_period, "reference_period")
        actual_text, actual = _level_thousands(
            self.actual_lexical, self.actual_thousands, "actual"
        )
        previous_period = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_period != _previous_month(period):
            raise ValueError("new-home-sales previous period differs")
        source_previous = _year_month(
            self.source_previous_reference_period,
            "source_previous_reference_period",
        )
        if source_previous != previous_period and period != "2001-02":
            raise ValueError("new-home-sales source period label differs")
        if period == "2001-02" and source_previous != "2000-01":
            raise ValueError("new-home-sales bounded source typo differs")
        revised_text, revised = _level_thousands(
            self.revised_previous_lexical,
            self.revised_previous_thousands,
            "revised_previous",
        )
        known_period = _optional_text(self.previous_as_known_reference_period)
        known_text = _optional_text(self.previous_as_known_lexical)
        known_uri = _optional_text(self.previous_artifact_uri)
        if known_period is None:
            if any(
                item is not None
                for item in (
                    self.previous_as_known_thousands,
                    known_text,
                    known_uri,
                )
            ):
                raise ValueError("new-home-sales prior evidence is partial")
        else:
            known_period = _year_month(
                known_period, "previous_as_known_reference_period"
            )
            if known_text is None or known_uri is None:
                raise ValueError("new-home-sales prior evidence is partial")
            known_text, _ = _level_thousands(
                known_text,
                self.previous_as_known_thousands,
                "previous_as_known",
            )
            known_uri = _https_uri(known_uri, "previous_artifact_uri")
        if not isinstance(self.revision_comparable, bool):
            raise TypeError("revision_comparable must be boolean")
        comparable = known_period == previous_period
        if self.revision_comparable != comparable:
            raise ValueError("new-home-sales comparison state differs")
        basis = _required_text(self.comparison_basis, "comparison_basis")
        expected_basis = (
            "prior-publication" if comparable else "same-publication-catch-up"
        )
        if basis != expected_basis:
            raise ValueError("new-home-sales comparison basis differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "actual_thousands", actual)
        object.__setattr__(self, "actual_lexical", actual_text)
        object.__setattr__(self, "previous_reference_period", previous_period)
        object.__setattr__(
            self, "source_previous_reference_period", source_previous
        )
        object.__setattr__(self, "revised_previous_thousands", revised)
        object.__setattr__(self, "revised_previous_lexical", revised_text)
        object.__setattr__(
            self, "previous_as_known_reference_period", known_period
        )
        object.__setattr__(self, "previous_as_known_lexical", known_text)
        object.__setattr__(self, "previous_artifact_uri", known_uri)
        object.__setattr__(self, "comparison_basis", basis)
        expected = _stable_id("new-home-sales-measure", self.identity_payload())
        if self.measure_id and self.measure_id != expected:
            raise ValueError("new-home-sales measure identity differs")
        object.__setattr__(self, "measure_id", expected)

    @property
    def source_previous_period_consistent(self) -> bool:
        return (
            self.source_previous_reference_period
            == self.previous_reference_period
        )

    @property
    def previous_was_revised(self) -> bool:
        return bool(
            self.revision_comparable
            and self.previous_as_known_thousands
            != self.revised_previous_thousands
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "measure_key": NEW_HOME_SALES_MEASURE_KEY,
            "actual_thousands": self.actual_thousands,
            "actual_lexical": self.actual_lexical,
            "previous_reference_period": self.previous_reference_period,
            "source_previous_reference_period": (
                self.source_previous_reference_period
            ),
            "revised_previous_thousands": self.revised_previous_thousands,
            "revised_previous_lexical": self.revised_previous_lexical,
            "previous_as_known_reference_period": (
                self.previous_as_known_reference_period
            ),
            "previous_as_known_thousands": self.previous_as_known_thousands,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_artifact_uri": self.previous_artifact_uri,
            "revision_comparable": self.revision_comparable,
            "comparison_basis": self.comparison_basis,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "measure_id": self.measure_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusNewHomeSalesMeasureV1:
        if data.get("measure_key") != NEW_HOME_SALES_MEASURE_KEY:
            raise ValueError("new-home-sales measure key differs")
        return cls(
            reference_period=str(data.get("reference_period", "")),
            actual_thousands=cast(int, data.get("actual_thousands")),
            actual_lexical=str(data.get("actual_lexical", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            source_previous_reference_period=str(
                data.get("source_previous_reference_period", "")
            ),
            revised_previous_thousands=cast(
                int, data.get("revised_previous_thousands")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            previous_as_known_reference_period=cast(
                str | None, data.get("previous_as_known_reference_period")
            ),
            previous_as_known_thousands=cast(
                int | None, data.get("previous_as_known_thousands")
            ),
            previous_as_known_lexical=cast(
                str | None, data.get("previous_as_known_lexical")
            ),
            previous_artifact_uri=cast(
                str | None, data.get("previous_artifact_uri")
            ),
            revision_comparable=cast(bool, data.get("revision_comparable")),
            comparison_basis=str(data.get("comparison_basis", "")),
            measure_id=str(data.get("measure_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusNewHomeSalesPublicationV1:
    """One real publication time, including catch-up values and aliases."""

    index_entry_ids: tuple[str, ...]
    primary_reference_period: str
    release_date: str
    released_lexical: str
    released_at_ns: int
    reported_zone: str
    zone_consistent: bool
    artifacts: tuple[CensusNewHomeSalesArtifactV1, ...]
    parser_era: str
    source_header_lexical: str
    historical_revision_notice: bool
    catch_up_publication: bool
    federal_disruption_notice: bool
    measures: tuple[CensusNewHomeSalesMeasureV1, ...]
    publication_id: str = ""
    schema_version: str = NEW_HOME_SALES_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NEW_HOME_SALES_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported new-home-sales publication schema")
        entry_ids = tuple(
            _required_text(item, "index_entry_ids")
            for item in self.index_entry_ids
        )
        primary = _year_month(
            self.primary_reference_period, "primary_reference_period"
        )
        expected_entries = 2 if primary == "2013-10" else 1
        if len(entry_ids) != expected_entries or len(set(entry_ids)) != len(
            entry_ids
        ):
            raise ValueError(
                "new-home-sales publication index identities differ"
            )
        if any(
            not item.startswith("new-home-sales-index-entry:sha256:")
            for item in entry_ids
        ):
            raise ValueError(
                "new-home-sales publication index identity is invalid"
            )
        release = _iso_date(self.release_date, "release_date")
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T10:00:00":
            raise ValueError("new-home-sales release lexical timestamp differs")
        if not isinstance(self.released_at_ns, int) or self.released_at_ns <= 0:
            raise ValueError("new-home-sales release timestamp differs")
        zone = _required_text(self.reported_zone, "reported_zone").upper()
        if zone not in {"EST", "EDT", "ET"} or not isinstance(
            self.zone_consistent, bool
        ):
            raise ValueError("new-home-sales reported zone differs")
        expected_ns = int(
            datetime.fromisoformat(released)
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .timestamp()
            * 1_000_000_000
        )
        if self.released_at_ns != expected_ns:
            raise ValueError("new-home-sales normalized timestamp differs")
        expected_zone = (
            datetime.fromisoformat(released)
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .tzname()
        )
        if self.zone_consistent != (zone == "ET" or zone == expected_zone):
            raise ValueError("new-home-sales zone consistency differs")
        artifacts = tuple(self.artifacts)
        if (
            len(artifacts) != expected_entries
            or any(
                not isinstance(item, CensusNewHomeSalesArtifactV1)
                for item in artifacts
            )
            or tuple(item.artifact_uri for item in artifacts)
            != tuple(sorted({item.artifact_uri for item in artifacts}))
        ):
            raise ValueError("new-home-sales publication artifacts differ")
        era = _required_text(self.parser_era, "parser_era")
        if era not in {
            "legacy-text",
            "native-pdf-layout",
            "native-pdf-standard",
            "hash-bound-catch-up-pdf",
        }:
            raise ValueError("new-home-sales parser era differs")
        expected_format = (
            OfficialSourceFormat.TEXT
            if era == "legacy-text"
            else OfficialSourceFormat.PDF
        )
        if any(item.source_format is not expected_format for item in artifacts):
            raise ValueError("new-home-sales parser era format differs")
        header = _required_text(
            self.source_header_lexical, "source_header_lexical"
        )
        if not all(
            isinstance(flag, bool)
            for flag in (
                self.historical_revision_notice,
                self.catch_up_publication,
                self.federal_disruption_notice,
            )
        ):
            raise TypeError("new-home-sales publication flags must be boolean")
        special = primary in NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS
        if self.catch_up_publication != special:
            raise ValueError("new-home-sales catch-up state differs")
        if self.federal_disruption_notice != (
            primary in NEW_HOME_SALES_DISRUPTION_PRIMARY_PERIODS
        ):
            raise ValueError("new-home-sales disruption state differs")
        measures = tuple(self.measures)
        expected_count = 2 if special else 1
        periods = tuple(item.reference_period for item in measures)
        if (
            len(measures) != expected_count
            or any(
                not isinstance(item, CensusNewHomeSalesMeasureV1)
                for item in measures
            )
            or periods != tuple(sorted(set(periods)))
            or periods[-1] != primary
        ):
            raise ValueError("new-home-sales publication measures differ")
        if special and periods[0] != _previous_month(primary):
            raise ValueError("new-home-sales catch-up inventory differs")
        expected_hash = _SPECIAL_PRIMARY_HASHES.get(primary)
        hashes = {item.content_sha256 for item in artifacts}
        if special:
            if expected_hash not in hashes:
                raise ValueError(
                    "new-home-sales catch-up artifact binding differs"
                )
            expected_rows = _SPECIAL_VALUES[expected_hash]
            observed_rows = tuple(
                (
                    item.reference_period,
                    item.actual_thousands,
                    item.source_previous_reference_period,
                    item.revised_previous_thousands,
                )
                for item in measures
            )
            if observed_rows != expected_rows:
                raise ValueError("new-home-sales catch-up values differ")
        elif hashes & set(_SPECIAL_VALUES):
            raise ValueError(
                "new-home-sales catch-up artifact is misclassified"
            )
        object.__setattr__(self, "index_entry_ids", entry_ids)
        object.__setattr__(self, "primary_reference_period", primary)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "parser_era", era)
        object.__setattr__(self, "source_header_lexical", header)
        object.__setattr__(self, "measures", measures)
        expected = _stable_id(
            "new-home-sales-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("new-home-sales publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_entry_ids": list(self.index_entry_ids),
            "primary_reference_period": self.primary_reference_period,
            "release_date": self.release_date,
            "released_lexical": self.released_lexical,
            "released_at_ns": self.released_at_ns,
            "reported_zone": self.reported_zone,
            "zone_consistent": self.zone_consistent,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "parser_era": self.parser_era,
            "source_header_lexical": self.source_header_lexical,
            "historical_revision_notice": self.historical_revision_notice,
            "catch_up_publication": self.catch_up_publication,
            "federal_disruption_notice": self.federal_disruption_notice,
            "measures": [item.to_dict() for item in self.measures],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusNewHomeSalesPublicationV1:
        return cls(
            index_entry_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("index_entry_ids"), "index_entry_ids"
                )
            ),
            primary_reference_period=str(
                data.get("primary_reference_period", "")
            ),
            release_date=str(data.get("release_date", "")),
            released_lexical=str(data.get("released_lexical", "")),
            released_at_ns=cast(int, data.get("released_at_ns")),
            reported_zone=str(data.get("reported_zone", "")),
            zone_consistent=cast(bool, data.get("zone_consistent")),
            artifacts=tuple(
                CensusNewHomeSalesArtifactV1.from_dict(
                    _mapping(item, "new-home-sales publication artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            parser_era=str(data.get("parser_era", "")),
            source_header_lexical=str(data.get("source_header_lexical", "")),
            historical_revision_notice=cast(
                bool, data.get("historical_revision_notice")
            ),
            catch_up_publication=cast(bool, data.get("catch_up_publication")),
            federal_disruption_notice=cast(
                bool, data.get("federal_disruption_notice")
            ),
            measures=tuple(
                CensusNewHomeSalesMeasureV1.from_dict(
                    _mapping(item, "new-home-sales publication measure")
                )
                for item in _sequence(data.get("measures"), "measures")
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusNewHomeSalesArchiveManifestV1:
    """Compact replay receipt for the complete NRS publication corpus."""

    registry_id: str
    profile_id: str
    release_index: CensusNewHomeSalesReleaseIndexV1
    predecessor: CensusNewHomeSalesArtifactV1
    publications: tuple[CensusNewHomeSalesPublicationV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    measure_occurrence_count: int
    previous_as_known_measure_count: int
    comparable_measure_count: int
    changed_revision_count: int
    corrected_link_count: int
    legacy_text_publication_count: int
    layout_pdf_publication_count: int
    standard_pdf_fallback_count: int
    hash_bound_special_publication_count: int
    catch_up_publication_count: int
    disruption_publication_count: int
    historical_revision_notice_count: int
    exact_minute_count: int
    zone_mismatch_count: int
    source_previous_period_mismatch_count: int
    manifest_id: str = ""
    schema_version: str = NEW_HOME_SALES_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NEW_HOME_SALES_ARCHIVE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported new-home-sales archive-manifest schema"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("new-home-sales registry identity differs")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("new-home-sales profile identity differs")
        if not isinstance(self.release_index, CensusNewHomeSalesReleaseIndexV1):
            raise TypeError("new-home-sales manifest requires a v1 index")
        if (
            not isinstance(self.predecessor, CensusNewHomeSalesArtifactV1)
            or self.predecessor.artifact_uri != NEW_HOME_SALES_PREDECESSOR_URI
            or self.predecessor.source_format is not OfficialSourceFormat.TEXT
        ):
            raise ValueError("new-home-sales predecessor evidence differs")
        publications = tuple(self.publications)
        chronology = tuple(item.released_at_ns for item in publications)
        if (
            len(publications) != len(self.release_index.releases) - 1
            or any(
                not isinstance(item, CensusNewHomeSalesPublicationV1)
                for item in publications
            )
            or chronology != tuple(sorted(set(chronology)))
        ):
            raise ValueError("new-home-sales publication chronology differs")
        indexed_ids = {item.entry_id for item in self.release_index.releases}
        publication_ids = [
            entry_id
            for publication in publications
            for entry_id in publication.index_entry_ids
        ]
        if set(publication_ids) != indexed_ids or len(publication_ids) != len(
            indexed_ids
        ):
            raise ValueError("new-home-sales index coverage differs")
        indexed_uris = {
            item.artifact_uri for item in self.release_index.releases
        }
        publication_uris = {
            artifact.artifact_uri
            for publication in publications
            for artifact in publication.artifacts
        }
        if publication_uris != indexed_uris:
            raise ValueError("new-home-sales artifact inventory differs")
        entries_by_id = {
            item.entry_id: item for item in self.release_index.releases
        }
        known: dict[str, tuple[int, str]] = {}
        for publication in publications:
            primary = entries_by_id[publication.index_entry_ids[-1]]
            if (
                primary.reference_period != publication.primary_reference_period
                or primary.artifact_uri
                not in {item.artifact_uri for item in publication.artifacts}
            ):
                raise ValueError("new-home-sales publication binding differs")
            prior_known = dict(known)
            for measure in publication.measures:
                expected_previous = prior_known.get(
                    measure.previous_reference_period
                )
                if expected_previous is None:
                    if (
                        measure.reference_period
                        == NEW_HOME_SALES_FIRST_REFERENCE_PERIOD
                    ):
                        if (
                            not measure.revision_comparable
                            or measure.previous_artifact_uri
                            != NEW_HOME_SALES_PREDECESSOR_URI
                        ):
                            raise ValueError(
                                "new-home-sales predecessor lineage differs"
                            )
                    elif (
                        measure.revision_comparable
                        or measure.previous_as_known_thousands is not None
                        or measure.previous_artifact_uri is not None
                    ):
                        raise ValueError(
                            "new-home-sales catch-up lineage differs"
                        )
                elif (
                    not measure.revision_comparable
                    or measure.previous_as_known_thousands
                    != expected_previous[0]
                    or measure.previous_artifact_uri != expected_previous[1]
                ):
                    raise ValueError("new-home-sales prior lineage differs")
            for measure in publication.measures:
                known[measure.reference_period] = (
                    measure.actual_thousands,
                    primary.artifact_uri,
                )
        measures = tuple(
            measure
            for publication in publications
            for measure in publication.measures
        )
        by_period = {item.reference_period: item for item in measures}
        if len(by_period) != len(measures):
            raise ValueError("new-home-sales repeats a measure occurrence")
        expected_periods: set[str] = set()
        period = NEW_HOME_SALES_FIRST_REFERENCE_PERIOD
        last = self.release_index.releases[-1].reference_period
        while period <= last:
            expected_periods.add(period)
            period = _next_month(period)
        if set(by_period) != expected_periods:
            raise ValueError("new-home-sales monthly coverage differs")
        artifacts = (
            CensusNewHomeSalesArtifactV1(
                artifact_uri=self.release_index.source_uri,
                source_format=OfficialSourceFormat.HTML,
                content_sha256=self.release_index.content_sha256,
                content_length=self.release_index.content_length,
            ),
            self.predecessor,
            *(
                artifact
                for publication in publications
                for artifact in publication.artifacts
            ),
        )
        observed = {
            "raw_artifact_count": len(artifacts),
            "unique_content_sha256_count": len(
                {item.content_sha256 for item in artifacts}
            ),
            "total_content_bytes": sum(
                item.content_length for item in artifacts
            ),
            "measure_occurrence_count": len(measures),
            "previous_as_known_measure_count": sum(
                item.previous_as_known_thousands is not None
                for item in measures
            ),
            "comparable_measure_count": sum(
                item.revision_comparable for item in measures
            ),
            "changed_revision_count": sum(
                item.previous_was_revised for item in measures
            ),
            "corrected_link_count": sum(
                item.link_corrected for item in self.release_index.releases
            ),
            "legacy_text_publication_count": sum(
                item.parser_era == "legacy-text" for item in publications
            ),
            "layout_pdf_publication_count": sum(
                item.parser_era == "native-pdf-layout" for item in publications
            ),
            "standard_pdf_fallback_count": sum(
                item.parser_era == "native-pdf-standard"
                for item in publications
            ),
            "hash_bound_special_publication_count": sum(
                item.parser_era == "hash-bound-catch-up-pdf"
                for item in publications
            ),
            "catch_up_publication_count": sum(
                item.catch_up_publication for item in publications
            ),
            "disruption_publication_count": sum(
                item.federal_disruption_notice for item in publications
            ),
            "historical_revision_notice_count": sum(
                item.historical_revision_notice for item in publications
            ),
            "exact_minute_count": len(publications),
            "zone_mismatch_count": sum(
                not item.zone_consistent for item in publications
            ),
            "source_previous_period_mismatch_count": sum(
                not item.source_previous_period_consistent for item in measures
            ),
        }
        for name, expected in observed.items():
            actual = getattr(self, name)
            if (
                not isinstance(actual, int)
                or isinstance(actual, bool)
                or actual != expected
            ):
                raise ValueError(f"new-home-sales manifest {name} differs")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "publications", publications)
        expected_id = _stable_id(
            "new-home-sales-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected_id:
            raise ValueError("new-home-sales manifest identity differs")
        object.__setattr__(self, "manifest_id", expected_id)

    @property
    def by_primary_period(
        self,
    ) -> Mapping[str, CensusNewHomeSalesPublicationV1]:
        return MappingProxyType(
            {item.primary_reference_period: item for item in self.publications}
        )

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, CensusNewHomeSalesMeasureV1]:
        return MappingProxyType(
            {
                measure.reference_period: measure
                for publication in self.publications
                for measure in publication.measures
            }
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "predecessor": self.predecessor.to_dict(),
            "publications": [item.to_dict() for item in self.publications],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "measure_occurrence_count": self.measure_occurrence_count,
            "previous_as_known_measure_count": (
                self.previous_as_known_measure_count
            ),
            "comparable_measure_count": self.comparable_measure_count,
            "changed_revision_count": self.changed_revision_count,
            "corrected_link_count": self.corrected_link_count,
            "legacy_text_publication_count": self.legacy_text_publication_count,
            "layout_pdf_publication_count": self.layout_pdf_publication_count,
            "standard_pdf_fallback_count": self.standard_pdf_fallback_count,
            "hash_bound_special_publication_count": (
                self.hash_bound_special_publication_count
            ),
            "catch_up_publication_count": self.catch_up_publication_count,
            "disruption_publication_count": self.disruption_publication_count,
            "historical_revision_notice_count": (
                self.historical_revision_notice_count
            ),
            "exact_minute_count": self.exact_minute_count,
            "zone_mismatch_count": self.zone_mismatch_count,
            "source_previous_period_mismatch_count": (
                self.source_previous_period_mismatch_count
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusNewHomeSalesArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=CensusNewHomeSalesReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "new-home-sales index")
            ),
            predecessor=CensusNewHomeSalesArtifactV1.from_dict(
                _mapping(data.get("predecessor"), "new-home-sales predecessor")
            ),
            publications=tuple(
                CensusNewHomeSalesPublicationV1.from_dict(
                    _mapping(item, "new-home-sales publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            measure_occurrence_count=cast(
                int, data.get("measure_occurrence_count")
            ),
            previous_as_known_measure_count=cast(
                int, data.get("previous_as_known_measure_count")
            ),
            comparable_measure_count=cast(
                int, data.get("comparable_measure_count")
            ),
            changed_revision_count=cast(
                int, data.get("changed_revision_count")
            ),
            corrected_link_count=cast(int, data.get("corrected_link_count")),
            legacy_text_publication_count=cast(
                int, data.get("legacy_text_publication_count")
            ),
            layout_pdf_publication_count=cast(
                int, data.get("layout_pdf_publication_count")
            ),
            standard_pdf_fallback_count=cast(
                int, data.get("standard_pdf_fallback_count")
            ),
            hash_bound_special_publication_count=cast(
                int, data.get("hash_bound_special_publication_count")
            ),
            catch_up_publication_count=cast(
                int, data.get("catch_up_publication_count")
            ),
            disruption_publication_count=cast(
                int, data.get("disruption_publication_count")
            ),
            historical_revision_notice_count=cast(
                int, data.get("historical_revision_notice_count")
            ),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            zone_mismatch_count=cast(int, data.get("zone_mismatch_count")),
            source_previous_period_mismatch_count=cast(
                int, data.get("source_previous_period_mismatch_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CensusNewHomeSalesArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("new-home-sales manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "new-home-sales manifest"))


def build_census_new_home_sales_index_request(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> OfficialSourceRequestV1:
    """Plan the exact official NRS archive-index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("new-home-sales index request requires a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(NEW_HOME_SALES_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=NEW_HOME_SALES_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
        page_number=1,
    )


def parse_census_new_home_sales_release_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> CensusNewHomeSalesReleaseIndexV1:
    """Parse every target link and close the December 2018 defect."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("new-home-sales index parser requires a v1 snapshot")
    as_of = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.source_key != NEW_HOME_SALES_SOURCE_KEY
        or snapshot.request.uri != NEW_HOME_SALES_INDEX_URI
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.window_start != US_BACKFILL_START_DATE
        or snapshot.request.window_end != as_of
        or snapshot.status_code != 200
        or len(snapshot.content) > MAX_NEW_HOME_SALES_INDEX_BYTES
    ):
        raise ValueError("new-home-sales index snapshot differs")
    try:
        source_text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("new-home-sales index is not UTF-8") from exc
    parser = _AnchorParser()
    parser.feed(source_text)
    observed: dict[str, str] = {}
    for anchor in parser.anchors:
        period = _index_period(anchor.get("title", ""))
        if period is None or period < NEW_HOME_SALES_FIRST_REFERENCE_PERIOD:
            continue
        listed = html.unescape(anchor.get("href", "")).strip()
        if not listed:
            raise ValueError("new-home-sales index target has no href")
        prior = observed.setdefault(period, listed)
        if prior != listed:
            raise ValueError("new-home-sales title repeats with another URI")
    if not observed:
        raise ValueError("new-home-sales index contains no target releases")
    entries: list[CensusNewHomeSalesReleaseIndexEntryV1] = []
    for period, listed in sorted(observed.items()):
        corrected = period == "2018-12"
        entries.append(
            CensusNewHomeSalesReleaseIndexEntryV1(
                reference_period=period,
                listed_uri=listed,
                artifact_uri=(
                    NEW_HOME_SALES_DECEMBER_2018_ARTIFACT_URI
                    if corrected
                    else listed
                ),
                correction_kind="wrong-target" if corrected else "none",
            )
        )
    return CensusNewHomeSalesReleaseIndexV1(
        source_uri=NEW_HOME_SALES_INDEX_URI,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        releases=tuple(entries),
    )


def _release_request(
    registry: OfficialSourceRegistryV1,
    *,
    uri: str,
    period: str,
    source_format: OfficialSourceFormat,
) -> OfficialSourceRequestV1:
    source = registry.source(NEW_HOME_SALES_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=f"{period}-01",
        window_end=_month_end(period),
    )


def _artifact_source_format(period: str) -> OfficialSourceFormat:
    return (
        OfficialSourceFormat.TEXT
        if period <= NEW_HOME_SALES_TEXT_LAST_REFERENCE_PERIOD
        else OfficialSourceFormat.PDF
    )


def build_census_new_home_sales_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: CensusNewHomeSalesReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan the predecessor and all target release artifact requests."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("new-home-sales requests require a v1 registry")
    if not isinstance(release_index, CensusNewHomeSalesReleaseIndexV1):
        raise TypeError("new-home-sales requests require a v1 index")
    requests = [
        _release_request(
            registry,
            uri=NEW_HOME_SALES_PREDECESSOR_URI,
            period=NEW_HOME_SALES_PREDECESSOR_REFERENCE_PERIOD,
            source_format=OfficialSourceFormat.TEXT,
        ),
        *(
            _release_request(
                registry,
                uri=item.artifact_uri,
                period=item.reference_period,
                source_format=_artifact_source_format(item.reference_period),
            )
            for item in release_index.releases
        ),
    ]
    if len({item.uri for item in requests}) != len(requests):
        raise ValueError("new-home-sales request plan repeats an artifact URI")
    return tuple(requests)


def _validate_release_snapshot(
    snapshot: OfficialRawSnapshotV1,
    *,
    uri: str,
    source_format: OfficialSourceFormat,
) -> None:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("new-home-sales release requires a v1 snapshot")
    signature_ok = (
        snapshot.content.startswith(b"%PDF-")
        if source_format is OfficialSourceFormat.PDF
        else not snapshot.content.startswith(b"%PDF-")
    )
    if (
        snapshot.request.source_key != NEW_HOME_SALES_SOURCE_KEY
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not source_format
        or snapshot.status_code != 200
        or not signature_ok
        or len(snapshot.content) > MAX_NEW_HOME_SALES_ARTIFACT_BYTES
    ):
        raise ValueError("new-home-sales release snapshot differs")


def _decode_text(content: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("new-home-sales text artifact cannot be decoded")


def _source_text_candidates(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[str, ...]:
    if snapshot.request.source_format is OfficialSourceFormat.TEXT:
        return (_normalized_source_text(_decode_text(snapshot.content)),)
    try:
        reader = PdfReader(BytesIO(snapshot.content), strict=False)
        pages = reader.pages[:2]
        layout = _normalized_source_text(
            "\n".join(
                page.extract_text(extraction_mode="layout") or ""
                for page in pages
            )
        )
        standard = _normalized_source_text(
            "\n".join(page.extract_text() or "" for page in pages)
        )
    except Exception as exc:
        raise ValueError("new-home-sales PDF is not readable") from exc
    candidates = tuple(dict.fromkeys((layout, standard)))
    if any(not item for item in candidates):
        raise ValueError("new-home-sales PDF has no extractable first pages")
    return candidates


def _release_metadata(
    candidates: Sequence[str],
) -> tuple[str, str, int, str, bool, str]:
    for source_text in reversed(candidates):
        marker = re.search(
            r"\bfor\s+(?:immediate\s+)?release(?:\s+at)?\b",
            source_text[:1600],
            re.IGNORECASE,
        )
        if marker is None:
            continue
        header = source_text[marker.start() : marker.start() + 700]
        date_match = _DATE_RE.search(header)
        time_match = _TIME_RE.search(header)
        if date_match is None or time_match is None:
            continue
        release_date = (
            f"{int(date_match.group('year')):04d}-"
            f"{_MONTH_NUMBERS[date_match.group('month').lower()]:02d}-"
            f"{int(date_match.group('day')):02d}"
        )
        released = f"{release_date}T10:00:00"
        local = datetime.fromisoformat(released).replace(
            tzinfo=ZoneInfo("America/New_York")
        )
        reported_zone = time_match.group("zone").upper()
        consistent = reported_zone == "ET" or reported_zone == local.tzname()
        finish = min(len(header), max(date_match.end(), time_match.end()) + 100)
        return (
            release_date,
            released,
            int(local.timestamp() * 1_000_000_000),
            reported_zone,
            consistent,
            header[:finish].strip(),
        )
    raise ValueError("new-home-sales release header is not parseable")


def _parse_source_levels(
    source_text: str, reference_period: str
) -> tuple[int, str, int] | None:
    month_name = calendar.month_name[int(reference_period[-2:])]
    previous_period = _previous_month(reference_period)
    previous_name = calendar.month_name[int(previous_period[-2:])]
    current_match = re.search(
        rf"Sales of new (?:one|single)[- ]family (?:houses|homes) in "
        rf"{month_name}(?:\s+{reference_period[:4]})? were at a "
        rf"seasonally[- ]adjusted annual rate of\s+(?P<value>\d[\d,]*)",
        source_text,
        re.IGNORECASE,
    )
    if current_match is None:
        return None
    tail = source_text[current_match.end() : current_match.end() + 1200]
    previous_match = re.search(
        rf"(?:revised\s+)?{previous_name}(?:\s+(?P<year>\d{{4}}))?\s+"
        rf"(?:(?:rate|estimate|figure)\s+)?(?:of\s+)?"
        rf"(?P<value>\d[\d,]*)",
        tail,
        re.IGNORECASE,
    )
    current_units = int(current_match.group("value").replace(",", ""))
    if current_units % 1000:
        raise ValueError("new-home-sales current level is not whole thousands")
    if previous_match is None:
        return None
    previous_units = int(previous_match.group("value").replace(",", ""))
    if previous_units % 1000:
        raise ValueError("new-home-sales previous level is not whole thousands")
    source_year = previous_match.group("year") or previous_period[:4]
    source_period = f"{source_year}-{previous_period[-2:]}"
    return current_units // 1000, source_period, previous_units // 1000


def _ordinary_row(
    candidates: Sequence[str], *, reference_period: str
) -> tuple[tuple[str, int, str, int], str]:
    for candidate_number, source_text in enumerate(candidates):
        parsed = _parse_source_levels(source_text, reference_period)
        if parsed is not None:
            current, source_previous_period, revised = parsed
            era = (
                "legacy-text"
                if len(candidates) == 1
                else (
                    "native-pdf-layout"
                    if candidate_number == 0
                    else "native-pdf-standard"
                )
            )
            return (
                reference_period,
                current,
                source_previous_period,
                revised,
            ), era
    raise ValueError(
        f"new-home-sales levels are not parseable for {reference_period}"
    )


def _artifact(
    snapshot: OfficialRawSnapshotV1,
) -> CensusNewHomeSalesArtifactV1:
    return CensusNewHomeSalesArtifactV1(
        artifact_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def _publication_groups(
    release_index: CensusNewHomeSalesReleaseIndexV1,
) -> tuple[tuple[CensusNewHomeSalesReleaseIndexEntryV1, ...], ...]:
    groups: list[tuple[CensusNewHomeSalesReleaseIndexEntryV1, ...]] = []
    releases = release_index.releases
    index = 0
    while index < len(releases):
        entry = releases[index]
        if entry.reference_period == "2013-09":
            if index + 1 >= len(releases):
                raise ValueError("new-home-sales 2013 alias pair is incomplete")
            following = releases[index + 1]
            if (
                entry.artifact_uri != NEW_HOME_SALES_2013_SEPTEMBER_URI
                or following.reference_period != "2013-10"
                or following.artifact_uri != NEW_HOME_SALES_2013_OCTOBER_URI
            ):
                raise ValueError("new-home-sales 2013 alias pair differs")
            groups.append((entry, following))
            index += 2
            continue
        groups.append((entry,))
        index += 1
    return tuple(groups)


def build_census_new_home_sales_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: CensusNewHomeSalesReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> CensusNewHomeSalesArchiveManifestV1:
    """Parse every retained artifact and bind the complete monthly history."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("new-home-sales manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("new-home-sales manifest requires a v1 U.S. profile")
    if not isinstance(release_index, CensusNewHomeSalesReleaseIndexV1):
        raise TypeError("new-home-sales manifest requires a v1 index")
    source = registry.source(NEW_HOME_SALES_SOURCE_KEY)
    program = profile.by_key[NEW_HOME_SALES_PROGRAM_KEY]
    if (
        profile.registry_id != registry.registry_id
        or program.source_key != NEW_HOME_SALES_SOURCE_KEY
        or program.archive_uri != NEW_HOME_SALES_INDEX_URI
        or source.archive_uri != NEW_HOME_SALES_INDEX_URI
        or source.parser_id != "official.census-nrs.v1"
    ):
        raise ValueError("new-home-sales registry/profile binding differs")
    required_uris = {
        NEW_HOME_SALES_PREDECESSOR_URI,
        *(item.artifact_uri for item in release_index.releases),
    }
    if set(snapshots_by_uri) != required_uris:
        missing = sorted(required_uris - set(snapshots_by_uri))
        unexpected = sorted(set(snapshots_by_uri) - required_uris)
        raise ValueError(
            "new-home-sales retained corpus differs: "
            f"missing={missing[:3]}, unexpected={unexpected[:3]}"
        )

    predecessor_snapshot = snapshots_by_uri[NEW_HOME_SALES_PREDECESSOR_URI]
    _validate_release_snapshot(
        predecessor_snapshot,
        uri=NEW_HOME_SALES_PREDECESSOR_URI,
        source_format=OfficialSourceFormat.TEXT,
    )
    predecessor_candidates = _source_text_candidates(predecessor_snapshot)
    _release_metadata(predecessor_candidates)
    predecessor_row, predecessor_era = _ordinary_row(
        predecessor_candidates,
        reference_period=NEW_HOME_SALES_PREDECESSOR_REFERENCE_PERIOD,
    )
    if predecessor_era != "legacy-text":
        raise ValueError("new-home-sales predecessor parser era differs")
    predecessor = _artifact(predecessor_snapshot)
    known: dict[str, tuple[int, str]] = {
        predecessor_row[0]: (predecessor_row[1], predecessor.artifact_uri)
    }

    publications: list[CensusNewHomeSalesPublicationV1] = []
    for group in _publication_groups(release_index):
        primary_entry = group[-1]
        primary_snapshot = snapshots_by_uri[primary_entry.artifact_uri]
        source_format = _artifact_source_format(primary_entry.reference_period)
        _validate_release_snapshot(
            primary_snapshot,
            uri=primary_entry.artifact_uri,
            source_format=source_format,
        )
        candidates = _source_text_candidates(primary_snapshot)
        metadata = _release_metadata(candidates)
        artifacts: list[CensusNewHomeSalesArtifactV1] = []
        entry_ids: list[str] = []
        for entry in group:
            snapshot = snapshots_by_uri[entry.artifact_uri]
            entry_format = _artifact_source_format(entry.reference_period)
            _validate_release_snapshot(
                snapshot,
                uri=entry.artifact_uri,
                source_format=entry_format,
            )
            if len(group) == 2:
                alias_candidates = _source_text_candidates(snapshot)
                if _release_metadata(alias_candidates)[:5] != metadata[:5]:
                    raise ValueError("new-home-sales 2013 alias times disagree")
                if alias_candidates[-1] != candidates[-1]:
                    raise ValueError("new-home-sales 2013 alias text differs")
            artifacts.append(_artifact(snapshot))
            entry_ids.append(entry.entry_id)

        special_rows = _SPECIAL_VALUES.get(primary_snapshot.content_sha256)
        if (
            primary_entry.reference_period
            in NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS
        ):
            if special_rows is None:
                raise ValueError("new-home-sales special-case PDF hash differs")
            rows = special_rows
            parser_era = "hash-bound-catch-up-pdf"
        else:
            if special_rows is not None:
                raise ValueError("new-home-sales special PDF is misclassified")
            row, parser_era = _ordinary_row(
                candidates,
                reference_period=primary_entry.reference_period,
            )
            rows = (row,)

        prior_known = dict(known)
        measures: list[CensusNewHomeSalesMeasureV1] = []
        for period, current, source_previous_period, revised_previous in rows:
            previous_period = _previous_month(period)
            known_previous = prior_known.get(previous_period)
            comparable = known_previous is not None
            measures.append(
                CensusNewHomeSalesMeasureV1(
                    reference_period=period,
                    actual_thousands=current,
                    actual_lexical=f"{current:,},000",
                    previous_reference_period=previous_period,
                    source_previous_reference_period=source_previous_period,
                    revised_previous_thousands=revised_previous,
                    revised_previous_lexical=f"{revised_previous:,},000",
                    previous_as_known_reference_period=(
                        previous_period if comparable else None
                    ),
                    previous_as_known_thousands=(
                        known_previous[0]
                        if known_previous is not None
                        else None
                    ),
                    previous_as_known_lexical=(
                        f"{known_previous[0]:,},000"
                        if known_previous is not None
                        else None
                    ),
                    previous_artifact_uri=(
                        known_previous[1]
                        if known_previous is not None
                        else None
                    ),
                    revision_comparable=comparable,
                    comparison_basis=(
                        "prior-publication"
                        if comparable
                        else "same-publication-catch-up"
                    ),
                )
            )
        for period, current, _, _ in rows:
            known[period] = (current, primary_entry.artifact_uri)
        release_date, released, released_ns, zone, consistent, header = metadata
        publications.append(
            CensusNewHomeSalesPublicationV1(
                index_entry_ids=tuple(entry_ids),
                primary_reference_period=primary_entry.reference_period,
                release_date=release_date,
                released_lexical=released,
                released_at_ns=released_ns,
                reported_zone=zone,
                zone_consistent=consistent,
                artifacts=tuple(
                    sorted(artifacts, key=lambda item: item.artifact_uri)
                ),
                parser_era=parser_era,
                source_header_lexical=header,
                historical_revision_notice=any(
                    _NOTICE_RE.search(item) is not None for item in candidates
                ),
                catch_up_publication=(
                    primary_entry.reference_period
                    in NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS
                ),
                federal_disruption_notice=(
                    primary_entry.reference_period
                    in NEW_HOME_SALES_DISRUPTION_PRIMARY_PERIODS
                ),
                measures=tuple(measures),
            )
        )

    target_artifacts = tuple(
        artifact
        for publication in publications
        for artifact in publication.artifacts
    )
    all_artifacts = (
        CensusNewHomeSalesArtifactV1(
            artifact_uri=release_index.source_uri,
            source_format=OfficialSourceFormat.HTML,
            content_sha256=release_index.content_sha256,
            content_length=release_index.content_length,
        ),
        predecessor,
        *target_artifacts,
    )
    flattened_measures = tuple(
        measure
        for publication in publications
        for measure in publication.measures
    )
    return CensusNewHomeSalesArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        predecessor=predecessor,
        publications=tuple(publications),
        raw_artifact_count=len(all_artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in all_artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in all_artifacts),
        measure_occurrence_count=len(flattened_measures),
        previous_as_known_measure_count=sum(
            item.previous_as_known_thousands is not None
            for item in flattened_measures
        ),
        comparable_measure_count=sum(
            item.revision_comparable for item in flattened_measures
        ),
        changed_revision_count=sum(
            item.previous_was_revised for item in flattened_measures
        ),
        corrected_link_count=sum(
            item.link_corrected for item in release_index.releases
        ),
        legacy_text_publication_count=sum(
            item.parser_era == "legacy-text" for item in publications
        ),
        layout_pdf_publication_count=sum(
            item.parser_era == "native-pdf-layout" for item in publications
        ),
        standard_pdf_fallback_count=sum(
            item.parser_era == "native-pdf-standard" for item in publications
        ),
        hash_bound_special_publication_count=sum(
            item.parser_era == "hash-bound-catch-up-pdf"
            for item in publications
        ),
        catch_up_publication_count=sum(
            item.catch_up_publication for item in publications
        ),
        disruption_publication_count=sum(
            item.federal_disruption_notice for item in publications
        ),
        historical_revision_notice_count=sum(
            item.historical_revision_notice for item in publications
        ),
        exact_minute_count=len(publications),
        zone_mismatch_count=sum(
            not item.zone_consistent for item in publications
        ),
        source_previous_period_mismatch_count=sum(
            not item.source_previous_period_consistent
            for item in flattened_measures
        ),
    )


def replay_census_new_home_sales_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshot: OfficialRawSnapshotV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    expected: CensusNewHomeSalesArchiveManifestV1,
) -> CensusNewHomeSalesArchiveManifestV1:
    """Recompute and compare the complete retained NRS corpus."""
    if not isinstance(expected, CensusNewHomeSalesArchiveManifestV1):
        raise TypeError("new-home-sales replay requires a v1 manifest")
    release_index = parse_census_new_home_sales_release_index(
        index_snapshot, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_census_new_home_sales_archive_manifest(
        registry, profile, release_index, snapshots_by_uri
    )
    if rebuilt != expected:
        raise ValueError("new-home-sales retained-corpus replay differs")
    return rebuilt


def census_new_home_sales_coverage_from_manifest(
    manifest: CensusNewHomeSalesArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete publication-level coverage for the U.S. profile."""
    if not isinstance(manifest, CensusNewHomeSalesArchiveManifestV1):
        raise TypeError("new-home-sales coverage requires a v1 manifest")
    publication_count = len(manifest.publications)
    previous_publications = sum(
        any(
            measure.previous_as_known_thousands is not None
            for measure in publication.measures
        )
        for publication in manifest.publications
    )
    return UnitedStatesProgramCoverageV1(
        program_key=NEW_HOME_SALES_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=publication_count,
        schedule_count=publication_count,
        initial_actual_count=publication_count,
        previous_as_known_count=previous_publications,
        revision_count=manifest.changed_revision_count,
        exact_minute_count=publication_count,
        forecast_count=0,
        artifact_sha256s=tuple(
            sorted(
                {
                    manifest.release_index.content_sha256,
                    manifest.predecessor.content_sha256,
                    *(
                        artifact.content_sha256
                        for publication in manifest.publications
                        for artifact in publication.artifacts
                    ),
                }
            )
        ),
        gap_reasons=(),
        notes=(
            "September and October 2013 are one real December 4 publication.",
            "Three 2025-2026 reports retain missed-month initial estimates inside the actual catch-up publication.",
            "Same-publication prior-month values are not mislabeled as ex-ante previous-as-known observations.",
            "The retired historical-data route and broken December 2018 link are preserved as bounded migrations.",
            "No historical event-level consensus is manufactured from another survey.",
        ),
    )


def packaged_census_new_home_sales_archive_manifest_path() -> Path:
    """Return the packaged NRS archive-manifest path."""
    return (
        Path(__file__).with_name("assets") / "us_new_home_sales_archive_v1.json"
    )


def packaged_census_new_home_sales_index_path() -> Path:
    """Return the packaged base64 NRS index-envelope path."""
    return (
        Path(__file__).with_name("assets") / "us_new_home_sales_index_v1.json"
    )


def load_packaged_census_new_home_sales_archive_manifest() -> (
    CensusNewHomeSalesArchiveManifestV1
):
    """Load and validate the packaged NRS archive manifest."""
    return CensusNewHomeSalesArchiveManifestV1.from_json(
        packaged_census_new_home_sales_archive_manifest_path().read_text(
            encoding="utf-8"
        )
    )


def load_packaged_census_new_home_sales_index_snapshot() -> (
    OfficialRawSnapshotV1
):
    """Restore the exact packaged NRS archive-page snapshot."""
    try:
        payload = json.loads(
            packaged_census_new_home_sales_index_path().read_text(
                encoding="utf-8"
            )
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            "packaged new-home-sales index envelope is invalid JSON"
        ) from exc
    data = _mapping(payload, "new-home-sales index envelope")
    if (
        data.get("schema_version")
        != NEW_HOME_SALES_INDEX_ENVELOPE_SCHEMA_VERSION
    ):
        raise ValueError("unsupported new-home-sales index-envelope schema")
    encoded = data.get("content_base64")
    if (
        not isinstance(encoded, str)
        or not encoded
        or len(encoded) > (MAX_NEW_HOME_SALES_INDEX_BYTES * 4 // 3) + 4
    ):
        raise ValueError("new-home-sales index content_base64 is invalid")
    try:
        content = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError(
            "new-home-sales index is not canonical base64"
        ) from exc
    snapshot = OfficialRawSnapshotV1.restore(
        _mapping(data.get("snapshot"), "new-home-sales index snapshot"),
        content,
    )
    if snapshot.request.uri != NEW_HOME_SALES_INDEX_URI:
        raise ValueError("packaged new-home-sales index URI differs")
    return snapshot


def load_packaged_census_new_home_sales_index() -> (
    CensusNewHomeSalesReleaseIndexV1
):
    """Replay the packaged index bytes at the manifest's as-of date."""
    manifest = load_packaged_census_new_home_sales_archive_manifest()
    return parse_census_new_home_sales_release_index(
        load_packaged_census_new_home_sales_index_snapshot(),
        as_of_date=manifest.release_index.as_of_date,
    )


__all__ = [
    "NEW_HOME_SALES_2013_OCTOBER_URI",
    "NEW_HOME_SALES_2013_SEPTEMBER_URI",
    "NEW_HOME_SALES_DECEMBER_2018_ARTIFACT_URI",
    "NEW_HOME_SALES_DECEMBER_2018_LISTED_URI",
    "NEW_HOME_SALES_DISRUPTION_PRIMARY_PERIODS",
    "NEW_HOME_SALES_FIRST_REFERENCE_PERIOD",
    "NEW_HOME_SALES_INDEX_URI",
    "NEW_HOME_SALES_MEASURE_KEY",
    "NEW_HOME_SALES_MISSED_SEPARATE_PERIODS",
    "NEW_HOME_SALES_PREDECESSOR_REFERENCE_PERIOD",
    "NEW_HOME_SALES_PREDECESSOR_URI",
    "NEW_HOME_SALES_PROGRAM_KEY",
    "NEW_HOME_SALES_RELEASE_SCHEDULE_URI",
    "NEW_HOME_SALES_RETIRED_INDEX_URI",
    "NEW_HOME_SALES_SOURCE_KEY",
    "NEW_HOME_SALES_SPECIAL_PRIMARY_PERIODS",
    "CensusNewHomeSalesArchiveManifestV1",
    "CensusNewHomeSalesArtifactV1",
    "CensusNewHomeSalesMeasureV1",
    "CensusNewHomeSalesPublicationV1",
    "CensusNewHomeSalesReleaseIndexEntryV1",
    "CensusNewHomeSalesReleaseIndexV1",
    "build_census_new_home_sales_archive_manifest",
    "build_census_new_home_sales_archive_requests",
    "build_census_new_home_sales_index_request",
    "census_new_home_sales_coverage_from_manifest",
    "load_packaged_census_new_home_sales_archive_manifest",
    "load_packaged_census_new_home_sales_index",
    "load_packaged_census_new_home_sales_index_snapshot",
    "packaged_census_new_home_sales_archive_manifest_path",
    "packaged_census_new_home_sales_index_path",
    "parse_census_new_home_sales_release_index",
    "replay_census_new_home_sales_archive",
]

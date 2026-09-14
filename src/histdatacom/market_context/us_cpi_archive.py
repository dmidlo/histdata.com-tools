"""Complete BLS Consumer Price Index archive qualification for issue #538.

The packaged manifest binds the official BLS CPI release index to every
published CPI-U all-items headline from January 2000 through its explicit
as-of date.  Release bytes remain an external corpus.  Each manifest entry
nevertheless binds the current and predecessor artifacts, their independently
normalized values, the exact 08:30 America/New_York publication time, and the
source-format era used to recover the headline.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import html
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicEventFamily,
    EconomicTimePrecision,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
    normalize_official_source_timestamp,
)
from histdatacom.market_context.us_backfill import (
    US_BACKFILL_START_DATE,
    UnitedStatesBackfillProfileV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesProgramCoverageV1,
    UnitedStatesReleaseTripletV1,
)
from histdatacom.runtime_contracts import JSONValue

BLS_CPI_INDEX_SCHEMA_VERSION = "histdatacom.bls-cpi-index.v1"
BLS_CPI_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.bls-cpi-index-entry.v1"
BLS_CPI_ARCHIVE_ENTRY_SCHEMA_VERSION = "histdatacom.bls-cpi-archive-entry.v1"
BLS_CPI_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.bls-cpi-archive-manifest.v1"
)

BLS_CPI_SOURCE_KEY = "us.bls.cpi"
BLS_CPI_PROGRAM_KEY = "us.bls.cpi"
BLS_CPI_INDEX_URI = "https://www.bls.gov/bls/news-release/cpi.htm"
BLS_CPI_PREDECESSOR_URI = (
    "https://www.bls.gov/news.release/history/cpi_01142000.txt"
)
BLS_CPI_PDF_SUBSTITUTION_REFERENCE_PERIOD = "2016-05"
BLS_CPI_PDF_SUBSTITUTION_NOTE = (
    "The indexed HTML returns an empty HTTP 200 response; use the official "
    "BLS PDF for this occurrence."
)
MAX_BLS_CPI_RELEASES = 512
MAX_BLS_CPI_INDEX_BYTES = 4 * 1024 * 1024
MAX_BLS_CPI_RELEASE_BYTES = 8 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_CPI_URI_RE = re.compile(
    r"^https://www\.bls\.gov/news\.release/(?P<section>archives|history)/"
    r"cpi_(?P<date>\d{8})\.(?P<suffix>htm|txt|pdf)$"
)
_CPI_RELATIVE_URI_RE = re.compile(
    r"href=[\"'](?P<uri>/news\.release/(?:archives|history)/"
    r"cpi_(?P<date>\d{8})\.(?P<suffix>htm|txt))[\"']",
    re.IGNORECASE,
)
_CPI_NUMBER_RE = re.compile(
    r"(?<![\w.])(?P<number>[-+]?(?:\d+(?:\.\d+)?|\.\d+))"
    r"(?P<revision>[rR])?(?![\w.])"
)
_MONTH_NUMBERS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}


def _required_text(value: object, name: str) -> str:
    result = str(value).strip()
    if not result or len(result) > 8192:
        raise ValueError(f"{name} is invalid")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _year_month(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _YEAR_MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a canonical year-month")
    return result


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


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


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.bls.gov"
        or parsed.fragment
    ):
        raise ValueError(f"{name} must be an official BLS HTTPS URI")
    return result


def _cpi_lexical_number(value: object, name: str) -> tuple[str, float]:
    lexical = _required_text(value, name)
    match = re.fullmatch(
        r"(?P<number>[-+]?(?:\d+(?:\.\d+)?|\.\d+))(?P<revision>[rR])?",
        lexical,
    )
    if match is None:
        raise ValueError(f"{name} is not a CPI numeric lexical value")
    normalized = match.group("number")
    if normalized.startswith(("-.", "+.")):
        normalized = normalized[:1] + "0" + normalized[1:]
    elif normalized.startswith("."):
        normalized = "0" + normalized
    return lexical, _finite(normalized, name)


def _release_date_from_uri(uri: str) -> str:
    match = _CPI_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("CPI artifact URI is invalid")
    try:
        return (
            datetime.strptime(match.group("date"), "%m%d%Y")
            .replace(tzinfo=timezone.utc)
            .date()
            .isoformat()
        )
    except ValueError as exc:
        raise ValueError("CPI artifact URI contains an invalid date") from exc


def _source_format_for_uri(uri: str) -> OfficialSourceFormat:
    match = _CPI_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("CPI artifact URI is invalid")
    return {
        "htm": OfficialSourceFormat.HTML,
        "txt": OfficialSourceFormat.TEXT,
        "pdf": OfficialSourceFormat.PDF,
    }[match.group("suffix")]


def _contiguous_months(first: str, last: str) -> tuple[str, ...]:
    year, month = (int(item) for item in first.split("-"))
    end_year, end_month = (int(item) for item in last.split("-"))
    result: list[str] = []
    while (year, month) <= (end_year, end_month):
        result.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return tuple(result)


@dataclass(frozen=True, slots=True)
class BlsCpiReleaseIndexEntryV1:
    """One published reference-period row from the official archive index."""

    reference_period: str
    release_date: str
    indexed_artifact_uri: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    selection_note: str | None = None
    entry_id: str = ""
    schema_version: str = BLS_CPI_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_CPI_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BLS CPI index-entry schema")
        reference = _year_month(self.reference_period, "reference_period")
        release = _iso_date(self.release_date, "release_date")
        indexed = _https_uri(self.indexed_artifact_uri, "indexed_artifact_uri")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format != _source_format_for_uri(artifact):
            raise ValueError("CPI selected format differs from artifact URI")
        if release != _release_date_from_uri(indexed):
            raise ValueError("CPI release date differs from indexed URI")
        note = _optional_text(self.selection_note)
        if reference == BLS_CPI_PDF_SUBSTITUTION_REFERENCE_PERIOD:
            expected_artifact = indexed.removesuffix(".htm") + ".pdf"
            if (
                artifact != expected_artifact
                or source_format is not OfficialSourceFormat.PDF
                or note != BLS_CPI_PDF_SUBSTITUTION_NOTE
            ):
                raise ValueError("May 2016 CPI requires its official PDF")
        elif (
            artifact != indexed
            or source_format
            not in {
                OfficialSourceFormat.HTML,
                OfficialSourceFormat.TEXT,
            }
            or note is not None
        ):
            raise ValueError("CPI index entry has an unexplained substitution")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "indexed_artifact_uri", indexed)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "selection_note", note)
        expected = _stable_id("bls-cpi-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BLS CPI index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_date": self.release_date,
            "indexed_artifact_uri": self.indexed_artifact_uri,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "selection_note": self.selection_note,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsCpiReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_date=str(data.get("release_date", "")),
            indexed_artifact_uri=str(data.get("indexed_artifact_uri", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            selection_note=_optional_text(data.get("selection_note")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsCpiReleaseIndexV1:
    """As-of interpretation of the retained BLS CPI archive page."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[BlsCpiReleaseIndexEntryV1, ...]
    unavailable_reference_periods: tuple[str, ...]
    index_id: str = ""
    schema_version: str = BLS_CPI_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_CPI_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported BLS CPI index schema")
        if self.source_uri != BLS_CPI_INDEX_URI:
            raise ValueError("BLS CPI index URI is invalid")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_CPI_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        values = tuple(self.releases)
        if (
            not values
            or len(values) > MAX_BLS_CPI_RELEASES
            or any(
                not isinstance(item, BlsCpiReleaseIndexEntryV1)
                for item in values
            )
        ):
            raise TypeError("BLS CPI index releases are invalid")
        references = tuple(item.reference_period for item in values)
        if references != tuple(sorted(set(references))):
            raise ValueError("BLS CPI index releases must be unique and sorted")
        if references[0] != "2000-01":
            raise ValueError("BLS CPI index omits the January 2000 boundary")
        if any(item.release_date > as_of for item in values):
            raise ValueError("BLS CPI index contains a future publication")
        if len({item.release_date for item in values}) != len(values):
            raise ValueError("BLS CPI index repeats a publication date")
        if tuple(item.release_date for item in values) != tuple(
            sorted(item.release_date for item in values)
        ):
            raise ValueError("BLS CPI publication dates are not chronological")
        unavailable = tuple(
            _year_month(item, "unavailable_reference_periods")
            for item in self.unavailable_reference_periods
        )
        if unavailable != tuple(sorted(set(unavailable))):
            raise ValueError(
                "unavailable_reference_periods must be unique and sorted"
            )
        if set(references) & set(unavailable):
            raise ValueError("published and unavailable CPI periods overlap")
        observed_periods = tuple(sorted((*references, *unavailable)))
        if observed_periods != _contiguous_months(
            "2000-01", observed_periods[-1]
        ):
            raise ValueError("BLS CPI index has an unexplained monthly gap")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", values)
        object.__setattr__(self, "unavailable_reference_periods", unavailable)
        expected = _stable_id("bls-cpi-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("BLS CPI index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, BlsCpiReleaseIndexEntryV1]:
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
            "unavailable_reference_periods": list(
                self.unavailable_reference_periods
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsCpiReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                BlsCpiReleaseIndexEntryV1.from_dict(
                    _mapping(item, "CPI index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            unavailable_reference_periods=tuple(
                str(item)
                for item in _sequence(
                    data.get("unavailable_reference_periods"),
                    "unavailable_reference_periods",
                )
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsCpiArchiveEntryV1:
    """One current/predecessor-backed normalized CPI release occurrence."""

    reference_period: str
    previous_reference_period: str
    release_date: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    previous_artifact_uri: str
    previous_content_sha256: str
    previous_content_length: int
    normalized_sha256: str
    released_lexical: str
    actual_value: float
    actual_lexical: str
    previous_as_known_value: float
    previous_as_known_lexical: str
    revised_previous_value: float
    revised_previous_lexical: str
    transformation: str
    revision_comparable: bool
    entry_id: str = ""
    schema_version: str = BLS_CPI_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_CPI_ARCHIVE_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BLS CPI archive-entry schema")
        reference = _year_month(self.reference_period, "reference_period")
        previous_reference = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_reference >= reference:
            raise ValueError(
                "CPI previous reference period must precede current"
            )
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        previous_artifact = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format != _source_format_for_uri(artifact):
            raise ValueError("CPI entry format differs from artifact URI")
        if release != _release_date_from_uri(artifact):
            raise ValueError("CPI entry release date differs from artifact URI")
        for name in (
            "content_sha256",
            "previous_content_sha256",
            "normalized_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_CPI_RELEASE_BYTES,
        )
        _positive_int(
            self.previous_content_length,
            "previous_content_length",
            MAX_BLS_CPI_RELEASE_BYTES,
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T08:30:00":
            raise ValueError("CPI released time differs from release date")
        for name in (
            "actual_value",
            "previous_as_known_value",
            "revised_previous_value",
        ):
            object.__setattr__(self, name, _finite(getattr(self, name), name))
        for lexical_name, numeric_name in (
            ("actual_lexical", "actual_value"),
            ("previous_as_known_lexical", "previous_as_known_value"),
            ("revised_previous_lexical", "revised_previous_value"),
        ):
            lexical, numeric = _cpi_lexical_number(
                getattr(self, lexical_name), lexical_name
            )
            if not math.isclose(
                numeric,
                getattr(self, numeric_name),
                rel_tol=0.0,
                abs_tol=1e-15,
            ):
                raise ValueError(f"{lexical_name} differs from {numeric_name}")
            object.__setattr__(self, lexical_name, lexical)
        transformation = _required_text(self.transformation, "transformation")
        if transformation not in {
            "month-over-month-percent-change",
            "two-month-percent-change",
        }:
            raise ValueError("CPI transformation is invalid")
        if not isinstance(self.revision_comparable, bool):
            raise TypeError("revision_comparable must be boolean")
        if not self.revision_comparable and reference != "2025-12":
            raise ValueError("only December 2025 has no comparable revision")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(
            self, "previous_reference_period", previous_reference
        )
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "previous_artifact_uri", previous_artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "transformation", transformation)
        expected = _stable_id("bls-cpi-archive-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BLS CPI archive-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    @property
    def previous_was_revised(self) -> bool:
        return self.revision_comparable and not math.isclose(
            self.previous_as_known_value,
            self.revised_previous_value,
            rel_tol=0.0,
            abs_tol=1e-15,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "previous_reference_period": self.previous_reference_period,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "previous_artifact_uri": self.previous_artifact_uri,
            "previous_content_sha256": self.previous_content_sha256,
            "previous_content_length": self.previous_content_length,
            "normalized_sha256": self.normalized_sha256,
            "released_lexical": self.released_lexical,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "revised_previous_value": self.revised_previous_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "transformation": self.transformation,
            "revision_comparable": self.revision_comparable,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsCpiArchiveEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            release_date=str(data.get("release_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            previous_artifact_uri=str(data.get("previous_artifact_uri", "")),
            previous_content_sha256=str(
                data.get("previous_content_sha256", "")
            ),
            previous_content_length=cast(
                int, data.get("previous_content_length")
            ),
            normalized_sha256=str(data.get("normalized_sha256", "")),
            released_lexical=str(data.get("released_lexical", "")),
            actual_value=cast(float, data.get("actual_value")),
            actual_lexical=str(data.get("actual_lexical", "")),
            previous_as_known_value=cast(
                float, data.get("previous_as_known_value")
            ),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            revised_previous_value=cast(
                float, data.get("revised_previous_value")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            transformation=str(data.get("transformation", "")),
            revision_comparable=cast(bool, data.get("revision_comparable")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsCpiArchiveManifestV1:
    """Compact closure evidence for the complete as-of BLS CPI archive."""

    registry_id: str
    profile_id: str
    release_index: BlsCpiReleaseIndexV1
    entries: tuple[BlsCpiArchiveEntryV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    revision_occurrence_count: int
    noncomparable_revision_count: int
    manifest_id: str = ""
    schema_version: str = BLS_CPI_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_CPI_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported BLS CPI archive-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("CPI manifest registry identity is invalid")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("CPI manifest profile identity is invalid")
        if not isinstance(self.release_index, BlsCpiReleaseIndexV1):
            raise TypeError("CPI manifest requires a v1 release index")
        entries = tuple(self.entries)
        if (
            not entries
            or len(entries) > MAX_BLS_CPI_RELEASES
            or any(
                not isinstance(item, BlsCpiArchiveEntryV1) for item in entries
            )
        ):
            raise TypeError("CPI manifest entries are invalid")
        references = tuple(item.reference_period for item in entries)
        if references != tuple(
            item.reference_period for item in self.release_index.releases
        ):
            raise ValueError("CPI manifest differs from its release index")
        releases = self.release_index.by_reference_period
        for item in entries:
            indexed = releases[item.reference_period]
            if (
                item.release_date != indexed.release_date
                or item.artifact_uri != indexed.artifact_uri
                or item.source_format is not indexed.source_format
            ):
                raise ValueError("CPI manifest entry differs from the index")
        for previous, current in pairwise(entries):
            if (
                current.previous_reference_period != previous.reference_period
                or current.previous_artifact_uri != previous.artifact_uri
                or current.previous_content_sha256 != previous.content_sha256
                or current.previous_content_length != previous.content_length
            ):
                raise ValueError("CPI predecessor artifact chain is broken")
        first = entries[0]
        if (
            first.previous_reference_period != "1999-12"
            or first.previous_artifact_uri != BLS_CPI_PREDECESSOR_URI
        ):
            raise ValueError("CPI manifest omits its December 1999 predecessor")
        artifact_evidence = {
            (item.artifact_uri, item.content_sha256, item.content_length)
            for item in entries
        }
        artifact_evidence.add(
            (
                first.previous_artifact_uri,
                first.previous_content_sha256,
                first.previous_content_length,
            )
        )
        raw_count = len(artifact_evidence)
        total = sum(item[2] for item in artifact_evidence)
        revisions = sum(item.previous_was_revised for item in entries)
        noncomparable = sum(not item.revision_comparable for item in entries)
        if self.raw_artifact_count != raw_count:
            raise ValueError("CPI manifest raw artifact count differs")
        if self.total_content_bytes != total:
            raise ValueError("CPI manifest total byte count differs")
        if self.revision_occurrence_count != revisions:
            raise ValueError("CPI manifest revision count differs")
        if self.noncomparable_revision_count != noncomparable:
            raise ValueError("CPI manifest noncomparable count differs")
        if len({item.content_sha256 for item in entries}) != len(entries):
            raise ValueError("CPI manifest repeats current artifact content")
        if len({item[1] for item in artifact_evidence}) != raw_count:
            raise ValueError("CPI manifest repeats a raw artifact hash")
        if len({item.normalized_sha256 for item in entries}) != len(entries):
            raise ValueError("CPI manifest repeats normalized content")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "bls-cpi-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("BLS CPI archive identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_reference_period(self) -> Mapping[str, BlsCpiArchiveEntryV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "revision_occurrence_count": self.revision_occurrence_count,
            "noncomparable_revision_count": self.noncomparable_revision_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsCpiArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=BlsCpiReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            entries=tuple(
                BlsCpiArchiveEntryV1.from_dict(_mapping(item, "archive entry"))
                for item in _sequence(data.get("entries"), "entries")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            noncomparable_revision_count=cast(
                int, data.get("noncomparable_revision_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> BlsCpiArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("CPI archive manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "archive manifest"))


def build_bls_cpi_index_request(
    registry: OfficialSourceRegistryV1,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    """Plan the exact official CPI archive-index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("CPI index request requires a v1 source registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(BLS_CPI_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=BLS_CPI_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
    )


def _visible_html(value: str) -> str:
    return " ".join(
        html.unescape(re.sub(r"<[^>]+>", " ", value))
        .replace("\xa0", " ")
        .split()
    )


def parse_bls_cpi_release_index(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> BlsCpiReleaseIndexV1:
    """Parse published and explicitly unavailable CPI reference periods."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("CPI index parser requires a v1 official snapshot")
    if (
        snapshot.request.source_key != BLS_CPI_SOURCE_KEY
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.uri != BLS_CPI_INDEX_URI
    ):
        raise ValueError("CPI index parser received a different source")
    as_of = _iso_date(as_of_date, "as_of_date")
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("BLS CPI release index is not UTF-8") from exc
    releases: list[BlsCpiReleaseIndexEntryV1] = []
    unavailable: list[str] = []
    seen: set[str] = set()
    for list_item in re.finditer(r"(?is)<li\b[^>]*>(.*?)</li>", text):
        markup = list_item.group(1)
        label = _visible_html(markup)
        label_match = re.search(
            r"(?P<month>January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(?P<year>\d{4})\s+"
            r"Consumer Price Index",
            label,
            re.IGNORECASE,
        )
        if label_match is None:
            continue
        reference = (
            f"{int(label_match.group('year')):04d}-"
            f"{_MONTH_NUMBERS[label_match.group('month').lower()]:02d}"
        )
        if reference < "2000-01":
            continue
        if reference in seen:
            raise ValueError("BLS CPI index repeats a reference period")
        seen.add(reference)
        uri_match = _CPI_RELATIVE_URI_RE.search(markup)
        if uri_match is None:
            if "not published" not in label.lower():
                raise ValueError("BLS CPI index row has no release artifact")
            unavailable.append(reference)
            continue
        indexed_uri = "https://www.bls.gov" + uri_match.group("uri")
        release_date = _release_date_from_uri(indexed_uri)
        if release_date > as_of:
            continue
        artifact_uri = indexed_uri
        source_format = _source_format_for_uri(indexed_uri)
        selection_note = None
        if reference == BLS_CPI_PDF_SUBSTITUTION_REFERENCE_PERIOD:
            artifact_uri = indexed_uri.removesuffix(".htm") + ".pdf"
            source_format = OfficialSourceFormat.PDF
            selection_note = BLS_CPI_PDF_SUBSTITUTION_NOTE
        releases.append(
            BlsCpiReleaseIndexEntryV1(
                reference_period=reference,
                release_date=release_date,
                indexed_artifact_uri=indexed_uri,
                artifact_uri=artifact_uri,
                source_format=source_format,
                selection_note=selection_note,
            )
        )
    return BlsCpiReleaseIndexV1(
        source_uri=BLS_CPI_INDEX_URI,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        releases=tuple(
            sorted(releases, key=lambda item: item.reference_period)
        ),
        unavailable_reference_periods=tuple(sorted(unavailable)),
    )


def _request_for_uri(
    registry: OfficialSourceRegistryV1,
    uri: str,
) -> OfficialSourceRequestV1:
    source = registry.source(BLS_CPI_SOURCE_KEY)
    source_format = _source_format_for_uri(uri)
    if source_format not in source.formats:
        raise ValueError("CPI source does not declare the artifact format")
    release_date = _release_date_from_uri(uri)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=release_date,
        window_end=release_date,
    )


def build_bls_cpi_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: BlsCpiReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the predecessor plus every selected published artifact request."""
    if not isinstance(release_index, BlsCpiReleaseIndexV1):
        raise TypeError("CPI archive requests require a v1 release index")
    uris = (BLS_CPI_PREDECESSOR_URI,) + tuple(
        item.artifact_uri for item in release_index.releases
    )
    return tuple(_request_for_uri(registry, uri) for uri in uris)


def _decode_release_text(content: bytes) -> tuple[str, str]:
    try:
        return content.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        try:
            return content.decode("windows-1252"), "windows-1252"
        except UnicodeDecodeError as exc:
            raise ValueError(
                "CPI release is neither UTF-8 nor Windows-1252"
            ) from exc


def _strip_html(value: str) -> str:
    result = re.sub(r"(?i)<br\s*/?>", "\n", value)
    result = re.sub(r"(?i)</(?:p|div|tr|li|h\d)>", "\n", result)
    return html.unescape(re.sub(r"<[^>]+>", " ", result)).replace("\xa0", " ")


def _main_html_release(value: str) -> str:
    candidates: list[str] = []
    for match in re.finditer(r"(?is)<pre\b[^>]*>(.*?)</pre>", value):
        candidate = _strip_html(match.group(1))
        if "8:30" in candidate and "CONSUMER PRICE INDEX" in candidate.upper():
            candidates.append(candidate)
    if candidates:
        return candidates[0]
    visible = _strip_html(value)
    if "8:30" in visible and "CONSUMER PRICE INDEX" in visible.upper():
        return visible
    raise ValueError("CPI HTML release body is missing or changed")


def _pdf_release_text(content: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(content))
        if not 1 <= len(reader.pages) <= 128:
            raise ValueError("CPI PDF page count is outside its bound")
        pages = tuple((page.extract_text() or "") for page in reader.pages)
    except Exception as exc:
        raise ValueError("CPI PDF cannot be parsed") from exc
    if not pages or not pages[0].strip():
        raise ValueError("CPI PDF has no extractable release text")
    return "\n".join(pages)


def _table_values_from_html(value: str) -> tuple[tuple[str, ...], str] | None:
    table = re.search(
        r"(?is)<table\b[^>]*\bid=[\"']cpi_pressa[\"'][^>]*>(.*?)</table>",
        value,
    )
    if table is None:
        return None
    for row in re.finditer(r"(?is)<tr\b[^>]*>(.*?)</tr>", table.group(1)):
        cells = tuple(
            " ".join(_strip_html(item).split())
            for item in re.findall(
                r"(?is)<t[hd]\b[^>]*>(.*?)</t[hd]>", row.group(1)
            )
        )
        if cells and cells[0].lower() == "all items":
            values = tuple(
                item
                for item in cells[1:]
                if item in {"-", "–", "—"}
                or re.fullmatch(r"[-+]?\d*\.\d+", item)
            )
            return values, "table#cpi_pressa:row:all-items"
    raise ValueError("CPI HTML Table A omits the all-items row")


def _table_values_from_text(value: str) -> tuple[tuple[str, ...], str]:
    start = value.lower().find("table a.")
    if start < 0:
        raise ValueError("CPI release omits Table A")
    match = re.search(
        r"^\s*All items(?:\.{2,}|\s{2,})(?P<row>[^\r\n]+)",
        value[start:],
        re.MULTILINE | re.IGNORECASE,
    )
    if match is None:
        raise ValueError("CPI Table A omits the all-items row")
    line_number = value[: start + match.start()].count("\n") + 1
    values = tuple(
        item.group("number") + (item.group("revision") or "")
        for item in _CPI_NUMBER_RE.finditer(match.group("row"))
    )
    return values, f"line:{line_number}:table-a:all-items"


def _table_values_from_pdf(value: str) -> tuple[tuple[str, ...], str]:
    table_start = value.find("Table 1. Consumer Price Index")
    if table_start < 0:
        raise ValueError("CPI PDF omits Table 1")
    match = re.search(
        r"(?im)^All items\.\s*\.{2,}\s*(?P<row>[^\r\n]+)",
        value[table_start:],
    )
    if match is None:
        raise ValueError("CPI PDF Table 1 omits the all-items row")
    values = tuple(
        item.group("number")
        for item in _CPI_NUMBER_RE.finditer(match.group("row"))
    )
    return values, "pdf:table-1:all-items"


@dataclass(frozen=True, slots=True)
class _BlsCpiObservation:
    reference_period: str
    release_date: str
    released_lexical: str
    reported_zone: str
    actual_value: float
    actual_lexical: str
    revised_previous_value: float | None
    revised_previous_lexical: str | None
    transformation: str
    release_time_locator: str
    actual_locator: str
    revision_locator: str
    limitations: tuple[str, ...]


def _parse_bls_cpi_observation(
    snapshot: OfficialRawSnapshotV1,
    reference_period: str,
) -> _BlsCpiObservation:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("CPI parser requires a v1 official snapshot")
    if snapshot.request.source_key != BLS_CPI_SOURCE_KEY:
        raise ValueError("CPI parser received a different official source")
    reference = _year_month(reference_period, "reference_period")
    source_format = snapshot.request.source_format
    if source_format is not _source_format_for_uri(snapshot.request.uri):
        raise ValueError("CPI request format differs from its artifact URI")
    source_encoding = source_format.value
    raw_html: str | None = None
    if source_format is OfficialSourceFormat.PDF:
        text = _pdf_release_text(snapshot.content)
        main_text = text
    elif source_format in {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }:
        text, source_encoding = _decode_release_text(snapshot.content)
        raw_html = text if source_format is OfficialSourceFormat.HTML else None
        main_text = (
            _main_html_release(text)
            if source_format is OfficialSourceFormat.HTML
            else text
        )
    else:
        raise ValueError("CPI parser received an unsupported source format")
    release_date = _release_date_from_uri(snapshot.request.uri)
    release = date.fromisoformat(release_date)
    header = " ".join(main_text[:6000].split())
    if (
        re.search(
            rf"{release.strftime('%B')}\s+{release.day},\s*{release.year}",
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("CPI release date is absent from the header")
    time_match = re.search(
        r"8:30\s*(?:A\.?M\.?)\s*\((?P<zone>E\.?S\.?T\.?|" r"E\.?D\.?T\.?|ET)\)",
        main_text,
        re.IGNORECASE,
    )
    if time_match is None:
        raise ValueError("CPI 08:30 release-time header is missing")
    reported_zone = time_match.group("zone").upper().replace(".", "")
    lexical = f"{release_date}T08:30:00"
    timestamp = normalize_official_source_timestamp(
        lexical,
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    expected_zone = (
        datetime.fromtimestamp(
            timestamp.utc_ns / 1_000_000_000, tz=timezone.utc
        )
        .astimezone(ZoneInfo("America/New_York"))
        .tzname()
    )
    if reported_zone in {"EST", "EDT"} and reported_zone != expected_zone:
        raise ValueError("CPI release timezone conflicts with release date")
    year, month = (int(item) for item in reference.split("-"))
    if (
        re.search(
            rf"CONSUMER PRICE INDEX[^\n]{{0,80}}{calendar.month_name[month]}\s+{year}",
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("CPI reference period is absent from the title")
    if source_format is OfficialSourceFormat.PDF:
        values, table_locator = _table_values_from_pdf(text)
    else:
        html_values = (
            _table_values_from_html(raw_html) if raw_html is not None else None
        )
        values, table_locator = (
            html_values
            if html_values is not None
            else _table_values_from_text(main_text)
        )
    if source_format is OfficialSourceFormat.PDF:
        if len(values) < 3:
            raise ValueError("CPI PDF all-items row is incomplete")
        actual_lexical = values[-1]
        revised_lexical: str | None = values[-2]
        transformation = "month-over-month-percent-change"
        actual_locator = f"{table_locator}:last-sa-change"
        revision_locator = f"{table_locator}:penultimate-sa-change"
    elif reference == "2025-11":
        compact = " ".join(main_text.split())
        actual_match = re.search(
            r"(?:increased|rose)\s+(?P<value>\d+(?:\.\d+)?)\s+percent\s+"
            r"on\s+a\s+seasonally\s+adjusted\s+basis\s+over\s+the\s+2\s+months",
            compact,
            re.IGNORECASE,
        )
        if actual_match is None or len(values) != 8:
            raise ValueError("November 2025 CPI shutdown measure is incomplete")
        actual_lexical = actual_match.group("value")
        revised_lexical = values[-4]
        transformation = "two-month-percent-change"
        actual_locator = "narrative:first-paragraph:two-month-change"
        revision_locator = f"{table_locator}:september-change"
    else:
        if len(values) not in {8, 9}:
            raise ValueError("CPI Table A all-items row shape changed")
        offset = 1 if len(values) == 9 else 0
        actual_lexical = values[-2 - offset]
        revised_lexical = values[-3 - offset]
        transformation = "month-over-month-percent-change"
        actual_locator = f"{table_locator}:current-month-change"
        revision_locator = f"{table_locator}:previous-published-month-change"
        if reference == "2025-12":
            if revised_lexical not in {"-", "–", "—"}:
                raise ValueError("December 2025 CPI should omit November data")
            revised_lexical = None
            revision_locator = f"{table_locator}:november-not-published"
    if actual_lexical in {"-", "–", "—"}:
        raise ValueError("CPI actual value is unavailable")
    actual_lexical, actual = _cpi_lexical_number(
        actual_lexical, "actual_lexical"
    )
    revised: float | None = None
    if revised_lexical is not None:
        if revised_lexical in {"-", "–", "—"}:
            raise ValueError("CPI revised-previous value is unavailable")
        revised_lexical, revised = _cpi_lexical_number(
            revised_lexical, "revised_previous_lexical"
        )
    release_line = main_text[: time_match.start()].count("\n") + 1
    limitations: tuple[str, ...] = (
        "This parser qualifies CPI-U all items only.",
        "Values are seasonally adjusted percent changes, not index levels or year-over-year rates.",
        f"The source header reports {reported_zone}; timezone normalization uses America/New_York.",
        f"The retained source format is {source_format.value} and text decoding is {source_encoding}.",
    )
    if reference == BLS_CPI_PDF_SUBSTITUTION_REFERENCE_PERIOD:
        limitations += (BLS_CPI_PDF_SUBSTITUTION_NOTE,)
    if reference == "2025-11":
        limitations += (
            "The November 2025 headline is a September-to-November two-month change because October data were not collected.",
        )
    if reference == "2025-12":
        limitations += (
            "BLS publishes no comparable revised November two-month value in the December release.",
        )
    return _BlsCpiObservation(
        reference_period=reference,
        release_date=release_date,
        released_lexical=lexical,
        reported_zone=reported_zone,
        actual_value=actual,
        actual_lexical=actual_lexical,
        revised_previous_value=revised,
        revised_previous_lexical=revised_lexical,
        transformation=transformation,
        release_time_locator=f"line:{release_line}:release-header",
        actual_locator=actual_locator,
        revision_locator=revision_locator,
        limitations=limitations,
    )


def parse_bls_cpi_release(
    snapshot: OfficialRawSnapshotV1,
    *,
    reference_period: str,
    previous_snapshot: OfficialRawSnapshotV1,
    previous_reference_period: str,
) -> UnitedStatesReleaseTripletV1:
    """Parse one CPI headline with its occurrence-specific predecessor."""
    current = _parse_bls_cpi_observation(snapshot, reference_period)
    previous = _parse_bls_cpi_observation(
        previous_snapshot, previous_reference_period
    )
    if previous.reference_period >= current.reference_period:
        raise ValueError("CPI predecessor does not precede the current release")
    comparable = current.revised_previous_value is not None
    revised_value = (
        cast(float, current.revised_previous_value)
        if comparable
        else previous.actual_value
    )
    revised_lexical = (
        cast(str, current.revised_previous_lexical)
        if comparable
        else previous.actual_lexical
    )
    timestamp = normalize_official_source_timestamp(
        current.released_lexical,
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    evidence_limitation = (
        "Previous-as-known evidence: "
        f"{previous_snapshot.request.uri} sha256:{previous_snapshot.content_sha256}."
    )
    return UnitedStatesReleaseTripletV1(
        program_key=BLS_CPI_PROGRAM_KEY,
        logical_event_key=(
            "us.bls.cpi.cpi-u-all-items."
            f"{current.transformation}.{current.reference_period}"
        ),
        event_family=EconomicEventFamily.INFLATION_PRICES,
        reference_period=current.reference_period,
        previous_reference_period=previous.reference_period,
        scheduled_lexical=current.released_lexical,
        released_lexical=current.released_lexical,
        source_timezone="America/New_York",
        scheduled_for_ns=timestamp.utc_ns,
        released_at_ns=timestamp.utc_ns,
        actual_value=current.actual_value,
        actual_lexical=current.actual_lexical,
        previous_as_known_value=previous.actual_value,
        previous_as_known_lexical=previous.actual_lexical,
        revised_previous_value=revised_value,
        revised_previous_lexical=revised_lexical,
        unit="percent",
        transformation=current.transformation,
        seasonality="seasonally-adjusted",
        snapshot_id=snapshot.snapshot_id,
        content_sha256=snapshot.content_sha256,
        source_locator=snapshot.resolved_uri,
        release_time_locator=current.release_time_locator,
        actual_locator=current.actual_locator,
        previous_locator=(
            f"{previous_snapshot.request.uri}::{previous.actual_locator}"
        ),
        revision_locator=current.revision_locator,
        limitations=current.limitations + (evidence_limitation,),
    )


def _snapshots_by_uri(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> Mapping[str, OfficialRawSnapshotV1]:
    result: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, OfficialRawSnapshotV1):
            raise TypeError("CPI archive requires v1 official snapshots")
        request = snapshot.request
        release_date = _release_date_from_uri(request.uri)
        if (
            request.source_key != BLS_CPI_SOURCE_KEY
            or request.source_format != _source_format_for_uri(request.uri)
            or request.window_start != release_date
            or request.window_end != release_date
        ):
            raise ValueError("CPI archive snapshot has a different scope")
        if request.uri in result:
            raise ValueError("CPI archive repeats an artifact snapshot")
        result[request.uri] = snapshot
    return MappingProxyType(result)


def _entry_from_triplet(
    indexed: BlsCpiReleaseIndexEntryV1,
    current: OfficialRawSnapshotV1,
    previous: OfficialRawSnapshotV1,
    triplet: UnitedStatesReleaseTripletV1,
) -> BlsCpiArchiveEntryV1:
    comparable = triplet.reference_period != "2025-12"
    return BlsCpiArchiveEntryV1(
        reference_period=triplet.reference_period,
        previous_reference_period=triplet.previous_reference_period,
        release_date=indexed.release_date,
        artifact_uri=current.request.uri,
        source_format=current.request.source_format,
        content_sha256=current.content_sha256,
        content_length=len(current.content),
        previous_artifact_uri=previous.request.uri,
        previous_content_sha256=previous.content_sha256,
        previous_content_length=len(previous.content),
        normalized_sha256=triplet.normalized_sha256,
        released_lexical=triplet.released_lexical,
        actual_value=triplet.actual_value,
        actual_lexical=triplet.actual_lexical,
        previous_as_known_value=triplet.previous_as_known_value,
        previous_as_known_lexical=triplet.previous_as_known_lexical,
        revised_previous_value=triplet.revised_previous_value,
        revised_previous_lexical=triplet.revised_previous_lexical,
        transformation=triplet.transformation,
        revision_comparable=comparable,
    )


def build_bls_cpi_archive_manifest(
    profile: UnitedStatesBackfillProfileV1,
    release_index: BlsCpiReleaseIndexV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> BlsCpiArchiveManifestV1:
    """Replay every indexed CPI release into compact closure evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("CPI manifest requires a v1 U.S. profile")
    program = profile.by_key.get(BLS_CPI_PROGRAM_KEY)
    if program is None or program.source_key != BLS_CPI_SOURCE_KEY:
        raise ValueError("U.S. profile omits the dedicated CPI archive source")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {BLS_CPI_PREDECESSOR_URI} | {
        item.artifact_uri for item in release_index.releases
    }
    if set(by_uri) != expected_uris:
        raise ValueError("CPI snapshots differ from the release index")
    entries: list[BlsCpiArchiveEntryV1] = []
    previous_snapshot = by_uri[BLS_CPI_PREDECESSOR_URI]
    previous_reference = "1999-12"
    for indexed in release_index.releases:
        current = by_uri[indexed.artifact_uri]
        triplet = parse_bls_cpi_release(
            current,
            reference_period=indexed.reference_period,
            previous_snapshot=previous_snapshot,
            previous_reference_period=previous_reference,
        )
        entries.append(
            _entry_from_triplet(indexed, current, previous_snapshot, triplet)
        )
        previous_snapshot = current
        previous_reference = indexed.reference_period
    artifact_evidence = {
        (item.request.uri, item.content_sha256): len(item.content)
        for item in by_uri.values()
    }
    values = tuple(entries)
    return BlsCpiArchiveManifestV1(
        registry_id=profile.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        entries=values,
        raw_artifact_count=len(artifact_evidence),
        total_content_bytes=sum(artifact_evidence.values()),
        revision_occurrence_count=sum(
            item.previous_was_revised for item in values
        ),
        noncomparable_revision_count=sum(
            not item.revision_comparable for item in values
        ),
    )


def replay_bls_cpi_archive(
    manifest: BlsCpiArchiveManifestV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[UnitedStatesReleaseTripletV1, ...]:
    """Recompute and verify every normalized CPI archive entry."""
    if not isinstance(manifest, BlsCpiArchiveManifestV1):
        raise TypeError("CPI replay requires a v1 archive manifest")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {BLS_CPI_PREDECESSOR_URI} | {
        item.artifact_uri for item in manifest.entries
    }
    if set(by_uri) != expected_uris:
        raise ValueError("CPI replay snapshots differ from the manifest")
    triplets: list[UnitedStatesReleaseTripletV1] = []
    for indexed, expected in zip(
        manifest.release_index.releases, manifest.entries
    ):
        current = by_uri[expected.artifact_uri]
        previous = by_uri[expected.previous_artifact_uri]
        triplet = parse_bls_cpi_release(
            current,
            reference_period=expected.reference_period,
            previous_snapshot=previous,
            previous_reference_period=expected.previous_reference_period,
        )
        if _entry_from_triplet(indexed, current, previous, triplet) != expected:
            raise ValueError(
                f"CPI replay differs for {expected.reference_period}"
            )
        triplets.append(triplet)
    return tuple(triplets)


def bls_cpi_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: BlsCpiArchiveManifestV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete CPI coverage only from verified manifest evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("CPI coverage requires a v1 U.S. profile")
    if not isinstance(manifest, BlsCpiArchiveManifestV1):
        raise TypeError("CPI coverage requires a v1 archive manifest")
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("CPI manifest differs from the U.S. profile")
    program = profile.by_key[BLS_CPI_PROGRAM_KEY]
    end = _iso_date(window_end_date, "window_end_date")
    if end < max(item.release_date for item in manifest.entries):
        raise ValueError("CPI coverage end precedes the latest release")
    count = len(manifest.entries)
    unavailable = ", ".join(
        manifest.release_index.unavailable_reference_periods
    )
    coverage = UnitedStatesProgramCoverageV1(
        program_key=program.program_key,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=end,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=count,
        previous_as_known_count=count,
        revision_count=manifest.revision_occurrence_count,
        exact_minute_count=count,
        forecast_count=0,
        artifact_sha256s=tuple(
            item.content_sha256 for item in manifest.entries
        ),
        gap_reasons=(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,),
        notes=(
            f"Release index: {manifest.release_index.index_id}",
            f"Archive manifest: {manifest.manifest_id}",
            f"Official unavailable reference periods: {unavailable}",
            (
                "One noncomparable revision field is explicitly retained for "
                "December 2025 after the appropriations lapse."
            ),
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError("CPI manifest does not qualify complete coverage")
    return coverage


def packaged_bls_cpi_index_path() -> Path:
    """Return the packaged base64 envelope for the retained CPI index."""
    return (
        Path(__file__).with_name("assets") / "us_cpi_release_index_v1.html.b64"
    )


def packaged_bls_cpi_archive_manifest_path() -> Path:
    """Return the packaged compact CPI archive-manifest path."""
    return Path(__file__).with_name("assets") / "us_cpi_archive_v1.json"


def load_packaged_bls_cpi_archive_manifest() -> BlsCpiArchiveManifestV1:
    """Load and validate the packaged complete as-of CPI manifest."""
    path = packaged_bls_cpi_archive_manifest_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            "packaged CPI archive manifest is unavailable"
        ) from exc
    return BlsCpiArchiveManifestV1.from_json(text)


def load_packaged_bls_cpi_index() -> bytes:
    """Decode and hash-check the exact CPI index used by the manifest."""
    path = packaged_bls_cpi_index_path()
    try:
        content = base64.b64decode(
            path.read_text(encoding="ascii").strip(), validate=True
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError(
            "packaged CPI index is unavailable or invalid"
        ) from exc
    manifest = load_packaged_bls_cpi_archive_manifest()
    if (
        len(content) != manifest.release_index.content_length
        or hashlib.sha256(content).hexdigest()
        != manifest.release_index.content_sha256
    ):
        raise ValueError("packaged CPI index differs from its manifest")
    return content


__all__ = [
    "BLS_CPI_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "BLS_CPI_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "BLS_CPI_INDEX_ENTRY_SCHEMA_VERSION",
    "BLS_CPI_INDEX_SCHEMA_VERSION",
    "BLS_CPI_INDEX_URI",
    "BLS_CPI_PDF_SUBSTITUTION_NOTE",
    "BLS_CPI_PREDECESSOR_URI",
    "BLS_CPI_PROGRAM_KEY",
    "BLS_CPI_SOURCE_KEY",
    "BlsCpiArchiveEntryV1",
    "BlsCpiArchiveManifestV1",
    "BlsCpiReleaseIndexEntryV1",
    "BlsCpiReleaseIndexV1",
    "bls_cpi_coverage_from_manifest",
    "build_bls_cpi_archive_manifest",
    "build_bls_cpi_archive_requests",
    "build_bls_cpi_index_request",
    "load_packaged_bls_cpi_archive_manifest",
    "load_packaged_bls_cpi_index",
    "packaged_bls_cpi_archive_manifest_path",
    "packaged_bls_cpi_index_path",
    "parse_bls_cpi_release",
    "parse_bls_cpi_release_index",
    "replay_bls_cpi_archive",
]

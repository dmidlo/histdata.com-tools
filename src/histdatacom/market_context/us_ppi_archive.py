"""Complete BLS Producer Price Index archive qualification for issue #538.

The packaged manifest binds the official BLS PPI release index to the
finished-goods headline through 2013 and the final-demand headline from 2014
through its explicit as-of date. Release bytes remain an external corpus.
Each manifest entry nevertheless binds the current and predecessor artifacts,
their independently normalized values, the exact 08:30 America/New_York
publication time, and the source-format era used to recover the headline.
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
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

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

BLS_PPI_INDEX_SCHEMA_VERSION = "histdatacom.bls-ppi-index.v1"
BLS_PPI_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.bls-ppi-index-entry.v1"
BLS_PPI_ARCHIVE_ENTRY_SCHEMA_VERSION = "histdatacom.bls-ppi-archive-entry.v1"
BLS_PPI_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.bls-ppi-archive-manifest.v1"
)

BLS_PPI_SOURCE_KEY = "us.bls.ppi"
BLS_PPI_PROGRAM_KEY = "us.bls.ppi"
BLS_PPI_INDEX_URI = "https://www.bls.gov/bls/news-release/ppi.htm"
BLS_PPI_PREDECESSOR_URI = (
    "https://www.bls.gov/news.release/history/ppi_01132000.txt"
)
BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD = "2014-01"
BLS_PPI_MALFORMED_HEADER_REFERENCE_PERIOD = "2016-12"
MAX_BLS_PPI_RELEASES = 512
MAX_BLS_PPI_INDEX_BYTES = 4 * 1024 * 1024
MAX_BLS_PPI_RELEASE_BYTES = 8 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_PPI_URI_RE = re.compile(
    r"^https://www\.bls\.gov/news\.release/(?P<section>archives|history)/"
    r"ppi_(?P<date>\d{8})\.(?P<suffix>htm|txt)$"
)
_PPI_RELATIVE_URI_RE = re.compile(
    r"href=[\"'](?P<uri>/news\.release/(?:archives|history)/"
    r"ppi_(?P<date>\d{8})\.(?P<suffix>htm|txt))[\"']",
    re.IGNORECASE,
)
_PPI_NUMBER_RE = re.compile(
    r"(?<![\w.])(?P<prefix>[rR])?\s*"
    r"(?P<number>[-+]?(?:\d+(?:\.\d+)?|\.\d+))"
    r"(?P<suffix>[rR])?(?![\w.])"
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


def _ppi_lexical_number(value: object, name: str) -> tuple[str, float]:
    lexical = _required_text(value, name)
    match = re.fullmatch(
        r"(?P<number>[-+]?(?:\d+(?:\.\d+)?|\.\d+))(?P<revision>[rR])?",
        lexical,
    )
    if match is None:
        raise ValueError(f"{name} is not a PPI numeric lexical value")
    normalized = match.group("number")
    if normalized.startswith(("-.", "+.")):
        normalized = normalized[:1] + "0" + normalized[1:]
    elif normalized.startswith("."):
        normalized = "0" + normalized
    return lexical, _finite(normalized, name)


def _release_date_from_uri(uri: str) -> str:
    match = _PPI_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("PPI artifact URI is invalid")
    try:
        return (
            datetime.strptime(match.group("date"), "%m%d%Y")
            .replace(tzinfo=timezone.utc)
            .date()
            .isoformat()
        )
    except ValueError as exc:
        raise ValueError("PPI artifact URI contains an invalid date") from exc


def _source_format_for_uri(uri: str) -> OfficialSourceFormat:
    match = _PPI_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("PPI artifact URI is invalid")
    return {
        "htm": OfficialSourceFormat.HTML,
        "txt": OfficialSourceFormat.TEXT,
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
class BlsPpiReleaseIndexEntryV1:
    """One published reference-period row from the official archive index."""

    reference_period: str
    release_date: str
    indexed_artifact_uri: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    selection_note: str | None = None
    entry_id: str = ""
    schema_version: str = BLS_PPI_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_PPI_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BLS PPI index-entry schema")
        reference = _year_month(self.reference_period, "reference_period")
        release = _iso_date(self.release_date, "release_date")
        indexed = _https_uri(self.indexed_artifact_uri, "indexed_artifact_uri")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format != _source_format_for_uri(artifact):
            raise ValueError("PPI selected format differs from artifact URI")
        if release != _release_date_from_uri(indexed):
            raise ValueError("PPI release date differs from indexed URI")
        note = _optional_text(self.selection_note)
        if (
            artifact != indexed
            or source_format
            not in {
                OfficialSourceFormat.HTML,
                OfficialSourceFormat.TEXT,
            }
            or note is not None
        ):
            raise ValueError("PPI index entry has an unexplained substitution")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "indexed_artifact_uri", indexed)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "selection_note", note)
        expected = _stable_id("bls-ppi-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BLS PPI index-entry identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> BlsPpiReleaseIndexEntryV1:
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
class BlsPpiReleaseIndexV1:
    """As-of interpretation of the retained BLS PPI archive page."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[BlsPpiReleaseIndexEntryV1, ...]
    unavailable_reference_periods: tuple[str, ...]
    index_id: str = ""
    schema_version: str = BLS_PPI_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_PPI_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported BLS PPI index schema")
        if self.source_uri != BLS_PPI_INDEX_URI:
            raise ValueError("BLS PPI index URI is invalid")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_PPI_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        values = tuple(self.releases)
        if (
            not values
            or len(values) > MAX_BLS_PPI_RELEASES
            or any(
                not isinstance(item, BlsPpiReleaseIndexEntryV1)
                for item in values
            )
        ):
            raise TypeError("BLS PPI index releases are invalid")
        references = tuple(item.reference_period for item in values)
        if references != tuple(sorted(set(references))):
            raise ValueError("BLS PPI index releases must be unique and sorted")
        if references[0] != "2000-01":
            raise ValueError("BLS PPI index omits the January 2000 boundary")
        if any(item.release_date > as_of for item in values):
            raise ValueError("BLS PPI index contains a future publication")
        if len({item.release_date for item in values}) != len(values):
            raise ValueError("BLS PPI index repeats a publication date")
        if tuple(item.release_date for item in values) != tuple(
            sorted(item.release_date for item in values)
        ):
            raise ValueError("BLS PPI publication dates are not chronological")
        unavailable = tuple(
            _year_month(item, "unavailable_reference_periods")
            for item in self.unavailable_reference_periods
        )
        if unavailable != tuple(sorted(set(unavailable))):
            raise ValueError(
                "unavailable_reference_periods must be unique and sorted"
            )
        if set(references) & set(unavailable):
            raise ValueError("published and unavailable PPI periods overlap")
        observed_periods = tuple(sorted((*references, *unavailable)))
        if observed_periods != _contiguous_months(
            "2000-01", observed_periods[-1]
        ):
            raise ValueError("BLS PPI index has an unexplained monthly gap")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", values)
        object.__setattr__(self, "unavailable_reference_periods", unavailable)
        expected = _stable_id("bls-ppi-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("BLS PPI index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, BlsPpiReleaseIndexEntryV1]:
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
    def from_dict(cls, data: Mapping[str, Any]) -> BlsPpiReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                BlsPpiReleaseIndexEntryV1.from_dict(
                    _mapping(item, "PPI index entry")
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
class BlsPpiArchiveEntryV1:
    """One current/predecessor-backed normalized PPI release occurrence."""

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
    series_lineage: str
    transformation: str
    revision_comparable: bool
    entry_id: str = ""
    schema_version: str = BLS_PPI_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_PPI_ARCHIVE_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BLS PPI archive-entry schema")
        reference = _year_month(self.reference_period, "reference_period")
        previous_reference = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_reference >= reference:
            raise ValueError(
                "PPI previous reference period must precede current"
            )
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        previous_artifact = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format != _source_format_for_uri(artifact):
            raise ValueError("PPI entry format differs from artifact URI")
        if release != _release_date_from_uri(artifact):
            raise ValueError("PPI entry release date differs from artifact URI")
        for name in (
            "content_sha256",
            "previous_content_sha256",
            "normalized_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_PPI_RELEASE_BYTES,
        )
        _positive_int(
            self.previous_content_length,
            "previous_content_length",
            MAX_BLS_PPI_RELEASE_BYTES,
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T08:30:00":
            raise ValueError("PPI released time differs from release date")
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
            lexical, numeric = _ppi_lexical_number(
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
        lineage = _required_text(self.series_lineage, "series_lineage")
        expected_lineage = (
            "finished-goods"
            if reference < BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD
            else "final-demand"
        )
        if lineage != expected_lineage:
            raise ValueError("PPI series lineage differs from reference period")
        transformation = _required_text(self.transformation, "transformation")
        if transformation != "month-over-month-percent-change":
            raise ValueError("PPI transformation is invalid")
        if not isinstance(self.revision_comparable, bool):
            raise TypeError("revision_comparable must be boolean")
        if (
            not self.revision_comparable
            and reference != BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD
        ):
            raise ValueError("only the PPI lineage transition is noncomparable")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(
            self, "previous_reference_period", previous_reference
        )
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "previous_artifact_uri", previous_artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "series_lineage", lineage)
        object.__setattr__(self, "transformation", transformation)
        expected = _stable_id("bls-ppi-archive-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BLS PPI archive-entry identity differs")
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
            "series_lineage": self.series_lineage,
            "transformation": self.transformation,
            "revision_comparable": self.revision_comparable,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsPpiArchiveEntryV1:
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
            series_lineage=str(data.get("series_lineage", "")),
            transformation=str(data.get("transformation", "")),
            revision_comparable=cast(bool, data.get("revision_comparable")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsPpiArchiveManifestV1:
    """Compact closure evidence for the complete as-of BLS PPI archive."""

    registry_id: str
    profile_id: str
    release_index: BlsPpiReleaseIndexV1
    entries: tuple[BlsPpiArchiveEntryV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    revision_occurrence_count: int
    noncomparable_revision_count: int
    manifest_id: str = ""
    schema_version: str = BLS_PPI_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_PPI_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported BLS PPI archive-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("PPI manifest registry identity is invalid")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("PPI manifest profile identity is invalid")
        if not isinstance(self.release_index, BlsPpiReleaseIndexV1):
            raise TypeError("PPI manifest requires a v1 release index")
        entries = tuple(self.entries)
        if (
            not entries
            or len(entries) > MAX_BLS_PPI_RELEASES
            or any(
                not isinstance(item, BlsPpiArchiveEntryV1) for item in entries
            )
        ):
            raise TypeError("PPI manifest entries are invalid")
        references = tuple(item.reference_period for item in entries)
        if references != tuple(
            item.reference_period for item in self.release_index.releases
        ):
            raise ValueError("PPI manifest differs from its release index")
        releases = self.release_index.by_reference_period
        for item in entries:
            indexed = releases[item.reference_period]
            if (
                item.release_date != indexed.release_date
                or item.artifact_uri != indexed.artifact_uri
                or item.source_format is not indexed.source_format
            ):
                raise ValueError("PPI manifest entry differs from the index")
        for previous, current in pairwise(entries):
            if (
                current.previous_reference_period != previous.reference_period
                or current.previous_artifact_uri != previous.artifact_uri
                or current.previous_content_sha256 != previous.content_sha256
                or current.previous_content_length != previous.content_length
            ):
                raise ValueError("PPI predecessor artifact chain is broken")
        first = entries[0]
        if (
            first.previous_reference_period != "1999-12"
            or first.previous_artifact_uri != BLS_PPI_PREDECESSOR_URI
        ):
            raise ValueError("PPI manifest omits its December 1999 predecessor")
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
            raise ValueError("PPI manifest raw artifact count differs")
        if self.total_content_bytes != total:
            raise ValueError("PPI manifest total byte count differs")
        if self.revision_occurrence_count != revisions:
            raise ValueError("PPI manifest revision count differs")
        if self.noncomparable_revision_count != noncomparable:
            raise ValueError("PPI manifest noncomparable count differs")
        if len({item.content_sha256 for item in entries}) != len(entries):
            raise ValueError("PPI manifest repeats current artifact content")
        if len({item[1] for item in artifact_evidence}) != raw_count:
            raise ValueError("PPI manifest repeats a raw artifact hash")
        if len({item.normalized_sha256 for item in entries}) != len(entries):
            raise ValueError("PPI manifest repeats normalized content")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "bls-ppi-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("BLS PPI archive identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_reference_period(self) -> Mapping[str, BlsPpiArchiveEntryV1]:
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
    def from_dict(cls, data: Mapping[str, Any]) -> BlsPpiArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=BlsPpiReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            entries=tuple(
                BlsPpiArchiveEntryV1.from_dict(_mapping(item, "archive entry"))
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
    def from_json(cls, text: str) -> BlsPpiArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("PPI archive manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "archive manifest"))


def build_bls_ppi_index_request(
    registry: OfficialSourceRegistryV1,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    """Plan the exact official PPI archive-index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("PPI index request requires a v1 source registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(BLS_PPI_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=BLS_PPI_INDEX_URI,
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


def parse_bls_ppi_release_index(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> BlsPpiReleaseIndexV1:
    """Parse published and explicitly unavailable PPI reference periods."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("PPI index parser requires a v1 official snapshot")
    if (
        snapshot.request.source_key != BLS_PPI_SOURCE_KEY
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.uri != BLS_PPI_INDEX_URI
    ):
        raise ValueError("PPI index parser received a different source")
    as_of = _iso_date(as_of_date, "as_of_date")
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("BLS PPI release index is not UTF-8") from exc
    releases: list[BlsPpiReleaseIndexEntryV1] = []
    unavailable: list[str] = []
    seen: set[str] = set()
    for list_item in re.finditer(r"(?is)<li\b[^>]*>(.*?)</li>", text):
        markup = list_item.group(1)
        label = _visible_html(markup)
        label_match = re.search(
            r"(?P<month>January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(?P<year>\d{4})\s+"
            r"Producer Price Index",
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
            raise ValueError("BLS PPI index repeats a reference period")
        seen.add(reference)
        uri_match = _PPI_RELATIVE_URI_RE.search(markup)
        if uri_match is None:
            if "not published" not in label.lower():
                raise ValueError("BLS PPI index row has no release artifact")
            unavailable.append(reference)
            continue
        indexed_uri = "https://www.bls.gov" + uri_match.group("uri")
        release_date = _release_date_from_uri(indexed_uri)
        if release_date > as_of:
            continue
        releases.append(
            BlsPpiReleaseIndexEntryV1(
                reference_period=reference,
                release_date=release_date,
                indexed_artifact_uri=indexed_uri,
                artifact_uri=indexed_uri,
                source_format=_source_format_for_uri(indexed_uri),
            )
        )
    return BlsPpiReleaseIndexV1(
        source_uri=BLS_PPI_INDEX_URI,
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
    source = registry.source(BLS_PPI_SOURCE_KEY)
    source_format = _source_format_for_uri(uri)
    if source_format not in source.formats:
        raise ValueError("PPI source does not declare the artifact format")
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


def build_bls_ppi_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: BlsPpiReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the predecessor plus every selected published artifact request."""
    if not isinstance(release_index, BlsPpiReleaseIndexV1):
        raise TypeError("PPI archive requests require a v1 release index")
    uris = (BLS_PPI_PREDECESSOR_URI,) + tuple(
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
                "PPI release is neither UTF-8 nor Windows-1252"
            ) from exc


def _strip_html(value: str) -> str:
    result = re.sub(r"(?i)<br\s*/?>", "\n", value)
    result = re.sub(r"(?i)</(?:p|div|tr|li|h\d)>", "\n", result)
    return html.unescape(re.sub(r"<[^>]+>", " ", result)).replace("\xa0", " ")


def _main_html_release(value: str) -> str:
    candidates: list[str] = []
    for match in re.finditer(r"(?is)<pre\b[^>]*>(.*?)</pre>", value):
        candidate = _strip_html(match.group(1))
        if "8:30" in candidate and "PRODUCER PRICE INDEX" in candidate.upper():
            candidates.append(candidate)
    if candidates:
        return candidates[0]
    visible = _strip_html(value)
    if "8:30" in visible and "PRODUCER PRICE INDEX" in visible.upper():
        return visible
    raise ValueError("PPI HTML release body is missing or changed")


_PPI_MONTH_RE = re.compile(
    r"^(?P<month>Jan|Feb|Mar|Apr|May|June|July|Aug|Sept?|Oct|Nov|Dec)\.?"
    r"(?:\s*\(\s*\d+\s*\))?$",
    re.IGNORECASE,
)


def _table_number(value: str, *, full: bool) -> str | None:
    match = (
        _PPI_NUMBER_RE.fullmatch(value)
        if full
        else _PPI_NUMBER_RE.search(value)
    )
    if match is None:
        return None
    return match.group("number") + (
        "r" if match.group("prefix") or match.group("suffix") else ""
    )


def _ppi_month_number(value: str) -> int:
    key = value[:3].lower()
    for name, number in _MONTH_NUMBERS.items():
        if name[:3] == key:
            return number
    raise ValueError("PPI Table A contains an unknown month")


def _table_values_from_html(
    value: str,
) -> tuple[Mapping[str, str], str] | None:
    table = re.search(
        r"(?is)<table\b[^>]*\bid=[\"'](?P<id>[^\"']*tablea)[\"']"
        r"[^>]*>(?P<body>.*?)</table>",
        value,
    )
    if table is None:
        return None
    year: int | None = None
    values: dict[str, str] = {}
    for row in re.finditer(r"(?is)<tr\b[^>]*>(.*?)</tr>", table.group("body")):
        cells = tuple(
            " ".join(_strip_html(item).split())
            for item in re.findall(
                r"(?is)<t[hd]\b[^>]*>(.*?)</t[hd]>", row.group(1)
            )
        )
        if not cells:
            continue
        if re.fullmatch(r"(?:19|20)\d{2}", cells[0]):
            year = int(cells[0])
            continue
        month_match = _PPI_MONTH_RE.fullmatch(cells[0])
        if month_match is None or year is None:
            continue
        lexical = next(
            (
                parsed
                for item in cells[1:]
                if (parsed := _table_number(item, full=True)) is not None
            ),
            None,
        )
        if lexical is None:
            raise ValueError("PPI HTML Table A row has no total value")
        reference = (
            f"{year:04d}-{_ppi_month_number(month_match.group('month')):02d}"
        )
        if reference in values:
            raise ValueError("PPI HTML Table A repeats a reference period")
        values[reference] = lexical
    if not values:
        raise ValueError("PPI HTML Table A has no monthly values")
    return MappingProxyType(values), f"table#{table.group('id')}"


def _table_values_from_text(value: str) -> tuple[Mapping[str, str], str]:
    start = value.lower().find("table a.")
    if start < 0:
        raise ValueError("PPI release omits Table A")
    table = value[start:]
    end = re.search(
        r"(?im)^\s*(?:r\s*=\s*revised|note:|table b\.)",
        table,
    )
    if end is not None:
        table = table[: end.start()]
    year: int | None = None
    values: dict[str, str] = {}
    first_line = value[:start].count("\n") + 1
    for line in table.splitlines():
        year_match = re.fullmatch(r"\s*((?:19|20)\d{2})\s*", line)
        if year_match is not None:
            year = int(year_match.group(1))
            continue
        row_match = re.match(
            r"^\s*(?P<month>Jan|Feb|Mar|Apr|May|June|July|Aug|Sept?|"
            r"Oct|Nov|Dec)\.?\s+(?P<values>.*)$",
            line,
            re.IGNORECASE,
        )
        if row_match is None or year is None:
            continue
        lexical = _table_number(row_match.group("values"), full=False)
        if lexical is None:
            raise ValueError("PPI text Table A row has no total value")
        reference = (
            f"{year:04d}-{_ppi_month_number(row_match.group('month')):02d}"
        )
        if reference in values:
            raise ValueError("PPI text Table A repeats a reference period")
        values[reference] = lexical
        if len(values) > 24:
            raise ValueError("PPI text Table A exceeds its month bound")
    if not values:
        raise ValueError("PPI text Table A has no monthly values")
    return MappingProxyType(values), f"line:{first_line}:table-a"


@dataclass(frozen=True, slots=True)
class _BlsPpiObservation:
    reference_period: str
    release_date: str
    released_lexical: str
    reported_zone: str
    actual_value: float
    actual_lexical: str
    revised_previous_value: float | None
    revised_previous_lexical: str | None
    series_lineage: str
    transformation: str
    release_time_locator: str
    actual_locator: str
    revision_locator: str
    limitations: tuple[str, ...]


def _parse_bls_ppi_observation(
    snapshot: OfficialRawSnapshotV1,
    reference_period: str,
    *,
    comparison_reference_period: str | None = None,
) -> _BlsPpiObservation:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("PPI parser requires a v1 official snapshot")
    if snapshot.request.source_key != BLS_PPI_SOURCE_KEY:
        raise ValueError("PPI parser received a different official source")
    reference = _year_month(reference_period, "reference_period")
    source_format = snapshot.request.source_format
    if source_format is not _source_format_for_uri(snapshot.request.uri):
        raise ValueError("PPI request format differs from its artifact URI")
    if source_format not in {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }:
        raise ValueError("PPI parser received an unsupported source format")
    text, source_encoding = _decode_release_text(snapshot.content)
    raw_html = text if source_format is OfficialSourceFormat.HTML else None
    main_text = (
        _main_html_release(text)
        if source_format is OfficialSourceFormat.HTML
        else text
    )
    release_date = _release_date_from_uri(snapshot.request.uri)
    release = date.fromisoformat(release_date)
    header = " ".join(main_text[:7000].split())
    date_pattern = (
        rf"{release.strftime('%B')}\s+{release.day},\s*{release.year}"
    )
    if reference == BLS_PPI_MALFORMED_HEADER_REFERENCE_PERIOD:
        date_pattern = (
            rf"{release.strftime('%B')},\s*{release.day},\s*{release.year}"
        )
    if (
        re.search(
            date_pattern,
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("PPI release date is absent from the header")
    time_match = re.search(
        r"8:30\s*(?:A\.?M\.?)\s*\((?P<zone>E\.?S\.?T\.?|" r"E\.?D\.?T\.?|ET)\)",
        main_text,
        re.IGNORECASE,
    )
    if time_match is None:
        raise ValueError("PPI 08:30 release-time header is missing")
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
        raise ValueError("PPI release timezone conflicts with release date")
    year, month = (int(item) for item in reference.split("-"))
    if (
        re.search(
            rf"PRODUCER PRICE INDEX(?:ES)?\s*[-–—]+\s*"
            rf"{calendar.month_name[month]}\s+{year}",
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("PPI reference period is absent from the title")
    html_values = (
        _table_values_from_html(raw_html) if raw_html is not None else None
    )
    values, table_locator = (
        html_values
        if html_values is not None
        else _table_values_from_text(main_text)
    )
    try:
        actual_lexical = values[reference]
    except KeyError as exc:
        raise ValueError("PPI Table A omits the reference period") from exc
    actual_lexical, actual = _ppi_lexical_number(
        actual_lexical, "actual_lexical"
    )
    comparison = (
        None
        if comparison_reference_period is None
        else _year_month(
            comparison_reference_period, "comparison_reference_period"
        )
    )
    if comparison is not None and comparison >= reference:
        raise ValueError("PPI comparison period must precede the reference")
    revised: float | None = None
    revised_lexical: str | None = None
    revision_locator = f"{table_locator}:no-comparison-requested"
    if comparison is not None:
        revision_locator = f"{table_locator}:row:{comparison}:total-headline"
        if reference != BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD:
            try:
                revised_lexical = values[comparison]
            except KeyError as exc:
                raise ValueError(
                    "PPI Table A omits the comparison period"
                ) from exc
            revised_lexical, revised = _ppi_lexical_number(
                revised_lexical, "revised_previous_lexical"
            )
        else:
            revision_locator = "table-a:finished-goods-to-final-demand-break"
    release_line = main_text[: time_match.start()].count("\n") + 1
    lineage = (
        "finished-goods"
        if reference < BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD
        else "final-demand"
    )
    limitations: tuple[str, ...] = (
        f"This parser qualifies the PPI {lineage} headline only.",
        "Values are seasonally adjusted percent changes, not index levels or year-over-year rates.",
        f"The source header reports {reported_zone}; timezone normalization uses America/New_York.",
        f"The retained source format is {source_format.value} and text decoding is {source_encoding}.",
    )
    if reference == BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD:
        limitations += (
            "Final demand starts in January 2014; the prior finished-goods headline is not a comparable revision.",
        )
    if reference == BLS_PPI_MALFORMED_HEADER_REFERENCE_PERIOD:
        limitations += (
            "The official header contains an extra comma between January and 13, 2017.",
        )
    if reference == "2025-11":
        limitations += (
            "October 2025 was not published; the November monthly change uses the subsequently calculated October index while previous-as-known remains September.",
        )
    if reference >= "2021-11":
        limitations += (
            "BLS interim PPI indexes are republished monthly during the four-month revision window.",
        )
    return _BlsPpiObservation(
        reference_period=reference,
        release_date=release_date,
        released_lexical=lexical,
        reported_zone=reported_zone,
        actual_value=actual,
        actual_lexical=actual_lexical,
        revised_previous_value=revised,
        revised_previous_lexical=revised_lexical,
        series_lineage=lineage,
        transformation="month-over-month-percent-change",
        release_time_locator=f"line:{release_line}:release-header",
        actual_locator=f"{table_locator}:row:{reference}:total-headline",
        revision_locator=revision_locator,
        limitations=limitations,
    )


def parse_bls_ppi_release(
    snapshot: OfficialRawSnapshotV1,
    *,
    reference_period: str,
    previous_snapshot: OfficialRawSnapshotV1,
    previous_reference_period: str,
) -> UnitedStatesReleaseTripletV1:
    """Parse one PPI headline with its occurrence-specific predecessor."""
    current = _parse_bls_ppi_observation(
        snapshot,
        reference_period,
        comparison_reference_period=previous_reference_period,
    )
    previous = _parse_bls_ppi_observation(
        previous_snapshot, previous_reference_period
    )
    if previous.reference_period >= current.reference_period:
        raise ValueError("PPI predecessor does not precede the current release")
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
        program_key=BLS_PPI_PROGRAM_KEY,
        logical_event_key=(
            f"us.bls.ppi.{current.series_lineage}."
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
            raise TypeError("PPI archive requires v1 official snapshots")
        request = snapshot.request
        release_date = _release_date_from_uri(request.uri)
        if (
            request.source_key != BLS_PPI_SOURCE_KEY
            or request.source_format != _source_format_for_uri(request.uri)
            or request.window_start != release_date
            or request.window_end != release_date
        ):
            raise ValueError("PPI archive snapshot has a different scope")
        if request.uri in result:
            raise ValueError("PPI archive repeats an artifact snapshot")
        result[request.uri] = snapshot
    return MappingProxyType(result)


def _entry_from_triplet(
    indexed: BlsPpiReleaseIndexEntryV1,
    current: OfficialRawSnapshotV1,
    previous: OfficialRawSnapshotV1,
    triplet: UnitedStatesReleaseTripletV1,
) -> BlsPpiArchiveEntryV1:
    comparable = (
        triplet.reference_period != BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD
    )
    lineage = (
        "finished-goods"
        if triplet.reference_period
        < BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD
        else "final-demand"
    )
    return BlsPpiArchiveEntryV1(
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
        series_lineage=lineage,
        transformation=triplet.transformation,
        revision_comparable=comparable,
    )


def build_bls_ppi_archive_manifest(
    profile: UnitedStatesBackfillProfileV1,
    release_index: BlsPpiReleaseIndexV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> BlsPpiArchiveManifestV1:
    """Replay every indexed PPI release into compact closure evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("PPI manifest requires a v1 U.S. profile")
    program = profile.by_key.get(BLS_PPI_PROGRAM_KEY)
    if program is None or program.source_key != BLS_PPI_SOURCE_KEY:
        raise ValueError("U.S. profile omits the dedicated PPI archive source")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {BLS_PPI_PREDECESSOR_URI} | {
        item.artifact_uri for item in release_index.releases
    }
    if set(by_uri) != expected_uris:
        raise ValueError("PPI snapshots differ from the release index")
    entries: list[BlsPpiArchiveEntryV1] = []
    previous_snapshot = by_uri[BLS_PPI_PREDECESSOR_URI]
    previous_reference = "1999-12"
    for indexed in release_index.releases:
        current = by_uri[indexed.artifact_uri]
        triplet = parse_bls_ppi_release(
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
    return BlsPpiArchiveManifestV1(
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


def replay_bls_ppi_archive(
    manifest: BlsPpiArchiveManifestV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[UnitedStatesReleaseTripletV1, ...]:
    """Recompute and verify every normalized PPI archive entry."""
    if not isinstance(manifest, BlsPpiArchiveManifestV1):
        raise TypeError("PPI replay requires a v1 archive manifest")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {BLS_PPI_PREDECESSOR_URI} | {
        item.artifact_uri for item in manifest.entries
    }
    if set(by_uri) != expected_uris:
        raise ValueError("PPI replay snapshots differ from the manifest")
    triplets: list[UnitedStatesReleaseTripletV1] = []
    for indexed, expected in zip(
        manifest.release_index.releases, manifest.entries
    ):
        current = by_uri[expected.artifact_uri]
        previous = by_uri[expected.previous_artifact_uri]
        triplet = parse_bls_ppi_release(
            current,
            reference_period=expected.reference_period,
            previous_snapshot=previous,
            previous_reference_period=expected.previous_reference_period,
        )
        if _entry_from_triplet(indexed, current, previous, triplet) != expected:
            raise ValueError(
                f"PPI replay differs for {expected.reference_period}"
            )
        triplets.append(triplet)
    return tuple(triplets)


def bls_ppi_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: BlsPpiArchiveManifestV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete PPI coverage only from verified manifest evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("PPI coverage requires a v1 U.S. profile")
    if not isinstance(manifest, BlsPpiArchiveManifestV1):
        raise TypeError("PPI coverage requires a v1 archive manifest")
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("PPI manifest differs from the U.S. profile")
    program = profile.by_key[BLS_PPI_PROGRAM_KEY]
    end = _iso_date(window_end_date, "window_end_date")
    if end < max(item.release_date for item in manifest.entries):
        raise ValueError("PPI coverage end precedes the latest release")
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
                "the January 2014 finished-goods to final-demand transition."
            ),
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError("PPI manifest does not qualify complete coverage")
    return coverage


def packaged_bls_ppi_index_path() -> Path:
    """Return the packaged base64 envelope for the retained PPI index."""
    return (
        Path(__file__).with_name("assets") / "us_ppi_release_index_v1.html.b64"
    )


def packaged_bls_ppi_archive_manifest_path() -> Path:
    """Return the packaged compact PPI archive-manifest path."""
    return Path(__file__).with_name("assets") / "us_ppi_archive_v1.json"


def load_packaged_bls_ppi_archive_manifest() -> BlsPpiArchiveManifestV1:
    """Load and validate the packaged complete as-of PPI manifest."""
    path = packaged_bls_ppi_archive_manifest_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            "packaged PPI archive manifest is unavailable"
        ) from exc
    return BlsPpiArchiveManifestV1.from_json(text)


def load_packaged_bls_ppi_index() -> bytes:
    """Decode and hash-check the exact PPI index used by the manifest."""
    path = packaged_bls_ppi_index_path()
    try:
        content = base64.b64decode(
            path.read_text(encoding="ascii").strip(), validate=True
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError(
            "packaged PPI index is unavailable or invalid"
        ) from exc
    manifest = load_packaged_bls_ppi_archive_manifest()
    if (
        len(content) != manifest.release_index.content_length
        or hashlib.sha256(content).hexdigest()
        != manifest.release_index.content_sha256
    ):
        raise ValueError("packaged PPI index differs from its manifest")
    return content


__all__ = [
    "BLS_PPI_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "BLS_PPI_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "BLS_PPI_FINAL_DEMAND_START_REFERENCE_PERIOD",
    "BLS_PPI_INDEX_ENTRY_SCHEMA_VERSION",
    "BLS_PPI_INDEX_SCHEMA_VERSION",
    "BLS_PPI_INDEX_URI",
    "BLS_PPI_MALFORMED_HEADER_REFERENCE_PERIOD",
    "BLS_PPI_PREDECESSOR_URI",
    "BLS_PPI_PROGRAM_KEY",
    "BLS_PPI_SOURCE_KEY",
    "BlsPpiArchiveEntryV1",
    "BlsPpiArchiveManifestV1",
    "BlsPpiReleaseIndexEntryV1",
    "BlsPpiReleaseIndexV1",
    "bls_ppi_coverage_from_manifest",
    "build_bls_ppi_archive_manifest",
    "build_bls_ppi_archive_requests",
    "build_bls_ppi_index_request",
    "load_packaged_bls_ppi_archive_manifest",
    "load_packaged_bls_ppi_index",
    "packaged_bls_ppi_archive_manifest_path",
    "packaged_bls_ppi_index_path",
    "parse_bls_ppi_release",
    "parse_bls_ppi_release_index",
    "replay_bls_ppi_archive",
]

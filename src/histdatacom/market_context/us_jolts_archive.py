"""Complete BLS JOLTS archive qualification for issue #538.

The packaged manifest binds the official BLS archive index to the total-
nonfarm seasonally adjusted levels for job openings, hires, and total
separations. Release bytes remain an external corpus. Each entry nevertheless
binds its current and predecessor artifacts, normalized values, exact 10:00
Eastern publication time, source-table era, and annual-benchmark status.
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
from urllib.parse import urljoin, urlparse
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

BLS_JOLTS_INDEX_SCHEMA_VERSION = "histdatacom.bls-jolts-index.v1"
BLS_JOLTS_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.bls-jolts-index-entry.v1"
BLS_JOLTS_MEASURE_SCHEMA_VERSION = "histdatacom.bls-jolts-measure.v1"
BLS_JOLTS_ARCHIVE_ENTRY_SCHEMA_VERSION = (
    "histdatacom.bls-jolts-archive-entry.v1"
)
BLS_JOLTS_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.bls-jolts-archive-manifest.v1"
)
BLS_JOLTS_RELEASE_SCHEMA_VERSION = "histdatacom.bls-jolts-release.v1"

BLS_JOLTS_SOURCE_KEY = "us.bls.jolts"
BLS_JOLTS_PROGRAM_KEY = "us.bls.jolts"
BLS_JOLTS_INDEX_URI = "https://www.bls.gov/bls/news-release/jolts.htm"
BLS_JOLTS_FIRST_REFERENCE_PERIOD = "2004-02"
BLS_JOLTS_FIRST_QUALIFIED_REFERENCE_PERIOD = "2004-03"
BLS_JOLTS_UNAVAILABLE_REFERENCE_PERIOD = "2025-09"
BLS_JOLTS_INDEX_TYPO_RELEASE_URI = (
    "https://www.bls.gov/news.release/archives/jolts_02122013.htm"
)
BLS_JOLTS_INDEX_TYPO_REFERENCE_PERIOD = "2012-12"

BLS_JOLTS_JOB_OPENINGS = "job-openings"
BLS_JOLTS_HIRES = "hires"
BLS_JOLTS_TOTAL_SEPARATIONS = "total-separations"
BLS_JOLTS_MEASURE_KEYS = (
    BLS_JOLTS_JOB_OPENINGS,
    BLS_JOLTS_HIRES,
    BLS_JOLTS_TOTAL_SEPARATIONS,
)

MAX_BLS_JOLTS_RELEASES = 512
MAX_BLS_JOLTS_INDEX_BYTES = 4 * 1024 * 1024
MAX_BLS_JOLTS_RELEASE_BYTES = 16 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_JOLTS_URI_RE = re.compile(
    r"^https://www\.bls\.gov/news\.release/(?P<section>archives|history)/"
    r"jolts_(?P<date>\d{8})\.(?P<suffix>htm|txt)$"
)
_JOLTS_INDEX_URI_RE = re.compile(
    r"href=[\"'](?P<uri>(?:https://www\.bls\.gov)?/news\.release/"
    r"(?:archives|history)/jolts_(?P<date>\d{8})\."
    r"(?P<suffix>htm|txt))[\"']",
    re.IGNORECASE,
)
_JOLTS_NUMBER_RE = re.compile(
    r"^(?P<prefix>[pPrRcC])?\$?"
    r"(?P<number>[-+]?(?:\d[\d,]*(?:\.\d+)?|\.\d+))"
    r"(?P<suffix>[pPrRcC])?$"
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


def _jolts_lexical_number(value: object, name: str) -> tuple[str, float]:
    lexical = "".join(_required_text(value, name).split())
    match = _JOLTS_NUMBER_RE.fullmatch(lexical)
    if match is None:
        raise ValueError(f"{name} is not a JOLTS number")
    normalized = match.group("number").replace(",", "")
    if normalized.startswith(("-.", "+.")):
        normalized = normalized[:1] + "0" + normalized[1:]
    elif normalized.startswith("."):
        normalized = "0" + normalized
    return lexical, _finite(normalized, name)


def _release_date_from_uri(uri: str) -> str:
    match = _JOLTS_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("JOLTS artifact URI is invalid")
    try:
        return (
            datetime.strptime(match.group("date"), "%m%d%Y")
            .replace(tzinfo=timezone.utc)
            .date()
            .isoformat()
        )
    except ValueError as exc:
        raise ValueError("JOLTS artifact URI contains an invalid date") from exc


def _source_format_for_uri(uri: str) -> OfficialSourceFormat:
    match = _JOLTS_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("JOLTS artifact URI is invalid")
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


def _previous_month(reference: str) -> str:
    year, month = (int(item) for item in reference.split("-"))
    if month == 1:
        return f"{year - 1:04d}-12"
    return f"{year:04d}-{month - 1:02d}"


def _measure_definition(measure_key: str) -> tuple[str, str, str]:
    if measure_key not in BLS_JOLTS_MEASURE_KEYS:
        raise ValueError("JOLTS measure key is invalid")
    return (
        "total-nonfarm-seasonally-adjusted",
        "thousand-persons",
        "level",
    )


@dataclass(frozen=True, slots=True)
class BlsJoltsReleaseIndexEntryV1:
    """One published reference-period row from the official archive index."""

    reference_period: str
    release_date: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    entry_id: str = ""
    schema_version: str = BLS_JOLTS_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_JOLTS_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BLS JOLTS index-entry schema")
        reference = _year_month(self.reference_period, "reference_period")
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format is not _source_format_for_uri(artifact):
            raise ValueError("JOLTS selected format differs from artifact URI")
        if release != _release_date_from_uri(artifact):
            raise ValueError("JOLTS release date differs from artifact URI")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        expected = _stable_id("bls-jolts-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BLS JOLTS index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsJoltsReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_date=str(data.get("release_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsJoltsReleaseIndexV1:
    """As-of interpretation of the retained JOLTS index."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[BlsJoltsReleaseIndexEntryV1, ...]
    unavailable_reference_periods: tuple[str, ...]
    index_id: str = ""
    schema_version: str = BLS_JOLTS_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_JOLTS_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported BLS JOLTS index schema")
        if self.source_uri != BLS_JOLTS_INDEX_URI:
            raise ValueError("JOLTS index URI is invalid")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_JOLTS_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        values = tuple(self.releases)
        if (
            not values
            or len(values) > MAX_BLS_JOLTS_RELEASES
            or any(
                not isinstance(item, BlsJoltsReleaseIndexEntryV1)
                for item in values
            )
        ):
            raise TypeError("JOLTS index releases are invalid")
        references = tuple(item.reference_period for item in values)
        if references != tuple(sorted(set(references))):
            raise ValueError("JOLTS index releases must be unique and sorted")
        if references[0] != BLS_JOLTS_FIRST_REFERENCE_PERIOD:
            raise ValueError("JOLTS index omits its first monthly release")
        if any(item.release_date > as_of for item in values):
            raise ValueError("JOLTS index contains a future publication")
        release_dates = tuple(item.release_date for item in values)
        if release_dates != tuple(sorted(set(release_dates))):
            raise ValueError(
                "JOLTS publication dates are not unique and chronological"
            )
        unavailable = tuple(
            _year_month(item, "unavailable_reference_periods")
            for item in self.unavailable_reference_periods
        )
        if unavailable != tuple(sorted(set(unavailable))):
            raise ValueError(
                "unavailable_reference_periods must be unique and sorted"
            )
        if set(references) & set(unavailable):
            raise ValueError("published and unavailable JOLTS periods overlap")
        observed = tuple(sorted((*references, *unavailable)))
        if observed != _contiguous_months(
            BLS_JOLTS_FIRST_REFERENCE_PERIOD, observed[-1]
        ):
            raise ValueError("JOLTS index has an unexplained monthly gap")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", values)
        object.__setattr__(self, "unavailable_reference_periods", unavailable)
        expected = _stable_id("bls-jolts-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("BLS JOLTS index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, BlsJoltsReleaseIndexEntryV1]:
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
    def from_dict(cls, data: Mapping[str, Any]) -> BlsJoltsReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                BlsJoltsReleaseIndexEntryV1.from_dict(
                    _mapping(item, "JOLTS index entry")
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
class BlsJoltsReleaseV1:
    """Three independent labour triplets from one JOLTS."""

    job_openings: UnitedStatesReleaseTripletV1
    hires: UnitedStatesReleaseTripletV1
    total_separations: UnitedStatesReleaseTripletV1
    release_id: str = ""
    schema_version: str = BLS_JOLTS_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_JOLTS_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported BLS JOLTS release schema")
        triplets = self.triplets
        if any(
            not isinstance(item, UnitedStatesReleaseTripletV1)
            for item in triplets
        ):
            raise TypeError("JOLTS release requires U.S. release triplets")
        expected_shapes = (
            (
                BLS_JOLTS_JOB_OPENINGS,
                "thousand-persons",
                "level",
            ),
            (
                BLS_JOLTS_HIRES,
                "thousand-persons",
                "level",
            ),
            (
                BLS_JOLTS_TOTAL_SEPARATIONS,
                "thousand-persons",
                "level",
            ),
        )
        first = triplets[0]
        for triplet, (measure, unit, transformation) in zip(
            triplets, expected_shapes
        ):
            if (
                triplet.program_key != BLS_JOLTS_PROGRAM_KEY
                or triplet.event_family is not EconomicEventFamily.LABOUR_MARKET
                or f".{measure}." not in triplet.logical_event_key
                or triplet.unit != unit
                or triplet.transformation != transformation
                or triplet.seasonality != "seasonally-adjusted"
            ):
                raise ValueError("JOLTS release measure identity differs")
            common = (
                triplet.reference_period,
                triplet.previous_reference_period,
                triplet.scheduled_for_ns,
                triplet.released_at_ns,
                triplet.snapshot_id,
                triplet.content_sha256,
                triplet.source_locator,
            )
            expected_common = (
                first.reference_period,
                first.previous_reference_period,
                first.scheduled_for_ns,
                first.released_at_ns,
                first.snapshot_id,
                first.content_sha256,
                first.source_locator,
            )
            if common != expected_common:
                raise ValueError("JOLTS package evidence is inconsistent")
        expected = _stable_id("bls-jolts-release", self.identity_payload())
        if self.release_id and self.release_id != expected:
            raise ValueError("BLS JOLTS release identity differs")
        object.__setattr__(self, "release_id", expected)

    @property
    def triplets(self) -> tuple[UnitedStatesReleaseTripletV1, ...]:
        return (
            self.job_openings,
            self.hires,
            self.total_separations,
        )

    @property
    def reference_period(self) -> str:
        return str(self.job_openings.reference_period)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "job_openings": self.job_openings.to_dict(),
            "hires": self.hires.to_dict(),
            "total_separations": self.total_separations.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsJoltsReleaseV1:
        return cls(
            job_openings=UnitedStatesReleaseTripletV1.from_dict(
                _mapping(data.get("job_openings"), "job_openings")
            ),
            hires=UnitedStatesReleaseTripletV1.from_dict(
                _mapping(
                    data.get("hires"),
                    "hires",
                )
            ),
            total_separations=UnitedStatesReleaseTripletV1.from_dict(
                _mapping(
                    data.get("total_separations"),
                    "total_separations",
                )
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsJoltsMeasureV1:
    """Compact normalized evidence for one measure in one publication."""

    measure_key: str
    actual_value: float
    actual_lexical: str
    previous_as_known_value: float
    previous_as_known_lexical: str
    revised_previous_value: float
    revised_previous_lexical: str
    series_lineage: str
    unit: str
    transformation: str
    revision_comparable: bool
    normalized_sha256: str
    measure_id: str = ""
    schema_version: str = BLS_JOLTS_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_JOLTS_MEASURE_SCHEMA_VERSION:
            raise ValueError("unsupported BLS JOLTS measure schema")
        measure = _required_text(self.measure_key, "measure_key")
        if measure not in BLS_JOLTS_MEASURE_KEYS:
            raise ValueError("JOLTS measure key is invalid")
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
            lexical, numeric = _jolts_lexical_number(
                getattr(self, lexical_name), lexical_name
            )
            if not math.isclose(
                numeric,
                getattr(self, numeric_name),
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise ValueError(f"{lexical_name} differs from {numeric_name}")
            object.__setattr__(self, lexical_name, lexical)
        for name in ("series_lineage", "unit", "transformation"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if not isinstance(self.revision_comparable, bool):
            raise TypeError("revision_comparable must be boolean")
        object.__setattr__(
            self,
            "normalized_sha256",
            _sha256(self.normalized_sha256, "normalized_sha256"),
        )
        object.__setattr__(self, "measure_key", measure)
        expected = _stable_id("bls-jolts-measure", self.identity_payload())
        if self.measure_id and self.measure_id != expected:
            raise ValueError("BLS JOLTS measure identity differs")
        object.__setattr__(self, "measure_id", expected)

    @property
    def previous_was_revised(self) -> bool:
        return self.revision_comparable and not math.isclose(
            self.previous_as_known_value,
            self.revised_previous_value,
            rel_tol=0.0,
            abs_tol=1e-12,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "measure_key": self.measure_key,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "revised_previous_value": self.revised_previous_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "series_lineage": self.series_lineage,
            "unit": self.unit,
            "transformation": self.transformation,
            "revision_comparable": self.revision_comparable,
            "normalized_sha256": self.normalized_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "measure_id": self.measure_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsJoltsMeasureV1:
        return cls(
            measure_key=str(data.get("measure_key", "")),
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
            unit=str(data.get("unit", "")),
            transformation=str(data.get("transformation", "")),
            revision_comparable=cast(bool, data.get("revision_comparable")),
            normalized_sha256=str(data.get("normalized_sha256", "")),
            measure_id=str(data.get("measure_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsJoltsArchiveEntryV1:
    """One current/predecessor-backed JOLTS publication."""

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
    released_lexical: str
    reported_zone: str
    source_era: str
    annual_benchmark_release: bool
    measures: tuple[BlsJoltsMeasureV1, ...]
    entry_id: str = ""
    schema_version: str = BLS_JOLTS_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_JOLTS_ARCHIVE_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BLS JOLTS archive-entry schema")
        reference = _year_month(self.reference_period, "reference_period")
        previous_reference = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_reference >= reference:
            raise ValueError(
                "JOLTS previous reference period must precede current"
            )
        if reference != "2025-10" and previous_reference != _previous_month(
            reference
        ):
            raise ValueError("JOLTS predecessor is not the prior month")
        if reference == "2025-10" and previous_reference != "2025-08":
            raise ValueError("JOLTS shutdown predecessor is invalid")
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        previous_artifact = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format is not _source_format_for_uri(artifact):
            raise ValueError("JOLTS entry format differs from artifact URI")
        if release != _release_date_from_uri(artifact):
            raise ValueError(
                "JOLTS entry release date differs from artifact URI"
            )
        for name in ("content_sha256", "previous_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_JOLTS_RELEASE_BYTES,
        )
        _positive_int(
            self.previous_content_length,
            "previous_content_length",
            MAX_BLS_JOLTS_RELEASE_BYTES,
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T10:00:00":
            raise ValueError("JOLTS released time differs from release date")
        zone = _required_text(self.reported_zone, "reported_zone")
        if zone not in {"EST", "EDT", "ET"}:
            raise ValueError("JOLTS reported zone is invalid")
        era = _required_text(self.source_era, "source_era")
        expected_eras = (
            {"fixed-width-text"}
            if source_format is OfficialSourceFormat.TEXT
            else {"preformatted-html", "semantic-html"}
        )
        if era not in expected_eras:
            raise ValueError("JOLTS source era differs from reference period")
        if not isinstance(self.annual_benchmark_release, bool):
            raise TypeError("annual_benchmark_release must be boolean")
        if self.annual_benchmark_release is not reference.endswith("-01"):
            raise ValueError("JOLTS annual benchmark status differs")
        measures = tuple(self.measures)
        if (
            len(measures) != len(BLS_JOLTS_MEASURE_KEYS)
            or any(not isinstance(item, BlsJoltsMeasureV1) for item in measures)
            or tuple(item.measure_key for item in measures)
            != BLS_JOLTS_MEASURE_KEYS
        ):
            raise ValueError("JOLTS entry measure package is invalid")
        for measure in measures:
            lineage, unit, transformation = _measure_definition(
                measure.measure_key
            )
            comparable = reference != "2025-10"
            if (
                measure.series_lineage != lineage
                or measure.unit != unit
                or measure.transformation != transformation
                or measure.revision_comparable is not comparable
            ):
                raise ValueError("JOLTS measure definition differs")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(
            self, "previous_reference_period", previous_reference
        )
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "previous_artifact_uri", previous_artifact)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "source_era", era)
        object.__setattr__(self, "measures", measures)
        expected = _stable_id(
            "bls-jolts-archive-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BLS JOLTS archive-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    @property
    def by_measure(self) -> Mapping[str, BlsJoltsMeasureV1]:
        return MappingProxyType(
            {item.measure_key: item for item in self.measures}
        )

    @property
    def previous_was_revised(self) -> bool:
        return any(item.previous_was_revised for item in self.measures)

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
            "released_lexical": self.released_lexical,
            "reported_zone": self.reported_zone,
            "source_era": self.source_era,
            "annual_benchmark_release": self.annual_benchmark_release,
            "measures": [item.to_dict() for item in self.measures],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsJoltsArchiveEntryV1:
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
            released_lexical=str(data.get("released_lexical", "")),
            reported_zone=str(data.get("reported_zone", "")),
            source_era=str(data.get("source_era", "")),
            annual_benchmark_release=cast(
                bool, data.get("annual_benchmark_release")
            ),
            measures=tuple(
                BlsJoltsMeasureV1.from_dict(_mapping(item, "JOLTS measure"))
                for item in _sequence(data.get("measures"), "measures")
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsJoltsArchiveManifestV1:
    """Compact closure evidence for the complete as-of publication archive."""

    registry_id: str
    profile_id: str
    release_index: BlsJoltsReleaseIndexV1
    entries: tuple[BlsJoltsArchiveEntryV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    revision_occurrence_count: int
    job_openings_revision_count: int
    hires_revision_count: int
    total_separations_revision_count: int
    noncomparable_revision_count: int
    annual_benchmark_release_count: int
    manifest_id: str = ""
    schema_version: str = BLS_JOLTS_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_JOLTS_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported BLS JOLTS archive-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("JOLTS manifest registry identity is invalid")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("JOLTS manifest profile identity is invalid")
        if not isinstance(self.release_index, BlsJoltsReleaseIndexV1):
            raise TypeError("JOLTS manifest requires a v1 release index")
        entries = tuple(self.entries)
        if (
            not entries
            or len(entries) > MAX_BLS_JOLTS_RELEASES
            or any(
                not isinstance(item, BlsJoltsArchiveEntryV1) for item in entries
            )
        ):
            raise TypeError("JOLTS manifest entries are invalid")
        references = tuple(item.reference_period for item in entries)
        indexed_entries = self.release_index.releases[1:]
        if references != tuple(
            item.reference_period for item in indexed_entries
        ):
            raise ValueError("JOLTS manifest differs from its release index")
        releases = self.release_index.by_reference_period
        for item in entries:
            indexed = releases[item.reference_period]
            if (
                item.release_date != indexed.release_date
                or item.artifact_uri != indexed.artifact_uri
                or item.source_format is not indexed.source_format
            ):
                raise ValueError("JOLTS manifest entry differs from the index")
        for previous, current in pairwise(entries):
            if (
                current.previous_reference_period != previous.reference_period
                or current.previous_artifact_uri != previous.artifact_uri
                or current.previous_content_sha256 != previous.content_sha256
                or current.previous_content_length != previous.content_length
            ):
                raise ValueError("JOLTS predecessor artifact chain is broken")
        first = entries[0]
        predecessor = self.release_index.releases[0]
        if (
            first.previous_reference_period != predecessor.reference_period
            or first.previous_artifact_uri != predecessor.artifact_uri
        ):
            raise ValueError(
                "JOLTS manifest omits its February 2004 predecessor"
            )
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
        occurrence_revisions = sum(
            item.previous_was_revised for item in entries
        )
        measure_counts = {
            measure: sum(
                item.by_measure[measure].previous_was_revised
                for item in entries
            )
            for measure in BLS_JOLTS_MEASURE_KEYS
        }
        noncomparable = sum(
            not measure.revision_comparable
            for item in entries
            for measure in item.measures
        )
        expected_counts = {
            "raw_artifact_count": raw_count,
            "total_content_bytes": total,
            "revision_occurrence_count": occurrence_revisions,
            "job_openings_revision_count": measure_counts[
                BLS_JOLTS_JOB_OPENINGS
            ],
            "hires_revision_count": measure_counts[BLS_JOLTS_HIRES],
            "total_separations_revision_count": measure_counts[
                BLS_JOLTS_TOTAL_SEPARATIONS
            ],
            "noncomparable_revision_count": noncomparable,
            "annual_benchmark_release_count": sum(
                item.annual_benchmark_release for item in entries
            ),
        }
        for name, expected_count in expected_counts.items():
            if getattr(self, name) != expected_count:
                raise ValueError(f"JOLTS manifest {name} differs")
        if len({item.content_sha256 for item in entries}) != len(entries):
            raise ValueError("JOLTS manifest repeats current artifact content")
        if len({item[1] for item in artifact_evidence}) != raw_count:
            raise ValueError("JOLTS manifest repeats a raw artifact hash")
        normalized = {
            measure.normalized_sha256
            for item in entries
            for measure in item.measures
        }
        if len(normalized) != len(entries) * len(BLS_JOLTS_MEASURE_KEYS):
            raise ValueError("JOLTS manifest repeats normalized content")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "bls-jolts-archive-manifest",
            self.identity_payload(),
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("BLS JOLTS archive identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, BlsJoltsArchiveEntryV1]:
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
            "job_openings_revision_count": self.job_openings_revision_count,
            "hires_revision_count": (self.hires_revision_count),
            "total_separations_revision_count": self.total_separations_revision_count,
            "noncomparable_revision_count": (self.noncomparable_revision_count),
            "annual_benchmark_release_count": self.annual_benchmark_release_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BlsJoltsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=BlsJoltsReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            entries=tuple(
                BlsJoltsArchiveEntryV1.from_dict(
                    _mapping(item, "JOLTS archive entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            job_openings_revision_count=cast(
                int, data.get("job_openings_revision_count")
            ),
            hires_revision_count=cast(int, data.get("hires_revision_count")),
            total_separations_revision_count=cast(
                int, data.get("total_separations_revision_count")
            ),
            noncomparable_revision_count=cast(
                int, data.get("noncomparable_revision_count")
            ),
            annual_benchmark_release_count=cast(
                int, data.get("annual_benchmark_release_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> BlsJoltsArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("JOLTS archive manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "archive manifest"))


def build_bls_jolts_index_request(
    registry: OfficialSourceRegistryV1,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    """Plan the exact official JOLTS index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("JOLTS index request requires a v1 source registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(BLS_JOLTS_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=BLS_JOLTS_INDEX_URI,
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


def parse_bls_jolts_release_index(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> BlsJoltsReleaseIndexV1:
    """Parse published rows and explicit gaps from the BLS archive page."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("JOLTS index parser requires a v1 snapshot")
    if (
        snapshot.request.source_key != BLS_JOLTS_SOURCE_KEY
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.uri != BLS_JOLTS_INDEX_URI
    ):
        raise ValueError("JOLTS index parser received a different source")
    as_of = _iso_date(as_of_date, "as_of_date")
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("BLS JOLTS release index is not UTF-8") from exc
    text = re.sub(r"(?is)<!--.*?-->", "", text)
    releases: list[BlsJoltsReleaseIndexEntryV1] = []
    unavailable: list[str] = []
    seen: set[str] = set()
    for list_item in re.finditer(r"(?is)<li\b[^>]*>(.*?)</li>", text):
        markup = list_item.group(1)
        label = _visible_html(markup)
        uri_matches = tuple(_JOLTS_INDEX_URI_RE.finditer(markup))
        if not uri_matches and "not published" not in label.lower():
            continue
        label_match = re.search(
            r"^(?P<month>January|February|March|April|May|June|July|"
            r"August|September|October|November|December)\s+"
            r"(?P<year>\d{4})\b",
            label,
            re.IGNORECASE,
        )
        if label_match is None:
            continue
        reference = (
            f"{int(label_match.group('year')):04d}-"
            f"{_MONTH_NUMBERS[label_match.group('month').lower()]:02d}"
        )
        if reference < BLS_JOLTS_FIRST_REFERENCE_PERIOD:
            continue
        if not uri_matches:
            if reference <= as_of[:7]:
                unavailable.append(reference)
                seen.add(reference)
            continue
        preferred = next(
            (
                item
                for item in uri_matches
                if item.group("suffix").lower() == "htm"
            ),
            uri_matches[0],
        )
        artifact_uri = urljoin("https://www.bls.gov", preferred.group("uri"))
        if artifact_uri == BLS_JOLTS_INDEX_TYPO_RELEASE_URI:
            reference = BLS_JOLTS_INDEX_TYPO_REFERENCE_PERIOD
        if reference in seen:
            raise ValueError(
                f"BLS JOLTS index repeats reference period {reference}"
            )
        release_date = _release_date_from_uri(artifact_uri)
        if release_date > as_of:
            continue
        seen.add(reference)
        releases.append(
            BlsJoltsReleaseIndexEntryV1(
                reference_period=reference,
                release_date=release_date,
                artifact_uri=artifact_uri,
                source_format=_source_format_for_uri(artifact_uri),
            )
        )
    return BlsJoltsReleaseIndexV1(
        source_uri=BLS_JOLTS_INDEX_URI,
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
    source = registry.source(BLS_JOLTS_SOURCE_KEY)
    source_format = _source_format_for_uri(uri)
    if source_format not in source.formats:
        raise ValueError("JOLTS source does not declare the artifact format")
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


def build_bls_jolts_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: BlsJoltsReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build every unique published artifact request."""
    if not isinstance(release_index, BlsJoltsReleaseIndexV1):
        raise TypeError("JOLTS requests require a v1 release index")
    uris = tuple(item.artifact_uri for item in release_index.releases)
    return tuple(_request_for_uri(registry, uri) for uri in uris)


def _decode_release_text(content: bytes) -> tuple[str, str]:
    try:
        return content.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        try:
            return content.decode("windows-1252"), "windows-1252"
        except UnicodeDecodeError as exc:
            raise ValueError(
                "JOLTS release is neither UTF-8 nor Windows-1252"
            ) from exc


def _strip_html(value: str) -> str:
    result = re.sub(r"(?i)<br\s*/?>", "\n", value)
    result = re.sub(r"(?i)</(?:p|div|tr|li|h\d)>", "\n", result)
    return html.unescape(re.sub(r"<[^>]+>", " ", result)).replace("\xa0", " ")


def _main_html_release(value: str) -> str:
    candidates: list[str] = []
    for match in re.finditer(r"(?is)<pre\b[^>]*>(.*?)</pre>", value):
        candidate = _strip_html(match.group(1))
        if (
            "10:00" in candidate
            and "JOB OPENINGS AND LABOR TURNOVER" in candidate.upper()
        ):
            candidates.append(candidate)
    if len(candidates) != 1:
        raise ValueError("JOLTS HTML release body is missing or changed")
    return candidates[0]


def _month_from_header(value: str) -> str | None:
    visible = _visible_html(value)
    match = re.fullmatch(
        r"(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"[a-z]*\.?\s+(?P<year>20\d{2})(?:\s*\([^)]*\))?",
        visible,
        re.IGNORECASE,
    )
    if match is None:
        return None
    month = next(
        number
        for name, number in _MONTH_NUMBERS.items()
        if name[:3] == match.group("month")[:3].lower()
    )
    return f"{int(match.group('year')):04d}-{month:02d}"


@dataclass(frozen=True, slots=True)
class _TableAValues:
    periods: tuple[str, ...]
    values: tuple[str, ...]
    locator: str


def _semantic_table_a(value: str) -> _TableAValues | None:
    table_id = "jolts_tablea"
    table = re.search(
        rf"(?is)<table\b[^>]*\bid=[\"']{table_id}[\"'][^>]*>"
        r"(?P<body>.*?)</table>",
        value,
    )
    if table is None:
        return None
    head = re.search(r"(?is)<thead\b[^>]*>(.*?)</thead>", table.group("body"))
    if head is None:
        raise ValueError(f"JOLTS table {table_id} has no header")
    periods = tuple(
        period
        for cell in re.findall(r"(?is)<th\b[^>]*>(.*?)</th>", head.group(1))
        if (period := _month_from_header(cell)) is not None
    )
    if (
        len(periods) != 9
        or periods[:3] != periods[3:6]
        or periods[:3] != periods[6:9]
        or len(set(periods[:3])) != 3
    ):
        raise ValueError("JOLTS Table A period header changed")
    candidates: list[tuple[str, tuple[str, ...]]] = []
    for row in re.finditer(
        r"(?is)<tr\b[^>]*>(?P<body>.*?)</tr>", table.group("body")
    ):
        heading = re.search(
            r"(?is)<th\b(?P<attrs>[^>]*)>(?P<body>.*?)</th>",
            row.group("body"),
        )
        if heading is None:
            continue
        label = _visible_html(heading.group("body")).lower()
        if label not in {"total", "total nonfarm"}:
            continue
        identifier = re.search(
            r"\bid=[\"'](?P<id>[^\"']+)",
            heading.group("attrs"),
            re.IGNORECASE,
        )
        if identifier is None:
            raise ValueError("JOLTS Table A total row has no identifier")
        values = tuple(
            _visible_html(cell)
            for cell in re.findall(
                r"(?is)<td\b[^>]*>(.*?)</td>", row.group("body")
            )
        )
        candidates.append((identifier.group("id"), values))
    if len(candidates) != 2:
        raise ValueError("JOLTS Table A total rows changed")
    row_id, values = candidates[0]
    if len(values) != len(periods):
        raise ValueError("JOLTS Table A total level row changed")
    return _TableAValues(
        periods=periods,
        values=values,
        locator=f"table#{table_id}:row#{row_id}:levels",
    )


@dataclass(frozen=True, slots=True)
class _LegacyRow:
    values: tuple[str, ...]
    line_number: int
    locator: str


def _legacy_summary_row(value: str) -> _LegacyRow:
    lines = value.splitlines()
    for index, line in enumerate(lines):
        compact = " ".join(line.split())
        if "|" not in compact:
            continue
        heading = compact.split("|", 1)[0].rstrip(". ")
        if (
            re.fullmatch(
                r"Total(?:\s+nonfarm)?(?:\s*(?:\([A-Za-z0-9,]+\)|\d+/))?",
                heading,
                re.IGNORECASE,
            )
            is None
        ):
            continue
        values = tuple(item.strip() for item in line.split("|")[1:])
        if len(values) != 9:
            continue
        return _LegacyRow(
            values=values,
            line_number=index + 1,
            locator=f"line:{index + 1}:summary-table-a:total-levels",
        )
    raise ValueError("JOLTS fixed-width Table A total level row is missing")


@dataclass(frozen=True, slots=True)
class _MeasureObservation:
    measure_key: str
    actual_value: float
    actual_lexical: str
    revised_previous_value: float | None
    revised_previous_lexical: str | None
    actual_locator: str
    revision_locator: str


@dataclass(frozen=True, slots=True)
class _JoltsObservation:
    reference_period: str
    release_date: str
    released_lexical: str
    reported_zone: str
    source_era: str
    source_encoding: str
    annual_benchmark_release: bool
    release_time_locator: str
    measures: tuple[_MeasureObservation, ...]

    @property
    def by_measure(self) -> Mapping[str, _MeasureObservation]:
        return MappingProxyType(
            {item.measure_key: item for item in self.measures}
        )


def _measure_observations(
    values: tuple[str, ...],
    *,
    locator: str,
    include_revision: bool,
) -> tuple[_MeasureObservation, ...]:
    if len(values) != 9:
        raise ValueError("JOLTS Table A level row has changed shape")
    observations: list[_MeasureObservation] = []
    for offset, measure_key in zip((0, 3, 6), BLS_JOLTS_MEASURE_KEYS):
        actual_lexical, actual = _jolts_lexical_number(
            values[offset + 2], f"{measure_key} actual"
        )
        revised_lexical: str | None = None
        revised: float | None = None
        if include_revision:
            revised_lexical, revised = _jolts_lexical_number(
                values[offset + 1], f"{measure_key} revised previous"
            )
        observations.append(
            _MeasureObservation(
                measure_key=measure_key,
                actual_value=actual,
                actual_lexical=actual_lexical,
                revised_previous_value=revised,
                revised_previous_lexical=revised_lexical,
                actual_locator=f"{locator}:column:{offset + 3}:current",
                revision_locator=(
                    f"{locator}:column:{offset + 2}:previous-final"
                    if include_revision
                    else f"{locator}:no-comparison-requested"
                ),
            )
        )
    return tuple(observations)


def _semantic_measures(
    raw_html: str,
    *,
    reference_period: str,
    comparison_reference_period: str | None,
) -> tuple[_MeasureObservation, ...]:
    result = _semantic_table_a(raw_html)
    if result is None:
        raise ValueError("JOLTS semantic Table A is absent")
    if result.periods[2] != reference_period:
        raise ValueError("JOLTS Table A omits the current reference period")
    if (
        comparison_reference_period is not None
        and result.periods[1] != comparison_reference_period
    ):
        raise ValueError("JOLTS Table A omits the comparison period")
    return _measure_observations(
        result.values,
        locator=result.locator,
        include_revision=comparison_reference_period is not None,
    )


def _legacy_measures(
    main_text: str,
    *,
    reference_period: str,
    comparison_reference_period: str | None,
) -> tuple[_MeasureObservation, ...]:
    del reference_period
    row = _legacy_summary_row(main_text)
    return _measure_observations(
        row.values,
        locator=row.locator,
        include_revision=comparison_reference_period is not None,
    )


def _parse_bls_jolts_observation(
    snapshot: OfficialRawSnapshotV1,
    reference_period: str,
    *,
    comparison_reference_period: str | None = None,
) -> _JoltsObservation:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("JOLTS parser requires a v1 official snapshot")
    if snapshot.request.source_key != BLS_JOLTS_SOURCE_KEY:
        raise ValueError("JOLTS parser received a different official source")
    reference = _year_month(reference_period, "reference_period")
    source_format = snapshot.request.source_format
    if source_format is not _source_format_for_uri(snapshot.request.uri):
        raise ValueError("JOLTS request format differs from artifact URI")
    if source_format not in {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }:
        raise ValueError("JOLTS parser received an unsupported format")
    text, source_encoding = _decode_release_text(snapshot.content)
    raw_html = text if source_format is OfficialSourceFormat.HTML else None
    main_text = (
        _main_html_release(text)
        if source_format is OfficialSourceFormat.HTML
        else text
    )
    release_date = _release_date_from_uri(snapshot.request.uri)
    release = date.fromisoformat(release_date)
    header = " ".join(main_text[:9000].split())
    if (
        re.search(
            rf"{release.strftime('%B')}\s+{release.day},\s*{release.year}",
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("JOLTS release date is absent from the header")
    time_match = re.search(
        r"10:00\s*(?:A\.?M\.?)\s*\(?(?P<zone>E\.?S\.?T\.?|"
        r"E\.?D\.?T\.?|ET)\)?",
        main_text,
        re.IGNORECASE,
    )
    if time_match is None:
        raise ValueError("JOLTS 10:00 release-time header is missing")
    reported_zone = time_match.group("zone").upper().replace(".", "")
    released_lexical = f"{release_date}T10:00:00"
    timestamp = normalize_official_source_timestamp(
        released_lexical,
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
        raise ValueError("JOLTS timezone conflicts with release date")
    year, month = (int(item) for item in reference.split("-"))
    if (
        re.search(
            rf"(?:THE\s+)?JOB OPENINGS AND LABOR TURNOVER\s*(?::|--?|–|—)+\s*"
            rf"{calendar.month_name[month]}\s+{year}",
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("JOLTS reference period is absent from the title")
    comparison = (
        None
        if comparison_reference_period is None
        else _year_month(
            comparison_reference_period, "comparison_reference_period"
        )
    )
    if comparison is not None and comparison >= reference:
        raise ValueError("JOLTS comparison period must precede current")
    semantic = (
        raw_html is not None
        and re.search(
            r"<table\b[^>]*\bid=[\"']jolts_tablea[\"']",
            raw_html,
            re.IGNORECASE,
        )
        is not None
    )
    if semantic:
        measures = _semantic_measures(
            cast(str, raw_html),
            reference_period=reference,
            comparison_reference_period=comparison,
        )
        source_era = "semantic-html"
    else:
        measures = _legacy_measures(
            main_text,
            reference_period=reference,
            comparison_reference_period=comparison,
        )
        source_era = (
            "fixed-width-text"
            if source_format is OfficialSourceFormat.TEXT
            else "preformatted-html"
        )
    release_line = main_text[: time_match.start()].count("\n") + 1
    annual_benchmark_release = (
        reference.endswith("-01")
        and re.search(r"\bannual\b", main_text, re.IGNORECASE) is not None
        and re.search(r"\brevis(?:ed|ion|ions)\b", main_text, re.IGNORECASE)
        is not None
    )
    return _JoltsObservation(
        reference_period=reference,
        release_date=release_date,
        released_lexical=released_lexical,
        reported_zone=reported_zone,
        source_era=source_era,
        source_encoding=source_encoding,
        annual_benchmark_release=annual_benchmark_release,
        release_time_locator=f"line:{release_line}:release-header",
        measures=measures,
    )


def _measure_limitations(
    measure_key: str,
    *,
    current: _JoltsObservation,
    previous_snapshot: OfficialRawSnapshotV1,
) -> tuple[str, ...]:
    specific = {
        BLS_JOLTS_JOB_OPENINGS: (
            "This measure is the total-nonfarm seasonally adjusted job-openings level in thousands on the last business day of the month.",
        ),
        BLS_JOLTS_HIRES: (
            "This measure is the total-nonfarm seasonally adjusted hires level in thousands over the month.",
        ),
        BLS_JOLTS_TOTAL_SEPARATIONS: (
            "This measure is the total-nonfarm seasonally adjusted total-separations level in thousands over the month.",
        ),
    }[measure_key]
    limitations: tuple[str, ...] = specific + (
        f"The source header reports {current.reported_zone}; timezone normalization uses America/New_York.",
        f"The retained source era is {current.source_era} and text decoding is {current.source_encoding}.",
        "No official event-level consensus is published; forecasts remain unavailable.",
        (
            "Previous-as-known evidence: "
            f"{previous_snapshot.request.uri} "
            f"sha256:{previous_snapshot.content_sha256}."
        ),
    )
    if current.reference_period == "2025-10":
        limitations += (
            "September 2025 was not published; previous-as-known remains the August 2025 publication while the October tables skip the unavailable month.",
        )
    return limitations


def parse_bls_jolts_release(
    snapshot: OfficialRawSnapshotV1,
    *,
    reference_period: str,
    previous_snapshot: OfficialRawSnapshotV1,
    previous_reference_period: str,
) -> BlsJoltsReleaseV1:
    """Parse the three headlines with their occurrence-specific predecessor."""
    comparable = reference_period != "2025-10"
    current = _parse_bls_jolts_observation(
        snapshot,
        reference_period,
        comparison_reference_period=(
            previous_reference_period if comparable else None
        ),
    )
    previous = _parse_bls_jolts_observation(
        previous_snapshot, previous_reference_period
    )
    if previous.reference_period >= current.reference_period:
        raise ValueError("JOLTS predecessor does not precede current")
    timestamp = normalize_official_source_timestamp(
        current.released_lexical,
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    triplets: list[UnitedStatesReleaseTripletV1] = []
    for measure_key in BLS_JOLTS_MEASURE_KEYS:
        observed = current.by_measure[measure_key]
        prior = previous.by_measure[measure_key]
        lineage, unit, transformation = _measure_definition(measure_key)
        if comparable and (
            observed.revised_previous_value is None
            or observed.revised_previous_lexical is None
        ):
            raise ValueError("JOLTS comparable revision is unavailable")
        revised_value = (
            cast(float, observed.revised_previous_value)
            if comparable
            else prior.actual_value
        )
        revised_lexical = (
            cast(str, observed.revised_previous_lexical)
            if comparable
            else prior.actual_lexical
        )
        triplets.append(
            UnitedStatesReleaseTripletV1(
                program_key=BLS_JOLTS_PROGRAM_KEY,
                logical_event_key=(
                    f"us.bls.jolts.{measure_key}.{lineage}.{current.reference_period}"
                ),
                event_family=EconomicEventFamily.LABOUR_MARKET,
                reference_period=current.reference_period,
                previous_reference_period=previous.reference_period,
                scheduled_lexical=current.released_lexical,
                released_lexical=current.released_lexical,
                source_timezone="America/New_York",
                scheduled_for_ns=timestamp.utc_ns,
                released_at_ns=timestamp.utc_ns,
                actual_value=observed.actual_value,
                actual_lexical=observed.actual_lexical,
                previous_as_known_value=prior.actual_value,
                previous_as_known_lexical=prior.actual_lexical,
                revised_previous_value=revised_value,
                revised_previous_lexical=revised_lexical,
                unit=unit,
                transformation=transformation,
                seasonality="seasonally-adjusted",
                snapshot_id=snapshot.snapshot_id,
                content_sha256=snapshot.content_sha256,
                source_locator=snapshot.resolved_uri,
                release_time_locator=current.release_time_locator,
                actual_locator=observed.actual_locator,
                previous_locator=(
                    f"{previous_snapshot.request.uri}::{prior.actual_locator}"
                ),
                revision_locator=observed.revision_locator,
                limitations=_measure_limitations(
                    measure_key,
                    current=current,
                    previous_snapshot=previous_snapshot,
                ),
            )
        )
    return BlsJoltsReleaseV1(
        job_openings=triplets[0],
        hires=triplets[1],
        total_separations=triplets[2],
    )


def _snapshots_by_uri(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> Mapping[str, OfficialRawSnapshotV1]:
    result: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, OfficialRawSnapshotV1):
            raise TypeError("JOLTS archive requires v1 official snapshots")
        request = snapshot.request
        release_date = _release_date_from_uri(request.uri)
        if (
            request.source_key != BLS_JOLTS_SOURCE_KEY
            or request.source_format is not _source_format_for_uri(request.uri)
            or request.window_start != release_date
            or request.window_end != release_date
        ):
            raise ValueError("JOLTS snapshot has a different scope")
        if request.uri in result:
            raise ValueError("JOLTS archive repeats an artifact snapshot")
        result[request.uri] = snapshot
    return MappingProxyType(result)


def _entry_from_release(
    indexed: BlsJoltsReleaseIndexEntryV1,
    current: OfficialRawSnapshotV1,
    previous: OfficialRawSnapshotV1,
    release: BlsJoltsReleaseV1,
) -> BlsJoltsArchiveEntryV1:
    comparable = indexed.reference_period != "2025-10"
    observation = _parse_bls_jolts_observation(
        current,
        indexed.reference_period,
        comparison_reference_period=(
            release.job_openings.previous_reference_period
            if comparable
            else None
        ),
    )
    measures: list[BlsJoltsMeasureV1] = []
    for measure_key, triplet in zip(BLS_JOLTS_MEASURE_KEYS, release.triplets):
        lineage, unit, transformation = _measure_definition(measure_key)
        measures.append(
            BlsJoltsMeasureV1(
                measure_key=measure_key,
                actual_value=triplet.actual_value,
                actual_lexical=triplet.actual_lexical,
                previous_as_known_value=triplet.previous_as_known_value,
                previous_as_known_lexical=(triplet.previous_as_known_lexical),
                revised_previous_value=triplet.revised_previous_value,
                revised_previous_lexical=triplet.revised_previous_lexical,
                series_lineage=lineage,
                unit=unit,
                transformation=transformation,
                revision_comparable=comparable,
                normalized_sha256=triplet.normalized_sha256,
            )
        )
    return BlsJoltsArchiveEntryV1(
        reference_period=indexed.reference_period,
        previous_reference_period=(
            release.job_openings.previous_reference_period
        ),
        release_date=indexed.release_date,
        artifact_uri=current.request.uri,
        source_format=current.request.source_format,
        content_sha256=current.content_sha256,
        content_length=len(current.content),
        previous_artifact_uri=previous.request.uri,
        previous_content_sha256=previous.content_sha256,
        previous_content_length=len(previous.content),
        released_lexical=release.job_openings.released_lexical,
        reported_zone=observation.reported_zone,
        source_era=observation.source_era,
        annual_benchmark_release=observation.annual_benchmark_release,
        measures=tuple(measures),
    )


def build_bls_jolts_archive_manifest(
    profile: UnitedStatesBackfillProfileV1,
    release_index: BlsJoltsReleaseIndexV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> BlsJoltsArchiveManifestV1:
    """Replay every indexed publication into compact closure evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("JOLTS manifest requires a v1 U.S. profile")
    program = profile.by_key.get(BLS_JOLTS_PROGRAM_KEY)
    if program is None or program.source_key != BLS_JOLTS_SOURCE_KEY:
        raise ValueError("U.S. profile omits the dedicated JOLTS source")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {item.artifact_uri for item in release_index.releases}
    if set(by_uri) != expected_uris:
        raise ValueError("JOLTS snapshots differ from the release index")
    entries: list[BlsJoltsArchiveEntryV1] = []
    predecessor = release_index.releases[0]
    previous_snapshot = by_uri[predecessor.artifact_uri]
    previous_reference = predecessor.reference_period
    for indexed in release_index.releases[1:]:
        current = by_uri[indexed.artifact_uri]
        try:
            release = parse_bls_jolts_release(
                current,
                reference_period=indexed.reference_period,
                previous_snapshot=previous_snapshot,
                previous_reference_period=previous_reference,
            )
        except ValueError as exc:
            raise ValueError(
                "JOLTS archive parse failed for "
                f"{indexed.reference_period} ({indexed.artifact_uri}): {exc}"
            ) from exc
        entries.append(
            _entry_from_release(indexed, current, previous_snapshot, release)
        )
        previous_snapshot = current
        previous_reference = indexed.reference_period
    artifact_evidence = {
        (item.request.uri, item.content_sha256): len(item.content)
        for item in by_uri.values()
    }
    values = tuple(entries)
    measure_counts = {
        measure: sum(
            item.by_measure[measure].previous_was_revised for item in values
        )
        for measure in BLS_JOLTS_MEASURE_KEYS
    }
    return BlsJoltsArchiveManifestV1(
        registry_id=profile.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        entries=values,
        raw_artifact_count=len(artifact_evidence),
        total_content_bytes=sum(artifact_evidence.values()),
        revision_occurrence_count=sum(
            item.previous_was_revised for item in values
        ),
        job_openings_revision_count=measure_counts[BLS_JOLTS_JOB_OPENINGS],
        hires_revision_count=measure_counts[BLS_JOLTS_HIRES],
        total_separations_revision_count=measure_counts[
            BLS_JOLTS_TOTAL_SEPARATIONS
        ],
        noncomparable_revision_count=sum(
            not measure.revision_comparable
            for item in values
            for measure in item.measures
        ),
        annual_benchmark_release_count=sum(
            item.annual_benchmark_release for item in values
        ),
    )


def replay_bls_jolts_archive(
    manifest: BlsJoltsArchiveManifestV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[BlsJoltsReleaseV1, ...]:
    """Recompute and verify every normalized publication package."""
    if not isinstance(manifest, BlsJoltsArchiveManifestV1):
        raise TypeError("JOLTS replay requires a v1 archive manifest")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {
        item.artifact_uri for item in manifest.release_index.releases
    }
    if set(by_uri) != expected_uris:
        raise ValueError("JOLTS replay snapshots differ from the manifest")
    releases: list[BlsJoltsReleaseV1] = []
    for indexed, expected in zip(
        manifest.release_index.releases[1:], manifest.entries
    ):
        current = by_uri[expected.artifact_uri]
        previous = by_uri[expected.previous_artifact_uri]
        release = parse_bls_jolts_release(
            current,
            reference_period=expected.reference_period,
            previous_snapshot=previous,
            previous_reference_period=expected.previous_reference_period,
        )
        if _entry_from_release(indexed, current, previous, release) != expected:
            raise ValueError(
                f"JOLTS replay differs for {expected.reference_period}"
            )
        releases.append(release)
    return tuple(releases)


def bls_jolts_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: BlsJoltsArchiveManifestV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete coverage only from verified manifest evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("JOLTS coverage requires a v1 U.S. profile")
    if not isinstance(manifest, BlsJoltsArchiveManifestV1):
        raise TypeError("JOLTS coverage requires a v1 archive manifest")
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("JOLTS manifest differs from the U.S. profile")
    program = profile.by_key[BLS_JOLTS_PROGRAM_KEY]
    end = _iso_date(window_end_date, "window_end_date")
    if end < max(item.release_date for item in manifest.entries):
        raise ValueError("JOLTS coverage end precedes the latest release")
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
        gap_reasons=(
            UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,
            UnitedStatesCoverageGapReason.PROGRAM_NOT_YET_PUBLISHED,
        ),
        notes=(
            f"Release index: {manifest.release_index.index_id}",
            f"Archive manifest: {manifest.manifest_id}",
            f"Official unavailable reference periods: {unavailable}",
            (
                "Distinct revisions: "
                f"job openings {manifest.job_openings_revision_count}, "
                f"hires {manifest.hires_revision_count}, total separations "
                f"{manifest.total_separations_revision_count}."
            ),
            (
                "The qualified triplet archive begins with March 2004; the "
                "February 2004 release is retained as predecessor evidence."
            ),
            "JOLTS was not yet published at the January 2000 profile boundary.",
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError("JOLTS manifest does not qualify complete coverage")
    return coverage


def packaged_bls_jolts_index_path() -> Path:
    """Return the packaged base64 envelope for the retained archive index."""
    return (
        Path(__file__).with_name("assets")
        / "us_jolts_release_index_v1.html.b64"
    )


def packaged_bls_jolts_archive_manifest_path() -> Path:
    """Return the packaged compact archive-manifest path."""
    return Path(__file__).with_name("assets") / "us_jolts_archive_v1.json"


def load_packaged_bls_jolts_archive_manifest() -> BlsJoltsArchiveManifestV1:
    """Load and validate the packaged complete as-of manifest."""
    path = packaged_bls_jolts_archive_manifest_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            "packaged JOLTS archive manifest is unavailable"
        ) from exc
    return BlsJoltsArchiveManifestV1.from_json(text)


def load_packaged_bls_jolts_index() -> bytes:
    """Decode and hash-check the exact archive index used by the manifest."""
    path = packaged_bls_jolts_index_path()
    try:
        content = base64.b64decode(
            path.read_text(encoding="ascii").strip(), validate=True
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError(
            "packaged JOLTS index is unavailable or invalid"
        ) from exc
    manifest = load_packaged_bls_jolts_archive_manifest()
    if (
        len(content) != manifest.release_index.content_length
        or hashlib.sha256(content).hexdigest()
        != manifest.release_index.content_sha256
    ):
        raise ValueError("packaged JOLTS index differs from its manifest")
    return content


__all__ = [
    "BLS_JOLTS_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "BLS_JOLTS_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "BLS_JOLTS_FIRST_QUALIFIED_REFERENCE_PERIOD",
    "BLS_JOLTS_FIRST_REFERENCE_PERIOD",
    "BLS_JOLTS_HIRES",
    "BLS_JOLTS_INDEX_ENTRY_SCHEMA_VERSION",
    "BLS_JOLTS_INDEX_SCHEMA_VERSION",
    "BLS_JOLTS_INDEX_TYPO_REFERENCE_PERIOD",
    "BLS_JOLTS_INDEX_TYPO_RELEASE_URI",
    "BLS_JOLTS_INDEX_URI",
    "BLS_JOLTS_JOB_OPENINGS",
    "BLS_JOLTS_MEASURE_KEYS",
    "BLS_JOLTS_MEASURE_SCHEMA_VERSION",
    "BLS_JOLTS_PROGRAM_KEY",
    "BLS_JOLTS_RELEASE_SCHEMA_VERSION",
    "BLS_JOLTS_SOURCE_KEY",
    "BLS_JOLTS_TOTAL_SEPARATIONS",
    "BLS_JOLTS_UNAVAILABLE_REFERENCE_PERIOD",
    "BlsJoltsArchiveEntryV1",
    "BlsJoltsArchiveManifestV1",
    "BlsJoltsMeasureV1",
    "BlsJoltsReleaseIndexEntryV1",
    "BlsJoltsReleaseIndexV1",
    "BlsJoltsReleaseV1",
    "bls_jolts_coverage_from_manifest",
    "build_bls_jolts_archive_manifest",
    "build_bls_jolts_archive_requests",
    "build_bls_jolts_index_request",
    "load_packaged_bls_jolts_archive_manifest",
    "load_packaged_bls_jolts_index",
    "packaged_bls_jolts_archive_manifest_path",
    "packaged_bls_jolts_index_path",
    "parse_bls_jolts_release",
    "parse_bls_jolts_release_index",
    "replay_bls_jolts_archive",
]

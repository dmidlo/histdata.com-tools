"""Complete BLS Employment Situation archive qualification for issue #538.

The packaged manifest binds the official BLS archive index to three distinct
headline measures in every publication: the CPS unemployment rate, the CES
nonfarm-payroll monthly change, and CES average hourly earnings.  Release
bytes remain an external corpus.  Each entry nevertheless binds its current
and predecessor artifacts, normalized values, exact 08:30 Eastern publication
time, source-table era, and the January 2010 earnings-lineage break.
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

BLS_EMPLOYMENT_SITUATION_INDEX_SCHEMA_VERSION = (
    "histdatacom.bls-employment-situation-index.v1"
)
BLS_EMPLOYMENT_SITUATION_INDEX_ENTRY_SCHEMA_VERSION = (
    "histdatacom.bls-employment-situation-index-entry.v1"
)
BLS_EMPLOYMENT_SITUATION_MEASURE_SCHEMA_VERSION = (
    "histdatacom.bls-employment-situation-measure.v1"
)
BLS_EMPLOYMENT_SITUATION_ARCHIVE_ENTRY_SCHEMA_VERSION = (
    "histdatacom.bls-employment-situation-archive-entry.v1"
)
BLS_EMPLOYMENT_SITUATION_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.bls-employment-situation-archive-manifest.v1"
)
BLS_EMPLOYMENT_SITUATION_RELEASE_SCHEMA_VERSION = (
    "histdatacom.bls-employment-situation-release.v1"
)

BLS_EMPLOYMENT_SITUATION_SOURCE_KEY = "us.bls.employment-situation"
BLS_EMPLOYMENT_SITUATION_PROGRAM_KEY = "us.bls.employment-situation"
BLS_EMPLOYMENT_SITUATION_INDEX_URI = (
    "https://www.bls.gov/bls/news-release/empsit.htm"
)
BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI = (
    "https://www.bls.gov/news.release/history/empsit_01192000.txt"
)
BLS_EMPLOYMENT_SITUATION_PREDECESSOR_RELEASE_DATE = "2000-01-07"
BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD = "2010-01"
BLS_EMPLOYMENT_SITUATION_MALFORMED_ZONE_REFERENCE_PERIOD = "2012-11"

BLS_EMPLOYMENT_UNEMPLOYMENT_RATE = "unemployment-rate"
BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE = "nonfarm-payroll-change"
BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS = "average-hourly-earnings"
BLS_EMPLOYMENT_MEASURE_KEYS = (
    BLS_EMPLOYMENT_UNEMPLOYMENT_RATE,
    BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE,
    BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS,
)

MAX_BLS_EMPLOYMENT_SITUATION_RELEASES = 512
MAX_BLS_EMPLOYMENT_SITUATION_INDEX_BYTES = 4 * 1024 * 1024
MAX_BLS_EMPLOYMENT_SITUATION_RELEASE_BYTES = 16 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_EMPLOYMENT_URI_RE = re.compile(
    r"^https://www\.bls\.gov/news\.release/(?P<section>archives|history)/"
    r"empsit_(?P<date>\d{8})\.(?P<suffix>htm|txt)$"
)
_EMPLOYMENT_INDEX_URI_RE = re.compile(
    r"href=[\"'](?P<uri>(?:https://www\.bls\.gov)?/news\.release/"
    r"(?:archives|history)/empsit_(?P<date>\d{8})\."
    r"(?P<suffix>htm|txt))[\"']",
    re.IGNORECASE,
)
_EMPLOYMENT_NUMBER_RE = re.compile(
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


def _employment_lexical_number(value: object, name: str) -> tuple[str, float]:
    lexical = "".join(_required_text(value, name).split())
    match = _EMPLOYMENT_NUMBER_RE.fullmatch(lexical)
    if match is None:
        raise ValueError(f"{name} is not an Employment Situation number")
    normalized = match.group("number").replace(",", "")
    if normalized.startswith(("-.", "+.")):
        normalized = normalized[:1] + "0" + normalized[1:]
    elif normalized.startswith("."):
        normalized = "0" + normalized
    return lexical, _finite(normalized, name)


def _release_date_from_uri(uri: str) -> str:
    if uri == BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI:
        return BLS_EMPLOYMENT_SITUATION_PREDECESSOR_RELEASE_DATE
    match = _EMPLOYMENT_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("Employment Situation artifact URI is invalid")
    try:
        return (
            datetime.strptime(match.group("date"), "%m%d%Y")
            .replace(tzinfo=timezone.utc)
            .date()
            .isoformat()
        )
    except ValueError as exc:
        raise ValueError(
            "Employment Situation artifact URI contains an invalid date"
        ) from exc


def _source_format_for_uri(uri: str) -> OfficialSourceFormat:
    match = _EMPLOYMENT_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("Employment Situation artifact URI is invalid")
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


def _measure_definition(
    measure_key: str, reference_period: str
) -> tuple[str, str, str, bool]:
    reference = _year_month(reference_period, "reference_period")
    definitions = {
        BLS_EMPLOYMENT_UNEMPLOYMENT_RATE: (
            "cps-all-workers",
            "percent",
            "level",
            True,
        ),
        BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE: (
            "ces-total-nonfarm",
            "thousand-persons",
            "month-over-month-change",
            True,
        ),
    }
    if measure_key in definitions:
        return definitions[measure_key]
    if measure_key != BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS:
        raise ValueError("Employment Situation measure key is invalid")
    if (
        reference
        < BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD
    ):
        return (
            "ces-private-production-and-nonsupervisory",
            "usd-per-hour",
            "level",
            True,
        )
    return (
        "ces-total-private-all-employees",
        "usd-per-hour",
        "level",
        reference
        != BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD,
    )


@dataclass(frozen=True, slots=True)
class BlsEmploymentSituationReleaseIndexEntryV1:
    """One published reference-period row from the official archive index."""

    reference_period: str
    release_date: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    entry_id: str = ""
    schema_version: str = BLS_EMPLOYMENT_SITUATION_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_EMPLOYMENT_SITUATION_INDEX_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported BLS Employment Situation index-entry schema"
            )
        reference = _year_month(self.reference_period, "reference_period")
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format is not _source_format_for_uri(artifact):
            raise ValueError(
                "Employment Situation selected format differs from artifact URI"
            )
        if release != _release_date_from_uri(artifact):
            raise ValueError(
                "Employment Situation release date differs from artifact URI"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        expected = _stable_id(
            "bls-employment-situation-index-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "BLS Employment Situation index-entry identity differs"
            )
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsEmploymentSituationReleaseIndexEntryV1:
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
class BlsEmploymentSituationReleaseIndexV1:
    """As-of interpretation of the retained Employment Situation index."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[BlsEmploymentSituationReleaseIndexEntryV1, ...]
    unavailable_reference_periods: tuple[str, ...]
    index_id: str = ""
    schema_version: str = BLS_EMPLOYMENT_SITUATION_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_EMPLOYMENT_SITUATION_INDEX_SCHEMA_VERSION:
            raise ValueError(
                "unsupported BLS Employment Situation index schema"
            )
        if self.source_uri != BLS_EMPLOYMENT_SITUATION_INDEX_URI:
            raise ValueError("Employment Situation index URI is invalid")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_EMPLOYMENT_SITUATION_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        values = tuple(self.releases)
        if (
            not values
            or len(values) > MAX_BLS_EMPLOYMENT_SITUATION_RELEASES
            or any(
                not isinstance(item, BlsEmploymentSituationReleaseIndexEntryV1)
                for item in values
            )
        ):
            raise TypeError("Employment Situation index releases are invalid")
        references = tuple(item.reference_period for item in values)
        if references != tuple(sorted(set(references))):
            raise ValueError(
                "Employment Situation index releases must be unique and sorted"
            )
        if references[0] != "2000-01":
            raise ValueError(
                "Employment Situation index omits the January 2000 boundary"
            )
        if any(item.release_date > as_of for item in values):
            raise ValueError(
                "Employment Situation index contains a future publication"
            )
        release_dates = tuple(item.release_date for item in values)
        if release_dates != tuple(sorted(set(release_dates))):
            raise ValueError(
                "Employment Situation publication dates are not unique and chronological"
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
            raise ValueError(
                "published and unavailable Employment Situation periods overlap"
            )
        observed = tuple(sorted((*references, *unavailable)))
        if observed != _contiguous_months("2000-01", observed[-1]):
            raise ValueError(
                "Employment Situation index has an unexplained monthly gap"
            )
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", values)
        object.__setattr__(self, "unavailable_reference_periods", unavailable)
        expected = _stable_id(
            "bls-employment-situation-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected:
            raise ValueError("BLS Employment Situation index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, BlsEmploymentSituationReleaseIndexEntryV1]:
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsEmploymentSituationReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                BlsEmploymentSituationReleaseIndexEntryV1.from_dict(
                    _mapping(item, "Employment Situation index entry")
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
class BlsEmploymentSituationReleaseV1:
    """Three independent labour triplets from one Employment Situation."""

    unemployment_rate: UnitedStatesReleaseTripletV1
    nonfarm_payroll_change: UnitedStatesReleaseTripletV1
    average_hourly_earnings: UnitedStatesReleaseTripletV1
    release_id: str = ""
    schema_version: str = BLS_EMPLOYMENT_SITUATION_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_EMPLOYMENT_SITUATION_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported BLS Employment Situation release schema"
            )
        triplets = self.triplets
        if any(
            not isinstance(item, UnitedStatesReleaseTripletV1)
            for item in triplets
        ):
            raise TypeError(
                "Employment Situation release requires U.S. release triplets"
            )
        expected_shapes = (
            (
                BLS_EMPLOYMENT_UNEMPLOYMENT_RATE,
                "percent",
                "level",
            ),
            (
                BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE,
                "thousand-persons",
                "month-over-month-change",
            ),
            (
                BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS,
                "usd-per-hour",
                "level",
            ),
        )
        first = triplets[0]
        for triplet, (measure, unit, transformation) in zip(
            triplets, expected_shapes
        ):
            if (
                triplet.program_key != BLS_EMPLOYMENT_SITUATION_PROGRAM_KEY
                or triplet.event_family is not EconomicEventFamily.LABOUR_MARKET
                or f".{measure}." not in triplet.logical_event_key
                or triplet.unit != unit
                or triplet.transformation != transformation
                or triplet.seasonality != "seasonally-adjusted"
            ):
                raise ValueError(
                    "Employment Situation release measure identity differs"
                )
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
                raise ValueError(
                    "Employment Situation package evidence is inconsistent"
                )
        expected = _stable_id(
            "bls-employment-situation-release", self.identity_payload()
        )
        if self.release_id and self.release_id != expected:
            raise ValueError(
                "BLS Employment Situation release identity differs"
            )
        object.__setattr__(self, "release_id", expected)

    @property
    def triplets(self) -> tuple[UnitedStatesReleaseTripletV1, ...]:
        return (
            self.unemployment_rate,
            self.nonfarm_payroll_change,
            self.average_hourly_earnings,
        )

    @property
    def reference_period(self) -> str:
        return str(self.unemployment_rate.reference_period)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "unemployment_rate": self.unemployment_rate.to_dict(),
            "nonfarm_payroll_change": self.nonfarm_payroll_change.to_dict(),
            "average_hourly_earnings": self.average_hourly_earnings.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsEmploymentSituationReleaseV1:
        return cls(
            unemployment_rate=UnitedStatesReleaseTripletV1.from_dict(
                _mapping(data.get("unemployment_rate"), "unemployment_rate")
            ),
            nonfarm_payroll_change=UnitedStatesReleaseTripletV1.from_dict(
                _mapping(
                    data.get("nonfarm_payroll_change"),
                    "nonfarm_payroll_change",
                )
            ),
            average_hourly_earnings=UnitedStatesReleaseTripletV1.from_dict(
                _mapping(
                    data.get("average_hourly_earnings"),
                    "average_hourly_earnings",
                )
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsEmploymentSituationMeasureV1:
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
    schema_version: str = BLS_EMPLOYMENT_SITUATION_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_EMPLOYMENT_SITUATION_MEASURE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported BLS Employment Situation measure schema"
            )
        measure = _required_text(self.measure_key, "measure_key")
        if measure not in BLS_EMPLOYMENT_MEASURE_KEYS:
            raise ValueError("Employment Situation measure key is invalid")
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
            lexical, numeric = _employment_lexical_number(
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
        expected = _stable_id(
            "bls-employment-situation-measure", self.identity_payload()
        )
        if self.measure_id and self.measure_id != expected:
            raise ValueError(
                "BLS Employment Situation measure identity differs"
            )
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsEmploymentSituationMeasureV1:
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
class BlsEmploymentSituationArchiveEntryV1:
    """One current/predecessor-backed Employment Situation publication."""

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
    measures: tuple[BlsEmploymentSituationMeasureV1, ...]
    entry_id: str = ""
    schema_version: str = BLS_EMPLOYMENT_SITUATION_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_EMPLOYMENT_SITUATION_ARCHIVE_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported BLS Employment Situation archive-entry schema"
            )
        reference = _year_month(self.reference_period, "reference_period")
        previous_reference = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_reference >= reference:
            raise ValueError(
                "Employment Situation previous reference period must precede current"
            )
        if reference != "2025-11" and previous_reference != _previous_month(
            reference
        ):
            raise ValueError(
                "Employment Situation predecessor is not the prior month"
            )
        if reference == "2025-11" and previous_reference != "2025-09":
            raise ValueError(
                "Employment Situation shutdown predecessor is invalid"
            )
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        previous_artifact = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format is not _source_format_for_uri(artifact):
            raise ValueError(
                "Employment Situation entry format differs from artifact URI"
            )
        if release != _release_date_from_uri(artifact):
            raise ValueError(
                "Employment Situation entry release date differs from artifact URI"
            )
        for name in ("content_sha256", "previous_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_EMPLOYMENT_SITUATION_RELEASE_BYTES,
        )
        _positive_int(
            self.previous_content_length,
            "previous_content_length",
            MAX_BLS_EMPLOYMENT_SITUATION_RELEASE_BYTES,
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T08:30:00":
            raise ValueError(
                "Employment Situation released time differs from release date"
            )
        zone = _required_text(self.reported_zone, "reported_zone")
        if zone not in {"EST", "EDT", "ET"}:
            raise ValueError("Employment Situation reported zone is invalid")
        era = _required_text(self.source_era, "source_era")
        expected_era = (
            "fixed-width-text"
            if source_format is OfficialSourceFormat.TEXT
            else (
                "preformatted-html"
                if reference
                < BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD
                else "semantic-html"
            )
        )
        if era != expected_era:
            raise ValueError(
                "Employment Situation source era differs from reference period"
            )
        measures = tuple(self.measures)
        if (
            len(measures) != len(BLS_EMPLOYMENT_MEASURE_KEYS)
            or any(
                not isinstance(item, BlsEmploymentSituationMeasureV1)
                for item in measures
            )
            or tuple(item.measure_key for item in measures)
            != BLS_EMPLOYMENT_MEASURE_KEYS
        ):
            raise ValueError(
                "Employment Situation entry measure package is invalid"
            )
        for measure in measures:
            lineage, unit, transformation, comparable = _measure_definition(
                measure.measure_key, reference
            )
            if (
                measure.series_lineage != lineage
                or measure.unit != unit
                or measure.transformation != transformation
                or measure.revision_comparable is not comparable
            ):
                raise ValueError(
                    "Employment Situation measure definition differs"
                )
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
            "bls-employment-situation-archive-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "BLS Employment Situation archive-entry identity differs"
            )
        object.__setattr__(self, "entry_id", expected)

    @property
    def by_measure(self) -> Mapping[str, BlsEmploymentSituationMeasureV1]:
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
            "measures": [item.to_dict() for item in self.measures],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsEmploymentSituationArchiveEntryV1:
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
            measures=tuple(
                BlsEmploymentSituationMeasureV1.from_dict(
                    _mapping(item, "Employment Situation measure")
                )
                for item in _sequence(data.get("measures"), "measures")
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsEmploymentSituationArchiveManifestV1:
    """Compact closure evidence for the complete as-of publication archive."""

    registry_id: str
    profile_id: str
    release_index: BlsEmploymentSituationReleaseIndexV1
    entries: tuple[BlsEmploymentSituationArchiveEntryV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    revision_occurrence_count: int
    unemployment_revision_count: int
    nonfarm_payroll_revision_count: int
    earnings_revision_count: int
    noncomparable_revision_count: int
    manifest_id: str = ""
    schema_version: str = (
        BLS_EMPLOYMENT_SITUATION_ARCHIVE_MANIFEST_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_EMPLOYMENT_SITUATION_ARCHIVE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported BLS Employment Situation archive-manifest schema"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "Employment Situation manifest registry identity is invalid"
            )
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError(
                "Employment Situation manifest profile identity is invalid"
            )
        if not isinstance(
            self.release_index, BlsEmploymentSituationReleaseIndexV1
        ):
            raise TypeError(
                "Employment Situation manifest requires a v1 release index"
            )
        entries = tuple(self.entries)
        if (
            not entries
            or len(entries) > MAX_BLS_EMPLOYMENT_SITUATION_RELEASES
            or any(
                not isinstance(item, BlsEmploymentSituationArchiveEntryV1)
                for item in entries
            )
        ):
            raise TypeError("Employment Situation manifest entries are invalid")
        references = tuple(item.reference_period for item in entries)
        if references != tuple(
            item.reference_period for item in self.release_index.releases
        ):
            raise ValueError(
                "Employment Situation manifest differs from its release index"
            )
        releases = self.release_index.by_reference_period
        for item in entries:
            indexed = releases[item.reference_period]
            if (
                item.release_date != indexed.release_date
                or item.artifact_uri != indexed.artifact_uri
                or item.source_format is not indexed.source_format
            ):
                raise ValueError(
                    "Employment Situation manifest entry differs from the index"
                )
        for previous, current in pairwise(entries):
            if (
                current.previous_reference_period != previous.reference_period
                or current.previous_artifact_uri != previous.artifact_uri
                or current.previous_content_sha256 != previous.content_sha256
                or current.previous_content_length != previous.content_length
            ):
                raise ValueError(
                    "Employment Situation predecessor artifact chain is broken"
                )
        first = entries[0]
        if (
            first.previous_reference_period != "1999-12"
            or first.previous_artifact_uri
            != BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI
        ):
            raise ValueError(
                "Employment Situation manifest omits its December 1999 predecessor"
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
            for measure in BLS_EMPLOYMENT_MEASURE_KEYS
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
            "unemployment_revision_count": measure_counts[
                BLS_EMPLOYMENT_UNEMPLOYMENT_RATE
            ],
            "nonfarm_payroll_revision_count": measure_counts[
                BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE
            ],
            "earnings_revision_count": measure_counts[
                BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS
            ],
            "noncomparable_revision_count": noncomparable,
        }
        for name, expected_count in expected_counts.items():
            if getattr(self, name) != expected_count:
                raise ValueError(
                    f"Employment Situation manifest {name} differs"
                )
        if len({item.content_sha256 for item in entries}) != len(entries):
            raise ValueError(
                "Employment Situation manifest repeats current artifact content"
            )
        if len({item[1] for item in artifact_evidence}) != raw_count:
            raise ValueError(
                "Employment Situation manifest repeats a raw artifact hash"
            )
        normalized = {
            measure.normalized_sha256
            for item in entries
            for measure in item.measures
        }
        if len(normalized) != len(entries) * len(BLS_EMPLOYMENT_MEASURE_KEYS):
            raise ValueError(
                "Employment Situation manifest repeats normalized content"
            )
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "bls-employment-situation-archive-manifest",
            self.identity_payload(),
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError(
                "BLS Employment Situation archive identity differs"
            )
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_reference_period(
        self,
    ) -> Mapping[str, BlsEmploymentSituationArchiveEntryV1]:
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
            "unemployment_revision_count": self.unemployment_revision_count,
            "nonfarm_payroll_revision_count": (
                self.nonfarm_payroll_revision_count
            ),
            "earnings_revision_count": self.earnings_revision_count,
            "noncomparable_revision_count": (self.noncomparable_revision_count),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsEmploymentSituationArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=BlsEmploymentSituationReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            entries=tuple(
                BlsEmploymentSituationArchiveEntryV1.from_dict(
                    _mapping(item, "Employment Situation archive entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            unemployment_revision_count=cast(
                int, data.get("unemployment_revision_count")
            ),
            nonfarm_payroll_revision_count=cast(
                int, data.get("nonfarm_payroll_revision_count")
            ),
            earnings_revision_count=cast(
                int, data.get("earnings_revision_count")
            ),
            noncomparable_revision_count=cast(
                int, data.get("noncomparable_revision_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> BlsEmploymentSituationArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Employment Situation archive manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload, "archive manifest"))


def build_bls_employment_situation_index_request(
    registry: OfficialSourceRegistryV1,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    """Plan the exact official Employment Situation index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError(
            "Employment Situation index request requires a v1 source registry"
        )
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(BLS_EMPLOYMENT_SITUATION_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=BLS_EMPLOYMENT_SITUATION_INDEX_URI,
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


def parse_bls_employment_situation_release_index(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> BlsEmploymentSituationReleaseIndexV1:
    """Parse published rows and explicit gaps from the BLS archive page."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError(
            "Employment Situation index parser requires a v1 snapshot"
        )
    if (
        snapshot.request.source_key != BLS_EMPLOYMENT_SITUATION_SOURCE_KEY
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.uri != BLS_EMPLOYMENT_SITUATION_INDEX_URI
    ):
        raise ValueError(
            "Employment Situation index parser received a different source"
        )
    as_of = _iso_date(as_of_date, "as_of_date")
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "BLS Employment Situation release index is not UTF-8"
        ) from exc
    text = re.sub(r"(?is)<!--.*?-->", "", text)
    releases: list[BlsEmploymentSituationReleaseIndexEntryV1] = []
    unavailable: list[str] = []
    seen: set[str] = set()
    for list_item in re.finditer(r"(?is)<li\b[^>]*>(.*?)</li>", text):
        markup = list_item.group(1)
        label = _visible_html(markup)
        label_match = re.search(
            r"(?P<month>January|February|March|April|May|June|July|"
            r"August|September|October|November|December)\s+"
            r"(?P<year>\d{4})\s+Employment Situation",
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
            raise ValueError(
                "BLS Employment Situation index repeats a reference period"
            )
        seen.add(reference)
        uri_matches = tuple(_EMPLOYMENT_INDEX_URI_RE.finditer(markup))
        if not uri_matches:
            if "not published" not in label.lower():
                raise ValueError(
                    "BLS Employment Situation index row has no release artifact"
                )
            if reference <= as_of[:7]:
                unavailable.append(reference)
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
        release_date = _release_date_from_uri(artifact_uri)
        if release_date > as_of:
            continue
        releases.append(
            BlsEmploymentSituationReleaseIndexEntryV1(
                reference_period=reference,
                release_date=release_date,
                artifact_uri=artifact_uri,
                source_format=_source_format_for_uri(artifact_uri),
            )
        )
    return BlsEmploymentSituationReleaseIndexV1(
        source_uri=BLS_EMPLOYMENT_SITUATION_INDEX_URI,
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
    source = registry.source(BLS_EMPLOYMENT_SITUATION_SOURCE_KEY)
    source_format = _source_format_for_uri(uri)
    if source_format not in source.formats:
        raise ValueError(
            "Employment Situation source does not declare the artifact format"
        )
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


def build_bls_employment_situation_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: BlsEmploymentSituationReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the predecessor plus every published artifact request."""
    if not isinstance(release_index, BlsEmploymentSituationReleaseIndexV1):
        raise TypeError(
            "Employment Situation requests require a v1 release index"
        )
    uris = (BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI,) + tuple(
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
                "Employment Situation release is neither UTF-8 nor Windows-1252"
            ) from exc


def _strip_html(value: str) -> str:
    result = re.sub(r"(?i)<br\s*/?>", "\n", value)
    result = re.sub(r"(?i)</(?:p|div|tr|li|h\d)>", "\n", result)
    return html.unescape(re.sub(r"<[^>]+>", " ", result)).replace("\xa0", " ")


def _main_html_release(value: str) -> str:
    candidates: list[str] = []
    for match in re.finditer(r"(?is)<pre\b[^>]*>(.*?)</pre>", value):
        candidate = _strip_html(match.group(1))
        if "8:30" in candidate and "EMPLOYMENT SITUATION" in candidate.upper():
            candidates.append(candidate)
    if len(candidates) != 1:
        raise ValueError(
            "Employment Situation HTML release body is missing or changed"
        )
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


def _html_table_row_values(
    value: str,
    *,
    table_id: str,
    row_ids: tuple[str, ...],
) -> tuple[Mapping[str, str], str] | None:
    table = re.search(
        rf"(?is)<table\b[^>]*\bid=[\"']{table_id}[\"'][^>]*>"
        r"(?P<body>.*?)</table>",
        value,
    )
    if table is None:
        return None
    head = re.search(r"(?is)<thead\b[^>]*>(.*?)</thead>", table.group("body"))
    if head is None:
        raise ValueError(f"Employment Situation table {table_id} has no header")
    periods = tuple(
        period
        for cell in re.findall(r"(?is)<th\b[^>]*>(.*?)</th>", head.group(1))
        if (period := _month_from_header(cell)) is not None
    )
    if len(periods) != 4 or len(set(periods)) != 4:
        raise ValueError(
            f"Employment Situation table {table_id} period header changed"
        )
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
        identifier = re.search(
            r"\bid=[\"'](?P<id>[^\"']+)",
            heading.group("attrs"),
            re.IGNORECASE,
        )
        if identifier is None or identifier.group("id") not in row_ids:
            continue
        values = tuple(
            _visible_html(cell)
            for cell in re.findall(
                r"(?is)<td\b[^>]*>(.*?)</td>", row.group("body")
            )
        )
        candidates.append((identifier.group("id"), values))
    if not candidates or (len(row_ids) == 1 and len(candidates) != 1):
        raise ValueError(
            f"Employment Situation table {table_id} target row changed"
        )
    row_id, values = candidates[0]
    if len(values) < len(periods):
        raise ValueError(
            f"Employment Situation table {table_id} target row is short"
        )
    return (
        MappingProxyType(dict(zip(periods, values))),
        f"table#{table_id}:row#{row_id}",
    )


@dataclass(frozen=True, slots=True)
class _LegacyRow:
    values: tuple[str, ...]
    line_number: int
    locator: str


def _legacy_summary_rows(value: str) -> Mapping[str, _LegacyRow]:
    lines = value.splitlines()
    observed: dict[str, _LegacyRow] = {}
    for index, line in enumerate(lines):
        compact = " ".join(line.split())
        key: str | None = None
        if re.match(r"^All workers\s*\.*\|", compact, re.IGNORECASE):
            key = BLS_EMPLOYMENT_UNEMPLOYMENT_RATE
        elif re.match(r"^Nonfarm employment\s*\.*\|", compact, re.IGNORECASE):
            key = BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE
        elif re.match(
            r"^(?:Average|Avg\.) hourly earnings,\s*\|",
            compact,
            re.IGNORECASE,
        ):
            for following_index in range(index + 1, min(index + 4, len(lines))):
                following = lines[following_index]
                if "total private" in following.lower() and "|" in following:
                    values = tuple(following.split("|")[1:])
                    observed[BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS] = (
                        _LegacyRow(
                            values=values,
                            line_number=following_index + 1,
                            locator=(
                                f"line:{following_index + 1}:summary-table-a:"
                                "average-hourly-earnings-total-private"
                            ),
                        )
                    )
                    break
            continue
        if key is not None:
            observed[key] = _LegacyRow(
                values=tuple(line.split("|")[1:]),
                line_number=index + 1,
                locator=f"line:{index + 1}:summary-table-a:{key}",
            )
    if set(observed) != set(BLS_EMPLOYMENT_MEASURE_KEYS):
        raise ValueError(
            "Employment Situation fixed-width Summary Table A row is missing"
        )
    if any(len(row.values) != 6 for row in observed.values()):
        raise ValueError(
            "Employment Situation fixed-width Summary Table A shape changed"
        )
    return MappingProxyType(observed)


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
class _EmploymentSituationObservation:
    reference_period: str
    release_date: str
    released_lexical: str
    reported_zone: str
    source_era: str
    source_encoding: str
    release_time_locator: str
    measures: tuple[_MeasureObservation, ...]

    @property
    def by_measure(self) -> Mapping[str, _MeasureObservation]:
        return MappingProxyType(
            {item.measure_key: item for item in self.measures}
        )


def _semantic_measure(
    raw_html: str,
    *,
    measure_key: str,
    reference_period: str,
    comparison_reference_period: str | None,
) -> _MeasureObservation:
    table_id, row_ids = {
        BLS_EMPLOYMENT_UNEMPLOYMENT_RATE: (
            "cps_empsit_sum",
            ("cps_empsit_sum.r.2.1.3.1",),
        ),
        BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE: (
            "ces_table10",
            ("ces_table10.r.1.1",),
        ),
        BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS: (
            "ces_table10",
            ("ces_table10.r.3.1.2", "ces_table10.r.4.1.2"),
        ),
    }[measure_key]
    result = _html_table_row_values(
        raw_html, table_id=table_id, row_ids=row_ids
    )
    if result is None:
        raise ValueError(
            f"Employment Situation semantic table {table_id} is absent"
        )
    values, locator = result
    try:
        actual_lexical, actual = _employment_lexical_number(
            values[reference_period], "actual_lexical"
        )
    except KeyError as exc:
        raise ValueError(
            f"Employment Situation {measure_key} omits the reference period"
        ) from exc
    revised: float | None = None
    revised_lexical: str | None = None
    revision_locator = f"{locator}:no-comparison-requested"
    if comparison_reference_period is not None:
        comparable = _measure_definition(measure_key, reference_period)[3]
        if comparable:
            try:
                revised_lexical, revised = _employment_lexical_number(
                    values[comparison_reference_period],
                    "revised_previous_lexical",
                )
            except KeyError as exc:
                raise ValueError(
                    f"Employment Situation {measure_key} omits the comparison period"
                ) from exc
            revision_locator = f"{locator}:column:{comparison_reference_period}"
        else:
            revision_locator = "summary-table-b:production-and-nonsupervisory-to-all-employees-break"
    return _MeasureObservation(
        measure_key=measure_key,
        actual_value=actual,
        actual_lexical=actual_lexical,
        revised_previous_value=revised,
        revised_previous_lexical=revised_lexical,
        actual_locator=f"{locator}:column:{reference_period}",
        revision_locator=revision_locator,
    )


def _legacy_measures(
    main_text: str,
    *,
    reference_period: str,
    comparison_reference_period: str | None,
) -> tuple[_MeasureObservation, ...]:
    if (
        comparison_reference_period is not None
        and comparison_reference_period != _previous_month(reference_period)
    ):
        raise ValueError(
            "fixed-width Employment Situation comparison is not the prior month"
        )
    rows = _legacy_summary_rows(main_text)
    unemployment_row = rows[BLS_EMPLOYMENT_UNEMPLOYMENT_RATE]
    nonfarm_row = rows[BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE]
    earnings_row = rows[BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS]
    unemployment_lexical, unemployment = _employment_lexical_number(
        unemployment_row.values[-2], "unemployment actual"
    )
    nonfarm_lexical, nonfarm = _employment_lexical_number(
        nonfarm_row.values[-1], "nonfarm actual"
    )
    earnings_lexical, earnings = _employment_lexical_number(
        earnings_row.values[-2], "earnings actual"
    )
    revised_unemployment: tuple[str, float] | None = None
    revised_nonfarm: tuple[str, float] | None = None
    revised_earnings: tuple[str, float] | None = None
    if comparison_reference_period is not None:
        revised_unemployment = _employment_lexical_number(
            unemployment_row.values[-3], "revised unemployment"
        )
        prior_level_lexical, prior_level = _employment_lexical_number(
            nonfarm_row.values[-3], "revised prior payroll level"
        )
        earlier_level_lexical, earlier_level = _employment_lexical_number(
            nonfarm_row.values[-4], "revised earlier payroll level"
        )
        derived = prior_level - earlier_level
        if not derived.is_integer():
            raise ValueError(
                "fixed-width payroll level difference is not an integer"
            )
        revised_nonfarm = (str(int(derived)), derived)
        revised_earnings = _employment_lexical_number(
            earnings_row.values[-3], "revised earnings"
        )
        nonfarm_revision_locator = (
            f"{nonfarm_row.locator}:derived-columns:"
            f"{prior_level_lexical}-minus-{earlier_level_lexical}"
        )
    else:
        nonfarm_revision_locator = (
            f"{nonfarm_row.locator}:no-comparison-requested"
        )
    return (
        _MeasureObservation(
            measure_key=BLS_EMPLOYMENT_UNEMPLOYMENT_RATE,
            actual_value=unemployment,
            actual_lexical=unemployment_lexical,
            revised_previous_value=(
                None
                if revised_unemployment is None
                else revised_unemployment[1]
            ),
            revised_previous_lexical=(
                None
                if revised_unemployment is None
                else revised_unemployment[0]
            ),
            actual_locator=(f"{unemployment_row.locator}:current-month-column"),
            revision_locator=(
                f"{unemployment_row.locator}:previous-month-column"
                if revised_unemployment is not None
                else f"{unemployment_row.locator}:no-comparison-requested"
            ),
        ),
        _MeasureObservation(
            measure_key=BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE,
            actual_value=nonfarm,
            actual_lexical=nonfarm_lexical,
            revised_previous_value=(
                None if revised_nonfarm is None else revised_nonfarm[1]
            ),
            revised_previous_lexical=(
                None if revised_nonfarm is None else revised_nonfarm[0]
            ),
            actual_locator=f"{nonfarm_row.locator}:published-change-column",
            revision_locator=nonfarm_revision_locator,
        ),
        _MeasureObservation(
            measure_key=BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS,
            actual_value=earnings,
            actual_lexical=earnings_lexical,
            revised_previous_value=(
                None if revised_earnings is None else revised_earnings[1]
            ),
            revised_previous_lexical=(
                None if revised_earnings is None else revised_earnings[0]
            ),
            actual_locator=f"{earnings_row.locator}:current-month-column",
            revision_locator=(
                f"{earnings_row.locator}:previous-month-column"
                if revised_earnings is not None
                else f"{earnings_row.locator}:no-comparison-requested"
            ),
        ),
    )


def _parse_bls_employment_situation_observation(
    snapshot: OfficialRawSnapshotV1,
    reference_period: str,
    *,
    comparison_reference_period: str | None = None,
) -> _EmploymentSituationObservation:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError(
            "Employment Situation parser requires a v1 official snapshot"
        )
    if snapshot.request.source_key != BLS_EMPLOYMENT_SITUATION_SOURCE_KEY:
        raise ValueError(
            "Employment Situation parser received a different official source"
        )
    reference = _year_month(reference_period, "reference_period")
    source_format = snapshot.request.source_format
    if source_format is not _source_format_for_uri(snapshot.request.uri):
        raise ValueError(
            "Employment Situation request format differs from artifact URI"
        )
    if source_format not in {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }:
        raise ValueError(
            "Employment Situation parser received an unsupported format"
        )
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
        raise ValueError(
            "Employment Situation release date is absent from the header"
        )
    time_match = re.search(
        r"8:30\s*(?:A\.?M\.?)\s*\((?P<zone>E\.?S\.?T\.?|" r"E\.?D\.?T\.?|ET)\)",
        main_text,
        re.IGNORECASE,
    )
    if time_match is None:
        raise ValueError(
            "Employment Situation 08:30 release-time header is missing"
        )
    reported_zone = time_match.group("zone").upper().replace(".", "")
    released_lexical = f"{release_date}T08:30:00"
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
    if (
        reported_zone in {"EST", "EDT"}
        and reported_zone != expected_zone
        and reference
        != BLS_EMPLOYMENT_SITUATION_MALFORMED_ZONE_REFERENCE_PERIOD
    ):
        raise ValueError(
            "Employment Situation timezone conflicts with release date"
        )
    year, month = (int(item) for item in reference.split("-"))
    if (
        re.search(
            rf"(?:THE\s+)?EMPLOYMENT SITUATION\s*(?::|--?|–|—)+\s*"
            rf"{calendar.month_name[month]}\s+{year}",
            header,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError(
            "Employment Situation reference period is absent from the title"
        )
    comparison = (
        None
        if comparison_reference_period is None
        else _year_month(
            comparison_reference_period, "comparison_reference_period"
        )
    )
    if comparison is not None and comparison >= reference:
        raise ValueError(
            "Employment Situation comparison period must precede current"
        )
    semantic = (
        raw_html is not None
        and re.search(
            r"<table\b[^>]*\bid=[\"']cps_empsit_sum[\"']",
            raw_html,
            re.IGNORECASE,
        )
        is not None
    )
    if semantic:
        measures = tuple(
            _semantic_measure(
                cast(str, raw_html),
                measure_key=measure_key,
                reference_period=reference,
                comparison_reference_period=comparison,
            )
            for measure_key in BLS_EMPLOYMENT_MEASURE_KEYS
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
    expected_era = (
        "semantic-html"
        if reference
        >= BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD
        else (
            "fixed-width-text"
            if source_format is OfficialSourceFormat.TEXT
            else "preformatted-html"
        )
    )
    if source_era != expected_era:
        raise ValueError(
            "Employment Situation source-table era conflicts with reference"
        )
    release_line = main_text[: time_match.start()].count("\n") + 1
    return _EmploymentSituationObservation(
        reference_period=reference,
        release_date=release_date,
        released_lexical=released_lexical,
        reported_zone=reported_zone,
        source_era=source_era,
        source_encoding=source_encoding,
        release_time_locator=f"line:{release_line}:release-header",
        measures=measures,
    )


def _measure_limitations(
    measure_key: str,
    *,
    current: _EmploymentSituationObservation,
    previous_snapshot: OfficialRawSnapshotV1,
) -> tuple[str, ...]:
    specific = {
        BLS_EMPLOYMENT_UNEMPLOYMENT_RATE: (
            "This measure is the CPS all-workers unemployment rate; household and establishment surveys are not interchangeable.",
        ),
        BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE: (
            "This measure is the CES total-nonfarm monthly change in thousands; benchmark revisions never rewrite the retained initial vintage.",
        ),
        BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS: (
            "This measure is the CES total-private average hourly earnings level in dollars per hour.",
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
    if (
        measure_key == BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS
        and current.reference_period
        == BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD
    ):
        limitations += (
            "January 2010 switches the summary earnings headline from production and nonsupervisory workers to all employees; the prior field is explicitly noncomparable.",
        )
    if current.reference_period == "2025-11":
        limitations += (
            "October 2025 was not published; previous-as-known remains the September 2025 publication while the November tables include subsequently calculated October values.",
        )
    if (
        current.reference_period
        == BLS_EMPLOYMENT_SITUATION_MALFORMED_ZONE_REFERENCE_PERIOD
    ):
        limitations += (
            "The December 7, 2012 source header reports EDT during standard time; normalization uses America/New_York and preserves the malformed lexical evidence.",
        )
    return limitations


def parse_bls_employment_situation_release(
    snapshot: OfficialRawSnapshotV1,
    *,
    reference_period: str,
    previous_snapshot: OfficialRawSnapshotV1,
    previous_reference_period: str,
) -> BlsEmploymentSituationReleaseV1:
    """Parse the three headlines with their occurrence-specific predecessor."""
    current = _parse_bls_employment_situation_observation(
        snapshot,
        reference_period,
        comparison_reference_period=previous_reference_period,
    )
    previous = _parse_bls_employment_situation_observation(
        previous_snapshot, previous_reference_period
    )
    if previous.reference_period >= current.reference_period:
        raise ValueError(
            "Employment Situation predecessor does not precede current"
        )
    timestamp = normalize_official_source_timestamp(
        current.released_lexical,
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    triplets: list[UnitedStatesReleaseTripletV1] = []
    for measure_key in BLS_EMPLOYMENT_MEASURE_KEYS:
        observed = current.by_measure[measure_key]
        prior = previous.by_measure[measure_key]
        lineage, unit, transformation, comparable = _measure_definition(
            measure_key, current.reference_period
        )
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
        if comparable and (
            observed.revised_previous_value is None
            or observed.revised_previous_lexical is None
        ):
            raise ValueError(
                "Employment Situation comparable revision is unavailable"
            )
        triplets.append(
            UnitedStatesReleaseTripletV1(
                program_key=BLS_EMPLOYMENT_SITUATION_PROGRAM_KEY,
                logical_event_key=(
                    f"us.bls.employment-situation.{measure_key}."
                    f"{lineage}.{current.reference_period}"
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
    return BlsEmploymentSituationReleaseV1(
        unemployment_rate=triplets[0],
        nonfarm_payroll_change=triplets[1],
        average_hourly_earnings=triplets[2],
    )


def _snapshots_by_uri(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> Mapping[str, OfficialRawSnapshotV1]:
    result: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, OfficialRawSnapshotV1):
            raise TypeError(
                "Employment Situation archive requires v1 official snapshots"
            )
        request = snapshot.request
        release_date = _release_date_from_uri(request.uri)
        if (
            request.source_key != BLS_EMPLOYMENT_SITUATION_SOURCE_KEY
            or request.source_format is not _source_format_for_uri(request.uri)
            or request.window_start != release_date
            or request.window_end != release_date
        ):
            raise ValueError(
                "Employment Situation snapshot has a different scope"
            )
        if request.uri in result:
            raise ValueError(
                "Employment Situation archive repeats an artifact snapshot"
            )
        result[request.uri] = snapshot
    return MappingProxyType(result)


def _entry_from_release(
    indexed: BlsEmploymentSituationReleaseIndexEntryV1,
    current: OfficialRawSnapshotV1,
    previous: OfficialRawSnapshotV1,
    release: BlsEmploymentSituationReleaseV1,
) -> BlsEmploymentSituationArchiveEntryV1:
    observation = _parse_bls_employment_situation_observation(
        current,
        indexed.reference_period,
        comparison_reference_period=(
            release.unemployment_rate.previous_reference_period
        ),
    )
    measures: list[BlsEmploymentSituationMeasureV1] = []
    for measure_key, triplet in zip(
        BLS_EMPLOYMENT_MEASURE_KEYS, release.triplets
    ):
        lineage, unit, transformation, comparable = _measure_definition(
            measure_key, indexed.reference_period
        )
        measures.append(
            BlsEmploymentSituationMeasureV1(
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
    return BlsEmploymentSituationArchiveEntryV1(
        reference_period=indexed.reference_period,
        previous_reference_period=(
            release.unemployment_rate.previous_reference_period
        ),
        release_date=indexed.release_date,
        artifact_uri=current.request.uri,
        source_format=current.request.source_format,
        content_sha256=current.content_sha256,
        content_length=len(current.content),
        previous_artifact_uri=previous.request.uri,
        previous_content_sha256=previous.content_sha256,
        previous_content_length=len(previous.content),
        released_lexical=release.unemployment_rate.released_lexical,
        reported_zone=observation.reported_zone,
        source_era=observation.source_era,
        measures=tuple(measures),
    )


def build_bls_employment_situation_archive_manifest(
    profile: UnitedStatesBackfillProfileV1,
    release_index: BlsEmploymentSituationReleaseIndexV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> BlsEmploymentSituationArchiveManifestV1:
    """Replay every indexed publication into compact closure evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError(
            "Employment Situation manifest requires a v1 U.S. profile"
        )
    program = profile.by_key.get(BLS_EMPLOYMENT_SITUATION_PROGRAM_KEY)
    if (
        program is None
        or program.source_key != BLS_EMPLOYMENT_SITUATION_SOURCE_KEY
    ):
        raise ValueError(
            "U.S. profile omits the dedicated Employment Situation source"
        )
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI} | {
        item.artifact_uri for item in release_index.releases
    }
    if set(by_uri) != expected_uris:
        raise ValueError(
            "Employment Situation snapshots differ from the release index"
        )
    entries: list[BlsEmploymentSituationArchiveEntryV1] = []
    previous_snapshot = by_uri[BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI]
    previous_reference = "1999-12"
    for indexed in release_index.releases:
        current = by_uri[indexed.artifact_uri]
        release = parse_bls_employment_situation_release(
            current,
            reference_period=indexed.reference_period,
            previous_snapshot=previous_snapshot,
            previous_reference_period=previous_reference,
        )
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
        for measure in BLS_EMPLOYMENT_MEASURE_KEYS
    }
    return BlsEmploymentSituationArchiveManifestV1(
        registry_id=profile.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        entries=values,
        raw_artifact_count=len(artifact_evidence),
        total_content_bytes=sum(artifact_evidence.values()),
        revision_occurrence_count=sum(
            item.previous_was_revised for item in values
        ),
        unemployment_revision_count=measure_counts[
            BLS_EMPLOYMENT_UNEMPLOYMENT_RATE
        ],
        nonfarm_payroll_revision_count=measure_counts[
            BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE
        ],
        earnings_revision_count=measure_counts[
            BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS
        ],
        noncomparable_revision_count=sum(
            not measure.revision_comparable
            for item in values
            for measure in item.measures
        ),
    )


def replay_bls_employment_situation_archive(
    manifest: BlsEmploymentSituationArchiveManifestV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[BlsEmploymentSituationReleaseV1, ...]:
    """Recompute and verify every normalized publication package."""
    if not isinstance(manifest, BlsEmploymentSituationArchiveManifestV1):
        raise TypeError(
            "Employment Situation replay requires a v1 archive manifest"
        )
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI} | {
        item.artifact_uri for item in manifest.entries
    }
    if set(by_uri) != expected_uris:
        raise ValueError(
            "Employment Situation replay snapshots differ from the manifest"
        )
    releases: list[BlsEmploymentSituationReleaseV1] = []
    for indexed, expected in zip(
        manifest.release_index.releases, manifest.entries
    ):
        current = by_uri[expected.artifact_uri]
        previous = by_uri[expected.previous_artifact_uri]
        release = parse_bls_employment_situation_release(
            current,
            reference_period=expected.reference_period,
            previous_snapshot=previous,
            previous_reference_period=expected.previous_reference_period,
        )
        if _entry_from_release(indexed, current, previous, release) != expected:
            raise ValueError(
                f"Employment Situation replay differs for {expected.reference_period}"
            )
        releases.append(release)
    return tuple(releases)


def bls_employment_situation_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: BlsEmploymentSituationArchiveManifestV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete coverage only from verified manifest evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError(
            "Employment Situation coverage requires a v1 U.S. profile"
        )
    if not isinstance(manifest, BlsEmploymentSituationArchiveManifestV1):
        raise TypeError(
            "Employment Situation coverage requires a v1 archive manifest"
        )
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError(
            "Employment Situation manifest differs from the U.S. profile"
        )
    program = profile.by_key[BLS_EMPLOYMENT_SITUATION_PROGRAM_KEY]
    end = _iso_date(window_end_date, "window_end_date")
    if end < max(item.release_date for item in manifest.entries):
        raise ValueError(
            "Employment Situation coverage end precedes the latest release"
        )
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
                "Distinct revisions: "
                f"CPS unemployment {manifest.unemployment_revision_count}, "
                "CES nonfarm payroll "
                f"{manifest.nonfarm_payroll_revision_count}, CES earnings "
                f"{manifest.earnings_revision_count}."
            ),
            (
                "The January 2010 production/nonsupervisory to all-employees "
                "earnings transition is explicitly noncomparable."
            ),
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError(
            "Employment Situation manifest does not qualify complete coverage"
        )
    return coverage


def packaged_bls_employment_situation_index_path() -> Path:
    """Return the packaged base64 envelope for the retained archive index."""
    return (
        Path(__file__).with_name("assets")
        / "us_employment_situation_release_index_v1.html.b64"
    )


def packaged_bls_employment_situation_archive_manifest_path() -> Path:
    """Return the packaged compact archive-manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "us_employment_situation_archive_v1.json"
    )


def load_packaged_bls_employment_situation_archive_manifest() -> (
    BlsEmploymentSituationArchiveManifestV1
):
    """Load and validate the packaged complete as-of manifest."""
    path = packaged_bls_employment_situation_archive_manifest_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            "packaged Employment Situation archive manifest is unavailable"
        ) from exc
    return BlsEmploymentSituationArchiveManifestV1.from_json(text)


def load_packaged_bls_employment_situation_index() -> bytes:
    """Decode and hash-check the exact archive index used by the manifest."""
    path = packaged_bls_employment_situation_index_path()
    try:
        content = base64.b64decode(
            path.read_text(encoding="ascii").strip(), validate=True
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError(
            "packaged Employment Situation index is unavailable or invalid"
        ) from exc
    manifest = load_packaged_bls_employment_situation_archive_manifest()
    if (
        len(content) != manifest.release_index.content_length
        or hashlib.sha256(content).hexdigest()
        != manifest.release_index.content_sha256
    ):
        raise ValueError(
            "packaged Employment Situation index differs from its manifest"
        )
    return content


__all__ = [
    "BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS",
    "BLS_EMPLOYMENT_MEASURE_KEYS",
    "BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE",
    "BLS_EMPLOYMENT_SITUATION_ALL_EMPLOYEES_START_REFERENCE_PERIOD",
    "BLS_EMPLOYMENT_SITUATION_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "BLS_EMPLOYMENT_SITUATION_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "BLS_EMPLOYMENT_SITUATION_INDEX_ENTRY_SCHEMA_VERSION",
    "BLS_EMPLOYMENT_SITUATION_INDEX_SCHEMA_VERSION",
    "BLS_EMPLOYMENT_SITUATION_INDEX_URI",
    "BLS_EMPLOYMENT_SITUATION_MALFORMED_ZONE_REFERENCE_PERIOD",
    "BLS_EMPLOYMENT_SITUATION_MEASURE_SCHEMA_VERSION",
    "BLS_EMPLOYMENT_SITUATION_PREDECESSOR_RELEASE_DATE",
    "BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI",
    "BLS_EMPLOYMENT_SITUATION_PROGRAM_KEY",
    "BLS_EMPLOYMENT_SITUATION_RELEASE_SCHEMA_VERSION",
    "BLS_EMPLOYMENT_SITUATION_SOURCE_KEY",
    "BLS_EMPLOYMENT_UNEMPLOYMENT_RATE",
    "BlsEmploymentSituationArchiveEntryV1",
    "BlsEmploymentSituationArchiveManifestV1",
    "BlsEmploymentSituationMeasureV1",
    "BlsEmploymentSituationReleaseIndexEntryV1",
    "BlsEmploymentSituationReleaseIndexV1",
    "BlsEmploymentSituationReleaseV1",
    "bls_employment_situation_coverage_from_manifest",
    "build_bls_employment_situation_archive_manifest",
    "build_bls_employment_situation_archive_requests",
    "build_bls_employment_situation_index_request",
    "load_packaged_bls_employment_situation_archive_manifest",
    "load_packaged_bls_employment_situation_index",
    "packaged_bls_employment_situation_archive_manifest_path",
    "packaged_bls_employment_situation_index_path",
    "parse_bls_employment_situation_release",
    "parse_bls_employment_situation_release_index",
    "replay_bls_employment_situation_archive",
]

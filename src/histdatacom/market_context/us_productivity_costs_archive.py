"""Complete BLS Productivity and Costs archive qualification for issue #538.

The archive keeps preliminary and revised quarterly publications as distinct
events.  For each event it binds the two selected nonfarm-business measures
to both the immediately preceding artifact and the comparison table printed
in the current release.  This distinction matters because preliminary
releases carry a third estimate for the preceding quarter, revised releases
carry the second estimate for the current quarter, and one shutdown-era
preliminary release published the headline measures as unavailable.
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicReleaseStage,
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
)
from histdatacom.runtime_contracts import JSONValue

BLS_PRODUCTIVITY_COSTS_INDEX_SCHEMA_VERSION = (
    "histdatacom.bls-productivity-costs-index.v1"
)
BLS_PRODUCTIVITY_COSTS_INDEX_ENTRY_SCHEMA_VERSION = (
    "histdatacom.bls-productivity-costs-index-entry.v1"
)
BLS_PRODUCTIVITY_COSTS_MEASURE_SCHEMA_VERSION = (
    "histdatacom.bls-productivity-costs-measure.v1"
)
BLS_PRODUCTIVITY_COSTS_ARCHIVE_ENTRY_SCHEMA_VERSION = (
    "histdatacom.bls-productivity-costs-archive-entry.v1"
)
BLS_PRODUCTIVITY_COSTS_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.bls-productivity-costs-archive-manifest.v1"
)

BLS_PRODUCTIVITY_COSTS_SOURCE_KEY = "us.bls.productivity-costs"
BLS_PRODUCTIVITY_COSTS_PROGRAM_KEY = "us.bls.productivity-costs"
BLS_PRODUCTIVITY_COSTS_INDEX_URI = (
    "https://www.bls.gov/bls/news-release/prod.htm"
)
BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI = (
    "https://www.bls.gov/news.release/history/prod2_12071999.txt"
)
BLS_PRODUCTIVITY_COSTS_UNAVAILABLE_URI = (
    "https://www.bls.gov/news.release/archives/prod2_02062019.htm"
)
BLS_PRODUCTIVITY_COSTS_CORRECTION_URI = (
    "https://www.bls.gov/news.release/archives/prod2_05022024.htm"
)

BLS_PRODUCTIVITY = "labor-productivity"
BLS_UNIT_LABOR_COSTS = "unit-labor-costs"
BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS = (
    BLS_PRODUCTIVITY,
    BLS_UNIT_LABOR_COSTS,
)

MAX_BLS_PRODUCTIVITY_COSTS_RELEASES = 512
MAX_BLS_PRODUCTIVITY_COSTS_INDEX_BYTES = 4 * 1024 * 1024
MAX_BLS_PRODUCTIVITY_COSTS_RELEASE_BYTES = 16 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_QUARTER_RE = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>[1-4])$")
_PRODUCTIVITY_URI_RE = re.compile(
    r"^https://www\.bls\.gov/news\.release/(?P<section>archives|history)/"
    r"prod2_(?P<date>\d{6}|\d{8})\.(?P<suffix>htm|txt)$"
)
_PRODUCTIVITY_ARTIFACT_LINK_RE = re.compile(
    r"/news\.release/(?:archives|history)/prod2_\d{6,8}\.(?:htm|txt)$",
    re.IGNORECASE,
)
_INDEX_LABEL_RE = re.compile(
    r"(?P<year>\d{4})\s+"
    r"(?P<quarter>First|Second|Third|Fourth)[ -]Quarter"
    r"(?:\s+and\s+Annual\s+Averages)?\s+"
    r"\((?P<stage>Preliminary|Revised)\)\s+"
    r"Productivity\s+and\s+Costs",
    re.IGNORECASE,
)
_TABLE_HEADING_RE = re.compile(r"^\s*Table [A-Z](?:\d)?\.", re.IGNORECASE)
_MAIN_TABLE_RE = re.compile(r"^\s*Table A(?:1)?\.")
_TABLE_IDENTITY_RE = re.compile(
    r"(?P<stage>Preliminary|Revised).*?"
    r"(?P<quarter>First|Second|Third|Fourth)[ -]Quarter\s+"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)
_VALUE_TOKEN_RE = re.compile(
    r"N\.A\.|(?<![A-Za-z0-9])[-+]?"
    r"(?:\d[\d,]*(?:\.\d+)?|\.\d+)(?:[pPrRcC])?(?![A-Za-z])",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(
    r"^(?P<number>[-+]?(?:\d[\d,]*(?:\.\d+)?|\.\d+))" r"(?P<suffix>[pPrRcC])?$"
)
_MONTH_RE = re.compile(
    r"\b(?P<month>JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|"
    r"MAY|JUN(?:E)?|JUL(?:Y)?|AUG(?:UST)?|SEP(?:T(?:EMBER)?)?|"
    r"OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?)\.?\s+"
    r"(?P<day>\d{1,2}),\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)
_MONTH_NUMBERS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_QUARTER_NUMBERS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
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


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _nonnegative_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= maximum
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


def _quarter(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _QUARTER_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-Qn")
    return result


def _quarter_ordinal(value: str) -> int:
    match = _QUARTER_RE.fullmatch(value)
    if match is None:
        raise ValueError("quarter is invalid")
    return int(match.group("year")) * 4 + int(match.group("quarter")) - 1


def _previous_quarter(value: str) -> str:
    ordinal = _quarter_ordinal(value) - 1
    year, zero_based_quarter = divmod(ordinal, 4)
    return f"{year:04d}-Q{zero_based_quarter + 1}"


def _release_date_from_uri(uri: str) -> str:
    match = _PRODUCTIVITY_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("Productivity and Costs artifact URI is invalid")
    compact = match.group("date")
    try:
        parsed = (
            datetime.strptime(
                compact, "%m%d%Y" if len(compact) == 8 else "%m%d%y"
            )
            .replace(tzinfo=timezone.utc)
            .date()
        )
    except ValueError as exc:
        raise ValueError(
            "Productivity and Costs artifact URI contains an invalid date"
        ) from exc
    return parsed.isoformat()


def _source_format_for_uri(uri: str) -> OfficialSourceFormat:
    match = _PRODUCTIVITY_URI_RE.fullmatch(uri)
    if match is None:
        raise ValueError("Productivity and Costs artifact URI is invalid")
    return {
        "htm": OfficialSourceFormat.HTML,
        "txt": OfficialSourceFormat.TEXT,
    }[match.group("suffix")]


def _published_value(value: object, name: str) -> tuple[str, float | None]:
    lexical = "".join(_required_text(value, name).split())
    if lexical.upper() == "N.A.":
        return "N.A.", None
    match = _NUMBER_RE.fullmatch(lexical)
    if match is None:
        raise ValueError(f"{name} is not a Productivity and Costs value")
    normalized = match.group("number").replace(",", "")
    if normalized.startswith(("-.", "+.")):
        normalized = normalized[:1] + "0" + normalized[1:]
    elif normalized.startswith("."):
        normalized = "0" + normalized
    return lexical, _finite(normalized, name)


def _optional_float(value: object, name: str) -> float | None:
    if value is None:
        return None
    return _finite(value, name)


def _value_pair(
    lexical: object, numeric: object, name: str
) -> tuple[str, float | None]:
    normalized_lexical, parsed = _published_value(lexical, name)
    normalized_numeric = _optional_float(numeric, f"{name}_value")
    if parsed is None:
        if normalized_numeric is not None:
            raise ValueError(f"{name} unavailable lexical has a value")
    elif normalized_numeric is None or not math.isclose(
        parsed, normalized_numeric, rel_tol=0.0, abs_tol=1e-12
    ):
        raise ValueError(f"{name} differs from its numeric value")
    return normalized_lexical, normalized_numeric


class _ProductivityReleaseListParser(HTMLParser):
    """Collect list-item text and links without trusting page presentation."""

    def __init__(self) -> None:
        super().__init__()
        self._in_list_item = False
        self._text: list[str] = []
        self._links: list[str] = []
        self.rows: list[tuple[str, tuple[str, ...]]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() == "li":
            self._in_list_item = True
            self._text = []
            self._links = []
        elif self._in_list_item and tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self._links.append(href)

    def handle_data(self, data: str) -> None:
        if self._in_list_item:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "li" and self._in_list_item:
            text = " ".join("".join(self._text).split())
            self.rows.append((text, tuple(self._links)))
            self._in_list_item = False


@dataclass(frozen=True, slots=True)
class BlsProductivityCostsReleaseIndexEntryV1:
    """One stage-specific row from the official release archive index."""

    reference_period: str
    release_stage: EconomicReleaseStage
    release_date: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    label: str
    entry_id: str = ""
    schema_version: str = BLS_PRODUCTIVITY_COSTS_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_PRODUCTIVITY_COSTS_INDEX_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Productivity and Costs index-entry schema"
            )
        reference = _quarter(self.reference_period, "reference_period")
        stage = EconomicReleaseStage.from_value(self.release_stage)
        if stage not in {
            EconomicReleaseStage.PRELIMINARY,
            EconomicReleaseStage.REVISION,
        }:
            raise ValueError("Productivity and Costs release stage is invalid")
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if release != _release_date_from_uri(artifact):
            raise ValueError("index release date differs from artifact URI")
        if source_format is not _source_format_for_uri(artifact):
            raise ValueError("index source format differs from artifact URI")
        label = _required_text(self.label, "label")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_stage", stage)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "label", label)
        expected = _stable_id(
            "bls-productivity-costs-index-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Productivity and Costs index-entry identity differs"
            )
        object.__setattr__(self, "entry_id", expected)

    @property
    def stage_key(self) -> str:
        return f"{self.reference_period}:{self.release_stage.value}"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_stage": self.release_stage.value,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "label": self.label,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsProductivityCostsReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_stage=EconomicReleaseStage.from_value(
                str(data.get("release_stage", ""))
            ),
            release_date=str(data.get("release_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            label=str(data.get("label", "")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsProductivityCostsReleaseIndexV1:
    """Bounded post-2000 inventory plus the required 1999 predecessor."""

    index_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    predecessor: BlsProductivityCostsReleaseIndexEntryV1
    releases: tuple[BlsProductivityCostsReleaseIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = BLS_PRODUCTIVITY_COSTS_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_PRODUCTIVITY_COSTS_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported Productivity and Costs index schema")
        index_uri = _https_uri(self.index_uri, "index_uri")
        if index_uri != BLS_PRODUCTIVITY_COSTS_INDEX_URI:
            raise ValueError("Productivity and Costs index URI differs")
        content_sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_PRODUCTIVITY_COSTS_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        predecessor = self.predecessor
        if not isinstance(predecessor, BlsProductivityCostsReleaseIndexEntryV1):
            raise TypeError("Productivity and Costs predecessor is invalid")
        if (
            predecessor.reference_period != "1999-Q3"
            or predecessor.release_stage is not EconomicReleaseStage.REVISION
            or predecessor.artifact_uri
            != BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI
        ):
            raise ValueError("Productivity and Costs predecessor differs")
        releases = tuple(self.releases)
        if (
            not releases
            or len(releases) > MAX_BLS_PRODUCTIVITY_COSTS_RELEASES
            or any(
                not isinstance(item, BlsProductivityCostsReleaseIndexEntryV1)
                for item in releases
            )
        ):
            raise TypeError("Productivity and Costs release index is invalid")
        if releases[0].reference_period != "1999-Q4":
            raise ValueError(
                "Productivity and Costs index omits its 2000 boundary"
            )
        dates = tuple(item.release_date for item in releases)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError(
                "Productivity and Costs index is not chronological"
            )
        if dates[-1] > as_of:
            raise ValueError(
                "Productivity and Costs index exceeds its as-of date"
            )
        if len({item.artifact_uri for item in releases}) != len(releases):
            raise ValueError("Productivity and Costs index repeats an artifact")
        grouped: dict[str, list[EconomicReleaseStage]] = {}
        for item in releases:
            grouped.setdefault(item.reference_period, []).append(
                item.release_stage
            )
        references = tuple(grouped)
        ordinals = tuple(_quarter_ordinal(item) for item in references)
        if any(
            current != previous + 1 for previous, current in pairwise(ordinals)
        ):
            raise ValueError("Productivity and Costs index has a quarterly gap")
        expected_pair = [
            EconomicReleaseStage.PRELIMINARY,
            EconomicReleaseStage.REVISION,
        ]
        for index, reference in enumerate(references):
            stages = grouped[reference]
            is_trailing = index == len(references) - 1
            if stages != expected_pair and not (
                is_trailing and stages == [EconomicReleaseStage.PRELIMINARY]
            ):
                raise ValueError(
                    "Productivity and Costs stage pairing is incomplete"
                )
        object.__setattr__(self, "index_uri", index_uri)
        object.__setattr__(self, "content_sha256", content_sha)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", releases)
        expected = _stable_id(
            "bls-productivity-costs-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected:
            raise ValueError("Productivity and Costs index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def artifacts(
        self,
    ) -> tuple[BlsProductivityCostsReleaseIndexEntryV1, ...]:
        return (self.predecessor, *self.releases)

    @property
    def by_stage_key(
        self,
    ) -> Mapping[str, BlsProductivityCostsReleaseIndexEntryV1]:
        return MappingProxyType(
            {item.stage_key: item for item in self.releases}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_uri": self.index_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "as_of_date": self.as_of_date,
            "predecessor": self.predecessor.to_dict(),
            "releases": [item.to_dict() for item in self.releases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsProductivityCostsReleaseIndexV1:
        return cls(
            index_uri=str(data.get("index_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            predecessor=BlsProductivityCostsReleaseIndexEntryV1.from_dict(
                _mapping(data.get("predecessor"), "predecessor")
            ),
            releases=tuple(
                BlsProductivityCostsReleaseIndexEntryV1.from_dict(
                    _mapping(item, "Productivity and Costs index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsProductivityCostsMeasureV1:
    """One nonfarm-business measure and its publication-time revision."""

    measure_key: str
    reference_period: str
    comparison_reference_period: str
    release_stage: EconomicReleaseStage
    actual_lexical: str
    actual_value: float | None
    previous_as_known_lexical: str
    previous_as_known_value: float | None
    source_comparison_previous_lexical: str
    source_comparison_previous_value: float | None
    revised_comparison_lexical: str
    revised_comparison_value: float | None
    revision_value: float | None
    predecessor_lineage_comparable: bool
    series_lineage: str
    unit: str
    transformation: str
    seasonality: str
    content_sha256: str
    actual_locator: str
    previous_locator: str
    revision_locator: str
    limitations: tuple[str, ...]
    measure_id: str = ""
    schema_version: str = BLS_PRODUCTIVITY_COSTS_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BLS_PRODUCTIVITY_COSTS_MEASURE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported Productivity and Costs measure schema"
            )
        measure = _required_text(self.measure_key, "measure_key")
        if measure not in BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS:
            raise ValueError("Productivity and Costs measure key is invalid")
        reference = _quarter(self.reference_period, "reference_period")
        comparison = _quarter(
            self.comparison_reference_period, "comparison_reference_period"
        )
        stage = EconomicReleaseStage.from_value(self.release_stage)
        expected_comparison = (
            _previous_quarter(reference)
            if stage is EconomicReleaseStage.PRELIMINARY
            else reference
        )
        if comparison != expected_comparison:
            raise ValueError("measure comparison period differs from stage")
        pairs = (
            ("actual_lexical", "actual_value"),
            ("previous_as_known_lexical", "previous_as_known_value"),
            (
                "source_comparison_previous_lexical",
                "source_comparison_previous_value",
            ),
            ("revised_comparison_lexical", "revised_comparison_value"),
        )
        for lexical_name, numeric_name in pairs:
            lexical, numeric = _value_pair(
                getattr(self, lexical_name),
                getattr(self, numeric_name),
                lexical_name,
            )
            object.__setattr__(self, lexical_name, lexical)
            object.__setattr__(self, numeric_name, numeric)
        revision = _optional_float(self.revision_value, "revision_value")
        current = self.revised_comparison_value
        previous = self.source_comparison_previous_value
        expected_revision = (
            None if current is None or previous is None else current - previous
        )
        if expected_revision is None:
            if revision is not None:
                raise ValueError(
                    "unavailable comparison cannot have a revision"
                )
        elif revision is None or not math.isclose(
            revision, expected_revision, rel_tol=0.0, abs_tol=1e-12
        ):
            raise ValueError("revision value differs from source comparison")
        if stage is EconomicReleaseStage.REVISION and (
            self.actual_value != self.revised_comparison_value
            or self.actual_lexical != self.revised_comparison_lexical
        ):
            raise ValueError(
                "revised-stage actual differs from comparison table"
            )
        if not isinstance(self.predecessor_lineage_comparable, bool):
            raise TypeError("predecessor_lineage_comparable must be boolean")
        expected_lineage_comparable = (
            self.previous_as_known_value is not None
            and self.source_comparison_previous_value is not None
            and math.isclose(
                self.previous_as_known_value,
                self.source_comparison_previous_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        )
        if (
            self.predecessor_lineage_comparable
            is not expected_lineage_comparable
        ):
            raise ValueError("measure predecessor comparability differs")
        for name in (
            "series_lineage",
            "unit",
            "transformation",
            "seasonality",
            "actual_locator",
            "previous_locator",
            "revision_locator",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        expected_definition = {
            BLS_PRODUCTIVITY: (
                "nonfarm-business-sector-labor-productivity",
                "percent",
            ),
            BLS_UNIT_LABOR_COSTS: (
                "nonfarm-business-sector-unit-labor-costs",
                "percent",
            ),
        }[measure]
        if (
            (self.series_lineage, self.unit) != expected_definition
            or self.transformation
            != "quarter-over-quarter-percent-change-at-annual-rate"
            or self.seasonality != "seasonally-adjusted"
        ):
            raise ValueError(
                "Productivity and Costs measure definition differs"
            )
        content_sha = _sha256(self.content_sha256, "content_sha256")
        limitations = tuple(
            sorted(
                {
                    _required_text(item, "limitations")
                    for item in self.limitations
                }
            )
        )
        if not limitations:
            raise ValueError("Productivity and Costs measure needs limitations")
        object.__setattr__(self, "measure_key", measure)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "comparison_reference_period", comparison)
        object.__setattr__(self, "release_stage", stage)
        object.__setattr__(self, "revision_value", revision)
        object.__setattr__(self, "content_sha256", content_sha)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "bls-productivity-costs-measure", self.identity_payload()
        )
        if self.measure_id and self.measure_id != expected:
            raise ValueError("Productivity and Costs measure identity differs")
        object.__setattr__(self, "measure_id", expected)

    @property
    def revision_was_observed(self) -> bool:
        return self.revision_value is not None

    @property
    def value_was_revised(self) -> bool:
        return self.revision_value is not None and not math.isclose(
            self.revision_value, 0.0, rel_tol=0.0, abs_tol=1e-12
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "measure_key": self.measure_key,
            "reference_period": self.reference_period,
            "comparison_reference_period": self.comparison_reference_period,
            "release_stage": self.release_stage.value,
            "actual_lexical": self.actual_lexical,
            "actual_value": self.actual_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "source_comparison_previous_lexical": (
                self.source_comparison_previous_lexical
            ),
            "source_comparison_previous_value": (
                self.source_comparison_previous_value
            ),
            "revised_comparison_lexical": self.revised_comparison_lexical,
            "revised_comparison_value": self.revised_comparison_value,
            "revision_value": self.revision_value,
            "predecessor_lineage_comparable": (
                self.predecessor_lineage_comparable
            ),
            "series_lineage": self.series_lineage,
            "unit": self.unit,
            "transformation": self.transformation,
            "seasonality": self.seasonality,
            "content_sha256": self.content_sha256,
            "actual_locator": self.actual_locator,
            "previous_locator": self.previous_locator,
            "revision_locator": self.revision_locator,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "measure_id": self.measure_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsProductivityCostsMeasureV1:
        return cls(
            measure_key=str(data.get("measure_key", "")),
            reference_period=str(data.get("reference_period", "")),
            comparison_reference_period=str(
                data.get("comparison_reference_period", "")
            ),
            release_stage=EconomicReleaseStage.from_value(
                str(data.get("release_stage", ""))
            ),
            actual_lexical=str(data.get("actual_lexical", "")),
            actual_value=cast(float | None, data.get("actual_value")),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            previous_as_known_value=cast(
                float | None, data.get("previous_as_known_value")
            ),
            source_comparison_previous_lexical=str(
                data.get("source_comparison_previous_lexical", "")
            ),
            source_comparison_previous_value=cast(
                float | None, data.get("source_comparison_previous_value")
            ),
            revised_comparison_lexical=str(
                data.get("revised_comparison_lexical", "")
            ),
            revised_comparison_value=cast(
                float | None, data.get("revised_comparison_value")
            ),
            revision_value=cast(float | None, data.get("revision_value")),
            predecessor_lineage_comparable=cast(
                bool, data.get("predecessor_lineage_comparable")
            ),
            series_lineage=str(data.get("series_lineage", "")),
            unit=str(data.get("unit", "")),
            transformation=str(data.get("transformation", "")),
            seasonality=str(data.get("seasonality", "")),
            content_sha256=str(data.get("content_sha256", "")),
            actual_locator=str(data.get("actual_locator", "")),
            previous_locator=str(data.get("previous_locator", "")),
            revision_locator=str(data.get("revision_locator", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            measure_id=str(data.get("measure_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsProductivityCostsArchiveEntryV1:
    """One stage-specific publication and its predecessor evidence."""

    reference_period: str
    comparison_reference_period: str
    release_stage: EconomicReleaseStage
    release_date: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    previous_artifact_uri: str
    previous_content_sha256: str
    previous_content_length: int
    released_lexical: str
    release_header_lexical: str
    reported_zone: str
    source_era: str
    table_layout: str
    historical_revision_notice: bool
    source_correction_notice: bool
    actual_available: bool
    release_time_locator: str
    measures: tuple[BlsProductivityCostsMeasureV1, ...]
    entry_id: str = ""
    schema_version: str = BLS_PRODUCTIVITY_COSTS_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_PRODUCTIVITY_COSTS_ARCHIVE_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Productivity and Costs archive-entry schema"
            )
        reference = _quarter(self.reference_period, "reference_period")
        comparison = _quarter(
            self.comparison_reference_period, "comparison_reference_period"
        )
        stage = EconomicReleaseStage.from_value(self.release_stage)
        expected_comparison = (
            _previous_quarter(reference)
            if stage is EconomicReleaseStage.PRELIMINARY
            else reference
        )
        if comparison != expected_comparison:
            raise ValueError(
                "archive-entry comparison period differs from stage"
            )
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        previous_artifact = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if release != _release_date_from_uri(artifact):
            raise ValueError("archive-entry date differs from artifact URI")
        if source_format is not _source_format_for_uri(artifact):
            raise ValueError("archive-entry format differs from artifact URI")
        for name in ("content_sha256", "previous_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BLS_PRODUCTIVITY_COSTS_RELEASE_BYTES,
        )
        _positive_int(
            self.previous_content_length,
            "previous_content_length",
            MAX_BLS_PRODUCTIVITY_COSTS_RELEASE_BYTES,
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T08:30:00":
            raise ValueError("archive-entry released time differs")
        header = _required_text(
            self.release_header_lexical, "release_header_lexical"
        )
        zone = _required_text(self.reported_zone, "reported_zone")
        if zone not in {"EST", "EDT", "ET"}:
            raise ValueError("archive-entry reported zone is invalid")
        era = _required_text(self.source_era, "source_era")
        expected_era = (
            "fixed-width-text"
            if source_format is OfficialSourceFormat.TEXT
            else "preformatted-html"
        )
        if era != expected_era:
            raise ValueError("archive-entry source era differs")
        layout = _required_text(self.table_layout, "table_layout")
        if layout not in {"sector-row", "measure-row"}:
            raise ValueError("archive-entry table layout is invalid")
        for name in (
            "historical_revision_notice",
            "source_correction_notice",
            "actual_available",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        if self.source_correction_notice is not (
            artifact == BLS_PRODUCTIVITY_COSTS_CORRECTION_URI
        ):
            raise ValueError("archive-entry correction status differs")
        locator = _required_text(
            self.release_time_locator, "release_time_locator"
        )
        measures = tuple(self.measures)
        if (
            len(measures) != len(BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS)
            or any(
                not isinstance(item, BlsProductivityCostsMeasureV1)
                for item in measures
            )
            or tuple(item.measure_key for item in measures)
            != BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS
        ):
            raise ValueError("archive-entry measure package is invalid")
        for measure in measures:
            if (
                measure.reference_period != reference
                or measure.comparison_reference_period != comparison
                or measure.release_stage is not stage
                or measure.content_sha256 != self.content_sha256
            ):
                raise ValueError("archive-entry measure evidence differs")
        available = all(item.actual_value is not None for item in measures)
        if self.actual_available is not available:
            raise ValueError("archive-entry availability differs")
        if not available and artifact != BLS_PRODUCTIVITY_COSTS_UNAVAILABLE_URI:
            raise ValueError(
                "unexpected unavailable Productivity and Costs actual"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "comparison_reference_period", comparison)
        object.__setattr__(self, "release_stage", stage)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "previous_artifact_uri", previous_artifact)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "release_header_lexical", header)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "source_era", era)
        object.__setattr__(self, "table_layout", layout)
        object.__setattr__(self, "release_time_locator", locator)
        object.__setattr__(self, "measures", measures)
        expected = _stable_id(
            "bls-productivity-costs-archive-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Productivity and Costs archive-entry identity differs"
            )
        object.__setattr__(self, "entry_id", expected)

    @property
    def stage_key(self) -> str:
        return f"{self.reference_period}:{self.release_stage.value}"

    @property
    def by_measure(self) -> Mapping[str, BlsProductivityCostsMeasureV1]:
        return MappingProxyType(
            {item.measure_key: item for item in self.measures}
        )

    @property
    def revision_was_observed(self) -> bool:
        return all(item.revision_was_observed for item in self.measures)

    @property
    def value_was_revised(self) -> bool:
        return any(item.value_was_revised for item in self.measures)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "comparison_reference_period": self.comparison_reference_period,
            "release_stage": self.release_stage.value,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "previous_artifact_uri": self.previous_artifact_uri,
            "previous_content_sha256": self.previous_content_sha256,
            "previous_content_length": self.previous_content_length,
            "released_lexical": self.released_lexical,
            "release_header_lexical": self.release_header_lexical,
            "reported_zone": self.reported_zone,
            "source_era": self.source_era,
            "table_layout": self.table_layout,
            "historical_revision_notice": self.historical_revision_notice,
            "source_correction_notice": self.source_correction_notice,
            "actual_available": self.actual_available,
            "release_time_locator": self.release_time_locator,
            "measures": [item.to_dict() for item in self.measures],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsProductivityCostsArchiveEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            comparison_reference_period=str(
                data.get("comparison_reference_period", "")
            ),
            release_stage=EconomicReleaseStage.from_value(
                str(data.get("release_stage", ""))
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
            release_header_lexical=str(data.get("release_header_lexical", "")),
            reported_zone=str(data.get("reported_zone", "")),
            source_era=str(data.get("source_era", "")),
            table_layout=str(data.get("table_layout", "")),
            historical_revision_notice=cast(
                bool, data.get("historical_revision_notice")
            ),
            source_correction_notice=cast(
                bool, data.get("source_correction_notice")
            ),
            actual_available=cast(bool, data.get("actual_available")),
            release_time_locator=str(data.get("release_time_locator", "")),
            measures=tuple(
                BlsProductivityCostsMeasureV1.from_dict(
                    _mapping(item, "Productivity and Costs measure")
                )
                for item in _sequence(data.get("measures"), "measures")
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BlsProductivityCostsArchiveManifestV1:
    """Compact replay evidence for every post-2000 release stage."""

    registry_id: str
    profile_id: str
    release_index: BlsProductivityCostsReleaseIndexV1
    entries: tuple[BlsProductivityCostsArchiveEntryV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    available_release_count: int
    unavailable_release_count: int
    revision_observation_count: int
    revision_occurrence_count: int
    productivity_revision_count: int
    unit_labor_cost_revision_count: int
    noncomparable_lineage_count: int
    historical_revision_notice_count: int
    manifest_id: str = ""
    schema_version: str = BLS_PRODUCTIVITY_COSTS_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BLS_PRODUCTIVITY_COSTS_ARCHIVE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Productivity and Costs manifest schema"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("manifest registry identity is invalid")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("manifest profile identity is invalid")
        if not isinstance(
            self.release_index, BlsProductivityCostsReleaseIndexV1
        ):
            raise TypeError("manifest requires a Productivity and Costs index")
        entries = tuple(self.entries)
        if len(entries) != len(self.release_index.releases) or any(
            not isinstance(item, BlsProductivityCostsArchiveEntryV1)
            for item in entries
        ):
            raise TypeError("manifest entries are invalid")
        for indexed, entry in zip(self.release_index.releases, entries):
            if (
                entry.stage_key != indexed.stage_key
                or entry.release_date != indexed.release_date
                or entry.artifact_uri != indexed.artifact_uri
                or entry.source_format is not indexed.source_format
            ):
                raise ValueError("manifest entry differs from release index")
        predecessor = self.release_index.predecessor
        first = entries[0]
        if first.previous_artifact_uri != predecessor.artifact_uri:
            raise ValueError("manifest omits the 1999 predecessor")
        for previous, current in pairwise(entries):
            if (
                current.previous_artifact_uri != previous.artifact_uri
                or current.previous_content_sha256 != previous.content_sha256
                or current.previous_content_length != previous.content_length
            ):
                raise ValueError("manifest predecessor chain is broken")
        artifacts = {
            (item.artifact_uri, item.content_sha256, item.content_length)
            for item in entries
        }
        artifacts.add(
            (
                first.previous_artifact_uri,
                first.previous_content_sha256,
                first.previous_content_length,
            )
        )
        measure_counts = {
            measure: sum(
                item.by_measure[measure].value_was_revised for item in entries
            )
            for measure in BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS
        }
        expected_counts = {
            "raw_artifact_count": len(artifacts),
            "total_content_bytes": sum(item[2] for item in artifacts),
            "available_release_count": sum(
                item.actual_available for item in entries
            ),
            "unavailable_release_count": sum(
                not item.actual_available for item in entries
            ),
            "revision_observation_count": sum(
                measure.revision_was_observed
                for item in entries
                for measure in item.measures
            ),
            "revision_occurrence_count": sum(
                item.value_was_revised for item in entries
            ),
            "productivity_revision_count": measure_counts[BLS_PRODUCTIVITY],
            "unit_labor_cost_revision_count": measure_counts[
                BLS_UNIT_LABOR_COSTS
            ],
            "noncomparable_lineage_count": sum(
                not measure.predecessor_lineage_comparable
                for item in entries
                for measure in item.measures
            ),
            "historical_revision_notice_count": sum(
                item.historical_revision_notice for item in entries
            ),
        }
        for name, expected_count in expected_counts.items():
            maximum = (
                MAX_BLS_PRODUCTIVITY_COSTS_RELEASES
                * MAX_BLS_PRODUCTIVITY_COSTS_RELEASE_BYTES
                if name == "total_content_bytes"
                else MAX_BLS_PRODUCTIVITY_COSTS_RELEASES * 2
            )
            _nonnegative_int(getattr(self, name), name, maximum)
            if getattr(self, name) != expected_count:
                raise ValueError(f"manifest {name} differs")
        if len({item[0] for item in artifacts}) != len(artifacts):
            raise ValueError("manifest repeats an artifact URI")
        if len({item[1] for item in artifacts}) != len(artifacts):
            raise ValueError("manifest repeats an artifact hash")
        normalized = {
            measure.measure_id for item in entries for measure in item.measures
        }
        if len(normalized) != len(entries) * len(
            BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS
        ):
            raise ValueError("manifest repeats normalized measure evidence")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "bls-productivity-costs-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Productivity and Costs manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_stage_key(
        self,
    ) -> Mapping[str, BlsProductivityCostsArchiveEntryV1]:
        return MappingProxyType({item.stage_key: item for item in self.entries})

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "available_release_count": self.available_release_count,
            "unavailable_release_count": self.unavailable_release_count,
            "revision_observation_count": self.revision_observation_count,
            "revision_occurrence_count": self.revision_occurrence_count,
            "productivity_revision_count": self.productivity_revision_count,
            "unit_labor_cost_revision_count": (
                self.unit_labor_cost_revision_count
            ),
            "noncomparable_lineage_count": self.noncomparable_lineage_count,
            "historical_revision_notice_count": (
                self.historical_revision_notice_count
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BlsProductivityCostsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=BlsProductivityCostsReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            entries=tuple(
                BlsProductivityCostsArchiveEntryV1.from_dict(
                    _mapping(item, "Productivity and Costs archive entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            available_release_count=cast(
                int, data.get("available_release_count")
            ),
            unavailable_release_count=cast(
                int, data.get("unavailable_release_count")
            ),
            revision_observation_count=cast(
                int, data.get("revision_observation_count")
            ),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            productivity_revision_count=cast(
                int, data.get("productivity_revision_count")
            ),
            unit_labor_cost_revision_count=cast(
                int, data.get("unit_labor_cost_revision_count")
            ),
            noncomparable_lineage_count=cast(
                int, data.get("noncomparable_lineage_count")
            ),
            historical_revision_notice_count=cast(
                int, data.get("historical_revision_notice_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, payload: str) -> BlsProductivityCostsArchiveManifestV1:
        parsed = json.loads(payload)
        return cls.from_dict(_mapping(parsed, "manifest"))


@dataclass(frozen=True, slots=True)
class _MainObservation:
    reference_period: str
    release_stage: EconomicReleaseStage
    productivity_lexical: str
    unit_labor_costs_lexical: str
    table_layout: str
    productivity_locator: str
    unit_labor_costs_locator: str


@dataclass(frozen=True, slots=True)
class _ComparisonObservation:
    previous_productivity_lexical: str
    previous_unit_labor_costs_lexical: str
    revised_productivity_lexical: str
    revised_unit_labor_costs_lexical: str
    productivity_locator: str
    unit_labor_costs_locator: str


@dataclass(frozen=True, slots=True)
class _ReleaseTimeObservation:
    released_lexical: str
    header_lexical: str
    reported_zone: str
    locator: str


def _decode_release(snapshot: OfficialRawSnapshotV1) -> tuple[str, ...]:
    try:
        decoded = snapshot.content.decode("utf-8")
    except UnicodeDecodeError:
        decoded = snapshot.content.decode("windows-1252")
    return tuple(html.unescape(decoded).splitlines())


def _table_blocks(
    lines: Sequence[str],
) -> tuple[tuple[int, tuple[str, ...]], ...]:
    starts = [
        index
        for index, line in enumerate(lines)
        if _TABLE_HEADING_RE.match(line)
    ]
    result: list[tuple[int, tuple[str, ...]]] = []
    for position, start in enumerate(starts):
        end = (
            starts[position + 1]
            if position + 1 < len(starts)
            else min(len(lines), start + 200)
        )
        result.append((start, tuple(lines[start:end])))
    return tuple(result)


def _value_tokens(line: str) -> tuple[str, ...]:
    return tuple(match.group(0) for match in _VALUE_TOKEN_RE.finditer(line))


def _measure_rows(
    block: Sequence[str],
) -> tuple[tuple[int, str] | None, tuple[int, str] | None]:
    productivity = next(
        (
            (index, line)
            for index, line in enumerate(block[:80])
            if re.match(r"^\s*(?:Labor\s+)?Productivity\b", line, re.IGNORECASE)
            and len(_value_tokens(line)) >= 2
        ),
        None,
    )
    unit_labor_costs: tuple[int, str] | None = None
    for index, line in enumerate(block[:80]):
        candidate = line
        if re.match(
            r"^\s*Unit labor(?:\s+costs)?\s*$", line, re.IGNORECASE
        ) and index + 1 < len(block):
            candidate = f"{line} {block[index + 1]}"
        if (
            re.match(r"^\s*Unit labor(?:\s+costs)?\b", line, re.IGNORECASE)
            and len(_value_tokens(candidate)) >= 2
        ):
            unit_labor_costs = (index, candidate)
            break
    return productivity, unit_labor_costs


def _parse_table_identity(
    heading: Sequence[str],
) -> tuple[str, EconomicReleaseStage]:
    text = " ".join(heading[:3])
    match = _TABLE_IDENTITY_RE.search(text)
    if match is None:
        raise ValueError("main table does not identify quarter and stage")
    quarter = _QUARTER_NUMBERS[match.group("quarter").lower()]
    reference = f"{int(match.group('year')):04d}-Q{quarter}"
    stage = {
        "preliminary": EconomicReleaseStage.PRELIMINARY,
        "revised": EconomicReleaseStage.REVISION,
    }[match.group("stage").lower()]
    return reference, stage


def _parse_main_observation(lines: Sequence[str]) -> _MainObservation:
    for start, block in _table_blocks(lines):
        if not _MAIN_TABLE_RE.match(block[0]):
            continue
        try:
            reference, stage = _parse_table_identity(block)
        except ValueError:
            continue
        sector_row = next(
            (
                (index, line)
                for index, line in enumerate(block[:80])
                if re.match(r"^\s*Nonfarm\s+Business\b", line, re.IGNORECASE)
            ),
            None,
        )
        if sector_row is not None:
            tokens = _value_tokens(sector_row[1])
            if len(tokens) >= 6:
                locator = (
                    f"line {start + sector_row[0] + 1}: {sector_row[1].strip()}"
                )
                return _MainObservation(
                    reference_period=reference,
                    release_stage=stage,
                    productivity_lexical=tokens[0],
                    unit_labor_costs_lexical=tokens[5],
                    table_layout="sector-row",
                    productivity_locator=locator,
                    unit_labor_costs_locator=locator,
                )
        productivity, unit_labor_costs = _measure_rows(block)
        if productivity is not None and unit_labor_costs is not None:
            productivity_tokens = _value_tokens(productivity[1])
            unit_labor_cost_tokens = _value_tokens(unit_labor_costs[1])
            return _MainObservation(
                reference_period=reference,
                release_stage=stage,
                productivity_lexical=productivity_tokens[0],
                unit_labor_costs_lexical=unit_labor_cost_tokens[0],
                table_layout="measure-row",
                productivity_locator=(
                    f"line {start + productivity[0] + 1}: "
                    f"{productivity[1].strip()}"
                ),
                unit_labor_costs_locator=(
                    f"line {start + unit_labor_costs[0] + 1}: "
                    f"{unit_labor_costs[1].strip()}"
                ),
            )
    raise ValueError("Productivity and Costs main Table A is unavailable")


def _parse_comparison_observation(
    lines: Sequence[str],
) -> _ComparisonObservation:
    for start, block in _table_blocks(lines):
        heading = " ".join(block[:30])
        if (
            re.search(r"previous(?:ly published)?", heading, re.IGNORECASE)
            is None
        ):
            continue
        for index, line in enumerate(block[:80]):
            if re.match(r"^\s*Nonfarm\s+business\s*:", line, re.IGNORECASE):
                following = block[index + 1 : index + 5]
                previous = next(
                    (
                        item
                        for item in following
                        if re.match(
                            r"^\s*Previous(?:ly published)?\b",
                            item,
                            re.IGNORECASE,
                        )
                    ),
                    None,
                )
                revised = next(
                    (
                        item
                        for item in following
                        if re.match(
                            r"^\s*(?:Current|Revised)\b", item, re.IGNORECASE
                        )
                    ),
                    None,
                )
                if previous is not None and revised is not None:
                    previous_tokens = _value_tokens(previous)
                    revised_tokens = _value_tokens(revised)
                    locator = f"lines {start + index + 1}-{start + index + 3}"
                    return _ComparisonObservation(
                        previous_productivity_lexical=previous_tokens[0],
                        previous_unit_labor_costs_lexical=previous_tokens[-1],
                        revised_productivity_lexical=revised_tokens[0],
                        revised_unit_labor_costs_lexical=revised_tokens[-1],
                        productivity_locator=locator,
                        unit_labor_costs_locator=locator,
                    )
            if re.match(
                r"^\s*Nonfarm\s+business\s+(?:Revised|Current)\b",
                line,
                re.IGNORECASE,
            ):
                previous = next(
                    (
                        item
                        for item in block[index + 1 : index + 4]
                        if re.match(
                            r"^\s*Previously published\b",
                            item,
                            re.IGNORECASE,
                        )
                    ),
                    None,
                )
                if previous is not None:
                    previous_tokens = _value_tokens(previous)
                    revised_tokens = _value_tokens(line)
                    locator = f"lines {start + index + 1}-{start + index + 2}"
                    return _ComparisonObservation(
                        previous_productivity_lexical=previous_tokens[0],
                        previous_unit_labor_costs_lexical=previous_tokens[-1],
                        revised_productivity_lexical=revised_tokens[0],
                        revised_unit_labor_costs_lexical=revised_tokens[-1],
                        productivity_locator=locator,
                        unit_labor_costs_locator=locator,
                    )
        top = " ".join(block[:18])
        if (
            re.search(r"\bNonfarm\b", top, re.IGNORECASE)
            and re.search(r"\bSector\b", top, re.IGNORECASE)
            and re.search(r"(?:Revised|Current)\s+Previous", top, re.IGNORECASE)
        ):
            productivity, unit_labor_costs = _measure_rows(block)
            if productivity is not None and unit_labor_costs is not None:
                productivity_tokens = _value_tokens(productivity[1])
                unit_labor_cost_tokens = _value_tokens(unit_labor_costs[1])
                return _ComparisonObservation(
                    previous_productivity_lexical=productivity_tokens[1],
                    previous_unit_labor_costs_lexical=unit_labor_cost_tokens[1],
                    revised_productivity_lexical=productivity_tokens[0],
                    revised_unit_labor_costs_lexical=unit_labor_cost_tokens[0],
                    productivity_locator=(
                        f"line {start + productivity[0] + 1}: "
                        f"{productivity[1].strip()}"
                    ),
                    unit_labor_costs_locator=(
                        f"line {start + unit_labor_costs[0] + 1}: "
                        f"{unit_labor_costs[1].strip()}"
                    ),
                )
    raise ValueError("Productivity and Costs comparison table is unavailable")


def _date_from_match(match: re.Match[str]) -> date:
    month = _MONTH_NUMBERS[match.group("month").lower().rstrip(".")]
    return date(int(match.group("year")), month, int(match.group("day")))


def _parse_release_time(
    lines: Sequence[str], expected_release_date: str
) -> _ReleaseTimeObservation:
    expected = date.fromisoformat(expected_release_date)
    for index, line in enumerate(lines):
        if "8:30" not in line:
            continue
        chunk = " ".join(
            " ".join(item.split()) for item in lines[index : index + 5]
        )
        for match in _MONTH_RE.finditer(chunk):
            if _date_from_match(match) != expected:
                continue
            start = chunk.find("8:30")
            lexical = chunk[start : match.end()].strip(" ,.")
            zone_match = re.search(r"\b(EST|EDT|ET)\b", lexical, re.IGNORECASE)
            if zone_match is None:
                raise ValueError("release header omits its Eastern zone")
            reported_zone = zone_match.group(1).upper()
            expected_zone = datetime(
                expected.year,
                expected.month,
                expected.day,
                8,
                30,
                tzinfo=ZoneInfo("America/New_York"),
            ).tzname()
            if reported_zone not in {"ET", expected_zone}:
                raise ValueError("release header zone conflicts with its date")
            return _ReleaseTimeObservation(
                released_lexical=f"{expected_release_date}T08:30:00",
                header_lexical=lexical,
                reported_zone=reported_zone,
                locator=f"lines {index + 1}-{index + 5}: {lexical}",
            )
    raise ValueError("exact 08:30 release header is unavailable")


def _measure_definition(measure_key: str) -> tuple[str, str]:
    try:
        return {
            BLS_PRODUCTIVITY: (
                "nonfarm-business-sector-labor-productivity",
                "percent",
            ),
            BLS_UNIT_LABOR_COSTS: (
                "nonfarm-business-sector-unit-labor-costs",
                "percent",
            ),
        }[measure_key]
    except KeyError as exc:
        raise ValueError(
            "Productivity and Costs measure key is invalid"
        ) from exc


def _release_index_entry(
    *, label: str, artifact_uri: str
) -> BlsProductivityCostsReleaseIndexEntryV1:
    match = _INDEX_LABEL_RE.search(label)
    if match is None:
        raise ValueError("Productivity and Costs index label is invalid")
    quarter = _QUARTER_NUMBERS[match.group("quarter").lower()]
    stage = {
        "preliminary": EconomicReleaseStage.PRELIMINARY,
        "revised": EconomicReleaseStage.REVISION,
    }[match.group("stage").lower()]
    return BlsProductivityCostsReleaseIndexEntryV1(
        reference_period=f"{int(match.group('year')):04d}-Q{quarter}",
        release_stage=stage,
        release_date=_release_date_from_uri(artifact_uri),
        artifact_uri=artifact_uri,
        source_format=_source_format_for_uri(artifact_uri),
        label=label,
    )


def build_bls_productivity_costs_index_request(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> OfficialSourceRequestV1:
    """Build the bounded official archive-index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("Productivity and Costs index requires a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(BLS_PRODUCTIVITY_COSTS_SOURCE_KEY)
    if source.archive_uri != BLS_PRODUCTIVITY_COSTS_INDEX_URI:
        raise ValueError("dedicated Productivity and Costs archive URI differs")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=BLS_PRODUCTIVITY_COSTS_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
    )


def parse_bls_productivity_costs_release_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> BlsProductivityCostsReleaseIndexV1:
    """Parse the authoritative stage inventory and fail on structural gaps."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("Productivity and Costs index requires a v1 snapshot")
    request = snapshot.request
    as_of = _iso_date(as_of_date, "as_of_date")
    if (
        request.source_key != BLS_PRODUCTIVITY_COSTS_SOURCE_KEY
        or request.uri != BLS_PRODUCTIVITY_COSTS_INDEX_URI
        or request.source_format is not OfficialSourceFormat.HTML
        or request.window_end != as_of
    ):
        raise ValueError("Productivity and Costs index snapshot scope differs")
    parser = _ProductivityReleaseListParser()
    try:
        parser.feed(snapshot.content.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError("Productivity and Costs index is not UTF-8") from exc
    entries: list[BlsProductivityCostsReleaseIndexEntryV1] = []
    for label, links in parser.rows:
        artifact_links = [
            urljoin("https://www.bls.gov", item)
            for item in links
            if _PRODUCTIVITY_ARTIFACT_LINK_RE.search(item)
        ]
        if not artifact_links or _INDEX_LABEL_RE.search(label) is None:
            continue
        entry = _release_index_entry(
            label=label, artifact_uri=artifact_links[0]
        )
        if entry.release_date <= as_of:
            entries.append(entry)
    by_uri = {item.artifact_uri: item for item in entries}
    predecessor = by_uri.get(BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI)
    if predecessor is None:
        raise ValueError("Productivity and Costs index omits its predecessor")
    releases = tuple(
        sorted(
            (
                item
                for item in entries
                if item.release_date >= US_BACKFILL_START_DATE
            ),
            key=lambda item: item.release_date,
        )
    )
    return BlsProductivityCostsReleaseIndexV1(
        index_uri=request.uri,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        predecessor=predecessor,
        releases=releases,
    )


def build_bls_productivity_costs_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: BlsProductivityCostsReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact request for every required release artifact."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("Productivity and Costs requests require a v1 registry")
    if not isinstance(release_index, BlsProductivityCostsReleaseIndexV1):
        raise TypeError("Productivity and Costs requests require a v1 index")
    source = registry.source(BLS_PRODUCTIVITY_COSTS_SOURCE_KEY)
    result: list[OfficialSourceRequestV1] = []
    for item in release_index.artifacts:
        result.append(
            OfficialSourceRequestV1(
                source_key=source.source_key,
                source_id=source.source_id,
                method=OfficialRequestMethod.GET,
                uri=item.artifact_uri,
                source_format=item.source_format,
                parser_id=source.parser_id,
                parser_version=source.parser_version,
                window_start=item.release_date,
                window_end=item.release_date,
            )
        )
    return tuple(result)


def _validate_release_snapshot(snapshot: OfficialRawSnapshotV1) -> None:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("Productivity and Costs release requires a v1 snapshot")
    request = snapshot.request
    release_date = _release_date_from_uri(request.uri)
    if (
        request.source_key != BLS_PRODUCTIVITY_COSTS_SOURCE_KEY
        or request.source_format is not _source_format_for_uri(request.uri)
        or request.window_start != release_date
        or request.window_end != release_date
        or not snapshot.content
        or len(snapshot.content) > MAX_BLS_PRODUCTIVITY_COSTS_RELEASE_BYTES
    ):
        raise ValueError("Productivity and Costs snapshot scope differs")


def _values_for_main(
    observation: _MainObservation,
) -> Mapping[str, tuple[str, float | None, str]]:
    productivity_lexical, productivity_value = _published_value(
        observation.productivity_lexical, "productivity actual"
    )
    unit_lexical, unit_value = _published_value(
        observation.unit_labor_costs_lexical, "unit labor costs actual"
    )
    return MappingProxyType(
        {
            BLS_PRODUCTIVITY: (
                productivity_lexical,
                productivity_value,
                observation.productivity_locator,
            ),
            BLS_UNIT_LABOR_COSTS: (
                unit_lexical,
                unit_value,
                observation.unit_labor_costs_locator,
            ),
        }
    )


def _values_for_comparison(
    observation: _ComparisonObservation,
) -> Mapping[str, tuple[str, float | None, str, str, float | None]]:
    result: dict[str, tuple[str, float | None, str, str, float | None]] = {}
    values = {
        BLS_PRODUCTIVITY: (
            observation.previous_productivity_lexical,
            observation.revised_productivity_lexical,
            observation.productivity_locator,
        ),
        BLS_UNIT_LABOR_COSTS: (
            observation.previous_unit_labor_costs_lexical,
            observation.revised_unit_labor_costs_lexical,
            observation.unit_labor_costs_locator,
        ),
    }
    for measure, (previous, revised, locator) in values.items():
        previous_lexical, previous_value = _published_value(
            previous, f"{measure} comparison previous"
        )
        revised_lexical, revised_value = _published_value(
            revised, f"{measure} comparison revised"
        )
        result[measure] = (
            previous_lexical,
            previous_value,
            locator,
            revised_lexical,
            revised_value,
        )
    return MappingProxyType(result)


def parse_bls_productivity_costs_release(
    snapshot: OfficialRawSnapshotV1,
    *,
    indexed_release: BlsProductivityCostsReleaseIndexEntryV1,
    previous_snapshot: OfficialRawSnapshotV1,
    previous_indexed_release: BlsProductivityCostsReleaseIndexEntryV1,
) -> BlsProductivityCostsArchiveEntryV1:
    """Recompute one stage, including the current comparison-table revision."""
    _validate_release_snapshot(snapshot)
    _validate_release_snapshot(previous_snapshot)
    if not isinstance(
        indexed_release, BlsProductivityCostsReleaseIndexEntryV1
    ) or not isinstance(
        previous_indexed_release, BlsProductivityCostsReleaseIndexEntryV1
    ):
        raise TypeError("Productivity and Costs release index entry is invalid")
    if snapshot.request.uri != indexed_release.artifact_uri:
        raise ValueError("current snapshot differs from indexed release")
    if previous_snapshot.request.uri != previous_indexed_release.artifact_uri:
        raise ValueError("previous snapshot differs from indexed release")
    expected_comparison = (
        _previous_quarter(indexed_release.reference_period)
        if indexed_release.release_stage is EconomicReleaseStage.PRELIMINARY
        else indexed_release.reference_period
    )
    if previous_indexed_release.reference_period != expected_comparison:
        raise ValueError("previous indexed release differs from current stage")
    lines = _decode_release(snapshot)
    previous_lines = _decode_release(previous_snapshot)
    main = _parse_main_observation(lines)
    previous_main = _parse_main_observation(previous_lines)
    comparison = _parse_comparison_observation(lines)
    release_time = _parse_release_time(lines, indexed_release.release_date)
    if (
        main.reference_period != indexed_release.reference_period
        or main.release_stage is not indexed_release.release_stage
    ):
        raise ValueError("main table differs from official index identity")
    current_values = _values_for_main(main)
    previous_values = _values_for_main(previous_main)
    comparison_values = _values_for_comparison(comparison)
    source_text = "\n".join(lines)
    previous_source_text = "\n".join(previous_lines)
    source_correction_notice = (
        indexed_release.artifact_uri == BLS_PRODUCTIVITY_COSTS_CORRECTION_URI
        and re.search(
            r"release was reissued on April 26, 2024",
            previous_source_text,
            re.IGNORECASE,
        )
        is not None
        and re.search(
            r"Third quarter, fourth quarter, and annual average data\s+"
            r"for 2023 were revised",
            source_text,
            re.IGNORECASE,
        )
        is not None
    )
    if (
        indexed_release.artifact_uri == BLS_PRODUCTIVITY_COSTS_CORRECTION_URI
        and not source_correction_notice
    ):
        raise ValueError("source correction notice is unavailable")
    historical_revision_notice = (
        re.search(
            r"annual benchmark revision|comprehensive revision|"
            r"historical revisions to productivity and costs data|"
            r"revised (?:back to|from) (?:19|20)\d{2}",
            source_text,
            re.IGNORECASE,
        )
        is not None
    )
    measures: list[BlsProductivityCostsMeasureV1] = []
    for measure_key in BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS:
        actual_lexical, actual_value, actual_locator = current_values[
            measure_key
        ]
        previous_lexical, previous_value, previous_locator = previous_values[
            measure_key
        ]
        (
            source_previous_lexical,
            source_previous_value,
            revision_locator,
            revised_lexical,
            revised_value,
        ) = comparison_values[measure_key]
        comparable = (
            previous_value is not None
            and source_previous_value is not None
            and math.isclose(
                previous_value,
                source_previous_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        )
        if not comparable and not (
            indexed_release.artifact_uri
            in {
                BLS_PRODUCTIVITY_COSTS_CORRECTION_URI,
                "https://www.bls.gov/news.release/archives/prod2_03072019.htm",
            }
            and (
                indexed_release.artifact_uri
                != BLS_PRODUCTIVITY_COSTS_CORRECTION_URI
                or measure_key == BLS_PRODUCTIVITY
            )
        ):
            raise ValueError(
                f"{measure_key} comparison differs from predecessor artifact"
            )
        if indexed_release.release_stage is EconomicReleaseStage.REVISION and (
            actual_lexical,
            actual_value,
        ) != (revised_lexical, revised_value):
            raise ValueError(
                f"{measure_key} revised actual differs from Table B"
            )
        revision = (
            None
            if source_previous_value is None or revised_value is None
            else revised_value - source_previous_value
        )
        lineage, unit = _measure_definition(measure_key)
        limitations = [
            "Forecast is unavailable because BLS publishes no event-level consensus.",
            "Values are nonfarm-business seasonally adjusted annualized quarter-over-quarter percent changes.",
        ]
        if actual_value is None:
            limitations.append(
                "The February 2019 preliminary release published this headline measure as N.A. because source data were unavailable."
            )
        if not comparable:
            limitations.append(
                "The current comparison table cannot be joined numerically to the immediately preceding artifact."
            )
        if source_correction_notice:
            limitations.append(
                "BLS reissued the March 2024 release with an error notice and carried corrected prior-quarter evidence in this release."
            )
        measures.append(
            BlsProductivityCostsMeasureV1(
                measure_key=measure_key,
                reference_period=indexed_release.reference_period,
                comparison_reference_period=expected_comparison,
                release_stage=indexed_release.release_stage,
                actual_lexical=actual_lexical,
                actual_value=actual_value,
                previous_as_known_lexical=previous_lexical,
                previous_as_known_value=previous_value,
                source_comparison_previous_lexical=source_previous_lexical,
                source_comparison_previous_value=source_previous_value,
                revised_comparison_lexical=revised_lexical,
                revised_comparison_value=revised_value,
                revision_value=revision,
                predecessor_lineage_comparable=comparable,
                series_lineage=lineage,
                unit=unit,
                transformation=(
                    "quarter-over-quarter-percent-change-at-annual-rate"
                ),
                seasonality="seasonally-adjusted",
                content_sha256=snapshot.content_sha256,
                actual_locator=actual_locator,
                previous_locator=previous_locator,
                revision_locator=revision_locator,
                limitations=tuple(limitations),
            )
        )
    released_at = normalize_official_source_timestamp(
        f"{indexed_release.release_date}T08:30:00",
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    if released_at.source_lexical != release_time.released_lexical:
        raise ValueError(
            "normalized Productivity and Costs release time differs"
        )
    return BlsProductivityCostsArchiveEntryV1(
        reference_period=indexed_release.reference_period,
        comparison_reference_period=expected_comparison,
        release_stage=indexed_release.release_stage,
        release_date=indexed_release.release_date,
        artifact_uri=indexed_release.artifact_uri,
        source_format=indexed_release.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        previous_artifact_uri=previous_indexed_release.artifact_uri,
        previous_content_sha256=previous_snapshot.content_sha256,
        previous_content_length=len(previous_snapshot.content),
        released_lexical=release_time.released_lexical,
        release_header_lexical=release_time.header_lexical,
        reported_zone=release_time.reported_zone,
        source_era=(
            "fixed-width-text"
            if indexed_release.source_format is OfficialSourceFormat.TEXT
            else "preformatted-html"
        ),
        table_layout=main.table_layout,
        historical_revision_notice=historical_revision_notice,
        source_correction_notice=source_correction_notice,
        actual_available=all(
            item.actual_value is not None for item in measures
        ),
        release_time_locator=release_time.locator,
        measures=tuple(measures),
    )


def _snapshots_by_uri(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> Mapping[str, OfficialRawSnapshotV1]:
    result: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        _validate_release_snapshot(snapshot)
        if snapshot.request.uri in result:
            raise ValueError(
                "Productivity and Costs archive repeats a snapshot"
            )
        result[snapshot.request.uri] = snapshot
    return MappingProxyType(result)


def build_bls_productivity_costs_archive_manifest(
    profile: UnitedStatesBackfillProfileV1,
    release_index: BlsProductivityCostsReleaseIndexV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> BlsProductivityCostsArchiveManifestV1:
    """Replay every indexed stage into compact closure evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("Productivity and Costs manifest requires a v1 profile")
    if not isinstance(release_index, BlsProductivityCostsReleaseIndexV1):
        raise TypeError("Productivity and Costs manifest requires a v1 index")
    program = profile.by_key.get(BLS_PRODUCTIVITY_COSTS_PROGRAM_KEY)
    if (
        program is None
        or program.source_key != BLS_PRODUCTIVITY_COSTS_SOURCE_KEY
    ):
        raise ValueError(
            "profile omits the dedicated Productivity and Costs source"
        )
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {item.artifact_uri for item in release_index.artifacts}
    if set(by_uri) != expected_uris:
        raise ValueError("Productivity and Costs snapshots differ from index")
    entries: list[BlsProductivityCostsArchiveEntryV1] = []
    previous = release_index.predecessor
    for indexed in release_index.releases:
        try:
            entry = parse_bls_productivity_costs_release(
                by_uri[indexed.artifact_uri],
                indexed_release=indexed,
                previous_snapshot=by_uri[previous.artifact_uri],
                previous_indexed_release=previous,
            )
        except ValueError as exc:
            raise ValueError(
                "Productivity and Costs archive parse failed for "
                f"{indexed.stage_key} ({indexed.artifact_uri}): {exc}"
            ) from exc
        entries.append(entry)
        previous = indexed
    artifacts = {
        (item.request.uri, item.content_sha256): len(item.content)
        for item in by_uri.values()
    }
    values = tuple(entries)
    return BlsProductivityCostsArchiveManifestV1(
        registry_id=profile.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        entries=values,
        raw_artifact_count=len(artifacts),
        total_content_bytes=sum(artifacts.values()),
        available_release_count=sum(item.actual_available for item in values),
        unavailable_release_count=sum(
            not item.actual_available for item in values
        ),
        revision_observation_count=sum(
            measure.revision_was_observed
            for item in values
            for measure in item.measures
        ),
        revision_occurrence_count=sum(
            item.value_was_revised for item in values
        ),
        productivity_revision_count=sum(
            item.by_measure[BLS_PRODUCTIVITY].value_was_revised
            for item in values
        ),
        unit_labor_cost_revision_count=sum(
            item.by_measure[BLS_UNIT_LABOR_COSTS].value_was_revised
            for item in values
        ),
        noncomparable_lineage_count=sum(
            not measure.predecessor_lineage_comparable
            for item in values
            for measure in item.measures
        ),
        historical_revision_notice_count=sum(
            item.historical_revision_notice for item in values
        ),
    )


def replay_bls_productivity_costs_archive(
    manifest: BlsProductivityCostsArchiveManifestV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[BlsProductivityCostsArchiveEntryV1, ...]:
    """Recompute and verify every stage-specific normalized entry."""
    if not isinstance(manifest, BlsProductivityCostsArchiveManifestV1):
        raise TypeError("Productivity and Costs replay requires a v1 manifest")
    by_uri = _snapshots_by_uri(snapshots)
    expected_uris = {
        item.artifact_uri for item in manifest.release_index.artifacts
    }
    if set(by_uri) != expected_uris:
        raise ValueError("Productivity and Costs replay snapshots differ")
    releases: list[BlsProductivityCostsArchiveEntryV1] = []
    previous = manifest.release_index.predecessor
    for indexed, expected in zip(
        manifest.release_index.releases, manifest.entries
    ):
        observed = parse_bls_productivity_costs_release(
            by_uri[indexed.artifact_uri],
            indexed_release=indexed,
            previous_snapshot=by_uri[previous.artifact_uri],
            previous_indexed_release=previous,
        )
        if observed != expected:
            raise ValueError(
                f"Productivity and Costs replay differs for {indexed.stage_key}"
            )
        releases.append(observed)
        previous = indexed
    return tuple(releases)


def bls_productivity_costs_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: BlsProductivityCostsArchiveManifestV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    """Derive quantified coverage only from verified manifest evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("Productivity and Costs coverage requires a v1 profile")
    if not isinstance(manifest, BlsProductivityCostsArchiveManifestV1):
        raise TypeError(
            "Productivity and Costs coverage requires a v1 manifest"
        )
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("Productivity and Costs manifest differs from profile")
    program = profile.by_key[BLS_PRODUCTIVITY_COSTS_PROGRAM_KEY]
    end = _iso_date(window_end_date, "window_end_date")
    if end < max(item.release_date for item in manifest.entries):
        raise ValueError("coverage end precedes latest Productivity release")
    count = len(manifest.entries)
    previous_count = sum(
        all(
            measure.previous_as_known_value is not None
            for measure in item.measures
        )
        for item in manifest.entries
    )
    coverage = UnitedStatesProgramCoverageV1(
        program_key=program.program_key,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=end,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=manifest.available_release_count,
        previous_as_known_count=previous_count,
        revision_count=sum(
            item.revision_was_observed for item in manifest.entries
        ),
        exact_minute_count=count,
        forecast_count=0,
        artifact_sha256s=tuple(
            item.content_sha256 for item in manifest.entries
        ),
        gap_reasons=(
            UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,
            UnitedStatesCoverageGapReason.OFFICIAL_MEASURE_UNAVAILABLE,
        ),
        notes=(
            f"Release index: {manifest.release_index.index_id}",
            f"Archive manifest: {manifest.manifest_id}",
            (
                "Qualified preliminary/revised stages: "
                f"{manifest.available_release_count} available and "
                f"{manifest.unavailable_release_count} source-unavailable."
            ),
            (
                "Observed nonzero comparison revisions: productivity "
                f"{manifest.productivity_revision_count}, unit labor costs "
                f"{manifest.unit_labor_cost_revision_count}."
            ),
            (
                "The February 2019 preliminary stage published N.A. for the "
                "selected measures; the following revised stage therefore "
                "has no numeric preliminary comparison."
            ),
            (
                "The May 2024 comparison table carries BLS-corrected prior "
                "productivity that differs from the reissued March artifact."
            ),
        ),
        officially_unavailable_count=manifest.unavailable_release_count,
    )
    if not coverage.is_complete_for(program):
        raise ValueError("manifest does not qualify Productivity coverage")
    return coverage


def packaged_bls_productivity_costs_index_path() -> Path:
    """Return the packaged base64 envelope for the retained archive index."""
    return (
        Path(__file__).with_name("assets")
        / "us_productivity_costs_release_index_v1.html.b64"
    )


def packaged_bls_productivity_costs_archive_manifest_path() -> Path:
    """Return the packaged compact archive-manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "us_productivity_costs_archive_v1.json"
    )


def load_packaged_bls_productivity_costs_archive_manifest() -> (
    BlsProductivityCostsArchiveManifestV1
):
    """Load and validate the packaged Productivity and Costs manifest."""
    path = packaged_bls_productivity_costs_archive_manifest_path()
    return BlsProductivityCostsArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


def load_packaged_bls_productivity_costs_index() -> bytes:
    """Decode and verify the exact packaged official archive index."""
    path = packaged_bls_productivity_costs_index_path()
    try:
        content = base64.b64decode(
            path.read_text(encoding="ascii").strip(), validate=True
        )
    except (ValueError, UnicodeError) as exc:
        raise ValueError(
            "packaged Productivity and Costs index is invalid"
        ) from exc
    manifest = load_packaged_bls_productivity_costs_archive_manifest()
    if (
        len(content) != manifest.release_index.content_length
        or hashlib.sha256(content).hexdigest()
        != manifest.release_index.content_sha256
    ):
        raise ValueError("packaged Productivity and Costs index differs")
    return content


__all__ = [
    "BLS_PRODUCTIVITY",
    "BLS_PRODUCTIVITY_COSTS_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "BLS_PRODUCTIVITY_COSTS_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "BLS_PRODUCTIVITY_COSTS_CORRECTION_URI",
    "BLS_PRODUCTIVITY_COSTS_INDEX_ENTRY_SCHEMA_VERSION",
    "BLS_PRODUCTIVITY_COSTS_INDEX_SCHEMA_VERSION",
    "BLS_PRODUCTIVITY_COSTS_INDEX_URI",
    "BLS_PRODUCTIVITY_COSTS_MEASURE_KEYS",
    "BLS_PRODUCTIVITY_COSTS_MEASURE_SCHEMA_VERSION",
    "BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI",
    "BLS_PRODUCTIVITY_COSTS_PROGRAM_KEY",
    "BLS_PRODUCTIVITY_COSTS_SOURCE_KEY",
    "BLS_PRODUCTIVITY_COSTS_UNAVAILABLE_URI",
    "BLS_UNIT_LABOR_COSTS",
    "BlsProductivityCostsArchiveEntryV1",
    "BlsProductivityCostsArchiveManifestV1",
    "BlsProductivityCostsMeasureV1",
    "BlsProductivityCostsReleaseIndexEntryV1",
    "BlsProductivityCostsReleaseIndexV1",
    "bls_productivity_costs_coverage_from_manifest",
    "build_bls_productivity_costs_archive_manifest",
    "build_bls_productivity_costs_archive_requests",
    "build_bls_productivity_costs_index_request",
    "load_packaged_bls_productivity_costs_archive_manifest",
    "load_packaged_bls_productivity_costs_index",
    "packaged_bls_productivity_costs_archive_manifest_path",
    "packaged_bls_productivity_costs_index_path",
    "parse_bls_productivity_costs_release",
    "parse_bls_productivity_costs_release_index",
    "replay_bls_productivity_costs_archive",
]

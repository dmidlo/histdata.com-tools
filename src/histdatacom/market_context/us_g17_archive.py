"""Complete Federal Reserve G.17 archive qualification for the U.S. backfill.

The compact packaged manifest binds the official release-date index to every
plain-text G.17 release from 2000 through its declared as-of date.  Raw release
bytes remain an external corpus, but each occurrence has an official URI, byte
length, raw SHA-256, independently normalized triplet SHA-256, and exact
headline values.  Replay therefore fails closed when either the remote archive
or the source-specific parser drifts.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

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
    UnitedStatesCoverageGapReason,
    UnitedStatesProgramCoverageV1,
    UnitedStatesReleaseTripletV1,
    build_federal_reserve_g17_archive_request,
    parse_federal_reserve_g17_release,
)
from histdatacom.runtime_contracts import JSONValue

FEDERAL_RESERVE_G17_INDEX_SCHEMA_VERSION = (
    "histdatacom.federal-reserve-g17-index.v1"
)
FEDERAL_RESERVE_G17_ARCHIVE_ENTRY_SCHEMA_VERSION = (
    "histdatacom.federal-reserve-g17-archive-entry.v1"
)
FEDERAL_RESERVE_G17_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.federal-reserve-g17-archive-manifest.v1"
)

FEDERAL_RESERVE_G17_INDEX_URI = (
    "https://www.federalreserve.gov/releases/g17/release_dates.htm"
)
FEDERAL_RESERVE_G17_PROGRAM_KEY = "us.frb.industrial-production"
MAX_FEDERAL_RESERVE_G17_RELEASES = 512
MAX_FEDERAL_RESERVE_G17_INDEX_BYTES = 4 * 1024 * 1024
MAX_FEDERAL_RESERVE_G17_RELEASE_BYTES = 8 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MONTH_ROW_RE = re.compile(
    r"^\s*(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<year>\d{4})\s+"
    r"(?P<value>[^\r\n]+?)\s*$",
    re.MULTILINE,
)
_RELEASE_DATE_RE = re.compile(
    r"(?P<day>\d{2})-(?P<month>January|February|March|April|May|June|"
    r"July|August|September|October|November|December)-(?P<year>\d{4})"
)
_MONTH_NUMBERS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}


def _required_text(value: object, name: str) -> str:
    text = str(value).strip()
    if not text or len(text) > 4096:
        raise ValueError(f"{name} is invalid")
    return text


def _iso_date(value: object, name: str) -> str:
    text = _required_text(value, name)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != text:
        raise ValueError(f"{name} must be a canonical ISO date")
    return text


def _year_month(value: object, name: str) -> str:
    text = _required_text(value, name)
    if re.fullmatch(r"\d{4}-(?:0[1-9]|1[0-2])", text) is None:
        raise ValueError(f"{name} must be a canonical year-month")
    return text


def _sha256(value: object, name: str) -> str:
    text = _required_text(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return text


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


def _g17_lexical_number(value: object, name: str) -> tuple[str, float]:
    lexical = _required_text(value, name)
    normalized = lexical
    if lexical.startswith(("-.", "+.")):
        normalized = lexical[:1] + "0" + lexical[1:]
    elif lexical.startswith("."):
        normalized = "0" + lexical
    try:
        numeric = float(normalized)
    except ValueError as exc:
        raise ValueError(f"{name} is not a G.17 numeric lexical value") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"{name} is not finite")
    return lexical, numeric


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


def _sorted_unique_dates(
    values: Sequence[object], name: str
) -> tuple[str, ...]:
    dates = tuple(_iso_date(item, name) for item in values)
    if dates != tuple(sorted(set(dates))):
        raise ValueError(f"{name} must be unique and sorted")
    return dates


@dataclass(frozen=True, slots=True)
class FederalReserveG17ReleaseIndexV1:
    """As-of interpretation of one retained official release-date page."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    published_release_dates: tuple[str, ...]
    future_release_dates: tuple[str, ...]
    unavailable_schedule_months: tuple[str, ...]
    index_id: str = ""
    schema_version: str = FEDERAL_RESERVE_G17_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEDERAL_RESERVE_G17_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported Federal Reserve G.17 index schema")
        if self.source_uri != FEDERAL_RESERVE_G17_INDEX_URI:
            raise ValueError("Federal Reserve G.17 index URI is invalid")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_FEDERAL_RESERVE_G17_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        object.__setattr__(self, "as_of_date", as_of)
        published = _sorted_unique_dates(
            self.published_release_dates, "published_release_dates"
        )
        future = _sorted_unique_dates(
            self.future_release_dates, "future_release_dates"
        )
        if not published or len(published) > MAX_FEDERAL_RESERVE_G17_RELEASES:
            raise ValueError("G.17 published release-date count is invalid")
        if any(item > as_of for item in published):
            raise ValueError("published G.17 date follows the as-of date")
        if any(item <= as_of for item in future):
            raise ValueError("future G.17 date does not follow the as-of date")
        if set(published) & set(future):
            raise ValueError("G.17 published and future dates overlap")
        unavailable = tuple(
            _year_month(item, "unavailable_schedule_months")
            for item in self.unavailable_schedule_months
        )
        if unavailable != tuple(sorted(set(unavailable))):
            raise ValueError(
                "unavailable_schedule_months must be unique and sorted"
            )
        object.__setattr__(self, "published_release_dates", published)
        object.__setattr__(self, "future_release_dates", future)
        object.__setattr__(self, "unavailable_schedule_months", unavailable)
        expected = _stable_id(
            "federal-reserve-g17-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected:
            raise ValueError("Federal Reserve G.17 index identity differs")
        object.__setattr__(self, "index_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "as_of_date": self.as_of_date,
            "published_release_dates": list(self.published_release_dates),
            "future_release_dates": list(self.future_release_dates),
            "unavailable_schedule_months": list(
                self.unavailable_schedule_months
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveG17ReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            published_release_dates=tuple(
                str(item)
                for item in _sequence(
                    data.get("published_release_dates"),
                    "published_release_dates",
                )
            ),
            future_release_dates=tuple(
                str(item)
                for item in _sequence(
                    data.get("future_release_dates"),
                    "future_release_dates",
                )
            ),
            unavailable_schedule_months=tuple(
                str(item)
                for item in _sequence(
                    data.get("unavailable_schedule_months"),
                    "unavailable_schedule_months",
                )
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveG17ArchiveEntryV1:
    """One raw-backed, independently normalized G.17 release occurrence."""

    archive_date: str
    release_date: str
    artifact_uri: str
    content_sha256: str
    content_length: int
    normalized_sha256: str
    reference_period: str
    released_lexical: str
    actual_value: float
    actual_lexical: str
    previous_as_known_value: float
    previous_as_known_lexical: str
    revised_previous_value: float
    revised_previous_lexical: str
    entry_id: str = ""
    schema_version: str = FEDERAL_RESERVE_G17_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != FEDERAL_RESERVE_G17_ARCHIVE_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Federal Reserve G.17 archive-entry schema"
            )
        archive_date = _iso_date(self.archive_date, "archive_date")
        release_date = _iso_date(self.release_date, "release_date")
        object.__setattr__(self, "archive_date", archive_date)
        object.__setattr__(self, "release_date", release_date)
        expected_uri = (
            "https://www.federalreserve.gov/releases/g17/"
            f"{archive_date.replace('-', '')}/g17.txt"
        )
        if self.artifact_uri != expected_uri:
            raise ValueError(
                "G.17 archive-entry URI differs from archive date: "
                f"{self.artifact_uri} != {expected_uri}"
            )
        for name in ("content_sha256", "normalized_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _positive_int(
            self.content_length,
            "content_length",
            MAX_FEDERAL_RESERVE_G17_RELEASE_BYTES,
        )
        object.__setattr__(
            self,
            "reference_period",
            _year_month(self.reference_period, "reference_period"),
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release_date}T09:15:00":
            raise ValueError("G.17 released time differs from release date")
        object.__setattr__(self, "released_lexical", released)
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
            lexical, numeric = _g17_lexical_number(
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
        expected = _stable_id(
            "federal-reserve-g17-archive-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Federal Reserve G.17 archive-entry identity differs"
            )
        object.__setattr__(self, "entry_id", expected)

    @property
    def previous_was_revised(self) -> bool:
        return not math.isclose(
            self.previous_as_known_value,
            self.revised_previous_value,
            rel_tol=0.0,
            abs_tol=1e-15,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "archive_date": self.archive_date,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "normalized_sha256": self.normalized_sha256,
            "reference_period": self.reference_period,
            "released_lexical": self.released_lexical,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "revised_previous_value": self.revised_previous_value,
            "revised_previous_lexical": self.revised_previous_lexical,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveG17ArchiveEntryV1:
        return cls(
            archive_date=str(data.get("archive_date", "")),
            release_date=str(data.get("release_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            normalized_sha256=str(data.get("normalized_sha256", "")),
            reference_period=str(data.get("reference_period", "")),
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
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveG17ArchiveManifestV1:
    """Compact closure evidence for the complete as-of G.17 archive."""

    registry_id: str
    profile_id: str
    release_index: FederalReserveG17ReleaseIndexV1
    entries: tuple[FederalReserveG17ArchiveEntryV1, ...]
    total_content_bytes: int
    revision_occurrence_count: int
    manifest_id: str = ""
    schema_version: str = FEDERAL_RESERVE_G17_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != FEDERAL_RESERVE_G17_ARCHIVE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Federal Reserve G.17 archive-manifest schema"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("G.17 manifest registry identity is invalid")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("G.17 manifest profile identity is invalid")
        if not isinstance(self.release_index, FederalReserveG17ReleaseIndexV1):
            raise TypeError("G.17 manifest requires a v1 release index")
        entries = tuple(self.entries)
        if (
            not entries
            or len(entries) > MAX_FEDERAL_RESERVE_G17_RELEASES
            or any(
                not isinstance(item, FederalReserveG17ArchiveEntryV1)
                for item in entries
            )
        ):
            raise TypeError("G.17 manifest entries are invalid")
        archive_dates = tuple(item.archive_date for item in entries)
        if archive_dates != tuple(sorted(set(archive_dates))):
            raise ValueError("G.17 manifest entries must be unique and sorted")
        if archive_dates != self.release_index.published_release_dates:
            raise ValueError("G.17 manifest differs from its release index")
        if len({item.release_date for item in entries}) != len(entries):
            raise ValueError("G.17 manifest repeats a publication date")
        if len({item.content_sha256 for item in entries}) != len(entries):
            raise ValueError("G.17 manifest repeats raw artifact content")
        if len({item.reference_period for item in entries}) != len(entries):
            raise ValueError("G.17 manifest repeats a reference period")
        total = sum(item.content_length for item in entries)
        revisions = sum(item.previous_was_revised for item in entries)
        if self.total_content_bytes != total:
            raise ValueError("G.17 manifest total byte count differs")
        if self.revision_occurrence_count != revisions:
            raise ValueError("G.17 manifest revision count differs")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id(
            "federal-reserve-g17-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Federal Reserve G.17 archive identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_release_date(
        self,
    ) -> Mapping[str, FederalReserveG17ArchiveEntryV1]:
        return MappingProxyType(
            {item.release_date: item for item in self.entries}
        )

    @property
    def by_archive_date(
        self,
    ) -> Mapping[str, FederalReserveG17ArchiveEntryV1]:
        return MappingProxyType(
            {item.archive_date: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
            "total_content_bytes": self.total_content_bytes,
            "revision_occurrence_count": self.revision_occurrence_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveG17ArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=FederalReserveG17ReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            entries=tuple(
                FederalReserveG17ArchiveEntryV1.from_dict(
                    _mapping(item, "archive entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> FederalReserveG17ArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("G.17 archive manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "archive manifest"))


def build_federal_reserve_g17_index_request(
    registry: OfficialSourceRegistryV1,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    """Plan the exact official G.17 release-date index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("G.17 index request requires a v1 source registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(FEDERAL_RESERVE_G17_PROGRAM_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=FEDERAL_RESERVE_G17_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
    )


def parse_federal_reserve_g17_release_index(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> FederalReserveG17ReleaseIndexV1:
    """Parse historical, unavailable, and scheduled dates from the index."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("G.17 index parser requires a v1 official snapshot")
    if (
        snapshot.request.source_key != FEDERAL_RESERVE_G17_PROGRAM_KEY
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.uri != FEDERAL_RESERVE_G17_INDEX_URI
    ):
        raise ValueError("G.17 index parser received a different source")
    as_of = _iso_date(as_of_date, "as_of_date")
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("G.17 release-date index is not UTF-8") from exc
    published: list[str] = []
    future: list[str] = []
    unavailable: list[str] = []
    seen_months: set[str] = set()
    for row in _MONTH_ROW_RE.finditer(text):
        month_number = _MONTH_NUMBERS[row.group("month").lower()]
        schedule_month = f"{int(row.group('year')):04d}-{month_number:02d}"
        if schedule_month < "2000-01":
            continue
        if schedule_month in seen_months:
            raise ValueError("G.17 release-date index repeats a month row")
        seen_months.add(schedule_month)
        value = row.group("value").strip()
        date_matches = tuple(_RELEASE_DATE_RE.finditer(value))
        if value == "NA":
            unavailable.append(schedule_month)
            continue
        if not date_matches:
            raise ValueError("G.17 release-date row has an unknown value")
        remainder = _RELEASE_DATE_RE.sub("", value)
        if remainder.strip().lower() not in {"", "and"}:
            raise ValueError("G.17 release-date row has unparsed content")
        for match in date_matches:
            release_date = date(
                int(match.group("year")),
                _MONTH_NUMBERS[match.group("month").lower()],
                int(match.group("day")),
            ).isoformat()
            (published if release_date <= as_of else future).append(
                release_date
            )
    if not seen_months or "2000-01" not in seen_months:
        raise ValueError("G.17 release-date index omits the 2000 boundary")
    return FederalReserveG17ReleaseIndexV1(
        source_uri=FEDERAL_RESERVE_G17_INDEX_URI,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        published_release_dates=tuple(sorted(published)),
        future_release_dates=tuple(sorted(future)),
        unavailable_schedule_months=tuple(sorted(unavailable)),
    )


def build_federal_reserve_g17_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: FederalReserveG17ReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact request for every published index occurrence."""
    if not isinstance(release_index, FederalReserveG17ReleaseIndexV1):
        raise TypeError("G.17 archive requests require a v1 release index")
    return tuple(
        build_federal_reserve_g17_archive_request(registry, item)
        for item in release_index.published_release_dates
    )


def _entry_from_triplet(
    snapshot: OfficialRawSnapshotV1,
    triplet: UnitedStatesReleaseTripletV1,
) -> FederalReserveG17ArchiveEntryV1:
    return FederalReserveG17ArchiveEntryV1(
        archive_date=cast(str, snapshot.request.window_start),
        release_date=triplet.released_lexical[:10],
        artifact_uri=snapshot.request.uri,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        normalized_sha256=triplet.normalized_sha256,
        reference_period=triplet.reference_period,
        released_lexical=triplet.released_lexical,
        actual_value=triplet.actual_value,
        actual_lexical=triplet.actual_lexical,
        previous_as_known_value=triplet.previous_as_known_value,
        previous_as_known_lexical=triplet.previous_as_known_lexical,
        revised_previous_value=triplet.revised_previous_value,
        revised_previous_lexical=triplet.revised_previous_lexical,
    )


def _g17_snapshots_by_date(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> Mapping[str, OfficialRawSnapshotV1]:
    result: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, OfficialRawSnapshotV1):
            raise TypeError("G.17 archive requires v1 official snapshots")
        release_date = snapshot.request.window_start
        if (
            snapshot.request.source_key != FEDERAL_RESERVE_G17_PROGRAM_KEY
            or snapshot.request.source_format is not OfficialSourceFormat.TEXT
            or release_date is None
            or snapshot.request.window_end != release_date
        ):
            raise ValueError("G.17 archive snapshot has a different scope")
        if release_date in result:
            raise ValueError("G.17 archive repeats a release-date snapshot")
        result[release_date] = snapshot
    return MappingProxyType(result)


def build_federal_reserve_g17_archive_manifest(
    profile: UnitedStatesBackfillProfileV1,
    release_index: FederalReserveG17ReleaseIndexV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> FederalReserveG17ArchiveManifestV1:
    """Replay all indexed raw releases and produce compact closure evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("G.17 manifest requires a v1 U.S. profile")
    program = profile.by_key.get(FEDERAL_RESERVE_G17_PROGRAM_KEY)
    if program is None:
        raise ValueError("U.S. profile omits the G.17 program")
    by_date = _g17_snapshots_by_date(snapshots)
    expected_dates = set(release_index.published_release_dates)
    if set(by_date) != expected_dates:
        raise ValueError("G.17 snapshots differ from the release-date index")
    entries: list[FederalReserveG17ArchiveEntryV1] = []
    for release_date in release_index.published_release_dates:
        snapshot = by_date[release_date]
        triplet = parse_federal_reserve_g17_release(snapshot)
        if triplet.program_key != program.program_key:
            raise ValueError("G.17 triplet differs from the U.S. profile")
        entries.append(_entry_from_triplet(snapshot, triplet))
    return FederalReserveG17ArchiveManifestV1(
        registry_id=profile.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        entries=tuple(entries),
        total_content_bytes=sum(item.content_length for item in entries),
        revision_occurrence_count=sum(
            item.previous_was_revised for item in entries
        ),
    )


def replay_federal_reserve_g17_archive(
    manifest: FederalReserveG17ArchiveManifestV1,
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[UnitedStatesReleaseTripletV1, ...]:
    """Recompute and verify every normalized entry against retained bytes."""
    if not isinstance(manifest, FederalReserveG17ArchiveManifestV1):
        raise TypeError("G.17 replay requires a v1 archive manifest")
    by_date = _g17_snapshots_by_date(snapshots)
    if set(by_date) != set(manifest.release_index.published_release_dates):
        raise ValueError("G.17 replay snapshots differ from the manifest")
    triplets: list[UnitedStatesReleaseTripletV1] = []
    for expected in manifest.entries:
        snapshot = by_date[expected.archive_date]
        triplet = parse_federal_reserve_g17_release(snapshot)
        observed = _entry_from_triplet(snapshot, triplet)
        if observed != expected:
            raise ValueError(f"G.17 replay differs for {expected.archive_date}")
        triplets.append(triplet)
    return tuple(triplets)


def federal_reserve_g17_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: FederalReserveG17ArchiveManifestV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    """Derive the G.17 closure slice only from verified manifest evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("G.17 coverage requires a v1 U.S. profile")
    if not isinstance(manifest, FederalReserveG17ArchiveManifestV1):
        raise TypeError("G.17 coverage requires a v1 archive manifest")
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("G.17 manifest differs from the U.S. profile")
    program = profile.by_key[FEDERAL_RESERVE_G17_PROGRAM_KEY]
    end = _iso_date(window_end_date, "window_end_date")
    if end < max(item.release_date for item in manifest.entries):
        raise ValueError("G.17 coverage end precedes the latest release")
    count = len(manifest.entries)
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
            (
                "Official unavailable schedule months: "
                + ", ".join(manifest.release_index.unavailable_schedule_months)
            ),
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError("G.17 manifest does not qualify complete coverage")
    return coverage


def packaged_federal_reserve_g17_index_path() -> Path:
    """Return the packaged base64 envelope for the retained index HTML."""
    return (
        Path(__file__).with_name("assets") / "us_g17_release_dates_v1.html.b64"
    )


def packaged_federal_reserve_g17_archive_manifest_path() -> Path:
    """Return the packaged compact G.17 archive-manifest path."""
    return Path(__file__).with_name("assets") / "us_g17_archive_v1.json"


def load_packaged_federal_reserve_g17_archive_manifest() -> (
    FederalReserveG17ArchiveManifestV1
):
    """Load and validate the packaged complete as-of G.17 manifest."""
    path = packaged_federal_reserve_g17_archive_manifest_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            "packaged G.17 archive manifest is unavailable"
        ) from exc
    return FederalReserveG17ArchiveManifestV1.from_json(text)


def load_packaged_federal_reserve_g17_index() -> bytes:
    """Decode and hash-check the exact index used by the archive manifest."""
    path = packaged_federal_reserve_g17_index_path()
    try:
        content = base64.b64decode(
            path.read_text(encoding="ascii").strip(), validate=True
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError(
            "packaged G.17 index is unavailable or invalid"
        ) from exc
    manifest = load_packaged_federal_reserve_g17_archive_manifest()
    if (
        len(content) != manifest.release_index.content_length
        or hashlib.sha256(content).hexdigest()
        != manifest.release_index.content_sha256
    ):
        raise ValueError("packaged G.17 index differs from its manifest")
    return content


__all__ = [
    "FEDERAL_RESERVE_G17_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "FEDERAL_RESERVE_G17_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "FEDERAL_RESERVE_G17_INDEX_SCHEMA_VERSION",
    "FEDERAL_RESERVE_G17_INDEX_URI",
    "FEDERAL_RESERVE_G17_PROGRAM_KEY",
    "FederalReserveG17ArchiveEntryV1",
    "FederalReserveG17ArchiveManifestV1",
    "FederalReserveG17ReleaseIndexV1",
    "build_federal_reserve_g17_archive_manifest",
    "build_federal_reserve_g17_archive_requests",
    "build_federal_reserve_g17_index_request",
    "federal_reserve_g17_coverage_from_manifest",
    "load_packaged_federal_reserve_g17_archive_manifest",
    "load_packaged_federal_reserve_g17_index",
    "packaged_federal_reserve_g17_archive_manifest_path",
    "packaged_federal_reserve_g17_index_path",
    "parse_federal_reserve_g17_release_index",
    "replay_federal_reserve_g17_archive",
]

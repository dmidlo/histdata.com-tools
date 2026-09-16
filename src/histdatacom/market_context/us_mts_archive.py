"""Deterministic qualification of the official Monthly Treasury Statement.

The FiscalData catalog exposes a continuous official PDF series.  This module
retains December 1999 as the predecessor and qualifies every January 2000
through August 2026 statement.  Table 1 supplies the contemporaneous budget
balance and the later revised prior month; the preceding PDF supplies the
previous-as-known value.  Release dates are bound to each PDF's schedule page
or to Treasury's year-end release evidence without inventing unavailable
times.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import itertools
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import Enum
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import EconomicTimePrecision
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

MTS_SOURCE_KEY: Final = "us.treasury.fiscal-data"
MTS_PROGRAM_KEY: Final = "us.treasury.monthly-statement"
MTS_PARSER_ID: Final = "official.treasury-mts.v1"
MTS_DATASET_ID: Final = "015-BFS-2014Q1-13"
MTS_API_TABLE_ID: Final = "mts_table_1"
MTS_DATASET_URI: Final = (
    "https://fiscaldata.treasury.gov/datasets/monthly-treasury-statement/"
)
MTS_CATALOG_URI: Final = (
    "https://fiscaldata.treasury.gov/page-data/datasets/"
    "monthly-treasury-statement/page-data.json"
)
MTS_API_URI: Final = (
    "https://api.fiscaldata.treasury.gov/services/api/"
    "fiscal_service/v1/accounting/mts/mts_table_1"
)
MTS_REPORT_URI_TEMPLATE: Final = (
    "https://fiscaldata.treasury.gov/static-data/published-reports/mts/"
    "MonthlyTreasuryStatement_{period}.pdf"
)
MTS_FIRST_REFERENCE_PERIOD: Final = "2000-01"
MTS_PREDECESSOR_REFERENCE_PERIOD: Final = "1999-12"
MTS_LATEST_PACKAGED_REFERENCE_PERIOD: Final = "2026-08"
MTS_SOURCE_TIMEZONE: Final = "America/New_York"

MTS_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.mts-artifact.v1"
MTS_INDEX_ENTRY_SCHEMA_VERSION: Final = "histdatacom.mts-index-entry.v1"
MTS_RELEASE_INDEX_SCHEMA_VERSION: Final = "histdatacom.mts-release-index.v1"
MTS_TIMING_SCHEMA_VERSION: Final = "histdatacom.mts-release-timing.v1"
MTS_VALUE_OVERRIDE_SCHEMA_VERSION: Final = "histdatacom.mts-value-override.v1"
MTS_REVIEWED_EXTRACTS_SCHEMA_VERSION: Final = (
    "histdatacom.mts-reviewed-extracts.v1"
)
MTS_PUBLICATION_SCHEMA_VERSION: Final = "histdatacom.mts-publication.v1"
MTS_ARCHIVE_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.mts-archive-manifest.v1"
)
MTS_CATALOG_ENVELOPE_SCHEMA_VERSION: Final = (
    "histdatacom.mts-catalog-envelope.v1"
)

MAX_MTS_ARTIFACTS: Final = 512
MAX_MTS_ARTIFACT_BYTES: Final = 16 * 1024 * 1024
MAX_MTS_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PERIOD_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")
_REPORT_RE = re.compile(
    r"^/static-data/published-reports/mts/"
    r"MonthlyTreasuryStatement_(?P<year>\d{4})(?P<month>\d{2})\.pdf$"
)
_NUMBER_RE = re.compile(r"[+\-−]?\d[\d,]*")
_MONTHS: Final = tuple(calendar.month_name)


class MtsTimingBasis(str, Enum):
    """First-party evidence used for a publication date and time."""

    PREVIOUS_REPORT = "previous-report-schedule"
    ANNUAL_TABLE = "annual-schedule-table"
    YEAR_END_PRESS_RELEASE = "year-end-press-release"
    RESPONSE_LAST_MODIFIED = "official-response-last-modified"
    REVIEWED_SOURCE_CORRECTION = "reviewed-source-correction"

    @classmethod
    def from_value(cls, value: str | MtsTimingBasis) -> MtsTimingBasis:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported MTS timing basis") from exc


def _required_text(value: object, name: str) -> str:
    result = str(value).strip()
    if not result or len(result) > 16_384:
        raise ValueError(f"{name} is invalid")
    return result


def _optional_text(value: object | None, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _period(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _PERIOD_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
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


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _bounded_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _next_period(value: str) -> str:
    year, month = (int(item) for item in value.split("-"))
    if month == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{month + 1:02d}"


def _previous_period(value: str) -> str:
    year, month = (int(item) for item in value.split("-"))
    if month == 1:
        return f"{year - 1:04d}-12"
    return f"{year:04d}-{month - 1:02d}"


def _periods(start: str, end: str) -> tuple[str, ...]:
    result = [start]
    while result[-1] < end:
        result.append(_next_period(result[-1]))
    return tuple(result)


def _report_uri(period: str) -> str:
    return MTS_REPORT_URI_TEMPLATE.format(period=period.replace("-", ""))


def _balance_lexical(value: int) -> str:
    return f"{value:+,d}" if value >= 0 else f"{value:,d}"


@dataclass(frozen=True, slots=True)
class TreasuryMtsArtifactV1:
    """One exact official catalog, report, or year-end artifact."""

    role: str
    uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = MTS_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported MTS artifact schema")
        role = _required_text(self.role, "role")
        if role not in {"catalog", "report", "year-end-release"}:
            raise ValueError("unsupported MTS artifact role")
        uri = _required_text(self.uri, "uri")
        if not uri.startswith("https://"):
            raise ValueError("MTS artifact URI must use HTTPS")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            "catalog": OfficialSourceFormat.JSON,
            "report": OfficialSourceFormat.PDF,
            "year-end-release": OfficialSourceFormat.HTML,
        }[role]
        if source_format is not expected_format:
            raise ValueError("MTS artifact format differs from its role")
        digest = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length, "content_length", MAX_MTS_ARTIFACT_BYTES
        )
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "uri", uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_sha256", digest)
        expected = _stable_id("mts-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("MTS artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "uri": self.uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsArtifactV1:
        return cls(
            role=str(data.get("role", "")),
            uri=str(data.get("uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryMtsIndexEntryV1:
    """One official statement enumerated by the FiscalData catalog."""

    reference_period: str
    report_uri: str
    entry_id: str = ""
    schema_version: str = MTS_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported MTS index-entry schema")
        period = _period(self.reference_period, "reference_period")
        if self.report_uri != _report_uri(period):
            raise ValueError("MTS report URI differs from its period")
        object.__setattr__(self, "reference_period", period)
        expected = _stable_id("mts-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("MTS index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "report_uri": self.report_uri,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            report_uri=str(data.get("report_uri", "")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryMtsReleaseIndexV1:
    """Hash-bound selected slice of the official report catalog."""

    as_of_date: str
    catalog: TreasuryMtsArtifactV1
    entries: tuple[TreasuryMtsIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = MTS_RELEASE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_RELEASE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported MTS release-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if self.catalog.role != "catalog":
            raise ValueError("MTS release index requires its catalog")
        entries = tuple(self.entries)
        expected_periods = _periods(
            MTS_PREDECESSOR_REFERENCE_PERIOD,
            MTS_LATEST_PACKAGED_REFERENCE_PERIOD,
        )
        if tuple(item.reference_period for item in entries) != expected_periods:
            raise ValueError("MTS selected report inventory differs")
        if len(entries) > MAX_MTS_ARTIFACTS:
            raise ValueError("MTS release index exceeds its bound")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id("mts-release-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("MTS release-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_period(self) -> Mapping[str, TreasuryMtsIndexEntryV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "catalog": self.catalog.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            catalog=TreasuryMtsArtifactV1.from_dict(
                _mapping(data.get("catalog"), "catalog")
            ),
            entries=tuple(
                TreasuryMtsIndexEntryV1.from_dict(
                    _mapping(item, "MTS index entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryMtsReleaseTimingV1:
    """Reviewed release metadata bound to exact first-party evidence."""

    reference_period: str
    release_date: str
    release_time_local: str | None
    time_precision: EconomicTimePrecision
    basis: MtsTimingBasis
    evidence_uri: str
    evidence_sha256: str
    source_lexical: str
    timing_id: str = ""
    schema_version: str = MTS_TIMING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_TIMING_SCHEMA_VERSION:
            raise ValueError("unsupported MTS timing schema")
        period = _period(self.reference_period, "reference_period")
        release_date = _iso_date(self.release_date, "release_date")
        local = _optional_text(self.release_time_local, "release_time_local")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if precision is EconomicTimePrecision.EXACT_MINUTE:
            if local is None or re.fullmatch(r"\d{2}:\d{2}", local) is None:
                raise ValueError("exact MTS timing requires HH:MM")
        elif precision in {
            EconomicTimePrecision.INFERRED_BOUNDED,
            EconomicTimePrecision.DATE_ONLY,
        }:
            if local is not None:
                raise ValueError("bounded MTS timing cannot invent a clock")
        else:
            raise ValueError("unsupported MTS timing precision")
        basis = MtsTimingBasis.from_value(self.basis)
        uri = _required_text(self.evidence_uri, "evidence_uri")
        if not uri.startswith("https://"):
            raise ValueError("MTS timing evidence must use HTTPS")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "release_time_local", local)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "evidence_uri", uri)
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        object.__setattr__(
            self,
            "source_lexical",
            _required_text(self.source_lexical, "source_lexical"),
        )
        expected = _stable_id("mts-release-timing", self.identity_payload())
        if self.timing_id and self.timing_id != expected:
            raise ValueError("MTS release-timing identity differs")
        object.__setattr__(self, "timing_id", expected)

    @property
    def released_at_ns(self) -> int | None:
        if self.time_precision is not EconomicTimePrecision.EXACT_MINUTE:
            return None
        utc_ns: object = normalize_official_source_timestamp(
            f"{self.release_date}T{self.release_time_local}:00",
            MTS_SOURCE_TIMEZONE,
            EconomicTimePrecision.EXACT_MINUTE,
        ).utc_ns
        if not isinstance(utc_ns, int):
            raise TypeError(
                "normalized MTS release timestamp must be an integer"
            )
        return utc_ns

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_date": self.release_date,
            "release_time_local": self.release_time_local,
            "time_precision": self.time_precision.value,
            "basis": self.basis.value,
            "evidence_uri": self.evidence_uri,
            "evidence_sha256": self.evidence_sha256,
            "source_lexical": self.source_lexical,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "timing_id": self.timing_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsReleaseTimingV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_date=str(data.get("release_date", "")),
            release_time_local=(
                None
                if data.get("release_time_local") is None
                else str(data.get("release_time_local"))
            ),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            basis=MtsTimingBasis.from_value(str(data.get("basis", ""))),
            evidence_uri=str(data.get("evidence_uri", "")),
            evidence_sha256=str(data.get("evidence_sha256", "")),
            source_lexical=str(data.get("source_lexical", "")),
            timing_id=str(data.get("timing_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryMtsValueOverrideV1:
    """Reviewed Table 1 values for a PDF with a damaged text map."""

    reference_period: str
    report_sha256: str
    actual_receipts: int
    actual_outlays: int
    actual_balance: int
    revised_previous_balance: int
    locator: str
    override_id: str = ""
    schema_version: str = MTS_VALUE_OVERRIDE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_VALUE_OVERRIDE_SCHEMA_VERSION:
            raise ValueError("unsupported MTS value-override schema")
        period = _period(self.reference_period, "reference_period")
        digest = _sha256(self.report_sha256, "report_sha256")
        for name in (
            "actual_receipts",
            "actual_outlays",
            "actual_balance",
            "revised_previous_balance",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an integer")
            if abs(value) > 5_000_000:
                raise ValueError(f"{name} is outside its bound")
        if self.actual_balance != self.actual_receipts - self.actual_outlays:
            raise ValueError("MTS override balance identity differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "report_sha256", digest)
        object.__setattr__(
            self, "locator", _required_text(self.locator, "locator")
        )
        expected = _stable_id("mts-value-override", self.identity_payload())
        if self.override_id and self.override_id != expected:
            raise ValueError("MTS value-override identity differs")
        object.__setattr__(self, "override_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "report_sha256": self.report_sha256,
            "actual_receipts": self.actual_receipts,
            "actual_outlays": self.actual_outlays,
            "actual_balance": self.actual_balance,
            "revised_previous_balance": self.revised_previous_balance,
            "locator": self.locator,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "override_id": self.override_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsValueOverrideV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            report_sha256=str(data.get("report_sha256", "")),
            actual_receipts=cast(int, data.get("actual_receipts")),
            actual_outlays=cast(int, data.get("actual_outlays")),
            actual_balance=cast(int, data.get("actual_balance")),
            revised_previous_balance=cast(
                int, data.get("revised_previous_balance")
            ),
            locator=str(data.get("locator", "")),
            override_id=str(data.get("override_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryMtsReviewedExtractsV1:
    """Hash-bound schedule review and exceptional table transcriptions."""

    timings: tuple[TreasuryMtsReleaseTimingV1, ...]
    value_overrides: tuple[TreasuryMtsValueOverrideV1, ...]
    review_method: str
    corpus_id: str = ""
    schema_version: str = MTS_REVIEWED_EXTRACTS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_REVIEWED_EXTRACTS_SCHEMA_VERSION:
            raise ValueError("unsupported MTS reviewed-extracts schema")
        timings = tuple(self.timings)
        overrides = tuple(self.value_overrides)
        expected_periods = _periods(
            MTS_FIRST_REFERENCE_PERIOD,
            MTS_LATEST_PACKAGED_REFERENCE_PERIOD,
        )
        if tuple(item.reference_period for item in timings) != expected_periods:
            raise ValueError("MTS timing inventory differs")
        if tuple(item.reference_period for item in overrides) != (
            "2023-09",
            "2024-01",
            "2024-04",
            "2024-05",
        ):
            raise ValueError("MTS value-override inventory differs")
        object.__setattr__(self, "timings", timings)
        object.__setattr__(self, "value_overrides", overrides)
        object.__setattr__(
            self,
            "review_method",
            _required_text(self.review_method, "review_method"),
        )
        expected = _stable_id("mts-reviewed-extracts", self.identity_payload())
        if self.corpus_id and self.corpus_id != expected:
            raise ValueError("MTS reviewed-extracts identity differs")
        object.__setattr__(self, "corpus_id", expected)

    @property
    def timing_by_period(self) -> Mapping[str, TreasuryMtsReleaseTimingV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.timings}
        )

    @property
    def override_by_period(self) -> Mapping[str, TreasuryMtsValueOverrideV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.value_overrides}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "timings": [item.to_dict() for item in self.timings],
            "value_overrides": [
                item.to_dict() for item in self.value_overrides
            ],
            "review_method": self.review_method,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "corpus_id": self.corpus_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TreasuryMtsReviewedExtractsV1:
        return cls(
            timings=tuple(
                TreasuryMtsReleaseTimingV1.from_dict(
                    _mapping(item, "MTS timing")
                )
                for item in _sequence(data.get("timings"), "timings")
            ),
            value_overrides=tuple(
                TreasuryMtsValueOverrideV1.from_dict(
                    _mapping(item, "MTS value override")
                )
                for item in _sequence(
                    data.get("value_overrides"), "value_overrides"
                )
            ),
            review_method=str(data.get("review_method", "")),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> TreasuryMtsReviewedExtractsV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("MTS reviewed extracts are invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "MTS reviewed extracts"))


@dataclass(frozen=True, slots=True)
class TreasuryMtsPublicationV1:
    """One point-in-time budget-balance triplet in millions of dollars."""

    reference_period: str
    previous_reference_period: str
    release_date: str
    release_time_local: str | None
    time_precision: EconomicTimePrecision
    released_at_ns: int | None
    timing_basis: MtsTimingBasis
    actual_balance_lexical: str
    actual_balance: int
    previous_as_known_lexical: str
    previous_as_known: int
    revised_previous_lexical: str
    revised_previous: int
    actual_receipts: int
    actual_outlays: int
    report: TreasuryMtsArtifactV1
    timing_id: str
    value_basis: str
    value_locator: str
    revision_locator: str
    publication_id: str = ""
    schema_version: str = MTS_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported MTS publication schema")
        period = _period(self.reference_period, "reference_period")
        previous = _period(
            self.previous_reference_period, "previous_reference_period"
        )
        if _next_period(previous) != period:
            raise ValueError("MTS publication periods are not consecutive")
        release_date = _iso_date(self.release_date, "release_date")
        local = _optional_text(self.release_time_local, "release_time_local")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if precision is EconomicTimePrecision.EXACT_MINUTE:
            if local is None or self.released_at_ns is None:
                raise ValueError("exact MTS publication lacks an instant")
            expected_ns = normalize_official_source_timestamp(
                f"{release_date}T{local}:00",
                MTS_SOURCE_TIMEZONE,
                EconomicTimePrecision.EXACT_MINUTE,
            ).utc_ns
            if self.released_at_ns != expected_ns:
                raise ValueError("MTS release instant differs")
        elif self.released_at_ns is not None or local is not None:
            raise ValueError("bounded MTS publication invents an instant")
        for lexical_name, value_name in (
            ("actual_balance_lexical", "actual_balance"),
            ("previous_as_known_lexical", "previous_as_known"),
            ("revised_previous_lexical", "revised_previous"),
        ):
            value = getattr(self, value_name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or abs(value) > 5_000_000
                or getattr(self, lexical_name) != _balance_lexical(value)
            ):
                raise ValueError(f"MTS {value_name} lexical value differs")
        if (
            abs(
                self.actual_balance
                - (self.actual_receipts - self.actual_outlays)
            )
            > 2
        ):
            raise ValueError("MTS current balance identity differs")
        if self.report.role != "report" or self.report.uri != _report_uri(
            period
        ):
            raise ValueError("MTS publication report differs")
        timing_id = _required_text(self.timing_id, "timing_id")
        if not timing_id.startswith("mts-release-timing:sha256:"):
            raise ValueError("MTS publication timing identity differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "previous_reference_period", previous)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "release_time_local", local)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(
            self, "timing_basis", MtsTimingBasis.from_value(self.timing_basis)
        )
        object.__setattr__(self, "timing_id", timing_id)
        for name in ("value_basis", "value_locator", "revision_locator"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        expected = _stable_id("mts-publication", self.identity_payload())
        if self.publication_id and self.publication_id != expected:
            raise ValueError("MTS publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def revision_delta(self) -> int:
        return self.revised_previous - self.previous_as_known

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "previous_reference_period": self.previous_reference_period,
            "release_date": self.release_date,
            "release_time_local": self.release_time_local,
            "time_precision": self.time_precision.value,
            "released_at_ns": self.released_at_ns,
            "timing_basis": self.timing_basis.value,
            "actual_balance_lexical": self.actual_balance_lexical,
            "actual_balance": self.actual_balance,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_as_known": self.previous_as_known,
            "revised_previous_lexical": self.revised_previous_lexical,
            "revised_previous": self.revised_previous,
            "actual_receipts": self.actual_receipts,
            "actual_outlays": self.actual_outlays,
            "report": self.report.to_dict(),
            "timing_id": self.timing_id,
            "value_basis": self.value_basis,
            "value_locator": self.value_locator,
            "revision_locator": self.revision_locator,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsPublicationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            release_date=str(data.get("release_date", "")),
            release_time_local=(
                None
                if data.get("release_time_local") is None
                else str(data.get("release_time_local"))
            ),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            released_at_ns=cast(int | None, data.get("released_at_ns")),
            timing_basis=MtsTimingBasis.from_value(
                str(data.get("timing_basis", ""))
            ),
            actual_balance_lexical=str(data.get("actual_balance_lexical", "")),
            actual_balance=cast(int, data.get("actual_balance")),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            previous_as_known=cast(int, data.get("previous_as_known")),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            revised_previous=cast(int, data.get("revised_previous")),
            actual_receipts=cast(int, data.get("actual_receipts")),
            actual_outlays=cast(int, data.get("actual_outlays")),
            report=TreasuryMtsArtifactV1.from_dict(
                _mapping(data.get("report"), "report")
            ),
            timing_id=str(data.get("timing_id", "")),
            value_basis=str(data.get("value_basis", "")),
            value_locator=str(data.get("value_locator", "")),
            revision_locator=str(data.get("revision_locator", "")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryMtsArchiveManifestV1:
    """Compact replay receipt for the complete qualified MTS corpus."""

    registry_id: str
    profile_id: str
    release_index: TreasuryMtsReleaseIndexV1
    reviewed_extracts_id: str
    predecessor: TreasuryMtsArtifactV1
    predecessor_balance: int
    publications: tuple[TreasuryMtsPublicationV1, ...]
    year_end_artifacts: tuple[TreasuryMtsArtifactV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    exact_minute_count: int
    inferred_bounded_count: int
    date_only_count: int
    revision_occurrence_count: int
    reviewed_value_count: int
    manifest_id: str = ""
    schema_version: str = MTS_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MTS_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported MTS archive-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("MTS registry identity differs")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("MTS profile identity differs")
        extracts_id = _required_text(
            self.reviewed_extracts_id, "reviewed_extracts_id"
        )
        if not extracts_id.startswith("mts-reviewed-extracts:sha256:"):
            raise ValueError("MTS reviewed-extracts identity differs")
        if (
            self.predecessor.role != "report"
            or self.predecessor.uri
            != _report_uri(MTS_PREDECESSOR_REFERENCE_PERIOD)
        ):
            raise ValueError("MTS predecessor differs")
        publications = tuple(self.publications)
        expected_periods = _periods(
            MTS_FIRST_REFERENCE_PERIOD,
            MTS_LATEST_PACKAGED_REFERENCE_PERIOD,
        )
        if tuple(item.reference_period for item in publications) != (
            expected_periods
        ):
            raise ValueError("MTS publication inventory differs")
        year_end = tuple(self.year_end_artifacts)
        if len(year_end) != 25 or any(
            item.role != "year-end-release" for item in year_end
        ):
            raise ValueError("MTS year-end evidence inventory differs")
        precision_counts = {
            EconomicTimePrecision.EXACT_MINUTE: self.exact_minute_count,
            EconomicTimePrecision.INFERRED_BOUNDED: (
                self.inferred_bounded_count
            ),
            EconomicTimePrecision.DATE_ONLY: self.date_only_count,
        }
        for precision, expected_count in precision_counts.items():
            if sum(
                item.time_precision is precision for item in publications
            ) != (expected_count):
                raise ValueError("MTS precision count differs")
        if sum(precision_counts.values()) != len(publications):
            raise ValueError("MTS total precision count differs")
        if self.revision_occurrence_count != sum(
            item.revision_delta != 0 for item in publications
        ):
            raise ValueError("MTS revision count differs")
        if self.reviewed_value_count != sum(
            item.value_basis == "reviewed-ocr-table-1" for item in publications
        ):
            raise ValueError("MTS reviewed-value count differs")
        artifacts = (
            self.release_index.catalog,
            self.predecessor,
            *(item.report for item in publications),
            *year_end,
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError("MTS raw-artifact count differs")
        if self.unique_content_sha256_count != len(
            {item.content_sha256 for item in artifacts}
        ):
            raise ValueError("MTS unique-hash count differs")
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError("MTS total byte count differs")
        _bounded_int(
            self.total_content_bytes,
            "total_content_bytes",
            MAX_MTS_TOTAL_BYTES,
        )
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "reviewed_extracts_id", extracts_id)
        object.__setattr__(self, "publications", publications)
        object.__setattr__(self, "year_end_artifacts", year_end)
        expected = _stable_id("mts-archive-manifest", self.identity_payload())
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("MTS archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_period(self) -> Mapping[str, TreasuryMtsPublicationV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.publications}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "reviewed_extracts_id": self.reviewed_extracts_id,
            "predecessor": self.predecessor.to_dict(),
            "predecessor_balance": self.predecessor_balance,
            "publications": [item.to_dict() for item in self.publications],
            "year_end_artifacts": [
                item.to_dict() for item in self.year_end_artifacts
            ],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": (self.unique_content_sha256_count),
            "total_content_bytes": self.total_content_bytes,
            "exact_minute_count": self.exact_minute_count,
            "inferred_bounded_count": self.inferred_bounded_count,
            "date_only_count": self.date_only_count,
            "revision_occurrence_count": self.revision_occurrence_count,
            "reviewed_value_count": self.reviewed_value_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TreasuryMtsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=TreasuryMtsReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            reviewed_extracts_id=str(data.get("reviewed_extracts_id", "")),
            predecessor=TreasuryMtsArtifactV1.from_dict(
                _mapping(data.get("predecessor"), "predecessor")
            ),
            predecessor_balance=cast(int, data.get("predecessor_balance")),
            publications=tuple(
                TreasuryMtsPublicationV1.from_dict(
                    _mapping(item, "MTS publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            year_end_artifacts=tuple(
                TreasuryMtsArtifactV1.from_dict(
                    _mapping(item, "year-end artifact")
                )
                for item in _sequence(
                    data.get("year_end_artifacts"), "year_end_artifacts"
                )
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            inferred_bounded_count=cast(
                int, data.get("inferred_bounded_count")
            ),
            date_only_count=cast(int, data.get("date_only_count")),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            reviewed_value_count=cast(int, data.get("reviewed_value_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> TreasuryMtsArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("MTS archive manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "MTS archive manifest"))


def _request(
    registry: OfficialSourceRegistryV1,
    *,
    uri: str,
    source_format: OfficialSourceFormat,
    window_start: str,
    window_end: str,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(MTS_SOURCE_KEY)
    if source.parser_id != MTS_PARSER_ID:
        raise ValueError("MTS source parser differs")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=window_start,
        window_end=window_end,
        page_number=page_number,
    )


def build_treasury_mts_catalog_request(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> OfficialSourceRequestV1:
    """Plan the exact FiscalData catalog-page receipt."""
    as_of = _iso_date(as_of_date, "as_of_date")
    return _request(
        registry,
        uri=MTS_CATALOG_URI,
        source_format=OfficialSourceFormat.JSON,
        window_start=f"{MTS_PREDECESSOR_REFERENCE_PERIOD}-01",
        window_end=as_of,
    )


def parse_treasury_mts_release_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> TreasuryMtsReleaseIndexV1:
    """Parse and select the continuous 1999-12 through 2026-08 PDFs."""
    if (
        not isinstance(snapshot, OfficialRawSnapshotV1)
        or snapshot.request.uri != MTS_CATALOG_URI
        or snapshot.request.source_key != MTS_SOURCE_KEY
        or snapshot.request.source_format is not OfficialSourceFormat.JSON
        or snapshot.status_code != 200
    ):
        raise ValueError("MTS catalog snapshot differs")
    try:
        payload = json.loads(snapshot.content)
        reports = payload["result"]["pageContext"]["config"]["publishedReports"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("MTS catalog structure differs") from exc
    by_period: dict[str, str] = {}
    for raw in _sequence(reports, "publishedReports"):
        report = _mapping(raw, "published report")
        match = _REPORT_RE.fullmatch(str(report.get("path", "")))
        if match is None:
            continue
        period = f"{match.group('year')}-{match.group('month')}"
        uri = _report_uri(period)
        if period in by_period and by_period[period] != uri:
            raise ValueError("MTS catalog repeats a period")
        by_period[period] = uri
    selected_periods = _periods(
        MTS_PREDECESSOR_REFERENCE_PERIOD,
        MTS_LATEST_PACKAGED_REFERENCE_PERIOD,
    )
    missing = tuple(item for item in selected_periods if item not in by_period)
    if missing:
        raise ValueError(f"MTS catalog is missing selected reports: {missing}")
    catalog = TreasuryMtsArtifactV1(
        role="catalog",
        uri=MTS_CATALOG_URI,
        source_format=OfficialSourceFormat.JSON,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )
    return TreasuryMtsReleaseIndexV1(
        as_of_date=as_of_date,
        catalog=catalog,
        entries=tuple(
            TreasuryMtsIndexEntryV1(
                reference_period=period, report_uri=by_period[period]
            )
            for period in selected_periods
        ),
    )


def build_treasury_mts_report_requests(
    registry: OfficialSourceRegistryV1,
    release_index: TreasuryMtsReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan predecessor and selected monthly report requests."""
    return tuple(
        _request(
            registry,
            uri=item.report_uri,
            source_format=OfficialSourceFormat.PDF,
            window_start=f"{item.reference_period}-01",
            window_end=f"{item.reference_period}-28",
            page_number=index,
        )
        for index, item in enumerate(release_index.entries, start=1)
    )


def _compact_months(value: str) -> str:
    result = value
    for month in _MONTHS[1:]:
        result = re.sub(
            r"\b" + r"\s*".join(month) + r"\b",
            month,
            result,
            flags=re.IGNORECASE,
        )
    return re.sub(r",\s+(?=\d)", ",", result)


def _number_variants(token: str) -> tuple[tuple[int, int], ...]:
    text = token.replace(",", "").replace("−", "-")
    sign = -1 if text.startswith("-") else 1
    digits = text.lstrip("+-")
    if not digits.isdigit():
        return ()
    return tuple(
        (sign * int(digits[stripped:]), stripped)
        for stripped in range(min(5, len(digits) - 1) + 1)
        if int(digits[stripped:]) <= 1_500_000
    )


def _parse_table_row(value: str) -> tuple[int, int, int]:
    tokens = _NUMBER_RE.findall(_compact_months(value))
    best: tuple[tuple[int, int, int, int], int, int, int] | None = None
    for indices in itertools.combinations(range(len(tokens)), 3):
        variants = tuple(_number_variants(tokens[index]) for index in indices)
        for receipts, outlays, balance in itertools.product(*variants):
            receipts_value, receipts_strip = receipts
            outlays_value, outlays_strip = outlays
            balance_value, balance_strip = balance
            if receipts_value < 10_000 or outlays_value < 10_000:
                continue
            error = abs(
                abs(outlays_value - receipts_value) - abs(balance_value)
            )
            if error > 2:
                continue
            skipped = sum(
                indices[index] - indices[index - 1] - 1 for index in range(1, 3)
            )
            score = (
                skipped,
                receipts_strip + outlays_strip + balance_strip,
                indices[-1],
                error,
            )
            candidate = (
                score,
                receipts_value,
                outlays_value,
                (
                    abs(balance_value)
                    if receipts_value >= outlays_value
                    else -abs(balance_value)
                ),
            )
            if best is None or candidate[0] < best[0]:
                best = candidate
    if best is None:
        raise ValueError("MTS Table 1 row cannot satisfy its identity")
    return best[1], best[2], best[3]


def _table_items(content: bytes) -> tuple[tuple[float, float, str], ...]:
    try:
        reader = PdfReader(BytesIO(content))
    except Exception as exc:
        raise ValueError("MTS report is not a readable PDF") from exc
    for page in reader.pages[:9]:
        try:
            text = page.extract_text() or ""
        except (KeyError, TypeError, ValueError):
            continue
        if (
            re.search(r"Table\s*1\.\s*Summary of Receipts", text, re.IGNORECASE)
            is None
        ):
            continue
        items: list[tuple[float, float, str]] = []

        def visit(
            fragment: str,
            _cm: Sequence[float],
            tm: Sequence[float],
            _font: Mapping[str, Any] | None,
            _size: float,
            target: list[tuple[float, float, str]] = items,
        ) -> None:
            normalized = " ".join(fragment.split())
            if normalized:
                target.append((float(tm[5]), float(tm[4]), normalized))

        page.extract_text(visitor_text=visit)
        return tuple(items)
    raise ValueError("MTS report lacks a readable Table 1")


def _table_row(
    items: Sequence[tuple[float, float, str]], reference_period: str
) -> tuple[int, int, int]:
    month = _MONTHS[int(reference_period[5:])]
    candidates: list[tuple[float, tuple[int, int, int]]] = []
    for y, x, fragment in items:
        if (
            x >= 180
            or re.match(
                rf"^\s*{month}\b", _compact_months(fragment), re.IGNORECASE
            )
            is None
        ):
            continue
        row = " ".join(
            _compact_months(item[2])
            for item in sorted(
                (item for item in items if abs(item[0] - y) <= 4.0),
                key=lambda item: item[1],
            )
        )
        try:
            candidates.append((y, _parse_table_row(row)))
        except ValueError:
            continue
    if not candidates:
        raise ValueError(f"MTS Table 1 lacks {reference_period}")
    return min(candidates, key=lambda item: item[0])[1]


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: str
) -> TreasuryMtsArtifactV1:
    return TreasuryMtsArtifactV1(
        role=role,
        uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def build_treasury_mts_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: TreasuryMtsReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    reviewed: TreasuryMtsReviewedExtractsV1,
) -> TreasuryMtsArchiveManifestV1:
    """Reparse every report and build the compact qualified manifest."""
    if profile.registry_id != registry.registry_id:
        raise ValueError("MTS registry and profile differ")
    program = profile.by_key.get(MTS_PROGRAM_KEY)
    if program is None or program.source_key != MTS_SOURCE_KEY:
        raise ValueError("MTS program source differs")
    required_report_uris = {item.report_uri for item in release_index.entries}
    evidence_uris = {
        item.evidence_uri
        for item in reviewed.timings
        if item.basis is MtsTimingBasis.YEAR_END_PRESS_RELEASE
    }
    if set(snapshots_by_uri) != required_report_uris | evidence_uris:
        raise ValueError("MTS retained snapshot inventory differs")
    reports: dict[str, TreasuryMtsArtifactV1] = {}
    rows: dict[str, tuple[int, int, int]] = {}
    table_items_by_period: dict[str, tuple[tuple[float, float, str], ...]] = {}
    for entry in release_index.entries:
        snapshot = snapshots_by_uri[entry.report_uri]
        if (
            snapshot.status_code != 200
            or snapshot.request.source_format is not OfficialSourceFormat.PDF
            or snapshot.request.source_key != MTS_SOURCE_KEY
        ):
            raise ValueError("MTS report snapshot differs")
        reports[entry.reference_period] = _artifact(snapshot, "report")
        override = reviewed.override_by_period.get(entry.reference_period)
        if override is not None:
            if override.report_sha256 != snapshot.content_sha256:
                raise ValueError("MTS reviewed table differs from its PDF")
            rows[entry.reference_period] = (
                override.actual_receipts,
                override.actual_outlays,
                override.actual_balance,
            )
        else:
            table_items = _table_items(snapshot.content)
            table_items_by_period[entry.reference_period] = table_items
            rows[entry.reference_period] = _table_row(
                table_items, entry.reference_period
            )
    year_end_artifacts = tuple(
        sorted(
            (
                _artifact(snapshots_by_uri[uri], "year-end-release")
                for uri in evidence_uris
            ),
            key=lambda item: item.uri,
        )
    )
    artifact_by_uri = {
        item.uri: item
        for item in (
            *reports.values(),
            *year_end_artifacts,
        )
    }
    publications: list[TreasuryMtsPublicationV1] = []
    for period in _periods(
        MTS_FIRST_REFERENCE_PERIOD, MTS_LATEST_PACKAGED_REFERENCE_PERIOD
    ):
        previous = _previous_period(period)
        timing = reviewed.timing_by_period[period]
        evidence = artifact_by_uri.get(timing.evidence_uri)
        if (
            evidence is None
            or evidence.content_sha256 != timing.evidence_sha256
        ):
            raise ValueError("MTS timing evidence differs from retained bytes")
        report = reports[period]
        receipts, outlays, actual = rows[period]
        previous_as_known = rows[previous][2]
        override = reviewed.override_by_period.get(period)
        if override is None:
            revised_previous = _table_row(
                table_items_by_period[period], previous
            )[2]
            basis = "native-pdf-table-1"
            value_locator = "release PDF Table 1 current fiscal-year row"
        else:
            revised_previous = override.revised_previous_balance
            basis = "reviewed-ocr-table-1"
            value_locator = override.locator
        publications.append(
            TreasuryMtsPublicationV1(
                reference_period=period,
                previous_reference_period=previous,
                release_date=timing.release_date,
                release_time_local=timing.release_time_local,
                time_precision=timing.time_precision,
                released_at_ns=timing.released_at_ns,
                timing_basis=timing.basis,
                actual_balance_lexical=_balance_lexical(actual),
                actual_balance=actual,
                previous_as_known_lexical=_balance_lexical(previous_as_known),
                previous_as_known=previous_as_known,
                revised_previous_lexical=_balance_lexical(revised_previous),
                revised_previous=revised_previous,
                actual_receipts=receipts,
                actual_outlays=outlays,
                report=report,
                timing_id=timing.timing_id,
                value_basis=basis,
                value_locator=value_locator,
                revision_locator=(
                    "release PDF Table 1 preceding-month row"
                    if override is None
                    else override.locator
                ),
            )
        )
    artifacts = (
        release_index.catalog,
        reports[MTS_PREDECESSOR_REFERENCE_PERIOD],
        *(item.report for item in publications),
        *year_end_artifacts,
    )
    return TreasuryMtsArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        reviewed_extracts_id=reviewed.corpus_id,
        predecessor=reports[MTS_PREDECESSOR_REFERENCE_PERIOD],
        predecessor_balance=rows[MTS_PREDECESSOR_REFERENCE_PERIOD][2],
        publications=tuple(publications),
        year_end_artifacts=year_end_artifacts,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        exact_minute_count=sum(
            item.time_precision is EconomicTimePrecision.EXACT_MINUTE
            for item in publications
        ),
        inferred_bounded_count=sum(
            item.time_precision is EconomicTimePrecision.INFERRED_BOUNDED
            for item in publications
        ),
        date_only_count=sum(
            item.time_precision is EconomicTimePrecision.DATE_ONLY
            for item in publications
        ),
        revision_occurrence_count=sum(
            item.revision_delta != 0 for item in publications
        ),
        reviewed_value_count=sum(
            item.value_basis == "reviewed-ocr-table-1" for item in publications
        ),
    )


def replay_treasury_mts_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    catalog_snapshot: OfficialRawSnapshotV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    reviewed: TreasuryMtsReviewedExtractsV1,
    expected: TreasuryMtsArchiveManifestV1,
) -> TreasuryMtsArchiveManifestV1:
    """Rebuild and compare every retained MTS artifact."""
    index = parse_treasury_mts_release_index(
        catalog_snapshot, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_treasury_mts_archive_manifest(
        registry, profile, index, snapshots_by_uri, reviewed
    )
    if rebuilt != expected:
        raise ValueError("MTS retained-corpus replay differs")
    return rebuilt


def treasury_mts_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: TreasuryMtsArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete occurrence-level MTS coverage."""
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("MTS manifest differs from the profile")
    count = len(manifest.publications)
    return UnitedStatesProgramCoverageV1(
        program_key=MTS_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=count,
        previous_as_known_count=count,
        revision_count=manifest.revision_occurrence_count,
        exact_minute_count=manifest.exact_minute_count,
        forecast_count=0,
        artifact_sha256s=tuple(
            item.report.content_sha256 for item in manifest.publications
        ),
        gap_reasons=(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,),
        notes=(
            f"Release index: {manifest.release_index.index_id}",
            f"Archive manifest: {manifest.manifest_id}",
            "Budget balance uses surplus-positive and deficit-negative signs in millions of dollars.",
            "The preceding PDF preserves previous-as-known; the current PDF Table 1 preserves revised previous.",
            "September year-end releases remain date-only, and six early annual schedule tables remain bounded without invented clocks.",
            "The current FiscalData API is a cross-check and does not replace retained occurrence PDFs.",
        ),
    )


def packaged_treasury_mts_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / "us_mts_archive_v1.json"


def packaged_treasury_mts_catalog_path() -> Path:
    return Path(__file__).with_name("assets") / "us_mts_catalog_v1.json"


def packaged_treasury_mts_reviewed_extracts_path() -> Path:
    return (
        Path(__file__).with_name("assets") / "us_mts_reviewed_extracts_v1.json"
    )


def load_packaged_treasury_mts_archive_manifest() -> (
    TreasuryMtsArchiveManifestV1
):
    """Load and validate the compact packaged MTS manifest."""
    return TreasuryMtsArchiveManifestV1.from_json(
        packaged_treasury_mts_manifest_path().read_text(encoding="utf-8")
    )


def load_packaged_treasury_mts_reviewed_extracts() -> (
    TreasuryMtsReviewedExtractsV1
):
    """Load and identity-check the packaged reviewed extracts."""
    reviewed = TreasuryMtsReviewedExtractsV1.from_json(
        packaged_treasury_mts_reviewed_extracts_path().read_text(
            encoding="utf-8"
        )
    )
    manifest = load_packaged_treasury_mts_archive_manifest()
    if reviewed.corpus_id != manifest.reviewed_extracts_id:
        raise ValueError("packaged MTS reviewed extracts differ")
    return reviewed


def load_packaged_treasury_mts_catalog() -> bytes:
    """Decode and hash-check the packaged FiscalData catalog receipt."""
    try:
        envelope = json.loads(
            packaged_treasury_mts_catalog_path().read_text(encoding="utf-8")
        )
        if (
            envelope.get("schema_version")
            != MTS_CATALOG_ENVELOPE_SCHEMA_VERSION
        ):
            raise ValueError("MTS catalog-envelope schema differs")
        content = base64.b64decode(envelope["content_base64"], validate=True)
    except (KeyError, OSError, UnicodeError, ValueError) as exc:
        raise ValueError("packaged MTS catalog is invalid") from exc
    artifact = (
        load_packaged_treasury_mts_archive_manifest().release_index.catalog
    )
    if (
        len(content) != artifact.content_length
        or hashlib.sha256(content).hexdigest() != artifact.content_sha256
    ):
        raise ValueError("packaged MTS catalog differs")
    return content


__all__ = [
    "MTS_API_TABLE_ID",
    "MTS_API_URI",
    "MTS_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "MTS_ARTIFACT_SCHEMA_VERSION",
    "MTS_CATALOG_ENVELOPE_SCHEMA_VERSION",
    "MTS_CATALOG_URI",
    "MTS_DATASET_ID",
    "MTS_DATASET_URI",
    "MTS_FIRST_REFERENCE_PERIOD",
    "MTS_INDEX_ENTRY_SCHEMA_VERSION",
    "MTS_LATEST_PACKAGED_REFERENCE_PERIOD",
    "MTS_PARSER_ID",
    "MTS_PREDECESSOR_REFERENCE_PERIOD",
    "MTS_PROGRAM_KEY",
    "MTS_PUBLICATION_SCHEMA_VERSION",
    "MTS_RELEASE_INDEX_SCHEMA_VERSION",
    "MTS_REPORT_URI_TEMPLATE",
    "MTS_REVIEWED_EXTRACTS_SCHEMA_VERSION",
    "MTS_SOURCE_KEY",
    "MTS_TIMING_SCHEMA_VERSION",
    "MTS_VALUE_OVERRIDE_SCHEMA_VERSION",
    "MtsTimingBasis",
    "TreasuryMtsArchiveManifestV1",
    "TreasuryMtsArtifactV1",
    "TreasuryMtsIndexEntryV1",
    "TreasuryMtsPublicationV1",
    "TreasuryMtsReleaseIndexV1",
    "TreasuryMtsReleaseTimingV1",
    "TreasuryMtsReviewedExtractsV1",
    "TreasuryMtsValueOverrideV1",
    "build_treasury_mts_archive_manifest",
    "build_treasury_mts_catalog_request",
    "build_treasury_mts_report_requests",
    "load_packaged_treasury_mts_archive_manifest",
    "load_packaged_treasury_mts_catalog",
    "load_packaged_treasury_mts_reviewed_extracts",
    "packaged_treasury_mts_catalog_path",
    "packaged_treasury_mts_manifest_path",
    "packaged_treasury_mts_reviewed_extracts_path",
    "parse_treasury_mts_release_index",
    "replay_treasury_mts_archive",
    "treasury_mts_coverage_from_manifest",
]

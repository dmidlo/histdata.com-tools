"""Point-in-time-safe CFTC Commitments of Traders positioning state.

Commitments of Traders is persistent weekly futures positioning, not a market
event and not spot-FX volume.  This module deliberately keeps COT outside the
``MarketContextEventV1`` timeline while reusing the same bounded acquisition,
content-addressed artifact, replay, preflight, and information-audit patterns.

The CFTC Public Reporting Environment (PRE) exposes current historical rows,
not a complete archive of every value as originally published.  Consequently,
PRE-derived rows remain ``current_state_only`` and cannot pass strict ex-ante
queries merely because an official publication time is known.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import re
import resource
import statistics
import sys
import time
import tempfile
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

import requests

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.information import (
    InformationInputKind,
    InformationMode,
    InformationScope,
    InformationSplitKind,
    InformationStage,
    ReconstructionInformationInputV1,
)

CFTC_POSITIONING_PROFILE_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-fetch-profile.v1"
)
CFTC_POSITIONING_SOURCE_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-source.v1"
)
CFTC_POSITIONING_MAPPING_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-symbol-mapping.v1"
)
CFTC_POSITIONING_RELEASE_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-release-evidence.v1"
)
CFTC_POSITIONING_SNAPSHOT_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-snapshot.v1"
)
CFTC_POSITIONING_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-coverage.v1"
)
CFTC_POSITIONING_ARCHIVE_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-archive-consistency.v1"
)
CFTC_POSITIONING_CORPUS_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-corpus.v1"
)
CFTC_POSITIONING_DIFF_SCHEMA_VERSION = "histdatacom.cftc-positioning-diff.v1"
CFTC_POSITIONING_PREFLIGHT_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-preflight.v1"
)
CFTC_POSITIONING_QUERY_SCHEMA_VERSION = "histdatacom.cftc-positioning-query.v1"
CFTC_POSITIONING_BINDING_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-consumer-binding.v1"
)
CFTC_POSITIONING_SMOKE_SCHEMA_VERSION = (
    "histdatacom.cftc-positioning-benchmark-smoke.v1"
)

CFTC_LEGACY_DATASET_ID = "srt6-5q2f"
CFTC_TFF_DATASET_ID = "udgc-27he"
CFTC_PRE_RESOURCE_TEMPLATE = (
    "https://publicreporting.cftc.gov/resource/{dataset_id}.json"
)
CFTC_PRE_METADATA_TEMPLATE = (
    "https://publicreporting.cftc.gov/api/views/{dataset_id}.json"
)
CFTC_RELEASE_SCHEDULE_URI = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm"
CFTC_SPECIAL_ANNOUNCEMENTS_URI = (
    "https://www.cftc.gov/MarketReports/CommitmentsofTraders/"
    "HistoricalSpecialAnnouncements/index.htm"
)
CFTC_HISTORICAL_COMPRESSED_URI = (
    "https://www.cftc.gov/MarketReports/CommitmentsofTraders/"
    "HistoricalCompressed/index.htm"
)
CFTC_2025_BACKLOG_URI = "https://www.cftc.gov/PressRoom/PressReleases/9147-25"
CFTC_WEB_POLICY_URI = "https://www.cftc.gov/WebPolicy/index.htm"
CME_FX_QUOTE_URI = (
    "https://www.cmegroup.com/education/courses/introduction-to-fx/"
    "understanding-fx-quote-conventions"
)
CME_EURGBP_RULE_URI = (
    "https://www.cmegroup.com/rulebook/CME/III/300/301/301.pdf"
)

CFTC_CONTRACT_CODES = ("096742", "099741", "299741")
CFTC_CONTRACT_SYMBOLS: Mapping[str, tuple[str, ...]] = {
    "096742": ("GBPUSD",),
    "099741": ("EURUSD",),
    "299741": ("EURGBP",),
}
CFTC_DIRECT_EURGBP_START = date(2014, 6, 10)
CFTC_TFF_START = date(2006, 6, 13)
CFTC_HISTORICAL_ARCHIVES = (
    ("legacy", "futures_only", "deacot1986_2016.zip"),
    ("legacy", "combined", "deahistfo_1995_2016.zip"),
    ("tff", "futures_only", "fin_fut_txt_2006_2016.zip"),
    ("tff", "combined", "fin_com_txt_2006_2016.zip"),
)
CFTC_HISTORICAL_ARCHIVE_TEMPLATE = (
    "https://www.cftc.gov/files/dea/history/{archive_name}"
)
DEFAULT_CFTC_USER_AGENT = "histdatacom-cftc-positioning/2.1.0 (+https://github.com/dmidlo/histdata.com-tools)"
CFTC_POSITIONING_ADAPTER_VERSION = "cftc-pre-positioning-adapter-v1"

DAY_NS = 86_400_000_000_000
MAX_CFTC_ROWS = 100_000
MAX_CFTC_SOURCES = 4096
MAX_CFTC_COVERAGE_SLICES = 2048
MAX_CFTC_VALUES = 160
MAX_CFTC_QUERY_SNAPSHOTS = 24
MAX_CFTC_QUERY_BYTES = 2 * 1024 * 1024
MAX_CFTC_CORPUS_BYTES = 128 * 1024 * 1024
MAX_CFTC_SOURCE_BYTES = 128 * 1024 * 1024
MAX_CFTC_TOTAL_SOURCE_BYTES = 384 * 1024 * 1024
MAX_CFTC_TEXT = 2048
MAX_CFTC_BINDING_VALUES = 256
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,191}$")
_CONTRACT_CODE_RE = re.compile(r"^[0-9A-Z+]{6}$")


class CftcReportFamily(str, Enum):
    """CFTC classification schema family."""

    LEGACY = "legacy"
    TFF = "tff"

    @classmethod
    def from_value(cls, value: str | "CftcReportFamily") -> "CftcReportFamily":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            raise ValueError("unsupported CFTC report family") from exc


class CftcReportScope(str, Enum):
    """Keep futures-only and futures/options-combined reports distinct."""

    FUTURES_ONLY = "futures_only"
    COMBINED = "combined"

    @classmethod
    def from_value(cls, value: str | "CftcReportScope") -> "CftcReportScope":
        if isinstance(value, cls):
            return value
        normalized = str(value).strip().lower().replace(" ", "_")
        if normalized in {"futonly", "futures", "futuresonly"}:
            normalized = cls.FUTURES_ONLY.value
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError("unsupported CFTC report scope") from exc


class CftcAvailabilityConfidence(str, Enum):
    """Strength of publication/knowledge-time evidence."""

    VERIFIED = "verified"
    NOMINAL = "nominal"
    UNKNOWN = "unknown"
    CORRECTION_QUALIFIED = "correction_qualified"
    RESTATEMENT_QUALIFIED = "restatement_qualified"

    @classmethod
    def from_value(
        cls, value: str | "CftcAvailabilityConfidence"
    ) -> "CftcAvailabilityConfidence":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            raise ValueError(
                "unsupported CFTC availability confidence"
            ) from exc


class CftcRestatementStatus(str, Enum):
    """Whether a row is an original publication vintage or current state."""

    CURRENT_STATE_ONLY = "current_state_only"
    ORIGINAL_VERIFIED = "original_verified"
    RESTATED_CURRENT_STATE = "restated_current_state"
    RESTATEMENT_INCOMPLETE = "restatement_incomplete"

    @classmethod
    def from_value(
        cls, value: str | "CftcRestatementStatus"
    ) -> "CftcRestatementStatus":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            raise ValueError("unsupported CFTC restatement status") from exc


class CftcMappingKind(str, Enum):
    """Direct contract state or explicit two-leg EURGBP composite."""

    DIRECT = "direct"
    DERIVED_TWO_LEG = "derived_two_leg"


class CftcPositioningQueryStatus(str, Enum):
    """Fail-closed latest-known-state query outcome."""

    READY = "ready"
    MISSING = "missing"
    STALE = "stale"
    NOT_AVAILABLE = "not_available_as_of"
    UNSUPPORTED = "unsupported"
    RESTATEMENT_INCOMPLETE = "restatement_incomplete"


class CftcPositioningConsumer(str, Enum):
    """Production seams that retain a positioning query identity."""

    BENCHMARK = "benchmark"
    MOTIF_SELECTION = "motif_selection"
    PLANNING = "planning"
    CARVING = "carving"


@dataclass(frozen=True, slots=True)
class CftcPositioningFetchProfileV1:
    """Bounded deterministic CFTC acquisition policy."""

    start_date: str
    end_date: str
    dataset_ids: tuple[str, ...] = (
        CFTC_LEGACY_DATASET_ID,
        CFTC_TFF_DATASET_ID,
    )
    contract_codes: tuple[str, ...] = CFTC_CONTRACT_CODES
    historical_archives: tuple[str, ...] = tuple(
        item[2] for item in CFTC_HISTORICAL_ARCHIVES
    )
    page_size: int = 1000
    max_pages_per_dataset: int = 128
    timeout_seconds: float = 45.0
    max_response_bytes: int = 32 * 1024 * 1024
    max_total_source_bytes: int = 256 * 1024 * 1024
    max_rows: int = MAX_CFTC_ROWS
    max_runtime_seconds: float = 600.0
    max_peak_memory_bytes: int = 2 * 1024**3
    max_staleness_days: int = 14
    user_agent: str = DEFAULT_CFTC_USER_AGENT
    schema_version: str = CFTC_POSITIONING_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning profile schema")
        start = _parse_date(self.start_date, "start_date")
        end = _parse_date(self.end_date, "end_date")
        if end < start:
            raise ValueError("CFTC positioning end_date precedes start_date")
        datasets = tuple(
            sorted({_required_text(item) for item in self.dataset_ids})
        )
        if set(datasets) != {CFTC_LEGACY_DATASET_ID, CFTC_TFF_DATASET_ID}:
            raise ValueError(
                "CFTC positioning requires Legacy and TFF datasets"
            )
        codes = tuple(
            sorted({_contract_code(item) for item in self.contract_codes})
        )
        if not set(CFTC_CONTRACT_CODES).issubset(codes):
            raise ValueError(
                "CFTC positioning requires EUR, GBP, and EURGBP codes"
            )
        archives = tuple(
            dict.fromkeys(
                _required_text(item) for item in self.historical_archives
            )
        )
        allowed_archives = {item[2] for item in CFTC_HISTORICAL_ARCHIVES}
        if not set(archives).issubset(allowed_archives):
            raise ValueError("unsupported CFTC historical archive")
        _bounded_int(self.page_size, "page_size", 1, 50_000)
        _bounded_int(
            self.max_pages_per_dataset, "max_pages_per_dataset", 1, 1024
        )
        _positive_float(self.timeout_seconds, "timeout_seconds")
        _bounded_int(
            self.max_response_bytes,
            "max_response_bytes",
            1,
            MAX_CFTC_SOURCE_BYTES,
        )
        _bounded_int(
            self.max_total_source_bytes,
            "max_total_source_bytes",
            1,
            MAX_CFTC_TOTAL_SOURCE_BYTES,
        )
        _bounded_int(self.max_rows, "max_rows", 1, MAX_CFTC_ROWS)
        _positive_float(self.max_runtime_seconds, "max_runtime_seconds")
        _bounded_int(
            self.max_peak_memory_bytes,
            "max_peak_memory_bytes",
            1,
            16 * 1024**3,
        )
        _bounded_int(self.max_staleness_days, "max_staleness_days", 1, 365)
        object.__setattr__(self, "start_date", start.isoformat())
        object.__setattr__(self, "end_date", end.isoformat())
        object.__setattr__(self, "dataset_ids", datasets)
        object.__setattr__(self, "contract_codes", codes)
        object.__setattr__(self, "historical_archives", archives)
        object.__setattr__(self, "user_agent", _required_text(self.user_agent))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "dataset_ids": list(self.dataset_ids),
            "contract_codes": list(self.contract_codes),
            "historical_archives": list(self.historical_archives),
            "page_size": self.page_size,
            "max_pages_per_dataset": self.max_pages_per_dataset,
            "timeout_seconds": self.timeout_seconds,
            "max_response_bytes": self.max_response_bytes,
            "max_total_source_bytes": self.max_total_source_bytes,
            "max_rows": self.max_rows,
            "max_runtime_seconds": self.max_runtime_seconds,
            "max_peak_memory_bytes": self.max_peak_memory_bytes,
            "max_staleness_days": self.max_staleness_days,
            "user_agent": self.user_agent,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CftcPositioningFetchProfileV1":
        _require_schema(data, CFTC_POSITIONING_PROFILE_SCHEMA_VERSION)
        return cls(
            start_date=str(data.get("start_date", "")),
            end_date=str(data.get("end_date", "")),
            dataset_ids=_string_tuple(data.get("dataset_ids")),
            contract_codes=_string_tuple(data.get("contract_codes")),
            historical_archives=_string_tuple(data.get("historical_archives")),
            page_size=_strict_int(data.get("page_size"), "page_size"),
            max_pages_per_dataset=_strict_int(
                data.get("max_pages_per_dataset"), "max_pages_per_dataset"
            ),
            timeout_seconds=_finite_float(
                data.get("timeout_seconds"), "timeout_seconds"
            ),
            max_response_bytes=_strict_int(
                data.get("max_response_bytes"), "max_response_bytes"
            ),
            max_total_source_bytes=_strict_int(
                data.get("max_total_source_bytes"), "max_total_source_bytes"
            ),
            max_rows=_strict_int(data.get("max_rows"), "max_rows"),
            max_runtime_seconds=_finite_float(
                data.get("max_runtime_seconds"), "max_runtime_seconds"
            ),
            max_peak_memory_bytes=_strict_int(
                data.get("max_peak_memory_bytes"), "max_peak_memory_bytes"
            ),
            max_staleness_days=_strict_int(
                data.get("max_staleness_days"), "max_staleness_days"
            ),
            user_agent=str(data.get("user_agent", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningRawSourceV1:
    """One retained official response plus deterministic query evidence."""

    source_key: str
    source_kind: str
    source_uri: str
    retrieved_at_ns: int
    content: bytes
    content_type: str
    query_parameters: Mapping[str, str] = field(default_factory=dict)
    dataset_id: str | None = None
    report_family: CftcReportFamily | None = None
    report_scope: CftcReportScope | None = None
    redistribution_allowed: bool = True
    limitations: tuple[str, ...] = ()
    schema_version: str = CFTC_POSITIONING_SOURCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_SOURCE_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC source schema")
        object.__setattr__(self, "source_key", _source_key(self.source_key))
        object.__setattr__(self, "source_kind", _source_key(self.source_kind))
        object.__setattr__(self, "source_uri", _required_text(self.source_uri))
        _bounded_int64(self.retrieved_at_ns, "retrieved_at_ns")
        if not isinstance(self.content, bytes) or not self.content:
            raise ValueError("CFTC source content must be non-empty bytes")
        if len(self.content) > MAX_CFTC_SOURCE_BYTES:
            raise ValueError("CFTC source exceeds per-response byte bound")
        object.__setattr__(
            self, "content_type", _required_text(self.content_type)
        )
        parameters = {
            _required_text(str(key)): _required_text(str(value))
            for key, value in sorted(self.query_parameters.items())
        }
        object.__setattr__(self, "query_parameters", parameters)
        dataset = _optional_text(self.dataset_id)
        if dataset is not None and dataset not in {
            CFTC_LEGACY_DATASET_ID,
            CFTC_TFF_DATASET_ID,
        }:
            raise ValueError("unsupported CFTC dataset ID")
        object.__setattr__(self, "dataset_id", dataset)
        if self.report_family is not None:
            object.__setattr__(
                self,
                "report_family",
                CftcReportFamily.from_value(self.report_family),
            )
        if self.report_scope is not None:
            object.__setattr__(
                self,
                "report_scope",
                CftcReportScope.from_value(self.report_scope),
            )
        if not isinstance(self.redistribution_allowed, bool):
            raise ValueError("redistribution_allowed must be boolean")
        object.__setattr__(
            self,
            "limitations",
            tuple(_required_text(item) for item in self.limitations),
        )

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def source_id(self) -> str:
        return _stable_id("cftc-positioning-source", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "adapter_version": CFTC_POSITIONING_ADAPTER_VERSION,
            "source_key": self.source_key,
            "source_kind": self.source_kind,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "size_bytes": len(self.content),
            "content_type": self.content_type,
            "query_parameters": dict(self.query_parameters),
            "dataset_id": self.dataset_id,
            "report_family": (
                self.report_family.value
                if self.report_family is not None
                else None
            ),
            "report_scope": (
                self.report_scope.value
                if self.report_scope is not None
                else None
            ),
            "redistribution_allowed": self.redistribution_allowed,
            "limitations": list(self.limitations),
        }

    def evidence_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "retrieved_at_ns": self.retrieved_at_ns,
            "source_id": self.source_id,
        }

    @classmethod
    def restore(
        cls, data: Mapping[str, Any], content: bytes
    ) -> "CftcPositioningRawSourceV1":
        _require_schema(data, CFTC_POSITIONING_SOURCE_SCHEMA_VERSION)
        if data.get("adapter_version") != CFTC_POSITIONING_ADAPTER_VERSION:
            raise ValueError("CFTC restored source adapter version differs")
        family = data.get("report_family")
        scope = data.get("report_scope")
        source = cls(
            source_key=str(data.get("source_key", "")),
            source_kind=str(data.get("source_kind", "")),
            source_uri=str(data.get("source_uri", "")),
            retrieved_at_ns=_strict_int(
                data.get("retrieved_at_ns"), "retrieved_at_ns"
            ),
            content=content,
            content_type=str(data.get("content_type", "")),
            query_parameters={
                str(key): str(value)
                for key, value in _mapping(data.get("query_parameters")).items()
            },
            dataset_id=_optional_text(data.get("dataset_id")),
            report_family=(
                CftcReportFamily.from_value(str(family))
                if family is not None
                else None
            ),
            report_scope=(
                CftcReportScope.from_value(str(scope))
                if scope is not None
                else None
            ),
            redistribution_allowed=_strict_bool(
                data.get("redistribution_allowed"), "redistribution_allowed"
            ),
            limitations=_string_tuple(data.get("limitations")),
            schema_version=str(data.get("schema_version", "")),
        )
        if source.content_sha256 != str(data.get("content_sha256", "")):
            raise ValueError("CFTC restored source hash differs")
        if len(content) != _strict_int(data.get("size_bytes"), "size_bytes"):
            raise ValueError("CFTC restored source size differs")
        if source.source_id != str(data.get("source_id", "")):
            raise ValueError("CFTC restored source identity differs")
        return source


@dataclass(frozen=True, slots=True)
class CftcPositioningSymbolMappingV1:
    """Versioned contract-code and quote-orientation evidence."""

    symbol: str
    contract_codes: tuple[str, ...]
    mapping_kind: CftcMappingKind
    quote_convention: str
    source_uris: tuple[str, ...]
    valid_from_date: str | None = None
    notes: tuple[str, ...] = ()
    mapping_id: str = ""
    schema_version: str = CFTC_POSITIONING_MAPPING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_MAPPING_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC symbol mapping schema")
        symbol = _normalized_symbol(self.symbol)
        codes = tuple(_contract_code(item) for item in self.contract_codes)
        kind = CftcMappingKind(self.mapping_kind)
        if kind is CftcMappingKind.DIRECT and len(codes) != 1:
            raise ValueError("direct CFTC mapping requires one contract")
        if kind is CftcMappingKind.DERIVED_TWO_LEG and (
            symbol != "EURGBP" or codes != ("099741", "096742")
        ):
            raise ValueError(
                "derived CFTC mapping must retain EUR and GBP legs"
            )
        valid_from = (
            _parse_date(self.valid_from_date, "valid_from_date").isoformat()
            if self.valid_from_date is not None
            else None
        )
        if symbol == "EURGBP" and kind is CftcMappingKind.DIRECT:
            if valid_from != CFTC_DIRECT_EURGBP_START.isoformat():
                raise ValueError("direct EURGBP mapping start date differs")
        uris = tuple(
            sorted({_required_text(item) for item in self.source_uris})
        )
        if not uris:
            raise ValueError("CFTC symbol mapping requires source URIs")
        notes = tuple(_required_text(item) for item in self.notes)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "contract_codes", codes)
        object.__setattr__(self, "mapping_kind", kind)
        object.__setattr__(
            self, "quote_convention", _required_text(self.quote_convention)
        )
        object.__setattr__(self, "source_uris", uris)
        object.__setattr__(self, "valid_from_date", valid_from)
        object.__setattr__(self, "notes", notes)
        expected = _stable_id("cftc-symbol-mapping", self.identity_payload())
        if self.mapping_id and self.mapping_id != expected:
            raise ValueError("CFTC symbol mapping identity differs")
        object.__setattr__(self, "mapping_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "contract_codes": list(self.contract_codes),
            "mapping_kind": self.mapping_kind.value,
            "quote_convention": self.quote_convention,
            "source_uris": list(self.source_uris),
            "valid_from_date": self.valid_from_date,
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "mapping_id": self.mapping_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CftcPositioningSymbolMappingV1":
        _require_schema(data, CFTC_POSITIONING_MAPPING_SCHEMA_VERSION)
        return cls(
            symbol=str(data.get("symbol", "")),
            contract_codes=_string_tuple(data.get("contract_codes")),
            mapping_kind=CftcMappingKind(str(data.get("mapping_kind", ""))),
            quote_convention=str(data.get("quote_convention", "")),
            source_uris=_string_tuple(data.get("source_uris")),
            valid_from_date=_optional_text(data.get("valid_from_date")),
            notes=_string_tuple(data.get("notes")),
            mapping_id=str(data.get("mapping_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def default_cftc_positioning_symbol_mappings() -> (
    tuple[CftcPositioningSymbolMappingV1, ...]
):
    """Return audited direction and direct/leg mapping contracts."""
    legacy = CFTC_PRE_METADATA_TEMPLATE.format(
        dataset_id=CFTC_LEGACY_DATASET_ID
    )
    tff = CFTC_PRE_METADATA_TEMPLATE.format(dataset_id=CFTC_TFF_DATASET_ID)
    datasets = (legacy, tff)
    return (
        CftcPositioningSymbolMappingV1(
            symbol="EURUSD",
            contract_codes=("099741",),
            mapping_kind=CftcMappingKind.DIRECT,
            quote_convention="USD per EUR",
            source_uris=(*datasets, CME_FX_QUOTE_URI),
            notes=("CFTC EUR FX futures positioning maps to EURUSD.",),
        ),
        CftcPositioningSymbolMappingV1(
            symbol="GBPUSD",
            contract_codes=("096742",),
            mapping_kind=CftcMappingKind.DIRECT,
            quote_convention="USD per GBP",
            source_uris=(*datasets, CME_FX_QUOTE_URI),
            notes=("CFTC British Pound futures positioning maps to GBPUSD.",),
        ),
        CftcPositioningSymbolMappingV1(
            symbol="EURGBP",
            contract_codes=("099741", "096742"),
            mapping_kind=CftcMappingKind.DERIVED_TWO_LEG,
            quote_convention=(
                "EURUSD and GBPUSD leg identities; no pooled composite position"
            ),
            source_uris=(*datasets, CME_FX_QUOTE_URI),
            notes=("Before direct support, retain both source-leg IDs.",),
        ),
        CftcPositioningSymbolMappingV1(
            symbol="EURGBP",
            contract_codes=("299741",),
            mapping_kind=CftcMappingKind.DIRECT,
            quote_convention="GBP per EUR",
            source_uris=(*datasets, CME_EURGBP_RULE_URI),
            valid_from_date=CFTC_DIRECT_EURGBP_START.isoformat(),
            notes=("Direct EURGBP CFTC state begins on 2014-06-10.",),
        ),
    )


@dataclass(frozen=True, slots=True)
class CftcReleaseEvidenceV1:
    """Publication/knowledge evidence for one report measurement date."""

    report_date: str
    confidence: CftcAvailabilityConfidence
    source_id: str
    publication_at_ns: int | None = None
    knowledge_at_ns: int | None = None
    restatement_detected_at_ns: int | None = None
    notes: tuple[str, ...] = ()
    evidence_id: str = ""
    schema_version: str = CFTC_POSITIONING_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC release evidence schema")
        report = _parse_date(self.report_date, "report_date")
        object.__setattr__(self, "report_date", report.isoformat())
        object.__setattr__(
            self,
            "confidence",
            CftcAvailabilityConfidence.from_value(self.confidence),
        )
        object.__setattr__(self, "source_id", _required_text(self.source_id))
        for name in (
            "publication_at_ns",
            "knowledge_at_ns",
            "restatement_detected_at_ns",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _bounded_int64(value, name))
        if self.publication_at_ns is not None and self.knowledge_at_ns is None:
            object.__setattr__(self, "knowledge_at_ns", self.publication_at_ns)
        if (
            self.knowledge_at_ns is not None
            and self.publication_at_ns is not None
            and self.knowledge_at_ns < self.publication_at_ns
        ):
            raise ValueError("CFTC knowledge time precedes publication")
        if self.confidence is CftcAvailabilityConfidence.UNKNOWN and (
            self.publication_at_ns is not None
            or self.knowledge_at_ns is not None
        ):
            raise ValueError(
                "unknown CFTC availability cannot invent timestamps"
            )
        notes = tuple(_required_text(item) for item in self.notes)
        object.__setattr__(self, "notes", notes)
        expected = _stable_id("cftc-release-evidence", self.identity_payload())
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("CFTC release evidence identity differs")
        object.__setattr__(self, "evidence_id", expected)

    @property
    def strict_ex_ante_time_eligible(self) -> bool:
        return (
            self.confidence is CftcAvailabilityConfidence.VERIFIED
            and self.publication_at_ns is not None
            and self.knowledge_at_ns is not None
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "report_date": self.report_date,
            "confidence": self.confidence.value,
            "source_id": self.source_id,
            "publication_at_ns": self.publication_at_ns,
            "knowledge_at_ns": self.knowledge_at_ns,
            "restatement_detected_at_ns": self.restatement_detected_at_ns,
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcReleaseEvidenceV1":
        _require_schema(data, CFTC_POSITIONING_RELEASE_SCHEMA_VERSION)
        return cls(
            report_date=str(data.get("report_date", "")),
            confidence=CftcAvailabilityConfidence.from_value(
                str(data.get("confidence", ""))
            ),
            source_id=str(data.get("source_id", "")),
            publication_at_ns=_optional_int(data.get("publication_at_ns")),
            knowledge_at_ns=_optional_int(data.get("knowledge_at_ns")),
            restatement_detected_at_ns=_optional_int(
                data.get("restatement_detected_at_ns")
            ),
            notes=_string_tuple(data.get("notes")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningSnapshotV1:
    """One immutable family/scope/contract/report-date positioning vector."""

    report_family: CftcReportFamily
    report_scope: CftcReportScope
    contract_code: str
    contract_name: str
    market_name: str
    report_date: str
    dataset_id: str
    source_id: str
    source_row_sha256: str
    pre_row_id: str
    release_evidence: CftcReleaseEvidenceV1
    restatement_status: CftcRestatementStatus
    values: Mapping[str, float]
    snapshot_id: str = ""
    schema_version: str = CFTC_POSITIONING_SNAPSHOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning snapshot schema")
        family = CftcReportFamily.from_value(self.report_family)
        scope = CftcReportScope.from_value(self.report_scope)
        code = _contract_code(self.contract_code)
        object.__setattr__(self, "report_family", family)
        object.__setattr__(self, "report_scope", scope)
        object.__setattr__(self, "contract_code", code)
        object.__setattr__(
            self, "contract_name", _required_text(self.contract_name)
        )
        object.__setattr__(
            self, "market_name", _required_text(self.market_name)
        )
        report = _parse_date(self.report_date, "report_date")
        object.__setattr__(self, "report_date", report.isoformat())
        expected_dataset = (
            CFTC_LEGACY_DATASET_ID
            if family is CftcReportFamily.LEGACY
            else CFTC_TFF_DATASET_ID
        )
        if self.dataset_id != expected_dataset:
            raise ValueError("CFTC snapshot dataset differs from report family")
        object.__setattr__(self, "source_id", _required_text(self.source_id))
        object.__setattr__(
            self,
            "source_row_sha256",
            _required_sha256(self.source_row_sha256, "source_row_sha256"),
        )
        object.__setattr__(self, "pre_row_id", _required_text(self.pre_row_id))
        if not isinstance(self.release_evidence, CftcReleaseEvidenceV1):
            raise ValueError("CFTC snapshot requires release evidence")
        if self.release_evidence.report_date != self.report_date:
            raise ValueError("CFTC release evidence report date differs")
        object.__setattr__(
            self,
            "restatement_status",
            CftcRestatementStatus.from_value(self.restatement_status),
        )
        values = {
            _source_key(str(key)): _finite_float(value, str(key))
            for key, value in sorted(self.values.items())
        }
        if not values or len(values) > MAX_CFTC_VALUES:
            raise ValueError("CFTC snapshot values are empty or unbounded")
        if "open_interest_all" not in values or values["open_interest_all"] < 0:
            raise ValueError("CFTC snapshot requires nonnegative open interest")
        object.__setattr__(self, "values", values)
        expected = _stable_id(
            "cftc-positioning-snapshot", self.identity_payload()
        )
        if self.snapshot_id and self.snapshot_id != expected:
            raise ValueError("CFTC snapshot identity differs")
        object.__setattr__(self, "snapshot_id", expected)

    @property
    def logical_key(self) -> str:
        return ":".join(
            (
                self.report_family.value,
                self.report_scope.value,
                self.contract_code,
                self.report_date,
            )
        )

    @property
    def measurement_start_ns(self) -> int:
        return _date_start_ns(_parse_date(self.report_date, "report_date"))

    @property
    def valid_from_ns(self) -> int | None:
        """Knowledge time when this state may begin to govern a query."""
        return self.release_evidence.knowledge_at_ns

    @property
    def strict_ex_ante_eligible(self) -> bool:
        return (
            self.release_evidence.strict_ex_ante_time_eligible
            and self.restatement_status
            is CftcRestatementStatus.ORIGINAL_VERIFIED
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "report_family": self.report_family.value,
            "report_scope": self.report_scope.value,
            "contract_code": self.contract_code,
            "contract_name": self.contract_name,
            "market_name": self.market_name,
            "report_date": self.report_date,
            "measurement_start_ns": self.measurement_start_ns,
            "valid_from_ns": self.valid_from_ns,
            "dataset_id": self.dataset_id,
            "source_id": self.source_id,
            "source_row_sha256": self.source_row_sha256,
            "pre_row_id": self.pre_row_id,
            "release_evidence": self.release_evidence.to_dict(),
            "restatement_status": self.restatement_status.value,
            "values": dict(self.values),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "logical_key": self.logical_key,
            "measurement_date_only": True,
            "snapshot_id": self.snapshot_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcPositioningSnapshotV1":
        _require_schema(data, CFTC_POSITIONING_SNAPSHOT_SCHEMA_VERSION)
        snapshot = cls(
            report_family=CftcReportFamily.from_value(
                str(data.get("report_family", ""))
            ),
            report_scope=CftcReportScope.from_value(
                str(data.get("report_scope", ""))
            ),
            contract_code=str(data.get("contract_code", "")),
            contract_name=str(data.get("contract_name", "")),
            market_name=str(data.get("market_name", "")),
            report_date=str(data.get("report_date", "")),
            dataset_id=str(data.get("dataset_id", "")),
            source_id=str(data.get("source_id", "")),
            source_row_sha256=str(data.get("source_row_sha256", "")),
            pre_row_id=str(data.get("pre_row_id", "")),
            release_evidence=CftcReleaseEvidenceV1.from_dict(
                _mapping(data.get("release_evidence"))
            ),
            restatement_status=CftcRestatementStatus.from_value(
                str(data.get("restatement_status", ""))
            ),
            values={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("values")).items()
            },
            snapshot_id=str(data.get("snapshot_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("logical_key") != snapshot.logical_key:
            raise ValueError("CFTC snapshot logical key differs")
        if data.get("measurement_date_only") is not True:
            raise ValueError("CFTC measurement date must remain date-only")
        if data.get("measurement_start_ns") != snapshot.measurement_start_ns:
            raise ValueError("CFTC measurement start differs from report date")
        if data.get("valid_from_ns") != snapshot.valid_from_ns:
            raise ValueError(
                "CFTC valid-from time differs from release evidence"
            )
        return snapshot


@dataclass(frozen=True, slots=True)
class CftcPositioningCoverageSliceV1:
    """Year/family/scope/contract coverage and missingness evidence."""

    year: int
    report_family: CftcReportFamily
    report_scope: CftcReportScope
    contract_code: str
    row_count: int
    first_report_date: str
    last_report_date: str
    missing_week_count: int
    duplicate_key_count: int
    contract_names: tuple[str, ...]
    market_names: tuple[str, ...]
    availability_counts: Mapping[str, int]
    restatement_counts: Mapping[str, int]
    source_hashes: tuple[str, ...]
    source_bytes: int
    processing_seconds: float
    schema_version: str = CFTC_POSITIONING_COVERAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_COVERAGE_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning coverage schema")
        _bounded_int(self.year, "year", 1900, 2200)
        object.__setattr__(
            self,
            "report_family",
            CftcReportFamily.from_value(self.report_family),
        )
        object.__setattr__(
            self, "report_scope", CftcReportScope.from_value(self.report_scope)
        )
        object.__setattr__(
            self, "contract_code", _contract_code(self.contract_code)
        )
        _bounded_int(self.row_count, "row_count", 1, MAX_CFTC_ROWS)
        first = _parse_date(self.first_report_date, "first_report_date")
        last = _parse_date(self.last_report_date, "last_report_date")
        if first.year != self.year or last.year != self.year or last < first:
            raise ValueError("CFTC coverage dates differ from year")
        object.__setattr__(self, "first_report_date", first.isoformat())
        object.__setattr__(self, "last_report_date", last.isoformat())
        _bounded_int(
            self.missing_week_count, "missing_week_count", 0, MAX_CFTC_ROWS
        )
        _bounded_int(
            self.duplicate_key_count, "duplicate_key_count", 0, MAX_CFTC_ROWS
        )
        names = tuple(
            sorted({_required_text(item) for item in self.contract_names})
        )
        markets = tuple(
            sorted({_required_text(item) for item in self.market_names})
        )
        if not names or not markets:
            raise ValueError("CFTC coverage requires contract and market names")
        object.__setattr__(self, "contract_names", names)
        object.__setattr__(self, "market_names", markets)
        availability = _count_mapping(
            self.availability_counts, "availability_counts"
        )
        restatements = _count_mapping(
            self.restatement_counts, "restatement_counts"
        )
        if sum(availability.values()) != self.row_count:
            raise ValueError("CFTC availability counts differ from rows")
        if sum(restatements.values()) != self.row_count:
            raise ValueError("CFTC restatement counts differ from rows")
        hashes = tuple(
            sorted(
                {
                    _required_sha256(item, "source_hash")
                    for item in self.source_hashes
                }
            )
        )
        if not hashes:
            raise ValueError("CFTC coverage requires source hashes")
        _bounded_int(
            self.source_bytes, "source_bytes", 0, MAX_CFTC_TOTAL_SOURCE_BYTES
        )
        _finite_nonnegative(self.processing_seconds, "processing_seconds")
        object.__setattr__(self, "availability_counts", availability)
        object.__setattr__(self, "restatement_counts", restatements)
        object.__setattr__(self, "source_hashes", hashes)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "year": self.year,
            "report_family": self.report_family.value,
            "report_scope": self.report_scope.value,
            "contract_code": self.contract_code,
            "row_count": self.row_count,
            "first_report_date": self.first_report_date,
            "last_report_date": self.last_report_date,
            "missing_week_count": self.missing_week_count,
            "duplicate_key_count": self.duplicate_key_count,
            "contract_names": list(self.contract_names),
            "market_names": list(self.market_names),
            "availability_counts": dict(self.availability_counts),
            "restatement_counts": dict(self.restatement_counts),
            "source_hashes": list(self.source_hashes),
            "source_bytes": self.source_bytes,
            "processing_seconds": self.processing_seconds,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CftcPositioningCoverageSliceV1":
        _require_schema(data, CFTC_POSITIONING_COVERAGE_SCHEMA_VERSION)
        return cls(
            year=_strict_int(data.get("year"), "year"),
            report_family=CftcReportFamily.from_value(
                str(data.get("report_family", ""))
            ),
            report_scope=CftcReportScope.from_value(
                str(data.get("report_scope", ""))
            ),
            contract_code=str(data.get("contract_code", "")),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            first_report_date=str(data.get("first_report_date", "")),
            last_report_date=str(data.get("last_report_date", "")),
            missing_week_count=_strict_int(
                data.get("missing_week_count"), "missing_week_count"
            ),
            duplicate_key_count=_strict_int(
                data.get("duplicate_key_count"), "duplicate_key_count"
            ),
            contract_names=_string_tuple(data.get("contract_names")),
            market_names=_string_tuple(data.get("market_names")),
            availability_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("availability_counts")
                ).items()
            },
            restatement_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("restatement_counts")
                ).items()
            },
            source_hashes=_string_tuple(data.get("source_hashes")),
            source_bytes=_strict_int(data.get("source_bytes"), "source_bytes"),
            processing_seconds=_finite_float(
                data.get("processing_seconds"), "processing_seconds"
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcArchiveConsistencyV1:
    """Comparison of PRE current state with official compressed history."""

    source_id: str
    report_family: CftcReportFamily
    report_scope: CftcReportScope
    selected_row_count: int
    matched_pre_rows: int
    missing_pre_rows: int
    open_interest_mismatch_count: int
    contract_name_change_count: int
    limitations: tuple[str, ...]
    evidence_id: str = ""
    schema_version: str = CFTC_POSITIONING_ARCHIVE_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != CFTC_POSITIONING_ARCHIVE_EVIDENCE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported CFTC archive evidence schema")
        object.__setattr__(self, "source_id", _required_text(self.source_id))
        object.__setattr__(
            self,
            "report_family",
            CftcReportFamily.from_value(self.report_family),
        )
        object.__setattr__(
            self, "report_scope", CftcReportScope.from_value(self.report_scope)
        )
        for name in (
            "selected_row_count",
            "matched_pre_rows",
            "missing_pre_rows",
            "open_interest_mismatch_count",
            "contract_name_change_count",
        ):
            _bounded_int(getattr(self, name), name, 0, MAX_CFTC_ROWS)
        if (
            self.matched_pre_rows + self.missing_pre_rows
            != self.selected_row_count
        ):
            raise ValueError("CFTC archive evidence counts do not reconcile")
        limitations = tuple(_required_text(item) for item in self.limitations)
        if not limitations:
            raise ValueError("CFTC archive evidence requires limitations")
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "cftc-archive-consistency", self.identity_payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("CFTC archive evidence identity differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "report_family": self.report_family.value,
            "report_scope": self.report_scope.value,
            "selected_row_count": self.selected_row_count,
            "matched_pre_rows": self.matched_pre_rows,
            "missing_pre_rows": self.missing_pre_rows,
            "open_interest_mismatch_count": self.open_interest_mismatch_count,
            "contract_name_change_count": self.contract_name_change_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcArchiveConsistencyV1":
        _require_schema(data, CFTC_POSITIONING_ARCHIVE_EVIDENCE_SCHEMA_VERSION)
        return cls(
            source_id=str(data.get("source_id", "")),
            report_family=CftcReportFamily.from_value(
                str(data.get("report_family", ""))
            ),
            report_scope=CftcReportScope.from_value(
                str(data.get("report_scope", ""))
            ),
            selected_row_count=_strict_int(
                data.get("selected_row_count"), "selected_row_count"
            ),
            matched_pre_rows=_strict_int(
                data.get("matched_pre_rows"), "matched_pre_rows"
            ),
            missing_pre_rows=_strict_int(
                data.get("missing_pre_rows"), "missing_pre_rows"
            ),
            open_interest_mismatch_count=_strict_int(
                data.get("open_interest_mismatch_count"),
                "open_interest_mismatch_count",
            ),
            contract_name_change_count=_strict_int(
                data.get("contract_name_change_count"),
                "contract_name_change_count",
            ),
            limitations=_string_tuple(data.get("limitations")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningCorpusV1:
    """Self-contained current-state corpus with immutable source lineage."""

    profile: CftcPositioningFetchProfileV1
    sources: tuple[Mapping[str, JSONValue], ...]
    symbol_mappings: tuple[CftcPositioningSymbolMappingV1, ...]
    snapshots: tuple[CftcPositioningSnapshotV1, ...]
    coverage: tuple[CftcPositioningCoverageSliceV1, ...]
    archive_consistency: tuple[CftcArchiveConsistencyV1, ...]
    duplicate_key_count: int
    runtime_seconds: float
    peak_memory_bytes: int
    limitations: tuple[str, ...]
    corpus_id: str = ""
    schema_version: str = CFTC_POSITIONING_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning corpus schema")
        if not isinstance(self.profile, CftcPositioningFetchProfileV1):
            raise ValueError("CFTC corpus requires a v1 profile")
        sources = tuple(
            sorted(
                (_json_value_mapping(item) for item in self.sources),
                key=lambda item: str(item.get("source_key", "")),
            )
        )
        if not sources or len(sources) > MAX_CFTC_SOURCES:
            raise ValueError("CFTC corpus sources are empty or unbounded")
        source_ids = {str(item.get("source_id", "")) for item in sources}
        if "" in source_ids or len(source_ids) != len(sources):
            raise ValueError("CFTC corpus source identities are invalid")
        mappings = tuple(
            sorted(
                self.symbol_mappings,
                key=lambda item: (item.symbol, item.mapping_kind.value),
            )
        )
        expected_mappings = {
            ("EURUSD", CftcMappingKind.DIRECT),
            ("GBPUSD", CftcMappingKind.DIRECT),
            ("EURGBP", CftcMappingKind.DIRECT),
            ("EURGBP", CftcMappingKind.DERIVED_TWO_LEG),
        }
        if {
            (item.symbol, item.mapping_kind) for item in mappings
        } != expected_mappings:
            raise ValueError("CFTC corpus symbol mappings are incomplete")
        if any(
            not set(item.contract_codes).issubset(self.profile.contract_codes)
            for item in mappings
        ):
            raise ValueError("CFTC symbol mapping exceeds profile contracts")
        snapshots = tuple(
            sorted(
                self.snapshots,
                key=lambda item: (
                    item.report_date,
                    item.report_family.value,
                    item.report_scope.value,
                    item.contract_code,
                ),
            )
        )
        if not snapshots or len(snapshots) > self.profile.max_rows:
            raise ValueError("CFTC corpus snapshots are empty or unbounded")
        keys = [item.logical_key for item in snapshots]
        if len(set(keys)) != len(keys):
            raise ValueError("CFTC corpus contains duplicate logical keys")
        if any(item.source_id not in source_ids for item in snapshots):
            raise ValueError("CFTC snapshot source is absent from corpus")
        coverage = tuple(
            sorted(
                self.coverage,
                key=lambda item: (
                    item.year,
                    item.report_family.value,
                    item.report_scope.value,
                    item.contract_code,
                ),
            )
        )
        if not coverage or len(coverage) > MAX_CFTC_COVERAGE_SLICES:
            raise ValueError("CFTC corpus coverage is empty or unbounded")
        expected_counts = Counter(
            (
                _parse_date(item.report_date, "report_date").year,
                item.report_family,
                item.report_scope,
                item.contract_code,
            )
            for item in snapshots
        )
        actual_counts = Counter(
            {
                (
                    item.year,
                    item.report_family,
                    item.report_scope,
                    item.contract_code,
                ): item.row_count
                for item in coverage
            }
        )
        if actual_counts != expected_counts:
            raise ValueError(
                "CFTC corpus coverage counts differ from snapshots"
            )
        archive = tuple(
            sorted(
                self.archive_consistency,
                key=lambda item: (
                    item.report_family.value,
                    item.report_scope.value,
                ),
            )
        )
        if len(archive) != len(self.profile.historical_archives):
            raise ValueError("CFTC archive evidence differs from profile")
        _bounded_int(
            self.duplicate_key_count,
            "duplicate_key_count",
            0,
            MAX_CFTC_ROWS,
        )
        _finite_nonnegative(self.runtime_seconds, "runtime_seconds")
        _bounded_int(
            self.peak_memory_bytes,
            "peak_memory_bytes",
            0,
            self.profile.max_peak_memory_bytes,
        )
        limitations = tuple(_required_text(item) for item in self.limitations)
        if not limitations:
            raise ValueError("CFTC corpus requires limitations")
        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "symbol_mappings", mappings)
        object.__setattr__(self, "snapshots", snapshots)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "archive_consistency", archive)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "cftc-positioning-corpus", self.identity_payload()
        )
        if self.corpus_id and self.corpus_id != expected:
            raise ValueError("CFTC corpus identity differs")
        object.__setattr__(self, "corpus_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "profile": self.profile.to_dict(),
            "sources": list(self.sources),
            "symbol_mappings": [
                item.to_dict() for item in self.symbol_mappings
            ],
            "snapshots": [item.to_dict() for item in self.snapshots],
            "coverage": [item.to_dict() for item in self.coverage],
            "archive_consistency": [
                item.to_dict() for item in self.archive_consistency
            ],
            "duplicate_key_count": self.duplicate_key_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "snapshot_count": len(self.snapshots),
            "source_count": len(self.sources),
            "source_bytes": sum(
                _strict_int(item.get("size_bytes"), "size_bytes")
                for item in self.sources
            ),
            "runtime_seconds": self.runtime_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "corpus_id": self.corpus_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcPositioningCorpusV1":
        _require_schema(data, CFTC_POSITIONING_CORPUS_SCHEMA_VERSION)
        corpus = cls(
            profile=CftcPositioningFetchProfileV1.from_dict(
                _mapping(data.get("profile"))
            ),
            sources=tuple(
                _json_value_mapping(item)
                for item in _mapping_sequence(data.get("sources"))
            ),
            symbol_mappings=tuple(
                CftcPositioningSymbolMappingV1.from_dict(item)
                for item in _mapping_sequence(data.get("symbol_mappings"))
            ),
            snapshots=tuple(
                CftcPositioningSnapshotV1.from_dict(item)
                for item in _mapping_sequence(data.get("snapshots"))
            ),
            coverage=tuple(
                CftcPositioningCoverageSliceV1.from_dict(item)
                for item in _mapping_sequence(data.get("coverage"))
            ),
            archive_consistency=tuple(
                CftcArchiveConsistencyV1.from_dict(item)
                for item in _mapping_sequence(data.get("archive_consistency"))
            ),
            duplicate_key_count=_strict_int(
                data.get("duplicate_key_count"), "duplicate_key_count"
            ),
            runtime_seconds=_finite_float(
                data.get("runtime_seconds"), "runtime_seconds"
            ),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes"), "peak_memory_bytes"
            ),
            limitations=_string_tuple(data.get("limitations")),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if _strict_int(data.get("snapshot_count"), "snapshot_count") != len(
            corpus.snapshots
        ):
            raise ValueError("CFTC corpus snapshot count differs")
        if _strict_int(data.get("source_count"), "source_count") != len(
            corpus.sources
        ):
            raise ValueError("CFTC corpus source count differs")
        return corpus


@dataclass(frozen=True, slots=True)
class CftcPositioningCorpusBuildV1:
    """In-memory corpus plus retained official responses for replay."""

    corpus: CftcPositioningCorpusV1
    raw_sources: tuple[CftcPositioningRawSourceV1, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.corpus, CftcPositioningCorpusV1):
            raise ValueError("CFTC build requires a v1 corpus")
        sources = tuple(
            sorted(self.raw_sources, key=lambda item: item.source_key)
        )
        evidence_ids = {
            str(item.get("source_id", "")) for item in self.corpus.sources
        }
        if {item.source_id for item in sources} != evidence_ids:
            raise ValueError("CFTC build sources differ from corpus evidence")
        if len({item.source_key for item in sources}) != len(sources):
            raise ValueError("CFTC build source keys must be unique")
        object.__setattr__(self, "raw_sources", sources)


@dataclass(frozen=True, slots=True)
class CftcPositioningDiffV1:
    """Bounded immutable refresh/restatement comparison."""

    previous_corpus_id: str
    current_corpus_id: str
    added_keys: tuple[str, ...]
    removed_keys: tuple[str, ...]
    changed_keys: tuple[str, ...]
    previous_snapshot_ids: Mapping[str, str]
    current_snapshot_ids: Mapping[str, str]
    diff_id: str = ""
    schema_version: str = CFTC_POSITIONING_DIFF_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_DIFF_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning diff schema")
        object.__setattr__(
            self, "previous_corpus_id", _required_text(self.previous_corpus_id)
        )
        object.__setattr__(
            self, "current_corpus_id", _required_text(self.current_corpus_id)
        )
        for name in ("added_keys", "removed_keys", "changed_keys"):
            values = tuple(
                sorted({_required_text(item) for item in getattr(self, name)})
            )
            if len(values) > MAX_CFTC_ROWS:
                raise ValueError("CFTC diff exceeds row bound")
            object.__setattr__(self, name, values)
        previous = _id_mapping(self.previous_snapshot_ids)
        current = _id_mapping(self.current_snapshot_ids)
        expected_keys = set(self.removed_keys) | set(self.changed_keys)
        if set(previous) != expected_keys:
            raise ValueError("CFTC diff previous snapshot IDs do not reconcile")
        expected_current = set(self.added_keys) | set(self.changed_keys)
        if set(current) != expected_current:
            raise ValueError("CFTC diff current snapshot IDs do not reconcile")
        object.__setattr__(self, "previous_snapshot_ids", previous)
        object.__setattr__(self, "current_snapshot_ids", current)
        expected = _stable_id("cftc-positioning-diff", self.identity_payload())
        if self.diff_id and self.diff_id != expected:
            raise ValueError("CFTC positioning diff identity differs")
        object.__setattr__(self, "diff_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "previous_corpus_id": self.previous_corpus_id,
            "current_corpus_id": self.current_corpus_id,
            "added_keys": list(self.added_keys),
            "removed_keys": list(self.removed_keys),
            "changed_keys": list(self.changed_keys),
            "previous_snapshot_ids": dict(self.previous_snapshot_ids),
            "current_snapshot_ids": dict(self.current_snapshot_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "diff_id": self.diff_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcPositioningDiffV1":
        _require_schema(data, CFTC_POSITIONING_DIFF_SCHEMA_VERSION)
        return cls(
            previous_corpus_id=str(data.get("previous_corpus_id", "")),
            current_corpus_id=str(data.get("current_corpus_id", "")),
            added_keys=_string_tuple(data.get("added_keys")),
            removed_keys=_string_tuple(data.get("removed_keys")),
            changed_keys=_string_tuple(data.get("changed_keys")),
            previous_snapshot_ids={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("previous_snapshot_ids")
                ).items()
            },
            current_snapshot_ids={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("current_snapshot_ids")
                ).items()
            },
            diff_id=str(data.get("diff_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningPreflightV1:
    """Explicit readiness decision for required window positioning state."""

    corpus_id: str
    start_ns: int
    end_ns: int
    information_mode: InformationMode
    as_of_ns: int | None
    symbols: tuple[str, ...]
    report_families: tuple[CftcReportFamily, ...]
    report_scopes: tuple[CftcReportScope, ...]
    ready: bool
    reasons: tuple[str, ...]
    schema_version: str = CFTC_POSITIONING_PREFLIGHT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_PREFLIGHT_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning preflight schema")
        object.__setattr__(self, "corpus_id", _required_text(self.corpus_id))
        start = _bounded_int64(self.start_ns, "start_ns")
        end = _bounded_int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("CFTC preflight end must follow start")
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if self.as_of_ns is None:
                raise ValueError("ex-ante CFTC preflight requires as_of_ns")
            as_of = _bounded_int64(self.as_of_ns, "as_of_ns")
            if as_of > start:
                raise ValueError(
                    "CFTC preflight as_of cannot follow window start"
                )
            object.__setattr__(self, "as_of_ns", as_of)
        elif self.as_of_ns is not None:
            raise ValueError("ex-post CFTC preflight does not accept as_of_ns")
        symbols = _normalized_symbols(self.symbols)
        families = tuple(
            sorted(
                {
                    CftcReportFamily.from_value(item)
                    for item in self.report_families
                },
                key=lambda item: item.value,
            )
        )
        scopes = tuple(
            sorted(
                {
                    CftcReportScope.from_value(item)
                    for item in self.report_scopes
                },
                key=lambda item: item.value,
            )
        )
        if not symbols or not families or not scopes:
            raise ValueError("CFTC preflight scope cannot be empty")
        if not isinstance(self.ready, bool):
            raise ValueError("CFTC preflight ready must be boolean")
        reasons = tuple(_required_text(item) for item in self.reasons)
        if self.ready == bool(reasons):
            raise ValueError("CFTC preflight readiness and reasons contradict")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "report_families", families)
        object.__setattr__(self, "report_scopes", scopes)
        object.__setattr__(self, "reasons", reasons)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "information_mode": self.information_mode.value,
            "as_of_ns": self.as_of_ns,
            "symbols": list(self.symbols),
            "report_families": [item.value for item in self.report_families],
            "report_scopes": [item.value for item in self.report_scopes],
            "ready": self.ready,
            "reasons": list(self.reasons),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcPositioningPreflightV1":
        _require_schema(data, CFTC_POSITIONING_PREFLIGHT_SCHEMA_VERSION)
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            as_of_ns=_optional_int(data.get("as_of_ns")),
            symbols=_string_tuple(data.get("symbols")),
            report_families=tuple(
                CftcReportFamily.from_value(str(item))
                for item in _sequence(data.get("report_families"))
            ),
            report_scopes=tuple(
                CftcReportScope.from_value(str(item))
                for item in _sequence(data.get("report_scopes"))
            ),
            ready=_strict_bool(data.get("ready"), "ready"),
            reasons=_string_tuple(data.get("reasons")),
            schema_version=str(data.get("schema_version", "")),
        )


class CftcPositioningPreflightError(ValueError):
    """Raised when required positioning context is unsupported."""

    def __init__(self, decision: CftcPositioningPreflightV1) -> None:
        self.decision = decision
        super().__init__(
            "CFTC positioning preflight failed: " + "; ".join(decision.reasons)
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningQueryV1:
    """Bounded latest-known positioning sidecar for one reconstruction window."""

    corpus_id: str
    information_mode: InformationMode
    start_ns: int
    end_ns: int
    as_of_ns: int | None
    symbols: tuple[str, ...]
    report_families: tuple[CftcReportFamily, ...]
    report_scopes: tuple[CftcReportScope, ...]
    snapshots: tuple[CftcPositioningSnapshotV1, ...]
    symbol_snapshot_ids: Mapping[str, tuple[str, ...]]
    mapping_kinds: Mapping[str, str]
    derived_values: Mapping[str, float]
    status: CftcPositioningQueryStatus
    reason: str
    age_seconds: Mapping[str, int]
    query_id: str = ""
    schema_version: str = CFTC_POSITIONING_QUERY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_QUERY_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning query schema")
        object.__setattr__(self, "corpus_id", _required_text(self.corpus_id))
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        if self.end_ns <= self.start_ns:
            raise ValueError("CFTC query end must follow start")
        _bounded_int64(self.start_ns, "start_ns")
        _bounded_int64(self.end_ns, "end_ns")
        if mode is InformationMode.EX_ANTE_SIMULATION:
            if self.as_of_ns is None:
                raise ValueError("ex-ante CFTC query requires as_of_ns")
            if self.as_of_ns > self.start_ns:
                raise ValueError("CFTC query as_of cannot follow window start")
            _bounded_int64(self.as_of_ns, "as_of_ns")
        elif self.as_of_ns is not None:
            raise ValueError("ex-post CFTC query does not accept as_of_ns")
        status = CftcPositioningQueryStatus(self.status)
        symbols = _normalized_symbols(self.symbols)
        families = tuple(
            sorted(
                {
                    CftcReportFamily.from_value(item)
                    for item in self.report_families
                },
                key=lambda item: item.value,
            )
        )
        scopes = tuple(
            sorted(
                {
                    CftcReportScope.from_value(item)
                    for item in self.report_scopes
                },
                key=lambda item: item.value,
            )
        )
        snapshots = tuple(
            sorted(self.snapshots, key=lambda item: item.snapshot_id)
        )
        if len(snapshots) > MAX_CFTC_QUERY_SNAPSHOTS:
            raise ValueError("CFTC query snapshot limit exceeded")
        if len({item.snapshot_id for item in snapshots}) != len(snapshots):
            raise ValueError("CFTC query snapshots must be unique")
        snapshot_ids = {item.snapshot_id for item in snapshots}
        symbol_ids: dict[str, tuple[str, ...]] = {}
        for symbol, values in sorted(self.symbol_snapshot_ids.items()):
            normalized = _normalized_symbol(symbol)
            ids = tuple(sorted({_required_text(item) for item in values}))
            if not ids or not set(ids).issubset(snapshot_ids):
                raise ValueError("CFTC symbol snapshot mapping is invalid")
            symbol_ids[normalized] = ids
        if set(symbol_ids) != set(symbols):
            if status is CftcPositioningQueryStatus.READY:
                raise ValueError("ready CFTC query lacks symbol snapshots")
        mapping_kinds = {
            _normalized_symbol(key): CftcMappingKind(value).value
            for key, value in sorted(self.mapping_kinds.items())
        }
        if set(mapping_kinds) != set(symbol_ids):
            raise ValueError("CFTC mapping kinds differ from symbol snapshots")
        derived = _metric_mapping(self.derived_values)
        reason = _required_text(self.reason)
        ages = {
            _required_text(key): _bounded_int(value, str(key), 0, 2**31 - 1)
            for key, value in sorted(self.age_seconds.items())
        }
        if not set(ages).issubset(snapshot_ids):
            raise ValueError("CFTC age evidence references unknown snapshots")
        if status is CftcPositioningQueryStatus.READY:
            if not snapshots or set(symbol_ids) != set(symbols):
                raise ValueError("ready CFTC query is incomplete")
            if reason != "ready":
                raise ValueError("ready CFTC query reason differs")
        else:
            if reason == "ready":
                raise ValueError("non-ready CFTC query requires refusal reason")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "report_families", families)
        object.__setattr__(self, "report_scopes", scopes)
        object.__setattr__(self, "snapshots", snapshots)
        object.__setattr__(self, "symbol_snapshot_ids", symbol_ids)
        object.__setattr__(self, "mapping_kinds", mapping_kinds)
        object.__setattr__(self, "derived_values", derived)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "age_seconds", ages)
        _ensure_payload_size(self.identity_payload(), MAX_CFTC_QUERY_BYTES)
        expected = _stable_id("cftc-positioning-query", self.identity_payload())
        if self.query_id and self.query_id != expected:
            raise ValueError("CFTC positioning query identity differs")
        object.__setattr__(self, "query_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "information_mode": self.information_mode.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "as_of_ns": self.as_of_ns,
            "symbols": list(self.symbols),
            "report_families": [item.value for item in self.report_families],
            "report_scopes": [item.value for item in self.report_scopes],
            "snapshots": [item.to_dict() for item in self.snapshots],
            "symbol_snapshot_ids": {
                key: list(value)
                for key, value in self.symbol_snapshot_ids.items()
            },
            "mapping_kinds": dict(self.mapping_kinds),
            "derived_values": dict(self.derived_values),
            "status": self.status.value,
            "reason": self.reason,
            "age_seconds": dict(self.age_seconds),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "query_id": self.query_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CftcPositioningQueryV1":
        _require_schema(data, CFTC_POSITIONING_QUERY_SCHEMA_VERSION)
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            as_of_ns=_optional_int(data.get("as_of_ns")),
            symbols=_string_tuple(data.get("symbols")),
            report_families=tuple(
                CftcReportFamily.from_value(str(item))
                for item in _sequence(data.get("report_families"))
            ),
            report_scopes=tuple(
                CftcReportScope.from_value(str(item))
                for item in _sequence(data.get("report_scopes"))
            ),
            snapshots=tuple(
                CftcPositioningSnapshotV1.from_dict(item)
                for item in _mapping_sequence(data.get("snapshots"))
            ),
            symbol_snapshot_ids={
                str(key): _string_tuple(value)
                for key, value in _mapping(
                    data.get("symbol_snapshot_ids")
                ).items()
            },
            mapping_kinds={
                str(key): str(value)
                for key, value in _mapping(data.get("mapping_kinds")).items()
            },
            derived_values={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("derived_values")).items()
            },
            status=CftcPositioningQueryStatus(str(data.get("status", ""))),
            reason=str(data.get("reason", "")),
            age_seconds={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(data.get("age_seconds")).items()
            },
            query_id=str(data.get("query_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningConsumerBindingV1:
    """Companion receipt binding an immutable v1 consumer to one query."""

    consumer: CftcPositioningConsumer
    consumer_artifact_id: str
    run_id: str
    window_id: str
    corpus_id: str
    query_id: str
    snapshot_ids: tuple[str, ...]
    information_input_ids: tuple[str, ...]
    state_label: str
    metrics: Mapping[str, float]
    binding_id: str = ""
    schema_version: str = CFTC_POSITIONING_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_BINDING_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning binding schema")
        object.__setattr__(
            self, "consumer", CftcPositioningConsumer(self.consumer)
        )
        for name in (
            "consumer_artifact_id",
            "run_id",
            "window_id",
            "corpus_id",
            "query_id",
            "state_label",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        snapshots = tuple(
            sorted({_required_text(item) for item in self.snapshot_ids})
        )
        inputs = tuple(
            sorted(
                {_required_text(item) for item in self.information_input_ids}
            )
        )
        if not snapshots or not inputs:
            raise ValueError("CFTC positioning binding requires lineage IDs")
        metrics = _metric_mapping(self.metrics)
        if len(metrics) > MAX_CFTC_BINDING_VALUES:
            raise ValueError("CFTC positioning binding metrics exceed limit")
        object.__setattr__(self, "snapshot_ids", snapshots)
        object.__setattr__(self, "information_input_ids", inputs)
        object.__setattr__(self, "metrics", metrics)
        expected = _stable_id(
            "cftc-positioning-binding", self.identity_payload()
        )
        if self.binding_id and self.binding_id != expected:
            raise ValueError("CFTC positioning binding identity differs")
        object.__setattr__(self, "binding_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "consumer": self.consumer.value,
            "consumer_artifact_id": self.consumer_artifact_id,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "corpus_id": self.corpus_id,
            "query_id": self.query_id,
            "snapshot_ids": list(self.snapshot_ids),
            "information_input_ids": list(self.information_input_ids),
            "state_label": self.state_label,
            "metrics": dict(self.metrics),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "binding_id": self.binding_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CftcPositioningConsumerBindingV1":
        _require_schema(data, CFTC_POSITIONING_BINDING_SCHEMA_VERSION)
        return cls(
            consumer=CftcPositioningConsumer(str(data.get("consumer", ""))),
            consumer_artifact_id=str(data.get("consumer_artifact_id", "")),
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            corpus_id=str(data.get("corpus_id", "")),
            query_id=str(data.get("query_id", "")),
            snapshot_ids=_string_tuple(data.get("snapshot_ids")),
            information_input_ids=_string_tuple(
                data.get("information_input_ids")
            ),
            state_label=str(data.get("state_label", "")),
            metrics={
                str(key): _finite_float(value, str(key))
                for key, value in _mapping(data.get("metrics")).items()
            },
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CftcPositioningBenchmarkSmokeV1:
    """Real-window deterministic benchmark-consumption evidence."""

    corpus_id: str
    query_id: str
    binding_id: str
    source_artifact_id: str
    source_sha256: str
    source_row_count: int
    benchmark_event_ids: tuple[str, ...]
    logical_output_sha256: str
    reload_output_sha256: str
    deterministic_reload: bool
    smoke_id: str = ""
    schema_version: str = CFTC_POSITIONING_SMOKE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CFTC_POSITIONING_SMOKE_SCHEMA_VERSION:
            raise ValueError("unsupported CFTC positioning smoke schema")
        for name in (
            "corpus_id",
            "query_id",
            "binding_id",
            "source_artifact_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "source_sha256",
            _required_sha256(self.source_sha256, "source_sha256"),
        )
        _bounded_int(self.source_row_count, "source_row_count", 1, 1_000_000)
        event_ids = tuple(
            _required_text(item) for item in self.benchmark_event_ids
        )
        if not event_ids or len(event_ids) != self.source_row_count:
            raise ValueError("CFTC smoke event IDs do not reconcile")
        object.__setattr__(self, "benchmark_event_ids", event_ids)
        for name in ("logical_output_sha256", "reload_output_sha256"):
            object.__setattr__(
                self,
                name,
                _required_sha256(getattr(self, name), name),
            )
        if not isinstance(self.deterministic_reload, bool):
            raise ValueError("deterministic_reload must be boolean")
        if self.deterministic_reload != (
            self.logical_output_sha256 == self.reload_output_sha256
        ):
            raise ValueError("CFTC smoke deterministic flag contradicts hashes")
        expected = _stable_id("cftc-positioning-smoke", self.identity_payload())
        if self.smoke_id and self.smoke_id != expected:
            raise ValueError("CFTC positioning smoke identity differs")
        object.__setattr__(self, "smoke_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "query_id": self.query_id,
            "binding_id": self.binding_id,
            "source_artifact_id": self.source_artifact_id,
            "source_sha256": self.source_sha256,
            "source_row_count": self.source_row_count,
            "benchmark_event_ids": list(self.benchmark_event_ids),
            "logical_output_sha256": self.logical_output_sha256,
            "reload_output_sha256": self.reload_output_sha256,
            "deterministic_reload": self.deterministic_reload,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "smoke_id": self.smoke_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CftcPositioningBenchmarkSmokeV1":
        _require_schema(data, CFTC_POSITIONING_SMOKE_SCHEMA_VERSION)
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            query_id=str(data.get("query_id", "")),
            binding_id=str(data.get("binding_id", "")),
            source_artifact_id=str(data.get("source_artifact_id", "")),
            source_sha256=str(data.get("source_sha256", "")),
            source_row_count=_strict_int(
                data.get("source_row_count"), "source_row_count"
            ),
            benchmark_event_ids=_string_tuple(data.get("benchmark_event_ids")),
            logical_output_sha256=str(data.get("logical_output_sha256", "")),
            reload_output_sha256=str(data.get("reload_output_sha256", "")),
            deterministic_reload=_strict_bool(
                data.get("deterministic_reload"), "deterministic_reload"
            ),
            smoke_id=str(data.get("smoke_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


class CftcSourceFetcherV1(Protocol):
    """Injectable bounded source fetch seam used by production and tests."""

    def __call__(
        self,
        url: str,
        *,
        parameters: Mapping[str, str],
        timeout_seconds: float,
        max_bytes: int,
        user_agent: str,
    ) -> tuple[bytes, str, str]: ...


def build_live_cftc_positioning_corpus(
    profile: CftcPositioningFetchProfileV1,
    *,
    fetcher: CftcSourceFetcherV1 | None = None,
    retrieved_at_ns: int | None = None,
) -> CftcPositioningCorpusBuildV1:
    """Acquire bounded official sources and build one replayable corpus."""
    if not isinstance(profile, CftcPositioningFetchProfileV1):
        raise ValueError("CFTC live build requires a v1 fetch profile")
    started = time.perf_counter()
    retrieved = (
        _bounded_int64(retrieved_at_ns, "retrieved_at_ns")
        if retrieved_at_ns is not None
        else int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    )
    get = fetcher or _fetch_http_source
    sources: list[CftcPositioningRawSourceV1] = []
    source_bytes = 0

    def acquire(
        *,
        source_key: str,
        source_kind: str,
        url: str,
        parameters: Mapping[str, str] | None = None,
        dataset_id: str | None = None,
        report_family: CftcReportFamily | None = None,
        report_scope: CftcReportScope | None = None,
        redistribution_allowed: bool = True,
        limitations: tuple[str, ...] = (),
    ) -> CftcPositioningRawSourceV1:
        nonlocal source_bytes
        params = dict(parameters or {})
        content, content_type, resolved_uri = get(
            url,
            parameters=params,
            timeout_seconds=profile.timeout_seconds,
            max_bytes=profile.max_response_bytes,
            user_agent=profile.user_agent,
        )
        source_bytes += len(content)
        if source_bytes > profile.max_total_source_bytes:
            raise ValueError("CFTC acquisition exceeds total source-byte limit")
        if time.perf_counter() - started > profile.max_runtime_seconds:
            raise ValueError("CFTC acquisition exceeds runtime limit")
        if _peak_memory_bytes() > profile.max_peak_memory_bytes:
            raise ValueError("CFTC acquisition exceeds peak-memory limit")
        source = CftcPositioningRawSourceV1(
            source_key=source_key,
            source_kind=source_kind,
            source_uri=resolved_uri,
            retrieved_at_ns=retrieved,
            content=content,
            content_type=content_type,
            query_parameters=params,
            dataset_id=dataset_id,
            report_family=report_family,
            report_scope=report_scope,
            redistribution_allowed=redistribution_allowed,
            limitations=limitations,
        )
        sources.append(source)
        return source

    family_by_dataset = {
        CFTC_LEGACY_DATASET_ID: CftcReportFamily.LEGACY,
        CFTC_TFF_DATASET_ID: CftcReportFamily.TFF,
    }
    for dataset_id in profile.dataset_ids:
        family = family_by_dataset[dataset_id]
        acquire(
            source_key=f"pre.{dataset_id}.metadata",
            source_kind="pre_metadata",
            url=CFTC_PRE_METADATA_TEMPLATE.format(dataset_id=dataset_id),
            dataset_id=dataset_id,
            report_family=family,
            limitations=(
                "PRE metadata describes the current dataset, not historical schema vintages.",
            ),
        )
        offset = 0
        for page_number in range(profile.max_pages_per_dataset):
            parameters = _pre_query_parameters(profile, offset=offset)
            source = acquire(
                source_key=f"pre.{dataset_id}.page-{page_number:04d}",
                source_kind="pre_data",
                url=CFTC_PRE_RESOURCE_TEMPLATE.format(dataset_id=dataset_id),
                parameters=parameters,
                dataset_id=dataset_id,
                report_family=family,
                limitations=(
                    "PRE history is current state and is not a complete original-vintage archive.",
                    "The PRE ID field is retained but is not used as the corpus primary key.",
                ),
            )
            rows = _json_rows(source.content, "PRE response")
            if len(rows) < profile.page_size:
                break
            offset += profile.page_size
        else:
            raise ValueError(
                "CFTC PRE pagination exceeded configured page limit"
            )

    official_pages = (
        (
            "official.release-schedule",
            "release_schedule",
            CFTC_RELEASE_SCHEDULE_URI,
            True,
            (
                "The schedule is tentative and usually maps Friday publication to the prior Tuesday.",
            ),
        ),
        (
            "official.special-announcements",
            "special_announcements",
            CFTC_SPECIAL_ANNOUNCEMENTS_URI,
            True,
            (
                "Special announcements are sparse and do not form a complete publication-vintage archive.",
            ),
        ),
        (
            "official.historical-compressed-index",
            "historical_index",
            CFTC_HISTORICAL_COMPRESSED_URI,
            True,
            (
                "Historical compressed files may be corrected after original publication.",
            ),
        ),
        (
            "official.2025-backlog",
            "backlog_schedule",
            CFTC_2025_BACKLOG_URI,
            True,
            (
                "The 2025 shutdown schedule supplies verified nonstandard publication dates.",
            ),
        ),
        (
            "official.web-policy",
            "license_policy",
            CFTC_WEB_POLICY_URI,
            True,
            (
                "CFTC requests acknowledgement when public-domain government information is reused.",
            ),
        ),
    )
    for (
        source_key,
        source_kind,
        uri,
        redistributable,
        limitations,
    ) in official_pages:
        acquire(
            source_key=source_key,
            source_kind=source_kind,
            url=uri,
            redistribution_allowed=redistributable,
            limitations=limitations,
        )

    archive_contracts = {item[2]: item[:2] for item in CFTC_HISTORICAL_ARCHIVES}
    for archive_name in profile.historical_archives:
        family_name, scope_name = archive_contracts[archive_name]
        acquire(
            source_key=f"official.archive.{archive_name.lower()}",
            source_kind="historical_archive",
            url=CFTC_HISTORICAL_ARCHIVE_TEMPLATE.format(
                archive_name=archive_name
            ),
            report_family=CftcReportFamily.from_value(family_name),
            report_scope=CftcReportScope.from_value(scope_name),
            limitations=(
                "Compressed history is current corrected state, not proof of the original publication vintage.",
            ),
        )

    return build_cftc_positioning_corpus_from_sources(
        sources,
        profile=profile,
        runtime_started=started,
    )


def build_cftc_positioning_corpus_from_sources(
    raw_sources: Sequence[CftcPositioningRawSourceV1],
    *,
    profile: CftcPositioningFetchProfileV1,
    runtime_started: float | None = None,
) -> CftcPositioningCorpusBuildV1:
    """Build a corpus deterministically from already-retained responses."""
    started = (
        time.perf_counter() if runtime_started is None else runtime_started
    )
    sources = tuple(sorted(raw_sources, key=lambda item: item.source_key))
    if not sources or len(sources) > MAX_CFTC_SOURCES:
        raise ValueError("CFTC source set is empty or unbounded")
    if len({item.source_key for item in sources}) != len(sources):
        raise ValueError("CFTC source keys must be unique")
    if (
        sum(len(item.content) for item in sources)
        > profile.max_total_source_bytes
    ):
        raise ValueError("CFTC source set exceeds total byte limit")
    _validate_required_sources(sources, profile)
    releases = _build_release_evidence(sources)
    duplicate_counts: Counter[tuple[str, str, str, str]] = Counter()
    candidates: dict[str, CftcPositioningSnapshotV1] = {}
    for source in sources:
        if source.source_kind != "pre_data":
            continue
        if source.dataset_id is None or source.report_family is None:
            raise ValueError("CFTC PRE page lacks dataset/family metadata")
        for row in _json_rows(source.content, "PRE response"):
            report_date = _parse_pre_report_date(
                str(row.get("report_date_as_yyyy_mm_dd", ""))
            )
            if not (
                _parse_date(profile.start_date, "start_date")
                <= report_date
                <= _parse_date(profile.end_date, "end_date")
            ):
                continue
            code = _contract_code(str(row.get("cftc_contract_market_code", "")))
            if code not in profile.contract_codes:
                continue
            scope = CftcReportScope.from_value(
                str(row.get("futonly_or_combined", ""))
            )
            key_tuple = (
                source.report_family.value,
                scope.value,
                code,
                report_date.isoformat(),
            )
            release = releases.get(report_date.isoformat())
            if release is None:
                release = _nominal_release_evidence(report_date, sources)
            row_hash = hashlib.sha256(
                canonical_contract_json(
                    cast(Mapping[str, JSONValue], _json_value_mapping(row))
                ).encode("utf-8")
            ).hexdigest()
            snapshot = CftcPositioningSnapshotV1(
                report_family=source.report_family,
                report_scope=scope,
                contract_code=code,
                contract_name=str(row.get("contract_market_name", "")).strip(),
                market_name=str(
                    row.get("market_and_exchange_names", "")
                ).strip(),
                report_date=report_date.isoformat(),
                dataset_id=source.dataset_id,
                source_id=source.source_id,
                source_row_sha256=row_hash,
                pre_row_id=str(row.get("id", "")),
                release_evidence=release,
                restatement_status=_restatement_status(
                    source.report_family, report_date
                ),
                values=_positioning_values(row),
            )
            existing = candidates.get(snapshot.logical_key)
            if existing is None:
                candidates[snapshot.logical_key] = snapshot
                continue
            duplicate_counts[key_tuple] += 1
            if (
                existing.values != snapshot.values
                or existing.contract_name != snapshot.contract_name
                or existing.market_name != snapshot.market_name
            ):
                raise ValueError(
                    "conflicting CFTC rows share family/scope/contract/report date"
                )
    snapshots = tuple(candidates.values())
    if not snapshots:
        raise ValueError("CFTC PRE sources contain no selected rows")
    if len(snapshots) > profile.max_rows:
        raise ValueError("CFTC selected rows exceed configured limit")
    source_evidence = tuple(source.evidence_dict() for source in sources)
    evidence_by_id = {source.source_id: source for source in sources}
    coverage = _build_cftc_coverage(
        snapshots,
        duplicate_counts=duplicate_counts,
        evidence_by_id=evidence_by_id,
    )
    archive_evidence = tuple(
        _archive_consistency(source, snapshots, profile)
        for source in sources
        if source.source_kind == "historical_archive"
    )
    elapsed = time.perf_counter() - started
    if elapsed > profile.max_runtime_seconds:
        raise ValueError("CFTC corpus build exceeds runtime limit")
    peak = _peak_memory_bytes()
    if peak > profile.max_peak_memory_bytes:
        raise ValueError("CFTC corpus build exceeds peak-memory limit")
    corpus = CftcPositioningCorpusV1(
        profile=profile,
        sources=source_evidence,
        symbol_mappings=default_cftc_positioning_symbol_mappings(),
        snapshots=snapshots,
        coverage=coverage,
        archive_consistency=archive_evidence,
        duplicate_key_count=sum(duplicate_counts.values()),
        runtime_seconds=round(elapsed, 6),
        peak_memory_bytes=int(peak),
        limitations=(
            "COT is futures positioning/open-interest context, not decentralized spot-FX volume, sentiment truth, or a causal event label.",
            "PRE and compressed history expose current corrected state and do not form a complete historical vintage archive.",
            "Nominal Friday publication estimates are excluded from strict ex-ante use.",
            "Legacy and TFF classifications and futures-only/combined scopes remain separate and cannot be pooled.",
            "Direct EURGBP state begins with contract 299741; earlier EURGBP state retains both EUR and GBP leg identities.",
            "No individual-trader inference, weekly interpolation, or automatic trading recommendation is supported.",
        ),
    )
    return CftcPositioningCorpusBuildV1(corpus=corpus, raw_sources=sources)


def _fetch_http_source(
    url: str,
    *,
    parameters: Mapping[str, str],
    timeout_seconds: float,
    max_bytes: int,
    user_agent: str,
) -> tuple[bytes, str, str]:
    """Fetch one official response with a strict byte cap."""
    last_error: requests.RequestException | None = None
    for _attempt in range(3):
        try:
            with requests.get(
                url,
                params=dict(parameters),
                headers={"User-Agent": user_agent, "Accept": "*/*"},
                timeout=timeout_seconds,
                stream=True,
            ) as response:
                response.raise_for_status()
                declared = response.headers.get("Content-Length")
                if declared is not None and int(declared) > max_bytes:
                    raise ValueError(
                        "CFTC response exceeds declared byte limit"
                    )
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError("CFTC response exceeds byte limit")
                    chunks.append(chunk)
                content = b"".join(chunks)
                if not content:
                    raise ValueError("CFTC response is empty")
                content_type = response.headers.get(
                    "Content-Type", "application/octet-stream"
                )
                return (
                    content,
                    content_type.split(";", 1)[0].strip(),
                    response.url,
                )
        except requests.RequestException as exc:
            last_error = exc
    if last_error is None:
        raise RuntimeError("CFTC fetch retry state is invalid")
    raise last_error


def _pre_query_parameters(
    profile: CftcPositioningFetchProfileV1, *, offset: int
) -> dict[str, str]:
    quoted_codes = ",".join(f"'{item}'" for item in profile.contract_codes)
    return {
        "$where": (
            f"cftc_contract_market_code in ({quoted_codes}) "
            "AND report_date_as_yyyy_mm_dd between "
            f"'{profile.start_date}T00:00:00.000' and "
            f"'{profile.end_date}T23:59:59.999'"
        ),
        "$order": (
            "report_date_as_yyyy_mm_dd,cftc_contract_market_code,futonly_or_combined,id"
        ),
        "$limit": str(profile.page_size),
        "$offset": str(offset),
    }


def compare_cftc_positioning_corpora(
    previous: CftcPositioningCorpusV1,
    current: CftcPositioningCorpusV1,
) -> CftcPositioningDiffV1:
    """Compare logical rows without confusing a refresh with a restatement."""
    previous_by_key = {item.logical_key: item for item in previous.snapshots}
    current_by_key = {item.logical_key: item for item in current.snapshots}
    previous_keys = set(previous_by_key)
    current_keys = set(current_by_key)
    added = tuple(sorted(current_keys - previous_keys))
    removed = tuple(sorted(previous_keys - current_keys))
    changed = tuple(
        sorted(
            key
            for key in previous_keys & current_keys
            if previous_by_key[key].source_row_sha256
            != current_by_key[key].source_row_sha256
        )
    )
    return CftcPositioningDiffV1(
        previous_corpus_id=previous.corpus_id,
        current_corpus_id=current.corpus_id,
        added_keys=added,
        removed_keys=removed,
        changed_keys=changed,
        previous_snapshot_ids={
            key: previous_by_key[key].snapshot_id for key in removed + changed
        },
        current_snapshot_ids={
            key: current_by_key[key].snapshot_id for key in added + changed
        },
    )


def write_cftc_positioning_corpus(
    build: CftcPositioningCorpusBuildV1,
    directory: str | Path,
    *,
    previous_corpus: CftcPositioningCorpusV1 | None = None,
) -> Mapping[str, ArtifactRef]:
    """Write raw, corpus, coverage, archive, and optional diff artifacts once."""
    if not isinstance(build, CftcPositioningCorpusBuildV1):
        raise ValueError("CFTC artifact writer requires a v1 corpus build")
    root = Path(directory).expanduser().resolve()
    raw_root = root / "sources"
    raw_root.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, ArtifactRef] = {}
    for source in build.raw_sources:
        suffix = _content_suffix(source.content_type, source.source_uri)
        source_path = (
            raw_root / f"{source.source_key}-{source.content_sha256}{suffix}"
        )
        _write_once(source_path, source.content)
        artifacts[f"source:{source.source_key}"] = ArtifactRef(
            kind="cftc_positioning_source_v1",
            path=str(source_path),
            size_bytes=len(source.content),
            sha256=source.content_sha256,
            metadata={
                "source_id": source.source_id,
                "source_uri": source.source_uri,
                "retrieved_at_ns": source.retrieved_at_ns,
                "redistribution_allowed": source.redistribution_allowed,
            },
        )

    artifacts["coverage"] = _write_json_artifact(
        root,
        "cftc-positioning-coverage",
        "cftc_positioning_coverage_v1",
        {
            "schema_version": CFTC_POSITIONING_COVERAGE_SCHEMA_VERSION,
            "corpus_id": build.corpus.corpus_id,
            "coverage": [item.to_dict() for item in build.corpus.coverage],
        },
        metadata={"corpus_id": build.corpus.corpus_id},
    )
    artifacts["archive_consistency"] = _write_json_artifact(
        root,
        "cftc-positioning-archive-consistency",
        "cftc_positioning_archive_consistency_v1",
        {
            "schema_version": CFTC_POSITIONING_ARCHIVE_EVIDENCE_SCHEMA_VERSION,
            "corpus_id": build.corpus.corpus_id,
            "archive_consistency": [
                item.to_dict() for item in build.corpus.archive_consistency
            ],
        },
        metadata={"corpus_id": build.corpus.corpus_id},
    )
    artifacts["corpus"] = _write_json_artifact(
        root,
        "cftc-positioning-corpus",
        "cftc_positioning_corpus_v1",
        build.corpus.to_dict(),
        metadata={
            "corpus_id": build.corpus.corpus_id,
            "snapshot_count": len(build.corpus.snapshots),
        },
    )
    if previous_corpus is not None:
        diff = compare_cftc_positioning_corpora(previous_corpus, build.corpus)
        artifacts["diff"] = _write_json_artifact(
            root,
            "cftc-positioning-diff",
            "cftc_positioning_diff_v1",
            diff.to_dict(),
            metadata={"diff_id": diff.diff_id},
        )
    return artifacts


def read_cftc_positioning_corpus(path: str | Path) -> CftcPositioningCorpusV1:
    """Load and hash-verify one self-contained corpus artifact."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_CFTC_CORPUS_BYTES:
        raise ValueError("CFTC corpus artifact exceeds size bound")
    match = re.fullmatch(
        r"cftc-positioning-corpus-([0-9a-f]{64})\.json", source.name
    )
    if match is None:
        raise ValueError("CFTC corpus artifact name is not content addressed")
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError("CFTC corpus artifact hash differs from name")
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("CFTC corpus artifact is invalid JSON") from exc
    return CftcPositioningCorpusV1.from_dict(_mapping(payload))


def replay_cftc_positioning_corpus(
    corpus_path: str | Path,
    *,
    source_directory: str | Path | None = None,
) -> CftcPositioningCorpusBuildV1:
    """Rebuild exactly from retained responses and verify corpus identity."""
    corpus = read_cftc_positioning_corpus(corpus_path)
    root = (
        Path(source_directory).expanduser().resolve()
        if source_directory is not None
        else Path(corpus_path).expanduser().resolve().parent / "sources"
    )
    restored: list[CftcPositioningRawSourceV1] = []
    for item in corpus.sources:
        source_key = str(item.get("source_key", ""))
        content_hash = str(item.get("content_sha256", ""))
        matches = sorted(root.glob(f"{source_key}-{content_hash}.*"))
        if len(matches) != 1:
            raise ValueError(
                f"expected one retained CFTC source for {source_key}"
            )
        content = matches[0].read_bytes()
        if len(content) > MAX_CFTC_SOURCE_BYTES:
            raise ValueError("retained CFTC source exceeds size bound")
        restored.append(CftcPositioningRawSourceV1.restore(item, content))
    rebuilt = build_cftc_positioning_corpus_from_sources(
        restored, profile=corpus.profile
    )
    if rebuilt.corpus.corpus_id != corpus.corpus_id:
        raise ValueError("replayed CFTC positioning corpus identity differs")
    return rebuilt


def query_cftc_positioning_corpus(
    corpus: CftcPositioningCorpusV1,
    *,
    start_ns: int,
    end_ns: int,
    information_mode: InformationMode,
    as_of_ns: int | None = None,
    symbols: Sequence[str] = ("EURUSD", "GBPUSD", "EURGBP"),
    report_families: Sequence[CftcReportFamily] = (
        CftcReportFamily.LEGACY,
        CftcReportFamily.TFF,
    ),
    report_scopes: Sequence[CftcReportScope] = (
        CftcReportScope.FUTURES_ONLY,
        CftcReportScope.COMBINED,
    ),
    max_staleness_days: int | None = None,
) -> CftcPositioningQueryV1:
    """Select latest known weekly state, keeping all report schemas separate."""
    mode = InformationMode.from_value(information_mode)
    selected_symbols = _normalized_symbols(symbols)
    families = tuple(
        sorted(
            {CftcReportFamily.from_value(item) for item in report_families},
            key=lambda item: item.value,
        )
    )
    scopes = tuple(
        sorted(
            {CftcReportScope.from_value(item) for item in report_scopes},
            key=lambda item: item.value,
        )
    )
    _validate_query_interval(start_ns, end_ns, mode, as_of_ns)
    if not selected_symbols or not families or not scopes:
        raise ValueError("CFTC query scope cannot be empty")
    supported_symbols = {item.symbol for item in corpus.symbol_mappings}
    unsupported_symbols = sorted(set(selected_symbols) - supported_symbols)
    if unsupported_symbols:
        return CftcPositioningQueryV1(
            corpus_id=corpus.corpus_id,
            information_mode=mode,
            start_ns=start_ns,
            end_ns=end_ns,
            as_of_ns=as_of_ns,
            symbols=selected_symbols,
            report_families=families,
            report_scopes=scopes,
            snapshots=(),
            symbol_snapshot_ids={},
            mapping_kinds={},
            derived_values={},
            status=CftcPositioningQueryStatus.UNSUPPORTED,
            reason="unsupported positioning symbols: "
            + ", ".join(unsupported_symbols),
            age_seconds={},
        )
    stale_days = (
        corpus.profile.max_staleness_days
        if max_staleness_days is None
        else _bounded_int(max_staleness_days, "max_staleness_days", 1, 365)
    )
    window_date = datetime.fromtimestamp(
        start_ns / 1_000_000_000, timezone.utc
    ).date()
    cutoff_ns = as_of_ns if mode is InformationMode.EX_ANTE_SIMULATION else None
    selected: dict[str, CftcPositioningSnapshotV1] = {}
    symbol_ids: dict[str, set[str]] = defaultdict(set)
    mapping_kinds: dict[str, str] = {}
    ages: dict[str, int] = {}
    missing: list[str] = []
    unavailable: list[str] = []
    restatement_incomplete: list[str] = []
    stale: list[str] = []

    for symbol in selected_symbols:
        symbol_mapping = _select_symbol_mapping(corpus, symbol, window_date)
        codes = symbol_mapping.contract_codes
        mapping_kinds[symbol] = symbol_mapping.mapping_kind.value
        for family in families:
            if family is CftcReportFamily.TFF and window_date < CFTC_TFF_START:
                missing.append(f"{symbol}/{family.value}:not-yet-published")
                continue
            for scope in scopes:
                for code in codes:
                    candidates = sorted(
                        (
                            item
                            for item in corpus.snapshots
                            if item.report_family is family
                            and item.report_scope is scope
                            and item.contract_code == code
                            and item.measurement_start_ns <= start_ns
                        ),
                        key=lambda item: item.measurement_start_ns,
                        reverse=True,
                    )
                    seam = f"{symbol}/{family.value}/{scope.value}/{code}"
                    if not candidates:
                        missing.append(seam)
                        continue
                    if mode is InformationMode.EX_ANTE_SIMULATION:
                        time_eligible = [
                            item
                            for item in candidates
                            if item.release_evidence.strict_ex_ante_time_eligible
                            and item.release_evidence.knowledge_at_ns
                            is not None
                            and cutoff_ns is not None
                            and item.release_evidence.knowledge_at_ns
                            <= cutoff_ns
                        ]
                        if not time_eligible:
                            unavailable.append(seam)
                            continue
                        vintage_eligible = [
                            item
                            for item in time_eligible
                            if item.strict_ex_ante_eligible
                        ]
                        if not vintage_eligible:
                            restatement_incomplete.append(seam)
                            continue
                        candidate = vintage_eligible[0]
                    else:
                        candidate = candidates[0]
                    age_seconds = max(
                        0,
                        (start_ns - candidate.measurement_start_ns)
                        // 1_000_000_000,
                    )
                    selected[candidate.snapshot_id] = candidate
                    symbol_ids[symbol].add(candidate.snapshot_id)
                    ages[candidate.snapshot_id] = int(age_seconds)
                    if age_seconds > stale_days * 86_400:
                        stale.append(
                            f"{seam}:age_seconds={age_seconds}:"
                            f"limit_seconds={stale_days * 86_400}"
                        )
                        continue

    complete_symbols = set(symbol_ids) == set(selected_symbols)
    if restatement_incomplete:
        status = CftcPositioningQueryStatus.RESTATEMENT_INCOMPLETE
        reason = "original publication vintage unavailable: " + ", ".join(
            sorted(restatement_incomplete)
        )
    elif unavailable:
        status = CftcPositioningQueryStatus.NOT_AVAILABLE
        reason = "positioning state unavailable as of cutoff: " + ", ".join(
            sorted(unavailable)
        )
    elif stale:
        status = CftcPositioningQueryStatus.STALE
        reason = "positioning state exceeds staleness limit: " + ", ".join(
            sorted(stale)
        )
    elif missing or not complete_symbols:
        status = CftcPositioningQueryStatus.MISSING
        reason = "positioning state missing: " + ", ".join(sorted(missing))
    else:
        status = CftcPositioningQueryStatus.READY
        reason = "ready"
    retain_state = status in {
        CftcPositioningQueryStatus.READY,
        CftcPositioningQueryStatus.STALE,
    }
    snapshots = tuple(selected.values()) if retain_state else ()
    ready_symbol_ids = (
        {key: tuple(value) for key, value in symbol_ids.items()}
        if retain_state
        else {}
    )
    ready_mappings = mapping_kinds if retain_state else {}
    return CftcPositioningQueryV1(
        corpus_id=corpus.corpus_id,
        information_mode=mode,
        start_ns=start_ns,
        end_ns=end_ns,
        as_of_ns=as_of_ns,
        symbols=selected_symbols,
        report_families=families,
        report_scopes=scopes,
        snapshots=snapshots,
        symbol_snapshot_ids=ready_symbol_ids,
        mapping_kinds=ready_mappings,
        derived_values=(
            _derive_query_values(
                corpus,
                snapshots,
                ready_symbol_ids,
                information_mode=mode,
                as_of_ns=as_of_ns,
            )
            if status is CftcPositioningQueryStatus.READY
            else {}
        ),
        status=status,
        reason=reason,
        age_seconds=(ages if retain_state else {}),
    )


def preflight_cftc_positioning_corpus(
    corpus: CftcPositioningCorpusV1,
    **query_parameters: Any,
) -> CftcPositioningPreflightV1:
    """Return a fail-closed readiness decision for one window."""
    query = query_cftc_positioning_corpus(corpus, **query_parameters)
    return CftcPositioningPreflightV1(
        corpus_id=corpus.corpus_id,
        start_ns=query.start_ns,
        end_ns=query.end_ns,
        information_mode=query.information_mode,
        as_of_ns=query.as_of_ns,
        symbols=query.symbols,
        report_families=query.report_families,
        report_scopes=query.report_scopes,
        ready=query.status is CftcPositioningQueryStatus.READY,
        reasons=(
            ()
            if query.status is CftcPositioningQueryStatus.READY
            else (query.reason,)
        ),
    )


def require_cftc_positioning_corpus(
    corpus: CftcPositioningCorpusV1,
    **query_parameters: Any,
) -> CftcPositioningPreflightV1:
    """Raise with structured evidence when positioning is not supported."""
    decision = preflight_cftc_positioning_corpus(corpus, **query_parameters)
    if not decision.ready:
        raise CftcPositioningPreflightError(decision)
    return decision


def cftc_positioning_information_inputs(
    query: CftcPositioningQueryV1,
    *,
    run_id: str,
    used_at_ns: int,
    split_kind: InformationSplitKind | None = None,
) -> tuple[ReconstructionInformationInputV1, ...]:
    """Declare every selected current-state row to the information audit."""
    if query.status is not CftcPositioningQueryStatus.READY:
        raise ValueError("CFTC information inputs require a ready query")
    used = _bounded_int64(used_at_ns, "used_at_ns")
    inputs: list[ReconstructionInformationInputV1] = []
    for snapshot in query.snapshots:
        evidence = snapshot.release_evidence
        available = (
            evidence.restatement_detected_at_ns
            or evidence.knowledge_at_ns
            or evidence.publication_at_ns
            or used
        )
        revision = snapshot.restatement_status in {
            CftcRestatementStatus.CURRENT_STATE_ONLY,
            CftcRestatementStatus.RESTATED_CURRENT_STATE,
            CftcRestatementStatus.RESTATEMENT_INCOMPLETE,
        }
        scope = (
            InformationScope.FULL_PERIOD_SUMMARY
            if revision
            and query.information_mode is InformationMode.EX_POST_RECONSTRUCTION
            and available > used
            else (
                InformationScope.REVISION
                if revision
                else InformationScope.POINT_IN_TIME
            )
        )
        inputs.append(
            ReconstructionInformationInputV1(
                run_id=run_id,
                artifact_id=f"{query.corpus_id}:{snapshot.snapshot_id}",
                information_mode=query.information_mode,
                input_kind=InformationInputKind.EXTERNAL,
                stage=InformationStage.FEATURE,
                scope=scope,
                event_time_ns=snapshot.measurement_start_ns,
                available_at_ns=available,
                used_at_ns=used,
                observation_start_ns=snapshot.measurement_start_ns,
                observation_end_ns=snapshot.measurement_start_ns,
                vintage_id=snapshot.snapshot_id,
                reason=(
                    "CFTC current corrected-state positioning sidecar"
                    if revision
                    else "CFTC verified original-vintage positioning sidecar"
                ),
                allowed_lookahead_ns=(
                    max(0, available - used)
                    if query.information_mode
                    is InformationMode.EX_POST_RECONSTRUCTION
                    else 0
                ),
                split_kind=split_kind,
            )
        )
    return tuple(inputs)


def bind_cftc_positioning_query(
    query: CftcPositioningQueryV1,
    *,
    consumer: CftcPositioningConsumer,
    consumer_artifact_id: str,
    run_id: str,
    window_id: str,
    information_inputs: Sequence[ReconstructionInformationInputV1],
) -> CftcPositioningConsumerBindingV1:
    """Bind immutable v1 consumers through a companion lineage receipt."""
    if query.status is not CftcPositioningQueryStatus.READY:
        raise ValueError("CFTC consumer binding requires a ready query")
    inputs = tuple(information_inputs)
    expected_artifacts = {
        f"{query.corpus_id}:{item.snapshot_id}" for item in query.snapshots
    }
    if {item.artifact_id for item in inputs} != expected_artifacts:
        raise ValueError("CFTC information inputs differ from query snapshots")
    if any(item.run_id != run_id for item in inputs):
        raise ValueError("CFTC information input run differs from binding")
    return CftcPositioningConsumerBindingV1(
        consumer=consumer,
        consumer_artifact_id=consumer_artifact_id,
        run_id=run_id,
        window_id=window_id,
        corpus_id=query.corpus_id,
        query_id=query.query_id,
        snapshot_ids=tuple(item.snapshot_id for item in query.snapshots),
        information_input_ids=tuple(item.input_id for item in inputs),
        state_label=cftc_positioning_state_label(query),
        metrics=query.derived_values,
    )


def cftc_positioning_state_label(query: CftcPositioningQueryV1) -> str:
    """Return a compact non-causal benchmark/motif state label."""
    if query.status is not CftcPositioningQueryStatus.READY:
        return f"cftc_positioning:none:{query.status.value}"
    dates = sorted({item.report_date for item in query.snapshots})
    return f"cftc_positioning:weekly:{dates[-1]}"


def apply_cftc_positioning_to_benchmark_events(
    events: Sequence[Any],
    binding: CftcPositioningConsumerBindingV1,
) -> tuple[Any, ...]:
    """Project a receipt onto the existing benchmark event-state seam."""
    if binding.consumer is not CftcPositioningConsumer.BENCHMARK:
        raise ValueError("CFTC benchmark projection requires benchmark binding")
    projected: list[Any] = []
    for event in events:
        if not hasattr(event, "event_state") or not hasattr(
            event, "benchmark_event_id"
        ):
            raise ValueError(
                "CFTC benchmark projection requires benchmark events"
            )
        projected.append(
            replace(
                event,
                event_state=f"{event.event_state}|{binding.state_label}",
                benchmark_event_id="",
            )
        )
    return tuple(projected)


def apply_cftc_positioning_to_motif_condition(
    condition: Any,
    binding: CftcPositioningConsumerBindingV1,
) -> Any:
    """Project a bounded label while the companion receipt retains metrics."""
    if binding.consumer is not CftcPositioningConsumer.MOTIF_SELECTION:
        raise ValueError("CFTC motif projection requires motif binding")
    if not hasattr(condition, "event_tags") or not hasattr(
        condition, "metrics"
    ):
        raise ValueError("CFTC motif projection requires a motif condition")
    tags = tuple(condition.event_tags) + (binding.state_label,)
    return replace(condition, event_tags=tags)


def validate_cftc_positioning_consumer_binding(
    consumer_artifact: Any,
    binding: CftcPositioningConsumerBindingV1,
) -> None:
    """Verify run/window/identity continuity for planning or carving sidecars."""
    artifact_id = _consumer_artifact_id(consumer_artifact)
    if artifact_id != binding.consumer_artifact_id:
        raise ValueError("CFTC consumer artifact identity differs from binding")
    if (
        hasattr(consumer_artifact, "run_id")
        and consumer_artifact.run_id != binding.run_id
    ):
        raise ValueError("CFTC consumer run differs from binding")
    if (
        hasattr(consumer_artifact, "window_id")
        and consumer_artifact.window_id != binding.window_id
    ):
        raise ValueError("CFTC consumer window differs from binding")


def build_cftc_positioning_benchmark_smoke(
    query: CftcPositioningQueryV1,
    binding: CftcPositioningConsumerBindingV1,
    events: Sequence[Any],
    *,
    source_artifact_id: str,
    source_sha256: str,
    reload_events: Sequence[Any] | None = None,
) -> CftcPositioningBenchmarkSmokeV1:
    """Record deterministic held-out benchmark consumption evidence."""
    if binding.consumer is not CftcPositioningConsumer.BENCHMARK:
        raise ValueError("CFTC smoke requires benchmark binding")
    if binding.query_id != query.query_id:
        raise ValueError("CFTC smoke query differs from binding")
    projected = apply_cftc_positioning_to_benchmark_events(events, binding)
    reloaded = apply_cftc_positioning_to_benchmark_events(
        events if reload_events is None else reload_events, binding
    )
    logical_hash = _event_output_hash(projected)
    reload_hash = _event_output_hash(reloaded)
    return CftcPositioningBenchmarkSmokeV1(
        corpus_id=query.corpus_id,
        query_id=query.query_id,
        binding_id=binding.binding_id,
        source_artifact_id=source_artifact_id,
        source_sha256=source_sha256,
        source_row_count=len(projected),
        benchmark_event_ids=tuple(
            item.benchmark_event_id for item in projected
        ),
        logical_output_sha256=logical_hash,
        reload_output_sha256=reload_hash,
        deterministic_reload=logical_hash == reload_hash,
    )


def write_cftc_positioning_benchmark_smoke(
    smoke: CftcPositioningBenchmarkSmokeV1,
    directory: str | Path,
) -> ArtifactRef:
    """Write content-addressed real-consumption smoke evidence."""
    return _write_json_artifact(
        Path(directory).expanduser().resolve(),
        "cftc-positioning-benchmark-smoke",
        "cftc_positioning_benchmark_smoke_v1",
        smoke.to_dict(),
        metadata={"smoke_id": smoke.smoke_id},
    )


def write_cftc_positioning_consumer_binding(
    binding: CftcPositioningConsumerBindingV1,
    directory: str | Path,
) -> ArtifactRef:
    """Write one content-addressed consumer-lineage receipt."""
    return _write_json_artifact(
        Path(directory).expanduser().resolve(),
        "cftc-positioning-consumer-binding",
        "cftc_positioning_consumer_binding_v1",
        binding.to_dict(),
        metadata={
            "binding_id": binding.binding_id,
            "consumer": binding.consumer.value,
        },
    )


def _validate_required_sources(
    sources: Sequence[CftcPositioningRawSourceV1],
    profile: CftcPositioningFetchProfileV1,
) -> None:
    by_key = {item.source_key: item for item in sources}
    required_keys = {
        "official.release-schedule",
        "official.special-announcements",
        "official.historical-compressed-index",
        "official.2025-backlog",
        "official.web-policy",
    }
    missing_keys = required_keys - set(by_key)
    if missing_keys:
        raise ValueError(
            "CFTC source set lacks required evidence: "
            + ", ".join(sorted(missing_keys))
        )
    for dataset_id in profile.dataset_ids:
        metadata = by_key.get(f"pre.{dataset_id}.metadata")
        pages = sorted(
            (
                item
                for item in sources
                if item.dataset_id == dataset_id
                and item.source_kind == "pre_data"
            ),
            key=lambda item: item.source_key,
        )
        if metadata is None or not pages:
            raise ValueError(f"CFTC source set is incomplete for {dataset_id}")
        expected_family = (
            CftcReportFamily.LEGACY
            if dataset_id == CFTC_LEGACY_DATASET_ID
            else CftcReportFamily.TFF
        )
        if metadata.report_family is not expected_family:
            raise ValueError("CFTC PRE metadata family differs from dataset")
        offsets: list[int] = []
        for page in pages:
            if page.report_family is not expected_family:
                raise ValueError("CFTC PRE page family differs from dataset")
            parameters = page.query_parameters
            expected = _pre_query_parameters(
                profile, offset=int(parameters.get("$offset", "-1"))
            )
            if dict(parameters) != expected:
                raise ValueError("CFTC PRE page query differs from profile")
            offsets.append(int(parameters["$offset"]))
        expected_offsets = list(
            range(0, len(pages) * profile.page_size, profile.page_size)
        )
        if sorted(offsets) != expected_offsets:
            raise ValueError("CFTC PRE pages are missing or non-contiguous")
    archive_names = {
        Path(item.source_uri.split("?", 1)[0]).name
        for item in sources
        if item.source_kind == "historical_archive"
    }
    if archive_names != set(profile.historical_archives):
        raise ValueError("CFTC historical archive sources differ from profile")
    if any(
        not isinstance(item, CftcPositioningRawSourceV1) for item in sources
    ):
        raise ValueError("CFTC source set contains unsupported values")


def _build_release_evidence(
    sources: Sequence[CftcPositioningRawSourceV1],
) -> dict[str, CftcReleaseEvidenceV1]:
    by_key = {item.source_key: item for item in sources}
    schedule = by_key["official.release-schedule"]
    special = by_key["official.special-announcements"]
    backlog = by_key["official.2025-backlog"]
    _require_source_terms(schedule, ("commitments", "release"))
    _require_source_terms(special, ("commitments",))
    _require_source_terms(backlog, ("2025",))
    evidence: dict[str, CftcReleaseEvidenceV1] = {}

    backlog_dates = {
        "2025-09-30": "2025-11-19",
        "2025-10-07": "2025-11-21",
        "2025-10-14": "2025-11-25",
        "2025-10-21": "2025-12-02",
        "2025-10-28": "2025-12-05",
        "2025-11-04": "2025-12-09",
        "2025-11-10": "2025-12-10",
        "2025-11-18": "2025-12-12",
        "2025-11-25": "2025-12-15",
        "2025-12-02": "2025-12-17",
        "2025-12-09": "2025-12-19",
        "2025-12-16": "2025-12-23",
        "2025-12-23": "2025-12-29",
    }
    for report_date, publication_date in backlog_dates.items():
        publication = _publication_ns(
            _parse_date(publication_date, "publication_date")
        )
        evidence[report_date] = CftcReleaseEvidenceV1(
            report_date=report_date,
            confidence=CftcAvailabilityConfidence.VERIFIED,
            source_id=backlog.source_id,
            publication_at_ns=publication,
            knowledge_at_ns=publication,
            notes=(
                "Actual delayed publication date verified by CFTC release 9147-25.",
                "The PRE row remains current corrected state rather than a captured original vintage.",
            ),
        )

    qualified = (
        (
            "2010-05-18",
            "2010-05-27",
            CftcAvailabilityConfidence.RESTATEMENT_QUALIFIED,
            "CFTC later corrected GBP and EUR rows.",
        ),
        (
            "2015-06-16",
            "2015-06-23",
            CftcAvailabilityConfidence.CORRECTION_QUALIFIED,
            "CFTC identified an incomplete or premature publication.",
        ),
        (
            "2015-06-30",
            "2015-07-06",
            CftcAvailabilityConfidence.CORRECTION_QUALIFIED,
            "CFTC special-announcement timing supersedes a nominal Friday estimate.",
        ),
        (
            "2018-09-18",
            "2018-09-26",
            CftcAvailabilityConfidence.RESTATEMENT_QUALIFIED,
            "CFTC later revised the published report.",
        ),
    )
    for report_text, detected_text, confidence, note in qualified:
        detected = _publication_ns(_parse_date(detected_text, "detected_date"))
        evidence[report_text] = CftcReleaseEvidenceV1(
            report_date=report_text,
            confidence=confidence,
            source_id=special.source_id,
            publication_at_ns=None,
            knowledge_at_ns=None,
            restatement_detected_at_ns=detected,
            notes=(note, "Original row vintage is not retained in PRE."),
        )
    return evidence


def _nominal_release_evidence(
    report_date: date,
    sources: Sequence[CftcPositioningRawSourceV1],
) -> CftcReleaseEvidenceV1:
    schedule = next(
        item
        for item in sources
        if item.source_key == "official.release-schedule"
    )
    days_to_friday = (4 - report_date.weekday()) % 7
    nominal_date = report_date + timedelta(days=days_to_friday)
    publication = _publication_ns(nominal_date)
    return CftcReleaseEvidenceV1(
        report_date=report_date.isoformat(),
        confidence=CftcAvailabilityConfidence.NOMINAL,
        source_id=schedule.source_id,
        publication_at_ns=publication,
        knowledge_at_ns=publication,
        notes=(
            "Nominal Friday 15:30 America/New_York estimate; not verified for strict ex-ante use.",
            "Holiday, shutdown, cyber-event, premature-release, and correction exceptions may apply.",
        ),
    )


def _restatement_status(
    report_family: CftcReportFamily, report_date: date
) -> CftcRestatementStatus:
    del report_family
    if report_date in {
        date(2010, 5, 18),
        date(2015, 6, 16),
        date(2015, 6, 30),
        date(2018, 9, 18),
    }:
        return CftcRestatementStatus.RESTATED_CURRENT_STATE
    return CftcRestatementStatus.CURRENT_STATE_ONLY


def _positioning_values(row: Mapping[str, Any]) -> dict[str, float]:
    values: dict[str, float] = {}
    allowed_fragments = (
        "open_interest",
        "positions_",
        "_positions_",
        "change_in_",
        "pct_of_oi",
        "traders_",
        "conc_",
    )
    for raw_key, raw_value in row.items():
        key = _source_key(str(raw_key).strip().lower())
        if not any(fragment in key for fragment in allowed_fragments):
            continue
        if raw_value is None or str(raw_value).strip() in {"", "."}:
            continue
        try:
            value = float(str(raw_value).replace(",", ""))
        except ValueError:
            continue
        if math.isfinite(value):
            values[key] = value
    if "open_interest_all" not in values:
        raise ValueError("CFTC PRE row lacks open_interest_all")
    return values


def _build_cftc_coverage(
    snapshots: Sequence[CftcPositioningSnapshotV1],
    *,
    duplicate_counts: Mapping[tuple[str, str, str, str], int],
    evidence_by_id: Mapping[str, CftcPositioningRawSourceV1],
) -> tuple[CftcPositioningCoverageSliceV1, ...]:
    grouped: dict[
        tuple[int, CftcReportFamily, CftcReportScope, str],
        list[CftcPositioningSnapshotV1],
    ] = defaultdict(list)
    for snapshot in snapshots:
        report = _parse_date(snapshot.report_date, "report_date")
        grouped[
            (
                report.year,
                snapshot.report_family,
                snapshot.report_scope,
                snapshot.contract_code,
            )
        ].append(snapshot)
    result: list[CftcPositioningCoverageSliceV1] = []
    for (year, family, scope, code), values in sorted(
        grouped.items(),
        key=lambda item: (
            item[0][0],
            item[0][1].value,
            item[0][2].value,
            item[0][3],
        ),
    ):
        ordered = sorted(values, key=lambda item: item.report_date)
        dates = [
            _parse_date(item.report_date, "report_date") for item in ordered
        ]
        missing_weeks = sum(
            max(0, ((right - left).days // 7) - 1)
            for left, right in zip(dates, dates[1:])
        )
        source_ids = {item.source_id for item in ordered}
        sources = [evidence_by_id[item] for item in source_ids]
        duplicate_count = sum(
            count
            for (
                family_name,
                scope_name,
                duplicate_code,
                report_date,
            ), count in duplicate_counts.items()
            if family_name == family.value
            and scope_name == scope.value
            and duplicate_code == code
            and _parse_date(report_date, "report_date").year == year
        )
        result.append(
            CftcPositioningCoverageSliceV1(
                year=year,
                report_family=family,
                report_scope=scope,
                contract_code=code,
                row_count=len(ordered),
                first_report_date=ordered[0].report_date,
                last_report_date=ordered[-1].report_date,
                missing_week_count=missing_weeks,
                duplicate_key_count=duplicate_count,
                contract_names=tuple(item.contract_name for item in ordered),
                market_names=tuple(item.market_name for item in ordered),
                availability_counts=dict(
                    Counter(
                        item.release_evidence.confidence.value
                        for item in ordered
                    )
                ),
                restatement_counts=dict(
                    Counter(item.restatement_status.value for item in ordered)
                ),
                source_hashes=tuple(item.content_sha256 for item in sources),
                source_bytes=sum(len(item.content) for item in sources),
                processing_seconds=0.0,
            )
        )
    return tuple(result)


def _archive_consistency(
    source: CftcPositioningRawSourceV1,
    snapshots: Sequence[CftcPositioningSnapshotV1],
    profile: CftcPositioningFetchProfileV1,
) -> CftcArchiveConsistencyV1:
    if source.report_family is None or source.report_scope is None:
        raise ValueError("CFTC historical archive lacks family/scope metadata")
    pre_by_key = {
        (item.contract_code, item.report_date): item
        for item in snapshots
        if item.report_family is source.report_family
        and item.report_scope is source.report_scope
    }
    selected = 0
    matched = 0
    missing = 0
    oi_mismatch = 0
    name_change = 0
    for row in _archive_rows(source.content):
        normalized = {
            _archive_field(key): value
            for key, value in row.items()
            if key is not None
        }
        code_text = _first_archive_value(
            normalized,
            "cftc_contract_market_code",
            "cftc_contract_market_code_quotes",
        )
        if not code_text:
            continue
        code = str(code_text).strip().strip('"').zfill(6)
        if code not in profile.contract_codes:
            continue
        report_text = _first_archive_value(
            normalized,
            "as_of_date_in_form_yyyymmdd",
            "as_of_date_in_form_yyyy_mm_dd",
            "as_of_date_in_form_yymmdd",
            "report_date_as_yyyy_mm_dd",
            "report_date_as_yyyymmdd",
        )
        report = _parse_archive_date(str(report_text or ""))
        if report is None or not (
            _parse_date(profile.start_date, "start_date")
            <= report
            <= _parse_date(profile.end_date, "end_date")
        ):
            continue
        selected += 1
        pre = pre_by_key.get((code, report.isoformat()))
        if pre is None:
            missing += 1
            continue
        matched += 1
        oi_text = _first_archive_value(normalized, "open_interest_all")
        if oi_text not in {None, ""}:
            try:
                archive_oi = float(str(oi_text).replace(",", ""))
            except ValueError:
                archive_oi = math.nan
            if (
                math.isfinite(archive_oi)
                and archive_oi != pre.values["open_interest_all"]
            ):
                oi_mismatch += 1
        archive_name = str(
            _first_archive_value(
                normalized, "market_and_exchange_names", "contract_market_name"
            )
            or ""
        ).strip()
        if archive_name and archive_name not in {
            pre.market_name,
            pre.contract_name,
        }:
            name_change += 1
    return CftcArchiveConsistencyV1(
        source_id=source.source_id,
        report_family=source.report_family,
        report_scope=source.report_scope,
        selected_row_count=selected,
        matched_pre_rows=matched,
        missing_pre_rows=missing,
        open_interest_mismatch_count=oi_mismatch,
        contract_name_change_count=name_change,
        limitations=(
            "Consistency compares current PRE rows with current compressed-history rows and does not prove original-vintage equality.",
            "Name changes are diagnostic metadata drift; numeric open-interest mismatches remain explicit.",
        ),
    )


def _archive_rows(content: bytes) -> Iterable[Mapping[str, str]]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as exc:
        raise ValueError("CFTC historical archive is not a valid ZIP") from exc
    members = sorted(
        (item for item in archive.infolist() if not item.is_dir()),
        key=lambda item: item.filename,
    )
    if not members:
        raise ValueError("CFTC historical archive is empty")
    total_uncompressed = sum(item.file_size for item in members)
    if total_uncompressed > MAX_CFTC_SOURCE_BYTES * 4:
        raise ValueError("CFTC historical archive expands beyond safety limit")
    selected = next(
        (
            item
            for item in members
            if item.filename.lower().endswith((".txt", ".csv"))
        ),
        members[0],
    )
    if selected.file_size > MAX_CFTC_SOURCE_BYTES * 4:
        raise ValueError("CFTC historical member exceeds safety limit")
    try:
        with archive.open(selected) as binary:
            with io.TextIOWrapper(
                binary, encoding="utf-8-sig", errors="replace", newline=""
            ) as text_content:
                yield from csv.DictReader(text_content)
    finally:
        archive.close()


def _peak_memory_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if not sys.platform.startswith("darwin"):
        peak *= 1024
    return int(peak)


def _derive_query_values(
    corpus: CftcPositioningCorpusV1,
    selected: Sequence[CftcPositioningSnapshotV1],
    symbol_snapshot_ids: Mapping[str, tuple[str, ...]],
    *,
    information_mode: InformationMode,
    as_of_ns: int | None,
) -> dict[str, float]:
    symbol_by_snapshot = {
        snapshot_id: symbol
        for symbol, snapshot_ids in symbol_snapshot_ids.items()
        for snapshot_id in snapshot_ids
    }
    metrics: dict[str, float] = {}
    for snapshot in selected:
        pair = _primary_position_pair(snapshot.values, snapshot.report_family)
        if pair is None:
            continue
        long_key, short_key = pair
        net = snapshot.values[long_key] - snapshot.values[short_key]
        oi = snapshot.values["open_interest_all"]
        prefix = ".".join(
            (
                symbol_by_snapshot[snapshot.snapshot_id].lower(),
                snapshot.report_family.value,
                snapshot.report_scope.value,
                snapshot.contract_code,
            )
        )
        metrics[f"{prefix}.net"] = net
        metrics[f"{prefix}.net_open_interest"] = net / oi if oi else 0.0
        history = sorted(
            (
                item
                for item in corpus.snapshots
                if item.report_family is snapshot.report_family
                and item.report_scope is snapshot.report_scope
                and item.contract_code == snapshot.contract_code
                and item.report_date <= snapshot.report_date
                and (
                    information_mode is InformationMode.EX_POST_RECONSTRUCTION
                    or (
                        item.strict_ex_ante_eligible
                        and item.release_evidence.knowledge_at_ns is not None
                        and as_of_ns is not None
                        and item.release_evidence.knowledge_at_ns <= as_of_ns
                    )
                )
            ),
            key=lambda item: item.report_date,
        )[-52:]
        historical_nets: list[float] = []
        for item in history:
            historical_pair = _primary_position_pair(
                item.values, item.report_family
            )
            if historical_pair is not None:
                historical_nets.append(
                    item.values[historical_pair[0]]
                    - item.values[historical_pair[1]]
                )
        deviation = (
            statistics.pstdev(historical_nets)
            if len(historical_nets) > 1
            else 0.0
        )
        metrics[f"{prefix}.net_change"] = (
            net - historical_nets[-2] if len(historical_nets) > 1 else 0.0
        )
        metrics[f"{prefix}.rolling_52w_zscore"] = (
            (net - statistics.fmean(historical_nets)) / deviation
            if deviation > 0
            else 0.0
        )
    return metrics


def _primary_position_pair(
    values: Mapping[str, float], family: CftcReportFamily
) -> tuple[str, str] | None:
    preferred = (
        ("noncomm_positions_long_all", "noncomm_positions_short_all")
        if family is CftcReportFamily.LEGACY
        else ("lev_money_positions_long_all", "lev_money_positions_short_all")
    )
    if preferred[0] in values and preferred[1] in values:
        return preferred
    for long_key in sorted(
        key for key in values if "positions_long_all" in key
    ):
        short_key = long_key.replace(
            "positions_long_all", "positions_short_all"
        )
        if short_key in values:
            return long_key, short_key
    return None


def _write_json_artifact(
    root: Path,
    stem: str,
    kind: str,
    payload: Mapping[str, JSONValue],
    *,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    root.mkdir(parents=True, exist_ok=True)
    content = canonical_contract_json(payload).encode("utf-8") + b"\n"
    digest = hashlib.sha256(content).hexdigest()
    artifact_path = root / f"{stem}-{digest}.json"
    _write_once(artifact_path, content)
    return ArtifactRef(
        kind=kind,
        path=str(artifact_path),
        size_bytes=len(content),
        sha256=digest,
        metadata=dict(metadata),
    )


def _write_once(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(f"content-addressed artifact differs: {path}")
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            if path.read_bytes() != content:
                raise ValueError(f"content-addressed artifact differs: {path}")
        else:
            temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _content_suffix(content_type: str, source_uri: str) -> str:
    media_type = content_type.lower()
    if "json" in media_type:
        return ".json"
    if "zip" in media_type or source_uri.lower().split("?", 1)[0].endswith(
        ".zip"
    ):
        return ".zip"
    if "pdf" in media_type or source_uri.lower().split("?", 1)[0].endswith(
        ".pdf"
    ):
        return ".pdf"
    if "html" in media_type:
        return ".html"
    if "csv" in media_type:
        return ".csv"
    return ".bin"


def _json_rows(content: bytes, label: str) -> tuple[Mapping[str, Any], ...]:
    if len(content) > MAX_CFTC_SOURCE_BYTES:
        raise ValueError(f"{label} exceeds size bound")
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is invalid JSON") from exc
    return _mapping_sequence(payload)


def _parse_pre_report_date(value: str) -> date:
    text = value.strip()
    if not text:
        raise ValueError("CFTC PRE row lacks report date")
    return _parse_date(text[:10], "report_date")


def _publication_ns(value: date) -> int:
    eastern = ZoneInfo("America/New_York")
    published = datetime.combine(value, datetime_time(15, 30), tzinfo=eastern)
    return int(published.timestamp() * 1_000_000_000)


def _date_start_ns(value: date) -> int:
    return int(
        datetime.combine(
            value, datetime_time(), tzinfo=timezone.utc
        ).timestamp()
        * 1_000_000_000
    )


def _validate_query_interval(
    start_ns: int,
    end_ns: int,
    mode: InformationMode,
    as_of_ns: int | None,
) -> None:
    start = _bounded_int64(start_ns, "start_ns")
    end = _bounded_int64(end_ns, "end_ns")
    if end <= start:
        raise ValueError("CFTC query end must follow start")
    if mode is InformationMode.EX_ANTE_SIMULATION:
        if as_of_ns is None:
            raise ValueError("ex-ante CFTC query requires as_of_ns")
        cutoff = _bounded_int64(as_of_ns, "as_of_ns")
        if cutoff > start:
            raise ValueError("CFTC query as_of cannot follow start")
    elif as_of_ns is not None:
        raise ValueError("ex-post CFTC query does not accept as_of_ns")


def _select_symbol_mapping(
    corpus: CftcPositioningCorpusV1, symbol: str, window_date: date
) -> CftcPositioningSymbolMappingV1:
    candidates = [
        item for item in corpus.symbol_mappings if item.symbol == symbol
    ]
    direct = [
        item
        for item in candidates
        if item.mapping_kind is CftcMappingKind.DIRECT
        and (
            item.valid_from_date is None
            or _parse_date(item.valid_from_date, "valid_from_date")
            <= window_date
        )
    ]
    if direct:
        return direct[-1]
    derived = [
        item
        for item in candidates
        if item.mapping_kind is CftcMappingKind.DERIVED_TWO_LEG
    ]
    if derived:
        return derived[0]
    raise ValueError(f"unsupported CFTC positioning symbol: {symbol}")


def _normalized_symbol(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z]", "", _required_text(value)).upper()
    if re.fullmatch(r"[A-Z]{6}", normalized) is None:
        raise ValueError("CFTC positioning symbol must be a six-letter pair")
    return normalized


def _normalized_symbols(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_normalized_symbol(item) for item in values}))


def _consumer_artifact_id(value: Any) -> str:
    if isinstance(value, str):
        return _required_text(value)
    for name in (
        "batch_id",
        "plan_id",
        "benchmark_id",
        "artifact_id",
        "condition_id",
    ):
        candidate = getattr(value, name, None)
        if candidate:
            return _required_text(str(candidate))
    raise ValueError("CFTC consumer artifact lacks a stable identity")


def _event_output_hash(events: Sequence[Any]) -> str:
    payload: list[JSONValue] = []
    for event in events:
        if not hasattr(event, "to_dict"):
            raise ValueError(
                "CFTC smoke event lacks deterministic serialization"
            )
        payload.append(_json_value_mapping(event.to_dict()))
    return hashlib.sha256(
        canonical_contract_json({"events": payload}).encode("utf-8")
    ).hexdigest()


def _require_source_terms(
    source: CftcPositioningRawSourceV1, terms: Sequence[str]
) -> None:
    content = source.content.decode("utf-8", errors="ignore").lower()
    if any(term.lower() not in content for term in terms):
        raise ValueError(
            f"CFTC source {source.source_key} lacks expected content"
        )


def _archive_field(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip(
        "_"
    )
    return normalized


def _first_archive_value(row: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


def _parse_archive_date(value: str) -> date | None:
    text = value.strip().strip('"')
    if not text:
        return None
    formats = (
        "%Y-%m-%d",
        "%Y%m%d",
        "%m/%d/%Y",
        "%m/%d/%Y %I:%M:%S %p",
        "%y%m%d",
    )
    for format_string in formats:
        try:
            return datetime.strptime(text, format_string).date()
        except ValueError:
            continue
    if len(text) >= 10:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            pass
    return None


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}-{digest}"


def _required_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("value must be text")
    normalized = value.strip()
    if not normalized or len(normalized) > MAX_CFTC_TEXT:
        raise ValueError("text is empty or exceeds limit")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value)


def _source_key(value: str) -> str:
    normalized = value.strip().lower()
    if _SOURCE_KEY_RE.fullmatch(normalized) is None:
        raise ValueError("invalid CFTC source key")
    return normalized


def _contract_code(value: str) -> str:
    normalized = value.strip().upper().zfill(6)
    if _CONTRACT_CODE_RE.fullmatch(normalized) is None:
        raise ValueError("invalid CFTC contract code")
    return normalized


def _parse_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(_required_text(value))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(normalized):
        raise ValueError(f"{name} must be finite")
    return normalized


def _positive_float(value: Any, name: str) -> float:
    normalized = _finite_float(value, name)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def _finite_nonnegative(value: Any, name: str) -> float:
    normalized = _finite_float(value, name)
    if normalized < 0:
        raise ValueError(f"{name} must be nonnegative")
    return normalized


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _strict_int(value, "optional integer")


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    normalized = _strict_int(value, name)
    if normalized < minimum or normalized > maximum:
        raise ValueError(f"{name} is outside bounds")
    return normalized


def _bounded_int64(value: Any, name: str) -> int:
    return _bounded_int(value, name, -(2**63), 2**63 - 1)


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _required_sha256(value: Any, name: str) -> str:
    normalized = _required_text(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(f"{name} must be lowercase SHA-256")
    return normalized


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("value must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("value must be an array")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    result: list[Mapping[str, Any]] = []
    for item in _sequence(value):
        result.append(_mapping(item))
    return tuple(result)


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value))


def _metric_mapping(value: Mapping[str, Any]) -> dict[str, float]:
    result = {
        _source_key(str(key)): _finite_float(item, str(key))
        for key, item in sorted(value.items())
    }
    if len(result) > MAX_CFTC_BINDING_VALUES:
        raise ValueError("CFTC metric mapping exceeds limit")
    return result


def _count_mapping(value: Mapping[str, Any], name: str) -> dict[str, int]:
    result = {
        _source_key(str(key)): _bounded_int(item, str(key), 0, MAX_CFTC_ROWS)
        for key, item in sorted(value.items())
    }
    if not result:
        raise ValueError(f"{name} cannot be empty")
    return result


def _id_mapping(value: Mapping[str, Any]) -> dict[str, str]:
    return {
        _required_text(str(key)): _required_text(str(item))
        for key, item in sorted(value.items())
    }


def _json_value_mapping(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    result: dict[str, JSONValue] = {}
    for key, item in value.items():
        normalized_key = _required_text(str(key))
        result[normalized_key] = _validate_json_value(item, depth=0)
    return result


def _validate_json_value(value: Any, *, depth: int) -> JSONValue:
    if depth > 16:
        raise ValueError("JSON value nesting exceeds limit")
    if value is None or isinstance(value, (str, bool, int)):
        return cast(JSONValue, value)
    if isinstance(value, float):
        return cast(JSONValue, _finite_float(value, "JSON float"))
    if isinstance(value, Mapping):
        return cast(
            JSONValue,
            {
                _required_text(str(key)): _validate_json_value(
                    item, depth=depth + 1
                )
                for key, item in value.items()
            },
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return cast(
            JSONValue,
            [_validate_json_value(item, depth=depth + 1) for item in value],
        )
    raise ValueError("unsupported JSON value")


def _ensure_payload_size(
    payload: Mapping[str, JSONValue], maximum: int
) -> None:
    if len(canonical_contract_json(payload).encode("utf-8")) > maximum:
        raise ValueError("CFTC payload exceeds size bound")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("unsupported CFTC artifact schema")

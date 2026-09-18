"""Deterministic qualification of ECB monthly balance-of-payments releases.

The ECB exposes this lineage across two first-party inventories.  The legacy
page enumerates BPM5 monthly releases through August 2014, while the current
lazy-load snippet and FOEDB publication database independently enumerate the
BPM6-era type-53 releases.  This module binds both surfaces, every selected
English release page, and every release-local English table or annex artifact.

Historical publication values remain source dated.  In particular, the
shared 2015 migration timestamp on older pages is retained as a known defect
and is never substituted for the original release date.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urljoin, urlparse, urlsplit
from zoneinfo import ZoneInfo

from pypdf import PdfReader
from pypdf.errors import PdfReadError

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.ecb_monetary_policy_archive import (
    ECB_FOEDB_ROOT_URI,
    ECB_FOEDB_VERSIONS_URI,
    ECB_SOURCE_TIMEZONE,
    _bounded_int,
    _https_uri,
    _iso_date,
    _mapping,
    _required_text,
    _sequence,
)
from histdatacom.market_context.economic_calendar import EconomicTimePrecision
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY: Final = "ea.ecb.balance-of-payments"
ECB_BALANCE_OF_PAYMENTS_PROGRAM_KEY: Final = (
    "ea.ecb.monthly-balance-of-payments"
)
ECB_BALANCE_OF_PAYMENTS_MEASURE_KEY: Final = "current-account-balance"
ECB_BALANCE_OF_PAYMENTS_PARSER_ID: Final = "official.ecb-balance-of-payments.v1"
ECB_BALANCE_OF_PAYMENTS_PARSER_VERSION: Final = "1"
ECB_BALANCE_OF_PAYMENTS_CURRENT_INDEX_URI: Final = (
    "https://www.ecb.europa.eu/press/stats/bop/html/index_bop.en.html"
)
ECB_BALANCE_OF_PAYMENTS_LEGACY_INDEX_URI: Final = (
    "https://www.ecb.europa.eu/press/stats/bop/html/previous_releases.en.html"
)
ECB_BALANCE_OF_PAYMENTS_ARCHIVE_START_PERIOD: Final = "1999-10"
ECB_LATEST_PACKAGED_BALANCE_OF_PAYMENTS_PERIOD: Final = "2026-07"

ECB_BALANCE_OF_PAYMENTS_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-artifact.v1"
)
ECB_BALANCE_OF_PAYMENTS_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-entry.v1"
)
ECB_BALANCE_OF_PAYMENTS_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-exclusion.v1"
)
ECB_BALANCE_OF_PAYMENTS_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-value.v1"
)
ECB_BALANCE_OF_PAYMENTS_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-release.v1"
)
ECB_BALANCE_OF_PAYMENTS_INDEX_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-index.v1"
)
ECB_BALANCE_OF_PAYMENTS_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.ecb-balance-of-payments-archive-manifest.v1"
)

MAX_ECB_BALANCE_OF_PAYMENTS_RELEASES: Final = 512
MAX_ECB_BALANCE_OF_PAYMENTS_EXCLUSIONS: Final = 512
MAX_ECB_BALANCE_OF_PAYMENTS_ARTIFACTS: Final = 2_048
MAX_ECB_BALANCE_OF_PAYMENTS_VALUES: Final = 8_192
MAX_ECB_BALANCE_OF_PAYMENTS_TOTAL_BYTES: Final = 1_000_000_000
MAX_ECB_BALANCE_OF_PAYMENTS_EXCERPT_CHARS: Final = 1_024

_ECB_BASE_URI: Final = "https://www.ecb.europa.eu"
_MONTH_LOOKUP: Final = {
    name.lower(): index
    for index, name in enumerate(calendar.month_name)
    if name
}
_MONTH_ABBR_LOOKUP: Final = {
    name.lower(): index
    for index, name in enumerate(calendar.month_abbr)
    if name
}
_MONTH_PATTERN: Final = "|".join(calendar.month_name[1:])
_MONTH_ABBR_PATTERN: Final = "|".join(calendar.month_abbr[1:])
_PERIOD_RE: Final = re.compile(r"\d{4}-\d{2}")
_CURRENT_TITLE_RE: Final = re.compile(
    rf"(?P<month>{_MONTH_PATTERN})\s+(?P<year>\d{{4}})\)?\s*$",
    re.IGNORECASE,
)
_LEGACY_URI_PERIOD_RE: Final = re.compile(
    r"/(?:pr(?P<legacy>\d{4})bop|(?:ecb\.)?bp(?P<dated>\d{6,8}))"
)
_DATED_URI_RE: Final = re.compile(r"(?:ecb\.)?bp\.?(?P<date>\d{6,8})")
_NUMBER_RE: Final = re.compile(r"-?\d+(?:\.\d+)?")
_NEXT_RELEASE_RE: Final = re.compile(
    rf"(?:next|following)\s+press\s+release[^.]{{0,260}}?"
    rf"(?:published|publication)\s+(?:on|for)\s+"
    rf"(?P<day>\d{{1,2}})\s+(?P<month>{_MONTH_PATTERN})"
    rf"(?:\s+(?P<year>\d{{4}}))?",
    re.IGNORECASE,
)
_SCHEDULED_RELEASE_RE: Final = re.compile(
    rf"next\s+press\s+release[^.]{{0,260}}?scheduled\s+for\s+publication\s+on\s+"
    rf"(?P<day>\d{{1,2}})\s+(?P<month>{_MONTH_PATTERN})"
    rf"(?:\s+(?P<year>\d{{4}}))?",
    re.IGNORECASE,
)
_CURRENT_ACCOUNT_PATTERNS: Final = (
    re.compile(
        r"current account(?: of the euro area)?[^.;]{0,220}?"
        r"(?P<direction>surplus|deficit)[^.;]{0,100}?"
        r"(?P<currency>EUR|€)\s*(?P<value>\d+(?:\.\d+)?)\s*"
        r"(?P<scale>billion|million)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<direction>surplus|deficit)[^.;]{0,100}?"
        r"current account[^.;]{0,180}?"
        r"(?P<currency>EUR|€)\s*(?P<value>\d+(?:\.\d+)?)\s*"
        r"(?P<scale>billion|million)",
        re.IGNORECASE,
    ),
)
_COMPONENT_RE: Final = re.compile(
    r"(?P<component>goods|services|primary income|secondary income|"
    r"current transfers|income)\s*\(\s*(?P<currency>EUR|€)\s*"
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<scale>billion|million)\s*\)",
    re.IGNORECASE,
)
_MISSING_REFERENCE_PERIODS: Final = frozenset({"2000-01", "2000-03"})
_SCHEDULE_YEAR_CORRECTIONS: Final[Mapping[str, str]] = {
    # The October 2004 page says 26 January 2004 for the following release.
    "2004-10": "2005-01-26",
}


@dataclass(frozen=True, slots=True)
class _FoedbView:
    version: str
    version_hash: str
    total_records: int
    chunk_size: int
    chunk_group_size: int
    records: tuple[Mapping[str, Any], ...]


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = str(canonical_contract_json(payload)).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _month(value: object, name: str) -> str:
    result = str(_required_text(value, name))
    if _PERIOD_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    parsed = date.fromisoformat(f"{result}-01")
    if parsed.strftime("%Y-%m") != result:
        raise ValueError(f"{name} is not a canonical month")
    return result


def _shift_month(value: str, offset: int) -> str:
    parsed = date.fromisoformat(f"{_month(value, 'month')}-01")
    ordinal = parsed.year * 12 + parsed.month - 1 + offset
    return f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"


def _month_range(start: str, end: str) -> tuple[str, ...]:
    result: list[str] = []
    current = _month(start, "start")
    stop = _month(end, "end")
    while current <= stop:
        result.append(current)
        current = _shift_month(current, 1)
    return tuple(result)


def _canonical_uri(value: str) -> str:
    parsed = urlsplit(_https_uri(value, "source_uri"))
    if parsed.hostname != "www.ecb.europa.eu":
        raise ValueError("ECB balance-of-payments artifact has another host")
    path = re.sub(r"/+", "/", parsed.path)
    return str(parsed._replace(path=path, query="", fragment="").geturl())


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY)
    if (
        source_format not in source.formats
        or cast(str, urlparse(uri).hostname) not in source.allowed_hosts
    ):
        raise ValueError("ECB BOP registry binding differs")
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


def build_ecb_balance_of_payments_foedb_versions_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the source-bound FOEDB version-pointer request."""
    return _request(registry, ECB_FOEDB_VERSIONS_URI, OfficialSourceFormat.JSON)


def parse_ecb_balance_of_payments_foedb_version(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[str, str]:
    """Parse and validate the one active FOEDB version."""
    _artifact(snapshot, EcbBalanceOfPaymentsArtifactRole.FOEDB_VERSIONS)
    try:
        payload = json.loads(snapshot.content)
        items = _sequence(payload, "FOEDB versions")
        if len(items) != 1:
            raise ValueError("ECB FOEDB must expose one active version")
        item = _mapping(items[0], "FOEDB version")
        version = _required_text(item.get("version"), "version")
        version_hash = _required_text(item.get("hash"), "hash")
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("ECB FOEDB version pointer is invalid") from exc
    if (
        re.fullmatch(r"\d+", version) is None
        or re.fullmatch(r"[A-Za-z0-9_-]+", version_hash) is None
    ):
        raise ValueError("ECB FOEDB version identity is invalid")
    return version, version_hash


def _foedb_base(version: str, version_hash: str) -> str:
    if (
        re.fullmatch(r"\d+", version) is None
        or re.fullmatch(r"[A-Za-z0-9_-]+", version_hash) is None
    ):
        raise ValueError("ECB FOEDB version identity is invalid")
    return f"{ECB_FOEDB_ROOT_URI}/{version}/{version_hash}"


def build_ecb_balance_of_payments_foedb_metadata_request(
    registry: OfficialSourceRegistryV1,
    version: str,
    version_hash: str,
) -> OfficialSourceRequestV1:
    """Build the source-bound versioned FOEDB metadata request."""
    return _request(
        registry,
        f"{_foedb_base(version, version_hash)}/metadata.json",
        OfficialSourceFormat.JSON,
    )


def build_ecb_balance_of_payments_foedb_chunk_requests(
    registry: OfficialSourceRegistryV1,
    version: str,
    version_hash: str,
    *,
    total_records: int,
    chunk_size: int,
    chunk_group_size: int,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build every bounded source-bound FOEDB chunk request."""
    total = _bounded_int(total_records, "total_records", 1_000_000)
    size = _bounded_int(chunk_size, "chunk_size", 1_000_000)
    group = _bounded_int(chunk_group_size, "chunk_group_size", 1_000_000)
    if not size or not group:
        raise ValueError("ECB FOEDB chunk sizes must be positive")
    count = math.ceil(total / size)
    if count > 512:
        raise ValueError("ECB FOEDB chunk count exceeds bound")
    base = _foedb_base(version, version_hash)
    return tuple(
        _request(
            registry,
            f"{base}/data/{index // group}/chunk_{index}.json",
            OfficialSourceFormat.JSON,
            page_number=index,
        )
        for index in range(count)
    )


def _decode_foedb(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
) -> _FoedbView:
    version, version_hash = parse_ecb_balance_of_payments_foedb_version(
        versions_snapshot
    )
    base = _foedb_base(version, version_hash)
    if metadata_snapshot.request.uri != f"{base}/metadata.json":
        raise ValueError("ECB FOEDB metadata version differs")
    try:
        metadata = _mapping(
            json.loads(metadata_snapshot.content), "FOEDB metadata"
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("ECB FOEDB metadata is invalid") from exc
    if metadata.get("name") != "publications.en":
        raise ValueError("ECB FOEDB metadata names another database")
    total = _bounded_int(
        metadata.get("total_records"), "total_records", 1_000_000
    )
    size = _bounded_int(metadata.get("chunk_size"), "chunk_size", 1_000_000)
    group = _bounded_int(
        metadata.get("chunk_group_size"), "chunk_group_size", 1_000_000
    )
    header = tuple(
        str(item) for item in _sequence(metadata.get("header"), "header")
    )
    expected_header = (
        "id",
        "pub_timestamp",
        "year",
        "issue_number",
        "type",
        "JEL_Code",
        "Taxonomy",
        "boardmember",
        "Authors",
        "documentTypes",
        "publicationProperties",
        "childrenPublication",
        "relatedPublications",
    )
    if header != expected_header:
        raise ValueError("ECB FOEDB record schema differs")
    count = math.ceil(total / size)
    by_page = {item.request.page_number: item for item in chunk_snapshots}
    if set(by_page) != set(range(count)):
        raise ValueError("ECB FOEDB chunk pages are not contiguous")
    records: list[Mapping[str, Any]] = []
    for index in range(count):
        snapshot = by_page[index]
        if (
            snapshot.request.uri
            != f"{base}/data/{index // group}/chunk_{index}.json"
        ):
            raise ValueError("ECB FOEDB chunk URI differs")
        try:
            flat = _sequence(json.loads(snapshot.content), "FOEDB chunk")
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB FOEDB chunk is invalid") from exc
        if len(flat) % len(header):
            raise ValueError("ECB FOEDB chunk has a partial record")
        records.extend(
            dict(zip(header, flat[offset : offset + len(header)], strict=True))
            for offset in range(0, len(flat), len(header))
        )
    if len(records) != total:
        raise ValueError("ECB FOEDB total record count differs")
    return _FoedbView(
        version=version,
        version_hash=version_hash,
        total_records=total,
        chunk_size=size,
        chunk_group_size=group,
        records=tuple(records),
    )


def _source_format(uri: str) -> OfficialSourceFormat:
    suffix = Path(urlsplit(uri).path).suffix.lower()
    if suffix == ".pdf":
        return OfficialSourceFormat.PDF
    if suffix in {".json"}:
        return OfficialSourceFormat.JSON
    if suffix in {".html", ".htm", ""}:
        return OfficialSourceFormat.HTML
    raise ValueError(f"unsupported ECB balance-of-payments artifact: {uri}")


class EcbBalanceOfPaymentsArtifactRole(str, Enum):
    """Semantic role of one retained official artifact."""

    FOEDB_VERSIONS = "foedb-versions"
    FOEDB_METADATA = "foedb-metadata"
    FOEDB_CHUNK = "foedb-chunk"
    CURRENT_INDEX = "current-index"
    LEGACY_INDEX = "legacy-index"
    RELEASE_HTML = "release-html"
    SUPPORT_HTML = "support-html"
    SUPPORT_PDF = "support-pdf"


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsArtifactV1:
    """Hash-bound receipt for one official raw artifact."""

    role: EcbBalanceOfPaymentsArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_BALANCE_OF_PAYMENTS_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB BOP artifact schema")
        object.__setattr__(self, "source_uri", _canonical_uri(self.source_uri))
        if not isinstance(self.role, EcbBalanceOfPaymentsArtifactRole):
            raise TypeError("ECB BOP artifact role is invalid")
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError("ECB BOP artifact format is invalid")
        digest = _required_text(self.content_sha256, "content_sha256")
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("ECB BOP artifact digest is invalid")
        length = _bounded_int(
            self.content_length,
            "content_length",
            MAX_ECB_BALANCE_OF_PAYMENTS_TOTAL_BYTES,
        )
        if length <= 0:
            raise ValueError("ECB BOP artifact is empty")
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError("ECB BOP artifact request identity is invalid")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "source_uri": self.source_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsArtifactV1:
        return cls(
            role=EcbBalanceOfPaymentsArtifactRole(str(data.get("role", ""))),
            source_uri=str(data.get("source_uri", "")),
            source_format=OfficialSourceFormat(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            request_id=str(data.get("request_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _artifact(
    snapshot: OfficialRawSnapshotV1,
    role: EcbBalanceOfPaymentsArtifactRole,
) -> EcbBalanceOfPaymentsArtifactV1:
    if (
        snapshot.request.source_key != ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY
        or snapshot.request.parser_id != ECB_BALANCE_OF_PAYMENTS_PARSER_ID
        or snapshot.request.parser_version
        != ECB_BALANCE_OF_PAYMENTS_PARSER_VERSION
        or snapshot.request.method is not OfficialRequestMethod.GET
        or re.fullmatch(
            r"official-source:sha256:[0-9a-f]{64}",
            snapshot.request.source_id,
        )
        is None
    ):
        raise ValueError("ECB BOP snapshot names another source")
    if snapshot.request.source_format is not _source_format(
        snapshot.request.uri
    ):
        raise ValueError("ECB BOP snapshot format differs from its URI")
    return EcbBalanceOfPaymentsArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=_sha256(snapshot.content),
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsInventoryEntryV1:
    """One selected monthly release from the two ECB inventories."""

    reference_period: str
    source_title: str
    source_uri: str
    inventory: str
    bpm_era: str
    euro_area_composition: str
    inventory_release_date: str | None = None
    foedb_record_id: int | None = None
    foedb_timestamp: int | None = None
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_BALANCE_OF_PAYMENTS_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported ECB BOP inventory-entry schema")
        period = _month(self.reference_period, "reference_period")
        object.__setattr__(self, "reference_period", period)
        _required_text(self.source_title, "source_title")
        object.__setattr__(self, "source_uri", _canonical_uri(self.source_uri))
        if self.inventory not in {"legacy-index", "current-index"}:
            raise ValueError("ECB BOP inventory origin is invalid")
        expected_era = "BPM5" if period <= "2014-08" else "BPM6"
        if self.bpm_era != expected_era:
            raise ValueError("ECB BOP methodology era differs")
        if re.fullmatch(r"EA\d{2}", self.euro_area_composition) is None:
            raise ValueError("ECB BOP euro-area composition is invalid")
        if self.euro_area_composition != _composition(period):
            raise ValueError("ECB BOP euro-area composition differs")
        if self.inventory_release_date is not None:
            _iso_date(self.inventory_release_date, "inventory_release_date")
        if self.inventory == "current-index":
            if self.inventory_release_date is None:
                raise ValueError("current ECB BOP entry omits its date")
            _bounded_int(self.foedb_record_id, "foedb_record_id", 2**63 - 1)
            _bounded_int(self.foedb_timestamp, "foedb_timestamp", 2**63 - 1)
        elif (
            self.foedb_record_id is not None or self.foedb_timestamp is not None
        ):
            raise ValueError("legacy ECB BOP entry carries FOEDB identity")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "source_title": self.source_title,
            "source_uri": self.source_uri,
            "inventory": self.inventory,
            "bpm_era": self.bpm_era,
            "euro_area_composition": self.euro_area_composition,
            "inventory_release_date": self.inventory_release_date,
            "foedb_record_id": self.foedb_record_id,
            "foedb_timestamp": self.foedb_timestamp,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsInventoryEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            source_title=str(data.get("source_title", "")),
            source_uri=str(data.get("source_uri", "")),
            inventory=str(data.get("inventory", "")),
            bpm_era=str(data.get("bpm_era", "")),
            euro_area_composition=str(data.get("euro_area_composition", "")),
            inventory_release_date=cast(
                str | None, data.get("inventory_release_date")
            ),
            foedb_record_id=cast(int | None, data.get("foedb_record_id")),
            foedb_timestamp=cast(int | None, data.get("foedb_timestamp")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsExcludedEntryV1:
    """One explicitly excluded item from a retained ECB inventory."""

    source_title: str
    source_uri: str
    reason: str
    inventory: str
    release_date: str | None = None
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_BALANCE_OF_PAYMENTS_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB BOP exclusion schema")
        _required_text(self.source_title, "source_title")
        object.__setattr__(self, "source_uri", _canonical_uri(self.source_uri))
        if self.reason not in {"quarterly-bop-iip", "annual-bop-iip"}:
            raise ValueError("ECB BOP exclusion reason is invalid")
        if self.inventory not in {"legacy-index", "current-index"}:
            raise ValueError("ECB BOP exclusion inventory is invalid")
        if self.release_date is not None:
            _iso_date(self.release_date, "release_date")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_title": self.source_title,
            "source_uri": self.source_uri,
            "reason": self.reason,
            "inventory": self.inventory,
            "release_date": self.release_date,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsExcludedEntryV1:
        return cls(
            source_title=str(data.get("source_title", "")),
            source_uri=str(data.get("source_uri", "")),
            reason=str(data.get("reason", "")),
            inventory=str(data.get("inventory", "")),
            release_date=cast(str | None, data.get("release_date")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsPublishedValueV1:
    """One source-authored balance value from a monthly release."""

    component: str
    reference_period: str
    adjustment: str
    currency: str
    scale: str
    lexical: str
    value: float
    source_uri: str
    source_locator: str
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_BALANCE_OF_PAYMENTS_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported ECB BOP value schema")
        if self.component not in {
            "current-account",
            "goods",
            "services",
            "income",
            "primary-income",
            "secondary-income",
            "current-transfers",
        }:
            raise ValueError("ECB BOP component is invalid")
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if self.adjustment not in {
            "seasonally-adjusted",
            "working-day-and-seasonally-adjusted",
            "not-seasonally-adjusted",
            "source-unspecified",
        }:
            raise ValueError("ECB BOP adjustment is invalid")
        if self.currency != "EUR" or self.scale not in {"billion", "million"}:
            raise ValueError("ECB BOP unit is invalid")
        lexical = _required_text(self.lexical, "lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError("ECB BOP lexical is invalid")
        numeric = float(self.value)
        if not math.isfinite(numeric) or abs(numeric) > 1_000_000:
            raise ValueError("ECB BOP numeric value is invalid")
        if not math.isclose(float(lexical), numeric, abs_tol=1e-9):
            raise ValueError("ECB BOP lexical and numeric values differ")
        object.__setattr__(self, "source_uri", _canonical_uri(self.source_uri))
        _required_text(self.source_locator, "source_locator")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "component": self.component,
            "reference_period": self.reference_period,
            "adjustment": self.adjustment,
            "currency": self.currency,
            "scale": self.scale,
            "lexical": self.lexical,
            "value": self.value,
            "source_uri": self.source_uri,
            "source_locator": self.source_locator,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsPublishedValueV1:
        numeric = data.get("value")
        if isinstance(numeric, bool) or not isinstance(numeric, (int, float)):
            raise TypeError("ECB BOP value must be numeric")
        return cls(
            component=str(data.get("component", "")),
            reference_period=str(data.get("reference_period", "")),
            adjustment=str(data.get("adjustment", "")),
            currency=str(data.get("currency", "")),
            scale=str(data.get("scale", "")),
            lexical=str(data.get("lexical", "")),
            value=float(numeric),
            source_uri=str(data.get("source_uri", "")),
            source_locator=str(data.get("source_locator", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsReleaseV1:
    """One normalized monthly ECB balance-of-payments release."""

    entry: EcbBalanceOfPaymentsInventoryEntryV1
    release_date: str | None
    release_date_provenance: str
    time_precision: EconomicTimePrecision | None
    published_at_ns: int | None
    published_lexical: str | None
    migration_timestamp: str | None
    next_release_date: str | None
    next_release_date_source_lexical: str | None
    source_excerpt: str
    artifacts: tuple[EcbBalanceOfPaymentsArtifactV1, ...]
    values: tuple[EcbBalanceOfPaymentsPublishedValueV1, ...]
    revision_disclosure: bool
    release_id: str = ""
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_BALANCE_OF_PAYMENTS_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB BOP release schema")
        if not isinstance(self.entry, EcbBalanceOfPaymentsInventoryEntryV1):
            raise TypeError("ECB BOP release entry is invalid")
        if self.release_date is None:
            if self.release_date_provenance != "unavailable":
                raise ValueError(
                    "undated ECB BOP release has another provenance"
                )
            if any(
                item is not None
                for item in (
                    self.time_precision,
                    self.published_at_ns,
                    self.published_lexical,
                )
            ):
                raise ValueError(
                    "undated ECB BOP release carries publication time"
                )
        else:
            _iso_date(self.release_date, "release_date")
            if self.release_date[:7] <= self.entry.reference_period:
                raise ValueError("ECB BOP release does not follow its period")
            if self.release_date_provenance not in {
                "foedb",
                "document-uri",
                "prior-release-schedule",
                "pdf-metadata",
            }:
                raise ValueError("ECB BOP date provenance is invalid")
            if self.time_precision is None:
                raise ValueError("dated ECB BOP release omits precision")
            if self.time_precision not in {
                EconomicTimePrecision.EXACT_SECOND,
                EconomicTimePrecision.EXACT_MINUTE,
                EconomicTimePrecision.DATE_ONLY,
            }:
                raise ValueError("ECB BOP release time precision is invalid")
            if self.time_precision in {
                EconomicTimePrecision.EXACT_SECOND,
                EconomicTimePrecision.EXACT_MINUTE,
            }:
                if (
                    self.published_at_ns is None
                    or self.published_lexical is None
                ):
                    raise ValueError("timed ECB BOP release omits its clock")
                published_ns = _bounded_int(
                    self.published_at_ns,
                    "published_at_ns",
                    2**63 - 1,
                )
                published_lexical = _required_text(
                    self.published_lexical, "published_lexical"
                )
                try:
                    published = datetime.fromisoformat(published_lexical)
                except ValueError as exc:
                    raise ValueError(
                        "ECB BOP publication clock is invalid"
                    ) from exc
                if (
                    published.tzinfo is None
                    or published.microsecond != 0
                    or published.date().isoformat() != self.release_date
                    or int(published.timestamp()) * 1_000_000_000
                    != published_ns
                ):
                    raise ValueError("ECB BOP publication clock differs")
                if (
                    self.time_precision is EconomicTimePrecision.EXACT_MINUTE
                    and published.second != 0
                ) or (
                    self.time_precision is EconomicTimePrecision.EXACT_SECOND
                    and published.second == 0
                ):
                    raise ValueError("ECB BOP clock precision differs")
            elif (
                self.published_at_ns is not None
                or self.published_lexical is not None
            ):
                raise ValueError(
                    "date-only ECB BOP release carries an exact clock"
                )
        if self.entry.inventory == "current-index":
            if (
                self.release_date != self.entry.inventory_release_date
                or self.release_date_provenance != "foedb"
                or (
                    self.time_precision,
                    self.published_at_ns,
                    self.published_lexical,
                )
                != _publication_time(self.entry, self.release_date)
            ):
                raise ValueError("current ECB BOP publication timing differs")
        elif self.release_date_provenance == "foedb" or self.time_precision in {
            EconomicTimePrecision.EXACT_SECOND,
            EconomicTimePrecision.EXACT_MINUTE,
        }:
            raise ValueError("legacy ECB BOP release carries FOEDB timing")
        if self.migration_timestamp is not None:
            _iso_date(self.migration_timestamp, "migration_timestamp")
        if self.next_release_date is not None:
            _iso_date(self.next_release_date, "next_release_date")
            _required_text(
                self.next_release_date_source_lexical,
                "next_release_date_source_lexical",
            )
        elif self.next_release_date_source_lexical is not None:
            raise ValueError("ECB BOP next-release lexical has no date")
        excerpt = _required_text(self.source_excerpt, "source_excerpt")
        if len(excerpt) > MAX_ECB_BALANCE_OF_PAYMENTS_EXCERPT_CHARS:
            raise ValueError("ECB BOP excerpt exceeds its bound")
        if not self.artifacts or self.artifacts[0].role is not (
            EcbBalanceOfPaymentsArtifactRole.RELEASE_HTML
        ):
            raise ValueError("ECB BOP release omits primary HTML")
        if len(self.artifacts) > MAX_ECB_BALANCE_OF_PAYMENTS_ARTIFACTS:
            raise ValueError("ECB BOP release has too many artifacts")
        artifact_uris = tuple(item.source_uri for item in self.artifacts)
        if (
            self.artifacts[0].source_uri != self.entry.source_uri
            or self.artifacts[0].source_format is not OfficialSourceFormat.HTML
        ):
            raise ValueError("ECB BOP primary artifact differs")
        if len(set(artifact_uris)) != len(artifact_uris):
            raise ValueError("ECB BOP release contains duplicate artifacts")
        if not self.values or self.values[0].component != "current-account":
            raise ValueError("ECB BOP release omits current-account value")
        if any(
            item.reference_period != self.entry.reference_period
            for item in self.values
        ):
            raise ValueError("ECB BOP release value period differs")
        if any(
            item.source_uri != self.entry.source_uri for item in self.values
        ):
            raise ValueError("ECB BOP release value source differs")
        components = tuple(item.component for item in self.values)
        if len(set(components)) != len(components):
            raise ValueError("ECB BOP release repeats a component")
        if not isinstance(self.revision_disclosure, bool):
            raise TypeError("ECB BOP revision_disclosure must be a boolean")
        expected = _stable_id("ecb-balance-of-payments-release", self.payload())
        if self.release_id and self.release_id != expected:
            raise ValueError("ECB BOP release identity differs")
        object.__setattr__(self, "release_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "entry": self.entry.to_dict(),
            "release_date": self.release_date,
            "release_date_provenance": self.release_date_provenance,
            "time_precision": (
                self.time_precision.value
                if self.time_precision is not None
                else None
            ),
            "published_at_ns": self.published_at_ns,
            "published_lexical": self.published_lexical,
            "migration_timestamp": self.migration_timestamp,
            "next_release_date": self.next_release_date,
            "next_release_date_source_lexical": self.next_release_date_source_lexical,
            "source_excerpt": self.source_excerpt,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "values": [item.to_dict() for item in self.values],
            "revision_disclosure": self.revision_disclosure,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsReleaseV1:
        precision = data.get("time_precision")
        revision_disclosure = data.get("revision_disclosure")
        if not isinstance(revision_disclosure, bool):
            raise TypeError("ECB BOP revision_disclosure must be a boolean")
        return cls(
            entry=EcbBalanceOfPaymentsInventoryEntryV1.from_dict(
                _mapping(data.get("entry"), "entry")
            ),
            release_date=cast(str | None, data.get("release_date")),
            release_date_provenance=str(
                data.get("release_date_provenance", "")
            ),
            time_precision=(
                EconomicTimePrecision(str(precision))
                if precision is not None
                else None
            ),
            published_at_ns=cast(int | None, data.get("published_at_ns")),
            published_lexical=cast(str | None, data.get("published_lexical")),
            migration_timestamp=cast(
                str | None, data.get("migration_timestamp")
            ),
            next_release_date=cast(str | None, data.get("next_release_date")),
            next_release_date_source_lexical=cast(
                str | None, data.get("next_release_date_source_lexical")
            ),
            source_excerpt=str(data.get("source_excerpt", "")),
            artifacts=tuple(
                EcbBalanceOfPaymentsArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            values=tuple(
                EcbBalanceOfPaymentsPublishedValueV1.from_dict(
                    _mapping(item, "value")
                )
                for item in _sequence(data.get("values"), "values")
            ),
            revision_disclosure=revision_disclosure,
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsReleaseIndexV1:
    """Complete frozen inventory and FOEDB cross-check."""

    as_of_date: str
    database_version: str
    database_version_hash: str
    database_total_records: int
    database_chunk_size: int
    database_chunk_group_size: int
    database_artifacts: tuple[EcbBalanceOfPaymentsArtifactV1, ...]
    inventory_artifacts: tuple[EcbBalanceOfPaymentsArtifactV1, ...]
    entries: tuple[EcbBalanceOfPaymentsInventoryEntryV1, ...]
    exclusions: tuple[EcbBalanceOfPaymentsExcludedEntryV1, ...]
    support_uris: tuple[str, ...]
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECB_BALANCE_OF_PAYMENTS_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported ECB BOP index schema")
        _iso_date(self.as_of_date, "as_of_date")
        _required_text(self.database_version, "database_version")
        _required_text(self.database_version_hash, "database_version_hash")
        _bounded_int(
            self.database_total_records, "database_total_records", 1_000_000
        )
        _bounded_int(self.database_chunk_size, "database_chunk_size", 1_000_000)
        _bounded_int(
            self.database_chunk_group_size,
            "database_chunk_group_size",
            1_000_000,
        )
        if self.database_chunk_size <= 0 or self.database_chunk_group_size <= 0:
            raise ValueError("ECB BOP database chunk sizes must be positive")
        expected_database_artifacts = 2 + math.ceil(
            self.database_total_records / self.database_chunk_size
        )
        if len(self.database_artifacts) != expected_database_artifacts:
            raise ValueError("ECB BOP database artifact coverage differs")
        if tuple(item.role for item in self.database_artifacts[:2]) != (
            EcbBalanceOfPaymentsArtifactRole.FOEDB_VERSIONS,
            EcbBalanceOfPaymentsArtifactRole.FOEDB_METADATA,
        ) or any(
            item.role is not EcbBalanceOfPaymentsArtifactRole.FOEDB_CHUNK
            for item in self.database_artifacts[2:]
        ):
            raise ValueError("ECB BOP database artifact roles differ")
        if tuple(item.role for item in self.inventory_artifacts) != (
            EcbBalanceOfPaymentsArtifactRole.CURRENT_INDEX,
            EcbBalanceOfPaymentsArtifactRole.LEGACY_INDEX,
        ):
            raise ValueError("ECB BOP inventory artifact roles differ")
        inventory_uris = tuple(
            item.source_uri
            for item in (*self.database_artifacts, *self.inventory_artifacts)
        )
        if len(set(inventory_uris)) != len(inventory_uris):
            raise ValueError("ECB BOP inventory repeats an artifact")
        if len(self.entries) > MAX_ECB_BALANCE_OF_PAYMENTS_RELEASES:
            raise ValueError("ECB BOP index exceeds release bound")
        if len(self.exclusions) > MAX_ECB_BALANCE_OF_PAYMENTS_EXCLUSIONS:
            raise ValueError("ECB BOP index exceeds exclusion bound")
        periods = tuple(item.reference_period for item in self.entries)
        expected = tuple(
            item
            for item in _month_range(
                ECB_BALANCE_OF_PAYMENTS_ARCHIVE_START_PERIOD,
                ECB_LATEST_PACKAGED_BALANCE_OF_PAYMENTS_PERIOD,
            )
            if item not in _MISSING_REFERENCE_PERIODS
        )
        if periods != expected:
            raise ValueError("ECB BOP reference-period lineage differs")
        if any(
            item.inventory_release_date is not None
            and item.inventory_release_date > self.as_of_date
            for item in self.entries
        ):
            raise ValueError("ECB BOP index includes a future release")
        if len({item.source_uri for item in self.entries}) != len(self.entries):
            raise ValueError("ECB BOP index contains duplicate release URI")
        if len({item.source_uri for item in self.exclusions}) != len(
            self.exclusions
        ):
            raise ValueError("ECB BOP index contains duplicate exclusion URI")
        if len(self.support_uris) > MAX_ECB_BALANCE_OF_PAYMENTS_ARTIFACTS:
            raise ValueError("ECB BOP index exceeds support-artifact bound")
        if tuple(sorted(set(self.support_uris))) != self.support_uris:
            raise ValueError("ECB BOP support URIs are not canonical")
        if tuple(_canonical_uri(item) for item in self.support_uris) != (
            self.support_uris
        ):
            raise ValueError("ECB BOP support URI differs")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "database_version": self.database_version,
            "database_version_hash": self.database_version_hash,
            "database_total_records": self.database_total_records,
            "database_chunk_size": self.database_chunk_size,
            "database_chunk_group_size": self.database_chunk_group_size,
            "database_artifacts": [
                item.to_dict() for item in self.database_artifacts
            ],
            "inventory_artifacts": [
                item.to_dict() for item in self.inventory_artifacts
            ],
            "entries": [item.to_dict() for item in self.entries],
            "exclusions": [item.to_dict() for item in self.exclusions],
            "support_uris": list(self.support_uris),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            database_version=str(data.get("database_version", "")),
            database_version_hash=str(data.get("database_version_hash", "")),
            database_total_records=cast(
                int, data.get("database_total_records")
            ),
            database_chunk_size=cast(int, data.get("database_chunk_size")),
            database_chunk_group_size=cast(
                int, data.get("database_chunk_group_size")
            ),
            database_artifacts=tuple(
                EcbBalanceOfPaymentsArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(
                    data.get("database_artifacts"), "database_artifacts"
                )
            ),
            inventory_artifacts=tuple(
                EcbBalanceOfPaymentsArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(
                    data.get("inventory_artifacts"), "inventory_artifacts"
                )
            ),
            entries=tuple(
                EcbBalanceOfPaymentsInventoryEntryV1.from_dict(
                    _mapping(item, "entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            exclusions=tuple(
                EcbBalanceOfPaymentsExcludedEntryV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            support_uris=tuple(
                str(item)
                for item in _sequence(data.get("support_uris"), "support_uris")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


class _BopHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_main = False
        self.main_depth = 0
        self.in_h1 = False
        self.h1: list[str] = []
        self.text: list[str] = []
        self.links: list[tuple[str, str]] = []
        self._link_href: str | None = None
        self._link_text: list[str] = []
        self.published_meta: str | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = dict(attrs)
        if tag == "meta" and values.get("property") == "article:published_time":
            self.published_meta = values.get("content")
        if tag == "main":
            self.in_main = True
            self.main_depth = 1
            return
        if self.in_main:
            self.main_depth += 1
            if tag == "h1":
                self.in_h1 = True
            if tag == "a" and values.get("href"):
                self._link_href = values["href"]
                self._link_text = []

    def handle_endtag(self, tag: str) -> None:
        if not self.in_main:
            return
        if tag == "h1":
            self.in_h1 = False
        if tag == "a" and self._link_href is not None:
            self.links.append(
                (self._link_href, " ".join(" ".join(self._link_text).split()))
            )
            self._link_href = None
            self._link_text = []
        if tag == "main":
            self.in_main = False
            self.main_depth = 0
            return
        self.main_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.in_main:
            return
        value = " ".join(data.split())
        if not value:
            return
        self.text.append(value)
        if self.in_h1:
            self.h1.append(value)
        if self._link_href is not None:
            self._link_text.append(value)


def _parse_html(content: bytes) -> _BopHtmlParser:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("windows-1252")
    parser = _BopHtmlParser()
    parser.feed(text)
    parser.close()
    return parser


def _period_from_title(title: str) -> str:
    match = _CURRENT_TITLE_RE.search(title.replace("\xa0", " "))
    if match is None:
        raise ValueError(f"ECB BOP title omits reference month: {title}")
    month_number = _MONTH_LOOKUP[match.group("month").lower()]
    return _month(
        f"{int(match.group('year')):04d}-{month_number:02d}",
        "reference_period",
    )


def _composition(period: str) -> str:
    if period <= "2000-12":
        return "EA11"
    if period <= "2006-12":
        return "EA12"
    if period <= "2007-12":
        return "EA13"
    if period <= "2008-12":
        return "EA15"
    if period <= "2010-12":
        return "EA16"
    if period <= "2013-12":
        return "EA17"
    if period <= "2014-12":
        return "EA18"
    if period <= "2022-12":
        return "EA19"
    if period <= "2025-12":
        return "EA20"
    return "EA21"


def _inventory_entry(
    *,
    period: str,
    title: str,
    uri: str,
    inventory: str,
    release_date: str | None = None,
    foedb_record_id: int | None = None,
    foedb_timestamp: int | None = None,
) -> EcbBalanceOfPaymentsInventoryEntryV1:
    return EcbBalanceOfPaymentsInventoryEntryV1(
        reference_period=period,
        source_title=title,
        source_uri=uri,
        inventory=inventory,
        bpm_era="BPM5" if period <= "2014-08" else "BPM6",
        euro_area_composition=_composition(period),
        inventory_release_date=release_date,
        foedb_record_id=foedb_record_id,
        foedb_timestamp=foedb_timestamp,
    )


def build_ecb_balance_of_payments_index_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build exact requests for the two official release inventories."""
    return (
        _request(
            registry,
            ECB_BALANCE_OF_PAYMENTS_CURRENT_INDEX_URI,
            OfficialSourceFormat.HTML,
        ),
        _request(
            registry,
            ECB_BALANCE_OF_PAYMENTS_LEGACY_INDEX_URI,
            OfficialSourceFormat.HTML,
        ),
    )


def _current_index_rows(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[
    tuple[tuple[str, str, str], ...],
    tuple[EcbBalanceOfPaymentsExcludedEntryV1, ...],
    tuple[str, ...],
]:
    if (
        _canonical_uri(snapshot.request.uri)
        != ECB_BALANCE_OF_PAYMENTS_CURRENT_INDEX_URI
    ):
        raise ValueError("ECB BOP current-index URI differs")
    try:
        text = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("ECB BOP current index is not UTF-8") from exc
    chunks = re.split(r"(?=<dt\s+isoDate=)", text, flags=re.IGNORECASE)[1:]
    monthly: list[tuple[str, str, str]] = []
    excluded: list[EcbBalanceOfPaymentsExcludedEntryV1] = []
    support: set[str] = set()
    for body in chunks:
        date_match = re.match(
            r'<dt\s+isoDate="(?P<date>\d{4}-\d{2}-\d{2})"[^>]*>',
            body,
            re.IGNORECASE,
        )
        category_match = re.search(
            r'<div class="category">(?P<category>.*?)</div>', body, re.DOTALL
        )
        title_match = re.search(
            r'<div class="title"><a href="(?P<href>[^"]+)"[^>]*>'
            r"(?P<title>.*?)</a></div>",
            body,
            re.DOTALL,
        )
        if date_match is None or category_match is None or title_match is None:
            raise ValueError("ECB BOP current index row differs")
        category = re.sub(r"<[^>]+>", " ", category_match.group("category"))
        category = " ".join(category.split())
        title = re.sub(r"<[^>]+>", " ", title_match.group("title"))
        title = " ".join(title.replace("&nbsp;", " ").split())
        uri = _canonical_uri(urljoin(_ECB_BASE_URI, title_match.group("href")))
        release_date = date_match.group("date")
        if (
            category == "BALANCE OF PAYMENTS (MONTHLY)"
            and not title.lower().startswith(("table", "annex"))
        ):
            monthly.append((release_date, title, uri))
        elif category == "BALANCE OF PAYMENTS (QUARTERLY)":
            excluded.append(
                EcbBalanceOfPaymentsExcludedEntryV1(
                    source_title=title,
                    source_uri=uri,
                    reason="quarterly-bop-iip",
                    inventory="current-index",
                    release_date=release_date,
                )
            )
        elif category in {"BALANCE OF PAYMENTS (MONTHLY)", "ANNEX"}:
            support.add(uri)
        else:
            raise ValueError(
                f"unclassified ECB BOP current-index row: {category}"
            )
    return tuple(monthly), tuple(excluded), tuple(sorted(support))


def _legacy_index_rows(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[
    tuple[tuple[str, str, str], ...],
    tuple[EcbBalanceOfPaymentsExcludedEntryV1, ...],
]:
    if (
        _canonical_uri(snapshot.request.uri)
        != ECB_BALANCE_OF_PAYMENTS_LEGACY_INDEX_URI
    ):
        raise ValueError("ECB BOP legacy-index URI differs")
    try:
        text = snapshot.content.decode("utf-8")
    except UnicodeDecodeError:
        text = snapshot.content.decode("windows-1252")
    rows: list[tuple[str, str, str]] = []
    excluded: list[EcbBalanceOfPaymentsExcludedEntryV1] = []
    for anchor in re.finditer(
        r"<a(?P<attrs>[^>]*)>(?P<label>.*?)</a>",
        text,
        re.DOTALL | re.IGNORECASE,
    ):
        href_match = re.search(
            r'href="(?P<href>[^"]+)"', anchor.group("attrs"), re.IGNORECASE
        )
        if href_match is None:
            continue
        href = href_match.group("href")
        if "/press/stats/bop/" not in href:
            continue
        label = " ".join(re.sub(r"<[^>]+>", " ", anchor.group("label")).split())
        uri = _canonical_uri(urljoin(_ECB_BASE_URI, href))
        if re.search(r"/ba\d+\.en\.html$", urlsplit(uri).path):
            excluded.append(
                EcbBalanceOfPaymentsExcludedEntryV1(
                    source_title=f"Annual developments {label}",
                    source_uri=uri,
                    reason="annual-bop-iip",
                    inventory="legacy-index",
                )
            )
            continue
        title_match = re.search(
            rf"press release of (?P<month>{_MONTH_ABBR_PATTERN})\s+"
            rf"(?P<year>\d{{4}})",
            anchor.group("attrs"),
            re.IGNORECASE,
        )
        if title_match is not None:
            period = (
                f"{int(title_match.group('year')):04d}-"
                f"{_MONTH_ABBR_LOOKUP[title_match.group('month').lower()]:02d}"
            )
            rows.append((period, label, uri))
            continue
        match = _LEGACY_URI_PERIOD_RE.search(urlsplit(uri).path)
        if match is None:
            continue
        legacy = match.group("legacy")
        if legacy is None:
            # Annual legacy releases use baYYMMDD names and never match.
            continue
        short_year = int(legacy[:2])
        year = 1900 + short_year if short_year >= 90 else 2000 + short_year
        period = f"{year:04d}-{int(legacy[2:]):02d}"
        rows.append((period, label, uri))
    unique = {item[2]: item for item in rows}
    if len(unique) != len(rows):
        raise ValueError("ECB BOP legacy index contains duplicate URI")
    return tuple(unique.values()), tuple(excluded)


def parse_ecb_balance_of_payments_inventory(
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    current_index_snapshot: OfficialRawSnapshotV1,
    legacy_index_snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> EcbBalanceOfPaymentsReleaseIndexV1:
    """Cross-check FOEDB type 53 and merge the two ECB inventories."""
    boundary = _iso_date(as_of_date, "as_of_date")
    database = _decode_foedb(
        versions_snapshot, metadata_snapshot, chunk_snapshots
    )
    current_rows, current_exclusions, support_uris = _current_index_rows(
        current_index_snapshot
    )
    legacy_rows, legacy_exclusions = _legacy_index_rows(legacy_index_snapshot)

    foedb: dict[str, tuple[int, int, str]] = {}
    for record in database.records:
        if record.get("type") != 53:
            continue
        properties = _mapping(
            record.get("publicationProperties") or {}, "publicationProperties"
        )
        title = _required_text(properties.get("Title"), "FOEDB title")
        documents = tuple(
            str(item)
            for item in _sequence(record.get("documentTypes"), "documentTypes")
        )
        english = [
            _canonical_uri(urljoin(_ECB_BASE_URI, item))
            for item in documents
            if item.endswith(".en.html")
        ]
        if len(english) != 1:
            raise ValueError("ECB BOP type-53 record English document differs")
        timestamp = _bounded_int(
            record.get("pub_timestamp"), "FOEDB timestamp", 2**63 - 1
        )
        record_id = _bounded_int(record.get("id"), "FOEDB record id", 2**63 - 1)
        local_date = (
            datetime.fromtimestamp(timestamp, timezone.utc)
            .astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
            .date()
            .isoformat()
        )
        if local_date <= boundary:
            foedb[english[0]] = (record_id, timestamp, title)

    entries: list[EcbBalanceOfPaymentsInventoryEntryV1] = []
    for period, label, uri in legacy_rows:
        if period > ECB_LATEST_PACKAGED_BALANCE_OF_PAYMENTS_PERIOD:
            continue
        entries.append(
            _inventory_entry(
                period=period,
                title=label,
                uri=uri,
                inventory="legacy-index",
            )
        )
    for release_date, title, uri in current_rows:
        if release_date > boundary:
            continue
        foedb_record = foedb.get(uri)
        if foedb_record is None:
            raise ValueError(
                "ECB BOP current entry is absent from FOEDB type 53"
            )
        if foedb_record[2].replace("\xa0", " ") != title.replace("\xa0", " "):
            raise ValueError("ECB BOP current-index and FOEDB titles differ")
        entries.append(
            _inventory_entry(
                period=_period_from_title(title),
                title=title,
                uri=uri,
                inventory="current-index",
                release_date=release_date,
                foedb_record_id=foedb_record[0],
                foedb_timestamp=foedb_record[1],
            )
        )
    entries.sort(key=lambda item: item.reference_period)
    selected_current_uris = {
        item.source_uri for item in entries if item.inventory == "current-index"
    }
    if selected_current_uris != set(foedb):
        raise ValueError("ECB BOP current inventory and FOEDB type 53 differ")

    database_snapshots = (
        versions_snapshot,
        metadata_snapshot,
        *chunk_snapshots,
    )
    database_artifacts = tuple(
        _artifact(
            snapshot,
            (
                EcbBalanceOfPaymentsArtifactRole.FOEDB_VERSIONS
                if index == 0
                else (
                    EcbBalanceOfPaymentsArtifactRole.FOEDB_METADATA
                    if index == 1
                    else EcbBalanceOfPaymentsArtifactRole.FOEDB_CHUNK
                )
            ),
        )
        for index, snapshot in enumerate(database_snapshots)
    )
    return EcbBalanceOfPaymentsReleaseIndexV1(
        as_of_date=boundary,
        database_version=database.version,
        database_version_hash=database.version_hash,
        database_total_records=database.total_records,
        database_chunk_size=database.chunk_size,
        database_chunk_group_size=database.chunk_group_size,
        database_artifacts=database_artifacts,
        inventory_artifacts=(
            _artifact(
                current_index_snapshot,
                EcbBalanceOfPaymentsArtifactRole.CURRENT_INDEX,
            ),
            _artifact(
                legacy_index_snapshot,
                EcbBalanceOfPaymentsArtifactRole.LEGACY_INDEX,
            ),
        ),
        entries=tuple(entries),
        exclusions=(*legacy_exclusions, *current_exclusions),
        support_uris=tuple(sorted(set(support_uris))),
    )


def build_ecb_balance_of_payments_release_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbBalanceOfPaymentsReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected monthly release."""
    return tuple(
        _request(registry, item.source_uri, OfficialSourceFormat.HTML)
        for item in release_index.entries
    )


def _support_link(uri: str, href: str, label: str) -> str | None:
    candidate = urljoin(uri, href)
    parsed = urlsplit(candidate)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
        "www.ecb.europa.eu",
        "ecb.europa.eu",
    }:
        return None
    path = parsed.path.lower()
    name = Path(path).name
    release_name = Path(urlsplit(uri).path).name.split(".en.html", 1)[0]
    table_label = label.lower().startswith(("table", "annex"))
    related_name = name.startswith(release_name.lower() + "_")
    legacy_table = bool(
        re.search(r"(?:pr\d{4}bop|bp\d{6})(?:_(?:sa|mki|iip|[123]))", name)
    )
    legacy_pdf = path.startswith("/press/pdf/bop/")
    if not (table_label or related_name or legacy_table or legacy_pdf):
        return None
    if not path.endswith((".pdf", ".en.html")):
        return None
    return _canonical_uri(candidate)


def discover_ecb_balance_of_payments_support_requests(
    registry: OfficialSourceRegistryV1,
    release_index: EcbBalanceOfPaymentsReleaseIndexV1,
    release_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Discover release-local English table and annex artifacts."""
    uris = set(release_index.support_uris)
    selected = {item.source_uri for item in release_index.entries}
    if set(release_snapshots) != selected:
        raise ValueError("ECB BOP release snapshot coverage differs")
    for uri in sorted(selected):
        parser = _parse_html(release_snapshots[uri].content)
        for href, label in parser.links:
            support = _support_link(uri, href, label)
            if support is not None and support not in selected:
                uris.add(support)
    return tuple(
        _request(registry, uri, _source_format(uri)) for uri in sorted(uris)
    )


def _next_release(
    text: str, reference_period: str
) -> tuple[str | None, str | None]:
    if reference_period in _SCHEDULE_YEAR_CORRECTIONS:
        corrected = _SCHEDULE_YEAR_CORRECTIONS[reference_period]
        return corrected, "26 January 2004 [source year corrected to 2005]"
    match = _NEXT_RELEASE_RE.search(text) or _SCHEDULED_RELEASE_RE.search(text)
    if match is None:
        return None, None
    lexical = match.group(0)
    year_text = match.group("year")
    if year_text is None:
        current = date.fromisoformat(f"{reference_period}-01")
        month_number = _MONTH_LOOKUP[match.group("month").lower()]
        year = current.year + (1 if month_number <= current.month else 0)
    else:
        year = int(year_text)
    value = date(
        year,
        _MONTH_LOOKUP[match.group("month").lower()],
        int(match.group("day")),
    ).isoformat()
    return value, lexical


def _uri_release_date(uri: str) -> str | None:
    match = _DATED_URI_RE.search(Path(urlsplit(uri).path).name)
    if match is None:
        return None
    token = match.group("date")
    if len(token) == 8:
        year, month, day = int(token[:4]), int(token[4:6]), int(token[6:])
    else:
        short_year = int(token[:2])
        year = 1900 + short_year if short_year >= 90 else 2000 + short_year
        month, day = int(token[2:4]), int(token[4:])
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _pdf_metadata_date(content: bytes) -> str | None:
    try:
        metadata = PdfReader(BytesIO(content)).metadata
    except (OSError, PdfReadError, ValueError):
        return None
    if metadata is None:
        return None
    subject = str(metadata.get("/Subject") or "")
    for pattern in (
        rf"(?P<day>\d{{1,2}})\s+(?P<month>{_MONTH_PATTERN})\s+(?P<year>\d{{4}})",
        rf"(?P<month>{_MONTH_PATTERN})\s+(?P<day>\d{{1,2}}),?\s+(?P<year>\d{{4}})",
    ):
        match = re.search(pattern, subject, re.IGNORECASE)
        if match is not None:
            return date(
                int(match.group("year")),
                _MONTH_LOOKUP[match.group("month").lower()],
                int(match.group("day")),
            ).isoformat()
    return None


def _adjustment(text: str, period: str) -> str:
    lower = text.lower()
    if "working day and seasonally adjusted" in lower or (
        "working-day and seasonally adjusted" in lower
    ):
        return "working-day-and-seasonally-adjusted"
    if "seasonally adjusted current account" in lower or (
        period >= "2004-01" and "seasonally adjusted" in lower
    ):
        return "seasonally-adjusted"
    if period <= "2003-12":
        return "not-seasonally-adjusted"
    return "source-unspecified"


def _candidate_score(context: str, period: str) -> int:
    lower = context.lower()
    parsed = date.fromisoformat(f"{period}-01")
    month_label = calendar.month_name[parsed.month].lower()
    score = 0
    if f"{month_label} {parsed.year}" in lower:
        score += 20
    if "seasonally adjusted" in lower:
        score += 12
    if "recorded" in lower or "registered" in lower or "showed" in lower:
        score += 8
    if "12-month" in lower or "cumulated" in lower or "cumulative" in lower:
        score -= 30
    if "compared" in lower and f"{month_label} {parsed.year}" not in lower:
        score -= 10
    return score


def _published_values(
    entry: EcbBalanceOfPaymentsInventoryEntryV1,
    text: str,
) -> tuple[EcbBalanceOfPaymentsPublishedValueV1, ...]:
    normalized = " ".join(text.replace("\xa0", " ").replace("−", "-").split())
    candidates: list[tuple[int, re.Match[str], str]] = []
    for pattern in _CURRENT_ACCOUNT_PATTERNS:
        for match in pattern.finditer(normalized):
            start = max(0, match.start() - 160)
            stop = min(len(normalized), match.end() + 160)
            context = normalized[start:stop]
            candidates.append(
                (
                    _candidate_score(context, entry.reference_period),
                    match,
                    context,
                )
            )
    if not candidates:
        raise ValueError(
            f"ECB BOP release omits current-account balance: {entry.reference_period}"
        )
    _, match, context = max(
        candidates, key=lambda item: (item[0], -item[1].start())
    )
    numeric = float(match.group("value"))
    if match.group("direction").lower() == "deficit":
        numeric = -numeric
    lexical = format(numeric, "g")
    adjustment = _adjustment(context, entry.reference_period)
    result = [
        EcbBalanceOfPaymentsPublishedValueV1(
            component="current-account",
            reference_period=entry.reference_period,
            adjustment=adjustment,
            currency="EUR",
            scale=match.group("scale").lower(),
            lexical=lexical,
            value=numeric,
            source_uri=entry.source_uri,
            source_locator=f"main:text:{match.start()}-{match.end()}",
        )
    ]
    component_map = {
        "goods": "goods",
        "services": "services",
        "income": "income",
        "primary income": "primary-income",
        "secondary income": "secondary-income",
        "current transfers": "current-transfers",
    }
    # Component sentences use an explicit plural direction followed by values
    # in parentheses.  Only those source-authored directions are retained.
    for direction_match in re.finditer(
        r"(?P<direction>surpluses?|deficits?)(?: were recorded)? (?:for|in) "
        r"(?P<body>[^.]{0,400})",
        normalized,
        re.IGNORECASE,
    ):
        sign = (
            -1.0
            if direction_match.group("direction").lower().startswith("deficit")
            else 1.0
        )
        body = direction_match.group("body")
        for item in _COMPONENT_RE.finditer(body):
            component = component_map[item.group("component").lower()]
            if any(existing.component == component for existing in result):
                continue
            number = sign * float(item.group("value"))
            result.append(
                EcbBalanceOfPaymentsPublishedValueV1(
                    component=component,
                    reference_period=entry.reference_period,
                    adjustment=adjustment,
                    currency="EUR",
                    scale=item.group("scale").lower(),
                    lexical=format(number, "g"),
                    value=number,
                    source_uri=entry.source_uri,
                    source_locator=(
                        f"main:text:{direction_match.start() + item.start()}-"
                        f"{direction_match.start() + item.end()}"
                    ),
                )
            )
    return tuple(result)


def _publication_time(
    entry: EcbBalanceOfPaymentsInventoryEntryV1,
    release_date: str | None,
) -> tuple[EconomicTimePrecision | None, int | None, str | None]:
    if release_date is None:
        return None, None, None
    if entry.foedb_timestamp is None:
        return EconomicTimePrecision.DATE_ONLY, None, None
    timestamp = datetime.fromtimestamp(entry.foedb_timestamp, timezone.utc)
    local = timestamp.astimezone(ZoneInfo(ECB_SOURCE_TIMEZONE))
    if local.date().isoformat() != release_date:
        raise ValueError("ECB BOP FOEDB date differs from inventory date")
    if (timestamp.hour, timestamp.minute, timestamp.second) == (0, 0, 0) or (
        local.hour,
        local.minute,
        local.second,
    ) == (0, 0, 0):
        return EconomicTimePrecision.DATE_ONLY, None, None
    precision = (
        EconomicTimePrecision.EXACT_SECOND
        if local.second != 0
        else EconomicTimePrecision.EXACT_MINUTE
    )
    return (
        precision,
        entry.foedb_timestamp * 1_000_000_000,
        local.isoformat(
            timespec=(
                "seconds"
                if precision is EconomicTimePrecision.EXACT_SECOND
                else "minutes"
            )
        ),
    )


def _release_artifacts(
    entry: EcbBalanceOfPaymentsInventoryEntryV1,
    primary: OfficialRawSnapshotV1,
    support_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> tuple[EcbBalanceOfPaymentsArtifactV1, ...]:
    parser = _parse_html(primary.content)
    uris: set[str] = set()
    for href, label in parser.links:
        support = _support_link(entry.source_uri, href, label)
        if support is not None:
            uris.add(support)
    artifacts = [
        _artifact(primary, EcbBalanceOfPaymentsArtifactRole.RELEASE_HTML)
    ]
    for uri in sorted(uris):
        snapshot = support_snapshots.get(uri)
        if snapshot is None:
            raise ValueError(f"ECB BOP support artifact is missing: {uri}")
        if _canonical_uri(snapshot.request.uri) != uri:
            raise ValueError("ECB BOP support snapshot URI differs")
        role = (
            EcbBalanceOfPaymentsArtifactRole.SUPPORT_PDF
            if snapshot.request.source_format is OfficialSourceFormat.PDF
            else EcbBalanceOfPaymentsArtifactRole.SUPPORT_HTML
        )
        artifacts.append(_artifact(snapshot, role))
    return tuple(artifacts)


def _release_date(
    entry: EcbBalanceOfPaymentsInventoryEntryV1,
    scheduled_from_previous: tuple[str, str] | None,
    artifacts: Sequence[EcbBalanceOfPaymentsArtifactV1],
    support_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> tuple[str | None, str]:
    if entry.inventory_release_date is not None:
        return entry.inventory_release_date, "foedb"
    uri_date = _uri_release_date(entry.source_uri)
    if uri_date is not None:
        return uri_date, "document-uri"
    if scheduled_from_previous is not None:
        return scheduled_from_previous[0], "prior-release-schedule"
    for artifact in artifacts:
        if artifact.role is not EcbBalanceOfPaymentsArtifactRole.SUPPORT_PDF:
            continue
        snapshot = support_snapshots[artifact.source_uri]
        metadata_date = _pdf_metadata_date(snapshot.content)
        if metadata_date is not None:
            return metadata_date, "pdf-metadata"
    return None, "unavailable"


@dataclass(frozen=True, slots=True)
class EcbBalanceOfPaymentsArchiveManifestV1:
    """Replayable qualification receipt for the complete monthly lineage."""

    registry_id: str
    source_id: str
    release_index: EcbBalanceOfPaymentsReleaseIndexV1
    releases: tuple[EcbBalanceOfPaymentsReleaseV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    release_count: int
    legacy_release_count: int
    current_release_count: int
    dated_release_count: int
    unavailable_date_count: int
    exact_second_count: int
    exact_minute_count: int
    date_only_count: int
    migration_timestamp_rejection_count: int
    support_html_count: int
    support_pdf_count: int
    published_value_count: int
    component_value_count: int
    revision_disclosure_count: int
    limitations: tuple[str, ...]
    manifest_id: str = ""
    schema_version: str = ECB_BALANCE_OF_PAYMENTS_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECB_BALANCE_OF_PAYMENTS_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("unsupported ECB BOP manifest schema")
        if (
            re.fullmatch(
                r"official-source-registry:sha256:[0-9a-f]{64}",
                self.registry_id,
            )
            is None
        ):
            raise ValueError("ECB BOP registry identity is invalid")
        if (
            re.fullmatch(r"official-source:sha256:[0-9a-f]{64}", self.source_id)
            is None
        ):
            raise ValueError("ECB BOP source identity is invalid")
        if len(self.releases) != len(self.release_index.entries):
            raise ValueError("ECB BOP manifest release coverage differs")
        if (
            tuple(item.entry for item in self.releases)
            != self.release_index.entries
        ):
            raise ValueError("ECB BOP manifest release order differs")
        if any(
            item.release_date is not None
            and item.release_date > self.release_index.as_of_date
            for item in self.releases
        ):
            raise ValueError("ECB BOP manifest includes a future release")
        artifacts = [
            *self.release_index.database_artifacts,
            *self.release_index.inventory_artifacts,
            *(
                artifact
                for item in self.releases
                for artifact in item.artifacts
            ),
        ]
        by_uri = {item.source_uri: item for item in artifacts}
        if len(by_uri) > MAX_ECB_BALANCE_OF_PAYMENTS_ARTIFACTS:
            raise ValueError("ECB BOP manifest exceeds artifact bound")
        if len(by_uri) != self.raw_artifact_count:
            raise ValueError("ECB BOP raw-artifact count differs")
        if len({item.content_sha256 for item in by_uri.values()}) != (
            self.unique_content_sha256_count
        ):
            raise ValueError("ECB BOP unique-content count differs")
        if (
            sum(item.content_length for item in by_uri.values())
            != self.total_content_bytes
        ):
            raise ValueError("ECB BOP total bytes differ")
        if self.total_content_bytes > MAX_ECB_BALANCE_OF_PAYMENTS_TOTAL_BYTES:
            raise ValueError("ECB BOP manifest exceeds byte bound")
        calculated = {
            "release_count": len(self.releases),
            "legacy_release_count": sum(
                item.entry.inventory == "legacy-index" for item in self.releases
            ),
            "current_release_count": sum(
                item.entry.inventory == "current-index"
                for item in self.releases
            ),
            "dated_release_count": sum(
                item.release_date is not None for item in self.releases
            ),
            "unavailable_date_count": sum(
                item.release_date is None for item in self.releases
            ),
            "exact_minute_count": sum(
                item.time_precision is EconomicTimePrecision.EXACT_MINUTE
                for item in self.releases
            ),
            "exact_second_count": sum(
                item.time_precision is EconomicTimePrecision.EXACT_SECOND
                for item in self.releases
            ),
            "date_only_count": sum(
                item.time_precision is EconomicTimePrecision.DATE_ONLY
                for item in self.releases
            ),
            "migration_timestamp_rejection_count": sum(
                item.migration_timestamp is not None
                and item.migration_timestamp != item.release_date
                for item in self.releases
            ),
            "support_html_count": sum(
                artifact.role is EcbBalanceOfPaymentsArtifactRole.SUPPORT_HTML
                for item in self.releases
                for artifact in item.artifacts
            ),
            "support_pdf_count": sum(
                artifact.role is EcbBalanceOfPaymentsArtifactRole.SUPPORT_PDF
                for item in self.releases
                for artifact in item.artifacts
            ),
            "published_value_count": sum(
                len(item.values) for item in self.releases
            ),
            "component_value_count": sum(
                value.component != "current-account"
                for item in self.releases
                for value in item.values
            ),
            "revision_disclosure_count": sum(
                item.revision_disclosure for item in self.releases
            ),
        }
        for name, value in calculated.items():
            if getattr(self, name) != value:
                raise ValueError(f"ECB BOP {name} differs")
        if not self.limitations or any(
            not str(item).strip() for item in self.limitations
        ):
            raise ValueError("ECB BOP manifest requires limitations")
        expected = _stable_id(
            "ecb-balance-of-payments-archive-manifest", self.payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("ECB BOP manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "program_key": ECB_BALANCE_OF_PAYMENTS_PROGRAM_KEY,
            "measure_key": ECB_BALANCE_OF_PAYMENTS_MEASURE_KEY,
            "frequency": "monthly",
            "release_index": self.release_index.to_dict(),
            "releases": [item.to_dict() for item in self.releases],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "release_count": self.release_count,
            "legacy_release_count": self.legacy_release_count,
            "current_release_count": self.current_release_count,
            "dated_release_count": self.dated_release_count,
            "unavailable_date_count": self.unavailable_date_count,
            "exact_second_count": self.exact_second_count,
            "exact_minute_count": self.exact_minute_count,
            "date_only_count": self.date_only_count,
            "migration_timestamp_rejection_count": self.migration_timestamp_rejection_count,
            "support_html_count": self.support_html_count,
            "support_pdf_count": self.support_pdf_count,
            "published_value_count": self.published_value_count,
            "component_value_count": self.component_value_count,
            "revision_disclosure_count": self.revision_disclosure_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        encoded: str = canonical_contract_json(self.to_dict())
        return encoded

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EcbBalanceOfPaymentsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            release_index=EcbBalanceOfPaymentsReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            releases=tuple(
                EcbBalanceOfPaymentsReleaseV1.from_dict(
                    _mapping(item, "release")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            release_count=cast(int, data.get("release_count")),
            legacy_release_count=cast(int, data.get("legacy_release_count")),
            current_release_count=cast(int, data.get("current_release_count")),
            dated_release_count=cast(int, data.get("dated_release_count")),
            unavailable_date_count=cast(
                int, data.get("unavailable_date_count")
            ),
            exact_second_count=cast(int, data.get("exact_second_count")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            date_only_count=cast(int, data.get("date_only_count")),
            migration_timestamp_rejection_count=cast(
                int, data.get("migration_timestamp_rejection_count")
            ),
            support_html_count=cast(int, data.get("support_html_count")),
            support_pdf_count=cast(int, data.get("support_pdf_count")),
            published_value_count=cast(int, data.get("published_value_count")),
            component_value_count=cast(int, data.get("component_value_count")),
            revision_disclosure_count=cast(
                int, data.get("revision_disclosure_count")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(
        cls, payload: str | bytes
    ) -> EcbBalanceOfPaymentsArchiveManifestV1:
        try:
            data = json.loads(payload)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("ECB BOP manifest JSON is invalid") from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_ecb_balance_of_payments_archive_manifest(
    registry: OfficialSourceRegistryV1,
    release_index: EcbBalanceOfPaymentsReleaseIndexV1,
    release_snapshots: Mapping[str, OfficialRawSnapshotV1],
    support_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> EcbBalanceOfPaymentsArchiveManifestV1:
    """Build the complete normalized archive from retained official bytes."""
    source = registry.source(ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY)
    if (
        source.parser_id != ECB_BALANCE_OF_PAYMENTS_PARSER_ID
        or source.parser_version != ECB_BALANCE_OF_PAYMENTS_PARSER_VERSION
    ):
        raise ValueError("ECB BOP registry parser binding differs")
    expected_release_uris = {item.source_uri for item in release_index.entries}
    if set(release_snapshots) != expected_release_uris:
        raise ValueError("ECB BOP release snapshot coverage differs")
    if any(
        snapshot.request.source_id != source.source_id
        for snapshot in (
            *release_snapshots.values(),
            *support_snapshots.values(),
        )
    ):
        raise ValueError("ECB BOP snapshot source identity differs")
    scheduled_by_period: dict[str, tuple[str, str]] = {}
    parsed_pages: dict[str, tuple[_BopHtmlParser, str]] = {}
    expected_support_uris = set(release_index.support_uris)
    for entry in release_index.entries:
        parser = _parse_html(release_snapshots[entry.source_uri].content)
        text = " ".join(parser.text)
        parsed_pages[entry.reference_period] = (parser, text)
        for href, label in parser.links:
            support = _support_link(entry.source_uri, href, label)
            if support is not None and support not in expected_release_uris:
                expected_support_uris.add(support)
        next_date, lexical = _next_release(text, entry.reference_period)
        if next_date is not None and lexical is not None:
            scheduled_by_period[_shift_month(entry.reference_period, 1)] = (
                next_date,
                lexical,
            )
    if set(support_snapshots) != expected_support_uris:
        raise ValueError("ECB BOP support snapshot coverage differs")

    # Several migrated 2003 pages repeat 27 May as the next date.  Retain the
    # first plausible occurrence, but never project one repeated or same-month
    # schedule onto later reference periods.
    used_schedule_dates: set[str] = set()
    for period in sorted(scheduled_by_period):
        scheduled_date = scheduled_by_period[period][0]
        if (
            scheduled_date[:7] <= period
            or scheduled_date in used_schedule_dates
        ):
            del scheduled_by_period[period]
            continue
        used_schedule_dates.add(scheduled_date)

    releases: list[EcbBalanceOfPaymentsReleaseV1] = []
    for entry in release_index.entries:
        primary = release_snapshots[entry.source_uri]
        parser, text = parsed_pages[entry.reference_period]
        artifacts = _release_artifacts(entry, primary, support_snapshots)
        release_date, date_provenance = _release_date(
            entry,
            scheduled_by_period.get(entry.reference_period),
            artifacts,
            support_snapshots,
        )
        precision, published_ns, published_lexical = _publication_time(
            entry, release_date
        )
        migration = parser.published_meta
        if migration is not None:
            try:
                migration = _iso_date(migration[:10], "migration timestamp")
            except ValueError:
                migration = None
        next_date, next_lexical = _next_release(text, entry.reference_period)
        values = _published_values(entry, text)
        excerpt = " ".join(text.split())[
            :MAX_ECB_BALANCE_OF_PAYMENTS_EXCERPT_CHARS
        ]
        releases.append(
            EcbBalanceOfPaymentsReleaseV1(
                entry=entry,
                release_date=release_date,
                release_date_provenance=date_provenance,
                time_precision=precision,
                published_at_ns=published_ns,
                published_lexical=published_lexical,
                migration_timestamp=migration,
                next_release_date=next_date,
                next_release_date_source_lexical=next_lexical,
                source_excerpt=excerpt,
                artifacts=artifacts,
                values=values,
                revision_disclosure=bool(
                    re.search(r"\b(?:data )?revisions?\b", text, re.IGNORECASE)
                ),
            )
        )

    retained_support_uris = {
        artifact.source_uri
        for item in releases
        for artifact in item.artifacts
        if artifact.role
        in {
            EcbBalanceOfPaymentsArtifactRole.SUPPORT_HTML,
            EcbBalanceOfPaymentsArtifactRole.SUPPORT_PDF,
        }
    }
    if retained_support_uris != set(support_snapshots):
        raise ValueError("ECB BOP retained support coverage differs")

    all_artifacts = [
        *release_index.database_artifacts,
        *release_index.inventory_artifacts,
        *(artifact for item in releases for artifact in item.artifacts),
    ]
    by_uri = {item.source_uri: item for item in all_artifacts}
    return EcbBalanceOfPaymentsArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        release_index=release_index,
        releases=tuple(releases),
        raw_artifact_count=len(by_uri),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in by_uri.values()}
        ),
        total_content_bytes=sum(
            item.content_length for item in by_uri.values()
        ),
        release_count=len(releases),
        legacy_release_count=sum(
            item.entry.inventory == "legacy-index" for item in releases
        ),
        current_release_count=sum(
            item.entry.inventory == "current-index" for item in releases
        ),
        dated_release_count=sum(
            item.release_date is not None for item in releases
        ),
        unavailable_date_count=sum(
            item.release_date is None for item in releases
        ),
        exact_second_count=sum(
            item.time_precision is EconomicTimePrecision.EXACT_SECOND
            for item in releases
        ),
        exact_minute_count=sum(
            item.time_precision is EconomicTimePrecision.EXACT_MINUTE
            for item in releases
        ),
        date_only_count=sum(
            item.time_precision is EconomicTimePrecision.DATE_ONLY
            for item in releases
        ),
        migration_timestamp_rejection_count=sum(
            item.migration_timestamp is not None
            and item.migration_timestamp != item.release_date
            for item in releases
        ),
        support_html_count=sum(
            artifact.role is EcbBalanceOfPaymentsArtifactRole.SUPPORT_HTML
            for item in releases
            for artifact in item.artifacts
        ),
        support_pdf_count=sum(
            artifact.role is EcbBalanceOfPaymentsArtifactRole.SUPPORT_PDF
            for item in releases
            for artifact in item.artifacts
        ),
        published_value_count=sum(len(item.values) for item in releases),
        component_value_count=sum(
            value.component != "current-account"
            for item in releases
            for value in item.values
        ),
        revision_disclosure_count=sum(
            item.revision_disclosure for item in releases
        ),
        limitations=(
            "The official legacy inventory omits January and March 2000; no release occurrence is fabricated for either reference month.",
            "Some early legacy pages do not expose a reliable source-authored publication day; those releases remain explicitly undated instead of using a migration timestamp or a repeated 2003 schedule date.",
            "Published release values remain distinct from later ECB Data Portal revisions, and no market-consensus or surprise history is manufactured.",
            "Component values are retained only when the release narrative binds an explicit surplus or deficit to an explicit EUR amount.",
        ),
    )


def replay_ecb_balance_of_payments_archive(
    registry: OfficialSourceRegistryV1,
    manifest: EcbBalanceOfPaymentsArchiveManifestV1,
    versions_snapshot: OfficialRawSnapshotV1,
    metadata_snapshot: OfficialRawSnapshotV1,
    chunk_snapshots: Sequence[OfficialRawSnapshotV1],
    current_index_snapshot: OfficialRawSnapshotV1,
    legacy_index_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Mapping[str, OfficialRawSnapshotV1],
    support_snapshots: Mapping[str, OfficialRawSnapshotV1],
) -> EcbBalanceOfPaymentsArchiveManifestV1:
    """Rebuild and byte-compare one retained archive manifest."""
    if manifest.registry_id != registry.registry_id:
        raise ValueError("ECB BOP replay registry identity differs")
    rebuilt_index = parse_ecb_balance_of_payments_inventory(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        current_index_snapshot,
        legacy_index_snapshot,
        as_of_date=manifest.release_index.as_of_date,
    )
    if rebuilt_index != manifest.release_index:
        raise ValueError("ECB BOP replay inventory differs")
    rebuilt = build_ecb_balance_of_payments_archive_manifest(
        registry,
        rebuilt_index,
        release_snapshots,
        support_snapshots,
    )
    if rebuilt.to_json() != manifest.to_json():
        raise ValueError("ECB BOP replay differs from retained manifest")
    return rebuilt


def packaged_ecb_balance_of_payments_manifest_path() -> Path:
    """Return the packaged monthly balance-of-payments manifest path."""
    return Path(__file__).with_name("assets") / (
        "ecb_balance_of_payments_archive_v1.json"
    )


def load_packaged_ecb_balance_of_payments_archive_manifest() -> (
    EcbBalanceOfPaymentsArchiveManifestV1
):
    """Load and validate the packaged monthly balance-of-payments archive."""
    path = packaged_ecb_balance_of_payments_manifest_path()
    return EcbBalanceOfPaymentsArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


__all__ = [
    "ECB_BALANCE_OF_PAYMENTS_ARCHIVE_START_PERIOD",
    "ECB_BALANCE_OF_PAYMENTS_ARTIFACT_SCHEMA_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_CURRENT_INDEX_URI",
    "ECB_BALANCE_OF_PAYMENTS_ENTRY_SCHEMA_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_EXCLUSION_SCHEMA_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_INDEX_SCHEMA_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_LEGACY_INDEX_URI",
    "ECB_BALANCE_OF_PAYMENTS_MANIFEST_SCHEMA_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_MEASURE_KEY",
    "ECB_BALANCE_OF_PAYMENTS_PARSER_ID",
    "ECB_BALANCE_OF_PAYMENTS_PARSER_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_PROGRAM_KEY",
    "ECB_BALANCE_OF_PAYMENTS_RELEASE_SCHEMA_VERSION",
    "ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY",
    "ECB_BALANCE_OF_PAYMENTS_VALUE_SCHEMA_VERSION",
    "ECB_LATEST_PACKAGED_BALANCE_OF_PAYMENTS_PERIOD",
    "EcbBalanceOfPaymentsArchiveManifestV1",
    "EcbBalanceOfPaymentsArtifactRole",
    "EcbBalanceOfPaymentsArtifactV1",
    "EcbBalanceOfPaymentsExcludedEntryV1",
    "EcbBalanceOfPaymentsInventoryEntryV1",
    "EcbBalanceOfPaymentsPublishedValueV1",
    "EcbBalanceOfPaymentsReleaseIndexV1",
    "EcbBalanceOfPaymentsReleaseV1",
    "build_ecb_balance_of_payments_archive_manifest",
    "build_ecb_balance_of_payments_foedb_chunk_requests",
    "build_ecb_balance_of_payments_foedb_metadata_request",
    "build_ecb_balance_of_payments_foedb_versions_request",
    "build_ecb_balance_of_payments_index_requests",
    "build_ecb_balance_of_payments_release_requests",
    "discover_ecb_balance_of_payments_support_requests",
    "load_packaged_ecb_balance_of_payments_archive_manifest",
    "packaged_ecb_balance_of_payments_manifest_path",
    "parse_ecb_balance_of_payments_foedb_version",
    "parse_ecb_balance_of_payments_inventory",
    "replay_ecb_balance_of_payments_archive",
]

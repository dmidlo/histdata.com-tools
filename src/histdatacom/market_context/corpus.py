"""Production market-context sources and immutable corpus artifacts.

The contracts in :mod:`histdatacom.market_context.contracts` define the
point-in-time event and query boundary.  This module supplies legally usable
official-source adapters, content-addressed acquisition evidence, deterministic
replay, coverage diagnostics, and installed-code loading seams for carving and
benchmark work.

Source snapshots are intentionally retained outside row-aligned market data.
Reconstruction windows consume the existing bounded ``MarketContextQueryV1``
sidecar, never repeated macro/news columns on every tick.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import resource
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import requests

from histdatacom.market_context.contracts import (
    MAX_MARKET_CONTEXT_EVENTS,
    MarketContextEventV1,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextQueryV1,
    MarketContextSourceV1,
    MarketContextTimelineV1,
    MarketContextView,
    normalize_market_context_datetime,
    query_market_context,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.market_context.contracts import canonical_contract_json

MARKET_CONTEXT_CORPUS_SCHEMA_VERSION = "histdatacom.market-context-corpus.v1"
MARKET_CONTEXT_SOURCE_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.market-context-source-evidence.v1"
)
MARKET_CONTEXT_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.market-context-coverage.v1"
)
MARKET_CONTEXT_PREFLIGHT_SCHEMA_VERSION = (
    "histdatacom.market-context-preflight.v1"
)
OPERATOR_MARKET_CONTEXT_CATALOG_SCHEMA_VERSION = (
    "histdatacom.operator-market-context-catalog.v1"
)

ONS_RELEASES_URI = "https://api.beta.ons.gov.uk/v1/search/releases"
ECB_POLICY_RATE_SERIES_KEY = "FM.D.U2.EUR.4F.KR.MRR_RT.LEV"
ECB_POLICY_RATE_URI = (
    "https://data-api.ecb.europa.eu/service/data/"
    "FM/D.U2.EUR.4F.KR.MRR_RT.LEV"
)
BOE_BANK_RATE_URI = (
    "https://www.bankofengland.co.uk/boeapps/database/" "Bank-Rate.asp?hl=en-GB"
)
FED_FOMC_CALENDAR_URI = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)
FED_FOMC_HISTORICAL_URI = (
    "https://www.federalreserve.gov/monetarypolicy/fomchistorical{year}.htm"
)

DEFAULT_ONS_QUERIES = (
    "consumer price inflation",
    "gross domestic product",
    "labour market",
    "retail sales",
    "public sector finances",
)
DEFAULT_MARKET_CONTEXT_SOURCES = (
    "ons",
    "ecb",
    "boe",
    "fed",
    "operator",
)
DEFAULT_USER_AGENT = (
    "histdatacom-market-context/2.1.0 "
    "(+https://github.com/dmidlo/histdata.com-tools)"
)

DAY_NS = 86_400_000_000_000
HOUR_NS = 3_600_000_000_000
MAX_CORPUS_BYTES = 64 * 1024 * 1024
MAX_SOURCE_EVIDENCE = 64
MAX_COVERAGE_SLICES = 128
MAX_DIAGNOSTICS = 256
_SOURCE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ONS_TITLE_PATTERNS = (
    re.compile(r"^consumer price inflation, uk:", re.IGNORECASE),
    re.compile(
        r"^gdp (?:monthly|first quarterly|quarterly national accounts)",
        re.IGNORECASE,
    ),
    re.compile(r"^labour market overview, uk:", re.IGNORECASE),
    re.compile(r"^retail sales, great britain:", re.IGNORECASE),
    re.compile(r"^public sector finances, uk:", re.IGNORECASE),
)


@dataclass(frozen=True, slots=True)
class MarketContextFetchProfileV1:
    """Bounded official-source acquisition and corpus-build policy."""

    start_date: str
    end_date: str
    sources: tuple[str, ...] = DEFAULT_MARKET_CONTEXT_SOURCES
    ons_queries: tuple[str, ...] = DEFAULT_ONS_QUERIES
    timeout_seconds: float = 30.0
    max_response_bytes: int = 16 * 1024 * 1024
    max_total_source_bytes: int = 64 * 1024 * 1024
    max_ons_pages_per_query: int = 8
    max_events: int = MAX_MARKET_CONTEXT_EVENTS
    max_runtime_seconds: float = 300.0

    def __post_init__(self) -> None:
        start = _parse_date(self.start_date, "start_date")
        end = _parse_date(self.end_date, "end_date")
        if end < start:
            raise ValueError("market-context end_date precedes start_date")
        sources = tuple(sorted({_source_key(item) for item in self.sources}))
        unsupported = set(sources).difference(DEFAULT_MARKET_CONTEXT_SOURCES)
        if unsupported:
            raise ValueError(
                "unsupported market-context source: "
                + ", ".join(sorted(unsupported))
            )
        queries = tuple(
            dict.fromkeys(_required_text(item) for item in self.ons_queries)
        )
        if len(queries) > 16:
            raise ValueError("ONS query count exceeds sixteen")
        if "ons" in sources and not queries:
            raise ValueError("ONS acquisition requires at least one query")
        _positive_float(self.timeout_seconds, "timeout_seconds")
        _bounded_int(
            self.max_response_bytes, "max_response_bytes", 1, MAX_CORPUS_BYTES
        )
        _bounded_int(
            self.max_total_source_bytes,
            "max_total_source_bytes",
            1,
            4 * MAX_CORPUS_BYTES,
        )
        _bounded_int(
            self.max_ons_pages_per_query, "max_ons_pages_per_query", 1, 64
        )
        _bounded_int(
            self.max_events, "max_events", 1, MAX_MARKET_CONTEXT_EVENTS
        )
        _positive_float(self.max_runtime_seconds, "max_runtime_seconds")
        object.__setattr__(self, "start_date", start.isoformat())
        object.__setattr__(self, "end_date", end.isoformat())
        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "ons_queries", queries)

    @property
    def coverage_start_ns(self) -> int:
        return _date_start_ns(_parse_date(self.start_date, "start_date"))

    @property
    def coverage_end_ns(self) -> int:
        return _date_start_ns(
            _parse_date(self.end_date, "end_date") + timedelta(days=1)
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "sources": list(self.sources),
            "ons_queries": list(self.ons_queries),
            "timeout_seconds": self.timeout_seconds,
            "max_response_bytes": self.max_response_bytes,
            "max_total_source_bytes": self.max_total_source_bytes,
            "max_ons_pages_per_query": self.max_ons_pages_per_query,
            "max_events": self.max_events,
            "max_runtime_seconds": self.max_runtime_seconds,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "MarketContextFetchProfileV1":
        return cls(
            start_date=str(data.get("start_date", "")),
            end_date=str(data.get("end_date", "")),
            sources=_string_tuple(data.get("sources")),
            ons_queries=_string_tuple(data.get("ons_queries")),
            timeout_seconds=_number(data.get("timeout_seconds")),
            max_response_bytes=_strict_int(
                data.get("max_response_bytes"), "max_response_bytes"
            ),
            max_total_source_bytes=_strict_int(
                data.get("max_total_source_bytes"), "max_total_source_bytes"
            ),
            max_ons_pages_per_query=_strict_int(
                data.get("max_ons_pages_per_query"), "max_ons_pages_per_query"
            ),
            max_events=_strict_int(data.get("max_events"), "max_events"),
            max_runtime_seconds=_number(data.get("max_runtime_seconds")),
        )


@dataclass(slots=True)
class _AcquisitionBudget:
    profile: MarketContextFetchProfileV1
    started: float
    consumed_bytes: int = 0

    def check(self) -> None:
        if (
            time.perf_counter() - self.started
            > self.profile.max_runtime_seconds
        ):
            raise ValueError(
                "market-context acquisition exceeded runtime limit"
            )
        if self.consumed_bytes > self.profile.max_total_source_bytes:
            raise ValueError(
                "market-context acquisition exceeded total source-byte limit"
            )

    @property
    def remaining_bytes(self) -> int:
        self.check()
        return self.profile.max_total_source_bytes - self.consumed_bytes

    @property
    def remaining_seconds(self) -> float:
        self.check()
        return max(
            0.001,
            self.profile.max_runtime_seconds
            - (time.perf_counter() - self.started),
        )

    def consume(self, size: int) -> None:
        self.consumed_bytes += size
        self.check()


@dataclass(frozen=True, slots=True)
class MarketContextSourceSnapshotV1:
    """One bounded source body with exact retrieval and reuse metadata."""

    source_key: str
    source_name: str
    source_uri: str
    retrieved_at_ns: int
    content: bytes
    content_type: str
    adapter_name: str
    adapter_version: str
    license_name: str
    redistribution_allowed: bool
    redistribution_constraints: tuple[str, ...]
    limitations: tuple[str, ...]
    metadata: Mapping[str, JSONValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_key", _source_key(self.source_key))
        for name in (
            "source_name",
            "source_uri",
            "content_type",
            "adapter_name",
            "adapter_version",
            "license_name",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        _bounded_int(self.retrieved_at_ns, "retrieved_at_ns", 0, 2**63 - 1)
        if not isinstance(self.content, bytes) or not self.content:
            raise ValueError("source snapshot content must be non-empty bytes")
        if len(self.content) > MAX_CORPUS_BYTES:
            raise ValueError("source snapshot exceeds corpus byte bound")
        if not isinstance(self.redistribution_allowed, bool):
            raise ValueError("redistribution_allowed must be a boolean")
        constraints = tuple(
            _required_text(item) for item in self.redistribution_constraints
        )
        limitations = tuple(_required_text(item) for item in self.limitations)
        if not limitations:
            raise ValueError("source snapshot requires limitations")
        if not self.redistribution_allowed and not constraints:
            raise ValueError("restricted source snapshot requires constraints")
        _validate_json_mapping(self.metadata, "snapshot metadata")
        object.__setattr__(self, "redistribution_constraints", constraints)
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    def source_contract(self) -> MarketContextSourceV1:
        """Return the contract embedded in every event from this snapshot."""
        return MarketContextSourceV1(
            name=self.source_name,
            source_version=f"sha256:{self.content_sha256}",
            retrieved_at_ns=self.retrieved_at_ns,
            content_sha256=self.content_sha256,
            adapter_name=self.adapter_name,
            adapter_version=self.adapter_version,
            license_name=self.license_name,
            redistribution_allowed=self.redistribution_allowed,
            redistribution_constraints=self.redistribution_constraints,
            limitations=self.limitations,
            source_uri=self.source_uri,
            metadata={**dict(self.metadata), "source_key": self.source_key},
        )


@dataclass(frozen=True, slots=True)
class MarketContextSourceEvidenceV1:
    """Publishable evidence for one retained raw source snapshot."""

    source_key: str
    source_name: str
    source_uri: str
    retrieved_at_ns: int
    content_sha256: str
    size_bytes: int
    content_type: str
    adapter_name: str
    adapter_version: str
    license_name: str
    redistribution_allowed: bool
    redistribution_constraints: tuple[str, ...]
    limitations: tuple[str, ...]
    metadata: Mapping[str, JSONValue]
    emitted_event_count: int
    diagnostics: tuple[str, ...] = ()
    schema_version: str = MARKET_CONTEXT_SOURCE_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_SOURCE_EVIDENCE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported market-context source evidence schema"
            )
        object.__setattr__(self, "source_key", _source_key(self.source_key))
        for name in (
            "source_name",
            "source_uri",
            "content_type",
            "adapter_name",
            "adapter_version",
            "license_name",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        _bounded_int(self.retrieved_at_ns, "retrieved_at_ns", 0, 2**63 - 1)
        _sha256(self.content_sha256, "content_sha256")
        _bounded_int(self.size_bytes, "size_bytes", 1, MAX_CORPUS_BYTES)
        _bounded_int(
            self.emitted_event_count,
            "emitted_event_count",
            0,
            MAX_MARKET_CONTEXT_EVENTS,
        )
        if not isinstance(self.redistribution_allowed, bool):
            raise ValueError("redistribution_allowed must be a boolean")
        constraints = tuple(
            _required_text(item) for item in self.redistribution_constraints
        )
        limitations = tuple(_required_text(item) for item in self.limitations)
        if not limitations:
            raise ValueError("source evidence requires limitations")
        if not self.redistribution_allowed and not constraints:
            raise ValueError("restricted source evidence requires constraints")
        _validate_json_mapping(self.metadata, "source evidence metadata")
        object.__setattr__(
            self,
            "redistribution_constraints",
            constraints,
        )
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(
            self,
            "diagnostics",
            tuple(
                _required_text(item)
                for item in self.diagnostics[:MAX_DIAGNOSTICS]
            ),
        )
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def from_snapshot(
        cls,
        snapshot: MarketContextSourceSnapshotV1,
        *,
        event_count: int,
        diagnostics: Sequence[str] = (),
    ) -> "MarketContextSourceEvidenceV1":
        return cls(
            source_key=snapshot.source_key,
            source_name=snapshot.source_name,
            source_uri=snapshot.source_uri,
            retrieved_at_ns=snapshot.retrieved_at_ns,
            content_sha256=snapshot.content_sha256,
            size_bytes=len(snapshot.content),
            content_type=snapshot.content_type,
            adapter_name=snapshot.adapter_name,
            adapter_version=snapshot.adapter_version,
            license_name=snapshot.license_name,
            redistribution_allowed=snapshot.redistribution_allowed,
            redistribution_constraints=snapshot.redistribution_constraints,
            limitations=snapshot.limitations,
            metadata=snapshot.metadata,
            emitted_event_count=event_count,
            diagnostics=tuple(diagnostics),
        )

    def source_contract(self) -> MarketContextSourceV1:
        """Return the event provenance contract represented by this evidence."""
        return MarketContextSourceV1(
            name=self.source_name,
            source_version=f"sha256:{self.content_sha256}",
            retrieved_at_ns=self.retrieved_at_ns,
            content_sha256=self.content_sha256,
            adapter_name=self.adapter_name,
            adapter_version=self.adapter_version,
            license_name=self.license_name,
            redistribution_allowed=self.redistribution_allowed,
            redistribution_constraints=self.redistribution_constraints,
            limitations=self.limitations,
            source_uri=self.source_uri,
            metadata={**dict(self.metadata), "source_key": self.source_key},
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_key": self.source_key,
            "source_name": self.source_name,
            "source_uri": self.source_uri,
            "retrieved_at_ns": self.retrieved_at_ns,
            "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes,
            "content_type": self.content_type,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "license_name": self.license_name,
            "redistribution_allowed": self.redistribution_allowed,
            "redistribution_constraints": list(self.redistribution_constraints),
            "limitations": list(self.limitations),
            "metadata": dict(self.metadata),
            "emitted_event_count": self.emitted_event_count,
            "diagnostics": list(self.diagnostics),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "MarketContextSourceEvidenceV1":
        return cls(
            source_key=str(data.get("source_key", "")),
            source_name=str(data.get("source_name", "")),
            source_uri=str(data.get("source_uri", "")),
            retrieved_at_ns=_strict_int(
                data.get("retrieved_at_ns"), "retrieved_at_ns"
            ),
            content_sha256=str(data.get("content_sha256", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "size_bytes"),
            content_type=str(data.get("content_type", "")),
            adapter_name=str(data.get("adapter_name", "")),
            adapter_version=str(data.get("adapter_version", "")),
            license_name=str(data.get("license_name", "")),
            redistribution_allowed=_strict_bool(
                data.get("redistribution_allowed"), "redistribution_allowed"
            ),
            redistribution_constraints=_string_tuple(
                data.get("redistribution_constraints")
            ),
            limitations=_string_tuple(data.get("limitations")),
            metadata=_mapping(data.get("metadata")),
            emitted_event_count=_strict_int(
                data.get("emitted_event_count"), "emitted_event_count"
            ),
            diagnostics=_string_tuple(data.get("diagnostics")),
            schema_version=str(data.get("schema_version", "")),
        )

    def restore_snapshot(self, content: bytes) -> MarketContextSourceSnapshotV1:
        """Restore and hash-check a raw snapshot for deterministic replay."""
        if len(content) != self.size_bytes:
            raise ValueError("source snapshot size differs from evidence")
        if hashlib.sha256(content).hexdigest() != self.content_sha256:
            raise ValueError("source snapshot hash differs from evidence")
        return MarketContextSourceSnapshotV1(
            source_key=self.source_key,
            source_name=self.source_name,
            source_uri=self.source_uri,
            retrieved_at_ns=self.retrieved_at_ns,
            content=content,
            content_type=self.content_type,
            adapter_name=self.adapter_name,
            adapter_version=self.adapter_version,
            license_name=self.license_name,
            redistribution_allowed=self.redistribution_allowed,
            redistribution_constraints=self.redistribution_constraints,
            limitations=self.limitations,
            metadata=self.metadata,
        )


@dataclass(frozen=True, slots=True)
class MarketContextCoverageSliceV1:
    """One explicit source/currency/kind support interval."""

    source_family: str
    currency: str
    kind: MarketContextKind
    coverage_start_ns: int
    coverage_end_ns: int
    complete: bool
    event_count: int
    missingness_reason: str
    schema_version: str = MARKET_CONTEXT_COVERAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_COVERAGE_SCHEMA_VERSION:
            raise ValueError("unsupported market-context coverage schema")
        object.__setattr__(
            self, "source_family", _source_key(self.source_family)
        )
        currency = _required_text(self.currency).upper()
        if not re.fullmatch(r"[A-Z]{3}", currency):
            raise ValueError("coverage currency must use ISO-4217 form")
        object.__setattr__(self, "currency", currency)
        object.__setattr__(
            self, "kind", MarketContextKind.from_value(self.kind)
        )
        start = _strict_int(self.coverage_start_ns, "coverage_start_ns")
        end = _strict_int(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise ValueError("coverage slice end must follow start")
        if not isinstance(self.complete, bool):
            raise ValueError("coverage complete must be a boolean")
        _bounded_int(
            self.event_count, "event_count", 0, MAX_MARKET_CONTEXT_EVENTS
        )
        object.__setattr__(
            self, "missingness_reason", _required_text(self.missingness_reason)
        )

    def supports(self, start_ns: int, end_ns: int) -> bool:
        return (
            self.complete
            and self.coverage_start_ns <= start_ns
            and self.coverage_end_ns >= end_ns
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_family": self.source_family,
            "currency": self.currency,
            "kind": self.kind.value,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "complete": self.complete,
            "event_count": self.event_count,
            "missingness_reason": self.missingness_reason,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "MarketContextCoverageSliceV1":
        return cls(
            source_family=str(data.get("source_family", "")),
            currency=str(data.get("currency", "")),
            kind=MarketContextKind.from_value(str(data.get("kind", ""))),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            complete=_strict_bool(data.get("complete"), "complete"),
            event_count=_strict_int(data.get("event_count"), "event_count"),
            missingness_reason=str(data.get("missingness_reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MarketContextCorpusV1:
    """Self-contained immutable corpus plus acquisition and coverage evidence."""

    profile: MarketContextFetchProfileV1
    timeline: MarketContextTimelineV1
    sources: tuple[MarketContextSourceEvidenceV1, ...]
    coverage: tuple[MarketContextCoverageSliceV1, ...]
    duplicate_event_count: int
    counts_by_year_currency_kind: Mapping[str, int]
    runtime_seconds: float
    peak_memory_bytes: int
    limitations: tuple[str, ...]
    corpus_id: str = ""
    schema_version: str = MARKET_CONTEXT_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported market-context corpus schema")
        if not isinstance(self.profile, MarketContextFetchProfileV1):
            raise ValueError("corpus requires a v1 fetch profile")
        if not isinstance(self.timeline, MarketContextTimelineV1):
            raise ValueError("corpus requires a v1 market-context timeline")
        sources = tuple(sorted(self.sources, key=lambda item: item.source_key))
        if not sources or len(sources) > MAX_SOURCE_EVIDENCE:
            raise ValueError(
                "market-context source evidence is empty or unbounded"
            )
        if len({item.source_key for item in sources}) != len(sources):
            raise ValueError("market-context source keys must be unique")
        actual_families = {
            _source_family_for_adapter(item.adapter_name) for item in sources
        }
        if actual_families != set(self.profile.sources):
            raise ValueError(
                "market-context source evidence differs from fetch profile"
            )
        if (
            self.timeline.coverage_start_ns != self.profile.coverage_start_ns
            or self.timeline.coverage_end_ns != self.profile.coverage_end_ns
        ):
            raise ValueError(
                "market-context timeline coverage differs from fetch profile"
            )
        evidence_by_key = {item.source_key: item for item in sources}
        for event in self.timeline.events:
            source_key = (event.source.metadata or {}).get("source_key")
            evidence = evidence_by_key.get(str(source_key))
            if (
                evidence is None
                or event.source.source_id
                != evidence.source_contract().source_id
            ):
                raise ValueError(
                    "market-context event provenance differs from source evidence"
                )
        coverage = tuple(
            sorted(
                self.coverage,
                key=lambda item: (
                    item.currency,
                    item.kind.value,
                    item.source_family,
                    item.coverage_start_ns,
                ),
            )
        )
        if not coverage or len(coverage) > MAX_COVERAGE_SLICES:
            raise ValueError("market-context coverage is empty or unbounded")
        _bounded_int(
            self.duplicate_event_count,
            "duplicate_event_count",
            0,
            MAX_MARKET_CONTEXT_EVENTS,
        )
        counts = {
            _required_text(key): _bounded_int(
                value, f"count {key}", 0, MAX_MARKET_CONTEXT_EVENTS
            )
            for key, value in sorted(self.counts_by_year_currency_kind.items())
        }
        expected_counts = _event_counts_by_year_currency_kind(
            self.timeline.events
        )
        if counts != expected_counts:
            raise ValueError(
                "market-context corpus counts differ from timeline events"
            )
        for item in coverage:
            if item.source_family not in actual_families:
                raise ValueError(
                    "market-context coverage has no source evidence"
                )
            if (
                item.coverage_start_ns < self.timeline.coverage_start_ns
                or item.coverage_end_ns > self.timeline.coverage_end_ns
            ):
                raise ValueError(
                    "market-context coverage exceeds timeline coverage"
                )
            event_count = sum(
                event.kind is item.kind
                and item.currency in event.affected_currencies
                and _source_family_for_adapter(event.source.adapter_name)
                == item.source_family
                for event in self.timeline.events
            )
            if item.event_count != event_count:
                raise ValueError(
                    "market-context coverage count differs from timeline events"
                )
        runtime = _finite_nonnegative(self.runtime_seconds, "runtime_seconds")
        peak = _bounded_int(
            self.peak_memory_bytes, "peak_memory_bytes", 0, 2**63 - 1
        )
        limitations = tuple(_required_text(item) for item in self.limitations)
        if not limitations:
            raise ValueError("market-context corpus requires limitations")
        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "counts_by_year_currency_kind", counts)
        object.__setattr__(self, "runtime_seconds", runtime)
        object.__setattr__(self, "peak_memory_bytes", peak)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("market-context-corpus", self.identity_payload())
        if self.corpus_id and self.corpus_id != expected:
            raise ValueError("corpus_id does not match deterministic identity")
        object.__setattr__(self, "corpus_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "profile": self.profile.to_dict(),
            "timeline": self.timeline.to_dict(),
            "sources": [item.to_dict() for item in self.sources],
            "coverage": [item.to_dict() for item in self.coverage],
            "duplicate_event_count": self.duplicate_event_count,
            "counts_by_year_currency_kind": dict(
                self.counts_by_year_currency_kind
            ),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "event_count": len(self.timeline.events),
            "source_count": len(self.sources),
            "source_bytes": sum(item.size_bytes for item in self.sources),
            "runtime_seconds": self.runtime_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "corpus_id": self.corpus_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MarketContextCorpusV1":
        if data.get("schema_version") != MARKET_CONTEXT_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported market-context corpus schema")
        corpus = cls(
            profile=MarketContextFetchProfileV1.from_dict(
                _mapping(data.get("profile"))
            ),
            timeline=MarketContextTimelineV1.from_dict(
                _mapping(data.get("timeline"))
            ),
            sources=tuple(
                MarketContextSourceEvidenceV1.from_dict(item)
                for item in _mapping_sequence(data.get("sources"))
            ),
            coverage=tuple(
                MarketContextCoverageSliceV1.from_dict(item)
                for item in _mapping_sequence(data.get("coverage"))
            ),
            duplicate_event_count=_strict_int(
                data.get("duplicate_event_count"), "duplicate_event_count"
            ),
            counts_by_year_currency_kind={
                str(key): _strict_int(value, f"count {key}")
                for key, value in _mapping(
                    data.get("counts_by_year_currency_kind")
                ).items()
            },
            runtime_seconds=_number(data.get("runtime_seconds")),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes"), "peak_memory_bytes"
            ),
            limitations=_string_tuple(data.get("limitations")),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if _strict_int(data.get("event_count"), "event_count") != len(
            corpus.timeline.events
        ):
            raise ValueError("market-context corpus event count differs")
        if _strict_int(data.get("source_count"), "source_count") != len(
            corpus.sources
        ):
            raise ValueError("market-context corpus source count differs")
        if _strict_int(data.get("source_bytes"), "source_bytes") != sum(
            item.size_bytes for item in corpus.sources
        ):
            raise ValueError("market-context corpus source bytes differ")
        return corpus


@dataclass(frozen=True, slots=True)
class MarketContextCorpusBuildV1:
    """In-memory build with raw snapshots retained for artifact writing."""

    corpus: MarketContextCorpusV1
    snapshots: tuple[MarketContextSourceSnapshotV1, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.corpus, MarketContextCorpusV1):
            raise ValueError("market-context build requires a v1 corpus")
        snapshots = tuple(
            sorted(self.snapshots, key=lambda item: item.source_key)
        )
        if not snapshots or len(snapshots) > MAX_SOURCE_EVIDENCE:
            raise ValueError("market-context build snapshots are unbounded")
        if len({item.source_key for item in snapshots}) != len(snapshots):
            raise ValueError(
                "market-context build snapshot keys must be unique"
            )
        evidence_by_key = {
            item.source_key: item for item in self.corpus.sources
        }
        if set(evidence_by_key) != {item.source_key for item in snapshots}:
            raise ValueError(
                "market-context build snapshots differ from source evidence"
            )
        for snapshot in snapshots:
            evidence = evidence_by_key[snapshot.source_key]
            expected = MarketContextSourceEvidenceV1.from_snapshot(
                snapshot,
                event_count=evidence.emitted_event_count,
                diagnostics=evidence.diagnostics,
            )
            if expected != evidence:
                raise ValueError(
                    "market-context build snapshot differs from source evidence"
                )
        object.__setattr__(self, "snapshots", snapshots)


@dataclass(frozen=True, slots=True)
class MarketContextCorpusPreflightV1:
    """Explicit support decision for a requested reconstruction interval."""

    corpus_id: str
    start_ns: int
    end_ns: int
    currencies: tuple[str, ...]
    kinds: tuple[MarketContextKind, ...]
    ready: bool
    reasons: tuple[str, ...]
    matched_coverage: tuple[str, ...]
    schema_version: str = MARKET_CONTEXT_PREFLIGHT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_PREFLIGHT_SCHEMA_VERSION:
            raise ValueError("unsupported market-context preflight schema")
        _required_text(self.corpus_id)
        if self.end_ns <= self.start_ns:
            raise ValueError("market-context preflight end must follow start")
        currencies = tuple(
            sorted({_required_text(item).upper() for item in self.currencies})
        )
        kinds = tuple(
            sorted(
                {MarketContextKind.from_value(item) for item in self.kinds},
                key=lambda item: item.value,
            )
        )
        if not isinstance(self.ready, bool):
            raise ValueError("preflight ready must be a boolean")
        reasons = tuple(_required_text(item) for item in self.reasons)
        if self.ready == bool(reasons):
            raise ValueError("preflight readiness and reasons contradict")
        object.__setattr__(self, "currencies", currencies)
        object.__setattr__(self, "kinds", kinds)
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(
            self,
            "matched_coverage",
            tuple(_required_text(item) for item in self.matched_coverage),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "currencies": list(self.currencies),
            "kinds": [item.value for item in self.kinds],
            "ready": self.ready,
            "reasons": list(self.reasons),
            "matched_coverage": list(self.matched_coverage),
        }


class MarketContextCorpusPreflightError(ValueError):
    """Raised when required market-context support is absent."""

    def __init__(self, decision: MarketContextCorpusPreflightV1) -> None:
        self.decision = decision
        super().__init__(
            "market-context corpus preflight failed: "
            + "; ".join(decision.reasons)
        )


class OnsReleaseCalendarAdapterV1:
    """Normalize one official ONS release-search response page."""

    adapter_name = "ons-release-calendar"
    adapter_version = "1.0"

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        _verify_snapshot_adapter(
            snapshot, self.adapter_name, self.adapter_version
        )
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()

    def load_events(self) -> Iterable[MarketContextEventV1]:
        try:
            payload = json.loads(self.snapshot.content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "ONS release page is not valid UTF-8 JSON"
            ) from exc
        releases = _sequence(_mapping(payload).get("releases"))
        source = self.snapshot.source_contract()
        events: list[MarketContextEventV1] = []
        diagnostics: list[str] = []
        for index, raw in enumerate(releases):
            item = _mapping(raw)
            description = _mapping(item.get("description"))
            title = str(description.get("title") or "").strip()
            if not any(
                pattern.search(title) for pattern in _ONS_TITLE_PATTERNS
            ):
                continue
            if bool(description.get("cancelled")):
                diagnostics.append(f"cancelled_release:{index}")
                continue
            release_time = str(description.get("release_date") or "").strip()
            uri = str(item.get("uri") or "").strip()
            if not title or not release_time or not uri:
                diagnostics.append(f"missing_release_field:{index}")
                continue
            try:
                event_time = normalize_market_context_datetime(
                    release_time, "UTC"
                )
            except ValueError:
                diagnostics.append(f"invalid_release_time:{index}")
                continue
            canonical = "ons." + re.sub(
                r"[^a-z0-9._:-]+", "-", uri.strip("/").lower()
            )
            changes = _sequence_or_empty(item.get("date_changes"))
            tags = ["ons", "scheduled", "macro_release"]
            if changes:
                tags.append("schedule_changed_without_change_timestamp")
            event_content = canonical_contract_json(
                cast(
                    Mapping[str, JSONValue],
                    _json_value_mapping(
                        {
                            "uri": uri,
                            "description": {
                                "title": title,
                                "release_date": release_time,
                                "cancelled": False,
                            },
                            "date_changes": changes,
                        }
                    ),
                )
            ).encode("utf-8")
            events.append(
                MarketContextEventV1(
                    canonical_key=canonical,
                    kind=MarketContextKind.MACRO_RELEASE,
                    title=title,
                    source=source,
                    source_event_time=release_time,
                    source_timezone="UTC",
                    event_time_ns=event_time,
                    first_known_at_ns=event_time,
                    available_at_ns=event_time,
                    pre_event_ns=30 * 60 * 1_000_000_000,
                    post_event_ns=2 * HOUR_NS,
                    affected_currencies=("GBP",),
                    affected_symbols=("EURGBP", "GBPUSD"),
                    confidence=1.0,
                    precision=MarketContextPrecision.EXACT,
                    limitations=(
                        "The current ONS release-search endpoint does not retain when a schedule was first published.",
                        "Date changes lack change-known timestamps and are not exposed as ex-ante revisions.",
                        "This calendar record contains no consensus expectation or realized statistic value.",
                    ),
                    vintage_id=f"ons-release:{uri}:{release_time}",
                    content_sha256=hashlib.sha256(event_content).hexdigest(),
                    tags=tuple(tags),
                )
            )
        self.diagnostics = tuple(diagnostics[:MAX_DIAGNOSTICS])
        return tuple(events)


class EcbPolicyRateAdapterV1:
    """Collapse the official daily MRO level series to effective changes."""

    adapter_name = "ecb-policy-rate"
    adapter_version = "1.0"

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        _verify_snapshot_adapter(
            snapshot, self.adapter_name, self.adapter_version
        )
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()
        self.coverage_start_ns: int | None = None
        self.coverage_end_ns: int | None = None
        self.coverage_complete = False

    def load_events(self) -> Iterable[MarketContextEventV1]:
        try:
            text = self.snapshot.content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("ECB policy-rate CSV is not UTF-8") from exc
        source = self.snapshot.source_contract()
        events: list[MarketContextEventV1] = []
        diagnostics: list[str] = []
        observations: list[tuple[date, float, Mapping[str | None, Any]]] = []
        for index, row in enumerate(csv.DictReader(io.StringIO(text))):
            series_key = str(row.get("KEY") or "").strip()
            if series_key and series_key != ECB_POLICY_RATE_SERIES_KEY:
                raise ValueError(
                    "ECB policy-rate response contains an unexpected series"
                )
            period = str(row.get("TIME_PERIOD") or "").strip()
            raw_value = str(row.get("OBS_VALUE") or "").strip()
            if not period or not raw_value:
                diagnostics.append(f"missing_rate_field:{index}")
                continue
            try:
                value = float(raw_value)
                observed_date = _parse_date(period, "ECB TIME_PERIOD")
            except ValueError:
                diagnostics.append(f"invalid_rate_row:{index}")
                continue
            observations.append((observed_date, value, dict(row)))

        observations.sort(key=lambda item: item[0])
        if not observations:
            self.diagnostics = tuple(diagnostics[:MAX_DIAGNOSTICS])
            return ()
        for previous, current in zip(observations, observations[1:]):
            if current[0] == previous[0]:
                if current[1] != previous[1]:
                    raise ValueError(
                        "ECB policy-rate response has conflicting daily values"
                    )
                diagnostics.append(
                    f"duplicate_rate_date:{current[0].isoformat()}"
                )
                continue
            if current[0] != previous[0] + timedelta(days=1):
                diagnostics.append(
                    "non_contiguous_rate_date:"
                    f"{previous[0].isoformat()}:{current[0].isoformat()}"
                )
        self.coverage_start_ns = _date_start_ns(observations[0][0])
        self.coverage_end_ns = _date_start_ns(
            observations[-1][0] + timedelta(days=1)
        )
        self.coverage_complete = not diagnostics

        previous_value: float | None = None
        emitted_dates: set[date] = set()
        for observed_date, value, observation_row in observations:
            if observed_date in emitted_dates:
                continue
            emitted_dates.add(observed_date)
            if previous_value is not None and value == previous_value:
                continue
            period = observed_date.isoformat()
            event_time = _date_start_ns(observed_date)
            conservative_available = event_time + DAY_NS
            title = str(
                observation_row.get("TITLE")
                or "ECB main refinancing operations rate"
            ).strip()
            initial_level = previous_value is None
            events.append(
                MarketContextEventV1(
                    canonical_key=f"ecb.fm.mrr.{period}",
                    kind=MarketContextKind.POLICY_RATE_CHANGE,
                    title=title,
                    source=source,
                    source_event_time=f"{period}T00:00:00+00:00",
                    source_timezone="UTC",
                    event_time_ns=event_time,
                    first_known_at_ns=conservative_available,
                    available_at_ns=conservative_available,
                    pre_event_ns=DAY_NS,
                    post_event_ns=DAY_NS,
                    affected_currencies=("EUR",),
                    affected_symbols=("EURGBP", "EURUSD"),
                    confidence=0.9,
                    precision=MarketContextPrecision.WINDOW_ONLY,
                    ambiguity_reason=(
                        "The SDMX observation is the rate-change effective date, not the policy announcement timestamp."
                    ),
                    limitations=(
                        "Effective-date observations do not prove the announcement time or original schedule vintage.",
                        "Ex-ante availability is delayed until the next UTC day instead of inferring an intraday publication time.",
                        "No historical consensus expectation is supplied.",
                    ),
                    vintage_id=f"ecb-fm-mrr:{period}:{value:g}",
                    actual_value=value,
                    previous_value=previous_value,
                    value_unit="percent",
                    content_sha256=_row_sha256(observation_row),
                    tags=(
                        "ecb",
                        "effective_date",
                        "main_refinancing_rate",
                        (
                            "series_initial_level"
                            if initial_level
                            else "effective_rate_change"
                        ),
                        "daily_level_collapsed_to_change",
                    ),
                )
            )
            previous_value = value
        self.diagnostics = tuple(diagnostics[:MAX_DIAGNOSTICS])
        return tuple(events)


class BankOfEnglandBankRateAdapterV1:
    """Normalize the official Bank of England Bank Rate history table."""

    adapter_name = "boe-bank-rate"
    adapter_version = "1.0"

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        _verify_snapshot_adapter(
            snapshot, self.adapter_name, self.adapter_version
        )
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()
        self.coverage_start_ns: int | None = None
        self.coverage_end_ns: int | None = None
        self.coverage_complete = False

    def load_events(self) -> Iterable[MarketContextEventV1]:
        try:
            text = self.snapshot.content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("Bank Rate history is not UTF-8 HTML") from exc
        parser = _BankRateTableParser()
        parser.feed(text)
        source = self.snapshot.source_contract()
        events: list[MarketContextEventV1] = []
        diagnostics: list[str] = []
        for index, row in enumerate(parser.rows):
            if len(row) < 2:
                diagnostics.append(f"missing_bank_rate_field:{index}")
                continue
            try:
                changed = datetime.strptime(row[0], "%d %b %y").date()
                value = float(row[1])
            except ValueError:
                diagnostics.append(f"invalid_bank_rate_row:{index}")
                continue
            source_time = f"{changed.isoformat()}T00:00:00+00:00"
            event_time = normalize_market_context_datetime(source_time, "UTC")
            conservative_available = event_time + DAY_NS
            events.append(
                MarketContextEventV1(
                    canonical_key=f"boe.bank-rate.{changed.isoformat()}",
                    kind=MarketContextKind.POLICY_RATE_CHANGE,
                    title="Bank of England official Bank Rate change",
                    source=source,
                    source_event_time=source_time,
                    source_timezone="UTC",
                    event_time_ns=event_time,
                    first_known_at_ns=conservative_available,
                    available_at_ns=conservative_available,
                    pre_event_ns=DAY_NS,
                    post_event_ns=DAY_NS,
                    affected_currencies=("GBP",),
                    affected_symbols=("EURGBP", "GBPUSD"),
                    confidence=0.9,
                    precision=MarketContextPrecision.WINDOW_ONLY,
                    ambiguity_reason=(
                        "The official table provides a change date but not the announcement timestamp."
                    ),
                    limitations=(
                        "The date-only history does not retain the original decision schedule or announcement time.",
                        "Ex-ante availability is delayed until the next UTC day instead of inferring an intraday publication time.",
                        "No historical consensus expectation is supplied.",
                    ),
                    vintage_id=f"boe-bank-rate:{changed.isoformat()}:{value:g}",
                    actual_value=value,
                    value_unit="percent",
                    content_sha256=hashlib.sha256(
                        f"{row[0]}|{row[1]}".encode("utf-8")
                    ).hexdigest(),
                    tags=("bank_of_england", "bank_rate", "effective_date"),
                )
            )
        self.diagnostics = tuple(diagnostics[:MAX_DIAGNOSTICS])
        if events:
            self.coverage_start_ns = min(item.event_time_ns for item in events)
            self.coverage_end_ns = (
                self.snapshot.retrieved_at_ns // DAY_NS + 1
            ) * DAY_NS
            self.coverage_complete = not diagnostics
        return tuple(events)


class FederalReserveFomcCalendarAdapterV1:
    """Normalize official FOMC meeting-end policy-decision timestamps."""

    adapter_name = "federal-reserve-fomc-calendar"
    adapter_version = "1.0"

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        _verify_snapshot_adapter(
            snapshot, self.adapter_name, self.adapter_version
        )
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()

    def load_events(self) -> Iterable[MarketContextEventV1]:
        try:
            text = self.snapshot.content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("FOMC calendar is not UTF-8 HTML") from exc
        source = self.snapshot.source_contract()
        events: list[MarketContextEventV1] = []
        diagnostics: list[str] = []
        for year_text, section in re.findall(
            r"(20\d{2}) FOMC Meetings(.*?)(?=(?:20\d{2}) FOMC Meetings|$)",
            text,
            re.DOTALL,
        ):
            year = int(year_text)
            pairs = re.findall(
                r"fomc-meeting__month[^>]*>\s*<strong>([^<]+)</strong>"
                r".*?fomc-meeting__date[^>]*>([^<]+)</div>",
                section,
                re.DOTALL,
            )
            for month_text, date_text in pairs:
                notation_vote = "notation" in date_text.lower()
                try:
                    month = _fomc_end_month(month_text)
                    day = int(re.findall(r"\d+", date_text)[-1])
                    event_date = date(year, month, day)
                except (IndexError, ValueError):
                    diagnostics.append(
                        f"invalid_fomc_date:{year_text}:{month_text}:{date_text}"
                    )
                    continue
                if notation_vote:
                    source_time = f"{event_date.isoformat()}T00:00:00"
                    precision = MarketContextPrecision.WINDOW_ONLY
                    ambiguity = "The calendar identifies a notation-vote date but not its release time."
                    pre_ns = 0
                    post_ns = DAY_NS
                else:
                    source_time = f"{event_date.isoformat()}T14:00:00"
                    precision = MarketContextPrecision.EXACT
                    ambiguity = None
                    pre_ns = 30 * 60 * 1_000_000_000
                    post_ns = 2 * HOUR_NS
                event_time = normalize_market_context_datetime(
                    source_time, "America/New_York"
                )
                available = min(event_time, self.snapshot.retrieved_at_ns)
                events.append(
                    MarketContextEventV1(
                        canonical_key=f"federal-reserve.fomc.{event_date.isoformat()}",
                        kind=MarketContextKind.CENTRAL_BANK_DECISION,
                        title=(
                            "Federal Reserve FOMC notation vote"
                            if notation_vote
                            else "Federal Reserve FOMC policy decision"
                        ),
                        source=source,
                        source_event_time=source_time,
                        source_timezone="America/New_York",
                        event_time_ns=event_time,
                        first_known_at_ns=available,
                        available_at_ns=available,
                        pre_event_ns=pre_ns,
                        post_event_ns=post_ns,
                        affected_currencies=("USD",),
                        affected_symbols=("EURUSD", "GBPUSD"),
                        confidence=0.95,
                        precision=precision,
                        ambiguity_reason=ambiguity,
                        limitations=(
                            "The live calendar does not prove when each schedule was first published.",
                            "Regular decisions use the Federal Reserve's documented 2 p.m. Eastern announcement time for this supported 2021+ calendar.",
                            "Unexpected timing exceptions require a later authoritative vintage rather than inference.",
                        ),
                        vintage_id=f"fomc-calendar:{event_date.isoformat()}:{date_text.strip()}",
                        content_sha256=hashlib.sha256(
                            f"{year_text}|{month_text}|{date_text}".encode(
                                "utf-8"
                            )
                        ).hexdigest(),
                        tags=(
                            "federal_reserve",
                            "fomc",
                            "notation_vote" if notation_vote else "scheduled",
                        ),
                    )
                )
        self.diagnostics = tuple(diagnostics[:MAX_DIAGNOSTICS])
        return tuple(events)


class FederalReserveFomcHistoricalAdapterV1:
    """Normalize official 2000-2020 FOMC historical meeting pages."""

    adapter_name = "federal-reserve-fomc-historical"
    adapter_version = "1.0"

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        _verify_snapshot_adapter(
            snapshot, self.adapter_name, self.adapter_version
        )
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()

    def load_events(self) -> Iterable[MarketContextEventV1]:
        try:
            text = self.snapshot.content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("historical FOMC page is not UTF-8 HTML") from exc
        source = self.snapshot.source_contract()
        events: list[MarketContextEventV1] = []
        diagnostics: list[str] = []
        headings = re.findall(r"<h5[^>]*>(.*?)</h5>", text, re.DOTALL)
        for index, raw_heading in enumerate(headings):
            heading = " ".join(re.sub(r"<[^>]+>", " ", raw_heading).split())
            if not (
                " Meeting" in heading or "notation vote" in heading.lower()
            ):
                continue
            match = re.search(
                r"([A-Za-z]+(?:/[A-Za-z]+)?)\s+"
                r"([0-9]+(?:[-/][0-9]+)?)"
                r"(?:\s+\([^)]*\))?\s+"
                r"(?:Meeting|\(notation vote\)|-).*?(20[0-2][0-9])",
                heading,
                re.IGNORECASE,
            )
            if match is None:
                diagnostics.append(f"unparsed_historical_heading:{index}")
                continue
            try:
                month = _fomc_end_month(match.group(1))
                day = int(re.findall(r"\d+", match.group(2))[-1])
                event_date = date(int(match.group(3)), month, day)
            except (IndexError, ValueError):
                diagnostics.append(f"invalid_historical_heading:{index}")
                continue
            if "cancelled" in heading.lower():
                diagnostics.append(
                    f"cancelled_historical_meeting:{event_date.isoformat()}"
                )
                continue
            notation_vote = "notation vote" in heading.lower()
            unscheduled = "unscheduled" in heading.lower()
            source_time = f"{event_date.isoformat()}T00:00:00"
            event_time = normalize_market_context_datetime(
                source_time, "America/New_York"
            )
            available = normalize_market_context_datetime(
                f"{event_date.isoformat()}T23:59:59", "America/New_York"
            )
            tags = ["federal_reserve", "fomc", "historical_page"]
            tags.append("unscheduled" if unscheduled else "scheduled")
            if notation_vote:
                tags.append("notation_vote")
            events.append(
                MarketContextEventV1(
                    canonical_key=(
                        f"federal-reserve.fomc.{event_date.isoformat()}"
                    ),
                    kind=MarketContextKind.CENTRAL_BANK_DECISION,
                    title=(
                        "Federal Reserve FOMC notation vote"
                        if notation_vote
                        else "Federal Reserve FOMC historical meeting"
                    ),
                    source=source,
                    source_event_time=source_time,
                    source_timezone="America/New_York",
                    event_time_ns=event_time,
                    first_known_at_ns=event_time,
                    available_at_ns=available,
                    pre_event_ns=0,
                    post_event_ns=DAY_NS,
                    affected_currencies=("USD",),
                    affected_symbols=("EURUSD", "GBPUSD"),
                    confidence=0.9,
                    precision=MarketContextPrecision.WINDOW_ONLY,
                    ambiguity_reason=(
                        "The historical page proves the meeting date but this adapter does not infer a release timestamp."
                    ),
                    limitations=(
                        "The date-only event becomes ex-ante eligible only after the local calendar day, preventing intraday look-ahead.",
                        "The current historical page does not prove the original schedule-publication time.",
                        "Conference calls without a retained meeting or notation-vote heading are excluded.",
                    ),
                    vintage_id=(
                        f"fomc-historical:{event_date.isoformat()}:{heading}"
                    ),
                    content_sha256=hashlib.sha256(
                        heading.encode("utf-8")
                    ).hexdigest(),
                    tags=tuple(tags),
                )
            )
        self.diagnostics = tuple(diagnostics[:MAX_DIAGNOSTICS])
        return tuple(events)


class OperatorMarketContextCatalogAdapterV1:
    """Normalize an operator-maintained shock/revision catalog."""

    adapter_name = "operator-market-context-catalog"
    adapter_version = "1.0"

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        _verify_snapshot_adapter(
            snapshot, self.adapter_name, self.adapter_version
        )
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()

    def load_events(self) -> Iterable[MarketContextEventV1]:
        try:
            payload = _mapping(
                json.loads(self.snapshot.content.decode("utf-8"))
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "operator catalog is not valid UTF-8 JSON"
            ) from exc
        if (
            payload.get("schema_version")
            != OPERATOR_MARKET_CONTEXT_CATALOG_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported operator market-context catalog schema"
            )
        raw_events = sorted(
            _mapping_sequence(payload.get("events")),
            key=lambda item: (
                str(item.get("canonical_key", "")),
                _strict_int(
                    item.get("revision_sequence", 0), "revision_sequence"
                ),
            ),
        )
        source = self.snapshot.source_contract()
        predecessors: dict[str, MarketContextEventV1] = {}
        events: list[MarketContextEventV1] = []
        for item in raw_events:
            canonical = str(item.get("canonical_key", ""))
            revision = _strict_int(
                item.get("revision_sequence", 0), "revision_sequence"
            )
            predecessor = predecessors.get(canonical)
            if revision != (
                0 if predecessor is None else predecessor.revision_sequence + 1
            ):
                raise ValueError(
                    "operator catalog revision sequence is not contiguous"
                )
            timezone_name = str(item.get("source_timezone", ""))
            source_time = str(item.get("source_event_time", ""))
            source_time_fold = _optional_int(item.get("source_time_fold"))
            event_time = _normalize_catalog_datetime(
                source_time, timezone_name, fold=source_time_fold
            )
            first_known = _normalize_catalog_datetime(
                str(item.get("first_known_at", "")),
                timezone_name,
                fold=source_time_fold,
            )
            available = _normalize_catalog_datetime(
                str(item.get("available_at", "")),
                timezone_name,
                fold=source_time_fold,
            )
            event = MarketContextEventV1(
                canonical_key=canonical,
                kind=MarketContextKind.from_value(str(item.get("kind", ""))),
                title=str(item.get("title", "")),
                source=source,
                source_event_time=source_time,
                source_timezone=timezone_name,
                source_time_fold=source_time_fold,
                event_time_ns=event_time,
                first_known_at_ns=first_known,
                available_at_ns=available,
                pre_event_ns=_strict_int(
                    item.get("pre_event_ns"), "pre_event_ns"
                ),
                post_event_ns=_strict_int(
                    item.get("post_event_ns"), "post_event_ns"
                ),
                affected_currencies=_string_tuple(
                    item.get("affected_currencies")
                ),
                affected_symbols=_string_tuple(item.get("affected_symbols")),
                confidence=_number(item.get("confidence")),
                precision=MarketContextPrecision.from_value(
                    str(item.get("precision", ""))
                ),
                ambiguity_reason=_optional_text(item.get("ambiguity_reason")),
                limitations=_string_tuple(item.get("limitations")),
                vintage_id=str(item.get("vintage_id", "")),
                revision_sequence=revision,
                supersedes_event_id=(
                    predecessor.event_id if predecessor else None
                ),
                expected_value=_optional_number(item.get("expected_value")),
                actual_value=_optional_number(item.get("actual_value")),
                previous_value=_optional_number(item.get("previous_value")),
                revised_previous_value=_optional_number(
                    item.get("revised_previous_value")
                ),
                value_unit=_optional_text(item.get("value_unit")),
                content_sha256=hashlib.sha256(
                    canonical_contract_json(
                        cast(Mapping[str, JSONValue], _json_value_mapping(item))
                    ).encode("utf-8")
                ).hexdigest(),
                tags=_string_tuple(item.get("tags")),
            )
            predecessors[canonical] = event
            events.append(event)
        return tuple(events)


def packaged_operator_catalog_path() -> Path:
    """Return the installed operator-maintained shock catalog path."""
    return Path(__file__).with_name("assets") / "operator_shocks_v1.json"


def build_live_market_context_corpus(
    profile: MarketContextFetchProfileV1,
    *,
    operator_catalog_path: str | Path | None = None,
) -> MarketContextCorpusBuildV1:
    """Acquire approved official sources and build one replayable corpus."""
    started = time.perf_counter()
    budget = _AcquisitionBudget(profile=profile, started=started)
    snapshots: list[MarketContextSourceSnapshotV1] = []
    if "ons" in profile.sources:
        snapshots.extend(_fetch_ons_snapshots(profile, budget))
    if "ecb" in profile.sources:
        snapshots.append(_fetch_ecb_snapshot(profile, budget))
    if "boe" in profile.sources:
        snapshots.append(_fetch_boe_snapshot(profile, budget))
    if "fed" in profile.sources:
        snapshots.extend(_fetch_fed_snapshots(profile, budget))
    if "operator" in profile.sources:
        operator_snapshot = _read_operator_snapshot(
            operator_catalog_path or packaged_operator_catalog_path()
        )
        budget.consume(len(operator_snapshot.content))
        snapshots.append(operator_snapshot)
    budget.check()
    return build_market_context_corpus_from_snapshots(
        snapshots,
        profile=profile,
        runtime_started=started,
    )


def build_market_context_corpus_from_snapshots(
    snapshots: Sequence[MarketContextSourceSnapshotV1],
    *,
    profile: MarketContextFetchProfileV1,
    runtime_started: float | None = None,
) -> MarketContextCorpusBuildV1:
    """Build the corpus deterministically from already retained snapshots."""
    started = (
        time.perf_counter() if runtime_started is None else runtime_started
    )
    values = tuple(sorted(snapshots, key=lambda item: item.source_key))
    if not values or len(values) > MAX_SOURCE_EVIDENCE:
        raise ValueError("market-context snapshot count is empty or unbounded")
    if len({item.source_key for item in values}) != len(values):
        raise ValueError("market-context snapshot keys must be unique")
    actual_families = {
        _source_family_for_adapter(item.adapter_name) for item in values
    }
    if actual_families != set(profile.sources):
        raise ValueError(
            "market-context snapshot families differ from fetch profile: "
            f"expected {sorted(profile.sources)}, got {sorted(actual_families)}"
        )
    if (
        sum(len(item.content) for item in values)
        > profile.max_total_source_bytes
    ):
        raise ValueError(
            "market-context snapshots exceed total source-byte limit"
        )
    all_events: list[MarketContextEventV1] = []
    evidence: list[MarketContextSourceEvidenceV1] = []
    source_events: dict[str, tuple[MarketContextEventV1, ...]] = {}
    source_support: dict[str, tuple[int, int, bool]] = {}
    source_diagnostics: dict[str, tuple[str, ...]] = {}
    for snapshot in values:
        adapter = _adapter_for_snapshot(snapshot)
        events = tuple(adapter.load_events())
        diagnostics = tuple(getattr(adapter, "diagnostics", ()))
        source_diagnostics[snapshot.source_key] = diagnostics
        support_start = getattr(adapter, "coverage_start_ns", None)
        support_end = getattr(adapter, "coverage_end_ns", None)
        support_complete = bool(getattr(adapter, "coverage_complete", False))
        if support_start is not None and support_end is not None:
            source_support[snapshot.source_key] = (
                support_start,
                support_end,
                support_complete,
            )
        source_events[snapshot.source_key] = events
        all_events.extend(events)
        evidence.append(
            MarketContextSourceEvidenceV1.from_snapshot(
                snapshot,
                event_count=len(events),
                diagnostics=diagnostics,
            )
        )
    deduplicated, duplicate_count = _deduplicate_events(all_events)
    retained = tuple(
        event
        for event in deduplicated
        if profile.coverage_start_ns
        <= event.event_time_ns
        < profile.coverage_end_ns
    )
    if not retained:
        raise ValueError("market-context corpus contains no in-range events")
    if len(retained) > profile.max_events:
        raise ValueError("market-context corpus exceeds configured event limit")
    timeline = MarketContextTimelineV1(
        timeline_version="official-source-corpus-v1",
        coverage_start_ns=profile.coverage_start_ns,
        coverage_end_ns=profile.coverage_end_ns,
        complete=True,
        events=retained,
        limitations=(
            "Completeness means every configured source response was acquired and parsed; it does not assert that every real-world event is represented.",
            "Absence of a matching event means no matching retained record, never proof that no event occurred.",
            "Current-source archives may omit historical schedule vintages and later corrections; source-level limitations remain authoritative.",
            "CFTC Commitments of Traders is persistent positioning state and is intentionally outside this event corpus (issue #468).",
        ),
    )
    coverage = _coverage_slices(
        profile,
        values,
        source_events,
        source_support,
        source_diagnostics,
        retained,
    )
    counts = _event_counts_by_year_currency_kind(retained)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if not sys.platform.startswith("darwin"):
        peak *= 1024
    elapsed = time.perf_counter() - started
    if elapsed > profile.max_runtime_seconds:
        raise ValueError("market-context corpus build exceeded runtime limit")
    corpus = MarketContextCorpusV1(
        profile=profile,
        timeline=timeline,
        sources=tuple(evidence),
        coverage=coverage,
        duplicate_event_count=duplicate_count,
        counts_by_year_currency_kind=counts,
        runtime_seconds=round(elapsed, 6),
        peak_memory_bytes=int(peak),
        limitations=timeline.limitations,
    )
    return MarketContextCorpusBuildV1(corpus=corpus, snapshots=values)


def write_market_context_corpus(
    build: MarketContextCorpusBuildV1,
    directory: str | Path,
) -> Mapping[str, ArtifactRef]:
    """Write content-addressed raw, timeline, and corpus artifacts once."""
    root = Path(directory).expanduser().resolve()
    raw_root = root / "sources"
    raw_root.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, ArtifactRef] = {}
    for snapshot in build.snapshots:
        suffix = _content_suffix(snapshot.content_type)
        path = (
            raw_root
            / f"{snapshot.source_key}-{snapshot.content_sha256}{suffix}"
        )
        _write_once(path, snapshot.content)
        artifacts[f"source:{snapshot.source_key}"] = ArtifactRef(
            kind="market_context_source_snapshot_v1",
            path=str(path),
            size_bytes=len(snapshot.content),
            sha256=snapshot.content_sha256,
            metadata={
                "adapter_name": snapshot.adapter_name,
                "retrieved_at_ns": snapshot.retrieved_at_ns,
                "source_uri": snapshot.source_uri,
            },
        )
    timeline_bytes = build.corpus.timeline.to_json().encode("utf-8") + b"\n"
    timeline_hash = hashlib.sha256(timeline_bytes).hexdigest()
    timeline_path = root / f"market-context-timeline-{timeline_hash}.json"
    _write_once(timeline_path, timeline_bytes)
    artifacts["timeline"] = ArtifactRef(
        kind="market_context_timeline_v1",
        path=str(timeline_path),
        size_bytes=len(timeline_bytes),
        sha256=timeline_hash,
        metadata={"timeline_id": build.corpus.timeline.timeline_id},
    )
    corpus_bytes = (
        canonical_contract_json(build.corpus.to_dict()).encode("utf-8") + b"\n"
    )
    corpus_hash = hashlib.sha256(corpus_bytes).hexdigest()
    corpus_path = root / f"market-context-corpus-{corpus_hash}.json"
    _write_once(corpus_path, corpus_bytes)
    artifacts["corpus"] = ArtifactRef(
        kind="market_context_corpus_v1",
        path=str(corpus_path),
        size_bytes=len(corpus_bytes),
        sha256=corpus_hash,
        metadata={
            "corpus_id": build.corpus.corpus_id,
            "timeline_id": build.corpus.timeline.timeline_id,
            "event_count": len(build.corpus.timeline.events),
        },
    )
    return artifacts


def read_market_context_corpus(path: str | Path) -> MarketContextCorpusV1:
    """Load and strictly verify one self-contained corpus artifact."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > MAX_CORPUS_BYTES:
        raise ValueError("market-context corpus artifact exceeds size bound")
    match = re.fullmatch(
        r"market-context-corpus-([0-9a-f]{64})\.json", source.name
    )
    if match is None:
        raise ValueError(
            "market-context corpus artifact name is not content addressed"
        )
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError(
            "market-context corpus artifact hash differs from name"
        )
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "market-context corpus artifact is invalid JSON"
        ) from exc
    return MarketContextCorpusV1.from_dict(_mapping(payload))


def replay_market_context_corpus(
    corpus_path: str | Path,
    *,
    source_directory: str | Path | None = None,
) -> MarketContextCorpusBuildV1:
    """Rebuild a corpus exactly from its retained content-addressed sources."""
    corpus = read_market_context_corpus(corpus_path)
    root = (
        Path(source_directory).expanduser().resolve()
        if source_directory is not None
        else Path(corpus_path).expanduser().resolve().parent / "sources"
    )
    snapshots: list[MarketContextSourceSnapshotV1] = []
    for item in corpus.sources:
        matches = sorted(
            root.glob(f"{item.source_key}-{item.content_sha256}.*")
        )
        if len(matches) != 1:
            raise ValueError(
                f"expected one retained snapshot for source {item.source_key}"
            )
        content = matches[0].read_bytes()
        snapshots.append(item.restore_snapshot(content))
    rebuilt = build_market_context_corpus_from_snapshots(
        snapshots,
        profile=corpus.profile,
    )
    if rebuilt.corpus.corpus_id != corpus.corpus_id:
        raise ValueError("replayed market-context corpus identity differs")
    return rebuilt


def preflight_market_context_corpus(
    corpus: MarketContextCorpusV1,
    *,
    start_ns: int,
    end_ns: int,
    currencies: Sequence[str],
    kinds: Sequence[MarketContextKind],
) -> MarketContextCorpusPreflightV1:
    """Return support evidence without treating an ordinary empty window as failure."""
    if end_ns <= start_ns:
        raise ValueError("market-context preflight end must follow start")
    selected_currencies = tuple(
        sorted({_required_text(item).upper() for item in currencies})
    )
    selected_kinds = tuple(
        sorted(
            {MarketContextKind.from_value(item) for item in kinds},
            key=lambda item: item.value,
        )
    )
    reasons: list[str] = []
    matched: list[str] = []
    if (
        start_ns < corpus.timeline.coverage_start_ns
        or end_ns > corpus.timeline.coverage_end_ns
    ):
        reasons.append(
            "requested interval lies outside corpus timeline coverage"
        )
    if not corpus.timeline.complete:
        reasons.append("corpus timeline is incomplete")
    for currency in selected_currencies:
        for kind in selected_kinds:
            candidates = [
                item
                for item in corpus.coverage
                if item.currency == currency and item.kind is kind
            ]
            supporting = [
                item for item in candidates if item.supports(start_ns, end_ns)
            ]
            if supporting:
                matched.extend(
                    f"{item.source_family}:{currency}:{kind.value}"
                    for item in supporting
                )
            else:
                reasons.append(
                    f"unsupported context coverage for {currency}/{kind.value}"
                )
    return MarketContextCorpusPreflightV1(
        corpus_id=corpus.corpus_id,
        start_ns=start_ns,
        end_ns=end_ns,
        currencies=selected_currencies,
        kinds=selected_kinds,
        ready=not reasons,
        reasons=tuple(dict.fromkeys(reasons)),
        matched_coverage=tuple(sorted(set(matched))),
    )


def require_market_context_corpus(
    corpus: MarketContextCorpusV1,
    *,
    start_ns: int,
    end_ns: int,
    currencies: Sequence[str],
    kinds: Sequence[MarketContextKind],
) -> MarketContextCorpusPreflightV1:
    """Fail closed when a reconstruction requires unsupported context."""
    decision = preflight_market_context_corpus(
        corpus,
        start_ns=start_ns,
        end_ns=end_ns,
        currencies=currencies,
        kinds=kinds,
    )
    if not decision.ready:
        raise MarketContextCorpusPreflightError(decision)
    return decision


def query_market_context_corpus(
    corpus: MarketContextCorpusV1,
    *,
    start_ns: int,
    end_ns: int,
    view: MarketContextView,
    as_of_ns: int | None = None,
    currencies: Sequence[str] = (),
    symbols: Sequence[str] = (),
    kinds: Sequence[MarketContextKind] = (),
    include_calendar: bool = True,
    max_events: int = 512,
    window_id: str | None = None,
    require_supported: bool = True,
) -> MarketContextQueryV1:
    """Query a corpus and preflight every declared currency/kind requirement."""
    required_currencies = {_required_text(item).upper() for item in currencies}
    for symbol in symbols:
        normalized = _required_text(symbol).upper()
        if re.fullmatch(r"[A-Z]{6}", normalized):
            required_currencies.update((normalized[:3], normalized[3:]))
    if require_supported and kinds and required_currencies:
        require_market_context_corpus(
            corpus,
            start_ns=start_ns,
            end_ns=end_ns,
            currencies=tuple(sorted(required_currencies)),
            kinds=kinds,
        )
    return query_market_context(
        corpus.timeline,
        start_ns=start_ns,
        end_ns=end_ns,
        view=view,
        as_of_ns=as_of_ns,
        currencies=currencies,
        symbols=symbols,
        kinds=kinds,
        include_calendar=include_calendar,
        max_events=max_events,
        window_id=window_id,
    )


def market_context_benchmark_event_state(query: MarketContextQueryV1) -> str:
    """Project a bounded context query onto the benchmark event-state seam."""
    if query.events:
        kinds = "+".join(sorted({item.kind.value for item in query.events}))
        tags = sorted({tag for item in query.events for tag in item.tags})[:4]
        suffix = ":" + "+".join(tags) if tags else ""
        return f"market_context:{kinds}{suffix}"
    reason = (
        query.missing_reason.value
        if query.missing_reason is not None
        else "unknown"
    )
    return f"market_context:none:{reason}"


def _fetch_ons_snapshots(
    profile: MarketContextFetchProfileV1,
    budget: _AcquisitionBudget,
) -> list[MarketContextSourceSnapshotV1]:
    snapshots: list[MarketContextSourceSnapshotV1] = []
    for query_number, query in enumerate(profile.ons_queries):
        offset = 0
        for page in range(profile.max_ons_pages_per_query):
            params = {
                "limit": "1000",
                "offset": str(offset),
                "sort": "release_date_asc",
                "fromDate": profile.start_date,
                "toDate": profile.end_date,
                "release-type": "type-published",
                "query": query,
            }
            uri = ONS_RELEASES_URI + "?" + urlencode(params)
            content, content_type, retrieved = _fetch_bytes(
                uri, profile, budget=budget
            )
            key = f"ons.q{query_number:02d}.p{page:02d}"
            snapshots.append(
                MarketContextSourceSnapshotV1(
                    source_key=key,
                    source_name="Office for National Statistics release calendar",
                    source_uri=uri,
                    retrieved_at_ns=retrieved,
                    content=content,
                    content_type=content_type,
                    adapter_name=OnsReleaseCalendarAdapterV1.adapter_name,
                    adapter_version=OnsReleaseCalendarAdapterV1.adapter_version,
                    license_name="Open Government Licence v3.0",
                    redistribution_allowed=True,
                    redistribution_constraints=(
                        "Attribute the Office for National Statistics and the OGL v3.0.",
                    ),
                    limitations=(
                        "The search endpoint exposes the current release record, not a complete schedule-vintage archive.",
                        "Search relevance can return unrelated titles; the adapter applies a documented allowlist.",
                    ),
                    metadata={
                        "provider": "ONS",
                        "query": query,
                        "offset": offset,
                        "limit": 1000,
                        "license_uri": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
                        "documentation_uri": "https://developer.ons.gov.uk/search/search-releases/",
                    },
                )
            )
            payload = _mapping(json.loads(content.decode("utf-8")))
            releases = _sequence(payload.get("releases"))
            breakdown = _mapping(payload.get("breakdown"))
            total = int(breakdown.get("total") or len(releases))
            offset += len(releases)
            if not releases or offset >= total:
                break
            budget.check()
        else:
            raise ValueError(f"ONS query exceeded page limit: {query}")
    return snapshots


def _fetch_ecb_snapshot(
    profile: MarketContextFetchProfileV1,
    budget: _AcquisitionBudget,
) -> MarketContextSourceSnapshotV1:
    params = {
        "format": "csvdata",
        "startPeriod": "1999-01-01",
        "endPeriod": profile.end_date,
        "includeHistory": "true",
    }
    uri = ECB_POLICY_RATE_URI + "?" + urlencode(params)
    content, content_type, retrieved = _fetch_bytes(uri, profile, budget=budget)
    return MarketContextSourceSnapshotV1(
        source_key="ecb.policy-rate",
        source_name="European Central Bank key interest rate SDMX series",
        source_uri=uri,
        retrieved_at_ns=retrieved,
        content=content,
        content_type=content_type,
        adapter_name=EcbPolicyRateAdapterV1.adapter_name,
        adapter_version=EcbPolicyRateAdapterV1.adapter_version,
        license_name="ECB statistics reuse policy",
        redistribution_allowed=True,
        redistribution_constraints=(
            "Attribute reused statistics as Source: ECB statistics.",
            "Preserve raw statistics and metadata unchanged; identify normalized corpus events as derived transformations.",
        ),
        limitations=(
            "The selected daily level series is collapsed to effective level transitions; these are not announcement timestamps.",
            "Historical revision availability is limited to the history exposed by this SDMX response.",
        ),
        metadata={
            "provider": "ECB",
            "series_key": ECB_POLICY_RATE_SERIES_KEY,
            "series_start_period": "1999-01-01",
            "license_uri": "https://www.ecb.europa.eu/stats/ecb_statistics/governance_and_quality_framework/html/usage_policy.en.html",
            "documentation_uri": "https://data.ecb.europa.eu/help/api/data",
        },
    )


def _fetch_boe_snapshot(
    profile: MarketContextFetchProfileV1,
    budget: _AcquisitionBudget,
) -> MarketContextSourceSnapshotV1:
    content, content_type, retrieved = _fetch_bytes(
        BOE_BANK_RATE_URI, profile, budget=budget
    )
    return MarketContextSourceSnapshotV1(
        source_key="boe.bank-rate",
        source_name="Bank of England official Bank Rate history",
        source_uri=BOE_BANK_RATE_URI,
        retrieved_at_ns=retrieved,
        content=content,
        content_type=content_type,
        adapter_name=BankOfEnglandBankRateAdapterV1.adapter_name,
        adapter_version=BankOfEnglandBankRateAdapterV1.adapter_version,
        license_name="Open Government Licence v3.0",
        redistribution_allowed=True,
        redistribution_constraints=(
            "Attribute the Bank of England and OGL v3.0; third-party database series remain excluded.",
        ),
        limitations=(
            "Only the official Bank Rate table is selected; no third-party exchange-rate series are reused.",
            "The table supplies date changes and levels but not historical publication vintages.",
        ),
        metadata={
            "provider": "Bank of England",
            "series": "IUDBEDR",
            "license_uri": "https://www.bankofengland.co.uk/legal",
        },
    )


def _fetch_fed_snapshots(
    profile: MarketContextFetchProfileV1,
    budget: _AcquisitionBudget,
) -> list[MarketContextSourceSnapshotV1]:
    snapshots: list[MarketContextSourceSnapshotV1] = []
    start_year = _parse_date(profile.start_date, "start_date").year
    end_year = _parse_date(profile.end_date, "end_date").year
    for year in range(max(2000, start_year), min(2020, end_year) + 1):
        uri = FED_FOMC_HISTORICAL_URI.format(year=year)
        content, content_type, retrieved = _fetch_bytes(
            uri, profile, budget=budget
        )
        snapshots.append(
            MarketContextSourceSnapshotV1(
                source_key=f"fed.fomc-historical.{year}",
                source_name=(
                    f"Federal Reserve FOMC historical materials {year}"
                ),
                source_uri=uri,
                retrieved_at_ns=retrieved,
                content=content,
                content_type=content_type,
                adapter_name=(
                    FederalReserveFomcHistoricalAdapterV1.adapter_name
                ),
                adapter_version=(
                    FederalReserveFomcHistoricalAdapterV1.adapter_version
                ),
                license_name="United States public domain",
                redistribution_allowed=True,
                redistribution_constraints=(
                    "Cite the Federal Reserve Board as source.",
                ),
                limitations=(
                    "Historical pages prove meeting-day records but not original schedule-publication times.",
                    "Date-only records are exposed ex-ante only after the local meeting day.",
                ),
                metadata={
                    "provider": "Federal Reserve Board",
                    "year": year,
                    "license_uri": "https://www.federalreserve.gov/disclaimer.htm",
                    "historical_index_uri": "https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm",
                },
            )
        )
    content, content_type, retrieved = _fetch_bytes(
        FED_FOMC_CALENDAR_URI, profile, budget=budget
    )
    snapshots.append(
        MarketContextSourceSnapshotV1(
            source_key="fed.fomc-calendar",
            source_name="Federal Reserve FOMC meeting calendar",
            source_uri=FED_FOMC_CALENDAR_URI,
            retrieved_at_ns=retrieved,
            content=content,
            content_type=content_type,
            adapter_name=FederalReserveFomcCalendarAdapterV1.adapter_name,
            adapter_version=FederalReserveFomcCalendarAdapterV1.adapter_version,
            license_name="United States public domain",
            redistribution_allowed=True,
            redistribution_constraints=(
                "Cite the Federal Reserve Board as source.",
            ),
            limitations=(
                "The live calendar currently covers 2021 onward and is not a historical schedule-vintage archive.",
                "First-known times are conservative lower-information bounds derived from event or retrieval time.",
            ),
            metadata={
                "provider": "Federal Reserve Board",
                "license_uri": "https://www.federalreserve.gov/disclaimer.htm",
                "announcement_time_evidence": "https://www.federalreserve.gov/economy-at-a-glance-policy-rate.htm",
            },
        )
    )
    return snapshots


def _read_operator_snapshot(path: str | Path) -> MarketContextSourceSnapshotV1:
    source = Path(path).expanduser().resolve()
    content = source.read_bytes()
    if not content or len(content) > MAX_CORPUS_BYTES:
        raise ValueError(
            "operator market-context catalog is empty or unbounded"
        )
    return MarketContextSourceSnapshotV1(
        source_key="operator.shock-catalog",
        source_name="HistDataCom operator-maintained public shock catalog",
        source_uri=source.as_uri(),
        retrieved_at_ns=time.time_ns(),
        content=content,
        content_type="application/json",
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        adapter_version=OperatorMarketContextCatalogAdapterV1.adapter_version,
        license_name="MIT-licensed normalized factual metadata; upstream terms cited per record",
        redistribution_allowed=True,
        redistribution_constraints=(
            "Retain the catalog's upstream public-record citations and limitations.",
        ),
        limitations=(
            "The catalog is deliberately selective and cannot establish that an unlisted shock did not occur.",
            "Window-only dates do not claim an exact market-impact timestamp or causal boundary.",
        ),
        metadata={"provider": "HistDataCom operator catalog"},
    )


def _fetch_bytes(
    uri: str,
    profile: MarketContextFetchProfileV1,
    *,
    budget: _AcquisitionBudget | None = None,
) -> tuple[bytes, str, int]:
    if budget is not None:
        budget.check()
    response_limit = profile.max_response_bytes
    request_timeout = profile.timeout_seconds
    if budget is not None:
        response_limit = min(response_limit, budget.remaining_bytes)
        request_timeout = min(request_timeout, budget.remaining_seconds)
    if response_limit <= 0:
        raise ValueError(
            "market-context acquisition exhausted total source-byte limit"
        )
    try:
        response = requests.get(
            uri,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "*/*"},
            timeout=request_timeout,
            stream=True,
        )
        try:
            response.raise_for_status()
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > response_limit:
                raise ValueError(
                    "market-context response exceeds declared byte limit"
                )
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                size += len(chunk)
                if size > response_limit:
                    raise ValueError(
                        "market-context response exceeds byte limit"
                    )
                chunks.append(chunk)
                if budget is not None:
                    budget.check()
        finally:
            response.close()
    except requests.RequestException as exc:
        raise ValueError(
            f"market-context source request failed: {uri}"
        ) from exc
    content = b"".join(chunks)
    if not content:
        raise ValueError(f"market-context source returned an empty body: {uri}")
    if budget is not None:
        budget.consume(len(content))
    return (
        content,
        str(
            response.headers.get("Content-Type") or "application/octet-stream"
        ).split(";", 1)[0],
        time.time_ns(),
    )


def _adapter_for_snapshot(snapshot: MarketContextSourceSnapshotV1) -> Any:
    adapters = {
        OnsReleaseCalendarAdapterV1.adapter_name: OnsReleaseCalendarAdapterV1,
        EcbPolicyRateAdapterV1.adapter_name: EcbPolicyRateAdapterV1,
        BankOfEnglandBankRateAdapterV1.adapter_name: BankOfEnglandBankRateAdapterV1,
        FederalReserveFomcCalendarAdapterV1.adapter_name: FederalReserveFomcCalendarAdapterV1,
        FederalReserveFomcHistoricalAdapterV1.adapter_name: FederalReserveFomcHistoricalAdapterV1,
        OperatorMarketContextCatalogAdapterV1.adapter_name: OperatorMarketContextCatalogAdapterV1,
    }
    try:
        adapter_type = adapters[snapshot.adapter_name]
    except KeyError as exc:
        raise ValueError(
            f"unsupported market-context adapter: {snapshot.adapter_name}"
        ) from exc
    return adapter_type(snapshot)


def _source_family_for_adapter(adapter_name: str) -> str:
    families = {
        OnsReleaseCalendarAdapterV1.adapter_name: "ons",
        EcbPolicyRateAdapterV1.adapter_name: "ecb",
        BankOfEnglandBankRateAdapterV1.adapter_name: "boe",
        FederalReserveFomcCalendarAdapterV1.adapter_name: "fed",
        FederalReserveFomcHistoricalAdapterV1.adapter_name: "fed",
        OperatorMarketContextCatalogAdapterV1.adapter_name: "operator",
    }
    try:
        return families[adapter_name]
    except KeyError as exc:
        raise ValueError(
            f"unsupported market-context adapter: {adapter_name}"
        ) from exc


def _event_counts_by_year_currency_kind(
    events: Sequence[MarketContextEventV1],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for event in events:
        year = datetime.fromtimestamp(
            event.event_time_ns / 1_000_000_000, tz=timezone.utc
        ).year
        for currency in event.affected_currencies:
            counts[f"{year}|{currency}|{event.kind.value}"] += 1
    return dict(sorted(counts.items()))


def _deduplicate_events(
    events: Sequence[MarketContextEventV1],
) -> tuple[tuple[MarketContextEventV1, ...], int]:
    grouped: dict[str, dict[int, list[MarketContextEventV1]]] = {}
    for event in events:
        grouped.setdefault(event.canonical_key, {}).setdefault(
            event.revision_sequence, []
        ).append(event)
    retained: list[MarketContextEventV1] = []
    duplicate_count = 0
    for canonical in sorted(grouped):
        revisions = grouped[canonical]
        expected_revisions = list(range(max(revisions) + 1))
        if sorted(revisions) != expected_revisions:
            raise ValueError(
                f"market-context revisions are not contiguous: {canonical}"
            )
        predecessor: MarketContextEventV1 | None = None
        for revision in expected_revisions:
            candidates = sorted(
                revisions[revision], key=lambda item: item.event_id
            )
            reference = candidates[0]
            for candidate in candidates[1:]:
                if _event_semantic_key(candidate) != _event_semantic_key(
                    reference
                ):
                    raise ValueError(
                        "conflicting duplicate market-context event: "
                        + canonical
                    )
                duplicate_count += 1
            expected_predecessor = (
                predecessor.event_id if predecessor is not None else None
            )
            if reference.supersedes_event_id != expected_predecessor:
                reference = replace(
                    reference,
                    supersedes_event_id=expected_predecessor,
                    event_id="",
                )
            retained.append(reference)
            predecessor = reference
    return (
        tuple(
            sorted(
                retained,
                key=lambda item: (
                    item.event_time_ns,
                    item.canonical_key,
                    item.revision_sequence,
                    item.event_id,
                ),
            )
        ),
        duplicate_count,
    )


def _event_semantic_key(event: MarketContextEventV1) -> tuple[Any, ...]:
    """Return duplicate equivalence excluding only acquisition provenance.

    The same upstream logical record can arrive through multiple ONS query
    pages, so source snapshot identity and the derived event ID are excluded.
    Every event semantic, window, value, precision, and normalized row hash is
    compared; disagreement fails closed instead of choosing one candidate.
    """
    return (
        event.schema_version,
        event.canonical_key,
        event.kind.value,
        event.title,
        event.source_event_time,
        event.source_timezone,
        event.source_time_fold,
        event.event_time_ns,
        event.first_known_at_ns,
        event.available_at_ns,
        event.pre_event_ns,
        event.post_event_ns,
        event.affected_currencies,
        event.affected_symbols,
        event.confidence,
        event.precision.value,
        event.ambiguity_reason,
        event.limitations,
        event.vintage_id,
        event.revision_sequence,
        event.expected_value,
        event.actual_value,
        event.previous_value,
        event.revised_previous_value,
        event.surprise_value,
        event.value_unit,
        event.content_sha256,
        event.tags,
    )


def _coverage_slices(
    profile: MarketContextFetchProfileV1,
    snapshots: Sequence[MarketContextSourceSnapshotV1],
    source_events: Mapping[str, tuple[MarketContextEventV1, ...]],
    source_support: Mapping[str, tuple[int, int, bool]],
    source_diagnostics: Mapping[str, tuple[str, ...]],
    retained: Sequence[MarketContextEventV1],
) -> tuple[MarketContextCoverageSliceV1, ...]:
    families = {item.adapter_name for item in snapshots}
    result: list[MarketContextCoverageSliceV1] = []
    official_specs = (
        (
            EcbPolicyRateAdapterV1.adapter_name,
            "ecb",
            "EUR",
            MarketContextKind.POLICY_RATE_CHANGE,
        ),
        (
            BankOfEnglandBankRateAdapterV1.adapter_name,
            "boe",
            "GBP",
            MarketContextKind.POLICY_RATE_CHANGE,
        ),
    )
    for adapter_name, family, currency, kind in official_specs:
        if adapter_name not in families:
            continue
        supports = [
            source_support[item.source_key]
            for item in snapshots
            if item.adapter_name == adapter_name
            and item.source_key in source_support
        ]
        if len(supports) != 1:
            continue
        support_start, support_end, support_complete = supports[0]
        start = max(profile.coverage_start_ns, support_start)
        end = min(profile.coverage_end_ns, support_end)
        if end <= start:
            continue
        count = sum(
            1
            for item in retained
            if item.kind is kind
            and currency in item.affected_currencies
            and item.source.adapter_name == adapter_name
        )
        result.append(
            MarketContextCoverageSliceV1(
                source_family=family,
                currency=currency,
                kind=kind,
                coverage_start_ns=start,
                coverage_end_ns=end,
                complete=support_complete,
                event_count=count,
                missingness_reason=(
                    "coverage is bounded by the parsed official source history; zero transitions inside a supported interval means no effective rate change"
                ),
            )
        )
    bounded_specs = (
        (
            OnsReleaseCalendarAdapterV1.adapter_name,
            "ons",
            "GBP",
            MarketContextKind.MACRO_RELEASE,
        ),
    )
    for adapter_name, family, currency, kind in bounded_specs:
        events = [
            item
            for item in retained
            if item.source.adapter_name == adapter_name
            if profile.coverage_start_ns
            <= item.event_time_ns
            < profile.coverage_end_ns
        ]
        if not events:
            continue
        start = min(item.event_time_ns for item in events)
        end = min(
            profile.coverage_end_ns,
            max(item.event_time_ns for item in events) + DAY_NS,
        )
        result.append(
            MarketContextCoverageSliceV1(
                source_family=family,
                currency=currency,
                kind=kind,
                coverage_start_ns=start,
                coverage_end_ns=end,
                complete=not _has_structural_diagnostics(
                    snapshots,
                    source_diagnostics,
                    adapter_name,
                    ignored_prefixes=("cancelled_release:",),
                ),
                event_count=len(events),
                missingness_reason=(
                    "coverage is bounded by the first and last retained live-archive records"
                ),
            )
        )
    retained_fed_events = [
        item
        for item in retained
        if item.source.adapter_name
        in {
            FederalReserveFomcHistoricalAdapterV1.adapter_name,
            FederalReserveFomcCalendarAdapterV1.adapter_name,
        }
        if profile.coverage_start_ns
        <= item.event_time_ns
        < profile.coverage_end_ns
    ]
    raw_fed_events = [
        event
        for snapshot in snapshots
        if snapshot.adapter_name
        in {
            FederalReserveFomcHistoricalAdapterV1.adapter_name,
            FederalReserveFomcCalendarAdapterV1.adapter_name,
        }
        for event in source_events.get(snapshot.source_key, ())
    ]
    if retained_fed_events and raw_fed_events:
        result.append(
            MarketContextCoverageSliceV1(
                source_family="fed",
                currency="USD",
                kind=MarketContextKind.CENTRAL_BANK_DECISION,
                coverage_start_ns=profile.coverage_start_ns,
                coverage_end_ns=min(
                    profile.coverage_end_ns,
                    max(item.event_time_ns for item in raw_fed_events) + DAY_NS,
                ),
                complete=not _has_structural_diagnostics(
                    snapshots,
                    source_diagnostics,
                    FederalReserveFomcHistoricalAdapterV1.adapter_name,
                    ignored_prefixes=("cancelled_historical_meeting:",),
                )
                and not _has_structural_diagnostics(
                    snapshots,
                    source_diagnostics,
                    FederalReserveFomcCalendarAdapterV1.adapter_name,
                    ignored_prefixes=(),
                ),
                event_count=len(retained_fed_events),
                missingness_reason=(
                    "coverage is bounded by retained official historical and current FOMC pages"
                ),
            )
        )
    operator_events = [
        item
        for item in retained
        if item.source.adapter_name
        == OperatorMarketContextCatalogAdapterV1.adapter_name
    ]
    for currency in ("EUR", "GBP", "USD"):
        count = sum(
            currency in item.affected_currencies
            and item.kind is MarketContextKind.UNSCHEDULED_SHOCK
            for item in operator_events
        )
        if count:
            result.append(
                MarketContextCoverageSliceV1(
                    source_family="operator",
                    currency=currency,
                    kind=MarketContextKind.UNSCHEDULED_SHOCK,
                    coverage_start_ns=profile.coverage_start_ns,
                    coverage_end_ns=profile.coverage_end_ns,
                    complete=False,
                    event_count=count,
                    missingness_reason=(
                        "the curated shock list is selective; absence cannot establish a no-shock state"
                    ),
                )
            )
    return tuple(result)


def _has_structural_diagnostics(
    snapshots: Sequence[MarketContextSourceSnapshotV1],
    diagnostics: Mapping[str, tuple[str, ...]],
    adapter_name: str,
    *,
    ignored_prefixes: tuple[str, ...],
) -> bool:
    return any(
        not item.startswith(ignored_prefixes)
        for snapshot in snapshots
        if snapshot.adapter_name == adapter_name
        for item in diagnostics.get(snapshot.source_key, ())
    )


class _BankRateTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_table = False
        self.in_cell = False
        self.cell_parts: list[str] = []
        self.current_row: list[str] = []
        self.rows: list[tuple[str, ...]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = dict(attrs)
        if tag.lower() == "table" and attributes.get("id") == "stats-table":
            self.in_table = True
        elif self.in_table and tag.lower() == "tr":
            self.current_row = []
        elif self.in_table and tag.lower() == "td":
            self.in_cell = True
            self.cell_parts = []

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self.in_table and tag.lower() == "td" and self.in_cell:
            self.current_row.append(" ".join("".join(self.cell_parts).split()))
            self.in_cell = False
        elif self.in_table and tag.lower() == "tr":
            if self.current_row:
                self.rows.append(tuple(self.current_row))
            self.current_row = []
        elif self.in_table and tag.lower() == "table":
            self.in_table = False


def _fomc_end_month(value: str) -> int:
    key = value.strip().split("/")[-1].lower()
    aliases = {
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
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }
    try:
        return aliases[key]
    except KeyError as exc:
        raise ValueError("unsupported FOMC month") from exc


def _verify_snapshot_adapter(
    snapshot: MarketContextSourceSnapshotV1,
    adapter_name: str,
    adapter_version: str,
) -> None:
    if (
        snapshot.adapter_name != adapter_name
        or snapshot.adapter_version != adapter_version
    ):
        raise ValueError("source snapshot adapter identity differs")


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(
                f"immutable artifact already exists with different content: {path}"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise ValueError(
                    "immutable artifact already exists with different content: "
                    f"{path}"
                ) from None
    finally:
        temporary.unlink(missing_ok=True)


def _content_suffix(content_type: str) -> str:
    lowered = content_type.lower()
    if "json" in lowered:
        return ".json"
    if "csv" in lowered:
        return ".csv"
    if "html" in lowered:
        return ".html"
    return ".bin"


def _row_sha256(row: Mapping[str | None, Any]) -> str:
    normalized = {
        str(key): str(value)
        for key, value in sorted(row.items(), key=lambda item: str(item[0]))
    }
    return hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _parse_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD") from exc


def _normalize_catalog_datetime(
    value: str,
    timezone_name: str,
    *,
    fold: int | None,
) -> int:
    text = str(value)
    try:
        parsed = datetime.fromisoformat(
            text.removesuffix("Z") + ("+00:00" if text.endswith("Z") else "")
        )
    except ValueError as exc:
        raise ValueError("operator catalog time must be ISO-8601") from exc
    return normalize_market_context_datetime(
        text,
        timezone_name,
        fold=fold if parsed.tzinfo is None else None,
    )


def _date_start_ns(value: date) -> int:
    return (
        int(
            datetime.combine(
                value, datetime.min.time(), tzinfo=timezone.utc
            ).timestamp()
        )
        * 1_000_000_000
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _source_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not _SOURCE_KEY_RE.fullmatch(text):
        raise ValueError("invalid market-context source key")
    return text


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("required text is empty")
    if len(text) > 2048:
        raise ValueError("text exceeds market-context corpus limit")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _required_text(text) if text else None


def _sha256(value: str, name: str) -> str:
    if not _SHA256_RE.fullmatch(str(value)):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return str(value)


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _strict_int(value, "optional integer")


def _bounded_int(value: Any, name: str, lower: int, upper: int) -> int:
    result = _strict_int(value, name)
    if not lower <= result <= upper:
        raise ValueError(f"{name} is outside its bound")
    return result


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("expected a numeric value")
    return float(value)


def _optional_number(value: Any) -> float | None:
    return None if value is None else _number(value)


def _positive_float(value: Any, name: str) -> float:
    result = _number(value)
    if not 0.0 < result < float("inf"):
        raise ValueError(f"{name} must be finite and positive")
    return result


def _finite_nonnegative(value: Any, name: str) -> float:
    result = _number(value)
    if not 0.0 <= result < float("inf"):
        raise ValueError(f"{name} must be finite and non-negative")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("expected a sequence")
    return value


def _sequence_or_empty(value: Any) -> Sequence[Any]:
    return () if value is None else _sequence(value)


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_value_mapping(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    restored = json.loads(encoded)
    return cast(dict[str, JSONValue], restored)


def _validate_json_mapping(value: Mapping[str, JSONValue], name: str) -> None:
    try:
        encoded = canonical_contract_json(dict(value)).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} is not JSON-compatible") from exc
    if len(encoded) > 16_384:
        raise ValueError(f"{name} exceeds 16 KiB")


__all__ = [
    "BankOfEnglandBankRateAdapterV1",
    "DEFAULT_MARKET_CONTEXT_SOURCES",
    "DEFAULT_ONS_QUERIES",
    "EcbPolicyRateAdapterV1",
    "FederalReserveFomcCalendarAdapterV1",
    "FederalReserveFomcHistoricalAdapterV1",
    "MARKET_CONTEXT_CORPUS_SCHEMA_VERSION",
    "MARKET_CONTEXT_COVERAGE_SCHEMA_VERSION",
    "MARKET_CONTEXT_PREFLIGHT_SCHEMA_VERSION",
    "MARKET_CONTEXT_SOURCE_EVIDENCE_SCHEMA_VERSION",
    "MarketContextCorpusBuildV1",
    "MarketContextCorpusPreflightError",
    "MarketContextCorpusPreflightV1",
    "MarketContextCorpusV1",
    "MarketContextCoverageSliceV1",
    "MarketContextFetchProfileV1",
    "MarketContextSourceEvidenceV1",
    "MarketContextSourceSnapshotV1",
    "OnsReleaseCalendarAdapterV1",
    "OperatorMarketContextCatalogAdapterV1",
    "build_live_market_context_corpus",
    "build_market_context_corpus_from_snapshots",
    "market_context_benchmark_event_state",
    "packaged_operator_catalog_path",
    "preflight_market_context_corpus",
    "query_market_context_corpus",
    "read_market_context_corpus",
    "replay_market_context_corpus",
    "require_market_context_corpus",
    "write_market_context_corpus",
]

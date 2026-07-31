"""Comprehensive, licensed economic-calendar ingestion and query contracts.

The existing :mod:`histdatacom.market_context.corpus` module deliberately uses
small public official-source corpora.  This module is the scalable companion
for an operator-licensed calendar provider.  It retains raw provider fields,
accumulates refresh vintages, and projects only bounded query results into the
existing ``MarketContextEventV1`` seam.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any, TypeAlias, cast
from urllib.parse import quote, urlencode

import requests

from histdatacom.fx_enums import Pairs
from histdatacom.market_context.contracts import (
    MarketContextEventV1,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextQueryV1,
    MarketContextSourceV1,
    MarketContextTimelineV1,
    MarketContextView,
    canonical_contract_json,
    query_market_context,
)
from histdatacom.market_context.corpus import (
    MarketContextCorpusPreflightError,
    MarketContextCorpusPreflightV1,
    MarketContextCorpusV1,
    MarketContextSourceSnapshotV1,
    preflight_market_context_corpus,
    query_market_context_corpus,
    read_market_context_corpus,
)
from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

ECONOMIC_CALENDAR_EVENT_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-event.v1"
)
ECONOMIC_CALENDAR_SOURCE_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-source.v1"
)
ECONOMIC_CALENDAR_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-coverage.v1"
)
ECONOMIC_CALENDAR_PROFILE_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-profile.v1"
)
ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-corpus.v1"
)
ECONOMIC_CALENDAR_FETCH_PLAN_SCHEMA_VERSION = (
    "histdatacom.economic-calendar-fetch-plan.v1"
)

TRADING_ECONOMICS_API_ROOT = "https://api.tradingeconomics.com"
TRADING_ECONOMICS_ADAPTER_NAME = "trading-economics-calendar"
TRADING_ECONOMICS_ADAPTER_VERSION = "1.0.0"
TRADING_ECONOMICS_LICENSE_NAME = "Trading Economics subscription terms"
TRADING_ECONOMICS_TERMS_URI = "https://tradingeconomics.com/terms.aspx"
TRADING_ECONOMICS_PRICING_URI = "https://tradingeconomics.com/api/pricing.aspx"
TRADING_ECONOMICS_SCHEMA_URI = (
    "https://docs.tradingeconomics.com/economic_calendar/schema/"
)
TRADING_ECONOMICS_RATE_LIMIT_URI = (
    "https://docs.tradingeconomics.com/get_started/rate-limits/"
)

DAY_NS = 86_400_000_000_000
HOUR_NS = 3_600_000_000_000
MAX_ECONOMIC_CALENDAR_EVENTS = 2_000_000
MAX_ECONOMIC_CALENDAR_SOURCES = 20_000
MAX_ECONOMIC_CALENDAR_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_ECONOMIC_CALENDAR_TOTAL_BYTES = 8 * 1024**3
MAX_ECONOMIC_CALENDAR_REQUESTS = 20_000
MAX_PROVIDER_ROWS_PER_REQUEST = 1_000
MAX_QUERY_CANDIDATES = 4_096

# Trading Economics country names for every fiat currency represented by the
# HistData.com public instrument catalog.  The euro area is the common currency
# economy; national index instruments add France or Germany below.
CURRENCY_ECONOMY: Mapping[str, str] = {
    "AUD": "Australia",
    "CAD": "Canada",
    "CHF": "Switzerland",
    "CZK": "Czech Republic",
    "DKK": "Denmark",
    "EUR": "Euro Area",
    "GBP": "United Kingdom",
    "HKD": "Hong Kong",
    "HUF": "Hungary",
    "JPY": "Japan",
    "MXN": "Mexico",
    "NOK": "Norway",
    "NZD": "New Zealand",
    "PLN": "Poland",
    "SEK": "Sweden",
    "SGD": "Singapore",
    "TRY": "Turkey",
    "USD": "United States",
    "ZAR": "South Africa",
}

# Non-FX HistData symbols are mapped to their underlying national economy.
# Metals have no single national economy and therefore inherit their quote
# currency economy.  Brent is global and likewise inherits USD; WTI adds the
# United States, which is already implied by its quote currency.
INSTRUMENT_ECONOMY: Mapping[str, str] = {
    "AUX": "Australia",
    "ETX": "Euro Area",
    "FRX": "France",
    "GRX": "Germany",
    "HKX": "Hong Kong",
    "JPX": "Japan",
    "NSX": "United States",
    "SPX": "United States",
    "UDX": "United States",
    "UKX": "United Kingdom",
    "WTI": "United States",
}

ECONOMY_CURRENCY: Mapping[str, str] = {
    **{economy: currency for currency, economy in CURRENCY_ECONOMY.items()},
    "France": "EUR",
    "Germany": "EUR",
}
DEFAULT_ECONOMIC_CALENDAR_ECONOMIES: tuple[str, ...] = tuple(
    sorted(ECONOMY_CURRENCY)
)

_SOURCE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,159}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
_CENTRAL_BANK_RE = re.compile(
    r"(?:interest rate decision|rate decision|central bank|fomc|monetary policy)",
    re.IGNORECASE,
)
_COMMUNICATION_RE = re.compile(
    r"(?:speech|press conference|minutes|testimony)", re.IGNORECASE
)
_MULTIPLIERS: Mapping[str, float] = {
    "K": 1_000.0,
    "M": 1_000_000.0,
    "B": 1_000_000_000.0,
    "T": 1_000_000_000_000.0,
}


def _required_text(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _bounded_int(value: object, name: str, low: int, high: int) -> int:
    result = _strict_int(value, name)
    if not low <= result <= high:
        raise ValueError(f"{name} is outside the supported range")
    return result


def _finite_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _optional_number(value: object) -> float | None:
    if value is None or value == "":
        return None
    return _finite_float(value, "calendar numeric value")


def _parse_date(value: object, name: str) -> date:
    try:
        return date.fromisoformat(_required_text(value, name))
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD") from exc


def _parse_utc_ns(value: object, name: str) -> int:
    text = _required_text(value, name)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{name} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1_000_000_000)


def _optional_utc_ns(value: object, name: str) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return _parse_utc_ns(value, name)


def _utc_text(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(
        timestamp_ns / 1_000_000_000, tz=timezone.utc
    ).isoformat()


def _canonical_json(payload: Mapping[str, JSONValue]) -> str:
    return str(canonical_contract_json(payload))


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _string_tuple(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _source_key(value: object) -> str:
    text = _required_text(value, "source_key").lower()
    if not _SOURCE_KEY_RE.fullmatch(text):
        raise ValueError("source_key is not canonical")
    return text


def _sha256(value: object, name: str) -> str:
    text = _required_text(value, name).lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return text


def validate_api_key_env_name(value: object) -> str:
    """Validate an environment-variable name without reading its secret."""
    text = _required_text(value, "api_key_env")
    if not _ENV_NAME_RE.fullmatch(text):
        raise ValueError("api_key_env must be an uppercase environment name")
    return text


def histdata_pair_economies() -> dict[str, tuple[str, ...]]:
    """Return complete pair-to-economy coverage for the HistData catalog."""
    result: dict[str, tuple[str, ...]] = {}
    for pair in Pairs:
        base, quote_code = pair.value.split("_", maxsplit=1)
        economies: set[str] = set()
        for code in (base, quote_code):
            economy = CURRENCY_ECONOMY.get(code) or INSTRUMENT_ECONOMY.get(code)
            if economy:
                economies.add(economy)
        if not economies:
            raise ValueError(
                f"HistData pair has no calendar economy: {pair.name}"
            )
        result[pair.name] = tuple(sorted(economies))
    return dict(sorted(result.items()))


def histdata_economy_symbols() -> dict[str, tuple[str, ...]]:
    """Return every HistData symbol affected by each calendar economy."""
    by_economy: dict[str, list[str]] = {
        economy: [] for economy in DEFAULT_ECONOMIC_CALENDAR_ECONOMIES
    }
    for symbol, economies in histdata_pair_economies().items():
        for economy in economies:
            by_economy.setdefault(economy, []).append(symbol)
    return {
        economy: tuple(sorted(set(symbols)))
        for economy, symbols in sorted(by_economy.items())
    }


def parse_calendar_number(value: object) -> float | None:
    """Parse a provider lexical value when numeric companion fields are absent."""
    text = _optional_text(value)
    if text is None:
        return None
    normalized = text.strip().replace(",", "").replace("−", "-")
    if normalized.lower() in {"n/a", "na", "nan", "none", "-", "--"}:
        return None
    negative = normalized.startswith("(") and normalized.endswith(")")
    if negative:
        normalized = normalized[1:-1]
    normalized = re.sub(r"^[^+\-\d.]+", "", normalized)
    normalized = normalized.rstrip("%")
    multiplier = 1.0
    if normalized and normalized[-1:].upper() in _MULTIPLIERS:
        multiplier = _MULTIPLIERS[normalized[-1].upper()]
        normalized = normalized[:-1]
    normalized = re.sub(r"[^0-9eE+\-.].*$", "", normalized)
    if not normalized or normalized in {"+", "-", "."}:
        return None
    try:
        result = float(normalized) * multiplier
    except ValueError:
        return None
    if negative:
        result = -abs(result)
    return result if math.isfinite(result) else None


def _provider_number(row: Mapping[str, Any], name: str) -> float | None:
    numeric_name = name + "Value"
    numeric = row.get(numeric_name)
    if numeric is not None and numeric != "":
        try:
            return _optional_number(numeric)
        except ValueError:
            pass
    return parse_calendar_number(row.get(name))


def _event_kind(category: str, title: str) -> MarketContextKind:
    combined = f"{category} {title}"
    if _COMMUNICATION_RE.search(combined):
        return MarketContextKind.SCHEDULED_COMMUNICATION
    if _CENTRAL_BANK_RE.search(combined):
        return MarketContextKind.CENTRAL_BANK_DECISION
    return MarketContextKind.MACRO_RELEASE


def _date_windows(
    start: date, end: date, *, maximum_days: int
) -> tuple[tuple[date, date], ...]:
    if maximum_days < 1:
        raise ValueError("maximum_days must be positive")
    result: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=maximum_days - 1))
        result.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return tuple(result)


def trading_economics_request_uri(economy: str, start: date, end: date) -> str:
    """Build one credential-free provider URI safe for logs and artifacts."""
    country = quote(_required_text(economy, "economy"), safe="")
    query = urlencode({"f": "json", "values": "true"})
    return (
        f"{TRADING_ECONOMICS_API_ROOT}/calendar/country/{country}/"
        f"{start.isoformat()}/{end.isoformat()}?{query}"
    )


@dataclass(frozen=True, slots=True)
class EconomicCalendarFetchProfileV1:
    """Bounded acquisition policy for a comprehensive licensed calendar."""

    start_date: str
    end_date: str
    economies: tuple[str, ...] = DEFAULT_ECONOMIC_CALENDAR_ECONOMIES
    initial_window_days: int = 366
    timeout_seconds: float = 45.0
    min_request_interval_seconds: float = 0.51
    max_response_bytes: int = 32 * 1024 * 1024
    max_total_source_bytes: int = 2 * 1024**3
    max_requests: int = 10_000
    max_events: int = 1_000_000
    max_runtime_seconds: float = 7_200.0
    pre_event_ns: int = HOUR_NS
    post_event_ns: int = 4 * HOUR_NS
    schema_version: str = ECONOMIC_CALENDAR_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported economic-calendar profile schema")
        start = _parse_date(self.start_date, "start_date")
        end = _parse_date(self.end_date, "end_date")
        if end < start:
            raise ValueError("economic-calendar end_date precedes start_date")
        economies = tuple(
            sorted({_required_text(item, "economy") for item in self.economies})
        )
        unsupported = set(economies).difference(
            DEFAULT_ECONOMIC_CALENDAR_ECONOMIES
        )
        if unsupported:
            raise ValueError(
                "unsupported HistData calendar economy: "
                + ", ".join(sorted(unsupported))
            )
        if not economies:
            raise ValueError("economic-calendar economies are empty")
        _bounded_int(self.initial_window_days, "initial_window_days", 1, 3660)
        timeout = _finite_float(self.timeout_seconds, "timeout_seconds")
        interval = _finite_float(
            self.min_request_interval_seconds,
            "min_request_interval_seconds",
        )
        if timeout <= 0 or interval < 0:
            raise ValueError("calendar timeout/interval bounds are invalid")
        _bounded_int(
            self.max_response_bytes,
            "max_response_bytes",
            1,
            MAX_ECONOMIC_CALENDAR_RESPONSE_BYTES,
        )
        _bounded_int(
            self.max_total_source_bytes,
            "max_total_source_bytes",
            1,
            MAX_ECONOMIC_CALENDAR_TOTAL_BYTES,
        )
        _bounded_int(
            self.max_requests,
            "max_requests",
            1,
            MAX_ECONOMIC_CALENDAR_REQUESTS,
        )
        _bounded_int(
            self.max_events,
            "max_events",
            1,
            MAX_ECONOMIC_CALENDAR_EVENTS,
        )
        runtime = _finite_float(self.max_runtime_seconds, "max_runtime_seconds")
        if runtime <= 0:
            raise ValueError("max_runtime_seconds must be positive")
        _bounded_int(self.pre_event_ns, "pre_event_ns", 0, 7 * DAY_NS)
        _bounded_int(self.post_event_ns, "post_event_ns", 1, 7 * DAY_NS)
        object.__setattr__(self, "start_date", start.isoformat())
        object.__setattr__(self, "end_date", end.isoformat())
        object.__setattr__(self, "economies", economies)

    @property
    def coverage_start_ns(self) -> int:
        return int(
            datetime.combine(
                _parse_date(self.start_date, "start_date"),
                datetime.min.time(),
                tzinfo=timezone.utc,
            ).timestamp()
            * 1_000_000_000
        )

    @property
    def coverage_end_ns(self) -> int:
        return int(
            datetime.combine(
                _parse_date(self.end_date, "end_date") + timedelta(days=1),
                datetime.min.time(),
                tzinfo=timezone.utc,
            ).timestamp()
            * 1_000_000_000
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "economies": list(self.economies),
            "initial_window_days": self.initial_window_days,
            "timeout_seconds": self.timeout_seconds,
            "min_request_interval_seconds": self.min_request_interval_seconds,
            "max_response_bytes": self.max_response_bytes,
            "max_total_source_bytes": self.max_total_source_bytes,
            "max_requests": self.max_requests,
            "max_events": self.max_events,
            "max_runtime_seconds": self.max_runtime_seconds,
            "pre_event_ns": self.pre_event_ns,
            "post_event_ns": self.post_event_ns,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicCalendarFetchProfileV1:
        return cls(
            start_date=str(data.get("start_date", "")),
            end_date=str(data.get("end_date", "")),
            economies=_string_tuple(data.get("economies")),
            initial_window_days=_strict_int(
                data.get("initial_window_days"), "initial_window_days"
            ),
            timeout_seconds=_finite_float(
                data.get("timeout_seconds"), "timeout_seconds"
            ),
            min_request_interval_seconds=_finite_float(
                data.get("min_request_interval_seconds"),
                "min_request_interval_seconds",
            ),
            max_response_bytes=_strict_int(
                data.get("max_response_bytes"), "max_response_bytes"
            ),
            max_total_source_bytes=_strict_int(
                data.get("max_total_source_bytes"),
                "max_total_source_bytes",
            ),
            max_requests=_strict_int(data.get("max_requests"), "max_requests"),
            max_events=_strict_int(data.get("max_events"), "max_events"),
            max_runtime_seconds=_finite_float(
                data.get("max_runtime_seconds"), "max_runtime_seconds"
            ),
            pre_event_ns=_strict_int(data.get("pre_event_ns"), "pre_event_ns"),
            post_event_ns=_strict_int(
                data.get("post_event_ns"), "post_event_ns"
            ),
            schema_version=str(data.get("schema_version", "")),
        )


def economic_calendar_fetch_plan(
    profile: EconomicCalendarFetchProfileV1,
) -> dict[str, JSONValue]:
    """Return a secret-free initial request plan before adaptive splits."""
    if not isinstance(profile, EconomicCalendarFetchProfileV1):
        raise TypeError("fetch plan requires an economic-calendar profile")
    windows = _date_windows(
        _parse_date(profile.start_date, "start_date"),
        _parse_date(profile.end_date, "end_date"),
        maximum_days=profile.initial_window_days,
    )
    requests_payload: list[dict[str, JSONValue]] = []
    for economy in profile.economies:
        for start, end in windows:
            requests_payload.append(
                {
                    "economy": economy,
                    "currency": ECONOMY_CURRENCY[economy],
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                    "uri": trading_economics_request_uri(economy, start, end),
                }
            )
    return {
        "schema_version": ECONOMIC_CALENDAR_FETCH_PLAN_SCHEMA_VERSION,
        "provider": "Trading Economics",
        "profile": profile.to_dict(),
        "pair_count": len(Pairs),
        "economy_count": len(profile.economies),
        "initial_request_count": len(requests_payload),
        "adaptive_split_policy": (
            "Responses at the documented 1,000-row ceiling are retained as "
            "probes and bisected until every leaf response is below the ceiling."
        ),
        "authentication": (
            "Authorization header from an operator-selected environment variable; "
            "credentials are never placed in request URIs or artifacts."
        ),
        "requests": cast(list[JSONValue], requests_payload),
    }


@dataclass(frozen=True, slots=True)
class EconomicCalendarSourceEvidenceV1:
    """Restricted-source evidence retained without embedding credentials."""

    source_key: str
    source_uri: str
    retrieved_at_ns: int
    content_sha256: str
    size_bytes: int
    content_type: str
    economy: str
    currency: str
    start_date: str
    end_date: str
    row_count: int
    complete_leaf: bool
    limitations: tuple[str, ...]
    schema_version: str = ECONOMIC_CALENDAR_SOURCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_SOURCE_SCHEMA_VERSION:
            raise ValueError("unsupported economic-calendar source schema")
        object.__setattr__(self, "source_key", _source_key(self.source_key))
        object.__setattr__(
            self, "source_uri", _required_text(self.source_uri, "source_uri")
        )
        _bounded_int(self.retrieved_at_ns, "retrieved_at_ns", 0, 2**63 - 1)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _bounded_int(
            self.size_bytes,
            "size_bytes",
            1,
            MAX_ECONOMIC_CALENDAR_RESPONSE_BYTES,
        )
        object.__setattr__(
            self,
            "content_type",
            _required_text(self.content_type, "content_type"),
        )
        economy = _required_text(self.economy, "economy")
        if economy not in ECONOMY_CURRENCY:
            raise ValueError("source economy is outside HistData coverage")
        currency = _required_text(self.currency, "currency").upper()
        if currency != ECONOMY_CURRENCY[economy]:
            raise ValueError("source currency differs from economy mapping")
        start = _parse_date(self.start_date, "start_date")
        end = _parse_date(self.end_date, "end_date")
        if end < start:
            raise ValueError("source date range is reversed")
        _bounded_int(
            self.row_count,
            "row_count",
            0,
            MAX_PROVIDER_ROWS_PER_REQUEST,
        )
        if not isinstance(self.complete_leaf, bool):
            raise TypeError("complete_leaf must be boolean")
        limitations = tuple(
            _required_text(item, "limitation") for item in self.limitations
        )
        if not limitations:
            raise ValueError("calendar source evidence requires limitations")
        object.__setattr__(self, "economy", economy)
        object.__setattr__(self, "currency", currency)
        object.__setattr__(self, "start_date", start.isoformat())
        object.__setattr__(self, "end_date", end.isoformat())
        object.__setattr__(self, "limitations", limitations)

    @classmethod
    def from_snapshot(
        cls, snapshot: MarketContextSourceSnapshotV1
    ) -> EconomicCalendarSourceEvidenceV1:
        metadata = snapshot.metadata
        return cls(
            source_key=snapshot.source_key,
            source_uri=snapshot.source_uri,
            retrieved_at_ns=snapshot.retrieved_at_ns,
            content_sha256=snapshot.content_sha256,
            size_bytes=len(snapshot.content),
            content_type=snapshot.content_type,
            economy=str(metadata.get("economy", "")),
            currency=str(metadata.get("currency", "")),
            start_date=str(metadata.get("start_date", "")),
            end_date=str(metadata.get("end_date", "")),
            row_count=_strict_int(metadata.get("row_count"), "row_count"),
            complete_leaf=bool(metadata.get("complete_leaf")),
            limitations=snapshot.limitations,
        )

    def source_contract(self) -> MarketContextSourceV1:
        return MarketContextSourceV1(
            name="Trading Economics economic calendar",
            source_version=f"sha256:{self.content_sha256}",
            retrieved_at_ns=self.retrieved_at_ns,
            content_sha256=self.content_sha256,
            adapter_name=TRADING_ECONOMICS_ADAPTER_NAME,
            adapter_version=TRADING_ECONOMICS_ADAPTER_VERSION,
            license_name=TRADING_ECONOMICS_LICENSE_NAME,
            redistribution_allowed=False,
            redistribution_constraints=(
                "Local analysis only unless the operator's Trading Economics agreement explicitly grants redistribution.",
                "Do not publish raw payloads or normalized provider data from this artifact without enterprise distribution rights.",
            ),
            limitations=self.limitations,
            source_uri=self.source_uri,
            metadata={
                "source_key": self.source_key,
                "provider": "Trading Economics",
                "economy": self.economy,
                "currency": self.currency,
                "request_start_date": self.start_date,
                "request_end_date": self.end_date,
                "terms_uri": TRADING_ECONOMICS_TERMS_URI,
                "pricing_uri": TRADING_ECONOMICS_PRICING_URI,
                "schema_uri": TRADING_ECONOMICS_SCHEMA_URI,
            },
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_key": self.source_key,
            "source_uri": self.source_uri,
            "retrieved_at_ns": self.retrieved_at_ns,
            "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes,
            "content_type": self.content_type,
            "economy": self.economy,
            "currency": self.currency,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "row_count": self.row_count,
            "complete_leaf": self.complete_leaf,
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicCalendarSourceEvidenceV1:
        return cls(
            source_key=str(data.get("source_key", "")),
            source_uri=str(data.get("source_uri", "")),
            retrieved_at_ns=_strict_int(
                data.get("retrieved_at_ns"), "retrieved_at_ns"
            ),
            content_sha256=str(data.get("content_sha256", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "size_bytes"),
            content_type=str(data.get("content_type", "")),
            economy=str(data.get("economy", "")),
            currency=str(data.get("currency", "")),
            start_date=str(data.get("start_date", "")),
            end_date=str(data.get("end_date", "")),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            complete_leaf=bool(data.get("complete_leaf")),
            limitations=_string_tuple(data.get("limitations")),
            schema_version=str(data.get("schema_version", "")),
        )

    def restore_snapshot(self, content: bytes) -> MarketContextSourceSnapshotV1:
        if len(content) != self.size_bytes:
            raise ValueError(
                "calendar source snapshot size differs from evidence"
            )
        if hashlib.sha256(content).hexdigest() != self.content_sha256:
            raise ValueError(
                "calendar source snapshot hash differs from evidence"
            )
        return MarketContextSourceSnapshotV1(
            source_key=self.source_key,
            source_name="Trading Economics economic calendar",
            source_uri=self.source_uri,
            retrieved_at_ns=self.retrieved_at_ns,
            content=content,
            content_type=self.content_type,
            adapter_name=TRADING_ECONOMICS_ADAPTER_NAME,
            adapter_version=TRADING_ECONOMICS_ADAPTER_VERSION,
            license_name=TRADING_ECONOMICS_LICENSE_NAME,
            redistribution_allowed=False,
            redistribution_constraints=(
                "Local analysis only unless the operator's subscription grants redistribution.",
                "Never commit or publish raw Trading Economics responses by default.",
            ),
            limitations=self.limitations,
            metadata={
                "provider": "Trading Economics",
                "economy": self.economy,
                "currency": self.currency,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "row_count": self.row_count,
                "complete_leaf": self.complete_leaf,
                "terms_uri": TRADING_ECONOMICS_TERMS_URI,
                "pricing_uri": TRADING_ECONOMICS_PRICING_URI,
                "schema_uri": TRADING_ECONOMICS_SCHEMA_URI,
                "rate_limit_uri": TRADING_ECONOMICS_RATE_LIMIT_URI,
            },
        )


@dataclass(frozen=True, slots=True)
class EconomicCalendarEventV1:
    """One immutable provider event vintage with complete lexical values."""

    provider_event_id: str
    economy: str
    currency: str
    category: str
    title: str
    reference: str | None
    reference_date_ns: int | None
    release_time_ns: int
    provider_updated_at_ns: int
    observed_at_ns: int
    first_known_at_ns: int
    available_at_ns: int
    timing_estimated: bool
    importance: int
    actual_text: str | None
    previous_text: str | None
    forecast_text: str | None
    provider_forecast_text: str | None
    revised_text: str | None
    actual_value: float | None
    previous_value: float | None
    forecast_value: float | None
    provider_forecast_value: float | None
    revised_value: float | None
    currency_symbol: str | None
    unit: str | None
    ticker: str | None
    indicator_symbol: str | None
    primary_source_name: str | None
    primary_source_url: str | None
    provider_url: str | None
    affected_currencies: tuple[str, ...]
    affected_symbols: tuple[str, ...]
    source_key: str
    source_content_sha256: str
    raw_row_sha256: str
    revision_sequence: int = 0
    supersedes_event_id: str | None = None
    event_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported economic-calendar event schema")
        object.__setattr__(
            self,
            "provider_event_id",
            _required_text(self.provider_event_id, "provider_event_id"),
        )
        economy = _required_text(self.economy, "economy")
        if economy not in ECONOMY_CURRENCY:
            raise ValueError("calendar event economy is unsupported")
        currency = _required_text(self.currency, "currency").upper()
        if currency != ECONOMY_CURRENCY[economy]:
            raise ValueError("calendar event currency differs from economy")
        object.__setattr__(self, "economy", economy)
        object.__setattr__(self, "currency", currency)
        object.__setattr__(
            self, "category", _required_text(self.category, "category")
        )
        object.__setattr__(self, "title", _required_text(self.title, "title"))
        object.__setattr__(self, "reference", _optional_text(self.reference))
        for name in (
            "reference_date_ns",
            "release_time_ns",
            "provider_updated_at_ns",
            "observed_at_ns",
            "first_known_at_ns",
            "available_at_ns",
        ):
            value = getattr(self, name)
            if value is not None:
                _bounded_int(value, name, 0, 2**63 - 1)
        if self.first_known_at_ns > self.available_at_ns:
            raise ValueError("event first-known time follows availability")
        if not isinstance(self.timing_estimated, bool):
            raise TypeError("timing_estimated must be boolean")
        _bounded_int(self.importance, "importance", 1, 3)
        for name in (
            "actual_text",
            "previous_text",
            "forecast_text",
            "provider_forecast_text",
            "revised_text",
            "currency_symbol",
            "unit",
            "ticker",
            "indicator_symbol",
            "primary_source_name",
            "primary_source_url",
            "provider_url",
        ):
            object.__setattr__(self, name, _optional_text(getattr(self, name)))
        for name in (
            "actual_value",
            "previous_value",
            "forecast_value",
            "provider_forecast_value",
            "revised_value",
        ):
            value = getattr(self, name)
            object.__setattr__(
                self,
                name,
                None if value is None else _finite_float(value, name),
            )
        currencies = tuple(
            sorted(
                {
                    _required_text(item, "currency").upper()
                    for item in self.affected_currencies
                }
            )
        )
        symbols = tuple(
            sorted(
                {
                    _required_text(item, "symbol").lower()
                    for item in self.affected_symbols
                }
            )
        )
        if currency not in currencies or not symbols:
            raise ValueError("calendar event affected coverage is incomplete")
        object.__setattr__(self, "affected_currencies", currencies)
        object.__setattr__(self, "affected_symbols", symbols)
        object.__setattr__(self, "source_key", _source_key(self.source_key))
        object.__setattr__(
            self,
            "source_content_sha256",
            _sha256(self.source_content_sha256, "source_content_sha256"),
        )
        object.__setattr__(
            self,
            "raw_row_sha256",
            _sha256(self.raw_row_sha256, "raw_row_sha256"),
        )
        revision = _bounded_int(
            self.revision_sequence,
            "revision_sequence",
            0,
            1_000_000,
        )
        supersedes = _optional_text(self.supersedes_event_id)
        if (revision == 0) != (supersedes is None):
            raise ValueError("calendar revision predecessor is inconsistent")
        object.__setattr__(self, "supersedes_event_id", supersedes)
        expected = _stable_id(
            "economic-calendar-event", self.identity_payload()
        )
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("economic-calendar event_id is not deterministic")
        object.__setattr__(self, "event_id", expected)

    @property
    def canonical_key(self) -> str:
        return f"trading-economics:{self.provider_event_id}"

    def semantic_payload(self) -> dict[str, JSONValue]:
        """Return provider semantics excluding acquisition and revision lineage."""
        return {
            "provider_event_id": self.provider_event_id,
            "economy": self.economy,
            "currency": self.currency,
            "category": self.category,
            "title": self.title,
            "reference": self.reference,
            "reference_date_ns": self.reference_date_ns,
            "release_time_ns": self.release_time_ns,
            "provider_updated_at_ns": self.provider_updated_at_ns,
            "timing_estimated": self.timing_estimated,
            "importance": self.importance,
            "actual_text": self.actual_text,
            "previous_text": self.previous_text,
            "forecast_text": self.forecast_text,
            "provider_forecast_text": self.provider_forecast_text,
            "revised_text": self.revised_text,
            "actual_value": self.actual_value,
            "previous_value": self.previous_value,
            "forecast_value": self.forecast_value,
            "provider_forecast_value": self.provider_forecast_value,
            "revised_value": self.revised_value,
            "currency_symbol": self.currency_symbol,
            "unit": self.unit,
            "ticker": self.ticker,
            "indicator_symbol": self.indicator_symbol,
            "primary_source_name": self.primary_source_name,
            "primary_source_url": self.primary_source_url,
            "provider_url": self.provider_url,
            "affected_currencies": list(self.affected_currencies),
            "affected_symbols": list(self.affected_symbols),
            "raw_row_sha256": self.raw_row_sha256,
        }

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            **self.semantic_payload(),
            "observed_at_ns": self.observed_at_ns,
            "first_known_at_ns": self.first_known_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_content_sha256": self.source_content_sha256,
            "revision_sequence": self.revision_sequence,
            "supersedes_event_id": self.supersedes_event_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "event_id": self.event_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarEventV1:
        return cls(
            provider_event_id=str(data.get("provider_event_id", "")),
            economy=str(data.get("economy", "")),
            currency=str(data.get("currency", "")),
            category=str(data.get("category", "")),
            title=str(data.get("title", "")),
            reference=_optional_text(data.get("reference")),
            reference_date_ns=(
                None
                if data.get("reference_date_ns") is None
                else _strict_int(
                    data.get("reference_date_ns"), "reference_date_ns"
                )
            ),
            release_time_ns=_strict_int(
                data.get("release_time_ns"), "release_time_ns"
            ),
            provider_updated_at_ns=_strict_int(
                data.get("provider_updated_at_ns"), "provider_updated_at_ns"
            ),
            observed_at_ns=_strict_int(
                data.get("observed_at_ns"), "observed_at_ns"
            ),
            first_known_at_ns=_strict_int(
                data.get("first_known_at_ns"), "first_known_at_ns"
            ),
            available_at_ns=_strict_int(
                data.get("available_at_ns"), "available_at_ns"
            ),
            timing_estimated=bool(data.get("timing_estimated")),
            importance=_strict_int(data.get("importance"), "importance"),
            actual_text=_optional_text(data.get("actual_text")),
            previous_text=_optional_text(data.get("previous_text")),
            forecast_text=_optional_text(data.get("forecast_text")),
            provider_forecast_text=_optional_text(
                data.get("provider_forecast_text")
            ),
            revised_text=_optional_text(data.get("revised_text")),
            actual_value=_optional_number(data.get("actual_value")),
            previous_value=_optional_number(data.get("previous_value")),
            forecast_value=_optional_number(data.get("forecast_value")),
            provider_forecast_value=_optional_number(
                data.get("provider_forecast_value")
            ),
            revised_value=_optional_number(data.get("revised_value")),
            currency_symbol=_optional_text(data.get("currency_symbol")),
            unit=_optional_text(data.get("unit")),
            ticker=_optional_text(data.get("ticker")),
            indicator_symbol=_optional_text(data.get("indicator_symbol")),
            primary_source_name=_optional_text(data.get("primary_source_name")),
            primary_source_url=_optional_text(data.get("primary_source_url")),
            provider_url=_optional_text(data.get("provider_url")),
            affected_currencies=_string_tuple(data.get("affected_currencies")),
            affected_symbols=_string_tuple(data.get("affected_symbols")),
            source_key=str(data.get("source_key", "")),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            raw_row_sha256=str(data.get("raw_row_sha256", "")),
            revision_sequence=_strict_int(
                data.get("revision_sequence", 0), "revision_sequence"
            ),
            supersedes_event_id=_optional_text(data.get("supersedes_event_id")),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _event_from_row(
    row: Mapping[str, Any],
    snapshot: MarketContextSourceSnapshotV1,
) -> EconomicCalendarEventV1:
    economy = _required_text(row.get("Country"), "Country")
    requested = _required_text(snapshot.metadata.get("economy"), "economy")
    if economy.casefold() != requested.casefold():
        raise ValueError(
            f"calendar row economy differs from request: {economy} != {requested}"
        )
    # Preserve the canonical spelling used by our coverage matrix.
    economy = requested
    currency = ECONOMY_CURRENCY[economy]
    release_time = _parse_utc_ns(row.get("Date"), "Date")
    provider_update = _optional_utc_ns(row.get("LastUpdate"), "LastUpdate")
    if provider_update is None:
        provider_update = release_time
    if provider_update > snapshot.retrieved_at_ns:
        raise ValueError("provider LastUpdate follows snapshot retrieval")
    actual_text = _optional_text(row.get("Actual"))
    historical = release_time <= snapshot.retrieved_at_ns
    if actual_text is not None or historical:
        first_known = release_time
        available = max(release_time, provider_update)
    else:
        first_known = snapshot.retrieved_at_ns
        available = snapshot.retrieved_at_ns
    raw_payload = cast(dict[str, JSONValue], dict(row))
    raw_hash = hashlib.sha256(
        _canonical_json(raw_payload).encode("utf-8")
    ).hexdigest()
    symbols = histdata_economy_symbols()[economy]
    provider_url = _optional_text(row.get("URL"))
    if provider_url and provider_url.startswith("/"):
        provider_url = "https://tradingeconomics.com" + provider_url
    return EconomicCalendarEventV1(
        provider_event_id=_required_text(row.get("CalendarId"), "CalendarId"),
        economy=economy,
        currency=currency,
        category=_required_text(row.get("Category"), "Category"),
        title=_required_text(row.get("Event"), "Event"),
        reference=_optional_text(row.get("Reference")),
        reference_date_ns=_optional_utc_ns(
            row.get("ReferenceDate"), "ReferenceDate"
        ),
        release_time_ns=release_time,
        provider_updated_at_ns=provider_update,
        observed_at_ns=snapshot.retrieved_at_ns,
        first_known_at_ns=first_known,
        available_at_ns=available,
        timing_estimated=str(row.get("DateSpan", "0")).strip() != "0",
        importance=_bounded_int(
            int(row.get("Importance") or 1), "Importance", 1, 3
        ),
        actual_text=actual_text,
        previous_text=_optional_text(row.get("Previous")),
        forecast_text=_optional_text(row.get("Forecast")),
        provider_forecast_text=_optional_text(row.get("TEForecast")),
        revised_text=_optional_text(row.get("Revised")),
        actual_value=_provider_number(row, "Actual"),
        previous_value=_provider_number(row, "Previous"),
        forecast_value=_provider_number(row, "Forecast"),
        provider_forecast_value=_provider_number(row, "TEForecast"),
        revised_value=_provider_number(row, "Revised"),
        currency_symbol=_optional_text(row.get("Currency")),
        unit=_optional_text(row.get("Unit")),
        ticker=_optional_text(row.get("Ticker")),
        indicator_symbol=_optional_text(row.get("Symbol")),
        primary_source_name=_optional_text(row.get("Source")),
        primary_source_url=_optional_text(row.get("SourceURL")),
        provider_url=provider_url,
        affected_currencies=(currency,),
        affected_symbols=symbols,
        source_key=snapshot.source_key,
        source_content_sha256=snapshot.content_sha256,
        raw_row_sha256=raw_hash,
    )


class TradingEconomicsCalendarAdapterV1:
    """Parse one exact Trading Economics JSON response."""

    adapter_name = TRADING_ECONOMICS_ADAPTER_NAME
    adapter_version = TRADING_ECONOMICS_ADAPTER_VERSION

    def __init__(self, snapshot: MarketContextSourceSnapshotV1) -> None:
        if snapshot.adapter_name != self.adapter_name:
            raise ValueError("calendar snapshot adapter identity differs")
        if snapshot.adapter_version != self.adapter_version:
            raise ValueError("calendar snapshot adapter version differs")
        self.snapshot = snapshot
        self.diagnostics: tuple[str, ...] = ()

    def load_events(self) -> tuple[EconomicCalendarEventV1, ...]:
        try:
            payload = json.loads(self.snapshot.content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "Trading Economics calendar response is not JSON"
            ) from exc
        rows = _sequence(payload, "Trading Economics response")
        expected_rows = _strict_int(
            self.snapshot.metadata.get("row_count"), "row_count"
        )
        if len(rows) != expected_rows:
            raise ValueError(
                "calendar response row count differs from evidence"
            )
        events = tuple(
            _event_from_row(_mapping(row, "calendar row"), self.snapshot)
            for row in rows
        )
        if len({item.provider_event_id for item in events}) != len(events):
            raise ValueError(
                "calendar response contains duplicate CalendarId rows"
            )
        return events


@dataclass(frozen=True, slots=True)
class EconomicCalendarCoverageV1:
    """One economy/currency coverage and field-missingness slice."""

    economy: str
    currency: str
    coverage_start_ns: int
    coverage_end_ns: int
    complete: bool
    event_count: int
    missing_actual_count: int
    missing_previous_count: int
    missing_forecast_count: int
    revised_previous_count: int
    affected_symbols: tuple[str, ...]
    limitation: str
    schema_version: str = ECONOMIC_CALENDAR_COVERAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_COVERAGE_SCHEMA_VERSION:
            raise ValueError("unsupported economic-calendar coverage schema")
        economy = _required_text(self.economy, "economy")
        if economy not in ECONOMY_CURRENCY:
            raise ValueError("calendar coverage economy is unsupported")
        currency = _required_text(self.currency, "currency").upper()
        if currency != ECONOMY_CURRENCY[economy]:
            raise ValueError("calendar coverage currency differs from economy")
        start = _bounded_int(
            self.coverage_start_ns, "coverage_start_ns", 0, 2**63 - 1
        )
        end = _bounded_int(
            self.coverage_end_ns, "coverage_end_ns", 1, 2**63 - 1
        )
        if end <= start:
            raise ValueError("calendar coverage end must follow start")
        if not isinstance(self.complete, bool):
            raise TypeError("calendar coverage complete must be boolean")
        for name in (
            "event_count",
            "missing_actual_count",
            "missing_previous_count",
            "missing_forecast_count",
            "revised_previous_count",
        ):
            _bounded_int(
                getattr(self, name), name, 0, MAX_ECONOMIC_CALENDAR_EVENTS
            )
        symbols = tuple(
            sorted(
                {
                    _required_text(item, "symbol").lower()
                    for item in self.affected_symbols
                }
            )
        )
        expected_symbols = histdata_economy_symbols()[economy]
        if symbols != expected_symbols:
            raise ValueError("coverage symbols differ from HistData mapping")
        object.__setattr__(self, "economy", economy)
        object.__setattr__(self, "currency", currency)
        object.__setattr__(self, "affected_symbols", symbols)
        object.__setattr__(
            self, "limitation", _required_text(self.limitation, "limitation")
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy": self.economy,
            "currency": self.currency,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "complete": self.complete,
            "event_count": self.event_count,
            "missing_actual_count": self.missing_actual_count,
            "missing_previous_count": self.missing_previous_count,
            "missing_forecast_count": self.missing_forecast_count,
            "revised_previous_count": self.revised_previous_count,
            "affected_symbols": list(self.affected_symbols),
            "limitation": self.limitation,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarCoverageV1:
        return cls(
            economy=str(data.get("economy", "")),
            currency=str(data.get("currency", "")),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            complete=bool(data.get("complete")),
            event_count=_strict_int(data.get("event_count"), "event_count"),
            missing_actual_count=_strict_int(
                data.get("missing_actual_count"), "missing_actual_count"
            ),
            missing_previous_count=_strict_int(
                data.get("missing_previous_count"), "missing_previous_count"
            ),
            missing_forecast_count=_strict_int(
                data.get("missing_forecast_count"), "missing_forecast_count"
            ),
            revised_previous_count=_strict_int(
                data.get("revised_previous_count"), "revised_previous_count"
            ),
            affected_symbols=_string_tuple(data.get("affected_symbols")),
            limitation=str(data.get("limitation", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _pair_coverage_payload(
    coverage: Sequence[EconomicCalendarCoverageV1],
    events: Sequence[EconomicCalendarEventV1],
) -> dict[str, JSONValue]:
    coverage_by_economy = {item.economy: item for item in coverage}
    result: dict[str, JSONValue] = {}
    for pair, economies in histdata_pair_economies().items():
        result[pair] = {
            "economies": list(economies),
            "currencies": sorted(
                {ECONOMY_CURRENCY[item] for item in economies}
            ),
            "complete": all(
                coverage_by_economy.get(item) is not None
                and coverage_by_economy[item].complete
                for item in economies
            ),
            "event_count": sum(
                pair in event.affected_symbols for event in events
            ),
        }
    return result


@dataclass(frozen=True, slots=True)
class EconomicCalendarCorpusV1:
    """Immutable comprehensive calendar records and auditable coverage."""

    profile: EconomicCalendarFetchProfileV1
    events: tuple[EconomicCalendarEventV1, ...]
    sources: tuple[EconomicCalendarSourceEvidenceV1, ...]
    coverage: tuple[EconomicCalendarCoverageV1, ...]
    pair_coverage: Mapping[str, JSONValue]
    counts_by_year_currency_category: Mapping[str, int]
    runtime_seconds: float
    peak_memory_bytes: int
    limitations: tuple[str, ...]
    corpus_id: str = ""
    schema_version: str = ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported economic-calendar corpus schema")
        if not isinstance(self.profile, EconomicCalendarFetchProfileV1):
            raise TypeError("calendar corpus requires a v1 profile")
        events = tuple(
            sorted(
                self.events,
                key=lambda item: (
                    item.release_time_ns,
                    item.provider_event_id,
                    item.revision_sequence,
                ),
            )
        )
        sources = tuple(sorted(self.sources, key=lambda item: item.source_key))
        coverage = tuple(sorted(self.coverage, key=lambda item: item.economy))
        if not events or len(events) > self.profile.max_events:
            raise ValueError(
                "calendar corpus event count is empty or unbounded"
            )
        if not sources or len(sources) > MAX_ECONOMIC_CALENDAR_SOURCES:
            raise ValueError(
                "calendar corpus source count is empty or unbounded"
            )
        if len({item.source_key for item in sources}) != len(sources):
            raise ValueError("calendar source keys are not unique")
        source_by_key = {item.source_key: item for item in sources}
        by_provider: dict[str, list[EconomicCalendarEventV1]] = {}
        for event in events:
            source = source_by_key.get(event.source_key)
            if (
                source is None
                or source.content_sha256 != event.source_content_sha256
            ):
                raise ValueError(
                    "calendar event provenance differs from evidence"
                )
            by_provider.setdefault(event.provider_event_id, []).append(event)
        for provider_id, revisions in by_provider.items():
            ordered = sorted(revisions, key=lambda item: item.revision_sequence)
            if [item.revision_sequence for item in ordered] != list(
                range(len(ordered))
            ):
                raise ValueError(
                    f"calendar revisions are not contiguous: {provider_id}"
                )
            for previous, current in pairwise(ordered):
                if current.supersedes_event_id != previous.event_id:
                    raise ValueError("calendar revision predecessor differs")
                if current.available_at_ns <= previous.available_at_ns:
                    raise ValueError(
                        "calendar revision availability does not advance"
                    )
        expected_economies = set(self.profile.economies)
        if {item.economy for item in coverage} != expected_economies:
            raise ValueError("calendar coverage differs from profile economies")
        expected_pair_coverage = _pair_coverage_payload(coverage, events)
        if dict(self.pair_coverage) != expected_pair_coverage:
            raise ValueError(
                "calendar pair coverage differs from catalog mapping"
            )
        expected_counts = _counts_by_year_currency_category(events)
        if dict(self.counts_by_year_currency_category) != expected_counts:
            raise ValueError("calendar category counts differ from events")
        runtime = _finite_float(self.runtime_seconds, "runtime_seconds")
        if runtime < 0:
            raise ValueError("runtime_seconds cannot be negative")
        _bounded_int(self.peak_memory_bytes, "peak_memory_bytes", 0, 2**63 - 1)
        limitations = tuple(
            _required_text(item, "limitation") for item in self.limitations
        )
        if not limitations:
            raise ValueError("calendar corpus requires limitations")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "pair_coverage", expected_pair_coverage)
        object.__setattr__(
            self, "counts_by_year_currency_category", expected_counts
        )
        object.__setattr__(self, "runtime_seconds", runtime)
        object.__setattr__(self, "limitations", limitations)
        expected_id = _stable_id(
            "economic-calendar-corpus", self.identity_payload()
        )
        supplied = _optional_text(self.corpus_id)
        if supplied is not None and supplied != expected_id:
            raise ValueError("economic-calendar corpus_id is not deterministic")
        object.__setattr__(self, "corpus_id", expected_id)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "profile": self.profile.to_dict(),
            "events": [item.to_dict() for item in self.events],
            "sources": [item.to_dict() for item in self.sources],
            "coverage": [item.to_dict() for item in self.coverage],
            "pair_coverage": dict(self.pair_coverage),
            "counts_by_year_currency_category": dict(
                self.counts_by_year_currency_category
            ),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "event_count": len(self.events),
            "source_count": len(self.sources),
            "source_bytes": sum(item.size_bytes for item in self.sources),
            "runtime_seconds": self.runtime_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "corpus_id": self.corpus_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarCorpusV1:
        return cls(
            profile=EconomicCalendarFetchProfileV1.from_dict(
                _mapping(data.get("profile"), "profile")
            ),
            events=tuple(
                EconomicCalendarEventV1.from_dict(_mapping(item, "event"))
                for item in _sequence(data.get("events"), "events")
            ),
            sources=tuple(
                EconomicCalendarSourceEvidenceV1.from_dict(
                    _mapping(item, "source")
                )
                for item in _sequence(data.get("sources"), "sources")
            ),
            coverage=tuple(
                EconomicCalendarCoverageV1.from_dict(_mapping(item, "coverage"))
                for item in _sequence(data.get("coverage"), "coverage")
            ),
            pair_coverage=_mapping(data.get("pair_coverage"), "pair_coverage"),
            counts_by_year_currency_category={
                str(key): _strict_int(value, f"count {key}")
                for key, value in _mapping(
                    data.get("counts_by_year_currency_category"), "counts"
                ).items()
            },
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


@dataclass(frozen=True, slots=True)
class EconomicCalendarCorpusBuildV1:
    """Corpus plus exact restricted raw snapshots for immutable writing."""

    corpus: EconomicCalendarCorpusV1
    snapshots: tuple[MarketContextSourceSnapshotV1, ...]


def _counts_by_year_currency_category(
    events: Sequence[EconomicCalendarEventV1],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for event in events:
        year = datetime.fromtimestamp(
            event.release_time_ns / 1_000_000_000, tz=timezone.utc
        ).year
        category = re.sub(r"\s+", " ", event.category.strip().lower())
        counts[f"{year}|{event.currency}|{category}"] += 1
    return dict(sorted(counts.items()))


def _snapshot(
    *,
    content: bytes,
    economy: str,
    start: date,
    end: date,
    retrieved_at_ns: int,
    content_type: str,
    row_count: int,
    complete_leaf: bool,
) -> MarketContextSourceSnapshotV1:
    slug = re.sub(r"[^a-z0-9]+", "-", economy.lower()).strip("-")
    digest = hashlib.sha256(content).hexdigest()[:32]
    source_key = (
        f"trading-economics.{slug}.{start.isoformat()}.{end.isoformat()}."
        f"{retrieved_at_ns}.{digest}"
    )
    return MarketContextSourceSnapshotV1(
        source_key=source_key,
        source_name="Trading Economics economic calendar",
        source_uri=trading_economics_request_uri(economy, start, end),
        retrieved_at_ns=retrieved_at_ns,
        content=content,
        content_type=content_type,
        adapter_name=TRADING_ECONOMICS_ADAPTER_NAME,
        adapter_version=TRADING_ECONOMICS_ADAPTER_VERSION,
        license_name=TRADING_ECONOMICS_LICENSE_NAME,
        redistribution_allowed=False,
        redistribution_constraints=(
            "Local analysis only unless the operator's subscription grants redistribution.",
            "Never commit or publish raw Trading Economics responses by default.",
        ),
        limitations=(
            "Historical rows preserve the provider's stored release state but do not expose when a schedule or consensus forecast was first published.",
            "The Revised field is the prior release's pre-revision value; later refreshes are accumulated as separate event vintages.",
            "Coverage and redistribution remain subject to the operator's active Trading Economics subscription.",
        ),
        metadata={
            "provider": "Trading Economics",
            "economy": economy,
            "currency": ECONOMY_CURRENCY[economy],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "row_count": row_count,
            "complete_leaf": complete_leaf,
            "terms_uri": TRADING_ECONOMICS_TERMS_URI,
            "pricing_uri": TRADING_ECONOMICS_PRICING_URI,
            "schema_uri": TRADING_ECONOMICS_SCHEMA_URI,
            "rate_limit_uri": TRADING_ECONOMICS_RATE_LIMIT_URI,
        },
    )


class _FetchBudget:
    def __init__(self, profile: EconomicCalendarFetchProfileV1) -> None:
        self.profile = profile
        self.started = time.monotonic()
        self.bytes = 0
        self.requests = 0
        self.last_request_started: float | None = None

    def before_request(self) -> None:
        if self.requests >= self.profile.max_requests:
            raise ValueError("economic-calendar request budget exhausted")
        elapsed = time.monotonic() - self.started
        if elapsed > self.profile.max_runtime_seconds:
            raise ValueError("economic-calendar runtime budget exhausted")
        if self.last_request_started is not None:
            remaining = self.profile.min_request_interval_seconds - (
                time.monotonic() - self.last_request_started
            )
            if remaining > 0:
                time.sleep(remaining)
        self.last_request_started = time.monotonic()
        self.requests += 1

    def consume(self, size: int) -> None:
        self.bytes += size
        if self.bytes > self.profile.max_total_source_bytes:
            raise ValueError("economic-calendar total byte budget exhausted")


def _fetch_response(
    session: requests.Session,
    uri: str,
    api_key: str,
    profile: EconomicCalendarFetchProfileV1,
    budget: _FetchBudget,
) -> tuple[bytes, str, int]:
    budget.before_request()
    try:
        response = session.get(
            uri,
            headers={
                "Authorization": api_key,
                "Accept": "application/json",
                "User-Agent": (
                    "histdatacom-economic-calendar/2.4.0 "
                    "(+https://github.com/dmidlo/histdata.com-tools)"
                ),
            },
            timeout=profile.timeout_seconds,
            stream=True,
        )
        try:
            response.raise_for_status()
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > profile.max_response_bytes:
                raise ValueError(
                    "calendar response exceeds declared byte limit"
                )
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                size += len(chunk)
                if size > profile.max_response_bytes:
                    raise ValueError("calendar response exceeds byte limit")
                chunks.append(chunk)
        finally:
            response.close()
    except requests.RequestException as exc:
        raise ValueError(
            f"Trading Economics calendar request failed: {uri}"
        ) from exc
    content = b"".join(chunks)
    if not content:
        raise ValueError("Trading Economics returned an empty response")
    budget.consume(len(content))
    return (
        content,
        str(response.headers.get("Content-Type") or "application/json").split(
            ";", 1
        )[0],
        time.time_ns(),
    )


def _fetch_range(
    *,
    session: requests.Session,
    api_key: str,
    profile: EconomicCalendarFetchProfileV1,
    budget: _FetchBudget,
    economy: str,
    start: date,
    end: date,
) -> list[MarketContextSourceSnapshotV1]:
    uri = trading_economics_request_uri(economy, start, end)
    content, content_type, retrieved = _fetch_response(
        session, uri, api_key, profile, budget
    )
    try:
        rows = _sequence(
            json.loads(content.decode("utf-8")), "calendar response"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Trading Economics returned invalid JSON") from exc
    if len(rows) > MAX_PROVIDER_ROWS_PER_REQUEST:
        raise ValueError("calendar response exceeds documented row limit")
    limit_hit = len(rows) == MAX_PROVIDER_ROWS_PER_REQUEST
    current = _snapshot(
        content=content,
        economy=economy,
        start=start,
        end=end,
        retrieved_at_ns=retrieved,
        content_type=content_type,
        row_count=len(rows),
        complete_leaf=not limit_hit,
    )
    if not limit_hit:
        return [current]
    if start == end:
        raise ValueError(
            f"calendar row ceiling reached for one day: {economy} {start}"
        )
    span_days = (end - start).days
    left_end = start + timedelta(days=span_days // 2)
    right_start = left_end + timedelta(days=1)
    return [
        current,
        *_fetch_range(
            session=session,
            api_key=api_key,
            profile=profile,
            budget=budget,
            economy=economy,
            start=start,
            end=left_end,
        ),
        *_fetch_range(
            session=session,
            api_key=api_key,
            profile=profile,
            budget=budget,
            economy=economy,
            start=right_start,
            end=end,
        ),
    ]


def fetch_trading_economics_calendar_snapshots(
    profile: EconomicCalendarFetchProfileV1,
    *,
    api_key: str,
    license_acknowledged: bool,
    session: requests.Session | None = None,
) -> tuple[MarketContextSourceSnapshotV1, ...]:
    """Fetch bounded source snapshots without persisting the API credential."""
    if not license_acknowledged:
        raise ValueError(
            "live calendar acquisition requires explicit provider-license acknowledgement"
        )
    secret = _required_text(api_key, "Trading Economics API key")
    owns_session = session is None
    selected_session = session or requests.Session()
    budget = _FetchBudget(profile)
    windows = _date_windows(
        _parse_date(profile.start_date, "start_date"),
        _parse_date(profile.end_date, "end_date"),
        maximum_days=profile.initial_window_days,
    )
    snapshots: list[MarketContextSourceSnapshotV1] = []
    try:
        for economy in profile.economies:
            for start, end in windows:
                snapshots.extend(
                    _fetch_range(
                        session=selected_session,
                        api_key=secret,
                        profile=profile,
                        budget=budget,
                        economy=economy,
                        start=start,
                        end=end,
                    )
                )
    finally:
        if owns_session:
            selected_session.close()
    return tuple(snapshots)


def _coverage_complete(
    profile: EconomicCalendarFetchProfileV1,
    evidence: Sequence[EconomicCalendarSourceEvidenceV1],
    economy: str,
) -> bool:
    intervals = sorted(
        (
            _parse_date(item.start_date, "start_date"),
            _parse_date(item.end_date, "end_date"),
        )
        for item in evidence
        if item.economy == economy and item.complete_leaf
    )
    cursor = _parse_date(profile.start_date, "start_date")
    target = _parse_date(profile.end_date, "end_date")
    for start, end in intervals:
        if end < cursor:
            continue
        if start > cursor:
            return False
        cursor = max(cursor, end + timedelta(days=1))
        if cursor > target:
            return True
    return cursor > target


def _merge_event(
    current: EconomicCalendarEventV1,
    revisions: list[EconomicCalendarEventV1],
) -> None:
    if not revisions:
        revisions.append(current)
        return
    latest = revisions[-1]
    if current.semantic_payload() == latest.semantic_payload():
        return
    available = max(
        current.available_at_ns,
        current.observed_at_ns,
        latest.available_at_ns + 1,
    )
    revisions.append(
        replace(
            current,
            first_known_at_ns=latest.first_known_at_ns,
            available_at_ns=available,
            revision_sequence=latest.revision_sequence + 1,
            supersedes_event_id=latest.event_id,
            event_id="",
        )
    )


def build_economic_calendar_corpus_from_snapshots(
    snapshots: Sequence[MarketContextSourceSnapshotV1],
    *,
    profile: EconomicCalendarFetchProfileV1,
    previous_corpus: EconomicCalendarCorpusV1 | None = None,
    previous_snapshots: Sequence[MarketContextSourceSnapshotV1] = (),
    runtime_seconds: float = 0.0,
) -> EconomicCalendarCorpusBuildV1:
    """Build an immutable corpus, accumulating provider corrections as vintages."""
    values = tuple(
        sorted(
            snapshots,
            key=lambda item: (item.retrieved_at_ns, item.source_key),
        )
    )
    if not values:
        raise ValueError("economic-calendar source snapshots are empty")
    if len(values) > MAX_ECONOMIC_CALENDAR_SOURCES:
        raise ValueError("economic-calendar source snapshots are unbounded")
    prior_values = tuple(
        sorted(
            previous_snapshots,
            key=lambda item: (item.retrieved_at_ns, item.source_key),
        )
    )
    if previous_corpus is None and prior_values:
        raise ValueError("previous snapshots require a previous corpus")
    if previous_corpus is not None:
        if previous_corpus.profile != profile:
            raise ValueError("previous economic-calendar profile differs")
        if not prior_values:
            raise ValueError(
                "previous economic-calendar corpus requires its replay snapshots"
            )
        prior_evidence = tuple(
            EconomicCalendarSourceEvidenceV1.from_snapshot(item)
            for item in prior_values
        )
        if {item.source_key: item for item in prior_evidence} != {
            item.source_key: item for item in previous_corpus.sources
        }:
            raise ValueError(
                "previous calendar snapshots differ from corpus evidence"
            )
    evidence = tuple(
        EconomicCalendarSourceEvidenceV1.from_snapshot(item) for item in values
    )
    by_provider: dict[str, list[EconomicCalendarEventV1]] = {}
    if previous_corpus is not None:
        for event in previous_corpus.events:
            by_provider.setdefault(event.provider_event_id, []).append(event)
    for snapshot in values:
        if not bool(snapshot.metadata.get("complete_leaf")):
            continue
        adapter = TradingEconomicsCalendarAdapterV1(snapshot)
        for event in adapter.load_events():
            _merge_event(
                event, by_provider.setdefault(event.provider_event_id, [])
            )
    events = tuple(
        event
        for provider_id in sorted(by_provider)
        for event in sorted(
            by_provider[provider_id], key=lambda item: item.revision_sequence
        )
    )
    if len(events) > profile.max_events:
        raise ValueError("economic-calendar event budget exhausted")
    combined_evidence: dict[str, EconomicCalendarSourceEvidenceV1] = {}
    if previous_corpus is not None:
        combined_evidence.update(
            {item.source_key: item for item in previous_corpus.sources}
        )
    combined_evidence.update({item.source_key: item for item in evidence})
    all_evidence = tuple(combined_evidence.values())
    coverage: list[EconomicCalendarCoverageV1] = []
    latest_events = [
        revisions[-1] for revisions in by_provider.values() if revisions
    ]
    economy_symbols = histdata_economy_symbols()
    for economy in profile.economies:
        economy_events = [
            item for item in latest_events if item.economy == economy
        ]
        coverage.append(
            EconomicCalendarCoverageV1(
                economy=economy,
                currency=ECONOMY_CURRENCY[economy],
                coverage_start_ns=profile.coverage_start_ns,
                coverage_end_ns=profile.coverage_end_ns,
                complete=_coverage_complete(profile, all_evidence, economy),
                event_count=len(economy_events),
                missing_actual_count=sum(
                    item.actual_text is None for item in economy_events
                ),
                missing_previous_count=sum(
                    item.previous_text is None for item in economy_events
                ),
                missing_forecast_count=sum(
                    item.forecast_text is None for item in economy_events
                ),
                revised_previous_count=sum(
                    item.revised_text is not None for item in economy_events
                ),
                affected_symbols=economy_symbols[economy],
                limitation=(
                    "Complete means every requested date interval returned below the provider's documented row ceiling; it does not assert every event field is populated."
                ),
            )
        )
    corpus = EconomicCalendarCorpusV1(
        profile=profile,
        events=events,
        sources=all_evidence,
        coverage=tuple(coverage),
        pair_coverage=_pair_coverage_payload(coverage, events),
        counts_by_year_currency_category=_counts_by_year_currency_category(
            events
        ),
        runtime_seconds=runtime_seconds,
        peak_memory_bytes=peak_rss_bytes(),
        limitations=(
            "Trading Economics data is restricted to operator-licensed use; redistribution is disabled by contract.",
            "Historical archive rows do not prove when schedules or consensus forecasts were first published, so backfilled forecasts become ex-ante eligible no earlier than release time.",
            "The provider Revised field preserves the prior release's pre-revision value; refresh-time changes to a CalendarId are retained as explicit corpus vintages.",
            "Field-level absence remains missing and is never inferred.",
        ),
    )
    return EconomicCalendarCorpusBuildV1(
        corpus=corpus,
        snapshots=(*prior_values, *values),
    )


def build_live_economic_calendar_corpus(
    profile: EconomicCalendarFetchProfileV1,
    *,
    api_key: str,
    license_acknowledged: bool,
    previous_corpus: EconomicCalendarCorpusV1 | None = None,
    previous_snapshots: Sequence[MarketContextSourceSnapshotV1] = (),
    session: requests.Session | None = None,
) -> EconomicCalendarCorpusBuildV1:
    started = time.monotonic()
    snapshots = fetch_trading_economics_calendar_snapshots(
        profile,
        api_key=api_key,
        license_acknowledged=license_acknowledged,
        session=session,
    )
    return build_economic_calendar_corpus_from_snapshots(
        snapshots,
        profile=profile,
        previous_corpus=previous_corpus,
        previous_snapshots=previous_snapshots,
        runtime_seconds=time.monotonic() - started,
    )


def _artifact_ref(path: Path, kind: str) -> ArtifactRef:
    content = path.read_bytes()
    return ArtifactRef(
        kind=kind,
        path=str(path),
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
    )


def _write_immutable(path: Path, content: bytes, *, restricted: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(f"refusing to replace immutable artifact: {path}")
        return
    path.write_bytes(content)
    if restricted:
        path.chmod(0o600)


def write_economic_calendar_corpus(
    build: EconomicCalendarCorpusBuildV1,
    artifact_dir: str | Path,
) -> dict[str, ArtifactRef]:
    """Write restricted raw snapshots and a content-addressed corpus."""
    root = Path(artifact_dir).expanduser().resolve()
    source_dir = root / "restricted-sources"
    artifacts: dict[str, ArtifactRef] = {}
    evidence_by_key = {item.source_key: item for item in build.corpus.sources}
    snapshots_by_key = {item.source_key: item for item in build.snapshots}
    if len(snapshots_by_key) != len(build.snapshots):
        raise ValueError("calendar build contains duplicate source snapshots")
    if set(snapshots_by_key) != set(evidence_by_key):
        raise ValueError(
            "calendar build is missing restricted replay snapshots"
        )
    for snapshot in build.snapshots:
        evidence = evidence_by_key[snapshot.source_key]
        suffix = ".json"
        path = (
            source_dir
            / f"{snapshot.source_key}-{snapshot.content_sha256}{suffix}"
        )
        _write_immutable(path, snapshot.content, restricted=True)
        artifacts[f"source:{snapshot.source_key}"] = _artifact_ref(
            path, "economic_calendar_restricted_source_v1"
        )
        if (
            evidence.content_sha256
            != artifacts[f"source:{snapshot.source_key}"].sha256
        ):
            raise ValueError(
                "written calendar source hash differs from evidence"
            )
    corpus_bytes = (
        json.dumps(build.corpus.to_dict(), indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(corpus_bytes).hexdigest()
    corpus_path = root / f"economic-calendar-corpus-{digest}.json"
    _write_immutable(corpus_path, corpus_bytes, restricted=True)
    artifacts["corpus"] = _artifact_ref(
        corpus_path, "economic_calendar_corpus_v1"
    )
    return artifacts


def read_economic_calendar_corpus(
    path: str | Path,
) -> EconomicCalendarCorpusV1:
    source = Path(path).expanduser().resolve()
    match = re.fullmatch(
        r"economic-calendar-corpus-([0-9a-f]{64})\.json", source.name
    )
    if match is None:
        raise ValueError(
            "economic-calendar corpus filename is not content addressed"
        )
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError(
            "economic-calendar corpus file hash differs from filename"
        )
    try:
        payload = _mapping(json.loads(content.decode("utf-8")), "corpus")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("economic-calendar corpus is not valid JSON") from exc
    return EconomicCalendarCorpusV1.from_dict(payload)


def replay_economic_calendar_corpus(
    corpus_path: str | Path,
) -> EconomicCalendarCorpusBuildV1:
    """Replay every restricted raw snapshot and require the logical corpus ID."""
    source = Path(corpus_path).expanduser().resolve()
    corpus = read_economic_calendar_corpus(source)
    root = source.parent
    snapshots: list[MarketContextSourceSnapshotV1] = []
    for evidence in corpus.sources:
        matches = tuple(
            (root / "restricted-sources").glob(
                f"{evidence.source_key}-{evidence.content_sha256}.*"
            )
        )
        if len(matches) != 1:
            raise ValueError(
                f"calendar replay source is missing or ambiguous: {evidence.source_key}"
            )
        snapshots.append(evidence.restore_snapshot(matches[0].read_bytes()))
    rebuilt = build_economic_calendar_corpus_from_snapshots(
        snapshots,
        profile=corpus.profile,
        runtime_seconds=0.0,
    )
    if rebuilt.corpus.corpus_id != corpus.corpus_id:
        raise ValueError(
            "economic-calendar replay differs from corpus identity"
        )
    return rebuilt


def _latest_visible_events(
    corpus: EconomicCalendarCorpusV1,
    *,
    view: MarketContextView,
    as_of_ns: int | None,
) -> tuple[EconomicCalendarEventV1, ...]:
    grouped: dict[str, list[EconomicCalendarEventV1]] = {}
    for event in corpus.events:
        grouped.setdefault(event.provider_event_id, []).append(event)
    if view is MarketContextView.EX_ANTE and as_of_ns is None:
        raise ValueError("ex-ante calendar query requires as_of_ns")
    selected: list[EconomicCalendarEventV1] = []
    for revisions in grouped.values():
        ordered = sorted(revisions, key=lambda item: item.revision_sequence)
        if view is MarketContextView.EX_POST:
            selected.append(ordered[-1])
            continue
        visible = [
            item
            for item in ordered
            if item.first_known_at_ns <= cast(int, as_of_ns)
            and item.available_at_ns <= cast(int, as_of_ns)
        ]
        # Retain the first hidden record when nothing was yet visible so the
        # shared query contract reports ``not_available_as_of`` rather than
        # incorrectly claiming that no matching event exists.
        selected.append(visible[-1] if visible else ordered[0])
    return tuple(selected)


def _project_market_event(
    event: EconomicCalendarEventV1,
    source: EconomicCalendarSourceEvidenceV1,
    profile: EconomicCalendarFetchProfileV1,
) -> MarketContextEventV1:
    previous_value = event.previous_value
    revised_previous = None
    if event.revised_value is not None:
        previous_value = event.revised_value
        revised_previous = event.previous_value
    tags = (
        f"economy:{event.economy.lower().replace(' ', '-')}",
        f"importance:{event.importance}",
        "licensed-provider:trading-economics",
        f"calendar-id:{event.provider_event_id}",
    )
    return MarketContextEventV1(
        canonical_key=(
            "trading-economics."
            + re.sub(r"[^a-z0-9]+", "-", event.provider_event_id.lower()).strip(
                "-"
            )
            + "."
            + event.event_id.rsplit(":", maxsplit=1)[-1][:16]
        ),
        kind=_event_kind(event.category, event.title),
        title=event.title,
        source=source.source_contract(),
        source_event_time=_utc_text(event.release_time_ns),
        source_timezone="UTC",
        event_time_ns=event.release_time_ns,
        first_known_at_ns=event.first_known_at_ns,
        available_at_ns=event.available_at_ns,
        pre_event_ns=profile.pre_event_ns,
        post_event_ns=profile.post_event_ns,
        affected_currencies=event.affected_currencies,
        affected_symbols=event.affected_symbols,
        confidence=1.0 if not event.timing_estimated else 0.75,
        precision=(
            MarketContextPrecision.EXACT
            if not event.timing_estimated
            else MarketContextPrecision.APPROXIMATE
        ),
        limitations=(
            "Projected from a restricted Trading Economics calendar record; consult the corpus event ID for complete lexical fields.",
            "Historical schedule/forecast first-known timestamps are unavailable and use conservative release-time eligibility.",
        ),
        vintage_id=event.event_id,
        ambiguity_reason=(
            None
            if not event.timing_estimated
            else "Trading Economics DateSpan marks the release time as estimated."
        ),
        expected_value=event.forecast_value,
        actual_value=event.actual_value,
        previous_value=previous_value,
        revised_previous_value=revised_previous,
        value_unit=event.unit,
        content_sha256=event.raw_row_sha256,
        tags=tags,
    )


def query_economic_calendar_corpus(
    corpus: EconomicCalendarCorpusV1,
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
) -> MarketContextQueryV1:
    """Query the scalable corpus and project only bounded matching records."""
    selected_view = MarketContextView.from_value(view)
    visible = _latest_visible_events(
        corpus, view=selected_view, as_of_ns=as_of_ns
    )
    candidate_start = start_ns - corpus.profile.post_event_ns
    candidate_end = end_ns + corpus.profile.pre_event_ns
    candidates = tuple(
        event
        for event in visible
        if candidate_start <= event.release_time_ns < candidate_end
    )
    if len(candidates) > MAX_QUERY_CANDIDATES:
        raise ValueError("economic-calendar query candidate bound exceeded")
    sources = {item.source_key: item for item in corpus.sources}
    projected = tuple(
        _project_market_event(event, sources[event.source_key], corpus.profile)
        for event in candidates
    )
    timeline = MarketContextTimelineV1(
        timeline_version="economic-calendar-projection-v1",
        coverage_start_ns=corpus.profile.coverage_start_ns,
        coverage_end_ns=corpus.profile.coverage_end_ns,
        complete=all(item.complete for item in corpus.coverage),
        events=projected,
        limitations=corpus.limitations,
    )
    return query_market_context(
        timeline,
        start_ns=start_ns,
        end_ns=end_ns,
        view=selected_view,
        as_of_ns=as_of_ns,
        currencies=currencies,
        symbols=symbols,
        kinds=kinds,
        include_calendar=include_calendar,
        max_events=max_events,
        window_id=window_id,
    )


MarketContextCorpusLike: TypeAlias = (
    MarketContextCorpusV1 | EconomicCalendarCorpusV1
)


def context_corpus_artifact_kind(corpus: MarketContextCorpusLike) -> str:
    """Return the versioned artifact kind for either supported corpus."""
    if isinstance(corpus, EconomicCalendarCorpusV1):
        return "economic_calendar_corpus_v1"
    if isinstance(corpus, MarketContextCorpusV1):
        return "market_context_corpus_v1"
    raise TypeError("unsupported market-context corpus type")


def context_corpus_event_times(
    corpus: MarketContextCorpusLike,
) -> tuple[int, ...]:
    """Return latest logical event times without materializing a v1 timeline."""
    if isinstance(corpus, MarketContextCorpusV1):
        return tuple(item.event_time_ns for item in corpus.timeline.events)
    latest = _latest_visible_events(
        corpus, view=MarketContextView.EX_POST, as_of_ns=None
    )
    return tuple(sorted(item.release_time_ns for item in latest))


def read_context_corpus(path: str | Path) -> MarketContextCorpusLike:
    """Load either the public official-source v1 or restricted calendar corpus."""
    source = Path(path).expanduser().resolve()
    if source.name.startswith("economic-calendar-corpus-"):
        return read_economic_calendar_corpus(source)
    return read_market_context_corpus(source)


def preflight_context_corpus(
    corpus: MarketContextCorpusLike,
    *,
    start_ns: int,
    end_ns: int,
    currencies: Sequence[str] = (),
    symbols: Sequence[str] = (),
    kinds: Sequence[MarketContextKind] = (),
) -> MarketContextCorpusPreflightV1:
    """Return a common support decision for either corpus generation."""
    if isinstance(corpus, MarketContextCorpusV1):
        return preflight_market_context_corpus(
            corpus,
            start_ns=start_ns,
            end_ns=end_ns,
            currencies=currencies,
            kinds=kinds,
        )
    if end_ns <= start_ns:
        raise ValueError("market-context preflight end must follow start")
    requested_currencies = tuple(
        sorted(
            {_required_text(item, "currency").upper() for item in currencies}
        )
    )
    requested_kinds = tuple(
        sorted(
            {MarketContextKind.from_value(item) for item in kinds},
            key=lambda item: item.value,
        )
    )
    requested_symbols = tuple(
        sorted({_required_text(item, "symbol").lower() for item in symbols})
    )
    reasons: list[str] = []
    matched: list[str] = []
    if (
        start_ns < corpus.profile.coverage_start_ns
        or end_ns > corpus.profile.coverage_end_ns
    ):
        reasons.append("requested interval lies outside corpus coverage")
    by_currency: dict[str, list[EconomicCalendarCoverageV1]] = {}
    for item in corpus.coverage:
        by_currency.setdefault(item.currency, []).append(item)
    for currency in requested_currencies:
        slices = by_currency.get(currency, [])
        if slices and all(item.complete for item in slices):
            matched.append(f"trading-economics:{currency}:calendar")
        else:
            reasons.append(
                f"unsupported comprehensive calendar coverage for {currency}"
            )
    for symbol in requested_symbols:
        pair = corpus.pair_coverage.get(symbol)
        if not isinstance(pair, Mapping) or not bool(pair.get("complete")):
            reasons.append(
                f"unsupported comprehensive calendar coverage for {symbol}"
            )
        else:
            matched.append(f"trading-economics:{symbol}:calendar")
    return MarketContextCorpusPreflightV1(
        corpus_id=corpus.corpus_id,
        start_ns=start_ns,
        end_ns=end_ns,
        currencies=requested_currencies,
        kinds=requested_kinds,
        ready=not reasons,
        reasons=tuple(dict.fromkeys(reasons)),
        matched_coverage=tuple(sorted(set(matched))),
    )


def query_context_corpus(
    corpus: MarketContextCorpusLike,
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
    """Dispatch a bounded query across v1 and comprehensive corpora."""
    if isinstance(corpus, MarketContextCorpusV1):
        return query_market_context_corpus(
            corpus,
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
            require_supported=require_supported,
        )
    if require_supported:
        decision = preflight_context_corpus(
            corpus,
            start_ns=start_ns,
            end_ns=end_ns,
            currencies=currencies,
            symbols=symbols,
            kinds=kinds,
        )
        if not decision.ready:
            raise MarketContextCorpusPreflightError(decision)
    return query_economic_calendar_corpus(
        corpus,
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


__all__ = [
    "CURRENCY_ECONOMY",
    "DEFAULT_ECONOMIC_CALENDAR_ECONOMIES",
    "ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_COVERAGE_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_EVENT_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_FETCH_PLAN_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_PROFILE_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_SOURCE_SCHEMA_VERSION",
    "ECONOMY_CURRENCY",
    "INSTRUMENT_ECONOMY",
    "TRADING_ECONOMICS_ADAPTER_NAME",
    "EconomicCalendarCorpusBuildV1",
    "EconomicCalendarCorpusV1",
    "EconomicCalendarCoverageV1",
    "EconomicCalendarEventV1",
    "EconomicCalendarFetchProfileV1",
    "EconomicCalendarSourceEvidenceV1",
    "MarketContextCorpusLike",
    "TradingEconomicsCalendarAdapterV1",
    "build_economic_calendar_corpus_from_snapshots",
    "build_live_economic_calendar_corpus",
    "context_corpus_artifact_kind",
    "context_corpus_event_times",
    "economic_calendar_fetch_plan",
    "fetch_trading_economics_calendar_snapshots",
    "histdata_economy_symbols",
    "histdata_pair_economies",
    "parse_calendar_number",
    "preflight_context_corpus",
    "query_context_corpus",
    "query_economic_calendar_corpus",
    "read_context_corpus",
    "read_economic_calendar_corpus",
    "replay_economic_calendar_corpus",
    "trading_economics_request_uri",
    "validate_api_key_env_name",
    "write_economic_calendar_corpus",
]

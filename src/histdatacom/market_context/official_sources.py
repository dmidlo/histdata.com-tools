"""Authoritative economic-source registry and bounded acquisition contracts.

The module deliberately stops before protocol-specific parsing.  It binds each
request to a reviewed legal producer, retains the exact response bytes, and
exposes a typed parser seam so SDMX, JSON-stat, spreadsheet, feed, HTML, and PDF
adapters can be implemented once without weakening source identity or replay.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, TypeVar, cast, runtime_checkable
from urllib.parse import (
    parse_qsl,
    quote,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)

import requests

from histdatacom.market_context.contracts import (
    canonical_contract_json,
    normalize_market_context_datetime,
)
from histdatacom.market_context.economic_calendar import (
    EconomicEventFamily,
    EconomicTimePrecision,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION = "histdatacom.official-source-entry.v1"
OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION = (
    "histdatacom.official-source-registry.v1"
)
OFFICIAL_FETCH_POLICY_SCHEMA_VERSION = "histdatacom.official-fetch-policy.v1"
OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION = (
    "histdatacom.official-source-request.v1"
)
OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.official-request-manifest.v1"
)
OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION = "histdatacom.official-raw-snapshot.v1"
OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION = "histdatacom.official-fetch-bundle.v1"

MAX_OFFICIAL_SOURCES = 256
MAX_OFFICIAL_REQUESTS = 512
MAX_OFFICIAL_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_OFFICIAL_TOTAL_BYTES = 512 * 1024 * 1024
MAX_OFFICIAL_EVENTS = 250_000
MAX_OFFICIAL_TEXT = 4096
MAX_OFFICIAL_REQUEST_BODY_BYTES = 1024 * 1024
MAX_OFFICIAL_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_OFFICIAL_HEADERS = 64
MAX_OFFICIAL_QUERY_PARAMETERS = 128
OFFICIAL_CREDENTIAL_PATH_PLACEHOLDER = "{credential}"

SCOPED_ECONOMIES: tuple[str, ...] = (
    "AU",
    "CA",
    "CH",
    "CZ",
    "DE",
    "DK",
    "EA",
    "FR",
    "GB",
    "HK",
    "HU",
    "JP",
    "MX",
    "NO",
    "NZ",
    "PL",
    "SE",
    "SG",
    "TR",
    "US",
    "ZA",
)

_CANONICAL_INDICATOR_ID_BY_FAMILY: Mapping[EconomicEventFamily, str] = {
    EconomicEventFamily.MONETARY_POLICY: "policy-rate-and-decisions",
    EconomicEventFamily.INFLATION_PRICES: "consumer-and-producer-prices",
    EconomicEventFamily.LABOUR_MARKET: "employment-unemployment-and-earnings",
    EconomicEventFamily.GDP_NATIONAL_ACCOUNTS: "gdp-and-national-accounts",
    EconomicEventFamily.RETAIL_CONSUMPTION: "retail-sales-and-household-consumption",
    EconomicEventFamily.INDUSTRIAL_PRODUCTION: "industrial-production",
    EconomicEventFamily.TRADE_EXTERNAL: "trade-and-external-accounts",
    EconomicEventFamily.HOUSING: "housing-and-construction",
    EconomicEventFamily.CONFIDENCE_SURVEY: "official-confidence-and-tendency-surveys",
    EconomicEventFamily.MONEY_CREDIT: "money-credit-and-financial-statistics",
    EconomicEventFamily.FISCAL: "government-finance",
    EconomicEventFamily.OTHER_OFFICIAL: "other-official-macro",
}

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_ENV_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_HEADERS = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
}
_RESERVED_REQUEST_HEADERS = {
    "accept",
    "accept-encoding",
    "content-type",
    "if-modified-since",
    "if-none-match",
    "user-agent",
}
_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_RETAINED_RESPONSE_HEADERS = {
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-length",
    "content-type",
    "date",
    "etag",
    "expires",
    "last-modified",
    "retry-after",
    "warning",
    "x-request-id",
}
_EnumT = TypeVar("_EnumT", bound=Enum)


class OfficialSourceFormat(str, Enum):
    """Machine and last-resort publication formats declared by a source."""

    SDMX_21 = "sdmx-2.1"
    SDMX_30 = "sdmx-3.0"
    JSON_STAT = "json-stat"
    JSON = "json"
    CSV = "csv"
    TSV = "tsv"
    XLS = "xls"
    XLSX = "xlsx"
    RSS = "rss"
    ATOM = "atom"
    ICS = "ics"
    HTML = "html"
    PDF = "pdf"
    ARCHIVE = "archive"
    DATA_CATALOG = "data-catalog"

    @classmethod
    def from_value(
        cls, value: str | OfficialSourceFormat
    ) -> OfficialSourceFormat:
        return _enum_value(cls, value, "source format")


class OfficialSourceCapability(str, Enum):
    """Claims an endpoint may support, kept distinct from empirical proof."""

    CURRENT_VALUES = "current-values"
    HISTORICAL_OBSERVATIONS = "historical-observations"
    HISTORICAL_VINTAGES = "historical-vintages"
    PUBLICATION_METADATA = "publication-metadata"
    SCHEDULE_METADATA = "schedule-metadata"
    REVISION_METADATA = "revision-metadata"
    SERIES_METADATA = "series-metadata"
    FORECAST_SURVEY = "forecast-survey"
    ARCHIVE_ENUMERATION = "archive-enumeration"

    @classmethod
    def from_value(
        cls, value: str | OfficialSourceCapability
    ) -> OfficialSourceCapability:
        return _enum_value(cls, value, "source capability")


class OfficialSourceRole(str, Enum):
    """Authority role of one source for its declared event families."""

    PRIMARY_PRODUCER = "primary-producer"
    OFFICIAL_ARCHIVE = "official-archive"
    RELEASE_CALENDAR = "release-calendar"
    INDEPENDENT_OFFICIAL_CROSS_CHECK = "independent-official-cross-check"

    @classmethod
    def from_value(cls, value: str | OfficialSourceRole) -> OfficialSourceRole:
        return _enum_value(cls, value, "source role")


class OfficialAuthenticationKind(str, Enum):
    """Non-secret authentication requirement recorded in the registry."""

    NONE = "none"
    OPTIONAL_API_KEY = "optional-api-key"
    REQUIRED_API_KEY = "required-api-key"

    @classmethod
    def from_value(
        cls, value: str | OfficialAuthenticationKind
    ) -> OfficialAuthenticationKind:
        return _enum_value(cls, value, "authentication kind")


class OfficialSourceVerificationStatus(str, Enum):
    """Review state without overstating series or vintage qualification."""

    REVIEWED_ENTRYPOINT = "reviewed-entrypoint"
    EMPIRICALLY_VERIFIED = "empirically-verified"
    SERIES_QUALIFICATION_REQUIRED = "series-qualification-required"
    HISTORICALLY_INCOMPLETE = "historically-incomplete"
    RETIRED = "retired"

    @classmethod
    def from_value(
        cls, value: str | OfficialSourceVerificationStatus
    ) -> OfficialSourceVerificationStatus:
        return _enum_value(cls, value, "verification status")


class OfficialRequestMethod(str, Enum):
    """Bounded HTTP methods supported by the acquisition layer."""

    GET = "GET"
    POST = "POST"

    @classmethod
    def from_value(
        cls, value: str | OfficialRequestMethod
    ) -> OfficialRequestMethod:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().upper())
        except ValueError as exc:
            raise ValueError("unsupported official request method") from exc


@dataclass(frozen=True, slots=True)
class OfficialSourceEntryV1:
    """Reviewed entrypoint owned by one legal producer or official mirror."""

    source_key: str
    economy_code: str
    economy_name: str
    institution: str
    jurisdiction: str
    legal_producer: bool
    roles: tuple[OfficialSourceRole, ...]
    event_families: tuple[EconomicEventFamily, ...]
    indicator_ids: tuple[str, ...]
    formats: tuple[OfficialSourceFormat, ...]
    capabilities: tuple[OfficialSourceCapability, ...]
    endpoint_uri_template: str
    allowed_hosts: tuple[str, ...]
    expected_media_types: tuple[str, ...]
    source_series_ids: tuple[str, ...]
    source_table_ids: tuple[str, ...]
    source_release_ids: tuple[str, ...]
    release_calendar_uri: str | None
    archive_uri: str | None
    source_timezone: str
    availability_time_precision: EconomicTimePrecision
    dst_convention: str
    historical_depth: str
    known_breaks: tuple[str, ...]
    revision_behavior: str
    rate_limit: str
    authentication: OfficialAuthenticationKind
    credential_env_name: str | None
    credential_header_name: str | None
    credential_parameter_name: str | None
    credential_path_placeholder: str | None
    terms_uri: str | None
    fallback_source_keys: tuple[str, ...]
    parser_id: str
    parser_version: str
    replay_strategy: str
    adapter_owner: str
    verification_status: OfficialSourceVerificationStatus
    reviewed_on: str
    verification_evidence_uris: tuple[str, ...]
    redistribution_notes: str
    limitations: tuple[str, ...]
    schema_version: str = OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported official source entry schema")
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        code = _economy_code(self.economy_code)
        object.__setattr__(self, "economy_code", code)
        for name in ("economy_name", "institution", "jurisdiction"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if not isinstance(self.legal_producer, bool):
            raise TypeError("legal_producer must be boolean")
        roles = _enum_tuple(OfficialSourceRole, self.roles, "roles")
        families = _enum_tuple(
            EconomicEventFamily, self.event_families, "event_families"
        )
        formats = _ordered_enum_tuple(
            OfficialSourceFormat, self.formats, "formats"
        )
        capabilities = _enum_tuple(
            OfficialSourceCapability, self.capabilities, "capabilities"
        )
        if not roles or not families or not formats or not capabilities:
            raise ValueError(
                "source roles, families, formats, and capabilities are required"
            )
        if (
            OfficialSourceRole.PRIMARY_PRODUCER in roles
            and not self.legal_producer
        ):
            raise ValueError(
                "a primary source must identify its legal producer"
            )
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "event_families", families)
        object.__setattr__(self, "formats", formats)
        object.__setattr__(self, "capabilities", capabilities)
        indicator_ids = _key_tuple(self.indicator_ids, "indicator_ids")
        expected_indicator_ids = {
            _CANONICAL_INDICATOR_ID_BY_FAMILY[family] for family in families
        }
        if set(indicator_ids) != expected_indicator_ids:
            raise ValueError(
                "indicator_ids must match the canonical identifier for each event family"
            )
        object.__setattr__(self, "indicator_ids", indicator_ids)
        for name in (
            "source_series_ids",
            "source_table_ids",
            "source_release_ids",
        ):
            object.__setattr__(
                self, name, _string_tuple(getattr(self, name), name)
            )
        endpoint = _network_https_uri(
            self.endpoint_uri_template,
            "endpoint_uri_template",
        )
        object.__setattr__(self, "endpoint_uri_template", endpoint)
        endpoint_host = cast(str, urlparse(endpoint).hostname).lower()
        hosts = tuple(
            sorted(
                {_host(item, "allowed_hosts") for item in self.allowed_hosts}
            )
        )
        if endpoint_host not in hosts:
            raise ValueError("allowed_hosts must contain the endpoint host")
        object.__setattr__(self, "allowed_hosts", hosts)
        media_types = tuple(
            sorted({_media_type(item) for item in self.expected_media_types})
        )
        if not media_types:
            raise ValueError("expected_media_types must not be empty")
        object.__setattr__(self, "expected_media_types", media_types)
        for name in ("release_calendar_uri", "archive_uri", "terms_uri"):
            value = getattr(self, name)
            object.__setattr__(
                self,
                name,
                _https_uri(value, name) if value is not None else None,
            )
        fetch_route_hosts = {
            cast(str, urlparse(uri).hostname).lower()
            for uri in (self.release_calendar_uri, self.archive_uri)
            if uri is not None
        }
        if not fetch_route_hosts.issubset(hosts):
            raise ValueError(
                "allowed_hosts must contain release-calendar and archive hosts"
            )
        if (
            OfficialSourceRole.RELEASE_CALENDAR in roles
            and self.release_calendar_uri is None
        ):
            raise ValueError("release-calendar sources require a calendar URI")
        if (
            OfficialSourceRole.OFFICIAL_ARCHIVE in roles
            and self.archive_uri is None
        ):
            raise ValueError("official-archive sources require an archive URI")
        try:
            normalize_market_context_datetime(
                "2000-01-15T12:00:00", self.source_timezone
            )
        except ValueError as exc:
            raise ValueError("unsupported official source timezone") from exc
        object.__setattr__(
            self,
            "availability_time_precision",
            EconomicTimePrecision.from_value(self.availability_time_precision),
        )
        for name in (
            "dst_convention",
            "historical_depth",
            "revision_behavior",
            "rate_limit",
            "parser_id",
            "parser_version",
            "replay_strategy",
            "adapter_owner",
            "redistribution_notes",
        ):
            value = (
                _key(getattr(self, name), name)
                if name == "parser_id"
                else _text(getattr(self, name), name)
            )
            object.__setattr__(self, name, value)
        object.__setattr__(
            self, "known_breaks", _text_tuple(self.known_breaks, "known_breaks")
        )
        object.__setattr__(
            self, "limitations", _text_tuple(self.limitations, "limitations")
        )
        object.__setattr__(
            self,
            "fallback_source_keys",
            _key_tuple(self.fallback_source_keys, "fallback_source_keys"),
        )
        authentication = OfficialAuthenticationKind.from_value(
            self.authentication
        )
        object.__setattr__(self, "authentication", authentication)
        env_name = _optional_text(
            self.credential_env_name, "credential_env_name"
        )
        header_name = _optional_text(
            self.credential_header_name, "credential_header_name"
        )
        parameter_name = _optional_text(
            self.credential_parameter_name, "credential_parameter_name"
        )
        path_placeholder = _optional_text(
            self.credential_path_placeholder, "credential_path_placeholder"
        )
        if authentication is OfficialAuthenticationKind.NONE:
            if (
                env_name is not None
                or header_name is not None
                or parameter_name is not None
                or path_placeholder is not None
            ):
                raise ValueError(
                    "an unauthenticated source cannot declare credentials"
                )
        elif (
            env_name is None
            or sum(
                item is not None
                for item in (header_name, parameter_name, path_placeholder)
            )
            != 1
        ):
            raise ValueError(
                "authenticated sources require env and exactly one header, parameter, "
                "or path placeholder"
            )
        if env_name is not None and _ENV_RE.fullmatch(env_name) is None:
            raise ValueError("credential_env_name is invalid")
        if header_name is not None:
            _header_name(header_name)
        if parameter_name is not None:
            _text(parameter_name, "credential_parameter_name")
        if path_placeholder is not None and (
            path_placeholder != OFFICIAL_CREDENTIAL_PATH_PLACEHOLDER
        ):
            raise ValueError("credential_path_placeholder is unsupported")
        object.__setattr__(self, "credential_env_name", env_name)
        object.__setattr__(self, "credential_header_name", header_name)
        object.__setattr__(self, "credential_parameter_name", parameter_name)
        object.__setattr__(
            self, "credential_path_placeholder", path_placeholder
        )
        verification_status = OfficialSourceVerificationStatus.from_value(
            self.verification_status
        )
        if (
            OfficialSourceRole.PRIMARY_PRODUCER in roles
            and verification_status is OfficialSourceVerificationStatus.RETIRED
        ):
            raise ValueError("a retired source cannot be a primary producer")
        if (
            verification_status
            is OfficialSourceVerificationStatus.EMPIRICALLY_VERIFIED
            and not (
                self.source_series_ids
                or self.source_table_ids
                or self.source_release_ids
            )
        ):
            raise ValueError(
                "empirically verified sources require a concrete source identifier"
            )
        object.__setattr__(self, "verification_status", verification_status)
        object.__setattr__(
            self, "reviewed_on", _iso_date(self.reviewed_on, "reviewed_on")
        )
        evidence = tuple(
            _https_uri(item, "verification_evidence_uris")
            for item in self.verification_evidence_uris
        )
        if not evidence:
            raise ValueError("verification evidence is required")
        object.__setattr__(self, "verification_evidence_uris", evidence)

    @property
    def source_id(self) -> str:
        return _stable_id("official-source", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_key": self.source_key,
            "economy_code": self.economy_code,
            "economy_name": self.economy_name,
            "institution": self.institution,
            "jurisdiction": self.jurisdiction,
            "legal_producer": self.legal_producer,
            "roles": [item.value for item in self.roles],
            "event_families": [item.value for item in self.event_families],
            "indicator_ids": list(self.indicator_ids),
            "formats": [item.value for item in self.formats],
            "capabilities": [item.value for item in self.capabilities],
            "endpoint_uri_template": self.endpoint_uri_template,
            "allowed_hosts": list(self.allowed_hosts),
            "expected_media_types": list(self.expected_media_types),
            "source_series_ids": list(self.source_series_ids),
            "source_table_ids": list(self.source_table_ids),
            "source_release_ids": list(self.source_release_ids),
            "release_calendar_uri": self.release_calendar_uri,
            "archive_uri": self.archive_uri,
            "source_timezone": self.source_timezone,
            "availability_time_precision": self.availability_time_precision.value,
            "dst_convention": self.dst_convention,
            "historical_depth": self.historical_depth,
            "known_breaks": list(self.known_breaks),
            "revision_behavior": self.revision_behavior,
            "rate_limit": self.rate_limit,
            "authentication": self.authentication.value,
            "credential_env_name": self.credential_env_name,
            "credential_header_name": self.credential_header_name,
            "credential_parameter_name": self.credential_parameter_name,
            "credential_path_placeholder": self.credential_path_placeholder,
            "terms_uri": self.terms_uri,
            "fallback_source_keys": list(self.fallback_source_keys),
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "replay_strategy": self.replay_strategy,
            "adapter_owner": self.adapter_owner,
            "verification_status": self.verification_status.value,
            "reviewed_on": self.reviewed_on,
            "verification_evidence_uris": list(self.verification_evidence_uris),
            "redistribution_notes": self.redistribution_notes,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "source_id": self.source_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OfficialSourceEntryV1:
        _require_schema(data, OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION)
        value = cls(
            source_key=str(data.get("source_key", "")),
            economy_code=str(data.get("economy_code", "")),
            economy_name=str(data.get("economy_name", "")),
            institution=str(data.get("institution", "")),
            jurisdiction=str(data.get("jurisdiction", "")),
            legal_producer=_strict_bool(
                data.get("legal_producer"), "legal_producer"
            ),
            roles=tuple(
                OfficialSourceRole.from_value(str(item))
                for item in _sequence(data.get("roles"), "roles")
            ),
            event_families=tuple(
                EconomicEventFamily.from_value(str(item))
                for item in _sequence(
                    data.get("event_families"), "event_families"
                )
            ),
            indicator_ids=_strings(data.get("indicator_ids"), "indicator_ids"),
            formats=tuple(
                OfficialSourceFormat.from_value(str(item))
                for item in _sequence(data.get("formats"), "formats")
            ),
            capabilities=tuple(
                OfficialSourceCapability.from_value(str(item))
                for item in _sequence(data.get("capabilities"), "capabilities")
            ),
            endpoint_uri_template=str(data.get("endpoint_uri_template", "")),
            allowed_hosts=_strings(data.get("allowed_hosts"), "allowed_hosts"),
            expected_media_types=_strings(
                data.get("expected_media_types"), "expected_media_types"
            ),
            source_series_ids=_strings(
                data.get("source_series_ids"), "source_series_ids"
            ),
            source_table_ids=_strings(
                data.get("source_table_ids"), "source_table_ids"
            ),
            source_release_ids=_strings(
                data.get("source_release_ids"), "source_release_ids"
            ),
            release_calendar_uri=_optional_text(
                data.get("release_calendar_uri"), "release_calendar_uri"
            ),
            archive_uri=_optional_text(data.get("archive_uri"), "archive_uri"),
            source_timezone=str(data.get("source_timezone", "")),
            availability_time_precision=EconomicTimePrecision.from_value(
                str(data.get("availability_time_precision", ""))
            ),
            dst_convention=str(data.get("dst_convention", "")),
            historical_depth=str(data.get("historical_depth", "")),
            known_breaks=_strings(data.get("known_breaks"), "known_breaks"),
            revision_behavior=str(data.get("revision_behavior", "")),
            rate_limit=str(data.get("rate_limit", "")),
            authentication=OfficialAuthenticationKind.from_value(
                str(data.get("authentication", ""))
            ),
            credential_env_name=_optional_text(
                data.get("credential_env_name"), "credential_env_name"
            ),
            credential_header_name=_optional_text(
                data.get("credential_header_name"), "credential_header_name"
            ),
            credential_parameter_name=_optional_text(
                data.get("credential_parameter_name"),
                "credential_parameter_name",
            ),
            credential_path_placeholder=_optional_text(
                data.get("credential_path_placeholder"),
                "credential_path_placeholder",
            ),
            terms_uri=_optional_text(data.get("terms_uri"), "terms_uri"),
            fallback_source_keys=_strings(
                data.get("fallback_source_keys"), "fallback_source_keys"
            ),
            parser_id=str(data.get("parser_id", "")),
            parser_version=str(data.get("parser_version", "")),
            replay_strategy=str(data.get("replay_strategy", "")),
            adapter_owner=str(data.get("adapter_owner", "")),
            verification_status=OfficialSourceVerificationStatus.from_value(
                str(data.get("verification_status", ""))
            ),
            reviewed_on=str(data.get("reviewed_on", "")),
            verification_evidence_uris=_strings(
                data.get("verification_evidence_uris"),
                "verification_evidence_uris",
            ),
            redistribution_notes=str(data.get("redistribution_notes", "")),
            limitations=_strings(data.get("limitations"), "limitations"),
            schema_version=str(data.get("schema_version", "")),
        )
        expected = data.get("source_id")
        if expected is not None and value.source_id != str(expected):
            raise ValueError("official source identity differs")
        return value


@dataclass(frozen=True, slots=True)
class OfficialSourceRegistryV1:
    """Reviewed source matrix with exactly one legal producer per cell."""

    reviewed_on: str
    scoped_economies: tuple[str, ...]
    sources: tuple[OfficialSourceEntryV1, ...]
    limitations: tuple[str, ...] = ()
    schema_version: str = OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION:
            raise ValueError("unsupported official source registry schema")
        object.__setattr__(
            self, "reviewed_on", _iso_date(self.reviewed_on, "reviewed_on")
        )
        scope = tuple(
            sorted({_economy_code(item) for item in self.scoped_economies})
        )
        if not scope:
            raise ValueError("official source registry scope is empty")
        object.__setattr__(self, "scoped_economies", scope)
        values = tuple(self.sources)
        if not values or len(values) > MAX_OFFICIAL_SOURCES:
            raise ValueError("official source count is outside v1 bounds")
        if any(not isinstance(item, OfficialSourceEntryV1) for item in values):
            raise ValueError("registry contains a non-v1 source")
        values = tuple(sorted(values, key=lambda item: item.source_key))
        if len({item.source_key for item in values}) != len(values):
            raise ValueError("duplicate official source_key")
        if len({item.source_id for item in values}) != len(values):
            raise ValueError("duplicate official source identity")
        unknown = {item.economy_code for item in values} - set(scope)
        if unknown:
            raise ValueError("official source lies outside registry scope")
        by_key = {item.source_key: item for item in values}
        for source in values:
            for fallback in source.fallback_source_keys:
                if fallback == source.source_key or fallback not in by_key:
                    raise ValueError("official fallback source is invalid")
                if by_key[fallback].economy_code != source.economy_code:
                    raise ValueError("official fallback source changes economy")
        object.__setattr__(self, "sources", values)
        object.__setattr__(
            self, "limitations", _text_tuple(self.limitations, "limitations")
        )
        _validate_primary_source_matrix(scope, values)

    @property
    def registry_id(self) -> str:
        return _stable_id("official-source-registry", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reviewed_on": self.reviewed_on,
            "scoped_economies": list(self.scoped_economies),
            "sources": [item.to_dict() for item in self.sources],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "registry_id": self.registry_id}

    def source(self, key_or_id: str) -> OfficialSourceEntryV1:
        matches = [
            item
            for item in self.sources
            if key_or_id in {item.source_key, item.source_id}
        ]
        if len(matches) != 1:
            raise ValueError("expected exactly one registered official source")
        return matches[0]

    def primary_source(
        self, economy_code: str, family: EconomicEventFamily
    ) -> OfficialSourceEntryV1:
        code = _economy_code(economy_code)
        normalized_family = EconomicEventFamily.from_value(family)
        matches = [
            item
            for item in self.sources
            if item.economy_code == code
            and normalized_family in item.event_families
            and OfficialSourceRole.PRIMARY_PRODUCER in item.roles
        ]
        if len(matches) != 1:
            raise ValueError("expected exactly one primary official source")
        return matches[0]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OfficialSourceRegistryV1:
        _require_schema(data, OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION)
        value = cls(
            reviewed_on=str(data.get("reviewed_on", "")),
            scoped_economies=_strings(
                data.get("scoped_economies"), "scoped_economies"
            ),
            sources=tuple(
                OfficialSourceEntryV1.from_dict(_mapping(item, "source"))
                for item in _sequence(data.get("sources"), "sources")
            ),
            limitations=_strings(data.get("limitations"), "limitations"),
            schema_version=str(data.get("schema_version", "")),
        )
        expected = data.get("registry_id")
        if expected is not None and value.registry_id != str(expected):
            raise ValueError("official source registry identity differs")
        return value


@dataclass(frozen=True, slots=True)
class OfficialFetchPolicyV1:
    """Fail-closed request, response, retry, event, and runtime bounds."""

    max_attempts: int = 3
    max_redirects: int = 5
    backoff_seconds: tuple[float, ...] = (0.25, 1.0)
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 45.0
    max_response_bytes: int = 32 * 1024 * 1024
    max_total_bytes: int = 256 * 1024 * 1024
    max_requests: int = 128
    max_pages: int = 64
    max_events: int = 100_000
    max_runtime_seconds: float = 600.0
    chunk_size_bytes: int = 64 * 1024
    user_agent: str = "histdatacom-official-source/1"
    schema_version: str = OFFICIAL_FETCH_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_FETCH_POLICY_SCHEMA_VERSION:
            raise ValueError("unsupported official fetch policy schema")
        _bounded_int(self.max_attempts, "max_attempts", 1, 10)
        _bounded_int(self.max_redirects, "max_redirects", 0, 10)
        values = tuple(
            _positive_float(item, "backoff_seconds")
            for item in self.backoff_seconds
        )
        if len(values) < self.max_attempts - 1 or len(values) > 9:
            raise ValueError("backoff_seconds do not cover bounded attempts")
        object.__setattr__(self, "backoff_seconds", values)
        object.__setattr__(
            self,
            "connect_timeout_seconds",
            _positive_float(
                self.connect_timeout_seconds, "connect_timeout_seconds"
            ),
        )
        object.__setattr__(
            self,
            "read_timeout_seconds",
            _positive_float(self.read_timeout_seconds, "read_timeout_seconds"),
        )
        _bounded_int(
            self.max_response_bytes,
            "max_response_bytes",
            1,
            MAX_OFFICIAL_RESPONSE_BYTES,
        )
        _bounded_int(
            self.max_total_bytes, "max_total_bytes", 1, MAX_OFFICIAL_TOTAL_BYTES
        )
        _bounded_int(
            self.max_requests, "max_requests", 1, MAX_OFFICIAL_REQUESTS
        )
        _bounded_int(self.max_pages, "max_pages", 1, MAX_OFFICIAL_REQUESTS)
        _bounded_int(self.max_events, "max_events", 1, MAX_OFFICIAL_EVENTS)
        object.__setattr__(
            self,
            "max_runtime_seconds",
            _positive_float(self.max_runtime_seconds, "max_runtime_seconds"),
        )
        _bounded_int(
            self.chunk_size_bytes, "chunk_size_bytes", 1024, 1024 * 1024
        )
        object.__setattr__(
            self,
            "user_agent",
            _header_value(self.user_agent, "user_agent"),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "max_attempts": self.max_attempts,
            "max_redirects": self.max_redirects,
            "backoff_seconds": list(self.backoff_seconds),
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "read_timeout_seconds": self.read_timeout_seconds,
            "max_response_bytes": self.max_response_bytes,
            "max_total_bytes": self.max_total_bytes,
            "max_requests": self.max_requests,
            "max_pages": self.max_pages,
            "max_events": self.max_events,
            "max_runtime_seconds": self.max_runtime_seconds,
            "chunk_size_bytes": self.chunk_size_bytes,
            "user_agent": self.user_agent,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OfficialFetchPolicyV1:
        _require_schema(data, OFFICIAL_FETCH_POLICY_SCHEMA_VERSION)
        return cls(
            max_attempts=_strict_int(data.get("max_attempts"), "max_attempts"),
            max_redirects=_strict_int(
                data.get("max_redirects"), "max_redirects"
            ),
            backoff_seconds=tuple(
                _strict_float(item, "backoff_seconds")
                for item in _sequence(
                    data.get("backoff_seconds"), "backoff_seconds"
                )
            ),
            connect_timeout_seconds=_strict_float(
                data.get("connect_timeout_seconds"), "connect_timeout_seconds"
            ),
            read_timeout_seconds=_strict_float(
                data.get("read_timeout_seconds"), "read_timeout_seconds"
            ),
            max_response_bytes=_strict_int(
                data.get("max_response_bytes"), "max_response_bytes"
            ),
            max_total_bytes=_strict_int(
                data.get("max_total_bytes"), "max_total_bytes"
            ),
            max_requests=_strict_int(data.get("max_requests"), "max_requests"),
            max_pages=_strict_int(data.get("max_pages"), "max_pages"),
            max_events=_strict_int(data.get("max_events"), "max_events"),
            max_runtime_seconds=_strict_float(
                data.get("max_runtime_seconds"), "max_runtime_seconds"
            ),
            chunk_size_bytes=_strict_int(
                data.get("chunk_size_bytes"), "chunk_size_bytes"
            ),
            user_agent=str(data.get("user_agent", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class OfficialSourceRequestV1:
    """Credential-free deterministic request bound to one source and parser."""

    source_key: str
    source_id: str
    method: OfficialRequestMethod
    uri: str
    source_format: OfficialSourceFormat
    query_parameters: Mapping[str, str] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    body_text: str | None = None
    body_content_type: str | None = None
    parser_id: str = ""
    parser_version: str = ""
    window_start: str | None = None
    window_end: str | None = None
    page_number: int | None = None
    if_none_match: str | None = None
    if_modified_since: str | None = None
    schema_version: str = OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION:
            raise ValueError("unsupported official source request schema")
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        _stable_digest_id(self.source_id, "official-source", "source_id")
        object.__setattr__(
            self, "method", OfficialRequestMethod.from_value(self.method)
        )
        object.__setattr__(self, "uri", _network_https_uri(self.uri, "uri"))
        object.__setattr__(
            self,
            "source_format",
            OfficialSourceFormat.from_value(self.source_format),
        )
        query = _string_mapping(
            self.query_parameters,
            "query_parameters",
            MAX_OFFICIAL_QUERY_PARAMETERS,
        )
        headers = _header_mapping(self.headers, allow_sensitive=False)
        if set(headers) & _RESERVED_REQUEST_HEADERS:
            raise ValueError("reserved transport headers cannot be serialized")
        object.__setattr__(self, "query_parameters", MappingProxyType(query))
        object.__setattr__(self, "headers", MappingProxyType(headers))
        body = _optional_request_body(self.body_text)
        body_type = _optional_text(self.body_content_type, "body_content_type")
        if self.method is OfficialRequestMethod.GET and body is not None:
            raise ValueError("GET requests cannot carry a request body")
        if (body is None) != (body_type is None):
            raise ValueError(
                "request body and content type must be declared together"
            )
        object.__setattr__(self, "body_text", body)
        object.__setattr__(
            self,
            "body_content_type",
            _media_type(body_type) if body_type is not None else None,
        )
        object.__setattr__(self, "parser_id", _key(self.parser_id, "parser_id"))
        object.__setattr__(
            self, "parser_version", _text(self.parser_version, "parser_version")
        )
        start = _optional_iso_date(self.window_start, "window_start")
        end = _optional_iso_date(self.window_end, "window_end")
        if (start is None) != (end is None):
            raise ValueError("request window requires both start and end")
        if start is not None and cast(str, end) < start:
            raise ValueError("request window end precedes start")
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        if self.page_number is not None:
            _bounded_int(
                self.page_number, "page_number", 0, MAX_OFFICIAL_REQUESTS
            )
        object.__setattr__(
            self,
            "if_modified_since",
            (
                _header_value(self.if_modified_since, "if_modified_since")
                if self.if_modified_since is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "if_none_match",
            (
                _header_value(self.if_none_match, "if_none_match")
                if self.if_none_match is not None
                else None
            ),
        )

    @property
    def request_id(self) -> str:
        return _stable_id("official-request", self.to_dict())

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_key": self.source_key,
            "source_id": self.source_id,
            "method": self.method.value,
            "uri": self.uri,
            "source_format": self.source_format.value,
            "query_parameters": dict(self.query_parameters),
            "headers": dict(self.headers),
            "body_text": self.body_text,
            "body_content_type": self.body_content_type,
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "page_number": self.page_number,
            "if_none_match": self.if_none_match,
            "if_modified_since": self.if_modified_since,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OfficialSourceRequestV1:
        _require_schema(data, OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION)
        return cls(
            source_key=str(data.get("source_key", "")),
            source_id=str(data.get("source_id", "")),
            method=OfficialRequestMethod.from_value(
                str(data.get("method", ""))
            ),
            uri=str(data.get("uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            query_parameters={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("query_parameters"), "query_parameters"
                ).items()
            },
            headers={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("headers"), "headers"
                ).items()
            },
            body_text=_optional_request_body(data.get("body_text")),
            body_content_type=_optional_text(
                data.get("body_content_type"), "body_content_type"
            ),
            parser_id=str(data.get("parser_id", "")),
            parser_version=str(data.get("parser_version", "")),
            window_start=_optional_text(
                data.get("window_start"), "window_start"
            ),
            window_end=_optional_text(data.get("window_end"), "window_end"),
            page_number=_optional_int(data.get("page_number"), "page_number"),
            if_none_match=_optional_text(
                data.get("if_none_match"), "if_none_match"
            ),
            if_modified_since=_optional_text(
                data.get("if_modified_since"), "if_modified_since"
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class OfficialRequestManifestV1:
    """Deterministic, bounded set of credential-free network requests."""

    registry_id: str
    policy: OfficialFetchPolicyV1
    requests: tuple[OfficialSourceRequestV1, ...]
    schema_version: str = OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported official request manifest schema")
        _stable_digest_id(
            self.registry_id, "official-source-registry", "registry_id"
        )
        if not isinstance(self.policy, OfficialFetchPolicyV1):
            raise TypeError("request manifest requires a v1 fetch policy")
        values = tuple(self.requests)
        if not values or len(values) > self.policy.max_requests:
            raise ValueError("official request count is outside policy bounds")
        if any(
            not isinstance(item, OfficialSourceRequestV1) for item in values
        ):
            raise ValueError("request manifest contains a non-v1 request")
        if len({item.request_id for item in values}) != len(values):
            raise ValueError("duplicate official request identity")
        if (
            sum(item.page_number is not None for item in values)
            > self.policy.max_pages
        ):
            raise ValueError("official request pages exceed policy bound")
        object.__setattr__(self, "requests", values)
        if (
            len(canonical_contract_json(self.to_dict()).encode("utf-8"))
            > MAX_OFFICIAL_MANIFEST_BYTES
        ):
            raise ValueError(
                "official request manifest exceeds serialized byte bound"
            )

    @property
    def manifest_id(self) -> str:
        return _stable_id("official-request-manifest", self.to_dict())

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "policy": self.policy.to_dict(),
            "requests": [
                {**item.to_dict(), "request_id": item.request_id}
                for item in self.requests
            ],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OfficialRequestManifestV1:
        _require_schema(data, OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION)
        requests_values: list[OfficialSourceRequestV1] = []
        for item in _sequence(data.get("requests"), "requests"):
            raw = _mapping(item, "request")
            request = OfficialSourceRequestV1.from_dict(raw)
            expected = raw.get("request_id")
            if expected != request.request_id:
                raise ValueError("official request identity differs")
            requests_values.append(request)
        return cls(
            registry_id=str(data.get("registry_id", "")),
            policy=OfficialFetchPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            requests=tuple(requests_values),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class OfficialRawSnapshotV1:
    """One immutable response, including a conditional 304 reuse receipt."""

    request: OfficialSourceRequestV1
    retrieved_at_ns: int
    completed_at_ns: int
    status_code: int
    resolved_uri: str
    response_headers: Mapping[str, str]
    content: bytes
    content_type: str
    content_encoding: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    source_date: str | None = None
    reused_from_snapshot_id: str | None = None
    attempt_count: int = 1
    schema_version: str = OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("unsupported official raw snapshot schema")
        if not isinstance(self.request, OfficialSourceRequestV1):
            raise TypeError("raw snapshot requires a v1 request")
        _bounded_int64(self.retrieved_at_ns, "retrieved_at_ns")
        _bounded_int64(self.completed_at_ns, "completed_at_ns")
        if self.completed_at_ns < self.retrieved_at_ns:
            raise ValueError("snapshot completion precedes retrieval")
        if self.status_code not in {200, 304}:
            raise ValueError("snapshot status is not a successful acquisition")
        object.__setattr__(
            self,
            "resolved_uri",
            _network_https_uri(self.resolved_uri, "resolved_uri"),
        )
        object.__setattr__(
            self,
            "response_headers",
            MappingProxyType(
                _header_mapping(self.response_headers, allow_sensitive=False)
            ),
        )
        if not isinstance(self.content, bytes) or not self.content:
            raise ValueError(
                "official raw snapshot content must be non-empty bytes"
            )
        if len(self.content) > MAX_OFFICIAL_RESPONSE_BYTES:
            raise ValueError("official raw snapshot exceeds byte bound")
        object.__setattr__(self, "content_type", _media_type(self.content_type))
        for name in (
            "content_encoding",
            "etag",
            "last_modified",
            "source_date",
            "reused_from_snapshot_id",
        ):
            object.__setattr__(
                self, name, _optional_text(getattr(self, name), name)
            )
        if self.status_code == 304:
            if self.reused_from_snapshot_id is None:
                raise ValueError("304 snapshots require a reused predecessor")
            _stable_digest_id(
                self.reused_from_snapshot_id,
                "official-snapshot",
                "reused_from_snapshot_id",
            )
        elif self.reused_from_snapshot_id is not None:
            raise ValueError("only 304 snapshots may reuse predecessor bytes")
        _bounded_int(self.attempt_count, "attempt_count", 1, 10)

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def snapshot_id(self) -> str:
        return _stable_id("official-snapshot", self.evidence_dict())

    def evidence_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "request": {
                **self.request.to_dict(),
                "request_id": self.request.request_id,
            },
            "retrieved_at_ns": self.retrieved_at_ns,
            "completed_at_ns": self.completed_at_ns,
            "status_code": self.status_code,
            "resolved_uri": self.resolved_uri,
            "response_headers": dict(self.response_headers),
            "content_sha256": self.content_sha256,
            "size_bytes": len(self.content),
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "source_date": self.source_date,
            "reused_from_snapshot_id": self.reused_from_snapshot_id,
            "attempt_count": self.attempt_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.evidence_dict(), "snapshot_id": self.snapshot_id}

    @classmethod
    def restore(
        cls, data: Mapping[str, Any], content: bytes
    ) -> OfficialRawSnapshotV1:
        _require_schema(data, OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION)
        request_data = _mapping(data.get("request"), "request")
        request = OfficialSourceRequestV1.from_dict(request_data)
        if request_data.get("request_id") != request.request_id:
            raise ValueError("restored official request identity differs")
        value = cls(
            request=request,
            retrieved_at_ns=_strict_int(
                data.get("retrieved_at_ns"), "retrieved_at_ns"
            ),
            completed_at_ns=_strict_int(
                data.get("completed_at_ns"), "completed_at_ns"
            ),
            status_code=_strict_int(data.get("status_code"), "status_code"),
            resolved_uri=str(data.get("resolved_uri", "")),
            response_headers={
                str(key): str(item)
                for key, item in _mapping(
                    data.get("response_headers"), "response_headers"
                ).items()
            },
            content=content,
            content_type=str(data.get("content_type", "")),
            content_encoding=_optional_text(
                data.get("content_encoding"), "content_encoding"
            ),
            etag=_optional_text(data.get("etag"), "etag"),
            last_modified=_optional_text(
                data.get("last_modified"), "last_modified"
            ),
            source_date=_optional_text(data.get("source_date"), "source_date"),
            reused_from_snapshot_id=_optional_text(
                data.get("reused_from_snapshot_id"), "reused_from_snapshot_id"
            ),
            attempt_count=_strict_int(
                data.get("attempt_count"), "attempt_count"
            ),
            schema_version=str(data.get("schema_version", "")),
        )
        if value.content_sha256 != str(data.get("content_sha256", "")):
            raise ValueError("restored official source hash differs")
        if len(content) != _strict_int(data.get("size_bytes"), "size_bytes"):
            raise ValueError("restored official source size differs")
        if value.snapshot_id != str(data.get("snapshot_id", "")):
            raise ValueError("restored official snapshot identity differs")
        return value


@dataclass(frozen=True, slots=True)
class OfficialFetchBundleV1:
    """Request manifest and its complete, ordered immutable responses."""

    request_manifest: OfficialRequestManifestV1
    snapshots: tuple[OfficialRawSnapshotV1, ...]
    schema_version: str = OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION:
            raise ValueError("unsupported official fetch bundle schema")
        if not isinstance(self.request_manifest, OfficialRequestManifestV1):
            raise TypeError("fetch bundle requires a v1 request manifest")
        values = tuple(self.snapshots)
        if any(not isinstance(item, OfficialRawSnapshotV1) for item in values):
            raise ValueError("fetch bundle contains a non-v1 snapshot")
        expected = [item.request_id for item in self.request_manifest.requests]
        observed = [item.request.request_id for item in values]
        if observed != expected:
            raise ValueError(
                "fetch bundle responses do not match request order"
            )
        if any(
            len(item.content) > self.request_manifest.policy.max_response_bytes
            for item in values
        ):
            raise ValueError("fetch bundle response exceeds source-byte policy")
        if (
            sum(len(item.content) for item in values)
            > self.request_manifest.policy.max_total_bytes
        ):
            raise ValueError("fetch bundle exceeds total source-byte policy")
        object.__setattr__(self, "snapshots", values)
        if (
            len(canonical_contract_json(self.to_dict()).encode("utf-8")) + 1
            > MAX_OFFICIAL_MANIFEST_BYTES
        ):
            raise ValueError(
                "official fetch bundle manifest exceeds serialized byte bound"
            )

    @property
    def bundle_id(self) -> str:
        return _stable_id("official-fetch-bundle", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "request_manifest": {
                **self.request_manifest.to_dict(),
                "manifest_id": self.request_manifest.manifest_id,
            },
            "snapshots": [item.to_dict() for item in self.snapshots],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "bundle_id": self.bundle_id}


@dataclass(frozen=True, slots=True)
class OfficialSourceTimestampV1:
    """Normalized instant that retains the official lexical timestamp."""

    source_lexical: str
    source_timezone: str
    precision: EconomicTimePrecision
    utc_ns: int
    source_time_fold: int | None = None

    def __post_init__(self) -> None:
        lexical = _text(self.source_lexical, "source_lexical")
        timezone_name = _text(self.source_timezone, "source_timezone")
        precision = EconomicTimePrecision.from_value(self.precision)
        utc_ns = _bounded_int64(self.utc_ns, "utc_ns")
        expected = normalize_market_context_datetime(
            lexical, timezone_name, fold=self.source_time_fold
        )
        if expected != utc_ns:
            raise ValueError("official timestamp differs from normalized UTC")
        object.__setattr__(self, "source_lexical", lexical)
        object.__setattr__(self, "source_timezone", timezone_name)
        object.__setattr__(self, "precision", precision)
        object.__setattr__(self, "utc_ns", utc_ns)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "source_lexical": self.source_lexical,
            "source_timezone": self.source_timezone,
            "precision": self.precision.value,
            "utc_ns": self.utc_ns,
            "source_time_fold": self.source_time_fold,
        }


class OfficialFetchError(RuntimeError):
    """Bounded official acquisition failed without endpoint substitution."""


class _RetryableOfficialFetchError(OfficialFetchError):
    """Internal signal for a transient response covered by retry policy."""


@runtime_checkable
class OfficialHttpResponseV1(Protocol):
    status_code: int
    headers: Mapping[str, str]
    url: str

    def iter_content(self, *, chunk_size: int) -> Iterable[bytes]: ...

    def close(self) -> None: ...


@runtime_checkable
class OfficialHttpTransportV1(Protocol):
    """Injectable requests-compatible transport used by production and tests."""

    def __call__(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, str],
        headers: Mapping[str, str],
        data: str | None,
        timeout: tuple[float, float],
        stream: bool,
        allow_redirects: bool,
    ) -> OfficialHttpResponseV1: ...


@runtime_checkable
class OfficialCredentialProviderV1(Protocol):
    """Runtime-only secret seam; returned values are never serialized."""

    def __call__(self, source: OfficialSourceEntryV1) -> str | None: ...


@runtime_checkable
class OfficialSourceParserV1(Protocol):
    """Typed protocol-specific parser seam implemented under adapter packs."""

    parser_id: str
    parser_version: str
    supported_formats: tuple[OfficialSourceFormat, ...]

    def parse(
        self, snapshot: OfficialRawSnapshotV1, *, max_events: int
    ) -> Sequence[Mapping[str, JSONValue]]: ...


def packaged_official_source_registry_path() -> Path:
    """Return the installed reviewed 21-economy registry asset."""
    return (
        Path(__file__).resolve().parent / "assets" / "official_sources_v1.json"
    )


def load_packaged_official_source_registry() -> OfficialSourceRegistryV1:
    """Load and validate the installed registry and its complete source matrix."""
    path = packaged_official_source_registry_path()
    if path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("packaged official source registry exceeds size bound")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "packaged official source registry is invalid JSON"
        ) from exc
    registry = OfficialSourceRegistryV1.from_dict(_mapping(payload, "registry"))
    if registry.scoped_economies != SCOPED_ECONOMIES:
        raise ValueError("packaged official source registry scope differs")
    return registry


def official_source_matrix(
    registry: OfficialSourceRegistryV1,
) -> Mapping[str, Mapping[EconomicEventFamily, str]]:
    """Return economy/family cells mapped to their primary source key."""
    return {
        economy: {
            family: registry.primary_source(economy, family).source_key
            for family in EconomicEventFamily
        }
        for economy in registry.scoped_economies
    }


def normalize_official_source_timestamp(
    source_lexical: str,
    source_timezone: str,
    precision: EconomicTimePrecision,
    *,
    fold: int | None = None,
) -> OfficialSourceTimestampV1:
    """Normalize a time while preserving its original source representation."""
    return OfficialSourceTimestampV1(
        source_lexical=source_lexical,
        source_timezone=source_timezone,
        precision=precision,
        utc_ns=normalize_market_context_datetime(
            source_lexical, source_timezone, fold=fold
        ),
        source_time_fold=fold,
    )


def split_official_date_windows(
    start_date: str, end_date: str, *, max_days: int
) -> tuple[tuple[str, str], ...]:
    """Split an inclusive archive interval into deterministic bounded windows."""
    start = date.fromisoformat(_iso_date(start_date, "start_date"))
    end = date.fromisoformat(_iso_date(end_date, "end_date"))
    _bounded_int(max_days, "max_days", 1, 3660)
    if end < start:
        raise ValueError("official archive end_date precedes start_date")
    result: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=max_days - 1))
        result.append((cursor.isoformat(), stop.isoformat()))
        cursor = stop + timedelta(days=1)
        if len(result) > MAX_OFFICIAL_REQUESTS:
            raise ValueError("official archive windows exceed v1 request bound")
    return tuple(result)


def plan_official_source_requests(
    source: OfficialSourceEntryV1,
    policy: OfficialFetchPolicyV1,
    *,
    method: OfficialRequestMethod = OfficialRequestMethod.GET,
    uri_template: str | None = None,
    source_format: OfficialSourceFormat | None = None,
    query_parameters: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    body_text: str | None = None,
    body_content_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    max_window_days: int | None = None,
    start_parameter: str | None = "startPeriod",
    end_parameter: str | None = "endPeriod",
    page_count: int = 1,
    page_parameter: str | None = None,
    page_start: int = 0,
    page_size: int | None = None,
    page_size_parameter: str | None = None,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan source-bound pagination and date-window requests without fetching."""
    if not isinstance(source, OfficialSourceEntryV1) or not isinstance(
        policy, OfficialFetchPolicyV1
    ):
        raise TypeError(
            "official request planning requires v1 source and policy"
        )
    _bounded_int(page_count, "page_count", 1, policy.max_pages)
    _bounded_int(page_start, "page_start", 0, MAX_OFFICIAL_REQUESTS)
    if page_count > 1 and page_parameter is None:
        raise ValueError("multi-page requests require a page_parameter")
    if page_size is not None:
        _bounded_int(page_size, "page_size", 1, MAX_OFFICIAL_EVENTS)
        if page_size_parameter is None:
            raise ValueError("page_size requires a page_size_parameter")
    if (start_date is None) != (end_date is None):
        raise ValueError(
            "request planning requires both start_date and end_date"
        )
    if start_date is None and max_window_days is not None:
        raise ValueError("max_window_days requires a date window")
    if start_date is None:
        windows: tuple[tuple[str | None, str | None], ...] = ((None, None),)
    else:
        size = 3660 if max_window_days is None else max_window_days
        windows = cast(
            tuple[tuple[str | None, str | None], ...],
            split_official_date_windows(
                start_date, cast(str, end_date), max_days=size
            ),
        )
    if len(windows) * page_count > policy.max_requests:
        raise ValueError("planned official requests exceed policy bound")
    if (
        page_parameter is not None
        and len(windows) * page_count > policy.max_pages
    ):
        raise ValueError("planned official pages exceed policy bound")
    template = _network_https_uri(
        uri_template or source.endpoint_uri_template,
        "uri_template",
    )
    if (
        cast(str, urlparse(template).hostname).lower()
        not in source.allowed_hosts
    ):
        raise ValueError("official request template host is not registered")
    selected_format = OfficialSourceFormat.from_value(
        source_format or source.formats[0]
    )
    if selected_format not in source.formats:
        raise ValueError("official request format is not registered")
    result: list[OfficialSourceRequestV1] = []
    for window_start, window_end in windows:
        for page_offset in range(page_count):
            page = page_start + page_offset
            parameters = dict(query_parameters or {})
            if window_start is not None:
                if start_parameter is not None:
                    parameters[start_parameter] = window_start
                if end_parameter is not None:
                    parameters[end_parameter] = cast(str, window_end)
            if page_parameter is not None:
                parameters[page_parameter] = str(page)
            if page_size_parameter is not None and page_size is not None:
                parameters[page_size_parameter] = str(page_size)
            uri = _expand_official_uri_template(
                template,
                start=window_start or "",
                end=window_end or "",
                page=page,
                page_size=page_size,
            )
            result.append(
                OfficialSourceRequestV1(
                    source_key=source.source_key,
                    source_id=source.source_id,
                    method=method,
                    uri=uri,
                    source_format=selected_format,
                    query_parameters=parameters,
                    headers=headers or {},
                    body_text=body_text,
                    body_content_type=body_content_type,
                    parser_id=source.parser_id,
                    parser_version=source.parser_version,
                    window_start=window_start,
                    window_end=window_end,
                    page_number=page if page_parameter is not None else None,
                )
            )
    return tuple(result)


def condition_official_request(
    request: OfficialSourceRequestV1, previous: OfficialRawSnapshotV1
) -> OfficialSourceRequestV1:
    """Bind ETag and/or Last-Modified validators from the exact prior request."""
    if previous.request.source_id != request.source_id:
        raise ValueError("conditional request changes official source identity")
    if not _same_official_request_target(previous.request, request):
        raise ValueError("conditional request changes official request target")
    if previous.etag is None and previous.last_modified is None:
        raise ValueError(
            "prior official snapshot has no conditional validators"
        )
    return replace(
        request,
        if_none_match=previous.etag,
        if_modified_since=previous.last_modified,
    )


def build_official_request_manifest(
    registry: OfficialSourceRegistryV1,
    requests_: Sequence[OfficialSourceRequestV1],
    *,
    policy: OfficialFetchPolicyV1 | None = None,
) -> OfficialRequestManifestV1:
    """Validate request/source/parser identities and create a stable manifest."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("official request manifest requires a v1 registry")
    values = tuple(requests_)
    for request in values:
        source = registry.source(request.source_key)
        _validate_request_source(request, source)
    return OfficialRequestManifestV1(
        registry_id=registry.registry_id,
        policy=policy or OfficialFetchPolicyV1(),
        requests=values,
    )


def fetch_official_request_manifest(
    registry: OfficialSourceRegistryV1,
    manifest: OfficialRequestManifestV1,
    *,
    transport: OfficialHttpTransportV1 | None = None,
    credential_provider: OfficialCredentialProviderV1 | None = None,
    previous_snapshots: Mapping[str, OfficialRawSnapshotV1] | None = None,
    clock_ns: Callable[[], int] | None = None,
    monotonic: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> OfficialFetchBundleV1:
    """Fetch every request under one shared fail-closed resource budget."""
    if registry.registry_id != manifest.registry_id:
        raise ValueError("official request manifest registry identity differs")
    request_fn = transport or cast(OfficialHttpTransportV1, requests.request)
    current_ns = clock_ns or time.time_ns
    monotonic_fn = monotonic or time.monotonic
    sleep_fn = sleeper or time.sleep
    previous = dict(previous_snapshots or {})
    started = monotonic_fn()
    deadline = started + manifest.policy.max_runtime_seconds
    total_bytes = 0
    snapshots: list[OfficialRawSnapshotV1] = []
    for request in manifest.requests:
        _remaining_official_runtime(monotonic_fn, deadline)
        source = registry.source(request.source_key)
        _validate_request_source(request, source)
        prior = previous.get(request.request_id)
        remaining_bytes = manifest.policy.max_total_bytes - total_bytes
        if remaining_bytes <= 0:
            raise OfficialFetchError(
                "official acquisition exceeds total byte bound"
            )
        snapshot = _fetch_official_request(
            request,
            source,
            manifest.policy,
            transport=request_fn,
            credential_provider=credential_provider,
            previous=prior,
            clock_ns=current_ns,
            monotonic=monotonic_fn,
            deadline=deadline,
            response_byte_limit=min(
                manifest.policy.max_response_bytes,
                remaining_bytes,
            ),
            sleeper=sleep_fn,
        )
        total_bytes += len(snapshot.content)
        if total_bytes > manifest.policy.max_total_bytes:
            raise OfficialFetchError(
                "official acquisition exceeds total byte bound"
            )
        _remaining_official_runtime(monotonic_fn, deadline)
        snapshots.append(snapshot)
    return OfficialFetchBundleV1(
        request_manifest=manifest,
        snapshots=tuple(snapshots),
    )


def environment_official_credentials(
    source: OfficialSourceEntryV1,
) -> str | None:
    """Resolve a declared API credential without placing it in any artifact."""
    if source.authentication is OfficialAuthenticationKind.NONE:
        return None
    env_name = cast(str, source.credential_env_name)
    secret = os.environ.get(env_name, "")
    if not secret:
        if source.authentication is OfficialAuthenticationKind.OPTIONAL_API_KEY:
            return None
        raise OfficialFetchError(
            f"required credential environment variable is absent: {env_name}"
        )
    return secret


def parse_official_snapshot(
    snapshot: OfficialRawSnapshotV1,
    parser: OfficialSourceParserV1,
    *,
    max_events: int,
) -> tuple[Mapping[str, JSONValue], ...]:
    """Run one exactly bound parser and enforce its normalized-event limit."""
    _bounded_int(max_events, "max_events", 1, MAX_OFFICIAL_EVENTS)
    if not isinstance(parser, OfficialSourceParserV1):
        raise TypeError("parser does not implement the official source seam")
    if (
        parser.parser_id != snapshot.request.parser_id
        or parser.parser_version != snapshot.request.parser_version
    ):
        raise ValueError("official parser identity differs from request")
    supported = tuple(
        OfficialSourceFormat.from_value(item)
        for item in parser.supported_formats
    )
    if snapshot.request.source_format not in supported:
        raise ValueError(
            "official parser does not support the requested source format"
        )
    values: list[Mapping[str, JSONValue]] = []
    for item in parser.parse(snapshot, max_events=max_events):
        if len(values) >= max_events:
            raise ValueError("official parser exceeded normalized-event bound")
        _mapping(item, "parsed event")
        if (
            len(canonical_contract_json(dict(item)).encode("utf-8"))
            > 1024 * 1024
        ):
            raise ValueError(
                "official parsed event exceeds serialized byte bound"
            )
        values.append(item)
    return tuple(values)


def write_official_fetch_bundle(
    bundle: OfficialFetchBundleV1, directory: str | Path
) -> Mapping[str, ArtifactRef]:
    """Write immutable raw blobs and a content-addressed replay manifest."""
    if not isinstance(bundle, OfficialFetchBundleV1):
        raise TypeError("official artifact writer requires a v1 fetch bundle")
    root = Path(directory).expanduser().resolve()
    source_root = root / "sources"
    source_root.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, ArtifactRef] = {}
    for snapshot in bundle.snapshots:
        source_path = source_root / f"{snapshot.content_sha256}.bin"
        _write_once(source_path, snapshot.content)
        artifacts[f"source:{snapshot.snapshot_id}"] = ArtifactRef(
            kind="official_raw_snapshot_v1",
            path=str(source_path),
            size_bytes=len(snapshot.content),
            sha256=snapshot.content_sha256,
            metadata={
                "source_key": snapshot.request.source_key,
                "source_id": snapshot.request.source_id,
                "request_id": snapshot.request.request_id,
                "snapshot_id": snapshot.snapshot_id,
            },
        )
    content = canonical_contract_json(bundle.to_dict()).encode("utf-8") + b"\n"
    digest = hashlib.sha256(content).hexdigest()
    manifest_path = root / f"official-fetch-bundle-{digest}.json"
    _write_once(manifest_path, content)
    artifacts["bundle"] = ArtifactRef(
        kind="official_fetch_bundle_v1",
        path=str(manifest_path),
        size_bytes=len(content),
        sha256=digest,
        metadata={
            "bundle_id": bundle.bundle_id,
            "request_manifest_id": bundle.request_manifest.manifest_id,
            "snapshot_count": len(bundle.snapshots),
        },
    )
    return artifacts


def replay_official_fetch_bundle(
    path: str | Path,
    *,
    source_directory: str | Path | None = None,
    registry: OfficialSourceRegistryV1 | None = None,
) -> OfficialFetchBundleV1:
    """Restore an exact fetch from retained bytes and verify every identity."""
    manifest_path = Path(path).expanduser().resolve()
    match = re.fullmatch(
        r"official-fetch-bundle-([0-9a-f]{64})\.json", manifest_path.name
    )
    if match is None:
        raise ValueError("official fetch bundle name is not content addressed")
    content = manifest_path.read_bytes()
    if len(content) > MAX_OFFICIAL_MANIFEST_BYTES:
        raise ValueError("official fetch bundle manifest exceeds size bound")
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError("official fetch bundle hash differs from name")
    try:
        payload = _mapping(json.loads(content.decode("utf-8")), "fetch bundle")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("official fetch bundle is invalid JSON") from exc
    _require_schema(payload, OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION)
    manifest_data = _mapping(
        payload.get("request_manifest"), "request_manifest"
    )
    manifest = OfficialRequestManifestV1.from_dict(manifest_data)
    if manifest_data.get("manifest_id") != manifest.manifest_id:
        raise ValueError("restored official request manifest identity differs")
    root = (
        Path(source_directory).expanduser().resolve()
        if source_directory is not None
        else manifest_path.parent / "sources"
    )
    snapshots: list[OfficialRawSnapshotV1] = []
    for item in _sequence(payload.get("snapshots"), "snapshots"):
        evidence = _mapping(item, "snapshot")
        digest = _sha256(evidence.get("content_sha256"), "content_sha256")
        source_path = root / f"{digest}.bin"
        raw = source_path.read_bytes()
        if len(raw) > manifest.policy.max_response_bytes:
            raise ValueError(
                "retained official source exceeds policy byte bound"
            )
        snapshots.append(OfficialRawSnapshotV1.restore(evidence, raw))
    bundle = OfficialFetchBundleV1(
        request_manifest=manifest,
        snapshots=tuple(snapshots),
        schema_version=str(payload.get("schema_version", "")),
    )
    if bundle.bundle_id != str(payload.get("bundle_id", "")):
        raise ValueError("restored official fetch bundle identity differs")
    if registry is not None:
        if registry.registry_id != manifest.registry_id:
            raise ValueError("replay registry identity differs")
        for request in manifest.requests:
            _validate_request_source(
                request, registry.source(request.source_key)
            )
    return bundle


def _fetch_official_request(
    request: OfficialSourceRequestV1,
    source: OfficialSourceEntryV1,
    policy: OfficialFetchPolicyV1,
    *,
    transport: OfficialHttpTransportV1,
    credential_provider: OfficialCredentialProviderV1 | None,
    previous: OfficialRawSnapshotV1 | None,
    clock_ns: Callable[[], int],
    monotonic: Callable[[], float],
    deadline: float,
    response_byte_limit: int,
    sleeper: Callable[[float], None],
) -> OfficialRawSnapshotV1:
    headers = {
        "Accept": ", ".join(source.expected_media_types),
        "Accept-Encoding": "identity",
        "User-Agent": policy.user_agent,
        **dict(request.headers),
    }
    parameters = dict(request.query_parameters)
    if request.body_content_type is not None:
        headers["Content-Type"] = request.body_content_type
    if request.if_none_match is not None:
        headers["If-None-Match"] = request.if_none_match
    if request.if_modified_since is not None:
        headers["If-Modified-Since"] = request.if_modified_since
    transport_uri = request.uri
    credential: str | None = None
    if source.authentication is not OfficialAuthenticationKind.NONE:
        provider = credential_provider or environment_official_credentials
        credential = provider(source)
        if credential is not None:
            secret = _text(credential, "credential")
            if source.credential_header_name is not None:
                headers[source.credential_header_name] = _header_value(
                    secret,
                    "credential",
                )
            elif source.credential_parameter_name is not None:
                parameters[source.credential_parameter_name] = secret
            else:
                placeholder = cast(str, source.credential_path_placeholder)
                if transport_uri.count(placeholder) != 1:
                    raise OfficialFetchError(
                        "official path credential placeholder is absent or ambiguous"
                    )
                transport_uri = transport_uri.replace(
                    placeholder,
                    quote(secret, safe=""),
                )
        elif source.credential_path_placeholder is not None:
            raise OfficialFetchError(
                "official path-authenticated request has no runtime credential"
            )
    _remaining_official_runtime(monotonic, deadline)
    retrieved = clock_ns()
    for attempt in range(1, policy.max_attempts + 1):
        response: OfficialHttpResponseV1 | None = None
        try:
            response = _request_with_bounded_redirects(
                request,
                source,
                policy,
                transport=transport,
                transport_uri=transport_uri,
                parameters=parameters,
                headers=headers,
                monotonic=monotonic,
                deadline=deadline,
            )
            status = int(response.status_code)
            if status in _RETRYABLE_STATUS_CODES:
                raise _RetryableOfficialFetchError(
                    f"retryable official HTTP status {status}"
                )
            if status not in {200, 304}:
                raise OfficialFetchError(f"official HTTP status {status}")
            response_headers = _safe_response_headers(response.headers)
            if status == 304:
                if previous is None:
                    raise OfficialFetchError(
                        "304 response has no prior retained snapshot"
                    )
                if previous.request.source_id != request.source_id:
                    raise OfficialFetchError(
                        "304 predecessor changes source identity"
                    )
                if not _same_official_request_target(previous.request, request):
                    raise OfficialFetchError(
                        "304 predecessor changes request target"
                    )
                if (
                    request.if_none_match is None
                    and request.if_modified_since is None
                ):
                    raise OfficialFetchError(
                        "304 response was not conditionally requested"
                    )
                if (
                    request.if_none_match is not None
                    and request.if_none_match != previous.etag
                ) or (
                    request.if_modified_since is not None
                    and request.if_modified_since != previous.last_modified
                ):
                    raise OfficialFetchError(
                        "304 predecessor validators differ from conditional request"
                    )
                content = previous.content
                if len(content) > response_byte_limit:
                    raise OfficialFetchError(
                        "reused official response exceeds remaining total byte bound"
                    )
                content_type = previous.content_type
                reused = previous.snapshot_id
            else:
                content = _read_bounded_response(
                    response,
                    response_headers,
                    policy,
                    byte_limit=response_byte_limit,
                    monotonic=monotonic,
                    deadline=deadline,
                )
                content_type = _response_content_type(response_headers)
                _validate_response_content(
                    source, request.source_format, content_type, content
                )
                reused = None
            resolved_uri = _sanitize_resolved_uri(
                str(response.url),
                source,
                credential=credential,
            )
            resolved_host = cast(str, urlparse(resolved_uri).hostname).lower()
            if resolved_host not in source.allowed_hosts:
                raise OfficialFetchError(
                    "official response redirected to an unregistered host"
                )
            return OfficialRawSnapshotV1(
                request=request,
                retrieved_at_ns=retrieved,
                completed_at_ns=clock_ns(),
                status_code=status,
                resolved_uri=resolved_uri,
                response_headers=response_headers,
                content=content,
                content_type=content_type,
                content_encoding=response_headers.get("content-encoding"),
                etag=response_headers.get("etag")
                or (previous.etag if status == 304 and previous else None),
                last_modified=response_headers.get("last-modified")
                or (
                    previous.last_modified
                    if status == 304 and previous
                    else None
                ),
                source_date=response_headers.get("date"),
                reused_from_snapshot_id=reused,
                attempt_count=attempt,
            )
        except (requests.RequestException, _RetryableOfficialFetchError):
            if attempt == policy.max_attempts:
                break
            delay = policy.backoff_seconds[attempt - 1]
            if delay >= _remaining_official_runtime(monotonic, deadline):
                raise OfficialFetchError(
                    "official acquisition exceeds runtime bound before retry"
                ) from None
            sleeper(delay)
            _remaining_official_runtime(monotonic, deadline)
        finally:
            if response is not None:
                response.close()
    raise OfficialFetchError(
        f"official request failed after {policy.max_attempts} attempts: {request.request_id}"
    ) from None


def _request_with_bounded_redirects(
    request: OfficialSourceRequestV1,
    source: OfficialSourceEntryV1,
    policy: OfficialFetchPolicyV1,
    *,
    transport: OfficialHttpTransportV1,
    transport_uri: str,
    parameters: Mapping[str, str],
    headers: Mapping[str, str],
    monotonic: Callable[[], float],
    deadline: float,
) -> OfficialHttpResponseV1:
    current_uri = transport_uri
    current_parameters = dict(parameters)
    for redirect_count in range(policy.max_redirects + 1):
        remaining = _remaining_official_runtime(monotonic, deadline)
        response = transport(
            request.method.value,
            current_uri,
            params=current_parameters,
            headers=headers,
            data=request.body_text,
            timeout=(
                min(policy.connect_timeout_seconds, remaining),
                min(policy.read_timeout_seconds, remaining),
            ),
            stream=True,
            allow_redirects=False,
        )
        try:
            _remaining_official_runtime(monotonic, deadline)
        except OfficialFetchError:
            response.close()
            raise
        status = int(response.status_code)
        if status not in _REDIRECT_STATUS_CODES:
            return response
        try:
            if source.authentication is not OfficialAuthenticationKind.NONE:
                raise OfficialFetchError(
                    "authenticated official requests cannot follow redirects"
                )
            if redirect_count >= policy.max_redirects:
                raise OfficialFetchError(
                    "official response exceeds redirect bound"
                )
            location = _safe_redirect_location(response.headers)
            if request.method is OfficialRequestMethod.POST and status in {
                301,
                302,
                303,
            }:
                raise OfficialFetchError(
                    "official redirect would change POST request semantics"
                )
            response_uri = _network_https_uri(
                str(response.url),
                "redirect response URI",
            )
            if cast(str, urlparse(response_uri).hostname).lower() not in (
                source.allowed_hosts
            ):
                raise OfficialFetchError(
                    "official response redirected from an unregistered host"
                )
            next_uri = _network_https_uri(
                urljoin(response_uri, location),
                "official redirect URI",
            )
            if (
                cast(str, urlparse(next_uri).hostname).lower()
                not in source.allowed_hosts
            ):
                raise OfficialFetchError(
                    "official response redirected to an unregistered host"
                )
        finally:
            response.close()
        current_uri = next_uri
        current_parameters = {}
    raise OfficialFetchError("official response exceeds redirect bound")


def _read_bounded_response(
    response: OfficialHttpResponseV1,
    headers: Mapping[str, str],
    policy: OfficialFetchPolicyV1,
    *,
    byte_limit: int,
    monotonic: Callable[[], float],
    deadline: float,
) -> bytes:
    content_encoding = (
        headers.get("content-encoding", "identity").strip().lower()
    )
    if content_encoding not in {"", "identity"}:
        raise OfficialFetchError(
            "official response uses an encoded representation; exact bytes unavailable"
        )
    declared_text = headers.get("content-length")
    declared: int | None = None
    if declared_text is not None:
        try:
            declared = int(declared_text)
        except ValueError as exc:
            raise OfficialFetchError(
                "official response Content-Length is invalid"
            ) from exc
        if declared < 0 or declared > byte_limit:
            message = (
                "official response exceeds remaining total byte bound"
                if byte_limit < policy.max_response_bytes
                else "official response exceeds declared byte bound"
            )
            raise OfficialFetchError(message)
    chunks: list[bytes] = []
    total = 0
    _remaining_official_runtime(monotonic, deadline)
    for chunk in response.iter_content(chunk_size=policy.chunk_size_bytes):
        _remaining_official_runtime(monotonic, deadline)
        if not isinstance(chunk, bytes):
            raise OfficialFetchError(
                "official response yielded non-byte content"
            )
        if not chunk:
            continue
        total += len(chunk)
        if total > byte_limit:
            message = (
                "official response exceeds remaining total byte bound"
                if byte_limit < policy.max_response_bytes
                else "official response exceeds byte bound"
            )
            raise OfficialFetchError(message)
        chunks.append(chunk)
    content = b"".join(chunks)
    if not content:
        raise OfficialFetchError("official response is empty")
    if declared is not None and declared != len(content):
        raise OfficialFetchError("official response is incomplete")
    return content


def _same_official_request_target(
    first: OfficialSourceRequestV1, second: OfficialSourceRequestV1
) -> bool:
    return replace(
        first,
        if_none_match=None,
        if_modified_since=None,
    ) == replace(
        second,
        if_none_match=None,
        if_modified_since=None,
    )


def _validate_request_source(
    request: OfficialSourceRequestV1, source: OfficialSourceEntryV1
) -> None:
    if (
        request.source_id != source.source_id
        or request.source_key != source.source_key
    ):
        raise ValueError("official request changes registered source identity")
    host = cast(str, urlparse(request.uri).hostname).lower()
    if host not in source.allowed_hosts:
        raise ValueError("official request host is not registered")
    if request.source_format not in source.formats:
        raise ValueError("official request format is not registered")
    if (
        request.parser_id != source.parser_id
        or request.parser_version != source.parser_version
    ):
        raise ValueError(
            "official request parser identity differs from registry"
        )
    if (
        source.credential_header_name is not None
        and source.credential_header_name.lower() in request.headers
    ):
        raise ValueError(
            "official request manifest contains a credential header"
        )
    if source.credential_parameter_name is not None and (
        source.credential_parameter_name.lower()
        in {name.lower() for name in request.query_parameters}
        or source.credential_parameter_name.lower()
        in {
            name.lower()
            for name, _ in parse_qsl(
                urlparse(request.uri).query,
                keep_blank_values=True,
            )
        }
    ):
        raise ValueError(
            "official request manifest contains a credential parameter"
        )
    placeholder_count = request.uri.count(OFFICIAL_CREDENTIAL_PATH_PLACEHOLDER)
    if source.credential_path_placeholder is not None:
        path_segments = urlparse(request.uri).path.split("/")
        if (
            placeholder_count != 1
            or path_segments.count(OFFICIAL_CREDENTIAL_PATH_PLACEHOLDER) != 1
        ):
            raise ValueError(
                "path-authenticated official request must contain one credential-only "
                "path segment"
            )
    elif placeholder_count:
        raise ValueError(
            "official request contains an undeclared credential placeholder"
        )


def _validate_response_content(
    source: OfficialSourceEntryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    if content_type not in source.expected_media_types:
        raise OfficialFetchError(
            "official response MIME type is not registered"
        )
    stripped = content.lstrip(b"\xef\xbb\xbf\x00\t\r\n ")
    sdmx_signature = (
        b"\x00" not in content
        if content_type in {"application/vnd.sdmx.data+csv", "text/csv"}
        else stripped.startswith((b"<", b"{", b"[", b"DATAFLOW"))
    )
    signatures = {
        OfficialSourceFormat.JSON: stripped[:1] in {b"{", b"["},
        OfficialSourceFormat.JSON_STAT: stripped[:1] in {b"{", b"["},
        OfficialSourceFormat.SDMX_21: sdmx_signature,
        OfficialSourceFormat.SDMX_30: sdmx_signature,
        OfficialSourceFormat.HTML: stripped[:256]
        .lower()
        .startswith((b"<!doctype", b"<html", b"<head", b"<body")),
        OfficialSourceFormat.PDF: content.startswith(b"%PDF-"),
        OfficialSourceFormat.XLSX: content.startswith(b"PK\x03\x04"),
        OfficialSourceFormat.ARCHIVE: content.startswith(
            (b"PK\x03\x04", b"\x1f\x8b")
        ),
        OfficialSourceFormat.XLS: content.startswith(
            b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
        ),
        OfficialSourceFormat.RSS: stripped.startswith(b"<"),
        OfficialSourceFormat.ATOM: stripped.startswith(b"<"),
        OfficialSourceFormat.ICS: stripped.startswith(b"BEGIN:VCALENDAR"),
        OfficialSourceFormat.CSV: b"\x00" not in content,
        OfficialSourceFormat.TSV: b"\x00" not in content,
        OfficialSourceFormat.DATA_CATALOG: stripped[:1] in {b"{", b"[", b"<"},
    }
    if not signatures[source_format]:
        raise OfficialFetchError(
            "official response signature does not match registry"
        )


def _safe_response_headers(values: Mapping[str, str]) -> dict[str, str]:
    """Retain bounded provenance headers while dropping cookies and secrets."""
    if len(values) > 256:
        raise OfficialFetchError("official response header count exceeds bound")
    retained = {
        str(key).lower(): str(value)
        for key, value in values.items()
        if str(key).lower() in _RETAINED_RESPONSE_HEADERS
    }
    try:
        return _header_mapping(retained, allow_sensitive=False)
    except ValueError as exc:
        raise OfficialFetchError(
            "official response headers are invalid"
        ) from exc


def _safe_redirect_location(values: Mapping[str, str]) -> str:
    matches = [
        str(value)
        for key, value in values.items()
        if str(key).lower() == "location"
    ]
    if len(matches) != 1:
        raise OfficialFetchError(
            "official redirect must contain exactly one Location"
        )
    try:
        return _header_value(matches[0], "redirect Location")
    except ValueError as exc:
        raise OfficialFetchError(
            "official redirect Location is invalid"
        ) from exc


def _sanitize_resolved_uri(
    value: str,
    source: OfficialSourceEntryV1,
    *,
    credential: str | None,
) -> str:
    """Strip runtime query/path credentials before retaining redirect evidence."""
    uri = _network_https_uri(value, "resolved_uri")
    parsed = urlparse(uri)
    if (
        source.credential_path_placeholder is not None
        and credential is not None
    ):
        encoded = quote(credential, safe="")
        path_segments = parsed.path.split("/")
        if path_segments.count(encoded) != 1:
            raise OfficialFetchError(
                "official resolved URI omits the expected path credential"
            )
        parsed = parsed._replace(
            path="/".join(
                (
                    source.credential_path_placeholder
                    if segment == encoded
                    else segment
                )
                for segment in path_segments
            )
        )
    parameter = source.credential_parameter_name
    if parameter is None:
        return urlunparse(parsed)
    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != parameter.lower()
    ]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def _response_content_type(headers: Mapping[str, str]) -> str:
    value = headers.get("content-type")
    if value is None:
        raise OfficialFetchError("official response omits Content-Type")
    return _media_type(value.split(";", 1)[0])


def _validate_primary_source_matrix(
    economies: Sequence[str], sources: Sequence[OfficialSourceEntryV1]
) -> None:
    for economy in economies:
        for family in EconomicEventFamily:
            matches = [
                item
                for item in sources
                if item.economy_code == economy
                and family in item.event_families
                and OfficialSourceRole.PRIMARY_PRODUCER in item.roles
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"official primary source matrix cell is not unique: {economy}/{family.value}"
                )


def _enum_value(
    enum_type: type[_EnumT], value: str | _EnumT, label: str
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as exc:
        raise ValueError(f"unsupported {label}") from exc


def _enum_tuple(
    enum_type: type[_EnumT], values: Iterable[str | _EnumT], label: str
) -> tuple[_EnumT, ...]:
    result = tuple(
        sorted(
            {_enum_value(enum_type, item, label) for item in values},
            key=lambda item: str(item.value),
        )
    )
    return result


def _ordered_enum_tuple(
    enum_type: type[_EnumT], values: Iterable[str | _EnumT], label: str
) -> tuple[_EnumT, ...]:
    result: list[_EnumT] = []
    for value in values:
        normalized = _enum_value(enum_type, value, label)
        if normalized not in result:
            result.append(normalized)
    return tuple(result)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _stable_digest_id(value: object, prefix: str, label: str) -> str:
    text = _text(value, label)
    if (
        re.fullmatch(rf"{re.escape(prefix)}:sha256:[0-9a-f]{{64}}", text)
        is None
    ):
        raise ValueError(f"{label} is invalid")
    return text


def _key(value: object, label: str) -> str:
    text = _text(value, label).lower()
    if _KEY_RE.fullmatch(text) is None:
        raise ValueError(f"{label} is invalid")
    return text


def _economy_code(value: object) -> str:
    text = _text(value, "economy_code").upper()
    if _ECONOMY_RE.fullmatch(text) is None:
        raise ValueError("economy_code is invalid")
    return text


def _text(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is empty")
    if len(text) > MAX_OFFICIAL_TEXT:
        raise ValueError(f"{label} exceeds text bound")
    return text


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _text(text, label) if text else None


def _optional_request_body(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("body_text must be a string")
    text = value
    if not text:
        return None
    if len(text.encode("utf-8")) > MAX_OFFICIAL_REQUEST_BODY_BYTES:
        raise ValueError("body_text exceeds byte bound")
    return text


def _expand_official_uri_template(
    template: str,
    *,
    start: str,
    end: str,
    page: int,
    page_size: int | None,
) -> str:
    replacements = {
        "{start}": quote(start, safe="-"),
        "{end}": quote(end, safe="-"),
        "{page}": str(page),
        "{page_size}": "" if page_size is None else str(page_size),
    }
    result = template
    for marker, value in replacements.items():
        result = result.replace(marker, value)
    unknown = result.replace(OFFICIAL_CREDENTIAL_PATH_PLACEHOLDER, "")
    if "{" in unknown or "}" in unknown:
        raise ValueError(
            "official URI template contains an unsupported placeholder"
        )
    return _network_https_uri(result, "planned official URI")


def _text_tuple(values: Iterable[object], label: str) -> tuple[str, ...]:
    return tuple(_text(item, label) for item in values)


def _string_tuple(values: Iterable[object], label: str) -> tuple[str, ...]:
    return tuple(sorted({_text(item, label) for item in values}))


def _key_tuple(values: Iterable[object], label: str) -> tuple[str, ...]:
    return tuple(sorted({_key(item, label) for item in values}))


def _https_uri(value: object, label: str) -> str:
    text = _text(value, label)
    parsed = urlparse(text)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{label} must be a valid HTTPS URI") from exc
    if any(ord(character) < 32 for character in text) or (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or port not in {None, 443}
    ):
        raise ValueError(
            f"{label} must be an HTTPS URI without userinfo or nonstandard ports"
        )
    return text


def _network_https_uri(value: object, label: str) -> str:
    text = _https_uri(value, label)
    if urlparse(text).fragment:
        raise ValueError(f"{label} must not contain a fragment")
    return text


def _host(value: object, label: str) -> str:
    text = _text(value, label).lower()
    if urlparse(f"https://{text}").hostname != text or "/" in text:
        raise ValueError(f"{label} contains an invalid host")
    return text


def _media_type(value: object) -> str:
    text = _text(value, "media type").lower()
    if "/" not in text or any(char.isspace() for char in text):
        raise ValueError("media type is invalid")
    return text


def _header_name(value: object) -> str:
    text = _text(value, "header name")
    if re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", text) is None:
        raise ValueError("header name is invalid")
    return text


def _string_mapping(
    values: Mapping[Any, Any], label: str, maximum: int
) -> dict[str, str]:
    if len(values) > maximum:
        raise ValueError(f"{label} exceeds item bound")
    return {
        _text(key, f"{label} key"): _text(item, f"{label} value")
        for key, item in sorted(values.items(), key=lambda pair: str(pair[0]))
    }


def _header_mapping(
    values: Mapping[Any, Any], *, allow_sensitive: bool
) -> dict[str, str]:
    result = _string_mapping(values, "headers", MAX_OFFICIAL_HEADERS)
    normalized: dict[str, str] = {}
    for key, value in result.items():
        _header_name(key)
        _header_value(value, "header value")
        lowered = key.lower()
        if not allow_sensitive and (
            lowered in _SENSITIVE_HEADERS
            or "token" in lowered
            or "secret" in lowered
        ):
            raise ValueError("sensitive headers cannot be serialized")
        normalized[lowered] = value
    return dict(sorted(normalized.items()))


def _header_value(value: object, label: str) -> str:
    text = _text(value, label)
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValueError(f"{label} contains a control character")
    return text


def _remaining_official_runtime(
    monotonic: Callable[[], float], deadline: float
) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise OfficialFetchError("official acquisition exceeds runtime bound")
    return remaining


def _iso_date(value: object, label: str) -> str:
    text = _text(value, label)
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO date") from exc


def _optional_iso_date(value: object, label: str) -> str | None:
    text = _optional_text(value, label)
    return _iso_date(text, label) if text is not None else None


def _strict_bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{label} must be boolean")
    return value


def _strict_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{label} must be an integer")
    return value


def _optional_int(value: object, label: str) -> int | None:
    return None if value is None else _strict_int(value, label)


def _strict_float(value: object, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"{label} must be numeric")
    return float(value)


def _positive_float(value: object, label: str) -> float:
    number = _strict_float(value, label)
    if not 0 < number < float("inf"):
        raise ValueError(f"{label} must be finite and positive")
    return number


def _bounded_int(value: object, label: str, minimum: int, maximum: int) -> int:
    number = _strict_int(value, label)
    if not minimum <= number <= maximum:
        raise ValueError(f"{label} is outside bounds")
    return number


def _bounded_int64(value: object, label: str) -> int:
    return _bounded_int(value, label, -(2**63), 2**63 - 1)


def _sha256(value: object, label: str) -> str:
    text = _text(value, label)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{label} is not a SHA-256 digest")
    return text


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, label: str) -> Sequence[Any]:
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"{label} must be an array")
    return cast(Sequence[Any], value)


def _strings(value: object, label: str) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value, label))


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("official source schema version differs")


def _write_once(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError("immutable official source artifact differs")
        return
    temporary = path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}"
    )
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "MAX_OFFICIAL_EVENTS",
    "MAX_OFFICIAL_MANIFEST_BYTES",
    "MAX_OFFICIAL_REQUESTS",
    "MAX_OFFICIAL_REQUEST_BODY_BYTES",
    "MAX_OFFICIAL_RESPONSE_BYTES",
    "MAX_OFFICIAL_SOURCES",
    "MAX_OFFICIAL_TOTAL_BYTES",
    "OFFICIAL_CREDENTIAL_PATH_PLACEHOLDER",
    "OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION",
    "OFFICIAL_FETCH_POLICY_SCHEMA_VERSION",
    "OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION",
    "OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION",
    "OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION",
    "OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION",
    "OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION",
    "SCOPED_ECONOMIES",
    "OfficialAuthenticationKind",
    "OfficialCredentialProviderV1",
    "OfficialFetchBundleV1",
    "OfficialFetchError",
    "OfficialFetchPolicyV1",
    "OfficialHttpResponseV1",
    "OfficialHttpTransportV1",
    "OfficialRawSnapshotV1",
    "OfficialRequestManifestV1",
    "OfficialRequestMethod",
    "OfficialSourceCapability",
    "OfficialSourceEntryV1",
    "OfficialSourceFormat",
    "OfficialSourceParserV1",
    "OfficialSourceRegistryV1",
    "OfficialSourceRequestV1",
    "OfficialSourceRole",
    "OfficialSourceTimestampV1",
    "OfficialSourceVerificationStatus",
    "build_official_request_manifest",
    "condition_official_request",
    "environment_official_credentials",
    "fetch_official_request_manifest",
    "load_packaged_official_source_registry",
    "normalize_official_source_timestamp",
    "official_source_matrix",
    "packaged_official_source_registry_path",
    "parse_official_snapshot",
    "plan_official_source_requests",
    "replay_official_fetch_bundle",
    "split_official_date_windows",
    "write_official_fetch_bundle",
]

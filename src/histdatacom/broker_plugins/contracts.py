"""Immutable, standard-library-only contracts for the public broker SDK.

The SDK describes evidence. It neither opens providers nor imports host
collectors, storage, reconstruction, certification, or experiment machinery.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import UnionType
from typing import (
    Any,
    ClassVar,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

BROKER_PLUGIN_SDK_VERSION = "1.0.0"
MAX_SDK_ARTIFACT_BYTES = 65_536
MAX_EXTENSION_BYTES = 8_192
MAX_SDK_COLLECTION_ITEMS = 128
_INT64_MAX = 2**63 - 1
_SHA256 = re.compile(r"[a-f0-9]{64}\Z")
_NAMESPACE = re.compile(r"[a-z][a-z0-9]*(?:[.-][a-z][a-z0-9_]*)+\Z")
_SEMVER = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_PRICE = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_CURRENCY = re.compile(r"[A-Z]{3}\Z")
_SYMBOL = re.compile(r"[A-Z]{6}\Z")
# Scan each maximal scheme-character run once, not once per possible letter.
# A run containing any letter has the same credential-bearing suffix as the
# former letter-start pattern, including digit/punctuation-prefixed schemes.
_SECRET_TEXT = re.compile(
    r"(?i)(?:\bbearer\s+\S{8,}|-----BEGIN .*PRIVATE KEY-----|"
    r"(?<![a-z0-9+.-])(?=[a-z0-9+.-]*[a-z])"
    r"[a-z0-9+.-]*://[^\s/:]+:[^\s/@]+@)"
)
_SECRET_KEYS = frozenset(
    {
        "accesstoken",
        "apikey",
        "authorization",
        "bearertoken",
        "clientsecret",
        "credential",
        "credentials",
        "password",
        "passwd",
        "privatekey",
        "refreshtoken",
        "secret",
        "token",
    }
)
_ArtifactT = TypeVar("_ArtifactT", bound="_Artifact")


def _text(value: str, name: str, maximum: int = 256) -> None:
    if not value or value != value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be nonempty bounded text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{name} cannot contain control characters")
    if _SECRET_TEXT.search(value):
        raise ValueError(f"{name} contains credential-like text")


def _ns(value: int, name: str, *, positive: bool = False) -> None:
    if not (int(positive) <= value <= _INT64_MAX):
        raise ValueError(f"{name} is outside the nonnegative int64 range")


def _identity(value: str, kind: str) -> None:
    prefix = f"broker-plugin-{kind}:sha256:"
    if not value.startswith(prefix) or not _SHA256.fullmatch(
        value[len(prefix) :]
    ):
        raise ValueError(f"invalid {kind} artifact identity")


def _decimal(value: str, name: str, *, positive: bool = False) -> Decimal:
    if len(value) > 80 or not _PRICE.fullmatch(value):
        raise ValueError(f"{name} requires an unsigned decimal lexeme")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} requires a finite decimal") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError(f"{name} has an invalid sign or magnitude")
    return result


def _semver(value: str) -> None:
    match = _SEMVER.fullmatch(value)
    if len(value) > 128 or match is None:
        raise ValueError("plugin_version must follow SemVer")
    prerelease = match.group(4)
    if prerelease and any(
        part.isdigit() and len(part) > 1 and part.startswith("0")
        for part in prerelease.split(".")
    ):
        raise ValueError(
            "numeric SemVer prerelease identifiers need no leading zero"
        )


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object member")
        result[key] = value
    return result


def _check_json(value: object, depth: int = 0) -> None:
    if depth > 16:
        raise ValueError("JSON nesting exceeds the SDK limit")
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if abs(value) > _INT64_MAX:
            raise ValueError("JSON integer exceeds int64")
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("non-finite JSON numbers are forbidden")
        return
    if type(value) is str:
        if len(value) > MAX_EXTENSION_BYTES:
            raise ValueError("JSON text exceeds the SDK limit")
        if _SECRET_TEXT.search(value):
            raise ValueError("JSON contains credential-like text")
        return
    if isinstance(value, dict):
        if len(value) > MAX_SDK_COLLECTION_ITEMS:
            raise ValueError("JSON object exceeds the SDK member limit")
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("JSON member names must be strings")
            _check_json(key, depth + 1)
            _check_json(item, depth + 1)
        return
    if isinstance(value, list):
        if len(value) > MAX_SDK_COLLECTION_ITEMS:
            raise ValueError("JSON array exceeds the SDK item limit")
        for item in value:
            _check_json(item, depth + 1)
        return
    raise ValueError("unsupported JSON value")


def _check_extension_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = re.sub(r"[^a-z]", "", key.lower())
            if normalized in _SECRET_KEYS:
                raise ValueError("extension contains a credential-like key")
            _check_extension_keys(item)
    elif isinstance(value, list):
        for item in value:
            _check_extension_keys(item)


def _parse_json(text: str, maximum: int = MAX_SDK_ARTIFACT_BYTES) -> object:
    if type(text) is not str or len(text.encode("utf-8")) > maximum:
        raise ValueError("JSON exceeds the SDK byte limit")
    try:
        result: object = json.loads(text, object_pairs_hook=_object_pairs)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid SDK JSON") from exc
    _check_json(result)
    return result


def canonical_broker_sdk_json(value: Mapping[str, object]) -> str:
    """Encode bounded JSON deterministically; NaN and coercion are forbidden."""
    data = dict(value)
    _check_json(data)
    result = json.dumps(
        data,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(result.encode("utf-8")) > MAX_SDK_ARTIFACT_BYTES:
        raise ValueError("SDK artifact exceeds byte limit")
    return result


def _wire(value: object, depth: int = 0) -> object:
    if depth > 16:
        raise ValueError("SDK field nesting exceeds the SDK limit")
    if isinstance(value, _Artifact):
        return value.to_dict()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        if len(value) > MAX_SDK_COLLECTION_ITEMS:
            raise ValueError("SDK field collection exceeds the SDK item limit")
        return [_wire(item, depth + 1) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise ValueError("SDK fields must be typed immutable values")


def _decode(annotation: object, value: object) -> object:
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        for option in get_args(annotation):
            try:
                return _decode(option, value)
            except (ValueError, TypeError):
                pass
        raise ValueError("SDK field does not match its declared optional type")
    if annotation is type(None):
        if value is not None:
            raise ValueError("expected null")
        return None
    if origin is tuple:
        if not isinstance(value, list) or len(value) > MAX_SDK_COLLECTION_ITEMS:
            raise ValueError("expected bounded JSON array")
        return tuple(_decode(get_args(annotation)[0], item) for item in value)
    if annotation in (str, int, bool):
        if type(value) is not annotation:
            raise ValueError("SDK scalar has the wrong exact type")
        return value
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if type(value) is not str:
            raise ValueError("SDK enums require strings")
        return annotation(value)
    if isinstance(annotation, type) and issubclass(annotation, _Artifact):
        if not isinstance(value, dict):
            raise TypeError("SDK child artifacts require objects")
        return annotation.from_dict(value)
    raise ValueError("unsupported SDK field type")


class _Artifact:
    """One strict versioned wire artifact with a derived content identity."""

    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        for item in fields(cast(Any, self)):
            value = _decode(hints[item.name], _wire(getattr(self, item.name)))
            object.__setattr__(self, item.name, value)
        self._validate()
        self.to_json()  # Apply the total artifact bound at construction too.

    def _validate(self) -> None:
        raise NotImplementedError

    @property
    def schema_version(self) -> str:
        return f"histdatacom.broker-plugin.{self.KIND}.v1"

    def identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **{
                item.name: _wire(getattr(self, item.name))
                for item in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        digest = hashlib.sha256(
            canonical_broker_sdk_json(self.identity_payload()).encode("utf-8")
        ).hexdigest()
        return f"broker-plugin-{self.KIND}:sha256:{digest}"

    def to_dict(self) -> dict[str, object]:
        """Return an independent mutable wire copy of this immutable object."""
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        return canonical_broker_sdk_json(self.to_dict())

    @classmethod
    def from_dict(  # noqa: PYI019
        cls: type[_ArtifactT], data: Mapping[str, object]
    ) -> _ArtifactT:
        # A bound TypeVar preserves Python 3.10 without typing_extensions.
        names = {item.name for item in fields(cast(Any, cls))}
        if set(data) != names | {"schema_version", "artifact_id"}:
            raise ValueError("SDK artifact fields are missing or unknown")
        if data["schema_version"] != f"histdatacom.broker-plugin.{cls.KIND}.v1":
            raise ValueError("unsupported SDK artifact schema")
        hints = get_type_hints(cls)
        kwargs = {name: _decode(hints[name], data[name]) for name in names}
        instance = cast(_ArtifactT, cast(Any, cls)(**kwargs))
        if data["artifact_id"] != instance.artifact_id:
            raise ValueError("SDK artifact identity mismatch")
        return instance

    @classmethod
    def from_json(  # noqa: PYI019
        cls: type[_ArtifactT], text: str
    ) -> _ArtifactT:
        data = _parse_json(text)
        if not isinstance(data, dict):
            raise TypeError("SDK artifact JSON requires an object")
        return cls.from_dict(data)


@dataclass(frozen=True, slots=True)
class BrokerExtensionV1(_Artifact):
    """Optional namespaced data; payload JSON is canonical and immutable."""

    namespace: str
    payload_json: str
    KIND: ClassVar[str] = "extension"

    def _validate(self) -> None:
        if len(self.namespace) > 128 or not _NAMESPACE.fullmatch(
            self.namespace
        ):
            raise ValueError("extensions require a namespaced identifier")
        if re.match(
            r"(?:histdatacom|broker[.-]plugin)(?:[.-]|$)", self.namespace
        ):
            raise ValueError("host and SDK extension namespaces are reserved")
        payload = _parse_json(self.payload_json, MAX_EXTENSION_BYTES)
        if not isinstance(payload, dict):
            raise TypeError("extension payload requires a JSON object")
        _check_extension_keys(payload)
        canonical = canonical_broker_sdk_json(payload)
        if len(canonical.encode()) > MAX_EXTENSION_BYTES:
            raise ValueError("canonical extension exceeds byte limit")
        object.__setattr__(self, "payload_json", canonical)

    def payload(self) -> dict[str, object]:
        """Return a detached copy; changing it cannot change the extension."""
        return cast(dict[str, object], _parse_json(self.payload_json))


def _extensions(values: tuple[BrokerExtensionV1, ...]) -> None:
    names = [value.namespace for value in values]
    if len(names) != len(set(names)):
        raise ValueError("extension namespaces must be unique")


@dataclass(frozen=True, slots=True)
class BrokerPluginMetadataV1(_Artifact):
    plugin_id: str
    plugin_version: str
    display_name: str
    sdk_version: str = BROKER_PLUGIN_SDK_VERSION
    extensions: tuple[BrokerExtensionV1, ...] = ()
    KIND: ClassVar[str] = "metadata"

    def _validate(self) -> None:
        if len(self.plugin_id) > 128 or not _NAMESPACE.fullmatch(
            self.plugin_id
        ):
            raise ValueError("plugin_id requires a namespaced identifier")
        _semver(self.plugin_version)
        _text(self.display_name, "display_name")
        if self.sdk_version != BROKER_PLUGIN_SDK_VERSION:
            raise ValueError("unsupported broker SDK version")
        _extensions(self.extensions)


class BrokerConfigurationType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"


@dataclass(frozen=True, slots=True)
class BrokerConfigurationFieldV1(_Artifact):
    name: str
    value_type: BrokerConfigurationType
    description: str
    required: bool = True
    secret: bool = False
    choices: tuple[str, ...] = ()
    KIND: ClassVar[str] = "configuration-field"

    def _validate(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name):
            raise ValueError("invalid configuration field name")
        _text(self.description, "configuration description", 512)
        if self.choices and (
            self.secret or self.value_type is not BrokerConfigurationType.STRING
        ):
            raise ValueError("choices require a non-secret string field")
        if len(set(self.choices)) != len(self.choices):
            raise ValueError("configuration choices must be unique")
        for value in self.choices:
            _text(value, "configuration choice")


@dataclass(frozen=True, slots=True)
class BrokerConfigurationSchemaV1(_Artifact):
    fields: tuple[BrokerConfigurationFieldV1, ...]
    KIND: ClassVar[str] = "configuration-schema"

    def _validate(self) -> None:
        names = [item.name for item in self.fields]
        if len(set(names)) != len(names):
            raise ValueError("configuration field names must be unique")

    def validate_configuration(self, values: Mapping[str, object]) -> None:
        """Validate ephemeral configuration without serializing its values."""
        known = {item.name: item for item in self.fields}
        if set(values) - set(known):
            raise ValueError("unknown configuration field")
        for name, declaration in known.items():
            if name not in values:
                if declaration.required:
                    raise ValueError("required configuration field is missing")
                continue
            value = values[name]
            permitted: tuple[type, ...] = {
                BrokerConfigurationType.STRING: (str,),
                BrokerConfigurationType.INTEGER: (int,),
                BrokerConfigurationType.NUMBER: (int, float),
                BrokerConfigurationType.BOOLEAN: (bool,),
            }[declaration.value_type]
            if type(value) not in permitted:
                raise ValueError("configuration field has the wrong type")
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and (abs(value) > _INT64_MAX or not math.isfinite(value))
            ):
                raise ValueError("configuration number is outside SDK bounds")
            if isinstance(value, str):
                if len(value) > MAX_EXTENSION_BYTES:
                    raise ValueError("configuration string exceeds SDK bounds")
                if declaration.choices and value not in declaration.choices:
                    raise ValueError("configuration choice is unsupported")


@dataclass(frozen=True, slots=True)
class BrokerInstrumentV1(_Artifact):
    symbol: str
    provider_symbol: str
    base_currency: str
    quote_currency: str
    price_increment: str | None = None
    KIND: ClassVar[str] = "instrument"

    def _validate(self) -> None:
        if not _CURRENCY.fullmatch(
            self.base_currency
        ) or not _CURRENCY.fullmatch(self.quote_currency):
            raise ValueError(
                "FX currencies require uppercase three-letter codes"
            )
        if (
            self.base_currency == self.quote_currency
            or self.symbol != self.base_currency + self.quote_currency
        ):
            raise ValueError(
                "symbol must equal distinct base and quote currencies"
            )
        _text(self.provider_symbol, "provider_symbol")
        if self.price_increment is not None:
            _decimal(self.price_increment, "price_increment", positive=True)


@dataclass(frozen=True, slots=True)
class BrokerSessionV1(_Artifact):
    metadata_id: str
    instance_nonce: str
    opened_at_utc_ns: int
    receive_clock_id: str
    KIND: ClassVar[str] = "session"

    def _validate(self) -> None:
        _identity(self.metadata_id, "metadata")
        if not re.fullmatch(r"[a-f0-9]{32}", self.instance_nonce):
            raise ValueError(
                "instance_nonce requires a fresh public 128-bit nonce"
            )
        _ns(self.opened_at_utc_ns, "opened_at_utc_ns")
        _text(self.receive_clock_id, "receive_clock_id")


class BrokerSourceTimeSemantics(str, Enum):
    BROKER_EVENT = "broker_event"
    EXCHANGE_EVENT = "exchange_event"
    ADAPTER_RECEIVE = "adapter_receive"


@dataclass(frozen=True, slots=True)
class BrokerSourceTimeV1(_Artifact):
    timestamp_ns: int
    precision_ns: int
    semantics: BrokerSourceTimeSemantics
    KIND: ClassVar[str] = "source-time"

    def _validate(self) -> None:
        _ns(self.timestamp_ns, "timestamp_ns")
        _ns(self.precision_ns, "precision_ns", positive=True)


@dataclass(frozen=True, slots=True)
class BrokerReceiveTimeV1(_Artifact):
    """Plugin transport observation, never an asserted host capture timestamp."""

    utc_ns: int
    monotonic_ns: int
    clock_id: str
    KIND: ClassVar[str] = "receive-time"

    def _validate(self) -> None:
        _ns(self.utc_ns, "utc_ns")
        _ns(self.monotonic_ns, "monotonic_ns")
        _text(self.clock_id, "clock_id")


class BrokerSizeSemantics(str, Enum):
    QUOTED_SIZE = "quoted_size"
    BROKER_SPECIFIC = "broker_specific"


@dataclass(frozen=True, slots=True)
class BrokerSizeV1(_Artifact):
    value: str
    semantics: BrokerSizeSemantics
    unit: str
    KIND: ClassVar[str] = "size"

    def _validate(self) -> None:
        _decimal(self.value, "size")
        _text(self.unit, "size unit", 64)


class BrokerActivitySemantics(str, Enum):
    MESSAGE_COUNT = "message_count"
    BROKER_ACTIVITY = "broker_activity"
    LIQUIDITY_PROXY = "liquidity_proxy"


@dataclass(frozen=True, slots=True)
class BrokerActivityV1(_Artifact):
    value: str
    semantics: BrokerActivitySemantics
    unit: str
    KIND: ClassVar[str] = "activity"

    def _validate(self) -> None:
        number = _decimal(self.value, "activity")
        _text(self.unit, "activity unit", 64)
        if self.semantics is BrokerActivitySemantics.MESSAGE_COUNT and (
            number != number.to_integral_value() or self.unit != "message"
        ):
            raise ValueError(
                "message count requires an integer and message unit"
            )


@dataclass(frozen=True, slots=True)
class BrokerQuoteV1(_Artifact):
    symbol: str
    bid: str
    ask: str
    bid_size: BrokerSizeV1 | None = None
    ask_size: BrokerSizeV1 | None = None
    activity: BrokerActivityV1 | None = None
    KIND: ClassVar[str] = "quote"

    def _validate(self) -> None:
        if (
            not _SYMBOL.fullmatch(self.symbol)
            or self.symbol[:3] == self.symbol[3:]
        ):
            raise ValueError("quote symbol requires a canonical FX pair")
        bid = _decimal(self.bid, "bid", positive=True)
        ask = _decimal(self.ask, "ask", positive=True)
        if bid > ask:
            raise ValueError("quote bid cannot exceed ask")


class BrokerGapScope(str, Enum):
    SOURCE = "source"
    TRANSPORT = "transport"
    COLLECTOR = "collector"


@dataclass(frozen=True, slots=True)
class BrokerGapV1(_Artifact):
    scope: BrokerGapScope
    started_at_utc_ns: int
    ended_at_utc_ns: int | None = None
    missing_message_count: int | None = None
    KIND: ClassVar[str] = "gap"

    def _validate(self) -> None:
        _ns(self.started_at_utc_ns, "gap start")
        if self.ended_at_utc_ns is not None:
            _ns(self.ended_at_utc_ns, "gap end")
            if self.ended_at_utc_ns < self.started_at_utc_ns:
                raise ValueError("gap ends before it starts")
        if self.missing_message_count is not None:
            _ns(self.missing_message_count, "missing_message_count")


class BrokerReasonCode(str, Enum):
    HEALTHY = "healthy"
    INVALID_CONFIGURATION = "invalid_configuration"
    AUTHENTICATION_REFUSED = "authentication_refused"
    UNSUPPORTED_INSTRUMENT = "unsupported_instrument"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    TRANSPORT_FAILURE = "transport_failure"
    TIMEOUT = "timeout"
    SOURCE_GAP = "source_gap"
    CLOCK_DISCONTINUITY = "clock_discontinuity"
    POLICY_REFUSAL = "policy_refusal"
    RESOURCE_LIMIT = "resource_limit"
    INVALID_SOURCE_MESSAGE = "invalid_source_message"
    INTERNAL_FAILURE = "internal_failure"


class BrokerDiagnosticSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class BrokerDiagnosticV1(_Artifact):
    code: BrokerReasonCode
    severity: BrokerDiagnosticSeverity
    summary: str
    retryable: bool = False
    KIND: ClassVar[str] = "diagnostic"

    def _validate(self) -> None:
        _text(self.summary, "diagnostic summary", 512)
        if (
            self.code is BrokerReasonCode.HEALTHY
            and self.severity is BrokerDiagnosticSeverity.ERROR
        ):
            raise ValueError("healthy cannot be an error diagnostic")


@dataclass(frozen=True, slots=True)
class BrokerRawProvenanceV1(_Artifact):
    """Permitted raw-message hash evidence; this grants no retention rights."""

    sha256: str
    byte_count: int
    media_type: str
    policy_id: str
    KIND: ClassVar[str] = "raw-provenance"

    def _validate(self) -> None:
        if not _SHA256.fullmatch(self.sha256):
            raise ValueError("raw provenance requires SHA-256")
        _ns(self.byte_count, "byte_count")
        if not re.fullmatch(r"[a-z0-9.+-]+/[a-z0-9.+-]+", self.media_type):
            raise ValueError(
                "media_type requires a MIME type without parameters"
            )
        _text(self.policy_id, "policy_id")


class BrokerEventKind(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    QUOTE = "quote"
    HEARTBEAT = "heartbeat"
    HEALTH = "health"
    GAP = "gap"
    CLOCK_CORRECTION = "clock_correction"


@dataclass(frozen=True, slots=True)
class BrokerEventV1(_Artifact):
    session_id: str
    sequence: int
    kind: BrokerEventKind
    connection_id: str
    source_time: BrokerSourceTimeV1 | None = None
    receive_time: BrokerReceiveTimeV1 | None = None
    instrument: str | None = None
    quote: BrokerQuoteV1 | None = None
    gap: BrokerGapV1 | None = None
    diagnostic: BrokerDiagnosticV1 | None = None
    raw_provenance: BrokerRawProvenanceV1 | None = None
    extensions: tuple[BrokerExtensionV1, ...] = ()
    KIND: ClassVar[str] = "event"

    def _validate(self) -> None:
        _identity(self.session_id, "session")
        _ns(self.sequence, "sequence")
        _text(self.connection_id, "connection_id")
        if self.instrument is not None and (
            not _SYMBOL.fullmatch(self.instrument)
            or self.instrument[:3] == self.instrument[3:]
        ):
            raise ValueError("event instrument requires a canonical FX pair")
        if (self.kind is BrokerEventKind.QUOTE) != (self.quote is not None):
            raise ValueError("only quote events carry a required quote")
        if self.quote is not None and self.instrument != self.quote.symbol:
            raise ValueError("event instrument and quote symbol differ")
        if (self.kind is BrokerEventKind.GAP) != (self.gap is not None):
            raise ValueError("only gap events carry required gap evidence")
        if (
            self.kind
            in (BrokerEventKind.SUBSCRIBED, BrokerEventKind.UNSUBSCRIBED)
            and self.instrument is None
        ):
            raise ValueError("subscription events require an instrument")
        if (
            self.kind
            in (
                BrokerEventKind.HEALTH,
                BrokerEventKind.GAP,
                BrokerEventKind.RECONNECTING,
                BrokerEventKind.CLOCK_CORRECTION,
            )
            and self.diagnostic is None
        ):
            raise ValueError(
                "health, gap, reconnect, and clock events require diagnostics"
            )
        if self.kind is BrokerEventKind.CLOCK_CORRECTION and (
            self.diagnostic is None
            or self.diagnostic.code is not BrokerReasonCode.CLOCK_DISCONTINUITY
            or self.receive_time is None
        ):
            raise ValueError(
                "clock corrections require clock evidence and reason"
            )
        _extensions(self.extensions)

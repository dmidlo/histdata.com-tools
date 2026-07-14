"""Versioned contracts for append-only live broker delivery capture.

Broker capture is measurement evidence, not synthetic output.  These contracts
keep source timestamps, collector wall-clock timestamps, monotonic ordering,
connection health, and public adapter provenance explicit while structurally
excluding credentials and raw secret-bearing configuration.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, TypeVar, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONValue

BROKER_ADAPTER_MESSAGE_SCHEMA_VERSION = "histdatacom.broker-adapter-message.v1"
BROKER_CAPTURE_SESSION_SCHEMA_VERSION = "histdatacom.broker-capture-session.v1"
BROKER_CAPTURE_EVENT_SCHEMA_VERSION = "histdatacom.broker-capture-event.v1"
BROKER_CAPTURE_STORAGE_POLICY_SCHEMA_VERSION = (
    "histdatacom.broker-capture-storage-policy.v1"
)
BROKER_CAPTURE_PARTITION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.broker-capture-partition-manifest.v1"
)
BROKER_CAPTURE_SESSION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.broker-capture-session-manifest.v1"
)
BROKER_CAPTURE_REPLAY_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.broker-capture-replay-summary.v1"
)

BROKER_CAPTURE_COLLECTOR_ID = "histdatacom.broker-capture"
BROKER_CAPTURE_COLLECTOR_VERSION = "1.0.0"
BROKER_CAPTURE_DATA_ARTIFACT_KIND = "broker_capture_jsonl"

INT64_MAX = 2**63 - 1
MAX_CAPTURE_TEXT = 1024
MAX_CAPTURE_METADATA_BYTES = 32_768
MAX_CAPTURE_KIND_COUNTS = 64
MAX_CAPTURE_PARTITIONS = 65_536
MAX_CAPTURE_MANIFEST_BYTES = 16_777_216

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9._:-]{3,32}$")
_SEMVER_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$")
_PRICE_LEXEME_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
_SENSITIVE_KEY_PARTS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "oauth",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "session_key",
        "token",
    }
)
_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s/:]+:[^\s/@]+@"),
)
_EnumT = TypeVar("_EnumT", bound=Enum)


class BrokerCaptureEventKind(str, Enum):
    """First-class quote, lifecycle, health, and clock evidence."""

    PROCESS_START = "process_start"
    PROCESS_STOP = "process_stop"
    PROCESS_RESTART = "process_restart"
    CONNECTION_OPEN = "connection_open"
    CONNECTION_CLOSE = "connection_close"
    RECONNECT = "reconnect"
    SUBSCRIPTION_ADD = "subscription_add"
    SUBSCRIPTION_REMOVE = "subscription_remove"
    QUOTE = "quote"
    HEARTBEAT = "heartbeat"
    GAP = "gap"
    OUTAGE_START = "outage_start"
    OUTAGE_END = "outage_end"
    CLOCK_CORRECTION = "clock_correction"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureEventKind"
    ) -> "BrokerCaptureEventKind":
        """Return a strict normalized event kind."""
        return _enum_value(cls, value, "broker capture event kind")


class BrokerCaptureSizeSemantics(str, Enum):
    """Meaning of optional bid/ask size values."""

    UNAVAILABLE = "unavailable"
    QUOTED_SIZE = "quoted_size"
    BROKER_SPECIFIC = "broker_specific"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureSizeSemantics"
    ) -> "BrokerCaptureSizeSemantics":
        """Return strict size semantics."""
        return _enum_value(cls, value, "broker capture size semantics")


class BrokerCapturePriceTextSemantics(str, Enum):
    """Provenance of exact quote lexemes used to measure feed precision."""

    UNAVAILABLE = "unavailable"
    SOURCE_LEXEME = "source_lexeme"
    ADAPTER_RENDERED = "adapter_rendered"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCapturePriceTextSemantics"
    ) -> "BrokerCapturePriceTextSemantics":
        """Return strict price-text semantics."""
        return _enum_value(cls, value, "broker capture price-text semantics")


class BrokerCaptureSourceTimestampSemantics(str, Enum):
    """Origin of an optional source-side event timestamp."""

    UNAVAILABLE = "unavailable"
    BROKER_EVENT = "broker_event"
    EXCHANGE_EVENT = "exchange_event"
    ADAPTER_RECEIVE = "adapter_receive"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureSourceTimestampSemantics"
    ) -> "BrokerCaptureSourceTimestampSemantics":
        """Return strict source-timestamp semantics."""
        return _enum_value(
            cls, value, "broker capture source-timestamp semantics"
        )


class BrokerCaptureActivitySemantics(str, Enum):
    """Meaning of an optional broker activity field."""

    UNAVAILABLE = "unavailable"
    MESSAGE_COUNT = "message_count"
    BROKER_ACTIVITY = "broker_activity"
    LIQUIDITY_PROXY = "liquidity_proxy"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureActivitySemantics"
    ) -> "BrokerCaptureActivitySemantics":
        """Return strict activity semantics."""
        return _enum_value(cls, value, "broker capture activity semantics")


class BrokerCaptureSessionState(str, Enum):
    """Publication state for a capture session."""

    OPEN = "open"
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureSessionState"
    ) -> "BrokerCaptureSessionState":
        """Return a strict normalized session state."""
        return _enum_value(cls, value, "broker capture session state")


class BrokerCaptureRetentionMode(str, Enum):
    """Fail-closed retention behavior for immutable capture evidence."""

    REFUSE = "refuse"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureRetentionMode"
    ) -> "BrokerCaptureRetentionMode":
        """Return the supported immutable retention mode."""
        return _enum_value(cls, value, "broker capture retention mode")


class BrokerCaptureBackpressureMode(str, Enum):
    """High-watermark behavior for the synchronous collector seam."""

    REFUSE = "refuse"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureBackpressureMode"
    ) -> "BrokerCaptureBackpressureMode":
        """Return the supported fail-closed backpressure mode."""
        return _enum_value(cls, value, "broker capture backpressure mode")


@dataclass(frozen=True, slots=True)
class BrokerAdapterMessageV1:
    """One public, credential-free message emitted by a broker adapter."""

    kind: BrokerCaptureEventKind
    source_event_time_ns: int | None = None
    source_timestamp_semantics: BrokerCaptureSourceTimestampSemantics = (
        BrokerCaptureSourceTimestampSemantics.UNAVAILABLE
    )
    source_timestamp_precision_ns: int | None = None
    source_sequence: int | None = None
    source_message_id: str | None = None
    source_batch_id: str | None = None
    symbol: str | None = None
    bid: float | None = None
    ask: float | None = None
    bid_text: str | None = None
    ask_text: str | None = None
    price_text_semantics: BrokerCapturePriceTextSemantics = (
        BrokerCapturePriceTextSemantics.UNAVAILABLE
    )
    bid_size: float | None = None
    ask_size: float | None = None
    size_semantics: BrokerCaptureSizeSemantics = (
        BrokerCaptureSizeSemantics.UNAVAILABLE
    )
    activity_value: float | None = None
    activity_semantics: BrokerCaptureActivitySemantics = (
        BrokerCaptureActivitySemantics.UNAVAILABLE
    )
    connection_id: str | None = None
    subscription_id: str | None = None
    gap_duration_ns: int | None = None
    reason_code: str | None = None
    raw_message_sha256: str | None = None
    public_metadata: dict[str, JSONValue] = field(default_factory=dict)
    message_id: str = ""
    schema_version: str = BROKER_ADAPTER_MESSAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_ADAPTER_MESSAGE_SCHEMA_VERSION:
            raise ValueError("unsupported broker adapter message schema")
        object.__setattr__(
            self, "kind", BrokerCaptureEventKind.from_value(self.kind)
        )
        object.__setattr__(
            self,
            "source_event_time_ns",
            _optional_nonnegative_int64(
                self.source_event_time_ns, "source_event_time_ns"
            ),
        )
        object.__setattr__(
            self,
            "source_timestamp_semantics",
            BrokerCaptureSourceTimestampSemantics.from_value(
                self.source_timestamp_semantics
            ),
        )
        object.__setattr__(
            self,
            "source_timestamp_precision_ns",
            _optional_nonnegative_int64(
                self.source_timestamp_precision_ns,
                "source_timestamp_precision_ns",
            ),
        )
        object.__setattr__(
            self,
            "source_sequence",
            _optional_nonnegative_int64(
                self.source_sequence, "source_sequence"
            ),
        )
        for name in (
            "source_message_id",
            "source_batch_id",
            "connection_id",
            "subscription_id",
            "reason_code",
        ):
            object.__setattr__(
                self,
                name,
                _optional_canonical_id(getattr(self, name), name),
            )
        symbol = _optional_symbol(self.symbol)
        object.__setattr__(self, "symbol", symbol)
        for name in ("bid", "ask", "bid_size", "ask_size", "activity_value"):
            object.__setattr__(
                self,
                name,
                _optional_nonnegative_float(getattr(self, name), name),
            )
        for name in ("bid_text", "ask_text"):
            object.__setattr__(
                self, name, _optional_price_lexeme(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "price_text_semantics",
            BrokerCapturePriceTextSemantics.from_value(
                self.price_text_semantics
            ),
        )
        object.__setattr__(
            self,
            "size_semantics",
            BrokerCaptureSizeSemantics.from_value(self.size_semantics),
        )
        object.__setattr__(
            self,
            "activity_semantics",
            BrokerCaptureActivitySemantics.from_value(self.activity_semantics),
        )
        object.__setattr__(
            self,
            "gap_duration_ns",
            _optional_nonnegative_int64(
                self.gap_duration_ns, "gap_duration_ns"
            ),
        )
        raw_hash = _optional_sha256(
            self.raw_message_sha256, "raw_message_sha256"
        )
        object.__setattr__(self, "raw_message_sha256", raw_hash)
        metadata = _secret_free_metadata(self.public_metadata)
        object.__setattr__(self, "public_metadata", metadata)
        self._validate_kind_fields()
        expected = _stable_id("broker-message", self.identity_payload())
        supplied = _optional_text(self.message_id)
        if supplied is not None and supplied != expected:
            raise ValueError("message_id does not match deterministic identity")
        object.__setattr__(self, "message_id", expected)

    def _validate_kind_fields(self) -> None:
        timestamp_present = self.source_event_time_ns is not None
        if timestamp_present:
            if (
                self.source_timestamp_semantics
                is BrokerCaptureSourceTimestampSemantics.UNAVAILABLE
            ):
                raise ValueError(
                    "source timestamps require explicit timestamp semantics"
                )
            if not self.source_timestamp_precision_ns:
                raise ValueError(
                    "source timestamps require positive timestamp precision"
                )
        elif (
            self.source_timestamp_semantics
            is not BrokerCaptureSourceTimestampSemantics.UNAVAILABLE
            or self.source_timestamp_precision_ns is not None
        ):
            raise ValueError(
                "source timestamp metadata requires a source timestamp"
            )
        quote_values = (self.bid, self.ask)
        if self.kind is BrokerCaptureEventKind.QUOTE:
            if self.symbol is None or any(
                value is None for value in quote_values
            ):
                raise ValueError("quote messages require symbol, bid, and ask")
            if cast(float, self.bid) <= 0 or cast(float, self.ask) <= 0:
                raise ValueError("quote prices must be positive")
            if cast(float, self.bid) > cast(float, self.ask):
                raise ValueError("quote bid must not exceed ask")
        elif any(value is not None for value in quote_values):
            raise ValueError("non-quote messages cannot contain bid or ask")

        text_present = self.bid_text is not None or self.ask_text is not None
        if text_present:
            if self.kind is not BrokerCaptureEventKind.QUOTE:
                raise ValueError("price lexemes are only valid on quotes")
            if self.bid_text is None or self.ask_text is None:
                raise ValueError(
                    "bid_text and ask_text must be supplied together"
                )
            if (
                self.price_text_semantics
                is BrokerCapturePriceTextSemantics.UNAVAILABLE
            ):
                raise ValueError(
                    "price lexemes require explicit price-text semantics"
                )
            if Decimal(self.bid_text) != Decimal(str(self.bid)) or Decimal(
                self.ask_text
            ) != Decimal(str(self.ask)):
                raise ValueError("price lexemes do not match numeric quotes")
        elif (
            self.price_text_semantics
            is not BrokerCapturePriceTextSemantics.UNAVAILABLE
        ):
            raise ValueError("price-text semantics require quote lexemes")

        sizes_present = self.bid_size is not None or self.ask_size is not None
        if sizes_present and self.kind is not BrokerCaptureEventKind.QUOTE:
            raise ValueError("sizes are only valid on quote messages")
        if (
            sizes_present
            and self.size_semantics is BrokerCaptureSizeSemantics.UNAVAILABLE
        ):
            raise ValueError("size values require explicit size semantics")
        if (
            not sizes_present
            and self.size_semantics
            is not BrokerCaptureSizeSemantics.UNAVAILABLE
        ):
            raise ValueError("size semantics require at least one size value")
        if (
            self.activity_value is None
            and self.activity_semantics
            is not BrokerCaptureActivitySemantics.UNAVAILABLE
        ):
            raise ValueError("activity semantics require an activity value")
        if (
            self.activity_value is not None
            and self.activity_semantics
            is BrokerCaptureActivitySemantics.UNAVAILABLE
        ):
            raise ValueError("activity values require explicit semantics")

        connection_kinds = {
            BrokerCaptureEventKind.CONNECTION_OPEN,
            BrokerCaptureEventKind.CONNECTION_CLOSE,
            BrokerCaptureEventKind.RECONNECT,
        }
        if self.kind in connection_kinds and self.connection_id is None:
            raise ValueError(
                "connection lifecycle messages require connection_id"
            )
        subscription_kinds = {
            BrokerCaptureEventKind.SUBSCRIPTION_ADD,
            BrokerCaptureEventKind.SUBSCRIPTION_REMOVE,
        }
        if self.kind in subscription_kinds and (
            self.subscription_id is None or self.symbol is None
        ):
            raise ValueError(
                "subscription messages require subscription_id and symbol"
            )
        gap_kinds = {
            BrokerCaptureEventKind.GAP,
            BrokerCaptureEventKind.OUTAGE_END,
        }
        if self.kind in gap_kinds and self.gap_duration_ns is None:
            raise ValueError("gap/outage-end messages require gap_duration_ns")
        if (
            self.kind is BrokerCaptureEventKind.PROCESS_RESTART
            and self.reason_code is None
        ):
            raise ValueError("process restart messages require a reason code")
        if (
            self.kind is BrokerCaptureEventKind.CLOCK_CORRECTION
            and self.reason_code != "wall_monotonic_divergence"
        ):
            raise ValueError(
                "clock corrections require the collector reason code"
            )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining stable adapter-message identity."""
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "source_event_time_ns": self.source_event_time_ns,
            "source_timestamp_semantics": self.source_timestamp_semantics.value,
            "source_timestamp_precision_ns": self.source_timestamp_precision_ns,
            "source_sequence": self.source_sequence,
            "source_message_id": self.source_message_id,
            "source_batch_id": self.source_batch_id,
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "bid_text": self.bid_text,
            "ask_text": self.ask_text,
            "price_text_semantics": self.price_text_semantics.value,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "size_semantics": self.size_semantics.value,
            "activity_value": self.activity_value,
            "activity_semantics": self.activity_semantics.value,
            "connection_id": self.connection_id,
            "subscription_id": self.subscription_id,
            "gap_duration_ns": self.gap_duration_ns,
            "reason_code": self.reason_code,
            "raw_message_sha256": self.raw_message_sha256,
            "public_metadata": dict(self.public_metadata),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible message metadata."""
        return {**self.identity_payload(), "message_id": self.message_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerAdapterMessageV1":
        """Restore and verify a version-one adapter message."""
        _require_schema(data, BROKER_ADAPTER_MESSAGE_SCHEMA_VERSION)
        return cls(
            kind=BrokerCaptureEventKind.from_value(str(data.get("kind", ""))),
            source_event_time_ns=cast(
                int | None, data.get("source_event_time_ns")
            ),
            source_timestamp_semantics=(
                BrokerCaptureSourceTimestampSemantics.from_value(
                    str(data.get("source_timestamp_semantics", ""))
                )
            ),
            source_timestamp_precision_ns=cast(
                int | None, data.get("source_timestamp_precision_ns")
            ),
            source_sequence=cast(int | None, data.get("source_sequence")),
            source_message_id=_mapping_optional_text(data, "source_message_id"),
            source_batch_id=_mapping_optional_text(data, "source_batch_id"),
            symbol=_mapping_optional_text(data, "symbol"),
            bid=cast(float | None, data.get("bid")),
            ask=cast(float | None, data.get("ask")),
            bid_text=_mapping_optional_text(data, "bid_text"),
            ask_text=_mapping_optional_text(data, "ask_text"),
            price_text_semantics=BrokerCapturePriceTextSemantics.from_value(
                str(data.get("price_text_semantics", ""))
            ),
            bid_size=cast(float | None, data.get("bid_size")),
            ask_size=cast(float | None, data.get("ask_size")),
            size_semantics=BrokerCaptureSizeSemantics.from_value(
                str(data.get("size_semantics", ""))
            ),
            activity_value=cast(float | None, data.get("activity_value")),
            activity_semantics=BrokerCaptureActivitySemantics.from_value(
                str(data.get("activity_semantics", ""))
            ),
            connection_id=_mapping_optional_text(data, "connection_id"),
            subscription_id=_mapping_optional_text(data, "subscription_id"),
            gap_duration_ns=cast(int | None, data.get("gap_duration_ns")),
            reason_code=_mapping_optional_text(data, "reason_code"),
            raw_message_sha256=_mapping_optional_text(
                data, "raw_message_sha256"
            ),
            public_metadata=_json_dict(data.get("public_metadata")),
            message_id=str(data.get("message_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerAdapterMessageV1":
        """Restore a message from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerCaptureSessionV1:
    """Public, credential-free identity and clock context for one session."""

    adapter_id: str
    adapter_version: str
    adapter_config_sha256: str
    protocol: str
    environment_id: str
    server_id: str
    started_at_utc_ns: int
    started_at_monotonic_ns: int
    account_id_sha256: str | None = None
    host_id_sha256: str | None = None
    clock_source: str = "system_wall_and_monotonic"
    clock_resolution_ns: int = 1
    collector_id: str = BROKER_CAPTURE_COLLECTOR_ID
    collector_version: str = BROKER_CAPTURE_COLLECTOR_VERSION
    public_metadata: dict[str, JSONValue] = field(default_factory=dict)
    session_id: str = ""
    schema_version: str = BROKER_CAPTURE_SESSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_CAPTURE_SESSION_SCHEMA_VERSION:
            raise ValueError("unsupported broker capture session schema")
        for name in (
            "adapter_id",
            "protocol",
            "environment_id",
            "server_id",
            "clock_source",
            "collector_id",
        ):
            object.__setattr__(
                self, name, _required_canonical_id(getattr(self, name), name)
            )
        for name in ("adapter_version", "collector_version"):
            value = _required_text(getattr(self, name))
            if not _SEMVER_RE.fullmatch(value):
                raise ValueError(f"{name} must be a semantic version")
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "adapter_config_sha256",
            _required_sha256(
                self.adapter_config_sha256, "adapter_config_sha256"
            ),
        )
        for name in ("account_id_sha256", "host_id_sha256"):
            object.__setattr__(
                self,
                name,
                _optional_sha256(getattr(self, name), name),
            )
        for name in ("started_at_utc_ns", "started_at_monotonic_ns"):
            object.__setattr__(
                self,
                name,
                _nonnegative_int64(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "clock_resolution_ns",
            _positive_int64(self.clock_resolution_ns, "clock_resolution_ns"),
        )
        metadata = _secret_free_metadata(self.public_metadata)
        object.__setattr__(self, "public_metadata", metadata)
        expected = _stable_id("broker-capture-session", self.identity_payload())
        supplied = _optional_text(self.session_id)
        if supplied is not None and supplied != expected:
            raise ValueError("session_id does not match deterministic identity")
        object.__setattr__(self, "session_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining this capture session."""
        return {
            "schema_version": self.schema_version,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "adapter_config_sha256": self.adapter_config_sha256,
            "protocol": self.protocol,
            "environment_id": self.environment_id,
            "server_id": self.server_id,
            "account_id_sha256": self.account_id_sha256,
            "host_id_sha256": self.host_id_sha256,
            "started_at_utc_ns": self.started_at_utc_ns,
            "started_at_monotonic_ns": self.started_at_monotonic_ns,
            "clock_source": self.clock_source,
            "clock_resolution_ns": self.clock_resolution_ns,
            "collector_id": self.collector_id,
            "collector_version": self.collector_version,
            "public_metadata": dict(self.public_metadata),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible session metadata."""
        return {**self.identity_payload(), "session_id": self.session_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerCaptureSessionV1":
        """Restore and verify a version-one capture session."""
        _require_schema(data, BROKER_CAPTURE_SESSION_SCHEMA_VERSION)
        return cls(
            adapter_id=str(data.get("adapter_id", "")),
            adapter_version=str(data.get("adapter_version", "")),
            adapter_config_sha256=str(data.get("adapter_config_sha256", "")),
            protocol=str(data.get("protocol", "")),
            environment_id=str(data.get("environment_id", "")),
            server_id=str(data.get("server_id", "")),
            started_at_utc_ns=cast(int, data.get("started_at_utc_ns")),
            started_at_monotonic_ns=cast(
                int, data.get("started_at_monotonic_ns")
            ),
            account_id_sha256=_mapping_optional_text(data, "account_id_sha256"),
            host_id_sha256=_mapping_optional_text(data, "host_id_sha256"),
            clock_source=str(data.get("clock_source", "")),
            clock_resolution_ns=cast(int, data.get("clock_resolution_ns")),
            collector_id=str(data.get("collector_id", "")),
            collector_version=str(data.get("collector_version", "")),
            public_metadata=_json_dict(data.get("public_metadata")),
            session_id=str(data.get("session_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerCaptureSessionV1":
        """Restore a session from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerCaptureEventV1:
    """One collector-stamped broker message in stable capture order."""

    session_id: str
    capture_sequence: int
    receive_time_utc_ns: int
    receive_time_monotonic_ns: int
    message: BrokerAdapterMessageV1
    clock_offset_change_ns: int | None = None
    event_id: str = ""
    schema_version: str = BROKER_CAPTURE_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_CAPTURE_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported broker capture event schema")
        object.__setattr__(
            self,
            "session_id",
            _required_prefixed_id(self.session_id, "broker-capture-session-"),
        )
        for name in (
            "capture_sequence",
            "receive_time_utc_ns",
            "receive_time_monotonic_ns",
        ):
            object.__setattr__(
                self, name, _nonnegative_int64(getattr(self, name), name)
            )
        if not isinstance(self.message, BrokerAdapterMessageV1):
            raise TypeError("message must be BrokerAdapterMessageV1")
        correction = _optional_int64(
            self.clock_offset_change_ns, "clock_offset_change_ns"
        )
        object.__setattr__(self, "clock_offset_change_ns", correction)
        if self.message.kind is BrokerCaptureEventKind.CLOCK_CORRECTION:
            if correction is None or correction == 0:
                raise ValueError(
                    "clock correction events require non-zero offset change"
                )
        elif correction is not None:
            raise ValueError("clock offset change is only valid on corrections")
        expected = _stable_id("broker-capture-event", self.identity_payload())
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("event_id does not match deterministic identity")
        object.__setattr__(self, "event_id", expected)

    @property
    def kind(self) -> BrokerCaptureEventKind:
        """Return the underlying broker-message kind."""
        return self.message.kind

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining immutable capture-event identity."""
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "capture_sequence": self.capture_sequence,
            "receive_time_utc_ns": self.receive_time_utc_ns,
            "receive_time_monotonic_ns": self.receive_time_monotonic_ns,
            "message": self.message.to_dict(),
            "clock_offset_change_ns": self.clock_offset_change_ns,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible event evidence."""
        return {**self.identity_payload(), "event_id": self.event_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerCaptureEventV1":
        """Restore and verify a version-one capture event."""
        _require_schema(data, BROKER_CAPTURE_EVENT_SCHEMA_VERSION)
        return cls(
            session_id=str(data.get("session_id", "")),
            capture_sequence=cast(int, data.get("capture_sequence")),
            receive_time_utc_ns=cast(int, data.get("receive_time_utc_ns")),
            receive_time_monotonic_ns=cast(
                int, data.get("receive_time_monotonic_ns")
            ),
            message=BrokerAdapterMessageV1.from_dict(
                _mapping(data.get("message"))
            ),
            clock_offset_change_ns=cast(
                int | None, data.get("clock_offset_change_ns")
            ),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerCaptureEventV1":
        """Restore an event from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerCaptureStoragePolicyV1:
    """Rotation, quota, retention, and backpressure limits."""

    max_partition_events: int = 100_000
    max_partition_bytes: int = 128 * 1024**2
    max_partition_duration_ns: int = 5 * 60 * 1_000_000_000
    max_session_bytes: int = 10 * 1024**3
    high_watermark_bytes: int = 8 * 1024**3
    max_retained_partitions: int = 4096
    manifest_reserve_bytes: int = 64 * 1024
    fsync_each_event: bool = True
    retention_mode: BrokerCaptureRetentionMode = (
        BrokerCaptureRetentionMode.REFUSE
    )
    backpressure_mode: BrokerCaptureBackpressureMode = (
        BrokerCaptureBackpressureMode.REFUSE
    )
    policy_id: str = ""
    schema_version: str = BROKER_CAPTURE_STORAGE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_CAPTURE_STORAGE_POLICY_SCHEMA_VERSION:
            raise ValueError("unsupported broker capture storage policy schema")
        for name in (
            "max_partition_events",
            "max_partition_bytes",
            "max_partition_duration_ns",
            "max_session_bytes",
            "high_watermark_bytes",
            "max_retained_partitions",
            "manifest_reserve_bytes",
        ):
            object.__setattr__(
                self, name, _positive_int64(getattr(self, name), name)
            )
        if (
            self.max_partition_bytes + self.manifest_reserve_bytes
            > self.max_session_bytes
        ):
            raise ValueError(
                "one partition plus manifest reserve exceeds session quota"
            )
        if self.high_watermark_bytes > self.max_session_bytes:
            raise ValueError("high watermark exceeds session quota")
        object.__setattr__(
            self,
            "fsync_each_event",
            _strict_bool(self.fsync_each_event, "fsync_each_event"),
        )
        retention = BrokerCaptureRetentionMode.from_value(self.retention_mode)
        backpressure = BrokerCaptureBackpressureMode.from_value(
            self.backpressure_mode
        )
        if retention is not BrokerCaptureRetentionMode.REFUSE:
            raise ValueError("v1 capture retention must refuse before deletion")
        if backpressure is not BrokerCaptureBackpressureMode.REFUSE:
            raise ValueError("v1 capture backpressure must refuse")
        object.__setattr__(self, "retention_mode", retention)
        object.__setattr__(self, "backpressure_mode", backpressure)
        expected = _stable_id("broker-capture-policy", self.identity_payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("policy_id does not match deterministic identity")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining storage behavior."""
        return {
            "schema_version": self.schema_version,
            "max_partition_events": self.max_partition_events,
            "max_partition_bytes": self.max_partition_bytes,
            "max_partition_duration_ns": self.max_partition_duration_ns,
            "max_session_bytes": self.max_session_bytes,
            "high_watermark_bytes": self.high_watermark_bytes,
            "max_retained_partitions": self.max_retained_partitions,
            "manifest_reserve_bytes": self.manifest_reserve_bytes,
            "fsync_each_event": self.fsync_each_event,
            "retention_mode": self.retention_mode.value,
            "backpressure_mode": self.backpressure_mode.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy metadata."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerCaptureStoragePolicyV1":
        """Restore and verify a version-one storage policy."""
        _require_schema(data, BROKER_CAPTURE_STORAGE_POLICY_SCHEMA_VERSION)
        return cls(
            max_partition_events=cast(int, data.get("max_partition_events")),
            max_partition_bytes=cast(int, data.get("max_partition_bytes")),
            max_partition_duration_ns=cast(
                int, data.get("max_partition_duration_ns")
            ),
            max_session_bytes=cast(int, data.get("max_session_bytes")),
            high_watermark_bytes=cast(int, data.get("high_watermark_bytes")),
            max_retained_partitions=cast(
                int, data.get("max_retained_partitions")
            ),
            manifest_reserve_bytes=cast(
                int, data.get("manifest_reserve_bytes")
            ),
            fsync_each_event=cast(bool, data.get("fsync_each_event")),
            retention_mode=BrokerCaptureRetentionMode.from_value(
                str(data.get("retention_mode", ""))
            ),
            backpressure_mode=BrokerCaptureBackpressureMode.from_value(
                str(data.get("backpressure_mode", ""))
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerCaptureStoragePolicyV1":
        """Restore a policy from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerCapturePartitionManifestV1:
    """Compact immutable manifest for one completed JSONL partition."""

    session_id: str
    policy_id: str
    partition_ordinal: int
    data_artifact: ArtifactRef
    event_count: int
    first_capture_sequence: int
    last_capture_sequence: int
    first_receive_time_utc_ns: int
    last_receive_time_utc_ns: int
    first_receive_time_monotonic_ns: int
    last_receive_time_monotonic_ns: int
    event_kind_counts: dict[str, int]
    completed: bool = True
    partition_id: str = ""
    schema_version: str = BROKER_CAPTURE_PARTITION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BROKER_CAPTURE_PARTITION_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("unsupported broker capture partition manifest")
        object.__setattr__(
            self,
            "session_id",
            _required_prefixed_id(self.session_id, "broker-capture-session-"),
        )
        object.__setattr__(
            self,
            "policy_id",
            _required_prefixed_id(self.policy_id, "broker-capture-policy-"),
        )
        for name in (
            "partition_ordinal",
            "event_count",
            "first_capture_sequence",
            "last_capture_sequence",
            "first_receive_time_utc_ns",
            "last_receive_time_utc_ns",
            "first_receive_time_monotonic_ns",
            "last_receive_time_monotonic_ns",
        ):
            object.__setattr__(
                self, name, _nonnegative_int64(getattr(self, name), name)
            )
        if self.event_count <= 0:
            raise ValueError("completed partitions must contain events")
        if self.last_capture_sequence < self.first_capture_sequence:
            raise ValueError("partition capture sequence bounds are reversed")
        if (
            self.last_capture_sequence - self.first_capture_sequence + 1
            != self.event_count
        ):
            raise ValueError("partition capture sequences must be contiguous")
        if (
            self.last_receive_time_monotonic_ns
            < self.first_receive_time_monotonic_ns
        ):
            raise ValueError("partition monotonic receive bounds are reversed")
        counts = _event_kind_counts(self.event_kind_counts)
        if sum(counts.values()) != self.event_count:
            raise ValueError("partition event-kind counts do not reconcile")
        object.__setattr__(self, "event_kind_counts", counts)
        artifact = _validated_capture_artifact(self.data_artifact)
        object.__setattr__(self, "data_artifact", artifact)
        if not _strict_bool(self.completed, "completed"):
            raise ValueError(
                "partition manifests only advertise completed data"
            )
        object.__setattr__(self, "completed", True)
        expected = _stable_id(
            "broker-capture-partition", self.identity_payload()
        )
        supplied = _optional_text(self.partition_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "partition_id does not match deterministic identity"
            )
        object.__setattr__(self, "partition_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining this immutable partition."""
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "policy_id": self.policy_id,
            "partition_ordinal": self.partition_ordinal,
            "data_artifact": self.data_artifact.to_dict(),
            "event_count": self.event_count,
            "first_capture_sequence": self.first_capture_sequence,
            "last_capture_sequence": self.last_capture_sequence,
            "first_receive_time_utc_ns": self.first_receive_time_utc_ns,
            "last_receive_time_utc_ns": self.last_receive_time_utc_ns,
            "first_receive_time_monotonic_ns": (
                self.first_receive_time_monotonic_ns
            ),
            "last_receive_time_monotonic_ns": (
                self.last_receive_time_monotonic_ns
            ),
            "event_kind_counts": dict(self.event_kind_counts),
            "completed": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible manifest metadata."""
        return {**self.identity_payload(), "partition_id": self.partition_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerCapturePartitionManifestV1":
        """Restore and verify a version-one partition manifest."""
        _require_schema(data, BROKER_CAPTURE_PARTITION_MANIFEST_SCHEMA_VERSION)
        return cls(
            session_id=str(data.get("session_id", "")),
            policy_id=str(data.get("policy_id", "")),
            partition_ordinal=cast(int, data.get("partition_ordinal")),
            data_artifact=ArtifactRef.from_dict(
                _mapping(data.get("data_artifact"))
            ),
            event_count=cast(int, data.get("event_count")),
            first_capture_sequence=cast(
                int, data.get("first_capture_sequence")
            ),
            last_capture_sequence=cast(int, data.get("last_capture_sequence")),
            first_receive_time_utc_ns=cast(
                int, data.get("first_receive_time_utc_ns")
            ),
            last_receive_time_utc_ns=cast(
                int, data.get("last_receive_time_utc_ns")
            ),
            first_receive_time_monotonic_ns=cast(
                int, data.get("first_receive_time_monotonic_ns")
            ),
            last_receive_time_monotonic_ns=cast(
                int, data.get("last_receive_time_monotonic_ns")
            ),
            event_kind_counts={
                str(key): cast(int, value)
                for key, value in _mapping(
                    data.get("event_kind_counts")
                ).items()
            },
            completed=cast(bool, data.get("completed")),
            partition_id=str(data.get("partition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerCapturePartitionManifestV1":
        """Restore a partition manifest from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerCaptureSessionManifestV1:
    """Bounded catalog of completed partitions and capture-health evidence."""

    session: BrokerCaptureSessionV1
    storage_policy: BrokerCaptureStoragePolicyV1
    state: BrokerCaptureSessionState
    partitions: tuple[BrokerCapturePartitionManifestV1, ...]
    event_count: int
    event_kind_counts: dict[str, int]
    first_capture_sequence: int | None
    last_capture_sequence: int | None
    partial_artifact_count: int = 0
    limitations: tuple[str, ...] = ()
    manifest_id: str = ""
    schema_version: str = BROKER_CAPTURE_SESSION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BROKER_CAPTURE_SESSION_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("unsupported broker capture session manifest")
        if not isinstance(self.session, BrokerCaptureSessionV1):
            raise TypeError("session must be BrokerCaptureSessionV1")
        if not isinstance(self.storage_policy, BrokerCaptureStoragePolicyV1):
            raise TypeError(
                "storage_policy must be BrokerCaptureStoragePolicyV1"
            )
        object.__setattr__(
            self, "state", BrokerCaptureSessionState.from_value(self.state)
        )
        partitions = tuple(
            sorted(self.partitions, key=lambda item: item.partition_ordinal)
        )
        if len(partitions) > MAX_CAPTURE_PARTITIONS:
            raise ValueError("capture session manifest has too many partitions")
        if len({item.partition_id for item in partitions}) != len(partitions):
            raise ValueError(
                "capture session manifest has duplicate partitions"
            )
        for expected_ordinal, partition in enumerate(partitions):
            if partition.partition_ordinal != expected_ordinal:
                raise ValueError(
                    "capture partition ordinals must be contiguous"
                )
            if partition.session_id != self.session.session_id:
                raise ValueError("capture partition session does not match")
            if partition.policy_id != self.storage_policy.policy_id:
                raise ValueError("capture partition policy does not match")
            if expected_ordinal and (
                partition.first_capture_sequence
                != partitions[expected_ordinal - 1].last_capture_sequence + 1
            ):
                raise ValueError(
                    "capture partition sequences must be contiguous"
                )
        object.__setattr__(self, "partitions", partitions)
        event_count = _nonnegative_int64(self.event_count, "event_count")
        if sum(item.event_count for item in partitions) != event_count:
            raise ValueError("capture session event count does not reconcile")
        object.__setattr__(self, "event_count", event_count)
        counts = _event_kind_counts(self.event_kind_counts, allow_empty=True)
        combined: dict[str, int] = {}
        for partition in partitions:
            for kind, count in partition.event_kind_counts.items():
                combined[kind] = combined.get(kind, 0) + count
        combined = dict(sorted(combined.items()))
        if counts != combined:
            raise ValueError(
                "capture session event-kind counts do not reconcile"
            )
        object.__setattr__(self, "event_kind_counts", counts)
        first = _optional_nonnegative_int64(
            self.first_capture_sequence, "first_capture_sequence"
        )
        last = _optional_nonnegative_int64(
            self.last_capture_sequence, "last_capture_sequence"
        )
        if partitions:
            if (
                first != partitions[0].first_capture_sequence
                or last != partitions[-1].last_capture_sequence
            ):
                raise ValueError(
                    "capture session sequence bounds do not reconcile"
                )
        elif first is not None or last is not None:
            raise ValueError(
                "empty capture session cannot have sequence bounds"
            )
        object.__setattr__(self, "first_capture_sequence", first)
        object.__setattr__(self, "last_capture_sequence", last)
        partial_count = _nonnegative_int64(
            self.partial_artifact_count, "partial_artifact_count"
        )
        if partial_count:
            raise ValueError(
                "published manifests cannot advertise partial artifacts"
            )
        object.__setattr__(self, "partial_artifact_count", 0)
        limitations = tuple(
            sorted({_bounded_text(value) for value in self.limitations})
        )
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "broker-capture-manifest", self.identity_payload()
        )
        supplied = _optional_text(self.manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "manifest_id does not match deterministic identity"
            )
        object.__setattr__(self, "manifest_id", expected)
        if len(self.to_json().encode("utf-8")) > MAX_CAPTURE_MANIFEST_BYTES:
            raise ValueError("capture session manifest exceeds bounded size")

    @property
    def complete(self) -> bool:
        """Return whether collection ended normally."""
        return self.state is BrokerCaptureSessionState.COMPLETED

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining this manifest snapshot."""
        return {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "storage_policy": self.storage_policy.to_dict(),
            "state": self.state.value,
            "partitions": [item.to_dict() for item in self.partitions],
            "event_count": self.event_count,
            "event_kind_counts": dict(self.event_kind_counts),
            "first_capture_sequence": self.first_capture_sequence,
            "last_capture_sequence": self.last_capture_sequence,
            "partial_artifact_count": 0,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible manifest metadata."""
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerCaptureSessionManifestV1":
        """Restore and verify a version-one session manifest."""
        _require_schema(data, BROKER_CAPTURE_SESSION_MANIFEST_SCHEMA_VERSION)
        return cls(
            session=BrokerCaptureSessionV1.from_dict(
                _mapping(data.get("session"))
            ),
            storage_policy=BrokerCaptureStoragePolicyV1.from_dict(
                _mapping(data.get("storage_policy"))
            ),
            state=BrokerCaptureSessionState.from_value(
                str(data.get("state", ""))
            ),
            partitions=tuple(
                BrokerCapturePartitionManifestV1.from_dict(item)
                for item in _mapping_sequence(data.get("partitions"))
            ),
            event_count=cast(int, data.get("event_count")),
            event_kind_counts={
                str(key): cast(int, value)
                for key, value in _mapping(
                    data.get("event_kind_counts")
                ).items()
            },
            first_capture_sequence=cast(
                int | None, data.get("first_capture_sequence")
            ),
            last_capture_sequence=cast(
                int | None, data.get("last_capture_sequence")
            ),
            partial_artifact_count=cast(
                int, data.get("partial_artifact_count", 0)
            ),
            limitations=tuple(
                str(value) for value in _sequence(data.get("limitations"))
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerCaptureSessionManifestV1":
        """Restore a session manifest from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerCaptureReplaySummaryV1:
    """Bounded integrity and ordering evidence from one replay."""

    session_id: str
    manifest_id: str
    partition_count: int
    event_count: int
    event_kind_counts: dict[str, int]
    first_capture_sequence: int | None
    last_capture_sequence: int | None
    logical_content_sha256: str
    summary_id: str = ""
    schema_version: str = BROKER_CAPTURE_REPLAY_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_CAPTURE_REPLAY_SUMMARY_SCHEMA_VERSION:
            raise ValueError("unsupported broker capture replay summary")
        object.__setattr__(
            self,
            "session_id",
            _required_prefixed_id(self.session_id, "broker-capture-session-"),
        )
        object.__setattr__(
            self,
            "manifest_id",
            _required_prefixed_id(self.manifest_id, "broker-capture-manifest-"),
        )
        for name in ("partition_count", "event_count"):
            object.__setattr__(
                self, name, _nonnegative_int64(getattr(self, name), name)
            )
        counts = _event_kind_counts(self.event_kind_counts, allow_empty=True)
        if sum(counts.values()) != self.event_count:
            raise ValueError("replay event-kind counts do not reconcile")
        object.__setattr__(self, "event_kind_counts", counts)
        first = _optional_nonnegative_int64(
            self.first_capture_sequence, "first_capture_sequence"
        )
        last = _optional_nonnegative_int64(
            self.last_capture_sequence, "last_capture_sequence"
        )
        if self.event_count:
            if (
                first is None
                or last is None
                or last - first + 1 != self.event_count
            ):
                raise ValueError("replay capture sequences do not reconcile")
        elif first is not None or last is not None:
            raise ValueError("empty replay cannot have sequence bounds")
        object.__setattr__(self, "first_capture_sequence", first)
        object.__setattr__(self, "last_capture_sequence", last)
        object.__setattr__(
            self,
            "logical_content_sha256",
            _required_sha256(
                self.logical_content_sha256, "logical_content_sha256"
            ),
        )
        expected = _stable_id("broker-capture-replay", self.identity_payload())
        supplied = _optional_text(self.summary_id)
        if supplied is not None and supplied != expected:
            raise ValueError("summary_id does not match deterministic identity")
        object.__setattr__(self, "summary_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return fields defining replay evidence."""
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "manifest_id": self.manifest_id,
            "partition_count": self.partition_count,
            "event_count": self.event_count,
            "event_kind_counts": dict(self.event_kind_counts),
            "first_capture_sequence": self.first_capture_sequence,
            "last_capture_sequence": self.last_capture_sequence,
            "logical_content_sha256": self.logical_content_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible replay evidence."""
        return {**self.identity_payload(), "summary_id": self.summary_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return canonical_capture_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerCaptureReplaySummaryV1":
        """Restore and verify version-one replay evidence."""
        _require_schema(data, BROKER_CAPTURE_REPLAY_SUMMARY_SCHEMA_VERSION)
        return cls(
            session_id=str(data.get("session_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            partition_count=cast(int, data.get("partition_count")),
            event_count=cast(int, data.get("event_count")),
            event_kind_counts={
                str(key): cast(int, value)
                for key, value in _mapping(
                    data.get("event_kind_counts")
                ).items()
            },
            first_capture_sequence=cast(
                int | None, data.get("first_capture_sequence")
            ),
            last_capture_sequence=cast(
                int | None, data.get("last_capture_sequence")
            ),
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerCaptureReplaySummaryV1":
        """Restore replay evidence from canonical JSON."""
        return cls.from_dict(_json_mapping(text))


def canonical_capture_json(value: Mapping[str, JSONValue]) -> str:
    """Return canonical compact UTF-8 JSON for capture contracts."""
    return json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def assert_secret_free_capture_value(value: JSONValue) -> None:
    """Fail closed if a public capture value resembles credential material."""
    _validate_secret_free(value, path="capture")


def logical_capture_content_sha256(
    events: Sequence[BrokerCaptureEventV1],
) -> str:
    """Hash ordered canonical event content without filesystem metadata."""
    digest = hashlib.sha256()
    for event in events:
        digest.update(event.to_json().encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _validated_capture_artifact(value: ArtifactRef) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise TypeError("data_artifact must be ArtifactRef")
    if value.kind != BROKER_CAPTURE_DATA_ARTIFACT_KIND:
        raise ValueError("unsupported broker capture artifact kind")
    path = _relative_artifact_path(value.path)
    size = _positive_int64(value.size_bytes, "data_artifact.size_bytes")
    digest = _required_sha256(value.sha256, "data_artifact.sha256")
    metadata = _secret_free_metadata(value.metadata)
    return ArtifactRef(
        kind=value.kind,
        path=path,
        size_bytes=size,
        sha256=digest,
        metadata=metadata,
    )


def _event_kind_counts(
    values: Mapping[str, int], *, allow_empty: bool = False
) -> dict[str, int]:
    if len(values) > MAX_CAPTURE_KIND_COUNTS:
        raise ValueError("event-kind count map exceeds limit")
    counts: dict[str, int] = {}
    for key, value in values.items():
        kind = BrokerCaptureEventKind.from_value(str(key)).value
        count = _nonnegative_int64(value, f"event_kind_counts.{kind}")
        if count:
            counts[kind] = count
    if not counts and not allow_empty:
        raise ValueError("event-kind counts cannot be empty")
    return dict(sorted(counts.items()))


def _secret_free_metadata(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    converted = cast(dict[str, JSONValue], dict(value))
    _validate_json_value(converted, "public_metadata")
    _validate_secret_free(converted, path="public_metadata")
    encoded = canonical_capture_json(converted)
    if len(encoded.encode("utf-8")) > MAX_CAPTURE_METADATA_BYTES:
        raise ValueError("public metadata exceeds bounded size")
    return dict(sorted(converted.items()))


def _validate_secret_free(value: JSONValue, *, path: str) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip(
                "_"
            )
            if any(
                part == normalized or part in normalized.split("_")
                for part in _SENSITIVE_KEY_PARTS
            ):
                raise ValueError(
                    f"sensitive metadata key is prohibited at {path}"
                )
            _validate_secret_free(item, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_secret_free(item, path=f"{path}[{index}]")
        return
    if isinstance(value, str):
        if any(pattern.search(value) for pattern in _SENSITIVE_VALUE_PATTERNS):
            raise ValueError(
                f"sensitive metadata value is prohibited at {path}"
            )


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string key")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise TypeError(f"{path} contains a non-JSON value")


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_capture_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}-{digest}"


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("text value is required")
    return _bounded_text(text)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _bounded_text(text) if text else None


def _bounded_text(value: Any) -> str:
    text = str(value).strip()
    if not text or len(text) > MAX_CAPTURE_TEXT:
        raise ValueError("capture text must be non-empty and bounded")
    return text


def _required_canonical_id(value: Any, name: str) -> str:
    text = _required_text(value)
    if not _CANONICAL_ID_RE.fullmatch(text):
        raise ValueError(f"{name} is not a canonical public identifier")
    return text


def _optional_canonical_id(value: Any, name: str) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    if not _CANONICAL_ID_RE.fullmatch(text):
        raise ValueError(f"{name} is not a canonical public identifier")
    return text


def _required_prefixed_id(value: Any, prefix: str) -> str:
    text = _required_text(value)
    if not text.startswith(prefix) or not _SHA256_RE.fullmatch(
        text[len(prefix) :]
    ):
        raise ValueError(f"identifier must use {prefix}sha256 form")
    return text


def _optional_symbol(value: Any) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    symbol = text.upper()
    if not _SYMBOL_RE.fullmatch(symbol):
        raise ValueError("unsupported broker capture symbol")
    return symbol


def _nonnegative_int64(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0 or value > INT64_MAX:
        raise ValueError(f"{name} is outside non-negative int64 range")
    return value


def _positive_int64(value: Any, name: str) -> int:
    number = _nonnegative_int64(value, name)
    if number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def _optional_nonnegative_int64(value: Any, name: str) -> int | None:
    return None if value is None else _nonnegative_int64(value, name)


def _optional_int64(value: Any, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < -(2**63) or value > INT64_MAX:
        raise ValueError(f"{name} is outside int64 range")
    return value


def _optional_nonnegative_float(value: Any, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return number


def _optional_price_lexeme(value: Any, name: str) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    if not _PRICE_LEXEME_RE.fullmatch(text):
        raise ValueError(f"{name} must preserve a plain decimal price lexeme")
    try:
        number = Decimal(text)
    except InvalidOperation as err:
        raise ValueError(f"{name} is not a decimal price lexeme") from err
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{name} must represent a finite positive price")
    return text


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _required_sha256(value: Any, name: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return text


def _optional_sha256(value: Any, name: str) -> str | None:
    text = _optional_text(value)
    return None if text is None else _required_sha256(text, name)


def _relative_artifact_path(value: Any) -> str:
    text = _required_text(value).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("capture artifact path must be relative and contained")
    return str(path)


def _enum_value(enum_type: type[_EnumT], value: Any, label: str) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as err:
        raise ValueError(f"unsupported {label}") from err


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError(f"unsupported schema; expected {expected}")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("mapping value is required")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("sequence value is required")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _json_dict(value: Any) -> dict[str, JSONValue]:
    mapping = _mapping(value)
    return cast(dict[str, JSONValue], dict(mapping))


def _mapping_optional_text(data: Mapping[str, Any], name: str) -> str | None:
    return _optional_text(data.get(name))


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)


__all__ = [
    "BROKER_ADAPTER_MESSAGE_SCHEMA_VERSION",
    "BROKER_CAPTURE_COLLECTOR_ID",
    "BROKER_CAPTURE_COLLECTOR_VERSION",
    "BROKER_CAPTURE_DATA_ARTIFACT_KIND",
    "BROKER_CAPTURE_EVENT_SCHEMA_VERSION",
    "BROKER_CAPTURE_PARTITION_MANIFEST_SCHEMA_VERSION",
    "BROKER_CAPTURE_REPLAY_SUMMARY_SCHEMA_VERSION",
    "BROKER_CAPTURE_SESSION_MANIFEST_SCHEMA_VERSION",
    "BROKER_CAPTURE_SESSION_SCHEMA_VERSION",
    "BROKER_CAPTURE_STORAGE_POLICY_SCHEMA_VERSION",
    "BrokerAdapterMessageV1",
    "BrokerCaptureActivitySemantics",
    "BrokerCaptureBackpressureMode",
    "BrokerCaptureEventKind",
    "BrokerCaptureEventV1",
    "BrokerCapturePartitionManifestV1",
    "BrokerCapturePriceTextSemantics",
    "BrokerCaptureReplaySummaryV1",
    "BrokerCaptureRetentionMode",
    "BrokerCaptureSessionManifestV1",
    "BrokerCaptureSessionState",
    "BrokerCaptureSessionV1",
    "BrokerCaptureSizeSemantics",
    "BrokerCaptureStoragePolicyV1",
    "BrokerCaptureSourceTimestampSemantics",
    "assert_secret_free_capture_value",
    "canonical_capture_json",
    "logical_capture_content_sha256",
]

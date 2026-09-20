"""Native host-owned lifecycle evidence; not legacy capture or certification."""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
import hashlib
import json
import re
from types import UnionType
from typing import (
    Any,
    ClassVar,
    Mapping,
    NoReturn,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerAdmittedInstrumentV1,
    BrokerAdmittedMetadataV1,
    BrokerCapabilityPlanV1,
    BrokerInvocationAssociation,
    BrokerInvocationBindingV1,
    canonical_capability_json,
    verify_broker_capability_plan,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugins import (
    BrokerConfigurationSchemaV1,
    BrokerSessionV1,
)

MAX_LIFECYCLE_BYTES = 524_288
_T = TypeVar("_T", bound="_Artifact")
_PUBLIC = (
    BrokerPluginInventoryV1,
    BrokerCapabilityPlanV1,
    BrokerAdmittedEventV1,
    BrokerAdmittedInstrumentV1,
    BrokerAdmittedMetadataV1,
    BrokerInvocationBindingV1,
    BrokerConfigurationSchemaV1,
    BrokerSessionV1,
)


class BrokerLifecycleState(str, Enum):
    DISCOVERED = "discovered"
    CONFIGURED = "configured"
    STARTING = "starting"
    ACTIVE = "active"
    DEGRADED = "degraded"
    RECONNECTING = "reconnecting"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class BrokerLifecycleReason(str, Enum):
    CONFIGURED = "configured"
    STARTING = "starting"
    ACTIVE = "active"
    EOF = "finite_stream_ended"
    CLOSED = "worker_closed"
    CANCELLED = "cancel_requested"
    STARTUP_TIMEOUT = "startup_deadline"
    RUN_TIMEOUT = "run_deadline"
    SHUTDOWN_TIMEOUT = "shutdown_deadline"
    QUEUE_SATURATED = "queue_saturated"
    OUTPUT_LIMIT = "worker_output_limit"
    FRAME_LIMIT = "ipc_frame_limit"
    MALFORMED_IPC = "malformed_ipc"
    MALFORMED_EVENT = "malformed_event"
    PLUGIN_FAILURE = "plugin_failure"
    WORKER_DIED = "worker_died"
    FORCED = "forced_termination"
    RECONNECT = "reconnect_requested"
    RETRY = "retry_scheduled"
    RETRY_EXHAUSTED = "retry_exhausted"
    GAP = "source_gap"
    DUPLICATE = "duplicate_delivery"
    EVENT_LIMIT = "event_limit"
    HEALTH_LIMIT = "health_limit"
    HEALTH_ERROR = "plugin_error_health"
    PERSISTENCE = "persistence_failure"
    INVALID_REQUEST = "invalid_request"
    AUTHORIZATION = "authorization_required"
    INTEGRITY = "integrity_failure"


class BrokerLifecycleError(ValueError):
    def __init__(self, reason: BrokerLifecycleReason) -> None:
        if type(reason) is not BrokerLifecycleReason:
            raise ValueError("invalid lifecycle reason")
        self.reason = reason
        super().__init__(reason.value)


def _invalid() -> NoReturn:
    raise BrokerLifecycleError(BrokerLifecycleReason.INTEGRITY)


def _count(value: int, maximum: int = 2**63 - 1, *, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        _invalid()


def _identity(value: str, kind: str) -> None:
    if type(value) is not str or not re.fullmatch(
        re.escape(kind) + r":sha256:[0-9a-f]{64}", value
    ):
        _invalid()


def _wire(
    value: object, depth: int = 0, budget: list[int] | None = None
) -> object:
    if budget is None:
        budget = [MAX_LIFECYCLE_BYTES]
    budget[0] -= 1 + (len(value) if type(value) is str else 0)
    if budget[0] < 0 or depth > 32:
        _invalid()
    if isinstance(value, _Artifact) or type(value) in _PUBLIC:
        return _wire(cast(Any, value).to_dict(), depth + 1, budget)
    if isinstance(value, Enum):
        return value.value
    if value is None or type(value) in (str, int, bool):
        return value
    if type(value) in (tuple, list):
        values = cast(tuple[object, ...] | list[object], value)
        if len(values) > 128:
            _invalid()
        return [_wire(item, depth + 1, budget) for item in values]
    if type(value) is dict:
        if len(value) > 128 or any(
            type(key) is not str or len(key) > 128 for key in value
        ):
            _invalid()
        return {
            key: _wire(item, depth + 1, budget) for key, item in value.items()
        }
    _invalid()


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            _invalid()
        result[key] = value
    return result


def parse_lifecycle_json(text: str) -> dict[str, object]:
    try:
        if (
            type(text) is not str
            or len(text.encode("utf-8")) > MAX_LIFECYCLE_BYTES
        ):
            _invalid()
        value = json.loads(text, object_pairs_hook=_pairs)
        if type(value) is not dict:
            _invalid()
        canonical_capability_json(value)
        return value
    except Exception:
        _invalid()


def _decode(annotation: object, value: object) -> Any:
    arguments, origin = get_args(annotation), get_origin(annotation)
    if origin is UnionType:
        if value is None and type(None) in arguments:
            return None
        for choice in arguments:
            if choice is not type(None):
                try:
                    return _decode(choice, value)
                except (ValueError, TypeError):
                    pass
        _invalid()
    if origin is tuple:
        if type(value) is not list or len(value) > 128:
            _invalid()
        return tuple(_decode(arguments[0], item) for item in value)
    if annotation in (str, int, bool):
        if type(value) is not annotation:
            _invalid()
        return value
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation(value)
    if isinstance(annotation, type) and (
        issubclass(annotation, _Artifact) or annotation in _PUBLIC
    ):
        if type(value) is not dict:
            _invalid()
        return cast(Any, annotation).from_dict(value)
    _invalid()


class _Artifact:
    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        try:
            annotations = get_type_hints(type(self))
            for field in fields(cast(Any, self)):
                object.__setattr__(
                    self,
                    field.name,
                    _decode(
                        annotations[field.name],
                        _wire(getattr(self, field.name)),
                    ),
                )
            self._validate()
            self.to_json()
        except Exception:
            _invalid()

    def _validate(self) -> None:
        raise NotImplementedError

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": "histdatacom.broker-lifecycle."
            + self.KIND
            + ".v1",
            **{
                field.name: _wire(getattr(self, field.name))
                for field in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        digest = hashlib.sha256(
            canonical_capability_json(self._payload()).encode("ascii")
        ).hexdigest()
        return "broker-lifecycle-" + self.KIND + ":sha256:" + digest

    def to_dict(self) -> dict[str, object]:
        return {**self._payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        encoded = canonical_capability_json(self.to_dict())
        if len(encoded.encode("ascii")) > MAX_LIFECYCLE_BYTES:
            _invalid()
        return str(encoded)

    @classmethod
    def from_dict(cls: type[_T], value: Mapping[str, object]) -> _T:
        try:
            canonical_capability_json(value)
            names = {field.name for field in fields(cast(Any, cls))}
            if set(value) != names | {"schema_version", "artifact_id"}:
                _invalid()
            restored = cls(**{name: value[name] for name in names})
            if restored.to_json() != canonical_capability_json(value):
                _invalid()
            return restored
        except Exception:
            _invalid()

    @classmethod
    def from_json(cls: type[_T], text: str) -> _T:
        return cls.from_dict(parse_lifecycle_json(text))


@dataclass(frozen=True, slots=True)
class BrokerLifecyclePolicyV1(_Artifact):
    queue_items: int = 32
    queue_bytes: int = 2_097_152
    frame_bytes: int = MAX_LIFECYCLE_BYTES
    stdout_bytes: int = 65_536
    stderr_bytes: int = 65_536
    max_events: int = 10_000
    max_health_records: int = 128
    startup_timeout_ms: int = 5000
    run_timeout_ms: int = 30_000
    shutdown_timeout_ms: int = 1000
    acknowledgement_timeout_ms: int = 500
    delivery_retries: int = 2
    retry_delays_ms: tuple[int, ...] = ()
    partition_events: int = 64
    partition_bytes: int = 2_097_152
    max_partitions: int = 128
    max_capture_bytes: int = 67_108_864
    version: str = "1.0.0"
    KIND: ClassVar[str] = "policy"

    def _validate(self) -> None:
        for name, maximum, minimum in (
            ("queue_items", 128, 1),
            ("queue_bytes", 16_777_216, 1024),
            ("frame_bytes", MAX_LIFECYCLE_BYTES, 1024),
            ("stdout_bytes", 1_048_576, 0),
            ("stderr_bytes", 1_048_576, 0),
            ("max_events", 1_000_000, 1),
            ("max_health_records", 1024, 8),
            ("startup_timeout_ms", 60_000, 10),
            ("run_timeout_ms", 3_600_000, 10),
            ("shutdown_timeout_ms", 10_000, 10),
            ("acknowledgement_timeout_ms", 10_000, 10),
            ("delivery_retries", 8, 0),
            ("partition_events", 10_000, 1),
            ("partition_bytes", 16_777_216, 1024),
            ("max_partitions", 128, 1),
            ("max_capture_bytes", 1_073_741_824, 1024),
        ):
            _count(getattr(self, name), maximum, minimum=minimum)
        if len(self.retry_delays_ms) > 8:
            _invalid()
        for delay in self.retry_delays_ms:
            _count(delay, 60_000)
        if (
            self.version != "1.0.0"
            or self.partition_bytes > self.max_capture_bytes
            or self.max_health_records < 6 + 4 * len(self.retry_delays_ms)
        ):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerLifecycleHeaderV1(_Artifact):
    inventory: BrokerPluginInventoryV1
    plan: BrokerCapabilityPlanV1
    policy: BrokerLifecyclePolicyV1
    symbols: tuple[str, ...]
    run_nonce: str
    host_python_version: str
    KIND: ClassVar[str] = "header"

    def _validate(self) -> None:
        verify_broker_capability_plan(self.plan, self.inventory)
        self.plan.require_admitted()
        required = {"configuration_schema", "open_session", "iter_events"}
        if not required <= set(self.plan.workflow.operations):
            _invalid()
        if self.symbols and not {
            "instruments",
            "subscribe",
            "unsubscribe",
        } <= set(self.plan.workflow.operations):
            _invalid()
        if self.symbols != tuple(sorted(set(self.symbols))) or any(
            not re.fullmatch(r"[A-Z]{6}", item) or item[:3] == item[3:]
            for item in self.symbols
        ):
            _invalid()
        if not re.fullmatch(
            r"[0-9a-f]{32}", self.run_nonce
        ) or not re.fullmatch(
            r"[0-9]+\.[0-9]+\.[0-9]+", self.host_python_version
        ):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerLifecycleTransitionV1(_Artifact):
    previous: BrokerLifecycleState
    current: BrokerLifecycleState
    reason: BrokerLifecycleReason
    epoch: int
    KIND: ClassVar[str] = "transition"

    def _validate(self) -> None:
        _count(self.epoch, 8)
        from .state_machine import validate_transition

        validate_transition(self.previous, self.current, self.reason)


@dataclass(frozen=True, slots=True)
class BrokerLifecycleIdentityV1(_Artifact):
    binding: BrokerInvocationBindingV1
    metadata: BrokerAdmittedMetadataV1
    configuration_schema: BrokerConfigurationSchemaV1
    KIND: ClassVar[str] = "worker-identity"

    def _validate(self) -> None:
        if (
            self.binding.association
            is not BrokerInvocationAssociation.INSTALLED_ENTRYPOINT
            or self.binding.metadata_id != self.metadata.metadata.artifact_id
            or self.binding.plan_id != self.metadata.plan_id
        ):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerLifecycleSessionV1(_Artifact):
    session: BrokerSessionV1
    instruments: tuple[BrokerAdmittedInstrumentV1, ...]
    KIND: ClassVar[str] = "worker-session"

    def _validate(self) -> None:
        names = tuple(item.instrument.symbol for item in self.instruments)
        if len(names) != len(set(names)):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerLifecycleRecordV1(_Artifact):
    run_id: str
    capture_sequence: int
    epoch: int
    receive_utc_ns: int
    receive_monotonic_ns: int
    kind: str
    payload_json: str
    delivery_sequence: int | None = None
    KIND: ClassVar[str] = "record"

    def _validate(self) -> None:
        _identity(self.run_id, "broker-lifecycle-header")
        for value in (
            self.capture_sequence,
            self.receive_utc_ns,
            self.receive_monotonic_ns,
        ):
            _count(value)
        _count(self.epoch, 8)
        constructors: dict[str, Any] = {
            "transition": BrokerLifecycleTransitionV1,
            "identity": BrokerLifecycleIdentityV1,
            "session": BrokerLifecycleSessionV1,
            "event": BrokerAdmittedEventV1,
        }
        if self.kind not in constructors:
            _invalid()
        decoded = constructors[self.kind].from_json(self.payload_json)
        if decoded.to_json() != self.payload_json:
            _invalid()
        if self.kind == "event":
            if self.delivery_sequence is None:
                _invalid()
            _count(self.delivery_sequence, 1_000_000)
        elif self.delivery_sequence is not None:
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerLifecyclePartitionV1(_Artifact):
    ordinal: int
    sha256: str
    byte_count: int
    first_sequence: int
    record_count: int
    event_count: int
    KIND: ClassVar[str] = "partition"

    def _validate(self) -> None:
        _count(self.ordinal, 127)
        if not re.fullmatch(r"[a-f0-9]{64}", self.sha256):
            _invalid()
        _count(self.byte_count, 16_777_216, minimum=1)
        _count(self.first_sequence)
        _count(self.record_count, 10_000, minimum=1)
        _count(self.event_count, self.record_count)


class BrokerLifecycleCompletion(str, Enum):
    OPEN = "open"
    COMPLETE = "complete_local_finite_run"
    PARTIAL = "partial"


@dataclass(frozen=True, slots=True)
class BrokerLifecycleManifestV1(_Artifact):
    header: BrokerLifecycleHeaderV1
    state: BrokerLifecycleState
    completion: BrokerLifecycleCompletion
    partitions: tuple[BrokerLifecyclePartitionV1, ...] = ()
    partial_partition: BrokerLifecyclePartitionV1 | None = None
    appended_records: int = 0
    appended_events: int = 0
    received_events: int = 0
    duplicate_deliveries: int = 0
    discarded_known: int = 0
    unknown_loss: bool = False
    stdout_discarded_bytes: int = 0
    stderr_discarded_bytes: int = 0
    worker_reaped: bool = False
    forced_terminations: int = 0
    error_diagnostics: int = 0
    source_continuity_verified: bool = False
    configuration_material_retained: bool = False
    KIND: ClassVar[str] = "manifest"

    def _validate(self) -> None:
        for name in (
            "appended_records",
            "appended_events",
            "received_events",
            "duplicate_deliveries",
            "discarded_known",
            "stdout_discarded_bytes",
            "stderr_discarded_bytes",
            "forced_terminations",
            "error_diagnostics",
        ):
            _count(getattr(self, name))
        if (
            self.source_continuity_verified
            or self.configuration_material_retained
        ):
            _invalid()
        offset = 0
        for ordinal, partition in enumerate(self.partitions):
            if (
                partition.ordinal != ordinal
                or partition.first_sequence != offset
            ):
                _invalid()
            offset += partition.record_count
        if len(self.partitions) > self.header.policy.max_partitions:
            _invalid()
        if self.partial_partition is not None:
            if (
                self.partial_partition.ordinal != len(self.partitions)
                or self.partial_partition.first_sequence != offset
            ):
                _invalid()
            offset += self.partial_partition.record_count
        if (
            offset > self.appended_records
            or self.appended_events > self.appended_records
            or self.appended_events > self.received_events
            or self.error_diagnostics > self.appended_events
            or self.discarded_known
            != self.received_events - self.appended_events
            or self.forced_terminations
            > len(self.header.policy.retry_delays_ms) + 1
        ):
            _invalid()
        if self.completion is BrokerLifecycleCompletion.COMPLETE and (
            self.partial_partition is not None
            or self.state is not BrokerLifecycleState.STOPPED
            or not self.worker_reaped
            or self.forced_terminations
            or self.error_diagnostics
            or self.unknown_loss
            or self.discarded_known
            or offset != self.appended_records
            or self.appended_events != self.received_events
        ):
            _invalid()
        if (
            self.completion is BrokerLifecycleCompletion.OPEN
            and self.state
            in (BrokerLifecycleState.STOPPED, BrokerLifecycleState.FAILED)
        ):
            _invalid()
        if (
            self.completion is BrokerLifecycleCompletion.PARTIAL
            and self.state
            not in (BrokerLifecycleState.STOPPED, BrokerLifecycleState.FAILED)
        ):
            _invalid()

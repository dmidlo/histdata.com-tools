"""Bounded immutable host capability receipts; no execution authorization."""

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
    TypeVar,
    NoReturn,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from histdatacom.broker_plugin_registry import BrokerPluginCandidateV1
from histdatacom.broker_plugins import (
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
)

MAX_CAPABILITY_ITEMS = 128
MAX_CAPABILITY_BYTES = 524_288
MAX_CAPABILITY_DEPTH = 32
_T = TypeVar("_T", bound="_Artifact")
_PUBLIC = (
    BrokerPluginCandidateV1,
    BrokerEventV1,
    BrokerInstrumentV1,
    BrokerPluginMetadataV1,
)
_ATOM = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\Z")


class BrokerCapabilityReason(str, Enum):
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    AUTHORIZATION_REQUIRED = "authorization_required"
    INVALID_ARTIFACT = "invalid_artifact"
    INVALID_PLAN = "invalid_plan"
    INVALID_STATE = "invalid_state"
    RUNTIME_IDENTITY = "runtime_identity_mismatch"
    CAPABILITY_VIOLATION = "capability_violation"
    PLUGIN_FAILURE = "plugin_failure"
    RESOURCE_LIMIT = "resource_limit"


class BrokerCapabilityError(ValueError):
    """Closed diagnostics: never render a plugin exception or configuration."""

    def __init__(self, reason: BrokerCapabilityReason) -> None:
        if type(reason) is not BrokerCapabilityReason:
            raise ValueError("invalid capability reason")
        self.reason = reason
        super().__init__(reason.value)

    def to_dict(self) -> dict[str, object]:
        return {"status": "refused", "reason": self.reason.value}


def _invalid() -> NoReturn:
    raise BrokerCapabilityError(BrokerCapabilityReason.INVALID_ARTIFACT)


def _atom(value: str) -> None:
    if type(value) is not str or len(value) > 128 or not _ATOM.fullmatch(value):
        _invalid()


def _atoms(value: tuple[str, ...]) -> None:
    if type(value) is not tuple or len(value) > MAX_CAPABILITY_ITEMS:
        _invalid()
    for item in value:
        _atom(item)
    if tuple(sorted(set(value))) != value:
        _invalid()


def _identity(value: str, kind: str) -> None:
    if type(value) is not str or not re.fullmatch(
        re.escape(kind) + r":sha256:[a-f0-9]{64}", value
    ):
        _invalid()


def _charge(remaining: list[int], cost: int) -> None:
    remaining[0] -= cost
    if remaining[0] < 0:
        _invalid()


def _wire(
    value: object, depth: int = 0, remaining: list[int] | None = None
) -> object:
    # Count the expanded canonical JSON, not unique Python object identities.
    # Compact repeated-reference graphs must not allocate unbounded copies
    # before the final envelope check. The budget is local to this traversal.
    if remaining is None:
        remaining = [MAX_CAPABILITY_BYTES]
    if depth > MAX_CAPABILITY_DEPTH:
        _invalid()
    if isinstance(value, _Artifact) or type(value) in _PUBLIC:
        return _wire(cast(Any, value).to_dict(), depth + 1, remaining)
    if isinstance(value, Enum):
        return _wire(value.value, depth, remaining)
    if value is None or type(value) in (str, bool, int):
        if type(value) is str and len(value) > 65_536:
            _invalid()
        if type(value) is int and not -(2**63) <= value < 2**63:
            _invalid()
        _charge(remaining, len(json.dumps(value, ensure_ascii=True)))
        return value
    if type(value) in (list, tuple):
        sequence = cast(list[object] | tuple[object, ...], value)
        if len(sequence) > MAX_CAPABILITY_ITEMS:
            _invalid()
        _charge(remaining, 2 + max(0, len(sequence) - 1))
        return [_wire(item, depth + 1, remaining) for item in sequence]
    if type(value) is dict:
        if len(value) > MAX_CAPABILITY_ITEMS:
            _invalid()
        _charge(remaining, 2 + max(0, len(value) - 1))
        result = {}
        for key, item in value.items():
            if type(key) is not str or len(key) > 128:
                _invalid()
            _charge(remaining, len(json.dumps(key, ensure_ascii=True)) + 1)
            result[key] = _wire(item, depth + 1, remaining)
        return result
    _invalid()
    raise AssertionError


def canonical_capability_json(value: object) -> str:
    try:
        result = json.dumps(
            _wire(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        if len(result) > MAX_CAPABILITY_BYTES:
            _invalid()
        return result
    except (TypeError, ValueError, RecursionError, OverflowError):
        raise BrokerCapabilityError(
            BrokerCapabilityReason.INVALID_ARTIFACT
        ) from None


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            _invalid()
        result[key] = value
    return result


def _decode(annotation: object, value: object) -> Any:
    args = get_args(annotation)
    origin = get_origin(annotation)
    if origin is UnionType:
        if value is None and type(None) in args:
            return None
        for choice in args:
            if choice is not type(None):
                try:
                    return _decode(choice, value)
                except (TypeError, ValueError):
                    pass
        _invalid()
    if origin is tuple:
        if type(value) is not list or len(value) > MAX_CAPABILITY_ITEMS:
            _invalid()
        return tuple(_decode(args[0], item) for item in value)
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
            for item in fields(cast(Any, self)):
                object.__setattr__(
                    self,
                    item.name,
                    _decode(
                        annotations[item.name], _wire(getattr(self, item.name))
                    ),
                )
            self._validate()
            self.to_json()
        except Exception:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.INVALID_ARTIFACT
            ) from None

    def _validate(self) -> None:
        raise NotImplementedError

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": "histdatacom.broker-capability."
            + self.KIND
            + ".v1",
            **{
                item.name: _wire(getattr(self, item.name))
                for item in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        return (
            "broker-capability-"
            + self.KIND
            + ":sha256:"
            + hashlib.sha256(
                canonical_capability_json(self._payload()).encode("ascii")
            ).hexdigest()
        )

    def to_dict(self) -> dict[str, object]:
        return {**self._payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        return canonical_capability_json(self.to_dict())

    @classmethod
    def from_dict(cls: type[_T], value: Mapping[str, object]) -> _T:
        try:
            canonical_capability_json(value)
            names = {item.name for item in fields(cast(Any, cls))}
            if set(value) != names | {"schema_version", "artifact_id"}:
                _invalid()
            result = cls(**{name: value[name] for name in names})
            if canonical_capability_json(value) != result.to_json():
                _invalid()
            return result
        except Exception:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.INVALID_ARTIFACT
            ) from None

    @classmethod
    def from_json(cls: type[_T], text: str) -> _T:
        try:
            if (
                type(text) is not str
                or len(text.encode("utf-8")) > MAX_CAPABILITY_BYTES
            ):
                _invalid()
            value = json.loads(text, object_pairs_hook=_pairs)
            if type(value) is not dict:
                _invalid()
            return cls.from_dict(value)
        except Exception:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.INVALID_ARTIFACT
            ) from None


@dataclass(frozen=True, slots=True)
class BrokerCapabilityDefinitionV1(_Artifact):
    capability_id: str
    meaning: str
    sdk_field: str
    KIND: ClassVar[str] = "definition"

    def _validate(self) -> None:
        _atom(self.capability_id)
        if not self.capability_id.endswith(".v1"):
            _invalid()
        for text in (self.meaning, self.sdk_field):
            if (
                not text
                or len(text) > 512
                or not text.isascii()
                or any(ord(c) < 32 for c in text)
            ):
                _invalid()


@dataclass(frozen=True, slots=True)
class BrokerOperationV1(_Artifact):
    operation_id: str
    sdk_method: str
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    executable: bool = True
    prerequisites: tuple[str, ...] = ()
    KIND: ClassVar[str] = "operation"

    def _validate(self) -> None:
        _atom(self.operation_id)
        _atom(self.sdk_method)
        _atoms(self.required)
        _atoms(self.optional)
        _atoms(self.prerequisites)
        if set(self.required) & set(self.optional):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerCapabilityWorkflowV1(_Artifact):
    operations: tuple[str, ...]
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    KIND: ClassVar[str] = "workflow"

    def _validate(self) -> None:
        for values in (self.operations, self.required, self.optional):
            _atoms(values)
        if not self.operations or set(self.required) & set(self.optional):
            _invalid()


class BrokerFieldSupport(str, Enum):
    PRESENT = "present"
    UNSUPPORTED = "unsupported"
    NOT_REPORTED = "supported_not_reported"
    NOT_REQUESTED = "not_requested"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class BrokerFieldReceiptV1(_Artifact):
    field: str
    capability_id: str
    support: BrokerFieldSupport
    KIND: ClassVar[str] = "field-receipt"

    def _validate(self) -> None:
        _atom(self.field)
        _atom(self.capability_id)


@dataclass(frozen=True, slots=True)
class BrokerCapabilityPlanV1(_Artifact):
    inventory_id: str
    candidate: BrokerPluginCandidateV1
    catalog_id: str
    workflow: BrokerCapabilityWorkflowV1
    required: tuple[str, ...]
    optional: tuple[str, ...]
    missing_required: tuple[str, ...]
    unsupported_required: tuple[str, ...]
    enabled_optional: tuple[str, ...]
    unsupported_operations: tuple[str, ...]
    KIND: ClassVar[str] = "plan"

    def _validate(self) -> None:
        _identity(self.inventory_id, "broker-plugin-inventory")
        _identity(self.catalog_id, "broker-capability-catalog")
        for name in (
            "required",
            "optional",
            "missing_required",
            "unsupported_required",
            "enabled_optional",
            "unsupported_operations",
        ):
            _atoms(getattr(self, name))
        from .negotiation import _verify_plan_fields

        _verify_plan_fields(self)

    @property
    def admitted(self) -> bool:
        return not (
            self.missing_required
            or self.unsupported_required
            or self.unsupported_operations
        )

    def require_admitted(self) -> None:
        if self.unsupported_operations:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.UNSUPPORTED_OPERATION
            )
        if not self.admitted:
            raise BrokerCapabilityError(
                BrokerCapabilityReason.UNSUPPORTED_CAPABILITY
            )


@dataclass(frozen=True, slots=True)
class BrokerAdmittedEventV1(_Artifact):
    plan_id: str
    event: BrokerEventV1
    fields: tuple[BrokerFieldReceiptV1, ...]
    KIND: ClassVar[str] = "admitted-event"

    def _validate(self) -> None:
        _identity(self.plan_id, "broker-capability-plan")
        names = tuple(item.field for item in self.fields)
        if names != tuple(sorted(set(names))):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerAdmittedInstrumentV1(_Artifact):
    plan_id: str
    instrument: BrokerInstrumentV1
    fields: tuple[BrokerFieldReceiptV1, ...]
    KIND: ClassVar[str] = "admitted-instrument"

    def _validate(self) -> None:
        _identity(self.plan_id, "broker-capability-plan")
        if tuple(item.field for item in self.fields) != ("price_increment",):
            _invalid()


@dataclass(frozen=True, slots=True)
class BrokerAdmittedMetadataV1(_Artifact):
    plan_id: str
    metadata: BrokerPluginMetadataV1
    fields: tuple[BrokerFieldReceiptV1, ...]
    KIND: ClassVar[str] = "admitted-metadata"

    def _validate(self) -> None:
        _identity(self.plan_id, "broker-capability-plan")
        if tuple(item.field for item in self.fields) != (
            "account_class",
            "feed_class",
            "server_label",
        ):
            _invalid()


class BrokerInvocationAssociation(str, Enum):
    CALLER_FACTORY = "caller_factory_identity_only"
    INSTALLED_ENTRYPOINT = "installed_entrypoint_module_verified"


@dataclass(frozen=True, slots=True)
class BrokerInvocationBindingV1(_Artifact):
    plan_id: str
    candidate_id: str
    metadata_id: str
    association: BrokerInvocationAssociation
    module_sha256: str | None = None
    KIND: ClassVar[str] = "invocation-binding"

    def _validate(self) -> None:
        _identity(self.plan_id, "broker-capability-plan")
        _identity(self.candidate_id, "broker-plugin-candidate")
        _identity(self.metadata_id, "broker-plugin-metadata")
        if self.association is BrokerInvocationAssociation.INSTALLED_ENTRYPOINT:
            if self.module_sha256 is None or not re.fullmatch(
                r"[a-f0-9]{64}", self.module_sha256
            ):
                _invalid()
        elif self.module_sha256 is not None:
            _invalid()

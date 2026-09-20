"""Bounded, immutable compatibility metadata; never executable migration code."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, fields
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

REGISTRY_SCHEMA_VERSION = "histdatacom.schema-compatibility-registry.v1"
MAX_REGISTRY_BYTES = 16 * 1024 * 1024
MAX_REGISTRY_NODES = 400_000
MAX_REGISTRY_ITEMS = 16_384
_T = TypeVar("_T", bound="Contract")


def canonical_json(value: object) -> str:
    """Bound expanded input before encoding, including repeated aliases."""
    pending = [(value, 0)]
    count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if count > MAX_REGISTRY_NODES or depth > 20:
            raise ValueError("compatibility metadata expansion bound")
        if type(item) is dict:
            if len(item) > MAX_REGISTRY_ITEMS:
                raise ValueError("compatibility metadata collection bound")
            for key, child in item.items():
                if type(key) is not str or len(key) > 512:
                    raise ValueError("invalid compatibility object key")
                pending.append((child, depth + 1))
        elif type(item) in (list, tuple):
            sequence = cast(list[object] | tuple[object, ...], item)
            if len(sequence) > MAX_REGISTRY_ITEMS:
                raise ValueError("compatibility metadata collection bound")
            pending.extend((child, depth + 1) for child in sequence)
        elif type(item) is str:
            if len(item) > 8192:
                raise ValueError("compatibility metadata string bound")
        elif item is not None and type(item) not in (bool, int):
            raise ValueError("invalid compatibility scalar")
        elif type(item) is int and not -(2**63) <= item < 2**63:
            raise ValueError("invalid compatibility integer")
    try:
        text = json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
    except (ValueError, TypeError, RecursionError) as exc:
        raise ValueError("invalid compatibility JSON") from exc
    if len(text) > MAX_REGISTRY_BYTES:
        raise ValueError("compatibility metadata byte bound")
    return text


def _wire(value: object) -> object:
    if isinstance(value, Contract):
        return value.to_dict()
    if isinstance(value, Enum):
        return value.value
    if type(value) is tuple:
        return [_wire(item) for item in value]
    return value


def _decode(hint: Any, value: object) -> Any:
    origin = get_origin(hint)
    args = get_args(hint)
    if origin in (Union, UnionType):
        for option in args:
            try:
                return _decode(option, value)
            except (ValueError, TypeError):
                pass
        raise ValueError("invalid compatibility optional value")
    if origin is tuple:
        if type(value) not in (list, tuple):
            raise ValueError("expected compatibility array")
        items = cast(Any, value)
        if len(items) > MAX_REGISTRY_ITEMS:
            raise ValueError("compatibility array bound")
        return tuple(_decode(args[0], item) for item in items)
    if isinstance(hint, type) and issubclass(hint, Contract):
        if type(value) is hint:
            return value
        if type(value) is not dict:
            raise ValueError("expected compatibility object")
        return hint.from_dict(cast(dict[str, object], value))
    if isinstance(hint, type) and issubclass(hint, Enum):
        if type(value) is hint:
            return value
        if type(value) is not str:
            raise ValueError("expected compatibility enum")
        return hint(value)
    if type(value) is not hint:
        raise ValueError("invalid compatibility scalar type")
    return value


def text(value: str) -> None:
    if (
        type(value) is not str
        or not value
        or len(value) > 8192
        or value != value.strip()
        or not value.isascii()
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError("invalid compatibility text")


def digest(value: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("invalid compatibility digest")


def unique(values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError("compatibility values must be sorted and unique")
    for value in values:
        text(value)


class Contract:
    """Strict fields and scalar types; detached deeply immutable tuples."""

    __slots__ = ()

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        for field in fields(cast(Any, self)):
            object.__setattr__(
                self,
                field.name,
                _decode(hints[field.name], getattr(self, field.name)),
            )
        self._validate()
        canonical_json(self.to_dict())

    def _validate(self) -> None:
        pass

    def to_dict(self) -> dict[str, object]:
        return {
            field.name: _wire(getattr(self, field.name))
            for field in fields(cast(Any, self))
        }

    @classmethod
    def from_dict(  # noqa: PYI019 -- stdlib-only Python 3.10 public API
        cls: type[_T], value: dict[str, object]
    ) -> _T:
        canonical_json(value)
        if type(value) is not dict or set(value) != {
            field.name for field in fields(cast(Any, cls))
        }:
            raise ValueError("unknown or missing compatibility field")
        return cls(**value)


class MigrationClassification(str, Enum):
    LOSSLESS_REPRESENTATION = "lossless_representation"
    SEMANTIC_SUCCESSOR = "semantic_successor"
    LOSSY_ADVISORY = "lossy_advisory"
    UNSUPPORTED = "unsupported"


class SupportStatus(str, Enum):
    SUPPORTED = "supported"
    READ_ONLY = "read_only"
    WRITE_ONLY = "write_only"
    DEPRECATED = "deprecated"
    UNSUPPORTED = "unsupported"


class EvidenceKind(str, Enum):
    SOURCE_DEFINITION = "source_definition"
    TEST_DEFINITION = "test_definition"
    INVARIANT_CHECK = "invariant_check"
    GOLDEN_CORPUS = "golden_corpus"
    COMPOSITION = "composition"


@dataclass(frozen=True, slots=True)
class ImplementationV1(Contract):
    implementation_id: str
    qualified_name: str
    version: str
    source_sha256: str

    def _validate(self) -> None:
        for value in (
            self.implementation_id,
            self.qualified_name,
            self.version,
        ):
            text(value)
        digest(self.source_sha256)


@dataclass(frozen=True, slots=True)
class EvidenceV1(Contract):
    evidence_id: str
    kind: EvidenceKind
    locator: str
    sha256: str

    def _validate(self) -> None:
        text(self.evidence_id)
        text(self.locator)
        digest(self.sha256)


@dataclass(frozen=True, slots=True)
class SchemaV1(Contract):
    schema_id: str
    family: str
    version: str
    wire_schema: str
    status: SupportStatus
    readers: tuple[str, ...]
    writers: tuple[str, ...]
    invariants: tuple[str, ...]
    evidence: tuple[str, ...]
    note: str

    def _validate(self) -> None:
        from .graph import version_key

        for value in (
            self.schema_id,
            self.family,
            self.version,
            self.wire_schema,
            self.note,
        ):
            text(value)
        version_key(self.version)
        for values in (
            self.readers,
            self.writers,
            self.invariants,
            self.evidence,
        ):
            unique(values)
        if (
            not (self.writers or self.readers)
            or not self.evidence
            or not self.invariants
        ):
            raise ValueError(
                "schema requires implementation, invariants and evidence"
            )
        if self.status is SupportStatus.SUPPORTED and not self.readers:
            raise ValueError("supported schema requires direct reader")


@dataclass(frozen=True, slots=True)
class MigrationEdgeV1(Contract):
    edge_id: str
    source: str
    destination: str
    classification: MigrationClassification
    priority: int
    implementation_id: str | None
    qualified: bool
    source_invariants: tuple[str, ...]
    destination_invariants: tuple[str, ...]
    evidence: tuple[str, ...]

    def _validate(self) -> None:
        for value in (self.edge_id, self.source, self.destination):
            text(value)
        if self.implementation_id is not None:
            text(self.implementation_id)
        if not 0 <= self.priority <= 1_000_000:
            raise ValueError("invalid migration priority")
        for values in (
            self.source_invariants,
            self.destination_invariants,
            self.evidence,
        ):
            unique(values)
        if self.qualified and (
            self.implementation_id is None
            or self.classification is MigrationClassification.UNSUPPORTED
            or not self.source_invariants
            or not self.destination_invariants
            or not self.evidence
        ):
            raise ValueError("incomplete migration qualification")


@dataclass(frozen=True, slots=True)
class CompositionV1(Contract):
    edge_ids: tuple[str, ...]
    evidence: tuple[str, ...]

    def _validate(self) -> None:
        if not 2 <= len(self.edge_ids) <= 64:
            raise ValueError("invalid composition length")
        for value in self.edge_ids:
            text(value)
        unique(self.evidence)
        if not self.evidence:
            raise ValueError("composition evidence required")


@dataclass(frozen=True, slots=True)
class ExemptionV1(Contract):
    qualified_name: str
    reason: str
    source_sha256: str
    wire_schema: str | None = None

    def _validate(self) -> None:
        text(self.qualified_name)
        text(self.reason)
        digest(self.source_sha256)
        if self.wire_schema is not None:
            text(self.wire_schema)


@dataclass(frozen=True, slots=True)
class CompatibilityRegistryV1(Contract):
    schemas: tuple[SchemaV1, ...]
    implementations: tuple[ImplementationV1, ...]
    evidence: tuple[EvidenceV1, ...]
    migrations: tuple[MigrationEdgeV1, ...]
    compositions: tuple[CompositionV1, ...]
    exemptions: tuple[ExemptionV1, ...]
    schema_version: str = REGISTRY_SCHEMA_VERSION
    KIND: ClassVar[str] = "schema-compatibility-registry"

    def _validate(self) -> None:
        if self.schema_version != REGISTRY_SCHEMA_VERSION:
            raise ValueError("unsupported compatibility registry schema")
        if (
            len(self.schemas) > 4096
            or len(self.migrations) > 512
            or len(self.compositions) > 512
        ):
            raise ValueError("compatibility graph capacity exceeded")
        from .graph import validate_registry

        validate_registry(self)

    @property
    def registry_id(self) -> str:
        return (
            "schema-compatibility:sha256:"
            + hashlib.sha256(self.to_json().encode("ascii")).hexdigest()
        )

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_json(cls, value: str) -> CompatibilityRegistryV1:
        if type(value) is not str or len(value) > MAX_REGISTRY_BYTES:
            raise ValueError("compatibility JSON byte bound")

        def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, item in items:
                if key in result:
                    raise ValueError("duplicate compatibility field")
                result[key] = item
            return result

        try:
            data = json.loads(value, object_pairs_hook=pairs)
        except (RecursionError, OverflowError) as exc:
            raise ValueError("invalid compatibility JSON") from exc
        result = cls.from_dict(data)
        if result.to_json() != value:
            raise ValueError("noncanonical compatibility JSON")
        return result

    def can_read(
        self,
        schema: str,
        *,
        reader: str | None = None,
        reader_version: str | None = None,
    ) -> bool:
        from .graph import can_read

        return can_read(
            self, schema, reader=reader, reader_version=reader_version
        )

    def migration_path(
        self, source: str, destination: str, *, exact: bool = False
    ) -> tuple[MigrationEdgeV1, ...]:
        from .graph import migration_path

        return migration_path(self, source, destination, exact=exact)

    def can_migrate(
        self, source: str, destination: str, *, exact: bool = False
    ) -> bool:
        from .graph import CompatibilityRefusal

        try:
            self.migration_path(source, destination, exact=exact)
        except CompatibilityRefusal:
            return False
        return True

    def exact_semantics(self, source: str, destination: str) -> bool:
        """Whether metadata qualifies identity or an exact lossless path."""
        return self.can_migrate(source, destination, exact=True)

"""Bounded exact wire encoding for retention authority, never native payloads."""

from __future__ import annotations

import hashlib
import json
import types
from dataclasses import fields
from enum import Enum
from typing import (
    Any,
    ClassVar,
    Literal,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

MAX_WIRE_BYTES = 16 * 1024 * 1024
MAX_WIRE_NODES = 262_144
MAX_WIRE_DEPTH = 32
MAX_COLLECTION_ITEMS = 8192
MAX_INTEGER = 2**63 - 1
_T = TypeVar("_T", bound="RetentionContract")


def sha256(value: bytes) -> str:
    """Hash exact bytes without normalizing an artifact."""
    if type(value) is not bytes:
        raise ValueError("retention hashing requires exact bytes")
    return hashlib.sha256(value).hexdigest()


def _integer(text: str) -> int:
    if len(text) > 20:
        raise ValueError("retention integer exceeds bounds")
    value = int(text)
    if not -MAX_INTEGER <= value <= MAX_INTEGER:
        raise ValueError("retention integer exceeds bounds")
    return value


def _no_float(_text: str) -> None:
    raise ValueError("retention authority does not admit floating numbers")


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate retention JSON key")
        result[key] = value
    return result


def _lexical_bound(text: str) -> None:
    """Bound containers and value starts before allocating a parsed tree."""
    if (
        type(text) is not str
        or len(text) > MAX_WIRE_BYTES
        or not text.isascii()
    ):
        raise ValueError("retention JSON requires bounded ASCII encoding")
    depth = nodes = 0
    quoted = escaped = atom = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            nodes += 1
            quoted = True
            atom = False
        elif char in "[{":
            depth += 1
            nodes += 1
            atom = False
        elif char in "]}":
            depth -= 1
            atom = False
        elif char in " ,:\r\n\t":
            atom = False
        elif not atom:
            nodes += 1
            atom = True
        if not 0 <= depth <= MAX_WIRE_DEPTH or nodes > MAX_WIRE_NODES:
            raise ValueError("retention JSON structural bound exceeded")


def _bound(value: Any, *, contracts: bool = False) -> None:
    pending = [(value, 0)]
    nodes = size = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > MAX_WIRE_DEPTH or nodes > MAX_WIRE_NODES:
            raise ValueError("retention wire structural bound exceeded")
        if contracts and isinstance(item, RetentionContract):
            members = fields(item)  # type: ignore[arg-type]
            size += len(item.SCHEMA) + 30
            pending.extend((member.name, depth + 1) for member in members)
            pending.extend(
                (getattr(item, member.name), depth + 1) for member in members
            )
        elif contracts and isinstance(item, Enum):
            pending.append((item.value, depth))
        elif type(item) is dict:
            if len(item) > MAX_COLLECTION_ITEMS or any(
                type(key) is not str for key in item
            ):
                raise ValueError("retention mapping exceeds bounds")
            size += 2 + 2 * len(item)
            pending.extend((key, depth + 1) for key in item)
            pending.extend((child, depth + 1) for child in item.values())
        elif type(item) in (list, tuple):
            if len(item) > MAX_COLLECTION_ITEMS:
                raise ValueError("retention collection exceeds bounds")
            size += 2 + len(item)
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item) > MAX_WIRE_BYTES:
                raise ValueError("retention string exceeds bounds")
            size += 2
            for char in item:
                code = ord(char)
                if 0xD800 <= code <= 0xDFFF:
                    raise ValueError(
                        "retention strings require Unicode scalars"
                    )
                size += (
                    2
                    if char in '\\"\b\f\n\r\t'
                    else (
                        6
                        if code < 32 or 127 <= code <= 65535
                        else 12 if code > 65535 else 1
                    )
                )
                if size > MAX_WIRE_BYTES:
                    raise ValueError("retention escaped string exceeds bounds")
        elif item is None or type(item) is bool:
            size += 5
        elif type(item) is int and -MAX_INTEGER <= item <= MAX_INTEGER:
            size += 20
        else:
            raise ValueError("unsupported retention wire value")
        if size > MAX_WIRE_BYTES or nodes + len(pending) > MAX_WIRE_NODES:
            raise ValueError("retention expanded wire exceeds bounds")


def canonical_json(value: Any) -> str:
    """Serialize a bounded exact JSON tree, without floats or coercion."""
    _bound(value)
    result = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    if len(result) > MAX_WIRE_BYTES:
        raise ValueError("retention encoded bytes exceed bounds")
    return result


def load_json(text: str) -> Any:
    """Parse bounded JSON; contract readers additionally require canonical bytes."""
    _lexical_bound(text)
    try:
        result = json.loads(
            text,
            object_pairs_hook=_pairs,
            parse_int=_integer,
            parse_float=_no_float,
            parse_constant=_no_float,
        )
    except (RecursionError, OverflowError, json.JSONDecodeError) as exc:
        raise ValueError("invalid retention JSON") from exc
    _bound(result)
    return result


def _decode(hint: Any, value: Any, *, wire: bool) -> Any:
    origin, args = get_origin(hint), get_args(hint)
    if origin in (Union, types.UnionType):
        for choice in args:
            try:
                return _decode(choice, value, wire=wire)
            except ValueError:
                continue
        raise ValueError("retention union value has an invalid exact type")
    if origin is Literal:
        if not any(
            type(value) is type(item) and value == item for item in args
        ):
            raise ValueError("unknown retention literal")
        return value
    if origin is tuple:
        if (
            len(args) != 2
            or args[1] is not Ellipsis
            or type(value) is not (list if wire else tuple)
            or len(value) > MAX_COLLECTION_ITEMS
        ):
            raise ValueError("retention collection type or bound is invalid")
        return tuple(_decode(args[0], item, wire=wire) for item in value)
    if isinstance(hint, type) and issubclass(hint, RetentionContract):
        if wire:
            return hint.from_dict(value)
        if type(value) is hint:
            value._readmit()
            return value
        raise ValueError("retention nested contract has an invalid exact type")
    if isinstance(hint, type) and issubclass(hint, Enum):
        if wire and type(value) is str:
            return hint(value)
        if not wire and type(value) is hint:
            return value
        raise ValueError("retention enum has an invalid exact type")
    if hint not in (str, int, bool, type(None)) or type(value) is not hint:
        raise ValueError("retention scalar has an invalid exact type")
    return value


def _wire(value: Any) -> Any:
    if isinstance(value, RetentionContract):
        return value.to_dict()
    if isinstance(value, Enum):
        return value.value
    if type(value) is tuple:
        return [_wire(item) for item in value]
    return value


def _shape_matches(hint: Any, value: Any) -> bool:
    """Select only an exact constructor type, without invoking its methods."""
    origin, args = get_origin(hint), get_args(hint)
    if origin in (Union, types.UnionType):
        return any(_shape_matches(choice, value) for choice in args)
    if origin is Literal:
        return any(type(value) is type(item) and value == item for item in args)
    return type(value) is (tuple if origin is tuple else hint)


def _constructor_shape(value: RetentionContract) -> None:
    """Check nested exact families before reading any nested contract fields.

    The bounded iterative pass also refuses forged cyclic frozen instances
    before recursive domain re-admission. Plain wire trees never enter here.
    """
    pending: list[tuple[Any, Any, int]] = [(type(value), value, 0)]
    nodes = 0
    while pending:
        hint, item, depth = pending.pop()
        nodes += 1
        if depth > MAX_WIRE_DEPTH or nodes + len(pending) > MAX_WIRE_NODES:
            raise ValueError("retention constructor structural bound exceeded")
        if not _shape_matches(hint, item):
            raise ValueError("retention constructor has an invalid exact type")
        origin, args = get_origin(hint), get_args(hint)
        if origin in (Union, types.UnionType):
            hint = next(
                choice for choice in args if _shape_matches(choice, item)
            )
            origin, args = get_origin(hint), get_args(hint)
        if origin is tuple:
            if (
                len(args) != 2
                or args[1] is not Ellipsis
                or len(item) > MAX_COLLECTION_ITEMS
            ):
                raise ValueError(
                    "retention collection type or bound is invalid"
                )
            pending.extend((args[0], child, depth + 1) for child in item)
        elif isinstance(hint, type) and issubclass(hint, RetentionContract):
            hints = get_type_hints(hint)
            pending.extend(
                (hints[member.name], getattr(item, member.name), depth + 1)
                for member in fields(hint)  # type: ignore[arg-type]
            )


class RetentionContract:
    """Abstract strict encoding; serialized declarations are not GC authority."""

    __slots__ = ()
    SCHEMA: ClassVar[str]
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        self._readmit()

    def _readmit(self) -> None:
        # Bound before recursively checking child contracts: even a forged
        # frozen instance containing a cycle must fail without recursion.
        _constructor_shape(self)
        _bound(self, contracts=True)
        hints = get_type_hints(type(self))
        for member in fields(self):  # type: ignore[arg-type]
            _decode(hints[member.name], getattr(self, member.name), wire=False)
        self._validate()

    def _validate(self) -> None:
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        self._readmit()
        return {
            "schema_version": self.SCHEMA,
            **{
                member.name: _wire(getattr(self, member.name))
                for member in fields(self)  # type: ignore[arg-type]
            },
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def artifact_id(self) -> str:
        return f"retention-{self.KIND}:sha256:{sha256(self.to_json().encode('ascii'))}"

    @classmethod
    def from_dict(cls: type[_T], value: Any) -> _T:  # noqa: PYI019
        _bound(value)
        members = {member.name for member in fields(cls)}  # type: ignore[arg-type]
        if (
            type(value) is not dict
            or set(value) != members | {"schema_version"}
            or value["schema_version"] != cls.SCHEMA
        ):
            raise ValueError("unknown retention fields or schema")
        hints = get_type_hints(cls)
        return cls(
            **{
                name: _decode(hints[name], value[name], wire=True)
                for name in members
            }
        )

    @classmethod
    def from_json(cls: type[_T], text: str) -> _T:  # noqa: PYI019
        result = cls.from_dict(load_json(text))
        if result.to_json() != text:
            raise ValueError("noncanonical retention contract bytes")
        return result

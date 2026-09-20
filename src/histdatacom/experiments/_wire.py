"""Strict, bounded v1 wire primitives; no external artifact authenticity claim."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import fields
from enum import Enum
from functools import lru_cache
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

MAX_BYTES = 8 * 1024 * 1024
MAX_NODES = 131_072
MAX_ITEMS = 4096
MAX_DEPTH = 20
MAX_TEXT = 65_536
_T = TypeVar("_T", bound="Record")


def plain(value: object) -> object:
    """Charge expanded aliases AND repeated string bytes before allocation."""
    nodes = 0
    size = 0

    def visit(item: object, depth: int) -> object:
        nonlocal nodes, size
        nodes += 1
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("experiment expanded node/depth bound")
        if isinstance(item, Record):
            item = {
                f.name: getattr(item, f.name) for f in fields(cast(Any, item))
            }
        if isinstance(item, Enum):
            item = item.value
        if type(item) is dict:
            if len(item) > MAX_ITEMS:
                raise ValueError("experiment collection bound")
            size += 2 + 2 * len(item)
            output: dict[str, object] = {}
            for key, child in item.items():
                if type(key) is not str or len(key) > 256:
                    raise ValueError("invalid experiment object key")
                visit(key, depth + 1)
                output[key] = visit(child, depth + 1)
            result: object = output
        elif type(item) in (list, tuple):
            sequence = cast(list[object] | tuple[object, ...], item)
            if len(sequence) > MAX_ITEMS:
                raise ValueError("experiment collection bound")
            size += 2 + len(sequence)
            result = [visit(child, depth + 1) for child in sequence]
        else:
            if type(item) is str:
                if len(item) > MAX_TEXT:
                    raise ValueError("experiment string bound")
            elif type(item) is int:
                if not -(2**63) <= item < 2**63:
                    raise ValueError("experiment integer bound")
            elif type(item) is float:
                if not math.isfinite(item):
                    raise ValueError("experiment non-finite number")
            elif item is not None and type(item) is not bool:
                raise ValueError("unsupported experiment scalar")
            try:
                size += len(
                    json.dumps(item, ensure_ascii=True, allow_nan=False)
                )
            except (ValueError, TypeError) as exc:
                raise ValueError("invalid experiment scalar") from exc
            result = item
        if size > MAX_BYTES:
            raise ValueError("experiment byte bound")
        return result

    return visit(value, 0)


def canonical_json(value: object) -> str:
    text = json.dumps(
        plain(value),
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(text) > MAX_BYTES:
        raise ValueError("experiment byte bound")
    return text


def load_json(text: str) -> object:
    if type(text) is not str or len(text) > MAX_BYTES:
        raise ValueError("experiment JSON byte bound")

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate experiment JSON key")
            result[key] = value
        return result

    try:
        result = json.loads(text, object_pairs_hook=pairs)
        if canonical_json(result) != text:
            raise ValueError("noncanonical experiment JSON")
    except (RecursionError, TypeError, OverflowError) as exc:
        raise ValueError("invalid experiment JSON") from exc
    return result


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def identity(kind: str, payload: object) -> str:
    return f"experiment-{kind}:sha256:" + sha256(canonical_json(payload))


def text(value: str) -> None:
    if not value or value != value.strip() or any(ord(c) < 32 for c in value):
        raise ValueError("invalid experiment text")


def digest(value: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("invalid experiment digest")


def identifier(value: str, kind: str | None = None) -> None:
    pattern = r"experiment-[a-z-]+:sha256:[0-9a-f]{64}"
    if re.fullmatch(pattern, value) is None or (
        kind is not None and not value.startswith(f"experiment-{kind}:sha256:")
    ):
        raise ValueError("invalid experiment artifact identity")


def ordered(values: tuple[str, ...], *, nonempty: bool = False) -> None:
    if (nonempty and not values) or values != tuple(sorted(set(values))):
        raise ValueError("experiment values must be sorted unique")
    for value in values:
        text(value)


@lru_cache(maxsize=64)
def _hints(cls: Any) -> dict[str, Any]:
    return get_type_hints(cls)


def _decode(hint: Any, value: object) -> Any:
    origin, args = get_origin(hint), get_args(hint)
    if origin in (Union, UnionType):
        for alternative in args:
            try:
                return _decode(alternative, value)
            except (ValueError, TypeError):
                pass
        raise ValueError("invalid experiment union")
    if origin is tuple:
        if type(value) not in (tuple, list):
            raise ValueError("expected experiment tuple")
        sequence = cast(list[object] | tuple[object, ...], value)
        if len(sequence) > MAX_ITEMS:
            raise ValueError("experiment collection bound")
        return tuple(_decode(args[0], child) for child in sequence)
    if isinstance(hint, type) and issubclass(hint, Record):
        if type(value) is hint:
            return value
        if type(value) is not dict:
            raise ValueError("expected experiment record")
        return hint.from_payload(cast(dict[str, object], value))
    if isinstance(hint, type) and issubclass(hint, Enum):
        if type(value) is hint:
            return value
        if type(value) is not str:
            raise ValueError("expected experiment enum")
        return hint(value)
    if type(value) is not hint:
        raise ValueError("invalid experiment scalar type")
    return value


class Record:
    """Frozen dataclass base with detached immutable fields and exact types."""

    __slots__ = ()

    def __post_init__(self) -> None:
        # Preflight before recursive decoding; compact DAGs cannot amplify work.
        plain(self)
        hints = _hints(cast(Any, type(self)))
        for field in fields(cast(Any, self)):
            object.__setattr__(
                self,
                field.name,
                _decode(hints[field.name], getattr(self, field.name)),
            )
        self._validate()

    def _validate(self) -> None:
        pass

    def to_payload(self) -> dict[str, object]:
        return cast(dict[str, object], plain(self))

    @classmethod
    def from_payload(
        cls: type[_T], payload: dict[str, object]
    ) -> _T:  # noqa: PYI019
        plain(payload)
        if type(payload) is not dict or set(payload) != {
            f.name for f in fields(cast(Any, cls))
        }:
            raise ValueError("unknown or missing experiment field")
        return cls(**payload)


class Artifact(Record):
    """Content address includes the schema; derived IDs are never user inputs."""

    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        super().__post_init__()
        # Constructor acceptance includes the complete outer wire envelope.
        self.to_json()

    @classmethod
    def schema_version(cls) -> str:
        return f"histdatacom.experiment-{cls.KIND}.v1"

    @property
    def artifact_id(self) -> str:
        return identity(
            self.KIND,
            {
                "schema_version": self.schema_version(),
                "payload": self.to_payload(),
            },
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version(),
            "artifact_id": self.artifact_id,
            "payload": self.to_payload(),
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls: type[_T], value: dict[str, object]
    ) -> _T:  # noqa: PYI019
        plain(value)
        if type(value) is not dict or set(value) != {
            "schema_version",
            "artifact_id",
            "payload",
        }:
            raise ValueError("unknown or missing experiment envelope field")
        artifact_cls = cast(type[Artifact], cls)
        if (
            value["schema_version"] != artifact_cls.schema_version()
            or type(value["payload"]) is not dict
        ):
            raise ValueError("wrong experiment schema")
        result = cls.from_payload(cast(dict[str, object], value["payload"]))
        if cast(Artifact, result).artifact_id != value["artifact_id"]:
            raise ValueError("experiment content identity mismatch")
        return result

    @classmethod
    def from_json(cls: type[_T], text: str) -> _T:  # noqa: PYI019
        value = load_json(text)
        if type(value) is not dict:
            raise ValueError("expected experiment envelope")
        return cast(_T, cast(Any, cls).from_dict(value))

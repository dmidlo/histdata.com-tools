"""Bounded canonical health evidence; no plugin-defined metadata or callbacks."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import fields
from enum import Enum
from types import UnionType
from typing import (
    Any,
    ClassVar,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

MAX_HEALTH_BYTES = 8_388_608
MAX_HEALTH_ITEMS = 4096
T = TypeVar("T", bound="Artifact")


def _fail() -> None:
    raise ValueError("invalid bounded host-health evidence")


def wire(value: object, depth: int = 0, budget: list[int] | None = None) -> Any:
    if budget is None:
        budget = [131_072]
    budget[0] -= 1
    if depth > 24 or budget[0] < 0:
        _fail()
    if isinstance(value, Artifact):
        return wire(value.to_dict(), depth + 1, budget)
    if isinstance(value, Enum):
        return value.value
    if value is None or type(value) is bool:
        return value
    if type(value) is int and abs(value) < 2**63:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is str and len(value) <= 4096:
        return value
    if type(value) in (tuple, list):
        if len(cast(Any, value)) > MAX_HEALTH_ITEMS:
            _fail()
        return [wire(item, depth + 1, budget) for item in cast(Any, value)]
    if type(value) is dict:
        values = cast(dict[object, object], value)
        if len(values) > 128 or any(
            type(key) is not str or len(key) > 128 for key in values
        ):
            _fail()
        return {
            key: wire(item, depth + 1, budget) for key, item in values.items()
        }
    _fail()


def canonical_health_json(value: object) -> str:
    result = json.dumps(
        wire(value),
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(result) > MAX_HEALTH_BYTES:
        _fail()
    return result


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            _fail()
        result[key] = value
    return result


def parse_health_json(text: str) -> dict[str, object]:
    if type(text) is not str or len(text) > MAX_HEALTH_BYTES:
        _fail()
    value = json.loads(text, object_pairs_hook=_pairs)
    if type(value) is not dict:
        _fail()
    canonical_health_json(value)
    return cast(dict[str, object], value)


def decode(annotation: object, value: object) -> Any:
    args, origin = get_args(annotation), get_origin(annotation)
    if origin is UnionType:
        if value is None and type(None) in args:
            return None
        for choice in args:
            if choice is not type(None):
                try:
                    return decode(choice, value)
                except (TypeError, ValueError):
                    pass
        _fail()
    if origin is tuple:
        if (
            type(value) not in (tuple, list)
            or len(cast(Any, value)) > MAX_HEALTH_ITEMS
        ):
            _fail()
        return tuple(decode(args[0], item) for item in cast(Any, value))
    if annotation in (str, int, float, bool):
        if type(value) is not annotation:
            _fail()
        return value
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation(value)
    if isinstance(annotation, type) and issubclass(annotation, Artifact):
        if type(value) is annotation:
            return annotation.from_dict(value.to_dict())
        if type(value) is not dict:
            _fail()
        return annotation.from_dict(cast(dict[str, object], value))
    _fail()


class Artifact:
    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        annotations = get_type_hints(type(self))
        for field in fields(cast(Any, self)):
            object.__setattr__(
                self,
                field.name,
                decode(annotations[field.name], getattr(self, field.name)),
            )
        self._validate()
        self.to_json()

    def _validate(self) -> None:
        raise NotImplementedError

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": "histdatacom.broker-host-health."
            + self.KIND
            + ".v1",
            **{
                field.name: wire(getattr(self, field.name))
                for field in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        digest = hashlib.sha256(
            canonical_health_json(self._payload()).encode("ascii")
        ).hexdigest()
        return "broker-host-health-" + self.KIND + ":sha256:" + digest

    def to_dict(self) -> dict[str, object]:
        return {**self._payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        return canonical_health_json(self.to_dict())

    @classmethod
    def from_dict(
        cls: type[T], value: Mapping[str, object]
    ) -> T:  # noqa: PYI019
        names = {field.name for field in fields(cast(Any, cls))}
        if type(value) is not dict or set(value) != names | {
            "schema_version",
            "artifact_id",
        }:
            _fail()
        restored = cls(**{name: value[name] for name in names})
        if restored.to_json() != canonical_health_json(value):
            _fail()
        return restored

    @classmethod
    def from_json(cls: type[T], text: str) -> T:  # noqa: PYI019
        return cls.from_dict(parse_health_json(text))

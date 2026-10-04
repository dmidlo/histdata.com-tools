"""Bounded canonical account wires and context-independent exact arithmetic.

These codecs validate structure, not execution, source authenticity or legal
eligibility. No Decimal context, binary float, IO or provider runtime is used.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, fields
from functools import total_ordering
from typing import Any, ClassVar, TypeVar, cast

MAX_COMPONENT_BITS = 256
MAX_WIRE_BYTES = 4 * 1024 * 1024
MAX_DEPTH = 20
MAX_NODES = 262144
MAX_EVENTS = 1024
MAX_ALLOCATIONS = 2 * MAX_EVENTS
MAX_PAIRS = 32
MAX_TEXT = 512
MAX_CLOCK = 2**63 - 1
_DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_IDENTIFIER = re.compile(r"[a-z][a-z0-9-]{0,63}:sha256:[0-9a-f]{64}\Z")


def _integer(value: Any, name: str) -> None:
    if type(value) is not int or abs(value).bit_length() > MAX_COMPONENT_BITS:
        raise ValueError(f"{name} requires an exact bounded integer")


@total_ordering
@dataclass(frozen=True, slots=True)
class ExactAmountV1:
    """A reduced fraction, with every public arithmetic result bounded."""

    numerator: int
    denominator: int = 1

    def __post_init__(self) -> None:
        _integer(self.numerator, "numerator")
        _integer(self.denominator, "denominator")
        if self.denominator <= 0:
            raise ValueError("denominator must be positive")
        if math.gcd(self.numerator, self.denominator) != 1:
            raise ValueError("fraction must be reduced, including zero as 0/1")

    @classmethod
    def from_ratio(cls, numerator: int, denominator: int = 1) -> ExactAmountV1:
        _integer(numerator, "numerator")
        _integer(denominator, "denominator")
        return _result(numerator, denominator)

    @classmethod
    def from_decimal(cls, text: str) -> ExactAmountV1:
        if (
            type(text) is not str
            or len(text) > 160
            or _DECIMAL.fullmatch(text) is None
        ):
            raise ValueError("amount requires a bounded plain decimal string")
        negative = text.startswith("-")
        unsigned = text[1:] if negative else text
        whole, dot, fraction = unsigned.partition(".")
        if len(whole) + len(fraction) > 154:
            raise ValueError("decimal coefficient exceeds bounded work")
        numerator = int(whole + fraction)
        if negative:
            numerator = -numerator
        return _result(numerator, 10 ** len(fraction) if dot else 1)

    def to_dict(self) -> dict[str, int]:
        require_amount(self, "amount")
        return {"numerator": self.numerator, "denominator": self.denominator}

    def to_json(self) -> str:
        return canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> ExactAmountV1:
        require_keys(value, {"numerator", "denominator"})
        return cls(value["numerator"], value["denominator"])

    @classmethod
    def from_json(cls, text: str) -> ExactAmountV1:
        result = cls.from_dict(load_canonical(text))
        if result.to_json() != text:
            raise ValueError("amount JSON is not canonical")
        return result

    def __add__(self, other: ExactAmountV1) -> ExactAmountV1:
        require_amount(self, "left amount")
        require_amount(other, "right amount")
        return _result(
            self.numerator * other.denominator
            + other.numerator * self.denominator,
            self.denominator * other.denominator,
        )

    def __sub__(self, other: ExactAmountV1) -> ExactAmountV1:
        require_amount(self, "left amount")
        require_amount(other, "right amount")
        return self + -other

    def __mul__(self, other: ExactAmountV1) -> ExactAmountV1:
        require_amount(self, "left amount")
        require_amount(other, "right amount")
        return _result(
            self.numerator * other.numerator,
            self.denominator * other.denominator,
        )

    def __truediv__(self, other: ExactAmountV1) -> ExactAmountV1:
        require_amount(self, "left amount")
        require_amount(other, "right amount")
        return _result(
            self.numerator * other.denominator,
            self.denominator * other.numerator,
        )

    def __neg__(self) -> ExactAmountV1:
        require_amount(self, "amount")
        return ExactAmountV1(-self.numerator, self.denominator)

    def __abs__(self) -> ExactAmountV1:
        require_amount(self, "amount")
        return ExactAmountV1(abs(self.numerator), self.denominator)

    def __lt__(self, other: Any) -> bool:
        require_amount(self, "left amount")
        require_amount(other, "right amount")
        admitted = cast(ExactAmountV1, other)
        return self.numerator * admitted.denominator < (
            admitted.numerator * self.denominator
        )


def _result(numerator: int, denominator: int) -> ExactAmountV1:
    if denominator == 0:
        raise ZeroDivisionError("exact amount denominator is zero")
    if denominator < 0:
        numerator, denominator = -numerator, -denominator
    divisor = math.gcd(numerator, denominator)
    return ExactAmountV1(numerator // divisor, denominator // divisor)


def require_amount(value: Any, name: str) -> None:
    if type(value) is not ExactAmountV1:
        raise TypeError(f"{name} requires exact ExactAmountV1")
    ExactAmountV1.__post_init__(value)


def require_text(value: Any, name: str, *, maximum: int = MAX_TEXT) -> None:
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum
        or value.strip() != value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError(f"{name} requires bounded nonempty text")


def require_id(value: Any, name: str, *, kind: str | None = None) -> None:
    if (
        type(value) is not str
        or len(value) > 136
        or _IDENTIFIER.fullmatch(value) is None
        or (kind is not None and not value.startswith(kind + ":sha256:"))
    ):
        raise ValueError(f"{name} requires a supported exact content ID")


def require_clock(value: Any, name: str) -> None:
    if type(value) is not int or not 0 <= value <= MAX_CLOCK:
        raise ValueError(
            f"{name} requires a bounded nonnegative nanosecond int"
        )


def require_symbol(value: Any) -> None:
    if (
        type(value) is not str
        or re.fullmatch(r"[A-Z]{6}", value) is None
        or value[:3] == value[3:]
    ):
        raise ValueError("symbol requires distinct uppercase currency codes")


def require_keys(value: Any, expected: set[str]) -> None:
    if type(value) is not dict or len(value) != len(expected):
        raise ValueError("account record requires exact fields")
    if any(type(key) is not str for key in value) or set(value) != expected:
        raise ValueError("account record contains missing or unknown fields")


def tree_bound(value: Any) -> None:
    """Preflight escaped size and primitive types before JSON allocation."""
    pending = [(value, 0)]
    count = 0
    budget = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        budget += 4
        if count > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("account wire tree exceeds bounds")
        if type(item) is dict:
            if len(item) > 32 or any(type(key) is not str for key in item):
                raise ValueError("account mapping exceeds bounds")
            pending.extend((key, depth + 1) for key in item)
            pending.extend((child, depth + 1) for child in item.values())
        elif type(item) in (list, tuple):
            if len(item) > MAX_EVENTS:
                raise ValueError("account sequence exceeds bounds")
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item) > MAX_TEXT:
                raise ValueError("account text exceeds bounds")
            budget += sum(
                (
                    12
                    if ord(char) > 0xFFFF
                    else (
                        6
                        if ord(char) < 32 or ord(char) >= 127
                        else (2 if char in '\\"' else 1)
                    )
                )
                for char in item
            )
        elif type(item) is int:
            _integer(item, "wire integer")
            budget += 79
        elif item is None:
            budget += 4
        else:
            raise ValueError("account wire has unsupported primitive")
        if budget > MAX_WIRE_BYTES:
            raise ValueError("account escaped wire exceeds bound")


def canonical(value: Any) -> str:
    tree_bound(value)
    text = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(text) > MAX_WIRE_BYTES:
        raise ValueError("account wire exceeds bound")
    return text


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate account JSON key")
        result[key] = value
    return result


def _no_number(text: str) -> Any:
    raise ValueError("account JSON forbids float and nonfinite numbers")


def _parse_integer(text: str) -> int:
    if len(text) > 79:
        raise ValueError("account JSON integer exceeds bound")
    value = int(text)
    _integer(value, "wire integer")
    return value


def load_canonical(text: str) -> Any:
    if (
        type(text) is not str
        or len(text) > MAX_WIRE_BYTES
        or not text.isascii()
    ):
        raise ValueError("account JSON requires bounded exact ASCII text")
    try:
        value = json.loads(
            text,
            object_pairs_hook=_pairs,
            parse_float=_no_number,
            parse_constant=_no_number,
            parse_int=_parse_integer,
        )
    except (RecursionError, json.JSONDecodeError) as exc:
        raise ValueError("invalid account JSON") from exc
    if canonical(value) != text:
        raise ValueError("account JSON must be canonical")
    return value


R = TypeVar("R", bound="AccountRecord")


class AccountRecord:
    """Shared wire template; only the closed concrete account types admit."""

    __slots__ = ()
    SCHEMA: ClassVar[str]
    KIND: ClassVar[str]
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {}
    MARKERS: ClassVar[dict[str, str]] = {}

    def _validate(self) -> None:
        raise NotImplementedError

    def __post_init__(self) -> None:
        _raw_record_bound(self)
        type(self)._validate(self)

    @property
    def artifact_id(self) -> str:
        return cast(str, _record_wire(self)["artifact_id"])

    def to_dict(self) -> dict[str, Any]:
        return _record_wire(self)

    def to_json(self) -> str:
        return canonical(_record_wire(self))

    @classmethod
    def from_dict(cls: type[R], value: Any) -> R:
        _require_record_type(cls)
        if cls.KIND == "account-ledger":
            _allocation_preflight(value, wire=True)
        tree_bound(value)
        names = {field.name for field in fields(cast(Any, cls))}
        require_keys(
            value, names | {"schema_version", "artifact_id"} | set(cls.MARKERS)
        )
        if value["schema_version"] != cls.SCHEMA or any(
            value[name] != expected for name, expected in cls.MARKERS.items()
        ):
            raise ValueError("unsupported account schema or semantic marker")
        kwargs = {name: value[name] for name in names}
        for name, (nested, repeated) in cls.NESTED.items():
            item = kwargs[name]
            if repeated:
                if type(item) is not list or len(item) > MAX_EVENTS:
                    raise ValueError("nested account list exceeds bounds")
                kwargs[name] = tuple(nested.from_dict(child) for child in item)
            else:
                kwargs[name] = nested.from_dict(item)
        result = cls(**kwargs)
        if _record_wire(result) != value:
            raise ValueError("account record identity or wire mismatch")
        return result

    @classmethod
    def from_json(cls: type[R], text: str) -> R:
        return cls.from_dict(load_canonical(text))


def _require_record_type(cls: Any) -> None:
    from .account_contracts import ACCOUNT_RECORD_TYPES

    if cls not in ACCOUNT_RECORD_TYPES:
        raise TypeError("unsupported exact account record type")


def _record_wire(record: AccountRecord) -> dict[str, Any]:
    cls = type(record)
    _require_record_type(cls)
    _raw_record_bound(record)
    cls._validate(record)
    payload: dict[str, Any] = {"schema_version": cls.SCHEMA, **cls.MARKERS}
    for field in fields(cast(Any, record)):
        item = object.__getattribute__(record, field.name)
        nested = cls.NESTED.get(field.name)
        if nested is None:
            payload[field.name] = item
        elif nested[1]:
            if type(item) is not tuple or len(item) > MAX_EVENTS:
                raise ValueError("nested account tuple exceeds bounds")
            payload[field.name] = [
                _nested_wire(child, nested[0]) for child in item
            ]
        else:
            payload[field.name] = _nested_wire(item, nested[0])
    digest = hashlib.sha256(canonical(payload).encode("ascii")).hexdigest()
    return {**payload, "artifact_id": f"{cls.KIND}:sha256:{digest}"}


def _nested_wire(value: Any, expected: type[Any]) -> dict[str, Any]:
    if type(value) is not expected:
        raise TypeError("account nested object requires its exact type")
    if expected is ExactAmountV1:
        return ExactAmountV1.to_dict(value)
    return _record_wire(value)


def readmit(value: Any, expected: type[R]) -> R:
    """Detach a closed record without invoking caller-controlled serializers."""
    _require_record_type(expected)
    if type(value) is not expected:
        raise TypeError(f"expected exact {expected.__name__}")
    return expected.from_dict(_record_wire(value))


def _raw_record_bound(value: AccountRecord) -> None:
    """Bound raw fields before validators expand a nested record into JSON."""
    from .account_contracts import ACCOUNT_RECORD_TYPES

    if (
        type(value) in ACCOUNT_RECORD_TYPES
        and type(value).KIND == "account-ledger"
    ):
        _allocation_preflight(value, wire=False)
    pending: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    budget = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        budget += 4
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("account raw record tree exceeds bounds")
        if type(item) in ACCOUNT_RECORD_TYPES:
            cls = type(item)
            # Include schema, identity, marker keys/values and field names.
            budget += 200 + len(cls.SCHEMA) + len(cls.KIND)
            budget += sum(
                len(key) + len(text) + 8 for key, text in cls.MARKERS.items()
            )
            for field in fields(item):
                budget += len(field.name) + 4
                pending.append(
                    (object.__getattribute__(item, field.name), depth + 1)
                )
        elif type(item) is ExactAmountV1:
            require_amount(item, "raw amount")
            budget += 192
        elif type(item) is tuple:
            if len(item) > MAX_EVENTS:
                raise ValueError("account raw tuple exceeds bounds")
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item) > MAX_TEXT:
                raise ValueError("account raw string exceeds bounds")
            budget += sum(
                (
                    12
                    if ord(char) > 0xFFFF
                    else (
                        6
                        if ord(char) < 32 or ord(char) >= 127
                        else (2 if char in '\\"' else 1)
                    )
                )
                for char in item
            )
        elif type(item) is int:
            _integer(item, "raw integer")
            budget += 79
        elif item is None:
            budget += 4
        else:
            raise TypeError("unsupported raw account field type")
        if budget > MAX_WIRE_BYTES:
            raise ValueError("account raw escaped wire exceeds bound")


def _allocation_preflight(value: Any, *, wire: bool) -> None:
    """Bound aggregate allocation work using lengths, before row expansion."""
    from .account_contracts import AccountFillReceiptV1

    if wire:
        if type(value) is not dict:
            raise TypeError("account ledger requires an exact mapping")
        receipts = value.get("receipts")
    else:
        receipts = object.__getattribute__(value, "receipts")
    expected = list if wire else tuple
    if type(receipts) is not expected or len(receipts) > MAX_EVENTS:
        raise ValueError("account receipt sequence exceeds bounds")
    total = 0
    for receipt in receipts:
        if wire:
            if type(receipt) is not dict:
                raise TypeError("account receipt requires an exact mapping")
            allocations = receipt.get("allocations")
        else:
            if type(receipt) is not AccountFillReceiptV1:
                raise TypeError("account receipt requires exact record type")
            allocations = object.__getattribute__(receipt, "allocations")
        if (
            type(allocations) is not expected
            or len(cast(Any, allocations)) > MAX_EVENTS
        ):
            raise ValueError("account allocation sequence exceeds bounds")
        total += len(cast(Any, allocations))
        if total > MAX_ALLOCATIONS:
            raise ValueError("account aggregate allocation count exceeds bound")

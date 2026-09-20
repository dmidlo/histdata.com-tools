"""Exact, bounded JSON trees for reviewed semantic domains (not tolerance)."""

from __future__ import annotations

import hashlib
import json
import math
import struct
from typing import Any

MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_PROOF_BYTES = 32 * 1024 * 1024
MAX_NODES = 262_144
MAX_DEPTH = 64


def bounded_tree(value: object, *, maximum: int = MAX_PROOF_BYTES) -> None:
    pending: list[tuple[Any, int]] = [(value, 0)]
    count = 0
    size = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if count > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError("semantic tree expansion bound")
        if type(item) is dict:
            if len(item) > MAX_NODES:
                raise ValueError("semantic object bound")
            if count + len(pending) + 2 * len(item) > MAX_NODES:
                raise ValueError("semantic pending expansion bound")
            size += 2 + len(item)
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("semantic keys must be strings")
                pending.append((key, depth + 1))
                pending.append((child, depth + 1))
        elif type(item) in (list, tuple):
            if len(item) > MAX_NODES:
                raise ValueError("semantic collection bound")
            if count + len(pending) + len(item) > MAX_NODES:
                raise ValueError("semantic pending expansion bound")
            size += 2 + len(item)
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            size += 2
            for char in item:
                code = ord(char)
                size += (
                    2
                    if char in '\\"\b\f\n\r\t'
                    else (
                        6
                        if code < 32 or 127 <= code <= 65535
                        else 12 if code > 65535 else 1
                    )
                )
                if size > maximum:
                    raise ValueError("semantic escaped string byte bound")
        elif type(item) is int:
            if not -(2**64) < item < 2**64:
                raise ValueError("semantic integer outside reviewed domain")
            size += 21
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("nonfinite semantic number")
            size += 32
        elif item is None or type(item) is bool:
            size += 5
        else:
            raise ValueError("unsupported semantic value")
        if size > maximum:
            raise ValueError("semantic tree byte bound")


def canonical_json(value: object, *, maximum: int = MAX_PROOF_BYTES) -> str:
    bounded_tree(value, maximum=maximum)
    result = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(result) > maximum:
        raise ValueError("semantic encoded byte bound")
    return result


def load_json(value: str, *, maximum: int = MAX_INPUT_BYTES) -> Any:
    if type(value) is not str or len(value) > maximum:
        raise ValueError("semantic input byte bound")
    try:
        if len(value.encode("utf-8")) > maximum:
            raise ValueError("semantic input byte bound")
    except UnicodeError:
        raise ValueError("invalid semantic input encoding") from None

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate semantic JSON field")
            result[key] = item
        return result

    try:
        result = json.loads(value, object_pairs_hook=pairs)
    except (RecursionError, OverflowError, json.JSONDecodeError):
        raise ValueError("invalid semantic JSON") from None
    bounded_tree(result, maximum=maximum)
    return result


def exact_tree(value: object) -> object:
    """Injective tagged tree: no bool/int, signed-zero or object collisions."""
    bounded_tree(value)
    pending: list[tuple[Any, int]] = [(value, 0)]
    expanded = 0
    while pending:
        item, depth = pending.pop()
        if depth > MAX_DEPTH:
            raise ValueError("semantic tagged depth bound")
        if type(item) is dict:
            expanded += 3 + 2 * len(item)
            pending.extend((child, depth + 3) for child in item.values())
        elif type(item) in (tuple, list):
            expanded += 3
            pending.extend((child, depth + 2) for child in item)
        else:
            expanded += 2 if item is None else 3
        if expanded + len(pending) > MAX_NODES:
            raise ValueError("semantic tagged expansion bound")

    def visit(item: Any) -> object:
        if item is None:
            return ["null"]
        if type(item) is bool:
            return ["boolean", item]
        if type(item) is int:
            return ["integer", str(item)]
        if type(item) is float:
            return ["binary64", struct.pack("!d", item).hex()]
        if type(item) is str:
            return ["string", item]
        if type(item) is dict:
            return ["object", [[key, visit(item[key])] for key in sorted(item)]]
        return ["array", [visit(child) for child in item]]

    result = visit(value)
    bounded_tree(result)
    return result


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def exact_unit_conversion(value: int, numerator: int, denominator: int) -> int:
    """Optional exact integer-unit primitive; never rounds a timestamp."""
    if (
        any(type(item) is not int for item in (value, numerator, denominator))
        or not -(2**63) <= value < 2**63
        or not 0 < numerator < 2**63
        or not 0 < denominator < 2**63
    ):
        raise ValueError("invalid exact unit conversion")
    scaled, remainder = divmod(value * numerator, denominator)
    if remainder or not -(2**63) <= scaled < 2**63:
        raise ValueError("unit conversion is not exact and bounded")
    if scaled * denominator != value * numerator:
        raise ValueError("unit conversion is not invertible")
    return scaled

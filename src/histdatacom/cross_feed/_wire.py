"""Bounded exact immutable wire for conditional cross-feed diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
import json
import re
from typing import TypeVar, cast

from histdatacom.broker_plugin_provenance._wire import (
    Artifact as _Artifact,
    MAX_BYTES,
    MAX_DEPTH,
    MAX_ITEMS,
    MAX_NODES,
    Record,
    canonical_json,
    digest,
    plain,
    sha256,
    text,
)

_T = TypeVar("_T", bound="Artifact")


class Artifact(_Artifact):
    """Content identity is integrity, never proof of native computation."""

    __slots__ = ()

    @classmethod
    def schema_version(cls) -> str:
        return f"histdatacom.cross-feed-{cls.KIND}.v1"

    def to_dict(self) -> dict[str, object]:
        schema = self.schema_version()
        payload = plain(self)
        identity = sha256(
            canonical_json({"schema_version": schema, "payload": payload})
        )
        return cast(
            dict[str, object],
            plain(
                {
                    "schema_version": schema,
                    "artifact_id": f"cross-feed-{self.KIND}:sha256:{identity}",
                    "payload": payload,
                }
            ),
        )

    @classmethod
    def from_json(cls: type[_T], value: str) -> _T:  # noqa: PYI019
        if type(value) is not str or len(value) > MAX_BYTES:
            raise ValueError("cross-feed JSON byte bound")
        depth = 0
        nodes = 0
        container_items: list[int] = []
        atom = False
        quoted = escaped = False
        for char in value:
            if quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
                nodes += 1
                atom = False
            elif char in "[{":
                depth += 1
                nodes += 1
                container_items.append(1)
                atom = False
                if depth > MAX_DEPTH:
                    raise ValueError("cross-feed JSON depth bound")
            elif char in "]}":
                depth -= 1
                if container_items:
                    container_items.pop()
                atom = False
            elif char == ",":
                atom = False
                if container_items:
                    container_items[-1] += 1
                    if container_items[-1] > MAX_ITEMS:
                        raise ValueError("cross-feed JSON collection bound")
            elif char in ": \r\n\t":
                atom = False
            elif not atom:
                nodes += 1
                atom = True
            if nodes > MAX_NODES:
                raise ValueError("cross-feed JSON expanded node bound")

        def integer(token: str) -> int:
            if len(token) > 20:
                raise ValueError("cross-feed JSON integer bound")
            return int(token)

        def no_float(token: str) -> object:
            raise ValueError(
                "cross-feed numbers must be exact integers/rationals"
            )

        def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
            output: dict[str, object] = {}
            for key, child in items:
                if key in output:
                    raise ValueError("duplicate cross-feed JSON key")
                output[key] = child
            return output

        decoded = json.loads(
            value,
            parse_int=integer,
            parse_float=no_float,
            parse_constant=no_float,
            object_pairs_hook=pairs,
        )
        if type(decoded) is not dict or canonical_json(decoded) != value:
            raise ValueError("noncanonical cross-feed artifact")
        return cls.from_dict(decoded)


@dataclass(frozen=True, slots=True)
class RationalV1(Record):
    """Reduced canonical rational; strings avoid int64 timestamp products."""

    numerator: str
    denominator: str

    def _validate(self) -> None:
        if not re.fullmatch(
            r"0|-?[1-9][0-9]{0,255}", self.numerator
        ) or not re.fullmatch(r"[1-9][0-9]{0,255}", self.denominator):
            raise ValueError("invalid bounded exact rational")
        value = self.value
        if (str(value.numerator), str(value.denominator)) != (
            self.numerator,
            self.denominator,
        ):
            raise ValueError("rational must be reduced")

    @property
    def value(self) -> Fraction:
        return Fraction(int(self.numerator), int(self.denominator))

    @classmethod
    def from_fraction(cls, value: Fraction | int) -> RationalV1:
        if type(value) not in (Fraction, int):
            raise ValueError("exact rational or integer required")
        number = Fraction(value)
        return cls(str(number.numerator), str(number.denominator))


__all__ = [
    "Artifact",
    "Record",
    "RationalV1",
    "canonical_json",
    "digest",
    "sha256",
    "text",
]

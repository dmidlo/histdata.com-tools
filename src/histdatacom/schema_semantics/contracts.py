"""Immutable semantic evidence, distinct from compatibility declarations."""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from typing import Any, ClassVar, TypeVar, get_args, get_origin, get_type_hints

from .canonical import (
    MAX_DEPTH,
    MAX_NODES,
    MAX_PROOF_BYTES,
    canonical_json,
    load_json,
    sha256,
)

_T = TypeVar("_T", bound="SemanticContract")


def digest(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("invalid semantic digest")


def label(value: str) -> None:
    if (
        type(value) is not str
        or not value
        or len(value) > 512
        or not value.isascii()
        or value.strip() != value
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError("invalid semantic label")


def _decode(hint: Any, value: Any) -> Any:
    if get_origin(hint) is tuple:
        if type(value) not in (tuple, list) or len(value) > 512:
            raise ValueError("semantic contract collection bound")
        return tuple(_decode(get_args(hint)[0], item) for item in value)
    if isinstance(hint, type) and issubclass(hint, SemanticContract):
        return value if type(value) is hint else hint.from_dict(value)
    if type(value) is not hint:
        raise ValueError("invalid semantic contract scalar type")
    return value


def _wire(value: Any) -> Any:
    if isinstance(value, SemanticContract):
        return value.to_dict()
    if type(value) is tuple:
        return [_wire(item) for item in value]
    return value


class SemanticContract:
    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        for item in fields(self):  # type: ignore[arg-type]
            object.__setattr__(
                self,
                item.name,
                _decode(hints[item.name], getattr(self, item.name)),
            )
        # Reserve expanded nested artifacts before _wire copies shared values.
        pending: list[tuple[Any, int]] = [(self, 0)]
        nodes = 0
        minimum_bytes = 0
        while pending:
            value, depth = pending.pop()
            nodes += 1
            if depth > MAX_DEPTH or nodes + len(pending) > MAX_NODES:
                raise ValueError("semantic contract expansion bound")
            if isinstance(value, SemanticContract):
                members = fields(value)  # type: ignore[arg-type]
                minimum_bytes += 100 + sum(len(f.name) + 4 for f in members)
                pending.extend(
                    (getattr(value, f.name), depth + 1) for f in members
                )
            elif type(value) is tuple:
                minimum_bytes += len(value) + 2
                pending.extend((v, depth + 1) for v in value)
            elif type(value) is str:
                minimum_bytes += len(value) + 2
            if minimum_bytes > MAX_PROOF_BYTES:
                raise ValueError("semantic contract aggregate byte bound")
        self._validate()
        self.to_json()

    def _validate(self) -> None:
        raise NotImplementedError

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": f"histdatacom.schema-semantics.{self.KIND}.v1",
            **{item.name: _wire(getattr(self, item.name)) for item in fields(self)},  # type: ignore[arg-type]
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def artifact_id(self) -> str:
        return f"schema-semantics-{self.KIND}:sha256:{sha256(self.to_json())}"

    @classmethod
    def from_dict(cls: type[_T], value: Any) -> _T:  # noqa: PYI019
        canonical_json(value)
        names = {item.name for item in fields(cls)}  # type: ignore[arg-type]
        if (
            type(value) is not dict
            or set(value) != names | {"schema_version"}
            or value["schema_version"]
            != f"histdatacom.schema-semantics.{cls.KIND}.v1"
        ):
            raise ValueError("unknown semantic contract fields/schema")
        return cls(**{name: value[name] for name in names})

    @classmethod
    def from_json(cls: type[_T], value: str) -> _T:  # noqa: PYI019
        result = cls.from_dict(load_json(value, maximum=MAX_PROOF_BYTES))
        if result.to_json() != value:
            raise ValueError("noncanonical semantic evidence")
        return result


@dataclass(frozen=True, slots=True)
class SemanticProjectionV1(SemanticContract):
    profile_id: str
    reader_sha256: str
    projector_sha256: str
    input_sha256: str
    semantic_json: str
    KIND: ClassVar[str] = "projection"

    def _validate(self) -> None:
        label(self.profile_id)
        for value in (
            self.reader_sha256,
            self.projector_sha256,
            self.input_sha256,
        ):
            digest(value)
        if (
            canonical_json(
                load_json(self.semantic_json, maximum=MAX_PROOF_BYTES)
            )
            != self.semantic_json
        ):
            raise ValueError("noncanonical semantic projection")

    @property
    def semantic_sha256(self) -> str:
        return sha256(self.semantic_json)


@dataclass(frozen=True, slots=True)
class SemanticMigrationProofV1(SemanticContract):
    edge_subject_id: str
    implementation_id: str
    implementation_sha256: str
    source_profile_id: str
    destination_profile_id: str
    source_json: str
    destination_json: str
    source_projection: SemanticProjectionV1
    destination_projection: SemanticProjectionV1
    KIND: ClassVar[str] = "migration-proof"

    def _validate(self) -> None:
        for value in (
            self.edge_subject_id,
            self.implementation_id,
            self.source_profile_id,
            self.destination_profile_id,
        ):
            label(value)
        digest(self.implementation_sha256)
        load_json(self.source_json)
        load_json(self.destination_json)
        if (
            self.source_projection.profile_id != self.source_profile_id
            or self.destination_projection.profile_id
            != self.destination_profile_id
        ):
            raise ValueError("semantic proof profile mismatch")
        if self.source_projection.input_sha256 != sha256(
            self.source_json
        ) or self.destination_projection.input_sha256 != sha256(
            self.destination_json
        ):
            raise ValueError("semantic proof input hash mismatch")
        if (
            self.source_projection.semantic_json
            != self.destination_projection.semantic_json
        ):
            raise ValueError("migration changes exact semantic meaning")


@dataclass(frozen=True, slots=True)
class SemanticCompositionProofV1(SemanticContract):
    steps: tuple[SemanticMigrationProofV1, ...]
    KIND: ClassVar[str] = "composition-proof"

    def _validate(self) -> None:
        if not 2 <= len(self.steps) <= 16:
            raise ValueError("semantic composition length")
        for left, right in zip(self.steps, self.steps[1:]):
            if (
                left.destination_json != right.source_json
                or left.destination_profile_id != right.source_profile_id
            ):
                raise ValueError("semantic composition is not byte-linked")


@dataclass(frozen=True, slots=True)
class SemanticRegistryValidationV1(SemanticContract):
    registry_id: str
    lossless_subject_ids: tuple[str, ...]
    proof_ids: tuple[str, ...]
    KIND: ClassVar[str] = "registry-validation"

    def _validate(self) -> None:
        label(self.registry_id)
        for values in (self.lossless_subject_ids, self.proof_ids):
            if tuple(sorted(set(values))) != values:
                raise ValueError(
                    "semantic validation IDs must be unique/sorted"
                )
            for value in values:
                label(value)

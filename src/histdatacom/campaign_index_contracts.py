"""Bounded campaign inspection receipts, never a substitute for fresh replay.

The historical campaign index V1 remains unchanged. These additive records
distinguish structural inspection from actual byte/native validation. Loading
or constructing a receipt grants no publication or certification permission.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, fields
from typing import Any, ClassVar, cast

from typing_extensions import Self

from histdatacom.runtime_contracts import ArtifactRef

MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_RECEIPT_NODES = 262144
MAX_RECEIPT_DEPTH = 24
MAX_OUT_OF_PLAN_PRODUCTS = 4096
VERIFIER_VERSION = "1.0.0"
VERIFICATION_NONCLAIM = (
    "Final native integrity/validation only, not scientific promotion or "
    "producer/carving replay. Discarded pre-cross candidate quotes are not "
    "recovered; parity-preserving alternative generated price paths are not "
    "distinguished without producer replay."
)
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _text(value: Any, name: str, maximum: int = 4096) -> None:
    if type(value) is not str or not value or len(value) > maximum:
        raise ValueError(f"{name} requires bounded nonempty text")


def _sha(value: Any, name: str) -> None:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise ValueError(f"{name} requires SHA-256")


def _integer(value: Any, name: str) -> None:
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{name} requires a nonnegative int64")


def _bound(value: Any) -> None:
    stack = [(value, 0)]
    nodes = charge = 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if depth > MAX_RECEIPT_DEPTH or nodes > MAX_RECEIPT_NODES:
            raise ValueError("campaign JSON structural bound exceeded")
        if item is None or type(item) is bool:
            charge += 5
        elif type(item) is int:
            if abs(item) > 2**63 - 1:
                raise ValueError("campaign integer exceeds int64")
            charge += 21
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("campaign JSON contains nonfinite float")
            charge += 32
        elif type(item) is str:
            if len(item) > MAX_RECEIPT_BYTES:
                raise ValueError("campaign JSON string exceeds bound")
            charge += 2 + sum(
                (
                    12
                    if ord(char) > 65535
                    else (
                        6
                        if ord(char) >= 127 or ord(char) < 32
                        else 2 if char in '\\"' else 1
                    )
                )
                for char in item
            )
        elif type(item) is list:
            if len(item) > MAX_RECEIPT_NODES:
                raise ValueError("campaign JSON list exceeds bound")
            charge += 2 + len(item)
            stack.extend((nested, depth + 1) for nested in item)
        elif type(item) is dict:
            if len(item) > MAX_RECEIPT_NODES or any(
                type(key) is not str for key in item
            ):
                raise ValueError("campaign JSON object exceeds bound")
            charge += 2 + 2 * len(item)
            for key, nested in item.items():
                stack.append((key, depth + 1))
                stack.append((nested, depth + 1))
        else:
            raise TypeError("campaign JSON requires exact primitive values")
        if charge > MAX_RECEIPT_BYTES:
            raise ValueError("campaign JSON escaped byte bound exceeded")


def canonical(value: Any) -> str:
    """Encode a bounded primitive tree without invoking custom serializers."""
    _bound(value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate campaign JSON key")
        result[key] = value
    return result


def _parse_integer(text: str) -> int:
    if len(text) > 20:
        raise ValueError("campaign integer exceeds bounds")
    value = int(text)
    if abs(value) > 2**63 - 1:
        raise ValueError("campaign integer exceeds bounds")
    return value


def _constant(_text: str) -> None:
    raise ValueError("nonfinite campaign JSON value")


def load_json(text: str) -> Any:
    """Bound depth/nodes before allocating the decoded receipt tree."""
    if (
        type(text) is not str
        or len(text) > MAX_RECEIPT_BYTES
        or not text.isascii()
    ):
        raise ValueError("campaign receipt requires bounded ASCII JSON")
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
        if not 0 <= depth <= MAX_RECEIPT_DEPTH or nodes > MAX_RECEIPT_NODES:
            raise ValueError("campaign JSON structural bound exceeded")
    result = json.loads(
        text,
        object_pairs_hook=_pairs,
        parse_int=_parse_integer,
        parse_constant=_constant,
    )
    _bound(result)
    return result


@dataclass(frozen=True, slots=True)
class CampaignArtifactRefV1:
    """An immutable exact artifact reference; metadata is canonical JSON."""

    kind: str
    path: str
    size_bytes: int
    sha256: str
    metadata_json: str = "{}"

    def __post_init__(self) -> None:
        _text(self.kind, "artifact kind", 256)
        _text(self.path, "artifact path")
        _integer(self.size_bytes, "artifact size")
        _sha(self.sha256, "artifact hash")
        metadata = load_json(self.metadata_json)
        if (
            type(metadata) is not dict
            or canonical(metadata) != self.metadata_json
        ):
            raise ValueError("artifact metadata requires canonical object")

    def to_dict(self) -> dict[str, Any]:
        CampaignArtifactRefV1.__post_init__(self)
        return {field.name: getattr(self, field.name) for field in fields(self)}

    @classmethod
    def from_dict(cls, value: Any) -> CampaignArtifactRefV1:
        if type(value) is not dict or set(value) != {
            "kind",
            "path",
            "size_bytes",
            "sha256",
            "metadata_json",
        }:
            raise ValueError("campaign artifact fields differ")
        return cls(**value)

    @classmethod
    def from_artifact_ref(cls, ref: ArtifactRef) -> CampaignArtifactRefV1:
        if type(ref) is not ArtifactRef:
            raise TypeError("exact ArtifactRef required")
        if type(ref.size_bytes) is not int:
            raise ValueError("campaign artifact requires an exact byte size")
        return cls(
            ref.kind,
            ref.path,
            ref.size_bytes,
            ref.sha256,
            canonical(ref.metadata),
        )

    def to_artifact_ref(self) -> ArtifactRef:
        CampaignArtifactRefV1.__post_init__(self)
        return ArtifactRef(
            self.kind,
            self.path,
            self.size_bytes,
            self.sha256,
            load_json(self.metadata_json),
        )


def _ref(value: Any) -> None:
    if type(value) is not CampaignArtifactRefV1:
        raise TypeError("exact immutable campaign artifact reference required")
    CampaignArtifactRefV1.__post_init__(value)


def _raw_record_bound(value: Any) -> None:
    """Reserve all retained string/ref bytes before nested parsing/expansion."""
    if type(value) not in _RECORDS:
        raise TypeError("exact campaign receipt class required")
    strings: list[str] = []
    for item in fields(value):
        raw = getattr(value, item.name)
        refs: tuple[Any, ...] = ()
        if item.name in {"index_ref", "product_ref"}:
            refs = (raw,)
        elif item.name == "out_of_plan_products":
            if type(raw) is not tuple or len(raw) > MAX_OUT_OF_PLAN_PRODUCTS:
                raise ValueError("out-of-plan reference bound exceeded")
            refs = raw
        elif type(raw) is str:
            strings.append(raw)
        for ref in refs:
            if type(ref) is not CampaignArtifactRefV1:
                raise TypeError(
                    "exact immutable campaign artifact reference required"
                )
            strings.extend((ref.kind, ref.path, ref.sha256, ref.metadata_json))
    # metadata_json remains a JSON string in this wire, so charging its escaped
    # form is intentional. The final complete tree gets its own exact preflight.
    _bound(strings)


class _Record:
    SCHEMA: ClassVar[str]
    KIND: ClassVar[str]
    LEVEL: ClassVar[str]

    @property
    def schema_version(self) -> str:
        return self.SCHEMA

    @property
    def verification_level(self) -> str:
        return self.LEVEL

    @property
    def verification_id(self) -> str:
        return (
            self.KIND
            + ":sha256:"
            + hashlib.sha256(
                canonical(self._payload()).encode("ascii")
            ).hexdigest()
        )

    def _payload(self) -> dict[str, Any]:
        if type(self) not in _RECORDS:
            raise TypeError("exact campaign receipt class required")
        _raw_record_bound(self)
        self.__post_init__()
        result: dict[str, Any] = {
            "schema_version": self.SCHEMA,
            "verification_level": self.LEVEL,
            "verifier_version": VERIFIER_VERSION,
            "authority": "historical_record_only_fresh_verification_required",
            "nonclaim": VERIFICATION_NONCLAIM,
        }
        for field in fields(cast(Any, self)):
            value = getattr(self, field.name)
            if type(value) is CampaignArtifactRefV1:
                value = value.to_dict()
            elif type(value) is tuple:
                value = [item.to_dict() for item in value]
            result[field.name] = value
        _bound(result)
        return result

    def __post_init__(self) -> None:
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload(), "verification_id": self.verification_id}

    def to_json(self) -> str:
        return canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if cls not in _RECORDS:
            raise TypeError("exact campaign receipt class required")
        _bound(value)
        names = {field.name for field in fields(cast(Any, cls))}
        fixed = {
            "schema_version",
            "verification_level",
            "verifier_version",
            "authority",
            "nonclaim",
            "verification_id",
        }
        if type(value) is not dict or set(value) != names | fixed:
            raise ValueError("campaign receipt fields differ")
        kwargs = {name: value[name] for name in names}
        for name in ("index_ref", "product_ref"):
            if name in kwargs:
                kwargs[name] = CampaignArtifactRefV1.from_dict(kwargs[name])
        if "out_of_plan_products" in kwargs:
            items = kwargs["out_of_plan_products"]
            if type(items) is not list or len(items) > MAX_OUT_OF_PLAN_PRODUCTS:
                raise ValueError("out-of-plan reference bound exceeded")
            kwargs["out_of_plan_products"] = tuple(
                CampaignArtifactRefV1.from_dict(item) for item in items
            )
        result = cast(Any, cls)(**kwargs)
        if canonical(result.to_dict()) != canonical(value):
            raise ValueError("campaign receipt derived fields differ")
        return cast(Self, result)

    @classmethod
    def from_json(cls, text: str) -> Self:
        result = cls.from_dict(load_json(text))
        if result.to_json() != text:
            raise ValueError("campaign receipt JSON is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class CampaignStructuralVerificationV1(_Record):
    """Actual index/shard inspection, explicitly no product verification."""

    index_ref: CampaignArtifactRefV1
    index_id: str
    plan_set_id: str
    support_artifact_id: str
    status: str
    shard_count: int
    support_window_count: int
    product_count: int
    missing_product_count: int
    empty_window_count: int
    refused_window_count: int
    observed_event_count: int
    synthetic_event_count: int
    shard_rows_sha256: str
    verified_inputs_sha256: str

    SCHEMA: ClassVar[str] = "histdatacom.campaign-structural-verification.v1"
    KIND: ClassVar[str] = "campaign-structural-verification"
    LEVEL: ClassVar[str] = "structural_only_products_unverified"

    def __post_init__(self) -> None:
        _common(self)


@dataclass(frozen=True, slots=True)
class CampaignDeepVerificationV1(_Record):
    """Fresh native verification record, not a reusable authorization token."""

    index_ref: CampaignArtifactRefV1
    index_id: str
    plan_set_id: str
    support_artifact_id: str
    status: str
    shard_count: int
    support_window_count: int
    product_count: int
    missing_product_count: int
    empty_window_count: int
    refused_window_count: int
    observed_event_count: int
    synthetic_event_count: int
    shard_rows_sha256: str
    verified_inputs_sha256: str
    product_verifications_sha256: str
    out_of_plan_products: tuple[CampaignArtifactRefV1, ...] = ()

    SCHEMA: ClassVar[str] = "histdatacom.campaign-deep-verification.v1"
    KIND: ClassVar[str] = "campaign-deep-verification"
    LEVEL: ClassVar[str] = "fresh_deep_native_validation"

    def __post_init__(self) -> None:
        _common(self)
        _sha(self.product_verifications_sha256, "product verification hash")
        if (
            type(self.out_of_plan_products) is not tuple
            or len(self.out_of_plan_products) > MAX_OUT_OF_PLAN_PRODUCTS
        ):
            raise ValueError("out-of-plan reference bound exceeded")
        for ref in self.out_of_plan_products:
            _ref(ref)
        keys = [(ref.path, ref.sha256) for ref in self.out_of_plan_products]
        if keys != sorted(set(keys)):
            raise ValueError("out-of-plan references must be unique and sorted")


def _common(value: Any) -> None:
    _raw_record_bound(value)
    _ref(value.index_ref)
    if value.index_ref.kind != "reconstruction_campaign_product_index_v1":
        raise ValueError("campaign receipt index kind differs")
    for name in ("index_id", "plan_set_id", "support_artifact_id"):
        _text(getattr(value, name), name, 256)
    for name in (
        "shard_count",
        "support_window_count",
        "product_count",
        "missing_product_count",
        "empty_window_count",
        "refused_window_count",
        "observed_event_count",
        "synthetic_event_count",
    ):
        _integer(getattr(value, name), name)
    if not 1 <= value.shard_count <= 4096:
        raise ValueError("campaign shard count exceeds bound")
    expected = "incomplete" if value.missing_product_count else "complete"
    if value.status != expected:
        raise ValueError("campaign receipt completion status differs")
    _sha(value.shard_rows_sha256, "shard row hash")
    _sha(value.verified_inputs_sha256, "verified input hash")


@dataclass(frozen=True, slots=True)
class CampaignProductVerificationV1(_Record):
    """One bounded native product validation, hashed into campaign receipts."""

    plan_id: str
    shard_id: str
    support_id: str
    run_id: str
    window_id: str
    ensemble_member_id: str
    product_ref: CampaignArtifactRefV1
    manifest_id: str
    configuration_id: str
    proposal_engine_id: str
    generator_config_id: str
    runtime_scope_json: str
    logical_content_sha256: str
    observed_content_sha256: str
    observed_event_count: int
    synthetic_event_count: int
    local_validation_sha256: str
    final_validation_json: str
    verified_inputs_sha256: str

    SCHEMA: ClassVar[str] = "histdatacom.campaign-product-verification.v1"
    KIND: ClassVar[str] = "campaign-product-verification"
    LEVEL: ClassVar[str] = "fresh_native_product_validation"

    def __post_init__(self) -> None:
        _raw_record_bound(self)
        _ref(self.product_ref)
        for name in (
            "plan_id",
            "shard_id",
            "support_id",
            "run_id",
            "window_id",
            "ensemble_member_id",
            "manifest_id",
            "configuration_id",
            "proposal_engine_id",
            "generator_config_id",
        ):
            _text(getattr(self, name), name, 256)
        for name in (
            "logical_content_sha256",
            "observed_content_sha256",
            "local_validation_sha256",
            "verified_inputs_sha256",
        ):
            _sha(getattr(self, name), name)
        _integer(self.observed_event_count, "observed count")
        _integer(self.synthetic_event_count, "synthetic count")
        for text in (self.runtime_scope_json, self.final_validation_json):
            parsed = load_json(text)
            if type(parsed) is not dict or canonical(parsed) != text:
                raise ValueError("product evidence requires canonical objects")


_RECORDS = (
    CampaignStructuralVerificationV1,
    CampaignDeepVerificationV1,
    CampaignProductVerificationV1,
)


__all__ = [
    "CampaignArtifactRefV1",
    "CampaignDeepVerificationV1",
    "CampaignProductVerificationV1",
    "CampaignStructuralVerificationV1",
]

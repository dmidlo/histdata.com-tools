"""Origin-preserving training contracts; hashes are not source verification.

Constructors check bounded, immutable, canonical structure. Only the replaying
materializers/readers verify external evidence. No historical-availability or
scientific qualification follows from constructing one of these objects.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, fields
from enum import Enum
from types import UnionType
from typing import Any, ClassVar, TypeVar, Union, cast, get_args, get_origin
from typing import get_type_hints

MAX_TRAINING_BYTES = 8 * 1024 * 1024
MAX_TRAINING_ITEMS = 4096
MAX_TRAINING_SOURCE_ROWS = 2_000_000
MAX_TRAINING_SOURCE_BYTES = 512 * 1024 * 1024
DAY_NS = 86_400_000_000_000
OWNERSHIP_POLICY = "utc-day-complete-graph-dependency-closure.v1"
NO_LABEL_SCHEMA = "histdatacom.training-label.none.v1"
_INT64 = 2**63 - 1
_T = TypeVar("_T", bound="TrainingContract")


class TrainingOrigin(str, Enum):
    """Reserved origin vocabulary; not every origin has a verified emitter."""

    OBSERVED = "observed"
    SYNTHETIC_RECONSTRUCTION = "synthetic_reconstruction"
    OFFICIAL_CONTEXT = "official_context"
    MACHINE_FORECAST = "machine_forecast"
    SYNTHETIC_CUSTOMER_FLOW = "synthetic_customer_flow"
    BROKER_CONDITIONED_COUNTERFACTUAL = "broker_conditioned_counterfactual"


class TrainingInformationMode(str, Enum):
    EX_ANTE = "ex_ante"
    EX_POST = "ex_post"
    MIXED_FORBIDDEN = "mixed_forbidden"


class TrainingConsumerMode(str, Enum):
    CAUSAL = "causal_forecasting_trading_research"
    RECONSTRUCTION = "reconstruction_sensitivity_research"
    AUGMENTATION = "representation_learning_synthetic_augmentation"
    DESCRIPTIVE = "descriptive_ex_post_analysis"


class TrainingVerificationLevel(str, Enum):
    SOURCE_BYTES = "source_bytes_and_normalized_values"
    PRODUCT_REPLAY = "committed_product_and_observed_anchor_replay"
    DERIVED_REPLAY = "derived_artifact_self_consistency_not_official_source"


NONCLAIMS = (
    "not_historical_availability_verification",
    "not_independent_synthetic_evidence",
    "not_split_weight_or_label_certification",
    "not_empirical_or_complete_corpus_qualification",
    "normalized_context_is_not_official_source_authenticity",
)


def _string_cost(value: str) -> int:
    return 2 + sum(
        (
            6
            if ord(c) < 32
            else (
                2
                if c in '\\"'
                else 1 if ord(c) < 128 else 6 if ord(c) <= 65535 else 12
            )
        )
        for c in value
    )


def _check(
    value: object, depth: int = 0, budget: list[int] | None = None
) -> None:
    budget = [MAX_TRAINING_BYTES, 200_000] if budget is None else budget
    budget[0] -= _string_cost(value) if type(value) is str else 2
    budget[1] -= 1
    if min(budget) < 0:
        raise ValueError("training JSON exceeds expanded traversal budget")
    if depth > 16:
        raise ValueError("training JSON exceeds nesting bound")
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if not -_INT64 <= value <= _INT64:
            raise ValueError("training integer is outside int64")
    elif type(value) is float:
        if not math.isfinite(value):
            raise ValueError("training numbers must be finite")
    elif type(value) is str:
        if len(value) > MAX_TRAINING_BYTES:
            raise ValueError("training string exceeds bound")
    elif isinstance(value, (dict, list)):
        if len(value) > MAX_TRAINING_ITEMS:
            raise ValueError("training collection exceeds bound")
        for key, item in (
            value.items() if isinstance(value, dict) else enumerate(value)
        ):
            if isinstance(value, dict) and type(key) is not str:
                raise ValueError("training object keys must be strings")
            if isinstance(value, dict):
                _check(key, depth + 1, budget)
            _check(item, depth + 1, budget)
    else:
        raise ValueError("unsupported training JSON value")


def training_json(value: object) -> str:
    """Canonical finite JSON with aggregate byte/depth/cardinality limits."""
    _check(value)
    result = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    if len(result) > MAX_TRAINING_BYTES:
        raise ValueError("training envelope exceeds aggregate byte bound")
    return result


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate training JSON member")
        result[key] = value
    return result


def training_load(text: str) -> dict[str, object]:
    if type(text) is not str or len(text) > MAX_TRAINING_BYTES:
        raise ValueError("training JSON exceeds byte bound")
    try:
        value: object = json.loads(text, object_pairs_hook=_pairs)
    except (ValueError, RecursionError) as exc:
        raise ValueError("invalid training JSON") from exc
    _check(value)
    if not isinstance(value, dict):
        raise ValueError("training envelope must be an object")
    training_json(value)
    return value


def _text(value: str) -> None:
    if (
        type(value) is not str
        or not value
        or len(value) > 1024
        or value != value.strip()
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError("expected bounded nonempty training text")


def _clock(value: int) -> None:
    if type(value) is not int or not 0 <= value <= _INT64:
        raise ValueError("expected nonnegative int64 training clock")


def _digest(value: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("invalid training SHA-256")


def _wire(
    value: object, depth: int = 0, budget: list[int] | None = None
) -> object:
    budget = [MAX_TRAINING_BYTES] if budget is None else budget
    if depth > 16:
        raise ValueError("training field nesting exceeds bound")
    if isinstance(value, TrainingContract):
        budget[0] -= len(value.to_json())
        if budget[0] < 0:
            raise ValueError("training child expansion exceeds byte budget")
        return value.to_dict()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        if len(value) > MAX_TRAINING_ITEMS:
            raise ValueError("training field cardinality exceeds bound")
        return [_wire(v, depth + 1, budget) for v in value]
    budget[0] -= _string_cost(value) if type(value) is str else 24
    if budget[0] < 0:
        raise ValueError("training field expansion exceeds byte budget")
    return value


def _decode(annotation: object, value: object) -> object:
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        for item in get_args(annotation):
            try:
                return _decode(item, value)
            except (TypeError, ValueError):
                pass
        raise ValueError("training optional field has incorrect type")
    if annotation is type(None):
        if value is not None:
            raise ValueError("expected null")
        return None
    if origin is tuple:
        if type(value) is not list or len(value) > MAX_TRAINING_ITEMS:
            raise ValueError("expected bounded training array")
        return tuple(_decode(get_args(annotation)[0], v) for v in value)
    if annotation in (str, int, float, bool):
        if type(value) is not annotation:
            raise ValueError("training scalars require exact types")
        return value
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if type(value) is not str:
            raise ValueError("training enum requires string")
        return annotation(value)
    if isinstance(annotation, type) and issubclass(
        annotation, TrainingContract
    ):
        if type(value) is not dict:
            raise ValueError("training child requires object")
        return annotation.from_dict(value)
    raise ValueError("unsupported training field type")


class TrainingContract:
    """Strict versioned wire envelope, with no writable identity fields."""

    __slots__ = ("_cached_id", "_cached_json")
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        for field in fields(cast(Any, self)):
            value = _wire(getattr(self, field.name))
            object.__setattr__(
                self, field.name, _decode(hints[field.name], value)
            )
        # Check aggregate size before nested source parsing or semantic work.
        self.to_json()
        self._validate()

    def _validate(self) -> None:
        raise NotImplementedError

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": f"histdatacom.training-{self.KIND}.v1",
            **{
                f.name: _wire(getattr(self, f.name))
                for f in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        cached = getattr(self, "_cached_id", None)
        if cached is not None:
            return cast(str, cached)
        digest = hashlib.sha256(
            training_json(self.payload()).encode()
        ).hexdigest()
        identity = f"training-{self.KIND}:sha256:{digest}"
        object.__setattr__(self, "_cached_id", identity)
        return identity

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        cached = getattr(self, "_cached_json", None)
        if cached is None:
            cached = training_json(self.to_dict())
            object.__setattr__(self, "_cached_json", cached)
        return cast(str, cached)

    @classmethod
    def from_dict(cls: type[_T], value: dict[str, object]) -> _T:
        training_json(value)
        names = {f.name for f in fields(cast(Any, cls))}
        if set(value) != names | {"schema_version", "artifact_id"}:
            raise ValueError("unknown or missing training envelope field")
        hints = get_type_hints(cls)
        result = cls(
            **{name: _decode(hints[name], value[name]) for name in names}
        )
        if training_json(value) != result.to_json():
            raise ValueError(
                "training schema, canonical payload or identity differs"
            )
        return result

    @classmethod
    def from_json(cls: type[_T], text: str) -> _T:
        return cls.from_dict(training_load(text))


@dataclass(frozen=True, slots=True)
class TrainingRootV1(TrainingContract):
    KIND: ClassVar[str] = "root"
    kind: str
    upstream_id: str
    content_sha256: str
    verification: TrainingVerificationLevel

    def _validate(self) -> None:
        _text(self.kind)
        _text(self.upstream_id)
        _digest(self.content_sha256)


@dataclass(frozen=True, slots=True)
class TrainingSourceV1(TrainingContract):
    """Replay locators; constructing this object does not verify them."""

    KIND: ClassVar[str] = "source"
    catalog_json: str
    dataset_version_id: str
    product_manifest_paths: tuple[str, ...] = ()
    context_artifact_paths: tuple[str, ...] = ()
    derived_artifact_paths: tuple[str, ...] = ()

    def _validate(self) -> None:
        _text(self.dataset_version_id)
        training_load(self.catalog_json)
        for paths in (
            self.product_manifest_paths,
            self.context_artifact_paths,
            self.derived_artifact_paths,
        ):
            if paths != tuple(sorted(set(paths))) or len(paths) > 32:
                raise ValueError(
                    "source paths must be sorted, unique and bounded"
                )
            for path in paths:
                _text(path)
        if set(self.context_artifact_paths) & set(self.derived_artifact_paths):
            raise ValueError("context and derived roles cannot overlap")


@dataclass(frozen=True, slots=True)
class TrainingEvidenceUnitV1(TrainingContract):
    KIND: ClassVar[str] = "evidence-unit"
    dataset_version_id: str
    start_ns: int
    end_ns: int
    graph_symbols: tuple[str, ...]
    anchor_content_sha256: str
    context_root_ids: tuple[str, ...] = ()
    ownership_policy: str = OWNERSHIP_POLICY

    def _validate(self) -> None:
        _text(self.dataset_version_id)
        _clock(self.start_ns)
        _clock(self.end_ns)
        if (
            self.end_ns <= self.start_ns
            or self.ownership_policy != OWNERSHIP_POLICY
        ):
            raise ValueError("invalid evidence ownership interval or policy")
        if not self.graph_symbols or self.graph_symbols != tuple(
            sorted(set(self.graph_symbols))
        ):
            raise ValueError(
                "evidence graph must be complete canonical symbols"
            )
        if any(
            re.fullmatch(r"[A-Z]{6}", s) is None for s in self.graph_symbols
        ):
            raise ValueError("invalid graph symbol")
        _digest(self.anchor_content_sha256)
        if self.context_root_ids != tuple(sorted(set(self.context_root_ids))):
            raise ValueError("context identities must be canonical")
        for value in self.context_root_ids:
            _text(value)


@dataclass(frozen=True, slots=True)
class TrainingOwnershipV1(TrainingContract):
    KIND: ClassVar[str] = "ownership"
    dataset_version_id: str
    units: tuple[TrainingEvidenceUnitV1, ...]
    dependency_roots: tuple[TrainingRootV1, ...]

    def _validate(self) -> None:
        _text(self.dataset_version_id)
        if not self.units or len(self.units) > MAX_TRAINING_ITEMS:
            raise ValueError("ownership requires bounded nonempty intervals")
        for index, unit in enumerate(self.units):
            if unit.dataset_version_id != self.dataset_version_id:
                raise ValueError("ownership source differs")
            if index and self.units[index - 1].end_ns != unit.start_ns:
                raise ValueError(
                    "ownership must be a complete nonoverlapping partition"
                )
            if unit.graph_symbols != self.units[0].graph_symbols:
                raise ValueError("ownership graph cannot vary by interval")
        if not self.dependency_roots:
            raise ValueError("ownership requires verification roots")
        _roots(self.dependency_roots)


def _roots(values: tuple[TrainingRootV1, ...]) -> None:
    if tuple(r.artifact_id for r in values) != tuple(
        sorted({r.artifact_id for r in values})
    ):
        raise ValueError("verification roots must be sorted and unique")


@dataclass(frozen=True, slots=True)
class TrainingRequestV1(TrainingContract):
    KIND: ClassVar[str] = "request"
    consumer_mode: TrainingConsumerMode
    start_ns: int
    end_ns: int
    symbols: tuple[str, ...]
    product_manifest_id: str | None = None
    decision_time_ns: int | None = None
    feature_artifact_id: str | None = None

    def _validate(self) -> None:
        _clock(self.start_ns)
        _clock(self.end_ns)
        if self.start_ns >= self.end_ns:
            raise ValueError("training selection interval is empty or reversed")
        if not self.symbols or self.symbols != tuple(sorted(set(self.symbols))):
            raise ValueError("training symbols must be sorted and unique")
        if any(re.fullmatch(r"[A-Z]{6}", s) is None for s in self.symbols):
            raise ValueError("invalid training symbol")
        if self.product_manifest_id is not None:
            _text(self.product_manifest_id)
        if self.feature_artifact_id is not None:
            _text(self.feature_artifact_id)
            if self.product_manifest_id is not None:
                raise ValueError("a row request selects one origin source")
        if self.decision_time_ns is not None:
            _clock(self.decision_time_ns)


@dataclass(frozen=True, slots=True)
class TrainingRowV1(TrainingContract):
    KIND: ClassVar[str] = "row"
    evidence_unit_id: str
    ownership_id: str
    origin: TrainingOrigin
    information_mode: TrainingInformationMode
    event_time_ns: int
    decision_time_ns: int
    available_at_ns: int | None
    symbol: str | None
    observed_dataset_version_id: str
    source_row_key: str
    run_id: str | None
    ensemble_member_id: str | None
    scenario_ids: tuple[str, ...]
    uncertainty_json: str
    feature_schema_version: str
    label_schema_version: str
    value_json: str
    verification_roots: tuple[TrainingRootV1, ...]
    admissible_modes: tuple[TrainingConsumerMode, ...]
    nonclaims: tuple[str, ...] = NONCLAIMS

    def _validate(self) -> None:
        for text in (
            self.evidence_unit_id,
            self.ownership_id,
            self.observed_dataset_version_id,
            self.source_row_key,
            self.feature_schema_version,
            self.label_schema_version,
        ):
            _text(text)
        _clock(self.event_time_ns)
        _clock(self.decision_time_ns)
        if self.decision_time_ns < self.event_time_ns:
            raise ValueError("training decision predates market row")
        if self.available_at_ns is not None:
            _clock(self.available_at_ns)
        if (
            self.symbol is not None
            and re.fullmatch(r"[A-Z]{6}", self.symbol) is None
        ):
            raise ValueError("invalid training row symbol")
        for optional_text in (self.run_id, self.ensemble_member_id):
            if optional_text is not None:
                _text(optional_text)
        if self.scenario_ids != tuple(sorted(set(self.scenario_ids))):
            raise ValueError("scenario lineage must be sorted and unique")
        for text in self.scenario_ids:
            _text(text)
        for text in (self.value_json, self.uncertainty_json):
            if training_json(training_load(text)) != text:
                raise ValueError("row payload must be canonical JSON")
        _roots(self.verification_roots)
        if not self.verification_roots or self.nonclaims != NONCLAIMS:
            raise ValueError("row must retain roots and nonclaims")
        if self.admissible_modes != admitted_modes(
            self.origin, self.information_mode
        ):
            raise ValueError("row admissibility differs from frozen policy")
        if self.information_mode is TrainingInformationMode.EX_ANTE:
            if (
                self.available_at_ns is None
                or self.available_at_ns > self.decision_time_ns
            ):
                raise ValueError(
                    "ex-ante row requires cutoff-qualified availability"
                )


def admitted_modes(
    origin: TrainingOrigin, information: TrainingInformationMode
) -> tuple[TrainingConsumerMode, ...]:
    """Closed v1 capability matrix, not an origin-string escape hatch."""
    if information is TrainingInformationMode.MIXED_FORBIDDEN:
        return ()
    if origin in (
        TrainingOrigin.OFFICIAL_CONTEXT,
        TrainingOrigin.MACHINE_FORECAST,
    ):
        return (
            (TrainingConsumerMode.DESCRIPTIVE,)
            if information is TrainingInformationMode.EX_POST
            else ()
        )
    if origin not in (
        TrainingOrigin.OBSERVED,
        TrainingOrigin.SYNTHETIC_RECONSTRUCTION,
        TrainingOrigin.BROKER_CONDITIONED_COUNTERFACTUAL,
    ):
        return ()
    # No supported legacy source proves historical availability. Reserved
    # ex-ante contracts are representable but no v1 materializer admits them.
    if information is TrainingInformationMode.EX_ANTE:
        return ()
    return (
        TrainingConsumerMode.RECONSTRUCTION,
        TrainingConsumerMode.AUGMENTATION,
        TrainingConsumerMode.DESCRIPTIVE,
    )


@dataclass(frozen=True, slots=True)
class TrainingBatchV1(TrainingContract):
    KIND: ClassVar[str] = "batch"
    source: TrainingSourceV1
    ownership: TrainingOwnershipV1
    request: TrainingRequestV1
    rows: tuple[TrainingRowV1, ...]

    def _validate(self) -> None:
        if self.source.dataset_version_id != self.ownership.dataset_version_id:
            raise ValueError("batch ownership source differs")
        units = {unit.artifact_id: unit for unit in self.ownership.units}
        ownership_id = self.ownership.artifact_id
        if len({row.source_row_key for row in self.rows}) != len(self.rows):
            raise ValueError("training batch cannot replicate source rows")
        for row in self.rows:
            unit = units.get(row.evidence_unit_id)
            if unit is None or row.ownership_id != ownership_id:
                raise ValueError("row lacks exact ownership")
            if not unit.start_ns <= row.event_time_ns < unit.end_ns:
                raise ValueError("row is outside its evidence unit")
            if (
                not self.request.start_ns
                <= row.event_time_ns
                < self.request.end_ns
            ):
                raise ValueError("row is outside requested interval")
            if (
                row.symbol is not None
                and row.symbol not in self.request.symbols
            ):
                raise ValueError("row is outside requested symbols")
            if self.request.consumer_mode not in row.admissible_modes:
                raise ValueError("row is inadmissible for requested consumer")
            if (
                row.observed_dataset_version_id
                != self.source.dataset_version_id
            ):
                raise ValueError("row source differs from batch")
        if self.request.consumer_mode is TrainingConsumerMode.CAUSAL:
            raise ValueError(
                "v1 legacy sources do not prove historical availability"
            )

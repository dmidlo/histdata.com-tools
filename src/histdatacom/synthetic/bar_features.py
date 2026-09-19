"""Closed-bar, cutoff-qualified market state over replay-verified products.

Event hashes prove values, not when a person could have known them. Availability
declarations are separately bound assertions; replay-clock declarations require
an explicit simulation opt-in and never certify historical knowledge. No API in
this module silently fills an absent bar or promotes partial support.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from enum import Enum
from types import UnionType
from typing import Any, ClassVar, TypeVar, Union, cast, get_args, get_origin
from typing import get_type_hints

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bars import (
    STANDARD_DERIVED_BAR_INTERVALS,
    DerivedBarV1,
    iter_committed_reconstruction_bars,
    iter_derived_bar_batches,
    load_derived_bar_manifest,
    verify_derived_bar_publication,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    load_reconstruction_manifest,
    verify_reconstruction_publication,
)

MAX_BAR_FEATURE_ARTIFACT_BYTES = 4 * 1024 * 1024
MAX_BAR_FEATURE_COLLECTION_ITEMS = 4096
MAX_BAR_FEATURE_SOURCE_EVENTS = 1_000_000
MAX_BAR_FEATURE_SOURCE_BARS = 100_000
BAR_FEATURE_REGISTRY_VERSION = "histdatacom.causal-bar-feature-registry.v1"
_INT64_MAX = 2**63 - 1
_DEPENDENCY_BARS = 4
_SCOPES = tuple(ActivitySliceScope)
_ArtifactT = TypeVar("_ArtifactT", bound="_Artifact")


def _text(value: str, name: str) -> None:
    if not value or value != value.strip() or len(value) > 1024:
        raise ValueError(f"{name} requires bounded nonempty text")
    if any(ord(char) < 32 for char in value):
        raise ValueError(f"{name} cannot contain control characters")


def _ns(value: int, name: str) -> None:
    if type(value) is not int or not 0 <= value <= _INT64_MAX:
        raise ValueError(f"{name} requires a nonnegative int64 timestamp")


def _symbol(value: str) -> None:
    if not re.fullmatch(r"[A-Z]{6}", value) or value[:3] == value[3:]:
        raise ValueError("symbol requires a canonical FX pair")


def _bar_id(value: str) -> None:
    if not re.fullmatch(r"derived-bar:sha256:[a-f0-9]{64}", value):
        raise ValueError("source bar identity is malformed")


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in values:
        if name in result:
            raise ValueError("duplicate bar-feature JSON member")
        result[name] = value
    return result


def _check_json(value: object, depth: int = 0) -> None:
    if depth > 16:
        raise ValueError("bar-feature JSON nesting exceeds its limit")
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if not -_INT64_MAX <= value <= _INT64_MAX:
            raise ValueError("bar-feature integer is outside int64")
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("bar-feature numbers must be finite")
        return
    if type(value) is str:
        if len(value) > MAX_BAR_FEATURE_ARTIFACT_BYTES:
            raise ValueError("bar-feature text exceeds its limit")
        return
    if isinstance(value, (dict, list)):
        if len(value) > MAX_BAR_FEATURE_COLLECTION_ITEMS:
            raise ValueError("bar-feature collection exceeds its limit")
        if isinstance(value, dict):
            for key, item in value.items():
                if type(key) is not str:
                    raise ValueError("bar-feature JSON keys must be text")
                _check_json(key, depth + 1)
                _check_json(item, depth + 1)
        else:
            for item in value:
                _check_json(item, depth + 1)
        return
    raise ValueError("unsupported bar-feature JSON value")


def canonical_bar_feature_json(value: Mapping[str, object]) -> str:
    """Serialize finite, bounded evidence without scalar coercion."""
    body = dict(value)
    _check_json(body)
    result = json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    if len(result.encode()) > MAX_BAR_FEATURE_ARTIFACT_BYTES:
        raise ValueError("bar-feature artifact exceeds its byte limit")
    return result


def _parse_json(text: str) -> dict[str, object]:
    if (
        type(text) is not str
        or len(text.encode()) > MAX_BAR_FEATURE_ARTIFACT_BYTES
    ):
        raise ValueError("bar-feature artifact exceeds its byte limit")
    try:
        value: object = json.loads(text, object_pairs_hook=_pairs)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid bar-feature JSON") from exc
    _check_json(value)
    if not isinstance(value, dict):
        raise ValueError("bar-feature JSON must be an object")
    return cast(dict[str, object], value)


def _wire(value: object, depth: int = 0) -> object:
    if depth > 16:
        raise ValueError("bar-feature field nesting exceeds its limit")
    if isinstance(value, (_Artifact, DerivedBarV1)):
        return value.to_dict()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        if len(value) > MAX_BAR_FEATURE_COLLECTION_ITEMS:
            raise ValueError("bar-feature collection exceeds its limit")
        return [_wire(item, depth + 1) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise ValueError("bar features require typed immutable fields")


def _decode(annotation: object, value: object) -> object:
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        for option in get_args(annotation):
            try:
                return _decode(option, value)
            except (TypeError, ValueError):
                pass
        raise ValueError("bar-feature optional field has the wrong type")
    if annotation is type(None):
        if value is not None:
            raise ValueError("expected null")
        return None
    if origin is tuple:
        if (
            not isinstance(value, list)
            or len(value) > MAX_BAR_FEATURE_COLLECTION_ITEMS
        ):
            raise ValueError("expected a bounded bar-feature array")
        return tuple(_decode(get_args(annotation)[0], item) for item in value)
    if annotation in (str, int, float, bool):
        if type(value) is not annotation:
            raise ValueError("bar-feature scalar has the wrong exact type")
        return value
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if type(value) is not str:
            raise ValueError("bar-feature enums require text")
        return annotation(value)
    if isinstance(annotation, type) and (
        issubclass(annotation, _Artifact) or annotation is DerivedBarV1
    ):
        if not isinstance(value, dict):
            raise ValueError("bar-feature child artifacts require objects")
        restored = annotation.from_dict(value)
        if annotation is DerivedBarV1 and canonical_bar_feature_json(
            {"bar": value}
        ) != canonical_bar_feature_json({"bar": restored.to_dict()}):
            raise ValueError(
                "nested bar payload is not exact canonical evidence"
            )
        return restored
    raise ValueError("unsupported bar-feature field type")


class _Artifact:
    __slots__ = ()
    KIND: ClassVar[str]

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        for item in fields(cast(Any, self)):
            value = _decode(hints[item.name], _wire(getattr(self, item.name)))
            object.__setattr__(self, item.name, value)
        self._validate()
        self.to_json()

    def _validate(self) -> None:
        raise NotImplementedError

    @property
    def schema_version(self) -> str:
        return f"histdatacom.causal-bar-{self.KIND}.v1"

    def identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **{
                item.name: _wire(getattr(self, item.name))
                for item in fields(cast(Any, self))
            },
        }

    @property
    def artifact_id(self) -> str:
        digest = hashlib.sha256(
            canonical_bar_feature_json(self.identity_payload()).encode()
        ).hexdigest()
        return f"causal-bar-{self.KIND}:sha256:{digest}"

    def to_dict(self) -> dict[str, object]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    def to_json(self) -> str:
        return canonical_bar_feature_json(self.to_dict())

    @classmethod
    def from_dict(  # noqa: PYI019
        cls: type[_ArtifactT], data: Mapping[str, object]
    ) -> _ArtifactT:
        names = {item.name for item in fields(cast(Any, cls))}
        if set(data) != names | {"schema_version", "artifact_id"}:
            raise ValueError(
                "bar-feature artifact fields are missing or unknown"
            )
        if data["schema_version"] != f"histdatacom.causal-bar-{cls.KIND}.v1":
            raise ValueError("unsupported bar-feature schema")
        hints = get_type_hints(cls)
        instance = cast(
            _ArtifactT,
            cast(Any, cls)(
                **{name: _decode(hints[name], data[name]) for name in names}
            ),
        )
        if data["artifact_id"] != instance.artifact_id:
            raise ValueError("bar-feature artifact identity differs")
        return instance

    @classmethod
    def from_json(  # noqa: PYI019
        cls: type[_ArtifactT], text: str
    ) -> _ArtifactT:
        return cls.from_dict(_parse_json(text))


class BarFeatureState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    MISSING_BAR = "missing_bar"
    EXPECTED_CLOSURE = "expected_closure"
    SOURCE_OUTAGE = "source_outage"
    UNSUPPORTED = "unsupported"
    INSUFFICIENT_WARMUP = "insufficient_warmup"
    PARTIAL_SUPPORT = "partial_support"
    NO_TRANSITION_SUPPORT = "no_transition_support"
    NO_CONFIDENCE_SUPPORT = "no_confidence_support"
    ZERO_ACTIVITY_DURATION = "zero_activity_duration"
    ZERO_SCALE = "zero_scale"


class BarAvailabilityBasis(str, Enum):
    DECLARED_SOURCE_CLOCK = "declared_source_clock"
    REPLAY_CLOCK_ASSUMPTION = "replay_clock_assumption"


@dataclass(frozen=True, slots=True)
class BarFeatureDefinitionV1(_Artifact):
    name: str
    units: str
    required_closed_bars: int
    lag_bars: int
    formula: str
    KIND: ClassVar[str] = "definition"

    def _validate(self) -> None:
        for name in ("name", "units", "formula"):
            _text(getattr(self, name), name)
        if (
            not 1 <= self.required_closed_bars <= _DEPENDENCY_BARS
            or self.lag_bars not in (0, 1)
        ):
            raise ValueError("unsupported closed-bar dependency span")


_OHLC_NAMES = tuple(
    f"{price}_{part}"
    for price in ("bid", "ask", "mid", "spread")
    for part in ("open", "high", "low", "close")
)
_COUNT_NAMES = (
    "event_count",
    "observed_event_count",
    "synthetic_event_count",
    "quote_update_count",
    "price_change_count",
    "stale_quote_count",
    "transition_count",
    "confidence_support_count",
)
_PRIMITIVES = (
    *_OHLC_NAMES,
    *_COUNT_NAMES,
    "mean_spread",
    "tick_intensity_per_second",
    "stale_quote_rate",
    "mean_event_confidence",
)
_INDICATORS = (
    "mid_log_open_close_return",
    "mid_log_close_return",
    "mid_log_range",
    "mid_body",
    "mid_upper_wick",
    "mid_lower_wick",
    "mid_true_range",
    "observed_proportion",
    "synthetic_proportion",
    "mid_body_range_ratio",
    "mid_sma_3",
    "mid_momentum_3",
    "mid_slope_3",
    "mid_realized_variation_3",
    "mid_atr_3",
    "mid_prior_mean_3",
    "mid_prior_std_3",
    "mid_prior_zscore_3",
)
_FEATURE_NAMES = (*_PRIMITIVES, *_INDICATORS)


def bar_feature_definitions() -> tuple[BarFeatureDefinitionV1, ...]:
    """Declare every supported feature, unit, dependency and frozen formula."""
    result = []
    for name in _PRIMITIVES:
        units = "quote_currency_per_base_currency"
        if name in _COUNT_NAMES:
            units = "events"
        elif name == "tick_intensity_per_second":
            units = "events_per_second_of_event_support"
        elif name in ("stale_quote_rate", "mean_event_confidence"):
            units = "proportion"
        result.append(
            BarFeatureDefinitionV1(
                name, units, 1, 0, f"exact verified bar.{name}"
            )
        )
    formulas = {
        "mid_log_open_close_return": "log(current.mid_close/current.mid_open)",
        "mid_log_close_return": "log(current.mid_close/previous.mid_close)",
        "mid_log_range": "log(current.mid_high/current.mid_low)",
        "mid_body": "current.mid_close-current.mid_open",
        "mid_upper_wick": "current.mid_high-max(current.mid_open,current.mid_close)",
        "mid_lower_wick": "min(current.mid_open,current.mid_close)-current.mid_low",
        "mid_true_range": "max(high-low,abs(high-previous.close),abs(low-previous.close))",
        "observed_proportion": "observed_event_count/event_count",
        "synthetic_proportion": "synthetic_event_count/event_count",
        "mid_body_range_ratio": "(close-open)/(high-low); zero range is unavailable",
        "mid_sma_3": "arithmetic mean of latest three consecutive closed mid closes",
        "mid_momentum_3": "current.close-close three closed bars earlier",
        "mid_slope_3": "OLS close slope over latest three bars at x=0,1,2: (current.close-close two bars earlier)/2",
        "mid_realized_variation_3": "sum of squared close log returns over latest three transitions; no annualization or square root",
        "mid_atr_3": "arithmetic mean of latest three true ranges, each using its immediately previous close",
        "mid_prior_mean_3": "arithmetic mean of three closes strictly preceding current bar",
        "mid_prior_std_3": "population standard deviation of three closes strictly preceding current bar",
        "mid_prior_zscore_3": "(current.close-prior_mean_3)/prior_std_3; zero standard deviation is unavailable",
    }
    for name in _INDICATORS:
        needed = _required_bars(name)
        prior = name in ("mid_prior_mean_3", "mid_prior_std_3")
        units = (
            "dimensionless"
            if "log" in name
            or "proportion" in name
            or name
            in (
                "mid_body_range_ratio",
                "mid_realized_variation_3",
                "mid_prior_zscore_3",
            )
            else "quote_currency_per_base_currency"
        )
        if name == "mid_slope_3":
            units = "quote_currency_per_base_currency_per_bar"
        result.append(
            BarFeatureDefinitionV1(
                name, units, needed, 1 if prior else 0, formulas[name]
            )
        )
    return tuple(result)


def _required_bars(name: str) -> int:
    if name in ("mid_sma_3", "mid_slope_3"):
        return 3
    if name.endswith("_3"):
        return 4
    return 2 if name in ("mid_log_close_return", "mid_true_range") else 1


@dataclass(frozen=True, slots=True)
class BarFeaturePolicyV1(_Artifact):
    information_mode: InformationMode
    intervals: tuple[str, ...] = tuple(STANDARD_DERIVED_BAR_INTERVALS)
    scopes: tuple[ActivitySliceScope, ...] = _SCOPES
    feature_names: tuple[str, ...] = _FEATURE_NAMES
    rounding_digits: int = 12
    allow_replay_clock_assumptions: bool = False
    allow_prior_generated_state: bool = False
    max_source_events: int = 100_000
    max_source_bars: int = 10_000
    registry_version: str = BAR_FEATURE_REGISTRY_VERSION
    KIND: ClassVar[str] = "policy"

    def _validate(self) -> None:
        for values, allowed, label in (
            (
                self.intervals,
                tuple(STANDARD_DERIVED_BAR_INTERVALS),
                "intervals",
            ),
            (self.scopes, _SCOPES, "scopes"),
            (self.feature_names, _FEATURE_NAMES, "features"),
        ):
            if (
                not values
                or len(values) != len(set(values))
                or not set(values) <= set(allowed)
            ):
                raise ValueError(
                    f"bar-feature {label} are empty, duplicate or unsupported"
                )
        object.__setattr__(
            self,
            "intervals",
            tuple(
                i for i in STANDARD_DERIVED_BAR_INTERVALS if i in self.intervals
            ),
        )
        object.__setattr__(
            self, "scopes", tuple(i for i in _SCOPES if i in self.scopes)
        )
        object.__setattr__(
            self,
            "feature_names",
            tuple(i for i in _FEATURE_NAMES if i in self.feature_names),
        )
        if not 0 <= self.rounding_digits <= 15:
            raise ValueError(
                "bar-feature rounding digits must be within [0,15]"
            )
        if not 1 <= self.max_source_events <= MAX_BAR_FEATURE_SOURCE_EVENTS:
            raise ValueError("bar-feature source event budget is invalid")
        if not 1 <= self.max_source_bars <= MAX_BAR_FEATURE_SOURCE_BARS:
            raise ValueError("bar-feature source bar budget is invalid")
        if self.registry_version != BAR_FEATURE_REGISTRY_VERSION:
            raise ValueError("unsupported bar-feature registry")


@dataclass(frozen=True, slots=True)
class BarAvailabilityDeclarationV1(_Artifact):
    """An explicit availability assertion, not a certificate of old knowledge.

    ``known_at_ns`` governs when this declaration itself may influence state.
    ``generated_at_ns`` is mandatory for any non-observed bar, including a
    separately admitted prior-generated simulation. Opaque IDs prove nothing.
    """

    bar_id: str
    available_at_ns: int
    known_at_ns: int
    basis: BarAvailabilityBasis
    explanation: str
    generated_at_ns: int | None = None
    KIND: ClassVar[str] = "availability"

    def _validate(self) -> None:
        _bar_id(self.bar_id)
        _ns(self.available_at_ns, "available_at_ns")
        _ns(self.known_at_ns, "known_at_ns")
        _text(self.explanation, "availability explanation")
        if self.generated_at_ns is not None:
            _ns(self.generated_at_ns, "generated_at_ns")
            if self.generated_at_ns > self.available_at_ns:
                raise ValueError(
                    "generated state cannot be available before generation"
                )


@dataclass(frozen=True, slots=True)
class BarAbsenceDeclarationV1(_Artifact):
    symbol: str
    scope: ActivitySliceScope
    interval_code: str
    bar_start_ns: int
    known_at_ns: int
    state: BarFeatureState
    explanation: str
    KIND: ClassVar[str] = "absence"

    def _validate(self) -> None:
        _symbol(self.symbol)
        _ns(self.bar_start_ns, "bar_start_ns")
        _ns(self.known_at_ns, "known_at_ns")
        _text(self.explanation, "absence explanation")
        duration = STANDARD_DERIVED_BAR_INTERVALS.get(self.interval_code)
        if duration is None or self.bar_start_ns % duration:
            raise ValueError(
                "absence requires a supported UTC aligned interval"
            )
        if self.state not in (
            BarFeatureState.MISSING_BAR,
            BarFeatureState.EXPECTED_CLOSURE,
            BarFeatureState.SOURCE_OUTAGE,
            BarFeatureState.UNSUPPORTED,
        ):
            raise ValueError("unsupported declared absence state")


@dataclass(frozen=True, slots=True)
class BarFeatureValueV1(_Artifact):
    name: str
    value: int | float | None
    state: BarFeatureState
    source_bar_ids: tuple[str, ...]
    available_at_ns: int | None
    KIND: ClassVar[str] = "value"

    def _validate(self) -> None:
        if self.name not in _FEATURE_NAMES:
            raise ValueError("unknown bar feature")
        if (self.value is not None) != (
            self.state is BarFeatureState.AVAILABLE
        ):
            raise ValueError("only available features have a value")
        if type(self.value) is float and not math.isfinite(self.value):
            raise ValueError("bar feature must be finite")
        if len(self.source_bar_ids) > _DEPENDENCY_BARS or len(
            set(self.source_bar_ids)
        ) != len(self.source_bar_ids):
            raise ValueError("bar feature evidence is ambiguous")
        for identity in self.source_bar_ids:
            _bar_id(identity)
        if self.available_at_ns is not None:
            _ns(self.available_at_ns, "available_at_ns")
        if self.value is not None and (
            not self.source_bar_ids or self.available_at_ns is None
        ):
            raise ValueError("available feature lacks exact source support")


@dataclass(frozen=True, slots=True)
class BarFeatureCellV1(_Artifact):
    scope: ActivitySliceScope
    interval_code: str
    bar_start_ns: int
    bar_end_ns: int
    state: BarFeatureState
    partial: bool
    values: tuple[BarFeatureValueV1, ...]
    KIND: ClassVar[str] = "cell"

    def _validate(self) -> None:
        duration = STANDARD_DERIVED_BAR_INTERVALS.get(self.interval_code)
        _ns(self.bar_start_ns, "bar_start_ns")
        _ns(self.bar_end_ns, "bar_end_ns")
        if (
            duration is None
            or self.bar_start_ns % duration
            or self.bar_end_ns != self.bar_start_ns + duration
        ):
            raise ValueError(
                "feature cell bounds require an aligned half-open interval"
            )
        if self.partial != (self.state is BarFeatureState.PARTIAL_SUPPORT):
            raise ValueError(
                "partial support cannot masquerade as a closed bar"
            )
        if not self.values or len({item.name for item in self.values}) != len(
            self.values
        ):
            raise ValueError("feature cell fields are empty or duplicate")

    def feature(self, name: str) -> BarFeatureValueV1:
        for value in self.values:
            if value.name == name:
                return value
        raise ValueError(f"feature is not enabled: {name}")


@dataclass(frozen=True, slots=True)
class CausalBarSnapshotV1(_Artifact):
    symbol: str
    decision_time_ns: int
    policy: BarFeaturePolicyV1
    bars: tuple[DerivedBarV1, ...]
    availability: tuple[BarAvailabilityDeclarationV1, ...]
    absences: tuple[BarAbsenceDeclarationV1, ...]
    cells: tuple[BarFeatureCellV1, ...]
    source_product_manifest_id: str
    derived_bar_manifest_id: str
    historical_availability_verified: bool = False
    KIND: ClassVar[str] = "snapshot"

    def _validate(self) -> None:
        _symbol(self.symbol)
        _ns(self.decision_time_ns, "decision_time_ns")
        if not re.fullmatch(
            r"reconstruction-manifest(?:-v[23])?:sha256:[a-f0-9]{64}",
            self.source_product_manifest_id,
        ):
            raise ValueError("snapshot source product identity is malformed")
        if not re.fullmatch(
            r"derived-bar-manifest:sha256:[a-f0-9]{64}",
            self.derived_bar_manifest_id,
        ):
            raise ValueError(
                "snapshot derived bar manifest identity is malformed"
            )
        if self.historical_availability_verified:
            raise ValueError(
                "bar replay cannot certify historical availability"
            )
        if len({bar.bar_id for bar in self.bars}) != len(self.bars):
            raise ValueError("snapshot has duplicate bar evidence")
        if (
            len(
                {
                    (
                        bar.source_product_manifest_id,
                        bar.policy_id,
                        bar.run_id,
                        bar.ensemble_member_id,
                    )
                    for bar in self.bars
                }
            )
            > 1
        ):
            raise ValueError(
                "snapshot mixes source product, bar policy, run or member"
            )
        declarations = {item.bar_id: item for item in self.availability}
        if len(declarations) != len(self.availability) or set(declarations) != {
            bar.bar_id for bar in self.bars
        }:
            raise ValueError(
                "snapshot availability must cover exact admitted bar evidence"
            )
        for bar in self.bars:
            if (
                bar.source_product_manifest_id
                != self.source_product_manifest_id
            ):
                raise ValueError(
                    "snapshot bars bind a different source product"
                )
            _admit_bar(
                bar,
                declarations[bar.bar_id],
                self.policy,
                self.symbol,
                self.decision_time_ns,
            )
            duration = STANDARD_DERIVED_BAR_INTERVALS[bar.interval_code]
            boundary = self.decision_time_ns // duration * duration
            if bar.bar_start_ns not in tuple(
                boundary - n * duration for n in range(1, _DEPENDENCY_BARS + 1)
            ):
                raise ValueError(
                    "snapshot includes evidence outside its dependency span"
                )
        for declaration in self.absences:
            _admit_absence(
                declaration, self.policy, self.symbol, self.decision_time_ns
            )
        expected = _cells(
            self.symbol,
            self.decision_time_ns,
            self.policy,
            self.bars,
            self.availability,
            self.absences,
        )
        if self.cells != expected:
            raise ValueError(
                "snapshot features do not replay from their exact evidence"
            )
        if self.bars != tuple(
            sorted(
                self.bars,
                key=lambda bar: (
                    bar.interval_ns,
                    bar.scope.value,
                    bar.bar_start_ns,
                ),
            )
        ):
            raise ValueError("snapshot bar evidence is not canonically ordered")
        if self.availability != tuple(
            sorted(self.availability, key=lambda item: item.bar_id)
        ):
            raise ValueError("snapshot availability is not canonically ordered")
        if self.absences != tuple(
            sorted(
                self.absences,
                key=lambda item: (
                    item.interval_code,
                    item.scope.value,
                    item.bar_start_ns,
                ),
            )
        ):
            raise ValueError(
                "snapshot absence declarations are not canonically ordered"
            )

    def cell(
        self, scope: ActivitySliceScope | str, interval_code: str
    ) -> BarFeatureCellV1:
        normalized = ActivitySliceScope(scope)
        for cell in self.cells:
            if cell.scope is normalized and cell.interval_code == interval_code:
                return cell
        raise ValueError("snapshot has no requested scope/timeframe")

    @property
    def uses_replay_clock(self) -> bool:
        return any(
            item.basis is BarAvailabilityBasis.REPLAY_CLOCK_ASSUMPTION
            for item in self.availability
        )


def _admit_bar(
    bar: DerivedBarV1,
    declaration: BarAvailabilityDeclarationV1,
    policy: BarFeaturePolicyV1,
    symbol: str,
    cutoff: int,
) -> None:
    if (
        bar.symbol.upper() != symbol
        or bar.scope not in policy.scopes
        or bar.interval_code not in policy.intervals
    ):
        raise ValueError("bar evidence differs from the snapshot axis")
    if declaration.bar_id != bar.bar_id:
        raise ValueError("bar availability binds different evidence")
    if declaration.known_at_ns > cutoff or declaration.available_at_ns > cutoff:
        raise ValueError("future availability cannot enter a causal snapshot")
    if declaration.available_at_ns < bar.bar_end_ns:
        raise ValueError("a standard closed bar is unavailable before its end")
    if (
        declaration.basis is BarAvailabilityBasis.REPLAY_CLOCK_ASSUMPTION
        and not policy.allow_replay_clock_assumptions
    ):
        raise ValueError(
            "replay-clock assumptions require explicit policy admission"
        )
    if bar.scope is not ActivitySliceScope.OBSERVED:
        if policy.information_mode is InformationMode.EX_ANTE_SIMULATION:
            raise ValueError(
                "source products lack a bound information audit; generated ex-ante state is unsupported"
            )
        if (
            not policy.allow_prior_generated_state
            or declaration.generated_at_ns is None
        ):
            raise ValueError(
                "prior-generated state requires explicit policy and generation clock"
            )
        if declaration.generated_at_ns > cutoff:
            raise ValueError("not-yet-generated state cannot enter a snapshot")


def _admit_absence(
    declaration: BarAbsenceDeclarationV1,
    policy: BarFeaturePolicyV1,
    symbol: str,
    cutoff: int,
) -> None:
    if (
        declaration.symbol != symbol
        or declaration.scope not in policy.scopes
        or declaration.interval_code not in policy.intervals
    ):
        raise ValueError("absence evidence differs from the snapshot axis")
    duration = STANDARD_DERIVED_BAR_INTERVALS[declaration.interval_code]
    if (
        declaration.known_at_ns > cutoff
        or declaration.bar_start_ns
        not in tuple(
            cutoff // duration * duration - n * duration
            for n in range(1, _DEPENDENCY_BARS + 1)
        )
    ):
        raise ValueError(
            "absence declaration is not cutoff-qualified for the selected bar"
        )


def _cells(
    symbol: str,
    cutoff: int,
    policy: BarFeaturePolicyV1,
    bars: tuple[DerivedBarV1, ...],
    availability: tuple[BarAvailabilityDeclarationV1, ...],
    absences: tuple[BarAbsenceDeclarationV1, ...],
) -> tuple[BarFeatureCellV1, ...]:
    indexed = {
        (bar.scope, bar.interval_code, bar.bar_start_ns): bar for bar in bars
    }
    if len(indexed) != len(bars):
        raise ValueError("ambiguous bar ownership")
    clocks = {
        item.bar_id: max(item.available_at_ns, item.known_at_ns)
        for item in availability
    }
    absent = {
        (item.scope, item.interval_code, item.bar_start_ns): item
        for item in absences
    }
    if len(absent) != len(absences):
        raise ValueError("ambiguous absence ownership")
    if set(indexed) & set(absent):
        raise ValueError(
            "absence declaration conflicts with available bar evidence"
        )
    result = []
    for interval in policy.intervals:
        duration = STANDARD_DERIVED_BAR_INTERVALS[interval]
        end = cutoff // duration * duration
        if end < duration:
            raise ValueError(
                "cutoff precedes the first representable closed bar"
            )
        for scope in policy.scopes:
            key = (scope, interval, end - duration)
            current = indexed.get(key)
            declaration = absent.get(key)
            if current is not None and declaration is not None:
                raise ValueError(
                    "absence declaration conflicts with available bar evidence"
                )
            state = (
                declaration.state
                if declaration is not None
                else BarFeatureState.UNAVAILABLE
            )
            if current is not None:
                state = (
                    BarFeatureState.PARTIAL_SUPPORT
                    if current.is_partial_start or current.is_partial_end
                    else BarFeatureState.AVAILABLE
                )
            history = tuple(
                indexed.get((scope, interval, end - n * duration))
                for n in range(_DEPENDENCY_BARS, 0, -1)
            )
            values = tuple(
                _feature(name, history, state, clocks, policy.rounding_digits)
                for name in policy.feature_names
            )
            result.append(
                BarFeatureCellV1(
                    scope,
                    interval,
                    end - duration,
                    end,
                    state,
                    state is BarFeatureState.PARTIAL_SUPPORT,
                    values,
                )
            )
    return tuple(result)


def _feature(
    name: str,
    history: tuple[DerivedBarV1 | None, ...],
    state: BarFeatureState,
    clocks: Mapping[str, int],
    digits: int,
) -> BarFeatureValueV1:
    current = history[-1]
    ids: tuple[str, ...] = () if current is None else (current.bar_id,)
    available = None if current is None else clocks[current.bar_id]
    if state is not BarFeatureState.AVAILABLE or current is None:
        return BarFeatureValueV1(name, None, state, ids, available)
    if name in _PRIMITIVES:
        value = cast(int | float | None, getattr(current, name))
        if value is None:
            missing = {
                "stale_quote_rate": BarFeatureState.NO_TRANSITION_SUPPORT,
                "mean_event_confidence": BarFeatureState.NO_CONFIDENCE_SUPPORT,
                "tick_intensity_per_second": BarFeatureState.ZERO_ACTIVITY_DURATION,
            }[name]
            return BarFeatureValueV1(name, None, missing, ids, available)
        return BarFeatureValueV1(name, value, state, ids, available)
    needed = history[-_required_bars(name) :]
    if any(
        bar is None or bar.is_partial_start or bar.is_partial_end
        for bar in needed
    ):
        ids = tuple(bar.bar_id for bar in needed if bar is not None)
        return BarFeatureValueV1(
            name,
            None,
            BarFeatureState.INSUFFICIENT_WARMUP,
            ids,
            max(clocks[identity] for identity in ids),
        )
    support = cast(tuple[DerivedBarV1, ...], needed)
    ids = tuple(bar.bar_id for bar in support)
    available = max(clocks[identity] for identity in ids)
    if name in ("mid_log_close_return", "mid_true_range"):
        previous = support[-2]
        available = max(clocks[identity] for identity in ids)
        value = (
            math.log(current.mid_close / previous.mid_close)
            if name == "mid_log_close_return"
            else max(
                current.mid_high - current.mid_low,
                abs(current.mid_high - previous.mid_close),
                abs(current.mid_low - previous.mid_close),
            )
        )
    elif name.endswith("_3"):
        closes = [bar.mid_close for bar in support]
        if name == "mid_sma_3":
            value = sum(closes) / 3
        elif name == "mid_slope_3":
            value = (closes[-1] - closes[0]) / 2
        elif name == "mid_momentum_3":
            value = closes[-1] - closes[0]
        elif name == "mid_realized_variation_3":
            value = sum(
                math.log(right / left) ** 2
                for left, right in zip(closes, closes[1:])
            )
        elif name == "mid_atr_3":
            value = (
                sum(
                    max(
                        right.mid_high - right.mid_low,
                        abs(right.mid_high - left.mid_close),
                        abs(right.mid_low - left.mid_close),
                    )
                    for left, right in zip(support, support[1:])
                )
                / 3
            )
        else:
            mean = sum(closes[:-1]) / 3
            std = math.sqrt(sum((item - mean) ** 2 for item in closes[:-1]) / 3)
            if name == "mid_prior_mean_3":
                value = mean
            elif name == "mid_prior_std_3":
                value = std
            elif std == 0:
                return BarFeatureValueV1(
                    name, None, BarFeatureState.ZERO_SCALE, ids, available
                )
            else:
                value = (closes[-1] - mean) / std
    elif name == "mid_body_range_ratio":
        width = current.mid_high - current.mid_low
        if width == 0:
            return BarFeatureValueV1(
                name, None, BarFeatureState.ZERO_SCALE, ids, available
            )
        value = (current.mid_close - current.mid_open) / width
    else:
        value = {
            "mid_log_open_close_return": math.log(
                current.mid_close / current.mid_open
            ),
            "mid_log_range": math.log(current.mid_high / current.mid_low),
            "mid_body": current.mid_close - current.mid_open,
            "mid_upper_wick": current.mid_high
            - max(current.mid_open, current.mid_close),
            "mid_lower_wick": min(current.mid_open, current.mid_close)
            - current.mid_low,
            "observed_proportion": current.observed_event_count
            / current.event_count,
            "synthetic_proportion": current.synthetic_event_count
            / current.event_count,
        }[name]
    return BarFeatureValueV1(name, round(value, digits), state, ids, available)


@dataclass(frozen=True, slots=True)
class BarFeatureSourceV1:
    """Filesystem inputs, never a caller-supplied 'verified' identity token.

    Every build/verify checks committed publications and replays bars from the
    verified event product. No source pathname enters the snapshot identity.
    """

    reconstruction_manifest_path: str
    bar_manifest_path: str

    def verified_bars(
        self, symbol: str, policy: BarFeaturePolicyV1
    ) -> tuple[DerivedBarV1, ...]:
        """Return replay-verified rows from bounded committed products."""
        return self._verified_evidence(symbol, policy)[2]

    def _verified_evidence(
        self, symbol: str, policy: BarFeaturePolicyV1
    ) -> tuple[str, str, tuple[DerivedBarV1, ...]]:
        _symbol(symbol)
        declared_product = load_reconstruction_manifest(
            self.reconstruction_manifest_path
        )
        declared_bars = load_derived_bar_manifest(self.bar_manifest_path)
        if declared_product.event_count > policy.max_source_events:
            raise ValueError(
                "total source event verification exceeds the feature budget"
            )
        if declared_bars.bar_count > policy.max_source_bars:
            raise ValueError(
                "total source bar verification exceeds the feature budget"
            )
        product = verify_reconstruction_publication(
            self.reconstruction_manifest_path
        )
        manifest = verify_derived_bar_publication(self.bar_manifest_path)
        if (
            product.manifest_id != declared_product.manifest_id
            or manifest.manifest_id != declared_bars.manifest_id
        ):
            raise ValueError("source manifests changed during verification")
        if (
            manifest.source_product_manifest_id != product.manifest_id
            or manifest.source_product_publication_id != product.publication_id
            or manifest.source_product_logical_sha256
            != product.replay.logical_content_sha256
        ):
            raise ValueError(
                "bar product does not bind the verified source product"
            )
        if (
            symbol.lower() not in product.symbols
            or symbol.lower() not in manifest.symbols
        ):
            raise ValueError(
                "symbol is not covered by the verified source products"
            )
        if (
            product.symbol_event_counts[symbol.lower()]
            > policy.max_source_events
        ):
            raise ValueError(
                "source event verification exceeds the feature budget"
            )
        if manifest.symbol_bar_counts[symbol.lower()] > policy.max_source_bars:
            raise ValueError(
                "source bar verification exceeds the feature budget"
            )
        stored = []
        for batch in iter_derived_bar_batches(
            self.bar_manifest_path, symbols=(symbol,)
        ):
            for value in batch.to_pylist():
                stored.append(DerivedBarV1.from_dict(value))
                if len(stored) > policy.max_source_bars:
                    raise ValueError(
                        "source bar verification exceeds the feature budget"
                    )
        replayed = []
        for bar in iter_committed_reconstruction_bars(
            self.reconstruction_manifest_path,
            policy=manifest.policy,
            symbols=(symbol,),
            start_ns=manifest.query_start_ns,
            end_ns=manifest.query_end_ns,
        ):
            replayed.append(bar)
            if len(replayed) > policy.max_source_bars:
                raise ValueError("bar replay exceeds the feature budget")

        def key(bar: DerivedBarV1) -> tuple[int, str, int]:
            return bar.interval_ns, bar.scope.value, bar.bar_start_ns

        ordered = tuple(sorted(stored, key=key))
        if ordered != tuple(sorted(replayed, key=key)):
            raise ValueError(
                "stored bar values do not replay from verified source events"
            )
        return product.manifest_id, manifest.manifest_id, ordered

    def snapshot(
        self,
        *,
        symbol: str,
        decision_time_ns: int,
        policy: BarFeaturePolicyV1,
        availability: Sequence[BarAvailabilityDeclarationV1] = (),
        absences: Sequence[BarAbsenceDeclarationV1] = (),
    ) -> CausalBarSnapshotV1:
        """Build a closed-only snapshot without inspecting future explanations."""
        _ns(decision_time_ns, "decision_time_ns")
        if not isinstance(policy, BarFeaturePolicyV1):
            raise TypeError("snapshot requires a typed bar-feature policy")
        if (
            len(availability) > MAX_BAR_FEATURE_COLLECTION_ITEMS
            or len(absences) > MAX_BAR_FEATURE_COLLECTION_ITEMS
        ):
            raise ValueError(
                "availability evidence exceeds its bounded input budget"
            )
        declarations: dict[str, BarAvailabilityDeclarationV1] = {}
        for item in availability:
            if not isinstance(item, BarAvailabilityDeclarationV1):
                raise TypeError(
                    "availability evidence must be typed declarations"
                )
            if (
                item.known_at_ns > decision_time_ns
                or item.available_at_ns > decision_time_ns
            ):
                continue
            if item.bar_id in declarations:
                raise ValueError("ambiguous admitted availability declarations")
            declarations[item.bar_id] = item
        selected = []
        selected_clocks = []
        product_id, bar_manifest_id, verified_bars = self._verified_evidence(
            symbol, policy
        )
        for bar in verified_bars:
            if (
                bar.scope not in policy.scopes
                or bar.interval_code not in policy.intervals
            ):
                continue
            duration = STANDARD_DERIVED_BAR_INTERVALS[bar.interval_code]
            boundary = decision_time_ns // duration * duration
            if bar.bar_start_ns not in tuple(
                boundary - n * duration for n in range(1, _DEPENDENCY_BARS + 1)
            ):
                continue
            declaration = declarations.get(bar.bar_id)
            if declaration is None:
                continue
            _admit_bar(bar, declaration, policy, symbol, decision_time_ns)
            selected.append(bar)
            selected_clocks.append(declaration)
        selected_absences = []
        for absence in absences:
            if not isinstance(absence, BarAbsenceDeclarationV1):
                raise TypeError("absence evidence must be typed declarations")
            if absence.known_at_ns > decision_time_ns:
                continue
            if (
                absence.symbol != symbol
                or absence.scope not in policy.scopes
                or absence.interval_code not in policy.intervals
            ):
                continue
            duration = STANDARD_DERIVED_BAR_INTERVALS[absence.interval_code]
            if absence.bar_start_ns not in tuple(
                decision_time_ns // duration * duration - n * duration
                for n in range(1, _DEPENDENCY_BARS + 1)
            ):
                continue
            _admit_absence(absence, policy, symbol, decision_time_ns)
            selected_absences.append(absence)
        bars = tuple(
            sorted(
                selected,
                key=lambda bar: (
                    bar.interval_ns,
                    bar.scope.value,
                    bar.bar_start_ns,
                ),
            )
        )
        clocks = tuple(sorted(selected_clocks, key=lambda item: item.bar_id))
        missing = tuple(
            sorted(
                selected_absences,
                key=lambda item: (
                    item.interval_code,
                    item.scope.value,
                    item.bar_start_ns,
                ),
            )
        )
        return CausalBarSnapshotV1(
            symbol,
            decision_time_ns,
            policy,
            bars,
            clocks,
            missing,
            _cells(symbol, decision_time_ns, policy, bars, clocks, missing),
            product_id,
            bar_manifest_id,
        )

    def verify_snapshot(
        self,
        snapshot: CausalBarSnapshotV1,
        *,
        information_mode: InformationMode,
    ) -> None:
        """Recheck actual source bytes and exact snapshot replay at consumption."""
        if not isinstance(snapshot, CausalBarSnapshotV1):
            raise TypeError("consumer requires a typed causal bar snapshot")
        if snapshot.policy.information_mode is not InformationMode(
            information_mode
        ):
            raise ValueError("consumer and snapshot information modes differ")
        expected = self.snapshot(
            symbol=snapshot.symbol,
            decision_time_ns=snapshot.decision_time_ns,
            policy=snapshot.policy,
            availability=snapshot.availability,
            absences=snapshot.absences,
        )
        if expected.to_json() != snapshot.to_json():
            raise ValueError(
                "snapshot does not match replay-verified source evidence"
            )


@dataclass(frozen=True, slots=True)
class BarFeatureConsumerResultV1(_Artifact):
    """Useful consumer output with immutable snapshot/policy provenance.

    This wrapper is not itself evidence that an underlying model is useful or
    that availability assertions reflect an actual historical information set.
    """

    consumer: str
    information_mode: InformationMode
    snapshot_ids: tuple[str, ...]
    policy_ids: tuple[str, ...]
    result_json: str
    historical_availability_verified: bool = False
    empirical_qualification_claim: bool = False
    KIND: ClassVar[str] = "consumer-result"

    def _validate(self) -> None:
        if self.consumer not in (
            "reference_conditioning",
            "strategy",
            "broker_fingerprint",
            "broker_comparison",
            "training",
        ):
            raise ValueError("unknown causal bar consumer")
        if not self.snapshot_ids or len(set(self.snapshot_ids)) != len(
            self.snapshot_ids
        ):
            raise ValueError("consumer snapshots are empty or duplicate")
        if not self.policy_ids or len(set(self.policy_ids)) != len(
            self.policy_ids
        ):
            raise ValueError("consumer policies are empty or duplicate")
        for identities, kind in (
            (self.snapshot_ids, "snapshot"),
            (self.policy_ids, "policy"),
        ):
            for identity in identities:
                if not re.fullmatch(
                    rf"causal-bar-{kind}:sha256:[a-f0-9]{{64}}", identity
                ):
                    raise ValueError(
                        "consumer provenance identity is malformed"
                    )
        if (
            self.historical_availability_verified
            or self.empirical_qualification_claim
        ):
            raise ValueError(
                "bar feature adoption does not qualify historical or empirical claims"
            )
        canonical = canonical_bar_feature_json(_parse_json(self.result_json))
        object.__setattr__(self, "result_json", canonical)

    def result(self) -> dict[str, object]:
        return _parse_json(self.result_json)


__all__ = [
    "BAR_FEATURE_REGISTRY_VERSION",
    "BarAbsenceDeclarationV1",
    "BarAvailabilityBasis",
    "BarAvailabilityDeclarationV1",
    "BarFeatureCellV1",
    "BarFeatureConsumerResultV1",
    "BarFeatureDefinitionV1",
    "BarFeaturePolicyV1",
    "BarFeatureSourceV1",
    "BarFeatureState",
    "BarFeatureValueV1",
    "CausalBarSnapshotV1",
    "bar_feature_definitions",
    "canonical_bar_feature_json",
]

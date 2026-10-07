"""Additive preprocessing requests; decoding is never fit/source authority.

The public executable boundary must replay native inputs, derive fit membership
and refit parameters. A content hash, a constructor or an externally supplied
coefficient array cannot establish that protected data was excluded.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
from types import UnionType
from typing import Any, ClassVar, TypeVar, Union, cast, get_args, get_origin
from typing import get_type_hints

from .training_contracts import (
    MAX_TRAINING_BYTES,
    TrainingContract,
    _clock,
    _digest,
    _text,
    training_json,
    training_load,
)
from .training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalSplitV1,
)
from .training_wide_contracts import TrainingWidePlanV1

PREPROCESSING_VERSION = "1.0.0"
MAX_PREPROCESSING_STEPS = 16
MAX_PREPROCESSING_COLUMNS = 64
MAX_PREPROCESSING_OUTPUT_COLUMNS = 256
MAX_PREPROCESSING_ROWS = 512
MAX_PREPROCESSING_CELLS = 32768

PREPROCESSING_FAMILIES = (
    "zscale",
    "robust_scale",
    "minmax",
    "winsorize",
    "quantile_rank",
    "categorical",
    "median_imputer",
    "variance_selector",
    "correlation_pruner",
    "pca",
    "svd",
    "whitening",
    "population_reduction",
    "linear_embedding",
)
STATELESS_DENSE_FAMILIES = ("prior_state", "confirmed_zero")
PREPROCESSING_NONCLAIMS = (
    "constructors_hashes_and_kernel_parameters_are_not_fit_evidence",
    "synthetic_contract_tests_not_empirical_qualification",
    "train_fit_not_historically_available_parameters_at_earlier_train_rows",
    "normalized_added_feature_clocks_not_historical_spine_authenticity",
    "imputation_not_observed_or_recovered_historical_facts",
    "retained_member_mass_not_calibrated_scenario_probability",
    "private_model_fits_and_protected_research_not_executed",
    "not_complete_525_681_691_or_published_corpus_certification",
)


class PreprocessingFitMode(str, Enum):
    TRAIN_FIT = "train_fit"
    ROLLING_PTI = "rolling_pti"
    FIXED_EXTERNAL = "fixed_external"
    NONE = "none"


class PreprocessingViewKind(str, Enum):
    EVIDENCE = "evidence"
    DENSE = "dense_model"


class PreprocessingMissingness(str, Enum):
    OBSERVED = "observed"
    DERIVED = "derived"
    UNSUPPORTED = "unsupported"
    NOT_YET_AVAILABLE = "not_yet_available"
    UNAVAILABLE = "unavailable"
    EMPTY_MARKET_INTERVAL = "empty_market_interval"
    INSUFFICIENT_WARMUP = "insufficient_warmup"
    MARKET_CLOSED = "market_closed"
    SOURCE_OUTAGE = "source_outage"
    NOT_APPLICABLE = "not_applicable"
    REFUSED = "refused"
    UNKNOWN_AVAILABILITY = "unknown_availability"
    STALE = "stale"
    MISSING_SPINE = "missing_spine_anchor"
    CONFIRMED_ABSENCE = "confirmed_absence"


def _names(
    values: tuple[str, ...], *, maximum: int, empty: bool = False
) -> None:
    if not (0 if empty else 1) <= len(values) <= maximum:
        raise ValueError("preprocessing name inventory is empty or unbounded")
    if len(set(values)) != len(values):
        raise ValueError("preprocessing names must be unique")
    for value in values:
        _text(value)


def _canonical(text: str) -> dict[str, object]:
    value = training_load(text)
    if training_json(value) != text:
        raise ValueError("preprocessing embedded object must be canonical")
    return value


def _prospective(
    value: object, hint: object, budget: list[int], depth: int
) -> None:
    """Inspect exact raw fields before cached/native serializers can run."""
    budget[0] -= 1
    if budget[0] < 0 or depth > 16:
        raise ValueError("preprocessing input exceeds traversal bound")
    origin = get_origin(hint)
    if origin in (Union, UnionType):
        hint = next((h for h in get_args(hint) if type(value) is h), None)
        if hint is None:
            raise TypeError("preprocessing optional field has wrong type")
        origin = get_origin(hint)
    if origin is tuple:
        if type(value) is not tuple or len(value) > MAX_PREPROCESSING_CELLS:
            raise TypeError("preprocessing collection must be bounded tuple")
        for item in value:
            _prospective(item, get_args(hint)[0], budget, depth + 1)
    elif isinstance(hint, type) and issubclass(hint, TrainingContract):
        if type(value) is not hint:
            raise TypeError("preprocessing child requires exact native type")
        hints = get_type_hints(hint)
        for field in fields(cast(Any, hint)):
            _prospective(
                getattr(value, field.name), hints[field.name], budget, depth + 1
            )
    elif type(value) is not hint:
        raise TypeError("preprocessing scalar requires exact declared type")
    elif type(value) is str:
        if len(value) > budget[1]:
            raise ValueError("preprocessing text exceeds prospective budget")
        cost = 2
        for char in value:
            code = ord(char)
            cost += (
                2
                if char in '\\"\b\f\n\r\t'
                else (
                    6
                    if code < 32 or 127 <= code <= 65535
                    else 12 if code > 65535 else 1
                )
            )
            if cost > budget[1]:
                raise ValueError(
                    "preprocessing text exceeds prospective budget"
                )
        budget[1] -= cost


class _PreprocessingContract(TrainingContract):
    __slots__ = ()

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        budget = [200_000, MAX_TRAINING_BYTES]
        for field in fields(cast(Any, self)):
            _prospective(
                getattr(self, field.name), hints[field.name], budget, 0
            )
        super().__post_init__()


_C = TypeVar("_C", bound=TrainingContract)


def readmit_preprocessing_contract(value: _C, expected: type[_C]) -> _C:
    """Detach raw fields and rerun constructors, ignoring cached wire and IDs."""
    if type(value) is not expected:
        raise TypeError("preprocessing boundary requires exact subject type")
    _prospective(value, expected, [200_000, MAX_TRAINING_BYTES], 0)

    def fresh(item: Any) -> Any:
        if isinstance(item, TrainingContract):
            return type(item)(
                **{
                    f.name: fresh(getattr(item, f.name))
                    for f in fields(cast(Any, item))
                }
            )
        if type(item) is tuple:
            return tuple(fresh(child) for child in item)
        return item

    return cast(_C, fresh(value))


@dataclass(frozen=True, slots=True)
class TrainingPreprocessingStepV1(_PreprocessingContract):
    KIND: ClassVar[str] = "preprocessing-step"
    name: str
    family: str
    columns: tuple[str, ...]
    options_json: str = "{}"
    version: str = PREPROCESSING_VERSION

    def _validate(self) -> None:
        _text(self.name)
        if self.version != PREPROCESSING_VERSION:
            raise ValueError("unsupported preprocessing step version")
        if self.family not in PREPROCESSING_FAMILIES + STATELESS_DENSE_FAMILIES:
            raise ValueError("unknown preprocessing family")
        _names(self.columns, maximum=MAX_PREPROCESSING_COLUMNS)
        if len(self.options_json) > 65536:
            raise ValueError("preprocessing options exceed 64 KiB")
        options = _canonical(self.options_json)
        if self.family == "confirmed_zero" and options:
            raise ValueError("confirmed absence zero has no fitted options")
        if self.family == "prior_state":
            if set(options) != {"max_age_ns"}:
                raise ValueError("prior state requires exact staleness policy")
            _clock(cast(int, options["max_age_ns"]))


@dataclass(frozen=True, slots=True)
class TrainingPreprocessingPlanV1(_PreprocessingContract):
    KIND: ClassVar[str] = "preprocessing-plan"
    wide_plan: TrainingWidePlanV1
    split: TrainingTemporalSplitV1
    fold_id: str
    fit_mode: PreprocessingFitMode
    fit_unit_ids: tuple[str, ...]
    columns: tuple[str, ...]
    steps: tuple[TrainingPreprocessingStepV1, ...]
    rolling_window_ns: int | None = None
    version: str = PREPROCESSING_VERSION

    def _validate(self) -> None:
        if self.version != PREPROCESSING_VERSION:
            raise ValueError("unsupported preprocessing plan version")
        _text(self.fold_id)
        _names(self.columns, maximum=MAX_PREPROCESSING_COLUMNS)
        if tuple(sorted(self.columns)) != self.columns:
            raise ValueError("preprocessing input columns must be canonical")
        _names(self.fit_unit_ids, maximum=4096, empty=True)
        if self.fit_unit_ids != tuple(sorted(self.fit_unit_ids)):
            raise ValueError("fit units must be sorted")
        if len(self.steps) > MAX_PREPROCESSING_STEPS:
            raise ValueError("preprocessing pipeline exceeds step bound")
        _names(tuple(s.name for s in self.steps), maximum=16, empty=True)
        native = self.wide_plan.tiles[0].join_plan.spine
        ownership = native.ownership
        assignments = self.split.assignments
        if self.split.ownership_id != ownership.artifact_id or tuple(
            a.evidence_unit_id for a in assignments
        ) != tuple(u.artifact_id for u in ownership.units):
            raise ValueError("preprocessing split must cover native ownership")
        ranks = [
            list(TemporalPartition).index(a.partition) for a in assignments
        ]
        if ranks != sorted(ranks):
            raise ValueError("preprocessing split must remain chronological")
        train = {
            a.evidence_unit_id
            for a in assignments
            if a.partition is TemporalPartition.TRAIN
        }
        if not set(self.fit_unit_ids) <= train:
            raise ValueError(
                "fit membership includes protected or unknown units"
            )
        if self.fit_mode is PreprocessingFitMode.NONE:
            if self.fit_unit_ids or any(
                s.family not in STATELESS_DENSE_FAMILIES for s in self.steps
            ):
                raise ValueError("none mode cannot conceal learned fit state")
        elif not self.fit_unit_ids or not self.steps:
            raise ValueError(
                "fitted preprocessing requires training units and steps"
            )
        if self.fit_mode is PreprocessingFitMode.ROLLING_PTI:
            if self.rolling_window_ns is None:
                raise ValueError("rolling preprocessing needs frozen window")
            _clock(self.rolling_window_ns)
            if self.rolling_window_ns == 0:
                raise ValueError("rolling window must be positive")
        elif self.rolling_window_ns is not None:
            raise ValueError("nonrolling mode cannot hide rolling parameters")
        registry = {c.column.name for c in self.wide_plan.columns}
        allowed = registry | {"state." + name for name in registry}
        if not set(self.columns) <= allowed:
            raise ValueError(
                "preprocessing input is outside native feature registry"
            )


@dataclass(frozen=True, slots=True)
class TrainingPreprocessingFitStepV1(_PreprocessingContract):
    KIND: ClassVar[str] = "preprocessing-fit-step"
    step_id: str
    kernel_fit_json: str
    input_schema_sha256: str
    output_schema_sha256: str
    parent_lineage_ids: tuple[str, ...]

    def _validate(self) -> None:
        _text(self.step_id)
        _canonical(self.kernel_fit_json)
        _digest(self.input_schema_sha256)
        _digest(self.output_schema_sha256)
        _names(self.parent_lineage_ids, maximum=4096)


@dataclass(frozen=True, slots=True)
class TrainingPreprocessingFitV1(_PreprocessingContract):
    KIND: ClassVar[str] = "preprocessing-fit"
    plan: TrainingPreprocessingPlanV1
    cutoff_ns: int | None
    parameter_identity: str
    membership_json: str
    support_json: str
    steps: tuple[TrainingPreprocessingFitStepV1, ...]
    nonclaims: tuple[str, ...] = PREPROCESSING_NONCLAIMS

    def _validate(self) -> None:
        _text(self.parameter_identity)
        _canonical(self.membership_json)
        _canonical(self.support_json)
        if self.cutoff_ns is not None:
            _clock(self.cutoff_ns)
        if (
            self.plan.fit_mode is PreprocessingFitMode.ROLLING_PTI
            and self.cutoff_ns is None
        ):
            raise ValueError("rolling fit requires its exact cutoff")
        if tuple(s.step_id for s in self.steps) != tuple(
            s.artifact_id for s in self.plan.steps
        ):
            raise ValueError("fit must retain every pipeline step in order")
        if self.nonclaims != PREPROCESSING_NONCLAIMS:
            raise ValueError(
                "preprocessing fit cannot change its claim boundary"
            )

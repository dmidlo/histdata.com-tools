"""Additive scenario-preserving training wire; constructors are not authority.

Native materialization must derive axes, inventory and roots from current
replayed sources. These contracts never qualify a caller's scalar arrays or
turn a scenario label into native provenance. Existing training v1 is unchanged.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, fields
from enum import Enum
from fractions import Fraction
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

from .training_contracts import (
    MAX_TRAINING_BYTES,
    TrainingConsumerMode,
    TrainingContract,
    TrainingOrigin,
    TrainingOwnershipV1,
    TrainingRootV1,
    TrainingRowV1,
    TrainingSourceV1,
    _clock,
    _digest,
    _roots,
    _text,
    training_json,
    training_load,
)
from .training_scenario_math import (
    MAX_SCENARIO_CELLS,
    MAX_SCENARIO_COORDINATES,
    MAX_SCENARIO_MEMBERS,
    MAX_SCENARIO_QUANTILES,
    MAX_SCENARIO_RATIONAL_BITS,
    checked_fraction,
    scenario_scalar_moments,
)

SCENARIO_VERSION = "1.0.0"
SCENARIO_WEIGHT_POLICY = "equal-complete-members.v1"
SCENARIO_QUANTILE_POLICY = "left-continuous-weighted-inverse-cdf.v1"
SCENARIO_NONCLAIMS = (
    "native_replay_not_historical_causal_availability",
    "equal_member_weights_not_empirically_calibrated_uncertainty",
    "equal_path_member_mixture_not_scientific_scenario_probabilities",
    "research_recipe_not_promoted_or_empirically_qualified_campaign",
    "joint_roster_sampling_not_contextual_posterior_or_calibration",
    "scenario_panels_not_independent_history_or_population_evidence",
    "exact_scalar_summaries_not_aligned_or_averaged_event_paths",
    "constructors_and_arithmetic_are_not_source_verification",
)
SCENARIO_FEATURES = (
    "ask_max",
    "ask_mean",
    "ask_min",
    "bid_max",
    "bid_mean",
    "bid_min",
    "event_count",
    "observed_count",
    "spread_mean",
    "synthetic_count",
)


class ScenarioViewKind(str, Enum):
    OBSERVED_ONLY = "observed_only"
    CENTRAL_COUNTERFACTUAL = "central_counterfactual"
    MEMBER_PANEL = "member_panel"
    SAMPLE_ONE_MEMBER = "sample_one_member_per_evidence_unit"
    MARGINALIZED_FEATURES = "marginalized_features"
    UNCERTAINTY_FEATURES = "uncertainty_features"


class ScenarioAxis(str, Enum):
    OBSERVATION_RETENTION = "observation_retention"
    TRANSITION = "transition"
    PATH_REALIZATION = "path_realization"
    ENGINE = "engine"
    DELIVERY_PROFILE = "delivery_profile"
    BROKER = "broker"
    POPULATION = "population"


SCENARIO_AXES = tuple(ScenarioAxis)


class ScenarioAxisState(str, Enum):
    KNOWN = "known"
    NOT_APPLICABLE = "not_applicable"
    UNAVAILABLE = "unavailable"


class ScenarioMemberStatus(str, Enum):
    ELIGIBLE = "eligible"
    EMPTY = "empty"
    REFUSED = "refused"
    UNAVAILABLE = "unavailable"


def _unique(values: tuple[str, ...], *, nonempty: bool = False) -> None:
    if nonempty and not values:
        raise ValueError("scenario identity inventory cannot be empty")
    for value in values:
        _text(value)
    if values != tuple(sorted(set(values))):
        raise ValueError("scenario identities must be sorted and unique")


def _interval(start_ns: int, end_ns: int) -> None:
    _clock(start_ns)
    _clock(end_ns)
    if start_ns >= end_ns:
        raise ValueError("scenario interval must be nonempty and half-open")


def _axis_order(values: tuple[ScenarioAxis, ...]) -> None:
    if values != tuple(axis for axis in SCENARIO_AXES if axis in values):
        raise ValueError(
            "scenario axes require the unique closed catalog order"
        )


def _ascii_string_bytes(value: str, maximum: int) -> int:
    """Count JSON ensure_ascii bytes without allocating the escaped string."""
    # Even unescaped content requires one byte per code point and two quotes.
    if maximum < 2 or len(value) > maximum - 2:
        raise ValueError("scenario text exceeds prospective byte budget")
    size = 2
    for character in value:
        code = ord(character)
        if character in '\\"\b\f\n\r\t':
            size += 2
        elif code < 32 or code >= 127:
            # Lone surrogates remain one \\uXXXX escape; non-BMP characters
            # require the two UTF-16 surrogate escapes used by ensure_ascii.
            size += 6 if code <= 65535 else 12
        else:
            size += 1
        if size > maximum:
            raise ValueError("scenario text exceeds prospective byte budget")
    return size


def _exact(
    value: object, hint: object, budget: list[int], depth: int = 0
) -> None:
    """Reject subclass serializers and expanded payloads before shared wiring."""
    budget[0] -= 1
    if depth > 16 or budget[0] < 0:
        raise ValueError("scenario input exceeds traversal budget")
    origin = get_origin(hint)
    if origin in (Union, UnionType):
        candidates = get_args(hint)
        hint = next((item for item in candidates if type(value) is item), None)
        if hint is None:
            raise TypeError("scenario optional field requires its exact type")
    if origin is tuple:
        if type(value) is not tuple or len(value) > MAX_SCENARIO_CELLS:
            raise TypeError(
                "scenario collection requires a bounded exact tuple"
            )
        for item in value:
            _exact(item, get_args(hint)[0], budget, depth + 1)
    elif isinstance(hint, type) and issubclass(hint, TrainingContract):
        if type(value) is not hint:
            raise TypeError("scenario nested contracts require exact types")
        hints = get_type_hints(hint)
        for field in fields(cast(Any, hint)):
            _exact(
                getattr(value, field.name), hints[field.name], budget, depth + 1
            )
    elif type(value) is not hint:
        raise TypeError("scenario field requires its exact declared type")
    elif type(value) is str:
        budget[1] -= _ascii_string_bytes(value, budget[1])


class _ScenarioContract(TrainingContract):
    __slots__ = ()

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        budget = [200_000, MAX_TRAINING_BYTES]
        for field in fields(cast(Any, self)):
            _exact(getattr(self, field.name), hints[field.name], budget)
        super().__post_init__()


@dataclass(frozen=True, slots=True)
class TrainingScenarioRationalV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-rational"
    numerator: str
    denominator: str

    def _validate(self) -> None:
        limit = MAX_SCENARIO_RATIONAL_BITS // 3 + 2
        if (
            len(self.numerator) > limit
            or len(self.denominator) > limit
            or re.fullmatch(r"0|-?[1-9][0-9]*", self.numerator) is None
            or re.fullmatch(r"[1-9][0-9]*", self.denominator) is None
        ):
            raise ValueError(
                "scenario rational requires bounded canonical integers"
            )
        value = checked_fraction(
            Fraction(int(self.numerator), int(self.denominator))
        )
        if (
            str(value.numerator) != self.numerator
            or str(value.denominator) != self.denominator
        ):
            raise ValueError("scenario rational must be reduced")

    @property
    def value(self) -> Fraction:
        return Fraction(int(self.numerator), int(self.denominator))

    @classmethod
    def from_fraction(cls, value: Fraction) -> TrainingScenarioRationalV1:
        checked_fraction(value)
        return cls(str(value.numerator), str(value.denominator))


@dataclass(frozen=True, slots=True)
class TrainingScenarioAxisV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-axis"
    axis: ScenarioAxis
    state: ScenarioAxisState
    value_id: str | None
    evidence_ids: tuple[str, ...]

    def _validate(self) -> None:
        _unique(self.evidence_ids, nonempty=True)
        if self.state is ScenarioAxisState.KNOWN:
            if self.value_id is None:
                raise ValueError("known scenario axis requires a native value")
            _text(self.value_id)
        elif self.value_id is not None:
            raise ValueError("non-known scenario axis cannot fabricate a value")


@dataclass(frozen=True, slots=True)
class TrainingScenarioPolicyV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-policy"
    features: tuple[str, ...] = SCENARIO_FEATURES
    collapse_axes: tuple[ScenarioAxis, ...] = (ScenarioAxis.PATH_REALIZATION,)
    central_member_keys: tuple[str, ...] = ()
    quantiles: tuple[TrainingScenarioRationalV1, ...] = ()
    weight_policy: str = SCENARIO_WEIGHT_POLICY
    quantile_policy: str = SCENARIO_QUANTILE_POLICY
    sampling_seed: str = "0"
    contract_version: str = SCENARIO_VERSION

    def _validate(self) -> None:
        _unique(self.features, nonempty=True)
        if not set(self.features) <= set(SCENARIO_FEATURES):
            raise ValueError(
                "unsupported scalar feature; paths/categories/times are panels"
            )
        _axis_order(self.collapse_axes)
        if any(
            axis is not ScenarioAxis.PATH_REALIZATION
            for axis in self.collapse_axes
        ):
            raise ValueError(
                "v1 collapses only path realizations; context axes stay separate"
            )
        _unique(self.central_member_keys)
        if len(self.central_member_keys) > MAX_SCENARIO_CELLS:
            raise ValueError("central scenario selection exceeds bound")
        if len(self.quantiles) > MAX_SCENARIO_QUANTILES:
            raise ValueError("too many scenario quantiles")
        probabilities = tuple(q.value for q in self.quantiles)
        if probabilities != tuple(sorted(set(probabilities))) or any(
            not 0 <= q <= 1 for q in probabilities
        ):
            raise ValueError(
                "scenario quantiles must be ordered unique probabilities"
            )
        if (
            self.weight_policy != SCENARIO_WEIGHT_POLICY
            or self.quantile_policy != SCENARIO_QUANTILE_POLICY
            or self.contract_version != SCENARIO_VERSION
        ):
            raise ValueError(
                "unsupported scenario policy; calibrated weights are not admitted"
            )
        _text(self.sampling_seed)


@dataclass(frozen=True, slots=True)
class TrainingScenarioCampaignBindingV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-campaign-binding"
    index_path: str
    index_id: str
    index_sha256: str

    def _validate(self) -> None:
        _text(self.index_path)
        _text(self.index_id)
        _digest(self.index_sha256)


@dataclass(frozen=True, slots=True)
class TrainingScenarioResearchBindingV1(_ScenarioContract):
    """Closed native research adapter locator, never a promotion claim."""

    KIND: ClassVar[str] = "scenario-research-binding"
    recipe_json: str
    expected_recipe_id: str

    def _validate(self) -> None:
        if training_json(training_load(self.recipe_json)) != self.recipe_json:
            raise ValueError("research recipe must be canonical JSON")
        if (
            re.fullmatch(
                r"training-scenario-research-recipe:sha256:[0-9a-f]{64}",
                self.expected_recipe_id,
            )
            is None
        ):
            raise ValueError(
                "research binding requires an exact retained recipe identity"
            )


@dataclass(frozen=True, slots=True)
class TrainingScenarioPlanV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-plan"
    source: TrainingSourceV1
    ownership: TrainingOwnershipV1
    policy: TrainingScenarioPolicyV1
    campaign_binding: TrainingScenarioCampaignBindingV1 | None = None
    research_binding: TrainingScenarioResearchBindingV1 | None = None

    def _validate(self) -> None:
        if self.source.dataset_version_id != self.ownership.dataset_version_id:
            raise ValueError("scenario source and ownership differ")
        if (
            self.campaign_binding is not None
            and self.research_binding is not None
        ):
            raise ValueError(
                "scenario campaign/research authority routes are exclusive"
            )


@dataclass(frozen=True, slots=True)
class TrainingScenarioRequestV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-request"
    view: ScenarioViewKind
    consumer_mode: TrainingConsumerMode
    start_ns: int
    end_ns: int
    symbols: tuple[str, ...]
    epoch: int = 0

    def _validate(self) -> None:
        _interval(self.start_ns, self.end_ns)
        _clock(self.epoch)
        _unique(self.symbols, nonempty=True)
        if any(
            re.fullmatch(r"[A-Z]{6}", symbol) is None for symbol in self.symbols
        ):
            raise ValueError("scenario symbols require canonical FX pairs")
        if self.consumer_mode is TrainingConsumerMode.CAUSAL:
            raise ValueError(
                "scenario v1 does not establish historical causal availability"
            )
        if self.view is not ScenarioViewKind.SAMPLE_ONE_MEMBER and self.epoch:
            raise ValueError(
                "sampling epoch applies only to the sampled-member view"
            )


@dataclass(frozen=True, slots=True)
class TrainingScenarioCoordinateV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-coordinate"
    evidence_unit_id: str
    symbol: str
    start_ns: int
    end_ns: int
    feature: str

    def _validate(self) -> None:
        _text(self.evidence_unit_id)
        _interval(self.start_ns, self.end_ns)
        if (
            re.fullmatch(r"[A-Z]{6}", self.symbol) is None
            or self.feature not in SCENARIO_FEATURES
        ):
            raise ValueError("unsupported scenario scalar coordinate")

    @property
    def coordinate_id(self) -> str:
        return self.artifact_id


@dataclass(frozen=True, slots=True)
class TrainingScenarioMemberV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-member"
    evidence_unit_id: str
    ensemble_member_id: str | None
    start_ns: int
    end_ns: int
    generator_config_ids: tuple[str, ...]
    run_ids: tuple[str, ...]
    product_manifest_ids: tuple[str, ...]
    axes: tuple[TrainingScenarioAxisV1, ...]
    status: ScenarioMemberStatus
    reason_codes: tuple[str, ...]
    native_evidence_ids: tuple[str, ...]

    def _validate(self) -> None:
        _text(self.evidence_unit_id)
        if self.ensemble_member_id is not None:
            _text(self.ensemble_member_id)
        _interval(self.start_ns, self.end_ns)
        for values in (
            self.generator_config_ids,
            self.run_ids,
            self.product_manifest_ids,
            self.reason_codes,
        ):
            _unique(values)
        _unique(self.native_evidence_ids, nonempty=True)
        if tuple(a.axis for a in self.axes) != SCENARIO_AXES:
            raise ValueError(
                "member must retain every scenario axis in catalog order"
            )
        if (
            self.status
            in (ScenarioMemberStatus.REFUSED, ScenarioMemberStatus.UNAVAILABLE)
            and not self.reason_codes
        ):
            raise ValueError(
                "refused/unavailable member requires explicit reasons"
            )
        if (
            self.status is ScenarioMemberStatus.ELIGIBLE
            and not self.product_manifest_ids
        ):
            raise ValueError(
                "eligible scenario member requires native product identities"
            )

    @property
    def member_key(self) -> str:
        """Semantic sampling key excludes paths, request, epoch and roster."""
        payload = {
            "schema_version": "histdatacom.training-scenario-member-key.v1",
            "evidence_unit_id": self.evidence_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "generator_config_ids": list(self.generator_config_ids),
            "axes": [
                [a.axis.value, a.state.value, a.value_id] for a in self.axes
            ],
        }
        return (
            "training-scenario-member:sha256:"
            + hashlib.sha256(training_json(payload).encode("ascii")).hexdigest()
        )


@dataclass(frozen=True, slots=True)
class TrainingScenarioPanelRowV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-panel-row"
    member_key: str | None
    row: TrainingRowV1

    def _validate(self) -> None:
        if self.member_key is not None:
            _text(self.member_key)


@dataclass(frozen=True, slots=True)
class TrainingScenarioScalarV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-scalar"
    coordinate: TrainingScenarioCoordinateV1
    member_key: str
    value: TrainingScenarioRationalV1 | None
    status: ScenarioMemberStatus
    reason_codes: tuple[str, ...] = ()

    def _validate(self) -> None:
        _text(self.member_key)
        _unique(self.reason_codes)
        if self.status is ScenarioMemberStatus.ELIGIBLE:
            if self.value is None:
                raise ValueError("eligible scalar requires its exact value")
        elif self.status is ScenarioMemberStatus.EMPTY:
            if self.coordinate.feature.endswith("_count"):
                if self.value is None or self.value.value != 0:
                    raise ValueError("admitted empty count must be exact zero")
            elif self.value is not None:
                raise ValueError("empty quote coordinate has no scalar value")
        elif self.value is not None or not self.reason_codes:
            raise ValueError(
                "refused/unavailable scalar has no value and needs reasons"
            )
        if (
            self.value is not None
            and self.coordinate.feature.endswith("_count")
            and (self.value.value < 0 or self.value.value.denominator != 1)
        ):
            raise ValueError("scenario counts must be nonnegative integers")


@dataclass(frozen=True, slots=True)
class TrainingScenarioDenominatorV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-denominator"
    total: int
    eligible: int
    empty: int
    refused: int
    unavailable: int

    def _validate(self) -> None:
        values = (
            self.total,
            self.eligible,
            self.empty,
            self.refused,
            self.unavailable,
        )
        if any(
            type(value) is not int or not 0 <= value <= MAX_SCENARIO_CELLS
            for value in values
        ):
            raise ValueError("scenario denominator exceeds bounded inventory")
        if self.total != sum(values[1:]):
            raise ValueError(
                "scenario denominator does not conserve complete membership"
            )


@dataclass(frozen=True, slots=True)
class TrainingScenarioWeightV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-weight"
    member_key: str
    weight: TrainingScenarioRationalV1

    def _validate(self) -> None:
        _text(self.member_key)
        if not 0 <= self.weight.value <= 1:
            raise ValueError("scenario weight is outside [0,1]")


@dataclass(frozen=True, slots=True)
class TrainingScenarioQuantileV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-quantile"
    probability: TrainingScenarioRationalV1
    value: TrainingScenarioRationalV1

    def _validate(self) -> None:
        if not 0 <= self.probability.value <= 1:
            raise ValueError("scenario quantile probability is outside [0,1]")


@dataclass(frozen=True, slots=True)
class TrainingScenarioSummaryV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-summary"
    coordinate: TrainingScenarioCoordinateV1
    preserved_axes: tuple[TrainingScenarioAxisV1, ...]
    member_keys: tuple[str, ...]
    denominator: TrainingScenarioDenominatorV1
    weights: tuple[TrainingScenarioWeightV1, ...]
    mean: TrainingScenarioRationalV1 | None
    population_variance: TrainingScenarioRationalV1 | None
    quantiles: tuple[TrainingScenarioQuantileV1, ...]
    reason_codes: tuple[str, ...] = ()
    generator_config_ids: tuple[str, ...] = ()

    def _validate(self) -> None:
        _axis_order(tuple(axis.axis for axis in self.preserved_axes))
        _unique(self.member_keys, nonempty=True)
        _unique(self.reason_codes)
        _unique(self.generator_config_ids)
        if len(
            self.member_keys
        ) > MAX_SCENARIO_MEMBERS or self.denominator.total != len(
            self.member_keys
        ):
            raise ValueError(
                "summary denominator differs from complete member roster"
            )
        if (self.mean is None) != (self.population_variance is None):
            raise ValueError(
                "scenario mean and variance must share availability"
            )
        if self.mean is None:
            if self.weights or self.quantiles or not self.reason_codes:
                raise ValueError(
                    "unavailable summary cannot carry numerical authority"
                )
        else:
            if self.denominator.refused or self.denominator.unavailable:
                raise ValueError(
                    "summary cannot renormalize away refused/missing members"
                )
            if tuple(
                w.member_key for w in self.weights
            ) != self.member_keys or any(
                w.weight.value != Fraction(1, len(self.member_keys))
                for w in self.weights
            ):
                raise ValueError(
                    "summary requires exact equal complete-member weights"
                )
            assert self.population_variance is not None
            if self.population_variance.value < 0:
                raise ValueError(
                    "scenario population variance cannot be negative"
                )
            probabilities = tuple(q.probability.value for q in self.quantiles)
            if len(
                probabilities
            ) > MAX_SCENARIO_QUANTILES or probabilities != tuple(
                sorted(set(probabilities))
            ):
                raise ValueError(
                    "summary quantiles must be bounded and ordered unique"
                )


@dataclass(frozen=True, slots=True)
class TrainingScenarioViewV1(_ScenarioContract):
    KIND: ClassVar[str] = "scenario-view"
    plan: TrainingScenarioPlanV1
    request: TrainingScenarioRequestV1
    members: tuple[TrainingScenarioMemberV1, ...]
    rows: tuple[TrainingScenarioPanelRowV1, ...]
    scalars: tuple[TrainingScenarioScalarV1, ...]
    summaries: tuple[TrainingScenarioSummaryV1, ...]
    denominator: TrainingScenarioDenominatorV1
    collapsed_axes: tuple[ScenarioAxis, ...]
    preserved_axes: tuple[ScenarioAxis, ...]
    verification_roots: tuple[TrainingRootV1, ...]
    selected_member_keys: tuple[str, ...] = ()
    nonclaims: tuple[str, ...] = SCENARIO_NONCLAIMS

    def _validate(self) -> None:
        if self.nonclaims != SCENARIO_NONCLAIMS:
            raise ValueError("scenario view must retain exact nonclaims")
        _roots(self.verification_roots)
        if not self.verification_roots:
            raise ValueError(
                "scenario view requires native/source verification roots"
            )
        _axis_order(self.collapsed_axes)
        _axis_order(self.preserved_axes)
        if set(self.collapsed_axes) & set(self.preserved_axes) or set(
            self.collapsed_axes
        ) | set(self.preserved_axes) != set(SCENARIO_AXES):
            raise ValueError(
                "collapsed/preserved axes must partition the full catalog"
            )
        if (
            len(self.members) > MAX_SCENARIO_CELLS
            or len(self.scalars) > MAX_SCENARIO_CELLS
            or len(self.summaries) > MAX_SCENARIO_COORDINATES
        ):
            raise ValueError("scenario expanded result exceeds bounds")
        keys = tuple(member.member_key for member in self.members)
        _unique(keys)
        if self.denominator.total != len(keys):
            raise ValueError(
                "view denominator differs from complete member inventory"
            )
        expected = tuple(
            sum(member.status is status for member in self.members)
            for status in ScenarioMemberStatus
        )
        if expected != (
            self.denominator.eligible,
            self.denominator.empty,
            self.denominator.refused,
            self.denominator.unavailable,
        ):
            raise ValueError("view denominator status counts differ")
        units = {unit.artifact_id: unit for unit in self.plan.ownership.units}
        members = {member.member_key: member for member in self.members}
        _unique(self.selected_member_keys)
        if not set(self.selected_member_keys) <= set(members):
            raise ValueError(
                "selected scenario member is outside complete roster"
            )
        if self.request.view is ScenarioViewKind.OBSERVED_ONLY:
            if self.selected_member_keys:
                raise ValueError(
                    "observed-only view cannot select scenario members"
                )
        elif self.request.view in (
            ScenarioViewKind.CENTRAL_COUNTERFACTUAL,
            ScenarioViewKind.SAMPLE_ONE_MEMBER,
        ):
            selected_units = tuple(
                members[key].evidence_unit_id
                for key in self.selected_member_keys
            )
            required_units = {
                member.evidence_unit_id for member in self.members
            }
            if (
                not required_units
                or len(selected_units) != len(set(selected_units))
                or set(selected_units) != required_units
            ):
                raise ValueError(
                    "central/sampled view requires exactly one member per complete native unit"
                )
            if (
                self.request.view is ScenarioViewKind.CENTRAL_COUNTERFACTUAL
                and self.selected_member_keys
                != self.plan.policy.central_member_keys
            ):
                raise ValueError(
                    "central selected members differ from the frozen selectors"
                )
        elif self.selected_member_keys != keys:
            raise ValueError(
                "panel/scalar view must select the complete native roster"
            )
        aggregates = self.request.view in (
            ScenarioViewKind.MARGINALIZED_FEATURES,
            ScenarioViewKind.UNCERTAINTY_FEATURES,
        )
        expected_collapsed = (
            self.plan.policy.collapse_axes if aggregates else ()
        )
        if self.collapsed_axes != expected_collapsed:
            raise ValueError(
                "view collapsed axes differ from frozen view semantics"
            )
        if not aggregates and self.summaries:
            raise ValueError(
                "panel/selection view cannot masquerade as a scalar summary"
            )
        if aggregates and self.rows:
            raise ValueError(
                "scalar summary view cannot mix unlabelled panel rows"
            )
        if self.request.view is ScenarioViewKind.OBSERVED_ONLY and self.scalars:
            raise ValueError("observed-only view cannot carry scenario scalars")
        if (
            self.request.start_ns < self.plan.ownership.units[0].start_ns
            or self.request.end_ns > self.plan.ownership.units[-1].end_ns
        ):
            raise ValueError("scenario request escapes evidence ownership")
        if not set(self.request.symbols) <= set(
            self.plan.ownership.units[0].graph_symbols
        ):
            raise ValueError("scenario request escapes complete evidence graph")
        for unit_id in units:
            if (
                sum(m.evidence_unit_id == unit_id for m in self.members)
                > MAX_SCENARIO_MEMBERS
            ):
                raise ValueError(
                    "scenario member roster exceeds per-unit bound"
                )
        for member in self.members:
            unit = units.get(member.evidence_unit_id)
            if (
                unit is None
                or member.start_ns < unit.start_ns
                or member.end_ns > unit.end_ns
            ):
                raise ValueError(
                    "scenario member escapes exact evidence ownership"
                )
        row_keys: set[tuple[str | None, str]] = set()
        for wrapped in self.rows:
            row = wrapped.row
            if (
                row.ownership_id != self.plan.ownership.artifact_id
                or row.observed_dataset_version_id
                != self.plan.source.dataset_version_id
            ):
                raise ValueError(
                    "scenario panel row has foreign source/ownership"
                )
            if (
                not self.request.start_ns
                <= row.event_time_ns
                < self.request.end_ns
                or row.symbol not in self.request.symbols
                or self.request.consumer_mode not in row.admissible_modes
            ):
                raise ValueError(
                    "scenario panel row escapes requested support/consumer"
                )
            unit = units.get(row.evidence_unit_id)
            if (
                unit is None
                or not unit.start_ns <= row.event_time_ns < unit.end_ns
            ):
                raise ValueError("scenario panel row escapes evidence unit")
            row_key = (wrapped.member_key, row.source_row_key)
            if row_key in row_keys:
                raise ValueError("duplicate scenario panel source row")
            row_keys.add(row_key)
            if wrapped.member_key is not None:
                if wrapped.member_key not in self.selected_member_keys:
                    raise ValueError(
                        "scenario panel row belongs to an unselected member"
                    )
                row_member = members.get(wrapped.member_key)
                if (
                    row_member is None
                    or row.evidence_unit_id != row_member.evidence_unit_id
                    or row.ensemble_member_id != row_member.ensemble_member_id
                ):
                    raise ValueError(
                        "scenario panel row has foreign member identity"
                    )
                if row_member.status is not ScenarioMemberStatus.ELIGIBLE:
                    raise ValueError(
                        "non-eligible scenario member cannot emit rows"
                    )
            elif row.origin is not TrainingOrigin.OBSERVED:
                raise ValueError("unbound scenario panel row must be observed")
            elif self.request.view is not ScenarioViewKind.OBSERVED_ONLY:
                raise ValueError(
                    "counterfactual panel cannot silently mix unbound observed rows"
                )
        coordinates: set[str] = set()
        scalar_keys: set[tuple[str, str]] = set()
        scalar_map: dict[tuple[str, str], TrainingScenarioScalarV1] = {}
        for scalar in self.scalars:
            scalar_member = members.get(scalar.member_key)
            coordinate = scalar.coordinate
            if (
                scalar_member is None
                or scalar_member.evidence_unit_id != coordinate.evidence_unit_id
            ):
                raise ValueError("scenario scalar escapes member support")
            outside = (
                coordinate.start_ns < scalar_member.start_ns
                or coordinate.end_ns > scalar_member.end_ns
            )
            if outside and not (
                scalar.status is ScenarioMemberStatus.UNAVAILABLE
                and scalar.value is None
                and "incomplete_member_support" in scalar.reason_codes
            ):
                raise ValueError(
                    "incomplete scenario support cannot yield a value"
                )
            unit = units[coordinate.evidence_unit_id]
            if (coordinate.start_ns, coordinate.end_ns) != (
                max(unit.start_ns, self.request.start_ns),
                min(unit.end_ns, self.request.end_ns),
            ):
                raise ValueError(
                    "scenario scalar must use the fixed common half-open coordinate"
                )
            if (
                coordinate.symbol not in self.request.symbols
                or coordinate.feature not in self.plan.policy.features
                or coordinate.start_ns < self.request.start_ns
                or coordinate.end_ns > self.request.end_ns
            ):
                raise ValueError(
                    "scenario scalar escapes declared coordinate policy"
                )
            scalar_key = (coordinate.artifact_id, scalar.member_key)
            if scalar_key in scalar_keys:
                raise ValueError("duplicate scenario scalar coordinate/member")
            scalar_keys.add(scalar_key)
            scalar_map[scalar_key] = scalar
            coordinates.add(coordinate.artifact_id)
        if len(coordinates) > MAX_SCENARIO_COORDINATES:
            raise ValueError(
                "scenario scalar coordinate inventory exceeds bound"
            )
        summary_keys: set[
            tuple[str, tuple[tuple[str, str, str | None], ...], tuple[str, ...]]
        ] = set()
        for summary in self.summaries:
            if (
                tuple(axis.axis for axis in summary.preserved_axes)
                != self.preserved_axes
            ):
                raise ValueError("summary omits preserved uncertainty axes")
            for key in summary.member_keys:
                if (summary.coordinate.artifact_id, key) not in scalar_keys:
                    raise ValueError(
                        "summary references absent native scalar membership"
                    )
            signature = tuple(
                (a.axis.value, a.state.value, a.value_id)
                for a in summary.preserved_axes
            )
            group_key = (
                summary.coordinate.artifact_id,
                signature,
                summary.generator_config_ids,
            )
            if group_key in summary_keys:
                raise ValueError("duplicate scenario summary group")
            summary_keys.add(group_key)
            expected_members = tuple(
                sorted(
                    m.member_key
                    for m in self.members
                    if m.evidence_unit_id == summary.coordinate.evidence_unit_id
                    and m.generator_config_ids == summary.generator_config_ids
                    and tuple(
                        (a.axis.value, a.state.value, a.value_id)
                        for a in m.axes
                        if a.axis in self.preserved_axes
                    )
                    == signature
                )
            )
            if summary.member_keys != expected_members:
                raise ValueError(
                    "summary omits complete preserved-axis membership"
                )
            cells = tuple(
                scalar_map[(summary.coordinate.artifact_id, key)]
                for key in summary.member_keys
            )
            counts = tuple(
                sum(cell.status is status for cell in cells)
                for status in ScenarioMemberStatus
            )
            if counts != (
                summary.denominator.eligible,
                summary.denominator.empty,
                summary.denominator.refused,
                summary.denominator.unavailable,
            ):
                raise ValueError(
                    "summary status denominator differs from retained scalars"
                )
            if summary.mean is not None:
                if any(
                    a.state is not ScenarioAxisState.KNOWN
                    for key in summary.member_keys
                    for a in members[key].axes
                    if a.axis in self.collapsed_axes
                ):
                    raise ValueError(
                        "unknown scenario axis cannot be numerically collapsed"
                    )
                if any(cell.value is None for cell in cells):
                    raise ValueError(
                        "summary cannot drop unsupported scalar values"
                    )
                values = tuple(
                    cell.value.value for cell in cells if cell.value is not None
                )
                result = scenario_scalar_moments(
                    values,
                    tuple(Fraction(1) for _ in cells),
                    tuple(q.value for q in self.plan.policy.quantiles),
                )
                if (
                    summary.population_variance is None
                    or summary.mean.value != result.mean
                    or summary.population_variance.value
                    != result.population_variance
                    or tuple(q.value.value for q in summary.quantiles)
                    != result.quantile_values
                    or tuple(q.probability for q in summary.quantiles)
                    != self.plan.policy.quantiles
                ):
                    raise ValueError(
                        "summary arithmetic differs from retained native scalars"
                    )


_T = TypeVar("_T", bound=TrainingContract)


def readmit_scenario(value: _T, expected: type[_T]) -> _T:
    """Exact detached constructor admission; never substitutes for replay."""
    if type(value) is not expected:
        raise TypeError("scenario API requires an exact native contract type")
    budget = [200_000, MAX_TRAINING_BYTES]
    _exact(value, expected, budget)
    return expected.from_dict(TrainingContract.to_dict(value))

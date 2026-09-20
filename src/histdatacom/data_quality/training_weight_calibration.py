"""Frozen joint-day radius mathematics, not source authentication.

These contracts replay their calculations. The lineage/artifact entry points
also replay the actual observed subsets and generated paths. A caller-created
calibration record, identity string, or successful round trip alone is not
verified empirical support or permission to run the preregistered experiment.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from fractions import Fraction
from typing import ClassVar, Sequence

from .training_contracts import TrainingContract
from .training_weight_contracts import PREREGISTRATION_CANONICAL_SHA256
from .training_weights import _finite_view

WEIGHT_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
WEIGHT_MEMBERS = tuple(f"member-{seed}" for seed in range(60701, 60705))
WEIGHT_PIP = 0.0001
WEIGHT_PROBES = 600
MIN_CALIBRATION_UNITS = 30
MAX_CALIBRATION_UNITS = 42


def _upper_view(value: Fraction) -> float:
    """Smallest representable float not below a nonnegative exact bound."""
    result = _finite_view(value)
    if Fraction(result) < value:
        result = math.nextafter(result, math.inf)
    if not math.isfinite(result):
        raise ValueError("weight upper bound is not representable")
    return result


def _label(value: str) -> None:
    if (
        type(value) is not str
        or not value
        or len(value) > 1024
        or value != value.strip()
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError("invalid bounded weight identity")


def _hash(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise ValueError("invalid weight SHA-256")


def _number(value: float, *, positive: bool = False) -> None:
    if (
        type(value) is not float
        or not math.isfinite(value)
        or value < 0
        or (positive and value == 0)
    ):
        raise ValueError("expected finite nonnegative weight number")


def _date(value: str) -> date:
    if type(value) is not str or date.fromisoformat(value).isoformat() != value:
        raise ValueError("weight date must be canonical ISO date")
    return date.fromisoformat(value)


class WeightSupport(str, Enum):
    SUPPORTED = "supported_conditional_fixed_model"
    INSUFFICIENT = "insufficient_calibration_units"
    DOMAIN = "outside_calibration_scale_domain"
    INCOMPATIBLE = "incompatible_fixed_applicability"


@dataclass(frozen=True, slots=True)
class TrainingWeightApplicabilityV1(TrainingContract):
    """Exact fitted-model/adapter stratum; hashes are verified by replay."""

    KIND: ClassVar[str] = "weight-applicability"
    preregistration_id: str
    model_index_id: str
    model_index_sha256: str
    generator_config_id: str
    adapter_sha256: str
    epoch_interval_id: str
    epoch_label: str = "technology_epoch_03"
    session: str = "london"
    horizon_ns: int = 600_000_000_000
    degradation_id: str = (
        "retain-boundaries-and-every-fourth-interior-source-row.v1"
    )
    information_mode: str = "ex_post"
    symbols: tuple[str, ...] = WEIGHT_SYMBOLS

    def _validate(self) -> None:
        for value in (
            self.preregistration_id,
            self.model_index_id,
            self.generator_config_id,
            self.epoch_interval_id,
        ):
            _label(value)
        _hash(self.model_index_sha256)
        _hash(self.adapter_sha256)
        if (
            self.preregistration_id
            != "training-weight-preregistration:sha256:"
            + PREREGISTRATION_CANONICAL_SHA256
            or self.epoch_label != "technology_epoch_03"
            or self.session != "london"
            or self.horizon_ns != 600_000_000_000
            or self.degradation_id
            != "retain-boundaries-and-every-fourth-interior-source-row.v1"
            or self.information_mode != "ex_post"
            or self.symbols != WEIGHT_SYMBOLS
        ):
            raise ValueError("unsupported weighting applicability stratum")


@dataclass(frozen=True, slots=True)
class TrainingMemberScaleV1(TrainingContract):
    KIND: ClassVar[str] = "member-scale"
    member_id: str
    symbol_scales: tuple[float, ...]

    def _validate(self) -> None:
        if self.member_id not in WEIGHT_MEMBERS or len(self.symbol_scales) != 3:
            raise ValueError("scale must cover the frozen member/symbol scope")
        for value in self.symbol_scales:
            _number(value, positive=True)
            if value < WEIGHT_PIP:
                raise ValueError("scale is below frozen pip floor")


@dataclass(frozen=True, slots=True)
class TrainingWeightScaleSetV1(TrainingContract):
    """Inputs available without application truth; source IDs are not proof."""

    KIND: ClassVar[str] = "weight-scale-set"
    historical_unit_id: str
    utc_date: str
    role: str
    applicability: TrainingWeightApplicabilityV1
    source_snapshot_id: str
    member_scales: tuple[TrainingMemberScaleV1, ...]

    def _validate(self) -> None:
        _label(self.historical_unit_id)
        _label(self.source_snapshot_id)
        day = _date(self.utc_date)
        if self.role not in ("calibration", "application"):
            raise ValueError("unsupported scale-set role")
        periods = (
            ((2010, 1), (2011, 1))
            if self.role == "calibration"
            else ((2011, 2),)
        )
        if (day.year, day.month) not in periods or day.weekday() >= 5:
            raise ValueError("scale-set date is not preregistered for its role")
        if tuple(s.member_id for s in self.member_scales) != WEIGHT_MEMBERS:
            raise ValueError("scales must contain every frozen member in order")


@dataclass(frozen=True, slots=True)
class TrainingWeightCalibrationDayV1(TrainingContract):
    KIND: ClassVar[str] = "weight-calibration-day"
    scales: TrainingWeightScaleSetV1
    maximum_errors: tuple[tuple[float, ...], ...]
    truth_snapshot_id: str

    def _validate(self) -> None:
        _label(self.truth_snapshot_id)
        if self.scales.role != "calibration":
            raise ValueError("application truth cannot enter calibration")
        if len(self.maximum_errors) != len(WEIGHT_MEMBERS):
            raise ValueError("error support differs from member scope")
        for row in self.maximum_errors:
            if len(row) != len(WEIGHT_SYMBOLS):
                raise ValueError("error support differs from symbol scope")
            for value in row:
                _number(value)

    @property
    def joint_score(self) -> float:
        return _upper_view(
            max(
                Fraction(error) / Fraction(scale)
                for row, member in zip(
                    self.maximum_errors, self.scales.member_scales
                )
                for error, scale in zip(row, member.symbol_scales)
            )
        )


def _fit_values(
    days: tuple[TrainingWeightCalibrationDayV1, ...],
) -> tuple[WeightSupport, float | None, tuple[tuple[float, float], ...]]:
    if not 0 < len(days) <= MAX_CALIBRATION_UNITS:
        raise ValueError("calibration support is empty or exceeds schedule")
    dates = tuple(day.scales.utc_date for day in days)
    units = tuple(day.scales.historical_unit_id for day in days)
    if dates != tuple(sorted(set(dates))) or len(set(units)) != len(units):
        raise ValueError(
            "calibration requires unique ordered triangle-day units"
        )
    first = days[0].scales.applicability.artifact_id
    if any(day.scales.applicability.artifact_id != first for day in days):
        raise ValueError("calibration cannot pool different fixed strata")
    domain = tuple(
        (
            min(
                m.symbol_scales[s] for d in days for m in d.scales.member_scales
            ),
            max(
                m.symbol_scales[s] for d in days for m in d.scales.member_scales
            ),
        )
        for s in range(3)
    )
    if len(days) < MIN_CALIBRATION_UNITS:
        return WeightSupport.INSUFFICIENT, None, domain
    # ceil((n+1)*9/10), one-based. No rank clipping or interpolated quantile.
    rank = ((len(days) + 1) * 9 + 9) // 10
    if rank > len(days):
        raise ValueError("conformal rank is outside finite calibration support")
    return (
        WeightSupport.SUPPORTED,
        sorted(day.joint_score for day in days)[rank - 1],
        domain,
    )


@dataclass(frozen=True, slots=True)
class TrainingWeightCalibrationV1(TrainingContract):
    """Replayable math; empirical authenticity requires source-bound reader."""

    KIND: ClassVar[str] = "weight-calibration"
    days: tuple[TrainingWeightCalibrationDayV1, ...]
    status: WeightSupport
    quantile: float | None
    scale_domains: tuple[tuple[float, float], ...]

    def _validate(self) -> None:
        if (self.status, self.quantile, self.scale_domains) != _fit_values(
            self.days
        ):
            raise ValueError(
                "calibration summaries differ from retained day evidence"
            )

    @property
    def applicability(self) -> TrainingWeightApplicabilityV1:
        return self.days[0].scales.applicability


def fit_training_weight_calibration_math(
    days: Sequence[TrainingWeightCalibrationDayV1],
) -> TrainingWeightCalibrationV1:
    """Fit the frozen rank, not empirical qualification of caller records."""
    if not 0 < len(days) <= MAX_CALIBRATION_UNITS:
        raise ValueError("calibration support exceeds frozen bound")
    ordered = tuple(sorted(days, key=lambda d: d.scales.utc_date))
    status, quantile, domain = _fit_values(ordered)
    return TrainingWeightCalibrationV1(ordered, status, quantile, domain)


@dataclass(frozen=True, slots=True)
class TrainingWeightRadiiV1(TrainingContract):
    KIND: ClassVar[str] = "weight-radii"
    calibration_id: str
    scales: TrainingWeightScaleSetV1
    status: WeightSupport
    symbol_radii: tuple[tuple[float, ...], ...]
    diagnostic_symbol_radii: tuple[tuple[float, ...], ...]

    def _validate(self) -> None:
        _label(self.calibration_id)
        if self.scales.role != "application":
            raise ValueError("radii application requires application role")
        if self.status is WeightSupport.SUPPORTED:
            if self.symbol_radii != self.diagnostic_symbol_radii:
                raise ValueError("supported radii must match diagnostic radii")
        elif self.symbol_radii:
            raise ValueError(
                "unsupported confidence cannot retain allocation radii"
            )
        if self.status not in (WeightSupport.SUPPORTED, WeightSupport.DOMAIN):
            if self.diagnostic_symbol_radii:
                raise ValueError(
                    "undefined calibration has no diagnostic radii"
                )
            return
        if len(self.diagnostic_symbol_radii) != 4 or any(
            len(r) != 3 for r in self.diagnostic_symbol_radii
        ):
            raise ValueError("radii differ from frozen member/symbol scope")
        for row in self.diagnostic_symbol_radii:
            for value in row:
                _number(value)

    @property
    def maximum_pip_radii(self) -> tuple[float, ...]:
        if self.status is not WeightSupport.SUPPORTED:
            raise ValueError("unsupported confidence has no allocation inputs")
        return tuple(
            _finite_view(Fraction(max(row)) / Fraction(WEIGHT_PIP))
            for row in self.symbol_radii
        )


def apply_training_weight_calibration_math(
    calibration: TrainingWeightCalibrationV1, scales: TrainingWeightScaleSetV1
) -> TrainingWeightRadiiV1:
    """Apply using available scales only, with no application truth parameter."""
    if scales.historical_unit_id in {
        day.scales.historical_unit_id for day in calibration.days
    }:
        raise ValueError("application historical unit overlaps calibration")
    status = calibration.status
    if (
        scales.applicability.artifact_id
        != calibration.applicability.artifact_id
    ):
        status = WeightSupport.INCOMPATIBLE
    elif status is WeightSupport.SUPPORTED and any(
        not lower <= value <= upper
        for member in scales.member_scales
        for value, (lower, upper) in zip(
            member.symbol_scales, calibration.scale_domains
        )
    ):
        status = WeightSupport.DOMAIN
    radii: tuple[tuple[float, ...], ...] = ()
    if status in (WeightSupport.SUPPORTED, WeightSupport.DOMAIN):
        if calibration.quantile is None:
            raise ValueError("supported calibration lacks finite quantile")
        radii = tuple(
            tuple(
                _upper_view(Fraction(calibration.quantile) * Fraction(s))
                for s in member.symbol_scales
            )
            for member in scales.member_scales
        )
    return TrainingWeightRadiiV1(
        calibration.artifact_id,
        scales,
        status,
        radii if status is WeightSupport.SUPPORTED else (),
        radii,
    )


def available_path_scale(
    mids: Sequence[float],
    spreads: Sequence[float],
    anchor_bridge: Sequence[float],
) -> float:
    """Available-input scale over the exact fixed probe support, without truth."""
    if any(
        len(values) != WEIGHT_PROBES
        for values in (mids, spreads, anchor_bridge)
    ):
        raise ValueError("path scale requires the complete frozen probe grid")
    for values, positive in (
        (mids, True),
        (spreads, False),
        (anchor_bridge, True),
    ):
        for value in values:
            _number(value, positive=positive)
    return _finite_view(
        max(
            Fraction(WEIGHT_PIP),
            sum((Fraction(s) for s in spreads), Fraction()) / WEIGHT_PROBES,
            max(
                abs(Fraction(m) - Fraction(a))
                for m, a in zip(mids, anchor_bridge)
            ),
        )
    )


def maximum_path_error(mids: Sequence[float], truth: Sequence[float]) -> float:
    if len(mids) != WEIGHT_PROBES or len(truth) != WEIGHT_PROBES:
        raise ValueError("path error requires the complete frozen probe grid")
    for values in (mids, truth):
        for value in values:
            _number(value, positive=True)
    return _upper_view(
        max(abs(Fraction(a) - Fraction(b)) for a, b in zip(mids, truth))
    )

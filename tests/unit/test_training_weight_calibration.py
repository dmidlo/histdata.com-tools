"""Independent calibration math canaries, not empirical market evidence."""

from dataclasses import FrozenInstanceError, replace
from fractions import Fraction
import math

import pytest

from histdatacom.data_quality.training_weight_calibration import (
    WEIGHT_MEMBERS,
    TrainingMemberScaleV1,
    TrainingWeightApplicabilityV1,
    TrainingWeightCalibrationDayV1,
    TrainingWeightCalibrationV1,
    TrainingWeightScaleSetV1,
    WeightSupport,
    apply_training_weight_calibration_math,
    available_path_scale,
    fit_training_weight_calibration_math,
    maximum_path_error,
)
from histdatacom.data_quality.training_weight_contracts import (
    read_training_weight_preregistration,
)
from histdatacom.data_quality.training_weights import (
    TrainingMemberPolicy,
    allocate_unit_mass,
)


def applicability():
    return TrainingWeightApplicabilityV1(
        read_training_weight_preregistration().artifact_id,
        "fixture:model-not-market-evidence",
        "a" * 64,
        "fixture:generator",
        "b" * 64,
        "fixture:epoch",
    )


def scale_set(day, *, role="calibration", scales=(1.0, 2.0, 4.0), context=None):
    return TrainingWeightScaleSetV1(
        "fixture:unit:" + day,
        day,
        role,
        applicability() if context is None else context,
        "fixture:source:" + day,
        tuple(
            TrainingMemberScaleV1(member, scales) for member in WEIGHT_MEMBERS
        ),
    )


def calibration_days(n=30):
    dates = read_training_weight_preregistration().scheduled_dates(
        "calibration"
    )
    context = applicability()
    return tuple(
        TrainingWeightCalibrationDayV1(
            scale_set(day, context=context),
            tuple((float(i + 1), 0.0, 0.0) for _ in WEIGHT_MEMBERS),
            "fixture:truth:" + day,
        )
        for i, day in enumerate(dates[:n])
    )


def test_joint_score_rank_is_day_level_not_member_or_symbol_count():
    days = calibration_days()
    result = fit_training_weight_calibration_math(days)
    assert len(result.days) == 30
    assert result.status is WeightSupport.SUPPORTED
    # ceil(31*0.9)=28; the joint day maximum is its first-symbol error.
    assert result.quantile == 28.0
    assert result.scale_domains == ((1.0, 1.0), (2.0, 2.0), (4.0, 4.0))
    assert TrainingWeightCalibrationV1.from_json(result.to_json()) == result
    with pytest.raises(FrozenInstanceError):
        result.quantile = 1.0
    with pytest.raises(ValueError, match="unique"):
        fit_training_weight_calibration_math(days + (days[0],))
    with pytest.raises(ValueError, match="summaries"):
        replace(result, quantile=27.0)
    raw = result.to_dict()
    raw["days"][0]["scales"]["later_truth"] = "unknown"
    with pytest.raises(ValueError, match="unknown"):
        TrainingWeightCalibrationV1.from_dict(raw)


def test_supported_positive_radius_allocation_and_no_application_truth_input():
    calibration = fit_training_weight_calibration_math(calibration_days())
    inputs = scale_set("2011-02-01", role="application")
    radii = apply_training_weight_calibration_math(calibration, inputs)
    assert radii.status is WeightSupport.SUPPORTED
    assert radii.symbol_radii == ((28.0, 56.0, 112.0),) * 4
    allocation = allocate_unit_mass(
        TrainingMemberPolicy.UNCERTAINTY,
        inputs.historical_unit_id,
        WEIGHT_MEMBERS,
        maximum_pip_radii=radii.maximum_pip_radii,
    )
    assert allocation.total_mass == 1
    assert [m.mass for m in allocation.members] == [Fraction(1, 4)] * 4
    assert allocation.filtered((WEIGHT_MEMBERS[0],)).total_mass == Fraction(
        1, 4
    )
    # Changing hidden application errors cannot affect this function: its
    # complete signature contains only calibration and available-input scales.
    with pytest.raises(ValueError, match="application truth"):
        TrainingWeightCalibrationDayV1(inputs, ((0.0, 0.0, 0.0),) * 4, "truth")


def test_insufficient_domain_model_adapter_and_role_refusal_are_explicit():
    inputs = scale_set("2011-02-01", role="application")
    insufficient = fit_training_weight_calibration_math(calibration_days(29))
    radii = apply_training_weight_calibration_math(insufficient, inputs)
    assert radii.status is WeightSupport.INSUFFICIENT
    assert radii.symbol_radii == () and insufficient.quantile is None
    with pytest.raises(ValueError, match="no allocation"):
        _ = radii.maximum_pip_radii
    calibration = fit_training_weight_calibration_math(calibration_days())
    for context in (
        replace(inputs.applicability, adapter_sha256="c" * 64),
        replace(inputs.applicability, model_index_sha256="c" * 64),
        replace(inputs.applicability, generator_config_id="changed"),
        replace(inputs.applicability, epoch_interval_id="changed"),
    ):
        result = apply_training_weight_calibration_math(
            calibration, replace(inputs, applicability=context)
        )
        assert result.status is WeightSupport.INCOMPATIBLE
        assert result.symbol_radii == ()
    outside = scale_set(
        "2011-02-01", role="application", scales=(0.5, 2.0, 4.0)
    )
    assert (
        apply_training_weight_calibration_math(calibration, outside).status
        is WeightSupport.DOMAIN
    )
    with pytest.raises(ValueError, match="strata"):
        fit_training_weight_calibration_math(
            (
                calibration.days[0],
                replace(
                    calibration.days[1],
                    scales=replace(
                        calibration.days[1].scales, applicability=context
                    ),
                ),
            )
        )
    for day, role in (
        ("2025-10-01", "application"),
        ("2011-02-01", "calibration"),
        ("2010-01-02", "calibration"),
    ):
        with pytest.raises(ValueError, match="preregistered"):
            scale_set(day, role=role)


def test_available_scale_uses_own_anchor_bridge_and_exact_complete_probes():
    assert available_path_scale([2.0] * 600, [0.5] * 600, [1.0] * 600) == 1.0
    assert available_path_scale([2.0] * 600, [1.5] * 600, [1.0] * 600) == 1.5
    assert available_path_scale([2.0] * 600, [0.0] * 600, [2.0] * 600) == 0.0001
    assert maximum_path_error([1.0] * 600, [2.0] * 600) == 1.0
    with pytest.raises(ValueError, match="complete"):
        available_path_scale([2.0] * 599, [0.5] * 600, [1.0] * 600)
    for bad in (float("inf"), float("nan"), -1.0, True):
        with pytest.raises(ValueError):
            available_path_scale([bad] * 600, [0.5] * 600, [1.0] * 600)
    shared = [0.0] * 600
    with pytest.raises(ValueError):
        available_path_scale(shared, shared, [1.0] * 600)


def test_zero_radius_is_supported_but_not_a_historical_truth_claim():
    days = tuple(
        replace(d, maximum_errors=((0.0, 0.0, 0.0),) * 4)
        for d in calibration_days()
    )
    calibration = fit_training_weight_calibration_math(days)
    assert calibration.quantile == 0.0
    radii = apply_training_weight_calibration_math(
        calibration, scale_set("2011-02-01", role="application")
    )
    assert radii.symbol_radii == ((0.0, 0.0, 0.0),) * 4
    assert radii.status.value == "supported_conditional_fixed_model"


def test_unrepresentable_radius_refuses_before_allocation():
    days = tuple(
        replace(
            d,
            scales=replace(
                d.scales,
                member_scales=tuple(
                    TrainingMemberScaleV1(m, (1e308, 1.0, 1.0))
                    for m in WEIGHT_MEMBERS
                ),
            ),
        )
        for d in calibration_days()
    )
    # First-symbol scale is enormous but the finite second-symbol error sets q.
    days = tuple(
        replace(d, maximum_errors=((0.0, 2.0, 0.0),) * 4) for d in days
    )
    calibration = fit_training_weight_calibration_math(days)
    inputs = scale_set(
        "2011-02-01", role="application", scales=(1e308, 1.0, 1.0)
    )
    with pytest.raises(ValueError, match="representable"):
        apply_training_weight_calibration_math(calibration, inputs)


def test_domain_abstention_retains_diagnostic_radius_not_weight_permission():
    calibration = fit_training_weight_calibration_math(calibration_days())
    inputs = scale_set("2011-02-01", role="application", scales=(0.5, 2.0, 4.0))
    result = apply_training_weight_calibration_math(calibration, inputs)
    assert result.status is WeightSupport.DOMAIN
    assert result.diagnostic_symbol_radii == ((14.0, 56.0, 112.0),) * 4
    assert result.symbol_radii == ()
    with pytest.raises(ValueError, match="no allocation"):
        _ = result.maximum_pip_radii
    overlapping = replace(
        inputs, historical_unit_id=calibration.days[0].scales.historical_unit_id
    )
    with pytest.raises(ValueError, match="overlaps"):
        apply_training_weight_calibration_math(calibration, overlapping)


def test_score_error_and_radius_round_outward_without_arbitrary_tolerance():
    days = tuple(
        replace(
            d,
            scales=replace(
                d.scales,
                member_scales=tuple(
                    TrainingMemberScaleV1(m, (10.0, 10.0, 10.0))
                    for m in WEIGHT_MEMBERS
                ),
            ),
            maximum_errors=((1.0, 0.0, 0.0),) * 4,
        )
        for d in calibration_days()
    )
    result = fit_training_weight_calibration_math(days)
    assert Fraction(result.quantile) >= Fraction(1, 10)
    assert Fraction(math.nextafter(result.quantile, -math.inf)) < Fraction(
        1, 10
    )
    inputs = scale_set(
        "2011-02-01", role="application", scales=(10.0, 10.0, 10.0)
    )
    applied = apply_training_weight_calibration_math(result, inputs)
    for radius in applied.symbol_radii[0]:
        bound = Fraction(result.quantile) * 10
        assert Fraction(radius) >= bound
        assert Fraction(math.nextafter(radius, -math.inf)) < bound
    actual = maximum_path_error([1.0] * 600, [0.1] * 600)
    exact = Fraction(1) - Fraction(0.1)
    assert Fraction(actual) >= exact
    assert Fraction(math.nextafter(actual, -math.inf)) < exact

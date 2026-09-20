"""Strict additive overlap structure and frozen policy boundaries."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapGeometryV1,
    TrainingOverlapPlanV1,
    TrainingOverlapSelectionV1,
)
from tests.fixtures.training_overlap_v1 import overlap_fixture


def test_geometry_identity_changes_with_stride_and_never_snaps_phase():
    a = TrainingOverlapGeometryV1(7, 3607, 900, 300)
    b = replace(a, stride_ns=450)
    assert a.intervals[0] == (7, 907)
    assert len(a.intervals) == 10
    assert a.maximum_multiplicity == 3
    assert a.artifact_id != b.artifact_id
    assert TrainingOverlapGeometryV1.from_json(a.to_json()) == a


@pytest.mark.parametrize("value", [True, 1.0, -1, 2**63])
def test_clocks_are_exact_bounded_integers(value):
    with pytest.raises(ValueError):
        TrainingOverlapGeometryV1(value, 4000, 900, 300)


def test_unknown_policy_and_noncanonical_selection_refuse():
    with pytest.raises(ValueError):
        TrainingOverlapGeometryV1(0, 4000, 900, 300, "right-anchored")
    with pytest.raises(ValueError):
        TrainingOverlapSelectionV1(window_ids=("b", "a"))
    with pytest.raises(ValueError):
        TrainingOverlapSelectionV1(member_ids=("a", "a"))


def test_complete_split_and_schema_roundtrip(tmp_path):
    plan = overlap_fixture(tmp_path)
    assert TrainingOverlapPlanV1.from_json(plan.to_json()) == plan
    with pytest.raises(ValueError, match="complete ownership"):
        replace(
            plan,
            split=replace(plan.split, assignments=plan.split.assignments[:-1]),
        )
    altered = plan.to_dict()
    altered["hidden_future_policy"] = True
    with pytest.raises(ValueError, match="unknown or missing"):
        TrainingOverlapPlanV1.from_dict(altered)

"""Synthetic native STATE donors obey the fitted membership/window boundary.

These tests publish and replay actual quote/calendar artifacts. They do not
replace native admission, invent membership receipts, or qualify market data.
"""

from __future__ import annotations

import json
import math
from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
)
from histdatacom.data_quality.training_join_contracts import JoinState
from histdatacom.data_quality.training_preprocessing_contracts import (
    PreprocessingFitMode,
    TrainingPreprocessingStepV1,
)
from histdatacom.data_quality.training_preprocessing_math import (
    ReferenceTransformFitV1,
)
from histdatacom.data_quality.training_preprocessing_views import (
    apply_training_preprocessing,
    fit_training_preprocessing,
    materialize_training_preprocessing_view,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    materialize_training_wide_view,
    project_training_wide_view,
)
from histdatacom.market_context.economic_calendar import (
    replay_economic_calendar_corpus,
    write_economic_calendar_corpus,
)
from tests.fixtures.forecast_contracts_v1 import RELEASE_TIME, utc_text
from tests.fixtures.training_join_v1 import controlled_spine
from tests.fixtures.training_preprocessing_v1 import (
    build_preprocessing_calendar_fixture,
    preprocessing_plan,
    weighted_moments,
)

FROZEN_POLICY = "frozen-fit-train-donors-plus-causal-nontrain.v1"
CAUSAL_POLICY = "supplied-application-history-causal-originals.v1"
GRID_STEP = DAY_NS // 4


@pytest.fixture(scope="module")
def donor_native(tmp_path_factory):
    directory = tmp_path_factory.mktemp("preprocessing-donor-membership")
    base = build_preprocessing_calendar_fixture(directory / "base")
    corpus = replay_economic_calendar_corpus(base["artifact_path"])
    first = RELEASE_TIME // DAY_NS * DAY_NS
    releases = []
    # Day 1 begins missing: its first actual observation is one grid step
    # later. Day 0 still has a legitimate, sufficiently recent STATE donor.
    for day, positions in enumerate(((0, 2), (1, 2), (0, 2))):
        old = sorted(
            (
                release
                for release in corpus.releases
                if release.source_release_id.startswith(
                    f"preprocessing-day-{day}-"
                )
            ),
            key=lambda release: release.available_at_ns,
        )
        assert len(old) == 2
        for release, position in zip(old, positions):
            clock = first + day * DAY_NS + position * GRID_STEP
            releases.append(
                replace(
                    release,
                    scheduled_for_ns=clock,
                    scheduled_lexical=utc_text(clock),
                    released_at_ns=clock,
                    released_lexical=utc_text(clock),
                    first_observed_at_ns=clock,
                    available_at_ns=clock,
                    release_id="",
                )
            )
    artifact = write_economic_calendar_corpus(
        replace(corpus, releases=tuple(releases), corpus_id=""),
        directory / "shifted-calendar",
    )
    clocks = tuple(first + index * GRID_STEP for index in range(12))
    batch = controlled_spine(directory / "grid-spine", clocks)
    original_join = base["wide_plan"].tiles[0].join_plan
    grain = TrainingWideGrainV1(
        WideRowGrain.GRID,
        first,
        first + 3 * DAY_NS,
        GRID_STEP,
        0,
    )
    joins = tuple(
        replace(
            original_join,
            spine=materialize_training_rows(
                batch.source,
                batch.ownership,
                TrainingRequestV1(
                    TrainingConsumerMode.DESCRIPTIVE,
                    cutoff,
                    cutoff + 1,
                    ("EURUSD",),
                    decision_time_ns=cutoff,
                ),
            ),
            sources=(
                replace(original_join.sources[0], paths=(str(artifact.path),)),
            ),
        )
        for cutoff in grain.cutoffs
    )
    wide = build_training_wide_plan(joins, grain=grain)
    view = materialize_training_wide_view(wide)
    records = project_training_wide_view(view)
    split = TrainingTemporalSplitV1(
        batch.ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                unit.artifact_id,
                (
                    TemporalPartition.TRAIN
                    if unit.start_ns < first + 2 * DAY_NS
                    else (
                        TemporalPartition.VALIDATION
                        if unit.start_ns < first + 3 * DAY_NS
                        else TemporalPartition.TEST
                    )
                ),
            )
            for unit in batch.ownership.units
        ),
    )
    return {
        "wide_plan": wide,
        "split": split,
        "records": records,
        "column": base["column"],
        "clocks": clocks,
        "train_units": tuple(
            records[index]["spine"]["evidence_unit_id"] for index in (0, 4)
        ),
    }


def _plan(native, *, all_train=False):
    name = native["column"]
    plan = preprocessing_plan(
        native,
        family="median_imputer",
        columns=(name,),
        fit_unit_ids=(
            native["train_units"] if all_train else (native["train_units"][1],)
        ),
    )
    return replace(
        plan,
        steps=(
            TrainingPreprocessingStepV1(
                "bounded-prior",
                "prior_state",
                (name,),
                training_json({"max_age_ns": 2 * DAY_NS}),
            ),
            *plan.steps,
            TrainingPreprocessingStepV1("scale", "zscale", (name,)),
        ),
    )


def _fit_apply(plan):
    fit = fit_training_preprocessing(plan)
    view = apply_training_preprocessing(fit, expected_fit_id=fit.artifact_id)
    return fit, view.records


def _parameters(fit, index):
    kernel = ReferenceTransformFitV1.from_json(fit.steps[index].kernel_fit_json)
    return json.loads(kernel.parameters_json)["columns"][0]


def test_actual_grid_has_known_donor_outside_selected_train_unit(donor_native):
    native = donor_native
    name = native["column"]
    assert tuple(row["values"][name] for row in native["records"]) == (
        3,
        None,
        9,
        None,
        None,
        4,
        12,
        None,
        2,
        None,
        8,
        None,
    )
    assert len(set(native["train_units"])) == 2
    for row, clock in zip(native["records"], native["clocks"]):
        assert row["grid_cutoff_ns"] == clock
        if row["values"][name] is not None:
            proof = row["cell_provenance"][name]
            assert row["states"][name] == JoinState.AVAILABLE.value
            assert proof["available_at_ns"] == proof["source_time_ns"] == clock
            assert proof["dependency_end_ns"] <= clock + 1
            assert proof["historical_availability_verified"] is False
        else:
            assert row["states"][name] == JoinState.UNAVAILABLE.value


def test_train_subset_preserves_fit_inputs_and_validation_own_donors(
    donor_native,
):
    name = donor_native["column"]
    fit, rows = _fit_apply(_plan(donor_native))
    assert len(rows) == 12  # Unselected TRAIN targets remain visible.
    members = json.loads(fit.membership_json)["rows"]
    member_ids = {member["row_id"] for member in members}
    assert member_ids == {row["row_id"] for row in rows[4:8]}
    assert _parameters(fit, 1)["median"] == "12"
    stats = _parameters(fit, 2)
    assert Fraction(stats["mean"]) == 10
    assert Fraction(stats["variance"]) == 12
    # During fit: [missing, 4, 12, missing] -> prior [missing, 4, 12, 12]
    # -> median [12, 4, 12, 12]. Application must have exactly those inputs.
    for row, value in zip(rows[4:8], (12, 4, 12, 12)):
        assert row["values"][name] == pytest.approx(
            (value - 10) / math.sqrt(12)
        )
    boundary = rows[4]["imputation_evidence"][name]
    assert boundary["method"] == "median_imputer"
    assert all(event["method"] != "prior_state" for event in boundary["events"])
    assert rows[4]["originals"][name]["value"] is None
    assert rows[4]["masks"][name] is True
    assert rows[1]["imputation_evidence"][name]["method"] == "median_imputer"
    assert rows[7]["imputation_evidence"][name]["donor_policy"] == FROZEN_POLICY
    assert {
        donor["row_id"]
        for donor in rows[7]["imputation_evidence"][name]["donors"]
    } == {rows[6]["row_id"]}
    # Causal VAL originals remain eligible even though they did not fit the
    # median/scaler. Their availability is not learned TRAIN information.
    for target, donor, raw in ((9, 8, 2), (11, 10, 8)):
        proof = rows[target]["imputation_evidence"][name]
        assert rows[donor]["row_id"] not in member_ids
        assert proof["method"] == "prior_state"
        assert proof["donor_policy"] == FROZEN_POLICY
        assert {entry["row_id"] for entry in proof["donors"]} == {
            rows[donor]["row_id"]
        }
        assert rows[target]["values"][name] == pytest.approx(
            (raw - 10) / math.sqrt(12)
        )


def test_all_train_membership_admits_older_unit_donor(donor_native):
    name = donor_native["column"]
    fit, rows = _fit_apply(_plan(donor_native, all_train=True))
    members = json.loads(fit.membership_json)["rows"]
    assert {member["row_id"] for member in members} == {
        row["row_id"] for row in rows[:8]
    }
    assert len({Fraction(member["mass"]) for member in members}) == 1
    expected = tuple(Fraction(value) for value in (3, 3, 9, 9, 9, 4, 12, 12))
    mean, variance = weighted_moments(expected, (Fraction(1),) * 8)
    stats = _parameters(fit, 2)
    assert Fraction(stats["mean"]) == mean
    assert Fraction(stats["variance"]) == variance
    for row, value in zip(rows[:8], expected):
        assert row["values"][name] == pytest.approx(
            float(value - mean) / math.sqrt(float(variance))
        )
    evidence = rows[4]["imputation_evidence"][name]
    assert evidence["method"] == "prior_state"
    assert evidence["donor_policy"] == FROZEN_POLICY
    assert evidence["age_ns"] == 2 * GRID_STEP
    assert {entry["row_id"] for entry in evidence["donors"]} == {
        rows[2]["row_id"]
    }


def test_none_mode_preserves_all_causal_native_history(donor_native):
    plan = _plan(donor_native)
    plan = replace(
        plan,
        fit_mode=PreprocessingFitMode.NONE,
        fit_unit_ids=(),
        steps=plan.steps[:1],
    )
    rows = materialize_training_preprocessing_view(plan).records
    name = donor_native["column"]
    assert rows[4]["values"][name] == 9
    evidence = rows[4]["imputation_evidence"][name]
    assert evidence["donor_policy"] == CAUSAL_POLICY
    assert {entry["row_id"] for entry in evidence["donors"]} == {
        rows[2]["row_id"]
    }


@pytest.mark.parametrize("window, expected", ((GRID_STEP, None), (DAY_NS, 9)))
def test_rolling_window_freezes_exact_row_donor_membership(
    donor_native, window, expected
):
    plan = _plan(donor_native, all_train=True)
    plan = replace(
        plan,
        fit_mode=PreprocessingFitMode.ROLLING_PTI,
        rolling_window_ns=window,
        steps=plan.steps[:1],
    )
    cutoff = donor_native["clocks"][4]
    fit = fit_training_preprocessing(plan, cutoff_ns=cutoff)
    view = apply_training_preprocessing(
        fit,
        expected_fit_id=fit.artifact_id,
        cutoff_ns=cutoff,
        require_dense=False,
    )
    assert len(view.records) == 1
    row = view.records[0]
    name = donor_native["column"]
    assert row["originals"][name]["value"] is None
    assert row["values"][name] == expected
    members = {
        item["row_id"] for item in json.loads(fit.membership_json)["rows"]
    }
    assert row["row_id"] in members
    assert len(members) == (2 if expected is None else 5)
    assert (
        json.loads(fit.steps[0].kernel_fit_json)["donor_policy"]
        == FROZEN_POLICY
    )
    if expected is None:
        assert not row["imputation_evidence"]
        assert "outside_rolling_window" in {
            reason
            for _, reason in json.loads(fit.support_json)["excluded_rows"]
        }
    else:
        proof = row["imputation_evidence"][name]
        assert proof["method"] == "prior_state"
        assert proof["age_ns"] == 2 * GRID_STEP
        assert proof["donor_policy"] == FROZEN_POLICY
        assert {donor["row_id"] for donor in proof["donors"]} <= members

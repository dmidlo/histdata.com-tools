"""Real native null/cancellation publications, never mocked fill authority."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_preprocessing_contracts import (
    PreprocessingFitMode,
    PreprocessingMissingness,
    TrainingPreprocessingPlanV1,
    TrainingPreprocessingStepV1,
)
from histdatacom.data_quality.training_preprocessing_views import (
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
)
from histdatacom.market_context.economic_calendar import (
    EconomicReleaseStatus,
    write_economic_calendar_corpus,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAbsenceDeclarationV1,
    BarAvailabilityBasis,
    BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1,
    BarFeatureState,
)
from histdatacom.synthetic.information import InformationMode
from tests.fixtures.forecast_contracts_v1 import RELEASE_TIME, calendar_fixture
from tests.fixtures.training_join_v1 import bound_column, controlled_spine
from tests.fixtures.training_substrate_v1 import BASE
from tests.unit.test_training_join_market import native as _native_fixture

SECOND = 1_000_000_000
native = _native_fixture


def _request(join, family="confirmed_zero", options="{}"):
    wide = build_training_wide_plan(
        (join,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    ownership = join.spine.ownership
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                unit.artifact_id, TemporalPartition.TRAIN
            )
            for unit in ownership.units
        ),
    )
    names = tuple(sorted(column.name for column in join.columns))
    return TrainingPreprocessingPlanV1(
        wide,
        split,
        "synthetic-states",
        PreprocessingFitMode.NONE,
        (),
        names,
        (TrainingPreprocessingStepV1("causal-policy", family, names, options),),
    )


def _bar_plan(native, *, absence=None, warmup=False, cutoff=BASE):
    source, ownership, _, _, bars, _, _, _, _, _ = native
    field = "mid_log_close_return" if warmup else "bid_close"
    policy = BarFeaturePolicyV1(
        InformationMode.EX_ANTE_SIMULATION,
        intervals=("1d",),
        scopes=(ActivitySliceScope.OBSERVED,),
        feature_names=(field,),
    )
    declarations = tuple(
        BarAvailabilityDeclarationV1(
            bar.bar_id,
            bar.bar_end_ns,
            bar.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "synthetic clock only",
        )
        for bar in bars.verified_bars("EURUSD", policy)
    )
    absences = (
        ()
        if absence is None
        else (
            BarAbsenceDeclarationV1(
                "EURUSD",
                ActivitySliceScope.OBSERVED,
                "1d",
                BASE - DAY_NS,
                BASE,
                BarFeatureState(absence),
                "synthetic absence declaration",
            ),
        )
    )
    snapshot = bars.snapshot(
        symbol="EURUSD",
        decision_time_ns=cutoff,
        policy=policy,
        availability=declarations,
        absences=absences,
    )
    spine = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            BASE,
            BASE + 1,
            ("EURUSD",),
            decision_time_ns=cutoff,
        ),
    )
    binding = TrainingJoinSourceV1(
        "native-bars",
        JoinFamily.INDICATOR if warmup else JoinFamily.BAR,
        paths=(bars.reconstruction_manifest_path, bars.bar_manifest_path),
        evidence_json=(training_json(snapshot.to_dict()),),
    )
    namespace = "indicator.EURUSD" if warmup else "bar.EURUSD.observed"
    column = bound_column(
        binding,
        f"market.{namespace}.1d.{field}",
        TrainingJoinEntityV1("EURUSD"),
        field,
        coordinate="observed:1d",
        meaning=JoinMeaning.CLOSED_BAR,
    )
    return TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (binding,), (column,)
    )


@pytest.mark.parametrize(
    "absence,missingness",
    [
        ("source_outage", "source_outage"),
        ("expected_closure", "market_closed"),
        ("missing_bar", "empty_market_interval"),
    ],
)
def test_native_absence_is_not_quiet_zero(native, absence, missingness):
    plan = _request(_bar_plan(native, absence=absence))
    row = materialize_training_preprocessing_view(
        plan, require_dense=False
    ).records[0]
    name = plan.columns[0]
    assert row["values"][name] is None
    assert row["originals"][name]["missingness"] == missingness
    assert missingness in {item.value for item in PreprocessingMissingness}
    assert row["masks"][name] is True
    assert row["imputation_evidence"] == {}
    with pytest.raises(ValueError, match="unresolved"):
        materialize_training_preprocessing_view(plan)


def test_indicator_warmup_stays_missing(native):
    plan = _request(_bar_plan(native, warmup=True, cutoff=BASE + DAY_NS))
    row = materialize_training_preprocessing_view(
        plan, require_dense=False
    ).records[0]
    name = plan.columns[0]
    assert row["values"][name] is None
    assert row["originals"][name]["missingness"] == "insufficient_warmup"
    assert row["imputation_evidence"] == {}


def test_closed_or_partial_bar_cannot_be_prior_filled(native):
    join = _bar_plan(native, cutoff=BASE + SECOND)
    plan = _request(join, "prior_state", training_json({"max_age_ns": DAY_NS}))
    with pytest.raises(ValueError, match="persistent state"):
        materialize_training_preprocessing_view(plan, require_dense=False)
    row = materialize_training_preprocessing_view(
        _request(join), require_dense=False
    ).records[0]
    assert row["values"][plan.columns[0]] is None


def _calendar_plan(
    tmp_path,
    *,
    cancel=False,
    delay=SECOND,
    mode=JoinInformationMode.NORMALIZED_AS_OF,
):
    corpus = calendar_fixture(availability_delay_ns=delay)
    if cancel:
        scheduled = next(
            release
            for release in corpus.releases
            if release.status is EconomicReleaseStatus.SCHEDULED
        )
        cancelled = replace(
            scheduled,
            status=EconomicReleaseStatus.CANCELLED,
            available_at_ns=RELEASE_TIME + delay,
            first_observed_at_ns=RELEASE_TIME + delay,
            schedule_change_reason="synthetic retained cancellation",
            release_id="",
        )
        corpus = replace(
            corpus, releases=(cancelled,), forecasts=(), corpus_id=""
        )
    artifact = write_economic_calendar_corpus(corpus, tmp_path / "calendar")
    binding = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(artifact.path),)
    )
    spine = controlled_spine(
        tmp_path / "spine",
        (RELEASE_TIME, RELEASE_TIME + SECOND, RELEASE_TIME + 2 * SECOND),
    )
    name = "calendar.cpi.occurrence" if cancel else "calendar.cpi.actual"
    column = bound_column(
        binding,
        name,
        TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom"),
        "occurrence" if cancel else "actual_value",
        coordinate="period-0",
        meaning=JoinMeaning.EVENT if cancel else JoinMeaning.STATE,
        direction=(
            JoinDirection.EXACT if cancel else JoinDirection.BOUNDED_PRIOR
        ),
        max_age_ns=0 if cancel else 10 * SECOND,
    )
    return _request(TrainingJoinPlanV1(spine, mode, (binding,), (column,)))


def test_delayed_release_never_backward_fills(tmp_path):
    plan = _calendar_plan(tmp_path)
    rows = materialize_training_preprocessing_view(
        plan, require_dense=False
    ).records
    name = plan.columns[0]
    assert rows[0]["values"][name] is None
    assert rows[0]["originals"][name]["missingness"] == "not_yet_available"
    assert rows[1]["values"][name] == rows[2]["values"][name] == 3.0
    assert rows[0]["imputation_evidence"] == {}


@pytest.mark.parametrize("delay", [0, SECOND])
@pytest.mark.parametrize(
    "mode", [JoinInformationMode.NORMALIZED_AS_OF, JoinInformationMode.EX_POST]
)
def test_confirmed_zero_requires_native_known_cancellation(
    tmp_path, delay, mode
):
    plan = _calendar_plan(tmp_path, cancel=True, delay=delay, mode=mode)
    rows = materialize_training_preprocessing_view(
        plan, require_dense=False
    ).records
    name = plan.columns[0]
    assert all(row["originals"][name]["value"] is None for row in rows)
    assert rows[1]["values"][name] is rows[2]["values"][name] is None
    if delay:
        assert rows[0]["values"][name] is None
        assert rows[0]["originals"][name]["missingness"] == "not_yet_available"
    else:
        assert rows[0]["values"][name] == 0
        original = rows[0]["originals"][name]
        assert original["native_state"] == "confirmed_absence"
        assert original["provenance"]["source_ids"]
        assert rows[0]["masks"][name] is True
        assert (
            rows[0]["imputation_evidence"][name]["method"] == "confirmed_zero"
        )
        assert rows[0]["output_status"][name] == "derived"

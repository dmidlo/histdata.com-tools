"""Executed comparators and mutation canaries, not synthetic skill claims."""

import json
from dataclasses import replace

import pytest

from histdatacom.forecasting import (
    BenchmarkComparator,
    ForecastBenchmarkRunV1,
    ForecastDistributionV1,
    ForecastFeatureScoreV1,
    ForecastHorizon,
    ForecastTargetKind,
    PointStatistic,
    default_forecast_benchmark_suite,
    execute_benchmark_prediction,
)
from histdatacom.forecasting import benchmark_runner as runner
from histdatacom.forecasting.benchmark_cases import BENCHMARK_RELEASE_NS, _utc
from histdatacom.forecasting.contracts import DAY_NS, SECOND_NS
from histdatacom.forecasting.feature_contracts import (
    FeatureKind,
    FeatureSourceMode,
)
from histdatacom.market_context.economic_calendar import EconomicReleaseStatus
from tests.fixtures.forecast_benchmark_v1 import benchmark_case


@pytest.mark.parametrize(
    "case_id",
    [f"{kind}-{i}" for kind in ("actual", "consensus") for i in range(1, 6)],
)
def test_all_horizons_and_both_objectives_execute_all_declared_comparators(
    case_id,
):
    _, case, evidence = benchmark_case(case_id)
    for key, expected in case.expected_points:
        forecast, members = execute_benchmark_prediction(evidence, key)
        assert forecast.distribution.point == pytest.approx(expected)
        score = ForecastFeatureScoreV1(
            forecast, evidence.corpus.to_json(), evidence.corpus.coverage_end_ns
        )
        assert score.metrics["observed_target"] == case.expected_target
        assert forecast.cutoff.horizon == case.horizon
        assert forecast.target.kind == case.target_kind
        assert (
            forecast.model.trained_at_ns
            <= forecast.generated_at_ns
            == forecast.cutoff.cutoff_at_ns
        )
        if members:
            assert len(members) == 3
            assert json.loads(forecast.model.state_json)["member_ids"] == [
                m.snapshot_id for m in members
            ]
            assert runner.combination_reference(members, key) == pytest.approx(
                expected
            )


@pytest.mark.parametrize(
    "case_id,code",
    [
        ("missing-consensus", "consensus-unavailable"),
        ("missing-feature", "missing-feature"),
        ("insufficient-support", "insufficient-support"),
        ("semantic-mismatch", "semantic-mismatch"),
        ("late-schedule", "schedule-unavailable"),
    ],
)
def test_each_refusal_replays_a_specific_source_condition(case_id, code):
    _, case, evidence = benchmark_case(case_id)
    with pytest.raises(runner.BenchmarkRefusal) as error:
        execute_benchmark_prediction(evidence, case.comparators[0])
    assert error.value.code == code


@pytest.mark.parametrize(
    "case_id,target",
    [
        ("future-consensus", 4.0),
        ("revision-level", 9.0),
        ("revision-change", 4.0),
        ("observed-surprise", 3.0),
        ("delayed-actual", 5.0),
    ],
)
def test_defined_semantic_cases_retain_exact_target_and_availability(
    case_id, target
):
    _, case, evidence = benchmark_case(case_id)
    forecast, _ = execute_benchmark_prediction(evidence, case.comparators[0])
    score = ForecastFeatureScoreV1(
        forecast, evidence.corpus.to_json(), evidence.corpus.coverage_end_ns
    )
    assert score.metrics["observed_target"] == target
    if case_id == "future-consensus":
        assert forecast.distribution.point == 2.0
        assert [
            x.value for x in forecast.inputs.calendar_inputs.calendar.forecasts
        ] == [2.0]
    if case_id == "delayed-actual":
        actual = evidence.corpus.releases[1]
        assert (
            actual.released_at_ns
            < forecast.cutoff.cutoff_at_ns
            < actual.available_at_ns
        )


def test_consensus_copy_is_explicit_and_not_an_actual_model_or_skill():
    _, _, evidence = benchmark_case("consensus-2")
    forecast, _ = execute_benchmark_prediction(
        evidence, BenchmarkComparator.CONSENSUS_PERSISTENCE
    )
    state = json.loads(forecast.model.state_json)
    assert (
        state["interpretation"]
        == "observable-reference-copy-not-predictive-skill"
    )
    assert forecast.distribution.point == 2.0
    _, _, actual = benchmark_case()
    with pytest.raises(ValueError, match="separate target"):
        execute_benchmark_prediction(
            actual, BenchmarkComparator.CONSENSUS_PERSISTENCE
        )


def test_future_survey_revision_metadata_and_market_insertions_preserve_prediction():
    _, _, evidence = benchmark_case()
    key = BenchmarkComparator.FORECAST_MEAN
    original, _ = execute_benchmark_prediction(evidence, key)
    old = evidence.observations[-1]
    later = replace(
        old,
        definition=replace(
            old.definition,
            kind=FeatureKind.MARKET,
            known_at_ns=BENCHMARK_RELEASE_NS,
        ),
        value=999.0,
        available_at_ns=BENCHMARK_RELEASE_NS,
        published_at_ns=BENCHMARK_RELEASE_NS,
        vintage_sequence=1,
        supersedes_id=old.observation_id,
    )
    changed = replace(evidence, observations=evidence.observations + (later,))
    result, _ = execute_benchmark_prediction(changed, key)
    assert result.to_json() == original.to_json()
    late = evidence.corpus.forecasts[-1]
    mutated = replace(late, value=999.0, lexical_value="999.0", forecast_id="")
    changed = replace(
        evidence,
        corpus=replace(
            evidence.corpus,
            forecasts=(evidence.corpus.forecasts[0], mutated),
            corpus_id="",
        ),
    )
    assert (
        execute_benchmark_prediction(changed, key)[0].to_json()
        == original.to_json()
    )


def test_newly_observable_consensus_at_generation_is_not_backdated_to_fit():
    _, _, evidence = benchmark_case("consensus-2")
    cutoff = evidence.cutoff.cutoff_at_ns
    early, late = evidence.corpus.forecasts
    early = replace(early, available_at_ns=cutoff, forecast_id="")
    evidence = replace(
        evidence,
        corpus=replace(evidence.corpus, forecasts=(early, late), corpus_id=""),
    )
    forecast, _ = execute_benchmark_prediction(
        evidence, BenchmarkComparator.CONSENSUS_PERSISTENCE
    )
    assert forecast.model.trained_at_ns == forecast.generated_at_ns == cutoff
    assert (
        forecast.model.training_inputs.calendar_inputs.calendar.forecasts
        == (early,)
    )


def test_changed_visible_vintage_changes_identity_and_cutoff_cannot_mix():
    _, _, evidence = benchmark_case()
    old, _ = execute_benchmark_prediction(
        evidence, BenchmarkComparator.HISTORICAL_MEAN
    )
    changed = replace(
        evidence,
        observations=(replace(evidence.observations[0], value=7.0),)
        + evidence.observations[1:],
    )
    new, _ = execute_benchmark_prediction(
        changed, BenchmarkComparator.HISTORICAL_MEAN
    )
    assert old.snapshot_id != new.snapshot_id
    assert old.distribution.point != new.distribution.point
    final = replace(
        evidence.cutoff,
        horizon=ForecastHorizon.FINAL_PRE_RELEASE,
        cutoff_at_ns=evidence.cutoff.scheduled_release_at_ns - SECOND_NS,
        final_pre_release_lead_ns=SECOND_NS,
    )
    final_evidence = replace(
        evidence,
        cutoff=final,
        target=replace(
            evidence.target,
            consensus_reference=replace(
                evidence.target.consensus_reference,
                target_at_ns=final.cutoff_at_ns,
            ),
        ),
    )
    final_prediction, _ = execute_benchmark_prediction(
        final_evidence, BenchmarkComparator.HISTORICAL_MEAN
    )
    with pytest.raises(ValueError, match="task, source or cutoff"):
        runner.combination_reference(
            (old, final_prediction), BenchmarkComparator.FORECAST_MEAN
        )


def test_combination_oracle_uses_support_and_rejects_incompatible_members(
    monkeypatch,
):
    _, _, evidence = benchmark_case()
    _, members = execute_benchmark_prediction(
        evidence, BenchmarkComparator.FORECAST_MEAN
    )
    for changed in (
        replace(
            members[0],
            target=replace(
                members[0].target, kind=ForecastTargetKind.CONSENSUS
            ),
        ),
    ):
        with pytest.raises(ValueError, match="component task"):
            runner.combination_reference(
                (changed,) + members[1:], BenchmarkComparator.FORECAST_MEAN
            )
    with pytest.raises(ValueError, match="duplicate"):
        runner.combination_reference(
            (members[0], members[0]), BenchmarkComparator.FORECAST_MEAN
        )
    monkeypatch.setattr(
        ForecastDistributionV1, "point", property(lambda self: 123.0)
    )
    assert runner.combination_reference(
        members, BenchmarkComparator.FORECAST_MEAN
    ) == pytest.approx(11 / 3)
    with pytest.raises(ValueError, match="decimal oracle"):
        execute_benchmark_prediction(
            evidence, BenchmarkComparator.FORECAST_MEAN
        )


def test_wrong_numeric_implementation_refuses_independent_golden_not_badge(
    monkeypatch,
):
    monkeypatch.setattr(runner, "_estimate", lambda key, values: 999.0)
    with pytest.raises(ValueError, match="decimal oracle|frozen point"):
        ForecastBenchmarkRunV1(default_forecast_benchmark_suite())


@pytest.mark.parametrize("change", ["source", "unit", "scale", "base"])
def test_individually_valid_components_cannot_mix_source_or_units(change):
    _, _, evidence = benchmark_case()
    original, _ = execute_benchmark_prediction(
        evidence, BenchmarkComparator.HISTORICAL_MEAN
    )
    if change == "source":
        changed = replace(
            evidence,
            observations=tuple(
                replace(
                    o,
                    evidence=replace(
                        o.evidence, source_name="Another synthetic source"
                    ),
                )
                for o in evidence.observations
            ),
        )
    else:
        semantics = {
            "unit": {"unit": "different-index"},
            "scale": {"scale": 100.0},
            "base": {"base": "different-base"},
        }[change]
        releases = []
        for release in evidence.corpus.releases:
            releases.append(
                replace(
                    release,
                    **semantics,
                    supersedes_release_id=(
                        releases[-1].release_id if releases else None
                    ),
                    release_id="",
                )
            )
        changed = replace(
            evidence,
            corpus=replace(
                evidence.corpus,
                releases=tuple(releases),
                forecasts=tuple(
                    replace(f, **semantics, forecast_id="")
                    for f in evidence.corpus.forecasts
                ),
                corpus_id="",
            ),
            observations=tuple(
                replace(o, definition=replace(o.definition, **semantics))
                for o in evidence.observations
            ),
            cutoff=replace(
                evidence.cutoff, schedule_release_id=releases[0].release_id
            ),
            target=replace(
                evidence.target, **semantics, series_id=releases[0].series_id
            ),
        )
    incompatible, _ = execute_benchmark_prediction(
        changed, BenchmarkComparator.HISTORICAL_MEAN
    )
    # Both components really execute; refusal is at combination, not a corrupt
    # object's construction. Equal numbers do not establish compatible units.
    assert incompatible.distribution.point == original.distribution.point
    with pytest.raises(ValueError, match="component task, source or cutoff"):
        runner.combination_reference(
            (original, incompatible), BenchmarkComparator.FORECAST_MEAN
        )


def test_late_schedule_update_does_not_rewrite_old_forecast_but_visible_one_does():
    _, _, evidence = benchmark_case()
    key = BenchmarkComparator.HISTORICAL_MEAN
    original, _ = execute_benchmark_prediction(evidence, key)
    schedule = evidence.corpus.releases[0]
    new_time = schedule.scheduled_for_ns + DAY_NS

    def updated(known):
        return replace(
            schedule,
            revision_sequence=1,
            supersedes_release_id=schedule.release_id,
            status=EconomicReleaseStatus.RESCHEDULED,
            scheduled_for_ns=new_time,
            scheduled_lexical=_utc(new_time),
            available_at_ns=known,
            first_observed_at_ns=known,
            schedule_change_reason="Synthetic benchmark delay",
            release_id="",
        )

    later = updated(evidence.cutoff.cutoff_at_ns + SECOND_NS)
    changed = replace(
        evidence,
        corpus=replace(
            evidence.corpus, releases=(schedule, later), corpus_id=""
        ),
    )
    assert execute_benchmark_prediction(changed, key)[0].to_json() == (
        original.to_json()
    )
    visible = updated(evidence.cutoff.cutoff_at_ns - 2 * SECOND_NS)
    changed = replace(
        changed,
        corpus=replace(
            changed.corpus, releases=(schedule, visible), corpus_id=""
        ),
    )
    with pytest.raises(ValueError, match="latest known schedule"):
        execute_benchmark_prediction(changed, key)
    cutoff = evidence.cutoff.cutoff_at_ns + DAY_NS
    changed = replace(
        changed,
        cutoff=replace(
            changed.cutoff,
            cutoff_at_ns=cutoff,
            scheduled_release_at_ns=new_time,
            schedule_release_id=visible.release_id,
        ),
        target=replace(
            changed.target,
            consensus_reference=replace(
                changed.target.consensus_reference, target_at_ns=cutoff
            ),
        ),
    )
    revised, _ = execute_benchmark_prediction(changed, key)
    assert revised.cutoff.schedule_release_id == visible.release_id
    assert revised.snapshot_id != original.snapshot_id
    assert type(revised).from_json(revised.to_json()) == revised


def test_even_combination_reference_respects_each_members_declared_point():
    _, _, evidence = benchmark_case()
    _, members = execute_benchmark_prediction(
        evidence, BenchmarkComparator.FORECAST_MEAN
    )
    first = replace(
        members[0],
        distribution=ForecastDistributionV1(
            ((1.0, 0.4), (10.0, 0.6)), PointStatistic.MEDIAN
        ),
    )
    second = replace(
        members[1], distribution=ForecastDistributionV1(((2.0, 1.0),))
    )
    assert (
        runner.combination_reference(
            (first, second), BenchmarkComparator.FORECAST_MEAN
        )
        == 6.0
    )
    assert (
        runner.combination_reference(
            (first, second), BenchmarkComparator.FORECAST_MEDIAN
        )
        == 6.0
    )


@pytest.mark.parametrize(
    "mode",
    [
        FeatureSourceMode.EX_POST,
        FeatureSourceMode.LATEST_REVISED,
        FeatureSourceMode.SYNTHETIC_RECONSTRUCTION,
    ],
)
def test_model_api_refuses_current_revised_and_expost_convenience_inputs(mode):
    _, _, evidence = benchmark_case()
    observation = evidence.observations[0]
    observation = replace(
        observation, evidence=replace(observation.evidence, mode=mode)
    )
    evidence = replace(
        evidence, observations=(observation,) + evidence.observations[1:]
    )
    with pytest.raises(ValueError):
        execute_benchmark_prediction(
            evidence, BenchmarkComparator.HISTORICAL_MEAN
        )


def test_unexpected_errors_do_not_become_successful_expected_refusals(
    monkeypatch,
):
    monkeypatch.setattr(
        runner,
        "execute_benchmark_prediction",
        lambda *args: (_ for _ in ()).throw(RuntimeError("unexpected")),
    )
    with pytest.raises(RuntimeError, match="unexpected"):
        ForecastBenchmarkRunV1(default_forecast_benchmark_suite())

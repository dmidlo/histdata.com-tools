"""Replay, first-release isolation and explicit forecasting metric semantics."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from pathlib import Path

import pytest

import histdatacom.forecasting.contracts as forecast_contracts
from histdatacom.forecasting import (
    ConsensusTiming,
    ForecastDistributionV1,
    ForecastScaleV1,
    ForecastScoreReportV1,
    ForecastScoreV1,
    ForecastTargetKind,
    RevisionMeasure,
    SurpriseReference,
    read_forecast_artifact,
    write_forecast_artifact,
)
from histdatacom.forecasting.contracts import DAY_NS
from tests.fixtures.forecast_contracts_v1 import (
    calendar_fixture,
    nested_forecast_state,
    snapshot_fixture,
)


def test_first_release_score_cannot_be_overwritten_by_revision() -> None:
    corpus = calendar_fixture(actual=3.0, revision=8.0)
    snapshot = snapshot_fixture(corpus, point=2.5)
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert score.metrics["observed_target"] == 3.0
    assert score.metrics["absolute_error"] == 0.5
    assert score.metrics["first_release_id"] == corpus.releases[1].release_id
    assert score.metrics["revision_release_id"] is None
    changed = replace(
        corpus.releases[2],
        actual_value=1000.0,
        actual_lexical="1000.0",
        release_id="",
    )
    updated = replace(
        corpus, releases=(*corpus.releases[:2], changed), corpus_id=""
    )
    later = ForecastScoreV1(
        snapshot.to_json(), updated.to_json(), updated.coverage_end_ns
    )
    assert later.metrics == score.metrics
    assert later.score_id != score.score_id
    assert ForecastScoreV1.from_json(score.to_json()) == score


def test_revision_only_history_cannot_substitute_for_missing_first_release() -> (
    None
):
    corpus = calendar_fixture()
    revision = replace(
        corpus.releases[2],
        revision_sequence=1,
        supersedes_release_id=corpus.releases[0].release_id,
        release_id="",
    )
    missing = replace(
        corpus, releases=(corpus.releases[0], revision), corpus_id=""
    )
    with pytest.raises(ValueError, match="first-release actual"):
        ForecastScoreV1(
            snapshot_fixture().to_json(),
            missing.to_json(),
            missing.coverage_end_ns,
        )


def test_consensus_replication_and_actual_value_added_are_distinct() -> None:
    corpus = calendar_fixture(
        actual=3.0, early_consensus=2.0, late_consensus=9.0
    )
    consensus = snapshot_fixture(
        corpus, point=2.5, kind=ForecastTargetKind.CONSENSUS
    )
    actual = snapshot_fixture(corpus, point=2.5)
    consensus_score = ForecastScoreV1(
        consensus.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    actual_score = ForecastScoreV1(
        actual.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert consensus_score.metrics["observed_target"] == 2.0
    assert consensus_score.metrics["consensus_replication_error"] == 0.5
    assert consensus_score.metrics["independent_value_added"] is None
    assert actual_score.metrics["consensus_replication_error"] is None
    assert actual_score.metrics["independent_value_added"] == 0.5
    assert (
        actual_score.metrics["reference_consensus_id"]
        == corpus.forecasts[0].forecast_id
    )


def test_future_consensus_evolution_scores_future_reference_only_explicitly() -> (
    None
):
    corpus = calendar_fixture(late_consensus=9.0)
    snapshot = snapshot_fixture(
        corpus, point=8.0, kind=ForecastTargetKind.CONSENSUS
    )
    reference = replace(
        snapshot.target.consensus_reference,
        timing=ConsensusTiming.FUTURE_EVOLUTION,
        target_at_ns=snapshot.cutoff.scheduled_release_at_ns - DAY_NS,
    )
    future = replace(
        snapshot, target=replace(snapshot.target, consensus_reference=reference)
    )
    score = ForecastScoreV1(
        future.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert score.metrics["observed_target"] == 9.0
    assert score.metrics["absolute_error"] == 1.0
    assert (
        score.metrics["reference_consensus_id"]
        == corpus.forecasts[1].forecast_id
    )


@pytest.mark.parametrize(
    "measure,expected",
    [
        (RevisionMeasure.LEVEL, 8.0),
        (RevisionMeasure.CHANGE_FROM_FIRST, 5.0),
    ],
)
def test_revision_target_has_separate_exact_sequence_and_measure(
    measure: RevisionMeasure, expected: float
) -> None:
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus, point=5.0, with_reference=False)
    target = replace(
        snapshot.target,
        kind=ForecastTargetKind.REVISION,
        revision_sequence=2,
        revision_measure=measure,
    )
    snapshot = replace(snapshot, target=target)
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert score.metrics["observed_target"] == expected
    assert score.metrics["revision_release_id"] == corpus.releases[2].release_id
    assert score.metrics["independent_value_added"] is None
    initial_only = replace(corpus, releases=corpus.releases[:2], corpus_id="")
    with pytest.raises(ValueError, match="requested revision"):
        ForecastScoreV1(
            snapshot.to_json(),
            initial_only.to_json(),
            initial_only.coverage_end_ns,
        )


def test_observed_and_predicted_consensus_surprises_are_explicit() -> None:
    corpus = calendar_fixture(actual=3.0, early_consensus=2.0)
    base = snapshot_fixture(corpus, point=0.25)
    observed = replace(
        base,
        target=replace(
            base.target,
            kind=ForecastTargetKind.SURPRISE,
            surprise_reference=SurpriseReference.OBSERVED_CONSENSUS,
        ),
    )
    observed_score = ForecastScoreV1(
        observed.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert observed_score.metrics["observed_target"] == 1.0
    assert observed_score.metrics["absolute_error"] == 0.75
    consensus = snapshot_fixture(
        corpus, point=2.5, kind=ForecastTargetKind.CONSENSUS
    )
    scale = ForecastScaleV1(
        "unit",
        1.0,
        "percent_mom",
        consensus.generated_at_ns,
        "Predeclared one-percentage-point unit.",
    )
    consensus = replace(consensus, normalizer_id=str(scale.to_dict()["id"]))
    predicted = replace(
        base,
        target=replace(
            base.target,
            kind=ForecastTargetKind.SURPRISE,
            consensus_reference=None,
            surprise_reference=SurpriseReference.PREDICTED_CONSENSUS,
        ),
        predicted_consensus=consensus,
    )
    score = ForecastScoreV1(
        predicted.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert score.metrics["observed_target"] == 0.5
    assert (
        score.metrics["predicted_consensus_snapshot_id"]
        == consensus.snapshot_id
    )
    assert score.metrics["reference_consensus_id"] is None
    # Aggregating surprise does not attempt to score the constituent consensus
    # or require its separate normalization receipt.
    assert ForecastScoreReportV1((score,)).metrics["mae"] == 0.25


def test_scores_preserve_delayed_availability_independently_from_publication() -> (
    None
):
    corpus = calendar_fixture(availability_delay_ns=DAY_NS)
    snapshot = snapshot_fixture(corpus)
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    assert (
        score.metrics["first_release_available_at_ns"]
        - score.metrics["first_release_published_at_ns"]
        == DAY_NS
    )
    with pytest.raises(ValueError, match="unavailable at scoring"):
        ForecastScoreV1(
            snapshot.to_json(),
            corpus.to_json(),
            corpus.releases[1].released_at_ns,
        )


def test_cutoff_after_early_actual_availability_rejected_at_score_time() -> (
    None
):
    from tests.fixtures.forecast_contracts_v1 import utc_text

    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    early = snapshot.cutoff.cutoff_at_ns
    initial = replace(
        corpus.releases[1],
        released_at_ns=early,
        released_lexical=utc_text(early),
        available_at_ns=early,
        first_observed_at_ns=early,
        release_id="",
    )
    revision = replace(
        corpus.releases[2],
        supersedes_release_id=initial.release_id,
        release_id="",
    )
    early_corpus = replace(
        corpus, releases=(corpus.releases[0], initial, revision), corpus_id=""
    )
    with pytest.raises(ValueError, match="before first-release availability"):
        ForecastScoreV1(
            snapshot.to_json(),
            early_corpus.to_json(),
            early_corpus.coverage_end_ns,
        )


def test_published_before_cutoff_but_unavailable_until_after_is_not_relabelled() -> (
    None
):
    from tests.fixtures.forecast_contracts_v1 import utc_text

    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    published = snapshot.cutoff.cutoff_at_ns - DAY_NS
    initial = replace(
        corpus.releases[1],
        released_at_ns=published,
        released_lexical=utc_text(published),
        release_id="",
    )
    revision = replace(
        corpus.releases[2],
        supersedes_release_id=initial.release_id,
        release_id="",
    )
    delayed = replace(
        corpus, releases=(corpus.releases[0], initial, revision), corpus_id=""
    )
    score = ForecastScoreV1(
        snapshot.to_json(), delayed.to_json(), delayed.coverage_end_ns
    )
    assert (
        score.metrics["first_release_published_at_ns"]
        < snapshot.cutoff.cutoff_at_ns
    )
    assert (
        score.metrics["first_release_available_at_ns"]
        > snapshot.cutoff.cutoff_at_ns
    )


@pytest.mark.parametrize(
    "denominator", [0.0, -1.0, float("nan"), float("inf"), 10**1000]
)
def test_normalization_denominator_is_finite_positive(
    denominator: float,
) -> None:
    with pytest.raises(ValueError, match="positive|finite"):
        ForecastScaleV1("unit", denominator, "percent_mom", 0, "Declared units")


def test_normalizer_must_be_precommitted_and_available_at_generation() -> None:
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    scale = ForecastScaleV1(
        "unit",
        2.0,
        "percent_mom",
        snapshot.generated_at_ns,
        "Declared two-percentage-point scale",
    )
    with pytest.raises(ValueError, match="not committed"):
        ForecastScoreV1(
            snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns, scale
        )
    committed = replace(snapshot, normalizer_id=str(scale.to_dict()["id"]))
    with pytest.raises(ValueError, match="committed normalization is missing"):
        ForecastScoreV1(
            committed.to_json(), corpus.to_json(), corpus.coverage_end_ns
        )
    score = ForecastScoreV1(
        committed.to_json(), corpus.to_json(), corpus.coverage_end_ns, scale
    )
    assert score.metrics["normalized_absolute_error"] == 0.25
    earlier_generation = replace(
        snapshot, generated_at_ns=snapshot.generated_at_ns - DAY_NS
    )
    earlier_generation = replace(
        earlier_generation, normalizer_id=str(scale.to_dict()["id"])
    )
    with pytest.raises(ValueError, match="follows forecast generation"):
        ForecastScoreV1(
            earlier_generation.to_json(),
            corpus.to_json(),
            corpus.coverage_end_ns,
            scale,
        )
    future_evidence = replace(
        scale, evidence_release_ids=(corpus.releases[1].release_id,)
    )
    with pytest.raises(ValueError, match="not available ex ante"):
        ForecastScoreV1(
            replace(
                snapshot, normalizer_id=str(future_evidence.to_dict()["id"])
            ).to_json(),
            corpus.to_json(),
            corpus.coverage_end_ns,
            future_evidence,
        )


def test_full_metric_semantics_and_replayable_report(tmp_path: Path) -> None:
    scores = []
    for index, point in enumerate((2.0, 5.0, 7.0)):
        corpus = calendar_fixture(index=index, actual=3.0, early_consensus=1.0)
        snapshot = snapshot_fixture(corpus, point=point)
        scale = ForecastScaleV1(
            "fixed-unit",
            2.0,
            "percent_mom",
            snapshot.generated_at_ns,
            "Predeclared two-percentage-point unit.",
        )
        snapshot = replace(snapshot, normalizer_id=str(scale.to_dict()["id"]))
        scores.append(
            ForecastScoreV1(
                snapshot.to_json(),
                corpus.to_json(),
                corpus.coverage_end_ns,
                scale,
            )
        )
    report = ForecastScoreReportV1(tuple(scores))
    assert report.metrics["count"] == 3
    assert report.metrics["mae"] == pytest.approx(7 / 3)
    assert report.metrics["rmse"] == pytest.approx(math.sqrt(7))
    assert report.metrics["median_absolute_error"] == 2.0
    assert report.metrics["normalized_mae"] == pytest.approx(7 / 6)
    assert report.metrics["normalized_rmse"] == pytest.approx(math.sqrt(7) / 2)
    assert report.metrics["normalized_median_absolute_error"] == 1.0
    assert report.metrics["mean_independent_value_added"] == pytest.approx(
        -1 / 3
    )
    assert report.metrics["mean_consensus_replication_error"] is None
    assert ForecastScoreReportV1.from_json(report.to_json()) == report
    assert (
        read_forecast_artifact(write_forecast_artifact(report, tmp_path))
        == report
    )
    assert (
        read_forecast_artifact(write_forecast_artifact(scores[0], tmp_path))
        == scores[0]
    )
    with pytest.raises(ValueError, match="counted twice"):
        ForecastScoreReportV1((scores[0], scores[0]))


def test_reports_reject_cross_target_or_model_tasks() -> None:
    first = calendar_fixture()
    second = calendar_fixture(index=1)
    actual = ForecastScoreV1(
        snapshot_fixture(first).to_json(),
        first.to_json(),
        first.coverage_end_ns,
    )
    consensus = ForecastScoreV1(
        snapshot_fixture(second, kind=ForecastTargetKind.CONSENSUS).to_json(),
        second.to_json(),
        second.coverage_end_ns,
    )
    with pytest.raises(ValueError, match="mismatched target/horizon"):
        ForecastScoreReportV1((actual, consensus))
    other = snapshot_fixture(second)
    other = replace(other, model=replace(other.model, version="2.0.0"))
    other_score = ForecastScoreV1(
        other.to_json(), second.to_json(), second.coverage_end_ns
    )
    with pytest.raises(ValueError, match="mismatched target/horizon"):
        ForecastScoreReportV1((actual, other_score))


def test_replay_rejects_forged_score_or_forecast_evidence() -> None:
    corpus = calendar_fixture()
    score = ForecastScoreV1(
        snapshot_fixture(corpus).to_json(),
        corpus.to_json(),
        corpus.coverage_end_ns,
    )
    payload = json.loads(score.to_json())
    payload["metrics"]["observed_target"] = 8.0
    with pytest.raises(ValueError, match="payload mismatch"):
        ForecastScoreV1.from_json(json.dumps(payload))
    payload = json.loads(score.to_json())
    payload["outcome_calendar"]["releases"][1]["actual_value"] = 8.0
    with pytest.raises(ValueError, match="identity"):
        ForecastScoreV1.from_json(json.dumps(payload))


def test_overflowing_score_fails_closed() -> None:
    corpus = calendar_fixture(actual=-1e308)
    snapshot = replace(
        snapshot_fixture(corpus),
        distribution=ForecastDistributionV1(((1e308, 1.0),)),
    )
    with pytest.raises(ValueError, match="finite"):
        ForecastScoreV1(
            snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
        )


def test_score_and_report_expanded_envelope_depth_boundaries(
    tmp_path: Path,
) -> None:
    corpus = calendar_fixture()
    original = snapshot_fixture(corpus)

    def make_score(state_depth: int) -> ForecastScoreV1:
        snapshot = replace(
            original,
            model=replace(
                original.model,
                state_json=json.dumps(nested_forecast_state(state_depth)),
            ),
        )
        return ForecastScoreV1(
            snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
        )

    # snapshot_json is expanded into a real object in the score wire envelope.
    # model.state adds 2 levels in snapshot, 3 in score, and 5 in a report array.
    with pytest.raises(ValueError, match="nesting bounds"):
        make_score(62)
    score = make_score(61)
    assert ForecastScoreV1.from_json(score.to_json()) == score
    assert (
        read_forecast_artifact(write_forecast_artifact(score, tmp_path))
        == score
    )
    with pytest.raises(ValueError, match="nesting bounds"):
        ForecastScoreReportV1((score,))
    with pytest.raises(ValueError, match="nesting bounds"):
        ForecastScoreReportV1((make_score(60),))
    report = ForecastScoreReportV1((make_score(59),))
    assert ForecastScoreReportV1.from_json(report.to_json()) == report
    assert (
        read_forecast_artifact(write_forecast_artifact(report, tmp_path))
        == report
    )


@pytest.mark.parametrize("kind", ["snapshot", "score", "report"])
def test_composed_artifact_final_byte_boundary(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    report = ForecastScoreReportV1((score,))
    original = {"snapshot": snapshot, "score": score, "report": report}[kind]
    data = original.to_dict()
    size = len(original.to_json().encode("utf-8"))
    body = {key: value for key, value in data.items() if key != "id"}
    assert (
        len(forecast_contracts.canonical_contract_json(body).encode("utf-8"))
        < size - 1
    )
    monkeypatch.setattr(forecast_contracts, "MAX_FORECAST_ARTIFACT_BYTES", size)
    accepted = replace(original)
    assert type(original).from_json(accepted.to_json()) == accepted
    monkeypatch.setattr(
        forecast_contracts, "MAX_FORECAST_ARTIFACT_BYTES", size - 1
    )
    with pytest.raises(ValueError, match="byte bound"):
        replace(original)

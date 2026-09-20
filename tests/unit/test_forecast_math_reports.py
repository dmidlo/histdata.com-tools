"""Executed subject verification, not attachment/pass-badge tests."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.forecasting import (
    ConsensusTiming,
    ForecastDistributionV1,
    ForecastHacPolicyV1,
    ForecastMathCheckedReportV1,
    ForecastMathVerificationV1,
    ForecastPairedLossReportV1,
    ForecastScaleV1,
    ForecastScoreReportV1,
    ForecastScoreV1,
    ForecastTargetKind,
    RevisionMeasure,
    SurpriseReference,
    read_math_artifact,
    write_math_artifact,
)
from histdatacom.forecasting import (
    math_artifacts,
    math_reports,
    math_verification,
)
from histdatacom.forecasting.contracts import DAY_NS
from tests.fixtures.forecast_contracts_v1 import (
    RELEASE_TIME,
    calendar_fixture,
    snapshot_fixture,
)
from tests.fixtures.forecast_engine_registry_v1 import engine_comparison_fixture
from tests.fixtures.forecast_math_v1 import math_score, paired_math_scores


@pytest.fixture
def receipt():
    return ForecastMathVerificationV1.current()


def test_legacy_score_aggregate_and_atomic_replay(receipt, tmp_path):
    scores = tuple(
        math_score(i, point=p) for i, p in enumerate((2.5, 2.0, 3.5))
    )
    legacy = ForecastScoreReportV1(scores)
    before = legacy.to_json()
    checked = ForecastMathCheckedReportV1(
        before, receipt, thresholds=(0.0, 2.0, 3.0)
    )
    data = checked.to_dict()
    assert data["aggregate_check"]["mae"] == pytest.approx(2 / 3)
    assert data["aggregate_check"]["rmse"] == pytest.approx(0.5**0.5)
    assert data["aggregate_check"]["median_absolute_error"] == 0.5
    assert len(data["checks"]) == 3
    assert "covariance-combination-v1" in data["harness_only_formula_ids"]
    assert "binary-log-score-v1" in data["subject_formula_ids"]
    assert legacy.to_json() == before
    assert ForecastMathCheckedReportV1.from_json(checked.to_json()) == checked
    path = write_math_artifact(checked, tmp_path)
    assert read_math_artifact(path) == checked
    assert write_math_artifact(checked, tmp_path) == path
    assert read_math_artifact(write_math_artifact(receipt, tmp_path)) == receipt


def test_real_engine_feature_envelopes_not_discarded(receipt):
    comparison = engine_comparison_fixture()
    score = comparison.scores[0]
    for subject in (score.feature_score, score, comparison):
        report = ForecastMathCheckedReportV1(subject.to_json(), receipt)
        wire = report.to_dict()
        assert wire["subject"] == subject.to_dict()
        assert ForecastMathCheckedReportV1.from_json(report.to_json()) == report
    assert wire["aggregate_check"]["evidence_unit_count"] == 1
    assert wire["aggregate_check"]["independent_model_count"] is None
    with pytest.raises(ValueError, match="complete supported scored"):
        ForecastMathCheckedReportV1(score.snapshot.to_json(), receipt)


@pytest.mark.parametrize(
    "kind,expected",
    (
        ("actual", 3.0),
        ("consensus", 2.0),
        ("future-consensus", 9.0),
        ("revision-level", 8.0),
        ("revision-change", 5.0),
        ("observed-surprise", 1.0),
        ("predicted-surprise", 0.5),
    ),
)
def test_independent_outcome_reference_and_revision_selection(
    receipt, kind, expected
):
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    if kind in ("consensus", "future-consensus"):
        snapshot = snapshot_fixture(corpus, kind=ForecastTargetKind.CONSENSUS)
        if kind == "future-consensus":
            named = replace(
                snapshot.target.consensus_reference,
                timing=ConsensusTiming.FUTURE_EVOLUTION,
                target_at_ns=RELEASE_TIME - DAY_NS,
            )
            snapshot = replace(
                snapshot,
                target=replace(snapshot.target, consensus_reference=named),
            )
    elif kind.startswith("revision"):
        snapshot = replace(
            snapshot,
            target=replace(
                snapshot.target,
                kind=ForecastTargetKind.REVISION,
                consensus_reference=None,
                revision_sequence=2,
                revision_measure=(
                    RevisionMeasure.LEVEL
                    if kind == "revision-level"
                    else RevisionMeasure.CHANGE_FROM_FIRST
                ),
            ),
        )
    elif kind.endswith("surprise"):
        predicted = (
            snapshot_fixture(corpus, kind=ForecastTargetKind.CONSENSUS)
            if kind == "predicted-surprise"
            else None
        )
        snapshot = replace(
            snapshot,
            target=replace(
                snapshot.target,
                kind=ForecastTargetKind.SURPRISE,
                consensus_reference=(
                    None if predicted else snapshot.target.consensus_reference
                ),
                surprise_reference=(
                    SurpriseReference.PREDICTED_CONSENSUS
                    if predicted
                    else SurpriseReference.OBSERVED_CONSENSUS
                ),
            ),
            predicted_consensus=predicted,
        )
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    report = ForecastMathCheckedReportV1(score.to_json(), receipt)
    metrics = report.to_dict()["checks"][0]["reference_point_metrics"]
    assert metrics["observed_target"] == expected
    assert metrics["first_release_id"] == corpus.releases[1].release_id


def test_scale_probability_scores_clocks_and_declared_direction(receipt):
    corpus = calendar_fixture(availability_delay_ns=123)
    snapshot = snapshot_fixture(corpus)
    snapshot = replace(
        snapshot,
        distribution=ForecastDistributionV1(((0.0, 0.25), (2.0, 0.75))),
    )
    scale = ForecastScaleV1(
        "fixed-unit",
        2.0,
        snapshot.target.unit,
        snapshot.generated_at_ns,
        "Declared fixture denominator",
    )
    snapshot = replace(snapshot, normalizer_id=str(scale.to_dict()["id"]))
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns, scale
    )
    report = ForecastMathCheckedReportV1(
        score.to_json(),
        receipt,
        thresholds=(2.0, 3.0),
        direction_polarity=-1,
        direction_label="Explicit inverted fixture convention",
    )
    row = report.to_dict()["checks"][0]
    assert row["reference_point_metrics"]["normalized_absolute_error"] == 0.75
    assert (
        row["reference_point_metrics"]["first_release_available_at_ns"]
        == RELEASE_TIME + 123
    )
    assert (
        row["reference_point_metrics"]["first_release_published_at_ns"]
        == RELEASE_TIME
    )
    assert row["error_raw_sign"] == -1 and row["error_mapped_sign"] == 1
    exceedance = row["distribution_metrics"]["exceedance_scores"][0]
    assert exceedance["probability"] == 0 and exceedance["brier"] == 1
    assert exceedance["log_score"] is None
    assert (
        exceedance["log_score_status"] == "positive_infinity_impossible_event"
    )
    assert row["distribution_metrics"]["weighted_crps"] == 1.125
    assert ForecastMathCheckedReportV1.from_json(report.to_json()) == report


@pytest.mark.parametrize(
    "metric",
    (
        "observed_target",
        "forecast_point",
        "absolute_error",
        "first_release_available_at_ns",
    ),
)
def test_production_metric_mutation_cannot_self_certify(
    receipt, monkeypatch, metric
):
    original = ForecastScoreV1.metrics.fget

    def wrong(self):
        result = original(self)
        result[metric] += 1
        return result

    monkeypatch.setattr(ForecastScoreV1, "metrics", property(wrong))
    subject = math_score().to_json()
    with pytest.raises(ValueError, match="independent"):
        ForecastMathCheckedReportV1(subject, receipt)


def test_distribution_point_is_independently_recomputed(receipt, monkeypatch):
    original = ForecastDistributionV1.point.fget
    monkeypatch.setattr(
        ForecastDistributionV1,
        "point",
        property(lambda self: original(self) + 0.1),
    )
    with pytest.raises(ValueError, match="independent"):
        ForecastMathCheckedReportV1(math_score().to_json(), receipt)


def test_aggregate_mutation_is_independently_rejected(receipt, monkeypatch):
    original = ForecastScoreReportV1.metrics.fget

    def wrong(self):
        result = original(self)
        result["mae"] += 0.25
        return result

    monkeypatch.setattr(ForecastScoreReportV1, "metrics", property(wrong))
    subject = ForecastScoreReportV1((math_score(),)).to_json()
    with pytest.raises(ValueError, match="independent"):
        ForecastMathCheckedReportV1(subject, receipt)


@pytest.mark.parametrize(
    "mutation", ("formula", "missing", "extra", "metric", "subject", "code")
)
def test_envelope_mutations_refuse(receipt, mutation):
    report = ForecastMathCheckedReportV1(math_score().to_json(), receipt)
    data = report.to_dict()
    if mutation == "formula":
        data["verification"]["formulas"][0][1] += " altered"
    elif mutation == "missing":
        data["required_formula_ids"].pop()
    elif mutation == "extra":
        data["required_formula_ids"].append("undeclared-v1")
    elif mutation == "metric":
        data["checks"][0]["reference_point_metrics"]["absolute_error"] += 1
    elif mutation == "subject":
        data["subject"]["snapshot"]["distribution"]["support"][0][0] += 1
    else:
        data["verification"]["code_bindings"][0][1] = "a" * 64
    with pytest.raises(ValueError):
        ForecastMathCheckedReportV1.from_dict(data)


def test_current_binding_is_not_static_badge(receipt, monkeypatch):
    report = ForecastMathCheckedReportV1(math_score().to_json(), receipt)
    original = math_verification._bindings
    monkeypatch.setattr(
        math_verification,
        "_bindings",
        lambda: (("changed", "a" * 64),) + original()[1:],
    )
    with pytest.raises(ValueError):
        report.to_json()


def test_paired_loss_full_evidence_hac_replay(receipt, tmp_path):
    baseline, candidate = paired_math_scores()
    report = ForecastPairedLossReportV1(
        baseline[::-1], candidate, ForecastHacPolicyV1(2, 1), receipt
    )
    data = report.to_dict()
    assert data["result"]["count"] == 20
    assert data["result"]["mean_difference"] > 0
    assert data["paired_rows"][0]["event_key"] == "fixture.cpi.event-0"
    assert (
        data["result"]["lower_95"]
        < data["result"]["mean_difference"]
        < data["result"]["upper_95"]
    )
    assert ForecastPairedLossReportV1.from_json(report.to_json()) == report
    assert read_math_artifact(write_math_artifact(report, tmp_path)) == report
    data["result"]["p_value"] = 0.999
    with pytest.raises(ValueError):
        ForecastPairedLossReportV1.from_dict(data)


@pytest.mark.parametrize(
    "failure", ("missing", "duplicate", "tiny", "publisher")
)
def test_missing_or_heterogeneous_comparison_refuses(receipt, failure):
    baseline, candidate = paired_math_scores(8)
    policy = ForecastHacPolicyV1(1, 1, minimum_count=8)
    if failure == "missing":
        candidate = candidate[:-1]
    elif failure == "duplicate":
        candidate = candidate[:-1] + (candidate[0],)
    elif failure == "tiny":
        policy = ForecastHacPolicyV1(1, 1)
    else:
        corpus = calendar_fixture()
        releases = []
        for old in corpus.releases:
            releases.append(
                replace(
                    old,
                    source=replace(
                        old.source,
                        adapter_name="other-official-adapter",
                        source_id="",
                    ),
                    supersedes_release_id=(
                        None if not releases else releases[-1].release_id
                    ),
                    release_id="",
                )
            )
        corpus = replace(corpus, releases=tuple(releases), corpus_id="")
        changed = snapshot_fixture(corpus, point=3.2)
        changed = replace(
            changed, model=replace(changed.model, name="candidate-fixture")
        )
        candidate = (
            ForecastScoreV1(
                changed.to_json(), corpus.to_json(), corpus.coverage_end_ns
            ).to_json(),
        ) + candidate[1:]
    with pytest.raises(
        ValueError,
        match="missing|duplicate|insufficient|heterogeneous|exact task",
    ):
        ForecastPairedLossReportV1(baseline, candidate, policy, receipt)


def test_symlink_artifact_is_refused(receipt, tmp_path):
    target = write_math_artifact(receipt, tmp_path)
    link = tmp_path / "symlink"
    try:
        link.symlink_to(target)
    except NotImplementedError:
        pytest.skip("symlink creation is unavailable on this platform")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink creation requires privilege")
        raise
    with pytest.raises(ValueError):
        read_math_artifact(link)


@pytest.mark.skipif(
    not hasattr(os, "mkfifo"), reason="FIFO creation requires os.mkfifo"
)
def test_fifo_artifact_is_refused(tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(ValueError):
        read_math_artifact(fifo)


def test_special_files_and_interrupted_publication(
    receipt, tmp_path, monkeypatch
):
    target = write_math_artifact(receipt, tmp_path)
    original = target.read_bytes()
    with pytest.raises(ValueError):
        read_math_artifact(tmp_path)

    def interrupted(*args, **kwargs):
        raise OSError("synthetic link interruption")

    monkeypatch.setattr(math_artifacts.os, "link", interrupted)
    with pytest.raises(OSError):
        write_math_artifact(receipt, tmp_path)
    assert target.read_bytes() == original
    assert not list(tmp_path.glob(".forecast-math-*"))


def test_composed_final_envelope_exact_byte_boundary(receipt, monkeypatch):
    report = ForecastMathCheckedReportV1(math_score().to_json(), receipt)
    text = report.to_json()
    monkeypatch.setattr(
        math_verification, "MAX_MATH_ARTIFACT_BYTES", len(text.encode())
    )
    assert ForecastMathCheckedReportV1.from_json(text).to_json() == text
    monkeypatch.setattr(
        math_verification, "MAX_MATH_ARTIFACT_BYTES", len(text.encode()) - 1
    )
    with pytest.raises(ValueError, match="byte"):
        ForecastMathCheckedReportV1(report.subject_json, receipt)


def test_code_hash_and_original_subject_id_bindings(receipt):
    for name, digest in receipt.code_bindings:
        assert (
            hashlib.sha256(
                Path(math_reports.__file__).with_name(name).read_bytes()
            ).hexdigest()
            == digest
        )
    score = math_score()
    report = ForecastMathCheckedReportV1(score.to_json(), receipt)
    assert json.loads(report.to_json())["subject"]["id"] == score.score_id

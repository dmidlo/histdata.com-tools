"""Tests for Git-bound, gate-derived release-holdout evaluation."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import histdatacom.synthetic.release_holdout_evaluation as evaluation_module
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.runtime_contracts import ArtifactRef, JSONScalar
from histdatacom.synthetic.benchmark_corpus import (
    BenchmarkWindowMetricObservationV1,
    BenchmarkWindowMetricTraceV1,
)
from histdatacom.synthetic.benchmark_gates import (
    BenchmarkGateComparator,
    BenchmarkGateObservationV1,
    BenchmarkGateScope,
    BenchmarkGateSeverity,
    BenchmarkPromotionDecisionV1,
    evaluate_benchmark_promotion_gates,
    load_default_benchmark_promotion_gate_policy,
)
from histdatacom.synthetic.release_holdout import (
    ReleaseHoldoutAlreadyConsumedError,
    ReleaseHoldoutEvaluationOutcome,
)
from histdatacom.synthetic.release_holdout_evaluation import (
    ReconstructionReleaseHoldoutAuthorizationV1,
    ReconstructionReleaseHoldoutReceiptV1,
    ReleaseHoldoutGateReportV1,
    authorize_reconstruction_release_holdout,
    build_release_holdout_gate_report,
    execute_reconstruction_release_holdout_once,
    load_default_release_holdout_evaluation_policy,
    read_release_holdout_evaluation_policy,
    read_release_holdout_gate_report,
    write_release_holdout_evaluation_policy,
    write_release_holdout_gate_report,
)

_TRACE_METRICS = {
    "event_count_relative_error": 0.1,
    "immutable_anchor_violation_count": 0.0,
    "interarrival_hist_l1": 0.1,
    "path_realized_variation_relative_error": 0.1,
    "spread_tail_relative_error": 0.1,
    "triangle_residual_p99_pips": 1.0,
    "unsupported_context_emission_count": 0.0,
    "update_transition_l1": 0.1,
}


def _artifact(tmp_path: Path, name: str) -> ArtifactRef:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"row_free":true}\n', encoding="utf-8")
    return artifact_ref_for_file(path, kind="row_free_test_evidence")


def _git(repo: Path, *arguments: str, date: str | None = None) -> str:
    environment = dict(os.environ)
    if date is not None:
        environment.update(
            {"GIT_AUTHOR_DATE": date, "GIT_COMMITTER_DATE": date}
        )
    result = subprocess.run(
        ("git", *arguments),
        cwd=repo,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _passing_value(
    comparator: BenchmarkGateComparator, threshold: JSONScalar
) -> JSONScalar:
    if comparator is BenchmarkGateComparator.TRUE:
        return True
    if comparator is BenchmarkGateComparator.FALSE:
        return False
    if comparator is BenchmarkGateComparator.ZERO:
        return 0
    return threshold


def _failing_value(
    comparator: BenchmarkGateComparator, threshold: JSONScalar
) -> JSONScalar:
    if comparator is BenchmarkGateComparator.TRUE:
        return False
    if comparator is BenchmarkGateComparator.FALSE:
        return True
    if comparator is BenchmarkGateComparator.ZERO:
        return 1
    if comparator is BenchmarkGateComparator.LESS_OR_EQUAL:
        assert isinstance(threshold, (int, float))
        return threshold + 1
    if comparator is BenchmarkGateComparator.GREATER_OR_EQUAL:
        assert isinstance(threshold, (int, float))
        return threshold - 1
    return "different"


def _decision(
    scope: BenchmarkGateScope,
    subject_id: str,
    *,
    missing_hard: bool = False,
    failing_hard: bool = False,
) -> BenchmarkPromotionDecisionV1:
    policy = load_default_benchmark_promotion_gate_policy()
    observations = []
    changed = False
    for requirement in policy.requirements_for(scope):
        if (
            not changed
            and missing_hard
            and requirement.severity is BenchmarkGateSeverity.HARD
        ):
            changed = True
            continue
        value = _passing_value(requirement.comparator, requirement.threshold)
        if (
            not changed
            and failing_hard
            and requirement.severity is BenchmarkGateSeverity.HARD
        ):
            value = _failing_value(
                requirement.comparator, requirement.threshold
            )
            changed = True
        observations.append(
            BenchmarkGateObservationV1(
                scope=scope,
                subject_id=subject_id,
                metric_name=requirement.metric_name,
                value=value,
                evidence_ids=("row-free-evidence:v1",),
            )
        )
    return evaluate_benchmark_promotion_gates(
        policy,
        observations,
        scope=scope,
        subject_id=subject_id,
    )


def _report(
    tmp_path: Path,
    *,
    campaign_decision: BenchmarkPromotionDecisionV1 | None = None,
    candidate_decision: BenchmarkPromotionDecisionV1 | None = None,
    outcome: ReleaseHoldoutEvaluationOutcome = (
        ReleaseHoldoutEvaluationOutcome.PASSED
    ),
) -> ReleaseHoldoutGateReportV1:
    corpus_ref = _artifact(tmp_path, "corpus.json")
    campaign_ref = _artifact(tmp_path, "campaign.json")
    trace_ref = _artifact(tmp_path, "trace.json")
    return ReleaseHoldoutGateReportV1(
        authorization_id="authorization:v1",
        manifest_id="manifest:v1",
        graph_id="graph:v1",
        scientific_candidate_id="scientific-candidate:v1",
        release_candidate_id="release-candidate:v1",
        evaluation_policy_id="evaluation-policy:v1",
        corpus_id="corpus:v1",
        campaign_id="campaign:v1",
        metric_trace_id="trace:v1",
        benchmark_candidate_id="benchmark-candidate:v1",
        config_id="config:v1",
        fit_id="fit:v1",
        window_id_map={"protected-window:v1": "benchmark-window:v1"},
        ensemble_member_ids=("member-01", "member-02"),
        candidate_metrics={"candidate_failure_count": 0},
        campaign_decision=campaign_decision
        or _decision(BenchmarkGateScope.CAMPAIGN, "corpus:v1"),
        candidate_decision=candidate_decision
        or _decision(BenchmarkGateScope.CANDIDATE, "benchmark-candidate:v1"),
        corpus_ref=corpus_ref,
        campaign_ref=campaign_ref,
        metric_trace_ref=trace_ref,
        outcome=outcome,
    )


def _builder_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    observations: tuple[BenchmarkWindowMetricObservationV1, ...],
) -> tuple[
    ReconstructionReleaseHoldoutAuthorizationV1, ArtifactRef, ArtifactRef
]:
    manifest_ref = _artifact(tmp_path, "manifest.json")
    graph_ref = _artifact(tmp_path, "graph.json")
    candidate_ref = _artifact(tmp_path, "candidate.json")
    policy_ref = _artifact(tmp_path, "policy.json")
    corpus_ref = _artifact(tmp_path, "corpus-builder.json")
    campaign_ref = _artifact(tmp_path, "campaign-builder.json")
    trace_ref = _artifact(tmp_path, "trace-builder.json")
    authorization = ReconstructionReleaseHoldoutAuthorizationV1(
        manifest_id="manifest:v1",
        manifest_ref=manifest_ref,
        graph_id="graph:v1",
        graph_ref=graph_ref,
        scientific_candidate_id="scientific-candidate:v1",
        release_candidate_id="release-candidate:v1",
        release_candidate_ref=candidate_ref,
        evaluation_policy_id="evaluation-policy:v1",
        evaluation_policy_ref=policy_ref,
        corpus_id="corpus:v1",
        corpus_ref=corpus_ref,
        ensemble_member_ids=("member-01", "member-02"),
        evidence_git_commit_sha="a" * 40,
        repository_root=str(tmp_path),
        manifest_git_path="manifest.json",
        evaluation_policy_git_path="policy.json",
        corpus_git_path="corpus-builder.json",
        authorized_at_utc="2026-08-26T04:00:00Z",
    )
    policy = load_default_release_holdout_evaluation_policy()
    manifest = SimpleNamespace(manifest_id="manifest:v1")
    graph = SimpleNamespace(
        graph_id="graph:v1", candidate_id="scientific-candidate:v1"
    )
    candidate = SimpleNamespace(candidate_id="release-candidate:v1")
    window = SimpleNamespace(
        window_id="benchmark-window:v1",
        session="london",
        epoch_label="modern",
        context_state="market-context:v1",
        positioning_state="positioning:v1",
    )
    corpus = SimpleNamespace(corpus_id="corpus:v1", windows=(window,))
    benchmark_candidate = SimpleNamespace(candidate_id="benchmark-candidate:v1")
    config = SimpleNamespace(
        config_id="config:v1",
        excitation_structure=SimpleNamespace(value="full"),
    )
    fit = SimpleNamespace(fit_id="fit:v1")
    campaign = SimpleNamespace(
        corpus_id="corpus:v1",
        campaign_id="campaign:v1",
        source_replay_verified=True,
        campaign_gate_decision=_decision(
            BenchmarkGateScope.CAMPAIGN, "corpus:v1"
        ),
        candidate_reports=(
            SimpleNamespace(
                candidate_id="benchmark-candidate:v1",
                method_name="marked_hawkes_full",
                role="candidate",
                provisional=False,
                ensemble_member_count=2,
            ),
        ),
        started_at_utc="2026-08-26T05:00:00Z",
        completed_at_utc="2026-08-26T06:00:00Z",
    )
    trace = BenchmarkWindowMetricTraceV1(
        corpus_id="corpus:v1",
        campaign_id="campaign:v1",
        observations=observations,
    )
    inputs: tuple[Any, ...] = (manifest, graph, candidate, policy, corpus)
    monkeypatch.setattr(
        evaluation_module,
        "_authorization_inputs",
        lambda _: inputs,
    )
    monkeypatch.setattr(
        evaluation_module,
        "_exact_benchmark_candidate",
        lambda *_: (config, fit, benchmark_candidate),
    )
    monkeypatch.setattr(
        evaluation_module,
        "_holdout_window_map",
        lambda *_: {"protected-window:v1": "benchmark-window:v1"},
    )
    monkeypatch.setattr(
        evaluation_module,
        "read_reverse_degradation_benchmark_campaign",
        lambda _: campaign,
    )
    monkeypatch.setattr(
        evaluation_module,
        "read_benchmark_window_metric_trace",
        lambda _: trace,
    )
    return authorization, campaign_ref, trace_ref


def _trace_observation(
    member_id: str,
    *,
    split_kind: str = "final_holdout",
    metrics: dict[str, float] | None = None,
    session: str = "london",
) -> BenchmarkWindowMetricObservationV1:
    return BenchmarkWindowMetricObservationV1(
        candidate_id="benchmark-candidate:v1",
        method_name="marked_hawkes_full",
        role="candidate",
        split_kind=split_kind,
        window_id="benchmark-window:v1",
        ensemble_member_id=member_id,
        reference_metrics={"event_rate_hz": 2.0},
        candidate_metrics={"event_rate_hz": 1.8},
        comparison_metrics=metrics or dict(_TRACE_METRICS),
        session=session,
        epoch_label="modern",
        context_state="market-context:v1",
        positioning_state="positioning:v1",
    )


def test_packaged_policy_is_predeclared_and_content_addressed(
    tmp_path: Path,
) -> None:
    policy = load_default_release_holdout_evaluation_policy()

    assert policy.issue_number == 512
    assert policy.required_split_kind == "final_holdout"
    assert policy.holdout_selection_role is False
    assert policy.frozen_before_release_holdout_results is True
    assert policy.benchmark_gate_policy_id == (
        load_default_benchmark_promotion_gate_policy().policy_id
    )

    ref = write_release_holdout_evaluation_policy(policy, tmp_path)
    assert ref.sha256 in Path(ref.path).name
    assert read_release_holdout_evaluation_policy(ref.path) == policy


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    ("field", "value"),
    (
        ("frozen_fit_required", False),
        ("holdout_only_metrics_required", False),
        ("source_replay_required", False),
        ("manifest_policy_and_corpus_git_commit_required", False),
        ("candidate_selected_without_holdout_results", False),
        ("holdout_selection_role", True),
        ("frozen_before_release_holdout_results", False),
    ),
)
def test_policy_cannot_be_weakened(field: str, value: bool) -> None:
    policy = load_default_release_holdout_evaluation_policy()
    payload = policy.to_dict()
    payload[field] = value
    payload["policy_id"] = ""

    with pytest.raises(ValueError):
        type(policy).from_dict(payload)


def test_report_pass_is_derived_and_round_trips(tmp_path: Path) -> None:
    report = _report(tmp_path)

    assert report.outcome is ReleaseHoldoutEvaluationOutcome.PASSED
    assert report.to_dict()["holdout_only"] is True
    assert report.to_dict()["event_rows_embedded"] is False
    assert report.to_dict()["automatic_winner"] is False
    ref = write_release_holdout_gate_report(report, tmp_path / "reports")
    assert read_release_holdout_gate_report(ref.path) == report

    with pytest.raises(ValueError, match="outcome differs from gates"):
        replace(
            report,
            outcome=ReleaseHoldoutEvaluationOutcome.FAILED,
            report_id="",
        )


def test_missing_hard_gate_derives_insufficient_evidence(
    tmp_path: Path,
) -> None:
    decision = _decision(
        BenchmarkGateScope.CANDIDATE,
        "benchmark-candidate:v1",
        missing_hard=True,
    )

    report = _report(
        tmp_path,
        candidate_decision=decision,
        outcome=ReleaseHoldoutEvaluationOutcome.INSUFFICIENT_EVIDENCE,
    )

    assert (
        report.outcome is ReleaseHoldoutEvaluationOutcome.INSUFFICIENT_EVIDENCE
    )


def test_measured_hard_gate_violation_derives_failure(tmp_path: Path) -> None:
    decision = _decision(
        BenchmarkGateScope.CANDIDATE,
        "benchmark-candidate:v1",
        failing_hard=True,
    )

    report = _report(
        tmp_path,
        candidate_decision=decision,
        outcome=ReleaseHoldoutEvaluationOutcome.FAILED,
    )

    assert report.outcome is ReleaseHoldoutEvaluationOutcome.FAILED


def test_receipt_cannot_relabel_a_gate_report(tmp_path: Path) -> None:
    report = _report(tmp_path)
    report_ref = write_release_holdout_gate_report(report, tmp_path / "reports")

    receipt = ReconstructionReleaseHoldoutReceiptV1(
        authorization_id=report.authorization_id,
        manifest_id=report.manifest_id,
        graph_id=report.graph_id,
        scientific_candidate_id=report.scientific_candidate_id,
        release_candidate_id=report.release_candidate_id,
        outcome=report.outcome,
        evaluated_at_utc="2026-08-26T04:00:00Z",
        report_id=report.report_id,
        report_ref=report_ref,
    )

    assert receipt.outcome is ReleaseHoldoutEvaluationOutcome.PASSED
    with pytest.raises(ValueError, match="operational failure cannot claim"):
        replace(
            receipt,
            outcome=ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE,
            receipt_id="",
        )


def test_builder_uses_only_complete_exact_holdout_cells(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observations = (
        _trace_observation("member-01"),
        _trace_observation("member-02"),
        _trace_observation("validation-member", split_kind="validation"),
    )
    authorization, campaign_ref, trace_ref = _builder_fixture(
        tmp_path, monkeypatch, observations
    )

    report = build_release_holdout_gate_report(
        authorization, campaign_ref, trace_ref
    )

    assert report.outcome is ReleaseHoldoutEvaluationOutcome.PASSED
    assert report.candidate_metrics["candidate_failure_count"] == 0
    assert report.candidate_metrics["uncertainty_interval_count"] == 6


def test_builder_counts_a_missing_member_as_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization, campaign_ref, trace_ref = _builder_fixture(
        tmp_path, monkeypatch, (_trace_observation("member-01"),)
    )

    report = build_release_holdout_gate_report(
        authorization, campaign_ref, trace_ref
    )

    assert report.outcome is ReleaseHoldoutEvaluationOutcome.FAILED
    assert report.candidate_metrics["candidate_failure_count"] == 1


def test_builder_rejects_incomplete_or_fractional_cell_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    incomplete = dict(_TRACE_METRICS)
    del incomplete["interarrival_hist_l1"]
    authorization, campaign_ref, trace_ref = _builder_fixture(
        tmp_path,
        monkeypatch,
        (
            _trace_observation("member-01", metrics=incomplete),
            _trace_observation("member-02"),
        ),
    )
    with pytest.raises(ValueError, match="metrics are incomplete"):
        build_release_holdout_gate_report(
            authorization, campaign_ref, trace_ref
        )

    fractional = dict(_TRACE_METRICS)
    fractional["immutable_anchor_violation_count"] = 0.5
    authorization, campaign_ref, trace_ref = _builder_fixture(
        tmp_path,
        monkeypatch,
        (
            _trace_observation("member-01", metrics=fractional),
            _trace_observation("member-02"),
        ),
    )
    with pytest.raises(ValueError, match="count is fractional"):
        build_release_holdout_gate_report(
            authorization, campaign_ref, trace_ref
        )


def test_builder_rejects_candidate_stratum_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization, campaign_ref, trace_ref = _builder_fixture(
        tmp_path,
        monkeypatch,
        (
            _trace_observation("member-01", session="new_york"),
            _trace_observation("member-02"),
        ),
    )

    with pytest.raises(ValueError, match="candidate metadata differs"):
        build_release_holdout_gate_report(
            authorization, campaign_ref, trace_ref
        )


def test_executor_rebuilds_report_and_consumes_before_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization, campaign_ref, trace_ref = _builder_fixture(
        tmp_path,
        monkeypatch,
        (_trace_observation("member-01"), _trace_observation("member-02")),
    )
    calls = 0

    def evaluator(*_: Any) -> ArtifactRef:
        nonlocal calls
        calls += 1
        report = build_release_holdout_gate_report(
            authorization, campaign_ref, trace_ref
        )
        return write_release_holdout_gate_report(
            report, tmp_path / "executor-reports"
        )

    receipt, _ = execute_reconstruction_release_holdout_once(
        authorization,
        tmp_path / "ledger",
        evaluator,
        evaluated_at_utc="2026-08-26T07:00:00Z",
    )

    assert calls == 1
    assert receipt.outcome is ReleaseHoldoutEvaluationOutcome.PASSED
    with pytest.raises(ReleaseHoldoutAlreadyConsumedError):
        execute_reconstruction_release_holdout_once(
            authorization,
            tmp_path / "ledger",
            evaluator,
            evaluated_at_utc="2026-08-26T08:00:00Z",
        )
    assert calls == 1


def test_executor_consumes_a_fabricated_report_as_operational_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization, _, _ = _builder_fixture(
        tmp_path,
        monkeypatch,
        (_trace_observation("member-01"), _trace_observation("member-02")),
    )
    fabricated_ref = write_release_holdout_gate_report(
        _report(tmp_path), tmp_path / "fabricated"
    )

    receipt, _ = execute_reconstruction_release_holdout_once(
        authorization,
        tmp_path / "fabricated-ledger",
        lambda *_: fabricated_ref,
        evaluated_at_utc="2026-08-26T07:00:00Z",
    )

    assert (
        receipt.outcome is ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE
    )
    assert receipt.operational_error_type == "ValueError"
    with pytest.raises(ReleaseHoldoutAlreadyConsumedError):
        execute_reconstruction_release_holdout_once(
            authorization,
            tmp_path / "fabricated-ledger",
            lambda *_: fabricated_ref,
            evaluated_at_utc="2026-08-26T08:00:00Z",
        )


def test_authorization_requires_byte_identical_ancestor_git_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Release Holdout Test")
    _git(repo, "config", "user.email", "holdout@example.test")
    repository_url = "https://example.test/histdatacom.git"
    _git(repo, "remote", "add", "origin", repository_url)
    evidence = repo / "evidence"
    manifest_ref = _artifact(evidence, "manifest.json")
    corpus_ref = _artifact(evidence, "corpus.json")
    selection_ref = _artifact(evidence, "selection.json")
    policy = load_default_release_holdout_evaluation_policy()
    policy_ref = write_release_holdout_evaluation_policy(policy, evidence)
    _git(repo, "add", "evidence")
    _git(
        repo,
        "commit",
        "-q",
        "-m",
        "test: freeze holdout evidence",
        date="2026-08-26T01:00:00Z",
    )
    evidence_commit = _git(repo, "rev-parse", "HEAD")

    graph_ref = _artifact(repo, "graph.json")
    candidate_ref = _artifact(repo, "candidate.json")
    registry_path = repo / "certification-policy.json"
    corpus_id = "corpus:v1"
    members = ("member-01", "member-02")
    registry_path.write_text(
        json.dumps(
            {
                "entries": {
                    "certification_policy": {
                        "payload": {
                            "release_holdout_evaluation_policy_id": (
                                policy.policy_id
                            ),
                            "release_holdout_corpus_id": corpus_id,
                            "benchmark_gate_policy_id": (
                                policy.benchmark_gate_policy_id
                            ),
                            "benchmark_gate_policy_commit": (
                                policy.benchmark_gate_policy_commit
                            ),
                            "release_holdout_ensemble_member_ids": list(
                                members
                            ),
                        }
                    }
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    registry_ref = artifact_ref_for_file(
        registry_path, kind="release_scientific_policy_registry_v1"
    )
    _git(repo, "add", ".")
    _git(
        repo,
        "commit",
        "-q",
        "-m",
        "test: freeze exact candidate",
        date="2026-08-26T03:00:00Z",
    )
    candidate_commit = _git(repo, "rev-parse", "HEAD")

    manifest = SimpleNamespace(
        manifest_id="manifest:v1",
        selection_dossier_id="selection:v1",
        selection_dossier_ref=selection_ref,
    )
    graph = SimpleNamespace(
        graph_id="graph:v1",
        manifest_id="manifest:v1",
        selection_dossier_id="selection:v1",
        candidate_id="scientific-candidate:v1",
        frozen_at_utc="2026-08-26T02:00:00Z",
    )
    corpus = SimpleNamespace(
        corpus_id=corpus_id,
        profile=SimpleNamespace(ensemble_member_ids=members),
        gate_policy_id=policy.benchmark_gate_policy_id,
        gate_policy_commit=policy.benchmark_gate_policy_commit,
    )
    dependencies = (
        SimpleNamespace(
            name="benchmark_corpus",
            artifact_id=corpus.corpus_id,
            artifact_ref=corpus_ref,
        ),
        SimpleNamespace(
            name="candidate_graph",
            artifact_id=graph.graph_id,
            artifact_ref=graph_ref,
        ),
        SimpleNamespace(
            name="protected_release_holdout",
            artifact_id=manifest.manifest_id,
            artifact_ref=manifest_ref,
        ),
        SimpleNamespace(
            name="product_selection_dossier",
            artifact_id=manifest.selection_dossier_id,
            artifact_ref=selection_ref,
        ),
        SimpleNamespace(
            name="certification_policy",
            artifact_id="certification-policy:v1",
            artifact_ref=registry_ref,
        ),
    )
    candidate = SimpleNamespace(
        candidate_id="release-candidate:v1",
        dependencies=dependencies,
        dependency=lambda name: next(
            item for item in dependencies if item.name == name
        ),
        git_identity=SimpleNamespace(
            repository_url=repository_url, commit_sha=candidate_commit
        ),
        frozen_at_utc="2026-08-26T04:00:00Z",
    )
    monkeypatch.setattr(
        evaluation_module,
        "read_protected_release_holdout_manifest",
        lambda _: manifest,
    )
    monkeypatch.setattr(
        evaluation_module, "read_release_candidate_freeze", lambda _: graph
    )
    monkeypatch.setattr(
        evaluation_module,
        "read_reconstruction_release_candidate",
        lambda _: candidate,
    )
    monkeypatch.setattr(
        evaluation_module,
        "read_reverse_degradation_benchmark_corpus",
        lambda _: corpus,
    )

    authorization = authorize_reconstruction_release_holdout(
        manifest_ref,
        graph_ref,
        candidate_ref,
        policy_ref,
        corpus_ref,
        repository_root=repo,
        evidence_git_commit_sha=evidence_commit,
        authorized_at_utc="2026-08-26T05:00:00Z",
    )

    assert authorization.evidence_git_commit_sha == evidence_commit
    assert authorization.ensemble_member_ids == members

    Path(corpus_ref.path).write_text('{"changed":true}\n', encoding="utf-8")
    changed_corpus_ref = artifact_ref_for_file(
        corpus_ref.path, kind=corpus_ref.kind
    )
    with pytest.raises(ValueError, match="benchmark_corpus binding differs"):
        authorize_reconstruction_release_holdout(
            manifest_ref,
            graph_ref,
            candidate_ref,
            policy_ref,
            changed_corpus_ref,
            repository_root=repo,
            evidence_git_commit_sha=evidence_commit,
            authorized_at_utc="2026-08-26T05:00:00Z",
        )
    benchmark_dependency = next(
        item for item in dependencies if item.name == "benchmark_corpus"
    )
    benchmark_dependency.artifact_ref = changed_corpus_ref
    with pytest.raises(ValueError, match="committed holdout evidence differs"):
        authorize_reconstruction_release_holdout(
            manifest_ref,
            graph_ref,
            candidate_ref,
            policy_ref,
            changed_corpus_ref,
            repository_root=repo,
            evidence_git_commit_sha=evidence_commit,
            authorized_at_utc="2026-08-26T05:00:00Z",
        )

"""Tests for sealed one-time release-holdout governance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import NoReturn

import pytest

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.release_holdout import (
    ProtectedReleaseHoldoutManifestV1,
    ProtectedReleaseHoldoutWindowV1,
    ReleaseCandidateFreezeV1,
    ReleaseHoldoutAccessPolicyV1,
    ReleaseHoldoutAlreadyConsumedError,
    ReleaseHoldoutAuditStatus,
    ReleaseHoldoutAuthorizationV1,
    ReleaseHoldoutDevelopmentUnitV1,
    ReleaseHoldoutEvaluationOutcome,
    ReleaseHoldoutEvaluationResultV1,
    audit_release_holdout_coverage,
    audit_release_holdout_leakage,
    authorize_release_holdout,
    build_protected_release_holdout_manifest,
    execute_release_holdout_once,
    freeze_release_candidate,
    read_protected_release_holdout_manifest,
    read_release_candidate_freeze,
    read_release_holdout_access_policy,
    read_release_holdout_authorization,
    read_release_holdout_evaluation_receipt,
    read_release_holdout_retirement_marker,
    retire_release_holdout,
    write_protected_release_holdout_manifest,
    write_release_candidate_freeze,
    write_release_holdout_access_policy,
    write_release_holdout_authorization,
    write_release_holdout_retirement_marker,
)

_DAY_NS = 24 * 60 * 60 * 1_000_000_000
_FROZEN_STAGES = (
    "fit",
    "preprocess",
    "support_tuning",
    "smoothing",
    "engine_selection",
    "scenario_policy",
    "adaptive_policy",
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _sketch(label: str) -> str:
    return _sha(label)[:16]


def _artifact(
    tmp_path: Path,
    name: str,
    *,
    metadata: dict[str, object] | None = None,
) -> ArtifactRef:
    path = tmp_path / name
    path.write_text(json.dumps({"artifact": name}), encoding="utf-8")
    return artifact_ref_for_file(
        path,
        kind="test_evidence",
        metadata=metadata,
    )


def _development_unit(
    *,
    label: str = "development",
    start_ns: int = 1,
    end_ns: int = _DAY_NS,
) -> ReleaseHoldoutDevelopmentUnitV1:
    return ReleaseHoldoutDevelopmentUnitV1(
        split_role="validation",
        period="202501",
        start_ns=start_ns,
        end_ns=end_ns,
        source_partition_ids=(f"partition:{label}",),
        source_hashes={"eurusd": _sha(f"source-hash:{label}")},
        source_signature_sha256=_sha(f"source-signature:{label}"),
        motif_signature_sha256=_sha(f"motif-signature:{label}"),
        context_signature_sha256=_sha(f"context-signature:{label}"),
        source_neighbor_sketch=_sketch(f"source-sketch:{label}"),
        motif_neighbor_sketch=_sketch(f"motif-sketch:{label}"),
        context_neighbor_sketch=_sketch(f"context-sketch:{label}"),
        cohesion_group_ids=(f"cohesion:{label}",),
        anchor_neighborhood_ids=(f"anchor:{label}",),
        context_event_ids=(f"context-event:{label}",),
    )


def _window(
    index: int,
    *,
    epoch: str,
    session: str,
    event: str,
    scenario: str,
    alignment: str,
    deficit: str,
) -> ProtectedReleaseHoldoutWindowV1:
    label = f"holdout-{index}"
    start_ns = 100 * _DAY_NS + index * 9 * _DAY_NS
    return ProtectedReleaseHoldoutWindowV1(
        period=f"2026{index + 1:02d}",
        start_ns=start_ns,
        end_ns=start_ns + _DAY_NS,
        source_partition_ids=(f"partition:{label}",),
        source_hashes={"eurusd": _sha(f"source-hash:{label}")},
        source_signature_sha256=_sha(f"source-signature:{label}"),
        motif_signature_sha256=_sha(f"motif-signature:{label}"),
        context_signature_sha256=_sha(f"context-signature:{label}"),
        source_neighbor_sketch=_sketch(f"source-sketch:{label}"),
        motif_neighbor_sketch=_sketch(f"motif-sketch:{label}"),
        context_neighbor_sketch=_sketch(f"context-sketch:{label}"),
        cohesion_group_ids=(f"cohesion:{label}",),
        anchor_neighborhood_ids=(f"anchor:{label}",),
        context_event_ids=(f"context-event:{label}",),
        symbol_event_counts={"eurusd": 100 + index},
        epoch_stratum=epoch,
        session_stratum=session,
        event_stratum=event,
        observation_scenario_id=scenario,
        alignment_kind=alignment,
        deficit_stratum=deficit,
    )


def _windows() -> tuple[ProtectedReleaseHoldoutWindowV1, ...]:
    epochs = ("early", "qualified_transition", "modern", "modern")
    sessions = ("asia", "london", "new_york", "overlap_closure")
    events = ("ordinary", "event", "ordinary", "event")
    scenarios = (
        "high_retention_low_infill",
        "central_fitted_retention",
        "low_retention_high_infill",
        "central_fitted_retention",
    )
    alignments = ("exact", "bounded_nearest", "exact", "bounded_nearest")
    deficits = ("low", "median", "high", "median")
    return tuple(
        _window(
            index,
            epoch=epochs[index],
            session=sessions[index],
            event=events[index],
            scenario=scenarios[index],
            alignment=alignments[index],
            deficit=deficits[index],
        )
        for index in range(4)
    )


def _manifest(
    tmp_path: Path,
) -> tuple[ReleaseHoldoutAccessPolicyV1, ProtectedReleaseHoldoutManifestV1]:
    selection_ref = _artifact(
        tmp_path,
        "selection.json",
        metadata={"dossier_id": "selection:dossier:v1"},
    )
    policy = ReleaseHoldoutAccessPolicyV1()
    development = (_development_unit(),)
    manifest = build_protected_release_holdout_manifest(
        policy,
        _windows(),
        development,
        selection_dossier_id="selection:dossier:v1",
        selection_dossier_ref=selection_ref,
        source_cutoff_ns=development[0].end_ns,
        claim_scope="v2.5-marked-hawkes-release-decision",
        frozen_at_utc="2026-08-20T12:00:00Z",
    )
    return policy, manifest


def _graph(
    tmp_path: Path, manifest: ProtectedReleaseHoldoutManifestV1
) -> ReleaseCandidateFreezeV1:
    stage_artifacts = {
        stage: _artifact(
            tmp_path,
            f"{stage}.json",
            metadata={"input_roles": ["calibration", "validation"]},
        )
        for stage in _FROZEN_STAGES
    }
    return freeze_release_candidate(
        manifest,
        candidate_id="release-candidate:v2.5.0",
        stage_artifacts=stage_artifacts,
        frozen_at_utc="2026-08-20T13:00:00Z",
    )


def _authorization(
    tmp_path: Path,
) -> tuple[
    ProtectedReleaseHoldoutManifestV1,
    ReleaseCandidateFreezeV1,
    ReleaseHoldoutAuthorizationV1,
]:
    policy, manifest = _manifest(tmp_path)
    graph = _graph(tmp_path, manifest)
    policy_ref = write_release_holdout_access_policy(policy, tmp_path)
    manifest_ref = write_protected_release_holdout_manifest(manifest, tmp_path)
    graph_ref = write_release_candidate_freeze(graph, tmp_path)
    authorization = authorize_release_holdout(
        manifest_ref,
        graph_ref,
        authorized_at_utc="2026-08-20T14:00:00Z",
    )
    authorization_ref = write_release_holdout_authorization(
        authorization, tmp_path
    )
    assert read_release_holdout_access_policy(policy_ref.path) == policy
    assert (
        read_protected_release_holdout_manifest(manifest_ref.path) == manifest
    )
    assert read_release_candidate_freeze(graph_ref.path) == graph
    assert (
        read_release_holdout_authorization(authorization_ref.path)
        == authorization
    )
    return manifest, graph, authorization


def test_manifest_is_row_free_content_addressed_and_coverage_complete(
    tmp_path: Path,
) -> None:
    policy, manifest = _manifest(tmp_path)

    assert manifest.leakage_audit.status is ReleaseHoldoutAuditStatus.PASS
    assert manifest.coverage_audit.status is ReleaseHoldoutAuditStatus.PASS
    assert manifest.to_dict()["results_opened"] is False
    assert manifest.to_dict()["candidate_identity_present"] is False
    assert "bid" not in manifest.to_json()
    assert "ask" not in manifest.to_json()
    assert '"events":' not in manifest.to_json()
    assert policy.to_dict()["maximum_evaluations"] == 1
    assert policy.to_dict()["selection_role_permitted"] is False

    ref = write_protected_release_holdout_manifest(manifest, tmp_path)
    assert ref.sha256 in Path(ref.path).name
    assert read_protected_release_holdout_manifest(ref.path) == manifest


def test_leakage_audit_detects_exact_near_temporal_and_group_reuse() -> None:
    policy = replace(
        ReleaseHoldoutAccessPolicyV1(),
        near_neighbor_hamming_distance=4,
        policy_id="",
    )
    development = _development_unit()
    baseline = _windows()[0]
    leaky = replace(
        baseline,
        start_ns=development.end_ns + _DAY_NS,
        end_ns=development.end_ns + 2 * _DAY_NS,
        source_partition_ids=development.source_partition_ids,
        source_hashes=development.source_hashes,
        source_signature_sha256=development.source_signature_sha256,
        motif_neighbor_sketch=development.motif_neighbor_sketch[:-1] + "0",
        cohesion_group_ids=development.cohesion_group_ids,
        window_id="",
    )

    audit = audit_release_holdout_leakage(policy, (leaky,), (development,))

    assert audit.status is ReleaseHoldoutAuditStatus.FAIL
    assert any(
        code.startswith("source_partition_reuse:")
        for code in audit.finding_codes
    )
    assert any(
        code.startswith("source_hash_reuse:") for code in audit.finding_codes
    )
    assert any(
        code.startswith("exact_source_duplicate:")
        for code in audit.finding_codes
    )
    assert any(
        code.startswith("near_motif_duplicate:") for code in audit.finding_codes
    )
    assert any(
        code.startswith("temporal_neighbor:") for code in audit.finding_codes
    )
    assert any(
        code.startswith("cross_role_cohesion_group_reuse:")
        for code in audit.finding_codes
    )


def test_missing_coverage_is_insufficient_not_a_weakened_split() -> None:
    policy = ReleaseHoldoutAccessPolicyV1()
    audit = audit_release_holdout_coverage(policy, (_windows()[0],))

    assert audit.status is ReleaseHoldoutAuditStatus.INSUFFICIENT_EVIDENCE
    assert "session:london" in audit.missing_strata
    assert audit.to_dict()["split_rules_weakened"] is False


def test_candidate_graph_forbids_holdout_input_at_every_policy_stage(
    tmp_path: Path,
) -> None:
    _, manifest = _manifest(tmp_path)
    good = _graph(tmp_path, manifest)
    assert set(good.stage_artifacts) == set(_FROZEN_STAGES)
    assert good.to_dict()["holdout_input_role"] is False

    for stage in _FROZEN_STAGES:
        bad = dict(good.stage_artifacts)
        ref = bad[stage]
        bad[stage] = replace(
            ref,
            metadata={"input_roles": ["protected_release_holdout"]},
        )
        with pytest.raises(ValueError, match="used holdout input"):
            ReleaseCandidateFreezeV1(
                manifest_id=manifest.manifest_id,
                selection_dossier_id=manifest.selection_dossier_id,
                candidate_id="bad-candidate",
                stage_artifacts=bad,
                frozen_at_utc="2026-08-20T13:00:00Z",
            )


def test_holdout_executes_exactly_once_and_retires(tmp_path: Path) -> None:
    manifest, graph, authorization = _authorization(tmp_path)
    calls: list[str] = []

    def evaluate(
        _: ProtectedReleaseHoldoutManifestV1,
        __: ReleaseCandidateFreezeV1,
    ) -> ReleaseHoldoutEvaluationResultV1:
        calls.append("opened")
        report_ref = _artifact(tmp_path, "release-report.json")
        return ReleaseHoldoutEvaluationResultV1(
            manifest_id=manifest.manifest_id,
            graph_id=graph.graph_id,
            candidate_id=graph.candidate_id,
            outcome=ReleaseHoldoutEvaluationOutcome.PASSED,
            report_ref=report_ref,
            reason_codes=("all_frozen_release_gates_passed",),
        )

    receipt, receipt_ref = execute_release_holdout_once(
        authorization,
        tmp_path / "ledger",
        evaluate,
        evaluated_at_utc="2026-08-20T15:00:00Z",
    )

    assert calls == ["opened"]
    assert receipt.outcome is ReleaseHoldoutEvaluationOutcome.PASSED
    assert receipt.to_dict()["holdout_selection_role"] is False
    assert read_release_holdout_evaluation_receipt(receipt_ref.path) == receipt
    with pytest.raises(ReleaseHoldoutAlreadyConsumedError):
        execute_release_holdout_once(
            authorization,
            tmp_path / "ledger",
            evaluate,
            evaluated_at_utc="2026-08-20T15:01:00Z",
        )
    assert calls == ["opened"]

    marker = retire_release_holdout(
        manifest,
        receipt,
        retired_at_utc="2026-08-20T16:00:00Z",
    )
    marker_ref = write_release_holdout_retirement_marker(marker, tmp_path)
    assert marker.to_dict()["reuse_permitted"] is False
    assert read_release_holdout_retirement_marker(marker_ref.path) == marker


def test_callback_failure_consumes_holdout_and_requires_successor(
    tmp_path: Path,
) -> None:
    manifest, _, authorization = _authorization(tmp_path)
    calls = 0

    def fail(
        _: ProtectedReleaseHoldoutManifestV1,
        __: ReleaseCandidateFreezeV1,
    ) -> NoReturn:
        nonlocal calls
        calls += 1
        raise RuntimeError("protected result must not be retried")

    receipt, _ = execute_release_holdout_once(
        authorization,
        tmp_path / "failed-ledger",
        fail,
        evaluated_at_utc="2026-08-20T15:00:00Z",
    )

    assert calls == 1
    assert (
        receipt.outcome is ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE
    )
    assert receipt.operational_error_type == "RuntimeError"
    with pytest.raises(ReleaseHoldoutAlreadyConsumedError):
        execute_release_holdout_once(
            authorization,
            tmp_path / "failed-ledger",
            fail,
            evaluated_at_utc="2026-08-20T15:01:00Z",
        )
    with pytest.raises(ValueError, match="text is required"):
        retire_release_holdout(
            manifest,
            receipt,
            retired_at_utc="2026-08-20T16:00:00Z",
        )
    marker = retire_release_holdout(
        manifest,
        receipt,
        retired_at_utc="2026-08-20T16:00:00Z",
        successor_manifest_id="fresh-successor:v2.5.1",
    )
    assert marker.to_dict()["same_holdout_tuning_permitted"] is False


def test_authorization_rejects_leaky_or_underpowered_manifest(
    tmp_path: Path,
) -> None:
    policy, manifest = _manifest(tmp_path)
    underpowered = replace(
        manifest,
        windows=(manifest.windows[0],),
        coverage_audit=audit_release_holdout_coverage(
            policy, (manifest.windows[0],)
        ),
        leakage_audit=audit_release_holdout_leakage(
            policy,
            (manifest.windows[0],),
            (_development_unit(),),
        ),
        manifest_id="",
    )
    graph = _graph(tmp_path, underpowered)
    manifest_ref = write_protected_release_holdout_manifest(
        underpowered, tmp_path
    )
    graph_ref = write_release_candidate_freeze(graph, tmp_path)

    with pytest.raises(ValueError, match="insufficient coverage"):
        authorize_release_holdout(
            manifest_ref,
            graph_ref,
            authorized_at_utc="2026-08-20T14:00:00Z",
        )


def test_content_addressed_manifest_rejects_tampering(tmp_path: Path) -> None:
    _, manifest = _manifest(tmp_path)
    ref = write_protected_release_holdout_manifest(manifest, tmp_path)
    path = Path(ref.path)
    path.write_text(path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    with pytest.raises(ValueError, match="content address differs"):
        read_protected_release_holdout_manifest(path)

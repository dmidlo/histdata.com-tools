"""Tests for reconstruction information modes and leakage auditing."""

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from histdatacom.synthetic import (
    RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION,
    RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION,
    RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION,
    RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION,
    RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION,
    RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION,
    InformationAuditReportV1,
    InformationAuditRule,
    InformationInputKind,
    InformationLeakageError,
    InformationMode,
    InformationScope,
    InformationSplitKind,
    InformationStage,
    ReconstructionInformationInputV1,
    ReconstructionInformationManifestV1,
    ReconstructionInformationPolicyV1,
    ReconstructionInformationSplitV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    audit_reconstruction_information,
    plan_reconstruction_windows,
    reconstruction_information_window_plan_id,
    require_reconstruction_information_audit,
)

BASE_NS = 1_700_000_000_000_000_000
SYMBOLS = ("eurusd", "gbpusd", "eurgbp")


def _policy(
    mode: InformationMode = InformationMode.EX_ANTE_SIMULATION,
    *,
    lookahead_ns: int = 0,
    max_findings: int = 128,
) -> ReconstructionInformationPolicyV1:
    return ReconstructionInformationPolicyV1(
        information_mode=mode,
        max_allowed_lookahead_ns=lookahead_ns,
        max_retained_findings=max_findings,
    )


def _run(
    policy: ReconstructionInformationPolicyV1,
    *,
    bind_policy: bool = True,
) -> ReconstructionRunV1:
    configuration_ids = ["config:sha256:reconstruction-v1"]
    if bind_policy:
        configuration_ids.append(policy.policy_id)
    return ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source:sha256:historical-v1",),
        configuration_ids=tuple(configuration_ids),
        ensemble_member_ids=("member-000",),
        base_seed=20260713,
    )


def _splits() -> tuple[ReconstructionInformationSplitV1, ...]:
    return (
        ReconstructionInformationSplitV1(
            InformationSplitKind.TRAIN,
            BASE_NS,
            BASE_NS + 100,
        ),
        ReconstructionInformationSplitV1(
            InformationSplitKind.CALIBRATION,
            BASE_NS + 100,
            BASE_NS + 200,
        ),
        ReconstructionInformationSplitV1(
            InformationSplitKind.VALIDATION,
            BASE_NS + 200,
            BASE_NS + 300,
        ),
    )


def _windows(
    run: ReconstructionRunV1,
    *,
    right_lookahead_ns: int = 0,
) -> tuple[ReconstructionWindowV1, ...]:
    return plan_reconstruction_windows(
        run,
        ensemble_member_id="member-000",
        start_ns=BASE_NS,
        end_ns=BASE_NS + 300,
        window_size_ns=100,
        right_lookahead_ns=right_lookahead_ns,
    )


def _input(
    run: ReconstructionRunV1,
    *,
    artifact_id: str,
    mode: InformationMode = InformationMode.EX_ANTE_SIMULATION,
    input_kind: InformationInputKind = InformationInputKind.EXTERNAL,
    stage: InformationStage = InformationStage.SOURCE,
    scope: InformationScope = InformationScope.POINT_IN_TIME,
    event_offset: int = 10,
    available_offset: int = 10,
    used_offset: int = 90,
    observation_start_offset: int | None = None,
    observation_end_offset: int | None = None,
    vintage_id: str = "vintage:initial",
    reason: str = "declared synthetic test input",
    revision_sequence: int = 0,
    supersedes_input_id: str | None = None,
    allowed_lookahead_ns: int = 0,
    parent_input_ids: tuple[str, ...] = (),
    split_kind: InformationSplitKind | None = None,
) -> ReconstructionInformationInputV1:
    return ReconstructionInformationInputV1(
        run_id=run.run_id,
        artifact_id=artifact_id,
        information_mode=mode,
        input_kind=input_kind,
        stage=stage,
        scope=scope,
        event_time_ns=BASE_NS + event_offset,
        available_at_ns=BASE_NS + available_offset,
        used_at_ns=BASE_NS + used_offset,
        observation_start_ns=BASE_NS
        + (
            event_offset
            if observation_start_offset is None
            else observation_start_offset
        ),
        observation_end_ns=BASE_NS
        + (
            event_offset
            if observation_end_offset is None
            else observation_end_offset
        ),
        vintage_id=vintage_id,
        reason=reason,
        revision_sequence=revision_sequence,
        supersedes_input_id=supersedes_input_id,
        allowed_lookahead_ns=allowed_lookahead_ns,
        parent_input_ids=parent_input_ids,
        split_kind=split_kind,
    )


def _valid_bundle(
    *,
    max_findings: int = 128,
) -> tuple[
    ReconstructionInformationPolicyV1,
    ReconstructionRunV1,
    ReconstructionInformationManifestV1,
    tuple[ReconstructionWindowV1, ...],
]:
    policy = _policy(max_findings=max_findings)
    run = _run(policy)
    windows = _windows(run)
    source = _input(
        run,
        artifact_id="artifact:source-ticks",
        observation_start_offset=0,
        observation_end_offset=90,
        split_kind=InformationSplitKind.TRAIN,
    )
    model = _input(
        run,
        artifact_id="artifact:model-fit",
        input_kind=InformationInputKind.DERIVED,
        stage=InformationStage.MODEL_FIT,
        event_offset=90,
        available_offset=90,
        used_offset=99,
        observation_start_offset=0,
        observation_end_offset=90,
        parent_input_ids=(source.input_id,),
        split_kind=InformationSplitKind.TRAIN,
    )
    calibration = _input(
        run,
        artifact_id="artifact:calibration",
        input_kind=InformationInputKind.DERIVED,
        stage=InformationStage.CALIBRATION,
        event_offset=150,
        available_offset=150,
        used_offset=175,
        observation_start_offset=100,
        observation_end_offset=150,
        parent_input_ids=(model.input_id,),
        split_kind=InformationSplitKind.CALIBRATION,
    )
    validation = _input(
        run,
        artifact_id="artifact:validation",
        input_kind=InformationInputKind.DERIVED,
        stage=InformationStage.VALIDATION,
        event_offset=250,
        available_offset=250,
        used_offset=275,
        observation_start_offset=200,
        observation_end_offset=250,
        parent_input_ids=(calibration.input_id,),
        split_kind=InformationSplitKind.VALIDATION,
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(windows),
        inputs=(validation, source, calibration, model),
        splits=_splits(),
    )
    return policy, run, manifest, windows


def _rules(report: InformationAuditReportV1) -> set[InformationAuditRule]:
    return {finding.rule_id for finding in report.findings}


def _with_inputs(
    manifest: ReconstructionInformationManifestV1,
    *inputs: ReconstructionInformationInputV1,
) -> ReconstructionInformationManifestV1:
    return replace(
        manifest,
        inputs=manifest.inputs + tuple(inputs),
        manifest_id="",
    )


def test_ex_ante_manifest_is_run_bound_serializable_and_strategy_valid() -> (
    None
):
    policy, run, manifest, windows = _valid_bundle()

    report = require_reconstruction_information_audit(
        manifest,
        policy,
        run=run,
        windows=windows,
    )

    assert report.accepted is True
    assert report.total_violation_count == 0
    assert report.valid_for_strategy_usefulness_claim is True
    assert "point_in_time_simulation" in report.valid_for
    assert (
        ReconstructionInformationPolicyV1.from_json(policy.to_json()) == policy
    )
    assert ReconstructionInformationManifestV1.from_json(
        manifest.to_json()
    ) == (manifest)
    assert InformationAuditReportV1.from_json(report.to_json()) == report
    assert tuple(item.input_id for item in manifest.inputs) == tuple(
        sorted(item.input_id for item in manifest.inputs)
    )


def test_diagnostic_retention_does_not_change_policy_run_or_seed_identity() -> (
    None
):
    narrow = _policy(max_findings=1)
    verbose = _policy(max_findings=256)
    narrow_run = _run(narrow)
    verbose_run = _run(verbose)

    assert narrow.policy_id == verbose.policy_id
    assert narrow_run.run_id == verbose_run.run_id
    assert narrow_run.seed_for(
        "member-000", "anchor-42"
    ) == verbose_run.seed_for("member-000", "anchor-42")
    assert narrow.to_dict()["max_retained_findings"] == 1
    assert verbose.to_dict()["max_retained_findings"] == 256


def test_ex_post_future_anchor_is_labeled_bounded_and_not_strategy_valid() -> (
    None
):
    policy = _policy(
        InformationMode.EX_POST_RECONSTRUCTION,
        lookahead_ns=100,
    )
    run = _run(policy)
    windows = _windows(run, right_lookahead_ns=50)
    anchor = _input(
        run,
        artifact_id="artifact:future-anchor",
        mode=policy.information_mode,
        stage=InformationStage.CARVING,
        scope=InformationScope.FUTURE_ANCHOR,
        event_offset=50,
        available_offset=50,
        used_offset=0,
        allowed_lookahead_ns=50,
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(windows),
        inputs=(anchor,),
        splits=_splits(),
    )

    report = require_reconstruction_information_audit(
        manifest,
        policy,
        run=run,
        windows=windows,
    )

    assert report.accepted is True
    assert report.valid_for_strategy_usefulness_claim is False
    assert "historically_informed_reconstruction" in report.valid_for
    assert "prospective_simulation" in report.invalid_for


def test_revision_unavailable_at_use_is_rejected_in_ex_ante_mode() -> None:
    policy = _policy()
    run = _run(policy)
    windows = _windows(run)
    initial = _input(
        run,
        artifact_id="macro:gdp:2025q4:initial",
        event_offset=20,
        available_offset=30,
        used_offset=60,
        vintage_id="2026-01-30-initial",
    )
    revision = _input(
        run,
        artifact_id="macro:gdp:2025q4:revision-1",
        scope=InformationScope.REVISION,
        event_offset=20,
        available_offset=80,
        used_offset=60,
        vintage_id="2026-03-30-revision-1",
        revision_sequence=1,
        supersedes_input_id=initial.input_id,
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(windows),
        inputs=(initial, revision),
        splits=_splits(),
    )

    report = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=windows,
    )

    assert report.accepted is False
    assert InformationAuditRule.EX_ANTE_REVISION_NOT_AVAILABLE in _rules(report)
    finding = next(
        item
        for item in report.findings
        if item.rule_id is InformationAuditRule.EX_ANTE_REVISION_NOT_AVAILABLE
    )
    assert finding.evidence["vintage_id"] == "2026-03-30-revision-1"


@pytest.mark.parametrize(
    ("scope", "expected_rule"),
    (
        (
            InformationScope.FUTURE_ANCHOR,
            InformationAuditRule.EX_ANTE_FUTURE_ANCHOR,
        ),
        (
            InformationScope.FULL_PERIOD_SUMMARY,
            InformationAuditRule.EX_ANTE_FULL_PERIOD_SUMMARY,
        ),
        (
            InformationScope.GLOBAL_NORMALIZATION,
            InformationAuditRule.EX_ANTE_GLOBAL_NORMALIZATION,
        ),
    ),
)
def test_ex_post_only_scopes_cannot_be_reused_as_ex_ante_features(
    scope: InformationScope,
    expected_rule: InformationAuditRule,
) -> None:
    policy, run, manifest, windows = _valid_bundle()
    source = manifest.inputs[0]
    feature = _input(
        run,
        artifact_id=f"artifact:{scope.value}",
        input_kind=InformationInputKind.DERIVED,
        stage=InformationStage.FEATURE,
        scope=scope,
        event_offset=50,
        available_offset=80,
        used_offset=90,
        observation_start_offset=0,
        observation_end_offset=90,
        parent_input_ids=(source.input_id,),
        split_kind=InformationSplitKind.TRAIN,
    )

    report = audit_reconstruction_information(
        _with_inputs(manifest, feature),
        policy,
        run=run,
        windows=windows,
    )

    assert expected_rule in _rules(report)


def test_future_event_and_motif_selection_leakage_have_distinct_rules() -> None:
    policy, run, manifest, windows = _valid_bundle()
    source = manifest.inputs[0]
    future_event = _input(
        run,
        artifact_id="calendar:event:future",
        stage=InformationStage.CALENDAR_CONTEXT,
        event_offset=95,
        available_offset=80,
        used_offset=90,
    )
    motif = _input(
        run,
        artifact_id="motif:selected-from-future",
        input_kind=InformationInputKind.DERIVED,
        stage=InformationStage.MOTIF_SELECTION,
        scope=InformationScope.EMPIRICAL_MOTIF,
        event_offset=50,
        available_offset=80,
        used_offset=90,
        observation_start_offset=0,
        observation_end_offset=95,
        parent_input_ids=(source.input_id,),
        split_kind=InformationSplitKind.TRAIN,
    )

    report = audit_reconstruction_information(
        _with_inputs(manifest, future_event, motif),
        policy,
        run=run,
        windows=windows,
    )

    assert InformationAuditRule.EX_ANTE_FUTURE_EVENT in _rules(report)
    assert InformationAuditRule.EX_ANTE_MOTIF_SELECTION_LEAKAGE in _rules(
        report
    )


def test_same_run_cannot_mix_information_modes() -> None:
    policy, run, manifest, windows = _valid_bundle()
    mixed = _input(
        run,
        artifact_id="artifact:mixed-mode",
        mode=InformationMode.EX_POST_RECONSTRUCTION,
        event_offset=20,
        available_offset=20,
        used_offset=30,
    )

    report = audit_reconstruction_information(
        _with_inputs(manifest, mixed),
        policy,
        run=run,
        windows=windows,
    )

    assert InformationAuditRule.INPUT_MODE_MISMATCH in _rules(report)


def test_exact_window_plan_and_right_lookahead_are_audited() -> None:
    policy, run, manifest, windows = _valid_bundle()
    leaking_windows = _windows(run, right_lookahead_ns=1)

    mismatch = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=leaking_windows,
    )
    empty = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=(),
    )

    assert reconstruction_information_window_plan_id(
        tuple(reversed(windows))
    ) == (manifest.window_plan_id)
    assert InformationAuditRule.WINDOW_PLAN_MISMATCH in _rules(mismatch)
    assert InformationAuditRule.WINDOW_LOOKAHEAD_EXCEEDS_POLICY in _rules(
        mismatch
    )
    assert InformationAuditRule.EX_ANTE_WINDOW_LOOKAHEAD in _rules(mismatch)
    assert InformationAuditRule.WINDOW_PLAN_EMPTY in _rules(empty)

    shifted = replace(
        windows[1],
        core_start_ns=windows[1].core_start_ns + 1,
        window_id="",
        synchronization_unit_id="",
    )
    gapped_windows = (windows[0], shifted, windows[2])
    gapped_manifest = replace(
        manifest,
        window_plan_id=reconstruction_information_window_plan_id(
            gapped_windows
        ),
        manifest_id="",
    )
    gapped = audit_reconstruction_information(
        gapped_manifest,
        policy,
        run=run,
        windows=gapped_windows,
    )
    assert InformationAuditRule.WINDOW_PLAN_INVALID in _rules(gapped)


def test_window_plan_must_cover_every_run_ensemble_member() -> None:
    policy = _policy()
    run = ReconstructionRunV1(
        symbols=SYMBOLS,
        source_version_ids=("source:sha256:historical-v1",),
        configuration_ids=(policy.policy_id,),
        ensemble_member_ids=("member-000", "member-001"),
        base_seed=20260713,
    )
    windows = _windows(run)
    source = _input(run, artifact_id="artifact:source")
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(windows),
        inputs=(source,),
        splits=_splits(),
    )

    report = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=windows,
    )

    assert InformationAuditRule.WINDOW_MEMBER_MISSING in _rules(report)


def test_policy_must_be_bound_to_the_reconstruction_run() -> None:
    policy, _, manifest, _ = _valid_bundle()
    unbound_run = _run(policy, bind_policy=False)
    unbound_windows = _windows(unbound_run)
    rebound_inputs = tuple(
        replace(item, run_id=unbound_run.run_id, input_id="")
        for item in manifest.inputs
    )
    rebound_manifest = ReconstructionInformationManifestV1(
        run_id=unbound_run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(
            unbound_windows
        ),
        inputs=rebound_inputs,
        splits=manifest.splits,
    )

    report = audit_reconstruction_information(
        rebound_manifest,
        policy,
        run=unbound_run,
        windows=unbound_windows,
    )

    assert InformationAuditRule.POLICY_NOT_BOUND_TO_RUN in _rules(report)


def test_splits_are_required_ordered_and_stage_specific() -> None:
    policy, run, manifest, windows = _valid_bundle()
    model = next(
        item
        for item in manifest.inputs
        if item.stage is InformationStage.MODEL_FIT
    )
    no_split_model = replace(model, split_kind=None, input_id="")
    inputs = tuple(
        no_split_model if item.input_id == model.input_id else item
        for item in manifest.inputs
    )
    unordered = replace(
        manifest,
        inputs=inputs,
        splits=tuple(reversed(manifest.splits)),
        manifest_id="",
    )

    report = audit_reconstruction_information(
        unordered,
        policy,
        run=run,
        windows=windows,
    )

    assert InformationAuditRule.SPLIT_DECLARATION_ORDER in _rules(report)
    assert InformationAuditRule.INPUT_SPLIT_MISSING in _rules(report)


def test_missing_parent_and_revision_vintage_fail_graph_audit() -> None:
    policy, run, manifest, windows = _valid_bundle()
    derived = _input(
        run,
        artifact_id="artifact:orphan-derived",
        input_kind=InformationInputKind.DERIVED,
        stage=InformationStage.FEATURE,
        parent_input_ids=("information-input:sha256:missing",),
    )
    revision = _input(
        run,
        artifact_id="macro:revision:orphan",
        scope=InformationScope.REVISION,
        vintage_id="revision-2",
        revision_sequence=2,
        supersedes_input_id="information-input:sha256:missing-vintage",
    )

    report = audit_reconstruction_information(
        _with_inputs(manifest, derived, revision),
        policy,
        run=run,
        windows=windows,
    )

    assert InformationAuditRule.MISSING_PARENT_INPUT in _rules(report)
    assert InformationAuditRule.REVISION_PARENT_MISSING in _rules(report)


def test_findings_are_deterministic_bounded_and_fail_closed() -> None:
    policy, run, manifest, windows = _valid_bundle(max_findings=2)
    violations = tuple(
        _input(
            run,
            artifact_id=f"artifact:future:{ordinal}",
            stage=InformationStage.NEWS_CONTEXT,
            event_offset=95 + ordinal,
            available_offset=95 + ordinal,
            used_offset=90,
        )
        for ordinal in range(5)
    )
    invalid = _with_inputs(manifest, *violations)

    first = audit_reconstruction_information(
        invalid,
        policy,
        run=run,
        windows=windows,
    )
    second = audit_reconstruction_information(
        invalid,
        policy,
        run=run,
        windows=windows,
    )

    assert first == second
    assert first.audit_id == second.audit_id
    assert first.total_violation_count > len(first.findings) == 2
    assert first.evidence_truncated is True
    assert first.valid_for == ()
    assert "strategy_usefulness_claims" in first.invalid_for
    assert first.findings[0].from_json(first.findings[0].to_json()) == (
        first.findings[0]
    )
    with pytest.raises(InformationLeakageError) as caught:
        require_reconstruction_information_audit(
            invalid,
            policy,
            run=run,
            windows=windows,
        )
    assert caught.value.report == first


def test_ex_post_future_use_must_be_labeled_and_within_lookahead() -> None:
    policy = _policy(
        InformationMode.EX_POST_RECONSTRUCTION,
        lookahead_ns=20,
    )
    run = _run(policy)
    windows = _windows(run, right_lookahead_ns=20)
    unlabeled = _input(
        run,
        artifact_id="artifact:unlabeled-future",
        mode=policy.information_mode,
        stage=InformationStage.CARVING,
        event_offset=50,
        available_offset=50,
        used_offset=0,
        allowed_lookahead_ns=10,
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(windows),
        inputs=(unlabeled,),
        splits=_splits(),
    )

    report = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=windows,
    )

    assert InformationAuditRule.EX_POST_UNLABELED_FUTURE_INFORMATION in _rules(
        report
    )
    assert InformationAuditRule.EX_POST_LOOKAHEAD_EXCEEDED in _rules(report)


def test_contract_readers_reject_identity_and_schema_drift() -> None:
    policy, _, manifest, _ = _valid_bundle()
    payload = manifest.to_dict()
    payload["future_key"] = "ignored"
    assert ReconstructionInformationManifestV1.from_dict(payload) == manifest

    wrong_id = dict(payload)
    wrong_id["manifest_id"] = "information-manifest:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="manifest_id does not match"):
        ReconstructionInformationManifestV1.from_dict(wrong_id)

    wrong_schema = policy.to_dict()
    wrong_schema["schema_version"] = (
        "histdatacom.reconstruction-information-policy.v2"
    )
    with pytest.raises(ValueError, match="unsupported schema version"):
        ReconstructionInformationPolicyV1.from_dict(wrong_schema)


def test_contract_validation_requires_complete_temporal_and_revision_metadata() -> (
    None
):
    policy = _policy()
    run = _run(policy)
    with pytest.raises(ValueError, match="event_time_ns must lie inside"):
        _input(
            run,
            artifact_id="artifact:bad-time",
            event_offset=10,
            observation_start_offset=20,
            observation_end_offset=30,
        )
    with pytest.raises(ValueError, match="requires supersedes_input_id"):
        _input(
            run,
            artifact_id="artifact:bad-revision",
            revision_sequence=1,
        )
    with pytest.raises(ValueError, match="requires zero look-ahead"):
        _policy(InformationMode.EX_ANTE_SIMULATION, lookahead_ns=1)


def test_schema_versions_and_json_are_stable_and_explicit() -> None:
    policy, run, manifest, windows = _valid_bundle()
    report = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=windows,
    )
    versions = (
        RECONSTRUCTION_INFORMATION_POLICY_SCHEMA_VERSION,
        RECONSTRUCTION_INFORMATION_INPUT_SCHEMA_VERSION,
        RECONSTRUCTION_INFORMATION_SPLIT_SCHEMA_VERSION,
        RECONSTRUCTION_INFORMATION_MANIFEST_SCHEMA_VERSION,
        RECONSTRUCTION_INFORMATION_AUDIT_FINDING_SCHEMA_VERSION,
        RECONSTRUCTION_INFORMATION_AUDIT_REPORT_SCHEMA_VERSION,
    )

    assert all(version.endswith(".v1") for version in versions)
    assert json.loads(manifest.to_json())["manifest_id"] == manifest.manifest_id
    assert json.loads(report.to_json())["accepted"] is True

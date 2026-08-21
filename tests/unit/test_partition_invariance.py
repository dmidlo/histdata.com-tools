"""Tests for adaptive-window partition-invariance qualification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from itertools import pairwise
from pathlib import Path

import pytest

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.partition_invariance import (
    REQUIRED_PARTITION_METRICS,
    AdaptivePartitionIntervalV1,
    AdaptivePartitionKind,
    AdaptivePartitionSpecV1,
    PartitionInvarianceCaseV1,
    PartitionInvariancePolicyV1,
    PartitionInvarianceQualificationV1,
    PartitionInvarianceRunV1,
    PartitionMetricToleranceV1,
    PartitionQualificationStatus,
    PartitionToleranceSeverity,
    audit_partition_history,
    audit_partition_source_ownership,
    build_partition_seed_ledger,
    energy_distance_squared,
    qualify_partition_invariance,
    read_partition_invariance_qualification,
    write_partition_invariance_qualification,
)

_CANDIDATE_ID = "reconstruction-release-candidate:test-v2.5"
_SOURCE_TIMES = (0, 249, 500, 750, 999)
_CASE_STRATA = (
    (
        "no_split",
        "early_sparse",
        "quiet",
        "ordinary",
        "exact",
        "high_retention_low_infill",
    ),
    (
        "binary_split",
        "qualified_transition",
        "high_activity",
        "event",
        "bounded_nearest",
        "central_fitted_retention",
    ),
    (
        "deep_recursive_split",
        "modern_dense",
        "quiet",
        "event",
        "exact",
        "low_retention_high_infill",
    ),
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _candidate_ref(tmp_path: Path) -> ArtifactRef:
    path = tmp_path / "release-candidate.json"
    if not path.exists():
        path.write_text(
            json.dumps({"candidate_id": _CANDIDATE_ID}), encoding="utf-8"
        )
    return artifact_ref_for_file(
        path,
        kind="reconstruction_release_candidate_v1",
        metadata={"candidate_id": _CANDIDATE_ID},
    )


def _intervals(
    boundaries: tuple[int, ...],
) -> tuple[AdaptivePartitionIntervalV1, ...]:
    return tuple(
        AdaptivePartitionIntervalV1(start_ns=start, end_ns=end)
        for start, end in pairwise(boundaries)
    )


def _partition(
    label: str,
    kind: AdaptivePartitionKind,
    boundaries: tuple[int, ...],
) -> AdaptivePartitionSpecV1:
    return AdaptivePartitionSpecV1(
        parent_span_id=f"source-span:{label}",
        parent_start_ns=0,
        parent_end_ns=1_000,
        kind=kind,
        intervals=_intervals(boundaries),
    )


def _partition_set(
    label: str, split_depth: str
) -> tuple[AdaptivePartitionSpecV1, ...]:
    planner: tuple[int, ...]
    if split_depth == "no_split":
        planner = (0, 1_000)
    elif split_depth == "binary_split":
        planner = (0, 500, 1_000)
    else:
        planner = (0, 250, 500, 750, 1_000)
    finer = tuple(sorted({*planner, *(range(0, 1_001, 125))}))
    return (
        _partition(label, AdaptivePartitionKind.COARSEST, (0, 1_000)),
        _partition(label, AdaptivePartitionKind.PLANNER, planner),
        _partition(label, AdaptivePartitionKind.FINER, finer),
    )


def _source_events(label: str) -> tuple[tuple[str, int], ...]:
    return tuple(
        (f"source-event:{label}:{index}", event_time)
        for index, event_time in enumerate(_SOURCE_TIMES)
    )


def _case(
    tmp_path: Path,
    index: int,
) -> tuple[PartitionInvarianceCaseV1, tuple[tuple[str, int], ...]]:
    (
        split_depth,
        epoch,
        activity,
        context,
        alignment,
        scenario,
    ) = _CASE_STRATA[index]
    label = f"case-{index}"
    events = _source_events(label)
    partitions = _partition_set(label, split_depth)
    source_hash = _sha(f"source-content:{label}")
    return (
        PartitionInvarianceCaseV1(
            candidate_id=_CANDIDATE_ID,
            release_candidate_ref=_candidate_ref(tmp_path),
            source_content_sha256=source_hash,
            model_fit_id="model-fit:accepted-v1",
            observation_scenario_id=scenario,
            split_depth_stratum=split_depth,
            epoch_stratum=epoch,
            activity_stratum=activity,
            context_stratum=context,
            alignment_kind=alignment,
            partitions=partitions,
        ),
        events,
    )


def _policy() -> PartitionInvariancePolicyV1:
    return PartitionInvariancePolicyV1(
        metric_tolerances=tuple(
            PartitionMetricToleranceV1(
                metric_name=name,
                absolute_tolerance=0.1,
                relative_tolerance=0.05,
                severity=(
                    PartitionToleranceSeverity.ADVISORY
                    if name in {"resource_work", "runtime_seconds"}
                    else PartitionToleranceSeverity.HARD
                ),
            )
            for name in sorted(REQUIRED_PARTITION_METRICS)
        ),
        energy_distance_hard_limit=0.1,
        energy_distance_advisory_limit=0.05,
    )


def _history(
    partition: AdaptivePartitionSpecV1,
    events: tuple[tuple[str, int], ...],
) -> dict[str, tuple[tuple[str, int], ...]]:
    return {
        interval.interval_id: tuple(
            event
            for event in events
            if interval.start_ns - 1_000 <= event[1] < interval.start_ns
        )
        for interval in partition.intervals
    }


def _run(
    case: PartitionInvarianceCaseV1,
    events: tuple[tuple[str, int], ...],
    kind: AdaptivePartitionKind,
    replicate: int,
) -> PartitionInvarianceRunV1:
    partition = case.partition(kind)
    member_id = f"semantic-member:{replicate}"
    ledger = build_partition_seed_ledger(
        partition,
        candidate_id=case.candidate_id,
        model_fit_id=case.model_fit_id,
        observation_scenario_id=case.observation_scenario_id,
        semantic_member_id=member_id,
        base_seed=10_000 + replicate,
    )
    ownership = audit_partition_source_ownership(
        partition,
        events,
        source_content_sha256=case.source_content_sha256,
        anchor_event_ids=(events[2][0], events[-1][0]),
    )
    history = audit_partition_history(
        partition,
        _history(partition, events),
        maximum_history_ns=1_000,
        known_source_event_ids=tuple(event[0] for event in events),
    )
    kind_offset = {
        AdaptivePartitionKind.COARSEST: 0.0,
        AdaptivePartitionKind.PLANNER: 0.001,
        AdaptivePartitionKind.FINER: -0.001,
    }[kind]
    replicate_offset = replicate / 1_000
    metrics = {
        name: 1.0 + replicate_offset + kind_offset
        for name in REQUIRED_PARTITION_METRICS
    }
    return PartitionInvarianceRunV1(
        case_id=case.case_id,
        partition_id=partition.partition_id,
        partition_kind=kind,
        replicate_id=f"replicate:{replicate}",
        semantic_member_id=member_id,
        source_content_sha256=case.source_content_sha256,
        model_fit_id=case.model_fit_id,
        observation_scenario_id=case.observation_scenario_id,
        seed_ledger=ledger,
        ownership_audit=ownership,
        history_audit=history,
        feature_vector=tuple(
            float(value) + replicate_offset + kind_offset for value in range(4)
        ),
        metrics=metrics,
    )


def _campaign(
    tmp_path: Path,
) -> tuple[
    PartitionInvariancePolicyV1,
    tuple[PartitionInvarianceCaseV1, ...],
    tuple[PartitionInvarianceRunV1, ...],
]:
    case_events = tuple(_case(tmp_path, index) for index in range(3))
    runs = tuple(
        _run(case, events, kind, replicate)
        for case, events in case_events
        for kind in AdaptivePartitionKind
        for replicate in range(4)
    )
    return _policy(), tuple(item[0] for item in case_events), runs


def _qualify(
    policy: PartitionInvariancePolicyV1,
    cases: tuple[PartitionInvarianceCaseV1, ...],
    runs: tuple[PartitionInvarianceRunV1, ...],
) -> PartitionInvarianceQualificationV1:
    return qualify_partition_invariance(
        policy,
        cases,
        runs,
        qualified_at_utc="2026-08-21T12:00:00Z",
    )


def test_one_nanosecond_boundary_change_changes_partition_identity() -> None:
    original = _partition(
        "boundary",
        AdaptivePartitionKind.PLANNER,
        (0, 500, 1_000),
    )
    shifted = _partition(
        "boundary",
        AdaptivePartitionKind.PLANNER,
        (0, 501, 1_000),
    )

    assert original.partition_id != shifted.partition_id
    assert original.intervals[0].interval_id != shifted.intervals[0].interval_id


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    "boundaries",
    ((0, 500, 499, 1_000), (0, 500, 500, 1_000)),
)
def test_partition_rejects_overlap_or_empty_child(
    boundaries: tuple[int, ...],
) -> None:
    with pytest.raises(ValueError):
        _partition("invalid", AdaptivePartitionKind.PLANNER, boundaries)


def test_case_requires_a_strict_deterministic_refinement(
    tmp_path: Path,
) -> None:
    case, _ = _case(tmp_path, 1)
    planner = case.partition(AdaptivePartitionKind.PLANNER)

    with pytest.raises(ValueError, match="not stricter"):
        replace(
            case,
            partitions=(
                case.partition(AdaptivePartitionKind.COARSEST),
                planner,
                replace(
                    planner,
                    kind=AdaptivePartitionKind.FINER,
                    partition_id="",
                ),
            ),
            case_id="",
        )


def test_source_ownership_is_half_open_exactly_once_and_preserves_seams() -> (
    None
):
    partition = _partition(
        "ownership",
        AdaptivePartitionKind.PLANNER,
        (0, 500, 1_000),
    )
    events = _source_events("ownership")
    audit = audit_partition_source_ownership(
        partition,
        events,
        source_content_sha256=_sha("source-content:ownership"),
        anchor_event_ids=(events[2][0], events[-1][0]),
    )

    assert audit.passed
    assert audit.assigned_event_count == len(events)
    assert audit.boundary_event_count == 1
    assert audit.missing_anchor_count == 0

    failed = audit_partition_source_ownership(
        partition,
        (*events, events[0], ("outside", 1_000)),
        source_content_sha256=_sha("source-content:ownership"),
        anchor_event_ids=(events[2][0], "missing-anchor"),
    )
    assert not failed.passed
    assert failed.duplicate_event_count == 1
    assert failed.lost_event_count == 1
    assert failed.missing_anchor_count == 1


def test_history_audit_detects_future_out_of_bound_and_unknown_rows() -> None:
    partition = _partition(
        "history",
        AdaptivePartitionKind.PLANNER,
        (0, 500, 1_000),
    )
    first, second = partition.intervals
    audit = audit_partition_history(
        partition,
        {
            first.interval_id: (),
            second.interval_id: (
                ("known", 499),
                ("future", 500),
                ("old", -501),
                ("unknown", 400),
            ),
        },
        maximum_history_ns=1_000,
        known_source_event_ids=("known", "future", "old"),
    )

    assert not audit.passed
    assert audit.future_event_count == 1
    assert audit.out_of_bound_event_count == 1
    assert audit.unknown_source_event_count == 1


def test_hierarchical_seeds_preserve_semantics_across_partition_and_workers() -> (
    None
):
    coarse = _partition(
        "seed",
        AdaptivePartitionKind.COARSEST,
        (0, 1_000),
    )
    refined = _partition(
        "seed",
        AdaptivePartitionKind.PLANNER,
        (0, 500, 1_000),
    )
    coarse_ledger = build_partition_seed_ledger(
        coarse,
        candidate_id=_CANDIDATE_ID,
        model_fit_id="fit:v1",
        observation_scenario_id="scenario:v1",
        semantic_member_id="member:v1",
        base_seed=42,
    )
    refined_ledger = build_partition_seed_ledger(
        refined,
        candidate_id=_CANDIDATE_ID,
        model_fit_id="fit:v1",
        observation_scenario_id="scenario:v1",
        semantic_member_id="member:v1",
        base_seed=42,
    )
    repeated_ledger = build_partition_seed_ledger(
        refined,
        candidate_id=_CANDIDATE_ID,
        model_fit_id="fit:v1",
        observation_scenario_id="scenario:v1",
        semantic_member_id="member:v1",
        base_seed=42,
    )

    assert coarse_ledger.parent_seed == refined_ledger.parent_seed
    assert refined_ledger == repeated_ledger
    assert coarse_ledger.payload()["worker_count_in_seed"] is False
    assert coarse_ledger.payload()["retry_attempt_in_seed"] is False
    assert coarse_ledger.payload()["partition_ordinal_in_seed"] is False


def test_empirical_energy_distance_has_expected_identity_and_separation() -> (
    None
):
    sample = ((0.0, 0.0), (1.0, 1.0))

    assert energy_distance_squared(sample, sample) == pytest.approx(0.0)
    assert energy_distance_squared(sample, ((10.0, 10.0),)) > 10.0
    with pytest.raises(ValueError, match="dimensions differ"):
        energy_distance_squared(sample, ((1.0,),))


def test_complete_campaign_passes_all_predeclared_strata(
    tmp_path: Path,
) -> None:
    policy, cases, runs = _campaign(tmp_path)

    result = _qualify(policy, cases, runs)

    assert result.status is PartitionQualificationStatus.PASS
    assert result.full_campaign_permitted
    assert len(result.comparisons) == 9
    assert result.coverage == {
        "activity_strata": ("high_activity", "quiet"),
        "alignment_kinds": ("bounded_nearest", "exact"),
        "context_strata": ("event", "ordinary"),
        "epoch_strata": (
            "early_sparse",
            "modern_dense",
            "qualified_transition",
        ),
        "observation_scenarios": (
            "central_fitted_retention",
            "high_retention_low_infill",
            "low_retention_high_infill",
        ),
        "split_depth_strata": (
            "binary_split",
            "deep_recursive_split",
            "no_split",
        ),
    }
    assert all(not item.hard_violations for item in result.comparisons)


def test_hard_and_advisory_tolerances_have_distinct_release_effects(
    tmp_path: Path,
) -> None:
    policy, cases, runs = _campaign(tmp_path)
    planner_runs = {
        item.run_id
        for item in runs
        if item.case_id == cases[0].case_id
        and item.partition_kind is AdaptivePartitionKind.PLANNER
    }

    def changed(
        metric: str, amount: float
    ) -> tuple[PartitionInvarianceRunV1, ...]:
        return tuple(
            (
                replace(
                    item,
                    metrics={
                        **item.metrics,
                        metric: item.metrics[metric] + amount,
                    },
                    run_id="",
                )
                if item.run_id in planner_runs
                else item
            )
            for item in runs
        )

    hard = _qualify(policy, cases, changed("maximum_excursion", 1.0))
    advisory = _qualify(policy, cases, changed("resource_work", 1.0))

    assert hard.status is PartitionQualificationStatus.FAIL
    assert not hard.full_campaign_permitted
    assert any("maximum_excursion" in item for item in hard.findings)
    assert advisory.status is PartitionQualificationStatus.PASS
    assert advisory.full_campaign_permitted
    assert any("resource_work" in item for item in advisory.findings)


def test_missing_strata_and_replicates_are_insufficient_evidence(
    tmp_path: Path,
) -> None:
    policy, cases, runs = _campaign(tmp_path)
    selected_runs = tuple(
        item for item in runs if item.case_id == cases[0].case_id
    )

    result = _qualify(policy, cases[:1], selected_runs)

    assert result.status is PartitionQualificationStatus.INSUFFICIENT_EVIDENCE
    assert not result.full_campaign_permitted
    assert "minimum_case_count_not_met" in result.findings
    assert any(
        item.startswith("missing_epoch_strata") for item in result.findings
    )


def test_scientific_lineage_and_candidate_bytes_are_verified(
    tmp_path: Path,
) -> None:
    policy, cases, runs = _campaign(tmp_path)
    wrong_source = replace(
        runs[0],
        source_content_sha256=_sha("another-source"),
        run_id="",
    )

    with pytest.raises(ValueError, match="scientific lineage differs"):
        _qualify(policy, cases, (wrong_source, *runs[1:]))

    wrong_ownership = replace(
        runs[0].ownership_audit,
        source_content_sha256=_sha("another-ownership-source"),
        audit_id="",
    )
    wrong_ownership_run = replace(
        runs[0],
        ownership_audit=wrong_ownership,
        run_id="",
    )
    with pytest.raises(ValueError, match="scientific lineage differs"):
        _qualify(policy, cases, (wrong_ownership_run, *runs[1:]))

    candidate_path = Path(cases[0].release_candidate_ref.path)
    candidate_path.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact (size|hash) differs"):
        _qualify(policy, cases, runs)


def test_report_round_trip_is_content_addressed_and_recomputes_decision(
    tmp_path: Path,
) -> None:
    policy, cases, runs = _campaign(tmp_path)
    result = _qualify(policy, cases, runs)
    ref = write_partition_invariance_qualification(result, tmp_path / "reports")

    restored = read_partition_invariance_qualification(ref.path)
    assert restored == result
    assert ref.metadata == {
        "qualification_id": result.qualification_id,
        "candidate_id": result.candidate_id,
        "status": "pass",
    }

    forged = result.to_dict()
    forged["status"] = "fail"
    forged["full_campaign_permitted"] = False
    forged["qualification_id"] = ""
    with pytest.raises(ValueError, match="decision differs"):
        PartitionInvarianceQualificationV1.from_dict(forged)

    report_path = Path(ref.path)
    report_path.write_text(
        report_path.read_text(encoding="utf-8") + " ", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="not content addressed"):
        read_partition_invariance_qualification(report_path)

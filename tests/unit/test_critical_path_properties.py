"""Bounded property checks for reconstruction critical-path invariants."""

from __future__ import annotations

import hashlib
from itertools import pairwise
from pathlib import Path

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from histdatacom.synthetic.alignment_qualification import (
    TriangleAlignmentPolicy,
    TriangleAlignmentTupleV1,
)
from histdatacom.synthetic.marked_hawkes import (
    MarkedHawkesGenerationEvidenceV1,
    MarkedHawkesGenerationStatus,
)
from histdatacom.synthetic.partition_invariance import (
    AdaptivePartitionIntervalV1,
    AdaptivePartitionKind,
    AdaptivePartitionSpecV1,
    audit_partition_source_ownership,
    build_partition_seed_ledger,
)
from histdatacom.synthetic.persistence import (
    RECONSTRUCTION_PRODUCT_DIRECTORY,
    discover_reconstruction_manifests,
)
from histdatacom.synthetic.streaming import (
    ReconstructionResourceEstimateV1,
    ReconstructionResourceLimitError,
    ReconstructionStoragePolicyV1,
    RejectionSummaryV1,
)

_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _partition(boundaries: tuple[int, ...]) -> AdaptivePartitionSpecV1:
    return AdaptivePartitionSpecV1(
        parent_span_id="source-span:property",
        parent_start_ns=0,
        parent_end_ns=1_000,
        kind=AdaptivePartitionKind.PLANNER,
        intervals=tuple(
            AdaptivePartitionIntervalV1(start_ns=start, end_ns=end)
            for start, end in pairwise(boundaries)
        ),
    )


@settings(max_examples=64, deadline=None)
@given(
    cuts=st.lists(
        st.integers(min_value=1, max_value=999),
        unique=True,
        max_size=12,
    ),
    event_times=st.lists(
        st.integers(min_value=0, max_value=999),
        unique=True,
        max_size=64,
    ),
)
def test_generated_partitions_are_contiguous_and_own_rows_exactly_once(
    cuts: list[int], event_times: list[int]
) -> None:
    """Random half-open refinements retain every source row exactly once."""
    boundaries = (0, *sorted(cuts), 1_000)
    partition = _partition(boundaries)
    events = tuple(
        (f"source-event:{index}:{event_time}", event_time)
        for index, event_time in enumerate(event_times)
    )

    audit = audit_partition_source_ownership(
        partition,
        events,
        source_content_sha256=_sha("property-source"),
        anchor_event_ids=tuple(event_id for event_id, _ in events[:2]),
    )

    assert partition.boundaries == boundaries
    assert audit.passed
    assert audit.assigned_event_count == len(events)
    assert audit.duplicate_event_count == 0
    assert audit.lost_event_count == 0


@settings(max_examples=48, deadline=None)
@given(
    left_end=st.integers(min_value=1, max_value=997),
    gap=st.integers(min_value=1, max_value=128),
)
def test_generated_partition_gaps_fail_closed(left_end: int, gap: int) -> None:
    """Random omitted support intervals cannot form a qualified partition."""
    assume(left_end + gap < 1_000)
    with pytest.raises(ValueError, match="not contiguous"):
        AdaptivePartitionSpecV1(
            parent_span_id="source-span:gap-property",
            parent_start_ns=0,
            parent_end_ns=1_000,
            kind=AdaptivePartitionKind.PLANNER,
            intervals=(
                AdaptivePartitionIntervalV1(start_ns=0, end_ns=left_end),
                AdaptivePartitionIntervalV1(
                    start_ns=left_end + gap, end_ns=1_000
                ),
            ),
        )


@settings(max_examples=48, deadline=None)
@given(
    probe_time=st.integers(min_value=1_000, max_value=1_000_000),
    future_delta=st.integers(min_value=1, max_value=999),
    maximum_age=st.integers(min_value=1, max_value=999),
)
def test_generated_future_alignment_tuples_fail_closed(
    probe_time: int, future_delta: int, maximum_age: int
) -> None:
    """A self-consistent negative age still cannot admit a future quote."""
    event_ids = {symbol: f"event:{symbol}" for symbol in _SYMBOLS}
    times = {symbol: probe_time for symbol in _SYMBOLS}
    ages = {symbol: 0 for symbol in _SYMBOLS}
    times["eurusd"] = probe_time + future_delta
    ages["eurusd"] = -future_delta

    with pytest.raises(ValueError, match="future or retimestamped"):
        TriangleAlignmentTupleV1(
            window_id="window:property",
            policy=TriangleAlignmentPolicy.BOUNDED_PRIOR,
            configured_max_age_ns=maximum_age,
            probe_symbol="eurgbp",
            probe_event_id=event_ids["eurgbp"],
            probe_time_ns=probe_time,
            selected_event_ids=event_ids,
            selected_event_times_ns=times,
            selected_event_content_sha256={
                symbol: _sha(f"event-content:{symbol}") for symbol in _SYMBOLS
            },
            ages_ns=ages,
        )


@settings(max_examples=48, deadline=None)
@given(
    base_seed=st.integers(min_value=0, max_value=2**64 - 1),
    cut=st.integers(min_value=1, max_value=999),
)
def test_generated_seed_identity_excludes_execution_tuning(
    base_seed: int, cut: int
) -> None:
    """Worker, retry, path, and partition ordinals never enter seed identity."""
    partition = _partition((0, cut, 1_000))
    first = build_partition_seed_ledger(
        partition,
        candidate_id="candidate:property",
        model_fit_id="fit:property",
        observation_scenario_id="scenario:property",
        semantic_member_id="member:property",
        base_seed=base_seed,
    )
    replay = build_partition_seed_ledger(
        partition,
        candidate_id="candidate:property",
        model_fit_id="fit:property",
        observation_scenario_id="scenario:property",
        semantic_member_id="member:property",
        base_seed=base_seed,
    )

    assert first == replay
    assert first.payload()["worker_count_in_seed"] is False
    assert first.payload()["retry_attempt_in_seed"] is False
    assert first.payload()["partition_ordinal_in_seed"] is False


@settings(max_examples=48, deadline=None)
@given(
    suffix=st.integers(min_value=0, max_value=1_000_000),
    cut=st.integers(min_value=1, max_value=998),
)
def test_generated_semantic_changes_invalidate_identity(
    suffix: int, cut: int
) -> None:
    """Source boundaries, candidate/config identity, and fit identity bind IDs."""
    partition = _partition((0, cut, 1_000))
    changed_partition = _partition((0, cut + 1, 1_000))
    common = {
        "candidate_id": f"candidate:{suffix}",
        "model_fit_id": f"fit:{suffix}",
        "observation_scenario_id": f"scenario:{suffix}",
        "semantic_member_id": "member:property",
        "base_seed": suffix,
    }
    baseline = build_partition_seed_ledger(partition, **common)
    changed_source = build_partition_seed_ledger(changed_partition, **common)
    changed_config = build_partition_seed_ledger(
        partition,
        **{**common, "model_fit_id": f"fit:{suffix}:changed"},
    )

    assert partition.partition_id != changed_partition.partition_id
    assert baseline.ledger_id != changed_source.ledger_id
    assert baseline.ledger_id != changed_config.ledger_id


@settings(max_examples=48, deadline=None)
@given(limit=st.integers(min_value=1, max_value=1_000_000))
def test_generated_resource_boundaries_are_inclusive_and_overflow_fails(
    limit: int,
) -> None:
    """Exact resource/amplification limits pass; one-unit overflow fails."""
    policy = ReconstructionStoragePolicyV1(
        max_events_per_batch=limit,
        max_candidate_amplification=2.0,
        max_inflight_batches=limit,
        max_memory_bytes=limit,
        max_scratch_bytes=limit,
        max_output_bytes=limit,
        max_retained_ensemble_members=limit,
    )
    estimate = ReconstructionResourceEstimateV1(
        input_event_count=limit,
        candidate_event_count=2 * limit,
        retained_ensemble_members=limit,
        inflight_batches=limit,
        peak_events_per_batch=limit,
        estimated_memory_bytes=limit,
        estimated_scratch_bytes=limit,
        estimated_output_bytes=limit,
        estimated_batch_count=limit,
    )

    assert policy.preflight(estimate) is estimate
    with pytest.raises(ReconstructionResourceLimitError):
        policy.preflight(
            ReconstructionResourceEstimateV1(
                **{
                    **estimate.identity_payload(),
                    "estimated_memory_bytes": limit + 1,
                }
            )
        )


@settings(max_examples=32, deadline=None)
@given(
    radius=st.one_of(
        st.just(1.0),
        st.floats(
            min_value=1.0,
            max_value=8.0,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
)
def test_spectral_radius_boundary_fails_closed(radius: float) -> None:
    """Unit or super-unit spectral radius cannot enter generation evidence."""
    with pytest.raises(ValueError, match="spectral radius is unstable"):
        MarkedHawkesGenerationEvidenceV1(
            fit_id="fit:property",
            window_id="window:property",
            ensemble_member_id="member:property",
            status=MarkedHawkesGenerationStatus.FAILED,
            attempted=True,
            generated_event_count=0,
            input_event_count=1,
            history_event_count=1,
            proposal_count=0,
            input_anchor_sha256=None,
            conditioning_support_level="property",
            conditioning_model_key=None,
            spectral_radius=radius,
            lineage_content_sha256=None,
            wall_time_ms=0,
            peak_memory_bytes=0,
            failure_reason="property failure",
        )


@settings(max_examples=64, deadline=None)
@given(
    accepted=st.integers(min_value=0, max_value=10_000),
    rejected=st.integers(min_value=0, max_value=10_000),
)
def test_generated_rejection_counts_reconcile(
    accepted: int, rejected: int
) -> None:
    """Accepted/rejected totals and reason counts reconcile exactly."""
    reasons = {"property_rejection": rejected} if rejected else {}
    valid = RejectionSummaryV1(
        run_id="run:property",
        window_id="window:property",
        candidate_count=accepted + rejected,
        accepted_count=accepted,
        rejected_count=rejected,
        reason_counts=reasons,
    )
    assert valid.accepted_count + valid.rejected_count == valid.candidate_count

    with pytest.raises(ValueError, match="accepted plus rejected"):
        RejectionSummaryV1(
            run_id="run:property",
            window_id="window:property",
            candidate_count=accepted + rejected + 1,
            accepted_count=accepted,
            rejected_count=rejected,
            reason_counts=reasons,
        )


@settings(
    max_examples=32,
    deadline=None,
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
@given(
    suffix=st.text(
        alphabet=st.characters(whitelist_categories=("Ll", "Nd")),
        min_size=1,
        max_size=12,
    )
)
def test_generated_scratch_manifests_are_never_discoverable(
    tmp_path: Path, suffix: str
) -> None:
    """Arbitrary transaction scratch names remain outside discovery."""
    scratch_manifest = (
        tmp_path
        / RECONSTRUCTION_PRODUCT_DIRECTORY
        / "axis"
        / ".scratch"
        / f"publication.tmp-{suffix}"
        / "manifest.json"
    )
    scratch_manifest.parent.mkdir(parents=True)
    scratch_manifest.write_text("{}", encoding="utf-8")

    assert discover_reconstruction_manifests(tmp_path) == ()

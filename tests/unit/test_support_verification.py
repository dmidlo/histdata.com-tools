"""Tests for final adaptive support verification contracts."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.reconstruction import (
    ReconstructionExecutionRequestV1,
    ReconstructionPlanError,
    ReconstructionPlanSetExecutionRequestV1,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic import support_verification as support_module
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.support_verification import (
    FINAL_SUPPORT_VERIFICATION_SHARD_ARTIFACT_KIND,
    FinalAdaptiveSupportMapIndexV1,
    FinalSupportPartitionReplayV1,
    FinalSupportVerificationError,
    FinalSupportVerificationShardV1,
    FinalSupportWindowVerificationV1,
    build_final_support_census,
)

_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")


def _window(
    start_ns: int,
    end_ns: int,
    *,
    status: str = "executable",
) -> FinalSupportWindowVerificationV1:
    counts = {symbol: 3 for symbol in _SYMBOLS}
    inputs = {symbol: 5 for symbol in _SYMBOLS}
    refused = status == "refused"
    empty = status == "empty"
    return FinalSupportWindowVerificationV1(
        start_ns=start_ns,
        end_ns=end_ns,
        plan_id="synthetic-infill-plan:sha256:" + "1" * 64,
        plan_shard_id="reconstruction-plan-shard:sha256:" + "2" * 64,
        claimed_support_id="reconstruction-plan-support-window:sha256:"
        + f"{start_ns:064x}"[-64:],
        status=status,
        core_event_counts=(
            {symbol: 0 for symbol in _SYMBOLS} if status == "empty" else counts
        ),
        input_event_counts=inputs,
        core_row_identity_digest="3" * 64,
        input_anchor_identity_digest="4" * 64,
        alignment_source_event_digest="5" * 64,
        common_exact_core_timestamp_count=0 if empty else 3,
        bounded_nearest_core_timestamp_count=0 if empty else 9,
        bounded_nearest_core_stale_timestamp_count=0,
        bounded_nearest_core_maximum_age_ns=0,
        bounded_nearest_core_p95_age_ns=0,
        selected_cross_series_alignment=(
            "unavailable" if empty else "exact_event_sequence"
        ),
        recommended_cross_series_event_time_ns=None if empty else start_ns,
        feed_epoch_label="modern",
        feed_epoch_assignment_ids=("epoch-modern",),
        transition_scenario_ids=(),
        session="london-new-york-overlap",
        event_state="market_context:none:unknown",
        cftc_query_status="ready",
        cftc_conditioning_mode="cftc-weekly-state-conditioned-v1",
        modeled_missing_event_count=2 if status == "executable" else 0,
        candidate_amplification=(2 / 15 if status == "executable" else 0.0),
        split_depth=1,
        member_count=2 if status == "executable" else 0,
        workflow_task_count=2 if status == "executable" else 0,
        refusal_code="source_triangle_incomplete" if refused else None,
        refusal_reason="immutable triangle is incomplete" if refused else None,
    )


def _artifact(path: Path, kind: str, **metadata: object) -> ArtifactRef:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n", encoding="utf-8")
    return artifact_ref_for_file(path, kind=kind, metadata=metadata)


def test_final_support_census_is_exact_content_addressed_and_round_trips() -> (
    None
):
    windows = (
        _window(0, 10),
        _window(10, 20),
        _window(20, 30, status="refused"),
    )

    census = build_final_support_census(windows)

    assert census.window_count == 3
    assert census.total_duration_ns == 30
    assert census.duration_counts_ns == {"10": 3}
    assert census.terminal_counts == {"executable": 2, "refused": 1}
    assert census.alignment_counts == {"exact_event_sequence": 3}
    assert census.alignment_age_quantiles_ns["p100"] == 0.0
    assert census.feed_epoch_counts == {"modern": 3}
    assert census.session_counts == {"london-new-york-overlap": 3}
    assert census.cftc_mode_counts == {"cftc-weekly-state-conditioned-v1": 3}
    assert census.split_depth_counts == {"1": 3}
    assert census.minimum_window_size_ns == 10
    assert census.maximum_window_size_ns == 10
    assert census.valid_common_data_implementation_refusal_count == 0
    assert type(census).from_dict(census.to_dict()) == census


def test_valid_common_data_implementation_refusal_blocks_a_shard() -> None:
    refusal = replace(
        _window(0, 10, status="refused"),
        refusal_code="feed_epoch_unsupported",
        refusal_reason="implemented feed assignment was not handled",
        verification_id="",
    )
    replay = FinalSupportPartitionReplayV1(
        partition_id="reconstruction-source-partition:sha256:" + "6" * 64,
        symbol="eurgbp",
        period="202001",
        artifact_sha256="7" * 64,
        row_count=1,
        coverage_start_ns=0,
        coverage_end_ns=10,
        first_timestamp_ms=0,
        last_timestamp_ms=0,
        in_requested_domain_row_count=1,
        outside_requested_domain_row_count=0,
        row_identity_digest="8" * 64,
    )
    census = build_final_support_census((refusal,))

    assert census.valid_common_data_implementation_refusal_count == 1
    with pytest.raises(
        FinalSupportVerificationError,
        match="valid-common-data implementation refusals",
    ):
        FinalSupportVerificationShardV1(
            plan_set_id="reconstruction-plan-set:sha256:" + "9" * 64,
            plan_shard_id=refusal.plan_shard_id,
            plan_id=refusal.plan_id,
            release_candidate_id=(
                "reconstruction-release-candidate:sha256:" + "a" * 64
            ),
            source_inventory_id=(
                "reconstruction-source-inventory:sha256:" + "b" * 64
            ),
            claimed_support_map_id=(
                "reconstruction-plan-support-map:sha256:" + "c" * 64
            ),
            requested_start_ns=0,
            requested_end_ns=10,
            partition_replays=(replay,),
            windows=(refusal,),
            census=census,
        )


def test_final_support_shard_and_index_fail_closed_on_one_ns_gap(
    tmp_path: Path,
) -> None:
    windows = (_window(0, 10), _window(10, 20))
    replay = FinalSupportPartitionReplayV1(
        partition_id="reconstruction-source-partition:sha256:" + "6" * 64,
        symbol="eurgbp",
        period="202001",
        artifact_sha256="7" * 64,
        row_count=4,
        coverage_start_ns=0,
        coverage_end_ns=20,
        first_timestamp_ms=0,
        last_timestamp_ms=1,
        in_requested_domain_row_count=4,
        outside_requested_domain_row_count=0,
        row_identity_digest="8" * 64,
    )
    shard = FinalSupportVerificationShardV1(
        plan_set_id="reconstruction-plan-set:sha256:" + "9" * 64,
        plan_shard_id=windows[0].plan_shard_id,
        plan_id=windows[0].plan_id,
        release_candidate_id="reconstruction-release-candidate:sha256:"
        + "a" * 64,
        source_inventory_id="reconstruction-source-inventory:sha256:"
        + "b" * 64,
        claimed_support_map_id="reconstruction-plan-support-map:sha256:"
        + "c" * 64,
        requested_start_ns=0,
        requested_end_ns=20,
        partition_replays=(replay,),
        windows=windows,
        census=build_final_support_census(windows),
    )
    shard_ref = _artifact(
        tmp_path / "verification.json",
        FINAL_SUPPORT_VERIFICATION_SHARD_ARTIFACT_KIND,
        verification_shard_id=shard.verification_shard_id,
        plan_set_id=shard.plan_set_id,
        release_candidate_id=shard.release_candidate_id,
        requested_start_ns=0,
        requested_end_ns=20,
        executable_window_count=2,
        empty_window_count=0,
        refused_window_count=0,
    )
    plan_set_ref = _artifact(
        tmp_path / "plan-set.json",
        "reconstruction_plan_set_v1",
        plan_set_id=shard.plan_set_id,
    )
    support_ref = _artifact(
        tmp_path / "support.json",
        "reconstruction_plan_support_map_index_v2",
        plan_set_id=shard.plan_set_id,
        status="ready",
        selected_proposal_engine_ids=[
            "histdatacom.marked-hawkes.diagonal_self_excitation"
        ],
    )
    candidate_ref = _artifact(
        tmp_path / "candidate.json",
        "reconstruction_release_candidate_v1",
        candidate_id=shard.release_candidate_id,
        source_cutoff_ns=20,
        selected_engine_id=(
            "histdatacom.marked-hawkes.diagonal_self_excitation"
        ),
    )

    index = FinalAdaptiveSupportMapIndexV1(
        plan_set_ref=plan_set_ref,
        claimed_support_ref=support_ref,
        release_candidate_ref=candidate_ref,
        verification_shard_refs=(shard_ref,),
        selected_engine_ids=(
            "histdatacom.marked-hawkes.diagonal_self_excitation",
        ),
        selected_scenario_ids=("member-0", "member-1"),
        source_cutoff_ns=20,
        requested_start_ns=0,
        requested_end_ns=20,
        census=shard.census,
    )

    assert type(index).from_dict(index.to_dict()) == index
    with pytest.raises(FinalSupportVerificationError, match="not contiguous"):
        FinalSupportVerificationShardV1(
            plan_set_id=shard.plan_set_id,
            plan_shard_id=shard.plan_shard_id,
            plan_id=shard.plan_id,
            release_candidate_id=shard.release_candidate_id,
            source_inventory_id=shard.source_inventory_id,
            claimed_support_map_id=shard.claimed_support_map_id,
            requested_start_ns=0,
            requested_end_ns=20,
            partition_replays=shard.partition_replays,
            windows=(
                windows[0],
                replace(
                    windows[1],
                    start_ns=11,
                    recommended_cross_series_event_time_ns=11,
                    verification_id="",
                ),
            ),
            census=shard.census,
        )


def test_final_campaign_request_binds_support_to_every_shard(
    tmp_path: Path,
) -> None:
    plan_set_ref = _artifact(
        tmp_path / "plan-set.json",
        "reconstruction_plan_set_v1",
        plan_set_id="reconstruction-plan-set:sha256:" + "d" * 64,
    )
    final_ref = _artifact(
        tmp_path / "final-support.json",
        "final_adaptive_support_map_index_v1",
        plan_set_id=plan_set_ref.metadata["plan_set_id"],
        final_support_map_id="final-adaptive-support-map:sha256:" + "e" * 64,
    )
    requests = tuple(
        ReconstructionExecutionRequestV1(
            plan_path=str(tmp_path / f"plan-{index}.json"),
            plan_id=f"synthetic-infill-plan:sha256:{index:064x}",
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            scientific_nonclaim_acknowledged=True,
            support_map_ref=final_ref,
        )
        for index in range(2)
    )

    request_set = ReconstructionPlanSetExecutionRequestV1(
        plan_set_ref=plan_set_ref,
        support_map_ref=final_ref,
        requests=requests,
    )

    assert all(
        item.support_map_ref == final_ref for item in request_set.requests
    )
    assert type(request_set).from_dict(request_set.to_dict()) == request_set
    with pytest.raises(ReconstructionPlanError, match="every final campaign"):
        ReconstructionPlanSetExecutionRequestV1(
            plan_set_ref=plan_set_ref,
            support_map_ref=final_ref,
            requests=tuple(
                replace(item, support_map_ref=None, request_id="")
                for item in requests
            ),
        )


def test_frozen_context_dependency_mismatch_fails_closed(
    tmp_path: Path,
) -> None:
    dependency_to_graph = {
        "alignment_policy": "cross_series_constraint_policy",
        "cftc_positioning": "cftc_positioning",
        "dataset_catalog": "dataset_catalog",
        "experiment_manifest": "experiment_manifest",
        "feed_epoch_definition": "feed_epochs",
        "market_context": "market_context",
        "observation_operator": "observation_operator",
        "scientific_ledger": "scientific_ledger",
    }
    dependency_refs = {
        dependency: _artifact(
            tmp_path / f"candidate-{dependency}.json",
            f"{dependency}_v1",
            artifact_id=f"{dependency}:v1",
        )
        for dependency in dependency_to_graph
    }
    graph = {
        graph_role: dependency_refs[dependency]
        for dependency, graph_role in dependency_to_graph.items()
    }
    graph["market_context"] = _artifact(
        tmp_path / "different-context.json",
        "market_context_corpus_v1",
        artifact_id="market-context:different",
    )
    candidate = SimpleNamespace(
        selected_engine_id="histdatacom.marked-hawkes.diagonal_self_excitation",
        dependency=lambda name: SimpleNamespace(
            artifact_ref=dependency_refs[name]
        ),
    )

    with pytest.raises(
        FinalSupportVerificationError,
        match="market_context",
    ):
        support_module._verify_release_candidate_dependencies(
            SimpleNamespace(artifact_graph=graph),
            candidate,
            SimpleNamespace(),
        )

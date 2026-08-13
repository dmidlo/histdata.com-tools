"""Tests for durable synthetic reconstruction orchestration."""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.streaming import (
    ReconstructionCommitPhase,
    ReconstructionResourceEstimateV1,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    plan_reconstruction_windows,
)
from histdatacom.orchestration import activities, workflows
from histdatacom.orchestration.client import submit_reconstruction_request
from histdatacom.orchestration.queues import build_orchestration_worker_config
from histdatacom.orchestration.reconstruction import (
    RECONSTRUCTION_STAGE_ORDER,
    ReconstructionArtifactError,
    ReconstructionCheckpointConflict,
    ReconstructionCheckpointStore,
    ReconstructionReportMismatch,
    ReconstructionStage,
    ReconstructionStageCommandV1,
    ReconstructionStageInvocationV1,
    ReconstructionStageOutcomeV1,
    ReconstructionStageStatus,
    ReconstructionWindowTaskV1,
    ReconstructionWorkflowRequestV1,
    RegisteredReconstructionStageExecutor,
    artifact_ref_for_file,
    execute_reconstruction_stage,
    plan_reconstruction_waves,
    reconcile_reconstruction_report,
    register_reconstruction_stage_handler,
    run_reconstruction_window,
    unregister_reconstruction_stage_handler,
    verify_artifact_ref,
)

RECONSTRUCTION_MODULE = sys.modules[reconcile_reconstruction_report.__module__]


def _run(
    *, policy: ReconstructionStoragePolicyV1 | None = None
) -> ReconstructionRunV1:
    return ReconstructionRunV1(
        symbols=("eurusd", "eurgbp", "gbpusd"),
        source_version_ids=("source:sha256:" + "1" * 64,),
        configuration_ids=("config:sha256:" + "2" * 64,),
        ensemble_member_ids=("member-0",),
        base_seed=42,
        storage_policy=policy or ReconstructionStoragePolicyV1(),
    )


def _estimate(*, memory_bytes: int = 1024) -> ReconstructionResourceEstimateV1:
    return ReconstructionResourceEstimateV1(
        input_event_count=10,
        candidate_event_count=20,
        retained_ensemble_members=1,
        inflight_batches=1,
        peak_events_per_batch=10,
        estimated_memory_bytes=memory_bytes,
        estimated_scratch_bytes=2048,
        estimated_output_bytes=1024,
        estimated_batch_count=1,
    )


def _task(
    tmp_path: Path,
    *,
    run: ReconstructionRunV1 | None = None,
    memory_bytes: int = 1024,
    offset: int = 0,
    handler_name: str = "test-handler",
) -> ReconstructionWindowTaskV1:
    resolved_run = run or _run()
    window = plan_reconstruction_windows(
        resolved_run,
        ensemble_member_id="member-0",
        start_ns=offset,
        end_ns=offset + 10_000,
        window_size_ns=10_000,
    )[0]
    scratch = tmp_path / f"scratch-{offset}"
    commands = tuple(
        ReconstructionStageCommandV1(
            stage=stage,
            handler_name=handler_name,
            receipt_path=str(scratch / "receipts" / f"{stage.value}.json"),
        )
        for stage in RECONSTRUCTION_STAGE_ORDER
    )
    return ReconstructionWindowTaskV1(
        window=window,
        resource_estimate=_estimate(memory_bytes=memory_bytes),
        commands=commands,
        scratch_directory=str(scratch),
    )


def _request(
    tmp_path: Path,
    *tasks: ReconstructionWindowTaskV1,
    run: ReconstructionRunV1 | None = None,
    max_parallel: int = 2,
    max_memory: int = 10_000,
) -> ReconstructionWorkflowRequestV1:
    resolved_tasks = tasks or (_task(tmp_path, run=run),)
    return ReconstructionWorkflowRequestV1(
        request_id="reconstruction-test",
        run=run or _run(),
        tasks=tuple(resolved_tasks),
        manifest_store_root=str(tmp_path / "status"),
        report_root=str(tmp_path / "reports"),
        task_queues={
            "orchestration": "test.orchestration",
            "cpu_file": "test.cpu-file",
        },
        max_parallel_windows=max_parallel,
        max_inflight_memory_bytes=max_memory,
    )


def _file_ref(path: Path, text: str, *, phase: str = "") -> ArtifactRef:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    metadata = {"commit_phase": phase} if phase else {}
    return artifact_ref_for_file(path, kind="test-artifact", metadata=metadata)


def _stage_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    stage = invocation.command.stage
    output_root = Path(invocation.task.scratch_directory) / "outputs"
    if stage is ReconstructionStage.ATOMIC_PARTITION_COMMIT:
        output_root = (
            Path(invocation.task.scratch_directory).parent
            / "committed"
            / invocation.task.window.window_id.replace(":", "-")
        )
    output = output_root / f"{stage.value}.json"
    phase = ""
    if stage is ReconstructionStage.VALIDATION:
        phase = "staged"
        text = "validated-manifest"
    elif stage is ReconstructionStage.ATOMIC_PARTITION_COMMIT:
        phase = "committed"
        text = "validated-manifest"
    else:
        text = stage.value
    ref = _file_ref(output, text, phase=phase)
    ref = replace(
        ref,
        metadata={
            **ref.metadata,
            "runtime_seconds": 1.0,
            "peak_rss_bytes": 1_000,
            "scratch_bytes": 100,
            "output_bytes": ref.size_bytes,
            "candidate_amplification": 2.0,
        },
    )
    return invocation.completed(
        output_refs=(ref,),
        observed_event_count=10,
        candidate_event_count=20,
        accepted_event_count=15,
        scratch_bytes=100,
        output_bytes=ref.size_bytes or 0,
    )


@pytest.fixture(autouse=True)
def _clean_handler() -> None:
    unregister_reconstruction_stage_handler("test-handler")
    yield
    unregister_reconstruction_stage_handler("test-handler")


def test_request_round_trip_contains_only_bounded_control_metadata(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)

    restored = ReconstructionWorkflowRequestV1.from_dict(request.to_dict())

    assert restored == request
    payload = json.dumps(request.to_dict(), sort_keys=True)
    assert '"events"' not in payload
    assert '"rows"' not in payload
    assert len(payload.encode("utf-8")) < 1_048_576


def test_request_rejects_inline_rows_hidden_in_artifact_metadata(
    tmp_path: Path,
) -> None:
    source = _file_ref(tmp_path / "source.json", "source")
    source = ArtifactRef(
        kind=source.kind,
        path=source.path,
        size_bytes=source.size_bytes,
        sha256=source.sha256,
        metadata={"rows": [1, 2, 3]},
    )
    task = _task(tmp_path)
    command = task.commands[0]
    commands = (
        ReconstructionStageCommandV1(
            stage=command.stage,
            handler_name=command.handler_name,
            receipt_path=command.receipt_path,
            input_manifest_refs=(source,),
        ),
        *task.commands[1:],
    )
    task = ReconstructionWindowTaskV1(
        window=task.window,
        resource_estimate=task.resource_estimate,
        commands=commands,
        scratch_directory=task.scratch_directory,
    )

    with pytest.raises(ValueError, match="cannot contain.*rows"):
        _request(tmp_path, task)


def test_stage_outcome_rejects_inline_events_hidden_in_artifact_metadata(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
    )
    output = _file_ref(tmp_path / "output.json", "output")
    output = ArtifactRef(
        kind=output.kind,
        path=output.path,
        size_bytes=output.size_bytes,
        sha256=output.sha256,
        metadata={"events": [{"timestamp": 1}]},
    )

    with pytest.raises(ValueError, match="cannot contain.*events"):
        invocation.completed(output_refs=(output,))


def test_artifact_metadata_participates_in_retry_identity(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
    )
    output = _file_ref(tmp_path / "output.json", "output")
    staged = ArtifactRef(
        kind=output.kind,
        path=output.path,
        size_bytes=output.size_bytes,
        sha256=output.sha256,
        metadata={"commit_phase": "staged"},
    )
    committed = ArtifactRef(
        kind=output.kind,
        path=output.path,
        size_bytes=output.size_bytes,
        sha256=output.sha256,
        metadata={"commit_phase": "committed"},
    )

    assert invocation.completed(output_refs=(staged,)).outcome_id != (
        invocation.completed(output_refs=(committed,)).outcome_id
    )


def test_receipts_must_remain_inside_window_scratch(tmp_path: Path) -> None:
    task = _task(tmp_path)
    commands = list(task.commands)
    commands[0] = ReconstructionStageCommandV1(
        stage=commands[0].stage,
        handler_name="test-handler",
        receipt_path=str(tmp_path / "outside.json"),
    )

    with pytest.raises(ValueError, match="inside window scratch"):
        ReconstructionWindowTaskV1(
            window=task.window,
            resource_estimate=task.resource_estimate,
            commands=tuple(commands),
            scratch_directory=task.scratch_directory,
        )


def test_request_rejects_overlapping_window_scratch_directories(
    tmp_path: Path,
) -> None:
    narrow = _task(tmp_path)
    broad_source = _task(tmp_path, offset=10_000)
    broad = ReconstructionWindowTaskV1(
        window=broad_source.window,
        resource_estimate=broad_source.resource_estimate,
        commands=broad_source.commands,
        scratch_directory=str(tmp_path),
    )

    with pytest.raises(
        ValueError, match="scratch directories must be disjoint"
    ):
        _request(tmp_path, narrow, broad)


def test_request_rejects_scratch_overlapping_durable_roots(
    tmp_path: Path,
) -> None:
    source = _task(tmp_path)
    unsafe = ReconstructionWindowTaskV1(
        window=source.window,
        resource_estimate=source.resource_estimate,
        commands=source.commands,
        scratch_directory=str(tmp_path),
    )

    with pytest.raises(ValueError, match="manifest or report storage"):
        _request(tmp_path, unsafe)


def test_request_rejects_partial_cross_symbol_window(tmp_path: Path) -> None:
    source = _task(tmp_path)
    partial = ReconstructionWindowTaskV1(
        window=replace(
            source.window,
            symbols=("eurusd",),
            window_id="",
            synchronization_unit_id="",
        ),
        resource_estimate=source.resource_estimate,
        commands=source.commands,
        scratch_directory=source.scratch_directory,
    )

    with pytest.raises(
        ValueError, match="complete synchronized run symbol set"
    ):
        _request(tmp_path, partial)


def test_request_rejects_overlapping_core_windows(tmp_path: Path) -> None:
    first = _task(tmp_path)
    overlapping = _task(tmp_path, offset=5_000)

    with pytest.raises(ValueError, match="core intervals must not overlap"):
        _request(tmp_path, first, overlapping)


def test_checkpoint_compare_and_swap_rejects_stale_worker(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    store = ReconstructionCheckpointStore(tmp_path / "status")
    planned = store.initialize("request-a", task)
    running = planned.running()
    assert store.save(running, expected_state_id=planned.state_id) == running
    failed_a = running.interrupted(ReconstructionCommitPhase.FAILED, "worker-a")
    failed_b = running.interrupted(ReconstructionCommitPhase.FAILED, "worker-b")
    stored = store.save(failed_a, expected_state_id=running.state_id)

    assert store.save(stored, expected_state_id=running.state_id) == stored
    with pytest.raises(ReconstructionCheckpointConflict, match="stale"):
        store.save(failed_b, expected_state_id=running.state_id)


def test_worker_loss_after_receipt_reuses_artifact_without_handler(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
    )
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    first = asyncio.run(execute_reconstruction_stage(invocation))
    unregister_reconstruction_stage_handler("test-handler")

    resumed = asyncio.run(execute_reconstruction_stage(invocation))

    assert resumed.outcome_id == first.outcome_id
    assert resumed.reused is True


def test_timeout_restarts_from_last_durable_stage(tmp_path: Path) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    calls: list[ReconstructionStage] = []

    def timeout_handler(
        invocation: ReconstructionStageInvocationV1,
    ) -> ReconstructionStageOutcomeV1:
        calls.append(invocation.command.stage)
        if invocation.command.stage is ReconstructionStage.PROPOSAL:
            raise TimeoutError("injected timeout")
        return _stage_handler(invocation)

    register_reconstruction_stage_handler("test-handler", timeout_handler)
    store = ReconstructionCheckpointStore(request.manifest_store_root)
    with pytest.raises(TimeoutError, match="injected"):
        asyncio.run(
            run_reconstruction_window(
                request,
                task,
                checkpoint_store=store,
                stage_executor=RegisteredReconstructionStageExecutor(),
            )
        )
    durable = store.load(task.window)
    assert durable is not None
    assert tuple(item.stage for item in durable.outcomes) == (
        ReconstructionStage.SOURCE_ENRICHMENT,
    )

    register_reconstruction_stage_handler(
        "test-handler", _stage_handler, replace_existing=True
    )
    finished = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    assert finished.checkpoint.phase is ReconstructionCommitPhase.COMMITTED
    assert calls.count(ReconstructionStage.SOURCE_ENRICHMENT) == 1


def test_full_window_is_restart_safe_and_heartbeats_are_bounded(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    heartbeats = []
    register_reconstruction_stage_handler("test-handler", _stage_handler)

    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
            heartbeat=heartbeats.append,
        )
    )
    unregister_reconstruction_stage_handler("test-handler")
    restarted = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )

    assert state.checkpoint.phase is ReconstructionCommitPhase.COMMITTED
    assert restarted.state_id == state.state_id
    assert len(state.outcomes) == len(RECONSTRUCTION_STAGE_ORDER)
    assert len(heartbeats) == len(RECONSTRUCTION_STAGE_ORDER) * 2
    assert (
        max(len(item.to_json().encode("utf-8")) for item in heartbeats) < 65_536
    )
    assert heartbeats[-1].accepted_event_count == 15


def test_cancellation_persists_state_and_removes_only_window_scratch(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    scratch = Path(task.scratch_directory)
    scratch.mkdir(parents=True)
    (scratch / "partial.bin").write_bytes(b"partial")
    sibling = tmp_path / "keep.bin"
    sibling.write_bytes(b"keep")

    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
            cancellation_requested=lambda: True,
        )
    )

    assert state.checkpoint.phase is ReconstructionCommitPhase.CANCELLED
    assert not scratch.exists()
    assert sibling.read_bytes() == b"keep"


def test_resource_preflight_records_refusal_without_running_handler(
    tmp_path: Path,
) -> None:
    policy = ReconstructionStoragePolicyV1(max_memory_bytes=100)
    run = _run(policy=policy)
    task = _task(tmp_path, run=run, memory_bytes=101)
    request = _request(tmp_path, task, run=run, max_memory=1000)

    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )

    assert state.checkpoint.phase is ReconstructionCommitPhase.FAILED
    assert "estimated_memory_bytes" in state.checkpoint.interruption_reason
    assert state.outcomes == ()


def test_measured_peak_rss_above_policy_fails_before_checkpoint(
    tmp_path: Path,
) -> None:
    policy = ReconstructionStoragePolicyV1(max_memory_bytes=2_000)
    run = _run(policy=policy)
    task = _task(tmp_path, run=run, memory_bytes=1_024)
    request = _request(tmp_path, task, run=run, max_memory=10_000)

    def over_memory_handler(
        invocation: ReconstructionStageInvocationV1,
    ) -> ReconstructionStageOutcomeV1:
        output = _file_ref(tmp_path / "over-memory.json", "output")
        measured = replace(
            output,
            metadata={"peak_rss_bytes": policy.max_memory_bytes + 1},
        )
        return invocation.completed(
            output_refs=(measured,),
            observed_event_count=10,
            candidate_event_count=20,
            accepted_event_count=15,
            scratch_bytes=100,
            output_bytes=measured.size_bytes or 0,
        )

    register_reconstruction_stage_handler("test-handler", over_memory_handler)
    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )

    assert state.checkpoint.phase is ReconstructionCommitPhase.FAILED
    assert "peak_rss_bytes 2001 exceeds admitted limit 2000" in (
        state.checkpoint.interruption_reason
    )
    assert state.outcomes == ()


def test_many_stage_refusal_reasons_persist_as_bounded_summary(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    reasons = tuple(
        f"infeasible_relationship_point:{index:02d}:" + "x" * 180
        for index in range(32)
    )

    def refusing_handler(
        invocation: ReconstructionStageInvocationV1,
    ) -> ReconstructionStageOutcomeV1:
        return invocation.refused(*reasons, message="bounded refusal")

    register_reconstruction_stage_handler("test-handler", refusing_handler)
    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )

    summary = state.checkpoint.interruption_reason
    assert state.checkpoint.phase is ReconstructionCommitPhase.FAILED
    assert summary.startswith("infeasible_relationship_point:00:")
    assert "more [sha256:" in summary
    assert len(summary.encode("utf-8")) <= 2_048
    assert state.outcomes == ()


def test_memory_weighted_waves_enforce_backpressure(tmp_path: Path) -> None:
    run = _run()
    tasks = tuple(
        _task(tmp_path, run=run, memory_bytes=60, offset=index * 10_000)
        for index in range(3)
    )

    waves = plan_reconstruction_waves(
        tasks,
        max_parallel_windows=3,
        max_inflight_memory_bytes=100,
    )

    assert tuple(len(wave) for wave in waves) == (1, 1, 1)


def test_oversize_lane_task_gets_singleton_preflight_wave(
    tmp_path: Path,
) -> None:
    run = _run()
    task = _task(tmp_path, run=run, memory_bytes=101)

    waves = plan_reconstruction_waves(
        (task,),
        max_parallel_windows=2,
        max_inflight_memory_bytes=100,
    )
    request = _request(tmp_path, task, run=run, max_memory=100)
    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    repeated = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )

    assert waves == ((task,),)
    assert state.checkpoint.phase is ReconstructionCommitPhase.FAILED
    assert repeated.state_id == state.state_id
    assert "lane limit" in state.checkpoint.interruption_reason


def test_stage_handler_can_emit_bounded_progress_and_observe_cancel(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    heartbeats = []
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
        heartbeat_callback=heartbeats.append,
        cancellation_check=lambda: True,
    )

    invocation.heartbeat(
        sequence=1,
        completed_units=3,
        total_units=10,
        candidate_event_count=20,
        scratch_bytes=100,
    )

    assert invocation.cancellation_requested is True
    assert heartbeats[0].completed_units == 3
    assert heartbeats[0].cancellation_requested is True
    assert len(heartbeats[0].to_json().encode("utf-8")) < 65_536


def test_corrupt_receipt_and_stale_output_fail_closed(tmp_path: Path) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
    )
    receipt = Path(task.commands[0].receipt_path)
    receipt.parent.mkdir(parents=True)
    receipt.write_text("not-json", encoding="utf-8")
    with pytest.raises(
        ReconstructionArtifactError, match="invalid stage receipt"
    ):
        asyncio.run(execute_reconstruction_stage(invocation))

    receipt.unlink()
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    outcome = asyncio.run(execute_reconstruction_stage(invocation))
    Path(outcome.output_refs[0].path).write_text("x" * 17, encoding="utf-8")
    with pytest.raises(ReconstructionArtifactError, match="sha256 differs"):
        verify_artifact_ref(outcome.output_refs[0])


def test_next_stage_rejects_corrupt_prior_stage_artifact(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    first_invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
    )
    first = asyncio.run(execute_reconstruction_stage(first_invocation))
    Path(first.output_refs[0].path).write_text("corrupt", encoding="utf-8")
    next_invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[1],
        prior_outcomes=(first,),
    )

    with pytest.raises(
        ReconstructionArtifactError, match="artifact .* differs"
    ):
        asyncio.run(execute_reconstruction_stage(next_invocation))


def test_duplicate_completion_resolves_to_newer_identical_prefix(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    store = ReconstructionCheckpointStore(request.manifest_store_root)
    planned = store.initialize(request.request_id, task)
    running = store.save(planned.running(), expected_state_id=planned.state_id)
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=task.commands[0],
        prior_outcomes=(),
    )
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    outcome = asyncio.run(execute_reconstruction_stage(invocation))
    completed = running.complete(outcome)
    winner = store.save(completed, expected_state_id=running.state_id)

    duplicate = store.save(completed, expected_state_id=running.state_id)

    assert duplicate.state_id == winner.state_id


def test_report_reconciles_checkpoint_scope_and_storage_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    fake_manifest = SimpleNamespace(
        run_id=request.run.run_id,
        window_id=task.window.window_id,
        synchronization_unit_id=task.window.synchronization_unit_id,
        symbols=task.window.symbols,
        observed_event_count=10,
        synthetic_event_count=15,
        event_count=25,
        manifest_id="manifest-1",
        publication_id="publication-1",
    )
    monkeypatch.setattr(
        RECONSTRUCTION_MODULE,
        "verify_reconstruction_publication",
        lambda _path: fake_manifest,
    )

    report = reconcile_reconstruction_report(request, (state,))
    activity_result = activities.reconstruction_report_activity(
        {"request": request.to_dict()}
    )

    assert report.status == "committed"
    assert report.observed_event_count == 10
    assert report.synthetic_event_count == 15
    assert report.committed_window_count == 1
    resources = report.window_states[0]["resource_usage"]
    assert isinstance(resources, dict)
    assert resources["runtime_seconds"] == 7.0
    assert resources["peak_rss_bytes"] == 1_000
    assert resources["peak_scratch_bytes"] == 100
    assert resources["peak_candidate_amplification"] == 2.0
    assert resources["basis"] == "sum-stage-runtime-max-stage-resources-v1"
    assert activity_result["report"]["report_id"] == report.report_id


def test_report_rejects_manifest_from_wrong_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    state = asyncio.run(
        run_reconstruction_window(
            request,
            task,
            checkpoint_store=ReconstructionCheckpointStore(
                request.manifest_store_root
            ),
            stage_executor=RegisteredReconstructionStageExecutor(),
        )
    )
    monkeypatch.setattr(
        RECONSTRUCTION_MODULE,
        "verify_reconstruction_publication",
        lambda _path: SimpleNamespace(
            run_id=request.run.run_id,
            window_id="wrong-window",
            synchronization_unit_id=task.window.synchronization_unit_id,
            symbols=task.window.symbols,
        ),
    )

    with pytest.raises(ReconstructionReportMismatch, match="scope differs"):
        reconcile_reconstruction_report(request, (state,))


def test_default_worker_registration_includes_reconstruction() -> None:
    assert workflows.ReconstructionRunWorkflow in workflows.DEFAULT_WORKFLOWS
    assert workflows.ReconstructionWindowWorkflow in workflows.DEFAULT_WORKFLOWS
    defaults = activities.default_activities()
    assert activities.reconstruction_window_activity in defaults
    assert activities.reconstruction_report_activity in defaults
    assert (
        workflows.activity_execution_policy(
            "reconstruction_window"
        ).heartbeat_timeout_seconds
        == 60
    )


def test_client_submission_adds_workspace_queues_and_persists_snapshot(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    config = build_orchestration_worker_config(
        workspace=tmp_path,
        runtime_home=tmp_path / "runtime",
    )

    class FakeClient:
        def __init__(self) -> None:
            self.calls = []

        async def start_workflow(self, workflow, payload, **options):
            self.calls.append((workflow, payload, options))
            return SimpleNamespace(id=options["id"], run_id="temporal-run")

    client = FakeClient()
    handle = asyncio.run(
        submit_reconstruction_request(request, config=config, client=client)
    )
    submitted_request = client.calls[0][1]["request"]
    snapshot = ReconstructionCheckpointStore(
        request.manifest_store_root
    ).store.get_job_snapshot(handle.workflow_id)

    assert client.calls[0][0] == "ReconstructionRunWorkflow"
    assert client.calls[0][2]["task_queue"] == config.task_queues.orchestration
    assert submitted_request["task_queues"]["cpu_file"] == (
        config.task_queues.cpu_file
    )
    assert snapshot is not None
    assert snapshot["metadata"]["window_count"] == 1


def test_client_submission_normalizes_missing_temporal_run_id(
    tmp_path: Path,
) -> None:
    """Temporal may omit a run ID from a newly started workflow handle."""
    request = _request(tmp_path)
    config = build_orchestration_worker_config(
        workspace=tmp_path,
        runtime_home=tmp_path / "runtime",
    )

    class FakeClient:
        async def start_workflow(self, workflow, payload, **options):
            return SimpleNamespace(id=options["id"], run_id=None)

    handle = asyncio.run(
        submit_reconstruction_request(
            request,
            config=config,
            client=FakeClient(),
        )
    )

    assert handle.run_id == ""
    snapshot = ReconstructionCheckpointStore(
        request.manifest_store_root
    ).store.get_job_snapshot(handle.workflow_id)
    assert snapshot is not None
    assert snapshot["run_id"] == ""


def test_recovery_submission_uses_fresh_parent_and_child_identities(
    tmp_path: Path,
) -> None:
    """A resume attempt must not collide with earlier Temporal child IDs."""
    request = _request(tmp_path)
    config = build_orchestration_worker_config(
        workspace=tmp_path,
        runtime_home=tmp_path / "runtime",
    )

    class FakeClient:
        def __init__(self) -> None:
            self.calls = []

        async def start_workflow(self, workflow, payload, **options):
            self.calls.append((workflow, payload, options))
            return SimpleNamespace(id=options["id"], run_id="resume-run")

    client = FakeClient()
    handle = asyncio.run(
        submit_reconstruction_request(
            request,
            config=config,
            client=client,
            workflow_id="reconstruction-resume-parent-001",
            execution_attempt_id="resume-001",
        )
    )
    payload = client.calls[0][1]
    snapshot = ReconstructionCheckpointStore(
        request.manifest_store_root
    ).store.get_job_snapshot(handle.workflow_id)
    initial_child_id = workflows._reconstruction_child_workflow_id(
        request.request_id, request.tasks[0].window.window_id
    )
    resumed_child_id = workflows._reconstruction_child_workflow_id(
        request.request_id,
        request.tasks[0].window.window_id,
        execution_attempt_id="resume-001",
    )

    assert handle.workflow_id == "reconstruction-resume-parent-001"
    assert payload["execution_attempt_id"] == "resume-001"
    assert payload["request"]["request_id"] == request.request_id
    assert initial_child_id != resumed_child_id
    assert snapshot is not None
    assert snapshot["metadata"]["execution_attempt_id"] == "resume-001"

    with pytest.raises(ValueError, match="unsupported characters"):
        asyncio.run(
            submit_reconstruction_request(
                request,
                config=config,
                client=client,
                execution_attempt_id="resume token must not enter history",
            )
        )


def test_stale_input_fingerprint_receipt_is_rejected(tmp_path: Path) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    command = task.commands[0]
    outcome = ReconstructionStageOutcomeV1(
        run_id=task.window.run_id,
        window_id=task.window.window_id,
        synchronization_unit_id=task.window.synchronization_unit_id,
        stage=command.stage,
        command_id=command.command_id,
        input_fingerprint="f" * 64,
        status=ReconstructionStageStatus.COMPLETED,
    )
    receipt = Path(command.receipt_path)
    receipt.parent.mkdir(parents=True)
    receipt.write_text(outcome.to_json(), encoding="utf-8")
    invocation = ReconstructionStageInvocationV1(
        run=request.run,
        task=task,
        command=command,
        prior_outcomes=(),
    )

    with pytest.raises(ReconstructionArtifactError, match="input fingerprint"):
        asyncio.run(execute_reconstruction_stage(invocation))


def test_state_store_survives_process_style_restart(tmp_path: Path) -> None:
    task = _task(tmp_path)
    first_store = ReconstructionCheckpointStore(tmp_path / "status")
    state = first_store.initialize("request-a", task)
    running = first_store.save(
        state.running(), expected_state_id=state.state_id
    )
    del first_store

    restored = ReconstructionCheckpointStore(tmp_path / "status").load(
        task.window
    )

    assert restored == running


def test_cancelled_window_resume_discards_entire_disposable_stage_prefix(
    tmp_path: Path,
) -> None:
    task = _task(tmp_path)
    request = _request(tmp_path, task)
    register_reconstruction_stage_handler("test-handler", _stage_handler)
    store = ReconstructionCheckpointStore(request.manifest_store_root)
    state = store.initialize(request.request_id, task)
    state = store.save(state.running(), expected_state_id=state.state_id)
    for command in task.commands[:6]:
        invocation = ReconstructionStageInvocationV1(
            run=request.run,
            task=task,
            command=command,
            prior_outcomes=state.outcomes,
        )
        outcome = asyncio.run(execute_reconstruction_stage(invocation))
        next_state = state.complete(outcome)
        state = store.save(next_state, expected_state_id=state.state_id)
        if command.stage is ReconstructionStage.VALIDATION:
            state = store.save(
                state.validated(), expected_state_id=state.state_id
            )
    cancelled = state.interrupted(ReconstructionCommitPhase.CANCELLED, "stop")
    cancelled = store.save(cancelled, expected_state_id=state.state_id)
    shutil.rmtree(task.scratch_directory)

    resumed = store.save(
        cancelled.running(), expected_state_id=cancelled.state_id
    )

    assert resumed.checkpoint.phase is ReconstructionCommitPhase.RUNNING
    assert resumed.outcomes == ()

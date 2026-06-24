"""Tests for issue-180 sidecar throughput benchmark helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import histdatacom.sidecar.live_smoke as live_smoke
from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.control import JobLifecycle
from histdatacom.sidecar.live_smoke import LiveSidecarStopError
from histdatacom.sidecar.performance import BenchmarkMeasurement
from histdatacom.sidecar.queues import (
    TaskQueueLane,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.runtime import (
    SidecarRuntimePolicy,
    build_sidecar_runtime_policy,
)
from histdatacom.sidecar.supervisor import SidecarStatus
from histdatacom.sidecar.throughput import (
    RuntimeBenchmarkResult,
    ThroughputBenchmarkScenario,
    ThroughputBenchmarkReport,
    ThroughputComparison,
    default_throughput_benchmark_matrix,
    run_live_sidecar_throughput_benchmark,
)


class _ThroughputSupervisor:
    def __init__(
        self,
        *,
        runtime_policy: SidecarRuntimePolicy,
        worker_lanes: tuple[TaskQueueLane, ...],
        **kwargs: object,
    ) -> None:
        self.runtime_policy = runtime_policy
        self.worker_lanes = worker_lanes
        self.kwargs = kwargs
        self.stopped = False

    def start(
        self,
        *,
        executable: Path | None,
        startup_timeout: float,
    ) -> SidecarStatus:
        return self._status("running")

    def stop(self, *, stop_timeout: float = 0.0) -> SidecarStatus:
        self.stopped = True
        return self._status("stopped")

    def status(self, *, repair: bool = False) -> SidecarStatus:
        if self.stopped:
            return self._status("stopped")
        return self._status("running")

    def _status(self, state: str) -> SidecarStatus:
        return SidecarStatus(
            state=state,
            message=state,
            state_dir=str(self.runtime_policy.paths.state_dir),
            pid_file=str(self.runtime_policy.paths.pid_file),
            lock_file=str(self.runtime_policy.paths.lock_file),
            logs={},
            pids={},
            components={
                "server": state,
                **{f"worker:{lane.value}": state for lane in self.worker_lanes},
            },
        )


class _ThroughputStuckStoppingSupervisor(_ThroughputSupervisor):
    def stop(self, *, stop_timeout: float = 0.0) -> SidecarStatus:
        self.stopped = True
        return self._status("stopping")

    def status(self, *, repair: bool = False) -> SidecarStatus:
        if self.stopped:
            return self._status("stopping")
        return self._status("running")


def test_default_throughput_matrix_covers_issue_180_operations(
    tmp_path: Path,
) -> None:
    """The reproducible matrix should cover every issue-180 operation family."""
    scenarios = default_throughput_benchmark_matrix(data_directory=tmp_path)

    operations = {
        operation for scenario in scenarios for operation in scenario.operations
    }

    assert {
        "repository_refresh",
        "validate_urls",
        "bounded_symbol_fanout",
        "download_archives",
        "extract_csv",
        "build_cache",
        "merge_cache",
        "import_skipped_no_influx",
    } <= operations
    assert all(
        not scenario.request.import_to_influxdb for scenario in scenarios
    )
    assert all(scenario.work_item_count >= 1 for scenario in scenarios)
    fanout = next(
        scenario
        for scenario in scenarios
        if scenario.name == "multi-partition-validate-fanout"
    )
    cache = next(
        scenario
        for scenario in scenarios
        if scenario.name == "cache-merge-no-influx"
    )
    assert fanout.work_item_count == 6
    assert fanout.request.timeframes == ("T",)
    assert fanout.request.metadata["temporal_fanout"] == {
        "max_parallel_child_workflows": 2
    }
    assert cache.request.api_return_type == "polars"


def test_throughput_report_serializes_performance_envelope(
    tmp_path: Path,
) -> None:
    """Reports should include runtime metrics and tuning policy."""
    [scenario, *_] = default_throughput_benchmark_matrix(
        data_directory=tmp_path
    )
    runtime = _runtime_result("temporal-runtime", scenario.name, 2.0, 0.75)
    runtime_policy = build_sidecar_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    config = build_sidecar_worker_config(runtime_policy=runtime_policy)
    status = SidecarStatus(
        state="running",
        message="running",
        state_dir=str(tmp_path / "state"),
        pid_file=str(tmp_path / "pid.json"),
        lock_file=str(tmp_path / "lock"),
        logs={},
        pids={},
    )
    report = ThroughputBenchmarkReport(
        comparisons=(
            ThroughputComparison(
                scenario=scenario,
                runtime=runtime,
            ),
        ),
        runtime_startup=BenchmarkMeasurement(
            name="runtime-startup",
            work_item_count=1,
            elapsed_seconds=0.25,
            cpu_seconds=0.0,
            peak_rss_bytes=0,
        ),
        worker_config=config,
        started_status=status,
    )

    payload = report.to_dict()

    assert payload["issue"] == 180
    assert payload["comparisons"][0]["runtime"]["runtime"] == "temporal-runtime"
    assert payload["runtime_startup"]["name"] == "runtime-startup"
    assert "baseline" not in payload["comparisons"][0]
    assert payload["accepted_envelope"]["batch_default"]
    assert payload["accepted_envelope"]["influx_batch_default"]
    assert payload["accepted_envelope"]["fanout_default"]
    assert payload["concurrency_profile"]["network_workers"] >= 1


def test_live_sidecar_throughput_benchmark_records_stopped_status(
    tmp_path: Path,
) -> None:
    [scenario, *_] = _single_scenario_matrix(tmp_path)

    report = run_live_sidecar_throughput_benchmark(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        data_directory=tmp_path / "data",
        scenarios=(scenario,),
        supervisor_factory=_ThroughputSupervisor,
        submit_job=_completed_observed_result,
    )

    assert report.stopped_status is not None
    assert report.stopped_status.state == "stopped"


def test_live_sidecar_throughput_benchmark_fails_on_stopping_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(live_smoke.time, "sleep", lambda _seconds: None)
    [scenario, *_] = _single_scenario_matrix(tmp_path)

    with pytest.raises(LiveSidecarStopError, match="state=stopping"):
        run_live_sidecar_throughput_benchmark(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
            scenarios=(scenario,),
            supervisor_factory=_ThroughputStuckStoppingSupervisor,
            submit_job=_completed_observed_result,
        )


def _runtime_result(
    runtime: str,
    scenario: str,
    elapsed: float,
    cpu: float,
) -> RuntimeBenchmarkResult:
    return RuntimeBenchmarkResult(
        runtime=runtime,
        scenario=scenario,
        measurement=BenchmarkMeasurement(
            name=f"{runtime}:{scenario}",
            work_item_count=1,
            elapsed_seconds=elapsed,
            cpu_seconds=cpu,
            peak_rss_bytes=0,
        ),
        status="COMPLETED",
    )


def _single_scenario_matrix(
    tmp_path: Path,
) -> tuple[ThroughputBenchmarkScenario, ...]:
    return (
        ThroughputBenchmarkScenario(
            name="validate-url",
            request=RunRequest(
                request_id="throughput-stop-check",
                pairs=("eurusd",),
                formats=("ascii",),
                timeframes=("M1",),
                start_yearmonth="202201",
                end_yearmonth="202201",
                data_directory=str(tmp_path / "data"),
                validate_urls=True,
            ),
            operations=("dataset_plan", "validate_urls"),
            work_item_count=1,
        ),
    )


def _completed_observed_result(request: RunRequest, **kwargs: object) -> object:
    return SimpleNamespace(
        snapshot=SimpleNamespace(lifecycle=JobLifecycle.SUCCEEDED),
        result={
            "workflow_name": "RunRequestWorkflow",
            "status": WorkStatus.COMPLETED.value,
            "stage_results": [],
            "artifacts": [],
            "work_items": [],
        },
    )

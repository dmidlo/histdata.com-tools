"""Reproducible foreground versus live sidecar throughput benchmarks."""

from __future__ import annotations

import asyncio
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, cast

from histdatacom import config
from histdatacom.foreground import ForegroundRun
from histdatacom.runtime_contracts import JSONValue, RunRequest, StageResult
from histdatacom.sidecar.client import submit_run_request_and_observe
from histdatacom.sidecar.control import JobLifecycle
from histdatacom.sidecar.live_smoke import (
    DEFAULT_LIVE_SIDECAR_SMOKE_COMPLETION_TIMEOUT,
    DEFAULT_LIVE_SIDECAR_SMOKE_LANES,
    DEFAULT_LIVE_SIDECAR_SMOKE_STARTUP_TIMEOUT,
    DEFAULT_LIVE_SIDECAR_SMOKE_STOP_TIMEOUT,
    TEMPORAL_EXECUTABLE_ENV,
    _stop_live_sidecar,
    _temporal_executable_from_inputs,
)
from histdatacom.sidecar.performance import (
    BenchmarkMeasurement,
    benchmark_operation,
    measure_startup,
)
from histdatacom.sidecar.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    SidecarWorkerConfig,
    TaskQueueLane,
    build_sidecar_worker_config,
)
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy
from histdatacom.sidecar.supervisor import SidecarStatus, SidecarSupervisor
from histdatacom.sidecar.workflows import (
    BATCHING_METADATA_KEY,
    FANOUT_METADATA_KEY,
    MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY,
    MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY,
)

LIVE_SIDECAR_THROUGHPUT_ENV = "HISTDATACOM_LIVE_SIDECAR_THROUGHPUT"
DEFAULT_THROUGHPUT_REQUEST_PREFIX = "live-throughput"
DEFAULT_THROUGHPUT_PERIOD = "202201"
DEFAULT_THROUGHPUT_FANOUT_END_PERIOD = "202203"
DEFAULT_THROUGHPUT_TIMEOUT_SECONDS = "30"

SubmitObservedJob = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class ThroughputBenchmarkScenario:
    """One request shape in the issue-180 benchmark matrix."""

    name: str
    request: RunRequest
    operations: tuple[str, ...]
    work_item_count: int
    notes: str = ""

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible scenario metadata."""
        return {
            "name": self.name,
            "request": self.request.to_dict(),
            "operations": list(self.operations),
            "work_item_count": self.work_item_count,
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class RuntimeBenchmarkResult:
    """Measured result for one runtime/scenario pair."""

    runtime: str
    scenario: str
    measurement: BenchmarkMeasurement
    status: str
    stage_counts: dict[str, int] = field(default_factory=dict)
    artifact_count: int = 0
    failure_count: int = 0
    retry_count: int = 0
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def cpu_utilization_ratio(self) -> float:
        """Return process CPU seconds divided by elapsed wall seconds."""
        elapsed = self.measurement.elapsed_seconds
        if elapsed <= 0:
            return 0.0
        return float(self.measurement.cpu_seconds) / float(elapsed)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible runtime measurement metadata."""
        return {
            "runtime": self.runtime,
            "scenario": self.scenario,
            "measurement": self.measurement.to_dict(),
            "status": self.status,
            "stage_counts": dict(self.stage_counts),
            "artifact_count": self.artifact_count,
            "failure_count": self.failure_count,
            "retry_count": self.retry_count,
            "cpu_utilization_ratio": self.cpu_utilization_ratio,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class ThroughputComparison:
    """Foreground and sidecar measurements for the same scenario."""

    scenario: ThroughputBenchmarkScenario
    foreground: RuntimeBenchmarkResult
    sidecar: RuntimeBenchmarkResult

    @property
    def sidecar_to_foreground_elapsed_ratio(self) -> float:
        """Return sidecar elapsed seconds divided by foreground seconds."""
        foreground_elapsed = self.foreground.measurement.elapsed_seconds
        if foreground_elapsed <= 0:
            return 0.0
        return float(self.sidecar.measurement.elapsed_seconds) / float(
            foreground_elapsed
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible comparison metadata."""
        return {
            "scenario": self.scenario.to_dict(),
            "foreground": self.foreground.to_dict(),
            "sidecar": self.sidecar.to_dict(),
            "sidecar_to_foreground_elapsed_ratio": (
                self.sidecar_to_foreground_elapsed_ratio
            ),
        }


@dataclass(frozen=True, slots=True)
class ThroughputBenchmarkReport:
    """Complete issue-180 throughput benchmark report."""

    comparisons: tuple[ThroughputComparison, ...]
    sidecar_startup: BenchmarkMeasurement
    worker_config: SidecarWorkerConfig
    started_status: SidecarStatus
    stopped_status: SidecarStatus | None = None
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible report metadata."""
        return {
            "schema_version": 1,
            "issue": 180,
            "matrix": [
                comparison.scenario.to_dict() for comparison in self.comparisons
            ],
            "comparisons": [
                comparison.to_dict() for comparison in self.comparisons
            ],
            "sidecar_startup": self.sidecar_startup.to_dict(),
            "worker_config": self.worker_config.to_dict(),
            "started_status": self.started_status.to_dict(),
            "stopped_status": (
                self.stopped_status.to_dict()
                if self.stopped_status is not None
                else None
            ),
            "concurrency_profile": (
                self.worker_config.concurrency_profile.to_dict()
            ),
            "accepted_envelope": accepted_performance_envelope(),
            "notes": list(self.notes),
        }

    def to_json(self) -> str:
        """Return formatted JSON for operator-facing reports."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


def default_throughput_benchmark_matrix(
    *,
    data_directory: Path | str,
    request_id_prefix: str = DEFAULT_THROUGHPUT_REQUEST_PREFIX,
    period: str = DEFAULT_THROUGHPUT_PERIOD,
    fanout_end_period: str = DEFAULT_THROUGHPUT_FANOUT_END_PERIOD,
    requests_timeout: str = DEFAULT_THROUGHPUT_TIMEOUT_SECONDS,
    max_work_items_per_batch: int = 1,
    max_parallel_child_workflows: int = 2,
) -> tuple[ThroughputBenchmarkScenario, ...]:
    """Return the issue-180/181 representative non-Influx benchmark matrix."""
    data_root = Path(data_directory).expanduser()
    metadata: dict[str, JSONValue] = {
        "requests_timeout": requests_timeout,
        BATCHING_METADATA_KEY: {
            MAX_WORK_ITEMS_PER_BATCH_METADATA_KEY: max_work_items_per_batch
        },
        FANOUT_METADATA_KEY: {
            MAX_PARALLEL_CHILD_WORKFLOWS_METADATA_KEY: (
                max_parallel_child_workflows
            )
        },
        "benchmark_issue": 180,
    }

    def request(name: str, **kwargs: Any) -> RunRequest:
        return RunRequest(
            request_id=f"{request_id_prefix}-{name}",
            pairs=("eurusd",),
            formats=("ascii",),
            timeframes=("M1",),
            start_yearmonth=period,
            end_yearmonth=period,
            data_directory=str(data_root / name),
            zip_persist=True,
            metadata=dict(metadata),
            **kwargs,
        )

    def fanout_request(name: str, **kwargs: Any) -> RunRequest:
        fanout_metadata: dict[str, JSONValue] = {
            **metadata,
            "benchmark_issue": 181,
            "benchmark_issues": cast(JSONValue, [180, 181]),
        }
        return RunRequest(
            request_id=f"{request_id_prefix}-{name}",
            pairs=("eurusd", "gbpusd"),
            formats=("ascii",),
            timeframes=("T",),
            start_yearmonth=period,
            end_yearmonth=fanout_end_period,
            data_directory=str(data_root / name),
            zip_persist=True,
            metadata=fanout_metadata,
            **kwargs,
        )

    return (
        ThroughputBenchmarkScenario(
            name="repository-refresh",
            request=request(
                "repository-refresh",
                available_remote_data=True,
            ),
            operations=("repository_refresh",),
            work_item_count=1,
            notes="Repository metadata fetch/list path.",
        ),
        ThroughputBenchmarkScenario(
            name="validate-url",
            request=request("validate-url", validate_urls=True),
            operations=("dataset_plan", "validate_urls"),
            work_item_count=1,
            notes="One EURUSD M1 HistData archive page validation.",
        ),
        ThroughputBenchmarkScenario(
            name="multi-partition-validate-fanout",
            request=fanout_request(
                "multi-partition-validate-fanout",
                validate_urls=True,
            ),
            operations=(
                "dataset_plan",
                "bounded_symbol_fanout",
                "validate_urls",
            ),
            work_item_count=2
            * _yearmonth_span_count(period, fanout_end_period),
            notes=(
                "Two-pair, multi-month tick validation request that exercises "
                "bounded parallel SymbolTimeframeWorkflow fan-out."
            ),
        ),
        ThroughputBenchmarkScenario(
            name="download-extract",
            request=request(
                "download-extract",
                validate_urls=True,
                download_data_archives=True,
                extract_csvs=True,
            ),
            operations=(
                "dataset_plan",
                "validate_urls",
                "download_archives",
                "extract_csv",
            ),
            work_item_count=1,
            notes="One archive download and CSV extraction without Influx.",
        ),
        ThroughputBenchmarkScenario(
            name="cache-merge-no-influx",
            request=request(
                "cache-merge-no-influx",
                validate_urls=True,
                download_data_archives=True,
                api_return_type="polars",
            ),
            operations=(
                "dataset_plan",
                "validate_urls",
                "download_archives",
                "build_cache",
                "merge_cache",
                "import_skipped_no_influx",
            ),
            work_item_count=1,
            notes=(
                "Cache build/merge path with ImportWorkflow intentionally "
                "omitted because no live Influx target is required."
            ),
        ),
    )


def _yearmonth_span_count(start: str, end: str) -> int:
    start_year = int(start[:4])
    start_month = int(start[4:])
    end_year = int(end[:4])
    end_month = int(end[4:])
    return (end_year - start_year) * 12 + end_month - start_month + 1


def accepted_performance_envelope() -> dict[str, JSONValue]:
    """Return the documented issue-180/181 performance acceptance envelope."""
    return {
        "lane_defaults": (
            "Keep orchestration=1, network=legacy CPU workers * 3, "
            "cpu-file=legacy CPU workers, influx=1."
        ),
        "batch_default": (
            "Keep max_work_items_per_batch=64 for production requests; "
            "the benchmark matrix uses 1 to force visible child handoff."
        ),
        "influx_batch_default": (
            "Keep the CLI/API Influx batch_size default at 5000; use smaller "
            "values only for constrained memory, diagnostics, or fixtures."
        ),
        "fanout_default": (
            "Keep max_parallel_child_workflows=4 for production requests; "
            "the live fan-out benchmark uses 2 to prove bounded windows."
        ),
        "fanout_policy": (
            "Only independent SymbolTimeframeWorkflow period batches fan out. "
            "Repository refresh, dataset planning, and operation-family child "
            "workflows preserve stage order inside each partition."
        ),
        "sidecar_overhead": (
            "Live sidecar runs may be slower than foreground for single-item "
            "requests because Temporal startup and workflow bookkeeping are "
            "fixed costs; throughput judgment should focus on bounded "
            "history, successful artifact handoff, and lane concurrency."
        ),
        "known_tradeoffs": [
            "Repository and HistData network timings are externally variable.",
            (
                "Influx import has contract-backed workflow coverage without "
                "a live target; live Influx auth, permissions, latency, and "
                "server-side rejection behavior remain target-specific."
            ),
            "Parent workflow summaries intentionally omit full leaf histories.",
        ],
    }


def benchmark_foreground_matrix(
    scenarios: Sequence[ThroughputBenchmarkScenario],
) -> tuple[RuntimeBenchmarkResult, ...]:
    """Run all scenarios through the queue-free foreground runtime."""
    return tuple(
        benchmark_foreground_scenario(scenario) for scenario in scenarios
    )


def benchmark_foreground_scenario(
    scenario: ThroughputBenchmarkScenario,
) -> RuntimeBenchmarkResult:
    """Run one scenario through the queue-free foreground runtime."""
    captured: dict[str, Any] = {}

    def run() -> None:
        _reset_foreground_globals()
        runner = ForegroundRun(
            scenario.request,
            _foreground_args(scenario.request),
        )
        captured["output"] = runner.run()
        captured["stage_results"] = runner.stage_results

    measurement = benchmark_operation(
        f"foreground:{scenario.name}",
        run,
        work_item_count=scenario.work_item_count,
        metadata={
            "runtime": "foreground",
            "operations": list(scenario.operations),
        },
    )
    stage_results = tuple(captured.get("stage_results") or ())
    return RuntimeBenchmarkResult(
        runtime="foreground",
        scenario=scenario.name,
        measurement=measurement,
        status=_status_from_stage_results(stage_results),
        stage_counts=_stage_counts(stage_results),
        artifact_count=_artifact_count(stage_results),
        failure_count=_failure_count(stage_results),
        retry_count=_retry_count(stage_results),
        metadata={"output_type": type(captured.get("output")).__name__},
    )


def run_live_sidecar_throughput_benchmark(
    *,
    workspace: Path | str,
    runtime_home: Path | str,
    data_directory: Path | str,
    temporal_executable: Path | str | None = None,
    scenarios: Sequence[ThroughputBenchmarkScenario] | None = None,
    startup_timeout: float = DEFAULT_LIVE_SIDECAR_SMOKE_STARTUP_TIMEOUT,
    completion_timeout: float = DEFAULT_LIVE_SIDECAR_SMOKE_COMPLETION_TIMEOUT,
    stop_timeout: float = DEFAULT_LIVE_SIDECAR_SMOKE_STOP_TIMEOUT,
    namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
    task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
    worker_lanes: Sequence[str | TaskQueueLane] = (
        DEFAULT_LIVE_SIDECAR_SMOKE_LANES
    ),
    environ: Mapping[str, str] | None = None,
    supervisor_factory: Callable[..., SidecarSupervisor] = SidecarSupervisor,
    submit_job: SubmitObservedJob | None = None,
) -> ThroughputBenchmarkReport:
    """Run foreground and live sidecar throughput comparisons."""
    scenario_matrix = tuple(
        scenarios
        if scenarios is not None
        else default_throughput_benchmark_matrix(data_directory=data_directory)
    )
    foreground_results = {
        result.scenario: result
        for result in benchmark_foreground_matrix(scenario_matrix)
    }
    env = dict(environ or {})
    executable = _temporal_executable_from_inputs(
        temporal_executable=temporal_executable,
        environ=env,
    )
    runtime_policy = build_sidecar_runtime_policy(
        workspace=workspace,
        runtime_home=runtime_home,
        check_ports=True,
    )
    lanes = tuple(TaskQueueLane.from_value(lane) for lane in worker_lanes)
    supervisor = supervisor_factory(
        runtime_policy=runtime_policy,
        namespace=namespace,
        task_queue_prefix=task_queue_prefix,
        worker_lanes=lanes,
    )
    started_status: SidecarStatus | None = None
    stopped_status: SidecarStatus | None = None
    worker_config: SidecarWorkerConfig | None = None
    sidecar_startup: BenchmarkMeasurement | None = None
    try:
        started_status, startup_seconds = measure_startup(
            lambda: supervisor.start(
                executable=executable,
                startup_timeout=startup_timeout,
            )
        )
        sidecar_startup = BenchmarkMeasurement(
            name="sidecar-startup",
            work_item_count=1,
            elapsed_seconds=startup_seconds,
            cpu_seconds=0.0,
            peak_rss_bytes=0,
            metadata={"runtime": "sidecar"},
        )
        worker_config = build_sidecar_worker_config(
            runtime_policy=supervisor.runtime_policy,
            namespace=namespace,
            task_queue_prefix=task_queue_prefix,
        )
        sidecar_results = {
            result.scenario: result
            for result in (
                _benchmark_sidecar_scenario(
                    scenario,
                    config=worker_config,
                    supervisor=supervisor,
                    completion_timeout=completion_timeout,
                    submit_job=submit_job,
                )
                for scenario in scenario_matrix
            )
        }
    finally:
        try:
            stopped_status = _stop_live_sidecar(
                supervisor,
                stop_timeout=stop_timeout,
            )
        except Exception:
            stopped_status = None
    if (
        started_status is None
        or worker_config is None
        or sidecar_startup is None
    ):
        raise RuntimeError("sidecar throughput benchmark did not start")

    comparisons = tuple(
        ThroughputComparison(
            scenario=scenario,
            foreground=foreground_results[scenario.name],
            sidecar=sidecar_results[scenario.name],
        )
        for scenario in scenario_matrix
    )
    return ThroughputBenchmarkReport(
        comparisons=comparisons,
        sidecar_startup=sidecar_startup,
        worker_config=worker_config,
        started_status=started_status,
        stopped_status=stopped_status,
        notes=(
            f"{TEMPORAL_EXECUTABLE_ENV} was "
            f"{'provided' if executable is not None else 'packaged/default'}.",
        ),
    )


def write_throughput_report(
    report: ThroughputBenchmarkReport,
    path: Path | str,
) -> Path:
    """Write a throughput benchmark report as formatted JSON."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(f"{report.to_json()}\n", encoding="utf-8")
    return output


def _benchmark_sidecar_scenario(
    scenario: ThroughputBenchmarkScenario,
    *,
    config: SidecarWorkerConfig,
    supervisor: SidecarSupervisor,
    completion_timeout: float,
    submit_job: SubmitObservedJob | None,
) -> RuntimeBenchmarkResult:
    captured: dict[str, Any] = {}

    def run() -> None:
        captured["cpu_before"] = _sidecar_process_cpu_snapshot(supervisor)
        if submit_job is not None:
            result = submit_job(
                scenario.request,
                config=config,
                supervisor=supervisor,
                start_if_needed=False,
                wait_for_result=True,
            )
        else:
            result = asyncio.run(
                asyncio.wait_for(
                    submit_run_request_and_observe(
                        scenario.request,
                        config=config,
                        supervisor=supervisor,
                        start_if_needed=False,
                        wait_for_result=True,
                    ),
                    timeout=completion_timeout,
                )
            )
        _validate_sidecar_job_result(result)
        captured["cpu_after"] = _sidecar_process_cpu_snapshot(supervisor)
        captured["result"] = result
        captured["payload"] = _sidecar_payload(result)

    measurement = benchmark_operation(
        f"sidecar:{scenario.name}",
        run,
        work_item_count=scenario.work_item_count,
        metadata={
            "runtime": "sidecar",
            "operations": list(scenario.operations),
        },
    )
    payload = _mapping(captured.get("payload"))
    stage_results = _stage_results_from_payload(payload)
    sidecar_cpu_seconds = _cpu_delta_seconds(
        _mapping(captured.get("cpu_before")),
        _mapping(captured.get("cpu_after")),
    )
    return RuntimeBenchmarkResult(
        runtime="sidecar",
        scenario=scenario.name,
        measurement=measurement,
        status=str(payload.get("status") or "UNKNOWN"),
        stage_counts=_stage_counts(stage_results),
        artifact_count=len(_list_payload(payload.get("artifacts"))),
        failure_count=_failure_count(stage_results),
        retry_count=_retry_count(stage_results),
        metadata={
            "workflow_name": str(payload.get("workflow_name", "")),
            "work_item_count": len(_list_payload(payload.get("work_items"))),
            "child_stage_count": sum(
                _int_metric(stage, "child_stage_count")
                for stage in stage_results
            ),
            "sidecar_process_cpu_seconds": sidecar_cpu_seconds,
            "sidecar_process_cpu_utilization_ratio": (
                sidecar_cpu_seconds / measurement.elapsed_seconds
                if measurement.elapsed_seconds > 0
                else 0.0
            ),
        },
    )


def _validate_sidecar_job_result(result: Any) -> None:
    snapshot = getattr(result, "snapshot", None)
    if snapshot is not None and snapshot.lifecycle != JobLifecycle.SUCCEEDED:
        raise RuntimeError(
            f"sidecar benchmark job did not succeed: {snapshot.lifecycle}"
        )


def _sidecar_payload(result: Any) -> Mapping[str, Any]:
    payload = getattr(result, "result", result)
    if isinstance(payload, Mapping):
        return payload
    snapshot = getattr(result, "snapshot", None)
    if snapshot is not None:
        snapshot_payload = snapshot.to_dict()
        result_payload = snapshot_payload.get("result")
        if isinstance(result_payload, Mapping):
            return result_payload
    return {}


def _foreground_args(request: RunRequest) -> dict[str, Any]:
    return {
        "default_download_dir": _data_dir_arg(request.data_directory),
        "requests_timeout": request.metadata.get(
            "requests_timeout",
            DEFAULT_THROUGHPUT_TIMEOUT_SECONDS,
        ),
        "from_api": bool(request.api_return_type)
        or request.available_remote_data,
        "batch_size": request.batch_size,
        "delete_after_influx": request.delete_after_influx,
        "by": str(request.metadata.get("repo_sort", "") or ""),
    }


def _data_dir_arg(path: str) -> str:
    value = str(Path(path).expanduser())
    return value if value.endswith("/") else f"{value}/"


def _reset_foreground_globals() -> None:
    config.REPO_DATA = {}
    config.REPO_DATA_FILE_EXISTS = False
    config.FILTER_PAIRS = None


def _status_from_stage_results(
    stage_results: Sequence[StageResult],
) -> str:
    if any(result.failure is not None for result in stage_results):
        return "FAILED"
    if any(result.status.value == "RETRIED" for result in stage_results):
        return "RETRIED"
    return "COMPLETED"


def _stage_counts(stage_results: Sequence[StageResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in stage_results:
        counts[result.stage] = counts.get(result.stage, 0) + 1
    return counts


def _artifact_count(stage_results: Sequence[StageResult]) -> int:
    return sum(len(result.artifacts) for result in stage_results)


def _failure_count(stage_results: Sequence[StageResult]) -> int:
    return sum(1 for result in stage_results if result.failure is not None)


def _retry_count(stage_results: Sequence[StageResult]) -> int:
    return sum(
        1 for result in stage_results if result.status.value == "RETRIED"
    )


def _stage_results_from_payload(
    payload: Mapping[str, Any],
) -> tuple[StageResult, ...]:
    return tuple(
        StageResult.from_dict(_mapping(item))
        for item in _list_payload(payload.get("stage_results"))
        if isinstance(item, Mapping)
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_payload(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _int_metric(result: StageResult, key: str) -> int:
    value = result.metrics.get(key, 0)
    if not isinstance(value, (int, float, str)):
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _sidecar_process_cpu_snapshot(
    supervisor: SidecarSupervisor,
) -> dict[str, float]:
    try:
        pids = supervisor.status(repair=False).pids
    except Exception:
        return {}
    return {
        component: seconds
        for component, pid in pids.items()
        if (seconds := _process_cpu_seconds(pid)) >= 0.0
    }


def _process_cpu_seconds(pid: int) -> float:
    try:
        completed = subprocess.run(
            ["ps", "-o", "time=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return -1.0
    if completed.returncode != 0:
        return -1.0
    return _parse_ps_time(completed.stdout.strip())


def _parse_ps_time(value: str) -> float:
    if not value:
        return -1.0
    day_count = 0
    time_value = value
    if "-" in value:
        days, time_value = value.split("-", 1)
        try:
            day_count = int(days)
        except ValueError:
            return -1.0
    parts = time_value.split(":")
    try:
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])
        elif len(parts) == 2:
            hours = 0
            minutes = int(parts[0])
            seconds = float(parts[1])
        else:
            return -1.0
    except ValueError:
        return -1.0
    return (day_count * 86400.0) + (hours * 3600.0) + (minutes * 60.0) + seconds


def _cpu_delta_seconds(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> float:
    total = 0.0
    for component, after_value in after.items():
        try:
            end = float(after_value)
            start = float(before.get(component, 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        total += max(0.0, end - start)
    return total

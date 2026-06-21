"""Tests for issue-180 sidecar throughput benchmark helpers."""

from __future__ import annotations

from pathlib import Path

from histdatacom.sidecar.performance import BenchmarkMeasurement
from histdatacom.sidecar.queues import build_sidecar_worker_config
from histdatacom.sidecar.runtime import build_sidecar_runtime_policy
from histdatacom.sidecar.supervisor import SidecarStatus
from histdatacom.sidecar.throughput import (
    RuntimeBenchmarkResult,
    ThroughputBenchmarkReport,
    ThroughputComparison,
    default_throughput_benchmark_matrix,
)


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
        "download_archives",
        "extract_csv",
        "build_cache",
        "merge_cache",
        "import_skipped_no_influx",
    } <= operations
    assert all(
        not scenario.request.import_to_influxdb for scenario in scenarios
    )
    assert all(scenario.work_item_count == 1 for scenario in scenarios)
    assert scenarios[-1].request.api_return_type == "polars"


def test_throughput_report_serializes_performance_envelope(
    tmp_path: Path,
) -> None:
    """Reports should include comparison metrics and tuning policy."""
    [scenario, *_] = default_throughput_benchmark_matrix(
        data_directory=tmp_path
    )
    foreground = _runtime_result("foreground", scenario.name, 1.0, 0.5)
    sidecar = _runtime_result("sidecar", scenario.name, 2.0, 0.75)
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
                foreground=foreground,
                sidecar=sidecar,
            ),
        ),
        sidecar_startup=BenchmarkMeasurement(
            name="sidecar-startup",
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
    assert (
        payload["comparisons"][0]["sidecar_to_foreground_elapsed_ratio"] == 2.0
    )
    assert payload["accepted_envelope"]["batch_default"]
    assert payload["concurrency_profile"]["network_workers"] >= 1


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

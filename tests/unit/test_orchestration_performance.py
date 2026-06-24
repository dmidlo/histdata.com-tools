"""Tests for orchestration performance policy and benchmark helpers."""

from __future__ import annotations

from pathlib import Path

from histdatacom.activity_stages import (
    UrlPageData,
    build_cache_work_item,
    import_to_influx_work_item,
    validate_url_work_item,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.runtime_contracts import RunRequest, WorkItem, WorkStatus
from histdatacom.orchestration.performance import (
    benchmark_operation,
    benchmark_partition_batching,
    build_orchestration_concurrency_profile,
    compare_partition_batching,
    measure_startup,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"


def test_orchestration_concurrency_profile_preserves_legacy_multipliers(
    monkeypatch,
) -> None:
    """Network and CPU/file lanes should mirror current pool sizing."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)

    profile = build_orchestration_concurrency_profile(cpu_utilization="medium")

    assert profile.base_workers == 5
    assert profile.workers_for_lane("network") == 15
    assert profile.workers_for_lane("cpu_file") == 5
    assert profile.workers_for_lane("influx") == 1
    assert profile.worker_options_for_lane("network") == {
        "max_concurrent_activities": 15
    }


def test_orchestration_concurrency_profile_allows_lane_overrides(
    monkeypatch,
) -> None:
    """Operators should be able to tune a lane without changing code."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)

    profile = build_orchestration_concurrency_profile(
        cpu_utilization="high",
        lane_overrides={"network": 9},
    )

    assert profile.workers_for_lane("network") == 9
    assert profile.workers_for_lane("cpu-file") == 7
    assert profile.to_dict()["source"] == "explicit_override"


def test_benchmark_operation_tracks_throughput_resources_and_startup() -> None:
    """Benchmark helpers should emit compact resource measurements."""
    value, startup_seconds = measure_startup(lambda: ["started"])

    measurement = benchmark_operation(
        "local-fake",
        lambda: value.append("ran"),
        work_item_count=1,
        retry_count=2,
        startup_seconds=startup_seconds,
        metadata={"lane": "network"},
    )

    payload = measurement.to_dict()
    assert value == ["started", "ran"]
    assert payload["name"] == "local-fake"
    assert payload["work_item_count"] == 1
    assert payload["retry_count"] == 2
    assert payload["elapsed_seconds"] >= 0.0
    assert payload["cpu_seconds"] >= 0.0
    assert payload["peak_rss_bytes"] >= 0
    assert payload["throughput_per_second"] >= 0.0
    assert payload["startup_seconds"] >= 0.0
    assert payload["metadata"] == {"lane": "network"}


def test_partition_batching_benchmark_compares_coarse_and_period_batches() -> (
    None
):
    """Batching benchmark metadata should expose bounded child fanout."""
    request = _batch_request()
    work_items = tuple(
        _batch_work_item(f"2022-{month:02d}") for month in range(1, 6)
    )

    comparison = compare_partition_batching(
        request,
        work_items,
        max_work_items_per_batch=2,
    )
    measurement = benchmark_partition_batching(
        request,
        work_items,
        max_work_items_per_batch=2,
    )

    assert comparison == {
        "coarse_partition_count": 1,
        "period_batch_count": 3,
        "work_item_count": 5,
        "max_work_items_per_batch": 2,
        "coarse_max_work_items_per_child": 5,
        "period_batch_max_work_items_per_child": 2,
        "max_child_work_item_reduction": 3,
    }
    assert measurement.name == "period-batch-partitioning"
    assert measurement.work_item_count == 5
    assert measurement.metadata == comparison


def test_fixture_baseline_measurements_cover_representative_operations(
    tmp_path: Path,
) -> None:
    """Local fakes/fixtures should support validate/cache/import baselines."""
    csv_path = FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv"
    source_csv = tmp_path / csv_path.name
    source_csv.write_bytes(csv_path.read_bytes())
    emitted_lines: list[list[str]] = []

    validate_item = WorkItem(
        work_id="work-validate",
        status=WorkStatus.URL_NEW,
        url=(
            "http://www.histdata.com/download-free-forex-data/"
            "?/ascii/1-minute-bar-quotes/eurusd/2022"
        ),
    )
    cache_item = WorkItem(
        work_id="work-cache",
        status=WorkStatus.CSV_FILE,
        data_dir=f"{tmp_path}/",
        csv_filename=source_csv.name,
        zip_filename="missing.zip",
        data_format="ascii",
        data_timeframe="M1",
        data_fxpair="eurusd",
    )
    import_item = cache_item.with_status(WorkStatus.CACHE_READY)
    import_item = WorkItem.from_dict(
        {
            **import_item.to_dict(),
            "cache_filename": CACHE_FILENAME,
        }
    )

    validate = benchmark_operation(
        "validate-url-fixture",
        lambda: validate_url_work_item(
            validate_item,
            args={"default_download_dir": f"{tmp_path}/"},
            fetch_page_data=lambda url, timeout: UrlPageData(
                html=_form_html(),
                encoding="gzip",
                bytes_length="123",
                headers={},
            ),
        ),
        work_item_count=1,
        metadata={"lane": "network"},
    )
    cache = benchmark_operation(
        "build-cache-fixture",
        lambda: build_cache_work_item(
            cache_item,
            args={"default_download_dir": f"{tmp_path}/"},
        ),
        work_item_count=1,
        metadata={"lane": "cpu-file"},
    )
    import_measurement = benchmark_operation(
        "import-influx-fixture",
        lambda: import_to_influx_work_item(
            import_item,
            args={
                "default_download_dir": f"{tmp_path}/",
                "batch_size": "2",
                "delete_after_influx": False,
            },
            emit_lines=emitted_lines.append,
        ),
        work_item_count=1,
        metadata={"lane": "influx"},
    )

    assert (tmp_path / CACHE_FILENAME).exists()
    assert len(emitted_lines) == 2
    assert {item.name for item in (validate, cache, import_measurement)} == {
        "validate-url-fixture",
        "build-cache-fixture",
        "import-influx-fixture",
    }
    assert all(item.throughput_per_second >= 0 for item in (validate, cache))


def _form_html(*, token: str = "token") -> str:
    """Return a minimal HistData download form."""
    return f"""
    <html>
      <form id="file_down">
        <input id="tk" value="{token}">
        <input id="date" value="2022">
        <input id="datemonth" value="2022">
        <input id="platform" value="ASCII">
        <input id="timeframe" value="M1">
        <input id="fxpair" value="eurusd">
      </form>
    </html>
    """


def _batch_request() -> RunRequest:
    return RunRequest(
        request_id="run-benchmark",
        pairs=("EURUSD",),
        formats=("ascii",),
        timeframes=("M1",),
        validate_urls=True,
    )


def _batch_work_item(datemonth: str) -> WorkItem:
    return WorkItem(
        work_id=f"work-eurusd-m1-{datemonth}",
        status=WorkStatus.URL_NEW,
        url=f"https://example.test/eurusd/M1/{datemonth}",
        data_format="ascii",
        data_timeframe="M1",
        data_fxpair="EURUSD",
        data_datemonth=datemonth,
    )

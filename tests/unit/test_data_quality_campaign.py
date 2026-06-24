"""Tests for data-quality full-dataset campaign planning."""

from __future__ import annotations

from histdatacom.data_quality.campaign import (
    CAMPAIGN_PLAN_SCHEMA_VERSION,
    CAMPAIGN_REPORT_SCHEMA_VERSION,
    build_full_dataset_campaign_report,
    build_storage_backed_campaign_plan,
)


def test_campaign_report_uses_repo_ranges_for_work_surface() -> None:
    """The campaign preflight should enumerate work from .repo rows."""
    report = build_full_dataset_campaign_report(
        issue_number=233,
        repo_data={
            "audusd": {"start": "202201", "end": "202203"},
            "bcousd": {"start": "202202", "end": "202203"},
            "hash": "repo-hash",
        },
        symbols=("audusd", "bcousd"),
        data_directory="data",
        current_yearmonth="202203",
    )

    assert report["schema_version"] == CAMPAIGN_REPORT_SCHEMA_VERSION
    assert report["status"] == "deferred"
    assert report["repo"]["pair_count"] == 2
    assert report["totals"]["work_item_count"] == 45
    assert report["totals"]["deep_quality_work_item_count"] == 10
    assert report["totals"]["deferred_work_item_count"] == 35
    assert report["totals"]["work_items_by_dimension"] == [
        {"format": "ascii", "timeframe": "M1", "work_item_count": 5},
        {"format": "ascii", "timeframe": "T", "work_item_count": 5},
        {"format": "excel", "timeframe": "M1", "work_item_count": 5},
        {"format": "metastock", "timeframe": "M1", "work_item_count": 5},
        {"format": "metatrader", "timeframe": "M1", "work_item_count": 5},
        {"format": "ninjatrader", "timeframe": "M1", "work_item_count": 5},
        {"format": "ninjatrader", "timeframe": "T_ASK", "work_item_count": 5},
        {"format": "ninjatrader", "timeframe": "T_BID", "work_item_count": 5},
        {"format": "ninjatrader", "timeframe": "T_LAST", "work_item_count": 5},
    ]


def test_campaign_report_blocks_on_disk_threshold() -> None:
    """Observed disk below the operator threshold should block the campaign."""
    report = build_full_dataset_campaign_report(
        issue_number=233,
        repo_data={"audusd": {"start": "202201", "end": "202201"}},
        symbols=("audusd",),
        data_directory="data",
        disk_available_bytes=10,
        minimum_free_bytes=100,
        current_yearmonth="202201",
    )

    assert report["status"] == "blocked"
    assert report["disk_preflight"]["status"] == "blocked"
    assert report["disk_preflight"]["available_gib"] == 0.0


def test_campaign_report_records_missing_repo_symbols() -> None:
    """Missing .repo rows should be explicit failures, not silent omissions."""
    report = build_full_dataset_campaign_report(
        issue_number=233,
        repo_data={},
        symbols=("audusd",),
        data_directory="data",
        current_yearmonth="202201",
    )

    assert report["status"] == "failed"
    assert report["missing_symbols"] == ["audusd"]
    assert report["symbols"][0]["repo_status"] == "missing"


def test_storage_backed_campaign_plan_updates_repo_before_cache_cleanup() -> (
    None
):
    """Each bounded slice should refresh .repo quality before cleanup."""
    plan = build_storage_backed_campaign_plan(
        issue_number=240,
        repo_data={
            "audusd": {"start": "202201", "end": "202202"},
            "bcousd": {"start": "202201", "end": "202201"},
        },
        symbols=("audusd", "bcousd"),
        data_directory="data",
        reports_directory="data/.quality/issue-240",
        formats=("ascii",),
        timeframes=("M1",),
        current_yearmonth="202202",
        slice_symbol_count=1,
        cleanup_mode="cache",
        platform_executable_bundled=True,
    )

    first_slice = plan["slices"][0]
    commands = first_slice["commands"]

    assert plan["schema_version"] == CAMPAIGN_PLAN_SCHEMA_VERSION
    assert plan["status"] == "ready"
    assert plan["slice_count"] == 2
    assert plan["repo_quality_contract"]["required_after_each_slice"] is True
    assert plan["repo_quality_contract"]["repo_path"] == "data/.repo"
    assert plan["cleanup_policy"]["mode"] == "cache"
    assert commands[0] == {
        "step": "download_extract_slice",
        "command": (
            "histdatacom -D -X -p audusd -f ascii -t 1-minute-bar-quotes "
            "--data-directory data"
        ),
    }
    assert commands[1]["step"] == "refresh_repo_quality"
    assert commands[1]["updates_repo"] is True
    assert commands[1]["repo_path"] == "data/.repo"
    assert commands[1]["command"] == (
        "histdatacom --repo-quality --quality-target data/ASCII/M1/audusd "
        "--quality-checks all --quality-report "
        "data/.quality/issue-240/"
        "issue-240-001-ascii-m1-audusd-quality.json "
        "--data-directory data"
    )
    assert commands[2] == {
        "step": "cleanup_after_repo_quality",
        "command": "find data/ASCII/M1/audusd -name .data -type f -delete",
        "preserves_repo": True,
        "preserves_quality_reports": True,
    }


def test_storage_backed_campaign_plan_preserves_cache_by_default() -> None:
    """Normal campaign planning should not delete cache artifacts."""
    plan = build_storage_backed_campaign_plan(
        issue_number=240,
        repo_data={"audusd": {"start": "202201", "end": "202201"}},
        symbols=("audusd",),
        data_directory="data",
        reports_directory="data/.quality/issue-240",
        formats=("ascii",),
        timeframes=("M1",),
        current_yearmonth="202201",
        platform_executable_bundled=True,
    )

    commands = plan["slices"][0]["commands"]

    assert plan["cleanup_policy"]["mode"] == "none"
    assert plan["cleanup_policy"]["removes"] == "nothing"
    assert [command["step"] for command in commands] == [
        "download_extract_slice",
        "refresh_repo_quality",
    ]
    assert commands[1]["updates_repo"] is True


def test_storage_backed_campaign_plan_can_remove_slice_artifacts() -> None:
    """Low-disk operators can remove slice artifacts after .repo is updated."""
    plan = build_storage_backed_campaign_plan(
        issue_number=240,
        repo_data={"audusd": {"start": "202201", "end": "202201"}},
        symbols=("audusd",),
        data_directory="/Volumes/histdata/data root",
        reports_directory="/Volumes/histdata/reports",
        formats=("ascii",),
        timeframes=("T",),
        current_yearmonth="202201",
        cleanup_mode="working-artifacts",
        platform_executable_bundled=True,
    )

    cleanup = plan["slices"][0]["commands"][2]

    assert plan["cleanup_policy"]["mode"] == "working-artifacts"
    assert plan["cleanup_policy"]["preserves_repo_file"] is True
    assert plan["cleanup_policy"]["preserves_quality_reports"] is True
    assert plan["slices"][0]["commands"][0]["command"] == (
        "histdatacom -D -X -p audusd -f ascii -t tick-data-quotes "
        "--data-directory '/Volumes/histdata/data root'"
    )
    assert cleanup["step"] == "cleanup_after_repo_quality"
    assert cleanup["command"] == (
        "rm -rf '/Volumes/histdata/data root/ASCII/T/audusd'"
    )


def test_storage_backed_campaign_plan_marks_source_checkout_boundary() -> None:
    """Metadata-only source checkouts should not look like campaign failures."""
    plan = build_storage_backed_campaign_plan(
        issue_number=240,
        repo_data={"audusd": {"start": "202201", "end": "202201"}},
        symbols=("audusd",),
        data_directory="data",
        reports_directory="data/.quality/issue-240",
        formats=("ascii",),
        timeframes=("M1",),
        current_yearmonth="202201",
        cleanup_mode="cache",
        platform_executable_bundled=False,
    )

    assert plan["status"] == "needs-platform-wheel"
    assert (
        plan["execution_environment"]["requires_bundled_platform_wheel"] is True
    )
    assert (
        plan["execution_environment"]["source_checkout_sdist_fallback_expected"]
        is True
    )
    assert plan["preflight_commands"][0] == "histdatacom-sidecar doctor --json"

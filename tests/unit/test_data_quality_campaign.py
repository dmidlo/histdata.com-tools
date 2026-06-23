"""Tests for data-quality full-dataset campaign planning."""

from __future__ import annotations

from histdatacom.data_quality.campaign import (
    CAMPAIGN_REPORT_SCHEMA_VERSION,
    build_full_dataset_campaign_report,
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

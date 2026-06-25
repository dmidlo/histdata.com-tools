"""Tests for repository quality metadata helpers."""

from __future__ import annotations

from histdatacom.repository_quality import (
    REPOSITORY_QUALITY_SCHEMA_VERSION,
    repository_data_with_quality_payload,
    repository_quality_columns,
)


def test_repository_quality_payload_updates_legacy_pair_entries() -> None:
    """Legacy start/end rows should gain bounded quality metadata."""
    repo = {
        "eurusd": {"start": "200005", "end": "202606"},
        "hash": "old",
        "hash_utc": 1.0,
    }
    payload = {
        "operation": "data-quality",
        "check_groups": ["inventory", "ingestion"],
        "report_schema_version": "histdatacom.quality-report.v1",
        "report_artifact": {
            "kind": "quality-report",
            "path": "/tmp/quality.json",
            "sha256": "abc",
        },
        "exit_decision": {"exit_code": 0, "reason": "ok"},
        "target_summaries": [
            {
                "target": {
                    "symbol": "EURUSD",
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "period": "201202",
                },
                "finding_count": 1,
                "info_count": 0,
                "warning_count": 1,
                "error_count": 0,
                "status": "warning",
                "max_severity": "warning",
            },
            {
                "target": {
                    "symbol": "EURUSD",
                    "data_format": "ascii",
                    "timeframe": "T",
                    "period": "201202",
                },
                "finding_count": 0,
                "info_count": 0,
                "warning_count": 0,
                "error_count": 0,
                "status": "clean",
                "max_severity": "info",
            },
        ],
    }

    updated = repository_data_with_quality_payload(
        repo,
        payload,
        request_id="request-1",
        checked_at_utc="2026-06-23T00:00:00Z",
    )

    quality = updated["eurusd"]["quality"]
    assert updated["eurusd"]["start"] == "200005"
    assert updated["eurusd"]["end"] == "202606"
    assert "hash" not in updated
    assert quality["schema_version"] == REPOSITORY_QUALITY_SCHEMA_VERSION
    assert quality["checked_at_utc"] == "2026-06-23T00:00:00Z"
    assert quality["status"] == "warning"
    assert quality["max_severity"] == "warning"
    assert quality["target_count"] == 2
    assert quality["clean_target_count"] == 1
    assert quality["warning_target_count"] == 1
    assert quality["finding_count"] == 1
    assert quality["warning_finding_count"] == 1
    assert quality["formats"] == ["ascii"]
    assert quality["timeframes"] == ["M1", "T"]
    assert quality["periods"] == ["201202"]
    assert quality["report_artifact"]["path"] == "quality.json"


def test_repository_quality_attributes_cross_target_findings() -> None:
    """Run-level domain findings should update each involved symbol."""
    repo = {
        "audcad": {"start": "200709", "end": "202606"},
        "audchf": {"start": "200803", "end": "202606"},
        "cadchf": {"start": "200803", "end": "202606"},
        "audjpy": {"start": "200208", "end": "202606"},
    }
    payload = {
        "operation": "data-quality",
        "check_groups": ["all"],
        "report_schema_version": "histdatacom.quality-report.v1",
        "report_artifact": {
            "kind": "quality-report",
            "path": ".quality/issue-241/ascii-m1-all-quality-report.json",
            "sha256": "abc",
        },
        "exit_decision": {"exit_code": 1, "reason": "error threshold"},
        "target_summaries": [
            {
                "target": {
                    "symbol": symbol.upper(),
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "period": "2008",
                },
                "finding_count": 0,
                "info_count": 0,
                "warning_count": 0,
                "error_count": 0,
                "status": "clean",
                "max_severity": "info",
            }
            for symbol in repo
        ],
        "cross_target_summaries": [
            {
                "target": {
                    "symbol": symbol.upper(),
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "period": "2008",
                },
                "finding_count": 1,
                "info_count": 0,
                "warning_count": 0,
                "error_count": 1,
                "status": "failed",
                "max_severity": "error",
            }
            for symbol in ("audcad", "audchf", "cadchf")
        ],
    }

    updated = repository_data_with_quality_payload(
        repo,
        payload,
        request_id="issue-233-ascii-m1-all-quality",
        checked_at_utc="2026-06-23T00:00:00Z",
    )

    for symbol in ("audcad", "audchf", "cadchf"):
        quality = updated[symbol]["quality"]
        assert quality["status"] == "failed"
        assert quality["max_severity"] == "error"
        assert quality["error_count"] == 1
        assert quality["failed_target_count"] == 1
        assert quality["report_artifact"]["path"].endswith(
            "ascii-m1-all-quality-report.json"
        )
    assert updated["audjpy"]["quality"]["status"] == "clean"


def test_repository_quality_columns_are_display_safe() -> None:
    """Table renderers should get stable strings for missing and present data."""
    assert repository_quality_columns({"start": "200005", "end": "202606"}) == {
        "status": "",
        "targets": "",
        "findings": "",
        "report": "",
    }
    assert repository_quality_columns(
        {
            "quality": {
                "status": "clean",
                "target_count": 4,
                "finding_count": 0,
                "report_artifact": {"path": "/tmp/quality.json"},
            }
        }
    ) == {
        "status": "clean",
        "targets": "4",
        "findings": "",
        "report": "quality.json",
    }

"""Tests for data-quality report output and exit policy helpers."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    QUALITY_NEXT_ACTIONS_METADATA_KEY,
    QUALITY_NEXT_ACTIONS_SCHEMA_VERSION,
    QUALITY_REMEDIATION_COVERAGE_METADATA_KEY,
    QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION,
    QUALITY_REPORT_SCHEMA_VERSION,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    QualityExitPolicy,
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    bounded_quality_payload,
    format_quality_console_summary,
    format_quality_remediation_coverage_lines,
    publish_safe_json_value,
    publish_safe_path,
    quality_next_actions_summary,
    quality_remediation_coverage_summary,
    quality_report_payload,
    quality_report_to_json,
    write_quality_report,
)
from histdatacom.runtime_contracts import ArtifactRef


def test_quality_json_report_is_deterministic_and_investigable(
    tmp_path: Path,
) -> None:
    """JSON reports should be stable and include finding context."""
    report = _mixed_report(tmp_path)
    first = quality_report_to_json(report)
    second = quality_report_to_json(report)

    assert first == second

    payload = json.loads(first)
    finding = payload["rule_results"][1]["findings"][0]

    assert payload["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert payload["summary"] == {
        "error_count": 1,
        "finding_count": 2,
        "info_count": 0,
        "max_severity": "error",
        "rule_count": 2,
        "status": "failed",
        "target_count": 3,
        "warning_count": 1,
    }
    assert finding["location"]["path"].endswith("warning.csv")
    assert finding["location"]["row_number"] == 7
    assert finding["location"]["timestamp_source"] == "20120201 000600"
    assert finding["location"]["timestamp_utc_ms"] == 1328072760000
    assert finding["target"]["symbol"] == "EURUSD"
    assert str(tmp_path) not in first


def test_quality_report_payload_is_publish_safe_by_default(
    tmp_path: Path,
) -> None:
    """Public report JSON should not expose local filesystem roots."""
    report = _mixed_report(tmp_path)
    payload = quality_report_payload(report)
    encoded = json.dumps(payload, sort_keys=True)

    assert str(tmp_path) not in encoded
    assert "/Users/" not in encoded
    assert "/home/" not in encoded
    assert payload["targets"][0]["path"] == "clean.csv"
    assert (
        payload["rule_results"][1]["findings"][0]["location"]["path"]
        == "warning.csv"
    )


def test_quality_report_payload_can_preserve_raw_local_paths(
    tmp_path: Path,
) -> None:
    """Local debugging callers can still opt into exact report paths."""
    report = _mixed_report(tmp_path)
    payload = quality_report_payload(report, publish_safe=False)

    assert payload["targets"][0]["path"] == str(tmp_path / "clean.csv")


def test_publish_safe_json_value_sanitizes_nested_path_metadata() -> None:
    """Metadata path fields and embedded local paths should be publishable."""
    payload = {
        "m1_path": (
            "/Users/alice/projects/histdata.com-tools/data/ASCII/M1/"
            "eurusd/2012/DAT_ASCII_EURUSD_M1_2012.csv"
        ),
        "message": (
            "read /Users/alice/projects/histdata.com-tools/"
            "data/ASCII/M1/eurusd/2012/input.csv"
        ),
        "store_root": (
            "/Users/alice/Library/Application Support/histdatacom/"
            "sidecar/workspaces/project/manifests"
        ),
    }

    safe = publish_safe_json_value(payload)

    assert safe["m1_path"] == (
        "data/ASCII/M1/eurusd/2012/DAT_ASCII_EURUSD_M1_2012.csv"
    )
    assert safe["message"] == "read data/ASCII/M1/eurusd/2012/input.csv"
    assert safe["store_root"] == "manifests"
    assert publish_safe_path("/tmp/quality.json") == "quality.json"


def test_quality_report_writer_returns_orchestration_artifact_ref(
    tmp_path: Path,
) -> None:
    """Written reports should have a stable quality-report artifact surface."""
    report = _mixed_report(tmp_path)
    output = tmp_path / "reports" / "quality.json"

    artifact = write_quality_report(report, output)

    assert artifact.kind == "quality-report"
    assert artifact.path == str(output.resolve())
    assert artifact.size_bytes == output.stat().st_size
    assert len(artifact.sha256) == 64
    assert artifact.metadata["schema_version"] == QUALITY_REPORT_SCHEMA_VERSION
    assert artifact.metadata["status"] == "failed"
    assert artifact.metadata["target_count"] == 3
    assert (
        json.loads(output.read_text(encoding="utf-8"))["summary"]["error_count"]
        == 1
    )


def test_bounded_payload_keeps_cross_target_finding_summaries(
    tmp_path: Path,
) -> None:
    """Run-level findings should stay visible without full rule history."""
    report = _cross_target_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("domain",),
        discovery={},
        report=report,
        decision=QualityExitPolicy.from_values(fail_on="never").evaluate(
            report.summary()
        ),
        artifact=None,
    )

    summaries = payload["cross_target_summaries"]

    assert "rule_results" not in payload
    assert isinstance(summaries, list)
    assert {summary["target"]["symbol"] for summary in summaries} == {
        "AUDCAD",
        "AUDCHF",
        "CADCHF",
    }
    assert {summary["target"]["period"] for summary in summaries} == {"2008"}
    assert {summary["status"] for summary in summaries} == {"failed"}
    assert {summary["error_count"] for summary in summaries} == {1}


def test_bounded_payload_sanitizes_discovery_and_artifact_paths(
    tmp_path: Path,
) -> None:
    """Bounded orchestration metadata should be safe to persist in reports."""
    report = _mixed_report(tmp_path)
    artifact = ArtifactRef(
        kind="quality-report",
        path=str(tmp_path / "reports" / "quality.json"),
    )
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("inventory",),
        discovery={
            "roots": [str(tmp_path / "data" / "ASCII")],
            "metadata": {
                "store_path": (
                    "/Users/alice/Library/Application Support/histdatacom/"
                    "sidecar/workspaces/project/manifests/.histdatacom/"
                    "manifest-status.sqlite3"
                )
            },
        },
        report=report,
        decision=QualityExitPolicy.from_values(fail_on="never").evaluate(
            report.summary()
        ),
        artifact=artifact,
    )
    encoded = json.dumps(payload, sort_keys=True)

    assert str(tmp_path) not in encoded
    assert "/Users/" not in encoded
    assert payload["discovery"]["roots"] == ["data/ASCII"]
    assert payload["discovery"]["metadata"]["store_path"] == (
        ".histdatacom/manifest-status.sqlite3"
    )
    assert payload["report_artifact"]["path"] == "reports/quality.json"


def test_bounded_payload_caps_cache_scale_target_lists(
    tmp_path: Path,
) -> None:
    """Large quality runs should return counts plus bounded target samples."""
    report = _many_target_report(tmp_path, clean_count=5)
    discovery_targets = [
        _target(tmp_path / f"discovered-{index}.csv").to_dict()
        for index in range(6)
    ]
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("inventory",),
        discovery={
            "roots": [str(tmp_path / "data")],
            "target_count": len(discovery_targets),
            "targets": discovery_targets,
        },
        report=report,
        decision=QualityExitPolicy.from_values(fail_on="never").evaluate(
            report.summary()
        ),
        artifact=None,
        discovery_target_limit=2,
        target_summary_limit=3,
        cross_target_summary_limit=1,
    )

    target_summaries = payload["target_summaries"]
    limits = payload["payload_limits"]

    assert payload["summary"]["target_count"] == 7
    assert payload["target_status_counts"] == {
        "clean": 5,
        "warning": 1,
        "failed": 1,
    }
    assert [summary["status"] for summary in target_summaries] == [
        "failed",
        "warning",
        "clean",
    ]
    assert payload["discovery"]["target_count"] == 6
    assert len(payload["discovery"]["targets"]) == 2
    assert payload["discovery"]["target_omitted_count"] == 4
    assert limits["target_summaries"]["omitted_count"] == 4
    assert limits["target_summaries"]["truncated"] is True


def test_quality_console_summary_separates_target_statuses(
    tmp_path: Path,
) -> None:
    """Human output should group clean, warning, and failed files."""
    output = format_quality_console_summary(
        _mixed_report(tmp_path),
        check_groups=("inventory", "time"),
    )

    assert "Data quality assessment" in output
    assert "checks: inventory, time" in output
    assert "status: failed" in output
    assert "targets: 3 clean: 1 warning: 1 failed: 1" in output
    assert "Clean files\n- csv:" in output
    assert "Warning files\n- csv:" in output
    assert "Failed files\n- csv:" in output


def test_quality_report_payload_adds_fingerprint_coverage_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize run coverage metadata."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY]

    assert summary["discovered_target_count"] == 2
    assert summary["evaluated_fingerprint_target_count"] == 2
    assert summary["fingerprint_target_count"] == 2
    assert summary["skipped_fingerprint_target_count"] == 0
    assert summary["supported_readable_count"] == 1
    assert summary["unavailable_count"] == 1
    assert summary["parsed_non_empty_coverage_count"] == 1
    assert summary["skipped_reason_counts"] == {}
    assert summary["source_kind_counts"] == {"cache": 1, "unavailable": 1}
    assert summary["cache_source_counts"] == {"direct": 1}
    assert summary["unavailable_reason_counts"] == {
        "unsupported_target_kind": 1,
    }


def test_quality_report_payload_adds_fingerprint_topology_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize human-readable topology metadata."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY
    ]
    target_summaries = summary["target_summaries"]

    assert summary["target_count"] == 2
    assert summary["included_target_count"] == 2
    assert summary["omitted_target_count"] == 0
    assert summary["status_counts"] == {
        "regular": 1,
        "unavailable": 1,
    }
    assert summary["computed_from_counts"] == {
        "direct_cache": 1,
        "unavailable": 1,
    }
    assert summary["cache_source_counts"] == {"direct": 1}
    assert summary["flag_counts"] == {
        "cache_backed": 1,
        "unavailable_topology": 1,
    }
    assert target_summaries[0]["target_axis"] == {
        "data_format": "ascii",
        "timeframe": "M1",
        "symbol": "EURUSD",
        "period": "201202",
        "kind": "cache",
    }
    assert target_summaries[0]["row_count"] == 3
    assert target_summaries[0]["parsed_row_count"] == 3
    assert target_summaries[0]["duplicate_timestamp_count"] == 0
    assert target_summaries[0]["non_monotonic_count"] == 0
    assert target_summaries[0]["median_interval_ms"] == 60_000
    assert target_summaries[0]["max_gap_ms"] == 60_000
    assert target_summaries[0]["suspicious_gap_count"] == 0
    assert target_summaries[0]["expected_session_closure_count"] == 0
    assert target_summaries[0]["weekend_activity_count"] == 0
    assert target_summaries[0]["sampling_basis"] == "observed_sequence"
    assert target_summaries[0]["computed_from"] == "direct_cache"
    assert target_summaries[0]["cache_source"] == "direct"


def test_quality_report_payload_adds_fingerprint_topology_attention_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize attention-first topology metadata."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY
    ]
    target_summaries = summary["target_summaries"]

    assert summary["topology_target_count"] == 2
    assert summary["attention_target_count"] == 1
    assert summary["included_attention_target_count"] == 1
    assert summary["omitted_attention_target_count"] == 0
    assert summary["truncated"] is False
    assert summary["attention_level_counts"] == {"unavailable": 1}
    assert summary["attention_flag_counts"] == {
        "unavailable_topology": 1,
    }
    assert target_summaries[0]["target_axis"] == {
        "data_format": "ascii",
        "timeframe": "M1",
        "symbol": "EURUSD",
        "period": "201202",
        "kind": "spreadsheet",
    }
    assert target_summaries[0]["attention_level"] == "unavailable"
    assert target_summaries[0]["attention_flags"] == [
        "unavailable_topology",
    ]
    assert target_summaries[0]["remediation_hints"] == [
        {
            "code": "verify_fingerprint_source",
            "message": "rebuild or choose a readable fingerprint source",
            "action_kind": "rebuild",
            "rule_id": SERIES_FINGERPRINT_RULE_ID,
            "flag": "unavailable_topology",
        }
    ]
    assert target_summaries[0]["status"] == "unavailable"
    assert target_summaries[0]["computed_from"] == "unavailable"


def test_quality_report_payload_adds_fingerprint_distribution_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize distribution summaries."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY
    ]
    attention = payload["metadata"][
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY
    ]

    assert summary["target_count"] == 2
    assert summary["distribution_target_count"] == 1
    assert summary["m1_bar_distribution_target_count"] == 1
    assert summary["tick_distribution_target_count"] == 0
    assert summary["missing_distribution_target_count"] == 0
    assert summary["unavailable_distribution_target_count"] == 1
    assert summary["cache_backed_distribution_target_count"] == 1
    assert summary["text_backed_distribution_target_count"] == 0
    assert summary["distribution_kind_counts"] == {"m1_bar": 1, "missing": 1}
    assert summary["precision_source_counts"] == {
        "cache_float": 1,
        "unavailable": 1,
    }
    assert summary["target_summaries"][0]["precision_source"] == "cache_float"
    assert attention["attention_target_count"] == 1
    assert attention["attention_flag_counts"] == {
        "cache_float_precision_basis": 1,
    }
    assert attention["target_summaries"][0]["attention_level"] == "precision"


def test_quality_report_payload_adds_mixed_next_actions(
    tmp_path: Path,
) -> None:
    """Report metadata should aggregate topology and finding remediation."""
    report = _next_action_report(tmp_path)

    payload = quality_report_payload(report)
    next_actions = payload["metadata"][QUALITY_NEXT_ACTIONS_METADATA_KEY]
    encoded = json.dumps(next_actions, sort_keys=True)

    assert str(tmp_path) not in encoded
    assert next_actions["schema_version"] == QUALITY_NEXT_ACTIONS_SCHEMA_VERSION
    assert next_actions["action_count"] == 2
    assert next_actions["included_action_count"] == 2
    assert next_actions["omitted_action_count"] == 0
    assert next_actions["source_counts"] == {
        "fingerprint_topology_attention": 1,
        "quality_finding": 1,
    }
    actions = next_actions["actions"]
    assert [action["code"] for action in actions] == [
        "verify_fingerprint_source",
        "inspect_duplicate_timestamp_rows",
    ]
    assert actions[0]["urgency"] == "high"
    assert actions[0]["max_attention_level"] == "unavailable"
    assert actions[0]["flag_counts"] == {"unavailable_topology": 1}
    assert actions[1]["urgency"] == "medium"
    assert actions[1]["max_severity"] == "warning"
    assert actions[1]["severity_counts"] == {"warning": 1}
    assert actions[1]["finding_code_counts"] == {
        "ASCII_M1_DUPLICATE_TIMESTAMP": 1,
    }
    assert actions[1]["target_axis_counts"] == [
        {
            "target_axis": {
                "data_format": "ascii",
                "timeframe": "M1",
                "symbol": "EURUSD",
                "period": "201202",
                "kind": "csv",
            },
            "count": 1,
        }
    ]


def test_quality_next_actions_are_bounded_and_stably_ordered(
    tmp_path: Path,
) -> None:
    """Next-action output should truncate actions and target axes explicitly."""
    action_summary = quality_next_actions_summary(
        _next_action_report(tmp_path),
        action_limit=1,
    )

    assert action_summary is not None
    assert action_summary["action_count"] == 2
    assert action_summary["included_action_count"] == 1
    assert action_summary["omitted_action_count"] == 1
    assert action_summary["truncated"] is True
    assert action_summary["actions"][0]["code"] == "verify_fingerprint_source"

    axis_summary = quality_next_actions_summary(
        _many_duplicate_next_action_report(tmp_path),
        action_limit=1,
        target_axis_limit=1,
    )

    assert axis_summary is not None
    assert axis_summary["action_count"] == 1
    assert axis_summary["included_action_count"] == 1
    assert axis_summary["omitted_action_count"] == 0
    action = axis_summary["actions"][0]
    assert action["code"] == "inspect_duplicate_timestamp_rows"
    assert action["occurrence_count"] == 3
    assert action["affected_target_count"] == 2
    assert action["target_axis_count"] == 2
    assert action["included_target_axis_count"] == 1
    assert action["omitted_target_axis_count"] == 1
    assert action["target_axis_truncated"] is True
    assert action["target_axis_counts"][0]["target_axis"]["symbol"] == "EURUSD"
    assert action["target_axis_counts"][0]["count"] == 2


def test_quality_report_payload_adds_remediation_coverage_metadata(
    tmp_path: Path,
) -> None:
    """Report metadata should expose remediation catalog coverage gaps."""
    report = _remediation_coverage_report(tmp_path)

    payload = quality_report_payload(report)
    coverage = payload["metadata"][QUALITY_REMEDIATION_COVERAGE_METADATA_KEY]
    encoded = json.dumps(coverage, sort_keys=True)

    assert str(tmp_path) not in encoded
    assert (
        coverage["schema_version"]
        == QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION
    )
    assert coverage["finding_count"] == 5
    assert coverage["mapped_finding_count"] == 1
    assert coverage["unmapped_finding_count"] == 4
    assert coverage["unmapped_warning_error_finding_count"] == 3
    assert coverage["severity_counts"] == {
        "error": 2,
        "info": 1,
        "warning": 2,
    }
    assert coverage["mapped_severity_counts"] == {"warning": 1}
    assert coverage["unmapped_severity_counts"] == {
        "error": 2,
        "info": 1,
        "warning": 1,
    }
    assert coverage["unmapped_warning_error_group_count"] == 2
    assert coverage["included_unmapped_warning_error_group_count"] == 2
    assert coverage["omitted_unmapped_warning_error_group_count"] == 0

    groups = coverage["unmapped_groups"]
    assert [group["max_severity"] for group in groups] == [
        "error",
        "warning",
        "info",
    ]
    assert groups[0]["rule_id"] == "file.exists"
    assert groups[0]["finding_code"] == "FILE_MISSING"
    assert groups[0]["occurrence_count"] == 2
    assert groups[0]["target_axis_count"] == 2


def test_quality_report_payload_adds_all_mapped_remediation_coverage(
    tmp_path: Path,
) -> None:
    """All-mapped runs should still report catalog coverage counts."""
    report = _many_duplicate_next_action_report(tmp_path)

    payload = quality_report_payload(report)
    coverage = payload["metadata"][QUALITY_REMEDIATION_COVERAGE_METADATA_KEY]

    assert coverage["finding_count"] == 3
    assert coverage["mapped_finding_count"] == 3
    assert coverage["unmapped_finding_count"] == 0
    assert coverage["mapped_finding_code_counts"] == [
        {"finding_code": "ASCII_M1_DUPLICATE_TIMESTAMP", "count": 3},
    ]
    assert coverage["unmapped_groups"] == []

    output = format_quality_console_summary(
        report,
        check_groups=("time",),
    )
    assert "Remediation coverage" not in output
    assert format_quality_remediation_coverage_lines(coverage) == []


def test_quality_report_surfaces_inventory_zip_remediation_actions(
    tmp_path: Path,
) -> None:
    """ZIP inventory hints should feed next actions and mapped coverage."""
    report = _inventory_archive_remediation_report(tmp_path)

    actions = quality_next_actions_summary(report)
    assert actions is not None
    assert actions["action_count"] == 2
    assert actions["source_counts"] == {"quality_finding": 3}
    assert actions["actions"][0]["code"] == "redownload_corrupt_zip_archive"
    assert actions["actions"][0]["occurrence_count"] == 1
    assert actions["actions"][0]["finding_code_counts"] == {
        "ZIP_CORRUPT": 1,
    }
    assert actions["actions"][1]["code"] == "rename_histdata_zip_archive"
    assert actions["actions"][1]["occurrence_count"] == 2
    assert actions["actions"][1]["finding_code_counts"] == {
        "HISTDATA_ZIP_FILENAME_INVALID": 2,
    }

    coverage = quality_remediation_coverage_summary(report)
    assert coverage is not None
    assert coverage["finding_count"] == 3
    assert coverage["mapped_finding_count"] == 3
    assert coverage["unmapped_finding_count"] == 0
    assert coverage["unmapped_warning_error_group_count"] == 0
    assert coverage["mapped_finding_code_counts"] == [
        {"finding_code": "HISTDATA_ZIP_FILENAME_INVALID", "count": 2},
        {"finding_code": "ZIP_CORRUPT", "count": 1},
    ]

    payload = quality_report_payload(report)
    encoded = json.dumps(payload, sort_keys=True)
    assert str(tmp_path) not in encoded
    assert (
        payload["metadata"][QUALITY_NEXT_ACTIONS_METADATA_KEY]["action_count"]
        == 2
    )

    output = format_quality_console_summary(
        report,
        check_groups=("inventory",),
    )
    assert "Next actions" in output
    assert (
        "redownload or replace the corrupt ZIP archive "
        "(redownload_corrupt_zip_archive, rule=inventory.zip.integrity"
    ) in output
    assert "Remediation coverage" not in output


def test_quality_remediation_coverage_is_bounded_and_stably_ordered(
    tmp_path: Path,
) -> None:
    """Coverage output should truncate code/rule and target-axis lists."""
    coverage = quality_remediation_coverage_summary(
        _remediation_coverage_report(tmp_path),
        group_limit=1,
        target_axis_limit=1,
    )

    assert coverage is not None
    assert coverage["unmapped_group_count"] == 3
    assert coverage["included_unmapped_group_count"] == 1
    assert coverage["omitted_unmapped_group_count"] == 2
    assert coverage["unmapped_truncated"] is True
    assert coverage["count_limits"]["rule_id_counts"] == {
        "limit": 1,
        "total_count": 4,
        "included_count": 1,
        "omitted_count": 3,
        "truncated": True,
    }
    assert coverage["count_limits"]["finding_code_counts"] == {
        "limit": 1,
        "total_count": 4,
        "included_count": 1,
        "omitted_count": 3,
        "truncated": True,
    }

    groups = coverage["unmapped_groups"]
    assert len(groups) == 1
    group = groups[0]
    assert group["max_severity"] == "error"
    assert group["target_axis_count"] == 2
    assert group["included_target_axis_count"] == 1
    assert group["omitted_target_axis_count"] == 1
    assert group["target_axis_truncated"] is True
    assert group["target_axis_counts"][0]["count"] == 1


def test_quality_console_summary_renders_remediation_coverage_gaps(
    tmp_path: Path,
) -> None:
    """Human quality output should highlight unmapped warning/error findings."""
    output = format_quality_console_summary(
        _remediation_coverage_report(tmp_path),
        check_groups=("time",),
    )

    assert "Remediation coverage" in output
    assert "- findings: 5 mapped: 1 unmapped: 4" in output
    assert "- unmapped warning/error groups: 2 included: 2 omitted: 0" in output
    assert "- error file.exists:FILE_MISSING findings=2 targets=2" in output
    assert (
        "- warning ticks.spread:NEGATIVE_SPREAD findings=1 targets=1" in output
    )
    assert "ingestion.ascii.schema:ASCII_SCHEMA_SUMMARY" not in output


def test_fingerprint_console_summary_reports_coverage_counts(
    tmp_path: Path,
) -> None:
    """Human output should summarize fingerprint coverage for operators."""
    output = format_quality_console_summary(
        _fingerprint_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint coverage" in output
    assert (
        "- targets: 2 supported/readable: 1 unavailable: 1 parsed/non-empty: 1"
    ) in output
    assert "- source kinds: cache=1, unavailable=1" in output
    assert "- cache sources: direct=1" in output
    assert "- unavailable reasons: unsupported_target_kind=1" in output
    assert "- target kinds: cache=1, spreadsheet=1" in output
    assert "- timeframes: M1=2" in output


def test_fingerprint_console_summary_reports_topology_lines(
    tmp_path: Path,
) -> None:
    """Human output should summarize per-target topology for operators."""
    output = format_quality_console_summary(
        _fingerprint_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint topology" in output
    assert "Fingerprint topology attention" in output
    assert "- targets needing attention: 1 included: 1 omitted: 0" in output
    assert (
        "- ascii EURUSD M1 201202 spreadsheet: unavailable, "
        "unavailable_topology, invalid=0, duplicates=0, non-monotonic=0, "
        "suspicious gaps=0, weekend activity=0, max gap unavailable, "
        "computed_from=unavailable, "
        "next=rebuild or choose a readable fingerprint source"
    ) in output
    assert (
        "- targets: 2 included: 2 regular: 1 irregular: 0 unavailable: 1"
    ) in output
    assert (
        "- ascii EURUSD M1 201202 cache: regular, observed_sequence, "
        "3 rows, 3 parsed, no duplicates, non-monotonic=0, "
        "median interval 60s, max gap 60s, 0 expected closures, "
        "0 suspicious gaps, weekend activity=0, "
        "computed_from=direct_cache, cache=direct"
    ) in output
    assert (
        "- ascii EURUSD M1 201202 spreadsheet: unavailable, unavailable, "
        "0 rows, unknown parsed, no duplicates, non-monotonic=0, "
        "median interval unavailable, max gap unavailable, "
        "0 expected closures, 0 suspicious gaps, weekend activity=0, "
        "computed_from=unavailable"
    ) in output
    assert "cache=unknown" not in output


def test_fingerprint_console_summary_reports_distribution_lines(
    tmp_path: Path,
) -> None:
    """Human output should summarize fingerprint distributions."""
    output = format_quality_console_summary(
        _fingerprint_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint distribution attention" in output
    assert "Fingerprint distributions" in output
    assert "- targets needing attention: 1 included: 1 omitted: 0" in output
    assert (
        "- thresholds: invalid rows >= 1 and rate >= 0; "
        "zero spreads >= 1 and rate >= 0; "
        "negative spreads >= 1 and rate >= 0; truncated=true; "
        "cache_float_precision=true"
    ) in output
    assert (
        "- ascii EURUSD M1 201202 cache: precision, m1_bar, "
        "cache_float_precision_basis, rows=3, usable=3, invalid=0, "
        "sampled=3, zero spread=unavailable, negative spread=unavailable, "
        "precision=cache_float, source=cache, cache=direct"
    ) in output
    assert (
        "- targets: 2 with distributions: 1 m1: 1 tick: 0 missing: 0" in output
    )
    assert "- sources: cache=1, unavailable=1" in output
    assert "- precision sources: cache_float=1, unavailable=1" in output
    assert (
        "- ascii EURUSD M1 201202 cache: available, m1_bar, 3 rows, "
        "3 usable, 0 invalid, 3 sampled, truncated=false, "
        "precision=cache_float, source=cache, cache=direct"
    ) in output


def test_quality_console_summary_renders_next_actions(
    tmp_path: Path,
) -> None:
    """Human quality output should list run-level next actions."""
    output = format_quality_console_summary(
        _next_action_report(tmp_path),
        check_groups=("fingerprint", "time"),
    )

    assert "Next actions" in output
    assert "- actions: 2 included: 2 omitted: 0" in output
    assert (
        "- high rebuild: rebuild or choose a readable fingerprint source "
        "(verify_fingerprint_source, rule=fingerprint.series, targets=1, "
        "attention=unavailable)"
    ) in output
    assert (
        "- medium inspect: inspect duplicate timestamp rows "
        "(inspect_duplicate_timestamp_rows, rule=time.ascii.sequence, "
        "targets=1, severity=warning)"
    ) in output


def test_fingerprint_console_summary_reports_skipped_targets(
    tmp_path: Path,
) -> None:
    """Human output should expose skipped fingerprint targets when present."""
    base_report = _fingerprint_report(tmp_path)
    payload = quality_report_payload(base_report)
    metadata = payload["metadata"]
    assert isinstance(metadata, dict)
    coverage = metadata[TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY]
    assert isinstance(coverage, dict)
    coverage = dict(coverage)
    coverage["discovered_target_count"] = 3
    coverage["skipped_fingerprint_target_count"] = 1
    coverage["skipped_reason_counts"] = {
        "duplicate_archive_preferred_csv": 1,
    }
    report = QualityReport(
        targets=base_report.targets,
        rule_results=base_report.rule_results,
        metadata={TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY: coverage},
    )

    output = format_quality_console_summary(
        report,
        check_groups=("fingerprint",),
    )

    assert "- skipped: 1" in output
    assert ("- skipped reasons: duplicate_archive_preferred_csv=1") in output


def test_bounded_quality_payload_includes_fingerprint_coverage(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose fingerprint coverage."""
    report = _fingerprint_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert payload["fingerprint_coverage"]["source_kind_counts"] == {
        "cache": 1,
        "unavailable": 1,
    }


def test_bounded_quality_payload_includes_fingerprint_distribution(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose distributions."""
    report = _fingerprint_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert payload["fingerprint_distribution"]["distribution_kind_counts"] == {
        "m1_bar": 1,
        "missing": 1,
    }
    assert payload["fingerprint_distribution_attention"][
        "attention_flag_counts"
    ] == {"cache_float_precision_basis": 1}
    assert (
        payload["fingerprint_distribution_attention"]["attention_thresholds"][
            "flag_cache_float_precision"
        ]
        is True
    )


def test_bounded_quality_payload_includes_fingerprint_topology(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose topology summaries."""
    report = _fingerprint_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert payload["fingerprint_topology"]["status_counts"] == {
        "regular": 1,
        "unavailable": 1,
    }
    assert (
        payload["fingerprint_topology"]["target_summaries"][0]["computed_from"]
        == "direct_cache"
    )


def test_bounded_quality_payload_includes_fingerprint_topology_attention(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose topology attention."""
    report = _fingerprint_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert payload["fingerprint_topology_attention"][
        "attention_flag_counts"
    ] == {"unavailable_topology": 1}
    assert (
        payload["fingerprint_topology_attention"]["target_summaries"][0][
            "attention_level"
        ]
        == "unavailable"
    )
    assert payload["fingerprint_topology_attention"]["target_summaries"][0][
        "remediation_hints"
    ] == [
        {
            "code": "verify_fingerprint_source",
            "message": "rebuild or choose a readable fingerprint source",
            "action_kind": "rebuild",
            "rule_id": SERIES_FINGERPRINT_RULE_ID,
            "flag": "unavailable_topology",
        }
    ]


def test_bounded_quality_payload_includes_next_actions(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose next-action metadata."""
    report = _next_action_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint", "time"),
        discovery={"roots": [str(tmp_path)], "target_count": 3},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    assert "rule_results" not in payload
    assert payload["next_actions"]["action_count"] == 2
    assert payload["payload_limits"]["next_actions"] == {
        "limit": 16,
        "total_count": 2,
        "included_count": 2,
        "omitted_count": 0,
        "truncated": False,
    }

    prebounded_actions = quality_next_actions_summary(report, action_limit=1)
    assert prebounded_actions is not None
    metadata_report = QualityReport(
        targets=report.targets,
        rule_results=report.rule_results,
        metadata={QUALITY_NEXT_ACTIONS_METADATA_KEY: prebounded_actions},
    )

    prebounded_payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint", "time"),
        discovery={"roots": [str(tmp_path)], "target_count": 3},
        report=metadata_report,
        decision=QualityExitPolicy.from_values().evaluate(
            metadata_report.summary()
        ),
        artifact=None,
    )

    assert len(prebounded_payload["next_actions"]["actions"]) == 1
    assert prebounded_payload["payload_limits"]["next_actions"] == {
        "limit": 16,
        "total_count": 2,
        "included_count": 1,
        "omitted_count": 1,
        "truncated": True,
    }


def test_bounded_quality_payload_includes_remediation_coverage(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose coverage gap metadata."""
    report = _remediation_coverage_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("time",),
        discovery={"roots": [str(tmp_path)], "target_count": 5},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )
    encoded = json.dumps(payload["remediation_coverage"], sort_keys=True)

    assert str(tmp_path) not in encoded
    assert "rule_results" not in payload
    assert payload["remediation_coverage"]["finding_count"] == 5
    assert payload["remediation_coverage"]["unmapped_finding_count"] == 4
    assert payload["payload_limits"]["remediation_coverage"] == {
        "limit": 16,
        "target_axis_limit": 8,
        "total_count": 3,
        "included_count": 3,
        "omitted_count": 0,
        "truncated": False,
    }


def test_quality_exit_policy_applies_error_warning_and_never_modes(
    tmp_path: Path,
) -> None:
    """Exit decisions should be derived from configured thresholds."""
    summary = _mixed_report(tmp_path).summary()

    assert QualityExitPolicy.from_values().evaluate(summary).exit_code == 1
    assert (
        QualityExitPolicy.from_values(max_errors=1).evaluate(summary).exit_code
        == 0
    )
    assert (
        QualityExitPolicy.from_values(
            fail_on="warning",
            max_errors=1,
            max_warnings=0,
        )
        .evaluate(summary)
        .reason
        == "quality warning threshold exceeded: 1 > 0"
    )
    assert (
        QualityExitPolicy.from_values(fail_on="never")
        .evaluate(summary)
        .exit_code
        == 0
    )


def _mixed_report(tmp_path: Path) -> QualityReport:
    clean = _target(tmp_path / "clean.csv")
    warning = _target(tmp_path / "warning.csv")
    failed = _target(tmp_path / "failed.csv")
    warning_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="M1_DUPLICATE_TIMESTAMP",
        message="duplicate minute bar timestamp",
        rule_id="m1.timestamp.unique",
        target=warning,
        location=QualityLocation(
            path=warning.path,
            row_number=7,
            timestamp_source="20120201 000600",
            timestamp_utc_ms=1328072760000,
            column="datetime",
        ),
    )
    error_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="FILE_MISSING",
        message="expected local file is missing",
        rule_id="file.exists",
        target=failed,
        location=QualityLocation(path=failed.path),
    )
    return QualityReport(
        targets=(clean, warning, failed),
        rule_results=(
            QualityRuleResult(rule_id="file.exists", target=clean),
            QualityRuleResult(
                rule_id="m1.timestamp.unique",
                target=warning,
                findings=(warning_finding,),
            ),
            QualityRuleResult(
                rule_id="file.exists",
                target=failed,
                findings=(error_finding,),
            ),
        ),
    )


def _fingerprint_report(tmp_path: Path) -> QualityReport:
    cache = QualityTarget(
        path=str(tmp_path / ".data"),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    unavailable = QualityTarget(
        path=str(tmp_path / "unsupported.xlsx"),
        kind=QualityTargetKind.SPREADSHEET,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    cache_finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=cache,
        metadata={
            "time_series_fingerprint": {
                "target_axis": {
                    "kind": "cache",
                    "timeframe": "M1",
                },
                "coverage": {
                    "row_count": 3,
                    "parsed_row_count": 3,
                },
                "m1_bar_distribution": {
                    "row_count": 3,
                    "sampled_row_count": 3,
                    "usable_row_count": 3,
                    "invalid_row_count": 0,
                    "truncated": False,
                    "precision": {
                        "precision_source": "cache_float",
                        "decimal_place_counts": {"6": 12},
                    },
                },
                "temporal_topology": {
                    "row_count": 3,
                    "parsed_row_count": 3,
                    "invalid_timestamp_count": 0,
                    "duplicate_timestamp_count": 0,
                    "non_monotonic_count": 0,
                    "median_interval_ms": 60_000,
                    "max_gap_ms": 60_000,
                    "suspicious_gap_count": 0,
                    "expected_session_closure_count": 0,
                    "weekend_activity_count": 0,
                    "sampling_basis": "observed_sequence",
                    "computed_from": "direct_cache",
                    "cache_source": "direct",
                },
                "source": {
                    "kind": "cache",
                    "cache_source": "direct",
                },
            }
        },
    )
    unavailable_finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SOURCE_UNAVAILABLE",
        message="Target source is unavailable for canonical fingerprinting.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=unavailable,
        metadata={
            "time_series_fingerprint": {
                "target_axis": {
                    "kind": "spreadsheet",
                    "timeframe": "M1",
                },
                "coverage": {
                    "row_count": 0,
                    "parsed_row_count": None,
                },
                "temporal_topology": {
                    "row_count": 0,
                    "parsed_row_count": None,
                    "invalid_timestamp_count": 0,
                    "duplicate_timestamp_count": 0,
                    "non_monotonic_count": 0,
                    "median_interval_ms": None,
                    "max_gap_ms": None,
                    "suspicious_gap_count": 0,
                    "expected_session_closure_count": 0,
                    "weekend_activity_count": 0,
                    "sampling_basis": "unavailable",
                    "computed_from": "unavailable",
                    "cache_source": None,
                },
                "source": {
                    "kind": "unavailable",
                    "reason": "unsupported_target_kind",
                },
            }
        },
    )
    return QualityReport(
        targets=(cache, unavailable),
        rule_results=(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=cache,
                findings=(cache_finding,),
            ),
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=unavailable,
                findings=(unavailable_finding,),
            ),
        ),
    )


def _next_action_report(tmp_path: Path) -> QualityReport:
    base_report = _fingerprint_report(tmp_path)
    duplicate = _target(tmp_path / "duplicate.csv")
    duplicate_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="ASCII_M1_DUPLICATE_TIMESTAMP",
        message="M1 file contains duplicate normalized timestamps.",
        rule_id="time.ascii.sequence",
        target=duplicate,
        location=QualityLocation(path=duplicate.path),
    )
    return QualityReport(
        targets=(*base_report.targets, duplicate),
        rule_results=(
            *base_report.rule_results,
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=duplicate,
                findings=(duplicate_finding,),
            ),
        ),
    )


def _many_duplicate_next_action_report(tmp_path: Path) -> QualityReport:
    first = _target(tmp_path / "first.csv")
    second = QualityTarget(
        path=str(tmp_path / "second.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="GBPUSD",
        period="201202",
    )

    def finding(target: QualityTarget) -> QualityFinding:
        return QualityFinding(
            severity=QualitySeverity.WARNING,
            code="ASCII_M1_DUPLICATE_TIMESTAMP",
            message="M1 file contains duplicate normalized timestamps.",
            rule_id="time.ascii.sequence",
            target=target,
            location=QualityLocation(path=target.path),
        )

    return QualityReport(
        targets=(first, second),
        rule_results=(
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=first,
                findings=(finding(first), finding(first)),
            ),
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=second,
                findings=(finding(second),),
            ),
        ),
    )


def _inventory_archive_remediation_report(tmp_path: Path) -> QualityReport:
    first = QualityTarget(
        path=str(tmp_path / "EURUSD_201202.zip"),
        kind=QualityTargetKind.ZIP,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    second = QualityTarget(
        path=str(tmp_path / "BROKEN.zip"),
        kind=QualityTargetKind.ZIP,
        data_format="ascii",
        timeframe="T",
        symbol="GBPUSD",
        period="201202",
    )
    filename_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="HISTDATA_ZIP_FILENAME_INVALID",
        message="ZIP filename does not match expected HistData metadata.",
        rule_id="inventory.zip.integrity",
        target=first,
        location=QualityLocation(path=first.path),
    )
    corrupt_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="ZIP_CORRUPT",
        message="ZIP archive could not be opened.",
        rule_id="inventory.zip.integrity",
        target=second,
        location=QualityLocation(path=second.path),
    )
    return QualityReport(
        targets=(first, second),
        rule_results=(
            QualityRuleResult(
                rule_id="inventory.zip.integrity",
                target=first,
                findings=(filename_finding, filename_finding),
            ),
            QualityRuleResult(
                rule_id="inventory.zip.integrity",
                target=second,
                findings=(corrupt_finding,),
            ),
        ),
    )


def _remediation_coverage_report(tmp_path: Path) -> QualityReport:
    mapped = _target(tmp_path / "mapped.csv")
    missing_eur = _target(tmp_path / "missing-eur.csv")
    missing_gbp = QualityTarget(
        path=str(tmp_path / "missing-gbp.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="GBPUSD",
        period="201202",
    )
    negative_spread = _target(tmp_path / "negative-spread.csv")
    schema_summary = _target(tmp_path / "schema-summary.csv")
    mapped_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="ASCII_M1_DUPLICATE_TIMESTAMP",
        message="M1 file contains duplicate normalized timestamps.",
        rule_id="time.ascii.sequence",
        target=mapped,
        location=QualityLocation(path=mapped.path),
    )
    missing_eur_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="FILE_MISSING",
        message="expected local file is missing",
        rule_id="file.exists",
        target=missing_eur,
        location=QualityLocation(path=missing_eur.path),
    )
    missing_gbp_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="FILE_MISSING",
        message="expected local file is missing",
        rule_id="file.exists",
        target=missing_gbp,
        location=QualityLocation(path=missing_gbp.path),
    )
    negative_spread_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="NEGATIVE_SPREAD",
        message="tick ask is below bid",
        rule_id="ticks.spread",
        target=negative_spread,
        location=QualityLocation(path=negative_spread.path),
    )
    schema_summary_finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="ASCII_SCHEMA_SUMMARY",
        message="ASCII M1 schema profile.",
        rule_id="ingestion.ascii.schema",
        target=schema_summary,
        location=QualityLocation(path=schema_summary.path),
    )
    return QualityReport(
        targets=(
            mapped,
            missing_eur,
            missing_gbp,
            negative_spread,
            schema_summary,
        ),
        rule_results=(
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=mapped,
                findings=(mapped_finding,),
            ),
            QualityRuleResult(
                rule_id="file.exists",
                target=missing_eur,
                findings=(missing_eur_finding,),
            ),
            QualityRuleResult(
                rule_id="file.exists",
                target=missing_gbp,
                findings=(missing_gbp_finding,),
            ),
            QualityRuleResult(
                rule_id="ticks.spread",
                target=negative_spread,
                findings=(negative_spread_finding,),
            ),
            QualityRuleResult(
                rule_id="ingestion.ascii.schema",
                target=schema_summary,
                findings=(schema_summary_finding,),
            ),
        ),
    )


def _many_target_report(tmp_path: Path, *, clean_count: int) -> QualityReport:
    clean_targets = tuple(
        _target(tmp_path / f"clean-{index}.csv") for index in range(clean_count)
    )
    warning = _target(tmp_path / "warning.csv")
    failed = _target(tmp_path / "failed.csv")
    warning_finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="M1_DUPLICATE_TIMESTAMP",
        message="duplicate minute bar timestamp",
        rule_id="m1.timestamp.unique",
        target=warning,
        location=QualityLocation(path=warning.path),
    )
    error_finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="FILE_MISSING",
        message="expected local file is missing",
        rule_id="file.exists",
        target=failed,
        location=QualityLocation(path=failed.path),
    )
    return QualityReport(
        targets=(*clean_targets, warning, failed),
        rule_results=(
            *(
                QualityRuleResult(rule_id="file.exists", target=target)
                for target in clean_targets
            ),
            QualityRuleResult(
                rule_id="m1.timestamp.unique",
                target=warning,
                findings=(warning_finding,),
            ),
            QualityRuleResult(
                rule_id="file.exists",
                target=failed,
                findings=(error_finding,),
            ),
        ),
    )


def _cross_target_report(tmp_path: Path) -> QualityReport:
    directory = QualityTarget(
        path=str(tmp_path / "data" / "ASCII" / "M1"),
        kind=QualityTargetKind.DIRECTORY,
        data_format="ascii",
    )
    finding = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR",
        message="triangular relationship differs from the direct pair",
        rule_id="domain.cross_instrument_consistency",
        target=directory,
        location=QualityLocation(
            path=directory.path,
            metadata={
                "direct_symbol": "AUDCAD",
                "period": "2008",
                "timeframe": "M1",
            },
        ),
        metadata={
            "samples": [
                {
                    "denominator_symbol": "CADCHF",
                    "direct_symbol": "AUDCAD",
                    "numerator_symbol": "AUDCHF",
                    "period": "2008",
                    "relationship": "AUDCHF / CADCHF ~= AUDCAD",
                    "timeframe": "M1",
                }
            ]
        },
    )
    return QualityReport(
        targets=(directory,),
        rule_results=(
            QualityRuleResult(
                rule_id="domain.cross_instrument_consistency",
                target=directory,
                findings=(finding,),
            ),
        ),
    )


def _target(path: Path) -> QualityTarget:
    return QualityTarget(
        path=str(path),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )

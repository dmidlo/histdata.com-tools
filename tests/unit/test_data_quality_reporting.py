"""Tests for data-quality report output and exit policy helpers."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    QUALITY_NEXT_ACTIONS_METADATA_KEY,
    QUALITY_NEXT_ACTIONS_SCHEMA_VERSION,
    QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY,
    QUALITY_REMEDIATION_COVERAGE_METADATA_KEY,
    QUALITY_REMEDIATION_COVERAGE_SCHEMA_VERSION,
    QUALITY_REPORTING_METADATA_KEY,
    QUALITY_REPORT_SCHEMA_VERSION,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION,
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
    format_quality_remediation_catalog_audit_lines,
    format_quality_remediation_coverage_lines,
    publish_safe_json_value,
    publish_safe_path,
    quality_next_actions_summary,
    quality_remediation_catalog_audit_summary,
    quality_remediation_coverage_summary,
    quality_report_payload,
    quality_report_to_json,
    series_fingerprint_readiness_summary,
    series_fingerprint_readiness_risk_summary,
    series_fingerprint_regime_summary,
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


def test_quality_report_payload_adds_fingerprint_readiness_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize audit-backed dynamics summaries."""
    payload = quality_report_payload(_fingerprint_dynamics_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
    ]
    target_summaries = summary["target_summaries"]

    assert (
        summary["schema_version"]
        == TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
    )
    assert summary["target_count"] == 3
    assert summary["applicable_dynamics_status_counts"] == {
        "limited": 1,
        "valid": 2,
    }
    assert summary["dynamics_status_counts"]["return_dynamics"] == {
        "limited": 1,
        "skipped": 1,
        "valid": 1,
    }
    assert summary["dynamics_status_counts"]["microstructure_dynamics"] == {
        "skipped": 2,
        "valid": 1,
    }
    assert summary["dependence_status_counts"] == {
        "limited": 1,
        "valid": 2,
    }
    assert summary["dependence_reason_counts"] == {
        "invalid_timestamps_skipped": 1,
    }
    assert summary["dependence_skipped_lag_reason_counts"] == {
        "insufficient_sample_count": 4,
        "zero_variance": 1,
    }
    assert summary["dependence_computed_lag_count"] == 17
    assert summary["dependence_skipped_lag_count"] == 5
    assert summary["topology_limitation_counts"] == {
        "duplicate_timestamps": 1,
        "expected_session_closures": 1,
        "invalid_timestamps_skipped": 1,
        "non_monotonic_timestamp_order": 1,
        "suspicious_gaps": 1,
    }
    assert summary["row_order_counts"] == {
        "cache_order": 1,
        "source_text_order": 2,
    }
    assert summary["cache_source_counts"] == {"direct": 1}
    assert summary["tick_spread_conditioning_status_counts"] == {
        "eligible": 1,
        "ineligible": 2,
    }
    assert [item["target_axis"]["symbol"] for item in target_summaries] == [
        "GBPUSD",
        "EURUSD",
        "EURUSD",
    ]

    limited = target_summaries[0]
    assert limited["applicable_dynamics_section"] == "return_dynamics"
    assert limited["applicable_dynamics_status"] == "limited"
    assert limited["applicable_dynamics_reason"] == "invalid_timestamps_skipped"
    assert limited["topology"]["duplicate_timestamp_count"] == 2
    assert limited["return_dynamics"]["limitations"] == [
        "invalid_timestamps_skipped",
        "non_monotonic_timestamp_order",
        "duplicate_timestamps",
        "suspicious_gaps",
        "expected_session_closures",
    ]
    assert limited["dependence"]["status"] == "limited"
    assert limited["dependence"]["reason"] == "invalid_timestamps_skipped"
    assert limited["dependence"]["acf_basis"] == "observed_sequence"
    assert limited["dependence"]["lags"] == [1, 3]
    assert limited["dependence"]["computed_lag_count"] == 3
    assert limited["dependence"]["skipped_lag_count"] == 5
    assert limited["dependence"]["skipped_lag_reason_counts"] == {
        "insufficient_sample_count": 4,
        "zero_variance": 1,
    }
    assert limited["dependence"]["series"]["close_log_return_acf"] == {
        "sample_count": 2,
        "computed_lag_count": 1,
        "skipped_lag_count": 1,
        "skipped_lag_reason_counts": {"insufficient_sample_count": 1},
    }

    tick = target_summaries[2]
    assert tick["applicable_dynamics_section"] == "microstructure_dynamics"
    assert tick["microstructure_dynamics"]["spread_jump"]["count"] == 1
    assert tick["microstructure_dynamics"]["stale_quote"]["run_count"] == 1
    assert tick["microstructure_dynamics"]["burst"]["burst_rate"] == 0.5
    assert tick["microstructure_dynamics"]["one_sided_movement"] == {
        "count": 2,
        "rate": 0.5,
        "bid_only_count": 1,
        "ask_only_count": 1,
        "run_count": 0,
    }
    assert tick["dependence"]["status"] == "valid"
    assert tick["dependence"]["series"]["spread_acf"]["sample_count"] == 5


def test_quality_report_payload_adds_fingerprint_readiness_risk_metadata(
    tmp_path: Path,
) -> None:
    """Fingerprint reports should serialize a bounded risk ranking."""
    payload = quality_report_payload(_fingerprint_dynamics_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY
    ]
    target_risks = summary["target_risks"]

    assert (
        summary["schema_version"]
        == TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION
    )
    assert summary["source_schema_version"] == (
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
    )
    assert summary["target_count"] == 3
    assert summary["risk_target_count"] == 3
    assert summary["included_target_count"] == 3
    assert summary["clean_target_count"] == 0
    assert summary["reason_counts"]["invalid_timestamps_skipped"] == 3
    assert summary["reason_counts"]["duplicate_timestamps"] == 3
    assert summary["reason_counts"]["suspicious_gaps"] == 3
    assert summary["reason_counts"]["skipped_dependence_lags"] == 1
    assert summary["reason_counts"]["insufficient_sample_count"] == 1
    assert summary["reason_counts"]["missing_regime_summary"] == 3
    assert summary["section_risk_counts"]["dependence"] == 1
    assert summary["report_surface_evidence"]["surface_count"] >= 1

    assert target_risks[0]["rank"] == 1
    assert target_risks[0]["target_axis"]["symbol"] == "GBPUSD"
    assert target_risks[0]["risk_level"] == "high"
    assert "temporal_topology" in {
        item["section"] for item in target_risks[0]["section_risks"]
    }
    assert set(target_risks[0]["reason_codes"]) >= {
        "invalid_timestamps_skipped",
        "duplicate_timestamps",
        "suspicious_gaps",
        "skipped_dependence_lags",
    }
    assert "time_series_fingerprint" in json.dumps(payload, sort_keys=True)


def test_fingerprint_readiness_summary_handles_absent_dynamics_sections(
    tmp_path: Path,
) -> None:
    """Reports should render stable skipped reasons for older fingerprints."""
    payload = quality_report_payload(_fingerprint_report(tmp_path))

    summary = payload["metadata"][
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
    ]
    target = summary["target_summaries"][0]

    assert summary["applicable_dynamics_status_counts"] == {"skipped": 2}
    assert summary["dynamics_reason_counts"]["return_dynamics"] == {
        "not_emitted": 1,
        "unsupported_target_kind": 1,
    }
    assert summary["dependence_status_counts"] == {"skipped": 2}
    assert summary["dependence_reason_counts"] == {
        "not_emitted": 1,
        "unsupported_target_kind": 1,
    }
    assert target["applicable_dynamics_section"] == "return_dynamics"
    assert target["return_dynamics"] == {
        "status": "skipped",
        "reason": "not_emitted",
        "basis": "unknown",
        "row_order": "unknown",
        "computed_from": "unknown",
        "cache_source": None,
        "regular_grid": False,
        "limitations": [],
        "row_count": 0,
        "sampled_row_count": 0,
        "usable_row_count": 0,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
    }
    assert target["dependence"] == _empty_dependence_summary("not_emitted")


def test_fingerprint_readiness_risk_summary_handles_missing_sections(
    tmp_path: Path,
) -> None:
    """Risk ranking should flag older or unsupported fingerprint payloads."""
    summary = series_fingerprint_readiness_risk_summary(
        _fingerprint_report(tmp_path).findings,
        target_limit=-1,
    )

    assert summary is not None
    assert summary["target_count"] == 2
    assert summary["risk_target_count"] == 2
    assert summary["reason_counts"]["not_emitted"] == 5
    assert summary["reason_counts"]["unsupported_target_kind"] == 4
    assert summary["target_risks"][0]["target_axis"]["kind"] == "spreadsheet"
    assert (
        "unsupported_target_kind" in summary["target_risks"][0]["reason_codes"]
    )


def test_fingerprint_console_summary_reports_readiness_lines(
    tmp_path: Path,
) -> None:
    """Human output should summarize fingerprint readiness and dynamics."""
    output = format_quality_console_summary(
        _fingerprint_dynamics_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint readiness" in output
    assert "- targets: 3 included: 3 omitted: 0" in output
    assert "- applicable dynamics statuses: limited=1, valid=2" in output
    assert (
        "- return dynamics: limited=1, skipped=1, valid=1 "
        "reasons: invalid_timestamps_skipped=1, unsupported_timeframe=1"
    ) in output
    assert (
        "- dependence: limited=1, valid=2 reasons: "
        "invalid_timestamps_skipped=1 skipped-lag reasons: "
        "insufficient_sample_count=4, zero_variance=1 "
        "acf_basis: observed_sequence=3 computed_lags=17 skipped_lags=5"
    ) in output
    assert (
        "- topology limitations: duplicate_timestamps=1, "
        "expected_session_closures=1, invalid_timestamps_skipped=1, "
        "non_monotonic_timestamp_order=1, suspicious_gaps=1"
    ) in output
    assert (
        "- dependence limitations: duplicate_timestamps=1, "
        "expected_session_closures=1, invalid_timestamps_skipped=1, "
        "non_monotonic_timestamp_order=1, suspicious_gaps=1"
    ) in output
    assert (
        "- ascii GBPUSD M1 201202 csv: return_dynamics limited, "
        "reason=invalid_timestamps_skipped"
    ) in output
    assert "row_order=source_text_order" in output
    assert (
        "limitations=invalid_timestamps_skipped, non_monotonic_timestamp_order"
        in output
    )
    assert "close_returns=3 median=0.0001" in output
    assert (
        "dependence=limited reason=invalid_timestamps_skipped "
        "acf_basis=observed_sequence lags=[1,3] computed_lags=3 "
        "skipped_lags=5 skipped_reasons=insufficient_sample_count=4, "
        "zero_variance=1"
    ) in output
    assert "close_log_return_acf:samples=2/computed=1/skipped=1" in output
    assert (
        "- ascii EURUSD T 201202 csv: microstructure_dynamics valid"
    ) in output
    assert "interarrival=4 median=250ms" in output
    assert "spread_jumps=1 rate=0.25" in output
    assert "one_sided=2 bid_only=1 ask_only=1" in output


def test_fingerprint_console_summary_reports_readiness_risk_lines(
    tmp_path: Path,
) -> None:
    """Human quality output should include ranked fingerprint risk targets."""
    output = format_quality_console_summary(
        _fingerprint_dynamics_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint readiness risk" in output
    assert "- targets: 3 risk: 3 clean: 0 included: 3 omitted: 0" in output
    assert "#1 ascii GBPUSD M1 201202 csv: high" in output
    assert "reasons=duplicate_timestamps" in output
    assert "invalid_timestamps_skipped" in output
    assert "skipped_dependence_lags" in output


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


def test_quality_report_omits_remediation_catalog_audit_by_default(
    tmp_path: Path,
) -> None:
    """Catalog audits should remain opt-in for report compatibility."""
    report = _remediation_catalog_audit_report(tmp_path)

    assert quality_remediation_catalog_audit_summary(report) is None

    payload = quality_report_payload(report)
    assert (
        QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY
        not in payload["metadata"]
    )


def test_quality_report_embeds_enabled_remediation_catalog_audit(
    tmp_path: Path,
) -> None:
    """Enabled reports should carry current-run catalog audit evidence."""
    report = _remediation_catalog_audit_report(tmp_path, enabled=True)

    payload = quality_report_payload(report)
    audit = payload["metadata"][QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY]
    encoded = json.dumps(audit, sort_keys=True)

    assert audit["report_coverage"][0]["source"] == "current-report"
    assert audit["summary"]["report_count"] == 1
    assert audit["summary"]["report_finding_count"] == 2
    assert audit["summary"]["report_unmapped_warning_error_group_count"] == 1
    observed_groups = audit["report_coverage"][0]["remediation_coverage"][
        "unmapped_groups"
    ]
    assert observed_groups[0]["finding_code"] == ("CUSTOM_REPORT_ONLY_GAP")
    assert observed_groups[0]["occurrence_count"] == 1
    assert str(tmp_path) not in encoded


def test_quality_console_summary_renders_remediation_catalog_audit(
    tmp_path: Path,
) -> None:
    """Human quality output should expose report-publication audit gaps."""
    report = _remediation_catalog_audit_report(tmp_path, enabled=True)
    audit = quality_remediation_catalog_audit_summary(report)

    lines = format_quality_remediation_catalog_audit_lines(audit)
    output = format_quality_console_summary(report, check_groups=("time",))

    assert audit is not None
    assert "Remediation catalog audit" in output
    assert "- observed report: reports=1 findings=2" in output
    assert "- observed error custom.report:CUSTOM_REPORT_ONLY_GAP" in output
    assert lines[0] == ""
    assert "CUSTOM_REPORT_ONLY_GAP" in "\n".join(lines)


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
    _assert_count_limit_metadata(
        coverage["count_limits"]["rule_id_counts"],
        limit=1,
        total_count=4,
        included_count=1,
        omitted_count=3,
        truncated=True,
        requested_limit=1,
        default_limit=16,
    )
    _assert_count_limit_metadata(
        coverage["count_limits"]["finding_code_counts"],
        limit=1,
        total_count=4,
        included_count=1,
        omitted_count=3,
        truncated=True,
        requested_limit=1,
        default_limit=16,
    )
    _assert_limit_metadata(
        coverage["limit_metadata"]["groups"],
        limit=1,
        requested_limit=1,
        default_limit=16,
    )
    _assert_limit_metadata(
        coverage["limit_metadata"]["target_axes"],
        limit=1,
        requested_limit=1,
        default_limit=8,
    )

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


def test_bounded_quality_payload_includes_fingerprint_readiness(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose readiness summaries."""
    report = _fingerprint_dynamics_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 3},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    readiness = payload["fingerprint_readiness"]

    assert readiness["applicable_dynamics_status_counts"] == {
        "limited": 1,
        "valid": 2,
    }
    assert readiness["target_summaries"][0]["target_axis"]["symbol"] == (
        "GBPUSD"
    )
    assert readiness["target_summaries"][0]["return_dynamics"]["truncated"] is (
        True
    )


def test_bounded_quality_payload_includes_fingerprint_readiness_risk(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose risk-ranked targets."""
    report = _fingerprint_dynamics_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 3},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    risk = payload["fingerprint_readiness_risk"]

    assert risk["schema_version"] == (
        TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION
    )
    assert risk["risk_target_count"] == 3
    assert risk["target_risks"][0]["target_axis"]["symbol"] == "GBPUSD"
    assert (
        "invalid_timestamps_skipped" in risk["target_risks"][0]["reason_codes"]
    )


def test_fingerprint_regime_summary_reports_calendar_and_conditioning(
    tmp_path: Path,
) -> None:
    """Regime summaries should expose calendar and conditioned spread facts."""
    summary = series_fingerprint_regime_summary(
        _fingerprint_regime_report(tmp_path).findings,
        target_limit=-1,
        count_limit=2,
    )

    assert summary is not None
    assert summary["schema_version"] == (
        TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION
    )
    assert summary["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    assert summary["target_count"] == 4
    assert summary["calendar_regime_target_count"] == 3
    assert summary["conditional_distribution_target_count"] == 1
    assert summary["calendar_status_counts"] == {
        "available": 3,
        "missing": 1,
    }
    assert summary["conditional_status_counts"] == {
        "absent": 1,
        "available": 1,
        "not_applicable": 2,
    }
    assert summary["calendar_profile"] == {
        "complete_count": 1,
        "incomplete_count": 3,
        "source_counts": {
            "operator-config": 1,
            "static_month_day_major_holidays": 2,
            "unknown": 1,
        },
        "static_advisory_count": 2,
        "version_counts": {"1": 2, "2026.06": 1, "unknown": 1},
    }
    assert summary["top_session_state_counts"][:2] == [
        {"value": "market_open", "count": 17},
        {"value": "weekend_closure", "count": 3},
    ]

    tick = next(
        item
        for item in summary["target_summaries"]
        if item["target_axis"]["timeframe"] == "T"
        and item["target_axis"]["symbol"] == "EURUSD"
    )
    conditional = tick["conditional_distributions"]

    assert conditional["status"] == "available"
    assert conditional["basis"] == "text"
    assert conditional["by_active_session"][0]["bucket"] == "london"
    assert conditional["by_active_session"][0]["spread"]["median"] == 0.0002
    assert conditional["by_special_tag"][0]["bucket"] == (
        "london_4pm_fix_window"
    )


def test_quality_report_payload_includes_fingerprint_regime_metadata(
    tmp_path: Path,
) -> None:
    """Full JSON reports should publish bounded regime summaries."""
    payload = quality_report_payload(_fingerprint_regime_report(tmp_path))
    metadata = payload["metadata"]

    assert TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY in metadata
    regime = metadata[TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY]
    assert regime["schema_version"] == (
        TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION
    )
    assert "calendar_policy" not in json.dumps(regime, sort_keys=True)
    assert "quantiles" not in json.dumps(regime, sort_keys=True)
    assert str(tmp_path) not in json.dumps(regime, sort_keys=True)


def test_bounded_quality_payload_includes_fingerprint_regimes(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose regime summaries."""
    report = _fingerprint_regime_report(tmp_path)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("fingerprint",),
        discovery={"roots": [str(tmp_path)], "target_count": 4},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )

    regimes = payload["fingerprint_regime"]

    assert regimes["calendar_status_counts"] == {
        "available": 3,
        "missing": 1,
    }
    assert regimes["conditional_status_counts"]["available"] == 1
    assert regimes["target_summaries"][0]["target_axis"]["symbol"] == "AUDUSD"


def test_quality_console_summary_renders_fingerprint_regimes(
    tmp_path: Path,
) -> None:
    """Console summaries should render regimes without nested JSON inspection."""
    output = format_quality_console_summary(
        _fingerprint_regime_report(tmp_path),
        check_groups=("fingerprint",),
    )

    assert "Fingerprint regimes" in output
    assert "- calendar statuses: available=3, missing=1" in output
    assert "conditioned-spread: 1" in output
    assert "profile=static_month_day_major_holidays/1" in output
    assert "conditioned_spread=text rows=3 usable=3" in output
    assert "london:n=2/median=0.0002/p95=0.0003" in output


def test_fingerprint_regime_summary_is_bounded_and_deterministic(
    tmp_path: Path,
) -> None:
    """Regime summaries should keep deterministic ordering and truncation."""
    summary = series_fingerprint_regime_summary(
        _fingerprint_regime_report(tmp_path).findings,
        target_limit=2,
        count_limit=0,
    )

    assert summary is not None
    assert summary["included_target_count"] == 2
    assert summary["omitted_target_count"] == 2
    assert summary["count_limit"] == 1
    assert summary["truncated"] is True
    assert summary["top_session_state_counts"] == [
        {"value": "market_open", "count": 17}
    ]
    assert [
        item["target_axis"]["symbol"] for item in summary["target_summaries"]
    ] == [
        "AUDUSD",
        "EURUSD",
    ]


def test_fingerprint_readiness_summary_is_bounded_and_issue_first(
    tmp_path: Path,
) -> None:
    """Readiness summaries should truncate after limited targets first."""
    summary = series_fingerprint_readiness_summary(
        _fingerprint_dynamics_report(tmp_path).findings,
        target_limit=1,
    )

    assert summary is not None
    assert summary["target_count"] == 3
    assert summary["included_target_count"] == 1
    assert summary["omitted_target_count"] == 2
    assert summary["truncated"] is True
    assert summary["target_summaries"][0]["target_axis"]["symbol"] == "GBPUSD"
    assert summary["target_summaries"][0]["applicable_dynamics_status"] == (
        "limited"
    )


def test_fingerprint_readiness_summary_orders_limited_dependence_first(
    tmp_path: Path,
) -> None:
    """Bounded readiness summaries should prioritize dependence limitations."""
    limited_target = QualityTarget(
        path=str(tmp_path / "limited-dependence.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="AUDUSD",
        period="201202",
    )
    valid_target = QualityTarget(
        path=str(tmp_path / "valid-dependence.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    limited_payload = _valid_m1_fingerprint_payload(kind="csv")
    axis = limited_payload["target_axis"]
    assert isinstance(axis, dict)
    axis["symbol"] = "AUDUSD"
    axis["kind"] = "csv"
    limited_payload["dependence"] = _dependence_payload(
        status="limited",
        reason="skipped_lags",
        row_count=4,
        sampled_row_count=4,
        usable_row_count=4,
        series={
            "close_log_return_acf": _acf_payload(
                sample_count=2,
                computed=1,
                skipped={"3": "insufficient_sample_count"},
            )
        },
    )
    audit = limited_payload["fingerprint_audit"]
    assert isinstance(audit, dict)
    section_statuses = audit["section_statuses"]
    assert isinstance(section_statuses, dict)
    section_statuses["dependence"] = "limited"
    valid_payload = _valid_m1_fingerprint_payload(kind="csv")

    summary = series_fingerprint_readiness_summary(
        (
            _fingerprint_series_finding(valid_target, valid_payload),
            _fingerprint_series_finding(limited_target, limited_payload),
        ),
        target_limit=1,
    )

    assert summary is not None
    assert summary["target_count"] == 2
    assert summary["included_target_count"] == 1
    assert summary["omitted_target_count"] == 1
    assert summary["target_summaries"][0]["target_axis"]["symbol"] == "AUDUSD"
    assert summary["target_summaries"][0]["applicable_dynamics_status"] == (
        "valid"
    )
    assert summary["target_summaries"][0]["dependence"]["status"] == "limited"


def test_fingerprint_readiness_risk_summary_is_bounded_and_stable(
    tmp_path: Path,
) -> None:
    """Risk ranking should bound target, section, and reason surfaces."""
    summary = series_fingerprint_readiness_risk_summary(
        _fingerprint_dynamics_report(tmp_path).findings,
        target_limit=1,
        section_limit=1,
        reason_limit=1,
    )

    assert summary is not None
    assert summary["target_count"] == 3
    assert summary["risk_target_count"] == 3
    assert summary["included_target_count"] == 1
    assert summary["omitted_target_count"] == 2
    assert summary["truncated"] is True
    assert summary["target_risks"][0]["target_axis"]["symbol"] == "GBPUSD"
    assert summary["target_risks"][0]["included_section_risk_count"] == 1
    assert summary["target_risks"][0]["omitted_section_risk_count"] > 0
    assert len(summary["target_risks"][0]["reason_codes"]) == 1
    assert summary["limit_metadata"]["targets"]["requested_limit"] == 1
    assert summary["limit_metadata"]["sections"]["requested_limit"] == 1
    assert summary["limit_metadata"]["reasons"]["requested_limit"] == 1


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
    _assert_count_limit_metadata(
        payload["payload_limits"]["next_actions"],
        limit=16,
        total_count=2,
        included_count=2,
        omitted_count=0,
        truncated=False,
        requested_limit=16,
        default_limit=16,
    )
    _assert_limit_metadata(
        payload["payload_limits"]["next_actions"]["target_axes"],
        limit=8,
        requested_limit=8,
        default_limit=8,
    )

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
    _assert_count_limit_metadata(
        prebounded_payload["payload_limits"]["next_actions"],
        limit=1,
        total_count=2,
        included_count=1,
        omitted_count=1,
        truncated=True,
        requested_limit=1,
        default_limit=16,
    )


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
    _assert_count_limit_metadata(
        payload["payload_limits"]["remediation_coverage"],
        limit=16,
        total_count=3,
        included_count=3,
        omitted_count=0,
        truncated=False,
        requested_limit=16,
        default_limit=16,
    )
    assert (
        payload["payload_limits"]["remediation_coverage"]["target_axis_limit"]
        == 8
    )
    _assert_limit_metadata(
        payload["payload_limits"]["remediation_coverage"]["target_axes"],
        limit=8,
        requested_limit=8,
        default_limit=8,
    )


def test_bounded_quality_payload_includes_enabled_remediation_catalog_audit(
    tmp_path: Path,
) -> None:
    """Bounded orchestration payloads should expose enabled catalog audits."""
    report = _remediation_catalog_audit_report(tmp_path, enabled=True)
    payload = bounded_quality_payload(
        operation="data-quality",
        check_groups=("time",),
        discovery={"roots": [str(tmp_path)], "target_count": 2},
        report=report,
        decision=QualityExitPolicy.from_values().evaluate(report.summary()),
        artifact=None,
    )
    audit = payload["remediation_catalog_audit"]
    encoded = json.dumps(audit, sort_keys=True)

    assert "rule_results" not in payload
    assert audit["summary"]["report_count"] == 1
    assert audit["summary"]["report_unmapped_warning_error_group_count"] == 1
    assert audit["report_coverage"][0]["remediation_coverage"][
        "unmapped_groups"
    ][0]["finding_code"] == ("CUSTOM_REPORT_ONLY_GAP")
    assert (
        payload["payload_limits"]["remediation_catalog_audit"][
            "target_axis_limit"
        ]
        == 8
    )
    assert str(tmp_path) not in encoded


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


def _remediation_catalog_audit_report(
    tmp_path: Path,
    *,
    enabled: bool = False,
) -> QualityReport:
    target = _target(tmp_path / "catalog-audit.csv")
    mapped = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="ASCII_M1_DUPLICATE_TIMESTAMP",
        message="duplicate minute bar timestamp",
        rule_id="time.ascii.sequence",
        target=target,
    )
    unmapped = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="CUSTOM_REPORT_ONLY_GAP",
        message="custom report-only finding without remediation guidance",
        rule_id="custom.report",
        target=target,
    )
    metadata = (
        {
            QUALITY_REPORTING_METADATA_KEY: {
                QUALITY_REMEDIATION_CATALOG_AUDIT_METADATA_KEY: {
                    "enabled": True,
                }
            }
        }
        if enabled
        else {}
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=target,
                findings=(mapped,),
            ),
            QualityRuleResult(
                rule_id="custom.report",
                target=target,
                findings=(unmapped,),
            ),
        ),
        metadata=metadata,
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


def _fingerprint_dynamics_report(tmp_path: Path) -> QualityReport:
    valid_m1 = QualityTarget(
        path=str(tmp_path / "valid.data"),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    limited_m1 = QualityTarget(
        path=str(tmp_path / "limited.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="GBPUSD",
        period="201202",
    )
    tick = QualityTarget(
        path=str(tmp_path / "ticks.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    findings = (
        _fingerprint_series_finding(
            valid_m1,
            _valid_m1_fingerprint_payload(kind="cache"),
        ),
        _fingerprint_series_finding(
            limited_m1,
            _limited_m1_fingerprint_payload(),
        ),
        _fingerprint_series_finding(tick, _tick_fingerprint_payload()),
    )
    return QualityReport(
        targets=(valid_m1, limited_m1, tick),
        rule_results=tuple(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=finding.target,
                findings=(finding,),
            )
            for finding in findings
        ),
    )


def _fingerprint_regime_report(tmp_path: Path) -> QualityReport:
    missing_m1 = QualityTarget(
        path=str(tmp_path / "missing-calendar.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="AUDUSD",
        period="201202",
    )
    valid_m1 = QualityTarget(
        path=str(tmp_path / "calendar.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    conditioned_tick = QualityTarget(
        path=str(tmp_path / "conditioned-ticks.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    absent_tick = QualityTarget(
        path=str(tmp_path / "plain-ticks.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="T",
        symbol="GBPUSD",
        period="201202",
    )

    missing_payload = _valid_m1_fingerprint_payload(kind="csv")
    _set_payload_axis(missing_payload, symbol="AUDUSD", kind="csv")
    missing_payload.pop("calendar_regimes", None)

    m1_payload = _valid_m1_fingerprint_payload(kind="csv")
    m1_payload["calendar_regimes"] = _calendar_regimes_payload(
        row_count=10,
        parsed_row_count=10,
        session_state_counts={"market_open": 8, "weekend_closure": 2},
        active_session_counts={"london": 4, "new_york": 3, "asia": 1},
        special_tag_counts={"daily_rollover": 2, "month_end": 1},
        holiday_tag_counts={"major_holiday:christmas_day": 1},
        event_tag_counts={},
        hour_of_day_counts={"11": 5, "12": 3, "17": 2},
        day_of_week_counts={"wednesday": 8, "sunday": 2},
        profile_name="static-major-holidays",
        profile_source="static_month_day_major_holidays",
        profile_version="1",
        profile_complete=False,
        profile_static_advisory=True,
    )

    conditioned_payload = _tick_fingerprint_payload()
    conditioned_payload["calendar_regimes"] = _calendar_regimes_payload(
        row_count=7,
        parsed_row_count=7,
        session_state_counts={"market_open": 6, "weekend_closure": 1},
        active_session_counts={"london": 4, "new_york": 2},
        special_tag_counts={"london_4pm_fix_window": 2},
        holiday_tag_counts={},
        event_tag_counts={"crisis:covid_shock": 1},
        hour_of_day_counts={"11": 4, "16": 2, "17": 1},
        day_of_week_counts={"wednesday": 6, "sunday": 1},
        profile_name="complete-calendar",
        profile_source="operator-config",
        profile_version="2026.06",
        profile_complete=True,
        profile_static_advisory=False,
    )
    conditioned_payload["conditional_distributions"] = (
        _conditional_distributions_payload()
    )

    absent_payload = _tick_fingerprint_payload()
    _set_payload_axis(absent_payload, symbol="GBPUSD", kind="csv")
    absent_payload["calendar_regimes"] = _calendar_regimes_payload(
        row_count=3,
        parsed_row_count=3,
        session_state_counts={"market_open": 3},
        active_session_counts={"london": 2, "new_york": 1},
        special_tag_counts={},
        holiday_tag_counts={},
        event_tag_counts={},
        hour_of_day_counts={"11": 2, "12": 1},
        day_of_week_counts={"thursday": 3},
        profile_name="static-major-holidays",
        profile_source="static_month_day_major_holidays",
        profile_version="1",
        profile_complete=False,
        profile_static_advisory=True,
    )
    absent_payload.pop("conditional_distributions", None)

    findings = (
        _fingerprint_series_finding(missing_m1, missing_payload),
        _fingerprint_series_finding(valid_m1, m1_payload),
        _fingerprint_series_finding(conditioned_tick, conditioned_payload),
        _fingerprint_series_finding(absent_tick, absent_payload),
    )
    return QualityReport(
        targets=(missing_m1, valid_m1, conditioned_tick, absent_tick),
        rule_results=tuple(
            QualityRuleResult(
                rule_id=SERIES_FINGERPRINT_RULE_ID,
                target=finding.target,
                findings=(finding,),
            )
            for finding in findings
        ),
    )


def _set_payload_axis(
    payload: dict[str, object],
    *,
    symbol: str,
    kind: str,
) -> None:
    axis = payload["target_axis"]
    assert isinstance(axis, dict)
    axis["symbol"] = symbol
    axis["kind"] = kind


def _calendar_regimes_payload(
    *,
    row_count: int,
    parsed_row_count: int,
    session_state_counts: dict[str, int],
    active_session_counts: dict[str, int],
    special_tag_counts: dict[str, int],
    holiday_tag_counts: dict[str, int],
    event_tag_counts: dict[str, int],
    hour_of_day_counts: dict[str, int],
    day_of_week_counts: dict[str, int],
    profile_name: str,
    profile_source: str,
    profile_version: str,
    profile_complete: bool,
    profile_static_advisory: bool,
) -> dict[str, object]:
    return {
        "status": "ok",
        "computed_from": "text_scan",
        "cache_source": None,
        "row_count": row_count,
        "parsed_row_count": parsed_row_count,
        "invalid_timestamp_count": 0,
        "session_state_counts": session_state_counts,
        "active_session_counts": active_session_counts,
        "special_tag_counts": special_tag_counts,
        "holiday_tag_counts": holiday_tag_counts,
        "event_tag_counts": event_tag_counts,
        "hour_of_day_counts": hour_of_day_counts,
        "day_of_week_counts": day_of_week_counts,
        "calendar_profile_complete": profile_complete,
        "missing_optional_calendar_data": not profile_complete,
        "calendar_policy": {
            "holiday_calendar_source": profile_source,
            "holiday_calendar_complete": profile_complete,
            "holiday_calendar_static_advisory": profile_static_advisory,
            "calendar_profile": {
                "name": profile_name,
                "source": profile_source,
                "version": profile_version,
                "complete": profile_complete,
                "static_advisory": profile_static_advisory,
            },
        },
    }


def _conditional_distributions_payload() -> dict[str, object]:
    return {
        "basis": "text",
        "metric": "tick_spread",
        "row_count": 3,
        "sampled_row_count": 3,
        "usable_row_count": 3,
        "invalid_row_count": 0,
        "truncated": False,
        "by_active_session": {
            "london": {"spread": _numeric(count=2, median=0.0002, p95=0.0003)},
            "new_york": {
                "spread": _numeric(count=1, median=0.0004, p95=0.0004)
            },
        },
        "by_special_tag": {
            "london_4pm_fix_window": {
                "spread": _numeric(count=1, median=0.0003, p95=0.0003)
            }
        },
    }


def _fingerprint_series_finding(
    target: QualityTarget,
    payload: dict[str, object],
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=target,
        metadata={"time_series_fingerprint": payload},
    )


def _valid_m1_fingerprint_payload(*, kind: str) -> dict[str, object]:
    return {
        "target_axis": {
            "data_format": "ascii",
            "timeframe": "M1",
            "symbol": "EURUSD",
            "period": "201202",
            "kind": kind,
        },
        "coverage": {"row_count": 4, "parsed_row_count": 4},
        "temporal_topology": _topology_payload(
            computed_from="direct_cache",
            cache_source="direct",
        ),
        "m1_bar_distribution": {"row_count": 4, "usable_row_count": 4},
        "return_dynamics": {
            "basis": "observed_sequence",
            "row_order": "cache_order",
            "computed_from": "direct_cache",
            "cache_source": "direct",
            "regular_grid": False,
            "sequence_status": "ok",
            "limitations": [],
            "row_count": 4,
            "sampled_row_count": 4,
            "usable_row_count": 4,
            "invalid_row_count": 0,
            "partial_row_count": 0,
            "truncated": False,
            "close_log_return": _numeric(count=3, median=0.0001),
            "absolute_return": _numeric(count=3, median=0.0001, p95=0.0003),
            "squared_return": _numeric(count=3, median=0.0),
            "open_jump": _numeric(count=3, median=0.0, p95=0.0002),
            "flatline": {
                "zero_return_count": 1,
                "zero_return_rate": 0.333333333333,
                "zero_return_run_count": 1,
                "ohlc_flatline_row_count": 0,
                "ohlc_flatline_rate": 0.0,
                "ohlc_flatline_run_count": 0,
                "ohlc_flatline_affected_row_count": 0,
            },
        },
        "dependence": _dependence_payload(
            status="ok",
            row_order="cache_order",
            computed_from="direct_cache",
            cache_source="direct",
            row_count=4,
            sampled_row_count=4,
            usable_row_count=4,
            series={
                "absolute_return_acf": _acf_payload(sample_count=3, computed=2),
                "close_log_return_acf": _acf_payload(
                    sample_count=3,
                    computed=2,
                ),
                "range_ratio_acf": _acf_payload(sample_count=4, computed=2),
                "squared_return_acf": _acf_payload(sample_count=3, computed=2),
            },
        ),
        "fingerprint_audit": _audit_payload(
            expected=(
                "coverage",
                "temporal_topology",
                "calendar_regimes",
                "m1_bar_distribution",
                "return_dynamics",
                "dependence",
            ),
            emitted=(
                "coverage",
                "temporal_topology",
                "m1_bar_distribution",
                "return_dynamics",
                "dependence",
            ),
            skipped={"calendar_regimes": "not_emitted"},
            section_statuses={
                "coverage": "valid",
                "temporal_topology": "valid",
                "calendar_regimes": "skipped",
                "m1_bar_distribution": "valid",
                "return_dynamics": "valid",
                "dependence": "valid",
            },
            return_status="valid",
            micro_status="skipped",
            micro_reason="unsupported_timeframe",
        ),
        "source": {"kind": "cache", "cache_source": "direct"},
    }


def _limited_m1_fingerprint_payload() -> dict[str, object]:
    limitations = [
        "invalid_timestamps_skipped",
        "non_monotonic_timestamp_order",
        "duplicate_timestamps",
        "suspicious_gaps",
        "expected_session_closures",
    ]
    return {
        "target_axis": {
            "data_format": "ascii",
            "timeframe": "M1",
            "symbol": "GBPUSD",
            "period": "201202",
            "kind": "csv",
        },
        "coverage": {"row_count": 4, "parsed_row_count": 3},
        "temporal_topology": _topology_payload(
            invalid_timestamp_count=1,
            duplicate_timestamp_count=2,
            non_monotonic_count=1,
            suspicious_gap_count=1,
            expected_session_closure_count=1,
        ),
        "m1_bar_distribution": {"row_count": 4, "usable_row_count": 3},
        "return_dynamics": {
            "basis": "observed_sequence",
            "row_order": "source_text_order",
            "computed_from": "text_scan",
            "cache_source": None,
            "regular_grid": False,
            "sequence_status": "limited",
            "limitations": limitations,
            "row_count": 4,
            "sampled_row_count": 3,
            "usable_row_count": 3,
            "invalid_row_count": 1,
            "partial_row_count": 1,
            "truncated": True,
            "close_log_return": _numeric(count=3, median=0.0001),
            "absolute_return": _numeric(count=3, median=0.0002, p95=0.0008),
            "squared_return": _numeric(count=3, median=0.0),
            "open_jump": _numeric(count=3, median=0.0001, p95=0.0004),
            "flatline": {
                "zero_return_count": 0,
                "zero_return_rate": 0.0,
                "zero_return_run_count": 0,
                "ohlc_flatline_row_count": 2,
                "ohlc_flatline_rate": 0.5,
                "ohlc_flatline_run_count": 1,
                "ohlc_flatline_affected_row_count": 2,
            },
        },
        "dependence": _dependence_payload(
            status="limited",
            reason="invalid_timestamps_skipped",
            limitations=tuple(limitations),
            row_count=4,
            sampled_row_count=3,
            usable_row_count=3,
            invalid_row_count=1,
            partial_row_count=1,
            truncated=True,
            series={
                "absolute_return_acf": _acf_payload(
                    sample_count=3,
                    computed=1,
                    skipped={"3": "insufficient_sample_count"},
                ),
                "close_log_return_acf": _acf_payload(
                    sample_count=2,
                    computed=1,
                    skipped={"3": "insufficient_sample_count"},
                ),
                "range_ratio_acf": _acf_payload(
                    sample_count=3,
                    computed=1,
                    skipped={"3": "insufficient_sample_count"},
                ),
                "squared_return_acf": _acf_payload(
                    sample_count=3,
                    computed=0,
                    skipped={
                        "1": "zero_variance",
                        "3": "insufficient_sample_count",
                    },
                ),
            },
        ),
        "fingerprint_audit": _audit_payload(
            expected=(
                "coverage",
                "temporal_topology",
                "calendar_regimes",
                "m1_bar_distribution",
                "return_dynamics",
                "dependence",
            ),
            emitted=(
                "coverage",
                "temporal_topology",
                "m1_bar_distribution",
                "return_dynamics",
                "dependence",
            ),
            skipped={"calendar_regimes": "not_emitted"},
            section_statuses={
                "coverage": "valid",
                "temporal_topology": "limited",
                "calendar_regimes": "skipped",
                "m1_bar_distribution": "valid",
                "return_dynamics": "limited",
                "dependence": "limited",
            },
            return_status="limited",
            return_reason="invalid_timestamps_skipped",
            return_limitations=tuple(limitations),
            micro_status="skipped",
            micro_reason="unsupported_timeframe",
        ),
        "source": {"kind": "csv_text"},
    }


def _tick_fingerprint_payload() -> dict[str, object]:
    return {
        "target_axis": {
            "data_format": "ascii",
            "timeframe": "T",
            "symbol": "EURUSD",
            "period": "201202",
            "kind": "csv",
        },
        "coverage": {"row_count": 5, "parsed_row_count": 5},
        "temporal_topology": _topology_payload(computed_from="text_scan"),
        "tick_distribution": {"row_count": 5, "usable_row_count": 5},
        "conditional_distributions": {"usable_row_count": 5},
        "microstructure_dynamics": {
            "basis": "observed_sequence",
            "row_order": "source_text_order",
            "computed_from": "text_scan",
            "cache_source": None,
            "regular_grid": False,
            "sequence_status": "ok",
            "limitations": [],
            "row_count": 5,
            "sampled_row_count": 5,
            "usable_row_count": 5,
            "invalid_row_count": 0,
            "partial_row_count": 0,
            "truncated": False,
            "interarrival_ms": _numeric(count=4, median=250.0, p95=500.0),
            "spread": _numeric(count=5, median=0.0001, p95=0.0003),
            "spread_change": _numeric(count=4, median=0.0, p95=0.0002),
            "absolute_spread_change": _numeric(
                count=4,
                median=0.0001,
                p95=0.0002,
            ),
            "zero_spread_count": 0,
            "negative_spread_count": 0,
            "zero_spread_rate": 0.0,
            "negative_spread_rate": 0.0,
            "spread_jump": {
                "threshold": 0.0002,
                "count": 1,
                "rate": 0.25,
            },
            "stale_quote": {
                "repeat_count": 1,
                "repeat_rate": 0.25,
                "run_count": 1,
                "affected_row_count": 2,
            },
            "burst": {
                "interval_count": 2,
                "burst_rate": 0.5,
                "run_count": 1,
                "tick_count": 3,
            },
            "one_sided_movement": {
                "count": 2,
                "rate": 0.5,
                "bid_only_count": 1,
                "ask_only_count": 1,
                "run_count": 0,
            },
        },
        "dependence": _dependence_payload(
            status="ok",
            lags=(1, 2),
            row_count=5,
            sampled_row_count=5,
            usable_row_count=5,
            series={
                "absolute_spread_change_acf": _acf_payload(
                    sample_count=4,
                    computed=2,
                ),
                "spread_acf": _acf_payload(sample_count=5, computed=2),
                "spread_change_acf": _acf_payload(sample_count=4, computed=2),
            },
        ),
        "fingerprint_audit": _audit_payload(
            expected=(
                "coverage",
                "temporal_topology",
                "calendar_regimes",
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
                "dependence",
            ),
            emitted=(
                "coverage",
                "temporal_topology",
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
                "dependence",
            ),
            skipped={"calendar_regimes": "not_emitted"},
            section_statuses={
                "coverage": "valid",
                "temporal_topology": "valid",
                "calendar_regimes": "skipped",
                "tick_distribution": "valid",
                "conditional_distributions": "valid",
                "microstructure_dynamics": "valid",
                "dependence": "valid",
            },
            return_status="skipped",
            return_reason="unsupported_timeframe",
            micro_status="valid",
            tick_spread_status="eligible",
            tick_spread_eligible=True,
            tick_spread_emitted=True,
        ),
        "source": {"kind": "csv_text"},
    }


def _topology_payload(
    *,
    row_count: int = 4,
    parsed_row_count: int = 4,
    invalid_timestamp_count: int = 0,
    duplicate_timestamp_count: int = 0,
    non_monotonic_count: int = 0,
    suspicious_gap_count: int = 0,
    expected_session_closure_count: int = 0,
    computed_from: str = "text_scan",
    cache_source: str | None = None,
) -> dict[str, object]:
    return {
        "row_count": row_count,
        "parsed_row_count": parsed_row_count,
        "invalid_timestamp_count": invalid_timestamp_count,
        "duplicate_timestamp_count": duplicate_timestamp_count,
        "non_monotonic_count": non_monotonic_count,
        "median_interval_ms": 60_000,
        "max_gap_ms": 60_000,
        "suspicious_gap_count": suspicious_gap_count,
        "expected_session_closure_count": expected_session_closure_count,
        "weekend_activity_count": 0,
        "sampling_basis": "observed_sequence",
        "computed_from": computed_from,
        "cache_source": cache_source,
    }


def _audit_payload(
    *,
    expected: tuple[str, ...],
    emitted: tuple[str, ...],
    skipped: dict[str, str],
    section_statuses: dict[str, str],
    return_status: str,
    micro_status: str,
    return_reason: str | None = None,
    micro_reason: str | None = None,
    return_limitations: tuple[str, ...] = (),
    tick_spread_status: str = "ineligible",
    tick_spread_eligible: bool = False,
    tick_spread_emitted: bool = False,
) -> dict[str, object]:
    tick_spread: dict[str, object] = {
        "eligible": tick_spread_eligible,
        "status": tick_spread_status,
        "emitted": tick_spread_emitted,
    }
    if not tick_spread_eligible:
        tick_spread["reason"] = "unsupported_timeframe"
    return {
        "sections_expected": list(expected),
        "sections_emitted": list(emitted),
        "sections_skipped": {
            section: {"reason": reason} for section, reason in skipped.items()
        },
        "section_statuses": section_statuses,
        "conditional_distribution_eligibility": {
            "tick_spread": tick_spread,
        },
        "profile_completeness": {
            "source": "quality_profile",
            "calendar_profile_complete": False,
            "missing_optional_calendar_data": True,
            "calendar_profile_name": "static-major-holidays",
            "calendar_profile_source": "static_month_day_major_holidays",
            "calendar_profile_version": "1",
            "calendar_profile_static_advisory": True,
        },
        "dynamics_readiness": {
            "return_dynamics": _readiness_payload(
                status=return_status,
                reason=return_reason,
                row_order=(
                    "source_text_order"
                    if return_status == "limited"
                    else "cache_order" if return_status == "valid" else None
                ),
                computed_from=(
                    "text_scan"
                    if return_status == "limited"
                    else "direct_cache" if return_status == "valid" else None
                ),
                cache_source="direct" if return_status == "valid" else None,
                limitations=return_limitations,
                row_count=4 if return_status in {"valid", "limited"} else 0,
                sampled_row_count=(
                    3
                    if return_status == "limited"
                    else 4 if return_status == "valid" else 0
                ),
                usable_row_count=(
                    3
                    if return_status == "limited"
                    else 4 if return_status == "valid" else 0
                ),
                invalid_row_count=1 if return_status == "limited" else 0,
                partial_row_count=1 if return_status == "limited" else 0,
                truncated=return_status == "limited",
            ),
            "microstructure_dynamics": _readiness_payload(
                status=micro_status,
                reason=micro_reason,
                row_order=(
                    "source_text_order" if micro_status == "valid" else None
                ),
                computed_from="text_scan" if micro_status == "valid" else None,
                row_count=5 if micro_status == "valid" else 0,
                sampled_row_count=5 if micro_status == "valid" else 0,
                usable_row_count=5 if micro_status == "valid" else 0,
            ),
        },
    }


def _dependence_payload(
    *,
    status: str,
    reason: str | None = None,
    lags: tuple[int, ...] = (1, 3),
    row_order: str = "source_text_order",
    computed_from: str = "text_scan",
    cache_source: str | None = None,
    limitations: tuple[str, ...] = (),
    row_count: int,
    sampled_row_count: int,
    usable_row_count: int,
    invalid_row_count: int = 0,
    partial_row_count: int = 0,
    truncated: bool = False,
    series: dict[str, dict[str, object]],
) -> dict[str, object]:
    computed_lag_count = sum(
        int(item["computed_lag_count"]) for item in series.values()
    )
    skipped_lag_count = sum(
        int(item["skipped_lag_count"]) for item in series.values()
    )
    payload: dict[str, object] = {
        "basis": "observed_sequence",
        "acf_basis": "observed_sequence",
        "row_order": row_order,
        "computed_from": computed_from,
        "cache_source": cache_source,
        "regular_grid": False,
        "dependence_status": status,
        "limitations": list(limitations),
        "row_count": row_count,
        "sampled_row_count": sampled_row_count,
        "usable_row_count": usable_row_count,
        "invalid_row_count": invalid_row_count,
        "partial_row_count": partial_row_count,
        "truncated": truncated,
        "lags": list(lags),
        "computed_lag_count": computed_lag_count,
        "skipped_lag_count": skipped_lag_count,
    }
    if reason:
        payload["reason"] = reason
    payload.update(series)
    return payload


def _acf_payload(
    *,
    sample_count: int,
    computed: int,
    skipped: dict[str, str] | None = None,
) -> dict[str, object]:
    skipped = skipped or {}
    return {
        "sample_count": sample_count,
        "lag_acf": {str(index + 1): 0.0 for index in range(computed)},
        "computed_lag_count": computed,
        "skipped_lags": {
            lag: {"reason": reason, "sample_count": sample_count}
            for lag, reason in skipped.items()
        },
        "skipped_lag_count": len(skipped),
    }


def _empty_dependence_summary(reason: str) -> dict[str, object]:
    return {
        "status": "skipped",
        "reason": reason,
        "basis": "unknown",
        "acf_basis": "unknown",
        "row_order": "unknown",
        "computed_from": "unknown",
        "cache_source": None,
        "regular_grid": False,
        "limitations": [],
        "row_count": 0,
        "sampled_row_count": 0,
        "usable_row_count": 0,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
        "lag_count": 0,
        "lag_limit": 16,
        "lags": [],
        "included_lag_count": 0,
        "omitted_lag_count": 0,
        "lags_truncated": False,
        "limit_metadata": {
            "lags": {
                "limit": 16,
                "effective_limit": 16,
                "requested_limit": None,
                "default_limit": 16,
                "minimum_limit": 0,
                "maximum_limit": None,
                "unbounded": False,
            }
        },
        "computed_lag_count": 0,
        "skipped_lag_count": 0,
        "skipped_lag_reason_counts": {},
        "series_count": 0,
        "series": {},
    }


def _readiness_payload(
    *,
    status: str,
    reason: str | None = None,
    row_order: str | None = None,
    computed_from: str | None = None,
    cache_source: str | None = None,
    limitations: tuple[str, ...] = (),
    row_count: int = 0,
    sampled_row_count: int = 0,
    usable_row_count: int = 0,
    invalid_row_count: int = 0,
    partial_row_count: int = 0,
    truncated: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": status,
        "basis": "observed_sequence" if row_order else "unknown",
        "row_order": row_order or "unknown",
        "computed_from": computed_from or "unknown",
        "cache_source": cache_source,
        "regular_grid": False,
        "limitations": list(limitations),
        "row_count": row_count,
        "sampled_row_count": sampled_row_count,
        "usable_row_count": usable_row_count,
        "invalid_row_count": invalid_row_count,
        "partial_row_count": partial_row_count,
        "truncated": truncated,
    }
    if reason:
        payload["reason"] = reason
    return payload


def _numeric(
    *,
    count: int,
    median: float,
    p95: float | None = None,
) -> dict[str, object]:
    return {
        "count": count,
        "min": 0.0,
        "max": p95 if p95 is not None else median,
        "mean": median,
        "median": median,
        "mad": 0.0,
        "quantiles": {
            "0.95": p95 if p95 is not None else median,
            "0.99": p95 if p95 is not None else median,
        },
    }


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


def _assert_count_limit_metadata(
    value: object,
    *,
    limit: int,
    total_count: int,
    included_count: int,
    omitted_count: int,
    truncated: bool,
    requested_limit: int | None,
    default_limit: int,
) -> None:
    assert isinstance(value, dict)
    _assert_limit_metadata(
        value,
        limit=limit,
        requested_limit=requested_limit,
        default_limit=default_limit,
    )
    assert value["total_count"] == total_count
    assert value["included_count"] == included_count
    assert value["omitted_count"] == omitted_count
    assert value["truncated"] is truncated


def _assert_limit_metadata(
    value: object,
    *,
    limit: int,
    requested_limit: int | None,
    default_limit: int,
) -> None:
    assert isinstance(value, dict)
    assert value["limit"] == limit
    assert value["effective_limit"] == limit
    assert value["requested_limit"] == requested_limit
    assert value["default_limit"] == default_limit
    assert value["minimum_limit"] == 0
    assert value["maximum_limit"] is None
    assert value["unbounded"] is (limit < 0)

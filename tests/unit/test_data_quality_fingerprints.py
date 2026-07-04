"""Tests for deterministic data-quality fingerprint plumbing."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from histdatacom.data_quality import (
    DEFAULT_FINGERPRINT_HISTOGRAM_BINS,
    DEFAULT_FINGERPRINT_LAGS,
    DEFAULT_FINGERPRINT_MAX_ROWS,
    DEFAULT_FINGERPRINT_QUANTILES,
    DEFAULT_FINGERPRINT_ROLLING_WINDOWS,
    DEFAULT_FINGERPRINT_ROUNDING_DIGITS,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    HistDataSeriesFingerprintRule,
    QualityFinding,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    quality_target_from_path,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
    series_fingerprint_coverage_summary,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    M1,
    TICK,
    parse_ascii_lines,
    to_polars_frame,
    write_polars_cache,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    CLEAN_M1_ROWS,
    CLEAN_TICK_CASE,
    CLEAN_TICK_ROWS,
    HistDataAsciiCase,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)


def test_fingerprint_group_registers_series_rule_surface() -> None:
    """The advertised fingerprint group should expose its target rule."""
    rules = quality_rules_for_groups(("fingerprint",))

    assert [rule.rule_id for rule in rules] == [SERIES_FINGERPRINT_RULE_ID]
    assert isinstance(rules[0], HistDataSeriesFingerprintRule)
    assert SERIES_FINGERPRINT_RULE_ID in {
        rule.rule_id for rule in quality_rules_for_groups(("all",))
    }
    assert quality_run_rules_for_groups(("fingerprint",)) == ()


def test_fingerprint_rule_emits_m1_csv_payload(tmp_path: Path) -> None:
    """Clean M1 CSV files should produce canonical coverage metadata."""
    target = _discovered_target(write_ascii_case(tmp_path, CLEAN_M1_CASE))
    finding = _fingerprint_finding(target)
    payload = _fingerprint_payload(finding)
    batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)

    assert finding.code == "FINGERPRINT_SERIES_SUMMARY"
    assert finding.severity is QualitySeverity.INFO
    assert payload["schema_version"] == TIME_SERIES_FINGERPRINT_SCHEMA_VERSION
    assert str(payload["fingerprint_id"]).startswith("sha256:")
    assert _mapping(payload["target_axis"]) == {
        "data_format": "ascii",
        "timeframe": "M1",
        "symbol": "EURUSD",
        "period": "201202",
        "kind": "csv",
    }
    assert _mapping(payload["coverage"]) == {
        "row_count": 3,
        "parsed_row_count": 3,
        "start_timestamp_utc_ms": batch.summary.start,
        "end_timestamp_utc_ms": batch.summary.end,
        "duration_ms": 120_000,
    }
    topology = _mapping(payload["temporal_topology"])
    assert topology["row_count"] == 3
    assert topology["parsed_row_count"] == 3
    assert topology["invalid_timestamp_count"] == 0
    assert topology["non_monotonic_count"] == 0
    assert topology["duplicate_timestamp_count"] == 0
    assert topology["min_interval_ms"] == 60_000
    assert topology["median_interval_ms"] == 60_000
    assert topology["max_gap_ms"] == 60_000
    assert topology["suspicious_gap_count"] == 0
    assert topology["expected_session_closure_count"] == 0
    assert topology["weekend_activity_count"] == 0
    assert topology["sampling_basis"] == "observed_sequence"
    assert topology["computed_from"] == "text_scan"
    assert topology["timestamp_projection"] == "text_scan"
    assert topology["cache_source"] is None
    assert _mapping(payload["source"])["kind"] == "csv_text"


def test_fingerprint_rule_emits_tick_csv_payload(tmp_path: Path) -> None:
    """Clean tick CSV files should produce millisecond coverage metadata."""
    target = _discovered_target(write_ascii_case(tmp_path, CLEAN_TICK_CASE))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)

    assert _mapping(payload["target_axis"])["timeframe"] == "T"
    assert _mapping(payload["coverage"]) == {
        "row_count": 3,
        "parsed_row_count": 3,
        "start_timestamp_utc_ms": batch.summary.start,
        "end_timestamp_utc_ms": batch.summary.end,
        "duration_ms": 11_330,
    }
    assert _mapping(payload["source"])["kind"] == "csv_text"


def test_fingerprint_rule_emits_zip_member_payload(tmp_path: Path) -> None:
    """ZIP artifacts should name the member used for coverage."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="HISTDATA_COM_ASCII_EURUSD_M1201202.zip",
    )
    target = _discovered_target(archive)
    payload = _fingerprint_payload(_fingerprint_finding(target))

    assert _mapping(payload["source"]) == {
        "kind": "zip_member",
        "path": "HISTDATA_COM_ASCII_EURUSD_M1201202.zip",
        "member": CLEAN_M1_CASE.filename,
    }
    assert _mapping(payload["coverage"])["row_count"] == 3


def test_fingerprint_rule_prefers_direct_cache_payload(
    tmp_path: Path,
) -> None:
    """Direct cache targets should be fingerprinted without text fallback."""
    cache_path = tmp_path / CACHE_FILENAME
    batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    target = QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    payload = _fingerprint_payload(_fingerprint_finding(target))

    assert _mapping(payload["source"]) == {
        "kind": "cache",
        "cache_source": "direct",
        "path": ".data",
    }
    assert _mapping(payload["coverage"]) == {
        "row_count": 3,
        "parsed_row_count": 3,
        "start_timestamp_utc_ms": batch.summary.start,
        "end_timestamp_utc_ms": batch.summary.end,
        "duration_ms": 120_000,
    }
    topology = _mapping(payload["temporal_topology"])
    assert topology["computed_from"] == "direct_cache"
    assert topology["timestamp_projection"] == "polars_cache"
    assert topology["cache_source"] == "direct"
    assert topology["row_count"] == 3
    assert topology["min_interval_ms"] == 60_000


def test_fingerprint_rule_prefers_fresh_sibling_cache(
    tmp_path: Path,
) -> None:
    """CSV targets should reuse fresh sibling cache data when available."""
    csv_path = write_ascii_case(tmp_path, CLEAN_M1_CASE)
    cache_path = csv_path.with_name(CACHE_FILENAME)
    batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    csv_mtime_ns = csv_path.stat().st_mtime_ns
    os.utime(
        cache_path,
        ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000),
    )
    target = _discovered_target(csv_path)

    payload = _fingerprint_payload(_fingerprint_finding(target))

    assert _mapping(payload["source"]) == {
        "kind": "cache",
        "cache_source": "sibling",
        "path": ".data",
    }
    assert _mapping(payload["coverage"])["row_count"] == 3
    topology = _mapping(payload["temporal_topology"])
    assert topology["computed_from"] == "fresh_sibling_cache"
    assert topology["timestamp_projection"] == "polars_cache"
    assert topology["cache_source"] == "sibling"


def test_fingerprint_temporal_topology_reports_m1_duplicate_timestamp(
    tmp_path: Path,
) -> None:
    """M1 duplicate timestamps should be descriptive fingerprint metadata."""
    target = _discovered_target(
        write_ascii_case(tmp_path, case_by_name("m1_duplicate_timestamp"))
    )
    payload = _fingerprint_payload(_fingerprint_finding(target))
    topology = _mapping(payload["temporal_topology"])

    assert topology["duplicate_timestamp_count"] == 1
    assert topology["m1_duplicate_timestamp_count"] == 1
    assert topology["tick_duplicate_row_count"] == 0
    assert _mapping(topology["duplicate_timestamp_source_counts"]) == {
        "m1_duplicate_timestamp": 1,
        "tick_duplicate_row": 0,
    }
    assert topology["min_interval_ms"] == 0


def test_fingerprint_temporal_topology_reports_tick_duplicate_row(
    tmp_path: Path,
) -> None:
    """Tick duplicate rows should be counted separately from M1 duplicates."""
    target = _discovered_target(
        write_ascii_case(tmp_path, case_by_name("tick_duplicate_row"))
    )
    payload = _fingerprint_payload(_fingerprint_finding(target))
    topology = _mapping(payload["temporal_topology"])

    assert topology["duplicate_timestamp_count"] == 1
    assert topology["m1_duplicate_timestamp_count"] == 0
    assert topology["tick_duplicate_row_count"] == 1
    assert _mapping(topology["duplicate_timestamp_source_counts"]) == {
        "m1_duplicate_timestamp": 0,
        "tick_duplicate_row": 1,
    }
    assert topology["min_interval_ms"] == 0


def test_fingerprint_temporal_topology_reports_expected_weekend_closure(
    tmp_path: Path,
) -> None:
    """Expected FX weekend closures should be topology, not defects."""
    case = HistDataAsciiCase(
        name="m1_expected_weekend_closure",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_WEEKEND.csv",
        rows=(
            "20120203 170000;1.306600;1.306600;1.306560;1.306560;0",
            "20120205 170000;1.306570;1.306570;1.306470;1.306560;17",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    topology = _mapping(payload["temporal_topology"])

    assert topology["expected_session_closure_count"] == 1
    assert topology["suspicious_gap_count"] == 0
    assert topology["max_gap_ms"] == 172_800_000
    assert _mapping(topology["gap_bucket_counts"])["gt_1d"] == 1


def test_fingerprint_temporal_topology_reports_suspicious_gap(
    tmp_path: Path,
) -> None:
    """Unexpected large gaps should be summarized without changing severity."""
    case = HistDataAsciiCase(
        name="m1_suspicious_gap",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_GAP.csv",
        rows=(
            "20120201 000000;1.306600;1.306600;1.306560;1.306560;0",
            "20120201 001000;1.306570;1.306570;1.306470;1.306560;17",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    finding = _fingerprint_finding(target)
    payload = _fingerprint_payload(finding)
    topology = _mapping(payload["temporal_topology"])

    assert finding.severity is QualitySeverity.INFO
    assert topology["suspicious_gap_count"] == 1
    assert topology["expected_session_closure_count"] == 0
    assert topology["max_gap_ms"] == 600_000
    assert _mapping(topology["gap_bucket_counts"])["gt_1m"] == 1
    assert _mapping(topology["gap_bucket_counts"])["gt_5m"] == 1


def test_fingerprint_rule_reports_unsupported_target(
    tmp_path: Path,
) -> None:
    """Unsupported targets should emit bounded source metadata, not crash."""
    path = tmp_path / "DAT_ASCII_EURUSD_M1_201202.xlsx"
    path.write_text("not a supported fingerprint source", encoding="utf-8")
    target = QualityTarget(
        path=str(path),
        kind=QualityTargetKind.SPREADSHEET,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    finding = _fingerprint_finding(target)
    payload = _fingerprint_payload(finding)

    assert finding.code == "FINGERPRINT_SOURCE_UNAVAILABLE"
    assert _mapping(payload["coverage"]) == {
        "row_count": 0,
        "parsed_row_count": None,
        "start_timestamp_utc_ms": None,
        "end_timestamp_utc_ms": None,
        "duration_ms": None,
    }
    assert _mapping(payload["source"])["reason"] == "unsupported_target_kind"


def test_series_fingerprint_coverage_summary_counts_mixed_sources(
    tmp_path: Path,
) -> None:
    """Run metadata should summarize fingerprint coverage without path data."""
    csv_target = _discovered_target(
        write_ascii_case(tmp_path / "csv", CLEAN_M1_CASE)
    )
    archive_target = _discovered_target(
        write_zip_case(
            tmp_path / "zip",
            CLEAN_M1_CASE,
            zip_filename="HISTDATA_COM_ASCII_GBPUSD_M1201202.zip",
        )
    )
    direct_cache_path = tmp_path / "direct-cache" / CACHE_FILENAME
    direct_cache_path.parent.mkdir(parents=True, exist_ok=True)
    tick_batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)
    write_polars_cache(to_polars_frame(tick_batch), direct_cache_path)
    direct_cache_target = QualityTarget(
        path=str(direct_cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    sibling_csv_path = write_ascii_case(
        tmp_path / "sibling-cache",
        CLEAN_M1_CASE,
    )
    sibling_cache_path = sibling_csv_path.with_name(CACHE_FILENAME)
    m1_batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)
    write_polars_cache(to_polars_frame(m1_batch), sibling_cache_path)
    csv_mtime_ns = sibling_csv_path.stat().st_mtime_ns
    os.utime(
        sibling_cache_path,
        ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000),
    )
    sibling_cache_target = _discovered_target(sibling_csv_path)
    unsupported_target = QualityTarget(
        path=str(tmp_path / "unsupported.xlsx"),
        kind=QualityTargetKind.SPREADSHEET,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    report = run_quality_assessment(
        (
            csv_target,
            archive_target,
            direct_cache_target,
            sibling_cache_target,
            unsupported_target,
        ),
        quality_rules_for_groups(("fingerprint",)),
    )

    summary = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY]
    )

    assert summary == series_fingerprint_coverage_summary(report.findings)
    assert summary["schema_version"] == (
        TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION
    )
    assert summary["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    assert summary["discovered_target_count"] == 5
    assert summary["evaluated_fingerprint_target_count"] == 5
    assert summary["fingerprint_target_count"] == 5
    assert summary["skipped_fingerprint_target_count"] == 0
    assert summary["supported_readable_count"] == 4
    assert summary["unavailable_count"] == 1
    assert summary["parsed_non_empty_coverage_count"] == 4
    assert summary["skipped_reason_counts"] == {}
    assert summary["source_kind_counts"] == {
        "cache": 2,
        "csv_text": 1,
        "unavailable": 1,
        "zip_member": 1,
    }
    assert summary["cache_source_counts"] == {
        "direct": 1,
        "sibling": 1,
    }
    assert summary["unavailable_reason_counts"] == {
        "unsupported_target_kind": 1,
    }
    assert summary["target_kind_counts"] == {
        "cache": 1,
        "csv": 2,
        "spreadsheet": 1,
        "zip": 1,
    }
    assert summary["timeframe_counts"] == {"M1": 4, "T": 1}
    assert json_safe_path_strings(summary)


def test_fingerprint_coverage_summary_reports_duplicate_archive_skip(
    tmp_path: Path,
) -> None:
    """Skipped duplicate ZIP fingerprint targets should be visible."""
    csv_target = _discovered_target(write_ascii_case(tmp_path, CLEAN_M1_CASE))
    archive_target = _discovered_target(
        write_zip_case(
            tmp_path,
            CLEAN_M1_CASE,
            zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
        )
    )

    report = run_quality_assessment(
        (archive_target, csv_target),
        quality_rules_for_groups(("fingerprint",)),
    )

    summary = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY]
    )

    assert [result.target.kind for result in report.rule_results] == [
        QualityTargetKind.CSV,
    ]
    assert summary["discovered_target_count"] == 2
    assert summary["evaluated_fingerprint_target_count"] == 1
    assert summary["fingerprint_target_count"] == 1
    assert summary["skipped_fingerprint_target_count"] == 1
    assert summary["skipped_reason_counts"] == {
        "duplicate_archive_preferred_csv": 1,
    }
    assert summary["source_kind_counts"] == {"csv_text": 1}
    assert summary["target_kind_counts"] == {"csv": 1}
    assert report.metadata["quality_engine"] == {
        "target_count": 2,
        "rule_count": 1,
        "target_rule_evaluation_count": 1,
        "skipped_duplicate_archive_rule_evaluation_count": 1,
        "duplicate_archive_scan_policy": (
            "prefer_extracted_csv_for_non_inventory_rules"
        ),
    }


def test_fingerprint_id_excludes_source_path_volatility(
    tmp_path: Path,
) -> None:
    """Identical content and target axis should hash the same across paths."""
    first = write_ascii_case(
        tmp_path / "first",
        HistDataAsciiCase(
            name="first_copy",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_FIRST.csv",
            rows=CLEAN_M1_ROWS,
        ),
    )
    second = write_ascii_case(
        tmp_path / "second",
        HistDataAsciiCase(
            name="second_copy",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_SECOND.csv",
            rows=CLEAN_M1_ROWS,
        ),
    )

    first_payload = _fingerprint_payload(
        _fingerprint_finding(_discovered_target(first))
    )
    second_payload = _fingerprint_payload(
        _fingerprint_finding(_discovered_target(second))
    )

    assert (
        _mapping(first_payload["source"])["path"]
        != _mapping(second_payload["source"])["path"]
    )
    assert first_payload["fingerprint_id"] == second_payload["fingerprint_id"]


def test_fingerprint_constants_are_stable() -> None:
    """The first schema surface should expose stable public identifiers."""
    assert (
        TIME_SERIES_FINGERPRINT_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint.v1"
    )
    assert TIME_SERIES_FINGERPRINT_METADATA_KEY == "time_series_fingerprint"
    assert DEFAULT_FINGERPRINT_QUANTILES == (
        0.01,
        0.05,
        0.25,
        0.5,
        0.75,
        0.95,
        0.99,
    )
    assert DEFAULT_FINGERPRINT_LAGS == (1, 2, 3, 5, 10, 30, 60, 240, 1440)
    assert DEFAULT_FINGERPRINT_ROLLING_WINDOWS == (60, 240, 1440)
    assert DEFAULT_FINGERPRINT_HISTOGRAM_BINS == 32
    assert DEFAULT_FINGERPRINT_MAX_ROWS == 1_000_000
    assert DEFAULT_FINGERPRINT_ROUNDING_DIGITS == 12


def _discovered_target(path: Path) -> QualityTarget:
    target = quality_target_from_path(path)
    assert target is not None
    return target


def _fingerprint_finding(target: QualityTarget) -> QualityFinding:
    rule = HistDataSeriesFingerprintRule()
    findings = tuple(rule.evaluate(target))
    assert len(findings) == 1
    finding = findings[0]
    assert finding.rule_id == SERIES_FINGERPRINT_RULE_ID
    assert finding.location.column == TIME_SERIES_FINGERPRINT_METADATA_KEY
    return finding


def _fingerprint_payload(
    finding: QualityFinding,
) -> dict[str, Any]:
    payload = finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    assert isinstance(payload, dict)
    return payload


def _mapping(value: Any) -> dict[str, Any]:
    assert isinstance(value, dict)
    return value


def json_safe_path_strings(value: Any) -> bool:
    encoded = str(value)
    return "/Users/" not in encoded and str(Path.home()) not in encoded

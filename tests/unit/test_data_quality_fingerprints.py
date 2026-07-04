"""Tests for deterministic data-quality fingerprint plumbing."""

from __future__ import annotations

import os
from collections.abc import Mapping
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
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
    HistDataFingerprintProfile,
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
    series_fingerprint_topology_attention_summary,
    series_fingerprint_topology_summary,
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
    distribution = _mapping(payload["m1_bar_distribution"])
    assert distribution["row_count"] == 3
    assert distribution["sampled_row_count"] == 3
    assert distribution["usable_row_count"] == 3
    assert distribution["invalid_row_count"] == 0
    assert distribution["truncated"] is False
    prices = _mapping(distribution["price"])
    open_summary = _mapping(prices["open"])
    assert open_summary["count"] == 3
    assert open_summary["min"] == 1.30652
    assert open_summary["max"] == 1.3066
    assert open_summary["mean"] == 1.306563333333
    assert open_summary["median"] == 1.30657
    assert _mapping(open_summary["quantiles"])["0.5"] == 1.30657
    shape = _mapping(distribution["ohlc_shape"])
    body_summary = _mapping(shape["body_ratio"])
    assert body_summary["count"] == 3
    assert body_summary["min"] == 0.1
    assert body_summary["median"] == 1.0
    range_ratio = _mapping(distribution["range_ratio"])
    assert range_ratio["count"] == 3
    assert range_ratio["median"] == 0.000030615213
    assert _mapping(distribution["precision"])["precision_source"] == "text"
    assert _mapping(distribution["precision"])["decimal_place_counts"] == {
        "6": 12,
    }
    assert _mapping(
        _mapping(distribution["precision"])["column_decimal_place_counts"]
    )["open"] == {"6": 3}
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
    distribution = _mapping(payload["tick_distribution"])
    assert distribution["row_count"] == 3
    assert distribution["sampled_row_count"] == 3
    assert distribution["usable_row_count"] == 3
    assert distribution["invalid_row_count"] == 0
    assert distribution["truncated"] is False
    spread_summary = _mapping(distribution["spread"])
    assert spread_summary["count"] == 3
    assert spread_summary["min"] == 0.00017
    assert spread_summary["max"] == 0.00017
    assert spread_summary["median"] == 0.00017
    assert _mapping(spread_summary["quantiles"])["0.5"] == 0.00017
    assert distribution["zero_spread_count"] == 0
    assert distribution["negative_spread_count"] == 0
    assert distribution["zero_spread_rate"] == 0.0
    assert distribution["negative_spread_rate"] == 0.0
    assert _mapping(payload["source"])["kind"] == "csv_text"


def test_fingerprint_tick_distribution_counts_zero_and_negative_spreads(
    tmp_path: Path,
) -> None:
    """Tick fingerprints should expose spread-defect rates descriptively."""
    case = HistDataAsciiCase(
        name="tick_spread_mix",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_SPREAD_MIX.csv",
        rows=(
            "20120201 000003660,1.000000,1.000000,0",
            "20120201 000003973,1.000200,1.000100,0",
            "20120201 000004990,1.000000,1.000300,0",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    distribution = _mapping(payload["tick_distribution"])

    assert distribution["usable_row_count"] == 3
    assert distribution["zero_spread_count"] == 1
    assert distribution["negative_spread_count"] == 1
    assert distribution["zero_spread_rate"] == 0.333333333333
    assert distribution["negative_spread_rate"] == 0.333333333333
    spread_summary = _mapping(distribution["spread"])
    assert spread_summary["min"] == -0.0001
    assert spread_summary["max"] == 0.0003


def test_fingerprint_distribution_handles_invalid_partial_and_empty_m1_rows(
    tmp_path: Path,
) -> None:
    """Distribution summaries should be bounded even for sparse bad input."""
    invalid_target = _discovered_target(
        write_ascii_case(tmp_path / "invalid", case_by_name("m1_bad_numeric"))
    )
    invalid_payload = _fingerprint_payload(_fingerprint_finding(invalid_target))
    invalid_distribution = _mapping(invalid_payload["m1_bar_distribution"])

    assert invalid_distribution["row_count"] == 2
    assert invalid_distribution["sampled_row_count"] == 1
    assert invalid_distribution["usable_row_count"] == 1
    assert invalid_distribution["invalid_row_count"] == 1
    close_summary = _mapping(_mapping(invalid_distribution["price"])["close"])
    assert close_summary["count"] == 1
    assert close_summary["median"] == 1.30656

    partial_target = _discovered_target(
        write_ascii_case(tmp_path / "partial", case_by_name("m1_malformed_row"))
    )
    partial_payload = _fingerprint_payload(_fingerprint_finding(partial_target))
    partial_distribution = _mapping(partial_payload["m1_bar_distribution"])

    assert partial_distribution["row_count"] == 2
    assert partial_distribution["sampled_row_count"] == 1
    assert partial_distribution["usable_row_count"] == 1
    assert partial_distribution["invalid_row_count"] == 1

    empty_target = _discovered_target(
        write_ascii_case(tmp_path / "empty", case_by_name("m1_empty_file"))
    )
    empty_payload = _fingerprint_payload(_fingerprint_finding(empty_target))
    empty_distribution = _mapping(empty_payload["m1_bar_distribution"])
    empty_summary = _mapping(_mapping(empty_distribution["price"])["open"])

    assert empty_distribution["row_count"] == 0
    assert empty_distribution["sampled_row_count"] == 0
    assert empty_distribution["usable_row_count"] == 0
    assert empty_distribution["invalid_row_count"] == 0
    assert empty_summary["count"] == 0
    assert empty_summary["median"] is None


def test_fingerprint_distribution_uses_profile_quantiles_and_rounding(
    tmp_path: Path,
) -> None:
    """Fingerprint profile knobs should shape distribution payloads."""
    target = _discovered_target(write_ascii_case(tmp_path, CLEAN_M1_CASE))
    profile = HistDataFingerprintProfile(
        quantiles=(0.0, 0.5, 1.0),
        max_rows=2,
        rounding_digits=5,
    )
    payload = _fingerprint_payload(_fingerprint_finding(target, profile))
    distribution = _mapping(payload["m1_bar_distribution"])
    open_summary = _mapping(_mapping(distribution["price"])["open"])

    assert distribution["row_count"] == 3
    assert distribution["sampled_row_count"] == 2
    assert distribution["usable_row_count"] == 3
    assert distribution["truncated"] is True
    assert open_summary["mean"] == 1.30658
    assert _mapping(open_summary["quantiles"]) == {
        "0.0": 1.30657,
        "0.5": 1.30658,
        "1.0": 1.3066,
    }


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
    distribution = _mapping(payload["m1_bar_distribution"])
    assert distribution["row_count"] == 3
    assert _mapping(_mapping(distribution["price"])["close"])["count"] == 3
    assert _mapping(distribution["precision"])["precision_source"] == (
        "cache_float"
    )
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
    assert _mapping(payload["m1_bar_distribution"])["row_count"] == 3
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


def test_series_fingerprint_topology_summary_reports_actionable_targets(
    tmp_path: Path,
) -> None:
    """Run metadata should summarize topology without requiring JSON spelunking."""
    clean_target = _discovered_target(
        write_ascii_case(tmp_path / "clean", CLEAN_M1_CASE)
    )
    duplicate_target = _discovered_target(
        write_ascii_case(
            tmp_path / "duplicate",
            case_by_name("m1_duplicate_timestamp"),
        )
    )
    suspicious_target = _discovered_target(
        write_ascii_case(tmp_path / "gap", _suspicious_gap_case())
    )
    invalid_target = _discovered_target(
        write_ascii_case(
            tmp_path / "invalid",
            case_by_name("m1_bad_timestamp"),
        )
    )
    non_monotonic_target = _discovered_target(
        write_ascii_case(
            tmp_path / "non-monotonic",
            case_by_name("m1_non_monotonic_timestamp"),
        )
    )
    weekend_activity_target = _discovered_target(
        write_ascii_case(
            tmp_path / "weekend-activity",
            _weekend_activity_case(),
        )
    )
    expected_closure_target = _discovered_target(
        write_ascii_case(
            tmp_path / "expected-closure",
            _expected_weekend_closure_case(),
        )
    )
    cache_path = tmp_path / "direct-cache" / CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    m1_batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)
    write_polars_cache(to_polars_frame(m1_batch), cache_path)
    cache_target = QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
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
            clean_target,
            duplicate_target,
            suspicious_target,
            invalid_target,
            non_monotonic_target,
            weekend_activity_target,
            expected_closure_target,
            cache_target,
            unsupported_target,
        ),
        quality_rules_for_groups(("fingerprint",)),
    )

    summary = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY]
    )

    assert summary == series_fingerprint_topology_summary(report.findings)
    assert summary["schema_version"] == (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION
    )
    assert summary["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    assert summary["target_count"] == 9
    assert summary["included_target_count"] == 9
    assert summary["omitted_target_count"] == 0
    assert summary["truncated"] is False
    assert summary["status_counts"] == {
        "irregular": 5,
        "regular": 3,
        "unavailable": 1,
    }
    assert summary["computed_from_counts"] == {
        "direct_cache": 1,
        "text_scan": 7,
        "unavailable": 1,
    }
    assert summary["cache_source_counts"] == {"direct": 1}
    assert summary["sampling_basis_counts"] == {
        "observed_sequence": 8,
        "unavailable": 1,
    }
    assert summary["flag_counts"] == {
        "cache_backed": 1,
        "duplicate_timestamps": 1,
        "expected_session_closures": 1,
        "invalid_timestamps": 1,
        "non_monotonic_timestamps": 1,
        "suspicious_gaps": 1,
        "unavailable_topology": 1,
        "weekend_activity": 1,
    }

    targets = _list(summary["target_summaries"])
    clean = _target_summary_with_status(
        targets,
        "regular",
        excluded_flags=("cache_backed", "expected_session_closures"),
    )
    assert clean["row_count"] == 3
    assert clean["parsed_row_count"] == 3
    assert clean["duplicate_timestamp_count"] == 0
    assert clean["non_monotonic_count"] == 0
    assert clean["median_interval_ms"] == 60_000
    assert clean["max_gap_ms"] == 60_000
    assert clean["suspicious_gap_count"] == 0
    assert clean["expected_session_closure_count"] == 0
    assert clean["weekend_activity_count"] == 0
    assert clean["sampling_basis"] == "observed_sequence"
    assert clean["computed_from"] == "text_scan"

    duplicate = _target_summary_with_flag(targets, "duplicate_timestamps")
    assert duplicate["status"] == "irregular"
    assert duplicate["duplicate_timestamp_count"] == 1

    suspicious = _target_summary_with_flag(targets, "suspicious_gaps")
    assert suspicious["status"] == "irregular"
    assert suspicious["suspicious_gap_count"] == 1
    assert suspicious["max_gap_ms"] == 600_000

    invalid = _target_summary_with_flag(targets, "invalid_timestamps")
    assert invalid["status"] == "irregular"
    assert invalid["invalid_timestamp_count"] == 1

    non_monotonic = _target_summary_with_flag(
        targets,
        "non_monotonic_timestamps",
    )
    assert non_monotonic["status"] == "irregular"
    assert non_monotonic["non_monotonic_count"] == 1

    weekend = _target_summary_with_flag(targets, "weekend_activity")
    assert weekend["status"] == "irregular"
    assert weekend["weekend_activity_count"] == 1

    expected = _target_summary_with_flag(
        targets,
        "expected_session_closures",
    )
    assert expected["status"] == "regular"
    assert expected["expected_session_closure_count"] == 1
    assert expected["max_gap_ms"] == 172_800_000

    cache = _target_summary_with_flag(targets, "cache_backed")
    assert cache["status"] == "regular"
    assert cache["computed_from"] == "direct_cache"
    assert cache["cache_source"] == "direct"

    attention = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY]
    )
    assert attention == series_fingerprint_topology_attention_summary(
        report.findings
    )
    assert attention["schema_version"] == (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION
    )
    assert attention["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    assert attention["topology_target_count"] == 9
    assert attention["attention_target_count"] == 6
    assert attention["included_attention_target_count"] == 6
    assert attention["omitted_attention_target_count"] == 0
    assert attention["truncated"] is False
    assert attention["attention_level_counts"] == {
        "sequence": 2,
        "session": 1,
        "structural": 2,
        "unavailable": 1,
    }
    assert attention["attention_flag_counts"] == {
        "duplicate_timestamps": 1,
        "invalid_timestamps": 1,
        "non_monotonic_timestamps": 1,
        "suspicious_gaps": 1,
        "unavailable_topology": 1,
        "weekend_activity": 1,
    }
    attention_targets = _list(attention["target_summaries"])
    assert [
        _mapping(target)["attention_level"] for target in attention_targets
    ] == [
        "unavailable",
        "structural",
        "structural",
        "sequence",
        "sequence",
        "session",
    ]
    unavailable_attention = _target_summary_with_flag(
        attention_targets,
        "unavailable_topology",
    )
    assert unavailable_attention["status"] == "unavailable"
    assert _remediation_hint_codes(unavailable_attention) == (
        "verify_fingerprint_source",
    )
    invalid_attention = _target_summary_with_flag(
        attention_targets,
        "invalid_timestamps",
    )
    assert invalid_attention["invalid_timestamp_count"] == 1
    assert _remediation_hint_codes(invalid_attention) == (
        "inspect_invalid_timestamp_rows",
    )
    non_monotonic_attention = _target_summary_with_flag(
        attention_targets,
        "non_monotonic_timestamps",
    )
    assert non_monotonic_attention["non_monotonic_count"] == 1
    assert _remediation_hint_codes(non_monotonic_attention) == (
        "repair_timestamp_order",
    )
    duplicate_attention = _target_summary_with_flag(
        attention_targets,
        "duplicate_timestamps",
    )
    assert duplicate_attention["duplicate_timestamp_count"] == 1
    assert _remediation_hint_codes(duplicate_attention) == (
        "inspect_duplicate_timestamp_rows",
    )
    suspicious_attention = _target_summary_with_flag(
        attention_targets,
        "suspicious_gaps",
    )
    assert suspicious_attention["suspicious_gap_count"] == 1
    assert _remediation_hint_codes(suspicious_attention) == (
        "inspect_gap_boundaries",
    )
    weekend_attention = _target_summary_with_flag(
        attention_targets,
        "weekend_activity",
    )
    assert weekend_attention["weekend_activity_count"] == 1
    assert _remediation_hint_codes(weekend_attention) == (
        "verify_weekend_session_policy",
    )
    assert not any(
        "expected_session_closures" in _list(_mapping(target)["flags"])
        for target in attention_targets
    )
    assert not any(
        "cache_backed" in _list(_mapping(target)["flags"])
        for target in attention_targets
    )
    assert json_safe_path_strings(attention)
    assert json_safe_path_strings(summary)


def test_series_fingerprint_topology_attention_orders_mixed_remediation_hints(
    tmp_path: Path,
) -> None:
    """Mixed flags should carry stable hint codes in attention-flag order."""
    target = QualityTarget(
        path=str(tmp_path / "DAT_ASCII_EURUSD_M1_201202_MIXED.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "target_axis": {
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "symbol": "EURUSD",
                    "period": "201202",
                    "kind": "csv",
                },
                "temporal_topology": {
                    "row_count": 4,
                    "parsed_row_count": 4,
                    "invalid_timestamp_count": 1,
                    "duplicate_timestamp_count": 1,
                    "non_monotonic_count": 1,
                    "median_interval_ms": 60_000,
                    "max_gap_ms": 600_000,
                    "suspicious_gap_count": 1,
                    "expected_session_closure_count": 1,
                    "weekend_activity_count": 1,
                    "sampling_basis": "observed_sequence",
                    "computed_from": "text_scan",
                    "cache_source": None,
                },
            }
        },
    )

    attention = _mapping(
        series_fingerprint_topology_attention_summary((finding,))
    )
    target_summary = _mapping(_list(attention["target_summaries"])[0])

    assert target_summary["attention_flags"] == [
        "invalid_timestamps",
        "non_monotonic_timestamps",
        "duplicate_timestamps",
        "suspicious_gaps",
        "weekend_activity",
    ]
    assert _remediation_hint_codes(target_summary) == (
        "inspect_invalid_timestamp_rows",
        "repair_timestamp_order",
        "inspect_duplicate_timestamp_rows",
        "inspect_gap_boundaries",
        "verify_weekend_session_policy",
    )
    assert "expected_session_closures" in _list(target_summary["flags"])


def test_series_fingerprint_topology_attention_ignores_context_only_targets(
    tmp_path: Path,
) -> None:
    """Expected closures alone should not create attention targets."""
    clean_target = _discovered_target(
        write_ascii_case(tmp_path / "clean", CLEAN_M1_CASE)
    )
    expected_closure_target = _discovered_target(
        write_ascii_case(
            tmp_path / "expected-closure",
            _expected_weekend_closure_case(),
        )
    )
    report = run_quality_assessment(
        (clean_target, expected_closure_target),
        quality_rules_for_groups(("fingerprint",)),
    )

    attention = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY]
    )

    assert attention["topology_target_count"] == 2
    assert attention["attention_target_count"] == 0
    assert attention["included_attention_target_count"] == 0
    assert attention["omitted_attention_target_count"] == 0
    assert attention["attention_level_counts"] == {}
    assert attention["attention_flag_counts"] == {}
    assert attention["target_summaries"] == []


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
    assert (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-topology-summary.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY
        == "time_series_fingerprint_topology_summary"
    )
    assert (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-topology-attention.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY
        == "time_series_fingerprint_topology_attention"
    )
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


def _fingerprint_finding(
    target: QualityTarget,
    profile: HistDataFingerprintProfile | None = None,
) -> QualityFinding:
    rule = HistDataSeriesFingerprintRule(
        profile=profile or HistDataFingerprintProfile()
    )
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


def _list(value: Any) -> list[Any]:
    assert isinstance(value, list)
    return value


def _target_summary_with_flag(
    targets: list[Any],
    flag: str,
) -> dict[str, Any]:
    matches = [
        _mapping(target)
        for target in targets
        if flag in _list(_mapping(target)["flags"])
    ]
    assert len(matches) == 1
    return matches[0]


def _target_summary_with_status(
    targets: list[Any],
    status: str,
    *,
    excluded_flags: tuple[str, ...],
) -> dict[str, Any]:
    for target in targets:
        item = _mapping(target)
        flags = set(_list(item["flags"]))
        if item["status"] == status and flags.isdisjoint(excluded_flags):
            return item
    raise AssertionError(f"missing topology target status {status!r}")


def _expected_weekend_closure_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="m1_expected_weekend_closure",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_WEEKEND.csv",
        rows=(
            "20120203 170000;1.306600;1.306600;1.306560;1.306560;0",
            "20120205 170000;1.306570;1.306570;1.306470;1.306560;17",
        ),
    )


def _suspicious_gap_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="m1_suspicious_gap",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_GAP.csv",
        rows=(
            "20120201 000000;1.306600;1.306600;1.306560;1.306560;0",
            "20120201 001000;1.306570;1.306570;1.306470;1.306560;17",
        ),
    )


def _weekend_activity_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="m1_weekend_activity",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_WEEKEND_ACTIVITY.csv",
        rows=("20120204 120000;1.306600;1.306600;1.306560;1.306560;0",),
    )


def _remediation_hint_codes(target: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(_mapping(hint)["code"])
        for hint in _list(_mapping(target)["remediation_hints"])
    )


def json_safe_path_strings(value: Any) -> bool:
    encoded = str(value)
    return "/Users/" not in encoded and str(Path.home()) not in encoded

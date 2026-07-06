"""Tests for deterministic data-quality fingerprint plumbing."""

from __future__ import annotations

import math
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
    QUALITY_PROFILE_SCHEMA_VERSION,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
    HistDataFingerprintDistributionAttentionProfile,
    HistDataFingerprintProfile,
    HistDataSeriesFingerprintRule,
    QualityFinding,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    discover_quality_targets,
    quality_target_from_path,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
    series_fingerprint_coverage_summary,
    series_fingerprint_distribution_attention_summary,
    series_fingerprint_distribution_summary,
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
    audit = _mapping(payload["fingerprint_audit"])
    assert (
        audit["schema_version"] == TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION
    )
    assert _list(audit["sections_expected"]) == [
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "m1_bar_distribution",
        "return_dynamics",
        "dependence",
    ]
    assert _list(audit["sections_emitted"]) == [
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "m1_bar_distribution",
        "return_dynamics",
        "dependence",
    ]
    assert _mapping(audit["sections_skipped"]) == {}
    statuses = _mapping(audit["section_statuses"])
    assert statuses["coverage"] == "valid"
    assert statuses["temporal_topology"] == "valid"
    assert statuses["calendar_regimes"] == "valid"
    assert statuses["m1_bar_distribution"] == "valid"
    assert statuses["return_dynamics"] == "valid"
    assert statuses["dependence"] == "limited"
    eligibility = _mapping(
        _mapping(audit["conditional_distribution_eligibility"])["tick_spread"]
    )
    assert eligibility == {
        "eligible": False,
        "status": "ineligible",
        "reason": "unsupported_timeframe",
    }
    completeness = _mapping(audit["profile_completeness"])
    assert completeness["source"] == "calendar_regimes"
    assert completeness["calendar_profile_complete"] is False
    assert completeness["missing_optional_calendar_data"] is True
    readiness = _mapping(audit["dynamics_readiness"])
    assert _mapping(readiness["return_dynamics"])["status"] == "valid"
    assert _mapping(readiness["return_dynamics"])["row_order"] == (
        "source_text_order"
    )
    assert _mapping(readiness["return_dynamics"])["limitations"] == []
    assert _mapping(readiness["microstructure_dynamics"]) == {
        "status": "skipped",
        "reason": "unsupported_timeframe",
    }


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
    audit = _mapping(payload["fingerprint_audit"])
    assert _list(audit["sections_expected"]) == [
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "tick_distribution",
        "conditional_distributions",
        "microstructure_dynamics",
        "dependence",
    ]
    assert _mapping(audit["sections_skipped"]) == {}
    statuses = _mapping(audit["section_statuses"])
    assert statuses["tick_distribution"] == "valid"
    assert statuses["conditional_distributions"] == "valid"
    assert statuses["microstructure_dynamics"] == "valid"
    assert statuses["dependence"] == "limited"
    eligibility = _mapping(
        _mapping(audit["conditional_distribution_eligibility"])["tick_spread"]
    )
    assert eligibility == {
        "eligible": True,
        "status": "eligible",
        "metric": "tick_spread",
        "grouped_by": ["active_session", "special_tag"],
        "emitted": True,
    }


def test_fingerprint_m1_return_dynamics_describe_sequence(
    tmp_path: Path,
) -> None:
    """M1 fingerprints should expose deterministic return dynamics."""
    case = HistDataAsciiCase(
        name="m1-dynamics",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202.csv",
        rows=(
            "20120201 000000;1.000000;1.000000;1.000000;1.000000;0",
            "20120201 000100;1.000000;1.000000;1.000000;1.000000;0",
            "20120201 000200;1.000000;1.000000;1.000000;1.000000;0",
            "20120201 000300;1.100000;1.200000;1.000000;1.100000;0",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    dynamics = _mapping(payload["return_dynamics"])

    assert dynamics["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION
    )
    assert dynamics["basis"] == "observed_sequence"
    assert dynamics["row_order"] == "source_text_order"
    assert dynamics["computed_from"] == "text_scan"
    assert dynamics["regular_grid"] is False
    assert dynamics["sequence_status"] == "ok"
    assert _list(dynamics["limitations"]) == []
    assert dynamics["row_count"] == 4
    assert dynamics["sampled_row_count"] == 4
    assert dynamics["usable_row_count"] == 4
    assert dynamics["invalid_row_count"] == 0
    assert dynamics["partial_row_count"] == 0
    assert dynamics["truncated"] is False

    close_returns = _mapping(dynamics["close_log_return"])
    assert close_returns["count"] == 3
    assert close_returns["median"] == 0.0
    assert close_returns["max"] == round(math.log(1.1), 12)
    absolute_returns = _mapping(dynamics["absolute_return"])
    assert absolute_returns["count"] == 3
    assert absolute_returns["max"] == round(math.log(1.1), 12)
    squared_returns = _mapping(dynamics["squared_return"])
    assert squared_returns["count"] == 3
    assert squared_returns["max"] == round(math.log(1.1) ** 2, 12)
    open_jump = _mapping(dynamics["open_jump"])
    assert open_jump["count"] == 3
    assert open_jump["max"] == 0.1

    flatline = _mapping(dynamics["flatline"])
    assert flatline["zero_return_count"] == 2
    assert flatline["zero_return_rate"] == 0.666666666667
    assert flatline["zero_return_run_count"] == 1
    assert _mapping(flatline["zero_return_run_length_counts"]) == {"3": 1}
    assert flatline["ohlc_flatline_row_count"] == 3
    assert flatline["ohlc_flatline_rate"] == 0.75
    assert flatline["ohlc_flatline_run_count"] == 1
    assert flatline["ohlc_flatline_affected_row_count"] == 3
    assert _mapping(flatline["ohlc_flatline_run_length_counts"]) == {"3": 1}


def test_fingerprint_tick_microstructure_dynamics_describe_sequence(
    tmp_path: Path,
) -> None:
    """Tick fingerprints should expose interarrival and quote dynamics."""
    case = HistDataAsciiCase(
        name="tick-dynamics",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202.csv",
        rows=(
            "20120201 000000000,1.000000,1.000200,0",
            "20120201 000000050,1.000000,1.000200,0",
            "20120201 000000090,1.000000,1.000200,0",
            "20120201 000000300,1.000100,1.000200,0",
            "20120201 000000500,1.000200,1.000200,0",
            "20120201 000001000,1.000200,1.001000,0",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    dynamics = _mapping(payload["microstructure_dynamics"])

    assert dynamics["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION
    )
    assert dynamics["basis"] == "observed_sequence"
    assert dynamics["row_order"] == "source_text_order"
    assert dynamics["computed_from"] == "text_scan"
    assert dynamics["sequence_status"] == "ok"
    assert dynamics["row_count"] == 6
    assert dynamics["sampled_row_count"] == 6
    assert dynamics["usable_row_count"] == 6

    interarrival = _mapping(dynamics["interarrival_ms"])
    assert interarrival["count"] == 5
    assert interarrival["min"] == 40.0
    assert interarrival["median"] == 200.0
    assert interarrival["max"] == 500.0
    spread_change = _mapping(dynamics["spread_change"])
    assert spread_change["count"] == 5
    assert spread_change["min"] == -0.0001
    assert spread_change["max"] == 0.0008

    spread_jump = _mapping(dynamics["spread_jump"])
    assert spread_jump["threshold"] == 0.0004
    assert spread_jump["count"] == 1
    assert spread_jump["rate"] == 0.2
    assert dynamics["zero_spread_count"] == 1
    assert dynamics["negative_spread_count"] == 0

    stale_quote = _mapping(dynamics["stale_quote"])
    assert stale_quote["repeat_count"] == 2
    assert stale_quote["repeat_rate"] == 0.4
    assert stale_quote["run_count"] == 1
    assert stale_quote["affected_row_count"] == 3
    assert _mapping(stale_quote["run_length_counts"]) == {"3": 1}
    burst = _mapping(dynamics["burst"])
    assert burst["interval_count"] == 2
    assert burst["burst_rate"] == 0.4
    assert burst["run_count"] == 1
    assert burst["tick_count"] == 3
    assert _mapping(burst["run_length_counts"]) == {"3": 1}
    one_sided = _mapping(dynamics["one_sided_movement"])
    assert one_sided["count"] == 3
    assert one_sided["rate"] == 0.6
    assert one_sided["bid_only_count"] == 2
    assert one_sided["ask_only_count"] == 1
    assert one_sided["run_count"] == 1
    assert _mapping(one_sided["run_length_counts"]) == {"2": 1}


def test_fingerprint_m1_dependence_describes_lag_acf(
    tmp_path: Path,
) -> None:
    """M1 fingerprints should expose configured observed-sequence ACF."""
    case = HistDataAsciiCase(
        name="m1-dependence",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_DEPENDENCE.csv",
        rows=(
            "20120201 000000;1.000000;1.100000;0.900000;1.000000;0",
            "20120201 000100;1.100000;1.300000;1.000000;1.100000;0",
            "20120201 000200;1.300000;1.500000;1.200000;1.300000;0",
            "20120201 000300;1.600000;1.900000;1.400000;1.600000;0",
            "20120201 000400;2.000000;2.400000;1.700000;2.000000;0",
        ),
    )
    profile = HistDataFingerprintProfile(lags=(1, 3, 5), rounding_digits=6)
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target, profile))
    dependence = _mapping(payload["dependence"])

    close_returns = [
        math.log(1.1 / 1.0),
        math.log(1.3 / 1.1),
        math.log(1.6 / 1.3),
        math.log(2.0 / 1.6),
    ]
    range_ratios = [
        (1.1 - 0.9) / ((1.1 + 0.9) / 2),
        (1.3 - 1.0) / ((1.3 + 1.0) / 2),
        (1.5 - 1.2) / ((1.5 + 1.2) / 2),
        (1.9 - 1.4) / ((1.9 + 1.4) / 2),
        (2.4 - 1.7) / ((2.4 + 1.7) / 2),
    ]

    assert dependence["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION
    )
    assert dependence["basis"] == "observed_sequence"
    assert dependence["acf_basis"] == "observed_sequence"
    assert dependence["row_order"] == "source_text_order"
    assert dependence["computed_from"] == "text_scan"
    assert dependence["regular_grid"] is False
    assert dependence["lags"] == [1, 3, 5]
    assert dependence["dependence_status"] == "limited"

    close_acf = _mapping(dependence["close_log_return_acf"])
    assert close_acf["sample_count"] == 4
    assert _mapping(close_acf["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(close_returns, 1), 6),
        "3": _rounded_for_test(_acf_for_test(close_returns, 3), 6),
    }
    assert _mapping(close_acf["skipped_lags"])["5"] == {
        "reason": "insufficient_sample_count",
        "sample_count": 4,
        "required_sample_count": 6,
    }

    range_acf = _mapping(dependence["range_ratio_acf"])
    assert _mapping(range_acf["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(range_ratios, 1), 6),
        "3": _rounded_for_test(_acf_for_test(range_ratios, 3), 6),
    }
    assert _mapping(range_acf["skipped_lags"])["5"] == {
        "reason": "insufficient_sample_count",
        "sample_count": 5,
        "required_sample_count": 6,
    }


def test_fingerprint_tick_dependence_describes_spread_acf(
    tmp_path: Path,
) -> None:
    """Tick fingerprints should expose spread and spread-change ACF."""
    case = HistDataAsciiCase(
        name="tick-dependence",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_DEPENDENCE.csv",
        rows=(
            "20120201 000000000,1.000000,1.000200,0",
            "20120201 000000100,1.000000,1.000100,0",
            "20120201 000000200,1.000000,1.000300,0",
            "20120201 000000300,1.000000,1.000600,0",
        ),
    )
    profile = HistDataFingerprintProfile(lags=(1, 2), rounding_digits=6)
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target, profile))
    dependence = _mapping(payload["dependence"])

    spreads = [0.0002, 0.0001, 0.0003, 0.0006]
    spread_changes = [-0.0001, 0.0002, 0.0003]
    absolute_spread_changes = [0.0001, 0.0002, 0.0003]

    assert dependence["dependence_status"] == "ok"
    spread_acf = _mapping(dependence["spread_acf"])
    assert _mapping(spread_acf["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(spreads, 1), 6),
        "2": _rounded_for_test(_acf_for_test(spreads, 2), 6),
    }
    spread_change_acf = _mapping(dependence["spread_change_acf"])
    assert _mapping(spread_change_acf["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(spread_changes, 1), 6),
        "2": _rounded_for_test(_acf_for_test(spread_changes, 2), 6),
    }
    absolute_change_acf = _mapping(dependence["absolute_spread_change_acf"])
    assert _mapping(absolute_change_acf["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(absolute_spread_changes, 1), 6),
        "2": _rounded_for_test(_acf_for_test(absolute_spread_changes, 2), 6),
    }


def test_fingerprint_dependence_records_invalid_and_uncomputable_series(
    tmp_path: Path,
) -> None:
    """Dependence should filter invalid values and explain skipped lags."""
    invalid_case = HistDataAsciiCase(
        name="m1-dependence-invalid-row",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_INVALID_DEPENDENCE.csv",
        rows=(
            "20120201 000000;1.000000;1.100000;0.900000;1.000000;0",
            "20120201 000100;bad;1.300000;1.000000;1.100000;0",
            "20120201 000200;1.300000;1.500000;1.200000;1.300000;0",
            "20120201 000300;1.600000;1.900000;1.400000;1.600000;0",
        ),
    )
    profile = HistDataFingerprintProfile(lags=(1,), rounding_digits=8)
    invalid_target = _discovered_target(
        write_ascii_case(tmp_path, invalid_case)
    )
    invalid_payload = _fingerprint_payload(
        _fingerprint_finding(invalid_target, profile)
    )
    invalid_dependence = _mapping(invalid_payload["dependence"])

    assert invalid_dependence["invalid_row_count"] == 1
    assert invalid_dependence["usable_row_count"] == 3
    assert _mapping(
        _mapping(invalid_dependence["close_log_return_acf"])["lag_acf"]
    ) == {
        "1": _rounded_for_test(
            _acf_for_test([math.log(1.3 / 1.0), math.log(1.6 / 1.3)], 1),
            8,
        )
    }

    flat_case = HistDataAsciiCase(
        name="m1-dependence-zero-variance",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_FLAT_DEPENDENCE.csv",
        rows=(
            "20120201 000000;1.000000;1.000000;1.000000;1.000000;0",
            "20120201 000100;1.000000;1.000000;1.000000;1.000000;0",
            "20120201 000200;1.000000;1.000000;1.000000;1.000000;0",
        ),
    )
    flat_target = _discovered_target(write_ascii_case(tmp_path, flat_case))
    flat_payload = _fingerprint_payload(
        _fingerprint_finding(flat_target, profile)
    )
    flat_dependence = _mapping(flat_payload["dependence"])
    close_acf = _mapping(flat_dependence["close_log_return_acf"])

    assert flat_dependence["dependence_status"] == "limited"
    assert flat_dependence["reason"] == "no_computable_lags"
    assert _mapping(close_acf["lag_acf"]) == {}
    assert _mapping(close_acf["skipped_lags"])["1"] == {
        "reason": "zero_variance",
        "sample_count": 2,
    }


def test_fingerprint_dynamics_mark_non_monotonic_sequences_limited(
    tmp_path: Path,
) -> None:
    """Observed-order dynamics should flag structurally limited sequences."""
    case = HistDataAsciiCase(
        name="m1-non-monotonic-dynamics",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202.csv",
        rows=(
            "20120201 000200;1.000000;1.000000;1.000000;1.000000;0",
            "20120201 000100;1.100000;1.100000;1.100000;1.100000;0",
            "20120201 000300;1.200000;1.200000;1.200000;1.200000;0",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    topology = _mapping(payload["temporal_topology"])
    dynamics = _mapping(payload["return_dynamics"])

    assert topology["non_monotonic_count"] == 1
    assert dynamics["basis"] == "observed_sequence"
    assert dynamics["row_order"] == "source_text_order"
    assert dynamics["sequence_status"] == "limited"
    assert "non_monotonic_timestamp_order" in _list(dynamics["limitations"])
    assert _mapping(dynamics["close_log_return"])["count"] == 2


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


def test_fingerprint_calendar_regimes_reports_session_and_special_counts(
    tmp_path: Path,
) -> None:
    """Fingerprints should expose deterministic calendar/session regimes."""
    case = HistDataAsciiCase(
        name="m1_calendar_regimes",
        timeframe=M1,
        filename="DAT_ASCII_EURUSD_M1_201202_REGIMES.csv",
        rows=(
            "20120205 170000;1.306600;1.306610;1.306590;1.306600;0",
            "20120203 165900;1.306600;1.306610;1.306590;1.306600;0",
            "20120201 110000;1.306600;1.306610;1.306590;1.306600;0",
            "20120331 110000;1.306600;1.306610;1.306590;1.306600;0",
            "20221225 120000;1.306600;1.306610;1.306590;1.306600;0",
            "20221231 110000;1.306600;1.306610;1.306590;1.306600;0",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    regimes = _mapping(payload["calendar_regimes"])

    assert regimes["schema_version"] == (
        TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION
    )
    assert regimes["status"] == "ok"
    assert regimes["computed_from"] == "text_scan"
    assert regimes["row_count"] == 6
    assert regimes["parsed_row_count"] == 6
    assert regimes["invalid_timestamp_count"] == 0
    assert _mapping(regimes["session_state_counts"]) == {
        "friday_close": 1,
        "market_open": 1,
        "sunday_open": 1,
        "weekend_closure": 3,
    }
    special = _mapping(regimes["special_tag_counts"])
    assert special["sunday_open"] == 1
    assert special["friday_close"] == 1
    assert special["daily_rollover"] == 2
    assert special["london_4pm_fix_window"] == 3
    assert special["month_end"] == 2
    assert _mapping(regimes["holiday_tag_counts"]) == {
        "major_holiday:christmas_day": 1
    }
    assert _mapping(regimes["hour_of_day_counts"]) == {
        "11": 3,
        "12": 1,
        "16": 1,
        "17": 1,
    }
    assert _mapping(regimes["day_of_week_counts"]) == {
        "friday": 1,
        "saturday": 2,
        "sunday": 2,
        "wednesday": 1,
    }
    assert _mapping(regimes["calendar_basis"])["day_of_week"] == (
        "source_calendar"
    )
    policy = _mapping(regimes["calendar_policy"])
    assert policy["holiday_calendar_complete"] is False
    assert regimes["missing_optional_calendar_data"] is True
    assert "conditional_distributions" not in payload


def test_fingerprint_calendar_regimes_use_direct_cache_projection(
    tmp_path: Path,
) -> None:
    """Cache-backed fingerprints should classify calendar regimes from cache."""
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
    regimes = _mapping(payload["calendar_regimes"])

    assert regimes["status"] == "ok"
    assert regimes["computed_from"] == "direct_cache"
    assert regimes["cache_source"] == "direct"
    assert regimes["row_count"] == 3
    assert _mapping(regimes["session_state_counts"]) == {"market_open": 3}
    assert _mapping(regimes["active_session_counts"]) == {"asia": 3}
    assert _mapping(regimes["hour_of_day_counts"]) == {"00": 3}
    assert _mapping(regimes["day_of_week_counts"]) == {"wednesday": 3}


def test_fingerprint_tick_conditional_distributions_by_calendar_bucket(
    tmp_path: Path,
) -> None:
    """Tick spread summaries should be conditionable by calendar bucket."""
    case = HistDataAsciiCase(
        name="tick_calendar_conditioned_spread",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_CONDITIONED.csv",
        rows=(
            "20120201 030000000,1.000000,1.000200,0",
            "20120201 110000000,1.000000,1.000300,0",
            "20120203 165900000,1.000000,1.000400,0",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    payload = _fingerprint_payload(_fingerprint_finding(target))
    conditional = _mapping(payload["conditional_distributions"])

    assert conditional["schema_version"] == (
        TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION
    )
    assert conditional["basis"] == "text"
    assert conditional["metric"] == "tick_spread"
    assert conditional["row_count"] == 3
    assert conditional["usable_row_count"] == 3
    by_session = _mapping(conditional["by_active_session"])
    asia_spread = _mapping(_mapping(by_session["asia"])["spread"])
    london_spread = _mapping(_mapping(by_session["london"])["spread"])
    new_york_spread = _mapping(_mapping(by_session["new_york"])["spread"])
    no_active_spread = _mapping(
        _mapping(by_session["no_active_session_window"])["spread"]
    )
    assert asia_spread["median"] == 0.0002
    assert london_spread["median"] == 0.0002
    assert new_york_spread["median"] == 0.0003
    assert no_active_spread["median"] == 0.0004
    by_special = _mapping(conditional["by_special_tag"])
    assert (
        _mapping(_mapping(by_special["london_4pm_fix_window"])["spread"])[
            "median"
        ]
        == 0.0003
    )
    assert (
        _mapping(_mapping(by_special["daily_rollover"])["spread"])["median"]
        == 0.0004
    )
    assert (
        _mapping(_mapping(by_special["friday_close"])["spread"])["median"]
        == 0.0004
    )


def test_fingerprint_audit_marks_absent_conditioning_ineligible(
    tmp_path: Path,
) -> None:
    """Readable tick targets should explain intentionally skipped conditioning."""
    target = _discovered_target(
        write_ascii_case(
            tmp_path,
            HistDataAsciiCase(
                name="empty_tick_conditioning",
                timeframe=TICK,
                filename="DAT_ASCII_EURUSD_T_201202_EMPTY.csv",
                rows=(),
            ),
        )
    )
    payload = _fingerprint_payload(_fingerprint_finding(target))
    audit = _mapping(payload["fingerprint_audit"])

    assert "conditional_distributions" not in payload
    assert _list(audit["sections_expected"]) == [
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "tick_distribution",
        "conditional_distributions",
        "microstructure_dynamics",
        "dependence",
    ]
    assert _mapping(audit["sections_skipped"]) == {
        "conditional_distributions": {
            "reason": "insufficient_rows",
            "details": {
                "metric": "tick_spread",
                "grouped_by": ["active_session", "special_tag"],
            },
        }
    }
    assert _mapping(audit["section_statuses"])["tick_distribution"] == (
        "limited"
    )
    assert _mapping(audit["section_statuses"])["microstructure_dynamics"] == (
        "unavailable"
    )
    assert _mapping(audit["section_statuses"])["dependence"] == "unavailable"
    eligibility = _mapping(
        _mapping(audit["conditional_distribution_eligibility"])["tick_spread"]
    )
    assert eligibility == {
        "eligible": False,
        "status": "ineligible",
        "reason": "insufficient_rows",
    }
    readiness = _mapping(
        _mapping(audit["dynamics_readiness"])["microstructure_dynamics"]
    )
    assert readiness["status"] == "unavailable"
    assert readiness["reason"] == "insufficient_sequence_rows"
    assert "insufficient_sequence_rows" in _list(readiness["limitations"])


def test_fingerprint_calendar_regimes_use_configured_complete_profile(
    tmp_path: Path,
) -> None:
    """Fingerprint calendar regimes should honor resolved profile metadata."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_configured_calendar_profile",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_202203_PROFILED.csv",
            rows=(
                "20220415 120000;1.306600;1.306610;1.306590;1.306600;0",
                "20221227 120000;1.306600;1.306610;1.306590;1.306600;0",
                "20200316 120000;1.306600;1.306610;1.306590;1.306600;0",
            ),
        ),
    )
    discovery = discover_quality_targets((path,))
    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(
            ("fingerprint",),
            profile=_complete_calendar_profile(),
        ),
    )
    payload = _fingerprint_payload(report.findings[0])
    regimes = _mapping(payload["calendar_regimes"])
    policy = _mapping(regimes["calendar_policy"])

    assert regimes["status"] == "ok"
    assert regimes["calendar_profile_complete"] is True
    assert regimes["missing_optional_calendar_data"] is False
    assert policy["holiday_calendar_source"] == "operator-config"
    assert policy["holiday_calendar_complete"] is True
    assert _mapping(policy["calendar_profile"])["version"] == "2026.06"
    assert _mapping(regimes["holiday_tag_counts"]) == {
        "market_holiday:good_friday": 1
    }
    assert _mapping(regimes["event_tag_counts"]) == {
        "crisis:covid_shock": 1,
        "thin_liquidity:christmas_new_year": 1,
    }
    audit = _mapping(payload["fingerprint_audit"])
    completeness = _mapping(audit["profile_completeness"])
    assert completeness["calendar_profile_complete"] is True
    assert completeness["missing_optional_calendar_data"] is False
    assert completeness["calendar_profile_source"] == "operator-config"
    assert completeness["calendar_profile_version"] == "2026.06"


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
    assert invalid_distribution["partial_row_count"] == 0
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
    assert partial_distribution["partial_row_count"] == 1

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
    assert empty_distribution["partial_row_count"] == 0
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
    audit = _mapping(payload["fingerprint_audit"])
    assert _mapping(audit["section_statuses"])["m1_bar_distribution"] == (
        "limited"
    )
    readiness = _mapping(
        _mapping(audit["dynamics_readiness"])["return_dynamics"]
    )
    assert readiness["status"] == "valid"
    assert readiness["sampled_row_count"] == 2
    assert readiness["usable_row_count"] == 3
    assert readiness["truncated"] is True


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
    dynamics = _mapping(payload["return_dynamics"])
    assert dynamics["row_order"] == "cache_order"
    assert dynamics["computed_from"] == "direct_cache"
    assert dynamics["cache_source"] == "direct"
    assert _mapping(dynamics["close_log_return"])["count"] == 2
    assert topology["cache_source"] == "direct"
    assert topology["row_count"] == 3
    assert topology["min_interval_ms"] == 60_000
    audit = _mapping(payload["fingerprint_audit"])
    readiness = _mapping(
        _mapping(audit["dynamics_readiness"])["return_dynamics"]
    )
    assert readiness["status"] == "valid"
    assert readiness["row_order"] == "cache_order"
    assert readiness["computed_from"] == "direct_cache"
    assert readiness["cache_source"] == "direct"
    assert _mapping(audit["source_status"]) == {
        "kind": "cache",
        "readable": True,
        "reason": None,
    }


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


def test_fingerprint_cache_distribution_counts_full_rows_and_fills_sample(
    tmp_path: Path,
) -> None:
    """Cache distributions should count all rows and fill samples from usable rows."""
    import polars as pl

    profile = HistDataFingerprintProfile(max_rows=1)
    m1_cache_path = tmp_path / "m1-cache" / CACHE_FILENAME
    m1_cache_path.parent.mkdir(parents=True, exist_ok=True)
    m1_frame = pl.DataFrame(
        {
            "datetime": [1, 2, 3],
            "open": [None, 1.1, 1.2],
            "high": [1.1, 1.2, 1.3],
            "low": [0.9, 1.0, 1.1],
            "close": [1.05, 1.15, 1.25],
            "vol": [0, 0, 0],
        },
        schema={
            "datetime": pl.Int64,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "vol": pl.Int32,
        },
    )
    write_polars_cache(m1_frame, m1_cache_path)
    m1_target = QualityTarget(
        path=str(m1_cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )

    m1_payload = _fingerprint_payload(_fingerprint_finding(m1_target, profile))
    m1_distribution = _mapping(m1_payload["m1_bar_distribution"])

    assert m1_distribution["row_count"] == 3
    assert m1_distribution["sampled_row_count"] == 1
    assert m1_distribution["usable_row_count"] == 2
    assert m1_distribution["invalid_row_count"] == 1
    assert m1_distribution["truncated"] is True
    open_summary = _mapping(_mapping(m1_distribution["price"])["open"])
    assert open_summary["count"] == 1
    assert open_summary["median"] == 1.1

    tick_cache_path = tmp_path / "tick-cache" / CACHE_FILENAME
    tick_cache_path.parent.mkdir(parents=True, exist_ok=True)
    tick_frame = pl.DataFrame(
        {
            "datetime": [1, 2, 3],
            "bid": [None, 1.1, 1.2],
            "ask": [1.0001, 1.1002, 1.2003],
            "vol": [0, 0, 0],
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    )
    write_polars_cache(tick_frame, tick_cache_path)
    tick_target = QualityTarget(
        path=str(tick_cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )

    tick_payload = _fingerprint_payload(
        _fingerprint_finding(tick_target, profile)
    )
    tick_distribution = _mapping(tick_payload["tick_distribution"])

    assert tick_distribution["row_count"] == 3
    assert tick_distribution["sampled_row_count"] == 1
    assert tick_distribution["usable_row_count"] == 2
    assert tick_distribution["invalid_row_count"] == 1
    assert tick_distribution["truncated"] is True
    spread_summary = _mapping(tick_distribution["spread"])
    assert spread_summary["count"] == 1
    assert spread_summary["median"] == 0.0002


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
    audit = _mapping(payload["fingerprint_audit"])
    assert _list(audit["sections_expected"]) == [
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "m1_bar_distribution",
        "return_dynamics",
        "dependence",
    ]
    assert _list(audit["sections_emitted"]) == [
        "coverage",
        "temporal_topology",
    ]
    assert _mapping(audit["sections_skipped"]) == {
        "calendar_regimes": {"reason": "unsupported_target_kind"},
        "m1_bar_distribution": {
            "reason": "unsupported_target_kind",
            "details": {"timeframe": "M1"},
        },
        "return_dynamics": {
            "reason": "unsupported_target_kind",
            "details": {"timeframe": "M1"},
        },
        "dependence": {
            "reason": "unsupported_target_kind",
            "details": {"timeframe": "M1"},
        },
    }
    assert _mapping(audit["target_capability"]) == {
        "supported": False,
        "unsupported_reason": "unsupported_target_kind",
    }
    assert _mapping(audit["source_status"]) == {
        "kind": "unavailable",
        "readable": False,
        "reason": "unsupported_target_kind",
    }
    statuses = _mapping(audit["section_statuses"])
    assert statuses["coverage"] == "unavailable"
    assert statuses["temporal_topology"] == "unavailable"
    assert statuses["calendar_regimes"] == "skipped"


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


def test_series_fingerprint_distribution_summary_counts_mixed_payloads(
    tmp_path: Path,
) -> None:
    """Run metadata should summarize distribution payloads and advisories."""
    profile = HistDataFingerprintProfile(max_rows=1)
    clean_m1 = _discovered_target(
        write_ascii_case(tmp_path / "clean-m1", CLEAN_M1_CASE)
    )
    invalid_m1 = _discovered_target(
        write_ascii_case(
            tmp_path / "invalid-m1", case_by_name("m1_bad_numeric")
        )
    )
    partial_m1 = _discovered_target(
        write_ascii_case(
            tmp_path / "partial-m1", case_by_name("m1_malformed_row")
        )
    )
    empty_m1 = _discovered_target(
        write_ascii_case(tmp_path / "empty-m1", case_by_name("m1_empty_file"))
    )
    tick_spread_mix = _discovered_target(
        write_ascii_case(
            tmp_path / "tick-spread",
            HistDataAsciiCase(
                name="tick_spread_mix",
                timeframe=TICK,
                filename="DAT_ASCII_EURUSD_T_201202_SPREAD_MIX.csv",
                rows=(
                    "20120201 000003660,1.000000,1.000000,0",
                    "20120201 000003973,1.000200,1.000100,0",
                    "20120201 000004990,1.000000,1.000300,0",
                ),
            ),
        )
    )
    cache_path = tmp_path / "direct-cache" / CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    batch = parse_ascii_lines(M1, CLEAN_M1_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    cache_target = QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    missing_target = QualityTarget(
        path=str(tmp_path / "missing-distribution.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    missing_finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=missing_target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "target_axis": {
                    "data_format": "ascii",
                    "timeframe": "M1",
                    "symbol": "EURUSD",
                    "period": "201202",
                    "kind": "csv",
                },
                "coverage": {
                    "row_count": 3,
                    "parsed_row_count": 3,
                },
                "source": {
                    "kind": "csv_text",
                    "path": str(missing_target.path),
                },
            }
        },
    )
    findings = (
        _fingerprint_finding(clean_m1, profile),
        _fingerprint_finding(invalid_m1, profile),
        _fingerprint_finding(partial_m1, profile),
        _fingerprint_finding(empty_m1, profile),
        _fingerprint_finding(tick_spread_mix),
        _fingerprint_finding(cache_target, profile),
        missing_finding,
    )

    summary = _mapping(series_fingerprint_distribution_summary(findings))

    assert summary["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION
    )
    assert summary["rule_id"] == SERIES_FINGERPRINT_RULE_ID
    assert summary["target_count"] == 7
    assert summary["distribution_target_count"] == 6
    assert summary["m1_bar_distribution_target_count"] == 5
    assert summary["tick_distribution_target_count"] == 1
    assert summary["missing_distribution_target_count"] == 1
    assert summary["unavailable_distribution_target_count"] == 0
    assert summary["empty_distribution_target_count"] == 1
    assert summary["invalid_row_target_count"] == 2
    assert summary["partial_row_target_count"] == 1
    assert summary["truncated_distribution_target_count"] == 2
    assert summary["cache_backed_distribution_target_count"] == 1
    assert summary["text_backed_distribution_target_count"] == 5
    assert summary["total_invalid_row_count"] == 2
    assert summary["total_partial_row_count"] == 1
    assert summary["distribution_kind_counts"] == {
        "m1_bar": 5,
        "missing": 1,
        "tick": 1,
    }
    assert summary["distribution_source_counts"] == {
        "cache": 1,
        "text": 5,
        "unavailable": 1,
    }
    assert summary["precision_source_counts"] == {
        "cache_float": 1,
        "text": 4,
        "unavailable": 2,
    }
    assert summary["status_counts"] == {"available": 6, "missing": 1}
    assert json_safe_path_strings(summary)

    attention = _mapping(
        series_fingerprint_distribution_attention_summary(
            findings,
            target_limit=3,
        )
    )
    assert attention["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION
    )
    assert attention["distribution_target_count"] == 7
    assert attention["attention_target_count"] == 7
    assert attention["included_attention_target_count"] == 3
    assert attention["omitted_attention_target_count"] == 4
    assert attention["truncated"] is True
    assert attention["attention_thresholds"] == (
        HistDataFingerprintDistributionAttentionProfile().to_metadata()
    )
    assert attention["attention_flag_counts"] == {
        "cache_float_precision_basis": 1,
        "empty_distribution": 1,
        "high_invalid_row_rate": 2,
        "missing_precision_counts": 1,
        "missing_distribution": 1,
        "negative_tick_spreads_present": 1,
        "partial_rows_present": 1,
        "truncated_distribution": 2,
        "zero_tick_spread_rate_present": 1,
    }
    included = _list(attention["target_summaries"])
    assert _mapping(included[0])["attention_level"] == "missing"
    full_attention = _mapping(
        series_fingerprint_distribution_attention_summary(
            findings,
            target_limit=-1,
        )
    )
    assert (
        _distribution_attention_with_flag(
            full_attention,
            "negative_tick_spreads_present",
        )["negative_spread_count"]
        == 1
    )
    custom_profile = HistDataFingerprintProfile(
        max_rows=1,
        distribution_attention=(
            HistDataFingerprintDistributionAttentionProfile(
                invalid_row_min_count=2,
                zero_spread_min_count=2,
                negative_spread_min_count=2,
                flag_truncated_distribution=False,
                flag_cache_float_precision=False,
            )
        ),
    )
    custom_attention = _mapping(
        series_fingerprint_distribution_attention_summary(
            findings,
            profile=custom_profile,
            target_limit=-1,
        )
    )
    custom_flag_counts = _mapping(custom_attention["attention_flag_counts"])
    assert custom_attention["attention_thresholds"] == (
        custom_profile.distribution_attention.to_metadata()
    )
    assert custom_flag_counts == {
        "empty_distribution": 1,
        "missing_precision_counts": 1,
        "missing_distribution": 1,
        "partial_rows_present": 1,
    }
    assert json_safe_path_strings(attention)


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
    assert (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-distribution-summary.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY
        == "time_series_fingerprint_distribution_summary"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-distribution-attention.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY
        == "time_series_fingerprint_distribution_attention"
    )
    assert (
        TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-calendar-regimes.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-conditional-distributions.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-dynamics.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-dependence.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-audit.v1"
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


def _acf_for_test(values: list[float], lag: int) -> float:
    assert len(values) > lag
    mean = sum(values) / len(values)
    centered = [value - mean for value in values]
    denominator = sum(value * value for value in centered)
    assert denominator > 0
    numerator = sum(
        centered[index] * centered[index - lag]
        for index in range(lag, len(centered))
    )
    return numerator / denominator


def _rounded_for_test(value: float, digits: int) -> float:
    rounded = round(value, digits)
    return 0.0 if rounded == 0 else rounded


def _complete_calendar_profile() -> dict[str, Any]:
    return {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "complete-calendar",
        "rules": {
            "domain.calendar_sessions": {
                "calendar_profile": {
                    "name": "operator-complete-calendar",
                    "source": "operator-config",
                    "version": "2026.06",
                    "complete": True,
                    "date_tags": [
                        {
                            "name": "good_friday",
                            "tag": "market_holiday:good_friday",
                            "rule": "good_friday",
                            "asset_classes": ["fx"],
                            "description": "Movable Good Friday market holiday.",
                        }
                    ],
                    "window_tags": [
                        {
                            "name": "christmas_new_year_thin_liquidity",
                            "tag": "thin_liquidity:christmas_new_year",
                            "category": "thin_liquidity",
                            "start_month": 12,
                            "start_day": 24,
                            "end_month": 1,
                            "end_day": 2,
                            "description": "Christmas/New Year thin liquidity.",
                        },
                        {
                            "name": "covid_shock",
                            "tag": "crisis:covid_shock",
                            "category": "crisis",
                            "start_date": "2020-03-01",
                            "end_date": "2020-03-31",
                            "description": "Configured crisis-period tag.",
                        },
                    ],
                }
            }
        },
    }


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


def _distribution_attention_with_flag(
    attention: Mapping[str, Any],
    flag: str,
) -> dict[str, Any]:
    matches = [
        _mapping(target)
        for target in _list(attention["target_summaries"])
        if flag in _list(_mapping(target)["attention_flags"])
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

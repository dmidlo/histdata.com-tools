"""Tests for deterministic tick-only data-quality fingerprint plumbing."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import histdatacom.data_quality.symbols as symbols_module

from histdatacom.data_quality import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    CROSS_SERIES_FINGERPRINT_RULE_ID,
    CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION,
    DEFAULT_FINGERPRINT_HISTOGRAM_BINS,
    DEFAULT_FINGERPRINT_LAGS,
    DEFAULT_FINGERPRINT_MAX_ROWS,
    DEFAULT_FINGERPRINT_QUANTILES,
    DEFAULT_FINGERPRINT_ROLLING_WINDOWS,
    DEFAULT_FINGERPRINT_ROUNDING_DIGITS,
    QUALITY_PROFILE_SCHEMA_VERSION,
    QUALITY_ENGINE_METADATA_KEY,
    QUALITY_SKIP_EVENTS_SCHEMA_VERSION,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
    HistDataFingerprintDistributionAttentionProfile,
    HistDataFingerprintProfile,
    HistDataCrossSeriesFingerprintRule,
    HistDataSeriesFingerprintRule,
    QualityFinding,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    discover_quality_targets,
    quality_rules_for_groups,
    quality_next_actions_summary,
    quality_run_rules_for_groups,
    quality_target_from_path,
    run_quality_assessment,
    series_fingerprint_coverage_summary,
    series_fingerprint_distribution_attention_summary,
    series_fingerprint_distribution_summary,
    series_fingerprint_topology_attention_summary,
    series_fingerprint_topology_summary,
)
from histdatacom.data_quality.reporting import quality_report_payload
from histdatacom.data_quality.training_features import TRAINING_SCHEMA_VERSION
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    TICK,
    parse_ascii_lines,
    to_polars_frame,
    write_polars_cache,
)
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_TICK_CASE,
    CLEAN_TICK_ROWS,
    HistDataAsciiCase,
    case_by_name,
    write_ascii_case,
    write_zip_case,
)

TICK_SECTIONS = [
    "coverage",
    "temporal_topology",
    "calendar_regimes",
    "tick_distribution",
    "conditional_distributions",
    "microstructure_dynamics",
    "dependence",
    "stationarity_diagnostics",
    "decomposition",
]


def test_fingerprint_group_registers_series_rule_surface() -> None:
    """The advertised fingerprint group should expose its target rule."""
    rules = quality_rules_for_groups(("fingerprint",))

    assert [rule.rule_id for rule in rules] == [SERIES_FINGERPRINT_RULE_ID]
    assert isinstance(rules[0], HistDataSeriesFingerprintRule)
    assert SERIES_FINGERPRINT_RULE_ID in {
        rule.rule_id for rule in quality_rules_for_groups(("all",))
    }
    run_rules = quality_run_rules_for_groups(("fingerprint",))
    assert [rule.rule_id for rule in run_rules] == [
        CROSS_SERIES_FINGERPRINT_RULE_ID
    ]
    assert isinstance(run_rules[0], HistDataCrossSeriesFingerprintRule)
    all_run_rules = quality_run_rules_for_groups(("all",))
    assert CROSS_SERIES_FINGERPRINT_RULE_ID in {
        rule.rule_id for rule in all_run_rules
    }
    shared_domain_rule = next(
        rule
        for rule in all_run_rules
        if rule.rule_id == "domain.cross_instrument_consistency"
    )
    shared_fingerprint_rule = next(
        rule
        for rule in all_run_rules
        if rule.rule_id == CROSS_SERIES_FINGERPRINT_RULE_ID
    )
    assert shared_domain_rule.scan_provider is not None
    assert shared_fingerprint_rule.scan_provider is (
        shared_domain_rule.scan_provider
    )


def test_cross_series_fingerprint_profiles_unequal_triangle_and_identity(
    tmp_path: Path,
) -> None:
    """Triangle fingerprints should preserve identity and limiting ranges."""
    cases = (
        _cross_series_case(
            "EURUSD",
            (
                "20120201 000000000,1.200000,1.200200,0",
                "20120201 000001000,1.210000,1.210200,0",
                "20120201 000001000,1.211000,1.211200,0",
                "20120201 000002000,1.220000,1.220200,0",
                "20120201 000003000,1.240000,1.240200,0",
            ),
        ),
        _cross_series_case(
            "GBPUSD",
            (
                "20120201 000000000,1.500000,1.500200,0",
                "20120201 000001000,1.510000,1.510200,0",
                "20120201 000002000,1.525000,1.525200,0",
                "20120201 000003000,1.540000,1.540200,0",
            ),
        ),
        _cross_series_case(
            "EURGBP",
            (
                "20120201 000001000,0.801000,0.801200,0",
                "20120201 000002000,0.900000,0.900200,0",
                "20120201 000003000,0.805000,0.805200,0",
            ),
        ),
    )
    targets = tuple(
        _discovered_target(write_ascii_case(tmp_path / case.name, case))
        for case in cases
    )

    report = run_quality_assessment(
        targets,
        quality_rules_for_groups(("fingerprint",)),
        run_rules=quality_run_rules_for_groups(("fingerprint",)),
        metadata={"roots": [str(tmp_path)]},
    )
    payload = _mapping(report.metadata[CROSS_SERIES_FINGERPRINT_METADATA_KEY])
    group = _mapping(_list(payload["groups"])[0])
    grid = _mapping(group["timestamp_grid"])
    ranges = _mapping(group["coverage_ranges"])
    series_by_symbol = {
        str(item["symbol"]): item
        for item in (_mapping(value) for value in _list(group["series"]))
    }

    assert payload["schema_version"] == CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION
    assert payload["rule_id"] == CROSS_SERIES_FINGERPRINT_RULE_ID
    assert payload["group_count"] == 1
    triangular = _mapping(payload["triangular_consistency"])
    assert triangular["candidate_count"] == 1
    assert grid["union_timestamp_count"] == 4
    assert grid["common_timestamp_count"] == 3
    assert grid["common_timestamp_ratio"] == 0.75
    assert ranges["unequal_ranges"] is True
    assert ranges["limiting_start_symbols"] == ["EURGBP"]
    assert series_by_symbol["EURUSD"]["row_count"] == 5
    assert series_by_symbol["EURUSD"]["unique_timestamp_count"] == 4
    assert series_by_symbol["EURUSD"]["duplicate_timestamp_row_count"] == 2
    assert series_by_symbol["EURUSD"]["identity_columns"] == [
        "series_id",
        "period",
        "row_id",
        "source_row_number",
        "event_seq",
    ]
    topology = _mapping(group["topology"])
    assert topology["target_count"] == 3
    assert topology["duplicate_timestamp_row_count"] == 2
    correlation = _mapping(group["return_correlation"])
    assert correlation["pair_count"] == 3
    assert any(
        _mapping(pair)["status"] == "valid"
        for pair in _list(correlation["pairs"])
    )
    triangle_samples = [
        *_list(triangular["warning_samples"]),
        *_list(triangular["error_samples"]),
    ]
    assert triangle_samples
    direct_identity = _mapping(
        _mapping(_mapping(triangle_samples[0])["row_identity"])["direct"]
    )
    assert direct_identity["series_id"] == "ascii:T:EURGBP:histdata.com"
    assert direct_identity["period"] == "201202"
    assert int(direct_identity["row_id"]) > 0
    cross_finding = next(
        finding
        for finding in report.findings
        if finding.rule_id == CROSS_SERIES_FINGERPRINT_RULE_ID
    )
    assert cross_finding.severity is QualitySeverity.INFO
    assert str(tmp_path) not in str(payload)
    rerun = quality_run_rules_for_groups(("fingerprint",))[0].evaluate_run(
        targets,
        metadata={
            TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY: (
                report.metadata[
                    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY
                ]
            )
        },
    )
    assert rerun.metadata[CROSS_SERIES_FINGERPRINT_METADATA_KEY] == payload


def test_all_group_shares_cross_instrument_scan_between_rule_surfaces(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """The all group should not read the same panel twice."""
    targets = tuple(
        _discovered_target(
            write_ascii_case(
                tmp_path / symbol.lower(),
                _cross_series_case(
                    symbol,
                    (
                        "20120201 000000000,1.200000,1.200200,0",
                        "20120201 000001000,1.210000,1.210200,0",
                    ),
                ),
            )
        )
        for symbol in ("EURGBP", "EURUSD", "GBPUSD")
    )
    calls = 0
    original_scan = symbols_module._scan_cross_instrument_consistency

    def counted_scan(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(
        symbols_module,
        "_scan_cross_instrument_consistency",
        counted_scan,
    )
    cross_rules = tuple(
        rule
        for rule in quality_run_rules_for_groups(("all",))
        if rule.rule_id
        in {
            "domain.cross_instrument_consistency",
            CROSS_SERIES_FINGERPRINT_RULE_ID,
        }
    )

    report = run_quality_assessment(targets, (), run_rules=cross_rules)

    assert calls == 1
    assert "cross_instrument_consistency" in report.metadata
    assert CROSS_SERIES_FINGERPRINT_METADATA_KEY in report.metadata


def test_cross_series_fingerprint_reports_inverse_sparse_and_stale_risk(
    tmp_path: Path,
) -> None:
    """Inverse and sparse panels should retain descriptive risk summaries."""
    cases = (
        _cross_series_case(
            "EURUSD",
            (
                "20120201 000000000,1.250000,1.250000,0",
                "20120201 000001000,1.260000,1.260000,0",
                "20120201 000002000,1.270000,1.270000,0",
                "20120201 000003000,1.280000,1.280000,0",
            ),
        ),
        _cross_series_case(
            "USDEUR",
            ("20120201 000000000,0.800000,0.800000,0",),
        ),
    )
    targets = tuple(
        _discovered_target(write_ascii_case(tmp_path / case.name, case))
        for case in cases
    )

    report = run_quality_assessment(
        targets,
        (),
        run_rules=quality_run_rules_for_groups(("fingerprint",)),
    )
    payload = _mapping(report.metadata[CROSS_SERIES_FINGERPRINT_METADATA_KEY])
    group = _mapping(_list(payload["groups"])[0])
    pair = _mapping(_list(_mapping(group["return_correlation"])["pairs"])[0])

    assert _mapping(payload["inverse_consistency"])["candidate_count"] == 1
    assert _mapping(payload["stale_join_risk"])["risk_count"] == 1
    assert _mapping(group["timestamp_grid"])["common_timestamp_ratio"] == 0.25
    assert pair == {
        "left_symbol": "EURUSD",
        "right_symbol": "USDEUR",
        "overlap_return_count": 0,
        "status": "unavailable",
        "reason": "insufficient_overlap",
    }
    assert payload["status"] == "limited"


def test_cross_series_fingerprint_reports_limiting_triangle_period_range(
    tmp_path: Path,
) -> None:
    """A later-starting triangle leg should limit common panel coverage."""
    cases = (
        _cross_series_case(
            "EURUSD",
            ("20000501 000000000,1.100000,1.100200,0",),
            period="200005",
        ),
        _cross_series_case(
            "GBPUSD",
            ("20000501 000000000,1.500000,1.500200,0",),
            period="200005",
        ),
        _cross_series_case(
            "EURUSD",
            ("20020301 000000000,1.200000,1.200200,0",),
            period="200203",
        ),
        _cross_series_case(
            "GBPUSD",
            ("20020301 000000000,1.500000,1.500200,0",),
            period="200203",
        ),
        _cross_series_case(
            "EURGBP",
            ("20020301 000000000,0.800000,0.800200,0",),
            period="200203",
        ),
    )
    targets = tuple(
        _discovered_target(
            write_ascii_case(tmp_path / f"{case.name}-{index}", case)
        )
        for index, case in enumerate(cases)
    )

    report = run_quality_assessment(
        targets,
        (),
        run_rules=quality_run_rules_for_groups(("fingerprint",)),
    )
    payload = _mapping(report.metadata[CROSS_SERIES_FINGERPRINT_METADATA_KEY])
    panel = _mapping(_list(payload["panel_coverage"])[0])
    groups = {
        str(_mapping(group)["group_id"]): _mapping(group)
        for group in _list(payload["groups"])
    }

    assert panel["union_period_count"] == 2
    assert panel["common_period_count"] == 1
    assert panel["common_first_period"] == "200203"
    assert panel["unequal_period_ranges"] is True
    assert panel["limiting_start_symbols"] == ["EURGBP"]
    assert _mapping(panel["missing_period_count_by_symbol"])["EURGBP"] == 1
    assert groups["ascii:T:200005"]["complete"] is False
    assert groups["ascii:T:200005"]["missing_symbols"] == ["EURGBP"]
    assert groups["ascii:T:200203"]["complete"] is True
    assert payload["incomplete_group_count"] == 1
    assert payload["status"] == "limited"


def test_cross_series_fingerprint_enriches_legacy_raw_cache_for_report(
    tmp_path: Path,
) -> None:
    """Legacy raw caches should be enriched in memory before projection."""
    targets: list[QualityTarget] = []
    for symbol, prices in {
        "EURUSD": ("1.200000", "1.210000", "1.220000"),
        "GBPUSD": ("1.500000", "1.510000", "1.520000"),
    }.items():
        rows = tuple(
            f"20120201 00000{index}000,{price},{price},0"
            for index, price in enumerate(prices)
        )
        batch = parse_ascii_lines(TICK, rows)
        cache_path = tmp_path / symbol.lower() / CACHE_FILENAME
        cache_path.parent.mkdir(parents=True)
        write_polars_cache(to_polars_frame(batch), cache_path)
        targets.append(
            QualityTarget(
                path=str(cache_path),
                kind=QualityTargetKind.CACHE,
                data_format="ascii",
                timeframe=TICK,
                symbol=symbol,
                period="201202",
            )
        )

    report = run_quality_assessment(
        targets,
        (),
        run_rules=quality_run_rules_for_groups(("fingerprint",)),
    )
    report_payload = quality_report_payload(report)
    payload = _mapping(
        _mapping(report_payload["metadata"])[
            CROSS_SERIES_FINGERPRINT_METADATA_KEY
        ]
    )
    series = [
        _mapping(item)
        for item in _list(_mapping(_list(payload["groups"])[0])["series"])
    ]

    assert {item["computed_from"] for item in series} == {"direct_cache"}
    assert {item["cache_source"] for item in series} == {"direct"}
    assert {item["training_schema_version"] for item in series} == {
        TRAINING_SCHEMA_VERSION
    }
    assert all(str(item["series_id"]).startswith("ascii:T:") for item in series)
    assert str(tmp_path) not in str(report_payload)


def test_cross_series_fingerprint_reports_mixed_cache_provenance(
    tmp_path: Path,
) -> None:
    """Group topology should expose direct, sibling, and text scan bases."""
    rows = (
        "20120201 000000000,1.200000,1.200200,0",
        "20120201 000001000,1.210000,1.210200,0",
        "20120201 000002000,1.220000,1.220200,0",
    )
    batch = parse_ascii_lines(TICK, rows)

    direct_path = tmp_path / "eurusd" / CACHE_FILENAME
    direct_path.parent.mkdir(parents=True)
    write_polars_cache(to_polars_frame(batch), direct_path)
    direct_target = QualityTarget(
        path=str(direct_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )

    sibling_case = _cross_series_case("GBPUSD", rows)
    sibling_csv = write_ascii_case(tmp_path / "gbpusd", sibling_case)
    sibling_cache = sibling_csv.with_name(CACHE_FILENAME)
    write_polars_cache(to_polars_frame(batch), sibling_cache)
    csv_mtime_ns = sibling_csv.stat().st_mtime_ns
    os.utime(
        sibling_cache,
        ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000),
    )
    sibling_target = _discovered_target(sibling_csv)

    text_target = _discovered_target(
        write_ascii_case(
            tmp_path / "eurgbp",
            _cross_series_case("EURGBP", rows),
        )
    )

    report = run_quality_assessment(
        (direct_target, sibling_target, text_target),
        quality_rules_for_groups(("fingerprint",)),
        run_rules=quality_run_rules_for_groups(("fingerprint",)),
    )
    payload = _mapping(report.metadata[CROSS_SERIES_FINGERPRINT_METADATA_KEY])
    topology = _mapping(_mapping(_list(payload["groups"])[0])["topology"])

    assert topology["computed_from_counts"] == {
        "direct_cache": 1,
        "fresh_sibling_cache": 1,
        "text_scan": 1,
    }
    assert topology["cache_source_counts"] == {"direct": 1, "sibling": 1}
    assert topology["topology_computed_from_counts"] == {
        "direct_cache": 1,
        "fresh_sibling_cache": 1,
        "text_scan": 1,
    }
    assert topology["mixed_computation_basis"] is True
    assert topology["mixed_cache_source"] is True


def test_fingerprint_rule_emits_tick_csv_payload(tmp_path: Path) -> None:
    """Clean tick CSV files should produce canonical coverage metadata."""
    target = _discovered_target(write_ascii_case(tmp_path, CLEAN_TICK_CASE))
    finding = _fingerprint_finding(target)
    payload = _fingerprint_payload(finding)
    batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)

    assert finding.code == "FINGERPRINT_SERIES_SUMMARY"
    assert finding.severity is QualitySeverity.INFO
    assert payload["schema_version"] == TIME_SERIES_FINGERPRINT_SCHEMA_VERSION
    assert str(payload["fingerprint_id"]).startswith("sha256:")
    assert _mapping(payload["target_axis"]) == {
        "data_format": "ascii",
        "timeframe": "T",
        "symbol": "EURUSD",
        "period": "201202",
        "kind": "csv",
    }
    assert _mapping(payload["coverage"]) == {
        "row_count": 3,
        "parsed_row_count": 3,
        "start_timestamp_utc_ms": batch.summary.start,
        "end_timestamp_utc_ms": batch.summary.end,
        "duration_ms": 11_330,
    }

    topology = _mapping(payload["temporal_topology"])
    assert topology["row_count"] == 3
    assert topology["parsed_row_count"] == 3
    assert topology["invalid_timestamp_count"] == 0
    assert topology["duplicate_timestamp_count"] == 0
    assert topology["tick_duplicate_row_count"] == 0
    assert topology["min_interval_ms"] == 313
    assert topology["max_gap_ms"] == 11_017
    assert topology["sampling_basis"] == "observed_sequence"
    assert topology["computed_from"] == "text_scan"

    distribution = _mapping(payload["tick_distribution"])
    assert distribution["row_count"] == 3
    assert distribution["usable_row_count"] == 3
    assert distribution["zero_spread_rate"] == 0.0
    assert distribution["negative_spread_rate"] == 0.0
    spread_summary = _mapping(distribution["spread"])
    assert spread_summary["count"] == 3
    assert spread_summary["median"] == 0.00017

    dynamics = _mapping(payload["microstructure_dynamics"])
    assert (
        dynamics["schema_version"]
        == TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION
    )
    assert dynamics["sequence_status"] == "ok"
    assert _mapping(dynamics["interarrival_ms"])["count"] == 2

    stationarity = _mapping(payload["stationarity_diagnostics"])
    assert stationarity["schema_version"] == (
        TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION
    )
    assert stationarity["metric"] == "mid_price"
    assert _mapping(stationarity["sample_counts"]) == {"level": 3, "return": 2}

    decomposition = _mapping(payload["decomposition"])
    assert decomposition["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION
    )
    assert decomposition["metric"] == "mid_price"
    assert _mapping(decomposition["sample_counts"]) == {
        "level": 3,
        "return": 2,
    }
    projection = _mapping(decomposition["training_projection"])
    assert projection["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION
    )
    assert projection["grain"] == "period"
    assert _list(projection["identity_fields"]) == [
        "series_id",
        "period",
        "row_id",
    ]

    audit = _mapping(payload["fingerprint_audit"])
    assert (
        audit["schema_version"] == TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION
    )
    assert _list(audit["sections_expected"]) == TICK_SECTIONS
    assert _list(audit["sections_emitted"]) == TICK_SECTIONS
    assert _mapping(audit["sections_skipped"]) == {}
    statuses = _mapping(audit["section_statuses"])
    assert statuses["tick_distribution"] == "valid"
    assert statuses["conditional_distributions"] == "valid"
    assert statuses["microstructure_dynamics"] == "valid"
    assert statuses["dependence"] == "limited"
    assert statuses["stationarity_diagnostics"] == "limited"
    assert statuses["decomposition"] == "limited"
    assert _mapping(audit["decomposition_readiness"])["status"] == "limited"
    assert _retired_bar_schema_keys(payload) == set()


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
    payload = _payload_for_case(tmp_path, case)
    dynamics = _mapping(payload["microstructure_dynamics"])

    assert dynamics["sequence_status"] == "ok"
    assert dynamics["row_count"] == 6
    assert dynamics["usable_row_count"] == 6
    assert _mapping(dynamics["interarrival_ms"])["median"] == 200.0
    assert _mapping(dynamics["spread_change"])["max"] == 0.0008
    assert _mapping(dynamics["spread_jump"])["count"] == 1
    assert dynamics["zero_spread_count"] == 1
    assert _mapping(dynamics["stale_quote"])["run_length_counts"] == {"3": 1}
    assert _mapping(dynamics["burst"])["run_length_counts"] == {"3": 1}
    assert _mapping(dynamics["one_sided_movement"])["run_length_counts"] == {
        "2": 1
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
    payload = _payload_for_case(tmp_path, case, profile)
    dependence = _mapping(payload["dependence"])

    spreads = [0.0002, 0.0001, 0.0003, 0.0006]
    spread_changes = [-0.0001, 0.0002, 0.0003]
    absolute_spread_changes = [0.0001, 0.0002, 0.0003]

    assert dependence["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION
    )
    assert dependence["dependence_status"] == "ok"
    assert _mapping(_mapping(dependence["spread_acf"])["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(spreads, 1), 6),
        "2": _rounded_for_test(_acf_for_test(spreads, 2), 6),
    }
    assert _mapping(_mapping(dependence["spread_change_acf"])["lag_acf"]) == {
        "1": _rounded_for_test(_acf_for_test(spread_changes, 1), 6),
        "2": _rounded_for_test(_acf_for_test(spread_changes, 2), 6),
    }
    assert _mapping(
        _mapping(dependence["absolute_spread_change_acf"])["lag_acf"]
    ) == {
        "1": _rounded_for_test(_acf_for_test(absolute_spread_changes, 1), 6),
        "2": _rounded_for_test(_acf_for_test(absolute_spread_changes, 2), 6),
    }


def test_fingerprint_stationarity_diagnostics_describe_stable_tick_series(
    tmp_path: Path,
) -> None:
    """Stationarity diagnostics should summarize stable bounded tick windows."""
    case = _tick_case_from_mid_prices(
        "tick-stable-stationarity",
        (0.9, 1.1, 1.0, 0.95, 1.05, 1.0, 1.0, 0.9, 1.1),
    )
    profile = HistDataFingerprintProfile(
        rolling_windows=(2, 3), rounding_digits=6
    )
    payload = _payload_for_case(tmp_path, case, profile)
    stationarity = _mapping(payload["stationarity_diagnostics"])

    assert stationarity["stationarity_status"] == "ok"
    assert stationarity["metric"] == "mid_price"
    assert stationarity["computed_window_count"] == 2
    assert _list(stationarity["recommended_transforms"]) == [
        "log_return",
        "session_conditioning",
    ]
    window_two = _mapping(_mapping(stationarity["rolling_windows"])["2"])
    level_mean_drift = _mapping(window_two["level_rolling_mean_drift"])
    assert level_mean_drift["first"] == 1.0
    assert level_mean_drift["last"] == 1.0
    assert level_mean_drift["absolute_change"] == 0.0


def test_fingerprint_decomposition_handles_flat_and_trending_ticks(
    tmp_path: Path,
) -> None:
    """Decomposition proxies should distinguish flat and linear tick levels."""
    profile = HistDataFingerprintProfile(
        rolling_windows=(2, 3), rounding_digits=6
    )
    flat = _mapping(
        _payload_for_case(
            tmp_path / "flat",
            _tick_case_from_mid_prices("tick-flat", (1.0,) * 9),
            profile,
        )["decomposition"]
    )
    trending = _mapping(
        _payload_for_case(
            tmp_path / "trend",
            _tick_case_from_mid_prices(
                "tick-trend",
                (1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8),
            ),
            profile,
        )["decomposition"]
    )

    assert flat["decomposition_status"] == "limited"
    assert "zero_variance" in _list(flat["limitations"])
    assert _mapping(flat["trend_proxy"])["direction"] == "flat"
    assert (
        _mapping(flat["residual_proxy"])["residual_to_level_variance_ratio"]
        is None
    )
    assert _mapping(flat["stationarity_basis"])["status"] == "valid"
    assert _mapping(flat["stationarity_basis"])["zero_variance_metrics"]

    trend = _mapping(trending["trend_proxy"])
    assert trend["direction"] == "increasing"
    assert trend["slope_per_observation"] == 0.1
    assert trend["trend_strength"] == 1.0
    assert trending["computed_window_count"] == 2
    assert _mapping(trending["stationarity_basis"])["status"] == "valid"
    assert _retired_bar_schema_keys(trending) == set()


def test_fingerprint_decomposition_seasonality_is_calendar_based_and_bounded(
    tmp_path: Path,
) -> None:
    """Seasonality buckets should reuse source-calendar sessions and limits."""
    case = HistDataAsciiCase(
        name="tick-decomposition-seasonality",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202.csv",
        rows=(
            "20120102 010000000,1.000000,1.000000,0",
            "20120102 090000000,1.100000,1.100000,0",
            "20120103 170000000,1.200000,1.200000,0",
            "20120104 230000000,1.300000,1.300000,0",
        ),
    )
    profile = HistDataFingerprintProfile(
        rolling_windows=(2,), histogram_bins=2, rounding_digits=6
    )
    decomposition = _mapping(
        _payload_for_case(tmp_path, case, profile)["decomposition"]
    )
    seasonality = _mapping(decomposition["seasonality_proxy"])
    by_hour = _mapping(seasonality["by_source_hour"])
    by_weekday = _mapping(seasonality["by_source_weekday"])
    by_session = _mapping(seasonality["by_active_session"])

    assert seasonality["grouped_by"] == [
        "source_hour",
        "source_weekday",
        "active_session",
    ]
    assert by_hour["bucket_count"] == 4
    assert by_hour["included_bucket_count"] == 2
    assert by_hour["truncated"] is True
    assert by_weekday["bucket_count"] == 3
    assert by_session["bucket_count"] >= 3
    assert all(
        len(_mapping(group)["buckets"]) <= 2
        for group in (
            by_hour,
            by_weekday,
            by_session,
        )
    )


def test_fingerprint_decomposition_insufficient_series_is_unavailable(
    tmp_path: Path,
) -> None:
    """A one-row series should report deterministic unavailable proxies."""
    decomposition = _mapping(
        _payload_for_case(
            tmp_path,
            _tick_case_from_mid_prices("tick-insufficient", (1.0,)),
            HistDataFingerprintProfile(rolling_windows=(2, 3)),
        )["decomposition"]
    )

    assert decomposition["decomposition_status"] == "unavailable"
    assert decomposition["reason"] == "insufficient_sequence_rows"
    assert "insufficient_sample_count" in _list(decomposition["limitations"])
    assert decomposition["computed_window_count"] == 0
    assert decomposition["skipped_window_count"] == 2
    assert _mapping(decomposition["structural_break_proxy"])["status"] == (
        "skipped"
    )
    assert _mapping(decomposition["stationarity_basis"])["status"] == (
        "unavailable"
    )


def test_fingerprint_decomposition_structural_break_proxy_is_deterministic(
    tmp_path: Path,
) -> None:
    """Structural candidates should rank a fixed step change identically."""
    case = _tick_case_from_mid_prices(
        "tick-structural-break",
        (1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 2.0),
    )
    profile = HistDataFingerprintProfile(
        rolling_windows=(2,), histogram_bins=3, rounding_digits=6
    )
    first = _mapping(
        _payload_for_case(tmp_path / "first", case, profile)["decomposition"]
    )
    second = _mapping(
        _payload_for_case(tmp_path / "second", case, profile)["decomposition"]
    )
    first_structural = _mapping(first["structural_break_proxy"])
    second_structural = _mapping(second["structural_break_proxy"])

    assert first_structural == second_structural
    assert first_structural["status"] == "computed"
    assert first_structural["candidate_count"] == 5
    assert first_structural["included_candidate_count"] == 3
    assert first_structural["truncated"] is True
    assert _mapping(first_structural["strongest_candidate"])["split_index"] == 3


def test_fingerprint_calendar_regimes_and_conditioning_are_tick_based(
    tmp_path: Path,
) -> None:
    """Fingerprints should expose calendar regimes and tick-spread conditioning."""
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
    payload = _payload_for_case(tmp_path, case)

    regimes = _mapping(payload["calendar_regimes"])
    assert regimes["schema_version"] == (
        TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION
    )
    assert regimes["status"] == "ok"
    assert _mapping(regimes["session_state_counts"]) == {
        "friday_close": 1,
        "market_open": 2,
    }
    assert _mapping(regimes["active_session_counts"]) == {
        "asia": 1,
        "london": 1,
        "new_york": 1,
        "no_active_session_window": 1,
    }

    conditional = _mapping(payload["conditional_distributions"])
    assert conditional["schema_version"] == (
        TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION
    )
    assert conditional["metric"] == "tick_spread"
    by_session = _mapping(conditional["by_active_session"])
    assert _mapping(_mapping(by_session["asia"])["spread"])["median"] == 0.0002
    assert (
        _mapping(_mapping(by_session["new_york"])["spread"])["median"] == 0.0003
    )
    assert (
        _mapping(
            _mapping(_mapping(conditional["by_special_tag"])["friday_close"])[
                "spread"
            ]
        )["median"]
        == 0.0004
    )


def test_fingerprint_calendar_regimes_use_configured_complete_profile(
    tmp_path: Path,
) -> None:
    """Fingerprint calendar regimes should honor resolved profile metadata."""
    case = HistDataAsciiCase(
        name="tick_configured_calendar_profile",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_202203_PROFILED.csv",
        rows=(
            "20220415 120000000,1.306600,1.306610,0",
            "20221227 120000000,1.306600,1.306610,0",
            "20200316 120000000,1.306600,1.306610,0",
        ),
    )
    path = write_ascii_case(tmp_path, case)
    discovery = discover_quality_targets((path,))
    report = run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(
            ("fingerprint",), profile=_complete_calendar_profile()
        ),
    )
    payload = _fingerprint_payload(report.findings[0])
    regimes = _mapping(payload["calendar_regimes"])
    policy = _mapping(regimes["calendar_policy"])

    assert regimes["status"] == "ok"
    assert regimes["calendar_profile_complete"] is True
    assert regimes["missing_optional_calendar_data"] is False
    assert policy["holiday_calendar_source"] == "operator-config"
    assert _mapping(regimes["holiday_tag_counts"]) == {
        "market_holiday:good_friday": 1
    }
    assert _mapping(regimes["event_tag_counts"]) == {
        "crisis:covid_shock": 1,
        "thin_liquidity:christmas_new_year": 1,
    }


def test_fingerprint_distribution_handles_invalid_partial_and_empty_tick_rows(
    tmp_path: Path,
) -> None:
    """Tick distribution summaries should stay bounded for sparse bad input."""
    invalid_payload = _payload_for_case(
        tmp_path / "invalid", case_by_name("tick_bad_numeric")
    )
    invalid_distribution = _mapping(invalid_payload["tick_distribution"])
    assert invalid_distribution["row_count"] == 2
    assert invalid_distribution["usable_row_count"] == 1
    assert invalid_distribution["invalid_row_count"] == 1

    partial_payload = _payload_for_case(
        tmp_path / "partial", case_by_name("tick_malformed_row")
    )
    partial_distribution = _mapping(partial_payload["tick_distribution"])
    assert partial_distribution["row_count"] == 2
    assert partial_distribution["usable_row_count"] == 1
    assert partial_distribution["partial_row_count"] == 1

    empty_payload = _payload_for_case(
        tmp_path / "empty", case_by_name("tick_empty_file")
    )
    empty_distribution = _mapping(empty_payload["tick_distribution"])
    spread_summary = _mapping(empty_distribution["spread"])
    assert empty_distribution["row_count"] == 0
    assert empty_distribution["usable_row_count"] == 0
    assert spread_summary["count"] == 0
    assert spread_summary["median"] is None


def test_fingerprint_distribution_uses_profile_quantiles_and_rounding(
    tmp_path: Path,
) -> None:
    """Fingerprint profile knobs should shape tick distribution payloads."""
    target = _discovered_target(write_ascii_case(tmp_path, CLEAN_TICK_CASE))
    profile = HistDataFingerprintProfile(
        quantiles=(0.0, 0.5, 1.0),
        max_rows=2,
        rounding_digits=5,
    )
    payload = _fingerprint_payload(_fingerprint_finding(target, profile))
    distribution = _mapping(payload["tick_distribution"])
    bid_summary = _mapping(distribution["bid"])

    assert distribution["row_count"] == 3
    assert distribution["sampled_row_count"] == 2
    assert distribution["usable_row_count"] == 3
    assert distribution["truncated"] is True
    assert bid_summary["mean"] == 1.30659
    assert _mapping(bid_summary["quantiles"]) == {
        "0.0": 1.30658,
        "0.5": 1.30659,
        "1.0": 1.3066,
    }
    audit = _mapping(payload["fingerprint_audit"])
    assert _mapping(audit["section_statuses"])["tick_distribution"] == "limited"
    readiness = _mapping(
        _mapping(audit["dynamics_readiness"])["microstructure_dynamics"]
    )
    assert readiness["sampled_row_count"] == 2
    assert readiness["usable_row_count"] == 3
    assert readiness["truncated"] is True


def test_fingerprint_rule_emits_zip_member_payload(tmp_path: Path) -> None:
    """ZIP artifacts should name the member used for coverage."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_TICK_CASE,
        zip_filename="HISTDATA_COM_ASCII_EURUSD_T201202.zip",
    )
    payload = _fingerprint_payload(
        _fingerprint_finding(_discovered_target(archive))
    )

    assert _mapping(payload["source"]) == {
        "kind": "zip_member",
        "path": "HISTDATA_COM_ASCII_EURUSD_T201202.zip",
        "member": CLEAN_TICK_CASE.filename,
    }
    assert _mapping(payload["coverage"])["row_count"] == 3


def test_fingerprint_rule_prefers_direct_cache_payload(tmp_path: Path) -> None:
    """Direct cache targets should be fingerprinted without text fallback."""
    cache_path = tmp_path / CACHE_FILENAME
    batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    target = QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
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
        "duration_ms": 11_330,
    }
    assert _mapping(payload["tick_distribution"])["row_count"] == 3
    topology = _mapping(payload["temporal_topology"])
    assert topology["computed_from"] == "direct_cache"
    assert topology["timestamp_projection"] == "polars_cache"
    assert topology["cache_source"] == "direct"
    dynamics = _mapping(payload["microstructure_dynamics"])
    assert dynamics["row_order"] == "cache_order"
    assert dynamics["computed_from"] == "direct_cache"
    assert dynamics["cache_source"] == "direct"


def test_direct_cache_topology_inspection_counts_duplicate_timestamps(
    tmp_path: Path,
) -> None:
    """Polars-backed topology should retain bounded duplicate evidence."""
    cache_path = tmp_path / CACHE_FILENAME
    rows = (CLEAN_TICK_ROWS[0], CLEAN_TICK_ROWS[0], CLEAN_TICK_ROWS[1])
    batch = parse_ascii_lines(TICK, rows)
    write_polars_cache(to_polars_frame(batch), cache_path)
    target = QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )

    payload = _fingerprint_payload(_fingerprint_finding(target))
    topology = _mapping(payload["temporal_topology"])
    duplicate = _mapping(
        _mapping(topology["inspection_context"])["duplicate_timestamps"]
    )

    assert topology["computed_from"] == "direct_cache"
    assert topology["duplicate_timestamp_count"] == 1
    assert duplicate["duplicate_row_count"] == 1
    assert _mapping(_list(duplicate["samples"])[0])["occurrence_count"] == 2


def test_fingerprint_rule_prefers_fresh_sibling_cache(tmp_path: Path) -> None:
    """CSV targets should reuse fresh sibling cache data when available."""
    csv_path = write_ascii_case(tmp_path, CLEAN_TICK_CASE)
    cache_path = csv_path.with_name(CACHE_FILENAME)
    batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    csv_mtime_ns = csv_path.stat().st_mtime_ns
    os.utime(
        cache_path, ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000)
    )
    payload = _fingerprint_payload(
        _fingerprint_finding(_discovered_target(csv_path))
    )

    assert _mapping(payload["source"]) == {
        "kind": "cache",
        "cache_source": "sibling",
        "path": ".data",
    }
    assert _mapping(payload["tick_distribution"])["row_count"] == 3
    topology = _mapping(payload["temporal_topology"])
    assert topology["computed_from"] == "fresh_sibling_cache"
    assert topology["cache_source"] == "sibling"


def test_fingerprint_cache_distribution_counts_full_rows_and_fills_sample(
    tmp_path: Path,
) -> None:
    """Cache distributions should count all rows and fill samples from usable rows."""
    import polars as pl

    profile = HistDataFingerprintProfile(max_rows=1)
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
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    payload = _fingerprint_payload(_fingerprint_finding(tick_target, profile))
    distribution = _mapping(payload["tick_distribution"])

    assert distribution["row_count"] == 3
    assert distribution["sampled_row_count"] == 1
    assert distribution["usable_row_count"] == 2
    assert distribution["invalid_row_count"] == 1
    assert distribution["truncated"] is True
    spread_summary = _mapping(distribution["spread"])
    assert spread_summary["count"] == 1
    assert spread_summary["median"] == 0.0002


def test_fingerprint_temporal_topology_reports_tick_duplicate_row(
    tmp_path: Path,
) -> None:
    """Tick duplicate rows should be descriptive fingerprint metadata."""
    payload = _payload_for_case(tmp_path, case_by_name("tick_duplicate_row"))
    topology = _mapping(payload["temporal_topology"])

    assert topology["duplicate_timestamp_count"] == 1
    assert topology["tick_duplicate_row_count"] == 1
    assert _mapping(topology["duplicate_timestamp_source_counts"]) == {
        "tick_duplicate_row": 1
    }
    assert topology["min_interval_ms"] == 0


def test_fingerprint_temporal_topology_reports_expected_weekend_closure(
    tmp_path: Path,
) -> None:
    """Expected FX weekend closures should be topology, not defects."""
    payload = _payload_for_case(tmp_path, _expected_weekend_closure_case())
    topology = _mapping(payload["temporal_topology"])

    assert topology["expected_session_closure_count"] == 1
    assert topology["suspicious_gap_count"] == 0
    assert topology["max_gap_ms"] == 172_800_000
    assert _mapping(topology["gap_bucket_counts"])["gt_1d"] == 1


def test_fingerprint_temporal_topology_reports_suspicious_gap(
    tmp_path: Path,
) -> None:
    """Unexpected large gaps should be summarized without changing severity."""
    finding = _fingerprint_finding(
        _discovered_target(write_ascii_case(tmp_path, _suspicious_gap_case()))
    )
    payload = _fingerprint_payload(finding)
    topology = _mapping(payload["temporal_topology"])

    assert finding.severity is QualitySeverity.INFO
    assert topology["suspicious_gap_count"] == 1
    assert topology["expected_session_closure_count"] == 0
    assert topology["max_gap_ms"] == 600_000
    assert _mapping(topology["gap_bucket_counts"])["gt_5m"] == 1


def test_fingerprint_rule_reports_unsupported_target(tmp_path: Path) -> None:
    """Unsupported targets should emit bounded source metadata, not crash."""
    path = tmp_path / "DAT_ASCII_EURUSD_T_201202.bin"
    path.write_text("not a supported fingerprint source", encoding="utf-8")
    target = QualityTarget(
        path=str(path),
        kind=QualityTargetKind.UNKNOWN,
        data_format="ascii",
        timeframe=TICK,
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
    audit = _mapping(payload["fingerprint_audit"])
    assert _list(audit["sections_expected"]) == TICK_SECTIONS
    assert _list(audit["sections_emitted"]) == ["coverage", "temporal_topology"]
    assert _mapping(audit["section_statuses"])["tick_distribution"] == "skipped"
    assert _mapping(audit["source_status"]) == {
        "kind": "unavailable",
        "readable": False,
        "reason": "unsupported_target_kind",
    }


def test_series_fingerprint_coverage_summary_counts_mixed_sources(
    tmp_path: Path,
) -> None:
    """Run metadata should summarize fingerprint coverage without path data."""
    csv_target = _discovered_target(
        write_ascii_case(tmp_path / "csv", CLEAN_TICK_CASE)
    )
    archive_target = _discovered_target(
        write_zip_case(
            tmp_path / "zip",
            CLEAN_TICK_CASE,
            zip_filename="HISTDATA_COM_ASCII_GBPUSD_T201202.zip",
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
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    sibling_csv_path = write_ascii_case(
        tmp_path / "sibling-cache", CLEAN_TICK_CASE
    )
    sibling_cache_path = sibling_csv_path.with_name(CACHE_FILENAME)
    write_polars_cache(to_polars_frame(tick_batch), sibling_cache_path)
    csv_mtime_ns = sibling_csv_path.stat().st_mtime_ns
    os.utime(
        sibling_cache_path,
        ns=(csv_mtime_ns + 1_000_000, csv_mtime_ns + 1_000_000),
    )
    sibling_cache_target = _discovered_target(sibling_csv_path)
    unsupported_target = QualityTarget(
        path=str(tmp_path / "unsupported.bin"),
        kind=QualityTargetKind.UNKNOWN,
        data_format="ascii",
        timeframe=TICK,
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
    assert summary["discovered_target_count"] == 5
    assert summary["supported_readable_count"] == 4
    assert summary["unavailable_count"] == 1
    assert summary["source_kind_counts"] == {
        "cache": 2,
        "csv_text": 1,
        "unavailable": 1,
        "zip_member": 1,
    }
    assert summary["timeframe_counts"] == {"T": 5}
    assert json_safe_path_strings(summary)


def test_fingerprint_coverage_summary_reports_duplicate_archive_skip(
    tmp_path: Path,
) -> None:
    """Skipped duplicate ZIP fingerprint targets should be visible."""
    csv_target = _discovered_target(write_ascii_case(tmp_path, CLEAN_TICK_CASE))
    archive_target = _discovered_target(
        write_zip_case(
            tmp_path,
            CLEAN_TICK_CASE,
            zip_filename="DAT_ASCII_EURUSD_T_201202.zip",
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
    assert summary["skipped_reason_counts"] == {
        "duplicate_archive_preferred_csv": 1,
    }
    assert summary["source_kind_counts"] == {"csv_text": 1}
    engine = _mapping(report.metadata[QUALITY_ENGINE_METADATA_KEY])
    skips = _mapping(engine["skip_events"])
    assert skips["schema_version"] == QUALITY_SKIP_EVENTS_SCHEMA_VERSION
    assert skips["event_count"] == summary["skipped_fingerprint_target_count"]
    assert skips["reason_counts"] == summary["skipped_reason_counts"]
    assert skips["rule_id_counts"] == {SERIES_FINGERPRINT_RULE_ID: 1}
    assert _mapping(_list(skips["events"])[0])["target_axis"] == {
        "data_format": "ascii",
        "timeframe": "T",
        "symbol": "EURUSD",
        "period": "201202",
        "kind": "zip",
    }


def test_series_fingerprint_distribution_summary_counts_tick_payloads(
    tmp_path: Path,
) -> None:
    """Run metadata should summarize tick distributions and advisories."""
    profile = HistDataFingerprintProfile(max_rows=1)
    clean_tick = _discovered_target(
        write_ascii_case(tmp_path / "clean-tick", CLEAN_TICK_CASE)
    )
    invalid_tick = _discovered_target(
        write_ascii_case(
            tmp_path / "invalid-tick", case_by_name("tick_bad_numeric")
        )
    )
    partial_tick = _discovered_target(
        write_ascii_case(
            tmp_path / "partial-tick", case_by_name("tick_malformed_row")
        )
    )
    empty_tick = _discovered_target(
        write_ascii_case(
            tmp_path / "empty-tick", case_by_name("tick_empty_file")
        )
    )
    tick_spread_mix = _discovered_target(
        write_ascii_case(tmp_path / "tick-spread", _spread_mix_case())
    )
    cache_path = tmp_path / "direct-cache" / CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    cache_target = QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    missing_target = QualityTarget(
        path=str(tmp_path / "missing-distribution.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )
    missing_finding = _synthetic_missing_distribution_finding(missing_target)
    findings = (
        _fingerprint_finding(clean_tick, profile),
        _fingerprint_finding(invalid_tick, profile),
        _fingerprint_finding(partial_tick, profile),
        _fingerprint_finding(empty_tick, profile),
        _fingerprint_finding(tick_spread_mix),
        _fingerprint_finding(cache_target, profile),
        missing_finding,
    )

    summary = _mapping(series_fingerprint_distribution_summary(findings))

    assert summary["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION
    )
    assert summary["target_count"] == 7
    assert summary["tick_distribution_target_count"] == 6
    assert summary["missing_distribution_target_count"] == 1
    assert summary["empty_distribution_target_count"] == 1
    assert summary["invalid_row_target_count"] == 2
    assert summary["partial_row_target_count"] == 1
    assert summary["truncated_distribution_target_count"] == 2
    assert summary["distribution_kind_counts"] == {"missing": 1, "tick": 6}
    assert summary["status_counts"] == {"available": 6, "missing": 1}
    assert json_safe_path_strings(summary)

    attention = _mapping(
        series_fingerprint_distribution_attention_summary(
            findings, target_limit=3
        )
    )
    assert attention["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION
    )
    assert attention["distribution_target_count"] == 7
    assert attention["included_attention_target_count"] == 3
    assert attention["truncated"] is True
    flag_counts = _mapping(attention["attention_flag_counts"])
    assert flag_counts["missing_distribution"] == 1
    assert flag_counts["negative_tick_spreads_present"] == 1
    assert flag_counts["zero_tick_spread_rate_present"] == 1
    assert flag_counts["partial_rows_present"] == 1
    assert (
        _distribution_attention_with_flag(
            series_fingerprint_distribution_attention_summary(
                findings, target_limit=-1
            ),
            "negative_tick_spreads_present",
        )["negative_spread_count"]
        == 1
    )
    assert attention["attention_thresholds"] == (
        HistDataFingerprintDistributionAttentionProfile().to_metadata()
    )


def test_series_fingerprint_topology_summary_reports_actionable_targets(
    tmp_path: Path,
) -> None:
    """Run metadata should summarize topology without requiring JSON spelunking."""
    targets = (
        _discovered_target(
            write_ascii_case(tmp_path / "clean", CLEAN_TICK_CASE)
        ),
        _discovered_target(
            write_ascii_case(
                tmp_path / "duplicate", case_by_name("tick_duplicate_row")
            )
        ),
        _discovered_target(
            write_ascii_case(tmp_path / "gap", _suspicious_gap_case())
        ),
        _discovered_target(
            write_ascii_case(
                tmp_path / "invalid", case_by_name("tick_bad_timestamp")
            )
        ),
        _discovered_target(
            write_ascii_case(
                tmp_path / "non-monotonic",
                case_by_name("tick_non_monotonic_timestamp"),
            )
        ),
        _discovered_target(
            write_ascii_case(
                tmp_path / "weekend-activity", _weekend_activity_case()
            )
        ),
        _discovered_target(
            write_ascii_case(
                tmp_path / "expected-closure",
                _expected_weekend_closure_case(),
            )
        ),
        _cache_target(tmp_path / "direct-cache"),
        QualityTarget(
            path=str(tmp_path / "unsupported.bin"),
            kind=QualityTargetKind.UNKNOWN,
            data_format="ascii",
            timeframe=TICK,
            symbol="EURUSD",
            period="201202",
        ),
    )
    report = run_quality_assessment(
        targets, quality_rules_for_groups(("fingerprint",))
    )
    summary = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY]
    )

    assert summary == series_fingerprint_topology_summary(report.findings)
    assert summary["schema_version"] == (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION
    )
    assert summary["target_count"] == 9
    assert summary["status_counts"] == {
        "irregular": 5,
        "regular": 3,
        "unavailable": 1,
    }
    assert summary["flag_counts"]["duplicate_timestamps"] == 1
    assert summary["flag_counts"]["expected_session_closures"] == 1
    assert summary["flag_counts"]["weekend_activity"] == 1
    target_summaries = _list(summary["target_summaries"])
    assert (
        _target_summary_with_flag(target_summaries, "duplicate_timestamps")[
            "duplicate_timestamp_count"
        ]
        == 1
    )
    assert (
        _target_summary_with_flag(target_summaries, "suspicious_gaps")[
            "max_gap_ms"
        ]
        == 600_000
    )
    assert (
        _target_summary_with_flag(
            target_summaries, "expected_session_closures"
        )["expected_session_closure_count"]
        == 1
    )

    attention = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY]
    )
    assert attention == series_fingerprint_topology_attention_summary(
        report.findings
    )
    assert attention["schema_version"] == (
        TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION
    )
    assert attention["topology_target_count"] == 9
    assert attention["attention_target_count"] == 6
    assert attention["attention_flag_counts"]["duplicate_timestamps"] == 1
    assert _remediation_hint_codes(
        _target_summary_with_flag(
            _list(attention["target_summaries"]),
            "invalid_timestamps",
        )
    ) == ("inspect_invalid_timestamp_rows",)
    assert json_safe_path_strings(attention)
    assert json_safe_path_strings(summary)


def test_series_fingerprint_topology_attention_orders_mixed_remediation_hints(
    tmp_path: Path,
) -> None:
    """Mixed flags should carry stable hint codes in attention-flag order."""
    target = QualityTarget(
        path=str(tmp_path / "DAT_ASCII_EURUSD_T_201202_MIXED.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe=TICK,
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
                    "timeframe": "T",
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


def test_topology_inspection_context_links_bounded_evidence_to_next_actions(
    tmp_path: Path,
) -> None:
    """Attention evidence should link to stable run-level action identities."""
    case = HistDataAsciiCase(
        name="tick_topology_inspection",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_INSPECTION.csv",
        rows=(
            "20120203 165900000,1.306600,1.306770,0",
            "bad-timestamp,1.306600,1.306770,0",
            "20120205 170100000,1.306570,1.306740,17",
            "20120205 172000000,1.306580,1.306750,18",
            "20120205 172000000,1.306580,1.306750,18",
            "20120205 171000000,1.306590,1.306760,19",
        ),
    )
    target = _discovered_target(write_ascii_case(tmp_path, case))
    report = run_quality_assessment(
        (target,),
        quality_rules_for_groups(
            ("fingerprint",),
            profile={
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "inspection-limit",
                "rules": {
                    SERIES_FINGERPRINT_RULE_ID: {
                        "topology_inspection_sample_limit": 1,
                    }
                },
            },
        ),
    )
    attention = _mapping(
        report.metadata[TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY]
    )
    target_summary = _mapping(_list(attention["target_summaries"])[0])
    context = _mapping(target_summary["inspection_context"])
    next_actions = _mapping(quality_next_actions_summary(report))
    action_codes = {
        _mapping(action)["code"] for action in _list(next_actions["actions"])
    }

    expected_links = {
        "invalid_timestamps": "inspect_invalid_timestamp_rows",
        "non_monotonic_timestamps": "repair_timestamp_order",
        "duplicate_timestamps": "inspect_duplicate_timestamp_rows",
        "suspicious_gaps": "inspect_gap_boundaries",
    }
    for section_name, action_code in expected_links.items():
        section = _mapping(context[section_name])
        next_action = _mapping(section["next_action"])
        assert section["actionable"] is True
        assert next_action["code"] == action_code
        assert next_action["rule_id"] == SERIES_FINGERPRINT_RULE_ID
        assert next_action["flag"] == section_name
        assert _mapping(section["target_axis"])["symbol"] == "EURUSD"
        assert action_code in action_codes
        assert section["included_count"] <= 1
    closure = _mapping(context["expected_session_closures"])
    assert closure["actionable"] is False
    assert closure["contextual_for"] == "suspicious_gaps"
    assert "next_action" not in closure
    assert str(tmp_path) not in str(context)
    assert "duplicate_row_values" not in str(context)


def test_series_fingerprint_topology_attention_ignores_context_only_targets(
    tmp_path: Path,
) -> None:
    """Expected closures alone should not create attention targets."""
    clean_target = _discovered_target(
        write_ascii_case(tmp_path / "clean", CLEAN_TICK_CASE)
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
    assert attention["target_summaries"] == []


def test_fingerprint_id_excludes_source_path_volatility(tmp_path: Path) -> None:
    """Identical content and target axis should hash the same across paths."""
    first = write_ascii_case(
        tmp_path / "first",
        HistDataAsciiCase(
            name="first_copy",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_FIRST.csv",
            rows=CLEAN_TICK_ROWS,
        ),
    )
    second = write_ascii_case(
        tmp_path / "second",
        HistDataAsciiCase(
            name="second_copy",
            timeframe=TICK,
            filename="DAT_ASCII_EURUSD_T_201202_SECOND.csv",
            rows=CLEAN_TICK_ROWS,
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
    """The schema surface should expose stable public identifiers."""
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
        TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-stationarity.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-decomposition.v1"
    )
    assert (
        TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION
        == "histdatacom.time-series-fingerprint-decomposition-training-projection.v1"
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


def _payload_for_case(
    directory: Path,
    case: HistDataAsciiCase,
    profile: HistDataFingerprintProfile | None = None,
) -> dict[str, Any]:
    target = _discovered_target(write_ascii_case(directory, case))
    return _fingerprint_payload(_fingerprint_finding(target, profile))


def _cross_series_case(
    symbol: str,
    rows: tuple[str, ...],
    *,
    period: str = "201202",
) -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name=f"cross_series_{symbol.lower()}",
        timeframe=TICK,
        filename=f"DAT_ASCII_{symbol}_T_{period}.csv",
        rows=rows,
    )


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


def _fingerprint_payload(finding: QualityFinding) -> dict[str, Any]:
    payload = finding.metadata[TIME_SERIES_FINGERPRINT_METADATA_KEY]
    assert isinstance(payload, dict)
    return payload


def _mapping(value: Any) -> dict[str, Any]:
    assert isinstance(value, dict)
    return value


def _list(value: Any) -> list[Any]:
    assert isinstance(value, list)
    return value


def _retired_bar_schema_keys(value: Any) -> set[str]:
    matches: set[str] = set()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower()
            if (
                "m1" in normalized
                or "ohlc" in normalized
                or normalized.startswith("bar_")
            ):
                matches.add(str(key))
            matches.update(_retired_bar_schema_keys(nested))
    elif isinstance(value, list):
        for nested in value:
            matches.update(_retired_bar_schema_keys(nested))
    return matches


def _tick_case_from_mid_prices(
    name: str,
    mid_prices: tuple[float, ...],
) -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name=name,
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202.csv",
        rows=tuple(
            (
                "20120201 "
                f"{index // 3600:02d}{(index // 60) % 60:02d}{index % 60:02d}"
                f"000,{price:.6f},{price:.6f},0"
            )
            for index, price in enumerate(mid_prices)
        ),
    )


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


def _spread_mix_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="tick_spread_mix",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_SPREAD_MIX.csv",
        rows=(
            "20120201 000003660,1.000000,1.000000,0",
            "20120201 000003973,1.000200,1.000100,0",
            "20120201 000004990,1.000000,1.000300,0",
        ),
    )


def _expected_weekend_closure_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="tick_expected_weekend_closure",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_WEEKEND.csv",
        rows=(
            "20120203 170000000,1.306600,1.306770,0",
            "20120205 170000000,1.306570,1.306740,17",
        ),
    )


def _suspicious_gap_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="tick_suspicious_gap",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_GAP.csv",
        rows=(
            "20120201 000000000,1.306600,1.306770,0",
            "20120201 001000000,1.306570,1.306740,17",
        ),
    )


def _weekend_activity_case() -> HistDataAsciiCase:
    return HistDataAsciiCase(
        name="tick_weekend_activity",
        timeframe=TICK,
        filename="DAT_ASCII_EURUSD_T_201202_WEEKEND_ACTIVITY.csv",
        rows=("20120204 120000000,1.306600,1.306770,0",),
    )


def _cache_target(directory: Path) -> QualityTarget:
    cache_path = directory / CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    batch = parse_ascii_lines(TICK, CLEAN_TICK_ROWS)
    write_polars_cache(to_polars_frame(batch), cache_path)
    return QualityTarget(
        path=str(cache_path),
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe=TICK,
        symbol="EURUSD",
        period="201202",
    )


def _synthetic_missing_distribution_finding(
    target: QualityTarget,
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.INFO,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="Canonical target time-series fingerprint.",
        rule_id=SERIES_FINGERPRINT_RULE_ID,
        target=target,
        metadata={
            TIME_SERIES_FINGERPRINT_METADATA_KEY: {
                "target_axis": {
                    "data_format": "ascii",
                    "timeframe": "T",
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
                    "path": str(target.path),
                },
            }
        },
    )


def _target_summary_with_flag(targets: list[Any], flag: str) -> dict[str, Any]:
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


def _remediation_hint_codes(target: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(_mapping(hint)["code"])
        for hint in _list(_mapping(target)["remediation_hints"])
    )


def json_safe_path_strings(value: Any) -> bool:
    encoded = str(value)
    return "/Users/" not in encoded and str(Path.home()) not in encoded

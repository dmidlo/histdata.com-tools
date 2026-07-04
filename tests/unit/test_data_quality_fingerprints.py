"""Tests for deterministic data-quality fingerprint plumbing."""

from __future__ import annotations

from histdatacom.data_quality import (
    DEFAULT_FINGERPRINT_HISTOGRAM_BINS,
    DEFAULT_FINGERPRINT_LAGS,
    DEFAULT_FINGERPRINT_MAX_ROWS,
    DEFAULT_FINGERPRINT_QUANTILES,
    DEFAULT_FINGERPRINT_ROLLING_WINDOWS,
    DEFAULT_FINGERPRINT_ROUNDING_DIGITS,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    HistDataSeriesFingerprintRule,
    QualityTarget,
    QualityTargetKind,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
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


def test_fingerprint_rule_is_noop_until_payload_issue() -> None:
    """The structural rule should not emit synthetic placeholder payloads."""
    rule = quality_rules_for_groups(("fingerprint",))[0]
    target = QualityTarget(
        path="DAT_ASCII_EURUSD_M1_201202.csv",
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )

    assert tuple(rule.evaluate(target)) == ()


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

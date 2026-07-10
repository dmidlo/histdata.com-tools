"""ASCII tick training feature substrate tests."""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import pytest

from histdatacom.activity_stages import create_cache_file
from histdatacom.data_quality import (
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.training_features import (
    DEFAULT_SUSPICIOUS_TICK_GAP_MS,
    QUALITY_ISSUE_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    enrich_tick_cache_with_training_features,
    quality_report_from_training_features,
    required_training_feature_columns,
    training_feature_definitions,
)
from histdatacom.histdata_ascii import CACHE_FILENAME, read_polars_cache
from histdatacom.records import Record

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"


def test_enriched_tick_frame_contains_flat_row_aligned_training_schema() -> (
    None
):
    """Training rows should need no side joins or report parsing."""
    frame = _tick_frame(
        [
            (1_000, 1.0000, 1.0002, 0),
            (1_300, 1.1000, 1.1002, 1),
        ]
    )

    enriched = enrich_tick_cache_with_training_features(
        frame,
        symbol="EURUSD",
        data_format="ascii",
        timeframe="T",
        period="201202",
    )
    rows = enriched.to_dicts()

    assert set(required_training_feature_columns()).issubset(enriched.columns)
    assert enriched.height == frame.height
    assert enriched.get_column("bid").to_list() == [1.0, 1.1]
    assert enriched.get_column("ask").to_list() == [1.0002, 1.1002]
    assert enriched.get_column("spread").to_list() == pytest.approx(
        [0.0002, 0.0002]
    )
    assert enriched.get_column("mid").to_list() == pytest.approx(
        [1.0001, 1.1001]
    )
    assert [row["row_id"] for row in rows] == [1, 2]
    assert [row["source_row_number"] for row in rows] == [1, 2]
    assert [row["event_seq"] for row in rows] == [1, 2]
    assert {row["series_id"] for row in rows} == {"ascii:T:EURUSD:histdata.com"}
    assert {row["period"] for row in rows} == {"201202"}
    assert rows[0]["timestamp_utc_ms"] == 1_000
    assert rows[0]["training_schema_version"] == TRAINING_SCHEMA_VERSION
    assert all(row["training_usable"] for row in rows)
    assert all(row["class_training_action_code"] == 0 for row in rows)
    assert all(row["synth_bid"] is None for row in rows)
    assert all(row["synth_ask"] is None for row in rows)

    masked = enriched.with_columns(
        pl.lit(None).cast(pl.Int64).alias("timestamp_utc_ms")
    )
    assert masked.select(["series_id", "period", "row_id"]).to_dicts() == (
        enriched.select(["series_id", "period", "row_id"]).to_dicts()
    )


def test_training_feature_issues_drive_deterministic_classification() -> None:
    """Issue columns should explain row state directly."""
    frame = _tick_frame(
        [
            (1_000, 1.0, 1.2, 0),
            (1_000, 1.3, 1.2, 1),
            (900, 1.0, 1.1, 2),
            (DEFAULT_SUSPICIOUS_TICK_GAP_MS + 1_000, 1.0, 1.1, 3),
        ]
    )

    enriched = enrich_tick_cache_with_training_features(
        frame,
        symbol="EURUSD",
        data_format="ascii",
        timeframe="T",
        period="201202",
    )
    rows = enriched.to_dicts()

    assert rows[0]["dq_issue_duplicate_timestamp"]
    assert rows[1]["dq_issue_duplicate_timestamp"]
    assert rows[1]["dq_issue_negative_spread"]
    assert rows[1]["class_spread_regime_code"] == 2
    assert rows[1]["training_usable"] is False
    assert rows[1]["class_training_action_code"] == 4
    assert rows[2]["dq_issue_non_monotonic_timestamp"]
    assert rows[2]["class_gap_state_code"] == 3
    assert rows[3]["dq_issue_suspicious_gap"]
    assert rows[3]["dq_issue_gap_after_previous"]
    assert rows[3]["class_gap_state_code"] == 2
    assert {row["period_negative_spread_rate"] for row in rows} == {0.25}
    assert {row["period_duplicate_timestamp_count"] for row in rows} == {2}
    assert {row["period_suspicious_gap_count"] for row in rows} == {1}
    assert all(row["row_id"] for row in rows)


def test_training_report_derives_issue_counts_from_enriched_columns() -> None:
    """Quality reports should be downstream audit artifacts."""
    target = QualityTarget(
        path="/tmp/data/ASCII/T/eurusd/2012/02/.data",
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    enriched = enrich_tick_cache_with_training_features(
        _tick_frame([(1_000, 1.0, 1.2, 0), (1_300, 1.4, 1.3, 1)]),
        target=target,
    )

    report = quality_report_from_training_features(enriched, target=target)

    assert report.status is QualityStatus.FAILED
    [finding] = [
        item
        for item in report.findings
        if item.code == "DQ_ISSUE_NEGATIVE_SPREAD"
    ]
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["issue_column"] == "dq_issue_negative_spread"
    assert finding.metadata["row_count"] == 1
    assert report.metadata["issue_counts"]["dq_issue_negative_spread"] == 1


def test_training_feature_registry_is_the_schema_catalog() -> None:
    """Required training-facing columns should be defined in one registry."""
    definitions = training_feature_definitions()
    names = [definition.name for definition in definitions]

    assert len(names) == len(set(names))
    assert set(required_training_feature_columns()).issubset(names)
    assert set(QUALITY_ISSUE_COLUMNS).issubset(names)


def test_training_enrichment_rejects_non_tick_training_inputs() -> None:
    """The initial training substrate is intentionally ASCII tick only."""
    frame = _tick_frame([(1_000, 1.0, 1.2, 0)])

    with pytest.raises(ValueError, match="ASCII tick"):
        enrich_tick_cache_with_training_features(
            frame,
            symbol="EURUSD",
            data_format="ascii",
            timeframe="M1",
            period="201202",
        )

    with pytest.raises(ValueError, match="ASCII tick"):
        enrich_tick_cache_with_training_features(
            frame,
            symbol="EURUSD",
            data_format="ninjatrader",
            timeframe="T",
            period="201202",
        )


def test_training_report_rejects_unsupported_target_dimensions() -> None:
    """Direct report derivation must not bypass training input validation."""
    enriched = enrich_tick_cache_with_training_features(
        _tick_frame([(1_000, 1.0, 1.2, 0)]),
        symbol="EURUSD",
        data_format="ascii",
        timeframe="T",
        period="201202",
    )
    target = QualityTarget(
        path="/tmp/retired.csv",
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )

    with pytest.raises(ValueError, match="ASCII tick"):
        quality_report_from_training_features(enriched, target=target)


def test_cache_build_writes_enriched_ascii_tick_data_as_canonical_cache(
    tmp_path: Path,
) -> None:
    """The application cache builder should write enriched training rows."""
    source = FIXTURES / "DAT_ASCII_EURUSD_T_201202.csv"
    target = tmp_path / source.name
    shutil.copyfile(source, target)
    record = Record(
        data_dir=str(tmp_path),
        csv_filename=source.name,
        data_format="ascii",
        data_timeframe="T",
        data_fxpair="eurusd",
        data_datemonth="201202",
    )

    create_cache_file(record, {})

    cache = read_polars_cache(tmp_path / CACHE_FILENAME)
    assert set(required_training_feature_columns()).issubset(cache.columns)
    assert cache.height == 3
    assert (
        cache.get_column("series_id").to_list()
        == ["ascii:T:EURUSD:histdata.com"] * 3
    )
    assert cache.get_column("row_id").to_list() == [1, 2, 3]
    assert (
        cache.get_column("training_schema_version").to_list()
        == [TRAINING_SCHEMA_VERSION] * 3
    )


def _tick_frame(rows: list[tuple[int, float, float, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "datetime": [row[0] for row in rows],
            "bid": [row[1] for row in rows],
            "ask": [row[2] for row in rows],
            "vol": [row[3] for row in rows],
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    )

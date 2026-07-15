"""Tests for active-time multivariate technological feed epochs."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from histdatacom.data_analytics import (
    FEED_EPOCH_DEFINITION_V2_SCHEMA_VERSION,
    FeedEpochEvidenceV2,
    FeedEpochFitConfigV2,
    analyze_active_time_feed_epochs,
    fit_active_time_feed_epochs,
    read_active_time_feed_epoch_definition,
    scan_active_time_evidence,
    write_active_time_feed_epoch_campaign,
)
from histdatacom.data_analytics.cli import main as analytics_main
from histdatacom.data_analytics.feed_epochs_v2 import _pelt_boundaries
from histdatacom.synthetic.observation import ObservationFitEvidenceV1


def _timestamp(period: str, day: int = 6) -> int:
    return int(
        datetime(
            int(period[:4]),
            int(period[4:]),
            day,
            tzinfo=timezone.utc,
        ).timestamp()
        * 1000
    )


def _cache(
    root: Path,
    symbol: str,
    period: str,
    *,
    dense: bool,
) -> Path:
    start = _timestamp(period)
    step = 100 if dense else 2_000
    rows = 120 if dense else 24
    path = (
        root
        / "ASCII"
        / "T"
        / symbol.lower()
        / period[:4]
        / str(int(period[4:]))
        / ".data"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    bids = [1.1 + (index % 7) * 0.00001 for index in range(rows)]
    frame = pl.DataFrame(
        {
            "datetime": [start + index * step for index in range(rows)],
            "bid": bids,
            "ask": [value + (0.0001 if dense else 0.0005) for value in bids],
            "vol": [0] * rows,
        }
    ).with_columns(pl.col("vol").cast(pl.Int32))
    frame.write_ipc(path)
    return path


def _evidence(
    symbol: str,
    index: int,
    *,
    value: float,
    omit_duplicate: bool = False,
) -> FeedEpochEvidenceV2:
    year = 2020 + index // 12
    month = index % 12 + 1
    period = f"{year}{month:02d}"
    features = {
        "log_calendar_tick_rate_per_hour": value,
        "duplicate_timestamp_rate": value / 100.0,
    }
    if omit_duplicate:
        del features["duplicate_timestamp_rate"]
    return FeedEpochEvidenceV2(
        symbol=symbol,
        period=period,
        source_path=f"fixture/{symbol}/{period}/.data",
        source_artifact_sha256="sha256:"
        + hashlib.sha256(f"{symbol}:{period}".encode()).hexdigest(),
        source_size_bytes=100,
        start_timestamp_utc_ms=_timestamp(period),
        end_timestamp_utc_ms=_timestamp(period) + 86_400_000,
        row_count=100,
        denominators_ms={
            "calendar_duration_ms": 1_000,
            "market_open_duration_ms": 800,
            "active_window_duration_ms": 500,
        },
        counts={"transition_count": 99},
        feature_values=features,
        feature_provenance={name: ("fixture",) for name in features},
        activity_bin_counts={str(index): 100, str(index + 1): 50},
        calendar_policy={"active_time_policy_version": "fixture.v1"},
    )


def _fit_config(**changes) -> FeedEpochFitConfigV2:
    values = {
        "feature_names": (
            "log_calendar_tick_rate_per_hour",
            "duplicate_timestamp_rate",
        ),
        "min_evidence_periods": 12,
        "min_segment_periods": 4,
        "min_feature_coverage": 0.75,
        "penalty_multiplier": 0.5,
        "min_boundary_support": 0.5,
    }
    values.update(changes)
    return FeedEpochFitConfigV2(**values)


def test_cache_scan_uses_explicit_denominators_and_real_source_hash(
    tmp_path: Path,
) -> None:
    """Rates must name their calendar, open, and observed-active bases."""
    path = _cache(tmp_path, "EURUSD", "202001", dense=True)

    evidence = scan_active_time_evidence(
        path,
        symbol="EURUSD",
        period="202001",
        config=_fit_config(),
    )

    assert evidence.row_count == 120
    assert set(evidence.denominators_ms) == {
        "calendar_duration_ms",
        "market_open_duration_ms",
        "active_window_duration_ms",
    }
    assert (
        evidence.denominators_ms["calendar_duration_ms"] == 31 * 24 * 3_600_000
    )
    assert (
        evidence.denominators_ms["market_open_duration_ms"]
        < evidence.denominators_ms["calendar_duration_ms"]
    )
    assert evidence.feature_values[
        "log_calendar_tick_rate_per_hour"
    ] == pytest.approx(
        math.log1p(120 * 3_600_000 / (31 * 24 * 3_600_000)),
        abs=1e-10,
    )
    assert evidence.counts["market_open_row_count"] == 120
    assert evidence.counts["active_window_interval_count"] == 119
    assert evidence.feature_values[
        "log_market_open_tick_rate_per_hour"
    ] == pytest.approx(
        math.log1p(
            evidence.counts["market_open_row_count"]
            * 3_600_000
            / evidence.denominators_ms["market_open_duration_ms"]
        ),
        abs=1e-10,
    )
    assert evidence.feature_values[
        "log_active_window_tick_rate_per_hour"
    ] == pytest.approx(
        math.log1p(
            evidence.counts["active_window_interval_count"]
            * 3_600_000
            / evidence.denominators_ms["active_window_duration_ms"]
        ),
        abs=1e-10,
    )
    payload = evidence.to_dict()
    del payload["counts"]["active_window_interval_count"]
    payload["evidence_id"] = ""
    with pytest.raises(ValueError, match="missing rate numerator"):
        FeedEpochEvidenceV2.from_dict(payload)
    assert evidence.source_artifact_sha256 == (
        "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    )
    assert evidence.calendar_policy["active_time_policy_version"]


def test_feature_extraction_is_bounded_and_deterministic(
    tmp_path: Path,
) -> None:
    """A repeated scan must preserve compact identities and feature values."""
    path = _cache(tmp_path, "EURUSD", "202001", dense=True)

    left = scan_active_time_evidence(
        path, symbol="EURUSD", period="202001", config=_fit_config()
    )
    right = scan_active_time_evidence(
        path, symbol="EURUSD", period="202001", config=_fit_config()
    )

    assert left == right
    assert len(left.activity_bin_counts) <= 800
    assert left.feature_values["price_precision_digits"] == 5
    assert left.feature_values["joint_move_rate"] > 0.9
    assert 0 <= left.feature_values["timestamp_last_digit_entropy"] <= 1
    assert (
        sum(
            count
            for name, count in left.counts.items()
            if name.startswith("timestamp_last_digit_")
        )
        == left.row_count
    )
    assert left.feature_values["session_activity_share_asia"] == 1.0


def test_stale_run_summary_crosses_arrow_record_batch_boundaries(
    tmp_path: Path,
) -> None:
    """Unchanged-run lengths must survive cache record-batch boundaries."""
    path = _cache(tmp_path, "EURUSD", "202001", dense=True)
    start = _timestamp("202001")
    prices = [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 3.0]
    chunks = []
    for offset, values in ((0, prices[:2]), (2, prices[2:])):
        chunks.append(
            pl.DataFrame(
                {
                    "datetime": [
                        start + (offset + index) * 100
                        for index in range(len(values))
                    ],
                    "bid": values,
                    "ask": [value + 0.0001 for value in values],
                    "vol": [0] * len(values),
                }
            ).with_columns(pl.col("vol").cast(pl.Int32))
        )
    pl.concat(chunks, rechunk=False).write_ipc(path)

    evidence = scan_active_time_evidence(
        path, symbol="EURUSD", period="202001", config=_fit_config()
    )

    assert evidence.counts["stale_run_count"] == 3
    assert evidence.counts["stale_run_p95"] == 3
    assert evidence.counts["stale_run_max"] == 3
    assert evidence.feature_values["log_stale_run_p95"] == pytest.approx(
        math.log1p(3), abs=1e-10
    )


@given(
    boundary=st.integers(min_value=4, max_value=20),
    left=st.floats(min_value=-5, max_value=-1, allow_nan=False),
    right=st.floats(min_value=1, max_value=5, allow_nan=False),
)
@settings(max_examples=30, deadline=None)
def test_pelt_recovers_controlled_single_boundary(
    boundary: int, left: float, right: float
) -> None:
    """The exact PELT objective should recover a strong supported shift."""
    count = boundary + 8
    rows = tuple(
        (left, left / 2) if index < boundary else (right, right / 2)
        for index in range(count)
    )

    detected, gain = _pelt_boundaries(rows, min_segment=4, penalty=0.5)

    assert detected == (boundary,)
    assert gain > 0


def test_pelt_respects_minimum_segment_and_missing_features() -> None:
    """Short excursions and absent cells must not create illegal segments."""
    rows = (
        *((0.0, None),) * 8,
        *((9.0, None),) * 2,
        *((0.0, None),) * 8,
        *((8.0, 1.0),) * 8,
    )

    detected, _gain = _pelt_boundaries(rows, min_segment=6, penalty=1.0)

    assert all(
        right - left >= 6
        for left, right in zip((0, *detected), (*detected, len(rows)))
    )
    assert 8 not in detected and 10 not in detected


def test_panel_fit_recovers_shared_epochs_and_symbol_deviation() -> None:
    """Shared boundaries and a later one-symbol shift stay separate."""
    evidence = []
    for symbol in ("EURUSD", "GBPUSD", "EURGBP"):
        for index in range(36):
            value = 1.0 if index < 12 else (9.0 if index < 24 else 15.0)
            if symbol == "GBPUSD" and index >= 30:
                value += 10.0
            evidence.append(_evidence(symbol, index, value=value))

    definition = fit_active_time_feed_epochs(evidence, config=_fit_config())

    assert [item.right_period for item in definition.boundaries] == [
        "202101",
        "202201",
    ]
    assert definition.stability.status == "pass"
    assert definition.valid_for_observation_models
    assert any(
        item.symbol == "GBPUSD" and item.right_period == "202207"
        for item in definition.symbol_deviations
    )
    assert definition.lineage["algorithm"] == "robust_multivariate_pelt"
    assert definition.schema_version == FEED_EPOCH_DEFINITION_V2_SCHEMA_VERSION
    projected = ObservationFitEvidenceV1.from_feed_epoch_evidence(
        evidence[0], definition
    )
    assert projected.evidence_kind == "active_time_feed_epoch"


def test_duplicate_only_candidate_is_rejected_by_family_holdout() -> None:
    """A boundary that vanishes without duplicates cannot enter the definition."""
    evidence = []
    for symbol in ("EURUSD", "GBPUSD", "EURGBP"):
        for index in range(24):
            item = _evidence(symbol, index, value=1.0)
            features = dict(item.feature_values)
            features["duplicate_timestamp_rate"] = 0.0 if index < 12 else 0.8
            evidence.append(
                replace(
                    item,
                    feature_values=features,
                    evidence_id="",
                )
            )

    definition = fit_active_time_feed_epochs(evidence, config=_fit_config())

    assert not definition.boundaries
    assert definition.stability.status == "fail"
    rejected = definition.stability.rejected_candidates["202101"]
    assert rejected["support_by_family"]["duplicate_policy"] == 0


def test_common_period_and_feature_coverage_fail_closed() -> None:
    """Missing axes reduce common support and unsupported features are omitted."""
    evidence = [
        _evidence(
            symbol,
            index,
            value=1.0 if index < 6 else 8.0,
            omit_duplicate=index % 3 == 0,
        )
        for symbol in ("EURUSD", "GBPUSD", "EURGBP")
        for index in range(12)
        if not (symbol == "EURGBP" and index == 3)
    ]
    config = _fit_config(
        min_evidence_periods=12,
        min_segment_periods=4,
        min_feature_coverage=0.9,
    )

    definition = fit_active_time_feed_epochs(evidence, config=config)

    assert definition.period_count == 11
    assert definition.feature_names == ("log_calendar_tick_rate_per_hour",)
    assert definition.stability.status == "fail"
    assert "insufficient_common_period_support" in definition.stability.reasons


def test_observation_projection_rejects_unstable_v2_definition() -> None:
    """Downstream fitting must not treat failed v2 definitions as valid."""
    evidence = [
        _evidence(symbol, index, value=1.0)
        for symbol in ("EURUSD", "GBPUSD", "EURGBP")
        for index in range(12)
    ]
    definition = fit_active_time_feed_epochs(evidence, config=_fit_config())
    assert not definition.valid_for_observation_models

    with pytest.raises(ValueError, match="has not passed stability"):
        ObservationFitEvidenceV1.from_feed_epoch_evidence(
            evidence[0], definition
        )


def test_campaign_scans_all_discovered_caches_and_writes_hashed_artifacts(
    tmp_path: Path,
) -> None:
    """The real execution path must publish definition/evidence/run sidecars."""
    periods = ("202001", "202002", "202003", "202004")
    for symbol in ("EURUSD", "GBPUSD", "EURGBP"):
        for index, period in enumerate(periods):
            _cache(tmp_path, symbol, period, dense=index >= 2)
    config = _fit_config(
        min_evidence_periods=4,
        min_segment_periods=2,
        min_boundary_support=0.0,
        penalty_multiplier=0.05,
    )

    campaign = analyze_active_time_feed_epochs([tmp_path], config=config)
    artifacts = write_active_time_feed_epoch_campaign(
        campaign, tmp_path / "artifacts"
    )

    assert campaign.source_count == 12
    assert campaign.definition.symbols == ("EURGBP", "EURUSD", "GBPUSD")
    assert set(artifacts) == {"campaign", "definition", "evidence"}
    for artifact in artifacts.values():
        path = Path(artifact.path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact.sha256
    definition = json.loads(Path(artifacts["definition"].path).read_text())
    assert definition["definition_id"] == campaign.definition.definition_id
    assert (
        read_active_time_feed_epoch_definition(artifacts["definition"].path)
        == campaign.definition
    )
    definition["period_count"] += 1
    tampered = tmp_path / "tampered-definition.json"
    tampered.write_text(json.dumps(definition), encoding="utf-8")
    with pytest.raises(ValueError, match="definition_id"):
        read_active_time_feed_epoch_definition(tampered)


def test_v2_cli_runs_the_cache_campaign(tmp_path: Path, capsys) -> None:
    """The existing analytics CLI must expose the complete v2 execution path."""
    periods = ("202001", "202002", "202003", "202004")
    for symbol in ("EURUSD", "GBPUSD", "EURGBP"):
        for index, period in enumerate(periods):
            _cache(tmp_path, symbol, period, dense=index >= 2)
    output = tmp_path / "artifacts"

    exit_code = analytics_main(
        [
            "feed-epochs-v2",
            "--target",
            str(tmp_path / "ASCII" / "T"),
            "--artifact-dir",
            str(output),
            "--features",
            "log_calendar_tick_rate_per_hour",
            "duplicate_timestamp_rate",
            "--min-evidence-periods",
            "4",
            "--min-segment-periods",
            "2",
            "--min-boundary-support",
            "0",
            "--penalty-multiplier",
            "0.05",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_count"] == 12
    assert payload["definition"]["schema_version"].endswith(".v2")
    assert Path(payload["artifacts"]["definition"]["path"]).exists()

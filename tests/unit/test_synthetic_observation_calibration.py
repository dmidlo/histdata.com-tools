"""Tests for real-evidence observation calibration and holdout gates."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from pathlib import Path

import polars as pl
import pytest

from histdatacom.data_analytics import (
    FeedEpochDefinitionV2,
    FeedEpochEvidenceV2,
    FeedEpochFitConfigV2,
    FeedEpochIntervalV2,
    FeedEpochStabilityV2,
)
from histdatacom.synthetic import (
    BenchmarkSplitKind,
    ObservationCalibrationCampaignV2,
    ObservationCalibrationProfileV2,
    ObservationContextV1,
    ObservationFitEvidenceV1,
    ObservationInputEventV1,
    ObservationOperatorFitConfigV1,
    ReconstructionWindowV1,
    calibrate_historical_observation_operators,
    estimate_paired_observation_evidence,
    fit_observation_operator,
    read_observation_calibration_campaign,
    write_observation_calibration_campaign,
)

SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")


def _month_start(period: str) -> int:
    return int(
        datetime(
            int(period[:4]),
            int(period[4:]),
            1,
            tzinfo=timezone.utc,
        ).timestamp()
        * 1000
    )


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _definition() -> FeedEpochDefinitionV2:
    start = _month_start("201901")
    end = _month_start("202301") - 1
    config = FeedEpochFitConfigV2()
    return FeedEpochDefinitionV2(
        config=config,
        symbols=SYMBOLS,
        coverage_start_utc_ms=start,
        coverage_end_utc_ms=end,
        evidence_count=144,
        period_count=48,
        feature_names=config.feature_names,
        boundaries=(),
        epochs=(
            FeedEpochIntervalV2(
                label="technology_epoch_01",
                period_start="201901",
                period_end="202212",
                start_timestamp_utc_ms=start,
                end_timestamp_utc_ms=end,
                evidence_count=144,
                feature_medians={name: 0.0 for name in config.feature_names},
            ),
        ),
        symbol_deviations=(),
        stability=FeedEpochStabilityV2(
            status="pass",
            reasons=(),
            run_count=1,
            run_counts={"fixture": 1},
            boundary_support={},
            boundary_support_by_family={},
            rejected_candidates={},
            feature_coverage={name: 1.0 for name in config.feature_names},
            common_period_count=48,
            symbol_count=3,
        ),
        lineage={"fixture": True},
    )


def _tick_cache(path: Path) -> str:
    start = _month_start("202001")
    rows = 24 * 60 * 4
    bids = [1.1 + (index % 11) * 0.00001 for index in range(rows)]
    frame = pl.DataFrame(
        {
            "datetime": [start + index * 15_000 for index in range(rows)],
            "bid": bids,
            "ask": [value + 0.0001 for value in bids],
            "vol": [0] * rows,
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_ipc(path)
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _epoch_evidence(
    *,
    symbol: str,
    period: str,
    path: Path,
    source_hash: str,
    row_count: int = 5760,
) -> FeedEpochEvidenceV2:
    start = _month_start(period)
    features = {
        "log_active_window_tick_rate_per_hour": 7.0,
        "timestamp_exact_second_rate": 1.0,
        "timestamp_last_digit_entropy": 0.0,
        "price_precision_digits": 5.0,
        "duplicate_timestamp_rate": 0.1,
        "log_interarrival_median_ms": 5.0,
        "bid_only_rate": 0.0,
        "ask_only_rate": 0.0,
        "joint_move_rate": 1.0,
        "unchanged_rate": 0.0,
        "session_activity_share_asia": 0.25,
        "session_activity_share_london": 0.25,
        "session_activity_share_new_york": 0.25,
        "session_activity_share_off_session": 0.25,
    }
    return FeedEpochEvidenceV2(
        symbol=symbol,
        period=period,
        source_path=str(path),
        source_artifact_sha256=source_hash,
        source_size_bytes=path.stat().st_size,
        start_timestamp_utc_ms=start,
        end_timestamp_utc_ms=start + 2_000_000,
        row_count=row_count,
        denominators_ms={
            "calendar_duration_ms": 2_000_000,
            "market_open_duration_ms": 1_500_000,
            "active_window_duration_ms": 1_000_000,
        },
        counts={
            "transition_count": row_count - 1,
            "market_open_row_count": row_count - 100,
            "active_window_interval_count": row_count - 200,
        },
        feature_values=features,
        feature_provenance={name: (f"fixture.{name}",) for name in features},
        activity_bin_counts={str(start // 3_600_000): 100},
        calendar_policy={"active_time_policy_version": "fixture.v1"},
    )


def _campaign_evidence(tmp_path: Path) -> tuple[FeedEpochEvidenceV2, ...]:
    cache_by_symbol: dict[str, tuple[Path, str]] = {}
    for symbol in SYMBOLS:
        path = tmp_path / symbol / ".data"
        cache_by_symbol[symbol] = (path, _tick_cache(path))
    result = []
    for year in range(2019, 2023):
        for month in range(1, 13):
            period = f"{year}{month:02d}"
            for symbol in SYMBOLS:
                path, source_hash = cache_by_symbol[symbol]
                result.append(
                    _epoch_evidence(
                        symbol=symbol,
                        period=period,
                        path=path,
                        source_hash=source_hash,
                    )
                )
    return tuple(result)


def test_real_campaign_is_blocked_bounded_replayable_and_fail_closed(
    tmp_path: Path,
) -> None:
    """A ready claim requires three time blocks and passing final holdouts."""
    definition = _definition()
    profile = ObservationCalibrationProfileV2(
        split_periods={
            "calibration": "202001",
            "validation": "202101",
            "final_holdout": "202201",
        },
        sessions=("asia", "london", "new_york"),
        max_events_per_window=128,
        minimum_events_per_window=64,
    )

    evidence = _campaign_evidence(tmp_path)
    campaign = calibrate_historical_observation_operators(
        evidence,
        epoch_definition=definition,
        profile=profile,
    )
    repeated = calibrate_historical_observation_operators(
        evidence,
        epoch_definition=definition,
        profile=profile,
    )

    assert campaign.valid_for_application
    assert repeated.campaign_id == campaign.campaign_id
    assert repeated.operator.operator_id == campaign.operator.operator_id
    assert {item.split_kind for item in campaign.windows} == set(
        (
            BenchmarkSplitKind.CALIBRATION,
            BenchmarkSplitKind.VALIDATION,
            BenchmarkSplitKind.FINAL_HOLDOUT,
        )
    )
    assert all(item.input_count <= 128 for item in campaign.windows)
    assert all(
        item.passed
        for item in campaign.windows
        if item.split_kind is BenchmarkSplitKind.FINAL_HOLDOUT
    )
    assert all(
        target.parameter_status["retention_probability"] == "supported"
        for target in campaign.targets
    )
    assert all(
        target.parameter_status["outage_window_ns"] == "unsupported"
        for target in campaign.targets
    )
    assert all(
        target.parameter_support_counts["retention_probability"] > 0
        and target.parameter_support_counts["outage_window_ns"] == 0
        for target in campaign.targets
    )
    assert all(
        target.mechanism_diagnostics["calendar_closure"]
        != target.mechanism_diagnostics["archive_gap"]
        for target in campaign.targets
    )
    context = ObservationContextV1(
        symbol="EURUSD",
        epoch_id="technology_epoch_01",
        state="update_joint",
        session="london",
    )
    campaign.require_application_ready(context)
    with pytest.raises(ValueError, match="unsupported"):
        campaign.require_application_ready(
            context,
            required_parameters=("outage_window_ns",),
        )

    artifacts = write_observation_calibration_campaign(
        campaign, tmp_path / "artifacts"
    )
    restored = read_observation_calibration_campaign(artifacts["campaign"].path)
    assert restored.campaign_id == campaign.campaign_id
    assert restored.operator.operator_id == campaign.operator.operator_id

    target = campaign.targets[0]
    unsafe = replace(
        target,
        parameter_reasons={
            **target.parameter_reasons,
            "retention_probability": "identity_without_dense_denominator",
        },
        target_id="",
    )
    with pytest.raises(ValueError, match="readiness evidence differs"):
        ObservationCalibrationCampaignV2(
            feed_epoch_definition_id=campaign.feed_epoch_definition_id,
            calibration_corpus_sha256=campaign.calibration_corpus_sha256,
            profile=campaign.profile,
            operator=campaign.operator,
            targets=(unsafe, *campaign.targets[1:]),
            fit_evidence=campaign.fit_evidence,
            windows=campaign.windows,
            readiness_status="ready",
            readiness_reasons=(),
            runtime_seconds=campaign.runtime_seconds,
            peak_memory_bytes=campaign.peak_memory_bytes,
        )


def test_controlled_pair_recovers_known_state_dependent_parameters() -> None:
    """Known state-specific thinning, timestamp grid, and duplicates recover."""
    definition = _definition()
    start_ns = _month_start("202001") * 1_000_000
    contexts = {
        "update_bid_only": ObservationContextV1(
            symbol="EURUSD",
            epoch_id="technology_epoch_01",
            state="update_bid_only",
            session="london",
        ),
        "update_joint": ObservationContextV1(
            symbol="EURUSD",
            epoch_id="technology_epoch_01",
            state="update_joint",
            session="london",
        ),
    }
    source_hash = _sha256("controlled-pair")
    fit_rows = []
    for ordinal, (state, probability) in enumerate(
        (("update_bid_only", 0.25), ("update_joint", 0.75)), start=1
    ):
        values = {
            "retention_probability": probability,
            "timestamp_quantum_ns": 1_000_000_000.0,
            "duplicate_probability": 0.2,
        }
        fit_rows.append(
            ObservationFitEvidenceV1(
                context=contexts[state],
                period="202001",
                start_timestamp_ns=start_ns,
                end_timestamp_ns=start_ns + 1_000_000_000_000,
                source_evidence_id=f"controlled-{ordinal}",
                source_artifact_sha256=source_hash,
                source_hash_basis="controlled_fixture_sha256",
                evidence_kind="controlled_fixture",
                parameter_values=values,
                parameter_lower_bounds=values,
                parameter_upper_bounds=values,
                parameter_support_counts={name: 2500 for name in values},
                parameter_basis={name: "controlled_fixture" for name in values},
                parameter_provenance={
                    name: (f"fixture.{name}",) for name in values
                },
            )
        )
    operator = fit_observation_operator(
        fit_rows,
        epoch_definition=definition,
        config=ObservationOperatorFitConfigV1(
            min_stratum_support=1,
            min_parameter_support=1,
            min_supported_parameters=3,
            max_input_events=5000,
        ),
    )
    events = tuple(
        ObservationInputEventV1(
            source_event_id=f"controlled-event-{index}",
            symbol="EURUSD",
            event_time_ns=start_ns + index * 137_000_000,
            event_sequence=index,
            bid=1.1 + (index % 7) * 0.00001,
            ask=1.1001 + (index % 7) * 0.00001,
            context=contexts[
                "update_bid_only" if index % 2 == 0 else "update_joint"
            ],
        )
        for index in range(5000)
    )
    applied = operator.degrade(
        events,
        window=ReconstructionWindowV1(
            run_id="controlled-recovery",
            ensemble_member_id="member-000",
            symbols=("EURUSD",),
            core_start_ns=start_ns,
            core_end_ns=(events[-1].event_time_ns // 1_000_000_000 + 1)
            * 1_000_000_000,
        ),
        source_start=True,
    )

    recovered = estimate_paired_observation_evidence(
        events,
        applied,
        period="202001",
        source_artifact_sha256=source_hash,
    )
    by_state = {item.context.state: item for item in recovered}
    assert by_state["update_bid_only"].parameter_values[
        "retention_probability"
    ] == pytest.approx(0.25, abs=0.04)
    assert by_state["update_joint"].parameter_values[
        "retention_probability"
    ] == pytest.approx(0.75, abs=0.04)
    assert all(
        item.parameter_values["timestamp_quantum_ns"] == 1_000_000_000
        for item in recovered
    )
    assert all(
        item.parameter_values["duplicate_probability"]
        == pytest.approx(0.2, abs=0.04)
        for item in recovered
    )


def test_real_v2_projection_caps_support_and_keeps_integral_durations(
    tmp_path: Path,
) -> None:
    """Monthly real row counts must fit the bounded v1 evidence bridge."""
    path = tmp_path / ".data"
    source_hash = _tick_cache(path)
    evidence = _epoch_evidence(
        symbol="EURUSD",
        period="202001",
        path=path,
        source_hash=source_hash,
        row_count=500_000,
    )

    projected = ObservationFitEvidenceV1.from_feed_epoch_evidence(
        evidence, _definition()
    )

    assert max(projected.parameter_support_counts.values()) == 250_000
    assert projected.parameter_support_counts["retention_probability"] == 0
    for name in (
        "timestamp_quantum_ns",
        "batch_window_ns",
        "burst_window_ns",
        "outage_window_ns",
    ):
        assert projected.parameter_values[name].is_integer()

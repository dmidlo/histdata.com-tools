"""Hermetic transition fixtures for reconstruction and observation tests."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from histdatacom.data_analytics.feed_epochs_v2 import (
    FeedEpochBoundaryV2,
    FeedEpochDefinitionV2,
    FeedEpochFitConfigV2,
    FeedEpochIntervalV2,
    FeedEpochStabilityV2,
)
from histdatacom.synthetic.observation import (
    OBSERVATION_PARAMETER_NAMES,
    ObservationContextV1,
    ObservationFitEvidenceV1,
    ObservationOperatorFitConfigV1,
    ObservationOperatorV1,
    fit_observation_operator,
)

_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
_BASE_PARAMETERS = {
    "retention_probability": 1.0,
    "unchanged_retention_probability": 1.0,
    "timestamp_quantum_ns": 10.0,
    "price_precision_digits": 5.0,
    "quote_transition_threshold": 0.0,
    "batch_window_ns": 0.0,
    "duplicate_probability": 0.0,
    "rate_cap_per_second": 0.0,
    "burst_window_ns": 100.0,
    "quiet_gap_probability": 0.0,
    "outage_window_ns": 100.0,
    "reconnect_duplicate_probability": 0.0,
}


def _timestamp_ns(year: int, month: int) -> int:
    return int(
        datetime(year, month, 15, tzinfo=timezone.utc).timestamp()
        * 1_000_000_000
    )


def _sha256_id(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def reconstruction_transition_definition() -> FeedEpochDefinitionV2:
    """Return a compact four-epoch definition mirroring real boundaries."""
    config = FeedEpochFitConfigV2()
    boundaries = (
        FeedEpochBoundaryV2(
            left_period="200503",
            right_period="200504",
            central_timestamp_utc_ms=1_112_313_600_000,
            support=1.0,
            uncertainty_start_period="200504",
            uncertainty_end_period="200504",
            objective_gain=10.0,
            supporting_features=("unchanged_rate",),
        ),
        FeedEpochBoundaryV2(
            left_period="200904",
            right_period="200905",
            central_timestamp_utc_ms=1_241_136_000_000,
            support=1.0,
            uncertainty_start_period="200905",
            uncertainty_end_period="200906",
            objective_gain=20.0,
            supporting_features=("price_precision_digits",),
        ),
        FeedEpochBoundaryV2(
            left_period="201811",
            right_period="201812",
            central_timestamp_utc_ms=1_543_622_400_000,
            support=1.0,
            uncertainty_start_period="201812",
            uncertainty_end_period="201812",
            objective_gain=30.0,
            supporting_features=("burst_interval_rate",),
        ),
    )
    epochs = (
        FeedEpochIntervalV2(
            label="technology_epoch_01",
            period_start="200203",
            period_end="200503",
            start_timestamp_utc_ms=1_014_940_800_000,
            end_timestamp_utc_ms=1_112_313_599_999,
            evidence_count=12,
            feature_medians={},
        ),
        FeedEpochIntervalV2(
            label="technology_epoch_02",
            period_start="200504",
            period_end="200904",
            start_timestamp_utc_ms=1_112_313_600_000,
            end_timestamp_utc_ms=1_241_135_999_999,
            evidence_count=12,
            feature_medians={},
        ),
        FeedEpochIntervalV2(
            label="technology_epoch_03",
            period_start="200905",
            period_end="201811",
            start_timestamp_utc_ms=1_241_136_000_000,
            end_timestamp_utc_ms=1_543_622_399_999,
            evidence_count=12,
            feature_medians={},
        ),
        FeedEpochIntervalV2(
            label="technology_epoch_04",
            period_start="201812",
            period_end="202606",
            start_timestamp_utc_ms=1_543_622_400_000,
            end_timestamp_utc_ms=1_782_863_999_999,
            evidence_count=12,
            feature_medians={},
        ),
    )
    boundary_support = {
        boundary.right_period: boundary.support for boundary in boundaries
    }
    return FeedEpochDefinitionV2(
        config=config,
        symbols=_SYMBOLS,
        coverage_start_utc_ms=epochs[0].start_timestamp_utc_ms,
        coverage_end_utc_ms=epochs[-1].end_timestamp_utc_ms,
        evidence_count=48,
        period_count=292,
        feature_names=config.feature_names,
        boundaries=boundaries,
        epochs=epochs,
        symbol_deviations=(),
        stability=FeedEpochStabilityV2(
            status="pass",
            reasons=(),
            run_count=1,
            run_counts={"controlled_fixture": 1},
            boundary_support=boundary_support,
            boundary_support_by_family={
                period: {"controlled_fixture": support}
                for period, support in boundary_support.items()
            },
            rejected_candidates={},
            feature_coverage={},
            common_period_count=292,
            symbol_count=len(_SYMBOLS),
        ),
        lineage={
            "source": "controlled-hermetic-transition-fixture",
            "production_artifacts_required": False,
        },
    )


def reconstruction_transition_operator(
    definition: FeedEpochDefinitionV2,
) -> ObservationOperatorV1:
    """Fit a compact operator with distinct adjacent-epoch cardinalities."""
    epoch_inputs = (
        ("technology_epoch_01", "200301", _timestamp_ns(2003, 1), 0.25),
        (
            "technology_epoch_02",
            "200701",
            _timestamp_ns(2007, 1),
            0.24065855,
        ),
        (
            "technology_epoch_03",
            "201201",
            _timestamp_ns(2012, 1),
            0.49569722,
        ),
        ("technology_epoch_04", "202001", _timestamp_ns(2020, 1), 0.80),
    )
    evidence = []
    for epoch_label, period, timestamp_ns, retention in epoch_inputs:
        values = {
            **_BASE_PARAMETERS,
            "retention_probability": retention,
            "unchanged_retention_probability": retention,
        }
        assert set(values) == set(OBSERVATION_PARAMETER_NAMES)
        for symbol in _SYMBOLS:
            evidence.append(
                ObservationFitEvidenceV1(
                    context=ObservationContextV1(
                        symbol=symbol,
                        epoch_id=epoch_label,
                    ),
                    period=period,
                    start_timestamp_ns=timestamp_ns,
                    end_timestamp_ns=timestamp_ns + 1_000_000,
                    source_evidence_id=(f"controlled:{symbol}:{epoch_label}"),
                    source_artifact_sha256=_sha256_id(
                        f"{symbol}:{epoch_label}"
                    ),
                    source_hash_basis="controlled_fixture_sha256",
                    evidence_kind="controlled_fixture",
                    parameter_values=values,
                    parameter_lower_bounds=values,
                    parameter_upper_bounds=values,
                    parameter_support_counts={name: 100 for name in values},
                    parameter_basis={
                        name: "controlled_fixture" for name in values
                    },
                    parameter_provenance={
                        name: (f"fixture.{name}",) for name in values
                    },
                )
            )
    return fit_observation_operator(
        evidence,
        epoch_definition=definition,
        config=ObservationOperatorFitConfigV1(
            min_stratum_support=1,
            min_parameter_support=1,
            min_supported_parameters=len(OBSERVATION_PARAMETER_NAMES),
        ),
    )


def reconstruction_transition_fixture() -> (
    tuple[FeedEpochDefinitionV2, ObservationOperatorV1]
):
    """Return an internally bound transition definition and operator."""
    definition = reconstruction_transition_definition()
    return definition, reconstruction_transition_operator(definition)

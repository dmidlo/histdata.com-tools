"""Tests for uncertainty-aware technological feed-epoch contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_analytics import (
    DEFAULT_FEED_EPOCH_FEATURES,
    FEED_EPOCH_DEFINITION_SCHEMA_VERSION,
    FEED_EPOCH_EVIDENCE_SCHEMA_VERSION,
    FEED_EPOCH_FIT_CONFIG_SCHEMA_VERSION,
    FeedEpochAssignmentV1,
    FeedEpochDefinitionV1,
    FeedEpochEvidenceV1,
    FeedEpochFitConfigV1,
    feed_epoch_definition_to_json,
    fit_feed_epochs,
    write_feed_epoch_definition,
)
from histdatacom.data_quality import TIME_SERIES_FINGERPRINT_SCHEMA_VERSION


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _fingerprint(
    index: int,
    *,
    modern: bool,
    symbol: str = "EURUSD",
    feature_mode: str = "full",
) -> dict[str, object]:
    period = f"2020{index + 1:02d}"
    start = index * 10_000
    row_count = 1_000 if modern else 100
    median_gap = 100.0 if modern else 1_000.0
    p95_gap = 250.0 if modern else 2_500.0
    spread = 0.0001 if modern else 0.0008
    precision = 6 if modern else 4
    payload: dict[str, object] = {
        "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
        "fingerprint_id": _sha256(f"{symbol}:{period}:{modern}:{feature_mode}"),
        "target_axis": {
            "data_format": "ascii",
            "timeframe": "T",
            "symbol": symbol,
            "period": period,
            "kind": "cache",
        },
        "coverage": {
            "row_count": row_count,
            "parsed_row_count": row_count,
            "start_timestamp_utc_ms": start,
            "end_timestamp_utc_ms": start + 9_000,
            "duration_ms": 3_600_000,
        },
        "temporal_topology": {
            "row_count": row_count,
            "parsed_row_count": row_count,
            "interval_count": row_count - 1,
            "min_interval_ms": 10 if modern else 1_000,
            "median_interval_ms": median_gap,
            "duplicate_timestamp_count": 0 if modern else 10,
            "suspicious_gap_count": 0 if modern else 5,
            "sampling_basis": "observed_sequence",
            "computed_from": "direct_cache",
            "cache_source": "direct",
        },
        "tick_distribution": {
            "spread": {
                "min": spread / 2,
                "median": spread,
                "mean": spread,
                "max": spread * 2,
            },
            "precision": {
                "column_decimal_place_counts": {
                    "bid": {str(precision): row_count},
                    "ask": {str(precision): row_count},
                }
            },
        },
        "microstructure_dynamics": {
            "basis": "observed_sequence",
            "computed_from": "direct_cache",
            "cache_source": "direct",
            "sequence_status": "ok",
            "limitations": [],
            "interarrival_ms": {
                "median": median_gap,
                "max": p95_gap * 2,
                "quantiles": {"0.95": p95_gap},
            },
            "absolute_spread_change": {"median": spread / (4 if modern else 2)},
            "stale_quote": {
                "repeat_rate": 0.02 if modern else 0.45,
                "repeat_count": 20 if modern else 45,
                "run_count": 1 if modern else 8,
                "affected_row_count": 20 if modern else 60,
            },
            "burst": {"burst_rate": 0.8 if modern else 0.1},
        },
        "calendar_regimes": {
            "schema_version": "histdatacom.time-series-fingerprint-calendar-regimes.v1",
            "status": "ok",
            "calendar_profile_name": "fixture",
            "calendar_profile_version": "1",
            "calendar_profile_complete": True,
            "session_state_counts": {"market_open": row_count},
            "active_session_counts": {
                "london": row_count // 2,
                "new_york": row_count - row_count // 2,
            },
            "special_tag_counts": {"daily_rollover": 1},
            "holiday_tag_counts": {},
            "event_tag_counts": {"fixture_release": 1},
        },
        "conditional_distributions": {
            "schema_version": "histdatacom.time-series-fingerprint-conditional-distributions.v1",
            "status": "ok",
            "by_active_session": {
                "london": {"spread": {"median": spread}},
                "new_york": {"spread": {"median": spread * 1.1}},
            },
        },
        "fingerprint_audit": {
            "schema_version": "histdatacom.time-series-fingerprint-audit.v1",
            "section_statuses": {
                "coverage": "valid",
                "temporal_topology": "valid",
                "calendar_regimes": "valid",
                "tick_distribution": "valid",
                "conditional_distributions": "valid",
                "microstructure_dynamics": "valid",
            },
            "sections_skipped": {},
            "source_status": {"kind": "cache", "readable": True},
        },
        "source": {
            "kind": "cache",
            "cache_source": "direct",
            "path": f"fixture/{symbol}/{period}/.data",
        },
    }
    if feature_mode == "single_change":
        payload["tick_distribution"] = {
            "spread": {
                "min": 0.0001,
                "median": spread,
                "mean": spread,
                "max": 0.0008,
            }
        }
        payload["microstructure_dynamics"] = {
            "basis": "observed_sequence",
            "computed_from": "direct_cache",
            "cache_source": "direct",
            "sequence_status": "ok",
            "limitations": [],
            "interarrival_ms": {
                "median": 100.0,
                "quantiles": {"0.95": 250.0},
            },
            "absolute_spread_change": {"median": 0.00001},
            "stale_quote": {"repeat_rate": 0.1},
            "burst": {"burst_rate": 0.2},
        }
        payload["temporal_topology"] = {
            "row_count": 100,
            "parsed_row_count": 100,
            "interval_count": 99,
            "min_interval_ms": 10,
            "duplicate_timestamp_count": 0,
            "suspicious_gap_count": 0,
            "sampling_basis": "observed_sequence",
            "computed_from": "direct_cache",
        }
    return payload


def _evidence_series(
    *, feature_mode: str = "full"
) -> tuple[FeedEpochEvidenceV1, ...]:
    return tuple(
        FeedEpochEvidenceV1.from_fingerprint(
            _fingerprint(
                index,
                modern=index >= 6,
                feature_mode=feature_mode,
            )
        )
        for index in range(12)
    )


def _annual_evidence_series(count: int = 24) -> tuple[FeedEpochEvidenceV1, ...]:
    result: list[FeedEpochEvidenceV1] = []
    for index in range(count):
        base = FeedEpochEvidenceV1.from_fingerprint(
            _fingerprint(index % 12, modern=index >= count // 2)
        )
        source_hash = _sha256(f"annual-source:{index}")
        result.append(
            replace(
                base,
                period=str(2000 + index),
                start_timestamp_utc_ms=index * 10_000,
                end_timestamp_utc_ms=index * 10_000 + 9_000,
                fingerprint_id=_sha256(f"annual-fingerprint:{index}"),
                source_artifact_sha256=source_hash,
                evidence_id="",
            )
        )
    return tuple(result)


def test_canonical_fingerprint_evidence_preserves_conditioning_and_quality() -> (
    None
):
    """Epoch evidence should retain hashes, feature provenance, and controls."""
    fingerprint = _fingerprint(0, modern=False)

    evidence = FeedEpochEvidenceV1.from_fingerprint(fingerprint)
    payload = evidence.to_dict()

    assert payload["schema_version"] == FEED_EPOCH_EVIDENCE_SCHEMA_VERSION
    assert payload["fingerprint_id"] == fingerprint["fingerprint_id"]
    assert payload["source_artifact_sha256"] == fingerprint["fingerprint_id"]
    assert payload["source_hash_basis"] == "canonical_fingerprint_id"
    assert payload["source_kind"] == "cache"
    assert payload["conditioning"]["calendar_status"] == "ok"
    assert payload["conditioning"]["event_tag_counts"] == {"fixture_release": 1}
    assert payload["quality"]["sequence_status"] == "ok"
    assert payload["feature_provenance"]["spread_median"] == [
        "tick_distribution.spread.median"
    ]
    assert FeedEpochEvidenceV1.from_dict(payload) == evidence


def test_synthetic_regime_changes_recover_stable_uncertain_boundary() -> None:
    """Known cadence, precision, spread, and stale changes should be recovered."""
    definition = fit_feed_epochs(_evidence_series())

    assert definition.schema_version == FEED_EPOCH_DEFINITION_SCHEMA_VERSION
    assert (
        definition.config.schema_version == FEED_EPOCH_FIT_CONFIG_SCHEMA_VERSION
    )
    assert definition.valid_for_observation_models is True
    assert definition.stability.status == "pass"
    assert definition.stability.usable_run_counts == {
        "feature_removal": len(DEFAULT_FEED_EPOCH_FEATURES),
        "missing_periods": 10,
        "sampling": 2,
    }
    assert len(definition.boundaries) == 1
    boundary = definition.boundaries[0]
    assert boundary.left_period == "202006"
    assert boundary.right_period == "202007"
    assert boundary.support == 1.0
    assert boundary.confidence > 0.0
    assert boundary.uncertainty_start_utc_ms < boundary.central_timestamp_utc_ms
    assert boundary.central_timestamp_utc_ms < boundary.uncertainty_end_utc_ms
    assert len(definition.epochs) == 2
    assert definition.epochs[0].period_end == "202006"
    assert definition.epochs[1].period_start == "202007"


def test_assignment_exposes_transition_and_refuses_unstable_artifacts() -> None:
    """Assignments must preserve uncertainty and enforce the stability gate."""
    stable = fit_feed_epochs(_evidence_series())
    boundary = stable.boundaries[0]

    transition = stable.assign(
        symbol="EURUSD",
        timestamp_utc_ms=boundary.central_timestamp_utc_ms,
    )
    early = stable.assign(symbol="EURUSD", timestamp_utc_ms=1_000)
    late = stable.assign(symbol="EURUSD", timestamp_utc_ms=118_000)

    assert transition.assignment_kind == "transition"
    assert transition.boundary_id == boundary.boundary_id
    assert FeedEpochAssignmentV1.from_dict(transition.to_dict()) == transition
    assert early.epoch_id == "epoch-001"
    assert late.epoch_id == "epoch-002"
    assert (
        stable.assign(symbol="GBPUSD", timestamp_utc_ms=1_000).assignment_kind
        == "out_of_scope"
    )

    unstable_config = FeedEpochFitConfigV1(
        feature_names=(
            "spread_median",
            "log_median_interarrival_ms",
            "burst_rate",
        ),
        min_boundary_support=0.8,
    )
    unstable = fit_feed_epochs(
        _evidence_series(feature_mode="single_change"),
        config=unstable_config,
    )
    assert unstable.stability.status == "fail"
    with pytest.raises(ValueError, match="stability"):
        unstable.assign(symbol="EURUSD", timestamp_utc_ms=1_000)
    assert (
        unstable.assign(
            symbol="EURUSD",
            timestamp_utc_ms=1_000,
            require_stable=False,
        ).assignment_kind
        == "epoch"
    )


def test_definition_is_order_independent_serializable_and_lineage_complete() -> (
    None
):
    """Source order must not change identity and every source hash must survive."""
    evidence = _evidence_series()
    first = fit_feed_epochs(evidence)
    second = fit_feed_epochs(reversed(evidence))

    assert first.definition_id == second.definition_id
    assert first.to_dict() == second.to_dict()
    restored = FeedEpochDefinitionV1.from_json(first.to_json())
    assert restored == first
    assert json.loads(feed_epoch_definition_to_json(first))[
        "definition_id"
    ] == (first.definition_id)
    sources = first.lineage["sources"]
    assert isinstance(sources, list)
    assert len(sources) == len(evidence)
    assert {source["fingerprint_id"] for source in sources} == {
        item.fingerprint_id for item in evidence
    }
    assert first.lineage["config_id"] == first.config.config_id
    assert str(first.lineage["lineage_sha256"]).startswith("sha256:")


def test_stability_quantifies_all_required_perturbation_families() -> None:
    """Sampling, missing-period, and feature-removal sensitivity stay explicit."""
    definition = fit_feed_epochs(_evidence_series())
    stability = definition.stability.to_dict()

    assert stability["run_counts"]["sampling"] == 2
    assert stability["run_counts"]["missing_periods"] == 10
    assert stability["run_counts"]["feature_removal"] > 0
    assert definition.boundaries[0].support_by_analysis == {
        "feature_removal": 1.0,
        "missing_periods": 1.0,
        "sampling": 1.0,
    }


def test_bounded_sensitivity_budget_retains_all_families_across_long_history() -> (
    None
):
    """Long histories must not spend the run budget before sampling is tested."""
    config = FeedEpochFitConfigV1(max_sensitivity_runs=20)

    definition = fit_feed_epochs(_annual_evidence_series(), config=config)

    assert sum(definition.stability.run_counts.values()) == 20
    assert definition.stability.run_counts["sampling"] == 2
    assert definition.stability.run_counts["feature_removal"] == len(
        DEFAULT_FEED_EPOCH_FEATURES
    )
    assert definition.stability.run_counts["missing_periods"] == 5
    assert all(definition.stability.usable_run_counts.values())
    assert definition.stability.status == "pass"


def test_no_change_produces_one_stable_epoch_without_false_boundaries() -> None:
    """A stable observation process should not be forced into date buckets."""
    evidence = tuple(
        FeedEpochEvidenceV1.from_fingerprint(_fingerprint(index, modern=True))
        for index in range(12)
    )

    definition = fit_feed_epochs(evidence)

    assert definition.boundaries == ()
    assert len(definition.epochs) == 1
    assert definition.epochs[0].period_start == "202001"
    assert definition.epochs[0].period_end == "202012"
    assert definition.stability.status == "pass"


def test_panel_membership_change_does_not_create_a_false_epoch() -> None:
    """A later stable symbol must not masquerade as a feed technology change."""
    eurusd = tuple(
        FeedEpochEvidenceV1.from_fingerprint(_fingerprint(index, modern=True))
        for index in range(12)
    )
    gbpusd: list[FeedEpochEvidenceV1] = []
    for index in range(6, 12):
        base = FeedEpochEvidenceV1.from_fingerprint(
            _fingerprint(index, modern=True, symbol="GBPUSD")
        )
        shifted = {
            name: value + (10.0 if "spread" not in name else 0.01)
            for name, value in base.feature_values.items()
        }
        gbpusd.append(replace(base, feature_values=shifted, evidence_id=""))

    definition = fit_feed_epochs((*eurusd, *gbpusd))

    assert definition.symbols == ("EURUSD", "GBPUSD")
    assert definition.boundaries == ()
    assert definition.stability.status == "pass"
    assert definition.lineage["panel_normalization"] == (
        "within_symbol_robust_then_period_median"
    )


def test_config_and_contract_identity_drift_fail_closed() -> None:
    """Config and artifact identities must reject semantic or serialized drift."""
    config = FeedEpochFitConfigV1()
    with pytest.raises(ValueError, match="config_id"):
        FeedEpochFitConfigV1(config_id="feed-epoch-config:sha256:" + "0" * 64)

    definition = fit_feed_epochs(_evidence_series(), config=config)
    payload = definition.to_dict()
    payload["period_count"] = 11
    with pytest.raises(ValueError, match="definition_id"):
        FeedEpochDefinitionV1.from_dict(payload)

    evidence_payload = _evidence_series()[0].to_dict()
    evidence_payload["schema_version"] = "histdatacom.feed-epoch-evidence.v2"
    with pytest.raises(ValueError, match="schema"):
        FeedEpochEvidenceV1.from_dict(evidence_payload)


def test_definition_rejects_forged_stability_lineage_and_epoch_alignment() -> (
    None
):
    """Strict artifact readers must reconcile trust evidence before identity."""
    definition = fit_feed_epochs(_evidence_series())

    stability_payload = definition.to_dict()
    stability_payload["stability"]["run_counts"]["sampling"] = 0
    stability_payload["stability"]["usable_run_counts"]["sampling"] = 0
    with pytest.raises(ValueError, match="stability status"):
        FeedEpochDefinitionV1.from_dict(stability_payload)

    lineage_payload = definition.to_dict()
    lineage_payload["lineage"]["panel_normalization"] = "untrusted"
    with pytest.raises(ValueError, match="lineage hash"):
        FeedEpochDefinitionV1.from_dict(lineage_payload)

    alignment_payload = definition.to_dict()
    alignment_payload["boundaries"][0]["right_period"] = "202008"
    with pytest.raises(ValueError, match="align with adjacent epochs"):
        FeedEpochDefinitionV1.from_dict(alignment_payload)


def test_invalid_or_non_tick_fingerprint_is_rejected() -> None:
    """Only readable canonical ASCII tick fingerprints may become evidence."""
    payload = _fingerprint(0, modern=False)
    payload["schema_version"] = "histdatacom.time-series-fingerprint.v2"
    with pytest.raises(ValueError, match="canonical"):
        FeedEpochEvidenceV1.from_fingerprint(payload)

    payload = _fingerprint(0, modern=False)
    payload["target_axis"]["timeframe"] = "M1"
    with pytest.raises(ValueError, match="tick"):
        FeedEpochEvidenceV1.from_fingerprint(payload)

    payload = _fingerprint(0, modern=False)
    payload["fingerprint_id"] = "not-a-hash"
    with pytest.raises(ValueError, match="sha256"):
        FeedEpochEvidenceV1.from_fingerprint(payload)

    payload = _fingerprint(0, modern=False)
    payload["target_axis"]["period"] = "202099"
    with pytest.raises(ValueError, match="period"):
        FeedEpochEvidenceV1.from_fingerprint(payload)


def test_duplicate_axes_insufficient_periods_and_resource_limits_fail_closed() -> (
    None
):
    """Malformed or unbounded evidence sets should fail before fitting."""
    evidence = _evidence_series()
    with pytest.raises(ValueError, match="duplicate feed epoch evidence_id"):
        fit_feed_epochs((*evidence, evidence[0]))
    duplicate_axis = replace(
        evidence[0],
        fingerprint_id=_sha256("duplicate-axis"),
        source_artifact_sha256=_sha256("duplicate-axis"),
        evidence_id="",
    )
    with pytest.raises(ValueError, match="duplicates a symbol-period"):
        fit_feed_epochs((*evidence, duplicate_axis))
    with pytest.raises(ValueError, match="at least 6"):
        fit_feed_epochs(evidence[:5])
    with pytest.raises(ValueError, match="configured maximum"):
        fit_feed_epochs(
            evidence,
            config=FeedEpochFitConfigV1(max_evidence=6),
        )


def test_definition_artifact_reference_is_compact_and_replayable(
    tmp_path: Path,
) -> None:
    """Durable output should be the compact definition, not analytical rows."""
    definition = fit_feed_epochs(_evidence_series())
    path = tmp_path / "epochs" / "definition.json"

    artifact = write_feed_epoch_definition(definition, path)

    assert artifact.kind == "feed-epoch-definition"
    assert artifact.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert artifact.metadata["definition_id"] == definition.definition_id
    assert artifact.metadata["valid_for_observation_models"] is True
    assert FeedEpochDefinitionV1.from_json(path.read_text()) == definition


def test_feature_and_sensitivity_limits_are_bounded() -> None:
    """Fit configuration should reject unbounded diagnostic policies."""
    with pytest.raises(ValueError, match="feature_names"):
        FeedEpochFitConfigV1(feature_names=())
    with pytest.raises(ValueError, match="max_sensitivity_runs"):
        FeedEpochFitConfigV1(max_sensitivity_runs=10_000)
    with pytest.raises(ValueError, match="unsupported feed epoch feature"):
        FeedEpochFitConfigV1(feature_names=("calendar_year",))

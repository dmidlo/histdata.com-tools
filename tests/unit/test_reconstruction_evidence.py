"""Point-in-time reconstruction quality evidence contracts and adapters."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl
import pytest

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.reconstruction_evidence import (
    HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION,
    HISTDATA_LEGACY_CACHE_SCHEMA_VERSION,
    PointInTimeEvidenceProjectionV1,
    ReconstructionEvidenceInformationMode,
    ReconstructionEvidenceKind,
    ReconstructionEvidencePolicyV1,
    ReconstructionEvidenceReadiness,
    ReconstructionEvidenceUseStatus,
    compile_histdata_point_in_time_evidence,
    reconstruction_evidence_use,
    resolve_reconstruction_evidence_thresholds,
)
from histdatacom.runtime_contracts import JSONScalar, JSONValue


@dataclass(frozen=True)
class _Event:
    event_time_ns: int
    source_row_id: int
    bid: float
    ask: float


def test_threshold_resolution_prefers_profile_then_fingerprint_then_policy() -> (
    None
):
    policy = ReconstructionEvidencePolicyV1(
        suspicious_gap_fallback_ms=99_000,
        wide_spread_multiplier=4.0,
    )

    profile = resolve_reconstruction_evidence_thresholds(
        spreads=(0.0001, 0.0002),
        classification_profile={
            "thresholds": {
                "suspicious_gap_ms": 12_000,
                "wide_spread_threshold": 0.0007,
            }
        },
        fingerprint_payload={
            "temporal_topology": {
                "gap_tolerance": {"suspicious_gap_ms": 24_000}
            }
        },
        policy=policy,
    )
    fingerprint = resolve_reconstruction_evidence_thresholds(
        spreads=(0.0001, 0.0002),
        fingerprint_payload={
            "temporal_topology": {
                "gap_tolerance": {"suspicious_gap_ms": 24_000}
            }
        },
        policy=policy,
    )
    fallback = resolve_reconstruction_evidence_thresholds(policy=policy)

    assert profile.suspicious_gap_ms == 12_000
    assert profile.suspicious_gap_basis.startswith("classification_profile")
    assert profile.wide_spread_threshold == pytest.approx(0.0007)
    assert fingerprint.suspicious_gap_ms == 24_000
    assert fallback.suspicious_gap_ms == 99_000
    assert fallback.suspicious_gap_basis == "evidence_policy:explicit_fallback"
    assert fallback.wide_spread_threshold is None


def test_projection_preserves_duplicate_rows_and_keeps_aggregates_sidecar() -> (
    None
):
    projection = _projection(
        (
            _Event(1_000_000_000, 1, 1.0, 1.0001),
            _Event(1_000_000_000, 2, 1.0, 1.0001),
            _Event(2_000_000_000, 3, 1.0, 1.0010),
        )
    )
    restored = PointInTimeEvidenceProjectionV1.from_json(projection.to_json())

    assert restored == projection
    duplicate_rows = tuple(
        item
        for item in projection.row_records
        if item.metric_id == "duplicate_timestamp"
    )
    assert {item.source_row_id for item in duplicate_rows} == {1, 2}
    assert all(
        item.target_grain.value == "row" for item in projection.row_records
    )
    assert all(
        item.target_grain.value != "row" for item in projection.sidecar_records
    )
    assert '"bid"' not in projection.to_json()
    assert '"ask"' not in projection.to_json()
    assert '"full_reports_embedded":false' in projection.to_json()


def test_exact_report_location_projects_but_period_summary_does_not_copy_to_rows() -> (
    None
):
    target = QualityTarget(
        path="cache/.data",
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    exact = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="ASCII_TICK_DUPLICATE_TIMESTAMP",
        message="duplicate",
        rule_id="ticks.duplicate.v1",
        target=target,
        location=QualityLocation(row_number=2, timestamp_utc_ms=1_000),
    )
    aggregate = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="FINGERPRINT_SERIES_SUMMARY",
        message="period summary",
        rule_id="fingerprint.series",
        target=target,
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="ticks.duplicate.v1", target=target, findings=(exact,)
            ),
            QualityRuleResult(
                rule_id="fingerprint.series",
                target=target,
                findings=(aggregate,),
            ),
        ),
    )

    projection = _projection(
        (
            _Event(500_000_000, 1, 1.0, 1.0001),
            _Event(1_000_000_000, 2, 1.0, 1.0001),
        ),
        quality_report=report,
    )

    exact_records = tuple(
        item
        for item in projection.row_records
        if item.calculation_basis == "quality_report_exact_row_location"
    )
    assert len(exact_records) == 1
    assert exact_records[0].source_row_id == 2
    assert all(
        item.metric_id != "fingerprint_series_summary"
        for item in projection.row_records
    )
    assert any(
        item.metric_id == "quality_report.status"
        for item in projection.sidecar_records
    )

    mismatched = _projection(
        (
            _Event(1_000_000_000, 1, 1.0, 1.0001),
            _Event(2_000_000_000, 2, 1.0, 1.0001),
        ),
        quality_report=report,
    )
    assert not any(
        item.calculation_basis == "quality_report_exact_row_location"
        for item in mismatched.row_records
    )


def test_ex_ante_projection_redacts_future_values_and_finding_counts() -> None:
    projection = _projection(
        (
            _Event(1_000_000_000, 1, 1.0, 1.0001),
            _Event(8_000_000_000, 2, 1.0, 1.1000),
        ),
        information_mode=ReconstructionEvidenceInformationMode.EX_ANTE_SIMULATION,
        as_of_ns=2_000_000_000,
        available_at_ns=10_000_000_000,
    )
    payload = projection.to_json()
    decision = reconstruction_evidence_use(
        (projection,), stage="source_enrichment", used_at_ns=2_000_000_000
    )

    assert "wide_spread_count" not in payload
    assert "suspicious_gap_count" not in payload
    assert "future_aggregate_evidence" in payload
    assert "future_values_and_future_finding_counts_not_retained" in payload
    assert all(
        item.available_at_ns <= item.as_of_ns for item in projection.row_records
    )
    assert decision.status in {
        ReconstructionEvidenceUseStatus.APPLIED,
        ReconstructionEvidenceUseStatus.NOT_APPLICABLE,
    }
    assert decision.effects["source_quality_score"] == pytest.approx(1.0)


def test_ex_ante_thresholds_do_not_depend_on_future_spreads() -> None:
    visible = (
        _Event(1_000_000_000, 1, 1.0, 1.0001),
        _Event(2_000_000_000, 2, 1.0, 1.0002),
    )
    baseline = _projection(
        visible,
        information_mode=ReconstructionEvidenceInformationMode.EX_ANTE_SIMULATION,
        as_of_ns=2_000_000_000,
        available_at_ns=10_000_000_000,
    )
    changed_future = _projection(
        (*visible, _Event(8_000_000_000, 3, 1.0, 9.0)),
        information_mode=ReconstructionEvidenceInformationMode.EX_ANTE_SIMULATION,
        as_of_ns=2_000_000_000,
        available_at_ns=10_000_000_000,
    )

    baseline_rows = tuple(item.to_dict() for item in baseline.row_records)
    changed_rows = tuple(item.to_dict() for item in changed_future.row_records)
    assert changed_rows == baseline_rows


def test_evidence_cannot_be_reused_before_its_declared_as_of_time() -> None:
    projection = _projection(
        (_Event(1_000_000_000, 1, 1.0, 1.0001),),
        as_of_ns=10_000_000_000,
        available_at_ns=1_000_000_000,
    )

    decision = reconstruction_evidence_use(
        (projection,), stage="proposal", used_at_ns=2_000_000_000
    )

    assert decision.status is ReconstructionEvidenceUseStatus.NOT_APPLICABLE
    assert not decision.consumed_record_ids


def test_evidence_use_exposes_thresholds_and_refuses_hard_row_fact() -> None:
    projection = _projection(
        (
            _Event(1_000_000_000, 1, 1.0, 1.0001),
            _Event(2_000_000_000, 2, 1.1, 1.0),
        )
    )
    decision = reconstruction_evidence_use(
        (projection,), stage="carving", used_at_ns=10_000_000_000
    )

    assert decision.status is ReconstructionEvidenceUseStatus.REFUSED
    assert decision.effects["max_anchor_gap_ns"] > 0
    assert decision.effects["wide_spread_threshold"] > 0
    assert decision.consumed_record_ids


def test_projection_is_bounded_and_reports_omissions() -> None:
    policy = ReconstructionEvidencePolicyV1(max_records=32, max_row_records=2)
    events = tuple(
        _Event(1_000_000_000 + index, index + 1, 1.0, 1.0)
        for index in range(20)
    )

    projection = _projection(events, policy=policy)

    assert len(projection.row_records) <= 2
    assert len(projection.records) <= 32
    assert projection.omitted_record_count > 0
    assert projection.status is ReconstructionEvidenceReadiness.LIMITED
    assert "evidence_records_truncated_by_policy" in projection.limitations


def test_unavailable_source_is_explicit_instead_of_inferred_as_clean() -> None:
    projection = _projection(())
    refused = reconstruction_evidence_use(
        (projection,), stage="source_enrichment", used_at_ns=10_000_000_000
    )
    permissive_policy = ReconstructionEvidencePolicyV1(
        fail_closed_on_source_unavailable=False
    )
    permissive_projection = _projection((), policy=permissive_policy)
    not_applicable = reconstruction_evidence_use(
        (permissive_projection,),
        stage="source_enrichment",
        used_at_ns=10_000_000_000,
        policy=permissive_policy,
    )

    assert projection.status is ReconstructionEvidenceReadiness.UNAVAILABLE
    assert len(projection.sidecar_records) == 1
    record = projection.sidecar_records[0]
    assert record.kind is ReconstructionEvidenceKind.UNAVAILABLE
    assert record.metric_id == "source_availability"
    assert record.value is None
    assert record.readiness is ReconstructionEvidenceReadiness.UNAVAILABLE
    assert refused.status is ReconstructionEvidenceUseStatus.REFUSED
    assert (
        not_applicable.status is ReconstructionEvidenceUseStatus.NOT_APPLICABLE
    )


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    ("severity", "expected_status", "expected_use"),
    (
        (None, "clean", ReconstructionEvidenceUseStatus.APPLIED),
        (
            QualitySeverity.WARNING,
            "warning",
            ReconstructionEvidenceUseStatus.APPLIED,
        ),
        (
            QualitySeverity.ERROR,
            "failed",
            ReconstructionEvidenceUseStatus.REFUSED,
        ),
    ),
)
def test_clean_warning_and_error_reports_retain_status_as_sidecar(
    severity: QualitySeverity | None,
    expected_status: str,
    expected_use: ReconstructionEvidenceUseStatus,
) -> None:
    target = QualityTarget(
        path="cache/.data",
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    findings = (
        ()
        if severity is None
        else (
            QualityFinding(
                severity=severity,
                code="SERIES_LEVEL_FINDING",
                message="bounded aggregate status fixture",
                rule_id="series.status.v1",
                target=target,
            ),
        )
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="series.status.v1",
                target=target,
                findings=findings,
            ),
        ),
    )

    projection = _projection(
        (_Event(1_000_000_000, 1, 1.0, 1.0001),),
        quality_report=report,
    )

    statuses = tuple(
        item.value
        for item in projection.sidecar_records
        if item.metric_id == "quality_report.status"
    )
    decision = reconstruction_evidence_use(
        (projection,), stage="source_enrichment", used_at_ns=10_000_000_000
    )
    assert statuses == (expected_status,)
    assert decision.status is expected_use
    assert decision.consumed_record_ids
    if severity is QualitySeverity.WARNING:
        assert decision.effects["source_quality_score"] == pytest.approx(0.95)


def test_every_supplied_evidence_surface_produces_bounded_lineage() -> None:
    projection = _projection(
        (_Event(1_000_000_000, 1, 1.0, 1.0001),),
        quality_payload={"unsupported_quality_field": "retained-by-reference"},
        fingerprint_payload={"fingerprint_id": "fingerprint:test"},
        classification_profile={"calendar_profile_complete": True},
    )

    metrics = {item.metric_id for item in projection.sidecar_records}
    assert "quality_payload.supplied" in metrics
    assert "fingerprint.fingerprint_id" in metrics
    assert "classification_profile.calendar_profile_complete" in metrics
    external = tuple(
        item
        for item in projection.sidecar_records
        if item.metric_id.startswith(
            (
                "quality_payload.",
                "fingerprint.",
                "classification_profile.",
            )
        )
    )
    assert len(external) == 3
    assert all(len(item.source_artifact_sha256) == 64 for item in external)
    assert all(item.source_artifact_sha256 != "a" * 64 for item in external)
    assert all(
        item.source_artifact_id.endswith(item.source_artifact_sha256)
        for item in external
    )


def test_projection_support_is_half_open_and_rejects_out_of_window_rows() -> (
    None
):
    with pytest.raises(ValueError, match="outside evidence support"):
        _projection((_Event(10_000_000_000, 1, 1.0, 1.0001),))

    projection = _projection(
        (
            _Event(0, 1, 1.0, 1.0001),
            _Event(9_999_999_999, 2, 1.0, 1.0001),
        )
    )

    assert any(
        item.metric_id == "row_count" and item.value == 2
        for item in projection.sidecar_records
    )
    assert projection.symbol == "eurusd"
    assert all(item.symbol == projection.symbol for item in projection.records)


def test_policy_reserves_mandatory_sidecar_capacity() -> None:
    with pytest.raises(ValueError, match="reserve capacity"):
        ReconstructionEvidencePolicyV1(max_records=24, max_row_records=1)


def test_legacy_and_enriched_cache_evidence_are_explicit_and_reconciled() -> (
    None
):
    events = (
        _Event(1_000_000_000, 1, 1.0, 1.0001),
        _Event(2_000_000_000, 2, 1.0, 1.0001),
    )
    legacy = _projection(events)
    enriched = _projection(
        events,
        source_cache_schema_version=HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION,
        cached_row_evidence={
            2: {"precision_warning": True, "fingerprint_unready": True}
        },
        cached_row_evidence_complete=True,
    )

    assert "legacy_cache_row_quality_columns_unavailable" in legacy.limitations
    assert any(
        item.metric_id == "source_cache_schema_version"
        and item.value == HISTDATA_LEGACY_CACHE_SCHEMA_VERSION
        for item in legacy.sidecar_records
    )
    assert {
        item.metric_id
        for item in enriched.row_records
        if item.calculation_basis == "histdata_enriched_cache_row_flag"
    } == {"precision_warning", "fingerprint_unready"}
    assert any(
        item.metric_id == "cached_row_evidence_additional_count"
        and item.value == 2
        for item in enriched.sidecar_records
    )
    with pytest.raises(ValueError, match="differs from source rows"):
        _projection(
            events,
            source_cache_schema_version=(
                HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION
            ),
            cached_row_evidence={1: {"duplicate_timestamp": True}},
            cached_row_evidence_complete=True,
        )


def test_enriched_non_monotonic_flag_reconciles_in_immutable_row_order() -> (
    None
):
    projection = _projection(
        (
            _Event(2_000_000_000, 1, 1.0, 1.0001),
            _Event(1_000_000_000, 2, 1.0, 1.0001),
        ),
        source_cache_schema_version=HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION,
        cached_row_evidence={2: {"non_monotonic_timestamp": True}},
        cached_row_evidence_complete=True,
    )

    assert any(
        item.metric_id == "non_monotonic_timestamp" and item.source_row_id == 2
        for item in projection.row_records
    )
    assert any(
        item.metric_id == "non_monotonic_timestamp_count" and item.value == 1
        for item in projection.sidecar_records
    )


def test_hard_aggregate_count_refuses_even_when_row_facts_are_truncated() -> (
    None
):
    policy = ReconstructionEvidencePolicyV1(max_records=32, max_row_records=1)
    projection = _projection(
        (
            _Event(1_000_000_000, 1, 1.1, 1.0),
            _Event(2_000_000_000, 2, 1.2, 1.0),
        ),
        policy=policy,
    )

    decision = reconstruction_evidence_use(
        (projection,),
        stage="source_enrichment",
        used_at_ns=10_000_000_000,
        policy=policy,
    )

    assert projection.omitted_record_count > 0
    assert any(
        item.metric_id == "negative_spread_count" and item.value == 2
        for item in projection.sidecar_records
    )
    assert decision.status is ReconstructionEvidenceUseStatus.REFUSED


def test_exact_external_hard_finding_retains_report_provenance_and_refuses() -> (
    None
):
    target = QualityTarget(
        path="cache/.data",
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="ticks.spread.v1",
                target=target,
                findings=(
                    QualityFinding(
                        severity=QualitySeverity.ERROR,
                        code="ASCII_TICK_NEGATIVE_SPREAD",
                        message="exact external hard finding",
                        rule_id="ticks.spread.v1",
                        target=target,
                        location=QualityLocation(
                            row_number=1, timestamp_utc_ms=1_000
                        ),
                    ),
                ),
            ),
        ),
    )
    projection = _projection(
        (_Event(1_000_000_000, 1, 1.0, 1.0001),),
        quality_report=report,
    )
    exact = next(
        item
        for item in projection.row_records
        if item.calculation_basis == "quality_report_exact_row_location"
    )
    decision = reconstruction_evidence_use(
        (projection,), stage="source_enrichment", used_at_ns=10_000_000_000
    )

    assert exact.source_artifact_sha256 != projection.source_artifact_sha256
    assert exact.source_artifact_id.endswith(exact.source_artifact_sha256)
    assert any(
        item.metric_id == "quality_report.reported_negative_spread_count"
        for item in projection.sidecar_records
    )
    assert decision.status is ReconstructionEvidenceUseStatus.REFUSED


def test_training_enrichment_consumes_profile_thresholds_without_changing_observed() -> (
    None
):
    frame = pl.DataFrame(
        {
            "datetime": [1_000, 2_000],
            "bid": [1.0, 1.0],
            "ask": [1.0001, 1.0005],
            "vol": [0, 0],
        }
    )

    enriched = enrich_tick_cache_with_training_features(
        frame,
        symbol="EURUSD",
        data_format="ascii",
        timeframe="T",
        period="201202",
        classification_profile={
            "thresholds": {
                "suspicious_gap_ms": 500,
                "wide_spread_threshold": 0.0002,
            }
        },
    )

    assert (
        enriched.get_column("bid").to_list()
        == frame.get_column("bid").to_list()
    )
    assert (
        enriched.get_column("ask").to_list()
        == frame.get_column("ask").to_list()
    )
    assert (
        enriched.get_column("datetime").to_list()
        == frame.get_column("datetime").to_list()
    )
    assert (
        enriched.get_column("vol").to_list()
        == frame.get_column("vol").to_list()
    )
    assert enriched.get_column("dq_issue_suspicious_gap").to_list() == [
        False,
        True,
    ]
    assert enriched.get_column("dq_issue_wide_spread").to_list() == [
        False,
        True,
    ]


def test_training_enrichment_projects_only_exact_external_row_findings() -> (
    None
):
    frame = pl.DataFrame(
        {
            "datetime": [1_000, 2_000],
            "bid": [1.0, 1.0],
            "ask": [1.0001, 1.0001],
            "vol": [0, 0],
        }
    )
    target = QualityTarget(
        path="cache/.data",
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
    )
    exact = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="ASCII_TICK_INVALID_ROW",
        message="external exact row finding",
        rule_id="ticks.validity.v1",
        target=target,
        location=QualityLocation(row_number=2, timestamp_utc_ms=2_000),
    )
    aggregate = QualityFinding(
        severity=QualitySeverity.ERROR,
        code="ASCII_TICK_NEGATIVE_SPREAD",
        message="aggregate finding has no row identity",
        rule_id="ticks.spread.v1",
        target=target,
    )
    report = QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="ticks.validity.v1", target=target, findings=(exact,)
            ),
            QualityRuleResult(
                rule_id="ticks.spread.v1", target=target, findings=(aggregate,)
            ),
        ),
    )

    enriched = enrich_tick_cache_with_training_features(
        frame, target=target, quality_report=report
    )

    assert enriched.get_column("dq_issue_invalid_row").to_list() == [
        False,
        True,
    ]
    assert enriched.get_column("dq_issue_negative_spread").to_list() == [
        False,
        False,
    ]

    wrong_target = QualityTarget(
        path="cache/.data",
        kind=QualityTargetKind.CACHE,
        data_format="ascii",
        timeframe="T",
        symbol="GBPUSD",
        period="201202",
    )
    mismatched = QualityReport(
        targets=(wrong_target,),
        rule_results=(
            QualityRuleResult(
                rule_id="ticks.validity.v1",
                target=wrong_target,
                findings=(
                    QualityFinding(
                        severity=QualitySeverity.ERROR,
                        code="ASCII_TICK_INVALID_ROW",
                        message="wrong series",
                        rule_id="ticks.validity.v1",
                        target=wrong_target,
                        location=QualityLocation(
                            row_number=2, timestamp_utc_ms=2_000
                        ),
                    ),
                ),
            ),
        ),
    )
    without_mismatch = enrich_tick_cache_with_training_features(
        frame, target=target, quality_report=mismatched
    )
    assert without_mismatch.get_column("dq_issue_invalid_row").to_list() == [
        False,
        False,
    ]

    with pytest.raises(ValueError, match="already enriched"):
        enrich_tick_cache_with_training_features(
            enriched, target=target, quality_report=report
        )


def _projection(
    events: tuple[_Event, ...],
    *,
    information_mode: ReconstructionEvidenceInformationMode = (
        ReconstructionEvidenceInformationMode.EX_POST_RECONSTRUCTION
    ),
    as_of_ns: int = 10_000_000_000,
    available_at_ns: int = 10_000_000_000,
    policy: ReconstructionEvidencePolicyV1 | None = None,
    quality_report: QualityReport | None = None,
    quality_payload: dict[str, JSONValue] | None = None,
    fingerprint_payload: dict[str, JSONValue] | None = None,
    classification_profile: dict[str, JSONValue] | None = None,
    source_cache_schema_version: str = HISTDATA_LEGACY_CACHE_SCHEMA_VERSION,
    cached_row_evidence: dict[int, dict[str, JSONScalar]] | None = None,
    cached_row_evidence_complete: bool = False,
) -> PointInTimeEvidenceProjectionV1:
    return compile_histdata_point_in_time_evidence(
        events,
        evidence_window_id="window-1",
        source_partition_id="partition-1",
        source_artifact_id="histdata_ascii_tick_arrow:sha256:" + "a" * 64,
        source_artifact_sha256="a" * 64,
        symbol="EURUSD",
        period="201202",
        support_start_ns=0,
        support_end_ns=10_000_000_000,
        available_at_ns=available_at_ns,
        as_of_ns=as_of_ns,
        information_mode=information_mode,
        policy=policy,
        quality_report=quality_report,
        quality_payload=quality_payload,
        fingerprint_payload=fingerprint_payload,
        classification_profile=classification_profile,
        source_cache_schema_version=source_cache_schema_version,
        cached_row_evidence=cached_row_evidence,
        cached_row_evidence_complete=cached_row_evidence_complete,
    )

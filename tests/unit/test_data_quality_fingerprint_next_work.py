"""Tests for saved-report next fingerprint work recommendations."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from histdatacom.data_quality.contracts import (
    QualityReport,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.fingerprint_discovery import (
    fingerprint_schema_discovery,
)
from histdatacom.data_quality.fingerprint_next_work import (
    FINGERPRINT_NEXT_WORK_SCHEMA_VERSION,
    fingerprint_next_work_recommendation,
    format_fingerprint_next_work,
)
from histdatacom.data_quality.fingerprints import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
)
from histdatacom.data_quality.remediation_audit import load_quality_report
from histdatacom.data_quality.reporting import quality_report_to_json
from histdatacom.data_quality.training_features import TRAINING_SCHEMA_VERSION
from histdatacom.runtime_contracts import JSONValue


def test_next_work_ranks_widespread_missing_section_first() -> None:
    """Missing evidence across more targets should outrank smaller gaps."""
    report = _report(
        targets=(
            _target("EURUSD"),
            _target("GBPUSD"),
            _target("USDJPY"),
        ),
        target_risks=(
            _target_risk(
                "EURUSD",
                "dependence",
                status="missing",
                score=35,
                reason="not_emitted",
            ),
            _target_risk(
                "GBPUSD",
                "dependence",
                status="missing",
                score=35,
                reason="not_emitted",
            ),
            _target_risk(
                "USDJPY",
                "stationarity_diagnostics",
                status="missing",
                score=50,
                reason="insufficient_samples",
            ),
        ),
    )

    payload = fingerprint_next_work_recommendation([("quality.json", report)])

    assert payload["schema_version"] == FINGERPRINT_NEXT_WORK_SCHEMA_VERSION
    assert payload["status"] == "recommended"
    assert payload["recommendation"]["capability"] == "dependence"
    assert payload["recommendation"]["affected_target_count"] == 2
    assert payload["recommendation"]["reason_codes"] == ["not_emitted"]


def test_next_work_handles_no_fingerprint_and_low_risk_reports() -> None:
    """No evidence and clean evidence without roadmap gaps should be no-work."""
    no_fingerprint = QualityReport(targets=(_target("EURUSD"),))
    no_evidence = fingerprint_next_work_recommendation(
        [("plain.json", no_fingerprint)]
    )
    assert no_evidence["status"] == "no_work"
    assert no_evidence["recommendation"] is None

    discovery = deepcopy(fingerprint_schema_discovery())
    discovery["sections"]["planned"]["target_sections"] = []
    discovery["sections"]["planned"]["run_sections"] = []
    clean = _report(
        targets=(_target("EURUSD"),),
        target_risks=(),
        section_status_counts={"stationarity_diagnostics": {"valid": 1}},
    )
    low_risk = fingerprint_next_work_recommendation(
        [("clean.json", clean)],
        discovery=discovery,
    )
    assert low_risk["status"] == "no_work"
    assert low_risk["recommendation_count"] == 0


def test_next_work_ignores_legacy_m1_as_a_base_grain() -> None:
    """M1 risk must not displace the supported ascii/T recommendation."""
    report = _report(
        targets=(
            _target("EURUSD", timeframe="M1"),
            _target("GBPUSD"),
        ),
        target_risks=(
            _target_risk(
                "EURUSD",
                "decomposition",
                status="missing",
                score=500,
                reason="not_emitted",
                timeframe="M1",
            ),
            _target_risk(
                "GBPUSD",
                "dependence",
                status="limited",
                score=10,
                reason="insufficient_samples",
            ),
        ),
    )

    payload = fingerprint_next_work_recommendation([("mixed.json", report)])

    assert payload["recommendation"]["capability"] == "dependence"
    assert payload["basis"]["ignored_non_base_target_count"] == 1
    assert payload["basis"]["base_grain"] == {
        "data_format": "ascii",
        "timeframe": "T",
    }


def test_next_work_uses_current_discovery_and_historical_decomposition_case() -> (
    None
):
    """A planned decomposition is recommended only with stationarity evidence."""
    discovery = deepcopy(fingerprint_schema_discovery())
    implemented = discovery["sections"]["implemented"]["target_sections"]
    discovery["sections"]["implemented"]["target_sections"] = [
        row for row in implemented if row["name"] != "decomposition"
    ]
    discovery["sections"]["planned"]["target_sections"] = [
        {
            "name": "decomposition",
            "status": "planned",
            "schema_version": None,
            "issue": "#330",
        }
    ]
    report = _report(
        targets=(_target("EURUSD"),),
        target_risks=(),
        section_status_counts={"stationarity_diagnostics": {"valid": 1}},
    )

    historical = fingerprint_next_work_recommendation(
        [("stationary.json", report)],
        discovery=discovery,
    )
    current = fingerprint_next_work_recommendation(
        [("stationary.json", report)]
    )

    assert historical["recommendation"]["capability"] == "decomposition"
    assert historical["recommendation"]["issue_reference"] == "#330"
    assert historical["recommendation"]["prerequisite_evidence"][0] == {
        "section": "stationarity_diagnostics",
        "implemented": True,
        "observed_valid_target_count": 1,
        "ready": True,
        "basis": "readiness_section_status_counts",
    }
    assert current["recommendation"]["capability"] == "synthetic_constraints"
    assert current["recommendation"]["issue_reference"] == "#333"


def test_next_work_bounds_alternates_axes_and_breaks_ties() -> None:
    """Candidate and axis ordering should be deterministic and explicitly bounded."""
    report = _report(
        targets=tuple(_target(symbol) for symbol in ("EURUSD", "GBPUSD")),
        target_risks=tuple(
            _target_risk(
                symbol,
                section,
                status="missing",
                score=35,
                reason="not_emitted",
            )
            for section in ("stationarity_diagnostics", "dependence")
            for symbol in ("EURUSD", "GBPUSD")
        ),
    )

    payload = fingerprint_next_work_recommendation(
        [("bounded.json", report)],
        alternate_limit=0,
        target_axis_limit=1,
    )

    assert payload["recommendation"]["capability"] == "dependence"
    assert payload["alternates"] == []
    assert payload["truncated"] is True
    assert payload["limit_metadata"]["alternates"]["truncated"] is True
    axes_limit = payload["recommendation"]["target_axis_limit_metadata"]
    assert axes_limit["included_count"] == 1
    assert axes_limit["omitted_count"] == 1
    assert axes_limit["truncated"] is True


def test_next_work_aggregates_multiple_reports_and_target_kinds() -> None:
    """Multiple saved reports should contribute to one product recommendation."""
    csv_report = _report(
        targets=(_target("EURUSD"),),
        target_risks=(
            _target_risk(
                "EURUSD",
                "dependence",
                status="missing",
                score=35,
                reason="not_emitted",
            ),
        ),
    )
    cache_report = _report(
        targets=(_target("GBPUSD", kind=QualityTargetKind.CACHE),),
        target_risks=(
            _target_risk(
                "GBPUSD",
                "dependence",
                status="limited",
                score=20,
                reason="insufficient_samples",
                kind="cache",
            ),
        ),
    )

    payload = fingerprint_next_work_recommendation(
        [("csv.json", csv_report), ("cache.json", cache_report)]
    )

    assert payload["input_report_count"] == 2
    assert payload["recommendation"]["capability"] == "dependence"
    assert payload["recommendation"]["affected_target_count"] == 2
    assert payload["recommendation"]["reason_codes"] == [
        "insufficient_samples",
        "not_emitted",
    ]
    assert {
        axis["kind"]
        for axis in payload["recommendation"]["representative_target_axes"]
    } == {"csv", "cache"}


def test_next_work_reports_surface_and_training_cross_series_evidence() -> None:
    """Recommendation basis should retain bounded training/cross-series facts."""
    report = _report(
        targets=(_target("EURUSD", kind=QualityTargetKind.CACHE),),
        target_risks=(),
        report_surface_evidence={
            "report_metadata_state_counts": {"present": 8, "missing": 1},
        },
        cross_series={
            "status": "limited",
            "group_count": 1,
            "incomplete_group_count": 0,
            "cache_source_counts": {"direct": 1},
            "row_identity": {
                "columns": [
                    "series_id",
                    "period",
                    "row_id",
                    "source_row_number",
                    "event_seq",
                ],
                "training_schema_version": TRAINING_SCHEMA_VERSION,
                "duplicate_timestamp_row_count": 2,
            },
            "groups": [{"coverage_ranges": {"unequal_ranges": True}}],
            "triangular_consistency": {
                "candidate_count": 1,
                "compared_timestamp_count": 3,
            },
        },
    )

    payload = fingerprint_next_work_recommendation([("cache.json", report)])
    training = payload["basis"]["training_substrate"]
    cross = payload["basis"]["cross_series"]

    assert payload["recommendation"]["capability"] == (
        "fingerprint_report_surfaces"
    )
    assert training["training_facing_columns_status"] == "confirmed"
    assert training["single_row_training_surface"] is True
    assert training["observed_enriched_cache_projection"] is True
    assert cross["duplicate_timestamp_row_count"] == 2
    assert cross["unequal_range_group_count"] == 1
    assert cross["triangle_candidate_count"] == 1


def test_next_work_is_publish_safe_and_does_not_mutate_report_golden(
    tmp_path: Path,
) -> None:
    """Report identity should be stable and the source report unchanged."""
    fixture = Path(
        "tests/fixtures/data_quality_reports/fingerprint_report.json"
    )
    report = load_quality_report(fixture)
    before = quality_report_to_json(report)

    first = fingerprint_next_work_recommendation(
        [(str(tmp_path / "fingerprint-report.json"), report)]
    )
    second = fingerprint_next_work_recommendation(
        [(str(tmp_path / "fingerprint-report.json"), report)]
    )

    assert first == second
    assert first["input_reports"][0]["report_name"] == (
        "fingerprint-report.json"
    )
    assert len(first["input_reports"][0]["content_sha256"]) == 64
    assert str(tmp_path) not in str(first)
    assert quality_report_to_json(report) == before
    assert "Next fingerprint work" in format_fingerprint_next_work(first)


def _report(
    *,
    targets: tuple[QualityTarget, ...],
    target_risks: tuple[dict[str, JSONValue], ...],
    section_status_counts: dict[str, JSONValue] | None = None,
    report_surface_evidence: dict[str, JSONValue] | None = None,
    cross_series: dict[str, JSONValue] | None = None,
) -> QualityReport:
    risk: dict[str, JSONValue] = {
        "schema_version": "histdatacom.time-series-fingerprint-readiness-risk.v1",
        "target_count": len(targets),
        "risk_target_count": len(target_risks),
        "included_target_count": len(target_risks),
        "truncated": False,
        "section_status_counts": section_status_counts or {},
        "report_surface_evidence": report_surface_evidence or {},
        "target_risks": list(target_risks),
    }
    metadata: dict[str, JSONValue] = {
        TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY: risk,
    }
    if cross_series is not None:
        metadata[CROSS_SERIES_FINGERPRINT_METADATA_KEY] = cross_series
    return QualityReport(targets=targets, metadata=metadata)


def _target(
    symbol: str,
    *,
    timeframe: str = "T",
    kind: QualityTargetKind = QualityTargetKind.CSV,
) -> QualityTarget:
    return QualityTarget(
        path=f"data/{symbol}-{timeframe}.csv",
        kind=kind,
        data_format="ascii",
        timeframe=timeframe,
        symbol=symbol,
        period="201202",
    )


def _target_risk(
    symbol: str,
    section: str,
    *,
    status: str,
    score: int,
    reason: str,
    timeframe: str = "T",
    kind: str = "csv",
) -> dict[str, JSONValue]:
    return {
        "target_axis": {
            "data_format": "ascii",
            "kind": kind,
            "period": "201202",
            "symbol": symbol,
            "timeframe": timeframe,
        },
        "risk_score": score,
        "section_risks": [
            {
                "section": section,
                "status": status,
                "score": score,
                "reasons": [reason],
            }
        ],
    }

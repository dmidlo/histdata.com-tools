"""Tests for remediation-catalog completeness audits."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    KnownQualityFindingCode,
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    audit_remediation_catalog,
    audit_remediation_catalog_report_paths,
    discover_known_quality_findings,
    format_remediation_catalog_audit,
    remediation_catalog_audit_has_warning_error_gaps,
    remediation_catalog_audit_to_json,
    quality_report_to_json,
)


def test_remediation_catalog_audit_accepts_fully_mapped_warning_codes() -> None:
    """A mapped warning/error known-code set should pass without gaps."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "time.ascii.sequence",
                "ASCII_M1_DUPLICATE_TIMESTAMP",
                QualitySeverity.WARNING,
            ),
        )
    )

    assert payload["status"] == "covered"
    assert payload["summary"]["known_code_count"] == 1
    assert payload["summary"]["mapped_known_code_count"] == 1
    assert payload["summary"]["unmapped_warning_error_gap_count"] == 0
    assert not remediation_catalog_audit_has_warning_error_gaps(payload)
    assert payload["known_unmapped_codes"] == []


def test_remediation_catalog_audit_reports_mixed_mapped_and_unmapped() -> None:
    """Unmapped warning/error codes should be highlighted before INFO gaps."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "time.ascii.sequence",
                "ASCII_M1_DUPLICATE_TIMESTAMP",
                QualitySeverity.WARNING,
            ),
            _known(
                "time.ascii.sequence",
                "ASCII_M1_GRANULARITY_DRIFT",
                QualitySeverity.ERROR,
            ),
            _known(
                "fingerprint.series",
                "FINGERPRINT_SERIES_SUMMARY",
                QualitySeverity.INFO,
            ),
        )
    )

    assert payload["status"] == "needs-remediation-guidance"
    assert payload["summary"]["mapped_known_code_count"] == 1
    assert payload["summary"]["unmapped_known_code_count"] == 2
    assert payload["summary"]["unmapped_warning_error_code_count"] == 1
    assert payload["known_unmapped_codes"][0]["finding_code"] == (
        "ASCII_M1_GRANULARITY_DRIFT"
    )
    assert payload["known_unmapped_codes"][0]["max_severity"] == "error"
    assert remediation_catalog_audit_has_warning_error_gaps(payload)


def test_remediation_catalog_audit_maps_inventory_archive_batch() -> None:
    """The ZIP inventory remediation batch should reduce audit gaps."""
    inventory_archive_codes = (
        "HISTDATA_ZIP_FILENAME_INVALID",
        "HISTDATA_ZIP_MEMBER_FILENAME_INVALID",
        "ZIP_MEMBER_MISSING",
        "ZIP_MEMBER_UNEXPECTED",
        "ZIP_EXTRA_MEMBER",
        "ZIP_CRC_ERROR",
        "ZIP_CORRUPT",
        "ZIP_UNREADABLE",
    )
    payload = audit_remediation_catalog(
        known_findings=(
            *(
                _known(
                    "inventory.zip.integrity",
                    code,
                    (
                        QualitySeverity.WARNING
                        if code == "ZIP_EXTRA_MEMBER"
                        else QualitySeverity.ERROR
                    ),
                    source_family="inventory",
                )
                for code in inventory_archive_codes
            ),
            _known(
                "inventory.format_support",
                "HISTDATA_FORMAT_UNSUPPORTED",
                QualitySeverity.ERROR,
                source_family="inventory",
            ),
        )
    )
    encoded = remediation_catalog_audit_to_json(payload)

    assert payload["status"] == "needs-remediation-guidance"
    assert payload["summary"]["known_code_count"] == 9
    assert payload["summary"]["mapped_known_code_count"] == 8
    assert payload["summary"]["unmapped_warning_error_gap_count"] == 1
    assert payload["known_unmapped_codes"] == [
        {
            "finding_code": "HISTDATA_FORMAT_UNSUPPORTED",
            "included_source_count": 1,
            "mapped": False,
            "max_severity": "error",
            "occurrence_count": 1,
            "omitted_source_count": 0,
            "rule_id": "inventory.format_support",
            "severity_counts": {"error": 1},
            "source_count": 1,
            "source_family": "inventory",
            "source_family_counts": [
                {"count": 1, "source_family": "inventory"},
            ],
            "sources": [
                {
                    "count": 1,
                    "source": "data_quality/inventory.format_support.py:1",
                },
            ],
        },
    ]
    assert payload["ranked_gaps"][0]["finding_code"] == (
        "HISTDATA_FORMAT_UNSUPPORTED"
    )
    assert {
        str(item["finding_code"]) for item in payload["known_unmapped_codes"]
    }.isdisjoint(inventory_archive_codes)
    assert {
        str(item["finding_code"]) for item in payload["ranked_gaps"]
    }.isdisjoint(inventory_archive_codes)
    assert encoded == remediation_catalog_audit_to_json(payload)


def test_remediation_catalog_audit_keeps_info_only_gaps_advisory() -> None:
    """INFO-only missing guidance should be visible without failing the audit."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "fingerprint.series",
                "FINGERPRINT_SERIES_SUMMARY",
                QualitySeverity.INFO,
            ),
        )
    )

    assert payload["status"] == "covered"
    assert payload["summary"]["unmapped_info_only_code_count"] == 1
    assert payload["summary"]["unmapped_warning_error_gap_count"] == 0
    assert not remediation_catalog_audit_has_warning_error_gaps(payload)
    assert "INFO-only unmapped known codes" in format_remediation_catalog_audit(
        payload
    )


def test_remediation_catalog_audit_truncates_deterministically() -> None:
    """Bounded output should keep deterministic warning/error ordering."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known("rule.b", "B_CODE", QualitySeverity.WARNING),
            _known("rule.a", "C_CODE", QualitySeverity.ERROR),
            _known("rule.a", "A_CODE", QualitySeverity.ERROR),
            _known(
                "rule.a",
                "A_CODE",
                QualitySeverity.ERROR,
                source="data_quality/a.py:2",
            ),
        ),
        code_limit=2,
        rule_limit=1,
        source_limit=1,
    )

    included = payload["known_unmapped_codes"]

    assert [item["finding_code"] for item in included] == [
        "A_CODE",
        "C_CODE",
    ]
    assert included[0]["occurrence_count"] == 2
    assert included[0]["omitted_source_count"] == 1
    assert payload["payload_limits"]["known_unmapped_codes"] == {
        "included_count": 2,
        "limit": 2,
        "omitted_count": 1,
        "total_count": 3,
        "truncated": True,
    }
    assert len(payload["known_code_counts"]["rule_id_counts"]) == 1


def test_remediation_catalog_audit_ranks_report_observed_gaps(
    tmp_path: Path,
) -> None:
    """Report frequency should move otherwise similar gaps up the backlog."""
    report = _report_with_findings(
        tmp_path,
        (
            _finding(
                tmp_path,
                "rule.a",
                "A_CODE",
                QualitySeverity.ERROR,
            ),
            _finding(
                tmp_path,
                "rule.b",
                "B_CODE",
                QualitySeverity.ERROR,
            ),
            _finding(
                tmp_path,
                "rule.b",
                "B_CODE",
                QualitySeverity.ERROR,
            ),
        ),
    )

    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "rule.a",
                "A_CODE",
                QualitySeverity.ERROR,
                source_family="bars",
            ),
            _known(
                "rule.b",
                "B_CODE",
                QualitySeverity.ERROR,
                source_family="ticks",
            ),
        ),
        reports=(("reports/quality.json", report),),
    )

    ranked = payload["ranked_gaps"]

    assert ranked[0]["rank"] == 1
    assert ranked[0]["finding_code"] == "B_CODE"
    assert ranked[0]["report_occurrence_count"] == 2
    assert ranked[0]["source_family"] == "ticks"
    assert "report_occurrences=2" in ranked[0]["rank_reasons"]
    assert "Ranked remediation gaps" in format_remediation_catalog_audit(
        payload
    )


def test_remediation_catalog_audit_ranks_report_only_gaps(
    tmp_path: Path,
) -> None:
    """Report-only gaps should appear in the prioritized backlog."""
    report = _report_with_findings(
        tmp_path,
        (
            _finding(
                tmp_path,
                "time.ascii.sequence",
                "ASCII_M1_GRANULARITY_DRIFT",
                QualitySeverity.ERROR,
            ),
            _finding(
                tmp_path,
                "time.ascii.sequence",
                "ASCII_M1_GRANULARITY_DRIFT",
                QualitySeverity.ERROR,
            ),
        ),
    )

    payload = audit_remediation_catalog(
        known_findings=(),
        reports=(("reports/quality.json", report),),
        source_limit=1,
    )
    ranked = payload["ranked_gaps"]

    assert payload["summary"]["unmapped_warning_error_gap_count"] == 1
    assert ranked[0]["rank"] == 1
    assert ranked[0]["finding_code"] == "ASCII_M1_GRANULARITY_DRIFT"
    assert ranked[0]["rule_id"] == "time.ascii.sequence"
    assert ranked[0]["source_family"] == "time"
    assert ranked[0]["known_source_occurrence_count"] == 0
    assert ranked[0]["report_occurrence_count"] == 2
    assert ranked[0]["reports"] == [
        {
            "count": 1,
            "source": "reports/quality.json",
        }
    ]
    assert "report_occurrences=2" in ranked[0]["rank_reasons"]


def test_discover_known_quality_findings_resolves_source_attribution(
    tmp_path: Path,
) -> None:
    """Static discovery should avoid placeholder rule IDs where possible."""
    source = tmp_path / "ticks.py"
    source.write_text(
        """
ASCII_TICK_SPREAD_RULE_ID = "ticks.ascii.spread"


class HistDataAsciiTickSpreadRule:
    rule_id: str = ASCII_TICK_SPREAD_RULE_ID

    def evaluate(self, target):
        return _finding(
            target,
            code="ASCII_TICK_NEGATIVE_SPREAD",
            rule_id=self.rule_id,
            severity=QualitySeverity.ERROR,
        )


def bundled(target, rules):
    spread_rule = rules[0]
    assert isinstance(spread_rule, HistDataAsciiTickSpreadRule)
    return _finding(
        target,
        code="ASCII_TICK_SPREAD_METADATA_UNSUPPORTED",
        rule_id=spread_rule.rule_id,
    )


def helper(target, rule_id: str):
    return _finding(
        target,
        code="ASCII_TICK_BID_ASK_INVALID",
        rule_id=rule_id,
    )


def source_error():
    raise _SourceReadError(
        code="ASCII_TICK_CACHE_SCHEMA_UNSUPPORTED",
        message="cache missing",
    )
""",
        encoding="utf-8",
    )

    findings = {
        item.finding_code: item
        for item in discover_known_quality_findings(tmp_path)
    }

    assert findings["ASCII_TICK_NEGATIVE_SPREAD"].rule_id == (
        "ticks.ascii.spread"
    )
    assert findings["ASCII_TICK_SPREAD_METADATA_UNSUPPORTED"].rule_id == (
        "ticks.ascii.spread"
    )
    assert findings["ASCII_TICK_BID_ASK_INVALID"].rule_id == (
        "ticks.unresolved"
    )
    assert findings["ASCII_TICK_CACHE_SCHEMA_UNSUPPORTED"].rule_id == (
        "ticks.unresolved"
    )
    assert {item.source_family for item in findings.values()} == {"ticks"}


def test_remediation_catalog_audit_uses_report_coverage_and_sanitizes_paths(
    tmp_path: Path,
) -> None:
    """Saved report evidence should reuse remediation coverage semantics."""
    report = _report_with_findings(
        tmp_path,
        (
            _finding(
                tmp_path,
                "time.ascii.sequence",
                "ASCII_M1_DUPLICATE_TIMESTAMP",
                QualitySeverity.WARNING,
            ),
            _finding(
                tmp_path,
                "time.ascii.sequence",
                "ASCII_M1_GRANULARITY_DRIFT",
                QualitySeverity.ERROR,
            ),
        ),
    )
    report_path = tmp_path / "reports" / "quality.json"
    report_path.parent.mkdir()
    report_path.write_text(quality_report_to_json(report), encoding="utf-8")

    payload = audit_remediation_catalog_report_paths(
        (report_path,),
        known_findings=(),
    )
    encoded = json.dumps(payload, sort_keys=True)

    assert payload["summary"]["report_count"] == 1
    assert payload["summary"]["report_finding_count"] == 2
    assert payload["summary"]["report_unmapped_warning_error_group_count"] == 1
    assert payload["summary"]["unmapped_warning_error_gap_count"] == 1
    assert payload["report_coverage"][0]["source"] == "reports/quality.json"
    assert str(tmp_path) not in encoded


def test_remediation_catalog_audit_json_matches_golden_fixture() -> None:
    """The audit JSON contract should remain stable for report consumers."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "time.ascii.sequence",
                "ASCII_M1_DUPLICATE_TIMESTAMP",
                QualitySeverity.WARNING,
            ),
            _known(
                "time.ascii.sequence",
                "ASCII_M1_GRANULARITY_DRIFT",
                QualitySeverity.ERROR,
            ),
            _known(
                "fingerprint.series",
                "FINGERPRINT_SERIES_SUMMARY",
                QualitySeverity.INFO,
            ),
        ),
        code_limit=4,
        rule_limit=4,
        source_limit=2,
    )
    fixture = Path(
        "tests/fixtures/data_quality_reports/remediation_catalog_audit.json"
    )

    assert remediation_catalog_audit_to_json(payload) == fixture.read_text(
        encoding="utf-8"
    )


def _known(
    rule_id: str,
    finding_code: str,
    severity: QualitySeverity,
    *,
    source: str = "",
    source_family: str = "",
) -> KnownQualityFindingCode:
    return KnownQualityFindingCode(
        rule_id=rule_id,
        finding_code=finding_code,
        severity=severity,
        source=source or f"data_quality/{rule_id}.py:1",
        source_family=source_family or rule_id.split(".", 1)[0],
    )


def _finding(
    tmp_path: Path,
    rule_id: str,
    finding_code: str,
    severity: QualitySeverity,
) -> QualityFinding:
    target = QualityTarget(
        path=str(tmp_path / "DAT_ASCII_EURUSD_M1_201202.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    return QualityFinding(
        severity=severity,
        code=finding_code,
        message="Finding under audit.",
        rule_id=rule_id,
        target=target,
    )


def _report_with_findings(
    tmp_path: Path,
    findings: tuple[QualityFinding, ...],
) -> QualityReport:
    target = QualityTarget(
        path=str(tmp_path / "DAT_ASCII_EURUSD_M1_201202.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="M1",
        symbol="EURUSD",
        period="201202",
    )
    normalized = tuple(
        QualityFinding(
            severity=finding.severity,
            code=finding.code,
            message=finding.message,
            rule_id=finding.rule_id,
            target=target,
        )
        for finding in findings
    )
    return QualityReport(
        targets=(target,),
        rule_results=(
            QualityRuleResult(
                rule_id="time.ascii.sequence",
                target=target,
                findings=normalized,
            ),
        ),
    )

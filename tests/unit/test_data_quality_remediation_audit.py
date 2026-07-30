"""Tests for remediation-catalog completeness audits."""

from __future__ import annotations

import json
import os
from pathlib import Path

from histdatacom.data_quality import (
    QUALITY_REMEDIATION_PLAN_SCHEMA_VERSION,
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
                "ASCII_TICK_DUPLICATE_ROW",
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
                "ASCII_TICK_DUPLICATE_ROW",
                QualitySeverity.WARNING,
            ),
            _known(
                "time.ascii.est_no_dst",
                "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
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
        "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH"
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
            "actionability": "unsupported_format_or_capability",
            "actionability_reason": "unsupported_format_rule",
            "attribution_reason": "provided_rule_id",
            "attribution_reason_counts": {"provided_rule_id": 1},
            "attribution_status": "exact",
            "attribution_status_counts": {"exact": 1},
            "finding_code": "HISTDATA_FORMAT_UNSUPPORTED",
            "finding_code_prefix_counts": [
                {
                    "count": 1,
                    "finding_code_prefix": "HISTDATA_FORMAT",
                },
            ],
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
            "source_helper_counts": [],
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


def test_remediation_catalog_audit_ranks_actionable_gaps_before_boundaries() -> (
    None
):
    """Actionable defects should outrank more frequent support boundaries."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "inventory.format_support",
                "HISTDATA_FORMAT_UNSUPPORTED",
                QualitySeverity.ERROR,
                source_family="inventory",
            ),
            _known(
                "inventory.format_support",
                "HISTDATA_FORMAT_UNSUPPORTED",
                QualitySeverity.ERROR,
                source="data_quality/inventory.py:2",
                source_family="inventory",
            ),
            _known(
                "inventory.format_support",
                "HISTDATA_FORMAT_UNSUPPORTED",
                QualitySeverity.ERROR,
                source="data_quality/inventory.py:3",
                source_family="inventory",
            ),
            _known(
                "custom.rule",
                "CUSTOM_REPAIRABLE_FAILURE",
                QualitySeverity.WARNING,
            ),
            KnownQualityFindingCode(
                rule_id="time.unresolved",
                finding_code="CUSTOM_SHARED_FAILURE",
                severity=QualitySeverity.ERROR,
                source="data_quality/time.py:1",
                source_family="time",
                attribution_status="unresolved",
                attribution_reason="ambiguous_helper_rules",
            ),
            _known(
                "custom.diagnostics",
                "DIAGNOSTIC_CONTEXT_MISSING",
                QualitySeverity.WARNING,
            ),
            _known(
                "modeling.readiness",
                "MODELING_CALENDAR_REGIME_POLICY_MISSING",
                QualitySeverity.WARNING,
            ),
            _known(
                "custom.repair",
                "DESTRUCTIVE_REPAIR_REQUIRED",
                QualitySeverity.ERROR,
            ),
        )
    )

    ranked = payload["ranked_gaps"]
    summary = payload["summary"]

    assert ranked[0]["finding_code"] == "CUSTOM_REPAIRABLE_FAILURE"
    assert ranked[0]["actionability"] == "remediable_defect"
    assert ranked[1]["actionability"] == "needs_diagnostic_context"
    assert ranked[2]["actionability"] == "needs_rule_attribution"
    assert ranked[-1]["actionability"] == ("unsupported_format_or_capability")
    assert "actionability=remediable_defect" in ranked[0]["rank_reasons"]
    assert summary["unmapped_actionable_warning_error_code_count"] == 1
    assert summary["blocked_by_attribution_warning_error_code_count"] == 1
    assert (
        summary["blocked_by_missing_diagnostics_warning_error_code_count"] == 1
    )
    assert summary["intentionally_unremediable_warning_error_code_count"] == 3


def test_remediation_catalog_audit_emits_fixability_ranked_plan() -> None:
    """Plan items should turn exact actionable gaps into catalog-edit inputs."""
    payload = audit_remediation_catalog(
        known_findings=(
            _known(
                "custom.rule",
                "CUSTOM_INVALID_ROW",
                QualitySeverity.ERROR,
            ),
            _known(
                "inventory.format_support",
                "HISTDATA_FORMAT_UNSUPPORTED",
                QualitySeverity.ERROR,
            ),
        )
    )

    plan = payload["remediation_plan"]
    first = plan["items"][0]

    assert plan["schema_version"] == QUALITY_REMEDIATION_PLAN_SCHEMA_VERSION
    assert plan["plan_item_count"] == 2
    assert plan["included_plan_item_count"] == 2
    assert plan["truncated"] is False
    assert first["rank"] == 1
    assert first["catalog_gap_rank"] == 1
    assert first["finding_code"] == "CUSTOM_INVALID_ROW"
    assert first["suggested_selector"] == {
        "shape": "exact_rule_and_finding",
        "rule_id": "custom.rule",
        "finding_code": "CUSTOM_INVALID_ROW",
        "finding_code_prefix": "CUSTOM_INVALID_ROW",
        "confidence": "high",
        "basis": "exact_rule_attribution",
    }
    assert first["draft_hint_code"] == "repair_custom_invalid_row"
    assert first["suggested_action"] == {
        "action_kind": "repair",
        "confidence": "high",
        "basis": "finding_code_marker=invalid",
        "concrete": True,
    }
    assert first["fixability"]["level"] == "high"
    assert first["fixability"]["score"] == 96
    assert first["missing_fields"] == ["message"]
    assert plan["items"][1]["fixability"]["level"] == "low"
    assert "Remediation plan" in format_remediation_catalog_audit(payload)


def test_remediation_plan_marks_unresolved_attribution_blocked() -> None:
    """An unresolved rule must not become an apparently exact catalog plan."""
    payload = audit_remediation_catalog(
        known_findings=(
            KnownQualityFindingCode(
                rule_id="time.unresolved",
                finding_code="CUSTOM_INVALID_ROW",
                severity=QualitySeverity.ERROR,
                source="data_quality/time.py:1",
                source_family="time",
                attribution_status="unresolved",
                attribution_reason="ambiguous_helper_rules",
            ),
        )
    )
    item = payload["remediation_plan"]["items"][0]

    assert item["suggested_selector"]["shape"] == "finding_family"
    assert item["fixability"]["level"] == "blocked"
    assert item["fixability"]["score"] == 24
    assert item["missing_fields"] == [
        "message",
        "exact_rule_id",
        "action_kind_confirmation",
        "blocking_evidence",
    ]


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
    _assert_count_limit_metadata(
        payload["payload_limits"]["known_unmapped_codes"],
        limit=2,
        total_count=3,
        included_count=2,
        omitted_count=1,
        truncated=True,
        requested_limit=2,
        default_limit=16,
    )
    assert len(payload["known_code_counts"]["rule_id_counts"]) == 1
    plan = payload["remediation_plan"]
    assert plan["plan_item_count"] == 3
    assert plan["included_plan_item_count"] == 2
    assert plan["omitted_plan_item_count"] == 1
    assert plan["truncated"] is True
    _assert_count_limit_metadata(
        payload["payload_limits"]["remediation_plan"],
        limit=2,
        total_count=3,
        included_count=2,
        omitted_count=1,
        truncated=True,
        requested_limit=2,
        default_limit=16,
    )


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
                "time.ascii.est_no_dst",
                "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
                QualitySeverity.ERROR,
            ),
            _finding(
                tmp_path,
                "time.ascii.est_no_dst",
                "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
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
    assert ranked[0]["finding_code"] == "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH"
    assert ranked[0]["rule_id"] == "time.ascii.est_no_dst"
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
    plan_item = payload["remediation_plan"]["items"][0]
    assert plan_item["catalog_gap_rank"] == 1
    assert plan_item["suggested_selector"]["shape"] == (
        "exact_rule_and_finding"
    )
    assert plan_item["suggested_selector"]["basis"] == (
        "reported_rule_and_finding"
    )
    assert plan_item["evidence"]["known_source_occurrence_count"] == 0
    assert plan_item["evidence"]["report_occurrence_count"] == 2
    assert plan_item["evidence"]["reports"] == [
        {"count": 1, "source": "reports/quality.json"}
    ]


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
        "ticks.ascii.spread"
    )
    assert (
        findings["ASCII_TICK_BID_ASK_INVALID"].attribution_reason
        == "finding_code_prefix"
    )
    assert findings["ASCII_TICK_CACHE_SCHEMA_UNSUPPORTED"].rule_id == (
        "ticks.ascii.spread"
    )
    assert (
        findings["ASCII_TICK_CACHE_SCHEMA_UNSUPPORTED"].attribution_reason
        == "unique_module_rule"
    )
    assert {item.source_family for item in findings.values()} == {"ticks"}


def test_discovery_infers_constructor_assignments_and_helper_callers(
    tmp_path: Path,
) -> None:
    """Local rule objects and single-rule helper chains should be inferable."""
    source = tmp_path / "custom.py"
    source.write_text(
        """
CUSTOM_RULE_ID = "custom.rule"


class CustomRule:
    rule_id: str = CUSTOM_RULE_ID

    def evaluate(self, target):
        return source_error(target)


def source_error(target):
    return _finding(target, code="CUSTOM_SOURCE_UNREADABLE")


def local_rule(target):
    rule = CustomRule()
    return _finding(
        target,
        code="CUSTOM_LOCAL_RULE",
        rule_id=rule.rule_id,
    )


def typed_rule(target, rule: CustomRule):
    return _finding(
        target,
        code="CUSTOM_TYPED_RULE",
        rule_id=rule.rule_id,
    )


def default_rule(target, rule_id: str = CUSTOM_RULE_ID):
    return _finding(
        target,
        code="CUSTOM_DEFAULT_RULE",
        rule_id=rule_id,
    )
""",
        encoding="utf-8",
    )

    findings = {
        item.finding_code: item
        for item in discover_known_quality_findings(tmp_path)
    }

    assert findings["CUSTOM_SOURCE_UNREADABLE"].rule_id == "custom.rule"
    assert (
        findings["CUSTOM_SOURCE_UNREADABLE"].attribution_reason
        == "unique_helper_rule"
    )
    assert findings["CUSTOM_LOCAL_RULE"].rule_id == "custom.rule"
    assert (
        findings["CUSTOM_LOCAL_RULE"].attribution_reason == "local_rule_object"
    )
    assert findings["CUSTOM_TYPED_RULE"].rule_id == "custom.rule"
    assert (
        findings["CUSTOM_TYPED_RULE"].attribution_reason == "local_rule_object"
    )
    assert findings["CUSTOM_DEFAULT_RULE"].rule_id == "custom.rule"
    assert (
        findings["CUSTOM_DEFAULT_RULE"].attribution_reason
        == "unique_helper_rule"
    )


def test_discovery_preserves_ambiguous_family_fallback_with_reason(
    tmp_path: Path,
) -> None:
    """Helpers shared by multiple rules should remain explicitly unresolved."""
    source = tmp_path / "ticks.py"
    source.write_text(
        """
FIRST_RULE_ID = "ticks.first"
SECOND_RULE_ID = "ticks.second"


class FirstRule:
    rule_id: str = FIRST_RULE_ID

    def evaluate(self, target):
        return shared_error(target)


class SecondRule:
    rule_id: str = SECOND_RULE_ID

    def evaluate(self, target):
        return shared_error(target)


def shared_error(target):
    return _finding(target, code="CUSTOM_SHARED_FAILURE")
""",
        encoding="utf-8",
    )

    finding = discover_known_quality_findings(tmp_path)[0]
    payload = audit_remediation_catalog(known_findings=(finding,))
    gap = payload["ranked_gaps"][0]

    assert finding.rule_id == "ticks.unresolved"
    assert finding.attribution_status == "unresolved"
    assert finding.attribution_reason == "ambiguous_helper_rules"
    assert gap["attribution_status"] == "unresolved"
    assert gap["attribution_reason"] == "ambiguous_helper_rules"
    assert gap["source_helper_counts"] == [
        {"count": 1, "source_helper": "shared_error"},
    ]
    assert payload["summary"]["unresolved_attribution_occurrence_count"] == 1
    assert payload["known_code_counts"][
        "unresolved_finding_code_prefix_counts"
    ] == [{"count": 1, "finding_code_prefix": "CUSTOM_SHARED_FAILURE"}]
    for key in (
        "attribution_reason_counts",
        "unresolved_source_helper_counts",
        "unresolved_finding_code_prefix_counts",
    ):
        _assert_count_limit_metadata(
            payload["payload_limits"][key],
            limit=16,
            total_count=1,
            included_count=1,
            omitted_count=0,
            truncated=False,
            requested_limit=16,
            default_limit=16,
        )


def test_current_top_gaps_have_specific_rule_attribution() -> None:
    """Current high-priority families should not regress to broad rule IDs."""
    findings = discover_known_quality_findings()
    by_code = {item.finding_code: item for item in findings}

    assert by_code["ASCII_TICK_SPREAD_CACHE_SCHEMA_UNSUPPORTED"].rule_id == (
        "ticks.ascii.spread"
    )
    assert by_code["DOMAIN_CALENDAR_SOURCE_UNREADABLE"].rule_id == (
        "domain.calendar_sessions"
    )
    assert by_code["PROVENANCE_CACHE_METADATA_MISMATCH"].rule_id == (
        "provenance.manifest.lineage"
    )
    unresolved = [
        item for item in findings if item.attribution_status == "unresolved"
    ]
    assert {item.source_family for item in unresolved} <= {
        "ingestion",
        "time",
    }
    assert all(
        item.attribution_reason == "ambiguous_helper_rules"
        for item in unresolved
    )


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
                "ASCII_TICK_DUPLICATE_ROW",
                QualitySeverity.WARNING,
            ),
            _finding(
                tmp_path,
                "time.ascii.est_no_dst",
                "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
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
                "ASCII_TICK_DUPLICATE_ROW",
                QualitySeverity.WARNING,
            ),
            _known(
                "time.ascii.est_no_dst",
                "ASCII_TIMESTAMP_SOURCE_PERIOD_MISMATCH",
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
    encoded = remediation_catalog_audit_to_json(payload)
    if os.environ.get("HISTDATACOM_UPDATE_QUALITY_GOLDENS") == "1":
        fixture.write_text(encoded, encoding="utf-8")

    assert encoded == fixture.read_text(encoding="utf-8")


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
        path=str(tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="T",
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
        path=str(tmp_path / "DAT_ASCII_EURUSD_T_201202.csv"),
        kind=QualityTargetKind.CSV,
        data_format="ascii",
        timeframe="T",
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


def _assert_count_limit_metadata(
    value: object,
    *,
    limit: int,
    total_count: int,
    included_count: int,
    omitted_count: int,
    truncated: bool,
    requested_limit: int | None,
    default_limit: int,
) -> None:
    assert isinstance(value, dict)
    assert value["limit"] == limit
    assert value["effective_limit"] == limit
    assert value["requested_limit"] == requested_limit
    assert value["default_limit"] == default_limit
    assert value["total_count"] == total_count
    assert value["included_count"] == included_count
    assert value["omitted_count"] == omitted_count
    assert value["truncated"] is truncated

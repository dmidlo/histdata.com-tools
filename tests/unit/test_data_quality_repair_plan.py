"""Tests for non-mutating quality repair plans."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality import (
    QUALITY_REPAIR_PLAN_SCHEMA_VERSION,
    QualityFinding,
    QualityReport,
    QualityRuleResult,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
    format_quality_repair_plan,
    quality_repair_plan,
    quality_repair_plan_to_json,
)
from histdatacom.runtime_contracts import JSONValue

_ZIP_OPERATION_CASES: tuple[tuple[str, str], ...] = (
    ("ZIP_UNREADABLE", "restore_read_access"),
    ("ZIP_CORRUPT", "redownload_archive"),
    ("ZIP_CRC_ERROR", "redownload_archive"),
    ("HISTDATA_ZIP_FILENAME_INVALID", "rename_archive"),
    ("ZIP_MEMBER_MISSING", "restore_archive_member"),
    ("ZIP_MEMBER_UNEXPECTED", "rebuild_archive_members"),
    ("HISTDATA_ZIP_MEMBER_FILENAME_INVALID", "rename_archive_member"),
    ("ZIP_EXTRA_MEMBER", "inspect_archive_members"),
)

_FINDING_METADATA: dict[str, dict[str, JSONValue]] = {
    "ZIP_UNREADABLE": {"error_type": "PermissionError"},
    "ZIP_CORRUPT": {"error_type": "BadZipFile"},
    "ZIP_CRC_ERROR": {"bad_member": "DAT_ASCII_EURUSD_T_201202.csv"},
    "HISTDATA_ZIP_FILENAME_INVALID": {
        "observed_filename": "DAT_ASCII_EURUSD_T_201202_DIRTY.zip",
        "expected_filename": "DAT_ASCII_EURUSD_T_201202.zip",
        "accepted_filenames": [
            "DAT_ASCII_EURUSD_T_201202.zip",
            "HISTDATA_COM_ASCII_EURUSD_T201202.zip",
        ],
    },
    "ZIP_MEMBER_MISSING": {
        "expected_member": "DAT_ASCII_EURUSD_T_201202.csv",
        "observed_members": [],
    },
    "ZIP_MEMBER_UNEXPECTED": {
        "expected_member": "DAT_ASCII_EURUSD_T_201202.csv",
        "observed_members": ["DAT_ASCII_EURUSD_T_201201.csv"],
    },
    "HISTDATA_ZIP_MEMBER_FILENAME_INVALID": {
        "observed_member": "EURUSD.csv",
        "expected_member": "DAT_ASCII_EURUSD_T_201202.csv",
    },
    "ZIP_EXTRA_MEMBER": {
        "expected_member": "DAT_ASCII_EURUSD_T_201202.csv",
        "extra_members": ["README.txt"],
    },
}


def test_repair_plan_supports_initial_archive_operations() -> None:
    """Every #350 archive hint should produce a concrete manual operation."""
    for finding_code, category in _ZIP_OPERATION_CASES:
        payload = quality_repair_plan(_report(_finding(finding_code)))

        assert payload["schema_version"] == QUALITY_REPAIR_PLAN_SCHEMA_VERSION
        assert payload["mode"] == "non_mutating"
        assert payload["apply_supported"] is False
        assert payload["mutating_operations_performed"] is False
        item = payload["items"][0]
        assert item["finding_code"] == finding_code
        assert item["rule_id"] == "inventory.zip.integrity"
        assert item["remediation_hint_code"]
        assert item["action_kind"]
        assert item["operation"]["category"] == category
        assert item["operation"]["proposal_status"] == "proposed"
        assert item["operation"]["specificity"] == "exact"
        assert item["operation"]["automation_status"] == "manual_only"
        assert item["preconditions"]
        assert item["evidence_needed"]
        assert item["confidence"]["level"] == "high"


def test_repair_plan_keeps_missing_context_visible() -> None:
    """A supported operation should not pretend incomplete evidence is exact."""
    finding = _finding(
        "HISTDATA_ZIP_MEMBER_FILENAME_INVALID",
        metadata={"observed_member": "EURUSD.csv"},
    )

    item = quality_repair_plan(_report(finding))["items"][0]

    assert item["operation"]["category"] == "rename_archive_member"
    assert item["operation"]["proposal_status"] == "needs_context"
    assert item["operation"]["specificity"] == "contextual"
    assert item["operation"]["automation_status"] == "manual_only"
    assert item["missing_evidence"] == ["expected_member"]
    assert item["confidence"]["level"] == "medium"


def test_repair_plan_marks_unmapped_and_unsupported_actions() -> None:
    """Unknown findings and out-of-scope hints should be explicit boundaries."""
    payload = quality_repair_plan(
        _report(
            _finding("CUSTOM_UNKNOWN_FINDING", rule_id="custom.rule"),
            _finding(
                "ASCII_TICK_DUPLICATE_ROW",
                rule_id="time.ascii.sequence",
            ),
        )
    )

    items = {item["finding_code"]: item for item in payload["items"]}
    unmapped = items["CUSTOM_UNKNOWN_FINDING"]
    unsupported = items["ASCII_TICK_DUPLICATE_ROW"]
    assert unmapped["remediation_hint_code"] == ""
    assert unmapped["operation"]["proposal_status"] == "unsupported"
    assert unmapped["operation"]["reason"] == "unmapped_finding"
    assert unsupported["remediation_hint_code"] == (
        "inspect_duplicate_tick_rows"
    )
    assert unsupported["operation"]["reason"] == "unsupported_action"
    assert payload["proposal_status_counts"] == {"unsupported": 2}


def test_repair_plan_is_bounded_and_reports_truncation() -> None:
    """Item and per-item evidence limits should expose complete count metadata."""
    findings = tuple(
        _finding(code, path=f"quality-fixtures/{index}.zip")
        for index, (code, _category) in enumerate(_ZIP_OPERATION_CASES)
    )

    payload = quality_repair_plan(
        _report(*findings),
        item_limit=3,
        evidence_limit=1,
    )

    assert payload["plan_item_count"] == 8
    assert payload["included_plan_item_count"] == 3
    assert payload["omitted_plan_item_count"] == 5
    assert payload["truncated"] is True
    assert len(payload["items"]) == 3
    assert payload["payload_limits"]["items"] == {
        "default_limit": 16,
        "effective_limit": 3,
        "included_count": 3,
        "limit": 3,
        "maximum_limit": 64,
        "minimum_limit": 0,
        "omitted_count": 5,
        "requested_limit": 3,
        "total_count": 8,
        "truncated": True,
        "unbounded": False,
    }
    assert all(
        item["evidence"]["included_count"] <= 1 for item in payload["items"]
    )


def test_repair_plan_is_publish_safe_and_excludes_raw_diagnostics() -> None:
    """Plans should sanitize local paths and omit arbitrary finding metadata."""
    finding = _finding(
        "ZIP_CORRUPT",
        path="/Users/alice/private/market/DAT_ASCII_EURUSD_T_201202.zip",
        metadata={
            "error_type": "BadZipFile",
            "error": (
                "token=secret at /Users/alice/private/market/"
                "DAT_ASCII_EURUSD_T_201202.zip"
            ),
            "raw_rows": ["sensitive row"],
        },
    )

    payload = quality_repair_plan(
        _report(finding),
        report_path="/Users/alice/private/reports/quality.json",
    )
    encoded = quality_repair_plan_to_json(payload)

    assert "/Users/alice" not in encoded
    assert "token=secret" not in encoded
    assert "sensitive row" not in encoded
    assert payload["input_report"]["path"] == "reports/quality.json"
    assert payload["items"][0]["target"]["path"] == (
        "DAT_ASCII_EURUSD_T_201202.zip"
    )


def test_repair_plan_preserves_bounded_topology_context_without_raw_samples() -> (
    None
):
    """Existing #343 context should remain useful without copying row samples."""
    finding = _finding(
        "CUSTOM_TOPOLOGY_FINDING",
        rule_id="fingerprint.series",
        metadata={
            "time_series_fingerprint": {
                "temporal_topology": {
                    "inspection_context": {
                        "schema_version": (
                            "histdatacom.timestamp-topology-inspection.v1"
                        ),
                        "duplicate_timestamps": {
                            "total_count": 4,
                            "included_count": 1,
                            "omitted_count": 3,
                            "truncated": True,
                            "samples": [
                                {
                                    "row_number": 42,
                                    "timestamp_source": "private raw value",
                                }
                            ],
                        },
                    }
                }
            }
        },
    )

    item = quality_repair_plan(_report(finding))["items"][0]
    evidence = item["evidence"]

    assert evidence["items"] == [
        {
            "kind": "inspection_context.duplicate_timestamps",
            "value": {
                "included_count": 1,
                "omitted_count": 3,
                "total_count": 4,
                "truncated": True,
            },
        }
    ]
    assert "private raw value" not in json.dumps(item)


def test_repair_plan_order_and_json_are_deterministic() -> None:
    """Input finding order should not change ranks or serialized JSON."""
    findings = tuple(_finding(code) for code, _category in _ZIP_OPERATION_CASES)
    forward = quality_repair_plan(_report(*findings))
    reverse = quality_repair_plan(_report(*reversed(findings)))

    assert forward == reverse
    assert quality_repair_plan_to_json(forward) == (
        quality_repair_plan_to_json(reverse)
    )
    assert [item["finding_code"] for item in forward["items"]] == [
        "ZIP_UNREADABLE",
        "ZIP_CORRUPT",
        "ZIP_CRC_ERROR",
        "HISTDATA_ZIP_FILENAME_INVALID",
        "ZIP_MEMBER_MISSING",
        "ZIP_MEMBER_UNEXPECTED",
        "HISTDATA_ZIP_MEMBER_FILENAME_INVALID",
        "ZIP_EXTRA_MEMBER",
    ]

    first_duplicate = _finding(
        "HISTDATA_ZIP_FILENAME_INVALID",
        metadata={
            "observed_filename": "z.zip",
            "expected_filename": "expected.zip",
        },
    )
    second_duplicate = _finding(
        "HISTDATA_ZIP_FILENAME_INVALID",
        metadata={
            "observed_filename": "a.zip",
            "expected_filename": "expected.zip",
        },
    )
    duplicate_forward = quality_repair_plan(
        _report(first_duplicate, second_duplicate)
    )
    duplicate_reverse = quality_repair_plan(
        _report(second_duplicate, first_duplicate)
    )
    assert duplicate_forward == duplicate_reverse


def test_repair_plan_generation_does_not_mutate_target_or_report(
    tmp_path: Path,
) -> None:
    """Pure planning must leave the report object and target bytes unchanged."""
    archive = tmp_path / "DAT_ASCII_EURUSD_T_201202.zip"
    archive.write_bytes(b"not a zip")
    finding = _finding("ZIP_CORRUPT", path=str(archive))
    report = _report(finding)
    report_before = report.to_dict()
    bytes_before = archive.read_bytes()

    payload = quality_repair_plan(report)

    assert payload["safety"] == {
        "advisory_only": True,
        "automatic_execution": "unsupported",
        "filesystem_mutation_performed": False,
        "network_access_performed": False,
        "requires_user_verification_before_action": True,
    }
    assert report.to_dict() == report_before
    assert archive.read_bytes() == bytes_before


def test_repair_plan_human_output_is_concise() -> None:
    """Human output should show actions without dumping evidence lists."""
    payload = quality_repair_plan(
        _report(_finding("ZIP_CORRUPT")),
        report_path="reports/quality.json",
    )

    rendered = format_quality_repair_plan(payload)

    assert "Quality repair plan" in rendered
    assert "mode: non_mutating" in rendered
    assert "ZIP_CORRUPT" in rendered
    assert "redownload_archive" in rendered
    assert "advisory only" in rendered
    assert "error_type" not in rendered

    omitted = format_quality_repair_plan(
        quality_repair_plan(
            _report(_finding("ZIP_CORRUPT")),
            item_limit=0,
        )
    )
    assert "all 1 plan items omitted by limit" in omitted


def test_repair_plan_schema_golden() -> None:
    """The representative repair-plan JSON should remain golden-testable."""
    report_path = Path(
        "tests/fixtures/data_quality_reports/corrupt_zip_report.json"
    )
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    report = QualityReport.from_dict(report_payload)
    payload = quality_repair_plan(report, report_path=str(report_path))
    golden_path = Path(
        "tests/fixtures/data_quality_reports/corrupt_zip_repair_plan.json"
    )

    assert quality_repair_plan_to_json(payload) == golden_path.read_text(
        encoding="utf-8"
    )


def _finding(
    code: str,
    *,
    rule_id: str = "inventory.zip.integrity",
    path: str = "quality-fixtures/DAT_ASCII_EURUSD_T_201202.zip",
    metadata: dict[str, JSONValue] | None = None,
) -> QualityFinding:
    target = QualityTarget(
        path=path,
        kind=QualityTargetKind.ZIP,
        data_format="ascii",
        timeframe="T",
        symbol="EURUSD",
        period="201202",
        metadata={"filename": Path(path).name},
    )
    severity = (
        QualitySeverity.WARNING
        if code == "ZIP_EXTRA_MEMBER"
        else QualitySeverity.ERROR
    )
    return QualityFinding(
        severity=severity,
        code=code,
        message=f"Finding {code}",
        rule_id=rule_id,
        target=target,
        metadata=dict(
            _FINDING_METADATA.get(code, {}) if metadata is None else metadata
        ),
    )


def _report(*findings: QualityFinding) -> QualityReport:
    target_items: list[QualityTarget] = []
    for finding in findings:
        if finding.target not in target_items:
            target_items.append(finding.target)
    targets = tuple(target_items)
    return QualityReport(
        targets=targets,
        rule_results=tuple(
            QualityRuleResult(
                rule_id=finding.rule_id,
                target=finding.target,
                findings=(finding,),
            )
            for finding in findings
        ),
    )

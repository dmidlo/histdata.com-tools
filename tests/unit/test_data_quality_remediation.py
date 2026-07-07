"""Tests for shared data-quality remediation hint catalog."""

from __future__ import annotations

from histdatacom.data_quality import (
    QualityFinding,
    QualitySeverity,
    QualityTarget,
    remediation_hint_payloads_for_finding,
    remediation_hint_payloads_for_flags,
    remediation_hints_for_finding_code,
    remediation_hints_for_flags,
)

TARGET = QualityTarget(path="DAT_ASCII_EURUSD_T_201202.csv")


def test_remediation_catalog_reproduces_topology_hint_codes_and_messages() -> (
    None
):
    flags = (
        "unavailable_topology",
        "invalid_timestamps",
        "non_monotonic_timestamps",
        "duplicate_timestamps",
        "suspicious_gaps",
        "weekend_activity",
    )

    payloads = remediation_hint_payloads_for_flags(flags)

    legacy_shape = [
        {
            "flag": payload["flag"],
            "code": payload["code"],
            "message": payload["message"],
        }
        for payload in payloads
    ]
    assert legacy_shape == [
        {
            "flag": "unavailable_topology",
            "code": "verify_fingerprint_source",
            "message": "rebuild or choose a readable fingerprint source",
        },
        {
            "flag": "invalid_timestamps",
            "code": "inspect_invalid_timestamp_rows",
            "message": "inspect invalid timestamp rows",
        },
        {
            "flag": "non_monotonic_timestamps",
            "code": "repair_timestamp_order",
            "message": "repair non-monotonic timestamp order",
        },
        {
            "flag": "duplicate_timestamps",
            "code": "inspect_duplicate_timestamp_rows",
            "message": "inspect duplicate timestamp rows",
        },
        {
            "flag": "suspicious_gaps",
            "code": "inspect_gap_boundaries",
            "message": "inspect largest gap boundaries",
        },
        {
            "flag": "weekend_activity",
            "code": "verify_weekend_session_policy",
            "message": "verify weekend-session policy",
        },
    ]
    assert {str(payload["rule_id"]) for payload in payloads} == {
        "fingerprint.series"
    }
    assert [payload["action_kind"] for payload in payloads] == [
        "rebuild",
        "inspect",
        "repair",
        "inspect",
        "inspect",
        "verify",
    ]


def test_remediation_catalog_preserves_input_order_and_ignores_unknowns() -> (
    None
):
    hints = remediation_hints_for_flags(
        (
            "unknown_flag",
            "duplicate_timestamps",
            "suspicious_gaps",
            "duplicate_timestamps",
        )
    )

    assert [hint.flag for hint in hints] == [
        "duplicate_timestamps",
        "suspicious_gaps",
    ]
    assert [hint.code for hint in hints] == [
        "inspect_duplicate_timestamp_rows",
        "inspect_gap_boundaries",
    ]


def test_remediation_catalog_maps_representative_time_finding() -> None:
    finding = QualityFinding(
        severity=QualitySeverity.WARNING,
        code="ASCII_TICK_DUPLICATE_ROW",
        message="Tick file contains exact duplicate rows.",
        rule_id="time.ascii.sequence",
        target=TARGET,
    )

    assert remediation_hint_payloads_for_finding(finding) == [
        {
            "code": "inspect_duplicate_tick_rows",
            "message": "inspect duplicate tick rows",
            "action_kind": "inspect",
            "rule_id": "time.ascii.sequence",
            "finding_code": "ASCII_TICK_DUPLICATE_ROW",
        }
    ]
    assert (
        remediation_hints_for_finding_code("ASCII_TICK_DUPLICATE_ROW")[
            0
        ].rule_id
        == "time.ascii.sequence"
    )


def test_remediation_catalog_maps_inventory_zip_findings() -> None:
    """ZIP inventory findings should expose bounded operator guidance."""
    expected = {
        "HISTDATA_ZIP_FILENAME_INVALID": {
            "code": "rename_histdata_zip_archive",
            "message": "rename ZIP archive to the expected HistData filename",
            "action_kind": "repair",
        },
        "HISTDATA_ZIP_MEMBER_FILENAME_INVALID": {
            "code": "rename_histdata_zip_member",
            "message": (
                "rename ZIP member to the expected HistData member filename"
            ),
            "action_kind": "repair",
        },
        "ZIP_MEMBER_MISSING": {
            "code": "restore_expected_zip_member",
            "message": (
                "restore the expected HistData data member inside the ZIP"
            ),
            "action_kind": "rebuild",
        },
        "ZIP_MEMBER_UNEXPECTED": {
            "code": "rebuild_expected_zip_member",
            "message": (
                "rebuild ZIP contents so the expected HistData data member is present"
            ),
            "action_kind": "rebuild",
        },
        "ZIP_EXTRA_MEMBER": {
            "code": "inspect_unexpected_zip_members",
            "message": "inspect unexpected extra ZIP members",
            "action_kind": "inspect",
        },
        "ZIP_CRC_ERROR": {
            "code": "redownload_zip_crc_failure",
            "message": (
                "redownload or replace the ZIP archive with CRC failures"
            ),
            "action_kind": "rebuild",
        },
        "ZIP_CORRUPT": {
            "code": "redownload_corrupt_zip_archive",
            "message": "redownload or replace the corrupt ZIP archive",
            "action_kind": "rebuild",
        },
        "ZIP_UNREADABLE": {
            "code": "restore_zip_read_access",
            "message": "restore ZIP archive read access or replace the archive",
            "action_kind": "repair",
        },
    }

    for finding_code, base_payload in expected.items():
        finding = QualityFinding(
            severity=QualitySeverity.ERROR,
            code=finding_code,
            message="ZIP inventory finding.",
            rule_id="inventory.zip.integrity",
            target=TARGET,
        )

        assert remediation_hint_payloads_for_finding(finding) == [
            {
                **base_payload,
                "rule_id": "inventory.zip.integrity",
                "finding_code": finding_code,
            }
        ]


def test_remediation_catalog_ignores_unknown_finding_codes() -> None:
    assert (
        remediation_hints_for_finding_code(
            "ASCII_TICK_DUPLICATE_ROW",
            rule_id="other.rule",
        )
        == ()
    )
    assert (
        remediation_hints_for_finding_code(
            "ZIP_CORRUPT",
            rule_id="other.rule",
        )
        == ()
    )
    finding = QualityFinding(
        severity=QualitySeverity.INFO,
        code="UNKNOWN",
        message="Unknown finding.",
        rule_id="time.ascii.sequence",
        target=TARGET,
    )

    assert remediation_hint_payloads_for_finding(finding) == []

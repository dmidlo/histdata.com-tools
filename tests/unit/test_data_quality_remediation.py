"""Tests for shared data-quality remediation hint catalog."""

from __future__ import annotations

import pytest

from histdatacom.data_quality import (
    CALENDAR_POLICY_REMEDIATION_CONTEXT_SCHEMA_VERSION,
    QualityFinding,
    QualitySeverity,
    QualityTarget,
    RemediationActionability,
    classify_remediation_actionability,
    remediation_hint_payloads_for_finding,
    remediation_hint_payloads_for_flags,
    remediation_hints_for_finding_code,
    remediation_hints_for_flags,
)

TARGET = QualityTarget(path="DAT_ASCII_EURUSD_T_201202.csv")


def _calendar_policy(
    *,
    weekend: str = "advisory",
    closures: str = "expected",
) -> dict:
    return {
        "source_timezone": "EST-no-DST",
        "canonical_timezone": "UTC",
        "holiday_calendar_complete": weekend != "advisory",
        "holiday_calendar_static_advisory": weekend == "advisory",
        "weekend_activity_policy": weekend,
        "expected_session_closure_policy": closures,
        "calendar_profile": {
            "name": "policy-profile",
            "source": "operator-config",
            "version": "2026.07",
        },
    }


def test_remediation_actionability_classifies_supported_boundaries() -> None:
    """Every public actionability status should have deterministic evidence."""
    cases = (
        (
            {
                "rule_id": "inventory.zip.integrity",
                "finding_code": "ZIP_CORRUPT",
                "severity": "error",
                "mapped": True,
            },
            RemediationActionability.REMEDIABLE_DEFECT,
            "mapped_remediation_hint",
        ),
        (
            {
                "rule_id": "modeling.readiness",
                "finding_code": "MODELING_CALENDAR_REGIME_POLICY_MISSING",
                "severity": "warning",
                "mapped": False,
            },
            RemediationActionability.POLICY_OR_PROFILE_DECISION,
            "policy_or_profile_context_required",
        ),
        (
            {
                "rule_id": "inventory.format_support",
                "finding_code": "HISTDATA_FORMAT_UNSUPPORTED",
                "severity": "error",
                "mapped": False,
            },
            RemediationActionability.UNSUPPORTED_FORMAT_OR_CAPABILITY,
            "unsupported_format_rule",
        ),
        (
            {
                "rule_id": "provenance.manifest.lineage",
                "finding_code": "PROVENANCE_MANIFEST_UNAVAILABLE",
                "severity": "warning",
                "mapped": False,
            },
            RemediationActionability.EXPECTED_ARTIFACT_OR_CONTEXT,
            "expected_artifact_or_context",
        ),
        (
            {
                "rule_id": "time.unresolved",
                "finding_code": "CUSTOM_SHARED_FAILURE",
                "severity": "error",
                "mapped": False,
                "attribution_status": "unresolved",
            },
            RemediationActionability.NEEDS_RULE_ATTRIBUTION,
            "unresolved_rule_attribution",
        ),
        (
            {
                "rule_id": "custom.diagnostics",
                "finding_code": "DIAGNOSTIC_CONTEXT_MISSING",
                "severity": "warning",
                "mapped": False,
            },
            RemediationActionability.NEEDS_DIAGNOSTIC_CONTEXT,
            "missing_diagnostic_context",
        ),
        (
            {
                "rule_id": "custom.repair",
                "finding_code": "DESTRUCTIVE_REPAIR_REQUIRED",
                "severity": "error",
                "mapped": False,
            },
            RemediationActionability.UNSAFE_TO_AUTOMATE,
            "unsafe_automatic_repair",
        ),
        (
            {
                "rule_id": "fingerprint.series",
                "finding_code": "FINGERPRINT_SERIES_SUMMARY",
                "severity": "info",
                "mapped": False,
            },
            RemediationActionability.INFORMATIONAL_ONLY,
            "informational_severity",
        ),
    )

    for arguments, expected_status, expected_reason in cases:
        decision = classify_remediation_actionability(**arguments)

        assert decision.actionability is expected_status
        assert decision.reason == expected_reason
        assert decision.to_payload() == {
            "actionability": expected_status.value,
            "actionability_reason": expected_reason,
        }


def test_remediation_actionability_defaults_warning_errors_to_actionable() -> (
    None
):
    """Unknown warning/error gaps must not be hidden as boundaries."""
    decision = classify_remediation_actionability(
        rule_id="custom.rule",
        finding_code="CUSTOM_FAILURE",
        severity="error",
        mapped=False,
    )

    assert decision.actionability is RemediationActionability.REMEDIABLE_DEFECT
    assert decision.reason == "unmapped_warning_or_error"


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


@pytest.mark.parametrize(
    ("policy", "message", "action_kind", "actionable"),
    (
        (
            "strict",
            "inspect weekend activity against strict no-weekend policy",
            "inspect",
            True,
        ),
        (
            "advisory",
            "verify weekend-session profile assumptions",
            "verify",
            True,
        ),
        (
            "allowed",
            "weekend activity is allowed by the active profile",
            "context",
            False,
        ),
    ),
)
def test_weekend_remediation_hint_is_calendar_policy_aware(
    policy: str,
    message: str,
    action_kind: str,
    actionable: bool,
) -> None:
    """Weekend guidance should preserve its code while policy changes advice."""
    payload = remediation_hint_payloads_for_flags(
        ("weekend_activity",),
        calendar_policy=_calendar_policy(weekend=policy),
    )[0]
    context = payload["policy_context"]

    assert payload["code"] == "verify_weekend_session_policy"
    assert payload["message"] == message
    assert payload["action_kind"] == action_kind
    assert context == {
        "schema_version": CALENDAR_POLICY_REMEDIATION_CONTEXT_SCHEMA_VERSION,
        "flag": "weekend_activity",
        "actionable": actionable,
        "profile_name": "policy-profile",
        "profile_source": "operator-config",
        "profile_version": "2026.07",
        "source_timezone": "EST-no-DST",
        "canonical_timezone": "UTC",
        "calendar_complete": policy != "advisory",
        "calendar_static_advisory": policy == "advisory",
        "weekend_activity_policy": policy,
        "expected_session_closure_policy": "expected",
    }


def test_expected_closure_hint_requires_explicit_unexpected_policy() -> None:
    """Expected closures remain contextual unless the profile says otherwise."""
    assert (
        remediation_hint_payloads_for_flags(
            ("expected_session_closures",),
            calendar_policy=_calendar_policy(closures="expected"),
        )
        == []
    )

    payload = remediation_hint_payloads_for_flags(
        ("expected_session_closures",),
        calendar_policy=_calendar_policy(closures="unexpected"),
    )[0]

    assert payload["code"] == "inspect_unexpected_session_closure"
    assert payload["action_kind"] == "inspect"
    assert (
        payload["policy_context"]["expected_session_closure_policy"]
        == "unexpected"
    )


def test_calendar_policy_context_bounds_profile_text() -> None:
    """Policy-aware hints should never echo unbounded profile metadata."""
    policy = _calendar_policy(weekend="strict")
    policy["calendar_profile"]["name"] = "x" * 500

    payload = remediation_hint_payloads_for_flags(
        ("weekend_activity",),
        calendar_policy=policy,
    )[0]

    assert payload["policy_context"]["profile_name"] == "x" * 128


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

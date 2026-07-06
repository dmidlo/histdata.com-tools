"""Tests for bounded quality-report payload contract audits."""

from __future__ import annotations

from copy import deepcopy

from histdatacom.data_quality.bounded_payload_contracts import (
    BOUNDED_PAYLOAD_CONTRACT_AUDIT_SCHEMA_VERSION,
    bounded_payload_contract_audit,
    format_bounded_payload_contract_audit,
    representative_bounded_quality_payload,
)
from histdatacom.runtime_contracts import JSONValue


def test_bounded_payload_contract_audit_passes_representative_payload() -> None:
    """Representative generated payload should satisfy the contract."""
    payload = bounded_payload_contract_audit()

    assert payload["schema_version"] == (
        BOUNDED_PAYLOAD_CONTRACT_AUDIT_SCHEMA_VERSION
    )
    assert payload["payload_source"] == "representative"
    assert payload["status"] == "pass"
    assert payload["finding_count"] == 0
    assert payload["findings"] == []
    assert payload["checked_surfaces"]["sequence_contract_count"] == 12
    assert "does not read local market data" in payload["non_goals"]


def test_bounded_payload_contract_audit_accepts_provided_payload() -> None:
    """Callers should be able to audit an already-generated payload."""
    payload = representative_bounded_quality_payload()

    audit = bounded_payload_contract_audit(payload)

    assert audit["payload_source"] == "provided"
    assert audit["status"] == "pass"
    assert audit["finding_count"] == 0


def test_bounded_payload_contract_audit_detects_missing_metadata() -> None:
    """Missing bounded metadata should fail with a stable finding code."""
    payload = _representative_payload_copy()
    payload["payload_limits"].pop("target_summaries")

    audit = bounded_payload_contract_audit(payload)

    assert audit["status"] == "fail"
    finding = _first_finding(audit, "bounded_payload_metadata_missing")
    assert finding["path"] == "payload_limits.target_summaries"


def test_bounded_payload_contract_audit_detects_count_mismatch() -> None:
    """Included counts should match the emitted bounded sequence length."""
    payload = _representative_payload_copy()
    limits = payload["payload_limits"]["target_summaries"]
    assert isinstance(limits, dict)
    limits["included_count"] = 99

    audit = bounded_payload_contract_audit(payload)

    assert audit["status"] == "fail"
    finding = _first_finding(audit, "bounded_payload_count_mismatch")
    assert finding["path"] == "payload_limits.target_summaries.included_count"


def test_bounded_payload_contract_audit_detects_effective_limit_mismatch() -> (
    None
):
    """Effective limits should follow requested/default clamp semantics."""
    payload = _representative_payload_copy()
    limits = payload["payload_limits"]["target_summaries"]
    assert isinstance(limits, dict)
    limits["effective_limit"] = 99

    audit = bounded_payload_contract_audit(payload)

    assert audit["status"] == "fail"
    finding = _first_finding(
        audit,
        "bounded_payload_effective_limit_mismatch",
    )
    assert finding["path"] == "payload_limits.target_summaries.effective_limit"


def test_bounded_payload_contract_audit_detects_truncation_mismatch() -> None:
    """Truncation flags should reflect omitted bounded items."""
    payload = _representative_payload_copy()
    limits = payload["payload_limits"]["target_summaries"]
    assert isinstance(limits, dict)
    limits["truncated"] = False

    audit = bounded_payload_contract_audit(payload)

    assert audit["status"] == "fail"
    finding = _first_finding(audit, "bounded_payload_truncation_mismatch")
    assert finding["path"] == "payload_limits.target_summaries.truncated"


def test_format_bounded_payload_contract_audit_renders_human_summary() -> None:
    """Human output should expose pass/fail audit state."""
    output = format_bounded_payload_contract_audit(
        bounded_payload_contract_audit()
    )

    assert output.startswith("Bounded Payload Contract Audit\n")
    assert "status: pass" in output
    assert "No bounded payload contract drift detected." in output


def _representative_payload_copy() -> dict[str, JSONValue]:
    return deepcopy(representative_bounded_quality_payload())


def _first_finding(
    audit: dict[str, JSONValue],
    code: str,
) -> dict[str, JSONValue]:
    findings = audit["findings"]
    assert isinstance(findings, list)
    for finding in findings:
        assert isinstance(finding, dict)
        if finding.get("code") == code:
            return finding
    raise AssertionError(f"missing finding code {code}")

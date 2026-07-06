"""Tests for fingerprint schema/profile discovery contracts."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from histdatacom.data_quality.fingerprint_discovery import (
    TIME_SERIES_FINGERPRINT_CONTRACT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_REPORT_SURFACE_EVIDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION,
    _audit_report_surface_evidence,
    fingerprint_contract_audit,
    fingerprint_report_surface_evidence,
    fingerprint_schema_discovery,
    format_fingerprint_contract_audit,
    format_fingerprint_schema_discovery,
)
from histdatacom.data_quality.fingerprint_contracts import (
    FINGERPRINT_DISTRIBUTION_ATTENTION_DEFAULTS,
    FINGERPRINT_REPORT_SURFACE_CONTRACTS,
    FINGERPRINT_SCHEMA_CONTRACTS,
    FINGERPRINT_SECTION_LIMIT_DEFAULTS,
    FingerprintReportSurfaceContract,
    IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS,
    PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS,
    PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS,
)
from histdatacom.data_quality.fingerprints import (
    FINGERPRINT_AUDIT_SECTIONS,
    FINGERPRINT_DYNAMICS_SECTIONS,
    SERIES_FINGERPRINT_RULE_ID,
    HistDataFingerprintProfile,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION,
)
from histdatacom.data_quality.profiles import (
    QUALITY_PROFILE_SCHEMA_VERSION,
    load_quality_profile_file,
)
from histdatacom.runtime_contracts import JSONValue


def test_fingerprint_schema_discovery_reports_contract_surface() -> None:
    """Discovery should expose current schemas, sections, and vocabulary."""
    payload = fingerprint_schema_discovery()

    assert (
        payload["schema_version"]
        == TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION
    )
    assert payload["entrypoints"] == {
        "api": "histdatacom.data_quality.fingerprint_schema_discovery",
        "cli_json": "histdatacom quality fingerprint-schema --json",
        "cli_text": "histdatacom quality fingerprint-schema",
    }
    schemas = payload["schemas"]
    assert schemas["series_fingerprint"]["schema_version"] == (
        TIME_SERIES_FINGERPRINT_SCHEMA_VERSION
    )
    assert schemas["fingerprint_audit"]["schema_version"] == (
        TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION
    )
    assert schemas["fingerprint_dependence"]["schema_version"] == (
        TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION
    )
    assert (
        schemas["fingerprint_stationarity_diagnostics"]["schema_version"]
        == TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION
    )
    assert schemas["fingerprint_readiness_summary"]["schema_version"] == (
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
    )
    assert schemas["fingerprint_readiness_risk"]["schema_version"] == (
        TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION
    )
    assert schemas["cross_series_fingerprint"]["status"] == "planned"
    assert payload["metadata_keys"]["finding_metadata"] == {
        "series_fingerprint": TIME_SERIES_FINGERPRINT_METADATA_KEY
    }
    assert payload["metadata_keys"]["report_metadata"]["readiness_summary"] == (
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
    )
    assert payload["metadata_keys"]["report_metadata"]["readiness_risk"] == (
        TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY
    )

    implemented = payload["sections"]["implemented"]["target_sections"]
    assert [section["name"] for section in implemented] == [
        "coverage",
        "temporal_topology",
        "calendar_regimes",
        "m1_bar_distribution",
        "tick_distribution",
        "conditional_distributions",
        "return_dynamics",
        "microstructure_dynamics",
        "dependence",
        "stationarity_diagnostics",
        "fingerprint_audit",
    ]
    planned = payload["sections"]["planned"]["target_sections"]
    assert [section["name"] for section in planned] == [
        "decomposition",
        "synthetic_constraints",
    ]
    assert "observed_sequence" in payload["calculation_bases"]["basis"]
    assert "source_text_order" in payload["calculation_bases"]["row_order"]
    assert "not_emitted" in payload["vocabularies"]["skip_and_reason_codes"]


def test_fingerprint_schema_discovery_uses_contract_registry() -> None:
    """Discovery should be generated from the shared contract registry."""
    payload = fingerprint_schema_discovery()

    assert list(payload["schemas"]) == [
        contract.key for contract in FINGERPRINT_SCHEMA_CONTRACTS
    ]
    assert payload["sections"]["implemented"]["target_sections"] == [
        contract.to_discovery_payload()
        for contract in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    ]
    assert payload["sections"]["planned"]["target_sections"] == [
        contract.to_discovery_payload()
        for contract in PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    ]
    assert payload["sections"]["planned"]["run_sections"] == [
        contract.to_discovery_payload()
        for contract in PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS
    ]
    assert payload["metadata_keys"]["report_metadata"] == {
        contract.key: contract.report_metadata_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    }
    assert payload["metadata_keys"]["bounded_payload"] == {
        contract.key: contract.bounded_payload_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    }
    assert payload["report_surfaces"]["summary_schema_keys"] == {
        contract.key: contract.summary_schema_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    }
    assert payload["report_surfaces"]["full_report_metadata"] == [
        contract.report_metadata_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ]
    assert payload["report_surfaces"]["bounded_payload_keys"] == [
        contract.bounded_payload_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ]
    assert payload["report_surfaces"]["cli_summary_sections"] == [
        contract.cli_summary_section
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ]
    assert payload["report_surfaces"]["cli_summary_headings"] == [
        contract.cli_summary_heading
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ]
    assert payload["report_surfaces"]["surface_matrix"] == [
        contract.to_discovery_payload()
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ]


def test_fingerprint_schema_discovery_registry_matches_runtime() -> None:
    """Registry-backed discovery should stay aligned with runtime owners."""
    payload = fingerprint_schema_discovery()

    assert payload["sections"]["implemented"]["audit_sections"] == list(
        FINGERPRINT_AUDIT_SECTIONS
    )
    assert payload["sections"]["implemented"]["dynamics_sections"] == list(
        FINGERPRINT_DYNAMICS_SECTIONS
    )
    assert payload["profile"]["section_limits"] == dict(
        FINGERPRINT_SECTION_LIMIT_DEFAULTS
    )
    assert payload["profile"]["distribution_attention_defaults"] == dict(
        FINGERPRINT_DISTRIBUTION_ATTENTION_DEFAULTS
    )
    assert payload["profile"]["default_fingerprint_profile"] == (
        HistDataFingerprintProfile().to_metadata()
    )


def test_fingerprint_contract_audit_reports_clean_contract() -> None:
    """Contract audit should expose deterministic pass/fail drift status."""
    payload = fingerprint_contract_audit()

    assert (
        payload["schema_version"]
        == TIME_SERIES_FINGERPRINT_CONTRACT_AUDIT_SCHEMA_VERSION
    )
    assert payload["status"] == "pass"
    assert payload["error_count"] == 0
    assert payload["warning_count"] == 0
    assert payload["findings"] == []
    checked = payload["checked_surfaces"]
    assert checked["schema_contract_count"] == len(FINGERPRINT_SCHEMA_CONTRACTS)
    assert checked["report_surface_count"] == len(
        FINGERPRINT_REPORT_SURFACE_CONTRACTS
    )
    assert [check["status"] for check in payload["checks"]] == [
        "pass",
        "pass",
        "pass",
        "pass",
        "pass",
        "pass",
        "pass",
    ]
    assert checked["report_surface_evidence_count"] == len(
        FINGERPRINT_REPORT_SURFACE_CONTRACTS
    )
    evidence = payload["report_surface_evidence"]
    assert isinstance(evidence, dict)
    assert evidence["schema_version"] == (
        TIME_SERIES_FINGERPRINT_REPORT_SURFACE_EVIDENCE_SCHEMA_VERSION
    )
    assert (
        _surface_row(evidence, "regime_summary")["report_metadata_state"]
        == "present"
    )
    assert "Fingerprint regimes" in evidence["cli_summary_headings"]
    assert "Fingerprint readiness risk" in evidence["cli_summary_headings"]
    assert "does not read local target data" in payload["non_goals"]


def test_fingerprint_contract_audit_reports_synthetic_drift() -> None:
    """Synthetic contract drift should produce deterministic failure details."""
    discovery = fingerprint_schema_discovery()
    del discovery["schemas"]["series_fingerprint"]

    payload = fingerprint_contract_audit(discovery=discovery)

    assert payload["status"] == "fail"
    assert payload["error_count"] == 1
    finding = payload["findings"][0]
    assert finding["severity"] == "error"
    assert finding["code"] == "missing_schema_contract"
    assert finding["path"] == "schemas.series_fingerprint"


def test_fingerprint_report_surface_evidence_reports_runtime_matrix() -> None:
    """Generated evidence should prove full, bounded, and CLI surfaces."""
    payload = fingerprint_report_surface_evidence()

    assert payload["schema_version"] == (
        TIME_SERIES_FINGERPRINT_REPORT_SURFACE_EVIDENCE_SCHEMA_VERSION
    )
    assert payload["source"] == "representative-generated-report"
    assert payload["surface_count"] == len(FINGERPRINT_REPORT_SURFACE_CONTRACTS)
    assert "time_series_fingerprint_regime_summary" in (
        payload["full_report_metadata_keys"]
    )
    assert "fingerprint_regime" in payload["bounded_payload_keys"]
    assert "Fingerprint regimes" in payload["cli_summary_headings"]
    for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS:
        row = _surface_row(payload, contract.key)
        assert row["summary_schema_key"] == contract.summary_schema_key
        assert row["report_metadata_state"] == "present"
        assert row["bounded_payload_state"] == "present"
        assert row["cli_summary_state"] == "present"


def test_fingerprint_contract_audit_detects_missing_runtime_metadata_key() -> (
    None
):
    """Representative evidence should fail when full report metadata is absent."""
    evidence = _surface_evidence_copy()
    _surface_row(evidence, "regime_summary")[
        "report_metadata_state"
    ] = "missing"

    payload = fingerprint_contract_audit(report_surface_evidence=evidence)

    assert payload["status"] == "fail"
    finding = _first_finding(payload, "missing_runtime_report_metadata_key")
    assert finding["path"] == (
        "report_surface_evidence.surface_matrix."
        "regime_summary.report_metadata_state"
    )


def test_fingerprint_contract_audit_detects_missing_runtime_bounded_key() -> (
    None
):
    """Representative evidence should fail when bounded output is absent."""
    evidence = _surface_evidence_copy()
    _surface_row(evidence, "regime_summary")[
        "bounded_payload_state"
    ] = "missing"

    payload = fingerprint_contract_audit(report_surface_evidence=evidence)

    assert payload["status"] == "fail"
    finding = _first_finding(payload, "missing_runtime_bounded_payload_key")
    assert finding["path"] == (
        "report_surface_evidence.surface_matrix."
        "regime_summary.bounded_payload_state"
    )


def test_fingerprint_contract_audit_detects_missing_cli_surface_declaration() -> (
    None
):
    """CLI/report surface declarations should stay explicit."""
    evidence = _surface_evidence_copy()
    _surface_row(evidence, "regime_summary")["cli_summary_section"] = ""

    payload = fingerprint_contract_audit(report_surface_evidence=evidence)

    assert payload["status"] == "fail"
    finding = _first_finding(payload, "missing_cli_summary_surface_declaration")
    assert finding["path"] == (
        "report_surface_evidence.surface_matrix."
        "regime_summary.cli_summary_section"
    )


def test_report_surface_evidence_accepts_intentionally_absent_cli_surface() -> (
    None
):
    """Surface contracts may declare CLI absence instead of silently omitting it."""
    contract = FingerprintReportSurfaceContract(
        key="readiness_api_only",
        summary_schema_key="fingerprint_readiness_summary",
        report_metadata_key="time_series_fingerprint_readiness_summary",
        bounded_payload_key="fingerprint_readiness",
        cli_summary_section="",
        cli_summary_heading="",
        intentional_absence_reason="covered by machine-readable API only",
    )
    evidence = fingerprint_report_surface_evidence(contracts=(contract,))
    findings: list[dict[str, object]] = []

    _audit_report_surface_evidence(evidence, findings, contracts=(contract,))

    assert findings == []
    row = _surface_row(evidence, "readiness_api_only")
    assert row["cli_summary_state"] == "intentionally_absent"
    assert row["intentional_absence_reason"] == (
        "covered by machine-readable API only"
    )


def test_format_fingerprint_contract_audit_renders_human_summary() -> None:
    """Human audit output should summarize pass/fail checks."""
    payload = fingerprint_contract_audit()

    output = format_fingerprint_contract_audit(payload)

    assert output.startswith("Fingerprint Contract Audit\n")
    assert "status: pass" in output
    assert "- schema_contracts: pass" in output
    assert "Report Surface Evidence" in output
    assert "coverage_summary" in output
    assert "present (Fingerprint coverage)" in output
    assert "regime_summary" in output
    assert "present (Fingerprint regimes)" in output
    assert "No contract drift detected." in output


def test_format_fingerprint_contract_audit_renders_intentional_absence() -> (
    None
):
    """Human audit output should explain intentionally absent CLI surfaces."""
    contract = FingerprintReportSurfaceContract(
        key="readiness_api_only",
        summary_schema_key="fingerprint_readiness_summary",
        report_metadata_key="time_series_fingerprint_readiness_summary",
        bounded_payload_key="fingerprint_readiness",
        cli_summary_section="",
        cli_summary_heading="",
        intentional_absence_reason="covered by machine-readable API only",
    )
    evidence = fingerprint_report_surface_evidence(contracts=(contract,))
    payload = {
        "schema_version": TIME_SERIES_FINGERPRINT_CONTRACT_AUDIT_SCHEMA_VERSION,
        "status": "pass",
        "error_count": 0,
        "warning_count": 0,
        "checks": [],
        "findings": [],
        "report_surface_evidence": evidence,
    }

    output = format_fingerprint_contract_audit(payload)

    assert "readiness_api_only" in output
    assert "intentionally_absent" in output
    assert "covered by machine-readable API only" in output


def test_fingerprint_schema_discovery_reflects_profile_overrides() -> None:
    """Effective profile controls should come from the quality profile."""
    payload = fingerprint_schema_discovery(
        {
            "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
            "name": "fingerprint-overrides",
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "quantiles": [0.1, 0.5, 0.9],
                    "lags": [1, 4],
                    "rolling_windows": [12, 24],
                    "histogram_bins": 12,
                    "max_rows": 250,
                    "rounding_digits": 6,
                    "distribution_attention": {
                        "zero_spread_min_rate": 0.25,
                        "negative_spread_min_count": 2,
                    },
                }
            },
        }
    )

    profile = payload["profile"]
    effective = profile["effective_fingerprint_profile"]
    assert profile["configured"] is True
    assert profile["configured_rule_ids"] == [SERIES_FINGERPRINT_RULE_ID]
    assert effective["quantiles"] == [0.1, 0.5, 0.9]
    assert effective["lags"] == [1, 4]
    assert effective["rolling_windows"] == [12, 24]
    assert effective["histogram_bins"] == 12
    assert effective["max_rows"] == 250
    assert effective["rounding_digits"] == 6
    assert effective["distribution_attention"]["zero_spread_min_rate"] == 0.25
    assert effective["distribution_attention"]["negative_spread_min_count"] == 2


def test_fingerprint_schema_discovery_is_deterministic_and_publish_safe(
    tmp_path: Path,
) -> None:
    """Payloads should not contain volatile local absolute paths."""
    profile_path = tmp_path / "quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "path-profile",
                "rules": {SERIES_FINGERPRINT_RULE_ID: {"max_rows": 10}},
            }
        ),
        encoding="utf-8",
    )
    profile = load_quality_profile_file(profile_path)

    first = fingerprint_schema_discovery(profile)
    second = fingerprint_schema_discovery(profile)

    assert first == second
    rendered = json.dumps(first, sort_keys=True)
    assert str(tmp_path) not in rendered
    assert first["profile"]["source_path"] == "quality-profile.json"
    assert first["examples"]["series_fingerprint_fragment"]["source"][
        "path"
    ].startswith("data/")


def test_format_fingerprint_schema_discovery_renders_human_summary() -> None:
    """Human output should summarize schemas and implemented sections."""
    payload = fingerprint_schema_discovery()

    output = format_fingerprint_schema_discovery(payload)

    assert output.startswith("Fingerprint Schema Discovery\n")
    assert (
        "series_fingerprint: histdatacom.time-series-fingerprint.v1" in output
    )
    assert "- return_dynamics: implemented; timeframes=[M1]" in output
    assert "- dependence: implemented; timeframes=[M1, T]" in output
    assert (
        "- stationarity_diagnostics: implemented; timeframes=[M1, T]" in output
    )
    assert "without reading source or running data quality checks" in output


def _surface_evidence_copy() -> dict[str, JSONValue]:
    return deepcopy(fingerprint_report_surface_evidence())


def _surface_row(
    evidence: dict[str, JSONValue] | JSONValue,
    key: str,
) -> dict[str, JSONValue]:
    assert isinstance(evidence, dict)
    matrix = evidence["surface_matrix"]
    assert isinstance(matrix, list)
    for item in matrix:
        assert isinstance(item, dict)
        if item.get("key") == key:
            return item
    raise AssertionError(f"missing surface row {key}")


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

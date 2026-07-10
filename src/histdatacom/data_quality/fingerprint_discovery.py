"""Application-owned discovery for time-series fingerprint contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import histdatacom
from histdatacom.data_quality.fingerprint_contracts import (
    FINGERPRINT_BASIS_DESCRIPTIONS,
    FINGERPRINT_CACHE_SOURCE_DESCRIPTIONS,
    FINGERPRINT_COMPUTED_FROM_DESCRIPTIONS,
    FINGERPRINT_CONDITIONAL_DISTRIBUTION_GROUPS,
    FINGERPRINT_DISTRIBUTION_ATTENTION_CONFIG_KEYS,
    FINGERPRINT_DISTRIBUTION_ATTENTION_DEFAULTS,
    FINGERPRINT_DYNAMICS_STATUSES,
    FINGERPRINT_ELIGIBILITY_STATUSES,
    FINGERPRINT_READINESS_STATUSES,
    FINGERPRINT_REPORT_SURFACE_CONTRACTS,
    FINGERPRINT_ROW_ORDER_DESCRIPTIONS,
    FINGERPRINT_SCHEMA_CONTRACTS,
    FINGERPRINT_SECTION_LIMIT_DEFAULTS,
    FINGERPRINT_SECTION_STATUSES,
    FINGERPRINT_SERIES_CONFIG_KEYS,
    FINGERPRINT_SKIP_REASON_CODES,
    FINGERPRINT_TOPOLOGY_LIMITATIONS,
    FingerprintReportSurfaceContract,
    IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS,
    IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS,
    PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS,
    PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS,
)
from histdatacom.data_quality.fingerprints import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    FINGERPRINT_AUDIT_SECTIONS,
    FINGERPRINT_DYNAMICS_SECTIONS,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    HistDataFingerprintProfile,
)
from histdatacom.data_quality.profiles import (
    QualityProfile,
    quality_profile_from_value,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import JSONValue

TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-schema-discovery.v1"
)
TIME_SERIES_FINGERPRINT_CONTRACT_AUDIT_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-contract-audit.v1"
)
TIME_SERIES_FINGERPRINT_REPORT_SURFACE_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-report-surface-evidence.v1"
)
FINGERPRINT_REPORT_SURFACE_EVIDENCE_TABLE_HEADERS = (
    "surface key",
    "summary schema key",
    "report metadata",
    "bounded payload",
    "CLI/report summary",
    "intentional absence",
)
FINGERPRINT_REPORT_SURFACE_EVIDENCE_DISPLAY_LIMIT = 12


def fingerprint_schema_discovery(
    profile: Mapping[str, Any] | QualityProfile | None = None,
) -> dict[str, JSONValue]:
    """Return deterministic discovery metadata for fingerprint contracts."""
    quality_profile = quality_profile_from_value(profile)
    fingerprint_profile = quality_profile.fingerprint_profile()
    payload: dict[str, JSONValue] = {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION
        ),
        "package": _package_payload(),
        "entrypoints": _entrypoint_payload(),
        "profile": _profile_payload(quality_profile, fingerprint_profile),
        "schemas": _schema_payload(),
        "metadata_keys": _metadata_key_payload(),
        "target_capabilities": _target_capability_payload(),
        "sections": _section_payload(),
        "report_surfaces": _report_surface_payload(),
        "calculation_bases": _calculation_basis_payload(),
        "vocabularies": _vocabulary_payload(),
        "examples": _example_payload(),
        "consumer_guidance": _consumer_guidance_payload(),
    }
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def format_fingerprint_schema_discovery(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return concise human-readable fingerprint schema discovery text."""
    profile = _mapping(payload.get("profile"))
    effective = _mapping(profile.get("effective_fingerprint_profile"))
    schemas = _mapping(payload.get("schemas"))
    sections = _mapping(payload.get("sections"))
    implemented = _mapping(sections.get("implemented"))
    planned = _mapping(sections.get("planned"))

    lines = [
        "Fingerprint Schema Discovery",
        f"schema: {payload.get('schema_version', '')}",
        f"package: histdatacom {_mapping(payload.get('package')).get('version', '')}",
        (
            "profile: "
            f"{profile.get('name', 'unknown')} "
            f"source={profile.get('source', 'unknown')}"
        ),
        (
            "fingerprint profile: "
            f"quantiles={_format_list(effective.get('quantiles'))} "
            f"lags={_format_list(effective.get('lags'))} "
            f"rolling_windows={_format_list(effective.get('rolling_windows'))} "
            f"max_rows={effective.get('max_rows', '')} "
            f"rounding_digits={effective.get('rounding_digits', '')}"
        ),
        "",
        "Schemas",
    ]
    for key in schemas:
        schema = _mapping(schemas.get(key))
        status = schema.get("status", "")
        schema_version = schema.get("schema_version") or "planned"
        lines.append(f"- {key}: {schema_version} ({status})")

    lines.extend(["", "Implemented Sections"])
    for section in _mapping_rows(implemented.get("target_sections")):
        capabilities = _format_list(section.get("target_timeframes"))
        lines.append(
            f"- {section.get('name', '')}: "
            f"{section.get('status', '')}; timeframes={capabilities}"
        )

    lines.extend(["", "Planned Sections"])
    for section in _mapping_rows(planned.get("target_sections")):
        lines.append(
            f"- {section.get('name', '')}: "
            f"{section.get('status', '')} ({section.get('issue', '')})"
        )

    lines.extend(
        [
            "",
            "Use this command to discover schemas, metadata keys, profile knobs, "
            "basis values, and examples without reading source or running data "
            "quality checks. Run `histdatacom --quality --quality-checks "
            "fingerprint` when you need fingerprints for real targets.",
        ]
    )
    return "\n".join(lines)


def fingerprint_contract_audit(
    profile: Mapping[str, Any] | QualityProfile | None = None,
    *,
    discovery: Mapping[str, JSONValue] | None = None,
    report_surface_evidence: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    """Return a deterministic audit of fingerprint contracts."""
    discovery_payload = (
        publish_safe_json_mapping(dict(discovery))
        if discovery is not None
        else fingerprint_schema_discovery(profile)
    )
    surface_evidence = (
        publish_safe_json_mapping(dict(report_surface_evidence))
        if report_surface_evidence is not None
        else fingerprint_report_surface_evidence()
    )
    findings: list[dict[str, JSONValue]] = []
    checks: list[dict[str, JSONValue]] = []

    _record_audit_check(
        checks,
        findings,
        "schema_contracts",
        _audit_schema_contracts(discovery_payload, findings),
    )
    _record_audit_check(
        checks,
        findings,
        "section_contracts",
        _audit_section_contracts(discovery_payload, findings),
    )
    _record_audit_check(
        checks,
        findings,
        "report_surfaces",
        _audit_report_surfaces(discovery_payload, findings),
    )
    _record_audit_check(
        checks,
        findings,
        "report_surface_evidence",
        _audit_report_surface_evidence(surface_evidence, findings),
    )
    _record_audit_check(
        checks,
        findings,
        "profile_defaults",
        _audit_profile_defaults(discovery_payload, findings),
    )
    _record_audit_check(
        checks,
        findings,
        "vocabularies",
        _audit_vocabularies(discovery_payload, findings),
    )
    _record_audit_check(
        checks,
        findings,
        "examples",
        _audit_examples(discovery_payload, findings),
    )

    error_count = sum(
        1 for finding in findings if finding["severity"] == "error"
    )
    warning_count = sum(
        1 for finding in findings if finding["severity"] == "warning"
    )
    payload: dict[str, JSONValue] = {
        "schema_version": TIME_SERIES_FINGERPRINT_CONTRACT_AUDIT_SCHEMA_VERSION,
        "status": "fail" if error_count else "pass",
        "package": _package_payload(),
        "discovery_schema_version": discovery_payload.get("schema_version"),
        "check_count": len(checks),
        "error_count": error_count,
        "warning_count": warning_count,
        "finding_count": len(findings),
        "checked_surfaces": {
            "schema_contract_count": len(FINGERPRINT_SCHEMA_CONTRACTS),
            "implemented_target_section_count": len(
                IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
            ),
            "implemented_run_section_count": len(
                IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS
            ),
            "planned_target_section_count": len(
                PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS
            ),
            "planned_run_section_count": len(
                PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS
            ),
            "report_surface_count": len(FINGERPRINT_REPORT_SURFACE_CONTRACTS),
            "vocabulary_group_count": len(_vocabulary_payload()),
            "calculation_basis_group_count": len(_calculation_basis_payload()),
            "report_surface_evidence_count": len(
                _mapping_rows(surface_evidence.get("surface_matrix"))
            ),
        },
        "checks": cast(JSONValue, checks),
        "findings": cast(JSONValue, findings),
        "report_surface_evidence": surface_evidence,
        "non_goals": _json_strings(
            (
                "does not read local target data",
                "does not run quality rules",
                "does not automate GitHub, CI, merge, or release workflow",
            )
        ),
    }
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def fingerprint_report_surface_evidence(
    *,
    report_payload: Mapping[str, JSONValue] | None = None,
    bounded_payload: Mapping[str, JSONValue] | None = None,
    cli_summary: str | None = None,
    contracts: tuple[FingerprintReportSurfaceContract, ...] = (
        FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ),
) -> dict[str, JSONValue]:
    """Return generated evidence for public fingerprint report surfaces."""
    if report_payload is None or cli_summary is None:
        from histdatacom.data_quality.bounded_payload_contracts import (
            representative_quality_report,
        )
        from histdatacom.data_quality.reporting import (
            format_quality_console_summary,
            quality_report_payload,
        )

        report = representative_quality_report()
        if report_payload is None:
            report_payload = quality_report_payload(report)
        if cli_summary is None:
            cli_summary = format_quality_console_summary(
                report,
                check_groups=("fingerprint", "time", "domain"),
            )
    if bounded_payload is None:
        from histdatacom.data_quality.bounded_payload_contracts import (
            representative_bounded_quality_payload,
        )

        bounded_payload = representative_bounded_quality_payload()

    metadata = _mapping(report_payload.get("metadata"))
    surface_matrix: list[dict[str, JSONValue]] = []
    for contract in contracts:
        report_metadata_present = contract.report_metadata_key in metadata
        bounded_payload_present = (
            contract.bounded_payload_key in bounded_payload
        )
        cli_heading = contract.cli_summary_heading
        cli_present = bool(cli_heading and cli_heading in cli_summary)
        if contract.intentional_absence_reason and not cli_heading:
            cli_state = "intentionally_absent"
        else:
            cli_state = "present" if cli_present else "missing"
        surface_matrix.append(
            {
                "key": contract.key,
                "summary_schema_key": contract.summary_schema_key,
                "report_metadata_key": contract.report_metadata_key,
                "report_metadata_state": (
                    "present" if report_metadata_present else "missing"
                ),
                "bounded_payload_key": contract.bounded_payload_key,
                "bounded_payload_state": (
                    "present" if bounded_payload_present else "missing"
                ),
                "cli_summary_section": contract.cli_summary_section,
                "cli_summary_heading": cli_heading,
                "cli_summary_state": cli_state,
                "intentional_absence_reason": (
                    contract.intentional_absence_reason
                ),
            }
        )

    payload: dict[str, JSONValue] = {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_REPORT_SURFACE_EVIDENCE_SCHEMA_VERSION
        ),
        "source": "representative-generated-report",
        "surface_count": len(surface_matrix),
        "full_report_metadata_keys": _json_strings(
            tuple(
                contract.report_metadata_key
                for contract in contracts
                if contract.report_metadata_key in metadata
            )
        ),
        "bounded_payload_keys": _json_strings(
            tuple(
                contract.bounded_payload_key
                for contract in contracts
                if contract.bounded_payload_key in bounded_payload
            )
        ),
        "cli_summary_headings": _json_strings(
            tuple(
                contract.cli_summary_heading
                for contract in contracts
                if contract.cli_summary_heading
                and contract.cli_summary_heading in cli_summary
            )
        ),
        "surface_matrix": cast(JSONValue, surface_matrix),
    }
    safe_payload: dict[str, JSONValue] = publish_safe_json_mapping(payload)
    return safe_payload


def format_fingerprint_contract_audit(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return concise human-readable fingerprint contract audit text."""
    lines = [
        "Fingerprint Contract Audit",
        f"schema: {payload.get('schema_version', '')}",
        f"status: {payload.get('status', 'unknown')}",
        (
            "findings: "
            f"errors={_int_value(payload.get('error_count'))} "
            f"warnings={_int_value(payload.get('warning_count'))}"
        ),
        "",
        "Checks",
    ]
    for check in _mapping_rows(payload.get("checks")):
        lines.append(
            f"- {check.get('name', '')}: {check.get('status', '')} "
            f"({check.get('checked_count', 0)} checked)"
        )
    evidence_rows = fingerprint_report_surface_evidence_table_rows(
        _mapping(payload.get("report_surface_evidence"))
    )
    if evidence_rows:
        lines.extend(
            [
                "",
                "Report Surface Evidence",
                *_plain_text_table(
                    FINGERPRINT_REPORT_SURFACE_EVIDENCE_TABLE_HEADERS,
                    evidence_rows,
                ),
            ]
        )
    findings = _mapping_rows(payload.get("findings"))
    if findings:
        lines.extend(["", "Findings"])
        for finding in findings:
            lines.append(
                "- "
                f"{str(finding.get('severity', '')).upper()} "
                f"{finding.get('code', '')} at {finding.get('path', '')}: "
                f"{finding.get('message', '')}"
            )
    else:
        lines.extend(["", "No contract drift detected."])
    return "\n".join(lines)


def fingerprint_report_surface_evidence_table_rows(
    evidence: Mapping[str, JSONValue],
    *,
    limit: int = FINGERPRINT_REPORT_SURFACE_EVIDENCE_DISPLAY_LIMIT,
) -> tuple[tuple[str, str, str, str, str, str], ...]:
    """Return bounded display rows for representative report-surface evidence."""
    rows: list[tuple[str, str, str, str, str, str]] = []
    surface_rows = _mapping_rows(evidence.get("surface_matrix"))
    display_limit = max(0, limit)
    for row in surface_rows[:display_limit]:
        rows.append(_report_surface_evidence_table_row(row))
    omitted = len(surface_rows) - len(rows)
    if omitted > 0:
        rows.append(
            (
                f"{omitted} additional surfaces omitted",
                "",
                "",
                "",
                "",
                "",
            )
        )
    return tuple(rows)


def _report_surface_evidence_table_row(
    row: Mapping[str, JSONValue],
) -> tuple[str, str, str, str, str, str]:
    heading = str(row.get("cli_summary_heading") or "")
    cli_state = str(row.get("cli_summary_state") or "")
    cli_display = f"{cli_state} ({heading})" if heading else cli_state
    return (
        str(row.get("key") or ""),
        str(row.get("summary_schema_key") or ""),
        str(row.get("report_metadata_state") or ""),
        str(row.get("bounded_payload_state") or ""),
        cli_display,
        str(row.get("intentional_absence_reason") or ""),
    )


def _plain_text_table(
    headers: tuple[str, ...],
    rows: tuple[tuple[str, ...], ...],
) -> list[str]:
    widths = [
        max(len(header), *(len(row[index]) for row in rows))
        for index, header in enumerate(headers)
    ]
    lines = [
        "  "
        + " | ".join(
            header.ljust(widths[index]) for index, header in enumerate(headers)
        ),
        "  " + "-+-".join("-" * width for width in widths),
    ]
    for row in rows:
        lines.append(
            "  "
            + " | ".join(
                row[index].ljust(widths[index]) for index in range(len(headers))
            )
        )
    return lines


def _record_audit_check(
    checks: list[dict[str, JSONValue]],
    findings: list[dict[str, JSONValue]],
    name: str,
    result: tuple[int, int, int],
) -> None:
    checked_count, before_error_count, before_warning_count = result
    error_count = sum(
        1 for finding in findings if finding["severity"] == "error"
    )
    warning_count = sum(
        1 for finding in findings if finding["severity"] == "warning"
    )
    new_errors = error_count - before_error_count
    new_warnings = warning_count - before_warning_count
    status = "fail" if new_errors else "warning" if new_warnings else "pass"
    checks.append(
        {
            "name": name,
            "status": status,
            "checked_count": checked_count,
            "error_count": new_errors,
            "warning_count": new_warnings,
        }
    )


def _audit_schema_contracts(
    discovery: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    schemas = _mapping(discovery.get("schemas"))
    expected = {
        contract.key: contract.to_discovery_payload()
        for contract in FINGERPRINT_SCHEMA_CONTRACTS
    }
    _compare_mapping(
        findings,
        path="schemas",
        expected=expected,
        actual=schemas,
        missing_code="missing_schema_contract",
        mismatch_code="schema_contract_mismatch",
        unexpected_code="orphan_schema_contract",
    )
    schema_keys = set(expected)
    for contract in FINGERPRINT_SCHEMA_CONTRACTS:
        if contract.status == "implemented" and not contract.schema_version:
            _add_audit_finding(
                findings,
                severity="error",
                code="implemented_schema_missing_version",
                path=f"schemas.{contract.key}.schema_version",
                message="implemented fingerprint schema contracts require a schema version",
                expected="non-empty schema version",
                actual=contract.schema_version,
            )
        if contract.status == "planned" and not contract.issue:
            _add_audit_finding(
                findings,
                severity="error",
                code="planned_schema_missing_issue",
                path=f"schemas.{contract.key}.issue",
                message="planned fingerprint schema contracts require an issue link",
                expected="issue reference",
                actual=contract.issue,
            )
        if contract.status not in {"implemented", "planned"}:
            _add_audit_finding(
                findings,
                severity="error",
                code="unknown_schema_status",
                path=f"schemas.{contract.key}.status",
                message="schema contract status must be implemented or planned",
                expected=["implemented", "planned"],
                actual=contract.status,
            )
    for section in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS:
        if section.schema_key not in schema_keys:
            _add_audit_finding(
                findings,
                severity="error",
                code="target_section_schema_missing",
                path=f"sections.implemented.target_sections.{section.name}.schema_key",
                message="implemented target section references an unknown schema",
                expected=sorted(schema_keys),
                actual=section.schema_key,
            )
    return (len(FINGERPRINT_SCHEMA_CONTRACTS), *before)


def _audit_section_contracts(
    discovery: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    sections = _mapping(discovery.get("sections"))
    implemented = _mapping(sections.get("implemented"))
    planned = _mapping(sections.get("planned"))
    expected_implemented = [
        contract.to_discovery_payload()
        for contract in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    ]
    expected_implemented_runs = [
        contract.to_discovery_payload()
        for contract in IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS
    ]
    expected_planned_targets = [
        contract.to_discovery_payload()
        for contract in PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    ]
    expected_planned_runs = [
        contract.to_discovery_payload()
        for contract in PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS
    ]
    _compare_value(
        findings,
        path="sections.implemented.target_sections",
        expected=expected_implemented,
        actual=implemented.get("target_sections"),
        code="implemented_target_sections_mismatch",
        message="implemented target section discovery must be generated from the registry",
    )
    _compare_value(
        findings,
        path="sections.implemented.run_sections",
        expected=expected_implemented_runs,
        actual=implemented.get("run_sections"),
        code="implemented_run_sections_mismatch",
        message="implemented run section discovery must be generated from the registry",
    )
    _compare_value(
        findings,
        path="sections.planned.target_sections",
        expected=expected_planned_targets,
        actual=planned.get("target_sections"),
        code="planned_target_sections_mismatch",
        message="planned target section discovery must stay separated from implemented sections",
    )
    _compare_value(
        findings,
        path="sections.planned.run_sections",
        expected=expected_planned_runs,
        actual=planned.get("run_sections"),
        code="planned_run_sections_mismatch",
        message="planned run section discovery must be generated from the registry",
    )
    _compare_value(
        findings,
        path="sections.implemented.audit_sections",
        expected=_json_strings(FINGERPRINT_AUDIT_SECTIONS),
        actual=implemented.get("audit_sections"),
        code="audit_sections_mismatch",
        message="implemented audit section names must match runtime fingerprint audit sections",
    )
    _compare_value(
        findings,
        path="sections.implemented.dynamics_sections",
        expected=_json_strings(FINGERPRINT_DYNAMICS_SECTIONS),
        actual=implemented.get("dynamics_sections"),
        code="dynamics_sections_mismatch",
        message="implemented dynamics section names must match runtime fingerprint dynamics sections",
    )
    implemented_names = {
        contract.name
        for contract in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    } | {
        contract.name
        for contract in IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS
    }
    planned_names = {
        contract.name
        for contract in PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS
    } | {
        contract.name for contract in PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS
    }
    overlap = sorted(implemented_names & planned_names)
    if overlap:
        _add_audit_finding(
            findings,
            severity="error",
            code="implemented_planned_section_overlap",
            path="sections",
            message="implemented and planned fingerprint sections must stay separated",
            expected=[],
            actual=overlap,
        )
    return (
        len(expected_implemented)
        + len(expected_implemented_runs)
        + len(expected_planned_targets)
        + len(expected_planned_runs)
        + 2,
        *before,
    )


def _audit_report_surfaces(
    discovery: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    schemas = _mapping(discovery.get("schemas"))
    metadata_keys = _mapping(discovery.get("metadata_keys"))
    report_surfaces = _mapping(discovery.get("report_surfaces"))
    expected_summary_schema = {
        contract.key: contract.summary_schema_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    }
    expected_report_metadata = {
        contract.key: contract.report_metadata_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    }
    expected_bounded_payload = {
        contract.key: contract.bounded_payload_key
        for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
    }
    _compare_mapping(
        findings,
        path="report_surfaces.summary_schema_keys",
        expected=expected_summary_schema,
        actual=_mapping(report_surfaces.get("summary_schema_keys")),
        missing_code="missing_report_surface_schema_key",
        mismatch_code="report_surface_schema_key_mismatch",
        unexpected_code="orphan_report_surface_schema_key",
    )
    _compare_mapping(
        findings,
        path="metadata_keys.report_metadata",
        expected=expected_report_metadata,
        actual=_mapping(metadata_keys.get("report_metadata")),
        missing_code="missing_report_metadata_key",
        mismatch_code="report_metadata_key_mismatch",
        unexpected_code="orphan_report_metadata_key",
    )
    _compare_mapping(
        findings,
        path="metadata_keys.bounded_payload",
        expected=expected_bounded_payload,
        actual=_mapping(metadata_keys.get("bounded_payload")),
        missing_code="missing_bounded_payload_key",
        mismatch_code="bounded_payload_key_mismatch",
        unexpected_code="orphan_bounded_payload_key",
    )
    _compare_value(
        findings,
        path="report_surfaces.full_report_metadata",
        expected=[
            contract.report_metadata_key
            for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        ],
        actual=report_surfaces.get("full_report_metadata"),
        code="report_surface_metadata_list_mismatch",
        message="report metadata surface list must match registry order",
    )
    _compare_value(
        findings,
        path="report_surfaces.bounded_payload_keys",
        expected=[
            contract.bounded_payload_key
            for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        ],
        actual=report_surfaces.get("bounded_payload_keys"),
        code="report_surface_bounded_payload_list_mismatch",
        message="bounded payload key list must match registry order",
    )
    _compare_value(
        findings,
        path="report_surfaces.cli_summary_sections",
        expected=[
            contract.cli_summary_section
            for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        ],
        actual=report_surfaces.get("cli_summary_sections"),
        code="report_surface_cli_summary_list_mismatch",
        message="CLI summary section list must match registry order",
    )
    _compare_value(
        findings,
        path="report_surfaces.cli_summary_headings",
        expected=[
            contract.cli_summary_heading
            for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        ],
        actual=report_surfaces.get("cli_summary_headings"),
        code="report_surface_cli_heading_list_mismatch",
        message="CLI summary heading list must match registry order",
    )
    _compare_value(
        findings,
        path="report_surfaces.surface_matrix",
        expected=[
            contract.to_discovery_payload()
            for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        ],
        actual=report_surfaces.get("surface_matrix"),
        code="report_surface_matrix_mismatch",
        message="report surface matrix must match registry declarations",
    )
    for contract in FINGERPRINT_REPORT_SURFACE_CONTRACTS:
        schema = _mapping(schemas.get(contract.summary_schema_key))
        if not schema:
            _add_audit_finding(
                findings,
                severity="error",
                code="missing_report_surface_schema_contract",
                path=f"schemas.{contract.summary_schema_key}",
                message="report surface must reference a declared schema contract",
                expected=contract.summary_schema_key,
                actual=None,
            )
        elif schema.get("status") != "implemented":
            _add_audit_finding(
                findings,
                severity="error",
                code="report_surface_schema_not_implemented",
                path=f"schemas.{contract.summary_schema_key}.status",
                message="report surface schema contract must be implemented",
                expected="implemented",
                actual=schema.get("status"),
            )
    return (len(FINGERPRINT_REPORT_SURFACE_CONTRACTS) * 9, *before)


def _audit_report_surface_evidence(
    evidence: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
    *,
    contracts: tuple[FingerprintReportSurfaceContract, ...] = (
        FINGERPRINT_REPORT_SURFACE_CONTRACTS
    ),
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    _compare_value(
        findings,
        path="report_surface_evidence.schema_version",
        expected=TIME_SERIES_FINGERPRINT_REPORT_SURFACE_EVIDENCE_SCHEMA_VERSION,
        actual=evidence.get("schema_version"),
        code="report_surface_evidence_schema_mismatch",
        message="report surface evidence must use the current schema version",
    )
    _compare_value(
        findings,
        path="report_surface_evidence.surface_count",
        expected=len(contracts),
        actual=evidence.get("surface_count"),
        code="report_surface_evidence_count_mismatch",
        message="report surface evidence count must match contract registry",
    )
    rows = {
        str(row.get("key") or ""): row
        for row in _mapping_rows(evidence.get("surface_matrix"))
    }
    for contract in contracts:
        row = _mapping(rows.get(contract.key))
        path = f"report_surface_evidence.surface_matrix.{contract.key}"
        if not row:
            _add_audit_finding(
                findings,
                severity="error",
                code="missing_report_surface_evidence_row",
                path=path,
                message="representative report surface evidence row is missing",
                expected=contract.to_discovery_payload(),
                actual=None,
            )
            continue
        _audit_report_surface_evidence_row(
            row,
            contract=contract,
            path=path,
            findings=findings,
        )
    return (2 + len(contracts) * 7, *before)


def _audit_report_surface_evidence_row(
    row: Mapping[str, JSONValue],
    *,
    contract: FingerprintReportSurfaceContract,
    path: str,
    findings: list[dict[str, JSONValue]],
) -> None:
    for field, expected in (
        ("summary_schema_key", contract.summary_schema_key),
        ("report_metadata_key", contract.report_metadata_key),
        ("bounded_payload_key", contract.bounded_payload_key),
        ("cli_summary_section", contract.cli_summary_section),
        ("cli_summary_heading", contract.cli_summary_heading),
    ):
        actual = row.get(field)
        if actual != expected:
            code = "report_surface_evidence_contract_mismatch"
            if field == "cli_summary_section":
                code = "missing_cli_summary_surface_declaration"
            _add_audit_finding(
                findings,
                severity="error",
                code=code,
                path=f"{path}.{field}",
                message="representative report surface evidence drifted from the contract registry",
                expected=expected,
                actual=actual,
            )
    _audit_surface_state(
        row,
        path=path,
        field="report_metadata_state",
        expected="present",
        code="missing_runtime_report_metadata_key",
        message="representative full report metadata is missing a fingerprint surface",
        findings=findings,
    )
    _audit_surface_state(
        row,
        path=path,
        field="bounded_payload_state",
        expected="present",
        code="missing_runtime_bounded_payload_key",
        message="representative bounded payload is missing a fingerprint surface",
        findings=findings,
    )
    expected_cli_state = (
        "intentionally_absent"
        if contract.intentional_absence_reason
        else "present"
    )
    _audit_surface_state(
        row,
        path=path,
        field="cli_summary_state",
        expected=expected_cli_state,
        code="missing_runtime_cli_summary_surface",
        message="representative CLI summary is missing a fingerprint surface",
        findings=findings,
    )
    if expected_cli_state == "intentionally_absent":
        _compare_value(
            findings,
            path=f"{path}.intentional_absence_reason",
            expected=contract.intentional_absence_reason,
            actual=row.get("intentional_absence_reason"),
            code="missing_cli_summary_intentional_absence_reason",
            message="intentionally absent CLI surfaces must declare a reason",
        )


def _audit_surface_state(
    row: Mapping[str, JSONValue],
    *,
    path: str,
    field: str,
    expected: str,
    code: str,
    message: str,
    findings: list[dict[str, JSONValue]],
) -> None:
    actual = row.get(field)
    if actual != expected:
        _add_audit_finding(
            findings,
            severity="error",
            code=code,
            path=f"{path}.{field}",
            message=message,
            expected=expected,
            actual=actual,
        )


def _audit_profile_defaults(
    discovery: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    profile = _mapping(discovery.get("profile"))
    _compare_value(
        findings,
        path="profile.default_fingerprint_profile",
        expected=HistDataFingerprintProfile().to_metadata(),
        actual=profile.get("default_fingerprint_profile"),
        code="default_fingerprint_profile_mismatch",
        message="discovery profile defaults must match runtime fingerprint defaults",
    )
    _compare_value(
        findings,
        path="profile.section_limits",
        expected=dict(FINGERPRINT_SECTION_LIMIT_DEFAULTS),
        actual=profile.get("section_limits"),
        code="fingerprint_section_limits_mismatch",
        message="discovery section limits must match registry defaults",
    )
    _compare_value(
        findings,
        path="profile.distribution_attention_defaults",
        expected=dict(FINGERPRINT_DISTRIBUTION_ATTENTION_DEFAULTS),
        actual=profile.get("distribution_attention_defaults"),
        code="fingerprint_distribution_attention_defaults_mismatch",
        message="distribution attention defaults must match registry defaults",
    )
    return (3, *before)


def _audit_vocabularies(
    discovery: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    _compare_value(
        findings,
        path="calculation_bases",
        expected=_calculation_basis_payload(),
        actual=discovery.get("calculation_bases"),
        code="calculation_basis_mismatch",
        message="calculation basis descriptions must match registry vocabulary",
    )
    _compare_value(
        findings,
        path="vocabularies",
        expected=_vocabulary_payload(),
        actual=discovery.get("vocabularies"),
        code="fingerprint_vocabulary_mismatch",
        message="status, reason, topology, and conditional-distribution vocabularies must match registry values",
    )
    return (
        len(_calculation_basis_payload()) + len(_vocabulary_payload()),
        *before,
    )


def _audit_examples(
    discovery: Mapping[str, JSONValue],
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int, int]:
    before = _finding_counts(findings)
    examples = _mapping(discovery.get("examples"))
    series = _mapping(examples.get("series_fingerprint_fragment"))
    source = _mapping(series.get("source"))
    audit = _mapping(series.get("fingerprint_audit"))
    expected_sections = list(_target_section_names_for_timeframe(TICK))
    emitted_sections = _string_values(audit.get("sections_emitted"))
    expected_skipped = {
        section: {"reason": "not_emitted"}
        for section in expected_sections
        if section not in emitted_sections
    }
    _compare_value(
        findings,
        path="examples.series_fingerprint_fragment.schema_version",
        expected=TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
        actual=series.get("schema_version"),
        code="series_example_schema_mismatch",
        message="series fingerprint example must use the runtime series schema version",
    )
    _compare_value(
        findings,
        path="examples.series_fingerprint_fragment.fingerprint_audit.schema_version",
        expected=TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
        actual=audit.get("schema_version"),
        code="audit_example_schema_mismatch",
        message="fingerprint audit example must use the runtime audit schema version",
    )
    _compare_value(
        findings,
        path="examples.series_fingerprint_fragment.fingerprint_audit.sections_expected",
        expected=expected_sections,
        actual=audit.get("sections_expected"),
        code="example_expected_sections_mismatch",
        message="example expected sections must follow implemented TICK contracts",
    )
    _compare_value(
        findings,
        path="examples.series_fingerprint_fragment.fingerprint_audit.sections_skipped",
        expected=expected_skipped,
        actual=audit.get("sections_skipped"),
        code="example_skipped_sections_mismatch",
        message="example skipped sections must reflect expected minus emitted sections",
    )
    _compare_value(
        findings,
        path="examples.readiness_summary_fragment.schema_version",
        expected=TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
        actual=_mapping(examples.get("readiness_summary_fragment")).get(
            "schema_version"
        ),
        code="readiness_example_schema_mismatch",
        message="readiness summary example must use the runtime readiness schema version",
    )
    source_path = str(source.get("path") or "")
    if source_path.startswith("/"):
        _add_audit_finding(
            findings,
            severity="error",
            code="example_source_path_not_publish_safe",
            path="examples.series_fingerprint_fragment.source.path",
            message="example source paths must be publish-safe relative paths",
            expected="relative path",
            actual=source_path,
        )
    return (6, *before)


def _finding_counts(
    findings: list[dict[str, JSONValue]],
) -> tuple[int, int]:
    return (
        sum(1 for finding in findings if finding["severity"] == "error"),
        sum(1 for finding in findings if finding["severity"] == "warning"),
    )


def _compare_mapping(
    findings: list[dict[str, JSONValue]],
    *,
    path: str,
    expected: Mapping[str, JSONValue],
    actual: Mapping[str, JSONValue],
    missing_code: str,
    mismatch_code: str,
    unexpected_code: str,
) -> None:
    for key in expected:
        if key not in actual:
            _add_audit_finding(
                findings,
                severity="error",
                code=missing_code,
                path=f"{path}.{key}",
                message="expected contract key is missing",
                expected=expected[key],
                actual=None,
            )
        elif actual[key] != expected[key]:
            _add_audit_finding(
                findings,
                severity="error",
                code=mismatch_code,
                path=f"{path}.{key}",
                message="contract value drifted from the registry",
                expected=expected[key],
                actual=actual[key],
            )
    for key in actual:
        if key not in expected:
            _add_audit_finding(
                findings,
                severity="warning",
                code=unexpected_code,
                path=f"{path}.{key}",
                message="discovery exposes a key not owned by the registry",
                expected=None,
                actual=actual[key],
            )


def _compare_value(
    findings: list[dict[str, JSONValue]],
    *,
    path: str,
    expected: object,
    actual: object,
    code: str,
    message: str,
) -> None:
    if actual != expected:
        _add_audit_finding(
            findings,
            severity="error",
            code=code,
            path=path,
            message=message,
            expected=expected,
            actual=actual,
        )


def _add_audit_finding(
    findings: list[dict[str, JSONValue]],
    *,
    severity: str,
    code: str,
    path: str,
    message: str,
    expected: object,
    actual: object,
) -> None:
    findings.append(
        {
            "severity": severity,
            "code": code,
            "path": path,
            "message": message,
            "expected": _json_value(expected),
            "actual": _json_value(actual),
        }
    )


def _json_value(value: object) -> JSONValue:
    return cast(JSONValue, value)


def _package_payload() -> dict[str, JSONValue]:
    return {"name": "histdatacom", "version": histdatacom.__version__}


def _entrypoint_payload() -> dict[str, JSONValue]:
    return {
        "cli_json": "histdatacom quality fingerprint-schema --json",
        "cli_text": "histdatacom quality fingerprint-schema",
        "api": "histdatacom.data_quality.fingerprint_schema_discovery",
    }


def _profile_payload(
    quality_profile: QualityProfile,
    fingerprint_profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    configured = quality_profile.rule_config(SERIES_FINGERPRINT_RULE_ID)
    metadata = quality_profile.to_metadata()
    metadata["source_path"] = publish_safe_path(
        str(metadata.get("source_path") or "")
    )
    return {
        "schema_version": str(metadata.get("schema_version") or ""),
        "name": str(metadata.get("name") or ""),
        "source": str(metadata.get("source") or ""),
        "source_path": str(metadata.get("source_path") or ""),
        "is_default": bool(metadata.get("is_default")),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "configured": bool(configured),
        "configured_rule_ids": _json_string_list(
            metadata.get("configured_rule_ids")
        ),
        "configurable_keys": _json_strings(FINGERPRINT_SERIES_CONFIG_KEYS),
        "distribution_attention_configurable_keys": _json_strings(
            FINGERPRINT_DISTRIBUTION_ATTENTION_CONFIG_KEYS
        ),
        "effective_fingerprint_profile": fingerprint_profile.to_metadata(),
        "default_fingerprint_profile": HistDataFingerprintProfile().to_metadata(),
        "cache_policy": {
            "preference": "cache_first",
            "direct_cache": True,
            "fresh_sibling_cache": True,
            "source_text_fallback": True,
            "profile_configurable": False,
        },
        "section_limits": dict(FINGERPRINT_SECTION_LIMIT_DEFAULTS),
        "distribution_attention_defaults": dict(
            FINGERPRINT_DISTRIBUTION_ATTENTION_DEFAULTS
        ),
    }


def _schema_payload() -> dict[str, JSONValue]:
    return {
        contract.key: contract.to_discovery_payload()
        for contract in FINGERPRINT_SCHEMA_CONTRACTS
    }


def _metadata_key_payload() -> dict[str, JSONValue]:
    return {
        "finding_metadata": {
            "series_fingerprint": TIME_SERIES_FINGERPRINT_METADATA_KEY,
            "cross_series_fingerprint": CROSS_SERIES_FINGERPRINT_METADATA_KEY,
        },
        "report_metadata": {
            surface.key: surface.report_metadata_key
            for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        },
        "bounded_payload": {
            surface.key: surface.bounded_payload_key
            for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        },
    }


def _target_capability_payload() -> dict[str, JSONValue]:
    run_contract = IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS[0]
    return {
        "supported_target_kinds": _json_strings(("csv", "zip", "cache")),
        "supported_data_format": "ascii",
        "supported_timeframes": _json_strings((TICK,)),
        "series_rule_id": SERIES_FINGERPRINT_RULE_ID,
        "run_rule_status": {
            "rule_id": run_contract.rule_id,
            "status": run_contract.status,
            "issue": run_contract.issue,
        },
    }


def _section_payload() -> dict[str, JSONValue]:
    return {
        "implemented": {
            "audit_sections": _json_strings(FINGERPRINT_AUDIT_SECTIONS),
            "dynamics_sections": _json_strings(FINGERPRINT_DYNAMICS_SECTIONS),
            "target_sections": [
                contract.to_discovery_payload()
                for contract in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
            ],
            "run_sections": [
                contract.to_discovery_payload()
                for contract in IMPLEMENTED_FINGERPRINT_RUN_SECTION_CONTRACTS
            ],
        },
        "planned": {
            "target_sections": [
                contract.to_discovery_payload()
                for contract in PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS
            ],
            "run_sections": [
                contract.to_discovery_payload()
                for contract in PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS
            ],
        },
    }


def _report_surface_payload() -> dict[str, JSONValue]:
    return {
        "summary_schema_keys": {
            surface.key: surface.summary_schema_key
            for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        },
        "full_report_metadata": _json_strings(
            tuple(
                surface.report_metadata_key
                for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
            )
        ),
        "bounded_payload_keys": _json_strings(
            tuple(
                surface.bounded_payload_key
                for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
            )
        ),
        "cli_summary_sections": _json_strings(
            tuple(
                surface.cli_summary_section
                for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
            )
        ),
        "cli_summary_headings": _json_strings(
            tuple(
                surface.cli_summary_heading
                for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
            )
        ),
        "surface_matrix": [
            surface.to_discovery_payload()
            for surface in FINGERPRINT_REPORT_SURFACE_CONTRACTS
        ],
    }


def _calculation_basis_payload() -> dict[str, JSONValue]:
    return {
        "basis": _description_mapping(FINGERPRINT_BASIS_DESCRIPTIONS),
        "row_order": _description_mapping(FINGERPRINT_ROW_ORDER_DESCRIPTIONS),
        "computed_from": _description_mapping(
            FINGERPRINT_COMPUTED_FROM_DESCRIPTIONS
        ),
        "cache_source": _description_mapping(
            FINGERPRINT_CACHE_SOURCE_DESCRIPTIONS
        ),
    }


def _vocabulary_payload() -> dict[str, JSONValue]:
    return {
        "section_statuses": _json_strings(FINGERPRINT_SECTION_STATUSES),
        "dynamics_statuses": _json_strings(FINGERPRINT_DYNAMICS_STATUSES),
        "readiness_statuses": _json_strings(FINGERPRINT_READINESS_STATUSES),
        "eligibility_statuses": _json_strings(FINGERPRINT_ELIGIBILITY_STATUSES),
        "skip_and_reason_codes": _json_strings(FINGERPRINT_SKIP_REASON_CODES),
        "topology_limitations": _json_strings(FINGERPRINT_TOPOLOGY_LIMITATIONS),
        "conditional_distribution_groups": _json_strings(
            FINGERPRINT_CONDITIONAL_DISTRIBUTION_GROUPS
        ),
    }


def _example_payload() -> dict[str, JSONValue]:
    expected_sections = _target_section_names_for_timeframe(TICK)
    emitted_sections = ("coverage", "temporal_topology")
    skipped_sections = tuple(
        section
        for section in expected_sections
        if section not in emitted_sections
    )
    return {
        "target_axis": {
            "data_format": "ascii",
            "timeframe": TICK,
            "symbol": "EURUSD",
            "period": "201202",
            "kind": "csv",
        },
        "series_fingerprint_fragment": {
            "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
            "fingerprint_id": "sha256:example",
            "target_axis": {
                "data_format": "ascii",
                "timeframe": TICK,
                "symbol": "EURUSD",
                "period": "201202",
                "kind": "csv",
            },
            "coverage": {
                "row_count": 0,
                "parsed_row_count": 0,
                "start_timestamp_utc_ms": None,
                "end_timestamp_utc_ms": None,
                "duration_ms": None,
            },
            "source": {
                "kind": "csv_text",
                "path": "data/ASCII/T/EURUSD/2012/02/DAT_ASCII_EURUSD_T_201202.csv",
            },
            "fingerprint_audit": {
                "schema_version": TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
                "sections_expected": _json_strings(expected_sections),
                "sections_emitted": _json_strings(emitted_sections),
                "sections_skipped": {
                    section: {"reason": "not_emitted"}
                    for section in skipped_sections
                },
                "section_statuses": {
                    section: (
                        "limited" if section in emitted_sections else "skipped"
                    )
                    for section in expected_sections
                },
            },
        },
        "readiness_summary_fragment": {
            "schema_version": (
                TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
            ),
            "rule_id": SERIES_FINGERPRINT_RULE_ID,
            "target_count": 1,
            "included_target_count": 1,
            "truncated": False,
            "section_status_counts": {
                "coverage": {"limited": 1},
                "temporal_topology": {"limited": 1},
            },
            "target_summaries": [
                {
                    "target_axis": {
                        "data_format": "ascii",
                        "timeframe": TICK,
                        "symbol": "EURUSD",
                        "period": "201202",
                        "kind": "csv",
                    },
                    "applicable_dynamics_section": "microstructure_dynamics",
                    "applicable_dynamics_status": "unavailable",
                }
            ],
        },
        "profile_override_fragment": {
            "schema_version": "histdatacom.quality-profile.v1",
            "rules": {
                SERIES_FINGERPRINT_RULE_ID: {
                    "quantiles": [0.05, 0.5, 0.95],
                    "lags": [1, 5, 30],
                    "rolling_windows": [60, 240],
                    "max_rows": 100000,
                }
            },
        },
    }


def _consumer_guidance_payload() -> dict[str, JSONValue]:
    return {
        "use_schema_discovery_for": _json_strings(
            (
                "discovering supported fingerprint schemas and metadata keys",
                "checking profile-controlled fingerprint knobs",
                "building downstream parsers or synthetic-data validators",
                "reading status, reason, basis, and limitation vocabulary",
            )
        ),
        "use_data_quality_for": _json_strings(
            (
                "generating fingerprints for real local targets",
                "computing distributions, dynamics, dependence, topology, and readiness",
                "writing full quality reports and bounded runtime payloads",
            )
        ),
        "non_goals": _json_strings(
            (
                "does not read target data",
                "does not generate fingerprints",
                "does not create GitHub issues or workflow artifacts",
                "does not expose unbounded golden fixtures",
            )
        ),
    }


def _json_strings(values: tuple[object, ...]) -> list[JSONValue]:
    return [str(value) for value in values]


def _json_string_list(value: object) -> list[JSONValue]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _string_values(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value)


def _description_mapping(
    pairs: tuple[tuple[str, str], ...],
) -> dict[str, JSONValue]:
    return {key: value for key, value in pairs}


def _target_section_names_for_timeframe(timeframe: str) -> tuple[str, ...]:
    return tuple(
        contract.name
        for contract in IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS
        if timeframe in contract.target_timeframes
        and contract.name != "fingerprint_audit"
    )


def _mapping(value: object) -> Mapping[str, JSONValue]:
    if isinstance(value, Mapping):
        return cast(Mapping[str, JSONValue], value)
    return {}


def _mapping_rows(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [
        cast(Mapping[str, JSONValue], item)
        for item in value
        if isinstance(item, Mapping)
    ]


def _format_list(value: object) -> str:
    if not isinstance(value, list):
        return "[]"
    return "[" + ", ".join(str(item) for item in value) + "]"


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return 0

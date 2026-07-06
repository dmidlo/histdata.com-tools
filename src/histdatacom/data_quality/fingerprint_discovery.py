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
    IMPLEMENTED_FINGERPRINT_TARGET_SECTION_CONTRACTS,
    PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS,
    PLANNED_FINGERPRINT_TARGET_SECTION_CONTRACTS,
)
from histdatacom.data_quality.fingerprints import (
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
from histdatacom.histdata_ascii import M1, TICK
from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_path,
)
from histdatacom.runtime_contracts import JSONValue

TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-schema-discovery.v1"
)


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
            "series_fingerprint": TIME_SERIES_FINGERPRINT_METADATA_KEY
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
    run_contract = PLANNED_FINGERPRINT_RUN_SECTION_CONTRACTS[0]
    return {
        "supported_target_kinds": _json_strings(("csv", "zip", "cache")),
        "supported_data_format": "ascii",
        "supported_timeframes": _json_strings((M1, TICK)),
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
    expected_sections = _target_section_names_for_timeframe(M1)
    emitted_sections = ("coverage", "temporal_topology")
    skipped_sections = tuple(
        section
        for section in expected_sections
        if section not in emitted_sections
    )
    return {
        "target_axis": {
            "data_format": "ascii",
            "timeframe": M1,
            "symbol": "EURUSD",
            "period": "201202",
            "kind": "csv",
        },
        "series_fingerprint_fragment": {
            "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
            "fingerprint_id": "sha256:example",
            "target_axis": {
                "data_format": "ascii",
                "timeframe": M1,
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
                "path": "data/ASCII/M1/EURUSD/2012/02/DAT_ASCII_EURUSD_M1_201202.csv",
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
                        "timeframe": M1,
                        "symbol": "EURUSD",
                        "period": "201202",
                        "kind": "csv",
                    },
                    "applicable_dynamics_section": "return_dynamics",
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

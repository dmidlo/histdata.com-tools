"""Application-owned discovery for time-series fingerprint contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import histdatacom
from histdatacom.data_quality.calendar import (
    TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION,
)
from histdatacom.data_quality.fingerprints import (
    CROSS_SERIES_FINGERPRINT_RULE_ID,
    DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION,
    DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED,
    DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE,
    DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE,
    DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT,
    DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE,
    DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
    DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
    DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
    FINGERPRINT_AUDIT_SECTIONS,
    FINGERPRINT_DYNAMICS_SECTIONS,
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
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

_SERIES_FINGERPRINT_CONFIG_KEYS = (
    "quantiles",
    "lags",
    "rolling_windows",
    "histogram_bins",
    "max_rows",
    "rounding_digits",
    "distribution_attention",
)
_DISTRIBUTION_ATTENTION_CONFIG_KEYS = (
    "invalid_row_min_count",
    "invalid_row_min_rate",
    "zero_spread_min_count",
    "zero_spread_min_rate",
    "negative_spread_min_count",
    "negative_spread_min_rate",
    "flag_truncated_distribution",
    "flag_cache_float_precision",
)
_IMPLEMENTED_TARGET_SECTION_NAMES = (
    "coverage",
    "temporal_topology",
    "calendar_regimes",
    "m1_bar_distribution",
    "tick_distribution",
    "conditional_distributions",
    "return_dynamics",
    "microstructure_dynamics",
    "dependence",
    "fingerprint_audit",
)
_PLANNED_TARGET_SECTION_NAMES = (
    "stationarity_diagnostics",
    "decomposition",
    "synthetic_constraints",
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
    for key in (
        "series_fingerprint",
        "fingerprint_audit",
        "fingerprint_dynamics",
        "fingerprint_dependence",
        "fingerprint_readiness_summary",
    ):
        schema = _mapping(schemas.get(key))
        lines.append(f"- {key}: {schema.get('schema_version', '')}")

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
        "configurable_keys": _json_strings(_SERIES_FINGERPRINT_CONFIG_KEYS),
        "distribution_attention_configurable_keys": _json_strings(
            _DISTRIBUTION_ATTENTION_CONFIG_KEYS
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
        "section_limits": {
            "topology_summary_target_limit": DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
            "topology_attention_target_limit": DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
            "distribution_summary_target_limit": DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
            "distribution_attention_target_limit": DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT,
            "readiness_summary_target_limit": DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
        },
        "distribution_attention_defaults": {
            "invalid_row_min_count": DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT,
            "invalid_row_min_rate": DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE,
            "zero_spread_min_count": DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT,
            "zero_spread_min_rate": DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE,
            "negative_spread_min_count": DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT,
            "negative_spread_min_rate": DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE,
            "flag_truncated_distribution": DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED,
            "flag_cache_float_precision": (
                DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION
            ),
        },
    }


def _schema_payload() -> dict[str, JSONValue]:
    return {
        "series_fingerprint": _schema_entry(
            TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_METADATA_KEY,
            status="implemented",
        ),
        "fingerprint_coverage_summary": _schema_entry(
            TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
            status="implemented",
        ),
        "fingerprint_topology_summary": _schema_entry(
            TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
            status="implemented",
        ),
        "fingerprint_topology_attention": _schema_entry(
            TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
            status="implemented",
        ),
        "fingerprint_distribution_summary": _schema_entry(
            TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
            status="implemented",
        ),
        "fingerprint_distribution_attention": _schema_entry(
            TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
            status="implemented",
        ),
        "fingerprint_calendar_regimes": _schema_entry(
            TIME_SERIES_FINGERPRINT_CALENDAR_REGIMES_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            payload_path="time_series_fingerprint.calendar_regimes",
            status="implemented",
        ),
        "fingerprint_conditional_distributions": _schema_entry(
            TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            payload_path="time_series_fingerprint.conditional_distributions",
            status="implemented",
        ),
        "fingerprint_dynamics": _schema_entry(
            TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            payload_path="time_series_fingerprint.return_dynamics|microstructure_dynamics",
            status="implemented",
        ),
        "fingerprint_dependence": _schema_entry(
            TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            payload_path="time_series_fingerprint.dependence",
            status="implemented",
        ),
        "fingerprint_audit": _schema_entry(
            TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            payload_path="time_series_fingerprint.fingerprint_audit",
            status="implemented",
        ),
        "fingerprint_readiness_summary": _schema_entry(
            TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
            rule_id=SERIES_FINGERPRINT_RULE_ID,
            metadata_key=TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
            bounded_payload_key="fingerprint_readiness",
            status="implemented",
        ),
        "cross_series_fingerprint": {
            "schema_version": None,
            "rule_id": CROSS_SERIES_FINGERPRINT_RULE_ID,
            "status": "planned",
            "issue": "#331",
        },
    }


def _schema_entry(
    schema_version: str,
    *,
    rule_id: str,
    status: str,
    metadata_key: str = "",
    payload_path: str = "",
    bounded_payload_key: str = "",
) -> dict[str, JSONValue]:
    entry: dict[str, JSONValue] = {
        "schema_version": schema_version,
        "rule_id": rule_id,
        "status": status,
    }
    if metadata_key:
        entry["metadata_key"] = metadata_key
    if payload_path:
        entry["payload_path"] = payload_path
    if bounded_payload_key:
        entry["bounded_payload_key"] = bounded_payload_key
    return entry


def _metadata_key_payload() -> dict[str, JSONValue]:
    return {
        "finding_metadata": {
            "series_fingerprint": TIME_SERIES_FINGERPRINT_METADATA_KEY
        },
        "report_metadata": {
            "coverage_summary": TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
            "topology_summary": TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
            "topology_attention": TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
            "distribution_summary": (
                TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY
            ),
            "distribution_attention": (
                TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY
            ),
            "readiness_summary": (
                TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
            ),
        },
        "bounded_payload": {
            "coverage_summary": "fingerprint_coverage",
            "topology_summary": "fingerprint_topology",
            "topology_attention": "fingerprint_topology_attention",
            "distribution_summary": "fingerprint_distribution",
            "distribution_attention": "fingerprint_distribution_attention",
            "readiness_summary": "fingerprint_readiness",
        },
    }


def _target_capability_payload() -> dict[str, JSONValue]:
    return {
        "supported_target_kinds": _json_strings(("csv", "zip", "cache")),
        "supported_data_format": "ascii",
        "supported_timeframes": _json_strings((M1, TICK)),
        "series_rule_id": SERIES_FINGERPRINT_RULE_ID,
        "run_rule_status": {
            "rule_id": CROSS_SERIES_FINGERPRINT_RULE_ID,
            "status": "planned",
            "issue": "#331",
        },
    }


def _section_payload() -> dict[str, JSONValue]:
    return {
        "implemented": {
            "audit_sections": _json_strings(FINGERPRINT_AUDIT_SECTIONS),
            "dynamics_sections": _json_strings(FINGERPRINT_DYNAMICS_SECTIONS),
            "target_sections": [
                _section_entry(
                    "coverage",
                    "all supported targets",
                    timeframes=(M1, TICK),
                    schema_key="series_fingerprint",
                    fields=(
                        "row_count",
                        "parsed_row_count",
                        "start_timestamp_utc_ms",
                        "end_timestamp_utc_ms",
                    ),
                ),
                _section_entry(
                    "temporal_topology",
                    "timestamp continuity, ordering, gaps, duplicates, and sampling basis",
                    timeframes=(M1, TICK),
                    schema_key="series_fingerprint",
                    basis=("observed_sequence",),
                ),
                _section_entry(
                    "calendar_regimes",
                    "session, special-window, holiday, event, hour, and weekday counts",
                    timeframes=(M1, TICK),
                    schema_key="fingerprint_calendar_regimes",
                    basis=("text_scan", "direct_cache", "fresh_sibling_cache"),
                ),
                _section_entry(
                    "m1_bar_distribution",
                    "OHLC price, bar shape, precision, and invalid-row summaries",
                    timeframes=(M1,),
                    schema_key="series_fingerprint",
                ),
                _section_entry(
                    "tick_distribution",
                    "bid, ask, spread, precision, zero/negative spread, and invalid-row summaries",
                    timeframes=(TICK,),
                    schema_key="series_fingerprint",
                ),
                _section_entry(
                    "conditional_distributions",
                    "bounded tick-spread summaries by active session and special tag",
                    timeframes=(TICK,),
                    schema_key="fingerprint_conditional_distributions",
                    basis=("text", "cache"),
                    extra={
                        "metric": "tick_spread",
                        "grouped_by": ["active_session", "special_tag"],
                    },
                ),
                _section_entry(
                    "return_dynamics",
                    "M1 close returns, open jumps, flatlines, and sequence limitations",
                    timeframes=(M1,),
                    schema_key="fingerprint_dynamics",
                    basis=("observed_sequence",),
                    row_order=("source_text_order", "cache_order"),
                ),
                _section_entry(
                    "microstructure_dynamics",
                    "tick interarrival, spread changes, stale quotes, bursts, and one-sided movement",
                    timeframes=(TICK,),
                    schema_key="fingerprint_dynamics",
                    basis=("observed_sequence",),
                    row_order=("source_text_order", "cache_order"),
                ),
                _section_entry(
                    "dependence",
                    "observed-sequence lag autocorrelation for returns, ranges, spreads, and spread changes",
                    timeframes=(M1, TICK),
                    schema_key="fingerprint_dependence",
                    basis=("observed_sequence",),
                    row_order=("source_text_order", "cache_order"),
                    extra={
                        "acf_basis": "observed_sequence",
                        "profile_controlled_by": ["lags", "rounding_digits"],
                    },
                ),
                _section_entry(
                    "fingerprint_audit",
                    "machine-readable expected/emitted/skipped section accounting and readiness",
                    timeframes=(M1, TICK),
                    schema_key="fingerprint_audit",
                ),
            ],
        },
        "planned": {
            "target_sections": [
                _planned_section("stationarity_diagnostics", "#329"),
                _planned_section("decomposition", "#330"),
                _planned_section("synthetic_constraints", "#333"),
            ],
            "run_sections": [
                {
                    "name": "cross_series_fingerprint",
                    "status": "planned",
                    "rule_id": CROSS_SERIES_FINGERPRINT_RULE_ID,
                    "issue": "#331",
                }
            ],
        },
    }


def _section_entry(
    name: str,
    description: str,
    *,
    timeframes: tuple[str, ...],
    schema_key: str,
    fields: tuple[str, ...] = (),
    basis: tuple[str, ...] = (),
    row_order: tuple[str, ...] = (),
    extra: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "name": name,
        "status": "implemented",
        "description": description,
        "target_timeframes": _json_strings(timeframes),
        "schema_key": schema_key,
    }
    if fields:
        payload["key_fields"] = _json_strings(fields)
    if basis:
        payload["basis_values"] = _json_strings(basis)
    if row_order:
        payload["row_order_values"] = _json_strings(row_order)
    if extra:
        payload.update(dict(extra))
    return payload


def _planned_section(name: str, issue: str) -> dict[str, JSONValue]:
    return {
        "name": name,
        "status": "planned",
        "schema_version": None,
        "issue": issue,
    }


def _report_surface_payload() -> dict[str, JSONValue]:
    return {
        "full_report_metadata": _json_strings(
            (
                TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY,
                TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY,
                TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY,
                TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY,
                TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY,
                TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
            )
        ),
        "bounded_payload_keys": _json_strings(
            (
                "fingerprint_coverage",
                "fingerprint_topology",
                "fingerprint_topology_attention",
                "fingerprint_distribution",
                "fingerprint_distribution_attention",
                "fingerprint_readiness",
            )
        ),
        "cli_summary_sections": _json_strings(
            (
                "coverage",
                "distribution_attention",
                "distribution_summary",
                "topology_attention",
                "topology_summary",
                "readiness_summary",
            )
        ),
    }


def _calculation_basis_payload() -> dict[str, JSONValue]:
    return {
        "basis": {
            "observed_sequence": "statistics computed over parsed row order without regular-grid imputation",
            "regular_grid": "reserved for future grid-regularized calculations",
            "limited": "section emitted with advisory limitations",
            "unavailable": "section could not compute enough contract data",
        },
        "row_order": {
            "source_text_order": "rows were scanned from source CSV or ZIP member text order",
            "cache_order": "rows were scanned from the selected Polars cache order",
            "none": "no row sequence was available",
            "unknown": "older or incomplete payload did not state row order",
        },
        "computed_from": {
            "text_scan": "source text was read directly",
            "direct_cache": "target itself was a cache",
            "fresh_sibling_cache": "fresh sibling cache was used for the source target",
            "unavailable": "source and cache projection were not usable",
            "unknown": "older or incomplete payload did not state source basis",
        },
        "cache_source": {
            "direct": "cache target was evaluated directly",
            "sibling": "fresh sibling cache was selected for a source target",
            "none": "no cache source participated",
        },
    }


def _vocabulary_payload() -> dict[str, JSONValue]:
    return {
        "section_statuses": _json_strings(
            ("valid", "limited", "skipped", "unavailable")
        ),
        "dynamics_statuses": _json_strings(("ok", "limited", "unavailable")),
        "readiness_statuses": _json_strings(
            (
                "computed",
                "valid",
                "limited",
                "skipped",
                "unavailable",
                "not_applicable",
            )
        ),
        "eligibility_statuses": _json_strings(("eligible", "ineligible")),
        "skip_and_reason_codes": _json_strings(
            (
                "unsupported_timeframe",
                "unsupported_target_kind",
                "source_unreadable",
                "cache_unavailable",
                "missing_required_columns",
                "metric_not_available",
                "insufficient_rows",
                "insufficient_sequence_rows",
                "insufficient_sample_count",
                "zero_variance",
                "no_computable_lags",
                "skipped_lags",
                "not_emitted",
            )
        ),
        "topology_limitations": _json_strings(
            (
                "timestamp_topology_unavailable",
                "no_parsed_timestamps",
                "invalid_timestamps_skipped",
                "non_monotonic_timestamp_order",
                "duplicate_timestamps",
                "suspicious_gaps",
                "expected_session_closures",
                "weekend_activity",
            )
        ),
        "conditional_distribution_groups": _json_strings(
            ("active_session", "special_tag")
        ),
    }


def _example_payload() -> dict[str, JSONValue]:
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
                "sections_expected": [
                    "coverage",
                    "temporal_topology",
                    "calendar_regimes",
                    "m1_bar_distribution",
                    "return_dynamics",
                    "dependence",
                ],
                "sections_emitted": ["coverage", "temporal_topology"],
                "sections_skipped": {
                    "calendar_regimes": {"reason": "not_emitted"},
                    "m1_bar_distribution": {"reason": "not_emitted"},
                    "return_dynamics": {"reason": "not_emitted"},
                    "dependence": {"reason": "not_emitted"},
                },
                "section_statuses": {
                    "coverage": "limited",
                    "temporal_topology": "limited",
                    "calendar_regimes": "skipped",
                    "m1_bar_distribution": "skipped",
                    "return_dynamics": "skipped",
                    "dependence": "skipped",
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

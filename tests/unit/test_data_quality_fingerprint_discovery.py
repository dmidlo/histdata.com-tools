"""Tests for fingerprint schema/profile discovery contracts."""

from __future__ import annotations

import json
from pathlib import Path

from histdatacom.data_quality.fingerprint_discovery import (
    TIME_SERIES_FINGERPRINT_SCHEMA_DISCOVERY_SCHEMA_VERSION,
    fingerprint_schema_discovery,
    format_fingerprint_schema_discovery,
)
from histdatacom.data_quality.fingerprints import (
    SERIES_FINGERPRINT_RULE_ID,
    TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY,
    TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION,
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
)
from histdatacom.data_quality.profiles import (
    QUALITY_PROFILE_SCHEMA_VERSION,
    load_quality_profile_file,
)


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
    assert schemas["fingerprint_readiness_summary"]["schema_version"] == (
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
    )
    assert schemas["cross_series_fingerprint"]["status"] == "planned"
    assert payload["metadata_keys"]["finding_metadata"] == {
        "series_fingerprint": TIME_SERIES_FINGERPRINT_METADATA_KEY
    }
    assert payload["metadata_keys"]["report_metadata"]["readiness_summary"] == (
        TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY
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
        "fingerprint_audit",
    ]
    planned = payload["sections"]["planned"]["target_sections"]
    assert [section["name"] for section in planned] == [
        "stationarity_diagnostics",
        "decomposition",
        "synthetic_constraints",
    ]
    assert "observed_sequence" in payload["calculation_bases"]["basis"]
    assert "source_text_order" in payload["calculation_bases"]["row_order"]
    assert "not_emitted" in payload["vocabularies"]["skip_and_reason_codes"]


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
    assert "without reading source or running data quality checks" in output

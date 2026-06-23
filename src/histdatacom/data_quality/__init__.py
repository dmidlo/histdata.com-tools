"""Data-quality assessment contracts and orchestration helpers."""

from __future__ import annotations

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityReport,
    QualityRule,
    QualityRuleResult,
    QualityRunRule,
    QualityRunSummary,
    QualitySeverity,
    QualityStatus,
    QualityTarget,
    QualityTargetKind,
    QualityTargetSummary,
)
from histdatacom.data_quality.discovery import (
    QUALITY_CHECK_GROUPS,
    QualityDiscoveryError,
    QualityDiscoveryResult,
    discover_quality_targets,
    normalize_quality_check_groups,
    quality_metadata_from_filename,
    quality_target_from_path,
)
from histdatacom.data_quality.engine import (
    evaluate_quality_rule,
    run_quality_assessment,
)
from histdatacom.data_quality.ingestion import (
    ASCII_ROW_COUNT_INGESTION_RULE_ID,
    ASCII_SCHEMA_INGESTION_RULE_ID,
    ASCII_TEXT_INGESTION_RULE_ID,
    HistDataAsciiRowCountIngestionRule,
    HistDataAsciiSchemaIngestionRule,
    HistDataAsciiTextIngestionRule,
    ingestion_quality_rules,
)
from histdatacom.data_quality.inventory import (
    HistDataZipInventoryRule,
    inventory_quality_rules,
)
from histdatacom.data_quality.manifest import (
    COVERAGE_MANIFEST_SCHEMA_VERSION,
    CoverageDimension,
    HistDataCoverageManifestRule,
    coverage_manifest_metadata,
    manifest_quality_run_rules,
)
from histdatacom.data_quality.reporting import (
    QUALITY_EXIT_TRIGGERS,
    QUALITY_REPORT_SCHEMA_VERSION,
    QualityExitDecision,
    QualityExitPolicy,
    QualityExitTrigger,
    bounded_quality_payload,
    format_quality_console_summary,
    quality_report_payload,
    quality_report_to_json,
    write_quality_report,
)
from histdatacom.data_quality.rules import (
    quality_rules_for_groups,
    quality_run_rules_for_groups,
)
from histdatacom.data_quality.time import (
    ASCII_EST_NO_DST_TIME_RULE_ID,
    ASCII_TIMESTAMP_SEQUENCE_RULE_ID,
    HistDataAsciiEstNoDstTimeRule,
    HistDataAsciiTimestampSequenceRule,
    time_quality_rules,
)

__all__ = [
    "ASCII_EST_NO_DST_TIME_RULE_ID",
    "ASCII_ROW_COUNT_INGESTION_RULE_ID",
    "ASCII_SCHEMA_INGESTION_RULE_ID",
    "ASCII_TEXT_INGESTION_RULE_ID",
    "ASCII_TIMESTAMP_SEQUENCE_RULE_ID",
    "COVERAGE_MANIFEST_SCHEMA_VERSION",
    "QUALITY_EXIT_TRIGGERS",
    "QUALITY_REPORT_SCHEMA_VERSION",
    "CoverageDimension",
    "HistDataAsciiEstNoDstTimeRule",
    "HistDataAsciiRowCountIngestionRule",
    "HistDataAsciiSchemaIngestionRule",
    "HistDataAsciiTextIngestionRule",
    "HistDataAsciiTimestampSequenceRule",
    "HistDataCoverageManifestRule",
    "HistDataZipInventoryRule",
    "QualityFinding",
    "QualityDiscoveryError",
    "QualityDiscoveryResult",
    "QualityExitDecision",
    "QualityExitPolicy",
    "QualityExitTrigger",
    "QualityLocation",
    "QualityReport",
    "QualityRule",
    "QualityRuleResult",
    "QualityRunRule",
    "QualityRunSummary",
    "QualitySeverity",
    "QualityStatus",
    "QualityTarget",
    "QualityTargetKind",
    "QualityTargetSummary",
    "QUALITY_CHECK_GROUPS",
    "bounded_quality_payload",
    "coverage_manifest_metadata",
    "discover_quality_targets",
    "evaluate_quality_rule",
    "format_quality_console_summary",
    "ingestion_quality_rules",
    "inventory_quality_rules",
    "manifest_quality_run_rules",
    "normalize_quality_check_groups",
    "quality_metadata_from_filename",
    "quality_report_payload",
    "quality_report_to_json",
    "quality_rules_for_groups",
    "quality_run_rules_for_groups",
    "quality_target_from_path",
    "run_quality_assessment",
    "time_quality_rules",
    "write_quality_report",
]

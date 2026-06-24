"""Rule selection helpers for data-quality assessment groups."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, TypeVar

from histdatacom.data_quality.contracts import (
    QualityRule,
    QualityRunRule,
    QualitySeverity,
)
from histdatacom.data_quality.bars import (
    DEFAULT_M1_OUTLIER_THRESHOLDS_BY_ASSET_CLASS,
    HistDataAsciiM1BarIntegrityRule,
    HistDataAsciiM1OutlierRule,
    HistDataAsciiM1PrecisionRule,
    HistDataAsciiM1TickReconstructionRule,
)
from histdatacom.data_quality.calendar import calendar_quality_rules
from histdatacom.data_quality.discovery import normalize_quality_check_groups
from histdatacom.data_quality.ingestion import (
    HistDataAsciiRowCountIngestionRule,
    HistDataAsciiSchemaIngestionRule,
    HistDataAsciiTextIngestionRule,
)
from histdatacom.data_quality.inventory import inventory_quality_rules
from histdatacom.data_quality.manifest import manifest_quality_run_rules
from histdatacom.data_quality.modeling import HistDataModelingReadinessRule
from histdatacom.data_quality.profiles import (
    QualityProfile,
    quality_profile_from_value,
)
from histdatacom.data_quality.symbols import (
    HistDataCrossInstrumentConsistencyRule,
    domain_quality_rules,
)
from histdatacom.data_quality.time import (
    HistDataAsciiEstNoDstTimeRule,
    HistDataAsciiTimestampContinuityRule,
    HistDataAsciiTimestampGapRule,
    HistDataAsciiTimestampSequenceRule,
)
from histdatacom.data_quality.ticks import (
    DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS_BY_ASSET_CLASS,
    DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS_BY_ASSET_CLASS,
    DEFAULT_TICK_SPREAD_THRESHOLDS_BY_ASSET_CLASS,
    HistDataAsciiTickMicrostructureRule,
    HistDataAsciiTickSpreadRegimeRule,
    HistDataAsciiTickSpreadRule,
)
from histdatacom.runtime_contracts import JSONValue

_ThresholdT = TypeVar("_ThresholdT")


def quality_rules_for_groups(
    groups: Iterable[str] | None,
    *,
    profile: Mapping[str, Any] | QualityProfile | None = None,
) -> tuple[QualityRule, ...]:
    """Return concrete quality rules for normalized check group selections."""
    normalized = normalize_quality_check_groups(groups)
    quality_profile = quality_profile_from_value(profile)
    rules: list[QualityRule] = []
    if "all" in normalized or "inventory" in normalized:
        rules.extend(inventory_quality_rules())
    if "all" in normalized or "ingestion" in normalized:
        rules.extend(_ingestion_quality_rules(quality_profile))
    if "all" in normalized or "time" in normalized:
        rules.extend(_time_quality_rules(quality_profile))
    if "all" in normalized or "bars" in normalized:
        rules.extend(_bars_quality_rules(quality_profile))
    if "all" in normalized or "ticks" in normalized:
        rules.extend(_ticks_quality_rules(quality_profile))
    if "all" in normalized or "domain" in normalized:
        rules.extend(domain_quality_rules())
        rules.extend(calendar_quality_rules())
    if "all" in normalized or "modeling" in normalized:
        rules.extend(_modeling_quality_rules(quality_profile))
    return tuple(rules)


def quality_run_rules_for_groups(
    groups: Iterable[str] | None,
    *,
    profile: Mapping[str, Any] | QualityProfile | None = None,
) -> tuple[QualityRunRule, ...]:
    """Return run-scoped quality rules for normalized check selections."""
    normalized = normalize_quality_check_groups(groups)
    quality_profile = quality_profile_from_value(profile)
    rules: list[QualityRunRule] = []
    if "all" in normalized or "inventory" in normalized:
        rules.extend(manifest_quality_run_rules())
    if "all" in normalized or "time" in normalized:
        rules.extend(_time_quality_run_rules(quality_profile))
    if "all" in normalized or "bars" in normalized:
        rules.extend(_bars_quality_run_rules(quality_profile))
    if "all" in normalized or "domain" in normalized:
        rules.extend(_domain_quality_run_rules(quality_profile))
    return tuple(rules)


def quality_profile_report_metadata(
    profile: Mapping[str, Any] | QualityProfile | None,
) -> dict[str, JSONValue]:
    """Return report metadata for a normalized quality profile."""
    return {
        "quality_profile": quality_profile_from_value(profile).to_metadata()
    }


def _ingestion_quality_rules(
    profile: QualityProfile,
) -> tuple[QualityRule, ...]:
    row_count = profile.row_count_profile()
    return (
        HistDataAsciiRowCountIngestionRule(
            min_row_count=row_count.min_row_count,
            min_size_bytes=row_count.min_size_bytes,
            tiny_severity=profile.severity(
                "ingestion.ascii.row_count",
                "tiny_severity",
                QualitySeverity.WARNING,
            ),
            size_severity=profile.severity(
                "ingestion.ascii.row_count",
                "size_severity",
                QualitySeverity.WARNING,
            ),
            truncation_severity=profile.severity(
                "ingestion.ascii.row_count",
                "truncation_severity",
                QualitySeverity.WARNING,
            ),
        ),
        HistDataAsciiTextIngestionRule(),
        HistDataAsciiSchemaIngestionRule(),
    )


def _time_quality_rules(profile: QualityProfile) -> tuple[QualityRule, ...]:
    return (
        HistDataAsciiEstNoDstTimeRule(),
        HistDataAsciiTimestampSequenceRule(),
        HistDataAsciiTimestampGapRule(
            tolerance=profile.gap_tolerance("time.ascii.gaps"),
            warning_severity=profile.severity(
                "time.ascii.gaps",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
    )


def _time_quality_run_rules(
    profile: QualityProfile,
) -> tuple[QualityRunRule, ...]:
    return (
        HistDataAsciiTimestampContinuityRule(
            tolerance=profile.gap_tolerance("time.ascii.continuity"),
            warning_severity=profile.severity(
                "time.ascii.continuity",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
    )


def _bars_quality_rules(profile: QualityProfile) -> tuple[QualityRule, ...]:
    return (
        HistDataAsciiM1BarIntegrityRule(),
        HistDataAsciiM1PrecisionRule(
            precision_rules_by_symbol=(profile.m1_precision_rules_by_symbol()),
            precision_rules_by_asset_class=(
                profile.m1_precision_rules_by_asset_class()
            ),
            warning_severity=profile.severity(
                "bars.ascii.m1_precision",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
        HistDataAsciiM1OutlierRule(
            thresholds=profile.m1_outlier_thresholds(),
            thresholds_by_symbol=profile.m1_outlier_thresholds_by_symbol(),
            thresholds_by_asset_class=_merged_thresholds(
                DEFAULT_M1_OUTLIER_THRESHOLDS_BY_ASSET_CLASS,
                profile.m1_outlier_thresholds_by_asset_class(),
            ),
            warning_severity=profile.severity(
                "bars.ascii.m1_outliers",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
    )


def _bars_quality_run_rules(
    profile: QualityProfile,
) -> tuple[QualityRunRule, ...]:
    return (
        HistDataAsciiM1TickReconstructionRule(
            tolerance=profile.m1_tick_reconstruction_tolerance(),
            warning_severity=profile.severity(
                "bars.ascii.m1_tick_reconstruction",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
    )


def _ticks_quality_rules(profile: QualityProfile) -> tuple[QualityRule, ...]:
    return (
        HistDataAsciiTickSpreadRule(
            thresholds=profile.tick_spread_thresholds(),
            thresholds_by_asset_class=_merged_thresholds(
                DEFAULT_TICK_SPREAD_THRESHOLDS_BY_ASSET_CLASS,
                profile.tick_spread_thresholds_by_asset_class(),
            ),
            zero_spread_severity=profile.severity(
                "ticks.ascii.spread",
                "zero_spread_severity",
                QualitySeverity.WARNING,
            ),
            negative_spread_severity=profile.severity(
                "ticks.ascii.spread",
                "negative_spread_severity",
                QualitySeverity.ERROR,
            ),
            schema_severity=profile.severity(
                "ticks.ascii.spread",
                "schema_severity",
                QualitySeverity.ERROR,
            ),
        ),
        HistDataAsciiTickMicrostructureRule(
            thresholds=profile.tick_microstructure_thresholds(),
            thresholds_by_symbol=(
                profile.tick_microstructure_thresholds_by_symbol()
            ),
            thresholds_by_session=(
                profile.tick_microstructure_thresholds_by_session()
            ),
            thresholds_by_asset_class=_merged_thresholds(
                DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS_BY_ASSET_CLASS,
                profile.tick_microstructure_thresholds_by_asset_class(),
            ),
            thresholds_by_symbol_session=(
                profile.tick_microstructure_thresholds_by_symbol_session()
            ),
            session_name=profile.tick_microstructure_session_name(),
            warning_severity=profile.severity(
                "ticks.ascii.microstructure",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
        HistDataAsciiTickSpreadRegimeRule(
            thresholds=profile.tick_spread_regime_thresholds(),
            thresholds_by_asset_class=_merged_thresholds(
                DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS_BY_ASSET_CLASS,
                profile.tick_spread_regime_thresholds_by_asset_class(),
            ),
            warning_severity=profile.severity(
                "ticks.ascii.spread_regimes",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
            schema_severity=profile.severity(
                "ticks.ascii.spread_regimes",
                "schema_severity",
                QualitySeverity.WARNING,
            ),
        ),
    )


def _merged_thresholds(
    defaults: Mapping[str, _ThresholdT],
    overrides: Mapping[str, _ThresholdT],
) -> dict[str, _ThresholdT]:
    merged = {
        str(key).strip().lower(): value for key, value in defaults.items()
    }
    merged.update(
        {str(key).strip().lower(): value for key, value in overrides.items()}
    )
    return merged


def _domain_quality_run_rules(
    profile: QualityProfile,
) -> tuple[QualityRunRule, ...]:
    return (
        HistDataCrossInstrumentConsistencyRule(
            tolerance=profile.cross_instrument_tolerance(),
            warning_severity=profile.severity(
                "domain.cross_instrument_consistency",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
            error_severity=profile.severity(
                "domain.cross_instrument_consistency",
                "error_severity",
                QualitySeverity.ERROR,
            ),
        ),
    )


def _modeling_quality_rules(
    profile: QualityProfile,
) -> tuple[QualityRule, ...]:
    return (
        HistDataModelingReadinessRule(
            assumptions=profile.modeling_profile_assumptions(),
            warning_severity=profile.severity(
                "modeling.readiness",
                "warning_severity",
                QualitySeverity.WARNING,
            ),
        ),
    )

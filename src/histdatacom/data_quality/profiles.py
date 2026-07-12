"""Operator-configurable data-quality profile contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, cast

from histdatacom.data_quality.calendar import DOMAIN_CALENDAR_SESSION_RULE_ID
from histdatacom.data_quality.calendar_profiles import (
    HistDataCalendarProfile,
    calendar_profile_from_mapping,
)
from histdatacom.data_quality.autoregressive import (
    AutoregressiveProfile,
    AutoregressiveSpecification,
)
from histdatacom.data_quality.classical_baselines import (
    MAX_BASELINE_ROLLING_WINDOWS,
    ClassicalBaselineProfile,
)
from histdatacom.data_quality.classical_model_contracts import (
    ClassicalModelInputProfile,
    ClassicalModelResourcePolicy,
)
from histdatacom.data_quality.contracts import QualitySeverity
from histdatacom.data_quality.exponential_smoothing import (
    ExponentialSmoothingProfile,
    ExponentialSmoothingSpecification,
)
from histdatacom.data_quality.fingerprints import (
    DEFAULT_FINGERPRINT_HISTOGRAM_BINS,
    DEFAULT_FINGERPRINT_LAGS,
    DEFAULT_FINGERPRINT_MAX_ROWS,
    DEFAULT_FINGERPRINT_PARITY_MISMATCH_LIMIT,
    DEFAULT_FINGERPRINT_QUANTILES,
    DEFAULT_FINGERPRINT_ROLLING_WINDOWS,
    DEFAULT_FINGERPRINT_ROUNDING_DIGITS,
    DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT,
    SERIES_FINGERPRINT_RULE_ID,
    HistDataFingerprintDistributionAttentionProfile,
    HistDataFingerprintParityProfile,
    HistDataFingerprintProfile,
)
from histdatacom.data_quality.ingestion import (
    ASCII_ROW_COUNT_INGESTION_RULE_ID,
    DEFAULT_MIN_ROW_COUNT,
    DEFAULT_MIN_SIZE_BYTES,
)
from histdatacom.data_quality.modeling import MODELING_READINESS_RULE_ID
from histdatacom.data_quality.symbols import (
    DEFAULT_CROSS_INSTRUMENT_TOLERANCE,
    DOMAIN_CROSS_INSTRUMENT_RULE_ID,
    HistDataCrossInstrumentTolerance,
    HistDataSymbolPrecisionRule,
    normalize_histdata_symbol,
)
from histdatacom.data_quality.ticks import (
    ASCII_TICK_MICROSTRUCTURE_RULE_ID,
    ASCII_TICK_SPREAD_REGIME_RULE_ID,
    ASCII_TICK_SPREAD_RULE_ID,
    DEFAULT_SESSION_PROFILE,
    DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
    DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
    DEFAULT_TICK_SPREAD_THRESHOLDS,
    HistDataTickMicrostructureThresholds,
    HistDataTickSpreadRegimeThresholds,
    HistDataTickSpreadThresholds,
)
from histdatacom.data_quality.time import (
    ASCII_TIMESTAMP_CONTINUITY_RULE_ID,
    ASCII_TIMESTAMP_GAP_RULE_ID,
    HistDataGapTolerance,
)
from histdatacom.runtime_contracts import JSONValue

QUALITY_PROFILE_SCHEMA_VERSION = "histdatacom.quality-profile.v1"
QUALITY_PROFILE_RESOLUTION_SCHEMA_VERSION = (
    "histdatacom.quality-profile-resolution.v1"
)
DEFAULT_QUALITY_PROFILE_NAME = "default"
DEFAULT_QUALITY_PROFILE_SOURCE = "default"
OPERATOR_QUALITY_PROFILE_SOURCE = "operator-config"

QUALITY_PROFILE_METADATA_KEY = "quality_profile"
QUALITY_REPORTING_METADATA_KEY = "quality_reporting"

CONFIGURABLE_QUALITY_RULE_IDS = frozenset(
    {
        ASCII_ROW_COUNT_INGESTION_RULE_ID,
        ASCII_TIMESTAMP_GAP_RULE_ID,
        ASCII_TIMESTAMP_CONTINUITY_RULE_ID,
        ASCII_TICK_SPREAD_RULE_ID,
        ASCII_TICK_MICROSTRUCTURE_RULE_ID,
        ASCII_TICK_SPREAD_REGIME_RULE_ID,
        DOMAIN_CROSS_INSTRUMENT_RULE_ID,
        DOMAIN_CALENDAR_SESSION_RULE_ID,
        MODELING_READINESS_RULE_ID,
        SERIES_FINGERPRINT_RULE_ID,
    }
)

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "name",
        "source",
        "source_path",
        "rules",
        "reporting",
        "modeling_assumptions",
    }
)


class QualityProfileError(ValueError):
    """Raised when an operator quality profile is invalid."""


@dataclass(frozen=True, slots=True)
class HistDataRowCountProfile:
    """Configured ingestion row-count and byte-size thresholds."""

    min_row_count: int = DEFAULT_MIN_ROW_COUNT
    min_size_bytes: int = DEFAULT_MIN_SIZE_BYTES


@dataclass(frozen=True, slots=True)
class QualityRemediationCatalogAuditProfile:
    """Configured publication behavior for remediation-catalog audits."""

    enabled: bool = False

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {"enabled": self.enabled}


@dataclass(frozen=True, slots=True)
class QualityReportingProfile:
    """Configured quality report publication behavior."""

    remediation_catalog_audit: QualityRemediationCatalogAuditProfile = field(
        default_factory=QualityRemediationCatalogAuditProfile
    )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "remediation_catalog_audit": (
                self.remediation_catalog_audit.to_metadata()
            )
        }


@dataclass(frozen=True, slots=True)
class QualityProfile:
    """Versioned operator profile for data-quality rule construction."""

    schema_version: str = QUALITY_PROFILE_SCHEMA_VERSION
    name: str = DEFAULT_QUALITY_PROFILE_NAME
    source: str = DEFAULT_QUALITY_PROFILE_SOURCE
    source_path: str = ""
    rules: Mapping[str, Mapping[str, JSONValue]] = field(default_factory=dict)
    reporting: Mapping[str, JSONValue] = field(default_factory=dict)
    modeling_assumptions: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate static profile metadata and configured rule IDs."""
        if self.schema_version != QUALITY_PROFILE_SCHEMA_VERSION:
            msg = f"unsupported quality profile schema_version: {self.schema_version!r}"
            raise QualityProfileError(msg)
        unknown = sorted(set(self.rules) - CONFIGURABLE_QUALITY_RULE_IDS)
        if unknown:
            msg = "unknown quality profile rule IDs: " + ", ".join(unknown)
            raise QualityProfileError(msg)

    @property
    def is_default(self) -> bool:
        """Return whether no operator profile settings are configured."""
        return (
            not self.rules
            and not self.reporting
            and not self.modeling_assumptions
            and self.source == DEFAULT_QUALITY_PROFILE_SOURCE
        )

    def rule_config(self, rule_id: str) -> Mapping[str, JSONValue]:
        """Return the raw config mapping for one rule ID."""
        return self.rules.get(rule_id, {})

    def severity(
        self,
        rule_id: str,
        key: str,
        default: QualitySeverity,
    ) -> QualitySeverity:
        """Return a configured severity value for one rule field."""
        config = self.rule_config(rule_id)
        if key not in config:
            return default
        try:
            return QualitySeverity.from_value(str(config[key]))
        except ValueError as exc:
            msg = f"{rule_id}.{key}: {exc}"
            raise QualityProfileError(msg) from exc

    def row_count_profile(self) -> HistDataRowCountProfile:
        """Return configured ingestion row-count thresholds."""
        config = self.rule_config(ASCII_ROW_COUNT_INGESTION_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "min_row_count",
                "min_size_bytes",
                "tiny_severity",
                "size_severity",
                "truncation_severity",
            },
            ASCII_ROW_COUNT_INGESTION_RULE_ID,
        )
        return HistDataRowCountProfile(
            min_row_count=_int_field(
                config,
                "min_row_count",
                DEFAULT_MIN_ROW_COUNT,
                minimum=0,
                path=ASCII_ROW_COUNT_INGESTION_RULE_ID,
            ),
            min_size_bytes=_int_field(
                config,
                "min_size_bytes",
                DEFAULT_MIN_SIZE_BYTES,
                minimum=0,
                path=ASCII_ROW_COUNT_INGESTION_RULE_ID,
            ),
        )

    def gap_tolerance(self, rule_id: str) -> HistDataGapTolerance:
        """Return configured timestamp gap/session tolerance."""
        config = self.rule_config(rule_id)
        _reject_unknown_keys(
            config,
            {"tolerance", "warning_severity"},
            rule_id,
        )
        return _gap_tolerance(
            _mapping_field(config, "tolerance", path=rule_id),
            path=f"{rule_id}.tolerance",
        )

    def tick_spread_thresholds(self) -> HistDataTickSpreadThresholds:
        """Return configured tick spread thresholds."""
        config = self.rule_config(ASCII_TICK_SPREAD_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "thresholds",
                "thresholds_by_asset_class",
                "zero_spread_severity",
                "negative_spread_severity",
                "schema_severity",
            },
            ASCII_TICK_SPREAD_RULE_ID,
        )
        return _tick_spread_thresholds(
            _mapping_field(
                config,
                "thresholds",
                path=ASCII_TICK_SPREAD_RULE_ID,
            ),
            base=DEFAULT_TICK_SPREAD_THRESHOLDS,
            path=f"{ASCII_TICK_SPREAD_RULE_ID}.thresholds",
        )

    def tick_spread_thresholds_by_asset_class(
        self,
    ) -> dict[str, HistDataTickSpreadThresholds]:
        """Return configured tick spread thresholds by asset class."""
        config = self.rule_config(ASCII_TICK_SPREAD_RULE_ID)
        return _tick_spread_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_asset_class",
                path=ASCII_TICK_SPREAD_RULE_ID,
            ),
            key_normalizer=_lower_key,
            path=f"{ASCII_TICK_SPREAD_RULE_ID}.thresholds_by_asset_class",
        )

    def tick_microstructure_thresholds(
        self,
    ) -> HistDataTickMicrostructureThresholds:
        """Return configured default tick microstructure thresholds."""
        config = self.rule_config(ASCII_TICK_MICROSTRUCTURE_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "thresholds",
                "thresholds_by_symbol",
                "thresholds_by_session",
                "thresholds_by_asset_class",
                "thresholds_by_symbol_session",
                "session_name",
                "warning_severity",
            },
            ASCII_TICK_MICROSTRUCTURE_RULE_ID,
        )
        return _tick_microstructure_thresholds(
            _mapping_field(
                config,
                "thresholds",
                path=ASCII_TICK_MICROSTRUCTURE_RULE_ID,
            ),
            base=DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
            path=f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}.thresholds",
        )

    def tick_microstructure_thresholds_by_symbol(
        self,
    ) -> dict[str, HistDataTickMicrostructureThresholds]:
        """Return tick microstructure thresholds keyed by symbol."""
        config = self.rule_config(ASCII_TICK_MICROSTRUCTURE_RULE_ID)
        return _tick_microstructure_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_symbol",
                path=ASCII_TICK_MICROSTRUCTURE_RULE_ID,
            ),
            key_normalizer=normalize_histdata_symbol,
            path=f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}.thresholds_by_symbol",
        )

    def tick_microstructure_thresholds_by_session(
        self,
    ) -> dict[str, HistDataTickMicrostructureThresholds]:
        """Return tick microstructure thresholds keyed by session."""
        config = self.rule_config(ASCII_TICK_MICROSTRUCTURE_RULE_ID)
        return _tick_microstructure_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_session",
                path=ASCII_TICK_MICROSTRUCTURE_RULE_ID,
            ),
            key_normalizer=_lower_key,
            path=f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}.thresholds_by_session",
        )

    def tick_microstructure_thresholds_by_asset_class(
        self,
    ) -> dict[str, HistDataTickMicrostructureThresholds]:
        """Return tick microstructure thresholds keyed by asset class."""
        config = self.rule_config(ASCII_TICK_MICROSTRUCTURE_RULE_ID)
        return _tick_microstructure_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_asset_class",
                path=ASCII_TICK_MICROSTRUCTURE_RULE_ID,
            ),
            key_normalizer=_lower_key,
            path=(
                f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}.thresholds_by_asset_class"
            ),
        )

    def tick_microstructure_thresholds_by_symbol_session(
        self,
    ) -> dict[str, HistDataTickMicrostructureThresholds]:
        """Return tick microstructure thresholds keyed by symbol:session."""
        config = self.rule_config(ASCII_TICK_MICROSTRUCTURE_RULE_ID)
        return _tick_microstructure_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_symbol_session",
                path=ASCII_TICK_MICROSTRUCTURE_RULE_ID,
            ),
            key_normalizer=_symbol_session_key,
            path=(
                f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}.thresholds_by_symbol_session"
            ),
        )

    def tick_microstructure_session_name(self) -> str:
        """Return the configured session override for tick microstructure."""
        config = self.rule_config(ASCII_TICK_MICROSTRUCTURE_RULE_ID)
        return str(config.get("session_name") or DEFAULT_SESSION_PROFILE)

    def tick_spread_regime_thresholds(
        self,
    ) -> HistDataTickSpreadRegimeThresholds:
        """Return configured tick spread-regime thresholds."""
        config = self.rule_config(ASCII_TICK_SPREAD_REGIME_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "thresholds",
                "thresholds_by_asset_class",
                "warning_severity",
                "schema_severity",
            },
            ASCII_TICK_SPREAD_REGIME_RULE_ID,
        )
        return _tick_spread_regime_thresholds(
            _mapping_field(
                config,
                "thresholds",
                path=ASCII_TICK_SPREAD_REGIME_RULE_ID,
            ),
            base=DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
            path=f"{ASCII_TICK_SPREAD_REGIME_RULE_ID}.thresholds",
        )

    def tick_spread_regime_thresholds_by_asset_class(
        self,
    ) -> dict[str, HistDataTickSpreadRegimeThresholds]:
        """Return tick spread-regime thresholds keyed by asset class."""
        config = self.rule_config(ASCII_TICK_SPREAD_REGIME_RULE_ID)
        return _tick_spread_regime_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_asset_class",
                path=ASCII_TICK_SPREAD_REGIME_RULE_ID,
            ),
            key_normalizer=_lower_key,
            path=(
                f"{ASCII_TICK_SPREAD_REGIME_RULE_ID}.thresholds_by_asset_class"
            ),
        )

    def cross_instrument_tolerance(self) -> HistDataCrossInstrumentTolerance:
        """Return configured cross-instrument consistency tolerance."""
        config = self.rule_config(DOMAIN_CROSS_INSTRUMENT_RULE_ID)
        _reject_unknown_keys(
            config,
            {"tolerance", "warning_severity", "error_severity"},
            DOMAIN_CROSS_INSTRUMENT_RULE_ID,
        )
        return _cross_instrument_tolerance(
            _mapping_field(
                config,
                "tolerance",
                path=DOMAIN_CROSS_INSTRUMENT_RULE_ID,
            ),
            path=f"{DOMAIN_CROSS_INSTRUMENT_RULE_ID}.tolerance",
        )

    def calendar_profile(self) -> HistDataCalendarProfile:
        """Return configured calendar/session profile."""
        config = self.rule_config(DOMAIN_CALENDAR_SESSION_RULE_ID)
        _reject_unknown_keys(
            config,
            {"calendar_profile", "profile_missing_severity"},
            DOMAIN_CALENDAR_SESSION_RULE_ID,
        )
        try:
            return calendar_profile_from_mapping(
                _mapping_field(
                    config,
                    "calendar_profile",
                    path=DOMAIN_CALENDAR_SESSION_RULE_ID,
                )
            )
        except ValueError as exc:
            msg = f"{DOMAIN_CALENDAR_SESSION_RULE_ID}: {exc}"
            raise QualityProfileError(msg) from exc

    def modeling_profile_assumptions(self) -> dict[str, JSONValue]:
        """Return configured modeling-readiness assumptions."""
        config = self.rule_config(MODELING_READINESS_RULE_ID)
        _reject_unknown_keys(
            config,
            {"assumptions", "warning_severity"},
            MODELING_READINESS_RULE_ID,
        )
        assumptions = dict(self.modeling_assumptions)
        assumptions.update(
            _mapping_field(
                config,
                "assumptions",
                path=MODELING_READINESS_RULE_ID,
            )
        )
        return assumptions

    def fingerprint_profile(self) -> HistDataFingerprintProfile:
        """Return configured deterministic fingerprint controls."""
        config = self.rule_config(SERIES_FINGERPRINT_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "quantiles",
                "lags",
                "rolling_windows",
                "histogram_bins",
                "max_rows",
                "rounding_digits",
                "topology_inspection_sample_limit",
                "distribution_attention",
                "cache_source_parity",
                "classical_baselines",
                "classical_model_input",
                "exponential_smoothing",
                "autoregressive",
            },
            SERIES_FINGERPRINT_RULE_ID,
        )
        return HistDataFingerprintProfile(
            quantiles=_fingerprint_quantiles(
                config,
                "quantiles",
                DEFAULT_FINGERPRINT_QUANTILES,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            lags=_fingerprint_int_sequence(
                config,
                "lags",
                DEFAULT_FINGERPRINT_LAGS,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            rolling_windows=_fingerprint_int_sequence(
                config,
                "rolling_windows",
                DEFAULT_FINGERPRINT_ROLLING_WINDOWS,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            histogram_bins=_int_field(
                config,
                "histogram_bins",
                DEFAULT_FINGERPRINT_HISTOGRAM_BINS,
                minimum=1,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            max_rows=_int_field(
                config,
                "max_rows",
                DEFAULT_FINGERPRINT_MAX_ROWS,
                minimum=1,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            rounding_digits=_int_field(
                config,
                "rounding_digits",
                DEFAULT_FINGERPRINT_ROUNDING_DIGITS,
                minimum=0,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            topology_inspection_sample_limit=_int_field(
                config,
                "topology_inspection_sample_limit",
                DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT,
                minimum=0,
                maximum=DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT,
                path=SERIES_FINGERPRINT_RULE_ID,
            ),
            calendar_profile=self.calendar_profile(),
            distribution_attention=(
                _fingerprint_distribution_attention_profile(
                    _mapping_field(
                        config,
                        "distribution_attention",
                        path=SERIES_FINGERPRINT_RULE_ID,
                    ),
                    path=f"{SERIES_FINGERPRINT_RULE_ID}.distribution_attention",
                )
            ),
            cache_source_parity=_fingerprint_parity_profile(
                _mapping_field(
                    config,
                    "cache_source_parity",
                    path=SERIES_FINGERPRINT_RULE_ID,
                ),
                path=f"{SERIES_FINGERPRINT_RULE_ID}.cache_source_parity",
            ),
            classical_baselines=_classical_baseline_profile(
                _mapping_field(
                    config,
                    "classical_baselines",
                    path=SERIES_FINGERPRINT_RULE_ID,
                ),
                path=f"{SERIES_FINGERPRINT_RULE_ID}.classical_baselines",
            ),
            classical_model_input=_classical_model_input_profile(
                _mapping_field(
                    config,
                    "classical_model_input",
                    path=SERIES_FINGERPRINT_RULE_ID,
                ),
                path=(f"{SERIES_FINGERPRINT_RULE_ID}.classical_model_input"),
            ),
            exponential_smoothing=_exponential_smoothing_profile(
                _mapping_field(
                    config,
                    "exponential_smoothing",
                    path=SERIES_FINGERPRINT_RULE_ID,
                ),
                path=f"{SERIES_FINGERPRINT_RULE_ID}.exponential_smoothing",
            ),
            autoregressive=_autoregressive_profile(
                _mapping_field(
                    config,
                    "autoregressive",
                    path=SERIES_FINGERPRINT_RULE_ID,
                ),
                path=f"{SERIES_FINGERPRINT_RULE_ID}.autoregressive",
            ),
        )

    def reporting_profile(self) -> QualityReportingProfile:
        """Return configured quality report publication controls."""
        return _quality_reporting_profile(self.reporting)

    def to_request_payload(self) -> dict[str, JSONValue]:
        """Return a JSON-safe profile payload for runtime requests."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "name": self.name,
            "source": self.source,
            "source_path": self.source_path,
            "rules": _json_mapping(self.rules),
            "modeling_assumptions": dict(self.modeling_assumptions),
        }
        if self.reporting:
            payload["reporting"] = _json_mapping(self.reporting)
        return payload

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return report metadata describing the active quality profile."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "name": self.name,
            "source": self.source,
            "source_path": self.source_path,
            "configured_rule_ids": cast(JSONValue, sorted(self.rules)),
            "configured_modeling_assumption_keys": cast(
                JSONValue,
                sorted(str(key) for key in self.modeling_assumptions),
            ),
            "rules": _json_mapping(self.rules),
            "is_default": self.is_default,
        }
        if self.reporting:
            payload["configured_reporting_keys"] = cast(
                JSONValue,
                sorted(str(key) for key in self.reporting),
            )
            payload["reporting"] = self.reporting_profile().to_metadata()
        return payload


@dataclass(frozen=True, slots=True)
class QualityProfileValueSource:
    """Value-level provenance retained while a quality profile is resolved."""

    path: str
    value: JSONValue
    source: str
    profile_name: str = ""
    source_path: str = ""
    selected_by: str = ""
    override: bool = False
    previous_source: str = ""
    previous_value: JSONValue | None = None
    previous_value_present: bool = False

    def to_payload(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible provenance metadata."""
        payload: dict[str, JSONValue] = {
            "path": self.path,
            "value": self.value,
            "source": self.source,
        }
        if self.profile_name:
            payload["profile_name"] = self.profile_name
        if self.source_path:
            payload["source_path"] = self.source_path
        if self.selected_by:
            payload["selected_by"] = self.selected_by
        if self.override:
            payload["override"] = True
            payload["previous_source"] = self.previous_source or "unknown"
            payload["overridden_source"] = self.previous_source or "unknown"
            if self.previous_value_present:
                payload["previous_value"] = self.previous_value
        return payload


@dataclass(frozen=True, slots=True)
class QualityProfileResolution:
    """Resolved quality profile plus deterministic value provenance."""

    profile: QualityProfile
    value_sources: tuple[QualityProfileValueSource, ...]
    input_channels: tuple[dict[str, JSONValue], ...]
    schema_version: str = QUALITY_PROFILE_RESOLUTION_SCHEMA_VERSION

    def to_payload(self) -> dict[str, JSONValue]:
        """Return the resolution contract as JSON-compatible metadata."""
        return {
            "schema_version": self.schema_version,
            "resolved_profile": _expanded_quality_profile_payload(self.profile),
            "input_channels": cast(JSONValue, list(self.input_channels)),
            "effective_value_sources": cast(
                JSONValue,
                [item.to_payload() for item in self.value_sources],
            ),
        }


def default_quality_profile() -> QualityProfile:
    """Return the deterministic default profile."""
    return QualityProfile()


def resolve_quality_profile(
    payload: Mapping[str, Any] | None = None,
    *,
    source: str = OPERATOR_QUALITY_PROFILE_SOURCE,
    source_path: str = "",
    config_path: str = "",
    selected_by: str = "",
) -> QualityProfileResolution:
    """Resolve a profile while retaining source metadata for every value."""
    raw_payload = dict(payload or {})
    profile = _quality_profile_from_mapping(
        raw_payload,
        source=source,
        source_path=source_path,
    )
    source_kind = (
        "built_in_default"
        if not raw_payload
        else quality_profile_source_kind(source)
    )
    explicit_values = dict(_flatten_profile_mapping(raw_payload))
    sources: list[QualityProfileValueSource] = []
    for path, value in _flatten_profile_mapping(
        _expanded_quality_profile_payload(profile)
    ):
        value_source = _resolved_profile_value_source(
            path,
            explicit_values=explicit_values,
            source_kind=source_kind,
        )
        sources.append(
            QualityProfileValueSource(
                path=path,
                value=value,
                source=value_source,
                profile_name=(
                    profile.name
                    if value_source not in {"built_in_default", "named_profile"}
                    else ""
                ),
                source_path=(
                    source_path if value_source != "built_in_default" else ""
                ),
                selected_by=(
                    selected_by
                    if value_source not in {"built_in_default", "named_profile"}
                    else ""
                ),
            )
        )
    channels: list[dict[str, JSONValue]] = [
        {
            "kind": "built_in_default",
            "description": "Built-in quality profile defaults.",
        }
    ]
    if config_path:
        _append_profile_input_channel(
            channels,
            {"kind": "yaml_config", "path": config_path},
        )
    if _is_named_quality_profile(profile, raw_payload):
        _append_profile_input_channel(
            channels,
            {"kind": "named_profile", "profile_name": profile.name},
        )
    if raw_payload:
        channel: dict[str, JSONValue] = {
            "kind": source_kind,
            "profile_name": profile.name,
        }
        if source_path:
            channel["source_path"] = source_path
        if selected_by:
            channel["selected_by"] = selected_by
        _append_profile_input_channel(channels, channel)
    return QualityProfileResolution(
        profile=profile,
        value_sources=tuple(sorted(sources, key=lambda item: item.path)),
        input_channels=tuple(channels),
    )


def apply_quality_profile_overrides(
    resolution: QualityProfileResolution,
    overrides: Mapping[str, JSONValue],
    *,
    source: str,
    source_path: str = "",
) -> QualityProfileResolution:
    """Apply value overrides while retaining previous source and value facts."""
    if not overrides:
        return resolution
    payload = resolution.profile.to_request_payload()
    if resolution.profile.is_default:
        payload["name"] = "operator"
        payload["source"] = _quality_profile_contract_source(source)
    override_pointers: set[str] = set()
    for path, value in sorted(overrides.items()):
        pointer = _profile_pointer(path)
        override_pointers.add(pointer)
        _set_profile_pointer(payload, pointer, value)
    profile = _quality_profile_from_mapping(payload)
    before = {item.path: item for item in resolution.value_sources}
    sources: list[QualityProfileValueSource] = []
    for path, value in _flatten_profile_mapping(
        _expanded_quality_profile_payload(profile)
    ):
        previous = before.get(path)
        changed = previous is None or previous.value != value
        explicitly_overridden = path in override_pointers
        if not changed and not explicitly_overridden and previous is not None:
            sources.append(previous)
            continue
        sources.append(
            QualityProfileValueSource(
                path=path,
                value=value,
                source=source,
                profile_name=profile.name,
                source_path=source_path,
                override=True,
                previous_source=(previous.source if previous else "unknown"),
                previous_value=(previous.value if previous else None),
                previous_value_present=previous is not None,
            )
        )
    channels = [dict(channel) for channel in resolution.input_channels]
    _append_profile_input_channel(
        channels,
        {
            "kind": source,
            "paths": cast(JSONValue, sorted(override_pointers)),
        },
    )
    return QualityProfileResolution(
        profile=profile,
        value_sources=tuple(sorted(sources, key=lambda item: item.path)),
        input_channels=tuple(channels),
    )


def load_quality_profile_file(path: str | Path) -> QualityProfile:
    """Load and validate a JSON quality profile file."""
    return load_quality_profile_file_resolution(path).profile


def load_quality_profile_file_resolution(
    path: str | Path,
    *,
    config_path: str = "",
    selected_by: str = "",
) -> QualityProfileResolution:
    """Load a JSON profile while preserving file and selection provenance."""
    profile_path = Path(path).expanduser()
    try:
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
    except OSError as exc:
        msg = f"quality profile could not be read: {profile_path}"
        raise QualityProfileError(msg) from exc
    except json.JSONDecodeError as exc:
        msg = f"quality profile is not valid JSON: {profile_path}: {exc.msg}"
        raise QualityProfileError(msg) from exc
    if not isinstance(payload, Mapping):
        msg = "quality profile JSON root must be an object"
        raise QualityProfileError(msg)
    return resolve_quality_profile(
        payload,
        source="file",
        source_path=str(profile_path),
        config_path=config_path,
        selected_by=selected_by,
    )


def quality_profile_from_mapping(
    payload: Mapping[str, Any] | None,
    *,
    source: str = OPERATOR_QUALITY_PROFILE_SOURCE,
    source_path: str = "",
) -> QualityProfile:
    """Validate and return a quality profile from a mapping payload."""
    return _quality_profile_from_mapping(
        payload,
        source=source,
        source_path=source_path,
    )


def _quality_profile_from_mapping(
    payload: Mapping[str, Any] | None,
    *,
    source: str = OPERATOR_QUALITY_PROFILE_SOURCE,
    source_path: str = "",
) -> QualityProfile:
    """Construct a profile without discarding resolver input metadata."""
    if not payload:
        return default_quality_profile()
    _reject_unknown_keys(payload, _TOP_LEVEL_KEYS, "quality_profile")
    rules = _rules_mapping(payload.get("rules", {}))
    profile = QualityProfile(
        schema_version=str(
            payload.get("schema_version") or QUALITY_PROFILE_SCHEMA_VERSION
        ),
        name=str(payload.get("name") or "operator"),
        source=str(payload.get("source") or source),
        source_path=str(payload.get("source_path") or source_path),
        rules=rules,
        reporting=_mapping_field(
            payload,
            "reporting",
            path="quality_profile",
        ),
        modeling_assumptions=_mapping_field(
            payload,
            "modeling_assumptions",
            path="quality_profile",
        ),
    )
    validate_quality_profile(profile)
    return profile


def quality_profile_resolution_from_value(
    value: Mapping[str, Any] | QualityProfile | None,
) -> QualityProfileResolution:
    """Normalize a public profile value into the structured resolution form."""
    if isinstance(value, QualityProfile):
        return resolve_quality_profile(
            value.to_request_payload(),
            source=value.source,
            source_path=value.source_path,
        )
    return resolve_quality_profile(value)


def quality_profile_from_value(
    value: Mapping[str, Any] | QualityProfile | None,
) -> QualityProfile:
    """Normalize an optional public quality-profile value."""
    if value is None:
        return default_quality_profile()
    if isinstance(value, QualityProfile):
        return value
    return quality_profile_from_mapping(value)


def quality_profile_metadata(
    value: Mapping[str, Any] | QualityProfile | None,
) -> dict[str, JSONValue]:
    """Return report metadata for an optional public profile value."""
    return quality_profile_from_value(value).to_metadata()


def quality_profile_source_kind(value: str | QualityProfile) -> str:
    """Return a stable public provenance kind for a profile source."""
    source = value.source if isinstance(value, QualityProfile) else str(value)
    aliases = {
        "default": "built_in_default",
        "file": "profile_file",
        "api-options": "api_options",
        "cli-options": "cli_options",
        "operator-config": "operator_config",
        "yaml-config": "yaml_config",
        "cli-override": "cli_override",
    }
    return aliases.get(source, source.replace("-", "_") or "unknown")


def _expanded_quality_profile_payload(
    profile: QualityProfile,
) -> dict[str, JSONValue]:
    payload = profile.to_request_payload()
    if "reporting" not in payload:
        payload["reporting"] = profile.reporting_profile().to_metadata()
    return payload


def _flatten_profile_mapping(
    value: Mapping[str, Any],
    *,
    prefix: str = "",
) -> list[tuple[str, JSONValue]]:
    flattened: list[tuple[str, JSONValue]] = []
    for key in sorted(value, key=str):
        path = f"{prefix}/{_profile_pointer_token(str(key))}"
        item = value[key]
        if isinstance(item, Mapping):
            if item:
                flattened.extend(_flatten_profile_mapping(item, prefix=path))
            else:
                flattened.append((path, {}))
        else:
            flattened.append((path, cast(JSONValue, item)))
    return flattened


def _resolved_profile_value_source(
    path: str,
    *,
    explicit_values: Mapping[str, JSONValue],
    source_kind: str,
) -> str:
    if path == "/name" and path in explicit_values:
        return "named_profile"
    if path in explicit_values:
        return source_kind
    if (
        path in {"/source", "/source_path"}
        and source_kind != "built_in_default"
    ):
        return source_kind
    return "built_in_default"


def _is_named_quality_profile(
    profile: QualityProfile,
    raw_payload: Mapping[str, Any],
) -> bool:
    return "name" in raw_payload and profile.name not in {
        DEFAULT_QUALITY_PROFILE_NAME,
        "operator",
    }


def _append_profile_input_channel(
    channels: list[dict[str, JSONValue]],
    channel: Mapping[str, JSONValue],
) -> None:
    kind = str(channel.get("kind") or "")
    for existing in channels:
        if existing.get("kind") != kind:
            continue
        for key, value in channel.items():
            if key == "paths":
                current = existing.get("paths")
                current_paths = (
                    list(current) if isinstance(current, list) else []
                )
                incoming = list(value) if isinstance(value, list) else []
                existing["paths"] = cast(
                    JSONValue,
                    sorted({str(item) for item in (*current_paths, *incoming)}),
                )
            elif key != "kind":
                existing[key] = value
        return
    channels.append(dict(channel))


def _profile_pointer(path: str) -> str:
    if path.startswith("/"):
        return path
    return "/" + "/".join(
        _profile_pointer_token(part) for part in path.split(".") if part
    )


def _profile_pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _profile_pointer_token_unescape(value: str) -> str:
    return value.replace("~1", "/").replace("~0", "~")


def _set_profile_pointer(
    payload: dict[str, JSONValue],
    pointer: str,
    value: JSONValue,
) -> None:
    tokens = [
        _profile_pointer_token_unescape(token)
        for token in pointer.strip("/").split("/")
        if token
    ]
    if not tokens:
        raise QualityProfileError(
            "quality profile override path cannot be empty"
        )
    current: dict[str, JSONValue] = payload
    for token in tokens[:-1]:
        existing = current.get(token)
        if isinstance(existing, dict):
            child = existing
        else:
            child = {}
            current[token] = child
        current = child
    current[tokens[-1]] = value


def _quality_profile_contract_source(source: str) -> str:
    aliases = {
        "api_options": "api-options",
        "cli_override": "cli-options",
        "yaml_config": "yaml-config",
    }
    return aliases.get(source, source.replace("_", "-"))


def validate_quality_profile(profile: QualityProfile) -> None:
    """Eagerly validate every configured rule stanza."""
    profile.row_count_profile()
    profile.gap_tolerance(ASCII_TIMESTAMP_GAP_RULE_ID)
    profile.gap_tolerance(ASCII_TIMESTAMP_CONTINUITY_RULE_ID)
    profile.tick_spread_thresholds()
    profile.tick_spread_thresholds_by_asset_class()
    profile.tick_microstructure_thresholds()
    profile.tick_microstructure_thresholds_by_symbol()
    profile.tick_microstructure_thresholds_by_session()
    profile.tick_microstructure_thresholds_by_asset_class()
    profile.tick_microstructure_thresholds_by_symbol_session()
    profile.tick_spread_regime_thresholds()
    profile.tick_spread_regime_thresholds_by_asset_class()
    profile.cross_instrument_tolerance()
    profile.calendar_profile()
    profile.modeling_profile_assumptions()
    profile.fingerprint_profile()
    profile.reporting_profile()
    _validate_configured_severities(profile)


def _validate_configured_severities(profile: QualityProfile) -> None:
    severity_fields = {
        ASCII_ROW_COUNT_INGESTION_RULE_ID: (
            "tiny_severity",
            "size_severity",
            "truncation_severity",
        ),
        ASCII_TIMESTAMP_GAP_RULE_ID: ("warning_severity",),
        ASCII_TIMESTAMP_CONTINUITY_RULE_ID: ("warning_severity",),
        ASCII_TICK_SPREAD_RULE_ID: (
            "zero_spread_severity",
            "negative_spread_severity",
            "schema_severity",
        ),
        ASCII_TICK_MICROSTRUCTURE_RULE_ID: ("warning_severity",),
        ASCII_TICK_SPREAD_REGIME_RULE_ID: (
            "warning_severity",
            "schema_severity",
        ),
        DOMAIN_CROSS_INSTRUMENT_RULE_ID: (
            "warning_severity",
            "error_severity",
        ),
        DOMAIN_CALENDAR_SESSION_RULE_ID: ("profile_missing_severity",),
        MODELING_READINESS_RULE_ID: ("warning_severity",),
    }
    for rule_id, keys in severity_fields.items():
        for key in keys:
            profile.severity(rule_id, key, QualitySeverity.WARNING)


def _rules_mapping(value: Any) -> dict[str, Mapping[str, JSONValue]]:
    if not value:
        return {}
    if not isinstance(value, Mapping):
        msg = "quality_profile.rules must be an object"
        raise QualityProfileError(msg)
    rules: dict[str, Mapping[str, JSONValue]] = {}
    for key, config in value.items():
        rule_id = str(key)
        if not isinstance(config, Mapping):
            msg = f"{rule_id}: rule config must be an object"
            raise QualityProfileError(msg)
        rules[rule_id] = _json_mapping(config)
    return rules


def _tick_spread_threshold_mapping(
    value: Mapping[str, JSONValue],
    *,
    key_normalizer: Any,
    path: str,
) -> dict[str, HistDataTickSpreadThresholds]:
    result: dict[str, HistDataTickSpreadThresholds] = {}
    for key, config in value.items():
        profile_key = str(key_normalizer(str(key)))
        if not profile_key:
            continue
        result[profile_key] = _tick_spread_thresholds(
            _expect_mapping(config, path=f"{path}.{key}"),
            base=DEFAULT_TICK_SPREAD_THRESHOLDS,
            path=f"{path}.{key}",
        )
    return result


def _tick_microstructure_threshold_mapping(
    value: Mapping[str, JSONValue],
    *,
    key_normalizer: Any,
    path: str,
) -> dict[str, HistDataTickMicrostructureThresholds]:
    result: dict[str, HistDataTickMicrostructureThresholds] = {}
    for key, config in value.items():
        profile_key = str(key_normalizer(str(key)))
        if not profile_key:
            continue
        result[profile_key] = _tick_microstructure_thresholds(
            _expect_mapping(config, path=f"{path}.{key}"),
            base=DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
            path=f"{path}.{key}",
        )
    return result


def _tick_spread_regime_threshold_mapping(
    value: Mapping[str, JSONValue],
    *,
    key_normalizer: Any,
    path: str,
) -> dict[str, HistDataTickSpreadRegimeThresholds]:
    result: dict[str, HistDataTickSpreadRegimeThresholds] = {}
    for key, config in value.items():
        profile_key = str(key_normalizer(str(key)))
        if not profile_key:
            continue
        result[profile_key] = _tick_spread_regime_thresholds(
            _expect_mapping(config, path=f"{path}.{key}"),
            base=DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
            path=f"{path}.{key}",
        )
    return result


def _precision_rule_mapping(
    value: Mapping[str, JSONValue],
    *,
    key_normalizer: Any,
    path: str,
) -> dict[str, HistDataSymbolPrecisionRule]:
    result: dict[str, HistDataSymbolPrecisionRule] = {}
    for key, config in value.items():
        profile_key = str(key_normalizer(str(key)))
        if not profile_key:
            continue
        result[profile_key] = _precision_rule(
            _expect_mapping(config, path=f"{path}.{key}"),
            path=f"{path}.{key}",
        )
    return result


def _tick_spread_thresholds(
    value: Mapping[str, JSONValue],
    *,
    base: HistDataTickSpreadThresholds,
    path: str,
) -> HistDataTickSpreadThresholds:
    _reject_unknown_keys(
        value,
        {"zero_spread_run_length"},
        path,
    )
    return HistDataTickSpreadThresholds(
        zero_spread_run_length=_int_field(
            value,
            "zero_spread_run_length",
            base.zero_spread_run_length,
            minimum=1,
            path=path,
        )
    )


def _tick_microstructure_thresholds(
    value: Mapping[str, JSONValue],
    *,
    base: HistDataTickMicrostructureThresholds,
    path: str,
) -> HistDataTickMicrostructureThresholds:
    _reject_unknown_keys(
        value,
        {
            "stale_quote_run_length",
            "stale_max_gap_ms",
            "burst_max_interval_ms",
            "burst_run_length",
            "one_sided_run_length",
        },
        path,
    )
    return HistDataTickMicrostructureThresholds(
        stale_quote_run_length=_int_field(
            value,
            "stale_quote_run_length",
            base.stale_quote_run_length,
            minimum=2,
            path=path,
        ),
        stale_max_gap_ms=_int_field(
            value,
            "stale_max_gap_ms",
            base.stale_max_gap_ms,
            minimum=0,
            path=path,
        ),
        burst_max_interval_ms=_int_field(
            value,
            "burst_max_interval_ms",
            base.burst_max_interval_ms,
            minimum=0,
            path=path,
        ),
        burst_run_length=_int_field(
            value,
            "burst_run_length",
            base.burst_run_length,
            minimum=2,
            path=path,
        ),
        one_sided_run_length=_int_field(
            value,
            "one_sided_run_length",
            base.one_sided_run_length,
            minimum=1,
            path=path,
        ),
    )


def _tick_spread_regime_thresholds(
    value: Mapping[str, JSONValue],
    *,
    base: HistDataTickSpreadRegimeThresholds,
    path: str,
) -> HistDataTickSpreadRegimeThresholds:
    _reject_unknown_keys(
        value,
        {
            "wide_spread_multiplier",
            "jump_spread_multiplier",
            "regime_median_multiplier",
            "minimum_wide_spread",
            "minimum_spread_jump",
        },
        path,
    )
    return HistDataTickSpreadRegimeThresholds(
        wide_spread_multiplier=_float_field(
            value,
            "wide_spread_multiplier",
            base.wide_spread_multiplier,
            minimum=1.0,
            minimum_exclusive=True,
            path=path,
        ),
        jump_spread_multiplier=_float_field(
            value,
            "jump_spread_multiplier",
            base.jump_spread_multiplier,
            minimum=0.0,
            minimum_exclusive=True,
            path=path,
        ),
        regime_median_multiplier=_float_field(
            value,
            "regime_median_multiplier",
            base.regime_median_multiplier,
            minimum=1.0,
            minimum_exclusive=True,
            path=path,
        ),
        minimum_wide_spread=_float_field(
            value,
            "minimum_wide_spread",
            base.minimum_wide_spread,
            minimum=0.0,
            path=path,
        ),
        minimum_spread_jump=_float_field(
            value,
            "minimum_spread_jump",
            base.minimum_spread_jump,
            minimum=0.0,
            path=path,
        ),
    )


def _gap_tolerance(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> HistDataGapTolerance:
    base = HistDataGapTolerance()
    _reject_unknown_keys(
        value,
        {
            "expected_interval_ms",
            "suspicious_gap_ms",
            "bucket_thresholds_ms",
            "session_boundary_grace_ms",
            "dynamic_window_initial_ms",
            "dynamic_window_max_ms",
            "dynamic_window_growth_factor",
            "dynamic_window_shrink_factor",
        },
        path,
    )
    tolerance = HistDataGapTolerance(
        expected_interval_ms=_int_field(
            value,
            "expected_interval_ms",
            base.expected_interval_ms,
            minimum=1,
            path=path,
        ),
        suspicious_gap_ms=_int_field(
            value,
            "suspicious_gap_ms",
            base.suspicious_gap_ms,
            minimum=1,
            path=path,
        ),
        bucket_thresholds_ms=_int_tuple_field(
            value,
            "bucket_thresholds_ms",
            base.bucket_thresholds_ms,
            minimum=1,
            path=path,
        ),
        session_boundary_grace_ms=_int_field(
            value,
            "session_boundary_grace_ms",
            base.session_boundary_grace_ms,
            minimum=0,
            path=path,
        ),
        dynamic_window_initial_ms=_int_field(
            value,
            "dynamic_window_initial_ms",
            base.dynamic_window_initial_ms,
            minimum=1,
            path=path,
        ),
        dynamic_window_max_ms=_int_field(
            value,
            "dynamic_window_max_ms",
            base.dynamic_window_max_ms,
            minimum=1,
            path=path,
        ),
        dynamic_window_growth_factor=_float_field(
            value,
            "dynamic_window_growth_factor",
            base.dynamic_window_growth_factor,
            minimum=1.0,
            minimum_exclusive=True,
            path=path,
        ),
        dynamic_window_shrink_factor=_float_field(
            value,
            "dynamic_window_shrink_factor",
            base.dynamic_window_shrink_factor,
            minimum=0.0,
            minimum_exclusive=True,
            path=path,
        ),
    )
    if tolerance.dynamic_window_initial_ms > tolerance.dynamic_window_max_ms:
        msg = (
            f"{path}.dynamic_window_initial_ms must be <= dynamic_window_max_ms"
        )
        raise QualityProfileError(msg)
    return tolerance


def _cross_instrument_tolerance(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> HistDataCrossInstrumentTolerance:
    base = DEFAULT_CROSS_INSTRUMENT_TOLERANCE
    _reject_unknown_keys(
        value,
        {
            "triangular_warning_relative_tolerance",
            "triangular_error_relative_tolerance",
            "inverse_warning_relative_tolerance",
            "inverse_error_relative_tolerance",
            "minimum_common_timestamp_ratio",
            "stale_forward_fill_min_run",
        },
        path,
    )
    return HistDataCrossInstrumentTolerance(
        triangular_warning_relative_tolerance=_float_field(
            value,
            "triangular_warning_relative_tolerance",
            base.triangular_warning_relative_tolerance,
            minimum=0.0,
            path=path,
        ),
        triangular_error_relative_tolerance=_float_field(
            value,
            "triangular_error_relative_tolerance",
            base.triangular_error_relative_tolerance,
            minimum=0.0,
            path=path,
        ),
        inverse_warning_relative_tolerance=_float_field(
            value,
            "inverse_warning_relative_tolerance",
            base.inverse_warning_relative_tolerance,
            minimum=0.0,
            path=path,
        ),
        inverse_error_relative_tolerance=_float_field(
            value,
            "inverse_error_relative_tolerance",
            base.inverse_error_relative_tolerance,
            minimum=0.0,
            path=path,
        ),
        minimum_common_timestamp_ratio=_float_field(
            value,
            "minimum_common_timestamp_ratio",
            base.minimum_common_timestamp_ratio,
            minimum=0.0,
            maximum=1.0,
            path=path,
        ),
        stale_forward_fill_min_run=_int_field(
            value,
            "stale_forward_fill_min_run",
            base.stale_forward_fill_min_run,
            minimum=1,
            path=path,
        ),
    )


def _precision_rule(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> HistDataSymbolPrecisionRule:
    _reject_unknown_keys(
        value,
        {
            "name",
            "expected_decimal_places",
            "pip_size",
            "tick_size",
            "quote_side",
        },
        path,
    )
    name = str(value.get("name") or "operator_precision_rule")
    expected = _int_tuple_field(
        value,
        "expected_decimal_places",
        (),
        minimum=0,
        path=path,
    )
    if not expected:
        msg = f"{path}.expected_decimal_places must not be empty"
        raise QualityProfileError(msg)
    return HistDataSymbolPrecisionRule(
        name=name,
        expected_decimal_places=expected,
        pip_size=str(value.get("pip_size") or ""),
        tick_size=str(value.get("tick_size") or ""),
        quote_side=str(value.get("quote_side") or "bid"),
    )


def _fingerprint_distribution_attention_profile(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> HistDataFingerprintDistributionAttentionProfile:
    base = HistDataFingerprintDistributionAttentionProfile()
    _reject_unknown_keys(
        value,
        {
            "invalid_row_min_count",
            "invalid_row_min_rate",
            "zero_spread_min_count",
            "zero_spread_min_rate",
            "negative_spread_min_count",
            "negative_spread_min_rate",
            "flag_truncated_distribution",
            "flag_cache_float_precision",
        },
        path,
    )
    return HistDataFingerprintDistributionAttentionProfile(
        invalid_row_min_count=_int_field(
            value,
            "invalid_row_min_count",
            base.invalid_row_min_count,
            minimum=1,
            path=path,
        ),
        invalid_row_min_rate=_float_field(
            value,
            "invalid_row_min_rate",
            base.invalid_row_min_rate,
            minimum=0.0,
            maximum=1.0,
            path=path,
        ),
        zero_spread_min_count=_int_field(
            value,
            "zero_spread_min_count",
            base.zero_spread_min_count,
            minimum=1,
            path=path,
        ),
        zero_spread_min_rate=_float_field(
            value,
            "zero_spread_min_rate",
            base.zero_spread_min_rate,
            minimum=0.0,
            maximum=1.0,
            path=path,
        ),
        negative_spread_min_count=_int_field(
            value,
            "negative_spread_min_count",
            base.negative_spread_min_count,
            minimum=1,
            path=path,
        ),
        negative_spread_min_rate=_float_field(
            value,
            "negative_spread_min_rate",
            base.negative_spread_min_rate,
            minimum=0.0,
            maximum=1.0,
            path=path,
        ),
        flag_truncated_distribution=_bool_field(
            value,
            "flag_truncated_distribution",
            base.flag_truncated_distribution,
            path=path,
        ),
        flag_cache_float_precision=_bool_field(
            value,
            "flag_cache_float_precision",
            base.flag_cache_float_precision,
            path=path,
        ),
    )


def _fingerprint_parity_profile(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> HistDataFingerprintParityProfile:
    _reject_unknown_keys(value, {"enabled", "mismatch_limit"}, path)
    return HistDataFingerprintParityProfile(
        enabled=_bool_field(value, "enabled", False, path=path),
        mismatch_limit=_int_field(
            value,
            "mismatch_limit",
            DEFAULT_FINGERPRINT_PARITY_MISMATCH_LIMIT,
            minimum=0,
            path=path,
        ),
    )


def _classical_baseline_profile(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> ClassicalBaselineProfile:
    base = ClassicalBaselineProfile()
    _reject_unknown_keys(
        value,
        {
            "enabled",
            "evaluation_fraction",
            "minimum_training_rows",
            "minimum_evaluation_rows",
            "rolling_windows",
            "session_seasonal_enabled",
            "rounding_digits",
        },
        path,
    )
    rolling_windows = _fingerprint_int_sequence(
        value,
        "rolling_windows",
        base.rolling_windows,
        path=path,
    )
    if len(rolling_windows) > MAX_BASELINE_ROLLING_WINDOWS:
        raise QualityProfileError(
            f"{path}.rolling_windows supports at most "
            f"{MAX_BASELINE_ROLLING_WINDOWS} values"
        )
    return ClassicalBaselineProfile(
        enabled=_bool_field(value, "enabled", base.enabled, path=path),
        evaluation_fraction=_float_field(
            value,
            "evaluation_fraction",
            base.evaluation_fraction,
            minimum=0.01,
            maximum=0.99,
            path=path,
        ),
        minimum_training_rows=_int_field(
            value,
            "minimum_training_rows",
            base.minimum_training_rows,
            minimum=1,
            path=path,
        ),
        minimum_evaluation_rows=_int_field(
            value,
            "minimum_evaluation_rows",
            base.minimum_evaluation_rows,
            minimum=1,
            path=path,
        ),
        rolling_windows=rolling_windows,
        session_seasonal_enabled=_bool_field(
            value,
            "session_seasonal_enabled",
            base.session_seasonal_enabled,
            path=path,
        ),
        rounding_digits=_int_field(
            value,
            "rounding_digits",
            base.rounding_digits,
            minimum=0,
            maximum=16,
            path=path,
        ),
    )


def _classical_model_input_profile(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> ClassicalModelInputProfile:
    base = ClassicalModelInputProfile()
    _reject_unknown_keys(
        value,
        {
            "enabled",
            "frequency_ms",
            "alignment_epoch_ms",
            "closed_side",
            "label_side",
            "midpoint_aggregation",
            "spread_aggregation",
            "minimum_observations_per_bin",
            "expected_closure_policy",
            "unexpected_missing_policy",
            "transform",
            "differencing_order",
            "seasonal_differencing_order",
            "seasonal_period",
            "horizons",
            "fold_kind",
            "minimum_training_observations",
            "minimum_evaluation_observations",
            "step_size",
            "rolling_window",
            "embargo_observations",
            "rounding_digits",
            "resources",
        },
        path,
    )
    resources = _classical_model_resource_policy(
        _mapping_field(value, "resources", path=path),
        path=f"{path}.resources",
    )
    try:
        return ClassicalModelInputProfile(
            enabled=_bool_field(value, "enabled", base.enabled, path=path),
            frequency_ms=_int_field(
                value,
                "frequency_ms",
                base.frequency_ms,
                minimum=1,
                path=path,
            ),
            alignment_epoch_ms=_int_field(
                value,
                "alignment_epoch_ms",
                base.alignment_epoch_ms,
                path=path,
            ),
            closed_side=_string_field(
                value, "closed_side", base.closed_side, path=path
            ),
            label_side=_string_field(
                value, "label_side", base.label_side, path=path
            ),
            midpoint_aggregation=_string_field(
                value,
                "midpoint_aggregation",
                base.midpoint_aggregation,
                path=path,
            ),
            spread_aggregation=_string_field(
                value,
                "spread_aggregation",
                base.spread_aggregation,
                path=path,
            ),
            minimum_observations_per_bin=_int_field(
                value,
                "minimum_observations_per_bin",
                base.minimum_observations_per_bin,
                minimum=1,
                path=path,
            ),
            expected_closure_policy=_string_field(
                value,
                "expected_closure_policy",
                base.expected_closure_policy,
                path=path,
            ),
            unexpected_missing_policy=_string_field(
                value,
                "unexpected_missing_policy",
                base.unexpected_missing_policy,
                path=path,
            ),
            transform=_string_field(
                value, "transform", base.transform, path=path
            ),
            differencing_order=_int_field(
                value,
                "differencing_order",
                base.differencing_order,
                minimum=0,
                maximum=2,
                path=path,
            ),
            seasonal_differencing_order=_int_field(
                value,
                "seasonal_differencing_order",
                base.seasonal_differencing_order,
                minimum=0,
                maximum=1,
                path=path,
            ),
            seasonal_period=_int_field(
                value,
                "seasonal_period",
                base.seasonal_period,
                minimum=0,
                path=path,
            ),
            horizons=_fingerprint_int_sequence(
                value,
                "horizons",
                base.horizons,
                path=path,
            ),
            fold_kind=_string_field(
                value, "fold_kind", base.fold_kind, path=path
            ),
            minimum_training_observations=_int_field(
                value,
                "minimum_training_observations",
                base.minimum_training_observations,
                minimum=1,
                path=path,
            ),
            minimum_evaluation_observations=_int_field(
                value,
                "minimum_evaluation_observations",
                base.minimum_evaluation_observations,
                minimum=1,
                path=path,
            ),
            step_size=_int_field(
                value,
                "step_size",
                base.step_size,
                minimum=1,
                path=path,
            ),
            rolling_window=_int_field(
                value,
                "rolling_window",
                base.rolling_window,
                minimum=0,
                path=path,
            ),
            embargo_observations=_int_field(
                value,
                "embargo_observations",
                base.embargo_observations,
                minimum=0,
                path=path,
            ),
            rounding_digits=_int_field(
                value,
                "rounding_digits",
                base.rounding_digits,
                minimum=0,
                maximum=16,
                path=path,
            ),
            resources=resources,
        )
    except ValueError as exc:
        raise QualityProfileError(f"{path}: {exc}") from exc


def _classical_model_resource_policy(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> ClassicalModelResourcePolicy:
    base = ClassicalModelResourcePolicy()
    keys = set(base.to_metadata())
    _reject_unknown_keys(value, keys, path)
    try:
        return ClassicalModelResourcePolicy(
            **{
                key: _int_field(
                    value,
                    key,
                    int(getattr(base, key)),
                    minimum=1,
                    path=path,
                )
                for key in keys
            }
        )
    except ValueError as exc:
        raise QualityProfileError(f"{path}: {exc}") from exc


def _exponential_smoothing_profile(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> ExponentialSmoothingProfile:
    base = ExponentialSmoothingProfile()
    _reject_unknown_keys(
        value,
        {
            "enabled",
            "specifications",
            "projection_specification_id",
            "projection_horizon",
            "baseline_rolling_windows",
            "rounding_digits",
        },
        path,
    )
    specifications = base.specifications
    if "specifications" in value:
        raw = value["specifications"]
        if not isinstance(raw, list) or not raw:
            raise QualityProfileError(
                f"{path}.specifications must be a non-empty list"
            )
        parsed: list[ExponentialSmoothingSpecification] = []
        for index, item in enumerate(raw):
            parsed.append(
                _exponential_smoothing_specification(
                    _expect_mapping(
                        item, path=f"{path}.specifications[{index}]"
                    ),
                    path=f"{path}.specifications[{index}]",
                )
            )
        specifications = tuple(parsed)
    try:
        return ExponentialSmoothingProfile(
            enabled=_bool_field(value, "enabled", base.enabled, path=path),
            specifications=specifications,
            projection_specification_id=_string_field(
                value,
                "projection_specification_id",
                (
                    specifications[0].specification_id
                    if "projection_specification_id" not in value
                    else base.projection_specification_id
                ),
                path=path,
            ),
            projection_horizon=_int_field(
                value,
                "projection_horizon",
                base.projection_horizon,
                minimum=1,
                path=path,
            ),
            baseline_rolling_windows=_fingerprint_int_sequence(
                value,
                "baseline_rolling_windows",
                base.baseline_rolling_windows,
                path=path,
            ),
            rounding_digits=_int_field(
                value,
                "rounding_digits",
                base.rounding_digits,
                minimum=0,
                maximum=16,
                path=path,
            ),
        )
    except ValueError as exc:
        raise QualityProfileError(f"{path}: {exc}") from exc


def _autoregressive_profile(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> AutoregressiveProfile:
    base = AutoregressiveProfile()
    _reject_unknown_keys(
        value,
        {
            "enabled",
            "specifications",
            "projection_specification_ids",
            "projection_horizon",
            "baseline_rolling_windows",
            "compare_exponential_smoothing",
            "rounding_digits",
        },
        path,
    )
    specifications = base.specifications
    if "specifications" in value:
        raw = value["specifications"]
        if not isinstance(raw, list) or not raw:
            raise QualityProfileError(
                f"{path}.specifications must be a non-empty list"
            )
        specifications = tuple(
            _autoregressive_specification(
                _expect_mapping(item, path=f"{path}.specifications[{index}]"),
                path=f"{path}.specifications[{index}]",
            )
            for index, item in enumerate(raw)
        )
    projection_ids = base.projection_specification_ids
    if "projection_specification_ids" in value:
        raw_ids = value["projection_specification_ids"]
        if not isinstance(raw_ids, list) or not raw_ids:
            raise QualityProfileError(
                f"{path}.projection_specification_ids must be a non-empty list"
            )
        if not all(isinstance(item, str) and item for item in raw_ids):
            raise QualityProfileError(
                f"{path}.projection_specification_ids must contain strings"
            )
        projection_ids = tuple(cast(list[str], raw_ids))
    elif "specifications" in value:
        by_family: dict[str, str] = {}
        for specification in specifications:
            by_family.setdefault(
                specification.family, specification.specification_id
            )
        projection_ids = tuple(by_family.values())
    try:
        return AutoregressiveProfile(
            enabled=_bool_field(value, "enabled", base.enabled, path=path),
            specifications=specifications,
            projection_specification_ids=projection_ids,
            projection_horizon=_int_field(
                value,
                "projection_horizon",
                base.projection_horizon,
                minimum=1,
                path=path,
            ),
            baseline_rolling_windows=_fingerprint_int_sequence(
                value,
                "baseline_rolling_windows",
                base.baseline_rolling_windows,
                path=path,
            ),
            compare_exponential_smoothing=_bool_field(
                value,
                "compare_exponential_smoothing",
                base.compare_exponential_smoothing,
                path=path,
            ),
            rounding_digits=_int_field(
                value,
                "rounding_digits",
                base.rounding_digits,
                minimum=0,
                maximum=16,
                path=path,
            ),
        )
    except ValueError as exc:
        raise QualityProfileError(f"{path}: {exc}") from exc


def _autoregressive_specification(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> AutoregressiveSpecification:
    _reject_unknown_keys(
        value,
        {
            "specification_id",
            "family",
            "p",
            "d",
            "q",
            "trend",
            "initialization_method",
            "estimation_method",
            "enforce_stationarity",
            "enforce_invertibility",
            "concentrate_scale",
            "fixed_parameters",
            "max_iterations",
        },
        path,
    )
    if (
        "specification_id" not in value
        or "family" not in value
        or "p" not in value
    ):
        raise QualityProfileError(
            f"{path} requires specification_id, family, and p"
        )
    family = _string_field(value, "family", "", path=path)
    fixed_parameters: tuple[tuple[str, float], ...] = ()
    if "fixed_parameters" in value:
        raw_fixed = _expect_mapping(
            value["fixed_parameters"], path=f"{path}.fixed_parameters"
        )
        parsed_fixed: list[tuple[str, float]] = []
        for name, raw_value in sorted(raw_fixed.items()):
            if isinstance(raw_value, bool):
                raise QualityProfileError(
                    f"{path}.fixed_parameters.{name} must be a number"
                )
            try:
                parsed_fixed.append((name, float(cast(Any, raw_value))))
            except (TypeError, ValueError) as exc:
                raise QualityProfileError(
                    f"{path}.fixed_parameters.{name} must be a number"
                ) from exc
        fixed_parameters = tuple(parsed_fixed)
    try:
        return AutoregressiveSpecification(
            specification_id=_string_field(
                value, "specification_id", "", path=path
            ),
            family=family,
            p=_int_field(value, "p", 0, minimum=0, path=path),
            d=_int_field(value, "d", 0, minimum=0, path=path),
            q=_int_field(value, "q", 0, minimum=0, path=path),
            trend=_string_field(value, "trend", "n", path=path),
            initialization_method=_string_field(
                value, "initialization_method", "default", path=path
            ),
            estimation_method=_string_field(
                value, "estimation_method", "statespace", path=path
            ),
            enforce_stationarity=_bool_field(
                value, "enforce_stationarity", True, path=path
            ),
            enforce_invertibility=_bool_field(
                value, "enforce_invertibility", True, path=path
            ),
            concentrate_scale=_bool_field(
                value, "concentrate_scale", False, path=path
            ),
            fixed_parameters=fixed_parameters,
            max_iterations=_int_field(
                value, "max_iterations", 200, minimum=1, path=path
            ),
        )
    except ValueError as exc:
        raise QualityProfileError(f"{path}: {exc}") from exc


def _exponential_smoothing_specification(
    value: Mapping[str, JSONValue],
    *,
    path: str,
) -> ExponentialSmoothingSpecification:
    base = ExponentialSmoothingSpecification()
    _reject_unknown_keys(
        value,
        {
            "specification_id",
            "family",
            "level",
            "error",
            "trend",
            "damped_trend",
            "seasonal",
            "seasonal_periods",
            "initialization_method",
            "initial_level",
            "initial_trend",
            "initial_seasonal",
            "optimized",
            "method",
            "use_brute",
            "remove_bias",
            "smoothing_level",
            "smoothing_trend",
            "smoothing_seasonal",
            "damping_trend",
            "parameter_bounds",
            "max_iterations",
        },
        path,
    )
    try:
        return ExponentialSmoothingSpecification(
            specification_id=_string_field(
                value,
                "specification_id",
                base.specification_id,
                path=path,
            ),
            family=_string_field(value, "family", base.family, path=path),
            level=_bool_field(value, "level", base.level, path=path),
            error=_string_field(value, "error", base.error, path=path),
            trend=_string_field(value, "trend", base.trend, path=path),
            damped_trend=_bool_field(
                value, "damped_trend", base.damped_trend, path=path
            ),
            seasonal=_string_field(value, "seasonal", base.seasonal, path=path),
            seasonal_periods=_int_field(
                value,
                "seasonal_periods",
                base.seasonal_periods,
                minimum=0,
                path=path,
            ),
            initialization_method=_string_field(
                value,
                "initialization_method",
                base.initialization_method,
                path=path,
            ),
            initial_level=_optional_float_profile_field(
                value, "initial_level", base.initial_level, path=path
            ),
            initial_trend=_optional_float_profile_field(
                value, "initial_trend", base.initial_trend, path=path
            ),
            initial_seasonal=_float_tuple_profile_field(
                value,
                "initial_seasonal",
                base.initial_seasonal,
                path=path,
            ),
            optimized=_bool_field(
                value, "optimized", base.optimized, path=path
            ),
            method=_optional_string_profile_field(
                value, "method", base.method, path=path
            ),
            use_brute=_bool_field(
                value, "use_brute", base.use_brute, path=path
            ),
            remove_bias=_bool_field(
                value, "remove_bias", base.remove_bias, path=path
            ),
            smoothing_level=_optional_float_profile_field(
                value,
                "smoothing_level",
                base.smoothing_level,
                minimum=0.0,
                maximum=1.0,
                path=path,
            ),
            smoothing_trend=_optional_float_profile_field(
                value,
                "smoothing_trend",
                base.smoothing_trend,
                minimum=0.0,
                maximum=1.0,
                path=path,
            ),
            smoothing_seasonal=_optional_float_profile_field(
                value,
                "smoothing_seasonal",
                base.smoothing_seasonal,
                minimum=0.0,
                maximum=1.0,
                path=path,
            ),
            damping_trend=_optional_float_profile_field(
                value,
                "damping_trend",
                base.damping_trend,
                minimum=0.0,
                maximum=1.0,
                path=path,
            ),
            parameter_bounds=_parameter_bounds_profile_field(
                value, "parameter_bounds", path=path
            ),
            max_iterations=_int_field(
                value,
                "max_iterations",
                base.max_iterations,
                minimum=1,
                path=path,
            ),
        )
    except ValueError as exc:
        raise QualityProfileError(f"{path}: {exc}") from exc


def _optional_float_profile_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: float | None,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    path: str,
) -> float | None:
    if key not in mapping or mapping[key] is None:
        return default
    return _float_field(
        mapping,
        key,
        0.0,
        minimum=minimum,
        maximum=maximum,
        path=path,
    )


def _float_tuple_profile_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: tuple[float, ...],
    *,
    path: str,
) -> tuple[float, ...]:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, list):
        raise QualityProfileError(f"{path}.{key} must be a list of numbers")
    parsed: list[float] = []
    for index, item in enumerate(value):
        if isinstance(item, bool):
            raise QualityProfileError(f"{path}.{key}[{index}] must be a number")
        try:
            parsed.append(float(item))  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise QualityProfileError(
                f"{path}.{key}[{index}] must be a number"
            ) from exc
    return tuple(parsed)


def _optional_string_profile_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: str,
    *,
    path: str,
) -> str:
    if key not in mapping or mapping[key] in (None, ""):
        return default
    return _string_field(mapping, key, default, path=path)


def _parameter_bounds_profile_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    *,
    path: str,
) -> tuple[tuple[str, float, float], ...]:
    if key not in mapping:
        return ()
    value = mapping[key]
    if not isinstance(value, list):
        raise QualityProfileError(f"{path}.{key} must be a list of objects")
    parsed: list[tuple[str, float, float]] = []
    for index, item in enumerate(value):
        bound_path = f"{path}.{key}[{index}]"
        bound = _expect_mapping(item, path=bound_path)
        _reject_unknown_keys(bound, {"parameter", "lower", "upper"}, bound_path)
        parameter = _string_field(bound, "parameter", "", path=bound_path)
        lower = _float_field(bound, "lower", 0.0, path=bound_path)
        upper = _float_field(bound, "upper", 0.0, path=bound_path)
        parsed.append((parameter, lower, upper))
    return tuple(parsed)


def _quality_reporting_profile(
    value: Mapping[str, JSONValue],
) -> QualityReportingProfile:
    _reject_unknown_keys(
        value,
        {"remediation_catalog_audit"},
        "quality_profile.reporting",
    )
    remediation_catalog_audit = _mapping_field(
        value,
        "remediation_catalog_audit",
        path="quality_profile.reporting",
    )
    _reject_unknown_keys(
        remediation_catalog_audit,
        {"enabled"},
        "quality_profile.reporting.remediation_catalog_audit",
    )
    return QualityReportingProfile(
        remediation_catalog_audit=QualityRemediationCatalogAuditProfile(
            enabled=_bool_field(
                remediation_catalog_audit,
                "enabled",
                False,
                path="quality_profile.reporting.remediation_catalog_audit",
            )
        )
    )


def _mapping_field(
    mapping: Mapping[str, Any],
    key: str,
    *,
    path: str,
) -> dict[str, JSONValue]:
    value = mapping.get(key, {})
    if value in (None, ""):
        return {}
    return _expect_mapping(value, path=f"{path}.{key}")


def _expect_mapping(value: Any, *, path: str) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        msg = f"{path} must be an object"
        raise QualityProfileError(msg)
    return _json_mapping(value)


def _json_mapping(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    result: dict[str, JSONValue] = {}
    for key, item in value.items():
        result[str(key)] = _json_value(item)
    return result


def _json_value(value: Any) -> JSONValue:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return _json_mapping(value)
    if isinstance(value, tuple | list):
        return [_json_value(item) for item in value]
    return str(value)


def _int_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
    path: str,
) -> int:
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, bool):
        msg = f"{path}.{key} must be an integer"
        raise QualityProfileError(msg)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        msg = f"{path}.{key} must be an integer"
        raise QualityProfileError(msg) from exc
    if minimum is not None and parsed < minimum:
        msg = f"{path}.{key} must be >= {minimum}"
        raise QualityProfileError(msg)
    if maximum is not None and parsed > maximum:
        msg = f"{path}.{key} must be <= {maximum}"
        raise QualityProfileError(msg)
    return parsed


def _int_tuple_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: tuple[int, ...],
    *,
    minimum: int | None = None,
    path: str,
) -> tuple[int, ...]:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, list):
        msg = f"{path}.{key} must be a list of integers"
        raise QualityProfileError(msg)
    parsed: list[int] = []
    for index, item in enumerate(value):
        if isinstance(item, bool):
            msg = f"{path}.{key}[{index}] must be an integer"
            raise QualityProfileError(msg)
        try:
            int_item = int(item)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            msg = f"{path}.{key}[{index}] must be an integer"
            raise QualityProfileError(msg) from exc
        if minimum is not None and int_item < minimum:
            msg = f"{path}.{key}[{index}] must be >= {minimum}"
            raise QualityProfileError(msg)
        parsed.append(int_item)
    return tuple(parsed)


def _fingerprint_int_sequence(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: tuple[int, ...],
    *,
    path: str,
) -> tuple[int, ...]:
    values = _int_tuple_field(mapping, key, default, minimum=1, path=path)
    if key not in mapping:
        return values
    if not values:
        msg = f"{path}.{key} must not be empty"
        raise QualityProfileError(msg)
    if tuple(sorted(values)) != values or len(set(values)) != len(values):
        msg = f"{path}.{key} must be a strictly increasing list"
        raise QualityProfileError(msg)
    return values


def _fingerprint_quantiles(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: tuple[float, ...],
    *,
    path: str,
) -> tuple[float, ...]:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, list):
        msg = f"{path}.{key} must be a list of numbers"
        raise QualityProfileError(msg)
    parsed: list[float] = []
    for index, item in enumerate(value):
        if isinstance(item, bool):
            msg = f"{path}.{key}[{index}] must be a number"
            raise QualityProfileError(msg)
        try:
            float_item = float(item)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            msg = f"{path}.{key}[{index}] must be a number"
            raise QualityProfileError(msg) from exc
        if float_item <= 0.0 or float_item >= 1.0:
            msg = f"{path}.{key}[{index}] must be > 0.0 and < 1.0"
            raise QualityProfileError(msg)
        parsed.append(float_item)
    quantiles = tuple(parsed)
    if not quantiles:
        msg = f"{path}.{key} must not be empty"
        raise QualityProfileError(msg)
    if tuple(sorted(quantiles)) != quantiles or len(set(quantiles)) != len(
        quantiles
    ):
        msg = f"{path}.{key} must be a strictly increasing list"
        raise QualityProfileError(msg)
    return quantiles


def _float_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    minimum_exclusive: bool = False,
    path: str,
) -> float:
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, bool):
        msg = f"{path}.{key} must be a number"
        raise QualityProfileError(msg)
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        msg = f"{path}.{key} must be a number"
        raise QualityProfileError(msg) from exc
    if minimum is not None:
        invalid = parsed <= minimum if minimum_exclusive else parsed < minimum
        if invalid:
            op = ">" if minimum_exclusive else ">="
            msg = f"{path}.{key} must be {op} {minimum}"
            raise QualityProfileError(msg)
    if maximum is not None and parsed > maximum:
        msg = f"{path}.{key} must be <= {maximum}"
        raise QualityProfileError(msg)
    return parsed


def _bool_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: bool,
    *,
    path: str,
) -> bool:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, bool):
        msg = f"{path}.{key} must be a boolean"
        raise QualityProfileError(msg)
    return value


def _string_field(
    mapping: Mapping[str, JSONValue],
    key: str,
    default: str,
    *,
    path: str,
) -> str:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, str) or not value.strip():
        msg = f"{path}.{key} must be a non-empty string"
        raise QualityProfileError(msg)
    return value.strip()


def _reject_unknown_keys(
    mapping: Mapping[str, Any],
    allowed: set[str] | frozenset[str],
    path: str,
) -> None:
    unknown = sorted(str(key) for key in mapping if str(key) not in allowed)
    if unknown:
        msg = f"{path} has unknown field(s): {', '.join(unknown)}"
        raise QualityProfileError(msg)


def _lower_key(value: str) -> str:
    return str(value or "").strip().lower()


def _symbol_session_key(value: str) -> str:
    raw = str(value or "").strip()
    if ":" not in raw:
        return ""
    symbol, session = raw.split(":", 1)
    symbol_key = normalize_histdata_symbol(symbol)
    session_key = _lower_key(session)
    if not symbol_key or not session_key:
        return ""
    return f"{symbol_key}:{session_key}"

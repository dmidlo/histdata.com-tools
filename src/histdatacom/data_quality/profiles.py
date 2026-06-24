"""Operator-configurable data-quality profile contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, cast

from histdatacom.data_quality.bars import (
    ASCII_M1_OUTLIER_RULE_ID,
    ASCII_M1_PRECISION_RULE_ID,
    ASCII_M1_TICK_RECONSTRUCTION_RULE_ID,
    DEFAULT_M1_OUTLIER_THRESHOLDS,
    DEFAULT_M1_TICK_RECONSTRUCTION_TOLERANCE,
    HistDataM1OutlierThresholds,
    HistDataM1TickReconstructionTolerance,
)
from histdatacom.data_quality.calendar import DOMAIN_CALENDAR_SESSION_RULE_ID
from histdatacom.data_quality.calendar_profiles import (
    HistDataCalendarProfile,
    calendar_profile_from_mapping,
)
from histdatacom.data_quality.contracts import QualitySeverity
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
DEFAULT_QUALITY_PROFILE_NAME = "default"
DEFAULT_QUALITY_PROFILE_SOURCE = "default"
OPERATOR_QUALITY_PROFILE_SOURCE = "operator-config"

QUALITY_PROFILE_METADATA_KEY = "quality_profile"

CONFIGURABLE_QUALITY_RULE_IDS = frozenset(
    {
        ASCII_ROW_COUNT_INGESTION_RULE_ID,
        ASCII_TIMESTAMP_GAP_RULE_ID,
        ASCII_TIMESTAMP_CONTINUITY_RULE_ID,
        ASCII_M1_PRECISION_RULE_ID,
        ASCII_M1_OUTLIER_RULE_ID,
        ASCII_M1_TICK_RECONSTRUCTION_RULE_ID,
        ASCII_TICK_SPREAD_RULE_ID,
        ASCII_TICK_MICROSTRUCTURE_RULE_ID,
        ASCII_TICK_SPREAD_REGIME_RULE_ID,
        DOMAIN_CROSS_INSTRUMENT_RULE_ID,
        DOMAIN_CALENDAR_SESSION_RULE_ID,
        MODELING_READINESS_RULE_ID,
    }
)

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "name",
        "source",
        "source_path",
        "rules",
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
class QualityProfile:
    """Versioned operator profile for data-quality rule construction."""

    schema_version: str = QUALITY_PROFILE_SCHEMA_VERSION
    name: str = DEFAULT_QUALITY_PROFILE_NAME
    source: str = DEFAULT_QUALITY_PROFILE_SOURCE
    source_path: str = ""
    rules: Mapping[str, Mapping[str, JSONValue]] = field(default_factory=dict)
    modeling_assumptions: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate static profile metadata and configured rule IDs."""
        if self.schema_version != QUALITY_PROFILE_SCHEMA_VERSION:
            msg = (
                "unsupported quality profile schema_version: "
                f"{self.schema_version!r}"
            )
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

    def m1_precision_rules_by_symbol(
        self,
    ) -> dict[str, HistDataSymbolPrecisionRule]:
        """Return configured M1 precision overrides by symbol."""
        config = self.rule_config(ASCII_M1_PRECISION_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "precision_rules_by_symbol",
                "precision_rules_by_asset_class",
                "warning_severity",
            },
            ASCII_M1_PRECISION_RULE_ID,
        )
        return _precision_rule_mapping(
            _mapping_field(
                config,
                "precision_rules_by_symbol",
                path=ASCII_M1_PRECISION_RULE_ID,
            ),
            key_normalizer=normalize_histdata_symbol,
            path=f"{ASCII_M1_PRECISION_RULE_ID}.precision_rules_by_symbol",
        )

    def m1_precision_rules_by_asset_class(
        self,
    ) -> dict[str, HistDataSymbolPrecisionRule]:
        """Return configured M1 precision overrides by asset class."""
        config = self.rule_config(ASCII_M1_PRECISION_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "precision_rules_by_symbol",
                "precision_rules_by_asset_class",
                "warning_severity",
            },
            ASCII_M1_PRECISION_RULE_ID,
        )
        return _precision_rule_mapping(
            _mapping_field(
                config,
                "precision_rules_by_asset_class",
                path=ASCII_M1_PRECISION_RULE_ID,
            ),
            key_normalizer=_lower_key,
            path=f"{ASCII_M1_PRECISION_RULE_ID}.precision_rules_by_asset_class",
        )

    def m1_outlier_thresholds(self) -> HistDataM1OutlierThresholds:
        """Return default M1 outlier thresholds from the profile."""
        config = self.rule_config(ASCII_M1_OUTLIER_RULE_ID)
        _reject_unknown_keys(
            config,
            {
                "thresholds",
                "thresholds_by_symbol",
                "thresholds_by_asset_class",
                "warning_severity",
            },
            ASCII_M1_OUTLIER_RULE_ID,
        )
        return _m1_outlier_thresholds(
            _mapping_field(config, "thresholds", path=ASCII_M1_OUTLIER_RULE_ID),
            base=DEFAULT_M1_OUTLIER_THRESHOLDS,
            path=f"{ASCII_M1_OUTLIER_RULE_ID}.thresholds",
        )

    def m1_outlier_thresholds_by_symbol(
        self,
    ) -> dict[str, HistDataM1OutlierThresholds]:
        """Return configured M1 outlier thresholds by symbol."""
        config = self.rule_config(ASCII_M1_OUTLIER_RULE_ID)
        return _m1_outlier_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_symbol",
                path=ASCII_M1_OUTLIER_RULE_ID,
            ),
            key_normalizer=normalize_histdata_symbol,
            path=f"{ASCII_M1_OUTLIER_RULE_ID}.thresholds_by_symbol",
        )

    def m1_outlier_thresholds_by_asset_class(
        self,
    ) -> dict[str, HistDataM1OutlierThresholds]:
        """Return configured M1 outlier thresholds by asset class."""
        config = self.rule_config(ASCII_M1_OUTLIER_RULE_ID)
        return _m1_outlier_threshold_mapping(
            _mapping_field(
                config,
                "thresholds_by_asset_class",
                path=ASCII_M1_OUTLIER_RULE_ID,
            ),
            key_normalizer=_lower_key,
            path=f"{ASCII_M1_OUTLIER_RULE_ID}.thresholds_by_asset_class",
        )

    def m1_tick_reconstruction_tolerance(
        self,
    ) -> HistDataM1TickReconstructionTolerance:
        """Return configured M1 tick reconstruction tolerance."""
        config = self.rule_config(ASCII_M1_TICK_RECONSTRUCTION_RULE_ID)
        _reject_unknown_keys(
            config,
            {"tolerance", "warning_severity"},
            ASCII_M1_TICK_RECONSTRUCTION_RULE_ID,
        )
        tolerance = _mapping_field(
            config,
            "tolerance",
            path=ASCII_M1_TICK_RECONSTRUCTION_RULE_ID,
        )
        return HistDataM1TickReconstructionTolerance(
            price_tolerance=_float_field(
                tolerance,
                "price_tolerance",
                DEFAULT_M1_TICK_RECONSTRUCTION_TOLERANCE.price_tolerance,
                minimum=0.0,
                path=f"{ASCII_M1_TICK_RECONSTRUCTION_RULE_ID}.tolerance",
            )
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
                f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}."
                "thresholds_by_asset_class"
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
                f"{ASCII_TICK_MICROSTRUCTURE_RULE_ID}."
                "thresholds_by_symbol_session"
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
                f"{ASCII_TICK_SPREAD_REGIME_RULE_ID}."
                "thresholds_by_asset_class"
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

    def to_request_payload(self) -> dict[str, JSONValue]:
        """Return a JSON-safe profile payload for runtime requests."""
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "source": self.source,
            "source_path": self.source_path,
            "rules": _json_mapping(self.rules),
            "modeling_assumptions": dict(self.modeling_assumptions),
        }

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return report metadata describing the active quality profile."""
        return {
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


def default_quality_profile() -> QualityProfile:
    """Return the deterministic default profile."""
    return QualityProfile()


def load_quality_profile_file(path: str | Path) -> QualityProfile:
    """Load and validate a JSON quality profile file."""
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
    return quality_profile_from_mapping(
        payload,
        source="file",
        source_path=str(profile_path),
    )


def quality_profile_from_mapping(
    payload: Mapping[str, Any] | None,
    *,
    source: str = OPERATOR_QUALITY_PROFILE_SOURCE,
    source_path: str = "",
) -> QualityProfile:
    """Validate and return a quality profile from a mapping payload."""
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
        modeling_assumptions=_mapping_field(
            payload,
            "modeling_assumptions",
            path="quality_profile",
        ),
    )
    validate_quality_profile(profile)
    return profile


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


def validate_quality_profile(profile: QualityProfile) -> None:
    """Eagerly validate every configured rule stanza."""
    profile.row_count_profile()
    profile.gap_tolerance(ASCII_TIMESTAMP_GAP_RULE_ID)
    profile.gap_tolerance(ASCII_TIMESTAMP_CONTINUITY_RULE_ID)
    profile.m1_precision_rules_by_symbol()
    profile.m1_precision_rules_by_asset_class()
    profile.m1_outlier_thresholds()
    profile.m1_outlier_thresholds_by_symbol()
    profile.m1_outlier_thresholds_by_asset_class()
    profile.m1_tick_reconstruction_tolerance()
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
        ASCII_M1_PRECISION_RULE_ID: ("warning_severity",),
        ASCII_M1_OUTLIER_RULE_ID: ("warning_severity",),
        ASCII_M1_TICK_RECONSTRUCTION_RULE_ID: ("warning_severity",),
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


def _m1_outlier_threshold_mapping(
    value: Mapping[str, JSONValue],
    *,
    key_normalizer: Any,
    path: str,
) -> dict[str, HistDataM1OutlierThresholds]:
    result: dict[str, HistDataM1OutlierThresholds] = {}
    for key, config in value.items():
        profile_key = str(key_normalizer(str(key)))
        if not profile_key:
            continue
        result[profile_key] = _m1_outlier_thresholds(
            _expect_mapping(config, path=f"{path}.{key}"),
            base=DEFAULT_M1_OUTLIER_THRESHOLDS,
            path=f"{path}.{key}",
        )
    return result


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


def _m1_outlier_thresholds(
    value: Mapping[str, JSONValue],
    *,
    base: HistDataM1OutlierThresholds,
    path: str,
) -> HistDataM1OutlierThresholds:
    _reject_unknown_keys(
        value,
        {
            "max_range_ratio",
            "max_open_jump_ratio",
            "flatline_run_length",
            "return_mad_multiplier",
            "return_absolute_ratio",
            "min_return_samples",
        },
        path,
    )
    return HistDataM1OutlierThresholds(
        max_range_ratio=_float_field(
            value,
            "max_range_ratio",
            base.max_range_ratio,
            minimum=0.0,
            minimum_exclusive=True,
            path=path,
        ),
        max_open_jump_ratio=_float_field(
            value,
            "max_open_jump_ratio",
            base.max_open_jump_ratio,
            minimum=0.0,
            minimum_exclusive=True,
            path=path,
        ),
        flatline_run_length=_int_field(
            value,
            "flatline_run_length",
            base.flatline_run_length,
            minimum=2,
            path=path,
        ),
        return_mad_multiplier=_float_field(
            value,
            "return_mad_multiplier",
            base.return_mad_multiplier,
            minimum=0.0,
            minimum_exclusive=True,
            path=path,
        ),
        return_absolute_ratio=_float_field(
            value,
            "return_absolute_ratio",
            base.return_absolute_ratio,
            minimum=0.0,
            minimum_exclusive=True,
            path=path,
        ),
        min_return_samples=_int_field(
            value,
            "min_return_samples",
            base.min_return_samples,
            minimum=2,
            path=path,
        ),
    )


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
            f"{path}.dynamic_window_initial_ms must be <= "
            "dynamic_window_max_ms"
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

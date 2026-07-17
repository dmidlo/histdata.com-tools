"""Real-evidence calibration and holdout certification for observation models.

The version-one observation operator remains the replay surface.  This module
adds the stricter version-two *claim* around it: parameters are estimated from
active-time epoch evidence relative to blocked dense reference windows, the
operator is exercised on later dense windows, and application readiness fails
closed when retention or another requested mechanism is not identifiable.

Only bounded aggregate evidence is persisted.  Dense reference rows and
degraded rows remain process-local.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from histdatacom.data_analytics.feed_epochs_v2 import (
    FEED_EPOCH_EVIDENCE_V2_SCHEMA_VERSION,
    FeedEpochDefinitionV2,
    FeedEpochEvidenceV2,
)
from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark import BenchmarkSplitKind
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.observation import (
    MAX_OBSERVATION_INPUT_EVENTS,
    OBSERVATION_PARAMETER_NAMES,
    ObservationApplicationResultV1,
    ObservationContextV1,
    ObservationFitEvidenceV1,
    ObservationInputEventV1,
    ObservationOperatorFitConfigV1,
    ObservationOperatorV1,
    fit_observation_operator,
)
from histdatacom.synthetic.streaming import ReconstructionWindowV1

OBSERVATION_CALIBRATION_PROFILE_V2_SCHEMA_VERSION = (
    "histdatacom.observation-calibration-profile.v2"
)
OBSERVATION_CALIBRATION_TARGET_V2_SCHEMA_VERSION = (
    "histdatacom.observation-calibration-target.v2"
)
OBSERVATION_CALIBRATION_WINDOW_V2_SCHEMA_VERSION = (
    "histdatacom.observation-calibration-window.v2"
)
OBSERVATION_CALIBRATION_CAMPAIGN_V2_SCHEMA_VERSION = (
    "histdatacom.observation-calibration-campaign.v2"
)
OBSERVATION_CALIBRATION_ENGINE_ID = "histdatacom.observation-calibration"
OBSERVATION_CALIBRATION_ENGINE_VERSION = "2.1.0"

OBSERVATION_UPDATE_STATES = (
    "update_bid_only",
    "update_ask_only",
    "update_joint",
    "update_unchanged",
)
OBSERVATION_CALIBRATION_SESSIONS = (
    "asia",
    "london",
    "new_york",
    "off_session",
)
OBSERVATION_CALIBRATION_SPLITS = (
    BenchmarkSplitKind.CALIBRATION,
    BenchmarkSplitKind.VALIDATION,
    BenchmarkSplitKind.FINAL_HOLDOUT,
)
OBSERVATION_CALIBRATION_REQUIRED_PARAMETERS = (
    "retention_probability",
    "unchanged_retention_probability",
    "timestamp_quantum_ns",
    "price_precision_digits",
    "duplicate_probability",
    "burst_window_ns",
)
OBSERVATION_CALIBRATION_MECHANISMS = (
    "calendar_closure",
    "archive_gap",
    "retention",
    "unchanged_filter",
    "timestamp_quantization",
    "batching",
    "duplicate",
    "rate_cap",
    "quiet_gap",
    "outage",
    "reconnect",
)

_STATUS_VALUES = {"supported", "bounded", "unsupported"}
_PROBABILITY_PARAMETERS = {
    "retention_probability",
    "unchanged_retention_probability",
    "duplicate_probability",
    "quiet_gap_probability",
    "reconnect_duplicate_probability",
}
_INTEGER_PARAMETERS = {
    "timestamp_quantum_ns",
    "price_precision_digits",
    "batch_window_ns",
    "burst_window_ns",
    "outage_window_ns",
}
_SPLIT_KEYS = tuple(item.value for item in OBSERVATION_CALIBRATION_SPLITS)
_SESSION_HOURS = {
    "asia": (0, 7),
    "london": (7, 12),
    "new_york": (12, 21),
    "off_session": (21, 24),
}
_UPDATE_FEATURES = {
    "update_bid_only": "bid_only_rate",
    "update_ask_only": "ask_only_rate",
    "update_joint": "joint_move_rate",
    "update_unchanged": "unchanged_rate",
}
_SESSION_FEATURES = {
    name: f"session_activity_share_{name}"
    for name in OBSERVATION_CALIBRATION_SESSIONS
}
_NEUTRAL_VALUES = {
    "retention_probability": 1.0,
    "unchanged_retention_probability": 1.0,
    "timestamp_quantum_ns": 1.0,
    "price_precision_digits": 16.0,
    "quote_transition_threshold": 0.0,
    "batch_window_ns": 0.0,
    "duplicate_probability": 0.0,
    "rate_cap_per_second": 0.0,
    "burst_window_ns": 0.0,
    "quiet_gap_probability": 0.0,
    "outage_window_ns": 0.0,
    "reconnect_duplicate_probability": 0.0,
}


@dataclass(frozen=True, slots=True)
class ObservationCalibrationProfileV2:
    """Blocked-time, resource, and acceptance policy for one calibration."""

    split_periods: Mapping[str, str] = field(default_factory=dict)
    sessions: tuple[str, ...] = OBSERVATION_CALIBRATION_SESSIONS[:3]
    max_events_per_window: int = 4096
    minimum_events_per_window: int = 512
    max_source_bytes: int = 2 * 1024**3
    max_runtime_seconds: float = 600.0
    max_peak_memory_bytes: int = 2 * 1024**3
    retention_tolerance: float = 0.06
    duplicate_tolerance: float = 0.03
    timestamp_tolerance: float = 0.06
    update_mix_l1_tolerance: float = 0.35
    rounding_digits: int = 8
    profile_id: str = ""
    schema_version: str = OBSERVATION_CALIBRATION_PROFILE_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != OBSERVATION_CALIBRATION_PROFILE_V2_SCHEMA_VERSION
        ):
            raise ValueError("unsupported observation calibration profile")
        periods = {
            str(key): str(value) for key, value in self.split_periods.items()
        }
        if periods:
            if tuple(sorted(periods)) != tuple(sorted(_SPLIT_KEYS)):
                raise ValueError(
                    "calibration split periods must define all splits"
                )
            if any(not _valid_month(value) for value in periods.values()):
                raise ValueError("calibration split periods must use YYYYMM")
            ordered = tuple(periods[key] for key in _SPLIT_KEYS)
            if tuple(sorted(ordered)) != ordered or len(set(ordered)) != 3:
                raise ValueError(
                    "calibration split periods must be chronological"
                )
        sessions = tuple(dict.fromkeys(str(value) for value in self.sessions))
        if not sessions or any(
            value not in _SESSION_HOURS for value in sessions
        ):
            raise ValueError("unsupported observation calibration session")
        if (
            not 64
            <= self.minimum_events_per_window
            <= self.max_events_per_window
        ):
            raise ValueError("calibration minimum window size is invalid")
        if not self.max_events_per_window <= MAX_OBSERVATION_INPUT_EVENTS:
            raise ValueError(
                "calibration window exceeds observation input bound"
            )
        if not 1 <= self.max_source_bytes <= 64 * 1024**3:
            raise ValueError("calibration source byte bound is invalid")
        if not 1.0 <= self.max_runtime_seconds <= 86_400.0:
            raise ValueError("calibration runtime bound is invalid")
        if not 64 * 1024**2 <= self.max_peak_memory_bytes <= 64 * 1024**3:
            raise ValueError("calibration memory bound is invalid")
        for name in (
            "retention_tolerance",
            "duplicate_tolerance",
            "timestamp_tolerance",
            "update_mix_l1_tolerance",
        ):
            value = _finite_float(getattr(self, name), name)
            if not 0.0 < value <= 1.0:
                raise ValueError(f"{name} must be in (0, 1]")
        if not 0 <= self.rounding_digits <= 12:
            raise ValueError("rounding_digits must be between zero and twelve")
        object.__setattr__(self, "split_periods", dict(sorted(periods.items())))
        object.__setattr__(self, "sessions", sessions)
        expected = _stable_id(
            "observation-calibration-profile-v2", self.identity_payload()
        )
        if self.profile_id and self.profile_id != expected:
            raise ValueError("observation calibration profile ID differs")
        object.__setattr__(self, "profile_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": OBSERVATION_CALIBRATION_ENGINE_ID,
            "engine_version": OBSERVATION_CALIBRATION_ENGINE_VERSION,
            "split_periods": dict(self.split_periods),
            "sessions": list(self.sessions),
            "max_events_per_window": self.max_events_per_window,
            "minimum_events_per_window": self.minimum_events_per_window,
            "max_source_bytes": self.max_source_bytes,
            "max_runtime_seconds": self.max_runtime_seconds,
            "max_peak_memory_bytes": self.max_peak_memory_bytes,
            "retention_tolerance": self.retention_tolerance,
            "duplicate_tolerance": self.duplicate_tolerance,
            "timestamp_tolerance": self.timestamp_tolerance,
            "update_mix_l1_tolerance": self.update_mix_l1_tolerance,
            "rounding_digits": self.rounding_digits,
            "dense_intermediate_policy": "process_local_bounded_rows",
            "persisted_evidence_policy": "aggregate_only",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationCalibrationProfileV2":
        if (
            data.get("engine_id") != OBSERVATION_CALIBRATION_ENGINE_ID
            or data.get("engine_version")
            != OBSERVATION_CALIBRATION_ENGINE_VERSION
        ):
            raise ValueError("unsupported observation calibration engine")
        return cls(
            schema_version=str(data.get("schema_version", "")),
            split_periods={
                str(key): str(value)
                for key, value in _mapping(data.get("split_periods")).items()
            },
            sessions=_string_tuple(data.get("sessions")),
            max_events_per_window=_strict_int(
                data.get("max_events_per_window"), "max_events_per_window"
            ),
            minimum_events_per_window=_strict_int(
                data.get("minimum_events_per_window"),
                "minimum_events_per_window",
            ),
            max_source_bytes=_strict_int(
                data.get("max_source_bytes"), "max_source_bytes"
            ),
            max_runtime_seconds=_finite_float(
                data.get("max_runtime_seconds"), "max_runtime_seconds"
            ),
            max_peak_memory_bytes=_strict_int(
                data.get("max_peak_memory_bytes"),
                "max_peak_memory_bytes",
            ),
            retention_tolerance=_finite_float(
                data.get("retention_tolerance"), "retention_tolerance"
            ),
            duplicate_tolerance=_finite_float(
                data.get("duplicate_tolerance"), "duplicate_tolerance"
            ),
            timestamp_tolerance=_finite_float(
                data.get("timestamp_tolerance"), "timestamp_tolerance"
            ),
            update_mix_l1_tolerance=_finite_float(
                data.get("update_mix_l1_tolerance"),
                "update_mix_l1_tolerance",
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            profile_id=str(data.get("profile_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationCalibrationTargetV2:
    """One symbol/epoch target with explicit identifiability and refusals."""

    symbol: str
    epoch_label: str
    reference_epoch_label: str
    calibration_end_period: str
    parameter_values: Mapping[str, float]
    parameter_lower_bounds: Mapping[str, float]
    parameter_upper_bounds: Mapping[str, float]
    parameter_support_counts: Mapping[str, int]
    parameter_status: Mapping[str, str]
    parameter_reasons: Mapping[str, str]
    target_statistics: Mapping[str, JSONValue]
    mechanism_diagnostics: Mapping[str, str]
    source_evidence_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    target_id: str = ""
    schema_version: str = OBSERVATION_CALIBRATION_TARGET_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != OBSERVATION_CALIBRATION_TARGET_V2_SCHEMA_VERSION
        ):
            raise ValueError("unsupported observation calibration target")
        symbol = str(self.symbol).strip().upper()
        if not symbol:
            raise ValueError("calibration target requires a symbol")
        epoch = _required_text(self.epoch_label, "epoch_label")
        reference = _required_text(
            self.reference_epoch_label, "reference_epoch_label"
        )
        if not _valid_month(self.calibration_end_period):
            raise ValueError("calibration target period must use YYYYMM")
        names = set(OBSERVATION_PARAMETER_NAMES)
        maps = (
            self.parameter_values,
            self.parameter_lower_bounds,
            self.parameter_upper_bounds,
            self.parameter_support_counts,
            self.parameter_status,
            self.parameter_reasons,
        )
        if any(set(value) != names for value in maps):
            raise ValueError("calibration target parameter keys differ")
        values: dict[str, float] = {}
        lowers: dict[str, float] = {}
        uppers: dict[str, float] = {}
        supports: dict[str, int] = {}
        statuses: dict[str, str] = {}
        reasons: dict[str, str] = {}
        for name in OBSERVATION_PARAMETER_NAMES:
            value = _calibration_parameter_value(
                name, self.parameter_values[name]
            )
            lower = _calibration_parameter_value(
                name, self.parameter_lower_bounds[name]
            )
            upper = _calibration_parameter_value(
                name, self.parameter_upper_bounds[name]
            )
            if not lower <= value <= upper:
                raise ValueError("calibration parameter lies outside bounds")
            status = str(self.parameter_status[name])
            if status not in _STATUS_VALUES:
                raise ValueError(
                    "unsupported calibration identifiability status"
                )
            support = _strict_int(
                self.parameter_support_counts[name], f"{name} support"
            )
            if support < 0:
                raise ValueError("calibration parameter support is negative")
            if status == "supported" and support == 0:
                raise ValueError(
                    "supported calibration parameter has no support"
                )
            values[name] = value
            lowers[name] = lower
            uppers[name] = upper
            supports[name] = support
            statuses[name] = status
            reasons[name] = _required_text(
                self.parameter_reasons[name], f"{name} reason"
            )
        mechanisms = {
            str(key): str(value)
            for key, value in self.mechanism_diagnostics.items()
        }
        if set(mechanisms) != set(OBSERVATION_CALIBRATION_MECHANISMS):
            raise ValueError("calibration mechanism diagnostics differ")
        evidence_ids = tuple(sorted(dict.fromkeys(self.source_evidence_ids)))
        hashes = tuple(sorted(dict.fromkeys(self.source_hashes)))
        if not evidence_ids or not hashes:
            raise ValueError("calibration target requires source lineage")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "epoch_label", epoch)
        object.__setattr__(self, "reference_epoch_label", reference)
        object.__setattr__(self, "parameter_values", values)
        object.__setattr__(self, "parameter_lower_bounds", lowers)
        object.__setattr__(self, "parameter_upper_bounds", uppers)
        object.__setattr__(self, "parameter_support_counts", supports)
        object.__setattr__(self, "parameter_status", statuses)
        object.__setattr__(self, "parameter_reasons", reasons)
        object.__setattr__(
            self, "target_statistics", dict(self.target_statistics)
        )
        object.__setattr__(self, "mechanism_diagnostics", mechanisms)
        object.__setattr__(self, "source_evidence_ids", evidence_ids)
        object.__setattr__(self, "source_hashes", hashes)
        expected = _stable_id(
            "observation-calibration-target-v2", self.identity_payload()
        )
        if self.target_id and self.target_id != expected:
            raise ValueError("observation calibration target ID differs")
        object.__setattr__(self, "target_id", expected)

    @property
    def unsupported_parameters(self) -> tuple[str, ...]:
        return tuple(
            name
            for name in OBSERVATION_PARAMETER_NAMES
            if self.parameter_status[name] != "supported"
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "epoch_label": self.epoch_label,
            "reference_epoch_label": self.reference_epoch_label,
            "calibration_end_period": self.calibration_end_period,
            "parameter_values": dict(self.parameter_values),
            "parameter_lower_bounds": dict(self.parameter_lower_bounds),
            "parameter_upper_bounds": dict(self.parameter_upper_bounds),
            "parameter_support_counts": dict(self.parameter_support_counts),
            "parameter_status": dict(self.parameter_status),
            "parameter_reasons": dict(self.parameter_reasons),
            "target_statistics": dict(self.target_statistics),
            "mechanism_diagnostics": dict(self.mechanism_diagnostics),
            "source_evidence_ids": list(self.source_evidence_ids),
            "source_hashes": list(self.source_hashes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "target_id": self.target_id,
            "unsupported_parameters": list(self.unsupported_parameters),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationCalibrationTargetV2":
        return cls(
            schema_version=str(data.get("schema_version", "")),
            symbol=str(data.get("symbol", "")),
            epoch_label=str(data.get("epoch_label", "")),
            reference_epoch_label=str(data.get("reference_epoch_label", "")),
            calibration_end_period=str(data.get("calibration_end_period", "")),
            parameter_values=_float_mapping(data.get("parameter_values")),
            parameter_lower_bounds=_float_mapping(
                data.get("parameter_lower_bounds")
            ),
            parameter_upper_bounds=_float_mapping(
                data.get("parameter_upper_bounds")
            ),
            parameter_support_counts=_int_mapping(
                data.get("parameter_support_counts")
            ),
            parameter_status=_string_mapping(data.get("parameter_status")),
            parameter_reasons=_string_mapping(data.get("parameter_reasons")),
            target_statistics=_mapping(data.get("target_statistics")),
            mechanism_diagnostics=_string_mapping(
                data.get("mechanism_diagnostics")
            ),
            source_evidence_ids=_string_tuple(data.get("source_evidence_ids")),
            source_hashes=_string_tuple(data.get("source_hashes")),
            target_id=str(data.get("target_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationCalibrationWindowV2:
    """Bounded aggregate result for one applied dense-reference window."""

    split_kind: BenchmarkSplitKind
    symbol: str
    period: str
    session: str
    epoch_label: str
    source_artifact_sha256: str
    input_count: int
    output_count: int
    retained_source_count: int
    target_metrics: Mapping[str, float]
    observed_metrics: Mapping[str, float]
    absolute_errors: Mapping[str, float]
    tolerances: Mapping[str, float]
    reason_counts: Mapping[str, int]
    transformation_counts: Mapping[str, int]
    passed: bool
    failure_reasons: tuple[str, ...]
    window_id: str = ""
    schema_version: str = OBSERVATION_CALIBRATION_WINDOW_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != OBSERVATION_CALIBRATION_WINDOW_V2_SCHEMA_VERSION
        ):
            raise ValueError("unsupported observation calibration window")
        split = BenchmarkSplitKind.from_value(self.split_kind)
        if not _valid_month(self.period):
            raise ValueError("calibration window period must use YYYYMM")
        if self.session not in _SESSION_HOURS:
            raise ValueError("unsupported calibration window session")
        if (
            self.input_count <= 0
            or not 0 <= self.retained_source_count <= self.input_count
        ):
            raise ValueError("invalid calibration window input counts")
        if self.output_count < self.retained_source_count:
            raise ValueError(
                "calibration output count precedes retained sources"
            )
        targets = _finite_mapping(self.target_metrics)
        observed = _finite_mapping(self.observed_metrics)
        errors = _finite_mapping(self.absolute_errors)
        tolerances = _finite_mapping(self.tolerances)
        if (
            not targets
            or set(targets) != set(observed)
            or set(errors) != set(targets)
        ):
            raise ValueError("calibration window metric keys differ")
        if any(name not in tolerances for name in errors):
            raise ValueError("calibration window tolerance is missing")
        failures = tuple(
            dict.fromkeys(str(value) for value in self.failure_reasons)
        )
        passed = bool(self.passed)
        if passed == bool(failures):
            raise ValueError(
                "calibration window pass state differs from failures"
            )
        object.__setattr__(self, "split_kind", split)
        object.__setattr__(self, "symbol", str(self.symbol).upper())
        object.__setattr__(self, "target_metrics", targets)
        object.__setattr__(self, "observed_metrics", observed)
        object.__setattr__(self, "absolute_errors", errors)
        object.__setattr__(self, "tolerances", tolerances)
        object.__setattr__(
            self, "reason_counts", _int_mapping(self.reason_counts)
        )
        object.__setattr__(
            self,
            "transformation_counts",
            _int_mapping(self.transformation_counts),
        )
        object.__setattr__(self, "passed", passed)
        object.__setattr__(self, "failure_reasons", failures)
        expected = _stable_id(
            "observation-calibration-window-v2", self.identity_payload()
        )
        if self.window_id and self.window_id != expected:
            raise ValueError("observation calibration window ID differs")
        object.__setattr__(self, "window_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_kind": self.split_kind.value,
            "symbol": self.symbol,
            "period": self.period,
            "session": self.session,
            "epoch_label": self.epoch_label,
            "source_artifact_sha256": self.source_artifact_sha256,
            "input_count": self.input_count,
            "output_count": self.output_count,
            "retained_source_count": self.retained_source_count,
            "target_metrics": dict(self.target_metrics),
            "observed_metrics": dict(self.observed_metrics),
            "absolute_errors": dict(self.absolute_errors),
            "tolerances": dict(self.tolerances),
            "reason_counts": dict(self.reason_counts),
            "transformation_counts": dict(self.transformation_counts),
            "passed": self.passed,
            "failure_reasons": list(self.failure_reasons),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "window_id": self.window_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationCalibrationWindowV2":
        return cls(
            schema_version=str(data.get("schema_version", "")),
            split_kind=BenchmarkSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            session=str(data.get("session", "")),
            epoch_label=str(data.get("epoch_label", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            input_count=_strict_int(data.get("input_count"), "input_count"),
            output_count=_strict_int(data.get("output_count"), "output_count"),
            retained_source_count=_strict_int(
                data.get("retained_source_count"), "retained_source_count"
            ),
            target_metrics=_float_mapping(data.get("target_metrics")),
            observed_metrics=_float_mapping(data.get("observed_metrics")),
            absolute_errors=_float_mapping(data.get("absolute_errors")),
            tolerances=_float_mapping(data.get("tolerances")),
            reason_counts=_int_mapping(data.get("reason_counts")),
            transformation_counts=_int_mapping(
                data.get("transformation_counts")
            ),
            passed=_strict_bool(data.get("passed"), "passed"),
            failure_reasons=_string_tuple(data.get("failure_reasons")),
            window_id=str(data.get("window_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationCalibrationCampaignV2:
    """Immutable fit, blocked holdouts, lineage, and fail-closed readiness."""

    feed_epoch_definition_id: str
    calibration_corpus_sha256: str
    profile: ObservationCalibrationProfileV2
    operator: ObservationOperatorV1
    targets: tuple[ObservationCalibrationTargetV2, ...]
    fit_evidence: tuple[ObservationFitEvidenceV1, ...]
    windows: tuple[ObservationCalibrationWindowV2, ...]
    readiness_status: str
    readiness_reasons: tuple[str, ...]
    runtime_seconds: float
    peak_memory_bytes: int
    campaign_id: str = ""
    schema_version: str = OBSERVATION_CALIBRATION_CAMPAIGN_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != OBSERVATION_CALIBRATION_CAMPAIGN_V2_SCHEMA_VERSION
        ):
            raise ValueError("unsupported observation calibration campaign")
        if (
            self.feed_epoch_definition_id
            != self.operator.feed_epoch_definition_id
        ):
            raise ValueError(
                "calibration campaign definition differs from operator"
            )
        corpus_hash = _sha256_text(self.calibration_corpus_sha256)
        targets = tuple(
            sorted(
                self.targets, key=lambda item: (item.symbol, item.epoch_label)
            )
        )
        evidence = tuple(
            sorted(self.fit_evidence, key=lambda item: item.evidence_id)
        )
        windows = tuple(sorted(self.windows, key=lambda item: item.window_id))
        if not targets or not evidence or not windows:
            raise ValueError(
                "calibration campaign requires targets, evidence, and windows"
            )
        status = str(self.readiness_status)
        if status not in {"ready", "failed"}:
            raise ValueError("unsupported observation calibration readiness")
        reasons = tuple(
            dict.fromkeys(str(value) for value in self.readiness_reasons)
        )
        if (status == "ready") == bool(reasons):
            raise ValueError("calibration readiness differs from reasons")
        target_keys = {(item.symbol, item.epoch_label) for item in targets}
        if any(
            (item.symbol, item.epoch_label) not in target_keys
            for item in windows
        ):
            raise ValueError("calibration window has no target")
        runtime_seconds = _finite_float(self.runtime_seconds, "runtime_seconds")
        peak_memory_bytes = _strict_int(
            self.peak_memory_bytes, "peak_memory_bytes"
        )
        if runtime_seconds < 0.0 or peak_memory_bytes < 0:
            raise ValueError("calibration resource evidence is negative")
        expected_reasons = _readiness_reasons(
            targets,
            windows,
            self.profile,
            runtime_seconds=runtime_seconds,
            peak_memory_bytes=peak_memory_bytes,
        )
        expected_status = "failed" if expected_reasons else "ready"
        if status != expected_status or reasons != expected_reasons:
            raise ValueError("calibration readiness evidence differs")
        object.__setattr__(self, "calibration_corpus_sha256", corpus_hash)
        object.__setattr__(self, "targets", targets)
        object.__setattr__(self, "fit_evidence", evidence)
        object.__setattr__(self, "windows", windows)
        object.__setattr__(self, "readiness_status", status)
        object.__setattr__(self, "readiness_reasons", reasons)
        object.__setattr__(self, "runtime_seconds", runtime_seconds)
        object.__setattr__(self, "peak_memory_bytes", peak_memory_bytes)
        expected = _stable_id(
            "observation-calibration-campaign-v2", self.identity_payload()
        )
        if self.campaign_id and self.campaign_id != expected:
            raise ValueError("observation calibration campaign ID differs")
        object.__setattr__(self, "campaign_id", expected)

    @property
    def valid_for_application(self) -> bool:
        return self.readiness_status == "ready"

    def require_application_ready(
        self,
        context: ObservationContextV1,
        *,
        required_parameters: Sequence[
            str
        ] = OBSERVATION_CALIBRATION_REQUIRED_PARAMETERS,
    ) -> ObservationCalibrationTargetV2:
        """Return the target or reject an unsupported calibration claim."""
        if not self.valid_for_application:
            raise ValueError("observation calibration campaign is not ready")
        target = next(
            (
                item
                for item in self.targets
                if item.symbol == context.symbol
                and item.epoch_label == context.epoch_id
            ),
            None,
        )
        if target is None:
            raise ValueError("observation context is not calibrated")
        requested = tuple(
            dict.fromkeys(str(value) for value in required_parameters)
        )
        if any(value not in OBSERVATION_PARAMETER_NAMES for value in requested):
            raise ValueError("unsupported requested observation parameter")
        unsupported = tuple(
            name
            for name in requested
            if target.parameter_status[name] != "supported"
        )
        if unsupported:
            raise ValueError(
                "requested observation parameters are unsupported: "
                + ", ".join(unsupported)
            )
        if target.parameter_reasons["retention_probability"] == (
            "identity_without_dense_denominator"
        ):
            raise ValueError(
                "identity retention without a dense denominator is unsafe"
            )
        return target

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "feed_epoch_definition_id": self.feed_epoch_definition_id,
            "calibration_corpus_sha256": self.calibration_corpus_sha256,
            "profile": self.profile.to_dict(),
            "operator": self.operator.to_dict(),
            "targets": [item.to_dict() for item in self.targets],
            "fit_evidence": [item.to_dict() for item in self.fit_evidence],
            "windows": [item.to_dict() for item in self.windows],
            "readiness_status": self.readiness_status,
            "readiness_reasons": list(self.readiness_reasons),
            "application_readiness_policy": {
                "default_required_parameters": list(
                    OBSERVATION_CALIBRATION_REQUIRED_PARAMETERS
                ),
                "unsupported_parameter_policy": "reject_requested_mechanism",
                "transition_epoch_policy": "reject_unfitted_context",
                "identity_retention_without_dense_denominator": "forbidden",
            },
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "campaign_id": self.campaign_id,
            "valid_for_application": self.valid_for_application,
            "runtime_seconds": self.runtime_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "resource_bounds": {
                "max_events_per_window": self.profile.max_events_per_window,
                "max_source_bytes": self.profile.max_source_bytes,
                "max_runtime_seconds": self.profile.max_runtime_seconds,
                "max_peak_memory_bytes": self.profile.max_peak_memory_bytes,
                "window_count": len(self.windows),
                "persisted_dense_rows": 0,
            },
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationCalibrationCampaignV2":
        return cls(
            schema_version=str(data.get("schema_version", "")),
            feed_epoch_definition_id=str(
                data.get("feed_epoch_definition_id", "")
            ),
            calibration_corpus_sha256=str(
                data.get("calibration_corpus_sha256", "")
            ),
            profile=ObservationCalibrationProfileV2.from_dict(
                _mapping(data.get("profile"))
            ),
            operator=ObservationOperatorV1.from_dict(
                _mapping(data.get("operator"))
            ),
            targets=tuple(
                ObservationCalibrationTargetV2.from_dict(_mapping(item))
                for item in _sequence(data.get("targets"))
            ),
            fit_evidence=tuple(
                ObservationFitEvidenceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("fit_evidence"))
            ),
            windows=tuple(
                ObservationCalibrationWindowV2.from_dict(_mapping(item))
                for item in _sequence(data.get("windows"))
            ),
            readiness_status=str(data.get("readiness_status", "")),
            readiness_reasons=_string_tuple(data.get("readiness_reasons")),
            runtime_seconds=_finite_float(
                data.get("runtime_seconds"), "runtime_seconds"
            ),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes"), "peak_memory_bytes"
            ),
            campaign_id=str(data.get("campaign_id", "")),
        )


def calibrate_historical_observation_operators(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    epoch_definition: FeedEpochDefinitionV2,
    profile: ObservationCalibrationProfileV2 | None = None,
) -> ObservationCalibrationCampaignV2:
    """Fit and certify one bounded real-evidence observation campaign."""
    started = time.perf_counter()
    selected = profile or ObservationCalibrationProfileV2()
    if not epoch_definition.valid_for_observation_models:
        raise ValueError("feed epoch definition has not passed stability")
    ordered = tuple(
        sorted(evidence, key=lambda item: (item.period, item.symbol))
    )
    if not ordered:
        raise ValueError("observation calibration evidence is empty")
    if any(item.symbol not in epoch_definition.symbols for item in ordered):
        raise ValueError(
            "observation calibration evidence is outside symbol scope"
        )
    split_periods = dict(selected.split_periods) or _automatic_split_periods(
        ordered, epoch_definition
    )
    selected = ObservationCalibrationProfileV2(
        split_periods=split_periods,
        sessions=selected.sessions,
        max_events_per_window=selected.max_events_per_window,
        minimum_events_per_window=selected.minimum_events_per_window,
        max_source_bytes=selected.max_source_bytes,
        max_runtime_seconds=selected.max_runtime_seconds,
        max_peak_memory_bytes=selected.max_peak_memory_bytes,
        retention_tolerance=selected.retention_tolerance,
        duplicate_tolerance=selected.duplicate_tolerance,
        timestamp_tolerance=selected.timestamp_tolerance,
        update_mix_l1_tolerance=selected.update_mix_l1_tolerance,
        rounding_digits=selected.rounding_digits,
    )
    reference_epoch = epoch_definition.epochs[-1]
    calibration_period = split_periods[BenchmarkSplitKind.CALIBRATION.value]
    source_rows = _fit_source_evidence(
        ordered,
        epoch_definition=epoch_definition,
        reference_epoch_label=reference_epoch.label,
        calibration_end_period=calibration_period,
    )
    corpus_payload: dict[str, JSONValue] = {
        "feed_epoch_definition_id": epoch_definition.definition_id,
        "profile_id": selected.profile_id,
        "calibration_end_period": calibration_period,
        "evidence_ids": [
            cast(JSONValue, item.evidence_id) for item in source_rows
        ],
        "source_hashes": [
            cast(JSONValue, item)
            for item in sorted(
                {row.source_artifact_sha256 for row in source_rows}
            )
        ],
        "claim": "relative_dense_reference_observation_calibration",
    }
    corpus_hash = (
        "sha256:"
        + hashlib.sha256(
            canonical_contract_json(corpus_payload).encode("utf-8")
        ).hexdigest()
    )
    targets = _build_targets(
        source_rows,
        epoch_definition=epoch_definition,
        reference_epoch_label=reference_epoch.label,
        calibration_end_period=calibration_period,
        rounding_digits=selected.rounding_digits,
    )
    fit_evidence = _build_fit_evidence(
        targets,
        epoch_definition=epoch_definition,
        corpus_hash=corpus_hash,
    )
    operator = fit_observation_operator(
        fit_evidence,
        epoch_definition=epoch_definition,
        config=ObservationOperatorFitConfigV1(
            min_stratum_support=16,
            min_parameter_support=1,
            min_supported_parameters=len(
                OBSERVATION_CALIBRATION_REQUIRED_PARAMETERS
            ),
            max_input_events=selected.max_events_per_window,
            max_strata=512,
            rounding_digits=selected.rounding_digits,
        ),
    )
    windows = _evaluate_windows(
        ordered,
        split_periods=split_periods,
        profile=selected,
        operator=operator,
        targets=targets,
    )
    peak = peak_rss_bytes()
    runtime_seconds = round(time.perf_counter() - started, 6)
    reasons = _readiness_reasons(
        targets,
        windows,
        selected,
        runtime_seconds=runtime_seconds,
        peak_memory_bytes=int(peak),
    )
    return ObservationCalibrationCampaignV2(
        feed_epoch_definition_id=epoch_definition.definition_id,
        calibration_corpus_sha256=corpus_hash,
        profile=selected,
        operator=operator,
        targets=targets,
        fit_evidence=fit_evidence,
        windows=windows,
        readiness_status="failed" if reasons else "ready",
        readiness_reasons=reasons,
        runtime_seconds=runtime_seconds,
        peak_memory_bytes=int(peak),
    )


def estimate_paired_observation_evidence(
    reference_events: Sequence[ObservationInputEventV1],
    observed: ObservationApplicationResultV1,
    *,
    period: str,
    source_artifact_sha256: str,
) -> tuple[ObservationFitEvidenceV1, ...]:
    """Recover bounded state-conditioned parameters from an explicit pair.

    This estimator is intentionally narrow.  It recovers only mechanisms with
    a directly observed denominator: state retention, timestamp grid, and
    duplicate emission.  It does not infer outage or archive gaps from silence.
    """
    if not _valid_month(period):
        raise ValueError("paired calibration period must use YYYYMM")
    source_hash = _sha256_text(source_artifact_sha256)
    by_id = {item.source_event_id: item for item in reference_events}
    if len(by_id) != len(reference_events) or not by_id:
        raise ValueError("paired calibration reference identities are invalid")
    if any(
        item.source_event_id not in by_id for item in observed.output_events
    ):
        raise ValueError("paired observation has an unknown source identity")
    outputs_by_id: Counter[str] = Counter(
        item.source_event_id for item in observed.output_events
    )
    grouped: dict[
        tuple[str, str, str | None, str | None], list[ObservationInputEventV1]
    ] = defaultdict(list)
    for item in reference_events:
        key = (
            item.context.symbol,
            item.context.epoch_id,
            item.context.state,
            item.context.session,
        )
        grouped[key].append(item)
    result: list[ObservationFitEvidenceV1] = []
    for items in grouped.values():
        context = items[0].context
        retained = [
            item for item in items if outputs_by_id[item.source_event_id] > 0
        ]
        output_rows = [
            output
            for output in observed.output_events
            if output.source_event_id
            in {item.source_event_id for item in items}
        ]
        values = {
            "retention_probability": len(retained) / len(items),
            "timestamp_quantum_ns": float(_timestamp_grid_ns(output_rows)),
            "duplicate_probability": (
                max(0, len(output_rows) - len(retained)) / max(1, len(retained))
            ),
        }
        supports = {
            "retention_probability": len(items),
            "timestamp_quantum_ns": len(output_rows),
            "duplicate_probability": len(retained),
        }
        bases = {name: "paired_dense_denominator" for name in values}
        provenance = {
            "retention_probability": (
                "pair.reference.source_event_id",
                "pair.observed.source_event_id",
            ),
            "timestamp_quantum_ns": ("pair.observed.observed_time_ns",),
            "duplicate_probability": ("pair.observed.duplicate_ordinal",),
        }
        start = min(item.event_time_ns for item in items)
        end = max(item.event_time_ns for item in items)
        result.append(
            ObservationFitEvidenceV1(
                context=context,
                period=period,
                start_timestamp_ns=start,
                end_timestamp_ns=end,
                source_evidence_id=_stable_id(
                    "paired-calibration-source",
                    {
                        "period": period,
                        "context": context.to_dict(),
                        "input_count": len(items),
                        "output_count": len(output_rows),
                    },
                ),
                source_artifact_sha256=source_hash,
                source_hash_basis="paired_calibration_artifact_sha256",
                evidence_kind="paired_calibration",
                parameter_values=values,
                parameter_lower_bounds=values,
                parameter_upper_bounds=values,
                parameter_support_counts=supports,
                parameter_basis=bases,
                parameter_provenance=provenance,
            )
        )
    return tuple(sorted(result, key=lambda item: item.evidence_id))


def write_observation_calibration_campaign(
    campaign: ObservationCalibrationCampaignV2,
    directory: str | Path,
) -> Mapping[str, ArtifactRef]:
    """Persist compact campaign, operator, and fit-evidence artifacts."""
    root = Path(directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    payloads: dict[str, Mapping[str, JSONValue]] = {
        "campaign": campaign.to_dict(),
        "operator": campaign.operator.to_dict(),
        "fit_evidence": {
            "schema_version": "histdatacom.observation-calibration-fit-evidence.v2",
            "campaign_id": campaign.campaign_id,
            "calibration_corpus_sha256": campaign.calibration_corpus_sha256,
            "evidence_count": len(campaign.fit_evidence),
            "evidence": [item.to_dict() for item in campaign.fit_evidence],
        },
    }
    artifacts: dict[str, ArtifactRef] = {}
    for name, payload in payloads.items():
        encoded = canonical_contract_json(payload).encode("utf-8") + b"\n"
        path = root / f"observation-calibration-v2-{name}.json"
        path.write_bytes(encoded)
        artifacts[name] = ArtifactRef(
            kind=f"observation_calibration_v2_{name}",
            path=str(path),
            size_bytes=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
            metadata={
                "schema_version": str(payload.get("schema_version", "")),
                "campaign_id": campaign.campaign_id,
                "operator_id": campaign.operator.operator_id,
            },
        )
    return artifacts


def read_observation_calibration_campaign(
    path: str | Path,
) -> ObservationCalibrationCampaignV2:
    """Read and strictly verify a persisted v2 campaign."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("observation calibration artifact exceeds size bound")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("observation calibration artifact must be an object")
    return ObservationCalibrationCampaignV2.from_dict(payload)


def read_feed_epoch_evidence_v2(
    path: str | Path,
) -> tuple[FeedEpochEvidenceV2, ...]:
    """Read the bounded evidence artifact produced by ``feed-epochs-v2``."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("feed epoch evidence artifact exceeds size bound")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("feed epoch evidence artifact must be an object")
    if payload.get("schema_version") != FEED_EPOCH_EVIDENCE_V2_SCHEMA_VERSION:
        raise ValueError("unsupported feed epoch evidence artifact schema")
    values = tuple(
        FeedEpochEvidenceV2.from_dict(_mapping(item))
        for item in _sequence(payload.get("evidence"))
    )
    if not values or _strict_int(
        payload.get("evidence_count"), "evidence_count"
    ) != len(values):
        raise ValueError("feed epoch evidence artifact count differs")
    return values


def _fit_source_evidence(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    epoch_definition: FeedEpochDefinitionV2,
    reference_epoch_label: str,
    calibration_end_period: str,
) -> tuple[FeedEpochEvidenceV2, ...]:
    rows: list[FeedEpochEvidenceV2] = []
    for item in evidence:
        assignment = epoch_definition.assign(
            symbol=item.symbol,
            timestamp_utc_ms=(
                item.start_timestamp_utc_ms + item.end_timestamp_utc_ms
            )
            // 2,
        )
        if assignment.assignment_kind != "epoch":
            continue
        if (
            assignment.label == reference_epoch_label
            and item.period > calibration_end_period
        ):
            continue
        rows.append(item)
    if not rows:
        raise ValueError("no in-epoch evidence is available for calibration")
    return tuple(rows)


def _build_targets(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    epoch_definition: FeedEpochDefinitionV2,
    reference_epoch_label: str,
    calibration_end_period: str,
    rounding_digits: int,
) -> tuple[ObservationCalibrationTargetV2, ...]:
    grouped: dict[tuple[str, str], list[FeedEpochEvidenceV2]] = defaultdict(
        list
    )
    for item in evidence:
        assignment = epoch_definition.assign(
            symbol=item.symbol,
            timestamp_utc_ms=(
                item.start_timestamp_utc_ms + item.end_timestamp_utc_ms
            )
            // 2,
        )
        if assignment.assignment_kind == "epoch":
            grouped[(item.symbol, assignment.label)].append(item)
    result: list[ObservationCalibrationTargetV2] = []
    for symbol in epoch_definition.symbols:
        reference = grouped.get((symbol, reference_epoch_label), [])
        if not reference:
            raise ValueError(
                f"dense reference evidence is missing for {symbol}"
            )
        reference_stats = _aggregate_epoch_rows(reference)
        for epoch in epoch_definition.epochs:
            rows = grouped.get((symbol, epoch.label), [])
            if not rows:
                raise ValueError(
                    f"epoch evidence is missing for {symbol} {epoch.label}"
                )
            stats = _aggregate_epoch_rows(rows)
            rate = min(
                1.0,
                stats["active_rate_per_hour"]
                / max(1e-12, reference_stats["active_rate_per_hour"]),
            )
            exact_second = stats["timestamp_exact_second_rate"]
            quantum = _quantum_from_timestamp_features(
                exact_second,
                stats["timestamp_last_digit_entropy"],
            )
            precision = int(round(stats["price_precision_digits"]))
            unchanged_ratio = 1.0
            values = dict(_NEUTRAL_VALUES)
            values.update(
                {
                    "retention_probability": rate,
                    "unchanged_retention_probability": unchanged_ratio,
                    "timestamp_quantum_ns": float(quantum),
                    "price_precision_digits": float(max(0, min(16, precision))),
                    "quote_transition_threshold": 10.0 ** (-max(0, precision)),
                    "batch_window_ns": float(
                        quantum if exact_second >= 0.99 else 0
                    ),
                    "duplicate_probability": min(
                        1.0, stats["duplicate_timestamp_rate"]
                    ),
                    "rate_cap_per_second": stats["active_rate_per_hour"]
                    / 3600.0,
                    "burst_window_ns": float(
                        max(
                            1,
                            int(
                                round(
                                    stats["interarrival_median_ms"] * 1_000_000
                                )
                            ),
                        )
                    ),
                }
            )
            lower = dict(values)
            upper = dict(values)
            target_rates = _feature_series(
                rows,
                "log_active_window_tick_rate_per_hour",
                transform=math.expm1,
            )
            reference_rates = _feature_series(
                reference,
                "log_active_window_tick_rate_per_hour",
                transform=math.expm1,
            )
            lower["retention_probability"] = min(
                rate,
                _quantile(target_rates, 0.25)
                / max(1e-12, _quantile(reference_rates, 0.75)),
            )
            upper["retention_probability"] = max(
                rate,
                min(
                    1.0,
                    _quantile(target_rates, 0.75)
                    / max(1e-12, _quantile(reference_rates, 0.25)),
                ),
            )
            lower["unchanged_retention_probability"] = unchanged_ratio
            upper["unchanged_retention_probability"] = unchanged_ratio
            duplicate_values = _feature_series(rows, "duplicate_timestamp_rate")
            lower["duplicate_probability"] = min(
                values["duplicate_probability"],
                _quantile(duplicate_values, 0.25),
            )
            upper["duplicate_probability"] = max(
                values["duplicate_probability"],
                _quantile(duplicate_values, 0.75),
            )
            precision_values = _feature_series(rows, "price_precision_digits")
            lower["price_precision_digits"] = float(
                max(0, math.floor(min(precision_values)))
            )
            upper["price_precision_digits"] = float(
                min(16, math.ceil(max(precision_values)))
            )
            interval_values = _feature_series(
                rows,
                "log_interarrival_median_ms",
                transform=lambda value: math.expm1(value) * 1_000_000,
            )
            lower["burst_window_ns"] = float(
                max(1, math.floor(_quantile(interval_values, 0.25)))
            )
            upper["burst_window_ns"] = float(
                max(
                    int(values["burst_window_ns"]),
                    math.ceil(_quantile(interval_values, 0.75)),
                )
            )
            status = {
                name: "unsupported" for name in OBSERVATION_PARAMETER_NAMES
            }
            for name in OBSERVATION_CALIBRATION_REQUIRED_PARAMETERS:
                status[name] = "supported"
            for name in (
                "quote_transition_threshold",
                "batch_window_ns",
                "rate_cap_per_second",
            ):
                status[name] = "bounded"
            parameter_support_counts = {
                name: (
                    int(stats["row_count"])
                    if status[name] in {"supported", "bounded"}
                    else 0
                )
                for name in OBSERVATION_PARAMETER_NAMES
            }
            reasons = {
                "retention_probability": "relative_active_time_dense_denominator",
                "unchanged_retention_probability": (
                    "state_conditioned_retention_captures_unchanged_filter"
                ),
                "timestamp_quantum_ns": "empirical_timestamp_grid_features",
                "price_precision_digits": "empirical_minimum_quote_step",
                "quote_transition_threshold": "bounded_by_empirical_price_precision",
                "batch_window_ns": "timestamp_quantization_and_batching_are_confounded",
                "duplicate_probability": "empirical_duplicate_timestamp_rate",
                "rate_cap_per_second": "upper_bound_from_active_time_rate",
                "burst_window_ns": "empirical_active_interarrival_median",
                "quiet_gap_probability": "silence_cannot_separate_archive_gap_from_omission",
                "outage_window_ns": "no_dense_pair_for_historical_outage_duration",
                "reconnect_duplicate_probability": "reconnect_identity_is_not_observed",
            }
            mechanisms = {
                "calendar_closure": "excluded_by_shared_histdata_calendar",
                "archive_gap": "unsupported_not_reclassified_as_delivery_omission",
                "retention": "supported_relative_dense_denominator",
                "unchanged_filter": "supported_as_state_conditioned_retention",
                "timestamp_quantization": "supported_empirical_grid",
                "batching": "bounded_confounded_with_quantization",
                "duplicate": "supported_empirical_duplicate_timestamp_rate",
                "rate_cap": "bounded_by_active_time_rate",
                "quiet_gap": "unsupported_archive_gap_confounding",
                "outage": "unsupported_without_paired_outage_evidence",
                "reconnect": "unsupported_without_reconnect_identity",
            }
            target_statistics: dict[str, JSONValue] = {
                "relative_retention": _round(rate, rounding_digits),
                "timestamp_exact_second_rate": _round(
                    exact_second, rounding_digits
                ),
                "duplicate_timestamp_rate": _round(
                    stats["duplicate_timestamp_rate"], rounding_digits
                ),
                "update_mix": {
                    state_name: _round(stats[state_name], rounding_digits)
                    for state_name in OBSERVATION_UPDATE_STATES
                },
                "session_mix": {
                    session: _round(stats[session], rounding_digits)
                    for session in OBSERVATION_CALIBRATION_SESSIONS
                },
                "conditioning_support": {
                    "update_type": "supported",
                    "session": "supported",
                    "timestamp_precision": "supported",
                    "spread_state": "unsupported_no_conditional_epoch_denominator",
                    "activity_state": "unsupported_no_conditional_epoch_denominator",
                    "volatility_state": "unsupported_no_conditional_epoch_denominator",
                },
                "target_row_count": int(stats["row_count"]),
                "reference_row_count": int(reference_stats["row_count"]),
                "uncertainty_basis": ("monthly_interquartile_empirical_bounds"),
            }
            result.append(
                ObservationCalibrationTargetV2(
                    symbol=symbol,
                    epoch_label=epoch.label,
                    reference_epoch_label=reference_epoch_label,
                    calibration_end_period=calibration_end_period,
                    parameter_values={
                        name: _round(value, rounding_digits)
                        for name, value in values.items()
                    },
                    parameter_lower_bounds={
                        name: _round(value, rounding_digits)
                        for name, value in lower.items()
                    },
                    parameter_upper_bounds={
                        name: _round(value, rounding_digits)
                        for name, value in upper.items()
                    },
                    parameter_support_counts=parameter_support_counts,
                    parameter_status=status,
                    parameter_reasons=reasons,
                    target_statistics=target_statistics,
                    mechanism_diagnostics=mechanisms,
                    source_evidence_ids=tuple(
                        item.evidence_id for item in (*rows, *reference)
                    ),
                    source_hashes=tuple(
                        item.source_artifact_sha256
                        for item in (*rows, *reference)
                    ),
                )
            )
    return tuple(result)


def _build_fit_evidence(
    targets: Sequence[ObservationCalibrationTargetV2],
    *,
    epoch_definition: FeedEpochDefinitionV2,
    corpus_hash: str,
) -> tuple[ObservationFitEvidenceV1, ...]:
    result: list[ObservationFitEvidenceV1] = []
    for target in targets:
        target_update = _json_float_mapping(
            target.target_statistics["update_mix"]
        )
        target_session = _json_float_mapping(
            target.target_statistics["session_mix"]
        )
        reference_target = next(
            item
            for item in targets
            if item.symbol == target.symbol
            and item.epoch_label == target.reference_epoch_label
        )
        reference_update = _json_float_mapping(
            reference_target.target_statistics["update_mix"]
        )
        reference_session = _json_float_mapping(
            reference_target.target_statistics["session_mix"]
        )
        desired = _conditioned_retention_grid(
            _finite_float(
                target.target_statistics["relative_retention"],
                "relative_retention",
            ),
            target_update=target_update,
            reference_update=reference_update,
            target_session=target_session,
            reference_session=reference_session,
        )
        epoch = next(
            item
            for item in epoch_definition.epochs
            if item.label == target.epoch_label
        )
        supported_names = tuple(
            name
            for name in OBSERVATION_PARAMETER_NAMES
            if target.parameter_status[name] == "supported"
        )
        for state in OBSERVATION_UPDATE_STATES:
            for session in OBSERVATION_CALIBRATION_SESSIONS:
                values = {
                    name: target.parameter_values[name]
                    for name in supported_names
                }
                lowers = {
                    name: target.parameter_lower_bounds[name]
                    for name in supported_names
                }
                uppers = {
                    name: target.parameter_upper_bounds[name]
                    for name in supported_names
                }
                total_retention = desired[(state, session)]
                retention = total_retention
                values["retention_probability"] = retention
                base_retention = max(
                    1e-12, target.parameter_values["retention_probability"]
                )
                lowers["retention_probability"] = max(
                    0.0,
                    min(
                        retention,
                        retention
                        * target.parameter_lower_bounds["retention_probability"]
                        / base_retention,
                    ),
                )
                uppers["retention_probability"] = min(
                    1.0,
                    max(
                        retention,
                        retention
                        * target.parameter_upper_bounds["retention_probability"]
                        / base_retention,
                    ),
                )
                support = min(
                    MAX_OBSERVATION_INPUT_EVENTS,
                    max(
                        1,
                        int(
                            _finite_float(
                                target.target_statistics["target_row_count"],
                                "target_row_count",
                            )
                            * target_update[state]
                            * target_session[session]
                        ),
                    ),
                )
                supports = {name: support for name in supported_names}
                bases = {
                    name: target.parameter_reasons[name]
                    for name in supported_names
                }
                provenance = {
                    name: (
                        "feed_epoch_v2.feature_values",
                        f"calibration_target.{target.target_id}.{name}",
                    )
                    for name in supported_names
                }
                result.append(
                    ObservationFitEvidenceV1(
                        context=ObservationContextV1(
                            symbol=target.symbol,
                            epoch_id=target.epoch_label,
                            state=state,
                            session=session,
                        ),
                        period=target.calibration_end_period,
                        start_timestamp_ns=epoch.start_timestamp_utc_ms
                        * 1_000_000,
                        end_timestamp_ns=epoch.end_timestamp_utc_ms * 1_000_000,
                        source_evidence_id=target.target_id,
                        source_artifact_sha256=corpus_hash,
                        source_hash_basis="paired_calibration_artifact_sha256",
                        evidence_kind="paired_calibration",
                        parameter_values=values,
                        parameter_lower_bounds=lowers,
                        parameter_upper_bounds=uppers,
                        parameter_support_counts=supports,
                        parameter_basis=bases,
                        parameter_provenance=provenance,
                    )
                )
    return tuple(result)


def _evaluate_windows(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    split_periods: Mapping[str, str],
    profile: ObservationCalibrationProfileV2,
    operator: ObservationOperatorV1,
    targets: Sequence[ObservationCalibrationTargetV2],
) -> tuple[ObservationCalibrationWindowV2, ...]:
    by_source = {(item.symbol, item.period): item for item in evidence}
    result: list[ObservationCalibrationWindowV2] = []
    for split in OBSERVATION_CALIBRATION_SPLITS:
        period = split_periods[split.value]
        for symbol in sorted({item.symbol for item in targets}):
            source = by_source.get((symbol, period))
            if source is None:
                raise ValueError(
                    f"dense reference window is missing: {symbol} {period}"
                )
            _verify_dense_source(
                source, max_source_bytes=profile.max_source_bytes
            )
            for session in profile.sessions:
                rows = _sample_dense_rows(
                    source, session=session, profile=profile
                )
                for target in (
                    item for item in targets if item.symbol == symbol
                ):
                    events = _observation_input_events(
                        rows,
                        symbol=symbol,
                        epoch_label=target.epoch_label,
                        session=session,
                        source_hash=source.source_artifact_sha256,
                    )
                    window_start, window_end = _aligned_window_bounds(
                        events, operator
                    )
                    window = ReconstructionWindowV1(
                        run_id=f"observation-calibration-v2:{split.value}",
                        ensemble_member_id=target.epoch_label,
                        symbols=(symbol,),
                        core_start_ns=window_start,
                        core_end_ns=window_end,
                    )
                    applied = operator.degrade(
                        events,
                        window=window,
                        source_start=True,
                    )
                    result.append(
                        _score_window(
                            split=split,
                            period=period,
                            session=session,
                            source=source,
                            target=target,
                            events=events,
                            applied=applied,
                            operator=operator,
                            profile=profile,
                        )
                    )
    return tuple(result)


def _sample_dense_rows(
    source: FeedEpochEvidenceV2,
    *,
    session: str,
    profile: ObservationCalibrationProfileV2,
) -> tuple[tuple[int, float, float], ...]:
    import polars as pl  # pylint: disable=import-outside-toplevel

    start_hour, end_hour = _SESSION_HOURS[session]
    lazy = pl.scan_ipc(source.source_path).select("datetime", "bid", "ask")
    timestamp = pl.col("datetime").cast(pl.Int64)
    hour = (timestamp // 3_600_000) % 24
    filtered = lazy.filter((hour >= start_hour) & (hour < end_hour))
    stride = max(
        1, source.row_count // max(1, profile.max_events_per_window * 4)
    )
    sampled = (
        filtered.with_row_index("_calibration_row")
        .filter((pl.col("_calibration_row") % stride) == 0)
        .head(profile.max_events_per_window)
        .select("datetime", "bid", "ask")
        .collect(engine="streaming")
    )
    rows = tuple(
        (int(timestamp_ms), float(bid), float(ask))
        for timestamp_ms, bid, ask in sampled.iter_rows()
        if float(bid) > 0 and float(ask) >= float(bid)
    )
    if len(rows) < profile.minimum_events_per_window:
        raise ValueError(
            f"dense reference window is too short: {source.symbol} "
            f"{source.period} {session} ({len(rows)})"
        )
    return rows


def _verify_dense_source(
    source: FeedEpochEvidenceV2, *, max_source_bytes: int
) -> None:
    """Verify selected dense bytes against the immutable epoch evidence."""
    path = Path(source.source_path)
    if source.source_size_bytes > max_source_bytes:
        raise ValueError(
            "dense reference source exceeds calibration byte bound"
        )
    if path.stat().st_size != source.source_size_bytes:
        raise ValueError(
            "dense reference source size differs from epoch evidence"
        )
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    actual = "sha256:" + digest.hexdigest()
    if actual != source.source_artifact_sha256:
        raise ValueError(
            "dense reference source hash differs from epoch evidence"
        )


def _observation_input_events(
    rows: Sequence[tuple[int, float, float]],
    *,
    symbol: str,
    epoch_label: str,
    session: str,
    source_hash: str,
) -> tuple[ObservationInputEventV1, ...]:
    result: list[ObservationInputEventV1] = []
    previous: tuple[float, float] | None = None
    for ordinal, (timestamp_ms, bid, ask) in enumerate(rows):
        state = _update_state(previous, bid, ask)
        source_event_id = _stable_id(
            "calibration-market-event",
            {
                "source_hash": source_hash,
                "epoch_label": epoch_label,
                "session": session,
                "ordinal": ordinal,
                "timestamp_ms": timestamp_ms,
                "bid": bid,
                "ask": ask,
            },
        )
        result.append(
            ObservationInputEventV1(
                source_event_id=source_event_id,
                symbol=symbol,
                event_time_ns=timestamp_ms * 1_000_000,
                event_sequence=ordinal,
                bid=bid,
                ask=ask,
                context=ObservationContextV1(
                    symbol=symbol,
                    epoch_id=epoch_label,
                    state=state,
                    session=session,
                ),
            )
        )
        previous = (bid, ask)
    return tuple(result)


def _score_window(
    *,
    split: BenchmarkSplitKind,
    period: str,
    session: str,
    source: FeedEpochEvidenceV2,
    target: ObservationCalibrationTargetV2,
    events: Sequence[ObservationInputEventV1],
    applied: ObservationApplicationResultV1,
    operator: ObservationOperatorV1,
    profile: ObservationCalibrationProfileV2,
) -> ObservationCalibrationWindowV2:
    outputs_by_source: Counter[str] = Counter(
        item.source_event_id for item in applied.output_events
    )
    retained = [
        item for item in events if outputs_by_source[item.source_event_id]
    ]
    expected_retention = statistics.fmean(
        _event_retention_probability(operator, item) for item in events
    )
    duplicate_target = target.parameter_values["duplicate_probability"]
    quantum = int(target.parameter_values["timestamp_quantum_ns"])
    timestamp_target = (
        1.0
        if quantum >= 1_000_000_000
        else (1_000_000_000 / max(1, quantum)) ** -1
    )
    expected_update_counts = Counter(
        {
            state: sum(
                _event_retention_probability(operator, item)
                for item in events
                if item.context.state == state
            )
            for state in OBSERVATION_UPDATE_STATES
        }
    )
    expected_update_total = sum(expected_update_counts.values())
    expected_update = {
        state: expected_update_counts[state] / max(1e-12, expected_update_total)
        for state in OBSERVATION_UPDATE_STATES
    }
    observed_update_counts = Counter(
        cast(str, item.context.state) for item in retained
    )
    observed_update = {
        state: observed_update_counts[state] / max(1, len(retained))
        for state in OBSERVATION_UPDATE_STATES
    }
    update_l1 = sum(
        abs(observed_update[state] - expected_update[state])
        for state in OBSERVATION_UPDATE_STATES
    )
    metrics = {
        "retention_probability": len(retained) / len(events),
        "duplicate_probability": (
            max(0, len(applied.output_events) - len(retained))
            / max(1, len(retained))
        ),
        "timestamp_exact_second_rate": (
            sum(
                item.observed_time_ns % 1_000_000_000 == 0
                for item in applied.output_events
            )
            / max(1, len(applied.output_events))
        ),
        "update_mix_l1": update_l1,
    }
    targets = {
        "retention_probability": expected_retention,
        "duplicate_probability": duplicate_target,
        "timestamp_exact_second_rate": timestamp_target,
        "update_mix_l1": 0.0,
    }
    tolerances = {
        "retention_probability": max(
            profile.retention_tolerance,
            _binomial_tolerance(expected_retention, len(events)),
        ),
        "duplicate_probability": max(
            profile.duplicate_tolerance,
            _binomial_tolerance(duplicate_target, max(1, len(retained))),
        ),
        "timestamp_exact_second_rate": profile.timestamp_tolerance,
        "update_mix_l1": profile.update_mix_l1_tolerance,
    }
    errors = {name: abs(metrics[name] - targets[name]) for name in metrics}
    failures = tuple(
        f"{name}_outside_tolerance"
        for name in metrics
        if errors[name] > tolerances[name]
    )
    transformations = Counter(
        value
        for item in applied.output_events
        for value in item.transformations
    )
    digits = profile.rounding_digits
    return ObservationCalibrationWindowV2(
        split_kind=split,
        symbol=source.symbol,
        period=period,
        session=session,
        epoch_label=target.epoch_label,
        source_artifact_sha256=source.source_artifact_sha256,
        input_count=len(events),
        output_count=len(applied.output_events),
        retained_source_count=len(retained),
        target_metrics={
            name: _round(value, digits) for name, value in targets.items()
        },
        observed_metrics={
            name: _round(value, digits) for name, value in metrics.items()
        },
        absolute_errors={
            name: _round(value, digits) for name, value in errors.items()
        },
        tolerances=tolerances,
        reason_counts=applied.reason_counts,
        transformation_counts=dict(transformations),
        passed=not failures,
        failure_reasons=failures,
    )


def _readiness_reasons(
    targets: Sequence[ObservationCalibrationTargetV2],
    windows: Sequence[ObservationCalibrationWindowV2],
    profile: ObservationCalibrationProfileV2,
    *,
    runtime_seconds: float,
    peak_memory_bytes: int,
) -> tuple[str, ...]:
    reasons: list[str] = []
    for target in targets:
        if target.parameter_reasons["retention_probability"] == (
            "identity_without_dense_denominator"
        ):
            reasons.append("identity_retention_without_dense_denominator")
        for name in OBSERVATION_CALIBRATION_REQUIRED_PARAMETERS:
            if target.parameter_status[name] != "supported":
                reasons.append(
                    f"required_parameter_unsupported:{target.symbol}:{target.epoch_label}:{name}"
                )
    expected_splits = set(_SPLIT_KEYS)
    actual_splits = {item.split_kind.value for item in windows}
    if actual_splits != expected_splits:
        reasons.append("blocked_time_split_coverage_incomplete")
    expected_cells = {
        (
            split.value,
            target.symbol,
            target.epoch_label,
            session,
            profile.split_periods[split.value],
        )
        for split in OBSERVATION_CALIBRATION_SPLITS
        for target in targets
        for session in profile.sessions
    }
    actual_cells = {
        (
            item.split_kind.value,
            item.symbol,
            item.epoch_label,
            item.session,
            item.period,
        )
        for item in windows
    }
    if actual_cells != expected_cells or len(windows) != len(expected_cells):
        reasons.append("blocked_time_cell_coverage_incomplete")
    years = {value[:4] for value in profile.split_periods.values()}
    if len(years) < 3:
        reasons.append("blocked_time_splits_do_not_span_three_years")
    holdout = [
        item
        for item in windows
        if item.split_kind is BenchmarkSplitKind.FINAL_HOLDOUT
    ]
    if not holdout or any(not item.passed for item in holdout):
        reasons.append("final_holdout_failed")
    if any(
        item.input_count > profile.max_events_per_window for item in windows
    ):
        reasons.append("window_resource_bound_exceeded")
    if runtime_seconds > profile.max_runtime_seconds:
        reasons.append("campaign_runtime_bound_exceeded")
    if peak_memory_bytes > profile.max_peak_memory_bytes:
        reasons.append("campaign_memory_bound_exceeded")
    return tuple(dict.fromkeys(reasons))


def _automatic_split_periods(
    evidence: Sequence[FeedEpochEvidenceV2],
    definition: FeedEpochDefinitionV2,
) -> dict[str, str]:
    reference = definition.epochs[-1]
    by_period: dict[str, set[str]] = defaultdict(set)
    for item in evidence:
        midpoint = (
            item.start_timestamp_utc_ms + item.end_timestamp_utc_ms
        ) // 2
        assignment = definition.assign(
            symbol=item.symbol, timestamp_utc_ms=midpoint
        )
        if (
            assignment.label == reference.label
            and assignment.assignment_kind == "epoch"
        ):
            by_period[item.period].add(item.symbol)
    periods = sorted(
        period
        for period, symbols in by_period.items()
        if symbols == set(definition.symbols)
    )
    if len(periods) < 36 or len({value[:4] for value in periods}) < 3:
        raise ValueError("dense reference epoch lacks three blocked years")
    indexes = (len(periods) // 4, len(periods) * 2 // 3, len(periods) * 9 // 10)
    selected = [periods[min(len(periods) - 1, index)] for index in indexes]
    for index in range(1, len(selected)):
        while selected[index][:4] == selected[index - 1][:4]:
            position = periods.index(selected[index]) + 1
            if position >= len(periods):
                raise ValueError("unable to create three-year blocked split")
            selected[index] = periods[position]
    return dict(zip(_SPLIT_KEYS, selected, strict=True))


def _aggregate_epoch_rows(
    rows: Sequence[FeedEpochEvidenceV2],
) -> dict[str, float]:
    def median_feature(name: str, default: float = 0.0) -> float:
        values = [
            item.feature_values[name]
            for item in rows
            if name in item.feature_values
        ]
        return statistics.median(values) if values else default

    result = {
        "row_count": float(sum(item.row_count for item in rows)),
        "active_rate_per_hour": math.expm1(
            median_feature("log_active_window_tick_rate_per_hour")
        ),
        "timestamp_exact_second_rate": median_feature(
            "timestamp_exact_second_rate"
        ),
        "timestamp_last_digit_entropy": median_feature(
            "timestamp_last_digit_entropy", 1.0
        ),
        "price_precision_digits": median_feature("price_precision_digits", 8.0),
        "duplicate_timestamp_rate": median_feature("duplicate_timestamp_rate"),
        "interarrival_median_ms": math.expm1(
            median_feature("log_interarrival_median_ms")
        ),
    }
    for state, feature in _UPDATE_FEATURES.items():
        result[state] = median_feature(feature)
    update_total = sum(result[state] for state in OBSERVATION_UPDATE_STATES)
    for state in OBSERVATION_UPDATE_STATES:
        result[state] = result[state] / max(1e-12, update_total)
    for session, feature in _SESSION_FEATURES.items():
        result[session] = median_feature(feature)
    session_total = sum(
        result[session] for session in OBSERVATION_CALIBRATION_SESSIONS
    )
    for session in OBSERVATION_CALIBRATION_SESSIONS:
        result[session] = result[session] / max(1e-12, session_total)
    return result


def _feature_series(
    rows: Sequence[FeedEpochEvidenceV2],
    name: str,
    *,
    transform: Callable[[float], float] | None = None,
) -> tuple[float, ...]:
    values = tuple(
        float(item.feature_values[name])
        for item in rows
        if name in item.feature_values
    )
    if not values:
        raise ValueError(f"calibration feature is unavailable: {name}")
    if transform is None:
        return values
    return tuple(float(transform(value)) for value in values)


def _quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("calibration quantile requires observations")
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _conditioned_retention_grid(
    base_retention: float,
    *,
    target_update: Mapping[str, float],
    reference_update: Mapping[str, float],
    target_session: Mapping[str, float],
    reference_session: Mapping[str, float],
) -> dict[tuple[str, str], float]:
    ratios: dict[tuple[str, str], float] = {}
    weights: dict[tuple[str, str], float] = {}
    for state in OBSERVATION_UPDATE_STATES:
        for session in OBSERVATION_CALIBRATION_SESSIONS:
            key = (state, session)
            ratios[key] = math.sqrt(
                target_update[state]
                / max(1e-9, reference_update[state])
                * target_session[session]
                / max(1e-9, reference_session[session])
            )
            weights[key] = reference_update[state] * reference_session[session]
    lower, upper = 0.0, 1000.0
    for _ in range(80):
        scale = (lower + upper) / 2.0
        achieved = sum(
            weights[key] * min(1.0, scale * ratios[key]) for key in ratios
        )
        if achieved < base_retention:
            lower = scale
        else:
            upper = scale
    return {key: min(1.0, upper * ratio) for key, ratio in ratios.items()}


def _event_retention_probability(
    operator: ObservationOperatorV1,
    event: ObservationInputEventV1,
) -> float:
    stratum, _ = operator.resolve_stratum(event.context)
    value = stratum.effective_value("retention_probability")
    if event.context.state == "update_unchanged":
        value *= stratum.effective_value("unchanged_retention_probability")
    return float(value)


def _aligned_window_bounds(
    events: Sequence[ObservationInputEventV1],
    operator: ObservationOperatorV1,
) -> tuple[int, int]:
    """Return bounds aligned to every fitted timestamp/batch quantum."""
    alignments = [1]
    for stratum in operator.strata:
        for name in ("timestamp_quantum_ns", "batch_window_ns"):
            value = int(stratum.effective_value(name))
            if value > 1:
                alignments.append(value)
    alignment = math.lcm(*alignments)
    start = events[0].event_time_ns // alignment * alignment
    end = (events[-1].event_time_ns // alignment + 1) * alignment
    return start, end


def _timestamp_grid_ns(outputs: Sequence[Any]) -> int:
    timestamps = sorted({int(item.observed_time_ns) for item in outputs})
    if len(timestamps) < 2:
        return 1
    result = 0
    for left, right in zip(timestamps, timestamps[1:], strict=False):
        result = math.gcd(result, right - left)
    return max(1, result)


def _quantum_from_timestamp_features(
    exact_second: float, entropy: float
) -> int:
    if exact_second >= 0.95:
        return 1_000_000_000
    if entropy <= 0.05:
        return 100_000_000
    if entropy <= 0.75:
        return 10_000_000
    return 1_000_000


def _binomial_tolerance(probability: float, count: int) -> float:
    """Return a predeclared finite-window three-sigma acceptance width."""
    return min(
        1.0,
        3.0 * math.sqrt(probability * (1.0 - probability) / max(1, count))
        + 3.0 / max(1, count),
    )


def _update_state(
    previous: tuple[float, float] | None,
    bid: float,
    ask: float,
) -> str:
    if previous is None:
        return "update_joint"
    bid_changed = bid != previous[0]
    ask_changed = ask != previous[1]
    if bid_changed and ask_changed:
        return "update_joint"
    if bid_changed:
        return "update_bid_only"
    if ask_changed:
        return "update_ask_only"
    return "update_unchanged"


def _json_float_mapping(value: JSONValue) -> dict[str, float]:
    return {
        str(key): _finite_float(item, str(key))
        for key, item in _mapping(value).items()
    }


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _sha256_text(value: str) -> str:
    text = str(value)
    raw = text.removeprefix("sha256:")
    if len(raw) != 64 or any(char not in "0123456789abcdef" for char in raw):
        raise ValueError("invalid SHA-256 value")
    return "sha256:" + raw


def _required_text(value: Any, name: str) -> str:
    result = str(value).strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _valid_month(value: str) -> bool:
    return len(value) == 6 and value.isdigit() and 1 <= int(value[4:]) <= 12


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _calibration_parameter_value(name: str, value: Any) -> float:
    result = _finite_float(value, name)
    if result < 0.0:
        raise ValueError(f"{name} must be non-negative")
    if name in _PROBABILITY_PARAMETERS and result > 1.0:
        raise ValueError(f"{name} must not exceed one")
    if name in _INTEGER_PARAMETERS and not result.is_integer():
        raise ValueError(f"{name} must be integral")
    if name == "price_precision_digits" and result > 16.0:
        raise ValueError("price_precision_digits exceeds sixteen")
    return result


def _round(value: float, digits: int) -> float:
    return round(_finite_float(value, "value"), digits)


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("expected a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _string_mapping(value: Any) -> dict[str, str]:
    return {str(key): str(item) for key, item in _mapping(value).items()}


def _float_mapping(value: Any) -> dict[str, float]:
    return {
        str(key): _finite_float(item, str(key))
        for key, item in _mapping(value).items()
    }


def _finite_mapping(value: Mapping[str, Any]) -> dict[str, float]:
    return {
        str(key): _finite_float(item, str(key)) for key, item in value.items()
    }


def _int_mapping(value: Any) -> dict[str, int]:
    mapping = value if isinstance(value, Mapping) else _mapping(value)
    result: dict[str, int] = {}
    for key, item in mapping.items():
        integer = _strict_int(item, str(key))
        if integer < 0:
            raise ValueError("count values must be non-negative")
        result[str(key)] = integer
    return result

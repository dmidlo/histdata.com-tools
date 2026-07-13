"""Versioned historical feed-observation operators.

This module models delivery technology separately from market-event
generation.  A fitted operator consumes bounded, provenance-bearing evidence
and renders a market-event surface into delivery observations without
mutating the source event identities.

Version-one contracts are strict and deterministic.  Semantic changes to
parameter meaning, fallback order, event identity, or application behavior
require a new schema version.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from statistics import median
from typing import TYPE_CHECKING, Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.streaming import ReconstructionWindowV1

if TYPE_CHECKING:
    from histdatacom.data_analytics.feed_epochs import (
        FeedEpochDefinitionV1,
        FeedEpochEvidenceV1,
    )

OBSERVATION_CONTEXT_SCHEMA_VERSION = "histdatacom.observation-context.v1"
OBSERVATION_FIT_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.observation-fit-evidence.v1"
)
OBSERVATION_PARAMETER_SCHEMA_VERSION = (
    "histdatacom.observation-parameter-estimate.v1"
)
OBSERVATION_FIT_CONFIG_SCHEMA_VERSION = (
    "histdatacom.observation-operator-fit-config.v1"
)
OBSERVATION_STRATUM_SCHEMA_VERSION = "histdatacom.observation-stratum.v1"
OBSERVATION_FIT_DIAGNOSTICS_SCHEMA_VERSION = (
    "histdatacom.observation-fit-diagnostics.v1"
)
OBSERVATION_OPERATOR_SCHEMA_VERSION = "histdatacom.observation-operator.v1"
OBSERVATION_INPUT_EVENT_SCHEMA_VERSION = (
    "histdatacom.observation-input-event.v1"
)
OBSERVATION_OUTPUT_EVENT_SCHEMA_VERSION = (
    "histdatacom.observation-output-event.v1"
)
OBSERVATION_CARRY_STATE_SCHEMA_VERSION = (
    "histdatacom.observation-carry-state.v1"
)
OBSERVATION_APPLICATION_RESULT_SCHEMA_VERSION = (
    "histdatacom.observation-application-result.v1"
)

MAX_OBSERVATION_EVIDENCE = 4096
MAX_OBSERVATION_STRATA = 512
MAX_OBSERVATION_PARAMETERS = 32
MAX_OBSERVATION_INPUT_EVENTS = 250_000
MAX_OBSERVATION_OUTPUTS_PER_INPUT = 3
MAX_OBSERVATION_DIAGNOSTIC_SAMPLES = 128
MAX_OBSERVATION_PROVENANCE_PATHS = 64
MAX_OBSERVATION_TEXT_LENGTH = 1024
MAX_OBSERVATION_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_OBSERVATION_WINDOW_ALIGNMENT_NS = 86_400_000_000_000
MAX_OBSERVATION_DURATION_NS = 31 * 86_400_000_000_000
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

OBSERVATION_PARAMETER_NAMES = (
    "retention_probability",
    "unchanged_retention_probability",
    "timestamp_quantum_ns",
    "price_precision_digits",
    "quote_transition_threshold",
    "batch_window_ns",
    "duplicate_probability",
    "rate_cap_per_second",
    "burst_window_ns",
    "quiet_gap_probability",
    "outage_window_ns",
    "reconnect_duplicate_probability",
)

OBSERVATION_BACKOFF_LEVELS = (
    "symbol_epoch_state_session_event",
    "symbol_epoch_state_session",
    "symbol_epoch_state",
    "symbol_epoch",
    "epoch",
    "global",
)

OBSERVATION_CARRY_FIELDS = (
    "last_source_time_ns",
    "last_observed_time_ns",
    "last_bid",
    "last_ask",
    "rate_bucket_start_ns",
    "rate_bucket_count",
    "outage_bucket_start_ns",
    "outage_active",
    "reconnect_pending",
)

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
_NONNEGATIVE_PARAMETERS = set(OBSERVATION_PARAMETER_NAMES).difference(
    _PROBABILITY_PARAMETERS
)
_NEUTRAL_PARAMETER_VALUES: dict[str, float] = {
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
_EVIDENCE_KINDS = {
    "canonical_fingerprint",
    "paired_calibration",
    "controlled_fixture",
}
_SOURCE_HASH_BASES = {
    "canonical_fingerprint_id",
    "canonical_fingerprint_aggregate_id",
    "persisted_fingerprint_artifact_sha256",
    "paired_calibration_artifact_sha256",
    "controlled_fixture_sha256",
}
_STRATUM_STATUSES = {"ready", "limited", "unsupported"}
_APPLICATION_REASONS = {
    "retained",
    "outage",
    "thinning",
    "unchanged_quote_filter",
    "quote_transition_filter",
    "rate_cap",
}
_OUTPUT_TRANSFORMATIONS = {
    "timestamp_quantized",
    "batched",
    "price_quantized",
    "reconnect_duplicate",
    "duplicated",
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PERIOD_RE = re.compile(r"^\d{4}(?:\d{2})?$")


@dataclass(frozen=True, slots=True)
class ObservationContextV1:
    """Conditioning coordinates used by the explicit fallback hierarchy."""

    symbol: str
    epoch_id: str
    state: str | None = None
    session: str | None = None
    event_tag: str | None = None
    schema_version: str = OBSERVATION_CONTEXT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_CONTEXT_SCHEMA_VERSION:
            raise ValueError("unsupported observation context schema")
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(self, "epoch_id", _required_text(self.epoch_id))
        for name in ("state", "session", "event_tag"):
            object.__setattr__(
                self,
                name,
                _optional_context_value(getattr(self, name)),
            )

    def pattern_for_level(self, level: str) -> dict[str, str] | None:
        """Return the concrete pattern for a supported fallback level."""
        if level not in OBSERVATION_BACKOFF_LEVELS:
            raise ValueError("unsupported observation backoff level")
        if level == "global":
            return {}
        if level == "epoch":
            return {"epoch_id": self.epoch_id}
        base = {"symbol": self.symbol, "epoch_id": self.epoch_id}
        if level == "symbol_epoch":
            return base
        if self.state is None:
            return None
        base["state"] = self.state
        if level == "symbol_epoch_state":
            return base
        if self.session is None:
            return None
        base["session"] = self.session
        if level == "symbol_epoch_state_session":
            return base
        if self.event_tag is None:
            return None
        base["event_tag"] = self.event_tag
        return base

    def key_for_level(self, level: str) -> str | None:
        """Return a deterministic key for one fallback level."""
        pattern = self.pattern_for_level(level)
        if pattern is None:
            return None
        return _stratum_key(level, pattern)

    def candidate_keys(
        self, levels: Sequence[str] = OBSERVATION_BACKOFF_LEVELS
    ) -> tuple[str, ...]:
        """Return ordered, de-duplicated resolution candidates."""
        result: list[str] = []
        for level in levels:
            key = self.key_for_level(level)
            if key is not None and key not in result:
                result.append(key)
        return tuple(result)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible context coordinates."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "epoch_id": self.epoch_id,
            "state": self.state,
            "session": self.session,
            "event_tag": self.event_tag,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationContextV1":
        """Read a strict version-one context."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            symbol=str(data.get("symbol", "")),
            epoch_id=str(data.get("epoch_id", "")),
            state=_mapping_optional_text(data, "state"),
            session=_mapping_optional_text(data, "session"),
            event_tag=_mapping_optional_text(data, "event_tag"),
        )


@dataclass(frozen=True, slots=True)
class ObservationFitEvidenceV1:
    """One bounded parameter projection used to fit an operator."""

    context: ObservationContextV1
    period: str
    start_timestamp_ns: int
    end_timestamp_ns: int
    source_evidence_id: str
    source_artifact_sha256: str
    source_hash_basis: str
    evidence_kind: str
    parameter_values: Mapping[str, float]
    parameter_lower_bounds: Mapping[str, float]
    parameter_upper_bounds: Mapping[str, float]
    parameter_support_counts: Mapping[str, int]
    parameter_basis: Mapping[str, str]
    parameter_provenance: Mapping[str, tuple[str, ...]]
    evidence_id: str = ""
    schema_version: str = OBSERVATION_FIT_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_FIT_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported observation fit evidence schema")
        if not isinstance(self.context, ObservationContextV1):
            raise ValueError("observation evidence requires a v1 context")
        period = _required_text(self.period)
        if not _valid_period(period):
            raise ValueError(
                "observation evidence period must use YYYY or YYYYMM"
            )
        start = _bounded_int(
            self.start_timestamp_ns,
            "start_timestamp_ns",
            INT64_MIN,
            INT64_MAX,
        )
        end = _bounded_int(
            self.end_timestamp_ns,
            "end_timestamp_ns",
            INT64_MIN,
            INT64_MAX,
        )
        if end < start:
            raise ValueError("observation evidence end precedes start")
        source_id = _required_text(self.source_evidence_id)
        source_hash = _required_sha256_id(
            self.source_artifact_sha256,
            "source_artifact_sha256",
        )
        source_hash_basis = _required_text(self.source_hash_basis)
        if source_hash_basis not in _SOURCE_HASH_BASES:
            raise ValueError("unsupported observation source hash basis")
        evidence_kind = _required_text(self.evidence_kind)
        if evidence_kind not in _EVIDENCE_KINDS:
            raise ValueError("unsupported observation evidence kind")
        names = tuple(sorted(self.parameter_values))
        if not names or len(names) > MAX_OBSERVATION_PARAMETERS:
            raise ValueError("observation evidence parameters are unbounded")
        if any(name not in OBSERVATION_PARAMETER_NAMES for name in names):
            raise ValueError("unsupported observation parameter")
        if any(
            set(mapping) != set(names)
            for mapping in (
                self.parameter_lower_bounds,
                self.parameter_upper_bounds,
                self.parameter_support_counts,
                self.parameter_basis,
                self.parameter_provenance,
            )
        ):
            raise ValueError("observation parameter evidence keys differ")
        values: dict[str, float] = {}
        lowers: dict[str, float] = {}
        uppers: dict[str, float] = {}
        supports: dict[str, int] = {}
        bases: dict[str, str] = {}
        provenance: dict[str, tuple[str, ...]] = {}
        for name in names:
            value = _parameter_value(name, self.parameter_values[name])
            lower = _parameter_value(name, self.parameter_lower_bounds[name])
            upper = _parameter_value(name, self.parameter_upper_bounds[name])
            if not lower <= value <= upper:
                raise ValueError("parameter estimate lies outside uncertainty")
            support = _bounded_int(
                self.parameter_support_counts[name],
                f"{name} support",
                0,
                MAX_OBSERVATION_INPUT_EVENTS,
            )
            basis = _required_text(self.parameter_basis[name])
            paths = tuple(
                dict.fromkeys(
                    _required_text(path)
                    for path in self.parameter_provenance[name]
                )
            )
            if not paths or len(paths) > MAX_OBSERVATION_PROVENANCE_PATHS:
                raise ValueError("parameter provenance is empty or unbounded")
            values[name] = value
            lowers[name] = lower
            uppers[name] = upper
            supports[name] = support
            bases[name] = basis
            provenance[name] = paths
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "start_timestamp_ns", start)
        object.__setattr__(self, "end_timestamp_ns", end)
        object.__setattr__(self, "source_evidence_id", source_id)
        object.__setattr__(self, "source_artifact_sha256", source_hash)
        object.__setattr__(self, "source_hash_basis", source_hash_basis)
        object.__setattr__(self, "evidence_kind", evidence_kind)
        object.__setattr__(self, "parameter_values", values)
        object.__setattr__(self, "parameter_lower_bounds", lowers)
        object.__setattr__(self, "parameter_upper_bounds", uppers)
        object.__setattr__(self, "parameter_support_counts", supports)
        object.__setattr__(self, "parameter_basis", bases)
        object.__setattr__(self, "parameter_provenance", provenance)
        expected = _stable_id("observation-evidence", self.identity_payload())
        supplied = str(self.evidence_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    @property
    def support_count(self) -> int:
        """Return the largest declared parameter support."""
        return max(self.parameter_support_counts.values(), default=0)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return complete semantic evidence identity."""
        return {
            "schema_version": self.schema_version,
            "context": self.context.to_dict(),
            "period": self.period,
            "start_timestamp_ns": self.start_timestamp_ns,
            "end_timestamp_ns": self.end_timestamp_ns,
            "source_evidence_id": self.source_evidence_id,
            "source_artifact_sha256": self.source_artifact_sha256,
            "source_hash_basis": self.source_hash_basis,
            "evidence_kind": self.evidence_kind,
            "parameter_values": dict(self.parameter_values),
            "parameter_lower_bounds": dict(self.parameter_lower_bounds),
            "parameter_upper_bounds": dict(self.parameter_upper_bounds),
            "parameter_support_counts": dict(self.parameter_support_counts),
            "parameter_basis": dict(self.parameter_basis),
            "parameter_provenance": {
                name: list(paths)
                for name, paths in sorted(self.parameter_provenance.items())
            },
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible evidence."""
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationFitEvidenceV1":
        """Read and verify fit evidence."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            context=ObservationContextV1.from_dict(
                _mapping(data.get("context"))
            ),
            period=str(data.get("period", "")),
            start_timestamp_ns=_strict_int(
                data.get("start_timestamp_ns"), "start_timestamp_ns"
            ),
            end_timestamp_ns=_strict_int(
                data.get("end_timestamp_ns"), "end_timestamp_ns"
            ),
            source_evidence_id=str(data.get("source_evidence_id", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            source_hash_basis=str(data.get("source_hash_basis", "")),
            evidence_kind=str(data.get("evidence_kind", "")),
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
            parameter_basis=_string_mapping(data.get("parameter_basis")),
            parameter_provenance={
                str(name): tuple(str(path) for path in _sequence(paths))
                for name, paths in _mapping(
                    data.get("parameter_provenance")
                ).items()
            },
            evidence_id=str(data.get("evidence_id", "")),
        )

    @classmethod
    def from_feed_epoch_evidence(
        cls,
        evidence: FeedEpochEvidenceV1,
        epoch_definition: FeedEpochDefinitionV1,
    ) -> "ObservationFitEvidenceV1":
        """Project canonical epoch evidence without claiming dense recovery."""
        if not epoch_definition.valid_for_observation_models:
            raise ValueError("feed epoch definition has not passed stability")
        midpoint_ms = (
            evidence.start_timestamp_utc_ms + evidence.end_timestamp_utc_ms
        ) // 2
        assignment = epoch_definition.assign(
            symbol=evidence.symbol,
            timestamp_utc_ms=midpoint_ms,
        )
        if assignment.assignment_kind == "out_of_scope":
            raise ValueError("feed epoch evidence is outside definition scope")
        features = evidence.feature_values
        profile = evidence.profile
        row_count = max(1, _optional_int(profile.get("row_count")) or 1)
        min_interval_ms = max(
            0.000001,
            features.get("minimum_observed_interval_ms", 0.001),
        )
        median_interval_ms = max(
            min_interval_ms,
            _optional_float(profile.get("median_interarrival_ms"))
            or min_interval_ms,
        )
        p95_interval_ms = max(
            median_interval_ms,
            _optional_float(profile.get("p95_interarrival_ms"))
            or median_interval_ms,
        )
        precision = int(round(features.get("price_precision_digits", 8.0)))
        precision = min(16, max(0, precision))
        stale_rate = _probability(features.get("stale_repeat_rate", 0.0))
        duplicate_rate = _probability(
            features.get("duplicate_timestamp_rate", 0.0)
        )
        gap_rate = _probability(features.get("suspicious_gap_rate", 0.0))
        tick_rate = (
            max(
                0.0,
                _optional_float(profile.get("tick_rate_per_hour")) or 0.0,
            )
            / 3600.0
        )
        parameter_values = {
            "retention_probability": 1.0,
            "unchanged_retention_probability": stale_rate,
            "timestamp_quantum_ns": float(round(min_interval_ms * 1_000_000)),
            "price_precision_digits": float(precision),
            "quote_transition_threshold": 10.0 ** (-precision),
            "batch_window_ns": float(round(min_interval_ms * 1_000_000)),
            "duplicate_probability": duplicate_rate,
            "rate_cap_per_second": max(
                tick_rate,
                1_000.0 / min_interval_ms,
            ),
            "burst_window_ns": float(round(median_interval_ms * 1_000_000)),
            "quiet_gap_probability": gap_rate,
            "outage_window_ns": float(round(p95_interval_ms * 1_000_000)),
            "reconnect_duplicate_probability": duplicate_rate,
        }
        parameter_values["timestamp_quantum_ns"] = max(
            1.0, parameter_values["timestamp_quantum_ns"]
        )
        bounds = _canonical_proxy_bounds(parameter_values)
        support_counts = {name: row_count for name in parameter_values}
        support_counts["retention_probability"] = 0
        bases = {
            name: "canonical_descriptive_proxy" for name in parameter_values
        }
        bases["retention_probability"] = "identity_without_dense_denominator"
        provenance = {
            name: _canonical_parameter_provenance(name)
            for name in parameter_values
        }
        return cls(
            context=ObservationContextV1(
                symbol=evidence.symbol,
                epoch_id=assignment.label,
                state=assignment.assignment_kind,
            ),
            period=evidence.period,
            start_timestamp_ns=evidence.start_timestamp_utc_ms * 1_000_000,
            end_timestamp_ns=evidence.end_timestamp_utc_ms * 1_000_000,
            source_evidence_id=evidence.evidence_id,
            source_artifact_sha256=evidence.source_artifact_sha256,
            source_hash_basis=evidence.source_hash_basis,
            evidence_kind="canonical_fingerprint",
            parameter_values=parameter_values,
            parameter_lower_bounds={
                name: pair[0] for name, pair in bounds.items()
            },
            parameter_upper_bounds={
                name: pair[1] for name, pair in bounds.items()
            },
            parameter_support_counts=support_counts,
            parameter_basis=bases,
            parameter_provenance=provenance,
        )


@dataclass(frozen=True, slots=True)
class ObservationParameterEstimateV1:
    """One fitted value with uncertainty, support, and provenance."""

    name: str
    value: float
    lower: float
    upper: float
    support_count: int
    evidence_count: int
    support_status: str
    estimation_bases: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    provenance: tuple[str, ...]
    schema_version: str = OBSERVATION_PARAMETER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_PARAMETER_SCHEMA_VERSION:
            raise ValueError("unsupported observation parameter schema")
        name = _required_text(self.name)
        if name not in OBSERVATION_PARAMETER_NAMES:
            raise ValueError("unsupported observation parameter")
        value = _parameter_value(name, self.value)
        lower = _parameter_value(name, self.lower)
        upper = _parameter_value(name, self.upper)
        if not lower <= value <= upper:
            raise ValueError("fitted parameter lies outside uncertainty")
        support = _bounded_int(
            self.support_count,
            "support_count",
            0,
            MAX_OBSERVATION_INPUT_EVENTS * MAX_OBSERVATION_EVIDENCE,
        )
        evidence_count = _bounded_int(
            self.evidence_count,
            "evidence_count",
            1,
            MAX_OBSERVATION_EVIDENCE,
        )
        status = _required_text(self.support_status)
        if status not in {"supported", "unsupported"}:
            raise ValueError("unsupported parameter support status")
        bases = _bounded_text_tuple(
            self.estimation_bases,
            "estimation basis",
            limit=MAX_OBSERVATION_PROVENANCE_PATHS,
        )
        evidence_ids = _bounded_text_tuple(self.evidence_ids, "evidence id")
        if len(evidence_ids) != evidence_count:
            raise ValueError("parameter evidence count differs from IDs")
        if any(
            _required_sha256_id(
                evidence_id,
                "evidence_id",
                prefix="observation-evidence",
            )
            != evidence_id
            for evidence_id in evidence_ids
        ):
            raise ValueError("parameter evidence IDs differ")
        if status == "supported" and support == 0:
            raise ValueError("supported parameter requires positive support")
        provenance = _bounded_text_tuple(
            self.provenance,
            "provenance",
            limit=MAX_OBSERVATION_PROVENANCE_PATHS,
        )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)
        object.__setattr__(self, "support_count", support)
        object.__setattr__(self, "evidence_count", evidence_count)
        object.__setattr__(self, "support_status", status)
        object.__setattr__(self, "estimation_bases", bases)
        object.__setattr__(self, "evidence_ids", evidence_ids)
        object.__setattr__(self, "provenance", provenance)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible parameter evidence."""
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "value": self.value,
            "lower": self.lower,
            "upper": self.upper,
            "support_count": self.support_count,
            "evidence_count": self.evidence_count,
            "support_status": self.support_status,
            "estimation_bases": list(self.estimation_bases),
            "evidence_ids": list(self.evidence_ids),
            "provenance": list(self.provenance),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationParameterEstimateV1":
        """Read one fitted parameter."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            name=str(data.get("name", "")),
            value=_finite_float(data.get("value"), "value"),
            lower=_finite_float(data.get("lower"), "lower"),
            upper=_finite_float(data.get("upper"), "upper"),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            evidence_count=_strict_int(
                data.get("evidence_count"), "evidence_count"
            ),
            support_status=str(data.get("support_status", "")),
            estimation_bases=_string_tuple(data.get("estimation_bases")),
            evidence_ids=_string_tuple(data.get("evidence_ids")),
            provenance=_string_tuple(data.get("provenance")),
        )


@dataclass(frozen=True, slots=True)
class ObservationOperatorFitConfigV1:
    """Bounded fitting, fallback, and application policy."""

    min_stratum_support: int = 16
    min_parameter_support: int = 1
    min_supported_parameters: int = 5
    max_evidence: int = MAX_OBSERVATION_EVIDENCE
    max_strata: int = MAX_OBSERVATION_STRATA
    max_input_events: int = MAX_OBSERVATION_INPUT_EVENTS
    max_outputs_per_input: int = MAX_OBSERVATION_OUTPUTS_PER_INPUT
    diagnostic_sample_limit: int = 32
    required_left_halo_ns: int = 0
    backoff_levels: tuple[str, ...] = OBSERVATION_BACKOFF_LEVELS
    rounding_digits: int = 8
    config_id: str = ""
    schema_version: str = OBSERVATION_FIT_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_FIT_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported observation fit config schema")
        _bounded_int(
            self.min_stratum_support,
            "min_stratum_support",
            1,
            MAX_OBSERVATION_INPUT_EVENTS,
        )
        _bounded_int(
            self.min_parameter_support,
            "min_parameter_support",
            1,
            MAX_OBSERVATION_INPUT_EVENTS,
        )
        _bounded_int(
            self.min_supported_parameters,
            "min_supported_parameters",
            1,
            len(OBSERVATION_PARAMETER_NAMES),
        )
        _bounded_int(
            self.max_evidence, "max_evidence", 1, MAX_OBSERVATION_EVIDENCE
        )
        _bounded_int(self.max_strata, "max_strata", 1, MAX_OBSERVATION_STRATA)
        _bounded_int(
            self.max_input_events,
            "max_input_events",
            1,
            MAX_OBSERVATION_INPUT_EVENTS,
        )
        _bounded_int(
            self.max_outputs_per_input,
            "max_outputs_per_input",
            1,
            MAX_OBSERVATION_OUTPUTS_PER_INPUT,
        )
        _bounded_int(
            self.diagnostic_sample_limit,
            "diagnostic_sample_limit",
            1,
            MAX_OBSERVATION_DIAGNOSTIC_SAMPLES,
        )
        _bounded_int(
            self.required_left_halo_ns,
            "required_left_halo_ns",
            0,
            MAX_OBSERVATION_WINDOW_ALIGNMENT_NS,
        )
        levels = tuple(dict.fromkeys(self.backoff_levels))
        if (
            not levels
            or levels[-1] != "global"
            or any(level not in OBSERVATION_BACKOFF_LEVELS for level in levels)
        ):
            raise ValueError("observation backoff levels must end in global")
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between zero and sixteen")
        object.__setattr__(self, "backoff_levels", levels)
        expected = _stable_id("observation-fit-config", self.identity_payload())
        supplied = str(self.config_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("config_id does not match deterministic identity")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic fit/application configuration."""
        return {
            "schema_version": self.schema_version,
            "min_stratum_support": self.min_stratum_support,
            "min_parameter_support": self.min_parameter_support,
            "min_supported_parameters": self.min_supported_parameters,
            "max_evidence": self.max_evidence,
            "max_strata": self.max_strata,
            "max_input_events": self.max_input_events,
            "max_outputs_per_input": self.max_outputs_per_input,
            "diagnostic_sample_limit": self.diagnostic_sample_limit,
            "required_left_halo_ns": self.required_left_halo_ns,
            "backoff_levels": list(self.backoff_levels),
            "rounding_digits": self.rounding_digits,
            "unsupported_parameter_policy": "explicit_neutral_identity",
            "unsupported_stratum_policy": "backoff_then_fail",
            "protected_anchor_policy": "preserve_exact",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible fit config."""
        return {**self.identity_payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationOperatorFitConfigV1":
        """Read and verify the fit config."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            min_stratum_support=_strict_int(
                data.get("min_stratum_support"), "min_stratum_support"
            ),
            min_parameter_support=_strict_int(
                data.get("min_parameter_support"), "min_parameter_support"
            ),
            min_supported_parameters=_strict_int(
                data.get("min_supported_parameters"),
                "min_supported_parameters",
            ),
            max_evidence=_strict_int(data.get("max_evidence"), "max_evidence"),
            max_strata=_strict_int(data.get("max_strata"), "max_strata"),
            max_input_events=_strict_int(
                data.get("max_input_events"), "max_input_events"
            ),
            max_outputs_per_input=_strict_int(
                data.get("max_outputs_per_input"), "max_outputs_per_input"
            ),
            diagnostic_sample_limit=_strict_int(
                data.get("diagnostic_sample_limit"),
                "diagnostic_sample_limit",
            ),
            required_left_halo_ns=_strict_int(
                data.get("required_left_halo_ns"), "required_left_halo_ns"
            ),
            backoff_levels=_string_tuple(data.get("backoff_levels")),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            config_id=str(data.get("config_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationStratumV1:
    """One fitted conditioning stratum and its explicit fallback keys."""

    level: str
    key: str
    pattern: Mapping[str, str]
    status: str
    support_count: int
    parameters: tuple[ObservationParameterEstimateV1, ...]
    evidence_ids: tuple[str, ...]
    fallback_keys: tuple[str, ...]
    stratum_id: str = ""
    schema_version: str = OBSERVATION_STRATUM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_STRATUM_SCHEMA_VERSION:
            raise ValueError("unsupported observation stratum schema")
        level = _required_text(self.level)
        if level not in OBSERVATION_BACKOFF_LEVELS:
            raise ValueError("unsupported observation stratum level")
        pattern = {
            _required_text(name): _required_text(value)
            for name, value in sorted(self.pattern.items())
        }
        context = ObservationContextV1(
            symbol=pattern.get("symbol", "GLOBAL"),
            epoch_id=pattern.get("epoch_id", "global"),
            state=pattern.get("state"),
            session=pattern.get("session"),
            event_tag=pattern.get("event_tag"),
        )
        if context.pattern_for_level(level) != pattern:
            raise ValueError("observation stratum pattern differs from level")
        expected_key = _stratum_key(level, pattern)
        if _required_text(self.key) != expected_key:
            raise ValueError("observation stratum key does not match pattern")
        status = _required_text(self.status)
        if status not in _STRATUM_STATUSES:
            raise ValueError("unsupported observation stratum status")
        support = _bounded_int(
            self.support_count,
            "support_count",
            0,
            MAX_OBSERVATION_INPUT_EVENTS * MAX_OBSERVATION_EVIDENCE,
        )
        parameters = tuple(sorted(self.parameters, key=lambda item: item.name))
        if not parameters or len(parameters) > MAX_OBSERVATION_PARAMETERS:
            raise ValueError("observation stratum parameters are unbounded")
        if len({parameter.name for parameter in parameters}) != len(parameters):
            raise ValueError("duplicate observation stratum parameter")
        evidence_ids = _bounded_text_tuple(self.evidence_ids, "evidence id")
        fallback_keys = tuple(
            dict.fromkeys(_required_text(key) for key in self.fallback_keys)
        )
        if len(fallback_keys) > MAX_OBSERVATION_STRATA:
            raise ValueError("observation fallback keys exceed limit")
        if self.key in fallback_keys:
            raise ValueError("observation stratum cannot fall back to itself")
        object.__setattr__(self, "level", level)
        object.__setattr__(self, "key", expected_key)
        object.__setattr__(self, "pattern", pattern)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "support_count", support)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "evidence_ids", evidence_ids)
        object.__setattr__(self, "fallback_keys", fallback_keys)
        expected = _stable_id("observation-stratum", self.identity_payload())
        supplied = str(self.stratum_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("stratum_id does not match deterministic identity")
        object.__setattr__(self, "stratum_id", expected)

    @property
    def parameter_map(self) -> dict[str, ObservationParameterEstimateV1]:
        """Return parameters indexed by stable name."""
        return {parameter.name: parameter for parameter in self.parameters}

    def effective_value(self, name: str) -> float:
        """Return a fitted value or an explicit neutral identity value."""
        if name not in OBSERVATION_PARAMETER_NAMES:
            raise ValueError("unsupported observation parameter")
        parameter = self.parameter_map.get(name)
        if parameter is None or parameter.support_status == "unsupported":
            return _NEUTRAL_PARAMETER_VALUES[name]
        return parameter.value

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic stratum identity."""
        return {
            "schema_version": self.schema_version,
            "level": self.level,
            "key": self.key,
            "pattern": dict(self.pattern),
            "status": self.status,
            "support_count": self.support_count,
            "parameters": [
                parameter.to_dict() for parameter in self.parameters
            ],
            "evidence_ids": list(self.evidence_ids),
            "fallback_keys": list(self.fallback_keys),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible stratum."""
        return {**self.identity_payload(), "stratum_id": self.stratum_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationStratumV1":
        """Read and verify one stratum."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            level=str(data.get("level", "")),
            key=str(data.get("key", "")),
            pattern=_string_mapping(data.get("pattern")),
            status=str(data.get("status", "")),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            parameters=tuple(
                ObservationParameterEstimateV1.from_dict(_mapping(item))
                for item in _sequence(data.get("parameters"))
            ),
            evidence_ids=_string_tuple(data.get("evidence_ids")),
            fallback_keys=_string_tuple(data.get("fallback_keys")),
            stratum_id=str(data.get("stratum_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationFitDiagnosticsV1:
    """Bounded, versioned fitting support and residual diagnostics."""

    evidence_count: int
    stratum_count: int
    status_counts: Mapping[str, int]
    parameter_support_counts: Mapping[str, int]
    parameter_residual_medians: Mapping[str, float]
    unsupported_parameter_names: tuple[str, ...]
    samples: tuple[Mapping[str, JSONValue], ...] = ()
    samples_truncated: bool = False
    diagnostics_id: str = ""
    schema_version: str = OBSERVATION_FIT_DIAGNOSTICS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_FIT_DIAGNOSTICS_SCHEMA_VERSION:
            raise ValueError("unsupported observation diagnostics schema")
        _bounded_int(
            self.evidence_count,
            "evidence_count",
            1,
            MAX_OBSERVATION_EVIDENCE,
        )
        _bounded_int(
            self.stratum_count, "stratum_count", 1, MAX_OBSERVATION_STRATA
        )
        statuses = {
            _required_text(name): _bounded_int(
                count, f"{name} count", 0, MAX_OBSERVATION_STRATA
            )
            for name, count in sorted(self.status_counts.items())
        }
        if (
            not statuses
            or not set(statuses).issubset(_STRATUM_STATUSES)
            or sum(statuses.values()) != self.stratum_count
        ):
            raise ValueError("observation diagnostic statuses do not reconcile")
        supports = {
            _required_text(name): _bounded_int(
                count,
                f"{name} support",
                0,
                MAX_OBSERVATION_INPUT_EVENTS * MAX_OBSERVATION_EVIDENCE,
            )
            for name, count in sorted(self.parameter_support_counts.items())
        }
        if not set(supports).issubset(OBSERVATION_PARAMETER_NAMES):
            raise ValueError("observation diagnostic support names differ")
        residuals = {
            _required_text(name): _finite_float(value, name)
            for name, value in sorted(self.parameter_residual_medians.items())
        }
        if not set(residuals).issubset(OBSERVATION_PARAMETER_NAMES) or any(
            value < 0.0 for value in residuals.values()
        ):
            raise ValueError("observation diagnostic residuals differ")
        unsupported = tuple(
            dict.fromkeys(
                _required_text(name)
                for name in self.unsupported_parameter_names
            )
        )
        if not set(unsupported).issubset(OBSERVATION_PARAMETER_NAMES):
            raise ValueError("observation unsupported parameter names differ")
        truncated = _strict_bool(self.samples_truncated, "samples_truncated")
        samples = tuple(dict(sample) for sample in self.samples)
        if len(samples) > MAX_OBSERVATION_DIAGNOSTIC_SAMPLES:
            raise ValueError("observation diagnostics samples exceed limit")
        object.__setattr__(self, "status_counts", statuses)
        object.__setattr__(self, "parameter_support_counts", supports)
        object.__setattr__(self, "parameter_residual_medians", residuals)
        object.__setattr__(self, "unsupported_parameter_names", unsupported)
        object.__setattr__(self, "samples", samples)
        object.__setattr__(self, "samples_truncated", truncated)
        expected = _stable_id(
            "observation-diagnostics", self.identity_payload()
        )
        supplied = str(self.diagnostics_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "diagnostics_id does not match deterministic identity"
            )
        object.__setattr__(self, "diagnostics_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic diagnostics identity."""
        return {
            "schema_version": self.schema_version,
            "evidence_count": self.evidence_count,
            "stratum_count": self.stratum_count,
            "status_counts": dict(self.status_counts),
            "parameter_support_counts": dict(self.parameter_support_counts),
            "parameter_residual_medians": dict(self.parameter_residual_medians),
            "unsupported_parameter_names": list(
                self.unsupported_parameter_names
            ),
            "samples": [dict(sample) for sample in self.samples],
            "samples_truncated": self.samples_truncated,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible diagnostics."""
        return {
            **self.identity_payload(),
            "diagnostics_id": self.diagnostics_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationFitDiagnosticsV1":
        """Read and verify fitting diagnostics."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            evidence_count=_strict_int(
                data.get("evidence_count"), "evidence_count"
            ),
            stratum_count=_strict_int(
                data.get("stratum_count"), "stratum_count"
            ),
            status_counts=_int_mapping(data.get("status_counts")),
            parameter_support_counts=_int_mapping(
                data.get("parameter_support_counts")
            ),
            parameter_residual_medians=_float_mapping(
                data.get("parameter_residual_medians")
            ),
            unsupported_parameter_names=_string_tuple(
                data.get("unsupported_parameter_names")
            ),
            samples=tuple(
                _mapping(sample) for sample in _sequence(data.get("samples"))
            ),
            samples_truncated=_strict_bool(
                data.get("samples_truncated", False), "samples_truncated"
            ),
            diagnostics_id=str(data.get("diagnostics_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationOperatorV1:
    """Replayable historical feed-observation operator artifact."""

    feed_epoch_definition_id: str
    feed_epoch_labels: tuple[str, ...]
    fit_config: ObservationOperatorFitConfigV1
    strata: tuple[ObservationStratumV1, ...]
    diagnostics: ObservationFitDiagnosticsV1
    source_hashes: tuple[str, ...]
    lineage: Mapping[str, JSONValue]
    required_left_halo_ns: int
    carry_required_after_first_window: bool = True
    carry_fields: tuple[str, ...] = OBSERVATION_CARRY_FIELDS
    operator_id: str = ""
    schema_version: str = OBSERVATION_OPERATOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_OPERATOR_SCHEMA_VERSION:
            raise ValueError("unsupported observation operator schema")
        definition_id = _required_sha256_id(
            self.feed_epoch_definition_id,
            "feed_epoch_definition_id",
            prefix="feed-epoch-definition",
        )
        labels = _bounded_text_tuple(
            self.feed_epoch_labels,
            "feed epoch label",
            limit=MAX_OBSERVATION_STRATA,
        )
        if not isinstance(self.fit_config, ObservationOperatorFitConfigV1):
            raise ValueError("operator requires a v1 fit config")
        carry_required = _strict_bool(
            self.carry_required_after_first_window,
            "carry_required_after_first_window",
        )
        if not carry_required:
            raise ValueError("v1 observation operators require streaming carry")
        strata = tuple(sorted(self.strata, key=lambda item: item.key))
        if not strata or len(strata) > self.fit_config.max_strata:
            raise ValueError("operator strata are empty or unbounded")
        if len({stratum.key for stratum in strata}) != len(strata):
            raise ValueError("operator contains duplicate stratum keys")
        stratum_keys = {stratum.key for stratum in strata}
        allowed_labels = set(labels)
        for stratum in strata:
            expected_support = max(
                (parameter.support_count for parameter in stratum.parameters),
                default=0,
            )
            if stratum.support_count != expected_support:
                raise ValueError("observation stratum support differs")
            for parameter in stratum.parameters:
                expected_parameter_status = (
                    "supported"
                    if parameter.support_count
                    >= self.fit_config.min_parameter_support
                    else "unsupported"
                )
                if parameter.support_status != expected_parameter_status:
                    raise ValueError("observation parameter status differs")
                if not set(parameter.evidence_ids).issubset(
                    stratum.evidence_ids
                ):
                    raise ValueError("observation parameter evidence differs")
            supported_count = sum(
                parameter.support_status == "supported"
                for parameter in stratum.parameters
            )
            expected_status = (
                "ready"
                if (
                    expected_support >= self.fit_config.min_stratum_support
                    and supported_count
                    >= self.fit_config.min_supported_parameters
                )
                else ("limited" if supported_count else "unsupported")
            )
            if stratum.status != expected_status:
                raise ValueError("observation stratum status differs")
            context = _context_from_pattern(stratum.pattern, allowed_labels)
            candidates = context.candidate_keys(self.fit_config.backoff_levels)
            offset = candidates.index(stratum.key) + 1
            expected_fallback = tuple(
                key for key in candidates[offset:] if key in stratum_keys
            )
            if stratum.fallback_keys != expected_fallback:
                raise ValueError("observation stratum fallback differs")
        global_stratum = next(
            (stratum for stratum in strata if stratum.level == "global"), None
        )
        if global_stratum is None or global_stratum.status != "ready":
            raise ValueError("operator requires a ready global stratum")
        if not isinstance(self.diagnostics, ObservationFitDiagnosticsV1):
            raise ValueError("operator requires v1 diagnostics")
        if self.diagnostics.stratum_count != len(strata):
            raise ValueError("operator diagnostics stratum count differs")
        if self.diagnostics.status_counts != dict(
            sorted(Counter(stratum.status for stratum in strata).items())
        ):
            raise ValueError("operator diagnostics status counts differ")
        global_parameters = global_stratum.parameter_map
        if self.diagnostics.parameter_support_counts != {
            name: global_parameters[name].support_count
            for name in OBSERVATION_PARAMETER_NAMES
            if name in global_parameters
        }:
            raise ValueError("operator diagnostics parameter support differs")
        expected_unsupported = tuple(
            name
            for name in OBSERVATION_PARAMETER_NAMES
            if name not in global_parameters
            or global_parameters[name].support_status == "unsupported"
        )
        if self.diagnostics.unsupported_parameter_names != expected_unsupported:
            raise ValueError("operator unsupported diagnostics differ")
        hashes = tuple(
            sorted(
                dict.fromkeys(
                    _required_sha256_id(value, "source hash")
                    for value in self.source_hashes
                )
            )
        )
        if not hashes:
            raise ValueError("operator requires source hashes")
        if len(hashes) > self.fit_config.max_evidence:
            raise ValueError("operator source hashes exceed fit evidence limit")
        halo = _bounded_int(
            self.required_left_halo_ns,
            "required_left_halo_ns",
            0,
            MAX_OBSERVATION_WINDOW_ALIGNMENT_NS,
        )
        carry_fields = _bounded_text_tuple(
            self.carry_fields,
            "carry field",
            limit=len(OBSERVATION_CARRY_FIELDS),
        )
        if carry_fields != OBSERVATION_CARRY_FIELDS:
            raise ValueError("operator carry fields do not match v1 contract")
        lineage = dict(self.lineage)
        _validate_operator_lineage(
            lineage,
            definition_id=definition_id,
            config_id=self.fit_config.config_id,
            source_hashes=hashes,
            evidence_count=self.diagnostics.evidence_count,
        )
        lineage_evidence_ids = set(_string_tuple(lineage.get("evidence_ids")))
        stratum_evidence_ids = {
            evidence_id
            for stratum in strata
            for evidence_id in stratum.evidence_ids
        }
        if stratum_evidence_ids != lineage_evidence_ids:
            raise ValueError("operator stratum evidence lineage differs")
        object.__setattr__(self, "feed_epoch_definition_id", definition_id)
        object.__setattr__(self, "feed_epoch_labels", labels)
        object.__setattr__(self, "strata", strata)
        object.__setattr__(self, "source_hashes", hashes)
        object.__setattr__(self, "lineage", lineage)
        object.__setattr__(self, "required_left_halo_ns", halo)
        object.__setattr__(
            self, "carry_required_after_first_window", carry_required
        )
        object.__setattr__(self, "carry_fields", carry_fields)
        expected = _stable_id("observation-operator", self.identity_payload())
        supplied = str(self.operator_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "operator_id does not match deterministic identity"
            )
        object.__setattr__(self, "operator_id", expected)

    @property
    def valid_for_application(self) -> bool:
        """Return whether the artifact passed its fail-closed constructor."""
        return True

    def resolve_stratum(
        self, context: ObservationContextV1
    ) -> tuple[ObservationStratumV1, tuple[str, ...]]:
        """Resolve a context through the versioned, explicit hierarchy."""
        if context.epoch_id not in self.feed_epoch_labels:
            raise ValueError("observation context epoch is not in operator")
        by_key = {stratum.key: stratum for stratum in self.strata}
        attempted: list[str] = []
        for key in context.candidate_keys(self.fit_config.backoff_levels):
            attempted.append(key)
            stratum = by_key.get(key)
            if stratum is not None and stratum.status == "ready":
                return stratum, tuple(attempted)
        raise ValueError("no supported observation stratum after fallback")

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return complete semantic operator identity."""
        return {
            "schema_version": self.schema_version,
            "feed_epoch_definition_id": self.feed_epoch_definition_id,
            "feed_epoch_labels": list(self.feed_epoch_labels),
            "fit_config": self.fit_config.to_dict(),
            "strata": [stratum.to_dict() for stratum in self.strata],
            "diagnostics": self.diagnostics.to_dict(),
            "source_hashes": list(self.source_hashes),
            "lineage": dict(self.lineage),
            "required_left_halo_ns": self.required_left_halo_ns,
            "carry_required_after_first_window": (
                self.carry_required_after_first_window
            ),
            "carry_fields": list(self.carry_fields),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible operator artifact."""
        return {
            **self.identity_payload(),
            "operator_id": self.operator_id,
            "valid_for_application": self.valid_for_application,
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationOperatorV1":
        """Read and verify a version-one operator artifact."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            feed_epoch_definition_id=str(
                data.get("feed_epoch_definition_id", "")
            ),
            feed_epoch_labels=_string_tuple(data.get("feed_epoch_labels")),
            fit_config=ObservationOperatorFitConfigV1.from_dict(
                _mapping(data.get("fit_config"))
            ),
            strata=tuple(
                ObservationStratumV1.from_dict(_mapping(item))
                for item in _sequence(data.get("strata"))
            ),
            diagnostics=ObservationFitDiagnosticsV1.from_dict(
                _mapping(data.get("diagnostics"))
            ),
            source_hashes=_string_tuple(data.get("source_hashes")),
            lineage=_mapping(data.get("lineage")),
            required_left_halo_ns=_strict_int(
                data.get("required_left_halo_ns"), "required_left_halo_ns"
            ),
            carry_required_after_first_window=_strict_bool(
                data.get("carry_required_after_first_window", True),
                "carry_required_after_first_window",
            ),
            carry_fields=_string_tuple(data.get("carry_fields")),
            operator_id=str(data.get("operator_id", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ObservationOperatorV1":
        """Read an operator from deterministic JSON."""
        data = json.loads(text)
        if not isinstance(data, Mapping):
            raise ValueError("observation operator JSON must be an object")
        return cls.from_dict(data)

    def apply(
        self,
        events: Sequence["ObservationInputEventV1"],
        *,
        window: ReconstructionWindowV1,
        carry: "ObservationCarryStateV1 | None" = None,
        information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
        source_start: bool = False,
    ) -> "ObservationApplicationResultV1":
        """Apply forward observation while preserving protected anchors."""
        return _apply_observation_operator(
            self,
            events,
            window=window,
            carry=carry,
            application_mode="apply",
            information_mode=information_mode,
            source_start=source_start,
            benchmark_protected_ids=None,
        )

    def degrade(
        self,
        events: Sequence["ObservationInputEventV1"],
        *,
        window: ReconstructionWindowV1,
        carry: "ObservationCarryStateV1 | None" = None,
        protected_event_ids: Sequence[str] = (),
        source_start: bool = False,
    ) -> "ObservationApplicationResultV1":
        """Apply controlled degradation for the generator-neutral benchmark."""
        protected = tuple(
            dict.fromkeys(
                _required_text(value) for value in protected_event_ids
            )
        )
        if len(protected) > self.fit_config.max_input_events:
            raise ValueError("protected observation IDs exceed input limit")
        return _apply_observation_operator(
            self,
            events,
            window=window,
            carry=carry,
            application_mode="degrade",
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            source_start=source_start,
            benchmark_protected_ids=frozenset(protected),
        )


@dataclass(frozen=True, slots=True)
class ObservationInputEventV1:
    """One market event plus observation-only conditioning metadata."""

    source_event_id: str
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    context: ObservationContextV1
    protected_anchor: bool = False
    schema_version: str = OBSERVATION_INPUT_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_INPUT_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported observation input event schema")
        source_id = _required_text(self.source_event_id)
        symbol = _normalized_symbol(self.symbol)
        timestamp = _bounded_int(
            self.event_time_ns, "event_time_ns", INT64_MIN, INT64_MAX
        )
        sequence = _bounded_int(
            self.event_sequence, "event_sequence", 0, 2**63 - 1
        )
        bid = _positive_float(self.bid, "bid")
        ask = _positive_float(self.ask, "ask")
        if ask < bid:
            raise ValueError("observation input ask precedes bid")
        if not isinstance(self.context, ObservationContextV1):
            raise ValueError("observation input requires v1 context")
        if self.context.symbol != symbol:
            raise ValueError("observation input context symbol differs")
        protected = _strict_bool(self.protected_anchor, "protected_anchor")
        object.__setattr__(self, "source_event_id", source_id)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "event_time_ns", timestamp)
        object.__setattr__(self, "event_sequence", sequence)
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(self, "protected_anchor", protected)

    @classmethod
    def from_synthetic_event(
        cls,
        event: SyntheticEventV1,
        *,
        context: ObservationContextV1,
        protected_anchor: bool | None = None,
    ) -> "ObservationInputEventV1":
        """Adapt an immutable market event without changing its identity."""
        protected = (
            event.origin is SyntheticEventOrigin.OBSERVED
            if protected_anchor is None
            else _strict_bool(protected_anchor, "protected_anchor")
        )
        return cls(
            source_event_id=event.event_id,
            symbol=event.symbol,
            event_time_ns=event.event_time_ns,
            event_sequence=event.event_sequence,
            bid=event.bid,
            ask=event.ask,
            context=context,
            protected_anchor=protected,
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible input event."""
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "context": self.context.to_dict(),
            "protected_anchor": self.protected_anchor,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationInputEventV1":
        """Read one input event."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            source_event_id=str(data.get("source_event_id", "")),
            symbol=str(data.get("symbol", "")),
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            event_sequence=_strict_int(
                data.get("event_sequence"), "event_sequence"
            ),
            bid=_finite_float(data.get("bid"), "bid"),
            ask=_finite_float(data.get("ask"), "ask"),
            context=ObservationContextV1.from_dict(
                _mapping(data.get("context"))
            ),
            protected_anchor=_strict_bool(
                data.get("protected_anchor", False), "protected_anchor"
            ),
        )


@dataclass(frozen=True, slots=True)
class ObservationOutputEventV1:
    """One operator-lineaged delivery observation."""

    source_event_id: str
    operator_id: str
    stratum_id: str
    symbol: str
    source_time_ns: int
    observed_time_ns: int
    observed_sequence: int
    bid: float
    ask: float
    duplicate_ordinal: int
    transformations: tuple[str, ...]
    protected_anchor: bool
    observation_id: str = ""
    schema_version: str = OBSERVATION_OUTPUT_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_OUTPUT_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported observation output event schema")
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        object.__setattr__(
            self,
            "operator_id",
            _required_sha256_id(
                self.operator_id, "operator_id", prefix="observation-operator"
            ),
        )
        object.__setattr__(
            self,
            "stratum_id",
            _required_sha256_id(
                self.stratum_id, "stratum_id", prefix="observation-stratum"
            ),
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self,
            "source_time_ns",
            _bounded_int(
                self.source_time_ns,
                "source_time_ns",
                INT64_MIN,
                INT64_MAX,
            ),
        )
        object.__setattr__(
            self,
            "observed_time_ns",
            _bounded_int(
                self.observed_time_ns,
                "observed_time_ns",
                INT64_MIN,
                INT64_MAX,
            ),
        )
        if self.observed_time_ns > self.source_time_ns:
            raise ValueError("observed time cannot follow source time in v1")
        object.__setattr__(
            self,
            "observed_sequence",
            _bounded_int(
                self.observed_sequence, "observed_sequence", 0, 2**63 - 1
            ),
        )
        bid = _positive_float(self.bid, "bid")
        ask = _positive_float(self.ask, "ask")
        if ask < bid:
            raise ValueError("observation output ask precedes bid")
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(
            self,
            "duplicate_ordinal",
            _bounded_int(
                self.duplicate_ordinal,
                "duplicate_ordinal",
                0,
                MAX_OBSERVATION_OUTPUTS_PER_INPUT - 1,
            ),
        )
        transforms = tuple(
            dict.fromkeys(
                _required_text(value) for value in self.transformations
            )
        )
        if not set(transforms).issubset(_OUTPUT_TRANSFORMATIONS):
            raise ValueError("unsupported observation transformation")
        protected = _strict_bool(self.protected_anchor, "protected_anchor")
        if protected and transforms:
            raise ValueError("protected anchors cannot carry transformations")
        object.__setattr__(self, "transformations", transforms)
        object.__setattr__(self, "protected_anchor", protected)
        expected = _stable_id("observation-event", self.identity_payload())
        supplied = str(self.observation_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "observation_id does not match deterministic identity"
            )
        object.__setattr__(self, "observation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic delivery-observation identity."""
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "operator_id": self.operator_id,
            "stratum_id": self.stratum_id,
            "symbol": self.symbol,
            "source_time_ns": self.source_time_ns,
            "observed_time_ns": self.observed_time_ns,
            "observed_sequence": self.observed_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "duplicate_ordinal": self.duplicate_ordinal,
            "transformations": list(self.transformations),
            "protected_anchor": self.protected_anchor,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible output event."""
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationOutputEventV1":
        """Read and verify one output event."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            source_event_id=str(data.get("source_event_id", "")),
            operator_id=str(data.get("operator_id", "")),
            stratum_id=str(data.get("stratum_id", "")),
            symbol=str(data.get("symbol", "")),
            source_time_ns=_strict_int(
                data.get("source_time_ns"), "source_time_ns"
            ),
            observed_time_ns=_strict_int(
                data.get("observed_time_ns"), "observed_time_ns"
            ),
            observed_sequence=_strict_int(
                data.get("observed_sequence"), "observed_sequence"
            ),
            bid=_finite_float(data.get("bid"), "bid"),
            ask=_finite_float(data.get("ask"), "ask"),
            duplicate_ordinal=_strict_int(
                data.get("duplicate_ordinal"), "duplicate_ordinal"
            ),
            transformations=_string_tuple(data.get("transformations")),
            protected_anchor=_strict_bool(
                data.get("protected_anchor", False), "protected_anchor"
            ),
            observation_id=str(data.get("observation_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationCarryStateV1:
    """Bounded state required for partition-independent application."""

    operator_id: str
    symbol: str
    last_source_time_ns: int | None = None
    last_observed_time_ns: int | None = None
    last_bid: float | None = None
    last_ask: float | None = None
    rate_bucket_start_ns: int | None = None
    rate_bucket_count: int = 0
    outage_bucket_start_ns: int | None = None
    outage_active: bool = False
    reconnect_pending: bool = False
    carry_id: str = ""
    schema_version: str = OBSERVATION_CARRY_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_CARRY_STATE_SCHEMA_VERSION:
            raise ValueError("unsupported observation carry-state schema")
        object.__setattr__(
            self,
            "operator_id",
            _required_sha256_id(
                self.operator_id, "operator_id", prefix="observation-operator"
            ),
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        for name in (
            "last_source_time_ns",
            "last_observed_time_ns",
            "rate_bucket_start_ns",
            "outage_bucket_start_ns",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self,
                    name,
                    _bounded_int(value, name, INT64_MIN, INT64_MAX),
                )
        if (self.last_bid is None) != (self.last_ask is None):
            raise ValueError("observation carry bid/ask must be paired")
        if self.last_bid is not None and self.last_ask is not None:
            bid = _positive_float(self.last_bid, "last_bid")
            ask = _positive_float(self.last_ask, "last_ask")
            if ask < bid:
                raise ValueError("observation carry ask precedes bid")
            object.__setattr__(self, "last_bid", bid)
            object.__setattr__(self, "last_ask", ask)
        if self.last_observed_time_ns is not None and (
            self.last_source_time_ns is None
            or self.last_observed_time_ns > self.last_source_time_ns
            or self.last_bid is None
        ):
            raise ValueError("observation carry delivered state is incomplete")
        if self.last_bid is not None and self.last_observed_time_ns is None:
            raise ValueError("observation carry quote lacks delivered time")
        outage_active = _strict_bool(self.outage_active, "outage_active")
        reconnect_pending = _strict_bool(
            self.reconnect_pending, "reconnect_pending"
        )
        if outage_active and (
            self.outage_bucket_start_ns is None or not reconnect_pending
        ):
            raise ValueError("observation carry outage state is incomplete")
        object.__setattr__(
            self,
            "rate_bucket_count",
            _bounded_int(
                self.rate_bucket_count,
                "rate_bucket_count",
                0,
                MAX_OBSERVATION_INPUT_EVENTS
                * MAX_OBSERVATION_OUTPUTS_PER_INPUT,
            ),
        )
        if self.rate_bucket_count and self.rate_bucket_start_ns is None:
            raise ValueError("observation carry rate state is incomplete")
        object.__setattr__(self, "outage_active", outage_active)
        object.__setattr__(self, "reconnect_pending", reconnect_pending)
        expected = _stable_id("observation-carry", self.identity_payload())
        supplied = str(self.carry_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("carry_id does not match deterministic identity")
        object.__setattr__(self, "carry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic carry identity."""
        return {
            "schema_version": self.schema_version,
            "operator_id": self.operator_id,
            "symbol": self.symbol,
            "last_source_time_ns": self.last_source_time_ns,
            "last_observed_time_ns": self.last_observed_time_ns,
            "last_bid": self.last_bid,
            "last_ask": self.last_ask,
            "rate_bucket_start_ns": self.rate_bucket_start_ns,
            "rate_bucket_count": self.rate_bucket_count,
            "outage_bucket_start_ns": self.outage_bucket_start_ns,
            "outage_active": self.outage_active,
            "reconnect_pending": self.reconnect_pending,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible carry state."""
        return {**self.identity_payload(), "carry_id": self.carry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ObservationCarryStateV1":
        """Read and verify carry state."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            operator_id=str(data.get("operator_id", "")),
            symbol=str(data.get("symbol", "")),
            last_source_time_ns=_optional_strict_int(
                data.get("last_source_time_ns"), "last_source_time_ns"
            ),
            last_observed_time_ns=_optional_strict_int(
                data.get("last_observed_time_ns"), "last_observed_time_ns"
            ),
            last_bid=_optional_float(data.get("last_bid")),
            last_ask=_optional_float(data.get("last_ask")),
            rate_bucket_start_ns=_optional_strict_int(
                data.get("rate_bucket_start_ns"), "rate_bucket_start_ns"
            ),
            rate_bucket_count=_strict_int(
                data.get("rate_bucket_count", 0), "rate_bucket_count"
            ),
            outage_bucket_start_ns=_optional_strict_int(
                data.get("outage_bucket_start_ns"),
                "outage_bucket_start_ns",
            ),
            outage_active=_strict_bool(
                data.get("outage_active", False), "outage_active"
            ),
            reconnect_pending=_strict_bool(
                data.get("reconnect_pending", False), "reconnect_pending"
            ),
            carry_id=str(data.get("carry_id", "")),
        )


@dataclass(frozen=True, slots=True)
class ObservationApplicationResultV1:
    """Bounded in-memory output and diagnostics for one owned window."""

    operator_id: str
    window_id: str
    symbol: str
    application_mode: str
    input_count: int
    output_events: tuple[ObservationOutputEventV1, ...]
    reason_counts: Mapping[str, int]
    fallback_counts: Mapping[str, int]
    diagnostic_samples: tuple[Mapping[str, JSONValue], ...]
    samples_truncated: bool
    carry_state: ObservationCarryStateV1
    result_id: str = ""
    schema_version: str = OBSERVATION_APPLICATION_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVATION_APPLICATION_RESULT_SCHEMA_VERSION:
            raise ValueError("unsupported observation application schema")
        object.__setattr__(
            self,
            "operator_id",
            _required_sha256_id(
                self.operator_id, "operator_id", prefix="observation-operator"
            ),
        )
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        mode = _required_text(self.application_mode)
        if mode not in {"apply", "degrade"}:
            raise ValueError("unsupported observation application mode")
        object.__setattr__(self, "application_mode", mode)
        _bounded_int(
            self.input_count,
            "input_count",
            0,
            MAX_OBSERVATION_INPUT_EVENTS,
        )
        outputs = tuple(self.output_events)
        if len(outputs) > (
            self.input_count * MAX_OBSERVATION_OUTPUTS_PER_INPUT
        ):
            raise ValueError("observation output amplification exceeds limit")
        if any(event.operator_id != self.operator_id for event in outputs):
            raise ValueError("observation output operator differs")
        if (
            tuple(
                sorted(
                    outputs,
                    key=lambda item: (
                        item.observed_time_ns,
                        item.observed_sequence,
                        item.observation_id,
                    ),
                )
            )
            != outputs
        ):
            raise ValueError("observation outputs are not ordered")
        reason_counts = _validated_count_mapping(
            self.reason_counts, allowed_names=_APPLICATION_REASONS
        )
        fallback_counts = _validated_count_mapping(
            self.fallback_counts,
            allowed_names=set(OBSERVATION_BACKOFF_LEVELS),
        )
        if sum(reason_counts.values()) != self.input_count:
            raise ValueError("observation reason counts do not cover inputs")
        events_by_source: dict[str, list[ObservationOutputEventV1]] = (
            defaultdict(list)
        )
        sequences_by_timestamp: dict[int, list[int]] = defaultdict(list)
        for event in outputs:
            events_by_source[event.source_event_id].append(event)
            sequences_by_timestamp[event.observed_time_ns].append(
                event.observed_sequence
            )
        outputs_by_source = {
            source_id: len(source_events)
            for source_id, source_events in events_by_source.items()
        }
        if any(
            count > MAX_OBSERVATION_OUTPUTS_PER_INPUT
            for count in outputs_by_source.values()
        ):
            raise ValueError("observation source output count exceeds limit")
        if len(outputs_by_source) != reason_counts.get("retained", 0):
            raise ValueError("observation retained count differs from outputs")
        for source_events in events_by_source.values():
            if tuple(
                event.duplicate_ordinal for event in source_events
            ) != tuple(range(len(source_events))):
                raise ValueError(
                    "observation duplicate ordinals are not contiguous"
                )
        for timestamp_sequences in sequences_by_timestamp.values():
            sequences = tuple(timestamp_sequences)
            if sequences != tuple(range(len(sequences))):
                raise ValueError(
                    "observation delivery sequences are not contiguous"
                )
        samples = tuple(dict(sample) for sample in self.diagnostic_samples)
        if len(samples) > MAX_OBSERVATION_DIAGNOSTIC_SAMPLES:
            raise ValueError("observation application samples exceed limit")
        if not isinstance(self.carry_state, ObservationCarryStateV1):
            raise ValueError("observation result requires v1 carry state")
        if (
            self.carry_state.operator_id != self.operator_id
            or self.carry_state.symbol != self.symbol
        ):
            raise ValueError("observation result carry scope differs")
        truncated = _strict_bool(self.samples_truncated, "samples_truncated")
        object.__setattr__(self, "output_events", outputs)
        object.__setattr__(self, "reason_counts", reason_counts)
        object.__setattr__(self, "fallback_counts", fallback_counts)
        object.__setattr__(self, "diagnostic_samples", samples)
        object.__setattr__(self, "samples_truncated", truncated)
        expected = _stable_id(
            "observation-application", self.identity_payload()
        )
        supplied = str(self.result_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("result_id does not match deterministic identity")
        object.__setattr__(self, "result_id", expected)

    @property
    def output_count(self) -> int:
        """Return the number of delivery observations."""
        return len(self.output_events)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic result identity."""
        return {
            "schema_version": self.schema_version,
            "operator_id": self.operator_id,
            "window_id": self.window_id,
            "symbol": self.symbol,
            "application_mode": self.application_mode,
            "input_count": self.input_count,
            "output_events": [event.to_dict() for event in self.output_events],
            "reason_counts": dict(self.reason_counts),
            "fallback_counts": dict(self.fallback_counts),
            "diagnostic_samples": [
                dict(sample) for sample in self.diagnostic_samples
            ],
            "samples_truncated": self.samples_truncated,
            "carry_state": self.carry_state.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible application result."""
        return {**self.identity_payload(), "result_id": self.result_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ObservationApplicationResultV1":
        """Read and verify one bounded application result."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            operator_id=str(data.get("operator_id", "")),
            window_id=str(data.get("window_id", "")),
            symbol=str(data.get("symbol", "")),
            application_mode=str(data.get("application_mode", "")),
            input_count=_strict_int(data.get("input_count"), "input_count"),
            output_events=tuple(
                ObservationOutputEventV1.from_dict(_mapping(item))
                for item in _sequence(data.get("output_events"))
            ),
            reason_counts=_int_mapping(data.get("reason_counts")),
            fallback_counts=_int_mapping(data.get("fallback_counts")),
            diagnostic_samples=tuple(
                _mapping(sample)
                for sample in _sequence(data.get("diagnostic_samples"))
            ),
            samples_truncated=_strict_bool(
                data.get("samples_truncated", False), "samples_truncated"
            ),
            carry_state=ObservationCarryStateV1.from_dict(
                _mapping(data.get("carry_state"))
            ),
            result_id=str(data.get("result_id", "")),
        )


def fit_observation_operator(
    evidence: Sequence[ObservationFitEvidenceV1],
    *,
    epoch_definition: FeedEpochDefinitionV1,
    config: ObservationOperatorFitConfigV1 | None = None,
) -> ObservationOperatorV1:
    """Fit a deterministic operator through an explicit hierarchy."""
    selected = config or ObservationOperatorFitConfigV1()
    if not epoch_definition.valid_for_observation_models:
        raise ValueError("feed epoch definition has not passed stability")
    ordered = tuple(sorted(evidence, key=lambda item: item.evidence_id))
    if not ordered or len(ordered) > selected.max_evidence:
        raise ValueError("observation evidence is empty or exceeds fit limit")
    if len({item.evidence_id for item in ordered}) != len(ordered):
        raise ValueError("duplicate observation fit evidence")
    allowed_labels = {
        *(epoch.label for epoch in epoch_definition.epochs),
        *(
            boundary.transition_label
            for boundary in epoch_definition.boundaries
        ),
    }
    for item in ordered:
        if item.context.symbol not in epoch_definition.symbols:
            raise ValueError(
                "observation evidence symbol is outside epoch scope"
            )
        if item.context.epoch_id not in allowed_labels:
            raise ValueError("observation evidence epoch is outside definition")
        midpoint_ms = (
            (item.start_timestamp_ns + item.end_timestamp_ns) // 2
        ) // 1_000_000
        assignment = epoch_definition.assign(
            symbol=item.context.symbol,
            timestamp_utc_ms=midpoint_ms,
        )
        if assignment.label != item.context.epoch_id:
            raise ValueError("observation evidence epoch assignment differs")

    groups: dict[tuple[str, str], list[ObservationFitEvidenceV1]] = defaultdict(
        list
    )
    patterns: dict[tuple[str, str], dict[str, str]] = {}
    for item in ordered:
        seen: set[str] = set()
        for level in selected.backoff_levels:
            pattern = item.context.pattern_for_level(level)
            if pattern is None:
                continue
            key = _stratum_key(level, pattern)
            if key in seen:
                continue
            seen.add(key)
            groups[(level, key)].append(item)
            patterns[(level, key)] = pattern
    if len(groups) > selected.max_strata:
        raise ValueError("fitted observation strata exceed configured limit")

    preliminary: list[ObservationStratumV1] = []
    for (level, key), items in sorted(groups.items(), key=lambda pair: pair[0]):
        parameters = tuple(
            _fit_parameter(name, items, selected)
            for name in OBSERVATION_PARAMETER_NAMES
            if any(name in item.parameter_values for item in items)
        )
        support_count = max(
            (parameter.support_count for parameter in parameters), default=0
        )
        supported_count = sum(
            parameter.support_status == "supported" for parameter in parameters
        )
        if (
            support_count >= selected.min_stratum_support
            and supported_count >= selected.min_supported_parameters
        ):
            status = "ready"
        elif supported_count:
            status = "limited"
        else:
            status = "unsupported"
        preliminary.append(
            ObservationStratumV1(
                level=level,
                key=key,
                pattern=patterns[(level, key)],
                status=status,
                support_count=support_count,
                parameters=parameters,
                evidence_ids=tuple(item.evidence_id for item in items),
                fallback_keys=(),
            )
        )
    keys = {stratum.key for stratum in preliminary}
    strata: list[ObservationStratumV1] = []
    for stratum in preliminary:
        context = _context_from_pattern(stratum.pattern, allowed_labels)
        candidates = context.candidate_keys(selected.backoff_levels)
        try:
            offset = candidates.index(stratum.key) + 1
        except ValueError:
            offset = len(candidates)
        fallback = tuple(key for key in candidates[offset:] if key in keys)
        strata.append(replace(stratum, fallback_keys=fallback, stratum_id=""))

    diagnostics = _fit_diagnostics(ordered, strata, selected)
    source_hashes = tuple(
        sorted({item.source_artifact_sha256 for item in ordered})
    )
    lineage_material: dict[str, JSONValue] = {
        "feed_epoch_definition_id": epoch_definition.definition_id,
        "config_id": selected.config_id,
        "evidence_count": len(ordered),
        "evidence_ids": [item.evidence_id for item in ordered],
        "source_hashes": list(source_hashes),
        "sources": [
            {
                "evidence_id": item.evidence_id,
                "source_evidence_id": item.source_evidence_id,
                "source_artifact_sha256": item.source_artifact_sha256,
                "source_hash_basis": item.source_hash_basis,
                "evidence_kind": item.evidence_kind,
            }
            for item in ordered
        ],
    }
    lineage = {
        **lineage_material,
        "lineage_sha256": _payload_sha256(lineage_material),
    }
    labels = tuple(sorted(allowed_labels))
    return ObservationOperatorV1(
        feed_epoch_definition_id=epoch_definition.definition_id,
        feed_epoch_labels=labels,
        fit_config=selected,
        strata=tuple(strata),
        diagnostics=diagnostics,
        source_hashes=source_hashes,
        lineage=lineage,
        required_left_halo_ns=selected.required_left_halo_ns,
    )


def write_observation_operator(
    operator: ObservationOperatorV1, path: Path
) -> ArtifactRef:
    """Write one deterministic operator artifact and return its reference."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (operator.to_json() + "\n").encode("utf-8")
    if len(encoded) > MAX_OBSERVATION_ARTIFACT_BYTES:
        raise ValueError("observation operator artifact exceeds byte limit")
    destination.write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    return ArtifactRef(
        kind="observation-operator",
        path=str(destination),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "schema_version": operator.schema_version,
            "operator_id": operator.operator_id,
            "feed_epoch_definition_id": operator.feed_epoch_definition_id,
        },
    )


def read_observation_operator_artifact(
    artifact: ArtifactRef,
) -> ObservationOperatorV1:
    """Read an operator only after byte-level artifact verification."""
    if artifact.kind != "observation-operator":
        raise ValueError("artifact is not an observation operator")
    path = Path(artifact.path)
    size = path.stat().st_size
    if size > MAX_OBSERVATION_ARTIFACT_BYTES:
        raise ValueError("observation operator artifact exceeds byte limit")
    encoded = path.read_bytes()
    if artifact.size_bytes is not None and len(encoded) != artifact.size_bytes:
        raise ValueError("observation operator artifact size differs")
    digest = hashlib.sha256(encoded).hexdigest()
    if not artifact.sha256 or digest != artifact.sha256:
        raise ValueError("observation operator artifact hash differs")
    operator = ObservationOperatorV1.from_json(encoded.decode("utf-8"))
    if (
        artifact.metadata.get("operator_id") != operator.operator_id
        or artifact.metadata.get("schema_version") != operator.schema_version
        or artifact.metadata.get("feed_epoch_definition_id")
        != operator.feed_epoch_definition_id
    ):
        raise ValueError("observation operator artifact metadata differs")
    return operator


@dataclass(slots=True)
class _MutableApplicationState:
    """Internal bounded state used while applying one window."""

    last_source_time_ns: int | None = None
    last_observed_time_ns: int | None = None
    last_bid: float | None = None
    last_ask: float | None = None
    rate_bucket_start_ns: int | None = None
    rate_bucket_count: int = 0
    outage_bucket_start_ns: int | None = None
    outage_active: bool = False
    reconnect_pending: bool = False


def _apply_observation_operator(
    operator: ObservationOperatorV1,
    events: Sequence[ObservationInputEventV1],
    *,
    window: ReconstructionWindowV1,
    carry: ObservationCarryStateV1 | None,
    application_mode: str,
    information_mode: InformationMode,
    source_start: bool,
    benchmark_protected_ids: frozenset[str] | None,
) -> ObservationApplicationResultV1:
    source_start = _strict_bool(source_start, "source_start")
    if len(events) > operator.fit_config.max_input_events:
        raise ValueError("observation application exceeds input event limit")
    selected_mode = InformationMode.from_value(information_mode)
    ordered = tuple(
        sorted(
            events,
            key=lambda item: (
                item.event_time_ns,
                item.event_sequence,
                item.source_event_id,
            ),
        )
    )
    if len({item.source_event_id for item in ordered}) != len(ordered):
        raise ValueError("duplicate observation source event identity")
    symbol = _application_symbol(ordered, window)
    window_symbols = {_normalized_symbol(value) for value in window.symbols}
    if symbol not in window_symbols:
        raise ValueError("observation symbol is outside reconstruction window")
    if any(item.symbol != symbol for item in ordered):
        raise ValueError("observation application requires one symbol")
    if any(not window.reads_event_time(item.event_time_ns) for item in ordered):
        raise ValueError("observation input event is outside window read scope")
    if carry is None and not source_start:
        if operator.carry_required_after_first_window:
            raise ValueError(
                "observation operator requires carry after the source start"
            )
        if window.left_halo_ns < operator.required_left_halo_ns:
            raise ValueError("observation window lacks required left halo")
    if carry is not None and (
        carry.operator_id != operator.operator_id or carry.symbol != symbol
    ):
        raise ValueError("observation carry scope differs from application")
    first_core_event = next(
        (
            item
            for item in ordered
            if window.core_start_ns <= item.event_time_ns < window.core_end_ns
        ),
        None,
    )
    if (
        carry is not None
        and carry.last_source_time_ns is not None
        and first_core_event is not None
        and first_core_event.event_time_ns <= carry.last_source_time_ns
    ):
        raise ValueError("observation carry watermark is stale or overlapping")
    if (
        application_mode == "apply"
        and selected_mode is InformationMode.EX_ANTE_SIMULATION
        and any(item.protected_anchor for item in ordered)
    ):
        raise ValueError("ex-ante observation application cannot use anchors")

    state = _state_from_carry(carry)
    global_stratum = next(
        stratum for stratum in operator.strata if stratum.level == "global"
    )
    _validate_window_alignment(
        window,
        {
            name: global_stratum.effective_value(name)
            for name in OBSERVATION_PARAMETER_NAMES
        },
    )
    tentative: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    fallbacks: Counter[str] = Counter()
    samples: list[dict[str, JSONValue]] = []
    inspected_owned = 0
    dropped_owned = 0
    for event in ordered:
        if carry is not None and event.event_time_ns < window.core_start_ns:
            continue
        if event.event_time_ns >= window.core_end_ns:
            continue
        owned = window.owns_event_time(event.event_time_ns)
        if owned:
            inspected_owned += 1
        stratum, attempted = operator.resolve_stratum(event.context)
        if owned and len(attempted) > 1:
            fallbacks[stratum.level] += 1
        params = {
            name: stratum.effective_value(name)
            for name in OBSERVATION_PARAMETER_NAMES
        }
        _validate_window_alignment(window, params)
        protected = (
            event.protected_anchor
            if benchmark_protected_ids is None
            else event.source_event_id in benchmark_protected_ids
        )
        dropped_reason: str | None = None
        transformed_time = event.event_time_ns
        transformed_bid = event.bid
        transformed_ask = event.ask
        transformations: list[str] = []
        rate_window = max(1, int(params["burst_window_ns"]) or 1_000_000_000)
        rate_bucket = event.event_time_ns // rate_window * rate_window
        if state.rate_bucket_start_ns != rate_bucket:
            state.rate_bucket_start_ns = rate_bucket
            state.rate_bucket_count = 0

        if not protected:
            outage_window = int(params["outage_window_ns"])
            quiet_probability = params["quiet_gap_probability"]
            outage_bucket = (
                event.event_time_ns // outage_window * outage_window
                if outage_window > 0
                else None
            )
            outage_active = bool(
                outage_bucket is not None
                and _selected(
                    quiet_probability,
                    operator.operator_id,
                    stratum.stratum_id,
                    "outage",
                    str(outage_bucket),
                )
            )
            state.outage_bucket_start_ns = outage_bucket
            state.outage_active = outage_active
            if outage_active:
                state.reconnect_pending = True
                dropped_reason = "outage"
            else:
                state.outage_active = False
                if not _selected(
                    params["retention_probability"],
                    operator.operator_id,
                    stratum.stratum_id,
                    "retention",
                    event.source_event_id,
                ):
                    dropped_reason = "thinning"

            timestamp_quantum = max(1, int(params["timestamp_quantum_ns"]))
            batch_window = int(params["batch_window_ns"])
            transformed_time = (
                event.event_time_ns // timestamp_quantum * timestamp_quantum
            )
            if transformed_time != event.event_time_ns:
                transformations.append("timestamp_quantized")
            if batch_window > 0:
                batched = transformed_time // batch_window * batch_window
                if batched != transformed_time:
                    transformations.append("batched")
                transformed_time = batched

            digits = int(params["price_precision_digits"])
            transformed_bid = round(event.bid, digits)
            transformed_ask = max(
                transformed_bid,
                round(event.ask, digits),
            )
            if transformed_bid != event.bid or transformed_ask != event.ask:
                transformations.append("price_quantized")
            unchanged = (
                state.last_bid is not None
                and state.last_ask is not None
                and transformed_bid == state.last_bid
                and transformed_ask == state.last_ask
            )
            if (
                dropped_reason is None
                and unchanged
                and not _selected(
                    params["unchanged_retention_probability"],
                    operator.operator_id,
                    stratum.stratum_id,
                    "unchanged",
                    event.source_event_id,
                )
            ):
                dropped_reason = "unchanged_quote_filter"
            threshold = params["quote_transition_threshold"]
            if (
                dropped_reason is None
                and not unchanged
                and threshold > 0
                and state.last_bid is not None
                and state.last_ask is not None
                and abs(transformed_bid - state.last_bid) < threshold
                and abs(transformed_ask - state.last_ask) < threshold
            ):
                dropped_reason = "quote_transition_filter"

            rate_cap = params["rate_cap_per_second"]
            if rate_cap > 0:
                allowed = max(
                    1,
                    int(math.floor(rate_cap * rate_window / 1_000_000_000)),
                )
                if (
                    state.rate_bucket_count >= allowed
                    and dropped_reason is None
                ):
                    dropped_reason = "rate_cap"
        else:
            state.outage_active = False

        state.last_source_time_ns = event.event_time_ns
        if dropped_reason is not None:
            if owned:
                reasons[dropped_reason] += 1
                dropped_owned += 1
                _append_application_sample(
                    samples,
                    operator.fit_config.diagnostic_sample_limit,
                    event,
                    stratum,
                    dropped_reason,
                )
            continue

        reconnect = state.reconnect_pending and not state.outage_active
        duplicate_count = 0
        if not protected and _selected(
            params["duplicate_probability"],
            operator.operator_id,
            stratum.stratum_id,
            "duplicate",
            event.source_event_id,
        ):
            duplicate_count += 1
        if (
            not protected
            and reconnect
            and duplicate_count + 1 < operator.fit_config.max_outputs_per_input
            and _selected(
                params["reconnect_duplicate_probability"],
                operator.operator_id,
                stratum.stratum_id,
                "reconnect",
                event.source_event_id,
            )
        ):
            duplicate_count += 1
            transformations.append("reconnect_duplicate")
        duplicate_count = min(
            duplicate_count,
            operator.fit_config.max_outputs_per_input - 1,
        )
        if duplicate_count:
            transformations.append("duplicated")

        state.last_observed_time_ns = transformed_time
        state.last_bid = transformed_bid
        state.last_ask = transformed_ask
        state.rate_bucket_count += 1 + duplicate_count
        state.reconnect_pending = False
        if owned:
            reasons["retained"] += 1
            for duplicate_ordinal in range(duplicate_count + 1):
                tentative.append(
                    {
                        "event": event,
                        "stratum": stratum,
                        "time": transformed_time,
                        "bid": transformed_bid,
                        "ask": transformed_ask,
                        "duplicate_ordinal": duplicate_ordinal,
                        "transformations": (
                            () if protected else tuple(transformations)
                        ),
                        "protected": protected,
                    }
                )

    max_outputs = (
        operator.fit_config.max_input_events
        * operator.fit_config.max_outputs_per_input
    )
    if len(tentative) > max_outputs:
        raise ValueError("observation application exceeds output event limit")
    output_events = _finalize_output_events(operator, tentative)
    carry_state = ObservationCarryStateV1(
        operator_id=operator.operator_id,
        symbol=symbol,
        last_source_time_ns=state.last_source_time_ns,
        last_observed_time_ns=state.last_observed_time_ns,
        last_bid=state.last_bid,
        last_ask=state.last_ask,
        rate_bucket_start_ns=state.rate_bucket_start_ns,
        rate_bucket_count=state.rate_bucket_count,
        outage_bucket_start_ns=state.outage_bucket_start_ns,
        outage_active=state.outage_active,
        reconnect_pending=state.reconnect_pending,
    )
    return ObservationApplicationResultV1(
        operator_id=operator.operator_id,
        window_id=window.window_id,
        symbol=symbol,
        application_mode=application_mode,
        input_count=inspected_owned,
        output_events=output_events,
        reason_counts=dict(sorted(reasons.items())),
        fallback_counts=dict(sorted(fallbacks.items())),
        diagnostic_samples=tuple(samples),
        samples_truncated=dropped_owned > len(samples),
        carry_state=carry_state,
    )


def _fit_parameter(
    name: str,
    evidence: Sequence[ObservationFitEvidenceV1],
    config: ObservationOperatorFitConfigV1,
) -> ObservationParameterEstimateV1:
    selected = [item for item in evidence if name in item.parameter_values]
    weighted = [
        (item.parameter_values[name], item.parameter_support_counts[name])
        for item in selected
    ]
    supported_weighted = [item for item in weighted if item[1] > 0]
    source = supported_weighted or weighted
    value = _weighted_median(source)
    lower = min(item.parameter_lower_bounds[name] for item in selected)
    upper = max(item.parameter_upper_bounds[name] for item in selected)
    support = sum(item.parameter_support_counts[name] for item in selected)
    status = (
        "supported"
        if support >= config.min_parameter_support
        else "unsupported"
    )
    value = round(value, config.rounding_digits)
    lower = min(value, round(lower, config.rounding_digits))
    upper = max(value, round(upper, config.rounding_digits))
    return ObservationParameterEstimateV1(
        name=name,
        value=value,
        lower=lower,
        upper=upper,
        support_count=support,
        evidence_count=len(selected),
        support_status=status,
        estimation_bases=tuple(
            sorted({item.parameter_basis[name] for item in selected})
        ),
        evidence_ids=tuple(item.evidence_id for item in selected),
        provenance=tuple(
            sorted(
                {
                    path
                    for item in selected
                    for path in item.parameter_provenance[name]
                }
            )
        )[:MAX_OBSERVATION_PROVENANCE_PATHS],
    )


def _fit_diagnostics(
    evidence: Sequence[ObservationFitEvidenceV1],
    strata: Sequence[ObservationStratumV1],
    config: ObservationOperatorFitConfigV1,
) -> ObservationFitDiagnosticsV1:
    global_stratum = next(
        stratum for stratum in strata if stratum.level == "global"
    )
    global_parameters = global_stratum.parameter_map
    residuals: dict[str, float] = {}
    supports: dict[str, int] = {}
    unsupported: list[str] = []
    for name in OBSERVATION_PARAMETER_NAMES:
        parameter = global_parameters.get(name)
        if parameter is None:
            unsupported.append(name)
            continue
        supports[name] = parameter.support_count
        if parameter.support_status == "unsupported":
            unsupported.append(name)
        values = [
            abs(item.parameter_values[name] - parameter.value)
            for item in evidence
            if name in item.parameter_values
        ]
        if values:
            residuals[name] = round(median(values), config.rounding_digits)
    sample_rows: list[Mapping[str, JSONValue]] = []
    for stratum in sorted(strata, key=lambda item: item.key):
        if len(sample_rows) >= config.diagnostic_sample_limit:
            break
        sample_rows.append(
            {
                "stratum_id": stratum.stratum_id,
                "key": stratum.key,
                "status": stratum.status,
                "support_count": stratum.support_count,
                "fallback_keys": list(stratum.fallback_keys),
            }
        )
    return ObservationFitDiagnosticsV1(
        evidence_count=len(evidence),
        stratum_count=len(strata),
        status_counts=dict(Counter(stratum.status for stratum in strata)),
        parameter_support_counts=supports,
        parameter_residual_medians=residuals,
        unsupported_parameter_names=tuple(unsupported),
        samples=tuple(sample_rows),
        samples_truncated=len(strata) > len(sample_rows),
    )


def _finalize_output_events(
    operator: ObservationOperatorV1,
    tentative: Sequence[Mapping[str, Any]],
) -> tuple[ObservationOutputEventV1, ...]:
    ordered = sorted(
        tentative,
        key=lambda row: (
            int(row["time"]),
            cast(ObservationInputEventV1, row["event"]).event_time_ns,
            cast(ObservationInputEventV1, row["event"]).event_sequence,
            cast(ObservationInputEventV1, row["event"]).source_event_id,
            int(row["duplicate_ordinal"]),
        ),
    )
    sequence_by_time: Counter[int] = Counter()
    result: list[ObservationOutputEventV1] = []
    for row in ordered:
        event = cast(ObservationInputEventV1, row["event"])
        stratum = cast(ObservationStratumV1, row["stratum"])
        timestamp = int(row["time"])
        sequence = sequence_by_time[timestamp]
        sequence_by_time[timestamp] += 1
        result.append(
            ObservationOutputEventV1(
                source_event_id=event.source_event_id,
                operator_id=operator.operator_id,
                stratum_id=stratum.stratum_id,
                symbol=event.symbol,
                source_time_ns=event.event_time_ns,
                observed_time_ns=timestamp,
                observed_sequence=sequence,
                bid=float(row["bid"]),
                ask=float(row["ask"]),
                duplicate_ordinal=int(row["duplicate_ordinal"]),
                transformations=tuple(
                    str(value)
                    for value in cast(Sequence[Any], row["transformations"])
                ),
                protected_anchor=bool(row["protected"]),
            )
        )
    return tuple(result)


def _state_from_carry(
    carry: ObservationCarryStateV1 | None,
) -> _MutableApplicationState:
    if carry is None:
        return _MutableApplicationState()
    return _MutableApplicationState(
        last_source_time_ns=carry.last_source_time_ns,
        last_observed_time_ns=carry.last_observed_time_ns,
        last_bid=carry.last_bid,
        last_ask=carry.last_ask,
        rate_bucket_start_ns=carry.rate_bucket_start_ns,
        rate_bucket_count=carry.rate_bucket_count,
        outage_bucket_start_ns=carry.outage_bucket_start_ns,
        outage_active=carry.outage_active,
        reconnect_pending=carry.reconnect_pending,
    )


def _validate_window_alignment(
    window: ReconstructionWindowV1, parameters: Mapping[str, float]
) -> None:
    for name in ("timestamp_quantum_ns", "batch_window_ns"):
        alignment = int(parameters[name])
        if alignment <= 1:
            continue
        if alignment > MAX_OBSERVATION_WINDOW_ALIGNMENT_NS:
            raise ValueError("observation alignment exceeds v1 limit")
        if (
            window.core_start_ns % alignment != 0
            or window.core_end_ns % alignment != 0
        ):
            raise ValueError(
                "reconstruction window is not aligned to observation quantum"
            )


def _append_application_sample(
    samples: list[dict[str, JSONValue]],
    limit: int,
    event: ObservationInputEventV1,
    stratum: ObservationStratumV1,
    reason: str,
) -> None:
    if len(samples) >= limit:
        return
    samples.append(
        {
            "source_event_id": event.source_event_id,
            "source_time_ns": event.event_time_ns,
            "stratum_id": stratum.stratum_id,
            "reason": reason,
        }
    )


def _application_symbol(
    events: Sequence[ObservationInputEventV1],
    window: ReconstructionWindowV1,
) -> str:
    if events:
        return events[0].symbol
    if len(window.symbols) != 1:
        raise ValueError(
            "empty observation input requires a single-symbol window"
        )
    return _normalized_symbol(window.symbols[0])


def _context_from_pattern(
    pattern: Mapping[str, str], allowed_labels: set[str]
) -> ObservationContextV1:
    epoch_id = pattern.get("epoch_id")
    if epoch_id is None:
        epoch_id = sorted(allowed_labels)[0]
    return ObservationContextV1(
        symbol=pattern.get("symbol", "GLOBAL"),
        epoch_id=epoch_id,
        state=pattern.get("state"),
        session=pattern.get("session"),
        event_tag=pattern.get("event_tag"),
    )


def _stratum_key(level: str, pattern: Mapping[str, str]) -> str:
    if level == "global":
        return "global"
    encoded = "|".join(
        f"{name}={_context_token(value)}"
        for name, value in sorted(pattern.items())
    )
    return f"{level}|{encoded}"


def _context_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]+", "_", value.strip())


def _canonical_proxy_bounds(
    values: Mapping[str, float],
) -> dict[str, tuple[float, float]]:
    result: dict[str, tuple[float, float]] = {}
    for name, value in values.items():
        if name in _PROBABILITY_PARAMETERS:
            result[name] = (0.0, 1.0)
        elif name == "price_precision_digits":
            result[name] = (max(0.0, value - 1.0), min(16.0, value + 1.0))
        elif value == 0.0:
            result[name] = (0.0, 0.0)
        else:
            result[name] = (max(0.0, value * 0.5), value * 2.0)
    return result


def _canonical_parameter_provenance(name: str) -> tuple[str, ...]:
    mapping = {
        "retention_probability": ("identity_assumption.no_dense_denominator",),
        "unchanged_retention_probability": (
            "feature_values.stale_repeat_rate",
        ),
        "timestamp_quantum_ns": (
            "feature_values.minimum_observed_interval_ms",
        ),
        "price_precision_digits": ("feature_values.price_precision_digits",),
        "quote_transition_threshold": (
            "feature_values.price_precision_digits",
        ),
        "batch_window_ns": ("feature_values.minimum_observed_interval_ms",),
        "duplicate_probability": ("feature_values.duplicate_timestamp_rate",),
        "rate_cap_per_second": ("profile.tick_rate_per_hour",),
        "burst_window_ns": ("profile.median_interarrival_ms",),
        "quiet_gap_probability": ("feature_values.suspicious_gap_rate",),
        "outage_window_ns": ("profile.p95_interarrival_ms",),
        "reconnect_duplicate_probability": (
            "feature_values.duplicate_timestamp_rate",
        ),
    }
    return mapping[name]


def _weighted_median(values: Sequence[tuple[float, int]]) -> float:
    if not values:
        raise ValueError("cannot fit an empty parameter")
    ordered = sorted((value, max(1, weight)) for value, weight in values)
    threshold = (sum(weight for _, weight in ordered) + 1) // 2
    cumulative = 0
    for value, weight in ordered:
        cumulative += weight
        if cumulative >= threshold:
            return value
    return ordered[-1][0]


def _selected(probability: float, *parts: str) -> bool:
    selected = _probability(probability)
    if selected <= 0.0:
        return False
    if selected >= 1.0:
        return True
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") / float(2**64)
    return value < selected


def _validate_operator_lineage(
    lineage: Mapping[str, JSONValue],
    *,
    definition_id: str,
    config_id: str,
    source_hashes: tuple[str, ...],
    evidence_count: int,
) -> None:
    expected_keys = {
        "feed_epoch_definition_id",
        "config_id",
        "evidence_count",
        "evidence_ids",
        "source_hashes",
        "sources",
        "lineage_sha256",
    }
    if set(lineage) != expected_keys:
        raise ValueError("operator lineage fields differ from v1 contract")
    if lineage.get("feed_epoch_definition_id") != definition_id:
        raise ValueError("operator lineage epoch definition differs")
    if lineage.get("config_id") != config_id:
        raise ValueError("operator lineage config differs")
    if lineage.get("evidence_count") != evidence_count:
        raise ValueError("operator lineage evidence count differs")
    evidence_ids = tuple(
        _required_sha256_id(value, "evidence_id", prefix="observation-evidence")
        for value in _sequence(lineage.get("evidence_ids"))
    )
    if (
        len(evidence_ids) != evidence_count
        or len(set(evidence_ids)) != evidence_count
    ):
        raise ValueError("operator lineage evidence IDs differ")
    lineage_hashes = tuple(
        sorted(
            _required_sha256_id(value, "source_artifact_sha256")
            for value in _sequence(lineage.get("source_hashes"))
        )
    )
    if lineage_hashes != source_hashes:
        raise ValueError("operator lineage source hashes differ")
    sources = _sequence(lineage.get("sources"))
    if len(sources) != evidence_count:
        raise ValueError("operator lineage source count differs")
    source_evidence_ids: list[str] = []
    source_artifact_hashes: set[str] = set()
    expected_source_keys = {
        "evidence_id",
        "source_evidence_id",
        "source_artifact_sha256",
        "source_hash_basis",
        "evidence_kind",
    }
    for value in sources:
        source = _mapping(value)
        if set(source) != expected_source_keys:
            raise ValueError("operator lineage source fields differ")
        source_evidence_ids.append(
            _required_sha256_id(
                source.get("evidence_id"),
                "evidence_id",
                prefix="observation-evidence",
            )
        )
        _required_text(source.get("source_evidence_id"))
        source_hash = _required_sha256_id(
            source.get("source_artifact_sha256"),
            "source_artifact_sha256",
        )
        source_artifact_hashes.add(source_hash)
        if _required_text(source.get("source_hash_basis")) not in (
            _SOURCE_HASH_BASES
        ):
            raise ValueError("operator lineage source hash basis differs")
        if _required_text(source.get("evidence_kind")) not in _EVIDENCE_KINDS:
            raise ValueError("operator lineage evidence kind differs")
    if tuple(source_evidence_ids) != evidence_ids:
        raise ValueError("operator lineage source evidence order differs")
    if source_artifact_hashes != set(source_hashes):
        raise ValueError("operator lineage source hash coverage differs")
    supplied = _required_sha256_id(
        lineage.get("lineage_sha256"), "lineage_sha256"
    )
    material = {
        key: value for key, value in lineage.items() if key != "lineage_sha256"
    }
    if supplied != _payload_sha256(cast(Mapping[str, JSONValue], material)):
        raise ValueError("operator lineage hash differs")


def _parameter_value(name: str, value: Any) -> float:
    result = _finite_float(value, name)
    if name in _PROBABILITY_PARAMETERS and not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must be between zero and one")
    if name in _NONNEGATIVE_PARAMETERS and result < 0.0:
        raise ValueError(f"{name} must be non-negative")
    if name in _INTEGER_PARAMETERS and not result.is_integer():
        raise ValueError(f"{name} must be integral")
    if name == "price_precision_digits" and result > 16:
        raise ValueError("price_precision_digits exceeds sixteen")
    if (
        name in {"timestamp_quantum_ns", "batch_window_ns"}
        and result > MAX_OBSERVATION_WINDOW_ALIGNMENT_NS
    ):
        raise ValueError(f"{name} exceeds v1 alignment limit")
    if (
        name in {"burst_window_ns", "outage_window_ns"}
        and result > MAX_OBSERVATION_DURATION_NS
    ):
        raise ValueError(f"{name} exceeds v1 duration limit")
    return result


def _stable_id(prefix: str, value: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(value).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _payload_sha256(value: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(value).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _required_sha256_id(
    value: Any, name: str, *, prefix: str | None = None
) -> str:
    text = _required_text(value)
    expected_prefix = f"{prefix}:sha256:" if prefix is not None else "sha256:"
    if prefix is not None and text.startswith(expected_prefix):
        digest = text.removeprefix(expected_prefix)
        if not _SHA256_RE.fullmatch(digest):
            raise ValueError(f"{name} must be a sha256 identifier")
        return expected_prefix + digest
    digest = text.removeprefix("sha256:")
    if not _SHA256_RE.fullmatch(digest):
        raise ValueError(f"{name} must be a sha256 identifier")
    return "sha256:" + digest


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text or len(text) > MAX_OBSERVATION_TEXT_LENGTH:
        raise ValueError("text value is empty or unbounded")
    return text


def _normalized_symbol(value: Any) -> str:
    symbol = _required_text(value).upper()
    if not re.fullmatch(r"[A-Z0-9._:-]+", symbol):
        raise ValueError("observation symbol contains unsupported characters")
    return symbol


def _valid_period(value: str) -> bool:
    if not _PERIOD_RE.fullmatch(value):
        return False
    if len(value) == 6 and not 1 <= int(value[4:]) <= 12:
        return False
    return True


def _optional_context_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return _context_token(text) if text else None


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _optional_strict_int(value: Any, name: str) -> int | None:
    return None if value is None else _strict_int(value, name)


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside [{minimum}, {maximum}]")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _positive_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0.0:
        raise ValueError(f"{name} must be positive")
    return result


def _optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _optional_int(value: Any) -> int | None:
    try:
        return _strict_int(value, "value")
    except ValueError:
        return None


def _probability(value: Any) -> float:
    return min(1.0, max(0.0, _finite_float(value, "probability")))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return value
    return ()


def _mapping_optional_text(data: Mapping[str, Any], name: str) -> str | None:
    value = data.get(name)
    return None if value is None else str(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _string_mapping(value: Any) -> dict[str, str]:
    return {str(name): str(item) for name, item in _mapping(value).items()}


def _float_mapping(value: Any) -> dict[str, float]:
    return {
        str(name): _finite_float(item, str(name))
        for name, item in _mapping(value).items()
    }


def _int_mapping(value: Any) -> dict[str, int]:
    return {
        str(name): _strict_int(item, str(name))
        for name, item in _mapping(value).items()
    }


def _bounded_text_tuple(
    values: Sequence[str],
    name: str,
    *,
    limit: int = MAX_OBSERVATION_EVIDENCE,
) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(_required_text(value) for value in values))
    if not result or len(result) > limit:
        raise ValueError(f"{name} values are empty or unbounded")
    return result


def _validated_count_mapping(
    values: Mapping[str, int], *, allowed_names: set[str] | None = None
) -> dict[str, int]:
    result = {
        _required_text(name): _bounded_int(
            count,
            f"{name} count",
            0,
            MAX_OBSERVATION_INPUT_EVENTS * MAX_OBSERVATION_OUTPUTS_PER_INPUT,
        )
        for name, count in sorted(values.items())
    }
    if allowed_names is not None and not set(result).issubset(allowed_names):
        raise ValueError("observation count names differ from v1 contract")
    return result

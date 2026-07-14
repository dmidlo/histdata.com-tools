"""Deterministic empirical-motif candidate generation.

This module owns the first variable-cardinality reconstruction candidate.  It
is deliberately upstream of hard carving, cross-series reconciliation,
broker conditioning, and final persistence.  A generated event is therefore
an auditable proposal, never an accepted or final synthetic tick.

Generation is performed for one pair of immutable observed anchors.  The
complete anchor interval is planned from semantic inputs, then each streaming
window emits only the timestamps it owns.  Seeds, transformed values, and
event identities never include a worker, retry, or window identifier.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import math
from typing import Any

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.benchmark import (
    BENCHMARK_EVENT_SCHEMA_VERSION,
    BenchmarkCandidateV1,
    BenchmarkEventV1,
    BenchmarkGeneratorV1,
    BenchmarkScenarioV1,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
    derive_anchor_interval_id,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.motifs import (
    ReferenceMotifConditionV1,
    ReferenceMotifFragmentV1,
    ReferenceMotifIndexV1,
    ReferenceMotifMatchV1,
    ReferenceMotifQueryResultV1,
    ReferenceMotifQueryStatus,
    ReferenceMotifQueryV1,
    query_reference_motifs,
)
from histdatacom.synthetic.streaming import (
    CarryStateV1,
    ReconstructionResourceEstimateV1,
    ReconstructionResourceLimitError,
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

MOTIF_GENERATOR_CONFIG_SCHEMA_VERSION = (
    "histdatacom.empirical-motif-generator-config.v1"
)
MOTIF_TRANSFORMATION_SCHEMA_VERSION = (
    "histdatacom.empirical-motif-transformation.v1"
)
MOTIF_EVENT_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.empirical-motif-event-lineage.v1"
)
MOTIF_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.empirical-motif-candidate-batch.v1"
)

EMPIRICAL_MOTIF_GENERATOR_ID = "histdatacom.empirical-motif-resampling"
EMPIRICAL_MOTIF_GENERATOR_VERSION = "1.1.0"
MOTIF_TRANSFORMATION_CONFIDENCE_QUANTITY = (
    "uncalibrated-motif-match-similarity-v1"
)
CANDIDATE_ONLY_CONSTRAINT_SET_ID = (
    "histdatacom.constraint-set.candidate-pre-carving.v1"
)

MAX_MOTIF_GENERATED_EVENTS_PER_INTERVAL = 100_000
MAX_MOTIF_TRANSFORMATIONS_PER_INTERVAL = 100_000
MAX_MOTIF_DECISION_DETAILS = 32
MAX_MOTIF_DECISION_DETAIL_TEXT = 1_024
MAX_PRICE_PRECISION_DIGITS = 12
NANOSECONDS_PER_SECOND = 1_000_000_000


class MotifGenerationStatus(str, Enum):
    """Whether an interval emitted candidates or made an explicit decision."""

    GENERATED = "generated"
    EMPTY = "empty"
    REFUSED = "refused"


class MotifGenerationDecision(str, Enum):
    """Bounded generation and refusal reason codes."""

    GENERATED = "generated"
    CLOSED_SESSION = "closed_session"
    ZERO_TARGET_ACTIVITY = "zero_target_activity"
    OUTSIDE_WINDOW_OWNERSHIP = "outside_window_ownership"
    ZERO_WIDTH_ANCHOR = "zero_width_anchor"
    REVERSED_ANCHOR = "reversed_anchor"
    NO_SUPPORTED_CELL = "no_supported_cell"
    NOT_AVAILABLE_AS_OF = "not_available_as_of"
    INTERVAL_EVENT_LIMIT = "interval_event_limit"
    RESOURCE_LIMIT = "resource_limit"
    UNSUPPORTED_TRANSFORM = "unsupported_transform"
    INVALID_TRANSFORMED_QUOTE = "invalid_transformed_quote"


@dataclass(frozen=True, slots=True)
class EmpiricalMotifGeneratorConfigV1:
    """Versioned candidate-only resampling and resource assumptions."""

    max_events_per_interval: int = 50_000
    max_transformations_per_interval: int = 25_000
    estimated_bytes_per_event: int = 512
    fallback_price_precision_digits: int = 8
    confidence_rounding_digits: int = 12
    closed_session_states: tuple[str, ...] = (
        "closed",
        "market_closed",
        "weekend_closed",
    )
    constraint_set_id: str = CANDIDATE_ONLY_CONSTRAINT_SET_ID
    config_id: str = ""
    schema_version: str = MOTIF_GENERATOR_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MOTIF_GENERATOR_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported empirical motif generator config")
        for name, upper in (
            (
                "max_events_per_interval",
                MAX_MOTIF_GENERATED_EVENTS_PER_INTERVAL,
            ),
            (
                "max_transformations_per_interval",
                MAX_MOTIF_TRANSFORMATIONS_PER_INTERVAL,
            ),
        ):
            value = _positive_int(getattr(self, name), name)
            if value > upper:
                raise ValueError(f"{name} exceeds the version-one bound")
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "estimated_bytes_per_event",
            _positive_int(
                self.estimated_bytes_per_event,
                "estimated_bytes_per_event",
            ),
        )
        for name, upper in (
            ("fallback_price_precision_digits", MAX_PRICE_PRECISION_DIGITS),
            ("confidence_rounding_digits", MAX_PRICE_PRECISION_DIGITS),
        ):
            value = _nonnegative_int(getattr(self, name), name)
            if value > upper:
                raise ValueError(f"{name} exceeds the supported precision")
            object.__setattr__(self, name, value)
        states = tuple(
            sorted(
                {
                    _required_text(item).strip().lower()
                    for item in self.closed_session_states
                }
            )
        )
        if not states:
            raise ValueError("closed_session_states cannot be empty")
        object.__setattr__(self, "closed_session_states", states)
        if self.constraint_set_id != CANDIDATE_ONLY_CONSTRAINT_SET_ID:
            raise ValueError(
                "v1 motif generation must declare the pre-carving "
                "candidate constraint set"
            )
        expected = _stable_id(
            "empirical-motif-generator-config", self.identity_payload()
        )
        supplied = _optional_text(self.config_id)
        if supplied is not None and supplied != expected:
            raise ValueError("empirical motif generator config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic configuration used for deterministic identity."""
        return {
            "schema_version": self.schema_version,
            "generator_id": EMPIRICAL_MOTIF_GENERATOR_ID,
            "generator_version": EMPIRICAL_MOTIF_GENERATOR_VERSION,
            "fallback_price_precision_digits": (
                self.fallback_price_precision_digits
            ),
            "confidence_rounding_digits": self.confidence_rounding_digits,
            "closed_session_states": list(self.closed_session_states),
            "constraint_set_id": self.constraint_set_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "max_events_per_interval": self.max_events_per_interval,
            "max_transformations_per_interval": (
                self.max_transformations_per_interval
            ),
            "estimated_bytes_per_event": self.estimated_bytes_per_event,
            "execution_limits_in_config_identity": False,
            "config_id": self.config_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EmpiricalMotifGeneratorConfigV1":
        return cls(
            max_events_per_interval=_strict_int(
                data.get("max_events_per_interval"),
                "max_events_per_interval",
            ),
            max_transformations_per_interval=_strict_int(
                data.get("max_transformations_per_interval"),
                "max_transformations_per_interval",
            ),
            estimated_bytes_per_event=_strict_int(
                data.get("estimated_bytes_per_event"),
                "estimated_bytes_per_event",
            ),
            fallback_price_precision_digits=_strict_int(
                data.get("fallback_price_precision_digits"),
                "fallback_price_precision_digits",
            ),
            confidence_rounding_digits=_strict_int(
                data.get("confidence_rounding_digits"),
                "confidence_rounding_digits",
            ),
            closed_session_states=_string_tuple(
                data.get("closed_session_states"),
                "closed_session_states",
            ),
            constraint_set_id=str(data.get("constraint_set_id", "")),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EmpiricalMotifGeneratorConfigV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class EmpiricalMotifTransformationV1:
    """One bounded source-fragment transform applied to output ordinals."""

    index_id: str
    query_id: str
    query_result_id: str
    source_fragment_id: str
    source_window_id: str
    source_series_id: str
    source_period: str
    source_artifact_sha256: str
    backoff_level: str
    cell_support: int
    match_distance: float
    segment_ordinal: int
    output_start_ordinal: int
    output_end_ordinal: int
    source_event_count: int
    time_scale: float
    time_warp_ratio: float
    requested_price_scale: float
    applied_price_scale: float
    price_scale_clamped: bool
    spread_shape_applied: bool
    seed: int
    confidence: float
    transformation_id: str = ""
    schema_version: str = MOTIF_TRANSFORMATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MOTIF_TRANSFORMATION_SCHEMA_VERSION:
            raise ValueError("unsupported empirical motif transformation")
        for name in (
            "index_id",
            "query_id",
            "query_result_id",
            "source_fragment_id",
            "source_window_id",
            "source_series_id",
            "source_period",
            "source_artifact_sha256",
            "backoff_level",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in (
            "cell_support",
            "segment_ordinal",
            "output_start_ordinal",
            "output_end_ordinal",
            "source_event_count",
        ):
            object.__setattr__(
                self, name, _positive_int(getattr(self, name), name)
            )
        if self.output_end_ordinal < self.output_start_ordinal:
            raise ValueError("transformation output ordinal range is reversed")
        for name in (
            "match_distance",
            "time_scale",
            "time_warp_ratio",
            "requested_price_scale",
            "applied_price_scale",
        ):
            value = _finite_float(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        for name in ("price_scale_clamped", "spread_shape_applied"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be boolean")
        object.__setattr__(self, "seed", _nonnegative_int(self.seed, "seed"))
        confidence = _finite_float(self.confidence, "confidence")
        if not 0.0 <= confidence <= 1.0:
            raise ValueError("transformation confidence is outside [0,1]")
        object.__setattr__(self, "confidence", confidence)
        expected = _stable_id("empirical-motif-transformation", self.payload())
        supplied = _optional_text(self.transformation_id)
        if supplied is not None and supplied != expected:
            raise ValueError("empirical motif transformation_id differs")
        object.__setattr__(self, "transformation_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_id": self.index_id,
            "query_id": self.query_id,
            "query_result_id": self.query_result_id,
            "source_fragment_id": self.source_fragment_id,
            "source_window_id": self.source_window_id,
            "source_series_id": self.source_series_id,
            "source_period": self.source_period,
            "source_artifact_sha256": self.source_artifact_sha256,
            "backoff_level": self.backoff_level,
            "cell_support": self.cell_support,
            "match_distance": self.match_distance,
            "segment_ordinal": self.segment_ordinal,
            "output_start_ordinal": self.output_start_ordinal,
            "output_end_ordinal": self.output_end_ordinal,
            "source_event_count": self.source_event_count,
            "time_scale": self.time_scale,
            "time_warp_ratio": self.time_warp_ratio,
            "requested_price_scale": self.requested_price_scale,
            "applied_price_scale": self.applied_price_scale,
            "price_scale_clamped": self.price_scale_clamped,
            "spread_shape_applied": self.spread_shape_applied,
            "seed": self.seed,
            "confidence": self.confidence,
            "endpoint_alignment": "detrended-linear-anchor-bridge-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "transformation_id": self.transformation_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EmpiricalMotifTransformationV1":
        return cls(
            index_id=str(data.get("index_id", "")),
            query_id=str(data.get("query_id", "")),
            query_result_id=str(data.get("query_result_id", "")),
            source_fragment_id=str(data.get("source_fragment_id", "")),
            source_window_id=str(data.get("source_window_id", "")),
            source_series_id=str(data.get("source_series_id", "")),
            source_period=str(data.get("source_period", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            backoff_level=str(data.get("backoff_level", "")),
            cell_support=_strict_int(data.get("cell_support"), "cell_support"),
            match_distance=_finite_float(
                data.get("match_distance"), "match_distance"
            ),
            segment_ordinal=_strict_int(
                data.get("segment_ordinal"), "segment_ordinal"
            ),
            output_start_ordinal=_strict_int(
                data.get("output_start_ordinal"), "output_start_ordinal"
            ),
            output_end_ordinal=_strict_int(
                data.get("output_end_ordinal"), "output_end_ordinal"
            ),
            source_event_count=_strict_int(
                data.get("source_event_count"), "source_event_count"
            ),
            time_scale=_finite_float(data.get("time_scale"), "time_scale"),
            time_warp_ratio=_finite_float(
                data.get("time_warp_ratio"), "time_warp_ratio"
            ),
            requested_price_scale=_finite_float(
                data.get("requested_price_scale"), "requested_price_scale"
            ),
            applied_price_scale=_finite_float(
                data.get("applied_price_scale"), "applied_price_scale"
            ),
            price_scale_clamped=_strict_bool(
                data.get("price_scale_clamped"), "price_scale_clamped"
            ),
            spread_shape_applied=_strict_bool(
                data.get("spread_shape_applied"), "spread_shape_applied"
            ),
            seed=_strict_int(data.get("seed"), "seed"),
            confidence=_finite_float(data.get("confidence"), "confidence"),
            transformation_id=str(data.get("transformation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EmpiricalMotifTransformationV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class EmpiricalMotifEventLineageV1:
    """Compact per-event pointer into one recoverable transform."""

    event_id: str
    transformation_id: str
    global_event_ordinal: int
    segment_event_ordinal: int
    source_progress: float
    anchor_progress: float
    requested_event_time_ns: int
    schema_version: str = MOTIF_EVENT_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MOTIF_EVENT_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported empirical motif event lineage")
        object.__setattr__(self, "event_id", _required_text(self.event_id))
        object.__setattr__(
            self,
            "transformation_id",
            _required_text(self.transformation_id),
        )
        for name in ("global_event_ordinal", "segment_event_ordinal"):
            object.__setattr__(
                self, name, _positive_int(getattr(self, name), name)
            )
        for name in ("source_progress", "anchor_progress"):
            value = _finite_float(getattr(self, name), name)
            if not 0.0 < value <= 1.0:
                raise ValueError(f"{name} must be inside (0,1]")
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "requested_event_time_ns",
            _strict_int(
                self.requested_event_time_ns, "requested_event_time_ns"
            ),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "transformation_id": self.transformation_id,
            "global_event_ordinal": self.global_event_ordinal,
            "segment_event_ordinal": self.segment_event_ordinal,
            "source_progress": self.source_progress,
            "anchor_progress": self.anchor_progress,
            "requested_event_time_ns": self.requested_event_time_ns,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EmpiricalMotifEventLineageV1":
        return cls(
            event_id=str(data.get("event_id", "")),
            transformation_id=str(data.get("transformation_id", "")),
            global_event_ordinal=_strict_int(
                data.get("global_event_ordinal"), "global_event_ordinal"
            ),
            segment_event_ordinal=_strict_int(
                data.get("segment_event_ordinal"), "segment_event_ordinal"
            ),
            source_progress=_finite_float(
                data.get("source_progress"), "source_progress"
            ),
            anchor_progress=_finite_float(
                data.get("anchor_progress"), "anchor_progress"
            ),
            requested_event_time_ns=_strict_int(
                data.get("requested_event_time_ns"),
                "requested_event_time_ns",
            ),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EmpiricalMotifEventLineageV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class EmpiricalMotifCandidateBatchV1:
    """Process-local candidate rows plus bounded deterministic evidence."""

    run_id: str
    window_id: str
    ensemble_member_id: str
    symbol: str
    anchor_interval_id: str
    left_anchor_event_id: str
    right_anchor_event_id: str
    generator_config: EmpiricalMotifGeneratorConfigV1
    query_result: ReferenceMotifQueryResultV1
    status: MotifGenerationStatus
    decision: MotifGenerationDecision
    target_event_count: int
    events: tuple[SyntheticEventV1, ...]
    transformations: tuple[EmpiricalMotifTransformationV1, ...]
    event_lineage: tuple[EmpiricalMotifEventLineageV1, ...]
    resource_estimate: ReconstructionResourceEstimateV1
    carry_state: CarryStateV1
    decision_details: tuple[str, ...] = ()
    batch_id: str = ""
    schema_version: str = MOTIF_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MOTIF_CANDIDATE_BATCH_SCHEMA_VERSION:
            raise ValueError("unsupported empirical motif candidate batch")
        for name in (
            "run_id",
            "window_id",
            "ensemble_member_id",
            "symbol",
            "anchor_interval_id",
            "left_anchor_event_id",
            "right_anchor_event_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(
            self.generator_config, EmpiricalMotifGeneratorConfigV1
        ):
            raise ValueError("candidate batch requires a v1 generator config")
        if not isinstance(self.query_result, ReferenceMotifQueryResultV1):
            raise ValueError("candidate batch requires a v1 motif query result")
        object.__setattr__(self, "status", MotifGenerationStatus(self.status))
        object.__setattr__(
            self, "decision", MotifGenerationDecision(self.decision)
        )
        target_count = _nonnegative_int(
            self.target_event_count, "target_event_count"
        )
        object.__setattr__(self, "target_event_count", target_count)
        events = tuple(
            sorted(
                self.events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.event_id,
                ),
            )
        )
        transforms = tuple(
            sorted(
                self.transformations,
                key=lambda item: (
                    item.segment_ordinal,
                    item.transformation_id,
                ),
            )
        )
        lineages = tuple(
            sorted(
                self.event_lineage,
                key=lambda item: (
                    item.global_event_ordinal,
                    item.event_id,
                ),
            )
        )
        if any(
            item.origin is not SyntheticEventOrigin.SYNTHETIC for item in events
        ):
            raise ValueError(
                "candidate batch can contain only synthetic events"
            )
        if len(events) != len(lineages):
            raise ValueError(
                "candidate events and event lineage do not reconcile"
            )
        if len(events) > target_count:
            raise ValueError("owned candidate count exceeds interval target")
        if self.status is MotifGenerationStatus.GENERATED and not events:
            raise ValueError("generated candidate batch requires events")
        if self.status is not MotifGenerationStatus.GENERATED and events:
            raise ValueError(
                "empty or refused candidate batch cannot have events"
            )
        if (self.status is MotifGenerationStatus.GENERATED) != (
            self.decision is MotifGenerationDecision.GENERATED
        ):
            raise ValueError("candidate status and decision disagree")
        event_ids = {item.event_id for item in events}
        if len(event_ids) != len(events):
            raise ValueError("candidate batch has duplicate event IDs")
        if event_ids != {item.event_id for item in lineages}:
            raise ValueError("event lineage does not cover candidate event IDs")
        transform_ids = {item.transformation_id for item in transforms}
        if len(transform_ids) != len(transforms):
            raise ValueError("candidate batch has duplicate transformations")
        if len({item.segment_ordinal for item in transforms}) != len(
            transforms
        ):
            raise ValueError("candidate batch has duplicate transform ordinals")
        if len({item.global_event_ordinal for item in lineages}) != len(
            lineages
        ):
            raise ValueError("candidate batch has duplicate global ordinals")
        if any(
            item.transformation_id not in transform_ids for item in lineages
        ):
            raise ValueError(
                "event lineage references an absent transformation"
            )
        if any(
            item.index_id != self.query_result.index_id
            or item.query_id != self.query_result.query.query_id
            or item.query_result_id != self.query_result.result_id
            for item in transforms
        ):
            raise ValueError("transformation differs from batch query lineage")
        matched_fragment_ids = {
            item.fragment.fragment_id for item in self.query_result.matches
        }
        if any(
            item.source_fragment_id not in matched_fragment_ids
            for item in transforms
        ):
            raise ValueError("transformation source was not a retrieved match")
        transform_by_id = {item.transformation_id: item for item in transforms}
        event_by_id = {item.event_id: item for item in events}
        for lineage in lineages:
            event = event_by_id[lineage.event_id]
            transform = transform_by_id[lineage.transformation_id]
            if (
                lineage.requested_event_time_ns != event.event_time_ns
                or lineage.global_event_ordinal != event.event_sequence
                or event.reference_id != self.query_result.result_id
                or event.motif_id != transform.source_fragment_id
                or event.confidence is not None
                or not (
                    transform.output_start_ordinal
                    <= lineage.global_event_ordinal
                    <= transform.output_end_ordinal
                )
                or lineage.segment_event_ordinal
                != (
                    lineage.global_event_ordinal
                    - transform.output_start_ordinal
                    + 1
                )
            ):
                raise ValueError("candidate event-to-transform lineage differs")
        if any(
            item.run_id != self.run_id
            or item.ensemble_member_id != self.ensemble_member_id
            or item.symbol != self.symbol
            or item.anchor_interval_id != self.anchor_interval_id
            or item.left_anchor_event_id != self.left_anchor_event_id
            or item.right_anchor_event_id != self.right_anchor_event_id
            or item.generator_config_id != self.generator_config.config_id
            or item.constraint_set_id != CANDIDATE_ONLY_CONSTRAINT_SET_ID
            for item in events
        ):
            raise ValueError("candidate event lineage differs from its batch")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "transformations", transforms)
        object.__setattr__(self, "event_lineage", lineages)
        if not isinstance(
            self.resource_estimate, ReconstructionResourceEstimateV1
        ):
            raise ValueError("candidate batch requires a resource estimate")
        if not isinstance(self.carry_state, CarryStateV1):
            raise ValueError("candidate batch requires v1 carry state")
        if (
            self.carry_state.run_id != self.run_id
            or self.carry_state.ensemble_member_id != self.ensemble_member_id
            or self.symbol not in self.carry_state.symbol_watermarks_ns
        ):
            raise ValueError("candidate batch carry state differs from scope")
        details = tuple(
            _bounded_text(
                item,
                "decision_detail",
                MAX_MOTIF_DECISION_DETAIL_TEXT,
            )
            for item in self.decision_details
        )
        if len(details) > MAX_MOTIF_DECISION_DETAILS:
            raise ValueError("candidate decision details exceed bounded limit")
        object.__setattr__(self, "decision_details", details)
        expected = _stable_id("empirical-motif-candidate-batch", self.payload())
        supplied = _optional_text(self.batch_id)
        if supplied is not None and supplied != expected:
            raise ValueError("empirical motif candidate batch_id differs")
        object.__setattr__(self, "batch_id", expected)

    @property
    def generator_config_id(self) -> str:
        """Return the semantic config ID repeated by every candidate event."""
        return self.generator_config.config_id

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic batch identity without embedding row payloads."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "anchor_interval_id": self.anchor_interval_id,
            "left_anchor_event_id": self.left_anchor_event_id,
            "right_anchor_event_id": self.right_anchor_event_id,
            "generator_config_id": self.generator_config.config_id,
            "query_result_id": self.query_result.result_id,
            "status": self.status.value,
            "decision": self.decision.value,
            "target_event_count": self.target_event_count,
            "owned_event_count": len(self.events),
            "transformation_count": len(self.transformations),
            "event_lineage_count": len(self.event_lineage),
            "event_content_sha256": _content_sha256(
                [item.to_dict() for item in self.events]
            ),
            "transformation_content_sha256": _content_sha256(
                [item.to_dict() for item in self.transformations]
            ),
            "event_lineage_content_sha256": _content_sha256(
                [item.to_dict() for item in self.event_lineage]
            ),
            "resource_estimate_id": self.resource_estimate.estimate_id,
            "carry_id": self.carry_state.carry_id,
            "decision_details": list(self.decision_details),
            "candidate_only": True,
            "hard_carving_status": "not_evaluated",
            "broker_conditioning_status": "not_applied",
            "final_storage_status": "not_persisted",
        }

    def metadata(self) -> dict[str, JSONValue]:
        """Return workflow-safe metadata while keeping candidate rows external."""
        attempts: list[JSONValue] = [
            item.to_dict() for item in self.query_result.backoff_attempts
        ]
        return {
            **self.payload(),
            "batch_id": self.batch_id,
            "generator_config": self.generator_config.to_dict(),
            "condition": self.query_result.query.condition.to_dict(),
            "query_status": self.query_result.status.value,
            "backoff_attempts": attempts,
            "events_inline": False,
            "transformations_inline": False,
            "event_lineage_inline": False,
        }

    def lineage_for(self, event_id: str) -> EmpiricalMotifEventLineageV1:
        """Return the compact lineage record for one emitted candidate."""
        wanted = _required_text(event_id)
        for lineage in self.event_lineage:
            if lineage.event_id == wanted:
                return lineage
        raise KeyError(wanted)

    def merged_stream(
        self,
        observed_events: Sequence[SyntheticEventV1],
    ) -> SyntheticEventStreamV1:
        """Merge proposals with the caller's unchanged observed objects."""
        return SyntheticEventStreamV1.merge(
            run_id=self.run_id,
            ensemble_member_id=self.ensemble_member_id,
            symbol=self.symbol,
            observed_events=observed_events,
            synthetic_events=self.events,
        )


@dataclass(frozen=True, slots=True)
class _PlannedTransform:
    match: ReferenceMotifMatchV1
    record: EmpiricalMotifTransformationV1
    start_index: int
    event_count: int


def generate_empirical_motif_candidates(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    query_result: ReferenceMotifQueryResultV1,
    config: EmpiricalMotifGeneratorConfigV1,
) -> EmpiricalMotifCandidateBatchV1:
    """Generate one deterministic, window-owned candidate anchor interval."""
    _validate_generation_scope(
        run,
        window,
        left_anchor,
        right_anchor,
        query_result,
        config,
    )
    interval_id = derive_anchor_interval_id(
        left_anchor.event_id, right_anchor.event_id
    )
    gap_ns = right_anchor.event_time_ns - left_anchor.event_time_ns
    zero_estimate = _resource_estimate(0, config)
    if gap_ns == 0:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=MotifGenerationDecision.ZERO_WIDTH_ANCHOR,
            target_event_count=0,
            estimate=zero_estimate,
        )
    if gap_ns < 0:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=MotifGenerationDecision.REVERSED_ANCHOR,
            target_event_count=0,
            estimate=zero_estimate,
        )

    condition = query_result.query.condition
    if condition.session_state.strip().lower() in config.closed_session_states:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.EMPTY,
            decision=MotifGenerationDecision.CLOSED_SESSION,
            target_event_count=0,
            estimate=zero_estimate,
        )

    target_count, cadence_ns = _target_cardinality(condition, gap_ns)
    if target_count == 0:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.EMPTY,
            decision=MotifGenerationDecision.ZERO_TARGET_ACTIVITY,
            target_event_count=0,
            estimate=zero_estimate,
        )
    estimate = _resource_estimate(target_count, config)
    if query_result.status is not ReferenceMotifQueryStatus.MATCHED:
        decision = (
            MotifGenerationDecision.NOT_AVAILABLE_AS_OF
            if query_result.status
            is ReferenceMotifQueryStatus.NOT_AVAILABLE_AS_OF
            else MotifGenerationDecision.NO_SUPPORTED_CELL
        )
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=decision,
            target_event_count=target_count,
            estimate=estimate,
        )
    if target_count > config.max_events_per_interval:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=MotifGenerationDecision.INTERVAL_EVENT_LIMIT,
            target_event_count=target_count,
            estimate=estimate,
            details=(
                f"target {target_count} exceeds interval limit "
                f"{config.max_events_per_interval}",
            ),
        )
    try:
        run.storage_policy.preflight(estimate)
    except ReconstructionResourceLimitError as err:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=MotifGenerationDecision.RESOURCE_LIMIT,
            target_event_count=target_count,
            estimate=err.estimate,
            details=err.violations,
        )
    event_times = _candidate_times(
        left_anchor.event_time_ns,
        right_anchor.event_time_ns,
        target_count,
        cadence_ns,
        condition,
    )
    plans, plan_error = _plan_transforms(
        run=run,
        ensemble_member_id=window.ensemble_member_id,
        interval_id=interval_id,
        event_times=event_times,
        cadence_ns=cadence_ns,
        left_time_ns=left_anchor.event_time_ns,
        query_result=query_result,
        config=config,
    )
    if plan_error is not None:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=MotifGenerationDecision.UNSUPPORTED_TRANSFORM,
            target_event_count=target_count,
            estimate=estimate,
            details=(plan_error,),
        )

    all_events: list[SyntheticEventV1] = []
    all_lineage: list[EmpiricalMotifEventLineageV1] = []
    quote_error: str | None = None
    for plan in plans:
        generated, lineages, error = _events_for_transform(
            run=run,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            event_times=event_times,
            plan=plan,
        )
        if error is not None:
            quote_error = error
            break
        all_events.extend(generated)
        all_lineage.extend(lineages)
    if quote_error is not None:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.REFUSED,
            decision=MotifGenerationDecision.INVALID_TRANSFORMED_QUOTE,
            target_event_count=target_count,
            estimate=estimate,
            details=(quote_error,),
        )

    owned_ids = {
        event.event_id
        for event in all_events
        if window.owns_event_time(event.event_time_ns)
    }
    owned_events = tuple(
        item for item in all_events if item.event_id in owned_ids
    )
    owned_lineage = tuple(
        item for item in all_lineage if item.event_id in owned_ids
    )
    referenced_transform_ids = {
        item.transformation_id for item in owned_lineage
    }
    owned_transforms = tuple(
        plan.record
        for plan in plans
        if plan.record.transformation_id in referenced_transform_ids
    )
    if not owned_events:
        return _decision_batch(
            run=run,
            window=window,
            left_anchor=left_anchor,
            right_anchor=right_anchor,
            interval_id=interval_id,
            query_result=query_result,
            config=config,
            status=MotifGenerationStatus.EMPTY,
            decision=MotifGenerationDecision.OUTSIDE_WINDOW_OWNERSHIP,
            target_event_count=target_count,
            estimate=estimate,
        )
    return EmpiricalMotifCandidateBatchV1(
        run_id=run.run_id,
        window_id=window.window_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol=left_anchor.symbol,
        anchor_interval_id=interval_id,
        left_anchor_event_id=left_anchor.event_id,
        right_anchor_event_id=right_anchor.event_id,
        generator_config=config,
        query_result=query_result,
        status=MotifGenerationStatus.GENERATED,
        decision=MotifGenerationDecision.GENERATED,
        target_event_count=target_count,
        events=owned_events,
        transformations=owned_transforms,
        event_lineage=owned_lineage,
        resource_estimate=estimate,
        carry_state=_carry_state(
            run, window, left_anchor, right_anchor, owned_events
        ),
    )


@dataclass(frozen=True, slots=True)
class EmpiricalMotifBenchmarkGeneratorV1(BenchmarkGeneratorV1):
    """Adapter exposing motif candidates to reverse-degradation scorecards."""

    candidate: BenchmarkCandidateV1
    run: ReconstructionRunV1
    motif_index: ReferenceMotifIndexV1
    condition: ReferenceMotifConditionV1
    config: EmpiricalMotifGeneratorConfigV1
    candidate_id: str = field(init=False)
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION
    as_of_ns: int | None = None
    event_schema_version: str = BENCHMARK_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.candidate.method_id != EMPIRICAL_MOTIF_GENERATOR_ID:
            raise ValueError(
                "benchmark candidate method is not motif generation"
            )
        if self.event_schema_version != BENCHMARK_EVENT_SCHEMA_VERSION:
            raise ValueError("benchmark adapter requires event schema v1")
        if self.config.config_id not in self.run.configuration_ids:
            raise ValueError("motif generator config is absent from the run")
        object.__setattr__(
            self, "candidate_id", str(self.candidate.candidate_id)
        )
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        if mode is InformationMode.EX_ANTE_SIMULATION and self.as_of_ns is None:
            raise ValueError("ex-ante benchmark generation requires as_of_ns")
        if (
            mode is InformationMode.EX_POST_RECONSTRUCTION
            and self.as_of_ns is not None
        ):
            raise ValueError("ex-post benchmark generation rejects as_of_ns")

    def generate(
        self,
        degraded_events: Sequence[BenchmarkEventV1],
        *,
        scenario: BenchmarkScenarioV1,
        window: ReconstructionWindowV1,
        ensemble_member_id: str,
    ) -> Sequence[BenchmarkEventV1]:
        """Generate a benchmark stream containing anchors and proposals."""
        if window.run_id != self.run.run_id:
            raise ValueError(
                "benchmark window differs from motif generator run"
            )
        if window.ensemble_member_id != ensemble_member_id:
            raise ValueError("benchmark ensemble member differs from window")
        if scenario.epoch_id != self.condition.feed_epoch_id:
            raise ValueError("benchmark epoch differs from motif condition")
        ordered = tuple(
            sorted(
                degraded_events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.benchmark_event_id,
                ),
            )
        )
        if len(ordered) < 2:
            return tuple(
                _benchmark_anchor(item, ensemble_member_id) for item in ordered
            )
        source_version_id = self.run.source_version_ids[0]
        anchors = tuple(
            SyntheticEventV1.observed(
                symbol=item.symbol,
                event_time_ns=item.event_time_ns,
                event_sequence=item.event_sequence,
                bid=item.bid,
                ask=item.ask,
                run_id=self.run.run_id,
                ensemble_member_id=ensemble_member_id,
                source_version_id=source_version_id,
                source_series_id=(
                    f"benchmark:{scenario.scenario_id}:{item.symbol}"
                ),
                source_period=scenario.severity_id,
                source_row_id=index,
            )
            for index, item in enumerate(ordered, start=1)
        )
        proposals: list[SyntheticEventV1] = []
        for left, right in zip(anchors, anchors[1:]):
            query = ReferenceMotifQueryV1(
                condition=self.condition,
                information_mode=self.information_mode,
                used_at_ns=right.event_time_ns,
                as_of_ns=self.as_of_ns,
                max_results=self.motif_index.config.max_matches,
            )
            result = query_reference_motifs(self.motif_index, query)
            batch = generate_empirical_motif_candidates(
                run=self.run,
                window=window,
                left_anchor=left,
                right_anchor=right,
                query_result=result,
                config=self.config,
            )
            proposals.extend(batch.events)
        benchmark_anchors = [
            _benchmark_anchor(item, ensemble_member_id) for item in ordered
        ]
        benchmark_proposals = [
            BenchmarkEventV1.from_synthetic_event(
                item,
                epoch_id=scenario.epoch_id,
                session=self.condition.session_state,
                event_state=self.condition.activity_regime,
                sparsity="empirical-motif-candidate",
            )
            for item in proposals
        ]
        return tuple(
            sorted(
                (*benchmark_anchors, *benchmark_proposals),
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.benchmark_event_id,
                ),
            )
        )


def _validate_generation_scope(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    query_result: ReferenceMotifQueryResultV1,
    config: EmpiricalMotifGeneratorConfigV1,
) -> None:
    if window.run_id != run.run_id:
        raise ValueError("generation window does not belong to the run")
    if window.ensemble_member_id not in run.ensemble_member_ids:
        raise ValueError("generation member does not belong to the run")
    if config.config_id not in run.configuration_ids:
        raise ValueError("motif generator config is absent from the run")
    for anchor in (left_anchor, right_anchor):
        if anchor.origin is not SyntheticEventOrigin.OBSERVED:
            raise ValueError("motif generation anchors must be observed")
        if anchor.run_id != run.run_id:
            raise ValueError("motif generation anchor run differs")
        if anchor.ensemble_member_id != window.ensemble_member_id:
            raise ValueError("motif generation anchor member differs")
        if (
            anchor.symbol not in window.symbols
            or anchor.symbol not in run.symbols
        ):
            raise ValueError("motif generation anchor symbol is outside scope")
        if anchor.source_version_id not in run.source_version_ids:
            raise ValueError("motif generation anchor source differs")
        if not window.reads_event_time(anchor.event_time_ns):
            raise ValueError("generation window does not read both anchors")
    if left_anchor.symbol != right_anchor.symbol:
        raise ValueError("motif generation anchors have different symbols")
    if left_anchor.source_version_id != right_anchor.source_version_id:
        raise ValueError("motif generation anchors have different sources")
    if (
        query_result.query.condition.symbol.upper()
        != left_anchor.symbol.upper()
    ):
        raise ValueError("motif query condition symbol differs from anchors")
    if query_result.query.used_at_ns != right_anchor.event_time_ns:
        raise ValueError(
            "motif query use time must equal the right anchor boundary"
        )


def _target_cardinality(
    condition: ReferenceMotifConditionV1,
    gap_ns: int,
) -> tuple[int, int]:
    metrics = condition.metrics
    intensity = metrics.get("tick_intensity")
    if intensity is not None:
        if intensity <= 0.0:
            return 0, gap_ns
        cadence = max(1, round(NANOSECONDS_PER_SECOND / intensity))
    else:
        interarrival = metrics.get("interarrival_ns")
        if interarrival is None or interarrival <= 0.0:
            return 0, gap_ns
        cadence = max(1, round(interarrival))
    return max(0, (gap_ns - 1) // cadence), cadence


def _candidate_times(
    left_time_ns: int,
    right_time_ns: int,
    count: int,
    cadence_ns: int,
    condition: ReferenceMotifConditionV1,
) -> tuple[int, ...]:
    gap_ns = right_time_ns - left_time_ns
    precision = max(
        1,
        round(condition.metrics.get("timestamp_precision_ns", 1.0)),
    )
    values: list[int] = []
    for ordinal in range(1, count + 1):
        requested_offset = ordinal * cadence_ns
        quantized = (
            (2 * requested_offset + precision) // (2 * precision)
        ) * precision
        quantized = max(1, min(gap_ns - 1, quantized))
        values.append(left_time_ns + quantized)
    return tuple(values)


def _plan_transforms(
    *,
    run: ReconstructionRunV1,
    ensemble_member_id: str,
    interval_id: str,
    event_times: tuple[int, ...],
    cadence_ns: int,
    left_time_ns: int,
    query_result: ReferenceMotifQueryResultV1,
    config: EmpiricalMotifGeneratorConfigV1,
) -> tuple[tuple[_PlannedTransform, ...], str | None]:
    matches = query_result.matches
    plans: list[_PlannedTransform] = []
    cursor = 0
    while cursor < len(event_times):
        segment_ordinal = len(plans) + 1
        if segment_ordinal > config.max_transformations_per_interval:
            return (), "transformation count exceeds configured interval limit"
        seed = run.seed_for(
            ensemble_member_id,
            f"{EMPIRICAL_MOTIF_GENERATOR_ID}:{config.config_id}:"
            f"{interval_id}:segment:{segment_ordinal}",
        )
        start_match = seed % len(matches)
        chosen: tuple[ReferenceMotifMatchV1, int, float] | None = None
        previous_time = left_time_ns if cursor == 0 else event_times[cursor - 1]
        for match_offset in range(len(matches)):
            match = matches[(start_match + match_offset) % len(matches)]
            # A compact empirical path may be interpolated to more or fewer
            # target events.  Its declared time-scale envelope, rather than
            # its source row count, is the admissibility boundary.
            capacity = len(event_times) - cursor
            for event_count in range(capacity, 0, -1):
                observed_duration = (
                    event_times[cursor + event_count - 1] - previous_time
                )
                duration = max(observed_duration, cadence_ns * event_count)
                time_scale = duration / match.fragment.duration_ns
                policy = match.fragment.transform_policy
                if policy.min_time_scale <= time_scale <= policy.max_time_scale:
                    chosen = (match, event_count, time_scale)
                    break
            if chosen is not None:
                break
        if chosen is None:
            return (
                (),
                "no retrieved fragment supports the required cadence/time scale "
                f"at output ordinal {cursor + 1}",
            )
        match, event_count, time_scale = chosen
        requested_price_scale, applied_price_scale = _price_scale(
            query_result.query.condition, match.fragment
        )
        confidence = round(
            1.0 / (1.0 + match.distance),
            config.confidence_rounding_digits,
        )
        fragment = match.fragment
        record = EmpiricalMotifTransformationV1(
            index_id=query_result.index_id,
            query_id=query_result.query.query_id,
            query_result_id=query_result.result_id,
            source_fragment_id=fragment.fragment_id,
            source_window_id=fragment.source_window_id,
            source_series_id=fragment.source_series_id,
            source_period=fragment.period,
            source_artifact_sha256=fragment.source_artifact.sha256,
            backoff_level=match.backoff_level,
            cell_support=match.cell_support,
            match_distance=match.distance,
            segment_ordinal=segment_ordinal,
            output_start_ordinal=cursor + 1,
            output_end_ordinal=cursor + event_count,
            source_event_count=len(fragment.event_offsets_ns),
            time_scale=time_scale,
            time_warp_ratio=1.0,
            requested_price_scale=requested_price_scale,
            applied_price_scale=applied_price_scale,
            price_scale_clamped=(requested_price_scale != applied_price_scale),
            spread_shape_applied=fragment.transform_policy.allow_spread_scaling,
            seed=seed,
            confidence=confidence,
        )
        plans.append(
            _PlannedTransform(
                match=match,
                record=record,
                start_index=cursor,
                event_count=event_count,
            )
        )
        cursor += event_count
    return tuple(plans), None


def _price_scale(
    target: ReferenceMotifConditionV1,
    fragment: ReferenceMotifFragmentV1,
) -> tuple[float, float]:
    target_volatility = target.metrics.get("volatility", 0.0)
    source_volatility = fragment.condition.metrics.get("volatility", 0.0)
    requested = (
        target_volatility / source_volatility
        if target_volatility > 0.0 and source_volatility > 0.0
        else 1.0
    )
    policy = fragment.transform_policy
    return requested, min(
        policy.max_price_scale,
        max(policy.min_price_scale, requested),
    )


def _events_for_transform(
    *,
    run: ReconstructionRunV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    interval_id: str,
    query_result: ReferenceMotifQueryResultV1,
    config: EmpiricalMotifGeneratorConfigV1,
    event_times: tuple[int, ...],
    plan: _PlannedTransform,
) -> tuple[
    tuple[SyntheticEventV1, ...],
    tuple[EmpiricalMotifEventLineageV1, ...],
    str | None,
]:
    fragment = plan.match.fragment
    left_mid = (left_anchor.bid + left_anchor.ask) / 2.0
    right_mid = (right_anchor.bid + right_anchor.ask) / 2.0
    left_spread = left_anchor.ask - left_anchor.bid
    right_spread = right_anchor.ask - right_anchor.bid
    gap_ns = right_anchor.event_time_ns - left_anchor.event_time_ns
    precision_digits = _price_precision_digits(
        query_result.query.condition, config
    )
    midpoint_deltas = tuple(
        (bid_delta + ask_delta) / 2.0
        for bid_delta, ask_delta in zip(
            fragment.bid_deltas, fragment.ask_deltas
        )
    )
    spread_deltas = tuple(
        ask_delta - bid_delta
        for bid_delta, ask_delta in zip(
            fragment.bid_deltas, fragment.ask_deltas
        )
    )
    events: list[SyntheticEventV1] = []
    lineages: list[EmpiricalMotifEventLineageV1] = []
    for local_index in range(plan.event_count):
        global_index = plan.start_index + local_index
        global_ordinal = global_index + 1
        event_time_ns = event_times[global_index]
        anchor_progress = (event_time_ns - left_anchor.event_time_ns) / gap_ns
        source_progress = (local_index + 1) / plan.event_count
        source_mid = _interpolate_fragment(
            fragment, midpoint_deltas, source_progress
        )
        source_mid_residual = source_mid - (
            source_progress * midpoint_deltas[-1]
        )
        mid = (
            left_mid
            + anchor_progress * (right_mid - left_mid)
            + plan.record.applied_price_scale * source_mid_residual
        )
        spread = left_spread + anchor_progress * (right_spread - left_spread)
        if plan.record.spread_shape_applied:
            source_spread = _interpolate_fragment(
                fragment, spread_deltas, source_progress
            )
            spread += plan.record.applied_price_scale * (
                source_spread - source_progress * spread_deltas[-1]
            )
        bid = round(mid - spread / 2.0, precision_digits)
        ask = round(mid + spread / 2.0, precision_digits)
        if not (
            math.isfinite(bid)
            and math.isfinite(ask)
            and bid > 0.0
            and ask > 0.0
            and ask >= bid
        ):
            return (
                (),
                (),
                (
                    "transformed quote violates positive bid/ask or spread domain "
                    f"at output ordinal {global_ordinal}"
                ),
            )
        event = SyntheticEventV1.generated(
            symbol=left_anchor.symbol,
            event_time_ns=event_time_ns,
            event_sequence=global_ordinal,
            bid=bid,
            ask=ask,
            run_id=run.run_id,
            ensemble_member_id=left_anchor.ensemble_member_id,
            source_version_id=left_anchor.source_version_id,
            anchor_interval_id=interval_id,
            left_anchor_event_id=left_anchor.event_id,
            right_anchor_event_id=right_anchor.event_id,
            generator_id=EMPIRICAL_MOTIF_GENERATOR_ID,
            generator_version=EMPIRICAL_MOTIF_GENERATOR_VERSION,
            generator_config_id=config.config_id,
            reference_id=query_result.result_id,
            motif_id=fragment.fragment_id,
            feed_epoch_id=query_result.query.condition.feed_epoch_id,
            constraint_set_id=config.constraint_set_id,
            # Motif similarity is retrieval evidence, not calibrated pointwise
            # probability.  It remains on the transformation while generated
            # events reserve confidence for explicitly calibrated quantities.
            confidence=None,
        )
        events.append(event)
        lineages.append(
            EmpiricalMotifEventLineageV1(
                event_id=event.event_id,
                transformation_id=plan.record.transformation_id,
                global_event_ordinal=global_ordinal,
                segment_event_ordinal=local_index + 1,
                source_progress=source_progress,
                anchor_progress=anchor_progress,
                requested_event_time_ns=event_time_ns,
            )
        )
    return tuple(events), tuple(lineages), None


def _interpolate_fragment(
    fragment: ReferenceMotifFragmentV1,
    values: tuple[float, ...],
    progress: float,
) -> float:
    target_offset = progress * fragment.duration_ns
    offsets = fragment.event_offsets_ns
    for index in range(1, len(offsets)):
        right_offset = offsets[index]
        if target_offset > right_offset:
            continue
        left_offset = offsets[index - 1]
        if right_offset == left_offset:
            return values[index]
        local = (target_offset - left_offset) / (right_offset - left_offset)
        return float(
            values[index - 1] + local * (values[index] - values[index - 1])
        )
    return values[-1]


def _price_precision_digits(
    condition: ReferenceMotifConditionV1,
    config: EmpiricalMotifGeneratorConfigV1,
) -> int:
    value = condition.metrics.get("price_precision_digits")
    if value is None:
        return config.fallback_price_precision_digits
    rounded = int(round(_finite_float(value, "price_precision_digits")))
    return min(MAX_PRICE_PRECISION_DIGITS, max(0, rounded))


def _resource_estimate(
    candidate_count: int,
    config: EmpiricalMotifGeneratorConfigV1,
) -> ReconstructionResourceEstimateV1:
    estimated_bytes = candidate_count * config.estimated_bytes_per_event
    return ReconstructionResourceEstimateV1(
        input_event_count=2,
        candidate_event_count=candidate_count,
        retained_ensemble_members=1,
        inflight_batches=1 if candidate_count else 0,
        peak_events_per_batch=candidate_count,
        estimated_memory_bytes=estimated_bytes,
        estimated_scratch_bytes=0,
        estimated_output_bytes=estimated_bytes,
        estimated_batch_count=1 if candidate_count else 0,
    )


def _decision_batch(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    interval_id: str,
    query_result: ReferenceMotifQueryResultV1,
    config: EmpiricalMotifGeneratorConfigV1,
    status: MotifGenerationStatus,
    decision: MotifGenerationDecision,
    target_event_count: int,
    estimate: ReconstructionResourceEstimateV1,
    details: Sequence[str] = (),
) -> EmpiricalMotifCandidateBatchV1:
    return EmpiricalMotifCandidateBatchV1(
        run_id=run.run_id,
        window_id=window.window_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol=left_anchor.symbol,
        anchor_interval_id=interval_id,
        left_anchor_event_id=left_anchor.event_id,
        right_anchor_event_id=right_anchor.event_id,
        generator_config=config,
        query_result=query_result,
        status=status,
        decision=decision,
        target_event_count=target_event_count,
        events=(),
        transformations=(),
        event_lineage=(),
        resource_estimate=estimate,
        carry_state=_carry_state(run, window, left_anchor, right_anchor, ()),
        decision_details=tuple(details),
    )


def _carry_state(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    events: Sequence[SyntheticEventV1],
) -> CarryStateV1:
    last_event = events[-1] if events else left_anchor
    if window.owns_event_time(right_anchor.event_time_ns):
        last_event = right_anchor
    watermark = min(
        window.core_end_ns - 1,
        max(window.core_start_ns, right_anchor.event_time_ns - 1),
    )
    return CarryStateV1(
        run_id=run.run_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol_watermarks_ns={left_anchor.symbol: watermark},
        last_event_ids={left_anchor.symbol: last_event.event_id},
    )


def _benchmark_anchor(
    event: BenchmarkEventV1,
    ensemble_member_id: str,
) -> BenchmarkEventV1:
    return BenchmarkEventV1(
        source_event_id=event.source_event_id,
        symbol=event.symbol,
        event_time_ns=event.event_time_ns,
        event_sequence=event.event_sequence,
        bid=event.bid,
        ask=event.ask,
        epoch_id=event.epoch_id,
        session=event.session,
        event_state=event.event_state,
        sparsity=event.sparsity,
        ensemble_member_id=ensemble_member_id,
        anchor_id=event.anchor_id or event.source_event_id,
        support_lower_mid=event.support_lower_mid,
        support_upper_mid=event.support_upper_mid,
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("value must be non-empty text")
    return value.strip()


def _bounded_text(value: Any, name: str, maximum: int) -> str:
    result = _required_text(value)
    if len(result) > maximum:
        raise ValueError(f"{name} exceeds bounded text length")
    return result


def _optional_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return _required_text(value)


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 1:
        raise ValueError(f"{name} must be positive")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be boolean")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")
    return tuple(_required_text(item) for item in value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise ValueError("contract JSON must contain an object")
    return value


def _content_sha256(value: Any) -> str:
    encoded = canonical_contract_json(value).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

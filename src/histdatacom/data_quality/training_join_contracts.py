"""Additive source-replayed wide-join contracts (SemVer 1.0.0).

Constructing an envelope verifies structure, never source authenticity. Only the
closed materializer/reader replays evidence. Normalized as-of applies to added
columns; it does not make the retained ex-post training spine causal.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from .training_contracts import (
    TrainingBatchV1,
    TrainingContract,
    _clock,
    _text,
    training_json,
    training_load,
)

JOIN_VERSION = "1.0.0"
MAX_JOIN_COLUMNS = 128
MAX_JOIN_ROWS = 256
MAX_JOIN_CELLS = 4096
JOIN_NONCLAIMS = (
    "source_value_replay_is_not_historical_availability_authenticity",
    "normalized_asof_qualifies_only_appended_columns_not_legacy_spine",
    "not_empirical_confidence_independence_or_holdout_qualification",
)
JOIN_NAMESPACES = (
    "market.tick.",
    "market.bar.",
    "market.indicator.",
    "triangle.",
    "calendar.",
    "forecast.",
    "positioning.",
    "activity.",
    "synthetic_flow.",
    "broker_style.",
    "uncertainty.",
    "lineage.",
)


class JoinInformationMode(str, Enum):
    NORMALIZED_AS_OF = "normalized_asof_added_columns_research"
    EX_POST = "explicit_expost_added_columns_research"


class JoinFamily(str, Enum):
    TICK = "verified_quote_events"
    BAR = "verified_closed_bar_features"
    INDICATOR = "verified_closed_bar_indicators"
    TRIANGLE = "verified_triangle_features"
    CALENDAR = "normalized_calendar_releases"
    VINTAGE = "recomputed_vintage_macro_features"
    FORECAST = "replayed_feature_forecast"
    POSITIONING = "replayed_cftc_positioning"
    ACTIVITY = "recomputed_committed_quote_activity"
    BROKER = "refitted_native_broker_fingerprint"
    UNSUPPORTED = "reserved_unimplemented_source_family"


class JoinDirection(str, Enum):
    EXACT = "exact"
    PRIOR = "prior"
    BOUNDED_PRIOR = "bounded_prior"
    INTERVAL = "half_open_interval_membership"


class JoinMeaning(str, Enum):
    STATE = "persistent_state"
    EVENT = "event_occurrence_no_fill"
    CLOSED_BAR = "last_fully_closed_bar_no_fill"
    SNAPSHOT = "cutoff_specific_snapshot_no_fill"


class JoinState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"
    NOT_APPLICABLE = "not_applicable"
    CONFIRMED_ABSENCE = "confirmed_absence"
    EMPTY_MARKET_INTERVAL = "empty_market_interval"
    INSUFFICIENT_WARMUP = "insufficient_warmup"
    SOURCE_OUTAGE = "source_outage"
    EXPECTED_CLOSURE = "expected_closure"
    STALE = "stale"
    UNKNOWN_AVAILABILITY = "unknown_availability"


def _unique(values: tuple[str, ...]) -> None:
    if len(set(values)) != len(values):
        raise ValueError("join inventory contains duplicate identities")
    for value in values:
        _text(value)


def _namespace(value: str) -> None:
    _text(value)
    if not value.startswith(JOIN_NAMESPACES) or not re.fullmatch(
        r"[A-Za-z0-9_.-]+", value
    ):
        raise ValueError("unknown or malformed training feature namespace")


@dataclass(frozen=True, slots=True)
class TrainingJoinEntityV1(TrainingContract):
    KIND: ClassVar[str] = "join-entity"
    symbol: str
    currency: str | None = None
    economy: str | None = None
    series: str | None = None

    def _validate(self) -> None:
        if re.fullmatch(r"[A-Z]{6}", self.symbol) is None:
            raise ValueError("join entity requires a graph FX symbol")
        if self.currency is not None and (
            re.fullmatch(r"[A-Z]{3}", self.currency) is None
            or self.currency not in (self.symbol[:3], self.symbol[3:])
        ):
            raise ValueError("currency does not belong to the declared pair")
        for value in (self.economy, self.series):
            if value is not None:
                _text(value)


@dataclass(frozen=True, slots=True)
class TrainingJoinSourceV1(TrainingContract):
    """Closed-adapter input, not a verification receipt.

    Paths have family-specific roles. Embedded strings must decode through the
    native family reader. No adapter accepts arbitrary values or claimed hashes.
    """

    KIND: ClassVar[str] = "join-source"
    key: str
    family: JoinFamily
    paths: tuple[str, ...] = ()
    evidence_json: tuple[str, ...] = ()
    configuration_json: str = "{}"

    def _validate(self) -> None:
        _text(self.key)
        if len(self.paths) > 32 or len(self.evidence_json) > 32:
            raise ValueError("join source input cardinality exceeds bound")
        _unique(self.paths)
        if sum(len(v) for v in self.evidence_json) > 2 * 1024 * 1024:
            raise ValueError("join embedded source evidence exceeds 2 MiB")
        for value in (*self.evidence_json, self.configuration_json):
            if training_json(training_load(value)) != value:
                raise ValueError("join source input must be canonical JSON")
        if len(self.configuration_json) > 64 * 1024:
            raise ValueError("join adapter configuration exceeds bound")


@dataclass(frozen=True, slots=True)
class TrainingJoinColumnV1(TrainingContract):
    KIND: ClassVar[str] = "join-column"
    name: str
    source_key: str
    entity: TrainingJoinEntityV1
    field: str
    direction: JoinDirection
    meaning: JoinMeaning
    max_age_ns: int
    coordinate: str = ""
    parent_namespaces: tuple[str, ...] = ()
    definition_version: str = JOIN_VERSION

    def _validate(self) -> None:
        _namespace(self.name)
        for value in (self.source_key, self.field):
            _text(value)
        if self.coordinate:
            _text(self.coordinate)
        _clock(self.max_age_ns)
        if self.definition_version != JOIN_VERSION:
            raise ValueError("unsupported join column definition version")
        if self.meaning in (JoinMeaning.EVENT, JoinMeaning.SNAPSHOT) and (
            self.direction is not JoinDirection.EXACT or self.max_age_ns
        ):
            raise ValueError("event/snapshot columns cannot be forward-filled")
        if self.direction is JoinDirection.EXACT and self.max_age_ns:
            raise ValueError("exact joins have zero source age allowance")
        if (
            self.direction is JoinDirection.BOUNDED_PRIOR
            and not self.max_age_ns
        ):
            raise ValueError("bounded prior requires a positive frozen age")
        if self.meaning is JoinMeaning.CLOSED_BAR and (
            self.direction is not JoinDirection.EXACT or self.max_age_ns
        ):
            raise ValueError("closed-bar selection is frozen by its snapshot")
        _unique(self.parent_namespaces)
        if (
            len(self.parent_namespaces) > 16
            or self.name in self.parent_namespaces
        ):
            raise ValueError("join parent namespaces are cyclic or unbounded")
        for value in self.parent_namespaces:
            _namespace(value)


def training_join_parent_namespaces(
    family: JoinFamily, column: TrainingJoinColumnV1
) -> tuple[str, ...]:
    """Frozen native dependency planes, accompanied by exact per-cell IDs.

    These describe evidence roles, not caller asserted derivation edges.
    """
    quote = f"market.tick.{column.entity.symbol}.quote"
    if family is JoinFamily.TICK:
        return (f"lineage.quote_record.{column.entity.symbol}",)
    if family in (JoinFamily.BAR, JoinFamily.INDICATOR):
        return (quote, f"lineage.closed_bar_support.{column.entity.symbol}")
    if family is JoinFamily.TRIANGLE:
        return (
            tuple(
                f"market.tick.{symbol}.quote"
                for symbol in ("EURGBP", "EURUSD", "GBPUSD")
            )
            + ("lineage.triangle_closed_bar_support",)
            + (
                ("lineage.triangle_projection_delivery",)
                if column.field.startswith("projection.")
                else ()
            )
        )
    if family is JoinFamily.ACTIVITY:
        return (quote, "lineage.committed_activity_product")
    if family is JoinFamily.CALENDAR:
        return ("lineage.calendar_release_or_schedule",)
    if family is JoinFamily.VINTAGE:
        return (
            "lineage.vintage_observations",
            "lineage.vintage_metadata_schedule_transform",
        )
    if family is JoinFamily.FORECAST:
        return (
            "lineage.forecast_training_inputs",
            "lineage.forecast_inference_inputs",
            "lineage.forecast_model_execution",
        )
    if family is JoinFamily.POSITIONING:
        return (
            "lineage.cftc_raw_release_and_vintage",
            "lineage.cftc_symbol_mapping",
        )
    if family is JoinFamily.BROKER:
        return (
            "lineage.broker_capture_sessions",
            "lineage.broker_full_support_fit",
        )
    return ()


@dataclass(frozen=True, slots=True)
class TrainingJoinPlanV1(TrainingContract):
    KIND: ClassVar[str] = "join-plan"
    spine: TrainingBatchV1
    information_mode: JoinInformationMode
    sources: tuple[TrainingJoinSourceV1, ...]
    columns: tuple[TrainingJoinColumnV1, ...]
    contract_version: str = JOIN_VERSION

    def _validate(self) -> None:
        if self.contract_version != JOIN_VERSION:
            raise ValueError("unsupported join contract version")
        if (
            len(self.sources) > 32
            or len(self.columns) > MAX_JOIN_COLUMNS
            or len(self.spine.rows) > MAX_JOIN_ROWS
            or len(self.spine.rows) * len(self.columns) > MAX_JOIN_CELLS
        ):
            raise ValueError("join projected row/column/cell budget exceeded")
        _unique(tuple(s.key for s in self.sources))
        _unique(tuple(c.name for c in self.columns))
        sources = {s.key: s for s in self.sources}
        graph = set(self.spine.ownership.units[0].graph_symbols)
        parents = {c.name: c.parent_namespaces for c in self.columns}
        for column in self.columns:
            if (
                column.source_key not in sources
                or column.entity.symbol not in graph
            ):
                raise ValueError(
                    "join column escapes source or graph inventory"
                )
            if column.parent_namespaces != training_join_parent_namespaces(
                sources[column.source_key].family, column
            ):
                raise ValueError(
                    "join parents differ from frozen native dependency namespaces"
                )
            seen: set[str] = set()
            pending = list(column.parent_namespaces)
            while pending:
                name = pending.pop()
                if name == column.name:
                    raise ValueError(
                        "join namespace dependencies contain a cycle"
                    )
                if name not in seen:
                    seen.add(name)
                    pending.extend(parents.get(name, ()))


@dataclass(frozen=True, slots=True)
class TrainingJoinRefusalEvidenceV1(TrainingContract):
    """Value-free native evidence explaining a refused candidate."""

    KIND: ClassVar[str] = "join-refusal-evidence"
    source_ids: tuple[str, ...]
    source_schema: str
    origin: str
    selection_time_ns: int
    source_time_ns: int
    available_at_ns: int | None
    dependency_start_ns: int
    dependency_end_ns: int
    parent_source_ids: tuple[str, ...]

    def _validate(self) -> None:
        _unique(self.source_ids)
        _unique(self.parent_source_ids)
        _text(self.source_schema)
        _text(self.origin)
        for clock in (
            self.selection_time_ns,
            self.source_time_ns,
            self.available_at_ns,
            self.dependency_start_ns,
            self.dependency_end_ns,
        ):
            if clock is not None:
                _clock(clock)
        if (
            not self.source_ids
            or self.dependency_end_ns <= self.dependency_start_ns
        ):
            raise ValueError(
                "refusal evidence requires exact native source support"
            )


@dataclass(frozen=True, slots=True)
class TrainingJoinValueV1(TrainingContract):
    KIND: ClassVar[str] = "join-value"
    column: str
    state: JoinState
    value_json: str
    reason: str
    source_binding_id: str
    source_ids: tuple[str, ...]
    source_schema: str
    origin: str
    source_time_ns: int | None
    available_at_ns: int | None
    dependency_start_ns: int | None
    dependency_end_ns: int | None
    parent_source_ids: tuple[str, ...]
    refusal_evidence: tuple[TrainingJoinRefusalEvidenceV1, ...] = ()
    historical_availability_verified: bool = False

    def _validate(self) -> None:
        _namespace(self.column)
        for value in (
            self.reason,
            self.source_binding_id,
            self.source_schema,
            self.origin,
        ):
            _text(value)
        payload = training_load(self.value_json)
        if (
            set(payload) != {"value"}
            or training_json(payload) != self.value_json
        ):
            raise ValueError("join value requires one canonical value member")
        if (payload["value"] is not None) != (
            self.state is JoinState.AVAILABLE
        ):
            raise ValueError("join value/null state mismatch")
        if isinstance(payload["value"], (dict, list)):
            raise ValueError("wide join columns require scalar values")
        _unique(self.source_ids)
        _unique(self.parent_source_ids)
        for clock in (
            self.source_time_ns,
            self.available_at_ns,
            self.dependency_start_ns,
            self.dependency_end_ns,
        ):
            if clock is not None:
                _clock(clock)
        if (self.dependency_start_ns is None) != (
            self.dependency_end_ns is None
        ):
            raise ValueError("join dependency span must be complete")
        if self.dependency_start_ns is not None and (
            self.dependency_end_ns is None
            or self.dependency_end_ns <= self.dependency_start_ns
        ):
            raise ValueError("join dependency interval is empty")
        if self.state is JoinState.AVAILABLE and (
            not self.source_ids
            or self.source_time_ns is None
            or self.dependency_start_ns is None
        ):
            raise ValueError("available join lacks exact source support")
        if self.historical_availability_verified:
            raise ValueError(
                "normalized join replay cannot attest historical clocks"
            )
        if len(self.refusal_evidence) > 32 or (
            self.state is JoinState.AVAILABLE and self.refusal_evidence
        ):
            raise ValueError(
                "refusal evidence is bounded and only accompanies nulls"
            )


@dataclass(frozen=True, slots=True)
class TrainingJoinRowV1(TrainingContract):
    KIND: ClassVar[str] = "join-row"
    spine_row_id: str
    evidence_unit_id: str
    decision_time_ns: int
    values: tuple[TrainingJoinValueV1, ...]

    def _validate(self) -> None:
        _text(self.spine_row_id)
        _text(self.evidence_unit_id)
        _clock(self.decision_time_ns)
        _unique(tuple(v.column for v in self.values))


@dataclass(frozen=True, slots=True)
class TrainingJoinBatchV1(TrainingContract):
    KIND: ClassVar[str] = "join-batch"
    plan: TrainingJoinPlanV1
    rows: tuple[TrainingJoinRowV1, ...]
    nonclaims: tuple[str, ...] = JOIN_NONCLAIMS

    def _validate(self) -> None:
        if self.nonclaims != JOIN_NONCLAIMS or len(self.rows) != len(
            self.plan.spine.rows
        ):
            raise ValueError(
                "joined batch must retain nonclaims and exact row grain"
            )
        names = tuple(c.name for c in self.plan.columns)
        bindings = {c.name: c.source_key for c in self.plan.columns}
        source_ids = {s.key: s.artifact_id for s in self.plan.sources}
        for spine, row in zip(self.plan.spine.rows, self.rows):
            if (
                row.spine_row_id,
                row.evidence_unit_id,
                row.decision_time_ns,
            ) != (
                spine.artifact_id,
                spine.evidence_unit_id,
                spine.decision_time_ns,
            ) or tuple(
                v.column for v in row.values
            ) != names:
                raise ValueError(
                    "joined row differs from exact spine/projection"
                )
            for value in row.values:
                if (
                    value.source_binding_id
                    != source_ids[bindings[value.column]]
                ):
                    raise ValueError("joined cell binds a different source")
                if (
                    self.plan.information_mode
                    is JoinInformationMode.NORMALIZED_AS_OF
                    and value.state is JoinState.AVAILABLE
                ):
                    if (
                        value.available_at_ns is None
                        or value.available_at_ns > row.decision_time_ns
                        or value.source_time_ns is None
                        or value.source_time_ns > row.decision_time_ns
                        or value.dependency_end_ns is None
                        or value.dependency_end_ns > row.decision_time_ns + 1
                    ):
                        raise ValueError(
                            "normalized join cell uses future/unknown support"
                        )

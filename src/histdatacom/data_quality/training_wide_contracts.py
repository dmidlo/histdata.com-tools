"""Additive bounded wide-layout contracts; layout metadata grants no authority.

Native join plans and batches remain complete, unchanged v1 replay subjects.
The new layer declares logical geometry and groups, never a new market event.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from .training_contracts import (
    MAX_TRAINING_BYTES,
    NONCLAIMS,
    TrainingContract,
    _clock,
    _digest,
    _text,
    training_json,
)
from .training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    JoinState,
    TrainingJoinColumnV1,
    TrainingJoinPlanV1,
)

WIDE_VERSION = "1.0.0"
MAX_WIDE_COLUMNS = 4096
MAX_WIDE_SPINES = 16
MAX_WIDE_TILES = 128
MAX_WIDE_ROWS = 512
MAX_WIDE_CELLS = 32768
MAX_WIDE_CHILD_BYTES = 32 * 1024 * 1024
TIMEFRAME_NS = {
    "1m": 60_000_000_000,
    "5m": 300_000_000_000,
    "15m": 900_000_000_000,
    "30m": 1_800_000_000_000,
    "1h": 3_600_000_000_000,
    "4h": 14_400_000_000_000,
    "1d": 86_400_000_000_000,
}
WIDE_NONCLAIMS = NONCLAIMS + (
    "not_complete_605_612_681_corpus_certification",
    "logical_width_is_not_independent_information",
    "projection_does_not_waive_complete_source_or_current_rights_replay",
    "layout_is_not_a_native_policy_receipt",
)


class WideRowGrain(str, Enum):
    EVENT = "event_clock"
    GRID = "fixed_decision_grid"
    BAR = "anchor_bar_clock"
    PANEL = "member_scenario_panel"


class WideValueType(str, Enum):
    INTEGER = "int64"
    NUMBER = "finite_number"
    BOOLEAN = "boolean"
    STRING = "string"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class TrainingWideGrainV1(TrainingContract):
    KIND: ClassVar[str] = "wide-grain"
    kind: WideRowGrain
    grid_start_ns: int | None = None
    grid_end_ns: int | None = None
    grid_step_ns: int | None = None
    max_anchor_age_ns: int | None = None
    anchor_timeframe: str | None = None

    def _validate(self) -> None:
        clocks = (
            self.grid_start_ns,
            self.grid_end_ns,
            self.grid_step_ns,
            self.max_anchor_age_ns,
        )
        if self.kind in (WideRowGrain.EVENT, WideRowGrain.PANEL):
            if (
                any(v is not None for v in clocks)
                or self.anchor_timeframe is not None
            ):
                raise ValueError("event/panel grain cannot invent grid clocks")
            return
        if any(v is None for v in clocks):
            raise ValueError("grid/bar grain requires its complete geometry")
        for value in clocks:
            if value is not None:
                _clock(value)
        start, end, step, age = clocks
        assert start is not None and end is not None
        assert step is not None and age is not None
        if (
            not step
            or start >= end
            or start % step
            or end % step
            or (end - start) // step > MAX_WIDE_SPINES
        ):
            raise ValueError("grid geometry is unaligned, empty or unbounded")
        if self.kind is WideRowGrain.BAR:
            if TIMEFRAME_NS.get(self.anchor_timeframe or "") != step:
                raise ValueError(
                    "bar-clock grain requires a standard UTC width"
                )
        elif self.anchor_timeframe is not None:
            raise ValueError("decision grid is not a declared closed-bar clock")

    @property
    def cutoffs(self) -> tuple[int, ...]:
        if self.grid_start_ns is None:
            return ()
        assert self.grid_end_ns is not None and self.grid_step_ns is not None
        return tuple(
            range(self.grid_start_ns, self.grid_end_ns, self.grid_step_ns)
        )


@dataclass(frozen=True, slots=True)
class TrainingWideColumnV1(TrainingContract):
    KIND: ClassVar[str] = "wide-column"
    column: TrainingJoinColumnV1
    family: JoinFamily
    dtype: WideValueType
    units: str | None
    timeframe: str | None
    lag_steps: int
    warmup_steps: int
    step_unit: str
    availability_rule: str
    origin_mode: JoinInformationMode
    native_semantics_json: str
    missingness_states: tuple[str, ...] = tuple(v.value for v in JoinState)
    semantic_version: str = WIDE_VERSION

    def _validate(self) -> None:
        from .training_contracts import training_load

        if self.semantic_version != WIDE_VERSION:
            raise ValueError("unsupported wide column semantics")
        if self.units is not None:
            _text(self.units)
        if self.timeframe is not None and self.timeframe not in TIMEFRAME_NS:
            raise ValueError("wide column has an unsupported timeframe")
        if (
            not 0 <= self.lag_steps <= 10000
            or not 0 <= self.warmup_steps <= 10001
        ):
            raise ValueError("wide column lag/warmup is unbounded")
        if self.step_unit not in (
            "closed_bars",
            "reference_periods",
            "not_applicable",
        ):
            raise ValueError("wide column lag/warmup lacks its exact step unit")
        if self.step_unit == "not_applicable" and (
            self.lag_steps or self.warmup_steps
        ):
            raise ValueError("wide steps cannot have an unspecified unit")
        _text(self.availability_rule)
        if (
            self.missingness_states != tuple(v.value for v in JoinState)
            or training_json(training_load(self.native_semantics_json))
            != self.native_semantics_json
        ):
            raise ValueError(
                "wide column must preserve canonical native semantics"
            )
        if self.dtype is WideValueType.UNAVAILABLE and self.units is not None:
            raise ValueError("unavailable source cannot invent units")


@dataclass(frozen=True, slots=True)
class TrainingWideTileV1(TrainingContract):
    KIND: ClassVar[str] = "wide-tile"
    join_plan_json: str

    def _validate(self) -> None:
        plan = self.join_plan
        if plan.to_json() != self.join_plan_json:
            raise ValueError(
                "wide tile requires the unchanged canonical join plan"
            )
        names = tuple(c.name for c in plan.columns)
        if not names or names != tuple(sorted(names)):
            raise ValueError("wide tile columns must be nonempty and sorted")

    @property
    def join_plan(self) -> TrainingJoinPlanV1:
        return TrainingJoinPlanV1.from_json(self.join_plan_json)


def _wide_geometry(
    grain: TrainingWideGrainV1, plans: tuple[TrainingJoinPlanV1, ...]
) -> tuple[str, ...]:
    """Pure complete-plan geometry; run before source replay as well as sealing."""
    if not 1 <= len(plans) <= MAX_WIDE_TILES:
        raise ValueError("wide plan count bound differs")
    names = tuple(sorted({c.name for p in plans for c in p.columns}))
    if not 1 <= len(names) <= MAX_WIDE_COLUMNS:
        raise ValueError("wide column count bound differs")
    keys = tuple(
        (p.spine.artifact_id, tuple(c.name for c in p.columns)) for p in plans
    )
    if keys != tuple(sorted(set(keys))):
        raise ValueError("wide tiles must have canonical unique coordinates")
    if any(
        not p.columns
        or tuple(c.name for c in p.columns)
        != tuple(sorted(c.name for c in p.columns))
        for p in plans
    ):
        raise ValueError("wide tile columns must be nonempty and sorted")
    if sum(len(p.to_json()) for p in plans) > MAX_TRAINING_BYTES:
        raise ValueError("wide plan aggregate native envelope budget exceeded")
    spines = {p.spine.artifact_id: p.spine for p in plans}
    if len(spines) > MAX_WIDE_SPINES:
        raise ValueError("wide complete-spine budget exceeded")
    if (
        len({p.spine.source.artifact_id for p in plans}) != 1
        or len({p.spine.ownership.artifact_id for p in plans}) != 1
    ):
        raise ValueError("wide tiles must share complete source/ownership")
    if (
        len({p.information_mode for p in plans}) != 1
        or len({p.spine.request.consumer_mode for p in plans}) != 1
    ):
        raise ValueError("wide tiles cannot mix information/consumer modes")
    definitions: dict[str, tuple[str, JoinFamily]] = {}
    for identity in spines:
        selected = tuple(p for p in plans if p.spine.artifact_id == identity)
        if len({tuple(s.to_json() for s in p.sources) for p in selected}) != 1:
            raise ValueError(
                "column tiles must retain the complete source inventory"
            )
        covered = tuple(c.name for p in selected for c in p.columns)
        if len(covered) != len(names) or set(covered) != set(names):
            raise ValueError("wide rectangle has missing/duplicate columns")
        for plan in selected:
            sources = {s.key: s for s in plan.sources}
            for column in plan.columns:
                definition = (
                    column.to_json(),
                    sources[column.source_key].family,
                )
                if (
                    column.name in definitions
                    and definitions[column.name] != definition
                ):
                    raise ValueError(
                        "wide definition differs from native join column"
                    )
                definitions[column.name] = definition
    if (
        grain.kind is not WideRowGrain.PANEL
        and len({p.spine.request.product_manifest_id for p in plans}) != 1
    ):
        raise ValueError("multiple native members require explicit panel grain")
    if grain.cutoffs:
        observed = tuple(
            sorted(
                s.request.decision_time_ns
                for s in spines.values()
                if s.request.decision_time_ns is not None
            )
        )
        if observed != grain.cutoffs:
            raise ValueError(
                "grid must contain exactly one complete spine per cutoff"
            )
    projected_rows = sum(
        len(s.request.symbols) if grain.cutoffs else len(s.rows)
        for s in spines.values()
    )
    if (
        projected_rows > MAX_WIDE_ROWS
        or projected_rows * len(names) > MAX_WIDE_CELLS
    ):
        raise ValueError("wide aggregate row/cell budget exceeded")
    return names


@dataclass(frozen=True, slots=True)
class TrainingWidePlanV1(TrainingContract):
    KIND: ClassVar[str] = "wide-plan"
    grain: TrainingWideGrainV1
    columns: tuple[TrainingWideColumnV1, ...]
    tiles: tuple[TrainingWideTileV1, ...]
    nonclaims: tuple[str, ...] = WIDE_NONCLAIMS
    contract_version: str = WIDE_VERSION

    def _validate(self) -> None:
        if (
            self.contract_version != WIDE_VERSION
            or self.nonclaims != WIDE_NONCLAIMS
            or not 1 <= len(self.columns) <= MAX_WIDE_COLUMNS
            or not 1 <= len(self.tiles) <= MAX_WIDE_TILES
        ):
            raise ValueError("wide plan version/nonclaims/count bound differs")
        names = tuple(c.column.name for c in self.columns)
        if names != tuple(sorted(set(names))):
            raise ValueError("wide registry names must be sorted and unique")
        definitions = {c.column.name: c for c in self.columns}
        plans = tuple(t.join_plan for t in self.tiles)
        if _wide_geometry(self.grain, plans) != names:
            raise ValueError("wide rectangle has missing/duplicate columns")
        for plan in plans:
            sources = {s.key: s for s in plan.sources}
            for column in plan.columns:
                definition = definitions[column.name]
                if (
                    definition.column != column
                    or definition.origin_mode is not plan.information_mode
                    or definition.family
                    is not sources[column.source_key].family
                ):
                    raise ValueError(
                        "wide definition differs from native join column"
                    )

    @property
    def schema_sha256(self) -> str:
        return hashlib.sha256(
            training_json(
                {"columns": [c.to_dict() for c in self.columns]}
            ).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class TrainingWideRowV1(TrainingContract):
    KIND: ClassVar[str] = "wide-row"
    spine_id: str
    symbol: str
    native_row_id: str | None
    grid_cutoff_ns: int | None
    state: str

    def _validate(self) -> None:
        for value in (self.spine_id, self.symbol):
            _text(value)
        if self.grid_cutoff_ns is not None:
            _clock(self.grid_cutoff_ns)
        if self.native_row_id is None:
            if (
                self.state != "missing_spine_anchor"
                or self.grid_cutoff_ns is None
            ):
                raise ValueError(
                    "absent wide anchor must remain explicitly unavailable"
                )
        else:
            _text(self.native_row_id)
            if self.state != "retained_native_row":
                raise ValueError(
                    "wide row reference cannot relabel a native row"
                )


@dataclass(frozen=True, slots=True)
class TrainingWideChildV1(TrainingContract):
    KIND: ClassVar[str] = "wide-child"
    tile_id: str
    batch_id: str
    file_sha256: str
    byte_count: int

    def _validate(self) -> None:
        _text(self.tile_id)
        _text(self.batch_id)
        _digest(self.file_sha256)
        if not 0 < self.byte_count <= 8 * 1024 * 1024:
            raise ValueError(
                "native wide child exceeds unchanged envelope bound"
            )

    @property
    def filename(self) -> str:
        return f"training-join-batch-{self.file_sha256}.json"


@dataclass(frozen=True, slots=True)
class TrainingWideControlV1(TrainingContract):
    KIND: ClassVar[str] = "wide-control"
    spine_id: str
    child: TrainingWideChildV1

    def _validate(self) -> None:
        _text(self.spine_id)


@dataclass(frozen=True, slots=True)
class TrainingWideGroupV1(TrainingContract):
    KIND: ClassVar[str] = "wide-group"
    spine_id: str
    columns: tuple[str, ...]
    child: TrainingWideChildV1

    def _validate(self) -> None:
        _text(self.spine_id)
        if not self.columns or self.columns != tuple(sorted(set(self.columns))):
            raise ValueError("wide group columns must be sorted and unique")
        for name in self.columns:
            _text(name)


@dataclass(frozen=True, slots=True)
class TrainingWideManifestV1(TrainingContract):
    """Value-free layout. Actual spines/evidence live in native control files."""

    KIND: ClassVar[str] = "wide-manifest"
    grain: TrainingWideGrainV1
    columns: tuple[TrainingWideColumnV1, ...]
    plan_id: str
    rows: tuple[TrainingWideRowV1, ...]
    controls: tuple[TrainingWideControlV1, ...]
    groups: tuple[TrainingWideGroupV1, ...]
    schema_sha256: str
    content_sha256: str
    nonclaims: tuple[str, ...] = WIDE_NONCLAIMS

    def _validate(self) -> None:
        _text(self.plan_id)
        _digest(self.content_sha256)
        expected_schema = hashlib.sha256(
            training_json(
                {"columns": [c.to_dict() for c in self.columns]}
            ).encode()
        ).hexdigest()
        if (
            self.schema_sha256 != expected_schema
            or self.nonclaims != WIDE_NONCLAIMS
        ):
            raise ValueError("wide manifest schema/nonclaims differ")
        if (
            not 1 <= len(self.columns) <= MAX_WIDE_COLUMNS
            or not 1 <= len(self.controls) <= MAX_WIDE_SPINES
            or not 1 <= len(self.groups) <= MAX_WIDE_TILES
            or tuple(c.spine_id for c in self.controls)
            != tuple(sorted({c.spine_id for c in self.controls}))
            or any(
                g.spine_id not in {c.spine_id for c in self.controls}
                for g in self.groups
            )
            or any(
                r.spine_id not in {c.spine_id for c in self.controls}
                for r in self.rows
            )
            or len(self.rows) > MAX_WIDE_ROWS
            or len(self.rows) * len(self.columns) > MAX_WIDE_CELLS
            or sum(c.child.byte_count for c in self.controls)
            + sum(g.child.byte_count for g in self.groups)
            > MAX_WIDE_CHILD_BYTES
        ):
            raise ValueError(
                "wide manifest aggregate/native-child budget differs"
            )
        if len({r.artifact_id for r in self.rows}) != len(self.rows):
            raise ValueError("wide manifest duplicates logical rows")

"""Row-free exploratory support census after an authorized source verification.

No function in this module opens source files, grants approval, generates paths,
or computes a calibration/outcome. Process-local ``_VerifiedSource`` objects are
inputs, not authentication receipts. The caller owns the source-byte and access
boundary. Complete reports bind the supplied source and recomputed ownership;
replaying a report requires the same independently verified source again.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from typing import Callable, ClassVar, TypeVar

from histdatacom.datasets import DatasetCatalog

from .training_contracts import (
    DAY_NS,
    MAX_TRAINING_SOURCE_ROWS,
    TrainingContract,
    TrainingOwnershipV1,
    _clock,
    _digest,
    _text,
    training_json,
)
from .training_lineage import _ObservedRow, _ownership, _VerifiedSource
from .training_weight_calibration import WEIGHT_SYMBOLS
from .training_weight_diagnostic_protocol import (
    TrainingWeightDiagnosticEvidenceKind,
    diagnostic_json,
    diagnostic_load,
    read_training_weight_diagnostic_protocol,
)
from .training_weight_lineage import (
    CORE_END_NS,
    CORE_START_NS,
    MAX_AGE_NS,
    MAX_SUPPORT_ROWS,
    SECOND_NS,
    TrainingWeightSourcePlanV1,
    WeightDayRefusal,
    WeightEvidenceKind,
    _day_ns,
    _selected_days,
    _thin_weight_support,
    _weight_anchor_support,
)

_GATE_NAMES = (
    "support_row_budget",
    "left_boundary",
    "right_boundary",
    "minimum_core_rows",
    "valid_support_quotes",
    "parent_probes",
    "subset_probes",
)
_PREREQUISITES = (
    (),
    (),
    (),
    (),
    ("support_row_budget", "left_boundary", "right_boundary"),
    (
        "support_row_budget",
        "left_boundary",
        "right_boundary",
        "valid_support_quotes",
    ),
    (
        "support_row_budget",
        "left_boundary",
        "right_boundary",
        "valid_support_quotes",
    ),
)
_D = TypeVar("_D", bound="_DiagnosticRecord")


class _DiagnosticRecord(TrainingContract):
    __slots__ = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        diagnostic_json(self.to_dict())

    @classmethod
    def from_json(cls: type[_D], text: str) -> _D:
        value = diagnostic_load(text)
        if not isinstance(value, dict):
            raise ValueError("source diagnostic envelope must be an object")
        return cls.from_dict(value)


@dataclass(frozen=True, slots=True)
class TrainingWeightProbeWitnessV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-probe-witness"
    status: str
    probe_offset: int
    probe_time_ns: int
    selected_parent_row_id: int | None
    selected_time_ns: int | None
    age_ns: int | None

    def _validate(self) -> None:
        if not 0 <= self.probe_offset < 600:
            raise ValueError("probe witness is outside the fixed grid")
        _clock(self.probe_time_ns)
        selected = (
            self.selected_parent_row_id,
            self.selected_time_ns,
            self.age_ns,
        )
        if self.status == "missing_prior":
            if selected != (None, None, None):
                raise ValueError("missing prior cannot invent selected support")
        elif self.status == "stale":
            row_id, time, age = selected
            if (
                row_id is None
                or not 0 < row_id <= MAX_TRAINING_SOURCE_ROWS
                or time is None
                or age is None
                or not 0 <= time <= self.probe_time_ns
                or age != self.probe_time_ns - time
                or age <= MAX_AGE_NS
            ):
                raise ValueError(
                    "stale witness does not identify stale support"
                )
        else:
            raise ValueError("unsupported probe witness status")


@dataclass(frozen=True, slots=True)
class TrainingWeightProbeDiagnosticsV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-probes"
    status: str
    blocked_by: tuple[str, ...]
    success_count: int
    missing_prior_count: int
    stale_count: int
    maximum_defined_age_ns: int | None
    earliest_failure: TrainingWeightProbeWitnessV1 | None

    def _validate(self) -> None:
        counts = self.success_count, self.missing_prior_count, self.stale_count
        if any(not 0 <= n <= 600 for n in counts):
            raise ValueError("probe counts exceed the fixed grid")
        if self.status == "not_evaluable":
            if (
                not self.blocked_by
                or any(g not in _GATE_NAMES for g in self.blocked_by)
                or counts != (0, 0, 0)
                or self.maximum_defined_age_ns is not None
                or self.earliest_failure is not None
            ):
                raise ValueError(
                    "unevaluated probes cannot invent observations"
                )
            return
        if self.status != "complete" or self.blocked_by or sum(counts) != 600:
            raise ValueError(
                "complete probe census must account for 600 probes"
            )
        if self.maximum_defined_age_ns is not None:
            _clock(self.maximum_defined_age_ns)
        if (self.maximum_defined_age_ns is None) != (
            self.missing_prior_count == 600
        ):
            raise ValueError("maximum probe age requires selected support")
        failed = self.missing_prior_count + self.stale_count
        if (self.earliest_failure is None) != (failed == 0):
            raise ValueError("probe failure count and witness disagree")
        if self.stale_count and (
            self.maximum_defined_age_ns is None
            or self.maximum_defined_age_ns <= MAX_AGE_NS
        ):
            raise ValueError("stale probes require a stale maximum age")
        if self.earliest_failure is not None and (
            (
                self.earliest_failure.status == "missing_prior"
                and not self.missing_prior_count
            )
            or (
                self.earliest_failure.status == "stale" and not self.stale_count
            )
            or self.earliest_failure.probe_offset > self.success_count
        ):
            raise ValueError(
                "earliest probe witness contradicts counted failures"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightSourceGateV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-gate"
    name: str
    status: str
    prerequisites: tuple[str, ...]

    def _validate(self) -> None:
        if self.name not in _GATE_NAMES or self.status not in (
            "passed",
            "failed",
            "not_evaluable",
        ):
            raise ValueError("unsupported source diagnostic gate")
        if self.prerequisites != _PREREQUISITES[_GATE_NAMES.index(self.name)]:
            raise ValueError(
                "source gate prerequisites differ from fixed rules"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightSourceCellV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-cell"
    utc_date: str
    symbol: str
    source_series_id: str
    source_partition_sha256: str
    historical_unit_id: str | None
    core_start_ns: int
    core_end_ns: int
    support_start_ns: int
    support_end_ns: int
    count_status: str
    window_row_count: int
    core_row_count: int
    duplicate_time_count: int
    invalid_window_quote_count: int
    left_parent_row_id: int | None
    left_time_ns: int | None
    left_distance_ns: int | None
    right_parent_row_id: int | None
    right_time_ns: int | None
    right_distance_ns: int | None
    selected_support_row_count: int | None
    invalid_support_quote_count: int | None
    retained_row_count: int | None
    hidden_row_count: int | None
    cell_local_ordinal_map_sha256: str | None
    gates: tuple[TrainingWeightSourceGateV1, ...]
    parent_probes: TrainingWeightProbeDiagnosticsV1
    subset_probes: TrainingWeightProbeDiagnosticsV1
    classification: str

    def _validate(self) -> None:
        day = _day_ns(self.utc_date)
        _text(self.source_series_id)
        _digest(self.source_partition_sha256)
        if self.historical_unit_id is not None:
            _text(self.historical_unit_id)
        if self.symbol not in WEIGHT_SYMBOLS or (
            self.core_start_ns,
            self.core_end_ns,
            self.support_start_ns,
            self.support_end_ns,
        ) != (
            day + CORE_START_NS,
            day + CORE_END_NS,
            day + CORE_START_NS - MAX_AGE_NS,
            day + CORE_END_NS + MAX_AGE_NS,
        ):
            raise ValueError("source cell differs from fixed symbol/time scope")
        if self.count_status != "complete_exact" or not (
            0
            <= self.core_row_count
            <= self.window_row_count
            <= MAX_TRAINING_SOURCE_ROWS
        ):
            raise ValueError("source census requires complete bounded counts")
        if not (
            0 <= self.duplicate_time_count <= max(0, self.window_row_count - 1)
            and 0 <= self.invalid_window_quote_count <= self.window_row_count
        ):
            raise ValueError("source diagnostic counts exceed window support")
        for row_id, time, distance, left in (
            (
                self.left_parent_row_id,
                self.left_time_ns,
                self.left_distance_ns,
                True,
            ),
            (
                self.right_parent_row_id,
                self.right_time_ns,
                self.right_distance_ns,
                False,
            ),
        ):
            if row_id is None:
                if time is not None or distance is not None:
                    raise ValueError("absent boundary cannot invent a distance")
            elif (
                not 0 < row_id <= MAX_TRAINING_SOURCE_ROWS
                or time is None
                or distance is None
                or not 0 <= distance <= MAX_AGE_NS
                or distance
                != (
                    self.core_start_ns - time
                    if left
                    else time - self.core_end_ns
                )
            ):
                raise ValueError("boundary identity/time/distance disagree")
        if tuple(g.name for g in self.gates) != _GATE_NAMES:
            raise ValueError("source cell must retain every fixed gate exactly")
        statuses = {g.name: g.status for g in self.gates}
        expected = (
            "passed" if self.window_row_count <= MAX_SUPPORT_ROWS else "failed",
            "passed" if self.left_parent_row_id is not None else "failed",
            "passed" if self.right_parent_row_id is not None else "failed",
            "passed" if self.core_row_count >= 64 else "failed",
        )
        if tuple(g.status for g in self.gates[:4]) != expected:
            raise ValueError("source gates differ from complete row counts")
        support_known = all(s == "passed" for s in expected[:3])
        if support_known:
            if (
                self.selected_support_row_count is None
                or not 2
                <= self.selected_support_row_count
                <= self.window_row_count
                or self.invalid_support_quote_count is None
                or not 0
                <= self.invalid_support_quote_count
                <= self.selected_support_row_count
                or self.invalid_support_quote_count
                > self.invalid_window_quote_count
            ):
                raise ValueError("known support requires exact bounded counts")
            quotes = "failed" if self.invalid_support_quote_count else "passed"
        else:
            if (
                self.selected_support_row_count is not None
                or self.invalid_support_quote_count is not None
            ):
                raise ValueError("blocked support cannot invent exact counts")
            quotes = "not_evaluable"
        if statuses["valid_support_quotes"] != quotes:
            raise ValueError(
                "quote gate differs from its support prerequisites"
            )
        blocked = tuple(k for k in _PREREQUISITES[5] if statuses[k] != "passed")
        for name, probes in (
            ("parent_probes", self.parent_probes),
            ("subset_probes", self.subset_probes),
        ):
            expected_status = (
                "not_evaluable"
                if blocked
                else "passed" if probes.success_count == 600 else "failed"
            )
            if (
                probes.blocked_by != blocked
                or statuses[name] != expected_status
            ):
                raise ValueError("probe gate differs from prerequisite census")
            if (
                probes.earliest_failure is not None
                and probes.earliest_failure.probe_time_ns
                != self.core_start_ns
                + probes.earliest_failure.probe_offset * SECOND_NS
            ):
                raise ValueError("probe witness differs from fixed time grid")
        if blocked:
            if (
                self.retained_row_count,
                self.hidden_row_count,
                self.cell_local_ordinal_map_sha256,
            ) != (None, None, None):
                raise ValueError("blocked subset cannot invent ordinal support")
        else:
            count = self.selected_support_row_count
            if (
                count is None
                or self.retained_row_count != 2 + (count - 2 + 3) // 4
                or self.hidden_row_count != count - self.retained_row_count
            ):
                raise ValueError(
                    "subset counts differ from exact ordinal thinning"
                )
            if self.cell_local_ordinal_map_sha256 is None:
                raise ValueError(
                    "subset requires its row-free ordinal-map digest"
                )
            _digest(self.cell_local_ordinal_map_sha256)
        expected_classification = (
            "not_evaluable"
            if blocked
            else (
                "parent_failed"
                if self.parent_probes.success_count != 600
                else (
                    "subset_only_failed"
                    if self.subset_probes.success_count != 600
                    else "both_passed"
                )
            )
        )
        if self.classification != expected_classification:
            raise ValueError(
                "paired support classification differs from probes"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightSourceDayV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-day"
    utc_date: str
    historical_unit_id: str | None
    original_admitted: bool
    original_reason: str | None
    first_failing_symbol: str | None
    cell_ids: tuple[str, ...]

    def _validate(self) -> None:
        _day_ns(self.utc_date)
        if self.original_admitted != (self.original_reason is None):
            raise ValueError("original admission and refusal disagree")
        if self.original_reason is not None:
            WeightDayRefusal(self.original_reason)
        if (
            self.first_failing_symbol is not None
            and self.first_failing_symbol not in WEIGHT_SYMBOLS
        ):
            raise ValueError("unknown first-failure symbol")
        if len(self.cell_ids) != 3 or len(set(self.cell_ids)) != 3:
            raise ValueError("source day requires its complete three cells")
        for value in self.cell_ids:
            _text(value)


@dataclass(frozen=True, slots=True)
class TrainingWeightSourceDiagnosticsV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-diagnostics"
    protocol_id: str
    evidence_kind: TrainingWeightDiagnosticEvidenceKind
    source_plan_id: str
    source_id: str
    dataset_version_id: str
    ownership_id: str
    period: str
    cells: tuple[TrainingWeightSourceCellV1, ...]
    days: tuple[TrainingWeightSourceDayV1, ...]

    def _validate(self) -> None:
        for value in (
            self.protocol_id,
            self.source_plan_id,
            self.source_id,
            self.dataset_version_id,
            self.ownership_id,
        ):
            _text(value)
        _validate_inventory(
            self.protocol_id, _month_dates(self.period), self.cells, self.days
        )


def _month_dates(period: str) -> tuple[str, ...]:
    if period not in ("201001", "201101", "201102"):
        raise ValueError("source diagnostic period is outside fixed scope")
    return tuple(
        d
        for d in read_training_weight_diagnostic_protocol().scheduled_dates()
        if d.replace("-", "").startswith(period)
    )


def _validate_inventory(
    protocol_id: str,
    dates: tuple[str, ...],
    cells: tuple[TrainingWeightSourceCellV1, ...],
    days: tuple[TrainingWeightSourceDayV1, ...],
) -> None:
    if protocol_id != read_training_weight_diagnostic_protocol().artifact_id:
        raise ValueError("source diagnostic protocol identity differs")
    if (
        not dates
        or tuple(d.utc_date for d in days) != dates
        or tuple((c.utc_date, c.symbol) for c in cells)
        != tuple((d, s) for d in dates for s in WEIGHT_SYMBOLS)
    ):
        raise ValueError(
            "source diagnostics require exact scheduled cell scope"
        )
    for index, day in enumerate(days):
        graph = cells[index * 3 : index * 3 + 3]
        reason, symbol = _original_reason(graph)
        if (
            day.cell_ids != tuple(c.artifact_id for c in graph)
            or any(
                c.historical_unit_id != day.historical_unit_id for c in graph
            )
            or (day.original_reason, day.first_failing_symbol)
            != (reason, symbol)
        ):
            raise ValueError("day reconciliation differs from all source cells")


@dataclass(frozen=True, slots=True)
class TrainingWeightSourceDiagnosticShardV1(_DiagnosticRecord):
    KIND: ClassVar[str] = "weight-source-diagnostic-shard"
    report_id: str
    protocol_id: str
    evidence_kind: TrainingWeightDiagnosticEvidenceKind
    source_plan_id: str
    source_id: str
    dataset_version_id: str
    ownership_id: str
    period: str
    shard_ordinal: int
    cells: tuple[TrainingWeightSourceCellV1, ...]
    days: tuple[TrainingWeightSourceDayV1, ...]

    def _validate(self) -> None:
        for value in (
            self.report_id,
            self.source_plan_id,
            self.source_id,
            self.dataset_version_id,
            self.ownership_id,
        ):
            _text(value)
        if not 1 <= self.shard_ordinal <= 3:
            raise ValueError("source diagnostic shard ordinal is outside scope")
        start = (self.shard_ordinal - 1) * 8
        _validate_inventory(
            self.protocol_id,
            _month_dates(self.period)[start : start + 8],
            self.cells,
            self.days,
        )


def split_training_weight_source_diagnostics(
    report: TrainingWeightSourceDiagnosticsV1,
) -> tuple[TrainingWeightSourceDiagnosticShardV1, ...]:
    """Exactly three canonical eight/eight/remainder shards per month."""
    if type(report) is not TrainingWeightSourceDiagnosticsV1:
        raise TypeError("source shard splitting requires its typed report")
    return tuple(
        TrainingWeightSourceDiagnosticShardV1(
            report.artifact_id,
            report.protocol_id,
            report.evidence_kind,
            report.source_plan_id,
            report.source_id,
            report.dataset_version_id,
            report.ownership_id,
            report.period,
            index + 1,
            report.cells[index * 24 : (index + 1) * 24],
            report.days[index * 8 : (index + 1) * 8],
        )
        for index in range(3)
    )


def verify_training_weight_source_diagnostic_shards(
    shards: tuple[TrainingWeightSourceDiagnosticShardV1, ...],
) -> TrainingWeightSourceDiagnosticsV1:
    """Reassemble exact metadata/content; this does not reverify source files."""
    if (
        type(shards) is not tuple
        or len(shards) != 3
        or any(
            type(s) is not TrainingWeightSourceDiagnosticShardV1 for s in shards
        )
        or tuple(s.shard_ordinal for s in shards) != (1, 2, 3)
    ):
        raise ValueError(
            "source replay requires all three ordered month shards"
        )
    first = shards[0]
    report = TrainingWeightSourceDiagnosticsV1(
        first.protocol_id,
        first.evidence_kind,
        first.source_plan_id,
        first.source_id,
        first.dataset_version_id,
        first.ownership_id,
        first.period,
        tuple(c for s in shards for c in s.cells),
        tuple(d for s in shards for d in s.days),
    )
    if split_training_weight_source_diagnostics(report) != shards:
        raise ValueError("source shards differ from exact report identity")
    return report


def _original_reason(
    cells: tuple[TrainingWeightSourceCellV1, ...],
) -> tuple[str | None, str | None]:
    # The old collector gives *any* symbol's overflow priority over every
    # other day-level refusal, even one earlier in lexical symbol order.
    if any(c.window_row_count > MAX_SUPPORT_ROWS for c in cells):
        return WeightDayRefusal.SUPPORT_LIMIT.value, None
    for cell in cells:
        statuses = {g.name: g.status for g in cell.gates}
        for names, reason in (
            (
                ("left_boundary", "right_boundary"),
                WeightDayRefusal.MISSING_BOUNDARY,
            ),
            (("minimum_core_rows",), WeightDayRefusal.INSUFFICIENT_ROWS),
            (("valid_support_quotes",), WeightDayRefusal.INVALID_QUOTES),
            (("parent_probes", "subset_probes"), WeightDayRefusal.STALE_PROBE),
        ):
            if any(statuses[n] != "passed" for n in names):
                return reason.value, cell.symbol
    return None, None


@dataclass
class _Census:
    rows: list[_ObservedRow] = field(default_factory=list)
    window_count: int = 0
    core_count: int = 0
    duplicate_count: int = 0
    invalid_count: int = 0
    previous_time: int | None = None
    left: _ObservedRow | None = None
    right: _ObservedRow | None = None


def _invalid(row: _ObservedRow) -> bool:
    return not (
        math.isfinite(row.bid)
        and math.isfinite(row.ask)
        and 0 < row.bid <= row.ask
    )


def _probes(
    rows: tuple[_ObservedRow, ...], start: int
) -> TrainingWeightProbeDiagnosticsV1:
    success = missing = stale = cursor = 0
    maximum: int | None = None
    first: TrainingWeightProbeWitnessV1 | None = None
    for offset in range(600):
        probe = start + offset * SECOND_NS
        while cursor < len(rows) and rows[cursor].event_time_ns <= probe:
            cursor += 1
        if cursor == 0:
            missing += 1
            if first is None:
                first = TrainingWeightProbeWitnessV1(
                    "missing_prior", offset, probe, None, None, None
                )
            continue
        row = rows[cursor - 1]
        age = probe - row.event_time_ns
        maximum = age if maximum is None else max(maximum, age)
        if age <= MAX_AGE_NS:
            success += 1
        else:
            stale += 1
            if first is None:
                first = TrainingWeightProbeWitnessV1(
                    "stale", offset, probe, row.row_id, row.event_time_ns, age
                )
    return TrainingWeightProbeDiagnosticsV1(
        "complete", (), success, missing, stale, maximum, first
    )


def _cell(
    plan: TrainingWeightSourcePlanV1,
    day: str,
    symbol: str,
    series_id: str,
    partition_sha256: str,
    unit_id: str | None,
    census: _Census,
) -> TrainingWeightSourceCellV1:
    start, end = _day_ns(day) + CORE_START_NS, _day_ns(day) + CORE_END_NS
    statuses = [
        "passed" if census.window_count <= MAX_SUPPORT_ROWS else "failed",
        "passed" if census.left is not None else "failed",
        "passed" if census.right is not None else "failed",
        "passed" if census.core_count >= 64 else "failed",
    ]
    support = (
        _weight_anchor_support(census.rows, start, end)
        if all(s == "passed" for s in statuses[:3])
        else None
    )
    bad = None if support is None else sum(_invalid(r) for r in support)
    statuses.append(
        "not_evaluable" if bad is None else "failed" if bad else "passed"
    )
    blocked = tuple(
        n
        for n in _PREREQUISITES[5]
        if statuses[_GATE_NAMES.index(n)] != "passed"
    )
    subset: tuple[_ObservedRow, ...] | None = None
    ordinal_digest = None
    if blocked:
        parent_probes = subset_probes = TrainingWeightProbeDiagnosticsV1(
            "not_evaluable", blocked, 0, 0, 0, None, None
        )
        classification = "not_evaluable"
    else:
        if support is None:
            raise ValueError("source support unexpectedly absent")
        subset = _thin_weight_support(support)
        parent_probes, subset_probes = _probes(support, start), _probes(
            subset, start
        )
        ordinal_digest = hashlib.sha256(
            training_json(
                {
                    "mapping": "cell-local-subset-ordinal.v1",
                    "source_id": plan.parent_source.artifact_id,
                    "source_series_id": series_id,
                    "period": plan.period,
                    "utc_date": day,
                    "ordinals": [
                        [i, r.row_id, r.event_time_ns]
                        for i, r in enumerate(subset, 1)
                    ],
                }
            ).encode("ascii")
        ).hexdigest()
        classification = (
            "parent_failed"
            if parent_probes.success_count != 600
            else (
                "subset_only_failed"
                if subset_probes.success_count != 600
                else "both_passed"
            )
        )
    statuses.extend(
        (
            "not_evaluable"
            if p.status == "not_evaluable"
            else "passed" if p.success_count == 600 else "failed"
        )
        for p in (parent_probes, subset_probes)
    )
    left, right = census.left, census.right
    return TrainingWeightSourceCellV1(
        day,
        symbol,
        series_id,
        partition_sha256,
        unit_id,
        start,
        end,
        start - MAX_AGE_NS,
        end + MAX_AGE_NS,
        "complete_exact",
        census.window_count,
        census.core_count,
        census.duplicate_count,
        census.invalid_count,
        None if left is None else left.row_id,
        None if left is None else left.event_time_ns,
        None if left is None else start - left.event_time_ns,
        None if right is None else right.row_id,
        None if right is None else right.event_time_ns,
        None if right is None else right.event_time_ns - end,
        None if support is None else len(support),
        bad,
        None if subset is None else len(subset),
        (
            None
            if subset is None or support is None
            else len(support) - len(subset)
        ),
        ordinal_digest,
        tuple(
            TrainingWeightSourceGateV1(n, s, p)
            for n, s, p in zip(_GATE_NAMES, statuses, _PREREQUISITES)
        ),
        parent_probes,
        subset_probes,
        classification,
    )


def diagnose_verified_training_weight_source(
    plan: TrainingWeightSourcePlanV1,
    verified: _VerifiedSource,
    ownership: TrainingOwnershipV1,
    *,
    protocol_id: str | None = None,
    on_cell: Callable[[str, str], None] | None = None,
) -> TrainingWeightSourceDiagnosticsV1:
    """Census one complete month without reading files or authorizing access.

    Counts are exact after a complete <=2,000,000-row traversal. Only 4096
    window rows per cell are retained; an overflow never truncates counts or
    gets treated as a passing probe. The ordinal digest uses one-based local
    subset positions, not invented globally published subset row identities.
    Quotes are inspected exactly as read. ``quote_projection`` is catalog
    metadata for other consumers, not proof that these raw rows were projected.
    Native timestamp ordering preserves physical IDs and duplicate-clock order,
    including bounded physical timestamp regressions allowed by the adapter.
    Missing/nonfinite sources rejected by the caller's reader are operational
    integrity failures, not a claimed completed census from this function.
    """
    expected_protocol = read_training_weight_diagnostic_protocol().artifact_id
    if protocol_id is not None and protocol_id != expected_protocol:
        raise ValueError("source diagnostic protocol identity differs")
    protocol_id = expected_protocol
    if (
        type(plan) is not TrainingWeightSourcePlanV1
        or type(verified) is not _VerifiedSource
        or type(ownership) is not TrainingOwnershipV1
    ):
        raise TypeError(
            "source diagnostics require exact typed process-local inputs"
        )
    if (
        verified.source != plan.parent_source
        or verified.products
        or verified.contexts
        or verified.features
        or verified.graph_symbols != WEIGHT_SYMBOLS
    ):
        raise ValueError(
            "source diagnostics require the exact unaugmented parent"
        )
    if (
        type(verified.observed) is not tuple
        or not 0 < len(verified.observed) <= MAX_TRAINING_SOURCE_ROWS
    ):
        raise ValueError(
            "source diagnostics exceed the complete-month row bound"
        )
    catalog = DatasetCatalog.from_json(plan.parent_source.catalog_json)
    version = next(
        v
        for v in catalog.versions
        if v.dataset_version_id == plan.parent_source.dataset_version_id
    )
    partitions = {p.symbol: p for p in version.partitions}
    period = plan.period
    if sum(p.row_count for p in partitions.values()) != len(verified.observed):
        raise ValueError(
            "source row count differs from complete partition scope"
        )
    # Verification preserves physical IDs but orders rows by timestamp, so
    # allowed raw timestamp regressions need not have monotone IDs here.
    # A bounded bitmap proves exact ordinal coverage without renumbering.
    seen = {s: bytearray(p.row_count + 1) for s, p in partitions.items()}
    previous_key: tuple[str, int, int] | None = None
    dates = plan.dates
    starts = {_day_ns(d): d for d in dates}
    # Index contiguous windows without a copied month or quote calculation.
    # Each cell's progress callback precedes its actual count/probe work.
    ranges: dict[tuple[str, str], tuple[int, int]] = {}
    for index, row in enumerate(verified.observed):
        if type(row) is not _ObservedRow or row.symbol not in partitions:
            raise ValueError("unexpected observed row in diagnostic source")
        partition = partitions[row.symbol]
        key = row.symbol, row.event_time_ns, row.row_id
        if (
            row.series_id != partition.series_id
            or row.period != period
            or type(row.row_id) is not int
            or not 1 <= row.row_id <= partition.row_count
            or seen[row.symbol][row.row_id]
            or (previous_key is not None and key <= previous_key)
        ):
            raise ValueError(
                "diagnostics require original ordered physical rows"
            )
        seen[row.symbol][row.row_id] = 1
        previous_key = key
        day_start = row.event_time_ns // DAY_NS * DAY_NS
        day = starts.get(day_start)
        if (
            day is None
            or not day_start + CORE_START_NS - MAX_AGE_NS
            <= row.event_time_ns
            <= day_start + CORE_END_NS + MAX_AGE_NS
        ):
            continue
        cell_key = day, row.symbol
        ranges[cell_key] = ranges.get(cell_key, (index, index))[0], index + 1
    if _ownership(verified) != ownership:
        raise ValueError(
            "source/ownership differs from the complete parent rows"
        )
    units = {u.start_ns: u.artifact_id for u in ownership.units}
    computed: list[TrainingWeightSourceCellV1] = []
    for day in dates:
        start, end = _day_ns(day) + CORE_START_NS, _day_ns(day) + CORE_END_NS
        for symbol in WEIGHT_SYMBOLS:
            if on_cell is not None:
                on_cell(day, symbol)
            census = _Census()
            lo, hi = ranges.get((day, symbol), (0, 0))
            for index in range(lo, hi):
                row = verified.observed[index]
                census.window_count += 1
                census.core_count += start <= row.event_time_ns < end
                census.duplicate_count += (
                    census.previous_time == row.event_time_ns
                )
                census.invalid_count += _invalid(row)
                census.previous_time = row.event_time_ns
                if row.event_time_ns <= start:
                    census.left = row
                if row.event_time_ns >= end and census.right is None:
                    census.right = row
                if len(census.rows) < MAX_SUPPORT_ROWS:
                    census.rows.append(row)
            computed.append(
                _cell(
                    plan,
                    day,
                    symbol,
                    partitions[symbol].series_id,
                    partitions[symbol].artifact.sha256,
                    units.get(_day_ns(day)),
                    census,
                )
            )
    cells = tuple(computed)
    old_admitted, old_refusals = _selected_days(plan, verified)
    old_reasons = {r.utc_date: r.reason.value for r in old_refusals}
    days = []
    for index, date in enumerate(dates):
        graph = cells[index * 3 : index * 3 + 3]
        reason, failing_symbol = _original_reason(graph)
        if reason != old_reasons.get(date) or (reason is None) != (
            date in old_admitted
        ):
            raise ValueError(
                "diagnostic census differs from original short circuit"
            )
        days.append(
            TrainingWeightSourceDayV1(
                date,
                units.get(_day_ns(date)),
                reason is None,
                reason,
                failing_symbol,
                tuple(c.artifact_id for c in graph),
            )
        )
    return TrainingWeightSourceDiagnosticsV1(
        protocol_id,
        (
            TrainingWeightDiagnosticEvidenceKind.FIXTURE
            if plan.evidence_kind is WeightEvidenceKind.FIXTURE
            else TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC
        ),
        plan.artifact_id,
        plan.parent_source.artifact_id,
        plan.parent_source.dataset_version_id,
        ownership.artifact_id,
        period,
        cells,
        tuple(days),
    )


def replay_verified_training_weight_source_diagnostics(
    report: TrainingWeightSourceDiagnosticsV1,
    plan: TrainingWeightSourcePlanV1,
    verified: _VerifiedSource,
    ownership: TrainingOwnershipV1,
) -> TrainingWeightSourceDiagnosticsV1:
    """Recompute against process-local source, never open or authorize files."""
    if type(report) is not TrainingWeightSourceDiagnosticsV1:
        raise TypeError("diagnostic replay requires its typed report")
    actual = diagnose_verified_training_weight_source(
        plan, verified, ownership, protocol_id=report.protocol_id
    )
    if actual != report:
        raise ValueError(
            "source diagnostic report differs from exact source replay"
        )
    return actual

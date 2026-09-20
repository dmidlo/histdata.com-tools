"""Closed raw-parent → known-degradation ordinal bridge for weighting.

This is an additive research workflow, not a legacy production publication.
The complete original observed month determines day ownership. The small
degraded catalog is a new observed subset, never relabelled as its parent.
Every invocation rechecks mutable source bytes; process-local verified rows
are reused within that invocation, never accepted as persistent receipts.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import ClassVar

import polars as pl

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file

from .training_contracts import (
    DAY_NS,
    TrainingContract,
    TrainingEvidenceUnitV1,
    TrainingOwnershipV1,
    TrainingSourceV1,
    training_json,
    training_load,
)
from .training_lineage import (
    _ObservedRow,
    _ownership,
    _VerifiedSource,
    verify_training_source,
)
from .training_weight_calibration import WEIGHT_SYMBOLS, _date, _number
from .training_weight_contracts import (
    _array,
    _object,
    _text,
    _preregistered_input_hashes,
    read_training_weight_preregistration,
)
from .training_weight_approval import (
    TrainingWeightExecutionApprovalV1,
    require_training_weight_approval,
)

SECOND_NS = 1_000_000_000
CORE_START_NS = 8 * 3600 * SECOND_NS
CORE_END_NS = CORE_START_NS + 600 * SECOND_NS
MAX_AGE_NS = 60 * SECOND_NS
MAX_SUPPORT_ROWS = 4096
MAX_SUBSET_ROWS = 70_000


class WeightEvidenceKind(str, Enum):
    PREREGISTERED = "preregistered_source_replay_not_empirical_qualification"
    FIXTURE = "synthetic_contract_fixture_not_empirical_evidence"


class WeightDayRefusal(str, Enum):
    MISSING_BOUNDARY = "missing_boundary_anchor_within_fixed_support"
    SUPPORT_LIMIT = "source_support_exceeds_frozen_row_budget"
    INSUFFICIENT_ROWS = "fewer_than_64_core_parent_rows"
    INVALID_QUOTES = "nonpositive_nonfinite_or_crossed_source_quotes"
    STALE_PROBE = "missing_or_stale_fixed_probe"


def _day_ns(day: str) -> int:
    _date(day)
    return (
        int(
            datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()
        )
        * SECOND_NS
    )


@dataclass(frozen=True, slots=True)
class TrainingWeightSourcePlanV1(TrainingContract):
    KIND: ClassVar[str] = "weight-source-plan"
    parent_source: TrainingSourceV1
    role: str
    evidence_kind: WeightEvidenceKind

    def _validate(self) -> None:
        if self.role not in ("calibration", "application"):
            raise ValueError("unsupported weighting source role")
        source = self.parent_source
        if (
            source.product_manifest_paths
            or source.context_artifact_paths
            or source.derived_artifact_paths
        ):
            raise ValueError(
                "weight protocol forbids external runtime dependencies"
            )
        raw = training_load(source.catalog_json)
        catalog = DatasetCatalog.from_dict(raw)
        if training_json(catalog.to_dict()) != training_json(raw):
            raise ValueError("weight catalog contains unknown/coercive fields")
        versions = tuple(
            v
            for v in catalog.versions
            if v.dataset_version_id == source.dataset_version_id
        )
        if len(versions) != 1:
            raise ValueError("weight source lacks exact parent version")
        version = versions[0]
        if (
            version.origin is not DatasetOrigin.OBSERVED
            or tuple(sorted(p.symbol for p in version.partitions))
            != WEIGHT_SYMBOLS
        ):
            raise ValueError(
                "weight source requires complete observed triangle"
            )
        periods = {p.period for p in version.partitions}
        allowed = (
            {"201001", "201101"} if self.role == "calibration" else {"201102"}
        )
        if len(periods) != 1 or not periods <= allowed:
            raise ValueError("source period is outside preregistered role")
        if any(
            p.source_provider_id != "histdata.com" for p in version.partitions
        ):
            raise ValueError(
                "weight subset bridge requires the closed IPC adapter"
            )
        if self.evidence_kind is WeightEvidenceKind.FIXTURE:
            known = _preregistered_input_hashes()
            if any(p.artifact.sha256 in known for p in version.partitions):
                raise ValueError(
                    "fixture cannot relabel a known preregistered input"
                )
            if version.dataset_id != "training-weight-contract-fixture":
                raise ValueError(
                    "fixture mode requires distinctly named fixture dataset"
                )
            return
        declared = tuple(
            _object(p)
            for p in _array(
                read_training_weight_preregistration().to_dict()[
                    "source_partitions"
                ]
            )
        )
        for partition in version.partitions:
            matches = [
                p
                for p in declared
                if p["symbol"] == partition.symbol
                and p["period"] == partition.period
            ]
            if len(matches) != 1:
                raise ValueError("parent partition is outside source allowlist")
            expected = matches[0]
            if (
                partition.artifact.sha256 != expected["sha256"]
                or partition.artifact.size_bytes != expected["size_bytes"]
                or partition.row_count != expected["row_count"]
            ):
                raise ValueError(
                    "parent catalog differs from preregistered source bytes"
                )

    @property
    def period(self) -> str:
        catalog = DatasetCatalog.from_json(self.parent_source.catalog_json)
        return _text(
            next(
                v
                for v in catalog.versions
                if v.dataset_version_id == self.parent_source.dataset_version_id
            )
            .partitions[0]
            .period
        )

    @property
    def dates(self) -> tuple[str, ...]:
        period = self.period
        return tuple(
            d
            for d in read_training_weight_preregistration().scheduled_dates(
                self.role
            )
            if d.replace("-", "").startswith(period)
        )


@dataclass(frozen=True, slots=True)
class TrainingWeightOrdinalV1(TrainingContract):
    KIND: ClassVar[str] = "weight-ordinal"
    parent_row_id: int
    subset_row_id: int
    event_time_ns: int
    bid: float
    ask: float

    def _validate(self) -> None:
        if (
            not 0 < self.parent_row_id <= 2_000_000
            or not 0 < self.subset_row_id <= MAX_SUBSET_ROWS
            or not 0 <= self.event_time_ns < 2**63
        ):
            raise ValueError("ordinal bridge coordinate exceeds source bounds")
        _number(self.bid, positive=True)
        _number(self.ask, positive=True)
        if self.ask < self.bid:
            raise ValueError("ordinal bridge cannot normalize crossed quotes")


@dataclass(frozen=True, slots=True)
class TrainingWeightSymbolBridgeV1(TrainingContract):
    KIND: ClassVar[str] = "weight-symbol-bridge"
    symbol: str
    parent_series_id: str
    subset_series_id: str
    period: str
    ordinals: tuple[TrainingWeightOrdinalV1, ...]

    def _validate(self) -> None:
        if (
            self.symbol not in WEIGHT_SYMBOLS
            or self.period not in ("201001", "201101", "201102")
            or not 2 <= len(self.ordinals) <= 1026
        ):
            raise ValueError("invalid bounded symbol ordinal bridge")
        keys = tuple((o.event_time_ns, o.parent_row_id) for o in self.ordinals)
        if keys != tuple(sorted(set(keys))) or len(
            {o.parent_row_id for o in self.ordinals}
        ) != len(keys):
            raise ValueError(
                "parent physical ordinals must be unique and ordered"
            )
        rows = tuple(o.subset_row_id for o in self.ordinals)
        if rows != tuple(range(rows[0], rows[0] + len(rows))):
            raise ValueError(
                "subset ordinals must be contiguous within day/symbol"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightDayBridgeV1(TrainingContract):
    KIND: ClassVar[str] = "weight-day-bridge"
    source_plan: TrainingWeightSourcePlanV1
    utc_date: str
    parent_unit: TrainingEvidenceUnitV1
    subset_source: TrainingSourceV1
    subset_unit: TrainingEvidenceUnitV1
    symbols: tuple[TrainingWeightSymbolBridgeV1, ...]

    def _validate(self) -> None:
        if self.utc_date not in self.source_plan.dates:
            raise ValueError("bridge date is outside frozen source schedule")
        start = _day_ns(self.utc_date)
        for unit, version in (
            (
                self.parent_unit,
                self.source_plan.parent_source.dataset_version_id,
            ),
            (self.subset_unit, self.subset_source.dataset_version_id),
        ):
            if (
                unit.dataset_version_id != version
                or unit.start_ns != start
                or unit.end_ns != start + DAY_NS
                or unit.graph_symbols != WEIGHT_SYMBOLS
                or unit.context_root_ids
            ):
                raise ValueError(
                    "bridge cannot split, merge or reassign source ownership"
                )
        if (
            self.subset_source.dataset_version_id
            == self.source_plan.parent_source.dataset_version_id
        ):
            raise ValueError(
                "degraded subset cannot masquerade as its original parent"
            )
        if tuple(s.symbol for s in self.symbols) != WEIGHT_SYMBOLS:
            raise ValueError("bridge must cover all three graph legs")
        for symbol in self.symbols:
            if symbol.period != self.source_plan.period or any(
                not start <= o.event_time_ns < start + DAY_NS
                for o in symbol.ordinals
            ):
                raise ValueError("bridge source support crosses historical day")


@dataclass(frozen=True, slots=True)
class TrainingWeightRefusedDayV1(TrainingContract):
    KIND: ClassVar[str] = "weight-refused-day"
    utc_date: str
    reason: WeightDayRefusal

    def _validate(self) -> None:
        _date(self.utc_date)


@dataclass(frozen=True, slots=True)
class TrainingWeightDegradation:
    """Process-local result; persistent readers must replay its source plan."""

    source_plan: TrainingWeightSourcePlanV1
    parent_ownership: TrainingOwnershipV1
    subset_source: TrainingSourceV1 | None
    subset_ownership: TrainingOwnershipV1 | None
    bridges: tuple[TrainingWeightDayBridgeV1, ...]
    refusals: tuple[TrainingWeightRefusedDayV1, ...]


def _probe_rows(
    rows: tuple[_ObservedRow, ...], start: int
) -> tuple[_ObservedRow, ...]:
    """One ordered bounded-prior pass; no future quotes or implicit carry."""
    selected: list[_ObservedRow] = []
    cursor = 0
    for offset in range(600):
        probe = start + offset * SECOND_NS
        while cursor < len(rows) and rows[cursor].event_time_ns <= probe:
            cursor += 1
        if not cursor or probe - rows[cursor - 1].event_time_ns > MAX_AGE_NS:
            raise ValueError(WeightDayRefusal.STALE_PROBE.value)
        selected.append(rows[cursor - 1])
    return tuple(selected)


def _selected_days(
    plan: TrainingWeightSourcePlanV1, verified: _VerifiedSource
) -> tuple[
    dict[str, dict[str, tuple[_ObservedRow, ...]]],
    tuple[TrainingWeightRefusedDayV1, ...],
]:
    starts = {_day_ns(d): d for d in plan.dates}
    candidates: dict[str, dict[str, list[_ObservedRow]]] = {
        d: {s: [] for s in WEIGHT_SYMBOLS} for d in plan.dates
    }
    overflow: set[str] = set()
    # A single complete-parent pass, not one full-month scan per member/day.
    for row in verified.observed:
        day_start = row.event_time_ns // DAY_NS * DAY_NS
        day = starts.get(day_start)
        if (
            day is None
            or not day_start + CORE_START_NS - MAX_AGE_NS
            <= row.event_time_ns
            <= day_start + CORE_END_NS + MAX_AGE_NS
        ):
            continue
        target = candidates[day][row.symbol]
        if len(target) == MAX_SUPPORT_ROWS:
            overflow.add(day)
        elif day not in overflow:
            target.append(row)
    admitted: dict[str, dict[str, tuple[_ObservedRow, ...]]] = {}
    refused: list[TrainingWeightRefusedDayV1] = []
    for day, symbols in candidates.items():
        reason: WeightDayRefusal | None = (
            WeightDayRefusal.SUPPORT_LIMIT if day in overflow else None
        )
        retained: dict[str, tuple[_ObservedRow, ...]] = {}
        start = _day_ns(day) + CORE_START_NS
        end = _day_ns(day) + CORE_END_NS
        for symbol, raw in symbols.items():
            if reason is not None:
                break
            left = [i for i, r in enumerate(raw) if r.event_time_ns <= start]
            right = [i for i, r in enumerate(raw) if r.event_time_ns >= end]
            if not left or not right:
                reason = WeightDayRefusal.MISSING_BOUNDARY
                break
            if sum(start <= r.event_time_ns < end for r in raw) < 64:
                reason = WeightDayRefusal.INSUFFICIENT_ROWS
                break
            support = tuple(raw[left[-1] : right[0] + 1])
            if any(
                r.bid <= 0
                or r.ask < r.bid
                or not (float("-inf") < r.bid <= r.ask < float("inf"))
                for r in support
            ):
                reason = WeightDayRefusal.INVALID_QUOTES
                break
            subset = (support[0], *support[1:-1:4], support[-1])
            try:
                _probe_rows(support, start)
                _probe_rows(subset, start)
            except ValueError:
                reason = WeightDayRefusal.STALE_PROBE
                break
            retained[symbol] = subset
        if reason is None:
            admitted[day] = retained
        else:
            refused.append(TrainingWeightRefusedDayV1(day, reason))
    return admitted, tuple(refused)


def _write_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _bridge_records(
    plan: TrainingWeightSourcePlanV1,
    parent_ownership: TrainingOwnershipV1,
    selected: dict[str, dict[str, tuple[_ObservedRow, ...]]],
    subset: _VerifiedSource,
    subset_ownership: TrainingOwnershipV1,
) -> tuple[TrainingWeightDayBridgeV1, ...]:
    parent_units = {u.start_ns: u for u in parent_ownership.units}
    subset_units = {u.start_ns: u for u in subset_ownership.units}
    rows = {
        s: tuple(r for r in subset.observed if r.symbol == s)
        for s in WEIGHT_SYMBOLS
    }
    offsets = {s: 0 for s in WEIGHT_SYMBOLS}
    bridges: list[TrainingWeightDayBridgeV1] = []
    for day, graph in selected.items():
        symbols: list[TrainingWeightSymbolBridgeV1] = []
        for symbol, values in graph.items():
            mapped = rows[symbol][
                offsets[symbol] : offsets[symbol] + len(values)
            ]
            if len(mapped) != len(values) or any(
                (a.event_time_ns, a.bid, a.ask)
                != (b.event_time_ns, b.bid, b.ask)
                or b.vol != 0
                for a, b in zip(values, mapped)
            ):
                raise ValueError(
                    "degraded subset differs from exact source ordinal projection"
                )
            offsets[symbol] += len(values)
            symbols.append(
                TrainingWeightSymbolBridgeV1(
                    symbol,
                    values[0].series_id,
                    mapped[0].series_id,
                    plan.period,
                    tuple(
                        TrainingWeightOrdinalV1(
                            a.row_id, b.row_id, a.event_time_ns, a.bid, a.ask
                        )
                        for a, b in zip(values, mapped)
                    ),
                )
            )
        start = _day_ns(day)
        bridges.append(
            TrainingWeightDayBridgeV1(
                plan,
                day,
                parent_units[start],
                subset.source,
                subset_units[start],
                tuple(symbols),
            )
        )
    if any(offsets[s] != len(rows[s]) for s in WEIGHT_SYMBOLS):
        raise ValueError(
            "subset contains rows outside the complete frozen schedule"
        )
    return tuple(bridges)


def create_training_weight_degradation(
    plan: TrainingWeightSourcePlanV1,
    directory: str | Path,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightDegradation:
    """Verify once and materialize a bounded, fresh monthly observed subset.

    This decodes raw source rows. Operator approval of the separately frozen
    adapter is required before invoking this on the real preregistered inputs.
    No calibration or candidate generation occurs here.
    """
    if type(plan) is not TrainingWeightSourcePlanV1:
        raise TypeError("weight degradation requires a typed source plan")
    if plan.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, "source-window-decoding")
        require_training_weight_approval(
            approval, "degraded-subset-materialization"
        )
    parent = verify_training_source(plan.parent_source)
    ownership = _ownership(parent)
    selected, refused = _selected_days(plan, parent)
    if not selected:
        return TrainingWeightDegradation(
            plan, ownership, None, None, (), refused
        )
    count = sum(
        len(rows) for graph in selected.values() for rows in graph.values()
    )
    if count > MAX_SUBSET_ROWS:
        raise ValueError("complete degraded subset exceeds aggregate row bound")
    root = Path(directory)
    if root.exists():
        raise ValueError("degradation output requires a new directory")
    evidence = root / "ordinal-plan.json"
    _write_exclusive(evidence, plan.to_json().encode("ascii"))
    cache = root / "ASCII" / "T"
    for symbol in WEIGHT_SYMBOLS:
        rows = tuple(r for graph in selected.values() for r in graph[symbol])
        frame = pl.DataFrame(
            {
                "datetime": [r.event_time_ns // 1_000_000 for r in rows],
                "bid": [r.bid for r in rows],
                "ask": [r.ask for r in rows],
                "vol": [0] * len(rows),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        )
        buffer = io.BytesIO()
        frame.write_ipc(buffer)
        _write_exclusive(
            histdata_cache_path(cache, symbol, plan.period), buffer.getvalue()
        )
    adapter = HistDataProviderAdapter()
    descriptor = DatasetDescriptorV1(
        "training-weight-known-degraded-subset",
        "Known-degraded observed subset",
        "Exact selected observed quotes; not an unabridged parent, historical availability or empirical qualification.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        cache,
        descriptor,
        symbols=WEIGHT_SYMBOLS,
        periods=(plan.period,),
        qualification_evidence=(
            artifact_ref_for_file(
                evidence,
                kind="known-degradation-source-plan-not-scientific-qualification",
            ),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    source = TrainingSourceV1(catalog.to_json(), version.dataset_version_id)
    subset = verify_training_source(source)
    subset_ownership = _ownership(subset)
    bridges = _bridge_records(
        plan, ownership, selected, subset, subset_ownership
    )
    return TrainingWeightDegradation(
        plan, ownership, source, subset_ownership, bridges, refused
    )


def replay_training_weight_degradation(
    result: TrainingWeightDegradation,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightDegradation:
    """Replay the whole monthly inventory, including all absence refusals."""
    if type(result) is not TrainingWeightDegradation:
        raise TypeError("degradation replay requires its typed result")
    if result.source_plan.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, "source-window-decoding")
    parent = verify_training_source(result.source_plan.parent_source)
    parent_ownership = _ownership(parent)
    if parent_ownership != result.parent_ownership:
        raise ValueError("raw-parent ownership differs from retained map")
    selected, refusals = _selected_days(result.source_plan, parent)
    if refusals != result.refusals:
        raise ValueError("source absence/refusal inventory differs")
    if not selected:
        if (
            result.subset_source is not None
            or result.subset_ownership is not None
            or result.bridges
        ):
            raise ValueError(
                "refused inventory cannot retain an admitted subset"
            )
        return result
    if result.subset_source is None:
        raise ValueError("admitted inventory lacks verified subset source")
    subset = verify_training_source(result.subset_source)
    subset_ownership = _ownership(subset)
    if (
        subset_ownership != result.subset_ownership
        or _bridge_records(
            result.source_plan,
            parent_ownership,
            selected,
            subset,
            subset_ownership,
        )
        != result.bridges
    ):
        raise ValueError("retained ordinal bridge differs from source replay")
    return result

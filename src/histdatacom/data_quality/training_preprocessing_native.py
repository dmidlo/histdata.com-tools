"""Source-replayed preprocessing evidence; process-local values are not authority.

Mass is descriptive, not calibrated uncertainty: one unit per native law
stratum, equal retained members, equal complete native rows. Projection never
renormalizes missing members or rows. Repeated decision-grid anchors share mass.
These helpers do not grant historical availability to legacy ex-post spines.
"""

from __future__ import annotations

import bisect
import hashlib
from collections import Counter
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import TypeAlias, cast

from .training_contracts import (
    TrainingRootV1,
    TrainingRowV1,
    training_json,
    training_load,
)
from .training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    JoinState,
    TrainingJoinValueV1,
)
from .training_lineage import verify_training_ownership
from .training_preprocessing_contracts import readmit_preprocessing_contract
from .training_provider_policy import require_training_derivation
from .training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalSplitV1,
)
from .training_wide_contracts import (
    MAX_WIDE_CELLS,
    MAX_WIDE_ROWS,
    TrainingWideColumnV1,
    TrainingWidePlanV1,
    WideRowGrain,
)
from .training_wide_views import materialize_training_wide_view

Scalar: TypeAlias = int | float | str | bool | None
MASS_POLICY = "equal-complete-unit-retained-member-native-row.v1"
ROLLING_MASS_POLICY = "equal-training-unit-available-prefix-member-row.v1"
INVENTORY_SCOPE = "complete-declared-native-source-and-retained-member-roster"
MAX_SOURCE_ANCHORS = 32768
MAX_DENOMINATORS = 4096


def _id(kind: str, value: object) -> str:
    return (
        kind
        + ":sha256:"
        + hashlib.sha256(training_json(value).encode("ascii")).hexdigest()
    )


@dataclass(frozen=True, slots=True)
class NativePreprocessingCell:
    column: str
    original_value: Scalar
    state: JoinState | None
    provenance: TrainingJoinValueV1 | None
    definition: TrainingWideColumnV1
    decision_time_ns: int | None
    age_ns: int | None
    reason: str

    @property
    def available_at_ns(self) -> int | None:
        return (
            None if self.provenance is None else self.provenance.available_at_ns
        )

    @property
    def dependency_start_ns(self) -> int | None:
        return (
            None
            if self.provenance is None
            else self.provenance.dependency_start_ns
        )

    @property
    def dependency_end_ns(self) -> int | None:
        return (
            None
            if self.provenance is None
            else self.provenance.dependency_end_ns
        )


@dataclass(frozen=True, slots=True)
class NativePreprocessingRow:
    wide_row_id: str
    spine_id: str
    native_row: TrainingRowV1 | None
    evidence_unit_id: str | None
    partition: TemporalPartition | None
    member_key: str | None
    stratum_id: str | None
    anchor_id: str | None
    weight: Fraction
    cells: tuple[NativePreprocessingCell, ...]


@dataclass(frozen=True, slots=True)
class NativePreprocessingDenominator:
    evidence_unit_id: str
    stratum_id: str
    member_key: str
    complete_row_count: int
    selected_unique_row_count: int
    selected_logical_row_count: int
    member_mass: Fraction
    selected_mass: Fraction
    withheld_mass: Fraction
    status: str


@dataclass(frozen=True, slots=True)
class NativePreprocessingEvidence:
    plan: TrainingWidePlanV1
    split: TrainingTemporalSplitV1
    rows: tuple[NativePreprocessingRow, ...]
    denominators: tuple[NativePreprocessingDenominator, ...]
    roots: tuple[TrainingRootV1, ...]
    source_anchor_ids: tuple[str, ...]
    content_id: str
    mass_policy: str = MASS_POLICY
    inventory_scope: str = INVENTORY_SCOPE


@dataclass(frozen=True, slots=True)
class NativePreprocessingFitSelection:
    rows: tuple[NativePreprocessingRow, ...]
    weights: tuple[Fraction, ...]
    fit_unit_ids: tuple[str, ...]
    excluded_rows: tuple[tuple[str, str], ...]
    selected_mass: Fraction
    withheld_mass: Fraction
    parameter_fit_identity: str
    mass_policy: str
    availability_scope: str = (
        "expost-native-integrity-not-historical-availability"
    )
    historical_availability_verified: bool = False


@dataclass(frozen=True, slots=True)
class _NativeCoordinate:
    unit: str
    stratum: str
    member: str
    anchor: str


def _split(plan: TrainingWidePlanV1, split: TrainingTemporalSplitV1) -> None:
    ownership = plan.tiles[0].join_plan.spine.ownership
    if split.ownership_id != ownership.artifact_id or tuple(
        a.evidence_unit_id for a in split.assignments
    ) != tuple(u.artifact_id for u in ownership.units):
        raise ValueError(
            "preprocessing split must cover complete ownership in order"
        )
    rank = {p: i for i, p in enumerate(TemporalPartition)}
    values = tuple(rank[a.partition] for a in split.assignments)
    if values != tuple(sorted(values)):
        raise ValueError("preprocessing split is not chronological")


def _anchor(symbol: str, time: int, bid: float, ask: float) -> str:
    # Independent of dataset names/paths, so copying a calibration source cannot
    # disguise reuse of an original observed quote as external evidence.
    return _id("preprocessing-anchor", [symbol.upper(), time, bid, ask])


def _member(run_id: str, member_id: str) -> str:
    """A repeated display label in another actual run is not the same path."""
    return _id("preprocessing-native-member", [run_id, member_id])


def inspect_training_preprocessing_native(
    plan: TrainingWidePlanV1, split: TrainingTemporalSplitV1
) -> NativePreprocessingEvidence:
    """Readmit and replay every source, including unused join bindings.

    No input arrays, weights, cached verification receipts or availability
    booleans are admitted. Missing grid anchors have no fabricated ownership.
    At most 32,768 distinct observed anchors are retained for exact external
    overlap checks; larger inventories refuse, never truncate. Complete row
    counts stream independently of the at-most-512 selected coordinates.
    """
    if (
        type(plan) is not TrainingWidePlanV1
        or type(split) is not TrainingTemporalSplitV1
    ):
        raise TypeError(
            "preprocessing requires exact native wide plan and split"
        )
    plan = readmit_preprocessing_contract(plan, TrainingWidePlanV1)
    split = readmit_preprocessing_contract(split, TrainingTemporalSplitV1)
    _split(plan, split)
    wide = materialize_training_wide_view(plan)
    if (
        len(wide.manifest.rows) > MAX_WIDE_ROWS
        or len(wide.manifest.rows) * len(plan.columns) > MAX_WIDE_CELLS
    ):
        raise ValueError("preprocessing native row/cell bound exceeded")
    spine = wide.controls[0].plan.spine
    verified = verify_training_ownership(spine.source, spine.ownership)
    require_training_derivation(verified)
    units = spine.ownership.units
    starts = tuple(u.start_ns for u in units)

    def unit_for(time: int) -> str:
        i = bisect.bisect_right(starts, time) - 1
        if i < 0 or not units[i].start_ns <= time < units[i].end_ns:
            raise ValueError("native row escapes complete evidence ownership")
        return str(units[i].artifact_id)

    observed_stratum = _id("preprocessing-stratum", {"origin": "observed"})
    native_rows = {
        (batch.plan.spine.artifact_id, row.artifact_id): row
        for batch in wide.controls
        for row in batch.plan.spine.rows
    }
    wanted = {
        native_rows[(row.spine_id, row.native_row_id)].source_row_key
        for row in wide.manifest.rows
        if row.native_row_id is not None
    }
    coordinates: dict[str, _NativeCoordinate] = {}
    members: dict[tuple[str, str], set[str]] = {
        (u.artifact_id, observed_stratum): {"observed"} for u in units
    }
    inventories: Counter[tuple[str, str, str]] = Counter()
    anchor_ids: set[str] = set()
    for observed in verified.observed:
        unit = unit_for(observed.event_time_ns)
        key = f"{spine.source.dataset_version_id}|{observed.series_id}|{observed.period}|{observed.row_id}"
        anchor = _anchor(
            observed.symbol, observed.event_time_ns, observed.bid, observed.ask
        )
        if anchor not in anchor_ids and len(anchor_ids) >= MAX_SOURCE_ANCHORS:
            raise ValueError("complete source anchor inventory exceeds bound")
        anchor_ids.add(anchor)
        if key in wanted:
            coordinates[key] = _NativeCoordinate(
                unit, observed_stratum, "observed", anchor
            )
        members.setdefault((unit, observed_stratum), set()).add("observed")
        inventories[(unit, observed_stratum, "observed")] += 1
    for product in verified.products:
        manifest = product.manifest
        stratum = _id(
            "preprocessing-stratum",
            {
                "origin": "native_reconstruction",
                "generator_ids": list(manifest.constraints.generator_ids),
                "generator_versions": list(
                    manifest.constraints.generator_versions
                ),
                "generator_config_ids": list(
                    manifest.constraints.generator_config_ids
                ),
                "constraint_set_ids": list(
                    manifest.constraints.constraint_set_ids
                ),
                "delivery_profile_id": getattr(
                    manifest, "delivery_profile_id", None
                ),
                "broker_profile_id": getattr(
                    manifest, "broker_profile_id", None
                ),
            },
        )
        # Native config/constraint and delivery identities remain separate; an
        # observed projection is never pooled with a reconstruction stratum.
        touched = {unit_for(event.event_time_ns) for event in product.events}
        for unit in touched:
            members.setdefault((unit, stratum), set()).update(
                _member(manifest.run_id, member)
                for member in manifest.ensemble.retained_member_ids
            )
        for event in product.events:
            key = f"{manifest.manifest_id}|{event.event_id}"
            unit = unit_for(event.event_time_ns)
            member = _member(manifest.run_id, manifest.ensemble_member_id)
            anchor = _anchor(
                event.symbol, event.event_time_ns, event.bid, event.ask
            )
            if key in wanted:
                coordinates[key] = _NativeCoordinate(
                    unit, stratum, member, anchor
                )
            inventories[(unit, stratum, member)] += 1
    values = {
        (batch.plan.spine.artifact_id, row.spine_row_id, value.column): value
        for batch in wide.groups
        for row in batch.rows
        for value in row.values
    }
    partition = {a.evidence_unit_id: a.partition for a in split.assignments}
    occurrences: Counter[str] = Counter()
    for row in wide.manifest.rows:
        if row.native_row_id is not None:
            counted_row = native_rows[(row.spine_id, row.native_row_id)]
            occurrences[counted_row.source_row_key] += 1
    selected: dict[tuple[str, str, str], set[str]] = {}
    logical: Counter[tuple[str, str, str]] = Counter()
    rows: list[NativePreprocessingRow] = []
    for row in wide.manifest.rows:
        original = (
            None
            if row.native_row_id is None
            else native_rows[(row.spine_id, row.native_row_id)]
        )
        coordinate = (
            None
            if original is None
            else coordinates.get(original.source_row_key)
        )
        if original is not None and (
            coordinate is None or coordinate.unit != original.evidence_unit_id
        ):
            raise ValueError(
                "native preprocessing row lacks exact source inventory coordinate"
            )
        cells: list[NativePreprocessingCell] = []
        for definition in plan.columns:
            name = definition.column.name
            value = (
                None
                if original is None
                else values[(row.spine_id, original.artifact_id, name)]
            )
            scalar = (
                None
                if value is None
                else training_load(value.value_json)["value"]
            )
            if scalar is not None and type(scalar) not in (
                int,
                float,
                str,
                bool,
            ):
                raise ValueError("native preprocessing cell is not scalar")
            cutoff = (
                row.grid_cutoff_ns
                if original is None
                else original.decision_time_ns
            )
            age = (
                None
                if value is None
                or value.source_time_ns is None
                or cutoff is None
                else cutoff - value.source_time_ns
            )
            cells.append(
                NativePreprocessingCell(
                    name,
                    cast(Scalar, scalar),
                    None if value is None else value.state,
                    value,
                    definition,
                    cutoff,
                    age,
                    row.state if value is None else value.reason,
                )
            )
        weight = Fraction(0)
        if coordinate is not None:
            assert original is not None
            group_key = (coordinate.unit, coordinate.stratum, coordinate.member)
            selected.setdefault(group_key, set()).add(original.source_row_key)
            logical[group_key] += 1
            weight = Fraction(
                1,
                len(members[group_key[:2]])
                * inventories[group_key]
                * occurrences[original.source_row_key],
            )
        rows.append(
            NativePreprocessingRow(
                row.artifact_id,
                row.spine_id,
                original,
                None if coordinate is None else coordinate.unit,
                None if coordinate is None else partition[coordinate.unit],
                None if coordinate is None else coordinate.member,
                None if coordinate is None else coordinate.stratum,
                None if coordinate is None else coordinate.anchor,
                weight,
                tuple(cells),
            )
        )
    denominators: list[NativePreprocessingDenominator] = []
    for (unit, stratum), roster in sorted(members.items()):
        for member in sorted(roster):
            if len(denominators) >= MAX_DENOMINATORS:
                raise ValueError(
                    "complete native denominator inventory exceeds bound"
                )
            denominator_key = unit, stratum, member
            complete = inventories[denominator_key]
            count = len(selected.get(denominator_key, ()))
            member_mass = Fraction(1, len(roster))
            retained = (
                member_mass * Fraction(count, complete)
                if complete
                else Fraction(0)
            )
            denominators.append(
                NativePreprocessingDenominator(
                    unit,
                    stratum,
                    member,
                    complete,
                    count,
                    logical[denominator_key],
                    member_mass,
                    retained,
                    member_mass - retained,
                    "retained" if complete else "declared_member_unavailable",
                )
            )
    require_training_derivation(verified)
    content = {
        "plan_id": plan.artifact_id,
        "split_id": split.artifact_id,
        "wide_content": wide.manifest.content_sha256,
        "roots": [r.to_dict() for r in verified.roots],
        "mass_policy": MASS_POLICY,
        "denominators": [
            [
                d.evidence_unit_id,
                d.stratum_id,
                d.member_key,
                d.complete_row_count,
                d.selected_unique_row_count,
                d.selected_logical_row_count,
                str(d.selected_mass),
                str(d.withheld_mass),
            ]
            for d in denominators
        ],
    }
    return NativePreprocessingEvidence(
        plan,
        split,
        tuple(rows),
        tuple(denominators),
        verified.roots,
        tuple(sorted(anchor_ids)),
        _id("preprocessing-native", content),
    )


def assert_training_preprocessing_native_current(
    evidence: NativePreprocessingEvidence,
) -> NativePreprocessingEvidence:
    """Reject even fully resealed process-local evidence after fresh replay."""
    if type(evidence) is not NativePreprocessingEvidence:
        raise TypeError("preprocessing requires actual native evidence")
    current = inspect_training_preprocessing_native(
        evidence.plan, evidence.split
    )
    if current != evidence:
        raise ValueError(
            "preprocessing evidence differs from fresh native replay"
        )
    return current


def select_training_preprocessing_fit(
    evidence: NativePreprocessingEvidence,
    *,
    fit_mode: str,
    fit_unit_ids: tuple[str, ...],
    fold_id: str,
    cutoff_ns: int | None = None,
    rolling_window_ns: int | None = None,
    columns: tuple[str, ...] | None = None,
) -> NativePreprocessingFitSelection:
    """Select complete TRAIN units without accepting caller arrays or weights.

    ``fixed_external`` selects independently supplied calibration evidence; the
    public orchestrator additionally binds/disjoins its source from application.
    Normalized GRID/BAR added-feature fitting is not a historical assertion
    about the quote spine. Its mass is over declared grid-prefix coordinates,
    never over future native quote counts. Ownership/fold remains frozen.
    """
    if fit_mode not in ("train_fit", "rolling_pti", "fixed_external", "none"):
        raise ValueError("unsupported preprocessing fit mode")
    if (
        type(fit_unit_ids) is not tuple
        or any(type(u) is not str for u in fit_unit_ids)
        or tuple(sorted(set(fit_unit_ids))) != fit_unit_ids
    ):
        raise ValueError("fit units must be an exact sorted unique tuple")
    if type(fold_id) is not str or not fold_id or len(fold_id) > 256:
        raise ValueError("preprocessing fit requires a bounded fold identity")
    if cutoff_ns is not None and (
        type(cutoff_ns) is not int or not 0 <= cutoff_ns < 2**63
    ):
        raise ValueError(
            "preprocessing cutoff must be an exact nonnegative clock"
        )
    if rolling_window_ns is not None and (
        type(rolling_window_ns) is not int or not 0 < rolling_window_ns < 2**63
    ):
        raise ValueError("rolling window must be a positive bounded clock")
    if fit_mode == "rolling_pti" and cutoff_ns is None:
        raise ValueError("rolling fit requires an explicit cutoff")
    if fit_mode != "rolling_pti" and rolling_window_ns is not None:
        raise ValueError("only rolling fit admits a rolling window")
    current = assert_training_preprocessing_native_current(evidence)
    names = tuple(c.column.name for c in current.plan.columns)
    if columns is None:
        columns = names
    if (
        type(columns) is not tuple
        or not columns
        or len(set(columns)) != len(columns)
        or any(type(c) is not str or c not in names for c in columns)
    ):
        raise ValueError("fit columns must select unique actual native columns")
    train = {
        a.evidence_unit_id
        for a in current.split.assignments
        if a.partition is TemporalPartition.TRAIN
    }
    if (fit_mode != "none" and not fit_unit_ids) or not set(
        fit_unit_ids
    ) <= train:
        raise ValueError(
            "fit membership must select nonempty whole TRAIN units"
        )
    chosen: list[NativePreprocessingRow] = []
    excluded: list[tuple[str, str]] = []
    normalized = fit_mode in ("rolling_pti", "fixed_external")
    if normalized:
        if current.plan.grain.kind not in (WideRowGrain.GRID, WideRowGrain.BAR):
            raise ValueError(
                "normalized fit requires predeclared GRID/BAR coordinates"
            )
        allowed = (
            JoinFamily.VINTAGE,
            JoinFamily.CALENDAR,
            JoinFamily.FORECAST,
            JoinFamily.POSITIONING,
        )
        if any(
            c.column.name in columns
            and (
                c.origin_mode is not JoinInformationMode.NORMALIZED_AS_OF
                or c.family not in allowed
            )
            for c in current.plan.columns
        ):
            raise ValueError(
                "normalized fit requires clocked added features, not raw quote/broker fields"
            )
    prefix_coordinates: dict[str, set[int]] = {}
    if normalized:
        ownership = current.plan.tiles[0].join_plan.spine.ownership
        for unit in ownership.units:
            if unit.artifact_id not in fit_unit_ids:
                continue
            for clock in current.plan.grain.cutoffs:
                if (
                    unit.start_ns <= clock < unit.end_ns
                    and (cutoff_ns is None or clock <= cutoff_ns)
                    and (
                        rolling_window_ns is None
                        or cutoff_ns is None
                        or clock >= cutoff_ns - rolling_window_ns
                    )
                ):
                    prefix_coordinates.setdefault(unit.artifact_id, set()).add(
                        clock
                    )
    for row in current.rows:
        original = row.native_row
        reason = ""
        if fit_mode == "none":
            reason = "no_fitted_state"
        elif original is None:
            reason = "missing_spine_anchor"
        elif row.evidence_unit_id not in fit_unit_ids:
            reason = "outside_fit_units"
        elif normalized:
            decision = original.decision_time_ns
            if cutoff_ns is not None and decision > cutoff_ns:
                reason = "after_fit_cutoff"
            elif (
                rolling_window_ns is not None
                and cutoff_ns is not None
                and decision < cutoff_ns - rolling_window_ns
            ):
                reason = "outside_rolling_window"
            else:
                # A past quote can legitimately anchor a later grid cutoff,
                # but it cannot transfer that cutoff into its older evidence
                # unit (or TRAIN partition). Keep native ownership intact and
                # refuse this unsupported cross-unit normalized coordinate.
                owned_prefix = prefix_coordinates.get(
                    row.evidence_unit_id or ""
                )
                if owned_prefix is None or decision not in owned_prefix:
                    raise ValueError(
                        "normalized grid coordinate crosses native evidence ownership"
                    )
                for cell in row.cells:
                    if cell.column not in columns:
                        continue
                    if cell.original_value is None:
                        # Keep native null and mass; the kernel records its
                        # unavailable support rather than filling from future.
                        continue
                    p = cell.provenance
                    if (
                        p is None
                        or p.available_at_ns is None
                        or p.available_at_ns > decision
                        or p.source_time_ns is None
                        or p.source_time_ns > decision
                        or p.dependency_end_ns is None
                        or p.dependency_end_ns > decision + 1
                    ):
                        raise ValueError(
                            "normalized fit refuses unknown or future cell dependencies"
                        )
        if reason:
            excluded.append((row.wide_row_id, reason))
        else:
            chosen.append(row)
    strata = {r.stratum_id for r in chosen}
    if len(strata) > 1:
        raise ValueError(
            "fit cannot pool different native scenario or engine configurations"
        )
    policy = MASS_POLICY
    if normalized:
        policy = ROLLING_MASS_POLICY
        # Equal declared unit/grid-coordinate mass, split among the graph's
        # declared symbols. Absent grid anchors retain withheld mass.
        repeats: Counter[tuple[str, int]] = Counter()
        for row in chosen:
            assert (
                row.native_row is not None and row.evidence_unit_id is not None
            )
            repeats[
                (row.evidence_unit_id, row.native_row.decision_time_ns)
            ] += 1
        symbols = current.plan.tiles[0].join_plan.spine.request.symbols
        chosen = [
            replace(
                row,
                weight=Fraction(
                    1,
                    len(prefix_coordinates[row.evidence_unit_id or ""])
                    * len(symbols),
                ),
            )
            for row in chosen
        ]
        if any(n > len(symbols) for n in repeats.values()):
            raise ValueError("normalized grid repeats a decision coordinate")
    selected_mass = sum((r.weight for r in chosen), Fraction(0))
    potential = sum(
        (
            d.member_mass
            for d in current.denominators
            if d.evidence_unit_id in fit_unit_ids
            and (not strata or d.stratum_id in strata)
        ),
        Fraction(0),
    )
    if normalized:
        potential = Fraction(len(prefix_coordinates))
    identity = _id(
        "preprocessing-fit-membership",
        {
            "mode": fit_mode,
            "fold": fold_id,
            "cutoff": cutoff_ns,
            "rolling_window_ns": rolling_window_ns,
            "mass_policy": policy,
            "rows": [
                [
                    r.anchor_id,
                    r.member_key,
                    r.stratum_id,
                    str(r.weight),
                    [
                        [
                            c.column,
                            c.original_value,
                            None if c.state is None else c.state.value,
                            c.available_at_ns,
                            c.dependency_start_ns,
                            c.dependency_end_ns,
                        ]
                        for c in r.cells
                        if c.column in columns
                    ],
                ]
                for r in chosen
            ],
        },
    )
    return NativePreprocessingFitSelection(
        tuple(chosen),
        tuple(r.weight for r in chosen),
        fit_unit_ids,
        tuple(excluded),
        selected_mass,
        potential - selected_mass,
        identity,
        policy,
        (
            "normalized-added-feature-clocks-only-expost-spine"
            if normalized
            else "expost-native-integrity-not-historical-availability"
        ),
    )

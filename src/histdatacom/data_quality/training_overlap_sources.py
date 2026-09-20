"""Fresh complete source/ownership replay and immutable event coordinates."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bars import (
    DerivedBarPolicyV1,
    derive_reconstruction_bars,
)
from histdatacom.synthetic.contracts import SyntheticEventOrigin
from histdatacom.synthetic.triangle_bar_features import (
    TriangleBarPolicyV1,
    synchronized_triangle_tuples,
)

from .training_contracts import (
    TrainingOwnershipV1,
    training_json,
)
from .training_lineage import _VerifiedSource, verify_training_ownership
from .training_overlap_contracts import (
    TrainingOverlapCoordinateV1,
    TrainingOverlapNativeV1,
    TrainingOverlapOccurrenceV1,
    TrainingOverlapPlanV1,
)
from .training_overlap_math import (
    MAX_OVERLAP_EVENTS,
    MAX_OVERLAP_VARIANTS,
    overlap_memberships,
)


def overlap_id(kind: str, value: object) -> str:
    return (
        f"overlap-{kind}:sha256:"
        + hashlib.sha256(training_json(value).encode()).hexdigest()
    )


def window_ids(plan: TrainingOverlapPlanV1) -> tuple[str, ...]:
    return tuple(
        overlap_id(
            "window",
            {
                "geometry": plan.geometry.artifact_id,
                "ownership": plan.ownership.artifact_id,
                "index": index,
                "start_ns": lo,
                "end_ns": hi,
            },
        )
        for index, (lo, hi) in enumerate(plan.geometry.intervals)
    )


def unit_at(ownership: TrainingOwnershipV1, time: int) -> str:
    matches = tuple(
        u.artifact_id for u in ownership.units if u.start_ns <= time < u.end_ns
    )
    if len(matches) != 1:
        raise ValueError(
            "overlap source coordinate requires exactly one original owner"
        )
    return matches[0]


@dataclass(frozen=True, slots=True)
class _OverlapSource:
    """Invocation-local replay result; never a public pass-token/cache."""

    verified: _VerifiedSource
    coordinates: tuple[TrainingOverlapCoordinateV1, ...]
    occurrences: tuple[TrainingOverlapOccurrenceV1, ...]
    variants: tuple[tuple[str, str], ...]


def verify_overlap_source(plan: TrainingOverlapPlanV1) -> _OverlapSource:
    verified = verify_training_ownership(plan.source, plan.ownership)
    # Source authenticity is checked even when geometry/selection is empty.
    count = len(verified.observed) + sum(
        len(p.events) for p in verified.products
    )
    if count > MAX_OVERLAP_EVENTS:
        raise ValueError(
            "complete overlap source/alias event inventory exceeds4096"
        )
    coordinates: dict[str, TrainingOverlapCoordinateV1] = {}
    occurrences = []
    variants = {("observed-baseline", "original-source")}
    intervals = plan.geometry.intervals
    raw_memberships = overlap_memberships(
        tuple(r.event_time_ns for r in verified.observed), intervals
    )
    for row, memberships in zip(verified.observed, raw_memberships):
        key = f"{plan.source.dataset_version_id}|{row.series_id}|{row.period}|{row.row_id}"
        value = row.payload()
        value.pop("vol")
        value["volume_state"] = "unavailable_pending_source_semantics"
        coordinate = TrainingOverlapCoordinateV1(
            key,
            unit_at(plan.ownership, row.event_time_ns),
            row.symbol,
            row.event_time_ns,
            training_json(value),
            memberships,
        )
        if key in coordinates:
            raise ValueError("duplicate immutable observed source coordinate")
        coordinates[key] = coordinate
        occurrences.append(
            TrainingOverlapOccurrenceV1(
                coordinate.coordinate_id,
                "observed-baseline",
                "original-source",
                None,
                None,
                row.event_time_ns,
                row.event_time_ns + 1,
            )
        )
    for product in sorted(
        verified.products, key=lambda p: p.manifest.manifest_id
    ):
        manifest_id = product.manifest.manifest_id
        scenario = overlap_id(
            "native-scenario",
            {
                "run_id": product.manifest.run_id,
                "generator_config_ids": sorted(
                    product.manifest.constraints.generator_config_ids
                ),
                "constraint_set_ids": sorted(
                    product.manifest.constraints.constraint_set_ids
                ),
            },
        )
        variants.add((product.manifest.ensemble_member_id, scenario))
        memberships_by_event = overlap_memberships(
            tuple(e.event_time_ns for e in product.events), intervals
        )
        for event, memberships in zip(product.events, memberships_by_event):
            if (event.run_id, event.ensemble_member_id) != (
                product.manifest.run_id,
                product.manifest.ensemble_member_id,
            ):
                raise ValueError(
                    "native event changes declared run/member variant"
                )
            if event.origin is SyntheticEventOrigin.OBSERVED:
                key = f"{event.source_version_id}|{event.source_series_id}|{event.source_period}|{event.source_row_id}"
                if key not in coordinates:
                    raise ValueError(
                        "native observed alias lacks original coordinate"
                    )
                coordinate = coordinates[key]
                if (coordinate.symbol, coordinate.event_time_ns) != (
                    event.symbol.upper(),
                    event.event_time_ns,
                ):
                    raise ValueError(
                        "native alias changes source ownership coordinate"
                    )
                dependency = (event.event_time_ns, event.event_time_ns + 1)
            else:
                key = f"{manifest_id}|{event.event_id}"
                if key in coordinates:
                    raise ValueError("duplicate generated source coordinate")
                coordinate = TrainingOverlapCoordinateV1(
                    key,
                    unit_at(plan.ownership, event.event_time_ns),
                    event.symbol.upper(),
                    event.event_time_ns,
                    training_json(event.to_dict()),
                    memberships,
                )
                coordinates[key] = coordinate
                dependency = (
                    product.dependency_start_ns,
                    product.dependency_end_ns,
                )
            occurrences.append(
                TrainingOverlapOccurrenceV1(
                    coordinate.coordinate_id,
                    event.ensemble_member_id,
                    scenario,
                    manifest_id,
                    training_json(event.to_dict()),
                    *dependency,
                )
            )
    if len(variants) > MAX_OVERLAP_VARIANTS:
        raise ValueError("native complete member/scenario inventory exceeds32")
    # Do not collapse separate physical duplicate rows. Only exact source aliases
    # are unified, and each alias retains its exact native event/product evidence.
    keys = [(o.coordinate_id, o.member_id, o.scenario_id) for o in occurrences]
    if len(set(keys)) != len(keys):
        raise ValueError("ambiguous repeated coordinate within native variant")
    ordered_coordinates = tuple(
        sorted(
            coordinates.values(),
            key=lambda c: (c.event_time_ns, c.symbol, c.source_row_key),
        )
    )
    if sum(len(c.window_indices) for c in ordered_coordinates) > 4096:
        raise ValueError("complete expanded coordinate membership exceeds4096")
    coordinate_by_id = {c.coordinate_id: c for c in ordered_coordinates}
    if (
        sum(
            len(coordinate_by_id[o.coordinate_id].window_indices)
            for o in occurrences
        )
        > 4096
    ):
        raise ValueError(
            "complete expanded native/observed descendant inventory exceeds4096"
        )
    return _OverlapSource(
        verified,
        ordered_coordinates,
        tuple(sorted(occurrences, key=lambda o: o.artifact_id)),
        tuple(sorted(variants)),
    )


def native_overlap_features(
    plan: TrainingOverlapPlanV1, source: _OverlapSource
) -> tuple[TrainingOverlapNativeV1, ...]:
    """Execute existing native timeframe and asynchronous triangle consumers.

    These are completed-window ex-post analytics. Partial native UTC bars remain
    marked partial, not rebranded as closed/causal bars at interior row clocks.
    """
    if len(plan.geometry.intervals) * len(source.verified.products) > 128:
        raise ValueError("native window/product replay inventory exceeds128")
    triangle_policy = None
    if plan.triangle_policy_json is not None:
        triangle_policy = TriangleBarPolicyV1.from_json(
            plan.triangle_policy_json
        )
        if (
            training_json(triangle_policy.to_dict())
            != plan.triangle_policy_json
        ):
            raise ValueError(
                "native triangle policy has discarded/coerced fields"
            )
        if triangle_policy.known_at_ns > plan.geometry.start_ns:
            raise ValueError(
                "triangle policy was not frozen before the first window"
            )
    ids = window_ids(plan)
    coordinate_by_id = {c.coordinate_id: c for c in source.coordinates}
    output = []
    retained_bytes = 0
    projected = sum(
        len(coordinate_by_id[o.coordinate_id].window_indices)
        for o in source.occurrences
        if o.product_manifest_id is not None
    )
    if projected * max(1, len(plan.native_intervals)) > 4096:
        raise ValueError("projected native timeframe output exceeds4096")
    for index, (lo, hi) in enumerate(plan.geometry.intervals):
        for product in sorted(
            source.verified.products, key=lambda p: p.manifest.manifest_id
        ):
            events = product.events
            run = product.manifest.run_id
            member = product.manifest.ensemble_member_id
            ordered = tuple(
                sorted(
                    events,
                    key=lambda e: (
                        e.symbol,
                        e.event_time_ns,
                        e.event_sequence,
                        e.event_id,
                    ),
                )
            )
            bars = (
                derive_reconstruction_bars(
                    ordered,
                    source_product_manifest_id=product.manifest.manifest_id,
                    run_id=run,
                    ensemble_member_id=member,
                    policy=DerivedBarPolicyV1(
                        intervals=plan.native_intervals, max_bars=4096
                    ),
                    start_ns=lo,
                    end_ns=hi,
                )
                if plan.native_intervals
                else ()
            )
            tuples = (
                synchronized_triangle_tuples(
                    ordered,
                    start_ns=lo,
                    end_ns=hi,
                    scope=ActivitySliceScope.MERGED,
                    policy=triangle_policy,
                )
                if triangle_policy is not None
                else ()
            )
            coordinate_ids = tuple(
                sorted(
                    o.coordinate_id
                    for o in source.occurrences
                    if o.product_manifest_id == product.manifest.manifest_id
                    and index
                    in coordinate_by_id[o.coordinate_id].window_indices
                )
            )
            result = TrainingOverlapNativeV1(
                ids[index],
                product.manifest.manifest_id,
                hi,
                max(hi, product.dependency_end_ns),
                coordinate_ids,
                tuple(training_json(b.to_dict()) for b in bars),
                tuple(training_json(t.to_dict()) for t in tuples),
            )
            retained_bytes += len(result.to_json().encode())
            if retained_bytes > 4 * 1024 * 1024:
                raise ValueError("complete native overlap evidence exceeds4MiB")
            output.append(result)
    return tuple(output)

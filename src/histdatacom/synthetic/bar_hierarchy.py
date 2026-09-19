"""Fail-closed hierarchical projections of qualified UTC derived bars.

Persisted v1 rows are not sufficient statistics for a complete parent row:
means are rounded, transition classification uses unrounded quotes, and the
event SHA hashes a sequential event projection (not child digests). This module
separates the useful bar-only subset from exact event-verified v1 reconstruction.
It does not change the existing event, bar, policy, or identity contracts.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bars import (
    DERIVED_BAR_ARROW_COLUMNS,
    NANOSECONDS_PER_SECOND,
    DerivedBarIntervalV1,
    DerivedBarPolicyV1,
    DerivedBarV1,
    derive_reconstruction_bars,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
)

_CONSTANT_FIELDS = (
    "schema_version",
    "event_schema_version",
    "source_product_manifest_id",
    "policy_id",
    "rounding_digits",
    "run_id",
    "ensemble_member_id",
    "symbol",
    "scope",
    "volume_state",
    "volume",
    "event_schema_augmented",
    "raw_m1_input",
    "centralized_traded_volume_claim",
)
_COUNT_FIELDS = (
    "event_count",
    "observed_event_count",
    "synthetic_event_count",
    "quote_update_count",
    "confidence_support_count",
)
_LINEAGE_FIELDS = (
    "source_version_ids",
    "generator_ids",
    "generator_versions",
    "generator_config_ids",
    "reference_ids",
    "motif_ids",
    "feed_epoch_ids",
    "broker_profile_ids",
    "constraint_set_ids",
)
BAR_HIERARCHY_EVENT_REQUIRED_FIELDS = (
    "bar_id",
    "mean_spread",
    "mean_event_confidence",
    "transition_count",
    "price_change_count",
    "stale_quote_count",
    "stale_quote_rate",
    "event_content_sha256",
)


def bar_hierarchy_field_rules() -> dict[str, str]:
    """Declare an operation for every current serialized ``DerivedBarV1`` field.

    Counts of transitions are intentionally not bar-only sums: a child row
    does not identify its incoming quote, and rounded endpoints cannot repair
    independently reset child transitions. Event-verified reconstruction checks
    the carry and then asserts these counts reconcile additively.
    """
    rules = {name: "require_equal" for name in _CONSTANT_FIELDS}
    rules.update({name: "sum_exact_integer_support" for name in _COUNT_FIELDS})
    rules.update({name: "sorted_bounded_set_union" for name in _LINEAGE_FIELDS})
    rules.update(
        {
            name: "must_recompute_from_events"
            for name in BAR_HIERARCHY_EVENT_REQUIRED_FIELDS
        }
    )
    for prefix in ("bid", "ask", "mid", "spread"):
        for suffix, operation in (
            ("open", "first_child"),
            ("high", "maximum"),
            ("low", "minimum"),
            ("close", "last_child"),
        ):
            rules[f"{prefix}_{suffix}"] = operation
    rules.update(
        {
            "interval_code": "explicit_supported_parent_interval",
            "interval_ns": "explicit_supported_parent_interval",
            "bar_start_ns": "explicit_utc_epoch_aligned_parent_bound",
            "bar_end_ns": "start_plus_parent_duration",
            "first_event_id": "first_child",
            "first_event_time_ns": "first_child",
            "last_event_id": "last_child",
            "last_event_time_ns": "last_child",
            "activity_duration_ns": "last_event_time_minus_first_event_time",
            "tick_intensity_per_second": "recompute_count_over_event_duration",
            "is_partial_start": "reject_partial_children_then_false",
            "is_partial_end": "reject_partial_children_then_false",
        }
    )
    if set(rules) != set(DERIVED_BAR_ARROW_COLUMNS):
        raise ValueError("hierarchical rules do not cover the exact bar schema")
    return {name: rules[name] for name in DERIVED_BAR_ARROW_COLUMNS}


def _partition(
    children: Sequence[DerivedBarV1],
    *,
    interval_code: str,
    bar_start_ns: int,
    policy: DerivedBarPolicyV1,
) -> tuple[DerivedBarIntervalV1, tuple[DerivedBarV1, ...]]:
    parent = DerivedBarIntervalV1(interval_code)
    if isinstance(bar_start_ns, bool) or not isinstance(bar_start_ns, int):
        raise TypeError("parent bar_start_ns must be an integer")
    if bar_start_ns % parent.duration_ns:
        raise ValueError("parent requires UTC Unix-epoch alignment")
    if parent.code not in policy.intervals:
        raise ValueError("parent interval is absent from the supplied policy")
    if not children:
        raise ValueError("empty bins have no hierarchical bar")
    first = children[0]
    if not isinstance(first, DerivedBarV1):
        raise TypeError("hierarchical children must be DerivedBarV1 rows")
    if (
        first.interval_ns > parent.duration_ns
        or parent.duration_ns % first.interval_ns
    ):
        raise ValueError("child interval cannot exactly partition the parent")
    expected_count = parent.duration_ns // first.interval_ns
    if len(children) != expected_count:
        raise ValueError("child gaps or excess support prevent complete parent")
    first_payload = first.to_dict()
    for index, child in enumerate(children):
        if not isinstance(child, DerivedBarV1):
            raise TypeError("hierarchical children must be DerivedBarV1 rows")
        # Revalidate supplied immutable contracts, including their content IDs.
        DerivedBarV1.from_dict(child.to_dict())
        if (
            child.interval_code != first.interval_code
            or child.interval_ns != first.interval_ns
            or child.bar_start_ns != bar_start_ns + index * first.interval_ns
        ):
            raise ValueError(
                "children have gaps, mixed intervals, or broken ordering"
            )
        if child.is_partial_start or child.is_partial_end:
            raise ValueError(
                "partial child support cannot become a full parent"
            )
        if (
            child.policy_id != policy.policy_id
            or child.rounding_digits != policy.rounding_digits
            or child.interval_code not in policy.intervals
            or child.scope not in policy.scopes
        ):
            raise ValueError("child policy differs from the supplied policy")
        payload = child.to_dict()
        if any(
            payload[name] != first_payload[name] for name in _CONSTANT_FIELDS
        ):
            raise ValueError(
                "children have different source, axis, or contract identity"
            )
    return parent, tuple(children)


def aggregate_qualified_bar_projection(
    children: Sequence[DerivedBarV1],
    *,
    interval_code: str,
    bar_start_ns: int,
    policy: DerivedBarPolicyV1,
) -> dict[str, JSONValue]:
    """Return only the exact bar-composable subset, never a complete v1 row.

    Callers must supply qualified source bars, in order, exactly partitioning
    one parent. Absent bins (including closures) are not padded. Query-partial
    children, differing source/policy/axis identities, and gaps are refused.
    A same-interval singleton is the identity base case for the finest 1m bar.

    The eight ``BAR_HIERARCHY_EVENT_REQUIRED_FIELDS`` are absent, not null or
    invented. In particular this result cannot be passed off as ``DerivedBarV1``.
    The subset can be computed without rereading or retaining any event rows.
    """
    rules = bar_hierarchy_field_rules()
    parent, ordered = _partition(
        children,
        interval_code=interval_code,
        bar_start_ns=bar_start_ns,
        policy=policy,
    )
    first, last = ordered[0], ordered[-1]
    result: dict[str, JSONValue] = first.to_dict()
    for name in BAR_HIERARCHY_EVENT_REQUIRED_FIELDS:
        result.pop(name)
    result.update(
        {
            "interval_code": parent.code,
            "interval_ns": parent.duration_ns,
            "bar_start_ns": bar_start_ns,
            "bar_end_ns": bar_start_ns + parent.duration_ns,
            "last_event_id": last.last_event_id,
            "last_event_time_ns": last.last_event_time_ns,
            "is_partial_start": False,
            "is_partial_end": False,
        }
    )
    for name in _COUNT_FIELDS:
        result[name] = sum(cast(int, getattr(child, name)) for child in ordered)
    for prefix in ("bid", "ask", "mid", "spread"):
        result[f"{prefix}_close"] = cast(
            float, getattr(last, f"{prefix}_close")
        )
        result[f"{prefix}_high"] = max(
            cast(float, getattr(child, f"{prefix}_high")) for child in ordered
        )
        result[f"{prefix}_low"] = min(
            cast(float, getattr(child, f"{prefix}_low")) for child in ordered
        )
    duration = last.last_event_time_ns - first.first_event_time_ns
    result["activity_duration_ns"] = duration
    result["tick_intensity_per_second"] = (
        round(
            cast(int, result["event_count"])
            * NANOSECONDS_PER_SECOND
            / duration,
            policy.rounding_digits,
        )
        if duration
        else None
    )
    for name in _LINEAGE_FIELDS:
        lineage = sorted(
            {
                value
                for child in ordered
                for value in cast(tuple[str, ...], getattr(child, name))
            }
        )
        if len(lineage) > policy.max_provenance_values:
            raise ValueError("parent lineage exceeds provenance limit")
        result[name] = list(lineage)
    if set(result) != set(rules).difference(
        BAR_HIERARCHY_EVENT_REQUIRED_FIELDS
    ):
        raise ValueError("hierarchical projection field inventory differs")
    return result


def reconstruct_bar_from_qualified_children(
    children: Sequence[DerivedBarV1],
    *,
    interval_code: str,
    bar_start_ns: int,
    policy: DerivedBarPolicyV1,
    events: Sequence[SyntheticEventV1],
    previous_event: SyntheticEventV1 | None = None,
) -> DerivedBarV1:
    """Complete a parent only with exact, child-verified ordered event support.

    ``events`` must contain exactly the in-parent events for the child symbol
    and scope. If the first child carries a preceding quote, pass that last
    same-scope event as ``previous_event``. It must precede the parent and is
    used solely for carry, never for parent prices, counts, duration, or hash.

    Replay must reproduce *every child field and ID exactly*, including its
    event-content SHA and incoming transition classification. This verifies
    the supplied in-bin support against the qualified children; it is not a
    substitute for verifying the original committed source publication. V1
    does not hash the incoming quote identity, so the caller still owns its
    source provenance. No differing incoming transition result is accepted.

    Only after verification are sequential floating reductions and the event
    SHA recomputed. Safe projected fields and transition count sums must match
    exactly. No tolerance, rounded-mean composition, or digest combination is
    used, and existing v1 bar IDs are preserved.
    """
    projection = aggregate_qualified_bar_projection(
        children,
        interval_code=interval_code,
        bar_start_ns=bar_start_ns,
        policy=policy,
    )
    first = children[0]
    end = cast(int, projection["bar_end_ns"])
    if not events:
        raise ValueError("complete v1 hierarchy requires exact event support")
    if len(events) != sum(child.event_count for child in children):
        raise ValueError("event support count differs from qualified children")
    support: list[SyntheticEventV1] = []
    if previous_event is not None:
        _check_event_axis(previous_event, first)
        if previous_event.event_time_ns >= bar_start_ns:
            raise ValueError("previous event must precede the parent interval")
        support.append(previous_event)
    for event in events:
        _check_event_axis(event, first)
        if not bar_start_ns <= event.event_time_ns < end:
            raise ValueError("event support falls outside the parent interval")
        support.append(event)
    replay = derive_reconstruction_bars(
        support,
        source_product_manifest_id=first.source_product_manifest_id,
        run_id=first.run_id,
        ensemble_member_id=first.ensemble_member_id,
        policy=policy,
    )
    reproduced = tuple(
        bar
        for bar in replay
        if bar.interval_code == first.interval_code
        and bar.scope == first.scope
        and bar_start_ns <= bar.bar_start_ns < end
    )
    if reproduced != tuple(children):
        raise ValueError(
            "event support or boundary carry does not reproduce exact children"
        )
    parents = tuple(
        bar
        for bar in replay
        if bar.interval_code == projection["interval_code"]
        and bar.scope == first.scope
        and bar.bar_start_ns == bar_start_ns
    )
    if len(parents) != 1:
        raise ValueError("verified event support did not produce one parent")
    parent = parents[0]
    payload = parent.to_dict()
    if any(payload[name] != value for name, value in projection.items()):
        raise ValueError("direct and hierarchical safe projections differ")
    for name in ("transition_count", "price_change_count", "stale_quote_count"):
        total = sum(cast(int, getattr(child, name)) for child in children)
        if getattr(parent, name) != total:
            raise ValueError("verified boundary-carry transition counts differ")
    return parent


def _check_event_axis(event: SyntheticEventV1, first: DerivedBarV1) -> None:
    if not isinstance(event, SyntheticEventV1):
        raise TypeError(
            "hierarchy event support requires SyntheticEventV1 rows"
        )
    if (
        event.run_id != first.run_id
        or event.ensemble_member_id != first.ensemble_member_id
        or event.symbol != first.symbol
    ):
        raise ValueError("event support source axis differs from children")
    if (
        first.scope is ActivitySliceScope.OBSERVED
        and event.origin is not SyntheticEventOrigin.OBSERVED
    ) or (
        first.scope is ActivitySliceScope.SYNTHETIC
        and event.origin is not SyntheticEventOrigin.SYNTHETIC
    ):
        raise ValueError("event support origin differs from child scope")


__all__ = [
    "BAR_HIERARCHY_EVENT_REQUIRED_FIELDS",
    "aggregate_qualified_bar_projection",
    "bar_hierarchy_field_rules",
    "reconstruct_bar_from_qualified_children",
]

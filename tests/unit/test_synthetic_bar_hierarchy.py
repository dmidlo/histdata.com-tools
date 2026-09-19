"""Frozen, exact direct-event versus qualified-child hierarchy matrix."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_hierarchy import (
    BAR_HIERARCHY_EVENT_REQUIRED_FIELDS,
    aggregate_qualified_bar_projection,
    bar_hierarchy_field_rules,
    reconstruct_bar_from_qualified_children,
)
from histdatacom.synthetic.bars import (
    DERIVED_BAR_ARROW_COLUMNS,
    STANDARD_DERIVED_BAR_INTERVALS,
    DerivedBarPolicyV1,
    DerivedBarV1,
    derive_reconstruction_bars,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
)
from tests.unit.test_synthetic_contracts import (
    BASE_TIME_NS,
    _generated,
    _observed,
)

MINUTE = STANDARD_DERIVED_BAR_INTERVALS["1m"]
DAY = STANDARD_DERIVED_BAR_INTERVALS["1d"]
START = BASE_TIME_NS // DAY * DAY
SOURCE = "product-manifest:sha256:hierarchy-fixture"


def _derive(
    events: tuple[SyntheticEventV1, ...],
    policy: DerivedBarPolicyV1,
    *,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> tuple[DerivedBarV1, ...]:
    return derive_reconstruction_bars(
        events,
        source_product_manifest_id=SOURCE,
        run_id=events[0].run_id,
        ensemble_member_id=events[0].ensemble_member_id,
        policy=policy,
        start_ns=start_ns,
        end_ns=end_ns,
    )


def _events(
    minutes: int = 5, *, start: int = START
) -> tuple[SyntheticEventV1, ...]:
    rows = []
    for minute in range(minutes):
        bid = 1.1 + (minute // 2 % 7) / 10_000
        left = _observed(
            2 * minute + 1, event_time_ns=start + minute * MINUTE + 1, bid=bid
        )
        right = _observed(
            2 * minute + 2,
            event_time_ns=start + minute * MINUTE + 30_000_000_000,
            bid=bid,
        )
        generated = replace(
            _generated(left, right),
            event_time_ns=left.event_time_ns,
            event_sequence=1,
            bid=bid + 0.0000000000001,
            ask=bid + 0.00012 + (minute % 3) / 100_000,
            confidence=0.543210987654 + (minute % 5) / 100,
            event_id="",
        )
        rows.extend((left, generated, right))
    return tuple(rows)


@pytest.fixture(scope="module")
def matrix() -> tuple[
    tuple[SyntheticEventV1, ...],
    DerivedBarPolicyV1,
    tuple[DerivedBarV1, ...],
]:
    events = _events(1440)
    policy = DerivedBarPolicyV1(scopes=tuple(ActivitySliceScope))
    return events, policy, _derive(events, policy)


def test_all_serialized_fields_have_explicit_hierarchy_rules() -> None:
    rules = bar_hierarchy_field_rules()
    assert tuple(rules) == DERIVED_BAR_ARROW_COLUMNS
    assert {
        name
        for name, rule in rules.items()
        if rule == "must_recompute_from_events"
    } == set(BAR_HIERARCHY_EVENT_REQUIRED_FIELDS)
    assert (
        rules["activity_duration_ns"]
        == "last_event_time_minus_first_event_time"
    )
    assert rules["confidence_support_count"] == "sum_exact_integer_support"


@pytest.mark.parametrize("interval", tuple(STANDARD_DERIVED_BAR_INTERVALS))
@pytest.mark.parametrize("scope", tuple(ActivitySliceScope))
def test_seven_interval_three_scope_exact_equivalence_matrix(
    matrix: tuple[
        tuple[SyntheticEventV1, ...],
        DerivedBarPolicyV1,
        tuple[DerivedBarV1, ...],
    ],
    interval: str,
    scope: ActivitySliceScope,
) -> None:
    """Every v1 field and ID matches exactly; 1m is the identity base case."""
    events, policy, direct = matrix
    end = START + STANDARD_DERIVED_BAR_INTERVALS[interval]
    children = tuple(
        bar
        for bar in direct
        if bar.interval_code == "1m"
        and bar.scope is scope
        and bar.bar_start_ns < end
    )
    expected = next(
        bar
        for bar in direct
        if bar.interval_code == interval
        and bar.scope is scope
        and bar.bar_start_ns == START
    )
    support = tuple(
        event
        for event in events
        if event.event_time_ns < end
        and (
            scope is ActivitySliceScope.MERGED
            or event.origin.value == scope.value
        )
    )
    projected = aggregate_qualified_bar_projection(
        children, interval_code=interval, bar_start_ns=START, policy=policy
    )
    assert set(projected) == set(DERIVED_BAR_ARROW_COLUMNS) - set(
        BAR_HIERARCHY_EVENT_REQUIRED_FIELDS
    )
    assert projected == {name: expected.to_dict()[name] for name in projected}
    actual = reconstruct_bar_from_qualified_children(
        children,
        interval_code=interval,
        bar_start_ns=START,
        policy=policy,
        events=support,
    )
    assert actual == expected
    assert actual.to_json() == expected.to_json()
    if interval == "1m":
        assert len(children) == 1
        assert actual.bar_id == children[0].bar_id


def test_reference_three_child_ohlc_count_and_supported_mean() -> None:
    policy = DerivedBarPolicyV1(intervals=("5m", "15m"))
    rows = []
    for child, (count, spread) in enumerate(
        ((3, 0.0001), (7, 0.0002), (10, 0.00036))
    ):
        for position in range(count):
            bid = 1.12
            if child == 0 and position == 0:
                bid = 1.10
            elif child == 0 and position == 1:
                bid = 1.16
            elif child == 1 and position == 1:
                bid = 1.09
            elif child == 2 and position == count - 1:
                bid = 1.11
            event = _observed(
                len(rows) + 1,
                event_time_ns=START + child * 5 * MINUTE + position + 1,
                bid=bid,
            )
            rows.append(replace(event, ask=bid + spread, event_id=""))
    events = tuple(rows)
    bars = _derive(events, policy)
    children = tuple(bar for bar in bars if bar.interval_code == "5m")
    parent = reconstruct_bar_from_qualified_children(
        children,
        interval_code="15m",
        bar_start_ns=START,
        policy=policy,
        events=events,
    )
    assert (
        parent.bid_open,
        parent.bid_high,
        parent.bid_low,
        parent.bid_close,
        parent.event_count,
        parent.mean_spread,
    ) == (1.10, 1.16, 1.09, 1.11, 20, 0.000265)


def test_rounded_child_means_are_not_sufficient_statistics() -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"), rounding_digits=1)
    rows = []
    for minute, spread in enumerate((0.14, 0.14, 0.04, 0.04, 0.04)):
        base = _observed(
            minute + 1, event_time_ns=START + minute * MINUTE + 1, bid=1.1
        )
        right = _observed(
            minute + 100, event_time_ns=base.event_time_ns + 2, bid=1.1
        )
        rows.append(
            replace(
                _generated(base, right),
                bid=1.1,
                ask=1.1 + spread,
                confidence=spread,
                event_id="",
            )
        )
    events = tuple(rows)
    bars = _derive(events, policy)
    children = tuple(bar for bar in bars if bar.interval_code == "1m")
    parent = reconstruct_bar_from_qualified_children(
        children,
        interval_code="5m",
        bar_start_ns=START,
        policy=policy,
        events=events,
    )
    assert parent.mean_spread == 0.1
    assert parent.mean_event_confidence == 0.1
    assert (
        round(
            sum(child.mean_spread * child.event_count for child in children)
            / parent.event_count,
            1,
        )
        == 0.0
    )
    projection = aggregate_qualified_bar_projection(
        children, interval_code="5m", bar_start_ns=START, policy=policy
    )
    assert "mean_spread" not in projection
    assert "mean_event_confidence" not in projection
    assert "event_content_sha256" not in projection
    assert "bar_id" not in projection


def test_incoming_quote_carry_is_verified_and_not_counted_as_support() -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"))
    events = _events()
    preceding = replace(events[0], event_time_ns=START - 3 * DAY, event_id="")
    bars = _derive((preceding, *events), policy)
    children = tuple(
        bar
        for bar in bars
        if bar.interval_code == "1m" and bar.bar_start_ns >= START
    )
    direct = next(
        bar
        for bar in bars
        if bar.interval_code == "5m" and bar.bar_start_ns == START
    )
    actual = reconstruct_bar_from_qualified_children(
        children,
        interval_code="5m",
        bar_start_ns=START,
        policy=policy,
        events=events,
        previous_event=preceding,
    )
    assert actual == direct
    assert actual.transition_count == actual.event_count
    assert actual.stale_quote_count == sum(
        child.stale_quote_count for child in children
    )
    assert actual.event_count == len(events)
    assert actual.first_event_id == events[0].event_id
    no_carry = next(
        bar for bar in _derive(events, policy) if bar.interval_code == "5m"
    )
    assert actual.event_content_sha256 == no_carry.event_content_sha256
    for previous in (None, replace(preceding, bid=1.0, event_id="")):
        with pytest.raises(ValueError, match="boundary carry"):
            reconstruct_bar_from_qualified_children(
                children,
                interval_code="5m",
                bar_start_ns=START,
                policy=policy,
                events=events,
                previous_event=previous,
            )


def test_rounded_quote_endpoints_cannot_repair_transition_carry() -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"), rounding_digits=12)
    events = tuple(
        replace(
            _observed(
                i + 1,
                event_time_ns=START + i * MINUTE + 1,
                bid=1.10000000000001 + i / 100_000_000_000_000,
            ),
            ask=1.2,
            event_id="",
        )
        for i in range(5)
    )
    bars = _derive(events, policy)
    children = tuple(bar for bar in bars if bar.interval_code == "1m")
    assert len({child.bid_close for child in children}) == 1
    parent = reconstruct_bar_from_qualified_children(
        children,
        interval_code="5m",
        bar_start_ns=START,
        policy=policy,
        events=events,
    )
    assert parent.price_change_count == 4
    assert parent.stale_quote_count == 0


@pytest.mark.parametrize(
    "damage",
    (
        "reverse",
        "duplicate",
        "gap",
        "partial_start",
        "partial_end",
        "source",
        "symbol",
        "member",
        "policy",
    ),
)
def test_broken_partitions_and_identity_fail_closed(damage: str) -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"))
    children = [
        bar for bar in _derive(_events(), policy) if bar.interval_code == "1m"
    ]
    if damage == "reverse":
        children.reverse()
    elif damage == "duplicate":
        children[1] = children[0]
    elif damage == "gap":
        children.pop(2)
    elif damage.startswith("partial_"):
        children[2] = replace(
            children[2], **{f"is_{damage}": True, "bar_id": ""}
        )
    else:
        field = {
            "source": "source_product_manifest_id",
            "symbol": "symbol",
            "member": "ensemble_member_id",
            "policy": "policy_id",
        }[damage]
        children[2] = replace(children[2], **{field: "other", "bar_id": ""})
    with pytest.raises(ValueError):
        aggregate_qualified_bar_projection(
            children, interval_code="5m", bar_start_ns=START, policy=policy
        )


@pytest.mark.parametrize(
    "damage",
    (
        "empty",
        "missing",
        "reversed",
        "changed",
        "extra",
        "outside",
        "wrong_origin",
        "wrong_axis",
    ),
)
def test_event_support_requires_exact_child_replay(damage: str) -> None:
    policy = DerivedBarPolicyV1(
        intervals=("1m", "5m"), scopes=(ActivitySliceScope.OBSERVED,)
    )
    events = tuple(
        event
        for event in _events()
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    children = tuple(
        bar for bar in _derive(events, policy) if bar.interval_code == "1m"
    )
    support = list(events)
    if damage == "empty":
        support.clear()
    elif damage == "missing":
        support.pop(1)
    elif damage == "reversed":
        support.reverse()
    elif damage == "changed":
        support[1] = replace(
            support[1], bid=support[1].bid - 0.00001, event_id=""
        )
    elif damage == "extra":
        support.insert(1, replace(support[0], event_sequence=1, event_id=""))
    elif damage == "outside":
        support.append(
            replace(support[-1], event_time_ns=START + 5 * MINUTE, event_id="")
        )
    elif damage == "wrong_origin":
        support[1] = _generated(support[0], support[1])
    elif damage == "wrong_axis":
        support[1] = replace(support[1], symbol="gbpusd", event_id="")
    with pytest.raises(ValueError):
        reconstruct_bar_from_qualified_children(
            children,
            interval_code="5m",
            bar_start_ns=START,
            policy=policy,
            events=tuple(support),
        )


def test_independently_reset_children_cannot_claim_continuous_parent() -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"))
    events = _events()
    children = tuple(
        next(
            bar
            for bar in _derive(
                tuple(
                    event
                    for event in events
                    if START + minute * MINUTE
                    <= event.event_time_ns
                    < START + (minute + 1) * MINUTE
                ),
                policy,
            )
            if bar.interval_code == "1m"
        )
        for minute in range(5)
    )
    assert all(
        child.transition_count == child.event_count - 1 for child in children
    )
    # The scientific subset is still exact; complete v1 transition claims are not.
    aggregate_qualified_bar_projection(
        children, interval_code="5m", bar_start_ns=START, policy=policy
    )
    with pytest.raises(ValueError, match="boundary carry"):
        reconstruct_bar_from_qualified_children(
            children,
            interval_code="5m",
            bar_start_ns=START,
            policy=policy,
            events=events,
        )


def test_unrounded_event_content_is_verified_even_when_summary_matches() -> (
    None
):
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"))
    events = _events()
    children = tuple(
        bar for bar in _derive(events, policy) if bar.interval_code == "1m"
    )
    changed = list(events)
    changed[1] = replace(
        changed[1], bid=changed[1].bid + 0.000000000000001, event_id=""
    )
    altered_child = next(
        bar
        for bar in _derive(tuple(changed), policy)
        if bar.interval_code == "1m"
    )
    assert {
        name: value
        for name, value in children[0].to_dict().items()
        if name not in {"event_content_sha256", "bar_id"}
    } == {
        name: value
        for name, value in altered_child.to_dict().items()
        if name not in {"event_content_sha256", "bar_id"}
    }
    assert (
        altered_child.event_content_sha256 != children[0].event_content_sha256
    )
    with pytest.raises(ValueError, match="exact children"):
        reconstruct_bar_from_qualified_children(
            children,
            interval_code="5m",
            bar_start_ns=START,
            policy=policy,
            events=tuple(changed),
        )


def test_empty_closure_and_partial_query_support_are_not_promoted() -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"))
    events = tuple(
        event
        for event in _events()
        if not START + 2 * MINUTE <= event.event_time_ns < START + 3 * MINUTE
    )
    children = tuple(
        bar for bar in _derive(events, policy) if bar.interval_code == "1m"
    )
    assert len(children) == 4
    for incomplete in ((), children):
        with pytest.raises(ValueError, match="empty bins|gaps"):
            aggregate_qualified_bar_projection(
                incomplete,
                interval_code="5m",
                bar_start_ns=START,
                policy=policy,
            )
    partial = tuple(
        bar
        for bar in _derive(
            _events(), policy, start_ns=START + 1, end_ns=START + 5 * MINUTE - 1
        )
        if bar.interval_code == "1m"
    )
    assert len(partial) == 5
    with pytest.raises(ValueError, match="partial"):
        aggregate_qualified_bar_projection(
            partial, interval_code="5m", bar_start_ns=START, policy=policy
        )


def test_alignment_policy_and_provenance_boundaries() -> None:
    policy = DerivedBarPolicyV1(intervals=("1m", "5m"), max_provenance_values=1)
    events = tuple(
        replace(
            _observed(i + 1, event_time_ns=START + i * MINUTE + 1),
            source_version_id=f"source-{i}",
            event_id="",
        )
        for i in range(5)
    )
    # Derive separately so every child fits the lineage cap while the union does not.
    children = tuple(
        next(
            bar
            for bar in _derive((event,), policy)
            if bar.interval_code == "1m"
        )
        for event in events
    )
    with pytest.raises(ValueError, match="lineage exceeds"):
        aggregate_qualified_bar_projection(
            children, interval_code="5m", bar_start_ns=START, policy=policy
        )
    normal_policy = DerivedBarPolicyV1(intervals=("1m", "5m"))
    normal_children = tuple(
        bar
        for bar in _derive(_events(), normal_policy)
        if bar.interval_code == "1m"
    )
    for interval, start in (
        ("1w", START),
        ("session", START),
        ("5m", START + 1),
        ("5m", True),
    ):
        with pytest.raises((ValueError, TypeError)):
            aggregate_qualified_bar_projection(
                normal_children,
                interval_code=interval,
                bar_start_ns=start,
                policy=normal_policy,
            )
    with pytest.raises(ValueError, match="absent"):
        aggregate_qualified_bar_projection(
            normal_children,
            interval_code="1h",
            bar_start_ns=START,
            policy=normal_policy,
        )

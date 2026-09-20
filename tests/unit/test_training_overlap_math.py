"""Independent integer-grid and exact-mass oracles; synthetic inputs only."""

from dataclasses import replace
from fractions import Fraction
from itertools import permutations, product

import polars as pl
import pytest

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingSourceV1,
    training_json,
    training_load,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapGeometryV1,
    TrainingOverlapPlanV1,
    TrainingOverlapSelectionV1,
)
from histdatacom.data_quality.training_overlap_math import (
    overlap_intervals,
    overlap_mass,
    overlap_memberships,
)
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
    overlap_unit_mass,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import BarFeaturePolicyV1
from histdatacom.synthetic.contracts import SyntheticEventV1
from histdatacom.synthetic.cross_currency import CrossCurrencyJoinPolicy
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_bar_features import (
    TriangleBarPolicyV1,
    TriangleQuoteTupleV1,
    synchronized_triangle_tuples,
)
from tests.fixtures.training_overlap_v1 import BASE, overlap_fixture

SECOND = 1_000_000_000
SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")


def _enumerated_windows(start, end, width, stride):
    # Enumerate all possible starts and test the declared admission predicates;
    # deliberately do not reproduce a floor/count implementation formula.
    return tuple(
        (candidate, candidate + width)
        for candidate in range(start, end)
        if (candidate - start) % stride == 0 and candidate + width <= end
    )


def _row_masses(ledger):
    return {
        (item.window_id, item.member_id, item.scenario_id): item.per_row
        for item in ledger
    }


def test_reference_geometry_has_ten_windows_not_ten_independent_units():
    windows = overlap_intervals(0, 3600, 900, 300)
    assert windows == tuple((300 * i, 900 + 300 * i) for i in range(10))
    times = (0, 299, 300, 599, 600, 899, 900, 3599, 3600)
    memberships = overlap_memberships(times, windows)
    by_time = dict(zip(times, memberships))
    assert max(map(len, memberships)) == 3
    assert by_time[0] == (0,)
    assert by_time[899] == (0, 1, 2)
    assert by_time[900] == (1, 2, 3)
    assert by_time[3599] == (9,)
    assert by_time[3600] == ()


def test_exhaustive_small_integer_grids_match_set_membership_oracle():
    for start, span, width in product((0, 3, 11), range(1, 13), range(1, 10)):
        for stride in range(1, width + 1):
            end = start + span
            expected = _enumerated_windows(start, end, width, stride)
            actual = overlap_intervals(start, end, width, stride)
            assert actual == expected, (start, span, width, stride)
            times = tuple(range(max(0, start - 1), end + 2))
            windows_as_sets = tuple(set(range(lo, hi)) for lo, hi in expected)
            membership = overlap_memberships(times, actual)
            assert membership == tuple(
                tuple(
                    i for i, points in enumerate(windows_as_sets) if t in points
                )
                for t in times
            )
            assert len(membership) == len(times)
            for left in range(len(expected)):
                for right in range(left, len(expected)):
                    common = {
                        t
                        for t, owned in zip(times, membership)
                        if left in owned and right in owned
                    }
                    assert (
                        common == windows_as_sets[left] & windows_as_sets[right]
                    )
            maximum = max(map(len, membership), default=0)
            assert maximum <= (width + stride - 1) // stride


def test_short_source_and_nondivisible_tail_keep_uncovered_coordinates():
    assert overlap_intervals(7, 10, 4, 2) == ()
    assert overlap_memberships((7, 8, 9), ()) == ((), (), ())
    windows = overlap_intervals(7, 18, 5, 3)
    assert windows == ((7, 12), (10, 15), (13, 18))
    # No extra non-grid right-anchored window is silently appended.
    windows = overlap_intervals(7, 19, 5, 3)
    assert windows == ((7, 12), (10, 15), (13, 18))
    assert overlap_memberships((17, 18, 18, 19), windows) == ((2,), (), (), ())


def test_alignment_is_relative_to_exact_start_without_float_or_epoch_snap():
    original = overlap_intervals(3, 20, 7, 4)
    assert original == ((3, 10), (7, 14), (11, 18))
    for shift in (1, 17, 2**53 + 1):
        shifted = overlap_intervals(3 + shift, 20 + shift, 7, 4)
        assert shifted == tuple((lo + shift, hi + shift) for lo, hi in original)
        assert overlap_memberships(
            tuple(t + shift for t in (3, 7, 9, 10, 18, 19)), shifted
        ) == overlap_memberships((3, 7, 9, 10, 18, 19), original)


def test_duplicate_source_clocks_remain_separate_coordinates():
    windows = overlap_intervals(0, 8, 4, 2)
    times = (0, 2, 2, 3, 4, 6, 7, 8)
    membership = overlap_memberships(times, windows)
    assert membership == ((0,), (0, 1), (0, 1), (0, 1), (1, 2), (2,), (2,), ())
    # A shared clock does not collapse the distinct physical row ordinals.
    assert len(membership) == 8
    assert sum(map(len, membership)) == 11


def test_native_asynchronous_triangle_legs_share_exact_window_membership():
    times_by_symbol = {
        "EURGBP": (1, 4, 7),
        "EURUSD": (2, 5, 8),
        "GBPUSD": (0, 3, 6),
    }
    events = tuple(
        SyntheticEventV1.observed(
            symbol=symbol,
            event_time_ns=time,
            event_sequence=ordinal,
            bid=1.0,
            ask=1.01,
            run_id="overlap-math-synthetic-run",
            ensemble_member_id="overlap-math-synthetic-member",
            source_version_id="overlap-math-synthetic-source",
            source_series_id="overlap-math-synthetic-series-" + symbol,
            source_period="197001",
            source_row_id=ordinal + 1,
        )
        for symbol, times in times_by_symbol.items()
        for ordinal, time in enumerate(times)
    )
    windows = overlap_intervals(0, 9, 6, 3)
    assert windows == ((0, 6), (3, 9))
    membership = overlap_memberships(
        tuple(event.event_time_ns for event in events), windows
    )
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(
            InformationMode.EX_POST_RECONSTRUCTION,
            scopes=(ActivitySliceScope.OBSERVED,),
        ),
        0,
        join_policy=CrossCurrencyJoinPolicy.NEAREST_PRIOR_BOUNDED_NO_FORWARD_FILL,
        max_quote_age_ns=2,
    )

    def evaluate(interval):
        lo, hi = interval
        return synchronized_triangle_tuples(
            events,
            start_ns=lo,
            end_ns=hi,
            scope=ActivitySliceScope.OBSERVED,
            policy=policy,
        )

    results = {window: evaluate(window) for window in windows}
    assert tuple(p.probe_time_ns for p in results[windows[0]]) == (2, 5)
    assert tuple(p.probe_time_ns for p in results[windows[1]]) == (5, 8)
    # Async clocks do not create per-leg window IDs or duplicate physical rows.
    shared_left = results[windows[0]][1]
    shared_right = results[windows[1]][0]
    assert tuple(e.event_id for e in shared_left.events) == tuple(
        e.event_id for e in shared_right.events
    )
    assert tuple(e.event_time_ns for e in shared_left.events) == (4, 5, 3)
    positions = {event.event_id: i for i, event in enumerate(events)}
    for window_index, window in enumerate(windows):
        for point in results[window]:
            assert point.endpoint_ns == window[1]
            for event in point.events:
                assert window_index in membership[positions[event.event_id]]
                assert window[0] <= event.event_time_ns < window[1]
                assert event.event_time_ns <= point.probe_time_ns
    assert {window: evaluate(window) for window in reversed(windows)} == results


@pytest.mark.parametrize(
    "geometry",
    ((0, 0, 1, 1), (2, 1, 1, 1), (0, 4, 0, 1), (0, 4, 2, 0), (0, 4, 2, 3)),
)
def test_invalid_geometry_is_not_silently_repaired(geometry):
    with pytest.raises((TypeError, ValueError)):
        overlap_intervals(*geometry)


@pytest.mark.parametrize("bad", (True, 1.0, "1", -1, 2**63))
def test_geometry_and_event_clocks_are_exact_nonnegative_int64(bad):
    for index in range(4):
        args = [0, 10, 3, 2]
        args[index] = bad
        with pytest.raises((TypeError, ValueError)):
            overlap_intervals(*args)
    with pytest.raises((TypeError, ValueError)):
        overlap_memberships((bad,), ((0, 3),))


def test_cartesian_window_member_scenario_rows_do_not_multiply_parent_mass():
    windows = ("w0", "w1", "w2")
    variants = tuple(product(("m0", "m1"), ("s0", "s1")))
    counts = tuple((w, m, s, 6) for w in windows for m, s in variants)
    ledger = overlap_mass(
        counts, Fraction(7, 11), window_ids=windows, variants=variants
    )
    assert len(ledger) == 12
    assert all(item.per_row == Fraction(7, 792) for item in ledger)
    assert all(item.reserved_mass == Fraction(7, 132) for item in ledger)
    assert sum(
        (item.per_row * item.row_count for item in ledger), Fraction()
    ) == Fraction(7, 11)
    # Six rows already include all three symbols. No independent symbol budget.
    rows = tuple(item.per_row for item in ledger for _ in range(item.row_count))
    assert len(rows) == 72
    assert sum(rows, Fraction()) == Fraction(7, 11)


def test_ragged_hierarchy_normalizes_windows_then_members_then_scenarios():
    windows = ("w0", "w1")
    variants = (("m0", "s0"), ("m0", "s1"), ("m1", "s0"))
    counts = (
        ("w0", "m0", "s0", 2),
        ("w0", "m0", "s1", 0),
        ("w0", "m1", "s0", 3),
        ("w1", "m0", "s0", 4),
        ("w1", "m0", "s1", 1),
        ("w1", "m1", "s0", 5),
    )
    ledger = overlap_mass(
        counts, Fraction(5, 7), window_ids=windows, variants=variants
    )
    assert _row_masses(ledger) == {
        ("w0", "m0", "s0"): Fraction(5, 112),
        ("w0", "m0", "s1"): Fraction(),
        ("w0", "m1", "s0"): Fraction(5, 84),
        ("w1", "m0", "s0"): Fraction(5, 224),
        ("w1", "m0", "s1"): Fraction(5, 56),
        ("w1", "m1", "s0"): Fraction(1, 28),
    }
    empty = next(item for item in ledger if item.row_count == 0)
    assert empty.reserved_mass == Fraction(5, 56)
    assert sum((item.reserved_mass for item in ledger), Fraction()) == Fraction(
        5, 7
    )
    assert sum(
        (item.per_row * item.row_count for item in ledger), Fraction()
    ) == Fraction(5, 8)


def test_empty_windows_and_output_selection_never_redistribute_mass():
    counts = (("full", "m", "s", 3), ("empty", "m", "s", 0))
    ledger = overlap_mass(
        counts, window_ids=("empty", "full"), variants=(("m", "s"),)
    )
    assert _row_masses(ledger) == {
        ("empty", "m", "s"): Fraction(),
        ("full", "m", "s"): Fraction(1, 6),
    }
    assert sum(
        (item.per_row * item.row_count for item in ledger), Fraction()
    ) == Fraction(1, 2)
    selected = tuple(item for item in ledger if item.window_id == "full")
    assert selected[0].per_row == Fraction(1, 6)
    assert selected[0].per_row * 2 == Fraction(1, 3)
    assert tuple(item for item in ledger if item.window_id == "missing") == ()


def test_worker_completion_order_does_not_change_exact_mass_ledger():
    counts = (("a", "m", "s", 2), ("b", "m", "s", 3), ("c", "m", "s", 0))
    kwargs = {"window_ids": ("a", "b", "c"), "variants": (("m", "s"),)}
    expected = overlap_mass(counts, Fraction(13, 17), **kwargs)
    for reordered in permutations(counts):
        assert overlap_mass(reordered, Fraction(13, 17), **kwargs) == expected


@pytest.mark.parametrize(
    "counts",
    (
        (),
        (("w0", "m", "s", 1),),
        (("w0", "m", "s", 1), ("w0", "m", "s", 2)),
        (("w0", "m", "s", 1), ("outside", "m", "s", 2)),
        (("w0", "m", "s", 1), ("w1", "other", "s", 2)),
    ),
)
def test_missing_duplicate_or_outside_branch_refuses_instead_of_renormalizing(
    counts,
):
    with pytest.raises((TypeError, ValueError)):
        overlap_mass(counts, window_ids=("w0", "w1"), variants=(("m", "s"),))


@pytest.mark.parametrize("count", (True, 1.0, "1", -1))
def test_complete_row_counts_are_exact_nonnegative_integers(count):
    with pytest.raises((TypeError, ValueError)):
        overlap_mass(
            (("w", "m", "s", count),), window_ids=("w",), variants=(("m", "s"),)
        )


def test_explicit_zero_budget_keeps_every_branch_without_scored_mass():
    ledger = overlap_mass(
        (("w", "m", "s", 3),),
        Fraction(),
        window_ids=("w",),
        variants=(("m", "s"),),
    )
    assert len(ledger) == 1
    assert ledger[0].row_count == 3
    assert ledger[0].reserved_mass == ledger[0].per_row == Fraction()


@pytest.mark.parametrize("mass", (True, 1, 1.0, Fraction(-1)))
def test_parent_budget_is_an_explicit_nonnegative_exact_fraction(mass):
    with pytest.raises((TypeError, ValueError)):
        overlap_mass(
            (("w", "m", "s", 1),),
            mass,
            window_ids=("w",),
            variants=(("m", "s"),),
        )


def _observed_plan(directory, points, geometry):
    """Genuine raw IPC with explicit controlled clocks/values, no reader mocks."""
    root = directory / "ASCII" / "T"
    adapter = HistDataProviderAdapter()
    for symbol in SYMBOLS:
        path = histdata_cache_path(root, symbol, "202001")
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "datetime": [time // 1_000_000 for time, _ in points],
                "bid": [float(value) for _, value in points],
                "ask": [float(value) for _, value in points],
                "vol": [0] * len(points),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        ).write_ipc(path)
    proof = directory / "generated-fixture.json"
    proof.write_text('{"synthetic_clock_and_value_fixture_only":true}')
    descriptor = DatasetDescriptorV1(
        "overlap-independent-math-fixture",
        "Overlap independent math fixture",
        "Generated raw quotes; not historical or empirical evidence.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=("202001",),
        qualification_evidence=(artifact_ref_for_file(proof, kind="fixture"),),
    )
    source = TrainingSourceV1(
        DatasetCatalog(
            (adapter.provider,),
            (adapter.descriptor,),
            (descriptor,),
            (version,),
        ).to_json(),
        version.dataset_version_id,
    )
    ownership = build_training_ownership(source)
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(u.artifact_id, TemporalPartition.TRAIN)
            for u in ownership.units
        ),
    )
    return TrainingOverlapPlanV1(
        source, ownership, split, geometry, native_intervals=()
    )


@pytest.fixture(scope="module")
def imbalanced(tmp_path_factory):
    points = tuple(
        (BASE + 3 * DAY_NS // 5 + i * SECOND, 1.0) for i in range(18)
    )
    points += tuple(
        (BASE + DAY_NS + fraction * DAY_NS // 20, 2.0)
        for fraction in (12, 13, 14)
    )
    points += ((BASE + 2 * DAY_NS + 3 * DAY_NS // 5, 3.0),)
    plan = _observed_plan(
        tmp_path_factory.mktemp("overlap-imbalanced"),
        points,
        TrainingOverlapGeometryV1(
            BASE, BASE + 3 * DAY_NS, DAY_NS // 2, DAY_NS // 4
        ),
    )
    return plan, materialize_training_overlap(plan)


def _actual_unit_masses(batch):
    allocations = {a.artifact_id: a for a in batch.allocations}
    totals = {u.artifact_id: Fraction() for u in batch.plan.ownership.units}
    for row in batch.rows:
        allocation = allocations[row.allocation_id]
        totals[allocation.evidence_unit_id] += allocation.per_row
    return totals


def test_imbalanced_source_density_cannot_borrow_other_original_unit_budget(
    imbalanced,
):
    plan, batch = imbalanced
    by_start = {u.start_ns: u.artifact_id for u in plan.ownership.units}
    totals = _actual_unit_masses(batch)
    assert len(plan.ownership.units) == 31
    assert tuple(totals[by_start[BASE + i * DAY_NS]] for i in range(3)) == (
        Fraction(1, 2),
        Fraction(2, 5),
        Fraction(1, 2),
    )
    assert all(Fraction() <= mass <= 1 for mass in totals.values())
    assert dict(overlap_unit_mass(batch)) == totals
    # The first day has54 coordinates, the second9, the third3. Density changes
    # per-row weight, not the independently reserved historical budget.
    counts = tuple(
        sum(
            c.evidence_unit_id == by_start[BASE + i * DAY_NS]
            for c in batch.coordinates
        )
        for i in range(3)
    )
    assert counts == (54, 9, 3)
    for unit_id in by_start.values():
        ledger = tuple(
            a for a in batch.allocations if a.evidence_unit_id == unit_id
        )
        assert sum((a.reserved_mass for a in ledger), Fraction()) == (
            1 if ledger else 0
        )
    assert all(row.historical_available_at_ns is None for row in batch.rows)


def test_source_backed_selection_preserves_all_denominators_and_unspent_mass(
    imbalanced,
):
    plan, complete = imbalanced
    window = complete.windows[1]
    coordinate_ids = tuple(
        sorted(
            c.coordinate_id
            for c in complete.coordinates
            if c.symbol == "EURUSD" and window.index in c.window_indices
        )
    )[:2]
    selected = materialize_training_overlap(
        plan,
        TrainingOverlapSelectionV1(
            window_ids=(window.window_id,),
            coordinate_ids=coordinate_ids,
            member_ids=("observed-baseline",),
        ),
    )
    assert selected.coordinates == complete.coordinates
    assert selected.occurrences == complete.occurrences
    assert selected.windows == complete.windows
    assert selected.carries == complete.carries
    assert selected.allocations == complete.allocations
    assert (
        selected.maximum_dependency_span_ns
        == complete.maximum_dependency_span_ns
    )
    assert len(selected.rows) == 2
    assert all(row in complete.rows for row in selected.rows)
    assert sum(_actual_unit_masses(selected).values(), Fraction()) == Fraction(
        1, 108
    )
    empty = materialize_training_overlap(
        plan, TrainingOverlapSelectionV1(coordinate_ids=())
    )
    assert not empty.rows
    assert empty.allocations == complete.allocations
    assert empty.windows == complete.windows
    assert empty.coordinates == complete.coordinates
    assert all(mass == 0 for _, mass in overlap_unit_mass(empty))


def test_stride_changes_window_identity_not_immutable_source_coordinates(
    imbalanced,
):
    plan, original = imbalanced
    changed = materialize_training_overlap(
        replace(plan, geometry=replace(plan.geometry, stride_ns=DAY_NS // 5))
    )
    assert {w.window_id for w in original.windows}.isdisjoint(
        w.window_id for w in changed.windows
    )
    assert {
        c.source_row_key: (c.coordinate_id, c.event_time_ns, c.value_json)
        for c in original.coordinates
    } == {
        c.source_row_key: (c.coordinate_id, c.event_time_ns, c.value_json)
        for c in changed.coordinates
    }
    for coordinate in changed.coordinates:
        assert coordinate.window_indices == tuple(
            w.index
            for w in changed.windows
            if w.start_ns <= coordinate.event_time_ns < w.end_ns
        )


def test_actual_three_way_split_purges_bridge_windows_but_retains_usable_groups(
    tmp_path,
):
    plan = overlap_fixture(tmp_path, multiple_splits=True)
    batch = materialize_training_overlap(plan)
    assert batch.maximum_dependency_span_ns == DAY_NS // 2
    assert tuple(w.index for w in batch.windows if not w.reasons) == (
        0,
        1,
        2,
        6,
        10,
    )
    assert tuple(
        w.index for w in batch.windows if "core_crosses_partition" in w.reasons
    ) == (3, 7)
    assert tuple(
        w.index for w in batch.windows if "embargo_dependency_span" in w.reasons
    ) == (4, 5, 8, 9)
    groups = {}
    for window in batch.windows:
        if window.reasons:
            assert window.group_id is None
            assert not any(
                row.window_id == window.window_id for row in batch.rows
            )
        else:
            groups.setdefault(window.group_id, set()).add(window.partition)
    assert len(groups) == 3
    assert all(len(partitions) == 1 for partitions in groups.values())
    assert set().union(*groups.values()) == set(TemporalPartition)
    assert batch.windows[0].group_id == batch.windows[2].group_id
    # All raw memberships survive the split exclusions; no support is erased.
    assert any(
        5 in coordinate.window_indices for coordinate in batch.coordinates
    )
    assert any(
        9 in coordinate.window_indices for coordinate in batch.coordinates
    )
    by_start = {u.start_ns: u.artifact_id for u in plan.ownership.units}
    totals = _actual_unit_masses(batch)
    assert tuple(totals[by_start[BASE + i * DAY_NS]] for i in range(3)) == (
        Fraction(1, 2),
        Fraction(1, 5),
        Fraction(1, 4),
    )
    assert (
        sum(
            (
                a.reserved_mass
                for a in batch.allocations
                if a.window_id == batch.windows[5].window_id
            ),
            Fraction(),
        )
        > 0
    )


def test_child_start_carry_excludes_boundary_duplicates_and_parent_future(
    tmp_path,
):
    points = tuple(
        (BASE + second * SECOND, value)
        for second, value in (
            (0, 1.0),
            (3, 10.0),
            (3, 20.0),
            (5, 30.0),
            (6, 100.0),
            (8, 200.0),
            (11, 300.0),
        )
    )
    plan = _observed_plan(
        tmp_path,
        points,
        TrainingOverlapGeometryV1(
            BASE, BASE + 12 * SECOND, 6 * SECOND, 3 * SECOND
        ),
    )
    batch = materialize_training_overlap(
        replace(plan, carry_lookback_ns=3 * SECOND)
    )
    by_id = {c.coordinate_id: c for c in batch.coordinates}
    carries = {(c.window_id, c.symbol): c for c in batch.carries}
    early = carries[batch.windows[1].window_id, "EURUSD"]
    later = carries[batch.windows[2].window_id, "EURUSD"]
    assert early.mean_midpoint == early.last_midpoint == 1.0
    assert len(early.coordinate_ids) == 1
    assert later.mean_midpoint == 20.0
    assert later.last_midpoint == 30.0
    assert len(later.coordinate_ids) == 3
    duplicates = [
        by_id[key]
        for key in later.coordinate_ids
        if by_id[key].event_time_ns == BASE + 3 * SECOND
    ]
    assert len(duplicates) == 2
    assert duplicates[0].source_row_key != duplicates[1].source_row_key
    assert training_load(duplicates[0].value_json)["bid"] == 10.0
    assert training_load(duplicates[1].value_json)["bid"] == 20.0
    for carry in (early, later):
        assert all(
            by_id[key].event_time_ns < carry.dependency_end_ns
            for key in carry.coordinate_ids
        )


def test_actual_native_generated_prefix_refuses_future_anchor_conditioning(
    tmp_path,
):
    plan = overlap_fixture(tmp_path, native=True)
    triangle_policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(
            InformationMode.EX_POST_RECONSTRUCTION,
            intervals=("1d",),
            scopes=(ActivitySliceScope.MERGED,),
            allow_prior_generated_state=True,
        ),
        BASE,
        join_policy=CrossCurrencyJoinPolicy.NEAREST_PRIOR_BOUNDED_NO_FORWARD_FILL,
        max_quote_age_ns=SECOND,
    )
    plan = replace(
        plan,
        geometry=TrainingOverlapGeometryV1(
            BASE + 3 * DAY_NS // 5,
            BASE + 17 * DAY_NS // 20,
            DAY_NS // 10,
            DAY_NS // 40,
        ),
        carry_lookback_ns=DAY_NS // 10,
        native_intervals=("1d",),
        triangle_policy_json=training_json(triangle_policy.to_dict()),
    )
    batch = materialize_training_overlap(plan)
    future_window = next(
        w for w in batch.windows if w.start_ns == BASE + 27 * DAY_NS // 40
    )
    failed = tuple(
        c
        for c in batch.carries
        if c.window_id == future_window.window_id
        and c.state == "unavailable_future_conditioning"
    )
    assert failed
    assert {c.member_id for c in failed} == {"member-a", "member-b"}
    assert all(
        c.last_midpoint is None
        and c.mean_midpoint is None
        and not c.coordinate_ids
        for c in failed
    )
    assert "unavailable_prefix_state" in future_window.reasons
    assert not any(
        row.window_id == future_window.window_id for row in batch.rows
    )
    assert any(
        c.window_id == future_window.window_id
        and c.member_id == "observed-baseline"
        and c.state == "available_strict_prior"
        for c in batch.carries
    )
    later_window = next(
        w for w in batch.windows if w.start_ns == BASE + 29 * DAY_NS // 40
    )
    assert any(
        c.window_id == later_window.window_id
        and c.member_id == "member-a"
        and c.state == "available_strict_prior"
        for c in batch.carries
    )
    assert batch.native
    assert all(
        item.feature_cutoff_ns
        == next(
            w.end_ns for w in batch.windows if w.window_id == item.window_id
        )
        for item in batch.native
    )
    windows = {w.window_id: w for w in batch.windows}
    coordinate_by_event = {
        (
            o.product_manifest_id,
            training_load(o.native_event_json)["event_id"],
        ): o.coordinate_id
        for o in batch.occurrences
        if o.native_event_json is not None
    }
    seen = {}
    for item in batch.native:
        window = windows[item.window_id]
        for encoded in item.triangle_tuples_json:
            point = TriangleQuoteTupleV1.from_json(encoded)
            assert tuple(e.symbol.upper() for e in point.events) == SYMBOLS
            assert point.endpoint_ns == window.end_ns
            event_coordinates = tuple(
                coordinate_by_event[item.product_manifest_id, event.event_id]
                for event in point.events
            )
            assert set(event_coordinates) <= set(item.event_coordinate_ids)
            assert all(
                window.start_ns <= event.event_time_ns < window.end_ns
                for event in point.events
            )
            seen.setdefault(event_coordinates, set()).add(item.window_id)
    # The same original/native tuple is an analytical descendant in multiple
    # windows, with the same three immutable coordinates and separate cutoffs.
    assert any(len(window_ids) > 1 for window_ids in seen.values())

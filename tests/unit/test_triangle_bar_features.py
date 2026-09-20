"""Independent triangle mathematics and causal/source boundary canaries."""

from dataclasses import FrozenInstanceError, replace
import json
import math
from statistics import median

import pytest

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import BarFeaturePolicyV1
from histdatacom.synthetic.bars import STANDARD_DERIVED_BAR_INTERVALS
from histdatacom.synthetic.cross_currency import CrossCurrencyJoinPolicy
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_bar_features import (
    TRIANGLE_SYMBOLS,
    TriangleBarPolicyV1,
    TriangleBarSnapshotV1,
    _cells,
    decode_triangle_event,
    event_order,
    synchronized_triangle_tuples,
    triangle_feature_definitions,
)
from tests.fixtures.triangle_bar_features_v1 import (
    BASE,
    POST,
    math_events,
    math_snapshot,
    published_triangle,
    quote,
    run_for,
)

OBS = ActivitySliceScope.OBSERVED


@pytest.mark.parametrize("interval", tuple(STANDARD_DERIVED_BAR_INTERVALS))
@pytest.mark.parametrize("scope", tuple(ActivitySliceScope))
def test_independent_feature_families_all_seven_widths_three_scopes(
    interval, scope
):
    snapshot = math_snapshot(interval, scope)
    cell = snapshot.cell(scope, interval)
    history = [
        tuple(
            round(base + bar * (leg + 1) * 0.1 + 0.25, 12)
            for leg, base in enumerate((2.0, 10.0, 5.0))
        )
        for bar in range(4)
    ]
    closes = history[-1]
    residuals = [math.log(u) - math.log(x) - math.log(g) for x, u, g in history]
    center = median(residuals[:3])
    scale = 1.4826 * median(abs(v - center) for v in residuals[:3])
    opens = (2.3, 10.6, 5.9)
    lows = (2.05, 10.35, 5.65)
    highs = (2.8, 11.1, 6.4)
    lower = math.log(lows[1]) - math.log(highs[0]) - math.log(highs[2])
    upper = math.log(highs[1]) - math.log(lows[0]) - math.log(lows[2])
    returns = [
        math.log(b) - math.log(a)
        for a, b in zip(history[-2], closes, strict=True)
    ]
    expected = {
        "residual_open": math.log(opens[1])
        - math.log(opens[0])
        - math.log(opens[2]),
        "residual_close": residuals[-1],
        "residual_lower_bound": lower,
        "residual_upper_bound": upper,
        "residual_range_bound": upper - lower,
        "residual_change": residuals[-1] - residuals[-2],
        "residual_robust_zscore_3": (residuals[-1] - center)
        / max(scale, 1e-12),
        "robust_scale_floored": 0.0,
        "common_return": sum(returns) / 3,
        "common_activity": (8 if scope is ActivitySliceScope.MERGED else 4)
        / (STANDARD_DERIVED_BAR_INTERVALS[interval] / 1e9),
        "observed_support_proportion": (
            1.0
            if scope is OBS
            else 0.5 if scope is ActivitySliceScope.MERGED else 0.0
        ),
        "synthetic_support_proportion": (
            0.0
            if scope is OBS
            else 0.5 if scope is ActivitySliceScope.MERGED else 1.0
        ),
    }
    for symbol, value, close in zip(
        TRIANGLE_SYMBOLS, returns, closes, strict=True
    ):
        expected[symbol.lower() + "_return"] = value
        expected[symbol.lower() + "_relative_spread"] = 0.02 / close
    for left, right in ((1, 0), (1, 2), (0, 2)):
        expected[
            TRIANGLE_SYMBOLS[left].lower()
            + "_minus_"
            + TRIANGLE_SYMBOLS[right].lower()
            + "_return"
        ] = (returns[left] - returns[right])
        expected[
            TRIANGLE_SYMBOLS[left].lower()
            + "_over_"
            + TRIANGLE_SYMBOLS[right].lower()
            + "_activity"
        ] = 1.0
    for name, value in expected.items():
        assert cell.feature(name).value == value, name
        assert cell.feature(name).available_at_ns == snapshot.decision_time_ns
    for label, point in (
        ("first", cell.first_tuple),
        ("last", cell.last_tuple),
    ):
        x, u, g = point.events
        assert cell.feature(label + "_sell_direct_gap").value == math.log(
            x.bid
        ) + math.log(g.bid) - math.log(u.ask)
        assert cell.feature(label + "_buy_direct_gap").value == math.log(
            u.bid
        ) - math.log(x.ask) - math.log(g.ask)
    assert len(cell.feature("residual_robust_zscore_3").source_bar_ids) == 12
    assert TriangleBarSnapshotV1.from_json(snapshot.to_json()) == snapshot
    assert all(
        d.construction and d.units for d in triangle_feature_definitions()
    )


@pytest.mark.parametrize("interval", tuple(STANDARD_DERIVED_BAR_INTERVALS))
def test_independent_boundary_oracle_and_real_future_quote_canary(interval):
    width = STANDARD_DERIVED_BAR_INTERVALS[interval]
    boundary = BASE + 4 * width
    events = list(math_events(interval))
    for symbol in TRIANGLE_SYMBOLS:
        for offset in (-1, 0, 1):
            events.append(
                quote(
                    run_for(),
                    symbol,
                    boundary + offset,
                    900 + offset,
                    999.0,
                    1000.0,
                )
            )
    events = tuple(sorted(events, key=event_order))
    for cutoff in (boundary - 1, boundary, boundary + 1):
        snapshot = math_snapshot(interval, events=events, cutoff=cutoff)
        cell = snapshot.cells[0]
        expected_boundary = (cutoff // width) * width
        assert cell.bar_end_ns == expected_boundary
        assert cell.bar_start_ns == expected_boundary - width
        assert all(
            decode_triangle_event(t).event_time_ns < expected_boundary
            for t in snapshot.event_json
        )
        if cutoff >= boundary:
            assert cell.last_tuple.probe_time_ns == boundary - 1
            assert all(e.bid == 999.0 for e in cell.last_tuple.events)


@pytest.mark.parametrize("mode", tuple(InformationMode))
@pytest.mark.parametrize("scope", tuple(ActivitySliceScope))
def test_information_modes_are_enforced(mode, scope):
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(
            mode,
            intervals=("1m",),
            scopes=(scope,),
            allow_prior_generated_state=True,
        ),
        BASE,
        max_endpoint_age_ns=60_000_000_000,
    )
    if mode is InformationMode.EX_ANTE_SIMULATION and scope is not OBS:
        with pytest.raises(ValueError, match="bound information audit"):
            math_snapshot(scope=scope, policy=policy)
    else:
        assert (
            math_snapshot(scope=scope, policy=policy)
            .cells[0]
            .feature("residual_close")
            .value
            is not None
        )


def test_bounded_prior_probe_no_future_no_previous_bar_carry_age_equality():
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(POST, intervals=("1m",), scopes=(OBS,)),
        BASE,
        join_policy=CrossCurrencyJoinPolicy.NEAREST_PRIOR_BOUNDED_NO_FORWARD_FILL,
        max_quote_age_ns=10,
        max_endpoint_age_ns=20,
    )
    run = run_for()
    events = [
        quote(run, "EURGBP", BASE + 10, 1, 0.8, 0.81),
        quote(run, "EURUSD", BASE + 20, 1, 1.2, 1.21),
        quote(run, "GBPUSD", BASE + 11, 1, 1.5, 1.51),
        quote(run, "EURGBP", BASE + 21, 2, 9.0, 9.1),
    ]
    tuples = synchronized_triangle_tuples(
        events, start_ns=BASE, end_ns=BASE + 30, scope=OBS, policy=policy
    )
    assert len(tuples) == 1
    assert tuples[0].ages_ns == (10, 0, 9)
    assert tuples[0].events[0].bid == 0.8
    assert not synchronized_triangle_tuples(
        events, start_ns=BASE + 11, end_ns=BASE + 30, scope=OBS, policy=policy
    )
    assert not synchronized_triangle_tuples(
        events,
        start_ns=BASE,
        end_ns=BASE + 30,
        scope=OBS,
        policy=replace(policy, max_quote_age_ns=9),
    )


def test_ohlc_endpoints_are_not_relabelled_as_synchronized_probes():
    events = list(math_events())
    events = [
        (
            replace(e, event_time_ns=e.event_time_ns + 5, event_id="")
            if e.symbol == "eurusd"
            else e
        )
        for e in events
    ]
    events.append(
        quote(
            run_for(),
            "EURGBP",
            BASE + 3 * 60_000_000_000 + 12_000_000_003,
            999,
            3.0,
            3.02,
        )
    )
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(POST, intervals=("1m",), scopes=(OBS,)),
        BASE,
        join_policy=CrossCurrencyJoinPolicy.NEAREST_PRIOR_BOUNDED_NO_FORWARD_FILL,
        max_quote_age_ns=5,
        max_endpoint_age_ns=60_000_000_000,
    )
    snapshot = math_snapshot(
        events=tuple(sorted(events, key=event_order)), policy=policy
    )
    cell = snapshot.cells[0]
    assert cell.first_tuple.ages_ns == (2, 0, 5)
    assert (
        cell.feature("first_synchronized_residual").value
        != cell.feature("residual_open").value
    )
    assert (
        cell.first_tuple.events[0].event_id
        != snapshot.leg_snapshots[0].bars[-1].first_event_id
    )
    assert cell.feature(
        "first_synchronized_residual"
    ).source_event_ids == tuple(e.event_id for e in cell.first_tuple.events)
    stale = replace(policy, max_endpoint_age_ns=0)
    old = math_snapshot(
        events=tuple(sorted(events, key=event_order)), policy=stale
    ).cells[0]
    assert old.feature("residual_close").value is not None
    assert old.feature("last_synchronized_residual").state == "stale_endpoint"
    assert old.feature("first_synchronized_residual").value is not None


def test_unknown_mixed_strata_warmup_and_wire_refusals():
    missing = math_snapshot(strata=False)
    assert (
        missing.cells[0].feature("residual_robust_zscore_3").state
        == "unknown_or_mixed_stratum"
    )
    snapshot = math_snapshot()
    partial = math_snapshot(partial=True)
    assert all(
        v.value is None and v.state == "missing_or_partial_leg"
        for v in partial.cells[0].values
    )
    changed = (replace(snapshot.strata[0], session="other"),) + snapshot.strata[
        1:
    ]
    events = tuple(decode_triangle_event(t) for t in snapshot.event_json)
    mixed = replace(
        snapshot,
        strata=changed,
        cells=_cells(snapshot.leg_snapshots, snapshot.policy, events, changed),
    )
    assert (
        mixed.cells[0].feature("residual_robust_zscore_3").state
        == "unknown_or_mixed_stratum"
    )
    generated = math_snapshot(scope=ActivitySliceScope.SYNTHETIC)
    bad_labels = tuple(
        replace(s, feed_epoch_id="contradictory-epoch")
        for s in generated.strata
    )
    generated_events = tuple(
        decode_triangle_event(t) for t in generated.event_json
    )
    mismatch = replace(
        generated,
        strata=bad_labels,
        cells=_cells(
            generated.leg_snapshots,
            generated.policy,
            generated_events,
            bad_labels,
        ),
    )
    assert (
        mismatch.cells[0].feature("residual_robust_zscore_3").state
        == "unknown_or_mixed_stratum"
    )
    gap = math_snapshot(missing=(("EURUSD", BASE + 60_000_000_000),))
    assert (
        gap.cells[0].feature("residual_robust_zscore_3").state
        == "insufficient_warmup"
    )
    data = snapshot.to_dict()
    data["leg_snapshots"][0]["bars"][0]["unknown"] = True
    with pytest.raises(ValueError, match="canonical"):
        TriangleBarSnapshotV1.from_dict(data)
    raw = json.loads(snapshot.event_json[0])
    raw["future_classification"] = "unknown"
    with pytest.raises(ValueError, match="canonical"):
        replace(
            snapshot, event_json=(json.dumps(raw),) + snapshot.event_json[1:]
        )
    raw.pop("future_classification")
    raw["bid"] = str(raw["bid"])
    with pytest.raises(ValueError):
        replace(
            snapshot, event_json=(json.dumps(raw),) + snapshot.event_json[1:]
        )
    with pytest.raises(FrozenInstanceError):
        snapshot.cells = ()


@pytest.mark.parametrize("crossed", (False, True))
def test_signed_executable_direction_and_zero_mad_floor(crossed):
    quotes = {
        "eurgbp": (0.8199, 0.8201) if crossed else (0.7999, 0.8001),
        "eurusd": (1.1999, 1.2001),
        "gbpusd": (1.4999, 1.5001),
    }
    events = tuple(
        replace(
            e, bid=quotes[e.symbol][0], ask=quotes[e.symbol][1], event_id=""
        )
        for e in math_events()
    )
    cell = math_snapshot(events=events).cells[0]
    expected_sell = (
        math.log(quotes["eurgbp"][0])
        + math.log(quotes["gbpusd"][0])
        - math.log(quotes["eurusd"][1])
    )
    expected_buy = (
        math.log(quotes["eurusd"][0])
        - math.log(quotes["eurgbp"][1])
        - math.log(quotes["gbpusd"][1])
    )
    assert cell.feature("last_sell_direct_gap").value == expected_sell
    assert cell.feature("last_buy_direct_gap").value == expected_buy
    assert (expected_sell > 0) is crossed
    assert expected_buy < 0
    assert cell.feature("robust_scale_floored").value == 1.0
    assert cell.feature("residual_robust_zscore_3").value == 0.0


def test_future_metadata_invariance_and_budget_before_deep_verification(
    tmp_path, monkeypatch
):
    source, snapshot, _, _ = published_triangle(tmp_path)
    clocks = tuple(
        a for leg in snapshot.leg_snapshots for a in leg.availability
    )
    future = replace(
        snapshot.strata[0],
        known_at_ns=snapshot.decision_time_ns + 1,
        session="future",
    )
    replay = source.snapshot(
        decision_time_ns=snapshot.decision_time_ns,
        policy=snapshot.policy,
        availability=clocks,
        strata=snapshot.strata + (future,),
    )
    assert replay.artifact_id == snapshot.artifact_id
    import histdatacom.synthetic.bar_features as module

    def forbidden(*args, **kwargs):
        pytest.fail("deep verification preceded budget refusal")

    monkeypatch.setattr(module, "verify_reconstruction_publication", forbidden)
    with pytest.raises(ValueError, match="total source event"):
        source.snapshot(
            decision_time_ns=snapshot.decision_time_ns,
            policy=replace(
                snapshot.policy,
                bar_policy=replace(
                    snapshot.policy.bar_policy, max_source_events=1
                ),
            ),
        )

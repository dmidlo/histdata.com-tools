"""Independent indicator arithmetic, cutoff canaries and verified products."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import json
import math
from pathlib import Path

import pytest

from histdatacom.broker_plugin_policy import provider_reconstruction_inputs

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAbsenceDeclarationV1,
    BarAvailabilityBasis,
    BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1,
    BarFeatureSourceV1,
    BarFeatureState,
    CausalBarSnapshotV1,
    _cells,
    bar_feature_definitions,
)
from histdatacom.synthetic.bars import (
    STANDARD_DERIVED_BAR_INTERVALS,
    DerivedBarPolicyV1,
    derive_reconstruction_bars,
    publish_derived_bars,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import publish_reconstruction_group
from tests.unit.test_synthetic_contracts import _generated, _observed
from tests.unit.test_synthetic_persistence import _publication_inputs_scope

POST = InformationMode.EX_POST_RECONSTRUCTION
ANTE = InformationMode.EX_ANTE_SIMULATION
OBS = ActivitySliceScope.OBSERVED
DAY = STANDARD_DERIVED_BAR_INTERVALS["1d"]
BASE = 20_000 * DAY
# Pure arithmetic has no retained ProductV1 or provider provenance. Actual
# broker products are exercised separately by the published_source fixture.
MATH_PRODUCT_ID = "reconstruction-manifest-v2:sha256:" + "a" * 64
MATH_BAR_MANIFEST_ID = "derived-bar-manifest:sha256:" + "b" * 64


def _math_snapshot(
    interval="1m",
    scope=OBS,
    *,
    mode=POST,
    missing=(),
    partial=False,
    partial_end=False,
    flat=False,
):
    duration = STANDARD_DERIVED_BAR_INTERVALS[interval]
    events = []
    for index, quotes in enumerate(
        ((10, 12, 9, 11), (11, 15, 10, 14), (14, 16, 12, 13), (13, 19, 11, 18))
    ):
        if index in missing:
            continue
        for point, bid in enumerate(quotes):
            if flat:
                bid = 10
            time = BASE + index * duration + (point + 1) * duration // 5
            observed = _observed(
                index * 10 + point + 1, event_time_ns=time, bid=bid
            )
            if scope is OBS:
                events.append(observed)
            else:
                right = _observed(
                    100 + index * 10 + point, event_time_ns=time + 100
                )
                generated = replace(
                    _generated(observed, right),
                    event_time_ns=time,
                    bid=bid,
                    ask=bid + 0.0001,
                    broker_profile_id=None,
                    event_id="",
                )
                events.append(generated)
                if scope is ActivitySliceScope.MERGED:
                    events.append(
                        replace(observed, event_time_ns=time + 1, event_id="")
                    )
    events.sort(
        key=lambda item: (
            item.event_time_ns,
            item.event_sequence,
            item.event_id,
        )
    )
    bars = derive_reconstruction_bars(
        events,
        source_product_manifest_id=MATH_PRODUCT_ID,
        run_id=events[0].run_id,
        ensemble_member_id=events[0].ensemble_member_id,
        policy=DerivedBarPolicyV1(intervals=(interval,), scopes=(scope,)),
        start_ns=BASE + 1 if partial else BASE,
        end_ns=BASE + 4 * duration - int(partial_end),
    )
    policy = BarFeaturePolicyV1(
        mode,
        intervals=(interval,),
        scopes=(scope,),
        allow_prior_generated_state=True,
    )
    cutoff = BASE + 4 * duration
    availability = tuple(
        sorted(
            (
                BarAvailabilityDeclarationV1(
                    bar.bar_id,
                    bar.bar_end_ns,
                    bar.bar_end_ns,
                    BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
                    "fixture assertion",
                    generated_at_ns=(
                        bar.bar_end_ns if scope is not OBS else None
                    ),
                )
                for bar in bars
            ),
            key=lambda item: item.bar_id,
        )
    )
    return CausalBarSnapshotV1(
        "EURUSD",
        cutoff,
        policy,
        bars,
        availability,
        (),
        _cells("EURUSD", cutoff, policy, bars, availability, ()),
        MATH_PRODUCT_ID,
        MATH_BAR_MANIFEST_ID,
    )


@pytest.mark.parametrize("interval", tuple(STANDARD_DERIVED_BAR_INTERVALS))
@pytest.mark.parametrize("scope", tuple(ActivitySliceScope))
def test_independent_math_all_intervals_and_scopes(interval, scope):
    snapshot = _math_snapshot(interval, scope)
    cell = snapshot.cell(scope, interval)
    q = 0.00005
    closes = [11 + q, 14 + q, 13 + q, 18 + q]
    mean = sum(closes[:3]) / 3
    std = math.sqrt(sum((x - mean) ** 2 for x in closes[:3]) / 3)
    expected = {
        "mid_open": 13 + q,
        "mid_high": 19 + q,
        "mid_low": 11 + q,
        "mid_close": 18 + q,
        "mid_body": 5.0,
        "mid_upper_wick": 1.0,
        "mid_lower_wick": 2.0,
        "mid_body_range_ratio": 5 / 8,
        "mid_log_open_close_return": math.log((18 + q) / (13 + q)),
        "mid_log_close_return": math.log(closes[3] / closes[2]),
        "mid_log_range": math.log((19 + q) / (11 + q)),
        "mid_true_range": 8.0,
        "mid_atr_3": (5 + 4 + 8) / 3,
        "mid_sma_3": sum(closes[1:]) / 3,
        "mid_momentum_3": closes[3] - closes[0],
        "mid_slope_3": (closes[3] - closes[1]) / 2,
        "mid_realized_variation_3": sum(
            math.log(b / a) ** 2 for a, b in zip(closes, closes[1:])
        ),
        "mid_prior_mean_3": mean,
        "mid_prior_std_3": std,
        "mid_prior_zscore_3": (closes[3] - mean) / std,
    }
    for name, value in expected.items():
        assert cell.feature(name).value == round(value, 12), name
        assert cell.feature(name).state is BarFeatureState.AVAILABLE
    assert len(cell.feature("mid_atr_3").source_bar_ids) == 4
    assert len(cell.feature("mid_prior_mean_3").source_bar_ids) == 4
    assert (
        cell.feature("mid_prior_mean_3").available_at_ns
        == BASE + 4 * STANDARD_DERIVED_BAR_INTERVALS[interval]
    )
    assert CausalBarSnapshotV1.from_json(snapshot.to_json()) == snapshot


@pytest.mark.parametrize("mode", (POST, ANTE))
@pytest.mark.parametrize("scope", tuple(ActivitySliceScope))
def test_information_mode_scope_admissibility(mode, scope):
    if mode is ANTE and scope is not OBS:
        with pytest.raises(ValueError, match="bound information audit"):
            _math_snapshot(scope=scope, mode=mode)
    else:
        assert (
            _math_snapshot(scope=scope, mode=mode).cell(scope, "1m").state
            is BarFeatureState.AVAILABLE
        )


def test_exact_warmup_partial_and_missing_are_not_filled():
    snapshot = _math_snapshot(missing=(1,))
    assert (
        snapshot.cell(OBS, "1m").feature("mid_atr_3").state
        is BarFeatureState.INSUFFICIENT_WARMUP
    )
    assert (
        snapshot.cell(OBS, "1m").feature("mid_log_close_return").value
        is not None
    )
    partial = _math_snapshot(partial=True)
    assert (
        partial.cell(OBS, "1m").feature("mid_atr_3").state
        is BarFeatureState.INSUFFICIENT_WARMUP
    )
    definitions = {item.name: item for item in bar_feature_definitions()}
    assert definitions["mid_prior_zscore_3"].lag_bars == 0
    assert definitions["mid_prior_mean_3"].lag_bars == 1
    assert definitions["mid_atr_3"].required_closed_bars == 4
    partial_current = _math_snapshot(partial_end=True).cells[0]
    assert partial_current.partial
    assert partial_current.state is BarFeatureState.PARTIAL_SUPPORT
    assert all(value.value is None for value in partial_current.values)
    missing_current = _math_snapshot(missing=(3,)).cells[0]
    assert missing_current.state is BarFeatureState.UNAVAILABLE
    assert all(value.value is None for value in missing_current.values)


def test_zero_scale_and_missing_confidence_are_not_zero_imputed():
    cell = _math_snapshot(flat=True).cells[0]
    for name in ("mid_body_range_ratio", "mid_prior_zscore_3"):
        assert cell.feature(name).value is None
        assert cell.feature(name).state is BarFeatureState.ZERO_SCALE
    assert cell.feature("mid_prior_std_3").value == 0.0
    assert cell.feature("mean_event_confidence").value is None
    assert (
        cell.feature("mean_event_confidence").state
        is BarFeatureState.NO_CONFIDENCE_SUPPORT
    )


def test_replay_and_generation_clocks_require_explicit_admission():
    snapshot = _math_snapshot()
    clocks = tuple(
        replace(item, basis=BarAvailabilityBasis.REPLAY_CLOCK_ASSUMPTION)
        for item in snapshot.availability
    )
    with pytest.raises(ValueError, match="explicit policy admission"):
        replace(snapshot, availability=clocks)
    replay = replace(
        snapshot,
        availability=clocks,
        policy=replace(snapshot.policy, allow_replay_clock_assumptions=True),
    )
    assert replay.uses_replay_clock
    generated = _math_snapshot(scope=ActivitySliceScope.SYNTHETIC)
    with pytest.raises(
        ValueError, match="explicit policy and generation clock"
    ):
        replace(
            generated,
            policy=replace(generated.policy, allow_prior_generated_state=False),
        )
    with pytest.raises(
        ValueError, match="explicit policy and generation clock"
    ):
        replace(
            generated,
            availability=tuple(
                replace(item, generated_at_ns=None)
                for item in generated.availability
            ),
        )


def test_contract_rejects_forged_features_and_mutable_or_nonfinite_fields():
    snapshot = _math_snapshot()
    with pytest.raises(FrozenInstanceError):
        snapshot.symbol = "GBPUSD"
    data = snapshot.to_dict()
    data["historical_availability_verified"] = True
    with pytest.raises(ValueError, match="cannot certify"):
        CausalBarSnapshotV1.from_dict(data)
    cell = snapshot.cells[0]
    with pytest.raises(ValueError, match="do not replay"):
        replace(
            snapshot,
            cells=(
                replace(
                    cell,
                    values=(
                        replace(cell.values[0], value=99.0),
                        *cell.values[1:],
                    ),
                ),
            ),
        )
    with pytest.raises(ValueError, match="finite"):
        replace(cell.values[0], value=float("nan"))
    with pytest.raises(ValueError, match="exact type"):
        replace(snapshot.policy, rounding_digits=True)
    with pytest.raises(ValueError, match="duplicate"):
        CausalBarSnapshotV1.from_json('{"schema_version":1,"schema_version":2}')
    with pytest.raises(ValueError, match="identity differs"):
        CausalBarSnapshotV1.from_dict(
            {**snapshot.to_dict(), "artifact_id": "fake"}
        )
    assert (
        json.loads(snapshot.to_json())["historical_availability_verified"]
        is False
    )
    nested_drift = snapshot.to_dict()
    nested_drift["bars"][0][
        "unrecognized_future_metadata"
    ] = "later-final-classification"
    with pytest.raises(ValueError, match="exact canonical evidence"):
        CausalBarSnapshotV1.from_dict(nested_drift)


@pytest.fixture
def published_source(tmp_path: Path):
    with _publication_inputs_scope(tmp_path) as inputs:
        rendered, anchors, storage, retention = inputs
        product = publish_reconstruction_group(
            tmp_path / "archive",
            rendered,
            immutable_source_anchors=anchors,
            symbol_group_id="eurusd-triangle",
            retention_plan=retention,
            storage_policy=storage,
        )
        bars = publish_derived_bars(
            tmp_path / "bars",
            product.manifest_path,
            policy=DerivedBarPolicyV1(
                intervals=("1m",), scopes=tuple(ActivitySliceScope)
            ),
        )
        source = BarFeatureSourceV1(
            str(product.manifest_path), str(bars.manifest_path)
        )
        with provider_reconstruction_inputs(product.manifest):
            yield source, product, tmp_path


def _published_snapshot(source, *, mode=POST, scope=OBS, delay=0):
    policy = BarFeaturePolicyV1(
        mode,
        intervals=("1m",),
        scopes=(scope,),
        allow_prior_generated_state=True,
    )
    bars = source.verified_bars("EURUSD", policy)
    selected = [bar for bar in bars if bar.scope is scope]
    cutoff = selected[0].bar_end_ns + delay
    clocks = tuple(
        BarAvailabilityDeclarationV1(
            bar.bar_id,
            bar.bar_end_ns + delay,
            bar.bar_end_ns + delay,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "operator fixture clock",
            bar.bar_end_ns if scope is not OBS else None,
        )
        for bar in selected
    )
    return source.snapshot(
        symbol="EURUSD",
        decision_time_ns=cutoff,
        policy=policy,
        availability=clocks,
    )


def test_verified_source_and_unavailable_future_do_not_change_snapshot(
    published_source,
):
    source, _, _ = published_source
    snapshot = _published_snapshot(source)
    source.verify_snapshot(snapshot, information_mode=POST)
    future = BarAvailabilityDeclarationV1(
        "derived-bar:sha256:" + "a" * 64,
        snapshot.decision_time_ns + 1,
        snapshot.decision_time_ns + 1,
        BarAvailabilityBasis.REPLAY_CLOCK_ASSUMPTION,
        "future assumption must not influence state",
    )
    late_absence = BarAbsenceDeclarationV1(
        "EURUSD",
        OBS,
        "1m",
        snapshot.cells[0].bar_start_ns,
        snapshot.decision_time_ns + 1,
        BarFeatureState.SOURCE_OUTAGE,
        "learned later",
    )
    unchanged = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=snapshot.decision_time_ns,
        policy=snapshot.policy,
        availability=(*snapshot.availability, future),
        absences=(late_absence,),
    )
    assert unchanged.to_json() == snapshot.to_json()
    with pytest.raises(ValueError, match="information modes"):
        source.verify_snapshot(snapshot, information_mode=ANTE)
    with pytest.raises(ValueError, match="total source event"):
        source.verified_bars(
            "EURUSD", replace(snapshot.policy, max_source_events=1)
        )
    with pytest.raises(ValueError, match="total source bar"):
        source.verified_bars(
            "EURUSD", replace(snapshot.policy, max_source_bars=1)
        )


@pytest.mark.parametrize(
    "reason",
    (
        BarFeatureState.EXPECTED_CLOSURE,
        BarFeatureState.MISSING_BAR,
        BarFeatureState.SOURCE_OUTAGE,
        BarFeatureState.UNSUPPORTED,
    ),
)
def test_cutoff_qualified_absence_reasons(published_source, reason):
    source, _, _ = published_source
    snapshot = _published_snapshot(source)
    declaration = BarAbsenceDeclarationV1(
        "EURUSD",
        OBS,
        "1m",
        snapshot.cells[0].bar_start_ns,
        snapshot.decision_time_ns,
        reason,
        "declared before decision",
    )
    missing = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=snapshot.decision_time_ns,
        policy=snapshot.policy,
        absences=(declaration,),
    )
    assert missing.cells[0].state is reason
    assert all(value.value is None for value in missing.cells[0].values)
    with pytest.raises(ValueError, match="conflicts"):
        source.snapshot(
            symbol="EURUSD",
            decision_time_ns=snapshot.decision_time_ns,
            policy=snapshot.policy,
            availability=snapshot.availability,
            absences=(declaration,),
        )


@pytest.mark.parametrize("interval", tuple(STANDARD_DERIVED_BAR_INTERVALS))
@pytest.mark.parametrize("offset", (-1, 0, 1))
def test_independent_closed_boundary_and_contradictory_future_canary(
    interval, offset, monkeypatch
):
    duration = STANDARD_DERIVED_BAR_INTERVALS[interval]
    boundary = BASE + 4 * duration
    policy = BarFeaturePolicyV1(ANTE, intervals=(interval,), scopes=(OBS,))
    times = (
        boundary - 2 * duration,
        boundary - duration - 1,
        boundary - duration,
        boundary - 1,
        boundary,
        boundary + 1,
    )

    def derive(future_price):
        events = tuple(
            _observed(
                index + 1,
                event_time_ns=time,
                bid=float(index + 1 if time < boundary else future_price),
            )
            for index, time in enumerate(times)
        )
        return derive_reconstruction_bars(
            events,
            source_product_manifest_id=MATH_PRODUCT_ID,
            run_id=events[0].run_id,
            ensemble_member_id=events[0].ensemble_member_id,
            policy=DerivedBarPolicyV1(intervals=(interval,), scopes=(OBS,)),
        )

    bars = derive(9999)
    changed_future = derive(999999)
    source = BarFeatureSourceV1("unit-replayed-source", "unit-replayed-bars")

    def build(values):
        monkeypatch.setattr(
            BarFeatureSourceV1,
            "_verified_evidence",
            lambda self, symbol, policy: (
                MATH_PRODUCT_ID,
                MATH_BAR_MANIFEST_ID,
                values,
            ),
        )
        clocks = tuple(
            BarAvailabilityDeclarationV1(
                bar.bar_id,
                bar.bar_end_ns,
                bar.bar_end_ns,
                BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
                "explicit unit boundary clock",
            )
            for bar in values
        )
        return source.snapshot(
            symbol="EURUSD",
            decision_time_ns=boundary + offset,
            policy=policy,
            availability=clocks,
        )

    snapshot = build(bars)
    independently_selected_end = ((boundary + offset) // duration) * duration
    assert snapshot.cells[0].bar_end_ns == independently_selected_end
    assert (
        snapshot.cells[0].bar_start_ns == independently_selected_end - duration
    )
    assert snapshot.cells[0].feature("bid_close").value == (
        2.0 if offset == -1 else 4.0
    )
    assert all(
        bar.last_event_time_ns < independently_selected_end
        for bar in snapshot.bars
    )
    assert snapshot.to_json() == build(changed_future).to_json()
    future = next(bar for bar in bars if bar.bar_start_ns == boundary)
    with pytest.raises(ValueError, match="dependency span|before its end"):
        forged_clock = BarAvailabilityDeclarationV1(
            future.bar_id,
            boundary + offset,
            boundary + offset,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "contradictory future assertion",
        )
        replace(
            snapshot,
            bars=(*snapshot.bars, future),
            availability=tuple(
                sorted(
                    (*snapshot.availability, forged_clock),
                    key=lambda item: item.bar_id,
                )
            ),
        )


def test_actual_stored_future_bar_and_late_outage_are_invisible(
    published_source,
):
    source, _, _ = published_source
    available = _published_snapshot(source)
    cutoff = available.decision_time_ns - 1
    before = source.snapshot(
        symbol="EURUSD", decision_time_ns=cutoff, policy=available.policy
    )
    with_future = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=cutoff,
        policy=available.policy,
        availability=available.availability,
    )
    assert before.to_json() == with_future.to_json()
    assert not with_future.bars
    declaration = BarAbsenceDeclarationV1(
        "EURUSD",
        OBS,
        "1m",
        before.cells[0].bar_start_ns,
        cutoff + 1,
        BarFeatureState.SOURCE_OUTAGE,
        "future outage explanation",
    )
    late = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=cutoff,
        policy=available.policy,
        availability=available.availability,
        absences=(declaration,),
    )
    assert late.to_json() == before.to_json()


def test_budget_refusal_precedes_deep_source_reads(
    published_source, monkeypatch
):
    from histdatacom.synthetic import bar_features

    source, _, _ = published_source

    def forbidden(*args, **kwargs):
        pytest.fail(
            "deep verification ran before declared total budget refusal"
        )

    monkeypatch.setattr(
        bar_features, "verify_reconstruction_publication", forbidden
    )
    monkeypatch.setattr(
        bar_features, "verify_derived_bar_publication", forbidden
    )
    for policy in (
        BarFeaturePolicyV1(POST, max_source_events=1),
        BarFeaturePolicyV1(POST, max_source_bars=1),
    ):
        with pytest.raises(ValueError, match="total source"):
            source.verified_bars("EURUSD", policy)


def test_unavailable_snapshot_still_binds_exact_verified_source_products(
    published_source,
):
    source, product, _ = published_source
    available = _published_snapshot(source)
    missing = source.snapshot(
        symbol="EURUSD",
        decision_time_ns=available.decision_time_ns,
        policy=available.policy,
    )
    assert missing.source_product_manifest_id == product.manifest.manifest_id
    assert missing.derived_bar_manifest_id == available.derived_bar_manifest_id
    assert not missing.bars
    different = replace(missing, source_product_manifest_id=MATH_PRODUCT_ID)
    assert missing.artifact_id != different.artifact_id
    with pytest.raises(ValueError, match="does not match replay"):
        source.verify_snapshot(different, information_mode=POST)
    with pytest.raises(ValueError, match="different source product"):
        replace(available, source_product_manifest_id=MATH_PRODUCT_ID)
    altered_bar_manifest = replace(
        missing, derived_bar_manifest_id=MATH_BAR_MANIFEST_ID
    )
    with pytest.raises(ValueError, match="does not match replay"):
        source.verify_snapshot(altered_bar_manifest, information_mode=POST)

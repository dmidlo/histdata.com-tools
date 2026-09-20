"""Native timeframe/async-triangle consumers over actual committed fixtures."""

from dataclasses import replace

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    training_json,
    training_load,
)
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapBatchV1,
    TrainingOverlapGeometryV1,
)
from histdatacom.data_quality.training_overlap_sources import (
    verify_overlap_source,
)
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
    replay_training_overlap,
)
from histdatacom.synthetic.bar_features import BarFeaturePolicyV1
from histdatacom.synthetic.bars import STANDARD_DERIVED_BAR_INTERVALS
from histdatacom.synthetic.cross_currency import CrossCurrencyJoinPolicy
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_bar_features import (
    TriangleBarPolicyV1,
    TriangleQuoteTupleV1,
)
from tests.fixtures.training_overlap_v1 import BASE, overlap_fixture


@pytest.fixture(scope="module")
def native(tmp_path_factory):
    return overlap_fixture(
        tmp_path_factory.mktemp("overlap-native"),
        native=True,
        multiple_splits=True,
        asynchronous=True,
    )


def test_native_seven_timeframes_and_asynchronous_triangle_are_actually_consumed(
    native,
):
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(
            InformationMode.EX_POST_RECONSTRUCTION,
            allow_prior_generated_state=True,
        ),
        BASE,
        join_policy=CrossCurrencyJoinPolicy.NEAREST_PRIOR_BOUNDED_NO_FORWARD_FILL,
        max_quote_age_ns=2,
    )
    plan = replace(native, triangle_policy_json=training_json(policy.to_dict()))
    batch = materialize_training_overlap(plan)
    assert len(batch.native) == len(plan.geometry.intervals) * 6
    bars = [
        training_load(text)
        for result in batch.native
        for text in result.bars_json
    ]
    assert {bar["interval_code"] for bar in bars} == set(
        STANDARD_DERIVED_BAR_INTERVALS
    )
    tuples = [
        TriangleQuoteTupleV1.from_json(text)
        for result in batch.native
        for text in result.triangle_tuples_json
    ]
    assert any(max(item.ages_ns) == 2 for item in tuples)
    windows = {w.window_id: w for w in batch.windows}
    by_id = {c.coordinate_id: c for c in batch.coordinates}
    for result in batch.native:
        window = windows[result.window_id]
        assert result.feature_cutoff_ns == window.end_ns
        assert result.dependency_end_ns >= result.feature_cutoff_ns
        assert all(
            window.start_ns <= by_id[c].event_time_ns < window.end_ns
            for c in result.event_coordinate_ids
        )
    assert {
        w.partition.value
        for w in batch.windows
        if any(row.window_id == w.window_id for row in batch.rows)
    } == {"train", "validation", "test"}
    assert all(
        row.analytical_cutoff_ns
        >= windows[row.window_id].end_ns
        > row.event_time_ns
        for row in batch.rows
    )


def test_native_observed_anchor_aliases_have_one_immutable_coordinate(native):
    source = verify_overlap_source(native)
    raw = [
        c
        for c in source.coordinates
        if "source_row_id" in training_load(c.value_json)
        and training_load(c.value_json)["source_row_id"] is not None
    ]
    assert len(raw) == 18
    occurrences = [
        o for o in source.occurrences if o.coordinate_id == raw[0].coordinate_id
    ]
    assert len(occurrences) == 3  # original + two exact native member aliases
    assert sum(o.product_manifest_id is not None for o in occurrences) == 2
    assert all(o.coordinate_id == raw[0].coordinate_id for o in occurrences)


def test_resealed_native_value_change_fails_fresh_publication_replay(native):
    plan = replace(native, native_intervals=("1d",))
    result = materialize_training_overlap(plan)
    chosen = next(n for n in result.native if n.bars_json)
    false = training_load(chosen.bars_json[0])
    false["bid_close"] = 123.0
    altered = replace(
        chosen, bars_json=(training_json(false), *chosen.bars_json[1:])
    )
    forged = replace(
        result,
        native=tuple(altered if n == chosen else n for n in result.native),
    )
    assert TrainingOverlapBatchV1.from_json(forged.to_json()) == forged
    with pytest.raises(ValueError, match="complete source/native/math replay"):
        replay_training_overlap(forged)


def test_generated_prefix_cannot_use_whole_product_future_conditioning(native):
    # The generated middle at .65day precedes child start .66day, but its
    # immutable right anchor at .7day is still future. Its prior-window terminal
    # state is not admissible as a child initializer.
    lo = BASE + 66 * DAY_NS // 100
    plan = replace(
        native,
        geometry=TrainingOverlapGeometryV1(
            lo, lo + DAY_NS // 100, DAY_NS // 100, DAY_NS // 100
        ),
        carry_lookback_ns=DAY_NS // 10,
        native_intervals=("1d",),
    )
    result = materialize_training_overlap(plan)
    assert any(
        c.state == "unavailable_future_conditioning" for c in result.carries
    )
    assert "unavailable_prefix_state" in result.windows[0].reasons
    assert not result.rows


def test_shuffled_verified_worker_results_preserve_native_and_parent_identity(
    native, monkeypatch
):
    from histdatacom.data_quality import (
        training_overlap_sources as implementation,
    )

    plan = replace(native, native_intervals=("1d",))
    expected = materialize_training_overlap(plan)
    original = implementation.verify_training_ownership

    def reordered(source, ownership):
        verified = original(source, ownership)
        return replace(
            verified,
            observed=tuple(reversed(verified.observed)),
            products=tuple(reversed(verified.products)),
        )

    monkeypatch.setattr(implementation, "verify_training_ownership", reordered)
    actual = materialize_training_overlap(plan)
    assert actual.to_json() == expected.to_json()


def test_complete_native_window_bound_refuses_before_native_execution(
    native, monkeypatch
):
    from histdatacom.data_quality import (
        training_overlap_sources as implementation,
    )

    plan = replace(
        native,
        geometry=TrainingOverlapGeometryV1(
            BASE, BASE + 3 * DAY_NS, DAY_NS, DAY_NS // 16
        ),
        native_intervals=(),
    )
    monkeypatch.setattr(
        implementation,
        "derive_reconstruction_bars",
        lambda *args, **kwargs: pytest.fail("native execution must not start"),
    )
    with pytest.raises(ValueError, match="window/product replay inventory"):
        materialize_training_overlap(plan)

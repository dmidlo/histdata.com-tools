"""Real observed enrichment and delayed origin-aware native feature planes."""

from dataclasses import replace

import polars as pl
import pytest

from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.data_quality.triangle_training_features import (
    append_triangle_features_to_reconstruction_rows,
    enrich_tick_cache_with_triangle_features,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_projection_features import (
    derive_triangle_projection_features,
)
from tests.fixtures.triangle_bar_features_v1 import POST, published_triangle


@pytest.fixture
def published(tmp_path):
    return published_triangle(tmp_path)


def test_actual_observed_enrichment_row_identity_and_refusal(published):
    source, full, _, delivered = published
    event = next(
        e
        for s in delivered.streams
        if s.symbol == "eurusd"
        for e in reversed(s.events)
        if e.origin.value == "observed"
    )
    policy = replace(
        full.policy,
        bar_policy=replace(
            full.policy.bar_policy,
            scopes=(ActivitySliceScope.OBSERVED,),
            information_mode=InformationMode.EX_ANTE_SIMULATION,
        ),
    )
    snapshot = source.snapshot(
        decision_time_ns=event.event_time_ns,
        policy=policy,
        availability=tuple(
            a for s in full.leg_snapshots for a in s.availability
        ),
        strata=full.strata,
    )
    frame = pl.DataFrame(
        {
            "datetime": [event.event_time_ns // 1_000_000] * 2,
            "bid": [event.bid] * 2,
            "ask": [event.ask] * 2,
            "vol": [0.0] * 2,
        }
    )
    baseline = enrich_tick_cache_with_training_features(
        frame,
        symbol="EURUSD",
        data_format="ascii",
        timeframe="T",
        period="202410",
    )
    enriched = enrich_tick_cache_with_triangle_features(
        frame,
        source=source,
        snapshots=(snapshot,),
        row_event_ids=(event.event_id,) * 2,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        symbol="EURUSD",
        period="202410",
    )
    assert enriched.select(baseline.columns).equals(baseline)
    assert enriched.height == 2
    assert (
        enriched["triangle_bar_v1__source_event_id"].to_list()
        == [event.event_id] * 2
    )
    assert (
        enriched[
            "triangle_bar_v1__observed__1m__residual_close__value"
        ].null_count()
        == 0
    )
    assert (
        enriched["triangle_bar_v1__historical_availability_verified"].to_list()
        == [False] * 2
    )
    for bad in (
        frame.with_columns(pl.col("bid") + 1),
        frame.with_columns(pl.col("datetime") + 1),
    ):
        with pytest.raises(ValueError, match="exact source event"):
            enrich_tick_cache_with_triangle_features(
                bad,
                source=source,
                snapshots=(snapshot,),
                row_event_ids=(event.event_id,) * 2,
                information_mode=InformationMode.EX_ANTE_SIMULATION,
                symbol="EURUSD",
                period="202410",
            )
    with pytest.raises(ValueError, match="overwritten"):
        enrich_tick_cache_with_triangle_features(
            enriched,
            source=source,
            snapshots=(snapshot,),
            row_event_ids=(event.event_id,) * 2,
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            symbol="EURUSD",
            period="202410",
        )


def test_actual_delayed_native_projection_preserves_every_origin_column(
    published,
):
    source, snapshot, evidence, delivered = published
    projection = derive_triangle_projection_features(
        source, snapshot, information_mode=POST, evidence=evidence
    )
    events = [e for s in delivered.streams for e in s.events][-3:]
    # Duplicate input rows are retained exactly, never introduced or weighted.
    events = [events[1], events[0], events[1], events[2]]
    frame = pl.DataFrame(
        [
            {**e.to_dict(), "decision_time_ns": snapshot.decision_time_ns}
            for e in events
        ]
    )
    result = append_triangle_features_to_reconstruction_rows(
        frame, source=source, snapshots=(snapshot,), projections=(projection,)
    )
    assert result.select(frame.columns).equals(frame)
    assert result.select(frame.columns).schema == frame.schema
    assert result.height == frame.height
    assert result["triangle_bar_v1__source_event_id"].to_list() == [
        e.event_id for e in events
    ]
    assert result["triangle_bar_v1__delayed_evidence"].to_list() == [True] * 4
    assert (
        result[
            "triangle_bar_v1__synthetic__1m__projection__mean_normalized_burden__value"
        ].min()
        > 0
    )
    assert (
        result["triangle_bar_v1__projection_window_id"].to_list()
        == [evidence.window.window_id] * 4
    )
    for bad in (
        frame.drop("source_series_id"),
        frame.with_columns(pl.col("bid") + 1),
        frame.with_columns(pl.col("event_time_ns") - 1),
        frame.with_columns(pl.lit(0).alias("decision_time_ns")),
    ):
        with pytest.raises(ValueError, match="native training row"):
            append_triangle_features_to_reconstruction_rows(
                bad,
                source=source,
                snapshots=(snapshot,),
                projections=(projection,),
            )


def test_unavailable_training_cells_keep_exact_nullable_schema(published):
    source, snapshot, _, delivered = published
    event = delivered.streams[0].events[-1]
    missing = source.snapshot(
        decision_time_ns=snapshot.decision_time_ns, policy=snapshot.policy
    )
    frame = pl.DataFrame(
        [{**event.to_dict(), "decision_time_ns": snapshot.decision_time_ns}]
    )
    populated = append_triangle_features_to_reconstruction_rows(
        frame, source=source, snapshots=(snapshot,)
    )
    empty = append_triangle_features_to_reconstruction_rows(
        frame, source=source, snapshots=(missing,)
    )
    assert empty.schema == populated.schema
    assert empty[
        "triangle_bar_v1__merged__1m__residual_close__value"
    ].to_list() == [None]
    assert empty[
        "triangle_bar_v1__merged__1m__residual_close__state"
    ].to_list() == ["missing_or_partial_leg"]


def test_wide_output_budget_refuses_before_source_replay(monkeypatch):
    from histdatacom.data_quality import triangle_training_features as module
    from tests.fixtures.triangle_bar_features_v1 import math_snapshot

    snapshot = math_snapshot()
    frame = pl.DataFrame({"event_id": ["unread"] * 4096})

    def forbidden(*args, **kwargs):
        pytest.fail("source replay ran before output budget refusal")

    monkeypatch.setattr(module, "_verify_inputs", forbidden)
    with pytest.raises(ValueError, match="output-cell budget"):
        module.append_triangle_features_to_reconstruction_rows(
            frame, source=None, snapshots=(snapshot,)
        )


@pytest.mark.parametrize("member", ("m" * 100_000, "😀" * 3_000))
def test_large_legacy_member_metadata_refuses_before_output_allocation(
    monkeypatch,
    member,
):
    from histdatacom.data_quality import triangle_training_features as module
    from tests.fixtures.triangle_bar_features_v1 import (
        BASE,
        math_events,
        math_snapshot,
    )

    snapshot = math_snapshot(cutoff=BASE)
    event = replace(math_events()[0], ensemble_member_id=member, event_id="")
    module._preflight_output(500, (snapshot,), ())

    def forbidden(*args, **kwargs):
        pytest.fail("output allocation preceded metadata budget refusal")

    monkeypatch.setattr(pl, "DataFrame", forbidden)
    with pytest.raises(ValueError, match="output-byte budget"):
        module._extension([(event, BASE)] * 500, {BASE: snapshot}, {})

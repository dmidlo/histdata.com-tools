"""Exact retained-event projection bridge, never aggregate prorating."""

from dataclasses import replace
import json

import pytest

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bars import STANDARD_DERIVED_BAR_INTERVALS
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionCellV1,
    TriangleProjectionEvidenceV1,
    TriangleProjectionPointV1,
    TriangleProjectionSnapshotV1,
    derive_triangle_projection_features,
)
from tests.fixtures.triangle_bar_features_v1 import (
    BASE,
    POST,
    published_triangle,
)


@pytest.mark.parametrize("interval", tuple(STANDARD_DERIVED_BAR_INTERVALS))
def test_positive_projection_all_widths_and_scope_states(tmp_path, interval):
    source, snapshot, evidence, _ = published_triangle(tmp_path, interval)
    result = derive_triangle_projection_features(
        source, snapshot, information_mode=POST, evidence=evidence
    )
    _, delivered = evidence.replay()
    pre = {
        (e.symbol, e.event_time_ns, e.event_sequence): e
        for s in evidence.proposals
        for e in s.events
    }
    post = {
        (e.symbol, e.event_time_ns, e.event_sequence): e
        for s in delivered.streams
        for e in s.events
    }
    for cell in result.cells:
        if cell.scope is ActivitySliceScope.OBSERVED:
            assert cell.state == "not_applicable_observed"
            assert all(v is None for v in cell.summary().values())
            continue
        assert cell.state == "available"
        assert len(cell.points) == 2
        assert cell.summary()["projected_tuple_count"] == 1
        assert cell.summary()["projected_rate"] == 0.5
        assert cell.points[0].normalized_burden > 0
        assert cell.points[1].normalized_burden == 0
        assert (
            cell.summary()["mean_normalized_burden"]
            == cell.points[0].normalized_burden / 2
        )
        assert cell.available_at_ns == snapshot.decision_time_ns
        for point in cell.points:
            before = [
                e for e in pre.values() if e.event_id in point.pre_event_ids
            ]
            after = [
                post[(e.symbol, e.event_time_ns, e.event_sequence)]
                for e in before
            ]
            displacement = sum(
                abs(a.bid - b.bid) + abs(a.ask - b.ask)
                for a, b in zip(before, after, strict=True)
            )
            scale = sum(max(e.ask - e.bid, 1e-12) for e in before)
            assert point.normalized_burden == displacement / scale
            assert point.spread_scale == scale
            assert cell.bar_start_ns <= point.probe_time_ns < cell.bar_end_ns
            assert len(point.pre_event_ids) == len(point.post_event_ids) == 3
    assert TriangleProjectionSnapshotV1.from_json(result.to_json()) == result
    assert (
        TriangleProjectionEvidenceV1.from_json(evidence.to_json()) == evidence
    )
    cutoff = BASE + 3 * STANDARD_DERIVED_BAR_INTERVALS[interval]
    earlier = source.snapshot(
        decision_time_ns=cutoff,
        policy=snapshot.policy,
        availability=tuple(
            a for s in snapshot.leg_snapshots for a in s.availability
        ),
        strata=snapshot.strata,
    )
    assert all(c.bar_end_ns == cutoff for c in earlier.cells)
    assert all(
        json.loads(text)["event_time_ns"] < cutoff
        for text in earlier.event_json
    )


def test_unavailable_future_evidence_does_not_change_snapshot_identity(
    tmp_path,
):
    source, snapshot, evidence, _ = published_triangle(tmp_path)
    absent = derive_triangle_projection_features(
        source, snapshot, information_mode=POST
    )
    future = replace(evidence, known_at_ns=snapshot.decision_time_ns + 1)
    late = derive_triangle_projection_features(
        source, snapshot, information_mode=POST, evidence=future
    )
    assert late == absent
    assert late.artifact_id == absent.artifact_id
    assert {c.state for c in late.cells} == {
        "not_applicable_observed",
        "unavailable_retained_evidence",
    }
    bad = replace(evidence, delivery_profile_id="wrong:delivery")
    with pytest.raises(ValueError, match="committed delivery identity"):
        derive_triangle_projection_features(
            source, snapshot, information_mode=POST, evidence=bad
        )


@pytest.mark.parametrize(
    "target", ("run_json", "window_json", "config_json", "proposal_stream_json")
)
def test_legacy_unknown_field_and_coercion_refusal(tmp_path, target):
    _, _, evidence, _ = published_triangle(tmp_path)
    text = getattr(evidence, target)
    payload = json.loads(text[0] if target == "proposal_stream_json" else text)
    payload["unrecognized_future_field"] = "later"
    changed = json.dumps(payload)
    with pytest.raises(ValueError, match="canonical"):
        replace(
            evidence,
            **{
                target: (
                    (changed,) + text[1:]
                    if target == "proposal_stream_json"
                    else changed
                )
            },
        )
    if target == "proposal_stream_json":
        payload.pop("unrecognized_future_field")
        payload["events"][0]["bid"] = str(payload["events"][0]["bid"])
        with pytest.raises(ValueError):
            replace(
                evidence, proposal_stream_json=(json.dumps(payload),) + text[1:]
            )


def test_projection_bin_ownership_uses_actual_probe_not_window_mean(tmp_path):
    source, snapshot, evidence, _ = published_triangle(tmp_path)
    result = derive_triangle_projection_features(
        source, snapshot, information_mode=POST, evidence=evidence
    )
    cell = result.cells[-1]
    assert all(
        p.probe_time_ns >= BASE + 3 * STANDARD_DERIVED_BAR_INTERVALS["1m"]
        for p in cell.points
    )
    assert sum(len(s.events) for s in evidence.proposals) > sum(
        len(p.pre_event_ids) for p in cell.points
    )
    with pytest.raises(ValueError, match="another bar"):
        replace(
            cell,
            bar_start_ns=cell.bar_end_ns,
            bar_end_ns=cell.bar_end_ns + 60_000_000_000,
        )


def test_unrepresentable_projection_total_refuses_boundedly():
    ids = tuple("event:sha256:" + char * 64 for char in "abc")
    point = TriangleProjectionPointV1(
        BASE + 1, ids, ids, 0.0, 0.0, 0.0, 0.0, 1e308, 1e308, 1.0, True
    )
    with pytest.raises(
        ValueError, match="aggregate totals are unrepresentable"
    ):
        TriangleProjectionCellV1(
            ActivitySliceScope.SYNTHETIC,
            "1m",
            BASE,
            BASE + 60_000_000_000,
            "available",
            BASE + 60_000_000_000,
            (point, point),
        )


def test_combined_envelope_budget_precedes_retained_engine_replay(
    tmp_path, monkeypatch
):
    from histdatacom.synthetic.bar_features import (
        MAX_BAR_FEATURE_ARTIFACT_BYTES,
    )

    _, _, evidence, _ = published_triangle(tmp_path)

    def forbidden(*args, **kwargs):
        pytest.fail("engine replay preceded aggregate envelope byte guard")

    monkeypatch.setattr(TriangleProjectionEvidenceV1, "replay", forbidden)
    with pytest.raises(ValueError, match="byte limit"):
        replace(evidence, config_json=" " * MAX_BAR_FEATURE_ARTIFACT_BYTES)

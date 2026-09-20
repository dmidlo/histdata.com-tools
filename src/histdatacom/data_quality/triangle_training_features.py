"""Opt-in triangle feature planes; no #605 corpus/weight/eligibility claim."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import canonical_bar_feature_json
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    load_reconstruction_manifest,
    read_reconstruction_streams,
)
from histdatacom.synthetic.triangle_bar_features import (
    MAX_TRIANGLE_EVENTS,
    TriangleBarSnapshotV1,
    TriangleBarSourceV1,
)
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionSnapshotV1,
    derive_triangle_projection_features,
)

TRIANGLE_TRAINING_NAMESPACE = "triangle_bar_v1"
MAX_TRIANGLE_OUTPUT_CELLS = 250_000
MAX_TRIANGLE_OUTPUT_ESTIMATED_BYTES = 32 * 1024 * 1024


def _preflight_output(
    row_count: int,
    snapshots: Sequence[TriangleBarSnapshotV1],
    projections: Sequence[TriangleProjectionSnapshotV1],
    *,
    metadata_characters: int = 0,
) -> None:
    """Conservative extension-only cell/string budget before source replay."""
    if not 1 <= len(snapshots) <= MAX_TRIANGLE_EVENTS or len(projections) > len(
        snapshots
    ):
        raise ValueError("triangle training input cardinality is invalid")
    max_columns = max(
        16 + 5 * sum(len(c.values) for c in s.cells) for s in snapshots
    )
    if projections:
        max_columns += max(14 * len(p.cells) for p in projections)
    if row_count * max_columns > MAX_TRIANGLE_OUTPUT_CELLS:
        raise ValueError(
            "triangle projected output-cell budget exceeded; use smaller chunks"
        )
    # Lineage IDs are ASCII. Astral legacy metadata needs up to twelve ASCII
    # bytes per Python character under ensure_ascii JSON. Fixed scalar/key/column
    # allowance is conservative; this is a wire-equivalent estimate, not an
    # assertion about allocator-dependent resident memory.
    max_lineage = max(
        sum(
            6 * sum(len(i) + 4 for i in v.source_bar_ids + v.source_event_ids)
            for c in s.cells
            for v in c.values
        )
        for s in snapshots
    )
    if projections:
        max_lineage += max(
            sum(
                6 * sum(len(point.artifact_id) + 4 for point in c.points)
                for c in p.cells
            )
            for p in projections
        )
    if (
        row_count * (max_columns * 256 + max_lineage) + 12 * metadata_characters
        > MAX_TRIANGLE_OUTPUT_ESTIMATED_BYTES
    ):
        raise ValueError(
            "triangle projected output-byte budget exceeded; use smaller chunks"
        )


def _verify_inputs(
    source: TriangleBarSourceV1,
    snapshots: Sequence[TriangleBarSnapshotV1],
    information_mode: InformationMode,
    projections: Sequence[TriangleProjectionSnapshotV1],
) -> tuple[
    dict[int, TriangleBarSnapshotV1],
    dict[int, TriangleProjectionSnapshotV1],
    dict[str, SyntheticEventV1],
]:
    if not 1 <= len(snapshots) <= MAX_TRIANGLE_EVENTS or len(projections) > len(
        snapshots
    ):
        raise ValueError("triangle training input cardinality is invalid")
    if len({s.policy.artifact_id for s in snapshots}) != 1:
        raise ValueError("triangle training requires one exact feature policy")
    indexed = {}
    for snapshot in snapshots:
        source.verify_snapshot(snapshot, information_mode=information_mode)
        if snapshot.decision_time_ns in indexed:
            raise ValueError("triangle training cutoff ownership is ambiguous")
        indexed[snapshot.decision_time_ns] = snapshot
    projected = {}
    for projection in projections:
        cutoff = projection.triangle.decision_time_ns
        if cutoff in projected or indexed.get(cutoff) != projection.triangle:
            raise ValueError(
                "triangle projection belongs to another training snapshot"
            )
        expected = derive_triangle_projection_features(
            source,
            projection.triangle,
            information_mode=information_mode,
            evidence=projection.evidence,
        )
        if expected != projection:
            raise ValueError("triangle training projection replay differs")
        projected[cutoff] = projection
    declared = load_reconstruction_manifest(
        source.bar_source.reconstruction_manifest_path
    )
    if declared.event_count > snapshots[0].policy.bar_policy.max_source_events:
        raise ValueError("triangle training total source event budget exceeded")
    if (
        declared.manifest_id
        != snapshots[0].leg_snapshots[0].source_product_manifest_id
    ):
        raise ValueError("triangle training source identity changed")
    events = {
        e.event_id: e
        for s in read_reconstruction_streams(
            source.bar_source.reconstruction_manifest_path
        )
        for e in s.events
    }
    return indexed, projected, events


def _extension(
    rows: Sequence[tuple[SyntheticEventV1, int]],
    snapshots: dict[int, TriangleBarSnapshotV1],
    projections: dict[int, TriangleProjectionSnapshotV1],
) -> Any:
    import polars as pl

    _preflight_output(
        len(rows),
        tuple(snapshots.values()),
        tuple(projections.values()),
        metadata_characters=sum(
            len(event.run_id) + len(event.ensemble_member_id)
            for event, _ in rows
        ),
    )
    if {cutoff for _, cutoff in rows} != set(snapshots):
        raise ValueError(
            "triangle training requires exact row-cutoff ownership without orphan snapshots"
        )
    output = []
    prefix = TRIANGLE_TRAINING_NAMESPACE + "__"
    for event, cutoff in rows:
        snapshot = snapshots[cutoff]
        projection = projections.get(cutoff)
        row: dict[str, object] = {
            prefix + "snapshot_id": snapshot.artifact_id,
            prefix + "policy_id": snapshot.policy.artifact_id,
            prefix
            + "source_product_manifest_id": snapshot.leg_snapshots[
                0
            ].source_product_manifest_id,
            prefix + "source_event_id": event.event_id,
            prefix + "event_time_ns": event.event_time_ns,
            prefix + "decision_time_ns": cutoff,
            prefix + "origin": event.origin.value,
            prefix + "run_id": event.run_id,
            prefix + "ensemble_member_id": event.ensemble_member_id,
            prefix
            + "information_mode": snapshot.policy.bar_policy.information_mode.value,
            prefix + "historical_availability_verified": False,
            prefix + "delayed_evidence": cutoff != event.event_time_ns,
            prefix
            + "replay_clock_assumption": any(
                s.uses_replay_clock for s in snapshot.leg_snapshots
            ),
            prefix
            + "projection_snapshot_id": (
                projection.artifact_id if projection else None
            ),
            prefix
            + "projection_window_id": (
                projection.evidence.window.window_id
                if projection and projection.evidence
                else None
            ),
            prefix
            + "projection_evidence_id": (
                projection.evidence.artifact_id
                if projection and projection.evidence
                else None
            ),
        }
        for cell in snapshot.cells:
            cell_prefix = (
                prefix + cell.scope.value + "__" + cell.interval_code + "__"
            )
            for value in cell.values:
                name = cell_prefix + value.name + "__"
                row[name + "value"] = value.value
                row[name + "state"] = value.state
                row[name + "available_at_ns"] = value.available_at_ns
                row[name + "source_bar_ids_json"] = canonical_bar_feature_json(
                    {"ids": list(value.source_bar_ids)}
                )
                row[name + "source_event_ids_json"] = (
                    canonical_bar_feature_json(
                        {"ids": list(value.source_event_ids)}
                    )
                )
        if projection:
            for projection_cell in projection.cells:
                cell_prefix = (
                    prefix
                    + projection_cell.scope.value
                    + "__"
                    + projection_cell.interval_code
                    + "__projection__"
                )
                row[cell_prefix + "state"] = projection_cell.state
                row[cell_prefix + "available_at_ns"] = (
                    projection_cell.available_at_ns
                )
                row[cell_prefix + "point_ids_json"] = (
                    canonical_bar_feature_json(
                        {"ids": [p.artifact_id for p in projection_cell.points]}
                    )
                )
                for name, summary_value in projection_cell.summary().items():
                    row[cell_prefix + name + "__value"] = (
                        None if summary_value is None else float(summary_value)
                    )
        output.append(row)
    if len({tuple(row) for row in output}) != 1:
        raise ValueError("triangle rows require consistent projection columns")
    schema = {}
    for name in output[0]:
        if name.endswith(
            (
                "__historical_availability_verified",
                "__delayed_evidence",
                "__replay_clock_assumption",
            )
        ):
            schema[name] = pl.Boolean
        elif name.endswith(
            ("__event_time_ns", "__decision_time_ns", "__available_at_ns")
        ):
            schema[name] = pl.Int64
        elif name.endswith(
            ("__quote_age_max_ns__value", "__endpoint_age_max_ns__value")
        ):
            schema[name] = pl.Int64
        elif name.endswith("__value"):
            schema[name] = pl.Float64
        else:
            schema[name] = pl.String
    return pl.DataFrame(output, schema_overrides=schema)


def _guard_frame(frame: Any) -> None:
    if not 1 <= frame.height <= MAX_TRIANGLE_EVENTS:
        raise ValueError(
            "triangle training frame exceeds its bounded row count"
        )
    if any(
        name.startswith(TRIANGLE_TRAINING_NAMESPACE + "__")
        for name in frame.columns
    ):
        raise ValueError("triangle training columns cannot be overwritten")


def enrich_tick_cache_with_triangle_features(
    frame: Any,
    *,
    source: TriangleBarSourceV1,
    snapshots: Sequence[TriangleBarSnapshotV1],
    row_event_ids: Sequence[str],
    information_mode: InformationMode,
    symbol: str,
    period: str,
) -> Any:
    """Run existing observed enrichment, then append exact-timestamp state.

    Exact source event IDs also bind bid/ask and row timestamps. The added
    namespace does not make older diagnostic enrichment columns causal.
    """
    _guard_frame(frame)
    _preflight_output(frame.height, snapshots, ())
    if len(row_event_ids) != frame.height:
        raise ValueError("observed rows require exact source event ownership")
    indexed, projections, events = _verify_inputs(
        source, snapshots, information_mode, ()
    )
    if any(
        s.policy.bar_policy.scopes != (ActivitySliceScope.OBSERVED,)
        for s in snapshots
    ):
        raise ValueError(
            "observed training admits only observed triangle scopes"
        )
    enriched = enrich_tick_cache_with_training_features(
        frame, symbol=symbol, data_format="ascii", timeframe="T", period=period
    )
    rows = []
    for row, event_id in zip(
        enriched.iter_rows(named=True), row_event_ids, strict=True
    ):
        event = events.get(event_id)
        timestamp = row["timestamp_utc_ms"]
        if (
            type(timestamp) is not int
            or event is None
            or event.origin is not SyntheticEventOrigin.OBSERVED
            or event.symbol.upper() != symbol
            or event.event_time_ns != timestamp * 1_000_000
            or event.bid != row["bid"]
            or event.ask != row["ask"]
        ):
            raise ValueError(
                "observed training row differs from exact source event"
            )
        rows.append((event, event.event_time_ns))
    return enriched.hstack(_extension(rows, indexed, projections))


def append_triangle_features_to_reconstruction_rows(
    frame: Any,
    *,
    source: TriangleBarSourceV1,
    snapshots: Sequence[TriangleBarSnapshotV1],
    projections: Sequence[TriangleProjectionSnapshotV1] = (),
) -> Any:
    """Append ex-post state to exact native rows at explicit decision clocks.

    All native event columns must be retained unchanged. A later decision is
    allowed and flagged; it never backdates projection knowledge to event time.
    No row replication, sample weight, training eligibility or split is added.
    """
    _guard_frame(frame)
    _preflight_output(frame.height, snapshots, projections)
    indexed, projected, events = _verify_inputs(
        source, snapshots, InformationMode.EX_POST_RECONSTRUCTION, projections
    )
    rows = []
    for row in frame.iter_rows(named=True):
        cutoff = row.get("decision_time_ns")
        event = events.get(row.get("event_id"))
        if (
            event is None
            or type(cutoff) is not int
            or cutoff < event.event_time_ns
        ):
            raise ValueError(
                "native training row requires exact source and non-early decision clock"
            )
        if any(
            name not in row
            or row[name] != value
            or type(row[name]) is not type(value)
            for name, value in event.to_dict().items()
        ):
            raise ValueError(
                "native training row altered immutable event provenance"
            )
        rows.append((event, cutoff))
    return frame.hstack(_extension(rows, indexed, projected))


__all__ = [
    "TRIANGLE_TRAINING_NAMESPACE",
    "enrich_tick_cache_with_triangle_features",
    "append_triangle_features_to_reconstruction_rows",
]

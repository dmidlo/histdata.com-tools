"""Executed quote, closed-bar, triangle and committed-activity adapters."""

from __future__ import annotations

import bisect
import math

from histdatacom.synthetic.activity import (
    ReconstructionActivityManifestV1,
    summarize_committed_reconstruction_activity,
)
from histdatacom.synthetic.bar_features import (
    BarFeatureSourceV1,
    CausalBarSnapshotV1,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.triangle_bar_features import (
    TriangleBarSnapshotV1,
    TriangleBarSourceV1,
)
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionSnapshotV1,
    derive_triangle_projection_features,
)

from .training_contracts import training_json, training_load
from .training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinMeaning,
    JoinState,
    TrainingJoinColumnV1,
    TrainingJoinSourceV1,
)
from .training_join_sources import (
    _JoinAdapter,
    _JoinRecord,
    _bound_file,
    _configuration,
    _hash,
    _prefix,
    _semantics,
)
from .training_lineage import _VerifiedSource


def _native_state(state: str) -> JoinState:
    if state == "available":
        return JoinState.AVAILABLE
    if state in ("unsupported", "not_applicable_observed"):
        return (
            JoinState.UNSUPPORTED
            if state == "unsupported"
            else JoinState.NOT_APPLICABLE
        )
    if state == "insufficient_warmup":
        return JoinState.INSUFFICIENT_WARMUP
    if state == "expected_closure":
        return JoinState.EXPECTED_CLOSURE
    if state == "source_outage":
        return JoinState.SOURCE_OUTAGE
    if state == "missing_bar":
        return JoinState.EMPTY_MARKET_INTERVAL
    return JoinState.UNAVAILABLE


def _market_entity(column: TrainingJoinColumnV1) -> None:
    if any(
        v is not None
        for v in (
            column.entity.currency,
            column.entity.economy,
            column.entity.series,
        )
    ):
        raise ValueError(
            "quote-family source does not prove a macro entity mapping"
        )


def _tick(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    config = _configuration(
        source, keys={"product_manifest_id", "ensemble_member_id"}
    )
    if source.paths or source.evidence_json:
        raise ValueError(
            "quote adapter uses only the complete verified606 inventory"
        )
    product_id = config["product_manifest_id"]
    member_id = config["ensemble_member_id"]
    if (product_id is None) != (member_id is None):
        raise ValueError(
            "native quote selection requires exact product and member"
        )
    # Values retain bid/ask; no unused or superseded quote is numerically changed.
    indexed: dict[str, dict[int, tuple[float, float, str, int, int, str]]] = {}
    partitions: dict[tuple[str, int], tuple[str, str]] = {}
    root_ids: tuple[str, ...]
    if product_id is None:
        for row in verified.observed:
            key = row.symbol, row.event_time_ns
            owner = row.series_id, row.period
            if key in partitions and partitions[key] != owner:
                raise ValueError(
                    "duplicate quote clock has ambiguous physical partition ownership"
                )
            partitions[key] = owner
            # Verified rows sort by physical ordinal after clock. Later ordinal
            # explicitly wins; this is not an artifact-ID/dictionary tie-break.
            indexed.setdefault(row.symbol, {})[row.event_time_ns] = (
                row.bid,
                row.ask,
                "observed-row:sha256:" + _hash(row.payload()),
                row.event_time_ns,
                row.event_time_ns + 1,
                "observed",
            )
        root_ids = (verified.source.dataset_version_id,)
    else:
        products = [
            p for p in verified.products if p.manifest.manifest_id == product_id
        ]
        if len(products) != 1 or type(member_id) is not str:
            raise ValueError(
                "native quote product is outside verified source inventory"
            )
        product = products[0]
        events = sorted(
            (e for e in product.events if e.ensemble_member_id == member_id),
            key=lambda e: (e.symbol, e.event_time_ns, e.event_sequence),
        )
        if not events:
            raise ValueError(
                "native quote member is absent from verified product"
            )
        for event in events:
            indexed.setdefault(event.symbol.upper(), {})[
                event.event_time_ns
            ] = (
                event.bid,
                event.ask,
                event.event_id,
                product.dependency_start_ns,
                product.dependency_end_ns,
                event.origin.value,
            )
        root_ids = (product.manifest.manifest_id, product.root.artifact_id)
    clocks = {
        symbol: tuple(sorted(values)) for symbol, values in indexed.items()
    }

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _market_entity(column)
        _prefix(column, f"market.tick.{column.entity.symbol}.")
        _semantics(column, JoinMeaning.STATE)
        if column.coordinate or column.field not in (
            "bid",
            "ask",
            "midpoint",
            "spread",
        ):
            raise ValueError("unknown native quote field or coordinate")
        if column.direction is JoinDirection.INTERVAL:
            raise ValueError(
                "quote points have no declared interval persistence"
            )
        times = clocks.get(column.entity.symbol, ())
        index = bisect.bisect_right(times, cutoff) - 1
        if index < 0:
            return ()
        time = times[index]
        if column.direction is JoinDirection.EXACT and time != cutoff:
            return ()
        bid, ask, identity, lo, hi, origin = indexed[column.entity.symbol][time]
        if column.field in ("midpoint", "spread") and not (
            0 < bid <= ask and math.isfinite(ask)
        ):
            raise ValueError(
                "selected quote cannot supply an uncrossed derived price"
            )
        value = {
            "bid": bid,
            "ask": ask,
            "midpoint": bid + (ask - bid) / 2,
            "spread": ask - bid,
        }[column.field]
        return (
            _JoinRecord(
                time,
                time,
                time + 1,
                None,
                lo,
                hi,
                (identity,),
                root_ids,
                "histdatacom.training-quote-value.v1",
                origin,
                value,
            ),
        )

    return _JoinAdapter(source, root_ids, select)


def _bar_source(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> BarFeatureSourceV1:
    _configuration(source, keys=set())
    if len(source.paths) != 2 or not source.evidence_json:
        raise ValueError(
            "bar adapter requires product/bar paths and native snapshots"
        )
    if source.paths[0] not in verified.source.product_manifest_paths:
        raise ValueError("bar parent is not in complete606 product ownership")
    for path in source.paths:
        _bound_file(path)
    return BarFeatureSourceV1(*source.paths)


def _coordinate(column: TrainingJoinColumnV1) -> tuple[str, str]:
    _market_entity(column)
    parts = column.coordinate.split(":")
    if len(parts) != 2:
        raise ValueError("bar coordinate requires scope:timeframe")
    return parts[0], parts[1]


def _bars(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    native = _bar_source(source, verified)
    snapshots = tuple(
        CausalBarSnapshotV1.from_json(v) for v in source.evidence_json
    )
    if len({(s.symbol, s.decision_time_ns) for s in snapshots}) != len(
        snapshots
    ):
        raise ValueError("duplicate declared bar snapshot coordinates")
    if len({s.policy.artifact_id for s in snapshots}) != 1:
        raise ValueError("bar source cannot mix feature policies")
    for text, snapshot in zip(source.evidence_json, snapshots):
        if training_json(snapshot.to_dict()) != text:
            raise ValueError("noncanonical native bar evidence")
        native.verify_snapshot(
            snapshot, information_mode=snapshot.policy.information_mode
        )
    by_key = {(s.symbol.upper(), s.decision_time_ns): s for s in snapshots}
    roots = tuple(
        dict.fromkeys(
            i
            for s in snapshots
            for i in (s.source_product_manifest_id, s.derived_bar_manifest_id)
        )
    )
    product = next(
        p
        for p in verified.products
        if p.manifest.manifest_id == snapshots[0].source_product_manifest_id
    )

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        scope, interval = _coordinate(column)
        prefix = (
            "market.bar"
            if source.family is JoinFamily.BAR
            else "market.indicator"
        )
        expected = f"{prefix}.{column.entity.symbol}."
        if source.family is JoinFamily.BAR:
            expected += f"{scope}.{interval}."
        else:
            expected += f"{interval}."
        _prefix(column, expected)
        _semantics(column, JoinMeaning.CLOSED_BAR)
        template = snapshots[0]
        if not any(
            c.scope.value == scope
            and c.interval_code == interval
            and any(v.name == column.field for v in c.values)
            for c in template.cells
        ):
            raise ValueError(
                "bar field/coordinate is outside declared native policy"
            )
        snapshot = by_key.get((column.entity.symbol, cutoff))
        if snapshot is None:
            return ()
        cell = next(
            c
            for c in snapshot.cells
            if c.scope.value == scope and c.interval_code == interval
        )
        if cell.partial:
            raise ValueError(
                "partial candle cannot masquerade as closed-bar feature"
            )
        value = cell.feature(column.field)
        support = [b for b in snapshot.bars if b.bar_id in value.source_bar_ids]
        lo = min((b.bar_start_ns for b in support), default=cell.bar_start_ns)
        hi = max((b.bar_end_ns for b in support), default=cell.bar_end_ns)
        if scope != "observed":
            lo = min(lo, product.dependency_start_ns)
            hi = max(hi, product.dependency_end_ns)
        available = (
            value.available_at_ns
            if snapshot.policy.information_mode
            is InformationMode.EX_ANTE_SIMULATION
            else None
        )
        return (
            _JoinRecord(
                cutoff,
                cell.bar_end_ns,
                cutoff + 1,
                available,
                lo,
                hi,
                value.source_bar_ids or (snapshot.artifact_id,),
                (snapshot.artifact_id, *roots),
                value.schema_version,
                f"{scope}_quote_bar",
                value.value,
                _native_state(value.state.value),
                value.state.value,
            ),
        )

    return _JoinAdapter(source, roots, select)


def _triangles(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    native = TriangleBarSourceV1(_bar_source(source, verified))
    snapshots = []
    projections: dict[int, TriangleProjectionSnapshotV1] = {}
    for text in source.evidence_json:
        wire = training_load(text)
        if (
            wire.get("schema_version")
            == "histdatacom.causal-bar-triangle-projection-snapshot.v1"
        ):
            projection = TriangleProjectionSnapshotV1.from_json(text)
            if training_json(projection.to_dict()) != text:
                raise ValueError(
                    "triangle projection envelope is not canonical native evidence"
                )
            if projection.triangle.decision_time_ns in projections:
                raise ValueError("duplicate triangle projection cutoff")
            expected = derive_triangle_projection_features(
                native,
                projection.triangle,
                information_mode=projection.triangle.policy.bar_policy.information_mode,
                evidence=projection.evidence,
            )
            if expected != projection:
                raise ValueError(
                    "triangle projection does not replay actual delivery"
                )
            projections[projection.triangle.decision_time_ns] = projection
            snapshot = projection.triangle
        else:
            snapshot = TriangleBarSnapshotV1.from_json(text)
            if training_json(snapshot.to_dict()) != text:
                raise ValueError(
                    "triangle envelope is not canonical native evidence"
                )
            native.verify_snapshot(
                snapshot,
                information_mode=snapshot.policy.bar_policy.information_mode,
            )
        snapshots.append(snapshot)
    if len({s.decision_time_ns for s in snapshots}) != len(snapshots):
        raise ValueError("duplicate declared triangle cutoff")
    if len({s.policy.artifact_id for s in snapshots}) != 1:
        raise ValueError("triangle source cannot mix feature policies")
    roots = tuple(
        dict.fromkeys(
            i
            for s in snapshots
            for leg in s.leg_snapshots
            for i in (
                leg.source_product_manifest_id,
                leg.derived_bar_manifest_id,
            )
        )
    )
    product = next(
        p
        for p in verified.products
        if p.manifest.manifest_id
        == snapshots[0].leg_snapshots[0].source_product_manifest_id
    )
    by_key = {s.decision_time_ns: s for s in snapshots}

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        scope, interval = _coordinate(column)
        if column.entity.symbol not in {
            leg.symbol.upper() for leg in snapshots[0].leg_snapshots
        }:
            raise ValueError("triangle entity is outside its exact native legs")
        _prefix(column, f"triangle.{interval}.{scope}.")
        _semantics(column, JoinMeaning.CLOSED_BAR)
        template = snapshots[0]
        if column.field.startswith("projection."):
            name = column.field.removeprefix("projection.")
            names = {
                "tuple_count",
                "projected_tuple_count",
                "projected_rate",
                "mean_pre_log_residual",
                "mean_post_log_residual",
                "mean_pre_absolute_constraint_residual",
                "mean_post_absolute_constraint_residual",
                "mean_normalized_burden",
                "spread_weighted_burden",
                "l1_quote_displacement_total",
                "spread_scale_total",
            }
            if name not in names or not any(
                c.scope.value == scope and c.interval_code == interval
                for c in template.cells
            ):
                raise ValueError(
                    "unknown triangle projection scalar coordinate"
                )
            snapshot = by_key.get(cutoff)
            if snapshot is None:
                return ()
            projection = projections.get(cutoff)
            bar_cell = next(
                c
                for c in snapshot.cells
                if c.scope.value == scope and c.interval_code == interval
            )
            if projection is None:
                return (
                    _JoinRecord(
                        cutoff,
                        bar_cell.bar_end_ns,
                        cutoff + 1,
                        None,
                        bar_cell.bar_start_ns,
                        bar_cell.bar_end_ns,
                        (snapshot.artifact_id,),
                        roots,
                        snapshot.schema_version,
                        f"{scope}_triangle_projection",
                        None,
                        JoinState.UNAVAILABLE,
                        "unavailable_retained_projection_evidence",
                    ),
                )
            projected = next(
                c
                for c in projection.cells
                if c.scope.value == scope and c.interval_code == interval
            )
            point_ids = tuple(p.artifact_id for p in projected.points)
            evidence_ids = (
                (projection.evidence.artifact_id,)
                if projection.evidence is not None
                else ()
            )
            return (
                _JoinRecord(
                    cutoff,
                    projected.bar_end_ns,
                    cutoff + 1,
                    (
                        projected.available_at_ns
                        if snapshot.policy.bar_policy.information_mode
                        is InformationMode.EX_ANTE_SIMULATION
                        else None
                    ),
                    min(projected.bar_start_ns, product.dependency_start_ns),
                    max(projected.bar_end_ns, product.dependency_end_ns),
                    point_ids or (projection.artifact_id,),
                    (projection.artifact_id, *evidence_ids, *roots),
                    projected.schema_version,
                    f"{scope}_triangle_projection",
                    projected.summary()[name],
                    _native_state(projected.state),
                    projected.state,
                ),
            )
        if not any(
            c.scope.value == scope
            and c.interval_code == interval
            and any(v.name == column.field for v in c.values)
            for c in template.cells
        ):
            raise ValueError(
                "triangle field/coordinate is outside native policy"
            )
        snapshot = by_key.get(cutoff)
        if snapshot is None:
            return ()
        cell = next(
            c
            for c in snapshot.cells
            if c.scope.value == scope and c.interval_code == interval
        )
        value = next(v for v in cell.values if v.name == column.field)
        support = [
            b
            for leg in snapshot.leg_snapshots
            for b in leg.bars
            if b.bar_id in value.source_bar_ids
        ]
        lo = min((b.bar_start_ns for b in support), default=cell.bar_start_ns)
        hi = max((b.bar_end_ns for b in support), default=cell.bar_end_ns)
        if scope != "observed":
            lo, hi = min(lo, product.dependency_start_ns), max(
                hi, product.dependency_end_ns
            )
        available = (
            value.available_at_ns
            if snapshot.policy.bar_policy.information_mode
            is InformationMode.EX_ANTE_SIMULATION
            else None
        )
        return (
            _JoinRecord(
                cutoff,
                cell.bar_end_ns,
                cutoff + 1,
                available,
                lo,
                hi,
                tuple(
                    dict.fromkeys(value.source_bar_ids + value.source_event_ids)
                )
                or (snapshot.artifact_id,),
                (snapshot.artifact_id, *roots),
                value.schema_version,
                f"{scope}_triangle_quotes",
                value.value,
                _native_state(value.state),
                value.state,
            ),
        )

    return _JoinAdapter(source, roots, select)


def _activity(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    _configuration(source, keys=set())
    if (
        len(source.paths) != 1
        or len(source.evidence_json) != 1
        or source.paths[0] not in verified.source.product_manifest_paths
    ):
        raise ValueError(
            "activity requires exact owned product and native manifest"
        )
    retained = ReconstructionActivityManifestV1.from_dict(
        training_load(source.evidence_json[0])
    )
    if training_json(retained.to_dict()) != source.evidence_json[0]:
        raise ValueError("noncanonical native activity evidence")
    actual = summarize_committed_reconstruction_activity(
        source.paths[0],
        information_mode=retained.information_mode,
        information_manifest_id=retained.information_manifest_id,
        as_of_ns=retained.as_of_ns,
        policy=retained.policy,
        calibration_report_id=retained.calibration_report_id,
        benchmark_evidence=retained.benchmark_evidence,
    )
    if actual != retained:
        raise ValueError("activity does not replay committed native events")
    product = next(
        p
        for p in verified.products
        if p.manifest.manifest_id == actual.product_manifest_id
    )
    roots = (actual.manifest_id, product.manifest.manifest_id)

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        _market_entity(column)
        _prefix(column, f"activity.{column.entity.symbol}.{column.coordinate}.")
        _semantics(column, JoinMeaning.SNAPSHOT)
        slices = [
            s
            for s in actual.slices
            if s.symbol.upper() == column.entity.symbol
            and s.scope.value == column.coordinate
        ]
        if len(slices) != 1:
            raise ValueError("activity symbol/scope is outside exact manifest")
        item = slices[0]
        metrics = [m for m in item.metrics if m.name == column.field]
        if len(metrics) != 1:
            raise ValueError(
                "unknown activity metric (spot traded volume is unsupported)"
            )
        metric = metrics[0]
        # Retained information IDs are not availability proof. Full-window
        # statistics never get backdated into individual event feature rows.
        time = max(item.end_event_time_ns, product.dependency_end_ns - 1)
        return (
            _JoinRecord(
                time,
                time,
                time + 1,
                None,
                min(item.start_event_time_ns, product.dependency_start_ns),
                max(item.end_event_time_ns + 1, product.dependency_end_ns),
                (item.slice_id,),
                roots,
                metric.schema_version,
                f"{item.scope.value}_quote_activity_not_traded_volume",
                metric.value,
                (
                    JoinState.AVAILABLE
                    if metric.value is not None
                    else JoinState.UNAVAILABLE
                ),
                "recomputed_full_window_activity_unknown_historical_clock",
            ),
        )

    return _JoinAdapter(source, roots, select)


def prepare_market_join(
    source: TrainingJoinSourceV1, verified: _VerifiedSource
) -> _JoinAdapter:
    if source.family is JoinFamily.TICK:
        return _tick(source, verified)
    if source.family in (JoinFamily.BAR, JoinFamily.INDICATOR):
        return _bars(source, verified)
    if source.family is JoinFamily.TRIANGLE:
        return _triangles(source, verified)
    if source.family is JoinFamily.ACTIVITY:
        return _activity(source, verified)
    raise ValueError("unsupported native market join family")

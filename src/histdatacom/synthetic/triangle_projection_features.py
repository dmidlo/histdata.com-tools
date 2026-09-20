"""Exact retained-proposal bridge for ex-post per-bar projection diagnostics.

Only exact-event triangle reconciliation followed by the modern-reference
identity delivery is supported in v1. Row-free #516 report aggregates are not
event evidence and are never prorated. This bridge does not qualify a model.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from statistics import mean
from typing import ClassVar

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    _Artifact,
    _ns,
    _parse_json,
    _text,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyConditionV1,
    CrossCurrencyReconciledGroupV1,
    CrossCurrencyReconciliationConfigV1,
    CrossCurrencyRelationshipKind,
    reconcile_cross_currency_window,
)
from histdatacom.synthetic.delivery import (
    ReconstructionDeliveredGroupV1,
    project_modern_reference_delivery,
    reconstruction_streams_content_sha256,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    ReconstructionProductManifestV2,
    ReconstructionProductManifestV3,
    load_reconstruction_manifest,
    read_reconstruction_streams,
    verify_reconstruction_publication,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)
from histdatacom.synthetic.triangle_bar_features import (
    MAX_TRIANGLE_EVENTS,
    TRIANGLE_SYMBOLS,
    TriangleBarSnapshotV1,
    TriangleBarSourceV1,
    exact_legacy_payload,
    scope_contains,
)


@dataclass(frozen=True, slots=True)
class TriangleProjectionEvidenceV1(_Artifact):
    """Full immutable inputs, not a receipt ID or process-local metadata.

    Clocks assert when this *whole* retained window was generated and known.
    They cannot certify historical knowledge or admit generated ex-ante state.
    """

    run_json: str
    window_json: str
    config_json: str
    proposal_stream_json: tuple[str, ...]
    condition_json: tuple[str, ...]
    delivery_profile_id: str
    known_at_ns: int
    generated_at_ns: int
    scale_epsilon: float = 1e-9
    KIND: ClassVar[str] = "triangle-projection-evidence"

    def _validate(self) -> None:
        # The shared base checks the envelope after semantic validation too;
        # retained JSON must obey that same total budget before engine replay.
        self.to_json()
        _ns(self.known_at_ns, "projection known_at_ns")
        _ns(self.generated_at_ns, "projection generated_at_ns")
        _text(self.delivery_profile_id, "delivery_profile_id")
        if not math.isfinite(self.scale_epsilon) or self.scale_epsilon <= 0:
            raise ValueError("projection epsilon must be finite and positive")
        if (
            len(self.proposal_stream_json) != 3
            or len(self.condition_json) > 128
        ):
            raise ValueError("projection retained input cardinality is invalid")
        self.replay()

    @property
    def run(self) -> ReconstructionRunV1:
        value = ReconstructionRunV1.from_dict(_parse_json(self.run_json))
        exact_legacy_payload(self.run_json, value.to_dict())
        return value

    @property
    def window(self) -> ReconstructionWindowV1:
        value = ReconstructionWindowV1.from_dict(_parse_json(self.window_json))
        exact_legacy_payload(self.window_json, value.to_dict())
        return value

    @property
    def proposals(self) -> tuple[SyntheticEventStreamV1, ...]:
        # Bound declared work before legacy readers allocate event objects.
        raw = tuple(_parse_json(text) for text in self.proposal_stream_json)
        total = 0
        for value in raw:
            rows = value.get("events")
            if not isinstance(rows, list):
                raise ValueError("proposal streams require retained event rows")
            total += len(rows)
            if total > MAX_TRIANGLE_EVENTS:
                raise ValueError("projection retained proposal budget exceeded")
        streams = tuple(
            SyntheticEventStreamV1.from_dict(value) for value in raw
        )
        for text, stream in zip(
            self.proposal_stream_json, streams, strict=True
        ):
            exact_legacy_payload(text, stream.to_dict())
        if tuple(s.symbol.upper() for s in streams) != TRIANGLE_SYMBOLS:
            raise ValueError(
                "projection requires exact ordered triangle proposals"
            )
        return streams

    def replay(
        self,
    ) -> tuple[CrossCurrencyReconciledGroupV1, ReconstructionDeliveredGroupV1]:
        config = CrossCurrencyReconciliationConfigV1.from_dict(
            _parse_json(self.config_json)
        )
        exact_legacy_payload(self.config_json, config.to_dict())
        if (
            len(config.relationships) != 1
            or config.relationships[0].kind
            is not CrossCurrencyRelationshipKind.TRIANGLE
            or config.relationships[0].symbols != ("eurgbp", "eurusd", "gbpusd")
        ):
            raise ValueError(
                "projection bridge supports only the fixed oriented triangle"
            )
        conditions = tuple(
            CrossCurrencyConditionV1.from_dict(_parse_json(text))
            for text in self.condition_json
        )
        for text, condition in zip(
            self.condition_json, conditions, strict=True
        ):
            exact_legacy_payload(text, condition.to_dict())
        group = reconcile_cross_currency_window(
            run=self.run,
            window=self.window,
            streams={s.symbol: s for s in self.proposals},
            config=config,
            conditions=conditions,
        )
        delivered = project_modern_reference_delivery(
            group, delivery_profile_id=self.delivery_profile_id
        )
        return group, delivered

    @property
    def available_at_ns(self) -> int:
        return max(
            self.known_at_ns,
            self.generated_at_ns,
            self.window.input_end_ns,
            *(e.event_time_ns for s in self.proposals for e in s.events),
        )

    @classmethod
    def retain(
        cls,
        *,
        run: ReconstructionRunV1,
        window: ReconstructionWindowV1,
        config: CrossCurrencyReconciliationConfigV1,
        proposals: Sequence[SyntheticEventStreamV1],
        delivery_profile_id: str,
        known_at_ns: int,
        generated_at_ns: int,
        conditions: Sequence[CrossCurrencyConditionV1] = (),
        scale_epsilon: float = 1e-9,
    ) -> TriangleProjectionEvidenceV1:
        if (
            len(proposals) != 3
            or sum(len(s.events) for s in proposals) > MAX_TRIANGLE_EVENTS
        ):
            raise ValueError("projection retained proposal budget exceeded")
        return cls(
            run.to_json(),
            window.to_json(),
            canonical_contract_json(config.to_dict()),
            tuple(
                s.to_json() for s in sorted(proposals, key=lambda s: s.symbol)
            ),
            tuple(
                canonical_contract_json(c.to_dict())
                for c in sorted(conditions, key=lambda c: c.condition_id)
            ),
            delivery_profile_id,
            known_at_ns,
            generated_at_ns,
            scale_epsilon,
        )


@dataclass(frozen=True, slots=True)
class TriangleProjectionPointV1(_Artifact):
    probe_time_ns: int
    pre_event_ids: tuple[str, ...]
    post_event_ids: tuple[str, ...]
    pre_log_residual: float
    post_log_residual: float
    pre_absolute_constraint_residual: float
    post_absolute_constraint_residual: float
    l1_quote_displacement: float
    spread_scale: float
    normalized_burden: float
    projected: bool
    KIND: ClassVar[str] = "triangle-projection-point"

    def _validate(self) -> None:
        _ns(self.probe_time_ns, "projection probe")
        if len(self.pre_event_ids) != 3 or len(self.post_event_ids) != 3:
            raise ValueError("projection point requires all three proposals")
        if (
            self.pre_absolute_constraint_residual < 0
            or self.post_absolute_constraint_residual < 0
        ):
            raise ValueError("absolute constraint residuals cannot be negative")
        if (
            self.spread_scale <= 0
            or self.l1_quote_displacement < 0
            or self.normalized_burden
            != self.l1_quote_displacement / self.spread_scale
        ):
            raise ValueError("projection burden denominator differs")
        if self.projected != (self.l1_quote_displacement > 0):
            raise ValueError("projection movement flag differs")


@dataclass(frozen=True, slots=True)
class TriangleProjectionCellV1(_Artifact):
    scope: ActivitySliceScope
    interval_code: str
    bar_start_ns: int
    bar_end_ns: int
    state: str
    available_at_ns: int | None
    points: tuple[TriangleProjectionPointV1, ...]
    KIND: ClassVar[str] = "triangle-projection-cell"

    def _validate(self) -> None:
        if self.state not in (
            "available",
            "not_applicable_observed",
            "unavailable_retained_evidence",
            "unsupported_window_mapping",
            "no_synthetic_tuple_support",
            "missing_or_partial_leg",
        ):
            raise ValueError("unsupported projection feature state")
        if (self.state == "available") != bool(self.points) or (
            self.available_at_ns is not None
        ) != bool(self.points):
            raise ValueError("projection cell state/support mismatch")
        if any(
            not self.bar_start_ns <= p.probe_time_ns < self.bar_end_ns
            for p in self.points
        ):
            raise ValueError("projection point belongs to another bar")
        self.summary()

    def summary(self) -> dict[str, float | int | None]:
        """Every mean/rate includes unchanged synthetic-support tuples."""
        if not self.points:
            return {
                name: None
                for name in (
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
                )
            }
        total = sum(p.l1_quote_displacement for p in self.points)
        scale = sum(p.spread_scale for p in self.points)
        if not math.isfinite(total) or not math.isfinite(scale):
            raise ValueError("projection aggregate totals are unrepresentable")
        projected = sum(p.projected for p in self.points)
        return {
            "tuple_count": len(self.points),
            "projected_tuple_count": projected,
            "projected_rate": projected / len(self.points),
            "mean_pre_log_residual": mean(
                p.pre_log_residual for p in self.points
            ),
            "mean_post_log_residual": mean(
                p.post_log_residual for p in self.points
            ),
            "mean_pre_absolute_constraint_residual": mean(
                p.pre_absolute_constraint_residual for p in self.points
            ),
            "mean_post_absolute_constraint_residual": mean(
                p.post_absolute_constraint_residual for p in self.points
            ),
            "mean_normalized_burden": mean(
                p.normalized_burden for p in self.points
            ),
            "spread_weighted_burden": total / scale,
            "l1_quote_displacement_total": total,
            "spread_scale_total": scale,
        }


def _log_residual(events: tuple[SyntheticEventV1, ...]) -> float:
    x, u, g = events
    return (
        math.log(u.bid / 2 + u.ask / 2)
        - math.log(x.bid / 2 + x.ask / 2)
        - math.log(g.bid / 2 + g.ask / 2)
    )


def _absolute_residual(events: tuple[SyntheticEventV1, ...]) -> float:
    x, u, g = events
    implied_bid, implied_ask = u.bid / g.ask, u.ask / g.bid
    return max(
        abs(x.bid - implied_bid) / implied_bid,
        abs(x.ask - implied_ask) / implied_ask,
    )


def _projection_cells(
    snapshot: TriangleBarSnapshotV1,
    evidence: TriangleProjectionEvidenceV1 | None,
) -> tuple[TriangleProjectionCellV1, ...]:
    points_by_scope: dict[
        ActivitySliceScope, tuple[TriangleProjectionPointV1, ...]
    ] = {}
    if evidence is not None:
        group, delivered = evidence.replay()
        if evidence.available_at_ns > snapshot.decision_time_ns:
            raise ValueError(
                "future retained projection evidence cannot enter snapshot"
            )
        if (
            snapshot.policy.bar_policy.information_mode
            is not InformationMode.EX_POST_RECONSTRUCTION
            or not snapshot.policy.bar_policy.allow_prior_generated_state
        ):
            raise ValueError(
                "projection features require explicit ex-post generated-state admission"
            )
        final_events = {
            e.event_id: e.to_json() for s in delivered.streams for e in s.events
        }
        if any(
            text
            != final_events.get(
                SyntheticEventV1.from_dict(_parse_json(text)).event_id
            )
            for text in snapshot.event_json
        ):
            raise ValueError(
                "projection output differs from retained triangle source events"
            )
        # Map by actual positions, never by floating-point quote similarity.
        post = {
            (e.symbol, e.event_time_ns, e.event_sequence): e
            for s in delivered.streams
            for e in s.events
        }
        proposals = evidence.proposals
        by_symbol: dict[str, dict[int, list[SyntheticEventV1]]] = {
            s.symbol: {} for s in proposals
        }
        for stream in proposals:
            for event in stream.events:
                if evidence.window.owns_event_time(event.event_time_ns):
                    by_symbol[stream.symbol].setdefault(
                        event.event_time_ns, []
                    ).append(event)
        common = set.intersection(*(set(v) for v in by_symbol.values()))
        all_points: list[
            tuple[TriangleProjectionPointV1, tuple[SyntheticEventV1, ...]]
        ] = []
        for time in sorted(common):
            for ordinal in range(min(len(v[time]) for v in by_symbol.values())):
                before = tuple(
                    by_symbol[s.lower()][time][ordinal]
                    for s in TRIANGLE_SYMBOLS
                )
                after = tuple(
                    post[(e.symbol, e.event_time_ns, e.event_sequence)]
                    for e in before
                )
                # A single fixed relationship and exact ordinal matching means
                # each source position participates once; no stale re-projection.
                displacement = sum(
                    abs(a.bid - b.bid) + abs(a.ask - b.ask)
                    for a, b in zip(before, after, strict=True)
                )
                scale = sum(
                    max(e.ask - e.bid, evidence.scale_epsilon) for e in before
                )
                point = TriangleProjectionPointV1(
                    time,
                    tuple(e.event_id for e in before),
                    tuple(e.event_id for e in after),
                    _log_residual(before),
                    _log_residual(after),
                    _absolute_residual(before),
                    _absolute_residual(after),
                    displacement,
                    scale,
                    displacement / scale,
                    displacement > 0,
                )
                all_points.append((point, after))
        # Include zero-movement tuples; exclude observed-only tuples from the
        # synthetic proposal burden denominator, without calling them absent.
        for scope in snapshot.policy.bar_policy.scopes:
            points_by_scope[scope] = tuple(
                p
                for p, events in all_points
                if any(
                    e.origin is SyntheticEventOrigin.SYNTHETIC for e in events
                )
                and all(scope_contains(scope, e) for e in events)
            )
        if sum(
            s.projected_count
            for s in group.generation_validation.relationship_support
        ) != sum(p.projected for p, _ in all_points):
            raise ValueError(
                "projection trace does not reconcile exact tuple support"
            )
    result = []
    for cell in snapshot.cells:
        state = "unavailable_retained_evidence"
        points: tuple[TriangleProjectionPointV1, ...] = ()
        available = None
        if cell.scope is ActivitySliceScope.OBSERVED:
            state = "not_applicable_observed"
        elif cell.feature("residual_close").value is None:
            state = "missing_or_partial_leg"
        elif evidence is not None:
            window = evidence.window
            if (
                not window.core_start_ns
                <= cell.bar_start_ns
                < cell.bar_end_ns
                <= window.core_end_ns
            ):
                state = "unsupported_window_mapping"
            else:
                points = tuple(
                    p
                    for p in points_by_scope[cell.scope]
                    if cell.bar_start_ns <= p.probe_time_ns < cell.bar_end_ns
                )
                state = "available" if points else "no_synthetic_tuple_support"
                if points:
                    available = max(
                        evidence.available_at_ns,
                        cell.feature("residual_close").available_at_ns or 0,
                    )
        result.append(
            TriangleProjectionCellV1(
                cell.scope,
                cell.interval_code,
                cell.bar_start_ns,
                cell.bar_end_ns,
                state,
                available,
                points,
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class TriangleProjectionSnapshotV1(_Artifact):
    triangle: TriangleBarSnapshotV1
    evidence: TriangleProjectionEvidenceV1 | None
    cells: tuple[TriangleProjectionCellV1, ...]
    KIND: ClassVar[str] = "triangle-projection-snapshot"

    def _validate(self) -> None:
        if self.cells != _projection_cells(self.triangle, self.evidence):
            raise ValueError(
                "projection cells do not replay retained proposals"
            )


def derive_triangle_projection_features(
    source: TriangleBarSourceV1,
    snapshot: TriangleBarSnapshotV1,
    *,
    information_mode: InformationMode,
    evidence: TriangleProjectionEvidenceV1 | None = None,
) -> TriangleProjectionSnapshotV1:
    """Verify publication bytes, exact delivery lineage and retained replay."""
    source.verify_snapshot(snapshot, information_mode=information_mode)
    admitted = evidence
    if evidence is not None and (
        evidence.known_at_ns > snapshot.decision_time_ns
        or evidence.generated_at_ns > snapshot.decision_time_ns
        or evidence.available_at_ns > snapshot.decision_time_ns
    ):
        admitted = None
    if admitted is not None:
        path = source.bar_source.reconstruction_manifest_path
        declared = load_reconstruction_manifest(path)
        if declared.event_count > snapshot.policy.bar_policy.max_source_events:
            raise ValueError(
                "projection total source verification budget exceeded"
            )
        manifest = verify_reconstruction_publication(path)
        if (
            manifest.manifest_id != declared.manifest_id
            or manifest.manifest_id
            != snapshot.leg_snapshots[0].source_product_manifest_id
        ):
            raise ValueError("projection source changed during verification")
        if not isinstance(
            manifest,
            (ReconstructionProductManifestV2, ReconstructionProductManifestV3),
        ):
            raise ValueError(
                "retained projection bridge requires modern-reference product"
            )
        _, delivered = admitted.replay()
        if (
            manifest.quality.delivery_manifest_id
            != delivered.manifest.manifest_id
            or manifest.quality.delivery_output_content_sha256
            != delivered.manifest.output_content_sha256
        ):
            raise ValueError(
                "retained proposals do not bind committed delivery identity"
            )
        committed = read_reconstruction_streams(path)
        if reconstruction_streams_content_sha256(
            committed
        ) != reconstruction_streams_content_sha256(delivered.streams):
            raise ValueError(
                "retained proposal replay differs from committed event bytes"
            )
    return TriangleProjectionSnapshotV1(
        snapshot, admitted, _projection_cells(snapshot, admitted)
    )


__all__ = [
    "TriangleProjectionEvidenceV1",
    "TriangleProjectionPointV1",
    "TriangleProjectionCellV1",
    "TriangleProjectionSnapshotV1",
    "derive_triangle_projection_features",
]

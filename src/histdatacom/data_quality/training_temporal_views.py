"""Executable temporal feature/label/split consumer over current source artifacts."""

from __future__ import annotations

import math

from histdatacom.forecasting.feature_contracts import FeatureKind

from .training_contracts import TrainingBatchV1
from .training_temporal_contracts import (
    TemporalFeatureKind,
    TemporalInformationMode,
    TemporalLabelKind,
    TemporalTargetKind,
    TrainingTemporalBatchV1,
    TrainingTemporalExampleV1,
    TrainingTemporalOutcomeV1,
    TrainingTemporalPlanV1,
    TrainingTemporalRowV1,
    TrainingTemporalSplitV1,
)
from .training_temporal_sources import (
    _TemporalPoint,
    _TemporalVerifiedSource,
    _market_points,
    _quote_points,
    temporal_feature_value,
    verify_training_temporal_source,
)
from .training_views import replay_training_batch


def temporal_plan_from_training_batch(
    batch: TrainingBatchV1,
    split: TrainingTemporalSplitV1,
    examples: tuple[TrainingTemporalExampleV1, ...],
) -> TrainingTemporalPlanV1:
    """Bind an existing executable #606 batch, never upgrade its unknown clocks.

    Materialization replays both the complete original batch and the temporal plan.
    Source-only construction is also supported through TrainingTemporalPlanV1.
    """
    if type(batch) is not TrainingBatchV1:
        raise TypeError(
            "temporal adapter requires a complete v1 training batch"
        )
    return TrainingTemporalPlanV1(
        batch.source,
        batch.ownership,
        split,
        TemporalInformationMode.EX_POST,
        examples,
        batch.to_json(),
    )


def _event_owner(
    source: _TemporalVerifiedSource, example: TrainingTemporalExampleV1
) -> str | None:
    identity = example.label.event_observation_id
    if identity is None:
        return None
    found = {
        item.observation_id: item
        for matrix in source.matrices.values()
        for item in matrix.observations
        if item.observation_id == identity
    }
    if len(found) != 1:
        raise ValueError("event label lacks exact retained release ownership")
    event = found[identity]
    if (
        event.definition.kind is not FeatureKind.MACRO
        or event.vintage_sequence != 0
    ):
        raise ValueError(
            "event response requires an initial macro release, not a quote/revision"
        )
    if event.definition.known_at_ns > example.decision_time_ns:
        raise ValueError(
            "release definition was not known at the decision cutoff"
        )
    if event.available_at_ns != example.decision_time_ns:
        raise ValueError(
            "event response starts at the retained release availability"
        )
    if (
        source.unit(event.available_at_ns).artifact_id
        != example.evidence_unit_id
    ):
        raise ValueError(
            "release and response have different historical owners"
        )
    return identity


def _label(
    source: _TemporalVerifiedSource,
    example: TrainingTemporalExampleV1,
    quotes: tuple[_TemporalPoint, ...],
) -> TrainingTemporalOutcomeV1:
    points = (
        quotes
        if example.label.target is TemporalTargetKind.QUOTES
        else _market_points(source, example)
    )
    start = example.decision_time_ns
    end = start + example.label.horizon_ns
    baseline = [p for p in points if p.time_ns == start]
    future = [p for p in points if start < p.time_ns <= end]
    if len(baseline) != 1 or not future or future[-1].time_ns != end:
        raise ValueError(
            "label needs exact decision/horizon endpoints and (t,t+h] support"
        )
    initial = baseline[0]
    if initial.available_at_ns is not None and initial.available_at_ns > start:
        raise ValueError("baseline price was unavailable at decision time")
    required = (initial, *future)
    if max(p.dependency_end_ns for p in required) > example.label_cutoff_ns + 1:
        raise ValueError(
            "label cutoff precedes complete target conditioning support"
        )
    availability = (
        None
        if any(p.available_at_ns is None for p in required)
        else max(
            p.available_at_ns for p in required if p.available_at_ns is not None
        )
    )
    if availability is not None and availability > example.label_cutoff_ns:
        raise ValueError("target source was unavailable at label cutoff")
    if (
        source.plan.information_mode is TemporalInformationMode.NORMALIZED_AS_OF
        and availability is None
    ):
        raise ValueError(
            "normalized-clock labels refuse unknown target availability"
        )
    # Difference of logs avoids overflowing a finite positive price ratio.
    total_return = math.log(future[-1].value) - math.log(initial.value)
    kind = example.label.kind
    if kind is TemporalLabelKind.REALIZED_VARIANCE:
        value = math.fsum(
            (math.log(right.value) - math.log(left.value)) ** 2
            for left, right in zip(required, required[1:])
        )
    elif kind is TemporalLabelKind.DIRECTION:
        value = float(total_return > example.label.deadband)
    else:
        value = total_return
    lo = min(p.dependency_start_ns for p in required)
    hi = max(p.dependency_end_ns for p in required)
    source.unit(lo)
    source.unit(hi - 1)
    units = tuple(
        unit.artifact_id
        for unit in source.plan.ownership.units
        if unit.start_ns < hi and lo < unit.end_ns
    )
    return TrainingTemporalOutcomeV1(
        value,
        tuple(p.record_id for p in future),
        initial.record_id,
        start,
        end,
        availability,
        _event_owner(source, example),
        units,
        lo,
        hi,
    )


def _verify_legacy_binding(plan: TrainingTemporalPlanV1) -> None:
    if plan.legacy_batch_json is None:
        return
    batch = replay_training_batch(
        TrainingBatchV1.from_json(plan.legacy_batch_json)
    )
    if batch.source != plan.source or batch.ownership != plan.ownership:
        raise ValueError("legacy replay source differs from temporal manifest")
    for example in plan.examples:
        if example.product_manifest_id != batch.request.product_manifest_id:
            raise ValueError(
                "temporal member/product differs from bound v1 batch"
            )
        if not any(
            row.event_time_ns == example.decision_time_ns
            and row.symbol == example.symbol
            and row.evidence_unit_id == example.evidence_unit_id
            and row.ensemble_member_id == example.ensemble_member_id
            for row in batch.rows
        ):
            raise ValueError(
                "temporal decision is not a row of the bound v1 batch"
            )


def materialize_training_temporal(
    plan: TrainingTemporalPlanV1,
) -> TrainingTemporalBatchV1:
    """Replay full evidence, compute labels, then derive all split exclusions.

    There is deliberately no pre-verification row filter. The complete plan always
    produces one result per example, including excluded rows; downstream projection
    occurs only after complete replay. Status 'admitted' means the declared research
    mode and split policy, never a general causal/ML eligibility certificate.
    """
    source = verify_training_temporal_source(plan)
    _verify_legacy_binding(plan)
    values = []
    spans = []
    for example in plan.examples:
        needs_quotes = example.label.target is TemporalTargetKind.QUOTES or any(
            feature.kind is not TemporalFeatureKind.VINTAGE
            for feature in example.features
        )
        quotes = _quote_points(source, example) if needs_quotes else ()
        features = tuple(
            temporal_feature_value(source, example, feature, quotes)
            for feature in example.features
        )
        outcome = _label(source, example, quotes)
        lo = min(
            outcome.dependency_start_ns,
            *(f.dependency_start_ns for f in features),
        )
        hi = max(
            outcome.target_end_inclusive_ns + 1,
            outcome.dependency_end_ns,
            *(f.dependency_end_ns for f in features),
        )
        spans.append(hi - lo)
        values.append((features, outcome))
    # Derived before considering any chosen output subset or admission result.
    maximum_span = max(spans)
    units = plan.ownership.units
    assignments = plan.split.assignments
    boundaries = tuple(
        right.start_ns
        for left_assignment, right_assignment, right in zip(
            assignments, assignments[1:], units[1:]
        )
        if left_assignment.partition != right_assignment.partition
    )
    partitions = {a.evidence_unit_id: a.partition for a in assignments}
    rows = []
    for example, (features, outcome) in zip(plan.examples, values):
        reasons = set()
        if any(
            partitions[identity] != example.partition
            for identity in outcome.target_unit_ids
        ):
            reasons.add("target_crosses_partition")
        for boundary in boundaries:
            if boundary - maximum_span < example.decision_time_ns < boundary:
                reasons.add("purged_dependency_overlap")
            elif boundary <= example.decision_time_ns < boundary + maximum_span:
                reasons.add("embargo_dependency_span")
        rows.append(
            TrainingTemporalRowV1(
                example.key,
                example.evidence_unit_id,
                example.partition,
                features,
                outcome,
                "excluded" if reasons else "admitted",
                tuple(sorted(reasons)),
            )
        )
    return TrainingTemporalBatchV1(plan, maximum_span, boundaries, tuple(rows))


def replay_training_temporal(
    batch: TrainingTemporalBatchV1,
) -> TrainingTemporalBatchV1:
    """A correctly re-sealed artifact still must reproduce from exact sources."""
    if type(batch) is not TrainingTemporalBatchV1:
        raise TypeError("temporal replay requires its complete typed batch")
    expected = materialize_training_temporal(batch.plan)
    if expected.to_json() != batch.to_json():
        raise ValueError(
            "temporal values, clocks, labels or split ledger differ from replay"
        )
    return expected


def training_temporal_rows(
    batch: TrainingTemporalBatchV1,
    *,
    information_mode: TemporalInformationMode,
    example_keys: tuple[str, ...] | None = None,
    admitted_only: bool = True,
) -> tuple[TrainingTemporalRowV1, ...]:
    """Verified typed consumer; even empty selections replay full ownership."""
    if type(batch) is not TrainingTemporalBatchV1:
        raise TypeError(
            "temporal row consumer requires its complete typed batch"
        )
    if (
        type(information_mode) is not TemporalInformationMode
        or information_mode != batch.plan.information_mode
    ):
        raise ValueError(
            "consumer must name the exact temporal information mode"
        )
    if type(admitted_only) is not bool:
        raise TypeError("admission filter must be a boolean")
    replay_training_temporal(batch)
    keys = (
        tuple(e.key for e in batch.plan.examples)
        if example_keys is None
        else example_keys
    )
    if (
        type(keys) is not tuple
        or len(keys) > 256
        or len(set(keys)) != len(keys)
    ):
        raise ValueError("temporal projection requires unique bounded keys")
    if not set(keys) <= {e.key for e in batch.plan.examples}:
        raise ValueError("temporal projection has an unknown example")
    return tuple(
        row
        for row in batch.rows
        if row.example_key in keys
        and (not admitted_only or row.status == "admitted")
    )

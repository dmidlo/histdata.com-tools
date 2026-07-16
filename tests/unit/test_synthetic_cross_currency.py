"""Tests for synchronized event-time cross-currency reconstruction."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from histdatacom.data_quality import (
    CROSS_INSTRUMENT_METADATA_KEY,
    QualityStatus,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic import (
    CrossCurrencyConditionV1,
    CrossCurrencyExcludedReason,
    CrossCurrencyGroupStatus,
    CrossCurrencyReconciledGroupV1,
    CrossCurrencyReconciliationConfigV1,
    CrossCurrencyRelationshipKind,
    CrossCurrencyRelationshipV1,
    CrossCurrencySymbolCoverageV1,
    CrossCurrencyValidationStage,
    CrossCurrencyValidationStatus,
    CrossCurrencyWindowPlanStatus,
    CrossCurrencyWindowPlanV1,
    EventBatchV1,
    PartitionManifestV1,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    ReconstructionWindowV1,
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    cross_currency_quality_report,
    eurusd_triangle_reconciliation_config,
    plan_cross_currency_windows,
    reconcile_cross_currency_window,
    validate_cross_currency_atomic_manifest,
    validate_cross_currency_output,
)

MEMBER = "member-000"
SOURCE = "source:sha256:historical-v1"
START_NS = 1_700_000_000_000_000_000
END_NS = START_NS + 1_000_000_000
SYNTHETIC_NS = START_NS + 500_000_000


def _run(
    config: CrossCurrencyReconciliationConfigV1,
    *,
    symbols: tuple[str, ...] = ("EURGBP", "EURUSD", "GBPUSD"),
) -> ReconstructionRunV1:
    return ReconstructionRunV1(
        symbols=symbols,
        source_version_ids=(SOURCE,),
        configuration_ids=(config.config_id,),
        ensemble_member_ids=(MEMBER,),
        base_seed=441,
        storage_policy=ReconstructionStoragePolicyV1(),
    )


def _window(run: ReconstructionRunV1) -> ReconstructionWindowV1:
    return ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id=MEMBER,
        symbols=run.symbols,
        core_start_ns=START_NS,
        core_end_ns=END_NS,
    )


def _observed(
    run: ReconstructionRunV1,
    *,
    symbol: str,
    timestamp_ns: int,
    sequence: int,
    row_id: int,
    midpoint: float,
) -> SyntheticEventV1:
    return SyntheticEventV1.observed(
        symbol=symbol,
        event_time_ns=timestamp_ns,
        event_sequence=sequence,
        bid=midpoint - 0.0001,
        ask=midpoint + 0.0001,
        run_id=run.run_id,
        ensemble_member_id=MEMBER,
        source_version_id=SOURCE,
        source_series_id=f"ascii:T:{symbol}:histdata.com",
        source_period="202001",
        source_row_id=row_id,
    )


def _stream(
    run: ReconstructionRunV1,
    *,
    symbol: str,
    anchor_midpoint: float,
    synthetic_midpoint: float,
    extra_synthetic: tuple[tuple[int, int, float], ...] = (),
    middle_origin: SyntheticEventOrigin = SyntheticEventOrigin.SYNTHETIC,
) -> SyntheticEventStreamV1:
    left = _observed(
        run,
        symbol=symbol,
        timestamp_ns=START_NS,
        sequence=0,
        row_id=1,
        midpoint=anchor_midpoint,
    )
    right = _observed(
        run,
        symbol=symbol,
        timestamp_ns=END_NS - 1,
        sequence=0,
        row_id=3,
        midpoint=anchor_midpoint,
    )
    if middle_origin is SyntheticEventOrigin.OBSERVED:
        middle = _observed(
            run,
            symbol=symbol,
            timestamp_ns=SYNTHETIC_NS,
            sequence=1,
            row_id=2,
            midpoint=synthetic_midpoint,
        )
    else:
        middle = SyntheticEventV1.generated(
            symbol=symbol,
            event_time_ns=SYNTHETIC_NS,
            event_sequence=1,
            bid=synthetic_midpoint - 0.0001,
            ask=synthetic_midpoint + 0.0001,
            run_id=run.run_id,
            ensemble_member_id=MEMBER,
            source_version_id=SOURCE,
            left_anchor_event_id=left.event_id,
            right_anchor_event_id=right.event_id,
            generator_id="empirical-motif",
            generator_version="1.0.0",
            generator_config_id="motif-config:fixture",
            constraint_set_id="carving-constraints:fixture",
            confidence=0.8,
            motif_id="motif:fixture",
            reference_id="reference:fixture",
            feed_epoch_id="epoch:modern",
        )
    extras = tuple(
        SyntheticEventV1.generated(
            symbol=symbol,
            event_time_ns=timestamp_ns,
            event_sequence=sequence,
            bid=midpoint - 0.0001,
            ask=midpoint + 0.0001,
            run_id=run.run_id,
            ensemble_member_id=MEMBER,
            source_version_id=SOURCE,
            left_anchor_event_id=left.event_id,
            right_anchor_event_id=right.event_id,
            generator_id="empirical-motif",
            generator_version="1.0.0",
            generator_config_id="motif-config:fixture",
            constraint_set_id="carving-constraints:fixture",
            confidence=0.7,
            motif_id=f"motif:extra:{sequence}",
            reference_id="reference:fixture",
            feed_epoch_id="epoch:modern",
        )
        for timestamp_ns, sequence, midpoint in extra_synthetic
    )
    return SyntheticEventStreamV1(
        run_id=run.run_id,
        ensemble_member_id=MEMBER,
        symbol=symbol,
        events=(left, middle, *extras, right),
    )


def _triangle_streams(
    run: ReconstructionRunV1,
    *,
    direct_midpoint: float = 0.82,
    middle_origin: SyntheticEventOrigin = SyntheticEventOrigin.SYNTHETIC,
    eurusd_extras: tuple[tuple[int, int, float], ...] = (),
) -> dict[str, SyntheticEventStreamV1]:
    return {
        "EURGBP": _stream(
            run,
            symbol="EURGBP",
            anchor_midpoint=0.8,
            synthetic_midpoint=direct_midpoint,
            middle_origin=middle_origin,
        ),
        "EURUSD": _stream(
            run,
            symbol="EURUSD",
            anchor_midpoint=1.2,
            synthetic_midpoint=1.2,
            extra_synthetic=eurusd_extras,
            middle_origin=middle_origin,
        ),
        "GBPUSD": _stream(
            run,
            symbol="GBPUSD",
            anchor_midpoint=1.5,
            synthetic_midpoint=1.5,
            middle_origin=middle_origin,
        ),
    }


def _condition() -> CrossCurrencyConditionV1:
    return CrossCurrencyConditionV1(
        start_ns=START_NS,
        end_ns=END_NS,
        session_key="london",
        event_key="ordinary",
        feed_epoch_key="modern-electronic",
    )


def _reconciled_group() -> tuple[
    ReconstructionRunV1,
    ReconstructionWindowV1,
    dict[str, SyntheticEventStreamV1],
    CrossCurrencyReconciledGroupV1,
]:
    config = eurusd_triangle_reconciliation_config()
    run = _run(config)
    window = _window(run)
    streams = _triangle_streams(run)
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=streams,
        config=config,
        conditions=(_condition(),),
    )
    return run, window, streams, group


def test_common_window_plan_records_unequal_and_missing_coverage() -> None:
    """Only common coverage is planned and every excluded span is explicit."""
    config = eurusd_triangle_reconciliation_config()
    run = _run(config)
    plan = plan_cross_currency_windows(
        run,
        ensemble_member_id=MEMBER,
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
        window_size_ns=200_000_000,
        coverages=(
            CrossCurrencySymbolCoverageV1(
                "EURGBP",
                START_NS + 200_000_000,
                END_NS - 100_000_000,
                ("200203", "200204"),
            ),
            CrossCurrencySymbolCoverageV1(
                "EURUSD", START_NS, END_NS, ("200005",)
            ),
            CrossCurrencySymbolCoverageV1(
                "GBPUSD", START_NS, END_NS, ("200005",)
            ),
        ),
    )

    assert plan.status is CrossCurrencyWindowPlanStatus.PLANNED
    assert plan.common_start_ns == START_NS + 200_000_000
    assert plan.common_end_ns == END_NS - 100_000_000
    assert plan.windows[0].symbols == run.symbols
    reasons = {(item.symbol, item.reason) for item in plan.excluded_spans}
    assert (
        "eurgbp",
        CrossCurrencyExcludedReason.SYMBOL_NOT_YET_AVAILABLE,
    ) in reasons
    assert (
        "eurgbp",
        CrossCurrencyExcludedReason.SYMBOL_NO_LONGER_AVAILABLE,
    ) in reasons
    assert CrossCurrencyWindowPlanV1.from_json(plan.to_json()) == plan

    missing = plan_cross_currency_windows(
        run,
        ensemble_member_id=MEMBER,
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
        window_size_ns=200_000_000,
        coverages=(
            CrossCurrencySymbolCoverageV1("EURUSD", START_NS, END_NS),
            CrossCurrencySymbolCoverageV1("GBPUSD", START_NS, END_NS),
        ),
    )
    assert missing.status is CrossCurrencyWindowPlanStatus.REFUSED
    assert missing.missing_symbols == ("eurgbp",)
    assert missing.windows == ()
    assert any(
        item.reason is CrossCurrencyExcludedReason.MISSING_SYMBOL
        for item in missing.excluded_spans
    )

    no_overlap = plan_cross_currency_windows(
        run,
        ensemble_member_id=MEMBER,
        requested_start_ns=START_NS,
        requested_end_ns=END_NS,
        window_size_ns=200_000_000,
        coverages=(
            CrossCurrencySymbolCoverageV1(
                "EURGBP", START_NS, START_NS + 400_000_000
            ),
            CrossCurrencySymbolCoverageV1(
                "EURUSD", START_NS + 500_000_000, END_NS
            ),
            CrossCurrencySymbolCoverageV1("GBPUSD", START_NS, END_NS),
        ),
    )
    assert no_overlap.status is CrossCurrencyWindowPlanStatus.REFUSED
    assert no_overlap.windows == ()
    assert any(
        item.symbol == "*"
        and item.reason is CrossCurrencyExcludedReason.NO_COMMON_SUPPORT
        for item in no_overlap.excluded_spans
    )


def test_triangle_reconciliation_is_deterministic_and_preserves_anchors() -> (
    None
):
    """The dependent synthetic leg is projected and observations stay exact."""
    run, window, inputs, group = _reconciled_group()

    assert group.status is CrossCurrencyGroupStatus.RECONCILED
    assert group.generation_ready is True
    assert group.requires_post_broker_validation is True
    assert group.generation_validation.passed is True
    direct = group.stream_for("EURGBP")
    projected = next(
        event for event in direct.events if event.event_time_ns == SYNTHETIC_NS
    )
    assert projected.bid == pytest.approx(1.1999 / 1.5001)
    assert projected.ask == pytest.approx(1.2001 / 1.4999)
    assert (projected.bid + projected.ask) / 2.0 == pytest.approx(0.8)
    assert len(group.projection_lineage) == 1
    lineage = group.projection_lineage[0]
    assert lineage.symbol == "eurgbp"
    assert lineage.input_event_id == lineage.output_event_id
    assert lineage.input_content_sha256 != lineage.output_content_sha256
    assert lineage.post_residual <= lineage.allowed_residual
    assert {
        (item.dimension, item.key)
        for item in group.generation_validation.residual_slices
    } >= {
        ("session", "london"),
        ("event", "ordinary"),
        ("feed_epoch", "modern-electronic"),
    }
    for symbol, stream in inputs.items():
        before = {
            event.event_id: event.to_dict()
            for event in stream.events
            if event.origin is SyntheticEventOrigin.OBSERVED
        }
        after = {
            event.event_id: event.to_dict()
            for event in group.stream_for(symbol).events
            if event.origin is SyntheticEventOrigin.OBSERVED
        }
        assert after == before

    retry = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=inputs,
        config=group.config,
        conditions=(_condition(),),
    )
    assert retry == group
    assert CrossCurrencyReconciledGroupV1.from_json(group.to_json()) == group


def test_triangle_projection_falls_back_after_negative_spread_target() -> None:
    """A later synthetic priority leg is tried when the first is infeasible."""
    config = eurusd_triangle_reconciliation_config()
    run = _run(config)
    window = _window(run)

    def requote(
        stream: SyntheticEventStreamV1,
        *,
        bid: float,
        ask: float,
    ) -> SyntheticEventStreamV1:
        return SyntheticEventStreamV1(
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            symbol=stream.symbol,
            events=tuple(
                (
                    replace(event, bid=bid, ask=ask, event_id="")
                    if event.event_time_ns == SYNTHETIC_NS
                    else event
                )
                for event in stream.events
            ),
            source_version_ids=stream.source_version_ids,
        )

    direct = requote(
        _stream(
            run,
            symbol="EURGBP",
            anchor_midpoint=0.8,
            synthetic_midpoint=0.86711,
            middle_origin=SyntheticEventOrigin.OBSERVED,
        ),
        bid=0.86699,
        ask=0.86723,
    )
    numerator = requote(
        _stream(
            run,
            symbol="EURUSD",
            anchor_midpoint=1.2,
            synthetic_midpoint=1.09413,
        ),
        bid=1.09401,
        ask=1.09425,
    )
    denominator = requote(
        _stream(
            run,
            symbol="GBPUSD",
            anchor_midpoint=1.5,
            synthetic_midpoint=1.26171,
        ),
        bid=1.26152,
        ask=1.26190,
    )

    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams={
            "EURGBP": direct,
            "EURUSD": numerator,
            "GBPUSD": denominator,
        },
        config=config,
    )

    assert group.status is CrossCurrencyGroupStatus.RECONCILED
    assert len(group.projection_lineage) == 1
    assert group.projection_lineage[0].symbol == "gbpusd"
    preserved = next(
        event
        for event in group.stream_for("EURGBP").events
        if event.event_time_ns == SYNTHETIC_NS
    )
    projected = next(
        event
        for event in group.stream_for("GBPUSD").events
        if event.event_time_ns == SYNTHETIC_NS
    )
    assert preserved.bid == 0.86699
    assert preserved.ask == 0.86723
    assert projected.ask >= projected.bid
    assert group.generation_validation.anchor_preserved is True


def test_observed_conflict_and_missing_leg_refuse_without_projection() -> None:
    """Infeasible anchors and absent legs remain visible refused results."""
    config = eurusd_triangle_reconciliation_config()
    run = _run(config)
    window = _window(run)
    observed_conflict = _triangle_streams(
        run,
        direct_midpoint=0.9,
        middle_origin=SyntheticEventOrigin.OBSERVED,
    )
    refused = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=observed_conflict,
        config=config,
    )
    assert refused.status is CrossCurrencyGroupStatus.REFUSED
    assert refused.generation_validation.anchor_preserved is True
    assert refused.projection_lineage == ()
    assert any(
        reason.startswith("infeasible_relationship_point:")
        for reason in refused.generation_validation.failure_reasons
    )

    missing = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams={
            symbol: stream
            for symbol, stream in observed_conflict.items()
            if symbol != "EURGBP"
        },
        config=config,
    )
    assert missing.status is CrossCurrencyGroupStatus.REFUSED
    assert missing.missing_symbols == ("eurgbp",)
    assert "missing_symbol:eurgbp" in (
        missing.generation_validation.failure_reasons
    )


def test_many_observed_conflicts_return_bounded_auditable_refusal() -> None:
    """Large immutable-anchor conflicts refuse instead of overflowing evidence."""
    config = eurusd_triangle_reconciliation_config()
    run = _run(config)
    window = _window(run)
    midpoints = {"eurgbp": 0.9, "eurusd": 1.2, "gbpusd": 1.5}
    streams = {
        symbol: SyntheticEventStreamV1(
            run_id=run.run_id,
            ensemble_member_id=MEMBER,
            symbol=symbol,
            events=tuple(
                _observed(
                    run,
                    symbol=symbol,
                    timestamp_ns=START_NS + index * 1_000_000,
                    sequence=0,
                    row_id=index + 1,
                    midpoint=midpoint,
                )
                for index in range(130)
            ),
        )
        for symbol, midpoint in midpoints.items()
    }

    refused = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=streams,
        config=config,
    )

    assert refused.status is CrossCurrencyGroupStatus.REFUSED
    reasons = refused.generation_validation.failure_reasons
    assert len(reasons) == 128
    assert any(
        reason.startswith("failure_reasons_truncated:total=130:sha256=")
        for reason in reasons
    )
    assert refused.generation_validation.anchor_preserved is True
    assert (
        CrossCurrencyReconciledGroupV1.from_json(refused.to_json()) == refused
    )


def test_duplicate_and_asynchronous_times_are_bounded_without_forward_fill() -> (
    None
):
    """Duplicate ordinals pair deterministically while sparse support is surfaced."""
    config = eurusd_triangle_reconciliation_config()
    run = _run(config)
    window = _window(run)
    streams = _triangle_streams(
        run,
        eurusd_extras=(
            (SYNTHETIC_NS, 2, 1.2),
            (SYNTHETIC_NS + 100_000_000, 3, 1.21),
            (SYNTHETIC_NS + 200_000_000, 4, 1.22),
            (SYNTHETIC_NS + 300_000_000, 5, 1.23),
        ),
    )
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=streams,
        config=config,
    )

    assert group.status is CrossCurrencyGroupStatus.RECONCILED
    assert group.generation_validation.duplicate_timestamp_event_count == 1
    assert group.generation_validation.union_timestamp_count == 6
    assert group.generation_validation.common_timestamp_count == 3
    assert group.generation_validation.asynchronous_timestamp_count == 3
    assert group.generation_validation.stale_join_risk_count == 2
    support = group.generation_validation.relationship_support[0]
    assert support.support_count == 3
    assert len(group.stream_for("EURUSD").events) == 7


def test_inverse_relationship_projects_a_synthetic_leg() -> None:
    """The extensible relationship contract also enforces inverse pairs."""
    relationship = CrossCurrencyRelationshipV1.inverse(
        left="EURUSD",
        right="USDEUR",
        projection_priority=("EURUSD", "USDEUR"),
    )
    assert relationship.kind is CrossCurrencyRelationshipKind.INVERSE
    config = CrossCurrencyReconciliationConfigV1(relationships=(relationship,))
    run = _run(config, symbols=("EURUSD", "USDEUR"))
    window = _window(run)
    streams = {
        "EURUSD": _stream(
            run,
            symbol="EURUSD",
            anchor_midpoint=1.2,
            synthetic_midpoint=1.3,
        ),
        "USDEUR": _stream(
            run,
            symbol="USDEUR",
            anchor_midpoint=1.0 / 1.2,
            synthetic_midpoint=0.8,
        ),
    }
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=streams,
        config=config,
    )
    assert group.status is CrossCurrencyGroupStatus.RECONCILED
    left = next(
        event
        for event in group.stream_for("EURUSD").events
        if event.event_time_ns == SYNTHETIC_NS
    )
    right = next(
        event
        for event in group.stream_for("USDEUR").events
        if event.event_time_ns == SYNTHETIC_NS
    )
    assert left.bid == pytest.approx(1.0 / right.ask)
    assert left.ask == pytest.approx(1.0 / right.bid)
    assert ((left.bid + left.ask) / 2.0) * (
        (right.bid + right.ask) / 2.0
    ) == pytest.approx(1.0)


def test_existing_cross_instrument_rule_consumes_reconciled_group() -> None:
    """The #331 diagnostic validates group rows without a cache roundtrip."""
    _, _, _, group = _reconciled_group()
    report = cross_currency_quality_report(group, period="202001")
    payload = report.metadata[CROSS_INSTRUMENT_METADATA_KEY]

    assert report.status is QualityStatus.CLEAN
    assert payload["triangular_candidate_count"] == 1
    assert payload["triangular_compared_timestamp_count"] == 3
    assert payload["triangular_warning_count"] == 0
    assert payload["triangular_error_count"] == 0


def test_post_broker_validation_and_atomic_manifest_gate() -> None:
    """A complete group cannot commit without content-bound final validation."""
    run, window, inputs, group = _reconciled_group()
    post_broker = validate_cross_currency_output(
        run=run,
        window=window,
        streams={item.symbol: item for item in group.streams},
        config=group.config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=(
            event
            for stream in inputs.values()
            for event in stream.events
            if event.origin is SyntheticEventOrigin.OBSERVED
        ),
        conditions=(_condition(),),
    )
    assert post_broker.status is CrossCurrencyValidationStatus.PASSED
    manifest = _manifest(window, group)
    group.validate_atomic_manifest(
        manifest,
        post_broker_validation=post_broker,
    )

    direct = group.stream_for("EURGBP")
    bad_events = tuple(
        (
            replace(event, bid=0.8999, ask=0.9001, event_id="")
            if event.event_time_ns == SYNTHETIC_NS
            else event
        )
        for event in direct.events
    )
    bad_streams = {
        item.symbol: (
            SyntheticEventStreamV1(
                run_id=item.run_id,
                ensemble_member_id=item.ensemble_member_id,
                symbol=item.symbol,
                events=bad_events,
                source_version_ids=item.source_version_ids,
            )
            if item.symbol == "eurgbp"
            else item
        )
        for item in group.streams
    }
    failed = validate_cross_currency_output(
        run=run,
        window=window,
        streams=bad_streams,
        config=group.config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=(
            event
            for stream in inputs.values()
            for event in stream.events
            if event.origin is SyntheticEventOrigin.OBSERVED
        ),
    )
    assert failed.status is CrossCurrencyValidationStatus.FAILED
    with pytest.raises(ValueError, match="passing post-broker"):
        validate_cross_currency_atomic_manifest(
            window_scope=(
                run.run_id,
                window.window_id,
                window.synchronization_unit_id,
                MEMBER,
                run.symbols,
            ),
            streams=tuple(bad_streams.values()),
            manifest=manifest,
            post_broker_validation=failed,
        )
    with pytest.raises(ValueError, match="every synchronized symbol"):
        validate_cross_currency_atomic_manifest(
            window_scope=(
                run.run_id,
                window.window_id,
                window.synchronization_unit_id,
                MEMBER,
                run.symbols,
            ),
            streams=group.streams[:-1],
            manifest=manifest,
            post_broker_validation=post_broker,
        )


def _ref(kind: str, path: str, content: str) -> ArtifactRef:
    encoded = content.encode("utf-8")
    return ArtifactRef(
        kind=kind,
        path=path,
        size_bytes=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _manifest(
    window: ReconstructionWindowV1,
    group: CrossCurrencyReconciledGroupV1,
) -> PartitionManifestV1:
    batches = tuple(
        EventBatchV1(
            run_id=window.run_id,
            window_id=window.window_id,
            synchronization_unit_id=window.synchronization_unit_id,
            ensemble_member_id=window.ensemble_member_id,
            symbol=stream.symbol,
            batch_ordinal=0,
            event_count=len(stream.events),
            ownership_start_ns=window.core_start_ns,
            ownership_end_ns=window.core_end_ns,
            first_event_time_ns=window.core_start_ns,
            last_event_time_ns=window.core_end_ns - 1,
            content_sha256=hashlib.sha256(
                stream.to_json().encode("utf-8")
            ).hexdigest(),
            artifact=_ref(
                "synthetic-event-batch",
                f"scratch/{stream.symbol}.parquet",
                stream.to_json(),
            ),
        )
        for stream in group.streams
    )
    return PartitionManifestV1(
        run_id=window.run_id,
        window_id=window.window_id,
        synchronization_unit_id=window.synchronization_unit_id,
        ensemble_member_id=window.ensemble_member_id,
        symbols=window.symbols,
        symbol_event_counts={
            stream.symbol: len(stream.events) for stream in group.streams
        },
        event_batches=batches,
        rejection_summary_ref=_ref(
            "rejection-summary", "scratch/rejections.json", "{}"
        ),
        carry_state_ref=_ref("carry-state", "scratch/carry.json", "{}"),
    )

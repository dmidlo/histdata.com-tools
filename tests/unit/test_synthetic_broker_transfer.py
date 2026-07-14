"""End-to-end broker rendering over reconciled synthetic event groups."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from histdatacom.broker_capture import fit_broker_delivery_fingerprint
from histdatacom.synthetic import (
    BrokerRenderedGroupV1,
    BrokerTransferConfigV1,
    BrokerTransferStatus,
    HistoricalCarvingConstraintSetV1,
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    eurusd_triangle_reconciliation_config,
    reconcile_cross_currency_window,
    render_broker_delivery,
)
from tests.unit.test_broker_delivery_fingerprints import (
    BASE_WALL_NS,
    _capture,
)
from tests.unit.test_synthetic_cross_currency import (
    START_NS,
    _condition,
    _reconciled_group,
    _run,
    _stream,
    _window,
)


def test_render_is_deterministic_preserves_anchors_and_validates_final_group(
    tmp_path: Path,
) -> None:
    """Rendering binds profile lineage and emits only fully validated output."""
    run, window, group, constraints = _group_with_constraints()
    manifest = _capture(tmp_path, seed=21, wall_start_ns=BASE_WALL_NS)
    fingerprint = fit_broker_delivery_fingerprint(tmp_path, (manifest,))
    config = BrokerTransferConfigV1(
        strength=0.25,
        max_events_per_group=100,
    )

    rendered = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=config,
        quality_period="202001",
    )
    retry = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=config,
        quality_period="202001",
    )

    assert rendered == retry
    assert rendered.status is BrokerTransferStatus.APPLIED
    assert rendered.post_broker_validation is not None
    assert rendered.post_broker_validation.passed
    assert rendered.manifest.local_validation_passed
    assert rendered.manifest.cross_instrument_quality_status == "clean"
    assert rendered.manifest.fingerprint_id == fingerprint.fingerprint_id
    assert rendered.manifest.selections[0].profile_effective_start_utc_ns == (
        fingerprint.effective_start_utc_ns
    )
    assert rendered.manifest.to_dict()[
        "profile_effective_periods_embedded_in_selections"
    ]
    assert rendered.manifest.synthetic_event_count == len(
        rendered.event_lineage
    )
    assert rendered.manifest.action_counts
    before_anchors = _observed_payloads(group.streams)
    after_anchors = _observed_payloads(rendered.streams)
    assert after_anchors == before_anchors
    assert {
        event.broker_profile_id
        for stream in rendered.streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.SYNTHETIC
    } == {fingerprint.fingerprint_id}
    assert BrokerRenderedGroupV1.from_json(rendered.to_json()) == rendered


def test_unsupported_profile_cell_refuses_without_exposing_partial_rows(
    tmp_path: Path,
) -> None:
    """A missing symbol cell never silently falls back to global support."""
    run, window, group, constraints = _group_with_constraints()
    manifest = _capture(tmp_path, seed=22, wall_start_ns=BASE_WALL_NS)
    fingerprint = fit_broker_delivery_fingerprint(tmp_path, (manifest,))

    refused = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        requested_conditions={
            "EURGBP": {"symbol": "EURGBP"},
            "EURUSD": {"symbol": "EURUSD"},
            "GBPUSD": {"symbol": "GBPUSD"},
        },
        config=BrokerTransferConfigV1(max_events_per_group=100),
    )

    assert refused.status is BrokerTransferStatus.REFUSED
    assert refused.streams == ()
    assert refused.event_lineage == ()
    assert any(
        "requested_condition_absent" in reason
        for reason in refused.manifest.reason_codes
    )
    assert BrokerRenderedGroupV1.from_json(refused.to_json()) == refused


def test_render_resource_limit_refuses_before_materializing_output(
    tmp_path: Path,
) -> None:
    """Event amplification and in-flight rows stay bounded by config."""
    run, window, group, constraints = _group_with_constraints()
    manifest = _capture(tmp_path, seed=23, wall_start_ns=BASE_WALL_NS)
    fingerprint = fit_broker_delivery_fingerprint(tmp_path, (manifest,))

    refused = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=BrokerTransferConfigV1(max_events_per_group=1),
    )

    assert refused.status is BrokerTransferStatus.REFUSED
    assert refused.manifest.reason_codes == ("max_events_per_group_exceeded",)
    assert refused.manifest.output_content_sha256 is None


def test_render_applies_measured_batching_to_dense_synthetic_rows(
    tmp_path: Path,
) -> None:
    """Batch presentation is exercised independently of stale behavior."""
    run, window, group, constraints = _group_with_constraints(dense=True)
    manifest = _capture(tmp_path, seed=24, wall_start_ns=BASE_WALL_NS)
    fingerprint = _profile_with_metrics(
        fit_broker_delivery_fingerprint(tmp_path, (manifest,)),
        source_batch_quote_count=3.0,
    )

    rendered = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=BrokerTransferConfigV1(
            strength=1.0,
            apply_stale_behavior=False,
            apply_exact_duplicates=False,
            max_events_per_group=100,
        ),
        quality_period="202001",
    )

    assert rendered.status is BrokerTransferStatus.APPLIED
    assert rendered.manifest.action_counts["batched_timestamp"] > 0


def test_render_applies_measured_stale_quotes_independently(
    tmp_path: Path,
) -> None:
    """A fully supported stale rate repeats prior synthetic presentation."""
    run, window, group, constraints = _group_with_constraints(dense=True)
    manifest = _capture(tmp_path, seed=25, wall_start_ns=BASE_WALL_NS)
    fingerprint = _profile_with_metrics(
        fit_broker_delivery_fingerprint(tmp_path, (manifest,)),
        stale_quote_rate=1.0,
        exact_duplicate_rate=0.0,
    )

    rendered = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=BrokerTransferConfigV1(
            strength=1.0,
            apply_exact_duplicates=False,
            apply_batching=False,
            max_events_per_group=100,
        ),
        quality_period="202001",
    )

    assert rendered.status is BrokerTransferStatus.APPLIED
    assert rendered.manifest.action_counts["stale_quote"] > 0
    assert "exact_duplicate_quote" not in rendered.manifest.action_counts


def test_render_applies_measured_exact_duplicates_independently(
    tmp_path: Path,
) -> None:
    """Exact-duplicate presentation remains distinct from stale behavior."""
    run, window, group, constraints = _group_with_constraints(dense=True)
    manifest = _capture(tmp_path, seed=26, wall_start_ns=BASE_WALL_NS)
    fingerprint = _profile_with_metrics(
        fit_broker_delivery_fingerprint(tmp_path, (manifest,)),
        stale_quote_rate=0.0,
        exact_duplicate_rate=1.0,
    )

    rendered = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=fingerprint.effective_start_utc_ns,
        config=BrokerTransferConfigV1(
            strength=1.0,
            apply_stale_behavior=False,
            apply_batching=False,
            max_events_per_group=100,
        ),
        quality_period="202001",
    )

    assert rendered.status is BrokerTransferStatus.APPLIED
    assert rendered.manifest.action_counts["exact_duplicate_quote"] > 0
    assert "stale_quote" not in rendered.manifest.action_counts


def _group_with_constraints(*, dense: bool = False):
    if dense:
        config = eurusd_triangle_reconciliation_config()
        run = _run(config)
        window = _window(run)
        inputs = {
            symbol: _stream(
                run,
                symbol=symbol,
                anchor_midpoint=midpoint,
                synthetic_midpoint=midpoint,
                extra_synthetic=(
                    (START_NS + 250_000_000, 2, midpoint),
                    (START_NS + 750_000_000, 3, midpoint),
                ),
            )
            for symbol, midpoint in {
                "EURGBP": 0.8,
                "EURUSD": 1.2,
                "GBPUSD": 1.5,
            }.items()
        }
    else:
        run, window, inputs, initial = _reconciled_group()
        config = initial.config
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fingerprint-constraints:fixture"
    )
    streams: dict[str, SyntheticEventStreamV1] = {}
    for symbol, stream in inputs.items():
        events = tuple(
            (
                replace(
                    event,
                    constraint_set_id=constraints.constraint_set_id,
                    event_id="",
                )
                if event.origin is SyntheticEventOrigin.SYNTHETIC
                else event
            )
            for event in stream.events
        )
        streams[symbol] = SyntheticEventStreamV1(
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            symbol=stream.symbol,
            events=events,
        )
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams=streams,
        config=config,
        conditions=(_condition(),),
    )
    return run, window, group, constraints


def _profile_with_metrics(fingerprint, **values: float):
    cells = []
    for cell in fingerprint.cells:
        if cell.condition.key != "global":
            cells.append(cell)
            continue
        metrics = tuple(
            (
                replace(
                    metric,
                    estimate=values[metric.name],
                    lower=values[metric.name],
                    upper=values[metric.name],
                    metric_id="",
                )
                if metric.name in values
                else metric
            )
            for metric in cell.metrics
        )
        cells.append(replace(cell, metrics=metrics, cell_id=""))
    return replace(fingerprint, cells=tuple(cells), fingerprint_id="")


def _observed_payloads(streams):
    return {
        event.event_id: event.to_dict()
        for stream in streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    }

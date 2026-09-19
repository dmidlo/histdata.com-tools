"""Opt-in reference retrieval from replay-verified causal bar state."""

from __future__ import annotations

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarFeatureConsumerResultV1,
    BarFeatureSourceV1,
    BarFeatureState,
    CausalBarSnapshotV1,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.motifs import (
    ReferenceMotifConditionV1,
    ReferenceMotifIndexV1,
    ReferenceMotifQueryV1,
    query_reference_motifs,
)


def query_reference_motifs_with_bar_state(
    index: ReferenceMotifIndexV1,
    *,
    source: BarFeatureSourceV1,
    snapshot: CausalBarSnapshotV1,
    information_mode: InformationMode,
    scope: ActivitySliceScope,
    interval_code: str,
    max_results: int = 16,
) -> BarFeatureConsumerResultV1:
    """Execute existing motif retrieval using two semantically equal metrics.

    Median spread, tick-level volatility and interarrival metrics are NOT
    approximated from bar means. Unknown categories remain unknown. This is
    an explicit experiment, not a change to frozen campaign selection.
    """
    source.verify_snapshot(snapshot, information_mode=information_mode)
    cell = snapshot.cell(scope, interval_code)
    if cell.state is not BarFeatureState.AVAILABLE:
        raise ValueError(
            "reference conditioning requires a complete closed bar"
        )
    metrics = {}
    for feature_name, metric in (
        ("mid_log_open_close_return", "return_value"),
        ("tick_intensity_per_second", "tick_intensity"),
    ):
        value = cell.feature(feature_name)
        if value.state is not BarFeatureState.AVAILABLE:
            raise ValueError("reference conditioning feature is unavailable")
        assert value.value is not None
        metrics[metric] = float(value.value)
    bar = next(
        bar
        for bar in snapshot.bars
        if bar.bar_id
        == cell.feature("mid_log_open_close_return").source_bar_ids[-1]
    )
    if len(bar.feed_epoch_ids) > 1:
        raise ValueError(
            "reference conditioning requires an unambiguous feed epoch"
        )
    condition = ReferenceMotifConditionV1(
        symbol=snapshot.symbol,
        feed_epoch_id=(
            bar.feed_epoch_ids[0] if bar.feed_epoch_ids else "unclassified"
        ),
        session_state="unknown",
        metrics=metrics,
    )
    query = ReferenceMotifQueryV1(
        condition=condition,
        information_mode=information_mode,
        used_at_ns=snapshot.decision_time_ns,
        as_of_ns=(
            snapshot.decision_time_ns
            if information_mode is InformationMode.EX_ANTE_SIMULATION
            else None
        ),
        max_results=max_results,
    )
    result = query_reference_motifs(index, query)
    return BarFeatureConsumerResultV1(
        consumer="reference_conditioning",
        information_mode=information_mode,
        snapshot_ids=(snapshot.artifact_id,),
        policy_ids=(snapshot.policy.artifact_id,),
        result_json=canonical_bar_feature_json(
            {
                "conversion_version": "histdatacom.causal-bar-motif-metrics.v1",
                "scope": scope.value,
                "interval_code": interval_code,
                "query": query.to_dict(),
                "retrieval": result.to_dict(),
            }
        ),
    )


__all__ = ["query_reference_motifs_with_bar_state"]

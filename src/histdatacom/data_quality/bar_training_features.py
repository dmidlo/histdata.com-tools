"""Row-exact, opt-in causal bar state for the existing tick training frame."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from histdatacom.data_quality.training_features import (
    enrich_tick_cache_with_training_features,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    MAX_BAR_FEATURE_COLLECTION_ITEMS,
    BarFeatureSourceV1,
    CausalBarSnapshotV1,
    bar_feature_definitions,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.information import InformationMode

CAUSAL_BAR_TRAINING_NAMESPACE = "causal_bar_v1"


def enrich_tick_cache_with_causal_bar_features(
    frame: Any,
    *,
    source: BarFeatureSourceV1,
    snapshots: Sequence[CausalBarSnapshotV1],
    information_mode: InformationMode,
    symbol: str,
    period: str,
) -> Any:
    """Execute tick enrichment then append exact-cutoff bar feature columns.

    Rows use the existing UTC millisecond tick timestamp as decision cutoff.
    No nearest-time joins, row multiplication, implicit fill or partial-bar
    substitution occur. Multiple ticks at one cutoff reuse exactly one state.
    This observed-tick API permits only observed state; generated training
    surfaces require their own explicit origin-aware row contract.
    """
    import polars as pl

    if not 1 <= len(snapshots) <= MAX_BAR_FEATURE_COLLECTION_ITEMS:
        raise ValueError("training snapshots exceed their bounded input count")
    if any(
        name.startswith(CAUSAL_BAR_TRAINING_NAMESPACE + "__")
        for name in frame.columns
    ):
        raise ValueError(
            "causal bar training evidence cannot be silently overwritten"
        )
    indexed = {}
    for snapshot in snapshots:
        source.verify_snapshot(snapshot, information_mode=information_mode)
        if snapshot.symbol != symbol or snapshot.policy.scopes != (
            ActivitySliceScope.OBSERVED,
        ):
            raise ValueError(
                "observed tick training requires matching observed-only state"
            )
        if snapshot.decision_time_ns in indexed:
            raise ValueError("training snapshot cutoff ownership is ambiguous")
        indexed[snapshot.decision_time_ns] = snapshot
    if len({item.policy.artifact_id for item in snapshots}) != 1:
        raise ValueError(
            "training snapshots must share one exact feature policy"
        )
    enriched = enrich_tick_cache_with_training_features(
        frame,
        symbol=symbol,
        data_format="ascii",
        timeframe="T",
        period=period,
    )
    times = enriched.get_column("timestamp_utc_ms").to_list()
    if any(type(value) is not int for value in times):
        raise ValueError(
            "training rows require exact non-null integer timestamps"
        )
    cutoffs = [value * 1_000_000 for value in times]
    if set(cutoffs) != set(indexed):
        raise ValueError(
            "training requires exact row-cutoff snapshots without orphan evidence"
        )
    rows = []
    for cutoff in cutoffs:
        snapshot = indexed[cutoff]
        prefix = CAUSAL_BAR_TRAINING_NAMESPACE + "__"
        row: dict[str, object] = {
            prefix + "snapshot_id": snapshot.artifact_id,
            prefix + "policy_id": snapshot.policy.artifact_id,
            prefix + "decision_time_ns": cutoff,
            prefix + "information_mode": snapshot.policy.information_mode.value,
            prefix + "historical_availability_verified": False,
            prefix + "replay_clock_assumption": snapshot.uses_replay_clock,
        }
        for cell in snapshot.cells:
            cell_prefix = (
                prefix + cell.scope.value + "__" + cell.interval_code + "__"
            )
            row[cell_prefix + "state"] = cell.state.value
            row[cell_prefix + "partial"] = cell.partial
            for feature in cell.values:
                feature_prefix = cell_prefix + feature.name + "__"
                row[feature_prefix + "value"] = feature.value
                row[feature_prefix + "state"] = feature.state.value
                row[feature_prefix + "available_at_ns"] = (
                    feature.available_at_ns
                )
                row[feature_prefix + "source_bar_ids_json"] = (
                    canonical_bar_feature_json(
                        {"ids": list(feature.source_bar_ids)}
                    )
                )
        rows.append(row)
    # Freeze nullable column types even when every retained value is missing.
    # Existing training eligibility/diagnostic columns keep their old meaning;
    # this information mode applies only to the added causal_bar_v1 namespace.
    definitions = {item.name: item for item in bar_feature_definitions()}
    schema: dict[str, Any] = {}
    for name in rows[0]:
        if (
            name.endswith("__partial")
            or name.endswith("__historical_availability_verified")
            or name.endswith("__replay_clock_assumption")
        ):
            schema[name] = pl.Boolean
        elif name.endswith("__available_at_ns") or name.endswith(
            "__decision_time_ns"
        ):
            schema[name] = pl.Int64
        elif name.endswith("__value"):
            feature_name = name.split("__")[-2]
            schema[name] = (
                pl.Int64
                if definitions[feature_name].units == "events"
                else pl.Float64
            )
        else:
            schema[name] = pl.String
    extension = pl.DataFrame(rows, schema_overrides=schema)
    if enriched.height != extension.height:
        raise ValueError("causal training projection changed row support")
    return enriched.hstack(extension)


__all__ = [
    "CAUSAL_BAR_TRAINING_NAMESPACE",
    "enrich_tick_cache_with_causal_bar_features",
]

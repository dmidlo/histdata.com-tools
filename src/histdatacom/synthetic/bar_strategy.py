"""Explicit diagnostic strategy evaluation using causal decision-time bars."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    MAX_BAR_FEATURE_COLLECTION_ITEMS,
    BarFeatureConsumerResultV1,
    BarFeatureSourceV1,
    BarFeatureState,
    CausalBarSnapshotV1,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.bars import load_derived_bar_manifest
from histdatacom.synthetic.information import InformationAuditReportV1
from histdatacom.synthetic.strategy_sensitivity import (
    StrategyEvaluationPlanV1,
    StrategyQuoteV1,
    StrategySignalEngineV1,
    StrategySourceKind,
    evaluate_strategy_sensitivity,
)


@dataclass(frozen=True, slots=True)
class CausalBarStrategyInputV1:
    """Source plus ordered snapshots; legacy normalized quotes are not inputs."""

    source: BarFeatureSourceV1
    snapshots: tuple[CausalBarSnapshotV1, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.source, BarFeatureSourceV1):
            raise TypeError(
                "causal strategy requires a replay-verifiable source"
            )
        if not 1 <= len(self.snapshots) <= MAX_BAR_FEATURE_COLLECTION_ITEMS:
            raise ValueError("causal strategy snapshot count exceeds bounds")
        object.__setattr__(self, "snapshots", tuple(self.snapshots))
        if any(
            not isinstance(item, CausalBarSnapshotV1) for item in self.snapshots
        ):
            raise TypeError(
                "causal strategy accepts snapshots, not legacy quotes"
            )
        cutoffs = tuple(item.decision_time_ns for item in self.snapshots)
        if cutoffs != tuple(sorted(set(cutoffs))):
            raise ValueError(
                "strategy snapshot cutoffs must be strictly increasing"
            )


def evaluate_causal_bar_strategy(
    plan: StrategyEvaluationPlanV1,
    inputs: Mapping[str, CausalBarStrategyInputV1],
    information_audits: Mapping[str, InformationAuditReportV1],
    engine: StrategySignalEngineV1,
) -> BarFeatureConsumerResultV1:
    """Run the existing engine with quotes timestamped at decision cutoff.

    Availability assertions cannot validate an actual historical backtest, so
    every such experiment must carry an explicit invalid-for-backtest reason.
    Existing information audits are still required by the existing evaluator;
    they never override the source/snapshot admission rules in this module.
    """
    if not plan.invalid_for_backtest_reason:
        raise ValueError(
            "causal bar availability assertions require a diagnostic invalid-for-backtest reason"
        )
    if set(inputs) != {case.case_id for case in plan.cases}:
        raise ValueError("causal strategy requires exact planned case inputs")
    modes = {case.information_mode for case in plan.cases}
    if len(modes) != 1:
        raise ValueError("causal strategy cannot mix information modes")
    streams: dict[str, tuple[StrategyQuoteV1, ...]] = {}
    snapshots: dict[str, CausalBarSnapshotV1] = {}
    for case in plan.cases:
        if case.source_kind is not StrategySourceKind.DERIVED_BARS:
            raise ValueError(
                "causal strategy accepts only explicit derived-bar cases"
            )
        item = inputs[case.case_id]
        if not isinstance(item, CausalBarStrategyInputV1):
            raise TypeError("causal strategy requires typed snapshot inputs")
        manifest = load_derived_bar_manifest(item.source.bar_manifest_path)
        if case.source_artifact_id != manifest.manifest_id:
            raise ValueError(
                "strategy case source identity differs from verified bar product"
            )
        scope = ActivitySliceScope(case.source_scope)
        assert case.bar_interval_code is not None
        quotes = []
        for snapshot in item.snapshots:
            item.source.verify_snapshot(
                snapshot, information_mode=case.information_mode
            )
            if (
                snapshot.symbol != case.symbol
                or not case.start_ns <= snapshot.decision_time_ns < case.end_ns
            ):
                raise ValueError("strategy snapshot lies outside its case axis")
            cell = snapshot.cell(scope, case.bar_interval_code)
            if cell.state is not BarFeatureState.AVAILABLE:
                raise ValueError(
                    "strategy refuses missing or partial bar state"
                )
            close = cell.feature("bid_close")
            bar = next(
                bar
                for bar in snapshot.bars
                if bar.bar_id == close.source_bar_ids[-1]
            )
            if (
                bar.run_id != case.run_id
                or bar.ensemble_member_id != case.ensemble_member_id
            ):
                raise ValueError(
                    "strategy snapshot run/member differs from case"
                )
            if len(bar.feed_epoch_ids) > 1 or len(bar.broker_profile_ids) > 1:
                raise ValueError(
                    "strategy bar has ambiguous epoch/profile ownership"
                )
            profile = (
                bar.broker_profile_ids[0] if bar.broker_profile_ids else None
            )
            if (
                case.broker_profile_id is not None
                and case.broker_profile_id != profile
            ):
                raise ValueError("strategy broker profile differs from source")
            quotes.append(
                StrategyQuoteV1(
                    source_event_id=snapshot.artifact_id,
                    symbol=snapshot.symbol,
                    event_time_ns=snapshot.decision_time_ns,
                    event_sequence=0,
                    bid=bar.bid_close,
                    ask=bar.ask_close,
                    epoch_id=(
                        bar.feed_epoch_ids[0]
                        if bar.feed_epoch_ids
                        else "unclassified"
                    ),
                    session="unclassified",
                    event_state="unclassified",
                    sparsity="causal_closed_bar",
                    ensemble_member_id=bar.ensemble_member_id,
                    broker_profile_id=profile,
                    source_scope=scope.value,
                    bar_interval_code=case.bar_interval_code,
                )
            )
            snapshots[snapshot.artifact_id] = snapshot
        streams[case.case_id] = tuple(quotes)
    report = evaluate_strategy_sensitivity(
        plan, streams, information_audits, engine
    )
    return BarFeatureConsumerResultV1(
        consumer="strategy",
        information_mode=next(iter(modes)),
        snapshot_ids=tuple(sorted(snapshots)),
        policy_ids=tuple(
            sorted({item.policy.artifact_id for item in snapshots.values()})
        ),
        result_json=canonical_bar_feature_json(
            {
                "quote_clock": "decision_time_ns",
                "report": report.to_dict(),
                "case_quote_ids": {
                    key: [quote.quote_id for quote in values]
                    for key, values in sorted(streams.items())
                },
            }
        ),
    )


__all__ = ["CausalBarStrategyInputV1", "evaluate_causal_bar_strategy"]

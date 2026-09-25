"""Executable row-preserving wide joins with fresh source/value replay."""

from __future__ import annotations

from .training_join_contracts import (
    TrainingJoinBatchV1,
    TrainingJoinPlanV1,
    TrainingJoinRowV1,
)
from .training_join_sources import join_value, prepare_training_join_sources


def materialize_training_joins(plan: TrainingJoinPlanV1) -> TrainingJoinBatchV1:
    if type(plan) is not TrainingJoinPlanV1:
        raise TypeError("join consumer requires a complete typed plan")
    adapters = prepare_training_join_sources(plan)
    rows = tuple(
        TrainingJoinRowV1(
            row.artifact_id,
            row.evidence_unit_id,
            row.decision_time_ns,
            tuple(
                join_value(
                    adapters[column.source_key],
                    column,
                    row,
                    plan.information_mode,
                )
                for column in plan.columns
            ),
        )
        for row in plan.spine.rows
    )
    result = TrainingJoinBatchV1(plan, rows)
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        require_provider_operation,
    )
    from .training_provider_policy import training_provider_subject

    subject = training_provider_subject(result)
    if subject is not None:
        require_provider_operation(subject, BrokerPolicyOperation.MATERIAL_USE)
        require_provider_operation(subject, BrokerPolicyOperation.DERIVE)
    return result


def replay_training_joins(batch: TrainingJoinBatchV1) -> TrainingJoinBatchV1:
    if type(batch) is not TrainingJoinBatchV1:
        raise TypeError("join replay requires the complete typed batch")
    expected = materialize_training_joins(batch.plan)
    if expected != batch:
        raise ValueError("joined values/null lineage differ from source replay")
    return expected


def training_join_records(
    batch: TrainingJoinBatchV1,
) -> tuple[dict[str, object], ...]:
    """Public executed consumer; no wide row duplicates or invisible filling."""
    from .training_contracts import training_load

    replay_training_joins(batch)
    return tuple(
        {
            "spine_row_id": row.spine_row_id,
            "evidence_unit_id": row.evidence_unit_id,
            "spine": spine.to_dict(),
            "spine_consumer_mode": batch.plan.spine.request.consumer_mode.value,
            "join_information_mode": batch.plan.information_mode.value,
            "historical_availability_verified": False,
            **{
                cell.column: training_load(cell.value_json)["value"]
                for cell in row.values
            },
            "join_states": {
                cell.column: cell.state.value for cell in row.values
            },
        }
        for spine, row in zip(batch.plan.spine.rows, batch.rows)
    )

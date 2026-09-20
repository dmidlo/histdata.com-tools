"""Actual verified observed/native row emitters, not provenance descriptors.

The narrow quote/event spine deliberately excludes the legacy enriched frame's
full-period diagnostics. Ex-post source integrity is not historical knowledge.
"""

from __future__ import annotations

import bisect
from typing import Any

from .training_contracts import (
    MAX_TRAINING_BYTES,
    MAX_TRAINING_ITEMS,
    NO_LABEL_SCHEMA,
    TrainingBatchV1,
    TrainingConsumerMode,
    TrainingInformationMode,
    TrainingOrigin,
    TrainingOwnershipV1,
    TrainingRequestV1,
    TrainingRowV1,
    TrainingSourceV1,
    admitted_modes,
    training_json,
    training_load,
)
from .training_lineage import verify_training_ownership

OBSERVED_FEATURE_SCHEMA = "histdatacom.training-observed-quote-spine.v1"
NATIVE_FEATURE_SCHEMA = "histdatacom.training-native-event-spine.v1"
CONTEXT_FEATURE_SCHEMA = "histdatacom.training-normalized-context-cell.v1"
FORECAST_FEATURE_SCHEMA = (
    "histdatacom.training-machine-forecast-distribution.v1"
)


def materialize_training_rows(
    source: TrainingSourceV1,
    ownership: TrainingOwnershipV1,
    request: TrainingRequestV1,
) -> TrainingBatchV1:
    """Reverify all inputs and emit every selected row exactly once.

    The immutable source inventory, not this request's window/member/symbols,
    determines independence. All currently supported rows are explicitly ex-post
    because these legacy sources do not retain historical-availability receipts.
    """
    if (
        type(request) is not TrainingRequestV1
        or type(ownership) is not TrainingOwnershipV1
    ):
        raise TypeError(
            "training materialization requires typed request/ownership"
        )
    if request.consumer_mode is TrainingConsumerMode.CAUSAL:
        raise ValueError(
            "legacy source does not verify historical availability"
        )
    verified = verify_training_ownership(source, ownership)
    if not set(request.symbols) <= set(verified.graph_symbols):
        raise ValueError("requested symbol escapes complete evidence graph")
    if request.start_ns < verified.start_ns or request.end_ns > verified.end_ns:
        raise ValueError("requested interval escapes source coverage")
    starts = [unit.start_ns for unit in ownership.units]
    ownership_id = ownership.artifact_id
    info = TrainingInformationMode.EX_POST
    rows: list[TrainingRowV1] = []
    retained = (
        len(source.to_json())
        + len(ownership.to_json())
        + len(request.to_json())
    )

    def append(
        *,
        time: int,
        symbol: str | None,
        origin: TrainingOrigin,
        key: str,
        value: dict[str, object],
        run: str | None = None,
        member: str | None = None,
        scenarios: tuple[str, ...] = (),
        uncertainty: dict[str, object] | None = None,
        decision: int | None = None,
        feature_schema: str | None = None,
        available: int | None = None,
    ) -> None:
        nonlocal retained
        if len(rows) >= MAX_TRAINING_ITEMS:
            raise ValueError(
                "selected training row count exceeds bound; request a slice"
            )
        unit = ownership.units[bisect.bisect_right(starts, time) - 1]
        row = TrainingRowV1(
            unit.artifact_id,
            ownership_id,
            origin,
            info,
            time,
            time if decision is None else decision,
            available,
            symbol,
            source.dataset_version_id,
            key,
            run,
            member,
            scenarios,
            training_json(
                uncertainty or {"confidence": None, "status": "unavailable"}
            ),
            feature_schema
            or (
                OBSERVED_FEATURE_SCHEMA
                if run is None
                else NATIVE_FEATURE_SCHEMA
            ),
            NO_LABEL_SCHEMA,
            training_json(value),
            verified.roots,
            admitted_modes(origin, info),
        )
        retained += len(row.to_json()) + 1
        if retained > MAX_TRAINING_BYTES - 1024:
            raise ValueError(
                "selected training payload exceeds aggregate budget"
            )
        rows.append(row)

    if request.feature_artifact_id is not None:
        from histdatacom.forecasting.feature_contracts import FeatureKind
        from histdatacom.forecasting.feature_forecasts import (
            ForecastFeatureSnapshotV1,
        )
        from histdatacom.forecasting.feature_store import (
            FeatureMatrixSnapshotV1,
        )

        if request.consumer_mode is not TrainingConsumerMode.DESCRIPTIVE:
            raise ValueError(
                "normalized context/forecast rows are descriptive only"
            )
        if request.symbols != verified.graph_symbols:
            raise ValueError(
                "context rows require full graph; no invented FX attribution"
            )
        features = [
            f
            for f in verified.features
            if f.root.upstream_id == request.feature_artifact_id
        ]
        if len(features) != 1:
            raise ValueError(
                "feature artifact is absent from admitted inventory"
            )
        feature = features[0]
        artifact = feature.artifact
        decision = request.decision_time_ns
        if decision is None:
            decision = feature.end_ns
        if decision < feature.end_ns:
            raise ValueError(
                "context decision precedes complete retained dependency support"
            )
        if isinstance(artifact, FeatureMatrixSnapshotV1):
            defined_keys = {
                o.definition.feature_key for o in artifact.observations
            }
            if not {
                c.feature_key for c in artifact.request.columns
            } <= defined_keys or any(
                o.definition.kind is not FeatureKind.MACRO
                for o in artifact.observations
            ):
                raise ValueError(
                    "context origin requires retained macro definitions for every key"
                )
            for index, cell in enumerate(artifact.cells):
                time = cell.period.end_ns - 1
                if not request.start_ns <= time < request.end_ns:
                    continue
                append(
                    time=time,
                    symbol=None,
                    origin=TrainingOrigin.OFFICIAL_CONTEXT,
                    key=f"{feature.root.upstream_id}|cell|{index}",
                    value=dict(cell.to_dict()),
                    decision=decision,
                    available=cell.available_at_ns,
                    feature_schema=CONTEXT_FEATURE_SCHEMA,
                )
        elif isinstance(artifact, ForecastFeatureSnapshotV1):
            time = artifact.cutoff.cutoff_at_ns
            if request.start_ns <= time < request.end_ns:
                append(
                    time=time,
                    symbol=None,
                    origin=TrainingOrigin.MACHINE_FORECAST,
                    key=feature.root.upstream_id,
                    value={
                        "point": artifact.distribution.point,
                        "point_statistic": artifact.distribution.point_statistic.value,
                        "distribution_json": training_json(
                            artifact.distribution.to_dict()
                        ),
                        "target_json": training_json(artifact.target.to_dict()),
                        "unit": artifact.target.unit,
                    },
                    decision=decision,
                    available=artifact.generated_at_ns,
                    feature_schema=FORECAST_FEATURE_SCHEMA,
                    uncertainty={
                        "distribution_id": artifact.distribution.to_dict()[
                            "id"
                        ],
                        "status": "retained_predictive_not_calibrated",
                    },
                )
        else:
            raise ValueError("unsupported feature-row origin")
    elif request.product_manifest_id is None:
        selected = tuple(
            row
            for row in verified.observed
            if row.symbol in request.symbols
            and request.start_ns <= row.event_time_ns < request.end_ns
        )
        if len(selected) > MAX_TRAINING_ITEMS:
            raise ValueError(
                "selected training row count exceeds bound; request a slice"
            )
        for row in selected:
            values = row.payload()
            values.pop("vol")
            values["volume_state"] = "unavailable_pending_source_semantics"
            append(
                time=row.event_time_ns,
                symbol=row.symbol,
                origin=TrainingOrigin.OBSERVED,
                key=f"{source.dataset_version_id}|{row.series_id}|{row.period}|{row.row_id}",
                value=values,
                decision=request.decision_time_ns,
            )
    else:
        matches = [
            p
            for p in verified.products
            if p.manifest.manifest_id == request.product_manifest_id
        ]
        if len(matches) != 1:
            raise ValueError(
                "product is absent from frozen admitted dependency inventory"
            )
        product = matches[0]
        if not set(request.symbols) <= {
            s.upper() for s in product.manifest.symbols
        }:
            raise ValueError("requested symbol is absent from product")
        decision = request.decision_time_ns
        if decision is None:
            decision = product.dependency_end_ns
        if decision < product.dependency_end_ns:
            raise ValueError(
                "native decision precedes full ex-post product dependency support"
            )
        for event in product.events:
            if (
                event.symbol.upper() not in request.symbols
                or not request.start_ns <= event.event_time_ns < request.end_ns
            ):
                continue
            origin = (
                TrainingOrigin.OBSERVED
                if event.origin.value == "observed"
                else (
                    TrainingOrigin.BROKER_CONDITIONED_COUNTERFACTUAL
                    if event.broker_profile_id is not None
                    else TrainingOrigin.SYNTHETIC_RECONSTRUCTION
                )
            )
            append(
                time=event.event_time_ns,
                symbol=event.symbol.upper(),
                origin=origin,
                key=f"{product.manifest.manifest_id}|{event.event_id}",
                value=dict(event.to_dict()),
                run=event.run_id,
                member=event.ensemble_member_id,
                scenarios=tuple(
                    sorted(
                        set(
                            product.manifest.constraints.generator_config_ids
                            + product.manifest.constraints.constraint_set_ids
                        )
                    )
                ),
                uncertainty={
                    "confidence": event.confidence,
                    "status": "retained_not_calibrated",
                    "ensemble_manifest_id": product.manifest.ensemble.ensemble_manifest_id,
                },
                decision=decision,
            )
    return TrainingBatchV1(source, ownership, request, tuple(rows))


def replay_training_batch(batch: TrainingBatchV1) -> TrainingBatchV1:
    """Recompute membership, values, units, lineage and admissibility from roots."""
    if type(batch) is not TrainingBatchV1:
        raise TypeError("training replay requires its complete typed batch")
    replayed = materialize_training_rows(
        batch.source, batch.ownership, batch.request
    )
    if replayed != batch:
        raise ValueError("training rows or lineage differ from source replay")
    return replayed


def training_frame(
    batch: TrainingBatchV1,
    *,
    consumer_mode: TrainingConsumerMode,
    value_columns: tuple[str, ...] | None = None,
) -> Any:
    """Return a useful row-aligned frame with mandatory origin/ownership fields.

    Projection selects value columns only: mandatory lineage cannot be excluded.
    This API cannot prevent a caller subsequently dropping DataFrame columns;
    that transformed frame is no longer a verified training-substrate artifact.
    """
    import polars as pl  # pylint: disable=import-outside-toplevel

    if (
        type(consumer_mode) is not TrainingConsumerMode
        or consumer_mode != batch.request.consumer_mode
    ):
        raise ValueError("consumer must declare the exact bound training mode")
    replay_training_batch(batch)
    native = batch.request.product_manifest_id is not None
    feature_id = batch.request.feature_artifact_id
    names: tuple[str, ...]
    if feature_id is not None:
        # Scalar projection stays useful without flattening nested provenance.
        forecast = any(
            row.origin is TrainingOrigin.MACHINE_FORECAST for row in batch.rows
        )
        if not batch.rows:
            forecast = feature_id.startswith("forecast-feature-snapshot:")
        names = (
            (
                "point",
                "point_statistic",
                "distribution_json",
                "target_json",
                "unit",
            )
            if forecast
            else (
                "column",
                "value",
                "status",
                "unit",
                "available_at_ns",
                "reference_age_ns",
            )
        )
    elif native:
        from histdatacom.synthetic.contracts import (
            SYNTHETIC_EVENT_ARROW_COLUMNS,
        )

        names = tuple(SYNTHETIC_EVENT_ARROW_COLUMNS)
    else:
        names = (
            "symbol",
            "source_series_id",
            "source_period",
            "source_row_id",
            "event_time_ns",
            "bid",
            "ask",
            "volume_state",
        )
    columns = names if value_columns is None else value_columns
    if (
        type(columns) is not tuple
        or len(set(columns)) != len(columns)
        or not set(columns) <= set(names)
    ):
        raise ValueError("invalid training value-column projection")
    floats = {"bid", "ask", "confidence", "point", "value"}
    integers = {
        "event_time_ns",
        "event_sequence",
        "source_row_id",
        "vol",
        "available_at_ns",
        "reference_age_ns",
    }
    schema: dict[str, Any] = {
        f"value.{name}": (
            pl.Float64
            if name in floats
            else pl.Int64 if name in integers else pl.String
        )
        for name in columns
    }
    lineage_names = (
        "row_id",
        "evidence_unit_id",
        "ownership_id",
        "origin",
        "information_mode",
        "consumer_mode",
        "observed_dataset_version_id",
        "source_row_key",
        "run_id",
        "ensemble_member_id",
        "scenario_ids",
        "uncertainty",
        "feature_schema_version",
        "label_schema_version",
        "verification_roots",
        "nonclaims",
    )
    schema.update({f"lineage.{name}": pl.String for name in lineage_names})
    schema.update(
        {
            f"lineage.{name}": pl.Int64
            for name in ("event_time_ns", "decision_time_ns", "available_at_ns")
        }
    )
    # JSON bounds also bound repeated lineage strings; guard projected cell count.
    if len(schema) * len(batch.rows) > 250_000:
        raise ValueError(
            "training frame cell budget exceeded; project or slice"
        )
    records = []
    for row in batch.rows:
        payload = training_load(row.value_json)
        record = {f"value.{name}": payload[name] for name in columns}
        record.update(
            {
                "lineage.row_id": row.artifact_id,
                "lineage.evidence_unit_id": row.evidence_unit_id,
                "lineage.ownership_id": row.ownership_id,
                "lineage.origin": row.origin.value,
                "lineage.information_mode": row.information_mode.value,
                "lineage.consumer_mode": consumer_mode.value,
                "lineage.observed_dataset_version_id": row.observed_dataset_version_id,
                "lineage.source_row_key": row.source_row_key,
                "lineage.run_id": row.run_id,
                "lineage.ensemble_member_id": row.ensemble_member_id,
                "lineage.scenario_ids": training_json(list(row.scenario_ids)),
                "lineage.uncertainty": row.uncertainty_json,
                "lineage.feature_schema_version": row.feature_schema_version,
                "lineage.label_schema_version": row.label_schema_version,
                "lineage.verification_roots": training_json(
                    [r.to_dict() for r in row.verification_roots]
                ),
                "lineage.nonclaims": training_json(list(row.nonclaims)),
                "lineage.event_time_ns": row.event_time_ns,
                "lineage.decision_time_ns": row.decision_time_ns,
                "lineage.available_at_ns": row.available_at_ns,
            }
        )
        records.append(record)
    return pl.DataFrame(records, schema=schema)

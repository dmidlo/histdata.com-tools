"""Native unit/definition projections; source replay remains a separate gate."""

from __future__ import annotations

from pathlib import Path

from .training_contracts import training_json, training_load
from .training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    TrainingJoinColumnV1,
    TrainingJoinSourceV1,
)
from .training_lineage import read_training_regular
from .training_wide_contracts import (
    TrainingWideColumnV1,
    WideValueType,
)


def _one_unit(values: tuple[str | None, ...]) -> str | None:
    units = set(values)
    if len(units) > 1:
        raise ValueError(
            "wide column crosses incompatible native unit definitions"
        )
    return next(iter(units), None)


def _describe_training_wide_column(
    source: TrainingJoinSourceV1,
    column: TrainingJoinColumnV1,
    *,
    information_mode: JoinInformationMode,
) -> tuple[TrainingWideColumnV1, tuple[str, ...]]:
    """Project declared native semantics, never grant source/rights admission.

    The materializer additionally executes every complete native join plan and
    checks these descriptors again against the same retained control inventory.
    Unknown native units remain explicit null, never an invented measurement.
    """
    if (
        type(source) is not TrainingJoinSourceV1
        or type(column) is not TrainingJoinColumnV1
    ):
        raise TypeError(
            "wide descriptors require exact native source/column types"
        )
    if (
        type(information_mode) is not JoinInformationMode
        or source.key != column.source_key
    ):
        raise ValueError(
            "wide descriptor has a different source or information mode"
        )
    family = source.family
    units: str | None = None
    timeframe: str | None = None
    lag = warmup = 0
    step_unit = "not_applicable"
    dtype = WideValueType.NUMBER
    basis: tuple[str, ...] = ()
    semantics: dict[str, object] = {
        "family": family.value,
        "field": column.field,
    }
    if family is JoinFamily.TICK:
        if column.coordinate or column.field not in (
            "bid",
            "ask",
            "midpoint",
            "spread",
        ):
            raise ValueError("unknown native quote field or coordinate")
        units = "quote_currency_per_base_currency"
        semantics["definition"] = "training-quote-value.v1"
    elif family in (JoinFamily.BAR, JoinFamily.INDICATOR):
        from histdatacom.synthetic.bar_features import bar_feature_definitions

        parts = column.coordinate.split(":")
        if len(parts) != 2:
            raise ValueError("wide bar column lacks exact scope/timeframe")
        timeframe = parts[1]
        step_unit = "closed_bars"
        definitions = {item.name: item for item in bar_feature_definitions()}
        if column.field not in definitions:
            raise ValueError("wide column lacks a native bar definition")
        definition = definitions[column.field]
        units, lag, warmup = (
            definition.units,
            definition.lag_bars,
            definition.required_closed_bars,
        )
        semantics["definition_id"] = definition.artifact_id
        if units == "events":
            dtype = WideValueType.INTEGER
    elif family is JoinFamily.TRIANGLE:
        from histdatacom.synthetic.triangle_bar_features import (
            triangle_feature_definitions,
        )

        parts = column.coordinate.split(":")
        if len(parts) != 2:
            raise ValueError("wide triangle column lacks exact scope/timeframe")
        timeframe = parts[1]
        step_unit = "closed_bars"
        triangle_definitions = {
            item.name: item for item in triangle_feature_definitions()
        }
        if column.field in triangle_definitions:
            triangle = triangle_definitions[column.field]
            units, lag, warmup = (
                triangle.units,
                triangle.lag_bars,
                triangle.required_closed_bars_per_leg,
            )
            semantics["definition_id"] = triangle.artifact_id
        elif column.field.startswith("projection."):
            field = column.field.removeprefix("projection.")
            if field in ("tuple_count", "projected_tuple_count"):
                units, dtype = "tuples", WideValueType.INTEGER
            elif field in ("mean_pre_log_residual", "mean_post_log_residual"):
                units = "log_ratio"
            elif field in ("l1_quote_displacement_total", "spread_scale_total"):
                units = "sum_of_leg_quote_units"
            elif field in (
                "projected_rate",
                "mean_normalized_burden",
                "spread_weighted_burden",
            ):
                units = "dimensionless"
            elif field in (
                "mean_pre_absolute_constraint_residual",
                "mean_post_absolute_constraint_residual",
            ):
                units = "EURUSD_quote_currency_per_base_currency"
            else:
                raise ValueError("unknown wide triangle projection definition")
            warmup = 1
            semantics["definition"] = "causal-bar-triangle-projection.v1"
        else:
            raise ValueError("wide column lacks a native triangle definition")
    elif family is JoinFamily.ACTIVITY:
        from histdatacom.synthetic.activity import (
            ReconstructionActivityManifestV1,
        )

        if len(source.evidence_json) != 1:
            raise ValueError(
                "wide activity requires exact native metric evidence"
            )
        native = ReconstructionActivityManifestV1.from_dict(
            training_load(source.evidence_json[0])
        )
        basis = (native.manifest_id,)
        metrics = tuple(
            m
            for item in native.slices
            if item.symbol.upper() == column.entity.symbol
            and item.scope.value == column.coordinate
            for m in item.metrics
            if m.name == column.field
        )
        if not metrics:
            raise ValueError("wide activity metric is absent")
        units = _one_unit(tuple(m.unit for m in metrics))
        semantics["aggregation"] = metrics[0].aggregation.value
        semantics["native_semantics"] = metrics[0].semantics.value
    elif family is JoinFamily.BROKER:
        from histdatacom.broker_capture.fingerprint_v2 import (
            parse_broker_delivery_fingerprint,
        )

        if len(source.evidence_json) != 1:
            raise ValueError(
                "wide broker column requires exact native fingerprint"
            )
        fingerprint = parse_broker_delivery_fingerprint(source.evidence_json[0])
        basis = (fingerprint.fingerprint_id,)
        broker_metrics = tuple(
            m
            for cell in fingerprint.cells
            if cell.condition.key == column.coordinate
            for m in cell.metrics
            if m.name == column.field
        )
        if not broker_metrics:
            raise ValueError("wide broker metric is absent")
        units = _one_unit(tuple(m.unit for m in broker_metrics))
        semantics["definition"] = broker_metrics[0].schema_version
    elif family in (JoinFamily.VINTAGE, JoinFamily.FORECAST):
        from histdatacom.forecasting.feature_artifacts import (
            read_feature_artifact,
        )
        from histdatacom.forecasting.feature_forecasts import (
            ForecastFeatureSnapshotV1,
        )
        from histdatacom.forecasting.feature_store import (
            FeatureMatrixSnapshotV1,
        )

        if len(source.paths) != 1:
            raise ValueError("wide feature column requires one native artifact")
        read_training_regular(Path(source.paths[0]))
        artifact = read_feature_artifact(source.paths[0])
        if (
            family is JoinFamily.FORECAST
            and type(artifact) is ForecastFeatureSnapshotV1
        ):
            basis = (artifact.snapshot_id,)
            units = artifact.target.unit
            semantics["definition"] = "forecast-target-unit.v1"
        elif (
            family is JoinFamily.VINTAGE
            and type(artifact) is FeatureMatrixSnapshotV1
        ):
            from histdatacom.forecasting.feature_contracts import (
                FeatureTransformKind,
            )

            basis = (artifact.snapshot_id,)
            specifications = tuple(
                c
                for c in artifact.request.columns
                if c.name == column.field
                and c.feature_key == column.entity.series
            )
            if len(specifications) != 1:
                raise ValueError(
                    "wide macro column lacks exact native transform"
                )
            specification = specifications[0]
            transform = specification.transform
            semantics["transform"] = transform.to_dict()
            semantics["max_age_ns"] = specification.max_age_ns
            step_unit = "reference_periods"
            if transform.kind in (
                FeatureTransformKind.LAG,
                FeatureTransformKind.DIFFERENCE,
            ):
                lag = transform.window
                warmup = transform.window + 1
            elif transform.kind is not FeatureTransformKind.LEVEL:
                warmup = transform.min_support
            macro_definitions = tuple(
                o.definition
                for o in artifact.observations
                if o.definition.feature_key == column.entity.series
            )
            units = (
                "standard-deviations"
                if transform.kind is FeatureTransformKind.ROLLING_ZSCORE
                else _one_unit(tuple(d.unit for d in macro_definitions))
            )
            scales = {d.scale for d in macro_definitions}
            if len(scales) > 1:
                raise ValueError(
                    "wide macro column crosses incompatible native scales"
                )
            semantics["scale"] = next(iter(scales), None)
            semantics["definition"] = "retained-feature-definition.v1"
        else:
            raise ValueError("wide feature artifact kind differs")
    elif family is JoinFamily.CALENDAR:
        from histdatacom.market_context.economic_calendar import (
            replay_economic_calendar_corpus,
        )

        if len(source.paths) != 1:
            raise ValueError("wide calendar requires one native corpus")
        read_training_regular(Path(source.paths[0]))
        corpus = replay_economic_calendar_corpus(source.paths[0])
        basis = (corpus.corpus_id,)
        if column.field == "occurrence":
            units, dtype = "occurrences", WideValueType.INTEGER
        elif column.field == "scheduled_for_ns":
            units, dtype = "UTC_nanoseconds", WideValueType.INTEGER
        elif column.field == "actual_value":
            units = _one_unit(
                tuple(
                    r.unit
                    for r in corpus.releases
                    if r.series_key == column.entity.series
                    and (
                        not column.coordinate
                        or r.reference_period == column.coordinate
                    )
                    and column.entity.currency == r.currency
                    and column.entity.economy == r.economy_code
                    and (
                        column.entity.symbol in r.affected_symbols
                        or column.entity.currency in r.affected_currencies
                    )
                    and r.released_at_ns is not None
                )
            )
        else:
            raise ValueError("unknown wide calendar field")
        semantics["definition"] = "retained-calendar-release.v1"
    elif family is JoinFamily.POSITIONING:
        if "pct_of_oi" in column.field:
            units = "percent_of_reported_open_interest"
        elif "conc_" in column.field:
            units = "percent_concentration_of_reported_open_interest"
        elif "traders_" in column.field:
            units = "reported_traders"
        elif column.field.startswith(
            (
                "open_interest_",
                "change_in_open_interest_",
                "positions_",
                "change_in_positions_",
            )
        ):
            units = "reported_futures_contracts_not_spot_volume"
        # The native parser admits substring-matched numeric fields, not a
        # closed unit registry. Unmapped names remain explicitly unknown.
        semantics["definition"] = "native-cftc-field.v1"
    elif family is JoinFamily.UNSUPPORTED:
        dtype = WideValueType.UNAVAILABLE
        semantics["definition"] = "reserved_no_verified_emitter"
    else:
        raise ValueError("unsupported wide source family")
    return (
        TrainingWideColumnV1(
            column,
            family,
            dtype,
            units,
            timeframe,
            lag,
            warmup,
            step_unit,
            "native_join_direction_age_dependency_and_explicit_null.v1",
            information_mode,
            training_json(semantics),
        ),
        basis,
    )


def describe_training_wide_column(
    source: TrainingJoinSourceV1,
    column: TrainingJoinColumnV1,
    *,
    information_mode: JoinInformationMode,
) -> TrainingWideColumnV1:
    """Return a declaration; only materialization binds it to replayed values."""
    return _describe_training_wide_column(
        source, column, information_mode=information_mode
    )[0]

"""Native-replayed preprocessing; pure numerical objects are not authority."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from fractions import Fraction
from typing import cast

from .training_contracts import training_json, training_load
from .training_join_contracts import JoinMeaning, JoinState
from .training_preprocessing_contracts import (
    MAX_PREPROCESSING_CELLS,
    MAX_PREPROCESSING_OUTPUT_COLUMNS,
    PREPROCESSING_NONCLAIMS,
    STATELESS_DENSE_FAMILIES,
    PreprocessingFitMode,
    PreprocessingMissingness,
    TrainingPreprocessingFitStepV1,
    TrainingPreprocessingFitV1,
    TrainingPreprocessingPlanV1,
    TrainingPreprocessingStepV1,
    readmit_preprocessing_contract,
)
from .training_preprocessing_math import (
    PROJECTIONS,
    ReferenceTransformFitV1,
    Scalar,
    apply_reference_transform,
    fit_reference_transform,
)
from .training_preprocessing_native import (
    NativePreprocessingCell,
    NativePreprocessingEvidence,
    NativePreprocessingRow,
    assert_training_preprocessing_native_current,
    inspect_training_preprocessing_native,
    select_training_preprocessing_fit,
)
from .training_temporal_contracts import TemporalPartition

FROZEN_DONOR_POLICY = "frozen-fit-train-donors-plus-causal-nontrain.v1"
CAUSAL_DONOR_POLICY = "supplied-application-history-causal-originals.v1"


def _identity(kind: str, value: object) -> str:
    return kind + ":sha256:" + _digest(value)


def _digest(value: object) -> str:
    return hashlib.sha256(training_json(value).encode("ascii")).hexdigest()


def _fresh_plan(
    plan: TrainingPreprocessingPlanV1,
) -> TrainingPreprocessingPlanV1:
    fresh = readmit_preprocessing_contract(plan, TrainingPreprocessingPlanV1)
    if fresh.artifact_id != plan.artifact_id:
        raise ValueError(
            "preprocessing plan raw fields differ from cached identity"
        )
    return fresh


def _base_name(name: str) -> str:
    return name.removeprefix("state.") if name.startswith("state.") else name


def _native_cell(
    row: NativePreprocessingRow, name: str
) -> NativePreprocessingCell:
    matches = [cell for cell in row.cells if cell.column == _base_name(name)]
    if len(matches) != 1:
        raise ValueError("preprocessing column lacks exactly one native cell")
    return matches[0]


def _evidence_value(
    row: NativePreprocessingRow, name: str
) -> int | float | str | bool | None:
    cell = _native_cell(row, name)
    if name.startswith("state."):
        return (
            cell.state.value
            if cell.state is not None
            else "missing_spine_anchor"
        )
    value: int | float | str | bool | None = cell.original_value
    if value is None or type(value) in (int, float, str, bool):
        return value
    raise ValueError("native evidence value has unsupported scalar type")


def _value(row: NativePreprocessingRow, name: str) -> Scalar:
    value = _evidence_value(row, name)
    if type(value) is bool:
        raise ValueError(
            "boolean fields need an explicit categorical representation"
        )
    if value is None or type(value) in (int, float, str):
        return value
    raise ValueError("native preprocessing value has unsupported scalar type")


def _schema(plan: TrainingPreprocessingPlanV1) -> dict[str, dict[str, object]]:
    native = {column.column.name: column for column in plan.wide_plan.columns}
    result: dict[str, dict[str, object]] = {}
    for name in plan.columns:
        column = native[_base_name(name)]
        state = name.startswith("state.")
        descriptor = {
            "name": name,
            "native_definition": column.to_dict(),
            "kind": "native_missingness_category" if state else "native_value",
            "ontology": (
                [s.value for s in JoinState] + ["missing_spine_anchor"]
                if state
                else None
            ),
        }
        result[name] = {
            "lineage_id": _identity("preprocessing-input-lineage", descriptor),
            "descriptor": descriptor,
        }
    return result


@dataclass(frozen=True, slots=True)
class TrainingEvidenceViewV1:
    """Process-local nullable evidence. Consumers must replay the retained plan."""

    plan: TrainingPreprocessingPlanV1
    native: NativePreprocessingEvidence
    nonclaims: tuple[str, ...] = PREPROCESSING_NONCLAIMS


def materialize_training_evidence_view(
    plan: TrainingPreprocessingPlanV1,
) -> TrainingEvidenceViewV1:
    admitted = _fresh_plan(plan)
    native = inspect_training_preprocessing_native(
        admitted.wide_plan, admitted.split
    )
    return TrainingEvidenceViewV1(admitted, native)


def _ratio(value: Fraction) -> str:
    return str(value)


def _stateless(
    step: TrainingPreprocessingStepV1,
    rows: tuple[NativePreprocessingRow, ...],
    table: list[dict[str, Scalar]],
    evidence: dict[str, object] | None = None,
    fit_member_rows: frozenset[str] | None = None,
) -> tuple[tuple[Scalar, ...], ...]:
    """Carry only original, clocked persistent states within one realization.

    Imputed or transformed outputs are never themselves donor evidence. A
    calendar event, closed bar or cutoff-specific snapshot cannot be carried.
    """
    result = []
    maximum = cast(int, training_load(step.options_json).get("max_age_ns", 0))
    for row, record in zip(rows, table):
        values = []
        for name in step.columns:
            cell = _native_cell(row, name)
            if name.startswith("state."):
                raise ValueError(
                    "stateless fill requires a native value column"
                )
            if record[name] != cell.original_value:
                raise ValueError(
                    "stateless fill cannot follow a learned transform"
                )
            value = record[name]
            if step.family == "confirmed_zero":
                proof = cell.provenance
                cutoff = cell.decision_time_ns
                if (
                    value is None
                    and cell.state is JoinState.CONFIRMED_ABSENCE
                    and proof is not None
                    and cutoff is not None
                    and proof.available_at_ns is not None
                    and proof.available_at_ns <= cutoff
                    and proof.source_time_ns is not None
                    and proof.source_time_ns <= cutoff
                    and proof.dependency_end_ns is not None
                    and proof.dependency_end_ns <= cutoff + 1
                ):
                    value = 0
            else:
                if cell.definition.column.meaning is not JoinMeaning.STATE:
                    raise ValueError(
                        "prior fill requires native persistent state"
                    )
                if (
                    value is None
                    and cell.state
                    in (
                        JoinState.UNAVAILABLE,
                        JoinState.STALE,
                        JoinState.SOURCE_OUTAGE,
                    )
                    and row.native_row is not None
                    and cell.decision_time_ns is not None
                ):
                    candidates = []
                    cutoff = cell.decision_time_ns
                    for donor_row in rows:
                        if (
                            fit_member_rows is not None
                            and donor_row.wide_row_id not in fit_member_rows
                            and (
                                row.partition is TemporalPartition.TRAIN
                                or donor_row.partition
                                not in (
                                    TemporalPartition.VALIDATION,
                                    TemporalPartition.TEST,
                                )
                            )
                        ):
                            continue
                        donor = _native_cell(donor_row, name)
                        proof = donor.provenance
                        if (
                            donor_row.native_row is not None
                            and donor_row.native_row.symbol
                            == row.native_row.symbol
                            and donor_row.member_key == row.member_key
                            and donor_row.stratum_id == row.stratum_id
                            and donor.state is JoinState.AVAILABLE
                            and proof is not None
                            and proof.source_time_ns is not None
                            and proof.available_at_ns is not None
                            and proof.dependency_end_ns is not None
                            and donor.decision_time_ns is not None
                            and donor.decision_time_ns < cutoff
                            and proof.available_at_ns <= cutoff
                            and proof.dependency_end_ns <= cutoff + 1
                            and 0 <= cutoff - proof.source_time_ns <= maximum
                        ):
                            candidates.append(
                                (
                                    proof.source_time_ns,
                                    donor.original_value,
                                    donor_row.wide_row_id,
                                    proof,
                                )
                            )
                    if candidates:
                        latest = max(item[0] for item in candidates)
                        admitted = {
                            item[1] for item in candidates if item[0] == latest
                        }
                        if len(admitted) != 1:
                            raise ValueError(
                                "ambiguous equally recent prior state"
                            )
                        value = cast(Scalar, next(iter(admitted)))
                        if evidence is not None:
                            evidence[row.wide_row_id + "/" + name] = {
                                "method": "prior_state",
                                "age_ns": cutoff - latest,
                                "policy_id": step.artifact_id,
                                "donor_policy": (
                                    CAUSAL_DONOR_POLICY
                                    if fit_member_rows is None
                                    else FROZEN_DONOR_POLICY
                                ),
                                "donors": [
                                    {
                                        "row_id": item[2],
                                        "provenance": item[3].to_dict(),
                                    }
                                    for item in candidates
                                    if item[0] == latest
                                ],
                            }
            values.append(value)
        result.append(tuple(values))
    return tuple(result)


def fit_training_preprocessing(
    plan: TrainingPreprocessingPlanV1, *, cutoff_ns: int | None = None
) -> TrainingPreprocessingFitV1:
    """Derive actual membership, then execute each closed transform in order.

    No external coefficients, row arrays, weights or fit-success flags are
    accepted. Stateful downstream steps see only earlier TRAIN-fitted outputs.
    """
    plan = _fresh_plan(plan)
    if plan.fit_mode is PreprocessingFitMode.NONE:
        raise ValueError("none mode does not produce fitted parameters")
    if (
        plan.fit_mode is PreprocessingFitMode.TRAIN_FIT
        and cutoff_ns is not None
    ):
        raise ValueError("train_fit cannot silently become a rolling fit")
    if (
        plan.fit_mode is PreprocessingFitMode.FIXED_EXTERNAL
        and cutoff_ns is None
    ):
        raise ValueError("fixed external calibration requires a known cutoff")
    native = inspect_training_preprocessing_native(plan.wide_plan, plan.split)
    selected = select_training_preprocessing_fit(
        native,
        fit_mode=plan.fit_mode.value,
        fit_unit_ids=plan.fit_unit_ids,
        cutoff_ns=cutoff_ns,
        rolling_window_ns=plan.rolling_window_ns,
        fold_id=plan.fold_id,
        columns=tuple(sorted({_base_name(name) for name in plan.columns})),
    )
    _, _, stages = _pipeline(plan, selected.rows, selected.weights)
    membership = {
        "fit_selection_id": selected.parameter_fit_identity,
        "fold_id": plan.fold_id,
        "mode": plan.fit_mode.value,
        "cutoff_ns": cutoff_ns,
        "fit_unit_ids": list(selected.fit_unit_ids),
        "rows": [
            {
                "row_id": row.wide_row_id,
                "unit_id": row.evidence_unit_id,
                "member_key": row.member_key,
                "stratum_id": row.stratum_id,
                "mass": _ratio(weight),
            }
            for row, weight in zip(selected.rows, selected.weights)
        ],
    }
    support = {
        "mass_policy": selected.mass_policy,
        "selected_mass": _ratio(selected.selected_mass),
        "withheld_mass": _ratio(selected.withheld_mass),
        "excluded_rows": [list(item) for item in selected.excluded_rows],
        "availability_scope": selected.availability_scope,
        "historical_availability_verified": selected.historical_availability_verified,
        "native_inventory_scope": native.inventory_scope,
    }
    result = TrainingPreprocessingFitV1(
        plan,
        cutoff_ns,
        _identity(
            "preprocessing-parameters",
            {
                "fit_selection_id": selected.parameter_fit_identity,
                "steps": [s.to_dict() for s in stages],
            },
        ),
        training_json(membership),
        training_json(support),
        tuple(stages),
    )
    assert_training_preprocessing_native_current(native)
    return result


def _pipeline(
    plan: TrainingPreprocessingPlanV1,
    rows: tuple[NativePreprocessingRow, ...],
    weights: tuple[Fraction, ...],
    fitted: tuple[TrainingPreprocessingFitStepV1, ...] | None = None,
    imputation_evidence: dict[str, object] | None = None,
    fit_member_rows: frozenset[str] | None = None,
) -> tuple[
    list[dict[str, Scalar]],
    dict[str, dict[str, object]],
    tuple[TrainingPreprocessingFitStepV1, ...],
]:
    table = [{name: _value(row, name) for name in plan.columns} for row in rows]
    if fitted is None and plan.fit_mode in (
        PreprocessingFitMode.TRAIN_FIT,
        PreprocessingFitMode.ROLLING_PTI,
    ):
        # Fitting receives only admitted selected rows. Freeze that same TRAIN
        # donor universe for later application, including inner folds/windows.
        fit_member_rows = frozenset(row.wide_row_id for row in rows)
    descriptors = _schema(plan)
    stages = []
    dependencies: dict[str, tuple[str, ...]] = {
        name: (name,) for name in plan.columns
    }
    paths: dict[str, list[dict[str, object]]] = {
        name: [] for name in plan.columns
    }
    histories: list[dict[str, list[dict[str, object]]]] = [
        {name: [] for name in plan.columns} for _ in rows
    ]
    for index, step in enumerate(plan.steps):
        if not set(step.columns) <= set(descriptors):
            raise ValueError(
                "preprocessing step references an unavailable prior output"
            )
        options = step.options_json
        if step.family == "categorical":
            ontology = _identity(
                "preprocessing-ontology",
                [descriptors[name] for name in step.columns],
            )
            supplied = training_load(options)
            if (
                "ontology_id" in supplied
                and supplied["ontology_id"] != ontology
            ):
                raise ValueError(
                    "categorical ontology differs from native column semantics"
                )
            options = training_json({**supplied, "ontology_id": ontology})
        inputs = tuple(
            tuple(row[name] for name in step.columns) for row in table
        )
        stateless_evidence: dict[str, object] = {}
        if step.family in STATELESS_DENSE_FAMILIES:
            output = _stateless(
                step, rows, table, stateless_evidence, fit_member_rows
            )
            kernel_json = training_json(
                {
                    "kind": "preprocessing-stateless",
                    "step": step.to_dict(),
                    "donor_policy": (
                        (
                            FROZEN_DONOR_POLICY
                            if plan.fit_mode
                            in (
                                PreprocessingFitMode.TRAIN_FIT,
                                PreprocessingFitMode.ROLLING_PTI,
                            )
                            else CAUSAL_DONOR_POLICY
                        )
                        if step.family == "prior_state"
                        else "known-absence-at-row-cutoff.v1"
                    ),
                }
            )
            kernel_id = _identity(
                "preprocessing-stateless", training_load(kernel_json)
            )
            output_columns = step.columns
        else:
            kernel = (
                fit_reference_transform(
                    step.family, step.columns, inputs, weights, options
                )
                if fitted is None
                else ReferenceTransformFitV1.from_json(
                    fitted[index].kernel_fit_json
                )
            )
            output = apply_reference_transform(kernel, inputs)
            kernel_json, kernel_id = kernel.to_json(), kernel.fit_id
            output_columns = kernel.output_columns
        remaining = tuple(
            name for name in descriptors if name not in step.columns
        )
        if set(remaining) & set(output_columns):
            raise ValueError(
                "preprocessing output aliases an untransformed column"
            )
        if (
            len(remaining) + len(output_columns)
            > MAX_PREPROCESSING_OUTPUT_COLUMNS
            or len(table) * (len(remaining) + len(output_columns))
            > MAX_PREPROCESSING_CELLS
        ):
            raise ValueError("preprocessing pipeline output budget exceeded")
        parents = tuple(
            str(descriptors[name]["lineage_id"]) for name in step.columns
        )
        input_hash = _digest([descriptors[name] for name in step.columns])
        replacements: dict[str, dict[str, object]] = {
            name: {
                "lineage_id": _identity(
                    "preprocessing-output-lineage",
                    {
                        "step": step.artifact_id,
                        "kernel": kernel_id,
                        "parents": list(parents),
                        "column": name,
                    },
                ),
                "descriptor": {
                    "family": step.family,
                    "kernel": kernel_id,
                    "column": name,
                },
            }
            for name in output_columns
        }
        stages.append(
            TrainingPreprocessingFitStepV1(
                step.artifact_id,
                kernel_json,
                input_hash,
                _digest([replacements[name] for name in output_columns]),
                parents,
            )
        )
        if fitted is not None and stages[-1] != fitted[index]:
            raise ValueError(
                "application schema/lineage differs from fitted pipeline"
            )
        source_columns = {
            name: (
                step.columns
                if step.family in PROJECTIONS
                else (
                    (step.columns[int(name.split(":", 1)[0])],)
                    if step.family == "categorical"
                    else (name,)
                )
            )
            for name in output_columns
        }
        new_paths: dict[str, list[dict[str, object]]] = {}
        for name, parents_for_output in source_columns.items():
            inherited = {
                training_json(event): event
                for parent in parents_for_output
                for event in paths[parent]
            }
            new_paths[name] = [
                *inherited.values(),
                {
                    "step_id": step.artifact_id,
                    "family": step.family,
                    "version": step.version,
                    "kernel_id": kernel_id,
                    "input_columns": list(parents_for_output),
                },
            ]
        paths = {**{name: paths[name] for name in remaining}, **new_paths}
        for row_index, (row, values) in enumerate(zip(rows, output)):
            updated = {name: histories[row_index][name] for name in remaining}
            for name, value in zip(output_columns, values):
                inherited_events = {
                    training_json(event): event
                    for parent in source_columns[name]
                    for event in histories[row_index][parent]
                }
                events = list(inherited_events.values())
                if (
                    step.family in ("median_imputer", *STATELESS_DENSE_FAMILIES)
                    and table[row_index][name] is None
                    and value is not None
                ):
                    event: dict[str, object] = {
                        "method": step.family,
                        "policy_id": step.artifact_id,
                        "kernel_id": kernel_id,
                        "column": name,
                        "confidence": "not_calibrated",
                    }
                    if step.family == "median_imputer":
                        event["fit_mode"] = plan.fit_mode.value
                        event["support"] = cast(
                            list[object],
                            training_load(kernel.support_json)["columns"],
                        )[step.columns.index(name)]
                    elif step.family == "prior_state":
                        event.update(
                            cast(
                                dict[str, object],
                                stateless_evidence[
                                    row.wide_row_id + "/" + name
                                ],
                            )
                        )
                    else:
                        event["support"] = "native_confirmed_absence"
                    events.append(event)
                updated[name] = events
            histories[row_index] = updated
        dependencies = {
            **{name: dependencies[name] for name in remaining},
            **{
                name: tuple(
                    sorted(
                        {
                            ancestor
                            for parent in parents_for_output
                            for ancestor in dependencies[parent]
                        }
                    )
                )
                for name, parents_for_output in source_columns.items()
            },
        }
        descriptors = {
            **{name: descriptors[name] for name in remaining},
            **replacements,
        }
        table = [
            {
                **{name: row[name] for name in remaining},
                **dict(zip(output_columns, values)),
            }
            for row, values in zip(table, output)
        ]
    if imputation_evidence is not None:
        for row, history in zip(rows, histories):
            for name, events in history.items():
                if events:
                    imputation_evidence[row.wide_row_id + "/" + name] = {
                        **events[-1],
                        "events": events,
                    }
    return (
        table,
        {
            name: {
                **descriptor,
                "native_dependencies": list(dependencies[name]),
                "transforms": paths[name],
            }
            for name, descriptor in descriptors.items()
        },
        tuple(stages),
    )


def replay_training_preprocessing_fit(
    fit: TrainingPreprocessingFitV1, *, expected_fit_id: str
) -> TrainingPreprocessingFitV1:
    """Freshly repeat native admission and fit, not merely verify a hash."""
    if (
        type(expected_fit_id) is not str
        or re.fullmatch(
            r"training-preprocessing-fit:sha256:[0-9a-f]{64}", expected_fit_id
        )
        is None
    ):
        raise ValueError(
            "preprocessing replay requires an exact expected fit identity"
        )
    fresh = readmit_preprocessing_contract(fit, TrainingPreprocessingFitV1)
    if (
        fresh.artifact_id != expected_fit_id
        or fit.artifact_id != expected_fit_id
    ):
        raise ValueError("preprocessing fit differs from the expected subject")
    actual = fit_training_preprocessing(fresh.plan, cutoff_ns=fresh.cutoff_ns)
    if actual.to_json() != fresh.to_json():
        raise ValueError("preprocessing fit differs from complete native refit")
    return actual


@dataclass(frozen=True, slots=True)
class TrainingPreprocessingViewV1:
    """Process-local convenience result, not a serialized evidence certificate.

    ``records`` is a detached snapshot. Use ``preprocessing_records`` to repeat
    current source admission before relying on or exporting a previous result.
    """

    plan: TrainingPreprocessingPlanV1
    fit: TrainingPreprocessingFitV1 | None
    records_json: str
    require_dense: bool
    cutoff_ns: int | None
    nonclaims: tuple[str, ...] = PREPROCESSING_NONCLAIMS

    @property
    def records(self) -> tuple[dict[str, object], ...]:
        return tuple(
            cast(
                list[dict[str, object]],
                training_load(self.records_json)["records"],
            )
        )


def _export(value: Scalar) -> int | float | str | None:
    if isinstance(value, Fraction):
        value = float(value)
    if type(value) is float and not math.isfinite(value):
        raise ValueError("preprocessing export is not finite")
    if value is not None and type(value) not in (int, float, str):
        raise TypeError("preprocessing export requires an exact scalar")
    result: int | float | str | None = value
    return result


def _missingness(cell: NativePreprocessingCell) -> str:
    if (
        cell.state is JoinState.CONFIRMED_ABSENCE
        and cell.provenance is not None
        and cell.decision_time_ns is not None
        and cell.provenance.available_at_ns is not None
        and cell.provenance.available_at_ns > cell.decision_time_ns
    ):
        return PreprocessingMissingness.NOT_YET_AVAILABLE.value
    if cell.provenance is not None and cell.provenance.refusal_evidence:
        cutoff = cell.decision_time_ns
        if (
            cell.state is JoinState.UNAVAILABLE
            and cutoff is not None
            and any(
                (
                    proof.available_at_ns is not None
                    and proof.available_at_ns > cutoff
                )
                or proof.source_time_ns > cutoff
                or proof.dependency_end_ns > cutoff + 1
                for proof in cell.provenance.refusal_evidence
            )
        ):
            return PreprocessingMissingness.NOT_YET_AVAILABLE.value
        if cell.state not in (
            JoinState.UNKNOWN_AVAILABILITY,
            JoinState.SOURCE_OUTAGE,
            JoinState.EXPECTED_CLOSURE,
        ):
            return PreprocessingMissingness.REFUSED.value
    if cell.state is JoinState.AVAILABLE:
        return (
            "observed"
            if cell.provenance is not None
            and cell.provenance.origin == "observed"
            else "derived"
        )
    return {
        None: "missing_spine_anchor",
        JoinState.UNAVAILABLE: "unavailable",
        JoinState.UNSUPPORTED: "unsupported",
        JoinState.NOT_APPLICABLE: "not_applicable",
        JoinState.CONFIRMED_ABSENCE: "confirmed_absence",
        JoinState.EMPTY_MARKET_INTERVAL: "empty_market_interval",
        JoinState.INSUFFICIENT_WARMUP: "insufficient_warmup",
        JoinState.SOURCE_OUTAGE: "source_outage",
        JoinState.EXPECTED_CLOSURE: "market_closed",
        JoinState.STALE: "stale",
        JoinState.UNKNOWN_AVAILABILITY: "unknown_availability",
    }[cell.state]


def _originals(
    row: NativePreprocessingRow, plan: TrainingPreprocessingPlanV1
) -> dict[str, object]:
    return {
        name: {
            "value": _evidence_value(row, name),
            "missing": _evidence_value(row, name) is None,
            "native_state": (
                None
                if (cell := _native_cell(row, name)).state is None
                else cell.state.value
            ),
            "missingness": _missingness(cell),
            "reason": cell.reason,
            "age_ns": cell.age_ns,
            "decision_time_ns": cell.decision_time_ns,
            "provenance": (
                None if cell.provenance is None else cell.provenance.to_dict()
            ),
        }
        for name in plan.columns
    }


def evidence_records(
    view: TrainingEvidenceViewV1,
) -> tuple[dict[str, object], ...]:
    """Nullable originals, reasons and typed states; never a model-ready claim."""
    if (
        type(view) is not TrainingEvidenceViewV1
        or view.nonclaims != PREPROCESSING_NONCLAIMS
    ):
        raise TypeError("exact native evidence view required")
    fresh = materialize_training_evidence_view(view.plan)
    if fresh.native.content_id != view.native.content_id:
        raise ValueError("native evidence changed after projection")
    return tuple(
        {
            "row_id": row.wide_row_id,
            "partition": None if row.partition is None else row.partition.value,
            "evidence_unit_id": row.evidence_unit_id,
            "member_key": row.member_key,
            "weight": _ratio(row.weight),
            "weight_policy": fresh.native.mass_policy,
            "values": {
                name: _evidence_value(row, name) for name in fresh.plan.columns
            },
            "originals": _originals(row, fresh.plan),
            "schema": _schema(fresh.plan),
            "kind": "evidence",
        }
        for row in fresh.native.rows
    )


def _validate_application(
    fit: TrainingPreprocessingFitV1,
    plan: TrainingPreprocessingPlanV1,
    native: NativePreprocessingEvidence,
    cutoff_ns: int | None,
) -> None:
    mode = fit.plan.fit_mode
    if mode is not PreprocessingFitMode.FIXED_EXTERNAL:
        if plan.artifact_id != fit.plan.artifact_id:
            raise ValueError(
                "fit cannot cross fold, split or native source plans"
            )
        if mode is PreprocessingFitMode.ROLLING_PTI:
            if cutoff_ns != fit.cutoff_ns:
                raise ValueError(
                    "rolling application requires the exact fitted cutoff"
                )
        elif cutoff_ns is not None:
            raise ValueError(
                "train fit application cannot claim point-in-time cutoff"
            )
        return
    if (
        plan.fit_mode is not mode
        or plan.fold_id != fit.plan.fold_id
        or plan.steps != fit.plan.steps
        or _schema(plan) != _schema(fit.plan)
        or cutoff_ns is None
        or fit.cutoff_ns is None
        or cutoff_ns < fit.cutoff_ns
    ):
        raise ValueError(
            "external fit requires matching schema/fold and causal cutoff"
        )
    calibration = inspect_training_preprocessing_native(
        fit.plan.wide_plan, fit.plan.split
    )
    if set(calibration.source_anchor_ids) & set(native.source_anchor_ids):
        raise ValueError(
            "external calibration overlaps native application anchors"
        )
    # Applying does not select or learn protected data. This call verifies the
    # normalized added-feature family/clock policy using existing TRAIN units.
    select_training_preprocessing_fit(
        native,
        fit_mode=mode.value,
        fit_unit_ids=plan.fit_unit_ids,
        fold_id=plan.fold_id,
        cutoff_ns=cutoff_ns,
        columns=tuple(sorted({_base_name(name) for name in plan.columns})),
    )


def _application(
    plan: TrainingPreprocessingPlanV1,
    fit: TrainingPreprocessingFitV1 | None,
    require_dense: bool,
    cutoff_ns: int | None,
) -> TrainingPreprocessingViewV1:
    if type(require_dense) is not bool or (
        cutoff_ns is not None and (type(cutoff_ns) is not int or cutoff_ns < 0)
    ):
        raise TypeError(
            "application requires exact boolean and nonnegative cutoff"
        )
    native = inspect_training_preprocessing_native(plan.wide_plan, plan.split)
    if fit is not None:
        _validate_application(fit, plan, native, cutoff_ns)
    elif (
        plan.fit_mode is not PreprocessingFitMode.NONE or cutoff_ns is not None
    ):
        raise ValueError("unfitted application requires explicit none mode")
    imputation: dict[str, object] = {}
    fit_member_rows = None
    if fit is not None and plan.fit_mode in (
        PreprocessingFitMode.TRAIN_FIT,
        PreprocessingFitMode.ROLLING_PTI,
    ):
        membership_rows = cast(
            list[dict[str, object]], training_load(fit.membership_json)["rows"]
        )
        fit_member_rows = frozenset(
            cast(str, member["row_id"]) for member in membership_rows
        )
    # Audit all retained inputs above, but never execute numerical transforms
    # on future rows when producing an earlier point-in-time application.
    # Even a finite, valid future value can overflow a frozen earlier scaler.
    application_rows = tuple(
        row
        for row in native.rows
        if cutoff_ns is None
        or all(
            cell.decision_time_ns is not None
            and cell.decision_time_ns <= cutoff_ns
            for cell in row.cells
        )
    )
    table, schema, stages = _pipeline(
        plan,
        application_rows,
        tuple(row.weight for row in application_rows),
        None if fit is None else fit.steps,
        imputation,
        fit_member_rows,
    )
    # Pure pipeline schema hashes support fit/apply equality. Consumer feature
    # lineage additionally binds the entire admitted experiment and its native
    # fit membership, even when another fold happens to have equal numbers.
    schema = {
        name: {
            **item,
            "pipeline_lineage_id": item["lineage_id"],
            "lineage_id": _identity(
                "preprocessing-view-lineage",
                {
                    "pipeline_lineage_id": item["lineage_id"],
                    "plan_id": plan.artifact_id,
                    "fit_id": None if fit is None else fit.artifact_id,
                    "fit_mode": plan.fit_mode.value,
                    "cutoff_ns": cutoff_ns,
                },
            ),
        }
        for name, item in schema.items()
    }
    # Keep native units, meaning, scalar types and state ontologies directly
    # interpretable after projection replaces the output descriptors. Output
    # native_dependencies reference this immutable, admitted input schema.
    input_schema = _schema(plan)
    records = []
    for row, values in zip(application_rows, table):
        clocks = {cell.decision_time_ns for cell in row.cells}
        if cutoff_ns is not None and clocks != {cutoff_ns}:
            continue
        if cutoff_ns is not None:
            for name in plan.columns:
                cell = _native_cell(row, name)
                proof = cell.provenance
                if cell.original_value is not None and (
                    proof is None
                    or proof.available_at_ns is None
                    or proof.available_at_ns > cutoff_ns
                    or proof.source_time_ns is None
                    or proof.source_time_ns > cutoff_ns
                    or proof.dependency_end_ns is None
                    or proof.dependency_end_ns > cutoff_ns + 1
                ):
                    raise ValueError(
                        "application refuses unknown or future cell dependencies"
                    )
        if not values or (
            require_dense
            and any(
                value is None or type(value) is str for value in values.values()
            )
        ):
            raise ValueError(
                "dense model view has unresolved missing or unencoded values"
            )
        originals = _originals(row, plan)
        record = {
            "row_id": row.wide_row_id,
            "evidence_unit_id": row.evidence_unit_id,
            "partition": None if row.partition is None else row.partition.value,
            "member_key": row.member_key,
            "weight": _ratio(row.weight),
            "weight_policy": native.mass_policy,
            "values": {name: _export(value) for name, value in values.items()},
            "exact_values": {
                name: str(value) if isinstance(value, Fraction) else None
                for name, value in values.items()
            },
            "originals": originals,
            "masks": {
                name: cast(dict[str, object], item)["missing"]
                for name, item in originals.items()
            },
            "imputation_evidence": {
                name: imputation[row.wide_row_id + "/" + name]
                for name in values
                if row.wide_row_id + "/" + name in imputation
            },
            "schema": schema,
            "input_schema": input_schema,
            "fit_id": None if fit is None else fit.artifact_id,
            "parameter_identity": (
                None if fit is None else fit.parameter_identity
            ),
            "step_ids": [stage.step_id for stage in stages],
            "fit_support": (
                None if fit is None else training_load(fit.support_json)
            ),
            "output_support": {
                name: {
                    "is_observed_or_supported": all(
                        not cast(dict[str, object], originals[parent])[
                            "missing"
                        ]
                        for parent in cast(
                            list[str], schema[name]["native_dependencies"]
                        )
                    ),
                    "has_missing_native_input": any(
                        cast(dict[str, object], originals[parent])["missing"]
                        for parent in cast(
                            list[str], schema[name]["native_dependencies"]
                        )
                    ),
                    "policy_id": plan.artifact_id,
                    "methods": [
                        event["family"]
                        for event in cast(
                            list[dict[str, object]], schema[name]["transforms"]
                        )
                    ],
                    "confidence": "not_calibrated",
                }
                for name in values
            },
            "output_status": {
                name: (
                    "unresolved"
                    if value is None
                    else "derived" if plan.steps else "native"
                )
                for name, value in values.items()
            },
            "kind": "dense_model" if require_dense else "transformed_nullable",
        }
        records.append(record)
    if not records:
        raise ValueError("application has no rows at requested cutoff")
    payload = training_json({"records": records})
    # The standard decoder enforces the canonical eight-MiB wire budget.
    training_load(payload)
    assert_training_preprocessing_native_current(native)
    return TrainingPreprocessingViewV1(
        plan, fit, payload, require_dense, cutoff_ns
    )


def apply_training_preprocessing(
    fit: TrainingPreprocessingFitV1,
    *,
    expected_fit_id: str,
    plan: TrainingPreprocessingPlanV1 | None = None,
    require_dense: bool = True,
    cutoff_ns: int | None = None,
) -> TrainingPreprocessingViewV1:
    """Apply frozen, freshly rederived coefficients; never fit application rows."""
    current = replay_training_preprocessing_fit(
        fit, expected_fit_id=expected_fit_id
    )
    return _application(
        _fresh_plan(current.plan if plan is None else plan),
        current,
        require_dense,
        cutoff_ns,
    )


def materialize_training_preprocessing_view(
    plan: TrainingPreprocessingPlanV1,
    *,
    fit: TrainingPreprocessingFitV1 | None = None,
    expected_fit_id: str | None = None,
    require_dense: bool = True,
    cutoff_ns: int | None = None,
) -> TrainingPreprocessingViewV1:
    if fit is None:
        if expected_fit_id is not None:
            raise ValueError("unfitted view cannot assert a fit identity")
        return _application(_fresh_plan(plan), None, require_dense, cutoff_ns)
    if expected_fit_id is None:
        raise ValueError(
            "application requires independent expected fit identity"
        )
    return apply_training_preprocessing(
        fit,
        expected_fit_id=expected_fit_id,
        plan=plan,
        require_dense=require_dense,
        cutoff_ns=cutoff_ns,
    )


def preprocessing_records(
    view: TrainingPreprocessingViewV1,
) -> tuple[dict[str, object], ...]:
    """Freshly repeat fit and application before exporting a previous result."""
    if (
        type(view) is not TrainingPreprocessingViewV1
        or view.nonclaims != PREPROCESSING_NONCLAIMS
    ):
        raise TypeError("exact preprocessing view required")
    fresh = materialize_training_preprocessing_view(
        view.plan,
        fit=view.fit,
        expected_fit_id=None if view.fit is None else view.fit.artifact_id,
        require_dense=view.require_dense,
        cutoff_ns=view.cutoff_ns,
    )
    if fresh.records_json != view.records_json:
        raise ValueError(
            "preprocessing view differs from complete native replay"
        )
    return fresh.records

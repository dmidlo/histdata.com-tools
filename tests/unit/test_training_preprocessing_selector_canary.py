"""Native selector-state contamination, not certification of hidden intent.

The attack deliberately consults invented VALIDATION labels, then reseals a
plausible selector result under an unchanged TRAIN-only declared policy. Every
constructor succeeds; only authoritative native refitting supplies the refusal.
"""

import hashlib
import json
from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_preprocessing import (
    TrainingPreprocessingFitV1,
    fit_training_preprocessing,
    replay_training_preprocessing_fit,
)
from histdatacom.data_quality.training_preprocessing_math import (
    ReferenceTransformFitV1,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
)
from tests.fixtures.training_preprocessing_v1 import (
    ASK,
    BID,
    build_preprocessing_fixture,
    preprocessing_plan,
)


def _digest(value):
    return hashlib.sha256(training_json(value).encode("ascii")).hexdigest()


def _identity(kind, value):
    return kind + ":sha256:" + _digest(value)


def test_label_tuned_selector_state_fails_fully_resealed_native_replay(
    tmp_path,
):
    native = build_preprocessing_fixture(tmp_path / "selector-contamination")
    plan = preprocessing_plan(
        native, family="variance_selector", columns=(BID, ASK)
    )
    fitted = fit_training_preprocessing(plan)
    original_step = fitted.steps[0]
    kernel = ReferenceTransformFitV1.from_json(original_step.kernel_fit_json)
    assert set(kernel.output_columns) == {BID, ASK}
    assert json.loads(kernel.options_json) == {"threshold": "0"}

    validation_units = {
        assignment.evidence_unit_id
        for assignment in native["split"].assignments
        if assignment.partition is TemporalPartition.VALIDATION
    }
    protected = tuple(
        record
        for record in native["records"]
        if record["spine"]["evidence_unit_id"] in validation_units
    )
    # Invented protected targets deliberately favor ASK. These never enter
    # the supported fitting API; they contaminate only this malicious result.
    labels = (100.5, 111.0, 121.5, 130.5, 200.5, 211.0, 221.5, 230.5)
    assert len(protected) == len(labels)
    losses = {
        column: sum(
            (
                (Fraction(record["values"][column]) - Fraction(label)) ** 2
                for record, label in zip(protected, labels)
            ),
            Fraction(),
        )
        for column in kernel.columns
    }
    assert losses == {ASK: Fraction(0), BID: Fraction(15, 2)}
    chosen = min(losses, key=lambda column: losses[column])
    assert chosen == ASK
    chosen_index = kernel.columns.index(chosen)

    parameters = json.loads(kernel.parameters_json)
    assert len(parameters["selected"]) == 2
    parameters["selected"] = [chosen_index]
    forged_kernel = replace(
        kernel,
        output_columns=(chosen,),
        parameters_json=training_json(parameters),
    )
    assert (
        ReferenceTransformFitV1.from_json(forged_kernel.to_json())
        == forged_kernel
    )
    descriptor = {
        "lineage_id": _identity(
            "preprocessing-output-lineage",
            {
                "step": original_step.step_id,
                "kernel": forged_kernel.fit_id,
                "parents": list(original_step.parent_lineage_ids),
                "column": chosen,
            },
        ),
        "descriptor": {
            "family": kernel.family,
            "kernel": forged_kernel.fit_id,
            "column": chosen,
        },
    }
    forged_step = replace(
        original_step,
        kernel_fit_json=forged_kernel.to_json(),
        output_schema_sha256=_digest([descriptor]),
    )
    membership = json.loads(fitted.membership_json)
    forged = replace(
        fitted,
        steps=(forged_step,),
        parameter_identity=_identity(
            "preprocessing-parameters",
            {
                "fit_selection_id": membership["fit_selection_id"],
                "steps": [forged_step.to_dict()],
            },
        ),
    )
    assert TrainingPreprocessingFitV1.from_json(forged.to_json()) == forged
    assert forged.plan == fitted.plan
    assert forged.membership_json == fitted.membership_json
    assert forged.support_json == fitted.support_json
    assert forged.artifact_id != fitted.artifact_id
    assert forged.parameter_identity != fitted.parameter_identity
    # Every forged kernel/stage/outer construction is outside this context:
    # malformed fields, stale output hashes or a bad expected ID cannot pass.
    with pytest.raises(
        ValueError, match="preprocessing fit differs from complete native refit"
    ):
        replay_training_preprocessing_fit(
            forged, expected_fit_id=forged.artifact_id
        )

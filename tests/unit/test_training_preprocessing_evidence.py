"""Private scalar-separation unit tests, NOT native Boolean producer proof.

The closed native wide registry currently emits numeric/integer/unavailable
columns, not Boolean columns. These deliberately process-local namespace
fixtures exercise the pure evidence/model scalar helpers only. They are never
submitted as native evidence, fitted membership or public replay authority.
"""

from types import SimpleNamespace

import pytest

from histdatacom.data_quality.training_contracts import (
    training_json,
    training_load,
)
from histdatacom.data_quality.training_join_contracts import JoinState
from histdatacom.data_quality.training_preprocessing_views import (
    _evidence_value,
    _originals,
    _value,
    replay_training_preprocessing_fit,
)

COLUMN = "uncertainty.unit_only.scalar"


def _process_local_scalar(value):
    cell = SimpleNamespace(
        column=COLUMN,
        original_value=value,
        state=JoinState.UNAVAILABLE if value is None else JoinState.AVAILABLE,
        reason="unit_scalar_fixture_not_native_evidence",
        age_ns=None,
        decision_time_ns=10,
        provenance=None,
    )
    return SimpleNamespace(cells=(cell,))


@pytest.mark.parametrize("value", [False, True])
def test_process_local_evidence_scalar_preserves_boolean_type(value):
    row = _process_local_scalar(value)
    evidence = _evidence_value(row, COLUMN)
    assert evidence is value
    assert type(evidence) is bool
    assert training_json({"value": evidence}) == (
        '{"value":true}' if value else '{"value":false}'
    )
    assert training_load(training_json({"value": evidence}))["value"] is value
    with pytest.raises(ValueError, match="boolean fields.*categorical"):
        _value(row, COLUMN)


@pytest.mark.parametrize("value", [False, True])
def test_process_local_original_metadata_does_not_coerce_boolean(value):
    row = _process_local_scalar(value)
    # This columns-only fixture is explicitly not a preprocessing source plan.
    originals = _originals(row, SimpleNamespace(columns=(COLUMN,)))
    assert originals[COLUMN]["value"] is value
    assert type(originals[COLUMN]["value"]) is bool
    assert originals[COLUMN]["missing"] is False
    assert originals[COLUMN]["native_state"] == JoinState.AVAILABLE.value
    assert originals[COLUMN]["provenance"] is None


@pytest.mark.parametrize("value", [False, True])
def test_process_local_state_category_does_not_encode_boolean_as_number(value):
    row = _process_local_scalar(value)
    # A state companion describes native missingness, not the Boolean value.
    assert _evidence_value(row, "state." + COLUMN) == JoinState.AVAILABLE.value
    assert _value(row, "state." + COLUMN) == JoinState.AVAILABLE.value


@pytest.mark.parametrize("value", [None, 0, 1, -2, 1.25, "", "category"])
def test_process_local_other_scalar_types_remain_exact(value):
    row = _process_local_scalar(value)
    evidence = _evidence_value(row, COLUMN)
    model = _value(row, COLUMN)
    assert type(evidence) is type(model) is type(value)
    assert evidence == model == value


def test_process_local_integer_subclass_is_not_evidence_scalar_authority():
    class UnadmittedInteger(int):
        pass

    row = _process_local_scalar(UnadmittedInteger(1))
    with pytest.raises(ValueError, match="unsupported scalar type"):
        _evidence_value(row, COLUMN)
    with pytest.raises(ValueError, match="unsupported scalar type"):
        _value(row, COLUMN)


class _ExpectedSubjectSubclass(str):
    """Even correctly spelled subclass text is not an exact subject string."""


@pytest.mark.parametrize(
    "subject",
    [
        _ExpectedSubjectSubclass(
            "training-preprocessing-fit:sha256:" + "a" * 64
        ),
        None,
        True,
        False,
        1,
        b"training-preprocessing-fit:sha256:" + b"a" * 64,
        "",
        "training-preprocessing-plan:sha256:" + "a" * 64,
        "training-preprocessing-fit:sha256:" + "a" * 63,
        "training-preprocessing-fit:sha256:" + "a" * 65,
        "training-preprocessing-fit:sha256:" + "A" * 64,
        "training-preprocessing-fit:sha256:" + "g" * 64,
        "training-preprocessing-fit:sha256:" + "a" * 64 + "\n",
    ],
)
def test_expected_subject_preflight_precedes_any_fit_admission(subject):
    # Deliberately not a fit object: invalid-subject refusal must happen before
    # native/object admission. This is negative API ordering, not fit proof.
    with pytest.raises(ValueError, match="exact expected fit identity"):
        replay_training_preprocessing_fit(object(), expected_fit_id=subject)

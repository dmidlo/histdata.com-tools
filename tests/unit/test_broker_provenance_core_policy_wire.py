"""Pure policy wire equivalence under host provenance's repeated fresh checks."""

import json
from dataclasses import fields

import pytest

from histdatacom.broker_plugin_policy import _wire
from tests.fixtures.broker_policy_core import core_context


def reference_wire(artifact):
    payload = artifact.to_payload()
    schema = artifact.schema_version()
    return _wire.canonical_json(
        {
            "schema_version": schema,
            "artifact_id": _wire.identity(
                artifact.KIND, {"schema_version": schema, "payload": payload}
            ),
            "payload": payload,
        }
    )


def artifacts(record):
    if isinstance(record, _wire.Artifact):
        yield record
    if isinstance(record, _wire.Record):
        for field in fields(record):
            value = getattr(record, field.name)
            if isinstance(value, _wire.Record):
                yield from artifacts(value)
            elif type(value) is tuple:
                for item in value:
                    yield from artifacts(item)


def test_every_generated_policy_parent_matches_independent_reference_wire():
    for artifact in artifacts(core_context()):
        expected = reference_wire(artifact)
        assert artifact.to_json() == expected
        assert artifact.artifact_id == json.loads(expected)["artifact_id"]
        assert type(artifact).from_json(expected).to_json() == expected


@pytest.mark.parametrize(
    "bound,limits",
    [
        ("MAX_NODES", range(1, 1400, 13)),
        ("MAX_DEPTH", range(1, 12)),
        ("MAX_ITEMS", range(1, 60)),
        ("MAX_BYTES", range(1, 36000, 251)),
        ("MAX_TEXT", range(1, 400, 13)),
    ],
)
def test_policy_full_envelope_limits_match_reference(
    monkeypatch, bound, limits
):
    context = core_context()
    for limit in limits:
        monkeypatch.setattr(_wire, bound, limit)
        try:
            expected = reference_wire(context)
        except ValueError:
            with pytest.raises(ValueError):
                context.to_json()
        else:
            assert context.to_json() == expected


@pytest.mark.parametrize(
    "corrupt", [float("nan"), float("inf"), 2**64, object()]
)
def test_policy_wire_rechecks_corrupted_frozen_nested_values(corrupt):
    context = core_context()
    context.to_json()
    assert context.artifact_id
    object.__setattr__(context.policies[0], "declared_at_ns", corrupt)
    for encode in (context.to_json, lambda: context.artifact_id):
        with pytest.raises(ValueError):
            encode()


def test_policy_exact_decode_rejects_frozen_bool_corruption_after_serialization():
    context = core_context()
    context.to_json()
    object.__setattr__(context.policies[0], "declared_at_ns", False)
    with pytest.raises(ValueError):
        type(context).from_json(context.to_json())


def test_policy_serialization_still_charges_expanded_aliases_and_cycles():
    context = core_context()
    aliased = ["x"]
    for _ in range(18):
        aliased = [aliased, aliased]
    object.__setattr__(context, "policies", aliased)
    with pytest.raises(ValueError):
        context.to_json()
    cyclic = []
    cyclic.append(cyclic)
    object.__setattr__(context, "policies", cyclic)
    with pytest.raises(ValueError):
        context.to_json()

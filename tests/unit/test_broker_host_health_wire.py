"""Pure serializer equivalence; no cached source bytes or authority."""

from dataclasses import fields
from enum import Enum
import hashlib
import json

import pytest

from histdatacom.broker_plugin_health._wire import Artifact
from histdatacom.broker_plugin_health import BrokerHostHealthPolicyV1
from tests.unit.test_broker_host_health_replay import legacy_fixture, replay


def reference(value):
    if isinstance(value, Artifact):
        payload = {
            "schema_version": "histdatacom.broker-host-health."
            + value.KIND
            + ".v1",
            **{
                field.name: reference(getattr(value, field.name))
                for field in fields(value)
            },
        }
        digest = hashlib.sha256(encode(payload).encode("ascii")).hexdigest()
        return {
            **payload,
            "artifact_id": "broker-host-health-"
            + value.KIND
            + ":sha256:"
            + digest,
        }
    if isinstance(value, Enum):
        return value.value
    if type(value) in (tuple, list):
        return [reference(item) for item in value]
    if type(value) is dict:
        return {key: reference(item) for key, item in value.items()}
    return value


def encode(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def test_nested_health_wire_and_identity_match_independent_reference():
    fixture = legacy_fixture()
    for value in (fixture[0].policy, fixture[0], *fixture[1], replay(fixture)):
        expected = reference(value)
        assert value.to_dict() == expected
        assert value.to_json() == encode(expected)
        assert value.artifact_id == expected["artifact_id"]
        assert type(value).from_json(encode(expected)) == value


@pytest.mark.parametrize("value", [float("nan"), 2**63, "x" * 4097, object()])
def test_health_serialization_rechecks_corrupted_frozen_instance(value):
    policy = BrokerHostHealthPolicyV1()
    policy.to_json()
    object.__setattr__(policy, "minimum_events", value)
    with pytest.raises(ValueError):
        policy.to_json()


def test_health_serialization_never_reuses_stale_instance_identity():
    policy = BrokerHostHealthPolicyV1()
    before = policy.to_json()
    object.__setattr__(policy, "minimum_events", policy.minimum_events + 1)
    assert before != policy.to_json()
    assert policy.to_json() == encode(reference(policy))

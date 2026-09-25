"""Closed schemas, exact identities, detached values and resource bounds."""

import json
from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom.broker_plugin_health import (
    BrokerHostHealthObservationKind as Kind,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthObservationV1 as Observation,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthPolicyV1,
    health_rate,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthReason as Reason,
)


def test_observation_canonical_roundtrip_and_immutable_identity():
    event = Observation(
        0,
        Kind.INGRESS,
        0,
        100,
        20,
        ingress_sequence=0,
        event_id="generated-event:sha256:" + "a" * 64,
    )
    assert Observation.from_json(event.to_json()) == event
    assert replace(event, monotonic_ns=21).artifact_id != event.artifact_id
    with pytest.raises(FrozenInstanceError):
        event.sequence = 3


@pytest.mark.parametrize(
    "changes",
    [
        {"sequence": True},
        {"epoch": -1},
        {"utc_ns": 2**63},
        {"monotonic_ns": 1.0},
        {"event_id": "https://user:secret@host"},
        {"native_record_id": "some-record"},
        {"queue_items": 0},
        {"reason": Reason.PERMISSION},
        {"ingress_sequence": None},
    ],
)
def test_ingress_invalid_field_combinations_refuse(changes):
    values = {
        "sequence": 0,
        "kind": Kind.INGRESS,
        "epoch": 0,
        "utc_ns": 100,
        "monotonic_ns": 20,
        "ingress_sequence": 0,
        "event_id": "some-event",
    }
    with pytest.raises(ValueError):
        Observation(**{**values, **changes})


def test_malformed_observation_never_retains_raw_payload():
    item = Observation(
        0, Kind.INGRESS, 0, 100, 20, ingress_sequence=0, reason=Reason.MALFORMED
    )
    assert item.event_id is None
    with pytest.raises(ValueError):
        replace(item, event_id="invented-payload-hash")
    with pytest.raises(TypeError):
        Observation(0, Kind.INGRESS, 0, 100, 20, raw_payload="private")


@pytest.mark.parametrize(
    "mutator",
    [
        lambda d: d.update(extra=None),
        lambda d: d.update(utc_ns=d["utc_ns"] + 1),
        lambda d: d.update(artifact_id="wrong"),
        lambda d: d.update(schema_version="future"),
    ],
)
def test_wire_unknown_or_self_inconsistent_evidence_refuses(mutator):
    original = Observation(0, Kind.QUEUE, 0, 100, 20, queue_items=0)
    payload = original.to_dict()
    mutator(payload)
    with pytest.raises(ValueError):
        Observation.from_json(json.dumps(payload))


def test_duplicate_json_keys_and_nan_refuse():
    item = Observation(0, Kind.QUEUE, 0, 100, 20, queue_items=0)
    duplicate = item.to_json().replace(
        '"sequence":0', '"sequence":0,"sequence":0'
    )
    with pytest.raises(ValueError):
        Observation.from_json(duplicate)
    with pytest.raises(ValueError):
        BrokerHostHealthPolicyV1(max_known_drop_rate=float("nan"))


@pytest.mark.parametrize(
    "changes",
    [
        {"max_buckets": 4097},
        {"bucket_width_ns": 0},
        {"max_observations": 1000001},
        {"max_samples_per_bucket": 100001},
        {"minimum_events": 0},
        {"max_known_drop_rate": 1},
        {"max_stale_rate": 1.1},
        {"version": "01.0.0"},
        {"require_known_upstream_loss": 1},
    ],
)
def test_policy_bounds_and_types_are_not_coerced(changes):
    with pytest.raises(ValueError):
        BrokerHostHealthPolicyV1(**changes)


def test_rate_artifact_recomputes_interval_not_just_digest():
    rate = health_rate(25, 10000)
    with pytest.raises(ValueError):
        replace(rate, wilson_upper=0.003)
    with pytest.raises(ValueError):
        replace(rate, z=2.0)


@pytest.mark.parametrize(
    "field",
    [
        "max_reported_source_gap_events",
        "max_reconnect_events",
        "max_clock_correction_events",
    ],
)
@pytest.mark.parametrize("value", [-1, True, 1_000_001])
def test_source_condition_ceilings_are_exact_bounded_counts(field, value):
    with pytest.raises(ValueError):
        BrokerHostHealthPolicyV1(**{field: value})

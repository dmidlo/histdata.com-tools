"""Synthetic wire parity and per-call traversal, never authority caching."""

from collections import Counter
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from histdatacom.broker_plugin_capabilities import contracts as capability
from histdatacom.broker_plugin_lifecycle import contracts as lifecycle
from histdatacom.broker_plugin_registry import contracts as registry
from histdatacom.broker_plugins import contracts as sdk

FAMILIES = ("sdk", "registry", "capability", "lifecycle")
GOLDEN = json.loads(
    (
        Path(__file__).parents[1]
        / "fixtures/broker_artifact_serialization_v1.json"
    ).read_text()
)["artifacts"]


def _artifacts():
    metadata = sdk.BrokerPluginMetadataV1(
        "org.example.fixture",
        "1.0.0",
        "Fixture",
        extensions=(
            sdk.BrokerExtensionV1("org.example.fixture", '{"answer":42}'),
        ),
    )
    registration = registry.BrokerPluginRegistrationV1(
        "org.example.fixture",
        "1.0.0",
        "Fixture",
        "fixture",
        "1.0.0",
        "fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("fixture",),
        ("quotes.bid-ask.v1",),
    )
    candidate = registry.BrokerPluginCandidateV1(
        registration, "1" * 64, "2" * 64
    )
    inventory = registry.BrokerPluginInventoryV1((candidate,))
    plan_id = "broker-capability-plan:sha256:" + "3" * 64
    fields = tuple(
        capability.BrokerFieldReceiptV1(
            name,
            "metadata." + name + ".v1",
            capability.BrokerFieldSupport.NOT_REPORTED,
        )
        for name in ("account_class", "feed_class", "server_label")
    )
    admitted = capability.BrokerAdmittedMetadataV1(plan_id, metadata, fields)
    binding = capability.BrokerInvocationBindingV1(
        plan_id,
        candidate.artifact_id,
        metadata.artifact_id,
        capability.BrokerInvocationAssociation.INSTALLED_ENTRYPOINT,
        "4" * 64,
    )
    identity = lifecycle.BrokerLifecycleIdentityV1(
        binding, admitted, sdk.BrokerConfigurationSchemaV1(())
    )
    return dict(zip(FAMILIES, (metadata, inventory, admitted, identity)))


def _leaf(artifact, family):
    if family == "registry":
        return artifact.candidates[0].registration
    if family == "capability":
        return artifact.metadata
    if family == "lifecycle":
        return artifact.metadata.metadata
    return artifact


@pytest.mark.parametrize("family", FAMILIES)
def test_historical_nested_canonical_bytes_and_ids_are_unchanged(family):
    artifact = _artifacts()[family]
    historical = GOLDEN[family]
    wire = artifact.to_json()
    assert wire == historical["canonical_json"]
    assert (
        hashlib.sha256(wire.encode()).hexdigest()
        == historical["canonical_sha256"]
    )
    assert artifact.artifact_id == historical["artifact_id"]
    assert type(artifact).from_json(wire) == artifact


@pytest.mark.parametrize("family", FAMILIES)
def test_each_serialization_traverses_each_nested_payload_once(family):
    artifact = _artifacts()[family]
    calls = Counter()
    targets = [
        (sdk._Artifact, "identity_payload"),
        (capability._Artifact, "_payload"),
        (lifecycle._Artifact, "_payload"),
        *(
            (cls, "_payload")
            for cls in (
                registry.BrokerPluginRegistrationV1,
                registry.BrokerPluginCandidateV1,
                registry.BrokerPluginInventoryV1,
            )
        ),
    ]

    def counted(original):
        def payload(self):
            calls[type(self).__name__] += 1
            return original(self)

        return payload

    expected = {name: 1 for name in GOLDEN[family]["payload_calls"]}
    if "BrokerFieldReceiptV1" in expected:
        expected["BrokerFieldReceiptV1"] = 3  # Three distinct field receipts.
    with ExitStack() as stack:
        for cls, name in targets:
            stack.enter_context(
                patch.object(cls, name, counted(getattr(cls, name)))
            )
        for method in (artifact.to_dict, artifact.to_json, artifact.to_dict):
            calls.clear()
            method()
            assert calls == expected


@pytest.mark.parametrize("family", FAMILIES)
def test_later_serialization_observes_forged_nested_changes_without_cache(
    family,
):
    artifact = _artifacts()[family]
    before = artifact.to_json()
    old_id = artifact.artifact_id
    object.__setattr__(
        _leaf(artifact, family), "display_name", "Changed fixture"
    )
    current = artifact.to_dict()
    assert current["artifact_id"] == artifact.artifact_id != old_id
    assert artifact.to_json() != before
    assert type(artifact).from_json(before).to_json() == before
    current["artifact_id"] = "detached caller mutation"
    assert artifact.to_dict()["artifact_id"] == artifact.artifact_id


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize(
    "bad",
    (object(), float("nan"), "x" * 65_537),
    ids=("type", "nonfinite", "oversize"),
)
def test_later_serialization_still_refuses_malformed_and_oversized_fields(
    family, bad
):
    artifact = _artifacts()[family]
    artifact.to_dict()
    object.__setattr__(_leaf(artifact, family), "display_name", bad)
    with pytest.raises(ValueError):
        artifact.to_dict()
    with pytest.raises(ValueError):
        artifact.to_json()


@pytest.mark.parametrize("family", FAMILIES)
def test_historical_reader_still_refuses_forged_identity_and_unknown_fields(
    family,
):
    artifact = _artifacts()[family]
    original = artifact.to_dict()
    for mutation in (
        {**original, "artifact_id": "forged"},
        {**original, "unknown": True},
    ):
        with pytest.raises(ValueError):
            type(artifact).from_dict(mutation)


def test_sdk_subclass_identity_override_is_not_an_admitted_extension_point():
    from histdatacom.broker_plugin_capabilities import (
        negotiate_broker_capabilities,
        validate_broker_metadata,
    )

    class UnsupportedMetadata(sdk.BrokerPluginMetadataV1):
        @property
        def artifact_id(self):
            return "not-a-native-content-identity"

    inventory = _artifacts()["registry"]
    plan = negotiate_broker_capabilities(
        inventory,
        capability.BrokerCapabilityWorkflowV1(("metadata",)),
        plugin_id="org.example.fixture",
    )
    metadata = UnsupportedMetadata("org.example.fixture", "1.0.0", "Fixture")
    with pytest.raises(capability.BrokerCapabilityError) as error:
        validate_broker_metadata(plan, metadata)
    assert (
        error.value.reason
        is capability.BrokerCapabilityReason.CAPABILITY_VIOLATION
    )

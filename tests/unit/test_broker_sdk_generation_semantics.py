"""Independent SDK evolution witnesses; not the all-family #635 corpus.

The V1 golden below was captured from clean dev aa410c7 before #622 changes.
Both generations keep the metadata wire schema, but their declared SDK contract
is scientific provenance: a generation change is not a lossless JSON upgrade.
"""

import hashlib
import json
from dataclasses import replace

import pytest

from histdatacom.broker_plugins import BrokerPluginMetadataV1
from histdatacom.schema_semantics import (
    project_semantics,
    validate_lossless_evidence,
    verify_semantic_projection,
    verify_semantic_proof,
)
from tests.fixtures.schema_semantics_graph import graph_fixture

RETAINED_SDK_10_JSON = (
    '{"artifact_id":"broker-plugin-metadata:sha256:'
    '59d85cdfde93f66618da1166e008701a4f2c0c1687c7f33b876f4a6614fdc1a7",'
    '"display_name":"Generated retained SDK baseline","extensions":[],'
    '"plugin_id":"org.example.retained-sdk","plugin_version":"1.0.0",'
    '"schema_version":"histdatacom.broker-plugin.metadata.v1",'
    '"sdk_version":"1.0.0"}'
)
RETAINED_SDK_10_SHA256 = (
    "caa2fb597b667aacc12279ddf48d10a9c33930ce2cefca9cd9273678989d9511"
)
PROFILE = "broker-metadata-v1"


def test_sdk_10_retained_canonical_bytes_and_default_are_unchanged():
    assert hashlib.sha256(RETAINED_SDK_10_JSON.encode()).hexdigest() == (
        RETAINED_SDK_10_SHA256
    )
    retained = BrokerPluginMetadataV1.from_json(RETAINED_SDK_10_JSON)
    default = BrokerPluginMetadataV1(
        "org.example.retained-sdk", "1.0.0", "Generated retained SDK baseline"
    )
    assert retained.to_json() == default.to_json() == RETAINED_SDK_10_JSON
    assert default.sdk_version == "1.0.0"


@pytest.mark.parametrize("sdk_version", ("1.0.0", "1.1.0"))
def test_each_supported_generation_has_an_exact_independent_projection(
    sdk_version,
):
    value = replace(
        BrokerPluginMetadataV1.from_json(RETAINED_SDK_10_JSON),
        sdk_version=sdk_version,
    )
    canonical = value.to_json()
    formatted = json.dumps(value.to_dict(), indent=2)
    first = project_semantics(PROFILE, canonical)
    second = project_semantics(PROFILE, formatted)
    assert first.input_sha256 != second.input_sha256
    assert first.semantic_json == second.semantic_json
    assert first.semantic_sha256 == second.semantic_sha256
    assert verify_semantic_projection(first, canonical) == first
    registry, proofs = graph_fixture(formatted, PROFILE)
    assert len(validate_lossless_evidence(registry, proofs).proof_ids) == 3


def test_sdk_and_plugin_version_axes_are_not_erased_from_meaning():
    old = BrokerPluginMetadataV1.from_json(RETAINED_SDK_10_JSON)
    sdk_upgrade = replace(old, sdk_version="1.1.0")
    plugin_upgrade = replace(old, plugin_version="1.1.0")
    values = (old, sdk_upgrade, plugin_upgrade)
    assert len({value.artifact_id for value in values}) == 3
    assert (
        len(
            {
                project_semantics(PROFILE, value.to_json()).semantic_sha256
                for value in values
            }
        )
        == 3
    )


def test_valid_successor_metadata_cannot_replace_lossless_encoding_output():
    old = BrokerPluginMetadataV1.from_json(RETAINED_SDK_10_JSON)
    successor = replace(old, sdk_version="1.1.0")
    registry, (proof, _, _) = graph_fixture(old.to_json(), PROFILE)
    destination = successor.to_json()
    destination_projection = project_semantics(PROFILE, destination)
    # Both objects are valid. Neither native-reader acceptance nor a caller's
    # self-consistent successor projection proves the admitted executor emitted
    # it or that the contract-version change preserves semantic identity.
    with pytest.raises(ValueError, match="migration changes exact semantic"):
        forged = replace(
            proof,
            destination_json=destination,
            destination_projection=destination_projection,
        )
        verify_semantic_proof(registry, forged)


@pytest.mark.parametrize("mutation", ("missing", "unknown", "sdk_type"))
def test_generation_metadata_requires_the_entire_exact_writer_envelope(
    mutation,
):
    value = json.loads(RETAINED_SDK_10_JSON)
    if mutation == "missing":
        del value["sdk_version"]
    elif mutation == "unknown":
        value["compatibility_claim"] = "installed-so-must-be-compatible"
    else:
        value["sdk_version"] = 1
    with pytest.raises(ValueError):
        project_semantics(PROFILE, json.dumps(value))

"""Synthetic compatibility governance; no package install or provider claims."""

from dataclasses import replace
import json

import pytest

from histdatacom.broker_plugin_compatibility import (
    BrokerPluginDirectoryEntryV1,
    BrokerSDKCompatibilityDecisionV1,
    BrokerSDKCompatibilityPolicyV1,
    BrokerSDKSupportV1,
    BrokerSDKUse,
    broker_sdk_compatibility_policy,
    negotiate_broker_sdk,
)
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginRegistrationV1,
)
from histdatacom.broker_plugins import (
    BROKER_PROVIDER_PROTOCOL_NAMESPACE,
    BrokerExtensionV1,
    BrokerPluginMetadataV1,
    BrokerProviderProtocolV1,
    declared_provider_protocol,
)

NOTICE = 180 * 86_400 * 1_000_000_000


def candidate(minimum="1.0.0", maximum="2.0.0", package="1.0.0"):
    registration = BrokerPluginRegistrationV1(
        "org.example.compatibility",
        package,
        "Generated compatibility fixture",
        "generated-broker",
        package,
        "generated_broker.plugin:factory",
        minimum,
        maximum,
        ("generated-provider",),
        ("events.v1", "session.v1"),
    )
    return BrokerPluginCandidateV1(registration, "a" * 64, "b" * 64)


def decide(subject=None, **changes):
    arguments = dict(
        host_version="3.2.0",
        use=BrokerSDKUse.LIVE,
        evaluated_at_utc_ns=NOTICE + 1,
    )
    arguments.update(changes)
    return negotiate_broker_sdk(subject or candidate(), **arguments)


@pytest.mark.parametrize(
    "minimum,maximum,expected",
    [
        ("1.0.0", "2.0.0", "1.1.0"),
        ("1.0.0", "1.1.0", "1.0.0"),
        ("1.1.0", "1.2.0", "1.1.0"),
        ("1.0.1", "1.1.0", None),
        ("2.0.0", "3.0.0", None),
        ("1.1.0-alpha.1", "1.1.0+build.1", None),
    ],
)
def test_actual_half_open_sdk_intersection(minimum, maximum, expected):
    decision = decide(candidate(minimum, maximum))
    assert decision.selected_sdk_version == expected
    assert (
        BrokerSDKCompatibilityDecisionV1.from_json(decision.to_json())
        == decision
    )
    if expected is None:
        with pytest.raises(ValueError, match="intersection"):
            decision.require_admitted()
    else:
        decision.require_admitted()


def test_requested_generation_cannot_be_silently_substituted():
    decision = decide(
        candidate("1.1.0", "1.2.0"), requested_sdk_version="1.0.0"
    )
    assert decision.compatible_versions == ("1.1.0",)
    assert decision.selected_sdk_version is None
    with pytest.raises(ValueError):
        replace(decision, selected_sdk_version="1.1.0")


def deprecated_policy():
    return BrokerSDKCompatibilityPolicyV1(
        (
            BrokerSDKSupportV1("1.0.0", "3.0.0", 1, "3.2.0", NOTICE + 1),
            BrokerSDKSupportV1("1.1.0"),
        )
    )


def test_live_deprecation_and_retained_replay_have_distinct_lifetimes():
    policy = deprecated_policy()
    subject = candidate("1.0.0", "1.1.0")
    assert decide(subject, policy=policy).selected_sdk_version is None
    before = decide(subject, policy=policy, evaluated_at_utc_ns=NOTICE)
    assert before.selected_sdk_version == "1.0.0"
    assert before.warnings == ("sdk_deprecated:1.0.0",)
    older_host = decide(subject, policy=policy, host_version="3.1.9")
    assert older_host.selected_sdk_version == "1.0.0"
    replay = decide(subject, policy=policy, use=BrokerSDKUse.REPLAY)
    assert replay.selected_sdk_version == "1.0.0"
    assert replay.warnings == (
        "sdk_deprecated:1.0.0",
        "sdk_retained_replay_only:1.0.0",
    )
    assert (
        BrokerSDKCompatibilityDecisionV1.from_json(replay.to_json()) == replay
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"live_removed_since_host": "3.1.0"},
        {"live_removal_not_before_utc_ns": NOTICE},
        {"announced_at_utc_ns": None},
        {"deprecated_since_host": None},
        {"sdk_version": "2.0.0"},
    ],
)
def test_short_or_incomplete_deprecation_notices_are_refused(changes):
    with pytest.raises(ValueError):
        replace(deprecated_policy().versions[0], **changes)


def test_default_support_does_not_announce_fictitious_removals():
    policy = broker_sdk_compatibility_policy()
    assert all(item.deprecated_since_host is None for item in policy.versions)
    assert decide(requested_sdk_version="1.0.0").warnings == ()
    with pytest.raises(ValueError):
        replace(policy, replay_removal_policy="remove_without_corpus_review")


@pytest.mark.parametrize("generation", ("1.0.0", "1.1.0"))
def test_policy_cannot_remove_retained_generation_by_omission(generation):
    with pytest.raises(ValueError, match="retain both SDK generations"):
        BrokerSDKCompatibilityPolicyV1((BrokerSDKSupportV1(generation),))


def test_plugin_upgrade_changes_exact_compatibility_manifest_identity():
    original = decide(candidate(package="1.0.0"))
    upgrade = decide(candidate(package="1.0.1"))
    assert original.selected_sdk_version == upgrade.selected_sdk_version
    assert original.artifact_id != upgrade.artifact_id
    assert original.candidate_json != upgrade.candidate_json


def test_wire_rejects_unknown_keys_and_tampered_decisions():
    decision = decide()
    value = json.loads(decision.to_json())
    value["payload"]["warnings"] = ["fictitious_qualification"]
    with pytest.raises(ValueError):
        BrokerSDKCompatibilityDecisionV1.from_dict(value)
    value = json.loads(decision.to_json())
    value["trust"] = True
    with pytest.raises(ValueError):
        BrokerSDKCompatibilityDecisionV1.from_dict(value)


@pytest.mark.parametrize(
    "status", ["community", "first-party", "deprecated", "unverified"]
)
def test_directory_status_never_conveys_trust(status):
    entry = BrokerPluginDirectoryEntryV1(
        candidate().to_json(),
        "Generated maintainer",
        "https://example.invalid/package",
        "https://example.invalid/source",
        status,
    )
    assert entry.latest_conformance is None
    assert "no_endorsement" in entry.nonclaim
    assert BrokerPluginDirectoryEntryV1.from_json(entry.to_json()) == entry
    with pytest.raises(ValueError):
        replace(entry, nonclaim="scientifically_qualified")
    with pytest.raises(ValueError):
        replace(
            entry, package_url="https://user:password@example.invalid/package"
        )


def test_sdk11_optional_info_has_typed_wire_and_sdk10_refusal():
    info = BrokerProviderProtocolV1(
        "generated-provider", "synthetic-protocol", "revision-A"
    )
    extension = BrokerExtensionV1(
        BROKER_PROVIDER_PROTOCOL_NAMESPACE, info.to_json()
    )
    metadata = BrokerPluginMetadataV1(
        "org.example.compatibility", "1.0.0", "Fixture", "1.1.0", (extension,)
    )
    assert declared_provider_protocol(metadata) == info
    assert BrokerProviderProtocolV1.from_json(info.to_json()) == info
    assert (
        declared_provider_protocol(replace(metadata, sdk_version="1.0.0"))
        is None
    )
    assert declared_provider_protocol(replace(metadata, extensions=())) is None
    assert (
        replace(info, protocol_version="revision-B").artifact_id
        != info.artifact_id
    )


def test_sdk10_reserved_namespace_stays_opaque_on_historical_validation():
    from histdatacom.broker_plugin_capabilities import validate_broker_metadata
    from tests.unit.test_broker_plugin_capabilities import _plan, EXTERNAL

    plan = _plan()
    metadata = replace(
        EXTERNAL["factory"]().metadata,
        extensions=(
            BrokerExtensionV1(
                BROKER_PROVIDER_PROTOCOL_NAMESPACE,
                '{"historical_opaque":true}',
            ),
        ),
    )
    original = metadata.to_json()
    admitted = validate_broker_metadata(plan, metadata)
    assert admitted.metadata.to_json() == original
    assert declared_provider_protocol(admitted.metadata) is None
    with pytest.raises(ValueError):
        declared_provider_protocol(replace(metadata, sdk_version="1.1.0"))

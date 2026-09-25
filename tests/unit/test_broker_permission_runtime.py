"""Generated runtime binding and response-release freshness regressions."""

import base64
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from histdatacom.broker_plugin_permissions import (
    BrokerHostSecretProfileV1,
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionEndpointV1,
    BrokerPermissionError,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    BrokerPermissionResourcesV1,
    BrokerPermissionRevocationV1,
    broker_permission_scope,
)
from histdatacom.broker_plugin_permissions.scope import current_host_resources
from histdatacom.broker_plugin_permissions.worker import (
    PermissionChannel,
    WorkerHostResources,
    WorkerPermissionSource,
)
from histdatacom.broker_plugin_policy.bindings import sdk_invocation_binding
from histdatacom.broker_plugin_policy.contracts import BrokerPolicyRevocationV1
from histdatacom.broker_plugin_policy.scope import provider_policy_scope
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    policy_context,
    sdk_policy_invocation,
)


def _inputs(*, grants=None):
    original = sdk_policy_invocation()
    request = replace(
        original,
        configuration_profile=replace(
            original.configuration_profile,
            provider_id="offline",
        ),
    )
    candidate = request.plan.candidate
    registration = candidate.registration
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        "1.0.0",
        registration.provider_ids,
        ("emit:quotes",),
        ("network:provider:offline", "secrets:read:login"),
        endpoints=(
            BrokerPermissionEndpointV1(
                "endpoint",
                "offline",
                "https://example.invalid",
                "/api/",
                ("POST",),
                64,
                64,
                1000,
                ("login",),
            ),
        ),
        secret_profiles=("login",),
    )
    binding = BrokerPermissionBindingV1(
        candidate.artifact_id,
        manifest.artifact_id,
        "1.0.0",
        "offline",
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        manifest.declared_atoms if grants is None else grants,
        "generated-operator",
        10,
        30,
        "a" * 32,
    )

    class Source:
        context = BrokerPermissionContextV1((grant,))

        def read_context(self):
            return self.context

    source, now = Source(), [15]
    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, source, lambda: now[0]
    )
    rights = MutablePolicySource(
        policy_context(sdk_invocation_binding(request))
    )
    return SimpleNamespace(
        request=request,
        authority=authority,
        source=source,
        now=now,
        grant=grant,
        rights=rights,
    )


def _resources(inputs, provider=None):
    return BrokerPermissionResourcesV1(
        inputs.authority,
        provider_request=inputs.request,
        secret_profiles=(
            (BrokerHostSecretProfileV1("login", "opaque-generated-handle"),)
            if provider is not None
            else ()
        ),
        secret_provider=provider,
    )


def test_narrow_grant_cannot_attach_another_resource_authority():
    narrow = _inputs(grants=("emit:quotes",))
    broader = _inputs()
    resources = _resources(broader)
    with (
        pytest.raises(
            BrokerPermissionError, match="exact_bound_host_resources"
        ),
        broker_permission_scope(narrow.authority, resources=resources),
    ):
        pytest.fail("mismatched host resources were attached")


@pytest.mark.parametrize("kind", ["custom", "subclass"])
def test_arbitrary_protocol_and_resource_subclass_are_not_authority(kind):
    inputs = _inputs()

    class Subclass(BrokerPermissionResourcesV1):
        pass

    resources = (
        object()
        if kind == "custom"
        else Subclass(
            inputs.authority,
            provider_request=inputs.request,
        )
    )
    with (
        pytest.raises(
            BrokerPermissionError, match="exact_bound_host_resources"
        ),
        broker_permission_scope(inputs.authority, resources=resources),
    ):
        pytest.fail("unverified resource implementation was attached")


def test_resource_lookup_rechecks_authority_association():
    inputs, other = _inputs(), _inputs()
    resources = _resources(inputs)
    with broker_permission_scope(inputs.authority, resources=resources):
        assert current_host_resources() is resources
        resources._authority = other.authority
        with pytest.raises(
            BrokerPermissionError, match="exact_bound_host_resources"
        ):
            current_host_resources()


@pytest.mark.parametrize(
    "association", ["same", "other_channel", "host_source"]
)
def test_worker_proxy_requires_the_same_exact_fresh_ledger_channel(
    monkeypatch,
    association,
):
    inputs = _inputs()
    channel = PermissionChannel(
        object(),
        0,
        inputs.request.artifact_id,
        0,
        65536,
        1,
    )

    def exchange(self, operation, payload):
        assert operation == "context" and payload == {}
        return inputs.source.context.to_json()

    monkeypatch.setattr(PermissionChannel, "exchange", exchange)
    source = (
        inputs.source
        if association == "host_source"
        else WorkerPermissionSource(channel)
    )
    authority = BrokerPermissionAuthorityV1(
        inputs.authority.manifest,
        inputs.authority.binding,
        inputs.grant.artifact_id,
        source,
        lambda: inputs.now[0],
    )
    resource_channel = (
        PermissionChannel(object(), 0, inputs.request.artifact_id, 0, 65536, 1)
        if association == "other_channel"
        else channel
    )
    resources = WorkerHostResources(resource_channel)
    if association == "same":
        with broker_permission_scope(authority, resources=resources):
            assert current_host_resources() is resources
    else:
        with (
            pytest.raises(
                BrokerPermissionError, match="exact_bound_host_resources"
            ),
            broker_permission_scope(authority, resources=resources),
        ):
            pytest.fail("unbound worker proxy was attached")


@pytest.mark.parametrize(
    "change", ["none", "revoked", "expired", "provider_revoked"]
)
def test_actual_response_release_rechecks_grant_and_provider_policy(
    monkeypatch,
    change,
):
    inputs = _inputs()
    calls = []

    class Provider:
        def resolve(self, handle):
            assert handle == "opaque-generated-handle"
            calls.append("resolve")
            return "generated-private-token"

    resources = _resources(inputs, Provider())

    def transport(command, **kwargs):
        calls.append("transport")
        assert "generated-private-token" not in repr(command)
        assert "generated-private-token" not in repr(kwargs.get("env"))
        assert (
            json.loads(kwargs["input"])["authorization"]
            == "Bearer generated-private-token"
        )
        if change == "revoked":
            inputs.source.context = BrokerPermissionContextV1(
                (inputs.grant,),
                (
                    BrokerPermissionRevocationV1(
                        inputs.grant.artifact_id, 15, "withdrawn"
                    ),
                ),
                1,
            )
        elif change == "expired":
            inputs.now[0] = 30
        elif change == "provider_revoked":
            current = inputs.rights.current
            policy = current.policies[0]
            inputs.rights.current = replace(
                current,
                revocations=(
                    BrokerPolicyRevocationV1(
                        policy.artifact_id,
                        1000,
                        1000,
                        policy.evidence_ids,
                        "withdrawn",
                    ),
                ),
            )
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "status": 200,
                    "body": base64.b64encode(b"generated-response").decode(
                        "ascii"
                    ),
                }
            ).encode("ascii"),
            stderr=b"",
        )

    monkeypatch.setattr(
        "histdatacom.broker_plugin_permissions.resources.subprocess.run",
        transport,
    )
    with (
        provider_policy_scope(inputs.rights),
        broker_permission_scope(inputs.authority, resources=resources),
    ):
        if change == "none":
            response = resources.request(
                "endpoint", "POST", "/api/read", secret_profile="login"
            )
            assert response.body == b"generated-response"
        else:
            with pytest.raises(
                ValueError, match="broker host transport refused"
            ):
                resources.request(
                    "endpoint", "POST", "/api/read", secret_profile="login"
                )
    assert calls == ["resolve", "transport"]
    # Even a refused response never forgets the resolved known-private value.
    with pytest.raises(ValueError):
        resources.check_public_text("generated-private-token")


@pytest.mark.parametrize("placement", ["path", "body", "encoded_body"])
def test_known_secret_cannot_escape_in_unprofiled_url_or_body(
    monkeypatch, placement
):
    inputs = _inputs()
    calls = []
    private = "generated-private-token"

    class Provider:
        def resolve(self, handle):
            return private

    resources = _resources(inputs, Provider())

    def transport(command, **kwargs):
        calls.append(json.loads(kwargs["input"])["authorization"])
        return SimpleNamespace(
            stdout=b'{"status":200,"body":"b2s="}', stderr=b""
        )

    monkeypatch.setattr(
        "histdatacom.broker_plugin_permissions.resources.subprocess.run",
        transport,
    )
    with (
        provider_policy_scope(inputs.rights),
        broker_permission_scope(inputs.authority, resources=resources),
    ):
        assert (
            resources.request(
                "endpoint", "POST", "/api/login", secret_profile="login"
            ).body
            == b"ok"
        )
        path = "/api/" + private if placement == "path" else "/api/read"
        body = (
            private.encode()
            if placement == "body"
            else (
                base64.b64encode(private.encode())
                if placement == "encoded_body"
                else b""
            )
        )
        with pytest.raises(ValueError, match="private_material_refused"):
            resources.request("endpoint", "POST", path, body=body)
    assert calls == ["Bearer " + private]


def test_rotating_secret_inventory_refuses_before_effect_at_bound(monkeypatch):
    inputs = _inputs()

    class Provider:
        def resolve(self, handle):
            return "generated-new-credential"

    resources = _resources(inputs, Provider())
    resources._private = {
        f"generated-previous-{index:04}" for index in range(384)
    }
    previous = resources._private.copy()
    monkeypatch.setattr(
        "histdatacom.broker_plugin_permissions.resources.subprocess.run",
        lambda *args, **kwargs: pytest.fail(
            "exhausted canary inventory reached transport"
        ),
    )
    with (
        provider_policy_scope(inputs.rights),
        broker_permission_scope(inputs.authority, resources=resources),
        pytest.raises(ValueError, match="secret resolution refused"),
    ):
        resources.request(
            "endpoint", "POST", "/api/login", secret_profile="login"
        )
    assert resources._private == previous


def _revoke(inputs):
    inputs.source.context = BrokerPermissionContextV1(
        (inputs.grant,),
        (
            BrokerPermissionRevocationV1(
                inputs.grant.artifact_id, 15, "withdrawn"
            ),
        ),
        1,
    )


def test_native_schema_return_cannot_outlive_permission_revoked_inside_plugin():
    from histdatacom.broker_plugin_capabilities import (
        invoke_authorized_broker_factory,
    )
    from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
    from histdatacom.broker_plugins import BrokerPluginMetadataV1

    inputs = _inputs()
    resources = _resources(inputs)
    registration = inputs.request.plan.candidate.registration

    class Plugin:
        metadata = BrokerPluginMetadataV1(
            registration.plugin_id,
            registration.plugin_version,
            registration.display_name,
        )

        @property
        def configuration_schema(self):
            _revoke(inputs)
            return inputs.request.configuration_profile.schema

    with (
        provider_policy_scope(inputs.rights),
        broker_permission_scope(inputs.authority, resources=resources),
    ):
        plugin = invoke_authorized_broker_factory(
            BrokerPluginInventoryV1((inputs.request.plan.candidate,)),
            inputs.request.plan,
            authorize=lambda _: True,
            factory=lambda resources: Plugin(),
            provider_request=inputs.request,
        )
        with pytest.raises(BrokerPermissionError, match="grant_revoked"):
            _ = plugin.configuration_schema


def test_final_native_manifest_publish_cannot_outlive_grant(tmp_path):
    from histdatacom.broker_plugin_lifecycle import (
        BrokerLifecycleHeaderV1,
        BrokerLifecyclePolicyV1,
    )
    from histdatacom.broker_plugin_lifecycle.storage import Journal
    from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1

    inputs = _inputs()
    resources = _resources(inputs)
    header = BrokerLifecycleHeaderV1(
        BrokerPluginInventoryV1((inputs.request.plan.candidate,)),
        inputs.request.plan,
        BrokerLifecyclePolicyV1(),
        (),
        "c" * 32,
        "3.10.19",
    )
    with (
        provider_policy_scope(inputs.rights),
        broker_permission_scope(inputs.authority, resources=resources),
    ):
        journal = Journal(
            tmp_path / "generated-run", header, provider_request=inputs.request
        )
        before = {
            path.name: path.read_bytes() for path in journal.directory.iterdir()
        }
        try:
            _revoke(inputs)
            with pytest.raises(BrokerPermissionError, match="grant_revoked"):
                journal.publish(journal.manifest)
            assert {
                path.name: path.read_bytes()
                for path in journal.directory.iterdir()
            } == before
        finally:
            journal.close()

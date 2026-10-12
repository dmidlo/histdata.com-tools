"""Cached SDK1.1 outputs retain current rights, without another callback."""

import hashlib
import runpy
import time
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.broker_plugin_capabilities import (
    invoke_authorized_broker_factory,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_conformance import BrokerConformanceDriverV1
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionContextV1,
    BrokerPermissionError,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    broker_permission_scope,
)
from histdatacom.broker_plugin_policy import provider_policy_scope
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
)
from tests.fixtures import broker_sdk_scientific_qualification as fixture


@pytest.mark.parametrize("output", ("metadata", "provider_protocol"))
def test_cached_output_rechecks_optional_raw_grant_without_callback(
    monkeypatch, output
):
    root = Path(__file__).parents[1] / "fixtures/broker_sdk11_plugin"
    package = root / "src/histdatacom_generated_sdk11"
    registration = (
        package / "_histdatacom_broker_plugins/org.example.sdk11.json"
    ).read_text()
    candidate = BrokerPluginCandidateV1(
        BrokerPluginRegistrationV1.from_json(registration),
        hashlib.sha256(registration.encode()).hexdigest(),
        hashlib.sha256((package / "plugin.py").read_bytes()).hexdigest(),
    )
    manifest = BrokerPermissionManifestV1.from_json(
        (
            package / "_histdatacom_broker_permissions/org.example.sdk11.json"
        ).read_text()
    )
    manifest = replace(
        manifest,
        required_atoms=("emit:health", "emit:quotes"),
        optional_atoms=("raw_payload:emit",),
    )
    driver = replace(
        BrokerConformanceDriverV1.from_json(
            (root / "conformance/org.example.sdk11.json").read_text()
        ),
        permission_manifest_id=manifest.artifact_id,
    )
    monkeypatch.setattr(
        fixture, "read_installed_broker_permissions", lambda *a, **kw: manifest
    )
    inventory = BrokerPluginInventoryV1((candidate,), sdk_version="1.1.0")
    request, _ = fixture.invocation(inventory, driver, "1.1.0")
    workflow = replace(
        request.plan.workflow,
        operations=tuple(
            sorted((*request.plan.workflow.operations, "metadata"))
        ),
    )
    request = replace(
        request,
        plan=negotiate_broker_capabilities(
            inventory, workflow, plugin_id=candidate.registration.plugin_id
        ),
    )
    full = fixture.authority(request, manifest)
    now = time.time_ns()
    grant = BrokerPermissionGrantV1(
        full.binding,
        ("emit:health", "emit:quotes"),
        "generated-fixture",
        now,
        now + 3600 * fixture.SECOND,
        "f" * 32,
    )
    limited = BrokerPermissionAuthorityV1(
        manifest,
        full.binding,
        grant.artifact_id,
        fixture.PermissionSource(BrokerPermissionContextV1((grant,))),
    )
    plugin = runpy.run_path(str(package / "plugin.py"))["factory"]()
    with provider_policy_scope(fixture.current_policy(request)):
        with broker_permission_scope(full):
            gated = invoke_authorized_broker_factory(
                inventory,
                request.plan,
                authorize=lambda _: True,
                factory=lambda: plugin,
                provider_request=request,
            )
            assert gated.metadata.metadata == plugin.metadata
            assert (
                gated.provider_protocol()
                == plugin.provider_protocol.__globals__["INFO"]
            )
            assert plugin.protocol_calls == 1
        with broker_permission_scope(limited):
            with pytest.raises(
                BrokerPermissionError, match="resource_permission_denied"
            ):
                value = getattr(gated, output)
                if output == "provider_protocol":
                    value()
            assert plugin.protocol_calls == 1

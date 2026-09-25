"""Run against an installed host wheel, with a separate fixture wheel path.

Usage: python qualify_broker_capabilities.py /absolute/plugin-fixture.whl
Only the offline fixture is installed/uninstalled; the host is never modified.
Copy broker_provider_policy.py beside this script for its explicit generated
declarations. The two helpers used here resolve only installed host APIs;
unrelated repository-only fixture builders are never invoked.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import histdatacom.broker_plugin_capabilities as capabilities
import histdatacom.broker_plugin_policy as provider_policy
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityError,
    BrokerCapabilityWorkflowV1,
    BrokerInvocationAssociation,
    invoke_authorized_installed_broker_plugin,
    negotiate_broker_capabilities,
    verify_broker_admitted_event,
)
from histdatacom.broker_plugin_policy import BrokerPolicyError
from broker_provider_policy import (
    generated_provider_scope,
    generated_sdk_request,
)
from histdatacom.broker_plugin_registry import (
    discover_broker_plugins,
    inspect_broker_plugins,
)


def main() -> None:
    assert capabilities.__file__ is not None
    assert "site-packages" in capabilities.__file__, capabilities.__file__
    assert provider_policy.__file__ is not None
    assert "site-packages" in provider_policy.__file__, provider_policy.__file__
    wheel = Path(sys.argv[1]).resolve(strict=True)
    before = discover_broker_plugins()
    assert not any(
        item.registration.plugin_id == "org.example.capabilities"
        for item in before.candidates
    )
    installed = False
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--no-index",
                str(wheel),
            ],
            check=True,
        )
        installed = True
        inventory = discover_broker_plugins()
        assert len(inventory.candidates) == len(before.candidates) + 1
        assert inspect_broker_plugins(
            inventory, plugin_id="org.example.capabilities"
        )
        operations = tuple(
            sorted(
                (
                    "metadata",
                    "configuration_schema",
                    "open_session",
                    "instruments",
                    "subscribe",
                    "iter_events",
                    "unsubscribe",
                    "close_session",
                )
            )
        )
        plan = negotiate_broker_capabilities(
            inventory,
            BrokerCapabilityWorkflowV1(operations),
            plugin_id="org.example.capabilities",
        )
        assert plan.admitted
        assert "capability_fixture" not in sys.modules
        request = generated_sdk_request(plan)
        try:
            invoke_authorized_installed_broker_plugin(
                inventory,
                plan,
                authorize=lambda _: True,
                provider_request=request,
            )
        except BrokerPolicyError as error:
            assert error.reason == "current_process_policy_scope_required"
        else:
            raise AssertionError("provider rights scope was not required")
        assert "capability_fixture" not in sys.modules
        with generated_provider_scope(request):
            try:
                invoke_authorized_installed_broker_plugin(
                    inventory,
                    plan,
                    authorize=lambda _: False,
                    provider_request=request,
                )
            except BrokerCapabilityError as error:
                assert str(error) == "authorization_required"
            else:
                raise AssertionError("unauthorized plugin executed")
            assert "capability_fixture" not in sys.modules
            invocation = invoke_authorized_installed_broker_plugin(
                inventory,
                plan,
                authorize=lambda _: True,
                provider_request=request,
            )
            assert (
                invocation.binding.association
                is BrokerInvocationAssociation.INSTALLED_ENTRYPOINT
            )
            invocation.open_session({})
            invocation.instruments()
            invocation.subscribe(("EURUSD",))
            evidence = list(invocation.iter_events())
            assert len(evidence) == 1
            verify_broker_admitted_event(evidence[0], plan)
            invocation.unsubscribe(("EURUSD",))
            invocation.close_session()
            print(
                json.dumps(
                    {
                        "host": capabilities.__file__,
                        "plan_id": plan.artifact_id,
                        "binding": invocation.binding.to_dict(),
                        "event_id": evidence[0].artifact_id,
                        "calls": sys.modules["capability_fixture.plugin"].CALLS,
                    },
                    sort_keys=True,
                )
            )
    finally:
        if installed:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "uninstall",
                    "-y",
                    "histdatacom-capability-fixture",
                ],
                check=True,
            )
    assert discover_broker_plugins().to_json() == before.to_json()
    print(
        "PASS: installed namespace, metadata-only preflight, unauthorized zero import, exact entrypoint workflow, original inventory restored"
    )


if __name__ == "__main__":
    main()

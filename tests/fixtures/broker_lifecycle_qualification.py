"""Qualify installed host + separately pip-installed offline plugin.

Usage: python broker_lifecycle_qualification.py /absolute/fixture.whl
Copy broker_runtime_policy.py and broker_provider_policy.py beside this script.
Does not install/modify the host or dependencies; restores plugin inventory.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile

import histdatacom.broker_plugin_lifecycle as lifecycle
import histdatacom
import histdatacom.broker_plugin_policy as policy_api
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecycleError,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason,
    inspect_broker_lifecycle,
    replay_broker_lifecycle as _replay,
    run_broker_plugin_lifecycle as _run,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from broker_runtime_policy import runtime_request, runtime_scope

_REQUESTS = {}


def run_broker_plugin_lifecycle(
    inventory, plan, configuration, symbols, output, **kwargs
):
    """Declare this generated fixture's exact rights at every public call."""
    request = runtime_request(plan, configuration)
    _REQUESTS[output] = request
    with runtime_scope(request):
        return _run(
            inventory,
            plan,
            configuration,
            symbols,
            output,
            provider_request=request,
            **kwargs,
        )


def replay_broker_lifecycle(output):
    request = _REQUESTS[output]
    with runtime_scope(request):
        yield from _replay(output, provider_request=request)


def main() -> None:
    assert histdatacom.__file__ is not None
    assert "site-packages" in histdatacom.__file__, histdatacom.__file__
    assert policy_api.__file__ is not None
    assert "site-packages" in policy_api.__file__, policy_api.__file__
    assert lifecycle.__file__ is not None
    assert "site-packages" in lifecycle.__file__, lifecycle.__file__
    wheel = Path(sys.argv[1]).resolve(strict=True)
    before = discover_broker_plugins()
    assert not any(
        item.registration.plugin_id == "org.example.lifecycle"
        for item in before.candidates
    )
    installed = False
    results: list[dict[str, object]] = []
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--no-deps",
                str(wheel),
            ],
            check=True,
        )
        installed = True
        inventory = discover_broker_plugins()
        operations = tuple(
            sorted(
                (
                    "configuration_schema",
                    "open_session",
                    "instruments",
                    "subscribe",
                    "iter_events",
                    "unsubscribe",
                )
            )
        )
        plan = negotiate_broker_capabilities(
            inventory,
            BrokerCapabilityWorkflowV1(operations),
            plugin_id="org.example.lifecycle",
        )
        assert plan.admitted
        with tempfile.TemporaryDirectory(
            prefix="broker-lifecycle-qualified-"
        ) as temporary:
            root = Path(temporary)
            try:
                run_broker_plugin_lifecycle(
                    inventory,
                    plan,
                    {"mode": "finite"},
                    ("EURUSD",),
                    root / "unauthorized",
                    authorize=lambda _: False,
                )
            except BrokerLifecycleError as error:
                assert error.reason is BrokerLifecycleReason.AUTHORIZATION
            else:
                raise AssertionError("unauthorized worker accepted")
            assert not (root / "unauthorized").exists()
            for mode in (
                "finite",
                "open_block",
                "next_block",
                "close_block",
                "reconnect",
                "health_gap",
                "health_error",
                "inherited_pipes",
            ):
                result = run_broker_plugin_lifecycle(
                    inventory,
                    plan,
                    {"mode": mode, "credential": "qualification-not-retained"},
                    ("EURUSD",),
                    root / mode,
                    authorize=lambda _: True,
                    policy=BrokerLifecyclePolicyV1(
                        startup_timeout_ms=8000,
                        run_timeout_ms=15000 if mode == "next_block" else 60000,
                        acknowledgement_timeout_ms=5000,
                        shutdown_timeout_ms=100,
                        retry_delays_ms=(0,) if mode == "reconnect" else (),
                    ),
                )
                assert result.manifest.worker_reaped
                assert not result.manifest.source_continuity_verified
                records = tuple(replay_broker_lifecycle(result.directory))
                assert len(records) == result.manifest.appended_records
                assert inspect_broker_lifecycle(result.directory).complete is (
                    mode == "finite"
                ), (mode, result.reason.value, result.manifest.state.value)
                if mode == "finite":
                    assert (
                        result.manifest.completion
                        is BrokerLifecycleCompletion.COMPLETE
                    )
                    assert result.manifest.appended_events == 2
                else:
                    assert (
                        result.manifest.completion
                        is BrokerLifecycleCompletion.PARTIAL
                    )
                retained = b"".join(
                    path.read_bytes() for path in result.directory.iterdir()
                )
                assert b"qualification-not-retained" not in retained
                results.append(
                    {
                        "mode": mode,
                        "reason": result.reason.value,
                        "manifest_id": result.manifest.artifact_id,
                        "events": result.manifest.appended_events,
                        "completion": result.manifest.completion.value,
                        "reaped": result.manifest.worker_reaped,
                    }
                )
        assert not any(
            name == "lifecycle_fixture" or name.startswith("lifecycle_fixture.")
            for name in sys.modules
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
                    "histdatacom-lifecycle-fixture",
                ],
                check=True,
            )
    assert discover_broker_plugins().to_json() == before.to_json()
    print(
        json.dumps(
            {
                "host": lifecycle.__file__,
                "python": sys.version.split()[0],
                "runs": results,
            },
            sort_keys=True,
        )
    )
    print(
        "PASS: installed worker lifecycle, bounded blocked calls, partial replay, no parent plugin import, exact inventory restoration"
    )


if __name__ == "__main__":
    main()

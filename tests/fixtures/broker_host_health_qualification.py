"""Installed third-party native-health matrix; observations come from the host.

Copy alongside broker_host_health_{external,wheel}.py and the permission
qualification/wheel/external and provider_policy helpers to a neutral directory.
No generated health number is supplied to the host recorder or reducer.
"""

from pathlib import Path
import json
import subprocess
import sys

import histdatacom
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthPolicyV1,
    BrokerHostHealthFailure as Failure,
    BrokerHostHealthState as State,
)
from histdatacom.broker_plugin_health.storage import read_lifecycle_host_health
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecyclePolicyV1,
    replay_broker_lifecycle,
    run_broker_plugin_lifecycle,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from broker_host_health_wheel import build_host_health_wheel
from broker_permission_qualification import admitted


def main():
    assert "site-packages" in histdatacom.__file__
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    modes = ("honest", "duplicate", "reorder", "gap", "delay", "unchanged")
    if len(sys.argv) == 3:
        selected = tuple(sys.argv[2].split(","))
        assert selected and len(set(selected)) == len(selected)
        assert set(selected) <= set(modes)
        modes = selected
    baseline = discover_broker_plugins().to_json()
    assert "org.example.permissions" not in baseline
    wheel = build_host_health_wheel(output / "fixture-wheel")
    installed = False
    outcomes = []
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
        plan = negotiate_broker_capabilities(
            inventory,
            BrokerCapabilityWorkflowV1(
                tuple(
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
                ),
                optional=(
                    "gaps.v1",
                    "heartbeat.v1",
                    "timestamps.broker-event.v1",
                ),
            ),
            plugin_id="org.example.permissions",
        )
        for mode in modes:
            configuration = {"mode": mode}
            with admitted(plan, configuration) as (request, secret):
                result = run_broker_plugin_lifecycle(
                    inventory,
                    plan,
                    configuration,
                    ("EURUSD",),
                    output / mode,
                    authorize=lambda _: True,
                    provider_request=request,
                    worker_python=sys.executable,
                    policy=BrokerLifecyclePolicyV1(
                        startup_timeout_ms=15000,
                        # This installed diagnostic lane verifies strict
                        # evidence semantics, not capture throughput. Fresh
                        # provider/permission checks before every retained
                        # observation add measurable host-side work.
                        run_timeout_ms=120000,
                        acknowledgement_timeout_ms=5000,
                    ),
                    health_policy=BrokerHostHealthPolicyV1(
                        bucket_width_ns=60_000_000_000,
                        # The source clock below is generated fixed evidence,
                        # not synchronized wall time; concurrent test load is
                        # not the condition this matrix is trying to diagnose.
                        max_clock_jump_ns=60_000_000_000,
                        max_persistence_p95_ns=60_000_000_000,
                        max_heartbeat_gap_ns=(
                            50_000_000 if mode == "delay" else None
                        ),
                        require_known_upstream_loss=True,
                    ),
                )
                assert result.manifest.worker_reaped
                assert result.manifest.completion is (
                    BrokerLifecycleCompletion.PARTIAL
                    if mode == "gap"
                    else BrokerLifecycleCompletion.COMPLETE
                ), result.reason
                records = tuple(
                    replay_broker_lifecycle(
                        result.directory, provider_request=request
                    )
                )
                audit = read_lifecycle_host_health(
                    result.health_directory, result.manifest, records, request
                )
                assert audit == result.health
                assert audit.complete_observations is (mode != "gap")
                assert audit.state is State.INSUFFICIENT
                assert Failure.UPSTREAM_UNKNOWN in audit.failures
                if mode in {"honest", "unchanged"}:
                    assert audit.failures == (Failure.UPSTREAM_UNKNOWN,)
                assert all(
                    bucket.upstream_loss_unknown for bucket in audit.buckets
                )
                assert (
                    sum(bucket.known_host_dropped for bucket in audit.buckets)
                    == 0
                )
                assert sum(
                    bucket.healthy_claim_count for bucket in audit.buckets
                ) == (0 if mode == "honest" else 1)
                expected = {
                    "duplicate": Failure.DUPLICATE,
                    "reorder": Failure.REORDER,
                    "gap": Failure.SOURCE_GAP,
                    "delay": Failure.HEARTBEAT_GAP,
                }.get(mode)
                if expected is not None:
                    assert expected in audit.failures, (mode, audit.failures)
                if mode in {"duplicate", "reorder", "gap", "delay"}:
                    assert Failure.PLUGIN_CLAIM in audit.failures
                    assert (
                        sum(
                            bucket.healthy_claim_discrepancies
                            for bucket in audit.buckets
                        )
                        == 1
                    )
                if mode == "duplicate":
                    assert (
                        sum(
                            bucket.exact_quote_duplicates
                            for bucket in audit.buckets
                        )
                        == 1
                    )
                if mode == "gap":
                    assert (
                        sum(bucket.gap_count for bucket in audit.buckets) == 1
                    )
                if mode == "unchanged":
                    assert (
                        sum(
                            bucket.exact_quote_duplicates
                            for bucket in audit.buckets
                        )
                        == 0
                    )
                    assert (
                        sum(bucket.unchanged_quotes for bucket in audit.buckets)
                        == 2
                    )
                    assert Failure.PLUGIN_CLAIM not in audit.failures
                if mode == "delay":
                    assert (
                        max(
                            bucket.max_heartbeat_gap_ns or 0
                            for bucket in audit.buckets
                        )
                        >= 150_000_000
                    )
                assert secret.calls == 0
                outcomes.append(
                    {
                        "mode": mode,
                        "state": audit.state.value,
                        "failures": [item.value for item in audit.failures],
                        "audit_id": audit.artifact_id,
                    }
                )
                print(json.dumps(outcomes[-1], sort_keys=True), flush=True)
        assert "permission_fixture.plugin" not in sys.modules
    finally:
        if installed:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "uninstall",
                    "-y",
                    "histdatacom-permission-fixture",
                ],
                check=True,
            )
    assert discover_broker_plugins().to_json() == baseline
    assert all(
        "site-packages" in str(getattr(module, "__file__", ""))
        for name, module in sys.modules.items()
        if name == "histdatacom" or name.startswith("histdatacom.")
    )
    print(
        json.dumps(
            {
                "cases": len(outcomes),
                "inventory_restored": True,
                "installed_host": histdatacom.__file__,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

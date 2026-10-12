"""Source-only genuine native capture helper; installed qualification is separate."""

import subprocess
import venv
from importlib import metadata
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

from histdatacom.broker_capture import fit_broker_delivery_fingerprint
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthPolicyV1,
    BrokerHostHealthState,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecyclePolicyV1,
    run_broker_plugin_lifecycle,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from tests.fixtures.broker_lifecycle_wheel import build_lifecycle_wheel
from tests.fixtures.broker_runtime_policy import runtime_request, runtime_scope


def scientific_lifecycle_policy():
    """Bound slow generated IPC without retrying or relaxing health gates."""
    return BrokerLifecyclePolicyV1(
        startup_timeout_ms=15000,
        run_timeout_ms=120000,
        acknowledgement_timeout_ms=10000,
        delivery_retries=0,
        retry_delays_ms=(),
    )


def qualified_sdk_capture(directory: Path, *, mode="honest"):
    """Return (native result, exact SDK request, V3) from one bounded worker.

    Caller supplies a fresh path. The wheel is extracted in an isolated no-pip
    worker environment, never installed into the caller or global interpreter.
    Replay needs no plugin distribution and uses explicit generated rights.
    """
    environment = directory / "worker"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    python = environment / "bin/python"
    site = Path(
        subprocess.check_output(
            [
                str(python),
                "-c",
                "import sysconfig; print(sysconfig.get_path('purelib'))",
            ],
            text=True,
        ).strip()
    )
    wheel = build_lifecycle_wheel(directory / "wheel", scientific=True)
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    distribution = next(metadata.distributions(path=[str(site)]))
    with patch.object(metadata, "distributions", lambda: iter((distribution,))):
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
                required=("health.v1",),
                optional=(
                    "timestamps.adapter-receive.v1",
                    "timestamps.broker-event.v1",
                ),
            ),
            plugin_id="org.example.lifecycle",
        )
        request = runtime_request(plan, {"mode": mode})
        with runtime_scope(request):
            result = run_broker_plugin_lifecycle(
                inventory,
                plan,
                {"mode": mode},
                ("EURUSD",),
                directory / "capture",
                authorize=lambda _: True,
                provider_request=request,
                worker_python=str(python),
                policy=scientific_lifecycle_policy(),
                health_policy=BrokerHostHealthPolicyV1(
                    max_observations=256,
                    max_buckets=256,
                    max_samples_per_bucket=64,
                    max_clock_jump_ns=1_000_000_000,
                    max_persistence_p95_ns=60_000_000_000,
                    require_known_upstream_loss=False,
                ),
            )
            assert (
                result.health.state is BrokerHostHealthState.QUALIFIED
            ), result.health.failures
            fingerprint = fit_broker_delivery_fingerprint(
                result.directory,
                (result.manifest,),
                provider_requests=(request,),
            )
    return result, request, fingerprint

"""Installed host + separately pip-installed generated permission adversary.

Copy this script, broker_permission_wheel.py, broker_permission_external.py,
and broker_provider_policy.py into a neutral directory. Run with an installed
host interpreter and an explicit output directory. No host/dependency install
is performed; the generated plugin is always uninstalled afterward.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib import metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
from unittest.mock import patch

import histdatacom
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleRecordV1,
    replay_broker_lifecycle,
)
from histdatacom.broker_plugin_permissions import (
    BrokerHostSecretProfileV1,
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionGrantV1,
    BrokerPermissionResourcesV1,
    broker_permission_scope,
    read_installed_broker_permissions,
    permission_resource_path,
    verify_permission_execution,
)
from histdatacom.broker_plugin_policy.bindings import (
    BrokerProviderConfigurationV1,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_policy.scope import (
    BrokerPolicyError,
    provider_policy_scope,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthObservationV1,
    BrokerHostHealthObservationKind,
    BrokerHostHealthReason,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_security import (
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerTrustTier,
    run_secure_broker_plugin,
    run_trusted_broker_plugin,
)
from histdatacom.broker_plugins import BrokerEventKind
from broker_permission_wheel import (
    build_permission_wheel,
    permission_fixture_schema,
)
from broker_provider_policy import MutablePolicySource, policy_context


class GrantSource:
    def __init__(self, grant):
        self.context = BrokerPermissionContextV1((grant,))

    def read_context(self):
        return self.context


class Secrets:
    value = "generated-installed-opaque-authentication-canary"

    def __init__(self):
        self.calls = 0

    def resolve(self, handle):
        assert handle == "generated-installed-opaque-handle"
        self.calls += 1
        return self.value


@contextmanager
def admitted(plan, configuration, *, denied=()):
    manifest = read_installed_broker_permissions(plan.candidate)
    request = BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            "fixture",
            "generated-permission-profile",
            permission_fixture_schema().to_json(),
            json.dumps(configuration, sort_keys=True, separators=(",", ":")),
        ),
        BrokerProviderOutputContractV1(
            "sdk-v1",
            tuple(sorted(kind.value for kind in BrokerEventKind)),
            allow_raw_hashes=True,
            allow_opaque_metadata=True,
        ),
    )
    binding = BrokerPermissionBindingV1(
        plan.candidate.artifact_id,
        manifest.artifact_id,
        "1.0.0",
        "fixture",
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        tuple(atom for atom in manifest.declared_atoms if atom not in denied),
        "generated-operator",
        0,
        2**63 - 1,
        "f" * 32,
    )
    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, GrantSource(grant)
    )
    secret = Secrets()
    resources = BrokerPermissionResourcesV1(
        authority,
        provider_request=request,
        secret_profiles=(
            BrokerHostSecretProfileV1(
                "paper", "generated-installed-opaque-handle"
            ),
        ),
        secret_provider=secret,
    )
    provider_source = MutablePolicySource(
        policy_context(sdk_invocation_binding(request))
    )
    with (
        provider_policy_scope(provider_source),
        broker_permission_scope(authority, resources=resources),
    ):
        yield request, secret


def main():
    assert (
        histdatacom.__file__ is not None
        and "site-packages" in histdatacom.__file__
    )
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    trusted_case = json.loads(sys.argv[2]) if len(sys.argv) == 3 else None
    baseline = discover_broker_plugins().to_json()
    if trusted_case is None:
        assert "org.example.permissions" not in baseline
    observed = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            observed.append((self.path, self.headers.get("Authorization")))
            data = b"generated-response"
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    installed = False
    results = []
    try:
        if trusted_case is None:
            wheel = build_permission_wheel(
                output / "fixture-wheel", port=server.server_port
            )
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
                )
            ),
            plugin_id="org.example.permissions",
        )
        assert "permission_fixture.plugin" not in sys.modules
        assert (
            read_installed_broker_permissions(plan.candidate).candidate_id
            == plan.candidate.artifact_id
        )
        assert "permission_fixture.plugin" not in sys.modules
        lifecycle = BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            acknowledgement_timeout_ms=5000,
        )

        def run(mode, *, isolated, denied=(), extra=None, expect=True):
            if not isolated and trusted_case is None:
                # Each installed trusted invocation requires a fresh caller:
                # the loader deliberately refuses a pre-imported entry module.
                before = len(observed)
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        str(output),
                        json.dumps(
                            {
                                "mode": mode,
                                "denied": denied,
                                "extra": extra,
                                "expect": expect,
                            }
                        ),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=90,
                    check=False,
                )
                assert completed.returncode == 0, completed.stderr
                result = json.loads(completed.stdout.strip().splitlines()[-1])
                assert result["complete"] is expect
                if not expect:
                    assert len(observed) == before
                results.append(result)
                print(json.dumps(result, sort_keys=True), flush=True)
                return
            configuration = {"mode": mode, **(extra or {})}
            target = output / (
                ("isolated-" if isolated else "trusted-")
                + mode
                + ("-denied" if denied else "")
            )
            before = len(observed)
            tracked_processes = []
            original_popen = subprocess.Popen

            def tracked_popen(*args, **kwargs):
                process = original_popen(*args, **kwargs)
                tracked_processes.append(process)
                return process

            with admitted(plan, configuration, denied=denied) as (
                request,
                secret,
            ):
                policy = BrokerSecurityPolicyV1(
                    plan.candidate.artifact_id,
                    (
                        BrokerTrustTier.DEVELOPMENT
                        if isolated
                        else BrokerTrustTier.REVIEWED
                    ),
                    (
                        BrokerSecurityMode.KERNEL_ISOLATED
                        if isolated
                        else BrokerSecurityMode.TRUSTED_IN_PROCESS
                    ),
                )
                try:
                    if isolated:
                        with (
                            patch.object(subprocess, "Popen", tracked_popen)
                            if mode.startswith("worker_bypass_")
                            else nullcontext()
                        ):
                            result = run_secure_broker_plugin(
                                inventory,
                                plan,
                                policy,
                                configuration,
                                ("EURUSD",),
                                target,
                                authorize=lambda _: True,
                                provider_request=request,
                                lifecycle_policy=lifecycle,
                                worker_python=sys.executable,
                            )
                        assert result.native.manifest.worker_reaped
                        complete = (
                            result.native.manifest.completion
                            is BrokerLifecycleCompletion.COMPLETE
                        )
                        retained = tuple(
                            replay_broker_lifecycle(
                                result.native.directory,
                                provider_request=request,
                            )
                        )
                        assert (
                            len(retained)
                            == result.native.manifest.appended_records
                        )
                        if mode.startswith("worker_bypass_"):
                            assert not any(
                                item.kind == "event" for item in retained
                            )
                            assert (
                                sum(
                                    bucket.refused
                                    for bucket in result.native.health.buckets
                                )
                                >= 1
                            )
                        encoded = (
                            "".join(item.to_json() for item in retained)
                            + result.security.to_json()
                        )
                        verify_permission_execution(
                            result.native.permissions,
                            request,
                            result.native.manifest,
                        )
                        assert secret.value not in encoded
                        reason = result.native.reason.value
                    else:
                        result = run_trusted_broker_plugin(
                            inventory,
                            plan,
                            policy,
                            configuration,
                            ("EURUSD",),
                            authorize=lambda _: True,
                            provider_request=request,
                        )
                        complete = True
                        encoded = result.receipt.to_json()
                        verify_permission_execution(
                            result.permissions, request, result.receipt
                        )
                        assert secret.value not in encoded
                        reason = "trusted_complete"
                except ValueError as error:
                    if expect:
                        raise
                    if mode.startswith("worker_bypass_"):
                        assert type(error) is BrokerPolicyError
                        assert error.reason == "worker_provider_policy_refused"
                        health = target.with_name(target.name + "-host-health")
                        assert not (health / "audit.json").exists()
                        assert not (
                            health / "permission-execution.json"
                        ).exists()
                        observations = tuple(
                            BrokerHostHealthObservationV1.from_json(line)
                            for line in (health / "observations.jsonl")
                            .read_text()
                            .splitlines()
                        )
                        assert any(
                            item.kind is BrokerHostHealthObservationKind.REFUSED
                            and item.reason is BrokerHostHealthReason.PERMISSION
                            for item in observations
                        )
                        records = tuple(
                            BrokerLifecycleRecordV1.from_json(line)
                            for path in target.glob("partition-*.partial")
                            for line in path.read_text().splitlines()
                        )
                        assert records and not any(
                            item.kind == "event" for item in records
                        )
                        workers = [
                            process
                            for process in tracked_processes
                            if any(
                                "broker_plugin_lifecycle.worker" in str(arg)
                                for arg in process.args
                            )
                        ]
                        assert len(workers) == 1
                        assert workers[0].returncode is not None
                        try:
                            os.waitpid(workers[0].pid, os.WNOHANG)
                        except ChildProcessError:
                            pass
                        else:
                            raise AssertionError(
                                "worker was not already reaped"
                            )
                    assert secret.value not in str(error)
                    complete = False
                    reason = type(error).__name__
                assert complete is expect, (mode, reason)
                if not expect:
                    assert len(observed) == before
                results.append(
                    {
                        "mode": mode,
                        "isolated": isolated,
                        "denied": list(denied),
                        "complete": complete,
                        "reason": reason,
                    }
                )
                print(json.dumps(results[-1], sort_keys=True), flush=True)

        if trusted_case is not None:
            run(isolated=trusted_case.pop("isolated", False), **trusted_case)
            return

        def check_cli(*, expected=0):
            completed = subprocess.run(
                [
                    str(Path(sys.executable).with_name("histdatacom")),
                    "broker-plugins",
                    "permissions",
                    "--plugin-id",
                    "org.example.permissions",
                    "--provider",
                    "fixture",
                    "--json",
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            assert completed.returncode == expected, completed.stderr
            if expected == 0:
                payload = json.loads(completed.stdout)
                assert payload["operator_grants_applied"] is False
                assert payload["activation_or_scientific_admission"] is False
                assert payload["permission_manifest"]
            else:
                assert completed.stdout == ""
                assert json.loads(completed.stderr) == {
                    "reason": "invalid_installed_permission_manifest",
                    "activation_or_scientific_admission": False,
                }
            assert "permission_fixture.plugin" not in sys.modules

        check_cli()
        for isolated in (False, True):
            for mode in (
                "finite",
                "cache",
                "network",
                "secret",
                "health",
                "sizes",
                "raw",
            ):
                run(mode, isolated=isolated)
            for mode in (
                "network_undeclared",
                "network_method",
                "network_path",
                "secret_undeclared",
                "cache_undeclared",
                "cache_path",
                "cache_size",
                "subprocess",
            ):
                run(mode, isolated=isolated, expect=False)
            for mode, atom in (
                ("network", "network:provider:fixture"),
                ("secret", "secrets:read:paper"),
                ("cache", "cache:plugin:prices"),
                ("sizes", "emit:sizes"),
                ("raw", "raw_payload:emit"),
            ):
                run(mode, isolated=isolated, denied=(atom,), expect=False)
        for mode in ("direct_fork", "direct_spawn", "direct_exec"):
            run(mode, isolated=True)
        for mode, atom in (
            ("worker_bypass_sizes", "emit:sizes"),
            ("worker_bypass_raw", "raw_payload:emit"),
        ):
            run(mode, isolated=True, denied=(atom,), expect=False)
        protected = output / "generated-scientific-store-canary"
        protected.write_bytes(b"generated-original")
        run("direct_write", isolated=True, extra={"target": str(protected)})
        assert protected.read_bytes() == b"generated-original"
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(0.05)
            run(
                "direct_network",
                isolated=True,
                extra={"port": listener.getsockname()[1]},
            )
            try:
                listener.accept()
            except TimeoutError:
                pass
            else:
                raise AssertionError("undeclared direct connection accepted")
        assert any(auth == "Bearer " + Secrets.value for _, auth in observed)
        blocked = build_permission_wheel(
            output / "blocked-import-wheel", block_import=True
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--no-deps",
                "--force-reinstall",
                str(blocked),
            ],
            check=True,
        )
        check_cli()
        distribution = metadata.distribution("histdatacom-permission-fixture")
        permission_path = Path(
            distribution.locate_file(
                permission_resource_path(
                    "org.example.permissions",
                    "permission_fixture.plugin:factory",
                )
            )
        )
        original = permission_path.read_bytes()
        try:
            permission_path.write_bytes(original + b" ")
            check_cli(expected=2)
            permission_path.unlink()
            check_cli(expected=2)
        finally:
            permission_path.write_bytes(original)
        check_cli()
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
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()
    assert discover_broker_plugins().to_json() == baseline
    assert all(
        "site-packages" in str(getattr(module, "__file__", ""))
        for name, module in sys.modules.items()
        if name == "histdatacom" or name.startswith("histdatacom.")
    )
    print(
        json.dumps(
            {
                "cases": len(results),
                "inventory_restored": True,
                "installed_host": histdatacom.__file__,
                "authenticated_only_host_side": True,
                "metadata_cli_cases": 5,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

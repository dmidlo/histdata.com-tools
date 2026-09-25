"""Qualify installed host + separately pip-installed offline security fixture.

Usage: python broker_security_qualification.py /absolute/fixture.whl
Copy broker_runtime_policy.py and broker_provider_policy.py beside this script.
No host/dependency installation; the fixture is always uninstalled afterward.
"""

from __future__ import annotations

import json
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit

import histdatacom
import histdatacom.broker_plugin_policy as policy_api
import histdatacom.broker_plugin_security as security
from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecyclePolicyV1,
)
from histdatacom.broker_plugin_lifecycle import (
    replay_broker_lifecycle as _replay,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_security import (
    BrokerSecurityError,
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerTrustTier,
    read_security_receipt,
    verify_security_capture,
)
from histdatacom.broker_plugin_security import (
    run_secure_broker_plugin as _secure,
)
from histdatacom.broker_plugin_security import (
    run_trusted_broker_plugin as _trusted,
)

if __package__:
    from .broker_runtime_policy import runtime_request, runtime_scope
else:
    from broker_runtime_policy import runtime_request, runtime_scope

from histdatacom.broker_plugin_permissions import (
    BrokerHostSecretProfileV1,
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionGrantV1,
    BrokerPermissionResourcesV1,
    read_installed_broker_permissions,
)

PRIVATE_CANARY = 'synthetic / private " ☃ fixture-only-known-marker'


@contextmanager
def security_permission_scope(request, provider=None):
    """Exact generated installed declaration, separate host-only auth binding."""
    manifest = read_installed_broker_permissions(request.plan.candidate)
    binding = BrokerPermissionBindingV1(
        request.plan.candidate.artifact_id,
        manifest.artifact_id,
        "1.0.0",
        request.configuration_profile.provider_id,
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        manifest.declared_atoms,
        "generated-security-operator",
        0,
        2**63 - 1,
        "d" * 32,
    )

    class Source:
        def read_context(self):
            return BrokerPermissionContextV1((grant,))

    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, Source()
    )
    resources = BrokerPermissionResourcesV1(
        authority,
        provider_request=request,
        secret_profiles=(
            (
                BrokerHostSecretProfileV1(
                    "fixture-login", "opaque-fixture-handle"
                ),
            )
            if provider is not None
            else ()
        ),
        secret_provider=provider,
    )
    with runtime_scope(request, authority=authority, resources=resources):
        yield authority


_REQUESTS = {}


def run_secure_broker_plugin(
    inventory, plan, policy, public, symbols, output, **kwargs
):
    """Explicit generated declarations; never a provider-name exemption."""
    request = runtime_request(plan, public, family="security")
    _REQUESTS[output] = request
    kwargs.setdefault(
        "lifecycle_policy",
        BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            acknowledgement_timeout_ms=5000,
        ),
    )
    provider = kwargs.pop("host_secret_provider", None)
    kwargs.setdefault("private_identifiers", (PRIVATE_CANARY,))
    with security_permission_scope(request, provider):
        return _secure(
            inventory,
            plan,
            policy,
            public,
            symbols,
            output,
            provider_request=request,
            **kwargs,
        )


def run_trusted_broker_plugin(
    inventory, plan, policy, public, symbols, **kwargs
):
    request = runtime_request(plan, public, family="security")
    provider = kwargs.pop("host_secret_provider", None)
    kwargs.setdefault("private_identifiers", (PRIVATE_CANARY,))
    with security_permission_scope(request, provider):
        return _trusted(
            inventory,
            plan,
            policy,
            public,
            symbols,
            provider_request=request,
            **kwargs,
        )


def replay_broker_lifecycle(output):
    request = _REQUESTS[output]
    with security_permission_scope(request):
        yield from _replay(output, provider_request=request)


class Provider:
    def __init__(self, value: str) -> None:
        self.value = value
        self.calls = 0

    def resolve(self, handle: str) -> str:
        assert handle == "opaque-fixture-handle"
        self.calls += 1
        return self.value


def main() -> None:
    assert histdatacom.__file__ is not None
    assert "site-packages" in histdatacom.__file__, histdatacom.__file__
    assert policy_api.__file__ is not None
    assert "site-packages" in policy_api.__file__, policy_api.__file__
    assert security.__file__ is not None
    assert "site-packages" in security.__file__, security.__file__
    wheel = Path(sys.argv[1]).resolve(strict=True)
    baseline = discover_broker_plugins().to_json()
    assert "org.example.security" not in baseline
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
            plugin_id="org.example.security",
        )
        policy = BrokerSecurityPolicyV1(
            plan.candidate.artifact_id,
            BrokerTrustTier.DEVELOPMENT,
            BrokerSecurityMode.KERNEL_ISOLATED,
        )
        with tempfile.TemporaryDirectory(
            prefix="security-qualification-"
        ) as directory:
            root = Path(directory)
            provider = Provider(secrets.token_urlsafe(24))
            for mode in (
                "finite",
                "output",
                "exception",
                "malformed",
                "block_next",
                "death",
            ):
                result = run_secure_broker_plugin(
                    inventory,
                    plan,
                    policy,
                    {"mode": mode},
                    ("EURUSD",),
                    root / mode,
                    authorize=lambda _: True,
                    host_secret_provider=provider,
                    lifecycle_policy=BrokerLifecyclePolicyV1(
                        startup_timeout_ms=15000,
                        run_timeout_ms=15000 if mode == "block_next" else 30000,
                        acknowledgement_timeout_ms=5000,
                        shutdown_timeout_ms=100,
                    ),
                )
                assert result.native.manifest.worker_reaped
                assert (
                    result.native.manifest.completion
                    is BrokerLifecycleCompletion.COMPLETE
                ) is (mode in ("finite", "output"))
                verify_security_capture(
                    read_security_receipt(result.receipt_path),
                    result.native.manifest,
                )
                records = tuple(
                    replay_broker_lifecycle(result.native.directory)
                )
                assert len(records) == result.native.manifest.appended_records
                retained = (
                    b"".join(
                        path.read_bytes()
                        for path in result.native.directory.iterdir()
                    )
                    + result.receipt_path.read_bytes()
                )
                assert provider.value.encode() not in retained
                results.append(
                    {
                        "mode": mode,
                        "reason": result.native.reason.value,
                        "events": result.native.manifest.appended_events,
                    }
                )
                if mode == "finite":
                    native_events = tuple(
                        BrokerAdmittedEventV1.from_json(
                            record.payload_json
                        ).event.to_json()
                        for record in records
                        if record.kind == "event"
                    )
            for mode in ("plain", "url", "base64", "json", "sha256", "session"):
                try:
                    run_secure_broker_plugin(
                        inventory,
                        plan,
                        policy,
                        {"mode": mode},
                        ("EURUSD",),
                        root / mode,
                        authorize=lambda _: True,
                        host_secret_provider=provider,
                    )
                except BrokerSecurityError:
                    results.append({"mode": mode, "refused": True})
                else:
                    raise AssertionError("private output accepted")
            listener = socket.socket()
            origin = urlsplit(
                read_installed_broker_permissions(plan.candidate)
                .endpoints[0]
                .origin
            )
            listener.bind(("127.0.0.1", origin.port))
            listener.listen(1)
            listener.settimeout(30)
            observed: list[bool] = []

            def serve() -> None:
                with listener.accept()[0] as client:
                    client.settimeout(5)
                    request = b""
                    while b"\r\n\r\n" not in request:
                        chunk = client.recv(4096)
                        assert chunk and len(request) + len(chunk) <= 8192
                        request += chunk
                    observed.append(
                        (b"Authorization: Bearer " + provider.value.encode())
                        in request
                    )
                    client.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\naccepted"
                    )

            thread = threading.Thread(target=serve)
            thread.start()
            try:
                result = run_secure_broker_plugin(
                    inventory,
                    plan,
                    policy,
                    {"mode": "authenticate"},
                    ("EURUSD",),
                    root / "auth",
                    authorize=lambda _: True,
                    host_secret_provider=provider,
                )
                assert (
                    result.native.manifest.completion
                    is BrokerLifecycleCompletion.COMPLETE
                )
            finally:
                thread.join(31)
                listener.close()
            assert observed == [True]
            assert "security_fixture.plugin" not in sys.modules
            trusted = run_trusted_broker_plugin(
                inventory,
                plan,
                replace(
                    policy,
                    trust=BrokerTrustTier.REVIEWED,
                    mode=BrokerSecurityMode.TRUSTED_IN_PROCESS,
                ),
                {"mode": "finite"},
                ("EURUSD",),
                authorize=lambda _: True,
                host_secret_provider=provider,
            )
            assert (
                tuple(
                    BrokerAdmittedEventV1.from_json(text).event.to_json()
                    for text in trusted.receipt.events_json
                )
                == native_events
            )
            assert provider.calls == 1
            assert provider.value.encode() not in b"".join(
                path.read_bytes() for path in root.rglob("*") if path.is_file()
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
                    "histdatacom-security-fixture",
                ],
                check=True,
            )
        # Only this fixture was deliberately imported by the trusted parity run.
        for name in ("security_fixture.plugin", "security_fixture"):
            sys.modules.pop(name, None)
    assert discover_broker_plugins().to_json() == baseline
    print(
        json.dumps(
            {
                "host": security.__file__,
                "python": sys.version.split()[0],
                "runs": results,
                "positive_auth": True,
                "native_parity": True,
                "restored_inventory": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

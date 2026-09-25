"""Offline security policy, explicit credentials and actual installed workers."""

from __future__ import annotations

from dataclasses import replace
from importlib import metadata
from pathlib import Path
import runpy
import secrets
import socket
import subprocess
import sys
import threading
import traceback
import venv
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    BrokerAdmittedEventV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion as Completion,
    BrokerLifecyclePolicyV1,
    replay_broker_lifecycle as _native_replay,
    inspect_broker_lifecycle,
)
from histdatacom.broker_plugin_security import (
    BrokerHostResources,
    BrokerNetworkMode as Network,
    BrokerPrivateMaterialGuard,
    BrokerSecurityError,
    BrokerSecurityMode as Mode,
    BrokerSecurityPolicyV1,
    BrokerSecurityReceiptV1,
    BrokerTrustTier as Trust,
    run_secure_broker_plugin as _native_secure,
    run_trusted_broker_plugin as _native_trusted,
    BrokerTrustedSecurityReceiptV1,
    verify_security_capture,
)
from histdatacom.broker_plugin_security import isolation
from histdatacom.broker_plugin_security.secrets import resolve_configuration
from histdatacom.broker_plugin_policy.scope import BrokerPolicyError
from tests.fixtures.broker_runtime_policy import runtime_request, runtime_scope

_PROVIDER_REQUESTS = {}


def run_secure_broker_plugin(
    inventory, plan, policy, public, symbols, output, **kwargs
):
    request = kwargs.pop("provider_request", None) or runtime_request(
        plan, public, family="security"
    )
    _PROVIDER_REQUESTS[output] = request
    kwargs.setdefault(
        "lifecycle_policy",
        BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            acknowledgement_timeout_ms=5000,
        ),
    )
    with runtime_scope(request):
        return _native_secure(
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
    with runtime_scope(request):
        return _native_trusted(
            inventory,
            plan,
            policy,
            public,
            symbols,
            provider_request=request,
            **kwargs,
        )


def replay_broker_lifecycle(directory):
    request = _PROVIDER_REQUESTS[directory]
    with runtime_scope(request):
        yield from _native_replay(directory, provider_request=request)


ROOT = Path(__file__).resolve().parents[2]
BUILD = runpy.run_path(str(ROOT / "tests/fixtures/broker_security_wheel.py"))[
    "build_security_wheel"
]
OPERATIONS = tuple(
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


@pytest.fixture(scope="module")
def installed(tmp_path_factory):
    if sys.platform != "darwin":
        pytest.skip("qualified kernel backend is macOS only")
    root = tmp_path_factory.mktemp("security-worker")
    env = root / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(env)
    python = env / "bin/python"
    site = Path(
        subprocess.check_output(
            [
                str(python),
                "-c",
                "import sysconfig;print(sysconfig.get_path('purelib'))",
            ],
            text=True,
        ).strip()
    )
    with ZipFile(BUILD(root / "wheels")) as archive:
        archive.extractall(site)
    return python, site


@pytest.fixture
def request_data(installed, monkeypatch):
    python, site = installed
    distribution = next(metadata.distributions(path=[str(site)]))
    monkeypatch.setattr(
        metadata, "distributions", lambda: iter((distribution,))
    )
    inventory = discover_broker_plugins()
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(OPERATIONS),
        plugin_id="org.example.security",
    )
    policy = BrokerSecurityPolicyV1(
        plan.candidate.artifact_id,
        Trust.DEVELOPMENT,
        Mode.KERNEL_ISOLATED,
        secret_fields=("credential",),
    )
    return python, inventory, plan, policy


class Provider:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def resolve(self, handle):
        assert handle == "opaque-fixture-handle"
        self.calls += 1
        return self.value


def run(
    request_data,
    tmp_path,
    mode="finite",
    *,
    value=None,
    policy=None,
    public=None,
    **kwargs,
):
    python, inventory, plan, default = request_data
    provider = Provider(value or secrets.token_urlsafe(24))
    result = run_secure_broker_plugin(
        inventory,
        plan,
        policy or default,
        public or {"mode": mode},
        ("EURUSD",),
        tmp_path / "run",
        authorize=lambda _: True,
        secret_handles={"credential": "opaque-fixture-handle"},
        secret_provider=provider,
        worker_python=str(python),
        **kwargs,
    )
    assert provider.calls == 1
    return result


def test_policy_strict_roundtrip_and_mode_refusals():
    policy = BrokerSecurityPolicyV1(
        "broker-plugin-candidate:sha256:" + "a" * 64,
        Trust.DEVELOPMENT,
        Mode.KERNEL_ISOLATED,
    )
    assert BrokerSecurityPolicyV1.from_json(policy.to_json()) == policy
    for changes in (
        {"mode": Mode.TRUSTED_IN_PROCESS},
        {"network": Network.PROVIDER_OWNED},
        {"loopback_ports": (True,)},
        {"tls_owner": "host_verified"},
    ):
        with pytest.raises(BrokerSecurityError):
            replace(policy, **changes)
    with pytest.raises(BrokerSecurityError):
        BrokerSecurityPolicyV1.from_dict({**policy.to_dict(), "unknown": True})


def test_resource_publication_is_always_denied_and_cache_is_bounded():
    resources = BrokerHostResources(cache_bytes=4)
    resources.put_cache("public", b"1234")
    assert resources.get_cache("public") == b"1234"
    with pytest.raises(BrokerSecurityError):
        resources.put_cache("extra", b"x")
    with pytest.raises(
        BrokerSecurityError, match="plugin_publication_forbidden"
    ):
        resources.publish_scientific_product(object())
    with pytest.raises(BrokerSecurityError):
        resources.request("write_product")


def test_known_private_material_guard_covers_encoded_forms():
    import base64
    import json
    from urllib.parse import quote

    value = 'synthetic / private " \u2603 ' + secrets.token_hex(8)
    guard = BrokerPrivateMaterialGuard((value,))
    for form in (
        value,
        json.dumps(value),
        json.dumps(json.dumps(value)),
        quote(value, safe=""),
        base64.b64encode(value.encode()).decode(),
        base64.urlsafe_b64encode(value.encode()).decode().rstrip("="),
    ):
        with pytest.raises(
            BrokerSecurityError, match="private_material_refused"
        ):
            guard.check(form)
    assert value not in repr(guard)


def test_finite_kernel_capture_replays_with_security_binding(
    request_data, tmp_path, monkeypatch
):
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKSecurityV1
    from histdatacom.broker_plugin_policy.storage import (
        read_broker_policy_receipt,
        verify_broker_policy_receipt,
    )

    value = secrets.token_urlsafe(24)
    monkeypatch.setenv("BROKER_SECURITY_AMBIENT_CANARY", value)
    result = run(request_data, tmp_path, value=value)
    assert (
        result.native.manifest.completion is Completion.COMPLETE
    ), result.native.reason
    assert result.native.manifest.appended_events == 2
    verify_security_capture(result.security, result.native.manifest)
    assert (
        BrokerSecurityReceiptV1.from_json(result.security.to_json())
        == result.security
    )
    assert tuple(replay_broker_lifecycle(result.native.directory))
    sidecar = result.receipt_path.with_name(
        result.receipt_path.name + ".provider-policy.json"
    )
    verify_broker_policy_receipt(
        read_broker_policy_receipt(sidecar),
        BrokerSDKSecurityV1(
            _PROVIDER_REQUESTS[result.native.directory],
            result.security,
            result.native.manifest,
        ),
        result.receipt_path,
    )
    retained = (
        b"".join(
            path.read_bytes() for path in result.native.directory.iterdir()
        )
        + result.security.to_json().encode()
        + sidecar.read_bytes()
    )
    assert value.encode() not in retained
    assert "security_fixture.plugin" not in sys.modules
    for changes in (
        {"registration_sha256": "f" * 64},
        {"implementation_sha256": "f" * 64},
        {"distribution_name": "different-package"},
        {"distribution_version": "99.0.0"},
        {"sdk_version": "99.0.0"},
    ):
        # Recompute all outer identities: byte hashing alone is insufficient.
        forged = replace(
            result.security,
            software=replace(result.security.software, **changes),
        )
        assert BrokerSecurityReceiptV1.from_json(forged.to_json()) == forged
        assert forged.artifact_id != result.security.artifact_id
        with pytest.raises(
            BrokerSecurityError, match="security_integrity_failure"
        ):
            verify_security_capture(forged, result.native.manifest)


def test_positive_explicit_secret_delivery_to_declared_loopback(
    request_data, tmp_path
):
    value = secrets.token_urlsafe(24)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(30)
    observed = []

    def serve():
        with listener.accept()[0] as client:
            observed.append(client.recv(256).decode().strip())
            client.sendall(b"accepted" if observed[-1] == value else b"denied")

    worker = threading.Thread(target=serve)
    worker.start()
    port = listener.getsockname()[1]
    try:
        result = run(
            request_data,
            tmp_path,
            value=value,
            policy=replace(
                request_data[3],
                network=Network.LOOPBACK,
                loopback_ports=(port,),
            ),
            public={"mode": "authenticate", "port": port},
        )
    finally:
        worker.join(31)
        listener.close()
    assert observed == [value]
    assert result.native.manifest.completion is Completion.COMPLETE
    assert not result.security.provider_tls_verified_by_host


@pytest.mark.parametrize("mode", ["plain", "url", "base64", "json", "session"])
def test_known_secret_output_is_refused_not_rewritten(
    request_data, tmp_path, mode
):
    value = 'synthetic / private " \u2603 ' + secrets.token_hex(8)
    with pytest.raises(
        BrokerSecurityError,
        match="private_material_refused|security_integrity_failure",
    ):
        run(request_data, tmp_path, mode, value=value)
    if (tmp_path / "run").exists():
        retained = b"".join(
            path.read_bytes() for path in (tmp_path / "run").iterdir()
        )
        retained += b"".join(
            path.read_bytes()
            for path in tmp_path.glob("*.provider-policy.json")
        )
        assert value.encode() not in retained


def test_unsupported_backend_precedes_resolver_and_output(
    request_data, tmp_path, monkeypatch
):
    provider = Provider(secrets.token_urlsafe(24))
    monkeypatch.setattr(isolation.sys, "platform", "win32")
    with pytest.raises(
        BrokerSecurityError, match="security_backend_unsupported"
    ):
        run_secure_broker_plugin(
            request_data[1],
            request_data[2],
            request_data[3],
            {"mode": "finite"},
            (),
            tmp_path / "run",
            authorize=lambda _: pytest.fail("authorization must not run"),
            secret_handles={"credential": "opaque-fixture-handle"},
            secret_provider=provider,
        )
    assert provider.calls == 0 and not (tmp_path / "run").exists()


@pytest.mark.parametrize(
    "mode",
    [
        "exception",
        "output_storm",
        "block_open",
        "block_next",
        "block_close",
        "malformed",
        "death",
    ],
)
def test_isolated_failure_modes_remain_bounded_and_incomplete(
    request_data, tmp_path, mode
):
    result = run(
        request_data,
        tmp_path,
        mode,
        lifecycle_policy=BrokerLifecyclePolicyV1(
            startup_timeout_ms=8000,
            run_timeout_ms=15000,
            shutdown_timeout_ms=100,
            stdout_bytes=256,
        ),
    )
    assert result.native.manifest.completion is Completion.PARTIAL
    assert result.native.manifest.worker_reaped
    records = tuple(replay_broker_lifecycle(result.native.directory))
    assert any(record.kind == "identity" for record in records)
    if mode in ("block_next", "block_close", "malformed"):
        assert any(record.kind == "session" for record in records)


def test_native_event_bytes_match_trusted_and_isolated_paths(
    request_data, installed, tmp_path, monkeypatch
):
    isolated = run(request_data, tmp_path)
    native_events = tuple(
        BrokerAdmittedEventV1.from_json(record.payload_json).event.to_json()
        for record in replay_broker_lifecycle(isolated.native.directory)
        if record.kind == "event"
    )
    monkeypatch.syspath_prepend(str(installed[1]))
    policy = replace(
        request_data[3], trust=Trust.FIRST_PARTY, mode=Mode.TRUSTED_IN_PROCESS
    )
    try:
        trusted = run_trusted_broker_plugin(
            request_data[1],
            request_data[2],
            policy,
            {"mode": "finite"},
            ("EURUSD",),
            authorize=lambda _: True,
            secret_handles={"credential": "opaque-fixture-handle"},
            secret_provider=Provider(secrets.token_urlsafe(24)),
        )
        assert (
            tuple(
                BrokerAdmittedEventV1.from_json(text).event.to_json()
                for text in trusted.events_json
            )
            == native_events
        )
        assert (
            BrokerTrustedSecurityReceiptV1.from_json(trusted.to_json())
            == trusted
        )
        metadata = __import__(
            "histdatacom.broker_plugin_capabilities",
            fromlist=["BrokerAdmittedMetadataV1"],
        ).BrokerAdmittedMetadataV1.from_json(trusted.metadata_json)
        forged = replace(
            metadata, plan_id="broker-capability-plan:sha256:" + "f" * 64
        )
        with pytest.raises(BrokerSecurityError):
            replace(trusted, metadata_json=forged.to_json())
        with pytest.raises(BrokerSecurityError):
            replace(
                trusted, public_configuration_json='{"credential":"private"}'
            )
        import hashlib
        from histdatacom.broker_plugin_security import canonical_security_json

        payload = trusted.to_dict()
        payload["public_configuration_json"] = '{"credential":"private"}'
        payload.pop("artifact_id")
        payload["artifact_id"] = (
            "broker-security-trusted-receipt:sha256:"
            + hashlib.sha256(
                canonical_security_json(payload).encode("ascii")
            ).hexdigest()
        )
        with pytest.raises(BrokerSecurityError):
            BrokerTrustedSecurityReceiptV1.from_dict(payload)
    finally:
        for name in ("security_fixture.plugin", "security_fixture"):
            sys.modules.pop(name, None)


def test_changing_credential_does_not_change_scientific_identity(
    request_data, tmp_path
):
    first_root = tmp_path / "first"
    first_root.mkdir()
    second_root = tmp_path / "second"
    second_root.mkdir()
    first = run(
        request_data,
        first_root,
        value=secrets.token_urlsafe(20),
        run_nonce="e" * 32,
        clock=lambda: (500, 500),
    )
    second = run(
        request_data,
        second_root,
        value=secrets.token_urlsafe(24),
        run_nonce="e" * 32,
        clock=lambda: (500, 500),
    )
    assert first.native.manifest.to_json() == second.native.manifest.to_json()
    assert first.security.to_json() == second.security.to_json()


def test_resolver_traceback_does_not_echo_private_exception():
    value = secrets.token_urlsafe(24)

    class BrokenProvider:
        def resolve(self, handle):
            raise RuntimeError(value)

    with pytest.raises(BrokerSecurityError) as caught:
        resolve_configuration(
            {},
            {"credential": "opaque-fixture-handle"},
            ("credential",),
            BrokenProvider(),
            (),
        )
    assert value not in "".join(traceback.format_exception(caught.value))
    assert value not in repr(caught.value)


def test_trusted_plugin_traceback_does_not_echo_private_exception(
    request_data, installed, monkeypatch
):
    value = secrets.token_urlsafe(24)
    monkeypatch.syspath_prepend(str(installed[1]))
    policy = replace(
        request_data[3], trust=Trust.REVIEWED, mode=Mode.TRUSTED_IN_PROCESS
    )
    try:
        with pytest.raises(BrokerSecurityError) as caught:
            run_trusted_broker_plugin(
                request_data[1],
                request_data[2],
                policy,
                {"mode": "exception"},
                ("EURUSD",),
                authorize=lambda _: True,
                secret_handles={"credential": "opaque-fixture-handle"},
                secret_provider=Provider(value),
            )
        assert value not in "".join(traceback.format_exception(caught.value))
    finally:
        for name in ("security_fixture.plugin", "security_fixture"):
            sys.modules.pop(name, None)


def test_full_inventory_private_identifier_refused_before_capture(
    request_data, tmp_path
):
    from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1

    account = "private-account-" + secrets.token_hex(8)
    candidate = request_data[2].candidate
    registration = replace(
        candidate.registration,
        plugin_id="org.example.unselected",
        display_name=account,
    )
    extra = replace(candidate, registration=registration)
    inventory = BrokerPluginInventoryV1((candidate, extra))
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(OPERATIONS),
        plugin_id="org.example.security",
    )
    with pytest.raises(BrokerSecurityError, match="private_material_refused"):
        run_secure_broker_plugin(
            inventory,
            plan,
            request_data[3],
            {"mode": "finite"},
            ("EURUSD",),
            tmp_path / "run",
            authorize=lambda _: True,
            secret_handles={"credential": "opaque-fixture-handle"},
            secret_provider=Provider(secrets.token_urlsafe(24)),
            private_identifiers=(account,),
            worker_python=str(request_data[0]),
        )
    assert not (tmp_path / "run").exists()


def test_raw_worker_output_is_discarded_and_secret_not_in_launch(
    request_data, tmp_path, monkeypatch, capfd
):
    from histdatacom.broker_plugin_lifecycle import supervisor

    value = secrets.token_urlsafe(24)
    original = supervisor.subprocess.Popen
    launches = []

    def capture(*args, **kwargs):
        launches.append((repr(args), repr(kwargs.get("env"))))
        return original(*args, **kwargs)

    monkeypatch.setattr(supervisor.subprocess, "Popen", capture)
    result = run(request_data, tmp_path, "output", value=value)
    assert result.native.manifest.completion is Completion.COMPLETE
    assert value not in repr(launches)
    assert value not in "".join(capfd.readouterr())
    assert value not in result.security.to_json()


def test_explicit_cancellation_remains_partial(request_data, tmp_path):
    with pytest.raises(BrokerSecurityError, match="security_integrity_failure"):
        run(request_data, tmp_path, cancellation=lambda: True)
    manifest = inspect_broker_lifecycle(tmp_path / "run").manifest
    assert manifest.completion is Completion.PARTIAL
    assert manifest.worker_reaped
    assert not (tmp_path / "run-security.json").exists()


@pytest.mark.parametrize(
    "variant",
    ["blocked_import", "non_secret_schema", "factory", "metadata", "schema"],
)
def test_startup_bound_and_schema_secret_refusal(
    installed, tmp_path, monkeypatch, variant
):
    # Separate installed distribution, never mutate the shared fixture wheel.
    site = tmp_path / "site"
    site.mkdir()
    with ZipFile(
        BUILD(
            tmp_path / "wheels",
            block_import=variant == "blocked_import",
            secret_schema=variant != "non_secret_schema",
            fail_before_schema=variant,
        )
    ) as archive:
        archive.extractall(site)
    distribution = next(metadata.distributions(path=[str(site)]))
    monkeypatch.setattr(
        metadata, "distributions", lambda: iter((distribution,))
    )
    # A venv's own site-packages is the worker metadata authority.
    env = tmp_path / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(env)
    python = env / "bin/python"
    worker_site = Path(
        subprocess.check_output(
            [
                str(python),
                "-c",
                "import sysconfig;print(sysconfig.get_path('purelib'))",
            ],
            text=True,
        ).strip()
    )
    with ZipFile(next((tmp_path / "wheels").glob("*.whl"))) as archive:
        archive.extractall(worker_site)
    inventory = discover_broker_plugins()
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(OPERATIONS),
        plugin_id="org.example.security",
    )
    policy = BrokerSecurityPolicyV1(
        plan.candidate.artifact_id,
        Trust.DEVELOPMENT,
        Mode.KERNEL_ISOLATED,
        secret_fields=("credential",),
    )
    args = (python, inventory, plan, policy)
    value = secrets.token_urlsafe(24)
    with pytest.raises(BrokerSecurityError, match="security_integrity_failure"):
        # The reviewed public/private projection now precedes invocation.
        # Supply it correctly so this test still reaches each hostile runtime
        # schema/factory/metadata path rather than an earlier config refusal.
        run(
            args,
            tmp_path,
            value=value,
            lifecycle_policy=BrokerLifecyclePolicyV1(
                startup_timeout_ms=(
                    250 if variant == "blocked_import" else 15000
                ),
                run_timeout_ms=30000,
                shutdown_timeout_ms=100,
            ),
        )
    manifest = inspect_broker_lifecycle(tmp_path / "run").manifest
    assert manifest.completion is Completion.PARTIAL
    assert manifest.worker_reaped
    assert not (tmp_path / "run-security.json").exists()
    assert value.encode() not in b"".join(
        path.read_bytes() for path in (tmp_path / "run").iterdir()
    )


def test_public_secret_with_bad_value_refused_before_schema_validation(
    request_data, tmp_path
):
    value = secrets.token_urlsafe(24)
    request = runtime_request(
        request_data[2], {"mode": "finite"}, family="security"
    )
    with pytest.raises(
        BrokerPolicyError, match="reviewed_security_configuration_mismatch"
    ):
        run_secure_broker_plugin(
            request_data[1],
            request_data[2],
            replace(request_data[3], secret_fields=()),
            {"mode": "finite", "credential": value, "port": "not-an-int"},
            ("EURUSD",),
            tmp_path / "run",
            authorize=lambda _: True,
            worker_python=str(request_data[0]),
            provider_request=request,
        )
    assert not (tmp_path / "run-security.json").exists()
    assert not (tmp_path / "run").exists()


def test_parent_requires_each_epoch_schema_and_rechecks_public_fields(
    request_data, tmp_path, monkeypatch
):
    from histdatacom.broker_plugin_security import execution
    from histdatacom.broker_plugin_lifecycle import (
        BrokerLifecycleTransitionV1,
        BrokerLifecycleState as State,
        BrokerLifecycleReason as Reason,
    )

    result = run(request_data, tmp_path)
    configuration = {"mode": "finite", "credential": "synthetic-private"}
    provider_request = _PROVIDER_REQUESTS[result.native.directory]
    with runtime_scope(provider_request):
        with pytest.raises(
            BrokerSecurityError, match="security_integrity_failure"
        ):
            execution._verify_configuration_classification(
                result.native,
                configuration,
                configuration,
                result.security.policy,
                provider_request,
            )
    records = tuple(replay_broker_lifecycle(result.native.directory))
    # Isolate the epoch-coverage check: authoritative replay validation itself
    # is covered by lifecycle tests, and is never bypassed in production.
    starting = next(item for item in records if item.kind == "transition")
    extra = replace(
        starting,
        epoch=1,
        payload_json=BrokerLifecycleTransitionV1(
            State.RECONNECTING, State.STARTING, Reason.STARTING, 1
        ).to_json(),
    )
    monkeypatch.setattr(
        execution,
        "replay_broker_lifecycle",
        lambda _, **kwargs: iter((*records, extra)),
    )
    with pytest.raises(BrokerSecurityError, match="security_integrity_failure"):
        execution._verify_configuration_classification(
            result.native,
            configuration,
            {"mode": "finite"},
            result.security.policy,
            provider_request,
        )

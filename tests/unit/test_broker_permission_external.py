"""Synthetic independent resource probes for separately admitted plugins."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib import metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import venv
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_security import BrokerSecurityError
from histdatacom.broker_plugin_security import isolation
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionBindingV1,
    BrokerPermissionCacheV1,
    BrokerPermissionContextV1,
    BrokerPermissionEndpointV1,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
    BrokerPermissionRevocationV1,
)
from histdatacom.broker_plugin_permissions.decisions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionError,
)
from histdatacom.broker_plugin_permissions.resources import (
    BrokerHostSecretProfileV1,
    BrokerPermissionResourcesV1,
)
from histdatacom.broker_plugin_capabilities import negotiate_broker_capabilities
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
    discover_broker_plugins,
)
from histdatacom.broker_plugin_permissions import (
    permission_resource_path,
    read_installed_broker_permissions,
)
from histdatacom.broker_plugin_policy.bindings import sdk_invocation_binding
from histdatacom.broker_plugin_policy.contracts import BrokerPolicyRevocationV1
from histdatacom.broker_plugin_policy.scope import (
    BrokerPolicyError,
    provider_policy_scope,
)
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    policy_context,
    sdk_policy_invocation,
)
from histdatacom.broker_plugin_lifecycle.ipc import FrameDecoder
from histdatacom.broker_plugin_permissions.dispatch import (
    dispatch_worker_permission,
)
from histdatacom.broker_plugin_permissions.scope import broker_permission_scope
from histdatacom.broker_plugin_permissions.worker import (
    PermissionChannel,
    WorkerHostResources,
)
from tests.fixtures.broker_permission_wheel import build_permission_wheel

ROOT = Path(__file__).resolve().parents[2]
requires_kernel = pytest.mark.skipif(
    sys.platform != "darwin", reason="qualified kernel backend is macOS only"
)


def _launch(tmp_path, *, python=None, ports=()):
    cwd = tmp_path / "worker-cwd"
    cwd.mkdir()
    store = tmp_path / "scientific-store"
    store.mkdir()
    return isolation.prepare_kernel_launch(
        str(python or sys.executable), ROOT / "src", cwd, (store,), ports
    )


def _run(launch, program, *args, python=None, flags=("-I", "-S", "-B")):
    return subprocess.run(
        [
            str(python or sys.executable),
            *flags,
            "-c",
            launch.bootstrap_source + "\n" + program,
            *map(str, args),
        ],
        cwd=launch.working_directory,
        env=launch.environment,
        capture_output=True,
        text=True,
        timeout=10,
    )


@requires_kernel
def test_initial_worker_runs_but_plugin_fork_spawn_and_same_python_exec_do_not(
    tmp_path,
):
    launch = _launch(tmp_path)
    result = _run(
        launch,
        """
import json,os,sys
checks={}
for name,operation in (
    ('fork',lambda:os.fork()),
    ('spawn_python',lambda:os.posix_spawn(sys.executable,[sys.executable,'-I','-S','-c','pass'],{'LANG':'C'})),
    ('spawn_shell',lambda:os.posix_spawn('/bin/echo',['echo','generated-child'],{'LANG':'C'})),
    ('exec_same_python',lambda:os.execve(sys.executable,[sys.executable,'-I','-S','-c','raise SystemExit(82)'],{'LANG':'C'})),
):
    try:
        value=operation()
    except PermissionError as error:
        checks[name]=error.errno==1
    else:
        if name=='fork' and value==0:os._exit(81)
        if name in ('fork','spawn_python','spawn_shell'):os.waitpid(value,0)
        checks[name]=False
print(json.dumps(checks,sort_keys=True))
""",
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "fork": True,
        "spawn_python": True,
        "spawn_shell": True,
        "exec_same_python": True,
    }
    assert launch.command_prefix == ()


@requires_kernel
def test_sealed_worker_keeps_resource_denials_and_exact_loopback_positive(
    tmp_path,
):
    with socket.socket() as allowed, socket.socket() as other:
        allowed.bind(("127.0.0.1", 0))
        allowed.listen(1)
        allowed.settimeout(1)
        other.bind(("127.0.0.1", 0))
        other.listen(1)
        other.settimeout(0.05)
        port = allowed.getsockname()[1]
        other_port = other.getsockname()[1]
        launch = _launch(tmp_path, ports=(port,))
        store = tmp_path / "scientific-store"
        original = store / "original"
        original.write_bytes(b"generated-original-only")
        alias = tmp_path / "alias"
        alias.symlink_to(store, target_is_directory=True)
        result = _run(
            launch,
            """
import json,os,socket,sys
from pathlib import Path
root=Path(sys.argv[1]); checks={}
for name,operation in (
    ('read',lambda:(root/'scientific-store/original').read_bytes()),
    ('write',lambda:(root/'scientific-store/new').write_bytes(b'generated')),
    ('truncate',lambda:os.truncate(root/'scientific-store/original',0)),
    ('rename',lambda:os.rename(root/'scientific-store/original',root/'renamed')),
    ('link',lambda:os.link(root/'scientific-store/original',root/'linked')),
    ('symlink',lambda:os.symlink(root/'scientific-store/original',root/'new-alias')),
    ('alias',lambda:(root/'alias/original').write_bytes(b'generated')),
    ('unlink',lambda:(root/'scientific-store/original').unlink()),
    ('network_other',lambda:socket.create_connection(('127.0.0.1',int(sys.argv[3])),.1)),
):
    try:operation()
    except PermissionError as error:checks[name]=error.errno==1
    else:checks[name]=False
with socket.create_connection(('127.0.0.1',int(sys.argv[2])),.1) as connection:
    connection.sendall(b'generated-loopback-only')
    checks['network_granted']=True
checks['stdlib_read']=bool(Path(os.__file__).read_bytes())
checks['host_import']=__import__('histdatacom.broker_plugins').__name__=='histdatacom'
print(json.dumps(checks,sort_keys=True))
""",
            tmp_path,
            port,
            other_port,
        )
        assert result.returncode == 0, result.stderr
        checks = json.loads(result.stdout)
        assert set(checks) == {
            "read",
            "write",
            "truncate",
            "rename",
            "link",
            "symlink",
            "alias",
            "unlink",
            "network_other",
            "network_granted",
            "stdlib_read",
            "host_import",
        }
        assert all(checks.values()), checks
        with allowed.accept()[0] as connection:
            assert connection.recv(64) == b"generated-loopback-only"
        with pytest.raises(TimeoutError):
            other.accept()
        assert original.read_bytes() == b"generated-original-only"
        assert list(store.iterdir()) == [original]


@requires_kernel
def test_worker_site_is_explicit_without_pth_or_sitecustomize_execution(
    tmp_path,
):
    environment = tmp_path / "isolated-env"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    python = environment / "bin" / "python"
    site = (
        environment
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    site.mkdir(parents=True, exist_ok=True)
    pth_marker = tmp_path / "pth-executed"
    customize_marker = tmp_path / "customize-executed"
    (site / "generated-malicious.pth").write_text(
        f"import pathlib; pathlib.Path({str(pth_marker)!r}).write_text('bad')\n"
    )
    (site / "sitecustomize.py").write_text(
        f"import pathlib; pathlib.Path({str(customize_marker)!r}).write_text('bad')\n"
    )
    (site / "generated_permission_probe.py").write_text(
        "VALUE = 'generated import succeeded'\n"
    )
    launch = _launch(tmp_path, python=python)
    result = _run(
        launch,
        """
import json,sys
import generated_permission_probe
print(json.dumps({'value':generated_permission_probe.VALUE,'site_loaded':'site' in sys.modules,'customize_loaded':'sitecustomize' in sys.modules}))
""",
        python=python,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "value": "generated import succeeded",
        "site_loaded": False,
        "customize_loaded": False,
    }
    assert str(site.resolve()) in launch.python_search_paths
    assert not pth_marker.exists()
    assert not customize_marker.exists()


@requires_kernel
@pytest.mark.parametrize("flags", [("-I", "-B"), ("-I", "-S")])
def test_bootstrap_refuses_missing_required_interpreter_flags(tmp_path, flags):
    launch = _launch(tmp_path)
    result = _run(launch, "print('plugin must not run')", flags=flags)
    assert result.returncode == 78
    assert "plugin must not run" not in result.stdout


@requires_kernel
def test_kernel_qualification_refuses_an_unsealed_bootstrap(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(isolation, "_sealed_bootstrap", lambda *_: "")
    with pytest.raises(
        BrokerSecurityError, match="security_enforcement_unavailable"
    ):
        _launch(tmp_path)


class _GrantSource:
    def __init__(self, context):
        self.context = context
        self.reads = 0

    def read_context(self):
        self.reads += 1
        return self.context


def _native_request():
    original = sdk_policy_invocation()
    registration = replace(
        original.plan.candidate.registration, provider_ids=("fixture",)
    )
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration.to_json().encode()).hexdigest(),
        original.plan.candidate.implementation_sha256,
    )
    plan = negotiate_broker_capabilities(
        BrokerPluginInventoryV1((candidate,)),
        original.plan.workflow,
        plugin_id=registration.plugin_id,
    )
    return replace(
        original,
        plan=plan,
        configuration_profile=replace(
            original.configuration_profile, provider_id="fixture"
        ),
    )


@pytest.fixture
def generated_provider_rights():
    request = _native_request()
    source = MutablePolicySource(
        policy_context(sdk_invocation_binding(request))
    )
    with provider_policy_scope(source):
        yield request, source


def _authority(*, port=1, granted=None, timeout_ms=1000, maximum=64):
    request = _native_request()
    candidate = request.plan.candidate
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        candidate.registration.distribution_name,
        candidate.registration.distribution_version,
        "1.0.0",
        ("fixture",),
        ("emit:quotes",),
        tuple(
            sorted(
                (
                    "cache:plugin:prices",
                    "emit:health",
                    "emit:sizes",
                    "network:provider:fixture",
                    "raw_payload:emit",
                    "secrets:read:paper",
                    "subprocess:requested",
                )
            )
        ),
        endpoints=(
            BrokerPermissionEndpointV1(
                "quotes",
                "fixture",
                f"http://127.0.0.1:{port}",
                "/v1/",
                ("GET", "POST"),
                16,
                maximum,
                timeout_ms,
                ("paper",),
            ),
        ),
        secret_profiles=("paper",),
        caches=(BrokerPermissionCacheV1("prices", 12, 2, 8),),
        subprocess_mode="isolated",
    )
    binding = BrokerPermissionBindingV1(
        manifest.candidate_id,
        manifest.artifact_id,
        "1.0.0",
        "fixture",
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        manifest.declared_atoms if granted is None else tuple(sorted(granted)),
        "generated-operator",
        10,
        100,
        "c" * 32,
    )
    source = _GrantSource(BrokerPermissionContextV1((grant,)))
    clock = [20]
    authority = BrokerPermissionAuthorityV1(
        manifest, binding, grant.artifact_id, source, lambda: clock[0]
    )
    return authority, source, grant, clock


def _revoke(source, grant):
    source.context = BrokerPermissionContextV1(
        (grant,),
        (
            BrokerPermissionRevocationV1(
                grant.artifact_id, 20, "generated-denial"
            ),
        ),
        1,
    )


@contextmanager
def _http_fixture(*, status=200, body=b"generated-response", delay=0):
    observed = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            incoming = self.rfile.read(
                int(self.headers.get("Content-Length", 0))
            )
            observed.append(
                (
                    self.command,
                    self.path,
                    self.headers.get("Authorization"),
                    incoming,
                )
            )
            if delay:
                time.sleep(delay)
            self.send_response(status)
            if 300 <= status < 400:
                self.send_header("Location", "/v1/redirected")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_POST = do_GET

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    try:
        yield server.server_port, observed
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()


class _SecretProvider:
    value = "generated-private-bearer-value"

    def __init__(self, callback=lambda: None):
        self.calls = []
        self.callback = callback

    def resolve(self, handle):
        self.calls.append(handle)
        self.callback()
        return self.value


def _resources(authority, provider=None):
    return BrokerPermissionResourcesV1(
        authority,
        provider_request=_native_request(),
        secret_profiles=(
            (BrokerHostSecretProfileV1("paper", "generated-opaque-handle"),)
            if provider is not None
            else ()
        ),
        secret_provider=provider,
    )


def test_actual_host_http_authentication_keeps_secret_out_of_public_response(
    generated_provider_rights,
):
    with _http_fixture() as (port, observed):
        authority, source, _, _ = _authority(port=port)
        provider = _SecretProvider()
        resources = _resources(authority, provider)
        result = resources.request(
            "quotes",
            "POST",
            "/v1/ticks",
            body=b"generated",
            secret_profile="paper",
        )
        assert result.status == 200
        assert result.body == b"generated-response"
        assert provider.calls == ["generated-opaque-handle"]
        assert observed == [
            ("POST", "/v1/ticks", "Bearer " + provider.value, b"generated")
        ]
        assert not hasattr(result, "headers")
        assert provider.value not in repr(result)
        assert source.reads >= 5
        with pytest.raises(BrokerSecurityError):
            resources.check_public_text(provider.value)


@pytest.mark.parametrize(
    "changes",
    [
        {"endpoint_id": "undeclared"},
        {"method": "DELETE"},
        {"path": "/v1-evil/ticks"},
        {"path": "/v1/../secret"},
        {"path": "/v1/%2e%2e/secret"},
        {"path": "//127.0.0.1/"},
        {"path": "/v1/ticks?unreviewed=1"},
        {"path": "/v1/ticks#fragment"},
        {"path": "/v1/\\secret"},
        {"path": "/v1/secret\r\nX-Test: x"},
        {"body": b"x" * 17},
        {"body": bytearray(b"bad")},
        {"secret_profile": "undeclared"},
    ],
)
def test_host_request_outside_declared_scope_never_reaches_transport(
    changes, generated_provider_rights
):
    with _http_fixture() as (port, observed):
        authority, _, _, _ = _authority(port=port)
        resources = _resources(authority)
        request = {
            "endpoint_id": "quotes",
            "method": "GET",
            "path": "/v1/ticks",
        }
        request.update(changes)
        with pytest.raises(ValueError):
            resources.request(**request)
        assert observed == []


@pytest.mark.parametrize(
    "atom", ["network:provider:fixture", "secrets:read:paper"]
)
def test_missing_optional_transport_or_secret_grant_refuses_without_io(
    atom, generated_provider_rights
):
    with _http_fixture() as (port, observed):
        authority, _, _, _ = _authority(port=port)
        granted = tuple(
            value
            for value in authority.manifest.declared_atoms
            if value != atom
        )
        authority, _, _, _ = _authority(port=port, granted=granted)
        provider = _SecretProvider()
        resources = _resources(authority, provider)
        with pytest.raises(
            BrokerPermissionError, match="resource_permission_denied"
        ):
            resources.request(
                "quotes", "GET", "/v1/ticks", secret_profile="paper"
            )
        assert observed == []
        assert provider.calls == []


def test_revocation_during_secret_callback_is_rechecked_before_network(
    generated_provider_rights,
):
    with _http_fixture() as (port, observed):
        authority, source, grant, _ = _authority(port=port)
        provider = _SecretProvider(lambda: _revoke(source, grant))
        resources = _resources(authority, provider)
        with pytest.raises(
            BrokerPermissionError, match="permission_grant_revoked"
        ):
            resources.request(
                "quotes", "GET", "/v1/ticks", secret_profile="paper"
            )
        assert provider.calls == ["generated-opaque-handle"]
        assert observed == []


@pytest.mark.parametrize(
    "server_options,maximum,timeout_ms",
    [
        ({"status": 302}, 64, 1000),
        ({"body": b"x" * 65}, 64, 1000),
        ({"delay": 0.3}, 64, 100),
        ({"body": _SecretProvider.value.encode()}, 64, 1000),
    ],
)
def test_actual_http_redirect_oversize_deadline_and_secret_echo_refuse(
    server_options, maximum, timeout_ms, generated_provider_rights
):
    with _http_fixture(**server_options) as (port, observed):
        authority, _, _, _ = _authority(
            port=port, maximum=maximum, timeout_ms=timeout_ms
        )
        resources = _resources(authority, _SecretProvider())
        started = time.monotonic()
        with pytest.raises(
            ValueError, match="broker host transport refused"
        ) as refusal:
            resources.request(
                "quotes", "GET", "/v1/ticks", secret_profile="paper"
            )
        assert time.monotonic() - started < 2
        assert _SecretProvider.value not in str(refusal.value)
        assert len(observed) <= 1
        assert all(record[1] == "/v1/ticks" for record in observed)


def test_cache_round_trip_overwrite_and_both_quota_dimensions(
    generated_provider_rights,
):
    authority, _, _, _ = _authority()
    resources = _resources(authority)
    assert resources.get_cache("prices", "missing") is None
    resources.put_cache("prices", "one", b"a" * 8)
    resources.put_cache("prices", "two", b"b" * 4)
    assert resources.get_cache("prices", "one") == b"a" * 8
    resources.put_cache("prices", "one", b"c" * 8)
    for key, value in (("one", b"x" * 9), ("two", b"x" * 5), ("three", b"")):
        with pytest.raises(ValueError):
            resources.put_cache("prices", key, value)
    assert resources.get_cache("prices", "one") == b"c" * 8
    assert resources.get_cache("prices", "two") == b"b" * 4
    assert resources.get_cache("prices", "three") is None
    assert _resources(authority).get_cache("prices", "one") is None


@pytest.mark.parametrize(
    "cache_id,key",
    [
        ("undeclared", "key"),
        ("prices", "../secret"),
        ("prices", "/data/products"),
        ("prices", "a/b"),
        ("prices", "a\\b"),
        ("prices", ""),
        ("prices", "a" * 129),
        ("prices", "key\x00"),
    ],
)
def test_cache_names_and_keys_cannot_be_filesystem_or_other_namespace_handles(
    cache_id, key, generated_provider_rights
):
    authority, _, _, _ = _authority()
    resources = _resources(authority)
    with pytest.raises(ValueError):
        resources.put_cache(cache_id, key, b"generated")
    with pytest.raises(ValueError):
        resources.get_cache(cache_id, key)


@pytest.mark.parametrize("deny", ["optional", "revoked", "expired"])
def test_cache_and_available_permissions_use_current_not_cached_grants(
    deny, generated_provider_rights
):
    authority, source, grant, clock = _authority()
    if deny == "optional":
        authority, source, grant, clock = _authority(granted=("emit:quotes",))
    resources = _resources(authority)
    if deny != "optional":
        resources.put_cache("prices", "one", b"ok")
        if deny == "revoked":
            _revoke(source, grant)
        else:
            clock[0] = 100
    for operation in (
        lambda: resources.get_cache("prices", "one"),
        lambda: resources.put_cache("prices", "one", b"bad"),
    ):
        with pytest.raises(BrokerPermissionError):
            operation()
    if deny == "optional":
        assert resources.available_permissions() == ("emit:quotes",)
    else:
        with pytest.raises(BrokerPermissionError):
            resources.available_permissions()


def test_granted_subprocess_is_still_unsupported_not_an_arbitrary_exec_escape(
    monkeypatch,
    generated_provider_rights,
):
    authority, _, _, _ = _authority()
    resources = _resources(authority)

    def unexpected(*args, **kwargs):
        raise AssertionError("unsupported subprocess backend was invoked")

    monkeypatch.setattr(subprocess, "run", unexpected)
    with pytest.raises(ValueError, match="subprocess resource unsupported"):
        resources.request_subprocess("generated-operation")


def test_provider_rights_revocation_inside_secret_callback_prevents_network(
    generated_provider_rights,
):
    _, policy_source = generated_provider_rights

    def revoke_rights():
        context = policy_source.current
        revocation = BrokerPolicyRevocationV1(
            context.selected_policy_ids[0],
            20,
            20,
            (context.evidence[0].artifact_id,),
            "withdrawn",
        )
        policy_source.current = replace(context, revocations=(revocation,))

    with _http_fixture() as (port, observed):
        authority, _, _, _ = _authority(port=port)
        provider = _SecretProvider(revoke_rights)
        resources = _resources(authority, provider)
        with pytest.raises(BrokerPolicyError):
            resources.request(
                "quotes", "GET", "/v1/ticks", secret_profile="paper"
            )
        assert provider.calls == ["generated-opaque-handle"]
        assert observed == []


def test_resource_grant_is_not_itself_current_provider_rights():
    authority, _, _, _ = _authority()
    resources = _resources(authority)
    for operation in (
        lambda: resources.request("quotes", "GET", "/v1/ticks"),
        lambda: resources.get_cache("prices", "one"),
        lambda: resources.put_cache("prices", "one", b"ok"),
    ):
        with pytest.raises(
            BrokerPolicyError, match="current_process_policy_scope_required"
        ):
            operation()


def test_resources_require_exact_actual_native_configuration_binding():
    authority, _, _, _ = _authority()
    request = _native_request()
    different = replace(
        request,
        configuration_profile=replace(
            request.configuration_profile, profile_id="different-profile"
        ),
    )
    with pytest.raises(
        ValueError, match="provider resource invocation binding mismatch"
    ):
        BrokerPermissionResourcesV1(authority, provider_request=different)


class _Controls:
    acknowledged_delivery = 4

    def __init__(self, replies):
        self.replies = list(replies)
        self.timeouts = []

    def receive(self, timeout):
        self.timeouts.append(timeout)
        return self.replies.pop(0) if self.replies else None


def _reply(**changes):
    value = {
        "type": "permission_reply",
        "invocation_id": "generated-invocation",
        "epoch": 2,
        "sequence": 0,
        "ok": True,
        "payload": "[]",
    }
    value.update(changes)
    return value


@contextmanager
def _channel(replies, *, maximum=4096):
    read_fd, write_fd = os.pipe()
    controls = _Controls(replies)
    try:
        yield PermissionChannel(
            controls, write_fd, "generated-invocation", 2, maximum, 0.1
        ), controls, read_fd
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_permission_rpc_real_frame_coordinates_sequence_and_stale_ack_handling():
    with _channel(
        [
            {"type": "ack", "delivery": 4},
            _reply(),
            _reply(sequence=1, payload="null"),
        ]
    ) as (channel, controls, read_fd):
        assert channel.exchange("available", {}) == []
        assert (
            channel.exchange("get_cache", {"cache_id": "prices", "key": "one"})
            is None
        )
        assert channel.sequence == 2
        frames = tuple(FrameDecoder().feed(os.read(read_fd, 65536)))
        assert len(frames) == 2
        first, second = (item[0] for item in frames)
        assert first == {
            "type": "permission_request",
            "invocation_id": "generated-invocation",
            "epoch": 2,
            "sequence": 0,
            "operation": "available",
            "payload": "{}",
        }
        assert second["sequence"] == 1
        assert json.loads(second["payload"]) == {
            "cache_id": "prices",
            "key": "one",
        }
        assert all(0 <= timeout <= 0.1 for timeout in controls.timeouts)


@pytest.mark.parametrize(
    "reply",
    [
        None,
        {},
        _reply(type="other"),
        _reply(epoch=True),
        _reply(epoch=3),
        _reply(sequence=True),
        _reply(sequence=1),
        _reply(sequence=-1),
        _reply(invocation_id="other-invocation"),
        _reply(ok=1),
        _reply(payload={}),
        _reply(extra="unrequested"),
        {"type": "ack", "delivery": 5},
        {"type": "stop"},
    ],
)
def test_permission_rpc_refuses_tampered_coordinate_shape_or_future_ack(reply):
    with _channel([reply]) as (channel, _, _):
        with pytest.raises(
            BrokerPermissionError, match="invalid_worker_permission_reply"
        ):
            channel.exchange("available", {})
        assert channel.sequence == 0


def test_permission_rpc_parent_refusal_consumes_exact_one_sequence():
    with _channel(
        [_reply(ok=False, payload='"resource_refused"'), _reply(sequence=1)]
    ) as (channel, _, _):
        with pytest.raises(
            BrokerPermissionError, match="parent_resource_refused"
        ):
            channel.exchange("available", {})
        assert channel.sequence == 1
        assert channel.exchange("available", {}) == []
        assert channel.sequence == 2


@pytest.mark.parametrize(
    "payload",
    [
        '{"value":1,"value":2}',
        "[NaN]",
        "[Infinity]",
        "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]",
        " " + "[]",
    ],
)
def test_permission_rpc_rejects_nested_payload_tampering(payload):
    with _channel([_reply(payload=payload)]) as (channel, _, _):
        with pytest.raises(ValueError):
            channel.exchange("available", {})


def test_permission_rpc_reentry_fails_before_writing_a_second_request():
    with _channel([_reply()]) as (channel, controls, read_fd):
        original = controls.receive
        nested = []

        def reenter(timeout):
            controls.receive = original
            try:
                channel.exchange("available", {})
            except BrokerPermissionError as error:
                nested.append(error.reason)
            return _reply()

        controls.receive = reenter
        assert channel.exchange("available", {}) == []
        assert len(nested) == 1
        frames = tuple(FrameDecoder().feed(os.read(read_fd, 65536)))
        assert len(frames) == 1
        assert channel.sequence == 1


@pytest.mark.parametrize(
    "inventory",
    [
        ["emit:quotes", "emit:quotes"],
        ["emit:quotes", "emit:health"],
        ["filesystem:*"],
        ["emit:quotes"] * 129,
    ],
)
def test_worker_resource_inventory_is_closed_unique_sorted_and_bounded(
    inventory,
):
    with _channel(
        [_reply(payload=json.dumps(inventory, separators=(",", ":")))]
    ) as (channel, _, _):
        with pytest.raises(ValueError):
            WorkerHostResources(channel).available_permissions()


def test_parent_dispatch_rejects_duplicate_nested_resource_fields(
    generated_provider_rights,
):
    authority, _, _, _ = _authority()
    resources = _resources(authority)
    with broker_permission_scope(authority, resources=resources):
        with pytest.raises(ValueError):
            dispatch_worker_permission(
                "get_cache",
                '{"cache_id":"undeclared","cache_id":"prices","key":"one"}',
                deadline=time.monotonic() + 1,
            )


def test_parent_dispatch_resource_round_trip_and_unsupported_operations(
    generated_provider_rights,
):
    authority, _, _, _ = _authority()
    resources = _resources(authority)
    with broker_permission_scope(authority, resources=resources):
        deadline = time.monotonic() + 2
        assert (
            dispatch_worker_permission(
                "put_cache",
                '{"cache_id":"prices","key":"one","value":"b2s="}',
                deadline=deadline,
            )
            == "null"
        )
        assert (
            dispatch_worker_permission(
                "get_cache",
                '{"cache_id":"prices","key":"one"}',
                deadline=deadline,
            )
            == '"b2s="'
        )
        for operation, payload in (
            ("subprocess", "{}"),
            ("resolve_secret", "{}"),
            ("get_cache", '{"cache_id":"prices","key":"one","extra":"bad"}'),
        ):
            with pytest.raises(ValueError):
                dispatch_worker_permission(
                    operation, payload, deadline=deadline
                )


def test_permission_rpc_bounds_actual_response_frame_before_decoding_payload():
    with _channel([_reply(payload='"' + "x" * 4096 + '"')]) as (channel, _, _):
        with pytest.raises(ValueError):
            channel.exchange("available", {})


def test_permission_rpc_rejects_other_thread_before_it_can_write():
    with _channel([_reply()]) as (channel, _, read_fd):
        failures = []

        def cross_thread():
            try:
                channel.exchange("available", {})
            except BrokerPermissionError as error:
                failures.append(error.reason)

        thread = threading.Thread(target=cross_thread)
        thread.start()
        thread.join(1)
        assert not thread.is_alive()
        assert len(failures) == 1
        assert channel.exchange("available", {}) == []
        assert len(tuple(FrameDecoder().feed(os.read(read_fd, 65536)))) == 1


def test_permission_rpc_sequence_exhaustion_refuses_before_writing():
    with _channel([_reply()]) as (channel, _, read_fd):
        channel.sequence = 2**63 - 1
        with pytest.raises(
            BrokerPermissionError, match="worker_permission_sequence_limit"
        ):
            channel.exchange("available", {})
        os.set_blocking(read_fd, False)
        with pytest.raises(BlockingIOError):
            os.read(read_fd, 65536)


@pytest.mark.parametrize("block_import", [False, True])
def test_external_wheel_permission_inspection_is_record_bound_and_never_imports(
    tmp_path, monkeypatch, block_import
):
    wheel = build_permission_wheel(
        tmp_path / "wheels", block_import=block_import
    )
    site = tmp_path / "site"
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    distribution = next(metadata.distributions(path=[str(site)]))
    monkeypatch.setattr(
        metadata, "distributions", lambda: iter((distribution,))
    )
    monkeypatch.syspath_prepend(str(site))
    assert "permission_fixture.plugin" not in sys.modules
    inventory = discover_broker_plugins()
    assert not inventory.diagnostics
    assert len(inventory.candidates) == 1
    candidate = inventory.candidates[0]
    manifest = read_installed_broker_permissions(candidate)
    assert manifest.candidate_id == candidate.artifact_id
    assert manifest.resource_abi == "host_resources_v1"
    assert manifest.required_atoms == ("emit:health", "emit:quotes")
    assert manifest.endpoints[0].origin == "http://127.0.0.1:1"
    assert "permission_fixture.plugin" not in sys.modules
    path = site / permission_resource_path(
        candidate.registration.plugin_id, candidate.registration.entry_point
    )
    path.write_text(path.read_text().replace("127.0.0.1:1", "127.0.0.1:2"))
    with pytest.raises(BrokerPermissionError):
        read_installed_broker_permissions(candidate)
    assert "permission_fixture.plugin" not in sys.modules


@pytest.mark.parametrize("block_import", [False, True])
@pytest.mark.parametrize("mutation", ["none", "missing", "tampered"])
def test_permission_cli_subprocess_never_activates_record_bound_fixture(
    tmp_path, block_import, mutation
):
    wheel = build_permission_wheel(
        tmp_path / "wheels", block_import=block_import
    )
    site = tmp_path / "site"
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    resource = site / permission_resource_path(
        "org.example.permissions", "permission_fixture.plugin:factory"
    )
    if mutation == "missing":
        resource.unlink()
    elif mutation == "tampered":
        resource.write_text(
            resource.read_text().replace("127.0.0.1:1", "127.0.0.1:2")
        )
    script = """
import sys
sys.path[:0] = sys.argv[1:3]
from histdatacom.broker_plugin_registry.cli import main
result = main(['permissions', '--plugin-id', 'org.example.permissions',
               '--provider', 'fixture', '--version', '>=1.0.0,<2.0.0', '--json'])
assert 'permission_fixture' not in sys.modules
assert 'permission_fixture.plugin' not in sys.modules
raise SystemExit(result)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            script,
            str(ROOT / "src"),
            str(site),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if mutation == "none":
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["operator_grants_applied"] is False
        assert payload["activation_or_scientific_admission"] is False
        manifest = BrokerPermissionManifestV1.from_dict(
            payload["permission_manifest"]
        )
        assert manifest.resource_abi == "host_resources_v1"
    else:
        assert result.returncode == 2, result.stderr
        assert result.stdout == ""
        assert json.loads(result.stderr) == {
            "activation_or_scientific_admission": False,
            "reason": "invalid_installed_permission_manifest",
        }


def test_resource_sdk_import_is_lightweight_under_isolated_no_site_python():
    script = """
import importlib.abc
import sys
class RefuseHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('histdatacom.') and not (
            fullname.startswith('histdatacom.broker_plugins')
            or fullname in {'histdatacom.options', 'histdatacom.fx_enums'}
        ):
            raise AssertionError('Unexpected host dependency: ' + fullname)
sys.meta_path.insert(0, RefuseHost())
sys.path.insert(0, sys.argv[1])
from histdatacom.broker_plugins import BrokerHostHTTPResponseV1, BrokerHostResourcesV1
assert BrokerHostHTTPResponseV1(200, b'generated').body == b'generated'
assert sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode
assert not {'pandas', 'numpy', 'polars', 'requests'} & sys.modules.keys()
print('resource-sdk-stdlib-only')
"""
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-B", "-c", script, str(ROOT / "src")],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "resource-sdk-stdlib-only\n"

"""Real worker-process lifecycle, byte-bound persistence and refusal tests."""

from __future__ import annotations

from dataclasses import dataclass, replace
from importlib import metadata
import os
from pathlib import Path
import runpy
import select
import signal
import subprocess
import sys
import threading
import time
import venv
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
    BrokerAdmittedEventV1,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion as Completion,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason as Reason,
    BrokerLifecycleState as State,
    BrokerLifecycleError,
    BrokerLifecycleTransitionV1,
    run_broker_plugin_lifecycle as _native_lifecycle,
    inspect_broker_lifecycle,
    replay_broker_lifecycle as _native_replay,
)
from histdatacom.broker_plugin_lifecycle import ipc, storage, supervisor
from histdatacom.broker_plugin_lifecycle import contracts
from tests.fixtures.broker_runtime_policy import runtime_request, runtime_scope

_PROVIDER_REQUESTS = {}


def run_broker_plugin_lifecycle(
    inventory, plan, configuration, symbols, output, **kwargs
):
    """The old conformance cases now explicitly declare generated rights."""
    if plan is None:
        return _native_lifecycle(
            inventory,
            plan,
            configuration,
            symbols,
            output,
            provider_request=None,
            **kwargs,
        )
    request = runtime_request(plan, configuration)
    _PROVIDER_REQUESTS[output] = request
    with runtime_scope(request):
        return _native_lifecycle(
            inventory,
            plan,
            configuration,
            symbols,
            output,
            provider_request=request,
            **kwargs,
        )


def replay_broker_lifecycle(directory):
    request = _PROVIDER_REQUESTS[directory]
    with runtime_scope(request):
        yield from _native_replay(directory, provider_request=request)


ROOT = Path(__file__).resolve().parents[2]
BUILD = runpy.run_path(str(ROOT / "tests/fixtures/broker_lifecycle_wheel.py"))[
    "build_lifecycle_wheel"
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
    if os.name != "posix":
        pytest.skip("lifecycle worker execution requires POSIX")
    directory = tmp_path_factory.mktemp("lifecycle-worker")
    environment = directory / "venv"
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
    wheel = BUILD(directory / "wheels")
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    return python, site, directory


@pytest.fixture
def request_data(installed, monkeypatch):
    python, site, _ = installed
    distribution = next(metadata.distributions(path=[str(site)]))
    monkeypatch.setattr(
        metadata, "distributions", lambda: iter((distribution,))
    )
    inventory = discover_broker_plugins()
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(OPERATIONS),
        plugin_id="org.example.lifecycle",
    )
    return python, inventory, plan


def run(request_data, tmp_path, mode="finite", **kwargs):
    python, inventory, plan = request_data
    policy = kwargs.pop(
        "policy",
        BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            shutdown_timeout_ms=100,
            acknowledgement_timeout_ms=5000,
        ),
    )
    return run_broker_plugin_lifecycle(
        inventory,
        plan,
        {"mode": mode, "credential": "do-not-persist-this-value"},
        ("EURUSD",),
        tmp_path / "run",
        authorize=lambda _: True,
        policy=policy,
        worker_python=str(python),
        **kwargs,
    )


def test_finite_installed_process_run_is_durable_and_replayable(
    request_data, tmp_path
):
    result = run(request_data, tmp_path)
    assert result.manifest.completion is Completion.COMPLETE, result.reason
    assert result.manifest.appended_events == 2
    assert not result.manifest.source_continuity_verified
    assert result.manifest.worker_reaped
    assert inspect_broker_lifecycle(result.directory).complete
    records = tuple(replay_broker_lifecycle(result.directory))
    assert [record.capture_sequence for record in records] == list(
        range(len(records))
    )
    events = [
        BrokerAdmittedEventV1.from_json(record.payload_json).event
        for record in records
        if record.kind == "event"
    ]
    assert [item.quote.bid for item in events] == ["1.1", "1.1"]
    assert events[0].receive_time.utc_ns == 200
    assert all("lifecycle_fixture" not in name for name in sys.modules)
    retained = b"".join(
        item.read_bytes() for item in result.directory.iterdir()
    )
    assert b"do-not-persist-this-value" not in retained
    for pid in result.worker_pids:
        with pytest.raises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)


@pytest.mark.parametrize(
    "mode,reason",
    [
        ("open_block", Reason.STARTUP_TIMEOUT),
        ("next_block", Reason.RUN_TIMEOUT),
        ("close_block", Reason.SHUTDOWN_TIMEOUT),
        ("exception", Reason.PLUGIN_FAILURE),
        ("malformed", Reason.MALFORMED_EVENT),
        ("stdout", Reason.OUTPUT_LIMIT),
        ("stderr", Reason.OUTPUT_LIMIT),
    ],
)
def test_blocked_and_invalid_workers_are_bounded_partial_reaped(
    request_data, tmp_path, mode, reason
):
    start = time.monotonic()
    result = run(
        request_data,
        tmp_path,
        mode,
        policy=BrokerLifecyclePolicyV1(
            startup_timeout_ms=8000,
            run_timeout_ms=15000,
            shutdown_timeout_ms=50,
            stdout_bytes=64,
            stderr_bytes=64,
        ),
    )
    # Includes fresh policy-source admission and durable sidecar work outside
    # the worker's own deadlines, not only the blocked provider call.
    assert time.monotonic() - start < 40
    assert result.reason is reason
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.worker_reaped and result.manifest.unknown_loss
    assert inspect_broker_lifecycle(result.directory).partial_files
    records = tuple(replay_broker_lifecycle(result.directory))
    assert records
    assert any(record.kind == "identity" for record in records)
    if mode in ("next_block", "close_block", "malformed"):
        assert any(record.kind == "session" for record in records)
    retained = b"".join(
        item.read_bytes() for item in result.directory.iterdir()
    )
    assert b"do-not-echo" not in retained


def test_reconnect_epochs_retain_identical_quotes_and_never_claim_continuity(
    request_data, tmp_path
):
    result = run(
        request_data,
        tmp_path,
        "reconnect",
        policy=BrokerLifecyclePolicyV1(
            retry_delays_ms=(0, 1),
            startup_timeout_ms=15000,
            run_timeout_ms=60000,
            acknowledgement_timeout_ms=5000,
            shutdown_timeout_ms=50,
        ),
    )
    assert result.reason is Reason.RETRY_EXHAUSTED
    assert result.manifest.completion is Completion.PARTIAL
    assert len(result.worker_pids) == 3
    records = tuple(replay_broker_lifecycle(result.directory))
    quotes = [
        record
        for record in records
        if record.kind == "event"
        and BrokerAdmittedEventV1.from_json(record.payload_json).event.quote
        is not None
    ]
    assert [record.epoch for record in quotes] == [0, 0, 1, 1, 2, 2]
    assert result.manifest.unknown_loss


def test_cancellation_cannot_claim_complete(request_data, tmp_path):
    cancelled = threading.Event()
    timer = threading.Timer(0.3, cancelled.set)
    timer.start()
    try:
        result = run(
            request_data, tmp_path, "after_block", cancellation=cancelled.is_set
        )
    finally:
        timer.cancel()
    assert result.reason is Reason.CANCELLED
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.worker_reaped


def _gate_first_startup_frame(monkeypatch, cause, *, target="session"):
    """Stop before a chosen native frame, after its fresh policy exchanges."""
    observed = {"ready": False, "offset": 0.0}
    selector_type = supervisor.selectors.DefaultSelector
    real_time = supervisor.time
    real_os = supervisor.os
    probe = ipc.FrameDecoder()
    buffered = {}

    class BufferedOS:
        def __getattr__(self, name):
            return getattr(real_os, name)

        def read(self, descriptor, size):
            if descriptor in buffered:
                return buffered.pop(descriptor)[1]
            return real_os.read(descriptor, size)

    class GatedSelector(selector_type):
        def select(self, timeout=None):
            if buffered:
                return [(next(iter(buffered.values()))[0], 1)]
            ready = super().select(timeout)
            if not observed["ready"]:
                for key, _ in ready:
                    if key.data != "ipc":
                        continue
                    data = real_os.read(key.fd, 65_536)
                    buffered[key.fd] = (key, data)
                    if any(
                        frame.get("type") == target
                        for frame, _ in probe.feed(data)
                    ):
                        observed["ready"] = True
                        observed["offset"] = {
                            "cancel": 0.0,
                            "startup": 20.0,
                            "run": 40.0,
                        }[cause]
                        return []
            return ready

    monkeypatch.setattr(supervisor, "os", BufferedOS())
    monkeypatch.setattr(supervisor.selectors, "DefaultSelector", GatedSelector)
    monkeypatch.setattr(
        supervisor,
        "time",
        SimpleNamespace(
            monotonic=lambda: real_time.monotonic() + observed["offset"],
            time_ns=real_time.time_ns,
            monotonic_ns=real_time.monotonic_ns,
            sleep=real_time.sleep,
        ),
    )
    return observed


@pytest.mark.parametrize(
    "cause,reason",
    [
        ("cancel", Reason.CANCELLED),
        ("startup", Reason.STARTUP_TIMEOUT),
        ("run", Reason.RUN_TIMEOUT),
    ],
)
def test_stop_before_decoding_valid_startup_drains_without_becoming_active(
    request_data, tmp_path, monkeypatch, cause, reason
):
    observed = _gate_first_startup_frame(monkeypatch, cause)
    acknowledgements = []
    encode = supervisor.encode_frame

    def capture_control(frame, maximum=524288):
        if frame.get("type") == "ack":
            acknowledgements.append(frame)
        return encode(frame, maximum)

    monkeypatch.setattr(supervisor, "encode_frame", capture_control)
    result = run(
        request_data,
        tmp_path,
        cancellation=lambda: cause == "cancel" and observed["ready"],
        policy=BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            shutdown_timeout_ms=1000,
        ),
    )
    assert observed["ready"]
    assert result.reason is reason
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.worker_reaped
    records = tuple(replay_broker_lifecycle(result.directory))
    assert [record.kind for record in records].count("identity") == 1
    assert [record.kind for record in records].count("session") == 1
    assert not acknowledgements
    assert result.manifest.appended_events == 0
    # Fresh per-callback policy exchange prevents a new provider pull after
    # cancellation, unlike the old startup-only authorization handshake.
    assert (
        result.manifest.received_events == result.manifest.discarded_known == 0
    )
    assert all(
        BrokerLifecycleTransitionV1.from_json(record.payload_json).current
        is not State.ACTIVE
        for record in records
        if record.kind == "transition"
    )


@pytest.mark.parametrize(
    "malformed",
    ["identity", "duplicate_identity", "reversed_session", "event_binding"],
)
def test_startup_stop_does_not_hide_invalid_late_evidence(
    request_data, tmp_path, monkeypatch, malformed
):
    observed = _gate_first_startup_frame(
        monkeypatch,
        "cancel",
        target="event" if malformed == "event_binding" else "identity",
    )
    decoder = supervisor.FrameDecoder

    class MalformedDecoder(decoder):
        def feed(self, data):
            for frame, size in super().feed(data):
                if frame.get("type") == "identity":
                    if malformed == "identity":
                        frame = {"type": "identity", "payload": "{}"}
                    elif malformed == "reversed_session":
                        frame = {"type": "session", "payload": frame["payload"]}
                    elif malformed == "duplicate_identity":
                        yield frame, size
                if (
                    frame.get("type") == "event"
                    and malformed == "event_binding"
                ):
                    admitted = BrokerAdmittedEventV1.from_json(frame["payload"])
                    frame = {
                        **frame,
                        "payload": replace(
                            admitted,
                            plan_id="broker-capability-plan:sha256:" + "a" * 64,
                        ).to_json(),
                    }
                yield frame, size

    monkeypatch.setattr(supervisor, "FrameDecoder", MalformedDecoder)
    result = run(
        request_data,
        tmp_path,
        cancellation=lambda: observed["ready"],
        policy=BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            shutdown_timeout_ms=1000,
        ),
    )
    assert observed["ready"]
    assert result.reason in (Reason.INTEGRITY, Reason.MALFORMED_EVENT)
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.appended_events == 0
    assert result.manifest.worker_reaped


def test_exited_worker_with_inherited_pipes_cannot_claim_complete(
    request_data, tmp_path
):
    result = run(request_data, tmp_path, "inherited_pipes")
    assert result.reason is Reason.SHUTDOWN_TIMEOUT
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.forced_terminations == 1
    assert result.manifest.worker_reaped
    assert tuple(replay_broker_lifecycle(result.directory))


@pytest.mark.skipif(
    os.name != "posix", reason="process-group cleanup requires POSIX"
)
def test_cleanup_signals_same_group_children_after_leader_exit():
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import os,time,signal; child=os.fork(); (signal.signal(signal.SIGTERM,signal.SIG_IGN),print(os.getpid(),flush=True),time.sleep(60)) if child == 0 else None; os._exit(0)",
        ],
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None
    try:
        assert select.select([process.stdout], [], [], 3)[0]
        child = int(process.stdout.readline())
        process.wait(timeout=3)
        reaped, signalled = supervisor._terminate(process, 0.05)
        assert reaped and signalled
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(child, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            pytest.fail("same-group child survived cleanup")
    finally:
        process.stdout.close()
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


@pytest.mark.parametrize(
    "mode,unknown,error_count,complete",
    [
        ("health_gap", True, 0, False),
        ("health_error", False, 1, False),
        ("health_warning", False, 0, True),
        ("health_info", False, 0, True),
    ],
)
def test_health_diagnostics_distinguish_source_loss_from_other_errors(
    request_data, tmp_path, mode, unknown, error_count, complete
):
    result = run(request_data, tmp_path, mode)
    assert result.manifest.unknown_loss is unknown
    assert result.manifest.error_diagnostics == error_count
    assert (result.manifest.completion is Completion.COMPLETE) is complete
    assert tuple(replay_broker_lifecycle(result.directory))
    if error_count:
        forged = replace(result.manifest, error_diagnostics=0)
        (result.directory / "manifest.json").write_text(forged.to_json())
        with pytest.raises(BrokerLifecycleError):
            next(replay_broker_lifecycle(result.directory))
        assert result.manifest.partial_partition is not None
        sealed = replace(
            result.manifest,
            partitions=result.manifest.partitions
            + (result.manifest.partial_partition,),
            partial_partition=None,
        )
        with pytest.raises(BrokerLifecycleError):
            replace(sealed, completion=Completion.COMPLETE)
        forged_complete = replace(
            sealed, error_diagnostics=0, completion=Completion.COMPLETE
        )
        partial = next(result.directory.glob("*.partial"))
        partial.rename(partial.with_suffix(".jsonl"))
        (result.directory / "manifest.json").write_text(
            forged_complete.to_json()
        )
        with pytest.raises(BrokerLifecycleError):
            next(replay_broker_lifecycle(result.directory))


def test_health_budget_reserves_storm_and_terminal_records():
    with pytest.raises(BrokerLifecycleError):
        BrokerLifecyclePolicyV1(max_health_records=8, retry_delays_ms=(0,))
    assert BrokerLifecyclePolicyV1(max_health_records=10, retry_delays_ms=(0,))


@pytest.mark.parametrize(
    "policy",
    [
        BrokerLifecyclePolicyV1(max_capture_bytes=1024, partition_bytes=1024),
        BrokerLifecyclePolicyV1(max_partitions=1, partition_events=1),
    ],
)
def test_exhausted_journal_budget_leaves_open_evidence_without_fabricating_terminal(
    request_data, tmp_path, policy
):
    with pytest.raises(BrokerLifecycleError, match="persistence_failure"):
        run(request_data, tmp_path, policy=policy)
    if policy.max_capture_bytes == 1024:
        # This budget cannot hold the initial native manifest AND its required
        # admission sidecar. Refuse before publishing either, not an invented
        # OPEN/terminal receipt outside the quota.
        assert not tuple((tmp_path / "run").iterdir())
        with pytest.raises(BrokerLifecycleError):
            inspect_broker_lifecycle(tmp_path / "run")
        return
    inspection = inspect_broker_lifecycle(tmp_path / "run")
    assert inspection.manifest.completion is Completion.OPEN
    assert not inspection.complete
    with pytest.raises(BrokerLifecycleError):
        next(replay_broker_lifecycle(tmp_path / "run"))


def test_oversized_identity_partition_can_still_finalize_an_honest_partial(
    request_data, tmp_path
):
    result = run(
        request_data,
        tmp_path,
        policy=BrokerLifecyclePolicyV1(partition_bytes=1024),
    )
    assert result.reason is Reason.PERSISTENCE
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.appended_events == 0
    assert tuple(replay_broker_lifecycle(result.directory))


def test_actual_short_write_poisons_journal_and_never_appends_after_unknown_tail(
    request_data, tmp_path, monkeypatch
):
    append = storage.Journal.append
    write = os.write

    def break_event(self, record):
        if record.kind == "event":

            def short_then_fail(descriptor, data):
                write(descriptor, data[:7])
                raise OSError("private filesystem failure detail")

            with monkeypatch.context() as temporary:
                temporary.setattr(storage.os, "write", short_then_fail)
                append(self, record)
        else:
            append(self, record)

    monkeypatch.setattr(storage.Journal, "append", break_event)
    with pytest.raises(BrokerLifecycleError, match="^persistence_failure$"):
        run(request_data, tmp_path)
    inspection = inspect_broker_lifecycle(tmp_path / "run")
    assert inspection.manifest.completion is Completion.OPEN
    assert inspection.partial_files
    with pytest.raises(BrokerLifecycleError, match="^integrity_failure$"):
        next(replay_broker_lifecycle(tmp_path / "run"))


def test_authorization_precedes_directory_and_worker(request_data, tmp_path):
    python, inventory, plan = request_data
    with pytest.raises(BrokerLifecycleError, match="authorization_required"):
        run_broker_plugin_lifecycle(
            inventory,
            plan,
            {"mode": "finite"},
            ("EURUSD",),
            tmp_path / "denied",
            authorize=lambda _: False,
            worker_python=str(python),
        )
    assert not (tmp_path / "denied").exists()


@pytest.mark.parametrize("platform_name", ["nt", "unsupported"])
def test_non_posix_refuses_before_authorization_output_or_worker(
    monkeypatch, tmp_path, platform_name
):
    touched = []

    def forbidden(*args, **kwargs):
        touched.append(True)
        pytest.fail("unsupported runtime crossed the platform guard")

    # Replace only this module's OS reference: mutating global os.name would
    # also change pathlib/pytest behavior on the actual test-runner platform.
    monkeypatch.setattr(supervisor, "os", SimpleNamespace(name=platform_name))
    monkeypatch.setattr(supervisor, "Journal", forbidden)
    monkeypatch.setattr(supervisor, "_epoch", forbidden)
    monkeypatch.setattr(
        supervisor, "subprocess", SimpleNamespace(Popen=forbidden)
    )
    output = tmp_path / "unsupported-run"
    with pytest.raises(BrokerLifecycleError, match="^invalid_request$"):
        run_broker_plugin_lifecycle(
            None,
            None,
            {},
            (),
            output,
            authorize=forbidden,
        )
    assert not touched
    assert not output.exists()


def test_partition_tamper_is_refused_before_first_replayed_record(
    request_data, tmp_path
):
    result = run(request_data, tmp_path)
    partition = next(result.directory.glob("*.jsonl"))
    data = partition.read_bytes()
    partition.write_bytes(data.replace(b'"epoch":0', b'"epoch":1', 1))
    with pytest.raises(BrokerLifecycleError):
        next(replay_broker_lifecycle(result.directory))


def test_transition_contract_refuses_unknown_and_terminal_edges():
    with pytest.raises(BrokerLifecycleError):
        BrokerLifecycleTransitionV1(
            State.STOPPED, State.ACTIVE, Reason.ACTIVE, 0
        )
    with pytest.raises(BrokerLifecycleError):
        BrokerLifecycleTransitionV1(
            State.DISCOVERED, State.CONFIGURED, Reason.EOF, 0
        )
    with pytest.raises(BrokerLifecycleError):
        BrokerLifecycleTransitionV1(
            State.ACTIVE, State.DEGRADED, Reason.ACTIVE, 0
        )
    with pytest.raises(BrokerLifecycleError):
        BrokerLifecycleTransitionV1(
            State.STOPPING, State.FAILED, Reason.CONFIGURED, 0
        )


@pytest.mark.parametrize(
    "previous,current,reason",
    [
        (State.DISCOVERED, State.CONFIGURED, Reason.CONFIGURED),
        (State.DISCOVERED, State.STOPPING, Reason.CANCELLED),
        (State.DISCOVERED, State.FAILED, Reason.PERSISTENCE),
        (State.CONFIGURED, State.STARTING, Reason.STARTING),
        (State.CONFIGURED, State.STOPPING, Reason.CANCELLED),
        (State.CONFIGURED, State.FAILED, Reason.PERSISTENCE),
        (State.STARTING, State.ACTIVE, Reason.ACTIVE),
        (State.STARTING, State.DEGRADED, Reason.RECONNECT),
        (State.STARTING, State.STOPPING, Reason.STARTUP_TIMEOUT),
        (State.STARTING, State.FAILED, Reason.PLUGIN_FAILURE),
        (State.ACTIVE, State.DEGRADED, Reason.GAP),
        (State.ACTIVE, State.STOPPING, Reason.EOF),
        (State.DEGRADED, State.RECONNECTING, Reason.RETRY),
        (State.DEGRADED, State.STOPPING, Reason.EOF),
        (State.DEGRADED, State.FAILED, Reason.RETRY_EXHAUSTED),
        (State.RECONNECTING, State.STARTING, Reason.STARTING),
        (State.RECONNECTING, State.STOPPING, Reason.CANCELLED),
        (State.RECONNECTING, State.FAILED, Reason.RETRY_EXHAUSTED),
        (State.STOPPING, State.STOPPED, Reason.CLOSED),
        (State.STOPPING, State.FAILED, Reason.FORCED),
    ],
)
def test_every_declared_state_edge_has_roundtrippable_health_evidence(
    previous, current, reason
):
    value = BrokerLifecycleTransitionV1(previous, current, reason, 0)
    assert BrokerLifecycleTransitionV1.from_json(value.to_json()) == value


def test_lost_ack_retries_only_exact_delivery_without_duplicate_persistence(
    request_data, tmp_path, monkeypatch
):
    original = supervisor.encode_frame
    lost = False

    def lose_first_ack(value, maximum=524288):
        nonlocal lost
        if value.get("type") == "ack" and not lost:
            lost = True
            return b""
        return original(value, maximum)

    monkeypatch.setattr(supervisor, "encode_frame", lose_first_ack)
    result = run(
        request_data,
        tmp_path,
        policy=BrokerLifecyclePolicyV1(
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            acknowledgement_timeout_ms=5000,
            shutdown_timeout_ms=100,
        ),
    )
    assert lost
    assert result.manifest.completion is Completion.COMPLETE
    assert result.manifest.duplicate_deliveries == 1
    assert result.manifest.appended_events == 2
    assert (
        len(
            [
                item
                for item in replay_broker_lifecycle(result.directory)
                if item.kind == "event"
            ]
        )
        == 2
    )


def test_queue_saturation_refuses_without_unbounded_growth(
    request_data, tmp_path, monkeypatch
):
    original = supervisor.encode_frame
    monkeypatch.setattr(
        supervisor,
        "encode_frame",
        lambda value, maximum=524288: (
            b"" if value.get("type") == "ack" else original(value, maximum)
        ),
    )
    append = storage.Journal.append

    def slow_append(self, record):
        append(self, record)
        if record.kind == "event":
            time.sleep(0.15)

    monkeypatch.setattr(storage.Journal, "append", slow_append)
    result = run(
        request_data,
        tmp_path,
        policy=BrokerLifecyclePolicyV1(
            queue_items=1,
            startup_timeout_ms=15000,
            run_timeout_ms=30000,
            acknowledgement_timeout_ms=20,
            delivery_retries=8,
            shutdown_timeout_ms=50,
        ),
    )
    assert result.reason is Reason.QUEUE_SATURATED
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.appended_events <= 1


@pytest.mark.parametrize("after_append", [False, True])
def test_persistence_interruption_before_ack_never_claims_complete(
    request_data, tmp_path, monkeypatch, after_append
):
    append = storage.Journal.append
    failed = False

    def fail_once(self, record):
        nonlocal failed
        if record.kind == "event" and not failed:
            failed = True
            if after_append:
                append(self, record)
            raise BrokerLifecycleError(Reason.PERSISTENCE)
        append(self, record)

    monkeypatch.setattr(storage.Journal, "append", fail_once)
    result = run(request_data, tmp_path)
    assert result.reason is Reason.PERSISTENCE
    assert result.manifest.completion is Completion.PARTIAL
    assert result.manifest.appended_events == int(after_append)
    assert result.manifest.worker_reaped
    assert tuple(replay_broker_lifecycle(result.directory))


def test_blocked_module_import_is_terminated_before_open(
    request_data, installed, tmp_path, monkeypatch
):
    python, site, directory = installed
    wheel = BUILD(directory / "blocked-wheel", block_import=True)
    original = {
        name: (site / name).read_bytes()
        for name in (
            "lifecycle_fixture/plugin.py",
            "histdatacom_lifecycle_fixture-1.0.0.dist-info/RECORD",
        )
    }
    try:
        with ZipFile(wheel) as archive:
            archive.extractall(site)
        inventory = discover_broker_plugins()
        plan = negotiate_broker_capabilities(
            inventory,
            BrokerCapabilityWorkflowV1(OPERATIONS),
            plugin_id="org.example.lifecycle",
        )
        result = run(
            (python, inventory, plan),
            tmp_path,
            policy=BrokerLifecyclePolicyV1(
                startup_timeout_ms=8000, shutdown_timeout_ms=50
            ),
        )
        assert result.reason is Reason.STARTUP_TIMEOUT
        assert result.manifest.forced_terminations == 1
        assert result.manifest.appended_events == 0
    finally:
        for name, data in original.items():
            (site / name).write_bytes(data)


def test_bootstrap_larger_than_queue_refuses_before_mutation(
    request_data, tmp_path
):
    with pytest.raises(BrokerLifecycleError, match="invalid_request"):
        run(
            request_data,
            tmp_path,
            policy=BrokerLifecyclePolicyV1(queue_bytes=1024),
        )
    assert not (tmp_path / "run").exists()


def test_exact_event_budget_never_probes_an_extra_event_or_claims_eof(
    request_data, tmp_path
):
    result = run(
        request_data,
        tmp_path,
        policy=BrokerLifecyclePolicyV1(
            max_events=2,
            startup_timeout_ms=15000,
            acknowledgement_timeout_ms=5000,
        ),
    )
    assert result.reason is Reason.EVENT_LIMIT
    assert result.manifest.appended_events == 2
    assert result.manifest.completion is Completion.PARTIAL


def test_partition_rotation_and_replay(request_data, tmp_path):
    result = run(
        request_data,
        tmp_path,
        policy=BrokerLifecyclePolicyV1(
            partition_events=1,
            startup_timeout_ms=15000,
            acknowledgement_timeout_ms=5000,
        ),
    )
    assert len(result.manifest.partitions) == result.manifest.appended_records
    assert (
        len(tuple(replay_broker_lifecycle(result.directory)))
        == result.manifest.appended_records
    )


def test_new_partition_directory_entry_is_fsynced_before_event_ack(
    request_data, tmp_path, monkeypatch
):
    synced = []
    sync = storage._sync_directory

    def record_sync(directory):
        sync(directory)
        synced.append(
            (directory, tuple(item.name for item in directory.iterdir()))
        )

    monkeypatch.setattr(storage, "_sync_directory", record_sync)
    encode = supervisor.encode_frame

    def verify_ack(value, maximum=524288):
        if value.get("type") == "ack":
            assert any(
                directory == tmp_path / "run"
                and any(name.endswith(".partial") for name in names)
                for directory, names in synced
            )
            assert any(directory == tmp_path for directory, _ in synced)
        return encode(value, maximum)

    monkeypatch.setattr(supervisor, "encode_frame", verify_ack)
    assert (
        run(request_data, tmp_path).manifest.completion is Completion.COMPLETE
    )


@pytest.mark.parametrize("mode", ["symlink", "fifo", "directory"])
def test_nonregular_manifest_is_refused(request_data, tmp_path, mode):
    result = run(request_data, tmp_path)
    manifest = result.directory / "manifest.json"
    saved = tmp_path / "saved.json"
    manifest.rename(saved)
    if mode == "symlink":
        manifest.symlink_to(saved)
    elif mode == "fifo":
        os.mkfifo(manifest)
    else:
        manifest.mkdir()
    with pytest.raises(BrokerLifecycleError):
        inspect_broker_lifecycle(result.directory)


def test_ipc_fragmentation_duplicate_keys_and_oversized_header():
    frame = ipc.encode_frame({"value": "hello"})
    decoder = ipc.FrameDecoder()
    values = []
    for byte in frame:
        values.extend(decoder.feed(bytes([byte])))
    decoder.finish()
    assert values == [({"value": "hello"}, len(frame))]
    with pytest.raises(BrokerLifecycleError):
        list(ipc.FrameDecoder().feed(b"\xff\xff\xff\xff"))
    invalid = b'{"x":1,"x":2}'
    with pytest.raises(BrokerLifecycleError):
        list(ipc.FrameDecoder().feed(len(invalid).to_bytes(4, "big") + invalid))
    decoder = ipc.FrameDecoder()
    list(decoder.feed(frame[:-1]))
    with pytest.raises(BrokerLifecycleError):
        decoder.finish()


def test_ipc_alias_expansion_and_cyclic_values_are_bounded():
    value = ["x" * 1024]
    for _ in range(4):
        value = [value] * 128
    for candidate in (value, float("nan"), 2**1000):
        with pytest.raises(BrokerLifecycleError):
            ipc.encode_frame({"value": candidate})
    cycle = []
    cycle.append(cycle)
    with pytest.raises(BrokerLifecycleError):
        ipc.encode_frame({"value": cycle})


def test_full_envelope_bound_accounts_for_multiple_fields_and_identity(
    monkeypatch,
):
    @dataclass(frozen=True)
    class Envelope(contracts._Artifact):
        left: str
        right: str
        KIND = "test-envelope"

        def _validate(self):
            pass

    monkeypatch.setattr(contracts, "MAX_LIFECYCLE_BYTES", 4096)
    accepted = Envelope("a" * 1900, "b" * 1900)
    assert Envelope.from_json(accepted.to_json()) == accepted
    with pytest.raises(BrokerLifecycleError):
        Envelope("a" * 2050, "b" * 2050)
    with pytest.raises(BrokerLifecycleError):
        Envelope("\u2603" * 1000, "b" * 1000)


def test_cancelled_before_start_never_imports_or_creates_worker(
    request_data, tmp_path
):
    result = run(request_data, tmp_path, cancellation=lambda: True)
    assert not result.worker_pids
    assert result.reason is Reason.CANCELLED
    assert result.manifest.completion is Completion.PARTIAL
    assert tuple(replay_broker_lifecycle(result.directory))


def test_unknown_loss_summary_cannot_erase_reconnect_evidence(
    request_data, tmp_path
):
    result = run(request_data, tmp_path, "reconnect")
    forged = replace(result.manifest, unknown_loss=False)
    (result.directory / "manifest.json").write_text(forged.to_json())
    with pytest.raises(BrokerLifecycleError):
        next(replay_broker_lifecycle(result.directory))


@pytest.mark.parametrize("after_append", [False, True])
def test_abrupt_parent_exit_leaves_open_partial_and_worker_self_terminates(
    installed, request_data, tmp_path, after_append
):
    python, _, _ = installed
    request = runtime_request(request_data[2], {"mode": "finite"})
    _PROVIDER_REQUESTS[tmp_path / "run"] = request
    with runtime_scope(request) as source:
        context_json = source.current.to_json()
    script = """
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_capabilities import BrokerCapabilityWorkflowV1, negotiate_broker_capabilities
from histdatacom.broker_plugin_lifecycle import run_broker_plugin_lifecycle, BrokerLifecyclePolicyV1
from histdatacom.broker_plugin_lifecycle import storage, supervisor
from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
from histdatacom.broker_plugin_policy.contracts import BrokerPolicyContextV1
from histdatacom.broker_plugin_policy.scope import provider_policy_scope
root = Path(sys.argv[2])
original_popen = supervisor.subprocess.Popen
def record_process(*args, **kwargs):
    process = original_popen(*args, **kwargs)
    (root / "worker-pid").write_text(str(process.pid))
    return process
supervisor.subprocess.Popen = record_process
original_append = storage.Journal.append
def crash(self, record):
    if record.kind == "event":
        if sys.argv[3] == "True":
            original_append(self, record)
        os._exit(87)
    return original_append(self, record)
storage.Journal.append = crash
inventory = discover_broker_plugins()
operations = tuple(sorted(("configuration_schema", "open_session", "instruments", "subscribe", "iter_events", "unsubscribe")))
plan = negotiate_broker_capabilities(inventory, BrokerCapabilityWorkflowV1(operations), plugin_id="org.example.lifecycle")
request = BrokerSDKInvocationV1.from_json(sys.argv[4])
class CurrentSource:
    def read_policy_context(self):
        return BrokerPolicyContextV1.from_json(sys.argv[5])
with provider_policy_scope(CurrentSource()):
    run_broker_plugin_lifecycle(inventory, plan, {"mode": "finite"}, ("EURUSD",), root / "run", authorize=lambda _: True, provider_request=request, policy=BrokerLifecyclePolicyV1(startup_timeout_ms=15000,run_timeout_ms=30000,acknowledgement_timeout_ms=5000))
"""
    parent = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            script,
            str(ROOT / "src"),
            str(tmp_path),
            str(after_append),
            request.to_json(),
            context_json,
        ],
        capture_output=True,
        timeout=60,
    )
    assert parent.returncode == 87, parent.stderr
    inspection = inspect_broker_lifecycle(tmp_path / "run")
    assert not inspection.complete
    assert inspection.manifest.completion is Completion.OPEN
    assert inspection.partial_files
    with pytest.raises(BrokerLifecycleError):
        next(replay_broker_lifecycle(tmp_path / "run"))
    pid = int((tmp_path / "worker-pid").read_text())
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.02)
    else:
        pytest.fail("worker survived abrupt parent death")

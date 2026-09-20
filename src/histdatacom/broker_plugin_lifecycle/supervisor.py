"""Caller-authorized POSIX worker supervision with host-owned completion."""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from copy import copy
from dataclasses import dataclass, replace
import os
from pathlib import Path
import platform
import selectors
import signal
import subprocess
import sys
import time
from typing import cast
import uuid

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityPlanV1,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1
from histdatacom.broker_plugins import (
    BrokerDiagnosticSeverity,
    BrokerEventKind,
    BrokerReasonCode,
)

from .contracts import (
    BrokerLifecycleCompletion as Completion,
    BrokerLifecycleError,
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason as Reason,
    BrokerLifecycleRecordV1,
    BrokerLifecycleState as State,
    BrokerLifecycleTransitionV1,
)
from .ipc import FrameDecoder, encode_frame
from .evidence import Evidence
from .storage import Journal

Clock = Callable[[], tuple[int, int]]


def _clock() -> tuple[int, int]:
    return time.time_ns(), time.monotonic_ns()


@dataclass(frozen=True, slots=True)
class BrokerLifecycleResultV1:
    directory: Path
    manifest: BrokerLifecycleManifestV1
    reason: Reason
    worker_pids: tuple[int, ...]


@dataclass
class _Counters:
    received: int = 0
    duplicates: int = 0
    stdout: int = 0
    stderr: int = 0
    unknown_loss: bool = False
    forced: int = 0


class _Run:
    def __init__(self, journal: Journal, clock: Clock) -> None:
        self.journal = journal
        self.clock = clock
        self.epoch = 0
        self.counters = _Counters()

    def append(
        self, kind: str, payload: str, delivery: int | None = None
    ) -> None:
        record = self.record(kind, payload, delivery)
        try:
            self.journal.append(record)
        except OSError:
            raise BrokerLifecycleError(Reason.PERSISTENCE) from None

    def record(
        self,
        kind: str,
        payload: str,
        delivery: int | None = None,
        *,
        sequence: int | None = None,
    ) -> BrokerLifecycleRecordV1:
        utc, monotonic = self.clock()
        return BrokerLifecycleRecordV1(
            self.journal.header.artifact_id,
            self.journal.evidence.sequence if sequence is None else sequence,
            self.epoch,
            utc,
            monotonic,
            kind,
            payload,
            delivery,
        )

    def transition(self, current: State, reason: Reason) -> None:
        self.append(
            "transition",
            BrokerLifecycleTransitionV1(
                self.journal.evidence.state, current, reason, self.epoch
            ).to_json(),
        )


def _terminate(
    process: subprocess.Popen[bytes], timeout: float
) -> tuple[bool, bool]:
    """Always reap our worker; process-group signaling is not a sandbox."""
    signalled = False
    try:
        # The leader may already have exited while an ordinary same-group
        # child still owns inherited pipes. Clean the group in either case.
        os.killpg(process.pid, signal.SIGTERM)
        signalled = True
    except OSError:
        if process.poll() is None:
            signalled = True
            try:
                process.terminate()
            except OSError:
                pass
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return False, signalled
    # A direct worker can exit before its same-group children respond to TERM.
    # Escalate for the whole group even when direct wait() returned promptly.
    try:
        os.killpg(process.pid, signal.SIGKILL)
        signalled = True
    except OSError:
        pass
    return True, signalled


def _epoch(
    run: _Run,
    configuration: dict[str, object],
    worker_python: str,
    cancellation: Callable[[], bool],
    deadline: float,
) -> tuple[Reason, bool, int, bool, bool]:
    header, policy = run.journal.header, run.journal.header.policy
    bootstrap = encode_frame(
        {"header": header.to_json(), "configuration": configuration},
        policy.frame_bytes,
    )
    read_fd, write_fd = os.pipe()
    # Internal trusted host package location, never a configurable plugin path.
    source_root = str(Path(__file__).resolve().parents[2])
    command = (
        "import sys; from pathlib import Path; p = "
        + repr(source_root)
        + "; sys.path.insert(0, p) if not any(str(Path(x).resolve()) == p for x in sys.path) else None; "
        "from histdatacom.broker_plugin_lifecycle.worker import main; main()"
    )
    process: subprocess.Popen[bytes] | None = None
    selector = selectors.DefaultSelector()
    reaped = False
    reason = Reason.WORKER_DIED
    clean = False
    closed = False
    try:
        process = subprocess.Popen(
            [worker_python, "-I", "-c", command, str(write_fd)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(write_fd,),
            start_new_session=True,
            close_fds=True,
        )
        os.close(write_fd)
        write_fd = -1
        assert (
            process.stdin is not None
            and process.stdout is not None
            and process.stderr is not None
        )
        streams = (
            (read_fd, "ipc"),
            (process.stdout.fileno(), "stdout"),
            (process.stderr.fileno(), "stderr"),
        )
        for descriptor, label in streams:
            os.set_blocking(descriptor, False)
            selector.register(descriptor, selectors.EVENT_READ, label)
        control_fd = process.stdin.fileno()
        os.set_blocking(control_fd, False)
        outgoing = bytearray(bootstrap)
        selector.register(control_fd, selectors.EVENT_WRITE, "control")
        decoder = FrameDecoder(policy.frame_bytes)
        pending: deque[tuple[dict[str, object], int]] = deque()
        pending_bytes = 0
        # Only exact transport identity is deduplicated, never quote content.
        delivered: OrderedDict[int, str] = OrderedDict()
        discarded: OrderedDict[int, str] = OrderedDict()
        draining: Evidence | None = None
        started = closed = eof = False
        stopping: float | None = None
        exited_deadline: float | None = None
        startup_deadline = time.monotonic() + policy.startup_timeout_ms / 1000

        def send(value: dict[str, object]) -> None:
            encoded = encode_frame(value, policy.frame_bytes)
            if len(outgoing) + len(encoded) > policy.queue_bytes:
                raise BrokerLifecycleError(Reason.QUEUE_SATURATED)
            outgoing.extend(encoded)
            try:
                selector.get_key(control_fd)
            except KeyError:
                selector.register(control_fd, selectors.EVENT_WRITE, "control")

        def stop(why: Reason) -> None:
            nonlocal stopping, reason
            if stopping is None:
                reason = why
                stopping = time.monotonic() + policy.shutdown_timeout_ms / 1000
                send({"type": "stop"})

        while True:
            now = time.monotonic()
            if exited_deadline is not None and now >= exited_deadline:
                if reason not in (
                    Reason.CANCELLED,
                    Reason.STARTUP_TIMEOUT,
                    Reason.RUN_TIMEOUT,
                ):
                    reason = Reason.SHUTDOWN_TIMEOUT
                break
            if stopping is None:
                if cancellation() is True:
                    stop(Reason.CANCELLED)
                elif now >= deadline:
                    stop(Reason.RUN_TIMEOUT)
                elif not started and now >= startup_deadline:
                    stop(Reason.STARTUP_TIMEOUT)
            if stopping is not None and now >= stopping:
                if reason is Reason.EOF:
                    reason = Reason.SHUTDOWN_TIMEOUT
                break
            for key, _ in selector.select(0.01):
                descriptor = key.fd
                if key.data == "control":
                    try:
                        count = os.write(descriptor, outgoing)
                    except BrokenPipeError:
                        selector.unregister(descriptor)
                        outgoing.clear()
                        continue
                    except BlockingIOError:
                        continue
                    del outgoing[:count]
                    if not outgoing:
                        selector.unregister(descriptor)
                    continue
                try:
                    data = os.read(descriptor, 65_536)
                except BlockingIOError:
                    continue
                if not data:
                    selector.unregister(descriptor)
                    if key.data == "ipc":
                        decoder.finish()
                    continue
                if key.data in ("stdout", "stderr"):
                    name = cast(str, key.data)
                    observed = getattr(run.counters, name) + len(data)
                    setattr(run.counters, name, observed)
                    if observed > getattr(policy, name + "_bytes"):
                        raise BrokerLifecycleError(Reason.OUTPUT_LIMIT)
                    continue
                for frame, size in decoder.feed(data):
                    kind = frame.get("type")
                    if kind in ("identity", "session"):
                        if (
                            closed
                            or eof
                            or set(frame) != {"type", "payload"}
                            or type(frame["payload"]) is not str
                        ):
                            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
                        run.append(str(kind), frame["payload"])
                        if kind == "session":
                            started = True
                            if stopping is None:
                                run.transition(State.ACTIVE, Reason.ACTIVE)
                    elif kind == "event":
                        if (
                            not started
                            or eof
                            or closed
                            or set(frame) != {"type", "payload", "delivery"}
                            or type(frame["payload"]) is not str
                            or type(frame["delivery"]) is not int
                        ):
                            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
                        if (
                            len(pending) >= policy.queue_items
                            or pending_bytes + size > policy.queue_bytes
                        ):
                            raise BrokerLifecycleError(Reason.QUEUE_SATURATED)
                        pending.append((frame, size))
                        pending_bytes += size
                    elif (
                        kind == "eof"
                        and set(frame) == {"type"}
                        and started
                        and not eof
                    ):
                        eof = True
                        stop(Reason.EOF)
                    elif (
                        kind == "closed"
                        and set(frame) == {"type"}
                        and stopping is not None
                        and not closed
                    ):
                        closed = True
                    elif kind == "failure" and set(frame) == {"type", "reason"}:
                        if frame["reason"] not in (
                            Reason.PLUGIN_FAILURE.value,
                            Reason.MALFORMED_EVENT.value,
                            Reason.EVENT_LIMIT.value,
                        ):
                            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
                        raise BrokerLifecycleError(Reason(frame["reason"]))
                    else:
                        raise BrokerLifecycleError(Reason.MALFORMED_IPC)
            while pending:
                frame, size = pending.popleft()
                pending_bytes -= size
                delivery = cast(int, frame["delivery"])
                admitted = BrokerAdmittedEventV1.from_json(
                    cast(str, frame["payload"])
                )
                fingerprint = admitted.artifact_id
                if delivery in delivered:
                    if delivered[delivery] != fingerprint:
                        raise BrokerLifecycleError(Reason.MALFORMED_EVENT)
                    run.counters.duplicates += 1
                    send({"type": "ack", "delivery": delivery})
                    continue
                if stopping is not None:
                    if delivery in discarded:
                        if discarded[delivery] != fingerprint:
                            raise BrokerLifecycleError(Reason.MALFORMED_EVENT)
                        run.counters.duplicates += 1
                        continue
                    # Verify late events against a transient copy only. A
                    # completed startup handshake after stop is not ACTIVE,
                    # and unpersisted deliveries must never receive an ACK.
                    if draining is None:
                        draining = copy(run.journal.evidence)
                        if (
                            draining.state is State.STARTING
                            and draining.session is not None
                        ):
                            draining.state = State.ACTIVE
                    try:
                        draining.accept(
                            run.record(
                                "event",
                                admitted.to_json(),
                                delivery,
                                sequence=draining.sequence,
                            )
                        )
                    except BrokerLifecycleError:
                        raise BrokerLifecycleError(
                            Reason.MALFORMED_EVENT
                        ) from None
                    discarded[delivery] = fingerprint
                    if len(discarded) > policy.queue_items:
                        discarded.popitem(last=False)
                    run.counters.received += 1
                    run.counters.unknown_loss = True
                    continue
                run.counters.received += 1
                if run.counters.received > policy.max_events:
                    raise BrokerLifecycleError(Reason.EVENT_LIMIT)
                try:
                    run.append("event", admitted.to_json(), delivery)
                except BrokerLifecycleError as error:
                    if error.reason is Reason.INTEGRITY:
                        raise BrokerLifecycleError(
                            Reason.MALFORMED_EVENT
                        ) from None
                    raise
                delivered[delivery] = fingerprint
                if len(delivered) > policy.queue_items:
                    delivered.popitem(last=False)
                send({"type": "ack", "delivery": delivery})
                event_kind = admitted.event.kind
                diagnostic = admitted.event.diagnostic
                source_gap = (
                    diagnostic is not None
                    and diagnostic.code is BrokerReasonCode.SOURCE_GAP
                )
                diagnostic_error = (
                    diagnostic is not None
                    and diagnostic.severity is BrokerDiagnosticSeverity.ERROR
                )
                if (
                    event_kind
                    in (
                        BrokerEventKind.GAP,
                        BrokerEventKind.DISCONNECTED,
                        BrokerEventKind.RECONNECTING,
                    )
                    or source_gap
                ):
                    run.counters.unknown_loss = True
                    if run.journal.evidence.state is State.ACTIVE:
                        run.transition(
                            State.DEGRADED,
                            (
                                Reason.GAP
                                if event_kind is BrokerEventKind.GAP
                                or source_gap
                                else Reason.RECONNECT
                            ),
                        )
                    if event_kind in (
                        BrokerEventKind.DISCONNECTED,
                        BrokerEventKind.RECONNECTING,
                    ):
                        stop(Reason.RECONNECT)
                elif (
                    diagnostic_error
                    and run.journal.evidence.state is State.ACTIVE
                ):
                    run.transition(State.DEGRADED, Reason.HEALTH_ERROR)
            if process.poll() is not None and not selector.get_map():
                break
            # Pipes can remain inherited by an unexpected descendant. Never
            # wait indefinitely after the supervised direct worker exits.
            if process.poll() is not None and exited_deadline is None:
                exited_deadline = (
                    time.monotonic() + policy.shutdown_timeout_ms / 1000
                )
            if process.poll() is not None and closed:
                # One last bounded drain lets already-ready output be counted.
                if not any(
                    key.data in ("ipc", "stdout", "stderr")
                    for key in selector.get_map().values()
                ):
                    break
        clean = (
            eof and closed and process.returncode == 0 and reason is Reason.EOF
        )
        if reason is Reason.EOF and not clean:
            reason = Reason.WORKER_DIED
    except BrokerLifecycleError as error:
        reason = error.reason
    except (Exception, SystemExit):
        reason = Reason.PLUGIN_FAILURE
    finally:
        selector.close()
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)
        if process is not None:
            reaped, signalled = _terminate(
                process, policy.shutdown_timeout_ms / 1000
            )
            if signalled:
                run.counters.forced += 1
                clean = False
                if reason is Reason.EOF:
                    reason = Reason.FORCED
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
    return reason, clean, 0 if process is None else process.pid, reaped, closed


def run_broker_plugin_lifecycle(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    configuration: Mapping[str, object],
    symbols: tuple[str, ...],
    output_directory: Path,
    *,
    authorize: Callable[[BrokerCapabilityPlanV1], bool],
    policy: BrokerLifecyclePolicyV1 | None = None,
    cancellation: Callable[[], bool] = lambda: False,
    clock: Clock = _clock,
    worker_python: str = sys.executable,
    run_nonce: str | None = None,
) -> BrokerLifecycleResultV1:
    """Run one explicitly authorized finite capture; never activate implicitly.

    Local filesystem and Python runtime are trusted. No OS sandbox, grants,
    credential policy or source-continuity certification is provided here.
    """
    try:
        if (
            os.name != "posix"
            or not isinstance(configuration, Mapping)
            or len(configuration) > 128
        ):
            raise ValueError
        ephemeral = dict(configuration)
        # Configuration is primitive, ephemeral and never hashed or persisted.
        if any(
            type(key) is not str or type(value) not in (str, int, float, bool)
            for key, value in ephemeral.items()
        ):
            raise ValueError
        if len(encode_frame(ephemeral)) > 32_768:
            raise ValueError
        header = BrokerLifecycleHeaderV1(
            inventory,
            plan,
            policy or BrokerLifecyclePolicyV1(),
            symbols,
            run_nonce or uuid.uuid4().hex,
            platform.python_version(),
        )
        bootstrap = encode_frame(
            {"header": header.to_json(), "configuration": ephemeral},
            header.policy.frame_bytes,
        )
        if len(bootstrap) > header.policy.queue_bytes:
            raise ValueError
    except Exception:
        raise BrokerLifecycleError(Reason.INVALID_REQUEST) from None
    try:
        if authorize(plan) is not True:
            raise ValueError
    except (Exception, SystemExit):
        raise BrokerLifecycleError(Reason.AUTHORIZATION) from None
    journal: Journal | None = None
    try:
        journal = Journal(output_directory, header)
        run = _Run(journal, clock)
        run.transition(State.CONFIGURED, Reason.CONFIGURED)
        pids: list[int] = []
        deadline = time.monotonic() + header.policy.run_timeout_ms / 1000
        all_reaped = True
        clean = False
        closed = False
        reason = Reason.CANCELLED
        for epoch in range(len(header.policy.retry_delays_ms) + 1):
            if cancellation() is True or time.monotonic() >= deadline:
                reason = (
                    Reason.CANCELLED
                    if cancellation() is True
                    else Reason.RUN_TIMEOUT
                )
                run.counters.unknown_loss = True
                break
            run.epoch = epoch
            run.transition(State.STARTING, Reason.STARTING)
            reason, clean, pid, reaped, closed = _epoch(
                run, ephemeral, worker_python, cancellation, deadline
            )
            if pid:
                pids.append(pid)
            all_reaped = all_reaped and reaped
            if not clean:
                run.counters.unknown_loss = True
            if reason is not Reason.RECONNECT:
                break
            if epoch >= len(header.policy.retry_delays_ms):
                reason = Reason.RETRY_EXHAUSTED
                break
            run.transition(State.RECONNECTING, Reason.RETRY)
            wake = (
                time.monotonic() + header.policy.retry_delays_ms[epoch] / 1000
            )
            while time.monotonic() < wake:
                if cancellation() is True or time.monotonic() >= deadline:
                    reason = (
                        Reason.CANCELLED
                        if cancellation() is True
                        else Reason.RUN_TIMEOUT
                    )
                    break
                time.sleep(min(0.01, max(0, wake - time.monotonic())))
            else:
                continue
            break
        run.transition(State.STOPPING, reason)
        stopped = (
            all_reaped
            and not run.counters.forced
            and (clean or (reason is Reason.CANCELLED and (closed or not pids)))
        )
        run.transition(
            State.STOPPED if stopped else State.FAILED,
            (
                (Reason.CLOSED if pids else Reason.CANCELLED)
                if stopped
                else (Reason.FORCED if run.counters.forced else reason)
            ),
        )
        counters = run.counters
        manifest = replace(
            journal.manifest,
            state=journal.evidence.state,
            completion=Completion.PARTIAL,
            partitions=tuple(journal.partitions),
            partial_partition=journal._receipt(),
            appended_records=journal.evidence.sequence,
            appended_events=journal.evidence.event_count,
            received_events=counters.received,
            duplicate_deliveries=counters.duplicates,
            discarded_known=max(
                0, counters.received - journal.evidence.event_count
            ),
            unknown_loss=counters.unknown_loss,
            stdout_discarded_bytes=counters.stdout,
            stderr_discarded_bytes=counters.stderr,
            worker_reaped=all_reaped,
            forced_terminations=counters.forced,
            error_diagnostics=journal.evidence.error_diagnostics,
        )
        complete = (
            clean
            and not counters.unknown_loss
            and not journal.evidence.error_diagnostics
            and all_reaped
        )
        manifest = journal.finish(manifest, complete=complete)
        return BrokerLifecycleResultV1(
            output_directory, manifest, reason, tuple(pids)
        )
    except BrokerLifecycleError:
        raise
    except (Exception, SystemExit):
        raise BrokerLifecycleError(Reason.PERSISTENCE) from None
    finally:
        ephemeral.clear()
        if journal is not None:
            journal.close()

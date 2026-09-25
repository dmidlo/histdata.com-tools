"""Caller-authorized POSIX worker supervision with host-owned completion."""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from copy import copy
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import subprocess
import sys
import time
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_health import (
        BrokerHostHealthAuditV1,
        BrokerHostHealthPolicyV1,
    )
    from histdatacom.broker_plugin_health.collector import HostHealthRecorder
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionExecutionV1,
    )
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


@dataclass(frozen=True, slots=True)
class BrokerLifecycleExecutionHooks:
    """Trusted host execution hooks, never plugin configuration or wire data.

    Defaults preserve the original lifecycle. A guard must raise on refusal;
    exceptions are translated to closed lifecycle errors before publication.
    Launch policy and its provenance are the caller's separate responsibility.
    """

    command_prefix: tuple[str, ...] = ()
    environment: Mapping[str, str] | None = None
    working_directory: Path | None = None
    secret_fields: tuple[str, ...] | None = None
    before_persist: Callable[[str], None] | None = None
    bootstrap_source: str | None = None


def _clock() -> tuple[int, int]:
    return time.time_ns(), time.monotonic_ns()


@dataclass(frozen=True, slots=True)
class BrokerLifecycleResultV1:
    directory: Path
    manifest: BrokerLifecycleManifestV1
    reason: Reason
    worker_pids: tuple[int, ...]
    health: BrokerHostHealthAuditV1
    health_directory: Path
    permissions: BrokerPermissionExecutionV1


@dataclass
class _Counters:
    received: int = 0
    duplicates: int = 0
    stdout: int = 0
    stderr: int = 0
    unknown_loss: bool = False
    forced: int = 0


class _Run:
    def __init__(
        self,
        journal: Journal,
        clock: Clock,
        hooks: BrokerLifecycleExecutionHooks | None = None,
        *,
        health: HostHealthRecorder,
    ) -> None:
        self.journal = journal
        self.clock = clock
        self.epoch = 0
        self.counters = _Counters()
        self.hooks = hooks
        self.health = health

    def append(
        self,
        kind: str,
        payload: str,
        delivery: int | None = None,
        *,
        ingress_sequence: int | None = None,
    ) -> None:
        record = self.record(kind, payload, delivery)
        try:
            if self.hooks is not None and self.hooks.before_persist is not None:
                try:
                    self.hooks.before_persist(record.to_json())
                except (Exception, SystemExit):
                    raise BrokerLifecycleError(Reason.INTEGRITY) from None
            self.journal.append(record)
            self.health.persisted(
                record.artifact_id,
                (
                    BrokerAdmittedEventV1.from_json(payload).artifact_id
                    if kind == "event"
                    else None
                ),
                self.epoch,
                ingress_sequence,
            )
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
    from histdatacom.broker_plugin_permissions.scope import (
        current_permission_authority,
    )
    from histdatacom.broker_plugin_permissions.decisions import (
        BrokerPermissionError,
    )
    from histdatacom.broker_plugin_permissions.dispatch import (
        dispatch_worker_permission,
    )
    from histdatacom.broker_plugin_health import (
        BrokerHostHealthReason as HealthReason,
    )

    authority = current_permission_authority()
    bootstrap = encode_frame(
        {
            "header": header.to_json(),
            "configuration": configuration,
            "provider_request": run.journal.provider_request.to_json(),
            "epoch": run.epoch,
            "permissions": {
                "manifest": authority.manifest.to_json(),
                "binding": authority.binding.to_json(),
                "grant_id": authority.grant_id,
            },
        },
        policy.frame_bytes,
    )
    from histdatacom.broker_plugin_capabilities.execution import _provider_call
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        read_current_provider_policy_context,
    )

    read_fd, write_fd = os.pipe()
    # Internal trusted host package location, never a configurable plugin path.
    source_root = str(Path(__file__).resolve().parents[2])
    command = "import sys; from pathlib import Path; p = " + repr(
        source_root
    ) + "; sys.path.insert(0, p) if not any(str(Path(x).resolve()) == p for x in sys.path) else None; " "from histdatacom.broker_plugin_lifecycle.worker import main; " + (
        "main()"
        if run.hooks is None or run.hooks.secret_fields is None
        else "main(secret_fields=" + repr(run.hooks.secret_fields) + ")"
    )
    sealed = run.hooks is not None and run.hooks.bootstrap_source is not None
    if sealed:
        assert run.hooks is not None and run.hooks.bootstrap_source is not None
        command = run.hooks.bootstrap_source + "\n" + command
    process: subprocess.Popen[bytes] | None = None
    selector = selectors.DefaultSelector()
    reaped = False
    reason = Reason.WORKER_DIED
    clean = False
    closed = False
    try:
        _provider_call(run.journal.provider_request, capture=False)
        process = subprocess.Popen(
            [
                *(run.hooks.command_prefix if run.hooks is not None else ()),
                worker_python,
                "-I",
                *(["-S", "-B"] if sealed else []),
                "-c",
                command,
                str(write_fd),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(write_fd,),
            start_new_session=True,
            close_fds=True,
            env=None if run.hooks is None else run.hooks.environment,
            cwd=None if run.hooks is None else run.hooks.working_directory,
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
        pending: deque[tuple[dict[str, object], int, int]] = deque()
        pending_bytes = 0
        # Only exact transport identity is deduplicated, never quote content.
        delivered: OrderedDict[int, str] = OrderedDict()
        discarded: OrderedDict[int, str] = OrderedDict()
        draining: Evidence | None = None
        started = closed = eof = False
        stopping: float | None = None
        exited_deadline: float | None = None
        startup_deadline = time.monotonic() + policy.startup_timeout_ms / 1000
        policy_sequence = 0
        permission_sequence = 0
        run.health.queue(0, run.epoch)

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
                    if kind == "permission_request":
                        if (
                            closed
                            or eof
                            or set(frame)
                            != {
                                "type",
                                "invocation_id",
                                "epoch",
                                "sequence",
                                "operation",
                                "payload",
                            }
                            or frame["invocation_id"]
                            != run.journal.provider_request.artifact_id
                            or type(frame["epoch"]) is not int
                            or frame["epoch"] != run.epoch
                            or type(frame["sequence"]) is not int
                            or frame["sequence"] != permission_sequence
                            or type(frame["operation"]) is not str
                            or type(frame["payload"]) is not str
                        ):
                            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
                        permission_sequence += 1
                        if stopping is not None:
                            send({"type": "stop"})
                            continue
                        authority.require_admission()
                        try:
                            response = dispatch_worker_permission(
                                frame["operation"],
                                frame["payload"],
                                deadline=(
                                    min(deadline, startup_deadline)
                                    if not started
                                    else deadline
                                ),
                            )
                            ok = True
                        except (Exception, SystemExit):
                            response, ok = '"resource_refused"', False
                        reply: dict[str, object] = {
                            "type": "permission_reply",
                            "ok": ok,
                            "invocation_id": frame["invocation_id"],
                            "epoch": frame["epoch"],
                            "sequence": frame["sequence"],
                            "payload": response,
                        }
                        if (
                            run.hooks is not None
                            and run.hooks.before_persist is not None
                        ):
                            run.hooks.before_persist(json.dumps(reply))
                        send(reply)
                    elif kind == "policy_context_request":
                        if (
                            closed
                            or eof
                            or set(frame)
                            != {"type", "invocation_id", "epoch", "sequence"}
                            or frame["invocation_id"]
                            != run.journal.provider_request.artifact_id
                            or type(frame["epoch"]) is not int
                            or frame["epoch"] != run.epoch
                            or type(frame["sequence"]) is not int
                            or frame["sequence"] != policy_sequence
                        ):
                            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
                        policy_sequence += 1
                        if stopping is not None:
                            send({"type": "stop"})
                            continue
                        # Returning the fresh ledger is not an operation grant.
                        # The child independently resolves and decides against
                        # its current clock immediately before its next call.
                        context = read_current_provider_policy_context()
                        if (
                            run.hooks is not None
                            and run.hooks.before_persist is not None
                        ):
                            try:
                                run.hooks.before_persist(context.to_json())
                            except (Exception, SystemExit):
                                raise BrokerLifecycleError(
                                    Reason.INTEGRITY
                                ) from None
                        send(
                            {
                                "type": "policy_context",
                                "invocation_id": frame["invocation_id"],
                                "epoch": frame["epoch"],
                                "sequence": frame["sequence"],
                                "context": context.to_json(),
                            }
                        )
                    elif kind in ("identity", "session"):
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
                        try:
                            incoming = BrokerAdmittedEventV1.from_json(
                                frame["payload"]
                            )
                        except Exception:
                            ingress = run.health.ingress(
                                None, run.epoch, malformed=True
                            )
                            run.health.refused(
                                ingress, None, run.epoch, HealthReason.MALFORMED
                            )
                            raise BrokerLifecycleError(
                                Reason.MALFORMED_EVENT
                            ) from None
                        ingress = run.health.ingress(
                            incoming.artifact_id, run.epoch
                        )
                        if (
                            len(pending) >= policy.queue_items
                            or pending_bytes + size > policy.queue_bytes
                        ):
                            run.health.refused(
                                ingress,
                                incoming.artifact_id,
                                run.epoch,
                                HealthReason.QUEUE_OVERFLOW,
                            )
                            run.health.queue(
                                len(pending), run.epoch, overflow=True
                            )
                            raise BrokerLifecycleError(Reason.QUEUE_SATURATED)
                        pending.append((frame, size, ingress))
                        pending_bytes += size
                        run.health.queue(len(pending), run.epoch)
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
                            Reason.AUTHORIZATION.value,
                            *(
                                (Reason.INTEGRITY.value,)
                                if run.hooks is not None
                                and run.hooks.secret_fields is not None
                                else ()
                            ),
                        ):
                            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
                        if (
                            frame["reason"] == Reason.AUTHORIZATION.value
                            and stopping is not None
                        ):
                            # A pending ledger read consumes the parent's stop
                            # control; core source refusal must not relabel an
                            # already requested cancellation/deadline.
                            raise BrokerLifecycleError(reason)
                        raise BrokerLifecycleError(Reason(frame["reason"]))
                    else:
                        raise BrokerLifecycleError(Reason.MALFORMED_IPC)
            while pending:
                frame, size, ingress = pending.popleft()
                pending_bytes -= size
                run.health.queue(len(pending), run.epoch)
                delivery = cast(int, frame["delivery"])
                admitted = BrokerAdmittedEventV1.from_json(
                    cast(str, frame["payload"])
                )
                fingerprint = admitted.artifact_id
                if delivery in delivered:
                    if delivered[delivery] != fingerprint:
                        run.health.refused(
                            ingress,
                            fingerprint,
                            run.epoch,
                            HealthReason.MALFORMED,
                        )
                        raise BrokerLifecycleError(Reason.MALFORMED_EVENT)
                    run.counters.duplicates += 1
                    run.health.refused(
                        ingress,
                        fingerprint,
                        run.epoch,
                        HealthReason.DUPLICATE_DELIVERY,
                    )
                    send({"type": "ack", "delivery": delivery})
                    continue
                if stopping is not None:
                    run.health.refused(
                        ingress, fingerprint, run.epoch, HealthReason.CANCELLED
                    )
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
                    run.health.refused(
                        ingress, fingerprint, run.epoch, HealthReason.CAPABILITY
                    )
                    raise BrokerLifecycleError(Reason.EVENT_LIMIT)
                try:
                    run.append(
                        "event",
                        admitted.to_json(),
                        delivery,
                        ingress_sequence=ingress,
                    )
                except BrokerPermissionError:
                    run.health.refused(
                        ingress, fingerprint, run.epoch, HealthReason.PERMISSION
                    )
                    raise
                except BrokerLifecycleError as error:
                    run.health.refused(
                        ingress,
                        fingerprint,
                        run.epoch,
                        (
                            HealthReason.MALFORMED
                            if error.reason is Reason.INTEGRITY
                            else HealthReason.PERSISTENCE
                        ),
                    )
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
    except (BrokerPolicyError, BrokerPermissionError):
        reason = Reason.AUTHORIZATION
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
    provider_request: BrokerSDKInvocationV1,
    policy: BrokerLifecyclePolicyV1 | None = None,
    cancellation: Callable[[], bool] = lambda: False,
    clock: Clock = _clock,
    worker_python: str = sys.executable,
    run_nonce: str | None = None,
    execution_hooks: BrokerLifecycleExecutionHooks | None = None,
    health_policy: BrokerHostHealthPolicyV1 | None = None,
) -> BrokerLifecycleResultV1:
    """Run one explicitly authorized finite capture; never activate implicitly.

    Local filesystem and Python runtime are trusted. No OS sandbox, grants,
    credential policy or source-continuity certification is provided here.
    """
    if os.name != "posix":
        raise BrokerLifecycleError(Reason.INVALID_REQUEST)
    from histdatacom.broker_plugin_capabilities.execution import (
        _provider_call,
        _provider_request,
    )
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        read_current_provider_policy_context,
    )
    from histdatacom.broker_plugin_permissions.scope import (
        current_permission_authority,
        require_native_permissions,
    )
    from histdatacom.broker_plugin_permissions.decisions import (
        BrokerPermissionError,
    )

    request = _provider_request(plan, provider_request)
    _provider_call(request, capture=False)
    authority = current_permission_authority()
    from histdatacom.broker_plugin_permissions.scope import (
        check_permission_public_output,
    )

    original_guard = (
        None if execution_hooks is None else execution_hooks.before_persist
    )

    def check_public(text: str) -> None:
        check_permission_public_output(text)
        if original_guard is not None:
            original_guard(text)

    execution_hooks = replace(
        execution_hooks or BrokerLifecycleExecutionHooks(),
        before_persist=check_public,
    )
    try:
        if (
            os.name != "posix"
            or not isinstance(configuration, Mapping)
            or len(configuration) > 128
            or (
                run_nonce is not None
                and (
                    type(run_nonce) is not str
                    or re.fullmatch(r"[0-9a-f]{32}", run_nonce) is None
                )
            )
        ):
            raise ValueError
        ephemeral = dict(configuration)
        request.configuration_profile.verify_configuration(ephemeral)
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
            hashlib.sha256(
                json.dumps(
                    [
                        uuid.uuid4().hex if run_nonce is None else run_nonce,
                        authority.manifest.artifact_id,
                        authority.binding.artifact_id,
                        authority.grant_id,
                    ],
                    separators=(",", ":"),
                ).encode("ascii")
            ).hexdigest()[:32],
            platform.python_version(),
        )
        bootstrap = encode_frame(
            {
                "header": header.to_json(),
                "configuration": ephemeral,
                "provider_request": request.to_json(),
                "epoch": 0,
                "permissions": {
                    "manifest": authority.manifest.to_json(),
                    "binding": authority.binding.to_json(),
                    "grant_id": authority.grant_id,
                },
            },
            header.policy.frame_bytes,
        )
        if len(bootstrap) > header.policy.queue_bytes:
            raise ValueError
        # Refuse a ledger too large for this native transport before creating
        # any output or child. A later expansion is checked on each reply.
        encode_frame(
            {
                "type": "policy_context",
                "invocation_id": request.artifact_id,
                "epoch": len(header.policy.retry_delays_ms),
                "sequence": 2**63 - 1,
                "context": read_current_provider_policy_context().to_json(),
            },
            header.policy.frame_bytes,
        )
        encode_frame(
            {
                "type": "permission_reply",
                "ok": True,
                "invocation_id": request.artifact_id,
                "epoch": len(header.policy.retry_delays_ms),
                "sequence": 2**63 - 1,
                "payload": json.dumps(authority.read_context().to_json()),
            },
            header.policy.frame_bytes,
        )
        if execution_hooks is not None:
            if type(execution_hooks) is not BrokerLifecycleExecutionHooks:
                raise ValueError
            if execution_hooks.before_persist is not None:
                execution_hooks.before_persist(header.to_json())
                execution_hooks.before_persist(request.to_json())
                execution_hooks.before_persist(
                    read_current_provider_policy_context().to_json()
                )
    except (BrokerPolicyError, BrokerPermissionError):
        raise
    except Exception:
        raise BrokerLifecycleError(Reason.INVALID_REQUEST) from None
    try:
        if authorize(plan) is not True:
            raise ValueError
    except (Exception, SystemExit):
        raise BrokerLifecycleError(Reason.AUTHORIZATION) from None
    journal: Journal | None = None
    from histdatacom.broker_plugin_health import (
        BrokerHostHealthPolicyV1,
        BrokerHostHealthReason as HealthReason,
        make_lifecycle_health_header,
        replay_lifecycle_host_health,
    )
    from histdatacom.broker_plugin_health.collector import HostHealthRecorder
    from histdatacom.broker_plugin_health.storage import (
        HostHealthEvidenceWriter,
    )
    from histdatacom.broker_plugin_policy.health_bindings import (
        BrokerHostHealthEvidenceV1,
    )
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )
    from histdatacom.broker_plugin_permissions import build_permission_execution
    from .storage import replay_broker_lifecycle

    writer: HostHealthEvidenceWriter | None = None
    try:
        permission_context, permission_decision = authority.snapshot_admission()
        require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
        provider_decision = require_provider_operation(
            request, BrokerPolicyOperation.CAPTURE
        )
        started_utc, started_monotonic = clock()
        health_header = make_lifecycle_health_header(
            header,
            request,
            authority.manifest,
            permission_context,
            permission_decision,
            provider_decision,
            started_at_utc_ns=started_utc,
            started_at_monotonic_ns=started_monotonic,
            policy=health_policy or BrokerHostHealthPolicyV1(),
        )

        def authorize_health(artifact: object) -> None:
            require_native_permissions(request)
            # The journal also stores the reviewed native invocation/header,
            # not just the health-only projection. Reauthorize that complete
            # declared envelope before each metadata-file retention effect.
            require_provider_operation(
                request, BrokerPolicyOperation.RETAIN_LOCAL
            )
            require_provider_operation(
                BrokerHostHealthEvidenceV1(
                    request, header, health_header, artifact
                ),
                BrokerPolicyOperation.RETAIN_LOCAL,
            )

        authorize_health(health_header)
        journal = Journal(
            output_directory,
            header,
            provider_request=request,
            before_persist=(
                None
                if execution_hooks is None
                else execution_hooks.before_persist
            ),
        )
        writer = HostHealthEvidenceWriter(
            output_directory.with_name(output_directory.name + "-host-health"),
            health_header,
            {
                "invocation.json": request.to_json(),
                "native-header.json": header.to_json(),
                "permission-manifest.json": authority.manifest.to_json(),
                "permission-context.json": permission_context.to_json(),
                "permission-decision.json": permission_decision.to_json(),
                "provider-decision.json": provider_decision.to_json(),
            },
            authorize_artifact=authorize_health,
            guard_text=check_public,
        )
        recorder = HostHealthRecorder(health_header, clock, writer.append)
        run = _Run(journal, clock, execution_hooks, health=recorder)
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
            if reason is Reason.AUTHORIZATION:
                # V1 has no durable ACTIVE -> STOPPING policy-refusal edge.
                # Do not mutate that frozen state machine or create terminal
                # bytes after expiry: leave the inspectable OPEN native run.
                raise BrokerPolicyError("worker_provider_policy_refused")
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
        recorder.close(
            run.epoch,
            HealthReason.NONE if complete else HealthReason.UNKNOWN_LOSS,
        )
        health = replay_lifecycle_host_health(
            health_header,
            recorder.observations,
            manifest,
            replay_broker_lifecycle(output_directory, provider_request=request),
            request,
            authority.manifest,
            permission_context,
            permission_decision,
            provider_decision,
        )
        permissions = build_permission_execution(request, manifest, authority)
        writer.finish(health, permission_execution=permissions)
        return BrokerLifecycleResultV1(
            output_directory,
            manifest,
            reason,
            tuple(pids),
            health,
            writer.directory,
            permissions,
        )
    except (BrokerLifecycleError, BrokerPolicyError):
        raise
    except (Exception, SystemExit):
        raise BrokerLifecycleError(Reason.PERSISTENCE) from None
    finally:
        ephemeral.clear()
        if journal is not None:
            journal.close()
        if writer is not None:
            writer.close()

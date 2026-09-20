"""Private supervised process entry point. No provider exceptions are echoed."""

from __future__ import annotations

from collections import deque
import os
import json
import select
import sys
import threading
import time
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from histdatacom.broker_plugin_security.secrets import (
        BrokerPrivateMaterialGuard,
    )

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityError,
    BrokerCapabilityReason,
    invoke_authorized_installed_broker_plugin,
)

from .contracts import (
    BrokerLifecycleHeaderV1,
    BrokerLifecycleIdentityV1,
    BrokerLifecycleSessionV1,
    BrokerLifecycleError,
    BrokerLifecycleReason as Reason,
)
from .ipc import FrameDecoder, write_frame


class Controls:
    def __init__(self) -> None:
        self.decoder = FrameDecoder()
        self.pending: deque[dict[str, object]] = deque()

    def receive(self, timeout: float) -> dict[str, object] | None:
        deadline = time.monotonic() + timeout
        while not self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([0], [], [], remaining)[0]:
                return None
            data = os.read(0, 65_536)
            if not data:
                raise BrokerLifecycleError(Reason.WORKER_DIED)
            for value, _ in self.decoder.feed(data):
                if len(self.pending) >= 128:
                    raise BrokerLifecycleError(Reason.QUEUE_SATURATED)
                self.pending.append(value)
        return self.pending.popleft()


def main(*, secret_fields: tuple[str, ...] | None = None) -> None:
    descriptor = int(sys.argv[1])
    parent_pid = os.getppid()

    def watch_parent() -> None:
        # Lifecycle cleanup after abrupt coordinator death, not hostile-code
        # containment. This thread never touches SDK objects or IPC messages.
        while os.getppid() == parent_pid:
            time.sleep(0.05)
        os._exit(71)

    threading.Thread(target=watch_parent, daemon=True).start()
    controls = Controls()
    started = False
    private_guard: BrokerPrivateMaterialGuard | None = None
    security_refused = False

    def emit(frame: dict[str, object], maximum: int) -> None:
        if private_guard is not None:
            private_guard.check(json.dumps(frame, ensure_ascii=True))
        write_frame(descriptor, frame, maximum)

    try:
        startup = controls.receive(60)
        if (
            startup is None
            or set(startup) != {"header", "configuration"}
            or type(startup["header"]) is not str
            or type(startup["configuration"]) is not dict
        ):
            raise ValueError
        header = BrokerLifecycleHeaderV1.from_json(startup["header"])
        policy = header.policy
        configuration = cast(dict[str, object], startup["configuration"])
        if secret_fields is not None:
            from histdatacom.broker_plugin_security.secrets import (
                BrokerPrivateMaterialGuard,
            )

            private_guard = BrokerPrivateMaterialGuard(
                tuple(cast(str, configuration[name]) for name in secret_fields)
            )
        plugin = invoke_authorized_installed_broker_plugin(
            header.inventory, header.plan, authorize=lambda _: True
        )
        identity = BrokerLifecycleIdentityV1(
            plugin.binding, plugin.metadata, plugin.configuration_schema
        )
        if secret_fields is not None:
            declared = {
                field.name
                for field in identity.configuration_schema.fields
                if field.secret
            }
            # The host must explicitly classify every supplied secret field;
            # neither ordinary configuration nor a plugin can reclassify it.
            if set(secret_fields) != declared.intersection(configuration):
                security_refused = True
                raise ValueError
        identity.configuration_schema.validate_configuration(configuration)
        emit(
            {"type": "identity", "payload": identity.to_json()},
            policy.frame_bytes,
        )
        session = plugin.open_session(configuration)
        instruments = plugin.instruments() if header.symbols else ()
        if header.symbols:
            plugin.subscribe(header.symbols)
        emit(
            {
                "type": "session",
                "payload": BrokerLifecycleSessionV1(
                    session, instruments
                ).to_json(),
            },
            policy.frame_bytes,
        )
        started = True
        stopped = False
        for delivery, event in enumerate(
            plugin.iter_events(max_events=policy.max_events)
        ):
            frame: dict[str, object] = {
                "type": "event",
                "delivery": delivery,
                "payload": event.to_json(),
            }
            acknowledged = False
            for attempt in range(policy.delivery_retries + 1):
                emit(frame, policy.frame_bytes)
                deadline = (
                    time.monotonic() + policy.acknowledgement_timeout_ms / 1000
                )
                while time.monotonic() < deadline:
                    control = controls.receive(
                        max(0, deadline - time.monotonic())
                    )
                    if control is None:
                        break
                    if control == {"type": "stop"}:
                        stopped = True
                        break
                    if (
                        set(control) != {"type", "delivery"}
                        or control["type"] != "ack"
                        or type(control["delivery"]) is not int
                        or control["delivery"] > delivery
                    ):
                        raise ValueError
                    if control["delivery"] == delivery:
                        acknowledged = True
                        break
                if stopped or acknowledged:
                    break
            if stopped:
                break
            if not acknowledged:
                raise BrokerLifecycleError(Reason.WORKER_DIED)
        if not stopped:
            emit({"type": "eof"}, policy.frame_bytes)
        if header.symbols:
            plugin.unsubscribe(header.symbols)
        plugin.close_session()
        emit({"type": "closed"}, policy.frame_bytes)
    except BaseException as error:
        reason = Reason.PLUGIN_FAILURE
        if security_refused or (
            private_guard is not None and private_guard.refused
        ):
            reason = Reason.INTEGRITY
        if started and isinstance(error, BrokerCapabilityError):
            if error.reason is BrokerCapabilityReason.RESOURCE_LIMIT:
                reason = Reason.EVENT_LIMIT
            elif error.reason is BrokerCapabilityReason.CAPABILITY_VIOLATION:
                reason = Reason.MALFORMED_EVENT
        try:
            write_frame(descriptor, {"type": "failure", "reason": reason.value})
        except BaseException:
            pass
        # No traceback/configuration/provider message crosses the boundary.
        os._exit(70)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    main()

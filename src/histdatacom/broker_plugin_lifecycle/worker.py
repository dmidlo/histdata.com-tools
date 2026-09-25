"""Private supervised process entry point. No provider exceptions are echoed."""

from __future__ import annotations

from collections import deque
from contextlib import ExitStack
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
from histdatacom.broker_plugin_policy.contracts import BrokerPolicyContextV1

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
        self.acknowledged_delivery = -1

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


class _PolicySource:
    """One fresh parent-ledger exchange per local guard, never a permit cache.

    The parent owns the changing review ledger. A returned context grants no
    operation: the child's ordinary native resolver and current-clock policy
    check still run. Revocation is checked before each call, not during an
    already executing/blocking provider call.
    """

    def __init__(
        self,
        controls: Controls,
        descriptor: int,
        invocation_id: str,
        epoch: int,
        maximum: int,
        timeout: float,
    ) -> None:
        self.controls = controls
        self.descriptor = descriptor
        self.invocation_id = invocation_id
        self.epoch = epoch
        self.maximum = maximum
        self.timeout = timeout
        self.sequence = 0

    def read_policy_context(self) -> BrokerPolicyContextV1:
        if self.sequence >= 2**63 - 1:
            raise BrokerLifecycleError(Reason.FRAME_LIMIT)
        coordinates: dict[str, object] = {
            "invocation_id": self.invocation_id,
            "epoch": self.epoch,
            "sequence": self.sequence,
        }
        write_frame(
            self.descriptor,
            {"type": "policy_context_request", **coordinates},
            self.maximum,
        )
        deadline = time.monotonic() + self.timeout
        reply = self.controls.receive(self.timeout)
        # Retransmitted deliveries can leave duplicate already-consumed ACKs
        # in the control pipe. They are not policy responses or new permits.
        while (
            reply is not None
            and set(reply) == {"type", "delivery"}
            and reply["type"] == "ack"
            and type(reply["delivery"]) is int
            and 0 <= reply["delivery"] <= self.controls.acknowledged_delivery
        ):
            reply = self.controls.receive(max(0, deadline - time.monotonic()))
        if reply == {"type": "stop"}:
            raise BrokerLifecycleError(Reason.CANCELLED)
        if (
            reply is None
            or set(reply) != {"type", "context", *coordinates}
            or reply["type"] != "policy_context"
            or type(reply["context"]) is not str
            or type(reply["epoch"]) is not int
            or type(reply["sequence"]) is not int
            or any(reply[key] != value for key, value in coordinates.items())
        ):
            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
        context = BrokerPolicyContextV1.from_json(reply["context"])
        if context.to_json() != reply["context"]:
            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
        self.sequence += 1
        return context


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
    scopes = ExitStack()
    plugin = None
    exit_status = 0

    def emit(frame: dict[str, object], maximum: int) -> None:
        if private_guard is not None:
            private_guard.check(json.dumps(frame, ensure_ascii=True))
        write_frame(descriptor, frame, maximum)

    try:
        startup = controls.receive(60)
        if (
            startup is None
            or set(startup)
            != {
                "header",
                "configuration",
                "provider_request",
                "epoch",
                "permissions",
            }
            or type(startup["header"]) is not str
            or type(startup["configuration"]) is not dict
            or type(startup["provider_request"]) is not str
            or type(startup["epoch"]) is not int
            or startup["epoch"] < 0
        ):
            raise ValueError
        header = BrokerLifecycleHeaderV1.from_json(startup["header"])
        policy = header.policy
        controls.decoder.maximum = policy.frame_bytes
        if startup["epoch"] > len(policy.retry_delays_ms):
            raise ValueError
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )
        from histdatacom.broker_plugin_policy.scope import provider_policy_scope

        provider_request = BrokerSDKInvocationV1.from_json(
            startup["provider_request"]
        )
        if provider_request.plan.to_json() != header.plan.to_json():
            raise ValueError
        from histdatacom.broker_plugin_permissions.contracts import (
            BrokerPermissionManifestV1,
            BrokerPermissionBindingV1,
        )
        from histdatacom.broker_plugin_permissions.decisions import (
            BrokerPermissionAuthorityV1,
        )
        from histdatacom.broker_plugin_permissions.scope import (
            broker_permission_scope,
        )
        from histdatacom.broker_plugin_permissions.worker import (
            PermissionChannel,
            WorkerPermissionSource,
            WorkerHostResources,
        )

        permissions = startup["permissions"]
        if (
            type(permissions) is not dict
            or set(permissions) != {"manifest", "binding", "grant_id"}
            or any(type(item) is not str for item in permissions.values())
        ):
            raise ValueError
        channel = PermissionChannel(
            controls,
            descriptor,
            provider_request.artifact_id,
            startup["epoch"],
            policy.frame_bytes,
            policy.startup_timeout_ms / 1000,
        )
        permission_manifest = BrokerPermissionManifestV1.from_json(
            permissions["manifest"]
        )
        permission_authority = BrokerPermissionAuthorityV1(
            permission_manifest,
            BrokerPermissionBindingV1.from_json(permissions["binding"]),
            permissions["grant_id"],
            WorkerPermissionSource(channel),
        )
        scopes.enter_context(
            broker_permission_scope(
                permission_authority,
                resources=(
                    WorkerHostResources(channel)
                    if permission_manifest.resource_abi == "host_resources_v1"
                    else None
                ),
            )
        )
        scopes.enter_context(
            provider_policy_scope(
                _PolicySource(
                    controls,
                    descriptor,
                    provider_request.artifact_id,
                    startup["epoch"],
                    policy.frame_bytes,
                    # Data ACK retry cadence is not the policy-control RPC
                    # budget. In particular a short deliberate ACK interval
                    # must not interrupt a fresh ledger exchange while the
                    # parent is durably appending an earlier record. Parent
                    # startup/run deadlines still terminate this bounded wait.
                    policy.startup_timeout_ms / 1000,
                )
            )
        )
        configuration = cast(dict[str, object], startup["configuration"])
        if secret_fields is not None:
            from histdatacom.broker_plugin_security.secrets import (
                BrokerPrivateMaterialGuard,
            )

            private_guard = BrokerPrivateMaterialGuard(
                tuple(cast(str, configuration[name]) for name in secret_fields)
            )
        plugin = invoke_authorized_installed_broker_plugin(
            header.inventory,
            header.plan,
            authorize=lambda _: True,
            provider_request=provider_request,
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
                        controls.acknowledged_delivery = delivery
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
        from histdatacom.broker_plugin_policy.scope import BrokerPolicyError
        from histdatacom.broker_plugin_permissions.decisions import (
            BrokerPermissionError,
        )

        reason = Reason.PLUGIN_FAILURE
        if isinstance(error, (BrokerPolicyError, BrokerPermissionError)):
            reason = Reason.AUTHORIZATION
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
        exit_status = 70
    finally:
        if plugin is not None:
            try:
                plugin.close_session()
            except BaseException:
                pass
        scopes.close()
        os.close(descriptor)
    if exit_status:
        os._exit(exit_status)


if __name__ == "__main__":
    main()

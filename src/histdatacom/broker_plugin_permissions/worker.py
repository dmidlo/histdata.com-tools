"""Private bounded fresh-ledger/resource RPC for supervised plugin workers."""

from __future__ import annotations

import base64
import threading
import time
from typing import NoReturn, Protocol

from histdatacom.broker_plugin_lifecycle.ipc import encode_frame, write_frame
from histdatacom.broker_plugins.resources import BrokerHostHTTPResponseV1

from .contracts import BrokerPermissionContextV1, validate_permission_atom
from .decisions import BrokerPermissionError
from ._wire import canonical_json, load_json


class _Controls(Protocol):
    acknowledged_delivery: int

    def receive(self, timeout: float) -> dict[str, object] | None: ...


class PermissionChannel:
    """One outstanding exchange, bound to invocation, epoch and sequence."""

    def __init__(
        self,
        controls: _Controls,
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
        self._lock = threading.Lock()
        self._owner = threading.get_ident()

    def exchange(self, operation: str, payload: dict[str, object]) -> object:
        if self._owner != threading.get_ident() or not self._lock.acquire(
            blocking=False
        ):
            raise BrokerPermissionError(
                "concurrent_or_reentrant_worker_resource"
            )
        try:
            return self._exchange(operation, payload)
        finally:
            self._lock.release()

    def _exchange(self, operation: str, payload: dict[str, object]) -> object:
        if self.sequence >= 2**63 - 1:
            raise BrokerPermissionError("worker_permission_sequence_limit")
        coordinates: dict[str, object] = {
            "invocation_id": self.invocation_id,
            "epoch": self.epoch,
            "sequence": self.sequence,
        }
        write_frame(
            self.descriptor,
            {
                "type": "permission_request",
                **coordinates,
                "operation": operation,
                "payload": canonical_json(payload),
            },
            self.maximum,
        )
        deadline = time.monotonic() + self.timeout
        reply = self.controls.receive(self.timeout)
        while (
            reply is not None
            and set(reply) == {"type", "delivery"}
            and reply["type"] == "ack"
            and type(reply["delivery"]) is int
            and 0 <= reply["delivery"] <= self.controls.acknowledged_delivery
        ):
            reply = self.controls.receive(max(0, deadline - time.monotonic()))
        if reply is not None:
            encode_frame(reply, self.maximum)
        if (
            reply is None
            or set(reply) != {"type", "payload", "ok", *coordinates}
            or reply["type"] != "permission_reply"
            or type(reply["ok"]) is not bool
            or type(reply["payload"]) is not str
            or type(reply["epoch"]) is not int
            or type(reply["sequence"]) is not int
            or any(reply[key] != value for key, value in coordinates.items())
        ):
            raise BrokerPermissionError("invalid_worker_permission_reply")
        self.sequence += 1
        if not reply["ok"]:
            raise BrokerPermissionError("parent_resource_refused")
        return load_json(reply["payload"])


class WorkerPermissionSource:
    def __init__(self, channel: PermissionChannel) -> None:
        self.channel = channel

    def read_context(self) -> BrokerPermissionContextV1:
        value = self.channel.exchange("context", {})
        if type(value) is not str:
            raise BrokerPermissionError("invalid_worker_permission_context")
        return BrokerPermissionContextV1.from_json(value)


class WorkerHostResources:
    """Only bounded parent RPC; no secret provider or transport in the worker."""

    def __init__(self, channel: PermissionChannel) -> None:
        self.channel = channel

    def available_permissions(self) -> tuple[str, ...]:
        result = self.channel.exchange("available", {})
        if type(result) is not list or any(
            type(item) is not str for item in result
        ):
            raise BrokerPermissionError("invalid_worker_permission_inventory")
        if len(result) > 128 or result != sorted(set(result)):
            raise BrokerPermissionError("invalid_worker_permission_inventory")
        for atom in result:
            validate_permission_atom(atom)
        return tuple(result)

    def request(
        self,
        endpoint_id: str,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        secret_profile: str | None = None,
    ) -> BrokerHostHTTPResponseV1:
        if type(body) is not bytes or len(body) > self.channel.maximum:
            raise BrokerPermissionError("worker_resource_body_bound")
        result = self.channel.exchange(
            "request",
            {
                "endpoint_id": endpoint_id,
                "method": method,
                "path": path,
                "body": base64.b64encode(body).decode("ascii"),
                "secret_profile": secret_profile,
            },
        )
        if type(result) is not dict or set(result) != {"status", "body"}:
            raise BrokerPermissionError("invalid_worker_resource_response")
        return BrokerHostHTTPResponseV1(
            result["status"], base64.b64decode(result["body"], validate=True)
        )

    def put_cache(self, cache_id: str, key: str, value: bytes) -> None:
        if type(value) is not bytes or len(value) > self.channel.maximum:
            raise BrokerPermissionError("worker_cache_item_bound")
        result = self.channel.exchange(
            "put_cache",
            {
                "cache_id": cache_id,
                "key": key,
                "value": base64.b64encode(value).decode("ascii"),
            },
        )
        if result is not None:
            raise BrokerPermissionError("invalid_worker_cache_response")

    def get_cache(self, cache_id: str, key: str) -> bytes | None:
        result = self.channel.exchange(
            "get_cache", {"cache_id": cache_id, "key": key}
        )
        if result is None:
            return None
        if type(result) is not str:
            raise BrokerPermissionError("invalid_worker_cache_response")
        return base64.b64decode(result, validate=True)

    def request_subprocess(self, operation: str) -> NoReturn:
        raise BrokerPermissionError("isolated_subprocess_resource_unsupported")

"""Bounded length-framed JSON, never pickle or unbounded line reads."""

from __future__ import annotations

from collections.abc import Iterator
import json
import math
import os
import struct

from .contracts import (
    MAX_LIFECYCLE_BYTES,
    BrokerLifecycleError,
    BrokerLifecycleReason as Reason,
)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
        result[key] = value
    return result


def _shape(
    value: object, depth: int = 0, budget: list[int] | None = None
) -> None:
    if budget is None:
        budget = [MAX_LIFECYCLE_BYTES]
    budget[0] -= 1 + (len(value) if type(value) is str else 0)
    if depth > 32 or budget[0] < 0:
        raise BrokerLifecycleError(Reason.FRAME_LIMIT)
    if type(value) is dict:
        if len(value) > 128 or any(
            type(key) is not str or len(key) > 128 for key in value
        ):
            raise BrokerLifecycleError(Reason.FRAME_LIMIT)
        for item in value.values():
            _shape(item, depth + 1, budget)
    elif type(value) is list:
        if len(value) > 128:
            raise BrokerLifecycleError(Reason.FRAME_LIMIT)
        for item in value:
            _shape(item, depth + 1, budget)
    elif type(value) is float:
        if not math.isfinite(value):
            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
    elif type(value) is int:
        if not -(2**63) <= value < 2**63:
            raise BrokerLifecycleError(Reason.MALFORMED_IPC)
    elif value is not None and type(value) not in (str, bool):
        raise BrokerLifecycleError(Reason.MALFORMED_IPC)


def encode_frame(
    value: dict[str, object], maximum: int = MAX_LIFECYCLE_BYTES
) -> bytes:
    try:
        _shape(value)
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        if not 1 <= len(encoded) <= maximum:
            raise BrokerLifecycleError(Reason.FRAME_LIMIT)
        return struct.pack("!I", len(encoded)) + encoded
    except BrokerLifecycleError:
        raise
    except Exception:
        raise BrokerLifecycleError(Reason.MALFORMED_IPC) from None


class FrameDecoder:
    def __init__(self, maximum: int = MAX_LIFECYCLE_BYTES) -> None:
        self.maximum = maximum
        self.buffer = bytearray()
        self.expected: int | None = None

    def feed(self, data: bytes) -> Iterator[tuple[dict[str, object], int]]:
        # Caller reads at most 64KiB; split messages without retaining a batch.
        if len(data) > 65_536:
            raise BrokerLifecycleError(Reason.FRAME_LIMIT)
        view = memoryview(data)
        while view:
            target = 4 if self.expected is None else self.expected
            take = min(target - len(self.buffer), len(view))
            self.buffer.extend(view[:take])
            view = view[take:]
            if len(self.buffer) < target:
                continue
            if self.expected is None:
                self.expected = struct.unpack("!I", self.buffer)[0]
                self.buffer.clear()
                if not 1 <= self.expected <= self.maximum:
                    raise BrokerLifecycleError(Reason.FRAME_LIMIT)
            else:
                size = self.expected
                try:
                    value = json.loads(self.buffer, object_pairs_hook=_pairs)
                    if type(value) is not dict:
                        raise ValueError
                    _shape(value)
                except BrokerLifecycleError:
                    raise
                except Exception:
                    raise BrokerLifecycleError(Reason.MALFORMED_IPC) from None
                self.buffer.clear()
                self.expected = None
                yield value, size + 4

    def finish(self) -> None:
        if self.buffer or self.expected is not None:
            raise BrokerLifecycleError(Reason.MALFORMED_IPC)


def write_frame(
    descriptor: int,
    value: dict[str, object],
    maximum: int = MAX_LIFECYCLE_BYTES,
) -> None:
    data = memoryview(encode_frame(value, maximum))
    while data:
        written = os.write(descriptor, data)
        if written <= 0:
            raise BrokerLifecycleError(Reason.WORKER_DIED)
        data = data[written:]

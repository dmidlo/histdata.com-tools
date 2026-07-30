"""Adapter and consumer seams for live and replayed broker capture evidence."""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from histdatacom.broker_capture.contracts import (
    BrokerAdapterMessageV1,
    BrokerCaptureEventKind,
    BrokerCaptureEventV1,
    BrokerCaptureSessionV1,
)


@runtime_checkable
class BrokerCaptureAdapterV1(Protocol):
    """Credential-opaque source of public broker messages.

    A real implementation owns its private connection configuration.  The
    collector deliberately reads only these public identifiers and the
    message iterator, so credentials cannot leak through contract reflection.
    """

    @property
    def adapter_id(self) -> str:
        """Return the public adapter identifier."""

    @property
    def adapter_version(self) -> str:
        """Return the adapter semantic version."""

    def iter_messages(self) -> Iterable[BrokerAdapterMessageV1]:
        """Yield public messages in adapter-observed order."""


@runtime_checkable
class BrokerCaptureClockV1(Protocol):
    """Clock seam sampled at the collector boundary for every message."""

    def sample(self) -> tuple[int, int]:
        """Return ``(UTC wall time ns, monotonic receive time ns)``."""


@runtime_checkable
class BrokerCaptureEventSourceV1(Protocol):
    """Common live/replay source consumed by downstream fingerprinting."""

    @property
    def session_id(self) -> str:
        """Return the capture session identity."""

    def iter_events(self) -> Iterable[BrokerCaptureEventV1]:
        """Yield collector-stamped events in capture order."""


@runtime_checkable
class BrokerCaptureEventSinkV1(Protocol):
    """Append-only persistence sink for captured events."""

    def append(self, event: BrokerCaptureEventV1) -> None:
        """Persist one event or fail before acknowledging it."""


@runtime_checkable
class BrokerCaptureEventConsumerV1(Protocol):
    """Identical downstream interface used for live and replay sources."""

    def on_event(self, event: BrokerCaptureEventV1) -> None:
        """Consume one verified capture event."""


@dataclass(frozen=True, slots=True)
class BrokerCaptureConsumeResultV1:
    """Small in-memory result from driving a source through consumers."""

    session_id: str
    event_count: int
    event_kind_counts: dict[str, int]
    first_capture_sequence: int | None
    last_capture_sequence: int | None


class SystemBrokerCaptureClockV1:
    """Production clock implementation using Python nanosecond clocks."""

    def sample(self) -> tuple[int, int]:
        """Return one adjacent wall/monotonic sample pair."""
        return time.time_ns(), time.monotonic_ns()


@dataclass(slots=True)
class SequenceBrokerCaptureClockV1:
    """Deterministic clock fixture for drift, burst, and quiet-gap tests."""

    samples: Sequence[tuple[int, int]]
    _index: int = 0

    def sample(self) -> tuple[int, int]:
        """Return the next fixture sample and fail when underspecified."""
        if self._index >= len(self.samples):
            raise RuntimeError("broker capture fixture clock is exhausted")
        value = self.samples[self._index]
        self._index += 1
        return value


@dataclass(frozen=True, slots=True)
class SequenceBrokerCaptureAdapterV1:
    """Deterministic public adapter used by synthetic capture fixtures."""

    adapter_id: str
    adapter_version: str
    messages: tuple[BrokerAdapterMessageV1, ...]

    def iter_messages(self) -> Iterable[BrokerAdapterMessageV1]:
        """Yield configured fixture messages without mutation."""
        return iter(self.messages)


@dataclass(frozen=True, slots=True)
class LiveBrokerCaptureSourceV1:
    """Collector-side source that stamps an adapter stream with two clocks."""

    session: BrokerCaptureSessionV1
    adapter: BrokerCaptureAdapterV1
    clock: BrokerCaptureClockV1
    clock_correction_threshold_ns: int = 5_000_000

    def __post_init__(self) -> None:
        if self.adapter.adapter_id != self.session.adapter_id:
            raise ValueError("adapter_id does not match capture session")
        if self.adapter.adapter_version != self.session.adapter_version:
            raise ValueError("adapter_version does not match capture session")
        if (
            isinstance(self.clock_correction_threshold_ns, bool)
            or not isinstance(self.clock_correction_threshold_ns, int)
            or self.clock_correction_threshold_ns <= 0
        ):
            raise ValueError("clock correction threshold must be positive")

    @property
    def session_id(self) -> str:
        """Return the capture session identity."""
        return self.session.session_id

    def iter_events(self) -> Iterable[BrokerCaptureEventV1]:
        """Yield ordered events and explicit wall-clock correction evidence."""
        return self._event_iterator()

    def _event_iterator(self) -> Iterator[BrokerCaptureEventV1]:
        sequence = 0
        previous_wall: int | None = None
        previous_monotonic: int | None = None
        for message in self.adapter.iter_messages():
            if not isinstance(message, BrokerAdapterMessageV1):
                raise TypeError("broker adapter yielded a non-contract message")
            if message.kind is BrokerCaptureEventKind.CLOCK_CORRECTION:
                raise ValueError(
                    "broker adapters cannot emit collector clock corrections"
                )
            wall, monotonic = self.clock.sample()
            _validate_clock_sample(wall, monotonic)
            if (
                previous_monotonic is not None
                and monotonic < previous_monotonic
            ):
                raise ValueError("collector monotonic clock moved backwards")
            if previous_wall is not None and previous_monotonic is not None:
                offset_change = (wall - previous_wall) - (
                    monotonic - previous_monotonic
                )
                if abs(offset_change) >= self.clock_correction_threshold_ns:
                    correction_message = BrokerAdapterMessageV1(
                        kind=BrokerCaptureEventKind.CLOCK_CORRECTION,
                        source_event_time_ns=message.source_event_time_ns,
                        source_timestamp_semantics=(
                            message.source_timestamp_semantics
                        ),
                        source_timestamp_precision_ns=(
                            message.source_timestamp_precision_ns
                        ),
                        reason_code="wall_monotonic_divergence",
                        public_metadata={
                            "basis": "wall_delta_minus_monotonic_delta",
                            "threshold_ns": self.clock_correction_threshold_ns,
                        },
                    )
                    yield BrokerCaptureEventV1(
                        session_id=self.session.session_id,
                        capture_sequence=sequence,
                        receive_time_utc_ns=wall,
                        receive_time_monotonic_ns=monotonic,
                        message=correction_message,
                        clock_offset_change_ns=offset_change,
                    )
                    sequence += 1
            yield BrokerCaptureEventV1(
                session_id=self.session.session_id,
                capture_sequence=sequence,
                receive_time_utc_ns=wall,
                receive_time_monotonic_ns=monotonic,
                message=message,
            )
            sequence += 1
            previous_wall = wall
            previous_monotonic = monotonic


def consume_broker_capture_source(
    source: BrokerCaptureEventSourceV1,
    *,
    sink: BrokerCaptureEventSinkV1 | None = None,
    consumers: Sequence[BrokerCaptureEventConsumerV1] = (),
) -> BrokerCaptureConsumeResultV1:
    """Drive either a live or replay source through the identical interface."""
    expected_sequence: int | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None
    counts: dict[str, int] = {}
    event_count = 0
    for event in source.iter_events():
        if event.session_id != source.session_id:
            raise ValueError("capture source yielded another session")
        if expected_sequence is None:
            expected_sequence = event.capture_sequence
            first_sequence = event.capture_sequence
        if event.capture_sequence != expected_sequence:
            raise ValueError("capture source sequence is not contiguous")
        if sink is not None:
            sink.append(event)
        for consumer in consumers:
            consumer.on_event(event)
        counts[event.kind.value] = counts.get(event.kind.value, 0) + 1
        event_count += 1
        last_sequence = event.capture_sequence
        expected_sequence += 1
    return BrokerCaptureConsumeResultV1(
        session_id=source.session_id,
        event_count=event_count,
        event_kind_counts=dict(sorted(counts.items())),
        first_capture_sequence=first_sequence,
        last_capture_sequence=last_sequence,
    )


def _validate_clock_sample(wall: int, monotonic: int) -> None:
    for name, value in (
        ("receive_time_utc_ns", wall),
        ("receive_time_monotonic_ns", monotonic),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
        if value < 0 or value > 2**63 - 1:
            raise ValueError(f"{name} is outside non-negative int64 range")


__all__ = [
    "BrokerCaptureAdapterV1",
    "BrokerCaptureClockV1",
    "BrokerCaptureConsumeResultV1",
    "BrokerCaptureEventConsumerV1",
    "BrokerCaptureEventSinkV1",
    "BrokerCaptureEventSourceV1",
    "LiveBrokerCaptureSourceV1",
    "SequenceBrokerCaptureAdapterV1",
    "SequenceBrokerCaptureClockV1",
    "SystemBrokerCaptureClockV1",
    "consume_broker_capture_source",
]

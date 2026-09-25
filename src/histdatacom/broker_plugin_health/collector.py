"""Host-only observation collection at ingress, queue and durable boundaries."""

from __future__ import annotations

from collections.abc import Callable

from .contracts import (
    BrokerHostHealthHeaderV1,
    BrokerHostHealthObservationV1,
)
from .contracts import (
    BrokerHostHealthObservationKind as Kind,
)
from .contracts import (
    BrokerHostHealthReason as Reason,
)

MAX_OBSERVATION_BYTES = 64 * 1024 * 1024


class HostHealthRecorder:
    """Bounded append-only observations; never accepts plugin health counters.

    The supplied clock and optional persistence callback are trusted host
    dependencies. A failed persistence callback aborts collection; it cannot
    produce a complete audit. Native bytes are independently replayed later.
    """

    def __init__(
        self,
        header: BrokerHostHealthHeaderV1,
        clock: Callable[[], tuple[int, int]],
        on_observation: (
            Callable[[BrokerHostHealthObservationV1], None] | None
        ) = None,
    ) -> None:
        self.header = BrokerHostHealthHeaderV1.from_json(header.to_json())
        self._clock = clock
        self._on_observation = on_observation
        self._observations: list[BrokerHostHealthObservationV1] = []
        self._ingress = 0
        self._bytes = 0
        self._closed = False
        self._last_monotonic = header.started_at_monotonic_ns

    @property
    def observations(self) -> tuple[BrokerHostHealthObservationV1, ...]:
        return tuple(self._observations)

    def _append(
        self,
        kind: Kind,
        epoch: int,
        *,
        ingress_sequence: int | None = None,
        event_id: str | None = None,
        native_record_id: str | None = None,
        queue_items: int | None = None,
        reason: Reason = Reason.NONE,
        sample: tuple[int, int] | None = None,
    ) -> BrokerHostHealthObservationV1:
        if (
            self._closed
            or len(self._observations) >= self.header.policy.max_observations
        ):
            raise ValueError("host health observation bound or closed recorder")
        utc, monotonic = self._clock() if sample is None else sample
        if monotonic < self._last_monotonic:
            raise ValueError("host health monotonic clock regressed")
        observation = BrokerHostHealthObservationV1(
            len(self._observations),
            kind,
            epoch,
            utc,
            monotonic,
            ingress_sequence,
            event_id,
            native_record_id,
            queue_items,
            reason,
        )
        size = len(observation.to_json().encode("ascii")) + 1
        if self._bytes + size > MAX_OBSERVATION_BYTES:
            raise ValueError("host health retained observation byte bound")
        if self._on_observation is not None:
            self._on_observation(observation)
        self._observations.append(observation)
        self._bytes += size
        self._last_monotonic = monotonic
        return observation

    def ingress(
        self,
        event_id: str | None,
        epoch: int,
        *,
        malformed: bool = False,
        sample: tuple[int, int] | None = None,
    ) -> int:
        sequence = self._ingress
        self._append(
            Kind.INGRESS,
            epoch,
            ingress_sequence=sequence,
            event_id=event_id,
            reason=Reason.MALFORMED if malformed else Reason.NONE,
            sample=sample,
        )
        self._ingress += 1
        return sequence

    def persisted(
        self,
        native_record_id: str,
        event_id: str | None,
        epoch: int,
        ingress_sequence: int | None = None,
    ) -> None:
        self._append(
            Kind.PERSISTED,
            epoch,
            ingress_sequence=ingress_sequence,
            event_id=event_id,
            native_record_id=native_record_id,
        )

    def refused(
        self,
        ingress_sequence: int,
        event_id: str | None,
        epoch: int,
        reason: Reason,
    ) -> None:
        self._append(
            Kind.REFUSED,
            epoch,
            ingress_sequence=ingress_sequence,
            event_id=event_id,
            reason=reason,
        )

    def queue(self, items: int, epoch: int, *, overflow: bool = False) -> None:
        if (
            type(items) is not int
            or not 0 <= items <= self.header.queue_capacity
        ):
            raise ValueError("host health queue outside actual capacity")
        self._append(
            Kind.QUEUE,
            epoch,
            queue_items=items,
            reason=Reason.QUEUE_OVERFLOW if overflow else Reason.NONE,
        )

    def close(self, epoch: int, reason: Reason = Reason.NONE) -> None:
        self._append(Kind.CLOSE, epoch, reason=reason)
        self._closed = True

"""Detached native projections. Only fresh native admission supplies evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._wire import Artifact, RationalV1, Record, digest, text


@dataclass(frozen=True, slots=True)
class NativeQuoteV1(Record):
    capture_id: str
    event_id: str
    record_id: str
    sequence: int
    epoch_id: str
    symbol: str
    source_time_ns: int | None
    source_precision_ns: int | None
    source_time_semantics: str
    receive_utc_ns: int
    receive_monotonic_ns: int
    clock_id: str
    bid: RationalV1
    ask: RationalV1
    price_basis: str
    source_sequence: str | None
    source_message_id: str | None
    source_batch_id: str | None
    health_state: str
    health_reasons: tuple[str, ...]
    health_bucket_ids: tuple[str, ...] = ()
    native_sha256: str = "0" * 64
    prefix_health_observation_id: str | None = None

    def _validate(self) -> None:
        for value in (
            self.capture_id,
            self.event_id,
            self.record_id,
            self.epoch_id,
            self.symbol,
            self.source_time_semantics,
            self.clock_id,
            self.price_basis,
            self.health_state,
        ):
            text(value)
        digest(self.native_sha256)
        if self.prefix_health_observation_id is not None:
            text(self.prefix_health_observation_id)
        if (
            min(self.sequence, self.receive_utc_ns, self.receive_monotonic_ns)
            < 0
        ):
            raise ValueError("negative native host coordinate")
        if self.source_time_ns is None:
            if self.source_precision_ns is not None:
                raise ValueError("precision without source clock")
        elif self.source_precision_ns is None or self.source_precision_ns < 1:
            raise ValueError("source clock requires positive precision")
        if not 0 < self.bid.value <= self.ask.value:
            raise ValueError("invalid native quote prices")
        for values in (self.health_reasons, self.health_bucket_ids):
            if values != tuple(sorted(set(values))):
                raise ValueError(
                    "native health references must be sorted unique"
                )


@dataclass(frozen=True, slots=True)
class NativeCaptureV1(Artifact):
    root_id: str
    manifest_id: str
    health_audit_id: str
    health_state: str
    health_reasons: tuple[str, ...]
    quotes: tuple[NativeQuoteV1, ...]
    control_digest: str
    control_count: int
    source_family: str = "synthetic_kernel_input"
    KIND: ClassVar[str] = "native-capture"

    def _validate(self) -> None:
        for value in (
            self.root_id,
            self.manifest_id,
            self.health_audit_id,
            self.health_state,
            self.source_family,
        ):
            text(value)
        digest(self.control_digest)
        if self.control_count < 0 or len(self.quotes) > 2048:
            raise ValueError("native projection resource bound")
        if self.health_reasons != tuple(sorted(set(self.health_reasons))):
            raise ValueError("native reasons must be sorted unique")
        sequences = tuple(q.sequence for q in self.quotes)
        if sequences != tuple(sorted(set(sequences))):
            raise ValueError("native quotes require unique host sequence order")
        if len({q.event_id for q in self.quotes}) != len(self.quotes):
            raise ValueError("duplicate native event identity")
        if any(q.capture_id != self.root_id for q in self.quotes):
            raise ValueError("quote belongs to a foreign native capture")

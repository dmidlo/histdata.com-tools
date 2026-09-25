"""Closed host-observed evidence. These artifacts are not provider truth."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from ._wire import Artifact, canonical_health_json


class BrokerHostHealthNativeFamily(str, Enum):
    LIFECYCLE_V1 = "lifecycle_v1"
    LEGACY_CAPTURE_V1 = "legacy_capture_v1"


class BrokerHostHealthObservationKind(str, Enum):
    INGRESS = "ingress"
    PERSISTED = "persisted"
    REFUSED = "refused"
    QUEUE = "queue"
    CLOSE = "close"


class BrokerHostHealthReason(str, Enum):
    NONE = "none"
    MALFORMED = "malformed_event"
    CAPABILITY = "capability_refused"
    PERMISSION = "permission_refused"
    PROVIDER_POLICY = "provider_policy_refused"
    QUEUE_OVERFLOW = "queue_overflow"
    DUPLICATE_DELIVERY = "duplicate_delivery"
    PERSISTENCE = "persistence_failure"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    WORKER_FAILURE = "worker_failure"
    UNKNOWN_LOSS = "unknown_loss"


class BrokerHostHealthState(str, Enum):
    QUALIFIED = "qualified_host_boundary_only"
    DEGRADED = "degraded"
    INSUFFICIENT = "insufficient_evidence"
    UNAVAILABLE = "historical_observations_unavailable"


class BrokerHostHealthFailure(str, Enum):
    NO_EVENTS = "no_canonical_events"
    INCOMPLETE = "observation_or_native_capture_incomplete"
    QUEUE_UNOBSERVED = "queue_not_observed"
    UPSTREAM_UNKNOWN = "upstream_loss_unidentified"
    HOST_DROP = "known_host_drop_rate"
    DUPLICATE = "duplicate_rate"
    STALE = "stale_rate"
    REORDER = "source_timestamp_reordering"
    PERSISTENCE_LAG = "receive_to_persist_p95"
    HEARTBEAT_GAP = "heartbeat_gap"
    QUEUE_SATURATION = "queue_saturation"
    CLOCK_JUMP = "clock_discontinuity"
    PLUGIN_CLAIM = "plugin_healthy_claim_disagrees"
    MALFORMED = "malformed_or_refused_event"
    SOURCE_GAP = "source_gap_or_reconnect"


def _count(value: int, maximum: int = 2**63 - 1, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid health integer")


def _identifier(value: str) -> None:
    if type(value) is not str or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,511}", value
    ):
        raise ValueError("invalid health public identity")


def _sha(value: str) -> None:
    if type(value) is not str or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("invalid health SHA-256")


def _symbol(value: str | None) -> None:
    if value is not None and (
        not re.fullmatch(r"[A-Z]{6}", value) or value[:3] == value[3:]
    ):
        raise ValueError("invalid health symbol")


def _finite(value: float) -> None:
    if type(value) is not float or not math.isfinite(value):
        raise ValueError("invalid finite health metric")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthPolicyV1(Artifact):
    """Versioned, explicit SLOs, never a measured provider quality promise."""

    version: str = "1.0.0"
    bucket_width_ns: int = 1_000_000_000
    max_observations: int = 1_000_000
    max_buckets: int = 4096
    max_samples_per_bucket: int = 8192
    minimum_events: int = 1
    max_known_drop_rate: float = 0.0
    max_duplicate_rate: float = 0.0
    max_stale_rate: float = 1.0
    max_persistence_p95_ns: int = 1_000_000_000
    max_heartbeat_gap_ns: int | None = None
    stale_after_ns: int = 1_000_000_000
    max_clock_jump_ns: int = 1_000_000_000
    max_queue_saturation_ns: int = 0
    max_reported_source_gap_events: int = 0
    max_reconnect_events: int = 0
    max_clock_correction_events: int = 0
    require_queue_observations: bool = True
    require_known_upstream_loss: bool = False
    stationary_window_declared: bool = False
    KIND: ClassVar[str] = "policy"

    def _validate(self) -> None:
        if not re.fullmatch(
            r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", self.version
        ):
            raise ValueError("health policy requires SemVer")
        _count(self.bucket_width_ns, 3_600_000_000_000, 1)
        _count(self.max_observations, 1_000_000, 1)
        _count(self.max_buckets, 4096, 1)
        _count(self.max_samples_per_bucket, 100_000, 1)
        _count(self.minimum_events, 1_000_000, 1)
        for value in (
            self.max_known_drop_rate,
            self.max_duplicate_rate,
            self.max_stale_rate,
        ):
            _finite(value)
            if not 0 <= value <= 1:
                raise ValueError("health rates must lie in [0,1]")
        for value in (
            self.max_persistence_p95_ns,
            self.stale_after_ns,
            self.max_clock_jump_ns,
            self.max_queue_saturation_ns,
        ):
            _count(value)
        if self.max_heartbeat_gap_ns is not None:
            _count(self.max_heartbeat_gap_ns, minimum=1)
        for value in (
            self.max_reported_source_gap_events,
            self.max_reconnect_events,
            self.max_clock_correction_events,
        ):
            _count(value, 1_000_000)


@dataclass(frozen=True, slots=True)
class BrokerHostHealthHeaderV1(Artifact):
    family: BrokerHostHealthNativeFamily
    capture_id: str
    plugin_id: str
    provider_id: str
    configuration_id: str
    permission_manifest_id: str | None
    permission_context_id: str | None
    permission_decision_id: str | None
    provider_decision_id: str
    symbols: tuple[str, ...]
    started_at_utc_ns: int
    started_at_monotonic_ns: int
    queue_capacity: int
    policy: BrokerHostHealthPolicyV1
    KIND: ClassVar[str] = "header"

    def _validate(self) -> None:
        for value in (
            self.capture_id,
            self.plugin_id,
            self.provider_id,
            self.configuration_id,
            self.provider_decision_id,
        ):
            _identifier(value)
        permissions = (
            self.permission_manifest_id,
            self.permission_context_id,
            self.permission_decision_id,
        )
        if self.family is BrokerHostHealthNativeFamily.LIFECYCLE_V1:
            if any(value is None for value in permissions):
                raise ValueError(
                    "SDK health needs retained permission evidence"
                )
        elif any(value is not None for value in permissions):
            raise ValueError("legacy capture has no SDK permission identity")
        for permission in permissions:
            if permission is not None:
                _identifier(permission)
        if (
            self.symbols != tuple(sorted(set(self.symbols)))
            or len(self.symbols) > 128
        ):
            raise ValueError("health symbols must be sorted unique and bounded")
        for symbol in self.symbols:
            _symbol(symbol)
        _count(self.started_at_utc_ns)
        _count(self.started_at_monotonic_ns)
        _count(self.queue_capacity, 1_000_000, 1)


@dataclass(frozen=True, slots=True)
class BrokerHostHealthObservationV1(Artifact):
    sequence: int
    kind: BrokerHostHealthObservationKind
    epoch: int
    utc_ns: int
    monotonic_ns: int
    ingress_sequence: int | None = None
    event_id: str | None = None
    native_record_id: str | None = None
    queue_items: int | None = None
    reason: BrokerHostHealthReason = BrokerHostHealthReason.NONE
    KIND: ClassVar[str] = "observation"

    def _validate(self) -> None:
        _count(self.sequence, 999_999)
        _count(self.epoch, 1_000_000)
        _count(self.utc_ns)
        _count(self.monotonic_ns)
        if self.ingress_sequence is not None:
            _count(self.ingress_sequence, 999_999)
        if self.queue_items is not None:
            _count(self.queue_items, 1_000_000)
        for value in (self.event_id, self.native_record_id):
            if value is not None:
                _identifier(value)
        kind = self.kind
        if kind is BrokerHostHealthObservationKind.INGRESS:
            if (
                self.ingress_sequence is None
                or self.native_record_id is not None
                or self.queue_items is not None
                or self.reason
                not in (
                    BrokerHostHealthReason.NONE,
                    BrokerHostHealthReason.MALFORMED,
                )
            ):
                raise ValueError("invalid ingress observation")
            if (self.event_id is None) != (
                self.reason is BrokerHostHealthReason.MALFORMED
            ):
                raise ValueError(
                    "unparseable ingress needs explicit malformed state"
                )
        elif kind is BrokerHostHealthObservationKind.PERSISTED:
            if (
                self.native_record_id is None
                or self.queue_items is not None
                or self.reason is not BrokerHostHealthReason.NONE
            ):
                raise ValueError("invalid persistence observation")
            if (self.ingress_sequence is None) != (self.event_id is None):
                raise ValueError("only host controls have no ingress")
        elif kind is BrokerHostHealthObservationKind.REFUSED:
            if (
                self.ingress_sequence is None
                or self.native_record_id is not None
                or self.queue_items is not None
                or self.reason is BrokerHostHealthReason.NONE
            ):
                raise ValueError("invalid refused ingress observation")
        elif kind is BrokerHostHealthObservationKind.QUEUE:
            if (
                self.queue_items is None
                or self.ingress_sequence is not None
                or self.event_id is not None
                or self.native_record_id is not None
                or self.reason
                not in (
                    BrokerHostHealthReason.NONE,
                    BrokerHostHealthReason.QUEUE_OVERFLOW,
                )
            ):
                raise ValueError("invalid queue observation")
        elif any(
            value is not None
            for value in (
                self.ingress_sequence,
                self.event_id,
                self.native_record_id,
                self.queue_items,
            )
        ):
            raise ValueError("close observation contains unrelated material")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthRateV1(Artifact):
    numerator: int
    denominator: int
    value: float | None
    wilson_lower: float | None
    wilson_upper: float | None
    z: float = 1.959963984540054
    KIND: ClassVar[str] = "rate"

    def _validate(self) -> None:
        _count(self.denominator, 1_000_000)
        _count(self.numerator, self.denominator)
        if self.z != 1.959963984540054:
            raise ValueError("V1 declares two-sided 95% Wilson interval")
        from .metrics import wilson_interval

        interval = wilson_interval(self.numerator, self.denominator)
        expected = (
            (None, None, None)
            if interval is None
            else (self.numerator / self.denominator, *interval)
        )
        if (self.value, self.wilson_lower, self.wilson_upper) != expected:
            raise ValueError("rate differs from count-based independent replay")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthDistributionV1(Artifact):
    count: int
    minimum_ns: int | None
    maximum_ns: int | None
    p50_ns: int | None
    p95_ns: int | None
    mean_ns: float | None
    KIND: ClassVar[str] = "distribution"

    def _validate(self) -> None:
        _count(self.count, 100_000)
        values = (
            self.minimum_ns,
            self.maximum_ns,
            self.p50_ns,
            self.p95_ns,
            self.mean_ns,
        )
        if self.count == 0:
            if any(value is not None for value in values):
                raise ValueError(
                    "empty distribution cannot invent zero latency"
                )
        else:
            if any(value is None for value in values):
                raise ValueError("nonempty distribution missing statistics")
            assert (
                self.minimum_ns is not None
                and self.maximum_ns is not None
                and self.p50_ns is not None
                and self.p95_ns is not None
                and self.mean_ns is not None
            )
            if (
                not -(2**63 - 1)
                <= self.minimum_ns
                <= self.p50_ns
                <= self.p95_ns
                <= self.maximum_ns
                < 2**63
            ):
                raise ValueError("invalid distribution ordering")
            _finite(self.mean_ns)
            if not self.minimum_ns <= self.mean_ns <= self.maximum_ns:
                raise ValueError("mean outside observation range")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthBucketV1(Artifact):
    epoch: int
    symbol: str | None
    bucket: int
    received: int
    persisted: int
    malformed: int
    refused: int
    known_host_dropped: int
    delivery_retries: int
    exact_quote_duplicates: int
    unchanged_quotes: int
    stale_quotes: int
    reordered_source_times: int
    heartbeat_count: int
    gap_count: int
    reconnect_count: int
    host_clock_jump_count: int
    source_clock_jump_count: int
    clock_correction_count: int
    clock_jump_count: int
    healthy_claim_count: int
    healthy_claim_discrepancies: int
    source_clock_missing: int
    upstream_loss_unknown: bool
    queue_samples: int
    queue_maximum: int | None
    queue_covered_ns: int
    queue_saturation_ns: int
    queue_saturation_events: int
    max_heartbeat_gap_ns: int | None
    source_to_receive_delta: BrokerHostHealthDistributionV1
    receive_to_persist: BrokerHostHealthDistributionV1
    known_drop_rate: BrokerHostHealthRateV1
    duplicate_rate: BrokerHostHealthRateV1
    stale_rate: BrokerHostHealthRateV1
    little_law_mean_in_system: float | None
    KIND: ClassVar[str] = "bucket"

    def _validate(self) -> None:
        from dataclasses import fields

        _symbol(self.symbol)
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is int:
                _count(value)
        if (
            self.persisted > self.received
            or self.known_host_dropped > self.received
            or self.healthy_claim_discrepancies > self.healthy_claim_count
        ):
            raise ValueError("inconsistent health bucket counts")
        if self.clock_jump_count != (
            self.host_clock_jump_count
            + self.source_clock_jump_count
            + self.clock_correction_count
        ):
            raise ValueError("clock diagnostic components differ")
        if (
            self.known_drop_rate.numerator != self.known_host_dropped
            or self.known_drop_rate.denominator != self.received
        ):
            raise ValueError("health drop denominator differs")
        if (
            self.duplicate_rate.numerator
            != self.delivery_retries + self.exact_quote_duplicates
            or self.duplicate_rate.denominator != self.received
        ):
            raise ValueError("health duplicate denominator differs")
        if (
            self.stale_rate.numerator != self.stale_quotes
            or self.stale_rate.denominator != self.persisted
        ):
            raise ValueError("health stale denominator differs")
        if (self.queue_samples == 0 and self.queue_covered_ns == 0) != (
            self.queue_maximum is None
        ):
            raise ValueError("unobserved queue cannot be zero")
        if not self.upstream_loss_unknown:
            raise ValueError(
                "V1 host metrics cannot identify pre-boundary provider loss"
            )
        if self.little_law_mean_in_system is not None:
            _finite(self.little_law_mean_in_system)
            if self.little_law_mean_in_system < 0:
                raise ValueError("negative Little law diagnostic")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthAuditV1(Artifact):
    header: BrokerHostHealthHeaderV1
    native_manifest_id: str
    native_records_sha256: str
    observations_sha256: str
    observation_count: int
    native_record_count: int
    complete_observations: bool
    state: BrokerHostHealthState
    failures: tuple[BrokerHostHealthFailure, ...]
    buckets: tuple[BrokerHostHealthBucketV1, ...]
    KIND: ClassVar[str] = "audit"

    def _validate(self) -> None:
        _identifier(self.native_manifest_id)
        _sha(self.native_records_sha256)
        _sha(self.observations_sha256)
        _count(self.observation_count, self.header.policy.max_observations)
        _count(self.native_record_count, self.header.policy.max_observations)
        if (
            self.failures != tuple(sorted(set(self.failures)))
            or len(self.buckets) > self.header.policy.max_buckets
        ):
            raise ValueError("invalid audit inventory")
        keys = tuple(
            (item.epoch, item.symbol or "", item.bucket)
            for item in self.buckets
        )
        if keys != tuple(sorted(set(keys))):
            raise ValueError("audit bucket keys are not canonical")
        if self.state is BrokerHostHealthState.QUALIFIED and (
            self.failures or not self.complete_observations
        ):
            raise ValueError("qualified audit has failed gates")
        if self.state is BrokerHostHealthState.UNAVAILABLE and (
            self.observation_count or self.buckets or self.complete_observations
        ):
            raise ValueError("historical audit fabricates observations")


@dataclass(frozen=True, slots=True)
class BrokerHostHealthUnavailableV1(Artifact):
    """Historical native metadata cannot retrospectively supply host timing."""

    family: BrokerHostHealthNativeFamily
    capture_id: str
    native_manifest_id: str
    state: BrokerHostHealthState = BrokerHostHealthState.UNAVAILABLE
    reason: str = "host_observation_stream_not_retained"
    KIND: ClassVar[str] = "unavailable"

    def _validate(self) -> None:
        _identifier(self.capture_id)
        _identifier(self.native_manifest_id)
        if (
            self.state is not BrokerHostHealthState.UNAVAILABLE
            or self.reason != "host_observation_stream_not_retained"
        ):
            raise ValueError(
                "historical unavailability cannot become a health claim"
            )


__all__ = [
    name for name in globals() if name.startswith("BrokerHostHealth")
] + ["canonical_health_json"]

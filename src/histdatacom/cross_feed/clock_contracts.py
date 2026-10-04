"""Conditional clock diagnostics; decoded records are never native authority.

The left source is the coordinate reference, not latent UTC. Calibration pairs
are predeclared assumptions. Frozen ex-ante means an offline sealed-capture
prefix simulation, not proof that a fitted artifact existed before the seal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from ._wire import Artifact, RationalV1, Record, digest, text

MAX_CLOCK_PAIRS = 128
MAX_CLOCK_SEGMENTS = 128
HEALTH_EXCEPTIONS = frozenset(
    {
        "upstream_loss_unidentified",
        "clock_discontinuity",
        "source_gap_or_reconnect",
        "source_timestamp_reordering",
    }
)


def _ordered(values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError("clock reasons must be sorted and unique")
    for value in values:
        text(value)


@dataclass(frozen=True, slots=True)
class ClockCandidatePairV1(Record):
    left_event_id: str
    right_event_id: str

    def _validate(self) -> None:
        text(self.left_event_id)
        text(self.right_event_id)


@dataclass(frozen=True, slots=True)
class ClockFitPolicyV1(Artifact):
    """Frozen robust rule and finite work; no estimated legal/health grant."""

    minimum_segment_pairs: int = 3
    jump_threshold_ns: int = 5_000_000
    maximum_drift_ppm: int = 1000
    maximum_offset_ns: int = 60_000_000_000
    maximum_prediction_horizon_ns: int = 0
    residual_band_multiplier: RationalV1 = field(
        default_factory=lambda: RationalV1("3", "1")
    )
    allowed_health_reasons: tuple[str, ...] = ("upstream_loss_unidentified",)
    fit_method: str = "theil_sen_median_intercept_v1"
    jump_method: str = "adjacent_and_sustained_median_v1"
    quantization_rule: str = "whole_declared_quantum_v1"
    KIND: ClassVar[str] = "clock-fit-policy"

    def _validate(self) -> None:
        if not 3 <= self.minimum_segment_pairs <= MAX_CLOCK_PAIRS:
            raise ValueError("clock minimum support outside bound")
        if not 0 < self.jump_threshold_ns <= self.maximum_offset_ns:
            raise ValueError("clock jump threshold outside offset bound")
        if not 0 <= self.maximum_drift_ppm <= 100_000:
            raise ValueError("clock drift bound outside supported range")
        if not 0 < self.maximum_offset_ns <= 86_400_000_000_000:
            raise ValueError("clock offset bound exceeds one day")
        if not 0 <= self.maximum_prediction_horizon_ns <= 86_400_000_000_000:
            raise ValueError("clock prediction horizon exceeds one day")
        if not 1 <= self.residual_band_multiplier.value <= 100:
            raise ValueError("clock descriptive multiplier outside bound")
        _ordered(self.allowed_health_reasons)
        if set(self.allowed_health_reasons) - HEALTH_EXCEPTIONS:
            raise ValueError("unsupported clock health exception")
        if (
            self.fit_method != "theil_sen_median_intercept_v1"
            or self.jump_method != "adjacent_and_sustained_median_v1"
            or self.quantization_rule != "whole_declared_quantum_v1"
        ):
            raise ValueError("unsupported clock rule")


@dataclass(frozen=True, slots=True)
class ClockFitRequestV1(Artifact):
    calibration_pairs: tuple[ClockCandidatePairV1, ...]
    left_cutoff_sequence: int
    right_cutoff_sequence: int
    policy: ClockFitPolicyV1 = field(default_factory=ClockFitPolicyV1)
    mode: str = "retrospective"
    candidate_pair_basis: str = "predeclared_conditional_correspondences"
    KIND: ClassVar[str] = "clock-fit-request"

    def _validate(self) -> None:
        if not 1 <= len(self.calibration_pairs) <= MAX_CLOCK_PAIRS:
            raise ValueError("clock pair count outside bound")
        pairs = tuple(
            (pair.left_event_id, pair.right_event_id)
            for pair in self.calibration_pairs
        )
        if pairs != tuple(sorted(set(pairs))):
            raise ValueError("clock candidate pairs must be sorted unique")
        if len({pair[0] for pair in pairs}) != len(pairs) or len(
            {pair[1] for pair in pairs}
        ) != len(pairs):
            raise ValueError("clock candidate event may appear only once")
        if min(self.left_cutoff_sequence, self.right_cutoff_sequence) < 0:
            raise ValueError("negative clock calibration cutoff")
        if self.mode not in ("retrospective", "frozen_ex_ante"):
            raise ValueError("unsupported clock causality mode")
        text(self.candidate_pair_basis)


@dataclass(frozen=True, slots=True)
class ClockPairUseV1(Record):
    pair: ClockCandidatePairV1
    status: str
    reasons: tuple[str, ...]

    def _validate(self) -> None:
        _ordered(self.reasons)
        if self.status not in ("included", "excluded") or (
            (self.status == "excluded") != bool(self.reasons)
        ):
            raise ValueError("clock pair status/reasons differ")


@dataclass(frozen=True, slots=True)
class ClockDelayDiagnosticV1(Record):
    """Source-to-host differences, never pure one-way delivery latency."""

    capture_id: str
    epoch_id: str
    health_state: str
    health_reasons: tuple[str, ...]
    sample_count: int
    missing_source_count: int
    minimum_ns: RationalV1 | None
    maximum_ns: RationalV1 | None
    median_ns: RationalV1 | None
    mad_ns: RationalV1 | None
    minimum_precision_ns: int | None
    maximum_precision_ns: int | None
    source_increment_gcd_ns: int = 0
    source_timestamp_reversal_count: int = 0
    host_offset_jump_count: int = 0
    maximum_abs_host_offset_change_ns: int = 0

    def _validate(self) -> None:
        for value in (self.capture_id, self.epoch_id, self.health_state):
            text(value)
        _ordered(self.health_reasons)
        if (
            min(
                self.sample_count,
                self.missing_source_count,
                self.source_increment_gcd_ns,
                self.source_timestamp_reversal_count,
                self.host_offset_jump_count,
                self.maximum_abs_host_offset_change_ns,
            )
            < 0
        ):
            raise ValueError("negative clock diagnostic count")
        fields = (
            self.minimum_ns,
            self.maximum_ns,
            self.median_ns,
            self.mad_ns,
            self.minimum_precision_ns,
            self.maximum_precision_ns,
        )
        if (self.sample_count == 0) != all(item is None for item in fields):
            raise ValueError("clock diagnostic support differs")
        if self.sample_count:
            if any(item is None for item in fields):
                raise ValueError("clock diagnostic fields are incomplete")
            assert self.minimum_ns is not None
            assert self.maximum_ns is not None
            assert self.median_ns is not None
            assert self.mad_ns is not None
            assert self.minimum_precision_ns is not None
            assert self.maximum_precision_ns is not None
            if not (
                self.minimum_ns.value
                <= self.median_ns.value
                <= self.maximum_ns.value
                and self.mad_ns.value >= 0
                and 0 < self.minimum_precision_ns <= self.maximum_precision_ns
            ):
                raise ValueError("clock diagnostic range differs")


@dataclass(frozen=True, slots=True)
class ClockSegmentV1(Artifact):
    left_epoch_id: str
    right_epoch_id: str
    left_first_sequence: int
    left_last_sequence: int
    right_first_sequence: int
    right_last_sequence: int
    left_minimum_time_ns: int
    left_maximum_time_ns: int
    right_minimum_time_ns: int
    right_maximum_time_ns: int
    origin_time_ns: int
    pair_count: int
    pair_digest: str
    offset_ns: RationalV1
    drift: RationalV1
    median_difference_ns: RationalV1
    difference_mad_ns: RationalV1
    scaled_difference_mad_ns: RationalV1
    residual_mad_ns: RationalV1
    scaled_mad_ns: RationalV1
    drift_radius: RationalV1
    left_precision_ns: int
    right_precision_ns: int
    status: str
    reasons: tuple[str, ...]
    KIND: ClassVar[str] = "clock-segment"

    def _validate(self) -> None:
        text(self.left_epoch_id)
        text(self.right_epoch_id)
        digest(self.pair_digest)
        _ordered(self.reasons)
        if self.status not in ("usable", "insufficient") or (
            (self.status == "insufficient") != bool(self.reasons)
        ):
            raise ValueError("clock segment status/reasons differ")
        for lo, hi in (
            (self.left_first_sequence, self.left_last_sequence),
            (self.right_first_sequence, self.right_last_sequence),
            (self.left_minimum_time_ns, self.left_maximum_time_ns),
            (self.right_minimum_time_ns, self.right_maximum_time_ns),
        ):
            if not 0 <= lo <= hi:
                raise ValueError("invalid clock segment interval")
        if (
            not (
                self.right_minimum_time_ns
                <= self.origin_time_ns
                <= self.right_maximum_time_ns
            )
            or not 1 <= self.pair_count <= MAX_CLOCK_PAIRS
        ):
            raise ValueError("invalid clock segment support")
        if min(self.left_precision_ns, self.right_precision_ns) < 1:
            raise ValueError("clock segment lacks declared precision")
        if (
            min(
                self.difference_mad_ns.value,
                self.scaled_difference_mad_ns.value,
                self.residual_mad_ns.value,
                self.scaled_mad_ns.value,
                self.drift_radius.value,
            )
            < 0
        ):
            raise ValueError("negative clock uncertainty")


@dataclass(frozen=True, slots=True)
class ClockModelV1(Artifact):
    left_capture_id: str
    right_capture_id: str
    left_projection_id: str
    right_projection_id: str
    left_health_audit_id: str
    right_health_audit_id: str
    left_prefix_sha256: str
    right_prefix_sha256: str
    request: ClockFitRequestV1
    pair_uses: tuple[ClockPairUseV1, ...]
    segments: tuple[ClockSegmentV1, ...]
    delay_diagnostics: tuple[ClockDelayDiagnosticV1, ...]
    status: str
    interpretation: str = "conditional_source_clock_difference_not_latency"
    causality_scope: str = "sealed_capture_offline_prefix_simulation"
    KIND: ClassVar[str] = "clock-model"

    def _validate(self) -> None:
        for value in (
            self.left_capture_id,
            self.right_capture_id,
            self.left_projection_id,
            self.right_projection_id,
            self.left_health_audit_id,
            self.right_health_audit_id,
        ):
            text(value)
        if self.left_capture_id == self.right_capture_id:
            raise ValueError("clock calibration requires distinct captures")
        digest(self.left_prefix_sha256)
        digest(self.right_prefix_sha256)
        if tuple(use.pair for use in self.pair_uses) != (
            self.request.calibration_pairs
        ):
            raise ValueError("clock pair denominator differs")
        if len(self.segments) > MAX_CLOCK_SEGMENTS:
            raise ValueError("clock segment bound exceeded")
        if sum(segment.pair_count for segment in self.segments) != sum(
            use.status == "included" for use in self.pair_uses
        ):
            raise ValueError("clock segment/pair denominator differs")
        keys = tuple(
            (segment.right_first_sequence, segment.left_first_sequence)
            for segment in self.segments
        )
        if keys != tuple(sorted(set(keys))):
            raise ValueError("clock segment ordering differs")
        usable = sum(segment.status == "usable" for segment in self.segments)
        expected = (
            "insufficient"
            if not usable
            else (
                "ready"
                if usable == len(self.segments)
                and all(use.status == "included" for use in self.pair_uses)
                else "partial"
            )
        )
        if self.status != expected:
            raise ValueError("clock model status differs")
        if (
            self.interpretation
            != "conditional_source_clock_difference_not_latency"
            or self.causality_scope
            != "sealed_capture_offline_prefix_simulation"
        ):
            raise ValueError("unsupported clock interpretation")


@dataclass(frozen=True, slots=True)
class ClockCorrectionV1(Record):
    status: str
    event_id: str
    capture_id: str
    model_id: str
    segment_id: str | None
    corrected_time_ns: RationalV1 | None
    uncertainty_radius_ns: RationalV1 | None
    reasons: tuple[str, ...]

    def _validate(self) -> None:
        for value in (self.event_id, self.capture_id, self.model_id):
            text(value)
        _ordered(self.reasons)
        if self.status == "usable":
            if (
                self.segment_id is None
                or self.corrected_time_ns is None
                or self.uncertainty_radius_ns is None
                or self.uncertainty_radius_ns.value < 0
                or self.reasons
            ):
                raise ValueError("usable clock correction lacks evidence")
        elif self.status != "unavailable" or (
            not self.reasons
            or self.corrected_time_ns is not None
            or self.uncertainty_radius_ns is not None
        ):
            raise ValueError("unavailable clock correction fields differ")

"""Streaming fit, drift comparison, and persistence for broker fingerprints."""

from __future__ import annotations

import hashlib
import heapq
import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from histdatacom.broker_capture.contracts import (
    BrokerCaptureEventKind,
    BrokerCaptureEventV1,
    BrokerCaptureSessionManifestV1,
    canonical_capture_json,
)
from histdatacom.broker_capture.fingerprint_contracts import (
    BROKER_DELIVERY_FINGERPRINT_ARTIFACT_KIND,
    BrokerCaptureEligibilityStatus,
    BrokerCaptureEligibilityV1,
    BrokerDeliveryCaptureEvidenceV1,
    BrokerDeliveryCellV1,
    BrokerDeliveryConditionV1,
    BrokerDeliveryDriftConfigV1,
    BrokerDeliveryDriftStatus,
    BrokerDeliveryFingerprintComparisonV1,
    BrokerDeliveryFingerprintV1,
    BrokerDeliveryFitConfigV1,
    BrokerDeliveryMetricComparisonV1,
    BrokerDeliveryMetricV1,
    BrokerDeliverySupportStatus,
)
from histdatacom.broker_capture.storage import (
    BrokerCaptureStorageError,
    inspect_broker_capture_session,
    replay_broker_capture_session,
)
from histdatacom.data_quality.calendar_profiles import HistDataCalendarProfile
from histdatacom.market_context.contracts import (
    MarketContextEventV1,
    MarketContextTimelineV1,
    market_context_calendar_state,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

_TAG_RE = re.compile(r"[^A-Za-z0-9._:-]+")
_NANOSECONDS_PER_SECOND = 1_000_000_000
_RATE_EVENT_KINDS = (
    BrokerCaptureEventKind.QUOTE,
    BrokerCaptureEventKind.RECONNECT,
    BrokerCaptureEventKind.GAP,
    BrokerCaptureEventKind.OUTAGE_START,
    BrokerCaptureEventKind.OUTAGE_END,
    BrokerCaptureEventKind.PROCESS_RESTART,
    BrokerCaptureEventKind.CLOCK_CORRECTION,
)
_RATE_TRANSITIONS = (
    (BrokerCaptureEventKind.QUOTE, BrokerCaptureEventKind.QUOTE),
    (BrokerCaptureEventKind.RECONNECT, BrokerCaptureEventKind.QUOTE),
    (BrokerCaptureEventKind.OUTAGE_END, BrokerCaptureEventKind.QUOTE),
    (BrokerCaptureEventKind.PROCESS_RESTART, BrokerCaptureEventKind.QUOTE),
)


class BrokerDeliveryFingerprintError(RuntimeError):
    """Base class for fail-closed broker fingerprint operations."""


class BrokerDeliveryIneligibleCaptureError(BrokerDeliveryFingerprintError):
    """At least one capture failed the declared fitting health gate."""

    def __init__(self, eligibility: BrokerCaptureEligibilityV1) -> None:
        self.eligibility = eligibility
        super().__init__(
            f"capture {eligibility.session_id} is ineligible: "
            + ", ".join(eligibility.reason_codes)
        )


class BrokerDeliveryResourceLimitError(BrokerDeliveryFingerprintError):
    """A configured resource limit was reached without truncating input."""


class BrokerDeliveryFingerprintIdentityError(BrokerDeliveryFingerprintError):
    """Capture or predecessor identities are incompatible."""


class BrokerDeliveryFingerprintArtifactError(BrokerDeliveryFingerprintError):
    """A fingerprint artifact failed immutable persistence verification."""


@dataclass(slots=True)
class _HealthConsumer:
    event_count: int = 0
    quote_count: int = 0
    clock_correction_count: int = 0
    max_abs_clock_correction_ns: int = 0
    explained_wall_regression_count: int = 0
    unexplained_wall_regression_count: int = 0
    first_receive_time_utc_ns: int | None = None
    last_receive_time_utc_ns: int | None = None
    _previous_wall_ns: int | None = None

    def on_event(self, event: BrokerCaptureEventV1) -> None:
        self.event_count += 1
        if event.kind is BrokerCaptureEventKind.QUOTE:
            self.quote_count += 1
        if event.kind is BrokerCaptureEventKind.CLOCK_CORRECTION:
            self.clock_correction_count += 1
            self.max_abs_clock_correction_ns = max(
                self.max_abs_clock_correction_ns,
                abs(event.clock_offset_change_ns or 0),
            )
        if (
            self._previous_wall_ns is not None
            and event.receive_time_utc_ns < self._previous_wall_ns
        ):
            if event.kind is BrokerCaptureEventKind.CLOCK_CORRECTION:
                self.explained_wall_regression_count += 1
            else:
                self.unexplained_wall_regression_count += 1
        self._previous_wall_ns = event.receive_time_utc_ns
        if self.first_receive_time_utc_ns is None:
            self.first_receive_time_utc_ns = event.receive_time_utc_ns
            self.last_receive_time_utc_ns = event.receive_time_utc_ns
        else:
            self.first_receive_time_utc_ns = min(
                self.first_receive_time_utc_ns, event.receive_time_utc_ns
            )
            assert self.last_receive_time_utc_ns is not None
            self.last_receive_time_utc_ns = max(
                self.last_receive_time_utc_ns, event.receive_time_utc_ns
            )


def assess_broker_capture_eligibility(
    root: str | Path,
    manifest: BrokerCaptureSessionManifestV1,
    *,
    config: BrokerDeliveryFitConfigV1 | None = None,
) -> BrokerCaptureEligibilityV1:
    """Verify one capture and return a deterministic fit-health decision."""
    policy = config or BrokerDeliveryFitConfigV1()
    hard_reasons: set[str] = set()
    limited_reasons: set[str] = set()
    inspection_clean = False
    integrity_verified = False
    logical_digest: str | None = None
    health = _HealthConsumer()

    if not manifest.complete:
        hard_reasons.add("capture_not_completed")
    if (
        manifest.session.collector_version
        not in policy.supported_collector_versions
    ):
        hard_reasons.add("unsupported_collector_version")
    if (
        policy.supported_adapter_ids
        and manifest.session.adapter_id not in policy.supported_adapter_ids
    ):
        hard_reasons.add("unsupported_adapter_id")
    for limitation in manifest.limitations:
        if any(
            limitation.startswith(prefix)
            for prefix in policy.fatal_limitation_prefixes
        ):
            hard_reasons.add("fatal_capture_limitation")
        else:
            limited_reasons.add("capture_limitation_present")

    try:
        inspection = inspect_broker_capture_session(
            root, manifest.session.session_id
        )
        inspection_clean = inspection.clean
        if not inspection_clean:
            hard_reasons.add("capture_inspection_not_clean")
        summary = replay_broker_capture_session(
            root, manifest, consumers=(health,)
        )
        integrity_verified = True
        logical_digest = summary.logical_content_sha256
    except (BrokerCaptureStorageError, OSError, TypeError, ValueError):
        hard_reasons.add("integrity_verification_failed")

    if health.event_count < policy.min_capture_events:
        hard_reasons.add("insufficient_capture_events")
    if health.quote_count < policy.min_quote_events:
        hard_reasons.add("insufficient_quote_events")
    if health.clock_correction_count > policy.max_clock_correction_events:
        hard_reasons.add("excessive_clock_corrections")
    elif health.clock_correction_count:
        limited_reasons.add("clock_corrections_present")
    if health.max_abs_clock_correction_ns > policy.max_abs_clock_correction_ns:
        hard_reasons.add("excessive_clock_correction_magnitude")
    if (
        health.unexplained_wall_regression_count
        > policy.max_unexplained_wall_regressions
    ):
        hard_reasons.add("unexplained_wall_clock_regression")

    if hard_reasons:
        status = BrokerCaptureEligibilityStatus.INELIGIBLE
        reasons = tuple(sorted(hard_reasons | limited_reasons))
    elif limited_reasons:
        status = BrokerCaptureEligibilityStatus.LIMITED
        reasons = tuple(sorted(limited_reasons))
    else:
        status = BrokerCaptureEligibilityStatus.ELIGIBLE
        reasons = ()
    return BrokerCaptureEligibilityV1(
        session_id=manifest.session.session_id,
        manifest_id=manifest.manifest_id,
        config_id=policy.config_id,
        status=status,
        reason_codes=reasons,
        manifest_complete=manifest.complete,
        inspection_clean=inspection_clean,
        integrity_verified=integrity_verified,
        event_count=health.event_count,
        quote_count=health.quote_count,
        clock_correction_count=health.clock_correction_count,
        max_abs_clock_correction_ns=health.max_abs_clock_correction_ns,
        explained_wall_regression_count=(
            health.explained_wall_regression_count
        ),
        unexplained_wall_regression_count=(
            health.unexplained_wall_regression_count
        ),
        first_receive_time_utc_ns=health.first_receive_time_utc_ns,
        last_receive_time_utc_ns=health.last_receive_time_utc_ns,
        logical_content_sha256=logical_digest,
    )


@dataclass(slots=True)
class _SampleAccumulator:
    name: str
    kind: str
    unit: str
    sample_limit: int
    rounding_digits: int
    support_count: int = 0
    total: float = 0.0
    total_squares: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    _samples: list[tuple[int, str, float]] = field(default_factory=list)

    def add(self, value: float, evidence_key: str) -> None:
        if not math.isfinite(value):
            return
        self.support_count += 1
        self.total += value
        self.total_squares += value * value
        self.minimum = (
            value if self.minimum is None else min(self.minimum, value)
        )
        self.maximum = (
            value if self.maximum is None else max(self.maximum, value)
        )
        score = int.from_bytes(
            hashlib.sha256(
                f"{self.name}\0{evidence_key}".encode("utf-8")
            ).digest(),
            "big",
        )
        row = (-score, evidence_key, value)
        if len(self._samples) < self.sample_limit:
            heapq.heappush(self._samples, row)
        elif score < -self._samples[0][0]:
            heapq.heapreplace(self._samples, row)

    def to_metric(self, quantiles: Sequence[float]) -> BrokerDeliveryMetricV1:
        if not self.support_count:
            return BrokerDeliveryMetricV1(
                name=self.name,
                kind=self.kind,
                unit=self.unit,
                support_count=0,
                sample_count=0,
                estimate=None,
                lower=None,
                upper=None,
                limitations=("metric_has_no_observations",),
            )
        estimate = self.total / self.support_count
        if self.kind == "rate":
            lower, upper = _wilson_interval(estimate, self.support_count)
        elif self.support_count == 1:
            lower = upper = estimate
        else:
            variance = max(
                0.0,
                (
                    self.total_squares
                    - self.total * self.total / self.support_count
                )
                / (self.support_count - 1),
            )
            half_width = 1.96 * math.sqrt(variance / self.support_count)
            lower, upper = estimate - half_width, estimate + half_width
        sample_values = sorted(row[2] for row in self._samples)
        limitations = (
            ("deterministic_bottom_hash_sample",)
            if len(sample_values) < self.support_count
            else ()
        )
        return BrokerDeliveryMetricV1(
            name=self.name,
            kind=self.kind,
            unit=self.unit,
            support_count=self.support_count,
            sample_count=len(sample_values),
            estimate=_rounded(estimate, self.rounding_digits),
            lower=_rounded(lower, self.rounding_digits),
            upper=_rounded(upper, self.rounding_digits),
            minimum=_rounded(self.minimum, self.rounding_digits),
            maximum=_rounded(self.maximum, self.rounding_digits),
            quantiles={
                _quantile_name(q): _rounded_required(
                    _quantile(sample_values, q), self.rounding_digits
                )
                for q in quantiles
            },
            limitations=limitations,
        )


@dataclass(slots=True)
class _CellAccumulator:
    condition: BrokerDeliveryConditionV1
    sample_limit: int
    rounding_digits: int
    support_count: int = 0
    metrics: dict[str, _SampleAccumulator] = field(default_factory=dict)
    active_runs: dict[str, int] = field(default_factory=dict)

    def observe_quote(self) -> None:
        self.support_count += 1

    def add(
        self,
        name: str,
        value: float,
        evidence_key: str,
        *,
        kind: str = "distribution",
        unit: str = "ratio",
    ) -> None:
        metric = self.metrics.get(name)
        if metric is None:
            metric = _SampleAccumulator(
                name=name,
                kind=kind,
                unit=unit,
                sample_limit=self.sample_limit,
                rounding_digits=self.rounding_digits,
            )
            self.metrics[name] = metric
        metric.add(value, evidence_key)

    def observe_run(
        self,
        name: str,
        active: bool,
        evidence_key: str,
    ) -> None:
        count = self.active_runs.get(name, 0)
        if active:
            self.active_runs[name] = count + 1
        elif count:
            self.add(
                f"{name}_run_length",
                float(count),
                evidence_key,
                unit="intervals",
            )
            self.active_runs[name] = 0

    def flush_runs(self, evidence_key: str) -> None:
        for name, count in sorted(self.active_runs.items()):
            if count:
                self.add(
                    f"{name}_run_length",
                    float(count),
                    evidence_key,
                    unit="intervals",
                )
        self.active_runs.clear()


@dataclass(frozen=True, slots=True)
class _PreviousQuote:
    monotonic_ns: int
    bid: float
    ask: float
    spread: float
    message_id: str


class _FingerprintConsumer:
    def __init__(
        self,
        config: BrokerDeliveryFitConfigV1,
        *,
        calendar_profile: HistDataCalendarProfile | None,
        context_events: Sequence[MarketContextEventV1],
    ) -> None:
        self.config = config
        self.calendar_profile = calendar_profile
        self.context_events = context_events
        self.event_count = 0
        self.quote_count = 0
        self.cells: dict[str, _CellAccumulator] = {}
        self._previous_event_monotonic_ns: int | None = None
        self._previous_event_kind: BrokerCaptureEventKind | None = None
        self._previous_quotes: dict[str, _PreviousQuote] = {}
        self._batch_id: str | None = None
        self._batch_run_count = 0
        self._lifecycle: str | None = None
        self._lifecycle_quotes_remaining = 0
        self._session_id = ""
        self._session_first_monotonic_ns: int | None = None
        self._session_last_monotonic_ns: int | None = None
        self._session_event_count = 0
        self._session_quote_count = 0
        self.calendar_profile_complete = True
        self._ensure_cell({})

    def start_session(self, session_id: str) -> None:
        self._previous_event_monotonic_ns = None
        self._previous_event_kind = None
        self._previous_quotes.clear()
        self._batch_id = None
        self._batch_run_count = 0
        self._lifecycle = None
        self._lifecycle_quotes_remaining = 0
        self._session_id = session_id
        self._session_first_monotonic_ns = None
        self._session_last_monotonic_ns = None
        self._session_event_count = 0
        self._session_quote_count = 0

    def end_session(self) -> None:
        self._flush_batch_run(f"{self._session_id}:capture-end")
        for cell in self.cells.values():
            cell.flush_runs(f"{self._session_id}:capture-end")
        start = self._session_first_monotonic_ns
        end = self._session_last_monotonic_ns
        if start is not None and end is not None and end > start:
            duration_seconds = (end - start) / _NANOSECONDS_PER_SECOND
            global_cell = self._ensure_cell({})
            global_cell.add(
                "event_intensity_hz",
                self._session_event_count / duration_seconds,
                self._session_id,
                unit="events_per_second",
            )
            global_cell.add(
                "quote_intensity_hz",
                self._session_quote_count / duration_seconds,
                self._session_id,
                unit="quotes_per_second",
            )

    def on_event(self, event: BrokerCaptureEventV1) -> None:
        self.event_count += 1
        self._session_event_count += 1
        if self._session_first_monotonic_ns is None:
            self._session_first_monotonic_ns = event.receive_time_monotonic_ns
        self._session_last_monotonic_ns = event.receive_time_monotonic_ns
        if self.event_count > self.config.max_input_events:
            raise BrokerDeliveryResourceLimitError(
                "broker capture events exceed max_input_events"
            )
        global_cell = self._ensure_cell({})
        if self._previous_event_monotonic_ns is not None:
            global_cell.add(
                "event_interarrival_ns",
                float(
                    event.receive_time_monotonic_ns
                    - self._previous_event_monotonic_ns
                ),
                event.event_id,
                unit="ns",
            )
        if self._previous_event_kind is not None:
            for previous_kind, current_kind in _RATE_TRANSITIONS[
                : self.config.max_transition_categories
            ]:
                global_cell.add(
                    f"event_transition.{previous_kind.value}_to_{current_kind.value}_rate",
                    float(
                        self._previous_event_kind is previous_kind
                        and event.kind is current_kind
                    ),
                    event.event_id,
                    kind="rate",
                )
        self._previous_event_monotonic_ns = event.receive_time_monotonic_ns
        self._previous_event_kind = event.kind
        for rate_kind in _RATE_EVENT_KINDS:
            global_cell.add(
                f"event_kind.{rate_kind.value}_rate",
                float(event.kind is rate_kind),
                event.event_id,
                kind="rate",
            )
        if event.message.gap_duration_ns is not None:
            global_cell.add(
                "outage_or_gap_duration_ns",
                float(event.message.gap_duration_ns),
                event.event_id,
                unit="ns",
            )
        if event.clock_offset_change_ns is not None:
            global_cell.add(
                "clock_correction_abs_ns",
                float(abs(event.clock_offset_change_ns)),
                event.event_id,
                unit="ns",
            )
        if event.kind in {
            BrokerCaptureEventKind.RECONNECT,
            BrokerCaptureEventKind.OUTAGE_END,
            BrokerCaptureEventKind.PROCESS_RESTART,
        }:
            self._lifecycle = {
                BrokerCaptureEventKind.RECONNECT: "post_reconnect",
                BrokerCaptureEventKind.OUTAGE_END: "post_outage",
                BrokerCaptureEventKind.PROCESS_RESTART: "post_restart",
            }[event.kind]
            self._lifecycle_quotes_remaining = (
                self.config.post_lifecycle_quote_count
            )
        if event.kind is BrokerCaptureEventKind.QUOTE:
            self._on_quote(event)

    def finish(self) -> None:
        self.end_session()

    def _on_quote(self, event: BrokerCaptureEventV1) -> None:
        message = event.message
        assert message.symbol is not None
        assert message.bid is not None
        assert message.ask is not None
        self.quote_count += 1
        self._session_quote_count += 1
        conditions = self._quote_conditions(event)
        cells = [self._ensure_cell(dimensions) for dimensions in conditions]
        for cell in cells:
            cell.observe_quote()
        spread = message.ask - message.bid
        previous = self._previous_quotes.get(message.symbol)
        for cell in cells:
            cell.add("spread", spread, event.event_id, unit="price")
            if message.source_timestamp_precision_ns is not None:
                cell.add(
                    "source_timestamp_precision_ns",
                    float(message.source_timestamp_precision_ns),
                    event.event_id,
                    unit="ns",
                )
            decimals = _price_decimal_places(message.bid_text, message.ask_text)
            if decimals is not None:
                cell.add(
                    "price_decimal_places",
                    float(decimals),
                    event.event_id,
                    unit="decimal_places",
                )
                cell.add(
                    "price_trailing_zero_rate",
                    float(
                        _has_trailing_zero(message.bid_text, message.ask_text)
                    ),
                    event.event_id,
                    kind="rate",
                )
        if previous is not None:
            interval = event.receive_time_monotonic_ns - previous.monotonic_ns
            changed = message.bid != previous.bid or message.ask != previous.ask
            burst = interval <= self.config.burst_interval_ns
            quiet = interval >= self.config.quiet_interval_ns
            stale = (
                not changed and interval <= self.config.stale_max_interval_ns
            )
            values = (
                (
                    "quote_interarrival_ns",
                    float(interval),
                    "distribution",
                    "ns",
                ),
                (
                    "burst_interval_rate",
                    float(burst),
                    "rate",
                    "ratio",
                ),
                (
                    "quiet_interval_rate",
                    float(quiet),
                    "rate",
                    "ratio",
                ),
                (
                    "stale_quote_rate",
                    float(stale),
                    "rate",
                    "ratio",
                ),
                ("transition_rate", float(changed), "rate", "ratio"),
                (
                    "exact_duplicate_rate",
                    float(message.message_id == previous.message_id),
                    "rate",
                    "ratio",
                ),
                (
                    "spread_change",
                    spread - previous.spread,
                    "distribution",
                    "price",
                ),
                (
                    "absolute_spread_change",
                    abs(spread - previous.spread),
                    "distribution",
                    "price",
                ),
            )
            for cell in cells:
                for name, value, kind, unit in values:
                    cell.add(
                        name,
                        value,
                        event.event_id,
                        kind=kind,
                        unit=unit,
                    )
                if not quiet:
                    cell.add(
                        "active_quote_interarrival_ns",
                        float(interval),
                        event.event_id,
                        unit="ns",
                    )
                cell.observe_run("burst_interval", burst, event.event_id)
                cell.observe_run("quiet_interval", quiet, event.event_id)
                cell.observe_run("stale_quote", stale, event.event_id)
        self._previous_quotes[message.symbol] = _PreviousQuote(
            monotonic_ns=event.receive_time_monotonic_ns,
            bid=message.bid,
            ask=message.ask,
            spread=spread,
            message_id=message.message_id,
        )
        self._observe_batch(message.source_batch_id, event.event_id)
        if self._lifecycle_quotes_remaining:
            self._lifecycle_quotes_remaining -= 1
            if not self._lifecycle_quotes_remaining:
                self._lifecycle = None

    def _quote_conditions(
        self, event: BrokerCaptureEventV1
    ) -> tuple[dict[str, str], ...]:
        symbol = event.message.symbol
        assert symbol is not None
        state = market_context_calendar_state(
            event.receive_time_utc_ns,
            calendar_profile=self.calendar_profile,
        )
        self.calendar_profile_complete = (
            self.calendar_profile_complete and state.profile_complete
        )
        dimensions: list[dict[str, str]] = [{}, {"symbol": symbol}]
        sessions = state.active_sessions or (state.session_state,)
        for dimension, tags in (
            ("session", sessions),
            ("overlap", state.overlaps),
            ("special", state.special_tags),
            ("holiday", state.holiday_tags),
            ("event", state.event_tags),
        ):
            for tag in tags:
                self._append_condition_pair(dimensions, symbol, dimension, tag)
        context_match_count = 0
        for context_event in self.context_events:
            if not _context_event_matches(
                context_event, event.receive_time_utc_ns, symbol
            ):
                continue
            tags = (f"market_{context_event.kind.value}", *context_event.tags)
            for tag in tags:
                context_match_count += 1
                if (
                    context_match_count
                    > self.config.max_market_matches_per_quote
                ):
                    raise BrokerDeliveryResourceLimitError(
                        "market context matches exceed max_market_matches_per_quote"
                    )
                self._append_condition_pair(dimensions, symbol, "event", tag)
        if self._lifecycle is not None and self._lifecycle_quotes_remaining:
            self._append_condition_pair(
                dimensions, symbol, "lifecycle", self._lifecycle
            )
        canonical = {
            BrokerDeliveryConditionV1(item).key: item for item in dimensions
        }
        return tuple(canonical[key] for key in sorted(canonical))

    def _append_condition_pair(
        self,
        conditions: list[dict[str, str]],
        symbol: str,
        dimension: str,
        tag: str,
    ) -> None:
        value = _safe_tag(tag)
        conditions.append({dimension: value})
        conditions.append({"symbol": symbol, dimension: value})

    def _ensure_cell(self, dimensions: Mapping[str, str]) -> _CellAccumulator:
        condition = BrokerDeliveryConditionV1(dict(dimensions))
        existing = self.cells.get(condition.condition_id)
        if existing is not None:
            return existing
        if len(self.cells) >= self.config.max_cells:
            raise BrokerDeliveryResourceLimitError(
                "condition cells exceed max_cells"
            )
        cell = _CellAccumulator(
            condition=condition,
            sample_limit=self.config.max_samples_per_metric,
            rounding_digits=self.config.rounding_digits,
        )
        self.cells[condition.condition_id] = cell
        return cell

    def _observe_batch(self, batch_id: str | None, evidence_key: str) -> None:
        if batch_id == self._batch_id:
            self._batch_run_count += 1
            return
        self._flush_batch_run(evidence_key)
        self._batch_id = batch_id
        self._batch_run_count = 1 if batch_id is not None else 0

    def _flush_batch_run(self, evidence_key: str) -> None:
        if self._batch_id is not None and self._batch_run_count:
            self._ensure_cell({}).add(
                "source_batch_quote_count",
                float(self._batch_run_count),
                evidence_key,
                unit="quotes",
            )
        self._batch_id = None
        self._batch_run_count = 0


def fit_broker_delivery_fingerprint(
    root: str | Path,
    manifests: Sequence[BrokerCaptureSessionManifestV1],
    *,
    config: BrokerDeliveryFitConfigV1 | None = None,
    calendar_profile: HistDataCalendarProfile | None = None,
    market_context_timeline: MarketContextTimelineV1 | None = None,
    supersedes: BrokerDeliveryFingerprintV1 | None = None,
    effective_start_utc_ns: int | None = None,
    effective_end_utc_ns: int | None = None,
) -> BrokerDeliveryFingerprintV1:
    """Fit one compact immutable profile with two verified streaming passes."""
    policy = config or BrokerDeliveryFitConfigV1()
    ordered = tuple(sorted(manifests, key=lambda item: item.session.session_id))
    if not ordered:
        raise ValueError("at least one capture manifest is required")
    if len(ordered) > policy.max_capture_manifests:
        raise BrokerDeliveryResourceLimitError(
            "capture manifests exceed max_capture_manifests"
        )
    if len({item.session.session_id for item in ordered}) != len(ordered):
        raise ValueError("capture manifests contain duplicate sessions")
    _assert_compatible_capture_identity(ordered)
    context_events = _bounded_context_events(market_context_timeline, policy)
    decisions: list[BrokerCaptureEligibilityV1] = []
    evidence: list[BrokerDeliveryCaptureEvidenceV1] = []
    for manifest in ordered:
        decision = assess_broker_capture_eligibility(
            root, manifest, config=policy
        )
        if not decision.fit_allowed:
            raise BrokerDeliveryIneligibleCaptureError(decision)
        assert decision.logical_content_sha256 is not None
        assert decision.first_receive_time_utc_ns is not None
        assert decision.last_receive_time_utc_ns is not None
        decisions.append(decision)
        evidence.append(
            BrokerDeliveryCaptureEvidenceV1(
                session_id=manifest.session.session_id,
                manifest_id=manifest.manifest_id,
                eligibility_decision_id=decision.decision_id,
                logical_content_sha256=decision.logical_content_sha256,
                partition_hashes_sha256=_partition_hashes_digest(manifest),
                partition_count=len(manifest.partitions),
                event_count=decision.event_count,
                first_receive_time_utc_ns=decision.first_receive_time_utc_ns,
                last_receive_time_utc_ns=decision.last_receive_time_utc_ns,
            )
        )
    total_events = sum(item.event_count for item in decisions)
    if total_events > policy.max_input_events:
        raise BrokerDeliveryResourceLimitError(
            "qualified captures exceed max_input_events"
        )

    consumer = _FingerprintConsumer(
        policy,
        calendar_profile=calendar_profile,
        context_events=context_events,
    )
    evidence_by_session = {item.session_id: item for item in evidence}
    for manifest in ordered:
        consumer.start_session(manifest.session.session_id)
        summary = replay_broker_capture_session(
            root, manifest, consumers=(consumer,)
        )
        consumer.end_session()
        expected = evidence_by_session[manifest.session.session_id]
        if summary.logical_content_sha256 != expected.logical_content_sha256:
            raise BrokerDeliveryFingerprintArtifactError(
                "capture content changed between eligibility and fitting passes"
            )
    cells = _finalize_cells(consumer.cells, policy)
    support_start = min(item.first_receive_time_utc_ns for item in evidence)
    support_end = max(item.last_receive_time_utc_ns for item in evidence)
    start = (
        support_start
        if effective_start_utc_ns is None
        else effective_start_utc_ns
    )
    if supersedes is not None:
        _assert_compatible_predecessor(ordered[0], supersedes, start)
    limitations = _fit_limitations(
        decisions,
        cells,
        timeline=market_context_timeline,
        total_events=total_events,
        policy=policy,
        support_start_utc_ns=support_start,
        support_end_utc_ns=support_end,
        calendar_profile_complete=consumer.calendar_profile_complete,
    )
    identity = ordered[0].session
    return BrokerDeliveryFingerprintV1(
        adapter_id=identity.adapter_id,
        adapter_version=identity.adapter_version,
        adapter_config_sha256=identity.adapter_config_sha256,
        protocol=identity.protocol,
        environment_id=identity.environment_id,
        server_id=identity.server_id,
        account_id_sha256=identity.account_id_sha256,
        collector_id=identity.collector_id,
        collector_version=identity.collector_version,
        fit_config=policy,
        capture_evidence=tuple(evidence),
        eligibility_decisions=tuple(decisions),
        support_start_utc_ns=support_start,
        support_end_utc_ns=support_end,
        effective_start_utc_ns=start,
        effective_end_utc_ns=effective_end_utc_ns,
        cells=cells,
        supersedes_fingerprint_id=(
            None if supersedes is None else supersedes.fingerprint_id
        ),
        limitations=limitations,
    )


def compare_broker_delivery_fingerprints(
    reference: BrokerDeliveryFingerprintV1,
    candidate: BrokerDeliveryFingerprintV1,
    *,
    config: BrokerDeliveryDriftConfigV1 | None = None,
) -> BrokerDeliveryFingerprintComparisonV1:
    """Compare matching conditioned metrics without an aggregate score."""
    if reference.fingerprint_id == candidate.fingerprint_id:
        raise ValueError("drift comparison requires distinct fingerprints")
    policy = config or BrokerDeliveryDriftConfigV1()
    reference_cells = {item.condition.key: item for item in reference.cells}
    candidate_cells = {item.condition.key: item for item in candidate.cells}
    retained_by_status: dict[
        BrokerDeliveryDriftStatus, list[BrokerDeliveryMetricComparisonV1]
    ] = {status: [] for status in BrokerDeliveryDriftStatus}
    candidate_count = 0
    for condition_key in sorted(set(reference_cells) | set(candidate_cells)):
        reference_cell = reference_cells.get(condition_key)
        candidate_cell = candidate_cells.get(condition_key)
        reference_metrics = _metrics_by_name(reference_cell)
        candidate_metrics = _metrics_by_name(candidate_cell)
        for name in sorted(set(reference_metrics) | set(candidate_metrics)):
            comparison = _compare_metric(
                condition_key,
                reference_cell,
                candidate_cell,
                reference_metrics.get(name),
                candidate_metrics.get(name),
                policy,
            )
            candidate_count += 1
            bucket = retained_by_status[comparison.status]
            if len(bucket) < policy.max_comparisons:
                bucket.append(comparison)
    priority = (
        BrokerDeliveryDriftStatus.MATERIAL_DRIFT,
        BrokerDeliveryDriftStatus.SAMPLING_NOISE,
        BrokerDeliveryDriftStatus.UNSUPPORTED,
        BrokerDeliveryDriftStatus.STABLE,
    )
    retained = tuple(
        item for status in priority for item in retained_by_status[status]
    )[: policy.max_comparisons]
    counts: dict[str, int] = {}
    for item in retained:
        counts[item.status.value] = counts.get(item.status.value, 0) + 1
    return BrokerDeliveryFingerprintComparisonV1(
        reference_fingerprint_id=reference.fingerprint_id,
        candidate_fingerprint_id=candidate.fingerprint_id,
        drift_config=policy,
        comparison_candidate_count=candidate_count,
        comparisons=retained,
        status_counts=counts,
        truncated=candidate_count > len(retained),
    )


def write_broker_delivery_fingerprint(
    path: str | Path,
    fingerprint: BrokerDeliveryFingerprintV1,
) -> ArtifactRef:
    """Atomically publish an immutable fingerprint or verify idempotence."""
    target = Path(path)
    payload = fingerprint.to_json() + "\n"
    encoded = payload.encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        try:
            existing = target.read_bytes()
        except OSError as err:
            raise BrokerDeliveryFingerprintArtifactError(
                "could not read existing fingerprint artifact"
            ) from err
        if existing != encoded:
            raise BrokerDeliveryFingerprintArtifactError(
                "immutable fingerprint artifact already exists with other content"
            )
    else:
        partial = target.with_name(target.name + ".partial")
        try:
            with partial.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            partial.replace(target)
            _fsync_directory(target.parent)
        except OSError as err:
            try:
                partial.unlink(missing_ok=True)
            except OSError:
                pass
            raise BrokerDeliveryFingerprintArtifactError(
                "atomic fingerprint publication failed"
            ) from err
    return ArtifactRef(
        kind=BROKER_DELIVERY_FINGERPRINT_ARTIFACT_KIND,
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "fingerprint_id": fingerprint.fingerprint_id,
            "schema_version": fingerprint.schema_version,
        },
    )


def load_broker_delivery_fingerprint(
    path: str | Path,
) -> BrokerDeliveryFingerprintV1:
    """Load and identity-verify one immutable fingerprint artifact."""
    try:
        return BrokerDeliveryFingerprintV1.from_json(
            Path(path).read_text(encoding="utf-8")
        )
    except (OSError, TypeError, ValueError) as err:
        raise BrokerDeliveryFingerprintArtifactError(
            "broker delivery fingerprint artifact is invalid"
        ) from err


def _finalize_cells(
    accumulators: Mapping[str, _CellAccumulator],
    policy: BrokerDeliveryFitConfigV1,
) -> tuple[BrokerDeliveryCellV1, ...]:
    supports = {
        condition_id: item.support_count
        for condition_id, item in accumulators.items()
    }
    cells: list[BrokerDeliveryCellV1] = []
    for accumulator in sorted(
        accumulators.values(), key=lambda item: item.condition.key
    ):
        parents = _backoff_conditions(accumulator.condition)
        parent_ids = tuple(item.condition_id for item in parents)
        support = accumulator.support_count
        effective: str | None
        if support >= policy.min_cell_support:
            status = BrokerDeliverySupportStatus.SUPPORTED
            effective = accumulator.condition.condition_id
            limitations: tuple[str, ...] = ()
        else:
            effective = next(
                (
                    condition_id
                    for condition_id in parent_ids
                    if supports.get(condition_id, 0) >= policy.min_cell_support
                ),
                None,
            )
            if effective is None:
                status = BrokerDeliverySupportStatus.UNSUPPORTED
                limitations = ("insufficient_support_no_qualified_backoff",)
            else:
                status = BrokerDeliverySupportStatus.BACKED_OFF
                limitations = ("insufficient_support_using_parent_condition",)
        cells.append(
            BrokerDeliveryCellV1(
                condition=accumulator.condition,
                support_count=support,
                support_status=status,
                backoff_condition_ids=parent_ids,
                effective_condition_id=effective,
                metrics=tuple(
                    metric.to_metric(policy.quantiles)
                    for _, metric in sorted(accumulator.metrics.items())
                ),
                limitations=limitations,
            )
        )
    return tuple(cells)


def _backoff_conditions(
    condition: BrokerDeliveryConditionV1,
) -> tuple[BrokerDeliveryConditionV1, ...]:
    dimensions = condition.dimensions
    if not dimensions:
        return ()
    parents: list[BrokerDeliveryConditionV1] = []
    if len(dimensions) > 1 and "symbol" in dimensions:
        parents.append(
            BrokerDeliveryConditionV1({"symbol": dimensions["symbol"]})
        )
        non_symbol = {
            key: value for key, value in dimensions.items() if key != "symbol"
        }
        parents.append(BrokerDeliveryConditionV1(non_symbol))
    parents.append(BrokerDeliveryConditionV1({}))
    unique: dict[str, BrokerDeliveryConditionV1] = {}
    for parent in parents:
        if parent.condition_id != condition.condition_id:
            unique.setdefault(parent.condition_id, parent)
    return tuple(unique.values())


def _compare_metric(
    condition_key: str,
    reference_cell: BrokerDeliveryCellV1 | None,
    candidate_cell: BrokerDeliveryCellV1 | None,
    reference: BrokerDeliveryMetricV1 | None,
    candidate: BrokerDeliveryMetricV1 | None,
    policy: BrokerDeliveryDriftConfigV1,
) -> BrokerDeliveryMetricComparisonV1:
    condition_id = (
        reference_cell.condition.condition_id
        if reference_cell is not None
        else (
            candidate_cell.condition.condition_id
            if candidate_cell is not None
            else condition_key
        )
    )
    name = reference.name if reference is not None else candidate.name  # type: ignore[union-attr]
    reference_support = 0 if reference is None else reference.support_count
    candidate_support = 0 if candidate is None else candidate.support_count
    reasons: tuple[str, ...]
    if reference is None or candidate is None:
        status = BrokerDeliveryDriftStatus.UNSUPPORTED
        reasons = ("metric_missing_from_one_fingerprint",)
        reference_estimate = None if reference is None else reference.estimate
        candidate_estimate = None if candidate is None else candidate.estimate
        difference = relative = uncertainty = None
    elif (
        reference.support_count < policy.min_metric_support
        or candidate.support_count < policy.min_metric_support
        or reference.estimate is None
        or candidate.estimate is None
    ):
        status = BrokerDeliveryDriftStatus.UNSUPPORTED
        reasons = ("metric_support_below_comparison_minimum",)
        reference_estimate = reference.estimate
        candidate_estimate = candidate.estimate
        difference = relative = uncertainty = None
    else:
        reference_estimate = reference.estimate
        candidate_estimate = candidate.estimate
        difference = abs(candidate.estimate - reference.estimate)
        scale = max(abs(reference.estimate), 10 ** (-policy.rounding_digits))
        relative = difference / scale
        reference_lower = (
            reference.estimate if reference.lower is None else reference.lower
        )
        reference_upper = (
            reference.estimate if reference.upper is None else reference.upper
        )
        candidate_lower = (
            candidate.estimate if candidate.lower is None else candidate.lower
        )
        candidate_upper = (
            candidate.estimate if candidate.upper is None else candidate.upper
        )
        reference_half = max(
            reference.estimate - reference_lower,
            reference_upper - reference.estimate,
        )
        candidate_half = max(
            candidate.estimate - candidate_lower,
            candidate_upper - candidate.estimate,
        )
        uncertainty = math.sqrt(reference_half**2 + candidate_half**2)
        absolute_threshold = policy.absolute_material_thresholds.get(
            name, policy.default_absolute_material_threshold
        )
        if difference == 0:
            status = BrokerDeliveryDriftStatus.STABLE
            reasons = ()
        elif difference <= uncertainty:
            status = BrokerDeliveryDriftStatus.SAMPLING_NOISE
            reasons = ("uncertainty_intervals_overlap",)
        elif (
            absolute_threshold > 0 and difference >= absolute_threshold
        ) or relative >= policy.relative_material_threshold:
            status = BrokerDeliveryDriftStatus.MATERIAL_DRIFT
            reasons = ("difference_exceeds_uncertainty_and_effect_threshold",)
        else:
            status = BrokerDeliveryDriftStatus.SAMPLING_NOISE
            reasons = ("effect_below_material_threshold",)
    return BrokerDeliveryMetricComparisonV1(
        condition_id=condition_id,
        condition_key=condition_key,
        metric_name=name,
        reference_support_count=reference_support,
        candidate_support_count=candidate_support,
        reference_estimate=_rounded(reference_estimate, policy.rounding_digits),
        candidate_estimate=_rounded(candidate_estimate, policy.rounding_digits),
        absolute_difference=_rounded(difference, policy.rounding_digits),
        relative_difference=_rounded(relative, policy.rounding_digits),
        combined_uncertainty=_rounded(uncertainty, policy.rounding_digits),
        status=status,
        reason_codes=reasons,
    )


def _metrics_by_name(
    cell: BrokerDeliveryCellV1 | None,
) -> dict[str, BrokerDeliveryMetricV1]:
    return {} if cell is None else {item.name: item for item in cell.metrics}


def _assert_compatible_capture_identity(
    manifests: Sequence[BrokerCaptureSessionManifestV1],
) -> None:
    first = manifests[0].session
    fields = (
        "adapter_id",
        "adapter_version",
        "adapter_config_sha256",
        "protocol",
        "environment_id",
        "server_id",
        "account_id_sha256",
        "collector_id",
        "collector_version",
    )
    for manifest in manifests[1:]:
        if any(
            getattr(manifest.session, field_name) != getattr(first, field_name)
            for field_name in fields
        ):
            raise BrokerDeliveryFingerprintIdentityError(
                "capture manifests describe different broker delivery identities"
            )


def _assert_compatible_predecessor(
    manifest: BrokerCaptureSessionManifestV1,
    predecessor: BrokerDeliveryFingerprintV1,
    effective_start_utc_ns: int,
) -> None:
    session = manifest.session
    fields = (
        "adapter_id",
        "adapter_version",
        "adapter_config_sha256",
        "protocol",
        "environment_id",
        "server_id",
        "account_id_sha256",
        "collector_id",
        "collector_version",
    )
    if any(
        getattr(session, field_name) != getattr(predecessor, field_name)
        for field_name in fields
    ):
        raise BrokerDeliveryFingerprintIdentityError(
            "successor profile broker identity differs from predecessor"
        )
    if effective_start_utc_ns <= predecessor.effective_start_utc_ns:
        raise BrokerDeliveryFingerprintIdentityError(
            "successor effective start must follow predecessor effective start"
        )


def _bounded_context_events(
    timeline: MarketContextTimelineV1 | None,
    policy: BrokerDeliveryFitConfigV1,
) -> tuple[MarketContextEventV1, ...]:
    if timeline is None:
        return ()
    if len(timeline.events) > policy.max_market_context_events:
        raise BrokerDeliveryResourceLimitError(
            "market context timeline exceeds max_market_context_events"
        )
    return tuple(timeline.events)


def _context_event_matches(
    event: MarketContextEventV1,
    timestamp_ns: int,
    symbol: str,
) -> bool:
    if not (
        event.event_time_ns - event.pre_event_ns
        <= timestamp_ns
        < event.event_time_ns + event.post_event_ns
    ):
        return False
    if symbol in event.affected_symbols:
        return True
    currencies = {symbol[:3], symbol[3:6]} if len(symbol) >= 6 else set()
    return bool(currencies.intersection(event.affected_currencies))


def _partition_hashes_digest(
    manifest: BrokerCaptureSessionManifestV1,
) -> str:
    payload: dict[str, JSONValue] = {
        "partitions": [
            {
                "partition_id": item.partition_id,
                "artifact_sha256": item.data_artifact.sha256,
            }
            for item in manifest.partitions
        ]
    }
    return hashlib.sha256(
        canonical_capture_json(payload).encode("utf-8")
    ).hexdigest()


def _fit_limitations(
    decisions: Sequence[BrokerCaptureEligibilityV1],
    cells: Sequence[BrokerDeliveryCellV1],
    *,
    timeline: MarketContextTimelineV1 | None,
    total_events: int,
    policy: BrokerDeliveryFitConfigV1,
    support_start_utc_ns: int,
    support_end_utc_ns: int,
    calendar_profile_complete: bool,
) -> tuple[str, ...]:
    limitations = {
        "profile_describes_broker_observation_delivery_not_market_truth",
        "cadence_uses_monotonic_receive_time",
        "calendar_conditioning_uses_utc_receive_time",
    }
    if any(
        item.status is BrokerCaptureEligibilityStatus.LIMITED
        for item in decisions
    ):
        limitations.add("one_or_more_captures_have_nonfatal_health_limitations")
    if any(
        item.support_status is not BrokerDeliverySupportStatus.SUPPORTED
        for item in cells
    ):
        limitations.add(
            "sparse_condition_cells_use_explicit_backoff_or_are_unsupported"
        )
    if total_events > policy.max_samples_per_metric:
        limitations.add(
            "large_metric_distributions_use_deterministic_bounded_samples"
        )
    if timeline is None:
        limitations.add("no_versioned_market_context_timeline_supplied")
    elif not timeline.complete:
        limitations.add("market_context_timeline_is_incomplete")
    if timeline is not None and (
        timeline.coverage_start_ns > support_start_utc_ns
        or timeline.coverage_end_ns <= support_end_utc_ns
    ):
        limitations.add(
            "market_context_timeline_does_not_cover_capture_support"
        )
    if not calendar_profile_complete:
        limitations.add("calendar_profile_is_incomplete")
    return tuple(sorted(limitations))


def _wilson_interval(rate: float, count: int) -> tuple[float, float]:
    z = 1.96
    denominator = 1 + z * z / count
    centre = (rate + z * z / (2 * count)) / denominator
    half = (
        z
        * math.sqrt(rate * (1 - rate) / count + z * z / (4 * count * count))
        / denominator
    )
    return max(0.0, centre - half), min(1.0, centre + half)


def _quantile(values: Sequence[float], probability: float) -> float:
    if len(values) == 1:
        return values[0]
    position = probability * (len(values) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _quantile_name(value: float) -> str:
    return f"q{value:.6f}".rstrip("0").rstrip(".")


def _rounded(value: float | None, digits: int) -> float | None:
    if value is None:
        return None
    rounded = round(float(value), digits)
    return 0.0 if rounded == 0 else rounded


def _rounded_required(value: float, digits: int) -> float:
    rounded = round(float(value), digits)
    return 0.0 if rounded == 0 else rounded


def _price_decimal_places(
    bid_text: str | None, ask_text: str | None
) -> int | None:
    if bid_text is None or ask_text is None:
        return None
    return max(_decimal_places(bid_text), _decimal_places(ask_text))


def _decimal_places(value: str) -> int:
    mantissa = value.lower().split("e", 1)[0]
    return len(mantissa.split(".", 1)[1]) if "." in mantissa else 0


def _has_trailing_zero(bid_text: str | None, ask_text: str | None) -> bool:
    return bool(
        bid_text
        and ask_text
        and "." in bid_text
        and "." in ask_text
        and (bid_text.endswith("0") or ask_text.endswith("0"))
    )


def _safe_tag(value: str) -> str:
    normalized = _TAG_RE.sub("_", str(value).strip()).strip("_").lower()
    return normalized[:256] or "unknown"


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        return


__all__ = [
    "BrokerDeliveryFingerprintArtifactError",
    "BrokerDeliveryFingerprintError",
    "BrokerDeliveryFingerprintIdentityError",
    "BrokerDeliveryIneligibleCaptureError",
    "BrokerDeliveryResourceLimitError",
    "assess_broker_capture_eligibility",
    "compare_broker_delivery_fingerprints",
    "fit_broker_delivery_fingerprint",
    "load_broker_delivery_fingerprint",
    "write_broker_delivery_fingerprint",
]

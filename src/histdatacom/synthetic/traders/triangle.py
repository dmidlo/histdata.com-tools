"""Source-replayed event queries, separate from OHLC/bar endpoint diagnostics.

The synchronization algorithm is owned by triangle_bar_features. This adapter
adds query scope, source replay and explicit availability assertions, not a new
join, trading simulator, empirical alignment qualification or knowledge proof.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAvailabilityBasis,
    BarFeaturePolicyV1,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.contracts import SyntheticEventOrigin
from histdatacom.synthetic.cross_currency import CrossCurrencyJoinPolicy
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    load_reconstruction_manifest,
    read_reconstruction_streams,
)
from histdatacom.synthetic.traders.inputs import bounded_clock
from histdatacom.synthetic.triangle_bar_features import (
    TRIANGLE_SYMBOLS,
    TriangleBarPolicyV1,
    TriangleQuoteTupleV1,
    scope_contains,
    synchronized_triangle_tuples,
)

MAX_TRADER_QUERY_EVENTS = 4096
MAX_TRADER_SOURCE_EVENTS = 100_000


class TraderTriangleState(str, Enum):
    """Missingness is never an invented flat/no-arbitrage observation."""

    READY = "ready"
    EMPTY_WINDOW = "empty_source_window"
    UNAVAILABLE = "availability_incomplete"
    NO_SUPPORT = "no_synchronized_support"


@dataclass(frozen=True, slots=True)
class TraderTriangleRequestV1:
    """Process-local explicit half-open query and fixed synchronization policy."""

    start_ns: int
    end_ns: int
    decision_at_ns: int
    policy_known_at_ns: int
    ensemble_member_id: str
    information_mode: InformationMode
    scope: ActivitySliceScope = ActivitySliceScope.OBSERVED
    join_policy: CrossCurrencyJoinPolicy = (
        CrossCurrencyJoinPolicy.EXACT_EVENT_TIME_NO_FORWARD_FILL
    )
    probe_symbol: str = "EURUSD"
    max_quote_age_ns: int = 0
    max_source_events: int = MAX_TRADER_SOURCE_EVENTS
    max_retained_events: int = MAX_TRADER_QUERY_EVENTS

    def __post_init__(self) -> None:
        for name in (
            "start_ns",
            "end_ns",
            "decision_at_ns",
            "policy_known_at_ns",
            "max_quote_age_ns",
        ):
            bounded_clock(getattr(self, name), name)
        if not self.start_ns < self.end_ns <= self.decision_at_ns:
            raise ValueError("event window must be closed at decision cutoff")
        if self.policy_known_at_ns > self.decision_at_ns:
            raise ValueError("future join policy cannot define past state")
        if (
            type(self.ensemble_member_id) is not str
            or not self.ensemble_member_id.strip()
            or len(self.ensemble_member_id) > 4096
        ):
            raise ValueError("member identity is empty or exceeds its bound")
        for name, maximum in (
            ("max_source_events", MAX_TRADER_SOURCE_EVENTS),
            ("max_retained_events", MAX_TRADER_QUERY_EVENTS),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} exceeds query budget")
        object.__setattr__(self, "scope", ActivitySliceScope(self.scope))
        object.__setattr__(
            self, "information_mode", InformationMode(self.information_mode)
        )
        object.__setattr__(
            self, "join_policy", CrossCurrencyJoinPolicy(self.join_policy)
        )
        if self.probe_symbol not in TRIANGLE_SYMBOLS:
            raise ValueError("unsupported triangle probe symbol")
        exact = CrossCurrencyJoinPolicy.EXACT_EVENT_TIME_NO_FORWARD_FILL
        if (self.join_policy is exact) != (self.max_quote_age_ns == 0):
            raise ValueError("join policy and quote age bound disagree")
        if (
            self.information_mode is InformationMode.EX_ANTE_SIMULATION
            and self.scope is not ActivitySliceScope.OBSERVED
        ):
            raise ValueError(
                "legacy generated products lack ex-ante source audit binding"
            )


@dataclass(frozen=True, slots=True)
class TraderQuoteAvailabilityV1:
    """Explicit event-bound assertion, never evidence of historic knowledge."""

    event_id: str
    available_at_ns: int
    known_at_ns: int
    explanation: str
    basis: BarAvailabilityBasis = BarAvailabilityBasis.DECLARED_SOURCE_CLOCK
    generated_at_ns: int | None = None

    def __post_init__(self) -> None:
        for name in ("event_id", "explanation"):
            text = getattr(self, name)
            if type(text) is not str or not text.strip() or len(text) > 4096:
                raise ValueError(f"{name} is empty or exceeds its bound")
        bounded_clock(self.available_at_ns, "available_at_ns")
        bounded_clock(self.known_at_ns, "known_at_ns")
        object.__setattr__(self, "basis", BarAvailabilityBasis(self.basis))
        if self.generated_at_ns is not None:
            bounded_clock(self.generated_at_ns, "generated_at_ns")
            if self.generated_at_ns > self.available_at_ns:
                raise ValueError("availability cannot precede generation")


@dataclass(frozen=True, slots=True)
class TraderTriangleResultV1:
    """Process-local native evidence; replay() is required for fresh verification.

    No serialization, certification flag or new artifact identity is introduced.
    Native tuple/event IDs retain their original owner. Counts are query-local;
    unavailable events are counted, not emitted as quotes or silently imputed.
    """

    request: TraderTriangleRequestV1
    source_product_manifest_id: str
    source_stream_ids: tuple[str, ...]
    source_version_ids: tuple[str, ...]
    state: TraderTriangleState
    window_event_count: int
    admitted_event_count: int
    availability: tuple[TraderQuoteAvailabilityV1, ...]
    tuples: tuple[TriangleQuoteTupleV1, ...]

    @property
    def historical_availability_verified(self) -> bool:
        return False


@runtime_checkable
class TraderTriangleReaderV1(Protocol):
    """Exact/bounded-prior event query, independent of any OHLC interval."""

    def query(
        self,
        request: TraderTriangleRequestV1,
        *,
        availability: Sequence[TraderQuoteAvailabilityV1] = (),
    ) -> TraderTriangleResultV1:
        """Return bounded native tuples and explicit support/availability state."""

    def replay(self, result: TraderTriangleResultV1) -> TraderTriangleResultV1:
        """Reopen current source bytes and reproduce the exact result."""


@dataclass(frozen=True, slots=True)
class CommittedTraderTriangleReaderV1:
    """Process-local locator adapter over verified committed native streams."""

    manifest_path: str

    def query(
        self,
        request: TraderTriangleRequestV1,
        *,
        availability: Sequence[TraderQuoteAvailabilityV1] = (),
    ) -> TraderTriangleResultV1:
        if not isinstance(request, TraderTriangleRequestV1):
            raise TypeError("triangle query requires its typed request")
        if len(availability) > MAX_TRADER_QUERY_EVENTS:
            raise ValueError("availability input exceeds query budget")
        admitted_clocks = {}
        for clock in availability:
            if not isinstance(clock, TraderQuoteAvailabilityV1):
                raise TypeError(
                    "availability requires typed event declarations"
                )
            if (
                max(clock.known_at_ns, clock.available_at_ns)
                > request.decision_at_ns
            ):
                continue
            if clock.event_id in admitted_clocks:
                raise ValueError("duplicate admitted event availability")
            admitted_clocks[clock.event_id] = clock
        manifest = load_reconstruction_manifest(self.manifest_path)
        if manifest.event_count > request.max_source_events:
            raise ValueError("total source verification exceeds event budget")
        if manifest.ensemble_member_id != request.ensemble_member_id:
            raise ValueError("query member differs from committed product")
        if {symbol.upper() for symbol in manifest.symbols} != set(
            TRIANGLE_SYMBOLS
        ):
            raise ValueError("committed source lacks exact supported triangle")
        streams = read_reconstruction_streams(self.manifest_path)
        if load_reconstruction_manifest(self.manifest_path) != manifest:
            raise ValueError("source manifest changed during replay")
        window = tuple(
            e
            for stream in streams
            for e in stream.events
            if request.start_ns <= e.event_time_ns < request.end_ns
            and scope_contains(request.scope, e)
        )
        if len(window) > request.max_retained_events:
            raise ValueError("query window exceeds retained-event budget")
        rows = []
        clocks = []
        for event in window:
            event_clock = admitted_clocks.get(event.event_id)
            if event_clock is None:
                continue
            if event_clock.available_at_ns < event.event_time_ns:
                raise ValueError("quote availability precedes its event clock")
            if (
                request.information_mode is InformationMode.EX_ANTE_SIMULATION
                and event_clock.basis
                is BarAvailabilityBasis.REPLAY_CLOCK_ASSUMPTION
            ):
                raise ValueError(
                    "replay clock assumption is not ex-ante evidence"
                )
            if event.origin is not SyntheticEventOrigin.OBSERVED:
                if event_clock.generated_at_ns is None:
                    raise ValueError(
                        "generated quote requires generation cutoff"
                    )
            rows.append(event)
            clocks.append(event_clock)
        # Native JSON is bounded before the reused join creates tuple artifacts.
        canonical_bar_feature_json({"events": [e.to_dict() for e in rows]})
        policy = TriangleBarPolicyV1(
            BarFeaturePolicyV1(request.information_mode),
            request.policy_known_at_ns,
            join_policy=request.join_policy,
            probe_symbol=request.probe_symbol,
            max_quote_age_ns=request.max_quote_age_ns,
            max_retained_events=request.max_retained_events,
        )
        tuples = synchronized_triangle_tuples(
            rows,
            start_ns=request.start_ns,
            end_ns=request.end_ns,
            scope=request.scope,
            policy=policy,
        )
        canonical_bar_feature_json({"tuples": [t.to_dict() for t in tuples]})
        if not window:
            state = TraderTriangleState.EMPTY_WINDOW
        elif len(rows) != len(window):
            state = TraderTriangleState.UNAVAILABLE
        elif not tuples:
            state = TraderTriangleState.NO_SUPPORT
        else:
            state = TraderTriangleState.READY
        return TraderTriangleResultV1(
            request,
            manifest.manifest_id,
            tuple(sorted(s.stream_id for s in streams)),
            tuple(sorted({v for s in streams for v in s.source_version_ids})),
            state,
            len(window),
            len(rows),
            tuple(sorted(clocks, key=lambda c: c.event_id)),
            tuples,
        )

    def replay(self, result: TraderTriangleResultV1) -> TraderTriangleResultV1:
        if not isinstance(result, TraderTriangleResultV1):
            raise TypeError("triangle replay requires a typed result")
        expected = self.query(result.request, availability=result.availability)
        if expected != result:
            raise ValueError(
                "triangle result differs from current source replay"
            )
        return expected

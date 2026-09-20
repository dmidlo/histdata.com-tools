"""Opt-in, replayable three-leg closed-bar diagnostics; never certification.

OHLC bounds are non-simultaneous interval diagnostics. Executable-side gaps
instead retain actual event tuples, their probe clocks and endpoint ages.
Availability/session declarations remain assertions, not historical proof.
"""

from __future__ import annotations

import hashlib
import math
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from statistics import median
from typing import ClassVar

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAbsenceDeclarationV1,
    BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1,
    BarFeatureSourceV1,
    CausalBarSnapshotV1,
    _Artifact,
    _ns,
    _parse_json,
    _text,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.bars import (
    DERIVED_BAR_EVENT_COLUMNS,
    STANDARD_DERIVED_BAR_INTERVALS,
    DerivedBarV1,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.cross_currency import CrossCurrencyJoinPolicy
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    load_reconstruction_manifest,
    read_reconstruction_streams,
)

TRIANGLE_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
TRIANGLE_FEATURE_REGISTRY = "histdatacom.triangle-bar-features.v1"
MAX_TRIANGLE_EVENTS = 4096
_OBS = ActivitySliceScope.OBSERVED
_EXACT = CrossCurrencyJoinPolicy.EXACT_EVENT_TIME_NO_FORWARD_FILL


def exact_legacy_payload(text: str, restored: Mapping[str, object]) -> None:
    """Reject discarded keys and every coercion at a new legacy-wire seam."""
    if canonical_bar_feature_json(_parse_json(text)) != (
        canonical_bar_feature_json(restored)
    ):
        raise ValueError("legacy evidence is not exact canonical content")


def decode_triangle_event(text: str) -> SyntheticEventV1:
    event = SyntheticEventV1.from_dict(_parse_json(text))
    exact_legacy_payload(text, event.to_dict())
    return event


def event_order(event: SyntheticEventV1) -> tuple[str, int, int, str]:
    return (
        event.symbol,
        event.event_time_ns,
        event.event_sequence,
        event.event_id,
    )


def scope_contains(scope: ActivitySliceScope, event: SyntheticEventV1) -> bool:
    return scope is ActivitySliceScope.MERGED or (
        (scope is _OBS) == (event.origin is SyntheticEventOrigin.OBSERVED)
    )


@dataclass(frozen=True, slots=True)
class TriangleBarPolicyV1(_Artifact):
    bar_policy: BarFeaturePolicyV1
    known_at_ns: int
    join_policy: CrossCurrencyJoinPolicy = _EXACT
    probe_symbol: str = "EURUSD"
    max_quote_age_ns: int = 0
    max_endpoint_age_ns: int = 1_000_000_000
    robust_epsilon: float = 1e-12
    max_retained_events: int = MAX_TRIANGLE_EVENTS
    registry_version: str = TRIANGLE_FEATURE_REGISTRY
    KIND: ClassVar[str] = "triangle-policy"

    def _validate(self) -> None:
        _ns(self.known_at_ns, "policy known_at_ns")
        _ns(self.max_quote_age_ns, "max_quote_age_ns")
        _ns(self.max_endpoint_age_ns, "max_endpoint_age_ns")
        if self.probe_symbol not in TRIANGLE_SYMBOLS:
            raise ValueError("triangle probe symbol is unsupported")
        if self.join_policy is _EXACT and self.max_quote_age_ns:
            raise ValueError("exact alignment cannot carry stale quotes")
        if not math.isfinite(self.robust_epsilon) or self.robust_epsilon <= 0:
            raise ValueError("robust epsilon must be finite and positive")
        if not 1 <= self.max_retained_events <= MAX_TRIANGLE_EVENTS:
            raise ValueError("triangle retained-event budget is invalid")
        if self.registry_version != TRIANGLE_FEATURE_REGISTRY:
            raise ValueError("unsupported triangle feature registry")


@dataclass(frozen=True, slots=True)
class TriangleBarStratumV1(_Artifact):
    """Cutoff-qualified session assertion for one exact immutable bar.

    Observed v1 rows forbid feed_epoch_id, so their epoch is an assertion
    bound to exact bar/source versions. Non-null generated epochs must match.
    """

    bar_id: str
    session: str
    known_at_ns: int
    explanation: str
    feed_epoch_id: str
    KIND: ClassVar[str] = "triangle-stratum"

    def _validate(self) -> None:
        _text(self.bar_id, "bar_id")
        _text(self.session, "session")
        _text(self.explanation, "stratum explanation")
        _text(self.feed_epoch_id, "stratum feed epoch")
        _ns(self.known_at_ns, "stratum known_at_ns")


@dataclass(frozen=True, slots=True)
class TriangleQuoteTupleV1(_Artifact):
    event_json: tuple[str, ...]
    probe_time_ns: int
    endpoint_ns: int
    KIND: ClassVar[str] = "triangle-quote-tuple"

    def _validate(self) -> None:
        self.to_json()
        if len(self.event_json) != 3:
            raise ValueError("tuple requires exactly three retained events")
        _ns(self.probe_time_ns, "probe_time_ns")
        _ns(self.endpoint_ns, "endpoint_ns")
        events = self.events
        if tuple(e.symbol.upper() for e in events) != TRIANGLE_SYMBOLS:
            raise ValueError("tuple requires exact ordered triangle legs")
        if self.probe_time_ns != max(e.event_time_ns for e in events):
            raise ValueError("tuple probe must be an actual latest event")
        if self.endpoint_ns < self.probe_time_ns:
            raise ValueError("tuple cannot use a future quote")
        if len({(e.run_id, e.ensemble_member_id) for e in events}) != 1:
            raise ValueError("tuple mixes run/member evidence")

    @property
    def events(self) -> tuple[SyntheticEventV1, ...]:
        return tuple(decode_triangle_event(text) for text in self.event_json)

    @property
    def ages_ns(self) -> tuple[int, ...]:
        return tuple(self.probe_time_ns - e.event_time_ns for e in self.events)


def synchronized_triangle_tuples(
    events: Sequence[SyntheticEventV1],
    *,
    start_ns: int,
    end_ns: int,
    scope: ActivitySliceScope,
    policy: TriangleBarPolicyV1,
) -> tuple[TriangleQuoteTupleV1, ...]:
    """Exact ordinal or fixed-probe latest-prior tuples, without gap carry.

    Mirrors the #515 event-time/ordinal semantics. This diagnostic policy does
    not assert that its age bound passed the empirical qualification campaign.
    """
    if len(events) > policy.max_retained_events:
        raise ValueError("triangle tuple input exceeds the retained budget")
    rows = {
        symbol: sorted(
            (
                e
                for e in events
                if e.symbol.upper() == symbol
                and start_ns <= e.event_time_ns < end_ns
                and scope_contains(scope, e)
            ),
            key=event_order,
        )
        for symbol in TRIANGLE_SYMBOLS
    }
    tuples: list[tuple[SyntheticEventV1, ...]] = []
    if policy.join_policy is _EXACT:
        indexed: dict[str, dict[int, list[SyntheticEventV1]]] = {}
        for symbol, stream in rows.items():
            indexed[symbol] = {}
            for event in stream:
                indexed[symbol].setdefault(event.event_time_ns, []).append(
                    event
                )
        common = set.intersection(*(set(v) for v in indexed.values()))
        for time in sorted(common):
            count = min(len(indexed[s][time]) for s in TRIANGLE_SYMBOLS)
            for ordinal in range(count):
                tuples.append(
                    tuple(indexed[s][time][ordinal] for s in TRIANGLE_SYMBOLS)
                )
    else:
        times = {
            s: [e.event_time_ns for e in stream] for s, stream in rows.items()
        }
        for probe in rows[policy.probe_symbol]:
            chosen = []
            for symbol in TRIANGLE_SYMBOLS:
                index = bisect_right(times[symbol], probe.event_time_ns) - 1
                if index < 0:
                    break
                event = (
                    probe
                    if symbol == policy.probe_symbol
                    else rows[symbol][index]
                )
                if (
                    probe.event_time_ns - event.event_time_ns
                    > policy.max_quote_age_ns
                ):
                    break
                chosen.append(event)
            if len(chosen) == 3:
                tuples.append(tuple(chosen))
    return tuple(
        TriangleQuoteTupleV1(
            tuple(e.to_json() for e in chosen),
            max(e.event_time_ns for e in chosen),
            end_ns,
        )
        for chosen in tuples
    )


@dataclass(frozen=True, slots=True)
class TriangleFeatureDefinitionV1(_Artifact):
    name: str
    units: str
    required_closed_bars_per_leg: int
    construction: str
    lag_bars: int = 0
    availability_rule: str = (
        "maximum of exact dependency bar availability/knowledge and policy knowledge; robust values also include stratum knowledge"
    )
    KIND: ClassVar[str] = "triangle-feature-definition"

    def _validate(self) -> None:
        _text(self.name, "feature name")
        _text(self.units, "units")
        _text(self.construction, "construction")
        if self.required_closed_bars_per_leg not in (1, 2, 4):
            raise ValueError("unsupported triangle dependency span")
        if self.lag_bars != 0:
            raise ValueError(
                "v1 outputs own the current closed bar; robust baseline alone is prior-only"
            )
        _text(self.availability_rule, "availability rule")


@lru_cache(maxsize=1)
def triangle_feature_definitions() -> tuple[TriangleFeatureDefinitionV1, ...]:
    specs = [
        (
            "residual_open",
            "log_ratio",
            1,
            "U.open-X.open-G.open in log prices; asynchronous OHLC diagnostic",
        ),
        (
            "residual_close",
            "log_ratio",
            1,
            "U.close-X.close-G.close in log prices; asynchronous OHLC diagnostic",
        ),
        (
            "residual_lower_bound",
            "log_ratio",
            1,
            "log(U.low)-log(X.high)-log(G.high); nonsimultaneous bound",
        ),
        (
            "residual_upper_bound",
            "log_ratio",
            1,
            "log(U.high)-log(X.low)-log(G.low); nonsimultaneous bound",
        ),
        (
            "residual_range_bound",
            "log_ratio",
            1,
            "upper bound minus lower bound, not sampled extrema",
        ),
        (
            "residual_change",
            "log_ratio",
            2,
            "current minus previous closed-bar residual",
        ),
        (
            "residual_robust_zscore_3",
            "dimensionless",
            4,
            "current residual minus prior-three median over max(1.4826*MAD,policy epsilon); same epoch/session strata",
        ),
        (
            "robust_scale_floored",
            "indicator",
            4,
            "one when prior-only robust scale uses declared epsilon",
        ),
        (
            "common_return",
            "log_return",
            2,
            "equal-weight arithmetic mean of three correlated pair returns; not a learned latent factor",
        ),
        (
            "spread_asymmetry",
            "log_ratio",
            1,
            "log(X.ask/X.bid)-log(U.ask/U.bid)-log(G.ask/G.bid) at asynchronous bar close",
        ),
        (
            "common_activity",
            "events_per_second",
            1,
            "sum of three event counts divided by three full interval durations; no volume claim",
        ),
        (
            "observed_support_proportion",
            "fraction",
            1,
            "sum observed events over sum all events; not an evidence weight",
        ),
        (
            "synthetic_support_proportion",
            "fraction",
            1,
            "sum synthetic events over sum all events; not an evidence weight",
        ),
        (
            "quote_age_max_ns",
            "nanoseconds",
            1,
            "maximum selected quote age at final supported probe",
        ),
        (
            "endpoint_age_max_ns",
            "nanoseconds",
            1,
            "maximum selected quote age at logical closed boundary",
        ),
    ]
    for symbol in TRIANGLE_SYMBOLS:
        specs.extend(
            [
                (
                    symbol.lower() + "_return",
                    "log_return",
                    2,
                    "log(current mid close)-log(previous mid close)",
                ),
                (
                    symbol.lower() + "_relative_spread",
                    "fraction",
                    1,
                    "close spread divided by close midpoint",
                ),
            ]
        )
    for left, right in (
        ("eurusd", "eurgbp"),
        ("eurusd", "gbpusd"),
        ("eurgbp", "gbpusd"),
    ):
        specs.extend(
            [
                (
                    left + "_minus_" + right + "_return",
                    "log_return",
                    2,
                    "left close return minus right close return",
                ),
                (
                    left + "_over_" + right + "_activity",
                    "ratio",
                    1,
                    "left event count divided by right event count; no volume claim",
                ),
            ]
        )
    for endpoint in ("first", "last"):
        for name, formula in (
            ("synchronized_residual", "log(U.mid)-log(X.mid)-log(G.mid)"),
            ("sell_direct_gap", "log(X.bid)+log(G.bid)-log(U.ask)"),
            ("buy_direct_gap", "log(U.bid)-log(X.ask)-log(G.ask)"),
        ):
            specs.append(
                (
                    endpoint + "_" + name,
                    "log_ratio",
                    1,
                    formula
                    + " on actual event tuple; excludes fees, liquidity and execution certainty",
                )
            )
    return tuple(TriangleFeatureDefinitionV1(*spec) for spec in specs)


@dataclass(frozen=True, slots=True)
class TriangleFeatureValueV1(_Artifact):
    name: str
    value: float | int | None
    state: str
    available_at_ns: int | None
    source_bar_ids: tuple[str, ...]
    source_event_ids: tuple[str, ...] = ()
    KIND: ClassVar[str] = "triangle-feature-value"

    def _validate(self) -> None:
        _text(self.name, "feature name")
        _text(self.state, "feature state")
        if (self.value is None) != (self.state != "available"):
            raise ValueError("triangle missing-state/value mismatch")
        if (self.available_at_ns is None) != (self.value is None):
            raise ValueError("triangle availability/value mismatch")
        if self.value is not None and not math.isfinite(self.value):
            raise ValueError("triangle value must be finite")


@dataclass(frozen=True, slots=True)
class TriangleBarCellV1(_Artifact):
    scope: ActivitySliceScope
    interval_code: str
    bar_start_ns: int
    bar_end_ns: int
    values: tuple[TriangleFeatureValueV1, ...]
    first_tuple: TriangleQuoteTupleV1 | None
    last_tuple: TriangleQuoteTupleV1 | None
    KIND: ClassVar[str] = "triangle-cell"

    def _validate(self) -> None:
        duration = STANDARD_DERIVED_BAR_INTERVALS.get(self.interval_code)
        if (
            duration is None
            or self.bar_start_ns % duration
            or self.bar_end_ns - self.bar_start_ns != duration
        ):
            raise ValueError("triangle cell requires aligned half-open bounds")
        if tuple(v.name for v in self.values) != tuple(
            d.name for d in triangle_feature_definitions()
        ):
            raise ValueError("triangle cell differs from frozen registry")

    def feature(self, name: str) -> TriangleFeatureValueV1:
        for value in self.values:
            if value.name == name:
                return value
        raise ValueError("unknown triangle feature")


def _residual(values: Sequence[float]) -> float:
    x, u, g = values
    return math.log(u) - math.log(x) - math.log(g)


def _bar_events(
    bar: DerivedBarV1, events: Sequence[SyntheticEventV1]
) -> tuple[SyntheticEventV1, ...]:
    return tuple(
        e
        for e in events
        if e.symbol == bar.symbol
        and bar.bar_start_ns <= e.event_time_ns < bar.bar_end_ns
        and scope_contains(bar.scope, e)
    )


def _cells(
    snapshots: tuple[CausalBarSnapshotV1, ...],
    policy: TriangleBarPolicyV1,
    events: tuple[SyntheticEventV1, ...],
    strata: tuple[TriangleBarStratumV1, ...],
) -> tuple[TriangleBarCellV1, ...]:
    cutoff = snapshots[0].decision_time_ns
    bars = {
        (b.symbol.upper(), b.scope, b.interval_code, b.bar_start_ns): b
        for s in snapshots
        for b in s.bars
    }
    clocks = {
        a.bar_id: max(a.available_at_ns, a.known_at_ns)
        for s in snapshots
        for a in s.availability
    }
    labels = {s.bar_id: s for s in strata}
    results = []
    for interval in policy.bar_policy.intervals:
        duration = STANDARD_DERIVED_BAR_INTERVALS[interval]
        boundary = cutoff // duration * duration
        for scope in policy.bar_policy.scopes:
            history = [
                tuple(
                    bars.get((symbol, scope, interval, boundary - n * duration))
                    for symbol in TRIANGLE_SYMBOLS
                )
                for n in (4, 3, 2, 1)
            ]
            current = history[-1]
            complete = all(
                b is not None
                and not b.is_partial_start
                and not b.is_partial_end
                for b in current
            )
            computed: dict[
                str,
                tuple[
                    float | int, tuple[DerivedBarV1, ...], tuple[str, ...], int
                ],
            ] = {}
            reasons: dict[str, str] = {}
            first = last = None
            if complete:
                active = tuple(b for b in current if b is not None)

                def put(
                    name: str,
                    value: float | int,
                    support: tuple[DerivedBarV1, ...] = active,
                    ids: tuple[str, ...] = (),
                    extra_clock: int = 0,
                ) -> None:
                    if not math.isfinite(value):
                        reasons[name] = "unrepresentable"
                    else:
                        computed[name] = (
                            value,
                            support,
                            ids,
                            max(
                                policy.known_at_ns,
                                extra_clock,
                                *(clocks[b.bar_id] for b in support),
                            ),
                        )

                put("residual_open", _residual([b.mid_open for b in active]))
                put("residual_close", _residual([b.mid_close for b in active]))
                x, u, g = active
                lower = (
                    math.log(u.mid_low)
                    - math.log(x.mid_high)
                    - math.log(g.mid_high)
                )
                upper = (
                    math.log(u.mid_high)
                    - math.log(x.mid_low)
                    - math.log(g.mid_low)
                )
                put("residual_lower_bound", lower)
                put("residual_upper_bound", upper)
                put("residual_range_bound", upper - lower)
                put(
                    "spread_asymmetry",
                    math.log(x.ask_close)
                    - math.log(x.bid_close)
                    - math.log(u.ask_close)
                    + math.log(u.bid_close)
                    - math.log(g.ask_close)
                    + math.log(g.bid_close),
                )
                counts = sum(b.event_count for b in active)
                put("common_activity", counts / (3 * duration / 1e9))
                put(
                    "observed_support_proportion",
                    sum(b.observed_event_count for b in active) / counts,
                )
                put(
                    "synthetic_support_proportion",
                    sum(b.synthetic_event_count for b in active) / counts,
                )
                for symbol, bar in zip(TRIANGLE_SYMBOLS, active, strict=True):
                    put(
                        symbol.lower() + "_relative_spread",
                        bar.spread_close / bar.mid_close,
                    )
                for left, right in ((1, 0), (1, 2), (0, 2)):
                    put(
                        TRIANGLE_SYMBOLS[left].lower()
                        + "_over_"
                        + TRIANGLE_SYMBOLS[right].lower()
                        + "_activity",
                        active[left].event_count / active[right].event_count,
                    )
                previous = history[-2]
                if all(
                    b is not None
                    and not b.is_partial_start
                    and not b.is_partial_end
                    for b in previous
                ):
                    prior = tuple(b for b in previous if b is not None)
                    support = prior + active
                    returns = [
                        math.log(b.mid_close) - math.log(a.mid_close)
                        for a, b in zip(prior, active, strict=True)
                    ]
                    put(
                        "residual_change",
                        _residual([b.mid_close for b in active])
                        - _residual([b.mid_close for b in prior]),
                        support,
                    )
                    put("common_return", sum(returns) / 3, support)
                    for symbol, value in zip(
                        TRIANGLE_SYMBOLS, returns, strict=True
                    ):
                        put(symbol.lower() + "_return", value, support)
                    for left, right in ((1, 0), (1, 2), (0, 2)):
                        put(
                            TRIANGLE_SYMBOLS[left].lower()
                            + "_minus_"
                            + TRIANGLE_SYMBOLS[right].lower()
                            + "_return",
                            returns[left] - returns[right],
                            support,
                        )
                flat = tuple(
                    b for group in history for b in group if b is not None
                )
                if len(flat) == 12 and all(
                    not b.is_partial_start and not b.is_partial_end
                    for b in flat
                ):
                    keys = []
                    for group in history:
                        key: list[tuple[str, str, tuple[str, ...]]] = []
                        for history_bar in group:
                            assert history_bar is not None
                            stratum = labels.get(history_bar.bar_id)
                            epochs = {
                                e.feed_epoch_id
                                for e in _bar_events(history_bar, events)
                            }
                            if stratum is None or (
                                epochs - {None, stratum.feed_epoch_id}
                            ):
                                key = []
                                break
                            key.append(
                                (
                                    stratum.feed_epoch_id,
                                    stratum.session,
                                    history_bar.source_version_ids,
                                )
                            )
                        keys.append(tuple(key))
                    if all(keys) and len(set(keys)) == 1:
                        residuals = [
                            _residual(
                                [b.mid_close for b in group if b is not None]
                            )
                            for group in history
                        ]
                        center = median(residuals[:3])
                        scale = 1.4826 * median(
                            [abs(v - center) for v in residuals[:3]]
                        )
                        extra = max(labels[b.bar_id].known_at_ns for b in flat)
                        put(
                            "residual_robust_zscore_3",
                            (residuals[-1] - center)
                            / max(scale, policy.robust_epsilon),
                            flat,
                            extra_clock=extra,
                        )
                        put(
                            "robust_scale_floored",
                            float(scale < policy.robust_epsilon),
                            flat,
                            extra_clock=extra,
                        )
                    else:
                        reasons["residual_robust_zscore_3"] = reasons[
                            "robust_scale_floored"
                        ] = "unknown_or_mixed_stratum"
                tuples = synchronized_triangle_tuples(
                    events,
                    start_ns=boundary - duration,
                    end_ns=boundary,
                    scope=scope,
                    policy=policy,
                )
                if tuples:
                    first, last = tuples[0], tuples[-1]
                    put(
                        "quote_age_max_ns",
                        max(last.ages_ns),
                        ids=tuple(e.event_id for e in last.events),
                    )
                    put(
                        "endpoint_age_max_ns",
                        max(boundary - e.event_time_ns for e in last.events),
                        ids=tuple(e.event_id for e in last.events),
                    )
                    for label, point in (("first", first), ("last", last)):
                        chosen = point.events
                        ids = tuple(e.event_id for e in chosen)
                        # First tuple is a supported probe, never called bar-open.
                        stale = (
                            label == "last"
                            and max(boundary - e.event_time_ns for e in chosen)
                            > policy.max_endpoint_age_ns
                        )
                        if stale:
                            for name in (
                                "synchronized_residual",
                                "sell_direct_gap",
                                "buy_direct_gap",
                            ):
                                reasons[label + "_" + name] = "stale_endpoint"
                            continue
                        ex, eu, eg = chosen
                        put(
                            label + "_synchronized_residual",
                            _residual([e.bid / 2 + e.ask / 2 for e in chosen]),
                            ids=ids,
                        )
                        put(
                            label + "_sell_direct_gap",
                            math.log(ex.bid)
                            + math.log(eg.bid)
                            - math.log(eu.ask),
                            ids=ids,
                        )
                        put(
                            label + "_buy_direct_gap",
                            math.log(eu.bid)
                            - math.log(ex.ask)
                            - math.log(eg.ask),
                            ids=ids,
                        )
            values = []
            for definition in triangle_feature_definitions():
                name = definition.name
                if name in computed:
                    value, support, ids, available = computed[name]
                    values.append(
                        TriangleFeatureValueV1(
                            name,
                            value,
                            "available",
                            available,
                            tuple(b.bar_id for b in support),
                            ids,
                        )
                    )
                else:
                    state = reasons.get(
                        name,
                        (
                            "insufficient_warmup"
                            if definition.required_closed_bars_per_leg > 1
                            else "no_synchronized_support"
                        ),
                    )
                    if not complete:
                        state = "missing_or_partial_leg"
                    values.append(
                        TriangleFeatureValueV1(name, None, state, None, ())
                    )
            results.append(
                TriangleBarCellV1(
                    scope,
                    interval,
                    boundary - duration,
                    boundary,
                    tuple(values),
                    first,
                    last,
                )
            )
    return tuple(results)


@dataclass(frozen=True, slots=True)
class TriangleBarSnapshotV1(_Artifact):
    policy: TriangleBarPolicyV1
    leg_snapshots: tuple[CausalBarSnapshotV1, ...]
    event_json: tuple[str, ...]
    strata: tuple[TriangleBarStratumV1, ...]
    cells: tuple[TriangleBarCellV1, ...]
    historical_availability_verified: bool = False
    KIND: ClassVar[str] = "triangle-snapshot"

    def _validate(self) -> None:
        # Enforce aggregate bytes before parsing/replaying retained row JSON.
        self.to_json()
        if tuple(s.symbol for s in self.leg_snapshots) != TRIANGLE_SYMBOLS:
            raise ValueError("triangle requires exact ordered leg snapshots")
        if (
            len(
                {
                    (
                        s.decision_time_ns,
                        s.policy.artifact_id,
                        s.source_product_manifest_id,
                        s.derived_bar_manifest_id,
                    )
                    for s in self.leg_snapshots
                }
            )
            != 1
        ):
            raise ValueError(
                "triangle leg policies, cutoffs or source identities differ"
            )
        if self.leg_snapshots[0].policy != self.policy.bar_policy:
            raise ValueError("triangle policy differs from source snapshots")
        if self.policy.known_at_ns > self.decision_time_ns:
            raise ValueError("future policy cannot define historical features")
        if self.historical_availability_verified:
            raise ValueError(
                "triangle replay cannot certify historical knowledge"
            )
        if len(self.event_json) > self.policy.max_retained_events:
            raise ValueError("triangle retained-event budget exceeded")
        events = tuple(decode_triangle_event(text) for text in self.event_json)
        if events != tuple(sorted(events, key=event_order)) or len(
            {e.event_id for e in events}
        ) != len(events):
            raise ValueError(
                "triangle source events are duplicate or unordered"
            )
        bars = tuple(b for s in self.leg_snapshots for b in s.bars)
        covered = set()
        for bar in bars:
            rows = _bar_events(bar, events)
            digest = hashlib.sha256(b"derived-bar-events-v1\n")
            for event in rows:
                payload = event.to_dict()
                digest.update(
                    canonical_contract_json(
                        {
                            name: payload[name]
                            for name in DERIVED_BAR_EVENT_COLUMNS
                        }
                    ).encode()
                )
                digest.update(b"\n")
                covered.add(event.event_id)
            if (
                len(rows) != bar.event_count
                or digest.hexdigest() != bar.event_content_sha256
            ):
                raise ValueError(
                    "triangle event support differs from exact bar lineage"
                )
        if covered != {e.event_id for e in events}:
            raise ValueError("triangle has orphan or future event evidence")
        bar_ids = {b.bar_id for b in bars}
        if self.strata != tuple(
            sorted(self.strata, key=lambda s: s.bar_id)
        ) or len({s.bar_id for s in self.strata}) != len(self.strata):
            raise ValueError("triangle strata are duplicate or unordered")
        if any(
            s.bar_id not in bar_ids or s.known_at_ns > self.decision_time_ns
            for s in self.strata
        ):
            raise ValueError(
                "triangle has orphan or future stratum declarations"
            )
        if self.cells != _cells(
            self.leg_snapshots, self.policy, events, self.strata
        ):
            raise ValueError(
                "triangle feature cells do not replay exact evidence"
            )

    @property
    def decision_time_ns(self) -> int:
        return self.leg_snapshots[0].decision_time_ns

    def cell(
        self, scope: ActivitySliceScope, interval: str
    ) -> TriangleBarCellV1:
        for cell in self.cells:
            if cell.scope is scope and cell.interval_code == interval:
                return cell
        raise ValueError("triangle snapshot has no requested cell")


@dataclass(frozen=True, slots=True)
class TriangleBarSourceV1:
    bar_source: BarFeatureSourceV1

    def snapshot(
        self,
        *,
        decision_time_ns: int,
        policy: TriangleBarPolicyV1,
        availability: Sequence[BarAvailabilityDeclarationV1] = (),
        absences: Sequence[BarAbsenceDeclarationV1] = (),
        strata: Sequence[TriangleBarStratumV1] = (),
    ) -> TriangleBarSnapshotV1:
        if len(strata) > MAX_TRIANGLE_EVENTS:
            raise ValueError("triangle stratum budget exceeded")
        legs = tuple(
            self.bar_source.snapshot(
                symbol=symbol,
                decision_time_ns=decision_time_ns,
                policy=policy.bar_policy,
                availability=availability,
                absences=absences,
            )
            for symbol in TRIANGLE_SYMBOLS
        )
        declared = load_reconstruction_manifest(
            self.bar_source.reconstruction_manifest_path
        )
        if declared.event_count > policy.bar_policy.max_source_events:
            raise ValueError("triangle total source event budget exceeded")
        if declared.manifest_id != legs[0].source_product_manifest_id:
            raise ValueError("triangle source changed during replay")
        streams = read_reconstruction_streams(
            self.bar_source.reconstruction_manifest_path
        )
        bars = tuple(b for leg in legs for b in leg.bars)
        selected = tuple(
            sorted(
                (
                    e
                    for stream in streams
                    for e in stream.events
                    if any(
                        e.symbol == b.symbol
                        and b.bar_start_ns <= e.event_time_ns < b.bar_end_ns
                        and scope_contains(b.scope, e)
                        for b in bars
                    )
                ),
                key=event_order,
            )
        )
        if len(selected) > policy.max_retained_events:
            raise ValueError("triangle retained-event budget exceeded")
        bar_ids = {b.bar_id for b in bars}
        admitted = tuple(
            sorted(
                (
                    s
                    for s in strata
                    if s.known_at_ns <= decision_time_ns and s.bar_id in bar_ids
                ),
                key=lambda s: s.bar_id,
            )
        )
        return TriangleBarSnapshotV1(
            policy,
            legs,
            tuple(e.to_json() for e in selected),
            admitted,
            _cells(legs, policy, selected, admitted),
        )

    def verify_snapshot(
        self,
        snapshot: TriangleBarSnapshotV1,
        *,
        information_mode: InformationMode,
    ) -> None:
        if snapshot.policy.bar_policy.information_mode is not InformationMode(
            information_mode
        ):
            raise ValueError("triangle consumer information mode differs")
        expected = self.snapshot(
            decision_time_ns=snapshot.decision_time_ns,
            policy=snapshot.policy,
            availability=tuple(
                a for s in snapshot.leg_snapshots for a in s.availability
            ),
            absences=tuple(
                a for s in snapshot.leg_snapshots for a in s.absences
            ),
            strata=snapshot.strata,
        )
        if expected.to_json() != snapshot.to_json():
            raise ValueError(
                "triangle snapshot differs from verified source replay"
            )


__all__ = [
    "TRIANGLE_FEATURE_REGISTRY",
    "TRIANGLE_SYMBOLS",
    "TriangleBarPolicyV1",
    "TriangleBarStratumV1",
    "TriangleQuoteTupleV1",
    "TriangleFeatureDefinitionV1",
    "TriangleFeatureValueV1",
    "TriangleBarCellV1",
    "TriangleBarSnapshotV1",
    "TriangleBarSourceV1",
    "triangle_feature_definitions",
    "synchronized_triangle_tuples",
]

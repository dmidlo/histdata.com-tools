"""Closed native-backed synthetic intervention, not a historical causal design."""

from __future__ import annotations

from dataclasses import dataclass, replace
from fractions import Fraction
from typing import ClassVar

from ._wire import Artifact, canonical_json, load_json, ordered, text
from .contracts import (
    AttributionEvidenceKind,
    AttributionFeatureV1,
    AttributionReferenceV1,
    AttributionSnapshotV1,
    AttributionState,
    AttributionValueV1,
    DecisionAttributionV1,
    FeatureSpace,
    reference,
)
from .reference import _float

INTERVENTION_ID = "synthetic-native-triangle-regeneration.v1"
UNIVERSE = ("EURGBP", "EURUSD", "GBPUSD")
UNITS = ("GBP_per_EUR", "USD_per_EUR", "USD_per_GBP")


@dataclass(frozen=True, slots=True)
class TriangleInterventionScenarioV1(Artifact):
    """Primitive synthetic prices; EURGBP and every dependent feature regenerate.

    The session and availability are fixture declarations, not verified calendar
    or historical observation. No macro/universe/session intervention is admitted.
    Nonempty external context is unsupported: a caller's retained value cannot
    establish independence from the perturbed prices.
    """

    KIND: ClassVar[str] = "triangle-scenario"
    eurusd: float
    gbpusd: float
    event_at_ns: int
    available_at_ns: int
    cutoff_at_ns: int
    session_start_ns: int
    session_end_ns: int
    session: str
    universe: tuple[str, ...] = UNIVERSE
    units: tuple[str, ...] = UNITS
    unchanged_context: tuple[AttributionValueV1, ...] = ()
    intervention_policy: str = INTERVENTION_ID

    def _validate(self) -> None:
        if min(self.eurusd, self.gbpusd) <= 0:
            raise ValueError("positive native triangle prices required")
        if (
            not 0
            <= self.session_start_ns
            <= self.event_at_ns
            <= self.available_at_ns
            <= self.cutoff_at_ns
            < self.session_end_ns
        ):
            raise ValueError("point-in-time/session intervention constraint")
        if (
            self.universe != UNIVERSE
            or self.units != UNITS
            or self.intervention_policy != INTERVENTION_ID
        ):
            raise ValueError(
                "triangle/unit/universe/intervention policy mismatch"
            )
        text(self.session)
        ordered(tuple(v.feature.name for v in self.unchanged_context))
        if len(self.unchanged_context) > 4 or any(
            v.available_at_ns is None or v.available_at_ns > self.cutoff_at_ns
            for v in self.unchanged_context
        ):
            raise ValueError(
                "future/unavailable context cannot enter perturbation"
            )
        if self.unchanged_context:
            raise ValueError(
                "external context has no admitted dependency-regeneration proof"
            )


def _native(
    scenario: TriangleInterventionScenarioV1,
) -> tuple[tuple[str, ...], str, AttributionSnapshotV1]:
    # Invoke existing canonical native event/tuple semantics, not a local
    # approximation to the synchronization or triangle residual implementation.
    from histdatacom.synthetic.contracts import SyntheticEventV1
    from histdatacom.synthetic.activity import ActivitySliceScope
    from histdatacom.synthetic.bar_features import BarFeaturePolicyV1
    from histdatacom.synthetic.information import InformationMode
    from histdatacom.synthetic.triangle_bar_features import (
        TriangleBarPolicyV1,
        synchronized_triangle_tuples,
        _residual,
    )

    prices = (
        _float(Fraction(scenario.eurusd) / Fraction(scenario.gbpusd)),
        scenario.eurusd,
        scenario.gbpusd,
    )
    events = tuple(
        SyntheticEventV1.observed(
            symbol=symbol,
            event_time_ns=scenario.event_at_ns,
            event_sequence=0,
            bid=price,
            ask=price,
            run_id="public-attribution-synthetic-fixture",
            ensemble_member_id="fixture",
            source_version_id="synthetic-primitive-scenario",
            source_series_id=symbol,
            source_period="200001",
            source_row_id=1,
        )
        for symbol, price in zip(UNIVERSE, prices)
    )
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(InformationMode.EX_POST_RECONSTRUCTION), 0
    )
    tuples = synchronized_triangle_tuples(
        events,
        start_ns=scenario.event_at_ns,
        end_ns=scenario.event_at_ns + 1,
        scope=ActivitySliceScope.OBSERVED,
        policy=policy,
    )
    if len(tuples) != 1 or tuples[0].events != events:
        raise ValueError("native intervention has no exact three-leg support")
    values = []
    for event, unit in zip(events, UNITS):
        definition = AttributionFeatureV1(
            "market.tick." + event.symbol + ".mid",
            "market.tick",
            "native-midpoint.v1",
            unit,
        )
        values.append(
            AttributionValueV1(
                definition, event.bid, event.event_id, scenario.available_at_ns
            )
        )
    residual = _residual(prices)
    definition = AttributionFeatureV1(
        "triangle.event_residual",
        "triangle",
        "native-synchronous-residual.v1",
        "log_ratio",
    )
    values.append(
        AttributionValueV1(
            definition,
            residual,
            tuples[0].artifact_id,
            scenario.available_at_ns,
        )
    )
    values.extend(scenario.unchanged_context)
    lineage = reference(scenario)
    # Exact native policy references retain proof scope, not a market calendar
    # or learned preprocessing certificate.
    from ._wire import sha256

    policy_json = policy.to_json()
    native_policy = AttributionReferenceV1(
        policy.schema_version,
        policy.artifact_id,
        sha256(policy_json),
        len(policy_json),
        AttributionEvidenceKind.FIXTURE,
    )
    snapshot = AttributionSnapshotV1(
        tuple(sorted(values, key=lambda v: v.feature.name)),
        scenario.cutoff_at_ns,
        FeatureSpace.RAW,
        native_policy,
        native_policy,
        lineage,
        scenario.universe,
        scenario.session,
        "synthetic_fixture",
    )
    return (
        tuple(event.to_json() for event in events),
        tuples[0].to_json(),
        snapshot,
    )


def generate_triangle_fixture(
    scenario: TriangleInterventionScenarioV1,
) -> AttributionSnapshotV1:
    return _native(scenario)[2]


@dataclass(frozen=True, slots=True)
class NativeTriangleInterventionV1(Artifact):
    KIND: ClassVar[str] = "native-triangle-intervention"
    before: TriangleInterventionScenarioV1
    after: TriangleInterventionScenarioV1
    before_events_json: tuple[str, ...]
    after_events_json: tuple[str, ...]
    before_tuple_json: str
    after_tuple_json: str
    before_snapshot: AttributionSnapshotV1
    after_snapshot: AttributionSnapshotV1

    def _validate(self) -> None:
        if (
            replace(
                self.before, eurusd=self.after.eurusd, gbpusd=self.after.gbpusd
            )
            != self.after
        ):
            raise ValueError(
                "intervention changed forbidden context/time/session/universe state"
            )
        # Count every embedded native node with the complete outer evidence.
        parsed = [
            load_json(s)
            for s in self.before_events_json
            + self.after_events_json
            + (self.before_tuple_json, self.after_tuple_json)
        ]
        canonical_json({"outer": self.to_payload(), "native": parsed})
        if _native(self.before) != (
            self.before_events_json,
            self.before_tuple_json,
            self.before_snapshot,
        ) or _native(self.after) != (
            self.after_events_json,
            self.after_tuple_json,
            self.after_snapshot,
        ):
            raise ValueError(
                "counterfactual differs from full native feature regeneration"
            )


def make_triangle_intervention(
    before: TriangleInterventionScenarioV1, *, eurusd: float, gbpusd: float
) -> NativeTriangleInterventionV1:
    after = replace(before, eurusd=eurusd, gbpusd=gbpusd)
    a, b = _native(before), _native(after)
    return NativeTriangleInterventionV1(
        before, after, a[0], b[0], a[1], b[1], a[2], b[2]
    )


@dataclass(frozen=True, slots=True)
class CounterfactualAttributionV1(Artifact):
    KIND: ClassVar[str] = "counterfactual"
    intervention: NativeTriangleInterventionV1
    before: DecisionAttributionV1
    after: DecisionAttributionV1
    raw_output_change: float

    def _validate(self) -> None:
        if any(
            a.evidence_kind is not AttributionEvidenceKind.FIXTURE
            or a.model is None
            or a.state
            not in (
                AttributionState.IDENTIFIED,
                AttributionState.NONIDENTIFIABLE,
            )
            for a in (self.before, self.after)
        ):
            raise ValueError(
                "counterfactual requires actually executed supported model accounting"
            )
        if (
            self.before.snapshot != self.intervention.before_snapshot
            or self.after.snapshot != self.intervention.after_snapshot
            or self.before.model != self.after.model
            or self.before.policy != self.after.policy
            or self.before.background != self.after.background
            or self.before.raw_output is None
            or self.after.raw_output is None
        ):
            raise ValueError(
                "counterfactual explanation loses executed feature/model ownership"
            )
        if self.raw_output_change != _float(
            Fraction(self.after.raw_output) - Fraction(self.before.raw_output)
        ):
            raise ValueError(
                "counterfactual output change is not model-replayed"
            )

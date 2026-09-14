# Monetary-policy event semantics

`histdatacom.market_context.monetary_policy` freezes provider-neutral meeting,
phase, setting, expectation, vote, and surprise contracts for official
central-bank evidence. It deliberately does not define a universal scalar
“interest rate decision.” Target ranges, facility sets, yield targets,
currency-board mechanics, exchange-rate bands, and categorical framework
changes retain their source-published shape.

## Meeting and phase identity

`MonetaryPolicyMeetingV1` is the shared identity for all publications arising
from one meeting or unscheduled action. Each publication is a distinct
`MonetaryPolicyPhaseEventV1` with its own immutable identifier, timestamp,
content hash, snapshot, source URI, and limitations. The supported phases are:

- meeting schedule, reschedule, and cancellation;
- decision and emergency action;
- statement and separately published forward guidance;
- vote split;
- press conference;
- minutes or accounts; and
- projections.

`MonetaryPolicyMeetingBundleV1` verifies the relationships. Decisions must bind
a decision or emergency phase at the same timestamp, vote tallies must bind a
separate vote phase and a decision in the same bundle, post-decision phases
cannot predate the decision, and a cancelled meeting cannot contain a
decision. Phase rows therefore cannot collapse into one commercial-calendar
row merely because several were published at the same time.

## Schedule and emergency evidence

Scheduled meetings require an exact local lexical time and contemporaneous or
archived calendar evidence that was known before the meeting. Unscheduled and
emergency actions cannot carry a fictional scheduled time; they require a
contemporaneous or archived release artifact. “Retrospective list” is not an
accepted evidence kind, so a later historical meeting list cannot manufacture
advance notice for an emergency action.

Every lexical timestamp retains its UTC offset and IANA source timezone. The
constructor verifies that its integer nanosecond value, offset, and timezone
agree.

## Policy setting shapes

`MonetaryPolicySettingV1` accepts one exact shape:

| Kind | Required representation | Examples |
| --- | --- | --- |
| scalar rate | one value and unit | Bank Rate, cash rate |
| target range | lower and upper bounds | Federal funds target range |
| rate set | named, independently valued components | ECB deposit/MRO/facility rates |
| yield target | value, unit, and tenor | yield-curve-control target |
| exchange-rate band | lower and upper bounds when officially disclosed | published band mechanics |
| currency-board rate | one source-defined value | currency-board base rate |
| categorical | versioned code and display label | undisclosed-band stance or framework change |

Incompatible fields fail closed. A target range has no scalar midpoint, a rate
set cannot drop or reorder a facility silently, and a categorical setting
cannot acquire an invented numeric value. Source lexical text and the setting
definition version are part of deterministic identity.

## Actual, previous, forecast, and votes

`MonetaryPolicyDecisionV1` binds:

- `new_setting`, the actual setting or categorical decision published at the
  decision phase;
- `previous_setting`, the setting known immediately before the decision; and
- one `MonetaryPolicyExpectationV1`.

The previous setting carries its own as-known cutoff and evidence identifier.
Hold comparisons use a deterministic semantic identity that ignores harmless
changes in source wording while retaining the raw wording in full content
identity; a unit, bound, component, definition, tenor, or categorical-code
change remains a real setting change.

An expectation is calendar-style eligible only when it is an exact event
consensus or an official-survey event target available no later than the
decision. Official period forecasts and central-bank projections retain their
real scope but cannot masquerade as event consensus. Where no legitimate
pre-decision expectation exists, an explicit unavailable record and reason are
required.

Hold and guidance-only decisions cannot change the setting. Tightening and
easing decisions must change it. Guidance-only decisions additionally link a
separate statement or forward-guidance phase.

`MonetaryPolicyVoteTallyV1` records source-published vote positions rather than
assuming every institution uses a for/against convention. Bucket counts plus
not-voting members must reconcile exactly to eligible voters. No tally is
created when an institution does not publish one.

## Surprise semantics

`compute_monetary_policy_rate_surprise()` operates only on an exact, eligible,
pre-decision expectation with a compatible setting shape. It returns:

- a scalar `new - expected` delta for scalar settings;
- separate lower/upper deltas for ranges;
- named component deltas for rate sets; or
- a categorical match flag without fabricating a number.

Missing, period-target, or shape-incompatible expectations produce an explicit
unavailable result. Statement, forward-guidance, and press-conference surprise
uses `declare_monetary_policy_text_surprise()` and the distinct
`textual-latent` kind. A text score cannot occupy the rate-delta fields or bind
itself as a numeric rate decision.

## Cross-economy framework qualification

`built_in_monetary_policy_profiles()` binds every economy in the official
source registry to its legal monetary-policy producer and a framework-aware
setting surface. The audit explicitly checks the difficult cases:

- United States target ranges;
- ECB facility/rate sets for the euro area, Germany, and France;
- Bank of Japan yield-curve/operating-framework changes;
- Hong Kong currency-board mechanics; and
- Singapore exchange-rate-band and categorical settings.

Other scoped economies retain policy-rate semantics while still supporting
categorical framework changes and unscheduled actions. Supported phases express
what an adapter can represent, not a claim that every institution publishes
every artifact. `audit_monetary_policy_frameworks()` compares all profiles to
the source registry; `require_monetary_policy_framework_coverage()` fails
closed on a missing economy, duplicate profile, wrong official owner, scalarized
special case, or absent emergency semantics.

All contracts carry exact schema versions and deterministic content identities.
Round-trip restoration recomputes nested identifiers, so a changed setting,
phase, expectation, decision, vote, or relationship cannot replay under the
old identity.

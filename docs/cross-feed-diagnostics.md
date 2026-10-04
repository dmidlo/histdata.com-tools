# Conditional cross-feed clock and matching diagnostics

Issues #628 and #629 add an offline, synthetic-tested diagnostic workflow:
independently anchored native capture → conditional frozen clock model →
one-to-one matching with explicit ambiguity. They do not establish latent market
truth, source completeness, one-way feed latency, independent provider loss,
or capture-recapture retention estimates (#630).

## Native inputs and current rights

`histdatacom.cross_feed.NativeCaptureRefV1` takes the actual directory, native
manifest, exact provider invocation, independently retained provenance seal,
and expected host-health audit ID. Legacy capture and SDK lifecycle formats are
supported. `admit_capture` replays actual partition bytes, the provenance chain,
health observations and current provider rights. SDK replay also checks the
actual permission execution. Unsealed, unanchored, substituted, incomplete and
revoked inputs fail closed. An operator must retain expected roots separately;
a hash read from the same mutable input directory is not an independent anchor.

SDK host time comes from `BrokerLifecycleRecordV1`, **not** the plugin's
`BrokerEventV1.receive_time`. Quotes retain exact native IDs, host sequence,
connection/clock epoch, source semantics and declared precision, host UTC and
monotonic times, price basis, and health-audit/bucket references. SDK decimal
strings remain exact decimal values, without claiming original provider
lexemes. Legacy source lexemes remain exact; absent source lexemes use exact
binary64 values with an explicit different basis. Controls are included through
their digest/count and the complete native root, not discarded as quotes.

Per-quote health uses only verified journal observations through that record's
`PERSISTED` frontier and preceding native controls. Its descriptive
`native_prefix_diagnostics` state is not a forged qualified audit. The original
complete audit state/reasons remain capture metadata. Later faults can change
the complete capture identity but cannot retroactively alter earlier prefix
health facts used in calibration. Full-bucket references are lineage only, not
causal gating inputs.

All public producers require the repository's current provider-policy scope.
The closed `CrossFeedDerivationV1` binding includes both actual native providers,
their seals, frozen configuration, and clock parent where applicable. Derivation
requires rights for statistical fingerprints, normalized quotes, health and
hashes, plus the native input classes. Input material-use permission alone does
not authorize a derived report. These functions return in-memory diagnostics;
they do not grant retention, publication or redistribution permission and do not
write reports or acquire provider data.

## Frozen conditional clock

Construct `ClockFitRequestV1` with sorted one-to-one
`ClockCandidatePairV1(left_event_id, right_event_id)` correspondences, each
capture's host-sequence calibration cutoff, a frozen `ClockFitPolicyV1`, and a
declared mode. Correspondence assumptions must be chosen independently of the
match evaluation; matching cannot bootstrap its own purported ground truth.

```python
from histdatacom.cross_feed import fit_clock_model, match_captures

# left_ref/right_ref: independently retained native capture references
# request: predeclared calibration pairs, cutoffs and clock policy
# matching_policy: declared scales, weights, gates and ambiguity threshold
# Execute inside an actual current provider-policy scope.
model = fit_clock_model(left_ref, right_ref, request)
expected_model_id = model.artifact_id  # retain independently for later replay
report = match_captures(
    left_ref,
    right_ref,
    model,
    matching_policy,
    expected_model_id=expected_model_id,
)
```

The left source is the coordinate reference, not true UTC. Exact rational
Theil–Sen drift and median intercept characterize `left_time - right_time`.
Median/MAD diagnostics, declared timestamp quanta, residual/drift spread and
prediction horizon determine descriptive uncertainty intervals. The scaled MAD
factor is exactly `7413/5000`; it is not a guaranteed confidence-coverage claim.
Insufficient or degenerate support yields unavailable corrections, not a
fabricated zero drift. Native epoch/correction boundaries and a frozen sustained
jump rule prevent averaging blindly across clock jumps. Large offsets consistent
with timezone/DST mistakes are diagnosed, not silently timezone-corrected.

Source-to-host timestamp differences are reported by epoch and health state;
they conflate source-clock offset and delivery effects and are not pure latency.
Unknown upstream loss remains unknown. The default narrow health exception for
unidentified upstream loss does not manufacture a qualified host audit. Other
supported clock/epoch exceptions must be predeclared; malformed or dropped
native observations cannot be silently admitted through that exception list.

`frozen_ex_ante` is an **offline sealed-capture prefix simulation**: calibration
cannot include future host-sequence observations, and corrections respect
decision cutoffs and prediction bounds. It does not prove the fitted artifact
existed before the original capture seal. Real online artifact availability is
a separate acquisition/deployment concern. `replay_clock_model` recomputes from
actual native evidence and compares the entire frozen artifact, not only its
self-consistent hash. Matching performs this same verification, then uses that
exact model; it does not fit a matcher-specific correction.

## Assignment, ambiguity and evaluation

`MatchingPolicyV1` freezes time tolerance, dimensioned normalization scales,
five nonnegative weights (time, bid, ask, spread, transition), explicit quote
compatibility gates, uncertainty ceiling and ambiguity margin. Consecutive
per-symbol mid-price transitions use native host order and never cross epochs
or price bases. A positively weighted missing transition is unavailable, not
zero. Optional price gates set to `None` mean explicitly unbounded price
compatibility, not evidence that a nearby quote has the same identity.

The matcher builds the complete bounded candidate graph, then solves maximum
cardinality followed by minimum exact total cost using augmenting paths.
Canonical identity ordering makes ties deterministic. For each selected edge it
forbids that edge and solves again: an alternative is admissible for ambiguity
only at the **same global cardinality**. The retained best/alternative totals and
margin therefore describe feasible assignments, not a misleading local second
nearest neighbor. Equal or small margins remain ambiguous even though one
deterministic assignment is emitted. Excessive clock uncertainty is retained in
the match status. No event is used twice.

Reports preserve matched, ambiguous, uncertain and source-only states, corrected
times and radii, candidate/rejected-pair denominators and residuals. Source-only
reasons distinguish unavailable clocks, no compatible candidate and one-to-one
competition; none means proven loss by the other source. This is the typed
uncertainty-bearing input boundary for #630, not an implementation of its
retention estimator.

Pure `evaluate_matching` and `evaluate_matching_sensitivity` are synthetic-test
utilities: evaluation replays a supplied match report; sensitivity recomputes
each grid report against admitted projections and an independently supplied
`MatchingTruthV1`. They report exact precision/recall,
ambiguity/source-only rates and bounded residual distributions. Sensitivity
retains every point in a predeclared tolerance grid using one frozen clock, with
no winner selection or hidden tuning. These pure functions do not replace the
native public producer's evidence or rights checks.

## Resource limits and validation scope

Native admission refuses more than 8,192 records, 16 MiB of native partition
bytes, 32,768 provenance entries, or 2,048 projected quotes per capture. Clock
calibration accepts at most 128 declared pairs. Matching uses at most 64 quotes
per side and 4,096 candidate pairs, with tighter prospective expanded-wire and
8,000,000-work-unit limits that can refuse smaller dense inputs. It refuses a
too-large block instead of truncating edges, changing the optimizer, or claiming
streaming-scale coverage. Sensitivity is limited to eight predeclared points and
one aggregate work allowance. Callers must select scientifically declared bounded
captures; this version does not silently partition across possible match edges.

Tests use invented feeds only. They include real local Legacy/SDK writers,
provenance and health replay, rights revocation and tampering; exact robust clock
arithmetic; shifted/drifting/quantized/jumping clocks; duplicates, batching,
source-only and reordered bursts; exhaustive independent assignment oracles;
and deterministic ordering. Neither synthetic success nor the closed native
integrity path qualifies real-feed identification or an empirical study.

# Bounded regime-switching marked Hawkes challenger

`histdatacom.synthetic.regime_hawkes` supplies two registered proposal engines.
Their challenger role tests whether a shared latent activity state adds value
beyond the empirical motif reference, all four classical event-clock families,
and all three static marked-Hawkes ablations. Retained evidence keeps both
benchmark-eligible but not reconstruction-eligible; neither can select a
winner.

The fixed regime registry contains exactly two nested ablations:

1. `baseline_only`: baseline rates and quote-transition marks vary by state,
   while the excitation matrix is shared; and
2. `baseline_and_excitation`: baseline rates, quote-transition marks, and the
   excitation matrix vary by state.

`default_regime_hawkes_configs()` returns both in that order. A benchmark may
receive neither or exactly one configuration for each ablation. This prevents
partial or post-hoc comparisons.

## Declared MMHP-delta approximation

The implementation is a bounded, synchronized, two-state Markov-modulated
Hawkes approximation on a fixed time grid. For destination symbol (i), bin
(b), and shared latent state (z_b),

\[
\lambda_i(b\mid z_b) = \mu_i[z_b]
  + \beta \sum_j \alpha_{ij}[z_b] R_j(b).
\]

The state and intensity are constant inside one declared delta bin. The
recursion (R_j(b)) contains strict-prior history only. Observed anchors and
generated events in bin (b) enter the recursion for bin (b+1), so event
ordering inside a bin cannot create hidden same-bin excitation.

This is not exact inference for a continuous-time latent-state MMHP. The
fixed bin width, decay, state count and ordering, transition smoothing,
initialization, convergence policy, fit boundaries, mark policy, and resource
limits all participate in the content-derived configuration identity.

The two anonymous fitted states are canonicalized by increasing expected
aggregate activity and exposed only as `calm` and `active`. They are model
labels, not claims about economic regimes or historical truth. A label
permutation, collapsed activity contrast, low posterior occupancy, or
unsupported transition estimate makes the fit unusable.

## Technology, session, context, and latent state

`RegimeHawkesWindowContextV1` keeps four axes separate:

- the latent market-activity state;
- the v2 technological feed-epoch assignment;
- the market session; and
- optional observed market context.

A stable technological epoch binds the feed-epoch definition and epoch IDs.
A transition window instead binds its boundary ID, support, and uncertainty
start/end periods. It remains a transition stratum; it is never renamed as a
latent volatility state.

Optional observed context may provide a filtered initial-state prior only
when its content identity, availability time, and use time are present and
the availability time does not follow the use time. That prior is an input,
not a smoothed state recovered from future observations.

The real reverse-degradation campaign reloads the exact v2 feed-epoch
artifact already bound by the corpus. It reassigns every calibration,
validation, and holdout window at its midpoint and refuses an identity or
label mismatch. Thus regime results cannot silently discard technological
transition uncertainty.

## Calibration and information boundaries

`fit_regime_hawkes_challenger()` accepts calibration-only
`EventClockCalibrationWindowV1` inputs. The fit binds two exact digests:

- event-content identities for every calibration window; and
- the complete window-context identities.

All excitation and state recursions reset at each calibration-window
boundary. Exact technology/session models are fitted alongside a declared
session backoff. There is no hidden global model.

Ex-ante fitting requires `as_of_ns` and refuses events or observed context
that were unavailable by that boundary. Ex-post fitting forbids an as-of
value. Failed and refused fits expose no parameters, uncertainty, or state
paths.

## Scaled inference, diagnostics, and uncertainty

The fixed-bin emission is a state-conditioned multivariate Poisson likelihood
with state-conditioned quote-transition marks. Bounded generalized
expectation maximization uses scaled forward-backward inference independently
for each calibration window. Scaling offsets are retained in the marginal
likelihood, preventing probability underflow from being mistaken for model
evidence. Accepted marginal likelihood is monotone: a decreasing M-step
proposal receives at most eight deterministic half-step interpolations, and
the fitter rolls back to the last accepted checkpoint if none is
non-decreasing. Rejected likelihoods and posteriors never enter the retained
trace or a later M-step.

The fit keeps filtered and smoothed state probabilities separate:

- filtered values are (P(z_b\mid y_{1:b})) and use information only through
  the current bin;
- smoothed values are (P(z_b\mid y_{1:B})), are labeled ex-post diagnostics,
  and are never accepted by generation or a strategy-valid surface.

Each conditioning model reports posterior occupancy, expected transition
counts, hard-path mean dwell bins, posterior entropy, aggregate activity
levels and contrast, marginal likelihood, iterations, excitation spectral
radii, and stability margins. The descriptive 95% intervals use posterior
effective state-bin counts; they are not an exact-coverage claim.

Every state-specific nonnegative excitation matrix is recomputed and checked
at fit construction, adapter binding, and generation. A model is usable only
when

\[
\rho(\alpha_z) < \texttt{maximum_branching_ratio} < 1
\]

for both states. The baseline-only ablation must have byte-for-byte identical
state excitation matrices. Serialized or in-memory structural tampering fails
closed.

## Synchronized generation and bounded history

`FittedRegimeHawkesBenchmarkGeneratorV1` evolves one state path for the full
EUR/GBP/USD synchronization unit. Its semantic seed binds the configuration,
fit, scenario, reconstruction window, ensemble member, and exact context.
The initial distribution is either the fitted window-reset distribution or a
point-in-time-valid filtered context prior.

Generation is active only where a destination symbol has a strict pair of
observed anchors. Candidate timestamps are unique and lie in the open anchor
interval. Quote projection uses the sampled `ask_only`, `bid_only`, `joint`,
or `unchanged` mark and the enclosing quotes. Successful output returns all
input anchors unchanged.

Explicit history must be from the synchronized symbols, strictly before the
window, and below the cardinality limit. Rows older than the declared
lookback are deterministically ignored and cannot affect output; present,
future, or foreign-symbol history refuses the whole attempt.

Bin count, Poisson work, per-bin, per-interval, per-window, amplification,
history, parameter bytes, estimated and measured memory, and wall time have
independent limits. Any violation discards every proposal. A refused or failed
attempt has no lineage digest and cannot be carved.

Successful and empty evidence binds:

- exact full input-event, anchor, retained-history, and window-context
  digests;
- fit/config/conditioning identities and backoff level;
- processed bins, Poisson work, state-bin counts and transitions;
- the initial-state policy and final filtered probabilities;
- the maximum state spectral radius; and
- a digest of all event lineage.

Each generated lineage records the bin, canonical state label, whether the
state changed, filtered state probability, conditional intensity, strongest
excitation source and its numeric contribution when present, and
quote-transition mark.

## Historical carving

`build_regime_hawkes_candidate_batches()` revalidates run, window, member,
config, fit, generation, context, and anchor identities. It groups proposals
by immutable anchor interval and creates candidate-only `SyntheticEventV1`
rows. `RegimeHawkesCandidateBatchV1` satisfies the shared
`ReconstructionCandidateBatchV1` protocol.

The historical carving engine remains authoritative for anchor integrity,
resource limits, fingerprints, context support, quarantine, session closure,
conditioned intensity, spread projection, and final local validation. A
latent-state fit never bypasses those rules.

## Reverse-degradation comparison

Passing
`regime_hawkes_configs=default_regime_hawkes_configs()` to
`run_reverse_degradation_benchmark_campaign()` installs both regime
challengers. Supplying the #450 and #451 registries at the same time yields ten
challenger variants over the identical corpus, splits, scenarios, ensemble
members, and anchors:

- the empirical motif baseline;
- four classical event clocks;
- three static marked-Hawkes ablations; and
- two regime-Hawkes ablations.

The four established baseline/control reports remain present separately. Each
regime report
adds configuration, fit, calibration/context digest, convergence, likelihood,
bin/event/window, resource, state occupancy, activity contrast, transition,
technological-transition, and stability evidence to the existing stream
metrics. Reports and campaigns retain `automatic_winner: false`.

Promotion requires consistent evidence across all three symbols and multiple
technological epochs or transition strata. Likelihood, one symbol, one
window, or one high-activity episode cannot promote a challenger.

Issue #453 adds one separate dependency-free RMTPP challenger after this
registry. Supplying its fixed config as well produces 11 challengers and 15
total reports without changing either regime ablation. See [Bounded recurrent
marked temporal point-process challenger](neural-tpp-challenger.md).

## Scientific nonclaims

- Latent states do not reveal historical economic truth.
- Technological feed transitions are not latent market regimes.
- The discretized model is not an exact continuous-time MMHP.
- Excitation is comparative statistical evidence, not market causality.
- Approximate uncertainty does not establish exact frequentist coverage.
- The challenger does not implement broker adaptation, neural intensity, or
  diffusion generation.
- This issue does not change the certified production default or make a
  standalone package release.

## Methodological references

- Wu et al., [*Markov-Modulated Hawkes Processes for Sporadic and Bursty
  Event Occurrences*](https://arxiv.org/abs/1903.03223).
- Fabre and Muni Toke, [*High-Frequency Market Manipulation Detection with a
  Markov-modulated Hawkes process*](https://arxiv.org/abs/2502.04027).

## Retained issue-452 closure evidence

The closure campaign reused the qualified #451 corpus and motif index without
rebuilding either scientific input:

- corpus:
  `reverse-degradation-corpus:sha256:a760a010d44de2d6258b7c3d71651b00bc24eaef53092f37bd75b3ae2395c5dc`;
- motif index:
  `reference-motif-index:sha256:b5d5e7d9580fac375c42677fe5d03be96fafc190f799364a52566af7aa5a2589`;
- campaign:
  `reverse-degradation-campaign:sha256:1af61ca95112323698b8f300c33dc684b4a2f6ed257a8e086fdaf59e04c33631`;
- scorecard SHA-256:
  `b483927b4f6bea8493c33f19752bddd3025144b31189c8bb02703b79976589d8`;
  and
- resource-audit SHA-256:
  `8bc8e5ffebbab9f6e8ee80c6ad73c83661605b38960e7f6f70c2a32b19afbb23`.

The row-free scorecard contains 14 reports: four baseline/control reports plus
the complete ten-challenger comparison (empirical motif, four clocks, three
static Hawkes, and two regime Hawkes). Source replay, all required degradation
families, the campaign integrity gate, and artifact replay passed. The
five-artifact set was 943,842 bytes; runtime was 37.557071 seconds and process
peak RSS was 162,398,208 bytes. The artifact declares
`automatic_winner: false`.

Both fits used the same 3,522 events, six calibration windows, 7,200 fixed
bins, all three symbols, and technology epoch 03. Validation/final-holdout
execution covered 12 windows, two ensemble members, all three sessions,
technology epoch 04, and all three symbols. The selected corpus contains no
technological transition window, which is reported rather than relabeled or
silently treated as a latent state.

| Fit evidence | baseline only | baseline + excitation |
|---|---:|---:|
| marginal log likelihood | -12,294.3028 | -12,196.6128 |
| maximum spectral radius | 0.0756 | 0.1500 |
| minimum state occupancy | 0.2761 | 0.2920 |
| minimum calm-state occupancy | 0.4312 | 0.4228 |
| minimum active-state occupancy | 0.2761 | 0.2920 |
| minimum activity contrast | 0.8471 | 0.8289 |
| minimum expected transitions | 192.9876 | 183.9714 |
| minimum / maximum mean dwell bins | 1.0000 / 18.7534 | 1.0000 / 20.8462 |
| mean posterior entropy | 0.2987 | 0.2919 |
| fit iterations | 49 | 48 |

Each ablation completed 24 window/member executions with zero failures, zero
refusals, and zero immutable-anchor violations. Neither passed the frozen
candidate promotion gates.

The comparative result is mixed rather than promotional. Allowing excitation
to switch improved mean event-count relative error from 0.5596 to 0.4257,
update-transition error from 0.7090 to 0.7084, path-variation error from
0.2829 to 0.2147, and fit likelihood. Baseline-only produced the better
interarrival histogram error (0.1306 versus 0.1643). Against static full
Hawkes, the baseline-and-excitation model improved interarrival shape (0.1643
versus 0.1930) and path variation (0.2147 versus 0.2255), but worsened event
count (0.4257 versus 0.3706) and update transitions (0.7084 versus 0.5905).
The empirical motif baseline remained materially better on event count,
update transitions, and path variation. Issue #452 therefore installs and
measures both regime ablations without promoting either one.

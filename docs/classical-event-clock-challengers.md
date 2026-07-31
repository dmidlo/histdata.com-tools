# Classical event-clock reconstruction challengers

`histdatacom.synthetic.event_clock` provides four transparent proposal engines.
“Challenger” in this historical filename describes their role in a benchmark,
not an optional architecture. All four are registered by the v2 proposal bank,
retain their failed promotion evidence, and are benchmark-eligible but not
reconstruction-eligible. They cannot select a product default.

The implementation follows one shared lifecycle:

1. accept only explicit calibration-split `EventClockCalibrationWindowV1`
   inputs;
2. fit a bounded family-specific configuration;
3. retain a deterministic `EventClockFitResultV1` with convergence, likelihood,
   resource, diagnostic, information-mode, and failure evidence;
4. bind a successful fit to `FittedEventClockBenchmarkGeneratorV1`;
5. generate a synchronized multi-symbol window between immutable degraded
   anchors; and
6. optionally project proposals to `EventClockCandidateBatchV1` for the same
   generator-neutral historical-carving engine used by empirical motifs.

Failed or refused fits contain no parameter payload. Refused generation emits
no partial candidate stream.

## Family contracts and likelihoods

Every family has a distinct schema and content-derived configuration ID. The
shared resource policy is itself versioned and content addressed.

### Piecewise non-homogeneous Poisson process

`NonHomogeneousPoissonConfigV1` divides the UTC day into a fixed number of
bins. For event times \(t_i\) and piecewise intensity \(\lambda(t)\), the fitted
objective is

\[
\ell = \sum_i \log \lambda(t_i) - \int \lambda(u)\,du.
\]

Exposure is accumulated across each calibration window rather than inferred
from row count. A declared smoothing count keeps empty calibration bins finite.
Generation draws a Poisson count in each immutable-anchor interval and places
the additional times uniformly inside the open interval.

### Gamma-mixed Cox process

`CoxProcessConfigV1` fits per-window count over-dispersion under

\[
N_w \mid \Lambda_w \sim \operatorname{Poisson}(\Lambda_w T_w),
\qquad
\Lambda_w \sim \operatorname{Gamma}(k, \theta).
\]

The implementation evaluates the resulting negative-binomial marginal count
likelihood. Shape bounds and a dispersion floor are explicit. Generation draws
one gamma rate per anchor interval, then a conditional Poisson count.

### Exponential ACD(1,1)

`AutoregressiveConditionalDurationConfigV1` uses the Engle-Russell duration
recursion

\[
x_i = \psi_i \epsilon_i,
\qquad
\psi_i = \omega + \alpha x_{i-1} + \beta \psi_{i-1},
\qquad
\epsilon_i \sim \operatorname{Exp}(1).
\]

The bounded coefficient grid enforces nonnegative coefficients and
\(\alpha+\beta < 1-\delta\), where the stationarity margin \(\delta\) is part of
the configuration. The likelihood is the exponential conditional-duration
likelihood. Each calibration window resets the duration recursion, so blocked
windows cannot create a fabricated cross-window duration. Grid cardinality
cannot exceed the iteration budget, and the fit reports the actual evaluated
coefficient count.

Each generated synchronized window starts ACD state at the fitted unconditional
mean; state then carries only across consecutive anchor intervals inside that
window. It does not pretend that the end of a distant calibration window is
adjacent to the requested reconstruction window.

### Two-state hidden Markov duration/mark model

`HiddenMarkovDurationMarkConfigV1` fits two latent states with lognormal
duration emissions, a categorical quote-transition mark distribution, and a
smoothed two-by-two transition matrix. Bounded hard-EM stops at the configured
tolerance or fails closed at the iteration limit. Variance floors and
probability smoothing are explicit. This v1 contract is a hidden Markov model,
not an unimplemented semi-Markov claim.

Initial-state and transition counts reset at every calibration-window boundary;
no transition is invented between distant windows. Generated marks are sampled
from the fitted probability vector for the simulated hidden state, not from the
generic quote-profile fallback.

Each generated window samples its initial hidden state from the fitted initial
distribution, then carries state only within the synchronized window.

Quote-transition marks are `ask_only`, `bid_only`, `joint`, and `unchanged`.
All four families retain a bounded empirical mark profile so generated event
times and quote-transition labels are evaluated together.

## Information and conditioning boundary

Fits are calibration-only. `EX_ANTE_SIMULATION` requires `as_of_ns` and refuses
any calibration window ending after that boundary. `EX_POST_RECONSTRUCTION`
rejects an as-of value. Calibration identities hash row-free window metadata
plus each window's content digest, so changing rows, split, or availability
changes the fit ID.

The fit records bounded support for symbol, feed epoch, and session cells.
Generation uses the following declared support hierarchy:

1. exact symbol + feed epoch + session;
2. symbol + session backoff when the requested epoch has no calibration cell;
3. refusal when session support is absent.

There is no silent global fallback. The selected level is recorded in
`EventClockGenerationEvidenceV1.conditioning_support_level`. Session backoff is
useful for later feed epochs while remaining explicit about the evidence it
uses. Event and special-calendar tags travel with carveable candidate batches;
the shared carving engine applies its ordinary context-support, closure,
quarantine, eligibility, intensity, and spread rules.

## Determinism, history, and synchronized generation

Fit identities depend on the complete semantic configuration and calibration
content. Generation seeds depend on the base seed, fit, scenario, synchronized
window, ensemble member, and the content hash of declared left history. Retry,
worker, and storage ordering are excluded.

`generate_with_evidence(..., history_events=...)` is the shared seam for later
history-dependent challengers. History must:

- contain only benchmark events for the synchronized symbol set;
- be strictly earlier than `core_start_ns`;
- fit within `max_history_ns` and `max_history_events`; and
- fit the declared memory envelope.

The classical v1 families bind this history into deterministic identity but do
not claim Hawkes-style excitation. The marked Hawkes challenger can reuse the
same prior-only seam without broadening future access.

Generation happens across the full synchronized symbol group. Every symbol
must have at least two immutable anchors. Proposals lie strictly inside
consecutive anchor intervals; the input anchors are returned unchanged. A
single interval, window, memory, symbol-support, or conditioning violation
refuses the synchronized attempt without a partial result.

## Resource and diagnostic bounds

`EventClockResourceLimitsV1` independently bounds:

- fit windows and events;
- fit iterations;
- generated events per anchor interval and synchronized window;
- prior-history age and cardinality;
- estimated fit, history, and generated-event memory; and
- retained diagnostic count.

Fit parameters also have a hard serialized-byte limit. Generation evidence
records attempted status, generated/input/history counts, conditioning level,
the exact input-anchor digest, wall time, operation-attributable peak-RSS
growth above the process entry baseline, and a bounded failure reason. The
deterministic preflight estimate remains the admission bound; this avoids
charging a long-lived worker for an unrelated earlier process high-water
mark. Diagnostics name the family
surface (`conditional_intensity`, `random_conditional_intensity`,
`conditional_duration`, or `hidden_duration_mark`) instead of fabricating a
common diagnostic that a model does not expose.

## Shared carving and output contracts

`ReconstructionCandidateBatchV1` is a runtime-checkable structural protocol.
The existing `carve_empirical_motif_candidates()` remains a strict empirical
wrapper, while `carve_reconstruction_candidates()` consumes the structural
surface. `build_event_clock_candidate_batches()` converts fitted benchmark
proposals into candidate-only `SyntheticEventV1` rows with:

- the same run, window, member, symbol, and immutable-anchor scope;
- generator/config/fit and source-event lineage;
- candidate-only constraint IDs;
- point-in-time mode and context tags; and
- compact transformation pointers for final carving lineage.

The carving engine then applies the unchanged hard precedence. Better
likelihood or benchmark loss cannot bypass anchors, resource limits,
fingerprint policy, context support, quarantine, session closure, or final
local validation.

## Reverse-degradation evidence

Passing `default_event_clock_configs()` to
`run_reverse_degradation_benchmark_campaign()` adds all four challengers while
retaining dense identity, degraded/no-fill identity, linear interpolation, the
qualified empirical motif candidate, and the anchor-drop negative control.
Each event-clock report includes its config/fit IDs, fit status, convergence,
iterations, calibration digest, likelihood, resource estimate, and family
diagnostic status. `automatic_winner` remains false.

The issue #450 retained campaign used the existing 18-window real EURGBP,
EURUSD, and GBPUSD corpus and two ensemble members. Its compact identities are:

- campaign:
  `reverse-degradation-campaign:sha256:08c1bdbb42bdbe1ac25f19d6a66f3802f08f8c53ec79ec5e2f625ad66c5c0c82`;
- scorecard SHA-256:
  `ff0f5250f090a474dab86e6255c239f568cb642e2eab785a6edd178189d7a29d`;
- resource-audit SHA-256:
  `1e2f23593f696a36771b5e90885747bfa4c5d35c50168492af1bd97ffd79a844`.

The campaign gate passed in 20.531284 seconds at 159,531,008 bytes peak RSS.
All four fits converged over 3,522 events and six calibration windows. Their 48
measured window/member runs had zero generation failures and zero immutable
anchor violations. No challenger was promotion eligible: each falsifiably
failed the predeclared event-count, path-variation, and update-transition hard
gates. Cox recorded one empty/refused candidate window (`1/24`); the other
families recorded none. This is comparison evidence, not a tuning target or a
winner declaration.

## Primary references

- Cox, *Some Statistical Methods Connected with Series of Events* (1955),
  [DOI 10.1111/j.2517-6161.1955.tb00188.x](https://doi.org/10.1111/j.2517-6161.1955.tb00188.x).
- Engle and Russell, *Autoregressive Conditional Duration* (1998),
  [DOI 10.2307/2999632](https://doi.org/10.2307/2999632).
- Johnson and Willsky, *Bayesian Nonparametric Hidden Semi-Markov Models*
  (2013), [JMLR 14](https://www.jmlr.org/papers/v14/johnson13a.html).
- Hawkes, *Spectra of Some Self-Exciting and Mutually Exciting Point
  Processes* (1971),
  [DOI 10.1093/biomet/58.1.83](https://doi.org/10.1093/biomet/58.1.83).
- Ogata, *On Lewis' Simulation Method for Point Processes* (1981),
  [DOI 10.1109/TIT.1981.1056305](https://doi.org/10.1109/TIT.1981.1056305).

The Hawkes references apply to the separate implemented marked-excitation
challenger. See [Bounded marked Hawkes reconstruction
challenger](marked-hawkes-challenger.md); the four classical families in this
module remain unchanged.

## Nonclaims

- These models do not identify the historical ticks that were actually
  missing.
- A likelihood, scorecard, or gate result does not select a production model.
- Generated quotes are research candidates until ordinary carving, validation,
  broker conditioning, and atomic persistence complete.
- The empirical generator and public reconstruction-plan v1 contract remain
  unchanged.
- Broker-specific adaptation and claims about broker-native truth are outside
  this issue.

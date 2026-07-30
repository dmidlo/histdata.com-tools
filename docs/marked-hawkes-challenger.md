# Bounded marked Hawkes reconstruction challenger

`histdatacom.synthetic.marked_hawkes` provides three opt-in, nested
point-process ablations for reverse-degradation research. The module is
separate from the fixed four-family classical event-clock registry. It does
not alter `ReconstructionPlanConfigurationV1`, replace the certified empirical
motif generator, or select a production default.

The ablations are:

1. `zero_excitation`, a fitted Poisson/null model;
2. `diagonal_self_excitation`, with only self-excitation; and
3. `full_self_cross_excitation`, with self- and synchronized cross-symbol
   excitation.

`default_marked_hawkes_configs()` returns exactly this registry order. The
benchmark accepts either no Hawkes configurations or one configuration for
each ablation, preventing an incomplete or post-hoc comparison.

## Conditional intensity and stability

For destination symbol (i), source symbol (j), and prior source-event times
(t_k^j), the v1 model is

\[
\lambda_i(t) = \mu_i + \sum_j \sum_{t_k^j < t}
  \alpha_{ij}\,\beta\,\exp[-\beta(t-t_k^j)].
\]

`alpha[i][j]` is integrated kernel mass, not peak intensity. The exponential
decay grid, structure mask, parameter floor, EM tolerance, maximum iterations,
mark smoothing, support threshold, and resource limits all participate in the
content-derived configuration ID.

For every conditioning model, the implementation recomputes the Perron
spectral radius of the nonnegative excitation matrix. A fit is usable only
when

\[
\rho(\alpha) < \texttt{maximum_branching_ratio} < 1.
\]

That check runs after fitting, in the immutable fit constructor, while binding
the benchmark adapter, and immediately before generation. Declared radii and
stability margins must match recomputation. Zero and diagonal structure masks
are also revalidated, so a serialized or in-memory tamper cannot turn a null
or self-only ablation into a cross-exciting model. Failed and refused fits
contain neither parameters nor uncertainty payloads.

## Calibration fit and likelihood

`fit_marked_hawkes_challenger()` accepts only
`EventClockCalibrationWindowV1` inputs. The calibration identity hashes the
row-free window metadata and each window's exact event-content digest.
`EX_ANTE_SIMULATION` requires an `as_of_ns` boundary and refuses unavailable
windows; ex-post reconstruction forbids an as-of value.

The fixed-decay bounded EM uses immigrant and source-dimension responsibilities
without retaining an event-pair matrix. For each calibration window it resets
all kernel recursions. Events sharing a timestamp are evaluated against the
same strict-prior state and are added to history only after every intensity at
that timestamp is evaluated.

For each decay candidate, selection uses the exact exponential-kernel log
likelihood

\[
\ell = \sum_n \log \lambda_{d_n}(t_n)
 - \sum_i \mu_i T
 - \sum_j \sum_{t_k^j}
   \sum_i \alpha_{ij}(1-e^{-\beta(T-t_k^j)}),
\]

summed separately over bounded calibration windows. The selected decay must
converge within the declared iteration envelope. Excitation updates are
projected inside the configured stability boundary; a nonconvergent or
unstable result fails closed.

The v1 uncertainty payload uses responsibility-count Wald intervals for
baseline rates and integrated excitation masses. It is explicitly labeled a
descriptive curvature approximation rather than an exact-coverage claim.

## Marks and conditioning

Quote transitions are derived from bid/ask changes within each symbol and
calibration window. The fixed state registry is `ask_only`, `bid_only`,
`joint`, and `unchanged`. The fit retains separately smoothed immigrant mark
probabilities by destination and excitation mark probabilities by
source/destination pair. Generation first samples the immigrant or source
component from its conditional-intensity contribution and then samples the
corresponding mark vector.

Every calibration window must contain one feed-epoch/session cell and the full
synchronized symbol set. Supported models follow one declared hierarchy:

1. exact feed epoch + session;
2. session backoff for an unseen feed epoch; and
3. refusal when the session is unsupported.

There is no hidden global fit. Generation evidence records the exact selected
model key and support level.

## Synchronized Ogata generation

`FittedMarkedHawkesBenchmarkGeneratorV1` runs one deterministic bounded Ogata
timeline for the complete symbol group. Its semantic seed binds the
configuration seed, fit, scenario, reconstruction window, ensemble member,
input-anchor content, and retained history content.

Observed degraded anchors and accepted generated events both update the
source-dimension recursion strictly after their timestamps. Candidate
intensity is active only while the destination symbol has an enclosing pair of
observed anchors. Every accepted time is therefore strictly inside an open
anchor interval. Cross-symbol effects share the same timeline rather than
being generated independently and merged afterward.

The fitted full-event intensity is scaled by the scenario's declared missing
fraction when a retention probability is available. Quote values are projected
from the enclosing bid/ask anchors under the sampled transition mark, then
checked for positivity, spread order, and midpoint support. Input anchors are
returned unchanged and their exact digest is included in successful or empty
evidence.

`generate_with_evidence(..., history_events=...)` accepts only prior events
from the synchronized symbols. Raw history cardinality is bounded. Events
inside `max_history_ns` seed the exponential recursion; older declared history
contributes exactly zero and is omitted from the retained-history count.
Present/future or foreign-symbol history refuses the whole attempt.

Proposal count, interval count, window count, output amplification, history,
estimated memory, measured operation-attributable peak-RSS growth, parameter
bytes, and wall time all have independent limits. A violation refuses the
entire synchronized attempt; no truncated or partial rows escape.

## Lineage and historical carving

Each process-local generation lineage records the generated source ID,
destination, selected excitation source or immigrant status, quote-transition
mark, and accepted conditional intensity. The evidence binds a digest of the
complete lineage set.

`build_marked_hawkes_candidate_batches()` validates run/window/config/fit
identity, generation evidence, source identity uniqueness, and the exact
observed-anchor digest. It converts proposals to candidate-only
`SyntheticEventV1` rows grouped by immutable anchor interval. The resulting
`MarkedHawkesCandidateBatchV1` satisfies the structural
`ReconstructionCandidateBatchV1` protocol and can be passed unchanged to
`carve_reconstruction_candidates()`.

The shared carving precedence remains authoritative. Statistical fit never
bypasses immutable anchors, fingerprint policy, context support, quarantine,
session closure, resource bounds, spread projection, or final local
validation.

## Reverse-degradation comparison

Passing `marked_hawkes_configs=default_marked_hawkes_configs()` to
`run_reverse_degradation_benchmark_campaign()` adds all three ablations beside:

- dense and degraded identity baselines;
- linear interpolation and anchor-drop controls;
- the qualified empirical motif baseline; and
- all four optional #450 event-clock challengers when their registry is also
  supplied.

Each Hawkes report carries its configuration and fit IDs, calibration digest,
fit status, convergence and iteration counts, likelihood, fitted event/window
counts, maximum spectral radius, stability margin, conditioning support, and
resource estimate. The existing stream comparison supplies event-count,
multiscale dispersion/clustering, interarrival, burst/quiet duration,
quote-transition, spread-jump/tail, path, anchor, cross-series, and calibration
metrics. Report and campaign contracts continue to require
`automatic_winner: false`.

## Nonclaims

- A Hawkes fit does not identify the historical events that were actually
  missing.
- Excitation is a comparative statistical mechanism, not proof of market
  causality or broker behavior.
- Approximate uncertainty, likelihood, or a benchmark gate does not promote a
  candidate automatically.
- Regime switching, neural intensity models, and broker-specific adaptation
  are separate work.
- Candidate rows remain research proposals until shared carving, validation,
  reconciliation, and atomic persistence complete.

## Retained #451 closure evidence

The retained closure campaign reused the exact qualified #450 corpus and motif
index instead of rebuilding either input:

- corpus:
  `reverse-degradation-corpus:sha256:a760a010d44de2d6258b7c3d71651b00bc24eaef53092f37bd75b3ae2395c5dc`;
- motif index:
  `reference-motif-index:sha256:b5d5e7d9580fac375c42677fe5d03be96fafc190f799364a52566af7aa5a2589`;
- campaign:
  `reverse-degradation-campaign:sha256:4f04587840029c551917ce87c1086b9de92242c6d569660aa59281e27c21299c`;
- scorecard SHA-256:
  `4133accde5b7dda8df66667a7342bf0fdd9aac10707f4e13e7368cfdd1aa7ebf`;
- resource-audit SHA-256:
  `f1af49b1114e29829332035305c7bd6ed83ae6c1f8dad4ac87108b842af29bc7`.

Source replay and the campaign gate passed. The run evaluated the qualified
empirical baseline, all four #450 event-clock families, and all three Hawkes
ablations over 12 validation/final-holdout windows and two ensemble members.
All three Hawkes fits used 3,522 calibration events and six windows. Their 72
combined window/member executions had zero failures, zero refusals, and zero
immutable-anchor violations. The compact five-artifact set was 924,193 bytes;
campaign runtime was 24.604101 seconds and process peak RSS was 161,857,536
bytes. The scorecard retained no event rows and declared
`automatic_winner: false`.

The ablation gives a mixed, falsifiable result:

| Evidence | zero | diagonal/self | full/cross |
|---|---:|---:|---:|
| fit log likelihood | -7373.1020 | -7287.6155 | -7282.5388 |
| maximum spectral radius | 0.0000 | 0.4938 | 0.4935 |
| mean event-count relative error | 0.4129 | 0.3898 | 0.3781 |
| mean interarrival-histogram L1 | 0.2373 | 0.2033 | 0.1961 |
| mean burst/quiet-rate error | 0.1001 | 0.0756 | 0.0925 |
| mean count-dispersion error | 0.6204 | 0.5668 | 0.5560 |
| mean update-transition L1 | 0.5085 | 0.5720 | 0.5791 |
| mean spread-jump error | 0.1150 | 0.1470 | 0.1410 |

Self-excitation materially improves calibration likelihood and several timing
metrics over the null. Full cross-excitation adds a smaller likelihood gain
and has the best event-count, interarrival, and dispersion values; the diagonal
model has the best burst/quiet value. Neither excitation structure improves
quote-transition marks or spread behavior over the null.
All three ablations fail the frozen event-count, path-variation,
update-transition, and triangle gates and are therefore not promotion
eligible. The qualified empirical motif candidate remains eligible in this
campaign. This is precisely the report-only outcome: excitation has measurable
comparative value, but the evidence does not justify default promotion.

## Primary references

- Hawkes, *Spectra of Some Self-Exciting and Mutually Exciting Point
  Processes* (1971), DOI 10.1093/biomet/58.1.83.
- Ogata, *On Lewis' Simulation Method for Point Processes* (1981), DOI
  10.1109/TIT.1981.1056305.

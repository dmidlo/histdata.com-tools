# Reconstruction math verification

Issue #507 freezes one installed, deterministic verification surface for the
release-critical equations used by reconstruction planning, generation,
qualification, and synchronization. The harness is independent of campaign
data: it persists no events, samples, predictive members, or numerical work
arrays.

## Versioned formula registry

`histdatacom.reconstruction_math` publishes formula version `1.0.0` and the
following named contracts:

| Formula | Definition | Production consumer |
| --- | --- | --- |
| `negative-binomial-failures-v1` | Failures before `r` retained events: `P(M=k)=Gamma(k+r)/(Gamma(r)k!) p^r(1-p)^k`, `E[M]=r(1-p)/p`, and `Var(M)=r(1-p)/p^2`. | Operator-conditioned marked-Hawkes cardinality. |
| `adaptive-cardinality-safety-v1` | `floor(max_events * safety_fraction)`; the current identity is `floor(8192 * 0.85) = 6963`. | Adaptive reconstruction planning. |
| `hawkes-integrated-kernel-v1` | For `g_ij(t)=sum_q alpha_ijq beta_q exp(-beta_q t)`, `K_ij=sum_q alpha_ijq`. Stationary admission is strict: `rho(K) < configured_bound < 1`. | Marked-Hawkes fitting, deserialization, and generation. |
| `time-rescaling-pit-v1` | `z=int lambda(s|H_s) ds` and `u=1-exp(-z)`. | Analytic point-process residual inputs. |
| `energy-score-finite-ensemble-v1` | `mean(||X-y||) - 0.5 mean(||X-X'||)`. | Powered multivariate qualification. |
| `variogram-score-finite-ensemble-v1` | `sum_ij w_ij (|y_i-y_j|^p-E|X_i-X_j|^p)^2`. | Powered dependence qualification. |
| `projection-burden-dimensionless-v1` | L1 bid/ask projection movement divided by original quoted spread, with an explicit positive epsilon for zero-spread rows and no clipping. | Cross-currency projection diagnostics. |
| `fx-triangle-bid-ask-envelope-v1` | `direct_bid=numerator_bid/denominator_ask`; `direct_ask=numerator_ask/denominator_bid`. | EURGBP/EURUSD/GBPUSD reconciliation. |
| `quote-age-nearest-prior-v1` | `age=probe-selected`, constrained to `0 <= age <= maximum_age`. | Exact or bounded-nearest synchronization without future fill. |

All public reference functions reject non-finite inputs, mismatched dimensions,
unidentified probabilities, future history, and missing predictive cells. The
proper-score missing-cell policy is rejection, not implicit imputation.

## Deterministic fixtures

`current_reconstruction_math_verification_report()` executes 23 checks:

- bounded negative-binomial tail sums for sparse, transition, and modern
  retention regimes, plus a fixed-seed production sampler comparison;
- the exact adaptive planning identity;
- multi-exponential integration, exact 2x2 Perron roots, strict checks below,
  at, and above the configured Hawkes bound, and serialized structural
  tampering refusal;
- analytic compensator versus dense numerical integration, PIT and inverse
  transforms, right censoring, reset boundaries, and a finite-difference
  compensator-gradient check;
- energy and variogram numerical goldens, permutation invariance,
  non-negativity, degenerate ensembles, scaling, and missing-cell refusal; and
- dimensionless projection burden, exact bid/ask triangle sides, quote age,
  exact support, stale refusal, and no-future-use refusal.

Each check identifies both its formula and the production code paths it
verifies. Floating values retained in report identity are rounded to a stable
12-significant-digit representation after pass/fail is evaluated at the
declared tolerance.

## Machine report and certification

`ReconstructionMathVerificationReportV1` contains the complete formula
registry, content-addressed checks, a derived summary, and a report ID. The
current report has 23 passing checks and sets both `event_rows_inline` and
`samples_inline` to `false`. `from_json()` and
`read_reconstruction_math_verification_report()` recompute every nested
identity and reject changed checks, formula text, or derived summaries.

The v2.5 modern-reference certification policy requires artifact kind
`reconstruction-math-verification-report` and scalar observation
`math_verification_report_valid = true`. A campaign must bind the exact report
subject ID, schema, file hash, and `/summary/passed` value through the normal
certification campaign contract. A handwritten boolean cannot stand in for
the producer report.

The report verifies formula implementation; it does not certify fitted model
quality, campaign support, historical truth, or release readiness by itself.

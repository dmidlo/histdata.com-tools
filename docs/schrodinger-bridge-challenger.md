# Constrained Schrödinger-bridge challenger

`histdatacom.synthetic.schrodinger_bridge` supplies one registered,
dependency-free CPU proposal engine. Its challenger role tests finite-state
path-space transport while immutable observations and historical constraints
stay authoritative. Because its implemented target is broker-conditioned, it
is research-only in the HistData milestone; OANDA and broker execution remain
deferred and it cannot enter product selection.

## Falsifiable hypothesis and comparators

The predeclared issue-455 hypothesis is that, on untouched reverse-degradation
validation and final holdouts, the fixed bridge improves joint timing, mark,
path, and triangle diagnostics over:

- dense and degraded/no-fill baselines;
- linear interpolation;
- the empirical-motif candidate; and
- four classical event clocks, three marked-Hawkes variants, and two
  regime-Hawkes variants.

This is a joint hypothesis. Improvement on selected marginals does not rescue
a worse transition, path, constraint, or resource result. A solver failure,
unsupported endpoint, stale broker target, missing synchronized anchor
support, or hard-constraint failure produces no plausible-looking output.

## Finite-state bridge

For one whole window, an atom is

```text
x = normalized time bin × destination symbol × quote-transition mark
```

where the three symbols are EURGBP, EURUSD, and GBPUSD, and the four marks are
ask-only, bid-only, joint, and unchanged. The default eight-bin vocabulary has
96 atoms; the retained bounded campaign uses two bins and 24 atoms.

Training windows define a first-order transition matrix `R`. Every row gets a
lazy self-transition plus bounded smoothing

```text
s(x, y) = smoothing × exp(-c(x, y) / epsilon),
```

before normalization. Observed train-only transitions are added to that
reference; protected validation/final rows never enter it. The endpoint
reference for `H` bridge steps is `R^H`. If `mu` is the empirical source
marginal, the reference endpoint coupling is

```text
Q(i, j) = mu(i) × (R^H)(i, j).
```

Iterative proportional fitting computes positive scalings `u` and `v` such
that

```text
pi(i, j) = u(i) × Q(i, j) × v(j)
```

has row marginal `mu` and broker-conditioned column marginal `nu`. This is the
finite KL projection of the endpoint reference onto the two marginal
constraints. Missing positive support, excessive work, unstable scaling, or a
residual above the configured tolerance is a fit refusal.

Given sampled endpoints `(i, j)`, intermediate states use the conditional
Markov-bridge/Doob law, not marginal interpolation:

```text
P(X_(k+1)=z | X_k=x, X_H=j)
  = R(x, z) × (R^(H-k-1))(z, j) / (R^(H-k))(x, j).
```

The finite powers are computed once per generation window and reused for all
proposals. Dense-matrix work is charged against the generation limit before
sampling begins.

## Broker and cross-currency roles

`build_schrodinger_bridge_broker_target()` accepts one applied
`BrokerProfileSelectionV1`, one bounded `BrokerTransferConfigV1`, and the
calibration windows. The content-addressed target binds the fingerprint and
selection IDs, support status, selection time, effective interval, transfer
config, strength, cadence, count, spread, and symbol/mark/time-bin weights.
Expired or not-yet-effective profile selections are rejected.

The endpoint target is an explicit blend

```text
nu = (1 - strength) × mu + strength × normalized broker profile.
```

The target count is the same strength blend of the historical mean window
count and the count implied by broker cadence. A separate exact Poisson law
samples only the missing cardinality. Stale/duplicate broker metrics increase
the unchanged-mark weight, supported symbol weights affect destination mass,
and supported spread evidence participates in final quote projection.

The transport cost is

```text
c(x, y) = w_time × normalized bin distance
        + w_mark × mark mismatch
        + w_fx × currency-exposure L1 distance.
```

The fixed exposure coordinates encode the base/quote currency legs of the
three-symbol triangle. This cost shapes the smoothed reference kernel and is
reported separately as expected transport cost. The solver also reports
relative entropy and `expected cost + epsilon × relative entropy`; that
diagnostic is not a promotion score.

For an accepted proposal, all three symbols must have enclosing anchors at the
proposal time. A missing common interval rejects that proposal and increments
the outside-support count. The selected marked quote is then moved in log-mid
space toward the value implied by the other two legs. The residual before
projection is measured from the marked proposal itself, and the residual
after projection may not increase. Final shared cross-series validation
remains authoritative.

## Anchors, exclusions, and information boundary

Observed events stay outside the stochastic state. They are concatenated back
byte/logically unchanged and never moved, thinned, relabeled, or used as
generated lineage. Every candidate must be unique, core-owned, and strictly
inside an enclosing destination-symbol anchor pair. A candidate in a shared
`HistoricalCarvingQuarantineV1` interval is skipped; a fully excluded interval
returns only its immutable anchors.

The fit deterministically divides the six calibration windows into three
time-ordered training windows and three tuning windows. Validation and final
holdouts are reduced before fitting to row-free, content-addressed protected
manifests containing only interval, role, count, context, digest, and
near-duplicate evidence. Cross-role identity, overlap, exact duplication, or
configured near-duplicate collision refuses the fit.

Config, broker target, context, protected evidence, dataset, solver,
checkpoint, fit, generation evidence, lineage, and candidate batches have
strict versioned identities and round-trip validation. Shared
`ReconstructionCandidateBatchV1` conversion revalidates those identities
before the existing historical-carving path can accept anything.

## Solver, approximation, and resource evidence

Fit evidence records the complete residual trace, source and target marginal
residuals, iterations, missing-support and numerical-repair counts, positive
kernel/scaling ranges, expected cost, relative entropy, regularized objective,
time-bin quantization error, stitched-window transition L1 error, work, wall
time, peak RSS, parameter count/bytes, CPU/OS/machine, and refusal reason.

Whole-window solving is the scientific reference. Streaming accepts only a
bounded strict-prior history. Its last supported symbol/mark state reweights
the source edge of the endpoint coupling; generation reports the L1 distance
between the original and conditioned couplings. Current/future history is a
failure, and unsupported prior state is a refusal. Tests cover both the
changed coupling and the zero-history reference behavior.

Generation has independent caps for Poisson work, path work, event count,
candidate amplification, estimated/measured memory, and wall time. It reports
requested/generated counts, asynchronous-support skips, quarantine skips,
collisions, triangle residuals, boundary error, semantic seed, all input and
lineage digests, work, time, memory, and exact refusal/failure reason. Fixed
sources, full configuration, window, member, context, and history produce the
same result; independent member IDs produce distinct paths.

## Retained real-corpus result

The final issue-455 campaign reused the content-addressed real histdata corpus
`reverse-degradation-corpus:sha256:a760a010d44de2d6258b7c3d71651b00bc24eaef53092f37bd75b3ae2395c5dc`.
It used six January 2010 calibration windows, six January 2024 validation
windows, and six October 2025 final-holdout windows across all three symbols
and Asia/London/New York sessions. Only the 12 validation/final windows were
evaluated; two ensemble members yielded 552 bridge window metrics.

No retained real broker fingerprint was available. The broker target is
therefore explicitly a controlled synthetic-capture profile, not evidence of
real broker adaptation. The repository's deterministic capture pipeline fit
fingerprint
`broker-delivery-fingerprint:sha256:7c02cb40467404f15a262343e41e1f754646cdc7692985f922cd5dde205ccf3d`
from 250 ms cadence, 0.0002 spread, millisecond timestamp precision, and five
decimal places. The bridge used strength 0.1 and a 24-state/two-bin bounded
configuration.

The solver converged in 159 iterations with maximum marginal residual
`9.546875651134101e-10`, zero missing-support cells, zero numerical repairs,
183,168 units of charged solver work, expected transport cost
`0.9261639534387887`, and boundary transition L1 error
`0.262783344813007`. Three training windows contained 1,835 events, three
tuning windows contained 1,687 events, and all 12 holdout windows stayed
row-free and protected.

Across 24 held-out member/window attempts, the bridge emitted 4,424 candidates
and skipped 5,989 proposals outside common synchronized anchor support. It had
zero fit/generation failures, refusals, collisions, immutable-anchor
violations, and unsupported-context emissions. Mean triangle residual fell
from `0.0006545535643166801` before projection to
`0.0004909151732375241` after projection. The complete 15-report campaign ran
in 40.66 seconds with 176,291,840 peak RSS bytes and 1,231,987 compact artifact
bytes.

The hypothesis was not supported as a joint claim. Representative maximum
errors were:

| Method | Count | Interarrival | Path variation | Spread tail | Update transition |
|---|---:|---:|---:|---:|---:|
| Schrödinger bridge | 0.6471 | 0.3156 | 1.0009 | 0.1250 | 0.8917 |
| Degraded/no-fill | 0.6742 | 0.3427 | 1.0161 | 0.0286 | 0.1142 |
| Linear interpolation | 4.5286 | 0.4436 | 0.5686 | 0.0925 | 0.5767 |
| Empirical motif | 0.6654 | 0.4793 | 19.4473 | 0.2727 | 0.3103 |
| Best marked-Hawkes value by column | 0.5924 | 0.2494 | 0.9421 | 0.0607 | 0.8389 |

The bridge improved count and interarrival error over degraded identity,
linear interpolation, and empirical motif, and greatly improved motif path
variation. It did not dominate the simpler point-process candidates and its
update-transition error was materially worse than degraded identity, linear
interpolation, and empirical motif. It failed the event-count, path-variation,
triangle-residual, and update-transition promotion gates. Anchor integrity,
execution, interarrival, refusal reporting, spread tail, uncertainty support,
and unsupported-emission gates passed. The report remains provisional and
`automatic_winner` is false.

The retained campaign and bridge report identities are
`reverse-degradation-campaign:sha256:28f6bebcd6ce242a637dbbc65bab4975eec05168011db89f02ccc1bd596d7f15`
and
`reverse-degradation-candidate-report:sha256:3490191d4a423bfb95ee373a9bf579f66f94e21cf6c52bb6e9440fe3007d6fc6`.
The config, target, dataset, fit, and checkpoint identities are:

- `schrodinger-bridge-config:sha256:4dadf4acd8980a3087397cc77e6d6c652f2cc42127da1cbd9441c868cd028c30`;
- `schrodinger-bridge-broker-target:sha256:2a63b3538e93200866f42fa462557bb229f890cc79788b4fe3dc6133d0e6d9a9`;
- `schrodinger-bridge-dataset:sha256:c89be8e4a16e161512f33af159b5d3f91eecdae74083d6b882f3ad067f84e152`;
- `schrodinger-bridge-fit:sha256:815b65f4c7cc371db73d0421be8f57b1d18e4981f791ba7ef83c345337c6b3b9`;
  and
- `schrodinger-bridge-checkpoint:sha256:bf8452f5d95f1c00db227ec84fdf68024829e3c9d7f93906b51a7fbc051182c0`.

The retained scorecard, resource audit, manifest, motif index, and leakage
audit SHA-256 digests are, respectively,
`97967b7b64572c5547d657026a0eb1ec711c92cc3024fe669a719219309dcbd7`,
`7d111e3ca888ccb4eef38335ebdd7ad416f5f034723c9794144ad1641b35e704`,
`d1ddf45d68ade8c1ba4abc3df5a60a26483bb3eab950d4c29f53709e9214ed24`,
`d1e0fd447fd6a803e3c0a3d72753bcfa6407b6bcb3747632fd4a12d82451247e`,
and `3f532feaebabf3f3503dd719b17a4c04398cd110c283f29fcd9c60bfee6269c5`.

## Scientific nonclaims

- These finite categorical states are not a continuous or neural diffusion.
- The controlled broker fixture does not prove adaptation to a real broker.
- Generated candidates are counterfactual proposals, not recovered ticks.
- A converged Sinkhorn solve does not imply realistic market paths.
- The failed joint hypothesis is not a reason to tune on protected holdouts.
- Production use requires a separate evidence-backed promotion issue.

## Primary references

- De Bortoli et al., [*Diffusion Schrödinger Bridge with
  Applications to Score-Based Generative Modeling*](https://arxiv.org/abs/2106.01357),
  NeurIPS 2021.
- Léonard, [*A Survey of the Schrödinger Problem and Some of Its Connections
  with Optimal Transport*](https://arxiv.org/abs/1308.0215), 2014.
- Di Marino and Gerolin, [*An Optimal Transport Approach for the Schrödinger
  Bridge Problem and Convergence of Sinkhorn
  Algorithm*](https://arxiv.org/abs/1911.06850), 2020.

# Bounded marked Add-Thin sequence challenger

`histdatacom.synthetic.add_thin` is one opt-in, dependency-free CPU research
challenger. It adds a variable-cardinality point-process diffusion comparison
after the empirical motif, four classical event clocks, three static marked
Hawkes models, two regime-Hawkes models, and the bounded RMTPP. It does not
change the certified empirical default.

## Method boundary

The implementation follows the forward point-process construction and the
B/C/D/E reverse decomposition from Lüdke et al., [*Add and Thin: Diffusion for
Temporal Point Processes*](https://arxiv.org/abs/2311.01139), NeurIPS 2023.
For per-step retention `alpha_n`, cumulative retention `alpha_bar_n`, clean
intensity `lambda_0`, and homogeneous-Poisson noise intensity `lambda_HPP`,
the forward marginal is

```text
lambda_n(t) = alpha_bar_n * lambda_0(t)
            + (1 - alpha_bar_n) * lambda_HPP.
```

Every forward step independently thins existing points and superposes an HPP.
The fixed schedule is:

| Step | `alpha_n` | `alpha_bar_n` |
|---:|---:|---:|
| 1 | 0.8 | 0.8 |
| 2 | 0.75 | 0.6 |
| 3 | 2/3 | 0.4 |
| 4 | 0.5 | 0.2 |

For a reverse step `n -> n-1`, the implementation uses the paper's exact
coefficients:

```text
C missing-clean coefficient = (alpha_bar_(n-1) - alpha_bar_n)
                              / (1 - alpha_bar_n)
D transient-noise coefficient = (1 - alpha_bar_(n-1)) * (1 - alpha_n)
E retained-noise probability = (alpha_n - alpha_bar_n) / (1 - alpha_bar_n)
```

The implementation is deliberately not a reproduction of the paper's CNN
classifier or Gaussian-mixture missing-intensity model. Its fixed
`histogram_marked_add_thin_cpu_v1` approximation estimates positive,
piecewise-constant time-bin × joint-mark tables from training windows only.
An empirical-Bayes table gives the probability that a point at each
step/bin/mark came from the clean process. A fixed additive-smoothing grid is
selected by the untouched tuning-window denoising objective.

## Project-specific marks

The paper models arrival times and leaves marks to future work. This module's
marks are therefore an explicit project extension, not a claim about the
reference method. The fixed 12-way vocabulary is the Cartesian product of
three destination symbols and four quote transitions:

- ask-only update;
- bid-only update;
- joint bid/ask update; and
- unchanged quote.

The versioned policy resets prior-quote state for every symbol at every
whole-window boundary. Generation projects a sampled transition using only
its enclosing immutable destination-symbol anchors. Endpoint exclusion,
nanosecond quantization, equal-time ordering, projection, unsupported-mark,
and collision behavior are separate immutable config fields. Unsupported
intervals are skipped and counted. Invalid quotes, nonfinite values, negative
spreads, unknown marks, or an unsupported symbol fail closed.

## Dataset and information boundary

The fit consumes exactly six calibration windows: the first Asia, London, and
New York occurrence is training data and the second occurrence of each is
tuning data. It records a row-free, content-addressed manifest with event and
context digests, interval bounds, counts, sessions, symbol/mark support,
deterministic order, and the fixed time-bin policy.

Validation and final-holdout windows are reduced to content digest,
near-duplicate signature, interval, count, role, and context identity before
fit. Their rows never enter rate estimation, HPP-noise estimation, smoothing
selection, corruption diagnostics, or checkpoint choice. Cross-role window
identity, interval overlap, exact duplication, or a configured SimHash
collision refuses the complete fit. Protected rows are never serialized.

Technological feed assignment, denoising step, session, benchmark split, and
optional observed context are separate axes. Stable epochs preserve the feed
label, immutable epoch ID, and definition identity; transition windows
instead preserve the boundary ID, support, and uncertainty periods. Ex-ante
fitting requires an as-of boundary and refuses future events or context.
Ex-post fitting forbids an as-of value.

Config, context, protected evidence, dataset, checkpoint, fit, generation
evidence, and lineage use strict versioned, bounded, content-addressed
contracts. Unknown fields, altered nested content, stale IDs, or oversized
JSON fail validation.

## Fit and checkpoint

The clean estimator uses 16 normalized half-open time bins and all joint
marks. Additive smoothing keeps every intensity finite and positive. The
retained-point classifier is the explicit clean-origin posterior for each
diffusion step and cell. Candidate smoothing values share the same semantic
tuning corruption, so checkpoint selection is not confounded by different
random draws. The uniform unconditional baseline is evaluated on that same
corruption.

The immutable checkpoint binds:

- architecture, time policy, HPP law, schedule, symbols, and mark vocabulary;
- selected smoothing, clean intensities, and every classifier table;
- train/tune retained-point BCE and missing-count Poisson NLL;
- joint objectives, tuning count error, tuning mark L1 error, and every
  smoothing-candidate objective; and
- exact table shape, parameter count, encoded parameter bytes, and digest.

Fit evidence records CPU-only execution, OS, machine, Python implementation
and version, deterministic-math scope, wall time, and peak RSS. Volatile
measurements do not enter scientific identity. The implementation imports no
ML framework and refuses a requested accelerator before materializing a
dataset.

## Reverse generation, anchors, and lineage

Generation starts from bounded HPP noise over the owned core interval and
executes every reverse step over the whole variable-cardinality point set.
Each step separately audits:

- B: current points classified and retained as clean;
- C: sampled missing-clean points;
- D: sampled transient reverse HPP noise;
- E: current noise points retained with the declared E probability; and
- thinned and timestamp-collision counts.

The final step retains B predictions and samples the final C intensity.
Observed anchors never enter this state: they cannot be thinned, moved,
replaced, or relabeled, and are concatenated back unchanged. Candidate points
must be unique, core-owned, and strictly enclosed by an observed anchor pair.
Nanosecond collisions are counted and skipped rather than shifted.

The semantic seed binds config, fit, dataset, checkpoint, scenario, window,
ensemble member, complete degraded input, bounded strict-prior history, and
context identities. Retained history contributes only through the recorded,
bounded event-count conditioning multiplier. Every emitted lineage records
its initial/reverse origin, creation step, survival count, time bin,
destination and transition, mark probability, bin intensity, parent pointer,
explicit final survival, two anchor IDs, anchor interval, and immutable
lineage ID.

Independent limits cover fit windows/events, bins, marks, schedule, smoothing
grid, corruption points, Poisson work, parameters, checkpoint bytes, history,
generation points/steps, events per bin and anchor interval, amplification,
memory, output bytes, and wall time. A limit breach returns no anchors,
candidate rows, or carveable lineage.

## Shared carving and benchmark seam

`build_add_thin_candidate_batches()` revalidates run, window, member,
configuration, fit, dataset, checkpoint, generation, context, and anchor
identities. Its batches implement the shared `ReconstructionCandidateBatchV1`
protocol. Historical carving remains authoritative for immutable anchors,
resources, fingerprint evidence, context, quarantine, session closure,
conditioned intensity, spread projection, synchronized validation, broker
conditioning, and final local quality.

Passing `add_thin_config=default_add_thin_config()` to
`run_reverse_degradation_benchmark_campaign()` adds exactly one report. With
all optional families enabled, the complete comparison has 16 reports and 12
challengers. Omitting the config retains the pre-existing campaign behavior
and candidate identities. The Add-Thin report exposes split/leakage, fit and
tuning objectives, checkpoint/resources, B/C/D/E cardinalities, refusals,
anchor integrity, and the common timing, dispersion, duration, mark,
spread/tail, and path metrics. `automatic_winner` is always false.

## Retained real-corpus result

The closure campaign uses the same content-addressed corpus and qualified
reference motif index as the preceding RMTPP comparison. It contains 16
reports, including all 12 challengers, over 18 windows, two ensemble members,
all three symbols and sessions, and the epoch-03 calibration to epoch-04
validation/final-holdout boundary. The selected corpus has no feed-transition
window, so transition handling is fixture-verified and is not claimed as
real-campaign coverage.

The Add-Thin fit used 1,835 training events in three whole windows and 1,687
tuning events in three whole windows. All 12 validation/final windows remained
protected. Exact duplicates, near-duplicate collisions, interval overlaps,
fit failures, generation failures, refusals, timestamp collisions, and
immutable-anchor violations were zero. The selected smoothing was 0.25;
tuning objective 1.803909 improved over the uniform baseline 2.749791.

Across 24 held-out member/window attempts, reverse evidence recorded 13,584
initial HPP points, 13,334 final points, 6,240 emitted proposals, 7,094 points
outside usable anchor intervals, 25,596 B decisions, 10,855 C additions,
6,620 D additions, 9,880 E retentions, and 17,725 thinnings. The challenger
failed its event-count, path-variation, triangle-residual, and
update-transition promotion gates. It remains an auditable research
comparison and does not replace the empirical default.

The retained campaign identity is
`reverse-degradation-campaign:sha256:374dd0fa89c38f85278740e16fc94d0b0e7721c921c85d85007cab5994da41f6`.
The Add-Thin report, config, fit, dataset, and checkpoint identities are,
respectively:

- `reverse-degradation-candidate-report:sha256:a11b8311bc0c70e9d057873bb662efa8e1ba3305ec2f6a497845e4a928832a83`;
- `add-thin-config:sha256:4ba5f2bd17474e13ddb3362a0795e9ea77d36c484861e5d130cf67fe1308b1b1`;
- `add-thin-fit:sha256:e87742213ed879cbc107cdc8928a18aa7c4d3e4c3a0c5788a0885ca53a92e3f3`;
- `add-thin-dataset:sha256:c88c895879c61eda4e85bd126a157d8919c02f4569cc842e0ae0b58f856bfd39`;
  and
- `add-thin-checkpoint:sha256:388abe044a8b789e9fe8901c16498513f5a619e3e23dde7377bc7a5d6fa1a87c`.

All five retained artifacts pass filename/content SHA-256 verification. The
scorecard and resource-audit digests are
`5ef8f3e2a4aa78b02606dd14b6f8bbcf7d87ae07db968b4feac5526ffa58cac4`
and `41aeb609ba036e5802d9a734b4a684c0747ccd28156c36bf7e0faacc3236b6c4`.
The reused row-free manifest, motif-index, and leakage-audit digests remain
`d1ddf45d68ade8c1ba4abc3df5a60a26483bb3eab950d4c29f53709e9214ed24`,
`048dd46deabf66643fc55b9cb2a996828c88f8c099a9d9573a4d29a632bce9a3`,
and `e1cd0fcc9da45756dfe5f121742bfd26e1ba0cdda46971fbae7eb691402d0165`.

## Scientific nonclaims

- This table estimator is not the paper's CNN/Gaussian-mixture architecture
  and is not claimed to be state of the art.
- The categorical marked extension is project-specific.
- A lower denoising objective is not evidence of reconstruction superiority.
- Denoised points are candidate proposals, not observed facts.
- This is not broker adaptation, Gaussian diffusion, a Schrödinger bridge, or
  a production simulator.
- The challenger cannot promote itself or change the certified default.

## Primary references

- Lüdke et al., [*Add and Thin: Diffusion for Temporal Point
  Processes*](https://proceedings.neurips.cc/paper_files/paper/2023/file/b1d9c7e7bd265d81aae8d74a7a6bd7f1-Paper-Conference.pdf),
  NeurIPS 2023.
- The authors' [reference implementation](https://github.com/davecasp/add-thin).

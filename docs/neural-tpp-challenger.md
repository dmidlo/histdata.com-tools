# Bounded recurrent marked temporal point-process challenger

`histdatacom.synthetic.neural_tpp` supplies one registered RMTPP proposal
engine. Its challenger role places it after the empirical, classical
event-clock, static Hawkes, and regime-Hawkes comparators in retained campaigns.
It remains benchmark-eligible, cannot promote a winner, and adds no neural
framework dependency.

## Fixed model

The implementation is `rmtpp_cpu_v1`, based on Du et al.'s recurrent marked
temporal point process. For history state \(h_i\), elapsed seconds \(\tau>0\),
and next joint mark \(k\), it uses

\[
h_i=\tanh(W_hh_{i-1}+W_xx_i+b_h),
\]

\[
\lambda(t_i+\tau\mid\mathcal H_i)
  =\exp(b_\lambda+v^Th_i+w\tau),
\]

and

\[
P(k\mid\mathcal H_i)=\operatorname{softmax}(W_mh_i+b_m)_k.
\]

The positive slope \(w\) is configuration-bound. The time compensator over an
interval of length \(d\) is exact:

\[
\Lambda(d)=\exp(b_\lambda+v^Th_i)\frac{\exp(wd)-1}{w}.
\]

Generation draws \(E=-\log(1-U)\) and uses the exact inverse

\[
d=\frac{1}{w}\log\left(1+
  \frac{wE}{\exp(b_\lambda+v^Th_i)}\right).
\]

There is no numerical quadrature, rejection sampler, or hidden
continuous-time approximation. The mark head is conditionally independent of
the next duration given the recurrent state. That deliberate restriction is
not presented as state of the art.

The joint vocabulary contains the three synchronized symbols crossed with
`ask_only`, `bid_only`, `joint`, and `unchanged`, for 12 possible marks in the
real campaign. The recurrent input has an explicit one-hot start token, the
previous joint mark, and a training-normalized `log1p` duration. State and
per-symbol prior quotes reset at every window boundary. Deterministic
time/symbol/source-sequence/event-identity ordering preserves equal-time rows;
only the recurrent duration feature receives the declared one-nanosecond
minimum, while exact end-of-window censoring remains unchanged.

## Dataset and information boundary

`NeuralTPPDatasetManifestV1` is row-free and content addressed. It records
each window's role, interval, event digest, fixed 64-bit near-duplicate
signature, event count, session, technological context, and ordering policy.
The real calibration split assigns the first occurrence of each Asia, London,
and New York session to training and the second to tuning. Preprocessing
statistics are learned from training durations only.

Validation and final-holdout rows never enter the manifest. Instead,
`NeuralTPPProtectedWindowV1` supplies only their content/signature/interval
evidence to the leakage audit. Cross-role interval overlap, exact content
duplication, or a signature collision at or below the configured Hamming
threshold fails the complete fit. Recurrent state never crosses a source
window.

`NeuralTPPWindowContextV1` keeps recurrent state, session, technological feed
epoch or transition, and optional observed context separate. Stable epochs
bind the v2 definition and epoch identities. Transition windows bind the
boundary identity, support, and uncertainty periods. Ex-ante fitting requires
an `as_of_ns` boundary and refuses future events or context; ex-post fitting
forbids an as-of value.

## Deterministic CPU training and checkpointing

Training is dependency-free standard-library Python, CPU-only, full-batch
backpropagation through time. Initialization, hidden width, learning rate,
gradient and parameter clipping, epoch bound, tuning-only checkpoint
selection, and early stopping are configuration-bound. Accelerator requests
are rejected when the configuration is constructed, before dataset material
is exposed or training starts.

The likelihood includes exact event-time density, joint-mark cross-entropy,
and exact right censoring at every window end. The bounded trace records
normalized train/tune negative log likelihood, tuning mark accuracy,
log-duration error, probability-integral-transform mean, gradient norm,
selected epoch, clipped epochs, and gradient work. Tests compare an analytic
recurrent-weight gradient with a central finite difference.

`NeuralTPPCheckpointV1` binds preprocessing statistics, vocabulary and tensor
shapes, every finite parameter, selected epoch, train/tune metrics, parameter
count, parameter bytes, and an immutable digest. The optimizer has no hidden
replay state. Runtime evidence records the Python implementation/version,
machine class, operating system, CPU-only policy, wall time, and incremental
peak RSS, but volatile measurements do not participate in the scientific
training or fit identity. Failed and refused fits expose no dataset,
checkpoint, parameters, or partial training manifest.

## Synchronized generation and lineage

`FittedNeuralTPPBenchmarkGeneratorV1` maintains one recurrent state for the
whole EUR/GBP/USD synchronization unit. Its semantic seed binds configuration,
fit, dataset, training, checkpoint, scenario, window, member, full input,
retained history, and context identities.

Observed anchors update the recurrent state only when the sampling cursor
reaches their time. Generated events update it only after emission. Every
candidate timestamp is unique, owned by the core window, and strictly inside
an observed destination-symbol anchor pair. Quote projection applies the
sampled transition to the enclosing quotes. A sampled mark with no supported
interval is explicitly counted and skipped; the hazard cursor advances but
the recurrent state does not. This is a declared proposal-conditioning rule,
not silent clamping.

Prior history is explicit, synchronized, cardinality bounded, inside the
declared lookback, and strictly before the input window. Older rows contribute
nothing. Present, future, duplicate, or foreign-symbol history refuses the
whole attempt. Step, event, per-interval, amplification, history, parameter,
estimated/measured memory, output-byte, and wall-time limits are independent.
Any breach returns no anchors, proposals, lineage, or carveable output.

Successful output retains every input anchor object unchanged. Each generated
lineage records sampled step, destination, transition mark and probability,
elapsed time, conditional intensity, log joint density, pre-event hidden-state
digest, parent identity, and benchmark anchor interval. Attempt evidence binds
all scientific identities and content digests. `GENERATED` and `EMPTY` are the
only successful states; `REFUSED` and `FAILED` cannot contain partial rows.

## Historical carving and campaign comparison

`build_neural_tpp_candidate_batches()` revalidates run, window, member,
configuration, fit, dataset, training, checkpoint, generation, context, and
anchor identities. Its batches implement the shared
`ReconstructionCandidateBatchV1` protocol. The existing historical carving
engine remains authoritative for immutable anchors, resources, fingerprint
evidence, context, quarantine, session closure, conditioned intensity, spread
projection, and final local validation.

Passing `neural_tpp_config=default_neural_tpp_config()` to
`run_reverse_degradation_benchmark_campaign()` adds exactly one neural report.
With all prior optional registries, the comparison has 15 reports: four
baseline/control reports and 11 challengers (empirical motif, four clocks,
three static Hawkes models, two regime-Hawkes models, and one RMTPP). The fit
report includes split/leakage, likelihood, mark, duration, checkpoint,
gradient, and resource evidence alongside the unchanged stream metrics.
`automatic_winner` remains false.

## Retained closure campaign

The retained real-corpus campaign is
`reverse-degradation-campaign:sha256:eb9741d3b1d5bb4678eca894685a216870e67f1fb103d18e0520dfb58fab820e`.
It reuses corpus
`reverse-degradation-corpus:sha256:a760a010d44de2d6258b7c3d71651b00bc24eaef53092f37bd75b3ae2395c5dc`
and reference motif index
`reference-motif-index:sha256:b5d5e7d9580fac375c42677fe5d03be96fafc190f799364a52566af7aa5a2589`.
Source replay passed for all nine inputs and all 54 symbol-window partitions,
with no source-hash, information-audit, holdout-neighbor, dense-identity, or
anchor-integrity violation. The campaign exercised 18 measured windows, two
ensemble members, all three sessions and symbols, the stable technological
epochs, and the declared epoch-03 calibration to epoch-04 evaluation
boundary. The real corpus did not select a feed transition window, so the
transition contract is verified by focused fixtures rather than claimed as
real-campaign coverage.

The selected fit is
`neural-tpp-fit:sha256:801842ad365e82349ea04cb14b53b2ca3d0a498f89eeaa1bff4076acc585aa17`.
Its associated dataset, training trace, and checkpoint are:

- `neural-tpp-dataset:sha256:19951c2f97711ffde5f2951a19696b1c9130d1cffa666484d40314c65c953b0b`
- `neural-tpp-training:sha256:c99fc606e4985e57fc2b5c8aa052140960ba50b6d9d43e0170585d8d57876e6f`
- `neural-tpp-checkpoint:sha256:02ea68506721c5e122ad62e7eba2ed40f4282dccd93282f2f946d53aa78bd3fc`

Training used 1,835 events in three whole windows; tuning used 1,687 events
in three separate whole windows. Epoch 40 was selected from 40 completed
epochs. The checkpoint contains 309 parameters (6,750 serialized parameter
bytes), and the bounded gradient work was 22,680,600 with maximum recorded
gradient norm 0.279580. Mean normalized negative log likelihood was 3.461239
for training and 3.549778 for tuning. The tuning decomposition was 1.070875
for time and 2.478903 for marks; mark accuracy was 0.221695, log-duration RMSE
was 0.592358, and mean probability-integral-transform value was 0.375962.
Exact, near-duplicate, and interval-overlap audit counts were all zero.

The campaign produced all 15 reports, including 11 challengers. The neural
candidate evaluated all 12 validation/final windows with two members, zero
failures, zero refusals, and zero immutable-anchor violations. Its selected
stream errors, compared with the directly inspectable empirical and prior
point-process challengers, were:

| Candidate | Event count | Interarrival | Update transition | Path variation | Spread tail |
| --- | ---: | ---: | ---: | ---: | ---: |
| Empirical motif | 0.108302 | 0.154104 | 0.369638 | 0.163958 | 0.041822 |
| Static full Hawkes | 0.378342 | 0.191042 | 0.581996 | 0.237544 | 0.022513 |
| Regime baseline + excitation | 0.429703 | 0.154513 | 0.721511 | 0.209439 | 0.038760 |
| RMTPP | 0.381555 | 0.210748 | 0.591464 | 0.303450 | 0.025364 |

The campaign integrity gate passed, but the RMTPP candidate promotion gate
failed on event-count, path-variation, and update-transition requirements.
It is therefore retained as an auditable research challenger only;
`automatic_winner` is false and the production default is unchanged. The
bounded campaign took 38.358593 seconds, observed 159,088,640 bytes of process
peak RSS, and emitted 954,190 bytes of retained artifacts. The scorecard,
manifest, motif artifact, leakage audit, and resource audit SHA-256 digests
are respectively
`fcd26e11535fe9fd499d0fa88f0bcf388fe7c0367a117a770056790debc88ada`,
`d1ddf45d68ade8c1ba4abc3df5a60a26483bb3eab950d4c29f53709e9214ed24`,
`048dd46deabf66643fc55b9cb2a996828c88f8c099a9d9573a4d29a632bce9a3`,
`e1cd0fcc9da45756dfe5f121742bfd26e1ba0cdda46971fbae7eb691402d0165`,
and `173eb81b97f612a5937e3d8b29ddbbaf99c4ea584924a30e84e2362325c0f265`.

## Scientific nonclaims

- The hidden vector is not an observed economic regime or causal explanation.
- Parameter count or neural complexity is not evidence of improvement.
- This is not a continuous-time LSTM Neural Hawkes implementation.
- The independent mark head is not universally adequate.
- This does not implement broker adaptation, diffusion, or Add-and-Thin.
- This does not replace the certified empirical default or create a release by
  itself.

## Methodological references

- Du et al., [*Recurrent Marked Temporal Point Processes: Embedding Event
  History to Vector*](https://doi.org/10.1145/2939672.2939875), KDD 2016.
- Mei and Eisner, [*The Neural Hawkes Process: A Neurally Self-Modulating
  Multivariate Point Process*](https://arxiv.org/abs/1612.09328), methodological
  context only.
- Shchur et al., [*Intensity-Free Learning of Temporal Point
  Processes*](https://openreview.net/forum?id=HygOjhEYDH), context for the
  limits of intensity-based parameterizations.

The next independent comparison is the [bounded marked Add-Thin
challenger](add-thin-challenger.md); it does not modify this RMTPP contract or
its retained 15-report closure campaign.

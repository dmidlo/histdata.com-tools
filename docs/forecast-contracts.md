# Forecast observation and scoring contracts

This is the v1 contract layer for issue #560 in the v2.7 forecasting roadmap
(#556). It can operate on provenance-complete point-in-time calendar fixtures;
it does not claim that the production calendar or model bank is complete.
Import the public contracts from `histdatacom.forecasting`.

The wheel and source distribution include `forecasting/py.typed`, scoped to
this subpackage rather than declaring the entire host package typed. Installed
consumers can check the public contracts with strict mypy without a source
checkout or `PYTHONPATH`; invalid constructor field types are rejected. The
existing calendar package's typing policy is unchanged.

## Observation unit and immutable inputs

`ForecastSnapshotV1` seals an event/indicator/series/reference-period target,
cutoff and horizon, exact input vintages, model or ensemble state, a predictive
distribution, and generation time. SHA-256 identities include nested content,
not just caller-provided names. Changing any input, parameter, target, schedule,
distribution or reference semantics changes the snapshot identity.

`capture_forecast_inputs(corpus, cutoff_at_ns=...)` retains **all** normalized
calendar release and consensus vintages available by the cutoff, including
earlier vintages in revision chains. It never selects revised values in place
of the original vintages. The captured corpus is explicitly incomplete: a
cutoff projection is not a statement of complete historical coverage.

`ForecastInputsV1` stores canonical JSON rather than a mutable calendar object.
Reading its `calendar` property returns a fresh decoded corpus. This also
isolates the snapshot from mutable dictionaries inside source provenance.
`verify_against(corpus)` requires every retained release/forecast identity and
payload to be present exactly; extra later vintages do not change the inputs.
Raw source downloads are not embedded. Existing calendar source hashes,
adapter versions, lexical evidence, availability and provenance are preserved.

The model contract retains name, version, implementation-code SHA-256,
canonical JSON model state and exact training inputs. Ensemble state can
retain member identities, configuration and weights in the same sealed JSON
state. Training inputs must be included in the forecast information set, and
their cutoff cannot follow training. Training cannot follow generation;
generation cannot follow the forecast cutoff. No input may arrive after
generation. These contracts do not execute or certify a model algorithm:
replaying inference requires the implementation bytes identified by the
digest, in addition to the retained state and inputs. Model runners and their
binary-artifact storage are later roadmap work.

## Clock and horizon semantics

| Horizon | Required cutoff relative to the known schedule |
| --- | --- |
| `T-30d` | Exactly 30 elapsed 24-hour days earlier |
| `T-7d` | Exactly 7 elapsed 24-hour days earlier |
| `T-1d` | Exactly 24 elapsed hours earlier |
| `T-1h` | Exactly 1 elapsed hour earlier |
| `final-pre-release` | Explicit positive lead of at most 1 hour |

All clocks are signed int64 Unix nanoseconds. Fixed horizons are elapsed UTC
durations, not local calendar-day arithmetic. Final-pre-release remains a
distinct task even if its selected lead equals one hour. Its explicit lead is
an ex-ante dispatch policy, not a hindsight claim that no later forecast or
schedule change occurred.

The anchor must be the latest schedule vintage observable in the information
set. An unknown eventual release timestamp cannot be used to retroactively
label a forecast. A reschedule observable by the cutoff invalidates an older
anchor. A later reschedule does not rewrite an already sealed snapshot.

At scoring, the cutoff must be **strictly before first-release availability**.
Publisher release time, source availability, source retrieval and scoring time
are distinct. A release published before cutoff but unavailable to the retained
information set until after cutoff is not silently relabelled as observable.
The score retains both publication and availability timestamps. This is an
availability-based information-set contract, not a claim that the public had
no other access path. Source adapters remain responsible for defensible
availability evidence. Equality at availability is too late for a forecast.

## Four distinct targets

| Target | Realized value used for scoring |
| --- | --- |
| `consensus` | Named institutional/professional source statistic as of its explicit observation cutoff |
| `first-release-actual` | The sole non-revision initial publication's actual |
| `revision` | Exact requested calendar revision sequence, either revised level or revision minus first actual |
| `surprise` | First actual minus explicitly observed or predicted consensus |

Event/series/indicator/period/unit/scale/base must match the schedule and outcome
release. Initial, flash, preliminary and final release stages retain the
calendar's logical-event identity; a later vintage revision is not a new
first-release actual. Missing first-release history causes rejection, even if
a revision with an actual value exists. No “use latest” fallback exists.

V1 revision targets are forecasts made **before the initial release**, aimed
at a named future revision vintage. `revision_sequence` means the exact
calendar vintage sequence (which can include schedule changes), not the nth
numeric revision. A post-initial revision-forecast runner is not implemented.

`ConsensusReferenceV1` names the source, adapter, event-eligible scope,
mean/median statistic and observation time. Central-bank projections, period
surveys and market-implied proxies do not silently become event consensus.
The latest uniquely identifiable vintage available by the reference time is
used; ambiguous equally recent vintages fail closed.

Ordinary consensus references must use `at-cutoff` with observation time
exactly equal to the snapshot cutoff. A T-7d snapshot therefore cannot be
scored against a T-1d consensus. The sole exception is an explicitly declared
`consensus` target with `future-consensus-evolution`, whose observation time
must lie after cutoff and before the known scheduled release. This different
task is kept separate in aggregation and must still precede actual release
availability once that is known. Future consensus cannot be used as an
ordinary actual-forecast value-added benchmark.

Observed-consensus surprise uses its named cutoff reference. Predicted-consensus
surprise embeds a separate sealed consensus prediction for the same event,
cutoff, horizon and information set. Its reference is that prediction's point
value, not subsequently observed consensus; the score retains its snapshot ID.
Scoring surprise does not imply that the embedded consensus task was scored.

## Distribution and metrics

`ForecastDistributionV1` is a finite probability distribution. Its support must
be finite and strictly increasing, its probabilities positive and summing to
one (absolute numerical tolerance `1e-12`). The point forecast is explicitly
the probability-weighted mean or lower weighted median. A singleton represents
a deterministic forecast. Parametric distributions and calibration metrics
are outside this initial contract layer; a discrete representation does not
claim calibration.

For point forecast `p`, observed target `y`, and positive denominator `s`:

- Signed error: `p - y`.
- Absolute error: `abs(p - y)`; squared error: `(p - y)^2`.
- MAE: arithmetic mean of absolute errors.
- RMSE: square root of the arithmetic mean of squared errors.
- Median absolute error: ordinary sample median (midpoint for even counts).
- Normalized MAE/RMSE/median: the same calculations using per-event errors
  divided by their committed denominator `s`.
- Consensus replication error, only for the consensus target:
  `abs(machine_consensus - reference_consensus)`.
- Independent value-added, only for the first-release-actual target with a
  valid cutoff reference:
  `abs(first_actual - reference_consensus) - abs(first_actual - machine_actual)`.
  Positive values mean the machine actual prediction beats the reference.

Consensus replication and actual prediction are never combined into one
target. Value-added is absent, not zero, when an admissible benchmark is absent.
All score arithmetic must remain finite; overflow fails closed.

`ForecastScaleV1` retains a positive finite denominator, named meaning, target
unit, establishment time, methodology and optional exact input-release IDs.
Its identity must be committed in the snapshot before scoring. The receipt is
required when scoring a snapshot with that commitment; changing it or adding
a denominator after the fact fails. Establishment must precede generation,
and all listed evidence must be available then. Denominator estimation is
external to this layer: the stored parameter makes the metric reproducible,
but does not prove that a described estimator was actually executed. Empty
evidence is appropriate only for an explicitly predeclared fixed unit, not an
undocumented empirical estimate.

`ForecastScoreReportV1` embeds all constituent score evidence and computes the
metrics above. It permits one forecast per event, rejects duplicate snapshots,
and only aggregates comparable target, horizon, final lead, series/unit/base,
reference policy, point statistic, model family/version/code and normalization
method. Rolling model states and scale values may differ between events; their
exact contents remain in each snapshot/score. Cross-indicator normalized
leaderboards are deliberately not inferred by this API.

## Replay and persistence

Create a score from a sealed snapshot and a canonical outcome calendar corpus:

```python
from histdatacom.forecasting import ForecastScoreV1, ForecastScoreReportV1
from histdatacom.forecasting import read_forecast_artifact, write_forecast_artifact

score = ForecastScoreV1(
    snapshot_json=snapshot.to_json(),
    outcome_calendar_json=outcome_corpus.to_json(),
    scored_at_ns=scoring_time,
)
report = ForecastScoreReportV1((score,))
path = write_forecast_artifact(report, artifact_directory)
assert read_forecast_artifact(path) == report
```

Outcome evidence must include exact snapshot inputs, preserve calendar revision
chains, and contain no vintage unavailable at scoring time. A later scoring
run may attach more revisions and obtain a new evidence identity, but its
first-release actual remains unchanged. Scores reconstruct metrics from
evidence instead of trusting serialized results. Restoration checks every
nested identity and rejects altered results, unknown fields and duplicate JSON
keys. Artifacts are limited to 32 MiB and 64 JSON nesting levels;
reports/distributions have 10,000-item bounds. The complete canonical UTF-8
envelope, including its identity field, consumes the byte budget. The envelope
root has depth zero; every contained object/array and scalar value consumes
another level. Python tuples count as the JSON arrays they serialize into.
JSON text fields such as model state, retained input calendars and score
`snapshot_json` are decoded into their canonical objects before the enclosing
artifact is checked; the report's score array also consumes depth. Ordinary
opaque string values are not recursively parsed as additional JSON.

Construction, sealed serialization and restoration enforce the same limits.
A valid child can be too large or deep for an enclosing model, snapshot, score
or report; that composition is rejected during construction, not first
discovered when reading its bytes. Accepted containers round-trip under the
same resource policy. Inputs/models also validate their own full envelope at
construction. Deep or cyclic Python values are refused without leaking a
recursive-encoder error. Readers verify content-addressed filenames and
canonical bytes.
Writers use exclusive creation and never overwrite differing existing bytes.

## Acceptance coverage and limitations

`test_forecast_contracts.py` and `test_forecast_scoring.py` use the uniquely named
offline `forecast_contracts_v1.py` fixture. They cover immutable nested metadata,
identity tampering, cutoff and schedule mismatch, unavailable inputs, exact
replay, fixed/final horizons, future-consensus exception, distribution validity,
first-actual/revision separation, both surprise definitions, delayed source
availability, ex-ante normalization, all required metric formulas, report task
separation and file corruption.

This layer does not download sources, train/select models, estimate consensus,
fit normalization scales, schedule operational forecasts, or assert financial
profitability. It freezes the observation/scoring unit needed by those later
components. Cryptographic hashes bind retained bytes; they cannot establish
that a source's asserted publication/availability clock or a producer's claimed
model execution was truthful.

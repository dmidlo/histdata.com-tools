# Uncertainty-preserving training views

Issue #609 adds a downstream view layer over retained native source and member
products. A view is not a new observed market history, an independent historical
sample, a fitted broker model or a qualification of a reconstruction engine.
The failed empirical study under #607 remains failed; its arithmetic contracts
do not establish calibrated confidence or justify a new empirical execution.

## Views and uncertainty axes

The six requested views have different, identity-bearing meanings:

| View | Meaning |
| --- | --- |
| `observed_only` | Actual source observations, without counting repeated member anchors as new evidence. |
| `central_counterfactual` | A frozen, explicit central scenario/member choice, not an ex-post best-performing path. |
| `member_panel` | Retained member paths with scenario coordinates still visible. |
| `sample_one_member_per_evidence_unit` | Exactly one semantically selected whole member per evidence unit for a specified epoch, with that member's named scenario coordinates attached. |
| `marginalized_features` | Frozen-weight summaries of declared, commensurate scalar features. |
| `uncertainty_features` | Dispersion together with denominator, support and refusal information. |

Observation retention, transition scenario, path realization, engine and
configuration, delivery profile, broker conditioning and population scenario
must not be conflated. Every result identifies preserved and collapsed axes.
An unavailable axis is not an observed central scenario. Optional provider or
trader-population sources without an admitted native provenance path refuse;
they are not silently merged into base history.

The native primary-member field does not by itself prove scientific centrality.
A central selection must be frozen explicitly and resolve to an actual admitted
member. Missing, ambiguous and contradictory selectors are failures, not reasons
to select whichever retained path happens to look favorable.

## Evidence and authority

The complete declared source inventory determines evidence-unit ownership and
membership. A projection does not authorize discarding an inconvenient parent,
redefining the denominator or bypassing source checks. Every public materializer
and retained-artifact reader replays its admitted native inputs.

An existing `TrainingRowV1.scenario_ids` collection contains generator and
constraint identities; those strings are not authoritative names for low,
central or high observation uncertainty. Named axes require their actual native
configuration, conditioning and runtime evidence.

There are two distinct native evidence scopes:

- The campaign path derives assignments from the actual complete index and
  independently verified native plan/runtime graph. It does not bypass campaign
  eligibility, publication or final-verification requirements.
- The research path retains a closed native source/conditioning/fit/generation
  recipe and reproduces its outputs against retained member products. It is
  explicitly conditional, unqualified research execution. It cannot manufacture
  a promotion report, become a qualified campaign or establish historical causal
  availability merely because the recipe is reproducible.

Synthetic native fixtures test these software distinctions. They do not certify
the complete historical training substrate or the downstream ML handoff.

## Scalar summaries are not average paths

Member event streams can have different event times and cardinalities. Position
17 in one stream need not correspond to position 17 in another. Event panels
retain native event order and identity rather than inventing that correspondence.

A scalar summary needs a shared, explicit half-open feature coordinate and the
same frozen feature definition. Timestamps, categorical states, source IDs and
unaligned paths are not numeric features to average. Quote or count reductions
retain their own units, support and empty-state semantics.

Weights conserve the declared evidence mass. Means and population variances use
the normalized frozen weights, and weighted quantiles use a declared deterministic
definition. Missing or refused members remain visible in the original inventory;
successful members are not silently reweighted into a complete ensemble.
Dispersion is descriptive counterfactual variation, not calibrated predictive
coverage, independent sample size or proof of historical truth.

Version `1.0.0` uses `equal-complete-members.v1`: only stochastic path
realizations may be collapsed, and only within the same evidence unit,
generator configuration and preserved scenario coordinates. Observation,
transition, engine, delivery, broker and population coordinates are never
numerically merged. A policy can also retain path realizations separately.
This is a descriptive equal-weight mixture, not the failed #607 study's
empirically calibrated weights. There is no automatic best-path selection.

Scalar coordinates intersect the request with each complete evidence unit and
use half-open `[start_ns, end_ns)` intervals and an explicit FX symbol. The
closed feature catalog contains `event_count`, `observed_count`,
`synthetic_count`, `bid_min`, `bid_mean`, `bid_max`, `ask_min`, `ask_mean`,
`ask_max` and `spread_mean`. Quote features use the native quote units;
`spread_mean` is the arithmetic mean of `ask - bid`. Means are event-weighted,
not elapsed-time-weighted. Counts include only events in the named member and
coordinate; repeated anchors across members do not become new observed evidence.
Binary64 prices are converted to exact rationals before reduction.

Quantiles use the left-continuous weighted inverse CDF: the first sorted value
whose cumulative mass reaches the requested probability, with probability zero
selecting the smallest positive-mass value. Population variance divides by
total mass, not a sample-size correction. Refused, unavailable or partially
supported members prevent a numerical summary; successful members are never
renormalized to erase them. A genuinely admitted empty interval has count zero
but no quote value.

## Whole-member selection

Central selection freezes exactly one actual `member_key` for every retained
native evidence unit. A known observation-retention coordinate must be
`central_fitted_retention`; other coordinates remain explicit. A motif baseline
with no observation uncertainty can have an explicit representative member but
does not thereby acquire scientific centrality.

Sampling selects exactly one member from each unit's complete named joint
roster, including any terminal refusal or empty outcome. It does not resample
until it finds success. The full roster, status denominator and selected keys
remain in the result, even for a narrower time/symbol projection. Selection is
not a numerical averaging of categorical axes: the chosen axes remain visible.

The additive v1 sampling law is SHA-256 minimum-score selection over the
canonical semantic seed, evidence-unit, group, epoch and member key under the
domain `histdatacom.training-scenario-sample.v1\n`. Inventory ordering and
worker scheduling do not enter the key. The view engine freezes the group key
as `joint-native-context-roster.v1`, not a caller-selected scenario stratum.
This is a separately versioned law,
not a claim of bit-for-bit compatibility with #607's sampler or the held #639
implementation. Existing source/ownership contracts may bind physical locators;
this interface does not claim arbitrary relocation preserves identity.

## Typed interface

The public import is `histdatacom.data_quality.training_scenarios`. Source and
ownership are the already retained #606 contracts, not caller assertion flags:

```python
from histdatacom.data_quality.training_contracts import TrainingConsumerMode
from histdatacom.data_quality.training_scenarios import (
    ScenarioViewKind,
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioRequestV1,
    materialize_training_scenario_view,
    read_training_scenario_artifact,
    write_training_scenario_artifact,
)

plan = TrainingScenarioPlanV1(source, ownership, TrainingScenarioPolicyV1())
request = TrainingScenarioRequestV1(
    ScenarioViewKind.OBSERVED_ONLY,
    TrainingConsumerMode.DESCRIPTIVE,
    start_ns, end_ns, ("EURUSD",),
)
view = materialize_training_scenario_view(plan, request)
path = write_training_scenario_artifact(view, output_directory)
replayed = read_training_scenario_artifact(
    path,
    consumer_mode=request.consumer_mode,
    expected_view_id=view.artifact_id,
)
```

Sources with reconstruction products require exactly one additional plan
binding: `TrainingScenarioCampaignBindingV1(index_path, index_id, index_sha256)`
or `TrainingScenarioResearchBindingV1(recipe_json, expected_recipe_id)`.
Campaign admission traverses the whole current native graph. Research admission
recomputes its closed native source/fit/generation recipe and compares actual
retained products. Caller fits, successful status tokens and verification
callbacks are not recipe inputs. Neither route grants broker permissions or
historical causal availability. Causal consumer mode, broker-derived V1
products and unsupported context/derived feature sources explicitly refuse,
including when an observed-only projection would omit those parents.

## Retained artifacts

The dedicated reader and writer in
`histdatacom.data_quality.training_scenario_artifacts` use canonical,
content-addressed files. Publication creates a new file without rewriting source
products. The writer replays the view before publication; the reader checks the
requested view identity and consumer mode and replays the whole native view.

Temporary files and self-consistent hashes are not successful execution evidence.
An expected identity selects the requested subject; it does not authenticate the
producer. Publication assumes a cooperative, quiescent local filesystem, not a
hostile process with access to replace paths or scientific input bytes.

## Bounded execution

The new contracts retain the existing 8 MiB canonical envelope limit with exact
types, bounded nesting and prospective expansion checks. Numerical processing
allows at most 128 members per unit, 1,024 feature coordinates, 4,096 scalar
cells, 16 quantiles and 8,192-bit rational components. Native admission limits
the declared product inventory to 32 and tracked files to 8,192; the underlying
source readers retain their own independent limits. These are software safety
bounds, not evidence of historical-scale performance or completeness.

The generated-source research adapter adds smaller source and output
reservations: a 128 KiB recipe, 24 source partitions, 4,096 observed source rows,
32 MiB source bytes, 32 crossed cells, and 16,384 reserved observed-plus-generated
output rows. Its full retained input inventory is limited to 512 files and
64 MiB. Generation spans at most one hour and must fit an actual evidence unit.
The view contract also bounds selected panel rows to 4,096. Its tests must execute
the real native pipeline; constructing a structurally valid view or resealing
its digest is not a successful oracle.

## Synthetic coverage and retained refusals

The generated research fixture crosses three observation-retention scenarios,
three transition scenarios and two path draws: 18 complete cells. Under its
fixed resource policy, native atomic uncertainty admission refuses all twelve
left/linear cells and admits six early-right members. The source, seeds and
resource limits are not adjusted to turn those refusals into passes. All 18
remain in the view denominator. The three admitted path-pair groups can expose
moments; the six refused groups expose reasons and unavailable summaries.
This proves software behavior on invented data, not historical qualification.

Research products retain `scientific_payload_sha256` under
`complete-canonical-native-replay-payload.v1`, committing the entire canonical
native scientific payload without exceeding the ordinary product metadata
limit. Replay recomputes that complete payload and compares actual event
streams plus native source, constraint, delivery quality and ensemble metadata.
A stored digest alone never substitutes for this execution.

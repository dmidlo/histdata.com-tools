"""Generated native fixture streams and a public example search registry.

No trained model, private search, protected source or empirical evidence.
"""

from dataclasses import replace

from histdatacom.experiments import (
    AttemptStatus,
    CandidateDeclarationV1,
    CandidateDispositionV1,
    CandidateOutcome,
    ComparisonKind,
    ComponentField,
    EnvironmentV1,
    EvidenceKind,
    ExperimentAttemptV1,
    ExperimentBundleV1,
    ExperimentComparisonV1,
    ExperimentLifecycleV1,
    ExperimentRegistryV1,
    ExperimentReportV1,
    ExperimentResultV1,
    ExperimentStatus,
    ExperimentSearchPlanV1,
    MetricDefinitionV1,
    MetricDirection,
    MetricPayloadV1,
    MetricRegistryV1,
    PromotionBindingV1,
    RandomnessV1,
    ReplayClass,
    ResearchCycleV1,
    ResourceEnvelopeV1,
    ResultReceiptV1,
    ResultState,
    ScientificComponentV1,
    ScientificInputsV1,
    fixture_specification,
    result_reference,
    retain_native_fixture,
    scientific_differences,
)
from histdatacom.experiments._wire import canonical_json
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)


def sorted_artifacts(items):
    return tuple(sorted(items, key=lambda item: item.artifact_id))


def native_fixture():
    event = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=10,
        event_sequence=0,
        bid=1.0,
        ask=1.1,
        run_id="synthetic-experiment-fixture",
        ensemble_member_id="fixture-0",
        source_version_id="fixture-not-historical-source",
        source_series_id="fixture",
        source_period="200001",
        source_row_id=1,
    )
    stream = SyntheticEventStreamV1(
        event.run_id, event.ensemble_member_id, event.symbol, (event,)
    )
    return retain_native_fixture(canonical_json(stream.to_dict()))


def metrics():
    return MetricRegistryV1(
        "1.0.0",
        (
            MetricDefinitionV1(
                "fixture_loss",
                "1.0.0",
                "sum(w_i * loss_i) / sum(w_i)",
                MetricDirection.MINIMIZE,
                "dimensionless",
                "one mass per fixture unit",
                "unavailable if no complete units",
                "mean over declared units",
                "none; synthetic reference only",
            ),
        ),
    )


def bundle_fixture(*, changes=None, cycle="cycle-1", protected=False):
    changes = changes or {}
    fixtures = []
    components = []
    native = native_fixture()
    fixtures.append(native)
    for field in sorted(ComponentField, key=lambda field: field.value):
        if (
            field is ComponentField.DATASET_PRODUCTS
            and field.value not in changes
        ):
            fixture = native
        else:
            fixture = fixture_specification(
                field.value, changes.get(field.value, "default")
            )
            fixtures.append(fixture)
        components.append(ScientificComponentV1(field, (fixture.reference,)))
    for name in ("fit_policy", "runtime", "lock", "sbom", "replay_policy"):
        fixture = fixture_specification(name, changes.get(name, "default"))
        fixtures.append(fixture)
    by_name = {
        name: next(
            f
            for f in fixtures
            if f.payload_json.startswith('{"name":"' + name + '"')
        )
        for name in ("fit_policy", "runtime", "lock", "sbom", "replay_policy")
    }
    inputs = ScientificInputsV1(
        changes.get("hypothesis_id", "fixture-hypothesis"),
        by_name["fit_policy"].reference,
        tuple(components),
        EnvironmentV1(
            changes.get("commit", "a" * 40),
            "b" * 40,
            by_name["runtime"].reference,
            by_name["lock"].reference,
            by_name["sbom"].reference,
        ),
        RandomnessV1(
            changes.get("namespace", "fixture-semantic-seeds"),
            changes.get("seeds", (7,)),
            ReplayClass.EXACT,
            by_name["replay_policy"].reference,
        ),
        metrics().artifact_id,
        ("fixture-all",),
        cycle,
        (
            (
                next(
                    c
                    for c in components
                    if c.field is ComponentField.DATASET_LINEAGE
                )
                .references[0]
                .native_id,
            )
            if protected
            else ()
        ),
        True,
        "experiment-search-plan:sha256:" + "0" * 64,
    )
    return ExperimentBundleV1(inputs, "Synthetic fixture", "dev"), tuple(
        fixtures
    )


def assemble(
    bundles,
    fixtures,
    *,
    outcomes=None,
    parents=None,
    cycle_times=None,
    predecessors=None,
):
    outcomes = outcomes or (CandidateOutcome.UNFAVORABLE,) * len(bundles)
    parents = parents or ((),) * len(bundles)
    cycle_times = cycle_times or {
        b.scientific_inputs.research_cycle_id: 0 for b in bundles
    }
    predecessors = predecessors or {}
    plans = {}
    for cycle_id in sorted(cycle_times, key=cycle_times.get):
        members = [
            (i, b)
            for i, b in enumerate(bundles)
            if b.scientific_inputs.research_cycle_id == cycle_id
        ]
        hashes = {
            ref.sha256
            for _, b in members
            for component in b.scientific_inputs.components
            if component.field is ComponentField.DATASET_LINEAGE
            for ref in component.references
            if ref.native_id in b.scientific_inputs.protected_evaluation_roots
        }
        plans[cycle_id] = ExperimentSearchPlanV1(
            cycle_id,
            cycle_times[cycle_id],
            tuple(
                sorted(
                    plans[p].artifact_id for p in predecessors.get(cycle_id, ())
                )
            ),
            tuple(sorted(hashes)),
            len(members),
            tuple(f"candidate-{i}" for i, _ in members),
        )
    bundles = tuple(
        replace(
            b,
            scientific_inputs=replace(
                b.scientific_inputs,
                search_plan_id=plans[
                    b.scientific_inputs.research_cycle_id
                ].artifact_id,
            ),
        )
        for b in bundles
    )
    attempts, results, receipts, dispositions, lifecycle = [], [], [], [], []
    for index, (bundle, outcome) in enumerate(zip(bundles, outcomes)):
        cycle_id = bundle.scientific_inputs.research_cycle_id
        base = cycle_times[cycle_id]
        planned = ExperimentLifecycleV1(
            bundle.experiment_id,
            None,
            ExperimentStatus.PLANNED,
            base,
            "declared synthetic plan",
        )
        lifecycle.append(planned)
        if outcome is CandidateOutcome.NOT_EXECUTED:
            dispositions.append(
                CandidateDispositionV1(
                    cycle_id,
                    bundle.experiment_id,
                    outcome,
                    (),
                    (),
                    "not executed synthetic candidate",
                )
            )
            continue
        start = ExperimentAttemptV1(
            bundle.experiment_id,
            bundle.artifact_id,
            "attempt-1",
            AttemptStatus.RUNNING,
            base + 10,
            None,
            bundle.scientific_inputs.environment.runtime,
            "synthetic-worker",
            ResourceEnvelopeV1(60, 1024 * 1024, 1),
            (),
            (),
            (),
            None,
            None,
        )
        failed = outcome is CandidateOutcome.FAILED
        result = ExperimentResultV1(
            bundle.experiment_id,
            metrics().metrics[0].artifact_id,
            "fixture-all",
            MetricPayloadV1(
                ResultState.AVAILABLE, 1.0 + index, 1, None, None, None
            ),
        )
        ref = result_reference(result, EvidenceKind.SYNTHETIC_FIXTURE)
        end = replace(
            start,
            status=AttemptStatus.FAILED if failed else AttemptStatus.COMPLETED,
            ended_at_ns=base + 20,
            produced_artifacts=() if failed else (ref,),
            reason="synthetic failure" if failed else None,
            previous_state_id=start.artifact_id,
        )
        attempts.extend((start, end))
        result_ids = ()
        if not failed:
            results.append(result)
            receipts.append(
                ResultReceiptV1(result.result_id, end.artifact_id, ref)
            )
            result_ids = (result.result_id,)
        dispositions.append(
            CandidateDispositionV1(
                cycle_id,
                bundle.experiment_id,
                outcome,
                tuple(sorted((start.artifact_id, end.artifact_id))),
                result_ids,
                "complete declared fixture outcome; no merit inference",
            )
        )
        running = ExperimentLifecycleV1(
            bundle.experiment_id,
            planned.artifact_id,
            ExperimentStatus.RUNNING,
            base + 11,
            "started fixture",
        )
        terminal = ExperimentLifecycleV1(
            bundle.experiment_id,
            running.artifact_id,
            ExperimentStatus.FAILED if failed else ExperimentStatus.COMPLETED,
            base + 21,
            "terminated fixture",
        )
        lifecycle.extend((running, terminal))
    cycles = []
    for cycle_id, timestamp in cycle_times.items():
        members = [
            (i, b)
            for i, b in enumerate(bundles)
            if b.scientific_inputs.research_cycle_id == cycle_id
        ]
        cycles.append(
            ResearchCycleV1(
                cycle_id,
                timestamp,
                tuple(sorted(predecessors.get(cycle_id, ()))),
                tuple(
                    sorted(
                        {
                            root
                            for _, b in members
                            for root in b.scientific_inputs.protected_evaluation_roots
                        }
                    )
                ),
                len(members),
                tuple(
                    CandidateDeclarationV1(
                        f"candidate-{i}",
                        b.experiment_id,
                        tuple(
                            sorted(bundles[p].experiment_id for p in parents[i])
                        ),
                    )
                    for i, b in members
                ),
                plans[cycle_id],
            )
        )
    retained = {f.artifact_id: f for f in fixtures}
    return ExperimentRegistryV1(
        sorted_artifacts(bundles),
        (metrics(),),
        sorted_artifacts(retained.values()),
        sorted_artifacts(attempts),
        sorted_artifacts(results),
        sorted_artifacts(receipts),
        sorted_artifacts(cycles),
        sorted_artifacts(dispositions),
        (),
        sorted_artifacts(lifecycle),
        (),
        (),
        (),
    )


def example_registry():
    a, fa = bundle_fixture()
    b, fb = bundle_fixture(
        changes={
            "feature_projection": "with-strategy",
            "model_weights": "model-with-strategy",
        }
    )
    c, fc = bundle_fixture(changes={"model_configuration": "failed-candidate"})
    registry = assemble(
        (a, b, c),
        fa + fb + fc,
        outcomes=(
            CandidateOutcome.UNFAVORABLE,
            CandidateOutcome.FAVORABLE,
            CandidateOutcome.FAILED,
        ),
        parents=((), (0,), (0,)),
    )
    a, b = rebound(registry, a), rebound(registry, b)
    results = {r.experiment_id: r for r in registry.results}
    comparison = ExperimentComparisonV1(
        a.experiment_id,
        b.experiment_id,
        ComparisonKind.CHAMPION_CHALLENGER,
        scientific_differences(a, b),
        results[a.experiment_id].result_id,
        results[b.experiment_id].result_id,
    )
    deployment = fixture_specification("deployment", "not-an-operation")
    policy = fixture_specification("promotion-policy", "fixture-only")
    promotion = PromotionBindingV1(
        deployment.reference,
        policy.reference,
        tuple(sorted((a.experiment_id, b.experiment_id))),
        tuple(sorted(r.result_id for r in registry.results)),
        (comparison.artifact_id,),
        30,
        ("synthetic fixture only; no scientific qualification",),
    )
    report = ExperimentReportV1(
        "Fixture result table",
        promotion.qualifying_result_ids,
        (comparison.artifact_id,),
    )
    return replace(
        registry,
        fixtures=sorted_artifacts((*registry.fixtures, deployment, policy)),
        comparisons=(comparison,),
        promotions=(promotion,),
        reports=(report,),
    )


def rebound(registry, bundle):
    original = bundle.scientific_inputs.to_payload()
    original.pop("search_plan_id")
    for candidate in registry.bundles:
        inputs = candidate.scientific_inputs.to_payload()
        inputs.pop("search_plan_id")
        if inputs == original:
            return candidate
    raise ValueError("fixture bundle not found after plan binding")

"""Actual closed synthetic search/result/report/promotion and negative controls."""

from dataclasses import replace

import pytest

from histdatacom.experiments import (
    AttemptStatus,
    CandidateOutcome,
    ExperimentLifecycleV1,
    ExperimentRegistryV1,
    ExperimentStatus,
    ProtectedExposureV1,
    assert_promotion_admissible,
    render_report_markdown,
    resolve_report,
    reverse_dependencies,
)
from histdatacom.experiments.lineage import RegistryView
from tests.fixtures.experiment_bundles import (
    assemble,
    bundle_fixture,
    example_registry,
    sorted_artifacts,
    rebound,
)


@pytest.fixture(scope="module")
def registry():
    return example_registry()


def test_actual_native_registry_report_and_promotion(registry):
    assert ExperimentRegistryV1.from_json(registry.to_json()) == registry
    assert {d.outcome for d in registry.dispositions} == {
        CandidateOutcome.UNFAVORABLE,
        CandidateOutcome.FAVORABLE,
        CandidateOutcome.FAILED,
    }
    report = resolve_report(registry, registry.reports[0])
    assert len(report.rows) == 2
    assert {r.value for r in report.rows} == {1.0, 2.0}
    assert report.comparisons[0].right_minus_left == 1.0
    rendered = render_report_markdown(registry, registry.reports[0])
    assert all(row.result_id in rendered for row in report.rows)
    assert "no empirical qualification" in rendered
    assert_promotion_admissible(registry, registry.promotions[0])


@pytest.mark.parametrize(
    "field",
    [
        "results",
        "result_receipts",
        "attempts",
        "dispositions",
        "cycles",
        "lifecycle",
        "fixtures",
    ],
)
def test_missing_retained_inventory_refuses(registry, field):
    with pytest.raises(ValueError):
        replace(registry, **{field: getattr(registry, field)[1:]})


def test_resealed_result_and_manual_report_values_refuse(registry):
    result = registry.results[0]
    changed = replace(result, payload=replace(result.payload, value=999.0))
    with pytest.raises(ValueError):
        replace(
            registry, results=sorted_artifacts((changed, *registry.results[1:]))
        )
    wire = registry.reports[0].to_dict()
    wire["payload"]["manually_copied_value"] = 999.0
    with pytest.raises(ValueError, match="field"):
        type(registry.reports[0]).from_dict(wire)


def test_undeclared_difference_and_mismatched_metric_result_refuse(registry):
    comparison = registry.comparisons[0]
    with pytest.raises(ValueError, match="differences"):
        replace(
            registry,
            comparisons=(
                replace(
                    comparison,
                    declared_differences=("components.feature_projection",),
                ),
            ),
        )
    with pytest.raises(ValueError, match="shared metric|ownership"):
        replace(
            registry,
            comparisons=(
                replace(
                    comparison,
                    left_result_id=comparison.right_result_id,
                    right_result_id=comparison.left_result_id,
                ),
            ),
        )


def test_complete_declared_search_catches_omitted_failed_candidate(registry):
    failed = next(
        d for d in registry.dispositions if d.outcome is CandidateOutcome.FAILED
    )
    with pytest.raises(ValueError):
        replace(
            registry,
            dispositions=tuple(d for d in registry.dispositions if d != failed),
        )
    cycle = registry.cycles[0]
    with pytest.raises(ValueError):
        replace(
            registry,
            cycles=(
                replace(
                    cycle,
                    candidates=tuple(
                        c
                        for c in cycle.candidates
                        if c.experiment_id != failed.experiment_id
                    ),
                ),
            ),
        )


def test_diamond_ancestry_and_cycle_refusal():
    pairs = [
        bundle_fixture(changes={"model_configuration": str(i)})
        for i in range(4)
    ]
    registry = assemble(
        tuple(b for b, _ in pairs),
        tuple(f for _, fs in pairs for f in fs),
        parents=((), (0,), (0,), (1, 2)),
    )
    root = rebound(registry, pairs[0][0]).experiment_id
    descendant = rebound(registry, pairs[3][0]).experiment_id
    assert descendant in reverse_dependencies(registry, root)
    cycle = registry.cycles[0]
    candidate = cycle.candidates[0]
    cyclic = replace(candidate, parent_experiment_ids=(descendant,))
    with pytest.raises(ValueError, match="cyclic"):
        replace(
            registry,
            cycles=(
                replace(cycle, candidates=(cyclic, *cycle.candidates[1:])),
            ),
        )


def test_invalidation_is_append_only_and_reverse_discoverable(registry):
    experiment = registry.promotions[0].qualifying_experiment_ids[0]
    old = next(
        e
        for e in registry.lifecycle
        if e.experiment_id == experiment
        and e.status is ExperimentStatus.COMPLETED
    )
    invalid = ExperimentLifecycleV1(
        experiment,
        old.artifact_id,
        ExperimentStatus.INVALIDATED,
        40,
        "fixture leakage discovered",
    )
    updated = replace(
        registry, lifecycle=sorted_artifacts((*registry.lifecycle, invalid))
    )
    assert registry.results == updated.results
    assert registry.to_json() != updated.to_json()
    assert updated.promotions == registry.promotions
    with pytest.raises(ValueError, match="valid|invalidated"):
        assert_promotion_admissible(updated, updated.promotions[0])
    dependents = reverse_dependencies(updated, experiment)
    assert updated.reports[0].artifact_id in dependents
    assert updated.promotions[0].artifact_id in dependents
    assert any(
        row.current_status is ExperimentStatus.INVALIDATED
        for row in resolve_report(updated, updated.reports[0]).rows
    )
    # Old retained snapshot remains valid; later invalidation is not a rewrite.
    assert_promotion_admissible(registry, registry.promotions[0])


def test_invalidation_before_promotion_refuses(registry):
    experiment = registry.promotions[0].qualifying_experiment_ids[0]
    old = next(
        e
        for e in registry.lifecycle
        if e.experiment_id == experiment
        and e.status is ExperimentStatus.COMPLETED
    )
    invalid = ExperimentLifecycleV1(
        experiment,
        old.artifact_id,
        ExperimentStatus.INVALIDATED,
        25,
        "defect known before promotion",
    )
    with pytest.raises(ValueError, match="valid|invalidated"):
        replace(
            registry, lifecycle=sorted_artifacts((*registry.lifecycle, invalid))
        )


def test_usable_running_failed_retry_completed_sequence():
    from histdatacom.experiments import (
        EvidenceKind,
        ExperimentResultV1,
        MetricPayloadV1,
        ResultReceiptV1,
        ResultState,
        result_reference,
    )
    from tests.fixtures.experiment_bundles import metrics

    bundle, fixtures = bundle_fixture()
    registry = assemble(
        (bundle,), fixtures, outcomes=(CandidateOutcome.FAILED,)
    )
    bundle = rebound(registry, bundle)
    failed = next(
        a for a in registry.attempts if a.status is AttemptStatus.FAILED
    )
    retry = replace(
        failed,
        attempt_label="attempt-2",
        status=AttemptStatus.RUNNING,
        started_at_ns=30,
        ended_at_ns=None,
        reason=None,
        previous_state_id=None,
        retry_of_attempt_id=failed.artifact_id,
        retry_reason="exact retry after fixture failure",
    )
    assert retry.attempt_id != failed.attempt_id
    result = ExperimentResultV1(
        bundle.experiment_id,
        metrics().metrics[0].artifact_id,
        "fixture-all",
        MetricPayloadV1(ResultState.AVAILABLE, 1.0, 1, None, None, None),
    )
    reference = result_reference(result, EvidenceKind.SYNTHETIC_FIXTURE)
    terminal = replace(
        retry,
        status=AttemptStatus.COMPLETED,
        ended_at_ns=40,
        reason=None,
        produced_artifacts=(reference,),
        previous_state_id=retry.artifact_id,
    )
    assert retry.attempt_id == terminal.attempt_id
    assert retry.artifact_id != terminal.artifact_id
    d = registry.dispositions[0]
    previous = next(
        e for e in registry.lifecycle if e.status is ExperimentStatus.FAILED
    )
    running = ExperimentLifecycleV1(
        bundle.experiment_id,
        previous.artifact_id,
        ExperimentStatus.RUNNING,
        31,
        "retry started",
    )
    end = ExperimentLifecycleV1(
        bundle.experiment_id,
        running.artifact_id,
        ExperimentStatus.COMPLETED,
        41,
        "retry completed",
    )
    updated = replace(
        registry,
        attempts=sorted_artifacts((*registry.attempts, retry, terminal)),
        results=(result,),
        result_receipts=(
            ResultReceiptV1(result.result_id, terminal.artifact_id, reference),
        ),
        dispositions=(
            replace(
                d,
                outcome=CandidateOutcome.UNFAVORABLE,
                result_ids=(result.result_id,),
                attempt_ids=tuple(
                    sorted(
                        (
                            *d.attempt_ids,
                            retry.artifact_id,
                            terminal.artifact_id,
                        )
                    )
                ),
            ),
        ),
        lifecycle=sorted_artifacts((*registry.lifecycle, running, end)),
    )
    assert ExperimentRegistryV1.from_json(updated.to_json()) == updated
    changed = replace(
        retry, runtime_identity=replace(retry.runtime_identity, sha256="f" * 64)
    )
    with pytest.raises(ValueError):
        replace(
            updated,
            attempts=sorted_artifacts((*registry.attempts, changed, terminal)),
        )


def protected_registry(*, same_cycle=False, reuse=False, relabel=False):
    a, fa = bundle_fixture(protected=True)
    changes = {"model_configuration": "redesigned"}
    if not reuse:
        changes.update(
            dataset_lineage="new-root",
            split_artifact="new-split",
            dataset_products="new-products",
        )
    b, fb = bundle_fixture(
        changes=changes,
        cycle="cycle-1" if same_cycle else "cycle-2",
        protected=True,
    )
    if relabel:
        # Identical protected bytes with a different external identity label.
        from histdatacom.experiments import ComponentField, EvidenceKind

        parts = []
        for component in b.scientific_inputs.components:
            if component.field is ComponentField.DATASET_LINEAGE:
                ref = replace(
                    component.references[0],
                    native_id="renamed-protected-root",
                    evidence_kind=EvidenceKind.DECLARED_EXTERNAL,
                )
                component = replace(component, references=(ref,))
            parts.append(component)
        b = replace(
            b,
            scientific_inputs=replace(
                b.scientific_inputs,
                components=tuple(parts),
                protected_evaluation_roots=("renamed-protected-root",),
            ),
        )
        fb = tuple(
            f
            for f in fb
            if f.reference.native_id
            != a.scientific_inputs.protected_evaluation_roots[0]
        )
    base = assemble(
        (a, b),
        fa + fb,
        parents=((), (0,)),
        cycle_times=(
            {"cycle-1": 0} if same_cycle else {"cycle-1": 0, "cycle-2": 100}
        ),
        predecessors={} if same_cycle else {"cycle-2": ("cycle-1",)},
    )
    a, b = rebound(base, a), rebound(base, b)
    result = next(r for r in base.results if r.experiment_id == a.experiment_id)
    exposure = ProtectedExposureV1(
        a.experiment_id,
        a.scientific_inputs.protected_evaluation_roots[0],
        22,
        (result.result_id,),
        (b.experiment_id,),
    )
    return base, exposure


def test_successor_cycle_with_new_content_and_exact_exposure():
    registry, exposure = protected_registry()
    updated = replace(registry, exposures=(exposure,))
    assert ExperimentRegistryV1.from_json(updated.to_json()) == updated


@pytest.mark.parametrize(
    "kwargs",
    [{"same_cycle": True}, {"reuse": True}, {"reuse": True, "relabel": True}],
)
def test_protected_reuse_or_redesign_without_successor_refuses(kwargs):
    registry, exposure = protected_registry(**kwargs)
    with pytest.raises(ValueError, match="protected|successor"):
        replace(registry, exposures=(exposure,))


def test_typed_empty_registry_and_no_model_only_promotion(registry):
    empty = ExperimentRegistryV1(*((),) * 13)
    assert ExperimentRegistryV1.from_json(empty.to_json()) == empty
    with pytest.raises(ValueError):
        replace(registry.promotions[0], qualifying_result_ids=())
    view = RegistryView(registry)
    view.validate()
    assert view.status(registry.bundles[0].experiment_id, at_ns=-1) is None


def test_running_lifecycle_cannot_use_obsolete_start_receipt(registry):
    bundle = registry.bundles[0]
    running = next(
        e
        for e in registry.lifecycle
        if e.experiment_id == bundle.experiment_id
        and e.status is ExperimentStatus.RUNNING
    )
    terminal = next(
        e
        for e in registry.lifecycle
        if e.previous_event_id == running.artifact_id
    )
    delayed = replace(running, recorded_at_ns=22)
    late_terminal = replace(
        terminal, recorded_at_ns=23, previous_event_id=delayed.artifact_id
    )
    events = [e for e in registry.lifecycle if e not in (running, terminal)]
    with pytest.raises(ValueError, match="execution evidence"):
        replace(
            registry,
            lifecycle=sorted_artifacts((*events, delayed, late_terminal)),
        )


def test_future_result_cannot_backfill_completed_lifecycle(registry):
    receipt = registry.result_receipts[0]
    producer = next(
        a for a in registry.attempts if a.artifact_id == receipt.attempt_id
    )
    late = replace(producer, ended_at_ns=70)
    early_empty = replace(
        producer,
        attempt_label="early-empty",
        produced_artifacts=(),
        previous_state_id=None,
    )
    disposition = next(
        d
        for d in registry.dispositions
        if d.experiment_id == producer.experiment_id
    )
    amended = replace(
        disposition,
        attempt_ids=tuple(
            sorted(
                (set(disposition.attempt_ids) - {producer.artifact_id})
                | {late.artifact_id, early_empty.artifact_id}
            )
        ),
    )
    with pytest.raises(ValueError, match="as of completion"):
        replace(
            registry,
            attempts=sorted_artifacts(
                tuple(a for a in registry.attempts if a != producer)
                + (late, early_empty)
            ),
            result_receipts=sorted_artifacts(
                tuple(r for r in registry.result_receipts if r != receipt)
                + (replace(receipt, attempt_id=late.artifact_id),)
            ),
            dispositions=sorted_artifacts(
                tuple(d for d in registry.dispositions if d != disposition)
                + (amended,)
            ),
        )


def test_promotion_clock_must_follow_exact_producer_even_if_other_result_completed(
    registry,
):
    # The helper's exact result availability query is the same guard used by
    # completed lifecycle AND promotion; no unrelated completed attempt counts.
    view = RegistryView(registry)
    view.validate()
    result = registry.results[0]
    assert not view.result_available_at(result.result_id, 19)
    assert view.result_available_at(result.result_id, 20)
    with pytest.raises(ValueError, match="producer"):
        view.assert_promotion(
            replace(registry.promotions[0], recorded_at_ns=19)
        )


def test_same_label_narrowed_search_plan_cannot_rebind_science(registry):
    cycle = registry.cycles[0]
    narrower = replace(
        cycle.search_plan,
        candidate_keys=cycle.search_plan.candidate_keys[:-1],
        search_budget=cycle.search_plan.search_budget - 1,
    )
    assert narrower.cycle_id == cycle.cycle_id
    assert narrower.artifact_id != cycle.search_plan.artifact_id
    changed = replace(
        cycle,
        candidates=cycle.candidates[:-1],
        search_budget=narrower.search_budget,
        search_plan=narrower,
    )
    with pytest.raises(ValueError, match="frozen search plan"):
        replace(registry, cycles=(changed,))
    # Original key inventory also rejects direct missing-candidate construction.
    with pytest.raises(ValueError, match="frozen search plan"):
        replace(cycle, candidates=cycle.candidates[:-1])


def test_later_exact_retry_does_not_invalidate_historical_promotion(registry):
    producer = next(
        a for a in registry.attempts if a.status is AttemptStatus.COMPLETED
    )
    retry = replace(
        producer,
        attempt_label="later-exact-retry",
        status=AttemptStatus.RUNNING,
        started_at_ns=50,
        ended_at_ns=None,
        reason=None,
        produced_artifacts=(),
        retry_of_attempt_id=producer.artifact_id,
        retry_reason="exact synthetic replay",
        previous_state_id=None,
    )
    end = replace(
        retry,
        status=AttemptStatus.COMPLETED,
        ended_at_ns=60,
        produced_artifacts=producer.produced_artifacts,
        previous_state_id=retry.artifact_id,
    )
    old_receipt = next(
        r
        for r in registry.result_receipts
        if r.attempt_id == producer.artifact_id
    )
    receipt = replace(old_receipt, attempt_id=end.artifact_id)
    disposition = next(
        d
        for d in registry.dispositions
        if d.experiment_id == producer.experiment_id
    )
    updated = replace(
        registry,
        attempts=sorted_artifacts((*registry.attempts, retry, end)),
        result_receipts=sorted_artifacts((*registry.result_receipts, receipt)),
        dispositions=sorted_artifacts(
            tuple(d for d in registry.dispositions if d != disposition)
            + (
                replace(
                    disposition,
                    attempt_ids=tuple(
                        sorted(
                            (
                                *disposition.attempt_ids,
                                retry.artifact_id,
                                end.artifact_id,
                            )
                        )
                    ),
                ),
            )
        ),
    )
    assert updated.promotions == registry.promotions
    assert_promotion_admissible(updated, updated.promotions[0])
    assert ExperimentRegistryV1.from_json(updated.to_json()) == updated


@pytest.mark.parametrize("field", ["deployment", "promotion_policy"])
def test_separately_supplied_promotion_cannot_bypass_reference_validation(
    registry, field
):
    promotion = registry.promotions[0]
    forged = replace(
        promotion,
        **{
            field: replace(
                getattr(promotion, field),
                native_id="unretained-synthetic-" + field,
                sha256="f" * 64,
            )
        },
    )
    with pytest.raises(ValueError, match="retained"):
        assert_promotion_admissible(registry, forged)


def test_supersession_and_later_invalidation_preserve_evidence(registry):
    cycle = registry.cycles[0]
    child = next(
        c
        for c in cycle.candidates
        if c.parent_experiment_ids
        and next(
            d
            for d in registry.dispositions
            if d.experiment_id == c.experiment_id
        ).outcome
        is not CandidateOutcome.FAILED
    )
    parent = child.parent_experiment_ids[0]
    terminal = next(
        e
        for e in registry.lifecycle
        if e.experiment_id == parent and e.status is ExperimentStatus.COMPLETED
    )
    superseded = ExperimentLifecycleV1(
        parent,
        terminal.artifact_id,
        ExperimentStatus.SUPERSEDED,
        40,
        "superseded by declared child",
        child.experiment_id,
    )
    updated = replace(
        registry, lifecycle=sorted_artifacts((*registry.lifecycle, superseded))
    )
    invalidated = ExperimentLifecycleV1(
        parent,
        superseded.artifact_id,
        ExperimentStatus.INVALIDATED,
        50,
        "later defect audit",
        child.experiment_id,
    )
    final = replace(
        updated, lifecycle=sorted_artifacts((*updated.lifecycle, invalidated))
    )
    assert registry.results == updated.results == final.results
    assert registry.promotions[0].artifact_id in reverse_dependencies(
        final, parent
    )
    with pytest.raises(ValueError):
        assert_promotion_admissible(final, final.promotions[0])


def test_other_produced_synthetic_outputs_require_exact_retained_bytes(
    registry,
):
    from histdatacom.experiments import (
        ArtifactReferenceV1,
        EvidenceKind,
        fixture_specification,
    )

    producer = next(
        a for a in registry.attempts if a.status is AttemptStatus.COMPLETED
    )
    old_receipt = next(
        r
        for r in registry.result_receipts
        if r.attempt_id == producer.artifact_id
    )
    disposition = next(
        d
        for d in registry.dispositions
        if d.experiment_id == producer.experiment_id
    )

    def with_output(reference, fixtures):
        output = replace(
            producer,
            produced_artifacts=tuple(
                sorted(
                    (*producer.produced_artifacts, reference),
                    key=lambda ref: __import__("json").dumps(
                        ref.to_payload(), sort_keys=True, separators=(",", ":")
                    ),
                )
            ),
        )
        return replace(
            registry,
            fixtures=sorted_artifacts(fixtures),
            attempts=sorted_artifacts(
                tuple(a for a in registry.attempts if a != producer) + (output,)
            ),
            result_receipts=sorted_artifacts(
                tuple(r for r in registry.result_receipts if r != old_receipt)
                + (replace(old_receipt, attempt_id=output.artifact_id),)
            ),
            dispositions=sorted_artifacts(
                tuple(d for d in registry.dispositions if d != disposition)
                + (
                    replace(
                        disposition,
                        attempt_ids=tuple(
                            sorted(
                                (
                                    set(disposition.attempt_ids)
                                    - {producer.artifact_id}
                                )
                                | {output.artifact_id}
                            )
                        ),
                    ),
                )
            ),
        )

    missing = ArtifactReferenceV1(
        "fixture:missing",
        "unretained-produced-fixture",
        "f" * 64,
        20,
        EvidenceKind.SYNTHETIC_FIXTURE,
    )
    with pytest.raises(ValueError, match="reference"):
        with_output(missing, registry.fixtures)
    fixture = fixture_specification("extra-output", "retained, not empirical")
    positive = with_output(fixture.reference, (*registry.fixtures, fixture))
    assert ExperimentRegistryV1.from_json(positive.to_json()) == positive

"""Exact finite coalition reference; no arbitrary callback is execution proof."""

from __future__ import annotations

from fractions import Fraction
from itertools import combinations
from math import factorial, isfinite
from typing import TYPE_CHECKING, TypedDict

from .contracts import (
    AttributionAmbiguityV1,
    AttributionBackgroundV1,
    AttributionEvidenceKind,
    AttributionGroupV1,
    AttributionSnapshotV1,
    AttributionState,
    ContributionV1,
    InteractionContributionV1,
    DecisionAttributionV1,
    ExplanationPolicyV1,
    ExplainedOutputV1,
    GroupMethod,
    ReferenceModelV1,
    reference,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class _Computed(TypedDict):
    state: AttributionState
    raw_output: float | None
    baseline_output: float | None
    contributions: tuple[ContributionV1, ...]
    groups: tuple[ContributionV1, ...]
    interactions: tuple[InteractionContributionV1, ...]
    ambiguity: tuple[AttributionAmbiguityV1, ...]
    additive_residual: float | None
    group_additive_residual: float | None
    effective_feature_dimension: float | None
    evaluations: int
    reason: str
    explained_output: ExplainedOutputV1


def _float(value: Fraction) -> float:
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError("unrepresentable reference diagnostic") from exc
    if not isfinite(result) or (value and not result):
        raise ValueError("nonfinite/underflowed reference diagnostic")
    return result


def validate_groups(
    groups: tuple[AttributionGroupV1, ...], cut: tuple[str, ...]
) -> None:
    by_name = {group.name: group for group in groups}
    if len(by_name) != len(groups) or not set(cut) <= by_name.keys():
        raise ValueError("unknown/duplicate reporting group")
    for group in groups:
        seen = {group.name}
        parent = group.parent
        while parent is not None:
            if parent not in by_name or parent in seen:
                raise ValueError("missing/cyclic group ancestor")
            if not set(group.members) <= set(by_name[parent].members):
                raise ValueError("child group escapes parent ownership")
            seen.add(parent)
            parent = by_name[parent].parent
    members: list[str] = []
    for name in cut:
        members.extend(by_name[name].members)
    if len(set(members)) != len(members):
        raise ValueError(
            "reporting cut double counts ancestor/child or shared feature"
        )
    if set(members) != {name for group in groups for name in group.members}:
        raise ValueError("reporting cut omits a feature")


def evaluate_reference_model(
    model: ReferenceModelV1, values: Sequence[float]
) -> float:
    if len(values) != len(model.features) or any(
        type(v) is not float or not isfinite(v) for v in values
    ):
        raise ValueError("model requires exact finite float coordinates")
    return _float(_evaluate(model, tuple(Fraction(v) for v in values)))


def _evaluate(
    model: ReferenceModelV1, values: tuple[Fraction, ...]
) -> Fraction:
    coordinates = dict(zip(model.features, values))
    total = Fraction()
    for term in model.terms:
        value = Fraction(term.coefficient)
        for name in term.features:
            value *= coordinates[name]
        total += value
    # Native finite floats have bounded bit length; also bound aggregate work.
    if (
        max(total.numerator.bit_length(), total.denominator.bit_length())
        > 32768
    ):
        raise ValueError("reference exact arithmetic bound")
    return total


def _coalitions(
    model: ReferenceModelV1,
    point: tuple[Fraction, ...],
    base: tuple[Fraction, ...],
    blocks: tuple[tuple[int, ...], ...],
) -> tuple[Fraction, ...]:
    values = []
    for mask in range(1 << len(blocks)):
        sample = list(base)
        for index, block in enumerate(blocks):
            if mask & (1 << index):
                for feature in block:
                    sample[feature] = point[feature]
        values.append(_evaluate(model, tuple(sample)))
    return tuple(values)


def _shapley(coalitions: tuple[Fraction, ...], n: int) -> tuple[Fraction, ...]:
    result = []
    for i in range(n):
        value = Fraction()
        for mask in range(1 << n):
            if not mask & (1 << i):
                size = mask.bit_count()
                weight = Fraction(
                    factorial(size) * factorial(n - size - 1), factorial(n)
                )
                value += weight * (
                    coalitions[mask | (1 << i)] - coalitions[mask]
                )
        result.append(value)
    return tuple(result)


def _reason(
    model: ReferenceModelV1,
    policy: ExplanationPolicyV1,
    snapshot: AttributionSnapshotV1,
    background: AttributionBackgroundV1,
) -> str | None:
    if policy.method != "exact-polynomial-coalitions.v1":
        return "method_has_no_public_executable_reference"
    if tuple(v.feature.name for v in snapshot.values) != model.features:
        return "model_projection_coordinates_differ"
    if set(model.features) != {
        name for group in policy.groups for name in group.members
    }:
        return "group_members_differ_from_model_projection"
    if background.role != "synthetic_fixture":
        return "background_not_admitted_synthetic_reference"
    if any(
        v.value is None
        or v.available_at_ns is None
        or v.available_at_ns > snapshot.cutoff_at_ns
        for v in snapshot.values
    ):
        return "target_snapshot_unavailable_at_cutoff"
    for base in background.snapshots:
        if (
            tuple(v.feature for v in base.values)
            != tuple(v.feature for v in snapshot.values)
            or base.space is not snapshot.space
            or base.preprocessing != snapshot.preprocessing
            or base.projection != snapshot.projection
            or base.universe != snapshot.universe
            or base.session != snapshot.session
            or base.domain != snapshot.domain
        ):
            return "background_semantic_domain_mismatch"
        if base.cutoff_at_ns > min(
            snapshot.cutoff_at_ns, background.declared_at_ns
        ):
            return "future_background_reference"
        if any(
            v.value is None
            or v.available_at_ns is None
            or v.available_at_ns > base.cutoff_at_ns
            for v in base.values
        ):
            return "background_unavailable_at_declared_cutoff"
    return None


def _computed(
    model: ReferenceModelV1,
    policy: ExplanationPolicyV1,
    snapshot: AttributionSnapshotV1,
    background: AttributionBackgroundV1,
) -> _Computed:
    output = ExplainedOutputV1(
        model.output_dimension,
        model.output_unit,
        model.output_class,
        model.horizon_ns,
    )
    reason = _reason(model, policy, snapshot, background)
    if reason is not None:
        return _Computed(
            state=AttributionState.UNSUPPORTED,
            raw_output=None,
            baseline_output=None,
            contributions=(),
            groups=(),
            interactions=(),
            ambiguity=(),
            additive_residual=None,
            group_additive_residual=None,
            effective_feature_dimension=None,
            evaluations=0,
            reason=reason,
            explained_output=output,
        )
    n, count = len(model.features), len(background.snapshots)
    by_group = {g.name: g for g in policy.groups}
    blocks = tuple(
        tuple(model.features.index(name) for name in by_group[group].members)
        for group in policy.reporting_cut
    )
    evaluations = count * (1 + n) * (1 << n)
    if policy.group_method is GroupMethod.DIRECT:
        evaluations += count * (1 << len(blocks))
    if evaluations > policy.maximum_evaluations:
        return _Computed(
            state=AttributionState.UNSUPPORTED,
            raw_output=None,
            baseline_output=None,
            contributions=(),
            groups=(),
            interactions=(),
            ambiguity=(),
            additive_residual=None,
            group_additive_residual=None,
            effective_feature_dimension=None,
            evaluations=0,
            reason="preflight_explanation_work_budget",
            explained_output=output,
        )
    point = tuple(
        Fraction(v.value) for v in snapshot.values if v.value is not None
    )
    singleton_blocks = tuple((i,) for i in range(n))
    all_phi: list[tuple[Fraction, ...]] = []
    all_group: list[tuple[Fraction, ...]] = []
    base_outputs = []
    pair_dividends: dict[tuple[int, int], Fraction] = {
        pair: Fraction() for pair in combinations(range(n), 2)
    }
    sibling = [Fraction() for _ in range(n)]
    for baseline in background.snapshots:
        base = tuple(
            Fraction(v.value) for v in baseline.values if v.value is not None
        )
        game = _coalitions(model, point, base, singleton_blocks)
        phi = _shapley(game, n)
        all_phi.append(phi)
        base_outputs.append(game[0])
        for pair in pair_dividends:
            i, j = pair
            # Exact pair dividend at the empty coalition; NOT a full SHAP
            # interaction matrix and NOT added again to the Shapley sum.
            pair_dividends[pair] += (
                game[(1 << i) | (1 << j)]
                - game[1 << i]
                - game[1 << j]
                + game[0]
            ) / count
        for j in range(n):
            removed = list(point)
            removed[j] = base[j]
            without = _shapley(
                _coalitions(model, tuple(removed), base, singleton_blocks), n
            )
            for i in range(n):
                if i != j:
                    sibling[i] = max(sibling[i], abs(without[i] - phi[i]))
        if policy.group_method is GroupMethod.DIRECT:
            all_group.append(
                _shapley(_coalitions(model, point, base, blocks), len(blocks))
            )
    phi_mean = tuple(
        sum((values[i] for values in all_phi), Fraction()) / count
        for i in range(n)
    )
    if policy.group_method is GroupMethod.SUM:
        group_mean = tuple(
            sum((phi_mean[i] for i in block), Fraction()) for block in blocks
        )
    else:
        group_mean = tuple(
            sum((values[i] for values in all_group), Fraction()) / count
            for i in range(len(blocks))
        )
    raw = game[-1]
    baseline_value = sum(base_outputs, Fraction()) / count
    if (
        sum(phi_mean, Fraction()) != raw - baseline_value
        or sum(group_mean, Fraction()) != raw - baseline_value
    ):
        raise ValueError("exact coalition accounting failed")
    contributions = tuple(
        ContributionV1(name, _float(value))
        for name, value in zip(model.features, phi_mean)
    )
    residual = (
        Fraction(_float(raw))
        - Fraction(_float(baseline_value))
        - sum((Fraction(c.value) for c in contributions), Fraction())
    )
    tolerance = Fraction(policy.absolute_tolerance) + Fraction(
        policy.relative_tolerance
    ) * abs(raw - baseline_value)
    if abs(residual) > tolerance:
        raise ValueError(
            "additive float serialization exceeds frozen tolerance"
        )
    group_values = tuple(
        ContributionV1(name, _float(value))
        for name, value in zip(policy.reporting_cut, group_mean)
    )
    group_residual = (
        Fraction(_float(raw))
        - Fraction(_float(baseline_value))
        - sum((Fraction(c.value) for c in group_values), Fraction())
    )
    if abs(group_residual) > tolerance:
        raise ValueError("group float serialization exceeds frozen tolerance")
    ambiguity = tuple(
        AttributionAmbiguityV1(
            name,
            _float(max(v[i] for v in all_phi) - min(v[i] for v in all_phi)),
            _float(sibling[i]),
        )
        for i, name in enumerate(model.features)
    )
    unstable = any(
        max(item.background_range, item.sibling_sensitivity)
        > policy.instability_threshold
        for item in ambiguity
    )
    dimension = None
    if count >= 2:
        from histdatacom.data_quality.training_weights import (
            member_correlation_dimension,
        )

        columns = []
        for i in range(n):
            support = []
            for sample in background.snapshots:
                value = sample.values[i].value
                assert value is not None
                support.append(value)
            columns.append(tuple(support))
        dimension = member_correlation_dimension(tuple(columns))
    return _Computed(
        state=(
            AttributionState.NONIDENTIFIABLE
            if unstable
            else AttributionState.IDENTIFIED
        ),
        raw_output=_float(raw),
        baseline_output=_float(baseline_value),
        contributions=contributions,
        groups=group_values,
        interactions=tuple(
            sorted(
                (
                    InteractionContributionV1(
                        (model.features[i], model.features[j]), _float(value)
                    )
                    for (i, j), value in pair_dividends.items()
                ),
                key=lambda item: item.name,
            )
        ),
        ambiguity=ambiguity,
        additive_residual=_float(residual),
        group_additive_residual=_float(group_residual),
        effective_feature_dimension=dimension,
        evaluations=evaluations,
        reason=(
            "unstable_singleton_credit_use_declared_groups"
            if unstable
            else "exact_synthetic_reference_accounting_only"
        ),
        explained_output=output,
    )


def explain_reference(
    model: ReferenceModelV1,
    policy: ExplanationPolicyV1,
    snapshot: AttributionSnapshotV1,
    background: AttributionBackgroundV1,
    *,
    generated_at_ns: int,
) -> DecisionAttributionV1:
    """Run the actual bounded model and explainer; constructors/readers replay."""
    if (
        type(model) is not ReferenceModelV1
        or type(policy) is not ExplanationPolicyV1
        or type(snapshot) is not AttributionSnapshotV1
        or type(background) is not AttributionBackgroundV1
    ):
        raise TypeError(
            "reference execution requires exact public immutable input contracts"
        )
    return DecisionAttributionV1(
        AttributionEvidenceKind.FIXTURE,
        policy,
        reference(model),
        model,
        snapshot,
        background,
        generated_at_ns,
        **_computed(model, policy, snapshot, background),
    )


def verify_attribution(attribution: DecisionAttributionV1) -> None:
    model = attribution.model
    if model is None or reference(model) != attribution.model_reference:
        raise ValueError("reference model bytes/identity mismatch")
    if attribution.generated_at_ns < max(
        attribution.policy.declared_at_ns, attribution.background.declared_at_ns
    ):
        raise ValueError("explanation precedes declared policy/background")
    expected = _computed(
        model, attribution.policy, attribution.snapshot, attribution.background
    )
    for key, value in expected.items():
        if getattr(attribution, key) != value:
            raise ValueError(
                "retained attribution differs from exact model/explainer replay: "
                + key
            )

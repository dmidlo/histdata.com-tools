"""Six native-bound uncertainty views with exact, explicit scenario semantics."""

from __future__ import annotations

import re
from collections import defaultdict
from fractions import Fraction
from typing import Any

from .training_contracts import (
    MAX_TRAINING_BYTES,
    TrainingOrigin,
    training_json,
    training_load,
)
from .training_scenario_contracts import (
    SCENARIO_AXES,
    ScenarioAxis,
    ScenarioAxisState,
    ScenarioMemberStatus,
    ScenarioViewKind,
    TrainingScenarioAxisV1,
    TrainingScenarioCoordinateV1,
    TrainingScenarioDenominatorV1,
    TrainingScenarioMemberV1,
    TrainingScenarioPanelRowV1,
    TrainingScenarioPlanV1,
    TrainingScenarioQuantileV1,
    TrainingScenarioRationalV1,
    TrainingScenarioRequestV1,
    TrainingScenarioScalarV1,
    TrainingScenarioSummaryV1,
    TrainingScenarioViewV1,
    TrainingScenarioWeightV1,
    readmit_scenario,
)
from .training_scenario_math import (
    MAX_SCENARIO_CELLS,
    MAX_SCENARIO_COORDINATES,
    checked_fraction,
    sample_scenario_member,
    scenario_scalar_moments,
)
from .training_scenario_native import (
    assert_scenario_native_current,
    inspect_scenario_native,
)


def _denominator(
    members: tuple[TrainingScenarioMemberV1, ...],
) -> TrainingScenarioDenominatorV1:
    return TrainingScenarioDenominatorV1(
        len(members),
        *(
            sum(member.status is status for member in members)
            for status in ScenarioMemberStatus
        ),
    )


def _axis_key(
    axes: tuple[TrainingScenarioAxisV1, ...],
) -> tuple[tuple[str, str, str | None], ...]:
    return tuple(
        (axis.axis.value, axis.state.value, axis.value_id) for axis in axes
    )


def _preserved(
    member: TrainingScenarioMemberV1, collapsed: tuple[ScenarioAxis, ...]
) -> tuple[TrainingScenarioAxisV1, ...]:
    return tuple(axis for axis in member.axes if axis.axis not in collapsed)


def _groups(
    members: tuple[TrainingScenarioMemberV1, ...],
    collapsed: tuple[ScenarioAxis, ...],
) -> dict[str, tuple[TrainingScenarioMemberV1, ...]]:
    groups: dict[str, list[TrainingScenarioMemberV1]] = defaultdict(list)
    for member in members:
        # Config identity remains a scientific stratum even if two configs
        # share the same engine name. An engine ID is not its fitted law.
        wire = training_json(
            [
                member.evidence_unit_id,
                list(member.generator_config_ids),
                [
                    list(item)
                    for item in _axis_key(_preserved(member, collapsed))
                ],
            ]
        )
        groups[wire].append(member)
    return {
        key: tuple(sorted(values, key=lambda item: item.member_key))
        for key, values in sorted(groups.items())
    }


def _central(
    plan: TrainingScenarioPlanV1,
    members: tuple[TrainingScenarioMemberV1, ...],
    *,
    required: bool,
) -> tuple[str, ...]:
    by_key = {member.member_key: member for member in members}
    selected = plan.policy.central_member_keys
    if not selected and not required:
        return ()
    if any(key not in by_key for key in selected):
        raise ValueError(
            "central selector is outside complete native member inventory"
        )
    units: set[str] = set()
    for key in selected:
        member = by_key[key]
        if member.evidence_unit_id in units:
            raise ValueError(
                "central policy selects multiple members for one evidence unit"
            )
        units.add(member.evidence_unit_id)
        observation = next(
            axis
            for axis in member.axes
            if axis.axis is ScenarioAxis.OBSERVATION_RETENTION
        )
        if observation.state is ScenarioAxisState.UNAVAILABLE:
            raise ValueError("central observation coordinate is unavailable")
        if (
            observation.state is ScenarioAxisState.KNOWN
            and observation.value_id != "central_fitted_retention"
        ):
            raise ValueError(
                "central policy must select actual fitted-central retention"
            )
        # No primary-member default, loss ranking or implicit central transition.
        if any(
            axis.state is ScenarioAxisState.UNAVAILABLE for axis in member.axes
        ):
            raise ValueError(
                "central member has an unavailable scenario coordinate"
            )
    if required and units != {member.evidence_unit_id for member in members}:
        raise ValueError(
            "central policy must select exactly one whole member per evidence unit"
        )
    return selected


def _selection(
    plan: TrainingScenarioPlanV1,
    request: TrainingScenarioRequestV1,
    members: tuple[TrainingScenarioMemberV1, ...],
) -> tuple[str, ...]:
    central = _central(
        plan,
        members,
        required=request.view is ScenarioViewKind.CENTRAL_COUNTERFACTUAL,
    )
    if request.view is ScenarioViewKind.OBSERVED_ONLY:
        return ()
    if request.view is ScenarioViewKind.CENTRAL_COUNTERFACTUAL:
        if not central:
            raise ValueError(
                "central counterfactual requires native member support"
            )
        return central
    if request.view is ScenarioViewKind.SAMPLE_ONE_MEMBER:
        # Select one joint-context whole path, not one path in every context.
        # This is descriptive uniform-roster selection, not a posterior law.
        units: dict[str, list[str]] = defaultdict(list)
        for member in members:
            units[member.evidence_unit_id].append(member.member_key)
        result = []
        for unit, keys in sorted(units.items()):
            result.append(
                sample_scenario_member(
                    tuple(sorted(keys)),
                    evidence_unit_id=unit,
                    epoch=request.epoch,
                    seed=plan.policy.sampling_seed,
                    group_key="joint-native-context-roster.v1",
                )
            )
        return tuple(sorted(result))
    return tuple(member.member_key for member in members)


def _scalar(
    member: TrainingScenarioMemberV1,
    coordinate: TrainingScenarioCoordinateV1,
    rows: tuple[Any, ...],
) -> TrainingScenarioScalarV1:
    status = member.status
    reason = member.reason_codes
    if (
        coordinate.start_ns < member.start_ns
        or coordinate.end_ns > member.end_ns
    ):
        status, reason = ScenarioMemberStatus.UNAVAILABLE, (
            "incomplete_member_support",
        )
    if status in (
        ScenarioMemberStatus.REFUSED,
        ScenarioMemberStatus.UNAVAILABLE,
    ):
        return TrainingScenarioScalarV1(
            coordinate, member.member_key, None, status, reason
        )
    selected = tuple(
        row
        for row in rows
        if row.symbol == coordinate.symbol
        and coordinate.start_ns <= row.event_time_ns < coordinate.end_ns
    )
    if not selected:
        empty_value = (
            TrainingScenarioRationalV1.from_fraction(Fraction(0))
            if coordinate.feature.endswith("_count")
            else None
        )
        return TrainingScenarioScalarV1(
            coordinate,
            member.member_key,
            empty_value,
            ScenarioMemberStatus.EMPTY,
            ("no_events_in_admitted_interval",),
        )
    feature = coordinate.feature
    if feature == "event_count":
        result = Fraction(len(selected))
    elif feature == "observed_count":
        result = Fraction(
            sum(row.origin is TrainingOrigin.OBSERVED for row in selected)
        )
    elif feature == "synthetic_count":
        result = Fraction(
            sum(
                row.origin is TrainingOrigin.SYNTHETIC_RECONSTRUCTION
                for row in selected
            )
        )
    else:
        values = []
        for row in selected:
            payload = training_load(row.value_json)
            native_bid, native_ask = payload["bid"], payload["ask"]
            if type(native_bid) is not float or type(native_ask) is not float:
                raise ValueError("native scenario quote is not exact binary64")
            bid = checked_fraction(Fraction.from_float(native_bid))
            ask = checked_fraction(Fraction.from_float(native_ask))
            values.append(
                checked_fraction(ask - bid)
                if feature == "spread_mean"
                else (bid if feature.startswith("bid_") else ask)
            )
        if feature.endswith("_min"):
            result = min(values)
        elif feature.endswith("_max"):
            result = max(values)
        else:
            total = Fraction(0)
            for value in values:
                total = checked_fraction(total + value)
            result = checked_fraction(total / len(values))
    return TrainingScenarioScalarV1(
        coordinate,
        member.member_key,
        TrainingScenarioRationalV1.from_fraction(result),
        ScenarioMemberStatus.ELIGIBLE,
    )


def _scalars(
    plan: TrainingScenarioPlanV1,
    request: TrainingScenarioRequestV1,
    members: tuple[TrainingScenarioMemberV1, ...],
    rows: tuple[tuple[str | None, Any], ...],
) -> tuple[TrainingScenarioScalarV1, ...]:
    units = {unit.artifact_id: unit for unit in plan.ownership.units}
    applicable = tuple(
        member
        for member in members
        if units[member.evidence_unit_id].start_ns < request.end_ns
        and request.start_ns < units[member.evidence_unit_id].end_ns
    )
    width = len(request.symbols) * len(plan.policy.features)
    if (
        len(applicable) * width > MAX_SCENARIO_CELLS
        or len({member.evidence_unit_id for member in applicable}) * width
        > MAX_SCENARIO_COORDINATES
    ):
        raise ValueError("scenario scalar expansion exceeds prospective bound")
    by_member: dict[str, list[Any]] = defaultdict(list)
    for key, row in rows:
        if key is not None:
            by_member[key].append(row)
    result = []
    for member in applicable:
        unit = units[member.evidence_unit_id]
        start, end = max(request.start_ns, unit.start_ns), min(
            request.end_ns, unit.end_ns
        )
        for symbol in request.symbols:
            for feature in plan.policy.features:
                coordinate = TrainingScenarioCoordinateV1(
                    unit.artifact_id, symbol, start, end, feature
                )
                result.append(
                    _scalar(
                        member, coordinate, tuple(by_member[member.member_key])
                    )
                )
    return tuple(
        sorted(
            result,
            key=lambda item: (item.coordinate.coordinate_id, item.member_key),
        )
    )


def _summaries(
    plan: TrainingScenarioPlanV1,
    members: tuple[TrainingScenarioMemberV1, ...],
    scalars: tuple[TrainingScenarioScalarV1, ...],
) -> tuple[TrainingScenarioSummaryV1, ...]:
    by_scalar = {
        (item.member_key, item.coordinate.coordinate_id): item
        for item in scalars
    }
    coordinates = {
        item.coordinate.coordinate_id: item.coordinate for item in scalars
    }
    result: list[TrainingScenarioSummaryV1] = []
    for group in _groups(members, plan.policy.collapse_axes).values():
        for coordinate_id, coordinate in sorted(coordinates.items()):
            if coordinate.evidence_unit_id != group[0].evidence_unit_id:
                continue
            if len(result) >= MAX_SCENARIO_COORDINATES:
                raise ValueError("scenario summary expansion exceeds bound")
            values = tuple(
                by_scalar[(member.member_key, coordinate_id)]
                for member in group
            )
            # Preserved axis evidence is the union, not whichever member sorts
            # first; actual per-window scenario identities remain auditable.
            preserved = tuple(
                TrainingScenarioAxisV1(
                    axis.axis,
                    axis.state,
                    axis.value_id,
                    tuple(
                        sorted(
                            {
                                identity
                                for member in group
                                for other in member.axes
                                if other.axis is axis.axis
                                for identity in other.evidence_ids
                            }
                        )
                    ),
                )
                for axis in _preserved(group[0], plan.policy.collapse_axes)
            )
            denominator = TrainingScenarioDenominatorV1(
                len(values),
                *(
                    sum(value.status is status for value in values)
                    for status in ScenarioMemberStatus
                ),
            )
            reasons = {
                reason for value in values for reason in value.reason_codes
            }
            unavailable = any(value.value is None for value in values)
            if any(
                axis.state is ScenarioAxisState.UNAVAILABLE
                for member in group
                for axis in member.axes
                if axis.axis in plan.policy.collapse_axes
            ):
                unavailable = True
                reasons.add("unavailable_collapsed_axis")
            # An unassigned terminal outcome cannot vanish into a separate
            # unknown stratum while a coincident known stratum claims completeness.
            if any(
                member.evidence_unit_id == coordinate.evidence_unit_id
                and member.start_ns < coordinate.end_ns
                and coordinate.start_ns < member.end_ns
                and any(
                    axis.state is ScenarioAxisState.UNAVAILABLE
                    for axis in member.axes
                )
                for member in members
            ):
                unavailable = True
                reasons.add("unallocated_terminal_support")
            if unavailable:
                result.append(
                    TrainingScenarioSummaryV1(
                        coordinate,
                        preserved,
                        tuple(member.member_key for member in group),
                        denominator,
                        (),
                        None,
                        None,
                        (),
                        tuple(sorted(reasons | {"incomplete_scalar_support"})),
                        generator_config_ids=group[0].generator_config_ids,
                    )
                )
                continue
            moments = scenario_scalar_moments(
                tuple(
                    value.value.value
                    for value in values
                    if value.value is not None
                ),
                (Fraction(1),) * len(values),
                tuple(
                    probability.value for probability in plan.policy.quantiles
                ),
            )
            result.append(
                TrainingScenarioSummaryV1(
                    coordinate,
                    preserved,
                    tuple(member.member_key for member in group),
                    denominator,
                    tuple(
                        TrainingScenarioWeightV1(
                            member.member_key,
                            TrainingScenarioRationalV1.from_fraction(weight),
                        )
                        for member, weight in zip(
                            group, moments.normalized_weights
                        )
                    ),
                    TrainingScenarioRationalV1.from_fraction(moments.mean),
                    TrainingScenarioRationalV1.from_fraction(
                        moments.population_variance
                    ),
                    tuple(
                        TrainingScenarioQuantileV1(
                            probability,
                            TrainingScenarioRationalV1.from_fraction(value),
                        )
                        for probability, value in zip(
                            plan.policy.quantiles, moments.quantile_values
                        )
                    ),
                    generator_config_ids=group[0].generator_config_ids,
                )
            )
    return tuple(sorted(result, key=lambda item: item.artifact_id))


def materialize_training_scenario_view(
    plan: TrainingScenarioPlanV1, request: TrainingScenarioRequestV1
) -> TrainingScenarioViewV1:
    """Fresh complete admission, deterministic view, then source revalidation."""
    original_plan, original_request = plan, request
    plan = readmit_scenario(plan, TrainingScenarioPlanV1)
    request = readmit_scenario(request, TrainingScenarioRequestV1)
    inventory = inspect_scenario_native(plan, request)
    members = inventory.members
    if request.view is not ScenarioViewKind.OBSERVED_ONLY and not members:
        raise ValueError(
            "counterfactual view requires a complete native member inventory"
        )
    selected = _selection(plan, request, members)
    eligible = {
        member.member_key
        for member in members
        if member.status is ScenarioMemberStatus.ELIGIBLE
    }
    panel_kinds = {
        ScenarioViewKind.OBSERVED_ONLY,
        ScenarioViewKind.MEMBER_PANEL,
        ScenarioViewKind.CENTRAL_COUNTERFACTUAL,
        ScenarioViewKind.SAMPLE_ONE_MEMBER,
    }
    rows = tuple(
        TrainingScenarioPanelRowV1(key, row)
        for key, row in inventory.rows
        if request.view in panel_kinds
        and (
            key is None
            if request.view is ScenarioViewKind.OBSERVED_ONLY
            else key in selected and key in eligible
        )
    )
    rows = tuple(
        sorted(
            rows,
            key=lambda item: (
                item.member_key or "",
                item.row.evidence_unit_id,
                item.row.event_time_ns,
                item.row.symbol or "",
                item.row.source_row_key,
            ),
        )
    )
    scalars = (
        ()
        if request.view is ScenarioViewKind.OBSERVED_ONLY
        else _scalars(plan, request, members, inventory.rows)
    )
    aggregate = request.view in (
        ScenarioViewKind.MARGINALIZED_FEATURES,
        ScenarioViewKind.UNCERTAINTY_FEATURES,
    )
    collapsed = plan.policy.collapse_axes if aggregate else ()
    summaries = _summaries(plan, members, scalars) if aggregate else ()
    # Charge nested canonical members before assembling the final expanded wire.
    retained = len(plan.to_json()) + len(request.to_json()) + 4096
    for value in (*members, *rows, *scalars, *summaries, *inventory.roots):
        retained += len(value.to_json()) + 2
        if retained > MAX_TRAINING_BYTES:
            raise ValueError(
                "scenario view exceeds prospective serialized byte bound"
            )
    result = TrainingScenarioViewV1(
        plan=plan,
        request=request,
        members=members,
        rows=rows,
        scalars=scalars,
        summaries=summaries,
        denominator=_denominator(members),
        collapsed_axes=collapsed,
        preserved_axes=tuple(
            axis for axis in SCENARIO_AXES if axis not in collapsed
        ),
        verification_roots=inventory.roots,
        selected_member_keys=selected,
    )
    assert_scenario_native_current(plan, inventory)
    if (
        readmit_scenario(original_plan, TrainingScenarioPlanV1) != plan
        or readmit_scenario(original_request, TrainingScenarioRequestV1)
        != request
    ):
        raise ValueError(
            "scenario caller inputs changed during native execution"
        )
    return result


def replay_training_scenario_view(
    view: TrainingScenarioViewV1, *, expected_view_id: str
) -> TrainingScenarioViewV1:
    """Rebuild selected native history; a resealed record is not proof."""
    if (
        type(expected_view_id) is not str
        or re.fullmatch(
            r"training-scenario-view:sha256:[0-9a-f]{64}", expected_view_id
        )
        is None
    ):
        raise ValueError(
            "scenario replay requires an independently selected view ID"
        )
    expected = readmit_scenario(view, TrainingScenarioViewV1)
    if expected.artifact_id != expected_view_id:
        raise ValueError(
            "scenario replay differs from independently selected view ID"
        )
    actual = materialize_training_scenario_view(expected.plan, expected.request)
    if actual.to_json() != expected.to_json():
        raise ValueError(
            "scenario view differs from fresh complete native replay"
        )
    return actual

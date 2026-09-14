from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import cast

import pytest

from histdatacom.market_context.economic_calendar import EconomicReleaseStage
from histdatacom.market_context.inflation_prices import (
    InflationComponentAggregationV1,
    InflationComponentContributionV1,
    InflationComponentCoverage,
    InflationConceptV1,
    InflationContributionBasis,
    InflationDerivationVintageBasis,
    InflationEconomyProfileV1,
    InflationExpectationKind,
    InflationExpectationV1,
    InflationFrequency,
    InflationIndexFamily,
    InflationMethodologyChangeKind,
    InflationMethodologyEventV1,
    InflationObservationBasis,
    InflationObservationV1,
    InflationProfileAuditV1,
    InflationRateDerivationV1,
    InflationReleaseSequenceV1,
    InflationReleaseTripletV1,
    InflationReleaseV1,
    InflationSeasonalBasis,
    InflationTransformation,
    InflationVariant,
    InflationVintageChainV1,
    audit_inflation_profiles,
    built_in_inflation_profiles,
    require_inflation_profile_coverage,
)
from histdatacom.market_context.official_sources import (
    load_packaged_official_source_registry,
)


def _ns(year: int, month: int, day: int = 1) -> int:
    return int(
        datetime(year, month, day, tzinfo=timezone.utc).timestamp()
        * 1_000_000_000
    )


JAN_END = _ns(2024, 1, 31)
FEB_END = _ns(2024, 2, 29)
JAN_2023_END = _ns(2023, 1, 31)
RELEASE = _ns(2024, 3, 12)


def _concept(
    *,
    transformation: InflationTransformation = (
        InflationTransformation.YEAR_OVER_YEAR_PERCENT
    ),
    variant: InflationVariant = InflationVariant.HEADLINE,
    component_path: tuple[str, ...] = (),
    excluded_components: tuple[str, ...] = (),
    family: InflationIndexFamily = InflationIndexFamily.CPI,
    frequency: InflationFrequency = InflationFrequency.MONTHLY,
    seasonal: InflationSeasonalBasis = (
        InflationSeasonalBasis.NOT_SEASONALLY_ADJUSTED
    ),
    source_series_id: str = "cuur0000sa0",
    methodology_era_id: str = "cpi-era-2024",
) -> InflationConceptV1:
    unit = (
        "index-points"
        if transformation is InflationTransformation.INDEX_LEVEL
        else "percent"
    )
    return InflationConceptV1(
        economy_code="US",
        indicator_key="us-cpi",
        source_series_id=source_series_id,
        index_family=family,
        variant=variant,
        transformation=transformation,
        frequency=frequency,
        seasonal_basis=seasonal,
        unit=unit,
        scale=1.0,
        component_path=component_path,
        excluded_components=excluded_components,
        coverage="United States urban consumers",
        price_basis="consumer transaction prices",
        index_base_period=(
            "1982-1984=100"
            if transformation is InflationTransformation.INDEX_LEVEL
            else None
        ),
        methodology_era_id=methodology_era_id,
    )


def _observation(
    concept: InflationConceptV1 | None = None,
    *,
    event: str = "us-cpi-2024-02-initial",
    period: str = "2024-02",
    period_end: int = FEB_END,
    value: float = 3.2,
    published: int = RELEASE,
    available: int | None = None,
    stage: EconomicReleaseStage = EconomicReleaseStage.INITIAL,
    revision: int = 0,
    supersedes: str | None = None,
    basis: InflationObservationBasis = InflationObservationBasis.SOURCE_PUBLISHED,
    derived_ids: tuple[str, ...] = (),
    derivation_id: str | None = None,
    source_release_id: str = "cpi-news-release-2024-03",
) -> InflationObservationV1:
    return InflationObservationV1(
        concept=concept or _concept(),
        logical_event_key=event,
        reference_period=period,
        reference_period_end_ns=period_end,
        stage=stage,
        basis=basis,
        value=value,
        raw_lexical=str(value),
        published_at_ns=published,
        available_at_ns=published + 1 if available is None else available,
        source_key="us.bls.public-data",
        source_release_id=source_release_id,
        source_request_id=f"request-{event}",
        content_sha256="a" * 64,
        revision_sequence=revision,
        supersedes_observation_id=supersedes,
        derived_from_observation_ids=derived_ids,
        derivation_id=derivation_id,
        limitations=("Fixture evidence is synthetic.",),
    )


def _expectation(
    concept: InflationConceptV1,
    *,
    event: str = "us-cpi-2024-02-initial",
    period: str = "2024-02",
    kind: InflationExpectationKind = InflationExpectationKind.EVENT_CONSENSUS,
    value: float | None = 3.1,
) -> InflationExpectationV1:
    unavailable = kind is InflationExpectationKind.UNAVAILABLE
    return InflationExpectationV1(
        target_event_key=event,
        concept=concept,
        reference_period=period,
        kind=kind,
        expected_value=None if unavailable else value,
        collection_start_ns=None if unavailable else RELEASE - 300,
        collection_cutoff_ns=None if unavailable else RELEASE - 200,
        available_at_ns=None if unavailable else RELEASE - 100,
        source_key=None if unavailable else "survey.vendor",
        source_snapshot_id=None if unavailable else "survey-snapshot-1",
        source_uri=None if unavailable else "https://example.test/survey",
        unavailable_reason=(
            "No exact-event forecast retained." if unavailable else None
        ),
        limitations=("Fixture expectation is synthetic.",),
    )


def _release(observation: InflationObservationV1) -> InflationReleaseV1:
    return InflationReleaseV1(
        logical_event_key=observation.logical_event_key,
        economy_code=observation.concept.economy_code,
        reference_period=observation.reference_period,
        reference_period_end_ns=observation.reference_period_end_ns,
        stage=observation.stage,
        published_at_ns=observation.published_at_ns,
        available_at_ns=observation.available_at_ns,
        source_key=observation.source_key,
        source_release_id=observation.source_release_id,
        source_request_id=observation.source_request_id,
        content_sha256=observation.content_sha256,
        observations=(observation,),
        display_observation_id=observation.observation_id,
        limitations=("Fixture release is synthetic.",),
    )


def test_concept_identity_keeps_transformation_variant_and_seasonality() -> (
    None
):
    headline_yoy = _concept()
    headline_mom = _concept(
        transformation=InflationTransformation.MONTH_OVER_MONTH_PERCENT
    )
    core_yoy = _concept(
        variant=InflationVariant.CORE,
        excluded_components=("food", "energy"),
    )
    adjusted_yoy = _concept(seasonal=InflationSeasonalBasis.SEASONALLY_ADJUSTED)

    assert (
        len(
            {
                headline_yoy.concept_id,
                headline_mom.concept_id,
                core_yoy.concept_id,
                adjusted_yoy.concept_id,
            }
        )
        == 4
    )
    assert InflationConceptV1.from_json(headline_yoy.to_json()) == headline_yoy


def test_concept_requires_index_base_core_exclusions_and_component_path() -> (
    None
):
    with pytest.raises(ValueError, match="base period"):
        replace(
            _concept(transformation=InflationTransformation.INDEX_LEVEL),
            index_base_period=None,
            concept_id="",
        )
    with pytest.raises(ValueError, match="exclusions"):
        _concept(variant=InflationVariant.CORE)
    with pytest.raises(ValueError, match="component path"):
        _concept(variant=InflationVariant.COMPONENT)


def test_observation_round_trip_and_tamper_resistance() -> None:
    observation = _observation()
    assert (
        InflationObservationV1.from_dict(observation.to_dict()) == observation
    )
    payload = observation.to_dict()
    payload["value"] = 9.9
    with pytest.raises(ValueError, match="observation_id"):
        InflationObservationV1.from_dict(payload)


def test_vintage_chain_retains_initial_and_every_revision() -> None:
    initial = _observation()
    first_revision = _observation(
        value=3.1,
        published=RELEASE + 1_000,
        revision=1,
        supersedes=initial.observation_id,
    )
    second_revision = _observation(
        value=3.0,
        published=RELEASE + 2_000,
        revision=2,
        supersedes=first_revision.observation_id,
    )
    chain = InflationVintageChainV1(
        observations=(second_revision, initial, first_revision)
    )

    assert chain.initial == initial
    assert chain.as_known_at(RELEASE + 1_500) == first_revision
    assert InflationVintageChainV1.from_dict(chain.to_dict()) == chain

    with pytest.raises(ValueError, match="supersede"):
        InflationVintageChainV1(
            observations=(
                initial,
                replace(
                    first_revision,
                    supersedes_observation_id="wrong-observation",
                    observation_id="",
                ),
            )
        )


def test_mom_and_yoy_derivations_use_exact_index_vintages() -> None:
    index = _concept(transformation=InflationTransformation.INDEX_LEVEL)
    january = _observation(
        index,
        event="us-cpi-index-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=100.0,
        published=RELEASE - 10_000,
    )
    february = _observation(index, value=101.0)
    mom = InflationRateDerivationV1(
        transformation=InflationTransformation.MONTH_OVER_MONTH_PERCENT,
        current_index=february,
        comparison_index=january,
        vintage_basis=InflationDerivationVintageBasis.INITIAL_RELEASE,
        claimed_initial_actual=True,
        derived_value=1.0,
        tolerance=1e-12,
    )
    assert mom.derived_value == pytest.approx(1.0)

    prior_year = _observation(
        index,
        event="us-cpi-index-2023-02",
        period="2023-02",
        period_end=_ns(2023, 2, 28),
        value=98.0,
        published=_ns(2023, 3, 14),
    )
    yoy = InflationRateDerivationV1(
        transformation=InflationTransformation.YEAR_OVER_YEAR_PERCENT,
        current_index=february,
        comparison_index=prior_year,
        vintage_basis=InflationDerivationVintageBasis.AS_KNOWN_AT_RELEASE,
        claimed_initial_actual=False,
        derived_value=100 * (101 / 98 - 1),
        tolerance=1e-12,
    )
    assert InflationRateDerivationV1.from_dict(yoy.to_dict()) == yoy


def test_quarterly_and_annualized_derivations_preserve_frequency() -> None:
    index = _concept(
        transformation=InflationTransformation.INDEX_LEVEL,
        family=InflationIndexFamily.PCE_DEFLATOR,
        frequency=InflationFrequency.QUARTERLY,
        source_series_id="pce-quarterly-index",
    )
    prior = _observation(
        index,
        event="us-pce-2023-q4",
        period="2023-Q4",
        period_end=_ns(2023, 12, 31),
        value=100.0,
        published=_ns(2024, 1, 25),
    )
    current = _observation(
        index,
        event="us-pce-2024-q1",
        period="2024-Q1",
        period_end=_ns(2024, 3, 31),
        value=101.0,
        published=_ns(2024, 4, 25),
    )
    qoq = InflationRateDerivationV1(
        transformation=InflationTransformation.QUARTER_OVER_QUARTER_PERCENT,
        current_index=current,
        comparison_index=prior,
        vintage_basis=InflationDerivationVintageBasis.INITIAL_RELEASE,
        claimed_initial_actual=True,
        derived_value=1.0,
        tolerance=1e-12,
    )
    annualized = InflationRateDerivationV1(
        transformation=(
            InflationTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT
        ),
        current_index=current,
        comparison_index=prior,
        vintage_basis=InflationDerivationVintageBasis.INITIAL_RELEASE,
        claimed_initial_actual=True,
        derived_value=100 * (1.01**4 - 1),
        tolerance=1e-12,
    )
    assert qoq.derived_value == pytest.approx(1.0)
    assert annualized.derived_value == pytest.approx(4.060401)


def test_current_database_cannot_masquerade_as_historical_initial_actual() -> (
    None
):
    index = _concept(transformation=InflationTransformation.INDEX_LEVEL)
    comparison = _observation(
        index,
        event="us-cpi-index-2023-02",
        period="2023-02",
        period_end=_ns(2023, 2, 28),
        value=98.0,
        published=_ns(2023, 3, 14),
    )
    current = _observation(index, value=101.0)
    revised = _observation(
        index,
        value=101.2,
        published=RELEASE + 10_000,
        revision=1,
        supersedes=current.observation_id,
    )

    with pytest.raises(ValueError, match="initial index vintages"):
        InflationRateDerivationV1(
            transformation=InflationTransformation.YEAR_OVER_YEAR_PERCENT,
            current_index=revised,
            comparison_index=comparison,
            vintage_basis=InflationDerivationVintageBasis.LATEST_CURRENT,
            claimed_initial_actual=True,
            derived_value=100 * (101.2 / 98 - 1),
            tolerance=1e-12,
        )


def test_derivation_rejects_wrong_horizon_or_methodology_era() -> None:
    index = _concept(transformation=InflationTransformation.INDEX_LEVEL)
    current = _observation(index, value=101.0)
    wrong_lag = _observation(
        index,
        event="us-cpi-index-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=100.0,
        published=RELEASE - 10_000,
    )
    with pytest.raises(ValueError, match="horizon"):
        InflationRateDerivationV1(
            transformation=InflationTransformation.YEAR_OVER_YEAR_PERCENT,
            current_index=current,
            comparison_index=wrong_lag,
            vintage_basis=InflationDerivationVintageBasis.INITIAL_RELEASE,
            claimed_initial_actual=True,
            derived_value=1.0,
            tolerance=1e-12,
        )

    prior_era = _concept(
        transformation=InflationTransformation.INDEX_LEVEL,
        methodology_era_id="cpi-era-2023",
    )
    with pytest.raises(ValueError, match="changes the index concept"):
        InflationRateDerivationV1(
            transformation=InflationTransformation.YEAR_OVER_YEAR_PERCENT,
            current_index=current,
            comparison_index=_observation(
                prior_era,
                event="us-cpi-index-2023-02",
                period="2023-02",
                period_end=_ns(2023, 2, 28),
                value=98.0,
                published=_ns(2023, 3, 14),
            ),
            vintage_basis=InflationDerivationVintageBasis.INITIAL_RELEASE,
            claimed_initial_actual=True,
            derived_value=3.0,
            tolerance=1.0,
        )


def test_triplet_accepts_exact_pre_release_forecast_and_previous_vintage() -> (
    None
):
    concept = _concept()
    actual = _observation(concept)
    previous_initial = _observation(
        concept,
        event="us-cpi-2024-01-initial",
        period="2024-01",
        period_end=JAN_END,
        value=3.0,
        published=RELEASE - 10_000,
    )
    previous_revision = _observation(
        concept,
        event="us-cpi-2024-01-initial",
        period="2024-01",
        period_end=JAN_END,
        value=3.1,
        published=RELEASE - 5_000,
        revision=1,
        supersedes=previous_initial.observation_id,
    )
    triplet = InflationReleaseTripletV1(
        target_event_key=actual.logical_event_key,
        actual_initial=actual,
        previous_as_known=previous_revision,
        previous_as_known_at_ns=RELEASE - 1,
        expectation=_expectation(concept),
    )

    assert (triplet.actual, triplet.previous, triplet.forecast) == (
        3.2,
        3.1,
        3.1,
    )
    assert InflationReleaseTripletV1.from_dict(triplet.to_dict()) == triplet


def test_triplet_rejects_core_or_transformation_substitution() -> None:
    actual = _observation()
    previous_mom = _observation(
        _concept(
            transformation=InflationTransformation.MONTH_OVER_MONTH_PERCENT
        ),
        event="us-cpi-2024-01-initial",
        period="2024-01",
        period_end=JAN_END,
        value=0.2,
        published=RELEASE - 10_000,
    )
    with pytest.raises(ValueError, match="changes inflation concept"):
        InflationReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous_mom,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(actual.concept),
        )

    core = _concept(
        variant=InflationVariant.CORE,
        excluded_components=("food", "energy"),
    )
    with pytest.raises(ValueError, match="exact released concept"):
        InflationReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=_observation(
                actual.concept,
                event="us-cpi-2024-01-initial",
                period="2024-01",
                period_end=JAN_END,
                value=3.0,
                published=RELEASE - 10_000,
            ),
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(core),
        )


def test_triplet_rejects_nonadjacent_previous_period() -> None:
    actual = _observation()
    stale_previous = _observation(
        actual.concept,
        event="us-cpi-2023-12-initial",
        period="2023-12",
        period_end=_ns(2023, 12, 31),
        value=3.0,
        published=RELEASE - 20_000,
    )
    with pytest.raises(ValueError, match="immediately preceding"):
        InflationReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=stale_previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(actual.concept),
        )


def test_period_projection_cannot_populate_calendar_forecast() -> None:
    actual = _observation()
    previous = _observation(
        actual.concept,
        event="us-cpi-2024-01-initial",
        period="2024-01",
        period_end=JAN_END,
        value=3.0,
        published=RELEASE - 10_000,
    )
    expectation = _expectation(
        actual.concept,
        kind=InflationExpectationKind.OFFICIAL_SURVEY_PERIOD_TARGET,
    )
    with pytest.raises(ValueError, match="period projection"):
        InflationReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=expectation,
        )


def test_unavailable_expectation_is_explicit_and_round_trips() -> None:
    expectation = _expectation(
        _concept(), kind=InflationExpectationKind.UNAVAILABLE
    )
    assert expectation.expected_value is None
    assert not expectation.calendar_style_eligible
    assert (
        InflationExpectationV1.from_dict(expectation.to_dict()) == expectation
    )


def test_complete_component_table_reconciles_weights_and_contributions() -> (
    None
):
    aggregate = _observation(value=2.4)
    food = _observation(
        _concept(
            variant=InflationVariant.COMPONENT,
            component_path=("food",),
            source_series_id="cpi-food",
        ),
        value=2.0,
    )
    energy = _observation(
        _concept(
            variant=InflationVariant.COMPONENT,
            component_path=("energy",),
            source_series_id="cpi-energy",
        ),
        value=3.0,
    )
    components = (
        InflationComponentContributionV1(
            observation=food,
            weight_percent=60.0,
            contribution_percentage_points=1.2,
            contribution_basis=InflationContributionBasis.COMPUTED_FROM_WEIGHT,
            weight_vintage_id="weights-2024",
            contribution_raw_lexical="1.2",
            tolerance=1e-12,
        ),
        InflationComponentContributionV1(
            observation=energy,
            weight_percent=40.0,
            contribution_percentage_points=1.2,
            contribution_basis=InflationContributionBasis.COMPUTED_FROM_WEIGHT,
            weight_vintage_id="weights-2024",
            contribution_raw_lexical="1.2",
            tolerance=1e-12,
        ),
    )
    aggregation = InflationComponentAggregationV1(
        aggregate=aggregate,
        components=components,
        coverage=InflationComponentCoverage.COMPLETE,
        missing_component_keys=(),
        residual_percentage_points=0.0,
        tolerance=1e-12,
        reconciled=True,
    )

    assert InflationComponentAggregationV1.from_dict(aggregation.to_dict()) == (
        aggregation
    )


def test_missing_component_vintage_must_remain_partial() -> None:
    aggregate = _observation(value=2.4)
    food = _observation(
        _concept(
            variant=InflationVariant.COMPONENT,
            component_path=("food",),
            source_series_id="cpi-food",
        ),
        value=2.0,
    )
    component = InflationComponentContributionV1(
        observation=food,
        weight_percent=60.0,
        contribution_percentage_points=1.2,
        contribution_basis=InflationContributionBasis.COMPUTED_FROM_WEIGHT,
        weight_vintage_id="weights-2024",
        contribution_raw_lexical="1.2",
        tolerance=1e-12,
    )
    partial = InflationComponentAggregationV1(
        aggregate=aggregate,
        components=(component,),
        coverage=InflationComponentCoverage.PARTIAL,
        missing_component_keys=("energy",),
        residual_percentage_points=None,
        tolerance=1e-12,
        reconciled=False,
    )
    assert partial.coverage is InflationComponentCoverage.PARTIAL

    with pytest.raises(ValueError, match="weights do not sum"):
        replace(
            partial,
            coverage=InflationComponentCoverage.COMPLETE,
            missing_component_keys=(),
            residual_percentage_points=0.0,
            reconciled=True,
            aggregation_id="",
        )


def test_component_table_cannot_mix_weight_vintages() -> None:
    aggregate = _observation(value=2.4)
    first = _observation(
        _concept(
            variant=InflationVariant.COMPONENT,
            component_path=("food",),
            source_series_id="cpi-food",
        ),
        value=2.0,
    )
    second = _observation(
        _concept(
            variant=InflationVariant.COMPONENT,
            component_path=("energy",),
            source_series_id="cpi-energy",
        ),
        value=3.0,
    )
    components = (
        InflationComponentContributionV1(
            observation=first,
            weight_percent=60.0,
            contribution_percentage_points=1.2,
            contribution_basis=InflationContributionBasis.COMPUTED_FROM_WEIGHT,
            weight_vintage_id="weights-2023",
            contribution_raw_lexical="1.2",
            tolerance=1e-12,
        ),
        InflationComponentContributionV1(
            observation=second,
            weight_percent=40.0,
            contribution_percentage_points=1.2,
            contribution_basis=InflationContributionBasis.COMPUTED_FROM_WEIGHT,
            weight_vintage_id="weights-2024",
            contribution_raw_lexical="1.2",
            tolerance=1e-12,
        ),
    )
    with pytest.raises(ValueError, match="mixes weight vintages"):
        InflationComponentAggregationV1(
            aggregate=aggregate,
            components=components,
            coverage=InflationComponentCoverage.COMPLETE,
            missing_component_keys=(),
            residual_percentage_points=0.0,
            tolerance=1e-12,
            reconciled=True,
        )


def test_release_binds_every_observation_to_one_source_snapshot() -> None:
    observation = _observation()
    release = _release(observation)
    assert release.display_observation == observation
    assert InflationReleaseV1.from_dict(release.to_dict()) == release

    mismatched = _observation(
        observation.concept,
        source_release_id="another-release",
    )
    with pytest.raises(ValueError, match="do not reconcile"):
        replace(release, observations=(mismatched,), release_id="")


def test_flash_and_final_hicp_are_distinct_ordered_releases() -> None:
    hicp = _concept(family=InflationIndexFamily.HICP)
    flash_observation = _observation(
        hicp,
        event="ea-hicp-2024-02-flash",
        value=2.6,
        published=RELEASE,
        stage=EconomicReleaseStage.FLASH,
        source_release_id="ea-hicp-flash-2024-02",
    )
    final_observation = _observation(
        hicp,
        event="ea-hicp-2024-02-final",
        value=2.6,
        published=RELEASE + 10_000,
        stage=EconomicReleaseStage.FINAL,
        source_release_id="ea-hicp-final-2024-02",
    )
    sequence = InflationReleaseSequenceV1(
        releases=(_release(final_observation), _release(flash_observation))
    )

    assert [item.stage for item in sequence.releases] == [
        EconomicReleaseStage.FLASH,
        EconomicReleaseStage.FINAL,
    ]
    assert sequence.releases[0].logical_event_key != (
        sequence.releases[1].logical_event_key
    )
    assert InflationReleaseSequenceV1.from_dict(sequence.to_dict()) == sequence


def test_release_sequence_rejects_flash_final_overwrite() -> None:
    concept = _concept(family=InflationIndexFamily.HICP)
    flash = _release(
        _observation(
            concept,
            event="ea-hicp-2024-02",
            value=2.6,
            published=RELEASE,
            stage=EconomicReleaseStage.FLASH,
            source_release_id="ea-hicp-flash-2024-02",
        )
    )
    final = _release(
        _observation(
            concept,
            event="ea-hicp-2024-02",
            value=2.5,
            published=RELEASE + 10_000,
            stage=EconomicReleaseStage.FINAL,
            source_release_id="ea-hicp-final-2024-02",
        )
    )
    with pytest.raises(ValueError, match="overwrite"):
        InflationReleaseSequenceV1(releases=(flash, final))


def test_rebase_and_reweight_events_require_explicit_old_new_identity() -> None:
    rebase = InflationMethodologyEventV1(
        economy_code="US",
        index_family=InflationIndexFamily.CPI,
        change_kind=InflationMethodologyChangeKind.REBASE,
        effective_reference_period="2024-01",
        effective_from_ns=JAN_END,
        published_at_ns=RELEASE,
        available_at_ns=RELEASE + 1,
        source_key="us.bls.public-data",
        source_request_id="request-rebase-2024",
        content_sha256="b" * 64,
        prior_base_period="1982-1984=100",
        new_base_period="2024=100",
        prior_weight_vintage_id=None,
        new_weight_vintage_id=None,
        bridge_factor=0.32,
        comparable_across_change=True,
        limitations=("Synthetic rebase fixture.",),
    )
    assert InflationMethodologyEventV1.from_dict(rebase.to_dict()) == rebase

    with pytest.raises(ValueError, match="weight vintages"):
        replace(
            rebase,
            change_kind=InflationMethodologyChangeKind.REWEIGHT,
            prior_base_period=None,
            new_base_period=None,
            prior_weight_vintage_id=None,
            new_weight_vintage_id=None,
            bridge_factor=None,
            comparable_across_change=False,
            event_id="",
        )


def test_profiles_cover_all_21_economies_and_special_price_frameworks() -> None:
    registry = load_packaged_official_source_registry()
    profiles = built_in_inflation_profiles(registry)
    audit = require_inflation_profile_coverage(registry, profiles)
    by_economy = {item.economy_code: item for item in profiles}

    assert len(profiles) == 21
    assert audit.complete
    assert InflationProfileAuditV1.from_dict(audit.to_dict()) == audit
    assert InflationIndexFamily.HICP in by_economy["EA"].index_families
    assert {
        EconomicReleaseStage.FLASH,
        EconomicReleaseStage.FINAL,
    }.issubset(by_economy["EA"].supported_stages)
    assert "tokyo-versus-national-cpi" in by_economy["JP"].special_cases
    assert (
        by_economy["JP"].owner_for(InflationIndexFamily.PPI).source_key
        == "jp.boj.time-series"
    )
    assert (
        by_economy["US"].owner_for(InflationIndexFamily.PCE_DEFLATOR).source_key
        == "us.bea.api"
    )
    assert "owner-equivalent-rent-and-shelter" in by_economy["US"].special_cases
    assert InflationEconomyProfileV1.from_dict(by_economy["US"].to_dict()) == (
        by_economy["US"]
    )


def test_profile_audit_fails_closed_on_source_or_semantic_drift() -> None:
    registry = load_packaged_official_source_registry()
    profiles = list(built_in_inflation_profiles(registry))
    us_index = next(
        index
        for index, item in enumerate(profiles)
        if item.economy_code == "US"
    )
    us = profiles[us_index]
    owners = list(us.source_owners)
    owners[0] = replace(
        owners[0], institution="Incorrect institution", owner_id=""
    )
    profiles[us_index] = replace(us, source_owners=tuple(owners), profile_id="")

    audit = audit_inflation_profiles(registry, profiles)
    assert not audit.complete
    assert any(item.startswith("US:") for item in audit.source_mismatches)
    with pytest.raises(ValueError, match="coverage is incomplete"):
        require_inflation_profile_coverage(registry, profiles)

    clean_profiles = list(built_in_inflation_profiles(registry))
    clean_us = clean_profiles[us_index]
    pce_index = next(
        index
        for index, owner in enumerate(clean_us.source_owners)
        if owner.index_family is InflationIndexFamily.PCE_DEFLATOR
    )
    wrong_owner = clean_us.source_owners[pce_index]
    bls = registry.source("us.bls.public-data")
    switched = list(clean_us.source_owners)
    switched[pce_index] = replace(
        wrong_owner,
        source_key=bls.source_key,
        institution=bls.institution,
        owner_id="",
    )
    clean_profiles[us_index] = replace(
        clean_us, source_owners=tuple(switched), profile_id=""
    )
    reassignment = audit_inflation_profiles(registry, clean_profiles)
    assert not reassignment.complete
    assert "US:pce-deflator" in reassignment.source_mismatches


def test_boolean_fields_and_deterministic_ids_fail_closed() -> None:
    index = _concept(transformation=InflationTransformation.INDEX_LEVEL)
    current = _observation(index, value=101.0)
    comparison = _observation(
        index,
        event="us-cpi-index-2023-02",
        period="2023-02",
        period_end=_ns(2023, 2, 28),
        value=98.0,
        published=_ns(2023, 3, 14),
    )
    with pytest.raises(TypeError, match="claimed_initial_actual"):
        InflationRateDerivationV1(
            transformation=InflationTransformation.YEAR_OVER_YEAR_PERCENT,
            current_index=current,
            comparison_index=comparison,
            vintage_basis=InflationDerivationVintageBasis.INITIAL_RELEASE,
            claimed_initial_actual=cast(bool, 1),
            derived_value=0.0,
            tolerance=0.0,
        )

    concept_payload = _concept().to_dict()
    concept_payload["concept_id"] = "inflation-concept:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="concept_id"):
        InflationConceptV1.from_dict(concept_payload)

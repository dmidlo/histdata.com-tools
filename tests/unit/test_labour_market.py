from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from histdatacom.market_context.economic_calendar import EconomicReleaseStage
from histdatacom.market_context.labour_market import (
    LabourChangeDerivationV1,
    LabourConceptV1,
    LabourDerivationVintageBasis,
    LabourEconomyProfileV1,
    LabourExpectationKind,
    LabourExpectationV1,
    LabourFrequency,
    LabourMeasure,
    LabourMethodologyChangeKind,
    LabourMethodologyEventV1,
    LabourObservationBasis,
    LabourObservationV1,
    LabourProfileAuditV1,
    LabourReleasePackageV1,
    LabourReleaseTripletV1,
    LabourReleaseV1,
    LabourRevisionKind,
    LabourSeasonalBasis,
    LabourSurveyBasis,
    LabourTransformation,
    LabourVintageChainV1,
    audit_labour_profiles,
    built_in_labour_profiles,
    require_labour_profile_coverage,
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
RELEASE = _ns(2024, 3, 8)


def _concept(
    *,
    measure: LabourMeasure = LabourMeasure.PAYROLL_CHANGE,
    survey: LabourSurveyBasis = LabourSurveyBasis.ESTABLISHMENT_SURVEY,
    transformation: LabourTransformation = LabourTransformation.PERIOD_CHANGE,
    frequency: LabourFrequency = LabourFrequency.MONTHLY,
    seasonal: LabourSeasonalBasis = LabourSeasonalBasis.SEASONALLY_ADJUSTED,
    indicator_key: str = "us-nonfarm-payrolls",
    source_series_id: str = "ces0000000001",
    methodology_era_id: str = "ces-era-2024",
    industry_scope: str | None = "total nonfarm",
    duration_scope: str | None = None,
) -> LabourConceptV1:
    unit = "thousand-persons"
    if transformation in {
        LabourTransformation.RATE_PERCENT,
        LabourTransformation.MONTH_OVER_MONTH_PERCENT,
        LabourTransformation.YEAR_OVER_YEAR_PERCENT,
    }:
        unit = "percent"
    elif transformation is LabourTransformation.AVERAGE_HOURS:
        unit = "hours"
    return LabourConceptV1(
        economy_code="US",
        indicator_key=indicator_key,
        source_series_id=source_series_id,
        measure=measure,
        survey_basis=survey,
        transformation=transformation,
        frequency=frequency,
        seasonal_basis=seasonal,
        population_scope="civilian noninstitutional population age 16+",
        industry_scope=industry_scope,
        worker_scope="employees on nonfarm payrolls",
        duration_scope=duration_scope,
        unit=unit,
        scale=1_000.0,
        methodology_era_id=methodology_era_id,
    )


def _observation(
    concept: LabourConceptV1 | None = None,
    *,
    event: str = "us-payrolls-2024-02",
    period: str = "2024-02",
    period_end: int = FEB_END,
    value: float = 275.0,
    published: int = RELEASE,
    available: int | None = None,
    revision: int = 0,
    revision_kind: LabourRevisionKind = LabourRevisionKind.INITIAL,
    supersedes: str | None = None,
    source_release_id: str = "employment-situation-2024-03",
    basis: LabourObservationBasis = LabourObservationBasis.SOURCE_PUBLISHED,
    derived_ids: tuple[str, ...] = (),
    derivation_id: str | None = None,
) -> LabourObservationV1:
    return LabourObservationV1(
        concept=concept or _concept(),
        logical_event_key=event,
        reference_period=period,
        reference_period_end_ns=period_end,
        stage=EconomicReleaseStage.INITIAL,
        basis=basis,
        value=value,
        raw_lexical=str(value),
        published_at_ns=published,
        available_at_ns=published + 1 if available is None else available,
        source_key="us.bls.public-data",
        source_release_id=source_release_id,
        source_request_id=f"request-{source_release_id}",
        content_sha256="a" * 64,
        revision_sequence=revision,
        revision_kind=revision_kind,
        supersedes_observation_id=supersedes,
        derived_from_observation_ids=derived_ids,
        derivation_id=derivation_id,
        limitations=("Fixture evidence is synthetic.",),
    )


def _expectation(
    concept: LabourConceptV1,
    *,
    event: str = "us-payrolls-2024-02",
    period: str = "2024-02",
    value: float | None = 200.0,
    kind: LabourExpectationKind = LabourExpectationKind.EVENT_CONSENSUS,
) -> LabourExpectationV1:
    unavailable = kind is LabourExpectationKind.UNAVAILABLE
    return LabourExpectationV1(
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


def _release(
    observation: LabourObservationV1,
    *,
    package_key: str = "us-employment-situation-2024-03",
) -> LabourReleaseV1:
    return LabourReleaseV1(
        package_key=package_key,
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


def test_concept_keeps_household_and_establishment_surveys_distinct() -> None:
    payroll = _concept()
    household = _concept(
        measure=LabourMeasure.EMPLOYMENT_LEVEL,
        survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
        transformation=LabourTransformation.LEVEL,
        indicator_key="us-household-employment",
        source_series_id="lns12000000",
        industry_scope=None,
    )

    assert payroll.concept_id != household.concept_id
    assert payroll.survey_basis is LabourSurveyBasis.ESTABLISHMENT_SURVEY
    assert LabourConceptV1.from_json(payroll.to_json()) == payroll


def test_concept_rejects_payroll_household_and_claims_survey_substitution() -> (
    None
):
    with pytest.raises(ValueError, match="establishment survey"):
        _concept(survey=LabourSurveyBasis.HOUSEHOLD_SURVEY)
    with pytest.raises(ValueError, match="administrative identity"):
        _concept(
            measure=LabourMeasure.INITIAL_BENEFIT_CLAIMS,
            survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
            transformation=LabourTransformation.LEVEL,
        )


def test_measure_transformation_rules_cover_earnings_hours_and_vacancies() -> (
    None
):
    earnings = _concept(
        measure=LabourMeasure.EARNINGS_CHANGE,
        transformation=LabourTransformation.MONTH_OVER_MONTH_PERCENT,
    )
    hours = _concept(
        measure=LabourMeasure.HOURS,
        transformation=LabourTransformation.AVERAGE_HOURS,
    )
    vacancies = _concept(
        measure=LabourMeasure.VACANCIES,
        survey=LabourSurveyBasis.VACANCY_SURVEY,
        transformation=LabourTransformation.LEVEL,
    )
    assert {earnings.unit, hours.unit, vacancies.unit} == {
        "percent",
        "hours",
        "thousand-persons",
    }
    with pytest.raises(ValueError, match="invalid for labour measure"):
        _concept(
            measure=LabourMeasure.UNEMPLOYMENT_RATE,
            survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
            transformation=LabourTransformation.PERIOD_CHANGE,
        )


def test_observation_round_trip_and_identity_tamper_resistance() -> None:
    observation = _observation()
    assert LabourObservationV1.from_dict(observation.to_dict()) == observation
    payload = observation.to_dict()
    payload["value"] = 999.0
    with pytest.raises(ValueError, match="observation_id"):
        LabourObservationV1.from_dict(payload)


def test_vintage_chain_retains_routine_and_benchmark_revisions() -> None:
    initial = _observation()
    routine = _observation(
        value=260.0,
        published=RELEASE + 1_000,
        revision=1,
        revision_kind=LabourRevisionKind.ROUTINE,
        supersedes=initial.observation_id,
    )
    benchmark = _observation(
        value=250.0,
        published=RELEASE + 2_000,
        revision=2,
        revision_kind=LabourRevisionKind.BENCHMARK,
        supersedes=routine.observation_id,
    )
    chain = LabourVintageChainV1(observations=(benchmark, initial, routine))

    assert [item.revision_kind for item in chain.observations] == [
        LabourRevisionKind.INITIAL,
        LabourRevisionKind.ROUTINE,
        LabourRevisionKind.BENCHMARK,
    ]
    assert chain.as_known_at(RELEASE + 1_500) == routine
    assert LabourVintageChainV1.from_dict(chain.to_dict()) == chain


def test_change_derivation_uses_two_exact_initial_level_vintages() -> None:
    level = _concept(
        measure=LabourMeasure.PAYROLL_LEVEL,
        transformation=LabourTransformation.LEVEL,
    )
    previous = _observation(
        level,
        event="us-payroll-level-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=157_000.0,
        published=RELEASE - 10_000,
    )
    current = _observation(level, value=157_275.0)
    derivation = LabourChangeDerivationV1(
        target_concept=_concept(),
        current_level=current,
        previous_level=previous,
        vintage_basis=LabourDerivationVintageBasis.INITIAL_RELEASE,
        source_change_unavailable=True,
        claimed_initial_actual=True,
        derived_value=275.0,
        tolerance=1e-12,
    )

    assert (
        LabourChangeDerivationV1.from_dict(derivation.to_dict()) == derivation
    )


def test_published_change_prevents_recomputed_initial_claim() -> None:
    level = _concept(
        measure=LabourMeasure.PAYROLL_LEVEL,
        transformation=LabourTransformation.LEVEL,
    )
    previous = _observation(
        level,
        event="us-payroll-level-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=157_000.0,
        published=RELEASE - 10_000,
    )
    current = _observation(level, value=157_275.0)
    with pytest.raises(ValueError, match="unavailable published change"):
        LabourChangeDerivationV1(
            target_concept=_concept(),
            current_level=current,
            previous_level=previous,
            vintage_basis=LabourDerivationVintageBasis.INITIAL_RELEASE,
            source_change_unavailable=False,
            claimed_initial_actual=True,
            derived_value=275.0,
            tolerance=1e-12,
        )


def test_current_revised_levels_cannot_fabricate_historical_initial_change() -> (
    None
):
    level = _concept(
        measure=LabourMeasure.PAYROLL_LEVEL,
        transformation=LabourTransformation.LEVEL,
    )
    previous_initial = _observation(
        level,
        event="us-payroll-level-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=157_000.0,
        published=RELEASE - 10_000,
    )
    current_initial = _observation(level, value=157_275.0)
    current_revised = _observation(
        level,
        value=157_250.0,
        published=RELEASE + 10_000,
        revision=1,
        revision_kind=LabourRevisionKind.BENCHMARK,
        supersedes=current_initial.observation_id,
    )
    with pytest.raises(ValueError, match="both initial level vintages"):
        LabourChangeDerivationV1(
            target_concept=_concept(),
            current_level=current_revised,
            previous_level=previous_initial,
            vintage_basis=LabourDerivationVintageBasis.LATEST_CURRENT,
            source_change_unavailable=True,
            claimed_initial_actual=True,
            derived_value=250.0,
            tolerance=1e-12,
        )


def test_change_derivation_rejects_household_establishment_switch() -> None:
    establishment = _concept(
        measure=LabourMeasure.PAYROLL_LEVEL,
        transformation=LabourTransformation.LEVEL,
    )
    household = _concept(
        measure=LabourMeasure.EMPLOYMENT_LEVEL,
        survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
        transformation=LabourTransformation.LEVEL,
        indicator_key="us-household-employment",
        source_series_id="lns12000000",
        industry_scope=None,
    )
    with pytest.raises(ValueError, match="matching level measures"):
        LabourChangeDerivationV1(
            target_concept=_concept(),
            current_level=_observation(establishment),
            previous_level=_observation(
                household,
                event="us-household-2024-01",
                period="2024-01",
                period_end=JAN_END,
                value=160_000.0,
                published=RELEASE - 10_000,
            ),
            vintage_basis=LabourDerivationVintageBasis.INITIAL_RELEASE,
            source_change_unavailable=True,
            claimed_initial_actual=True,
            derived_value=-2_725.0,
            tolerance=1e-12,
        )


def test_triplet_accepts_simultaneous_previous_revision_at_release() -> None:
    concept = _concept()
    actual = _observation(concept)
    previous_initial = _observation(
        concept,
        event="us-payrolls-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=229.0,
        published=RELEASE - 10_000,
    )
    previous_revision = _observation(
        concept,
        event="us-payrolls-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=210.0,
        published=RELEASE,
        available=RELEASE,
        revision=1,
        revision_kind=LabourRevisionKind.SIMULTANEOUS_PREVIOUS,
        supersedes=previous_initial.observation_id,
    )
    triplet = LabourReleaseTripletV1(
        target_event_key=actual.logical_event_key,
        actual_initial=actual,
        previous_as_known=previous_revision,
        previous_as_known_at_ns=RELEASE,
        expectation=_expectation(concept),
    )

    assert (triplet.actual, triplet.previous, triplet.forecast) == (
        275.0,
        210.0,
        200.0,
    )
    assert LabourReleaseTripletV1.from_dict(triplet.to_dict()) == triplet


def test_triplet_rejects_wrong_survey_or_nonadjacent_period() -> None:
    actual = _observation()
    household_rate = _concept(
        measure=LabourMeasure.UNEMPLOYMENT_RATE,
        survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
        transformation=LabourTransformation.RATE_PERCENT,
        indicator_key="us-unemployment-rate",
        source_series_id="lns14000000",
        industry_scope=None,
    )
    previous = _observation(
        actual.concept,
        event="us-payrolls-2023-12",
        period="2023-12",
        period_end=_ns(2023, 12, 31),
        value=200.0,
        published=RELEASE - 20_000,
    )
    with pytest.raises(ValueError, match="immediately preceding"):
        LabourReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(actual.concept),
        )
    adjacent = replace(
        previous,
        reference_period="2024-01",
        reference_period_end_ns=JAN_END,
        observation_id="",
    )
    with pytest.raises(ValueError, match="exact released concept"):
        LabourReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=adjacent,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(household_rate),
        )


def test_weekly_triplet_requires_exactly_the_preceding_week() -> None:
    claims = _concept(
        measure=LabourMeasure.INITIAL_BENEFIT_CLAIMS,
        survey=LabourSurveyBasis.BENEFIT_CLAIMS_ADMINISTRATIVE,
        transformation=LabourTransformation.LEVEL,
        frequency=LabourFrequency.WEEKLY,
        indicator_key="us-initial-benefit-claims",
        source_series_id="dol-initial-claims",
        industry_scope=None,
    )
    actual = _observation(
        claims,
        event="us-initial-claims-2024-03-02",
        period="2024-03-02",
        period_end=_ns(2024, 3, 2),
    )
    previous = _observation(
        claims,
        event="us-initial-claims-2024-02-17",
        period="2024-02-17",
        period_end=_ns(2024, 2, 17),
        published=RELEASE - 10_000,
    )

    with pytest.raises(ValueError, match="immediately preceding week"):
        LabourReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(
                claims,
                event=actual.logical_event_key,
                period=actual.reference_period,
            ),
        )


def test_period_projection_cannot_populate_release_forecast() -> None:
    actual = _observation()
    previous = _observation(
        actual.concept,
        event="us-payrolls-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=229.0,
        published=RELEASE - 10_000,
    )
    with pytest.raises(ValueError, match="period projection"):
        LabourReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(
                actual.concept,
                kind=LabourExpectationKind.OFFICIAL_SURVEY_PERIOD_TARGET,
            ),
        )


def test_unavailable_expectation_is_explicit() -> None:
    expectation = _expectation(
        _concept(), kind=LabourExpectationKind.UNAVAILABLE
    )
    assert expectation.expected_value is None
    assert not expectation.calendar_style_eligible
    assert LabourExpectationV1.from_dict(expectation.to_dict()) == expectation


def test_release_package_keeps_opposing_indicator_events_distinct() -> None:
    payroll = _observation()
    unemployment_concept = _concept(
        measure=LabourMeasure.UNEMPLOYMENT_RATE,
        survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
        transformation=LabourTransformation.RATE_PERCENT,
        indicator_key="us-unemployment-rate",
        source_series_id="lns14000000",
        industry_scope=None,
    )
    unemployment = _observation(
        unemployment_concept,
        event="us-unemployment-2024-02",
        value=3.9,
    )
    releases = (_release(payroll), _release(unemployment))
    package = LabourReleasePackageV1(
        package_key="us-employment-situation-2024-03",
        economy_code="US",
        published_at_ns=RELEASE,
        available_at_ns=RELEASE + 1,
        source_key="us.bls.public-data",
        source_release_id="employment-situation-2024-03",
        source_request_id="request-employment-situation-2024-03",
        content_sha256="a" * 64,
        releases=releases,
        limitations=("Fixture package is synthetic.",),
    )

    assert package.release_for_measure(LabourMeasure.PAYROLL_CHANGE) == (
        _release(payroll)
    )
    assert package.release_for_measure(LabourMeasure.UNEMPLOYMENT_RATE) == (
        _release(unemployment)
    )
    assert LabourReleasePackageV1.from_dict(package.to_dict()) == package


def test_release_package_rejects_collapsed_indicator_or_mixed_evidence() -> (
    None
):
    payroll = _release(_observation())
    unemployment_concept = _concept(
        measure=LabourMeasure.UNEMPLOYMENT_RATE,
        survey=LabourSurveyBasis.HOUSEHOLD_SURVEY,
        transformation=LabourTransformation.RATE_PERCENT,
        indicator_key="us-unemployment-rate",
        source_series_id="lns14000000",
        industry_scope=None,
    )
    unemployment = _release(
        _observation(
            unemployment_concept,
            event="us-unemployment-2024-02",
            value=3.9,
        )
    )
    package = LabourReleasePackageV1(
        package_key="us-employment-situation-2024-03",
        economy_code="US",
        published_at_ns=RELEASE,
        available_at_ns=RELEASE + 1,
        source_key="us.bls.public-data",
        source_release_id="employment-situation-2024-03",
        source_request_id="request-employment-situation-2024-03",
        content_sha256="a" * 64,
        releases=(payroll, unemployment),
        limitations=("Fixture package is synthetic.",),
    )
    collapsed_observation = replace(
        unemployment.observations[0],
        logical_event_key=payroll.logical_event_key,
        observation_id="",
    )
    collapsed_release = _release(collapsed_observation)
    with pytest.raises(ValueError, match="collapses distinct indicators"):
        replace(
            package,
            releases=(payroll, collapsed_release),
            package_id="",
        )
    moved_observation = replace(
        unemployment.observations[0],
        published_at_ns=RELEASE + 2,
        available_at_ns=RELEASE + 2,
        observation_id="",
    )
    moved_release = replace(
        unemployment,
        published_at_ns=RELEASE + 2,
        available_at_ns=RELEASE + 2,
        observations=(moved_observation,),
        display_observation_id=moved_observation.observation_id,
        release_id="",
    )
    with pytest.raises(ValueError, match="share package evidence"):
        replace(
            package,
            releases=(payroll, moved_release),
            package_id="",
        )


def test_methodology_event_requires_benchmark_or_population_evidence() -> None:
    concept = _concept()
    benchmark = LabourMethodologyEventV1(
        economy_code="US",
        change_kind=LabourMethodologyChangeKind.BENCHMARK_REVISION,
        affected_concept_ids=(concept.concept_id,),
        effective_reference_period="2023-03",
        effective_from_ns=_ns(2023, 3, 31),
        published_at_ns=RELEASE,
        available_at_ns=RELEASE + 1,
        source_key="us.bls.public-data",
        source_request_id="request-benchmark-2024",
        content_sha256="b" * 64,
        prior_methodology_era_id="ces-era-2023",
        new_methodology_era_id="ces-era-2024",
        benchmark_reference_period="2023-03",
        prior_population_control_id=None,
        new_population_control_id=None,
        comparability_bridge_id="ces-bridge-2023-2024",
        comparable_across_change=True,
        limitations=("Synthetic benchmark fixture.",),
    )
    assert LabourMethodologyEventV1.from_dict(benchmark.to_dict()) == benchmark
    with pytest.raises(ValueError, match="old and new controls"):
        replace(
            benchmark,
            change_kind=LabourMethodologyChangeKind.POPULATION_CONTROL,
            benchmark_reference_period=None,
            prior_population_control_id=None,
            new_population_control_id=None,
            comparability_bridge_id=None,
            comparable_across_change=False,
            event_id="",
        )


def test_profiles_cover_all_21_economies_and_difficult_cases() -> None:
    registry = load_packaged_official_source_registry()
    profiles = built_in_labour_profiles(registry)
    audit = require_labour_profile_coverage(registry, profiles)
    by_economy = {item.economy_code: item for item in profiles}

    assert len(profiles) == 21
    assert audit.complete
    assert LabourProfileAuditV1.from_dict(audit.to_dict()) == audit
    assert "harmonized-unemployment-versus-national-series" in (
        by_economy["EA"].special_cases
    )
    assert "labour-force-survey-claimant-and-payrolled-employees" in (
        by_economy["GB"].special_cases
    )
    assert LabourSurveyBasis.VACANCY_SURVEY in (
        by_economy["US"].required_survey_bases
    )
    assert LabourMeasure.INITIAL_BENEFIT_CLAIMS in (
        by_economy["US"].required_measures
    )
    assert LabourEconomyProfileV1.from_dict(by_economy["US"].to_dict()) == (
        by_economy["US"]
    )


def test_profile_audit_fails_closed_on_source_or_semantic_drift() -> None:
    registry = load_packaged_official_source_registry()
    profiles = list(built_in_labour_profiles(registry))
    us_index = next(
        index
        for index, item in enumerate(profiles)
        if item.economy_code == "US"
    )
    profiles[us_index] = replace(
        profiles[us_index], institution="Incorrect institution", profile_id=""
    )
    audit = audit_labour_profiles(registry, profiles)
    assert not audit.complete
    assert "US" in audit.source_mismatches
    with pytest.raises(ValueError, match="coverage is incomplete"):
        require_labour_profile_coverage(registry, profiles)


def test_deterministic_concept_identity_fails_closed() -> None:
    payload = _concept().to_dict()
    payload["concept_id"] = "labour-concept:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="concept_id"):
        LabourConceptV1.from_dict(payload)

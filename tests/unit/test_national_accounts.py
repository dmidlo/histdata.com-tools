from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from histdatacom.market_context.economic_calendar import EconomicReleaseStage
from histdatacom.market_context.national_accounts import (
    NationalAccountsBenchmarkRevisionV1,
    NationalAccountsComponentCoverage,
    NationalAccountsComponentEntryV1,
    NationalAccountsComponentFramework,
    NationalAccountsComponentReconciliationV1,
    NationalAccountsComponentRole,
    NationalAccountsConceptV1,
    NationalAccountsDerivationVintageBasis,
    NationalAccountsEconomyProfileV1,
    NationalAccountsExpectationKind,
    NationalAccountsExpectationV1,
    NationalAccountsFrequency,
    NationalAccountsGrowthDerivationV1,
    NationalAccountsMeasure,
    NationalAccountsMethodologyChangeKind,
    NationalAccountsMethodologyEventV1,
    NationalAccountsObservationBasis,
    NationalAccountsObservationV1,
    NationalAccountsOutputBasis,
    NationalAccountsProfileAuditV1,
    NationalAccountsReconciliationBasis,
    NationalAccountsReleaseSequenceV1,
    NationalAccountsReleaseTripletV1,
    NationalAccountsReleaseV1,
    NationalAccountsRevisionKind,
    NationalAccountsSeasonalBasis,
    NationalAccountsTransformation,
    NationalAccountsVintageChainV1,
    audit_national_accounts_profiles,
    built_in_national_accounts_profiles,
    require_national_accounts_profile_coverage,
)
from histdatacom.market_context.official_sources import (
    load_packaged_official_source_registry,
)


def _ns(year: int, month: int, day: int = 1) -> int:
    return int(
        datetime(year, month, day, tzinfo=timezone.utc).timestamp()
        * 1_000_000_000
    )


Q3_END = _ns(2023, 9, 30)
Q4_END = _ns(2023, 12, 31)
Q1_END = _ns(2024, 3, 31)
ADVANCE_RELEASE = _ns(2024, 4, 25)
SECOND_RELEASE = _ns(2024, 5, 30)


def _concept(
    *,
    transformation: NationalAccountsTransformation = (
        NationalAccountsTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT
    ),
    output_basis: NationalAccountsOutputBasis = (
        NationalAccountsOutputBasis.REAL_CHAIN_VOLUME
    ),
    measure: NationalAccountsMeasure = NationalAccountsMeasure.GDP,
    framework: NationalAccountsComponentFramework | None = None,
    path: tuple[str, ...] = (),
    frequency: NationalAccountsFrequency = NationalAccountsFrequency.QUARTERLY,
    seasonal: NationalAccountsSeasonalBasis = (
        NationalAccountsSeasonalBasis.SEASONALLY_ADJUSTED
    ),
    indicator_key: str = "us-real-gdp",
    source_series_id: str = "bea-gdp-real",
    methodology_era_id: str = "nipa-2023-benchmark",
) -> NationalAccountsConceptV1:
    percent_transformations = {
        NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT,
        NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
        NationalAccountsTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT,
        NationalAccountsTransformation.YEAR_OVER_YEAR_PERCENT,
        NationalAccountsTransformation.CONTRIBUTION_PERCENTAGE_POINTS,
        NationalAccountsTransformation.SHARE_PERCENT,
    }
    return NationalAccountsConceptV1(
        economy_code="US",
        indicator_key=indicator_key,
        source_series_id=source_series_id,
        measure=measure,
        component_framework=framework,
        component_path=path,
        output_basis=output_basis,
        transformation=transformation,
        frequency=frequency,
        seasonal_basis=seasonal,
        valuation_basis="market prices",
        unit="percent" if transformation in percent_transformations else "usd",
        scale=(
            1.0 if transformation in percent_transformations else 1_000_000_000
        ),
        price_base_period=(
            None
            if output_basis is NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE
            else "2017"
        ),
        chain_linking_method=(
            "fisher-chain"
            if output_basis is NationalAccountsOutputBasis.REAL_CHAIN_VOLUME
            else None
        ),
        methodology_era_id=methodology_era_id,
    )


def _observation(
    concept: NationalAccountsConceptV1 | None = None,
    *,
    event: str = "us-gdp-2024q1-advance",
    period: str = "2024-Q1",
    period_end: int = Q1_END,
    stage: EconomicReleaseStage = EconomicReleaseStage.ADVANCE,
    value: float = 2.5,
    published: int = ADVANCE_RELEASE,
    available: int | None = None,
    revision: int = 0,
    revision_kind: NationalAccountsRevisionKind = (
        NationalAccountsRevisionKind.STAGE_INITIAL
    ),
    supersedes: str | None = None,
    source_release_id: str = "bea-gdp-2024q1-advance",
    basis: NationalAccountsObservationBasis = (
        NationalAccountsObservationBasis.SOURCE_PUBLISHED
    ),
    derived_ids: tuple[str, ...] = (),
    derivation_id: str | None = None,
) -> NationalAccountsObservationV1:
    return NationalAccountsObservationV1(
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
        source_key="us.bea.api",
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
    concept: NationalAccountsConceptV1,
    *,
    event: str = "us-gdp-2024q1-advance",
    stage: EconomicReleaseStage = EconomicReleaseStage.ADVANCE,
    period: str = "2024-Q1",
    kind: NationalAccountsExpectationKind = (
        NationalAccountsExpectationKind.EVENT_CONSENSUS
    ),
    value: float | None = 2.2,
) -> NationalAccountsExpectationV1:
    unavailable = kind is NationalAccountsExpectationKind.UNAVAILABLE
    return NationalAccountsExpectationV1(
        target_event_key=event,
        target_stage=stage,
        concept=concept,
        reference_period=period,
        kind=kind,
        expected_value=None if unavailable else value,
        collection_start_ns=None if unavailable else ADVANCE_RELEASE - 300,
        collection_cutoff_ns=None if unavailable else ADVANCE_RELEASE - 200,
        available_at_ns=None if unavailable else ADVANCE_RELEASE - 100,
        source_key=None if unavailable else "survey.vendor",
        source_snapshot_id=None if unavailable else "survey-snapshot-1",
        source_uri=None if unavailable else "https://example.test/survey",
        unavailable_reason=(
            "No exact-stage forecast retained." if unavailable else None
        ),
        limitations=("Fixture expectation is synthetic.",),
    )


def _release(
    observations: tuple[NationalAccountsObservationV1, ...],
    *,
    reconciliations: tuple[NationalAccountsComponentReconciliationV1, ...] = (),
) -> NationalAccountsReleaseV1:
    first = observations[0]
    return NationalAccountsReleaseV1(
        sequence_key="us-gdp-2024q1",
        logical_event_key=first.logical_event_key,
        economy_code=first.concept.economy_code,
        reference_period=first.reference_period,
        reference_period_end_ns=first.reference_period_end_ns,
        stage=first.stage,
        published_at_ns=first.published_at_ns,
        available_at_ns=first.available_at_ns,
        source_key=first.source_key,
        source_release_id=first.source_release_id,
        source_request_id=first.source_request_id,
        content_sha256=first.content_sha256,
        observations=observations,
        reconciliations=reconciliations,
        display_observation_id=first.observation_id,
        limitations=("Fixture release is synthetic.",),
    )


def _methodology_event(
    concept: NationalAccountsConceptV1,
) -> NationalAccountsMethodologyEventV1:
    return NationalAccountsMethodologyEventV1(
        event_key="us-nipa-2023-comprehensive-update",
        economy_code="US",
        kind=NationalAccountsMethodologyChangeKind.BENCHMARK_REVISION,
        affected_concept_ids=(concept.concept_id,),
        old_methodology_era_id="nipa-2018-benchmark",
        new_methodology_era_id="nipa-2023-benchmark",
        effective_reference_period="2017-Q1",
        benchmark_reference_period="2017",
        comparable_across_event=True,
        bridge_id="bea-nipa-2023-bridge",
        published_at_ns=ADVANCE_RELEASE,
        available_at_ns=ADVANCE_RELEASE,
        source_key="us.bea.api",
        source_request_id="request-bea-nipa-2023-benchmark",
        content_sha256="b" * 64,
        source_uri="https://www.bea.gov/news/2023-comprehensive-update",
        notes=("Comprehensive update fixture.",),
    )


def test_concept_distinguishes_growth_output_and_valuation_identity() -> None:
    annualized = _concept()
    simple = _concept(
        transformation=NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT
    )
    nominal = _concept(
        output_basis=NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE,
        source_series_id="bea-gdp-nominal",
    )

    assert (
        len({annualized.concept_id, simple.concept_id, nominal.concept_id}) == 3
    )
    assert (
        NationalAccountsConceptV1.from_json(annualized.to_json()) == annualized
    )


def test_concept_rejects_frequency_saar_and_component_substitution() -> None:
    with pytest.raises(ValueError, match="quarterly growth"):
        _concept(frequency=NationalAccountsFrequency.MONTHLY)
    with pytest.raises(ValueError, match="quarterly SAAR"):
        _concept(transformation=NationalAccountsTransformation.ANNUALIZED_LEVEL)
    with pytest.raises(ValueError, match="aggregate measure"):
        _concept(
            framework=NationalAccountsComponentFramework.EXPENDITURE,
            path=("household-consumption",),
        )
    with pytest.raises(ValueError, match="component measure"):
        _concept(measure=NationalAccountsMeasure.EXPENDITURE_COMPONENT)


def test_observation_round_trip_and_stage_initial_revision_rules() -> None:
    observation = _observation()
    assert (
        NationalAccountsObservationV1.from_dict(observation.to_dict())
        == observation
    )
    with pytest.raises(ValueError, match="stage-initial"):
        replace(
            observation,
            revision_kind=NationalAccountsRevisionKind.ROUTINE,
            observation_id="",
        )
    payload = observation.to_dict()
    payload["observation_id"] = (
        "national-accounts-observation:sha256:" + "0" * 64
    )
    with pytest.raises(ValueError, match="deterministic identity"):
        NationalAccountsObservationV1.from_dict(payload)


def test_vintage_chain_preserves_stage_and_point_in_time_values() -> None:
    initial = _observation()
    routine = _observation(
        value=2.3,
        published=SECOND_RELEASE,
        revision=1,
        revision_kind=NationalAccountsRevisionKind.ROUTINE,
        supersedes=initial.observation_id,
    )
    chain = NationalAccountsVintageChainV1((routine, initial))

    assert chain.initial == initial
    assert chain.as_known_at(ADVANCE_RELEASE + 1) == initial
    assert chain.as_known_at(SECOND_RELEASE + 1) == routine
    assert NationalAccountsVintageChainV1.from_dict(chain.to_dict()) == chain


def test_vintage_chain_rejects_cross_stage_rewrite() -> None:
    initial = _observation()
    final = _observation(
        stage=EconomicReleaseStage.FINAL,
        value=2.0,
        published=SECOND_RELEASE,
        revision=1,
        revision_kind=NationalAccountsRevisionKind.ROUTINE,
        supersedes=initial.observation_id,
    )
    with pytest.raises(ValueError, match="stage or concept"):
        NationalAccountsVintageChainV1((initial, final))


def test_growth_derivation_keeps_simple_and_annualized_formulas_distinct() -> (
    None
):
    level_concept = _concept(
        transformation=NationalAccountsTransformation.LEVEL
    )
    comparison = _observation(
        level_concept,
        event="us-gdp-2023q4-advance",
        period="2023-Q4",
        period_end=Q4_END,
        value=100.0,
        published=ADVANCE_RELEASE - 1_000,
    )
    current = _observation(level_concept, value=101.0)
    simple = NationalAccountsGrowthDerivationV1(
        target_concept=_concept(
            transformation=NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT
        ),
        current_level=current,
        comparison_level=comparison,
        vintage_basis=NationalAccountsDerivationVintageBasis.INITIAL_AT_STAGE,
        source_transformation_unavailable=True,
        claimed_stage_initial_actual=True,
        derived_value=1.0,
        tolerance=1e-12,
    )
    annualized = NationalAccountsGrowthDerivationV1(
        target_concept=_concept(),
        current_level=current,
        comparison_level=comparison,
        vintage_basis=NationalAccountsDerivationVintageBasis.INITIAL_AT_STAGE,
        source_transformation_unavailable=True,
        claimed_stage_initial_actual=True,
        derived_value=4.060401,
        tolerance=1e-9,
    )

    assert simple.derived_value == 1.0
    assert annualized.derived_value == pytest.approx(4.060401)
    assert (
        NationalAccountsGrowthDerivationV1.from_dict(annualized.to_dict())
        == annualized
    )


def test_growth_derivation_refuses_published_or_latest_level_fabrication() -> (
    None
):
    level_concept = _concept(
        transformation=NationalAccountsTransformation.LEVEL
    )
    comparison = _observation(
        level_concept,
        event="us-gdp-2023q4-advance",
        period="2023-Q4",
        period_end=Q4_END,
        value=100.0,
        published=ADVANCE_RELEASE - 1_000,
    )
    current = _observation(level_concept, value=101.0)
    target = _concept(
        transformation=NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT
    )
    with pytest.raises(ValueError, match="takes precedence"):
        NationalAccountsGrowthDerivationV1(
            target_concept=target,
            current_level=current,
            comparison_level=comparison,
            vintage_basis=NationalAccountsDerivationVintageBasis.INITIAL_AT_STAGE,
            source_transformation_unavailable=False,
            claimed_stage_initial_actual=True,
            derived_value=1.0,
            tolerance=1e-12,
        )
    with pytest.raises(ValueError, match="current levels cannot fabricate"):
        NationalAccountsGrowthDerivationV1(
            target_concept=target,
            current_level=current,
            comparison_level=comparison,
            vintage_basis=NationalAccountsDerivationVintageBasis.LATEST_CURRENT,
            source_transformation_unavailable=True,
            claimed_stage_initial_actual=True,
            derived_value=1.0,
            tolerance=1e-12,
        )


def test_growth_derivation_rejects_wrong_lag_or_methodology_era() -> None:
    level_concept = _concept(
        transformation=NationalAccountsTransformation.LEVEL
    )
    current = _observation(level_concept, value=101.0)
    too_old = _observation(
        level_concept,
        event="us-gdp-2023q3-advance",
        period="2023-Q3",
        period_end=Q3_END,
        value=100.0,
        published=ADVANCE_RELEASE - 1_000,
    )
    with pytest.raises(ValueError, match="required lag"):
        NationalAccountsGrowthDerivationV1(
            target_concept=_concept(
                transformation=(
                    NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT
                )
            ),
            current_level=current,
            comparison_level=too_old,
            vintage_basis=NationalAccountsDerivationVintageBasis.INITIAL_AT_STAGE,
            source_transformation_unavailable=True,
            claimed_stage_initial_actual=True,
            derived_value=1.0,
            tolerance=1e-12,
        )
    old_era = _observation(
        _concept(
            transformation=NationalAccountsTransformation.LEVEL,
            methodology_era_id="nipa-2018-benchmark",
        ),
        event="us-gdp-2023q4-advance",
        period="2023-Q4",
        period_end=Q4_END,
        value=100.0,
        published=ADVANCE_RELEASE - 1_000,
    )
    with pytest.raises(ValueError, match="source series identity"):
        NationalAccountsGrowthDerivationV1(
            target_concept=_concept(),
            current_level=current,
            comparison_level=old_era,
            vintage_basis=NationalAccountsDerivationVintageBasis.INITIAL_AT_STAGE,
            source_transformation_unavailable=True,
            claimed_stage_initial_actual=True,
            derived_value=4.060401,
            tolerance=1e-9,
        )


def test_triplet_accepts_simultaneous_previous_revision_at_stage_release() -> (
    None
):
    concept = _concept()
    actual = _observation(concept)
    previous_initial = _observation(
        concept,
        event="us-gdp-2023q4-final",
        period="2023-Q4",
        period_end=Q4_END,
        stage=EconomicReleaseStage.FINAL,
        value=3.4,
        published=ADVANCE_RELEASE - 20_000,
        source_release_id="bea-gdp-2023q4-final",
    )
    previous_revision = _observation(
        concept,
        event="us-gdp-2023q4-final",
        period="2023-Q4",
        period_end=Q4_END,
        stage=EconomicReleaseStage.FINAL,
        value=3.2,
        published=ADVANCE_RELEASE,
        available=ADVANCE_RELEASE,
        revision=1,
        revision_kind=NationalAccountsRevisionKind.SIMULTANEOUS_PREVIOUS,
        supersedes=previous_initial.observation_id,
        source_release_id="bea-gdp-2024q1-advance",
    )
    triplet = NationalAccountsReleaseTripletV1(
        target_event_key=actual.logical_event_key,
        actual_stage_initial=actual,
        previous_as_known=previous_revision,
        previous_as_known_at_ns=ADVANCE_RELEASE,
        expectation=_expectation(concept),
    )

    assert (triplet.actual, triplet.previous, triplet.forecast) == (
        2.5,
        3.2,
        2.2,
    )
    assert (
        NationalAccountsReleaseTripletV1.from_dict(triplet.to_dict()) == triplet
    )


def test_triplet_rejects_forecast_for_another_stage_or_period_projection() -> (
    None
):
    concept = _concept()
    actual = _observation(concept)
    previous = _observation(
        concept,
        event="us-gdp-2023q4-final",
        period="2023-Q4",
        period_end=Q4_END,
        stage=EconomicReleaseStage.FINAL,
        value=3.4,
        published=ADVANCE_RELEASE - 20_000,
        source_release_id="bea-gdp-2023q4-final",
    )
    with pytest.raises(ValueError, match="exact GDP stage"):
        NationalAccountsReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_stage_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=ADVANCE_RELEASE - 1,
            expectation=_expectation(concept, stage=EconomicReleaseStage.FINAL),
        )
    with pytest.raises(ValueError, match="period projection"):
        NationalAccountsReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_stage_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=ADVANCE_RELEASE - 1,
            expectation=_expectation(
                concept,
                kind=(
                    NationalAccountsExpectationKind.OFFICIAL_SURVEY_PERIOD_TARGET
                ),
            ),
        )


def test_unavailable_expectation_retains_exact_stage_without_fake_value() -> (
    None
):
    expectation = _expectation(
        _concept(), kind=NationalAccountsExpectationKind.UNAVAILABLE
    )
    assert expectation.expected_value is None
    assert not expectation.calendar_style_eligible
    assert (
        NationalAccountsExpectationV1.from_dict(expectation.to_dict())
        == expectation
    )


def test_release_sequence_keeps_advance_and_second_actuals_distinct() -> None:
    concept = _concept()
    advance = _observation(concept)
    second = _observation(
        concept,
        event="us-gdp-2024q1-second",
        stage=EconomicReleaseStage.SECOND,
        value=1.3,
        published=SECOND_RELEASE,
        source_release_id="bea-gdp-2024q1-second",
    )
    sequence = NationalAccountsReleaseSequenceV1(
        sequence_key="us-gdp-2024q1",
        releases=(_release((second,)), _release((advance,))),
    )

    assert sequence.release_for_stage(EconomicReleaseStage.ADVANCE).stage is (
        EconomicReleaseStage.ADVANCE
    )
    assert (
        sequence.release_for_stage(
            EconomicReleaseStage.SECOND
        ).display_observation.value
        == 1.3
    )
    assert (
        sequence.releases[0].logical_event_key
        != sequence.releases[1].logical_event_key
    )
    assert (
        NationalAccountsReleaseSequenceV1.from_dict(sequence.to_dict())
        == sequence
    )


def test_release_sequence_rejects_out_of_order_stage_publications() -> None:
    concept = _concept()
    final = _observation(
        concept,
        event="us-gdp-2024q1-final",
        stage=EconomicReleaseStage.FINAL,
        published=ADVANCE_RELEASE,
        source_release_id="bea-gdp-2024q1-final",
    )
    second = _observation(
        concept,
        event="us-gdp-2024q1-second",
        stage=EconomicReleaseStage.SECOND,
        published=SECOND_RELEASE,
        source_release_id="bea-gdp-2024q1-second",
    )
    with pytest.raises(ValueError, match="strictly chronological"):
        NationalAccountsReleaseSequenceV1(
            sequence_key="us-gdp-2024q1",
            releases=(_release((final,)), _release((second,))),
        )


def _component_observation(
    path: str,
    value: float,
    *,
    output_basis: NationalAccountsOutputBasis = (
        NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE
    ),
    transformation: NationalAccountsTransformation = (
        NationalAccountsTransformation.LEVEL
    ),
) -> NationalAccountsObservationV1:
    concept = _concept(
        transformation=transformation,
        output_basis=output_basis,
        measure=NationalAccountsMeasure.EXPENDITURE_COMPONENT,
        framework=NationalAccountsComponentFramework.EXPENDITURE,
        path=(path,),
        indicator_key=f"us-gdp-{path}",
        source_series_id=f"bea-gdp-{path}",
    )
    return _observation(concept, value=value)


def test_additive_component_table_reconciles_imports_with_negative_sign() -> (
    None
):
    aggregate = _observation(
        _concept(
            transformation=NationalAccountsTransformation.LEVEL,
            output_basis=NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE,
            source_series_id="bea-gdp-nominal",
        ),
        value=100.0,
    )
    entries = (
        NationalAccountsComponentEntryV1(
            _component_observation("consumption", 60.0),
            NationalAccountsComponentRole.ADDITIVE,
        ),
        NationalAccountsComponentEntryV1(
            _component_observation("investment", 25.0),
            NationalAccountsComponentRole.ADDITIVE,
        ),
        NationalAccountsComponentEntryV1(
            _component_observation("government", 20.0),
            NationalAccountsComponentRole.ADDITIVE,
        ),
        NationalAccountsComponentEntryV1(
            _component_observation("exports", 10.0),
            NationalAccountsComponentRole.ADDITIVE,
        ),
        NationalAccountsComponentEntryV1(
            _component_observation("imports", 15.0),
            NationalAccountsComponentRole.SUBTRACTIVE,
        ),
    )
    reconciliation = NationalAccountsComponentReconciliationV1(
        aggregate=aggregate,
        framework=NationalAccountsComponentFramework.EXPENDITURE,
        entries=entries,
        basis=NationalAccountsReconciliationBasis.ADDITIVE_LEVELS,
        coverage=NationalAccountsComponentCoverage.COMPLETE,
        missing_component_keys=(),
        official_table_id="bea-nipa-table-1.1.5",
        residual=0.0,
        tolerance=1e-12,
        reconciled=True,
    )

    assert (
        NationalAccountsComponentReconciliationV1.from_dict(
            reconciliation.to_dict()
        )
        == reconciliation
    )
    release = _release(
        (aggregate, *(entry.observation for entry in entries)),
        reconciliations=(reconciliation,),
    )
    assert NationalAccountsReleaseV1.from_dict(release.to_dict()) == release


def test_chain_volume_components_are_explicitly_non_additive() -> None:
    aggregate = _observation(
        _concept(transformation=NationalAccountsTransformation.LEVEL),
        value=100.0,
    )
    entries = (
        NationalAccountsComponentEntryV1(
            _component_observation(
                "consumption",
                70.0,
                output_basis=NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
            ),
            NationalAccountsComponentRole.ADDITIVE,
        ),
        NationalAccountsComponentEntryV1(
            _component_observation(
                "investment",
                30.0,
                output_basis=NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
            ),
            NationalAccountsComponentRole.ADDITIVE,
        ),
    )
    non_additive = NationalAccountsComponentReconciliationV1(
        aggregate=aggregate,
        framework=NationalAccountsComponentFramework.EXPENDITURE,
        entries=entries,
        basis=NationalAccountsReconciliationBasis.CHAIN_VOLUME_NON_ADDITIVE,
        coverage=NationalAccountsComponentCoverage.NON_ADDITIVE,
        missing_component_keys=(),
        official_table_id="bea-chain-volume-table",
        residual=None,
        tolerance=1e-12,
        reconciled=False,
    )
    assert (
        non_additive.coverage is NationalAccountsComponentCoverage.NON_ADDITIVE
    )
    with pytest.raises(ValueError, match="cannot be forced additive"):
        replace(
            non_additive,
            basis=NationalAccountsReconciliationBasis.ADDITIVE_LEVELS,
            coverage=NationalAccountsComponentCoverage.COMPLETE,
            residual=0.0,
            reconciled=True,
            reconciliation_id="",
        )


def test_source_published_growth_contributions_reconcile_chain_output() -> None:
    aggregate = _observation(_concept(), value=2.5)
    entries = (
        NationalAccountsComponentEntryV1(
            _component_observation(
                "domestic-demand",
                1.5,
                output_basis=NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
                transformation=(
                    NationalAccountsTransformation.CONTRIBUTION_PERCENTAGE_POINTS
                ),
            ),
            NationalAccountsComponentRole.ADDITIVE,
        ),
        NationalAccountsComponentEntryV1(
            _component_observation(
                "net-exports",
                1.0,
                output_basis=NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
                transformation=(
                    NationalAccountsTransformation.CONTRIBUTION_PERCENTAGE_POINTS
                ),
            ),
            NationalAccountsComponentRole.ADDITIVE,
        ),
    )
    reconciliation = NationalAccountsComponentReconciliationV1(
        aggregate=aggregate,
        framework=NationalAccountsComponentFramework.EXPENDITURE,
        entries=entries,
        basis=(
            NationalAccountsReconciliationBasis.SOURCE_PUBLISHED_CONTRIBUTIONS
        ),
        coverage=NationalAccountsComponentCoverage.COMPLETE,
        missing_component_keys=(),
        official_table_id="bea-real-gdp-contributions",
        residual=0.0,
        tolerance=1e-12,
        reconciled=True,
    )

    assert reconciliation.reconciled


def test_partial_and_unavailable_component_tables_cannot_claim_reconciliation() -> (
    None
):
    aggregate = _observation(
        _concept(
            transformation=NationalAccountsTransformation.LEVEL,
            output_basis=NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE,
            source_series_id="bea-gdp-nominal",
        ),
        value=100.0,
    )
    entry = NationalAccountsComponentEntryV1(
        _component_observation("consumption", 60.0),
        NationalAccountsComponentRole.ADDITIVE,
    )
    partial = NationalAccountsComponentReconciliationV1(
        aggregate=aggregate,
        framework=NationalAccountsComponentFramework.EXPENDITURE,
        entries=(entry,),
        basis=NationalAccountsReconciliationBasis.ADDITIVE_LEVELS,
        coverage=NationalAccountsComponentCoverage.PARTIAL,
        missing_component_keys=("investment",),
        official_table_id="bea-nipa-table-1.1.5",
        residual=None,
        tolerance=1e-12,
        reconciled=False,
    )
    unavailable = NationalAccountsComponentReconciliationV1(
        aggregate=aggregate,
        framework=NationalAccountsComponentFramework.INDUSTRY,
        entries=(),
        basis=NationalAccountsReconciliationBasis.UNAVAILABLE,
        coverage=NationalAccountsComponentCoverage.UNAVAILABLE,
        missing_component_keys=("official-table",),
        official_table_id=None,
        residual=None,
        tolerance=0.0,
        reconciled=False,
    )
    assert partial.missing_component_keys == ("investment",)
    assert unavailable.entries == ()


def test_release_rejects_component_reconciliation_from_other_evidence() -> None:
    aggregate = _observation(
        _concept(
            transformation=NationalAccountsTransformation.LEVEL,
            output_basis=NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE,
            source_series_id="bea-gdp-nominal",
        ),
        value=100.0,
    )
    foreign_component = _component_observation("consumption", 100.0)
    entry = NationalAccountsComponentEntryV1(
        foreign_component, NationalAccountsComponentRole.ADDITIVE
    )
    reconciliation = NationalAccountsComponentReconciliationV1(
        aggregate=aggregate,
        framework=NationalAccountsComponentFramework.EXPENDITURE,
        entries=(entry,),
        basis=NationalAccountsReconciliationBasis.ADDITIVE_LEVELS,
        coverage=NationalAccountsComponentCoverage.COMPLETE,
        missing_component_keys=(),
        official_table_id="bea-nipa-table-1.1.5",
        residual=0.0,
        tolerance=1e-12,
        reconciled=True,
    )
    with pytest.raises(ValueError, match="unretained value"):
        _release((aggregate,), reconciliations=(reconciliation,))


def test_benchmark_revision_set_retains_multiple_historical_stage_cells() -> (
    None
):
    concept = _concept()
    methodology = _methodology_event(concept)
    q3_initial = _observation(
        concept,
        event="us-gdp-2023q3-final",
        period="2023-Q3",
        period_end=Q3_END,
        stage=EconomicReleaseStage.FINAL,
        published=ADVANCE_RELEASE - 30_000,
        source_release_id="bea-gdp-2023q3-final",
    )
    q4_initial = _observation(
        concept,
        event="us-gdp-2023q4-final",
        period="2023-Q4",
        period_end=Q4_END,
        stage=EconomicReleaseStage.FINAL,
        published=ADVANCE_RELEASE - 20_000,
        source_release_id="bea-gdp-2023q4-final",
    )
    revisions = tuple(
        _observation(
            concept,
            event=initial.logical_event_key,
            period=initial.reference_period,
            period_end=initial.reference_period_end_ns,
            stage=initial.stage,
            value=value,
            published=ADVANCE_RELEASE,
            available=ADVANCE_RELEASE,
            revision=1,
            revision_kind=NationalAccountsRevisionKind.BENCHMARK,
            supersedes=initial.observation_id,
            source_release_id="bea-nipa-2023-benchmark",
        )
        for initial, value in ((q3_initial, 4.8), (q4_initial, 3.1))
    )
    benchmark = NationalAccountsBenchmarkRevisionV1(
        benchmark_event_key="us-nipa-2023-comprehensive-update",
        economy_code="US",
        benchmark_reference_period="2017",
        observations=revisions,
        methodology_event_id=methodology.event_id,
        published_at_ns=ADVANCE_RELEASE,
        available_at_ns=ADVANCE_RELEASE,
        source_key="us.bea.api",
        source_release_id="bea-nipa-2023-benchmark",
        source_request_id="request-bea-nipa-2023-benchmark",
        content_sha256="a" * 64,
        limitations=("Benchmark fixture is synthetic.",),
    )

    assert benchmark.reference_period_bounds_ns == (Q3_END, Q4_END)
    assert (
        NationalAccountsBenchmarkRevisionV1.from_dict(benchmark.to_dict())
        == benchmark
    )


def test_benchmark_revision_set_rejects_routine_revision() -> None:
    concept = _concept()
    initial = _observation(concept)
    routine = _observation(
        concept,
        value=2.4,
        revision=1,
        revision_kind=NationalAccountsRevisionKind.ROUTINE,
        supersedes=initial.observation_id,
    )
    with pytest.raises(ValueError, match="source or revision kind"):
        NationalAccountsBenchmarkRevisionV1(
            benchmark_event_key="us-nipa-2023-comprehensive-update",
            economy_code="US",
            benchmark_reference_period="2017",
            observations=(routine, replace(routine, observation_id="")),
            methodology_event_id="methodology-event-id",
            published_at_ns=ADVANCE_RELEASE,
            available_at_ns=ADVANCE_RELEASE + 1,
            source_key="us.bea.api",
            source_release_id="bea-gdp-2024q1-advance",
            source_request_id="request-bea-gdp-2024q1-advance",
            content_sha256="a" * 64,
            limitations=("Synthetic.",),
        )


def test_methodology_event_requires_benchmark_period_and_explicit_bridge() -> (
    None
):
    concept = _concept()
    event = _methodology_event(concept)
    assert (
        NationalAccountsMethodologyEventV1.from_dict(event.to_dict()) == event
    )
    with pytest.raises(ValueError, match="benchmark period"):
        replace(event, benchmark_reference_period=None, event_id="")
    with pytest.raises(ValueError, match="explicit bridge"):
        replace(event, bridge_id=None, event_id="")


def test_profiles_cover_21_economies_and_difficult_stage_cases() -> None:
    registry = load_packaged_official_source_registry()
    profiles = built_in_national_accounts_profiles(registry)
    audit = require_national_accounts_profile_coverage(registry, profiles)
    by_economy = {item.economy_code: item for item in profiles}

    assert len(profiles) == 21
    assert audit.complete
    assert EconomicReleaseStage.ADVANCE in by_economy["US"].supported_stages
    assert EconomicReleaseStage.SECOND in by_economy["JP"].supported_stages
    assert EconomicReleaseStage.FLASH in by_economy["EA"].supported_stages
    assert NationalAccountsFrequency.MONTHLY in (
        by_economy["GB"].supported_frequencies
    )
    assert "nipa-comprehensive-updates" in by_economy["US"].special_cases
    assert NationalAccountsProfileAuditV1.from_dict(audit.to_dict()) == audit


def test_profile_audit_fails_closed_on_source_or_stage_drift() -> None:
    registry = load_packaged_official_source_registry()
    profiles = list(built_in_national_accounts_profiles(registry))
    us_index = next(
        index
        for index, profile in enumerate(profiles)
        if profile.economy_code == "US"
    )
    us = profiles[us_index]
    profiles[us_index] = replace(us, source_key="us.fake", profile_id="")
    audit = audit_national_accounts_profiles(registry, profiles)
    assert not audit.complete
    assert "US" in audit.source_mismatches
    with pytest.raises(ValueError, match="coverage is incomplete"):
        require_national_accounts_profile_coverage(registry, profiles)
    with pytest.raises(ValueError, match="stage or revision semantics"):
        NationalAccountsEconomyProfileV1(
            **{
                **us.identity_payload(),
                "supported_stages": (EconomicReleaseStage.ADVANCE,),
                "profile_id": "",
            }
        )


def test_deterministic_concept_identity_fails_closed() -> None:
    payload = _concept().to_dict()
    payload["valuation_basis"] = "basic prices"
    with pytest.raises(ValueError, match="deterministic identity"):
        NationalAccountsConceptV1.from_dict(payload)

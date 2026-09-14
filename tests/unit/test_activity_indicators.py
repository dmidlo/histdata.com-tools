from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from histdatacom import market_context
from histdatacom.market_context.activity_indicators import (
    ActivityConceptV1,
    ActivityEconomyProfileV1,
    ActivityExpectationKind,
    ActivityExpectationV1,
    ActivityFrequency,
    ActivityIndicatorFamily,
    ActivityObservationV1,
    ActivityProfileAuditV1,
    ActivityReleasePackageV1,
    ActivityReleaseTripletV1,
    ActivityReleaseV1,
    ActivityRevisionKind,
    ActivitySeasonalBasis,
    ActivityStatisticKind,
    ActivityTransformation,
    ActivityUnitKind,
    ActivityUnsupportedReason,
    ActivityVariant,
    ActivityVintageChainV1,
    ActivityWindowKind,
    ActivityWindowV1,
    UnsupportedActivityGapV1,
    audit_activity_profiles,
    built_in_activity_profiles,
    require_activity_profile_coverage,
)
from histdatacom.market_context.economic_calendar import (
    EconomicEventFamily,
    EconomicReleaseStage,
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
RELEASE = _ns(2024, 3, 14)


def _concept(
    *,
    family: ActivityIndicatorFamily = ActivityIndicatorFamily.RETAIL_SALES,
    statistic: ActivityStatisticKind = ActivityStatisticKind.SALES,
    transformation: ActivityTransformation = (
        ActivityTransformation.MONTH_OVER_MONTH_PERCENT
    ),
    frequency: ActivityFrequency = ActivityFrequency.MONTHLY,
    variant: ActivityVariant = ActivityVariant.HEADLINE,
    path: tuple[str, ...] = (),
    window: ActivityWindowV1 | None = None,
    indicator_key: str = "us-retail-sales",
    source_series_id: str = "census-retail-sales",
    producer_defines_percentage: bool = False,
) -> ActivityConceptV1:
    unit_kind = ActivityUnitKind.COUNT
    unit = "units"
    currency: str | None = None
    if transformation in {
        ActivityTransformation.MONTH_OVER_MONTH_PERCENT,
        ActivityTransformation.QUARTER_OVER_QUARTER_PERCENT,
        ActivityTransformation.YEAR_OVER_YEAR_PERCENT,
        ActivityTransformation.THREE_MONTH_OVER_THREE_MONTH_PERCENT,
        ActivityTransformation.NET_BALANCE_PERCENT,
    }:
        unit_kind = ActivityUnitKind.PERCENT
        unit = "percent"
    elif transformation is ActivityTransformation.INDEX_LEVEL:
        unit_kind = ActivityUnitKind.INDEX_POINTS
        unit = "index-points"
    elif transformation is ActivityTransformation.DIFFUSION_INDEX_LEVEL:
        unit_kind = ActivityUnitKind.DIFFUSION_INDEX_POINTS
        unit = "diffusion-index-points"
    elif statistic in {
        ActivityStatisticKind.VALUE,
        ActivityStatisticKind.BALANCE,
        ActivityStatisticKind.ORDERS,
        ActivityStatisticKind.SALES,
    }:
        unit_kind = ActivityUnitKind.CURRENCY
        unit = "usd"
        currency = "USD"
    return ActivityConceptV1(
        economy_code="US",
        indicator_key=indicator_key,
        source_series_id=source_series_id,
        official_program="U.S. Census official release",
        family=family,
        statistic_kind=statistic,
        variant=variant,
        transformation=transformation,
        frequency=frequency,
        seasonal_basis=ActivitySeasonalBasis.SEASONALLY_ADJUSTED,
        unit_kind=unit_kind,
        unit=unit,
        scale=1.0,
        currency_code=currency,
        component_path=path,
        window=window or ActivityWindowV1.single_period(),
        producer_defines_percentage=producer_defines_percentage,
        methodology_era_id="census-era-2024",
    )


def _observation(
    concept: ActivityConceptV1 | None = None,
    *,
    event: str = "us-retail-sales-2024-02",
    period: str = "2024-02",
    period_end: int = FEB_END,
    value: float = 0.6,
    published: int = RELEASE,
    available: int | None = None,
    revision: int = 0,
    revision_kind: ActivityRevisionKind = ActivityRevisionKind.INITIAL,
    supersedes: str | None = None,
    source_key: str = "us.census.economic-indicators",
    source_release_id: str = "census-retail-2024-03",
    content_sha256: str = "a" * 64,
) -> ActivityObservationV1:
    return ActivityObservationV1(
        concept=concept or _concept(),
        logical_event_key=event,
        reference_period=period,
        reference_period_end_ns=period_end,
        stage=EconomicReleaseStage.INITIAL,
        value=value,
        raw_lexical=str(value),
        published_at_ns=published,
        available_at_ns=published + 1 if available is None else available,
        source_key=source_key,
        source_release_id=source_release_id,
        source_request_id=f"request-{source_release_id}",
        content_sha256=content_sha256,
        revision_sequence=revision,
        revision_kind=revision_kind,
        supersedes_observation_id=supersedes,
        limitations=("Fixture evidence is synthetic.",),
    )


def _expectation(
    concept: ActivityConceptV1,
    *,
    event: str = "us-retail-sales-2024-02",
    period: str = "2024-02",
    kind: ActivityExpectationKind = ActivityExpectationKind.EVENT_CONSENSUS,
) -> ActivityExpectationV1:
    unavailable = kind is ActivityExpectationKind.UNAVAILABLE
    return ActivityExpectationV1(
        target_event_key=event,
        concept=concept,
        reference_period=period,
        kind=kind,
        expected_value=None if unavailable else 0.4,
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
    observation: ActivityObservationV1,
    *,
    package_key: str = "us-retail-2024-03",
) -> ActivityReleaseV1:
    return ActivityReleaseV1(
        package_key=package_key,
        logical_event_key=observation.logical_event_key,
        family=observation.concept.family,
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


def test_indicator_families_map_to_five_canonical_event_families() -> None:
    assert ActivityIndicatorFamily.RETAIL_SALES.event_family is (
        EconomicEventFamily.RETAIL_CONSUMPTION
    )
    assert ActivityIndicatorFamily.DURABLE_GOODS_ORDERS.event_family is (
        EconomicEventFamily.INDUSTRIAL_PRODUCTION
    )
    assert ActivityIndicatorFamily.TRADE_BALANCE.event_family is (
        EconomicEventFamily.TRADE_EXTERNAL
    )
    assert ActivityIndicatorFamily.HOUSING_STARTS.event_family is (
        EconomicEventFamily.HOUSING
    )
    assert ActivityIndicatorFamily.BUSINESS_CONFIDENCE.event_family is (
        EconomicEventFamily.CONFIDENCE_SURVEY
    )


def test_activity_contracts_are_available_from_the_public_facade() -> None:
    assert market_context.ActivityConceptV1 is ActivityConceptV1
    assert market_context.ActivityReleasePackageV1 is ActivityReleasePackageV1
    assert market_context.UnsupportedActivityGapV1 is UnsupportedActivityGapV1
    assert (
        market_context.built_in_activity_profiles is built_in_activity_profiles
    )


def test_concept_keeps_balance_value_and_growth_distinct() -> None:
    balance = _concept(
        family=ActivityIndicatorFamily.TRADE_BALANCE,
        statistic=ActivityStatisticKind.BALANCE,
        transformation=ActivityTransformation.BALANCE_LEVEL,
        indicator_key="us-trade-balance",
        source_series_id="census-trade-balance",
    )
    growth = _concept(
        family=ActivityIndicatorFamily.TRADE_BALANCE,
        statistic=ActivityStatisticKind.BALANCE,
        transformation=ActivityTransformation.YEAR_OVER_YEAR_PERCENT,
        indicator_key="us-trade-balance-yoy",
        source_series_id="census-trade-balance",
    )

    assert balance.concept_id != growth.concept_id
    assert ActivityConceptV1.from_json(balance.to_json()) == balance


def test_concept_rejects_unit_frequency_and_external_balance_mismatch() -> None:
    concept = _concept()
    with pytest.raises(ValueError, match="percent unit"):
        replace(concept, unit_kind=ActivityUnitKind.COUNT, concept_id="")
    with pytest.raises(ValueError, match="monthly frequency"):
        replace(concept, frequency=ActivityFrequency.QUARTERLY, concept_id="")
    with pytest.raises(ValueError, match="balance statistic"):
        _concept(
            family=ActivityIndicatorFamily.CURRENT_ACCOUNT,
            statistic=ActivityStatisticKind.VALUE,
            transformation=ActivityTransformation.LEVEL,
            indicator_key="us-current-account",
            source_series_id="bea-current-account",
        )


def test_diffusion_index_is_not_silently_treated_as_percent() -> None:
    diffusion = _concept(
        family=ActivityIndicatorFamily.OFFICIAL_DIFFUSION_SURVEY,
        statistic=ActivityStatisticKind.DIFFUSION_INDEX,
        transformation=ActivityTransformation.DIFFUSION_INDEX_LEVEL,
        indicator_key="us-official-diffusion-index",
        source_series_id="official-diffusion-index",
    )
    assert diffusion.unit_kind is ActivityUnitKind.DIFFUSION_INDEX_POINTS
    with pytest.raises(ValueError, match="diffusion-index identity"):
        replace(
            diffusion,
            transformation=ActivityTransformation.INDEX_LEVEL,
            unit_kind=ActivityUnitKind.INDEX_POINTS,
            concept_id="",
        )


def test_net_balance_percent_requires_explicit_producer_definition() -> None:
    survey = _concept(
        family=ActivityIndicatorFamily.BUSINESS_CONFIDENCE,
        statistic=ActivityStatisticKind.NET_BALANCE,
        transformation=ActivityTransformation.NET_BALANCE_PERCENT,
        variant=ActivityVariant.OFFICIAL_SURVEY_AGGREGATE,
        indicator_key="us-official-business-net-balance",
        source_series_id="official-business-net-balance",
        producer_defines_percentage=True,
    )
    assert survey.producer_defines_percentage
    with pytest.raises(ValueError, match="producer definition"):
        replace(survey, producer_defines_percentage=False, concept_id="")


def test_exact_rolling_cumulative_and_three_month_windows() -> None:
    rolling = ActivityWindowV1(
        kind=ActivityWindowKind.TRAILING,
        periods=12,
        anchor=None,
        overlapping=True,
    )
    cumulative = ActivityWindowV1(
        kind=ActivityWindowKind.CUMULATIVE_CALENDAR_YEAR,
        periods=2,
        anchor="january",
        overlapping=False,
    )
    three_month = ActivityWindowV1(
        kind=ActivityWindowKind.THREE_MONTH_OVER_THREE_MONTH,
        periods=3,
        anchor=None,
        overlapping=True,
    )
    assert ActivityWindowV1.from_dict(rolling.to_dict()) == rolling
    assert (
        _concept(
            transformation=ActivityTransformation.ROLLING_SUM,
            window=rolling,
        ).window.periods
        == 12
    )
    assert (
        _concept(
            transformation=ActivityTransformation.CUMULATIVE_SUM,
            window=cumulative,
        ).window.anchor
        == "january"
    )
    assert (
        _concept(
            transformation=(
                ActivityTransformation.THREE_MONTH_OVER_THREE_MONTH_PERCENT
            ),
            window=three_month,
        ).window.kind
        is ActivityWindowKind.THREE_MONTH_OVER_THREE_MONTH
    )
    with pytest.raises(ValueError, match="aggregate window"):
        _concept(window=rolling)


def test_component_path_is_part_of_exact_identity() -> None:
    component = _concept(
        variant=ActivityVariant.COMPONENT,
        path=("building-materials",),
        indicator_key="us-retail-building-materials",
        source_series_id="census-retail-building-materials",
    )
    assert component.component_path == ("building-materials",)
    with pytest.raises(ValueError, match="component path"):
        replace(component, component_path=(), concept_id="")


def test_proprietary_pmi_is_an_explicit_non_observable_gap() -> None:
    gap = UnsupportedActivityGapV1(
        economy_code="US",
        indicator_key="us-proprietary-pmi",
        family=ActivityIndicatorFamily.BUSINESS_CONFIDENCE,
        requested_label="Purchasing Managers Index",
        producer_name="private survey vendor",
        reason=ActivityUnsupportedReason.PROPRIETARY_SOURCE_UNQUALIFIED,
        official_replacement_concept_id=None,
        notes=("No qualified official source.",),
    )
    assert UnsupportedActivityGapV1.from_dict(gap.to_dict()) == gap
    with pytest.raises(ValueError, match="producer identity"):
        replace(gap, producer_name=None, gap_id="")
    with pytest.raises(TypeError, match="official activity concept"):
        ActivityObservationV1(
            concept=gap,  # type: ignore[arg-type]
            logical_event_key="us-proprietary-pmi-2024-02",
            reference_period="2024-02",
            reference_period_end_ns=FEB_END,
            stage=EconomicReleaseStage.INITIAL,
            value=50.0,
            raw_lexical="50.0",
            published_at_ns=RELEASE,
            available_at_ns=RELEASE,
            source_key="us.fake",
            source_release_id="fake-release",
            source_request_id="fake-request",
            content_sha256="a" * 64,
            revision_sequence=0,
            revision_kind=ActivityRevisionKind.INITIAL,
            supersedes_observation_id=None,
            limitations=("Must fail.",),
        )


def test_observation_round_trip_and_revision_rules() -> None:
    observation = _observation()
    assert ActivityObservationV1.from_dict(observation.to_dict()) == observation
    with pytest.raises(ValueError, match="revision evidence"):
        replace(
            observation,
            revision_kind=ActivityRevisionKind.ROUTINE,
            observation_id="",
        )
    payload = observation.to_dict()
    payload["observation_id"] = "activity-observation:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="deterministic identity"):
        ActivityObservationV1.from_dict(payload)


def test_vintage_chain_retains_simultaneous_and_later_revisions() -> None:
    initial = _observation()
    simultaneous = _observation(
        value=0.4,
        published=RELEASE + 100,
        revision=1,
        revision_kind=ActivityRevisionKind.SIMULTANEOUS_PREVIOUS,
        supersedes=initial.observation_id,
    )
    benchmark = _observation(
        value=0.3,
        published=RELEASE + 200,
        revision=2,
        revision_kind=ActivityRevisionKind.BENCHMARK,
        supersedes=simultaneous.observation_id,
    )
    chain = ActivityVintageChainV1((benchmark, initial, simultaneous))

    assert chain.initial == initial
    assert chain.as_known_at(RELEASE + 1) == initial
    assert chain.as_known_at(RELEASE + 201) == benchmark
    assert ActivityVintageChainV1.from_dict(chain.to_dict()) == chain


def test_vintage_chain_rejects_transformation_substitution() -> None:
    initial = _observation()
    yoy = _observation(
        _concept(transformation=ActivityTransformation.YEAR_OVER_YEAR_PERCENT),
        revision=1,
        revision_kind=ActivityRevisionKind.ROUTINE,
        supersedes=initial.observation_id,
    )
    with pytest.raises(ValueError, match="event or concept"):
        ActivityVintageChainV1((initial, yoy))


def test_triplet_accepts_simultaneous_previous_revision() -> None:
    concept = _concept()
    actual = _observation(concept)
    previous_initial = _observation(
        concept,
        event="us-retail-sales-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=-0.8,
        published=RELEASE - 10_000,
    )
    previous_revision = _observation(
        concept,
        event="us-retail-sales-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=-1.0,
        published=RELEASE,
        available=RELEASE,
        revision=1,
        revision_kind=ActivityRevisionKind.SIMULTANEOUS_PREVIOUS,
        supersedes=previous_initial.observation_id,
    )
    triplet = ActivityReleaseTripletV1(
        target_event_key=actual.logical_event_key,
        actual_initial=actual,
        previous_as_known=previous_revision,
        previous_as_known_at_ns=RELEASE,
        expectation=_expectation(concept),
    )

    assert (triplet.actual, triplet.previous, triplet.forecast) == (
        0.6,
        -1.0,
        0.4,
    )
    assert ActivityReleaseTripletV1.from_dict(triplet.to_dict()) == triplet


def test_triplet_rejects_window_or_nonadjacent_previous_period() -> None:
    actual = _observation()
    previous = _observation(
        actual.concept,
        event="us-retail-sales-2023-12",
        period="2023-12",
        period_end=_ns(2023, 12, 31),
        published=RELEASE - 10_000,
    )
    with pytest.raises(ValueError, match="immediately preceding"):
        ActivityReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(actual.concept),
        )
    trailing = ActivityWindowV1(
        kind=ActivityWindowKind.TRAILING,
        periods=12,
        anchor=None,
        overlapping=True,
    )
    wrong_window = _concept(
        transformation=ActivityTransformation.ROLLING_SUM,
        window=trailing,
    )
    adjacent = replace(
        previous,
        reference_period="2024-01",
        reference_period_end_ns=JAN_END,
        observation_id="",
    )
    with pytest.raises(ValueError, match="exact released concept"):
        ActivityReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=adjacent,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(wrong_window),
        )


def test_period_projection_and_unavailable_forecast_stay_distinct() -> None:
    actual = _observation()
    previous = _observation(
        actual.concept,
        event="us-retail-sales-2024-01",
        period="2024-01",
        period_end=JAN_END,
        published=RELEASE - 10_000,
    )
    with pytest.raises(ValueError, match="period projection"):
        ActivityReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(
                actual.concept,
                kind=ActivityExpectationKind.OFFICIAL_SURVEY_PERIOD_TARGET,
            ),
        )
    unavailable = _expectation(
        actual.concept, kind=ActivityExpectationKind.UNAVAILABLE
    )
    assert unavailable.expected_value is None
    assert ActivityExpectationV1.from_dict(unavailable.to_dict()) == unavailable


def _trade_observation(
    family: ActivityIndicatorFamily,
    statistic: ActivityStatisticKind,
    transformation: ActivityTransformation,
    value: float,
) -> ActivityObservationV1:
    slug = family.value
    concept = _concept(
        family=family,
        statistic=statistic,
        transformation=transformation,
        indicator_key=f"us-{slug}",
        source_series_id=f"census-{slug}",
    )
    return _observation(
        concept,
        event=f"us-{slug}-2024-01",
        period="2024-01",
        period_end=JAN_END,
        value=value,
        source_key="us.census.international-trade",
        source_release_id="census-trade-2024-03",
    )


def test_release_package_preserves_import_export_and_balance_events() -> None:
    exports = _trade_observation(
        ActivityIndicatorFamily.EXPORTS,
        ActivityStatisticKind.VALUE,
        ActivityTransformation.LEVEL,
        260.0,
    )
    imports = _trade_observation(
        ActivityIndicatorFamily.IMPORTS,
        ActivityStatisticKind.VALUE,
        ActivityTransformation.LEVEL,
        330.0,
    )
    balance = _trade_observation(
        ActivityIndicatorFamily.TRADE_BALANCE,
        ActivityStatisticKind.BALANCE,
        ActivityTransformation.BALANCE_LEVEL,
        -70.0,
    )
    releases = tuple(
        _release(item, package_key="us-trade-2024-03")
        for item in (exports, imports, balance)
    )
    package = ActivityReleasePackageV1(
        package_key="us-trade-2024-03",
        releases=releases,
        limitations=("Fixture package is synthetic.",),
    )

    assert (
        package.release_for_family(
            ActivityIndicatorFamily.TRADE_BALANCE
        ).display_observation.value
        == -70.0
    )
    assert len({item.logical_event_key for item in package.releases}) == 3
    assert ActivityReleasePackageV1.from_dict(package.to_dict()) == package


def test_release_package_rejects_collapsed_event_or_mixed_evidence() -> None:
    exports = _trade_observation(
        ActivityIndicatorFamily.EXPORTS,
        ActivityStatisticKind.VALUE,
        ActivityTransformation.LEVEL,
        260.0,
    )
    export_release = _release(exports, package_key="us-trade-2024-03")
    with pytest.raises(ValueError, match="collapses distinct events"):
        ActivityReleasePackageV1(
            package_key="us-trade-2024-03",
            releases=(export_release, export_release),
            limitations=("Must fail.",),
        )
    imports = _trade_observation(
        ActivityIndicatorFamily.IMPORTS,
        ActivityStatisticKind.VALUE,
        ActivityTransformation.LEVEL,
        330.0,
    )
    late_import_observation = replace(
        imports,
        published_at_ns=RELEASE + 1,
        available_at_ns=RELEASE + 1,
        observation_id="",
    )
    late_imports = _release(
        late_import_observation,
        package_key="us-trade-2024-03",
    )
    with pytest.raises(ValueError, match="publication evidence"):
        ActivityReleasePackageV1(
            package_key="us-trade-2024-03",
            releases=(export_release, late_imports),
            limitations=("Must fail.",),
        )


def test_release_can_retain_headline_and_component_without_merging_identity() -> (
    None
):
    headline = _observation()
    component = _observation(
        _concept(
            variant=ActivityVariant.COMPONENT,
            path=("building-materials",),
            indicator_key="us-retail-building-materials",
            source_series_id="census-retail-building-materials",
        ),
        value=0.2,
    )
    release = replace(
        _release(headline),
        observations=(headline, component),
        release_id="",
    )

    assert len(release.observations) == 2
    assert release.display_observation == headline
    assert ActivityReleaseV1.from_dict(release.to_dict()) == release


def test_weekly_triplet_requires_exactly_seven_days() -> None:
    weekly = _concept(
        family=ActivityIndicatorFamily.HOUSE_PRICES,
        statistic=ActivityStatisticKind.PRICE,
        transformation=ActivityTransformation.INDEX_LEVEL,
        frequency=ActivityFrequency.WEEKLY,
        indicator_key="us-weekly-house-price-index",
        source_series_id="official-weekly-house-price-index",
    )
    actual = _observation(
        weekly,
        event="us-weekly-house-price-2024-03-08",
        period="2024-03-08",
        period_end=_ns(2024, 3, 8),
    )
    previous = _observation(
        weekly,
        event="us-weekly-house-price-2024-02-23",
        period="2024-02-23",
        period_end=_ns(2024, 2, 23),
        published=RELEASE - 10_000,
    )
    with pytest.raises(ValueError, match="immediately preceding"):
        ActivityReleaseTripletV1(
            target_event_key=actual.logical_event_key,
            actual_initial=actual,
            previous_as_known=previous,
            previous_as_known_at_ns=RELEASE - 1,
            expectation=_expectation(
                weekly,
                event=actual.logical_event_key,
                period=actual.reference_period,
            ),
        )


def test_profiles_cover_all_21_economies_families_and_private_gaps() -> None:
    registry = load_packaged_official_source_registry()
    profiles = built_in_activity_profiles(registry)
    audit = require_activity_profile_coverage(registry, profiles)
    by_economy = {item.economy_code: item for item in profiles}

    assert len(profiles) == 21
    assert audit.complete
    assert len(by_economy["US"].source_owners) == 5
    assert set(by_economy["US"].required_indicator_families) == set(
        ActivityIndicatorFamily
    )
    assert by_economy["JP"].proprietary_gaps[0].reason is (
        ActivityUnsupportedReason.PROPRIETARY_SOURCE_UNQUALIFIED
    )
    assert "retail-control-group" in by_economy["US"].special_cases
    assert "official-boj-tankan-versus-private-pmi" in (
        by_economy["JP"].special_cases
    )
    assert ActivityProfileAuditV1.from_dict(audit.to_dict()) == audit


def test_profile_audit_fails_closed_on_owner_or_semantic_drift() -> None:
    registry = load_packaged_official_source_registry()
    profiles = list(built_in_activity_profiles(registry))
    us_index = next(
        index
        for index, profile in enumerate(profiles)
        if profile.economy_code == "US"
    )
    us = profiles[us_index]
    bad_owner = replace(us.source_owners[0], source_key="us.fake", owner_id="")
    profiles[us_index] = replace(
        us,
        source_owners=(bad_owner, *us.source_owners[1:]),
        profile_id="",
    )
    audit = audit_activity_profiles(registry, profiles)
    assert not audit.complete
    assert any(item.startswith("US:") for item in audit.source_mismatches)
    with pytest.raises(ValueError, match="coverage is incomplete"):
        require_activity_profile_coverage(registry, profiles)
    with pytest.raises(ValueError, match="cross-family mechanics"):
        ActivityEconomyProfileV1(
            economy_code=us.economy_code,
            source_owners=us.source_owners,
            required_indicator_families=us.required_indicator_families,
            proprietary_gaps=us.proprietary_gaps,
            supports_exact_windows=False,
            supports_release_packages=True,
            supports_simultaneous_previous_revisions=True,
            special_cases=us.special_cases,
            rationale=us.rationale,
        )


def test_deterministic_concept_identity_fails_closed() -> None:
    payload = _concept().to_dict()
    payload["official_program"] = "A different official program"
    with pytest.raises(ValueError, match="deterministic identity"):
        ActivityConceptV1.from_dict(payload)

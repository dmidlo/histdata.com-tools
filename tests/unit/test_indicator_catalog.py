"""Tests for the canonical economic-indicator and coverage catalog."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from histdatacom.market_context import (
    SCOPED_ECONOMIES,
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicExpectedOccurrenceStatus,
    EconomicExpectedOccurrenceV1,
    EconomicExpectedReleaseRuleV1,
    EconomicIndicatorCatalogEntryV1,
    EconomicIndicatorCatalogV1,
    EconomicIndicatorCoverageAuditV1,
    EconomicIndicatorFrequency,
    EconomicIndicatorLineageKind,
    EconomicIndicatorLineageV1,
    EconomicIndicatorMethodologyEraV1,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicTimePrecision,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    audit_economic_indicator_coverage,
    build_official_indicator_catalog,
    load_packaged_official_source_registry,
    read_economic_indicator_catalog,
    validate_indicator_catalog_sources,
    write_economic_indicator_catalog,
)

SECOND_NS = 1_000_000_000
START_NS = 1_700_000_000 * SECOND_NS
END_NS = START_NS + 100 * SECOND_NS


@pytest.fixture(scope="module")
def catalog() -> EconomicIndicatorCatalogV1:
    return build_official_indicator_catalog(
        load_packaged_official_source_registry()
    )


def _qualified_catalog(
    catalog: EconomicIndicatorCatalogV1,
) -> EconomicIndicatorCatalogV1:
    original = catalog.entry("us.consumer-and-producer-prices")
    original_era = original.methodology_eras[0]
    era = replace(
        original_era,
        source_series_ids=("CUUR0000SA0",),
        source_table_ids=("cpi-table-1",),
        source_release_ids=("cpi-news-release",),
        unit="index",
        seasonal_basis="not-seasonally-adjusted",
        reference_period_convention="calendar month",
        notes=("Exact CPI fixture semantics.",),
        era_id="",
    )
    original_rule = original.expected_release_rules[0]
    rule = replace(
        original_rule,
        frequency=EconomicIndicatorFrequency.MONTHLY,
        time_precision=EconomicTimePrecision.EXACT_MINUTE,
        rule_description="Monthly CPI publication from the official BLS calendar.",
        qualified=True,
        qualification_evidence=(
            "https://www.bls.gov/schedule/news_release/cpi.htm",
        ),
        limitations=("Holiday shifts are represented as occurrence states.",),
        rule_id="",
    )
    entry = replace(
        original,
        frequency=EconomicIndicatorFrequency.MONTHLY,
        reference_period_convention="calendar month",
        normal_schedule_rule="Official monthly BLS release calendar.",
        unit="index",
        seasonal_basis="not-seasonally-adjusted",
        methodology_eras=(era,),
        expected_release_rules=(rule,),
        limitations=("Fixture does not qualify pre-1970 CPI history.",),
        entry_id="",
    )
    return replace(
        catalog,
        entries=tuple(
            entry if item.indicator_key == entry.indicator_key else item
            for item in catalog.entries
        ),
        catalog_id="",
    )


def _occurrence(
    catalog: EconomicIndicatorCatalogV1,
    suffix: str,
    offset: int,
    status: EconomicExpectedOccurrenceStatus,
    *,
    reason: str | None = None,
) -> EconomicExpectedOccurrenceV1:
    entry = catalog.entry("us.consumer-and-producer-prices")
    return EconomicExpectedOccurrenceV1(
        logical_event_key=f"us.cpi.2026-{suffix}",
        indicator_key=entry.indicator_key,
        rule_id=entry.expected_release_rules[0].rule_id,
        reference_period=f"2026-{suffix}",
        reference_period_end_ns=START_NS + (offset - 1) * SECOND_NS,
        expected_for_ns=START_NS + offset * SECOND_NS,
        status=status,
        status_known_at_ns=START_NS + offset * SECOND_NS,
        source_key=entry.legal_producer_source_key,
        source_occurrence_id=f"cpi.2026-{suffix}",
        evidence_sha256=str(offset % 10) * 64,
        reason=reason,
    )


def _utc_text(value: int) -> str:
    return datetime.fromtimestamp(
        value / SECOND_NS, tz=timezone.utc
    ).isoformat()


def _release(
    logical_event_key: str,
    offset: int,
    *,
    series_key: str = "us.consumer-and-producer-prices",
) -> EconomicCalendarReleaseV1:
    published = START_NS + offset * SECOND_NS
    digest = str(offset % 10) * 64
    source = MarketContextSourceV1(
        name="U.S. Bureau of Labor Statistics fixture",
        source_version="2026-09-13",
        retrieved_at_ns=published,
        content_sha256=digest,
        adapter_name="bls-cpi-fixture",
        adapter_version="1",
        license_name="Public official fixture",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute BLS.",),
        limitations=("Fixture only.",),
        source_uri="https://example.invalid/bls-cpi",
    )
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key=series_key,
        series_version="v1",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="U.S. Bureau of Labor Statistics",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="consumer-and-producer-prices",
        source_series_id="cuur0000sa0",
        source_table_id="cpi-table-1",
        source_release_id=f"release.{offset}",
        source_request_id=f"request.{offset}",
        title="Consumer Price Index",
        reference_period="2026-07",
        reference_period_end_ns=published - SECOND_NS,
        frequency="monthly",
        seasonality="not-seasonally-adjusted",
        unit="index",
        scale=1.0,
        base="1982-84=100",
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.RELEASED,
        scheduled_for_ns=published,
        scheduled_lexical=_utc_text(published),
        released_at_ns=published,
        released_lexical=_utc_text(published),
        first_observed_at_ns=published,
        available_at_ns=published,
        source_timezone="UTC",
        timezone_evidence="Official UTC fixture.",
        time_precision=EconomicTimePrecision.EXACT_SECOND,
        precision=MarketContextPrecision.EXACT,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=source,
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        limitations=("Fixture only.",),
        actual_value=308.1,
        actual_lexical="308.1",
        content_sha256=digest,
    )


def test_bootstrap_catalog_covers_every_reviewed_economy_family_cell(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    assert catalog.scoped_economies == SCOPED_ECONOMIES
    assert len(catalog.entries) == len(SCOPED_ECONOMIES) * len(
        EconomicEventFamily
    )
    assert {
        (entry.economy_code, entry.event_family) for entry in catalog.entries
    } == {
        (economy, family)
        for economy in SCOPED_ECONOMIES
        for family in EconomicEventFamily
    }
    assert all(
        not entry.has_qualified_expected_model for entry in catalog.entries
    )
    assert (
        catalog.entry(
            "us.consumer-and-producer-prices"
        ).legal_producer_source_key
        == "us.bls.public-data"
    )
    assert EconomicIndicatorCatalogV1.from_json(catalog.to_json()) == catalog
    validate_indicator_catalog_sources(
        catalog, load_packaged_official_source_registry()
    )


def test_exact_indicator_semantics_and_expected_rule_are_identity_bound(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    qualified = _qualified_catalog(catalog)
    entry = qualified.entry("us.consumer-and-producer-prices")
    assert entry.frequency is EconomicIndicatorFrequency.MONTHLY
    assert entry.methodology_eras[0].source_series_ids == ("CUUR0000SA0",)
    assert entry.expected_release_rules[0].qualified
    assert qualified.catalog_id != catalog.catalog_id

    payload = entry.to_dict()
    payload["display_name"] = "Tampered CPI"
    with pytest.raises(ValueError, match="entry_id"):
        EconomicIndicatorCatalogEntryV1.from_dict(payload)


def test_catalog_rejects_missing_matrix_cells_and_registry_drift(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    with pytest.raises(ValueError, match="every economy/family cell"):
        replace(catalog, entries=catalog.entries[1:], catalog_id="")
    with pytest.raises(ValueError, match="registry identity differs"):
        validate_indicator_catalog_sources(
            replace(
                catalog,
                source_registry_id="official-source-registry:sha256:"
                + "0" * 64,
                catalog_id="",
            ),
            load_packaged_official_source_registry(),
        )


def test_lineage_retains_rename_rebase_split_and_rejects_cycles(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    original = catalog.entry("us.consumer-and-producer-prices")
    era = EconomicIndicatorMethodologyEraV1(
        era_key="us.cpi-v2.2026",
        effective_from="2026-01-01",
        effective_to=None,
        source_series_ids=("cpi-v2",),
        source_table_ids=("table-v2",),
        source_release_ids=("release-v2",),
        unit="index",
        scale=1.0,
        seasonal_basis="not-seasonally-adjusted",
        reference_period_convention="calendar month",
        methodology_evidence=("https://example.invalid/cpi-methodology",),
        notes=("Rebased successor fixture.",),
    )
    rule = EconomicExpectedReleaseRuleV1(
        rule_key="us.cpi-v2.monthly",
        frequency=EconomicIndicatorFrequency.MONTHLY,
        effective_from="2026-01-01",
        effective_to=None,
        source_key=original.legal_producer_source_key,
        source_timezone="America/New_York",
        time_precision=EconomicTimePrecision.EXACT_MINUTE,
        rule_description="Official monthly schedule.",
        qualified=True,
        qualification_evidence=("https://example.invalid/cpi-calendar",),
        limitations=("Holiday shifts require occurrence evidence.",),
    )
    successor = EconomicIndicatorCatalogEntryV1(
        indicator_key="us.consumer-prices-v2",
        economy_code="US",
        economy_name="United States",
        affected_currencies=("USD",),
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="consumer-and-producer-prices",
        display_name="United States Consumer Prices v2",
        aliases=("CPI v2",),
        legal_producer_source_key=original.legal_producer_source_key,
        frequency=EconomicIndicatorFrequency.MONTHLY,
        reference_period_convention="calendar month",
        normal_schedule_rule="Official monthly schedule.",
        release_stages=(EconomicReleaseStage.INITIAL,),
        unit="index",
        scale=1.0,
        seasonal_basis="not-seasonally-adjusted",
        valid_from="2026-01-01",
        valid_to=None,
        methodology_eras=(era,),
        expected_release_rules=(rule,),
        structural_importance=3,
        importance_policy_version="structural-fx-family-prior.v1",
        importance_rationale="Structural prior, not vendor evidence.",
        limitations=("Fixture successor.",),
    )
    edge = EconomicIndicatorLineageV1(
        from_indicator_key=original.indicator_key,
        to_indicator_key=successor.indicator_key,
        kind=EconomicIndicatorLineageKind.REBASED,
        effective_on="2026-01-01",
        source_key=original.legal_producer_source_key,
        evidence=("https://example.invalid/cpi-rebase",),
        notes=("Old observations are not rewritten.",),
    )
    linked = replace(
        catalog,
        entries=(*catalog.entries, successor),
        lineage=(edge,),
        catalog_id="",
    )
    assert EconomicIndicatorCatalogV1.from_json(linked.to_json()) == linked
    reverse = EconomicIndicatorLineageV1(
        from_indicator_key=successor.indicator_key,
        to_indicator_key=original.indicator_key,
        kind=EconomicIndicatorLineageKind.MERGED,
        effective_on="2027-01-01",
        source_key=original.legal_producer_source_key,
        evidence=("https://example.invalid/cpi-cycle",),
        notes=(),
    )
    with pytest.raises(ValueError, match="lineage contains a cycle"):
        replace(linked, lineage=(edge, reverse), catalog_id="")


def test_expected_occurrence_requires_reason_and_exact_rule_binding(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    qualified = _qualified_catalog(catalog)
    unavailable = _occurrence(
        qualified,
        "08",
        20,
        EconomicExpectedOccurrenceStatus.SOURCE_UNAVAILABLE,
        reason="Official archive returned an explicit bounded gap.",
    )
    assert (
        EconomicExpectedOccurrenceV1.from_json(unavailable.to_json())
        == unavailable
    )
    with pytest.raises(ValueError, match="requires a reason"):
        replace(unavailable, reason=None, occurrence_id="")
    with pytest.raises(ValueError, match="not bound to its catalog rule"):
        audit_economic_indicator_coverage(
            qualified,
            (
                replace(
                    unavailable,
                    rule_id="economic-expected-release-rule:sha256:" + "0" * 64,
                    occurrence_id="",
                ),
            ),
            (),
            start_ns=START_NS,
            end_ns=END_NS,
        )


def test_coverage_distinguishes_gaps_cancellations_and_unmatched_releases(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    qualified = _qualified_catalog(catalog)
    released = _occurrence(
        qualified, "07", 10, EconomicExpectedOccurrenceStatus.RELEASED
    )
    missing = _occurrence(
        qualified,
        "08",
        20,
        EconomicExpectedOccurrenceStatus.SOURCE_UNAVAILABLE,
        reason="Contemporaneous official artifact is missing.",
    )
    cancelled = _occurrence(
        qualified,
        "09",
        30,
        EconomicExpectedOccurrenceStatus.CANCELLED,
        reason="Official calendar cancelled the occurrence.",
    )
    matched_release = _release(released.logical_event_key, 10)
    unmatched_release = _release("us.cpi.emergency", 40)
    audit = audit_economic_indicator_coverage(
        qualified,
        (released, missing, cancelled),
        (matched_release, unmatched_release),
        start_ns=START_NS,
        end_ns=END_NS,
    )
    result = next(
        item
        for item in audit.slices
        if item.indicator_key == "us.consumer-and-producer-prices"
    )
    assert result.expected_model_qualified
    assert result.expected_count == 3
    assert result.releasable_expected_count == 2
    assert result.reconstructed_count == 1
    assert result.structural_coverage == 0.5
    assert result.missing_logical_event_keys == (missing.logical_event_key,)
    assert audit.unexplained_gaps == (missing.logical_event_key,)
    assert audit.unmatched_release_ids == (unmatched_release.release_id,)
    assert EconomicIndicatorCoverageAuditV1.from_json(audit.to_json()) == audit

    with pytest.raises(ValueError, match="non-released expected occurrence"):
        audit_economic_indicator_coverage(
            qualified,
            (
                replace(
                    released,
                    status=EconomicExpectedOccurrenceStatus.SCHEDULED,
                    occurrence_id="",
                ),
            ),
            (matched_release,),
            start_ns=START_NS,
            end_ns=END_NS,
        )


def test_unqualified_expected_model_never_emits_a_coverage_percentage(
    catalog: EconomicIndicatorCatalogV1,
) -> None:
    occurrence = _occurrence(
        catalog, "07", 10, EconomicExpectedOccurrenceStatus.RELEASED
    )
    audit = audit_economic_indicator_coverage(
        catalog,
        (occurrence,),
        (_release(occurrence.logical_event_key, 10),),
        start_ns=START_NS,
        end_ns=END_NS,
    )
    result = next(
        item
        for item in audit.slices
        if item.indicator_key == occurrence.indicator_key
    )
    assert not result.expected_model_qualified
    assert result.structural_coverage is None


def test_catalog_persistence_round_trip_and_release_identity_fail_closed(
    catalog: EconomicIndicatorCatalogV1, tmp_path: Path
) -> None:
    destination = write_economic_indicator_catalog(
        catalog, tmp_path / "catalog.json"
    )
    assert read_economic_indicator_catalog(destination) == catalog

    qualified = _qualified_catalog(catalog)
    occurrence = _occurrence(
        qualified, "07", 10, EconomicExpectedOccurrenceStatus.RELEASED
    )
    with pytest.raises(ValueError, match="differs from catalog identity"):
        audit_economic_indicator_coverage(
            qualified,
            (occurrence,),
            (
                _release(
                    occurrence.logical_event_key,
                    10,
                    series_key="us.unregistered-cpi",
                ),
            ),
            start_ns=START_NS,
            end_ns=END_NS,
        )

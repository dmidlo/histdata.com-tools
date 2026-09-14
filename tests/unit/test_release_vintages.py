"""Tests for economic release-time and vintage reconstruction."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from histdatacom.market_context.contracts import (
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
)
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicTimePrecision,
    query_economic_calendar_as_known,
)
from histdatacom.market_context.release_vintages import (
    EconomicReleaseAvailabilityBasis,
    EconomicReleaseRevisionKind,
    EconomicReleaseTimeEvidenceV1,
    EconomicReleaseVintageChainV1,
    EconomicReleaseVintageMutationV1,
    apply_economic_release_vintage_mutation,
    audit_economic_release_vintages,
    build_economic_calendar_corpus_from_vintage_chains,
    build_economic_release_vintage_chain,
    select_release_time_evidence,
)

SECOND_NS = 1_000_000_000
DAY_NS = 86_400 * SECOND_NS
COVERAGE_START = 1_700_000_000 * SECOND_NS
COVERAGE_END = 1_750_000_000 * SECOND_NS
PRIOR_RELEASE = 1_704_067_200 * SECOND_NS
CURRENT_RELEASE = 1_706_745_600 * SECOND_NS


def _utc_text(value: int) -> str:
    return datetime.fromtimestamp(
        value / SECOND_NS, tz=timezone.utc
    ).isoformat()


def _source(*, observed: int, digest: str) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name="Official archive fixture",
        source_version="2026-09-13",
        retrieved_at_ns=observed,
        content_sha256=digest,
        adapter_name="official-archive-fixture",
        adapter_version="1.0",
        license_name="Public fixture terms",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute the producer.",),
        limitations=("Fixture proves temporal semantics only.",),
        source_uri="https://example.invalid/archive",
    )


def _time(
    *,
    scheduled: int,
    observed: int,
    published: int | None = None,
    updated: int | None = None,
    available: int | None = None,
    basis: EconomicReleaseAvailabilityBasis = (
        EconomicReleaseAvailabilityBasis.FIRST_OBSERVED
    ),
    precision: EconomicTimePrecision = EconomicTimePrecision.EXACT_SECOND,
    lower: int | None = None,
    upper: int | None = None,
) -> EconomicReleaseTimeEvidenceV1:
    return EconomicReleaseTimeEvidenceV1(
        scheduled_for_ns=scheduled,
        scheduled_lexical=_utc_text(scheduled),
        actual_published_at_ns=published,
        actual_published_lexical=(
            None if published is None else _utc_text(published)
        ),
        first_observed_at_ns=observed,
        source_updated_at_ns=updated,
        source_updated_lexical=None if updated is None else _utc_text(updated),
        available_at_ns=observed if available is None else available,
        publication_not_before_ns=lower,
        publication_not_after_ns=upper,
        source_timezone="UTC",
        timezone_evidence="Official UTC archive timestamps.",
        time_precision=precision,
        precision=(
            MarketContextPrecision.EXACT
            if precision
            in {
                EconomicTimePrecision.EXACT_SECOND,
                EconomicTimePrecision.EXACT_MINUTE,
            }
            else MarketContextPrecision.WINDOW_ONLY
        ),
        availability_basis=basis,
    )


def _initial_release(
    *,
    logical_event_key: str,
    reference_period: str,
    reference_period_end_ns: int,
    evidence: EconomicReleaseTimeEvidenceV1,
    digest: str,
    status: EconomicReleaseStatus,
    actual_value: float | None = None,
    series_version: str = "v1",
) -> EconomicCalendarReleaseV1:
    actual_status = status in {
        EconomicReleaseStatus.RELEASED,
        EconomicReleaseStatus.UNSCHEDULED,
    }
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key="us.cpi.all-items.sa.mom",
        series_version=series_version,
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="U.S. Bureau of Labor Statistics",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="cpi-all-items",
        source_series_id="cuur0000sa0",
        source_table_id="cpi-table-1",
        source_release_id=f"release.{digest[:8]}",
        source_request_id=f"request.{digest[:8]}",
        title="Consumer Price Index",
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        scale=1.0,
        base="1982-84=100",
        stage=EconomicReleaseStage.INITIAL,
        status=status,
        scheduled_for_ns=evidence.scheduled_for_ns,
        scheduled_lexical=evidence.scheduled_lexical,
        released_at_ns=evidence.actual_published_at_ns,
        released_lexical=evidence.actual_published_lexical,
        first_observed_at_ns=evidence.first_observed_at_ns,
        available_at_ns=evidence.available_at_ns,
        source_timezone=evidence.source_timezone,
        timezone_evidence=evidence.timezone_evidence,
        time_precision=evidence.time_precision,
        precision=evidence.precision,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=_source(observed=evidence.first_observed_at_ns, digest=digest),
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        limitations=("Fixture proves temporal semantics only.",),
        actual_value=actual_value,
        actual_lexical=None if not actual_status else f"{actual_value}%",
        content_sha256=digest,
    )


def _mutation(
    previous: EconomicCalendarReleaseV1,
    *,
    evidence: EconomicReleaseTimeEvidenceV1,
    digest: str,
    status: EconomicReleaseStatus,
    stage: EconomicReleaseStage = EconomicReleaseStage.INITIAL,
    actual_value: float | None = None,
    reason: str | None = None,
    revision_kind: EconomicReleaseRevisionKind | None = None,
    periods: tuple[str, ...] = (),
    trigger: str | None = None,
    series_id: str | None = None,
) -> EconomicReleaseVintageMutationV1:
    return EconomicReleaseVintageMutationV1(
        previous_release_id=previous.release_id,
        logical_event_key=previous.logical_event_key,
        series_id=previous.series_id if series_id is None else series_id,
        source_release_id=f"release.{digest[:8]}",
        source_request_id=f"request.{digest[:8]}",
        stage=stage,
        status=status,
        time_evidence=evidence,
        source=_source(observed=evidence.first_observed_at_ns, digest=digest),
        limitations=("Fixture proves temporal semantics only.",),
        schedule_change_reason=reason,
        actual_value=actual_value,
        actual_lexical=None if actual_value is None else f"{actual_value}%",
        content_sha256=digest,
        revision_kind=revision_kind,
        affected_reference_periods=periods,
        triggering_logical_event_key=trigger,
    )


def _direct_actual_chain(
    *,
    logical_event_key: str,
    reference_period: str,
    reference_period_end_ns: int,
    published: int,
    value: float,
    digest: str,
    scheduled: int | None = None,
) -> EconomicReleaseVintageChainV1:
    scheduled_at = published if scheduled is None else scheduled
    evidence = _time(
        scheduled=scheduled_at,
        published=published,
        observed=published + 10 * SECOND_NS,
        available=published,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    release = _initial_release(
        logical_event_key=logical_event_key,
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        evidence=evidence,
        digest=digest,
        status=EconomicReleaseStatus.RELEASED,
        actual_value=value,
    )
    return build_economic_release_vintage_chain(
        release,
        initial_time_evidence=evidence,
        limitations=("Fixture chain is intentionally bounded.",),
    )


def test_time_hierarchy_accepts_later_archive_fetch_and_prefers_publication() -> (
    None
):
    published = CURRENT_RELEASE
    observed = published + DAY_NS
    official = _time(
        scheduled=published,
        published=published,
        observed=observed,
        available=published,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    updated = _time(
        scheduled=published,
        published=published,
        updated=published + SECOND_NS,
        observed=observed,
        available=published + SECOND_NS,
        basis=EconomicReleaseAvailabilityBasis.SOURCE_UPDATE,
    )
    observed_only = _time(scheduled=published, observed=observed)

    selected = select_release_time_evidence((observed_only, updated, official))

    assert selected is official
    release = _initial_release(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=published - 10 * DAY_NS,
        evidence=official,
        digest="a" * 64,
        status=EconomicReleaseStatus.RELEASED,
        actual_value=0.3,
    )
    assert release.available_at_ns == published
    assert release.first_observed_at_ns == observed
    assert release.source.retrieved_at_ns == observed
    conflicting = _time(
        scheduled=published,
        published=published + SECOND_NS,
        observed=observed,
        available=published + SECOND_NS,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    with pytest.raises(ValueError, match="official publication time"):
        select_release_time_evidence((official, conflicting))


def test_time_hierarchy_rejects_false_precision_and_unbounded_inference() -> (
    None
):
    with pytest.raises(ValueError, match="requires exact precision"):
        _time(
            scheduled=CURRENT_RELEASE,
            published=CURRENT_RELEASE,
            observed=CURRENT_RELEASE + DAY_NS,
            available=CURRENT_RELEASE,
            basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
            precision=EconomicTimePrecision.DATE_ONLY,
        )
    with pytest.raises(ValueError, match="bounded upper endpoint"):
        _time(
            scheduled=CURRENT_RELEASE,
            observed=CURRENT_RELEASE + DAY_NS,
            available=CURRENT_RELEASE,
            basis=EconomicReleaseAvailabilityBasis.INFERRED_BOUNDED,
            precision=EconomicTimePrecision.INFERRED_BOUNDED,
        )


def test_schedule_release_revision_chain_replays_and_exposes_deltas() -> None:
    original_schedule = CURRENT_RELEASE
    moved_schedule = CURRENT_RELEASE + DAY_NS
    schedule_evidence = _time(
        scheduled=original_schedule,
        observed=original_schedule - 30 * DAY_NS,
        precision=EconomicTimePrecision.SCHEDULED_ONLY,
    )
    schedule = _initial_release(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=original_schedule - 10 * DAY_NS,
        evidence=schedule_evidence,
        digest="a" * 64,
        status=EconomicReleaseStatus.SCHEDULED,
    )
    reschedule_evidence = _time(
        scheduled=moved_schedule,
        observed=original_schedule - 20 * DAY_NS,
        precision=EconomicTimePrecision.SCHEDULED_ONLY,
    )
    reschedule = _mutation(
        schedule,
        evidence=reschedule_evidence,
        digest="b" * 64,
        status=EconomicReleaseStatus.RESCHEDULED,
        reason="Producer moved the publication date.",
    )
    rescheduled_release = apply_economic_release_vintage_mutation(
        schedule, reschedule
    )
    publication_evidence = _time(
        scheduled=moved_schedule,
        published=moved_schedule,
        observed=moved_schedule + 5 * SECOND_NS,
        available=moved_schedule,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    publication = _mutation(
        rescheduled_release,
        evidence=publication_evidence,
        digest="c" * 64,
        status=EconomicReleaseStatus.RELEASED,
        actual_value=0.3,
    )
    published_release = apply_economic_release_vintage_mutation(
        rescheduled_release, publication
    )
    revision_time = moved_schedule + 2 * DAY_NS
    revision_evidence = _time(
        scheduled=moved_schedule,
        published=revision_time,
        observed=revision_time + 5 * SECOND_NS,
        available=revision_time,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    revision = _mutation(
        published_release,
        evidence=revision_evidence,
        digest="d" * 64,
        status=EconomicReleaseStatus.RELEASED,
        stage=EconomicReleaseStage.REVISION,
        actual_value=0.4,
        revision_kind=EconomicReleaseRevisionKind.ROUTINE,
        periods=("2024-01",),
    )
    chain = build_economic_release_vintage_chain(
        schedule,
        initial_time_evidence=schedule_evidence,
        mutations=(reschedule, publication, revision),
        limitations=("Fixture chain is intentionally bounded.",),
    )
    restored = EconomicReleaseVintageChainV1.from_json(chain.to_json())
    corpus = build_economic_calendar_corpus_from_vintage_chains(
        (restored,),
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        limitations=("Fixture corpus is intentionally bounded.",),
    )
    query = query_economic_calendar_as_known(
        corpus,
        start_ns=moved_schedule,
        end_ns=moved_schedule + SECOND_NS,
        decision_at_ns=revision_time,
    )

    assert restored == chain
    assert chain.replay() == chain.releases
    assert chain.as_known_at(moved_schedule - SECOND_NS) == rescheduled_release
    assert query.events[0].actual_initial == 0.3
    assert query.events[0].actual_latest == 0.4
    assert query.events[0].actual_revision_deltas == (
        (chain.releases[-1].release_id, pytest.approx(0.1)),
    )
    audit = audit_economic_release_vintages(
        corpus,
        (chain,),
        limitations=("Fixture audit covers one event.",),
    )
    restored_audit = type(audit).from_json(audit.to_json())
    assert audit.passed
    assert restored_audit == audit
    assert audit.initial_actual_count == 1
    assert audit.revision_count == 1


def test_simultaneous_previous_revision_never_leaks_into_previous_as_known() -> (
    None
):
    prior = _direct_actual_chain(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=PRIOR_RELEASE - 10 * DAY_NS,
        published=PRIOR_RELEASE,
        value=0.2,
        digest="1" * 64,
    )
    current = _direct_actual_chain(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE - 10 * DAY_NS,
        published=CURRENT_RELEASE,
        value=0.3,
        digest="2" * 64,
        scheduled=CURRENT_RELEASE - DAY_NS,
    )
    simultaneous_evidence = _time(
        scheduled=PRIOR_RELEASE,
        published=CURRENT_RELEASE,
        observed=CURRENT_RELEASE + 10 * SECOND_NS,
        available=CURRENT_RELEASE,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    simultaneous = _mutation(
        prior.releases[-1],
        evidence=simultaneous_evidence,
        digest="3" * 64,
        status=EconomicReleaseStatus.RELEASED,
        stage=EconomicReleaseStage.REVISION,
        actual_value=0.25,
        revision_kind=EconomicReleaseRevisionKind.SIMULTANEOUS_PREVIOUS,
        periods=("2023-12",),
        trigger=current.initial_release.logical_event_key,
    )
    prior_revised = build_economic_release_vintage_chain(
        prior.initial_release,
        initial_time_evidence=prior.initial_time_evidence,
        mutations=(simultaneous,),
        limitations=("Fixture chain is intentionally bounded.",),
    )
    current_revision_time = CURRENT_RELEASE + DAY_NS
    current_revision = _mutation(
        current.initial_release,
        evidence=_time(
            scheduled=CURRENT_RELEASE - DAY_NS,
            published=current_revision_time,
            observed=current_revision_time + 10 * SECOND_NS,
            available=current_revision_time,
            basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
        ),
        digest="a" * 64,
        status=EconomicReleaseStatus.RELEASED,
        stage=EconomicReleaseStage.REVISION,
        actual_value=0.35,
        revision_kind=EconomicReleaseRevisionKind.ROUTINE,
        periods=("2024-01",),
    )
    current_revised = build_economic_release_vintage_chain(
        current.initial_release,
        initial_time_evidence=current.initial_time_evidence,
        mutations=(current_revision,),
        limitations=("Fixture chain is intentionally bounded.",),
    )
    corpus = build_economic_calendar_corpus_from_vintage_chains(
        (prior_revised, current_revised),
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        limitations=("Fixture corpus is intentionally bounded.",),
    )
    query = query_economic_calendar_as_known(
        corpus,
        start_ns=CURRENT_RELEASE,
        end_ns=CURRENT_RELEASE + SECOND_NS,
        decision_at_ns=current_revision_time,
    )
    state = query.events[0]

    assert state.release.stage is EconomicReleaseStage.REVISION
    assert state.release.event_time_ns == CURRENT_RELEASE - DAY_NS
    assert state.event_time_ns == CURRENT_RELEASE
    assert state.previous_value == 0.2
    assert state.previous_latest == 0.25
    assert state.previous_as_known == prior_revised.initial_release
    audit = audit_economic_release_vintages(
        corpus,
        (prior_revised, current_revised),
        limitations=("Fixture audit covers simultaneous publication.",),
    )
    assert audit.passed
    assert dict(audit.revision_kind_counts)["simultaneous-previous"] == 1


def test_later_current_database_revision_cannot_leak_backward() -> None:
    chain = _direct_actual_chain(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE - 10 * DAY_NS,
        published=CURRENT_RELEASE,
        value=0.3,
        digest="4" * 64,
    )
    revision_time = CURRENT_RELEASE + 30 * DAY_NS
    mutation = _mutation(
        chain.initial_release,
        evidence=_time(
            scheduled=CURRENT_RELEASE,
            published=revision_time,
            observed=revision_time + 10 * SECOND_NS,
            available=revision_time,
            basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
        ),
        digest="5" * 64,
        status=EconomicReleaseStatus.RELEASED,
        stage=EconomicReleaseStage.REVISION,
        actual_value=0.8,
        revision_kind=EconomicReleaseRevisionKind.BENCHMARK,
        periods=("2023-12", "2024-01"),
    )
    revised = build_economic_release_vintage_chain(
        chain.initial_release,
        initial_time_evidence=chain.initial_time_evidence,
        mutations=(mutation,),
        limitations=("Fixture chain is intentionally bounded.",),
    )
    corpus = build_economic_calendar_corpus_from_vintage_chains(
        (revised,),
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        limitations=("Fixture corpus is intentionally bounded.",),
    )
    before = query_economic_calendar_as_known(
        corpus,
        start_ns=CURRENT_RELEASE,
        end_ns=CURRENT_RELEASE + SECOND_NS,
        decision_at_ns=revision_time - SECOND_NS,
    )
    after = query_economic_calendar_as_known(
        corpus,
        start_ns=CURRENT_RELEASE,
        end_ns=CURRENT_RELEASE + SECOND_NS,
        decision_at_ns=revision_time,
    )

    assert before.events[0].actual_latest == 0.3
    assert after.events[0].actual_latest == 0.8
    assert after.events[0].actual_revision_deltas[0][1] == pytest.approx(0.5)


def test_series_migrations_and_rebases_require_a_new_series_chain() -> None:
    chain = _direct_actual_chain(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE - 10 * DAY_NS,
        published=CURRENT_RELEASE,
        value=0.3,
        digest="6" * 64,
    )
    evidence = _time(
        scheduled=CURRENT_RELEASE,
        published=CURRENT_RELEASE + DAY_NS,
        observed=CURRENT_RELEASE + DAY_NS + SECOND_NS,
        available=CURRENT_RELEASE + DAY_NS,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    mutation = _mutation(
        chain.initial_release,
        evidence=evidence,
        digest="7" * 64,
        status=EconomicReleaseStatus.RELEASED,
        stage=EconomicReleaseStage.REVISION,
        actual_value=0.4,
        revision_kind=EconomicReleaseRevisionKind.CORRECTION,
        periods=("2024-01",),
        series_id="economic-calendar-series:sha256:" + "f" * 64,
    )

    with pytest.raises(ValueError, match="require a new chain"):
        apply_economic_release_vintage_mutation(chain.initial_release, mutation)
    distinct_stage = _mutation(
        chain.initial_release,
        evidence=evidence,
        digest="e" * 64,
        status=EconomicReleaseStatus.RELEASED,
        stage=EconomicReleaseStage.FINAL,
        actual_value=0.4,
    )
    with pytest.raises(ValueError, match="distinct publication stage"):
        apply_economic_release_vintage_mutation(
            chain.initial_release, distinct_stage
        )


def test_cancellation_and_unscheduled_decision_remain_explicit_vintages() -> (
    None
):
    scheduled_evidence = _time(
        scheduled=CURRENT_RELEASE,
        observed=CURRENT_RELEASE - 30 * DAY_NS,
        precision=EconomicTimePrecision.SCHEDULED_ONLY,
    )
    scheduled = _initial_release(
        logical_event_key="us.fomc.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE - DAY_NS,
        evidence=scheduled_evidence,
        digest="b" * 64,
        status=EconomicReleaseStatus.SCHEDULED,
    )
    cancellation_evidence = _time(
        scheduled=CURRENT_RELEASE,
        observed=CURRENT_RELEASE - 20 * DAY_NS,
        precision=EconomicTimePrecision.SCHEDULED_ONLY,
    )
    cancellation = _mutation(
        scheduled,
        evidence=cancellation_evidence,
        digest="c" * 64,
        status=EconomicReleaseStatus.CANCELLED,
        reason="Producer cancelled the scheduled release.",
    )
    cancelled = build_economic_release_vintage_chain(
        scheduled,
        initial_time_evidence=scheduled_evidence,
        mutations=(cancellation,),
        limitations=("Fixture chain is intentionally bounded.",),
    )
    unscheduled_time = CURRENT_RELEASE + DAY_NS
    unscheduled_evidence = _time(
        scheduled=unscheduled_time,
        published=unscheduled_time,
        observed=unscheduled_time + SECOND_NS,
        available=unscheduled_time,
        basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )
    unscheduled = _initial_release(
        logical_event_key="us.fomc.unscheduled-2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE,
        evidence=unscheduled_evidence,
        digest="d" * 64,
        status=EconomicReleaseStatus.UNSCHEDULED,
        actual_value=5.5,
    )

    assert cancelled.releases[-1].status is EconomicReleaseStatus.CANCELLED
    assert cancelled.releases[-1].actual_value is None
    assert unscheduled.status is EconomicReleaseStatus.UNSCHEDULED
    assert unscheduled.released_at_ns == unscheduled_time


def test_audit_reports_missing_initial_actual_without_inventing_one() -> None:
    evidence = _time(
        scheduled=CURRENT_RELEASE,
        observed=CURRENT_RELEASE - 30 * DAY_NS,
        precision=EconomicTimePrecision.SCHEDULED_ONLY,
    )
    release = _initial_release(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE - 10 * DAY_NS,
        evidence=evidence,
        digest="8" * 64,
        status=EconomicReleaseStatus.SCHEDULED,
    )
    chain = build_economic_release_vintage_chain(
        release,
        initial_time_evidence=evidence,
        limitations=("No archive publication was retained.",),
    )
    corpus = build_economic_calendar_corpus_from_vintage_chains(
        (chain,),
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=False,
        limitations=("Publication evidence is missing.",),
    )

    audit = audit_economic_release_vintages(
        corpus,
        (chain,),
        limitations=("Missing evidence remains explicit.",),
    )

    assert audit.passed
    assert audit.initial_actual_count == 0
    assert audit.missing_initial_actual_event_keys == ("us.cpi.2024-01",)


def test_tampered_chain_identity_and_release_payload_are_rejected() -> None:
    chain = _direct_actual_chain(
        logical_event_key="us.cpi.2024-01",
        reference_period="2024-01",
        reference_period_end_ns=CURRENT_RELEASE - 10 * DAY_NS,
        published=CURRENT_RELEASE,
        value=0.3,
        digest="9" * 64,
    )
    with pytest.raises(ValueError, match="chain_id"):
        replace(
            chain, chain_id="economic-release-vintage-chain:sha256:" + "0" * 64
        )
    with pytest.raises(
        ValueError,
        match="release_id|retained releases differ",
    ):
        replace(
            chain,
            releases=(replace(chain.initial_release, actual_value=0.4),),
            chain_id="",
        )

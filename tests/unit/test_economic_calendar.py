"""Tests for provider-neutral economic release vintages and queries."""

from __future__ import annotations

import inspect
from dataclasses import replace
from pathlib import Path

import pytest

import histdatacom.market_context.economic_calendar as calendar_module
from histdatacom.market_context import (
    EconomicCalendarAsKnownReaderV1,
    EconomicCalendarCorpusV1,
    EconomicCalendarForecastV1,
    EconomicCalendarQueryV1,
    EconomicCalendarReleaseV1,
    EconomicForecastKind,
    EconomicForecastScope,
    EconomicReleaseStage,
    MarketContextEventV1,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    StaticOfficialEconomicCalendarAdapterV1,
    build_economic_calendar_corpus,
    economic_release_from_market_context,
    project_economic_calendar_state,
    query_economic_calendar_as_known,
    read_economic_calendar_corpus,
    replay_economic_calendar_corpus,
    write_economic_calendar_corpus,
)

SECOND_NS = 1_000_000_000
DAY_NS = 86_400 * SECOND_NS
PRIOR_TIME = 1_700_000_000 * SECOND_NS
TARGET_TIME = PRIOR_TIME + 30 * DAY_NS
COVERAGE_START = PRIOR_TIME - DAY_NS
COVERAGE_END = TARGET_TIME + DAY_NS
RETRIEVED_AT = COVERAGE_END


def _source(
    adapter_name: str,
    *,
    digest: str,
    source_name: str = "Official statistical producer fixture",
) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name=source_name,
        source_version=f"fixture-{digest[0]}",
        retrieved_at_ns=RETRIEVED_AT,
        content_sha256=digest,
        adapter_name=adapter_name,
        adapter_version="1.0",
        license_name="Public official-source fixture terms",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute the statistical producer.",),
        limitations=("Fixture proves contracts, not production coverage.",),
        source_uri=f"https://example.invalid/{adapter_name}/{digest[0]}",
        metadata={"source_kind": "official_fixture"},
    )


def _release(
    *,
    logical_event_key: str,
    reference_period: str,
    reference_period_end_ns: int,
    event_time_ns: int,
    available_at_ns: int,
    stage: EconomicReleaseStage,
    revision_sequence: int,
    supersedes_release_id: str | None = None,
    actual_value: float | None = None,
    adapter_name: str = "bls-public-release",
    digest: str = "a" * 64,
    currency: str = "USD",
) -> EconomicCalendarReleaseV1:
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key="us.cpi.all-items.sa.mom",
        economy="United States",
        currency=currency,
        institution="U.S. Bureau of Labor Statistics",
        event_family="consumer-price-index",
        indicator_id="cpi-all-items",
        source_series_id="cuur0000sa0",
        title="Consumer Price Index",
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        scale=1.0,
        base="1982-84=100",
        stage=stage,
        scheduled_for_ns=event_time_ns,
        released_at_ns=(
            event_time_ns
            if stage
            in {EconomicReleaseStage.INITIAL, EconomicReleaseStage.REVISION}
            else None
        ),
        first_observed_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
        precision=MarketContextPrecision.EXACT,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=_source(adapter_name, digest=digest),
        affected_currencies=(currency,),
        affected_symbols=("EURUSD", "GBPUSD"),
        limitations=("Official fixture omits intraday publication latency.",),
        actual_value=actual_value,
        actual_lexical=None if actual_value is None else f"{actual_value:.1f}%",
        content_sha256=digest,
        revision_sequence=revision_sequence,
        supersedes_release_id=supersedes_release_id,
    )


def _forecast(
    *,
    kind: EconomicForecastKind,
    available_at_ns: int,
    value: float,
    digest: str,
) -> EconomicCalendarForecastV1:
    adapter = (
        "public-consensus-observation"
        if kind is EconomicForecastKind.OBSERVED_CONSENSUS
        else "point-in-time-machine-projection"
    )
    return EconomicCalendarForecastV1(
        logical_event_key="us.cpi.2023-12",
        kind=kind,
        scope=EconomicForecastScope.EVENT_RELEASE,
        produced_at_ns=available_at_ns - SECOND_NS,
        available_at_ns=available_at_ns,
        value=value,
        lexical_value=f"{value:.1f}%",
        unit="percent_mom",
        scale=1.0,
        base="1982-84=100",
        source=_source(adapter, digest=digest),
        limitations=("Fixture expectation has no production coverage claim.",),
    )


def _corpus() -> EconomicCalendarCorpusV1:
    prior_initial = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="a" * 64,
    )
    prior_revision = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=TARGET_TIME - DAY_NS,
        stage=EconomicReleaseStage.REVISION,
        revision_sequence=1,
        supersedes_release_id=prior_initial.release_id,
        actual_value=1.1,
        digest="b" * 64,
    )
    late_prior_revision = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=TARGET_TIME + SECOND_NS,
        stage=EconomicReleaseStage.REVISION,
        revision_sequence=2,
        supersedes_release_id=prior_revision.release_id,
        actual_value=1.2,
        digest="c" * 64,
    )
    schedule = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME - 20 * DAY_NS,
        stage=EconomicReleaseStage.SCHEDULED,
        revision_sequence=0,
        digest="d" * 64,
    )
    initial = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=1,
        supersedes_release_id=schedule.release_id,
        actual_value=2.0,
        digest="e" * 64,
    )
    revision = _release(
        logical_event_key="us.cpi.2023-12",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        event_time_ns=TARGET_TIME,
        available_at_ns=TARGET_TIME + 2 * SECOND_NS,
        stage=EconomicReleaseStage.REVISION,
        revision_sequence=2,
        supersedes_release_id=initial.release_id,
        actual_value=2.1,
        digest="f" * 64,
    )
    forecasts = (
        _forecast(
            kind=EconomicForecastKind.OBSERVED_CONSENSUS,
            available_at_ns=TARGET_TIME - 2 * DAY_NS,
            value=1.9,
            digest="1" * 64,
        ),
        _forecast(
            kind=EconomicForecastKind.MACHINE_PROJECTION,
            available_at_ns=TARGET_TIME - DAY_NS,
            value=1.8,
            digest="2" * 64,
        ),
        _forecast(
            kind=EconomicForecastKind.OBSERVED_CONSENSUS,
            available_at_ns=TARGET_TIME + SECOND_NS,
            value=2.0,
            digest="3" * 64,
        ),
    )
    return EconomicCalendarCorpusV1(
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        releases=(
            prior_initial,
            prior_revision,
            late_prior_revision,
            schedule,
            initial,
            revision,
        ),
        forecasts=forecasts,
        limitations=(
            "Completeness applies only to configured official-source evidence.",
        ),
    )


def _query(
    corpus: EconomicCalendarCorpusV1, decision_at_ns: int
) -> EconomicCalendarQueryV1:
    return query_economic_calendar_as_known(
        corpus,
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + SECOND_NS,
        decision_at_ns=decision_at_ns,
        currencies=("usd",),
        symbols=("eurusd",),
        event_families=("consumer-price-index",),
    )


def test_as_known_query_keeps_schedule_actual_and_revision_distinct() -> None:
    corpus = _corpus()

    scheduled = _query(corpus, TARGET_TIME - SECOND_NS).events[0]
    initial = _query(corpus, TARGET_TIME).events[0]
    revised = _query(corpus, TARGET_TIME + 3 * SECOND_NS).events[0]

    assert scheduled.release.stage is EconomicReleaseStage.SCHEDULED
    assert scheduled.actual_initial is None
    assert scheduled.previous_value == 1.1
    assert scheduled.observed_consensus is not None
    assert scheduled.observed_consensus.value == 1.9
    assert scheduled.machine_projection is not None
    assert scheduled.machine_projection.value == 1.8
    assert initial.release.stage is EconomicReleaseStage.INITIAL
    assert initial.actual_initial == 2.0
    assert initial.actual_latest == 2.0
    assert initial.observed_surprise == pytest.approx(0.1)
    assert initial.machine_surprise == pytest.approx(0.2)
    assert revised.release.stage is EconomicReleaseStage.REVISION
    assert revised.actual_initial == 2.0
    assert revised.actual_latest == 2.1


def test_previous_as_known_excludes_revision_published_after_release() -> None:
    state = _query(_corpus(), TARGET_TIME + 3 * SECOND_NS).events[0]

    assert state.previous_as_known is not None
    assert state.previous_as_known.revision_sequence == 1
    assert state.previous_as_known.actual_value == 1.1
    assert state.previous_as_known.available_at_ns < TARGET_TIME


def test_post_release_consensus_never_replaces_pre_release_vintage() -> None:
    state = _query(_corpus(), TARGET_TIME + 3 * SECOND_NS).events[0]

    assert state.observed_consensus is not None
    assert state.observed_consensus.value == 1.9
    assert state.observed_consensus.available_at_ns < TARGET_TIME


def test_forecast_available_at_release_boundary_is_not_admitted() -> None:
    corpus = _corpus()
    boundary = replace(
        corpus.forecasts[0],
        produced_at_ns=TARGET_TIME - SECOND_NS,
        available_at_ns=TARGET_TIME,
        value=2.5,
        source=_source("public-consensus-observation", digest="7" * 64),
        forecast_id="",
    )
    bounded = replace(
        corpus,
        forecasts=(*corpus.forecasts, boundary),
        corpus_id="",
    )

    state = _query(bounded, TARGET_TIME).events[0]

    assert state.observed_consensus is not None
    assert state.observed_consensus.value == 1.9


def test_query_rejects_intervals_outside_declared_coverage() -> None:
    corpus = _corpus()

    with pytest.raises(ValueError, match="outside corpus coverage"):
        query_economic_calendar_as_known(
            corpus,
            start_ns=COVERAGE_START - SECOND_NS,
            end_ns=COVERAGE_START + SECOND_NS,
            decision_at_ns=TARGET_TIME,
        )


def test_query_round_trip_and_provider_neutral_reader_protocol() -> None:
    corpus = _corpus()
    reader: EconomicCalendarAsKnownReaderV1 = corpus

    assert isinstance(reader, EconomicCalendarAsKnownReaderV1)
    query = reader.as_known_at(
        start_ns=TARGET_TIME,
        end_ns=TARGET_TIME + SECOND_NS,
        decision_at_ns=TARGET_TIME,
    )
    assert EconomicCalendarQueryV1.from_json(query.to_json()) == query
    assert query.coverage_start_ns == corpus.coverage_start_ns
    assert query.coverage_end_ns == corpus.coverage_end_ns
    assert query.corpus_complete is True


def test_two_official_adapters_build_the_same_neutral_contract() -> None:
    bls_release = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="4" * 64,
    )
    ons_release = replace(
        _release(
            logical_event_key="uk.cpi.2023-11",
            reference_period="2023-11",
            reference_period_end_ns=PRIOR_TIME - 9 * DAY_NS,
            event_time_ns=PRIOR_TIME + SECOND_NS,
            available_at_ns=PRIOR_TIME + SECOND_NS,
            stage=EconomicReleaseStage.INITIAL,
            revision_sequence=0,
            actual_value=0.4,
            adapter_name="ons-public-release",
            digest="5" * 64,
        ),
        series_key="uk.cpi.all-items.mom",
        economy="United Kingdom",
        currency="GBP",
        institution="Office for National Statistics",
        indicator_id="uk-cpi-all-items",
        source_series_id="ons-cpi-d7bt",
        affected_currencies=("GBP",),
        affected_symbols=("EURGBP", "GBPUSD"),
        release_id="",
    )
    corpus = build_economic_calendar_corpus(
        (
            StaticOfficialEconomicCalendarAdapterV1(
                adapter_name="bls-public-release",
                adapter_version="1.0",
                releases=(bls_release,),
            ),
            StaticOfficialEconomicCalendarAdapterV1(
                adapter_name="ons-public-release",
                adapter_version="1.0",
                releases=(ons_release,),
            ),
        ),
        coverage_start_ns=COVERAGE_START,
        coverage_end_ns=COVERAGE_END,
        complete=True,
        limitations=("Only two official fixture adapters are configured.",),
    )

    query = corpus.as_known_at(
        start_ns=PRIOR_TIME,
        end_ns=PRIOR_TIME + 2 * SECOND_NS,
        decision_at_ns=PRIOR_TIME + 2 * SECOND_NS,
    )

    assert len(query.events) == 2
    assert {item.release.source.adapter_name for item in query.events} == {
        "bls-public-release",
        "ons-public-release",
    }
    assert all(isinstance(item, type(query.events[0])) for item in query.events)


def test_market_context_projection_preserves_projection_provenance() -> None:
    state = _query(_corpus(), TARGET_TIME).events[0]

    projected = project_economic_calendar_state(
        state, pre_event_ns=SECOND_NS, post_event_ns=SECOND_NS
    )

    assert isinstance(projected, MarketContextEventV1)
    assert projected.actual_value == 2.0
    assert projected.previous_value == 1.1
    assert projected.expected_value == 1.9
    assert "projection:observed_consensus" in projected.tags


def test_official_market_context_event_bridges_without_provider_fields() -> (
    None
):
    source = _source("official-market-context", digest="6" * 64)
    event = MarketContextEventV1(
        canonical_key="official.cpi.2023-12",
        kind=MarketContextKind.MACRO_RELEASE,
        title="Official CPI release",
        source=source,
        source_event_time="2023-12-14T13:30:00+00:00",
        source_timezone="UTC",
        event_time_ns=1_702_560_600_000_000_000,
        first_known_at_ns=1,
        available_at_ns=1,
        pre_event_ns=0,
        post_event_ns=1,
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        confidence=1.0,
        precision=MarketContextPrecision.EXACT,
        limitations=("Official fixture only.",),
        vintage_id="official-initial",
        actual_value=2.0,
        value_unit="percent_mom",
    )

    release = economic_release_from_market_context(
        event,
        logical_event_key="us.cpi.2023-12",
        series_key="us.cpi.all-items.sa.mom",
        economy="United States",
        currency="USD",
        institution="U.S. Bureau of Labor Statistics",
        event_family="consumer-price-index",
        indicator_id="cpi-all-items",
        source_series_id="cuur0000sa0",
        reference_period="2023-12",
        reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="percent_mom",
        stage=EconomicReleaseStage.INITIAL,
    )

    assert release.stage is EconomicReleaseStage.INITIAL
    assert release.source is source
    assert release.actual_value == 2.0


def test_content_addressed_corpus_round_trip_and_tamper_rejection(
    tmp_path: Path,
) -> None:
    corpus = _corpus()
    artifact = write_economic_calendar_corpus(corpus, tmp_path)

    assert read_economic_calendar_corpus(artifact.path) == corpus
    assert replay_economic_calendar_corpus(artifact.path) == corpus
    path = Path(artifact.path)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash differs"):
        read_economic_calendar_corpus(path)


def test_invalid_revision_chain_is_rejected() -> None:
    corpus = _corpus()
    broken = replace(
        corpus.releases[-1],
        supersedes_release_id="economic-calendar-release:sha256:" + "0" * 64,
        release_id="",
    )

    with pytest.raises(ValueError, match="does not supersede"):
        replace(
            corpus,
            releases=(*corpus.releases[:-1], broken),
            corpus_id="",
        )


def test_revision_availability_cannot_move_backward() -> None:
    corpus = _corpus()
    revision = replace(
        corpus.releases[1],
        first_observed_at_ns=PRIOR_TIME - SECOND_NS,
        available_at_ns=PRIOR_TIME - SECOND_NS,
        release_id="",
    )

    with pytest.raises(ValueError, match="availability moves backward"):
        replace(
            corpus,
            releases=(corpus.releases[0], revision, *corpus.releases[2:]),
            corpus_id="",
        )


def test_series_units_cannot_drift_between_reference_periods() -> None:
    first = _release(
        logical_event_key="us.cpi.2023-11",
        reference_period="2023-11",
        reference_period_end_ns=PRIOR_TIME - 10 * DAY_NS,
        event_time_ns=PRIOR_TIME,
        available_at_ns=PRIOR_TIME,
        stage=EconomicReleaseStage.INITIAL,
        revision_sequence=0,
        actual_value=1.0,
        digest="8" * 64,
    )
    second = replace(
        _release(
            logical_event_key="us.cpi.2023-12",
            reference_period="2023-12",
            reference_period_end_ns=TARGET_TIME - 10 * DAY_NS,
            event_time_ns=TARGET_TIME,
            available_at_ns=TARGET_TIME,
            stage=EconomicReleaseStage.INITIAL,
            revision_sequence=0,
            actual_value=2.0,
            digest="9" * 64,
        ),
        unit="index_points",
        release_id="",
    )

    with pytest.raises(ValueError, match="series semantic identity drifts"):
        EconomicCalendarCorpusV1(
            coverage_start_ns=COVERAGE_START,
            coverage_end_ns=COVERAGE_END,
            complete=True,
            releases=(first, second),
            forecasts=(),
            limitations=("Unit-drift rejection fixture.",),
        )


def test_forecasts_require_matching_scale_and_unambiguous_slots() -> None:
    corpus = _corpus()
    wrong_scale = replace(corpus.forecasts[0], scale=100.0, forecast_id="")
    with pytest.raises(ValueError, match="scale/base differs"):
        replace(
            corpus,
            forecasts=(wrong_scale, *corpus.forecasts[1:]),
            corpus_id="",
        )

    duplicate_slot = replace(
        corpus.forecasts[0],
        value=1.85,
        source=_source("public-consensus-observation", digest="0" * 64),
        forecast_id="",
    )
    with pytest.raises(ValueError, match="ambiguous forecast"):
        replace(
            corpus,
            forecasts=(*corpus.forecasts, duplicate_slot),
            corpus_id="",
        )


def test_canonical_module_has_no_commercial_calendar_dependency() -> None:
    source = inspect.getsource(calendar_module).lower()

    assert "trading_economics" not in source
    assert "trading economics" not in source
    assert "api_key" not in source
    assert "import requests" not in source

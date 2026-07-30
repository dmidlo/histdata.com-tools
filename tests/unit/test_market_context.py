"""Tests for point-in-time market-context contracts and bounded joins."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    MARKET_CONTEXT_EVENT_SCHEMA_VERSION,
    MARKET_CONTEXT_SOURCE_SCHEMA_VERSION,
    MARKET_CONTEXT_TIMELINE_SCHEMA_VERSION,
    MarketContextEventV1,
    MarketContextKind,
    MarketContextMissingReason,
    MarketContextPrecision,
    MarketContextQueryLimitError,
    MarketContextQueryStatus,
    MarketContextQueryV1,
    MarketContextSourceV1,
    MarketContextTimelineV1,
    MarketContextView,
    StaticMarketContextSourceAdapterV1,
    build_market_context_timeline,
    market_context_calendar_state,
    market_context_information_inputs,
    normalize_market_context_datetime,
    query_market_context,
    query_market_context_window,
)
from histdatacom.synthetic import (
    InformationSplitKind,
    InformationMode,
    InformationScope,
    ReconstructionInformationManifestV1,
    ReconstructionInformationPolicyV1,
    ReconstructionInformationSplitV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    audit_reconstruction_information,
    reconstruction_information_window_plan_id,
)

HOUR_NS = 3_600_000_000_000
DAY_NS = 24 * HOUR_NS
EVENT_TIME_TEXT = "2022-07-13T08:30:00-04:00"
EVENT_TIME_NS = 1_657_715_400_000_000_000


def _source(
    *,
    version: str = "2022-07-06-schedule",
    retrieved_at_ns: int = EVENT_TIME_NS - 6 * DAY_NS,
    digest: str = "a" * 64,
    adapter_name: str = "fixture-macro",
) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name="Operator-approved macro fixture",
        source_version=version,
        retrieved_at_ns=retrieved_at_ns,
        content_sha256=digest,
        adapter_name=adapter_name,
        adapter_version="1.0",
        license_name="Fixture-only license",
        redistribution_allowed=False,
        redistribution_constraints=("Do not redistribute source text.",),
        limitations=("Fixture values are not a licensed production feed.",),
        source_uri="https://example.invalid/macro/fixture",
        metadata={"retrieval_method": "operator_fixture"},
    )


def _initial_event(
    *,
    canonical_key: str = "us.cpi.headline.2022-07",
    source: MarketContextSourceV1 | None = None,
) -> MarketContextEventV1:
    return MarketContextEventV1(
        canonical_key=canonical_key,
        kind=MarketContextKind.MACRO_RELEASE,
        title="US CPI headline",
        source=source or _source(),
        source_event_time=EVENT_TIME_TEXT,
        source_timezone="America/New_York",
        event_time_ns=EVENT_TIME_NS,
        first_known_at_ns=EVENT_TIME_NS - 7 * DAY_NS,
        available_at_ns=EVENT_TIME_NS - 7 * DAY_NS,
        pre_event_ns=30 * 60 * 1_000_000_000,
        post_event_ns=60 * 60 * 1_000_000_000,
        affected_currencies=("usd",),
        affected_symbols=("eurusd", "gbpusd"),
        confidence=1.0,
        precision=MarketContextPrecision.EXACT,
        limitations=("Schedule vintage contains no realized actual value.",),
        vintage_id="schedule-v1",
        expected_value=2.0,
        value_unit="percent_yoy",
        tags=("inflation", "scheduled"),
    )


def _revision_event(initial: MarketContextEventV1) -> MarketContextEventV1:
    return MarketContextEventV1(
        canonical_key=initial.canonical_key,
        kind=initial.kind,
        title=initial.title,
        source=_source(
            version="2022-07-13-initial-release",
            retrieved_at_ns=EVENT_TIME_NS + HOUR_NS,
            digest="b" * 64,
        ),
        source_event_time=initial.source_event_time,
        source_timezone=initial.source_timezone,
        event_time_ns=initial.event_time_ns,
        first_known_at_ns=initial.first_known_at_ns,
        available_at_ns=EVENT_TIME_NS,
        pre_event_ns=initial.pre_event_ns,
        post_event_ns=initial.post_event_ns,
        affected_currencies=initial.affected_currencies,
        affected_symbols=initial.affected_symbols,
        confidence=1.0,
        precision=initial.precision,
        limitations=("Initial actual may be revised by the source.",),
        vintage_id="initial-actual-v1",
        revision_sequence=1,
        supersedes_event_id=initial.event_id,
        expected_value=2.0,
        actual_value=3.0,
        value_unit="percent_yoy",
        tags=initial.tags,
    )


def _timeline(
    *,
    complete: bool = True,
    events: tuple[MarketContextEventV1, ...] | None = None,
) -> MarketContextTimelineV1:
    initial = _initial_event()
    revision = _revision_event(initial)
    return MarketContextTimelineV1(
        timeline_version="fixture-2022-07-v1",
        coverage_start_ns=EVENT_TIME_NS - 30 * DAY_NS,
        coverage_end_ns=EVENT_TIME_NS + 30 * DAY_NS,
        complete=complete,
        events=events if events is not None else (revision, initial),
        limitations=("Only operator-approved fixture events are included.",),
    )


def test_source_and_event_contracts_are_versioned_and_reproducible() -> None:
    source = _source()
    event = _initial_event(source=source)

    assert source.schema_version == MARKET_CONTEXT_SOURCE_SCHEMA_VERSION
    assert event.schema_version == MARKET_CONTEXT_EVENT_SCHEMA_VERSION
    assert event.source.content_sha256 == "a" * 64
    assert event.source.redistribution_allowed is False
    assert event.source.redistribution_constraints
    assert event.window_start_ns == EVENT_TIME_NS - event.pre_event_ns
    assert event.window_end_ns == EVENT_TIME_NS + event.post_event_ns
    assert MarketContextSourceV1.from_json(source.to_json()) == source
    assert MarketContextEventV1.from_json(event.to_json()) == event


def test_timezone_normalization_rejects_ambiguous_and_nonexistent_times() -> (
    None
):
    with pytest.raises(ValueError, match="ambiguous"):
        normalize_market_context_datetime(
            "2022-11-06T01:30:00",
            "America/New_York",
        )

    first = normalize_market_context_datetime(
        "2022-11-06T01:30:00",
        "America/New_York",
        fold=0,
    )
    second = normalize_market_context_datetime(
        "2022-11-06T01:30:00",
        "America/New_York",
        fold=1,
    )
    assert second - first == HOUR_NS

    with pytest.raises(ValueError, match="nonexistent"):
        normalize_market_context_datetime(
            "2022-03-13T02:30:00",
            "America/New_York",
        )
    with pytest.raises(ValueError, match="does not match"):
        normalize_market_context_datetime(
            "2022-07-13T08:30:00-05:00",
            "America/New_York",
        )


def test_event_time_must_match_source_timezone_normalization() -> None:
    with pytest.raises(ValueError, match="does not match normalized"):
        replace(_initial_event(), event_time_ns=EVENT_TIME_NS + 1)


def test_timeline_retains_initial_and_revision_without_overwrite() -> None:
    timeline = _timeline()

    assert timeline.schema_version == MARKET_CONTEXT_TIMELINE_SCHEMA_VERSION
    assert [item.revision_sequence for item in timeline.events] == [0, 1]
    assert timeline.events[1].supersedes_event_id == timeline.events[0].event_id
    assert timeline.events[0].actual_value is None
    assert timeline.events[1].actual_value == 3.0
    assert timeline.events[1].surprise_value == 1.0
    assert MarketContextTimelineV1.from_json(timeline.to_json()) == timeline


def test_timeline_rejects_duplicate_and_invalid_revision_fixtures() -> None:
    initial = _initial_event()
    revision = _revision_event(initial)

    with pytest.raises(ValueError, match="duplicate market context event_id"):
        _timeline(events=(initial, initial))

    orphan = replace(
        revision,
        supersedes_event_id="market-context-event:sha256:" + "0" * 64,
        event_id="",
    )
    with pytest.raises(ValueError, match="predecessor is absent"):
        _timeline(events=(initial, orphan))

    competing_initial = replace(
        initial,
        source=_source(version="competing-source", digest="c" * 64),
        vintage_id="competing-vintage",
        event_id="",
    )
    with pytest.raises(ValueError, match="duplicate logical"):
        _timeline(events=(initial, competing_initial))


def test_ex_ante_view_hides_unavailable_actual_and_revision() -> None:
    timeline = _timeline()
    before_release = query_market_context(
        timeline,
        start_ns=EVENT_TIME_NS - HOUR_NS,
        end_ns=EVENT_TIME_NS + HOUR_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=EVENT_TIME_NS - DAY_NS,
        currencies=("USD",),
    )
    after_release = query_market_context(
        timeline,
        start_ns=EVENT_TIME_NS - HOUR_NS,
        end_ns=EVENT_TIME_NS + HOUR_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=EVENT_TIME_NS + HOUR_NS,
        currencies=("USD",),
    )
    ex_post = query_market_context(
        timeline,
        start_ns=EVENT_TIME_NS - HOUR_NS,
        end_ns=EVENT_TIME_NS + HOUR_NS,
        view=MarketContextView.EX_POST,
        currencies=("USD",),
    )

    assert [item.revision_sequence for item in before_release.events] == [0]
    assert [item.revision_sequence for item in after_release.events] == [0, 1]
    assert [item.revision_sequence for item in ex_post.events] == [0, 1]
    assert (
        MarketContextQueryV1.from_json(after_release.to_json()) == after_release
    )


def test_ex_ante_view_reports_context_not_yet_available() -> None:
    result = query_market_context(
        _timeline(),
        start_ns=EVENT_TIME_NS - HOUR_NS,
        end_ns=EVENT_TIME_NS + HOUR_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=EVENT_TIME_NS - 8 * DAY_NS,
    )

    assert result.status is MarketContextQueryStatus.MISSING
    assert result.missing_reason is (
        MarketContextMissingReason.NOT_AVAILABLE_AS_OF
    )


def test_information_bridge_preserves_vintages_and_revision_lineage() -> None:
    query = query_market_context(
        _timeline(),
        start_ns=EVENT_TIME_NS - HOUR_NS,
        end_ns=EVENT_TIME_NS + HOUR_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=EVENT_TIME_NS + HOUR_NS,
    )

    inputs = market_context_information_inputs(
        query,
        run_id="run-context-fixture",
        used_at_ns=EVENT_TIME_NS + HOUR_NS,
    )

    assert len(inputs) == 2
    assert all(
        item.information_mode is InformationMode.EX_ANTE_SIMULATION
        for item in inputs
    )
    assert inputs[0].scope is InformationScope.POINT_IN_TIME
    assert inputs[1].scope is InformationScope.REVISION
    assert inputs[1].supersedes_input_id == inputs[0].input_id
    assert all(item.available_at_ns <= item.used_at_ns for item in inputs)


def test_known_schedule_binds_at_knowledge_time_not_future_release_time() -> (
    None
):
    used_at_ns = EVENT_TIME_NS - DAY_NS
    query = query_market_context(
        _timeline(),
        start_ns=EVENT_TIME_NS - HOUR_NS,
        end_ns=EVENT_TIME_NS + HOUR_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=used_at_ns,
    )

    inputs = market_context_information_inputs(
        query,
        run_id="run-scheduled-context-fixture",
        used_at_ns=used_at_ns,
    )

    assert len(inputs) == 1
    assert inputs[0].event_time_ns == query.events[0].available_at_ns
    assert inputs[0].event_time_ns < query.events[0].event_time_ns
    assert inputs[0].event_time_ns <= inputs[0].used_at_ns


def test_point_in_time_context_binding_passes_existing_leakage_audit() -> None:
    used_at_ns = EVENT_TIME_NS + HOUR_NS
    policy = ReconstructionInformationPolicyV1(
        information_mode=InformationMode.EX_ANTE_SIMULATION,
    )
    run = ReconstructionRunV1(
        symbols=("EURUSD", "GBPUSD"),
        source_version_ids=("source:fixture",),
        configuration_ids=(policy.policy_id, "context:fixture"),
        ensemble_member_ids=("member-000",),
        base_seed=437,
    )
    window = ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id="member-000",
        symbols=run.symbols,
        core_start_ns=EVENT_TIME_NS - HOUR_NS,
        core_end_ns=EVENT_TIME_NS + 2 * HOUR_NS,
    )
    query = query_market_context_window(
        _timeline(),
        window,
        view=MarketContextView.EX_ANTE,
        as_of_ns=used_at_ns,
    )
    inputs = market_context_information_inputs(
        query,
        run_id=run.run_id,
        used_at_ns=used_at_ns,
    )
    splits = (
        ReconstructionInformationSplitV1(
            kind=InformationSplitKind.TRAIN,
            start_ns=EVENT_TIME_NS - 30 * DAY_NS,
            end_ns=EVENT_TIME_NS - 20 * DAY_NS,
        ),
        ReconstructionInformationSplitV1(
            kind=InformationSplitKind.CALIBRATION,
            start_ns=EVENT_TIME_NS - 20 * DAY_NS,
            end_ns=EVENT_TIME_NS - 10 * DAY_NS,
        ),
        ReconstructionInformationSplitV1(
            kind=InformationSplitKind.VALIDATION,
            start_ns=EVENT_TIME_NS - 10 * DAY_NS,
            end_ns=EVENT_TIME_NS + 10 * DAY_NS,
        ),
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id((window,)),
        inputs=inputs,
        splits=splits,
    )

    report = audit_reconstruction_information(
        manifest,
        policy,
        run=run,
        windows=(window,),
    )

    assert report.accepted is True
    assert report.total_violation_count == 0


def test_pre_and_post_event_windows_have_half_open_overlap_semantics() -> None:
    event = _initial_event()
    timeline = _timeline(events=(event,))

    before = query_market_context(
        timeline,
        start_ns=event.window_start_ns - 2,
        end_ns=event.window_start_ns,
        view=MarketContextView.EX_POST,
        include_calendar=False,
    )
    boundary = query_market_context(
        timeline,
        start_ns=event.window_start_ns,
        end_ns=event.window_start_ns + 1,
        view=MarketContextView.EX_POST,
        include_calendar=False,
    )
    after = query_market_context(
        timeline,
        start_ns=event.window_end_ns,
        end_ns=event.window_end_ns + 1,
        view=MarketContextView.EX_POST,
        include_calendar=False,
    )

    assert before.status is MarketContextQueryStatus.MISSING
    assert [item.event_id for item in boundary.events] == [event.event_id]
    assert after.status is MarketContextQueryStatus.MISSING


def test_unscheduled_context_cannot_silently_claim_exact_time() -> None:
    initial = _initial_event()

    with pytest.raises(ValueError, match="requires non-exact precision"):
        replace(
            initial,
            kind=MarketContextKind.UNSCHEDULED_SHOCK,
            canonical_key="us.unscheduled.fixture",
            event_id="",
        )

    shock = replace(
        initial,
        kind=MarketContextKind.UNSCHEDULED_SHOCK,
        canonical_key="us.unscheduled.fixture",
        precision=MarketContextPrecision.WINDOW_ONLY,
        ambiguity_reason="Publication time is only known to the minute.",
        confidence=0.6,
        event_id="",
    )
    assert shock.precision is MarketContextPrecision.WINDOW_ONLY
    assert shock.ambiguity_reason
    assert shock.confidence == 0.6


def test_streaming_window_join_is_bounded_and_reuses_calendar_state() -> None:
    window = ReconstructionWindowV1(
        run_id="run-context-fixture",
        ensemble_member_id="member-000",
        symbols=("EURUSD", "GBPUSD"),
        core_start_ns=EVENT_TIME_NS - HOUR_NS,
        core_end_ns=EVENT_TIME_NS + HOUR_NS,
    )

    result = query_market_context_window(
        _timeline(),
        window,
        view=MarketContextView.EX_ANTE,
        as_of_ns=EVENT_TIME_NS + HOUR_NS,
        currencies=("USD",),
    )

    assert result.window_id == window.window_id
    assert len(result.events) == 2
    assert result.calendar_state is not None
    assert result.calendar_state.session_state == "market_open"
    assert "london" in result.calendar_state.clock_sessions
    assert "rows" not in result.to_dict()
    assert "dataframe" not in result.to_dict()

    with pytest.raises(MarketContextQueryLimitError):
        query_market_context_window(
            _timeline(),
            window,
            view=MarketContextView.EX_POST,
            max_events=1,
        )


def test_streaming_window_matches_currency_only_context() -> None:
    event = replace(_initial_event(), affected_symbols=(), event_id="")
    window = ReconstructionWindowV1(
        run_id="run-currency-context-fixture",
        ensemble_member_id="member-000",
        symbols=("EURUSD",),
        core_start_ns=EVENT_TIME_NS - HOUR_NS,
        core_end_ns=EVENT_TIME_NS + HOUR_NS,
    )

    result = query_market_context_window(
        _timeline(events=(event,)),
        window,
        view=MarketContextView.EX_POST,
    )

    assert [item.event_id for item in result.events] == [event.event_id]
    assert result.requested_currencies == ("EUR", "USD")


def test_missing_context_is_explicit_inside_outside_and_incomplete() -> None:
    timeline = _timeline()
    quiet_start = EVENT_TIME_NS + 10 * DAY_NS

    quiet = query_market_context(
        timeline,
        start_ns=quiet_start,
        end_ns=quiet_start + HOUR_NS,
        view=MarketContextView.EX_POST,
    )
    outside = query_market_context(
        timeline,
        start_ns=timeline.coverage_end_ns,
        end_ns=timeline.coverage_end_ns + HOUR_NS,
        view=MarketContextView.EX_POST,
    )
    incomplete = query_market_context(
        _timeline(complete=False),
        start_ns=quiet_start,
        end_ns=quiet_start + HOUR_NS,
        view=MarketContextView.EX_POST,
    )

    assert quiet.missing_reason is MarketContextMissingReason.NO_MATCHING_EVENT
    assert outside.missing_reason is (
        MarketContextMissingReason.OUTSIDE_TIMELINE_COVERAGE
    )
    assert incomplete.missing_reason is (
        MarketContextMissingReason.TIMELINE_INCOMPLETE
    )
    assert quiet.calendar_state is not None


def test_adapter_seam_retains_source_identity_and_rejects_mismatch() -> None:
    initial = _initial_event()
    adapter = StaticMarketContextSourceAdapterV1(
        adapter_name="fixture-macro",
        adapter_version="1.0",
        events=(initial,),
    )

    timeline = build_market_context_timeline(
        (adapter,),
        timeline_version="adapter-fixture-v1",
        coverage_start_ns=EVENT_TIME_NS - DAY_NS,
        coverage_end_ns=EVENT_TIME_NS + DAY_NS,
        complete=True,
        limitations=("Fixture adapter only.",),
    )

    assert timeline.events[0].source.source_id == initial.source.source_id
    assert timeline.events[0].source.content_sha256 == "a" * 64

    with pytest.raises(ValueError, match="adapter_name does not match"):
        StaticMarketContextSourceAdapterV1(
            adapter_name="wrong-adapter",
            adapter_version="1.0",
            events=(initial,),
        )


def test_source_requires_explicit_nonredistribution_constraints() -> None:
    with pytest.raises(ValueError, match="require explicit constraints"):
        replace(
            _source(),
            redistribution_constraints=(),
            source_id="",
        )


def test_calendar_state_reuses_rollover_and_period_end_classifier() -> None:
    timestamp_ns = normalize_market_context_datetime(
        "2022-03-31T21:59:00Z",
        "UTC",
    )

    state = market_context_calendar_state(timestamp_ns)

    assert "daily_rollover" in state.special_tags
    assert "month_end" in state.special_tags
    assert "quarter_end" in state.special_tags
    assert state.profile_source == "static_month_day_major_holidays"
    assert state.profile_complete is False
    assert state.limitations

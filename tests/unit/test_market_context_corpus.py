"""Production-source and artifact tests for the market-context corpus."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from histdatacom.data_analytics.cli import main as analytics_main
from histdatacom.market_context import (
    MARKET_CONTEXT_CORPUS_SCHEMA_VERSION,
    BankOfEnglandBankRateAdapterV1,
    EcbPolicyRateAdapterV1,
    FederalReserveFomcCalendarAdapterV1,
    FederalReserveFomcHistoricalAdapterV1,
    MarketContextCorpusBuildV1,
    MarketContextCorpusPreflightError,
    MarketContextFetchProfileV1,
    MarketContextKind,
    MarketContextSourceEvidenceV1,
    MarketContextSourceSnapshotV1,
    MarketContextTimelineV1,
    MarketContextView,
    OnsReleaseCalendarAdapterV1,
    OperatorMarketContextCatalogAdapterV1,
    build_live_market_context_corpus,
    build_market_context_corpus_from_snapshots,
    market_context_benchmark_event_state,
    normalize_market_context_datetime,
    packaged_operator_catalog_path,
    preflight_market_context_corpus,
    query_market_context,
    query_market_context_corpus,
    read_market_context_corpus,
    replay_market_context_corpus,
    require_market_context_corpus,
    write_market_context_corpus,
)

DAY_NS = 86_400_000_000_000
RETRIEVED_NS = normalize_market_context_datetime(
    "2030-01-01T00:00:00+00:00", "UTC"
)


def _snapshot(
    *,
    source_key: str,
    content: bytes,
    adapter_name: str,
    content_type: str,
) -> MarketContextSourceSnapshotV1:
    return MarketContextSourceSnapshotV1(
        source_key=source_key,
        source_name=f"Fixture {source_key}",
        source_uri=f"https://example.invalid/{source_key}",
        retrieved_at_ns=RETRIEVED_NS,
        content=content,
        content_type=content_type,
        adapter_name=adapter_name,
        adapter_version="1.0",
        license_name="Official-source fixture terms",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute the fixture provider.",),
        limitations=("Parser fixture, not a production acquisition.",),
        metadata={"fixture": True},
    )


def _operator_snapshot(
    payload: dict[str, object],
    *,
    source_key: str = "operator.shock-catalog",
) -> MarketContextSourceSnapshotV1:
    return _snapshot(
        source_key=source_key,
        content=json.dumps(payload, sort_keys=True).encode("utf-8"),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )


def _ecb_daily_csv(
    start: date,
    end: date,
    changes: dict[date, float],
) -> bytes:
    rows = ["KEY,TIME_PERIOD,OBS_VALUE,TITLE"]
    current: float | None = None
    observed = start
    while observed <= end:
        if observed in changes:
            current = changes[observed]
        if current is None:
            raise AssertionError("ECB fixture requires an initial level")
        rows.append(
            "FM.D.U2.EUR.4F.KR.MRR_RT.LEV,"
            f"{observed.isoformat()},{current},Main refinancing operations"
        )
        observed += timedelta(days=1)
    return ("\n".join(rows) + "\n").encode("utf-8")


def test_ons_adapter_is_point_in_time_conservative_and_filters_titles() -> None:
    payload = {
        "breakdown": {"total": 4},
        "releases": [
            {
                "uri": "/releases/consumerpriceinflationukjanuary2025",
                "date_changes": [
                    {
                        "previous_date": "2025-02-18T07:00:00.000Z",
                        "change_notice": "Rescheduled.",
                    }
                ],
                "description": {
                    "title": "Consumer price inflation, UK: January 2025",
                    "release_date": "2025-02-19T07:00:00.000Z",
                    "cancelled": False,
                },
            },
            {
                "uri": "/releases/unrelated",
                "date_changes": None,
                "description": {
                    "title": "Population estimates",
                    "release_date": "2025-02-20T07:00:00.000Z",
                    "cancelled": False,
                },
            },
            {
                "uri": "/releases/retailsalescancelled",
                "date_changes": None,
                "description": {
                    "title": "Retail sales, Great Britain: January 2025",
                    "release_date": "2025-02-21T07:00:00.000Z",
                    "cancelled": True,
                },
            },
            {
                "uri": "/releases/labourmarketmissingdate",
                "date_changes": None,
                "description": {
                    "title": "Labour market overview, UK: February 2025",
                    "release_date": "",
                    "cancelled": False,
                },
            },
        ],
    }
    adapter = OnsReleaseCalendarAdapterV1(
        _snapshot(
            source_key="ons.q00.p00",
            content=json.dumps(payload).encode("utf-8"),
            adapter_name=OnsReleaseCalendarAdapterV1.adapter_name,
            content_type="application/json",
        )
    )

    events = tuple(adapter.load_events())

    assert len(events) == 1
    event = events[0]
    assert event.kind is MarketContextKind.MACRO_RELEASE
    assert event.affected_currencies == ("GBP",)
    assert event.first_known_at_ns == event.event_time_ns
    assert event.available_at_ns == event.event_time_ns
    assert "schedule_changed_without_change_timestamp" in event.tags
    assert "cancelled_release:2" in adapter.diagnostics
    assert "missing_release_field:3" in adapter.diagnostics


def test_ecb_and_boe_adapters_preserve_date_only_limitations() -> None:
    ecb_csv = (
        "TIME_PERIOD,OBS_VALUE,TITLE,OBS_STATUS\n"
        "2022-07-27,0.5,Main refinancing operations,A\n"
        "2022-09-14,,Main refinancing operations,A\n"
    ).encode("utf-8")
    ecb = EcbPolicyRateAdapterV1(
        _snapshot(
            source_key="ecb.policy-rate",
            content=ecb_csv,
            adapter_name=EcbPolicyRateAdapterV1.adapter_name,
            content_type="text/csv",
        )
    )
    ecb_events = tuple(ecb.load_events())

    boe_html = b"""
      <table id="stats-table"><thead><tr><th>Date Changed</th><th>Rate</th></tr></thead>
      <tbody><tr><td>07 Nov 24</td><td>4.75</td></tr>
      <tr><td>missing</td><td></td></tr></tbody></table>
    """
    boe = BankOfEnglandBankRateAdapterV1(
        _snapshot(
            source_key="boe.bank-rate",
            content=boe_html,
            adapter_name=BankOfEnglandBankRateAdapterV1.adapter_name,
            content_type="text/html",
        )
    )
    boe_events = tuple(boe.load_events())

    assert len(ecb_events) == 1
    assert ecb_events[0].actual_value == 0.5
    assert ecb_events[0].precision.value == "window_only"
    assert "missing_rate_field:1" in ecb.diagnostics
    assert len(boe_events) == 1
    assert boe_events[0].actual_value == 4.75
    assert boe_events[0].affected_symbols == ("EURGBP", "GBPUSD")
    assert "invalid_bank_rate_row:1" in boe.diagnostics


def test_ecb_adapter_collapses_daily_state_across_variable_rate_era() -> None:
    ecb = EcbPolicyRateAdapterV1(
        _snapshot(
            source_key="ecb.policy-rate",
            content=_ecb_daily_csv(
                date(2002, 12, 1),
                date(2003, 3, 10),
                {
                    date(2002, 12, 1): 3.25,
                    date(2002, 12, 6): 2.75,
                    date(2003, 3, 7): 2.5,
                },
            ),
            adapter_name=EcbPolicyRateAdapterV1.adapter_name,
            content_type="text/csv",
        )
    )

    events = tuple(ecb.load_events())

    assert [item.actual_value for item in events] == [3.25, 2.75, 2.5]
    assert [item.previous_value for item in events] == [None, 3.25, 2.75]
    assert events[1].source_event_time == "2002-12-06T00:00:00+00:00"
    assert "effective_rate_change" in events[1].tags
    assert ecb.coverage_complete is True


def test_ecb_adapter_marks_non_contiguous_daily_state_incomplete() -> None:
    ecb = EcbPolicyRateAdapterV1(
        _snapshot(
            source_key="ecb.policy-rate",
            content=(
                b"KEY,TIME_PERIOD,OBS_VALUE,TITLE\n"
                b"FM.D.U2.EUR.4F.KR.MRR_RT.LEV,2002-01-01,3.25,MRO\n"
                b"FM.D.U2.EUR.4F.KR.MRR_RT.LEV,2002-01-03,3.25,MRO\n"
            ),
            adapter_name=EcbPolicyRateAdapterV1.adapter_name,
            content_type="text/csv",
        )
    )

    tuple(ecb.load_events())

    assert ecb.coverage_complete is False
    assert ecb.diagnostics == (
        "non_contiguous_rate_date:2002-01-01:2002-01-03",
    )


def test_ecb_coverage_is_policy_change_not_decision_completeness() -> None:
    snapshot = _snapshot(
        source_key="ecb.policy-rate",
        content=_ecb_daily_csv(
            date(2023, 1, 1),
            date(2023, 1, 31),
            {date(2023, 1, 1): 2.5},
        ),
        adapter_name=EcbPolicyRateAdapterV1.adapter_name,
        content_type="text/csv",
    )
    corpus = build_market_context_corpus_from_snapshots(
        (snapshot,),
        profile=MarketContextFetchProfileV1(
            start_date="2023-01-01",
            end_date="2023-01-31",
            sources=("ecb",),
        ),
    ).corpus

    assert (
        corpus.timeline.events[0].kind is MarketContextKind.POLICY_RATE_CHANGE
    )
    assert [item.kind for item in corpus.coverage] == [
        MarketContextKind.POLICY_RATE_CHANGE
    ]


def test_federal_reserve_adapters_cover_current_cross_month_and_history() -> (
    None
):
    current_html = b"""
      <h4><a>2024 FOMC Meetings</a></h4>
      <div class="fomc-meeting"><div class="fomc-meeting__month"><strong>Apr/May</strong></div>
      <div class="fomc-meeting__date">30-1</div></div>
      <div class="fomc-meeting"><div class="fomc-meeting__month"><strong>August</strong></div>
      <div class="fomc-meeting__date">22 (notation vote)</div></div>
    """
    current = FederalReserveFomcCalendarAdapterV1(
        _snapshot(
            source_key="fed.fomc-calendar",
            content=current_html,
            adapter_name=FederalReserveFomcCalendarAdapterV1.adapter_name,
            content_type="text/html",
        )
    )
    current_events = tuple(current.load_events())

    historical_html = b"""
      <h5>January 29-30 Meeting - 2008</h5>
      <h5>March 10 Conference Call - 2008</h5>
      <h5>April 29-30 (cancelled) Meeting - 2008</h5>
      <h5>October 8 (unscheduled) Meeting - 2008</h5>
    """
    historical = FederalReserveFomcHistoricalAdapterV1(
        _snapshot(
            source_key="fed.fomc-historical.2008",
            content=historical_html,
            adapter_name=FederalReserveFomcHistoricalAdapterV1.adapter_name,
            content_type="text/html",
        )
    )
    historical_events = tuple(historical.load_events())

    assert [item.canonical_key for item in current_events] == [
        "federal-reserve.fomc.2024-05-01",
        "federal-reserve.fomc.2024-08-22",
    ]
    assert current_events[0].precision.value == "exact"
    assert current_events[1].precision.value == "window_only"
    assert [item.canonical_key for item in historical_events] == [
        "federal-reserve.fomc.2008-01-30",
        "federal-reserve.fomc.2008-10-08",
    ]
    assert (
        historical_events[0].available_at_ns
        > historical_events[0].event_time_ns
    )
    assert "unscheduled" in historical_events[1].tags
    assert "cancelled_historical_meeting:2008-04-30" in historical.diagnostics


def test_operator_adapter_retains_revisions_and_late_publication() -> None:
    common = {
        "affected_currencies": ["USD"],
        "affected_symbols": ["EURUSD", "GBPUSD"],
        "canonical_key": "operator.macro.fixture.2022-07-13",
        "confidence": 1.0,
        "first_known_at": "2022-07-06T08:30:00-04:00",
        "kind": "macro_release",
        "limitations": ["Point-in-time adapter fixture."],
        "post_event_ns": 3_600_000_000_000,
        "pre_event_ns": 1_800_000_000_000,
        "precision": "exact",
        "source_event_time": "2022-07-13T08:30:00-04:00",
        "source_timezone": "America/New_York",
        "tags": ["fixture", "scheduled"],
        "title": "US CPI fixture",
        "value_unit": "percent_yoy",
    }
    payload = {
        "schema_version": "histdatacom.operator-market-context-catalog.v1",
        "events": [
            {
                **common,
                "available_at": "2022-07-06T08:30:00-04:00",
                "expected_value": 2.0,
                "revision_sequence": 0,
                "vintage_id": "schedule-v1",
            },
            {
                **common,
                "actual_value": 3.0,
                "available_at": "2022-07-13T10:00:00-04:00",
                "expected_value": 2.0,
                "revision_sequence": 1,
                "vintage_id": "late-actual-v1",
            },
        ],
    }
    events = tuple(
        OperatorMarketContextCatalogAdapterV1(
            _operator_snapshot(payload)
        ).load_events()
    )
    event_time = events[0].event_time_ns
    timeline = MarketContextTimelineV1(
        timeline_version="operator-revision-test",
        coverage_start_ns=event_time - DAY_NS,
        coverage_end_ns=event_time + DAY_NS,
        complete=True,
        events=events,
        limitations=("Fixture timeline.",),
    )

    before_late_actual = query_market_context(
        timeline,
        start_ns=event_time - 1,
        end_ns=event_time + 1,
        view=MarketContextView.EX_ANTE,
        as_of_ns=normalize_market_context_datetime(
            "2022-07-13T09:00:00-04:00", "America/New_York"
        ),
        currencies=("USD",),
        include_calendar=False,
    )
    after_late_actual = query_market_context(
        timeline,
        start_ns=event_time - 1,
        end_ns=event_time + 1,
        view=MarketContextView.EX_ANTE,
        as_of_ns=normalize_market_context_datetime(
            "2022-07-13T10:01:00-04:00", "America/New_York"
        ),
        currencies=("USD",),
        include_calendar=False,
    )

    assert [item.revision_sequence for item in events] == [0, 1]
    assert events[1].supersedes_event_id == events[0].event_id
    assert events[1].surprise_value == 1.0
    assert [item.revision_sequence for item in before_late_actual.events] == [0]
    assert [item.revision_sequence for item in after_late_actual.events] == [
        0,
        1,
    ]


def test_operator_adapter_fails_closed_on_dst_and_missing_values() -> None:
    event = {
        "affected_currencies": ["USD"],
        "affected_symbols": ["EURUSD"],
        "ambiguity_reason": "Window-only fixture.",
        "available_at": "2022-11-06T01:30:00",
        "canonical_key": "operator.shock.dst.2022-11-06",
        "confidence": 1.0,
        "first_known_at": "2022-11-06T01:30:00",
        "kind": "unscheduled_shock",
        "limitations": ["DST fixture."],
        "post_event_ns": DAY_NS,
        "pre_event_ns": 0,
        "precision": "window_only",
        "revision_sequence": 0,
        "source_event_time": "2022-11-06T01:30:00",
        "source_timezone": "America/New_York",
        "tags": ["fixture"],
        "title": "DST fixture",
        "vintage_id": "dst-v1",
    }
    payload = {
        "schema_version": "histdatacom.operator-market-context-catalog.v1",
        "events": [event],
    }
    with pytest.raises(ValueError, match="ambiguous"):
        tuple(
            OperatorMarketContextCatalogAdapterV1(
                _operator_snapshot(payload)
            ).load_events()
        )

    payload["events"] = [{**event, "source_time_fold": 1}]
    parsed = tuple(
        OperatorMarketContextCatalogAdapterV1(
            _operator_snapshot(payload)
        ).load_events()
    )
    assert parsed[0].source_time_fold == 1

    payload["events"] = [
        {
            **{key: value for key, value in event.items() if key != "title"},
            "source_time_fold": 1,
        }
    ]
    with pytest.raises(ValueError, match="required text"):
        tuple(
            OperatorMarketContextCatalogAdapterV1(
                _operator_snapshot(payload)
            ).load_events()
        )


def test_corpus_artifacts_replay_and_do_not_rewrite_prior_content(
    tmp_path: Path,
) -> None:
    content = packaged_operator_catalog_path().read_bytes()
    snapshot = _snapshot(
        source_key="operator.shock-catalog",
        content=content,
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )
    profile = MarketContextFetchProfileV1(
        start_date="2001-01-01",
        end_date="2023-12-31",
        sources=("operator",),
    )
    build = build_market_context_corpus_from_snapshots(
        (snapshot,), profile=profile
    )

    artifacts = write_market_context_corpus(build, tmp_path)
    loaded = read_market_context_corpus(artifacts["corpus"].path)
    replayed = replay_market_context_corpus(artifacts["corpus"].path)
    repeated = write_market_context_corpus(build, tmp_path)

    assert loaded.schema_version == MARKET_CONTEXT_CORPUS_SCHEMA_VERSION
    assert replayed.corpus.corpus_id == loaded.corpus_id
    assert repeated["corpus"].sha256 == artifacts["corpus"].sha256
    assert len(loaded.timeline.events) == 6
    assert Path(artifacts["timeline"].path).exists()
    raw = next((tmp_path / "sources").glob("operator.shock-catalog-*.json"))
    raw.write_bytes(b"different")
    with pytest.raises(ValueError, match="different content"):
        write_market_context_corpus(build, tmp_path)
    corpus_path = Path(artifacts["corpus"].path)
    corpus_path.write_bytes(corpus_path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash differs from name"):
        read_market_context_corpus(corpus_path)


def test_corpus_rejects_internally_inconsistent_reports_and_provenance() -> (
    None
):
    snapshot = _snapshot(
        source_key="operator.shock-catalog",
        content=packaged_operator_catalog_path().read_bytes(),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )
    build = build_market_context_corpus_from_snapshots(
        (snapshot,),
        profile=MarketContextFetchProfileV1(
            start_date="2001-01-01",
            end_date="2023-12-31",
            sources=("operator",),
        ),
    )
    counts = dict(build.corpus.counts_by_year_currency_kind)
    first_count = next(iter(counts))
    counts[first_count] += 1
    with pytest.raises(ValueError, match="counts differ"):
        replace(
            build.corpus,
            counts_by_year_currency_kind=counts,
            corpus_id="",
        )

    coverage = list(build.corpus.coverage)
    coverage[0] = replace(coverage[0], event_count=coverage[0].event_count + 1)
    with pytest.raises(ValueError, match="coverage count differs"):
        replace(build.corpus, coverage=tuple(coverage), corpus_id="")

    events = list(build.corpus.timeline.events)
    changed_source = replace(
        events[0].source,
        license_name="Different source terms",
        source_id="",
    )
    events[0] = replace(events[0], source=changed_source, event_id="")
    changed_timeline = replace(
        build.corpus.timeline, events=tuple(events), timeline_id=""
    )
    with pytest.raises(ValueError, match="provenance differs"):
        replace(
            build.corpus,
            timeline=changed_timeline,
            corpus_id="",
        )


def test_corpus_build_rejects_snapshot_evidence_mismatch() -> None:
    snapshot = _snapshot(
        source_key="operator.shock-catalog",
        content=packaged_operator_catalog_path().read_bytes(),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )
    build = build_market_context_corpus_from_snapshots(
        (snapshot,),
        profile=MarketContextFetchProfileV1(
            start_date="2001-01-01",
            end_date="2023-12-31",
            sources=("operator",),
        ),
    )
    changed_snapshot = replace(
        snapshot, source_uri="https://example.invalid/changed-source"
    )
    with pytest.raises(ValueError, match="snapshot differs"):
        MarketContextCorpusBuildV1(
            corpus=build.corpus, snapshots=(changed_snapshot,)
        )


def test_source_evidence_enforces_reuse_and_limitation_invariants() -> None:
    snapshot = _snapshot(
        source_key="operator.shock-catalog",
        content=packaged_operator_catalog_path().read_bytes(),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )
    evidence = MarketContextSourceEvidenceV1.from_snapshot(
        snapshot, event_count=0
    )

    with pytest.raises(ValueError, match="requires limitations"):
        replace(evidence, limitations=())
    with pytest.raises(ValueError, match="requires constraints"):
        replace(
            evidence,
            redistribution_allowed=False,
            redistribution_constraints=(),
        )


@pytest.mark.parametrize(
    ("timeout_seconds", "max_runtime_seconds"),
    ((float("inf"), 300.0), (30.0, float("inf"))),
)
def test_fetch_profile_rejects_infinite_runtime_bounds(
    timeout_seconds: float, max_runtime_seconds: float
) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        MarketContextFetchProfileV1(
            start_date="2023-01-01",
            end_date="2023-01-02",
            sources=("operator",),
            timeout_seconds=timeout_seconds,
            max_runtime_seconds=max_runtime_seconds,
        )


def test_corpus_preflight_distinguishes_support_from_no_matching_event() -> (
    None
):
    operator = _snapshot(
        source_key="operator.shock-catalog",
        content=packaged_operator_catalog_path().read_bytes(),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )
    ecb = _snapshot(
        source_key="ecb.policy-rate",
        content=_ecb_daily_csv(
            date(2022, 1, 1),
            date(2023, 12, 31),
            {
                date(2022, 1, 1): 0.0,
                date(2022, 7, 27): 0.5,
            },
        ),
        adapter_name=EcbPolicyRateAdapterV1.adapter_name,
        content_type="text/csv",
    )
    profile = MarketContextFetchProfileV1(
        start_date="2001-01-01",
        end_date="2023-12-31",
        sources=("ecb", "operator"),
    )
    corpus = build_market_context_corpus_from_snapshots(
        (operator, ecb), profile=profile
    ).corpus
    start = normalize_market_context_datetime(
        "2023-01-10T00:00:00+00:00", "UTC"
    )
    end = start + DAY_NS

    ready = preflight_market_context_corpus(
        corpus,
        start_ns=start,
        end_ns=end,
        currencies=("EUR",),
        kinds=(MarketContextKind.POLICY_RATE_CHANGE,),
    )
    query = query_market_context_corpus(
        corpus,
        start_ns=start,
        end_ns=end,
        view=MarketContextView.EX_POST,
        currencies=("EUR",),
        include_calendar=False,
    )

    assert ready.ready is True
    assert query.missing_reason is not None
    assert query.missing_reason.value == "no_matching_event"
    assert market_context_benchmark_event_state(query).endswith(
        "no_matching_event"
    )

    unsupported = preflight_market_context_corpus(
        corpus,
        start_ns=start,
        end_ns=end,
        currencies=("USD",),
        kinds=(MarketContextKind.CENTRAL_BANK_DECISION,),
    )
    assert unsupported.ready is False
    with pytest.raises(MarketContextCorpusPreflightError):
        require_market_context_corpus(
            corpus,
            start_ns=start,
            end_ns=end,
            currencies=("USD",),
            kinds=(MarketContextKind.CENTRAL_BANK_DECISION,),
        )

    shocks = preflight_market_context_corpus(
        corpus,
        start_ns=start,
        end_ns=end,
        currencies=("EUR",),
        kinds=(MarketContextKind.UNSCHEDULED_SHOCK,),
    )
    assert shocks.ready is False

    with pytest.raises(MarketContextCorpusPreflightError):
        query_market_context_corpus(
            corpus,
            start_ns=start,
            end_ns=end,
            view=MarketContextView.EX_POST,
            currencies=("USD",),
            kinds=(MarketContextKind.CENTRAL_BANK_DECISION,),
            include_calendar=False,
        )


def test_corpus_deduplicates_query_specific_ons_highlights() -> None:
    item = {
        "uri": "/releases/consumerpriceinflationukjanuary2025",
        "date_changes": None,
        "description": {
            "title": "Consumer price inflation, UK: January 2025",
            "release_date": "2025-02-19T07:00:00.000Z",
            "cancelled": False,
        },
        "highlight": {"title": "<em>Consumer</em> price inflation"},
    }
    other = json.loads(json.dumps(item))
    other["highlight"] = {"title": "Consumer price <em>inflation</em>"}
    snapshots = tuple(
        _snapshot(
            source_key=f"ons.q{index:02d}.p00",
            content=json.dumps(
                {"breakdown": {"total": 1}, "releases": [release]}
            ).encode("utf-8"),
            adapter_name=OnsReleaseCalendarAdapterV1.adapter_name,
            content_type="application/json",
        )
        for index, release in enumerate((item, other))
    )

    corpus = build_market_context_corpus_from_snapshots(
        snapshots,
        profile=MarketContextFetchProfileV1(
            start_date="2025-01-01",
            end_date="2025-12-31",
            sources=("ons",),
        ),
    ).corpus

    assert len(corpus.timeline.events) == 1
    assert corpus.duplicate_event_count == 1


def test_corpus_rejects_conflicting_duplicate_logical_events() -> None:
    first = {
        "breakdown": {"total": 1},
        "releases": [
            {
                "uri": "/releases/retailsalesgreatbritainjanuary2025",
                "date_changes": None,
                "description": {
                    "title": "Retail sales, Great Britain: January 2025",
                    "release_date": "2025-02-21T07:00:00.000Z",
                    "cancelled": False,
                },
            }
        ],
    }
    second = json.loads(json.dumps(first))
    second["releases"][0]["description"][
        "release_date"
    ] = "2025-02-21T08:00:00.000Z"
    snapshots = (
        _snapshot(
            source_key="ons.q00.p00",
            content=json.dumps(first).encode("utf-8"),
            adapter_name=OnsReleaseCalendarAdapterV1.adapter_name,
            content_type="application/json",
        ),
        _snapshot(
            source_key="ons.q01.p00",
            content=json.dumps(second).encode("utf-8"),
            adapter_name=OnsReleaseCalendarAdapterV1.adapter_name,
            content_type="application/json",
        ),
    )
    profile = MarketContextFetchProfileV1(
        start_date="2025-01-01",
        end_date="2025-12-31",
        sources=("ons",),
    )

    with pytest.raises(ValueError, match="conflicting duplicate"):
        build_market_context_corpus_from_snapshots(snapshots, profile=profile)


def test_snapshot_builder_rejects_profile_source_drift() -> None:
    operator = _snapshot(
        source_key="operator.shock-catalog",
        content=packaged_operator_catalog_path().read_bytes(),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )

    with pytest.raises(ValueError, match="families differ"):
        build_market_context_corpus_from_snapshots(
            (operator,),
            profile=MarketContextFetchProfileV1(
                start_date="2001-01-01",
                end_date="2023-12-31",
                sources=("operator", "ecb"),
            ),
        )


def test_live_acquisition_enforces_running_total_byte_budget(
    monkeypatch,
) -> None:
    calls: list[str] = []
    closed: list[bool] = []

    class _Response:
        headers = {
            "Content-Length": "6",
            "Content-Type": "application/octet-stream",
        }

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, *, chunk_size: int):
            del chunk_size
            yield b"123456"

        def close(self) -> None:
            closed.append(True)

    def fake_get(uri, **kwargs):
        del kwargs
        calls.append(uri)
        return _Response()

    import histdatacom.market_context.corpus as corpus_module

    monkeypatch.setattr(corpus_module.requests, "get", fake_get)
    profile = MarketContextFetchProfileV1(
        start_date="2023-01-01",
        end_date="2023-01-02",
        sources=("ecb", "boe"),
        max_response_bytes=10,
        max_total_source_bytes=10,
    )

    with pytest.raises(ValueError, match="declared byte limit"):
        build_live_market_context_corpus(profile)

    assert len(calls) == 2
    assert len(closed) == 2


def test_packaged_date_only_shock_is_not_visible_intraday_ex_ante() -> None:
    snapshot = _snapshot(
        source_key="operator.shock-catalog",
        content=packaged_operator_catalog_path().read_bytes(),
        adapter_name=OperatorMarketContextCatalogAdapterV1.adapter_name,
        content_type="application/json",
    )
    corpus = build_market_context_corpus_from_snapshots(
        (snapshot,),
        profile=MarketContextFetchProfileV1(
            start_date="2001-09-11",
            end_date="2001-09-11",
            sources=("operator",),
        ),
    ).corpus
    start = normalize_market_context_datetime(
        "2001-09-11T00:00:00+00:00", "UTC"
    )

    hidden = query_market_context_corpus(
        corpus,
        start_ns=start,
        end_ns=start + DAY_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=start + 60 * 60 * 1_000_000_000,
        currencies=("USD",),
        include_calendar=False,
    )
    visible = query_market_context_corpus(
        corpus,
        start_ns=start,
        end_ns=start + DAY_NS,
        view=MarketContextView.EX_ANTE,
        as_of_ns=start + DAY_NS,
        currencies=("USD",),
        include_calendar=False,
    )

    assert hidden.missing_reason is not None
    assert hidden.missing_reason.value == "not_available_as_of"
    assert [item.title for item in visible.events] == ["September 11 attacks"]


def test_market_context_cli_writes_operator_only_corpus(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact_dir = tmp_path / "context"

    exit_code = analytics_main(
        [
            "market-context-corpus",
            "--artifact-dir",
            str(artifact_dir),
            "--start-date",
            "2001-01-01",
            "--end-date",
            "2023-12-31",
            "--sources",
            "operator",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_count"] == 1
    assert payload["event_count"] == 6
    assert Path(payload["artifacts"]["corpus"]["path"]).exists()
    assert Path(payload["artifacts"]["timeline"]["path"]).exists()


def test_market_context_cli_reads_yaml_config(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact_dir = tmp_path / "configured-context"
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        "\n".join(
            (
                "histdatacom:",
                "  analytics:",
                "    command: market-context-corpus",
                f"    artifact_dir: {artifact_dir}",
                "    start_date: 2001-01-01",
                "    end_date: 2023-12-31",
                "    sources:",
                "      - operator",
                "    ons_queries:",
                "      - consumer price inflation",
                "    max_events: 32",
                "    max_runtime_seconds: 30",
                "    json: true",
            )
        ),
        encoding="utf-8",
    )

    exit_code = analytics_main(["--config", str(config_path)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_count"] == 1
    assert payload["event_count"] == 6


def test_market_context_cli_reports_invalid_profile_cleanly(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = analytics_main(
        [
            "market-context-corpus",
            "--artifact-dir",
            str(tmp_path),
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-01",
            "--sources",
            "operator",
        ]
    )

    assert exit_code == 1
    assert "market-context corpus error:" in capsys.readouterr().err

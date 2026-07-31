"""Comprehensive economic-calendar source, vintage, and query tests."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from histdatacom.data_analytics.cli import main as analytics_main
from histdatacom.fx_enums import Pairs
from histdatacom.market_context import (
    DEFAULT_ECONOMIC_CALENDAR_ECONOMIES,
    EconomicCalendarFetchProfileV1,
    MarketContextMissingReason,
    MarketContextQueryStatus,
    MarketContextView,
    TradingEconomicsCalendarAdapterV1,
    build_economic_calendar_corpus_from_snapshots,
    context_corpus_artifact_kind,
    context_corpus_event_times,
    economic_calendar_fetch_plan,
    fetch_trading_economics_calendar_snapshots,
    histdata_economy_symbols,
    histdata_pair_economies,
    parse_calendar_number,
    preflight_context_corpus,
    query_context_corpus,
    query_economic_calendar_corpus,
    read_context_corpus,
    read_economic_calendar_corpus,
    replay_economic_calendar_corpus,
    trading_economics_request_uri,
    validate_api_key_env_name,
    write_economic_calendar_corpus,
)
from histdatacom.market_context.corpus import MarketContextSourceSnapshotV1


def _ns(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.astimezone(timezone.utc).timestamp() * 1_000_000_000)


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "CalendarId": "319282",
        "Date": "2023-04-03T14:00:00",
        "Country": "United States",
        "Category": "Business Confidence",
        "Event": "ISM Manufacturing PMI",
        "Reference": "Mar",
        "ReferenceDate": "2023-03-31T00:00:00",
        "Source": "Institute for Supply Management",
        "SourceURL": "https://www.ismworld.org",
        "Actual": "46.3",
        "Previous": "47.7",
        "Forecast": "47.5",
        "TEForecast": "49",
        "ActualValue": 46.3,
        "PreviousValue": 47.7,
        "ForecastValue": 47.5,
        "TEForecastValue": 49.0,
        "URL": "/united-states/business-confidence",
        "DateSpan": "0",
        "Importance": 3,
        "LastUpdate": "2023-04-03T14:00:00.680",
        "Revised": "47.6",
        "Currency": "",
        "Unit": "",
        "Ticker": "NAPMPMI",
        "Symbol": "NAPMPMI",
    }
    row.update(overrides)
    return row


def _snapshot(
    rows: list[dict[str, Any]],
    *,
    retrieved_at: str = "2023-05-01T00:00:00Z",
    start_date: str = "2023-04-01",
    end_date: str = "2023-04-30",
    complete_leaf: bool = True,
) -> MarketContextSourceSnapshotV1:
    content = json.dumps(rows, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    source_key = (
        f"trading-economics.united-states.{start_date}.{end_date}."
        f"{digest[:16]}"
    )
    return MarketContextSourceSnapshotV1(
        source_key=source_key,
        source_name="Trading Economics economic calendar",
        source_uri=trading_economics_request_uri(
            "United States",
            datetime.fromisoformat(start_date).date(),
            datetime.fromisoformat(end_date).date(),
        ),
        retrieved_at_ns=_ns(retrieved_at),
        content=content,
        content_type="application/json",
        adapter_name="trading-economics-calendar",
        adapter_version="1.0.0",
        license_name="Trading Economics subscription terms",
        redistribution_allowed=False,
        redistribution_constraints=("local licensed use only",),
        limitations=("historical first-known schedule time is unavailable",),
        metadata={
            "economy": "United States",
            "currency": "USD",
            "start_date": start_date,
            "end_date": end_date,
            "row_count": len(rows),
            "complete_leaf": complete_leaf,
        },
    )


def _profile() -> EconomicCalendarFetchProfileV1:
    return EconomicCalendarFetchProfileV1(
        start_date="2023-04-01",
        end_date="2023-04-30",
        economies=("United States",),
    )


def test_histdata_pair_matrix_covers_every_public_instrument() -> None:
    pairs = histdata_pair_economies()
    assert set(pairs) == Pairs.list_keys()
    assert len(pairs) == 66
    assert pairs["eurusd"] == ("Euro Area", "United States")
    assert pairs["grxeur"] == ("Euro Area", "Germany")
    assert pairs["xaugbp"] == ("United Kingdom",)

    symbols = histdata_economy_symbols()
    assert set(symbols) == set(DEFAULT_ECONOMIC_CALENDAR_ECONOMIES)
    assert "eurusd" in symbols["Euro Area"]
    assert "grxeur" in symbols["Germany"]
    assert "xaugbp" in symbols["United Kingdom"]


def test_full_history_plan_is_secret_free_and_bounded() -> None:
    profile = EconomicCalendarFetchProfileV1(
        start_date="2000-01-01", end_date="2026-07-30"
    )
    plan = economic_calendar_fetch_plan(profile)
    assert plan["pair_count"] == 66
    assert plan["economy_count"] == 21
    assert plan["initial_request_count"] == 567
    requests = plan["requests"]
    assert isinstance(requests, list)
    assert "Authorization" not in json.dumps(requests)
    assert "c=" not in str(requests[0])


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    ("raw", "expected"),
    [
        ("$-70.5B", -70_500_000_000.0),
        ("(1.2M)", -1_200_000.0),
        ("3.4%", 3.4),
        ("198K", 198_000.0),
        ("", None),
        ("N/A", None),
    ],
)
def test_calendar_number_parser_preserves_scale(
    raw: str, expected: float | None
) -> None:
    assert parse_calendar_number(raw) == expected


def test_adapter_preserves_consensus_actual_previous_and_revision() -> None:
    snapshot = _snapshot([_row()])
    events = TradingEconomicsCalendarAdapterV1(snapshot).load_events()
    assert len(events) == 1
    event = events[0]
    assert event.actual_value == 46.3
    assert event.previous_value == 47.7
    assert event.forecast_value == 47.5
    assert event.provider_forecast_value == 49.0
    assert event.revised_value == 47.6
    assert event.importance == 3
    assert event.reference == "Mar"
    assert event.currency == "USD"
    assert "eurusd" in event.affected_symbols
    assert event.available_at_ns == _ns("2023-04-03T14:00:00.680Z")
    assert event.observed_at_ns == snapshot.retrieved_at_ns


def test_query_hides_historical_backfill_until_release_availability() -> None:
    build = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([_row()]),), profile=_profile()
    )
    start = _ns("2023-04-03T13:30:00Z")
    end = _ns("2023-04-03T14:30:00Z")
    hidden = query_economic_calendar_corpus(
        build.corpus,
        start_ns=start,
        end_ns=end,
        view=MarketContextView.EX_ANTE,
        as_of_ns=_ns("2023-04-03T13:59:59Z"),
        symbols=("eurusd",),
        include_calendar=False,
    )
    assert hidden.status is MarketContextQueryStatus.MISSING
    assert (
        hidden.missing_reason is MarketContextMissingReason.NOT_AVAILABLE_AS_OF
    )

    visible = query_economic_calendar_corpus(
        build.corpus,
        start_ns=start,
        end_ns=end,
        view=MarketContextView.EX_ANTE,
        as_of_ns=_ns("2023-04-03T14:00:01Z"),
        symbols=("eurusd",),
        include_calendar=False,
    )
    assert visible.status is MarketContextQueryStatus.MATCHED
    assert visible.events[0].expected_value == 47.5
    assert visible.events[0].actual_value == 46.3
    # Trading Economics Revised is the old previous value; Previous is the
    # corrected value.  The shared projection preserves that ordering.
    assert visible.events[0].previous_value == 47.6
    assert visible.events[0].revised_previous_value == 47.7


def test_refresh_accumulates_correction_and_reschedule_vintage() -> None:
    initial = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([_row()], retrieved_at="2023-05-01T00:00:00Z"),),
        profile=_profile(),
    )
    changed = _row(
        Date="2023-04-04T14:00:00",
        Actual="46.1",
        ActualValue=46.1,
        LastUpdate="2023-05-02T12:00:00",
    )
    refreshed = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([changed], retrieved_at="2023-05-03T00:00:00Z"),),
        profile=_profile(),
        previous_corpus=initial.corpus,
        previous_snapshots=initial.snapshots,
    )
    revisions = refreshed.corpus.events
    assert [item.revision_sequence for item in revisions] == [0, 1]
    assert revisions[1].supersedes_event_id == revisions[0].event_id
    assert revisions[1].release_time_ns == _ns("2023-04-04T14:00:00Z")
    assert revisions[1].available_at_ns == _ns("2023-05-03T00:00:00Z")


def test_refresh_artifact_is_self_contained_for_raw_replay(
    tmp_path: Path,
) -> None:
    initial = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([_row()], retrieved_at="2023-05-01T00:00:00Z"),),
        profile=_profile(),
    )
    changed = _row(Actual="46.1", ActualValue=46.1)
    refreshed = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([changed], retrieved_at="2023-05-03T00:00:00Z"),),
        profile=_profile(),
        previous_corpus=initial.corpus,
        previous_snapshots=initial.snapshots,
    )
    artifacts = write_economic_calendar_corpus(refreshed, tmp_path / "refresh")

    replayed = replay_economic_calendar_corpus(artifacts["corpus"].path)

    assert replayed.corpus.corpus_id == refreshed.corpus.corpus_id
    assert len(replayed.snapshots) == 2


def test_corpus_write_read_and_raw_replay(tmp_path: Path) -> None:
    build = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([_row()]),), profile=_profile()
    )
    artifacts = write_economic_calendar_corpus(build, tmp_path)
    corpus_path = Path(artifacts["corpus"].path)
    restored = read_economic_calendar_corpus(corpus_path)
    replayed = replay_economic_calendar_corpus(corpus_path)
    assert restored.corpus_id == build.corpus.corpus_id
    assert replayed.corpus.corpus_id == build.corpus.corpus_id
    source_path = Path(
        next(
            value.path
            for key, value in artifacts.items()
            if key.startswith("source:")
        )
    )
    assert source_path.stat().st_mode & 0o777 == 0o600


def test_common_consumer_seam_accepts_comprehensive_corpus(
    tmp_path: Path,
) -> None:
    build = build_economic_calendar_corpus_from_snapshots(
        (_snapshot([_row()]),), profile=_profile()
    )
    artifacts = write_economic_calendar_corpus(build, tmp_path)
    restored = read_context_corpus(artifacts["corpus"].path)
    assert (
        context_corpus_artifact_kind(restored) == "economic_calendar_corpus_v1"
    )
    assert context_corpus_event_times(restored) == (
        _ns("2023-04-03T14:00:00Z"),
    )
    decision = preflight_context_corpus(
        restored,
        start_ns=_ns("2023-04-03T13:30:00Z"),
        end_ns=_ns("2023-04-03T14:30:00Z"),
        currencies=("USD",),
        symbols=("nsxusd",),
    )
    assert decision.ready
    query = query_context_corpus(
        restored,
        start_ns=decision.start_ns,
        end_ns=decision.end_ns,
        view=MarketContextView.EX_POST,
        symbols=("nsxusd",),
        include_calendar=False,
    )
    assert query.status is MarketContextQueryStatus.MATCHED
    assert query.events[0].expected_value == 47.5


class _FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(content)),
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        del chunk_size
        return [self.content]

    def close(self) -> None:
        return None


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, uri: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"uri": uri, **kwargs})
        return self.response


class _QueueSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, uri: str, **kwargs: Any) -> _FakeResponse:
        del kwargs
        self.calls.append(uri)
        return self.responses.pop(0)


def test_fetch_uses_authorization_header_and_never_credentials_in_uri() -> None:
    content = json.dumps([_row()]).encode("utf-8")
    session = _FakeSession(_FakeResponse(content))
    profile = _profile()
    snapshots = fetch_trading_economics_calendar_snapshots(
        profile,
        api_key="client:super-secret",
        license_acknowledged=True,
        session=session,  # type: ignore[arg-type]
    )
    assert len(snapshots) == 1
    call = session.calls[0]
    assert call["headers"]["Authorization"] == "client:super-secret"
    assert "super-secret" not in call["uri"]
    assert "c=" not in call["uri"]
    assert "super-secret" not in snapshots[0].source_uri


def test_fetch_adaptively_splits_provider_row_ceiling() -> None:
    parent = [_row(CalendarId=str(index)) for index in range(1000)]
    left = [_row(CalendarId="left", Date="2023-04-03T14:00:00")]
    right = [_row(CalendarId="right", Date="2023-04-04T14:00:00")]
    session = _QueueSession(
        [
            _FakeResponse(json.dumps(rows).encode("utf-8"))
            for rows in (parent, left, right)
        ]
    )
    profile = EconomicCalendarFetchProfileV1(
        start_date="2023-04-03",
        end_date="2023-04-04",
        economies=("United States",),
        initial_window_days=2,
        min_request_interval_seconds=0.0,
    )

    snapshots = fetch_trading_economics_calendar_snapshots(
        profile,
        api_key="client:secret",
        license_acknowledged=True,
        session=session,  # type: ignore[arg-type]
    )
    build = build_economic_calendar_corpus_from_snapshots(
        snapshots,
        profile=profile,
    )

    assert len(session.calls) == 3
    assert [item.metadata["complete_leaf"] for item in snapshots] == [
        False,
        True,
        True,
    ]
    assert {item.provider_event_id for item in build.corpus.events} == {
        "left",
        "right",
    }
    assert build.corpus.coverage[0].complete


def test_live_fetch_requires_explicit_license_acknowledgement() -> None:
    with pytest.raises(ValueError, match="license acknowledgement"):
        fetch_trading_economics_calendar_snapshots(
            _profile(), api_key="client:secret", license_acknowledged=False
        )


def test_api_key_environment_name_is_strict() -> None:
    assert (
        validate_api_key_env_name("TRADING_ECONOMICS_API_KEY")
        == "TRADING_ECONOMICS_API_KEY"
    )
    with pytest.raises(ValueError, match="uppercase environment"):
        validate_api_key_env_name("bad-name")


def test_cli_plan_only_does_not_require_credentials(
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = analytics_main(
        [
            "economic-calendar-corpus",
            "--start-date",
            "2000-01-01",
            "--end-date",
            "2000-12-31",
            "--plan-only",
        ]
    )
    assert status == 0
    output = capsys.readouterr().out
    assert "HistData pairs: 66" in output
    assert "economies: 21" in output

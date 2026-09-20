"""Synthetic mathematical/contract cases, not empirical model qualifications."""

from __future__ import annotations

from dataclasses import replace

from histdatacom.forecasting import ForecastScoreV1
from histdatacom.forecasting.contracts import DAY_NS
from tests.fixtures.forecast_contracts_v1 import (
    RELEASE_TIME,
    calendar_fixture,
    snapshot_fixture,
    utc_text,
)


def math_calendar(index: int = 0):
    """Shift a complete fixture, including acquisition and all vintage clocks."""
    corpus = calendar_fixture()
    shift = 40 * index * DAY_NS
    key = f"fixture.cpi.event-{index}"
    releases = []
    for old in corpus.releases:
        released = (
            None if old.released_at_ns is None else old.released_at_ns + shift
        )
        releases.append(
            replace(
                old,
                logical_event_key=key,
                reference_period=f"period-{index}",
                source_release_id=key,
                source_request_id=f"request-{index}",
                reference_period_end_ns=old.reference_period_end_ns + shift,
                scheduled_for_ns=old.scheduled_for_ns + shift,
                scheduled_lexical=utc_text(old.scheduled_for_ns + shift),
                released_at_ns=released,
                released_lexical=(
                    None if released is None else utc_text(released)
                ),
                first_observed_at_ns=old.first_observed_at_ns + shift,
                available_at_ns=old.available_at_ns + shift,
                source=replace(
                    old.source,
                    retrieved_at_ns=old.source.retrieved_at_ns + shift,
                    source_id="",
                ),
                supersedes_release_id=(
                    None if not releases else releases[-1].release_id
                ),
                release_id="",
            )
        )
    forecasts = tuple(
        replace(
            old,
            logical_event_key=key,
            collection_started_at_ns=old.collection_started_at_ns + shift,
            collection_ended_at_ns=old.collection_ended_at_ns + shift,
            produced_at_ns=old.produced_at_ns + shift,
            available_at_ns=old.available_at_ns + shift,
            source=replace(
                old.source,
                retrieved_at_ns=old.source.retrieved_at_ns + shift,
                source_id="",
            ),
            forecast_id="",
        )
        for old in corpus.forecasts
    )
    return replace(
        corpus,
        coverage_start_ns=corpus.coverage_start_ns + shift,
        coverage_end_ns=corpus.coverage_end_ns + shift,
        releases=tuple(releases),
        forecasts=forecasts,
        corpus_id="",
    )


def math_score(
    index: int = 0, *, point: float = 2.5, model: str = "math-fixture"
) -> ForecastScoreV1:
    corpus = math_calendar(index)
    snapshot = snapshot_fixture(corpus, point=point)
    snapshot = replace(snapshot, model=replace(snapshot.model, name=model))
    return ForecastScoreV1(
        snapshot.to_json(),
        corpus.to_json(),
        RELEASE_TIME + (40 * index + 40) * DAY_NS,
    )


def paired_math_scores(
    count: int = 20,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    baseline = tuple(
        math_score(
            i, point=4.0 + (i % 3) / 4, model="baseline-fixture"
        ).to_json()
        for i in range(count)
    )
    candidate = tuple(
        math_score(
            i, point=3.2 + (i % 4) / 10, model="candidate-fixture"
        ).to_json()
        for i in range(count)
    )
    return baseline, candidate

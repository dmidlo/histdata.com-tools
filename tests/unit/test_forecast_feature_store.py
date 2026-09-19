"""Independent numerical, knowledge-time, ragged-edge and replay canaries."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from threading import Barrier

import pytest

from histdatacom.forecasting import (
    FeatureCellStatus,
    FeatureColumnV1,
    FeatureDefinitionV1,
    FeatureKind,
    FeatureMatrixSnapshotV1,
    FeaturePeriodV1,
    FeatureRequestV1,
    FeatureScheduleV1,
    FeatureSourceMode,
    FeatureTransformKind,
    FeatureTransformV1,
    VintageFeatureStoreV1,
    calendar_actual_feature_key,
    calendar_consensus_feature_key,
    calendar_feature_records,
    observed_bar_feature,
)
from histdatacom.forecasting.contracts import DAY_NS, _json
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarCorpusV1,
)
from histdatacom.market_context.release_schedules import (
    EconomicReleaseScheduleEvidenceV1,
    EconomicScheduleExceptionKind,
)
from tests.fixtures.forecast_contracts_v1 import RELEASE_TIME, calendar_fixture
from tests.fixtures.forecast_feature_store_v1 import (
    CUTOFF,
    PERIODS,
    evidence,
    observation,
    observed_bar_fixture,
    revision,
    store_fixture,
)


def request(
    *,
    cutoff: int = CUTOFF,
    transform: FeatureTransformV1 = FeatureTransformV1(),
    max_age_ns: int | None = None,
) -> FeatureRequestV1:
    return FeatureRequestV1(
        cutoff,
        PERIODS,
        (FeatureColumnV1("cpi", "macro.cpi", transform, max_age_ns),),
    )


def test_ragged_edge_never_backfills_future_release() -> None:
    before = store_fixture().snapshot(request(cutoff=PERIODS[1].end_ns))
    assert [cell.value for cell in before.cells] == [1.0, None, None]
    assert [cell.status for cell in before.cells] == [
        FeatureCellStatus.AVAILABLE,
        FeatureCellStatus.UNAVAILABLE,
        FeatureCellStatus.UNAVAILABLE,
    ]
    assert before.cells[0].reference_age_ns == 10 * DAY_NS
    assert before.cells[0].value_age_ns == 9 * DAY_NS
    assert FeatureMatrixSnapshotV1.from_json(before.to_json()) == before


@pytest.mark.parametrize("kind", list(FeatureKind))
def test_future_release_survey_market_alternative_classification_are_invisible(
    kind: FeatureKind,
) -> None:
    known = observation(0, kind=kind)
    future = observation(1, kind=kind, available_at_ns=CUTOFF + 1)
    old = VintageFeatureStoreV1((known,)).snapshot(request())
    new_store = VintageFeatureStoreV1((known, future))
    assert new_store.snapshot(request()).to_json() == old.to_json()
    old.verify_against(new_store)
    assert new_store.snapshot(request(cutoff=CUTOFF + 1)).cells[1].value == 2.0


def test_later_revision_cannot_rewrite_historical_hash_or_news() -> None:
    initial = observation(0)
    revised = revision(initial)
    store = VintageFeatureStoreV1((revised, initial))
    before = store.snapshot(request())
    assert (
        before.to_json()
        == VintageFeatureStoreV1((initial,)).snapshot(request()).to_json()
    )
    assert before.cells[0].revision_news is None
    after = store.snapshot(request(cutoff=revised.available_at_ns))
    assert after.cells[0].value == 99.0
    assert after.cells[0].revision_news == 98.0
    assert after.cells[0].revision_observation_ids == (
        initial.observation_id,
        revised.observation_id,
    )


def test_final_sample_metadata_is_not_backdated() -> None:
    initial = observation(0)
    revised = revision(initial)
    final = replace(
        revised,
        definition=replace(
            initial.definition,
            classification="full-sample-regime",
            known_at_ns=revised.available_at_ns,
        ),
    )
    store = VintageFeatureStoreV1((initial, final))
    old = VintageFeatureStoreV1((initial,)).snapshot(request())
    assert store.snapshot(request()).snapshot_id == old.snapshot_id
    after = store.snapshot(request(cutoff=final.available_at_ns))
    assert after.cells[0].semantic_id != old.cells[0].semantic_id
    assert after.cells[0].revision_news is None
    with pytest.raises(ValueError, match="metadata is unavailable"):
        replace(initial, definition=final.definition)


def test_schedule_changes_use_knowledge_not_scheduled_event_clock() -> None:
    initial = observation(0)
    first = FeatureScheduleV1(
        "macro.cpi",
        PERIODS[0],
        initial.available_at_ns - 5,
        PERIODS[0].start_ns,
        evidence("calendar-first"),
    )
    later = replace(
        first,
        scheduled_at_ns=initial.available_at_ns + 10,
        known_at_ns=CUTOFF + 1,
        evidence=evidence("calendar-later"),
    )
    before = VintageFeatureStoreV1((initial,), (first,)).snapshot(request())
    store = VintageFeatureStoreV1((initial,), (later, first))
    assert store.snapshot(request()) == before
    assert before.cells[0].availability_lag_ns == 5
    after = store.snapshot(request(cutoff=CUTOFF + 1))
    assert after.cells[0].schedule_id == later.to_dict()["id"]
    assert after.cells[0].availability_lag_ns == -10


@pytest.mark.parametrize(
    "kind,window,support,expected",
    [
        (FeatureTransformKind.LEVEL, 1, 1, [1.0, 2.0, 3.0]),
        (FeatureTransformKind.LAG, 1, 1, [None, 1.0, 2.0]),
        (FeatureTransformKind.DIFFERENCE, 1, 1, [None, 1.0, 1.0]),
        (FeatureTransformKind.ROLLING_MEAN, 2, 2, [None, 1.5, 2.5]),
        (FeatureTransformKind.EXPANDING_MEAN, 1, 2, [None, 1.5, 2.0]),
        (FeatureTransformKind.ROLLING_ZSCORE, 3, 2, [None, 2**-0.5, 1.0]),
    ],
)
def test_executed_transforms_have_hand_computed_values_and_exact_lineage(
    kind: FeatureTransformKind,
    window: int,
    support: int,
    expected: list[float | None],
) -> None:
    matrix = store_fixture().snapshot(
        request(transform=FeatureTransformV1(kind, window, support))
    )
    for cell, value in zip(matrix.cells, expected):
        assert (
            cell.value == pytest.approx(value)
            if value is not None
            else cell.value is None
        )
        assert cell.fit_cutoff_at_ns == CUTOFF
        assert all(
            identity in {item.observation_id for item in matrix.observations}
            for identity in cell.observation_ids
        )


def test_transform_cannot_skip_missing_or_cross_semantic_era() -> None:
    left = observation(0)
    right = observation(1)
    changed = replace(
        right, definition=replace(right.definition, base="rebased-2020")
    )
    transform = FeatureTransformV1(FeatureTransformKind.ROLLING_MEAN, 2, 2)
    mixed = VintageFeatureStoreV1((left, changed)).snapshot(
        request(transform=transform)
    )
    assert mixed.cells[1].status is FeatureCellStatus.SEMANTIC_BREAK
    missing = VintageFeatureStoreV1((left, observation(2))).snapshot(
        request(transform=transform)
    )
    assert missing.cells[2].status is FeatureCellStatus.UNAVAILABLE
    constant = VintageFeatureStoreV1(
        tuple(observation(index, value=2.0) for index in range(3))
    ).snapshot(
        request(
            transform=FeatureTransformV1(
                FeatureTransformKind.ROLLING_ZSCORE, 2, 2
            )
        )
    )
    assert constant.cells[1].status is FeatureCellStatus.ZERO_SCALE


def test_lagged_value_and_current_revision_news_have_separate_lineage() -> None:
    left, right = observation(0), observation(1)
    revised = revision(right, available_at_ns=CUTOFF - 1)
    matrix = VintageFeatureStoreV1((left, right, revised)).snapshot(
        request(transform=FeatureTransformV1(FeatureTransformKind.LAG))
    )
    cell = matrix.cells[1]
    assert cell.value == 1.0
    assert cell.observation_ids == (left.observation_id,)
    assert cell.revision_observation_ids == (
        right.observation_id,
        revised.observation_id,
    )
    assert cell.availability_lag_ns is None


def test_source_missing_and_stale_are_distinct_and_not_imputed() -> None:
    missing = replace(observation(0), value=None)
    result = VintageFeatureStoreV1((missing, observation(1))).snapshot(
        request(max_age_ns=0)
    )
    assert result.cells[0].status is FeatureCellStatus.SOURCE_MISSING
    assert result.cells[1].status is FeatureCellStatus.STALE
    assert result.cells[2].status is FeatureCellStatus.UNAVAILABLE
    assert all(cell.value is None for cell in result.cells)


@pytest.mark.parametrize(
    "mode",
    [
        FeatureSourceMode.LATEST_REVISED,
        FeatureSourceMode.EX_POST,
        FeatureSourceMode.SYNTHETIC_RECONSTRUCTION,
    ],
)
def test_current_revised_and_expost_convenience_sources_refused(
    mode: FeatureSourceMode,
) -> None:
    original = observation(0)
    with pytest.raises(ValueError, match="refuses"):
        VintageFeatureStoreV1(
            (replace(original, evidence=replace(original.evidence, mode=mode)),)
        )


def test_mutated_missing_duplicate_and_backward_vintage_chains_refuse() -> None:
    initial = observation(0)
    revised = revision(initial)
    for records in (
        (revised,),
        (initial, initial),
        (initial, replace(revised, supersedes_id="wrong")),
        (
            initial,
            replace(
                revised,
                available_at_ns=initial.available_at_ns - 1,
                published_at_ns=initial.published_at_ns,
            ),
        ),
    ):
        with pytest.raises(ValueError):
            VintageFeatureStoreV1(records)
    with pytest.raises(ValueError, match="missing or changed"):
        VintageFeatureStoreV1((initial,)).snapshot(request()).verify_against(
            VintageFeatureStoreV1((replace(initial, value=7),))
        )


def test_derived_cell_and_nested_payload_tampering_refuse() -> None:
    matrix = store_fixture().snapshot(request())
    for part in ("cell", "observation", "request"):
        data = json.loads(matrix.to_json())
        if part == "cell":
            data["cells"][0]["value"] = 999
        elif part == "observation":
            data["observations"][0]["evidence"]["record"]["record"] = "mutated"
        else:
            data["request"]["cutoff_at_ns"] += 1
        with pytest.raises(ValueError):
            FeatureMatrixSnapshotV1.from_json(json.dumps(data))
    with pytest.raises(FrozenInstanceError):
        matrix.request.cutoff_at_ns += 1


def test_cache_is_byte_and_entry_bounded_and_thread_safe() -> None:
    records = tuple(observation(index) for index in range(3))
    example = VintageFeatureStoreV1(records).snapshot(request())
    byte_bound = len(example.to_json().encode()) * 2 + 100
    store = VintageFeatureStoreV1(
        records, cache_entries=2, cache_bytes=byte_bound
    )
    barrier = Barrier(8)

    def run(index: int) -> str:
        barrier.wait()
        return store.snapshot(request(cutoff=CUTOFF + index % 3)).snapshot_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(run, range(8)))
    assert ids[0] == ids[3] == ids[6]
    assert store.cache_usage[0] <= 2
    assert store.cache_usage[1] <= byte_bound
    # Accounting equals retained bytes, including concurrent same-key misses.
    assert store.cache_usage == (
        len(store._cache),
        sum(len(item.encode()) for item in store._cache.values()),
    )
    no_cache = VintageFeatureStoreV1(records, cache_entries=0)
    assert no_cache.snapshot(request()) == example
    assert no_cache.cache_usage == (0, 0)


def test_calendar_actual_and_survey_adapters_preserve_source_records() -> None:
    corpus = calendar_fixture()
    release = corpus.releases[0]
    period = FeaturePeriodV1(
        release.reference_period,
        release.reference_period_end_ns - 30 * DAY_NS,
        release.reference_period_end_ns,
    )
    observations, schedules = calendar_feature_records(
        corpus, {(release.series_key, release.reference_period): period}
    )
    assert len(observations) == 4
    assert len(schedules) == 3
    keys = (
        calendar_actual_feature_key(release),
        calendar_consensus_feature_key(release, corpus.forecasts[0]),
    )
    query = FeatureRequestV1(
        CUTOFF,
        (period,),
        tuple(
            FeatureColumnV1(str(index), key) for index, key in enumerate(keys)
        ),
    )
    matrix = VintageFeatureStoreV1(observations, schedules).snapshot(query)
    assert [cell.value for cell in matrix.cells] == [None, 2.0]
    survey = matrix.observations[0]
    record = json.loads(survey.evidence.record_json)
    assert record["metadata_release"]["release_id"] == release.release_id
    assert record["forecast"]["forecast_id"] == corpus.forecasts[0].forecast_id


def test_early_survey_cannot_borrow_metadata_from_future_release() -> None:
    corpus = calendar_fixture()
    release = replace(
        corpus.releases[0],
        available_at_ns=RELEASE_TIME - 5 * DAY_NS,
        first_observed_at_ns=RELEASE_TIME - 5 * DAY_NS,
        release_id="",
    )
    delayed = EconomicCalendarCorpusV1(
        corpus.coverage_start_ns,
        corpus.coverage_end_ns,
        False,
        (release,),
        (corpus.forecasts[0],),
        corpus.limitations,
    )
    period = FeaturePeriodV1(
        release.reference_period,
        release.reference_period_end_ns - DAY_NS,
        release.reference_period_end_ns,
    )
    with pytest.raises(
        ValueError, match="metadata has no release vintage visible"
    ):
        calendar_feature_records(
            delayed, {(release.series_key, release.reference_period): period}
        )


def test_complete_observed_bar_requires_exact_receipt_and_availability() -> (
    None
):
    bar = observed_bar_fixture()
    definition = FeatureDefinitionV1(
        "market.eurusd.mid",
        FeatureKind.MARKET,
        "USD/EUR",
        1.0,
        "unadjusted",
        "observed-mid-close-v1",
        "1m",
        bar.bar_start_ns,
    )
    receipt = evidence(
        bar.bar_id,
        mode=FeatureSourceMode.OBSERVED,
        record_json=_json(bar.to_dict()),
    )
    record = observed_bar_feature(
        bar,
        definition=definition,
        available_at_ns=bar.bar_end_ns + 5,
        evidence=receipt,
    )
    assert record.value == 1.15
    assert record.available_at_ns == bar.bar_end_ns + 5
    with pytest.raises(ValueError, match="has not ended"):
        observed_bar_feature(
            bar,
            definition=definition,
            available_at_ns=bar.bar_end_ns - 1,
            evidence=receipt,
        )
    with pytest.raises(ValueError, match="exact observed bar"):
        observed_bar_feature(
            bar,
            definition=definition,
            available_at_ns=bar.bar_end_ns,
            evidence=replace(receipt, record_json='{"changed":true}'),
        )
    partial = replace(bar, is_partial_end=True, bar_id="")
    with pytest.raises(ValueError, match="complete observed-only"):
        observed_bar_feature(
            partial,
            definition=definition,
            available_at_ns=bar.bar_end_ns,
            evidence=receipt,
        )


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), 10**1000])
def test_invalid_numerical_values_fail_without_overflow(value: object) -> None:
    with pytest.raises((ValueError, TypeError)):
        replace(observation(0), value=value)


def test_multiple_logical_events_are_not_stitched_into_one_revision_chain() -> (
    None
):
    corpus = calendar_fixture()
    schedule, actual = corpus.releases[:2]
    second_schedule = replace(
        schedule, logical_event_key="another-event", release_id=""
    )
    second_actual = replace(
        actual,
        logical_event_key="another-event",
        supersedes_release_id=second_schedule.release_id,
        release_id="",
    )
    combined = EconomicCalendarCorpusV1(
        corpus.coverage_start_ns,
        corpus.coverage_end_ns,
        False,
        (schedule, actual, second_schedule, second_actual),
        (),
        corpus.limitations,
    )
    period = FeaturePeriodV1(
        schedule.reference_period,
        schedule.reference_period_end_ns - DAY_NS,
        schedule.reference_period_end_ns,
    )
    with pytest.raises(ValueError, match="multiple events/stages"):
        calendar_feature_records(
            combined, {(schedule.series_key, schedule.reference_period): period}
        )


def test_external_schedule_evidence_keeps_its_independent_known_at_clock() -> (
    None
):
    corpus = calendar_fixture()
    release = corpus.releases[0]
    period = FeaturePeriodV1(
        release.reference_period,
        release.reference_period_end_ns - DAY_NS,
        release.reference_period_end_ns,
    )
    external = EconomicReleaseScheduleEvidenceV1(
        release.release_id,
        "fixture-source",
        CUTOFF + DAY_NS,
        "fixture-tz",
        "fixture-holidays",
        None,
        EconomicScheduleExceptionKind.ROUTINE,
        None,
        "a" * 64,
    )
    observations, schedules = calendar_feature_records(
        corpus,
        {(release.series_key, release.reference_period): period},
        schedule_evidence=(external,),
    )
    query = FeatureRequestV1(
        CUTOFF,
        (period,),
        (FeatureColumnV1("cpi", calendar_actual_feature_key(release)),),
    )
    store = VintageFeatureStoreV1(observations, schedules)
    assert store.snapshot(query).cells[0].schedule_id is None
    later = store.snapshot(replace(query, cutoff_at_ns=CUTOFF + DAY_NS))
    assert later.cells[0].schedule_id == schedules[0].to_dict()["id"]


def test_lineage_amplification_is_bounded_before_large_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import histdatacom.forecasting.feature_store as implementation

    monkeypatch.setattr(implementation, "MAX_FEATURE_LINEAGE_REFERENCES", 4)
    with pytest.raises(ValueError, match="lineage exceeds"):
        store_fixture().snapshot(
            request(
                transform=FeatureTransformV1(
                    FeatureTransformKind.EXPANDING_MEAN
                )
            )
        )


def test_legal_extreme_constant_mean_is_finite() -> None:
    store = VintageFeatureStoreV1(
        tuple(observation(index, value=1e308) for index in range(3))
    )
    result = store.snapshot(
        request(
            transform=FeatureTransformV1(FeatureTransformKind.EXPANDING_MEAN)
        )
    )
    assert [item.value for item in result.cells] == [1e308, 1e308, 1e308]


def test_direct_cell_age_refuses_boolean_zero() -> None:
    initial = observation(0)
    cell = (
        VintageFeatureStoreV1((initial,))
        .snapshot(request(cutoff=initial.available_at_ns))
        .cells[0]
    )
    assert cell.value_age_ns == 0
    with pytest.raises(ValueError, match="availability/age mismatch"):
        replace(cell, value_age_ns=False)
    with pytest.raises(ValueError, match="semantic and source lineage"):
        replace(cell, available_at_ns=None, value_age_ns=None)

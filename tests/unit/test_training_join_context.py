"""Native context artifacts and real refits over synthetic retained evidence."""

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from histdatacom.data_quality.training_contracts import (
    training_json,
    training_load,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    JoinState,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
    replay_training_joins,
)
from histdatacom.forecasting.feature_artifacts import write_feature_artifact
from histdatacom.market_context.economic_calendar import (
    write_economic_calendar_corpus,
)
from tests.fixtures.forecast_contracts_v1 import calendar_fixture, RELEASE_TIME
from tests.fixtures.forecast_feature_store_v1 import feature_forecast_fixture
from tests.fixtures.training_join_v1 import bound_column, controlled_spine
from tests.fixtures.training_temporal_v1 import SECOND


def _plan(spine, binding, *columns, mode=JoinInformationMode.NORMALIZED_AS_OF):
    return TrainingJoinPlanV1(spine, mode, (binding,), tuple(columns))


def test_real_calendar_asof_delayed_release_and_event_no_fill(tmp_path):
    corpus = calendar_fixture(availability_delay_ns=SECOND)
    artifact = write_economic_calendar_corpus(corpus, tmp_path / "calendar")
    binding = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(artifact.path),)
    )
    spine = controlled_spine(
        tmp_path / "spine",
        (RELEASE_TIME, RELEASE_TIME + SECOND, RELEASE_TIME + 2 * SECOND),
    )
    entity = TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom")
    actual = bound_column(
        binding,
        "calendar.cpi.actual",
        entity,
        "actual_value",
        coordinate="period-0",
        direction=JoinDirection.BOUNDED_PRIOR,
        max_age_ns=10 * SECOND,
    )
    occurrence = bound_column(
        binding,
        "calendar.cpi.occurrence",
        entity,
        "occurrence",
        coordinate="period-0",
        meaning=JoinMeaning.EVENT,
    )
    result = materialize_training_joins(
        _plan(spine, binding, actual, occurrence)
    )
    assert result.rows[0].values[0].state is JoinState.UNAVAILABLE
    assert (
        result.rows[0].values[0].refusal_evidence[0].available_at_ns
        == RELEASE_TIME + SECOND
    )
    assert training_load(result.rows[1].values[0].value_json)["value"] == 3.0
    assert all(r.values[1].value_json == '{"value":null}' for r in result.rows)
    assert replay_training_joins(result) == result


def test_future_known_null_cannot_erase_an_older_available_calendar_value(
    tmp_path,
):
    from histdatacom.market_context.economic_calendar import (
        EconomicReleaseStatus,
    )

    corpus = calendar_fixture()
    initial = next(r for r in corpus.releases if r.actual_value == 3.0)
    future_null = replace(
        initial,
        logical_event_key="later-event",
        reference_period="later-period",
        source_release_id="later-source",
        status=EconomicReleaseStatus.CANCELLED,
        schedule_change_reason="controlled future-known cancellation",
        released_at_ns=RELEASE_TIME + SECOND,
        released_lexical=datetime.fromtimestamp(
            (RELEASE_TIME + SECOND) // SECOND, timezone.utc
        ).isoformat(),
        available_at_ns=RELEASE_TIME + 10 * SECOND,
        first_observed_at_ns=RELEASE_TIME + 10 * SECOND,
        actual_value=None,
        actual_lexical=None,
        revision_sequence=0,
        supersedes_release_id=None,
        release_id="",
    )
    corpus = replace(
        corpus, releases=corpus.releases + (future_null,), corpus_id=""
    )
    path = write_economic_calendar_corpus(corpus, tmp_path / "calendar").path
    binding = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(path),)
    )
    spine = controlled_spine(tmp_path / "spine", (RELEASE_TIME + 2 * SECOND,))
    column = bound_column(
        binding,
        "calendar.cpi.actual",
        TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom"),
        "actual_value",
        direction=JoinDirection.BOUNDED_PRIOR,
        max_age_ns=10 * SECOND,
    )
    value = (
        materialize_training_joins(_plan(spine, binding, column))
        .rows[0]
        .values[0]
    )
    assert value.state is JoinState.AVAILABLE
    assert training_load(value.value_json)["value"] == 3.0
    assert initial.release_id in value.source_ids


def test_calendar_duplicate_native_tie_refuses_and_wrong_entity_is_null(
    tmp_path,
):
    corpus = calendar_fixture()
    release = next(r for r in corpus.releases if r.actual_value == 3.0)
    duplicate = replace(
        release,
        reference_period="another-period",
        logical_event_key="another-event",
        source_release_id="another-event",
        revision_sequence=0,
        supersedes_release_id=None,
        release_id="",
    )
    corpus = replace(
        corpus, releases=corpus.releases + (duplicate,), corpus_id=""
    )
    artifact = write_economic_calendar_corpus(corpus, tmp_path / "calendar")
    binding = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(artifact.path),)
    )
    spine = controlled_spine(tmp_path / "spine", (RELEASE_TIME,))
    column = bound_column(
        binding,
        "calendar.cpi.actual",
        TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom"),
        "actual_value",
    )
    with pytest.raises(ValueError, match="ambiguous duplicate"):
        materialize_training_joins(_plan(spine, binding, column))
    wrong = replace(column, entity=replace(column.entity, economy="GB"))
    assert (
        materialize_training_joins(_plan(spine, binding, wrong))
        .rows[0]
        .values[0]
        .state
        is JoinState.UNAVAILABLE
    )


def test_native_executable_forecast_replays_actual_model_and_inputs(tmp_path):
    forecast = feature_forecast_fixture()
    artifact = write_feature_artifact(forecast, tmp_path / "forecast")
    spine = controlled_spine(
        tmp_path / "spine", (forecast.generated_at_ns,), derived=(artifact,)
    )
    binding = TrainingJoinSourceV1(
        "forecast", JoinFamily.FORECAST, paths=(str(artifact),)
    )
    column = bound_column(
        binding,
        "forecast.cpi.point",
        TrainingJoinEntityV1("EURUSD", "USD", "US", forecast.target.series_id),
        "point",
        coordinate=forecast.target.reference_period,
    )
    result = materialize_training_joins(_plan(spine, binding, column))
    value = result.rows[0].values[0]
    assert (
        training_load(value.value_json)["value"] == forecast.distribution.point
    )
    assert forecast.model.model_id in value.parent_source_ids
    assert value.dependency_start_ns <= min(
        p.start_ns
        for p in forecast.model.training_inputs.features.request.periods
    )
    assert value.dependency_end_ns <= forecast.generated_at_ns + 1
    assert value.origin == "machine_forecast_not_actual_or_revision"
    assert replay_training_joins(result) == result


def test_registered_engine_forecast_executes_full_nonbaseline_receipt(tmp_path):
    from histdatacom.forecasting.engine_runner import (
        ForecastEngineRunnerV1,
        HISTORICAL_MEDIAN,
        default_forecast_registry,
    )

    seed = feature_forecast_fixture()
    runner = ForecastEngineRunnerV1(default_forecast_registry())
    model = runner.fit(
        HISTORICAL_MEDIAN,
        seed.model.training_inputs,
        column="cpi",
        horizon=seed.cutoff.horizon,
        target_kind=seed.target.kind,
        trained_at_ns=seed.model.trained_at_ns,
    )
    executed = runner.generate(
        model,
        seed.inputs,
        cutoff=seed.cutoff,
        target=seed.target,
        generated_at_ns=seed.generated_at_ns,
    )
    forecast = executed.forecast
    path = write_feature_artifact(forecast, tmp_path / "forecast")
    spine = controlled_spine(
        tmp_path / "spine", (forecast.generated_at_ns,), derived=(path,)
    )
    binding = TrainingJoinSourceV1(
        "forecast",
        JoinFamily.FORECAST,
        paths=(str(path),),
        evidence_json=(training_json(executed.to_dict()),),
    )
    column = bound_column(
        binding,
        "forecast.cpi.median",
        TrainingJoinEntityV1("EURUSD", "USD", "US", forecast.target.series_id),
        "point",
        coordinate=forecast.target.reference_period,
    )
    result = materialize_training_joins(_plan(spine, binding, column))
    assert (
        training_load(result.rows[0].values[0].value_json)["value"]
        == forecast.distribution.point
    )
    assert executed.snapshot_id in result.rows[0].values[0].parent_source_ids
    # A correctly re-sealed feature artifact with changed model output is not
    # authorized by a real engine receipt retaining the original output.
    altered = replace(
        forecast,
        distribution=replace(
            forecast.distribution,
            support=((forecast.distribution.point + 123, 1.0),),
        ),
    )
    altered_path = write_feature_artifact(altered, tmp_path / "altered")
    altered_spine = controlled_spine(
        tmp_path / "altered-spine",
        (forecast.generated_at_ns,),
        derived=(altered_path,),
    )
    with pytest.raises(ValueError, match="differs"):
        materialize_training_joins(
            _plan(
                altered_spine,
                replace(binding, paths=(str(altered_path),)),
                column,
            )
        )


def test_vintage_exact_inventory_cutoff_and_postverification_replacement(
    tmp_path, monkeypatch
):
    from pathlib import Path
    from histdatacom.data_quality import training_join_context as context
    from histdatacom.forecasting.feature_store import VintageFeatureStoreV1
    from tests.fixtures.training_temporal_v1 import vintage_matrix

    matrix = vintage_matrix()
    path = write_feature_artifact(matrix, tmp_path / "matrix")
    spine = controlled_spine(
        tmp_path / "spine", (matrix.request.cutoff_at_ns,), derived=(path,)
    )
    binding = TrainingJoinSourceV1(
        "macro", JoinFamily.VINTAGE, paths=(str(path),)
    )
    column = bound_column(
        binding,
        "calendar.macro.level",
        TrainingJoinEntityV1("EURUSD", series="macro"),
        "macro",
        coordinate="1",
        meaning=JoinMeaning.SNAPSHOT,
    )
    plan = _plan(spine, binding, column)
    expected = materialize_training_joins(plan)
    assert training_load(expected.rows[0].values[0].value_json)["value"] == 9.0
    assert len(expected.rows[0].values[0].parent_source_ids) == 1
    other = VintageFeatureStoreV1(
        tuple(o for o in matrix.observations if o.vintage_sequence == 0),
        matrix.schedules,
    ).snapshot(matrix.request)
    original = context.prepare_context_join
    old = Path(path).read_bytes()

    def raced(source, verified):
        Path(path).write_bytes(other.to_json().encode())
        return original(source, verified)

    monkeypatch.setattr(context, "prepare_context_join", raced)
    try:
        assert materialize_training_joins(plan) == expected
    finally:
        Path(path).write_bytes(old)


def test_unknown_empty_feature_grid_has_no_fabricated_macro_origin(tmp_path):
    from histdatacom.forecasting.feature_store import VintageFeatureStoreV1
    from tests.fixtures.training_temporal_v1 import vintage_matrix

    request = vintage_matrix().request
    matrix = VintageFeatureStoreV1(()).snapshot(request)
    path = write_feature_artifact(matrix, tmp_path / "matrix")
    spine = controlled_spine(
        tmp_path / "spine", (request.cutoff_at_ns,), derived=(path,)
    )
    binding = TrainingJoinSourceV1(
        "macro", JoinFamily.VINTAGE, paths=(str(path),)
    )
    column = bound_column(
        binding,
        "calendar.macro.level",
        TrainingJoinEntityV1("EURUSD", series="macro"),
        "macro",
        coordinate="1",
        meaning=JoinMeaning.SNAPSHOT,
    )
    value = (
        materialize_training_joins(_plan(spine, binding, column))
        .rows[0]
        .values[0]
    )
    assert value.state is JoinState.UNAVAILABLE
    assert value.origin == "explicit_null_not_an_observation"


@pytest.fixture(scope="module")
def cftc_source(tmp_path_factory):
    from histdatacom.market_context.positioning import (
        build_cftc_positioning_corpus_from_sources,
        write_cftc_positioning_corpus,
    )
    from tests.unit.test_cftc_positioning import _profile, _sources

    root = tmp_path_factory.mktemp("join-cftc-synthetic")
    build = build_cftc_positioning_corpus_from_sources(
        _sources(), profile=_profile()
    )
    paths = write_cftc_positioning_corpus(build, root)
    return build, root, paths["corpus"].path


def test_current_cftc_is_expost_only_not_historical_original(
    tmp_path, cftc_source
):
    build, root, path = cftc_source
    cutoff = int(datetime(2015, 7, 1, tzinfo=timezone.utc).timestamp()) * SECOND
    binding = TrainingJoinSourceV1(
        "cftc", JoinFamily.POSITIONING, paths=(path, str(root / "sources"))
    )
    spine = controlled_spine(tmp_path / "spine", (cutoff,))
    column = bound_column(
        binding,
        "positioning.EURUSD.open_interest",
        TrainingJoinEntityV1("EURUSD", series="099741"),
        "open_interest_all",
        coordinate="legacy:futures_only:099741",
        direction=JoinDirection.PRIOR,
        max_age_ns=30 * 86400 * SECOND,
    )
    plan = _plan(spine, binding, column)
    asof = materialize_training_joins(plan)
    assert asof.rows[0].values[0].state is JoinState.UNKNOWN_AVAILABILITY
    post = materialize_training_joins(
        replace(plan, information_mode=JoinInformationMode.EX_POST)
    )
    value = post.rows[0].values[0]
    assert value.state is JoinState.AVAILABLE
    assert value.available_at_ns is None
    assert value.origin == "official_futures_positioning_not_spot_volume"
    assert build.corpus.corpus_id in value.parent_source_ids


@pytest.mark.parametrize(
    "delta,expected", [(-1, "2014-06-09"), (0, "2014-06-10"), (1, "2014-06-10")]
)
def test_cftc_mapping_utc_day_does_not_round_nanoseconds(
    tmp_path, cftc_source, monkeypatch, delta, expected
):
    from histdatacom.data_quality import training_join_context as context

    _, root, path = cftc_source
    midnight = (
        int(datetime(2014, 6, 10, tzinfo=timezone.utc).timestamp()) * SECOND
    )
    seen = []
    original = context._select_symbol_mapping

    def mapping(corpus, symbol, day):
        seen.append(day.isoformat())
        return original(corpus, symbol, day)

    monkeypatch.setattr(context, "_select_symbol_mapping", mapping)
    spine = controlled_spine(
        tmp_path / "spine", (midnight - SECOND,), decision=midnight + delta
    )
    binding = TrainingJoinSourceV1(
        "cftc", JoinFamily.POSITIONING, paths=(path, str(root / "sources"))
    )
    column = bound_column(
        binding,
        "positioning.EURUSD.open_interest",
        TrainingJoinEntityV1("EURUSD", series="099741"),
        "open_interest_all",
        coordinate="legacy:futures_only:099741",
        direction=JoinDirection.PRIOR,
        max_age_ns=30 * 86400 * SECOND,
    )
    materialize_training_joins(_plan(spine, binding, column))
    assert seen and set(seen) == {expected}


def test_cftc_raw_source_tamper_refuses_even_without_selected_rows(
    tmp_path, cftc_source
):
    from pathlib import Path
    import shutil

    _, root, path = cftc_source
    copied = tmp_path / "raw"
    shutil.copytree(root / "sources", copied)
    raw = next(p for p in copied.iterdir() if p.is_file())
    raw.write_bytes(raw.read_bytes() + b"tampered")
    cutoff = int(datetime(2015, 7, 1, tzinfo=timezone.utc).timestamp()) * SECOND
    spine = controlled_spine(tmp_path / "spine", (cutoff,))
    from histdatacom.data_quality.training_views import (
        materialize_training_rows,
    )

    empty = materialize_training_rows(
        spine.source,
        spine.ownership,
        replace(spine.request, start_ns=cutoff + 1, end_ns=cutoff + 2),
    )
    binding = TrainingJoinSourceV1(
        "cftc", JoinFamily.POSITIONING, paths=(str(Path(path)), str(copied))
    )
    with pytest.raises(ValueError):
        materialize_training_joins(_plan(empty, binding))


def test_broker_refits_native_sessions_and_never_backdates_fitting(tmp_path):
    from histdatacom.broker_capture import (
        BrokerDeliveryFitConfigV1,
        fit_broker_delivery_fingerprint,
    )
    from histdatacom.broker_capture.storage import (
        discover_broker_capture_session_manifests,
    )
    from tests.unit.test_broker_delivery_fingerprints import (
        _capture,
        BASE_WALL_NS,
    )

    root = tmp_path / "capture"
    manifest = _capture(root, seed=610, wall_start_ns=BASE_WALL_NS)
    from histdatacom.broker_plugin_policy import provider_native_inputs
    from tests.fixtures.broker_provider_policy import (
        generated_legacy_request,
        generated_provider_scope,
    )

    provider_request = generated_legacy_request(manifest.session)
    with (
        generated_provider_scope(provider_request),
        provider_native_inputs(provider_request),
    ):
        profile = fit_broker_delivery_fingerprint(
            root,
            (manifest,),
            config=BrokerDeliveryFitConfigV1(min_cell_support=4),
        )
        paths = discover_broker_capture_session_manifests(root)
        binding = TrainingJoinSourceV1(
            "broker",
            JoinFamily.BROKER,
            paths=(
                str(root),
                *(
                    str(root / p.session.session_id / "session.manifest.json")
                    for p in paths
                ),
            ),
            evidence_json=(training_json(profile.to_dict()),),
        )
        cutoff = profile.support_end_utc_ns + SECOND
        spine = controlled_spine(
            tmp_path / "spine", (cutoff // 1_000_000 * 1_000_000,)
        )
        column = bound_column(
            binding,
            "broker_style.EURUSD.spread",
            TrainingJoinEntityV1("EURUSD"),
            "spread",
            coordinate="global",
            direction=JoinDirection.PRIOR,
            max_age_ns=10 * SECOND,
        )
        plan = _plan(spine, binding, column)
        normal = materialize_training_joins(plan)
        assert normal.rows[0].values[0].state is JoinState.UNKNOWN_AVAILABILITY
        refused = normal.rows[0].values[0].refusal_evidence[0]
        assert refused.dependency_end_ns == profile.support_end_utc_ns + 1
        post = materialize_training_joins(
            replace(plan, information_mode=JoinInformationMode.EX_POST)
        )
        assert training_load(post.rows[0].values[0].value_json)[
            "value"
        ] == pytest.approx(0.0002)
        assert manifest.manifest_id in post.rows[0].values[0].parent_source_ids
        early = controlled_spine(
            tmp_path / "early",
            (profile.support_start_utc_ns // 1_000_000 * 1_000_000,),
        )
        early_value = (
            materialize_training_joins(
                _plan(early, binding, column, mode=JoinInformationMode.EX_POST)
            )
            .rows[0]
            .values[0]
        )
        assert early_value.state is JoinState.UNAVAILABLE
        assert (
            early_value.refusal_evidence[0].dependency_end_ns
            == profile.support_end_utc_ns + 1
        )

        end = cutoff + SECOND
        finite_profile = fit_broker_delivery_fingerprint(
            root,
            (manifest,),
            config=profile.fit_config,
            effective_start_utc_ns=profile.effective_start_utc_ns,
            effective_end_utc_ns=end,
        )
        finite_binding = replace(
            binding, evidence_json=(training_json(finite_profile.to_dict()),)
        )
        from histdatacom.data_quality.training_views import (
            materialize_training_rows,
        )

        for direction in (JoinDirection.PRIOR, JoinDirection.INTERVAL):
            interval_column = replace(column, direction=direction)
            for delta, expected in (
                (-1, JoinState.AVAILABLE),
                (0, JoinState.STALE),
                (1, JoinState.STALE),
            ):
                at_end = materialize_training_rows(
                    spine.source,
                    spine.ownership,
                    replace(spine.request, decision_time_ns=end + delta),
                )
                value = (
                    materialize_training_joins(
                        _plan(
                            at_end,
                            finite_binding,
                            interval_column,
                            mode=JoinInformationMode.EX_POST,
                        )
                    )
                    .rows[0]
                    .values[0]
                )
                assert value.state is expected
                if delta >= 0:
                    assert value.refusal_evidence[0].source_ids

        from pathlib import Path

        partition = root / Path(manifest.partitions[0].data_artifact.path)
        original = partition.read_bytes()
        partition.write_bytes(original + b"tampered")
        from histdatacom.broker_capture.fingerprints import (
            BrokerDeliveryIneligibleCaptureError,
        )

        with pytest.raises(
            BrokerDeliveryIneligibleCaptureError,
            match="integrity_verification_failed",
        ):
            materialize_training_joins(plan)

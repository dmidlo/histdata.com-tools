"""Actual retained synthetic context adapters and current-rights boundaries."""

from dataclasses import replace
from datetime import datetime, timezone
from itertools import count

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerPolicyDataClass as DataClass,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    provider_native_inputs,
    provider_policy_scope,
    resolve_provider_subject,
    scope,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyOperation as Operation,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyStatus as Status,
)
from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinState,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_wide_artifacts import (
    read_training_wide_artifact,
    write_training_wide_artifact,
)
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    diagnose_training_wide_view,
    materialize_training_wide_view,
    project_training_wide_view,
)
from histdatacom.forecasting.feature_artifacts import write_feature_artifact
from tests.fixtures.broker_provider_policy import (
    POLICY_NOW,
    MutablePolicySource,
    generated_legacy_request,
    generated_provider_scope,
    policy_context,
)
from tests.fixtures.forecast_feature_store_v1 import feature_forecast_fixture
from tests.fixtures.training_join_v1 import bound_column, controlled_spine
from tests.fixtures.training_temporal_v1 import SECOND


def _wide(plan):
    return materialize_training_wide_view(
        build_training_wide_plan(
            (plan,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )
    )


def test_actual_forecast_context_keeps_target_units_and_machine_origin(
    tmp_path,
):
    forecast = feature_forecast_fixture()
    artifact = write_feature_artifact(forecast, tmp_path / "forecast")
    spine = controlled_spine(
        tmp_path / "spine", (forecast.generated_at_ns,), derived=(artifact,)
    )
    source = TrainingJoinSourceV1(
        "forecast", JoinFamily.FORECAST, paths=(str(artifact),)
    )
    column = bound_column(
        source,
        "forecast.cpi.point",
        TrainingJoinEntityV1("EURUSD", "USD", "US", forecast.target.series_id),
        "point",
        coordinate=forecast.target.reference_period,
    )
    view = _wide(
        TrainingJoinPlanV1(
            spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
        )
    )
    assert view.manifest.columns[0].units == forecast.target.unit
    record = project_training_wide_view(view)[0]
    assert record["values"][column.name] == forecast.distribution.point
    assert (
        record["cell_provenance"][column.name]["origin"]
        == "machine_forecast_not_actual_or_revision"
    )


def test_actual_cftc_replay_retains_expost_refusal_and_units(tmp_path):
    from histdatacom.market_context.positioning import (
        build_cftc_positioning_corpus_from_sources,
        write_cftc_positioning_corpus,
    )
    from tests.unit.test_cftc_positioning import _profile, _sources

    root = tmp_path / "cftc"
    build = build_cftc_positioning_corpus_from_sources(
        _sources(), profile=_profile()
    )
    path = write_cftc_positioning_corpus(build, root)["corpus"].path
    cutoff = int(datetime(2015, 7, 1, tzinfo=timezone.utc).timestamp()) * SECOND
    spine = controlled_spine(tmp_path / "spine", (cutoff,))
    source = TrainingJoinSourceV1(
        "cftc", JoinFamily.POSITIONING, paths=(path, str(root / "sources"))
    )
    column = bound_column(
        source,
        "positioning.EURUSD.open_interest",
        TrainingJoinEntityV1("EURUSD", series="099741"),
        "open_interest_all",
        coordinate="legacy:futures_only:099741",
        direction=JoinDirection.PRIOR,
        max_age_ns=30 * 86400 * SECOND,
    )
    plan = TrainingJoinPlanV1(
        spine, JoinInformationMode.NORMALIZED_AS_OF, (source,), (column,)
    )
    asof = _wide(plan)
    assert (
        project_training_wide_view(asof)[0]["states"][column.name]
        == JoinState.UNKNOWN_AVAILABILITY.value
    )
    post = _wide(replace(plan, information_mode=JoinInformationMode.EX_POST))
    result = project_training_wide_view(post)[0]
    assert result["states"][column.name] == JoinState.AVAILABLE.value
    assert (
        post.manifest.columns[0].units
        == "reported_futures_contracts_not_spot_volume"
    )
    assert (
        result["cell_provenance"][column.name]["origin"]
        == "official_futures_positioning_not_spot_volume"
    )


@pytest.fixture(scope="module")
def broker_view(tmp_path_factory):
    from histdatacom.broker_capture import (
        BrokerDeliveryFitConfigV1,
        fit_broker_delivery_fingerprint,
    )
    from tests.unit.test_broker_delivery_fingerprints import (
        BASE_WALL_NS,
        _capture,
    )

    root = tmp_path_factory.mktemp("wide-broker-synthetic")
    capture = root / "capture"
    manifest = _capture(capture, seed=652, wall_start_ns=BASE_WALL_NS)
    request = generated_legacy_request(manifest.session)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(scope, "_now_ns", count(POLICY_NOW).__next__)
        with (
            generated_provider_scope(request, capture_roots=(capture,)),
            provider_native_inputs(request),
        ):
            fingerprint = fit_broker_delivery_fingerprint(
                capture,
                (manifest,),
                config=BrokerDeliveryFitConfigV1(min_cell_support=4),
            )
            binding = TrainingJoinSourceV1(
                "broker",
                JoinFamily.BROKER,
                paths=(
                    str(capture),
                    str(
                        capture
                        / manifest.session.session_id
                        / "session.manifest.json"
                    ),
                ),
                evidence_json=(training_json(fingerprint.to_dict()),),
            )
            cutoff = fingerprint.support_end_utc_ns + SECOND
            spine = controlled_spine(
                root / "spine", (cutoff // 1_000_000 * 1_000_000,)
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
            # A non-broker projected column still cannot discard broker rights.
            absent = TrainingJoinSourceV1("missing", JoinFamily.UNSUPPORTED)
            null = bound_column(
                absent,
                "uncertainty.EURUSD.missing",
                TrainingJoinEntityV1("EURUSD"),
                "not_implemented",
            )
            plans = tuple(
                TrainingJoinPlanV1(
                    spine, JoinInformationMode.EX_POST, (binding, absent), (c,)
                )
                for c in (column, null)
            )
            view = materialize_training_wide_view(
                build_training_wide_plan(
                    plans, grain=TrainingWideGrainV1(WideRowGrain.EVENT)
                )
            )
        from histdatacom.broker_capture import broker_fingerprint_sources

        with broker_fingerprint_sources(capture):
            yield fingerprint, view, request


def test_broker_values_native_receipts_and_missing_companions(
    tmp_path, broker_view
):
    fingerprint, view, request = broker_view
    with (
        generated_provider_scope(fingerprint, request),
        provider_native_inputs(fingerprint, request),
    ):
        path = write_training_wide_artifact(view, tmp_path)
        result = read_training_wide_artifact(
            path, information_mode=JoinInformationMode.EX_POST
        )
        assert result.records[0]["values"][
            "broker_style.EURUSD.spread"
        ] == pytest.approx(0.0002)
        assert all(
            (tmp_path / (child.filename + ".provider-policy.json")).is_file()
            for child in (
                view.manifest.controls[0].child,
                *(g.child for g in view.manifest.groups),
            )
        )
        control = tmp_path / (
            view.manifest.controls[0].child.filename + ".provider-policy.json"
        )
        control.unlink()
        with pytest.raises(ValueError, match="receipt"):
            read_training_wide_artifact(
                path, information_mode=JoinInformationMode.EX_POST, columns=()
            )


@pytest.mark.parametrize(
    "operation",
    (Operation.MATERIAL_USE, Operation.DERIVE, Operation.RETAIN_LOCAL),
)
def test_fresh_denial_prevents_material_or_retained_boundary(
    tmp_path, broker_view, operation
):
    fingerprint, view, request = broker_view
    (binding,) = resolve_provider_subject(fingerprint).bindings
    source = MutablePolicySource(
        policy_context(
            binding,
            changes=((operation, DataClass.FINGERPRINTS, Status.DENIED),),
        )
    )
    with (
        provider_policy_scope(source),
        provider_native_inputs(fingerprint, request),
    ):
        with pytest.raises(BrokerPolicyError):
            write_training_wide_artifact(view, tmp_path / "refused")
        if operation is not Operation.RETAIN_LOCAL:
            with pytest.raises(BrokerPolicyError):
                project_training_wide_view(
                    view, columns=("uncertainty.EURUSD.missing",)
                )
            with pytest.raises(BrokerPolicyError):
                diagnose_training_wide_view(view)
    assert not (tmp_path / "refused").exists()


def test_revoked_unprojected_broker_source_refuses_existing_artifact(
    tmp_path, broker_view
):
    fingerprint, view, request = broker_view
    with (
        generated_provider_scope(fingerprint, request),
        provider_native_inputs(fingerprint, request),
    ):
        path = write_training_wide_artifact(view, tmp_path)
    (binding,) = resolve_provider_subject(fingerprint).bindings
    denied = MutablePolicySource(
        policy_context(
            binding,
            changes=(
                (Operation.MATERIAL_USE, DataClass.FINGERPRINTS, Status.DENIED),
            ),
        )
    )
    with (
        provider_policy_scope(denied),
        provider_native_inputs(fingerprint, request),
        pytest.raises(BrokerPolicyError),
    ):
        read_training_wide_artifact(
            path,
            information_mode=JoinInformationMode.EX_POST,
            columns=("uncertainty.EURUSD.missing",),
        )

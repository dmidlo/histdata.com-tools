"""Small real IPC/catalog/committed-product fixtures, never market evidence."""

from dataclasses import replace
from contextlib import contextmanager
from datetime import datetime, timezone

import polars as pl

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.data_quality.training_contracts import DAY_NS, TrainingSourceV1
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)
from histdatacom.synthetic.carving import HistoricalCarvingConstraintSetV1
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyValidationStage,
    eurusd_triangle_reconciliation_config,
    reconcile_cross_currency_window,
    validate_cross_currency_output,
)
from histdatacom.synthetic.delivery import project_modern_reference_delivery
from histdatacom.synthetic.persistence import (
    commit_delivery_reconstruction_publication,
    estimate_reconstruction_retention,
    stage_delivery_reconstruction_publication,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

BASE = (
    int(datetime(2020, 1, 2, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
)
SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
TIMES = (
    BASE + 1_000_000_000,
    BASE + 3_000_000_000,
    BASE + DAY_NS - 1_000_000_000,
    BASE + DAY_NS + 1_000_000_000,
    BASE + 2 * DAY_NS + 1_000_000_000,
    BASE + 2 * DAY_NS + 3_000_000_000,
)
QUOTES = {
    "EURUSD": (1.1999, 1.2001),
    "GBPUSD": (1.4999, 1.5001),
    "EURGBP": (1.1999 / 1.5001, 1.2001 / 1.4999),
}


def observed_source(tmp_path, *, periods=("202001",)):
    root = tmp_path / "ASCII" / "T"
    adapter = HistDataProviderAdapter()
    for period in periods:
        for symbol in SYMBOLS:
            times = (
                TIMES
                if period == "202001"
                else tuple(
                    int(
                        datetime(
                            int(period[:4]),
                            int(period[4:]),
                            2,
                            tzinfo=timezone.utc,
                        ).timestamp()
                    )
                    * 1_000_000_000
                    + i * 1_000_000_000
                    for i in range(6)
                )
            )
            bid, ask = QUOTES[symbol]
            frame = pl.DataFrame(
                {
                    "datetime": [t // 1_000_000 for t in times],
                    "bid": [bid] * 6,
                    "ask": [ask] * 6,
                    "vol": [0] * 6,
                },
                schema={
                    "datetime": pl.Int64,
                    "bid": pl.Float64,
                    "ask": pl.Float64,
                    "vol": pl.Int32,
                },
            )
            path = histdata_cache_path(root, symbol, period)
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.write_ipc(path)
    evidence = tmp_path / "qualification.json"
    evidence.write_text('{"fixture":true}', encoding="utf-8")
    descriptor = DatasetDescriptorV1(
        "training-observed-fixture",
        "Training fixture",
        "Normalized fixture, not an empirical feed.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=periods,
        qualification_evidence=(
            artifact_ref_for_file(evidence, kind="fixture-evidence"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    return (
        TrainingSourceV1(catalog.to_json(), version.dataset_version_id),
        version,
    )


def published_product(
    tmp_path,
    version,
    *,
    member="member-a",
    seed=606,
    indices=(0, 1),
    wrong_anchor=False,
    storage_version=2,
    wrong_dependency=False,
    _provider_roots=None,
):
    config = eurusd_triangle_reconciliation_config()
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="fixture"
    )
    run = ReconstructionRunV1(
        SYMBOLS,
        (version.dataset_version_id,),
        (config.config_id,),
        (member,),
        seed,
    )
    left_index, right_index = indices
    left_time, right_time = TIMES[left_index], TIMES[right_index]
    window = ReconstructionWindowV1(
        run.run_id, member, SYMBOLS, left_time, right_time + 1
    )
    streams = []
    for symbol in SYMBOLS:
        partition = next(
            p
            for p in version.partitions
            if p.symbol == symbol and p.period == "202001"
        )
        bid, ask = QUOTES[symbol]
        anchors = tuple(
            SyntheticEventV1.observed(
                symbol=symbol,
                event_time_ns=TIMES[index],
                event_sequence=0,
                bid=bid + (0.000001 if wrong_anchor else 0),
                ask=ask,
                run_id=run.run_id,
                ensemble_member_id=member,
                source_version_id=version.dataset_version_id,
                source_series_id=partition.series_id,
                source_period=partition.period,
                source_row_id=index + 1,
            )
            for index in indices
        )
        middle = SyntheticEventV1.generated(
            symbol=symbol,
            event_time_ns=(left_time + right_time) // 2,
            event_sequence=100,
            bid=bid,
            ask=ask,
            run_id=run.run_id,
            ensemble_member_id=member,
            source_version_id=version.dataset_version_id,
            left_anchor_event_id=(
                "event:unverified" if wrong_dependency else anchors[0].event_id
            ),
            right_anchor_event_id=anchors[1].event_id,
            generator_id="fixture",
            generator_version="1.0.0",
            generator_config_id="fixture:generator",
            constraint_set_id=constraints.constraint_set_id,
        )
        streams.append(
            SyntheticEventStreamV1(
                run.run_id, member, symbol, (anchors[0], middle, anchors[1])
            )
        )
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams={s.symbol: s for s in streams},
        config=config,
    )
    delivered = project_modern_reference_delivery(
        group, delivery_profile_id="modern-reference:training-fixture"
    )
    anchors = tuple(
        e
        for s in delivered.streams
        for e in s.events
        if e.origin.value == "observed"
    )
    validation = validate_cross_currency_output(
        run=run,
        window=window,
        streams={s.symbol: s for s in delivered.streams},
        config=config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=anchors,
    )
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=member,
        retained_member_event_counts={member: 9},
        estimated_partition_count=9,
        storage_policy=run.storage_policy,
    )
    if storage_version == 1:
        from histdatacom.broker_capture import fit_broker_delivery_fingerprint
        from histdatacom.synthetic.broker_transfer import (
            BrokerTransferConfigV1,
            render_broker_delivery,
        )
        from histdatacom.synthetic.persistence import (
            publish_reconstruction_group,
        )
        from tests.unit.test_broker_delivery_fingerprints import (
            BASE_WALL_NS,
            _capture,
        )
        from histdatacom.broker_plugin_policy import provider_native_inputs
        from tests.fixtures.broker_provider_policy import (
            generated_legacy_request,
            generated_provider_scope,
        )

        capture = _capture(
            tmp_path / "capture", seed=606, wall_start_ns=BASE_WALL_NS
        )
        request = generated_legacy_request(capture.session)
        with generated_provider_scope(request), provider_native_inputs(request):
            fingerprint = fit_broker_delivery_fingerprint(
                tmp_path / "capture", (capture,)
            )
        with (
            generated_provider_scope(fingerprint),
            provider_native_inputs(fingerprint),
        ):
            rendered = render_broker_delivery(
                run=run,
                window=window,
                group=group,
                fingerprint=fingerprint,
                constraints=constraints,
                selected_at_utc_ns=fingerprint.effective_start_utc_ns,
                config=BrokerTransferConfigV1(
                    strength=0.0, apply_exact_duplicates=False
                ),
                quality_period="202001",
            )
            product = publish_reconstruction_group(
                tmp_path / "archive",
                rendered,
                immutable_source_anchors=anchors,
                symbol_group_id=window.synchronization_unit_id,
                retention_plan=retention,
                storage_policy=run.storage_policy,
            )
        if _provider_roots is not None:
            _provider_roots.append(fingerprint)
        return product, rendered
    staged = stage_delivery_reconstruction_publication(
        tmp_path / "archive",
        delivered,
        final_validation=validation,
        benchmark_artifact_ids=("fixture:benchmark",),
        benchmark_evidence={"gate": "fixture"},
        immutable_source_anchors=anchors,
        immutable_source_artifacts=(
            {p.series_id: p.artifact for p in version.partitions}
            if storage_version == 3
            else None
        ),
        symbol_group_id=window.synchronization_unit_id,
        retention_plan=retention,
        storage_policy=run.storage_policy,
        staging_root=tmp_path / "scratch",
    )
    product = commit_delivery_reconstruction_publication(staged)
    return product, delivered


@contextmanager
def published_product_scope(tmp_path, version, **kwargs):
    """Opt-in exact provider roots for a generated V1 product's consumers."""
    from histdatacom.broker_plugin_policy import provider_native_inputs
    from tests.fixtures.broker_provider_policy import generated_provider_scope

    roots = []
    result = published_product(
        tmp_path, version, _provider_roots=roots, **kwargs
    )
    if roots:
        with generated_provider_scope(*roots), provider_native_inputs(*roots):
            yield result
    else:
        yield result


def with_products(source, *products):
    return replace(
        source,
        product_manifest_paths=tuple(
            sorted(str(p.manifest_path) for p in products)
        ),
    )


def macro_matrix(*, value=2.0, end=BASE + 4_000_000_000):
    from histdatacom.forecasting import (
        FeatureColumnV1,
        FeatureDefinitionV1,
        FeatureEvidenceV1,
        FeatureKind,
        FeatureMatrixSnapshotV1,
        FeatureObservationV1,
        FeaturePeriodV1,
        FeatureRequestV1,
        FeatureSourceMode,
    )

    period = FeaturePeriodV1("fixture-period", BASE, end)
    definition = FeatureDefinitionV1(
        "macro.fixture",
        FeatureKind.MACRO,
        "percent",
        1.0,
        "unadjusted",
        "fixture",
        "fixture",
        BASE,
    )
    evidence = FeatureEvidenceV1(
        "Fixture",
        "https://example.invalid",
        "a" * 64,
        "fixture-record",
        "fixture",
        "1.0.0",
        "asserted fixture clock",
        end + 2,
        FeatureSourceMode.HISTORICAL_VINTAGE,
        '{"fixture":true}',
    )
    observation = FeatureObservationV1(
        definition, period, value, end + 1, end + 1, 0, None, evidence
    )
    return FeatureMatrixSnapshotV1(
        FeatureRequestV1(
            end + 2,
            (period,),
            (FeatureColumnV1("macro_value", "macro.fixture"),),
        ),
        (observation,),
    )


def installed_training_smoke(tmp_path):
    """Run against site-packages with only the tests package added to sys.path."""
    from histdatacom.data_quality.training_artifacts import (
        read_training_artifact,
        write_training_artifact,
    )
    from histdatacom.data_quality.training_contracts import (
        TrainingConsumerMode,
        TrainingRequestV1,
    )
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.data_quality.training_views import (
        materialize_training_rows,
        training_frame,
    )
    from histdatacom.forecasting.feature_artifacts import write_feature_artifact
    from tests.fixtures.forecast_feature_store_v1 import (
        feature_forecast_fixture,
    )

    mode = TrainingConsumerMode.DESCRIPTIVE
    source, version = observed_source(tmp_path / "market")
    products = tuple(
        published_product(
            tmp_path / f"v{number}", version, storage_version=number
        )[0]
        for number in (1, 2, 3)
    )
    matrix = macro_matrix()
    matrix_path = write_feature_artifact(matrix, tmp_path / "features")
    source = replace(
        with_products(source, *products),
        context_artifact_paths=(str(matrix_path),),
    )
    ownership = build_training_ownership(source)
    requests = [TrainingRequestV1(mode, BASE, BASE + 3 * DAY_NS, SYMBOLS)]
    requests += [
        replace(requests[0], product_manifest_id=p.manifest.manifest_id)
        for p in products
    ]
    requests.append(
        replace(requests[0], feature_artifact_id=matrix.snapshot_id)
    )
    counts = []
    for request in requests:
        batch = materialize_training_rows(source, ownership, request)
        restored = read_training_artifact(
            write_training_artifact(batch, tmp_path / "rows"),
            consumer_mode=mode,
        )
        assert restored == batch
        frame = training_frame(restored, consumer_mode=mode)
        assert frame.height == len(batch.rows)
        counts.append(frame.height)
    forecast_source, _ = observed_source(
        tmp_path / "forecast-market",
        periods=("202309", "202310", "202311", "202312", "202401"),
    )
    forecast = feature_forecast_fixture()
    forecast_path = write_feature_artifact(forecast, tmp_path / "features")
    forecast_source = replace(
        forecast_source, derived_artifact_paths=(str(forecast_path),)
    )
    ownership = build_training_ownership(forecast_source)
    request = TrainingRequestV1(
        mode,
        forecast.cutoff.cutoff_at_ns,
        forecast.cutoff.cutoff_at_ns + 1,
        SYMBOLS,
        feature_artifact_id=forecast.to_dict()["id"],
    )
    batch = materialize_training_rows(forecast_source, ownership, request)
    restored = read_training_artifact(
        write_training_artifact(batch, tmp_path / "rows"), consumer_mode=mode
    )
    counts.append(training_frame(restored, consumer_mode=mode).height)
    assert counts == [18, 9, 9, 9, 1, 1]
    return {"row_counts": counts, "forecast_origin": batch.rows[0].origin.value}

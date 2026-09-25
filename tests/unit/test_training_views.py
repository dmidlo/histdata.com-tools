"""Executable row consumers, durable replay and origin/clock refusal paths."""

import hashlib
import json
import os
from dataclasses import replace

import pytest

from histdatacom.data_quality.training_artifacts import (
    read_training_artifact,
    write_training_artifact,
)
from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingBatchV1,
    TrainingConsumerMode,
    TrainingInformationMode,
    TrainingOrigin,
    TrainingRequestV1,
    TrainingVerificationLevel,
    training_json,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_views import (
    materialize_training_rows,
    replay_training_batch,
    training_frame,
)
from histdatacom.forecasting.feature_artifacts import write_feature_artifact
from tests.fixtures.training_substrate_v1 import (
    BASE,
    SYMBOLS,
    TIMES,
    macro_matrix,
    observed_source,
    published_product,
    published_product_scope,
    with_products,
)


def request(*, product=None, mode=TrainingConsumerMode.DESCRIPTIVE, **kwargs):
    return TrainingRequestV1(
        mode,
        kwargs.pop("start", BASE),
        kwargs.pop("end", BASE + 3 * DAY_NS),
        kwargs.pop("symbols", SYMBOLS),
        product_manifest_id=(
            None if product is None else product.manifest.manifest_id
        ),
        **kwargs,
    )


@pytest.fixture
def source(tmp_path):
    return observed_source(tmp_path)


@pytest.mark.parametrize("mode", list(TrainingConsumerMode))
def test_real_observed_rows_modes_clocks_and_immutable_source_selection(
    source, mode
):
    source, version = source
    ownership = build_training_ownership(source)
    selected = request(
        mode=mode, start=TIMES[0], end=TIMES[1], symbols=("EURUSD",)
    )
    if mode is TrainingConsumerMode.CAUSAL:
        with pytest.raises(ValueError, match="historical availability"):
            materialize_training_rows(source, ownership, selected)
        return
    batch = materialize_training_rows(source, ownership, selected)
    assert len(batch.rows) == 1
    row = batch.rows[0]
    assert row.origin is TrainingOrigin.OBSERVED
    assert row.information_mode is TrainingInformationMode.EX_POST
    assert row.event_time_ns == row.decision_time_ns == TIMES[0]
    assert row.available_at_ns is None
    assert row.observed_dataset_version_id == version.dataset_version_id
    assert row.label_schema_version.endswith("none.v1")
    frame = training_frame(batch, consumer_mode=mode)
    assert frame.height == 1
    assert frame["value.bid"].to_list() == [1.1999]
    assert not any(name.endswith("vol") for name in frame.columns)
    assert (
        frame["value.volume_state"].item()
        == "unavailable_pending_source_semantics"
    )
    assert not any(
        "median" in name or "regime" in name for name in frame.columns
    )
    assert frame["lineage.evidence_unit_id"].item() == row.evidence_unit_id


def test_native_rows_preserve_every_event_field_and_member_ownership(
    source, tmp_path
):
    source, version = source
    a, delivered = published_product(tmp_path / "a", version)
    b, _ = published_product(
        tmp_path / "b", version, member="member-b", seed=607
    )
    source = with_products(source, a, b)
    ownership = build_training_ownership(source)
    batches = [
        materialize_training_rows(source, ownership, request(product=p))
        for p in (a, b)
    ]
    assert [len(batch.rows) for batch in batches] == [9, 9]
    assert {row.evidence_unit_id for row in batches[0].rows} == {
        row.evidence_unit_id for row in batches[1].rows
    }
    assert batches[0].artifact_id != batches[1].artifact_id
    assert {row.origin for row in batches[0].rows} == {
        TrainingOrigin.OBSERVED,
        TrainingOrigin.SYNTHETIC_RECONSTRUCTION,
    }
    assert [json.loads(row.value_json) for row in batches[0].rows] == [
        e.to_dict() for stream in delivered.streams for e in stream.events
    ]
    assert all(row.available_at_ns is None for row in batches[0].rows)
    assert all(row.run_id == a.manifest.run_id for row in batches[0].rows)
    assert all(
        row.ensemble_member_id == a.manifest.ensemble_member_id
        for row in batches[0].rows
    )
    frame = training_frame(
        batches[0], consumer_mode=TrainingConsumerMode.DESCRIPTIVE
    )
    assert frame.height == 9
    assert frame["value.confidence"].dtype.is_float()
    observed = materialize_training_rows(
        source, ownership, request(symbols=("EURUSD",))
    )
    native_unit = batches[0].rows[0].evidence_unit_id
    assert observed.rows[0].evidence_unit_id == native_unit
    selected = materialize_training_rows(
        source,
        ownership,
        request(product=a, start=TIMES[0], end=TIMES[1], symbols=("EURUSD",)),
    )
    assert {r.evidence_unit_id for r in selected.rows} == {native_unit}
    with pytest.raises(ValueError, match="precedes full"):
        materialize_training_rows(
            source, ownership, request(product=a, decision_time_ns=TIMES[0])
        )


def test_native_undeclared_product_and_missing_source_fail(source, tmp_path):
    source, version = source
    product, _ = published_product(tmp_path / "product", version)
    ownership = build_training_ownership(source)
    with pytest.raises(ValueError, match="absent from frozen"):
        materialize_training_rows(source, ownership, request(product=product))


@pytest.mark.parametrize("storage_version", [1, 3])
def test_real_legacy_broker_and_source_referenced_products(
    source, tmp_path, storage_version
):
    source, version = source
    with published_product_scope(
        tmp_path / "product", version, storage_version=storage_version
    ) as (product, _):
        source = with_products(source, product)
        batch = materialize_training_rows(
            source, build_training_ownership(source), request(product=product)
        )
        assert len(batch.rows) == 9
        expected = (
            TrainingOrigin.BROKER_CONDITIONED_COUNTERFACTUAL
            if storage_version == 1
            else TrainingOrigin.SYNTHETIC_RECONSTRUCTION
        )
        assert {row.origin for row in batch.rows} == {
            TrainingOrigin.OBSERVED,
            expected,
        }
        assert all(
            row.information_mode is TrainingInformationMode.EX_POST
            for row in batch.rows
        )
        source = with_products(source, product)
        ownership = build_training_ownership(source)
        path = (
            product.manifest_path.parent
            / product.manifest.partitions[0].relative_path
        )
        path.write_bytes(b"tampered")
        with pytest.raises(ValueError):
            materialize_training_rows(
                source, ownership, request(product=product)
            )


def test_actual_macro_rows_retain_weaker_verification_and_missing_cells(
    source, tmp_path
):
    source, _ = source
    matrix = macro_matrix()
    path = write_feature_artifact(matrix, tmp_path / "features")
    source = replace(source, context_artifact_paths=(str(path),))
    ownership = build_training_ownership(source)
    batch = materialize_training_rows(
        source, ownership, request(feature_artifact_id=matrix.snapshot_id)
    )
    assert len(batch.rows) == 1
    row = batch.rows[0]
    assert row.origin is TrainingOrigin.OFFICIAL_CONTEXT
    assert row.symbol is None
    assert row.available_at_ns == matrix.cells[0].available_at_ns
    assert json.loads(row.value_json) == matrix.cells[0].to_dict()
    assert TrainingVerificationLevel.DERIVED_REPLAY in {
        r.verification for r in row.verification_roots
    }
    frame = training_frame(
        batch, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
    )
    assert frame["value.value"].item() == 2.0
    assert frame["lineage.origin"].item() == "official_context"
    assert (
        "normalized_context_is_not_official_source_authenticity"
        in row.nonclaims
    )
    for mode in (
        TrainingConsumerMode.CAUSAL,
        TrainingConsumerMode.AUGMENTATION,
        TrainingConsumerMode.RECONSTRUCTION,
    ):
        with pytest.raises(ValueError, match="availability|descriptive"):
            materialize_training_rows(
                source,
                ownership,
                request(mode=mode, feature_artifact_id=matrix.snapshot_id),
            )
    with pytest.raises(ValueError, match="full graph"):
        materialize_training_rows(
            source,
            ownership,
            request(
                feature_artifact_id=matrix.snapshot_id, symbols=("EURUSD",)
            ),
        )
    with pytest.raises(ValueError, match="precedes complete"):
        materialize_training_rows(
            source,
            ownership,
            request(
                feature_artifact_id=matrix.snapshot_id, decision_time_ns=BASE
            ),
        )


def test_actual_machine_forecast_distribution_rows_are_descriptive_not_official(
    tmp_path,
):
    from tests.fixtures.forecast_feature_store_v1 import (
        feature_forecast_fixture,
    )

    source, _ = observed_source(
        tmp_path, periods=("202309", "202310", "202311", "202312", "202401")
    )
    forecast = feature_forecast_fixture()
    path = write_feature_artifact(forecast, tmp_path / "features")
    source = replace(source, derived_artifact_paths=(str(path),))
    ownership = build_training_ownership(source)
    identity = forecast.to_dict()["id"]
    cutoff = forecast.cutoff.cutoff_at_ns
    selected = request(
        start=cutoff, end=cutoff + 1, feature_artifact_id=identity
    )
    batch = materialize_training_rows(source, ownership, selected)
    assert len(batch.rows) == 1
    row = batch.rows[0]
    assert row.origin is TrainingOrigin.MACHINE_FORECAST
    assert row.information_mode is TrainingInformationMode.EX_POST
    assert row.event_time_ns == cutoff
    assert row.available_at_ns == forecast.generated_at_ns
    assert (
        json.loads(json.loads(row.value_json)["distribution_json"])
        == forecast.distribution.to_dict()
    )
    frame = training_frame(
        batch, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
    )
    assert frame["value.point"].item() == forecast.distribution.point
    assert frame["value.unit"].item() == forecast.target.unit
    assert (
        read_training_artifact(
            write_training_artifact(batch, tmp_path / "rows"),
            consumer_mode=TrainingConsumerMode.DESCRIPTIVE,
        )
        == batch
    )


def test_projection_keeps_origin_and_lineage_and_empty_schema(source):
    source, _ = source
    ownership = build_training_ownership(source)
    batch = materialize_training_rows(source, ownership, request())
    frame = training_frame(
        batch,
        consumer_mode=TrainingConsumerMode.DESCRIPTIVE,
        value_columns=("bid",),
    )
    assert "value.ask" not in frame.columns
    assert "lineage.origin" in frame.columns
    assert "lineage.verification_roots" in frame.columns
    with pytest.raises(ValueError, match="projection"):
        training_frame(
            batch,
            consumer_mode=TrainingConsumerMode.DESCRIPTIVE,
            value_columns=("lineage.origin",),
        )
    empty = materialize_training_rows(
        source, ownership, request(start=BASE + 10, end=BASE + 11)
    )
    empty_frame = training_frame(
        empty, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
    )
    full_frame = training_frame(
        batch, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
    )
    assert empty_frame.schema == full_frame.schema
    with pytest.raises(ValueError, match="mode"):
        training_frame(batch, consumer_mode=TrainingConsumerMode.CAUSAL)


def test_write_read_replay_refuses_resealed_row_mutation_and_wrong_mode(
    source, tmp_path
):
    source, _ = source
    ownership = build_training_ownership(source)
    batch = materialize_training_rows(source, ownership, request())
    path = write_training_artifact(batch, tmp_path / "out")
    assert write_training_artifact(batch, tmp_path / "out") == path
    assert (
        read_training_artifact(
            path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        )
        == batch
    )
    with pytest.raises(ValueError, match="mode"):
        read_training_artifact(
            path, consumer_mode=TrainingConsumerMode.AUGMENTATION
        )
    value = json.loads(batch.rows[0].value_json)
    value["bid"] = 2.0
    mutated = replace(
        batch,
        rows=(replace(batch.rows[0], value_json=training_json(value)),)
        + batch.rows[1:],
    )
    assert mutated.artifact_id != batch.artifact_id
    with pytest.raises(ValueError, match="source replay"):
        replay_training_batch(mutated)
    with pytest.raises(ValueError, match="source replay"):
        write_training_artifact(mutated, tmp_path / "bad")
    data = mutated.to_json().encode()
    malicious = (
        tmp_path / f"training-batch-{hashlib.sha256(data).hexdigest()}.json"
    )
    malicious.write_bytes(data)
    with pytest.raises(ValueError, match="source replay"):
        read_training_artifact(
            malicious, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        )


def test_persistence_rejects_symlink_when_supported(source, tmp_path):
    source, _ = source
    batch = materialize_training_rows(
        source, build_training_ownership(source), request()
    )
    path = write_training_artifact(batch, tmp_path / "out")
    link = tmp_path / path.name
    try:
        link.symlink_to(path)
    except NotImplementedError as error:
        pytest.skip(f"symlink creation unavailable: {error}")
    except OSError as error:
        if os.name == "nt" and getattr(error, "winerror", None) == 1314:
            pytest.skip(f"symlink creation unavailable: {error}")
        raise
    with pytest.raises(ValueError, match="regular"):
        read_training_artifact(
            link, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        )


def test_persistence_filename_canonical_and_source_integrity(source, tmp_path):
    source, version = source
    batch = materialize_training_rows(
        source, build_training_ownership(source), request()
    )
    path = write_training_artifact(batch, tmp_path / "out")
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="digest"):
        read_training_artifact(
            path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        )
    path.write_text(batch.to_json(), encoding="utf-8")
    from pathlib import Path

    Path(version.partitions[0].artifact.path).write_bytes(b"changed source")
    with pytest.raises(ValueError):
        read_training_artifact(
            path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        )


def test_empty_causal_request_still_refuses_before_source_reads(
    source, monkeypatch
):
    from histdatacom.data_quality import training_views as module

    source, _ = source
    ownership = build_training_ownership(source)
    monkeypatch.setattr(
        module,
        "verify_training_ownership",
        lambda *args: pytest.fail("causal source read"),
    )
    with pytest.raises(ValueError, match="historical availability"):
        materialize_training_rows(
            source, ownership, request(mode=TrainingConsumerMode.CAUSAL)
        )


def test_resealed_rows_cannot_remove_or_replicate_support(source):
    source, _ = source
    batch = materialize_training_rows(
        source, build_training_ownership(source), request()
    )
    subset = replace(batch, rows=batch.rows[:-1])
    assert TrainingBatchV1.from_json(subset.to_json()) == subset
    with pytest.raises(ValueError, match="source replay"):
        replay_training_batch(subset)


@pytest.mark.parametrize("unknown", [False, True])
def test_context_all_missing_or_unknown_column_never_invents_macro_origin(
    source, tmp_path, unknown
):
    from histdatacom.forecasting import FeatureColumnV1

    source, _ = source
    matrix = macro_matrix()
    if unknown:
        matrix = replace(
            matrix,
            request=replace(
                matrix.request,
                columns=matrix.request.columns
                + (FeatureColumnV1("unknown", "anything.unknown"),),
            ),
        )
    else:
        matrix = replace(matrix, observations=(), schedules=())
    path = write_feature_artifact(matrix, tmp_path / "features")
    source = replace(source, context_artifact_paths=(str(path),))
    with pytest.raises(ValueError, match="retained macro definitions"):
        materialize_training_rows(
            source,
            build_training_ownership(source),
            request(feature_artifact_id=matrix.snapshot_id),
        )


def test_missing_period_for_retained_macro_definition_stays_missing(
    source, tmp_path
):
    from histdatacom.forecasting import FeaturePeriodV1

    source, _ = source
    matrix = macro_matrix()
    period = matrix.request.periods[0]
    later = FeaturePeriodV1("missing-later", period.end_ns, period.end_ns + 100)
    matrix = replace(
        matrix,
        request=replace(
            matrix.request,
            periods=(period, later),
            cutoff_at_ns=later.end_ns + 1,
        ),
    )
    path = write_feature_artifact(matrix, tmp_path / "features")
    source = replace(source, context_artifact_paths=(str(path),))
    batch = materialize_training_rows(
        source,
        build_training_ownership(source),
        request(feature_artifact_id=matrix.snapshot_id),
    )
    frame = training_frame(
        batch, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
    )
    assert frame["value.value"].to_list() == [2.0, None]
    assert frame["lineage.origin"].to_list() == [
        "official_context",
        "official_context",
    ]
    empty = materialize_training_rows(
        source,
        batch.ownership,
        request(
            feature_artifact_id=matrix.snapshot_id, start=BASE, end=BASE + 1
        ),
    )
    assert (
        training_frame(
            empty, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        ).schema
        == frame.schema
    )

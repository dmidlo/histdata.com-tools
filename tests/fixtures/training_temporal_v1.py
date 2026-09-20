"""Controlled IPC and canonical vintage clocks; never empirical evidence."""

from dataclasses import replace

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
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalFeatureKind,
    TemporalInformationMode,
    TemporalLabelKind,
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalExampleV1,
    TrainingTemporalFeatureV1,
    TrainingTemporalLabelV1,
    TrainingTemporalPlanV1,
    TrainingTemporalSplitV1,
)
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
from histdatacom.forecasting.feature_artifacts import write_feature_artifact
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from tests.fixtures.training_substrate_v1 import BASE, SYMBOLS

SECOND = 1_000_000_000


def temporal_source(
    tmp_path,
    *,
    matrix=False,
    crossed_seconds=(),
    duplicate_crossed=False,
    dense_points=0,
):
    root = tmp_path / "ASCII" / "T"
    adapter = HistDataProviderAdapter()
    times = [
        BASE + day * DAY_NS + second * SECOND
        for day in range(3)
        for second in (1, 2, 3, 4)
    ]
    if dense_points:
        times = [BASE + SECOND + i * 1_000_000 for i in range(dense_points)]
    for symbol in SYMBOLS:
        path = histdata_cache_path(root, symbol, "202001")
        path.parent.mkdir(parents=True, exist_ok=True)
        mids = [1.0 + index / 100 for index in range(len(times))]
        frame = pl.DataFrame(
            {
                "datetime": [t // 1_000_000 for t in times],
                "bid": [mid - 0.0001 for mid in mids],
                "ask": [
                    (
                        mid - 0.0002
                        if index + 1 in crossed_seconds
                        else mid + 0.0001
                    )
                    for index, mid in enumerate(mids)
                ],
                "vol": [0] * len(times),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        )
        if duplicate_crossed:
            earlier = frame.head(1).with_columns(
                (pl.col("bid") - 0.0001).alias("ask")
            )
            frame = pl.concat((earlier, frame))
        frame.write_ipc(path)
    qualification = tmp_path / "controlled-fixture.json"
    qualification.write_text('{"synthetic_fixture_not_market_evidence":true}')
    descriptor = DatasetDescriptorV1(
        "temporal-controlled-fixture",
        "Temporal controlled fixture",
        "Generated test quotes, not an empirical source.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=("202001",),
        qualification_evidence=(
            artifact_ref_for_file(qualification, kind="fixture"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    source = TrainingSourceV1(catalog.to_json(), version.dataset_version_id)
    snapshot = vintage_matrix() if matrix else None
    if snapshot is not None:
        path = write_feature_artifact(snapshot, tmp_path / "vintages")
        source = replace(source, derived_artifact_paths=(str(path),))
    return source, build_training_ownership(source), snapshot


def vintage_matrix():
    periods = tuple(
        FeaturePeriodV1(str(i), BASE + (i - 1) * SECOND, BASE + i * SECOND)
        for i in (1, 2, 3, 4)
    )

    def observation(key, kind, period, value, available, *, previous=None):
        definition = FeatureDefinitionV1(
            key,
            kind,
            "price" if kind is FeatureKind.MARKET else "index",
            1.0,
            "unadjusted",
            "controlled fixture",
            "one-second",
            BASE if previous is None else available,
            classification="initial" if previous is None else "final",
        )
        evidence = FeatureEvidenceV1(
            "controlled fixture",
            "fixture://temporal",
            "a" * 64,
            f"{key}-{period.label}-{available}",
            "controlled-fixture",
            "1.0.0",
            "explicit controlled simulation clock; not historical authenticity",
            BASE + 10 * SECOND,
            FeatureSourceMode.OBSERVED,
            '{"controlled_simulation":true}',
        )
        return FeatureObservationV1(
            definition,
            period,
            value,
            available,
            available,
            0 if previous is None else 1,
            None if previous is None else previous.observation_id,
            evidence,
        )

    market = tuple(
        observation("price", FeatureKind.MARKET, p, 10.0 + i, p.end_ns)
        for i, p in enumerate(periods)
    )
    macro = observation(
        "macro", FeatureKind.MACRO, periods[0], 1.0, BASE + SECOND
    )
    revision = observation(
        "macro",
        FeatureKind.MACRO,
        periods[0],
        9.0,
        BASE + 3 * SECOND,
        previous=macro,
    )
    classification = observation(
        "classification",
        FeatureKind.CLASSIFICATION,
        periods[0],
        0.0,
        BASE + SECOND,
    )
    final = observation(
        "classification",
        FeatureKind.CLASSIFICATION,
        periods[0],
        1.0,
        BASE + 3 * SECOND,
        previous=classification,
    )
    return FeatureMatrixSnapshotV1(
        FeatureRequestV1(
            BASE + 10 * SECOND,
            periods,
            tuple(
                FeatureColumnV1(key, key)
                for key in ("price", "macro", "classification")
            ),
        ),
        (*market, macro, revision, classification, final),
    )


def temporal_plan(
    source,
    ownership,
    *,
    example=None,
    partitions=None,
    mode=TemporalInformationMode.EX_POST,
):
    current = next(
        i
        for i, unit in enumerate(ownership.units)
        if unit.start_ns <= BASE < unit.end_ns
    )
    if partitions is not None and len(partitions) == 3:
        partitions = (
            (partitions[0],) * (current + 1)
            + (partitions[1],)
            + (partitions[2],) * (len(ownership.units) - current - 2)
        )
    partitions = partitions or (TemporalPartition.TRAIN,) * len(ownership.units)
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(unit.artifact_id, partition)
            for unit, partition in zip(ownership.units, partitions)
        ),
    )
    example = example or TrainingTemporalExampleV1(
        "one",
        BASE + SECOND,
        BASE + 4 * SECOND,
        "EURUSD",
        ownership.units[current].artifact_id,
        partitions[current],
        (
            TrainingTemporalFeatureV1(
                "last", TemporalFeatureKind.QUOTE_AT_DECISION
            ),
        ),
        TrainingTemporalLabelV1(TemporalLabelKind.LOG_RETURN, 2 * SECOND),
    )
    return TrainingTemporalPlanV1(source, ownership, split, mode, (example,))

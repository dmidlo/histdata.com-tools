"""API-ready V2 ASCII/T projections and synthetic companion lineage."""

from __future__ import annotations

from typing import Any

from histdatacom.data_quality.training_features import (
    TRAINING_SCHEMA_VERSION,
    enrich_tick_cache_with_training_features,
)
from histdatacom.datasets.adapters import ProviderAdapter
from histdatacom.datasets.contracts import (
    CanonicalObservedPartitionV2,
    DatasetContractError,
    DatasetEventLineageV2,
    DatasetFailureCode,
    DatasetOrigin,
    DatasetVersionManifestV1,
)
from histdatacom.synthetic.contracts import SyntheticEventV1

DATASET_TICK_PROJECTION_SCHEMA_VERSION = "histdatacom.api-ascii-tick.v2"

DATASET_TICK_LINEAGE_COLUMNS = (
    "dataset_lineage_schema_version",
    "source_provider_id",
    "dataset_id",
    "dataset_version_id",
    "origin",
    "delivery_profile_id",
    "source_series_id",
    "source_period",
    "source_row_id",
    "dataset_row_key",
)


def project_observed_ascii_ticks_v2(
    adapter: ProviderAdapter,
    manifest: DatasetVersionManifestV1,
    partition: CanonicalObservedPartitionV2,
    *,
    delivery_profile_id: str | None = None,
) -> Any:
    """Return canonical observed values plus explicit provider/version lineage.

    The V1 enriched-training columns and their identities are left intact.  V2
    lineage is appended under a new schema marker so clients do not need to
    inspect quality reports or private catalog side tables.
    """
    if manifest.origin is not DatasetOrigin.OBSERVED:
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_ORIGIN,
            "observed projection requires an observed dataset version",
        )
    if partition not in manifest.partitions:
        raise DatasetContractError(
            DatasetFailureCode.IDENTITY_MISMATCH,
            "partition is absent from immutable dataset version",
        )
    frame = adapter.read_partition(partition)
    enriched = enrich_tick_cache_with_training_features(
        frame,
        symbol=partition.symbol,
        data_format="ascii",
        timeframe="T",
        period=partition.period,
        source=partition.source_provider_id,
    )
    if set(enriched.get_column("training_schema_version").unique()) != {
        TRAINING_SCHEMA_VERSION
    }:
        raise DatasetContractError(
            DatasetFailureCode.IDENTITY_MISMATCH,
            "observed projection changed the V1 training schema marker",
        )
    import polars as pl  # pylint: disable=import-outside-toplevel

    projected = enriched.with_columns(
        [
            pl.lit(DATASET_TICK_PROJECTION_SCHEMA_VERSION).alias(
                "dataset_lineage_schema_version"
            ),
            pl.lit(partition.source_provider_id).alias("source_provider_id"),
            pl.lit(manifest.dataset_id).alias("dataset_id"),
            pl.lit(manifest.dataset_version_id).alias("dataset_version_id"),
            pl.lit(DatasetOrigin.OBSERVED.value).alias("origin"),
            pl.lit(delivery_profile_id or manifest.delivery_profile_id)
            .cast(pl.Utf8)
            .alias("delivery_profile_id"),
            pl.lit(partition.series_id).alias("source_series_id"),
            pl.lit(partition.period).alias("source_period"),
            pl.col("source_row_number").cast(pl.Int64).alias("source_row_id"),
        ]
    )
    return projected.with_columns(
        pl.concat_str(
            [
                pl.col("dataset_version_id"),
                pl.col("source_series_id"),
                pl.col("source_period"),
                pl.col("source_row_id").cast(pl.Utf8),
            ],
            separator="|",
        ).alias("dataset_row_key")
    )


def synthetic_event_lineage_v2(
    event: SyntheticEventV1,
    manifest: DatasetVersionManifestV1,
) -> DatasetEventLineageV2:
    """Return V2 dataset lineage without mutating SyntheticEventV1 Arrow."""
    if event.origin.value != "synthetic":
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_ORIGIN,
            "synthetic companion lineage requires a synthetic event",
        )
    if manifest.origin is not DatasetOrigin.SYNTHETIC:
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_ORIGIN,
            "synthetic event requires a synthetic dataset version",
        )
    parent_versions = tuple(
        item.parent_dataset_version_id for item in manifest.parents
    )
    if event.source_version_id not in parent_versions:
        raise DatasetContractError(
            DatasetFailureCode.IDENTITY_MISMATCH,
            "synthetic event source is absent from dataset parent lineage",
        )
    return DatasetEventLineageV2(
        dataset_id=manifest.dataset_id,
        dataset_version_id=manifest.dataset_version_id,
        origin=manifest.origin,
        source_provider_id=None,
        parent_dataset_version_ids=parent_versions,
        delivery_profile_id=manifest.delivery_profile_id,
        ensemble_member_id=event.ensemble_member_id,
        event_id=event.event_id,
        anchor_interval_id=event.anchor_interval_id,
        generator_id=event.generator_id,
    )


__all__ = [
    "DATASET_TICK_LINEAGE_COLUMNS",
    "DATASET_TICK_PROJECTION_SCHEMA_VERSION",
    "project_observed_ascii_ticks_v2",
    "synthetic_event_lineage_v2",
]

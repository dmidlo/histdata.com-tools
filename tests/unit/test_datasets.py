"""Closure gates for provider-neutral adapters and versioned datasets."""

from __future__ import annotations

import json
import shutil
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

from histdatacom.dataset_cli import main as dataset_cli_main
from histdatacom.datasets import (
    CANONICAL_TICK_PROJECTION_SCHEMA_VERSION,
    DATASET_TICK_PROJECTION_SCHEMA_VERSION,
    CanonicalObservedPartitionV2,
    DatasetAliasV1,
    DatasetCatalog,
    DatasetContractError,
    DatasetDescriptorV1,
    DatasetEventLineageV2,
    DatasetFailureCode,
    DatasetLicensingPolicy,
    DatasetOrigin,
    DatasetParentV1,
    DatasetQualificationStatus,
    DatasetQueryScopeV1,
    DatasetVerificationV1,
    DatasetVersionManifestV1,
    FixtureProviderAdapter,
    HistDataProviderAdapter,
    ProviderSourceInventoryV2,
    SourceProviderDescriptorV1,
    build_observed_dataset_version,
    fixture_csv_path,
    histdata_cache_path,
    project_observed_ascii_ticks_v2,
    read_resolution_receipt,
    synthetic_event_lineage_v2,
)
from histdatacom.histdata_com import main as histdatacom_main
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.synthetic.contracts import SyntheticEventV1

_SYMBOL = "EURUSD"
_PERIOD = "202001"
_START_MS = int(datetime(2020, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)


def _write_histdata_cache(root: Path) -> tuple[Path, pl.DataFrame]:
    path = histdata_cache_path(root, _SYMBOL, _PERIOD)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "datetime": [_START_MS, _START_MS, _START_MS + 1000],
            "bid": [1.1000, 1.1001, 1.1002],
            "ask": [1.1002, 1.1003, 1.1004],
            "vol": [0, 0, 0],
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    )
    frame.write_ipc(path)
    return path, frame


def _write_fixture_csv(
    root: Path,
    *,
    ambiguous: bool = False,
    symbol: str = _SYMBOL,
    period: str = _PERIOD,
) -> Path:
    path = fixture_csv_path(root, symbol, period)
    path.parent.mkdir(parents=True, exist_ok=True)
    zone = "" if ambiguous else "Z"
    date = f"{period[:4]}-{period[4:]}-02"
    path.write_text(
        "timestamp,bid,ask,vol,native_id\n"
        f"{date}T00:00:00{zone},1.1000,1.1002,0,native-1\n"
        f"{date}T00:00:01{zone},1.1001,1.1003,0,native-2\n",
        encoding="utf-8",
    )
    return path


def _evidence(tmp_path: Path) -> Any:
    path = tmp_path / "qualification.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"qualified": True, "checks": ["schema", "quotes"]}),
        encoding="utf-8",
    )
    return artifact_ref_for_file(path, kind="dataset_qualification_v1")


def _descriptor(
    dataset_id: str = "histdata-observed-ticks",
) -> DatasetDescriptorV1:
    return DatasetDescriptorV1(
        dataset_id=dataset_id,
        display_name="Observed tick fixture",
        description="Qualified canonical ASCII/T observed ticks.",
        allowed_origins=(DatasetOrigin.OBSERVED,),
    )


def _catalog(
    adapter: Any,
    root: Path,
    tmp_path: Path,
    *,
    dataset_id: str = "histdata-observed-ticks",
    alias: str = "latest-qualified",
) -> tuple[DatasetCatalog, DatasetVersionManifestV1]:
    descriptor = _descriptor(dataset_id)
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
        qualification_evidence=(_evidence(tmp_path),),
    )
    catalog = DatasetCatalog(
        providers=(adapter.provider,),
        adapters=(adapter.descriptor,),
        datasets=(descriptor,),
        versions=(version,),
        aliases=(
            DatasetAliasV1(
                alias=alias,
                dataset_id=descriptor.dataset_id,
                dataset_version_id=version.dataset_version_id,
                revision=1,
            ),
        ),
    )
    return catalog, version


def test_histdata_adapter_preserves_observed_cache_values_and_v1_identity(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    _, expected = _write_histdata_cache(source_root)
    adapter = HistDataProviderAdapter()
    catalog, version = _catalog(adapter, source_root, tmp_path)
    partition = version.partitions[0]

    direct = adapter.read_partition(partition)
    projected = project_observed_ascii_ticks_v2(adapter, version, partition)

    assert direct.select(expected.columns).equals(expected)
    assert projected.select(expected.columns).equals(expected)
    assert projected.get_column("series_id").unique().to_list() == [
        "ascii:T:EURUSD:histdata.com"
    ]
    assert projected.get_column("source_series_id").unique().to_list() == [
        "ascii:T:EURUSD:histdata.com"
    ]
    assert projected.get_column("row_id").to_list() == [1, 2, 3]
    assert projected.get_column("source_row_id").to_list() == [1, 2, 3]
    assert projected.get_column("dataset_version_id").unique().to_list() == [
        version.dataset_version_id
    ]
    assert projected.get_column("source_provider_id").unique().to_list() == [
        "histdata.com"
    ]
    assert projected.get_column("origin").unique().to_list() == ["observed"]
    assert projected.get_column(
        "dataset_lineage_schema_version"
    ).unique().to_list() == [DATASET_TICK_PROJECTION_SCHEMA_VERSION]
    assert adapter.descriptor.projection_schema_version == (
        CANONICAL_TICK_PROJECTION_SCHEMA_VERSION
    )
    assert catalog.verify("latest-qualified").partition_count == 1
    resolution = catalog.resolve("latest-qualified")
    inventory = catalog.reconstruction_inventory(
        resolution,
        requested_start_ns=partition.coverage_start_ns,
        requested_end_ns=partition.coverage_end_ns,
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
    )
    assert (
        catalog.preflight_reconstruction_inventory(inventory).partition_count
        == 1
    )
    assert ProviderSourceInventoryV2.from_json(inventory.to_json()) == inventory


def test_histdata_adapter_preserves_and_flags_vendor_timestamp_regression(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    path, _ = _write_histdata_cache(source_root)
    regressed = pl.DataFrame(
        {
            "datetime": [_START_MS, _START_MS + 3_600_000, _START_MS + 1_000],
            "bid": [1.1000, 1.1001, 1.1002],
            "ask": [1.1002, 1.1003, 1.1004],
            "vol": [0, 0, 0],
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    )
    regressed.write_ipc(path)
    adapter = HistDataProviderAdapter()
    catalog, version = _catalog(adapter, source_root, tmp_path)
    partition = version.partitions[0]

    direct = adapter.read_partition(partition)
    projected = project_observed_ascii_ticks_v2(adapter, version, partition)

    assert adapter.descriptor.adapter_version == "1.1.0"
    assert direct.equals(regressed)
    assert partition.artifact.metadata["first_timestamp_ms"] == _START_MS
    assert partition.artifact.metadata["last_timestamp_ms"] == (
        _START_MS + 3_600_000
    )
    assert projected.get_column("source_row_id").to_list() == [1, 2, 3]
    assert projected.get_column(
        "dq_issue_non_monotonic_timestamp"
    ).to_list() == [
        False,
        False,
        True,
    ]
    assert catalog.verify("latest-qualified").partition_count == 1


@pytest.mark.parametrize(
    "timestamps",
    (
        (_START_MS, _START_MS + 3_600_001, _START_MS),
        (
            _START_MS,
            _START_MS + 3_600_000,
            _START_MS + 1_000,
            _START_MS + 3_600_001,
            _START_MS + 2_000,
        ),
    ),
)
def test_histdata_adapter_rejects_timestamp_regression_outside_vendor_bound(
    tmp_path: Path,
    timestamps: tuple[int, ...],
) -> None:
    source_root = tmp_path / "ASCII" / "T"
    path, _ = _write_histdata_cache(source_root)
    pl.DataFrame(
        {
            "datetime": timestamps,
            "bid": [
                1.1000 + index * 0.0001 for index in range(len(timestamps))
            ],
            "ask": [
                1.1002 + index * 0.0001 for index in range(len(timestamps))
            ],
            "vol": [0] * len(timestamps),
        },
        schema={
            "datetime": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "vol": pl.Int32,
        },
    ).write_ipc(path)

    with pytest.raises(DatasetContractError) as raised:
        HistDataProviderAdapter().inspect_partition(
            source_root,
            symbol=_SYMBOL,
            period=_PERIOD,
        )

    assert raised.value.code is DatasetFailureCode.INCONSISTENT_COVERAGE


def test_fixture_adapter_proves_different_clock_partition_and_provider_boundary(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "fixture"
    _write_fixture_csv(source_root)
    adapter = FixtureProviderAdapter()
    catalog, version = _catalog(
        adapter,
        source_root,
        tmp_path,
        dataset_id="fixture-observed-ticks",
    )
    partition = version.partitions[0]
    projected = project_observed_ascii_ticks_v2(adapter, version, partition)

    assert partition.source_provider_id == "fixture.reference"
    assert partition.clock_policy_id == "explicit-rfc3339-utc-v1"
    assert partition.partition_policy_id == "fixture-symbol-yyyy-mm-csv-v1"
    assert partition.series_id == "ascii:T:EURUSD:fixture.reference"
    assert projected.get_column("native_record_id").to_list() == [
        "native-1",
        "native-2",
    ]
    resolution = catalog.resolve("latest-qualified")
    inventory = catalog.reconstruction_inventory(
        resolution,
        requested_start_ns=partition.coverage_start_ns,
        requested_end_ns=partition.coverage_end_ns,
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
    )
    verified = catalog.preflight_reconstruction_inventory(inventory)
    assert verified.dataset_version_id == version.dataset_version_id
    assert inventory.to_dict()["source_provider_ids"] == ["fixture.reference"]


def test_identical_bytes_under_distinct_provider_and_license_do_not_collapse(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    first_path = _write_fixture_csv(first_root)
    second_root = tmp_path / "second"
    second_path = fixture_csv_path(second_root, _SYMBOL, _PERIOD)
    second_path.parent.mkdir(parents=True)
    shutil.copyfile(first_path, second_path)
    first_adapter = FixtureProviderAdapter(
        source_provider_id="provider.one",
        licensing_policy=DatasetLicensingPolicy.PUBLIC,
    )
    second_adapter = FixtureProviderAdapter(
        source_provider_id="provider.two",
        licensing_policy=DatasetLicensingPolicy.LOCAL_ONLY,
        redistribution_allowed=False,
    )
    first = build_observed_dataset_version(
        first_adapter,
        first_root,
        _descriptor("provider-one-ticks"),
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
        qualification_evidence=(_evidence(tmp_path),),
    )
    second = build_observed_dataset_version(
        second_adapter,
        second_root,
        _descriptor("provider-two-ticks"),
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
        qualification_evidence=(_evidence(tmp_path),),
    )

    assert (
        first.partitions[0].artifact.sha256
        == second.partitions[0].artifact.sha256
    )
    assert first.partitions[0].partition_id != second.partitions[0].partition_id
    assert first.dataset_version_id != second.dataset_version_id
    assert first.source_provider_ids == ("provider.one",)
    assert second.source_provider_ids == ("provider.two",)


def test_dataset_and_inventory_identity_ignore_local_artifact_relocation(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first" / "source"
    first_path = _write_fixture_csv(first_root)
    second_root = tmp_path / "second" / "source"
    second_path = fixture_csv_path(second_root, _SYMBOL, _PERIOD)
    second_path.parent.mkdir(parents=True)
    shutil.copyfile(first_path, second_path)
    adapter = FixtureProviderAdapter()
    descriptor = _descriptor("relocatable-observed-ticks")
    first = build_observed_dataset_version(
        adapter,
        first_root,
        descriptor,
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
        qualification_evidence=(_evidence(tmp_path / "first"),),
    )
    second_evidence = tmp_path / "second" / "qualification.json"
    second_evidence.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(tmp_path / "first" / "qualification.json", second_evidence)
    second = build_observed_dataset_version(
        adapter,
        second_root,
        descriptor,
        symbols=(_SYMBOL,),
        periods=(_PERIOD,),
        qualification_evidence=(
            artifact_ref_for_file(
                second_evidence, kind="dataset_qualification_v1"
            ),
        ),
    )

    assert first.dataset_version_id == second.dataset_version_id
    assert first.manifest_sha256 == second.manifest_sha256
    assert (
        first.to_dict()["partitions"][0]["artifact"]["path"]
        != second.to_dict()["partitions"][0]["artifact"]["path"]
    )
    first_partition = first.partitions[0]
    second_partition = second.partitions[0]
    first_inventory = ProviderSourceInventoryV2(
        dataset_id=first.dataset_id,
        dataset_version_id=first.dataset_version_id,
        manifest_sha256=first.manifest_sha256,
        requested_start_ns=first_partition.coverage_start_ns,
        requested_end_ns=first_partition.coverage_end_ns,
        partitions=(first_partition,),
    )
    second_inventory = ProviderSourceInventoryV2(
        dataset_id=second.dataset_id,
        dataset_version_id=second.dataset_version_id,
        manifest_sha256=second.manifest_sha256,
        requested_start_ns=second_partition.coverage_start_ns,
        requested_end_ns=second_partition.coverage_end_ns,
        partitions=(second_partition,),
    )
    assert first_inventory.inventory_id == second_inventory.inventory_id


def test_alias_resolution_replay_and_cursor_are_version_and_query_scoped(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ASCII" / "T"
    _write_histdata_cache(root)
    catalog, version = _catalog(HistDataProviderAdapter(), root, tmp_path)
    scope = DatasetQueryScopeV1(
        symbols=(_SYMBOL,), periods=(_PERIOD,), origin=DatasetOrigin.OBSERVED
    )
    receipt = catalog.resolve("latest-qualified", query_scope=scope)
    cursor = catalog.cursor(
        receipt,
        origin=DatasetOrigin.OBSERVED,
        series_id=version.partitions[0].series_id,
        period=_PERIOD,
        row_id=2,
    )

    assert catalog.replay(receipt) == receipt
    catalog.validate_cursor(cursor, receipt)
    moved_alias = replace(
        catalog.aliases[0],
        revision=2,
        alias_id="",
    )
    moved_catalog = replace(catalog, aliases=(moved_alias,), catalog_id="")
    assert moved_catalog.replay(receipt) == receipt
    with pytest.raises(DatasetContractError) as stale:
        moved_catalog.require_current_alias(receipt)
    assert stale.value.code is DatasetFailureCode.STALE_ALIAS

    other_scope = replace(scope, periods=(), scope_id="")
    other_receipt = catalog.resolve("latest-qualified", query_scope=other_scope)
    with pytest.raises(DatasetContractError) as mismatch:
        catalog.validate_cursor(cursor, other_receipt)
    assert mismatch.value.code is DatasetFailureCode.CURSOR_SCOPE_MISMATCH
    with pytest.raises(DatasetContractError) as wrong_origin:
        catalog.cursor(
            receipt,
            origin=DatasetOrigin.SYNTHETIC,
            ensemble_member_id="member:wrong-origin",
            event_id="event:wrong-origin",
        )
    assert wrong_origin.value.code is DatasetFailureCode.CURSOR_SCOPE_MISMATCH


def test_synthetic_lineage_keeps_parent_and_delivery_orthogonal_to_provider(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ASCII" / "T"
    _write_histdata_cache(root)
    _, observed = _catalog(HistDataProviderAdapter(), root, tmp_path)
    parent = DatasetParentV1(
        parent_dataset_version_id=observed.dataset_version_id,
        role="observed-base",
        ordinal=0,
    )
    derived = DatasetVersionManifestV1(
        dataset_id="synthetic-counterfactual",
        origin=DatasetOrigin.SYNTHETIC,
        normalization_policy_id="synthetic-event-v1-companion-lineage-v2",
        qualification_status=DatasetQualificationStatus.QUALIFIED,
        parents=(parent,),
        qualification_evidence=(_evidence(tmp_path),),
        delivery_profile_id="modern-reference-v1",
    )
    event = SyntheticEventV1.generated(
        symbol=_SYMBOL,
        event_time_ns=_START_MS * 1_000_000 + 500_000_000,
        event_sequence=1,
        bid=1.10005,
        ask=1.10025,
        run_id="run:fixture",
        ensemble_member_id="member:001",
        source_version_id=observed.dataset_version_id,
        left_anchor_event_id="event:left",
        right_anchor_event_id="event:right",
        generator_id="generator:fixture",
        generator_version="1.0.0",
        generator_config_id="generator-config:fixture",
        constraint_set_id="constraints:fixture",
    )

    lineage = synthetic_event_lineage_v2(event, derived)

    assert lineage.origin is DatasetOrigin.SYNTHETIC
    assert lineage.source_provider_id is None
    assert lineage.parent_dataset_version_ids == (observed.dataset_version_id,)
    assert lineage.delivery_profile_id == "modern-reference-v1"
    assert DatasetEventLineageV2.from_dict(lineage.to_dict()) == lineage
    with pytest.raises(ValueError, match="cannot claim a source provider"):
        replace(lineage, source_provider_id="synthetic", lineage_id="")
    unrelated_event = replace(
        event,
        source_version_id="dataset-version:sha256:" + "9" * 64,
        event_id="",
    )
    with pytest.raises(DatasetContractError) as missing_parent:
        synthetic_event_lineage_v2(unrelated_event, derived)
    assert missing_parent.value.code is DatasetFailureCode.IDENTITY_MISMATCH


def test_provider_inventory_rejects_incomplete_requested_interval(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ASCII" / "T"
    _write_histdata_cache(root)
    _, version = _catalog(HistDataProviderAdapter(), root, tmp_path)
    partition = version.partitions[0]

    with pytest.raises(DatasetContractError) as incomplete:
        ProviderSourceInventoryV2(
            dataset_id=version.dataset_id,
            dataset_version_id=version.dataset_version_id,
            manifest_sha256=version.manifest_sha256,
            requested_start_ns=partition.coverage_start_ns,
            requested_end_ns=partition.coverage_end_ns + 1,
            partitions=(partition,),
        )
    assert incomplete.value.code is DatasetFailureCode.INCONSISTENT_COVERAGE


def test_resolution_rejects_absent_symbol_period_cell_in_sparse_version(
    tmp_path: Path,
) -> None:
    root = tmp_path / "fixture"
    _write_fixture_csv(root, symbol="EURUSD", period="202001")
    _write_fixture_csv(root, symbol="GBPUSD", period="202002")
    adapter = FixtureProviderAdapter()
    descriptor = _descriptor("sparse-observed-ticks")
    version = DatasetVersionManifestV1(
        dataset_id=descriptor.dataset_id,
        origin=DatasetOrigin.OBSERVED,
        normalization_policy_id="fixture-sparse-matrix-v1",
        qualification_status=DatasetQualificationStatus.QUALIFIED,
        partitions=(
            adapter.inspect_partition(root, symbol="EURUSD", period="202001"),
            adapter.inspect_partition(root, symbol="GBPUSD", period="202002"),
        ),
        qualification_evidence=(_evidence(tmp_path),),
    )
    catalog = DatasetCatalog(
        providers=(adapter.provider,),
        adapters=(adapter.descriptor,),
        datasets=(descriptor,),
        versions=(version,),
    )

    with pytest.raises(DatasetContractError) as missing_cell:
        catalog.resolve(
            version.dataset_version_id,
            query_scope=DatasetQueryScopeV1(
                symbols=("EURUSD",), periods=("202002",)
            ),
        )
    assert missing_cell.value.code is DatasetFailureCode.INCONSISTENT_COVERAGE


def test_composed_dataset_identity_is_deterministic_and_parent_order_independent(
    tmp_path: Path,
) -> None:
    parents = (
        DatasetParentV1(
            parent_dataset_version_id="dataset-version:sha256:" + "1" * 64,
            role="left-provider",
            ordinal=0,
        ),
        DatasetParentV1(
            parent_dataset_version_id="dataset-version:sha256:" + "2" * 64,
            role="right-provider",
            ordinal=1,
        ),
    )
    first = DatasetVersionManifestV1(
        dataset_id="composed-observed-view",
        origin=DatasetOrigin.COMPOSED,
        normalization_policy_id="explicit-composition-no-dedup-v1",
        qualification_status=DatasetQualificationStatus.QUALIFIED,
        parents=parents,
        qualification_evidence=(_evidence(tmp_path),),
    )
    second = DatasetVersionManifestV1(
        dataset_id="composed-observed-view",
        origin=DatasetOrigin.COMPOSED,
        normalization_policy_id="explicit-composition-no-dedup-v1",
        qualification_status=DatasetQualificationStatus.QUALIFIED,
        parents=tuple(reversed(parents)),
        qualification_evidence=(_evidence(tmp_path),),
    )

    assert first == second
    assert DatasetVersionManifestV1.from_dict(first.to_dict()) == first


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ("ambiguous-clock", DatasetFailureCode.AMBIGUOUS_CLOCK),
        ("malformed-quote", DatasetFailureCode.MALFORMED_QUOTE),
        ("missing-hash", DatasetFailureCode.MISSING_HASH),
        ("inconsistent-coverage", DatasetFailureCode.INCONSISTENT_COVERAGE),
    ],
)
def test_provider_failures_use_stable_reason_codes(
    tmp_path: Path,
    mutation: str,
    code: DatasetFailureCode,
) -> None:
    root = tmp_path / mutation
    path = _write_fixture_csv(root, ambiguous=mutation == "ambiguous-clock")
    if mutation == "malformed-quote":
        path.write_text(
            "timestamp,bid,ask\n2020-01-02T00:00:00Z,nope,1.1\n",
            encoding="utf-8",
        )
    adapter = FixtureProviderAdapter()
    with pytest.raises(DatasetContractError) as raised:
        if mutation == "missing-hash":
            adapter.inspect_partition(
                root, symbol=_SYMBOL, period=_PERIOD, expected_sha256="missing"
            )
        elif mutation == "inconsistent-coverage":
            adapter.discover(
                root, symbols=(_SYMBOL, "GBPUSD"), periods=(_PERIOD,)
            )
        else:
            adapter.inspect_partition(root, symbol=_SYMBOL, period=_PERIOD)
    assert raised.value.code is code


def test_contracts_fail_closed_for_invalid_dimensions_license_and_secrets(
    tmp_path: Path,
) -> None:
    with pytest.raises(DatasetContractError) as symbol:
        fixture_csv_path(tmp_path, "EURUS7", _PERIOD)
    assert symbol.value.code is DatasetFailureCode.INVALID_SYMBOL
    with pytest.raises(DatasetContractError) as period:
        fixture_csv_path(tmp_path, _SYMBOL, "202013")
    assert period.value.code is DatasetFailureCode.INVALID_PERIOD
    artifact_path = tmp_path / "partition.data"
    artifact_path.write_bytes(b"partition")
    artifact = artifact_ref_for_file(
        artifact_path, kind="provider_ascii_tick_partition_v2"
    )
    partition_kwargs = {
        "source_provider_id": "fixture.reference",
        "adapter_id": "fixture-adapter",
        "adapter_version": "1.0.0",
        "symbol": _SYMBOL,
        "period": _PERIOD,
        "artifact": artifact,
        "source_artifact_sha256": artifact.sha256,
        "row_count": 1,
        "coverage_start_ns": 1,
        "coverage_end_ns": 2,
        "clock_policy_id": "explicit-utc-v1",
        "partition_policy_id": "fixture-v1",
        "row_identity_policy_id": "source-row-v1",
        "licensing_policy": DatasetLicensingPolicy.PUBLIC,
    }
    with pytest.raises(DatasetContractError) as data_format:
        CanonicalObservedPartitionV2(**partition_kwargs, format="binary")
    assert data_format.value.code is DatasetFailureCode.UNSUPPORTED_FORMAT
    with pytest.raises(DatasetContractError) as timeframe:
        CanonicalObservedPartitionV2(**partition_kwargs, granularity="M1")
    assert timeframe.value.code is DatasetFailureCode.UNSUPPORTED_TIMEFRAME
    with pytest.raises(DatasetContractError) as license_error:
        SourceProviderDescriptorV1(
            source_provider_id="unknown.provider",
            display_name="Unknown",
            attribution="Unknown license fixture",
            licensing_policy=DatasetLicensingPolicy.UNKNOWN,
            redistribution_allowed=False,
        )
    assert license_error.value.code is (
        DatasetFailureCode.UNSUPPORTED_LICENSING_POLICY
    )

    path = tmp_path / "secret.json"
    path.write_text("{}", encoding="utf-8")
    secret_ref = artifact_ref_for_file(
        path, kind="qualification", metadata={"token": "do-not-store"}
    )
    with pytest.raises(DatasetContractError) as secret:
        DatasetVersionManifestV1(
            dataset_id="secret-fixture",
            origin=DatasetOrigin.SYNTHETIC,
            normalization_policy_id="fixture-v1",
            qualification_status=DatasetQualificationStatus.QUALIFIED,
            parents=(
                DatasetParentV1(
                    parent_dataset_version_id=(
                        "dataset-version:sha256:" + "1" * 64
                    ),
                    role="fixture-parent",
                    ordinal=0,
                ),
            ),
            qualification_evidence=(secret_ref,),
        )
    assert secret.value.code is DatasetFailureCode.SECRET_MATERIAL


def test_catalog_round_trip_cli_and_receipt_replay_are_installed_surfaces(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "ASCII" / "T"
    _write_histdata_cache(root)
    catalog, version = _catalog(HistDataProviderAdapter(), root, tmp_path)
    catalog_path = catalog.write(tmp_path / "catalog.json")
    receipt_path = tmp_path / "receipt.json"

    assert DatasetCatalog.read(catalog_path) == catalog
    assert dataset_cli_main(["--catalog", str(catalog_path), "list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["datasets"][0]["dataset_id"] == version.dataset_id
    assert (
        dataset_cli_main(
            [
                "--catalog",
                str(catalog_path),
                "describe",
                "latest-qualified",
            ]
        )
        == 0
    )
    described = json.loads(capsys.readouterr().out)
    assert described["kind"] == "dataset_alias"
    assert (
        dataset_cli_main(
            ["--catalog", str(catalog_path), "verify", "latest-qualified"]
        )
        == 0
    )
    verified_payload = json.loads(capsys.readouterr().out)
    assert verified_payload["status"] == "verified"
    assert (
        DatasetVerificationV1.from_dict(verified_payload).partition_count == 1
    )

    assert (
        dataset_cli_main(
            [
                "--catalog",
                str(catalog_path),
                "resolve",
                "latest-qualified",
                "--symbol",
                _SYMBOL,
                "--period",
                _PERIOD,
                "--receipt",
                str(receipt_path),
            ]
        )
        == 0
    )
    resolved = json.loads(capsys.readouterr().out)
    assert resolved["dataset_version_id"] == version.dataset_version_id
    assert read_resolution_receipt(receipt_path).dataset_version_id == (
        version.dataset_version_id
    )

    assert (
        dataset_cli_main(
            ["--catalog", str(catalog_path), "replay", str(receipt_path)]
        )
        == 0
    )
    replayed = json.loads(capsys.readouterr().out)
    assert not replayed["alias_re_resolved"]
    assert replayed["verification"]["status"] == "verified"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "datasets",
            "--catalog",
            str(catalog_path),
            "verify",
            "latest-qualified",
        ],
    )
    assert histdatacom_main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "verified"


@given(
    st.lists(
        st.sampled_from(["EURUSD", "GBPUSD", "EURGBP"]),
        min_size=1,
        max_size=12,
    )
)
def test_query_scope_identity_is_order_and_duplicate_independent(
    symbols: list[str],
) -> None:
    first = DatasetQueryScopeV1(symbols=tuple(symbols), periods=(_PERIOD,))
    second = DatasetQueryScopeV1(
        symbols=tuple(reversed(symbols + symbols)), periods=(_PERIOD, _PERIOD)
    )
    assert first == second
    assert DatasetQueryScopeV1.from_dict(first.to_dict()) == first

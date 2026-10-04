"""Real persisted anchors: native aliases are byte-bound, never caller maps."""

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from histdatacom.data_quality.training_contracts import TrainingSourceV1
from histdatacom.data_quality.training_lineage import verify_training_source
from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    build_observed_dataset_version,
)
from histdatacom.datasets.adapters import FixtureProviderAdapter
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)
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
    read_reconstruction_streams,
    stage_delivery_reconstruction_publication,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)
from tests.fixtures.training_substrate_v1 import (
    QUOTES,
    SYMBOLS,
    TIMES,
    observed_source,
    published_product,
    with_products,
)

pytest_plugins = ("tests.fixtures.campaign_verification",)


def _native_series(partition):
    return (
        f"ascii-tick:{partition.symbol.lower()}:{partition.period}:"
        f"sha256:{partition.artifact.sha256}"
    )


def _foreign_source(root):
    """Actual non-HistData adapter, CSV bytes and native observed catalog."""
    adapter = FixtureProviderAdapter()
    for symbol in SYMBOLS:
        path = root / symbol / "2020-01.csv"
        path.parent.mkdir(parents=True)
        bid, ask = QUOTES[symbol]
        rows = ["timestamp,bid,ask,vol"]
        for time in TIMES:
            timestamp = datetime.fromtimestamp(
                time // 1_000_000_000, timezone.utc
            ).isoformat()
            rows.append(f"{timestamp},{bid!r},{ask!r},0")
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    evidence = root / "fixture.json"
    evidence.write_text('{"synthetic_fixture":true}', encoding="utf-8")
    descriptor = DatasetDescriptorV1(
        "native-anchor-foreign-fixture",
        "Foreign fixture",
        "Invented quotes, not a market feed or empirical qualification.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=("202001",),
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


def _product(root, version, *, violation=None, duplicate=None):
    """Ordinary V2 writer/readback of explicit synthetic anchor input vectors.

    No successful verification is patched. A deliberately wrong source claim
    can satisfy native physical persistence yet must fail training admission.
    """
    config = eurusd_triangle_reconciliation_config()
    member = "native-anchor-fixture"
    run = ReconstructionRunV1(
        SYMBOLS,
        (version.dataset_version_id,),
        (config.config_id,),
        (member,),
        774,
    )
    window = ReconstructionWindowV1(
        run.run_id, member, SYMBOLS, TIMES[0], TIMES[1] + 2
    )
    streams = []
    for symbol in SYMBOLS:
        partition = next(p for p in version.partitions if p.symbol == symbol)
        bid, ask = QUOTES[symbol]
        anchors = []
        for index in (0, 1):
            fields = {
                "symbol": symbol,
                "event_time_ns": TIMES[index],
                "event_sequence": 0,
                "bid": bid,
                "ask": ask,
                "run_id": run.run_id,
                "ensemble_member_id": member,
                "source_version_id": version.dataset_version_id,
                "source_series_id": _native_series(partition),
                "source_period": partition.period,
                "source_row_id": index + 1,
            }
            if symbol == "EURUSD" and index == 0:
                if violation == "digest":
                    fields["source_series_id"] = (
                        "ascii-tick:eurusd:202001:sha256:" + "0" * 64
                    )
                elif violation == "symbol":
                    fields["source_series_id"] = _native_series(
                        partition
                    ).replace(":eurusd:", ":usdjpy:")
                elif violation == "uppercase":
                    fields["source_series_id"] = _native_series(
                        partition
                    ).replace(":eurusd:", ":EURUSD:")
                elif violation == "mixed_case":
                    fields["source_series_id"] = _native_series(
                        partition
                    ).replace(":eurusd:", ":EURusd:")
                elif violation == "alias_period":
                    fields["source_series_id"] = _native_series(
                        partition
                    ).replace(":202001:", ":202002:")
                elif violation == "period":
                    fields["source_period"] = "202002"
                elif violation == "ordinal":
                    fields["source_row_id"] = partition.row_count + 1
                elif violation == "other_row":
                    fields["source_row_id"] = 2
                elif violation == "time":
                    fields["event_time_ns"] += 1
                elif violation == "bid":
                    fields["bid"] += 0.000001
                elif violation == "ask":
                    fields["ask"] += 0.000001
                elif violation == "source_version":
                    fields["source_version_id"] = "foreign-source-version"
                if duplicate == "canonical_first":
                    fields["source_series_id"] = partition.series_id
            anchor = SyntheticEventV1.observed(**fields)
            anchors.append(anchor)
            if symbol == "EURUSD" and index == 0 and duplicate is not None:
                # Same physical source row and quote, different event identity.
                fields["event_sequence"] = 1
                fields["source_series_id"] = (
                    partition.series_id
                    if duplicate == "native_first"
                    else _native_series(partition)
                )
                repeated = SyntheticEventV1.observed(**fields)
                assert repeated.event_id != anchor.event_id
                anchors.append(repeated)
        streams.append(
            SyntheticEventStreamV1(run.run_id, member, symbol, tuple(anchors))
        )
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams={stream.symbol: stream for stream in streams},
        config=config,
    )
    delivered = project_modern_reference_delivery(
        group, delivery_profile_id="modern-reference:native-anchor-fixture"
    )
    anchors = tuple(event for s in delivered.streams for event in s.events)
    validation = validate_cross_currency_output(
        run=run,
        window=window,
        streams={stream.symbol: stream for stream in delivered.streams},
        config=config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=anchors,
    )
    assert validation.passed
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=member,
        retained_member_event_counts={member: len(anchors)},
        estimated_partition_count=6,
        storage_policy=run.storage_policy,
    )
    staged = stage_delivery_reconstruction_publication(
        root / "archive",
        delivered,
        final_validation=validation,
        benchmark_artifact_ids=("fixture:unqualified",),
        benchmark_evidence={"scope": "synthetic-anchor-fixture-only"},
        immutable_source_anchors=anchors,
        symbol_group_id=window.synchronization_unit_id,
        retention_plan=retention,
        storage_policy=run.storage_policy,
        staging_root=root / "scratch",
    )
    product = commit_delivery_reconstruction_publication(staged)
    actual = read_reconstruction_streams(product.manifest_path)
    assert tuple(s.to_dict() for s in actual) == tuple(
        s.to_dict() for s in delivered.streams
    )
    return product


def test_genuine_native_campaign_aliases_preserve_catalog_and_products(
    native_campaign,
):
    catalog_path = Path(
        native_campaign.plan.artifact_graph["dataset_catalog"].path
    )
    catalog_bytes = catalog_path.read_bytes()
    manifest_bytes = tuple(
        p.read_bytes() for p in native_campaign.manifest_paths
    )
    source = TrainingSourceV1(
        catalog_bytes.decode(),
        native_campaign.plan.run.source_version_ids[0],
        product_manifest_paths=tuple(
            sorted(str(path) for path in native_campaign.manifest_paths)
        ),
    )
    verified = verify_training_source(source)
    assert len(verified.products) == 2
    events = tuple(event for p in verified.products for event in p.events)
    assert len(events) == 18
    anchors = tuple(
        event for event in events if event.origin.value == "observed"
    )
    assert len(anchors) == 12
    assert all(
        event.source_series_id.startswith("ascii-tick:") for event in anchors
    )
    assert {event.source_series_id.split(":")[1] for event in anchors} == {
        "eurgbp",
        "eurusd",
        "gbpusd",
    }
    assert all(
        row.series_id.startswith("ascii:T:") for row in verified.observed
    )
    assert catalog_path.read_bytes() == catalog_bytes
    assert (
        tuple(p.read_bytes() for p in native_campaign.manifest_paths)
        == manifest_bytes
    )


def test_canonical_keys_remain_unchanged_and_native_aliases_match(tmp_path):
    source, version = observed_source(tmp_path)
    canonical, _ = published_product(tmp_path / "canonical", version)
    native = _product(tmp_path / "native", version)
    original = source.catalog_json
    plain = verify_training_source(source)
    first = verify_training_source(with_products(source, canonical))
    second = verify_training_source(with_products(source, native))
    assert plain.observed == first.observed == second.observed
    assert source.catalog_json == original
    assert all(row.series_id.startswith("ascii:T:") for row in second.observed)


@pytest.mark.parametrize(
    "violation,reason",
    (
        ("digest", "no exact source row"),
        ("symbol", "no exact source row"),
        ("uppercase", "no exact source row"),
        ("mixed_case", "no exact source row"),
        ("alias_period", "no exact source row"),
        ("period", "no exact source row"),
        ("ordinal", "no exact source row"),
        ("other_row", "differs from source values"),
        ("time", "differs from source values"),
        ("bid", "differs from source values"),
        ("ask", "differs from source values"),
        ("source_version", "product parent is not the verified"),
    ),
)
def test_native_alias_does_not_relax_exact_anchor_checks(
    tmp_path, violation, reason
):
    source, version = observed_source(tmp_path)
    product = _product(tmp_path / "product", version, violation=violation)
    with pytest.raises(ValueError, match=reason):
        verify_training_source(with_products(source, product))


@pytest.mark.parametrize(
    "duplicate", ("canonical_first", "native_first", "native")
)
def test_aliases_cannot_repeat_a_semantic_anchor_with_distinct_event_ids(
    tmp_path, duplicate
):
    source, version = observed_source(tmp_path)
    product = _product(tmp_path / "product", version, duplicate=duplicate)
    with pytest.raises(ValueError, match="repeats an observed anchor"):
        verify_training_source(with_products(source, product))


def test_non_histdata_provider_cannot_acquire_native_alias_authority(tmp_path):
    source, version = _foreign_source(tmp_path / "foreign")
    canonical, _ = published_product(tmp_path / "canonical", version)
    assert verify_training_source(with_products(source, canonical)).products
    product = _product(tmp_path / "native", version)
    with pytest.raises(ValueError, match="no exact source row"):
        verify_training_source(with_products(source, product))


def test_canonical_series_cannot_shadow_another_native_partition(tmp_path):
    source, version = observed_source(tmp_path / "histdata")
    foreign_source, foreign_version = _foreign_source(tmp_path / "foreign")
    histdata = next(p for p in version.partitions if p.symbol == "EURUSD")
    foreign = next(
        p for p in foreign_version.partitions if p.symbol == "GBPUSD"
    )
    foreign = replace(
        foreign, series_id=_native_series(histdata), partition_id=""
    )
    mixed = replace(
        version,
        partitions=(histdata, foreign),
        dataset_version_id="",
        manifest_sha256="",
    )
    catalog = DatasetCatalog.from_json(source.catalog_json)
    foreign_catalog = DatasetCatalog.from_json(foreign_source.catalog_json)
    mixed_catalog = DatasetCatalog(
        catalog.providers + foreign_catalog.providers,
        catalog.adapters + foreign_catalog.adapters,
        catalog.datasets,
        (mixed,),
    )
    with pytest.raises(ValueError, match="alias collides with a canonical"):
        verify_training_source(
            TrainingSourceV1(mixed_catalog.to_json(), mixed.dataset_version_id)
        )


def test_native_alias_still_requires_current_source_bytes(tmp_path):
    source, version = observed_source(tmp_path)
    product = _product(tmp_path / "product", version)
    path = Path(version.partitions[0].artifact.path)
    original = path.read_bytes()
    path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
    with pytest.raises(ValueError, match="hash|mismatch"):
        verify_training_source(with_products(source, product))


def test_caller_cannot_supply_an_alias_map(tmp_path):
    source, _ = observed_source(tmp_path)
    data = source.to_dict()
    data["native_series_aliases"] = {"caller": "alias"}
    with pytest.raises(ValueError, match="field|unknown"):
        TrainingSourceV1.from_dict(data)

"""Independent accounting over genuine generated native campaign products.

These tests do not claim historical/model qualification. Receipt parsing is
structural; only the real traversal grants current product-integrity evidence.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from histdatacom.campaign_receipt_contracts import (
    CampaignReceiptRefV1,
    CampaignVerificationSummaryV1,
)

pytest_plugins = ("tests.fixtures.campaign_receipt_tree",)


def _helpers() -> Any:
    # Lazy fixture import avoids importing a pytest plugin before registration.
    from tests.fixtures import campaign_receipt_tree

    return campaign_receipt_tree


def _events_hash(events: tuple[Any, ...], header: bytes) -> str:
    digest = hashlib.sha256(header)
    for event in sorted(
        events,
        key=lambda row: (
            row.symbol,
            row.event_time_ns,
            row.event_sequence,
            row.event_id,
        ),
    ):
        digest.update(_helpers().canonical_bytes(event.to_dict()) + b"\n")
    return digest.hexdigest()


def _source_path(manifest_path: Path, segment: Any) -> Path:
    path = Path(segment.source_artifact.path)
    if path.is_absolute():
        return path
    bundle = next(
        parent
        for parent in manifest_path.parents
        if parent.name == "reconstruction-products"
    )
    return bundle / path


def _product_rows(store: Path, root: Any) -> tuple[Any, ...]:
    return _helpers().tree_products(store, root)


def _input_digest(files: tuple[Any, ...]) -> str:
    digest = hashlib.sha256(b"histdatacom.campaign-inputs.v1\n")
    for item in files:
        digest.update(
            _helpers().canonical_bytes(
                {
                    "path": item.path,
                    "sha256": item.sha256,
                    "size_bytes": item.size_bytes,
                }
            )
            + b"\n"
        )
    return digest.hexdigest()


def test_real_tree_has_exact_two_member_two_shard_independent_accounting(
    receipt_campaign: dict[str, Any],
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import ipc

    from histdatacom.campaign_receipt_store import (
        read_campaign_verification_tree,
    )
    from histdatacom.synthetic import persistence

    fixture = receipt_campaign
    store, root = fixture["store"], fixture["root"]
    native = fixture["native"]
    assert (
        read_campaign_verification_tree(
            store, expected_root_id=root.artifact_id
        )
        == root
    )
    assert root.product_count == 2
    assert root.products_per_shard == 1
    assert len(root.shards) == 2
    assert root.initial_inventory_sha256 == root.final_inventory_sha256
    controls = tuple(
        _helpers().read_receipt(store, ref)
        for ref in (*root.global_controls, *root.plan_controls)
    )
    assert root.global_controls
    assert len(root.plan_controls) == 1
    assert tuple(control.ordinal for control in controls) == tuple(
        range(len(controls))
    )
    assert controls[-1].scope == "plan"
    assert controls[-1].plan_id == native.plan.plan_id
    for control in controls:
        assert control.run_id == root.run_id
        assert control.inputs_sha256 == _input_digest(control.input_files)
        for item in control.input_files:
            raw = Path(item.path).read_bytes()
            assert len(raw) == item.size_bytes
            assert hashlib.sha256(raw).hexdigest() == item.sha256
    rows = _product_rows(store, root)
    assert tuple(item.ordinal for item in rows) == (0, 1)
    expected = {
        (
            str(path.resolve()),
            product.manifest.run_id,
            product.manifest.window_id,
            product.manifest.ensemble_member_id,
        )
        for path, product in zip(
            native.manifest_paths, native.products, strict=True
        )
    }
    actual = {
        (
            row.verification.product_ref.path,
            row.verification.run_id,
            row.verification.window_id,
            row.verification.ensemble_member_id,
        )
        for row in rows
    }
    assert actual == expected
    observed_total = synthetic_total = 0
    for ordinal, (shard_ref, row) in enumerate(
        zip(root.shards, rows, strict=True)
    ):
        shard = _helpers().read_receipt(store, shard_ref)
        assert shard.ordinal == shard.first_product_ordinal == ordinal
        assert shard.products_per_shard == 1
        assert len(shard.products) == 1
        assert shard.run_id == row.run_id == root.run_id
        core = row.verification
        path = Path(core.product_ref.path)
        manifest = persistence.verify_reconstruction_publication(path)
        streams = persistence.read_reconstruction_streams(path)
        events = tuple(event for stream in streams for event in stream.events)
        observed = tuple(e for e in events if e.origin.value == "observed")
        synthetic = tuple(e for e in events if e.origin.value == "synthetic")
        assert len(events) == 9
        assert len(observed) == core.observed_event_count == 6
        assert len(synthetic) == core.synthetic_event_count == 3
        assert shard.observed_event_count == 6
        assert shard.synthetic_event_count == 3
        assert core.manifest_id == manifest.manifest_id
        lineage = json.loads(row.lineage_json)
        assert lineage == {
            "publication_id": manifest.publication_id,
            "source": manifest.source.to_dict(),
            "quality": manifest.quality.to_dict(),
            "replay": manifest.replay.to_dict(),
            "ensemble": manifest.ensemble.to_dict(),
            "constraints": manifest.constraints.to_dict(),
            "runtime_scope": json.loads(core.runtime_scope_json),
            "scope": "native_final_validation_not_producer_replay",
        }
        assert core.logical_content_sha256 == _events_hash(
            events, b"sha256-canonical-event-json-lines-v1\n"
        )
        assert core.observed_content_sha256 == _events_hash(
            observed, b"histdatacom-observed-anchors-v1\n"
        )
        final_validation = json.loads(core.final_validation_json)
        assert final_validation["status"] == "passed"
        assert final_validation["failure_reasons"] == []
        files = {item.path: item for item in row.input_files}
        assert len(files) == len(row.input_files)
        for filename, item in files.items():
            raw = Path(filename).read_bytes()
            assert item.size_bytes == len(raw)
            assert item.sha256 == hashlib.sha256(raw).hexdigest()
        assert row.read_bytes >= sum(item.size_bytes for item in files.values())
        assert row.elapsed_ns >= 0
        expected_parquet = tuple(
            sorted(
                str((path.parent / part.relative_path).resolve())
                for part in manifest.partitions
            )
        )
        assert row.parquet_paths == expected_parquet
        assert set(row.parquet_paths) == {
            str(item.resolve()) for item in path.parent.rglob("*.parquet")
        }
        delta_count = 0
        for filename in row.parquet_paths:
            parquet = pq.ParquetFile(filename)
            for batch in parquet.iter_batches(batch_size=2):
                raw_rows = batch.to_pylist()
                assert all(e["origin"] == "synthetic" for e in raw_rows)
                delta_count += len(raw_rows)
        assert delta_count == 3
        # Independently read exact native source row ranges, not claimed counts.
        source_quotes = []
        for segment in manifest.observed_anchor_segments:
            source = _source_path(path, segment)
            assert str(source.resolve()) in files
            with pa.memory_map(str(source), "r") as mapped:
                table = ipc.open_file(mapped).read_all()
            subset = table.slice(
                segment.source_row_start_id - 1, segment.row_count
            ).to_pylist()
            source_quotes.extend(
                (
                    segment.symbol,
                    row["datetime"] * 1_000_000,
                    row["bid"],
                    row["ask"],
                )
                for row in subset
            )
        assert sorted(source_quotes) == sorted(
            (event.symbol, event.event_time_ns, event.bid, event.ask)
            for event in observed
        )
        observed_total += len(observed)
        synthetic_total += len(synthetic)
    summary = CampaignVerificationSummaryV1.from_json(root.summary_json)
    assert (observed_total, synthetic_total) == (12, 6)
    assert (summary.observed_event_count, summary.synthetic_event_count) == (
        12,
        6,
    )
    assert summary.product_count == 2
    assert summary.missing_product_count == 0
    assert summary.out_of_plan_json == "[]"
    assert summary.status == "complete"
    run = next(
        record
        for _, record in _helpers().stored_records(store, "run")
        if record.artifact_id == root.run_id
    )
    assert run.index_id == summary.index_id
    assert (
        run.index_sha256
        == hashlib.sha256(
            Path(fixture["index_ref"].path).read_bytes()
        ).hexdigest()
    )
    assert run.products_per_shard == 1
    assert (
        run.authority == "historical_execution_only_current_integrity_required"
    )
    assert json.loads(run.environment_json)


def test_sample_is_exact_partial_current_evidence_not_a_new_full_root(
    receipt_campaign: dict[str, Any],
) -> None:
    from histdatacom.campaign_receipt_runner import (
        audit_campaign_verification_tree,
    )

    fixture = receipt_campaign
    store, root = fixture["store"], fixture["root"]
    roots_before = tuple((store / "root").glob("*.json"))
    sample = audit_campaign_verification_tree(
        fixture["index_ref"].path,
        store,
        expected_root_id=root.artifact_id,
        product_ordinals=(1,),
    )
    assert sample.root_id == root.artifact_id
    assert sample.product_ordinals == (1,)
    assert (
        sample.authority
        == "sampled_products_only_not_full_campaign_verification"
    )
    assert len(sample.products) == 1
    row = _helpers().read_receipt(store, sample.products[0])
    assert row.ordinal == 1
    assert row.verification == _product_rows(store, root)[1].verification
    assert tuple((store / "root").glob("*.json")) == roots_before


@pytest.mark.parametrize("kind", ("parquet", "source", "manifest", "index"))
def test_same_size_restored_mtime_tamper_cannot_use_retained_root(
    receipt_campaign: dict[str, Any],
    kind: str,
) -> None:
    from histdatacom.campaign_receipt_runner import (
        audit_campaign_verification_tree,
    )
    from histdatacom.campaign_receipt_store import (
        read_campaign_verification_tree,
    )
    from histdatacom.synthetic import persistence

    fixture = receipt_campaign
    store, root = fixture["store"], fixture["root"]
    row = _product_rows(store, root)[0]
    manifest_path = Path(row.verification.product_ref.path)
    manifest = persistence.verify_reconstruction_publication(manifest_path)
    paths = {
        "parquet": Path(row.parquet_paths[0]),
        "source": _source_path(
            manifest_path, manifest.observed_anchor_segments[0]
        ),
        "manifest": manifest_path,
        "index": Path(fixture["index_ref"].path),
    }
    path = paths[kind]
    original = path.stat()
    with _helpers().altered_file(path, _helpers().flipped_bytes(path)):
        assert path.stat().st_size == original.st_size
        assert path.stat().st_mtime_ns == original.st_mtime_ns
        # A structural read remains historical, intentionally not a re-hash.
        assert (
            read_campaign_verification_tree(
                store, expected_root_id=root.artifact_id
            )
            == root
        )
        with pytest.raises((ValueError, OSError, RuntimeError)):
            audit_campaign_verification_tree(
                fixture["index_ref"].path,
                store,
                expected_root_id=root.artifact_id,
                product_ordinals=(0,),
            )


@pytest.mark.parametrize("kind", ("parquet", "manifest", "source"))
def test_missing_native_input_never_becomes_success_on_resume(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
    kind: str,
) -> None:
    from histdatacom.campaign_receipt_runner import resume_campaign_verification
    from histdatacom.synthetic import persistence

    fixture = receipt_campaign
    store = tmp_path / "retained-store"
    _helpers().clone_retained_store(fixture["store"], store, fixture["root"])
    row = _product_rows(store, fixture["root"])[0]
    manifest_path = Path(row.verification.product_ref.path)
    manifest = persistence.verify_reconstruction_publication(manifest_path)
    path = {
        "parquet": Path(row.parquet_paths[0]),
        "manifest": manifest_path,
        "source": _source_path(
            manifest_path, manifest.observed_anchor_segments[0]
        ),
    }[kind]
    roots_before = set((store / "root").glob("*.json"))
    held = tmp_path / "temporarily-missing"
    path.rename(held)
    try:
        with pytest.raises((ValueError, OSError, RuntimeError)):
            resume_campaign_verification(
                fixture["index_ref"].path, store_directory=store
            )
        assert set((store / "root").glob("*.json")) == roots_before
    finally:
        held.rename(path)


@pytest.mark.parametrize("kind", ("unreferenced_parquet", "duplicate_product"))
def test_real_extra_native_file_or_product_invalidates_full_inventory(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
    kind: str,
) -> None:
    from histdatacom.campaign_receipt_runner import resume_campaign_verification

    fixture = receipt_campaign
    store = tmp_path / "retained-store"
    _helpers().clone_retained_store(fixture["store"], store, fixture["root"])
    row = _product_rows(store, fixture["root"])[0]
    directory = Path(row.verification.product_ref.path).parent
    if kind == "unreferenced_parquet":
        added = directory / "unreferenced.parquet"
        assert not added.exists()
        shutil.copyfile(row.parquet_paths[0], added)
    else:
        added = directory.parent / "unreferenced-product"
        assert not added.exists()
        shutil.copytree(directory, added)
    roots_before = set((store / "root").glob("*.json"))
    try:
        with pytest.raises((ValueError, OSError, RuntimeError)):
            resume_campaign_verification(
                fixture["index_ref"].path, store_directory=store
            )
        assert set((store / "root").glob("*.json")) == roots_before
    finally:
        if kind == "unreferenced_parquet":
            added.unlink()
        else:
            shutil.rmtree(added)


def _persist_adversarial_record(
    store: Path, record: Any
) -> CampaignReceiptRefV1:
    """Write forged but internally content-addressed test input, not proof."""
    raw = record.to_json().encode("utf-8")
    ref = CampaignReceiptRefV1(
        record.KIND,
        record.artifact_id,
        len(raw),
        hashlib.sha256(raw).hexdigest(),
    )
    path = _helpers().receipt_path(store, ref)
    path.parent.mkdir(parents=True, exist_ok=True)
    assert not path.exists()
    path.write_bytes(raw)
    return ref


@pytest.mark.parametrize("mutation", ("reverse", "count", "ordinal", "run"))
def test_resealed_tree_cannot_hide_child_order_counts_or_foreign_run(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
    mutation: str,
) -> None:
    from histdatacom.campaign_receipt_store import (
        read_campaign_verification_tree,
    )

    fixture = receipt_campaign
    store = tmp_path / "forged-store"
    _helpers().clone_retained_store(fixture["store"], store, fixture["root"])
    root = fixture["root"]
    shards = list(root.shards)
    if mutation == "reverse":
        shards.reverse()
    else:
        shard = _helpers().read_receipt(store, shards[0])
        if mutation == "count":
            shard = replace(
                shard, observed_event_count=shard.observed_event_count + 1
            )
        else:
            product = _helpers().read_receipt(store, shard.products[0])
            product = replace(
                product,
                **(
                    {"ordinal": 1}
                    if mutation == "ordinal"
                    else {"run_id": "campaign-receipt-run:sha256:" + "f" * 64}
                ),
            )
            shard = replace(
                shard, products=(_persist_adversarial_record(store, product),)
            )
        shards[0] = _persist_adversarial_record(store, shard)
    forged = replace(root, shards=tuple(shards))
    forged_ref = _persist_adversarial_record(store, forged)
    from histdatacom.campaign_receipt_contracts import (
        CampaignVerificationJournalV1,
    )

    journal_path, journal = max(
        _helpers().stored_records(store, "journal"),
        key=lambda pair: pair[1].ordinal,
    )
    raw_journal = journal_path.read_bytes()
    previous = CampaignReceiptRefV1(
        "journal",
        journal.artifact_id,
        len(raw_journal),
        hashlib.sha256(raw_journal).hexdigest(),
    )
    ordinal = journal.ordinal + 1

    def append(
        event: str, subject: Any, product_ordinal: int | None = None
    ) -> None:
        nonlocal ordinal, previous
        previous = _persist_adversarial_record(
            store,
            CampaignVerificationJournalV1(
                root.run_id,
                ordinal,
                previous,
                event,
                subject,
                product_ordinal,
            ),
        )
        ordinal += 1

    run_path, run = next(
        (path, run)
        for path, run in _helpers().stored_records(store, "run")
        if run.artifact_id == root.run_id
    )
    raw_run = run_path.read_bytes()
    run_ref = CampaignReceiptRefV1(
        "run",
        run.artifact_id,
        len(raw_run),
        hashlib.sha256(raw_run).hexdigest(),
    )
    # Reseal a complete alternate attempt, not just an uncommitted forged root.
    # Child order/count defects must be rejected by real prefix/tree admission.
    append("started", run_ref)
    for shard_ref in shards:
        shard = _helpers().read_receipt(store, shard_ref)
        for product_ref in shard.products:
            product = _helpers().read_receipt(store, product_ref)
            append("product", product_ref, product.ordinal)
        append("shard", shard_ref)
    append("completed", forged_ref)
    message = {
        "reverse": "journal product prefix differs",
        "count": "shard measured totals",
        "ordinal": "journal product prefix differs",
        "run": "foreign receipt run",
    }[mutation]
    with pytest.raises(ValueError, match=message):
        read_campaign_verification_tree(
            store, expected_root_id=forged.artifact_id
        )


@pytest.mark.parametrize("ordinals", ((), (1, 0), (0, 0), (2,)))
def test_sample_scope_cannot_omit_bounds_or_claim_unknown_products(
    receipt_campaign: dict[str, Any],
    ordinals: tuple[int, ...],
) -> None:
    from histdatacom.campaign_receipt_runner import (
        audit_campaign_verification_tree,
    )

    fixture = receipt_campaign
    with pytest.raises((ValueError, TypeError)):
        audit_campaign_verification_tree(
            fixture["index_ref"].path,
            fixture["store"],
            expected_root_id=fixture["root"].artifact_id,
            product_ordinals=ordinals,
        )


def test_changed_earlier_product_after_durable_receipt_prevents_root(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from histdatacom.campaign_receipt_runner import (
        CampaignReceiptExecutionError,
        run_campaign_verification,
    )
    from histdatacom.campaign_receipt_store import CampaignReceiptStore

    original_write = CampaignReceiptStore.write_immutable
    changed: list[tuple[Path, bytes]] = []

    def mutate_after_real_publication(self: Any, record: Any) -> Any:
        ref = original_write(self, record)
        if record.KIND == "product" and record.ordinal == 0 and not changed:
            path = Path(record.parquet_paths[0])
            changed.append((path, path.read_bytes()))
            path.write_bytes(_helpers().flipped_bytes(path))
        return ref

    monkeypatch.setattr(
        CampaignReceiptStore, "write_immutable", mutate_after_real_publication
    )
    output = tmp_path / "interrupted-native-integrity"
    try:
        with pytest.raises(CampaignReceiptExecutionError) as caught:
            run_campaign_verification(
                receipt_campaign["index_ref"].path,
                output_directory=output,
                products_per_shard=1,
            )
        assert (
            changed
        ), "adversary must follow an actual verified product receipt"
        assert caught.value.failure_ref is not None
        failure = _helpers().read_receipt(output, caught.value.failure_ref)
        assert failure.reason_code and failure.message
        assert not tuple((output / "root").glob("*.json"))
        products = _helpers().stored_records(output, "product")
        assert any(row.ordinal == 0 for _, row in products)
    finally:
        for path, data in changed:
            path.write_bytes(data)


def test_resealed_checkpoint_core_is_rejected_by_fresh_native_replay(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
) -> None:
    from histdatacom.campaign_receipt_contracts import (
        CampaignVerificationCheckpointV1,
        CampaignVerificationJournalV1,
    )
    from histdatacom.campaign_receipt_runner import (
        CampaignReceiptExecutionError,
        resume_campaign_verification,
    )
    from histdatacom.campaign_receipt_store import CampaignReceiptStore

    fixture = receipt_campaign
    run = next(
        run
        for _, run in _helpers().stored_records(fixture["store"], "run")
        if run.artifact_id == fixture["root"].run_id
    )
    original_shard = _helpers().read_receipt(
        fixture["store"], fixture["root"].shards[0]
    )
    original_product = _helpers().read_receipt(
        fixture["store"], original_shard.products[0]
    )
    core = original_product.verification
    false_digest = (
        "f" * 64 if core.logical_content_sha256 != "f" * 64 else "e" * 64
    )
    forged = replace(
        original_product,
        core_json=replace(core, logical_content_sha256=false_digest).to_json(),
    )
    directory = tmp_path / "resealed-prefix"
    with CampaignReceiptStore(directory, create=run) as store:
        head = None
        ordinal = 0

        def append(
            event: str, subject: Any, product_ordinal: int | None = None
        ) -> None:
            nonlocal head, ordinal
            head = store.write_immutable(
                CampaignVerificationJournalV1(
                    run.artifact_id,
                    ordinal,
                    head,
                    event,
                    subject,
                    product_ordinal,
                )
            )
            ordinal += 1

        append("started", store.run_ref)
        forged_ref = store.write_immutable(forged)
        append("product", forged_ref, 0)
        shard_ref = store.write_immutable(
            replace(original_shard, products=(forged_ref,))
        )
        append("shard", shard_ref)
        checkpoint = CampaignVerificationCheckpointV1(
            run.artifact_id,
            head,
            (shard_ref,),
            (),
            1,
            fixture["root"].initial_inventory_sha256,
            1,
        )
        checkpoint_ref = store.write_immutable(checkpoint)
        append("checkpoint", checkpoint_ref)
        assert tuple(entry.event for _, entry in store.journal()) == (
            "started",
            "product",
            "shard",
            "checkpoint",
        )
    # This is a well-framed adversarial claim, not native success evidence.
    # Exact byte hashes, native file references, shard totals and journal shape
    # all admit; none of them authenticates the forged logical-content result.
    with CampaignReceiptStore(directory) as store:
        assert store.find(
            checkpoint.artifact_id, CampaignVerificationCheckpointV1
        ) == (checkpoint_ref, checkpoint)
        assert len(store.journal()) == 4
    with pytest.raises(
        CampaignReceiptExecutionError, match="fresh native replay"
    ) as caught:
        resume_campaign_verification(
            fixture["index_ref"].path,
            store_directory=directory,
            expected_checkpoint_id=checkpoint.artifact_id,
        )
    assert caught.value.failure_ref is not None
    assert not tuple((directory / "root").glob("*.json"))
    assert _helpers().read_receipt(directory, forged_ref) == forged
    assert _helpers().read_receipt(directory, checkpoint_ref) == checkpoint


def test_native_zero_product_refused_plan_retains_and_reverifies_source_closure(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
) -> None:
    from histdatacom.campaign_receipt_runner import (
        assert_current_campaign_verification_tree,
        run_campaign_verification,
    )
    from histdatacom.campaign_receipt_store import (
        read_campaign_verification_tree,
    )

    with _helpers().refused_campaign_index(
        receipt_campaign["native"],
        tmp_path / "declared-zero-work",
    ) as index_ref:
        store = tmp_path / "zero-product-receipts"
        root = run_campaign_verification(
            index_ref.path,
            output_directory=store,
            products_per_shard=1,
        )
        summary = CampaignVerificationSummaryV1.from_json(root.summary_json)
        assert root.product_count == 0
        assert root.shards == ()
        assert summary.status == "complete"
        assert summary.product_count == summary.missing_product_count == 0
        assert summary.refused_window_count == 1
        assert (
            summary.observed_event_count == summary.synthetic_event_count == 0
        )
        assert root.global_controls
        assert len(root.plan_controls) == 1
        plan_control = _helpers().read_receipt(store, root.plan_controls[0])
        assert plan_control.scope == "plan"
        assert plan_control.inputs_sha256 == _input_digest(
            plan_control.input_files
        )
        raw_source = next(
            Path(item.path)
            for item in plan_control.input_files
            if Path(item.path).name == ".data"
        )
        assert_current_campaign_verification_tree(
            index_ref.path,
            store,
            expected_root_id=root.artifact_id,
        )
        root_files = tuple((store / "root").glob("*.json"))
        with _helpers().altered_file(
            raw_source, _helpers().flipped_bytes(raw_source)
        ):
            # Zero products does not mean the plan's source roots are dispensable.
            assert (
                read_campaign_verification_tree(
                    store,
                    expected_root_id=root.artifact_id,
                )
                == root
            )
            with pytest.raises((ValueError, OSError, RuntimeError)):
                assert_current_campaign_verification_tree(
                    index_ref.path,
                    store,
                    expected_root_id=root.artifact_id,
                )
            assert tuple((store / "root").glob("*.json")) == root_files

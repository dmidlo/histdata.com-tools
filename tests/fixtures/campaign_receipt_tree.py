"""One genuine synthetic campaign, shared by the receipt-tree controls.

The native fixture proves software integrity only, not historical processing or
scientific promotion. This wrapper does not replace any native verifier.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterator

import pytest

pytest_plugins = ("tests.fixtures.campaign_verification",)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def receipt_path(store: Path, ref: Any) -> Path:
    return store / ref.kind / f"{ref.sha256}.json"


def read_receipt(store: Path, ref: Any) -> Any:
    """Independently check bytes/domain before the strict structural reader."""
    from histdatacom.campaign_receipt_contracts import RECEIPT_TYPES

    raw = receipt_path(store, ref).read_bytes()
    assert len(raw) == ref.size_bytes
    assert hashlib.sha256(raw).hexdigest() == ref.sha256
    envelope = json.loads(raw)
    domain = canonical_bytes(
        {
            "schema_version": envelope["schema_version"],
            "payload": envelope["payload"],
        }
    )
    expected = (
        f"campaign-receipt-{ref.kind}:sha256:"
        + hashlib.sha256(domain).hexdigest()
    )
    assert envelope["artifact_id"] == ref.artifact_id == expected
    cls = next(cls for cls in RECEIPT_TYPES if cls.KIND == ref.kind)
    return cls.from_dict(envelope)


def stored_records(store: Path, kind: str) -> tuple[tuple[Path, Any], ...]:
    from histdatacom.campaign_receipt_contracts import RECEIPT_TYPES

    cls = next(cls for cls in RECEIPT_TYPES if cls.KIND == kind)
    return tuple(
        (path, cls.from_dict(json.loads(path.read_bytes())))
        for path in sorted((store / kind).glob("*.json"))
    )


def tree_products(store: Path, root: Any) -> tuple[Any, ...]:
    return tuple(
        read_receipt(store, product_ref)
        for shard_ref in root.shards
        for product_ref in read_receipt(store, shard_ref).products
    )


def clone_retained_store(source: Path, destination: Path, root: Any) -> None:
    """Copy historical receipts into a real, independently initialized store.

    The native store marker binds its actual path/inode, so copying that marker
    would only test a moved-store refusal and conceal the intended adversary.
    This helper never calls a verification-success double.
    """
    from histdatacom.campaign_receipt_contracts import RECEIPT_KINDS
    from histdatacom.campaign_receipt_store import (
        CampaignReceiptStore,
        read_campaign_verification_tree,
    )

    run = next(
        record
        for _, record in stored_records(source, "run")
        if record.artifact_id == root.run_id
    )
    with CampaignReceiptStore(destination, create=run):
        pass
    for kind in RECEIPT_KINDS:
        for path in (source / kind).glob("*.json"):
            target = destination / kind / path.name
            if target.exists():
                assert target.read_bytes() == path.read_bytes()
            else:
                shutil.copy2(path, target)
    assert (
        read_campaign_verification_tree(
            destination, expected_root_id=root.artifact_id
        )
        == root
    )


@contextmanager
def altered_file(path: Path, replacement: bytes) -> Iterator[None]:
    """Alter only generated test data; restore exact bytes and old mtime."""
    raw = path.read_bytes()
    before = path.stat()
    try:
        path.write_bytes(replacement)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        yield
    finally:
        path.write_bytes(raw)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert path.read_bytes() == raw


def flipped_bytes(path: Path) -> bytes:
    data = bytearray(path.read_bytes())
    assert data
    data[len(data) // 2] ^= 1
    return bytes(data)


@contextmanager
def refused_campaign_index(native: Any, directory: Path) -> Iterator[Any]:
    """Admit a declared synthetic zero-work plan with the real native readers.

    This is a plan-integrity vector, not a claim that an empirical calibration
    newly failed. Its source graph remains genuine and must still be verified.
    Previously generated products are retained outside every scanned output
    root for this scope and restored exactly afterward.
    """
    from histdatacom import reconstruction as public
    from histdatacom.synthetic import reconstruction_plan as plans
    from tests.fixtures.campaign_verification import write_json

    directory.mkdir()
    old = native.plan
    refusal = plans.ReconstructionPlanRefusalV1(
        start_ns=old.requested_start_ns,
        end_ns=old.requested_end_ns,
        code=plans.ReconstructionPlanRefusalCode.OBSERVATION_CARDINALITY_UNSUPPORTED,
        reason="Declared synthetic zero-work integrity vector; no calibration claim.",
    )
    resources = replace(
        old.resources,
        executable_window_count=0,
        refused_window_count=1,
        workflow_request_count=0,
        estimated_input_event_count=0,
        estimated_candidate_event_count=0,
        estimated_candidate_bytes=0,
        estimated_peak_memory_bytes=0,
        estimated_peak_scratch_bytes=0,
        estimated_output_bytes=0,
        estimated_partition_count=0,
        summary_id="",
    )
    execution = plans.read_reconstruction_plan_execution_manifest(
        old.artifact_graph["execution_manifest"].path
    )
    execution = replace(
        execution,
        executable_window_count=0,
        refusal_ids=(refusal.refusal_id,),
        manifest_id="",
    )
    execution_ref = write_json(
        directory,
        "reconstruction-plan-execution",
        execution,
        kind=plans.PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND,
        metadata={"manifest_id": execution.manifest_id},
    )
    terminal = replace(
        old,
        workflow_requests=(),
        execution_manifest_id=execution.manifest_id,
        artifact_graph={
            **old.artifact_graph,
            "execution_manifest": execution_ref,
        },
        resources=resources,
        refusals=(refusal,),
        plan_id="",
    )
    plan_ref = plans.write_synthetic_infill_plan(terminal, directory)
    original_set = public.read_reconstruction_plan_set(native.plan_set_path)
    shard = replace(
        original_set.shards[0],
        plan_id=terminal.plan_id,
        plan_ref=plan_ref,
        preflight_status=terminal.status,
        refusal_count=1,
        resource_summary=resources.to_dict(),
        shard_id="",
    )
    summaries: list[Any] = []
    partitions: dict[Any, Any] = {}
    public._accumulate_plan_set_resources(
        terminal,
        resource_summaries=summaries,
        source_partitions=partitions,
    )
    plan_set = replace(
        original_set,
        shards=(shard,),
        status=terminal.status,
        resource_summary=public._aggregate_plan_set_resources(
            summaries, partitions
        ),
        plan_set_id="",
    )
    plan_set_ref = public.write_reconstruction_plan_set(plan_set, directory)
    client = public.ReconstructionClient()
    support_ref = client.construct_plan_support_map(
        plan_set_ref.path,
        output_directory=directory,
    )
    support = public.read_reconstruction_plan_support_map(support_ref.path)
    assert len(support.windows) == 1
    assert support.windows[0].status == "refused"
    assert support.windows[0].member_ids == ()
    retained = directory / "retained-products"
    retained.mkdir()
    moved: list[tuple[Path, Path]] = []
    try:
        for ordinal, manifest in enumerate(native.manifest_paths):
            source = manifest.parent
            destination = retained / str(ordinal)
            source.rename(destination)
            moved.append((source, destination))
        index_ref = client.construct_verified_campaign_product_index(
            plan_set_ref.path,
            support_ref.path,
            output_directory=directory / "index",
        )
        yield index_ref
    finally:
        for source, destination in reversed(moved):
            destination.rename(source)


@pytest.fixture(scope="session")
def receipt_campaign(
    native_campaign: Any, tmp_path_factory: Any
) -> dict[str, Any]:
    from histdatacom.campaign_receipt_runner import run_campaign_verification
    from histdatacom.reconstruction import ReconstructionClient

    directory = tmp_path_factory.mktemp("campaign-receipt-tree")
    index_ref = (
        ReconstructionClient().construct_verified_campaign_product_index(
            native_campaign.plan_set_path,
            native_campaign.support_map_path,
            output_directory=directory / "index",
        )
    )
    store = directory / "receipts"
    root = run_campaign_verification(
        index_ref.path,
        output_directory=store,
        products_per_shard=1,
    )
    return {
        "native": native_campaign,
        "index_ref": index_ref,
        "store": store,
        "root": root,
    }

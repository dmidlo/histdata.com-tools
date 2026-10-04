"""Real process-death recovery, not an exception substituted for a crash.

The child-only hook blocks after the genuine store writer has durably published
a receipt. The parent kills that exact owned PID and a new process resumes using
the ordinary native verifier. No production callback or successful double exists.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any

import pytest

pytest_plugins = ("tests.fixtures.campaign_receipt_tree",)

CHILD = r"""
import json
import os
from pathlib import Path
import sys

from histdatacom.campaign_receipt_runner import (
    run_campaign_verification, resume_campaign_verification,
)
from histdatacom.campaign_receipt_store import CampaignReceiptStore

mode, index, directory, stop_kind, barrier, result, checkpoint, stop_count = sys.argv[1:]
if mode == "interrupt":
    original = CampaignReceiptStore.write_immutable
    stopped = False
    seen = 0
    def durable_boundary(self, record):
        global stopped, seen
        ref = original(self, record)
        selected = record.KIND == stop_kind
        if stop_kind == "checkpoint":
            # The checkpoint body alone is not a committed resume prefix.
            selected = record.KIND == "journal" and record.event == "checkpoint"
        if selected:
            seen += 1
        if selected and seen == int(stop_count) and not stopped:
            stopped = True
            Path(barrier).write_text(json.dumps({
                "pid": os.getpid(), "kind": stop_kind,
                "receipt_kind": record.KIND,
                "artifact_id": ref.artifact_id,
                "sha256": ref.sha256, "size_bytes": ref.size_bytes,
            }, sort_keys=True), encoding="utf-8")
            print("DURABLE_BARRIER", flush=True)
            sys.stdin.buffer.read(1)
        return ref
    CampaignReceiptStore.write_immutable = durable_boundary
    root = run_campaign_verification(index, output_directory=directory,
                                     products_per_shard=1)
else:
    root = resume_campaign_verification(
        index, store_directory=directory,
        expected_checkpoint_id=checkpoint or None,
    )
Path(result).write_text(root.to_json(), encoding="utf-8")
print("ACTUAL_ROOT", root.artifact_id, flush=True)
"""


def _fixture_helpers() -> Any:
    from tests.fixtures import campaign_receipt_tree

    return campaign_receipt_tree


def _command(
    fixture: dict[str, Any],
    directory: Path,
    mode: str,
    kind: str,
    barrier: Path,
    result: Path,
    checkpoint: str = "",
    stop_count: int = 1,
) -> list[str]:
    return [
        sys.executable,
        "-B",
        "-c",
        CHILD,
        mode,
        fixture["index_ref"].path,
        str(directory),
        kind,
        str(barrier),
        str(result),
        checkpoint,
        str(stop_count),
    ]


def _environment() -> dict[str, str]:
    # Preserve the exact running interpreter/dependencies; never install anything.
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def _interrupt_after_durable_receipt(
    fixture: dict[str, Any],
    directory: Path,
    work: Path,
    kind: str,
    stop_count: int = 1,
) -> tuple[str | None, dict[Path, bytes]]:
    barrier = work / "durable-barrier.json"
    result = work / "premature-root.json"
    log_path = work / "interrupted-child.log"
    command = _command(
        fixture,
        directory,
        "interrupt",
        kind,
        barrier,
        result,
        stop_count=stop_count,
    )
    with log_path.open("wb") as log:
        child = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=_environment(),
        )
        try:
            # Fixed finite harness budget; no scientific threshold or retry search.
            deadline = time.monotonic() + 180
            while not barrier.exists():
                assert child.poll() is None, log_path.read_text()
                assert time.monotonic() < deadline, log_path.read_text()
                time.sleep(0.025)
            evidence = json.loads(barrier.read_text())
            assert evidence["pid"] == child.pid
            assert evidence["kind"] == kind
            assert child.poll() is None
            assert not result.exists()
            receipt = (
                directory
                / evidence["receipt_kind"]
                / (evidence["sha256"] + ".json")
            )
            assert receipt.is_file()
            assert receipt.stat().st_size == evidence["size_bytes"]
            # No shell pattern, process group, or foreign PID is signalled.
            os.kill(child.pid, signal.SIGKILL)
            assert child.wait(timeout=30) == -signal.SIGKILL
            assert not result.exists()
            assert not tuple((directory / "root").glob("*.json"))
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=30)
            if child.stdin is not None:
                child.stdin.close()
    checkpoints = _fixture_helpers().stored_records(directory, "checkpoint")
    committed = {
        record.subject.artifact_id
        for _, record in _fixture_helpers().stored_records(directory, "journal")
        if record.event == "checkpoint"
    }
    expected = max(
        (
            record
            for _, record in checkpoints
            if record.artifact_id in committed
        ),
        key=lambda record: record.next_product_ordinal,
        default=None,
    )
    prefix = {
        path: path.read_bytes()
        for receipt_kind in (
            "control",
            "product",
            "shard",
            "checkpoint",
            "journal",
        )
        for path in (directory / receipt_kind).glob("*.json")
    }
    return (None if expected is None else expected.artifact_id), prefix


def _reference(path: Path, record: Any) -> Any:
    from histdatacom.campaign_receipt_contracts import CampaignReceiptRefV1

    raw = path.read_bytes()
    return CampaignReceiptRefV1(
        record.KIND,
        record.artifact_id,
        len(raw),
        hashlib.sha256(raw).hexdigest(),
    )


@pytest.mark.parametrize(
    "kind,selection,stop_count",
    (
        ("control", "default", 1),
        ("product", "default", 1),
        ("shard", "default", 1),
        ("checkpoint", "explicit", 1),
        ("checkpoint", "default", 1),
        ("checkpoint", "default", 2),
        ("checkpoint", "explicit_first", 2),
    ),
)
def test_real_sigkill_then_new_process_native_resume_has_exact_coverage(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
    kind: str,
    selection: str,
    stop_count: int,
) -> None:
    from histdatacom.campaign_receipt_contracts import (
        CampaignVerificationRootV1,
    )
    from histdatacom.campaign_receipt_store import (
        read_campaign_verification_tree,
    )

    store = tmp_path / "receipt-store"
    checkpoint, prefix = _interrupt_after_durable_receipt(
        receipt_campaign,
        store,
        tmp_path,
        kind,
        stop_count,
    )
    old_products = {
        _reference(path, product): product
        for path, product in _fixture_helpers().stored_records(store, "product")
    }
    old_shards = {
        _reference(path, shard)
        for path, shard in _fixture_helpers().stored_records(store, "shard")
    }
    selected_ref = selected = None
    if kind == "checkpoint":
        assert checkpoint is not None
        checkpoints = _fixture_helpers().stored_records(store, "checkpoint")
        assert len(checkpoints) == stop_count
        if selection == "explicit_first":
            selected_path, selected = min(
                checkpoints, key=lambda pair: pair[1].next_product_ordinal
            )
            checkpoint = selected.artifact_id
        else:
            selected_path, selected = next(
                (path, record)
                for path, record in checkpoints
                if record.artifact_id == checkpoint
            )
        selected_ref = _reference(selected_path, selected)
    else:
        assert checkpoint is None
    result = tmp_path / "resumed-root.json"
    with (tmp_path / "resumed-child.log").open("wb") as log:
        resumed = subprocess.run(
            _command(
                receipt_campaign,
                store,
                "resume",
                "",
                tmp_path / "unused",
                result,
                (checkpoint or "") if selection != "default" else "",
            ),
            stdout=log,
            stderr=subprocess.STDOUT,
            env=_environment(),
            timeout=180,
            check=False,
        )
    assert resumed.returncode == 0, (tmp_path / "resumed-child.log").read_text()
    root = CampaignVerificationRootV1.from_json(result.read_text())
    assert (
        read_campaign_verification_tree(
            store, expected_root_id=root.artifact_id
        )
        == root
    )
    products = _fixture_helpers().tree_products(store, root)
    assert root.product_count == 2
    assert len(root.shards) == 2
    assert tuple(product.ordinal for product in products) == (0, 1)
    assert len({product.verification.manifest_id for product in products}) == 2
    assert {product.verification.product_ref.path for product in products} == {
        str(path.resolve())
        for path in receipt_campaign["native"].manifest_paths
    }
    assert root.reused_checkpoint == selected_ref
    assert root.elapsed_scope == (
        "current_finalization_attempt_not_reused_historical_leaf_timings"
    )
    final_refs = tuple(
        product_ref
        for shard_ref in root.shards
        for product_ref in _fixture_helpers()
        .read_receipt(store, shard_ref)
        .products
    )
    reused_refs = (
        ()
        if selected is None
        else tuple(
            product_ref
            for shard_ref in selected.shards
            for product_ref in _fixture_helpers()
            .read_receipt(store, shard_ref)
            .products
        )
    )
    prefix_count = 0 if selected is None else selected.next_product_ordinal
    assert final_refs[:prefix_count] == reused_refs
    assert set(final_refs) & set(old_products) == set(reused_refs)
    assert set(root.shards) & old_shards == (
        set() if selected is None else set(selected.shards)
    )
    if selected is not None:
        assert root.shards[: len(selected.shards)] == selected.shards
        for ref in reused_refs:
            ordinal = old_products[ref].ordinal
            # Fresh native replay must not publish duplicate outcomes for the
            # checkpointed prefix, even though it measures that work again.
            assert {
                record.artifact_id
                for _, record in _fixture_helpers().stored_records(
                    store, "product"
                )
                if record.ordinal == ordinal
            } == {ref.artifact_id}
    journal = sorted(
        (
            record
            for _, record in _fixture_helpers().stored_records(store, "journal")
        ),
        key=lambda record: record.ordinal,
    )
    resumed_events = [record for record in journal if record.event == "resumed"]
    assert tuple(record.subject for record in resumed_events) == (
        () if selected_ref is None else (selected_ref,)
    )
    attempt_start = next(
        record.ordinal
        for record in reversed(journal)
        if record.event in {"started", "resumed"}
    )
    assert tuple(
        record.product_ordinal
        for record in journal
        if record.ordinal > attempt_start and record.event == "product"
    ) == tuple(range(prefix_count, 2))
    # Prefix artifacts are immutable history, including unjournalled orphan work.
    assert prefix
    for path, original in prefix.items():
        assert path.read_bytes() == original


def test_changed_input_after_real_crash_is_reverified_not_skipped(
    receipt_campaign: dict[str, Any],
    tmp_path: Path,
) -> None:
    store = tmp_path / "receipt-store"
    checkpoint, prefix = _interrupt_after_durable_receipt(
        receipt_campaign,
        store,
        tmp_path,
        "checkpoint",
    )
    assert checkpoint is not None
    products = _fixture_helpers().stored_records(store, "product")
    first = next(record for _, record in products if record.ordinal == 0)
    parquet = Path(first.parquet_paths[0])
    result = tmp_path / "must-not-exist.json"
    with _fixture_helpers().altered_file(
        parquet, _fixture_helpers().flipped_bytes(parquet)
    ):
        with (tmp_path / "failed-resume.log").open("wb") as log:
            resumed = subprocess.run(
                _command(
                    receipt_campaign,
                    store,
                    "resume",
                    "",
                    tmp_path / "unused",
                    result,
                    checkpoint,
                ),
                stdout=log,
                stderr=subprocess.STDOUT,
                env=_environment(),
                timeout=180,
                check=False,
            )
        assert resumed.returncode != 0
        assert not result.exists()
        assert not tuple((store / "root").glob("*.json"))
        failures = _fixture_helpers().stored_records(store, "failure")
        assert (
            failures
        ), "failed native resume must retain bounded failure evidence"
        assert all(
            record.reason_code and record.message for _, record in failures
        )
    for path, original in prefix.items():
        assert path.read_bytes() == original

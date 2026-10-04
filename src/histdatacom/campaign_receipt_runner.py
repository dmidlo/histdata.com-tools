"""Durable receipt execution and full-native recovery, never cached authority.

Resume deliberately repeats native verification, even with an independently
retained checkpoint ID. The checkpoint selects historical provenance only.
Measured read bytes count guarded hashing reads, not hidden Arrow/library I/O.
"""

from __future__ import annotations

import hashlib
import os
import platform
import stat
import sys
import time
from collections.abc import Iterator
from importlib import metadata
from pathlib import Path
from typing import Any

from histdatacom.campaign_index_contracts import canonical
from histdatacom.campaign_receipt_contracts import (
    MAX_CONTROL_RECEIPTS,
    MAX_VERIFICATION_PRODUCTS,
    MAX_VERIFICATION_SHARDS,
    CampaignControlReceiptV1,
    CampaignProductReceiptV1,
    CampaignReceipt,
    CampaignReceiptRefV1,
    CampaignVerificationCheckpointV1,
    CampaignVerificationFailureV1,
    CampaignVerificationJournalV1,
    CampaignVerificationRootV1,
    CampaignVerificationRunV1,
    CampaignVerificationSampleV1,
    CampaignVerificationShardV1,
    CampaignVerifiedFileV1,
)
from histdatacom.campaign_receipt_store import (
    CampaignReceiptStore,
    CampaignReceiptStoreError,
    _absolute,
    _identity,
    _open_directory,
    iter_checkpoint_products,
    iter_tree_controls,
    iter_tree_products,
)
from histdatacom.campaign_receipt_store import (
    read_campaign_verification_tree as _read_tree,
)


class CampaignReceiptExecutionError(ValueError):
    """Execution failed; retained refs describe failure, never completion."""

    def __init__(
        self,
        message: str,
        *,
        failure_ref: CampaignReceiptRefV1 | None = None,
        checkpoint_ref: CampaignReceiptRefV1 | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_ref = failure_ref
        self.checkpoint_ref = checkpoint_ref


def _elapsed(start: int) -> int:
    result = time.perf_counter_ns() - start
    if not 0 <= result < 2**63:
        raise CampaignReceiptExecutionError(
            "verification monotonic clock regressed"
        )
    return result


def _file_bytes(
    path: Path,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    counter: list[int] | None = None,
) -> tuple[int, str]:
    directory = _open_directory(path.parent)
    try:
        before = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or before.st_size < 0:
            raise CampaignReceiptStoreError("verification input is not regular")
        if expected_size is not None and before.st_size != expected_size:
            raise CampaignReceiptStoreError("verification input size changed")
        fd = os.open(
            path.name,
            os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW,
            dir_fd=directory,
        )
        try:
            if _identity(os.fstat(fd)) != _identity(before):
                raise CampaignReceiptStoreError(
                    "verification input changed on open"
                )
            digest = hashlib.sha256()
            size = 0
            while chunk := os.read(fd, 1024 * 1024):
                size += len(chunk)
                if counter is not None:
                    counter[0] += len(chunk)
                    if counter[0] >= 2**63:
                        raise CampaignReceiptExecutionError(
                            "measured input byte bound"
                        )
                if size > before.st_size:
                    raise CampaignReceiptStoreError("verification input grew")
                digest.update(chunk)
            if size != before.st_size or _identity(os.fstat(fd)) != _identity(
                before
            ):
                raise CampaignReceiptStoreError(
                    "verification input changed on read"
                )
        finally:
            os.close(fd)
        if _identity(
            os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        ) != _identity(before):
            raise CampaignReceiptStoreError("verification input replaced")
        actual = digest.hexdigest()
        if expected_sha256 is not None and actual != expected_sha256:
            raise CampaignReceiptStoreError(
                "verification input SHA-256 changed"
            )
        return size, actual
    finally:
        os.close(directory)


def _runtime_identity() -> tuple[str, str]:
    """Actual package Python bytes plus explicit, non-attested runtime metadata."""
    package = Path(__file__).parent
    paths: list[tuple[Path, int]] = []
    total = 0
    for path in package.rglob("*.py"):
        if len(paths) >= 4096:
            raise CampaignReceiptExecutionError(
                "implementation file-count bound"
            )
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise CampaignReceiptExecutionError(
                "implementation file is not regular"
            )
        total += info.st_size
        if total > 64 * 1024 * 1024:
            raise CampaignReceiptExecutionError("implementation byte bound")
        paths.append((path, info.st_size))
    if not paths:
        raise CampaignReceiptExecutionError("implementation file-count bound")
    digest = hashlib.sha256(b"histdatacom.campaign-verifier-package.v1\n")
    total = 0
    for path, admitted_size in sorted(paths):
        size, value = _file_bytes(path, expected_size=admitted_size)
        total += size
        if total > 64 * 1024 * 1024:
            raise CampaignReceiptExecutionError("implementation byte bound")
        digest.update(
            canonical(
                {
                    "path": str(path.relative_to(package)),
                    "sha256": value,
                    "bytes": size,
                }
            ).encode("ascii")
            + b"\n"
        )
    versions: dict[str, str | None] = {}
    for name in ("histdatacom", "pyarrow", "polars", "numpy", "scipy"):
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = None
    environment = canonical(
        {
            "schema_version": "histdatacom.campaign-verifier-environment.v1",
            "python_version": sys.version,
            "python_implementation": sys.implementation.name,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "dependency_versions": versions,
            "scope": "runtime_versions_not_binary_environment_attestation",
            "read_bytes_scope": "guarded_hashing_reads_excludes_native_library_reopens",
        }
    )
    return digest.hexdigest(), environment


def _run_record(
    index_path: str | Path, traversal: Any, products_per_shard: int
) -> CampaignVerificationRunV1:
    if type(products_per_shard) is not int or not 1 <= products_per_shard <= 64:
        raise CampaignReceiptExecutionError("products_per_shard must be1..64")
    count = len(traversal.inventory.coordinates)
    if count > min(
        MAX_VERIFICATION_PRODUCTS, products_per_shard * MAX_VERIFICATION_SHARDS
    ):
        raise CampaignReceiptExecutionError(
            "prospective receipt shard/product bound"
        )
    implementation, environment = _runtime_identity()
    structural = traversal.inventory.structural
    return CampaignVerificationRunV1(
        str(_absolute(index_path)),
        structural.index_id,
        structural.index_ref.sha256,
        implementation,
        environment,
        tuple(
            sorted({str(path) for path in traversal.inventory.forbidden_roots})
        ),
        products_per_shard,
    )


def _product(
    run: CampaignVerificationRunV1, evidence: Any
) -> CampaignProductReceiptV1:
    files = tuple(
        CampaignVerifiedFileV1(
            str(item.path), item.size_bytes, item.sha256, item.role
        )
        for item in evidence.files
    )
    return CampaignProductReceiptV1(
        run.artifact_id,
        evidence.ordinal,
        evidence.verification.to_json(),
        files,
        tuple(evidence.parquet_paths),
        evidence.lineage_json,
        evidence.elapsed_ns,
        evidence.read_bytes,
    )


def _semantic(product: CampaignProductReceiptV1) -> tuple[object, ...]:
    return (
        product.run_id,
        product.ordinal,
        product.core_json,
        product.input_files,
        product.parquet_paths,
        product.lineage_json,
    )


class _Capacity:
    """Known metadata reserve plus exact leaf charging; not a disk guarantee.

    Repeated checkpoint refs are quadratic in SHARDS, not in products. Unknown
    product/control byte totals are admitted incrementally and may refuse a
    campaign before completion. Cardinality ceilings do not promise it fits.
    """

    def __init__(
        self, store: CampaignReceiptStore, count: int, reused_count: int = 0
    ) -> None:
        self.store = store
        group = store.run.products_per_shard
        prior_shards = reused_count // group
        shards = (count + group - 1) // group - prior_shards
        checkpoints = count // group
        suffix = count - reused_count
        checkpoint_refs = (
            checkpoints * (checkpoints + 1) - prior_shards * (prior_shards + 1)
        ) // 2
        self.files = 2 * suffix + 4 * shards + MAX_CONTROL_RECEIPTS + 8
        self.bytes = (
            4096 * (suffix + 3 * shards + 4)
            + 512 * (suffix + checkpoint_refs)
            + 2 * 4 * 1024 * 1024
        )
        store.reserve_capacity(files=self.files, size_bytes=self.bytes)

    def write(self, record: CampaignReceipt) -> CampaignReceiptRefV1:
        size = len(record.to_json().encode("ascii"))
        metadata = record.KIND not in {"control", "product"}
        self.store.reserve_capacity(
            files=self.files + 1, size_bytes=self.bytes + size
        )
        result = self.store.write_immutable(record)
        self.files = max(0, self.files - 1)
        if metadata:
            self.bytes = max(0, self.bytes - size)
        return result


def _controls(
    store: CampaignReceiptStore,
    traversal: Any,
    *,
    old_root: CampaignVerificationRootV1 | None,
    capacity: _Capacity | None,
) -> tuple[tuple[CampaignReceiptRefV1, ...], tuple[CampaignReceiptRefV1, ...]]:
    old = iter_tree_controls(store, old_root) if old_root is not None else None
    groups: tuple[list[CampaignReceiptRefV1], list[CampaignReceiptRefV1]] = (
        [],
        [],
    )
    for evidence in traversal.iter_control_evidence():
        control = CampaignControlReceiptV1(
            store.run.artifact_id,
            evidence.ordinal,
            evidence.scope,
            evidence.plan_id,
            evidence.shard_id,
            tuple(
                CampaignVerifiedFileV1(
                    str(item.path), item.size_bytes, item.sha256, item.role
                )
                for item in evidence.files
            ),
            evidence.inputs_sha256,
            evidence.elapsed_ns,
            evidence.read_bytes,
        )
        if old is None:
            assert capacity is not None
            ref = capacity.write(control)
        else:
            previous = next(old, None)
            if previous is None:
                raise CampaignReceiptExecutionError(
                    "fresh controls exceed retained coverage"
                )
            ref, retained = previous
            if (
                control.run_id,
                control.ordinal,
                control.scope,
                control.plan_id,
                control.shard_id,
                control.input_files,
                control.inputs_sha256,
            ) != (
                retained.run_id,
                retained.ordinal,
                retained.scope,
                retained.plan_id,
                retained.shard_id,
                retained.input_files,
                retained.inputs_sha256,
            ):
                raise CampaignReceiptExecutionError(
                    "fresh control evidence differs"
                )
        groups[control.scope == "plan"].append(ref)
    if old is not None and next(old, None) is not None:
        raise CampaignReceiptExecutionError(
            "fresh controls omit retained coverage"
        )
    return tuple(groups[0]), tuple(groups[1])


class _Journal:
    def __init__(
        self, store: CampaignReceiptStore, capacity: _Capacity | None = None
    ) -> None:
        self.store = store
        self.capacity = capacity
        entries = store.journal()
        self.ordinal = len(entries)
        self.head = entries[-1][0] if entries else None

    def append(
        self,
        event: str,
        subject: CampaignReceiptRefV1,
        product_ordinal: int | None = None,
    ) -> CampaignReceiptRefV1:
        value = CampaignVerificationJournalV1(
            self.store.run.artifact_id,
            self.ordinal,
            self.head,
            event,
            subject,
            product_ordinal,
        )
        result = (
            self.capacity.write(value)
            if self.capacity is not None
            else self.store.write_immutable(value)
        )
        self.ordinal += 1
        self.head = result
        return result


def _shard(
    store: CampaignReceiptStore,
    refs: tuple[CampaignReceiptRefV1, ...],
    ordinal: int,
    capacity: _Capacity,
) -> CampaignReceiptRefV1:
    totals = [0, 0, 0, 0]
    for ref in refs:
        product = store.read(ref, CampaignProductReceiptV1)
        core = product.verification
        for n, value in enumerate(
            (
                core.observed_event_count,
                core.synthetic_event_count,
                product.elapsed_ns,
                product.read_bytes,
            )
        ):
            totals[n] += value
    return capacity.write(
        CampaignVerificationShardV1(
            store.run.artifact_id,
            ordinal,
            ordinal * store.run.products_per_shard,
            refs,
            totals[0],
            totals[1],
            totals[2],
            totals[3],
            store.run.products_per_shard,
        )
    )


def _rehash_files(files: Any, counter: list[int]) -> None:
    for item in files:
        _file_bytes(
            Path(item.path),
            expected_size=item.size_bytes,
            expected_sha256=item.sha256,
            counter=counter,
        )


def _rehash_controls(
    store: CampaignReceiptStore,
    refs: tuple[CampaignReceiptRefV1, ...],
    counter: list[int],
) -> None:
    for ref in refs:
        _rehash_files(
            store.read(ref, CampaignControlReceiptV1).input_files, counter
        )


def _rehash_products(
    store: CampaignReceiptStore,
    refs: tuple[CampaignReceiptRefV1, ...],
    counter: list[int],
) -> None:
    for ref in refs:
        shard = store.read(ref, CampaignVerificationShardV1)
        for product_ref in shard.products:
            product = store.read(product_ref, CampaignProductReceiptV1)
            _rehash_files(product.input_files, counter)


def _check_runtime(run: CampaignVerificationRunV1) -> None:
    if _runtime_identity() != (run.implementation_sha256, run.environment_json):
        raise CampaignReceiptExecutionError(
            "verification implementation/environment changed"
        )


def _failure(
    store: CampaignReceiptStore,
    journal: _Journal,
    error: BaseException,
    checkpoint: CampaignReceiptRefV1 | None,
    ordinal: int | None,
    start: int,
    read_bytes: int,
) -> CampaignReceiptExecutionError:
    message = " ".join(str(error).split())[:1024] or type(error).__name__
    ref = None
    try:
        failure = CampaignVerificationFailureV1(
            store.run.artifact_id,
            ordinal,
            checkpoint,
            type(error).__name__[:128],
            message,
            _elapsed(start),
            read_bytes,
        )
        ref = store.write_immutable(failure)
        journal.append("failed", ref)
    except (OSError, ValueError, TypeError):
        # A failed write may have left bytes. Never pretend their durability or
        # an accepted failure journal was confirmed when it was not.
        ref = None
    return CampaignReceiptExecutionError(
        message, failure_ref=ref, checkpoint_ref=checkpoint
    )


def _execute(
    store: CampaignReceiptStore,
    traversal: Any,
    *,
    existing_root: CampaignVerificationRootV1 | None = None,
    selected_checkpoint: (
        tuple[CampaignReceiptRefV1, CampaignVerificationCheckpointV1] | None
    ) = None,
    record_failure: bool = True,
    started_ns: int | None = None,
) -> CampaignVerificationRootV1:
    start = time.perf_counter_ns() if started_ns is None else started_ns
    journal = _Journal(store)
    checkpoint = None if selected_checkpoint is None else selected_checkpoint[0]
    reused_checkpoint = checkpoint
    prefix_count = (
        0
        if selected_checkpoint is None
        else selected_checkpoint[1].next_product_ordinal
    )
    ordinal = None
    read_bytes = 0
    rereads = [0]
    shards: list[CampaignReceiptRefV1] = (
        []
        if selected_checkpoint is None
        else list(selected_checkpoint[1].shards)
    )
    pending: list[CampaignReceiptRefV1] = []
    old_products: (
        Iterator[tuple[CampaignReceiptRefV1, CampaignProductReceiptV1]] | None
    ) = (
        iter_tree_products(store, existing_root)
        if existing_root is not None
        else None
    )
    prefix_products = (
        None
        if selected_checkpoint is None
        else iter_checkpoint_products(store, selected_checkpoint[1])
    )
    try:
        if selected_checkpoint is not None and (
            existing_root is not None
            or selected_checkpoint[1].inventory_sha256
            != traversal.inventory.inventory_sha256
            or prefix_count > len(traversal.inventory.coordinates)
        ):
            raise CampaignReceiptExecutionError(
                "checkpoint native inventory/prefix differs"
            )
        capacity = (
            _Capacity(store, len(traversal.inventory.coordinates), prefix_count)
            if existing_root is None
            else None
        )
        journal.capacity = capacity
        if existing_root is None:
            if reused_checkpoint is None:
                journal.append("started", store.run_ref)
            else:
                journal.append("resumed", reused_checkpoint)
        global_controls, plan_controls = _controls(
            store,
            traversal,
            old_root=existing_root,
            capacity=capacity,
        )
        next_ordinal = 0
        for evidence in traversal:
            ordinal = evidence.ordinal
            if ordinal != next_ordinal:
                raise CampaignReceiptExecutionError(
                    "native product traversal skipped or duplicated coordinate"
                )
            product = _product(store.run, evidence)
            read_bytes += product.read_bytes
            if old_products is not None:
                old = next(old_products, None)
                if old is None or _semantic(product) != _semantic(old[1]):
                    raise CampaignReceiptExecutionError(
                        "completed product receipt differs from fresh native replay"
                    )
            elif ordinal < prefix_count:
                assert prefix_products is not None
                retained = next(prefix_products, None)
                if retained is None or _semantic(product) != _semantic(
                    retained[1]
                ):
                    raise CampaignReceiptExecutionError(
                        "checkpoint product differs from fresh native replay"
                    )
            else:
                assert capacity is not None
                ref = capacity.write(product)
                journal.append("product", ref, ordinal)
                pending.append(ref)
                if len(pending) == store.run.products_per_shard:
                    ref = _shard(store, tuple(pending), len(shards), capacity)
                    journal.append("shard", ref)
                    shards.append(ref)
                    pending.clear()
                    checkpoint = capacity.write(
                        CampaignVerificationCheckpointV1(
                            store.run.artifact_id,
                            journal.head,
                            tuple(shards),
                            (),
                            ordinal + 1,
                            traversal.inventory.inventory_sha256,
                            store.run.products_per_shard,
                        )
                    )
                    journal.append("checkpoint", checkpoint)
            next_ordinal += 1
        if (
            prefix_products is not None
            and next(prefix_products, None) is not None
        ):
            raise CampaignReceiptExecutionError(
                "checkpoint prefix exceeds fresh native replay"
            )
        if old_products is not None:
            if next(old_products, None) is not None:
                raise CampaignReceiptExecutionError(
                    "completed tree has extra products"
                )
            assert existing_root is not None
            final_shards = existing_root.shards
        else:
            if pending:
                assert capacity is not None
                ref = _shard(store, tuple(pending), len(shards), capacity)
                journal.append("shard", ref)
                shards.append(ref)
            final_shards = tuple(shards)
        # Discarded native guards cannot protect earlier products. Stream ALL
        # persisted product closures again immediately before final inventory.
        _rehash_controls(store, global_controls + plan_controls, rereads)
        _rehash_products(store, final_shards, rereads)
        summary = traversal.finish()
        if summary is None:
            raise CampaignReceiptExecutionError(
                "sample traversal cannot complete a campaign"
            )
        _check_runtime(store.run)
        store.admit_namespace()
        if existing_root is not None:
            if summary.to_json() != existing_root.summary_json or (
                traversal.inventory.inventory_sha256
                != existing_root.initial_inventory_sha256
            ):
                raise CampaignReceiptExecutionError(
                    "completed root differs from fresh native summary"
                )
            return existing_root
        root = CampaignVerificationRootV1(
            store.run.artifact_id,
            final_shards,
            summary.to_json(),
            traversal.inventory.inventory_sha256,
            traversal.inventory.inventory_sha256,
            next_ordinal,
            _elapsed(start),
            traversal.read_bytes + rereads[0],
            store.run.products_per_shard,
            global_controls,
            plan_controls,
            reused_checkpoint=reused_checkpoint,
        )
        for _ in iter_tree_products(store, root):
            pass
        assert capacity is not None
        ref = capacity.write(root)
        journal.append("completed", ref)
        store.admit_namespace()
        return root
    except (Exception, KeyboardInterrupt) as error:
        if not record_failure:
            raise CampaignReceiptExecutionError(str(error)) from error
        raise _failure(
            store,
            journal,
            error,
            checkpoint,
            ordinal,
            start,
            max(read_bytes, traversal.read_bytes) + rereads[0],
        ) from error


def read_campaign_verification_tree(
    store_directory: str | Path, *, expected_root_id: str
) -> CampaignVerificationRootV1:
    """Structural-only public reader; no current input verification implied."""
    return _read_tree(store_directory, expected_root_id=expected_root_id)


def run_campaign_verification(
    index_path: str | Path,
    *,
    output_directory: str | Path,
    products_per_shard: int = 64,
) -> CampaignVerificationRootV1:
    """Run full native verification into a new, external immutable tree."""
    from histdatacom.campaign_verification import open_campaign_verification

    start = time.perf_counter_ns()
    traversal = open_campaign_verification(index_path)
    run = _run_record(index_path, traversal, products_per_shard)
    with CampaignReceiptStore(output_directory, create=run) as store:
        return _execute(store, traversal, started_ns=start)


def resume_campaign_verification(
    index_path: str | Path,
    *,
    store_directory: str | Path,
    expected_checkpoint_id: str | None = None,
) -> CampaignVerificationRootV1:
    """Reverify all products. No checkpoint, even anchored, skips native work."""
    from histdatacom.campaign_verification import open_campaign_verification

    start = time.perf_counter_ns()
    with CampaignReceiptStore(store_directory) as store:
        journal = _Journal(store)
        checkpoint = None
        selected = None
        try:
            events = store.journal()
            if expected_checkpoint_id is not None:
                checkpoint, selected = store.find(
                    expected_checkpoint_id, CampaignVerificationCheckpointV1
                )
                if selected.run_id != store.run.artifact_id or not any(
                    event.event == "checkpoint"
                    and event.subject == checkpoint
                    and event.previous == selected.journal_head
                    for _, event in events
                ):
                    raise CampaignReceiptExecutionError(
                        "checkpoint is not a committed prefix"
                    )
            else:
                committed = [
                    event.subject
                    for _, event in events
                    if event.event == "checkpoint"
                ]
                if committed:
                    checkpoint = committed[-1]
                    selected = store.read(
                        checkpoint, CampaignVerificationCheckpointV1
                    )
            if selected is not None:
                for _ in iter_checkpoint_products(store, selected):
                    pass
            traversal = open_campaign_verification(index_path)
            if (
                _run_record(index_path, traversal, store.run.products_per_shard)
                != store.run
            ):
                raise CampaignReceiptExecutionError(
                    "resume run inputs or implementation/environment differ"
                )
            completed = [
                event.subject
                for _, event in events
                if event.event == "completed"
            ]
            old = (
                store.read(completed[-1], CampaignVerificationRootV1)
                if completed
                else None
            )
        except (Exception, KeyboardInterrupt) as error:
            raise _failure(
                store, journal, error, checkpoint, None, start, 0
            ) from error
        return _execute(
            store,
            traversal,
            existing_root=old,
            started_ns=start,
            selected_checkpoint=(
                None
                if old is not None or checkpoint is None or selected is None
                else (checkpoint, selected)
            ),
        )


def assert_current_campaign_verification_tree(
    index_path: str | Path,
    store_directory: str | Path,
    *,
    expected_root_id: str,
) -> CampaignVerificationRootV1:
    """Non-writing FULL native replay of one independently selected old root."""
    from histdatacom.campaign_verification import open_campaign_verification

    start = time.perf_counter_ns()
    with CampaignReceiptStore(store_directory) as store:
        ref, root = store.find(expected_root_id, CampaignVerificationRootV1)
        if not any(
            event.event == "completed" and event.subject == ref
            for _, event in store.journal()
        ):
            raise CampaignReceiptExecutionError(
                "root lacks durable completion event"
            )
        traversal = open_campaign_verification(index_path)
        if (
            _run_record(index_path, traversal, store.run.products_per_shard)
            != store.run
        ):
            raise CampaignReceiptExecutionError(
                "current run source/environment differs"
            )
        return _execute(
            store,
            traversal,
            existing_root=root,
            record_failure=False,
            started_ns=start,
        )


def audit_campaign_verification_tree(
    index_path: str | Path,
    store_directory: str | Path,
    *,
    expected_root_id: str,
    product_ordinals: tuple[int, ...],
) -> CampaignVerificationSampleV1:
    """Fresh selected-product replay; never a current full-campaign proof."""
    from histdatacom.campaign_verification import open_campaign_verification

    if (
        type(product_ordinals) is not tuple
        or not 1 <= len(product_ordinals) <= 64
        or any(
            type(value) is not int or not 0 <= value < MAX_VERIFICATION_PRODUCTS
            for value in product_ordinals
        )
        or product_ordinals != tuple(sorted(set(product_ordinals)))
    ):
        raise CampaignReceiptExecutionError(
            "explicit sorted bounded sample ordinals required"
        )
    start = time.perf_counter_ns()
    with CampaignReceiptStore(store_directory) as store:
        root_ref, root = store.find(
            expected_root_id, CampaignVerificationRootV1
        )
        if not any(
            event.event == "completed" and event.subject == root_ref
            for _, event in store.journal()
        ):
            raise CampaignReceiptExecutionError(
                "sample root lacks completion event"
            )
        retained = {}
        for ref, product in iter_tree_products(store, root):
            if product.ordinal in product_ordinals:
                retained[product.ordinal] = (ref, product)
        if len(retained) != len(product_ordinals):
            raise CampaignReceiptExecutionError(
                "sample ordinal outside completed tree"
            )
        traversal = open_campaign_verification(
            index_path, selected_ordinals=product_ordinals
        )
        if (
            _run_record(index_path, traversal, store.run.products_per_shard)
            != store.run
            or traversal.inventory.inventory_sha256
            != root.initial_inventory_sha256
        ):
            raise CampaignReceiptExecutionError(
                "sample native inventory or environment differs"
            )
        global_controls, plan_controls = _controls(
            store,
            traversal,
            old_root=root,
            capacity=None,
        )
        refs = []
        inputs = hashlib.sha256(b"histdatacom.campaign-sampled-inputs.v1\n")
        seen = []
        rereads = [0]
        for evidence in traversal:
            product = _product(store.run, evidence)
            if evidence.ordinal not in retained or _semantic(
                product
            ) != _semantic(retained[evidence.ordinal][1]):
                raise CampaignReceiptExecutionError(
                    "sample differs from retained native evidence"
                )
            seen.append(evidence.ordinal)
            for item in product.input_files:
                inputs.update(
                    canonical(item.to_payload()).encode("ascii") + b"\n"
                )
            refs.append(store.write_immutable(product))
        _rehash_controls(store, global_controls + plan_controls, rereads)
        for ref in refs:
            _rehash_files(
                store.read(ref, CampaignProductReceiptV1).input_files, rereads
            )
        if tuple(seen) != product_ordinals or traversal.finish() is not None:
            raise CampaignReceiptExecutionError(
                "sample execution denominator differs"
            )
        _check_runtime(store.run)
        store.admit_namespace()
        sample = CampaignVerificationSampleV1(
            store.run.artifact_id,
            root.artifact_id,
            product_ordinals,
            tuple(refs),
            traversal.inventory.inventory_sha256,
            inputs.hexdigest(),
            _elapsed(start),
            traversal.read_bytes + rereads[0],
        )
        store.write_immutable(sample)
        store.admit_namespace()
        return sample

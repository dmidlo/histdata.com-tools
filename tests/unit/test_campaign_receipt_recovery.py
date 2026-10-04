"""Generated filesystem/recovery controls, not fake native-verifier positives.

The genuine native fixture and subprocess kill/resume oracles live in the
independent integration modules. These small tests exercise real store I/O and
fail-closed paths without replacing native science with successful doubles.
"""

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.campaign_index_contracts import canonical
from histdatacom.campaign_receipt_contracts import (
    CampaignVerificationFailureV1,
    CampaignVerificationJournalV1,
    CampaignVerificationRunV1,
)
from histdatacom.campaign_receipt_store import (
    CampaignReceiptStore,
    read_campaign_verification_tree,
)


@pytest.fixture
def run_spec(tmp_path):
    products = tmp_path / "native-products"
    products.mkdir()
    index = tmp_path / "index.json"
    index.write_bytes(b"{}")
    return CampaignVerificationRunV1(
        str(index),
        "invented-low-level-contract-index",
        hashlib.sha256(b"{}").hexdigest(),
        "a" * 64,
        canonical({"scope": "pure-store-tests-not-native-verification"}),
        (str(products),),
        products_per_shard=1,
    )


def failure(run):
    return CampaignVerificationFailureV1(
        run.artifact_id,
        None,
        None,
        "invented_test_refusal",
        "This is not a successful native verification.",
        1,
        0,
    )


def test_create_read_and_idempotent_closed_receipt_write(tmp_path, run_spec):
    directory = tmp_path / "receipts"
    with CampaignReceiptStore(directory, create=run_spec) as store:
        first = store.write_immutable(failure(run_spec))
        assert store.write_immutable(failure(run_spec)) == first
        assert store.read(first, CampaignVerificationFailureV1) == failure(
            run_spec
        )
        assert store.run == run_spec
    with CampaignReceiptStore(directory) as reopened:
        assert reopened.run == run_spec
        assert tuple(reopened.refs("failure")) == (first,)
    assert not tuple((directory / "root").iterdir())


@pytest.mark.parametrize("kind", ("nonempty", "managed", "symlink"))
def test_initialization_refuses_unsafe_namespace_without_changing_contents(
    tmp_path, run_spec, kind
):
    root = tmp_path / "receipts"
    if kind == "symlink":
        target = tmp_path / "target"
        target.mkdir()
        root.symlink_to(target, target_is_directory=True)
    else:
        root.mkdir()
        (
            root
            / (
                "kept.txt"
                if kind == "nonempty"
                else ".histdatacom-retention.json"
            )
        ).write_bytes(b"keep")
    with pytest.raises((ValueError, OSError)):
        CampaignReceiptStore(root, create=run_spec)
    if kind == "symlink":
        assert not tuple(target.iterdir())
    else:
        assert len(tuple(root.iterdir())) == 1


@pytest.mark.parametrize("relationship", ("same", "child", "parent"))
def test_receipt_root_cannot_overlap_generation_in_either_direction(
    tmp_path, run_spec, relationship
):
    product = Path(run_spec.forbidden_roots[0])
    target = {
        "same": product,
        "child": product / "receipts",
        "parent": tmp_path,
    }[relationship]
    with pytest.raises(ValueError, match="overlaps"):
        CampaignReceiptStore(target, create=run_spec)
    assert not (product / ".lock").exists()


def test_second_owner_cannot_enter_locked_store(tmp_path, run_spec):
    root = tmp_path / "receipts"
    with (
        CampaignReceiptStore(root, create=run_spec),
        pytest.raises(ValueError, match="busy"),
    ):
        CampaignReceiptStore(root)


@pytest.mark.parametrize("target_kind", ("marker", "directory", "lock"))
def test_pinned_namespace_swaps_are_refused(tmp_path, run_spec, target_kind):
    root = tmp_path / "receipts"
    with CampaignReceiptStore(root, create=run_spec) as store:
        if target_kind == "marker":
            target = root / "store.json"
            target.chmod(0o600)
            target.write_bytes(b"{}")
        elif target_kind == "lock":
            target = root / ".lock"
            target.rename(root / "old-lock")
            target.write_bytes(b"")
        else:
            target = root / "failure"
            target.rename(root / "old-failure")
            target.mkdir()
        with pytest.raises((ValueError, OSError)):
            store.write_immutable(failure(run_spec))


def test_external_hardlink_is_not_an_interrupted_owned_publication(
    tmp_path, run_spec
):
    root = tmp_path / "receipts"
    with CampaignReceiptStore(root, create=run_spec) as store:
        ref = store.run_ref
    os.link(root / "run" / (ref.sha256 + ".json"), tmp_path / "external")
    with pytest.raises(ValueError, match="unaccounted.*hardlink"):
        CampaignReceiptStore(root)


def test_exact_internal_two_link_interruption_can_be_read_without_cleanup(
    tmp_path, run_spec
):
    root = tmp_path / "receipts"
    with CampaignReceiptStore(root, create=run_spec) as store:
        ref = store.write_immutable(failure(run_spec))
    temporary = root / "tmp" / ("1" * 32 + ".tmp")
    os.link(root / "failure" / (ref.sha256 + ".json"), temporary)
    before = temporary.read_bytes()
    with CampaignReceiptStore(root) as store:
        assert store.read(ref, CampaignVerificationFailureV1) == failure(
            run_spec
        )
    assert temporary.read_bytes() == before
    assert temporary.stat().st_nlink == 2
    assert not tuple((root / "root").iterdir())


@pytest.mark.parametrize("phase", ("before_link", "after_link"))
def test_publication_fault_retains_temp_but_never_fabricates_success(
    tmp_path, run_spec, monkeypatch, phase
):
    root = tmp_path / "receipts"
    original_link = os.link
    original_sync = os.fsync
    linked = False

    def link(*args, **kwargs):
        nonlocal linked
        if phase == "before_link":
            raise OSError("invented link interruption")
        original_link(*args, **kwargs)
        linked = True

    def sync(fd):
        if linked:
            raise OSError("invented post-link interruption")
        original_sync(fd)

    with (
        CampaignReceiptStore(root, create=run_spec) as store,
        monkeypatch.context() as patch,
    ):
        patch.setattr(os, "link", link)
        patch.setattr(os, "fsync", sync)
        with pytest.raises(OSError, match="interruption"):
            store.write_immutable(failure(run_spec))
    assert len(tuple((root / "tmp").iterdir())) == 1
    with CampaignReceiptStore(root) as store:
        assert len(tuple(store.refs("failure"))) == (phase == "after_link")
        assert not tuple(store.refs("root"))


def test_unknown_or_corrupted_members_are_not_silently_ignored(
    tmp_path, run_spec
):
    root = tmp_path / "receipts"
    with CampaignReceiptStore(root, create=run_spec):
        pass
    (root / "failure" / ("f" * 64 + ".json")).write_bytes(b"{}")
    with pytest.raises(ValueError, match="hash differs"):
        CampaignReceiptStore(root)


def test_wrong_reference_hash_or_kind_refuses(tmp_path, run_spec):
    with CampaignReceiptStore(tmp_path / "receipts", create=run_spec) as store:
        ref = store.write_immutable(failure(run_spec))
        with pytest.raises(ValueError, match="kind"):
            store.read(ref, CampaignVerificationRunV1)
        with pytest.raises((ValueError, OSError)):
            store.read(
                replace(ref, sha256="f" * 64), CampaignVerificationFailureV1
            )


def test_mutated_exact_receipt_is_readmitted_before_writing(tmp_path, run_spec):
    value = failure(run_spec)
    object.__setattr__(value, "elapsed_ns", -1)
    with CampaignReceiptStore(tmp_path / "receipts", create=run_spec) as store:
        with pytest.raises(ValueError):
            store.write_immutable(value)
        assert not tuple(store.refs("failure"))


def test_arbitrary_serializer_is_never_called(tmp_path, run_spec):
    class Poison:
        def to_json(self):
            pytest.fail("untrusted serializer called")

    with (
        CampaignReceiptStore(tmp_path / "receipts", create=run_spec) as store,
        pytest.raises(ValueError, match="exact closed"),
    ):
        store.write_immutable(Poison())


def test_journal_chain_and_subject_are_checked(tmp_path, run_spec):
    with CampaignReceiptStore(tmp_path / "receipts", create=run_spec) as store:
        first = store.write_immutable(
            CampaignVerificationJournalV1(
                run_spec.artifact_id,
                0,
                None,
                "started",
                store.run_ref,
                None,
            )
        )
        fail = store.write_immutable(failure(run_spec))
        store.write_immutable(
            CampaignVerificationJournalV1(
                run_spec.artifact_id,
                2,
                first,
                "failed",
                fail,
                None,
            )
        )
        with pytest.raises(ValueError, match="contiguous"):
            store.journal()


def test_bare_expected_root_id_never_creates_authority(tmp_path, run_spec):
    root = tmp_path / "receipts"
    with CampaignReceiptStore(root, create=run_spec):
        pass
    with pytest.raises(ValueError, match="not retained"):
        read_campaign_verification_tree(
            root, expected_root_id="campaign-receipt-root:sha256:" + "f" * 64
        )


@pytest.mark.parametrize("kind", ("symlink", "fifo", "directory"))
def test_rehash_refuses_nonregular_inputs_before_open(
    tmp_path, monkeypatch, kind
):
    from histdatacom.campaign_receipt_runner import _file_bytes

    target = tmp_path / "payload"
    if kind == "symlink":
        target.symlink_to(tmp_path / "absent")
    elif kind == "fifo":
        os.mkfifo(target)
    else:
        target.mkdir()
    with pytest.raises((ValueError, OSError)):
        _file_bytes(target)


def test_rehash_reads_real_bytes_not_declared_hash(tmp_path):
    from histdatacom.campaign_receipt_runner import _file_bytes

    target = tmp_path / "payload"
    target.write_bytes(b"actual")
    assert _file_bytes(target) == (6, hashlib.sha256(b"actual").hexdigest())
    with pytest.raises(ValueError, match="SHA-256"):
        _file_bytes(target, expected_size=6, expected_sha256="f" * 64)


def test_changed_environment_refuses_without_native_work(run_spec, monkeypatch):
    from histdatacom import campaign_receipt_runner as runner

    monkeypatch.setattr(
        runner,
        "_runtime_identity",
        lambda: ("b" * 64, run_spec.environment_json),
    )
    with pytest.raises(ValueError, match="implementation/environment"):
        runner._check_runtime(run_spec)


@pytest.mark.parametrize("target", ("root", "directory"))
def test_idempotent_write_rechecks_namespace_after_actual_read(
    tmp_path, run_spec, monkeypatch, target
):
    path = tmp_path / "receipts"
    with CampaignReceiptStore(path, create=run_spec) as store:
        value = failure(run_spec)
        ref = store.write_immutable(value)
        original = store._read_member

        def changed(kind, name):
            data = original(kind, name)
            if kind == "failure":
                current = path if target == "root" else path / "failure"
                current.rename(tmp_path / "detached")
                current.mkdir()
            return data

        monkeypatch.setattr(store, "_read_member", changed)
        with pytest.raises((ValueError, OSError)):
            store.write_immutable(value)
        assert ref.size_bytes > 0
        assert (
            not tuple((path / "root").iterdir())
            if target == "directory"
            else not tuple(path.iterdir())
        )


def test_rehash_failure_preserves_actual_read_counter(tmp_path):
    from histdatacom.campaign_receipt_runner import _file_bytes

    path = tmp_path / "input"
    path.write_bytes(b"invented")
    counter = [0]
    with pytest.raises(ValueError, match="SHA-256"):
        _file_bytes(path, expected_sha256="f" * 64, counter=counter)
    assert counter == [8]


def test_known_metadata_reservation_is_per_shard_not_per_product(
    tmp_path, run_spec
):
    from histdatacom.campaign_receipt_runner import _Capacity

    spec = replace(run_spec, products_per_shard=64)
    with CampaignReceiptStore(tmp_path / "receipts", create=spec) as store:
        capacity = _Capacity(store, 262144)
        # Conservative metadata reserve remains bounded below 8GiB; unknown
        # product/control leaf sizes still receive exact incremental admission.
        assert capacity.bytes < 8 * 1024**3
        assert capacity.files < 2 * 262144 + 5 * 4096 + 128


def test_capacity_refuses_before_next_receipt_write(tmp_path, run_spec):
    from histdatacom.campaign_receipt_store import MAX_STORE_BYTES

    with CampaignReceiptStore(tmp_path / "receipts", create=run_spec) as store:
        with pytest.raises(ValueError, match="capacity"):
            store.reserve_capacity(files=1, size_bytes=MAX_STORE_BYTES)
        assert not tuple(store.refs("failure"))


@pytest.mark.parametrize("place", ("root", "kind"))
def test_final_namespace_admission_refuses_late_unknown_members(
    tmp_path, run_spec, place
):
    root = tmp_path / "receipts"
    with CampaignReceiptStore(root, create=run_spec) as store:
        ref = store.write_immutable(failure(run_spec))
        target = root if place == "root" else root / "failure"
        (target / "untracked.json").write_bytes(b"{}")
        # Existing payload reads are not a complete namespace census.
        assert store.read(ref, CampaignVerificationFailureV1) == failure(
            run_spec
        )
        with pytest.raises(
            ValueError, match="unexpected receipt|inventory file bound"
        ):
            store.admit_namespace()
    assert (target / "untracked.json").read_bytes() == b"{}"


def test_resumed_journal_cannot_select_an_uncommitted_checkpoint(
    tmp_path, run_spec
):
    from histdatacom.campaign_receipt_contracts import (
        CampaignVerificationCheckpointV1,
    )

    with CampaignReceiptStore(tmp_path / "receipts", create=run_spec) as store:
        started = store.write_immutable(
            CampaignVerificationJournalV1(
                run_spec.artifact_id,
                0,
                None,
                "started",
                store.run_ref,
                None,
            )
        )
        checkpoint = store.write_immutable(
            CampaignVerificationCheckpointV1(
                run_spec.artifact_id,
                started,
                (),
                (),
                0,
                "f" * 64,
                1,
            )
        )
        store.write_immutable(
            CampaignVerificationJournalV1(
                run_spec.artifact_id,
                1,
                started,
                "resumed",
                checkpoint,
                None,
            )
        )
        with pytest.raises(ValueError, match="not previously committed"):
            store.journal()

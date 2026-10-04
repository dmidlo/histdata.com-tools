"""Synthetic syscall-boundary tests, not high-level GC authorization tests."""

from dataclasses import replace
import hashlib
import os
from pathlib import Path
import stat

import pytest

from histdatacom.artifact_retention import secure_fs as fs
from histdatacom.artifact_retention.contracts import (
    RetentionPolicyV1,
    StoreMarkerV1,
)
from histdatacom.managed_artifact_boundary import (
    MANAGED_ARTIFACT_MARKER,
    ManagedArtifactBoundaryError,
    assert_unmanaged_mutation_paths,
)

PAYLOAD = "objects/" + "1" * 32 + ".data"
SECOND = "objects/" + "2" * 32 + ".scratch"


@pytest.fixture
def store(tmp_path):
    root = tmp_path.resolve() / "managed"
    fs.create_store_filesystem(root, RetentionPolicyV1())
    return root


def test_initialize_real_empty_namespace_binds_policy_and_location(tmp_path):
    root = tmp_path.resolve() / "managed"
    policy = RetentionPolicyV1(scratch_ttl_ns=123)
    marker = fs.create_store_filesystem(root, policy)
    observed = StoreMarkerV1.from_json(
        (root / MANAGED_ARTIFACT_MARKER).read_text()
    )
    assert marker == observed
    assert marker.policy == policy
    assert marker.state == "ready"
    assert marker.absolute_path == str(root)
    assert (marker.device, marker.inode) == (
        root.stat().st_dev,
        root.stat().st_ino,
    )
    assert set(path.name for path in root.iterdir()) == set(
        fs.STORE_DIRECTORIES
    ) | {MANAGED_ARTIFACT_MARKER, fs.LOCK_NAME}
    with fs.StoreSession(root) as session:
        assert session.marker == marker
        assert [item.relative_path for item in session.inventory()] == sorted(
            (MANAGED_ARTIFACT_MARKER, fs.LOCK_NAME)
        )
    with pytest.raises(ManagedArtifactBoundaryError):
        assert_unmanaged_mutation_paths((root,), recursive=True)


def test_existing_empty_directory_can_be_explicitly_initialized(tmp_path):
    root = tmp_path.resolve() / "empty"
    root.mkdir()
    before = root.stat().st_ino
    marker = fs.create_store_filesystem(root, RetentionPolicyV1())
    assert marker.inode == before


def test_nonempty_directory_is_not_adopted(tmp_path):
    root = tmp_path.resolve() / "existing"
    root.mkdir()
    source = root / "source.csv"
    source.write_bytes(b"generated original source")
    with pytest.raises(fs.RetentionFilesystemError, match="empty"):
        fs.create_store_filesystem(root, RetentionPolicyV1())
    assert source.read_bytes() == b"generated original source"
    assert list(root.iterdir()) == [source]


@pytest.mark.parametrize("which", ["root", "home", "traversal"])
def test_broad_or_parent_traversal_paths_refused_before_creation(
    tmp_path, which
):
    target = {
        "root": "/",
        "home": str(Path.home()),
        "traversal": str(tmp_path.resolve()) + "/child/../store",
    }[which]
    with pytest.raises(fs.RetentionFilesystemError):
        fs.create_store_filesystem(target, RetentionPolicyV1())


@pytest.mark.parametrize("component", ["parent", "leaf"])
def test_symlink_components_never_followed(tmp_path, component):
    base = tmp_path.resolve()
    destination = base / "destination"
    destination.mkdir()
    link = base / "alias"
    link.symlink_to(destination, target_is_directory=True)
    target = link / "managed" if component == "parent" else link
    with pytest.raises((OSError, fs.RetentionFilesystemError)):
        fs.create_store_filesystem(target, RetentionPolicyV1())
    assert not list(destination.iterdir())


def test_nested_or_containing_managed_store_refused(store):
    with pytest.raises(ManagedArtifactBoundaryError):
        fs.create_store_filesystem(
            store / "objects/nested", RetentionPolicyV1()
        )
    with pytest.raises(ManagedArtifactBoundaryError):
        fs.create_store_filesystem(store.parent, RetentionPolicyV1())


def test_exclusive_lock_and_release(store):
    with fs.StoreSession(store) as first:
        with pytest.raises(fs.RetentionFilesystemError, match="busy"):
            fs.StoreSession(store)
        first.inventory()
    with fs.StoreSession(store) as second:
        second.inventory()
    with pytest.raises(fs.RetentionFilesystemError, match="closed"):
        second.inventory()
    second.close()


def test_exclusive_payload_and_content_bound_control_roundtrip(store):
    data = b"synthetic immutable payload\n"
    control = b'{"synthetic":true}'
    control_path = "journal/" + hashlib.sha256(control).hexdigest() + ".json"
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, data)
        session.write_immutable(control_path, control)
        assert observed.sha256 == hashlib.sha256(data).hexdigest()
        assert observed.size_bytes == len(data)
        assert stat.S_IMODE(observed.mode) == 0o400
        assert session.read_bytes(PAYLOAD) == data
        assert session.read_bytes(control_path) == control
        assert observed in session.inventory()
        with pytest.raises(FileExistsError):
            session.write_immutable(PAYLOAD, b"replacement")
        assert session.read_bytes(PAYLOAD) == data


@pytest.mark.parametrize(
    "relative",
    [
        "../outside",
        "/absolute",
        "objects/../outside",
        "objects/name.data",
        "objects/" + "A" * 32 + ".data",
        "objects/" + "1" * 32 + ".zip",
        "unknown/" + "1" * 64 + ".json",
        "descriptors/wrong.json",
        "objects",
        MANAGED_ARTIFACT_MARKER,
        fs.LOCK_NAME,
    ],
)
def test_unsafe_or_reserved_publication_names_refused_without_effect(
    store, relative
):
    with fs.StoreSession(store) as session:
        before = session.inventory()
        with pytest.raises(fs.RetentionFilesystemError):
            session.write_immutable(relative, b"generated")
        assert session.inventory() == before


def test_control_filename_must_match_exact_bytes(store):
    with fs.StoreSession(store) as session:
        with pytest.raises(fs.RetentionFilesystemError, match="exact bytes"):
            session.write_immutable("journal/" + "0" * 64 + ".json", b"{}")
        assert not list((store / "journal").iterdir())


@pytest.mark.parametrize("maximum", [-1, True, fs.MAX_PAYLOAD_BYTES + 1])
def test_invalid_read_limits_refused(store, maximum):
    with fs.StoreSession(store) as session:
        with pytest.raises(fs.RetentionFilesystemError, match="bound"):
            session.read_bytes(MANAGED_ARTIFACT_MARKER, maximum=maximum)


def test_size_bound_checked_before_payload_read(store, monkeypatch):
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, b"123456")
        actual_read = fs.os.read

        def reject_payload_read(descriptor, size):
            assert os.fstat(descriptor).st_ino != observed.inode
            return actual_read(descriptor, size)

        monkeypatch.setattr(fs.os, "read", reject_payload_read)
        with pytest.raises(fs.RetentionFilesystemError, match="size"):
            session.read_bytes(PAYLOAD, maximum=5)


@pytest.mark.parametrize("shape", ["symlink", "hardlink", "fifo", "directory"])
def test_unsafe_payload_types_never_read_or_unlinked(store, shape):
    outside = store.parent / "generated-external"
    outside.write_bytes(b"preserved generated control")
    target = store / PAYLOAD
    if shape == "symlink":
        target.symlink_to(outside)
    elif shape == "hardlink":
        os.link(outside, target)
    elif shape == "fifo":
        os.mkfifo(target)
    else:
        target.mkdir()
    with fs.StoreSession(store) as session:
        with pytest.raises((OSError, fs.RetentionFilesystemError)):
            session.inventory()
    assert target.lstat()
    assert outside.read_bytes() == b"preserved generated control"


@pytest.mark.parametrize("where", ["root", "objects", "journal"])
def test_extra_unknown_member_refuses_complete_inventory(store, where):
    parent = store if where == "root" else store / where
    extra = parent / "unregistered-hidden-reference.json"
    extra.write_bytes(b'{"requires":"unknown"}')
    with pytest.raises(fs.RetentionFilesystemError):
        with fs.StoreSession(store) as session:
            session.inventory()
    assert extra.read_bytes() == b'{"requires":"unknown"}'


def test_exact_low_level_unlink_preserves_other_files(store):
    with fs.StoreSession(store) as session:
        first = session.write_immutable(PAYLOAD, b"generated disposable")
        second = session.write_immutable(SECOND, b"generated retained")
        # This exercises a private syscall seam only. High-level store tests
        # must separately establish native graph authority and durable intent.
        session._unlink_payload(first)
        assert not (store / PAYLOAD).exists()
        assert session.read_bytes(SECOND) == b"generated retained"
        assert second in session.inventory()
        with pytest.raises(FileNotFoundError):
            session._unlink_payload(first)


def test_changed_payload_observation_refuses_unlink(store):
    with fs.StoreSession(store) as session:
        original = session.write_immutable(PAYLOAD, b"original")
        target = store / PAYLOAD
        target.chmod(0o600)
        target.write_bytes(b"modified")
        target.chmod(0o400)
        with pytest.raises(fs.RetentionFilesystemError, match="changed"):
            session._unlink_payload(original)
        assert target.read_bytes() == b"modified"


def test_forged_observation_or_control_target_cannot_delete(store):
    with fs.StoreSession(store) as session:
        original = session.write_immutable(PAYLOAD, b"original")
        for forged in (
            replace(original, sha256="0" * 64),
            replace(original, inode=original.inode + 1),
            replace(original, relative_path=MANAGED_ARTIFACT_MARKER),
        ):
            with pytest.raises(fs.RetentionFilesystemError):
                session._unlink_payload(forged)
        assert (store / PAYLOAD).read_bytes() == b"original"


def test_child_directory_swap_refuses_pinned_session(store):
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, b"original")
        moved = store.parent / "moved-objects"
        (store / "objects").rename(moved)
        (store / "objects").mkdir()
        with pytest.raises(
            fs.RetentionFilesystemError, match="directory identity"
        ):
            session._unlink_payload(observed)
        assert (moved / Path(PAYLOAD).name).read_bytes() == b"original"


def test_root_relocation_cannot_retarget_pinned_session(store):
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, b"original")
        moved = store.parent / "relocated"
        store.rename(moved)
        store.mkdir()
        with pytest.raises(fs.RetentionFilesystemError, match="root identity"):
            session._unlink_payload(observed)
        assert (moved / PAYLOAD).read_bytes() == b"original"
    with pytest.raises(fs.RetentionFilesystemError, match="relocated"):
        fs.StoreSession(moved)


def test_lock_replacement_is_detected_before_effect(store):
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, b"original")
        lock = store / fs.LOCK_NAME
        lock.rename(store.parent / "old-lock")
        lock.write_bytes(b"")
        with pytest.raises(fs.RetentionFilesystemError, match="lock identity"):
            session._unlink_payload(observed)
        assert (store / PAYLOAD).read_bytes() == b"original"


def test_marker_policy_mutation_is_detected_in_inventory(store):
    with fs.StoreSession(store) as session:
        marker = replace(session.marker, policy=RetentionPolicyV1(0))
        path = store / MANAGED_ARTIFACT_MARKER
        path.chmod(0o600)
        path.write_text(marker.to_json())
        path.chmod(0o400)
        with pytest.raises(fs.RetentionFilesystemError, match="policy marker"):
            session.inventory()


@pytest.mark.parametrize("operation", ["read", "write", "unlink"])
def test_changed_policy_refused_at_each_operation_boundary(store, operation):
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, b"must remain intact")
        marker = replace(session.marker, policy=RetentionPolicyV1(0))
        path = store / MANAGED_ARTIFACT_MARKER
        path.chmod(0o600)
        path.write_text(marker.to_json())
        path.chmod(0o400)
        with pytest.raises(fs.RetentionFilesystemError, match="policy marker"):
            if operation == "read":
                session.read_bytes(PAYLOAD)
            elif operation == "write":
                session.write_immutable(SECOND, b"unpublished")
            else:
                session._unlink_payload(observed)
        assert (store / PAYLOAD).read_bytes() == b"must remain intact"
        assert not (store / SECOND).exists()


def test_bounded_directory_reader_stops_without_full_materialization(
    monkeypatch,
):
    class Names:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def __iter__(self):
            for number in range(4):
                yield type("Entry", (), {"name": str(number)})()
            pytest.fail("enumeration continued beyond max plus one")

    monkeypatch.setattr(fs.os, "scandir", lambda _fd: Names())
    with pytest.raises(fs.RetentionFilesystemError, match="bound"):
        fs._bounded_names(0, 3)


def test_normal_store_operations_do_not_call_unbounded_listdir(
    store, monkeypatch
):
    monkeypatch.setattr(
        fs.os, "listdir", lambda *_a: pytest.fail("unbounded listing")
    )
    with fs.StoreSession(store) as session:
        session.inventory()
        session.write_immutable(PAYLOAD, b"bounded")
        session.inventory()


def test_oversized_raw_path_refused_before_normalization(monkeypatch):
    monkeypatch.setattr(
        fs.os.path, "abspath", lambda *_a: pytest.fail("normalization reached")
    )
    with pytest.raises(fs.RetentionFilesystemError):
        fs._absolute_path("x" * 4097)


def test_initialization_failure_retains_protective_incomplete_marker(
    tmp_path, monkeypatch
):
    root = tmp_path.resolve() / "failed-initialization"
    real_fsync = fs.os.fsync

    def fail_regular(descriptor):
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("injected file fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(fs.os, "fsync", fail_regular)
    with pytest.raises(OSError, match="injected"):
        fs.create_store_filesystem(root, RetentionPolicyV1())
    marker = StoreMarkerV1.from_json(
        (root / MANAGED_ARTIFACT_MARKER).read_text()
    )
    assert marker.state == "initializing"
    with pytest.raises(ManagedArtifactBoundaryError):
        assert_unmanaged_mutation_paths((root,), recursive=True)


def test_failed_immutable_write_is_retained_not_rolled_back(store, monkeypatch):
    with fs.StoreSession(store) as session:
        monkeypatch.setattr(
            fs.os,
            "fsync",
            lambda *_a: (_ for _ in ()).throw(
                OSError("injected durability failure")
            ),
        )
        with pytest.raises(OSError, match="durability"):
            session.write_immutable(PAYLOAD, b"observed but durability unknown")
        assert (
            store / PAYLOAD
        ).read_bytes() == b"observed but durability unknown"


def test_post_unlink_fsync_failure_is_not_absence_as_success(
    store, monkeypatch
):
    with fs.StoreSession(store) as session:
        observed = session.write_immutable(PAYLOAD, b"generated")

        def fail(_descriptor):
            raise OSError("injected post-unlink fsync failure")

        monkeypatch.setattr(fs.os, "fsync", fail)
        with pytest.raises(OSError, match="post-unlink"):
            session._unlink_payload(observed)
        assert not (store / PAYLOAD).exists()
        with pytest.raises(FileNotFoundError):
            session._unlink_payload(observed)


def test_unsupported_platform_primitives_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(fs.os, "supports_dir_fd", set())
    root = tmp_path.resolve() / "unsupported"
    with pytest.raises(fs.RetentionFilesystemError, match="dirfd"):
        fs.create_store_filesystem(root, RetentionPolicyV1())
    assert not root.exists()


def test_inventory_aggregate_budget_blocks_before_further_use(
    store, monkeypatch
):
    with fs.StoreSession(store) as session:
        session.write_immutable(PAYLOAD, b"one")
        session.write_immutable(SECOND, b"two")
        monkeypatch.setattr(fs, "MAX_STORE_FILES", 3)
        with pytest.raises(fs.RetentionFilesystemError, match="aggregate"):
            session.inventory()
        assert (store / PAYLOAD).read_bytes() == b"one"
        assert (store / SECOND).read_bytes() == b"two"

"""File/census boundaries only; actual native positives live in the oracle.

These tests never manufacture a passed native product or replace its verifier.
Simple namespaces below describe directory membership, not scientific products.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from histdatacom import campaign_verification as verifier
from histdatacom.runtime_contracts import ArtifactRef


def _ref(path, *, kind="declared-test-bytes"):
    payload = path.read_bytes()
    return ArtifactRef(
        kind, str(path), len(payload), hashlib.sha256(payload).hexdigest()
    )


def _membership(tmp_path, names=("parts/data.parquet",)):
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="ascii")
    partitions = []
    for name in names:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"directory-census-only-not-Parquet")
        partitions.append(SimpleNamespace(relative_path=name))
    return manifest, SimpleNamespace(partitions=tuple(partitions))


def test_measured_file_bytes_include_actual_rereads_and_preserve_legacy_digest(
    tmp_path,
):
    path = tmp_path / "control.json"
    payload = b'{"literal":true}'
    path.write_bytes(payload)
    guard = verifier._Guard()
    assert guard.document(path) == {"literal": True}
    assert guard.read_bytes == len(payload)
    actual = guard.finish()
    assert guard.read_bytes == 2 * len(payload)
    # Independent literal old domain/order oracle; new metrics are not hashed
    # into the historical public verifier's deterministic input digest.
    row = {
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"))
    assert (
        actual
        == hashlib.sha256(
            b"histdatacom.campaign-inputs.v1\n" + encoded.encode() + b"\n"
        ).hexdigest()
    )


@pytest.mark.parametrize(
    "name,control,role",
    [
        ("partition.data", False, "source"),
        ("partition.parquet", False, "parquet"),
        ("control.json", True, "control"),
        ("opaque.bin", False, "input"),
    ],
)
def test_detached_file_evidence_is_measured_and_immutable(
    tmp_path, name, control, role
):
    path = tmp_path / name
    path.write_bytes(b"{}")
    guard = verifier._Guard()
    guard.read(path, control=control)
    item = verifier._file_evidence(guard)[0]
    info = path.stat()
    assert (item.path, item.size_bytes, item.sha256, item.role) == (
        str(path),
        2,
        hashlib.sha256(b"{}").hexdigest(),
        role,
    )
    assert item.identity == (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )
    with pytest.raises(FrozenInstanceError):
        item.sha256 = "a" * 64
    guard.files.clear()
    guard.roles.clear()
    assert item.sha256 == hashlib.sha256(b"{}").hexdigest()


def test_each_product_guard_records_all_reused_transitive_source_bytes(
    tmp_path,
):
    source = tmp_path / "shared.data"
    source.write_bytes(b"a shared source payload")
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps({"source": _ref(source).to_dict()}), encoding="ascii"
    )
    graph = {"context": _ref(context).to_dict()}
    evidence = []
    for _ in range(2):
        guard = verifier._Guard()
        guard.graph(graph)
        guard.finish()
        evidence.append(verifier._file_evidence(guard))
        assert (
            guard.read_bytes >= source.stat().st_size + context.stat().st_size
        )
    assert evidence[0] == evidence[1]
    assert {item.path for item in evidence[1]} == {str(source), str(context)}


def test_plan_closure_budget_is_not_a_global_campaign_file_budget(
    tmp_path, monkeypatch
):
    from histdatacom import campaign_receipt_contracts

    # Four global controls exceed a two-file product closure. This is an I/O
    # boundary vector, not a native plan or successful scientific verifier.
    monkeypatch.setattr(
        campaign_receipt_contracts, "MAX_PRODUCT_INPUT_FILES", 2
    )
    global_guard = verifier._Guard()
    unrelated = []
    for ordinal in range(4):
        path = tmp_path / f"other-plan-{ordinal}.json"
        path.write_bytes(b"{}")
        global_guard.document(path)
        unrelated.append(path)
    source = tmp_path / "this-plan-source.data"
    source.write_bytes(b"only this plan's shared source")
    plan_path = tmp_path / "this-plan.json"
    graph = {"source": _ref(source).to_dict()}
    plan_path.write_text(json.dumps(graph), encoding="ascii")
    descriptor = SimpleNamespace(plan_ref=_ref(plan_path))
    plan_graph = SimpleNamespace(to_dict=lambda: graph)
    product_guard = verifier._plan_input_guard(descriptor, plan_graph)
    product_guard.finish()
    assert len(global_guard.files) == 4
    assert product_guard.maximum_files == 2
    assert set(product_guard.files) == {source, plan_path}
    assert not set(unrelated) & set(product_guard.files)


def test_plan_closure_rechecks_each_actual_reused_dependency(tmp_path):
    source = tmp_path / "source.data"
    source.write_bytes(b"original")
    graph = {"source": _ref(source).to_dict()}
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(graph), encoding="ascii")
    descriptor = SimpleNamespace(plan_ref=_ref(path))
    plan_graph = SimpleNamespace(to_dict=lambda: graph)
    first = verifier._plan_input_guard(descriptor, plan_graph)
    first.finish()
    source.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="SHA-256 differs"):
        verifier._plan_input_guard(descriptor, plan_graph)


def test_changed_shared_source_refuses_before_emitting_a_second_observation(
    tmp_path,
):
    source = tmp_path / "shared.data"
    source.write_bytes(b"first")
    graph = {"source": _ref(source).to_dict()}
    first = verifier._Guard()
    first.graph(graph)
    first.finish()
    source.write_bytes(b"other")
    with pytest.raises(ValueError, match="SHA-256 differs"):
        verifier._Guard().graph(graph)


def test_finish_refuses_same_size_changed_bytes(tmp_path):
    path = tmp_path / "evidence"
    path.write_bytes(b"before")
    guard = verifier._Guard()
    guard.read(path)
    path.write_bytes(b"after!")
    with pytest.raises(ValueError, match="changed during verification"):
        guard.finish()


def test_file_bound_precedes_open_of_next_input(tmp_path, monkeypatch):
    one = tmp_path / "one"
    two = tmp_path / "two"
    one.write_bytes(b"1")
    two.write_bytes(b"2")
    guard = verifier._Guard(maximum_files=1)
    guard.read(one)
    monkeypatch.setattr(
        verifier.os,
        "open",
        lambda *a, **k: pytest.fail("opened over-bound file"),
    )
    with pytest.raises(ValueError, match="file-inventory bound"):
        guard.read(two)


def test_parquet_census_matches_all_declared_paths_in_canonical_order(tmp_path):
    path, manifest = _membership(tmp_path, ("z/z.parquet", "a/a.parquet"))
    assert verifier._parquet_membership(path, manifest) == (
        str(tmp_path / "a/a.parquet"),
        str(tmp_path / "z/z.parquet"),
    )


@pytest.mark.parametrize(
    "extra", ["extra.parquet", "nested/deeper/extra.parquet"]
)
def test_parquet_census_rejects_unreferenced_files(tmp_path, extra):
    path, manifest = _membership(tmp_path)
    target = tmp_path / extra
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"not listed in any manifest")
    with pytest.raises(ValueError, match="actual Parquet membership differs"):
        verifier._parquet_membership(path, manifest)


def test_parquet_census_rejects_missing_declared_file(tmp_path):
    path, manifest = _membership(tmp_path)
    (tmp_path / "parts/data.parquet").unlink()
    with pytest.raises(ValueError, match="actual Parquet membership differs"):
        verifier._parquet_membership(path, manifest)


@pytest.mark.parametrize("relative", ["../escape.parquet", "/outside.parquet"])
def test_parquet_declaration_cannot_escape_commit_directory(tmp_path, relative):
    path, _ = _membership(tmp_path)
    manifest = SimpleNamespace(
        partitions=(SimpleNamespace(relative_path=relative),)
    )
    with pytest.raises(ValueError, match="partition escapes"):
        verifier._parquet_membership(path, manifest)


@pytest.mark.parametrize("mode", ["duplicate", "extension"])
def test_parquet_declarations_reject_duplicate_or_nonparquet(tmp_path, mode):
    path, manifest = _membership(tmp_path)
    manifest.partitions = (
        manifest.partitions * 2
        if mode == "duplicate"
        else (SimpleNamespace(relative_path="opaque.data"),)
    )
    with pytest.raises(ValueError, match="Parquet declarations differ"):
        verifier._parquet_membership(path, manifest)


def test_parquet_census_refuses_symlink_without_following(tmp_path):
    path, manifest = _membership(tmp_path)
    (tmp_path / "alias").symlink_to(
        tmp_path / "parts", target_is_directory=True
    )
    with pytest.raises(ValueError, match="symlink"):
        verifier._parquet_membership(path, manifest)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX-only admission")
def test_parquet_census_refuses_fifo_without_reading(tmp_path):
    path, manifest = _membership(tmp_path)
    os.mkfifo(tmp_path / "special")
    with pytest.raises(ValueError, match="special file"):
        verifier._parquet_membership(path, manifest)


def test_parquet_census_refuses_bounded_tree_overflow(tmp_path, monkeypatch):
    from histdatacom import campaign_receipt_contracts

    path, manifest = _membership(tmp_path)
    monkeypatch.setattr(
        campaign_receipt_contracts, "MAX_PRODUCT_INPUT_FILES", 1
    )
    with pytest.raises(ValueError, match="directory bound"):
        verifier._parquet_membership(path, manifest)


@pytest.mark.parametrize(
    "selected",
    [[], (), (True,), (-1,), (1.0,), ("1",), (1, 1), (1, 0), tuple(range(65))],
)
def test_selected_ordinals_reject_mistyped_duplicate_unsorted_or_overbound(
    selected,
):
    with pytest.raises(ValueError, match="sample ordinals"):
        verifier._selected_product_ordinals(selected, 100)


def test_selected_ordinals_require_actual_existing_coordinates():
    assert verifier._selected_product_ordinals(None, 0) == ()
    assert verifier._selected_product_ordinals(None, 3) == (0, 1, 2)
    assert verifier._selected_product_ordinals((0, 2), 3) == (0, 2)
    with pytest.raises(ValueError, match="sample ordinals"):
        verifier._selected_product_ordinals((3,), 3)


def test_invalid_sample_shape_is_refused_before_index_reads(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        verifier._Guard,
        "read",
        lambda *a, **k: pytest.fail("read before admission"),
    )
    with pytest.raises(ValueError, match="sample ordinals"):
        verifier.open_campaign_verification(
            tmp_path / "missing", selected_ordinals=(True,)
        )


@pytest.mark.parametrize(
    "failed,finished,exhausted",
    [(True, False, True), (False, True, True), (False, False, False)],
)
def test_finish_rejects_failed_repeated_or_partial_traversal_before_io(
    failed,
    finished,
    exhausted,
    monkeypatch,
):
    # State-machine negative only: no fabricated successful summary/verification.
    traversal = object.__new__(verifier.CampaignVerificationTraversal)
    traversal._failed = failed
    traversal._finished = finished
    traversal._exhausted = exhausted
    traversal._controls_exhausted = True
    monkeypatch.setattr(
        verifier.CampaignVerificationTraversal,
        "_scan",
        lambda *a, **k: pytest.fail("scan before completion admission"),
    )
    with pytest.raises(ValueError, match="exhaust successfully"):
        traversal.finish()


def test_products_require_complete_control_evidence_before_native_work():
    traversal = object.__new__(verifier.CampaignVerificationTraversal)
    traversal._failed = False
    traversal._finished = False
    traversal._controls_exhausted = False
    with pytest.raises(ValueError, match="control evidence must exhaust"):
        next(traversal)


def test_finish_cannot_skip_control_evidence_even_with_no_products():
    traversal = object.__new__(verifier.CampaignVerificationTraversal)
    traversal._failed = False
    traversal._finished = False
    traversal._exhausted = True
    traversal._controls_exhausted = False
    with pytest.raises(ValueError, match="exhaust successfully"):
        traversal.finish()
